"""Parquet writing with explicit Arrow schemas, and the optional DuckDB load."""

from __future__ import annotations

import math
import operator
import os
import re
from collections.abc import Mapping, Sequence
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .schemas import SCHEMAS


def _clean(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if v is pd.NaT:
        return None
    if isinstance(v, np.generic):
        return v.item()
    return v


def _py_to_type(values: list, t: pa.DataType) -> pa.Array:
    if pa.types.is_timestamp(t) or pa.types.is_integer(t):
        out = []
        for v in values:
            v = _clean(v)
            if v is None:
                out.append(None)
            elif isinstance(v, pd.Timestamp):
                out.append(int(v.value // 1_000_000))
            elif isinstance(v, bool):
                out.append(int(v))
            else:
                out.append(int(v))
        arr = pa.array(out, type=pa.int64())
        return arr.cast(t)
    if pa.types.is_floating(t):
        return pa.array([None if (x := _clean(v)) is None else float(x) for v in values], type=t)
    if pa.types.is_boolean(t):
        return pa.array([None if (x := _clean(v)) is None else bool(x) for v in values], type=t)
    if pa.types.is_string(t):
        return pa.array([None if (x := _clean(v)) is None else str(x) for v in values], type=t)
    if pa.types.is_list(t):
        vt = t.value_type
        out = []
        for v in values:
            if v is None or (isinstance(v, float) and math.isnan(v)):
                out.append(None)
            else:
                out.append([_clean(x) if not pa.types.is_integer(vt) else (None if _clean(x) is None else int(x))
                            for x in list(v)])
        return pa.array(out, type=t)
    return pa.array([_clean(v) for v in values], type=t)


def _fast_array(values: list, t: pa.DataType) -> pa.Array:
    """pa.array on already-clean python values (ints for timestamps); falls back to the cleaning path."""
    try:
        return pa.array(values, type=t)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError, OverflowError):
        return _py_to_type(list(values), t)


def _series_to_type(s: pd.Series, t: pa.DataType) -> pa.Array:
    if (pa.types.is_integer(t) or pa.types.is_timestamp(t) or pa.types.is_floating(t)) and \
            pd.api.types.is_numeric_dtype(s.dtype) and not pd.api.types.is_bool_dtype(s.dtype):
        if pd.api.types.is_integer_dtype(s.dtype) and not pa.types.is_floating(t):
            arr = pa.array(s, from_pandas=True).cast(pa.int64())
            return arr.cast(t) if not pa.types.is_int64(t) else arr
        arr = pa.array(s.to_numpy(dtype="float64", na_value=np.nan), from_pandas=True)
        if pa.types.is_floating(t):
            return arr.cast(t)
        arr = arr.cast(pa.int64(), safe=False)
        return arr.cast(t) if not pa.types.is_int64(t) else arr
    return _py_to_type(s.tolist(), t)


def to_arrow_table(data, schema: pa.Schema | str) -> pa.Table:
    """DataFrame / list of dicts / dict of lists -> pa.Table with exactly `schema` (missing columns = nulls,
    extra keys ignored)."""
    if isinstance(schema, str):
        schema = SCHEMAS[schema]
    arrays = []
    if isinstance(data, pd.DataFrame):
        n = len(data)
        for f in schema:
            arrays.append(_series_to_type(data[f.name], f.type) if f.name in data.columns
                          else pa.nulls(n, type=f.type))
    elif isinstance(data, Mapping):  # columnar
        n = len(next(iter(data.values()))) if data else 0
        for f in schema:
            arrays.append(_fast_array(data[f.name], f.type) if f.name in data else pa.nulls(n, type=f.type))
    else:
        rows: Sequence[Mapping] = data if isinstance(data, list) else list(data or [])
        for f in schema:
            name = f.name
            arrays.append(_fast_array([r.get(name) for r in rows], f.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def _replace(tmp: Path, path: Path) -> None:
    try:
        os.replace(tmp, path)
    except PermissionError:  # Windows: target open by a reader; best effort
        path.unlink(missing_ok=True)
        os.replace(tmp, path)


def write_parquet(data, path: str | os.PathLike, schema: pa.Schema | str | None = None) -> int:
    """Write `data` with an explicit schema (default: SCHEMAS[<file stem>]). Returns the row count."""
    path = Path(path)
    if schema is None:
        schema = SCHEMAS[path.stem]
    table = to_arrow_table(data, schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    _replace(tmp, path)
    return table.num_rows


class BatchWriter:
    """Streams columnar batches into one Parquet file (used for log_lines, which can be millions of rows)."""

    def __init__(self, path: str | os.PathLike, schema: pa.Schema | str, batch_rows: int = 200_000):
        self.path = Path(path)
        self.schema = SCHEMAS[schema] if isinstance(schema, str) else schema
        self.batch_rows = batch_rows
        self.tmp = self.path.with_name(self.path.name + ".tmp")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.writer = pq.ParquetWriter(self.tmp, self.schema, compression="zstd")
        self.names = [f.name for f in self.schema]
        self._get = operator.itemgetter(*self.names)
        self.buf: list[tuple] = []
        self.rows = 0

    def add(self, row: Mapping) -> None:
        try:
            self.buf.append(self._get(row))
        except KeyError:
            self.buf.append(tuple(row.get(n) for n in self.names))
        if len(self.buf) >= self.batch_rows:
            self.flush()

    def flush(self) -> None:
        n = len(self.buf)
        if n:
            cols = {name: list(col) for name, col in zip(self.names, zip(*self.buf))}
            self.buf = []
            self.writer.write_table(to_arrow_table(cols, self.schema))
            self.rows += n

    def close(self) -> int:
        self.flush()
        self.writer.close()
        _replace(self.tmp, self.path)
        return self.rows

    def abort(self) -> None:
        try:
            self.writer.close()
        finally:
            self.tmp.unlink(missing_ok=True)


def duckdb_schema_name(cluster_id: str) -> str:
    return "c_" + re.sub(r"[^0-9A-Za-z_]", "_", cluster_id)


def load_duckdb(output_root: str | os.PathLike, cluster_id: str, db_path: str | os.PathLike | None = None) -> str:
    """Create/replace one table per dataset in schema c_<cluster_id> of output/insights.duckdb."""
    import duckdb

    root = Path(output_root)
    db = Path(db_path) if db_path else root / "insights.duckdb"
    schema = duckdb_schema_name(cluster_id)
    con = duckdb.connect(str(db))
    try:
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        for name in SCHEMAS:
            p = root / cluster_id / f"{name}.parquet"
            if p.exists():
                con.execute(f'CREATE OR REPLACE TABLE "{schema}"."{name}" AS SELECT * FROM read_parquet(?)',
                            [str(p)])
    finally:
        con.close()
    return str(db)
