"""DuckDB helpers that read ``output/<cluster_id>/<dataset>.parquet`` for the HTTP API.

All SQL built here is safe by construction:

* dataset names are whitelisted against :data:`DATASETS`;
* column names are whitelisted against the Parquet schema of the file being read and quoted;
* every user supplied value is passed as a bound parameter.

Every public function returns plain Python values (dicts / lists / ints / floats / str / None) that are
JSON-serializable: timestamps become epoch milliseconds, NaN/NaT become ``None``, numpy scalars become
Python scalars and lists stay lists.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import difflib
import json
import math
import re
import threading
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from ..parsing.plans import jdbc_source, name_paths_in_text, name_tables, table_names
from ..util import is_null

# ---------------------------------------------------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------------------------------------------------

try:  # single source of truth: the Parquet schemas (16 contract datasets + the Revision 2/3 ones)
    from ..schemas import DATASETS as _SCHEMA_DATASETS

    DATASETS: tuple[str, ...] = tuple(_SCHEMA_DATASETS)
except Exception:  # noqa: BLE001  pragma: no cover
    DATASETS = (
        "files", "apps", "log_lines", "log_signals", "log_errors", "tasks", "stages", "spark_jobs", "sql_queries",
        "executors", "findings", "timeline", "query_profile", "stage_executor_profile", "executor_profile",
        "run_story", "event_counts", "file_lines", "gc_events", "connect_operations", "cluster_info", "task_retries",
        "spill_shuffle_timeline",
    )

#: Main timestamp column per dataset (used by ``ts_from`` / ``ts_to``).
TS_COLUMN: dict[str, str] = {
    "log_lines": "ts",
    "log_signals": "ts",
    "log_errors": "ts",
    "findings": "ts",
    "run_story": "ts",
    "tasks": "launch_time",
    "stages": "start_time",
    "sql_queries": "start_time",
    "spark_jobs": "start_time",
    "query_profile": "start_time",
    "executors": "added_time",
    "executor_profile": "added_time",
    "timeline": "minute",
    "apps": "start_time",
    "gc_events": "ts",
    "connect_operations": "start_time",
    "task_retries": "first_failure_time",
    "files": "modified",
    "spill_shuffle_timeline": "minute",
}
try:  # keep in step with schemas.MAIN_TS
    from ..schemas import MAIN_TS as _MAIN_TS

    TS_COLUMN.update({k: v for k, v in _MAIN_TS.items() if k not in TS_COLUMN})
except Exception:  # noqa: BLE001  pragma: no cover
    pass

#: Default deterministic ordering per dataset (so that limit/offset pagination is stable).
DEFAULT_ORDER: dict[str, tuple[str, ...]] = {
    "files": ("folder", "path"),
    "apps": ("start_time", "spark_context_id"),
    "log_lines": ("seq",),
    "log_signals": ("seq",),
    "log_errors": ("seq",),
    "tasks": ("spark_context_id", "launch_time", "task_id", "task_attempt"),
    "stages": ("spark_context_id", "stage_id", "stage_attempt"),
    "spark_jobs": ("spark_context_id", "spark_job_id"),
    "sql_queries": ("spark_context_id", "sql_execution_id"),
    "executors": ("spark_context_id", "added_time", "executor_id"),
    "findings": ("finding_id",),
    "timeline": ("minute", "signal"),
    "query_profile": ("spark_context_id", "sql_execution_id"),
    "stage_executor_profile": ("spark_context_id", "stage_id", "stage_attempt", "executor_id"),
    "executor_profile": ("spark_context_id", "added_time", "executor_id"),
    "run_story": ("story_seq",),
    "event_counts": ("spark_context_id", "event_type"),
    "file_lines": ("folder", "file_path"),
    "gc_events": ("seq",),
    "connect_operations": ("spark_context_id", "start_time", "operation_id"),
    "sql_plan_nodes": ("spark_context_id", "sql_execution_id", "node_id"),
    "incidents": ("incident_rank", "finding_id"),
    "cluster_info": ("spark_context_id",),
    "task_retries": ("spark_context_id", "stage_id", "stage_attempt", "task_index"),
    "spill_shuffle_timeline": ("spark_context_id", "minute", "executor_id", "stage_id", "stage_attempt"),
    "hotspots": ("spark_context_id", "kind", "ts_start"),
    "runs": ("start_time", "run_key"),
    "executor_busy": ("spark_context_id", "executor_id", "busy_start"),
}

#: Query-string parameters of the generic datasets endpoint that are NOT column filters.
RESERVED_PARAMS = frozenset({"limit", "offset", "sort", "desc", "q", "ts_from", "ts_to", "columns", "run"})

#: Value of an equality filter that means "IS NULL".
NULL_TOKEN = "__null__"

DEFAULT_LIMIT = 200
MAX_LIMIT = 5000
MAX_TASK_DURATIONS = 5000
MAX_TIMELINE_TASKS = 3000  # stage task timeline: every failed task, the longest, then an even sample by start time
FACET_LIMIT = 200
BIG_TEXT_LIMIT = 300  # truncation for "no big text" in the hierarchy

SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2, "info": 3}

_CLUSTER_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-]*$")
_EXPR_ID_RE = re.compile(r"#\d+")
_FAR_FUTURE = "TIMESTAMP '9999-12-31 00:00:00'"


# ---------------------------------------------------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------------------------------------------------


class ApiError(Exception):
    status_code = 400

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


class NotFound(ApiError):
    status_code = 404


class BadRequest(ApiError):
    status_code = 400


# ---------------------------------------------------------------------------------------------------------------------
# JSON cleaning
# ---------------------------------------------------------------------------------------------------------------------

_EPOCH = _dt.datetime(1970, 1, 1)


def _dt_to_ms(value: _dt.datetime) -> int:
    if value.tzinfo is not None:
        value = value.astimezone(_dt.timezone.utc).replace(tzinfo=None)
    delta = value - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1000 + delta.microseconds // 1000


def clean(value: Any) -> Any:
    """Recursively convert a value into something ``json.dumps(allow_nan=False)`` accepts."""
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [clean(v) for v in value]
    if isinstance(value, _dt.datetime):
        return _dt_to_ms(value)
    if isinstance(value, _dt.date):
        return _dt_to_ms(_dt.datetime(value.year, value.month, value.day))
    if isinstance(value, _dt.timedelta):
        return int(value.total_seconds() * 1000)
    if isinstance(value, decimal.Decimal):
        return clean(float(value))
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    # numpy / pandas scalars & arrays (imported lazily; both are installed but keep this module light)
    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            return [clean(v) for v in value.tolist()]
        if isinstance(value, np.generic):
            return clean(value.item())
    except ImportError:  # pragma: no cover
        pass
    try:
        import pandas as pd

        if value is pd.NaT:
            return None
        if isinstance(value, pd.Timestamp):
            return None if pd.isna(value) else _dt_to_ms(value.to_pydatetime())
        if isinstance(value, pd.Timedelta):
            return None if pd.isna(value) else int(value.total_seconds() * 1000)
    except ImportError:  # pragma: no cover
        pass
    if hasattr(value, "item"):
        try:
            return clean(value.item())
        except Exception:  # noqa: BLE001
            pass
    return str(value)


def arrow_to_rows(table: pa.Table) -> list[dict[str, Any]]:
    """Arrow table -> list of JSON-safe dicts. Timestamp columns become epoch ms ints."""
    arrays = []
    names = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        t = field.type
        if pa.types.is_timestamp(t):
            if t.tz is not None:
                col = pc.cast(col, pa.timestamp(t.unit, tz="UTC"))
            col = pc.cast(col, pa.timestamp("ms", tz=t.tz), safe=False).cast(pa.int64())
        elif pa.types.is_duration(t):
            col = pc.cast(col, pa.duration("ms"), safe=False).cast(pa.int64())
        arrays.append(col)
        names.append(field.name)
    converted = pa.Table.from_arrays(arrays, names=names) if arrays else table
    return [clean(r) for r in converted.to_pylist()]


# ---------------------------------------------------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------------------------------------------------


def qi(name: str) -> str:
    """Quote an identifier (only ever called on whitelisted names)."""
    return '"' + name.replace('"', '""') + '"'


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    v = value.strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    raise BadRequest(f"invalid boolean value: {value!r}")


def _parse_int(name: str, value: Any, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            f = float(value)
        except (TypeError, ValueError):
            raise BadRequest(f"invalid integer for {name}: {value!r}") from None
        if math.isnan(f) or math.isinf(f):
            raise BadRequest(f"invalid integer for {name}: {value!r}")
        return int(f)


def _is_list_type(t: str) -> bool:
    return t.endswith("[]")


def _is_ts_type(t: str) -> bool:
    return t.startswith("TIMESTAMP")


def _is_string_type(t: str) -> bool:
    return t == "VARCHAR"


def _job_status(result: Any, end_time: Any) -> str:
    if result == "JobSucceeded":
        return "succeeded"
    if result == "JobFailed":
        return "failed"
    if result == "JobReplanned":
        return "replanned"
    if result:
        return str(result).lower()
    return "incomplete"


# ---------------------------------------------------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------------------------------------------------


class Store:
    """Read-only access to the datasets of every built cluster under ``output_root``."""

    def __init__(self, output_root: Path):
        self.output_root = Path(output_root)
        self._schema_cache: dict[tuple[str, int, int], dict[str, str]] = {}
        self._names_cache: dict[tuple[str, int, int], dict[str, str]] = {}
        self._history: tuple[tuple, dict] | None = None
        self._lock = threading.Lock()

    # -- paths ----------------------------------------------------------------------------------------------------

    def cluster_dir(self, cid: str) -> Path:
        if not cid or not _CLUSTER_ID_RE.match(cid) or ".." in cid:
            raise NotFound(f"unknown cluster: {cid!r}")
        d = self.output_root / cid
        if not d.is_dir():
            raise NotFound(f"unknown cluster: {cid!r} (no {d.as_posix()})")
        return d

    def dataset_path(self, cid: str, name: str, *, required: bool = True) -> Path | None:
        if name not in DATASETS:
            raise NotFound(f"unknown dataset: {name!r}")
        p = self.cluster_dir(cid) / f"{name}.parquet"
        if not p.is_file():
            if required:
                raise NotFound(f"dataset {name!r} not built for cluster {cid!r}")
            return None
        return p

    def src(self, path: Path) -> str:
        return f"read_parquet({_sql_str(path.resolve().as_posix())})"

    # -- duckdb ---------------------------------------------------------------------------------------------------

    @staticmethod
    def connect() -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(database=":memory:")
        try:
            con.execute("SET TimeZone='UTC'")
        except Exception:  # noqa: BLE001  (ICU extension might be unavailable)
            pass
        return con

    def schema(self, con: duckdb.DuckDBPyConnection, path: Path) -> dict[str, str]:
        """Ordered mapping column -> DuckDB type name of a Parquet file (cached by path+mtime+size)."""
        st = path.stat()
        key = (str(path), st.st_mtime_ns, st.st_size)
        with self._lock:
            hit = self._schema_cache.get(key)
        if hit is not None:
            return hit
        rows = con.execute(f"DESCRIBE SELECT * FROM {self.src(path)}").fetchall()
        sch = {r[0]: str(r[1]) for r in rows}
        with self._lock:
            self._schema_cache[key] = sch
        return sch

    def table_names(self, con: duckdb.DuckDBPyConnection, cid: str) -> dict[str, str]:
        """{table id: catalog.schema.table} from the cluster's plans (cached by file): names the tables that queries
        wrote or read by storage path only. Outputs analyzed before names were kept get them here."""
        path = self.dataset_path(cid, "sql_queries", required=False)
        if path is None:
            return {}
        st = path.stat()
        key = (str(path), st.st_mtime_ns, st.st_size)
        with self._lock:
            hit = self._names_cache.get(key)
        if hit is not None:
            return hit
        sch = self.schema(con, path)
        plans = [c for c in ("final_plan", "initial_plan") if c in sch]
        names: dict[str, str] = {}
        if plans:
            cond = " OR ".join(f"{qi(c)} LIKE '%tables/%'" for c in plans)
            rows = con.execute(f"SELECT {', '.join(qi(c) for c in plans)} FROM {self.src(path)} WHERE {cond}").fetchall()
            names = table_names(p for r in rows for p in r)
            # the JDBC source each query read, keyed "jdbc|<ctx>|<query id>" (a key no table id can have)
            if "spark_context_id" in sch and "sql_execution_id" in sch:
                cond = " OR ".join(f"{qi(c)} LIKE '%JDBCRelation%'" for c in plans)
                for r in con.execute(f"SELECT spark_context_id, sql_execution_id, {', '.join(qi(c) for c in plans)} "
                                     f"FROM {self.src(path)} WHERE {cond}").fetchall():
                    j = next((x for x in (jdbc_source(p) for p in r[2:]) if x), None)
                    if j and j["source"]:
                        names[f"jdbc|{r[0]}|{r[1]}"] = "jdbc:" + j["source"]
        with self._lock:
            self._names_cache[key] = names
        return names

    def history(self, con: duckdb.DuckDBPyConnection) -> dict[tuple[str, str], list[tuple[int, int, str]]]:
        """Every analyzed run under the output folder by what it ran: (program, subject) -> [(start, duration, cluster)],
        in start order. Gives "usual" a baseline outside one batch: the same task on the same table, on earlier days or
        other job clusters. Cached while no runs file changed."""
        files = sorted(self.output_root.glob("*/runs.parquet"))
        stamp = tuple((str(f), f.stat().st_mtime_ns) for f in files)
        with self._lock:
            if self._history and self._history[0] == stamp:
                return self._history[1]
        out: dict[tuple[str, str], list[tuple[int, int, str]]] = {}
        for f in files:
            try:
                sch = self.schema(con, f)
                if not {"program", "subject", "start_time", "duration_ms"} <= set(sch):
                    continue
                for r in self.rows(con, f"SELECT program, subject, start_time, duration_ms FROM {self.src(f)} "
                                        "WHERE program IS NOT NULL AND subject IS NOT NULL AND duration_ms IS NOT NULL "
                                        "AND status = 'succeeded'" if "status" in sch else
                                   f"SELECT program, subject, start_time, duration_ms FROM {self.src(f)} "
                                   "WHERE program IS NOT NULL AND subject IS NOT NULL AND duration_ms IS NOT NULL"):
                    t = _as_ms(r["start_time"])
                    if t is not None:
                        out.setdefault((r["program"].lower(), r["subject"].lower()), []).append((t, int(r["duration_ms"]), f.parent.name))
            except Exception:  # noqa: BLE001  (one unreadable output never breaks the others)
                continue
        for v in out.values():
            v.sort()
        with self._lock:
            self._history = (stamp, out)
        return out

    def with_history(self, con: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> None:
        """"Usual" from the same task on the same table in earlier runs (this or other analyzed clusters): the median
        of the last 10 that succeeded, when there are 2 or more. Replaces the batch's usual (which mixes tables)."""
        hist = self.history(con)
        if not hist:
            return
        for r in rows:
            p, sj, t0, d = r.get("program"), r.get("subject"), _as_ms(r.get("start_time")), r.get("duration_ms")
            if not p or not sj or t0 is None:
                continue
            past = [x for x in hist.get((p.lower(), sj.lower()), []) if x[0] < t0 - 60_000][-10:]
            if len(past) < 2:
                continue
            durs = sorted(x[1] for x in past)
            typ = durs[(len(durs) - 1) // 2]
            r["typical_duration_ms"] = typ
            r["vs_typical"] = round(d / typ, 2) if d is not None and typ else None
            r["usual_runs"] = len(past)
            r["usual_from"] = "history"

    @staticmethod
    def rows(con: duckdb.DuckDBPyConnection, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        return arrow_to_rows(_fetch_arrow(con.execute(sql, list(params))))

    def select(
        self,
        con: duckdb.DuckDBPyConnection,
        cid: str,
        name: str,
        where: str = "TRUE",
        params: Sequence[Any] = (),
        *,
        exclude: Iterable[str] = (),
        order: str | None = None,
        limit: int | None = None,
        truncate: Mapping[str, int] | None = None,
    ) -> list[dict[str, Any]]:
        """Run ``SELECT <cols> FROM <dataset> WHERE <where>``; returns [] if the dataset file is missing."""
        path = self.dataset_path(cid, name, required=False)
        if path is None:
            return []
        sch = self.schema(con, path)
        excl = set(exclude)
        cols = []
        for c in sch:
            if c in excl:
                continue
            if truncate and c in truncate and _is_string_type(sch[c]):
                cols.append(f"substr({qi(c)}, 1, {int(truncate[c])}) AS {qi(c)}")
            else:
                cols.append(qi(c))
        # Spark Connect and some notebooks name every stage "start at <unknown>:0": name it by what it does instead
        named = name == "stages" and "stage_name" in sch and "stage_name" not in excl and "rdd_scopes" in sch
        if named:
            cols.append(f"{qi('rdd_scopes')} AS __scopes")
        sql = f"SELECT {', '.join(cols) or '*'} FROM {self.src(path)} WHERE {where}"
        if order:
            sql += f" ORDER BY {order}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        out = self.rows(con, sql, params)
        if name == "runs" and out and "program" in sch:
            self.with_history(con, out)
        # storage paths of tables the plans name -> the names
        lists = [c for c in ("tables_read", "tables_written") if c in sch and c not in excl]
        texts = [c for c in _NAMED_TEXT.get(name, ()) if c in sch and c not in excl]
        if out and (lists or texts):
            tn = self.table_names(con, cid)
            if tn:
                for r in out:
                    for c in lists:
                        r[c] = name_tables(r.get(c), tn)
                    src = tn.get(f"jdbc|{r.get('spark_context_id')}|{r.get('sql_execution_id')}") if name == "sql_queries" else None
                    if src and "tables_read" in lists and src not in (r.get("tables_read") or []):
                        r["tables_read"] = [*(r.get("tables_read") or []), src]
                    for c in texts:
                        r[c] = name_paths_in_text(r.get(c), tn)
        if named:
            for r in out:
                scopes = r.pop("__scopes", None)
                if not r.get("stage_name") or "<unknown>" in r["stage_name"]:
                    r["stage_name"] = stage_what(scopes, r) or r.get("stage_name")
        return out

    def has_columns(self, con, cid: str, name: str, *cols: str) -> bool:
        path = self.dataset_path(cid, name, required=False)
        if path is None:
            return False
        sch = self.schema(con, path)
        return all(c in sch for c in cols)


# free-text columns that can carry a table's storage path
_NAMED_TEXT = {"findings": ("evidence", "entity"), "run_story": ("detail",), "stages": ("rdd_names",)}


def stage_what(scopes, s: Mapping[str, Any]) -> str | None:
    """What a stage does, from the operators Spark ran in it and its bytes: "scan my_catalog.sales.orders, 408 GB",
    "sort-merge join, spilled 67 GB", "write 45 GB"."""
    sc = list(scopes or [])
    bits: list[str] = []
    for x in sc:
        m = re.match(r"Scan (?:parquet|delta|orc|csv|json|text) (.+)", x)
        if m:
            bits.append(f"scan {m.group(1)}")
        elif "mergeMaterializedSource" in x:
            bits.append("read the MERGE source")
        elif x.startswith("Scan JDBCRelation") or "JDBC" in x:
            bits.append("read from the database")
        elif x.startswith("Scan ExistingRDD"):
            bits.append("read the stream batch")
        elif x.startswith("InMemoryTableScan"):
            bits.append("read the DataFrame cache")
        elif x.startswith("InMemoryRelation") or x.startswith("TableCacheQueryStage"):
            bits.append("build a DataFrame cache")
        elif x.startswith("Generate"):
            bits.append("explode")
    joins = {"SortMergeJoin": "sort-merge join", "BroadcastHashJoin": "broadcast join", "ShuffledHashJoin": "hash join",
             "BroadcastNestedLoopJoin": "nested-loop join", "CartesianProduct": "cartesian join"}
    for k, v in joins.items():
        if k in sc:
            bits.append(v)
            break
    # the last aggregate of a count says little next to the cache or scan it ran on
    if any("Aggregate" in x for x in sc) and not any(b.endswith("DataFrame cache") for b in bits):
        bits.append("aggregate")
    if "Window" in sc:
        bits.append("window")
    if any(x in ("WriteFiles", "Execute WriteIntoDeltaCommand") or x.startswith("Write") for x in sc):
        bits.append("write")
    if not bits and sc:
        bits.append("shuffle" if sc[0] == "Exchange" or "AQEShuffleRead" in sc else sc[0])
    if not bits:
        return None
    gb = lambda v: _fmt_bytes(v)  # noqa: E731
    big = max((("read", s.get("input_bytes") or 0), ("shuffle read", s.get("shuffle_read") or 0),
               ("wrote", s.get("output_bytes") or 0)), key=lambda kv: kv[1])
    text = ", ".join(dict.fromkeys(bits))
    if big[1] >= 1 << 20 and big[0] == "wrote" and "write" in bits:
        text = text.replace("write", f"write {gb(big[1])}")
    elif big[1] >= 1 << 20:
        text += f", {big[0]} {gb(big[1])}"
    if (s.get("disk_spill") or 0) >= 1 << 30:
        text += f", spilled {gb(s['disk_spill'])}"
    return text


def _fetch_arrow(rel) -> pa.Table:
    # duckdb >= 1.1 : fetch_arrow_table ; older: arrow()
    if hasattr(rel, "fetch_arrow_table"):
        return rel.fetch_arrow_table()
    return rel.arrow()  # pragma: no cover


# ---------------------------------------------------------------------------------------------------------------------
# Clusters / summary
# ---------------------------------------------------------------------------------------------------------------------


def read_summary(store: Store, cid: str) -> dict[str, Any]:
    d = store.cluster_dir(cid)
    p = d / "summary.json"
    if not p.is_file():
        raise NotFound(f"summary.json not found for cluster {cid!r}")
    try:
        return clean(json.loads(p.read_text(encoding="utf-8")))
    except json.JSONDecodeError as e:
        raise ApiError(f"summary.json of {cid!r} is not valid JSON: {e}") from e


_CLUSTER_KEYS = (
    "cluster_id",
    "built_at",
    "status",
    "start_time",
    "end_time",
    "duration_ms",
    "counts",
    "findings_by_severity",
    "empty_reason",
)


def _has_files(d: Path) -> bool:
    try:
        return d.is_dir() and any(f.is_file() for f in d.rglob("*"))
    except OSError:
        return False


def list_clusters(store: Store) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    root = store.output_root
    if not root.is_dir():
        return out
    for d in root.iterdir():
        p = d / "summary.json"
        if not d.is_dir() or not p.is_file():
            continue
        try:
            s = json.loads(p.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001  (skip unreadable summaries, never fail the list)
            continue
        row = {k: s.get(k) for k in _CLUSTER_KEYS}
        row["cluster_id"] = row["cluster_id"] or d.name
        # the raw logs it was built from (a local folder or the download cache): can it be analyzed again?
        raw = s.get("input_dir")
        row["raw_available"] = bool(raw) and _has_files(Path(raw))
        row["_mtime"] = p.stat().st_mtime
        out.append(row)
    out.sort(key=lambda r: (str(r.get("built_at") or ""), r["_mtime"]), reverse=True)
    for r in out:
        r.pop("_mtime", None)
    return clean(out)


# ---------------------------------------------------------------------------------------------------------------------
# Generic datasets endpoint
# ---------------------------------------------------------------------------------------------------------------------


def _filter_clause(col: str, typ: str, values: list[str], params: list[Any]) -> str:
    """Equality / IN filter for one column (values are strings from the query string)."""
    want_null = any(v == NULL_TOKEN for v in values)
    vals = [v for v in values if v != NULL_TOKEN]
    parts: list[str] = []
    c = qi(col)
    if vals:
        if _is_list_type(typ):
            elem = typ[:-2]
            sub = []
            for v in vals:
                sub.append(f"list_contains({c}, TRY_CAST(? AS {elem}))")
                params.append(v)
            parts.append("(" + " OR ".join(sub) + ")")
        elif _is_ts_type(typ):
            ph = []
            for v in vals:
                if re.fullmatch(r"-?\d+", v.strip()):
                    ph.append("epoch_ms(CAST(? AS BIGINT))")
                else:
                    ph.append("TRY_CAST(? AS TIMESTAMP)")
                params.append(v.strip())
            parts.append(f"{c} IN ({', '.join(ph)})")
        elif _is_string_type(typ):
            parts.append(f"{c} IN ({', '.join('?' for _ in vals)})")
            params.extend(vals)
        else:
            # numeric / boolean etc. ; TRY_CAST so that a bad value simply matches nothing
            parts.append(f"{c} IN ({', '.join(f'TRY_CAST(? AS {typ})' for _ in vals)})")
            params.extend(v.strip() for v in vals)
    if want_null:
        parts.append(f"{c} IS NULL")
    return "(" + " OR ".join(parts) + ")"


def table_query(
    store: Store,
    cid: str,
    name: str,
    query_items: Iterable[tuple[str, str]],
) -> dict[str, Any]:
    """Generic table endpoint. ``query_items`` = the raw (multi) query-string items."""
    path = store.dataset_path(cid, name)
    assert path is not None
    single: dict[str, str] = {}
    filters: dict[str, list[str]] = {}
    for k, v in query_items:
        if k in RESERVED_PARAMS:
            single[k] = v
        else:
            filters.setdefault(k, []).append(v)

    limit = _parse_int("limit", single.get("limit"), DEFAULT_LIMIT)
    limit = max(0, min(int(limit), MAX_LIMIT))
    offset = max(0, int(_parse_int("offset", single.get("offset"), 0)))
    desc = _parse_bool(single.get("desc"), False)

    with store.connect() as con:
        sch = store.schema(con, path)

        # columns
        cols_param = (single.get("columns") or "").strip()
        if cols_param:
            cols = [c.strip() for c in cols_param.split(",") if c.strip()]
            unknown = [c for c in cols if c not in sch]
            if unknown:
                raise BadRequest(f"unknown column(s) for {name}: {', '.join(unknown)}")
        else:
            cols = list(sch)

        where: list[str] = []
        params: list[Any] = []

        # equality filters (unknown parameter names are ignored so that UI-only params do not break the API)
        for col, values in filters.items():
            if col not in sch:
                continue
            where.append(_filter_clause(col, sch[col], values, params))

        # text search over string columns (and list<string> columns)
        q = single.get("q")
        if q:
            needle = q.lower()
            ors = []
            for c, t in sch.items():
                if _is_string_type(t):
                    ors.append(f"strpos(lower({qi(c)}), ?) > 0")
                    params.append(needle)
                elif t == "VARCHAR[]":
                    ors.append(f"strpos(lower(array_to_string({qi(c)}, chr(10))), ?) > 0")
                    params.append(needle)
            where.append("(" + (" OR ".join(ors) or "FALSE") + ")")

        # run scope (Revision 6): run-scoped datasets filter on run_key; executor-level datasets on affected_runs;
        # other shared datasets (logs, GC, executors, ...) are cut to the run's time window
        run = (single.get("run") or "").strip()
        if run:
            win = run_window(store, con, cid, run) if name in WITH_SHARED_ROWS or name in SPAN_COLUMNS else None
            tcol = TS_COLUMN.get(name)
            if "run_key" in sch and name in WITH_SHARED_ROWS and win and tcol in sch:
                # the run's own rows, and the cluster's shared rows (executors, logs) in the run's time; a row tied to
                # a job, stage or query is never shared, even without a run (a query that ran no Spark job)
                own = "".join(f" AND {qi(c)} IS NULL" for c in ("spark_job_id", "stage_id", "sql_execution_id") if c in sch)
                where.append(f"(run_key = ? OR (run_key IS NULL{own} AND {qi(tcol)} BETWEEN epoch_ms(CAST(? AS BIGINT)) "
                             f"AND epoch_ms(CAST(? AS BIGINT))))")
                params.extend([run, *win])
            elif "run_key" in sch:
                where.append("run_key = ?")
                params.append(run)
            elif name in SPAN_COLUMNS and win and all(c in sch for c in SPAN_COLUMNS[name]):
                a, b = SPAN_COLUMNS[name]  # anything that overlaps the run, not only what started in it
                where.append(f"(({qi(b)} IS NULL OR {qi(b)} >= epoch_ms(CAST(? AS BIGINT))) "
                             f"AND ({qi(a)} IS NULL OR {qi(a)} <= epoch_ms(CAST(? AS BIGINT))))")
                params.extend(win)
            elif "affected_runs" in sch:
                where.append("list_contains(affected_runs, ?)")
                params.append(run)
            else:
                win = run_window(store, con, cid, run)
                tcol = TS_COLUMN.get(name)
                if win and tcol and tcol in sch:
                    where.append(f"({qi(tcol)} IS NULL OR {qi(tcol)} BETWEEN epoch_ms(CAST(? AS BIGINT)) "
                                 f"AND epoch_ms(CAST(? AS BIGINT)))")
                    params.extend(win)

        # time range
        ts_col = TS_COLUMN.get(name)
        ts_from = _parse_int("ts_from", single.get("ts_from"))
        ts_to = _parse_int("ts_to", single.get("ts_to"))
        if ts_col and ts_col in sch and (ts_from is not None or ts_to is not None):
            if ts_from is not None:
                where.append(f"{qi(ts_col)} >= epoch_ms(CAST(? AS BIGINT))")
                params.append(ts_from)
            if ts_to is not None:
                where.append(f"{qi(ts_col)} <= epoch_ms(CAST(? AS BIGINT))")
                params.append(ts_to)

        where_sql = " AND ".join(where) if where else "TRUE"

        # order
        sort = (single.get("sort") or "").strip()
        if sort:
            if sort not in sch:
                raise BadRequest(f"unknown sort column for {name}: {sort!r}")
            order_cols = [sort] + [c for c in DEFAULT_ORDER.get(name, ()) if c in sch and c != sort]
            direction = "DESC" if desc else "ASC"
            first = [f"{qi(sort)} {direction} NULLS LAST"]
            if _is_string_type(sch[sort]):
                # ids kept as text (executor "2", "10", "driver"): numbers in numeric order, then the rest as text
                first = [f"TRY_CAST({qi(sort)} AS BIGINT) {direction} NULLS LAST"] + first
            order = ", ".join(first + [f"{qi(c)} ASC NULLS LAST" for c in order_cols[1:]])
        else:
            order_cols = [c for c in DEFAULT_ORDER.get(name, ()) if c in sch]
            direction = "DESC" if desc else "ASC"
            order = ", ".join(f"{qi(c)} {direction} NULLS LAST" for c in order_cols) if order_cols else ""

        src = store.src(path)
        total = con.execute(f"SELECT count(*) FROM {src} WHERE {where_sql}", params).fetchone()[0]
        sql = f"SELECT {', '.join(qi(c) for c in cols)} FROM {src} WHERE {where_sql}"
        if order:
            sql += f" ORDER BY {order}"
        sql += f" LIMIT {limit} OFFSET {offset}"
        rows = store.rows(con, sql, params)

    return {"columns": cols, "rows": rows, "total": int(total), "limit": limit, "offset": offset}


# run scope: datasets that mix a run's own rows with rows shared by the cluster, and datasets of time spans
WITH_SHARED_ROWS = frozenset({"run_story", "findings"})
SPAN_COLUMNS = {"executor_busy": ("busy_start", "busy_end"), "executors": ("added_time", "removed_time")}


def run_window(store: Store, con, cid: str, run: str) -> tuple[int, int] | None:
    """[start, end] (epoch ms) of a run, for cutting shared datasets to the run's time."""
    rows = store.select(con, cid, "runs", "run_key = ?", [run], limit=1)
    if not rows:
        return None
    r = rows[0]
    if r.get("start_time") is None or r.get("end_time") is None:
        return None
    return int(r["start_time"]), int(r["end_time"])


def runs(store: Store, cid: str) -> dict[str, Any]:
    store.cluster_dir(cid)
    with store.connect() as con:
        rows = store.select(con, cid, "runs", "TRUE", (), order="start_time NULLS LAST, run_key")
        info = (store.select(con, cid, "cluster_info", limit=1) or [{}])[0]
        # stage attempts that failed, per run (retried or not): the runs table shows them next to failed tasks
        failed_stages: dict[str, int] = {}
        spath = store.dataset_path(cid, "stages", required=False)
        if spath is not None and "run_key" in store.schema(con, spath):
            for r in store.rows(con, f"SELECT run_key, count(*) AS n FROM {store.src(spath)} "
                                     "WHERE status = 'failed' AND run_key IS NOT NULL GROUP BY 1"):
                failed_stages[r["run_key"]] = r["n"]
        # where each run's input came from: storage, or a DataFrame cache (outputs built before these columns: none)
        reads: dict[str, tuple] = {}
        if spath is not None and {"run_key", "storage_bytes", "df_cache_bytes"} <= set(store.schema(con, spath)):
            for r in store.rows(con, f"SELECT run_key, sum(storage_bytes) AS st, sum(df_cache_bytes) AS dc FROM {store.src(spath)} "
                                     "WHERE run_key IS NOT NULL AND status <> 'failed' GROUP BY 1"):
                reads[r["run_key"]] = (r["st"], r["dc"])
        times = _run_times(store, con, cid)
        # the biggest file read and the biggest shuffle read of one task in each run
        tmax: dict[str, tuple] = {}
        tpath = store.dataset_path(cid, "tasks", required=False)
        if tpath is not None and "run_key" in store.schema(con, tpath):
            for r in store.rows(con, f"SELECT run_key, max(input_bytes) AS mi, max(shuffle_read) AS ms FROM {store.src(tpath)} "
                                     "WHERE run_key IS NOT NULL GROUP BY 1"):
                tmax[r["run_key"]] = (r["mi"], r["ms"])
    for r in rows:
        r["failed_stages"] = failed_stages.get(r["run_key"], 0)
        st_, dc_ = reads.get(r["run_key"], (None, None))
        r["storage_read"] = None if st_ is None else int(st_)
        r["cache_read"] = None if dc_ is None else int(dc_)
        r["max_task_input"], r["max_task_shuffle"] = tmax.get(r["run_key"], (None, None))
        t = times.get(r["run_key"]) or {}
        r["waiting_ms"] = t.get("waiting_ms")
        r["running_ms"] = t.get("running_ms")
    # a job cluster runs one job run: its task runs belong to it even when the event log did not say so per run
    # (lets the run picker find them by the job run id Databricks shows)
    jr, top, jid = info.get("job_run_id"), info.get("parent_run_id"), info.get("databricks_job_id")
    ids = " ".join(str(x) for x in (jr, top) if x)
    for r in rows:
        if r.get("kind") == "task_run" and (not jid or r.get("databricks_job_id") in (None, jid)):
            if r.get("parent_run_id") is None and jr:
                r["parent_run_id"] = jr
            r["job_run_ids"] = ids or None  # the cluster's job run and the run above it, both searchable
    s = read_summary(store, cid).get("runs") or {}
    return {"runs": rows, "default_run": s.get("default_run"), "note": s.get("note"), "clock": times.get("__clock__")}


# ---------------------------------------------------------------------------------------------------------------------
# Facets
# ---------------------------------------------------------------------------------------------------------------------


def facets(store: Store, cid: str, name: str, column: str) -> list[dict[str, Any]]:
    path = store.dataset_path(cid, name)
    assert path is not None
    with store.connect() as con:
        sch = store.schema(con, path)
        if not column or column not in sch:
            raise BadRequest(f"unknown column for {name}: {column!r}")
        c = qi(column)
        src = store.src(path)
        if _is_list_type(sch[column]):
            sql = (
                f"SELECT v AS value, count(*) AS count FROM (SELECT unnest({c}) AS v FROM {src}) "
                f"GROUP BY 1 ORDER BY 2 DESC, 1 ASC NULLS LAST LIMIT {FACET_LIMIT}"
            )
        else:
            sql = f"SELECT {c} AS value, count(*) AS count FROM {src} GROUP BY 1 ORDER BY 2 DESC, 1 ASC NULLS LAST LIMIT {FACET_LIMIT}"
        return store.rows(con, sql)


# ---------------------------------------------------------------------------------------------------------------------
# Log context
# ---------------------------------------------------------------------------------------------------------------------


def log_context(store: Store, cid: str, file_path: str, seq: int, before: int = 20, after: int = 40) -> dict[str, Any]:
    if not file_path:
        raise BadRequest("file_path is required")
    before = max(0, min(int(before), MAX_LIMIT))
    after = max(0, min(int(after), MAX_LIMIT))
    path = store.dataset_path(cid, "log_lines")
    assert path is not None
    src = store.src(path)
    with store.connect() as con:
        sch = store.schema(con, path)
        cols = ", ".join(qi(c) for c in sch)
        sql = (
            f"SELECT * FROM ("
            f"  (SELECT {cols} FROM {src} WHERE file_path = ? AND seq < ? ORDER BY seq DESC LIMIT {before})"
            f"  UNION ALL"
            f"  (SELECT {cols} FROM {src} WHERE file_path = ? AND seq >= ? ORDER BY seq ASC LIMIT {after + 1})"
            f") ORDER BY seq"
        )
        rows = store.rows(con, sql, [file_path, int(seq), file_path, int(seq)])
    return {
        "columns": list(sch),
        "rows": rows,
        "total": len(rows),
        "limit": before + after + 1,
        "offset": 0,
        "file_path": file_path,
        "seq": int(seq),
    }


# ---------------------------------------------------------------------------------------------------------------------
# Errors / signals summaries
# ---------------------------------------------------------------------------------------------------------------------


ERR_TASK_SLACK_S = 2  # an executor error belongs to the tasks running on that executor within this many seconds
ERR_DRIVER_AFTER_S = 60  # a driver error belongs to the runs going at its time, or that ended this shortly before


def _error_hits_sql(store: Store, cid: str) -> str | None:
    """SQL of (file_path, seq, run_key, spark_context_id, stage_id, stage_attempt): where each logged error happened.
    An executor error hit the stage attempts that had a task running on that executor at its time; a driver error
    (or an executor error with no task then) hit the runs that were going at its time. None without log_errors."""
    epath = store.dataset_path(cid, "log_errors", required=False)
    if epath is None:
        return None
    tpath = store.dataset_path(cid, "tasks", required=False)
    rpath = store.dataset_path(cid, "runs", required=False)
    parts = []
    e = f"(SELECT file_path, seq, ts, nullif(executor_id, '') AS ex FROM {store.src(epath)} WHERE ts IS NOT NULL)"
    if tpath is not None:
        parts.append(f"""
            SELECT DISTINCT file_path, seq, run_key, spark_context_id, stage_id, stage_attempt FROM (
                -- when one of the tasks running then failed, the error is that task's: keep only the failed ones
                SELECT e.file_path, e.seq, t.run_key, t.spark_context_id, t.stage_id, t.stage_attempt, t.failed,
                       bool_or(t.failed) OVER (PARTITION BY e.file_path, e.seq) AS any_failed
                FROM {e} e JOIN {store.src(tpath)} t
                  ON t.executor_id = e.ex
                 AND e.ts BETWEEN t.launch_time - INTERVAL {ERR_TASK_SLACK_S} SECOND
                              AND t.finish_time + INTERVAL {ERR_TASK_SLACK_S} SECOND)
            WHERE failed OR NOT any_failed""")
    if rpath is not None:
        on_task = (f"""AND NOT EXISTS (SELECT 1 FROM {store.src(tpath)} t WHERE t.executor_id = e.ex
                       AND e.ts BETWEEN t.launch_time - INTERVAL {ERR_TASK_SLACK_S} SECOND
                                    AND t.finish_time + INTERVAL {ERR_TASK_SLACK_S} SECOND)"""
                   if tpath is not None else "")
        parts.append(f"""
            SELECT e.file_path, e.seq, r.run_key, NULL AS spark_context_id, NULL AS stage_id, NULL AS stage_attempt
            FROM {e} e JOIN {store.src(rpath)} r
              ON e.ts BETWEEN r.start_time AND r.end_time + INTERVAL {ERR_DRIVER_AFTER_S} SECOND
            WHERE (e.ex IS NULL {on_task})""")
    return " UNION ALL ".join(parts) if parts else None


def error_where(store: Store, cid: str) -> dict[str, dict]:
    """Per fingerprint: the runs and stage attempts its lines hit (lines counted once per run / stage)."""
    epath = store.dataset_path(cid, "log_errors", required=False)
    hits = _error_hits_sql(store, cid)
    if epath is None or hits is None:
        return {}
    spath = store.dataset_path(cid, "stages", required=False)
    q_of = (f"LEFT JOIN (SELECT spark_context_id, stage_id, stage_attempt, any_value(sql_execution_id) AS sql_execution_id "
            f"FROM {store.src(spath)} GROUP BY ALL) s USING (spark_context_id, stage_id, stage_attempt)"
            if spath is not None else "")
    sql = f"""
        WITH h AS ({hits}), x AS (
            SELECT e.fingerprint, h.* FROM h JOIN {store.src(epath)} e USING (file_path, seq))
        SELECT fingerprint, run_key, x.spark_context_id, x.stage_id, x.stage_attempt,
               {"s.sql_execution_id" if spath is not None else "NULL AS sql_execution_id"},
               count(DISTINCT (file_path, seq)) AS lines
        FROM x {q_of.replace("USING (spark_context_id, stage_id, stage_attempt)",
                             "ON s.spark_context_id = x.spark_context_id AND s.stage_id = x.stage_id AND s.stage_attempt = x.stage_attempt")}
        GROUP BY ALL ORDER BY lines DESC"""
    out: dict[str, dict] = {}
    with store.connect() as con:
        for r in store.rows(con, sql):
            g = out.setdefault(r["fingerprint"], {"runs": {}, "stages": []})
            if r.get("run_key"):
                g["runs"][r["run_key"]] = g["runs"].get(r["run_key"], 0) + r["lines"]
            if r.get("stage_id") is not None:
                g["stages"].append({k: r.get(k) for k in ("run_key", "spark_context_id", "stage_id", "stage_attempt",
                                                          "sql_execution_id", "lines")})
    for g in out.values():
        g["runs"] = [{"run_key": k, "lines": v} for k, v in sorted(g["runs"].items(), key=lambda kv: -kv[1])]
        g["stages"] = g["stages"][:10]
    return out


def error_groups(store: Store, cid: str, run: str | None = None) -> list[dict[str, Any]]:
    """Exception groups; with `run`, only the lines that hit that run (see _error_hits_sql), each group with the runs
    and stages it hit."""
    path = store.dataset_path(cid, "log_errors", required=False)
    if path is None:
        return []
    src = store.src(path)
    where = "TRUE"
    params: list = []
    if run:
        hits = _error_hits_sql(store, cid)
        if hits is None:
            return []
        where = f"(file_path, seq) IN (SELECT file_path, seq FROM ({hits}) WHERE run_key = ?)"
        params = [run]
    key = f"coalesce(ts, {_FAR_FUTURE}), seq"
    sql = f"""
        SELECT fingerprint,
               first(exception_class ORDER BY {key}) AS exception_class,
               count(*) AS occurrences,
               count(DISTINCT nullif(executor_id, '')) AS executors_affected,
               min(ts) AS first_seen,
               max(ts) AS last_seen,
               first(message ORDER BY {key}) AS sample_message,
               first(top_frames ORDER BY {key}) AS sample_stack,
               first(user_frame ORDER BY {key}) FILTER (WHERE user_frame IS NOT NULL) AS user_frame,
               list(DISTINCT source ORDER BY source) FILTER (WHERE source IS NOT NULL) AS sources,
               first(file_path ORDER BY {key}) AS sample_file_path,
               first(seq ORDER BY {key}) AS sample_seq
        FROM {src}
        WHERE {where}
        GROUP BY fingerprint
        ORDER BY occurrences DESC, first_seen ASC NULLS LAST, fingerprint
    """
    with store.connect() as con:
        rows = store.rows(con, sql, params)
    wh = error_where(store, cid)
    for r in rows:
        w = wh.get(r["fingerprint"]) or {"runs": [], "stages": []}
        r["hit_runs"] = [x for x in w["runs"] if not run or x["run_key"] == run]
        r["hit_stages"] = [x for x in w["stages"] if not run or x["run_key"] == run]
        if r.get("sources") is None:
            r["sources"] = []
        if r.get("sample_stack") is None:
            r["sample_stack"] = []
    return rows


def signal_groups(store: Store, cid: str) -> list[dict[str, Any]]:
    """Port of notebook ``summarize_signals`` (+ sample file path / seq for jump-to-log)."""
    path = store.dataset_path(cid, "log_signals", required=False)
    if path is None:
        return []
    src = store.src(path)
    key = f"coalesce(ts, {_FAR_FUTURE}), seq"
    sql = f"""
        SELECT signal, severity, fix,
               count(*) AS occurrences,
               count(DISTINCT nullif(executor_id, '')) AS executors_affected,
               min(ts) AS first_seen,
               max(ts) AS last_seen,
               first(line ORDER BY {key}) AS sample_line,
               first(file_path ORDER BY {key}) AS sample_file_path,
               first(seq ORDER BY {key}) AS sample_seq
        FROM {src}
        GROUP BY signal, severity, fix
        ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                 occurrences DESC, signal
    """
    with store.connect() as con:
        return store.rows(con, sql)


# ---------------------------------------------------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------------------------------------------------


def hierarchy(store: Store, cid: str) -> dict[str, Any]:
    store.cluster_dir(cid)
    with store.connect() as con:
        apps = store.select(con, cid, "apps", order="start_time NULLS LAST, spark_context_id")
        jobs = store.select(
            con, cid, "spark_jobs", order="spark_context_id, spark_job_id", truncate={"call_site": 1000}
        )
        stages = store.select(
            con,
            cid,
            "stages",
            order="spark_context_id, stage_id, stage_attempt",
            truncate={"failure_reason": BIG_TEXT_LIMIT, "job_description": BIG_TEXT_LIMIT, "stage_name": BIG_TEXT_LIMIT},
        )
        queries = store.select(con, cid, "query_profile", order="spark_context_id, sql_execution_id")

    app_by_ctx: dict[Any, dict[str, Any]] = {}
    out_apps: list[dict[str, Any]] = []
    for a in apps:
        ctx = a.get("spark_context_id")
        if ctx in app_by_ctx:  # several app rows for the same context: keep the first
            continue
        node = dict(a)
        node["jobs"] = []
        node["queries"] = []
        node["orphan_stages"] = []
        app_by_ctx[ctx] = node
        out_apps.append(node)

    def app_for(ctx: Any) -> dict[str, Any]:
        node = app_by_ctx.get(ctx)
        if node is None:  # context seen in events but no apps row: synthesize one
            node = {
                "cluster_id": cid,
                "spark_context_id": ctx,
                "app_id": None,
                "app_name": None,
                "spark_version": None,
                "user": None,
                "start_time": None,
                "end_time": None,
                "duration_ms": None,
                "jobs": [],
                "queries": [],
                "orphan_stages": [],
                "synthetic": True,
            }
            app_by_ctx[ctx] = node
            out_apps.append(node)
        return node

    job_index: dict[tuple[Any, Any], dict[str, Any]] = {}
    for j in jobs:
        node = dict(j)
        node["status"] = _job_status(j.get("result"), j.get("end_time"))
        node["stages"] = []
        app_for(j.get("spark_context_id"))["jobs"].append(node)
        job_index[(j.get("spark_context_id"), j.get("spark_job_id"))] = node

    for s in stages:
        ctx = s.get("spark_context_id")
        job = job_index.get((ctx, s.get("spark_job_id"))) if s.get("spark_job_id") is not None else None
        if job is not None:
            job["stages"].append(s)
        else:
            app_for(ctx)["orphan_stages"].append(s)

    for q in queries:
        app_for(q.get("spark_context_id"))["queries"].append(q)

    return {"apps": out_apps}


# ---------------------------------------------------------------------------------------------------------------------
# Stage detail
# ---------------------------------------------------------------------------------------------------------------------

_QUANTILES = (("p25", 0.25), ("p50", 0.5), ("p75", 0.75), ("p90", 0.9), ("p99", 0.99))
#: per-task columns summarized for the stage's task table (the "every other task" row)
TASK_STAT_COLS = ("task_ms", "gc_ms", "peak_mem", "mem_spill", "disk_spill", "input_bytes", "shuffle_read",
                  "shuffle_write", "output_bytes", "cpu_ms", "run_ms")


def _task_column_stats(con, src: str, sch: dict, where: str, params: list) -> dict[str, Any]:
    """col -> {min, p10, p50, p90, p95, max, sum, nonzero} over every task of the stage attempt (one scan)."""
    cols = [c for c in TASK_STAT_COLS if c in sch]
    if not cols:
        return {}
    parts = []
    for c in cols:
        q = qi(c)
        parts += [f"quantile_disc({q}, 0.5)", f"quantile_disc({q}, 0.95)", f"max({q})", f"sum({q})",
                  f"count(*) FILTER (WHERE {q} > 0)", f"min({q})", f"quantile_disc({q}, 0.1)",
                  f"quantile_disc({q}, 0.9)"]
    row = con.execute(f"SELECT {', '.join(parts)} FROM {src} WHERE {where}", params).fetchone()
    out: dict[str, Any] = {}
    n = 8
    for i, c in enumerate(cols):
        p50, p95, mx, sm, nz, mn, p10, p90 = row[i * n:(i + 1) * n]
        out[c] = {"p50": p50, "p95": p95, "max": mx, "sum": sm, "nonzero": int(nz or 0), "min": mn, "p10": p10,
                  "p90": p90}
    return clean(out)


TIMELINE_COLS = ("task_id", "task_index", "task_attempt", "executor_id", "launch_time", "task_ms", "failed",
                 "gc_ms", "peak_mem", "mem_spill", "disk_spill", "shuffle_read", "input_bytes")


def _task_timeline(con, src: str, sch: dict, where: str, params: list) -> tuple[list[dict], bool]:
    """Tasks of one stage attempt for the task timeline. Up to MAX_TIMELINE_TASKS: every failed task first, then the
    longest, then an even sample by start time, so the stragglers and failures are always drawn."""
    cols = [c for c in TIMELINE_COLS if c in sch]
    if "launch_time" not in cols or "task_ms" not in cols:
        return [], False
    sel = ", ".join(qi(c) for c in cols)
    n = con.execute(f"SELECT count(*) FROM {src} WHERE {where} AND launch_time IS NOT NULL", params).fetchone()[0] or 0
    base = f"FROM {src} WHERE {where} AND launch_time IS NOT NULL"
    if n <= MAX_TIMELINE_TASKS:
        rows = con.execute(f"SELECT {sel} {base} ORDER BY launch_time, task_id", params).fetchall()
        return [dict(zip(cols, r)) for r in rows], False
    failed = "coalesce(failed, false)" if "failed" in cols else "false"
    keep_long = MAX_TIMELINE_TASKS // 3
    rows = con.execute(
        f"SELECT {sel} FROM (SELECT *, row_number() OVER (ORDER BY {failed} DESC, task_ms DESC NULLS LAST) AS _top, "
        f"row_number() OVER (ORDER BY launch_time, task_id) AS _rn {base}) "
        f"WHERE {failed} OR _top <= {keep_long} OR (_rn - 1) % {math.ceil(n / (MAX_TIMELINE_TASKS - keep_long))} = 0 "
        f"ORDER BY launch_time, task_id LIMIT {MAX_TIMELINE_TASKS * 2}",
        params,
    ).fetchall()
    return [dict(zip(cols, r)) for r in rows], True


def stage_detail(store: Store, cid: str, ctx: str, stage_id: int, attempt: int) -> dict[str, Any]:
    store.cluster_dir(cid)
    key_where = "spark_context_id = ? AND stage_id = ? AND stage_attempt = ?"
    key_params = [ctx, int(stage_id), int(attempt)]
    with store.connect() as con:
        st = store.select(con, cid, "stages", key_where, key_params, limit=1)
        if not st:
            raise NotFound(f"stage {stage_id}.{attempt} not found in context {ctx!r}")
        stage = st[0]

        tasks_summary: dict[str, Any] = {"count": 0, "failed": 0, "min": None, "max": None, "mean": None}
        for k, _ in _QUANTILES:
            tasks_summary[k] = None
        durations: list[Any] = []
        sampled = False
        task_columns: dict[str, Any] = {}
        timeline: list[dict] = []
        timeline_sampled = False
        tpath = store.dataset_path(cid, "tasks", required=False)
        sharing = None
        placement = None
        if tpath is not None:
            src = store.src(tpath)
            task_columns = _task_column_stats(con, src, store.schema(con, tpath), key_where, key_params)
            qs = ", ".join(str(q) for _, q in _QUANTILES)
            row = con.execute(
                f"SELECT count(task_ms), count(*) FILTER (WHERE failed), min(task_ms), max(task_ms), avg(task_ms), "
                f"quantile_disc(task_ms, [{qs}]) FROM {src} WHERE {key_where}",
                key_params,
            ).fetchone()
            n = int(row[0] or 0)
            tasks_summary.update(
                {"count": n, "failed": int(row[1] or 0), "min": row[2], "max": row[3], "mean": row[4]}
            )
            if row[5] is not None:
                for (k, _), v in zip(_QUANTILES, row[5]):
                    tasks_summary[k] = v
            if n:
                step = max(1, math.ceil(n / MAX_TASK_DURATIONS))
                sampled = step > 1
                durations = [
                    r[0]
                    for r in con.execute(
                        f"SELECT task_ms FROM (SELECT task_ms, row_number() OVER (ORDER BY task_ms, task_id) AS rn "
                        f"FROM {src} WHERE {key_where} AND task_ms IS NOT NULL) "
                        f"WHERE (rn - 1) % {step} = 0 OR rn = {n} ORDER BY rn",
                        key_params,
                    ).fetchall()
                ]
            timeline, timeline_sampled = _task_timeline(con, src, store.schema(con, tpath), key_where, key_params)
            sharing = _stage_sharing(store, con, cid, src, store.schema(con, tpath), stage, key_where, key_params)
            placement = _task_placement(store, con, src, store.schema(con, tpath), key_where, key_params)
        tasks_summary = clean(tasks_summary)
        if isinstance(tasks_summary.get("mean"), float):
            tasks_summary["mean"] = round(tasks_summary["mean"], 1)

        executors = store.select(
            con, cid, "stage_executor_profile", key_where, key_params, order="task_ms_sum DESC NULLS LAST, executor_id"
        )
        findings = store.select(
            con,
            cid,
            "findings",
            "spark_context_id = ? AND stage_id = ? AND (stage_attempt = ? OR stage_attempt IS NULL)",
            key_params,
            order="finding_id",
        )
        job = None
        if stage.get("spark_job_id") is not None:
            j = store.select(
                con, cid, "spark_jobs", "spark_context_id = ? AND spark_job_id = ?", [ctx, stage["spark_job_id"]], limit=1
            )
            if j:
                job = j[0]
                job["status"] = _job_status(job.get("result"), job.get("end_time"))
        query = None
        if stage.get("sql_execution_id") is not None:
            q = store.select(
                con,
                cid,
                "sql_queries",
                "spark_context_id = ? AND sql_execution_id = ?",
                [ctx, stage["sql_execution_id"]],
                exclude=("final_plan", "initial_plan"),
                limit=1,
            )
            query = q[0] if q else None
        hot = store.select(con, cid, "hotspots", key_where + " AND kind = 'skew_task'", key_params,
                           order="ratio DESC NULLS LAST")

    return {
        "hotspots": hot,
        "stage": stage,
        "tasks_summary": tasks_summary,
        "task_columns": task_columns,
        "task_durations": clean(durations),
        "sampled": sampled,
        "task_timeline": clean(timeline),
        "task_timeline_sampled": timeline_sampled,
        "executors": executors,
        "findings": findings,
        "job": job,
        "query": query,
        "sharing": sharing,
        "placement": placement,
    }


def _task_placement(store: Store, con, src: str, sch: dict, key_where: str, key_params) -> list[dict] | None:
    """Revision 14: per executor, how many of the stage's successful tasks fell in each size band (storage input +
    shuffle read) and how much they read: whether the big tasks landed on one executor or were spread out."""
    if "executor_id" not in sch or "input_bytes" not in sch:
        return None
    b = "(coalesce(input_bytes, 0) + coalesce(shuffle_read, 0))" if "shuffle_read" in sch else "coalesce(input_bytes, 0)"
    mb = 1 << 20
    host = "any_value(host)" if "host" in sch else "NULL"
    rows = store.rows(con, f"""
        SELECT executor_id, {host} AS host, count(*) AS tasks,
               count(*) FILTER (WHERE {b} <= 0) AS tasks_none,
               count(*) FILTER (WHERE {b} > 0 AND {b} < {10 * mb}) AS tasks_lt10,
               count(*) FILTER (WHERE {b} >= {10 * mb} AND {b} < {128 * mb}) AS tasks_10_128,
               count(*) FILTER (WHERE {b} >= {128 * mb} AND {b} < {256 * mb}) AS tasks_128_256,
               count(*) FILTER (WHERE {b} >= {256 * mb}) AS tasks_ge256,
               sum({b}) AS bytes_in, max({b}) AS max_bytes_in, sum(task_ms) AS task_ms, max(task_ms) AS max_task_ms,
               -- Revision 15: the spread of task time and data on each executor
               CAST(quantile_cont(task_ms, 0.1) AS BIGINT) AS p10_task_ms, CAST(median(task_ms) AS BIGINT) AS p50_task_ms,
               CAST(quantile_cont(task_ms, 0.9) AS BIGINT) AS p90_task_ms, min(task_ms) AS min_task_ms,
               CAST(median({b}) AS BIGINT) AS p50_bytes_in, CAST(quantile_cont({b}, 0.9) AS BIGINT) AS p90_bytes_in,
               {"sum(disk_spill)" if "disk_spill" in sch else "NULL"} AS disk_spill,
               {"sum(gc_ms)" if "gc_ms" in sch else "NULL"} AS gc_ms
        FROM {src} WHERE {key_where} AND NOT coalesce(failed, false)
        GROUP BY executor_id ORDER BY bytes_in DESC NULLS LAST, executor_id""", list(key_params))
    return clean(rows)


def stage_why(store: Store, cid: str, ctx: str, query: int | None = None, job: int | None = None) -> dict[str, Any]:
    """Revision 15: for every stage of a query or a Spark job, the time facts that explain a slow stage beyond its own
    metrics: how long it waited for its first task (cores busy), how long its last 10% of tasks dragged on, and how
    much of its executors' core time other stages took while it ran."""
    store.cluster_dir(cid)
    spath = store.dataset_path(cid, "stages", required=False)
    tpath = store.dataset_path(cid, "tasks", required=False)
    epath = store.dataset_path(cid, "executors", required=False)
    if spath is None or tpath is None:
        return {"stages": []}
    where, prm = "spark_context_id = ?", [ctx]
    if query is not None:
        where += " AND sql_execution_id = ?"
        prm.append(int(query))
    if job is not None:
        where += " AND spark_job_id = ?"
        prm.append(int(job))
    cores = (f"(SELECT executor_id, max(cores) AS cores FROM {store.src(epath)} WHERE spark_context_id = ? GROUP BY 1)"
             if epath is not None else "(SELECT NULL AS executor_id, NULL AS cores WHERE FALSE)")
    sql = f"""
        WITH st AS (SELECT stage_id, stage_attempt, start_time AS s0, end_time AS s1 FROM {store.src(spath)}
                    WHERE {where} AND start_time IS NOT NULL AND end_time IS NOT NULL),
             t AS (SELECT stage_id, stage_attempt, executor_id, launch_time, finish_time, task_ms
                   FROM {store.src(tpath)} WHERE spark_context_id = ? AND launch_time IS NOT NULL AND finish_time IS NOT NULL),
             own AS (SELECT t.stage_id, t.stage_attempt, min(t.launch_time) AS first_launch,
                            quantile_cont(epoch_ms(t.finish_time), 0.9) AS p90_end
                     FROM t JOIN st USING (stage_id, stage_attempt) GROUP BY 1, 2),
             ex AS (SELECT DISTINCT t.stage_id, t.stage_attempt, t.executor_id FROM t JOIN st USING (stage_id, stage_attempt)),
             slot AS (SELECT ex.stage_id, ex.stage_attempt, sum(c.cores) AS cores
                      FROM ex JOIN {cores} c ON c.executor_id = ex.executor_id GROUP BY 1, 2),
             oth AS (SELECT ex.stage_id, ex.stage_attempt,
                            sum(epoch_ms(least(o.finish_time, st.s1)) - epoch_ms(greatest(o.launch_time, st.s0))) AS other_ms
                     FROM ex JOIN st USING (stage_id, stage_attempt)
                     JOIN t o ON o.executor_id = ex.executor_id AND o.launch_time < st.s1 AND o.finish_time > st.s0
                             AND NOT (o.stage_id = ex.stage_id AND o.stage_attempt = ex.stage_attempt)
                     GROUP BY 1, 2)
        SELECT st.stage_id, st.stage_attempt,
               epoch_ms(own.first_launch) - epoch_ms(st.s0) AS wait_ms,
               epoch_ms(st.s1) - CAST(own.p90_end AS BIGINT) AS tail_ms,
               oth.other_ms, slot.cores * (epoch_ms(st.s1) - epoch_ms(st.s0)) AS slot_ms
        FROM st LEFT JOIN own USING (stage_id, stage_attempt) LEFT JOIN oth USING (stage_id, stage_attempt)
        LEFT JOIN slot USING (stage_id, stage_attempt)"""
    with store.connect() as con:
        rows = store.rows(con, sql, prm + [ctx] + ([ctx] if epath is not None else []))
        # where each stage's tasks ran: tasks and task time per executor
        by_exec: dict[tuple, list] = {}
        for e in store.rows(con, f"""
                SELECT t.stage_id, t.stage_attempt, t.executor_id, count(*) AS tasks, sum(t.task_ms) AS task_ms
                FROM {store.src(tpath)} t JOIN (SELECT stage_id, stage_attempt FROM {store.src(spath)} WHERE {where}) st
                  USING (stage_id, stage_attempt)
                WHERE t.spark_context_id = ? GROUP BY ALL ORDER BY task_ms DESC""", prm + [ctx]):
            by_exec.setdefault((e["stage_id"], e["stage_attempt"]), []).append(
                {"executor_id": e["executor_id"], "tasks": e["tasks"], "task_ms": e["task_ms"]})
    for r in rows:
        r["by_exec"] = by_exec.get((r["stage_id"], r["stage_attempt"]), [])
        r["other_share"] = round(min(1.0, (r.get("other_ms") or 0) / r["slot_ms"]), 3) if r.get("slot_ms") else None
        for k in ("wait_ms", "tail_ms"):
            if r.get(k) is not None and r[k] < 0:
                r[k] = 0
    return {"stages": rows}


def _union_ms(spans: list[tuple[int, int]]) -> int:
    """Total length of the union of [start, end) spans."""
    tot, cur_s, cur_e = 0, None, None
    for a, b in sorted(x for x in spans if x[1] > x[0]):
        if cur_e is None or a > cur_e:
            if cur_e is not None:
                tot += cur_e - cur_s
            cur_s, cur_e = a, b
        else:
            cur_e = max(cur_e, b)
    if cur_e is not None:
        tot += cur_e - cur_s
    return tot


def _clip(spans: list[tuple[int, int]], cut: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """spans minus cut (both lists of [start, end))."""
    out = []
    cut = sorted(c for c in cut if c[1] > c[0])
    for a, b in spans:
        pieces = [(a, b)]
        for c0, c1 in cut:
            nxt = []
            for x0, x1 in pieces:
                if c1 <= x0 or c0 >= x1:
                    nxt.append((x0, x1))
                    continue
                if c0 > x0:
                    nxt.append((x0, c0))
                if c1 < x1:
                    nxt.append((c1, x1))
            pieces = nxt
        out += pieces
    return out


def query_time(store: Store, cid: str, ctx: str, query: int) -> dict[str, Any]:
    """Revision 15: where a query's wall-clock time went: its stages (and those of the queries that ran inside it)
    waiting for a free core, running tasks, and the rest with no Spark stage of it running at all (driver work:
    planning, commits, listing files, Python between actions). Queries inside it: linked by root execution id, or,
    when the event log does not link them (a MERGE's own steps), the same run's queries that ran within its window
    with the same description."""
    store.cluster_dir(cid)
    with store.connect() as con:
        q = (store.select(con, cid, "sql_queries", "spark_context_id = ? AND sql_execution_id = ?", [ctx, int(query)],
                          exclude=("final_plan", "initial_plan", "details"), limit=1) or [None])[0]
        if q is None:
            raise NotFound(f"query {query} not found in context {ctx!r}")
        q0, q1 = _as_ms(q.get("start_time")), _as_ms(q.get("end_time"))
        if q0 is None or q1 is None:
            return {"query": query, "total_ms": None, "inner": [], "waiting_ms": 0, "running_ms": 0, "outside_ms": 0}
        others = store.select(con, cid, "sql_queries", "spark_context_id = ? AND sql_execution_id <> ?", [ctx, int(query)],
                              exclude=("final_plan", "initial_plan", "details"), truncate={"description": 200})
        head = (q.get("description") or "")[:40]
        inner = []
        for o in others:
            o0, o1 = _as_ms(o.get("start_time")), _as_ms(o.get("end_time"))
            if o0 is None or o1 is None or o0 < q0 or o1 > q1:
                continue
            linked = o.get("root_execution_id") == query
            same = (head and (o.get("description") or "").startswith(head) and o.get("run_key") == q.get("run_key"))
            if linked or same:
                inner.append({"sql_execution_id": o["sql_execution_id"], "start": o0, "end": o1, "duration_ms": o1 - o0,
                              "description": o.get("description"), "linked": bool(linked), "tasks": o.get("tasks")})
        ids = [int(query)] + [i["sql_execution_id"] for i in inner]
        ph = ", ".join("?" for _ in ids)
        stages = store.select(con, cid, "stages", f"spark_context_id = ? AND sql_execution_id IN ({ph})", [ctx, *ids])
        firsts: dict[tuple, int] = {}
        causes = None
        tpath = store.dataset_path(cid, "tasks", required=False)
        if tpath is not None and stages:
            for r in store.rows(con, f"""
                    SELECT stage_id, stage_attempt, min(launch_time) AS first FROM {store.src(tpath)}
                    WHERE spark_context_id = ? AND stage_id IN ({", ".join("?" for _ in stages)})
                    GROUP BY 1, 2""", [ctx, *[st["stage_id"] for st in stages]]):
                firsts[(r["stage_id"], r["stage_attempt"])] = _as_ms(r["first"])
            # where its task time went (the cause bar), over the tasks of its stages
            tsch = store.schema(con, tpath)
            # the time buckets count successful attempts only: a failed attempt's time is its own bucket
            col = lambda c: (f"sum(CASE WHEN NOT t.failed THEN t.{c} END)" if c != "disk_spill" else f"sum(t.{c})") if c in tsch else "NULL"  # noqa: E731
            keys = sorted({(st["stage_id"], st["stage_attempt"]) for st in stages})
            causes = _causes(store.rows(con, f"""
                    SELECT sum(t.task_ms) AS task_ms, {col('cpu_ms')} AS cpu_ms, {col('gc_ms')} AS gc_ms,
                           {col('fetch_wait_ms')} AS fetch_wait_ms, {col('disk_spill')} AS disk_spill,
                           {col('shuffle_write_ms')} AS shuffle_write_ms,
                           sum(CASE WHEN t.failed THEN t.task_ms ELSE 0 END) AS failed_ms
                    FROM {store.src(tpath)} t WHERE t.spark_context_id = ?
                      AND (t.stage_id, t.stage_attempt) IN ({", ".join("(?, ?)" for _ in keys)})""",
                    [ctx, *[x for k in keys for x in k]]))
    wait_sp, run_sp, steps = [], [], []
    for st in stages:
        s0, s1 = _as_ms(st.get("start_time")), _as_ms(st.get("end_time"))
        if s0 is None or s1 is None:
            continue
        f = firsts.get((st["stage_id"], st["stage_attempt"]))
        f = min(max(f, s0), s1) if f is not None else s0
        wait_sp.append((max(s0, q0), min(f, q1)))
        run_sp.append((max(f, q0), min(s1, q1)))
        # each stage step by step: submitted, got its first core, finished
        steps.append({"sql_execution_id": st.get("sql_execution_id"), "spark_job_id": st.get("spark_job_id"),
                      "stage_id": st["stage_id"], "stage_attempt": st["stage_attempt"], "status": st.get("status"),
                      "tasks": st.get("tasks") if st.get("tasks") is not None else st.get("num_tasks"),
                      "submitted": s0, "first_task": f, "end": s1, "wait_ms": f - s0, "run_ms": s1 - f,
                      # rows passed along: in from storage and from the shuffle (its parent stages), out to the
                      # shuffle (the next stage) and to storage
                      "rows_read": st.get("input_records"), "rows_from_shuffle": st.get("shuffle_read_records"),
                      "rows_to_shuffle": st.get("shuffle_write_records"), "rows_written": st.get("output_records"),
                      "parent_ids": list(st.get("parent_ids") or [])})
    running = _union_ms(run_sp)
    waiting = _union_ms(_clip(wait_sp, run_sp))
    total = q1 - q0
    inner.sort(key=lambda i: i["start"])
    return {"query": query, "total_ms": total, "running_ms": running, "waiting_ms": waiting,
            "outside_ms": max(0, total - running - waiting), "inner": inner, "causes": causes,
            "start": q0, "end": q1, "steps": sorted(steps, key=lambda x: (x["submitted"], x["stage_id"]))}


MAX_OTHER_TASKS = 4000  # other stages' tasks drawn on a stage's executor timeline (the longest first)


def _stage_sharing(store: Store, con, cid: str, src: str, sch: dict, stage: dict, key_where: str,
                   key_params: list) -> dict | None:
    """Revision 13: what else ran on this stage's executors while it ran (other stages, other runs), and how much of
    their core time it took, next to this stage's own share: a slow stage may have been short of cores, not slow."""
    return _scope_sharing(store, con, cid, src, sch, key_params[0], [(int(stage["stage_id"]), int(stage["stage_attempt"]))],
                          stage.get("start_time"), stage.get("end_time"), stage.get("run_key"))


def _scope_sharing(store: Store, con, cid: str, src: str, sch: dict, ctx: str, keys: list, s0, s1,
                   my_run) -> dict | None:
    """Revision 18: the same over any set of stage attempts (one stage, a Spark job, a SQL query) and a window: their
    executors, the other tasks on them in the window, and per executor theirs, the others' and both together."""
    if s0 is None or s1 is None or s1 <= s0 or not keys:
        return None
    mine_cond = "(" + " OR ".join("(stage_id = ? AND stage_attempt = ?)" for _ in keys) + ")"
    mine_prm = [v for k in keys for v in k]
    key_where = f"spark_context_id = ? AND {mine_cond}"
    key_params = [ctx, *mine_prm]
    kset = set(keys)
    rk = "run_key" if "run_key" in sch else "NULL"
    win = "launch_time < epoch_ms(CAST(? AS BIGINT)) AND finish_time > epoch_ms(CAST(? AS BIGINT))"
    overlap = ("sum(epoch_ms(least(finish_time, epoch_ms(CAST(? AS BIGINT)))) "
               "- epoch_ms(greatest(launch_time, epoch_ms(CAST(? AS BIGINT)))))")
    mine = f"SELECT DISTINCT executor_id FROM {src} WHERE {key_where}"
    rows = con.execute(
        f"SELECT {rk} AS run_key, stage_id, stage_attempt, count(*) AS tasks, {overlap} AS task_ms, "
        f"count(DISTINCT executor_id) AS executors FROM {src} "
        f"WHERE spark_context_id = ? AND executor_id IN ({mine}) AND {win} "
        f"GROUP BY ALL ORDER BY task_ms DESC NULLS LAST",
        [s1, s0, key_params[0], *key_params, s1, s0],
    ).fetchall()
    execs = [r[0] for r in con.execute(mine, key_params).fetchall()]
    own = sum(r[4] or 0 for r in rows if (r[1], r[2]) in kset)
    others = [r for r in rows if (r[1], r[2]) not in kset]
    cores = 0
    for e in store.select(con, cid, "executors", "spark_context_id = ?", [key_params[0]]):
        if e.get("executor_id") in execs:
            cores += e.get("cores") or 0
    names: dict = {}
    ids = sorted({r[1] for r in others[:15]})
    if ids:
        for st in store.select(con, cid, "stages", f"spark_context_id = ? AND stage_id IN ({', '.join('?' * len(ids))})",
                               [key_params[0], *ids], truncate={"stage_name": 120}):
            names[(st["stage_id"], st["stage_attempt"])] = st
    out = []
    for r in others[:15]:
        st = names.get((r[1], r[2]), {})
        out.append({"run_key": r[0], "stage_id": r[1], "stage_attempt": r[2], "tasks": r[3], "task_ms": r[4],
                    "executors": r[5], "spark_job_id": st.get("spark_job_id"),
                    "sql_execution_id": st.get("sql_execution_id"), "stage_name": st.get("stage_name"),
                    "same_run": r[0] == my_run})
    # Revision 15: the other stages' tasks themselves (longest first, capped), to draw next to this stage's tasks
    cols = [c for c in ("executor_id", "launch_time", "task_ms", "stage_id", "stage_attempt", "task_id", "failed",
                        "disk_spill", "input_bytes", "shuffle_read", "gc_ms") if c in sch]
    other_tasks: list[dict] = []
    n_other = 0
    if "launch_time" in cols and "task_ms" in cols:
        base = (f"FROM {src} WHERE spark_context_id = ? AND executor_id IN ({mine}) AND {win} "
                f"AND NOT {mine_cond}")
        prm = [key_params[0], *key_params, s1, s0, *mine_prm]
        n_other = con.execute(f"SELECT count(*) {base}", prm).fetchone()[0] or 0
        sel = ", ".join(qi(c) for c in cols) + (f", {rk} AS run_key")
        for r in con.execute(f"SELECT {sel} {base} ORDER BY task_ms DESC NULLS LAST LIMIT {MAX_OTHER_TASKS}", prm).fetchall():
            d = dict(zip(cols + ["run_key"], r))
            d["launch_time"] = _as_ms(d.get("launch_time"))
            other_tasks.append(d)
    # Revision 17: per executor, this stage's tasks and the other stages' tasks in its window, side by side: their
    # time (inside the window), data, shuffle, spill, GC and memory
    agg = lambda c, f="sum": f"{f}({qi(c)})" if c in sch else "NULL"
    failed = "sum(CASE WHEN failed THEN 1 ELSE 0 END)" if "failed" in sch else "NULL"
    by_exec = []
    for r in con.execute(
            f"SELECT executor_id, {mine_cond} AS mine, count(*) AS tasks, {overlap} AS task_ms, "
            f"{agg('input_bytes')}, {agg('shuffle_read')}, {agg('shuffle_write')}, {agg('mem_spill')}, {agg('disk_spill')}, "
            f"{agg('gc_ms')}, {agg('peak_mem', 'max')}, {failed}, count(DISTINCT stage_id), count(DISTINCT {rk}) "
            f"FROM {src} WHERE spark_context_id = ? AND executor_id IN ({mine}) AND {win} GROUP BY 1, 2",
            [*mine_prm, s1, s0, key_params[0], *key_params, s1, s0]).fetchall():
        by_exec.append(dict(zip(("executor_id", "mine", "tasks", "task_ms", "input_bytes", "shuffle_read", "shuffle_write",
                                 "mem_spill", "disk_spill", "gc_ms", "peak_mem", "failed", "stages", "runs"), r)))
    # and both together per executor (stages and runs counted once)
    for r in con.execute(
            f"SELECT executor_id, NULL AS mine, count(*) AS tasks, {overlap} AS task_ms, "
            f"{agg('input_bytes')}, {agg('shuffle_read')}, {agg('shuffle_write')}, {agg('mem_spill')}, {agg('disk_spill')}, "
            f"{agg('gc_ms')}, {agg('peak_mem', 'max')}, {failed}, count(DISTINCT (stage_id, stage_attempt)), count(DISTINCT {rk}) "
            f"FROM {src} WHERE spark_context_id = ? AND executor_id IN ({mine}) AND {win} GROUP BY 1",
            [s1, s0, key_params[0], *key_params, s1, s0]).fetchall():
        by_exec.append(dict(zip(("executor_id", "mine", "tasks", "task_ms", "input_bytes", "shuffle_read", "shuffle_write",
                                 "mem_spill", "disk_spill", "gc_ms", "peak_mem", "failed", "stages", "runs"), r)))
    # each other stage's typical task over the whole stage (not only the part inside this window)
    typical = []
    keys = sorted({(t["stage_id"], t["stage_attempt"]) for t in other_tasks})
    if keys:
        ph = " OR ".join("(stage_id = ? AND stage_attempt = ?)" for _ in keys)
        for st in store.select(con, cid, "stages", f"spark_context_id = ? AND ({ph})", [key_params[0], *[v for k in keys for v in k]],
                               exclude=("details", "rdd_scopes", "rdd_names", "parent_ids", "stage_name", "failure_reason")):
            typical.append({"stage_id": st["stage_id"], "stage_attempt": st["stage_attempt"], "p50_task_ms": st.get("p50_task_ms"),
                            "p50_read": st.get("p50_task_bytes_in"), "run_key": st.get("run_key")})
    info = []
    for e in store.select(con, cid, "executors", "spark_context_id = ?", [key_params[0]]):
        if e.get("executor_id") in execs:
            info.append({k: e.get(k) for k in ("executor_id", "host", "cores", "added_time", "removed_time", "removal_category")})
    return {"start": s0, "end": s1, "executors": execs, "cores": cores or None, "slot_ms": (cores * (s1 - s0)) or None,
            "own_task_ms": own, "other_task_ms": sum(r[4] or 0 for r in others), "other_stages": len(others),
            "other_runs": len({r[0] for r in others if r[0] is not None and r[0] != my_run}), "rows": out,
            "other_tasks": other_tasks, "other_task_count": n_other, "by_exec": by_exec, "typical": typical,
            "exec_info": info, "run_key": my_run}


# ---------------------------------------------------------------------------------------------------------------------
# Query detail
# ---------------------------------------------------------------------------------------------------------------------


def query_detail(store: Store, cid: str, ctx: str, exec_id: int) -> dict[str, Any]:
    store.cluster_dir(cid)
    kp = [ctx, int(exec_id)]
    kw = "spark_context_id = ? AND sql_execution_id = ?"
    with store.connect() as con:
        q = store.select(con, cid, "sql_queries", kw, kp, limit=1)
        prof = store.select(con, cid, "query_profile", kw, kp, limit=1)
        if not q and not prof:
            raise NotFound(f"query {exec_id} not found in context {ctx!r}")
        jobs = store.select(con, cid, "spark_jobs", kw, kp, order="spark_job_id")
        for j in jobs:
            j["status"] = _job_status(j.get("result"), j.get("end_time"))
        stages = store.select(con, cid, "stages", kw, kp, order="stage_id, stage_attempt")
        stage_ids = sorted({s["stage_id"] for s in stages if s.get("stage_id") is not None})
        if stage_ids:
            ph = ", ".join("?" for _ in stage_ids)
            findings = store.select(
                con,
                cid,
                "findings",
                f"spark_context_id = ? AND (sql_execution_id = ? OR stage_id IN ({ph}))",
                kp + stage_ids,
                order="finding_id",
            )
        else:
            findings = store.select(con, cid, "findings", kw, kp, order="finding_id")
        eng, aqe_set = _engine_settings(store, con, cid)
    return {
        "query": q[0] if q else None,
        "engine": engine_use(q[0], eng, aqe_set) if q else None,
        "profile": prof[0] if prof else None,
        "jobs": jobs,
        "stages": stages,
        "findings": findings,
        "logic": query_logic(q[0]) if q else None,
    }


# ---------------------------------------------------------------------------------------------------------------------
# Plan diff
# ---------------------------------------------------------------------------------------------------------------------


def normalize_plan_lines(plan: str | None) -> list[str]:
    if not plan:
        return []
    return [_EXPR_ID_RE.sub("", line).rstrip() for line in plan.splitlines()]


def plan_diff(
    store: Store, cid: str, a_ctx: str, a_id: int, b_ctx: str, b_id: int, which: str = "final",
    b_cid: str | None = None,
) -> dict[str, Any]:
    if which not in ("final", "initial"):
        raise BadRequest("which must be 'final' or 'initial'")
    col = "final_plan" if which == "final" else "initial_plan"
    b_cid = b_cid or cid
    store.cluster_dir(cid)
    store.cluster_dir(b_cid)
    meta_cols = ("description", "status", "duration_ms", "start_time", "plan_hash")
    side: dict[str, dict[str, Any]] = {}
    lines: dict[str, list[str]] = {}
    with store.connect() as con:
        for label, qcid, ctx, eid in (("a", cid, a_ctx, a_id), ("b", b_cid, b_ctx, b_id)):
            rows = store.select(
                con, qcid, "sql_queries", "spark_context_id = ? AND sql_execution_id = ?", [ctx, int(eid)], limit=1
            )
            if not rows:
                raise NotFound(f"query {eid} not found in context {ctx!r} of cluster {qcid!r}")
            r = rows[0]
            plan = r.get(col) or ""
            if not plan and which == "final":
                plan = r.get("initial_plan") or ""
            lines[label] = normalize_plan_lines(plan)
            info = {"cluster_id": qcid, "spark_context_id": ctx, "sql_execution_id": int(eid), "which": which}
            info.update({k: r.get(k) for k in meta_cols})
            info["plan"] = plan
            info["lines"] = len(lines[label])
            side[label] = info

    a_lines, b_lines = lines["a"], lines["b"]
    diff: list[dict[str, Any]] = []
    sm = difflib.SequenceMatcher(a=a_lines, b=b_lines, autojunk=False)
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            for k in range(i2 - i1):
                diff.append({"op": "equal", "a_line": i1 + k + 1, "b_line": j1 + k + 1, "text": a_lines[i1 + k]})
            continue
        if tag in ("delete", "replace"):
            for i in range(i1, i2):
                diff.append({"op": "delete", "a_line": i + 1, "b_line": None, "text": a_lines[i]})
        if tag in ("insert", "replace"):
            for j in range(j1, j2):
                diff.append({"op": "insert", "a_line": None, "b_line": j + 1, "text": b_lines[j]})
    side["a"]["same_hash"] = side["b"]["same_hash"] = (
        side["a"].get("plan_hash") is not None and side["a"].get("plan_hash") == side["b"].get("plan_hash")
    )
    return {"a": side["a"], "b": side["b"], "diff": diff, "ratio": round(sm.ratio(), 4)}


# ---------------------------------------------------------------------------------------------------------------------
# Plan candidates (Revision 5): the same query in other runs first, then in this run, then any other query
# ---------------------------------------------------------------------------------------------------------------------

_LITERALS = re.compile(r"'(?:[^']|'')*'|\b\d+(?:\.\d+)?\b")


def query_key(description: Any) -> str:
    """Normalized query identity: lower case, string and number literals replaced, whitespace collapsed, so the same
    statement (or the same call site such as 'saveAsTable at main.py:18') matches across runs."""
    if description is None:
        return ""
    s = _LITERALS.sub("?", str(description).lower())
    return " ".join(s.split())[:500]


_CANDIDATE_LIMIT = {"same_query_other_run": 50, "same_query_this_run": 50, "other_query": 150}
_CANDIDATE_COLS = ("spark_context_id", "sql_execution_id", "description", "status", "start_time", "duration_ms",
                   "plan_hash")


def plan_candidates(store: Store, cid: str, ctx: str, exec_id: int) -> list[dict[str, Any]]:
    store.cluster_dir(cid)
    excl = ("final_plan", "initial_plan", "details")
    with store.connect() as con:
        me = store.select(con, cid, "sql_queries", "spark_context_id = ? AND sql_execution_id = ?",
                          [ctx, int(exec_id)], exclude=excl, limit=1)
        if not me:
            raise NotFound(f"query {exec_id} not found in context {ctx!r}")
        me = me[0]
        key = query_key(me.get("description"))
        out: dict[str, list[dict[str, Any]]] = {g: [] for g in _CANDIDATE_LIMIT}
        for qcid in [c["cluster_id"] for c in list_clusters(store)]:
            try:
                summ = read_summary(store, qcid)
            except ApiError:
                continue
            rows = store.select(con, qcid, "sql_queries", "TRUE", (), exclude=excl,
                                order="start_time NULLS LAST, sql_execution_id")
            for r in rows:
                if qcid == cid and r.get("spark_context_id") == ctx and r.get("sql_execution_id") == int(exec_id):
                    continue
                same = bool(key) and query_key(r.get("description")) == key
                if qcid != cid:
                    if not same:
                        continue
                    group = "same_query_other_run"
                else:
                    group = "same_query_this_run" if same else "other_query"
                out[group].append({
                    "group": group, "cluster_id": qcid, **{c: r.get(c) for c in _CANDIDATE_COLS},
                    "same_plan_hash": r.get("plan_hash") is not None and r.get("plan_hash") == me.get("plan_hash"),
                    "run_status": summ.get("status"), "run_start_time": summ.get("start_time"),
                })
    out["same_query_other_run"].sort(key=lambda r: (r.get("run_start_time") or 0), reverse=True)
    res: list[dict[str, Any]] = []
    for g, lim in _CANDIDATE_LIMIT.items():
        res.extend(out[g][:lim])
    return clean(res)


_HOTSPOT_ORDER = ("CASE kind WHEN 'skew_task' THEN 0 WHEN 'shuffle_peak' THEN 1 WHEN 'spill_peak' THEN 2 "
                  "WHEN 'gc_peak' THEN 3 ELSE 4 END, ratio DESC NULLS LAST, ts_start")


def hotspots(store: Store, cid: str, ctx: str | None = None, kind: str | None = None,
             run: str | None = None) -> list[dict[str, Any]]:
    store.cluster_dir(cid)
    where, params = ["TRUE"], []
    if run:
        where.append("(run_key = ? OR list_contains(affected_runs, ?))")
        params.extend([run, run])
    if ctx:
        where.append("spark_context_id = ?")
        params.append(ctx)
    if kind:
        where.append("kind = ?")
        params.append(kind)
    with store.connect() as con:
        return store.select(con, cid, "hotspots", " AND ".join(where), params, order=_HOTSPOT_ORDER)


# ---------------------------------------------------------------------------------------------------------------------
# Gantt
# ---------------------------------------------------------------------------------------------------------------------

MAX_MARKERS_PER_KIND = 2000


def contexts(store: Store, con, cid: str) -> list[str]:
    parts = []
    for name in ("apps", "spark_jobs", "stages", "tasks"):
        p = store.dataset_path(cid, name, required=False)
        if p is not None and "spark_context_id" in store.schema(con, p):
            parts.append(f"SELECT DISTINCT spark_context_id AS c FROM {store.src(p)}")
    if not parts:
        return []
    rows = con.execute(
        f"SELECT DISTINCT c FROM ({' UNION ALL '.join(parts)}) WHERE c IS NOT NULL ORDER BY c"
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------------------------------------------------
# Flow (Revision 13): the SQL queries and Spark jobs of a run on one clock, and what led to what
# ---------------------------------------------------------------------------------------------------------------------

FLOW_MAX_NODES = 3000
_DELTA_SUFFIX = re.compile(r"/(_delta_log|_checkpoint|_commits)(/.*)?$")


def _table_key(t: str) -> str:
    """the table behind a name or a storage path (a Delta log file read means the table was read)"""
    return _DELTA_SUFFIX.sub("", str(t).strip().rstrip("/")).lower()


def _short_table(t: str) -> str:
    t = str(t).rstrip("/")
    return t.rsplit("/", 1)[-1] if "/" in t else t


def _merge(spans: list[tuple[int, int]]) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for a, b in sorted(x for x in spans if x[1] > x[0]):
        if out and a <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], b))
        else:
            out.append((a, b))
    return out


def _wait_and_run(stages: list[dict], firsts: Mapping[tuple, int], key) -> dict[Any, tuple[list, list]]:
    """See analysis.contention.wait_and_run."""
    from ..analysis.contention import wait_and_run

    return wait_and_run(stages, firsts, key)


def _query_waits(stages: list[dict], firsts: Mapping[tuple, int]) -> dict[tuple, list[list[int]]]:
    """Per query (ctx, sql_execution_id): its waits for a free core (see _wait_and_run), 1 s or more."""
    key = (lambda st: (st["spark_context_id"], st["sql_execution_id"]) if st.get("sql_execution_id") is not None else None)
    out: dict[tuple, list[list[int]]] = {}
    for k, (waits, _) in _wait_and_run(stages, firsts, key).items():
        spans = [[a, b] for a, b in waits if b - a >= 1000]
        if spans:
            out[k] = spans
    return out


_RUN_TIMES: dict[tuple, dict] = {}


def _run_times(store: Store, con, cid: str) -> dict[str, dict]:
    """Per run: time waiting for a free core (a stage submitted, none of the run's stages running a task) and time
    running tasks. Cached per cluster while its stages and tasks files are unchanged."""
    spath = store.dataset_path(cid, "stages", required=False)
    tpath = store.dataset_path(cid, "tasks", required=False)
    if spath is None or tpath is None or "run_key" not in store.schema(con, spath):
        return {}
    stamp = (cid, str(spath), Path(spath).stat().st_mtime if Path(spath).exists() else 0,
             str(tpath), Path(tpath).stat().st_mtime if Path(tpath).exists() else 0)
    if stamp in _RUN_TIMES:
        return _RUN_TIMES[stamp]
    stages = store.rows(con, f"SELECT spark_context_id, stage_id, stage_attempt, run_key, start_time, end_time "
                             f"FROM {store.src(spath)} WHERE run_key IS NOT NULL")
    firsts = {(r["spark_context_id"], r["stage_id"], r["stage_attempt"]): _as_ms(r["first"]) for r in store.rows(
        con, f"SELECT spark_context_id, stage_id, stage_attempt, min(launch_time) AS first FROM {store.src(tpath)} GROUP BY 1, 2, 3")}
    wr = _wait_and_run(stages, firsts, lambda st: st.get("run_key"))
    out = {k: {"waiting_ms": sum(b - a for a, b in w), "running_ms": sum(b - a for a, b in r)} for k, (w, r) in wr.items()}
    # on the cluster's clock (runs overlap, so the sums above can be days): time with at least one run waiting for a
    # free core, with at least one running tasks, and with both at once
    ws = _merge([x for w, _ in wr.values() for x in w])
    rs = _merge([x for _, r in wr.values() for x in r])
    both = sum(max(0, min(b, d) - max(a, c)) for a, b in ws for c, d in rs if c < b and a < d)
    out["__clock__"] = {"waiting_ms": sum(b - a for a, b in ws), "running_ms": sum(b - a for a, b in rs), "both_ms": both}
    _RUN_TIMES.clear()
    _RUN_TIMES[stamp] = out
    return out


def flow(store: Store, cid: str, run: str | None = None) -> dict[str, Any]:
    """Nodes: SQL queries that ran Spark jobs, and Spark jobs without SQL. Edges, each with why:
      inside  - the query ran inside another one (rootExecutionId, Spark 3.4+)
      table   - it read a table an earlier query wrote (the last writer before it started)
      shuffle - one of its jobs reused shuffle output an earlier job computed (the stage was skipped here)
    """
    store.cluster_dir(cid)
    run = (run or "").strip() or None
    with store.connect() as con:
        def rows(name: str, order: str, trunc: Mapping[str, int] | None = None) -> list[dict]:
            p = store.dataset_path(cid, name, required=False)
            if p is None:
                return []
            if run and "run_key" in store.schema(con, p):
                return store.select(con, cid, name, "run_key = ?", [run], order=order, truncate=trunc,
                                    exclude=("final_plan", "initial_plan", "details"))
            return store.select(con, cid, name, order=order, truncate=trunc,
                                exclude=("final_plan", "initial_plan", "details"))

        queries = rows("sql_queries", "spark_context_id, sql_execution_id", {"description": 300, "error": 500})
        jobs = rows("spark_jobs", "spark_context_id, spark_job_id", {"description": 300, "call_site": 300, "error": 500})
        stages = rows("stages", "spark_context_id, stage_id, stage_attempt", {"stage_name": 200, "job_description": 1,
                                                                            "failure_reason": 1})
        # when each stage got its first core: before that it was waiting for one (cores busy with other work)
        firsts: dict[tuple, int] = {}
        tpath = store.dataset_path(cid, "tasks", required=False)
        if tpath is not None and stages:
            by_run = run and "run_key" in store.schema(con, tpath)
            for r in store.rows(con, f"""
                    SELECT spark_context_id, stage_id, stage_attempt, min(launch_time) AS first FROM {store.src(tpath)}
                    WHERE {"run_key = ?" if by_run else "TRUE"} GROUP BY 1, 2, 3""", [run] if by_run else []):
                firsts[(r["spark_context_id"], r["stage_id"], r["stage_attempt"])] = _as_ms(r["first"])
    waits_of = _query_waits(stages, firsts)
    jobs_of: dict[tuple, list] = {}
    for j in jobs:
        jobs_of.setdefault((j["spark_context_id"], j.get("sql_execution_id")), []).append(j)
    qby = {(q["spark_context_id"], q["sql_execution_id"]): q for q in queries}
    roots = {(q["spark_context_id"], q.get("root_execution_id")) for q in queries
             if q.get("root_execution_id") is not None and q.get("root_execution_id") != q["sql_execution_id"]}

    nodes: list[dict] = []
    node_of_job: dict[tuple, str] = {}
    keep = ("input_bytes", "input_records", "output_bytes", "output_records", "shuffle_read", "shuffle_write",
            "p10_task_bytes_in", "p50_task_bytes_in", "p90_task_bytes_in", "max_task_bytes_in", "avg_task_bytes_in",
            "p50_task_rows_in", "avg_task_rows_in", "disk_spill", "tasks", "wmed_task_bytes_in", "p90_task_ms",
            "photon_share", "tasks_none", "tasks_lt10", "tasks_10_128", "tasks_128_256", "tasks_ge256", "bytes_lt10",
            "bytes_10_128", "bytes_128_256", "bytes_ge256")
    for q in queries:
        k = (q["spark_context_id"], q["sql_execution_id"])
        js = jobs_of.get(k, [])
        if not js and k not in roots:
            continue
        key = f"q:{k[0]}:{k[1]}"
        for j in js:
            node_of_job[(k[0], j["spark_job_id"])] = key
        nodes.append({"key": key, "kind": "query", "ctx": k[0], "id": k[1], "run_key": q.get("run_key"),
                      "start": q.get("start_time"),
                      "end": q.get("end_time"), "status": q.get("status"), "what": q.get("description"),
                      "error": q.get("error"), "jobs": [j["spark_job_id"] for j in js],
                      "tables_read": sorted({_short_table(t) for t in q.get("tables_read") or []}),
                      "tables_written": sorted({_short_table(t) for t in q.get("tables_written") or []}),
                      "waits": waits_of.get(k, []),
                      **{c: q.get(c) for c in keep}})
    for j in jobs:
        if j.get("sql_execution_id") is not None and (j["spark_context_id"], j["sql_execution_id"]) in qby:
            continue
        key = f"j:{j['spark_context_id']}:{j['spark_job_id']}"
        node_of_job[(j["spark_context_id"], j["spark_job_id"])] = key
        st = _job_status(j.get("result"), j.get("end_time"))
        nodes.append({"key": key, "kind": "job", "ctx": j["spark_context_id"], "id": j["spark_job_id"],
                      "run_key": j.get("run_key"),
                      "start": j.get("start_time"), "end": j.get("end_time"), "status": st,
                      "what": j.get("description") or j.get("call_site"), "error": j.get("error"),
                      "jobs": [j["spark_job_id"]], "tables_read": [], "tables_written": [],
                      **{c: j.get(c) for c in keep}})
    nodes.sort(key=lambda n: (n["start"] is None, n["start"] or 0, n["key"]))
    truncated = len(nodes) > FLOW_MAX_NODES
    nodes = nodes[:FLOW_MAX_NODES]
    present = {n["key"] for n in nodes}

    edges: dict[tuple, dict] = {}

    def link(a: str | None, b: str | None, kind: str, label: str | None = None, rows: int | None = None,
             size: int | None = None):
        if not a or not b or a == b or a not in present or b not in present:
            return
        e = edges.setdefault((a, b, kind), {"from": a, "to": b, "kind": kind, "labels": [], "rows": None, "bytes": None})
        if label and label not in e["labels"] and len(e["labels"]) < 5:
            e["labels"].append(label)
            # what passed along it: a table: the rows its writer wrote; a reused shuffle: the rows that stage wrote
            if rows is not None:
                e["rows"] = (e["rows"] or 0) + int(rows)
            if size is not None:
                e["bytes"] = (e["bytes"] or 0) + int(size)

    # inside: the root query
    for q in queries:
        r = q.get("root_execution_id")
        if r is not None and r != q["sql_execution_id"]:
            link(f"q:{q['spark_context_id']}:{r}", f"q:{q['spark_context_id']}:{q['sql_execution_id']}", "inside")
    # table: the last query that finished writing the table before this one started
    writes: dict[str, list[tuple[int, str, str]]] = {}
    for q in queries:
        for t in q.get("tables_written") or []:
            if q.get("end_time") is not None:
                writes.setdefault(_table_key(t), []).append(
                    (q["end_time"], f"q:{q['spark_context_id']}:{q['sql_execution_id']}", _short_table(t),
                     q.get("output_records"), q.get("output_bytes")))
    for w in writes.values():
        w.sort(key=lambda x: (x[0], x[1]))
    for q in queries:
        me = f"q:{q['spark_context_id']}:{q['sql_execution_id']}"
        if q.get("start_time") is None:
            continue
        for t in q.get("tables_read") or []:
            before = [w for w in writes.get(_table_key(t), []) if w[0] <= q["start_time"] and w[1] != me]
            if before:
                link(before[-1][1], me, "table", before[-1][2], before[-1][3], before[-1][4])
    # shuffle: a stage this job lists ran in an earlier job, so this job reused its output
    ran_in = {(s["spark_context_id"], s["stage_id"]): s.get("spark_job_id") for s in stages
              if s.get("spark_job_id") is not None}
    wrote_sh = {(s["spark_context_id"], s["stage_id"]): (s.get("shuffle_write_records"), s.get("shuffle_write"))
                for s in stages if s.get("status") != "failed"}
    for j in jobs:
        me = node_of_job.get((j["spark_context_id"], j["spark_job_id"]))
        for sid in j.get("stage_ids") or []:
            src = ran_in.get((j["spark_context_id"], sid))
            if src is not None and src != j["spark_job_id"]:
                link(node_of_job.get((j["spark_context_id"], src)), me, "shuffle", f"stage {sid}",
                     *wrote_sh.get((j["spark_context_id"], sid), (None, None)))
    times = [t for n in nodes for t in (n["start"], n["end"]) if isinstance(t, int)]
    return {"nodes": nodes, "edges": list(edges.values()), "truncated": truncated,
            "start": min(times) if times else None, "end": max(times) if times else None, "run": run}


def memory_use(store: Store, con, cid: str, executors: list[dict]) -> list[dict]:
    """Revision 15: per minute, the JVM heap in use (heap after GC, from the GC log lines in stdout) against the heap
    of the executors that were up: all executors together, the fullest one (out of memory happens on one executor,
    not on average) and the driver. An executor keeps its last measured value until its next GC. [] without GC logs."""
    path = store.dataset_path(cid, "gc_events", required=False)
    if path is None:
        return []
    rows = store.rows(con, f"""
        SELECT CAST(epoch_ms(date_trunc('minute', ts)) AS BIGINT) AS minute, coalesce(executor_id, 'driver') AS ex,
               max(heap_after_mb) AS after_mb, max(heap_total_mb) AS total_mb
        FROM {store.src(path)}
        WHERE ts IS NOT NULL AND heap_after_mb IS NOT NULL AND (source = 'driver' OR executor_id IS NOT NULL)
        GROUP BY ALL ORDER BY minute""")
    if not rows:
        return []
    life = {str(e["executor_id"]): e for e in executors}
    by_ex: dict[str, dict[int, tuple]] = {}
    for r in rows:
        by_ex.setdefault(r["ex"], {})[r["minute"]] = (r["after_mb"], r["total_mb"])
    first, last = min(r["minute"] for r in rows), max(r["minute"] for r in rows)
    cur: dict[str, tuple] = {}
    out: list[dict] = []
    for m in range(first, last + 60_000, 60_000):
        used = up = 0.0
        top: tuple[float, str | None] = (0.0, None)
        driver = None
        for ex, pts in by_ex.items():
            if m in pts:
                cur[ex] = pts[m]
            if ex not in cur:
                continue
            after, total = cur[ex]
            if ex == "driver":
                driver = round(after / total, 3) if total else None
                continue
            e = life.get(ex) or {}
            if (e.get("added_time") is not None and e["added_time"] >= m + 60_000) or \
                    (e.get("removed_time") is not None and e["removed_time"] <= m):
                continue
            heap = e.get("heap_mb") or total
            if not heap:
                continue
            used += after
            up += heap
            if after / heap > top[0]:
                top = (after / heap, ex)
        if not up and driver is None:
            continue
        out.append({"minute": m, "used_mb": round(used), "up_mb": round(up),
                    "share": round(min(1.0, used / up), 3) if up else None,
                    "max_share": round(min(1.0, top[0]), 3) if top[1] else None, "max_exec": top[1],
                    "driver_share": driver})
    return out


def cluster_view(store: Store, cid: str) -> dict[str, Any]:
    """Revision 13: the cluster level only: which run started when, the executors, and per minute the CPU in use
    and the spill. Everything else is looked at per run."""
    store.cluster_dir(cid)
    with store.connect() as con:
        runs_ = store.select(con, cid, "runs", "TRUE", (), order="start_time NULLS LAST, run_key")
        ex = store.select(con, cid, "executor_profile", order="added_time NULLS LAST, executor_id") or \
            store.select(con, cid, "executors", order="added_time NULLS LAST, executor_id")
        executors = [{k: e.get(k) for k in ("spark_context_id", "executor_id", "host", "cores", "added_time",
                                            "removed_time", "removed_reason", "removal_category", "heap_mb",
                                            "unified_memory", "tasks", "disk_spill", "idle_ms", "lifetime_ms")}
                     for e in ex]
        # heap size when the event log did not give it: the largest heap the JVM's GC lines report for that executor
        gp = store.dataset_path(cid, "gc_events", required=False)
        if gp is not None and any(not e.get("heap_mb") for e in executors):
            gsch = store.schema(con, gp)
            if "heap_total_mb" in gsch:
                app = {e.get("spark_context_id"): x.get("app_id") for e, x in zip(executors, ex)}
                tot = {(r["app_id"], str(r["executor_id"])): r["mb"] for r in store.rows(
                    con, f"SELECT {'app_id' if 'app_id' in gsch else 'NULL'} AS app_id, executor_id, max(heap_total_mb) AS mb "
                         f"FROM {store.src(gp)} WHERE executor_id IS NOT NULL GROUP BY 1, 2")}
                for e in executors:
                    if not e.get("heap_mb"):
                        mb = tot.get((app.get(e.get("spark_context_id")), str(e["executor_id"]))) or                             next((v for (a_, x_), v in tot.items() if x_ == str(e["executor_id"])), None)
                        if mb:
                            e["heap_mb"], e["heap_from_gc"] = round(mb), True
        minutes: list[dict] = []
        p = store.dataset_path(cid, "spill_shuffle_timeline", required=False)
        if p is not None:
            minutes = store.rows(con, f"SELECT minute, sum(run_ms) AS run_ms, sum(disk_spill) AS disk_spill, "
                                      f"sum(mem_spill) AS mem_spill, sum(tasks) AS tasks, "
                                      f"count(DISTINCT executor_id) AS executors_busy "
                                      f"FROM {store.src(p)} GROUP BY minute ORDER BY minute", [])
        apps = [{k: a.get(k) for k in ("spark_context_id", "app_id", "app_name", "start_time", "end_time")}
                for a in store.select(con, cid, "apps", order="start_time NULLS LAST")]
        info = store.select(con, cid, "cluster_info", limit=1)
        # when each executor ran tasks; gaps under a minute are joined so a long day stays a few bars per executor
        busy: list[dict] = []
        for r in store.select(con, cid, "executor_busy", order="spark_context_id, executor_id, busy_start"):
            last = busy[-1] if busy else None
            if (last and last["executor_id"] == r["executor_id"] and last["spark_context_id"] == r["spark_context_id"]
                    and r["busy_start"] - last["busy_end"] < 60_000):
                last["busy_end"] = max(last["busy_end"], r["busy_end"])
            else:
                busy.append({k: r[k] for k in ("spark_context_id", "executor_id", "busy_start", "busy_end")})
        memory = memory_use(store, con, cid, executors)
        # where the task time went, for the whole cluster and per run (the cause bar)
        causes, run_causes = None, {}
        tp = store.dataset_path(cid, "tasks", required=False)
        if tp is not None:
            tsch = store.schema(con, tp)
            # the time buckets count successful attempts only: a failed attempt's time is its own bucket
            col = lambda c: (f"sum(CASE WHEN NOT failed THEN {c} END)" if c != "disk_spill" else f"sum({c})") if c in tsch else "NULL"
            rk = "run_key" if "run_key" in tsch else "NULL"
            rows = store.rows(con, f"""SELECT {rk} AS run_key, sum(task_ms) AS task_ms, {col('cpu_ms')} AS cpu_ms,
                    {col('gc_ms')} AS gc_ms, {col('fetch_wait_ms')} AS fetch_wait_ms, {col('disk_spill')} AS disk_spill,
                    {col('shuffle_write_ms')} AS shuffle_write_ms,
                    sum(CASE WHEN failed THEN task_ms ELSE 0 END) AS failed_ms FROM {store.src(tp)} GROUP BY 1""")
            causes = _causes(rows)
            run_causes = {r["run_key"]: _causes([r]) for r in rows if r["run_key"] is not None}
    # a job cluster runs one run of one Databricks job; its runs are that job run's task runs
    ci = info[0] if info else {}
    job = {k: ci.get(k) for k in ("cluster_name", "workload_type", "databricks_job_id", "job_run_id", "parent_run_id")}
    # cores up in each minute, from the executors' lifetimes
    for m in minutes:
        t0, t1 = m["minute"], m["minute"] + 60_000
        m["cores"] = sum((e["cores"] or 0) for e in executors
                         if (e["added_time"] is None or e["added_time"] < t1)
                         and (e["removed_time"] is None or e["removed_time"] > t0)) or None
        m["cpu_share"] = round(min(1.0, (m["run_ms"] or 0) / (m["cores"] * 60_000)), 3) if m["cores"] else None
    s = read_summary(store, cid)
    return {"job": job, "apps": apps, "busy": busy, "runs": runs_, "executors": executors, "minutes": minutes, "start": s.get("start_time"),
            "end": s.get("end_time"), "compute": compute_use(executors, minutes, s.get("start_time"), s.get("end_time")),
            "memory": memory, "causes": causes, "run_causes": run_causes}


IDLE_SHARE = 0.10          # a minute in which tasks used under 10% of the cores that were up counts as idle
IDLE_MIN_MINUTES = 3       # idle stretches shorter than this are left out


def compute_use(executors: list[dict], minutes: list[dict], start: int | None, end: int | None) -> dict | None:
    """Revision 14: what the executors' cores were paid for against what tasks used, and the stretches in which the
    executors were up but ran (next to) nothing. `minutes`: per minute run_ms (task time) from the timeline."""
    up = [(e["added_time"] if e["added_time"] is not None else start,
           e["removed_time"] if e["removed_time"] is not None else end, e["cores"] or 0) for e in executors]
    up = [(a, b, c) for a, b, c in up if a is not None and b is not None and b > a]
    if not up:
        return None
    used = {m["minute"]: (m["run_ms"] or 0) for m in minutes}
    t0 = min(a for a, _, _ in up) // 60_000 * 60_000
    t1 = max(b for _, b, _ in up)
    core_up = core_used = 0
    rows = []
    for m in range(t0, t1, 60_000):
        cu = sum(c * max(0, min(b, m + 60_000) - max(a, m)) for a, b, c in up)
        n = sum(1 for a, b, _ in up if a < m + 60_000 and b > m)
        u = min(used.get(m, 0), cu)
        core_up += cu
        core_used += u
        rows.append((m, cu, u, n))
    stretches: list[dict] = []
    cur = None
    for m, cu, u, n in rows:
        if cu > 0 and u < IDLE_SHARE * cu:
            if cur is None:
                cur = {"start": m, "end": m + 60_000, "idle_core_ms": 0, "executors": 0, "cores_max": 0}
            cur["end"] = m + 60_000
            cur["idle_core_ms"] += cu - u
            cur["executors"] = max(cur["executors"], n)
            cur["cores_max"] = max(cur["cores_max"], int(round(cu / 60_000)))
        elif cur is not None:
            stretches.append(cur)
            cur = None
    if cur is not None:
        stretches.append(cur)
    stretches = [x for x in stretches if x["end"] - x["start"] >= IDLE_MIN_MINUTES * 60_000]
    busy_minutes = [m for m, _, u, _ in rows if u > 0]
    return {
        "core_ms_up": int(core_up), "core_ms_used": int(core_used),
        "idle_share": round(1 - core_used / core_up, 3) if core_up else None,
        "idle_core_ms_in_stretches": int(sum(x["idle_core_ms"] for x in stretches)),
        "stretches": sorted(sorted(stretches, key=lambda x: -x["idle_core_ms"])[:12], key=lambda x: x["start"]),
        "stretch_count": len(stretches),
        "executors_from": t0, "executors_to": t1,
        "first_task": busy_minutes[0] if busy_minutes else None,
        "last_task": busy_minutes[-1] + 60_000 if busy_minutes else None,
        "cluster_start": start, "cluster_end": end,
    }


# Revision 15: did a query use adaptive execution (AQE) and Photon, and if not, why. From its final physical plan,
# the cluster's runtime engine and spark.sql.adaptive.enabled.
_CMD_ROOT = re.compile(r"^(Execute\b|CommandResult|Create\w*|Drop\w*|Alter\w*|AppendData|OverwriteByExpression|"
                       r"OverwritePartitionsDynamic|ReplaceTable|SetCatalog|ShowTables|DescribeTable|LocalTableScan|"
                       r"SetCommand|ResetCommand|CacheTable|UncacheTable|AddJars|Refresh\w*)")
_PHOTON_WHY = re.compile(r"Photon does not fully support the query because:?\s*(.+)", re.I)


def _plan_root(plan: str) -> str:
    for line in plan.splitlines():
        t = line.strip().lstrip("+-:* ").strip()
        if t and not t.startswith("=="):
            return t
    return ""


def engine_use(q: dict, runtime_engine: str | None, aqe_setting: str | None) -> dict[str, Any]:
    """{"aqe": {"state": used|no|na, "why"}, "photon": {"state", "why", "share"}} for one query."""
    plan = q.get("final_plan") or ""
    root = _plan_root(plan)
    cmd = bool(_CMD_ROOT.match(root))
    shuffles = "Exchange" in plan or "Subquery" in plan
    from ..parsing.eventlog import readable_description
    streaming = ((readable_description(q.get("description")) or "").startswith("Streaming batch")
                 or "MicroBatchScan" in plan)
    if not plan:
        aqe = {"state": "na", "why": "no plan in the event log"}
    elif "AdaptiveSparkPlan" in plan:
        aqe = {"state": "used", "why": "adaptive plan: Spark sized shuffles and joins at run time"}
    elif (aqe_setting or "").lower() == "false":
        aqe = {"state": "no", "why": "turned off: spark.sql.adaptive.enabled = false"}
    elif root.startswith("LocalTableScan"):
        aqe = {"state": "na", "why": "local data on the driver: nothing runs on the executors"}
    elif cmd:
        aqe = {"state": "na", "why": f"a command ({root.split(' (')[0]}): its reads and writes run as separate queries"}
    elif streaming:
        aqe = {"state": "no", "why": "a streaming micro-batch: Spark does not use adaptive execution for streaming"}
    elif not shuffles:
        aqe = {"state": "na", "why": "no shuffle or subquery: nothing to adapt"}
    else:
        aqe = {"state": "no", "why": "not adaptive although it shuffles (the plan does not say why)"}
    share = q.get("photon_share")
    eng = (runtime_engine or "").upper()
    m = _PHOTON_WHY.search(plan)
    if share:
        photon = {"state": "used", "share": share,
                  "why": "all operators ran in Photon" if share >= 1 else
                  f"{round(share * 100)}% of the operators ran in Photon, the rest fell back to Spark"
                  + (f": {m.group(1).strip()[:300]}" if m else "")}
    elif eng and eng != "PHOTON":
        photon = {"state": "no", "share": share, "why": f"off for this cluster (runtime engine {eng})"}
    elif m:
        photon = {"state": "no", "share": share, "why": m.group(1).strip()[:300]}
    elif not plan or cmd:
        photon = {"state": "na", "share": share, "why": "a command or no plan"}
    else:
        photon = {"state": "no", "share": share, "why": "no Photon operators in the plan"
                  + ("" if eng else " (is Photon on for this cluster?)")}
    return {"aqe": aqe, "photon": photon}


def _engine_settings(store: Store, con, cid: str) -> tuple[str | None, str | None]:
    """(runtime engine, spark.sql.adaptive.enabled) of the cluster, None when unknown."""
    info = (store.select(con, cid, "cluster_info", limit=1) or [{}])[0]
    eng = info.get("runtime_engine")
    aqe = None
    for r in store.select(con, cid, "settings", "key IN (?, ?)",
                          ["spark.sql.adaptive.enabled", "spark.databricks.clusterUsageTags.runtimeEngine"]):
        if r["key"] == "spark.sql.adaptive.enabled" and r.get("value") is not None:
            aqe = r["value"]
        elif r.get("value") and not eng:
            eng = r["value"]
    return eng, aqe


def engine_summary(qs: list[dict], runtime_engine: str | None, aqe_setting: str | None) -> dict[str, Any]:
    """Over a run's queries: how many could use AQE / Photon, how many did, and the commonest reason they did not."""
    out: dict[str, Any] = {}
    uses = [engine_use(q, runtime_engine, aqe_setting) for q in qs]
    for k in ("aqe", "photon"):
        st = [u[k] for u in uses]
        could = [x for x in st if x["state"] != "na"]
        used = [x for x in could if x["state"] == "used"]
        why: dict[str, int] = {}
        for x in could:
            if x["state"] == "no":
                why[x["why"]] = why.get(x["why"], 0) + 1
        out[k] = {"queries": len(qs), "could": len(could), "used": len(used),
                  "why_not": sorted(why.items(), key=lambda kv: -kv[1])[:3]}
    return out


END_ERRORS_BEFORE_MS = 10 * 60_000  # errors logged this long before a run's end belong to "how it ended"
END_ERRORS_AFTER_MS = 60_000


def run_end(store: Store, cid: str, run: str) -> dict[str, Any]:
    """How one run ended, from everything the logs have: its last query, real Spark failures (and jobs adaptive
    execution only re-planned), the errors logged in the minutes before its end, executors that went away while it
    ran, and whether the cluster stopped right after it. The event log never has a notebook's own Python error: when
    Spark saw no failure, the errors around the end are the best lead."""
    store.cluster_dir(cid)
    rr = [r for r in runs(store, cid)["runs"] if r["run_key"] == run]
    if not rr:
        raise NotFound(f"no run {run!r} on cluster {cid!r}")
    r = rr[0]
    t0, t1 = _as_ms(r.get("start_time")), _as_ms(r.get("end_time"))
    with store.connect() as con:
        qs = store.select(con, cid, "sql_queries", "run_key = ?", [run],
                          exclude=("initial_plan", "details"),
                          truncate={"description": 200, "error": 600, "final_plan": 20000},
                          order="end_time DESC NULLS FIRST")
        eng, aqe_set = _engine_settings(store, con, cid)
        jobs = store.select(con, cid, "spark_jobs", "run_key = ?", [run], truncate={"error": 600},
                            order="end_time DESC NULLS FIRST")
        apps = store.select(con, cid, "apps")
        execs = store.select(con, cid, "executors")
        errs: list[dict] = []
        epath = store.dataset_path(cid, "log_errors", required=False)
        if epath is not None and t1 is not None:
            errs = store.rows(con, f"""
                SELECT exception_class, fingerprint, count(*) AS lines, min(ts) AS first, max(ts) AS last,
                       first(substr(message, 1, 300) ORDER BY ts DESC) AS message,
                       first(user_frame ORDER BY ts DESC) FILTER (WHERE user_frame IS NOT NULL) AS user_frame,
                       list(DISTINCT source) FILTER (WHERE source IS NOT NULL) AS sources,
                       list(DISTINCT executor_id) FILTER (WHERE executor_id IS NOT NULL AND executor_id <> '') AS executors
                FROM {store.src(epath)}
                WHERE ts BETWEEN epoch_ms(CAST(? AS BIGINT)) AND epoch_ms(CAST(? AS BIGINT))
                GROUP BY exception_class, fingerprint ORDER BY last DESC LIMIT 8""",
                [max(t0 or 0, t1 - END_ERRORS_BEFORE_MS), t1 + END_ERRORS_AFTER_MS])
        # the same error near the end of the other runs of the same code: one that most runs log is not why this one ended
        all_rs = runs(store, cid)["runs"]
        if errs and epath is not None:
            from ..analysis.runs import run_family
            fam = [x for x in all_rs if run_family(x) == run_family(r) and _as_ms(x.get("end_time"))]
            ends = [(_as_ms(x["end_time"]) - END_ERRORS_BEFORE_MS, _as_ms(x["end_time"]) + END_ERRORS_AFTER_MS) for x in fam]
            fps = [e["fingerprint"] for e in errs]
            hits: dict[str, set] = {f: set() for f in fps}
            if ends:
                lo, hi = min(a for a, _ in ends), max(b for _, b in ends)
                ph = ", ".join("?" for _ in fps)
                for x in store.rows(con, f"SELECT fingerprint, epoch_ms(ts) AS t FROM {store.src(epath)} "
                                         f"WHERE fingerprint IN ({ph}) AND ts BETWEEN epoch_ms(CAST(? AS BIGINT)) "
                                         "AND epoch_ms(CAST(? AS BIGINT))", [*fps, lo, hi]):
                    for i, (a, b) in enumerate(ends):
                        if a <= x["t"] <= b:
                            hits[x["fingerprint"]].add(i)
            for e in errs:
                e["family_runs"], e["runs_with"] = len(fam), len(hits.get(e["fingerprint"], ()))
    failed_j = [j for j in jobs if j.get("result") == "JobFailed"]
    replanned = [j for j in jobs if j.get("result") == "JobReplanned"]
    failed_q = [q for q in qs if q.get("status") == "failed"]
    open_q = [q for q in qs if q.get("end_time") is None]
    last_q = next((q for q in qs if q.get("end_time") is not None), None)
    app_end = max((_as_ms(a.get("end_time")) or 0 for a in apps), default=0) or None
    gone = [e for e in execs if e.get("removed_time") and t0 and t1
            and t0 <= _as_ms(e["removed_time"]) <= t1 + END_ERRORS_AFTER_MS]
    lines: list[dict] = []  # {tone, text}: the story of the end, most telling first

    def say(tone, text):
        lines.append({"tone": tone, "text": text})

    if failed_q or failed_j:
        first = (failed_q or failed_j)[-1]
        what = f"query {first['sql_execution_id']}" if failed_q else f"Spark job {first['spark_job_id']}"
        say("bad", f"Spark failed: {what} ended with an error" + (f": {first['error'][:300]}" if first.get("error") else "."))
    elif open_q or r.get("status") == "incomplete":
        say("warn", "The event log has no end for this run's last Spark work: it was still running when the log ends "
                    "(the cluster stopped, or the log was cut).")
    else:
        say("ok", f"Spark saw no failure: all {len(qs)} queries and {len(jobs) - len(replanned)} Spark jobs finished. "
                  "If Databricks reports this task as failed, the error happened outside Spark (Python code, the "
                  "driver, a timeout or a cancel) and is not in the event log: check the errors below and the "
                  "task's output in Databricks.")
    if replanned:
        say("info", f"{len(replanned)} Spark job{'s were' if len(replanned) > 1 else ' was'} cancelled by adaptive query "
                    "execution after it re-planned the query: normal, not a failure.")
    if app_end and t1 and 0 <= app_end - t1 <= 2 * 60_000:
        say("warn", f"The cluster stopped {_fmt_ms(app_end - t1) if app_end - t1 >= 60_000 else f'{round((app_end - t1) / 1000)} s'} "
                    "after this run's last Spark work: the run may have been cut off by the cluster ending "
                    "(job timeout, cancel, or auto-termination).")
    if gone:
        cats = {}
        for e in gone:
            cats[e.get("removal_category") or "other"] = cats.get(e.get("removal_category") or "other", 0) + 1
        bad = {k: v for k, v in cats.items() if k in ("oom", "lost", "killed")}
        words = {"autoscale": "removed by autoscaling", "termination": "stopped with the cluster", "oom": "out of memory",
                 "lost": "lost (spot or node)", "killed": "killed", "other": "other reason"}
        say("bad" if bad else "info", f"{_plural(len(gone), 'executor')} went away while it ran ("
            + ", ".join(f"{v} {words.get(k, k)}" for k, v in cats.items()) + ")"
            + (": their tasks and cached data were lost." if bad else "."))
    if errs:
        say("warn", f"{_plural(sum(e['lines'] for e in errs), 'error line')} ({_plural(len(errs), 'kind')}) were logged in the "
                    f"{END_ERRORS_BEFORE_MS // 60_000} minutes before it ended (below, newest first).")
    return {"run_key": run, "status": r.get("status"), "start_time": t0, "end_time": t1,
            "lines": lines, "last_query": last_q and {k: last_q.get(k) for k in (
                "spark_context_id", "sql_execution_id", "description", "status", "start_time", "end_time", "duration_ms")},
            "queries": len(qs), "spark_jobs": len(jobs), "failed_queries": len(failed_q), "failed_jobs": len(failed_j),
            "replanned_jobs": len(replanned), "errors": errs, "executors_gone": [
                {k: e.get(k) for k in ("executor_id", "removed_time", "removal_category", "removed_reason")} for e in gone],
            "cluster_end": app_end, "engine": engine_summary(qs, eng, aqe_set)}


GROUP_TOP = 15  # problems listed per kind on a group summary


def _as_ms(v) -> int | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, _dt.datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=_dt.timezone.utc)
        return int(v.timestamp() * 1000)
    return None


def group_view(store: Store, cid: str, run_keys: Sequence[str]) -> dict[str, Any]:
    """A group of runs (e.g. all task runs of one notebook) at a glance: per finding kind and per error kind how many
    of the runs had it, and per run its status, findings and errors. Details stay on the one-run pages."""
    keys = [k for k in dict.fromkeys(run_keys) if k]
    store.cluster_dir(cid)
    all_runs = runs(store, cid)["runs"]
    rs = [r for r in all_runs if r["run_key"] in set(keys)]
    if not rs:
        return {"runs": [], "findings": [], "errors": [], "note": "None of these runs are on this cluster."}
    win = [(r["run_key"], _as_ms(r.get("start_time")), _as_ms(r.get("end_time"))) for r in rs]
    with store.connect() as con:
        ph = ", ".join("?" for _ in rs)
        fs = store.select(con, cid, "findings", f"run_key IN ({ph})", [r["run_key"] for r in rs],
                          truncate={"evidence": 300, "fix": 300})
        lo = min((a for _, a, _b in win if a is not None), default=None)
        hi = max((b for _, _a, b in win if b is not None), default=None)
        errs = store.select(con, cid, "log_errors", exclude=("top_frames", "file_path"),
                            truncate={"message": 240}) if lo is not None else []
    per_run = {r["run_key"]: {"findings": 0, "errors": 0} for r in rs}
    # findings: one line per kind (category + signal), the runs that had it and the worst severity
    fk: dict[tuple, dict] = {}
    for f in fs:
        k = (f.get("category"), f.get("signal"))
        g = fk.setdefault(k, {"category": f.get("category"), "signal": f.get("signal"), "severity": f.get("severity"),
                              "count": 0, "runs": set(), "example": f.get("evidence"), "fix": f.get("fix")})
        g["count"] += 1
        g["runs"].add(f["run_key"])
        if SEVERITY_RANK.get(f.get("severity"), 9) < SEVERITY_RANK.get(g["severity"], 9):
            g["severity"], g["example"], g["fix"] = f.get("severity"), f.get("evidence"), f.get("fix")
        per_run[f["run_key"]]["findings"] += 1
    # errors: log lines carry only a time; a line belongs to every run of the group that was running then
    ek: dict[tuple, dict] = {}
    for e in errs:
        t = _as_ms(e.get("ts"))
        if t is None or t < lo or t > hi:
            continue
        hit = [k for k, a, b in win if a is not None and a <= t <= (b if b is not None else hi)]
        if not hit:
            continue
        k = (e.get("exception_class"), e.get("fingerprint"))
        g = ek.setdefault(k, {"exception_class": e.get("exception_class"), "message": e.get("message"),
                              "source": e.get("source"), "count": 0, "runs": set(), "first": t, "last": t})
        g["count"] += 1
        g["runs"].update(hit)
        g["first"], g["last"] = min(g["first"], t), max(g["last"], t)
        for h in hit:
            per_run[h]["errors"] += 1
    failed = {r["run_key"] for r in rs if r.get("status") == "failed"}

    def out(g: dict) -> dict:
        r = sorted(g.pop("runs"))
        return dict(g, runs=len(r), failed_runs=len([x for x in r if x in failed]), run_keys=r[:50])

    findings = sorted((out(g) for g in fk.values()),
                      key=lambda g: (SEVERITY_RANK.get(g["severity"], 9), -g["failed_runs"], -g["runs"]))
    errors = sorted((out(g) for g in ek.values()), key=lambda g: (-g["failed_runs"], -g["runs"], -g["count"]))
    run_rows = [{"run_key": r["run_key"], "label": r.get("label"), "status": r.get("status"),
                 "start_time": r.get("start_time"), "duration_ms": r.get("duration_ms"),
                 "vs_typical": r.get("vs_typical"), **per_run[r["run_key"]]} for r in rs]
    run_rows.sort(key=lambda r: (r["status"] != "failed", -(r["findings"] or 0)))
    return {"runs": run_rows, "findings": findings[:GROUP_TOP], "errors": errors[:GROUP_TOP],
            "more_findings": max(0, len(findings) - GROUP_TOP), "more_errors": max(0, len(errors) - GROUP_TOP),
            "note": None}


BURST_GAP_MS = 2 * 60_000  # runs starting at most this far apart belong to one burst
BURST_MIN_RUNS = 5


def _start_bursts(runs: list[dict]) -> list[dict]:
    """Groups of BURST_MIN_RUNS or more runs that each started within BURST_GAP_MS of the previous one."""
    rs = sorted((r for r in runs if r.get("start_time")), key=lambda r: r["start_time"])
    out: list[dict] = []
    cur: list[dict] = []
    for r in rs:
        if cur and r["start_time"] - cur[-1]["start_time"] > BURST_GAP_MS:
            out.append(cur)
            cur = []
        cur.append(r)
    if cur:
        out.append(cur)
    return [{"first": g[0]["start_time"], "last": g[-1]["start_time"], "runs": g}
            for g in out if len(g) >= BURST_MIN_RUNS]


def _plural(n: int, word: str) -> str:
    return f"{n} {word}" + ("" if n == 1 else "s")


def _hhmm(ms: int) -> str:
    return _dt.datetime.fromtimestamp(ms / 1000, _dt.timezone.utc).strftime("%H:%M")


ADVICE_STAGES = 50


def settings_view(store: Store, cid: str) -> dict[str, Any]:
    """Revision 14: the settings that decide speed and cost, and what each meant for this cluster's work: a list of
    outliers, each with the evidence from the data and what to change."""
    from ..analysis.aggregate import storage_bytes
    from ..settings_catalog import SETTINGS, TAGS, short_key, size_bytes

    store.cluster_dir(cid)
    with store.connect() as con:
        rows = store.select(con, cid, "settings", order="spark_context_id, key")
        stages = store.select(con, cid, "stages", "tasks > 0", (),
                              exclude=("details", "stage_name", "rdd_names", "rdd_scopes", "parent_ids"))
        qs = store.select(con, cid, "sql_queries", exclude=("final_plan", "initial_plan", "details"))
        info = (store.select(con, cid, "cluster_info", limit=1) or [{}])[0]
        newer = store.select(con, cid, "findings", "category IN ('cores_full', 'merge_rewrite', 'waited_for_cores')", ())             if store.dataset_path(cid, "findings", required=False) is not None else []
    view = cluster_view(store, cid)
    # one value per setting over the cluster's apps: the first that was set, else the default
    order = {k: i for i, (k, *_r) in enumerate(SETTINGS)}
    merged: dict[str, dict] = {}
    for r in rows:
        m = merged.get(r["key"])
        if m is None or (m["source"] == "default" and r["source"] != "default"):
            merged[r["key"]] = dict(r, label=short_key(r["key"]))
        elif r.get("session_values"):
            m["session_values"] = sorted(set(m.get("session_values") or []) | set(r["session_values"]))
    settings = sorted(merged.values(), key=lambda r: order.get(r["key"], 999))
    defaults = {k: d for k, _, d, _ in SETTINGS}

    def val(key: str) -> str | None:
        r = merged.get(key)
        if not r:
            return defaults.get(key)
        return r.get("value") if r.get("value") is not None else r.get("default")

    MB, GB = 1 << 20, 1 << 30
    advice: list[dict] = []

    labels = {r["run_key"]: r.get("label") or r.get("subject") or r["run_key"] for r in view["runs"] if r.get("run_key")}

    def refs(xs, by):
        """Revision 18: the stages behind a piece of advice, biggest first, to open each one."""
        out = []
        for x in sorted(xs, key=lambda x: -(by(x) or 0))[:ADVICE_STAGES]:
            out.append({k: x.get(k) for k in ("spark_context_id", "stage_id", "stage_attempt", "spark_job_id", "sql_execution_id",
                                              "run_key", "duration_ms", "tasks", "input_bytes", "shuffle_read", "shuffle_write",
                                              "disk_spill", "max_task_bytes_in", "wmed_task_bytes_in", "storage_bytes",
                                              "df_cache_bytes")})
            out[-1]["run_label"] = labels.get(x.get("run_key"))
        return {"list": out, "count": len(xs)}

    def run_refs(keys):
        return [{"run_key": k, "label": labels.get(k, k)} for k in dict.fromkeys(k for k in keys if k)][:ADVICE_STAGES]

    def sev_rank(x):
        return {"high": 0, "medium": 1, "info": 2}.get(x, 3)

    def add(severity, key, title, facts, cause, fixes, stages=None, runs=None, queries=None):
        """Revision 18: each piece of advice as points: what the data shows, the likely cause, what to change, and
        links to the stages, runs and queries it is about. evidence / change keep the same as running text."""
        facts = [f for f in facts if f]
        advice.append({"severity": severity, "key": key, "title": title, "facts": facts, "cause": cause, "fixes": fixes,
                       "evidence": " ".join(f if f.endswith(".") else f + "." for f in facts), "change": " ".join(fixes),
                       "stages": (stages or {}).get("list", []), "stage_count": (stages or {}).get("count", 0),
                       "runs": runs or [], "queries": queries or []})

    # 1. shuffle partitions: big stages that ran exactly the configured number of tasks
    sp = val("spark.sql.shuffle.partitions")
    auto = ((val("spark.databricks.adaptive.autoOptimizeShuffle.enabled") or "").lower() == "true"
            or (sp or "").lower() == "auto")
    n = int(sp) if sp and sp.isdigit() else None
    if n and not auto:
        at_n = [x for x in stages if x.get("tasks") == n and (x.get("shuffle_read") or 0) > 0]
        big = [x for x in at_n if max(x.get("wmed_task_bytes_in") or 0, x.get("p50_task_bytes_in") or 0) >= 128 * MB]
        if big:
            need = max((x.get("shuffle_read") or 0) for x in big)
            suggest = max(n, int(math.ceil(need / (128 * MB) / 100.0)) * 100)
            spill = sum(x.get("disk_spill") or 0 for x in big)
            dflt = merged.get("spark.sql.shuffle.partitions", {}).get("source") in (None, "default")
            add("high" if spill >= GB else "medium", "spark.sql.shuffle.partitions",
                f"{len(big)} big shuffle {'stage was' if len(big) == 1 else 'stages were'} cut into only {n} tasks",
                [f"{len(at_n)} {'stage' if len(at_n) == 1 else 'stages'} ran exactly {n} tasks, {'the default' if dflt else 'the setting'}",
                 f"In {len(big)} of them half the data was in tasks of 128 MB or more",
                 f"The biggest read {_fmt_bytes(need)} of shuffle",
                 f"They spilled {_fmt_bytes(spill)} to disk" if spill else None],
                f"spark.sql.shuffle.partitions = {n} is too few for this much data: each task gets more than fits in its memory.",
                [f"Set spark.sql.shuffle.partitions ≈ {suggest} for these jobs (biggest shuffle read ÷ 128 MB)",
                 "or turn on spark.databricks.adaptive.autoOptimizeShuffle.enabled (shuffle.partitions = auto) so Databricks sizes it"],
                refs(big, lambda x: x.get("shuffle_read")))
    # 2. adaptive execution off
    if (val("spark.sql.adaptive.enabled") or "true").lower() == "false":
        add("high", "spark.sql.adaptive.enabled", "Adaptive query execution is off",
            ["spark.sql.adaptive.enabled = false"],
            "Without it Spark can neither merge tiny shuffle partitions nor split skewed join partitions.",
            ["Remove the setting (it is on by default)"])
    skewed = [x for x in stages if (x.get("data_skew") or 0) >= 5 and (x.get("max_task_bytes_in") or 0) >= 256 * MB]
    if skewed and (val("spark.sql.adaptive.skewJoin.enabled") or "true").lower() == "false":
        add("high", "spark.sql.adaptive.skewJoin.enabled", "Skew-join handling is off while stages are skewed",
            [f"{len(skewed)} stages had a task with at least 5× the median data and 256 MB or more",
             "spark.sql.adaptive.skewJoin.enabled = false"],
            "A few keys hold most of the data and Spark is not allowed to split them.",
            ["Remove spark.sql.adaptive.skewJoin.enabled = false"], refs(skewed, lambda x: x.get("max_task_bytes_in")))
    # 3. file-reading tasks far above the split size: files that cannot be split
    mpb = size_bytes(val("spark.sql.files.maxPartitionBytes")) or 128 * MB
    # reads from files only: a stage that read mostly a DataFrame cache has no files to split
    unsplit = [x for x in stages if storage_bytes(x) > 0 and not x.get("shuffle_read")
               and (x.get("df_cache_bytes") or 0) < storage_bytes(x) and (x.get("wmed_task_bytes_in") or 0) >= 2 * mpb]
    if unsplit:
        add("medium", "spark.sql.files.maxPartitionBytes",
            f"{len(unsplit)} file-reading {'stage' if len(unsplit) == 1 else 'stages'} had tasks far above the split size",
            [f"Half their data was read by tasks of up to {_fmt_bytes(max(x['wmed_task_bytes_in'] for x in unsplit))}",
             f"The split size is {_fmt_bytes(mpb)}"],
            "The files cannot be split: gzip, one huge file, or one huge row group.",
            ["Store the input as splittable Parquet / Delta with files of 128 MB – 1 GB (OPTIMIZE)",
             "or repartition right after reading"],
            refs(unsplit, lambda x: x.get("wmed_task_bytes_in")))
    # 4. spill: execution memory per task
    spill = sum(x.get("disk_spill") or 0 for x in stages)
    if spill >= GB:
        heap = None
        hm = sorted(e["heap_mb"] for e in view["executors"] if e.get("heap_mb"))
        cores = sorted(e["cores"] for e in view["executors"] if e.get("cores"))
        if hm and cores:
            heap = (hm[len(hm) // 2] * MB, cores[len(cores) // 2])
        try:
            frac = float(val("spark.memory.fraction") or 0.6)
        except ValueError:
            frac = 0.6
        worst = sorted([x for x in stages if (x.get("disk_spill") or 0) > 0], key=lambda x: -(x.get("disk_spill") or 0))[:3]
        spilled = [x for x in stages if (x.get("disk_spill") or 0) > 0]
        add("high" if spill >= 50 * GB else "medium", "spark.executor.memory", f"{_fmt_bytes(spill)} spilled to disk",
            [f"{_plural(len(spilled), 'stage')} spilled; most in "
             + ", ".join(f"stage {x['stage_id']} ({_fmt_bytes(x.get('disk_spill'))})" for x in worst),
             (f"Each running task gets about {_fmt_bytes(heap[0] * frac / heap[1])} of memory "
              f"({_fmt_bytes(heap[0])} heap × {frac} ÷ {heap[1]} cores)") if heap else None],
            "Tasks held more data than their share of executor memory, so they wrote the rest to local disk and read it back.",
            ["First make the tasks smaller: more shuffle partitions",
             "If the data per task is already small: a memory-optimised worker type, or fewer cores per executor"],
            refs(spilled, lambda x: x.get("disk_spill")))
    # 4b. a DataFrame cache bigger than memory: the cause of the out-of-memory and GC findings it brings, so it goes
    # first, and the memory findings go under it (its key names memory)
    cache_adv = None
    with store.connect() as con:
        mem_f = store.select(con, cid, "findings", "category IN ('dataframe_cache', 'oom_site', 'gc_stuck')", ()) \
            if store.dataset_path(cid, "findings", required=False) is not None else []
    caches = [x for x in mem_f if x["category"] == "dataframe_cache"]
    if caches:
        facts = [x.strip().rstrip(".") for f in caches for x in re.split(r"(?<=\.)\s+", f["evidence"] or "") if x.strip()]
        cache_ooms = [x for x in mem_f if x["category"] == "oom_site" and "DataFrame cache" in (x.get("entity") or "")]
        facts += [x["evidence"] for x in cache_ooms[:3]]
        stuck = [x for x in mem_f if x["category"] == "gc_stuck"]
        if stuck:
            facts.append(f"{', '.join(x['entity'] for x in stuck[:4])} {'was' if len(stuck) == 1 else 'were'} stuck in GC: "
                         "the heap stayed full after every collection")
        ids = {(x.get("spark_context_id"), x.get("stage_id")) for x in cache_ooms}
        add("high", "spark.executor.memory", "DataFrame cache larger than memory", facts,
            "The code caches (cache() / persist()) more data than the executors' memory holds: blocks do not fit, are "
            "dropped and rebuilt, and the heap fills until executors stall in GC or run out of memory. More shuffle "
            "partitions will not help.",
            ["Cache only the columns that are reused, or write an intermediate Delta table and read it back",
             "Unpersist the cache when it is no longer needed, and drop count() calls that only fill it",
             "If it must stay cached: memory-optimized workers"],
            refs([x for x in stages if (x.get("spark_context_id"), x.get("stage_id")) in ids], lambda x: x.get("duration_ms")),
            queries=[{"spark_context_id": x.get("spark_context_id"), "sql_execution_id": x.get("sql_execution_id"),
                      "run_key": x.get("run_key"), "label": labels.get(x.get("run_key"))} for x in caches + cache_ooms
                     if x.get("sql_execution_id") is not None])
        cache_adv = advice[-1]
    # 5. sharing: runs at once on the same executors
    runs_ = view["runs"]
    ev = sorted([(r["start_time"], 1) for r in runs_ if r.get("start_time")]
                + [(r["end_time"], -1) for r in runs_ if r.get("end_time")])
    peak = cur = 0
    for _, k in ev:
        cur += k
        peak = max(peak, cur)
    if peak > 1:
        mode = val("spark.scheduler.mode") if rows else None
        pre = (val("spark.databricks.preemption.enabled") or "").lower() == "true"
        add("info", "spark.scheduler.mode", f"Up to {peak} runs shared the executors at once",
            [f"Scheduler mode {mode}{', preemption on' if pre else ''}" if mode else None,
             "Each stage page shows who else was on its executors"],
            "The runs' stages took turns on the same cores, so a run's time also depends on what ran next to it.",
            ["For runs whose time matters: give the big ones their own job cluster", "or run fewer at once"])
    # 5b. bursts: many runs started within a couple of minutes (a for-each task at high concurrency), and what the
    # executors did around it (autoscaling that removed executors just before, or added them only minutes later)
    workers = [e for e in view["executors"] if str(e.get("executor_id")) != "driver" and e.get("cores")]
    for b in _start_bursts(runs_):
        t0 = b["first"]
        up = [e for e in workers if (e.get("added_time") or 0) <= t0
              and (e.get("removed_time") is None or e["removed_time"] > t0)]
        cores = sum(e["cores"] for e in up)
        gone = [e for e in workers if e.get("removed_time") and t0 - BURST_GAP_MS <= e["removed_time"] <= b["last"]
                + BURST_GAP_MS and e.get("removal_category") == "autoscale"]
        late = sorted(e["added_time"] for e in workers if e.get("added_time") and e["added_time"] > t0
                      and e["added_time"] <= t0 + 30 * 60_000)
        long_ = [r for r in b["runs"] if (r.get("duration_ms") or 0) >= 5 * 60_000]
        failed = sum(1 for r in b["runs"] if r.get("status") == "failed")
        heavy = len(long_) >= 5 and (not cores or len(long_) * 2 > cores)
        spread = (b["last"] - t0) / 1000
        add("high" if heavy and (failed or gone) else "medium" if heavy else "info", None,
            f"{len(b['runs'])} runs started within " + (f"{round(spread)} s" if spread < 120 else _fmt_ms(spread * 1000)),
            [f"They started at {_hhmm(t0)} UTC" + (f" on {_plural(len(up), 'executor')} with {cores} cores" if cores else ""),
             f"{len(long_)} of them ran 5 minutes or more" if long_ else None,
             f"{failed} failed" if failed else None,
             f"Autoscaling removed {_plural(len(gone), 'executor')} as they started" if gone else None,
             (f"More executors came only {_fmt_ms(late[0] - t0) if late[0] - t0 >= 60_000 else 'seconds'} later") if late else None],
            "Started at once, they split the same cores and memory: each runs slower, spills more and holds its memory longer.",
            ["Lower the for-each task's concurrency (or the job's max concurrent runs) to about the cores the runs can share",
             "or stagger the starts",
             "With autoscaling: raise the minimum workers for this schedule so executors are there before the burst"],
            runs=run_refs(r["run_key"] for r in sorted(b["runs"], key=lambda r: -(r.get("duration_ms") or 0))))
    # 5c. Revision 17: the cores were full (runs waited for them) and MERGEs that read far more than they merge
    for f in [x for x in newer if x["category"] == "cores_full"][:1]:
        waited = sorted((x for x in newer if x["category"] == "waited_for_cores"),
                        key=lambda x: ({"high": 0, "medium": 1}.get(x["severity"], 2)))
        add(f["severity"], TAGS + "clusterWorkers", "The runs waited for cores more than they ran",
            [x.strip().rstrip(".") for x in re.split(r"(?<=\.)\s+", f["evidence"]) if x.strip()],
            "The cores were the bottleneck, not the data: more runs were started than the executors had cores for, so their stages queued.",
            ["Give the job more cores when the runs start: raise the job cluster's minimum workers (autoscaling adds workers only minutes later), and the maximum",
             "or start fewer at once: the for-each task's concurrency, or groups of runs one after another",
             "Job compute bills per core-hour used: more cores for the same work costs about the same and finishes sooner"],
            runs=run_refs(x.get("run_key") for x in waited))
    merges = [x for x in newer if x["category"] == "merge_rewrite"]
    if merges:
        facts = []
        for x in merges[:5]:
            e = x["evidence"]
            q = next((y for y in qs if y.get("sql_execution_id") == x.get("sql_execution_id")
                      and y.get("spark_context_id") == x.get("spark_context_id")), {})
            files = re.search(r"rewrote (\d+) files", e)
            src = re.search(r"source of ([\d.,]+ \w+) \(([\d,]+)x less\)", e)
            facts.append(f"Query {x['sql_execution_id']}: read {_fmt_bytes(q.get('input_bytes'))} of the target"
                         + (f" to merge {src.group(1)} ({src.group(2)}× less)" if src else "")
                         # Spark labels the last step "rewriting N files"; with deletion vectors it writes only the
                         # changed rows, so say what it wrote when that is far below what it read
                         + (f", wrote only {_fmt_bytes(q.get('output_bytes'))} (changed rows: deletion vectors)"
                            if q.get("output_bytes") and q.get("input_bytes") and q["output_bytes"] < 0.3 * q["input_bytes"]
                            else f", rewrote {int(files.group(1)):,} files" if files else "")
                         + (f", spilled {_fmt_bytes(q.get('disk_spill'))}" if q.get("disk_spill") else ""))
        if len(merges) > 5:
            facts.append(f"and {len(merges) - 5} more")
        add("high" if any(x["severity"] == "high" for x in merges) else "medium", None,
            f"{_plural(len(merges), 'MERGE')} read far more of the target than they merged",
            facts,
            "Delta could not skip files: the ON condition does not narrow the target, so every file that might match is read and rewritten.",
            ["Add the target's partition or clustering column to the MERGE ON condition (e.g. t.load_date >= the oldest date in the source)",
             "Cluster the target on the merge key (Liquid Clustering, or ZORDER BY)",
             "If deletion vectors are off, turn them on so a matched row does not rewrite its whole file (already on when a MERGE wrote far less than it read)"],
            queries=[{"spark_context_id": x.get("spark_context_id"), "sql_execution_id": x.get("sql_execution_id"),
                      "run_key": x.get("run_key"), "label": labels.get(x.get("run_key"))} for x in merges])
    # 6. Photon
    eng = (info.get("runtime_engine") or val(TAGS + "runtimeEngine") or "").upper()
    ph = [q.get("photon_share") for q in qs if q.get("photon_share") is not None]
    if eng != "PHOTON" and (not ph or max(ph) == 0):
        heavy = [q for q in qs if any(w in (q.get("description") or "").upper() for w in ("MERGE", "JOIN"))
                 or (q.get("input_bytes") or 0) + (q.get("shuffle_read") or 0) >= 10 * GB]
        if heavy:
            add("info", TAGS + "runtimeEngine", "Photon was not used",
                [f"No query plan has a Photon operator ({eng.lower() or 'standard'} runtime)",
                 f"{len(heavy)} queries were MERGEs or joins or read 10 GB or more"],
                "The job runs on the standard engine.",
                ["Try the job on a Photon runtime: MERGE, joins and aggregations often run 2–3× faster, at a higher DBU rate"])
    # 7. paying for executors that ran nothing
    cu = view.get("compute")
    if cu and cu["idle_share"] is not None and cu["idle_share"] >= 0.3:
        fixed = val(TAGS + "clusterMaxWorkers") is None or val(TAGS + "clusterMinWorkers") == val(TAGS + "clusterMaxWorkers")
        lead = (cu["first_task"] - cu["executors_from"]) if cu["first_task"] else None
        tail = (cu["executors_to"] - cu["last_task"]) if cu["last_task"] else None
        facts = [f"{round(cu['idle_share'] * 100)}% of the executors' core time ran no task "
                 f"({(cu['core_ms_up'] - cu['core_ms_used']) / 3_600_000:.1f} of {cu['core_ms_up'] / 3_600_000:.1f} core-hours)"]
        if cu["stretch_count"]:
            facts.append(f"{cu['stretch_count']} stretches of {IDLE_MIN_MINUTES} minutes or more with next to nothing running")
        if tail and tail >= 5 * 60_000:
            facts.append(f"Executors stayed up {_fmt_ms(tail)} after the last task")
        if lead and lead >= 5 * 60_000:
            facts.append(f"They were up {_fmt_ms(lead)} before the first task")
        fixes = (["Turn on autoscaling, or use fewer workers: this cluster had a fixed size"] if fixed else
                 ["Lower the minimum workers", "or set spark.databricks.aggressiveWindowDownS so idle workers go sooner"])
        at = val(TAGS + "autoTerminationMinutes")
        if (info.get("workload_type") or "").upper() != "AUTOMATED" and at and at.isdigit() and int(at) > 20:
            fixes.append(f"Auto-termination is {at} minutes: lower it for an all-purpose cluster")
        add("high" if cu["idle_share"] >= 0.5 else "medium", TAGS + "clusterWorkers", "Paying for executors that ran nothing",
            facts, "More workers were up than the work needed, or they stayed up after it.", fixes)
    # 8. stragglers that did not have more data: a slow node, not skew
    strag = [x for x in stages if (x.get("max_task_ms") or 0) >= 300_000 and (x.get("p50_task_ms") or 0) > 0
             and x["max_task_ms"] >= 10 * x["p50_task_ms"]
             and (not x.get("max_task_bytes_in") or not x.get("p50_task_bytes_in")
                  or x["max_task_bytes_in"] < 2 * x["p50_task_bytes_in"])]
    if strag and (val("spark.speculation") or "false").lower() == "false":
        add("info", "spark.speculation", f"{len(strag)} stages waited on a slow task that did not have more data",
            ["The slowest task took 10× the median and over 5 minutes", "It read at most twice the median data"],
            "A slow or busy node rather than skew.",
            ["spark.speculation = true starts a copy of such a task elsewhere (only for tasks that are safe to run twice)"],
            refs(strag, lambda x: x.get("max_task_ms")))
    # the same lookup table read again by many runs: a table no run writes, read by 5 runs or more, that cost a few
    # minutes in all (each run pays for the full read again)
    written = {t for q in qs for t in (q.get("tables_written") or [])}
    by_table: dict[str, list[dict]] = {}
    for q in qs:
        for t in set(q.get("tables_read") or []):
            if t in written or "_delta_log" in t or t.startswith("jdbc:") or "/" in t:
                continue
            by_table.setdefault(t, []).append(q)
    for t, rq in sorted(by_table.items(), key=lambda kv: -sum(q.get("duration_ms") or 0 for q in kv[1])):
        rk = {q.get("run_key") for q in rq if q.get("run_key")}
        took = sum(q.get("duration_ms") or 0 for q in rq)
        if len(rk) < 5 or took < 120_000:
            continue
        rows_ = [q.get("input_records") or 0 for q in rq]
        med = sorted(rows_)[(len(rows_) - 1) // 2]
        add("medium" if took >= 600_000 else "info", None, f"{len(rk)} runs each read {t} again",
            [f"{len(rq)} queries in {len(rk)} runs read it, {_fmt_dur(took)} in all"
             + (f", about {int(med):,} rows each time" if med else ""),
             "No run writes it: it is a lookup every run pays for again"],
            "Each run reads the whole table for a few values (a watermark, a mapping), and nothing keeps it between runs.",
            ["Read it once in the parent job and pass the values to the runs (task values or parameters)",
             "or filter it to the rows each run needs (its key in the WHERE), so Delta skips most files",
             "or keep it small: compact it (OPTIMIZE) and drop the history it does not need"],
            runs=run_refs(q.get("run_key") for q in rq),
            queries=[{"spark_context_id": q["spark_context_id"], "sql_execution_id": q["sql_execution_id"],
                      "run_key": q.get("run_key"), "label": labels.get(q.get("run_key"))} for q in rq[:ADVICE_STAGES]])
        if len([a for a in advice if a["title"].endswith(" again")]) >= 3:
            break
    # one concurrency item per cluster: the bursts and the sharing are the same story as "waited for cores"
    cores = next((a for a in advice if a["key"] == TAGS + "clusterWorkers"), None)
    if cores is not None:
        for a in [a for a in advice if a is not cores and (a["key"] == "spark.scheduler.mode"
                                                           or re.match(r"\d+ runs started within", a["title"]))]:
            cores["facts"].append(a["title"])
            cores["runs"] = (cores["runs"] + [r for r in a["runs"] if r not in cores["runs"]])[:ADVICE_STAGES]
            if sev_rank(a["severity"]) < sev_rank(cores["severity"]):
                cores["severity"] = a["severity"]
            advice.remove(a)
        cores["evidence"] = " ".join(f if f.endswith(".") else f + "." for f in cores["facts"])
    # cause before symptom: spill and big tasks inside a MERGE's queries are the MERGE's, so it goes first
    merge_q = {(q["spark_context_id"], q["sql_execution_id"]) for a in advice if a["title"].endswith("than they merged")
               for q in a["queries"]}
    for a in advice:
        mine = [x for x in a["stages"] if (x.get("spark_context_id"), x.get("sql_execution_id")) in merge_q]
        if merge_q and mine and a["key"] in ("spark.executor.memory", "spark.sql.shuffle.partitions"):
            a["facts"].append(f"{len(mine)} of these stages are inside the MERGEs above: fixing the MERGE removes most of it")
            a["evidence"] = " ".join(f if f.endswith(".") else f + "." for f in a["facts"])
    first = lambda a: 0 if a["title"].endswith("than they merged") else 1 if a is cores else 2  # noqa: E731
    advice.sort(key=lambda a: (sev_rank(a["severity"]), first(a)))
    if cache_adv is not None:  # the root cause of the memory findings ranks first
        advice.insert(0, advice.pop(advice.index(cache_adv)))
    envs = val(TAGS + "clusterNumSparkEnvVars")
    return {"settings": settings, "advice": advice, "env_vars": int(envs) if envs and envs.isdigit() else None,
            "has_settings": bool(rows), "compute": cu}


def _fmt_dur(ms) -> str:
    s_ = int(round((ms or 0) / 1000))
    return f"{s_ // 3600}h {s_ % 3600 // 60}m" if s_ >= 3600 else f"{s_ // 60}m {s_ % 60}s" if s_ >= 60 else f"{s_}s"


def _fmt_bytes(v) -> str:
    v = float(v or 0)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if v < 1024 or u == "TB":
            return f"{v:.0f} {u}" if u in ("B", "KB") or v >= 100 else f"{v:.1f} {u}"
        v /= 1024
    return f"{v:.1f} TB"


def _fmt_ms(ms) -> str:
    m = int((ms or 0) // 60_000)
    return f"{m // 60} h {m % 60} min" if m >= 60 else f"{m} min"


# Task metrics sent with each Gantt task: the UI spreads them over the task's run time to show
# per-executor CPU, memory, shuffle, spill and GC over time.
GANTT_TASK_METRICS = ("run_ms", "cpu_ms", "gc_ms", "peak_mem", "mem_spill", "disk_spill", "input_bytes",
                      "shuffle_read", "shuffle_write", "fetch_wait_ms")


def gantt(store: Store, cid: str, ctx: str | None, max_tasks: int = 20000, run: str | None = None,
          start: int | None = None, end: int | None = None) -> dict[str, Any]:
    """Jobs, stages, executors and (sampled) tasks of one Spark context for the Timeline. With `run`, only that run's
    jobs, stages and tasks, on the run's own clock; executors and markers are those inside the run's window."""
    store.cluster_dir(cid)
    max_tasks = max(1, int(max_tasks))
    with store.connect() as con:
        ctxs = contexts(store, con, cid)
        if not ctx:
            if len(ctxs) == 1:
                ctx = ctxs[0]
            elif not ctxs:
                return {
                    "ctx": None, "contexts": [], "start": None, "end": None, "jobs": [], "stages": [],
                    "executors": [], "tasks": [], "tasks_total": 0, "sampled": False, "markers": [],
                }
            else:
                raise BadRequest(f"ctx is required: this cluster has {len(ctxs)} Spark contexts: {', '.join(ctxs)}")
        elif ctx not in ctxs:
            raise NotFound(f"unknown spark context {ctx!r}")

        cw, cp = "spark_context_id = ?", [ctx]
        run = (run or "").strip() or None
        win = run_window(store, con, cid, run) if run else None

        def scoped(name: str) -> tuple[str, list]:
            """the context filter, plus run_key = run for run-scoped datasets"""
            p = store.dataset_path(cid, name, required=False)
            if run and p is not None and "run_key" in store.schema(con, p):
                return cw + " AND run_key = ?", cp + [run]
            return cw, cp

        jw, jp = scoped("spark_jobs")
        sw, sp = scoped("stages")
        tw, tp = scoped("tasks")
        if start is not None and end is not None:  # Revision 15: only the tasks running in [start, end], every run's
            tw += " AND launch_time < epoch_ms(CAST(? AS BIGINT)) AND finish_time > epoch_ms(CAST(? AS BIGINT))"
            tp = tp + [int(end), int(start)]

        jobs = [
            {
                "spark_job_id": j.get("spark_job_id"),
                "start": j.get("start_time"),
                "end": j.get("end_time"),
                "result": j.get("result"),
                "status": _job_status(j.get("result"), j.get("end_time")),
                "description": (j.get("description") or None) and str(j.get("description"))[:BIG_TEXT_LIMIT],
                "stage_ids": j.get("stage_ids") or [],
                "sql_execution_id": j.get("sql_execution_id"),
            }
            for j in store.select(con, cid, "spark_jobs", jw, jp, order="spark_job_id")
        ]
        stages = [
            {
                "stage_id": s.get("stage_id"),
                "stage_attempt": s.get("stage_attempt"),
                "start": s.get("start_time"),
                "end": s.get("end_time"),
                "status": s.get("status"),
                "name": s.get("stage_name"),
                "spark_job_id": s.get("spark_job_id"),
            }
            for s in store.select(
                con, cid, "stages", sw, sp, order="stage_id, stage_attempt", truncate={"stage_name": BIG_TEXT_LIMIT}
            )
        ]

        # executors: executor_profile has removal_category; fall back to executors dataset
        ex_rows = store.select(con, cid, "executor_profile", cw, cp, order="added_time NULLS LAST, executor_id")
        if not ex_rows:
            ex_rows = store.select(con, cid, "executors", cw, cp, order="added_time NULLS LAST, executor_id")
        executors = []
        for e in ex_rows:
            cat = e.get("removal_category")
            if cat is None and e.get("removed_reason"):
                cat = _removal_category(e.get("removed_reason"))
            executors.append(
                {
                    "executor_id": e.get("executor_id"),
                    "host": e.get("host"),
                    "cores": e.get("cores"),
                    "added": e.get("added_time"),
                    "removed": e.get("removed_time"),
                    "removed_reason": e.get("removed_reason"),
                    "removal_category": cat,
                }
            )
        if win:  # executors alive at some point of the run
            executors = [e for e in executors if (e["added"] is None or e["added"] <= win[1])
                         and (e["removed"] is None or e["removed"] >= win[0])]

        # tasks (evenly sampled, failed tasks always kept)
        tasks: list[dict[str, Any]] = []
        tasks_total = 0
        sampled = False
        tpath = store.dataset_path(cid, "tasks", required=False)
        if tpath is not None:
            src = store.src(tpath)
            tasks_total = int(con.execute(f"SELECT count(*) FROM {src} WHERE {tw}", tp).fetchone()[0])
            step = max(1, math.ceil(tasks_total / max_tasks))
            sampled = step > 1
            keep = "TRUE" if step == 1 else f"((rn - 1) % {step} = 0 OR coalesce(failed, FALSE))"
            # per-task metrics for the executor usage chart; columns missing on older builds are skipped
            have = {d[0] for d in con.execute(f"SELECT * FROM {src} LIMIT 0").description}
            extra = "".join(f", {c}" for c in GANTT_TASK_METRICS if c in have)
            tasks = store.rows(
                con,
                f"SELECT task_id, task_attempt, stage_id, stage_attempt, executor_id, launch_time AS start, "
                f"finish_time AS \"end\", coalesce(failed, FALSE) AS failed{extra} FROM ("
                f"  SELECT *, row_number() OVER (ORDER BY launch_time NULLS LAST, task_id, task_attempt) AS rn "
                f"  FROM {src} WHERE {tw}"
                f") WHERE {keep} ORDER BY rn",
                tp,
            )

        # time bounds
        times: list[int] = []
        apps = store.select(con, cid, "apps", cw, cp)
        app_ids = sorted({a.get("app_id") for a in apps if a.get("app_id")})
        for coll, ks in (
            (apps, ("start_time", "end_time")),
            (jobs, ("start", "end")),
            (stages, ("start", "end")),
            (executors, ("added", "removed")),
        ):
            for r in coll:
                for k in ks:
                    if isinstance(r.get(k), int):
                        times.append(r[k])
        if tpath is not None and tasks_total:
            lo, hi = con.execute(
                f"SELECT min(launch_time), max(finish_time) FROM {store.src(tpath)} WHERE {cw}", cp
            ).fetchone()
            times += [v for v in clean([lo, hi]) if v is not None]
        start = min(times) if times else None
        end = max(times) if times else None
        if win:  # the run's own clock, not the cluster's
            start, end = win

        markers = _gantt_markers(store, con, cid, executors, app_ids, start, end, single_ctx=len(ctxs) <= 1)
        in_win = (lambda m: m.get("ts") is None or win[0] <= m["ts"] <= win[1]) if win else (lambda m: True)
        try:  # Revision 4: aggregated disk-spill markers (one per executor per time bucket)
            from .graph import spill_markers

            markers += spill_markers(store, con, cid, ctx, MAX_MARKERS_PER_KIND)
            markers = [m for m in markers if in_win(m)]
            markers.sort(key=lambda m: (m["ts"] if m["ts"] is not None else 0, m["kind"]))
        except ApiError:
            raise
        except Exception:  # noqa: BLE001  (older build: no spill markers rather than a failed Gantt)
            pass
        # Revision 12: exact busy stretches per executor (the task list above may be sampled); null on older builds
        busy = None
        if store.dataset_path(cid, "executor_busy", required=False) is not None:
            busy = [{k: v for k, v in r.items() if k in ("executor_id", "busy_start", "busy_end")}
                    for r in store.select(con, cid, "executor_busy", "spark_context_id = ?", [ctx],
                                          order="executor_id, busy_start")]
            if win:  # executors are shared: busy here may be another run's tasks in the same window
                busy = [{**b, "busy_start": max(b["busy_start"], win[0]), "busy_end": min(b["busy_end"], win[1])}
                        for b in busy if b["busy_end"] >= win[0] and b["busy_start"] <= win[1]]

    return {
        "ctx": ctx,
        "contexts": ctxs,
        "start": start,
        "end": end,
        "jobs": jobs,
        "stages": stages,
        "executors": executors,
        "tasks": tasks,
        "tasks_total": tasks_total,
        "sampled": sampled,
        "markers": markers,
        "busy": busy,
        "run": run if win else None,
    }


_RULES: list = []


def _removal_category(reason: str | None) -> str | None:
    """Fallback for builds without executor_profile: the rules.toml removal categories."""
    if not reason:
        return None
    if not _RULES:
        from ..config import load_rules

        _RULES.append(load_rules())
    return _RULES[0].removal_category(reason)


#: gantt marker kind per executor removal category (others: executor_removed)
REMOVAL_MARKER = {"oom": "oom", "lost": "executor_lost", "killed": "executor_killed"}


def _log_scope(
    sch: Mapping[str, str], app_ids: list[str], start: int | None, end: int | None, single_ctx: bool
) -> tuple[str, list[Any]]:
    """WHERE clause selecting log rows that belong to the chosen Spark context.

    Executor logs carry ``app_id`` (mapped to the context through ``apps``). Driver logs have no app_id, so they
    are kept when they fall inside the context's time window (or always when the cluster has one context).
    """
    params: list[Any] = []
    conds: list[str] = []
    if app_ids and "app_id" in sch:
        conds.append(f"app_id IN ({', '.join('?' for _ in app_ids)})")
        params.extend(app_ids)
    if single_ctx:
        conds.append("TRUE")
    elif start is not None and end is not None:
        app_null = "app_id IS NULL AND " if "app_id" in sch else ""
        conds.append(f"({app_null}ts BETWEEN epoch_ms(CAST(? AS BIGINT)) AND epoch_ms(CAST(? AS BIGINT)))")
        params.extend([start, end])
    if not conds:
        return "FALSE", []
    return "(" + " OR ".join(conds) + ")", params


def _gantt_markers(store, con, cid, executors, app_ids, start, end, *, single_ctx) -> list[dict[str, Any]]:
    markers: list[dict[str, Any]] = []
    for e in executors:
        eid = e.get("executor_id")
        if e.get("added") is not None:
            markers.append({"ts": e["added"], "executor_id": eid, "kind": "executor_added", "label": f"executor {eid} added"})
        if e.get("removed") is not None:
            cat = e.get("removal_category")
            kind = REMOVAL_MARKER.get(cat or "", "executor_removed")
            reason = (e.get("removed_reason") or "").strip()
            label = f"executor {eid} removed" + (f": {reason[:200]}" if reason else "")
            markers.append({"ts": e["removed"], "executor_id": eid, "kind": kind, "label": label})

    spath = store.dataset_path(cid, "log_signals", required=False)
    if spath is not None:
        sch = store.schema(con, spath)
        scope, params = _log_scope(sch, app_ids, start, end, single_ctx)
        rows = store.rows(
            con,
            f"SELECT ts, executor_id, signal, severity, line FROM {store.src(spath)} "
            f"WHERE ts IS NOT NULL AND {scope} AND (signal IN ('executor_oom', 'executor_lost') OR severity = 'high') "
            f"ORDER BY ts, seq LIMIT {MAX_MARKERS_PER_KIND * 2}",
            params,
        )
        for r in rows:
            kind = {"executor_oom": "oom", "executor_lost": "executor_lost"}.get(r.get("signal") or "", "signal")
            markers.append(
                {
                    "ts": r["ts"],
                    "executor_id": r.get("executor_id") or None,
                    "kind": kind,
                    "label": f"{r.get('signal')}: {(r.get('line') or '')[:200]}",
                }
            )

    epath = store.dataset_path(cid, "log_errors", required=False)
    if epath is not None:
        sch = store.schema(con, epath)
        scope, params = _log_scope(sch, app_ids, start, end, single_ctx)
        rows = store.rows(
            con,
            f"SELECT ts, executor_id, exception_class, message FROM {store.src(epath)} "
            f"WHERE ts IS NOT NULL AND {scope} ORDER BY ts, seq LIMIT {MAX_MARKERS_PER_KIND}",
            params,
        )
        for r in rows:
            msg = (r.get("message") or "").strip()
            markers.append(
                {
                    "ts": r["ts"],
                    "executor_id": r.get("executor_id") or None,
                    "kind": "error",
                    "label": f"{r.get('exception_class')}" + (f": {msg[:200]}" if msg else ""),
                }
            )
    gpath = store.dataset_path(cid, "gc_events", required=False)
    if gpath is not None:
        sch = store.schema(con, gpath)
        scope, params = _log_scope(sch, app_ids, start, end, single_ctx)
        rows = store.rows(
            con,
            f"SELECT ts, executor_id, kind, cause, pause_ms, heap_before_mb, heap_after_mb, heap_total_mb "
            f"FROM {store.src(gpath)} WHERE ts IS NOT NULL AND {scope} AND kind LIKE 'Pause Full%' "
            f"ORDER BY ts, seq LIMIT {MAX_MARKERS_PER_KIND}",
            params,
        )
        for r in rows:
            heap = ""
            if r.get("heap_before_mb") is not None and r.get("heap_after_mb") is not None:
                heap = f", heap {r['heap_before_mb']:.0f} -> {r['heap_after_mb']:.0f} MB"
                if r.get("heap_total_mb") is not None:
                    heap += f" of {r['heap_total_mb']:.0f} MB"
            markers.append(
                {
                    "ts": r["ts"],
                    "executor_id": r.get("executor_id") or None,
                    "kind": "full_gc",
                    "label": f"Full GC {r.get('pause_ms') or 0:.0f} ms" + (f" ({r['cause']})" if r.get("cause") else "")
                    + heap,
                }
            )
    markers.sort(key=lambda m: (m["ts"] if m["ts"] is not None else 0, m["kind"]))
    return markers


def _exec_split(rows: list[dict]) -> dict[str, Any]:
    """A stage's tasks per executor (most work first), and its CPU share and shuffle wait over all of them."""
    run_ms = sum(r.get("run_ms") or 0 for r in rows)
    cpu = sum(r.get("cpu_ms") or 0 for r in rows) if any(r.get("cpu_ms") is not None for r in rows) else None
    fw = sum(r.get("fetch_wait_ms") or 0 for r in rows) if any(r.get("fetch_wait_ms") is not None for r in rows) else None
    return {"cpu_share": (cpu / run_ms) if cpu is not None and run_ms else None, "fetch_wait_ms": fw,
            "by_exec": [{k: r.get(k) for k in ("executor_id", "tasks", "failed", "task_ms", "bytes_in", "disk_spill")} for r in rows]}


def run_steps(store: Store, cid: str, run: str) -> dict[str, Any]:
    """One run step by step: each of its queries (and Spark jobs without SQL) in the order they started, with the time
    a stage of it waited for a free core while none of its stages ran, the time its stages ran tasks, and each stage;
    the whole run's waiting / running / no-Spark-work split, and the stretches of a minute or more with no Spark work."""
    store.cluster_dir(cid)
    run = (run or "").strip()
    if not run:
        raise BadRequest("run is required")
    with store.connect() as con:
        r = (store.select(con, cid, "runs", "run_key = ?", [run], limit=1) or [None])[0]
        if r is None:
            raise NotFound(f"no run {run!r}")
        stages = store.select(con, cid, "stages", "run_key = ?", [run], order="start_time",
                              exclude=("details", "rdd_scopes", "rdd_names", "failure_reason", "job_description"))
        queries = {q["sql_execution_id"]: q for q in store.select(
            con, cid, "sql_queries", "run_key = ?", [run], truncate={"description": 200},
            exclude=("final_plan", "initial_plan", "details"))}
        jobs = {j["spark_job_id"]: j for j in store.select(con, cid, "spark_jobs", "run_key = ?", [run],
                                                            truncate={"description": 200, "call_site": 200})} \
            if store.dataset_path(cid, "spark_jobs", required=False) is not None else {}
        firsts: dict[tuple, int] = {}
        by_exec: dict[tuple, list] = {}
        tpath = store.dataset_path(cid, "tasks", required=False)
        if tpath is not None and stages:
            by_run = "run_key" in store.schema(con, tpath)
            ids = sorted({st["stage_id"] for st in stages})
            where = "run_key = ?" if by_run else f"stage_id IN ({', '.join('?' for _ in ids)})"
            for t in store.rows(con, f"SELECT spark_context_id, stage_id, stage_attempt, min(launch_time) AS first "
                                     f"FROM {store.src(tpath)} WHERE {where} GROUP BY 1, 2, 3", [run] if by_run else ids):
                firsts[(t["spark_context_id"], t["stage_id"], t["stage_attempt"])] = _as_ms(t["first"])
            # per stage and executor: how many of its tasks, how much of its work, its data and spill, CPU and shuffle wait
            sch = store.schema(con, tpath)
            # the time buckets count successful attempts only: a failed attempt's time is its own bucket
            col = lambda c: f"sum(CASE WHEN NOT failed THEN {c} END)" if c in sch else "NULL"
            for t in store.rows(con, f"""
                    SELECT spark_context_id, stage_id, stage_attempt, executor_id, count(*) AS tasks,
                           sum(CASE WHEN failed THEN 1 ELSE 0 END) AS failed, sum(task_ms) AS task_ms,
                           sum(coalesce(input_bytes, 0) + coalesce(shuffle_read, 0)) AS bytes_in, sum(disk_spill) AS disk_spill,
                           {col('cpu_ms')} AS cpu_ms, {col('run_ms')} AS run_ms, {col('fetch_wait_ms')} AS fetch_wait_ms,
                           {col('gc_ms')} AS gc_ms, {col('shuffle_write_ms')} AS shuffle_write_ms,
                           sum(CASE WHEN failed THEN task_ms ELSE 0 END) AS failed_ms
                    FROM {store.src(tpath)} WHERE {where} GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 7 DESC""",
                    [run] if by_run else ids):
                by_exec.setdefault((t["spark_context_id"], t["stage_id"], t["stage_attempt"]), []).append(t)
    r0, r1 = _as_ms(r.get("start_time")), _as_ms(r.get("end_time"))

    def gkey(st):
        q = st.get("sql_execution_id")
        return ("query", st["spark_context_id"], q) if q is not None else ("job", st["spark_context_id"], st.get("spark_job_id"))

    per = _wait_and_run(stages, firsts, gkey)
    whole = _wait_and_run(stages, firsts, lambda st: "run").get("run", ([], []))
    groups: dict[tuple, dict] = {}
    for st in stages:
        s0, s1 = _as_ms(st.get("start_time")), _as_ms(st.get("end_time"))
        if s0 is None or s1 is None:
            continue
        k = gkey(st)
        f = firsts.get((st["spark_context_id"], st["stage_id"], st["stage_attempt"]))
        f = min(max(f, s0), s1) if f is not None else s0
        g = groups.get(k)
        if g is None:
            src = queries.get(k[2]) if k[0] == "query" else jobs.get(k[2])
            desc = (src or {}).get("description") or (src or {}).get("call_site")
            g = groups[k] = {"kind": k[0], "ctx": k[1], "id": k[2], "description": desc, "status": (src or {}).get("status"),
                             "start": s0, "end": s1, "stages": [],
                             "tables_read": sorted({_short_table(x) for x in (src or {}).get("tables_read") or []}),
                             "tables_written": sorted({_short_table(x) for x in (src or {}).get("tables_written") or []}),
                             "operators": list((src or {}).get("operators") or [])[:24],
                             "input_records": (src or {}).get("input_records"), "output_records": (src or {}).get("output_records")}
            if k[0] == "query" and src:
                g["start"] = min(s0, _as_ms(src.get("start_time")) or s0)
                g["end"] = max(s1, _as_ms(src.get("end_time")) or s1)
        g["start"], g["end"] = min(g["start"], s0), max(g["end"], s1)
        g["stages"].append({"stage_id": st["stage_id"], "stage_attempt": st["stage_attempt"], "spark_job_id": st.get("spark_job_id"),
                            "status": st.get("status"), "tasks": st.get("tasks") if st.get("tasks") is not None else st.get("num_tasks"),
                            "submitted": s0, "first_task": f, "end": s1, "wait_ms": f - s0, "run_ms": s1 - f,
                            "disk_spill": st.get("disk_spill"), "failed_tasks": st.get("failed_tasks"),
                            "rows_read": st.get("input_records"), "rows_from_shuffle": st.get("shuffle_read_records"),
                            "rows_to_shuffle": st.get("shuffle_write_records"), "rows_written": st.get("output_records"),
                            "parent_ids": list(st.get("parent_ids") or []),
                            **{c: st.get(c) for c in ("min_task_ms", "p10_task_ms", "p50_task_ms", "p90_task_ms", "max_task_ms",
                                                      "p10_task_bytes_in", "p50_task_bytes_in", "p90_task_bytes_in", "max_task_bytes_in",
                                                      "input_bytes", "input_records", "shuffle_read", "shuffle_write", "output_bytes",
                                                      "mem_spill", "max_peak_mem", "gc_share", "executors_used", "skew")},
                            **_exec_split(by_exec.get((st["spark_context_id"], st["stage_id"], st["stage_attempt"]), []))})
    out = []
    for k, g in groups.items():
        w, b = per.get(k, ([], []))
        g["waiting_ms"] = sum(e - a for a, e in w)
        g["running_ms"] = sum(e - a for a, e in b)
        for c in ("tasks", "failed_tasks", "input_bytes", "shuffle_read", "shuffle_write", "output_bytes", "mem_spill", "disk_spill"):
            g[c] = sum(x.get(c) or 0 for x in g["stages"])
        g["stages"].sort(key=lambda x: (x["submitted"], x["stage_id"]))
        g["causes"] = _causes([t for x in g["stages"] for t in by_exec.get((k[1], x["stage_id"], x["stage_attempt"]), [])])
        out.append(g)
    out.sort(key=lambda g: (g["start"], g["id"] if g["id"] is not None else -1))
    # stretches of the run with no stage of it submitted or running: its code outside Spark (or waiting outside Spark)
    gaps = []
    if r0 is not None and r1 is not None:
        cur = r0
        for a, e in _merge([(g["start"], g["end"]) for g in out]):
            if a - cur >= 60_000:
                gaps.append([cur, a])
            cur = max(cur, e)
        if r1 - cur >= 60_000:
            gaps.append([cur, r1])
    waiting = sum(e - a for a, e in whole[0])
    running = sum(e - a for a, e in whole[1])
    total = (r1 - r0) if r0 is not None and r1 is not None else None
    return {"run_key": run, "start": r0, "end": r1, "total_ms": total, "waiting_ms": waiting, "running_ms": running,
            "outside_ms": max(0, total - waiting - running) if total is not None else None, "groups": out, "gaps": gaps,
            "causes": _causes([t for ts in by_exec.values() for t in ts]), "batches": _micro_batches(out)}


_BATCH_RE = re.compile(r"Streaming batch (\d+)\W+stream ([0-9a-f]{6,})", re.I)


def _micro_batches(groups: list[dict]) -> list[dict[str, Any]]:
    """Structured Streaming micro-batches of one run, from its queries' descriptions ("Streaming batch 234 · stream
    e3403166 · ..."): per stream and batch, when it ran, its queries and their waiting and running time."""
    out: dict[tuple, dict[str, Any]] = {}
    for g in groups:
        m = _BATCH_RE.search(g.get("description") or "")
        if not m:
            continue
        k = (m.group(2), int(m.group(1)))
        b = out.setdefault(k, {"stream": k[0], "batch": k[1], "start": g["start"], "end": g["end"], "queries": [],
                               "waiting_ms": 0, "running_ms": 0})
        b["start"], b["end"] = min(b["start"], g["start"]), max(b["end"], g["end"])
        b["queries"].append(g["id"])
        b["waiting_ms"] += g.get("waiting_ms") or 0
        b["running_ms"] += g.get("running_ms") or 0
    return sorted(out.values(), key=lambda b: (b["start"], b["stream"], b["batch"]))


def _causes(rows: list[dict]) -> dict[str, float] | None:
    """Where task time went, summed over tasks (per stage and executor rows): CPU, GC, waiting for shuffle data, writing
    shuffle files, failed or cancelled attempts, and the rest of the run time (reading and writing storage, network,
    Python, locks), plus the disk spill in bytes. The run's "running" wall time splits in these shares; Spark does not
    time spill. Executor CPU time is the task thread's own CPU: GC runs on other threads, so it is not taken off."""
    if not rows:
        return None
    tot = lambda c: float(sum(r.get(c) or 0 for r in rows))
    task, failed = tot("task_ms"), tot("failed_ms")
    if task <= 0:
        return None
    gc, fetch, write = tot("gc_ms"), tot("fetch_wait_ms"), tot("shuffle_write_ms")
    good = max(0.0, task - failed)
    cpu = min(tot("cpu_ms"), good)
    # the buckets never add up to more than the good task time (CPU is sampled apart from the others)
    write = min(write, max(0.0, good - cpu - gc - fetch))
    other = max(0.0, good - cpu - gc - fetch - write)
    return {"task_ms": task, "cpu_ms": cpu, "gc_ms": gc, "fetch_wait_ms": fetch, "shuffle_write_ms": write,
            "other_ms": other, "failed_ms": failed, "disk_spill": tot("disk_spill"), "has_cpu": bool(tot("cpu_ms"))}


TASK_COLS = ("executor_id", "stage_id", "stage_attempt", "task_id", "launch_time", "task_ms", "cpu_ms", "gc_ms",
             "input_bytes", "shuffle_read", "shuffle_write", "mem_spill", "disk_spill", "peak_mem", "output_bytes",
             "fetch_wait_ms", "failed")
MAX_TASK_COLUMNS = 60_000


def task_columns(store: Store, cid: str, ctx: str, stage: int | None = None, attempt: int | None = None,
                 job: int | None = None, query: int | None = None) -> dict[str, Any]:
    """Revision 17: every task of one stage attempt, one Spark job or one SQL query, as columns (one list per measure),
    for statistics, distributions and per-executor splits drawn in the browser; plus the executors that ran them and,
    for a job or a query, each stage's wait for a free core (from submitted to its first task)."""
    store.cluster_dir(cid)
    with store.connect() as con:
        if stage is not None:
            stages = store.select(con, cid, "stages", "spark_context_id = ? AND stage_id = ? AND stage_attempt = ?",
                                  [ctx, int(stage), int(attempt or 0)], exclude=("details", "rdd_scopes", "rdd_names"))
        elif job is not None:
            stages = store.select(con, cid, "stages", "spark_context_id = ? AND spark_job_id = ?", [ctx, int(job)],
                                  exclude=("details", "rdd_scopes", "rdd_names"))
        elif query is not None:
            stages = store.select(con, cid, "stages", "spark_context_id = ? AND sql_execution_id = ?", [ctx, int(query)],
                                  exclude=("details", "rdd_scopes", "rdd_names"))
        else:
            raise BadRequest("give stage (+ attempt), job or query")
        tpath = store.dataset_path(cid, "tasks", required=False)
        out: dict[str, Any] = {"n": 0, "total": 0, "sampled": False, "cols": {}, "present": {}, "executors": [], "stage_waits": []}
        if tpath is None or not stages:
            return out
        sch = store.schema(con, tpath)
        src = store.src(tpath)
        keys = [(s["stage_id"], s["stage_attempt"]) for s in stages]
        where = "spark_context_id = ? AND (" + " OR ".join("(stage_id = ? AND stage_attempt = ?)" for _ in keys) + ")"
        prm = [ctx] + [v for k in keys for v in k]
        cols = [c for c in TASK_COLS if c in sch]
        total = con.execute(f"SELECT count(*) FROM {src} WHERE {where}", prm).fetchone()[0] or 0
        sel = ", ".join(("epoch_ms(launch_time)" if c == "launch_time" else qi(c)) for c in cols)
        # beyond the cap: every failed task, the longest, and an even sample of the rest
        if total > MAX_TASK_COLUMNS:
            sql = (f"SELECT {sel} FROM (SELECT *, row_number() OVER (ORDER BY task_ms DESC NULLS LAST) AS rn, "
                   f"row_number() OVER (ORDER BY random()) AS rr FROM {src} WHERE {where}) "
                   f"WHERE failed OR rn <= 2000 OR rr <= {MAX_TASK_COLUMNS - 2000}")
        else:
            sql = f"SELECT {sel} FROM {src} WHERE {where}"
        rows = con.execute(sql, prm).fetchall()
        data = {c: [r[i] for r in rows] for i, c in enumerate(cols)}
        firsts = {}
        for r in con.execute(f"SELECT stage_id, stage_attempt, min(launch_time) FROM {src} WHERE {where} GROUP BY 1, 2", prm).fetchall():
            firsts[(r[0], r[1])] = _as_ms(r[2])
        execs = sorted({str(x) for x in data.get("executor_id", []) if x is not None})
        info = [{k: e.get(k) for k in ("executor_id", "host", "cores", "added_time", "removed_time", "removal_category")}
                for e in store.select(con, cid, "executors", "spark_context_id = ?", [ctx]) if str(e.get("executor_id")) in execs]
    waits = []
    for s in stages:
        s0 = _as_ms(s.get("start_time"))
        f = firsts.get((s["stage_id"], s["stage_attempt"]))
        if s0 is not None and f is not None:
            waits.append({"stage_id": s["stage_id"], "stage_attempt": s["stage_attempt"], "spark_job_id": s.get("spark_job_id"),
                          "submitted": s0, "first_task": f, "wait_ms": max(0, f - s0), "tasks": s.get("tasks")})
    if stage is None:
        st = [x for x in stages if _as_ms(x.get("start_time")) is not None and _as_ms(x.get("end_time")) is not None]
        if st:
            with store.connect() as con:
                runs = {x.get("run_key") for x in st if x.get("run_key")}
                out["sharing"] = _scope_sharing(store, con, cid, src, sch, ctx, keys, min(_as_ms(x["start_time"]) for x in st),
                                                max(_as_ms(x["end_time"]) for x in st), next(iter(runs)) if len(runs) == 1 else None)
    out.update({"n": len(rows), "total": int(total), "sampled": len(rows) < total, "cols": data,
                "present": {c: any(v is not None for v in data[c]) for c in cols}, "executors": info,
                "stage_waits": sorted(waits, key=lambda w: (w["submitted"], w["stage_id"]))})
    return out


TOP_KINDS = {"stages": "stages", "jobs": "spark_jobs", "queries": "sql_queries", "tasks": "tasks"}
TOP_BY = ("duration", "ran", "wait", "spill", "shuffle", "read", "tasks", "skew", "task_read", "task_shuffle")
MAX_TOP = 200


def top(store: Store, cid: str, kind: str, by: str = "duration", run: str | None = None, limit: int = 25,
        q: str | None = None) -> dict[str, Any]:
    """Revision 18: the slowest, longest-waiting, most spilling, biggest-shuffle or biggest-read stages, Spark jobs,
    SQL queries or tasks of the cluster (or one run), to find the one to open. A stage's wait is from submitted to its
    first task; a job's or a query's is its stages' waits summed; a task's is from its stage's submission to launch."""
    if kind not in TOP_KINDS:
        raise BadRequest(f"kind must be one of {', '.join(TOP_KINDS)}")
    if by not in TOP_BY:
        raise BadRequest(f"by must be one of {', '.join(TOP_BY)}")
    limit = max(1, min(int(limit), MAX_TOP))
    store.cluster_dir(cid)
    with store.connect() as con:
        path = store.dataset_path(cid, TOP_KINDS[kind], required=False)
        spath = store.dataset_path(cid, "stages", required=False)
        tpath = store.dataset_path(cid, "tasks", required=False)
        if path is None:
            return {"kind": kind, "by": by, "rows": [], "total": 0}
        sch = dict(store.schema(con, path))
        src = store.src(path)
        if kind == "jobs" and spath is not None:
            # a job's spill, tasks and skew are its stages'
            ssch = store.schema(con, spath)
            agg = [f"{f}({c}) AS {c}" for c, f in (("disk_spill", "sum"), ("mem_spill", "sum"), ("tasks", "sum"), ("data_skew", "max"))
                   if c in ssch and c not in sch]
            if agg:
                src = (f"(SELECT j.*, {', '.join('a.' + a.split(' AS ')[1] for a in agg)} FROM {src} j LEFT JOIN "
                       f"(SELECT spark_context_id, spark_job_id, {', '.join(agg)} FROM {store.src(spath)} GROUP BY 1, 2) a "
                       f"USING (spark_context_id, spark_job_id))")
                for a in agg:
                    sch[a.split(' AS ')[1]] = "BIGINT"
        n = lambda c: f"coalesce(t.{qi(c)}, 0)" if c in sch else "CAST(0 AS BIGINT)"
        where, prm = ["TRUE"], []
        if run and "run_key" in sch:
            where.append("t.run_key = ?")
            prm.append(run)
        # each stage's wait for a free core: submitted -> first task
        waits = None
        if spath is not None and tpath is not None and kind != "tasks":
            waits = (f"(SELECT s.spark_context_id, s.stage_id, s.stage_attempt, s.spark_job_id, s.sql_execution_id, "
                     f"greatest(0, epoch_ms(f.first) - epoch_ms(s.start_time)) AS wait_ms, "
                     f"greatest(0, coalesce(s.duration_ms, 0) - greatest(0, epoch_ms(f.first) - epoch_ms(s.start_time))) AS run_ms "
                     f"FROM {store.src(spath)} s JOIN "
                     f"(SELECT spark_context_id, stage_id, stage_attempt, min(launch_time) AS first FROM {store.src(tpath)} "
                     f"GROUP BY 1, 2, 3) f USING (spark_context_id, stage_id, stage_attempt))")
        if kind == "stages":
            keys, dur, label = ["spark_context_id", "stage_id", "stage_attempt"], n("duration_ms"), "stage_name"
            if q:
                where.append("(CAST(t.stage_id AS VARCHAR) = ? OR t.stage_name ILIKE ? OR t.job_description ILIKE ?)")
                prm += [q, f"%{q}%", f"%{q}%"]
        elif kind == "jobs":
            keys, dur, label = ["spark_context_id", "spark_job_id"], n("duration_ms"), "description"
            if q:
                where.append("(CAST(t.spark_job_id AS VARCHAR) = ? OR t.description ILIKE ?)")
                prm += [q, f"%{q}%"]
        elif kind == "queries":
            keys, dur, label = ["spark_context_id", "sql_execution_id"], n("duration_ms"), "description"
            if q:
                where.append("(CAST(t.sql_execution_id AS VARCHAR) = ? OR t.description ILIKE ?)")
                prm += [q, f"%{q}%"]
        else:
            keys, dur, label = ["spark_context_id", "stage_id", "stage_attempt", "task_id"], n("task_ms"), None
            if q:
                where.append("(CAST(t.stage_id AS VARCHAR) = ? OR CAST(t.executor_id AS VARCHAR) = ?)")
                prm += [q, q]
        shuffle = f"{n('shuffle_read')} + {n('shuffle_write')}"
        read = f"{n('input_bytes')} + {n('shuffle_read')}"
        join = ""
        wait = "NULL"
        ran = n("task_ms") if kind == "tasks" else f"greatest(0, {dur} - coalesce(w.wait_ms, 0))"
        if kind == "tasks":
            if spath is not None and "launch_time" in sch:
                join = (f" LEFT JOIN (SELECT spark_context_id, stage_id, stage_attempt, start_time AS st_start, spark_job_id AS st_job, "
                        f"sql_execution_id AS st_query FROM {store.src(spath)}) st USING (spark_context_id, stage_id, stage_attempt)")
                wait = "greatest(0, epoch_ms(t.launch_time) - epoch_ms(st.st_start))"
        elif waits is not None:
            if kind == "stages":
                join = f" LEFT JOIN {waits} w USING (spark_context_id, stage_id, stage_attempt)"
                wait = "w.wait_ms"
            else:
                k = keys[1]
                join = (f" LEFT JOIN (SELECT spark_context_id, {k}, sum(wait_ms) AS wait_ms, sum(run_ms) AS run_ms FROM {waits} "
                        f"GROUP BY 1, 2) w USING (spark_context_id, {k})")
                wait = "w.wait_ms"
                # a job's or a query's processing: its stages' running time, summed (stages that ran side by side add up)
                ran = "w.run_ms"
        # the biggest file read and the biggest shuffle read of one task: a stage's own, a job's or query's over its stages
        tmax_in, tmax_sh = "NULL", "NULL"
        if kind == "tasks":
            tmax_in, tmax_sh = n("input_bytes"), n("shuffle_read")
        elif tpath is not None and spath is not None:
            tm = (f"(SELECT s.spark_context_id, s.stage_id, s.stage_attempt, s.spark_job_id, s.sql_execution_id, m.mi, m.ms FROM "
                  f"{store.src(spath)} s JOIN (SELECT spark_context_id, stage_id, stage_attempt, max(input_bytes) AS mi, "
                  f"max(shuffle_read) AS ms FROM {store.src(tpath)} GROUP BY 1, 2, 3) m USING (spark_context_id, stage_id, stage_attempt))")
            gk = keys if kind == "stages" else keys[:2]
            join += (f" LEFT JOIN (SELECT {', '.join(gk)}, max(mi) AS max_task_input, max(ms) AS max_task_shuffle FROM {tm} "
                     f"GROUP BY {', '.join(str(i + 1) for i in range(len(gk)))}) tm USING ({', '.join(gk)})")
            tmax_in, tmax_sh = "tm.max_task_input", "tm.max_task_shuffle"
        if kind == "tasks":
            dur = f"{n('task_ms')} + coalesce({wait}, 0)"  # slowest: waited to start + ran
        elif waits is None:
            ran = dur
        order = {"duration": dur, "ran": f"coalesce({ran}, 0)", "wait": f"coalesce({wait}, 0)", "spill": n("disk_spill"), "shuffle": shuffle, "read": read,
                 "tasks": n("tasks") if kind != "tasks" else n("task_ms"),
                 "skew": (n("data_skew") if "data_skew" in sch else n("max_stage_skew") if "max_stage_skew" in sch
                          else n("skew")),
                 "task_read": f"coalesce({tmax_in}, 0)", "task_shuffle": f"coalesce({tmax_sh}, 0)"}[by]
        cols = [c for c in (*keys, "run_key", "spark_job_id", "sql_execution_id", "executor_id", "status", "result", "failed",
                            "start_time", "launch_time", "duration_ms", "task_ms", "tasks", "stages", "num_stages",
                            "input_bytes", "shuffle_read", "shuffle_write", "disk_spill", "mem_spill", "gc_ms", "p50_task_ms",
                            "max_task_ms", "p90_task_ms", "max_task_bytes_in", "data_skew", "skew", "max_stage_skew")
                if c in sch]
        cols = list(dict.fromkeys(cols))
        sel = ", ".join(f"t.{qi(c)}" for c in cols)
        if label and label in sch:
            sel += f", substr(t.{qi(label)}, 1, 160) AS label"
        if kind == "tasks" and join:
            sel += ", st.st_job AS stage_job, st.st_query AS stage_query"
        sel += f", {wait} AS wait_ms, {ran} AS ran_ms, {tmax_in} AS max_task_input, {tmax_sh} AS max_task_shuffle"
        w = " AND ".join(where)
        total = con.execute(f"SELECT count(*) FROM {src} t WHERE {w}", prm).fetchone()[0] or 0
        rows = store.rows(con, f"SELECT {sel} FROM {src} t{join} WHERE {w} ORDER BY {order} DESC NULLS LAST LIMIT {limit}", prm)
    for r in rows:
        for k in ("start_time", "launch_time"):
            if k in r:
                r[k] = _as_ms(r[k])
        if kind == "tasks":
            r.setdefault("spark_job_id", r.pop("stage_job", None))
            r.setdefault("sql_execution_id", r.pop("stage_query", None))
            r.pop("stage_job", None)
            r.pop("stage_query", None)
    return {"kind": kind, "by": by, "rows": rows, "total": int(total), "wait_known": wait != "NULL"}


# ---------------------------------------------------------------------------------------------------------------------
# Revision 20: what one run read and wrote
# ---------------------------------------------------------------------------------------------------------------------

_JDBC_FROM = re.compile(r"\bFROM\s+(?:\(\s*SELECT .*?\bFROM\s+)?([A-Za-z_][\w$]*\.[A-Za-z_][\w$]*)", re.I | re.S)
_DELTA_ROOT = re.compile(r"^(.*?)/_delta_log(?:/.*)?$")
_MERGE_STEP = re.compile(r"MERGE operation - (?:MERGE operation - )?(.+)$")
_SCAN_METRICS = {
    "number of files read": "files_read", "number of files pruned": "files_pruned", "size of files read": "bytes_read",
    "number of bytes pruned": "bytes_pruned", "size of files pruned": "bytes_pruned", "number of partitions read": "partitions_read",
    "number of partition columns": "partition_cols", "dynamic pruning - num filters (DPP)": "dpp_filters",
    "dynamic pruning - num filters (DFP)": "dfp_filters",
    # Databricks only: the smallest and the largest file the scan read
    "size of the smallest file read": "min_file_bytes", "size of the largest file read": "max_file_bytes",
}
# rows a write reported, by metric name (appends and overwrites: output rows; Delta MERGE, UPDATE and DELETE: what
# each did to the target; "copied" = rows of rewritten files that did not change)
_WRITE_METRICS = {
    "number of output rows": "rows", "number of source rows": "source_rows",
    "number of inserted rows": "inserted", "number of updated rows": "updated", "number of rows updated": "updated",
    "number of deleted rows": "deleted", "number of rows deleted": "deleted", "number of rows deleted.": "deleted",
    "number of target rows rewritten unmodified": "copied", "number of rows copied": "copied",
}
_WRITE_NODE = ("Append", "Overwrite", "Replace", "Execute", "Merge", "Write", "Update", "Delete", "TableAsSelect")


def _scan_table(name: str) -> tuple[str | None, str, str | None]:
    """A plan scan node's name -> (table, how it is read, the filter pushed to the source)."""
    n = " ".join((name or "").split())
    if n.startswith("Scan JDBCRelation"):
        m = _JDBC_FROM.search(n)
        where = re.search(r"\bWHERE\s+(.+?)\s*\)", n, re.I)
        how = ("counts rows in the source database" if "COUNT(*)" in n.upper()
               else "aggregates in the source database" if "GROUP BY" in n.upper()
               else "samples the source database (LIMIT)" if re.search(r"\bLIMIT\s+\d+", n, re.I)
               else "reads the source database over JDBC")
        return (m.group(1) if m else None), how, (where.group(1)[:120] if where else None)
    m = re.match(r"Scan (?:parquet|delta|json|csv|orc|text)\s+(\S+)", n)
    if m:
        return m.group(1), "reads files", None
    if "mergeMaterializedSource" in n:
        return None, "reads the MERGE's materialized source", None
    return None, "", None


def _op_of(desc: str, ops: list[str]) -> str:
    """What a query does, in a few words: the MERGE step, a Delta write, a stream batch, a read."""
    d = " ".join((desc or "").split())
    m = _MERGE_STEP.search(d)
    if m:
        step = m.group(1).strip()
        return "MERGE: " + (step[:1].lower() + step[1:])
    if any("MergeInto" in o for o in ops) or "MERGE" in d:
        return "MERGE"
    for k, v in (("DeleteCommand", "DELETE"), ("UpdateCommand", "UPDATE"), ("OptimizeTableCommand", "OPTIMIZE"),
                 ("VacuumCommand", "VACUUM"), ("CreateTableAsSelect", "CREATE TABLE AS SELECT"),
                 ("ReplaceTableAsSelect", "REPLACE TABLE AS SELECT"), ("OverwriteByExpression", "overwrite"),
                 ("AppendData", "append"), ("WriteIntoDeltaCommand", "write (Delta)"), ("InsertIntoHadoopFsRelation", "insert")):
        if any(k in o for o in ops):
            return v
    if re.search(r"\bbatch\s*=", d) or "Streaming batch" in d:
        return "stream batch"
    if any("JDBCRelation" in o for o in ops):
        return "read (JDBC)"
    return "read" if any(o.startswith("Scan") for o in ops) else "compute"


def run_tables(store: Store, cid: str, run: str) -> dict[str, Any]:
    """Every table and path one run read and wrote: per table who read it (and how much was pruned) and who wrote it;
    per query what it did, the tables it read and wrote, the earlier queries of the run that fed it (they wrote a table
    it reads, or materialized its MERGE source), and its Spark jobs and stages with the tables each stage scanned."""
    store.cluster_dir(cid)
    with store.connect() as con:
        qs = store.select(con, cid, "sql_queries", "run_key = ?", [run],
                          order="start_time NULLS LAST, sql_execution_id")
        sts = store.select(con, cid, "stages", "run_key = ?", [run], order="start_time NULLS LAST, stage_id")
        keys = sorted({(q["spark_context_id"], q["sql_execution_id"]) for q in qs})
        nodes: list[dict[str, Any]] = []
        if keys and store.dataset_path(cid, "sql_plan_nodes", required=False) is not None:
            ph = ", ".join("(?, ?)" for _ in keys)
            nodes = store.select(con, cid, "sql_plan_nodes",
                                 f"(spark_context_id, sql_execution_id) IN ({ph}) AND (name LIKE 'Scan%' OR name LIKE '%Scan %')",
                                 [x for k in keys for x in k])
            like = " OR ".join("name LIKE ?" for _ in _WRITE_NODE)
            wnodes = store.select(con, cid, "sql_plan_nodes",
                                  f"(spark_context_id, sql_execution_id) IN ({ph}) AND ({like}) AND name NOT LIKE 'Scan%'",
                                  [x for k in keys for x in k] + [f"%{w}%" for w in _WRITE_NODE])
        else:
            wnodes = []
    # rows each query wrote: the largest of each metric over its write nodes (a command and the node it wraps can
    # both report them)
    wrote: dict[tuple, dict[str, int]] = {}
    for nd in wnodes:
        try:
            ms = json.loads(nd.get("metrics_json") or "[]")
        except (ValueError, TypeError):
            continue
        d = wrote.setdefault((nd["spark_context_id"], nd["sql_execution_id"]), {})
        for x in ms:
            k = _WRITE_METRICS.get(x.get("name"))
            if k and x.get("total") is not None:
                d[k] = max(d.get(k, 0), int(x["total"]))
    # Delta paths -> the table name: a MERGE that reads one table by name and writes one path rewrote that table
    alias: dict[str, str] = {}
    for q in qs:
        rd = [t for t in (q.get("tables_read") or []) if "/" not in t]
        wr = [t for t in (q.get("tables_written") or []) if "/" in t]
        if "MERGE" in (q.get("description") or "") and len(set(rd)) == 1 and len(set(wr)) == 1:
            alias[wr[0].rstrip("/")] = rd[0]
    # a MERGE whose steps run as separate queries: the step that writes names only the path, the step that scans
    # for matches names the table; one of each in the run is the same table
    merge_named = {t for q in qs if "scanning files for matches" in (q.get("description") or "")
                   for t in (q.get("tables_read") or []) if "/" not in t}
    merge_paths = {t.rstrip("/") for q in qs if "MERGE" in (q.get("description") or "")
                   for t in (q.get("tables_written") or []) if "/" in t} - set(alias)
    if len(merge_named) == 1 and len(merge_paths) == 1:
        alias[merge_paths.pop()] = merge_named.pop()

    def norm(t: str) -> tuple[str, str | None]:
        """A table or path -> (the table, a note when it was its Delta log or checkpoint that was read)."""
        m = _DELTA_ROOT.match(t)
        root = (m.group(1) if m else t).rstrip("/")
        return alias.get(root, root), ("its Delta log" if m else None)

    scans: dict[tuple, list[dict[str, Any]]] = {}
    for nd in nodes:
        tbl, how, filt = _scan_table(nd.get("name") or "")
        if not how:
            continue
        mt: dict[str, Any] = {"how": how, "filter": filt}
        if nd.get("rows_out") is not None and not is_null(nd.get("rows_out")):
            mt["rows"] = int(nd["rows_out"])
        try:
            for x in json.loads(nd.get("metrics_json") or "[]"):
                k = _SCAN_METRICS.get(x.get("name"))
                if k:
                    mt[k] = x.get("total")
        except (ValueError, TypeError):
            pass
        scans.setdefault((nd["spark_context_id"], nd["sql_execution_id"]), []).append({"table": tbl, **mt})

    by_q: dict[tuple, list[dict[str, Any]]] = {}
    for st in sts:
        by_q.setdefault((st.get("spark_context_id"), st.get("sql_execution_id")), []).append(st)

    tables: dict[str, dict[str, Any]] = {}
    queries: list[dict[str, Any]] = []
    written_by: dict[str, list[tuple]] = {}  # table -> queries that wrote it so far (in start order)
    materialized: list[tuple] = []
    for q in qs:
        k = (q["spark_context_id"], q["sql_execution_id"])
        ops = list(q.get("operators") or [])
        op = _op_of(q.get("description") or "", ops)
        reads: dict[str, dict[str, Any]] = {}
        for t in q.get("tables_read") or []:
            name, note = norm(t)
            r = reads.setdefault(name, {"table": name, "how": "reads its Delta log" if note else "reads files"})
            if not note:
                r["how"] = "reads files"
        for sc in scans.get(k, []):
            if sc["table"] is None:
                continue
            name, _ = norm(sc["table"])
            r = reads.setdefault(name, {"table": name})
            r["how"] = sc["how"]
            for f in ("rows", "files_read", "files_pruned", "bytes_read", "bytes_pruned", "partitions_read", "partition_cols", "dpp_filters", "dfp_filters"):
                if sc.get(f) is not None:
                    r[f] = (r.get(f) or 0) + sc[f] if f not in ("partition_cols",) else max(r.get(f) or 0, sc[f])
            if sc.get("min_file_bytes"):
                r["min_file_bytes"] = min(r.get("min_file_bytes") or sc["min_file_bytes"], sc["min_file_bytes"])
            if sc.get("max_file_bytes"):
                r["max_file_bytes"] = max(r.get("max_file_bytes") or 0, sc["max_file_bytes"])
            if sc.get("filter"):
                r["filter"] = sc["filter"]
        writes = []
        for t in q.get("tables_written") or []:
            name, _ = norm(t)
            if name not in [w["table"] for w in writes]:
                writes.append({"table": name, "how": op if op.startswith("MERGE") else op if op not in ("read", "compute") else "writes"})
        if len(writes) == 1 and wrote.get(k):  # the rows go to the one table it wrote
            writes[0]["rows"] = wrote[k]
        # its jobs and stages, with the tables each stage scanned (from the stage's operator names)
        stages = []
        for st in by_q.get(k, []):
            sr = []
            for sc in st.get("rdd_scopes") or []:
                tbl, how, _ = _scan_table(sc)
                if tbl:
                    sr.append(norm(tbl)[0])
                elif how == "reads the MERGE's materialized source":
                    sr.append("(MERGE source)")
            scopes = st.get("rdd_scopes") or []
            stages.append({
                "stage_id": st["stage_id"], "stage_attempt": st["stage_attempt"], "spark_job_id": st.get("spark_job_id"),
                "reads": list(dict.fromkeys(sr)), "writes": [w["table"] for w in writes] if any("WriteFiles" in x for x in scopes) else [],
                "input_bytes": st.get("input_bytes"), "input_records": st.get("input_records"), "shuffle_read": st.get("shuffle_read"), "shuffle_write": st.get("shuffle_write"),
                "output_bytes": st.get("output_bytes"), "tasks": st.get("num_tasks") or st.get("tasks"),
                "max_task_bytes_in": st.get("max_task_bytes_in"),
            })
        # a table only a stage names (its plan was not recorded) is still read by the query
        for x in stages:
            for t in x["reads"]:
                if t != "(MERGE source)":
                    reads.setdefault(t, {"table": t, "how": "reads files"})
        # what its stages pulled from that table's files: only the columns it needs (Parquet is columnar)
        for x in stages:
            for t in x["reads"]:
                if t in reads and x.get("input_bytes"):
                    reads[t]["from_files"] = (reads[t].get("from_files") or 0) + x["input_bytes"]
        # rows read, when the plan's scan did not count them: the input records of the stages that read only that table
        for t in reads:
            if reads[t].get("rows") is None:
                n = [x["input_records"] for x in stages if x["reads"] == [t] and x.get("input_records") is not None]
                if n:
                    reads[t]["rows"] = int(sum(n))
        # who fed it: an earlier query of this run wrote a table it reads; a MERGE's materialized source
        fed = []
        for name in reads:
            for w in written_by.get(name, []):
                if w != k:
                    fed.append({"ctx": w[0], "id": w[1], "table": name, "why": "wrote it earlier in this run"})
        if any(sc.get("how") == "reads the MERGE's materialized source" for sc in scans.get(k, [])) or \
                any("mergeMaterializedSource" in o for o in ops):
            if materialized:
                fed.append({"ctx": materialized[-1][0], "id": materialized[-1][1], "table": None, "why": "materialized the MERGE source"})
        if op.startswith("MERGE: materialize"):
            materialized.append(k)
        for w in writes:
            written_by.setdefault(w["table"], []).append(k)
        t0, t1 = _as_ms(q.get("start_time")), _as_ms(q.get("end_time"))
        for name, r in reads.items():
            tables.setdefault(name, {"table": name, "reads": [], "writes": []})["reads"].append({"ctx": k[0], "id": k[1], "op": op, "start": t0, "end": t1, **r})
        for w in writes:
            tables.setdefault(w["table"], {"table": w["table"], "reads": [], "writes": []})["writes"].append({"ctx": k[0], "id": k[1], "op": w["how"], "start": t0, "end": t1,
                                                                                                         "step": op, "bytes": q.get("output_bytes"), "read_bytes": q.get("input_bytes"),
                                                                                                         "rows": w.get("rows")})
        logic = query_logic(q)
        queries.append({
            "logic": logic,
            "ctx": k[0], "id": k[1], "start": _as_ms(q.get("start_time")), "end": _as_ms(q.get("end_time")), "status": q.get("status"),
            "description": (q.get("description") or "")[:200], "op": op, "reads": list(reads.values()), "writes": writes, "fed_by": fed,
            "jobs": sorted({s["spark_job_id"] for s in stages if s["spark_job_id"] is not None}), "stages": stages,
            "input_bytes": q.get("input_bytes"), "output_bytes": q.get("output_bytes"),
        })
    for t in tables.values():
        # reading only its Delta log (the commits and checkpoint, e.g. a write loading the table's version) is not
        # reading its data
        data = [x for x in t["reads"] if x.get("how") != "reads its Delta log"]
        t["role"] = ("read and written" if data and t["writes"] else "written (its Delta log read too)" if t["writes"] and t["reads"]
                     else "written" if t["writes"] else "read" if data else "read (only its Delta log)")
        rs = [x["start"] for x in data if x.get("start") is not None]
        ws = [x["start"] for x in t["writes"] if x.get("start") is not None]
        t["first_read"], t["first_write"] = (min(rs) if rs else None), (min(ws) if ws else None)
        t["last"] = max([x["end"] for x in t["reads"] + t["writes"] if x.get("end") is not None], default=None)
        t["path"] = next((p for p, n in alias.items() if n == t["table"]), None)
    # a MERGE target: the columns it matches on (from its join), and whether its scan of the target filtered anything
    for q in queries:
        if not q["op"].startswith("MERGE") or not q["logic"]["joins"]:
            continue
        j = q["logic"]["joins"][0]
        for r in q["reads"]:
            t = tables.get(r["table"])
            if t is None or not t["writes"] or "merge" in t:
                continue
            sc = next((x for x in q["logic"]["scans"] if x["table"] and norm(x["table"])[0] == r["table"]), None)
            t["merge"] = {"keys": j["left_keys"], "null_safe": j["null_safe"], "join": j["type"], "how": j["how"],
                          "query": q["id"], "ctx": q["ctx"],
                          "target_filters": (sc["partition_filters"] + sc["data_filters"]) if sc else []}
    with store.connect() as con:
        stats = table_stats(store, con, cid, run)
    for t in tables.values():
        t["stats"] = stats.get(t["table"])
    # in time order: the first table it touched first
    first = lambda t: min(x for x in (t["first_read"], t["first_write"]) if x is not None) if (t["first_read"] or t["first_write"]) else float("inf")
    return {"run_key": run, "tables": sorted(tables.values(), key=lambda t: (first(t), t["table"])), "queries": queries,
            "merges": merge_cycles(queries), "jdbc": jdbc_steps(qs)}


def jdbc_steps(qs: list[dict]) -> list[dict[str, Any]]:
    """The queries of one run that read from a database over JDBC, in start order: what each asked the database
    (count, probe, key ranges, read), its rows (JDBC reports rows, not bytes), partitions and time, and whether the same
    source query already ran earlier in the run (a re-read the database answers again)."""
    out: list[dict[str, Any]] = []
    first: dict[str, int] = {}
    for q in sorted(qs, key=lambda x: (_as_ms(x.get("start_time")) or 0, x.get("sql_execution_id") or 0)):
        j = jdbc_source(q.get("final_plan")) or jdbc_source(q.get("initial_plan"))
        if not j:
            continue
        key = re.sub(r"\s+", " ", j["sql"]).strip().lower()
        same = first.get(key)
        if same is None:
            first[key] = q["sql_execution_id"]
        out.append({"query": q["sql_execution_id"], "ctx": q.get("spark_context_id"), "kind": j["kind"],
                    "source": j["source"], "partitions": j["partitions"], "sql": j["sql"][:400],
                    "rows": q.get("input_records"), "took_ms": q.get("duration_ms"),
                    "wrote_rows": q.get("output_records"), "wrote_bytes": q.get("output_bytes"),
                    "written": [t for t in (q.get("tables_written") or [])][:3], "same_as": same,
                    # how even the partitions were: rows and time of the median and the biggest task
                    "spread": {k: q.get(k) for k in ("p50_task_rows_in", "max_task_rows_in", "min_task_rows_in",
                                                     "p50_task_ms", "p90_task_ms")}})
    return out


# ---------------------------------------------------------------------------------------------------------------------
# Revision 20: what a query joins on and filters by, from its physical plan
# ---------------------------------------------------------------------------------------------------------------------

_EXPR_ID = re.compile(r"#\d+L?")
_NODE_HEAD = re.compile(r"^\((\d+)\) (.+?)(?: \[codegen id : \d+\])?$")
_INTERNAL = re.compile(r"_databricks_internal|_source_row_present_|_target_row_present_|_row_dropped_|fileInScanId|"
                       r"deletionVector|raise_error\(|__is_cdc|packedCdc|packedData|^isnotnull\((add|remove|path)\)$|partitionValuesStr|sidecar\.|^true$")
_JOINS = ("SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin", "BroadcastNestedLoopJoin", "CartesianProduct")


def _split_top(s: str) -> list[str]:
    """Split "a, f(b, c), d" on the commas that are not inside brackets."""
    out, depth, cur = [], 0, []
    for ch in s:
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        if ch == "," and depth == 0:
            out.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        out.append("".join(cur).strip())
    return out


def _list_of(v: str) -> list[str]:
    v = v.strip()
    if v.startswith("[") and v.endswith("]"):
        v = v[1:-1]
    return [x for x in _split_top(v) if x]


def _keys(items: list[str]) -> tuple[list[str], bool]:
    """Join keys; Spark writes a null-safe key (a <=> b) as coalesce(a, <default>), isnull(a): fold those back."""
    keys, null_safe, i = [], False, 0
    while i < len(items):
        m = re.fullmatch(r"coalesce\((.+?), [^)]*\)", items[i])
        if m and i + 1 < len(items) and items[i + 1] == f"isnull({m.group(1)})":
            keys.append(m.group(1))
            null_safe = True
            i += 2
            continue
        keys.append(items[i])
        i += 1
    return keys, null_safe


def plan_logic(plan: str | None) -> dict[str, Any]:
    """From a physical plan's details: the joins (type, how, keys, null-safe, extra condition), the filters (Spark's
    and Delta's internal ones left out), what each scan pushed down (partition, data and pushed filters), the group-by
    keys and the window partitions. Expression ids (#123) are dropped."""
    nodes: list[dict[str, Any]] = []
    cur: dict[str, Any] | None = None
    for raw in (plan or "").splitlines():
        line = _EXPR_ID.sub("", raw.rstrip())
        h = _NODE_HEAD.match(line)
        if h:
            cur = {"id": int(h.group(1)), "name": h.group(2).strip(), "attrs": {}}
            nodes.append(cur)
            continue
        if cur is not None and ": " in line and not line.startswith(" "):
            k, v = line.split(": ", 1)
            k = re.sub(r"\s*\[\d+\]$", "", k.strip())
            cur["attrs"][k] = v.strip()
    joins, filters, scans, groups, windows = [], [], [], [], []
    for n in nodes:
        name, a = n["name"], n["attrs"]
        kind = name.split(" ")[0]
        if kind in _JOINS:
            lk, ns1 = _keys(_list_of(a.get("Left keys", "")))
            rk, ns2 = _keys(_list_of(a.get("Right keys", "")))
            cond = a.get("Join condition")
            joins.append({"node": n["id"], "how": kind, "type": a.get("Join type") or (name.split(" ")[1] if " " in name else None),
                          "left_keys": lk, "right_keys": rk, "null_safe": ns1 or ns2,
                          "condition": None if cond in (None, "None") else cond[:300]})
        elif kind == "Filter" and a.get("Condition"):
            c = a["Condition"]
            if not _INTERNAL.search(c):
                filters.append({"node": n["id"], "condition": c[:300]})
        elif name.startswith("Scan ") or kind in ("FileScan", "PhotonScan"):
            pf = [x for x in _list_of(a.get("PartitionFilters", "[]")) if not _INTERNAL.search(x)]
            df = [x for x in _list_of(a.get("DataFilters", "[]")) if not _INTERNAL.search(x)]
            pu = [x for x in _list_of(a.get("PushedFilters", "[]")) if not _INTERNAL.search(x)]
            tbl, _, _ = _scan_table(name)
            scans.append({"node": n["id"], "table": tbl or name[5:].strip() or None, "partition_filters": pf,
                          "data_filters": df, "pushed_filters": pu})
        elif kind in ("HashAggregate", "ObjectHashAggregate", "SortAggregate") and a.get("Keys"):
            ks = _list_of(a["Keys"])
            if ks and not any(_INTERNAL.search(k) for k in ks) and ks not in [g["keys"] for g in groups]:
                groups.append({"node": n["id"], "keys": ks})
        elif kind in ("Window", "WindowGroupLimit", "RunningWindowFunction") and a.get("Arguments"):
            parts = _split_top(a["Arguments"])
            lists = [_list_of(x) for x in parts if x.startswith("[")]
            if kind == "WindowGroupLimit" and len(lists) >= 2:
                w = {"node": n["id"], "partition_by": lists[0], "order_by": lists[1], "keeps": "the first rows per key"}
                rank = re.search(r"(\w+)\(.*?\), (\d+),", a["Arguments"])
                if rank:
                    w["keeps"] = f"the top {rank.group(2)} by {rank.group(1)} per key"
                if w["partition_by"] not in [x["partition_by"] for x in windows]:
                    windows.append(w)
    # plan signals worth a sentence: Delta features that add work
    p = plan or ""
    facts = []
    if "__is_cdc" in p or "packedCdc" in p:
        facts.append("Change Data Feed is on for the table it writes: the write also stores change rows.")
    if "deltaoptimizedwritepartitioning" in p.lower():
        facts.append(OPT_WRITE_FACT)
    if "mergeMaterializedSource" in p:
        facts.append(MAT_SOURCE_FACT)
    if "deletionVector" in p:
        facts.append("Deletion vectors are on: a matched row is marked deleted instead of rewriting its whole file.")
    return {"joins": joins, "filters": filters, "scans": scans, "groups": groups, "windows": windows, "facts": facts}


MAT_SOURCE_FACT = "The MERGE materialized its source first (a copy of the source rows, read again by each step)."


_BATCH_KEY = re.compile(r"^(.*?[Bb]atch \d+.*?) · MERGE")
OPT_WRITE_FACT = "Optimized write is on: one more shuffle before the write, to make bigger files."


def _related_where(q: Mapping[str, Any], qsch, window: bool) -> tuple[str, list]:
    """The queries of the same operation as `q`, as SQL over `q` (sql_queries): a streaming MERGE's steps are separate
    queries named "Streaming batch N · stream X · MERGE ...", tied by the batch and stream; with `window`, otherwise
    the queries of the same run that started while it ran; else only itself."""
    m = _BATCH_KEY.match(q.get("description") or "")
    if m:
        like = m.group(1).replace("!", "!!").replace("%", "!%").replace("_", "!_")
        return "q.description LIKE ? ESCAPE '!'", [like + " · MERGE%"]
    t0, t1 = _as_ms(q.get("start_time")), _as_ms(q.get("end_time"))
    if window and t0 is not None and t1 is not None:
        where, params = "epoch_ms(q.start_time) BETWEEN ? AND ?", [t0, t1]
        if "run_key" in qsch and q.get("run_key") is not None:
            where += " AND q.run_key = ?"
            params.append(q["run_key"])
        return where, params
    return "q.sql_execution_id = ?", [q["sql_execution_id"]]


def _related_stages(store: Store, con, cid: str, q: Mapping[str, Any], scope_like: str, window: bool) -> list[dict]:
    """The successful stages of `q`'s operation whose operators match `scope_like` (SQL LIKE over the scopes)."""
    spath = store.dataset_path(cid, "stages", required=False)
    qpath = store.dataset_path(cid, "sql_queries", required=False)
    if spath is None or qpath is None or "rdd_scopes" not in store.schema(con, spath):
        return []
    where, params = _related_where(q, store.schema(con, qpath), window)
    return store.rows(con, f"""
        SELECT s.input_bytes, s.shuffle_read, s.shuffle_write, s.duration_ms
        FROM {store.src(spath)} s JOIN {store.src(qpath)} q USING (spark_context_id, sql_execution_id)
        WHERE s.spark_context_id = ? AND {where} AND array_to_string(s.rdd_scopes, '|') LIKE ?
          AND coalesce(s.status, '') <> 'failed'""", [q["spark_context_id"], *params, scope_like])


def _source_copy_fact(store: Store, con, cid: str, q: Mapping[str, Any]) -> str | None:
    """What the MERGE's source copy cost: the stages of this MERGE that read the copy back, how much each read and
    how many times. "Source copy: 30.9 GB on local disk, read 2 times (61.8 GB in all)"."""
    reads = [r["input_bytes"] or 0 for r in _related_stages(store, con, cid, q, "%mergeMaterializedSource%", True)]
    reads = [b for b in reads if b > 0]
    if not reads:
        return None
    n = len(reads)
    return (f"Source copy: {_fmt_bytes(max(reads))} on local disk, read {n} {'time' if n == 1 else 'times'}"
            + (f" ({_fmt_bytes(sum(reads))} in all)" if n > 1 else "") + ". The MERGE copies its source first so every step sees the same rows.")


def _optimized_write_fact(store: Store, con, cid: str, q: Mapping[str, Any]) -> str | None:
    """What optimized write cost: the extra shuffle its write stages read back before writing."""
    rows = [r for r in _related_stages(store, con, cid, q, "%WriteFiles%", False) if (r["shuffle_read"] or 0) > 0]
    if not rows:
        return None
    b = sum(r["shuffle_read"] or 0 for r in rows)
    return f"Optimized write: one more shuffle of {_fmt_bytes(b)} before the write, to make bigger files."


#: plan notes that can carry what they cost, and how to find it
_FACT_COST = {MAT_SOURCE_FACT: _source_copy_fact, OPT_WRITE_FACT: _optimized_write_fact}


def query_logic(q: Mapping[str, Any]) -> dict[str, Any]:
    """plan_logic of the final plan; when adaptive execution replaced the join (an empty side made it an empty
    result), the joins and scans of the initial plan, marked so."""
    lg = plan_logic(q.get("final_plan"))
    if not lg["joins"] and q.get("initial_plan"):
        ini = plan_logic(q.get("initial_plan"))
        if ini["joins"]:
            lg["joins"] = ini["joins"]
            lg["scans"] = lg["scans"] or ini["scans"]
            lg["from_initial"] = True
    return lg


def query_logic_view(store: Store, cid: str, ctx: str, exec_id: int) -> dict[str, Any]:
    """Revision 20: what one query joins on and filters by, for the query header."""
    store.cluster_dir(cid)
    with store.connect() as con:
        q = store.select(con, cid, "sql_queries", "spark_context_id = ? AND sql_execution_id = ?", [ctx, int(exec_id)], limit=1)
        if not q:
            return plan_logic(None)
        lg = query_logic(q[0])
        # a note with what it cost, in place of the bare sentence
        lg["facts"] = [(_FACT_COST[f](store, con, cid, q[0]) if f in _FACT_COST else None) or f for f in lg["facts"]]
    return lg



# ---------------------------------------------------------------------------------------------------------------------
# Revision 20: how big each table is and what reading it cost; the MERGE cycles of a run
# ---------------------------------------------------------------------------------------------------------------------

_BATCH = re.compile(r"[Bb]atch[ =]+(\w+)")


def table_stats(store: Store, con, cid: str, run: str | None = None, query: tuple[str, int] | None = None) -> dict[str, dict[str, Any]]:
    """Per table read from files (one query, one run, or the whole cluster): its size and file count from a scan that skipped
    nothing (files read + files skipped; the largest scan, as tables grow), the average file, how many times it was
    scanned and in how many runs, the smallest and largest file read (Databricks scans record both), the average rows per
    file, what the scan stages pulled from the files (only the needed columns: Parquet is
    columnar), and their wall and task time."""
    npath = store.dataset_path(cid, "sql_plan_nodes", required=False)
    qpath = store.dataset_path(cid, "sql_queries", required=False)
    spath = store.dataset_path(cid, "stages", required=False)
    tpath = store.dataset_path(cid, "tasks", required=False)
    out: dict[str, dict[str, Any]] = {}
    if npath is None or qpath is None:
        return out
    qsch = store.schema(con, qpath)
    by_run = run is not None and "run_key" in qsch
    rk = "q.run_key" if "run_key" in qsch else "NULL"
    qw, qp = ("", [])
    if query is not None:
        qw, qp = " AND q.spark_context_id = ? AND q.sql_execution_id = ?", [query[0], int(query[1])]
    elif by_run:
        qw, qp = " AND q.run_key = ?", [run]
    rows = store.rows(con, f"SELECT n.name, n.metrics_json, {rk} AS run_key FROM {store.src(npath)} n JOIN {store.src(qpath)} q "
                           f"USING (spark_context_id, sql_execution_id) WHERE n.name LIKE 'Scan %'{qw}", qp)
    for r in rows:
        tbl, how, _ = _scan_table(r["name"] or "")
        if not tbl or how != "reads files":
            continue
        mt = {}
        try:
            mt = {x.get("name"): x.get("total") or 0 for x in json.loads(r.get("metrics_json") or "[]")}
        except (ValueError, TypeError):
            pass
        t = out.setdefault(tbl, {"table": tbl, "scans": 0, "runs": set(), "size_bytes": None, "files": None,
                                 "files_read": 0, "bytes_read": 0, "files_pruned": 0, "bytes_pruned": 0,
                                 "min_file_bytes": None, "max_file_bytes": None,
                                 "partition_cols": 0, "scan_stages": 0, "bytes_from_files": 0, "rows_from_files": 0, "scan_wall_ms": 0, "scan_task_ms": 0})
        t["scans"] += 1
        if r.get("run_key"):
            t["runs"].add(r["run_key"])
        fr, fp = mt.get("number of files read", 0), mt.get("number of files pruned", 0)
        br, bp = mt.get("size of files read", 0), mt.get("number of bytes pruned") or mt.get("size of files pruned", 0)
        t["files_read"] += fr
        t["files_pruned"] += fp
        t["bytes_read"] += br
        t["bytes_pruned"] += bp
        t["partition_cols"] = max(t["partition_cols"], mt.get("number of partition columns", 0))
        lo, hi = mt.get("size of the smallest file read", 0), mt.get("size of the largest file read", 0)
        if lo and fr:
            t["min_file_bytes"] = lo if t["min_file_bytes"] is None else min(t["min_file_bytes"], lo)
        if hi:
            t["max_file_bytes"] = max(t["max_file_bytes"] or 0, hi)
        if br + bp and (t["size_bytes"] is None or br + bp > t["size_bytes"]):
            t["size_bytes"], t["files"] = br + bp, fr + fp
    if out and spath is not None:
        ssch = store.schema(con, spath)
        where, wp = "TRUE", []
        if query is not None and "sql_execution_id" in ssch:
            where, wp = "spark_context_id = ? AND sql_execution_id = ?", [query[0], int(query[1])]
        elif run is not None and "run_key" in ssch:
            where, wp = "run_key = ?", [run]
        rec = "input_records" if "input_records" in ssch else "NULL AS input_records"
        sts = store.rows(con, f"SELECT spark_context_id, stage_id, stage_attempt, rdd_scopes, input_bytes, {rec}, duration_ms FROM "
                              f"{store.src(spath)} WHERE {where}", wp)
        tm: dict[tuple, float] = {}
        if tpath is not None:
            tsch = store.schema(con, tpath)
            tw = "run_key = ?" if run is not None and query is None and "run_key" in tsch else "TRUE"
            if query is not None:
                tw = "spark_context_id = ?"
            for r in store.rows(con, f"SELECT spark_context_id, stage_id, stage_attempt, sum(task_ms) AS ms FROM {store.src(tpath)} "
                                     f"WHERE {tw} GROUP BY 1, 2, 3", [query[0]] if query is not None else [run] if tw != "TRUE" else []):
                tm[(r["spark_context_id"], r["stage_id"], r["stage_attempt"])] = r["ms"] or 0
        for st in sts:
            for sc in st.get("rdd_scopes") or []:
                tbl, how, _ = _scan_table(sc)
                if tbl in out and how == "reads files":
                    t = out[tbl]
                    t["scan_stages"] += 1
                    t["bytes_from_files"] += st.get("input_bytes") or 0
                    t["rows_from_files"] += st.get("input_records") or 0
                    t["scan_wall_ms"] += st.get("duration_ms") or 0
                    t["scan_task_ms"] += tm.get((st["spark_context_id"], st["stage_id"], st["stage_attempt"]), 0)
                    break
    for t in out.values():
        t["runs"] = len(t["runs"])
        t["avg_file_bytes"] = round(t["size_bytes"] / t["files"]) if t["size_bytes"] and t["files"] else None
        # rows per file: only an average (the logs count rows per scan, not per file)
        t["rows_per_file"] = round(t["rows_from_files"] / t["files_read"]) if t["rows_from_files"] and t["files_read"] else None
    return out


def merge_cycles(queries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The MERGEs of a run, in order: each starts where its source is materialized (or at its first MERGE step) and
    has its steps, the batch it belongs to, whether it upserts or deletes (from the source filter), its target and how
    many times it scanned the target."""
    merges: list[dict[str, Any]] = []
    cur: dict[str, Any] | None = None
    for q in queries:
        op = q.get("op") or ""
        if not op.startswith("MERGE"):
            continue
        if cur is None or op.startswith("MERGE: materialize"):
            b = _BATCH.search(q.get("description") or "")
            cur = {"batch": b.group(1) if b else None, "start": q.get("start"), "end": q.get("end"), "steps": [], "kind": None,
                   "target": None, "target_scans": 0}
            merges.append(cur)
        cur["steps"].append({"ctx": q["ctx"], "id": q["id"], "op": op})
        cur["end"] = q.get("end") or cur["end"]
        conds = " ".join(f["condition"] for f in (q.get("logic") or {}).get("filters", []))
        if op.startswith("MERGE: materialize") and cur["kind"] is None:
            cur["kind"] = "deletes" if re.search(r"_change_type = delete\b", conds) else "upserts" if "_change_type" in conds else None
        for w in q.get("writes") or []:
            cur["target"] = w["table"]
        if "scanning files" in op or "rewriting" in op:
            for r in q.get("reads") or []:
                if r.get("how") == "reads files":
                    cur["target"] = cur["target"] or r["table"]
                    cur["target_scans"] += 1
    return merges


def cluster_tables(store: Store, cid: str, run: str | None = None, ctx: str | None = None, query: int | None = None) -> dict[str, Any]:
    """Every table the cluster (or one run, or one query) read from files: size, files, average file, scans, runs, read
    from the files, scan time."""
    store.cluster_dir(cid)
    with store.connect() as con:
        st = table_stats(store, con, cid, run or None, (ctx, query) if ctx and query is not None else None)
    return {"tables": sorted(st.values(), key=lambda t: -(t["scan_task_ms"] or 0))}
