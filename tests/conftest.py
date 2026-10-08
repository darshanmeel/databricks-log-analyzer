"""Shared fixtures: generate the synthetic clusters once per session, build every cluster with
``pipeline.build`` and expose helpers to read the produced datasets."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
for p in (str(SRC), str(FIXTURES)):
    if p not in sys.path:
        sys.path.insert(0, p)

import make_fixtures as mf  # noqa: E402


def to_ms(v):
    """Normalize a timestamp-ish value (epoch-ms int, datetime, pandas Timestamp, None/NaT) to epoch ms."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, bool):
        raise TypeError("bool is not a timestamp")
    if isinstance(v, (int,)) or (hasattr(v, "dtype") and getattr(v, "dtype").kind in "iu"):
        return int(v)
    if isinstance(v, float):
        return int(v)
    ts = pd.Timestamp(v)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return int(ts.value // 1_000_000)


def ms(dt: datetime) -> int:
    return mf.epoch_ms(dt)


@pytest.fixture(scope="session")
def fixture_root(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("cluster_logs")
    mf.generate(root)
    return root


@pytest.fixture(scope="session")
def cluster_ids(fixture_root) -> dict:
    return {"main": mf.MAIN, "healthy": mf.HEALTHY, "empty": mf.EMPTY, "rev3": mf.REV3}


@pytest.fixture(scope="session")
def expected(fixture_root) -> dict:
    return mf.EXPECTED


@pytest.fixture(scope="session")
def pipeline_mod():
    return pytest.importorskip("databricks_cluster_log_analyzer.pipeline")


@pytest.fixture(scope="session")
def output_root(tmp_path_factory, fixture_root, pipeline_mod) -> Path:
    out = tmp_path_factory.mktemp("output")
    for cid in (mf.MAIN, mf.HEALTHY, mf.EMPTY, mf.REV3):
        pipeline_mod.build(fixture_root / cid, out)
    return out


@pytest.fixture(scope="session")
def cache_root(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("cache")


class Datasets:
    """Reads ``output/<cluster_id>/<name>.parquet`` with pandas (cached)."""

    def __init__(self, output_root: Path):
        self.root = output_root
        self._cache: dict = {}

    def path(self, cid: str, name: str) -> Path:
        return self.root / cid / f"{name}.parquet"

    def __call__(self, cid: str, name: str) -> pd.DataFrame:
        key = (cid, name)
        if key not in self._cache:
            p = self.path(cid, name)
            assert p.exists(), f"dataset {name} missing for {cid}: {p}"
            self._cache[key] = pd.read_parquet(p)
        return self._cache[key].copy()

    def summary(self, cid: str) -> dict:
        p = self.root / cid / "summary.json"
        assert p.exists(), f"summary.json missing for {cid}"
        return json.loads(p.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def ds(output_root) -> Datasets:
    return Datasets(output_root)


@pytest.fixture(scope="session")
def main_id() -> str:
    return mf.MAIN


@pytest.fixture(scope="session")
def ctx_a() -> str:
    return mf.CTX_A


@pytest.fixture(scope="session")
def ctx_b() -> str:
    return mf.CTX_B


@pytest.fixture(scope="session")
def rev3_id() -> str:
    return mf.REV3


@pytest.fixture(scope="session")
def ctx_r() -> str:
    return mf.CTX_R


@pytest.fixture(scope="session")
def rules():
    cfg = pytest.importorskip("databricks_cluster_log_analyzer.config")
    return cfg.load_rules()
