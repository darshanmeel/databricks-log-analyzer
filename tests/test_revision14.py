"""Revision 14: settings that decide speed and cost, idle compute, and where a stage's big tasks ran."""

from __future__ import annotations

import pandas as pd
import pytest

import make_fixtures as mf

pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

server = pytest.importorskip("databricks_cluster_log_analyzer.api.server")
MAIN, A = mf.MAIN, mf.CTX_A


@pytest.fixture(scope="module")
def client(output_root, cache_root):
    with TestClient(server.create_app(output_root, cache_root)) as c:
        yield c


def get(client, url, **params):
    r = client.get(url, params=params or None)
    assert r.status_code == 200, r.text
    return r.json()


def test_settings_dataset(output_root):
    st = pd.read_parquet(output_root / MAIN / "settings.parquet")
    assert {"key", "value", "source", "default", "session_values"} <= set(st)
    sp = st[st["key"] == "spark.sql.shuffle.partitions"]
    assert not sp.empty


def test_note_settings_session_differs():
    from databricks_cluster_log_analyzer.parsing.eventlog import EventTables, _note_settings

    t = EventTables(cluster_id="c", spark_context_id="x")
    _note_settings(t, "x", {"spark.sql.shuffle.partitions": "200", "unrelated.key": "1"}, "cluster")
    _note_settings(t, "x", {"spark.sql.shuffle.partitions": "200"}, "session")
    _note_settings(t, "x", {"spark.sql.shuffle.partitions": "800"}, "session")
    got = t.settings["x"]
    assert set(got) == {"spark.sql.shuffle.partitions"}
    assert got["spark.sql.shuffle.partitions"] == {"cluster": "200", "session": {"800"}}


def test_compute_use_idle_stretch():
    from databricks_cluster_log_analyzer.api.queries import compute_use

    ex = [{"added_time": 0, "removed_time": 20 * 60_000, "cores": 4}]
    # busy (full) for the first 5 minutes, nothing after
    minutes = [{"minute": m * 60_000, "run_ms": 4 * 60_000} for m in range(5)]
    c = compute_use(ex, minutes, 0, 20 * 60_000)
    assert c["core_ms_up"] == 4 * 20 * 60_000 and c["core_ms_used"] == 4 * 5 * 60_000
    assert c["idle_share"] == 0.75 and c["stretch_count"] == 1
    assert (c["stretches"][0]["start"], c["stretches"][0]["end"]) == (5 * 60_000, 20 * 60_000)
    assert c["last_task"] == 5 * 60_000


def test_settings_endpoint(client):
    v = get(client, f"/api/clusters/{MAIN}/settings")
    assert {"settings", "advice", "env_vars", "compute"} <= set(v)
    for a in v["advice"]:
        assert a["severity"] in ("high", "medium", "info") and a["evidence"] and a["change"]


def test_stage_placement(client):
    body = get(client, f"/api/clusters/{MAIN}/stages/{A}/2/0")
    pl = body["placement"]
    assert pl and {"executor_id", "tasks", "tasks_ge256", "bytes_in"} <= set(pl[0])
    assert sum(r["tasks"] for r in pl) > 0


def test_start_bursts():
    from databricks_cluster_log_analyzer.api.queries import _start_bursts
    t = 1_700_000_000_000
    runs = [{"start_time": t + i * 1000} for i in range(6)]          # 6 runs in 5 s: one burst
    runs += [{"start_time": t + 3_600_000 + i * 600_000} for i in range(6)]  # 10 min apart: no burst
    runs += [{"start_time": None}]
    b = _start_bursts(runs)
    assert len(b) == 1 and len(b[0]["runs"]) == 6
    assert b[0]["last"] - b[0]["first"] == 5000


def test_group_endpoint(client):
    runs = get(client, f"/api/clusters/{MAIN}/runs")["runs"]
    keys = [r["run_key"] for r in runs]
    v = get(client, f"/api/clusters/{MAIN}/group?runs={','.join(keys)}")
    assert len(v["runs"]) == len(keys)
    for f in v["findings"]:
        assert 1 <= f["runs"] <= len(keys) and f["failed_runs"] <= f["runs"]
    assert sum(r["findings"] for r in v["runs"]) >= sum(f["count"] for f in v["findings"])
    assert get(client, f"/api/clusters/{MAIN}/group?runs=nope")["runs"] == []
