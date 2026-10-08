"""Revision 13: data spread per task (p10 / p90 / average / data-weighted median), executor resources, run names and
the cluster -> run -> query -> stage levels (cluster view, flow, run-scoped story and Timeline, stage sharing)."""

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


def test_task_dist_percentiles():
    from databricks_cluster_log_analyzer.analysis.aggregate import task_dist

    t = pd.DataFrame({"k": ["a"] * 10, "failed": [False] * 10, "task_ms": [float(i) for i in range(1, 11)],
                      "input_bytes": [float(i * 100) for i in range(1, 11)], "shuffle_read": [float("nan")] * 10,
                      "input_records": [1.0] * 10, "shuffle_read_records": [float("nan")] * 10})
    d = task_dist(t, ["k"]).loc["a"]
    assert (d["min_task_bytes_in"], d["p10_task_bytes_in"], d["p50_task_bytes_in"], d["p90_task_bytes_in"],
            d["max_task_bytes_in"]) == (100, 100, 500, 900, 1000)
    assert d["avg_task_bytes_in"] == 550 and (d["p10_task_ms"], d["p90_task_ms"]) == (1, 9)
    # half of the 5500 bytes were read by tasks of 800 bytes or more (800+900+1000 = 2700 < 2750, +700 = 3400)
    assert d["wmed_task_bytes_in"] == 700


def test_mib_parsing():
    from databricks_cluster_log_analyzer.analysis.aggregate import _mib

    assert _mib("7284m") == 7284 and _mib("4g") == 4096 and _mib("512") == 512
    assert _mib("2147483648", "b") == 2048 and _mib("nonsense") is None


def test_new_columns_written(output_root):
    d = output_root / MAIN
    st = pd.read_parquet(d / "stages.parquet")
    assert {"p10_task_ms", "p90_task_ms", "p90_task_bytes_in", "avg_task_rows_in", "wmed_task_bytes_in"} <= set(st)
    assert st["p90_task_ms"].notna().any()
    jobs = pd.read_parquet(d / "spark_jobs.parquet")
    assert {"input_bytes", "output_records", "p50_task_bytes_in", "databricks_job_name"} <= set(jobs)
    q = pd.read_parquet(d / "sql_queries.parquet")
    assert {"root_execution_id", "photon_share", "p90_task_bytes_in"} <= set(q)
    ex = pd.read_parquet(d / "executors.parquet")
    assert {"heap_mb", "unified_memory", "storage_memory", "task_cpus"} <= set(ex)
    assert "run_key" in pd.read_parquet(d / "run_story.parquet")
    runs = pd.read_parquet(d / "runs.parquet")
    assert {"program", "subject", "typical_duration_ms", "vs_typical", "job_name", "parent_run_id"} <= set(runs)


def test_large_tasks_finding():
    from databricks_cluster_log_analyzer.analysis.findings import _large_task_findings
    from databricks_cluster_log_analyzer.config import load_rules

    rules = load_rules()
    MB = 1 << 20
    st = [{"spark_context_id": "c", "stage_id": 3, "stage_attempt": 0, "p50_task_bytes_in": 400 * MB,
           "p90_task_bytes_in": 450 * MB, "duration_ms": 600_000, "tasks": 200, "disk_spill": 5 << 30,
           "start_time": 1, "spark_job_id": 1, "sql_execution_id": 2},
          {"spark_context_id": "c", "stage_id": 4, "stage_attempt": 0, "p50_task_bytes_in": 40 * MB,
           "duration_ms": 600_000, "tasks": 200, "disk_spill": 0, "start_time": 1}]
    f = _large_task_findings("x", st, rules)
    assert len(f) == 1 and f[0]["severity"] == "high" and f[0]["stage_id"] == 3
    assert "spark.sql.shuffle.partitions" in f[0]["evidence"]


def test_big_read_finding():
    from databricks_cluster_log_analyzer.analysis.findings import _big_read_findings
    from databricks_cluster_log_analyzer.config import load_rules

    rules = load_rules()
    GB = 1 << 30
    st = [{"spark_context_id": "c", "stage_id": 7, "stage_attempt": 0, "input_bytes": 408 * GB, "tasks": 3465,
           "duration_ms": 1_200_000, "start_time": 1, "spark_job_id": 1, "sql_execution_id": 2,
           "rdd_scopes": ["Scan delta my_catalog.sales.orders", "Exchange"]},
          {"spark_context_id": "c", "stage_id": 8, "stage_attempt": 0, "input_bytes": 30 * GB, "tasks": 100,
           "duration_ms": 60_000, "start_time": 2, "sql_execution_id": 3, "rdd_scopes": []},
          {"spark_context_id": "c", "stage_id": 9, "stage_attempt": 0, "input_bytes": 1 * GB, "tasks": 10,
           "duration_ms": 6_000, "start_time": 3}]
    qs = [{"spark_context_id": "c", "sql_execution_id": 3, "tables_read": ["my_catalog.sales.customers", "jdbc:src.x"]}]
    f = {r["stage_id"]: r for r in _big_read_findings("x", st, qs, rules)}
    assert set(f) == {7, 8}
    assert f[7]["severity"] == "high" and "my_catalog.sales.orders" in f[7]["entity"] and "408 GB" in f[7]["evidence"]
    # no scan in the stage: the query's only (non-JDBC) table
    assert f[8]["severity"] == "medium" and f[8]["entity"].endswith("my_catalog.sales.customers")


def test_run_names():
    from databricks_cluster_log_analyzer.analysis.runs import run_family, run_names

    d = {"spark_jobs": [{"run_key": "r1", "notebook_path": "/Shared/x/load_table", "description": "Load of sales_daily"},
                        {"run_key": "r2", "notebook_path": None, "description": "com.example.etl.MainJob args"}],
         "sql_queries": [{"run_key": "r1", "tables_written": ["main.sales.sales_daily"], "tables_read": []}]}
    n = run_names(d)
    assert n["r1"] == ("load_table", "sales_daily") and n["r2"][0] == "MainJob"
    assert run_family({"databricks_job_id": "7", "program": "a"}) != run_family({"databricks_job_id": "7", "program": "b"})


def test_cluster_view(client):
    v = get(client, f"/api/clusters/{MAIN}/cluster-view")
    assert v["runs"] and v["executors"] and v["minutes"] and "busy" in v and "apps" in v
    m = next(m for m in v["minutes"] if m["cpu_share"] is not None)
    assert 0 <= m["cpu_share"] <= 1 and m["cores"]


def test_flow(client):
    f = get(client, f"/api/clusters/{MAIN}/flow")
    assert f["nodes"] and {"key", "kind", "start", "jobs"} <= set(f["nodes"][0])
    keys = {n["key"] for n in f["nodes"]}
    assert all(e["from"] in keys and e["to"] in keys and e["kind"] in ("inside", "table", "shuffle") for e in f["edges"])


def test_gantt_run_scope(client):
    run = get(client, f"/api/clusters/{MAIN}/runs")["runs"][0]
    g = get(client, f"/api/clusters/{MAIN}/gantt", ctx=run["spark_context_id"], run=run["run_key"])
    assert g["run"] == run["run_key"] and (g["start"], g["end"]) == (run["start_time"], run["end_time"])


def test_stage_sharing(client):
    body = get(client, f"/api/clusters/{MAIN}/stages/{A}/2/0")
    sh = body["sharing"]
    assert sh is not None and sh["executors"] and sh["own_task_ms"] > 0 and "rows" in sh


def test_task_size_bands():
    from databricks_cluster_log_analyzer.analysis.aggregate import task_dist

    MB = 1 << 20
    sizes = [0, 0, 0, 5 * MB, 50 * MB, 200 * MB, 300 * MB, 400 * MB]
    t = pd.DataFrame({"k": ["a"] * 9, "failed": [False] * 8 + [True], "task_ms": [1.0] * 9,
                      "input_bytes": [float(s) for s in sizes] + [900.0 * MB], "shuffle_read": [float("nan")] * 9,
                      "input_records": [1.0] * 9, "shuffle_read_records": [float("nan")] * 9})
    d = task_dist(t, ["k"], nonzero=True).loc["a"]
    # the failed task is left out; the empty ones are counted even when the spread skips them
    assert [d[f"tasks_{b}"] for b in ("none", "lt10", "10_128", "128_256", "ge256")] == [3, 1, 1, 1, 2]
    assert d["bytes_ge256"] == 700 * MB and d["bytes_lt10"] == 5 * MB
