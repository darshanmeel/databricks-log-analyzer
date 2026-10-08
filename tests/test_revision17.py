"""Revision 17: findings about cores (runs waiting for a free core, the cores were full) and MERGE reading too much,
and refresh-findings for outputs built before them. All ids and names are made up."""

import shutil

import make_fixtures as mf
import pyarrow.parquet as pq
import pytest

from databricks_cluster_log_analyzer.analysis.contention import add_findings, contention_findings, first_tasks
from databricks_cluster_log_analyzer.config import load_rules
from databricks_cluster_log_analyzer.refresh import refresh_findings

CID = "0101-000000-abcd1234"


@pytest.fixture(scope="module")
def client(output_root, cache_root):
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from databricks_cluster_log_analyzer.api import server
    with TestClient(server.create_app(output_root, cache_root)) as c:
        yield c
CTX = "1111111111111111111"
MIN = 60_000
GB = 1 << 30
T0 = 1_700_000_000_000


def stage(run, sid, start, first, end, q=None):
    return {"spark_context_id": CTX, "stage_id": sid, "stage_attempt": 0, "run_key": run, "start_time": start,
            "end_time": end, "spark_job_id": sid, "sql_execution_id": q, "_first": first}


def scenario():
    """Three runs started together on one 8-core executor; a second executor arrives 7 minutes later. Run b's stages
    each wait about 20 minutes for a core and then run briefly; its MERGE reads 120 GB of the target for a 2 GB source."""
    runs, stages = [], []
    for i, k in enumerate("abc"):
        rk = f"job_run:42-{k}"
        runs.append({"run_key": rk, "spark_context_id": CTX, "start_time": T0, "end_time": T0 + 60 * MIN,
                     "program": "load_table", "subject": f"table_{k}", "label": rk, "vs_typical": 3.0 if k == "b" else 1.0,
                     "same_job_runs": 3, "typical_duration_ms": 20 * MIN})
        for j in range(3):
            s0 = T0 + j * 20 * MIN
            stages.append(stage(rk, i * 10 + j, s0, s0 + (19 * MIN if k != "a" else MIN), s0 + 20 * MIN, q=100 + i))
    firsts = {(s["spark_context_id"], s["stage_id"], s["stage_attempt"]): s["_first"] for s in stages}
    executors = [{"spark_context_id": CTX, "executor_id": "1", "cores": 8, "added_time": T0 - MIN, "removed_time": None},
                 {"spark_context_id": CTX, "executor_id": "2", "cores": 8, "added_time": T0 + 7 * MIN, "removed_time": None},
                 {"spark_context_id": CTX, "executor_id": "driver", "cores": 8, "added_time": T0 - 2 * MIN}]
    queries = [
        {"spark_context_id": CTX, "sql_execution_id": 7, "run_key": "job_run:42-b", "start_time": T0,
         "description": "Streaming batch 3 · MERGE operation - materialize source", "input_bytes": 2 * GB},
        {"spark_context_id": CTX, "sql_execution_id": 8, "run_key": "job_run:42-b", "start_time": T0 + MIN,
         "description": "Streaming batch 3 · MERGE operation - scanning files for matches", "input_bytes": 30 * GB},
        {"spark_context_id": CTX, "sql_execution_id": 9, "run_key": "job_run:42-b", "start_time": T0 + 2 * MIN,
         "description": "Streaming batch 3 · MERGE operation - Rewriting 321 files and writing modified data",
         "input_bytes": 120 * GB, "output_bytes": 15 * GB, "disk_spill": 60 * GB, "input_records": 5_000_000},
        # a small MERGE: not flagged
        {"spark_context_id": CTX, "sql_execution_id": 10, "run_key": "job_run:42-c", "start_time": T0,
         "description": "MERGE operation - Rewriting 2 files", "input_bytes": GB},
    ]
    info = [{"spark_context_id": CTX, "min_workers": 1, "max_workers": 2}]
    return runs, stages, firsts, executors, queries, info


def test_contention_findings():
    runs, stages, firsts, executors, queries, info = scenario()
    out = contention_findings(CID, stages, firsts, runs, executors, queries, info, load_rules())
    by = {}
    for f in out:
        by.setdefault(f["category"], []).append(f)
    # runs b and c waited 57 of their 60 minutes; run a did not wait
    waited = {f["run_key"]: f for f in by["waited_for_cores"]}
    assert set(waited) == {"job_run:42-b", "job_run:42-c"}
    assert waited["job_run:42-b"]["severity"] == "medium"  # 3x its usual
    # run c is at its usual, but it mostly waited (57 of 60 minutes): flagged whatever its "x usual"
    assert waited["job_run:42-c"]["severity"] == "medium" and "It mostly waited" in waited["job_run:42-c"]["evidence"]
    assert "Waited 57 m" in waited["job_run:42-b"]["evidence"] and "stages waited longer than they ran" in waited["job_run:42-b"]["evidence"]
    cf = by["cores_full"][0]
    assert cf["severity"] == "high" and "Up to 3 runs ran at once" in cf["evidence"]
    assert "8 cores" in cf["evidence"] and "16" in cf["evidence"] and "between 1 and 2 workers" in cf["evidence"]
    mr = by["merge_rewrite"]
    assert [f["sql_execution_id"] for f in mr] == [9]
    assert mr[0]["severity"] == "high" and "rewrote 321 files" in mr[0]["evidence"] and "2.0 GB" in mr[0]["evidence"]
    assert mr[0]["run_key"] == "job_run:42-b"


def test_add_findings_numbers_and_run_counts():
    runs, stages, firsts, executors, queries, info = scenario()
    old = [{"finding_id": "F001", "severity": "medium", "category": "disk_spill", "run_key": "job_run:42-a", "ts": T0},
           {"finding_id": "F002", "severity": "low", "category": "waited_for_cores", "run_key": "job_run:42-a", "ts": T0}]
    new = contention_findings(CID, stages, firsts, runs, executors, queries, info, load_rules())
    allf = add_findings(old, runs, new, replace=("waited_for_cores", "cores_full", "merge_rewrite"))
    ids = [f["finding_id"] for f in allf]
    assert ids[0] == "F001" and "F002" in ids and len(set(ids)) == len(ids)  # the replaced F002 is gone, its id reused
    assert not any(f["category"] == "waited_for_cores" and f["run_key"] == "job_run:42-a" for f in allf)
    r = {x["run_key"]: x for x in runs}
    assert r["job_run:42-a"]["findings"] == 1 and r["job_run:42-a"]["max_severity"] == "medium"
    assert r["job_run:42-b"]["findings"] == 2 and r["job_run:42-b"]["max_severity"] == "high"


def test_first_tasks_from_rows():
    rows = [{"spark_context_id": CTX, "stage_id": 1, "stage_attempt": 0, "launch_time": T0 + 5},
            {"spark_context_id": CTX, "stage_id": 1, "stage_attempt": 0, "launch_time": T0 + 2}]
    assert first_tasks(rows) == {(CTX, 1, 0): T0 + 2}


def test_refresh_findings_is_repeatable(output_root, tmp_path):
    d = tmp_path / mf.MAIN
    shutil.copytree(output_root / mf.MAIN, d)
    before = pq.read_table(d / "findings.parquet").num_rows
    a = refresh_findings(d, load_rules())
    n1 = pq.read_table(d / "findings.parquet").num_rows
    b = refresh_findings(d, load_rules())
    n2 = pq.read_table(d / "findings.parquet").num_rows
    assert a["added"] == b["added"] and n1 == n2
    assert n1 >= before
    ids = pq.read_table(d / "findings.parquet").column("finding_id").to_pylist()
    assert len(set(ids)) == len(ids)


def test_task_columns_endpoint(client):
    """Every task of a stage, a job or a query as columns, with the executors and each stage's wait for a core."""
    st = max(client.get(f"/api/clusters/{mf.MAIN}/datasets/stages?limit=500").json()["rows"], key=lambda s: s.get("tasks") or 0)
    d = client.get(f"/api/clusters/{mf.MAIN}/task-columns", params={"ctx": st["spark_context_id"], "stage": st["stage_id"],
                                                                    "attempt": st["stage_attempt"]}).json()
    assert d["n"] == d["total"] == st["tasks"] and not d["sampled"]
    assert len(d["cols"]["task_ms"]) == d["n"] and d["present"]["task_ms"]
    assert d["stage_waits"] and d["stage_waits"][0]["wait_ms"] >= 0
    j = client.get(f"/api/clusters/{mf.MAIN}/task-columns", params={"ctx": st["spark_context_id"], "job": st["spark_job_id"]}).json()
    assert j["n"] >= d["n"] and len(j["stage_waits"]) >= 1


def test_task_columns_job_sharing(client):
    """Revision 18: a job's tasks also come with what else ran on its executors over its time, per executor."""
    st = max(client.get(f"/api/clusters/{mf.MAIN}/datasets/stages?limit=500").json()["rows"], key=lambda s: s.get("tasks") or 0)
    j = client.get(f"/api/clusters/{mf.MAIN}/task-columns", params={"ctx": st["spark_context_id"], "job": st["spark_job_id"]}).json()
    sh = j["sharing"]
    assert sh and sh["end"] > sh["start"] and sh["executors"]
    mine = [x for x in sh["by_exec"] if x["mine"] is True]
    assert sum(x["tasks"] for x in mine) == j["total"]
    assert {x["executor_id"] for x in sh["by_exec"] if x["mine"] is None} == set(sh["executors"])
    s = client.get(f"/api/clusters/{mf.MAIN}/task-columns", params={"ctx": st["spark_context_id"], "stage": st["stage_id"],
                                                                    "attempt": st["stage_attempt"]}).json()
    assert "sharing" not in s


@pytest.mark.parametrize("kind", ["stages", "jobs", "queries", "tasks"])
@pytest.mark.parametrize("by", ["duration", "wait", "spill", "shuffle", "read"])
def test_top(client, kind, by):
    """Revision 18: the top stages, jobs, queries or tasks by one measure, of the cluster or one run, biggest first."""
    d = client.get(f"/api/clusters/{mf.MAIN}/top", params={"kind": kind, "by": by, "limit": 5}).json()
    assert d["total"] >= len(d["rows"]) > 0
    if by == "duration":
        # a task's slowest is waited to start + ran
        ms = [((r.get("task_ms") or 0) + (r.get("wait_ms") or 0)) if kind == "tasks" else r.get("duration_ms") or 0 for r in d["rows"]]
        assert ms == sorted(ms, reverse=True)
    rk = next((r["run_key"] for r in d["rows"] if r.get("run_key")), None)
    if rk:
        one = client.get(f"/api/clusters/{mf.MAIN}/top", params={"kind": kind, "by": by, "run": rk}).json()
        assert all(r["run_key"] == rk for r in one["rows"]) and one["total"] <= d["total"] or one["total"] >= 1


def test_top_search_and_bad_input(client):
    st = client.get(f"/api/clusters/{mf.MAIN}/top", params={"kind": "stages", "limit": 1}).json()["rows"][0]
    d = client.get(f"/api/clusters/{mf.MAIN}/top", params={"kind": "stages", "q": str(st["stage_id"])}).json()
    assert any(r["stage_id"] == st["stage_id"] for r in d["rows"])
    assert client.get(f"/api/clusters/{mf.MAIN}/top", params={"kind": "nope"}).status_code == 400


def test_advice_lists_its_stages(client):
    for a in client.get(f"/api/clusters/{mf.MAIN}/settings").json()["advice"]:
        assert len(a["stages"]) <= 50 and a["stage_count"] == len(a["stages"])
        for s in a["stages"]:
            assert {"spark_context_id", "stage_id", "stage_attempt", "run_key"} <= set(s)


def test_top_by_processing_time(client):
    """Revision 19: 'ran' ranks by processing time (running, not waiting for cores) and returns it as ran_ms."""
    for kind in ("stages", "jobs", "queries", "tasks"):
        r = client.get(f"/api/clusters/{mf.MAIN}/top", params={"kind": kind, "by": "ran", "limit": 50})
        assert r.status_code == 200, r.text
        rows = r.json()["rows"]
        ran = [x.get("ran_ms") or 0 for x in rows]
        assert ran == sorted(ran, reverse=True)


def test_top_by_biggest_task_read(client):
    """Revision 20: the biggest file read and the biggest shuffle read of one task, apart, and ranked by either."""
    for kind in ("stages", "jobs", "queries", "tasks"):
        for by, col in (("task_read", "max_task_input"), ("task_shuffle", "max_task_shuffle")):
            r = client.get(f"/api/clusters/{mf.MAIN}/top", params={"kind": kind, "by": by, "limit": 50})
            assert r.status_code == 200, r.text
            vals = [x.get(col) or 0 for x in r.json()["rows"]]
            assert vals == sorted(vals, reverse=True)
    runs = client.get(f"/api/clusters/{mf.MAIN}/runs").json()["runs"]
    assert all("max_task_input" in x and "max_task_shuffle" in x for x in runs)


def test_run_tables(client):
    """Revision 20: per run, the tables read and written, and per query its operation, jobs and stages."""
    runs = client.get(f"/api/clusters/{mf.MAIN}/runs").json()["runs"]
    assert runs
    for run in runs[:5]:
        r = client.get(f"/api/clusters/{mf.MAIN}/run-tables", params={"run": run["run_key"]})
        assert r.status_code == 200, r.text
        d = r.json()
        assert set(d) >= {"run_key", "tables", "queries"}
        for t in d["tables"]:
            assert t["role"] in ("read", "written", "read and written", "written (its Delta log read too)", "read (only its Delta log)")
        for q in d["queries"]:
            assert q["op"] and isinstance(q["stages"], list)
            assert all(s["spark_job_id"] in q["jobs"] for s in q["stages"] if s["spark_job_id"] is not None)


PLAN = """== Physical Plan ==
AdaptiveSparkPlan (9)
+- SortMergeJoin LeftOuter (8)

(1) Scan parquet my_catalog.sales.orders
Output [3]: [order_id#1L, day#2, amount#3]
PartitionFilters: [isnotnull(day#2), (day#2 >= 2026-01-01)]
PushedFilters: [IsNotNull(order_id)]
DataFilters: [isnotnull(order_id#1L)]

(2) Filter
Input [3]: [order_id#1L, day#2, amount#3]
Condition : (amount#3 > 0)

(3) Filter
Condition : _source_row_present_

(4) HashAggregate
Keys [2]: [customer_id#7, day#2]

(5) WindowGroupLimit
Arguments: [order_id#1L], [version#9 DESC NULLS LAST], rank(version#9), 1, Final

(8) SortMergeJoin
Left keys [4]: [coalesce(order_id#1L, 0), isnull(order_id#1L), coalesce(day#2, ), isnull(day#2)]
Right keys [4]: [coalesce(order_id#11L, 0), isnull(order_id#11L), coalesce(day#12, ), isnull(day#12)]
Join type: LeftOuter
Join condition: None
"""


def test_plan_logic():
    """Revision 20: joins (null-safe keys folded back), user filters (internal ones left out), what a scan pushed
    down, group-by keys and window partitions, from a plan's details. Made-up plan."""
    from databricks_cluster_log_analyzer.api.queries import plan_logic
    lg = plan_logic(PLAN)
    j = lg["joins"][0]
    assert (j["how"], j["type"], j["left_keys"], j["null_safe"], j["condition"]) == ("SortMergeJoin", "LeftOuter", ["order_id", "day"], True, None)
    assert [f["condition"] for f in lg["filters"]] == ["(amount > 0)"]
    sc = lg["scans"][0]
    assert sc["table"] == "my_catalog.sales.orders"
    assert sc["partition_filters"] == ["isnotnull(day)", "(day >= 2026-01-01)"]
    assert sc["pushed_filters"] == ["IsNotNull(order_id)"]
    assert lg["groups"][0]["keys"] == ["customer_id", "day"]
    assert lg["windows"][0]["partition_by"] == ["order_id"] and lg["windows"][0]["order_by"] == ["version DESC NULLS LAST"]
    assert plan_logic(None) == {"joins": [], "filters": [], "scans": [], "groups": [], "windows": [], "facts": []}


def test_table_stats_and_merge_cycles(client):
    """Revision 20: the cluster's tables with size and scan cost, and per run the MERGE cycles."""
    r = client.get(f"/api/clusters/{mf.MAIN}/tables")
    assert r.status_code == 200, r.text
    for t in r.json()["tables"]:
        assert t["scans"] >= 1 and isinstance(t["runs"], int)
    every = {t["table"]: t["scans"] for t in r.json()["tables"]}
    # one query's (or one run's) tables are a subset of the cluster's, scanned no more often
    qs = client.get(f"/api/clusters/{mf.MAIN}/datasets/sql_queries", params={"limit": 50}).json()["rows"]
    for q in qs[:10]:
        one = client.get(f"/api/clusters/{mf.MAIN}/tables", params={"ctx": q["spark_context_id"], "query": q["sql_execution_id"]})
        assert one.status_code == 200, one.text
        for t in one.json()["tables"]:
            assert t["table"] in every and t["scans"] <= every[t["table"]]
    for rk in {q.get("run_key") for q in qs if q.get("run_key")}:
        for t in client.get(f"/api/clusters/{mf.MAIN}/tables", params={"run": rk}).json()["tables"]:
            assert t["scans"] <= every[t["table"]]
    from databricks_cluster_log_analyzer.api.queries import merge_cycles
    qs = [
        {"ctx": "1", "id": 1, "op": "MERGE: materialize source", "description": "batch 7 · MERGE", "start": 1, "end": 2, "reads": [], "writes": [],
         "logic": {"filters": [{"condition": "(_change_type = insert)"}]}},
        {"ctx": "1", "id": 2, "op": "MERGE: scanning files for matches", "description": "", "start": 2, "end": 3,
         "reads": [{"table": "my_catalog.s.t", "how": "reads files"}], "writes": [], "logic": {"filters": []}},
        {"ctx": "1", "id": 3, "op": "MERGE: materialize source", "description": "batch 7", "start": 4, "end": 5, "reads": [], "writes": [],
         "logic": {"filters": [{"condition": "((_tx_rank = 1) AND (_change_type = delete))"}]}},
        {"ctx": "1", "id": 4, "op": "MERGE: scanning files for matches", "description": "", "start": 5, "end": 6,
         "reads": [{"table": "my_catalog.s.t", "how": "reads files"}], "writes": [], "logic": {"filters": []}},
    ]
    ms = merge_cycles(qs)
    assert [(m["batch"], m["kind"], m["target"], m["target_scans"], [s["id"] for s in m["steps"]]) for m in ms] == [
        ("7", "upserts", "my_catalog.s.t", 1, [1, 2]), ("7", "deletes", "my_catalog.s.t", 1, [3, 4])]


def test_stage_what_names_stages_by_what_they_do():
    from databricks_cluster_log_analyzer.api.queries import stage_what
    GB_ = 1 << 30
    assert stage_what(["Exchange", "Scan parquet my_catalog.sales.orders", "Filter"], {"input_bytes": 408 * GB_}) \
        == "scan my_catalog.sales.orders, read 408 GB"
    assert stage_what(["AQEShuffleRead", "Sort", "SortMergeJoin"], {"shuffle_read": 34 * GB_, "disk_spill": 67 * GB_}) \
        == "sort-merge join, shuffle read 34.0 GB, spilled 67.0 GB"
    assert stage_what(["WriteFiles", "AQEShuffleRead", "Sort"], {"output_bytes": 45 * GB_}) == "write 45.0 GB"
    assert stage_what([], {}) is None


def test_planned_decommission_is_not_an_executor_lost():
    from databricks_cluster_log_analyzer.analysis.findings import _real_losses
    rules = load_rules()
    lines = [{"signal": "executor_lost", "line": "Asked to decommission executor app-1/0"},
             {"signal": "disk_spill", "line": "spilling sort data"}]
    planned = [{"removed_reason": "kill request from HTTP endpoint", "removal_category": "autoscale"},
               {"removed_reason": "cluster termination", "removal_category": "termination"}]
    assert [r["signal"] for r in _real_losses(lines, planned, rules)] == ["disk_spill"]
    lost = planned + [{"removed_reason": "worker lost", "removal_category": "lost"}]
    assert len(_real_losses(lines, lost, rules)) == 2
    real = lines + [{"signal": "executor_lost", "line": "ExecutorLostFailure (executor 3 exited)"}]
    assert len(_real_losses(real, planned, rules)) == 2


def test_plan_facts():
    from databricks_cluster_log_analyzer.api.queries import plan_logic
    p = "(1) Exchange\nArguments: deltaoptimizedwritepartitioning(x, 200)\n(2) Project [__is_cdc, deletionVector]\n"
    facts = plan_logic(p)["facts"]
    assert any("Change Data Feed" in f for f in facts) and any("Optimized write" in f for f in facts)
    assert any("Deletion vectors" in f for f in facts)
