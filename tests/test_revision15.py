"""Revision 15: log signals ignore config/telemetry dumps, AQE re-plans are not failures, readable streaming batch
names, AQE / Photon use per query, how a run ended."""
import pytest

import make_fixtures as mf
from databricks_cluster_log_analyzer.analysis.aggregate import mark_replanned
from databricks_cluster_log_analyzer.api.queries import engine_use, engine_summary
from databricks_cluster_log_analyzer.config import load_rules
from databricks_cluster_log_analyzer.parsing.eventlog import readable_description
from databricks_cluster_log_analyzer.parsing.loglines import find_signal

R = load_rules()


@pytest.fixture(scope="module")
def client(output_root, cache_root):
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient
    from databricks_cluster_log_analyzer.api import server
    with TestClient(server.create_app(output_root, cache_root)) as c:
        yield c


def get(client, url):
    r = client.get(url)
    assert r.status_code == 200, r.text
    return r.json()


def sig(line):
    hit = find_signal(line, R)
    return hit and hit[0]


def test_config_lines_are_not_signals():
    assert sig("spark.decommission.enabled=true") is None
    assert sig("spark.storage.decommission.rddBlocks.enabled=true") is None
    assert sig("spark.driver.maxResultSize=4g") is None
    assert sig("spark.hadoop.fs.s3a.retry.throttle.interval=500ms") is None
    assert sig("26/01/01 00:00:01 WARN ThrottledLogger$: [30 messages suppressed]") is None
    assert sig("x [$anonfun$throttledInfoLogger$1] y") is None
    assert sig("INFO ErrorEventListener: monitoring module errors with a throttling threshold of 5") is None
    assert sig('2026-01-01 00:00:01 - com.databricks.UsageLogging - {"metric":"configChangeEvent", '
               '"AnalysisException"}') is None


def test_real_lines_still_match():
    assert sig("WARN TaskSetManager: Lost task 1.0: ExecutorLostFailure (executor 3 exited)") == "executor_lost"
    assert sig("INFO BlockManager: Decommissioning executor 2") == "executor_lost"
    assert sig("Total size of serialized results of 10 tasks (4.1 GiB) is bigger than "
               "spark.driver.maxResultSize (4.0 GiB)") == "driver_unresponsive"
    assert sig("org.apache.spark.sql.AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] x") == "schema_error"
    assert sig("Request was throttled by the storage account") == "storage_throttling"
    assert sig("Status Code: 503; Error Code: SlowDown") == "storage_throttling"


def test_aqe_replanned_is_not_failed():
    msg = ("[SPARK_JOB_CANCELLED] Job 7 cancelled Adaptive query execution has replanned the query and cancelled "
           "unused stages SQLSTATE: HY008")
    jobs = [{"result": "JobFailed", "error": msg}, {"result": "JobFailed", "error": "boom"}]
    stages = [{"status": "failed", "failure_reason": msg}, {"status": "failed", "failure_reason": "boom"}]
    mark_replanned(jobs, stages, R)
    assert [j["result"] for j in jobs] == ["JobReplanned", "JobFailed"]
    assert [s["status"] for s in stages] == ["replanned", "failed"]


def test_streaming_batch_description():
    d = "\nid = 11111111-2222-3333-4444-555555555555\nrunId = 66666666-7777-8888-9999-000000000000\nbatch = 12"
    assert readable_description(d) == "Streaming batch 12 · stream 11111111"
    assert readable_description("select 1") == "select 1"
    assert readable_description(None) is None


def test_engine_use():
    adaptive = {"final_plan": "== Physical Plan ==\nAdaptiveSparkPlan isFinalPlan=true\n+- Exchange hashpartitioning"}
    cmd = {"final_plan": "== Physical Plan ==\nExecute WriteIntoDeltaCommand (3)"}
    shuffle = {"final_plan": "== Physical Plan ==\nHashAggregate (3)\n+- Exchange (2)"}
    u = engine_use(adaptive, "STANDARD", None)
    assert u["aqe"]["state"] == "used" and u["photon"]["state"] == "no" and "STANDARD" in u["photon"]["why"]
    assert engine_use(cmd, None, None)["aqe"]["state"] == "na"
    assert engine_use(shuffle, None, "false")["aqe"]["state"] == "no"
    assert engine_use(dict(shuffle, photon_share=0.5), "PHOTON", None)["photon"]["state"] == "used"
    s = engine_summary([adaptive, cmd, shuffle], "STANDARD", None)
    assert s["aqe"]["could"] == 2 and s["aqe"]["used"] == 1


def test_run_end_endpoint(client):
    MAIN = mf.MAIN
    runs = get(client, f"/api/clusters/{MAIN}/runs")["runs"]
    v = get(client, f"/api/clusters/{MAIN}/run-end?run={runs[0]['run_key']}")
    assert v["lines"] and {"tone", "text"} <= set(v["lines"][0])
    assert {"aqe", "photon"} <= set(v["engine"])


def test_errors_hit_runs_and_stages(client):
    MAIN = mf.MAIN
    groups = get(client, f"/api/clusters/{MAIN}/errors")
    assert groups and all("hit_runs" in g and "hit_stages" in g for g in groups)
    hit = [g for g in groups if g["hit_runs"]]
    if hit:  # a run's scoped list holds only the groups that hit it, each counting at most its lines
        run = hit[0]["hit_runs"][0]["run_key"]
        scoped = get(client, f"/api/clusters/{MAIN}/errors?run={run}")
        keys = {g["fingerprint"] for g in scoped}
        assert hit[0]["fingerprint"] in keys
        full = {g["fingerprint"]: g["occurrences"] for g in groups}
        assert all(g["occurrences"] <= full[g["fingerprint"]] for g in scoped)
        assert all(all(h["run_key"] == run for h in g["hit_runs"]) for g in scoped)


def test_error_on_executor_hits_its_stage(tmp_path):
    """An executor error logged while a task ran on that executor hits that task's stage and run."""
    import duckdb  # noqa: F401
    import pandas as pd
    from databricks_cluster_log_analyzer.api.queries import Store, error_groups
    cid = "0101-000000-abcd1234"
    d = tmp_path / cid
    d.mkdir()
    t = pd.Timestamp("2026-01-01 10:00:00")
    pd.DataFrame([{"cluster_id": cid, "source": "executor", "app_id": None, "executor_id": "1", "file_path": "f",
                   "file_name": "f", "seq": 1, "ts": t + pd.Timedelta(seconds=30), "exception_class": "java.io.IOException",
                   "message": "boom", "top_frames": ["a"], "fingerprint": "fp1", "user_frame": None}]).to_parquet(d / "log_errors.parquet")
    pd.DataFrame([
        {"spark_context_id": "c", "stage_id": 7, "stage_attempt": 0, "executor_id": "1", "launch_time": t,
         "finish_time": t + pd.Timedelta(minutes=1), "failed": True, "run_key": "r1"},
        {"spark_context_id": "c", "stage_id": 8, "stage_attempt": 0, "executor_id": "1", "launch_time": t,
         "finish_time": t + pd.Timedelta(minutes=1), "failed": False, "run_key": "r2"},
        {"spark_context_id": "c", "stage_id": 9, "stage_attempt": 0, "executor_id": "2", "launch_time": t,
         "finish_time": t + pd.Timedelta(minutes=1), "failed": True, "run_key": "r3"},
    ]).to_parquet(d / "tasks.parquet")
    g = error_groups(Store(tmp_path), cid)[0]
    assert [s["stage_id"] for s in g["hit_stages"]] == [7]  # the failed task on executor 1, not stage 8 or 9
    assert [r["run_key"] for r in g["hit_runs"]] == ["r1"]
    assert error_groups(Store(tmp_path), cid, "r2") == []


def test_memory_use_per_minute(tmp_path):
    """Heap after GC per minute: executors summed against their heap, the fullest one, the driver; carried forward."""
    import pandas as pd
    from databricks_cluster_log_analyzer.api.queries import Store, memory_use
    cid = "0101-000000-abcd1234"
    d = tmp_path / cid
    d.mkdir()
    t = pd.Timestamp("2026-01-01 10:00:10")
    rows = [("executor", "1", 0, 800), ("executor", "2", 0, 200), ("driver", None, 0, 100), ("executor", "1", 2, 900)]
    pd.DataFrame([{"cluster_id": cid, "source": s, "app_id": None, "executor_id": e, "file_path": "f", "seq": i,
                   "ts": t + pd.Timedelta(minutes=m), "gc_id": i, "kind": "Pause Young", "cause": None,
                   "heap_before_mb": 1000.0, "heap_after_mb": float(a), "heap_total_mb": 1000.0, "pause_ms": 1.0}
                  for i, (s, e, m, a) in enumerate(rows)]).to_parquet(d / "gc_events.parquet")
    st = Store(tmp_path)
    with st.connect() as con:
        mem = memory_use(st, con, cid, [{"executor_id": "1", "heap_mb": 1000}, {"executor_id": "2", "heap_mb": 1000}])
    assert [m["share"] for m in mem] == [0.5, 0.5, 0.55]  # minute 1 carries minute 0 forward
    assert mem[2]["max_share"] == 0.9 and mem[2]["max_exec"] == "1"
    assert mem[0]["driver_share"] == 0.1


def test_stage_detail_executor_spread_and_other_tasks(client):
    body = get(client, f"/api/clusters/{mf.MAIN}/stages/{mf.CTX_A}/2/0")
    pl = body["placement"]
    assert pl and {"p10_task_ms", "p50_task_ms", "p90_task_ms", "p50_bytes_in", "disk_spill"} <= set(pl[0])
    for r in pl:
        if r["p50_task_ms"] is not None:
            assert r["p10_task_ms"] <= r["p50_task_ms"] <= r["p90_task_ms"] <= r["max_task_ms"]
    sh = body.get("sharing")
    if sh:
        assert "other_tasks" in sh and sh["other_task_count"] >= len(sh["other_tasks"])
        assert all((t["stage_id"], t["stage_attempt"]) != (2, 0) for t in sh["other_tasks"])
        # Revision 17: per executor, this stage's tasks and the other stages' tasks, side by side
        mine = [x for x in sh["by_exec"] if x["mine"]]
        assert sum(x["tasks"] for x in mine) == sum(r["tasks"] for r in pl)
        assert all(x["stages"] >= 1 for x in sh["by_exec"] if not x["mine"])


def test_stage_why_endpoint(client):
    st = get(client, f"/api/clusters/{mf.MAIN}/stage-why?ctx={mf.CTX_A}")["stages"]
    assert st and {"stage_id", "stage_attempt", "wait_ms", "tail_ms", "other_share"} <= set(st[0])
    for r in st:
        assert r["wait_ms"] is None or r["wait_ms"] >= 0
        assert r["other_share"] is None or 0 <= r["other_share"] <= 1


def test_query_time_steps(client):
    """Revision 16: a query step by step: each stage's wait for a core and run, adding up to the bar's totals."""
    flow = get(client, f"/api/clusters/{mf.MAIN}/flow?ctx={mf.CTX_A}")
    q = next(n for n in flow["nodes"] if n["kind"] == "query" and n["ctx"] == mf.CTX_A)
    t = get(client, f"/api/clusters/{mf.MAIN}/query-time?ctx={mf.CTX_A}&query={q['id']}")
    assert t["steps"], "a query with jobs has at least one stage step"
    assert {"steps", "start", "end", "waiting_ms", "running_ms"} <= set(t)
    for s in t["steps"]:
        assert s["submitted"] <= s["first_task"] <= s["end"]
        assert s["wait_ms"] == s["first_task"] - s["submitted"] and s["run_ms"] == s["end"] - s["first_task"]
    assert [s["submitted"] for s in t["steps"]] == sorted(s["submitted"] for s in t["steps"])
    if t["total_ms"]:
        assert t["waiting_ms"] + t["running_ms"] + t["outside_ms"] == t["total_ms"]


def test_query_waits_cut_out_running_time():
    """A query's wait for cores counts only while none of its stages ran a task."""
    from databricks_cluster_log_analyzer.api.queries import _query_waits
    st = lambda sid, s, e: {"spark_context_id": "c", "sql_execution_id": 7, "stage_id": sid, "stage_attempt": 0,
                            "start_time": s, "end_time": e}
    # stage 1 waits 0-10 s then runs to 20 s; stage 2 is submitted at 5 s, waits until 30 s, runs to 40 s
    firsts = {("c", 1, 0): 10_000, ("c", 2, 0): 30_000}
    waits = _query_waits([st(1, 0, 20_000), st(2, 5_000, 40_000)], firsts)
    assert waits == {("c", 7): [[0, 10_000], [20_000, 30_000]]}
    # under a second of waiting is not worth drawing
    assert _query_waits([st(1, 0, 5_000)], {("c", 1, 0): 500}) == {}


def test_runs_count_failed_stages(client):
    """Each run says how many of its stage attempts failed, matching the stages table."""
    MAIN = mf.MAIN
    runs = get(client, f"/api/clusters/{MAIN}/runs")["runs"]
    assert all(isinstance(r["failed_stages"], int) for r in runs)
    stages = get(client, f"/api/clusters/{MAIN}/datasets/stages?limit=5000&run=")["rows"]
    failed = sum(1 for s in stages if s.get("status") == "failed" and s.get("run_key"))
    assert sum(r["failed_stages"] for r in runs) == failed


def test_runs_waiting_and_processing_time(client):
    """Each run with stages has its time waiting for a core and running tasks, neither longer than the run."""
    runs = get(client, f"/api/clusters/{mf.MAIN}/runs")["runs"]
    timed = [r for r in runs if r.get("running_ms") is not None]
    assert timed
    for r in timed:
        assert r["waiting_ms"] >= 0 and r["running_ms"] >= 0
        if r.get("duration_ms"):
            assert r["waiting_ms"] + r["running_ms"] <= r["duration_ms"] + 1000


def test_run_steps(client):
    """A run step by step: its queries in start order, waiting + running + outside = the run's time."""
    runs = get(client, f"/api/clusters/{mf.MAIN}/runs")["runs"]
    r = max(runs, key=lambda x: x.get("stages") or 0)
    d = get(client, f"/api/clusters/{mf.MAIN}/run-steps?run={r['run_key']}")
    assert d["groups"]
    starts = [g["start"] for g in d["groups"]]
    assert starts == sorted(starts)
    assert d["waiting_ms"] + d["running_ms"] + d["outside_ms"] == d["total_ms"] or d["outside_ms"] == 0
    for g in d["groups"]:
        assert g["stages"] and g["waiting_ms"] >= 0 and g["running_ms"] >= 0
        assert all(s["wait_ms"] >= 0 and s["run_ms"] >= 0 for s in g["stages"])
