"""Revision 19: storage reads apart from DataFrame-cache reads, incident chains that do not invent losses or links,
the cache advice first, quieter platform noise, and refresh-findings recomputing more. All ids, names and numbers are
made up."""

import json
import shutil

import make_fixtures as mf
import pandas as pd
import pyarrow.parquet as pq

from databricks_cluster_log_analyzer import ANALYZER_REVISION
from databricks_cluster_log_analyzer.analysis.findings import (_big_read_findings, _event_findings, _exception_findings,
                                                                build_findings_rows)
from databricks_cluster_log_analyzer.analysis.incidents import build_incidents
from databricks_cluster_log_analyzer.analysis.runs import run_names
from databricks_cluster_log_analyzer.config import load_rules
from databricks_cluster_log_analyzer.parsing.eventlog import EV_STAGE_COMPLETED, EventTables, consume_events
from databricks_cluster_log_analyzer.parsing.loglines import ExceptionGrouper, find_signal, is_framework_frame
from databricks_cluster_log_analyzer.refresh import refresh_findings
from databricks_cluster_log_analyzer.store import write_parquet

H = 3_600_000
MIN = 60_000
C = "ctx"
GB = 1 << 30


# ---- #2: the cloud storage metric of a stage -------------------------------------------------------------------
def _stage_event(sid, accs):
    return {"Event": EV_STAGE_COMPLETED, "Stage Info": {
        "Stage ID": sid, "Stage Attempt ID": 0, "Stage Name": "s", "Number of Tasks": 2,
        "Submission Time": 1000, "Completion Time": 2000,
        "Accumulables": [{"ID": i, "Name": n, "Value": v} for i, (n, v) in enumerate(accs)]}}


def test_stage_reads_cloud_storage_and_disk_cache_bytes():
    t = EventTables("c", C)
    consume_events([(EV_STAGE_COMPLETED, _stage_event(1, [("cloud storage response size", "700"),
                                                           ("cloud storage response size", "300"),
                                                           ("cache hits size", "50"),
                                                           ("internal.metrics.input.bytesRead", 1200)])),
                    (EV_STAGE_COMPLETED, _stage_event(2, [("internal.metrics.input.bytesRead", 900)]))], t)
    a, b = t.stage_completed
    assert (a["cloud_bytes"], a["disk_cache_bytes"]) == (1000, 50)
    assert (b["cloud_bytes"], b["disk_cache_bytes"]) == (None, None)


def test_big_read_counts_storage_not_the_dataframe_cache():
    rules = load_rules()
    st = [{"cluster_id": "c", "spark_context_id": C, "stage_id": 7, "stage_attempt": 0, "status": "succeeded",
           "input_bytes": 120 * GB, "storage_bytes": 0, "df_cache_bytes": 120 * GB, "sql_execution_id": 3,
           "tasks": 10, "duration_ms": MIN, "start_time": 0, "rdd_scopes": ["InMemoryTableScan"]},
          {"cluster_id": "c", "spark_context_id": C, "stage_id": 8, "stage_attempt": 0, "status": "succeeded",
           "input_bytes": 60 * GB, "storage_bytes": 40 * GB, "df_cache_bytes": 20 * GB, "sql_execution_id": 3,
           "tasks": 10, "duration_ms": MIN, "start_time": 0, "rdd_scopes": ["Scan parquet shop.orders"]}]
    f = _big_read_findings("c", st, [], rules)
    assert [r["stage_id"] for r in f] == [8]
    assert "read 40 GiB from storage" in f[0]["evidence"] and "another 20 GiB came from a DataFrame cache" in f[0]["evidence"]


# ---- #5: incidents ------------------------------------------------------------------------------------------------
def _f(fid, category, ts, severity="high", **kw):
    base = {"finding_id": fid, "category": category, "severity": severity, "ts": ts, "spark_context_id": None,
            "stage_id": None, "stage_attempt": None, "executor_id": None, "sql_execution_id": None,
            "spark_job_id": None, "entity": None, "evidence": "", "fix": None, "signal": None, "fingerprint": None}
    return {**base, **kw}


def _exec(eid, category=None):
    return {"spark_context_id": C, "executor_id": eid, "host": f"10.0.0.{eid}", "added_time": 0,
            "removed_time": H if category else None, "removal_category": category}


def _by(rows):
    return {r["finding_id"]: r for r in rows}


def test_one_executor_signal_does_not_bridge_two_incidents():
    # GC lines on executor 2 first, on executor 5 ten minutes later: alone, the signal keeps only its earliest lines
    sigs = [{"signal": "gc_pressure", "line": "Full GC on executor 2", "ts": H, "executor_id": "2"},
            {"signal": "gc_pressure", "line": "Full GC on executor 5", "ts": H + 10 * MIN, "executor_id": "5"}]
    rows = _by(build_incidents("c", [_f("F1", "executor_oom", H + 1000, executor_id="2", spark_context_id=C),
                                     _f("F2", "executor_oom", H + 10 * MIN + 1000, executor_id="5", spark_context_id=C),
                                     _f("F3", "log:gc_pressure", H, severity="medium", signal="gc_pressure")],
                               [], [_exec("2", "oom"), _exec("5", "oom")], [], [], sigs, [], None, []))
    assert rows["F1"]["incident_id"] != rows["F2"]["incident_id"]
    assert rows["F1"]["caused_by"] == "F3" and rows["F2"]["caused_by"] is None
    assert rows["F3"]["executors"] == "2"


def test_autoscaled_executor_is_not_counted_as_lost():
    sigs = [{"signal": "executor_lost", "line": "Lost executor 3 on 10.0.0.3: worker decommissioned", "ts": H,
             "executor_id": None},
            {"signal": "executor_lost", "line": "Lost executor 4 on 10.0.0.4: heartbeat timed out", "ts": H + 500,
             "executor_id": None}]
    rows = build_incidents("c", [_f("F1", "log:executor_lost", H, severity="medium", signal="executor_lost")], [],
                           [_exec("3", "autoscale"), _exec("4", "lost")], [], [], sigs, [], None, [])
    assert rows[0]["incident_impact"] == "1 executor lost"


def test_same_out_of_memory_message_on_two_executors_is_two_problems():
    errs = [{"fingerprint": "a", "message": "Java heap space", "executor_id": "1", "ts": H, "source": "executor"},
            {"fingerprint": "b", "message": "Java heap space", "executor_id": "2", "ts": H + 2000, "source": "executor"}]
    rows = _by(build_incidents("c", [
        _f("F1", "exception", H, entity="java.lang.OutOfMemoryError", fingerprint="a", executor_id="1"),
        _f("F2", "exception", H + 2000, entity="java.lang.OutOfMemoryError", fingerprint="b", executor_id="2"),
        _f("F3", "jvm_full_gc", H - 1000, severity="medium", executor_id="2", spark_context_id=C)],
        [], [_exec("1", "oom"), _exec("2")], [], [], [], errs, None, []))
    assert rows["F1"]["problem_id"] != rows["F2"]["problem_id"]
    # executor 2's GC explains executor 2's out of memory, not executor 1's
    assert rows["F2"]["caused_by"] == "F3" and rows["F1"]["caused_by"] is None


def test_no_skew_to_gc_link_when_the_slow_task_was_stuck_in_gc():
    stages = [{"spark_context_id": C, "stage_id": 4, "stage_attempt": 0, "sql_execution_id": 2, "status": "succeeded",
               "failed_tasks": 0, "start_time": H, "end_time": H + 5 * MIN, "stage_name": "s", "spark_job_id": 1,
               "duration_ms": 5 * MIN}]
    tasks = pd.DataFrame([{"spark_context_id": C, "stage_id": 4, "stage_attempt": 0, "executor_id": ex,
                           "launch_time": H, "finish_time": H + d} for ex, d in (("1", 1000), ("1", 1200), ("3", 4 * MIN))])
    findings = [_f("F1", "task_skew", H + 5 * MIN, stage_id=4, stage_attempt=0, spark_context_id=C),
                _f("F2", "gc_pressure", H + 5 * MIN, severity="medium", stage_id=4, stage_attempt=0, spark_context_id=C)]
    linked = _by(build_incidents("c", findings, stages, [_exec("1"), _exec("3")], [], [], [], [], tasks, []))
    assert linked["F2"]["caused_by"] == "F1"
    stuck = findings + [_f("F3", "gc_stuck", H, executor_id="3", spark_context_id=C)]
    rows = _by(build_incidents("c", stuck, stages, [_exec("1"), _exec("3")], [], [], [], [], tasks, []))
    assert rows["F2"]["caused_by"] != "F1"


# ---- small ones -----------------------------------------------------------------------------------------------
def test_signal_lines_on_the_driver_and_jvm_full_gc_severity():
    rules = load_rules()
    sigs = [{"signal": "cache_lost", "severity": "low", "fix": "x", "ts": H, "executor_id": None, "line": "No more replicas",
             "file_path": "driver/log4j-active.log", "seq": 1}]
    f = build_findings_rows({"cluster_id": "c", "log_signals": sigs}, rules)
    assert f[0]["evidence"].startswith("1 lines on the driver.")
    gc = [{"spark_context_id": C, "executor_id": "1", "full_gcs": 40, "gc_pause_ms": 30_000, "lifetime_ms": 60 * MIN,
           "heap_after_p50": 0.5},
          {"spark_context_id": C, "executor_id": "2", "full_gcs": 40, "gc_pause_ms": 30_000, "lifetime_ms": 60 * MIN,
           "heap_after_p50": 0.85},
          {"spark_context_id": C, "executor_id": "3", "full_gcs": 40, "gc_pause_ms": 12 * MIN, "lifetime_ms": 60 * MIN}]
    sev = {r["executor_id"]: r["severity"] for r in _event_findings("c", [], [], [], rules, gc_profile=gc)}
    assert sev == {"1": "low", "2": "medium", "3": "medium"}


def test_oom_signal_fix_names_the_cache_when_every_oom_built_one():
    rules = load_rules()
    sigs = [{"signal": "executor_oom", "severity": "high", "fix": "raise shuffle partitions", "ts": H, "executor_id": "1",
             "line": "java.lang.OutOfMemoryError: Java heap space", "file_path": "executor/1/stderr", "seq": 1}]
    ooms = [{"spark_context_id": C, "stage_id": 3, "executor_id": "1", "oom_site": "cache", "ts": H}]
    fix = {r["category"]: r["fix"] for r in build_findings_rows({"cluster_id": "c", "log_signals": sigs, "task_ooms": ooms}, rules)}
    assert fix["log:executor_oom"].startswith("A cache() or persist()")
    ooms.append({"spark_context_id": C, "stage_id": 4, "executor_id": "2", "oom_site": "sort_aggregate", "ts": H})
    fix = {r["category"]: r["fix"] for r in build_findings_rows({"cluster_id": "c", "log_signals": sigs, "task_ooms": ooms}, rules)}
    assert "raise shuffle partitions" in fix["log:executor_oom"]


def _errors(lines):
    g = ExceptionGrouper(load_rules(), "c")
    out = []
    for i, (line, own) in enumerate(lines):
        out += g.feed({"file_path": "driver/log4j-active.log", "file_name": "log4j-active.log", "source": "driver",
                       "seq": i, "ts": H, "line": line, "_own_ts": own, "level": "WARN" if own else None})
    return out + g.flush()


def _group(e):
    return {"exception_class": e["exception_class"], "occurrences": 1, "sources": ["driver"], "executors_affected": 0,
            "user_frame": None, "sample_message": e["message"], "sample_stack": e["top_frames"], "first_seen": H,
            "fingerprint": e["fingerprint"], "sample_file_path": "driver/log4j-active.log", "sample_seq": 0,
            "sample_executor_id": None, "sample_logged_by": e.get("logged_by")}


def test_listener_concurrent_modification_is_benign_by_its_stack():
    rules = load_rules()
    frames = [("\tat java.util.HashMap$HashIterator.nextNode(HashMap.java:1)", False)]
    listener = _errors([("26/01/01 10:00:00 ERROR AsyncEventQueue: Listener StatusListener threw an exception", True),
                        ("java.util.ConcurrentModificationException", False), *frames])
    writer = _errors([("26/01/01 10:00:00 ERROR Writer: commit failed", True),
                      ("java.util.ConcurrentModificationException", False), *frames])
    sev = [_exception_findings("c", [_group(e)], rules)[0]["severity"] for e in (listener[0], writer[0])]
    assert sev == ["low", "medium"]


def test_platform_noise_and_vendor_frames():
    rules = load_rules()
    noise = [{"exception_class": "com.amazonaws.SdkClientException",
              "sample_message": "The requested metadata is not found at http://169.254.169.254/latest/meta-data"},
             {"exception_class": "com.databricks.api.base.DatabricksServiceException",
              "sample_message": "BAD_REQUEST: [SHOULD_USE_AUTOSCALING_INFO] "}]
    for g in noise:
        e = {"exception_class": g["exception_class"], "message": g["sample_message"], "top_frames": [], "fingerprint": "x"}
        assert _exception_findings("c", [_group(e)], rules)[0]["severity"] == "low"
    for frame in ("at com.sap.db.jdbc.Driver.connect(Driver.java:1)", "at oracle.jdbc.driver.T4C.read(T4C.java:2)",
                  "at org.postgresql.core.Stream.read(Stream.java:3)", "at org.mlflow.tracking.Client.log(Client.java:4)",
                  "at com.microsoft.sqlserver.jdbc.TDS.read(TDS.java:5)"):
        assert is_framework_frame(frame, tuple(rules.framework_frame_prefixes))


def test_executor_lost_at_cluster_termination_is_not_a_signal():
    rules = load_rules()
    assert find_signal("26/01/01 10:00:00 ERROR TaskSchedulerImpl: Lost executor 1 on 10.0.0.1: cluster termination", rules) is None
    assert find_signal("26/01/01 10:00:00 ERROR TaskSchedulerImpl: Lost executor 1 on 10.0.0.1: heartbeat timed out",
                       rules)[0] == "executor_lost"


def test_run_is_not_named_after_the_connect_session():
    d = {"spark_jobs": [{"run_key": "r", "description": 'Spark Connect - session_id: "abc" user_id: "u" operation_id: "o"'},
                        {"run_key": "r", "description": "load daily_orders_raw"}],
         "sql_queries": []}
    assert run_names(d)["r"] == (None, "daily_orders_raw")


# ---- #11: refresh recomputes from the datasets ------------------------------------------------------------------
def test_refresh_recomputes_status_stages_and_revision(output_root, tmp_path):
    d = tmp_path / mf.MAIN
    shutil.copytree(output_root / mf.MAIN, d)
    s0 = json.loads((d / "summary.json").read_text("utf-8"))
    assert s0["analyzer_revision"] == ANALYZER_REVISION
    # an output from an older build: no revision, no storage columns, a wrong status
    s0.pop("analyzer_revision")
    s0["status"] = "unknown"
    (d / "summary.json").write_text(json.dumps(s0), "utf-8")
    st = pq.read_table(d / "stages.parquet").drop(["storage_bytes", "df_cache_bytes", "cloud_bytes", "disk_cache_bytes"])
    pq.write_table(st, d / "stages.parquet")
    refresh_findings(d, load_rules())
    s1 = json.loads((d / "summary.json").read_text("utf-8"))
    assert s1["analyzer_revision"] == ANALYZER_REVISION and s1["status"] == mf.EXPECTED[mf.MAIN]["status"]
    st = pq.read_table(d / "stages.parquet").to_pandas()
    has = st["input_bytes"].notna()
    # no cloud storage metric in an older output: what was read counts as storage
    assert (st.loc[has, "storage_bytes"] == st.loc[has, "input_bytes"]).all()


def test_refresh_unflags_tasks_adaptive_execution_cancelled(output_root, tmp_path):
    d = tmp_path / mf.MAIN
    shutil.copytree(output_root / mf.MAIN, d)
    t = pq.read_table(d / "tasks.parquet").to_pandas()
    i = t.index[~t["failed"]][0]
    t.loc[i, ["failed", "end_reason", "error"]] = [True, "TaskKilled",
                                                   "Adaptive query execution has replanned the query and cancelled unused stages"]
    write_parquet(t, d / "tasks.parquet", "tasks")
    refresh_findings(d, load_rules())
    after = pq.read_table(d / "tasks.parquet").to_pandas()
    assert not after.loc[i, "failed"] and len(after) == len(t)


# ---- #1 / #10: the DataFrame cache advice ranks first and takes the memory findings ------------------------------
def test_cache_advice_ranks_first(output_root, tmp_path):
    from databricks_cluster_log_analyzer.api.queries import Store, settings_view

    d = tmp_path / mf.MAIN
    shutil.copytree(output_root / mf.MAIN, d)
    f = pq.read_table(d / "findings.parquet").to_pylist()
    n = len(f)
    f += [_f(f"F{n + 1:03d}", "dataframe_cache", H, entity="query 3: DataFrame cache", spark_context_id=C,
             sql_execution_id=3, evidence="2 queries read a DataFrame cache (cache() or persist()); the largest is 40 GiB. "
                                          "It was never released (no unpersist)."),
          _f(f"F{n + 2:03d}", "gc_stuck", H, entity="executor 2", executor_id="2", spark_context_id=C,
             evidence="stuck in GC")]
    write_parquet(f, d / "findings.parquet", "findings")
    adv = settings_view(Store(tmp_path), mf.MAIN)["advice"]
    a = adv[0]
    assert a["title"] == "DataFrame cache larger than memory" and a["severity"] == "high"
    assert "memory" in a["key"] and a["queries"][0]["sql_execution_id"] == 3
    assert "It was never released (no unpersist)" in a["facts"] and any("executor 2" in x for x in a["facts"])
