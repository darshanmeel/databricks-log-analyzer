"""Incidents: findings resolved to stage / executor / query and chained cause -> effect."""


def _by_finding(ds, cid):
    inc = ds(cid, "incidents")
    return inc, {r["finding_id"]: r for r in inc.to_dict("records")}


def test_every_finding_is_in_one_incident(ds, main_id):
    inc, _ = _by_finding(ds, main_id)
    f = ds(main_id, "findings")
    assert sorted(inc["finding_id"]) == sorted(f["finding_id"])
    assert inc["incident_rank"].min() == 1


def test_query_failure_chain_roots_in_user_code(ds, main_id):
    inc, rows = _by_finding(ds, main_id)
    f = ds(main_id, "findings").set_index("finding_id")
    qf = f[f["category"] == "query_failed"].index[0]
    q = rows[qf]
    assert q["incident_title"].startswith("Query 3 failed") and q["incident_rank"] == 1
    members = inc[inc["incident_id"] == q["incident_id"]]
    root = members[members["role"] == "root"].iloc[0]
    assert root["kind"] == "error in task code" and root["stages"] == "6.1" and root["executors"] == "3"
    # the query failed because stage 6.1 failed, which failed because of the code error
    stage = rows[q["caused_by"]]
    assert stage["kind"] == "stage failed" and stage["caused_by"] == root["finding_id"]


def test_oom_explains_the_first_attempt_and_signals_get_links(ds, main_id):
    inc, rows = _by_finding(ds, main_id)
    f = ds(main_id, "findings").set_index("finding_id")
    oom_sig = f[f["category"] == "log:executor_oom"].index[0]
    r = rows[oom_sig]
    # a log signal has no links of its own: they come from its lines
    assert r["executor_id"] == "1" and r["stage_id"] == 6 and r["stage_attempt"] == 0 and r["sql_execution_id"] == 3
    fetch = f[f["category"] == "log:fetch_failure"].index[0]
    assert rows[fetch]["incident_id"] == r["incident_id"]
    chain = rows[fetch]
    assert chain["kind"] == "shuffle fetch failure" and rows[chain["caused_by"]]["kind"] == "out of memory"
    assert "shuffle files" in chain["because"]


def test_unrelated_performance_findings_group_by_query(ds, main_id):
    inc, rows = _by_finding(ds, main_id)
    f = ds(main_id, "findings").set_index("finding_id")
    skew = rows[f[f["category"] == "task_skew"].index[0]]
    spill = rows[f[f["category"] == "disk_spill"].index[0]]
    assert skew["incident_id"] == spill["incident_id"]
    assert skew["incident_title"].startswith("Query 0 ran slow")


def test_summary_root_cause_follows_the_top_incident(ds, main_id):
    s = ds.summary(main_id)
    rc = next(d for d in s["diagnosis"] if d["kind"] == "root_cause")
    # the query failed because of the code error, not because of the earlier spill / GC / OOM on the first attempt
    assert rc["text"].startswith("Query 3 failed")
    assert "In time order: error in task code (" in rc["text"]
    assert ("Also: Stage 6.0 failed (query 3), starting from out of memory (an earlier failure in the same query"
            in rc["text"])
    assert "error reported to the notebook / job (18:07:54)" in rc["text"]
    assert [lk["type"] for lk in rc["links"]] == ["finding"] * 4


# ---- hand-made cases from the code review --------------------------------------------------------------------
import time  # noqa: E402

from databricks_cluster_log_analyzer.analysis.diagnosis import _root_cause  # noqa: E402
from databricks_cluster_log_analyzer.analysis.incidents import FETCH_HOST_RE, build_incidents  # noqa: E402

H = 3_600_000
C = "ctx"


def _f(fid, category, ts, severity="high", **kw):
    base = {"finding_id": fid, "category": category, "severity": severity, "ts": ts, "spark_context_id": None,
            "stage_id": None, "stage_attempt": None, "executor_id": None, "sql_execution_id": None,
            "spark_job_id": None, "entity": None, "evidence": "", "fix": None, "signal": None, "fingerprint": None}
    return {**base, **kw}


def _stage(sid, att=0, q=None, status="succeeded", failed=0, start=0, end=None):
    return {"spark_context_id": C, "stage_id": sid, "stage_attempt": att, "sql_execution_id": q, "status": status,
            "failed_tasks": failed, "start_time": start, "end_time": end, "stage_name": f"stage {sid}",
            "spark_job_id": sid, "duration_ms": 1000}


def _exec(eid, host=None):
    return {"spark_context_id": C, "executor_id": eid, "host": host or f"10.0.0.{eid}", "added_time": 0,
            "removed_time": None}


def _build(findings, stages=(), executors=(), signals=(), errors=(), queries=()):
    return build_incidents("c", list(findings), list(stages), list(executors), [], list(queries), list(signals),
                           list(errors), None, [])


def _by(rows):
    return {r["finding_id"]: r for r in rows}


def test_two_different_errors_on_one_executor_stay_two_problems():
    errs = [{"fingerprint": "a", "message": "invalid literal", "executor_id": "0", "ts": 1 * H, "source": "executor",
             "user_frame": 'File "x.py", line 1'},
            {"fingerprint": "b", "message": "KeyError: k", "executor_id": "0", "ts": 6 * H, "source": "executor",
             "user_frame": 'File "y.py", line 9'}]
    rows = _by(_build([_f("F1", "exception", 1 * H, entity="ValueError", fingerprint="a", executor_id="0"),
                       _f("F2", "exception", 6 * H, entity="KeyError", fingerprint="b", executor_id="0")],
                      executors=[_exec("0")], errors=errs))
    assert rows["F1"]["problem_id"] != rows["F2"]["problem_id"]
    assert rows["F2"]["role"] != "same"


def test_cluster_wide_log_signal_does_not_bridge_two_failures():
    stages = [_stage(4, 0, q=1, status="failed", start=H - 1000, end=H + 1000),
              _stage(9, 0, q=7, status="failed", start=8 * H - 1000, end=8 * H + 1000)]
    sigs = [{"signal": "executor_lost", "line": "Lost executor 2 on 10.0.0.2", "ts": H, "executor_id": None},
            {"signal": "executor_lost", "line": "Lost executor 5 on 10.0.0.5", "ts": 8 * H, "executor_id": None}]
    rows = _by(_build([_f("F1", "executor_lost", H, executor_id="2", spark_context_id=C),
                       _f("F2", "stage_failed", H + 500, stage_id=4, stage_attempt=0, spark_context_id=C,
                          evidence="Stage 4.0 failed: lost executor 2"),
                       _f("F3", "executor_lost", 8 * H, executor_id="5", spark_context_id=C),
                       _f("F4", "stage_failed", 8 * H + 500, stage_id=9, stage_attempt=0, spark_context_id=C,
                          evidence="Stage 9.0 failed: lost executor 5"),
                       _f("F5", "log:executor_lost", H, signal="executor_lost")],
                      stages=stages, executors=[_exec("2"), _exec("5")], signals=sigs))
    assert rows["F2"]["incident_id"] != rows["F4"]["incident_id"]
    assert rows["F4"]["caused_by"] == "F3"
    assert rows["F5"]["incident_id"] in (rows["F1"]["incident_id"], rows["F3"]["incident_id"])


def test_title_names_the_query_that_failed():
    stages = [_stage(3, 0, q=2, status="failed", failed=4, start=H - 5000, end=H + 1000),
              _stage(10, 0, q=9, start=H - 5000, end=H + 5000)]
    queries = [{"spark_context_id": C, "sql_execution_id": 2, "description": "guilty"},
               {"spark_context_id": C, "sql_execution_id": 9, "description": "innocent"}]
    rows = _build([_f("F1", "executor_lost", H, executor_id="2", spark_context_id=C,
                      evidence="Lost executor 2; it ran stage 3.0 and stage 10.0"),
                   _f("F2", "stage_failed", H + 500, stage_id=3, stage_attempt=0, spark_context_id=C, sql_execution_id=2),
                   _f("F3", "query_failed", H + 900, spark_context_id=C, sql_execution_id=2)],
                  stages=stages, executors=[_exec("2")], queries=queries)
    assert rows[0]["incident_title"] == "Query 2 failed: guilty"


def test_root_cause_text_follows_one_branch_of_the_chain():
    stages = [_stage(4, 0, status="failed", failed=2), _stage(5, 0, status="failed", failed=2)]
    findings = [_f("F1", "executor_oom", H, executor_id="1", spark_context_id=C,
                   evidence="Executor 1 out of memory while running stage 4.0 and stage 5.0"),
                _f("F2", "stage_failed", H + 100, stage_id=4, stage_attempt=0, spark_context_id=C),
                _f("F3", "stage_failed", H + 200, stage_id=5, stage_attempt=0, spark_context_id=C)]
    inc = _build(findings, stages=stages, executors=[_exec("1")])
    rc = _root_cause({"incidents": inc, "findings": findings}, None)
    assert "out of memory (" in rc["text"]
    assert rc["text"].count("stage failed") == 1  # two separate effects, not one stage failing the other
    assert len(rc["links"]) == 2


def test_bare_notebook_error_ranks_below_the_real_failure():
    errs = [{"fingerprint": "p", "message": "An error occurred while calling o1.collect.", "executor_id": None,
             "ts": H + 9000, "source": "driver", "user_frame": None}]
    rows = _by(_build([_f("F1", "executor_oom", H, executor_id="1", spark_context_id=C),
                       _f("F2", "exception", H + 9000, entity="py4j.protocol.Py4JJavaError", fingerprint="p")],
                      executors=[_exec("1")], errors=errs))
    assert rows["F1"]["incident_rank"] == 1 and rows["F2"]["incident_rank"] == 2


def test_unmapped_signal_keeps_its_own_name():
    rows = _by(_build([_f("F1", "log:storage_throttling", H, signal="storage_throttling")],
                      signals=[{"signal": "storage_throttling", "line": "503 SlowDown", "ts": H, "executor_id": None}]))
    assert rows["F1"]["kind"] == "storage throttling"


def test_fetch_host_regex_reads_java_host_slash_ip():
    for text, host in [("Failed to connect to ip-10-0-0-1.ec2.internal/10.0.0.1:4048", "10.0.0.1"),
                       ("Failed to connect to /10.0.0.7:4048", "10.0.0.7"),
                       ("Failed to connect to 10.0.0.9/10.0.0.9:4048", "10.0.0.9")]:
        assert FETCH_HOST_RE.search(text).group(2) == host


def test_many_findings_build_quickly():
    n = 3000
    stages = [_stage(i, 0, q=i // 10, start=i * 1000, end=i * 1000 + 500) for i in range(n)]
    findings = [_f(f"F{i}", "task_skew" if i % 2 else "disk_spill", i * 1000, severity="medium", stage_id=i,
                   stage_attempt=0, spark_context_id=C, sql_execution_id=i // 10) for i in range(n)]
    t0 = time.perf_counter()
    rows = _build(findings, stages=stages, executors=[_exec(str(e)) for e in range(8)])
    assert len(rows) == n
    assert time.perf_counter() - t0 < 5
