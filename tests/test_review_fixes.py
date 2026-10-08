"""Fixes from the correctness and MERGE reviews (made-up numbers)."""

import pandas as pd

from databricks_cluster_log_analyzer.analysis.combined import spill_shuffle_timeline
from databricks_cluster_log_analyzer.analysis.incidents import build_incidents
from databricks_cluster_log_analyzer.config import load_rules

M = 60_000
T0 = 1_790_000_000_000 // M * M
C = "ctx"


def test_task_numbers_are_spread_over_the_minutes_the_task_ran():
    # one task of 4 minutes from 0:30 to 4:30: its 240 s of run time go 30 / 60 / 60 / 60 / 30 into five minutes,
    # not all into the minute it ended
    t = pd.DataFrame([{"spark_context_id": C, "executor_id": "1", "stage_id": 1, "stage_attempt": 0,
                       "launch_time": T0 + M // 2, "finish_time": T0 + 4 * M + M // 2, "run_ms": 240_000,
                       "gc_ms": 0, "mem_spill": 0, "disk_spill": 4_800, "shuffle_read": 0, "shuffle_write": 0,
                       "input_bytes": 0, "output_bytes": 0}])
    df = spill_shuffle_timeline(t, "c", load_rules()).sort_values("minute")
    assert df["minute"].tolist() == [T0 + i * M for i in range(5)]
    assert df["run_ms"].tolist() == [30_000, 60_000, 60_000, 60_000, 30_000]
    assert df["disk_spill"].sum() == 4_800
    assert df["tasks"].tolist() == [0, 0, 0, 0, 1]  # counted once, where it finished


def _f(fid, category, ts, **kw):
    base = {"finding_id": fid, "category": category, "severity": "high", "ts": ts, "spark_context_id": C,
            "stage_id": None, "stage_attempt": None, "executor_id": None, "sql_execution_id": None,
            "spark_job_id": None, "entity": None, "evidence": "", "fix": None, "signal": None, "fingerprint": None,
            "run_key": "r1"}
    return {**base, **kw}


def test_a_cache_bigger_than_memory_is_the_root_of_the_gc_and_the_oom():
    execs = [{"spark_context_id": C, "executor_id": "4", "host": "10.0.0.4", "added_time": T0, "removed_time": None}]
    rows = {r["finding_id"]: r for r in build_incidents("c", [
        _f("F1", "dataframe_cache", T0 + M, sql_execution_id=104, evidence="the largest is 144 GB"),
        _f("F2", "gc_stuck", T0 + 10 * M, executor_id="4"),
        _f("F3", "executor_oom", T0 + 15 * M, executor_id="4"),
        _f("F4", "capacity_bound", T0, evidence="Tasks queued on a full cluster")], [], execs, [], [], [], [], None, [])}
    assert "F4" not in rows  # run-level findings stay outside incidents
    assert rows["F1"]["role"] == "root"
    assert rows["F2"]["incident_id"] == rows["F1"]["incident_id"] == rows["F3"]["incident_id"]
    assert rows["F2"]["caused_by"] == "F1"


def test_autoscaling_that_removed_executors_is_the_cause_of_their_errors():
    execs = [{"spark_context_id": C, "executor_id": e, "host": f"10.0.0.{e}", "added_time": T0, "removed_time": None}
             for e in ("6", "7")]
    rows = {r["finding_id"]: r for r in build_incidents("c", [
        _f("F1", "autoscale_removed", T0 + M, evidence="At 18:31:11 autoscaling removed 2 executors (6, 7) while"),
        _f("F2", "exception", T0 + M + 1_000, entity="java.lang.IllegalStateException", executor_id="7")],
        [], execs, [], [], [], [], None, [])}
    assert rows["F2"]["caused_by"] == "F1"


def test_missing_rolled_event_log_files_are_a_gap():
    from pathlib import Path

    from databricks_cluster_log_analyzer.analysis.coverage import event_log_gaps
    names = ["eventlog-2026-10-06--17-50.gz", "eventlog-2026-10-06--18-00.gz", "eventlog-2026-10-06--18-30.gz",
             "eventlog-2026-10-06--18-40.gz", "eventlog"]
    gaps = event_log_gaps(Path(n) for n in names)
    assert len(gaps) == 1 and gaps[0][1] - gaps[0][0] == 30 * M
