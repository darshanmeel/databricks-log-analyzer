"""End-to-end: pipeline.build over the synthetic fixtures, assertions on every dataset + summary.json."""

from __future__ import annotations

import hashlib
import math
import re
from datetime import datetime, timedelta

import pandas as pd
import pyarrow.parquet as pq
import pytest

import make_fixtures as mf
from conftest import ms, to_ms

A, B = mf.CTX_A, mf.CTX_B
GiB = 1 << 30

DATASETS = ["files", "apps", "log_lines", "log_signals", "log_errors", "tasks", "stages", "spark_jobs",
            "sql_queries", "executors", "findings", "timeline", "query_profile", "stage_executor_profile",
            "executor_profile", "run_story"]
EVENT_DATASETS = ["apps", "tasks", "stages", "spark_jobs", "sql_queries", "executors", "query_profile",
                  "stage_executor_profile", "executor_profile"]


def ta(sec: float) -> int:
    return ms(mf.T0A + timedelta(seconds=sec))


def tb(sec: float) -> int:
    return ms(mf.T0B + timedelta(seconds=sec))


def one(df: pd.DataFrame) -> pd.Series:
    assert len(df) == 1, f"expected exactly one row, got {len(df)}:\n{df}"
    return df.iloc[0]


def stage(ds, ctx, sid, att=0):
    s = ds(mf.MAIN, "stages")
    return one(s[(s.spark_context_id == ctx) & (s.stage_id == sid) & (s.stage_attempt == att)])


def lower_median(values):
    v = sorted(values)
    return v[math.ceil(0.5 * len(v)) - 1]


def plan_hash(plan: str) -> str:
    return hashlib.sha256(re.sub(r"#\d+|\[plan_id=\d+\]|\(\d+\)|\d+", "", plan).encode("utf-8")).hexdigest()[:12]


def is_null(v) -> bool:
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


# ============================================================================================ files / schema
@pytest.mark.parametrize("name", DATASETS)
def test_every_dataset_written_for_every_cluster(ds, name):
    for cid in (mf.MAIN, mf.HEALTHY, mf.EMPTY):
        p = ds.path(cid, name)
        assert p.exists(), p
        df = ds(cid, name)
        assert "cluster_id" in df.columns
        assert (df.cluster_id == cid).all()
        if name in EVENT_DATASETS:
            assert "spark_context_id" in df.columns


def test_timestamp_columns_are_timestamp_ms(ds):
    import pyarrow as pa
    checks = {"stages": ["start_time", "end_time"], "tasks": ["launch_time", "finish_time"],
              "log_lines": ["ts"], "findings": ["ts"], "executors": ["added_time", "removed_time"],
              "run_story": ["ts"], "timeline": ["minute"]}
    for name, cols in checks.items():
        schema = pq.read_schema(ds.path(mf.MAIN, name))
        for c in cols:
            t = schema.field(c).type
            assert pa.types.is_timestamp(t) and t.unit == "ms" and t.tz is None, (name, c, t)
    for name, cols in {"stages": ["duration_ms", "disk_spill"], "tasks": ["task_ms", "run_ms"]}.items():
        schema = pq.read_schema(ds.path(mf.MAIN, name))
        for c in cols:
            assert schema.field(c).type == pa.int64(), (name, c)


def test_empty_cluster_datasets_have_schema_and_no_rows(ds):
    main_cols = {n: list(ds(mf.MAIN, n).columns) for n in DATASETS}
    for n in DATASETS:
        df = ds(mf.EMPTY, n)
        assert len(df) == 0, n
        assert list(df.columns) == main_cols[n], n


def test_files(ds, expected):
    f = ds(mf.MAIN, "files")
    assert len(f) == expected[mf.MAIN]["files"]
    assert set(f.folder) <= {"driver", "executor", "eventlog", "other"}
    assert not f.path.str.contains("\\\\", regex=False).any()
    assert "driver/log4j-active.log" in set(f.path)
    assert (f["size"] > 0).all()


# ============================================================================================ apps
def test_apps_two_contexts(ds):
    apps = ds(mf.MAIN, "apps")
    assert len(apps) == 2
    a = one(apps[apps.spark_context_id == A])
    b = one(apps[apps.spark_context_id == B])
    assert a.app_id == mf.APP_A and b.app_id == mf.APP_B
    assert a.spark_version == b.spark_version == "3.5.0"
    assert a.app_name == "Databricks Shell"
    assert a.user == "root"
    assert to_ms(a.start_time) == ta(0)
    assert to_ms(b.start_time) == tb(0) and to_ms(b.end_time) == tb(60)
    assert b.duration_ms == 60_000
    assert a.eventlog_files == 2 and b.eventlog_files == 1
    assert a.malformed_lines == 1 and b.malformed_lines == 0
    assert a.events_read > 2000


# ============================================================================================ log lines
def test_log_lines_counts_and_seq(ds, expected):
    ll = ds(mf.MAIN, "log_lines")
    assert len(ll) == expected[mf.MAIN]["log_lines"]
    assert ll.seq.is_unique
    for fp, g in ll.groupby("file_path"):
        g = g.sort_values("line_no")
        assert list(g.line_no) == list(range(1, len(g) + 1)), fp
        assert g.seq.is_monotonic_increasing, fp
    assert set(ll.source) == {"driver", "executor"}
    drv = ll[ll.source == "driver"]
    assert drv.executor_id.isna().all()
    ex = ll[ll.source == "executor"]
    assert ex.executor_id.notna().all() and ex.app_id.notna().all()
    assert set(ex.app_id) == {mf.APP_A, mf.APP_B}


def test_log_lines_ts_inheritance(ds):
    ll = ds(mf.MAIN, "log_lines")
    stdout = ll[ll.file_path == "driver/stdout"]
    assert len(stdout) == 4 and stdout.ts.isna().all() and stdout.level.isna().all()
    # the "\tcontinuation..." line of the rolled log4j file inherits the previous line's ts
    rolled = ll[ll.file_name == "log4j-2026-10-06-17.log.gz"].sort_values("line_no")
    cont = one(rolled[rolled.line.str.startswith("\tcontinuation")])
    prev = rolled[rolled.line_no == cont.line_no - 1].iloc[0]
    assert to_ms(cont.ts) == to_ms(prev.ts) == ta(300)
    assert cont.continuation and cont.level == prev.level
    # executor 1 OOM header line inherits from the ERROR line above it
    e1 = ll[(ll.app_id == mf.APP_A) & (ll.executor_id == "1") & (ll.file_name == "stderr")].sort_values("line_no")
    hdr = e1[e1.line == "java.lang.OutOfMemoryError: Java heap space"].iloc[0]
    assert to_ms(hdr.ts) == ta(1150)
    # executor 0 stdout is a legacy (JDK 8) GC log with no log4j timestamps. Revision 3 adds ISO-8601 line-start
    # timestamps; "2026-10-06T17:48:20.001+0000:" may or may not be recognised (contract leaves the offset form open)
    gc = ll[(ll.app_id == mf.APP_A) & (ll.executor_id == "0") & (ll.file_name == "stdout")].sort_values("line_no")
    assert len(gc) == 3 and gc.level.isna().all()
    got = [to_ms(x) for x in gc.ts]
    iso = [ms(datetime(2026, 10, 6, 17, 48, 20, 1000)), ms(datetime(2026, 10, 6, 18, 4, 10, 500000)),
           ms(datetime(2026, 10, 6, 18, 4, 20, 500000))]
    assert got == [None, None, None] or got == iso, got


def test_log_lines_rolled_before_active(ds):
    ll = ds(mf.MAIN, "log_lines")
    rolled = ll[ll.file_name == "log4j-2026-10-06-17.log.gz"].seq
    active = ll[ll.file_name == "log4j-active.log"].seq
    assert rolled.max() < active.min()
    drv = ll[ll.source == "driver"].seq
    exe = ll[ll.source == "executor"].seq
    assert drv.max() < exe.min()  # driver folder processed before executor folders


# ============================================================================================ signals / errors
def test_log_signals(ds):
    sig = ds(mf.MAIN, "log_signals")
    names = set(sig.signal)
    assert {"executor_oom", "executor_lost", "fetch_failure", "disk_spill", "gc_pressure", "python_error"} <= names
    assert not names & {"disk_full", "broadcast_timeout", "schema_error", "driver_unresponsive"}
    # first-match-wins: "Lost executor 1 ... Container killed ..." is executor_oom, not executor_lost
    row = one(sig[sig.line.str.contains("Lost executor 1 on", regex=False)])
    assert row.signal == "executor_oom" and row.severity == "high" and row.fix
    row2 = one(sig[sig.line.str.contains("Lost executor 2 on", regex=False)])
    assert row2.signal == "executor_lost" and row2.severity == "medium"
    assert (sig.line.str.len() <= 500).all()
    ll = ds(mf.MAIN, "log_lines")
    assert len(sig) == ll.signal.notna().sum()
    # signals in executor files keep their executor id
    spill = sig[sig.signal == "disk_spill"]
    assert len(spill) == 5 and set(spill.executor_id) == {"0"}


def test_log_errors(ds):
    err = ds(mf.MAIN, "log_errors")
    oom = err[err.exception_class == "java.lang.OutOfMemoryError"]
    assert len(oom) == 2
    assert set(oom.executor_id) == {"1"} and set(oom.app_id) == {mf.APP_A}
    assert oom.fingerprint.nunique() == 1  # different line numbers, same fingerprint
    assert all(len(list(tf)) == 5 for tf in oom.top_frames)
    assert oom.user_frame.isna().all()
    assert oom.message.iloc[0] == "Java heap space"

    py = err[err.exception_class.str.endswith("PythonException")]
    assert len(py) >= 2  # executor 3 stderr + driver stderr
    assert set(py.user_frame.dropna()) == {mf.USER_FRAME}
    assert {"driver", "executor"} <= set(py.source)
    drv_py = one(py[py.file_path == "driver/stderr"])
    assert drv_py.message == ""

    fetch = err[err.exception_class == "org.apache.spark.shuffle.FetchFailedException"]
    assert len(fetch) >= 1
    assert set(err.exception_class) >= {"java.io.IOException", "org.apache.spark.SparkException",
                                        "py4j.protocol.Py4JJavaError", "ValueError"}
    assert err.fingerprint.str.len().eq(12).all()


# ============================================================================================ tasks
def test_tasks(ds, expected):
    t = ds(mf.MAIN, "tasks")
    for ctx in (A, B):
        g = t[t.spark_context_id == ctx]
        assert len(g) == expected[mf.MAIN]["tasks"][ctx]
        assert int(g.failed.sum()) == expected[mf.MAIN]["failed_tasks"][ctx]
    s0 = t[(t.spark_context_id == A) & (t.stage_id == 0)]
    longest = s0.sort_values("task_ms").iloc[-1]
    assert longest.task_ms == 900_000 and longest.executor_id == "0" and longest.host == mf.HOSTS["0"]
    assert to_ms(longest.launch_time) == ta(21) and to_ms(longest.finish_time) == ta(921)
    reasons = set(t.end_reason)
    assert {"Success", "ExceptionFailure", "ExecutorLostFailure", "FetchFailed", "TaskKilled"} <= reasons
    ff = t[t.end_reason == "FetchFailed"]
    assert len(ff) == 2 and ff.error.str.contains("FetchFailedException").all()
    lost = one(t[t.end_reason == "ExecutorLostFailure"])
    assert "decommission" in lost.error
    killed = t[t.end_reason == "TaskKilled"]
    assert killed.error.str.contains("Stage cancelled").all()
    oomt = one(t[(t.end_reason == "ExceptionFailure") & (t.stage_id == 6) & (t.stage_attempt == 0)])
    assert oomt.error.startswith("Java heap space") and oomt.executor_id == "1"
    retry = one(t[(t.spark_context_id == A) & (t.stage_id == 4) & (t.task_attempt == 1) & (t.executor_id == "0")])
    assert not retry.failed
    assert (t.error.dropna().str.len() <= 1000).all()
    s1 = t[(t.spark_context_id == A) & (t.stage_id == 1)]
    assert (s1.disk_spill == 768 * (1 << 20)).all()
    assert (s1.shuffle_read == 64 * (1 << 20)).all()  # remote + local


# ============================================================================================ stages
def test_stage_counts_and_ids_restart(ds, expected):
    s = ds(mf.MAIN, "stages")
    for ctx in (A, B):
        assert len(s[s.spark_context_id == ctx]) == expected[mf.MAIN]["stages"][ctx]
    assert not s.duplicated(["spark_context_id", "stage_id", "stage_attempt"]).any()
    b0 = stage(ds, B, 0)
    a0 = stage(ds, A, 0)
    assert b0.tasks == 4 and a0.tasks == 20  # same stage id, keyed separately per context
    assert to_ms(b0.start_time) == tb(11)


def test_stage_skew(ds):
    s0 = stage(ds, A, 0)
    assert s0.p50_task_ms == 5000 and s0.max_task_ms == 900_000
    assert s0["skew"] == pytest.approx(180.0)
    # Revision 11: every task read the same 128 MiB, so the time skew is not data skew
    assert s0.data_skew == pytest.approx(1.0) and s0.p50_task_bytes_in == s0.max_task_bytes_in == 128 * 2**20
    assert s0.input_records == 20_000_000 and s0.shuffle_write_records == 20_000
    assert s0.status == "succeeded" and is_null(s0.failure_reason)
    assert to_ms(s0.start_time) == ta(20) and to_ms(s0.end_time) == ta(922)
    assert s0.duration_ms == 902_000
    assert s0.spark_job_id == 0 and s0.sql_execution_id == 0
    assert s0.job_description == "Write orders"
    assert s0.stage_name == "mapPartitions at transform.py:42"
    assert s0.num_tasks == 20 and s0.executors_used == 4


def test_stage_spill_and_two_jobs(ds):
    s1 = stage(ds, A, 1)  # listed by job 0 and job 1 -> one row, lowest job id
    assert s1.disk_spill == 3 * GiB and s1.mem_spill == 8 * GiB
    assert s1.spark_job_id == 0 and s1.sql_execution_id == 0
    assert s1.max_peak_mem == 3 * GiB


def test_stage_tiny_tasks_and_gc(ds):
    t = ds(mf.MAIN, "tasks")
    s2 = stage(ds, A, 2)
    assert s2.tasks == 2000
    assert s2.p50_task_ms == lower_median(t[(t.spark_context_id == A) & (t.stage_id == 2)].task_ms)
    assert s2.p50_task_ms < 200
    assert s2.min_task_ms == t[(t.spark_context_id == A) & (t.stage_id == 2)].task_ms.min() <= s2.p50_task_ms
    assert s2.spark_job_id == 1 and s2.sql_execution_id == 1
    s3 = stage(ds, A, 3)
    assert s3.gc_share == pytest.approx(0.3)
    assert s3.spark_job_id == 2 and s3.sql_execution_id == 2


def test_stage_retries_and_failures(ds):
    s4 = stage(ds, A, 4)
    assert s4.status == "succeeded" and s4.failed_tasks == 2 and s4.tasks == 10
    assert s4.spark_job_id == 3 and is_null(s4.sql_execution_id)
    s60 = stage(ds, A, 6, 0)
    assert s60.status == "failed" and s60.failure_reason.startswith("org.apache.spark.shuffle.FetchFailedException")
    assert s60.failed_tasks == 3
    s61 = stage(ds, A, 6, 1)
    assert s61.status == "failed" and "PythonException" in s61.failure_reason
    assert s61.tasks == 7 and s61.failed_tasks == 7
    s51 = stage(ds, A, 5, 1)
    assert s51.status == "succeeded" and s51.tasks == 1
    for st in (s60, s61, s51, stage(ds, A, 5, 0)):
        assert st.spark_job_id == 4 and st.sql_execution_id == 3


def test_stage_gc_share_formula(ds):
    s = ds(mf.MAIN, "stages")
    t = ds(mf.MAIN, "tasks")
    for _, r in s.iterrows():
        g = t[(t.spark_context_id == r.spark_context_id) & (t.stage_id == r.stage_id)
              & (t.stage_attempt == r.stage_attempt)]
        if g.run_ms.sum() > 0:
            assert r.gc_share == pytest.approx(round(g.gc_ms.sum() / g.run_ms.sum(), 3)), (r.stage_id, r.stage_attempt)
        assert r.tasks == len(g)
        # skew over the successful attempts, as Spark's stage summary (all of them when none succeeded)
        ok = g[~g.failed.astype(bool)] if (~g.failed.astype(bool)).any() else g
        assert r["skew"] == pytest.approx(round(ok.task_ms.max() / max(lower_median(ok.task_ms), 1), 1))


# ============================================================================================ jobs / queries
def test_spark_jobs(ds, expected):
    j = ds(mf.MAIN, "spark_jobs")
    for ctx in (A, B):
        assert len(j[j.spark_context_id == ctx]) == expected[mf.MAIN]["spark_jobs"][ctx]
    j1 = one(j[(j.spark_context_id == A) & (j.spark_job_id == 1)])
    assert list(j1.stage_ids) == [1, 2] and j1.num_stages == 2
    j4 = one(j[(j.spark_context_id == A) & (j.spark_job_id == 4)])
    assert j4.result == "JobFailed" and j4.error.startswith("Job aborted due to stage failure")
    assert len(j4.error) <= 500
    assert j4.sql_execution_id == 3
    assert j4.databricks_job_id == "918273645" and j4.databricks_run_id == "5550123"
    assert j4.notebook_path == "/Workspace/Users/me/etl/main"
    assert j4.description == "Write daily totals"
    assert to_ms(j4.start_time) == ta(1100) and to_ms(j4.end_time) == ta(1181) and j4.duration_ms == 81_000
    j3 = one(j[(j.spark_context_id == A) & (j.spark_job_id == 3)])
    assert j3.result == "JobSucceeded" and is_null(j3.sql_execution_id)
    assert (j[j.spark_context_id == B].result == "JobSucceeded").all()


def test_sql_queries(ds, expected):
    q = ds(mf.MAIN, "sql_queries")
    for ctx in (A, B):
        assert len(q[q.spark_context_id == ctx]) == expected[mf.MAIN]["sql_queries"][ctx]
    q3 = one(q[(q.spark_context_id == A) & (q.sql_execution_id == 3)])
    assert q3.status == "failed"
    assert q3.error.startswith("Job aborted due to stage failure") and len(q3.error) <= 1000
    assert q3.final_plan.strip() == mf.PLAN_Q3_FINAL.strip()  # LAST AQE update, not the first
    assert "AQEShuffleRead" in q3.final_plan
    assert q3.initial_plan.strip() == mf.PLAN_Q3_INITIAL.strip()
    assert q3.plan_hash == plan_hash(q3.final_plan)
    assert "/Workspace/Users/me/etl/main.py:57" in q3.details
    assert q3.description == "INSERT OVERWRITE sales.daily_totals"
    assert to_ms(q3.start_time) == ta(1095) and to_ms(q3.end_time) == ta(1182)
    assert q3.stages == 4  # 5.0, 6.0, 5.1, 6.1
    t = ds(mf.MAIN, "tasks")
    assert q3.tasks == len(t[(t.spark_context_id == A) & (t.stage_id.isin([5, 6]))])
    q0 = one(q[(q.spark_context_id == A) & (q.sql_execution_id == 0)])
    assert q0.status == "succeeded" and (is_null(q0.error) or q0.error == "")
    assert q0.final_plan.strip() == q0.initial_plan.strip() == mf.PLAN_Q0_A.strip()  # no AQE update -> start plan
    assert q0.disk_spill == 3 * GiB and q0.max_stage_skew == pytest.approx(180.0)


def test_plan_hash_stable_across_contexts(ds):
    q = ds(mf.MAIN, "sql_queries")
    a0 = one(q[(q.spark_context_id == A) & (q.sql_execution_id == 0)])
    b0 = one(q[(q.spark_context_id == B) & (q.sql_execution_id == 0)])
    assert a0.final_plan != b0.final_plan  # different expression ids / plan ids
    assert a0.plan_hash == b0.plan_hash == plan_hash(mf.PLAN_Q0_A)
    a3 = one(q[(q.spark_context_id == A) & (q.sql_execution_id == 3)])
    assert a3.plan_hash != a0.plan_hash


def test_stage_job_query_linkage(ds):
    s = ds(mf.MAIN, "stages")
    j = ds(mf.MAIN, "spark_jobs")
    q = ds(mf.MAIN, "sql_queries")
    for _, r in s[s.spark_job_id.notna()].iterrows():
        job = one(j[(j.spark_context_id == r.spark_context_id) & (j.spark_job_id == r.spark_job_id)])
        assert r.stage_id in list(job.stage_ids)
        # lowest job listing the stage
        listing = j[(j.spark_context_id == r.spark_context_id)
                    & j.stage_ids.apply(lambda ids, sid=r.stage_id: sid in list(ids))]
        assert r.spark_job_id == listing.spark_job_id.min()
        if not is_null(job.sql_execution_id):
            assert r.sql_execution_id == job.sql_execution_id
            one(q[(q.spark_context_id == r.spark_context_id) & (q.sql_execution_id == r.sql_execution_id)])


# ============================================================================================ executors
def test_executors(ds, expected):
    e = ds(mf.MAIN, "executors")
    for ctx in (A, B):
        assert len(e[e.spark_context_id == ctx]) == expected[mf.MAIN]["executors"][ctx]
    e1 = one(e[(e.spark_context_id == A) & (e.executor_id == "1")])
    assert e1.removed_reason == mf.OOM_REASON and to_ms(e1.removed_time) == ta(1151)
    assert e1.host == mf.HOSTS["1"] and e1.cores == 4 and to_ms(e1.added_time) == ta(6)
    e2 = one(e[(e.spark_context_id == A) & (e.executor_id == "2")])
    assert e2.removed_reason == mf.DECOM_REASON
    e0 = one(e[(e.spark_context_id == A) & (e.executor_id == "0")])
    assert is_null(e0.removed_time) and is_null(e0.removed_reason)


# ============================================================================================ findings
def _event_findings(f, ctx):
    g = f[f.spark_context_id == ctx]
    return g[~g.category.str.startswith("log:") & (g.category != "exception")]


def test_findings_event_categories(ds):
    f = ds(mf.MAIN, "findings")
    a = _event_findings(f, A)
    # Revision 12: executors 1-3 sat idle while executor 0 ran the 15-minute straggler
    idle = a[a.category == "executors_idle"]
    assert len(idle) == 1 and idle.iloc[0].severity == "low" and "3 of 4 executors" in idle.iloc[0].evidence
    a = a[a.category != "executors_idle"]
    assert sorted(a.category) == sorted(["stage_failed", "stage_failed", "task_skew", "disk_spill", "gc_pressure",
                                         "task_retries", "tiny_tasks", "executor_oom", "executor_lost",
                                         "query_failed", "oom_site"])
    assert len(_event_findings(f, B)) == 0


def test_findings_details(ds):
    f = ds(mf.MAIN, "findings")
    a = f[f.spark_context_id == A]
    skew = one(a[a.category == "task_skew"])
    assert skew.severity == "high" and skew.entity == "stage 0: mapPartitions at transform.py:42"
    assert "180.0" in skew.evidence and "900" in skew.evidence
    assert skew.stage_id == 0 and skew.stage_attempt == 0 and to_ms(skew.ts) == ta(922)
    spill = one(a[a.category == "disk_spill"])
    assert spill.severity == "medium" and "3.0 GiB" in spill.evidence and spill.stage_id == 1
    gc = one(a[a.category == "gc_pressure"])
    # 30% GC on a short stage cost it under a minute: low, not medium
    assert gc.severity == "low" and "30" in gc.evidence and gc.stage_id == 3
    tiny = one(a[a.category == "tiny_tasks"])
    assert tiny.stage_id == 2 and "2000" in tiny.evidence
    retries = one(a[a.category == "task_retries"])
    assert retries.stage_id == 4 and retries.evidence.startswith("2 failed task attempts")
    failed = a[a.category == "stage_failed"]
    assert set(failed.stage_attempt) == {0, 1} and (failed.stage_id == 6).all() and (failed.severity == "high").all()
    oom = one(a[a.category == "executor_oom"])
    assert oom.entity == "executor 1" and oom.severity == "high" and oom.executor_id == "1"
    assert "exit code 137" in oom.evidence and to_ms(oom.ts) == ta(1151)
    lost = one(a[a.category == "executor_lost"])
    assert lost.entity == "executor 2" and lost.severity == "medium"
    qf = one(a[a.category == "query_failed"])
    assert qf.entity == "query 3: INSERT OVERWRITE sales.daily_totals" and qf.severity == "high"
    assert qf.sql_execution_id == 3 and to_ms(qf.ts) == ta(1182)


def test_findings_log_and_exception(ds):
    f = ds(mf.MAIN, "findings")
    sig = ds(mf.MAIN, "log_signals")
    log_cats = set(f.category[f.category.str.startswith("log:")])
    assert log_cats == {f"log:{s}" for s in sig.signal.unique()}
    exc = f[f.category == "exception"]
    err = ds(mf.MAIN, "log_errors")
    assert len(exc) == err.fingerprint.nunique()
    assert set(exc.fingerprint) == set(err.fingerprint)
    oom_fp = err[err.exception_class == "java.lang.OutOfMemoryError"].fingerprint.iloc[0]
    oom = one(exc[exc.fingerprint == oom_fp])
    assert oom.severity == "high" and "2" in oom.evidence
    py_fp = err[(err.user_frame == mf.USER_FRAME)].fingerprint.unique()
    for fp in py_fp:
        row = one(exc[exc.fingerprint == fp])
        assert row.severity == "high"
        assert "transform.py" in row.evidence and "transform.py" in row.fix
    io_fp = err[err.exception_class == "java.io.IOException"].fingerprint.iloc[0]
    assert one(exc[exc.fingerprint == io_fp]).severity == "medium"


def test_findings_ids_and_order(ds):
    f = ds(mf.MAIN, "findings")
    assert list(f.finding_id) == [f"F{i:03d}" for i in range(1, len(f) + 1)] or \
        sorted(f.finding_id) == [f"F{i:03d}" for i in range(1, len(f) + 1)]
    f = f.sort_values("finding_id")
    rank = f.severity.map({"high": 0, "medium": 1, "low": 2})
    assert rank.is_monotonic_increasing
    assert set(f.severity) <= {"high", "medium", "low"}
    assert f.fix.notna().all() and f.evidence.notna().all()


def test_healthy_cluster_has_no_problems(ds):
    f = ds(mf.HEALTHY, "findings")
    assert len(f[f.severity == "high"]) == 0
    assert len(f) == 0
    s = ds(mf.HEALTHY, "stages")
    assert len(s) == 2 and (s.status == "succeeded").all()
    assert len(ds(mf.HEALTHY, "log_errors")) == 0
    assert len(ds(mf.HEALTHY, "log_signals")) == 0


# ============================================================================================ timeline
def test_timeline(ds):
    tl = ds(mf.MAIN, "timeline")
    sig = ds(mf.MAIN, "log_signals")
    assert int(tl["count"].sum()) == int(sig.ts.notna().sum())
    assert set(tl.signal) <= set(sig.signal)
    assert all(pd.Timestamp(m).second == 0 for m in tl.minute)


# ============================================================================================ combined
def test_query_profile(ds):
    qp = ds(mf.MAIN, "query_profile")
    q = ds(mf.MAIN, "sql_queries")
    assert len(qp) == len(q) == 5
    p3 = one(qp[(qp.spark_context_id == A) & (qp.sql_execution_id == 3)])
    assert p3.status == "failed" and p3.error
    assert p3.spark_jobs == 1 and p3.stages == 4
    assert p3.failed_tasks == 10  # 3 in 6.0 + 7 in 6.1
    assert p3.executors_lost >= 1  # executor 1 removed (OOM) while the query ran
    assert p3.findings >= 1 and p3.max_severity == "high"
    assert p3.plan_hash == one(q[(q.spark_context_id == A) & (q.sql_execution_id == 3)]).plan_hash
    p0 = one(qp[(qp.spark_context_id == A) & (qp.sql_execution_id == 0)])
    assert p0.disk_spill == 3 * GiB and p0.max_stage_skew == pytest.approx(180.0)
    assert p0.max_stage_ms == 902_000 and p0.tasks == 24 and p0.executors_lost == 0
    b0 = one(qp[(qp.spark_context_id == B) & (qp.sql_execution_id == 0)])
    assert b0.status == "succeeded" and b0.failed_tasks == 0


def test_stage_executor_profile(ds):
    sep = ds(mf.MAIN, "stage_executor_profile")
    g = sep[(sep.spark_context_id == A) & (sep.stage_id == 0) & (sep.stage_attempt == 0)]
    assert set(g.executor_id) == {"0", "1", "2", "3"}
    assert int(g.tasks.sum()) == 20
    e0 = one(g[g.executor_id == "0"])
    assert e0.task_ms_max == 900_000 and e0.share_of_stage_ms > 0.9
    assert g.share_of_stage_ms.sum() == pytest.approx(1.0, abs=1e-3)  # shares may be rounded
    assert ((sep.share_of_stage_ms >= 0) & (sep.share_of_stage_ms <= 1)).all()
    oom = one(sep[(sep.spark_context_id == A) & (sep.stage_id == 6) & (sep.stage_attempt == 0)
                  & (sep.executor_id == "1")])
    assert oom.failed_tasks == 1


def test_executor_profile(ds):
    ep = ds(mf.MAIN, "executor_profile")
    ll = ds(mf.MAIN, "log_lines")
    assert len(ep) == 6
    cat = {(r.spark_context_id, r.executor_id): r.removal_category for r in ep.itertuples()}
    assert cat[(A, "1")] == "oom" and cat[(A, "2")] == "lost"
    assert is_null(cat[(A, "0")]) and is_null(cat[(B, "0")])
    e1 = one(ep[(ep.spark_context_id == A) & (ep.executor_id == "1")])
    assert e1.app_id == mf.APP_A
    assert e1.log_lines == len(ll[(ll.app_id == mf.APP_A) & (ll.executor_id == "1")])
    assert e1.exceptions >= 2 and "OutOfMemoryError" in e1.top_exception
    assert "executor_oom" in list(e1.top_signals)
    assert e1.lifetime_ms == ta(1151) - ta(6)
    # executor "0" exists in both contexts; logs are attributed via app_id
    a0 = one(ep[(ep.spark_context_id == A) & (ep.executor_id == "0")])
    b0 = one(ep[(ep.spark_context_id == B) & (ep.executor_id == "0")])
    assert a0.log_lines == len(ll[(ll.app_id == mf.APP_A) & (ll.executor_id == "0")])
    assert b0.log_lines == len(ll[(ll.app_id == mf.APP_B) & (ll.executor_id == "0")])
    assert a0.disk_spill == 768 * (1 << 20)
    bs = ep.busy_share.dropna()
    assert ((bs >= 0) & (bs <= 1)).all()
    assert all(len(list(x)) <= 5 for x in ep.top_signals.dropna())


def test_run_story(ds):
    rs = ds(mf.MAIN, "run_story")
    assert len(rs) > 0
    allowed = {"app_start", "app_end", "job_start", "job_end", "stage_start", "stage_end", "stage_failed",
               "query_start", "query_end", "query_failed", "executor_added", "executor_removed", "log_error",
               "log_signal", "finding", "task_retry"}
    assert set(rs.kind) <= allowed
    assert {"app_start", "job_start", "job_end", "stage_start", "stage_end", "stage_failed", "query_start",
            "query_failed", "executor_added", "executor_removed", "log_signal"} <= set(rs.kind)
    assert set(rs.severity) <= {"info", "low", "medium", "high"}
    assert (rs["count"] >= 1).all()
    # story_seq is 0..n-1 in ts order (globally, or per spark context)
    seqs = sorted(rs.story_seq)
    if seqs == list(range(len(rs))):
        groups = [rs]
    else:
        groups = [g for _, g in rs.groupby(rs.spark_context_id.fillna(""))]
        for g in groups:
            assert sorted(g.story_seq) == list(range(len(g)))
    for g in groups:
        g = g.sort_values("story_seq")
        ts = [to_ms(x) for x in g.ts if to_ms(x) is not None]
        assert ts == sorted(ts)
    # failing stage / query appear
    sf = rs[(rs.kind == "stage_failed") & (rs.spark_context_id == A)]
    assert set(sf.stage_id) == {6}
    qf = one(rs[(rs.kind == "query_failed") & (rs.spark_context_id == A)])
    assert qf.sql_execution_id == 3 and qf.severity == "high"
    # plain INFO lines never make it into the story
    logs = rs[rs.kind.isin(["log_error", "log_signal"])]
    assert not logs.title.fillna("").str.contains("Running Spark version").any()


# ============================================================================================ summary.json
def test_summary_main(ds, expected):
    s = ds.summary(mf.MAIN)
    for k in ("cluster_id", "built_at", "input_dir", "tool_version", "empty_reason", "status", "start_time",
              "end_time", "duration_ms", "spark_versions", "counts", "totals", "findings_by_severity", "rows",
              "diagnosis"):
        assert k in s, k
    assert s["cluster_id"] == mf.MAIN
    assert s["empty_reason"] is None
    assert s["status"] == "failed"
    assert s["spark_versions"] == ["3.5.0"]
    assert s["start_time"] == ta(0) and isinstance(s["start_time"], int)
    assert s["end_time"] == tb(60)
    c = s["counts"]
    assert c["malformed_event_lines"] == 1
    assert c["apps"] == 2
    assert c["stages"] == len(ds(mf.MAIN, "stages"))
    assert c["failed_stages"] == 2
    assert c["spark_jobs"] == 7 and c["failed_jobs"] == 1
    assert c["sql_queries"] == 5 and c["failed_queries"] == 1
    assert c["tasks"] == sum(expected[mf.MAIN]["tasks"].values())
    assert c["failed_tasks"] == sum(expected[mf.MAIN]["failed_tasks"].values())
    assert c["executors"] == 6 and c["executors_lost"] in (1, 2)  # lost-only or all removals
    assert c["log_lines"] == expected[mf.MAIN]["log_lines"]
    assert c["findings"] == len(ds(mf.MAIN, "findings"))
    assert c["story_rows"] == len(ds(mf.MAIN, "run_story"))
    assert c["files"] == expected[mf.MAIN]["files"]
    for name in DATASETS:
        if name in s["rows"]:
            assert s["rows"][name] == len(ds(mf.MAIN, name)), name
    f = ds(mf.MAIN, "findings")
    assert s["findings_by_severity"] == {k: int((f.severity == k).sum()) for k in ("high", "medium", "low")}
    assert s["totals"]["disk_spill"] >= 3 * GiB


def test_summary_diagnosis(ds):
    d = ds.summary(mf.MAIN)["diagnosis"]
    assert len(d) >= 3
    assert [x["step"] for x in d] == list(range(1, len(d) + 1))
    kinds = [x["kind"] for x in d]
    order = ["outcome", "first_error", "root_cause", "performance", "code_location", "next_steps"]
    assert set(kinds) <= set(order)
    assert kinds == sorted(kinds, key=order.index)
    assert kinds[0] == "outcome" and "next_steps" in kinds and "first_error" in kinds
    assert d[0]["severity"] == "high"
    for x in d:
        assert x["severity"] in {"high", "medium", "low", "info"}
        assert x["title"] and x["text"]
        for link in x.get("links", []):
            assert link["type"] in {"stage", "query", "executor", "job", "finding", "log", "error"}
    if "code_location" in kinds:
        cl = d[kinds.index("code_location")]
        blob = cl["text"] + " " + " ".join(str(v) for v in cl.get("links", []))
        # user_frame of the PythonException, or the notebook path / query call site of the failed job
        assert "transform.py" in blob or "/Workspace/Users/me/etl" in blob, blob


def test_summary_healthy_and_empty(ds):
    h = ds.summary(mf.HEALTHY)
    assert h["status"] == "succeeded" and h["empty_reason"] is None
    assert h["findings_by_severity"]["high"] == 0
    e = ds.summary(mf.EMPTY)
    assert e["empty_reason"]
    assert e["status"] == "unknown"
    assert e["counts"]["files"] == 0 and e["counts"]["findings"] == 0
    assert e["start_time"] is None
