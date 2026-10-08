"""CONTRACT Revision 3 (real-cluster lessons) over the synthetic REV3 fixture cluster, plus the Revision 2 / 3
HTTP endpoints (/api/sources, /api/sources/clusters, /api/ingest, /steps, /steps/{id}).

Strict where the contract is explicit, loose where it leaves details open (noted inline)."""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timedelta

import pandas as pd
import pytest

import make_fixtures as mf
from conftest import ms, to_ms

R = mf.REV3
CTX = mf.CTX_R
APP = mf.APP_R
ll_mod = pytest.importorskip("databricks_cluster_log_analyzer.parsing.loglines")


def tr(sec: float) -> int:
    return ms(mf.T0R + timedelta(seconds=sec))


def one(df: pd.DataFrame) -> pd.Series:
    assert len(df) == 1, f"expected exactly one row, got {len(df)}:\n{df}"
    return df.iloc[0]


def is_null(v) -> bool:
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def is_true(v) -> bool:
    return (not is_null(v)) and bool(v) is True


def exp():
    return mf.EXPECTED[R]


def fpath(name: str) -> str:
    return f"executor/{APP}/{name}" if not name.startswith("driver/") else name


def parse(lines, rules, *, source="driver", executor_id=None, app_id=None, file_path="driver/stderr", start_seq=0):
    return list(ll_mod.parse_log_file(iter(lines), source=source, executor_id=executor_id, app_id=app_id,
                                      file_path=file_path, file_name=file_path.rsplit("/", 1)[-1],
                                      start_seq=start_seq, rules=rules))


# ============================================================================================ fixture sanity
def test_rev3_fixture_layout(fixture_root):
    c = fixture_root / R
    assert (c / "driver" / "stdout--2026-10-09--18-00").is_file()
    assert (c / "driver" / "stderr--2026-10-09--18-00").is_file()
    assert (c / "driver" / "2026-10-09-18.stacktrace.log.gz").read_bytes()[:2] == b"\x1f\x8b"
    assert (c / "executor" / APP / "0" / "stderr--2026-10-09--18.gz").read_bytes()[:2] == b"\x1f\x8b"
    assert (c / "executor" / APP / "1" / "stdout--2026-10-09--18.gz").read_bytes()[:2] == b"\x1f\x8b"
    ev = c / "eventlog" / mf.HASH_R / CTX
    assert (ev / "eventlog-2026-10-09--18-00.gz").read_bytes()[:2] == b"\x1f\x8b"
    assert (ev / "eventlog").is_file()
    assert any((c / "init_scripts").rglob("*.log"))
    # Revision 3 item 1: no double nesting; the cluster folder holds driver/ executor/ eventlog/ directly
    assert not (c / R).exists()


# ============================================================================================ files / ordering
def test_init_scripts_ignored(ds):
    for name in ("log_lines", "log_errors", "log_signals", "file_lines", "gc_events"):
        df = ds(R, name)
        assert not df.file_path.astype(str).str.startswith("init_scripts").any(), name
    files = ds(R, "files")
    real = files[~files.path.str.startswith("init_scripts/")]
    assert len(real) == exp()["files"]
    # if init_scripts files are listed at all, they are not classified as driver/executor/eventlog
    init = files[files.path.str.startswith("init_scripts/")]
    assert set(init.folder) <= {"other"}
    err = ds(R, "log_errors")
    assert "java.lang.RuntimeException" not in set(err.exception_class)


def test_log_lines_count(ds):
    ll = ds(R, "log_lines")
    assert len(ll) == exp()["log_lines"]
    assert ll.seq.is_unique
    assert set(ll.app_id.dropna()) == {APP}


def test_rolled_before_active_rev3_names(ds):
    ll = ds(R, "log_lines")
    pairs = [
        ("driver/stdout--2026-10-09--18-00", "driver/stdout"),
        ("driver/stderr--2026-10-09--18-00", "driver/stderr"),
        ("driver/2026-10-09-18.stacktrace.log.gz", "driver/stacktrace.log"),
        (f"executor/{APP}/0/stderr--2026-10-09--18.gz", f"executor/{APP}/0/stderr"),
        (f"executor/{APP}/1/stdout--2026-10-09--18.gz", f"executor/{APP}/1/stdout"),
    ]
    for rolled, active in pairs:
        r, a = ll[ll.file_path == rolled].seq, ll[ll.file_path == active].seq
        assert len(r) and len(a), (rolled, active)
        assert r.max() < a.min(), (rolled, active)
    assert ll[ll.source == "driver"].seq.max() < ll[ll.source == "executor"].seq.min()


# ============================================================================================ timestamps
def test_iso_and_jvm_timestamps_driver_stdout(ds):
    ll = ds(R, "log_lines")
    so = ll[ll.file_path == "driver/stdout"].sort_values("line_no")
    ts = [to_ms(x) for x in so.ts]
    assert ts[0] == ms(datetime(2026, 10, 9, 18, 0, 20, 123000))   # 2026-10-09T18:00:20.123456Z
    assert ts[1] == ms(datetime(2026, 10, 9, 18, 0, 21, 456000))   # 2026-10-09 18:00:21,456
    assert ts[2] == ts[1]                                           # no timestamp -> inherited
    assert ts[3] == tr(18.5)                                        # [..+0200] JVM line -> UTC
    rolled = ll[ll.file_path == "driver/stdout--2026-10-09--18-00"].sort_values("line_no")
    assert [to_ms(x) for x in rolled.ts] == [ms(datetime(2026, 10, 9, 18, 0, 13, 250000)),
                                             ms(datetime(2026, 10, 9, 18, 0, 14, 500000))]
    # the "plain line" carries no level of its own unless inherited from a level-bearing line (open)
    assert is_null(so.iloc[2].level) or so.iloc[2].level in ("INFO", "WARN", "WARNING")


def test_jvm_unified_level_and_logger(ds):
    ll = ds(R, "log_lines")
    e1 = ll[ll.file_path == f"executor/{APP}/1/stdout"].sort_values("line_no")
    assert len(e1) == 5
    assert [to_ms(x) for x in e1.ts] == [tr(120), tr(126), tr(132), tr(134), tr(141)]
    warn = e1.iloc[2]
    assert warn.level == "WARN"
    assert str(warn.logger).startswith("gc")
    info = e1.iloc[0]
    assert info.level == "INFO" and info.logger == "gc"
    e0 = ll[ll.file_path == f"executor/{APP}/0/stdout"].sort_values("line_no")
    assert e0.ts.notna().all() and set(e0.level) == {"INFO"}


def test_jvm_line_parse_unit(rules):
    rows = parse(["[2026-10-06T18:00:01.123+0000][1.234s][info][gc] GC(12) Pause Young (Normal) "
                  "(G1 Evacuation Pause) 2048M->512M(8192M) 12.345ms",
                  "[2026-10-06T20:00:02.000+0200][2.000s][warning][gc] To-space exhausted",
                  "[2026-10-06T18:00:03.000+0000][3.000s][error][gc] something bad",
                  "no timestamp"], rules, source="executor", executor_id="1", app_id="app-1",
                 file_path="executor/app-1/1/stdout")
    assert [to_ms(r["ts"]) for r in rows] == [ms(datetime(2026, 10, 6, 18, 0, 1, 123000)),
                                              ms(datetime(2026, 10, 6, 18, 0, 2)),
                                              ms(datetime(2026, 10, 6, 18, 0, 3)),
                                              ms(datetime(2026, 10, 6, 18, 0, 3))]
    assert [r["level"] for r in rows[:3]] == ["INFO", "WARN", "ERROR"]
    assert rows[0]["logger"] == "gc"


def test_iso_line_parse_unit(rules):
    rows = parse(["2026-10-06T18:00:01.123456Z INFO something",
                  "2026-10-06 18:00:02,500 some python logging",
                  "next"], rules, file_path="driver/stdout")
    assert [to_ms(r["ts"]) for r in rows] == [ms(datetime(2026, 10, 6, 18, 0, 1, 123000)),
                                              ms(datetime(2026, 10, 6, 18, 0, 2, 500000)),
                                              ms(datetime(2026, 10, 6, 18, 0, 2, 500000))]


# ============================================================================================ continuation lines
def test_continuation_lines_inherit_level_logger(ds):
    ll = ds(R, "log_lines")
    e0 = ll[ll.file_path == f"executor/{APP}/0/stderr"].sort_values("line_no").reset_index(drop=True)
    hdr = one(e0[e0.line.str.contains("Error while releasing resources")])
    assert hdr.level == "ERROR" and hdr.logger == "TaskResources"
    assert not is_true(hdr.continuation)
    conts = e0[e0.line_no.isin([hdr.line_no + 1, hdr.line_no + 2])]
    assert [s.strip() for s in conts.line] == ["stage 2 attempt 0 (TID 14)", "[X_123_45])"]
    for r in conts.itertuples():
        assert is_true(r.continuation), r.line
        assert r.level == "ERROR" and r.logger == "TaskResources"
        assert to_ms(r.ts) == tr(141)
    bm = one(e0[e0.line.str.strip() == "(block manager 0 on 10.0.0.11)"])
    assert is_true(bm.continuation) and bm.level == "WARN" and bm.logger == "BlockManager"
    # plain log4j lines are not continuations
    plain = e0[e0.line.str.match(r"^\d{2}/\d{2}/\d{2} ")]
    assert not plain.continuation.map(is_true).any()


def test_continuation_parse_unit(rules):
    rows = parse(["26/10/06 18:00:00 ERROR TaskResources: first part of",
                  " the message",
                  " [X_123_45])",
                  "26/10/06 18:00:05 INFO Executor: next"], rules, source="executor", executor_id="0",
                 app_id="app-1", file_path="executor/app-1/0/stderr")
    assert [r["level"] for r in rows] == ["ERROR", "ERROR", "ERROR", "INFO"]
    assert [r["logger"] for r in rows] == ["TaskResources"] * 3 + ["Executor"]
    assert [bool(r.get("continuation")) for r in rows] == [False, True, True, False]


# ============================================================================================ thread dumps
def test_thread_dump_frames_not_attached_to_exception(ds):
    err = ds(R, "log_errors")
    drv = err[err.file_path == "driver/stderr"]
    ise = one(drv)
    assert ise.exception_class == "java.lang.IllegalStateException"
    assert ise.message == "Connection pool shut down"
    frames = list(ise.top_frames)
    assert len(frames) == 3, frames
    assert frames[1] == "at com.example.etl.Loader.fetch(Loader.java:88)"
    assert "com.example.etl.Loader.fetch(Loader.java:88)" in str(ise.user_frame)
    for tf in err.top_frames:
        blob = " ".join(tf)
        assert "Main.waitLoop" not in blob and "Listener.poll" not in blob and "Reader.next" not in blob
    assert len(err[err.file_path == "driver/stacktrace.log"]) == 0
    to = one(err[err.file_path == "driver/2026-10-09-18.stacktrace.log.gz"])
    assert to.exception_class == "java.util.concurrent.TimeoutException"
    assert len(list(to.top_frames)) == 2
    assert "com.example.etl.Waiter.await" in str(to.user_frame)
    assert set(err.exception_class) == {"java.lang.IllegalStateException", "java.util.concurrent.TimeoutException",
                                        "java.io.IOException"}


def test_thread_dump_parse_unit(rules):
    lines = ["java.lang.IllegalStateException: boom",
             "\tat com.acme.A.a(A.java:1)",
             "Full thread dump OpenJDK 64-Bit Server VM (17.0.12+7-LTS mixed mode):",
             "",
             '"main" #1 prio=5 os_prio=0 cpu=1.00ms elapsed=2.00s tid=0x1 nid=0x1 waiting on condition',
             "   java.lang.Thread.State: TIMED_WAITING (sleeping)",
             "\tat java.base@17.0.12/java.lang.Thread.sleep(Native Method)",
             "\tat com.acme.Main.loop(Main.java:5)",
             "",
             '"worker-7" #12 daemon prio=5 os_prio=0 cpu=1.00ms elapsed=2.00s tid=0x2 nid=0x2 runnable',
             "java.base@17.0.12/java.io.FileInputStream.readBytes(Native Method)"]
    errs = ll_mod.extract_errors(parse(lines, rules), rules)
    assert len(errs) == 1
    assert list(errs[0]["top_frames"]) == ["at com.acme.A.a(A.java:1)"]


def test_contiguous_frames_only(rules):
    lines = ["java.lang.IllegalArgumentException: bad",
             "\tat com.acme.X.x(X.java:1)",
             "\t... 3 more",
             "26/10/06 18:00:00 INFO Something: unrelated",
             "\tat com.acme.Late.frame(Late.java:9)",
             "Caused by: java.io.IOException: io",
             "\tat com.acme.Y.y(Y.java:2)"]
    errs = sorted(ll_mod.extract_errors(parse(lines, rules), rules), key=lambda e: e["seq"])
    assert [e["exception_class"] for e in errs] == ["java.lang.IllegalArgumentException", "java.io.IOException"]
    assert "at com.acme.Late.frame(Late.java:9)" not in list(errs[0]["top_frames"])
    assert list(errs[0]["top_frames"])[0] == "at com.acme.X.x(X.java:1)"
    assert list(errs[1]["top_frames"]) == ["at com.acme.Y.y(Y.java:2)"]


def test_file_lines_thread_dumps(ds, fixture_root):
    fl = ds(R, "file_lines")
    for col in ("cluster_id", "folder", "file_path", "lines", "bytes", "thread_dumps"):
        assert col in fl.columns
    by = {r.file_path: r for r in fl.itertuples()}
    assert by["driver/stderr"].thread_dumps >= 1
    assert by["driver/stacktrace.log"].thread_dumps >= 1
    for p in ("driver/log4j-active.log", "driver/stdout", f"executor/{APP}/0/stderr", f"executor/{APP}/1/stdout"):
        assert by[p].thread_dumps == 0, p
    text = (fixture_root / R / "driver" / "stderr").read_text("utf-8")
    assert by["driver/stderr"].lines == len(text.splitlines())
    assert by["driver/stderr"].folder == "driver"
    assert by[f"executor/{APP}/1/stdout"].folder == "executor"


# ============================================================================================ gc_events
def test_gc_events_parsing_and_units(ds):
    gc = ds(R, "gc_events")
    for col in ("cluster_id", "source", "app_id", "executor_id", "file_path", "seq", "ts", "gc_id", "kind", "cause",
                "heap_before_mb", "heap_after_mb", "heap_total_mb", "pause_ms"):
        assert col in gc.columns, col
    assert gc.gc_id.notna().all() and gc.kind.notna().all()
    e0 = gc[(gc.executor_id == "0") & (gc.app_id == APP)]
    assert (e0.source == "executor").all()

    def row(df, gid, prefix):
        return one(df[(df.gc_id == gid) & df.kind.str.startswith(prefix)])

    r = row(e0, 0, "Pause Young")
    assert to_ms(r.ts) == tr(8.1)
    assert (r.heap_before_mb, r.heap_after_mb, r.heap_total_mb) == (2048.0, 512.0, 8192.0)
    assert math.isclose(r.pause_ms, 12.345)
    assert "G1 Evacuation Pause" in str(r.cause) or "Normal" in str(r.cause)
    r = row(e0, 1, "Pause Young")                         # G units
    assert (r.heap_before_mb, r.heap_after_mb, r.heap_total_mb) == (3072.0, 1024.0, 8192.0)
    assert math.isclose(r.pause_ms, 20.5)
    r = row(e0, 4, "Pause Young")                         # K units
    assert (r.heap_before_mb, r.heap_after_mb, r.heap_total_mb) == (512.0, 256.0, 8192.0)
    assert math.isclose(r.pause_ms, 4.0)
    r = row(e0, 2, "Pause Remark")
    assert (r.heap_before_mb, r.heap_after_mb) == (3500.0, 3400.0) and math.isclose(r.pause_ms, 3.21)
    row(e0, 3, "Pause Cleanup")
    # concurrent phases are optional rows; when present they are "Concurrent ..." kinds
    assert set(k for k in e0.kind if not k.startswith("Pause")) <= {k for k in e0.kind if k.startswith("Concurrent")}

    e1 = gc[(gc.executor_id == "1") & gc.kind.str.startswith("Pause")]
    assert len(e1) == 6
    full = e1[e1.kind.str.startswith("Pause Full")]
    assert sorted(full.gc_id) == [2, 3, 4, 5]
    assert all("G1 Compaction Pause" in str(c) for c in full.cause)
    assert sorted(full.pause_ms) == [1500.0, 1600.0, 1700.0, 1800.0]
    # rolled .gz first in seq order
    assert e1[e1.gc_id == 0].seq.iloc[0] < e1[e1.gc_id == 2].seq.iloc[0]
    assert e1[e1.gc_id == 0].file_path.iloc[0] == f"executor/{APP}/1/stdout--2026-10-09--18.gz"

    drv = one(gc[gc.source == "driver"])
    assert is_null(drv.executor_id)
    assert to_ms(drv.ts) == tr(18.5)                      # +0200 offset normalized to UTC
    assert (drv.heap_before_mb, drv.heap_after_mb, drv.heap_total_mb) == (1024.0, 256.0, 4096.0)
    # stderr / log4j files never produce GC events
    assert not gc.file_path.str.contains("stderr|log4j").any()


def test_executor_profile_gc_columns(ds):
    ep = ds(R, "executor_profile")
    for col in ("gc_pauses", "gc_pause_ms", "full_gcs", "max_heap_after_mb"):
        assert col in ep.columns
    e1 = one(ep[(ep.spark_context_id == CTX) & (ep.executor_id == "1")])
    g = exp()["gc"]["1"]
    assert e1.gc_pauses == g["gc_pauses"]
    assert e1.full_gcs == g["full_gcs"]
    assert math.isclose(e1.gc_pause_ms, g["gc_pause_ms"])
    assert math.isclose(e1.max_heap_after_mb, g["max_heap_after_mb"])
    e0 = one(ep[(ep.spark_context_id == CTX) & (ep.executor_id == "0")])
    assert e0.full_gcs == 0
    assert e0.gc_pauses in (5, 6)  # 5 pauses (+ the concurrent cycle if counted)
    e2 = one(ep[(ep.spark_context_id == CTX) & (ep.executor_id == "2")])
    assert is_null(e2.gc_pauses) or e2.gc_pauses == 0


def test_jvm_full_gc_finding(ds):
    f = ds(R, "findings")
    full = one(f[f.category == "jvm_full_gc"])
    assert full.severity == "medium"
    assert full.executor_id == "1"
    assert "4" in str(full.evidence)
    h = ds(mf.HEALTHY, "findings")
    assert not (h.category == "jvm_full_gc").any()


# ============================================================================================ event log
def test_event_counts_include_skipped_types(ds):
    ec = ds(R, "event_counts")
    ec = ec[ec.spark_context_id == CTX]
    got = dict(zip(ec.event_type, ec["count"]))
    for k, v in exp()["event_counts"].items():
        assert got.get(k) == v, (k, got.get(k), v)
    assert got["SparkListenerTaskStart"] == exp()["tasks"][CTX]


def test_task_start_skipped_and_task_columns(ds):
    t = ds(R, "tasks")
    t = t[t.spark_context_id == CTX]
    assert len(t) == exp()["tasks"][CTX]  # TaskStart events produce no rows
    assert int(t.failed.sum()) == exp()["failed_tasks"][CTX]
    for col in ("task_index", "task_attempt", "speculative"):
        assert col in t.columns
    spec = one(t[t.speculative.map(is_true)])
    assert (spec.stage_id, spec.stage_attempt, spec.task_index, spec.task_attempt) == (2, 0, 4, 1)
    assert spec.end_reason == "TaskKilled" and spec.executor_id == "2"
    retried = one(t[(t.stage_id == 2) & (t.stage_attempt == 0) & (t.task_index == 3) & (t.task_attempt == 1)])
    assert retried.executor_id == "0" and not is_true(retried.failed)
    lost = one(t[(t.stage_id == 2) & (t.stage_attempt == 0) & (t.task_index == 3) & (t.task_attempt == 0)])
    assert lost.end_reason == "ExecutorLostFailure" and "Command exited with code 9" in lost.error
    reasons = set(t.end_reason)
    assert {"Success", "ExecutorLostFailure", "ExceptionFailure", "FetchFailed", "TaskKilled"} <= reasons


def test_connect_operations(ds):
    co = ds(R, "connect_operations")
    assert len(co) == 3
    assert set(co.spark_context_id) == {CTX}
    for op, e in exp()["connect_operations"].items():
        r = one(co[co.operation_id == op])
        assert r.session_id == mf.R_SESSION
        assert r.user_id == mf.R_USER and r.user_name == mf.R_USER
        assert r.statement_text == e["statement_text"]
        assert r.job_tag == e["job_tag"]
        for col in ("start_time", "analyzed_time", "ready_time", "finish_time", "closed_time"):
            assert to_ms(getattr(r, col)) == e[col], (op, col)
        assert r.status == "finished"
        assert is_null(r.error)
        assert r.duration_ms in (e["finish_time"] - e["start_time"], e["closed_time"] - e["start_time"])


def test_spark_jobs_connect_link_and_tags(ds):
    sj = ds(R, "spark_jobs")
    sj = sj[sj.spark_context_id == CTX]
    assert len(sj) == 3
    assert set(sj.result) == {"JobSucceeded"}
    for op, e in exp()["connect_operations"].items():
        j = one(sj[sj.spark_job_id == e["spark_job_id"]])
        assert j.connect_operation_id == op
        assert e["job_tag"] in str(j.job_tags)
        assert j.databricks_task_run_id == "555000111"
        assert j.databricks_job_id == "123456789"
        assert j.databricks_run_id == "987654321"
    # the old fixtures have no Connect tags -> no link
    m = ds(mf.MAIN, "spark_jobs")
    assert m.connect_operation_id.isna().all()


def test_cluster_info(ds):
    ci = ds(R, "cluster_info")
    row = one(ci[ci.spark_context_id == CTX])
    for k, v in exp()["cluster_info"].items():
        assert getattr(row, k) == v, (k, getattr(row, k), v)
    assert row.cluster_id == R
    assert row.parent_run_id in (None, "987654000") or is_null(row.parent_run_id)


# ============================================================================================ executors
def test_removal_reasons_json_and_categories(ds):
    ex = ds(R, "executors")
    ex = ex[ex.spark_context_id == CTX]
    assert len(ex) == 4
    for eid, (cat, cause, raw, _finding) in exp()["removal"].items():
        r = one(ex[ex.executor_id == eid])
        assert r.removal_category == cat, (eid, r.removal_category)
        assert r.removed_reason == cause
        assert r.removed_reason_raw == raw
        assert to_ms(r.removed_time) == exp()["removed_time"][eid]
    ep = ds(R, "executor_profile")
    ep = ep[ep.spark_context_id == CTX]
    assert dict(zip(ep.executor_id, ep.removal_category)) == {k: v[0] for k, v in exp()["removal"].items()}


def test_removal_findings(ds):
    f = ds(R, "findings")
    rem = f[f.category.isin(["executor_oom", "executor_lost", "executor_killed"])]
    killed = one(rem)
    assert killed.category == "executor_killed" and killed.severity == "medium"
    assert killed.executor_id == "1"
    # autoscale / termination removals: no findings, but they are in the story
    rs = ds(R, "run_story")
    removed = rs[(rs.kind == "executor_removed") & (rs.spark_context_id == CTX)]
    assert set(removed.executor_id) == {"0", "1", "2", "3"}
    for r in removed.itertuples():
        if r.executor_id in ("0", "2", "3"):
            assert r.severity in ("info", "low"), (r.executor_id, r.severity)


# ============================================================================================ retries
def test_task_retries_rows(ds):
    tr_df = ds(R, "task_retries")
    tr_df = tr_df[tr_df.spark_context_id == CTX]
    keys = {(r.stage_id, r.stage_attempt, r.task_index) for r in tr_df.itertuples()}
    assert keys == set(exp()["task_retries"])
    for key, e in exp()["task_retries"].items():
        r = one(tr_df[(tr_df.stage_id == key[0]) & (tr_df.stage_attempt == key[1]) & (tr_df.task_index == key[2])])
        assert r.attempts == e["attempts"], key
        assert r.spark_job_id == e["spark_job_id"], key
        assert is_null(r.sql_execution_id)
        for col in ("first_attempt_executor_id", "first_attempt_host", "first_failure_reason", "final_status",
                    "final_executor_id", "retry_delay_ms", "wasted_ms"):
            if col in e:
                assert getattr(r, col) == e[col], (key, col, getattr(r, col))
        if "first_failure_categories" in e:
            assert r.first_failure_category in e["first_failure_categories"], (key, r.first_failure_category)
        if "first_failure_error" in e:
            assert e["first_failure_error"] in str(r.first_failure_error), key
        if "first_failure_time" in e:
            assert to_ms(r.first_failure_time) == e["first_failure_time"], key
        if "executor_removed_reason" in e:
            if e["executor_removed_reason"] is None:
                assert is_null(r.executor_removed_reason), key
            else:
                assert r.executor_removed_reason == e["executor_removed_reason"], key
    fetch = one(tr_df[(tr_df.stage_id == 3) & (tr_df.stage_attempt == 0)])
    assert fetch.final_status in ("failed", "succeeded", "succeeded later")  # the retry ran in stage attempt 1
    allowed = {"executor_lost", "oom", "fetch_failed", "exception", "killed", "other"}
    assert set(tr_df.first_failure_category.dropna()) <= allowed


def test_task_retries_other_clusters(ds):
    assert len(ds(mf.HEALTHY, "task_retries")) == 0
    m = ds(mf.MAIN, "task_retries")
    a = m[m.spark_context_id == mf.CTX_A]
    # stage 4: index 2 lost on the decommissioned executor, index 6 IOException; both retried and succeeded
    s4 = a[(a.stage_id == 4) & (a.stage_attempt == 0)]
    assert set(s4.task_index) == {2, 6}
    assert set(s4.final_status) == {"succeeded"}
    i2 = one(s4[s4.task_index == 2])
    assert i2.first_failure_reason == "ExecutorLostFailure" and i2.final_executor_id == "0"
    assert i2.retry_delay_ms == 1000


def test_stage_retry_of_failure(ds):
    st = ds(R, "stages")
    st = st[st.spark_context_id == CTX]
    assert len(st) == exp()["stages"][CTX]
    s30 = one(st[(st.stage_id == 3) & (st.stage_attempt == 0)])
    assert s30.status == "failed" and "FetchFailedException" in str(s30.failure_reason)
    s31 = one(st[(st.stage_id == 3) & (st.stage_attempt == 1)])
    assert s31.status == "succeeded"
    assert "FetchFailedException" in str(s31.retry_of_failure)
    s21 = one(st[(st.stage_id == 2) & (st.stage_attempt == 1)])
    assert s21.status == "succeeded"
    for r in st[st.stage_attempt == 0].itertuples():
        assert is_null(r.retry_of_failure), r.stage_id


def test_task_retries_finding_and_story(ds):
    f = ds(R, "findings")
    tf = f[f.category == "task_retries"]
    assert len(tf) >= 1
    assert 2 in set(tf.stage_id.dropna().astype(int))
    rs = ds(R, "run_story")
    blob = " ".join(rs.title.fillna("") + " " + rs.detail.fillna(""))
    assert re.search(r"retr", blob, re.I)


def test_summary_rev3_retries_step(ds):
    s = ds.summary(R)
    assert s["status"] == "succeeded"
    c = s["counts"]
    assert c["spark_jobs"] == 3 and c["failed_jobs"] == 0
    assert c["tasks"] == exp()["tasks"][CTX] and c["failed_tasks"] == exp()["failed_tasks"][CTX]
    titles = [x["title"] for x in s["diagnosis"]]
    assert any(t.startswith("Retries") for t in titles), titles
    assert "Retries (job still succeeded)" in titles
    assert [x["step"] for x in s["diagnosis"]] == list(range(1, len(titles) + 1))
    for name in ("gc_events", "connect_operations", "cluster_info", "task_retries", "event_counts", "file_lines"):
        if name in s["rows"]:
            assert s["rows"][name] == len(ds(R, name)), name
    # no retries step when jobs failed (MAIN) or nothing was retried (HEALTHY)
    for cid in (mf.MAIN, mf.HEALTHY):
        assert "Retries (job still succeeded)" not in [x["title"] for x in ds.summary(cid)["diagnosis"]]


# ============================================================================================ API
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

server = pytest.importorskip("databricks_cluster_log_analyzer.api.server")
SOURCE_TYPES = {"local", "volume", "adls", "s3"}


@pytest.fixture(scope="module")
def client(output_root, cache_root):
    with TestClient(server.create_app(output_root, cache_root)) as c:
        yield c


def get(client, url, status=200, **params):
    r = client.get(url, params=params or None)
    assert r.status_code == status, (url, r.status_code, r.text[:500])
    return r.json()


def assert_envelope(body):
    assert set(body) >= {"columns", "rows", "total", "limit", "offset"}
    assert isinstance(body["rows"], list) and isinstance(body["total"], int)


@pytest.mark.parametrize("name", ["gc_events", "connect_operations", "cluster_info", "task_retries",
                                  "event_counts", "file_lines"])
def test_api_new_datasets(client, ds, name):
    body = get(client, f"/api/clusters/{R}/datasets/{name}", limit=500)
    assert_envelope(body)
    assert body["total"] == len(ds(R, name))
    for row in body["rows"]:
        for col in ("ts", "start_time", "finish_time", "first_failure_time"):
            v = row.get(col)
            if v is not None:
                assert isinstance(v, int) and 1_500_000_000_000 < v < 2_500_000_000_000, (col, v)


def test_api_sources(client):
    body = get(client, "/api/sources")
    assert isinstance(body, list)
    by = {s["type"]: s for s in body}
    assert SOURCE_TYPES <= set(by)
    for s in body:
        assert set(s) >= {"type", "label", "available", "reason", "fields"}
        assert isinstance(s["available"], bool) and isinstance(s["label"], str) and s["label"]
        if not s["available"]:
            assert isinstance(s["reason"], str) and s["reason"]
        assert isinstance(s["fields"], list)
        for f in s["fields"]:
            assert {"name", "label"} <= set(f)
    assert by["local"]["available"] is True


def test_api_sources_missing_extra(output_root, cache_root, monkeypatch):
    import sys
    monkeypatch.setitem(sys.modules, "boto3", None)
    for m in ("azure.storage.filedatalake", "azure.identity"):
        monkeypatch.setitem(sys.modules, m, None)
    with TestClient(server.create_app(output_root, cache_root)) as c:
        by = {s["type"]: s for s in get(c, "/api/sources")}
        assert by["s3"]["available"] is False
        assert re.search(r"s3|boto3", by["s3"]["reason"], re.I)
        assert by["adls"]["available"] is False
        assert re.search(r"adls|azure", by["adls"]["reason"], re.I)
        r = c.post("/api/sources/clusters", json={"type": "s3", "root": "s3://example-bucket/cluster-logs",
                                                  "options": {}})
        assert r.status_code == 400, r.text
        detail = r.json()["detail"].lower()
        assert ("s3" in detail or "boto3" in detail) and ("install" in detail or "extra" in detail)
        r = c.post("/api/sources/clusters", json={"type": "adls",
                                                  "root": "abfss://logs@exampleacct.dfs.core.windows.net/cluster-logs",
                                                  "options": {}})
        assert r.status_code == 400, r.text
        assert re.search(r"adls|azure", r.json()["detail"], re.I)


def test_api_sources_clusters_local(client, fixture_root, tmp_path, cache_root):
    body = client.post("/api/sources/clusters", json={"type": "local", "root": str(fixture_root), "options": {}})
    assert body.status_code == 200, body.text
    rows = body.json()
    assert isinstance(rows, list)
    by = {r["cluster_id"]: r for r in rows}
    assert {mf.MAIN, mf.HEALTHY, R} <= set(by)
    for r in rows:
        assert set(r) >= {"cluster_id", "last_modified", "analyzed"}
        assert isinstance(r["analyzed"], bool)
        v = r["last_modified"]
        assert v is None or (isinstance(v, int) and 1_500_000_000_000 < v < 2_500_000_000_000), v
    assert by[R]["analyzed"] is True  # built into the client's output_root
    # with an empty output root nothing is analyzed
    with TestClient(server.create_app(tmp_path / "out", cache_root)) as c2:
        rows2 = c2.post("/api/sources/clusters", json={"type": "local", "root": str(fixture_root),
                                                       "options": {}}).json()
        assert rows2 and not any(r["analyzed"] for r in rows2)


def test_api_sources_clusters_errors(client, tmp_path):
    r = client.post("/api/sources/clusters", json={"type": "local", "root": str(tmp_path / "missing"), "options": {}})
    assert r.status_code in (400, 404), r.text
    assert "detail" in r.json()
    r = client.post("/api/sources/clusters", json={"type": "nope", "root": "x", "options": {}})
    assert r.status_code in (400, 422), r.text


def test_api_ingest_local(fixture_root, tmp_path, cache_root):
    out = tmp_path / "out"
    with TestClient(server.create_app(out, cache_root)) as c:
        r = c.post("/api/ingest", json={"type": "local", "root": str(fixture_root), "cluster_id": mf.HEALTHY,
                                        "options": {}})
        assert r.status_code == 200, r.text
        body = r.json()
        assert set(body) >= {"download", "summary"}
        assert body["download"] is None  # local sources are built in place
        assert body["summary"]["cluster_id"] == mf.HEALTHY and body["summary"]["status"] == "succeeded"
        assert (out / mf.HEALTHY / "summary.json").is_file()
        ids = {x["cluster_id"] for x in get(c, "/api/clusters")}
        assert mf.HEALTHY in ids
        r = c.post("/api/ingest", json={"type": "local", "root": str(fixture_root), "cluster_id": "9999-999999-nothere1",
                                        "options": {}})
        assert r.status_code in (400, 404), r.text


def test_api_steps(client, ds):
    steps = get(client, f"/api/clusters/{R}/steps")
    assert isinstance(steps, list) and steps
    ids = [s["id"] for s in steps]
    assert len(ids) == len(set(ids))
    for s in steps:
        assert set(s) >= {"id", "group", "step", "title", "description", "rows"}
        assert isinstance(s["title"], str) and s["title"]
        assert s["rows"] is None or (isinstance(s["rows"], int) and s["rows"] >= 0)
    groups = " ".join(s["group"] for s in steps).lower()
    for g in ("setup", "driver", "executor", "event", "analysis"):
        assert g in groups, g
    for s in steps:
        body = get(client, f"/api/clusters/{R}/steps/{s['id']}", limit=5)
        assert_envelope(body)
        assert len(body["rows"]) <= 5
        assert body["total"] >= len(body["rows"])
    get(client, f"/api/clusters/{R}/steps/no-such-step", status=404)
    get(client, "/api/clusters/9999-999999-nothere1/steps", status=404)


def test_api_steps_paging(client):
    steps = get(client, f"/api/clusters/{mf.MAIN}/steps")
    big = [s for s in steps if (s["rows"] or 0) > 3]
    assert big, steps
    sid = big[0]["id"]
    a = get(client, f"/api/clusters/{mf.MAIN}/steps/{sid}", limit=2, offset=0)
    b = get(client, f"/api/clusters/{mf.MAIN}/steps/{sid}", limit=2, offset=2)
    assert a["limit"] == 2 and b["offset"] == 2
    assert a["total"] == b["total"] > 3
    assert json.dumps(a["rows"], sort_keys=True) != json.dumps(b["rows"], sort_keys=True)
