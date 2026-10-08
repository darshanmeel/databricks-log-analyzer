"""Pure-function tests for parsing.eventlog.iter_events: string pre-filter, lenient spacing, malformed lines."""

from __future__ import annotations

import json

import pytest

import make_fixtures as mf

ev = pytest.importorskip("databricks_cluster_log_analyzer.parsing.eventlog")

SQL_UI = "org.apache.spark.sql.execution.ui."
WANTED = {"SparkListenerTaskEnd", "SparkListenerJobStart", SQL_UI + "SparkListenerSQLExecutionStart"}


def run(lines, wanted=WANTED):
    return list(ev.iter_events(iter(lines), wanted))


def test_lenient_spacing():
    lines = ['{"Event":"SparkListenerJobStart","Job ID":0}',
             '{"Event": "SparkListenerJobStart", "Job ID": 1}',
             '{"Event" :  "SparkListenerJobStart" , "Job ID":2}',
             '{"Event":\t"SparkListenerJobStart","Job ID":3}']
    out = run(lines)
    assert [e for e, _ in out] == ["SparkListenerJobStart"] * 4
    assert [d["Job ID"] for _, d in out] == [0, 1, 2, 3]


def test_unwanted_events_filtered():
    lines = [json.dumps({"Event": "SparkListenerTaskStart", "Stage ID": 0}),
             json.dumps({"Event": "SparkListenerEnvironmentUpdate"}),
             json.dumps({"Event": "SparkListenerTaskEnd", "Stage ID": 0}),
             json.dumps({"Event": SQL_UI + "SparkListenerSQLExecutionStart", "executionId": 3}),
             json.dumps({"Event": SQL_UI + "SparkListenerDriverAccumUpdates", "executionId": 3})]
    out = run(lines)
    assert [e for e, _ in out] == ["SparkListenerTaskEnd", SQL_UI + "SparkListenerSQLExecutionStart"]
    assert out[1][1]["executionId"] == 3


def test_task_end_prefix_does_not_match_longer_name():
    # "SparkListenerTaskEnd" must not match e.g. a hypothetical "SparkListenerTaskEndX"
    out = run(['{"Event":"SparkListenerTaskEndX","Stage ID":0}'])
    assert out == []


def test_malformed_lines_skipped_not_fatal():
    lines = ['{"Event":"SparkListenerTaskEnd","Stage ID":6,"Task Info":{"Task ID":99',  # truncated
             '{"Event":"SparkListenerJobStart","Job ID":7}',
             "",
             "garbage that is not json at all",
             '{"Event":"SparkListenerJobStart","Job ID":8}']
    out = run(lines)
    assert [d["Job ID"] for _, d in out] == [7, 8]


def test_prefilter_before_json(monkeypatch):
    """Lines that don't carry a wanted event name must never reach json.loads."""
    if not hasattr(ev, "json"):
        pytest.skip("eventlog module does not use the json module directly")
    import types

    real_json = ev.json
    calls = []

    def loads(s, *a, **k):
        calls.append(s)
        return real_json.loads(s, *a, **k)

    fake = types.SimpleNamespace(**{k: getattr(real_json, k) for k in dir(real_json) if not k.startswith("__")})
    fake.loads = loads
    monkeypatch.setattr(ev, "json", fake)
    lines = ['{"Event":"SparkListenerBlockManagerAdded","x":1}'] * 500 + ['{"Event":"SparkListenerJobStart","Job ID":1}']
    out = run(lines)
    assert len(out) == 1
    assert len(calls) == 1


def test_rolled_fixture_eventlog_has_tiny_tasks(fixture_root):
    readers = pytest.importorskip("databricks_cluster_log_analyzer.readers")
    p = fixture_root / mf.MAIN / "eventlog" / mf.HASH_A / mf.CTX_A / "eventlog-2026-10-06--18-00.gz"
    out = list(ev.iter_events(readers.open_text_lines(p), {"SparkListenerTaskEnd"}))
    tiny = [d for _, d in out if d["Stage ID"] == 2]
    assert len(tiny) == 2000


def test_fixture_active_eventlog_malformed_line_skipped(fixture_root):
    readers = pytest.importorskip("databricks_cluster_log_analyzer.readers")
    p = fixture_root / mf.MAIN / "eventlog" / mf.HASH_A / mf.CTX_A / "eventlog"
    wanted = set(getattr(ev, "DEFAULT_EVENTS", ())) or {
        "SparkListenerTaskEnd", "SparkListenerStageCompleted", "SparkListenerJobStart", "SparkListenerJobEnd",
        "SparkListenerExecutorAdded", "SparkListenerExecutorRemoved", SQL_UI + "SparkListenerSQLExecutionStart",
        SQL_UI + "SparkListenerSQLExecutionEnd", SQL_UI + "SparkListenerSQLAdaptiveExecutionUpdate"}
    out = list(ev.iter_events(readers.open_text_lines(p), wanted))
    aqe = [d for e, d in out if e.endswith("SparkListenerSQLAdaptiveExecutionUpdate")]
    assert len(aqe) == 2 and "isFinalPlan=true" in aqe[-1]["physicalPlanDescription"]
    # every yielded payload is a dict
    assert all(isinstance(d, dict) for _, d in out)
