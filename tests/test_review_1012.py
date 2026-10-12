"""Review of 2026-10-12: log lines the event log does not back, source database timeouts, "usual" per table, the
local catch-all run, and the helpers that rank what to change."""
from databricks_cluster_log_analyzer.analysis.findings import confirmed_signals
from databricks_cluster_log_analyzer.analysis.retries import failure_category
from databricks_cluster_log_analyzer.analysis.runs import set_typical
from databricks_cluster_log_analyzer.api.queries import _at_once, _clause, _words_ms
from databricks_cluster_log_analyzer.config import load_rules

MIN = 60_000


def test_a_fetch_failure_line_needs_the_event_log_to_back_it():
    sig = [{"signal": "fetch_failure", "ts": 1_000, "line": "at ...tryOrFetchFailedException(ShuffleBlockFetcherIterator.scala:1722)"}]
    stages = [{"stage_attempt": 0, "failure_reason": None}]
    assert confirmed_signals(sig, stages, [], []) == []
    assert confirmed_signals(sig, [{"stage_attempt": 1, "failure_reason": None}], [], []) == sig
    assert confirmed_signals(sig, stages, [{"first_failure_category": "fetch_failed"}], []) == sig
    assert confirmed_signals(sig, [], [], []) == sig  # no event log: the logs are all there is


def test_executors_lost_as_the_app_stops_are_not_a_loss():
    end = 10 * MIN
    sig = [{"signal": "executor_lost", "ts": end - 1_000, "line": "Lost executor 1"},
           {"signal": "cache_lost", "ts": end - 500, "line": "No more replicas available for rdd_1_0"},
           {"signal": "executor_lost", "ts": end - 5 * MIN, "line": "Lost executor 2"}]
    out = confirmed_signals(sig, [{"stage_attempt": 0}], [], [{"end_time": end}])
    assert [r["ts"] for r in out] == [end - 5 * MIN]


def test_a_source_database_timeout_is_not_an_error_in_task_code():
    rules = load_rules()
    hana = ("Data receive failed. java.io.IOException: Connection timed out: 10.0.0.1:50214 -> 10.0.0.2:30215 "
            "System:X ConnectionID:358636 SessionID:1679451178677251")
    assert failure_category("ExceptionFailure", hana, None, rules) == "source_db"
    assert failure_category("ExceptionFailure", "SAP DBTech JDBC: Cannot connect to jdbc:sap://h:30215 (socket timeout)",
                            None, rules) == "source_db"
    assert failure_category("ExceptionFailure", "ValueError: bad row", None, rules) == "exception"


def test_usual_compares_runs_on_the_same_table_only():
    rows = [{"run_key": f"r{i}", "program": "load", "subject": "t1" if i < 3 else f"t{i}", "databricks_job_id": "9",
             "duration_ms": 60_000 * (1 + i)} for i in range(6)]
    set_typical(rows)
    by = {r["run_key"]: r for r in rows}
    assert by["r0"]["typical_duration_ms"] == 120_000 and by["r2"]["vs_typical"] == 1.5
    assert by["r4"]["typical_duration_ms"] is None  # one run on its table: no usual yet


def test_what_to_change_helpers():
    assert _words_ms("4 queries only counted rows: 6 h 11 m of Spark work") == (6 * 60 + 11) * MIN
    assert _words_ms("nothing timed") == 0
    assert _clause("No Spark stage ran for 12 m (73%): 8 m inside statements") == "No Spark stage ran for 12 m (73%)"
    assert _clause("136 stages ran 50 tasks each (6,800 of 7,154 tasks; 95%); median 20 ms") == "136 stages ran 50 tasks each (6,800 of 7,154 tasks; 95%)"
    t = 60 * MIN  # a start time of 0 is "no start time"
    runs = [{"start_time": t, "end_time": t + 10 * MIN}, {"start_time": t, "end_time": t + 10 * MIN},
            {"start_time": t + 20 * MIN, "end_time": t + 30 * MIN}]
    assert _at_once(runs) == 1.5  # 30 minutes of runs over 20 minutes with one going
