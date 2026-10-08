"""Pure-function tests for parsing.loglines (line iterators in, row dicts out)."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime

import pytest

from conftest import ms, to_ms

ll = pytest.importorskip("databricks_cluster_log_analyzer.parsing.loglines")


def parse(lines, rules, *, source="driver", executor_id=None, app_id=None, file_path="driver/log4j-active.log",
          start_seq=0):
    return list(ll.parse_log_file(iter(lines), source=source, executor_id=executor_id, app_id=app_id,
                                  file_path=file_path, file_name=file_path.rsplit("/", 1)[-1],
                                  start_seq=start_seq, rules=rules))


def ref_fingerprint(cls, frames):
    parts = [cls] + [re.sub(r":\d+\)|\d+|0x[0-9a-f]+", "", f) for f in frames[:3]]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:12]


# ---------------------------------------------------------------------------------------------- timestamps
def test_ts_parse_level_logger_message(rules):
    rows = parse(["26/10/06 17:48:12 INFO SparkContext: Running Spark version 3.5.0",
                  "26/10/06 17:48:13 ERROR TaskSchedulerImpl:Lost executor 1 on host"], rules)
    assert to_ms(rows[0]["ts"]) == ms(datetime(2026, 10, 6, 17, 48, 12))
    assert rows[0]["level"] == "INFO"
    assert rows[0]["logger"] == "SparkContext"
    assert rows[0]["message"] == "Running Spark version 3.5.0"
    assert rows[1]["level"] == "ERROR"
    assert rows[1]["message"] == "Lost executor 1 on host"  # ": ?" -> space after colon optional
    assert rows[0]["line"].startswith("26/10/06 17:48:12 INFO")


def test_ts_inheritance_within_file(rules):
    lines = ["no timestamp yet",
             "26/10/06 17:48:12 INFO A: first",
             "\tat org.apache.Foo.bar(Foo.java:1)",
             "26/10/06 17:48:15 WARN B: second",
             "plain continuation"]
    rows = parse(lines, rules)
    t1, t2 = ms(datetime(2026, 10, 6, 17, 48, 12)), ms(datetime(2026, 10, 6, 17, 48, 15))
    assert [to_ms(r["ts"]) for r in rows] == [None, t1, t1, t2, t2]
    # an indented stack-trace line is a continuation and inherits its record's level; a plain line does not
    assert [r["level"] for r in rows] == [None, "INFO", "INFO", "WARN", None]
    assert [r["continuation"] for r in rows] == [False, False, True, False, False]
    assert rows[0]["message"] == "no timestamp yet"  # message = raw line when no log4j prefix
    assert rows[4]["message"] == "plain continuation"


def test_ts_not_inherited_across_files(rules):
    parse(["26/10/06 17:48:12 INFO A: x"], rules, file_path="driver/log4j-active.log")
    rows = parse(["no ts here"], rules, file_path="driver/stdout", start_seq=10)
    assert to_ms(rows[0]["ts"]) is None


def test_seq_and_line_no(rules):
    rows = parse(["a", "b", "c"], rules, start_seq=100)
    assert [r["seq"] for r in rows] == [100, 101, 102]
    assert [r["line_no"] for r in rows] == [1, 2, 3]


def test_line_truncated_to_2000(rules):
    rows = parse(["x" * 5000], rules)
    assert len(rows[0]["line"]) == 2000


# ---------------------------------------------------------------------------------------------- signals
@pytest.mark.parametrize("line,expected", [
    ("java.lang.OutOfMemoryError: Java heap space", "executor_oom"),
    # matches executor_oom ("Container killed") AND executor_lost ("Lost executor"): first in list wins
    ("26/10/06 18:07:23 ERROR TaskSchedulerImpl: Lost executor 1 on h: Container killed by YARN", "executor_oom"),
    # executor_lost and storage_throttling both match; executor_lost comes first
    ("Lost executor 3: throttled by S3", "executor_lost"),
    ("org.apache.spark.shuffle.FetchFailedException: Failed to connect", "fetch_failure"),
    ("Thread 75 spilling sort data of 768.0 MiB to disk", "disk_spill"),
    ("[Full GC (Ergonomics) 2.94 secs]", "gc_pressure"),
    ("Total size of serialized results is bigger than spark.driver.maxResultSize", "driver_unresponsive"),
    ("Could not execute broadcast in 300 secs.", "broadcast_timeout"),
    ("java.io.IOException: No space left on device", "disk_full"),
    ("Status Code: 503; Error Code: SlowDown", "storage_throttling"),
    ("[UNRESOLVED_COLUMN.WITH_SUGGESTION] AnalysisException", "schema_error"),
    ("Traceback (most recent call last):", "python_error"),
    ("Executor decommission: spot", "executor_lost"),
])
def test_find_signal_first_match_wins(rules, line, expected):
    hit = ll.find_signal(line, rules)
    assert hit is not None, line
    sig, severity, fix = hit
    assert sig == expected
    assert severity in ("high", "medium", "low")
    assert fix


def test_find_signal_no_match(rules):
    assert ll.find_signal("26/10/06 17:48:12 INFO SparkContext: Running Spark version 3.5.0", rules) is None
    # case-sensitive like the notebook regex
    assert ll.find_signal("full gc happened", rules) is None


def test_signals_in_order(rules):
    names = [s.name if hasattr(s, "name") else s[0] for s in rules.signals]
    assert names == ["executor_oom", "driver_unresponsive", "gc_pressure", "disk_spill", "cache_not_fit",
                     "cache_dropped", "cache_lost", "fetch_failure",
                     "executor_lost", "broadcast_timeout", "disk_full", "storage_throttling", "schema_error",
                     "python_error"]


def test_parse_rows_carry_signal(rules):
    rows = parse(["26/10/06 18:07:23 WARN TaskSetManager: Lost task 0.0: java.lang.OutOfMemoryError: Java heap space",
                  "26/10/06 18:07:24 INFO X: fine"], rules)
    assert rows[0]["signal"] == "executor_oom"
    assert rows[1]["signal"] is None


# ---------------------------------------------------------------------------------------------- exceptions
OOM_BLOCK = [
    "26/10/06 18:07:20 ERROR Executor: Exception in task 0.0 in stage 6.0 (TID 2046)",
    "java.lang.OutOfMemoryError: Java heap space",
    "\tat java.util.Arrays.copyOf(Arrays.java:3236)",
    "\tat org.apache.spark.unsafe.map.BytesToBytesMap.growAndRehash(BytesToBytesMap.java:906)",
    "\tat org.apache.spark.sql.execution.joins.UnsafeHashedRelation$.apply(HashedRelation.scala:458)",
    "\tat org.apache.spark.scheduler.Task.run(Task.scala:141)",
    "\tat org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:620)",
    "\tat java.lang.Thread.run(Thread.java:750)",
]


def errors_of(lines, rules, **kw):
    return ll.extract_errors(parse(lines, rules, **kw), rules)


def test_exception_grouping_basic(rules):
    lines = ["\tat orphan.Frame.before(AnyHeader.java:1)"] + OOM_BLOCK + [
        "Caused by: java.io.IOException: Failed to connect to /10.0.0.1:41234",
        "\tat org.apache.spark.network.client.TransportClientFactory.createClient(TransportClientFactory.java:298)",
        "26/10/06 18:07:30 INFO Executor: done",
    ]
    errs = errors_of(lines, rules, source="executor", executor_id="1", app_id="app-1",
                     file_path="executor/app-1/1/stderr")
    assert len(errs) == 2  # the orphan frame before the first header (grp 0) is dropped
    oom, io = sorted(errs, key=lambda e: e["seq"])
    assert oom["exception_class"] == "java.lang.OutOfMemoryError"
    assert oom["message"] == "Java heap space"
    assert oom["seq"] == 2  # header line (seq 0 = orphan frame, 1 = log4j line)
    assert to_ms(oom["ts"]) == ms(datetime(2026, 10, 6, 18, 7, 20))  # inherited ts
    assert list(oom["top_frames"]) == [f.strip() for f in OOM_BLOCK[2:7]]  # first 5, stripped
    assert oom["user_frame"] is None  # only framework frames
    assert oom["fingerprint"] == ref_fingerprint("java.lang.OutOfMemoryError", [f.strip() for f in OOM_BLOCK[2:5]])
    assert io["exception_class"] == "java.io.IOException"  # "Caused by: " prefix
    assert io["message"].startswith("Failed to connect")
    assert len(io["top_frames"]) == 1


def test_exception_header_with_log4j_prefix(rules):
    errs = errors_of(["26/10/06 18:07:53 ERROR Instrumentation: org.apache.spark.SparkException: Job aborted",
                      "\tat org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2985)"], rules)
    assert len(errs) == 1
    assert errs[0]["exception_class"] == "org.apache.spark.SparkException"
    assert errs[0]["message"] == "Job aborted"


def test_exception_empty_message_is_empty_string(rules):
    errs = errors_of(["pyspark.errors.exceptions.captured.PythonException: ",
                      '  File "/databricks/spark/python/pyspark/worker.py", line 1876, in main'], rules)
    assert errs[0]["exception_class"] == "pyspark.errors.exceptions.captured.PythonException"
    assert errs[0]["message"] == ""
    errs = errors_of(["java.lang.NullPointerException", "\tat com.acme.Foo.bar(Foo.java:10)"], rules)
    assert errs[0]["message"] == ""


def test_frame_line_is_never_a_header(rules):
    # frames whose text contains "Error" stay frames of the current group (they never start a new one)
    errs = errors_of(["java.lang.IllegalStateException: boom",
                      "\tat com.acme.SomeError(Foo.java:1)",
                      '  File "/x/MyError"'], rules)
    assert len(errs) == 1
    assert len(errs[0]["top_frames"]) == 2


def test_message_truncated_to_500(rules):
    errs = errors_of(["java.lang.RuntimeException: " + "m" * 900], rules)
    assert len(errs[0]["message"]) == 500


def test_fingerprint_stable_digits_and_hex_stripped():
    a = ["at java.util.Arrays.copyOf(Arrays.java:3236)", "at x.Y.z(Y.java:12)", "at 0x7f3a2b.q(Q.java:7)"]
    # NB: with the notebook's alternation order `\d+` wins over `0x[0-9a-f]+`, so only the digits of a hex
    # address are removed ("0x7f3a2b" -> "xfab"); addresses differing only in digits still collapse.
    b = ["at java.util.Arrays.copyOf(Arrays.java:3237)", "at x.Y.z(Y.java:99)", "at 0x1f9a8b.q(Q.java:8)"]
    assert ll.fingerprint("java.lang.OutOfMemoryError", a) == ll.fingerprint("java.lang.OutOfMemoryError", b)
    assert ll.fingerprint("java.lang.OutOfMemoryError", a) == ref_fingerprint("java.lang.OutOfMemoryError", a)
    assert len(ll.fingerprint("X", a)) == 12
    # only the top 3 frames count
    assert ll.fingerprint("X", a + ["at other.Frame(F.java:1)"]) == ll.fingerprint("X", a)
    # the class matters
    assert ll.fingerprint("X", a) != ll.fingerprint("Y", a)
    # no frames: hash of the class alone (Spark concat_ws of class + empty array)
    assert ll.fingerprint("ValueError", []) == hashlib.sha256(b"ValueError").hexdigest()[:12]


def test_fingerprint_same_for_repeated_oom_with_other_line_numbers(rules):
    second = [ln.replace("3236", "3237").replace("906", "912").replace("458", "459") for ln in OOM_BLOCK]
    errs = errors_of(OOM_BLOCK + second, rules)
    assert len(errs) == 2
    assert errs[0]["fingerprint"] == errs[1]["fingerprint"]


def test_user_frame_python(rules):
    lines = ["org.apache.spark.api.python.PythonException: Traceback (most recent call last):",
             '  File "/databricks/spark/python/pyspark/worker.py", line 1876, in main',
             "    process()",
             '  File "/Workspace/Users/me/etl/transform.py", line 42, in parse_amount',
             "    return int(x)",
             "ValueError: invalid literal for int() with base 10: 'n/a'"]
    errs = sorted(errors_of(lines, rules), key=lambda e: e["seq"])
    assert [e["exception_class"] for e in errs] == ["org.apache.spark.api.python.PythonException", "ValueError"]
    assert errs[0]["user_frame"] == 'File "/Workspace/Users/me/etl/transform.py", line 42, in parse_amount'
    assert errs[1]["user_frame"] is None


def test_user_frame_scans_beyond_top5(rules):
    frames = [f"\tat org.apache.spark.Frame{i}.run(Frame.scala:{i})" for i in range(7)]
    errs = errors_of(["org.apache.spark.SparkException: x"] + frames + ["\tat com.acme.etl.Job.main(Job.scala:42)"],
                     rules)
    assert len(errs[0]["top_frames"]) == 5
    assert errs[0]["user_frame"] == "at com.acme.etl.Job.main(Job.scala:42)"


def test_exceptions_grouped_per_file(rules):
    # extract_errors on rows from two files: a header in file 1 must not capture frames from file 2
    r1 = parse(["java.lang.IllegalStateException: one"], rules, file_path="driver/stderr", start_seq=0)
    r2 = parse(["\tat com.acme.X.y(X.java:1)", "java.lang.IllegalArgumentException: two",
                "\tat com.acme.Z.w(Z.java:2)"], rules, file_path="driver/stacktrace.log", start_seq=1)
    errs = ll.extract_errors(r1 + r2, rules)
    by_cls = {e["exception_class"]: e for e in errs}
    assert len(by_cls["java.lang.IllegalStateException"]["top_frames"]) == 0
    assert list(by_cls["java.lang.IllegalArgumentException"]["top_frames"]) == ["at com.acme.Z.w(Z.java:2)"]
