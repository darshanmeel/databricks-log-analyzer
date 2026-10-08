"""Deterministic generator of synthetic Databricks cluster log folders.

Layout (exactly as Databricks cluster log delivery writes it):

    <root>/<cluster_id>/
      driver/     log4j-active.log, log4j-YYYY-MM-DD-HH.log.gz, stdout, stderr, stderr--<date>, stacktrace.log
      executor/   <app-id>/<executor-id>/stdout, stderr (+ stderr--<date>)
      eventlog/   <cluster_id>_<hash>/<spark_context_id>/eventlog (+ eventlog-<date>.gz rolled)

Clusters produced:

* ``MAIN``   (1006-120000-sample01): a failing job cluster with TWO spark contexts (cluster restart).
  Context A covers every problem scenario (skew, spill, tiny tasks, GC pressure, task retries,
  decommissioned executor, executor OOM, FetchFailed, failed SQL query with two AQE updates,
  PythonException with a user frame, stage listed by two jobs, malformed event line).
  Context B is a small healthy run whose stage/job IDs restart at 0 and whose SQL query 0 has
  the same plan as context A's query 0 (different expression ids) -> identical plan_hash.
* ``HEALTHY`` (1007-090000-healthy1): one small successful run, no problems.
* ``EMPTY``  (1008-120000-emptyab1): an empty cluster folder (what serverless "produces").
* ``REV3``   (1009-180000-rev3conn): CONTRACT Revision 3 shapes (see ``_rev3``): rolled names
  ``stdout--YYYY-MM-DD--HH-MM`` / ``stderr--...`` (driver), ``stderr--YYYY-MM-DD--HH.gz`` /
  ``stdout--...gz`` (executor), ``eventlog-YYYY-MM-DD--HH-MM.gz``, ``YYYY-MM-DD-HH.stacktrace.log.gz``,
  an ``init_scripts/`` folder (ignored), JVM unified GC logs (several ``Pause Full`` on executor 1),
  ISO-8601 driver stdout, a JVM thread dump right after an exception, log4j continuation lines,
  Spark Connect operations linked to Spark jobs via ``spark.job.tags``, clusterUsageTags, JSON
  ``Removed Reason`` values (autoscale / killed / termination) and a run where every job succeeds
  but tasks were retried (ExecutorLostFailure, ExceptionFailure, FetchFailed + stage retry).

Everything is deterministic: fixed timestamps, gzip mtime=0, fixed file mtimes.

Usage:  python tests/fixtures/make_fixtures.py [out_dir]   (default tests/fixtures/clusters)
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --------------------------------------------------------------------------------------------
# identifiers
# --------------------------------------------------------------------------------------------
MAIN = "1006-120000-sample01"
HEALTHY = "1007-090000-healthy1"
EMPTY = "1008-120000-emptyab1"
REV3 = "1009-180000-rev3conn"

CTX_A = "7834019912345678901"
CTX_B = "8102233445566778899"
CTX_H = "5550001112223334445"

APP_A = "app-20261006174812-0000"
APP_B = "app-20261006190015-0000"
APP_H = "app-20261007090012-0000"

HASH_A = f"{MAIN}_10_139_64_5"
HASH_B = f"{MAIN}_10_139_64_9"
HASH_H = f"{HEALTHY}_10_139_70_2"

SPARK_VERSION = "3.5.0"
SQL_UI = "org.apache.spark.sql.execution.ui."

T0A = datetime(2026, 10, 6, 17, 48, 12)   # context A application start (UTC, naive)
T0B = datetime(2026, 10, 6, 19, 0, 15)    # context B application start
T0H = datetime(2026, 10, 7, 9, 0, 12)     # healthy cluster application start

KiB, MiB, GiB = 1 << 10, 1 << 20, 1 << 30
FIXED_MTIME = 1791302400  # 2026-10-07 00:00:00 UTC; applied to every generated file

USER_FRAME = 'File "/Workspace/Users/me/etl/transform.py", line 42, in parse_amount'

HOSTS = {"0": "10.139.64.11", "1": "10.139.64.12", "2": "10.139.64.13", "3": "10.139.64.14"}
HOSTS_B = {"0": "10.139.64.21", "1": "10.139.64.22"}
HOSTS_H = {"0": "10.139.70.11", "1": "10.139.70.12"}

OOM_REASON = "Executor killed: Container killed for exceeding memory limits (exit code 137)"
DECOM_REASON = "Executor decommission: worker decommissioned because of spot instance preemption."
FETCH_FAILURE = "org.apache.spark.shuffle.FetchFailedException: Failed to connect to /10.139.64.12:41234"
PY_TRACE = (
    "Traceback (most recent call last):\n"
    '  File "/databricks/spark/python/pyspark/worker.py", line 1876, in main\n'
    "    process()\n"
    f"  {USER_FRAME}\n"
    "    return int(x)\n"
    "ValueError: invalid literal for int() with base 10: 'n/a'\n"
)
JOB_ABORT = (
    "Job aborted due to stage failure: Task 0 in stage 6.1 failed 4 times, most recent failure: "
    "Lost task 0.3 in stage 6.1 (TID 2072) (10.139.64.14 executor 3): "
    "org.apache.spark.api.python.PythonException: " + PY_TRACE
)

PLAN_Q0_A = """== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   Exchange hashpartitioning(customer_id#12, 200), ENSURE_REQUIREMENTS, [plan_id=45]
   +- *(1) Project [customer_id#12, amount#13]
      +- *(1) Filter isnotnull(customer_id#12)
         +- FileScan parquet sales.orders[customer_id#12,amount#13] Batched: true, PartitionFilters: [], ReadSchema: struct<customer_id:bigint,amount:string>
"""
PLAN_Q0_B = PLAN_Q0_A.replace("#12", "#57").replace("#13", "#58").replace("plan_id=45", "plan_id=101").replace("*(1)", "*(2)")

PLAN_Q3_INITIAL = """== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- InsertIntoHadoopFsRelationCommand dbfs:/user/hive/warehouse/sales.db/daily_totals, Overwrite
   +- SortMergeJoin [customer_id#101], [customer_id#140], Inner
      :- Sort [customer_id#101 ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(customer_id#101, 200), ENSURE_REQUIREMENTS, [plan_id=310]
      :     +- FileScan parquet sales.orders[customer_id#101,amount#102]
      +- Sort [customer_id#140 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(customer_id#140, 200), ENSURE_REQUIREMENTS, [plan_id=311]
            +- FileScan parquet sales.customers[customer_id#140,region#141]
"""
PLAN_Q3_AQE1 = """== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- InsertIntoHadoopFsRelationCommand dbfs:/user/hive/warehouse/sales.db/daily_totals, Overwrite
   +- SortMergeJoin [customer_id#101], [customer_id#140], Inner
      :- Sort [customer_id#101 ASC NULLS FIRST], false, 0
      :  +- ShuffleQueryStage 0
      :     +- Exchange hashpartitioning(customer_id#101, 200), ENSURE_REQUIREMENTS, [plan_id=330]
      +- Sort [customer_id#140 ASC NULLS FIRST], false, 0
         +- ShuffleQueryStage 1
            +- Exchange hashpartitioning(customer_id#140, 200), ENSURE_REQUIREMENTS, [plan_id=331]
"""
PLAN_Q3_FINAL = """== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   InsertIntoHadoopFsRelationCommand dbfs:/user/hive/warehouse/sales.db/daily_totals, Overwrite
   +- *(3) SortMergeJoin(skew=true) [customer_id#101], [customer_id#140], Inner
      :- AQEShuffleRead coalesced and skewed
      :  +- ShuffleQueryStage 0
      +- AQEShuffleRead coalesced and skewed
         +- ShuffleQueryStage 1
"""
PLAN_Q1_A = """== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[count(1)])
   +- FileScan json raw.events[] Batched: false, ReadSchema: struct<>
"""
PLAN_Q2_A = """== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- InMemoryTableScan [customer_id#77, total#78]
   +- InMemoryRelation [customer_id#77, total#78], StorageLevel(disk, memory, deserialized, 1 replicas)
"""
PLAN_H = """== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [id#0L]
   +- Range (0, 1000, step=1, splits=8)
"""


# --------------------------------------------------------------------------------------------
# Revision 7: sparkPlanInfo trees with SQL metrics (accumulator ids), updated per task
# --------------------------------------------------------------------------------------------
ACC_NAMES: dict[int, str] = {}


def pnode(name: str, simple: str | None = None, metrics: list[tuple[int, str, str]] = (), children: list = ()) -> dict:
    """sparkPlanInfo node; metrics are (accumulatorId, name, metricType)."""
    for aid, n, _ in metrics:
        ACC_NAMES[aid] = n
    return {"nodeName": name, "simpleString": simple or name, "children": list(children), "metadata": {},
            "metrics": [{"name": n, "accumulatorId": aid, "metricType": mt} for aid, n, mt in metrics]}


def accums(**updates: int) -> list[tuple[int, int]]:
    """accums(a1001=5) -> [(1001, 5)]; keys are 'a' + accumulator id."""
    return [(int(k[1:]), v) for k, v in updates.items()]


# Query 0 (context A): FileScan -> Filter -> Project in codegen stage 1 -> Exchange
Q0 = dict(scan_rows=1001, scan_files=1002, scan_size=1003, scan_time=1004, c2r_rows=1005, filter_rows=1006,
          cg1=1007, ex_bytes=1008, ex_records=1009, ex_time=1010, ex_size=1011)


def plan_q0() -> dict:
    scan = pnode("Scan parquet sales.orders", "FileScan parquet sales.orders[customer_id#12,amount#13]",
                 [(Q0["scan_rows"], "number of output rows", "sum"), (Q0["scan_files"], "number of files read", "sum"),
                  (Q0["scan_size"], "size of files read", "size"), (Q0["scan_time"], "scan time", "timing")])
    cg = pnode("WholeStageCodegen (1)", "WholeStageCodegen (1)", [(Q0["cg1"], "duration", "timing")], [
        pnode("Project", "Project [customer_id#12, amount#13]", [], [
            pnode("Filter", "Filter isnotnull(customer_id#12)", [(Q0["filter_rows"], "number of output rows", "sum")], [
                pnode("ColumnarToRow", "ColumnarToRow", [(Q0["c2r_rows"], "number of output rows", "sum")], [
                    pnode("InputAdapter", "InputAdapter", [], [scan])])])])])
    ex = pnode("Exchange", "Exchange hashpartitioning(customer_id#12, 200), ENSURE_REQUIREMENTS, [plan_id=45]",
               [(Q0["ex_bytes"], "shuffle bytes written", "size"), (Q0["ex_records"], "shuffle records written", "sum"),
                (Q0["ex_time"], "shuffle write time", "nsTiming"), (Q0["ex_size"], "data size", "size")], [cg])
    return pnode("AdaptiveSparkPlan", "AdaptiveSparkPlan isFinalPlan=true", [], [ex])


def q0_task_accums(run_ms: int, records: int, size: int, shuffle_write: int) -> list[tuple[int, int]]:
    kept = records * 97 // 100
    return [(Q0["scan_rows"], records), (Q0["scan_files"], 1), (Q0["scan_size"], size),
            (Q0["scan_time"], run_ms * 4 // 10), (Q0["c2r_rows"], records), (Q0["filter_rows"], kept),
            (Q0["cg1"], run_ms * 9 // 10), (Q0["ex_bytes"], shuffle_write), (Q0["ex_records"], kept),
            (Q0["ex_time"], run_ms * 50_000), (Q0["ex_size"], shuffle_write * 8 // 5)]


# Query 3 (context A): two scans -> Exchange -> (AQE) ShuffleQueryStage -> AQEShuffleRead -> SortMergeJoin -> insert
Q3 = dict(o_rows=3001, o_size=3002, o_time=3003, o_ex_bytes=3004, o_ex_rec=3005, o_ex_time=3006,
          c_rows=3011, c_size=3012, c_time=3013, c_ex_bytes=3014, c_ex_rec=3015, c_ex_time=3016,
          o_read_parts=3021, o_read_skewed=3022, c_read_parts=3023, smj_rows=3031, smj_spill=3032, smj_peak=3033,
          cg3=3034, ins_files=3041, ins_bytes=3042, ins_rows=3043)


def _q3_branch(side: str, table: str, final: bool) -> dict:
    k = side
    scan = pnode(f"Scan parquet sales.{table}", f"FileScan parquet sales.{table}",
                 [(Q3[f"{k}_rows"], "number of output rows", "sum"), (Q3[f"{k}_size"], "size of files read", "size"),
                  (Q3[f"{k}_time"], "scan time", "timing")])
    ex = pnode("Exchange", f"Exchange hashpartitioning(customer_id, 200), ENSURE_REQUIREMENTS",
               [(Q3[f"{k}_ex_bytes"], "shuffle bytes written", "size"), (Q3[f"{k}_ex_rec"], "shuffle records written", "sum"),
                (Q3[f"{k}_ex_time"], "shuffle write time", "nsTiming")], [scan])
    if not final:
        return pnode("Sort", "Sort [customer_id ASC NULLS FIRST], false, 0", [], [ex])
    stage = pnode("ShuffleQueryStage", f"ShuffleQueryStage {0 if k == 'o' else 1}", [], [ex])
    read_metrics = [(Q3[f"{k}_read_parts"], "number of partitions", "sum")]
    if k == "o":
        read_metrics.append((Q3["o_read_skewed"], "number of skewed partitions", "sum"))
    return pnode("InputAdapter", "InputAdapter", [], [pnode("AQEShuffleRead", "AQEShuffleRead coalesced and skewed", read_metrics, [stage])])


def plan_q3(final: bool) -> dict:
    smj = pnode("SortMergeJoin", "SortMergeJoin(skew=true) [customer_id], [customer_id], Inner" if final else
                "SortMergeJoin [customer_id], [customer_id], Inner",
                [(Q3["smj_rows"], "number of output rows", "sum"), (Q3["smj_spill"], "spill size", "size"),
                 (Q3["smj_peak"], "peak memory", "size")],
                [_q3_branch("o", "orders", final), _q3_branch("c", "customers", final)])
    body = pnode("WholeStageCodegen (3)", "WholeStageCodegen (3)", [(Q3["cg3"], "duration", "timing")], [smj]) if final else smj
    ins = pnode("Execute InsertIntoHadoopFsRelationCommand",
                "Execute InsertIntoHadoopFsRelationCommand dbfs:/user/hive/warehouse/sales.db/daily_totals, Overwrite",
                [(Q3["ins_files"], "number of written files", "sum"), (Q3["ins_bytes"], "written output", "size"),
                 (Q3["ins_rows"], "number of output rows", "sum")], [body])
    return pnode("AdaptiveSparkPlan", f"AdaptiveSparkPlan isFinalPlan={'true' if final else 'false'}", [], [ins])


# Populated by generate(): expected values the tests assert on.
EXPECTED: dict = {}


# --------------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------------
def epoch_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def log4j_ts(dt: datetime) -> str:
    return dt.strftime("%y/%m/%d %H:%M:%S")


class LogFile:
    """Collects lines of one log file (driver or executor)."""

    def __init__(self, t0: datetime):
        self.t0 = t0
        self.lines: list[str] = []

    def log(self, sec: float, level: str, logger: str, msg: str) -> "LogFile":
        self.lines.append(f"{log4j_ts(self.t0 + timedelta(seconds=sec))} {level} {logger}: {msg}")
        return self

    def raw(self, *lines: str) -> "LogFile":
        for block in lines:
            self.lines.extend(block.split("\n"))
        return self

    def text(self) -> str:
        return "\n".join(self.lines) + "\n"


class EventLog:
    """Collects Spark event JSON lines. Every third event is written with Python's default
    spacing (`"Event": "..."`), the rest compact like Spark (`"Event":"..."`)."""

    def __init__(self):
        self.lines: list[str] = []
        self.n = 0

    def add(self, ev: dict) -> None:
        self.n += 1
        if self.n % 3 == 0:
            self.lines.append(json.dumps(ev))
        else:
            self.lines.append(json.dumps(ev, separators=(",", ":")))

    def raw(self, line: str) -> None:
        self.lines.append(line)

    def text(self) -> str:
        return "\n".join(self.lines) + "\n"


def _write(path: Path, data: str | bytes, *, gz: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = data.encode("utf-8") if isinstance(data, str) else data
    if gz:
        raw = gzip.compress(raw, mtime=0)
    path.write_bytes(raw)
    os.utime(path, (FIXED_MTIME, FIXED_MTIME))


# --------------------------------------------------------------------------------------------
# Spark event builders (field names exactly as Spark's JsonProtocol writes them)
# --------------------------------------------------------------------------------------------
def ev_log_start() -> dict:
    return {"Event": "SparkListenerLogStart", "Spark Version": SPARK_VERSION}


def ev_app_start(app_id: str, ts: int) -> dict:
    return {"Event": "SparkListenerApplicationStart", "App Name": "Databricks Shell", "App ID": app_id,
            "Timestamp": ts, "User": "root"}


def ev_app_end(ts: int) -> dict:
    return {"Event": "SparkListenerApplicationEnd", "Timestamp": ts}


def ev_env_update() -> dict:
    return {"Event": "SparkListenerEnvironmentUpdate",
            "JVM Information": {"Java Version": "1.8.0_412 (Azul Systems, Inc.)", "Scala Version": "version 2.12.15"},
            "Spark Properties": {"spark.databricks.clusterUsageTags.clusterId": MAIN,
                                 "spark.executor.memory": "8g", "spark.sql.adaptive.enabled": "true"},
            "Hadoop Properties": {}, "System Properties": {}, "Classpath Entries": {}}


def ev_resource_profile() -> dict:
    return {"Event": "SparkListenerResourceProfileAdded", "Resource Profile Id": 0,
            "Executor Resource Requests": {"cores": {"Resource Name": "cores", "Amount": 4,
                                                     "Discovery Script": "", "Vendor": ""}},
            "Task Resource Requests": {"cpus": {"Resource Name": "cpus", "Amount": 1.0}}}


def ev_block_manager_added(executor_id: str, host: str, ts: int) -> dict:
    return {"Event": "SparkListenerBlockManagerAdded",
            "Block Manager ID": {"Executor ID": executor_id, "Host": host, "Port": 41234},
            "Maximum Memory": 4 * GiB, "Timestamp": ts, "Maximum Onheap Memory": 4 * GiB,
            "Maximum Offheap Memory": 0}


def ev_executor_added(executor_id: str, host: str, ts: int, cores: int = 4) -> dict:
    return {"Event": "SparkListenerExecutorAdded", "Timestamp": ts, "Executor ID": executor_id,
            "Executor Info": {"Host": host, "Total Cores": cores,
                              "Log Urls": {"stdout": f"http://{host}:40001/logPage/?appId=x&executorId={executor_id}&logType=stdout",
                                           "stderr": f"http://{host}:40001/logPage/?appId=x&executorId={executor_id}&logType=stderr"},
                              "Attributes": {}, "Resources": {}, "Resource Profile Id": 0,
                              "Registration Time": ts, "Request Time": ts - 1000}}


def ev_executor_removed(executor_id: str, ts: int, reason: str) -> dict:
    return {"Event": "SparkListenerExecutorRemoved", "Timestamp": ts, "Executor ID": executor_id,
            "Removed Reason": reason}


def job_properties(sql_execution_id: int | None, description: str, *, databricks: bool = True) -> dict:
    p = {"spark.job.interruptOnCancel": "true",
         "spark.rdd.scope": '{"id":"12","name":"Exchange"}',
         "callSite.short": "saveAsTable at NativeMethodAccessorImpl.java:0",
         "callSite.long": "org.apache.spark.sql.DataFrameWriter.saveAsTable(DataFrameWriter.scala:731)",
         "spark.job.description": description,
         "spark.jobGroup.id": "1006-120000-sample01_job-918273645-run-5550123-action-7766",
         "user": "me@example.com"}
    if databricks:
        p.update({"spark.databricks.job.id": "918273645", "spark.databricks.job.runId": "5550123",
                  "spark.databricks.notebook.path": "/Workspace/Users/me/etl/main"})
    if sql_execution_id is not None:
        p["spark.sql.execution.id"] = str(sql_execution_id)
        p["spark.sql.execution.root.id"] = str(sql_execution_id)
    return p


# map stage -> the stages it reads (Stage Info "Parent IDs"): 0 -> 1 and 5 -> 6 in every context
STAGE_PARENTS = {1: [0], 6: [5]}


def stage_info(stage_id: int, attempt: int, name: str, num_tasks: int, *, submit: int | None = None,
               complete: int | None = None, failure: str | None = None) -> dict:
    d = {"Stage ID": stage_id, "Stage Attempt ID": attempt, "Stage Name": name, "Number of Tasks": num_tasks,
         "RDD Info": [{"RDD ID": stage_id * 3, "Name": "MapPartitionsRDD", "Scope": '{"id":"3","name":"WholeStageCodegen (1)"}',
                       "Callsite": name, "Parent IDs": [], "Storage Level": {"Use Disk": False, "Use Memory": False,
                       "Deserialized": False, "Replication": 1}, "Barrier": False, "DeterministicLevel": "DETERMINATE",
                       "Number of Partitions": num_tasks, "Number of Cached Partitions": 0, "Memory Size": 0, "Disk Size": 0}],
         "Parent IDs": STAGE_PARENTS.get(stage_id, []), "Details": f"org.apache.spark.sql.Dataset.{name}",
         "Accumulables": [], "Resource Profile Id": 0, "Shuffle Push Enabled": False, "Shuffle Push Mergers Count": 0}
    if submit is not None:
        d["Submission Time"] = submit
    if complete is not None:
        d["Completion Time"] = complete
    if failure is not None:
        d["Failure Reason"] = failure
    return d


def ev_job_start(job_id: int, ts: int, stage_ids: list[int], props: dict, infos: list[dict]) -> dict:
    return {"Event": "SparkListenerJobStart", "Job ID": job_id, "Submission Time": ts,
            "Stage Infos": infos, "Stage IDs": stage_ids, "Properties": props}


def ev_job_end(job_id: int, ts: int, error: str | None = None) -> dict:
    if error is None:
        result = {"Result": "JobSucceeded"}
    else:
        result = {"Result": "JobFailed", "Exception": {"Message": error, "Stack Trace": [
            {"Declaring Class": "org.apache.spark.scheduler.DAGScheduler", "Method Name": "failJobAndIndependentStages",
             "File Name": "DAGScheduler.scala", "Line Number": 3051}]}}
    return {"Event": "SparkListenerJobEnd", "Job ID": job_id, "Completion Time": ts, "Job Result": result}


def ev_stage_submitted(info: dict, props: dict) -> dict:
    return {"Event": "SparkListenerStageSubmitted", "Stage Info": info, "Properties": props}


def ev_stage_completed(info: dict) -> dict:
    return {"Event": "SparkListenerStageCompleted", "Stage Info": info}


def ev_task_start(stage_id: int, attempt: int, task_id: int, index: int, task_attempt: int,
                  executor_id: str, host: str, launch: int) -> dict:
    return {"Event": "SparkListenerTaskStart", "Stage ID": stage_id, "Stage Attempt ID": attempt,
            "Task Info": {"Task ID": task_id, "Index": index, "Attempt": task_attempt, "Partition ID": index,
                          "Launch Time": launch, "Executor ID": executor_id, "Host": host,
                          "Locality": "PROCESS_LOCAL", "Speculative": False, "Getting Result Time": 0,
                          "Finish Time": 0, "Failed": False, "Killed": False, "Accumulables": []}}


def ev_task_end(stage_id: int, attempt: int, task_id: int, index: int, task_attempt: int, executor_id: str,
                host: str, launch: int, finish: int, *, run_ms: int | None = None, gc_ms: int = 0, peak: int = 0,
                mem_spill: int = 0, disk_spill: int = 0, input_bytes: int = 0, input_records: int = 0,
                output_bytes: int = 0, shuffle_remote: int = 0, shuffle_local: int = 0, shuffle_write: int = 0,
                reason: dict | None = None, compact: bool = False, task_type: str = "ResultTask",
                accum: list[tuple[int, int]] | None = None) -> dict:
    reason = reason or {"Reason": "Success"}
    failed = reason["Reason"] != "Success"
    if run_ms is None:
        run_ms = max(finish - launch - 50, 0)
    info = {"Task ID": task_id, "Index": index, "Attempt": task_attempt, "Partition ID": index,
            "Launch Time": launch, "Executor ID": executor_id, "Host": host, "Locality": "PROCESS_LOCAL",
            "Speculative": False, "Getting Result Time": 0, "Finish Time": finish, "Failed": failed,
            "Killed": reason["Reason"] == "TaskKilled",
            "Accumulables": [{"ID": aid, "Name": ACC_NAMES.get(aid, f"metric {aid}"), "Update": str(v), "Value": str(v),
                              "Internal": False, "Count Failed Values": False, "Metadata": "sql"}
                             for aid, v in (accum or [])]}
    if compact:
        metrics = {"Executor Run Time": run_ms, "JVM GC Time": gc_ms,
                   "Input Metrics": {"Bytes Read": input_bytes, "Records Read": input_records}}
    else:
        metrics = {
            "Executor Deserialize Time": 12, "Executor Deserialize CPU Time": 9_000_000,
            "Executor Run Time": run_ms, "Executor CPU Time": run_ms * (450_000 if shuffle_remote else 900_000),
            "Peak Execution Memory": peak, "Result Size": 2_345, "JVM GC Time": gc_ms,
            "Result Serialization Time": 1, "Memory Bytes Spilled": mem_spill, "Disk Bytes Spilled": disk_spill,
            "Shuffle Read Metrics": {"Remote Blocks Fetched": 8 if shuffle_remote else 0,
                                     "Local Blocks Fetched": 2 if shuffle_local else 0,
                                     "Fetch Wait Time": run_ms * 3 // 10 if shuffle_remote else 3,
                                     "Remote Bytes Read": shuffle_remote, "Remote Bytes Read To Disk": 0,
                                     "Local Bytes Read": shuffle_local, "Total Records Read": 1000,
                                     "Remote Requests Duration": 5, "Push Based Shuffle": {}},
            "Shuffle Write Metrics": {"Shuffle Bytes Written": shuffle_write, "Shuffle Write Time": 1_000_000,
                                      "Shuffle Records Written": 1000 if shuffle_write else 0},
            "Input Metrics": {"Bytes Read": input_bytes, "Records Read": input_records},
            "Output Metrics": {"Bytes Written": output_bytes, "Records Written": 10 if output_bytes else 0},
            "Updated Blocks": []}
    return {"Event": "SparkListenerTaskEnd", "Stage ID": stage_id, "Stage Attempt ID": attempt,
            "Task Type": task_type, "Task End Reason": reason, "Task Info": info,
            "Task Executor Metrics": {"JVMHeapMemory": peak, "OnHeapExecutionMemory": peak},
            "Task Metrics": metrics}


def ev_sql_start(exec_id: int, ts: int, description: str, plan: str, details: str, info: dict | None = None) -> dict:
    return {"Event": SQL_UI + "SparkListenerSQLExecutionStart", "executionId": exec_id, "rootExecutionId": exec_id,
            "description": description, "details": details, "physicalPlanDescription": plan,
            "sparkPlanInfo": info or {"nodeName": "AdaptiveSparkPlan", "simpleString": "AdaptiveSparkPlan isFinalPlan=false",
                                      "children": [], "metadata": {}, "metrics": []},
            "time": ts, "modifiedConfigs": {}, "jobTags": []}


def ev_sql_aqe(exec_id: int, plan: str, info: dict | None = None) -> dict:
    return {"Event": SQL_UI + "SparkListenerSQLAdaptiveExecutionUpdate", "executionId": exec_id,
            "physicalPlanDescription": plan,
            "sparkPlanInfo": info or {"nodeName": "AdaptiveSparkPlan", "simpleString": "AdaptiveSparkPlan",
                                      "children": [], "metadata": {}, "metrics": []}}


def ev_sql_end(exec_id: int, ts: int, error: str | None = None) -> dict:
    d = {"Event": SQL_UI + "SparkListenerSQLExecutionEnd", "executionId": exec_id, "time": ts}
    if error is not None:
        d["errorMessage"] = error
    return d


def ev_driver_accum(exec_id: int) -> dict:
    return {"Event": SQL_UI + "SparkListenerDriverAccumUpdates", "executionId": exec_id,
            "accumUpdates": [[101, 3], [102, 0]]}


def ex_failure(class_name: str, description: str, frames: list[tuple[str, str, str, int]]) -> dict:
    return {"Reason": "ExceptionFailure", "Class Name": class_name, "Description": description,
            "Stack Trace": [{"Declaring Class": c, "Method Name": m, "File Name": f, "Line Number": n}
                            for c, m, f, n in frames],
            "Full Stack Trace": f"{class_name}: {description}\n" + "\n".join(
                f"\tat {c}.{m}({f}:{n})" for c, m, f, n in frames),
            "Accumulator Updates": []}


OOM_FRAMES = [("java.util.Arrays", "copyOf", "Arrays.java", 3236),
              ("org.apache.spark.unsafe.map.BytesToBytesMap", "growAndRehash", "BytesToBytesMap.java", 906),
              ("org.apache.spark.scheduler.Task", "run", "Task.scala", 141)]
PY_FRAMES = [("org.apache.spark.api.python.BasePythonRunner$ReaderIterator", "handlePythonException",
              "PythonRunner.scala", 694)]
IO_FRAMES = [("org.apache.spark.network.client.TransportClient", "sendRpc", "TransportClient.java", 318)]


# --------------------------------------------------------------------------------------------
# main cluster: event logs
# --------------------------------------------------------------------------------------------
def _context_a_events() -> tuple[str, str, dict]:
    """Returns (rolled_text, active_text, stats). The rolled file holds everything up to the end
    of job 1 (incl. the 2000 tiny tasks), the active file the rest."""
    t = lambda sec: epoch_ms(T0A) + int(round(sec * 1000))  # noqa: E731
    ev = EventLog()
    stats = {"tasks": 0, "failed_tasks": 0}
    tid = [0]

    def task(stage, att, index, tatt, ex, launch, finish, **kw):
        tt = tid[0]
        tid[0] += 1
        stats["tasks"] += 1
        if kw.get("reason") and kw["reason"]["Reason"] != "Success":
            stats["failed_tasks"] += 1
        if not kw.get("compact"):
            ev.add(ev_task_start(stage, att, tt, index, tatt, ex, HOSTS[ex], launch))
        ev.add(ev_task_end(stage, att, tt, index, tatt, ex, HOSTS[ex], launch, finish, **kw))
        return tt

    ev.add(ev_log_start())
    ev.add(ev_resource_profile())
    ev.add(ev_block_manager_added("driver", "10.139.64.5", t(0)))
    ev.add(ev_env_update())
    ev.add(ev_app_start(APP_A, t(0)))
    for i, ex in enumerate(["0", "1", "2", "3"]):
        ev.add(ev_executor_added(ex, HOSTS[ex], t(5 + i)))
        ev.add(ev_block_manager_added(ex, HOSTS[ex], t(5 + i)))

    details = ("org.apache.spark.sql.DataFrameWriter.saveAsTable(DataFrameWriter.scala:731)\n"
               "sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n"
               "py4j.commands.CallCommand.execute(CallCommand.java:79)\n"
               "/Workspace/Users/me/etl/main.py:18")

    # ---- job 0 / sql 0: stage 0 (skew) + stage 1 (disk spill)
    ev.add(ev_sql_start(0, t(19), "saveAsTable at main.py:18", PLAN_Q0_A, details, plan_q0()))
    s0 = stage_info(0, 0, "mapPartitions at transform.py:42", 20)
    s1 = stage_info(1, 0, "save at main.py:18", 4)
    p0 = job_properties(0, "Write orders")
    ev.add(ev_job_start(0, t(20), [0, 1], p0, [s0, s1]))
    ev.add(ev_stage_submitted(stage_info(0, 0, s0["Stage Name"], 20, submit=t(20)), p0))
    for i in range(20):
        ex = str(i % 4)
        if i == 0:
            launch, finish = t(21), t(921)          # the 900 s straggler
        else:
            launch = t(21 + (i // 16) * 6)
            finish = launch + 5000                  # 5 s tasks
        dur = finish - launch
        task(0, 0, i, 0, ex, launch, finish, run_ms=dur - 50, gc_ms=(dur - 50) // 100, peak=256 * MiB,
             input_bytes=128 * MiB, input_records=1_000_000, shuffle_write=64 * MiB, task_type="ShuffleMapTask",
             accum=q0_task_accums(dur - 50, 1_000_000, 128 * MiB, 64 * MiB))
    ev.add(ev_stage_completed(stage_info(0, 0, s0["Stage Name"], 20, submit=t(20), complete=t(922))))
    ev.add(ev_stage_submitted(stage_info(1, 0, s1["Stage Name"], 4, submit=t(925)), p0))
    for i in range(4):
        ex = str(i)
        task(1, 0, i, 0, ex, t(926), t(956), run_ms=29_900, gc_ms=600, peak=3 * GiB, mem_spill=2 * GiB,
             disk_spill=768 * MiB, shuffle_remote=48 * MiB, shuffle_local=16 * MiB, output_bytes=200 * MiB)
    ev.add(ev_stage_completed(stage_info(1, 0, s1["Stage Name"], 4, submit=t(925), complete=t(958))))
    ev.add(ev_job_end(0, t(960)))
    ev.add(ev_driver_accum(0))
    ev.add(ev_sql_end(0, t(961)))

    # ---- job 1 / sql 1: lists stage 1 again (skipped) + stage 2 (2000 tiny tasks)
    ev.add(ev_sql_start(1, t(963), "count at main.py:25", PLAN_Q1_A, "/Workspace/Users/me/etl/main.py:25"))
    s2 = stage_info(2, 0, "count at main.py:25", 2000)
    p1 = job_properties(1, "Count raw events")
    ev.add(ev_job_start(1, t(964), [1, 2], p1, [s1, s2]))
    ev.add(ev_stage_submitted(stage_info(2, 0, s2["Stage Name"], 2000, submit=t(965)), p1))
    for i in range(2000):
        ex = str(i % 4)
        launch = t(966) + (i // 16) * 60
        dur = 45 + (i % 10)
        task(2, 0, i, 0, ex, launch, launch + dur, run_ms=dur - 5, gc_ms=0, input_bytes=4 * KiB,
             input_records=10, compact=True)
    ev.add(ev_stage_completed(stage_info(2, 0, s2["Stage Name"], 2000, submit=t(965), complete=t(975))))
    ev.add(ev_job_end(1, t(976)))
    ev.add(ev_sql_end(1, t(977)))
    rolled = ev.text()
    ev.lines = []

    # ---- job 2 / sql 2: stage 3 (GC pressure, 30 %)
    ev.add(ev_sql_start(2, t(980), "cache at main.py:31", PLAN_Q2_A, "/Workspace/Users/me/etl/main.py:31"))
    s3 = stage_info(3, 0, "cache at main.py:31", 8)
    p2 = job_properties(2, "Cache totals")
    ev.add(ev_job_start(2, t(981), [3], p2, [s3]))
    ev.add(ev_stage_submitted(stage_info(3, 0, s3["Stage Name"], 8, submit=t(982)), p2))
    for i in range(8):
        task(3, 0, i, 0, str(i % 4), t(983), t(993), run_ms=9_800, gc_ms=2_940, peak=int(1.5 * GiB),
             input_bytes=64 * MiB, input_records=500_000)
    ev.add(ev_stage_completed(stage_info(3, 0, s3["Stage Name"], 8, submit=t(982), complete=t(994))))
    ev.add(ev_job_end(2, t(995)))
    ev.add(ev_sql_end(2, t(996)))

    # ---- job 3 (no SQL): stage 4 with two failed task attempts that were retried (stage succeeds)
    s4 = stage_info(4, 0, "foreachPartition at main.py:40", 8)
    p3 = job_properties(None, "Validate partitions")
    ev.add(ev_job_start(3, t(1000), [4], p3, [s4]))
    ev.add(ev_stage_submitted(stage_info(4, 0, s4["Stage Name"], 8, submit=t(1001)), p3))
    for i in range(8):
        if i == 2:  # runs on executor 2 which is decommissioned (spot preemption)
            task(4, 0, 2, 0, "2", t(1002), t(1041), run_ms=38_000, gc_ms=300,
                 reason={"Reason": "ExecutorLostFailure", "Executor ID": "2", "Exit Caused By App": False,
                         "Loss Reason": DECOM_REASON})
        elif i == 6:  # transient IO error on executor 3
            task(4, 0, 6, 0, "3", t(1002), t(1003), run_ms=900, gc_ms=10,
                 reason=ex_failure("java.io.IOException", "Connection reset by peer", IO_FRAMES))
            task(4, 0, 6, 1, "1", t(1010), t(1015), run_ms=4_900, gc_ms=50)
        else:
            ex = {0: "0", 1: "1", 3: "3", 4: "0", 5: "1", 7: "3"}[i]
            task(4, 0, i, 0, ex, t(1002), t(1007), run_ms=4_900, gc_ms=50)
    ev.add(ev_executor_removed("2", t(1041), DECOM_REASON))
    task(4, 0, 2, 1, "0", t(1042), t(1047), run_ms=4_900, gc_ms=50)
    ev.add(ev_stage_completed(stage_info(4, 0, s4["Stage Name"], 8, submit=t(1001), complete=t(1048))))
    ev.add(ev_job_end(3, t(1049)))

    # ---- job 4 / sql 3: fails. stage 5 (map), stage 6.0 OOM + FetchFailed, 5.1 recompute, 6.1 PythonException
    ev.add(ev_sql_start(3, t(1095), "INSERT OVERWRITE sales.daily_totals", PLAN_Q3_INITIAL,
                        "org.apache.spark.sql.DataFrameWriter.saveAsTable(DataFrameWriter.scala:731)\n"
                        "/Workspace/Users/me/etl/main.py:57", plan_q3(final=False)))
    ev.add(ev_sql_aqe(3, PLAN_Q3_AQE1))
    s5 = stage_info(5, 0, "saveAsTable at main.py:57 (map)", 4)
    s6 = stage_info(6, 0, "saveAsTable at main.py:57", 4)
    p4 = job_properties(3, "Write daily totals")
    ev.add(ev_job_start(4, t(1100), [5, 6], p4, [s5, s6]))
    ev.add(ev_stage_submitted(stage_info(5, 0, s5["Stage Name"], 4, submit=t(1100)), p4))
    for i, ex in enumerate(["0", "1", "3", "0"]):
        side = "o" if i < 3 else "c"  # three orders splits, one customers split
        task(5, 0, i, 0, ex, t(1101), t(1116), run_ms=14_900, gc_ms=150, shuffle_write=256 * MiB,
             input_bytes=256 * MiB, input_records=2_000_000, task_type="ShuffleMapTask",
             accum=[(Q3[f"{side}_rows"], 2_000_000), (Q3[f"{side}_size"], 256 * MiB), (Q3[f"{side}_time"], 6_000),
                    (Q3[f"{side}_ex_bytes"], 256 * MiB), (Q3[f"{side}_ex_rec"], 2_000_000),
                    (Q3[f"{side}_ex_time"], 900_000_000)])
    ev.add(ev_stage_completed(stage_info(5, 0, s5["Stage Name"], 4, submit=t(1100), complete=t(1118))))
    # malformed (truncated) event line: must be counted and skipped
    ev.raw('{"Event":"SparkListenerTaskEnd","Stage ID":6,"Stage Attempt ID":0,"Task Info":{"Task ID":99')
    ev.add(ev_stage_submitted(stage_info(6, 0, s6["Stage Name"], 4, submit=t(1119)), p4))
    task(6, 0, 0, 0, "1", t(1120), t(1150), run_ms=29_900, gc_ms=1_000, peak=7 * GiB,
         reason=ex_failure("java.lang.OutOfMemoryError", "Java heap space", OOM_FRAMES))
    ev.add(ev_sql_aqe(3, PLAN_Q3_FINAL, plan_q3(final=True)))  # second (last) AQE update = final plan
    ev.add({"Event": SQL_UI + "SparkListenerDriverAccumUpdates", "executionId": 3,
            "accumUpdates": [[Q3["o_read_parts"], 3], [Q3["o_read_skewed"], 1], [Q3["c_read_parts"], 1]]})
    ev.add(ev_executor_removed("1", t(1151), OOM_REASON))
    fetch_reason = {"Reason": "FetchFailed",
                    "Block Manager Address": {"Executor ID": "1", "Host": HOSTS["1"], "Port": 41234},
                    "Shuffle ID": 2, "Map ID": 2046, "Map Index": 1, "Reduce ID": 1, "Message": FETCH_FAILURE}
    task(6, 0, 1, 0, "3", t(1151), t(1152), run_ms=900, gc_ms=5, reason=dict(fetch_reason))
    task(6, 0, 2, 0, "0", t(1151), t(1152), run_ms=900, gc_ms=5, reason=dict(fetch_reason, **{"Reduce ID": 2}))
    ev.add(ev_stage_completed(stage_info(6, 0, s6["Stage Name"], 4, submit=t(1119), complete=t(1153),
                                         failure=FETCH_FAILURE)))
    ev.add(ev_stage_submitted(stage_info(5, 1, s5["Stage Name"], 4, submit=t(1154)), p4))
    task(5, 1, 1, 0, "0", t(1155), t(1165), run_ms=9_900, gc_ms=100, shuffle_write=256 * MiB,
         input_bytes=256 * MiB, input_records=2_000_000, task_type="ShuffleMapTask",
         accum=[(Q3["o_rows"], 2_000_000), (Q3["o_size"], 256 * MiB), (Q3["o_time"], 4_000),
                (Q3["o_ex_bytes"], 256 * MiB), (Q3["o_ex_rec"], 2_000_000), (Q3["o_ex_time"], 600_000_000)])
    ev.add(ev_stage_completed(stage_info(5, 1, s5["Stage Name"], 4, submit=t(1154), complete=t(1166))))
    ev.add(ev_stage_submitted(stage_info(6, 1, s6["Stage Name"], 4, submit=t(1167)), p4))
    for a in range(4):
        task(6, 1, 0, a, "3", t(1168 + 3 * a), t(1170 + 3 * a), run_ms=1_900, gc_ms=20,
             reason=ex_failure("org.apache.spark.api.python.PythonException", PY_TRACE, PY_FRAMES))
    for i in (1, 2, 3):
        task(6, 1, i, 0, "0", t(1168), t(1180), run_ms=11_000, gc_ms=100,
             reason={"Reason": "TaskKilled", "Kill Reason": "Stage cancelled: job 4 aborted", "Accumulator Updates": []})
    ev.add(ev_stage_completed(stage_info(6, 1, s6["Stage Name"], 4, submit=t(1167), complete=t(1180),
                                         failure=JOB_ABORT)))
    ev.add(ev_job_end(4, t(1181), error=JOB_ABORT))
    ev.add(ev_sql_end(3, t(1182), error=JOB_ABORT))
    # no ApplicationEnd: the driver restarted (context B)
    return rolled, ev.text(), stats


def _context_b_events() -> tuple[str, dict]:
    t = lambda sec: epoch_ms(T0B) + int(round(sec * 1000))  # noqa: E731
    ev = EventLog()
    stats = {"tasks": 0}
    tid = [0]

    def task(stage, index, ex, launch, finish, **kw):
        tt = tid[0]
        tid[0] += 1
        stats["tasks"] += 1
        ev.add(ev_task_end(stage, 0, tt, index, 0, ex, HOSTS_B[ex], launch, finish, **kw))

    ev.add(ev_log_start())
    ev.add(ev_env_update())
    ev.add(ev_app_start(APP_B, t(0)))
    for i, ex in enumerate(["0", "1"]):
        ev.add(ev_executor_added(ex, HOSTS_B[ex], t(5 + i)))
    ev.add(ev_sql_start(0, t(10), "saveAsTable at main.py:18", PLAN_Q0_B, "/Workspace/Users/me/etl/main.py:18"))
    s0 = stage_info(0, 0, "mapPartitions at transform.py:42", 4)
    s1 = stage_info(1, 0, "save at main.py:18", 4)
    p0 = job_properties(0, "Write orders")
    ev.add(ev_job_start(0, t(11), [0, 1], p0, [s0, s1]))
    ev.add(ev_stage_submitted(stage_info(0, 0, s0["Stage Name"], 4, submit=t(11)), p0))
    for i in range(4):
        task(0, i, str(i % 2), t(12), t(14), gc_ms=20, input_bytes=32 * MiB, input_records=100_000,
             shuffle_write=8 * MiB)
    ev.add(ev_stage_completed(stage_info(0, 0, s0["Stage Name"], 4, submit=t(11), complete=t(15))))
    ev.add(ev_stage_submitted(stage_info(1, 0, s1["Stage Name"], 4, submit=t(16)), p0))
    for i in range(4):
        task(1, i, str(i % 2), t(17), t(19), gc_ms=20, shuffle_remote=4 * MiB, shuffle_local=4 * MiB,
             output_bytes=16 * MiB)
    ev.add(ev_stage_completed(stage_info(1, 0, s1["Stage Name"], 4, submit=t(16), complete=t(20))))
    ev.add(ev_job_end(0, t(21)))
    ev.add(ev_sql_end(0, t(22)))
    s2 = stage_info(2, 0, "collect at main.py:60", 2)
    p1 = job_properties(None, "Collect summary")
    ev.add(ev_job_start(1, t(25), [2], p1, [s2]))
    ev.add(ev_stage_submitted(stage_info(2, 0, s2["Stage Name"], 2, submit=t(25)), p1))
    for i in range(2):
        task(2, i, str(i), t(26), t(27), gc_ms=5)
    ev.add(ev_stage_completed(stage_info(2, 0, s2["Stage Name"], 2, submit=t(25), complete=t(28))))
    ev.add(ev_job_end(1, t(29)))
    ev.add(ev_app_end(t(60)))
    return ev.text(), stats


# --------------------------------------------------------------------------------------------
# main cluster: driver + executor logs
# --------------------------------------------------------------------------------------------
def _driver_logs() -> dict[str, str]:
    a = lambda: LogFile(T0A)  # noqa: E731
    rolled = a()
    rolled.log(-2, "INFO", "DriverDaemon", "Started Log4j2")
    rolled.log(-2, "INFO", "DriverDaemon$", "Current JVM Version 1.8.0_412")
    rolled.log(0, "INFO", "SparkContext", f"Running Spark version {SPARK_VERSION}")
    rolled.log(0, "INFO", "SparkContext", "Submitted application: Databricks Shell")
    for i in range(4):
        rolled.log(5 + i, "INFO", "StandaloneAppClient$ClientEndpoint",
                   f"Executor added: {APP_A}/{i} on worker-{i} ({HOSTS[str(i)]}:36161) with 4 core(s)")
    rolled.log(20, "INFO", "DAGScheduler", "Got job 0 (saveAsTable at NativeMethodAccessorImpl.java:0) with 20 output partitions")
    rolled.log(20, "INFO", "DAGScheduler", "Submitting ShuffleMapStage 0 (MapPartitionsRDD[0] at mapPartitions at transform.py:42), which has no missing parents")
    rolled.log(21, "INFO", "TaskSetManager", f"Starting task 0.0 in stage 0.0 (TID 0) ({HOSTS['0']}, executor 0, partition 0, PROCESS_LOCAL, 9876 bytes)")
    rolled.log(40, "INFO", "TaskSetManager", f"Finished task 19.0 in stage 0.0 (TID 19) in 5000 ms on {HOSTS['3']} (executor 3) (19/20)")
    rolled.log(300, "WARN", "DriverDaemon", "Driver heartbeat delayed, current load is high")
    rolled.raw("\tcontinuation of the previous message without a timestamp")
    rolled.log(600, "INFO", "BlockManagerInfo", f"Added broadcast_3_piece0 in memory on {HOSTS['0']}:41234 (size: 12.0 KiB, free: 4.0 GiB)")

    active = a()
    active.log(712, "INFO", "DriverDaemon", "Log rolled over, continuing in log4j-active.log")
    active.log(921, "INFO", "TaskSetManager", f"Finished task 0.0 in stage 0.0 (TID 0) in 900000 ms on {HOSTS['0']} (executor 0) (20/20)")
    active.log(922, "INFO", "DAGScheduler", "ShuffleMapStage 0 (mapPartitions at transform.py:42) finished in 902.0 s")
    active.log(975, "INFO", "DAGScheduler", "ResultStage 2 (count at main.py:25) finished in 10.0 s")
    active.log(1041, "ERROR", "TaskSchedulerImpl", f"Lost executor 2 on {HOSTS['2']}: {DECOM_REASON}")
    active.log(1041, "WARN", "TaskSetManager",
               f"Lost task 2.0 in stage 4.0 (TID 2026) ({HOSTS['2']} executor 2): ExecutorLostFailure "
               f"(executor 2 exited caused by one of the running tasks) Reason: {DECOM_REASON}")
    active.log(1150, "WARN", "TaskSetManager",
               f"Lost task 0.0 in stage 6.0 (TID 2046) ({HOSTS['1']} executor 1): java.lang.OutOfMemoryError: Java heap space")
    active.log(1151, "ERROR", "TaskSchedulerImpl", f"Lost executor 1 on {HOSTS['1']}: {OOM_REASON}")
    active.log(1152, "WARN", "TaskSetManager",
               f"Lost task 1.0 in stage 6.0 (TID 2048) ({HOSTS['3']} executor 3): FetchFailed(BlockManagerId(1, "
               f"{HOSTS['1']}, 41234, None), shuffleId=2, mapIndex=1, mapId=2046, reduceId=1, message=")
    active.raw(FETCH_FAILURE,
               "\tat org.apache.spark.errors.SparkCoreErrors$.fetchFailedError(SparkCoreErrors.scala:437)",
               "\tat org.apache.spark.storage.ShuffleBlockFetcherIterator.throwFetchFailedException(ShuffleBlockFetcherIterator.scala:1239)",
               "\tat org.apache.spark.storage.ShuffleBlockFetcherIterator.next(ShuffleBlockFetcherIterator.scala:971)",
               ")")
    active.log(1153, "INFO", "DAGScheduler",
               "Resubmitting ShuffleMapStage 5 (saveAsTable at main.py:57 (map)) and ResultStage 6 (saveAsTable at main.py:57) due to fetch failure")
    for k in range(4):
        active.log(1170 + 3 * k, "WARN", "TaskSetManager",
                   f"Lost task 0.{k} in stage 6.1 (TID {2050 + k}) ({HOSTS['3']} executor 3): "
                   "org.apache.spark.api.python.PythonException: Traceback (most recent call last):")
    active.log(1180, "ERROR", "TaskSetManager", "Task 0 in stage 6.1 failed 4 times; aborting job")
    active.log(1181, "ERROR", "Instrumentation", "org.apache.spark.SparkException: " + JOB_ABORT.split("\n")[0])
    active.raw("\tat org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:3051)",
               "\tat org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2987)",
               "\tat scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)",
               "\tat org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:280)")
    # context B (after restart) starts writing to the same active log
    b = (T0B - T0A).total_seconds()
    active.log(b - 5, "INFO", "DriverDaemon", "Starting driver daemon after restart")
    active.log(b, "INFO", "SparkContext", f"Running Spark version {SPARK_VERSION}")
    active.log(b, "INFO", "SparkContext", "Submitted application: Databricks Shell")
    active.log(b + 29, "INFO", "DAGScheduler", "Job 1 finished: collect at main.py:60, took 4.0 s")

    stdout = LogFile(T0A).raw("Starting ETL run for 2026-10-06", "Loaded 20 input files",
                              "Writing sales.orders", "Writing sales.daily_totals")
    stderr_rolled = LogFile(T0A).raw(
        "OpenJDK 64-Bit Server VM warning: ignoring option MaxPermSize=512m; support was removed in 8.0",
        "ANTLR Tool version 4.8 used for code generation does not match the current runtime version 4.9.3")
    stderr = LogFile(T0A)
    stderr.log(1182, "INFO", "ProgressReporter$", "Removed result fetcher for 1234567890_6543210")
    stderr.raw("Traceback (most recent call last):",
               '  File "/Workspace/Users/me/etl/main.py", line 57, in <module>',
               '    totals.write.mode("overwrite").saveAsTable("sales.daily_totals")',
               '  File "/databricks/spark/python/pyspark/sql/readwriter.py", line 1586, in saveAsTable',
               "    self._jwrite.saveAsTable(name)",
               '  File "/databricks/spark/python/pyspark/errors/exceptions/captured.py", line 261, in deco',
               "    raise converted from None",
               "pyspark.errors.exceptions.captured.PythonException: ",
               "  An exception was thrown from the Python worker. Please see the stack trace below.",
               "Traceback (most recent call last):",
               '  File "/databricks/spark/python/pyspark/worker.py", line 1876, in main',
               f"  {USER_FRAME}",
               "    return int(x)",
               "ValueError: invalid literal for int() with base 10: 'n/a'")
    stacktrace = LogFile(T0A).raw(
        "py4j.protocol.Py4JJavaError: An error occurred while calling o412.saveAsTable.",
        ": org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 6.1 failed 4 times",
        "\tat org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:3051)",
        "\tat org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2985)",
        "\tat py4j.Gateway.invoke(Gateway.java:306)")
    return {
        "log4j-2026-10-06-17.log.gz": rolled.text(),
        "log4j-active.log": active.text(),
        "stdout": stdout.text(),
        "stderr--2026-10-06--17-00": stderr_rolled.text(),
        "stderr": stderr.text(),
        "stacktrace.log": stacktrace.text(),
    }


def _executor_logs() -> dict[str, str]:
    """Returns {relative path under executor/: text}."""
    out: dict[str, str] = {}
    # executor 0: startup (rolled), spill, GC log in stdout
    e0r = LogFile(T0A)
    e0r.log(6, "INFO", "CoarseGrainedExecutorBackend", "Started daemon with process name: 4242@worker-0")
    e0r.log(6, "INFO", "CoarseGrainedExecutorBackend", "Successfully registered with driver")
    out[f"{APP_A}/0/stderr--2026-10-06--17-00"] = e0r.text()
    e0 = LogFile(T0A)
    e0.log(21, "INFO", "Executor", "Running task 0.0 in stage 0.0 (TID 0)")
    e0.log(921, "INFO", "Executor", "Finished task 0.0 in stage 0.0 (TID 0). 2345 bytes result sent to driver")
    for k in range(4):
        e0.log(930 + 5 * k, "INFO", "UnsafeExternalSorter",
               f"Thread {75 + k} spilling sort data of 768.0 MiB to disk ({k}  time so far)")
    e0.log(950, "INFO", "ExternalAppendOnlyMap", "Thread 79 spilling in-memory map of 1.2 GiB to disk (1 time so far)")
    e0.log(1152, "ERROR", "ShuffleBlockFetcherIterator", f"Failed to get block(s) from {HOSTS['1']}:41234")
    out[f"{APP_A}/0/stderr"] = e0.text()
    out[f"{APP_A}/0/stdout"] = LogFile(T0A).raw(
        "2026-10-06T17:48:20.001+0000: [GC (Allocation Failure) [PSYoungGen: 524800K->12345K(611840K)] 524800K->12353K(2010112K), 0.0123 secs]",
        "2026-10-06T18:04:10.500+0000: [Full GC (Ergonomics) [PSYoungGen: 98304K->0K(611840K)] [ParOldGen: 1398101K->1201234K(1398272K)], 2.9400 secs]",
        "2026-10-06T18:04:20.500+0000: [Full GC (Ergonomics) [PSYoungGen: 98304K->0K(611840K)] [ParOldGen: 1398101K->1301234K(1398272K)], 3.1000 secs]",
    ).text()

    # executor 1: OOM (twice, different line numbers -> same fingerprint)
    e1 = LogFile(T0A)
    e1.log(1120, "INFO", "Executor", "Running task 0.0 in stage 6.0 (TID 2046)")
    e1.log(1145, "WARN", "TaskMemoryManager", "Failed to allocate a page (67108864 bytes), try again.")
    e1.log(1150, "ERROR", "Executor", "Exception in task 0.0 in stage 6.0 (TID 2046)")
    e1.raw("java.lang.OutOfMemoryError: Java heap space",
           "\tat java.util.Arrays.copyOf(Arrays.java:3236)",
           "\tat org.apache.spark.unsafe.map.BytesToBytesMap.growAndRehash(BytesToBytesMap.java:906)",
           "\tat org.apache.spark.sql.execution.joins.UnsafeHashedRelation$.apply(HashedRelation.scala:458)",
           "\tat org.apache.spark.scheduler.Task.run(Task.scala:141)",
           "\tat java.lang.Thread.run(Thread.java:750)")
    e1.log(1150, "ERROR", "SparkUncaughtExceptionHandler",
           "Uncaught exception in thread Thread[Executor task launch worker for task 0.0 in stage 6.0 (TID 2046),5,main]")
    e1.raw("java.lang.OutOfMemoryError: Java heap space",
           "\tat java.util.Arrays.copyOf(Arrays.java:3237)",
           "\tat org.apache.spark.unsafe.map.BytesToBytesMap.growAndRehash(BytesToBytesMap.java:912)",
           "\tat org.apache.spark.sql.execution.joins.UnsafeHashedRelation$.apply(HashedRelation.scala:459)",
           "\tat org.apache.spark.scheduler.Task.run(Task.scala:142)",
           "\tat java.lang.Thread.run(Thread.java:750)")
    out[f"{APP_A}/1/stderr"] = e1.text()
    out[f"{APP_A}/1/stdout"] = LogFile(T0A).raw(
        "2026-10-06T18:07:20.100+0000: [GC (Allocation Failure) [PSYoungGen: 524800K->512345K(611840K)], 0.8123 secs]").text()

    # executor 2: decommissioned (spot)
    e2 = LogFile(T0A)
    e2.log(1002, "INFO", "Executor", "Running task 2.0 in stage 4.0 (TID 2026)")
    e2.log(1040, "WARN", "CoarseGrainedExecutorBackend", "Received decommission executor message")
    e2.log(1040, "INFO", "Executor", "Decommission: told to decommission, waiting for running tasks to finish")
    out[f"{APP_A}/2/stderr"] = e2.text()

    # executor 3: FetchFailed + PythonException with user frame
    e3 = LogFile(T0A)
    e3.log(1151, "INFO", "Executor", "Running task 1.0 in stage 6.0 (TID 2048)")
    e3.log(1152, "WARN", "Executor", "Task 2048 failed with fetch failure")
    e3.raw(FETCH_FAILURE,
           "\tat org.apache.spark.errors.SparkCoreErrors$.fetchFailedError(SparkCoreErrors.scala:437)",
           "\tat org.apache.spark.storage.ShuffleBlockFetcherIterator.throwFetchFailedException(ShuffleBlockFetcherIterator.scala:1239)",
           "\tat org.apache.spark.storage.ShuffleBlockFetcherIterator.next(ShuffleBlockFetcherIterator.scala:971)",
           "Caused by: java.io.IOException: Failed to connect to /10.139.64.12:41234",
           "\tat org.apache.spark.network.client.TransportClientFactory.createClient(TransportClientFactory.java:298)",
           "\tat org.apache.spark.network.client.TransportClientFactory.createClient(TransportClientFactory.java:218)")
    e3.log(1168, "INFO", "Executor", "Running task 0.0 in stage 6.1 (TID 2050)")
    e3.log(1170, "ERROR", "Executor", "Exception in task 0.0 in stage 6.1 (TID 2050)")
    e3.raw("org.apache.spark.api.python.PythonException: " + PY_TRACE.rstrip("\n"),
           "\tat org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:694)",
           "\tat org.apache.spark.sql.execution.python.PythonArrowOutput$$anon$1.read(PythonArrowOutput.scala:118)")
    out[f"{APP_A}/3/stderr"] = e3.text()
    out[f"{APP_A}/3/stdout"] = LogFile(T0A).raw("parse_amount: processing batch 1").text()

    # context B executor 0 (plain, healthy)
    eb = LogFile(T0B)
    eb.log(5, "INFO", "CoarseGrainedExecutorBackend", "Successfully registered with driver")
    eb.log(12, "INFO", "Executor", "Running task 0.0 in stage 0.0 (TID 0)")
    eb.log(14, "INFO", "Executor", "Finished task 0.0 in stage 0.0 (TID 0). 2345 bytes result sent to driver")
    out[f"{APP_B}/0/stderr"] = eb.text()
    return out


# --------------------------------------------------------------------------------------------
# healthy cluster
# --------------------------------------------------------------------------------------------
def _healthy() -> tuple[dict[str, str], dict[str, str], str, dict]:
    t = lambda sec: epoch_ms(T0H) + int(round(sec * 1000))  # noqa: E731
    ev = EventLog()
    stats = {"tasks": 0}
    tid = [0]
    ev.add(ev_log_start())
    ev.add(ev_app_start(APP_H, t(0)))
    for i, ex in enumerate(["0", "1"]):
        ev.add(ev_executor_added(ex, HOSTS_H[ex], t(4 + i)))
    ev.add(ev_sql_start(0, t(10), "range at nightly.py:5", PLAN_H, "/Workspace/Users/me/jobs/nightly.py:5"))
    s0 = stage_info(0, 0, "range at nightly.py:5", 4)
    s1 = stage_info(1, 0, "collect at nightly.py:6", 4)
    p0 = job_properties(0, "Nightly range", databricks=False)
    ev.add(ev_job_start(0, t(11), [0, 1], p0, [s0, s1]))
    for sid, (sub, st, en, comp) in enumerate([(11, 12, 15, 16), (17, 18, 21, 22)]):
        ev.add(ev_stage_submitted(stage_info(sid, 0, [s0, s1][sid]["Stage Name"], 4, submit=t(sub)), p0))
        for i in range(4):
            ex = str(i % 2)
            ev.add(ev_task_end(sid, 0, tid[0], i, 0, ex, HOSTS_H[ex], t(st), t(en), gc_ms=30,
                               input_bytes=MiB, input_records=250))
            tid[0] += 1
            stats["tasks"] += 1
        ev.add(ev_stage_completed(stage_info(sid, 0, [s0, s1][sid]["Stage Name"], 4, submit=t(sub), complete=t(comp))))
    ev.add(ev_job_end(0, t(23)))
    ev.add(ev_sql_end(0, t(24)))
    ev.add(ev_app_end(t(40)))

    d = LogFile(T0H)
    d.log(0, "INFO", "SparkContext", f"Running Spark version {SPARK_VERSION}")
    d.log(11, "INFO", "DAGScheduler", "Got job 0 (collect at nightly.py:6) with 4 output partitions")
    d.log(23, "INFO", "DAGScheduler", "Job 0 finished: collect at nightly.py:6, took 12.0 s")
    driver = {"log4j-active.log": d.text(),
              "stdout": LogFile(T0H).raw("nightly: 1000 rows").text(),
              "stderr": LogFile(T0H).raw("ANTLR Tool version 4.8 used for code generation does not match the current runtime version 4.9.3").text()}
    execs = {}
    for ex in ("0", "1"):
        e = LogFile(T0H)
        e.log(4, "INFO", "CoarseGrainedExecutorBackend", "Successfully registered with driver")
        e.log(12, "INFO", "Executor", f"Running task {ex}.0 in stage 0.0 (TID {ex})")
        execs[f"{APP_H}/{ex}/stderr"] = e.text()
    return driver, execs, ev.text(), stats


# --------------------------------------------------------------------------------------------
# REV3 cluster: CONTRACT Revision 3 shapes (all values synthetic)
# --------------------------------------------------------------------------------------------
CTX_R = "6660001112223334446"
APP_R = "app-20261009180012-0000"
HASH_R = f"{REV3}_10_0_0_5"
T0R = datetime(2026, 10, 9, 18, 0, 12)   # REV3 application start (UTC, naive)
HOSTS_R = {"0": "10.0.0.11", "1": "10.0.0.12", "2": "10.0.0.13", "3": "10.0.0.14"}
DRIVER_HOST_R = "10.0.0.5"
CONNECT = "org.apache.spark.sql.connect.service."
R_USER = "user@example.com"
R_SESSION = "0d9a7c1e-0000-4000-8000-000000000001"
R_OPS = {  # operation id -> (statement text, start sec)
    "op-0001": ("spark.read.table('main.sales.orders').groupBy('region').count().collect()", 15),
    "op-0002": ("spark.read.table('main.sales.orders').join(spark.read.table('main.sales.customers'), 'customer_id')"
                ".write.mode('overwrite').saveAsTable('main.sales.enriched')", 100),
    "op-0003": ("spark.read.table('main.sales.enriched').limit(10).collect()", 220),
}
R_KILLED_RAW = '{"cause":"Command exited with code 9","detectionMechanism":null}'
R_AUTOSCALE_RAW = '{"cause":"kill request from HTTP endpoint","detectionMechanism":null}'
R_TERMINATION_RAW = '{"cause":"cluster termination","detectionMechanism":null}'
R_KILLED_CAUSE = "Command exited with code 9"
R_FETCH = "org.apache.spark.shuffle.FetchFailedException: Failed to connect to /10.0.0.12:41234"
R_IO_ERROR = "Connection reset by peer"
R_USAGE_TAGS = {
    "spark.databricks.clusterUsageTags.clusterId": REV3,
    "spark.databricks.clusterUsageTags.clusterName": "etl-shared-cluster",
    "spark.databricks.clusterUsageTags.clusterCreator": "user@example.com",
    "spark.databricks.clusterUsageTags.effectiveSparkVersion": "15.4.x-scala2.12",
    "spark.databricks.clusterUsageTags.sparkVersion": "15.4.x-scala2.12",
    "spark.databricks.clusterUsageTags.clusterNodeType": "Standard_D8ds_v5",
    "spark.databricks.clusterUsageTags.driverNodeType": "Standard_D4ds_v5",
    "spark.databricks.clusterUsageTags.clusterMinWorkers": "2",
    "spark.databricks.clusterUsageTags.clusterMaxWorkers": "4",
    "spark.databricks.clusterUsageTags.clusterTargetWorkers": "3",
    "spark.databricks.clusterUsageTags.clusterWorkers": "3",
    "spark.databricks.clusterUsageTags.clusterScalingType": "autoscaling",
    "spark.databricks.clusterUsageTags.runtimeEngine": "STANDARD",
    "spark.databricks.clusterUsageTags.cloudProvider": "Azure",
    "spark.databricks.clusterUsageTags.region": "westeurope",
    "spark.databricks.clusterUsageTags.clusterWorkloadType": "Jobs",
    "spark.databricks.clusterUsageTags.jobId": "123456789",
    "spark.databricks.job.id": "123456789",
    "spark.databricks.job.runId": "987654321",
    "spark.databricks.job.taskRunId": "555000111",
    "spark.databricks.job.parentRunId": "987654000",
}


def r_job_tag(op: str) -> str:
    return f"SparkConnect_OperationTag_User_{R_USER}_Session_{R_SESSION}_Operation_{op}"


def jvm_line(dt: datetime, uptime_s: float, level: str, tags: str, msg: str, offset: str = "+0000") -> str:
    """JVM unified logging line, e.g. [2026-10-09T18:00:01.123+0000][1.234s][info][gc] GC(12) ..."""
    if offset != "+0000":
        sign = 1 if offset[0] == "+" else -1
        dt = dt + sign * timedelta(hours=int(offset[1:3]), minutes=int(offset[3:5]))
    stamp = dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}{offset}"
    return f"[{stamp}][{uptime_s:.3f}s][{level}][{tags}] {msg}"


def ev_connect(kind: str, op: str | None, ts: int, **fields) -> dict:
    d = {"Event": CONNECT + f"SparkListenerConnect{kind}", "eventTime": ts}
    if op is not None:
        d["jobTag"] = r_job_tag(op)
        d["operationId"] = op
    d.update(fields)
    d.setdefault("extraTags", {})
    return d


def _rev3_events() -> tuple[str, str, dict]:
    """Returns (rolled_text, active_text, info)."""
    t = lambda sec: epoch_ms(T0R) + int(round(sec * 1000))  # noqa: E731
    ev = EventLog()
    info = {"tasks": 0, "failed_tasks": 0, "task_ids": {}, "event_counts": {}}
    tid = [0]

    def add(e: dict) -> None:
        info["event_counts"][e["Event"]] = info["event_counts"].get(e["Event"], 0) + 1
        ev.add(e)

    def task(stage, att, index, tatt, ex, launch, finish, *, speculative=False, **kw):
        tt = tid[0]
        tid[0] += 1
        info["tasks"] += 1
        if kw.get("reason") and kw["reason"]["Reason"] != "Success":
            info["failed_tasks"] += 1
        info["task_ids"][(stage, att, index, tatt)] = tt
        start = ev_task_start(stage, att, tt, index, tatt, ex, HOSTS_R[ex], t(launch))
        start["Task Info"]["Speculative"] = speculative
        add(start)
        end = ev_task_end(stage, att, tt, index, tatt, ex, HOSTS_R[ex], t(launch), t(finish), **kw)
        end["Task Info"]["Speculative"] = speculative
        add(end)
        return tt

    def props(op: str, desc: str) -> dict:
        p = {"spark.job.interruptOnCancel": "true", "spark.job.description": desc,
             "callSite.short": "collect at <unknown>:0",
             "spark.job.tags": f"{r_job_tag(op)},etl-nightly",
             "spark.jobGroup.id": f"{REV3}_job-123456789-run-987654321-action-1",
             "user": R_USER}
        p.update(R_USAGE_TAGS)
        return p

    def op_start(op: str, sec: float) -> None:
        stmt = R_OPS[op][0]
        add(ev_connect("OperationStarted", op, t(sec), sessionId=R_SESSION, userId=R_USER, userName=R_USER,
                       statementText=stmt, planRequest=None))
        add(ev_connect("OperationAnalyzed", op, t(sec + 1), analyzedPlan=None))
        add(ev_connect("OperationReadyForExecution", op, t(sec + 2)))

    def op_end(op: str, sec: float) -> None:
        add(ev_connect("OperationFinished", op, t(sec), producedRowCount=10))
        add(ev_connect("OperationClosed", op, t(sec + 1)))

    add(ev_log_start())
    add(ev_resource_profile())
    add(ev_block_manager_added("driver", DRIVER_HOST_R, t(0)))
    env = ev_env_update()
    env["Spark Properties"] = {"spark.databricks.clusterUsageTags.clusterId": REV3,
                               "spark.executor.memory": "8g"}
    add(env)
    add(ev_app_start(APP_R, t(0)))
    for i, ex in enumerate(["0", "1", "2"]):
        add(ev_executor_added(ex, HOSTS_R[ex], t(5 + i)))
    add(ev_connect("SessionStarted", None, t(10), sessionId=R_SESSION, userId=R_USER, userName=R_USER))

    # ---- op-0001 -> job 0 (stages 0, 1), all fine
    op_start("op-0001", 15)
    s0 = stage_info(0, 0, "collect at <unknown>:0 (scan)", 4)
    s1 = stage_info(1, 0, "collect at <unknown>:0", 4)
    p0 = props("op-0001", "Count orders per region")
    add(ev_job_start(0, t(18), [0, 1], p0, [s0, s1]))
    add(ev_stage_submitted(stage_info(0, 0, s0["Stage Name"], 4, submit=t(18)), p0))
    for i in range(4):
        task(0, 0, i, 0, str(i % 3), 19, 24, gc_ms=20, input_bytes=16 * MiB, input_records=10_000,
             shuffle_write=MiB, task_type="ShuffleMapTask")
    add(ev_stage_completed(stage_info(0, 0, s0["Stage Name"], 4, submit=t(18), complete=t(25))))
    add(ev_stage_submitted(stage_info(1, 0, s1["Stage Name"], 4, submit=t(26)), p0))
    for i in range(4):
        task(1, 0, i, 0, str(i % 3), 27, 30, gc_ms=10, shuffle_remote=MiB // 2, shuffle_local=MiB // 2)
    add(ev_stage_completed(stage_info(1, 0, s1["Stage Name"], 4, submit=t(26), complete=t(31))))
    add(ev_job_end(0, t(32)))
    op_end("op-0001", 33)
    rolled = ev.text()
    ev.lines = []

    # ---- op-0002 -> job 1 (stages 2, 3): every job succeeds, but tasks are retried
    op_start("op-0002", 100)
    s2 = stage_info(2, 0, "saveAsTable at <unknown>:0 (map)", 8)
    s3 = stage_info(3, 0, "saveAsTable at <unknown>:0", 4)
    p1 = props("op-0002", "Write enriched orders")
    add(ev_job_start(1, t(103), [2, 3], p1, [s2, s3]))
    add(ev_stage_submitted(stage_info(2, 0, s2["Stage Name"], 8, submit=t(103)), p1))
    mkw = dict(gc_ms=50, input_bytes=32 * MiB, input_records=20_000, shuffle_write=8 * MiB, task_type="ShuffleMapTask")
    task(2, 0, 6, 0, "2", 104, 106, reason=ex_failure("java.io.IOException", R_IO_ERROR, IO_FRAMES), **mkw)
    task(2, 0, 6, 1, "2", 107, 112, **mkw)
    for i, ex in ((0, "0"), (1, "1"), (2, "2"), (7, "0")):
        task(2, 0, i, 0, ex, 104, 114, **mkw)
    task(2, 0, 5, 0, "2", 115, 125, **mkw)
    # index 4: slow on executor 0, a speculative copy on executor 2 is killed when the original finishes
    task(2, 0, 4, 1, "2", 130, 140, speculative=True,
         reason={"Reason": "TaskKilled", "Kill Reason": "another attempt succeeded", "Accumulator Updates": []},
         **mkw)
    task(2, 0, 4, 0, "0", 115, 140, **mkw)
    # index 3: executor 1 is killed by the OS (exit code 9) while running it
    task(2, 0, 3, 0, "1", 104, 150,
         reason={"Reason": "ExecutorLostFailure", "Executor ID": "1", "Exit Caused By App": True,
                 "Loss Reason": "Command exited with code 9"}, **mkw)
    add(ev_executor_removed("1", t(150), R_KILLED_RAW))
    task(2, 0, 3, 1, "0", 155, 165, **mkw)
    add(ev_stage_completed(stage_info(2, 0, s2["Stage Name"], 8, submit=t(103), complete=t(166))))
    # stage 3.0: FetchFailed (map output of executor 1 is gone) -> stage 2.1 recompute -> stage 3.1
    add(ev_stage_submitted(stage_info(3, 0, s3["Stage Name"], 4, submit=t(167)), p1))
    fetch = {"Reason": "FetchFailed", "Block Manager Address": {"Executor ID": "1", "Host": HOSTS_R["1"], "Port": 41234},
             "Shuffle ID": 1, "Map ID": 9, "Map Index": 1, "Reduce ID": 0, "Message": R_FETCH}
    rkw = dict(gc_ms=30, shuffle_remote=4 * MiB, shuffle_local=4 * MiB, output_bytes=8 * MiB)
    task(3, 0, 0, 0, "2", 168, 171, reason=fetch, **rkw)
    for i, ex in ((1, "0"), (2, "2"), (3, "0")):
        task(3, 0, i, 0, ex, 168, 173, **rkw)
    add(ev_stage_completed(stage_info(3, 0, s3["Stage Name"], 4, submit=t(167), complete=t(174), failure=R_FETCH)))
    add(ev_stage_submitted(stage_info(2, 1, s2["Stage Name"], 8, submit=t(175)), p1))
    task(2, 1, 1, 0, "0", 176, 186, **mkw)
    add(ev_stage_completed(stage_info(2, 1, s2["Stage Name"], 8, submit=t(175), complete=t(187))))
    add(ev_stage_submitted(stage_info(3, 1, s3["Stage Name"], 4, submit=t(188)), p1))
    task(3, 1, 0, 0, "2", 189, 194, **rkw)
    add(ev_stage_completed(stage_info(3, 1, s3["Stage Name"], 4, submit=t(188), complete=t(195))))
    add(ev_job_end(1, t(196)))
    op_end("op-0002", 197)

    # ---- autoscale up, op-0003 -> job 2 (stage 4) on the new executor
    add(ev_executor_added("3", HOSTS_R["3"], t(210)))
    op_start("op-0003", 220)
    s4 = stage_info(4, 0, "collect at <unknown>:0", 2)
    p2 = props("op-0003", "Preview enriched orders")
    add(ev_job_start(2, t(223), [4], p2, [s4]))
    add(ev_stage_submitted(stage_info(4, 0, s4["Stage Name"], 2, submit=t(223)), p2))
    task(4, 0, 0, 0, "3", 224, 230, gc_ms=5, input_bytes=MiB, input_records=10)
    task(4, 0, 1, 0, "0", 224, 228, gc_ms=5, input_bytes=MiB, input_records=10)
    add(ev_stage_completed(stage_info(4, 0, s4["Stage Name"], 2, submit=t(223), complete=t(231))))
    add(ev_job_end(2, t(232)))
    op_end("op-0003", 233)
    add(ev_executor_removed("3", t(380), R_AUTOSCALE_RAW))      # autoscale down: no finding
    add(ev_connect("SessionClosed", None, t(500), sessionId=R_SESSION, userId=R_USER, userName=R_USER))
    add(ev_executor_removed("0", t(600), R_TERMINATION_RAW))   # cluster termination: no finding
    add(ev_executor_removed("2", t(600), R_TERMINATION_RAW))
    add(ev_app_end(t(601)))
    return rolled, ev.text(), info


def _rev3_logs() -> tuple[dict[str, tuple[str, bool]], dict[str, tuple[str, bool]]]:
    """Returns (driver files, executor files): {relative name: (text, gzip)}."""
    T = lambda sec: T0R + timedelta(seconds=sec)  # noqa: E731
    driver: dict[str, tuple[str, bool]] = {}

    d = LogFile(T0R)
    d.log(0, "INFO", "SparkContext", f"Running Spark version {SPARK_VERSION}")
    d.log(10, "INFO", "SparkConnectService", f"New session {R_SESSION} for user {R_USER}")
    d.log(150, "WARN", "TaskSetManager",
          f"Lost task 3.0 in stage 2.0 (TID 11) ({HOSTS_R['1']} executor 1): ExecutorLostFailure "
          f"(executor 1 exited caused by one of the running tasks) Reason: Command exited with code 9")
    d.log(150, "ERROR", "TaskSchedulerImpl", f"Lost executor 1 on {HOSTS_R['1']}: Command exited with code 9")
    d.log(196, "INFO", "DAGScheduler", "Job 1 finished: saveAsTable at <unknown>:0, took 93.0 s")
    d.log(380, "INFO", "StandaloneSchedulerBackend", "Requesting to kill executor(s) 3")
    driver["log4j-active.log"] = (d.text(), False)

    # driver stdout: ISO-8601 timestamps (Python logging) plus a few JVM unified GC lines
    driver["stdout--2026-10-09--18-00"] = ("\n".join([
        "2026-10-09T18:00:13.250000Z INFO etl.main: bootstrapping",
        "2026-10-09 18:00:14,500 INFO py4j.clientserver: Received command c on object id p0",
    ]) + "\n", False)
    driver["stdout"] = ("\n".join([
        "2026-10-09T18:00:20.123456Z INFO etl.main: starting run",
        "2026-10-09 18:00:21,456 WARNING py4j.clientserver: Closing down clientserver connection",
        "plain line without a timestamp",
        jvm_line(T(18.5), 18.5, "info", "gc", "GC(3) Pause Young (Normal) (G1 Evacuation Pause) 1024M->256M(4096M) 5.000ms",
                 offset="+0200"),
    ]) + "\n", False)

    # driver stderr: a Java exception immediately followed by a JVM thread dump
    driver["stderr--2026-10-09--18-00"] = (
        "OpenJDK 64-Bit Server VM warning: Options -Xverify:none and -noverify were deprecated in JDK 13\n", False)
    driver["stderr"] = ("\n".join([
        f"{log4j_ts(T(250))} ERROR SparkConnectService: Error while handling a request",
        "java.lang.IllegalStateException: Connection pool shut down",
        "\tat org.apache.http.impl.conn.PoolingHttpClientConnectionManager.requestConnection(PoolingHttpClientConnectionManager.java:269)",
        "\tat com.example.etl.Loader.fetch(Loader.java:88)",
        "\tat java.base/java.lang.Thread.run(Thread.java:840)",
        "2026-10-09 18:04:23",
        "Full thread dump OpenJDK 64-Bit Server VM (17.0.12+7-LTS mixed mode, sharing):",
        "",
        '"main" #1 prio=5 os_prio=0 cpu=1234.56ms elapsed=250.12s tid=0x00007f0000001000 nid=0x1 waiting on condition  [0x00007f0000aff000]',
        "   java.lang.Thread.State: TIMED_WAITING (sleeping)",
        "\tat java.base@17.0.12/java.lang.Thread.sleep(Native Method)",
        "\tat com.example.etl.Main.waitLoop(Main.java:55)",
        "\tat java.base@17.0.12/java.lang.Thread.run(Thread.java:840)",
        "",
        '"spark-listener-group-shared" #12 daemon prio=5 os_prio=0 cpu=55.10ms elapsed=240.00s tid=0x00007f0000002000 nid=0x2c waiting on condition  [0x00007f0000bff000]',
        "   java.lang.Thread.State: WAITING (parking)",
        "\tat java.base@17.0.12/jdk.internal.misc.Unsafe.park(Native Method)",
        "\t- parking to wait for  <0x00000000c0a1b2c3> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)",
        "\tat java.base@17.0.12/java.util.concurrent.locks.LockSupport.park(LockSupport.java:341)",
        "\tat com.example.etl.Listener.poll(Listener.java:21)",
        "",
    ]) + "\n", False)

    # stacktrace.log: thread dump frames WITHOUT "at"; the rolled one holds a real exception
    driver["2026-10-09-18.stacktrace.log.gz"] = ("\n".join([
        "java.util.concurrent.TimeoutException: Futures timed out after [300 seconds]",
        "\tat scala.concurrent.impl.Promise$DefaultPromise.tryAwait(Promise.scala:259)",
        "\tat com.example.etl.Waiter.await(Waiter.scala:12)",
    ]) + "\n", True)
    driver["stacktrace.log"] = ("\n".join([
        '"Executor task launch worker for task 3.0 in stage 2.0 (TID 11)" #88 daemon prio=5 os_prio=0 cpu=10.00ms elapsed=30.00s tid=0x00007f0000003000 nid=0x58 runnable',
        "   java.lang.Thread.State: RUNNABLE",
        "java.base@17.0.12/java.io.FileInputStream.readBytes(Native Method)",
        "java.base@17.0.12/java.io.FileInputStream.read(FileInputStream.java:276)",
        "org.apache.spark.storage.DiskStore.getBytes(DiskStore.scala:120)",
        "com.example.etl.Reader.next(Reader.java:44)",
        "",
        '"dispatcher-event-loop-1" #40 daemon prio=5 os_prio=0 cpu=5.00ms elapsed=30.00s tid=0x00007f0000004000 nid=0x30 waiting on condition',
        "   java.lang.Thread.State: WAITING (parking)",
        "java.base@17.0.12/jdk.internal.misc.Unsafe.park(Native Method)",
    ]) + "\n", False)

    execs: dict[str, tuple[str, bool]] = {}
    # executor 0: log4j stderr (rolled gz + active with multi-line continuation messages), GC log stdout
    e0r = LogFile(T0R)
    e0r.log(5, "INFO", "CoarseGrainedExecutorBackend", "Started daemon with process name: 4242@10.0.0.11")
    e0r.log(6, "INFO", "CoarseGrainedExecutorBackend", "Successfully registered with driver")
    execs[f"{APP_R}/0/stderr--2026-10-09--18.gz"] = (e0r.text(), True)
    e0 = LogFile(T0R)
    e0.log(104, "INFO", "Executor", "Running task 0.0 in stage 2.0 (TID 9)")
    e0.log(141, "ERROR", "TaskResources", "Error while releasing resources for task [X_123_45] of")
    e0.raw(" stage 2 attempt 0 (TID 14)",
           " [X_123_45])")
    e0.log(142, "INFO", "Executor", "Finished task 4.0 in stage 2.0 (TID 14). 2345 bytes result sent to driver")
    e0.log(143, "WARN", "BlockManager", "Block rdd_9_4 could not be removed as it was not found on disk or in memory")
    e0.raw(" (block manager 0 on 10.0.0.11)")
    execs[f"{APP_R}/0/stderr"] = (e0.text(), False)
    execs[f"{APP_R}/0/stdout"] = ("\n".join([
        jvm_line(T(0.5), 0.01, "info", "gc,init", "Version: 17.0.12+7-LTS (release)"),
        jvm_line(T(8.1), 8.1, "info", "gc", "GC(0) Pause Young (Normal) (G1 Evacuation Pause) 2048M->512M(8192M) 12.345ms"),
        jvm_line(T(20), 20.0, "info", "gc", "GC(1) Pause Young (Concurrent Start) (G1 Humongous Allocation) 3G->1G(8G) 20.500ms"),
        jvm_line(T(20.6), 20.6, "info", "gc", "GC(1) Concurrent Mark Cycle 45.678ms"),
        jvm_line(T(21), 21.0, "info", "gc", "GC(2) Pause Remark 3500M->3400M(8192M) 3.210ms"),
        jvm_line(T(21.5), 21.5, "info", "gc", "GC(3) Pause Cleanup 3400M->3400M(8192M) 0.100ms"),
        jvm_line(T(110), 110.0, "info", "gc", "GC(4) Pause Young (Normal) (G1 Evacuation Pause) 524288K->262144K(8388608K) 4.000ms"),
    ]) + "\n", False)

    # executor 1: GC log with several Full GCs before the OS kills it (exit code 9)
    execs[f"{APP_R}/1/stdout--2026-10-09--18.gz"] = ("\n".join([
        jvm_line(T(60), 60.0, "info", "gc", "GC(0) Pause Young (Normal) (G1 Evacuation Pause) 4096M->3900M(8192M) 50.000ms"),
        jvm_line(T(90), 90.0, "info", "gc", "GC(1) Pause Young (Normal) (G1 Evacuation Pause) 4000M->3950M(8192M) 60.000ms"),
    ]) + "\n", True)
    execs[f"{APP_R}/1/stdout"] = ("\n".join([
        jvm_line(T(120), 120.0, "info", "gc", "GC(2) Pause Full (G1 Compaction Pause) 8100M->7800M(8192M) 1500.000ms"),
        jvm_line(T(126), 126.0, "info", "gc", "GC(3) Pause Full (G1 Compaction Pause) 8150M->7850M(8192M) 1600.000ms"),
        jvm_line(T(132), 132.0, "warning", "gc,alloc", "Executor task launch worker for task 3.0: Retried waiting for GCLocker too often allocating 256 words"),
        jvm_line(T(134), 134.0, "info", "gc", "GC(4) Pause Full (G1 Compaction Pause) 8180M->7900M(8192M) 1700.000ms"),
        jvm_line(T(141), 141.0, "info", "gc", "GC(5) Pause Full (G1 Compaction Pause) 8190M->7950M(8192M) 1800.000ms"),
    ]) + "\n", False)
    e1 = LogFile(T0R)
    e1.log(104, "INFO", "Executor", "Running task 3.0 in stage 2.0 (TID 11)")
    e1.log(145, "WARN", "Executor", "Issue communicating with driver in heartbeater")
    execs[f"{APP_R}/1/stderr"] = (e1.text(), False)

    # executor 2: plain stderr
    e2 = LogFile(T0R)
    e2.log(104, "INFO", "Executor", "Running task 6.0 in stage 2.0 (TID 0)")
    e2.log(106, "ERROR", "Executor", "Exception in task 6.0 in stage 2.0 (TID 0)")
    e2.raw("java.io.IOException: Connection reset by peer",
           "\tat org.apache.spark.network.client.TransportClient.sendRpc(TransportClient.java:318)")
    execs[f"{APP_R}/2/stderr"] = (e2.text(), False)

    # executor 3: added by autoscaling, removed when idle
    e3 = LogFile(T0R)
    e3.log(210, "INFO", "CoarseGrainedExecutorBackend", "Successfully registered with driver")
    e3.log(380, "INFO", "CoarseGrainedExecutorBackend", "Driver commanded a shutdown")
    execs[f"{APP_R}/3/stderr"] = (e3.text(), False)
    return driver, execs


def _rev3(root: Path) -> dict:
    c = root / REV3
    driver, execs = _rev3_logs()
    n_lines = 0
    n_files = 0
    for name, (text, gz) in driver.items():
        _write(c / "driver" / name, text, gz=gz)
        n_lines += _count_lines(text)
        n_files += 1
    for rel, (text, gz) in execs.items():
        _write(c / "executor" / rel, text, gz=gz)
        n_lines += _count_lines(text)
        n_files += 1
    rolled, active, info = _rev3_events()
    ev_dir = c / "eventlog" / HASH_R / CTX_R
    _write(ev_dir / "eventlog-2026-10-09--18-00.gz", rolled, gz=True)
    _write(ev_dir / "eventlog", active)
    n_files += 2
    # init_scripts/ must be ignored (file names deliberately contain "stderr"/"stdout")
    init = c / "init_scripts" / f"{REV3}_10_0_0_5"
    _write(init / "20261009_180000_00_setup.sh.stderr.log",
           "ERROR: pip install failed\njava.lang.RuntimeException: init script boom\n\tat com.example.Init.run(Init.java:1)\n")
    _write(init / "20261009_180000_00_setup.sh.stdout.log", "installing packages\n")

    t = lambda sec: epoch_ms(T0R) + int(round(sec * 1000))  # noqa: E731
    tids = info["task_ids"]
    ops = {}
    for i, (op, (stmt, sec)) in enumerate(R_OPS.items()):
        end = {"op-0001": 33, "op-0002": 197, "op-0003": 233}[op]
        ops[op] = {"spark_job_id": i, "statement_text": stmt, "job_tag": r_job_tag(op),
                   "start_time": t(sec), "analyzed_time": t(sec + 1), "ready_time": t(sec + 2),
                   "finish_time": t(end), "closed_time": t(end + 1), "status": "finished"}
    return {
        "files": n_files,                       # driver + executor + eventlog files (init_scripts excluded)
        "init_script_files": 2,
        "log_lines": n_lines,
        "contexts": {CTX_R: APP_R},
        "tasks": {CTX_R: info["tasks"]},
        "failed_tasks": {CTX_R: info["failed_tasks"]},
        "task_ids": tids,
        # stage attempts 0, 1, 2.0, 3.0, 2.1, 3.1, 4
        "stages": {CTX_R: 7},
        "spark_jobs": {CTX_R: 3},
        "sql_queries": {CTX_R: 0},
        "executors": {CTX_R: 4},
        "malformed_lines": {CTX_R: 0},
        "status": "succeeded",
        "event_counts": dict(info["event_counts"]),
        "connect_operations": ops,
        "removal": {  # executor -> (category, cause, raw, finding category or None)
            "1": ("killed", R_KILLED_CAUSE, R_KILLED_RAW, "executor_killed"),
            "3": ("autoscale", "kill request from HTTP endpoint", R_AUTOSCALE_RAW, None),
            "0": ("termination", "cluster termination", R_TERMINATION_RAW, None),
            "2": ("termination", "cluster termination", R_TERMINATION_RAW, None),
        },
        "removed_time": {"1": t(150), "3": t(380), "0": t(600), "2": t(600)},
        # (stage_id, stage_attempt, task_index) -> expected task_retries row (None = assert loosely)
        "task_retries": {
            (2, 0, 3): {"attempts": 2, "first_attempt_executor_id": "1", "first_attempt_host": HOSTS_R["1"],
                        "first_failure_reason": "ExecutorLostFailure", "first_failure_categories": {"executor_lost", "killed"},
                        "first_failure_error": "Command exited with code 9", "first_failure_time": t(150),
                        "executor_removed_reason": R_KILLED_CAUSE, "final_status": "succeeded",
                        "final_executor_id": "0", "retry_delay_ms": 5000, "wasted_ms": 46000, "spark_job_id": 1},
            (2, 0, 6): {"attempts": 2, "first_attempt_executor_id": "2", "first_attempt_host": HOSTS_R["2"],
                        "first_failure_reason": "ExceptionFailure", "first_failure_categories": {"exception"},
                        "first_failure_error": R_IO_ERROR, "first_failure_time": t(106),
                        "executor_removed_reason": None, "final_status": "succeeded",
                        "final_executor_id": "2", "retry_delay_ms": 1000, "wasted_ms": 2000, "spark_job_id": 1},
            (2, 0, 4): {"attempts": 2, "final_status": "succeeded", "final_executor_id": "0", "spark_job_id": 1},
            (3, 0, 0): {"attempts": 1, "first_attempt_executor_id": "2", "first_failure_reason": "FetchFailed",
                        "first_failure_categories": {"fetch_failed"}, "first_failure_error": "FetchFailedException",
                        "first_failure_time": t(171), "wasted_ms": 3000, "spark_job_id": 1},
        },
        "fetch_failure": R_FETCH,
        "gc": {  # executor 1 GC totals
            "1": {"gc_pauses": 6, "full_gcs": 4, "gc_pause_ms": 50 + 60 + 1500 + 1600 + 1700 + 1800,
                  "max_heap_after_mb": 7950.0},
        },
        "cluster_info": {"cluster_name": "etl-shared-cluster", "spark_version": "15.4.x-scala2.12",
                         "driver_node_type": "Standard_D4ds_v5", "worker_node_type": "Standard_D8ds_v5",
                         "min_workers": 2, "max_workers": 4, "target_workers": 3,
                         "databricks_job_id": "123456789", "job_run_id": "987654321", "task_run_id": "555000111"},
    }


# --------------------------------------------------------------------------------------------
# public API
# --------------------------------------------------------------------------------------------
def _count_lines(text: str) -> int:
    return len(text.splitlines())


def generate(root: Path) -> dict:
    """Write the fixture clusters under ``root``. Returns
    ``{"main": <id>, "healthy": <id>, "empty": <id>, "rev3": <id>}``. Also fills module-level ``EXPECTED``."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    for cid in (MAIN, HEALTHY, EMPTY, REV3):
        if (root / cid).exists():
            shutil.rmtree(root / cid)

    exp: dict = {}

    # ---- main cluster
    c = root / MAIN
    n_log_lines = 0
    n_files = 0
    for name, text in _driver_logs().items():
        _write(c / "driver" / name, text, gz=name.endswith(".gz"))
        n_log_lines += _count_lines(text)
        n_files += 1
    for rel, text in _executor_logs().items():
        _write(c / "executor" / rel, text)
        n_log_lines += _count_lines(text)
        n_files += 1
    rolled, active, stats_a = _context_a_events()
    _write(c / "eventlog" / HASH_A / CTX_A / "eventlog-2026-10-06--18-00.gz", rolled, gz=True)
    _write(c / "eventlog" / HASH_A / CTX_A / "eventlog", active)
    text_b, stats_b = _context_b_events()
    _write(c / "eventlog" / HASH_B / CTX_B / "eventlog", text_b)
    n_files += 3
    exp[MAIN] = {
        "files": n_files,
        "log_lines": n_log_lines,
        "contexts": {CTX_A: APP_A, CTX_B: APP_B},
        "tasks": {CTX_A: stats_a["tasks"], CTX_B: stats_b["tasks"]},
        "failed_tasks": {CTX_A: stats_a["failed_tasks"], CTX_B: 0},
        # stage attempts: A = 0,1,2,3,4,5.0,6.0,5.1,6.1 ; B = 0,1,2
        "stages": {CTX_A: 9, CTX_B: 3},
        "spark_jobs": {CTX_A: 5, CTX_B: 2},
        "sql_queries": {CTX_A: 4, CTX_B: 1},
        "executors": {CTX_A: 4, CTX_B: 2},
        "malformed_lines": {CTX_A: 1, CTX_B: 0},
        "status": "failed",
    }

    # ---- healthy cluster
    h = root / HEALTHY
    driver, execs, ev_text, stats_h = _healthy()
    n_log_lines = 0
    for name, text in driver.items():
        _write(h / "driver" / name, text)
        n_log_lines += _count_lines(text)
    for rel, text in execs.items():
        _write(h / "executor" / rel, text)
        n_log_lines += _count_lines(text)
    _write(h / "eventlog" / HASH_H / CTX_H / "eventlog", ev_text)
    exp[HEALTHY] = {
        "files": len(driver) + len(execs) + 1,
        "log_lines": n_log_lines,
        "contexts": {CTX_H: APP_H},
        "tasks": {CTX_H: stats_h["tasks"]},
        "failed_tasks": {CTX_H: 0},
        "stages": {CTX_H: 2},
        "spark_jobs": {CTX_H: 1},
        "sql_queries": {CTX_H: 1},
        "executors": {CTX_H: 2},
        "malformed_lines": {CTX_H: 0},
        "status": "succeeded",
    }

    # ---- empty cluster (serverless: nothing delivered)
    (root / EMPTY).mkdir(parents=True, exist_ok=True)
    exp[EMPTY] = {"files": 0, "log_lines": 0, "status": "unknown"}

    # ---- Revision 3 cluster
    exp[REV3] = _rev3(root)

    EXPECTED.clear()
    EXPECTED.update(exp)
    return {"main": MAIN, "healthy": HEALTHY, "empty": EMPTY, "rev3": REV3}


if __name__ == "__main__":
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent / "clusters"
    ids = generate(out)
    for key, cid in ids.items():
        files = [p for p in (out / cid).rglob("*") if p.is_file()]
        print(f"{key:8s} {cid}: {len(files)} files, {sum(p.stat().st_size for p in files):,} bytes")
