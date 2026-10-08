"""Write a synthetic Databricks cluster log folder for the README screenshots and for trying the analyzer.

Everything here is made up: a small shop's nightly ETL on one 8-worker cluster. Tables: shop.raw.customer_updates,
shop.raw.orders_landing, shop.sales.customer, shop.sales.orders, shop.staging.order_lines_new,
shop.sales.order_line_item and shop.sales.daily_revenue. Four Databricks job runs overlap, so some wait for cores; one
join is skewed and spills; one task fails on a Python error in the job's own code and is retried; order_line_item has files over 1 GB and is read whole.

    python scripts/make_demo_cluster.py ./demo_logs
    dbx-log-analyzer ui --output ./demo_out --cache ./demo_cache      # then analyze ./demo_logs, cluster 0112-020000-demo0001
"""
from __future__ import annotations

import heapq
import json
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

CLUSTER = "0112-020000-demo0001"
CTX = "4400112233445566778"
APP = "app-20260112020000-0000"
SQL = "org.apache.spark.sql.execution.ui."
GB, MB = 1 << 30, 1 << 20
EXECUTORS = 8
CORES = 4
T0 = int(datetime(2026, 1, 12, 2, 0, tzinfo=timezone.utc).timestamp() * 1000)
rng = random.Random(7)

_acc = [10_000]


def acc_id() -> int:
    _acc[0] += 1
    return _acc[0]


# ------------------------------------------------------------------ plan nodes with SQL metrics (accumulators)
class Node:
    def __init__(self, name: str, simple: str, metrics: list[tuple[str, str]] = (), children: list["Node"] = ()):
        self.name, self.simple, self.children = name, simple, list(children)
        self.metrics = {n: (acc_id(), t) for n, t in metrics}

    def info(self) -> dict:
        return {"nodeName": self.name, "simpleString": self.simple, "children": [c.info() for c in self.children], "metadata": {},
                "metrics": [{"name": n, "accumulatorId": a, "metricType": t} for n, (a, t) in self.metrics.items()]}

    def text(self, depth: int = 0) -> list[str]:
        pad = "" if depth == 0 else "   " * (depth - 1) + "+- "
        out = [pad + self.simple]
        for c in self.children:
            out += c.text(depth + 1)
        return out


SCAN_M = [("number of output rows", "sum"), ("number of files read", "sum"), ("number of files pruned", "sum"),
          ("size of files read", "size"), ("number of bytes pruned", "size"), ("number of partition columns", "sum"),
          ("size of the smallest file read", "size"), ("size of the largest file read", "size"), ("scan time", "timing")]


def scan(table: str, cols: str, fmt: str = "parquet") -> Node:
    return Node(f"Scan {fmt} {table}", f"Scan {fmt} {table}[{cols}] Batched: true, DataFilters: [], Format: {fmt.title()}", SCAN_M)


def exchange(keys: str, child: Node) -> Node:
    return Node("Exchange", f"Exchange hashpartitioning({keys}, 200), ENSURE_REQUIREMENTS",
                [("shuffle records written", "sum"), ("shuffle bytes written", "size")], [child])


# ------------------------------------------------------------------ the work: runs -> queries -> stages
# a stage: tasks, rows and bytes per task, what it reads (scan node / shuffle parents) and writes
def stage(key, tasks, ms, *, scan_node=None, table_bytes=0, files=0, pruned_files=0, pruned_bytes=0, part_cols=0, rows=0,
          parents=(), shuffle_out=0, rows_out=0, write_node=None, write_rows=None, skew=1.0, spill=0, fail_one=False):
    return dict(key=key, tasks=tasks, ms=ms, scan=scan_node, table_bytes=table_bytes, files=files, pruned_files=pruned_files,
                pruned_bytes=pruned_bytes, part_cols=part_cols, rows=rows, parents=list(parents), shuffle_out=shuffle_out,
                rows_out=rows_out, write=write_node, write_rows=write_rows or {}, skew=skew, spill=spill, fail_one=fail_one)


def q_load_customers(day: str):
    src = scan("shop.raw.customer_updates", "customer_id#1,name#2,email#3,segment#4,updated_at#5", "json")
    tgt = scan("shop.sales.customer", "customer_id#11,name#12,email#13,segment#14,updated_at#15")
    join = Node("SortMergeJoin", "SortMergeJoin [customer_id#1], [customer_id#11], FullOuter",
                [("number of output rows", "sum"), ("spill size", "size")], [exchange("customer_id#1", src), exchange("customer_id#11", tgt)])
    merge = Node("Execute MergeIntoCommandEdge", "Execute MergeIntoCommandEdge shop.sales.customer",
                 [("number of source rows", "sum"), ("number of updated rows", "sum"), ("number of inserted rows", "sum"),
                  ("number of target rows rewritten unmodified", "sum"), ("number of output rows", "sum")], [join])
    stages = [
        stage("src", 16, 9_000, scan_node=src, table_bytes=2 * GB, files=48, rows=1_900_000, shuffle_out=int(1.4 * GB), rows_out=1_900_000),
        stage("tgt", 48, 14_000, scan_node=tgt, table_bytes=6 * GB, files=96, rows=12_400_000, shuffle_out=int(4.1 * GB), rows_out=12_400_000),
        stage("merge", 64, 21_000, parents=["src", "tgt"], write_node=merge,
              write_rows={"number of source rows": 1_900_000, "number of updated rows": 1_450_000, "number of inserted rows": 450_000,
                          "number of target rows rewritten unmodified": 10_950_000, "number of output rows": 12_850_000}),
    ]
    return dict(desc=f"MERGE INTO shop.sales.customer USING customer_updates ({day})", root=merge, stages=stages)


def q_load_orders():
    src = scan("shop.raw.orders_landing", "order_id#21,customer_id#22,order_ts#23,status#24,total#25", "json")
    w = Node("Execute WriteIntoDeltaCommand", "Execute WriteIntoDeltaCommand shop.sales.orders, Append",
             [("number of output rows", "sum"), ("number of written files", "sum"), ("written output", "size")],
             [exchange("order_date#26", src)])
    stages = [
        stage("land", 96, 26_000, scan_node=src, table_bytes=24 * GB, files=960, rows=38_000_000, shuffle_out=11 * GB, rows_out=38_000_000),
        stage("write", 64, 30_000, parents=["land"], write_node=w, write_rows={"number of output rows": 38_000_000}),
    ]
    return dict(desc="INSERT INTO shop.sales.orders SELECT * FROM orders_landing", root=w, stages=stages)


def q_order_lines_new():
    o = scan("shop.sales.orders", "order_id#31,customer_id#32,order_ts#33,lines#34")
    c = scan("shop.sales.customer", "customer_id#41,segment#42")
    j = Node("SortMergeJoin", "SortMergeJoin(skew=true) [customer_id#32], [customer_id#41], Inner",
             [("number of output rows", "sum"), ("spill size", "size"), ("peak memory", "size")],
             [exchange("customer_id#32", o), exchange("customer_id#41", c)])
    w = Node("Execute WriteIntoDeltaCommand", "Execute WriteIntoDeltaCommand shop.staging.order_lines_new, Overwrite",
             [("number of output rows", "sum"), ("number of written files", "sum")], [j])
    stages = [
        stage("orders", 400, 24_000, scan_node=o, table_bytes=182 * GB, files=1_420, pruned_files=980, pruned_bytes=126 * GB, part_cols=1,
              rows=96_000_000, shuffle_out=31 * GB, rows_out=96_000_000),
        stage("cust", 40, 8_000, scan_node=c, table_bytes=6 * GB, files=96, rows=12_850_000, shuffle_out=int(1.2 * GB), rows_out=12_850_000),
        stage("join", 200, 34_000, parents=["orders", "cust"], write_node=w, write_rows={"number of output rows": 214_000_000},
              skew=14.0, spill=9 * GB),
    ]
    return dict(desc="CREATE OR REPLACE TABLE shop.staging.order_lines_new AS SELECT ... FROM orders JOIN customer", root=w, stages=stages)


def q_merge_line_items():
    src = scan("shop.staging.order_lines_new", "order_id#51,line_no#52,sku#53,qty#54,price#55,segment#56")
    tgt = scan("shop.sales.order_line_item", "order_id#61,line_no#62,sku#63,qty#64,price#65")
    j = Node("SortMergeJoin", "SortMergeJoin [order_id#51, line_no#52], [order_id#61, line_no#62], LeftOuter",
             [("number of output rows", "sum"), ("spill size", "size")],
             [exchange("order_id#51, line_no#52", src), exchange("order_id#61, line_no#62", tgt)])
    merge = Node("Execute MergeIntoCommandEdge", "Execute MergeIntoCommandEdge shop.sales.order_line_item",
                 [("number of source rows", "sum"), ("number of updated rows", "sum"), ("number of inserted rows", "sum"),
                  ("number of target rows rewritten unmodified", "sum"), ("number of output rows", "sum")], [j])
    stages = [
        stage("src", 120, 12_000, scan_node=src, table_bytes=38 * GB, files=200, rows=214_000_000, shuffle_out=29 * GB, rows_out=214_000_000),
        stage("tgt", 300, 41_000, scan_node=tgt, table_bytes=430 * GB, files=300, rows=1_640_000_000, shuffle_out=212 * GB,
              rows_out=1_640_000_000),
        stage("merge", 200, 52_000, parents=["src", "tgt"], write_node=merge, spill=22 * GB, fail_one=True,
              write_rows={"number of source rows": 214_000_000, "number of updated rows": 31_000_000, "number of inserted rows": 183_000_000,
                          "number of target rows rewritten unmodified": 1_209_000_000, "number of output rows": 1_423_000_000}),
    ]
    return dict(desc="MERGE INTO shop.sales.order_line_item USING order_lines_new", root=merge, stages=stages)


def q_daily_revenue():
    li = scan("shop.sales.order_line_item", "order_id#71,sku#72,qty#73,price#74")
    agg = Node("HashAggregate", "HashAggregate(keys=[order_date#75], functions=[sum(qty#73 * price#74)])", [("number of output rows", "sum")],
               [exchange("order_date#75", li)])
    w = Node("Execute ReplaceTableAsSelect", "Execute ReplaceTableAsSelect shop.sales.daily_revenue",
             [("number of output rows", "sum"), ("number of written files", "sum")], [agg])
    stages = [
        stage("lines", 300, 33_000, scan_node=li, table_bytes=452 * GB, files=310, rows=1_850_000_000, shuffle_out=int(0.6 * GB),
              rows_out=4_200_000),
        stage("agg", 8, 6_000, parents=["lines"], write_node=w, write_rows={"number of output rows": 3_650}),
    ]
    return dict(desc="CREATE OR REPLACE TABLE shop.sales.daily_revenue AS SELECT order_date, sum(qty * price) ...", root=w, stages=stages)


RUNS = [  # (job id, run id, job name, start offset s, queries)
    ("501", "880101", "load_customers", 0, [q_load_customers("2026-01-11")]),
    ("502", "880102", "load_orders", 40, [q_load_orders()]),
    ("503", "880103", "build_order_line_item", 120, [q_order_lines_new(), q_merge_line_items()]),
    ("504", "880104", "daily_revenue", 2_400, [q_daily_revenue()]),
    ("501", "880105", "load_customers", 3_900, [q_load_customers("2026-01-12 late files")]),
]


# ------------------------------------------------------------------ scheduling on 8 x 4 cores
HOSTS = {str(i): f"10.20.0.{11 + i}" for i in range(EXECUTORS)}


def build() -> list[dict]:
    slots = [(T0 + 60_000, e, c) for e in range(EXECUTORS) for c in range(CORES)]  # executors up a minute after start
    heapq.heapify(slots)
    events: list[tuple[int, int, dict]] = []
    seq = [0]

    def emit(ts: int, ev: dict) -> None:
        seq[0] += 1
        events.append((ts, seq[0], ev))

    # ready queue of (ready time, order, run index, query index)
    sid = [0]
    jid = [0]
    tid = [0]
    exec_id = [0]
    ready = [(T0 + off * 1000, i, i, 0) for i, (_, _, _, off, _) in enumerate(RUNS)]
    heapq.heapify(ready)
    while ready:
        t_ready, _, ri, qi = heapq.heappop(ready)
        job_id, run_id, name, _, queries = RUNS[ri]
        q = queries[qi]
        x = exec_id[0]
        exec_id[0] += 1
        props = {"spark.job.description": q["desc"], "spark.sql.execution.id": str(x), "spark.sql.execution.root.id": str(x),
                 "spark.databricks.job.id": job_id, "spark.databricks.job.runId": run_id,
                 "spark.jobGroup.id": f"{CLUSTER}_job-{job_id}-run-{run_id}-action-{x}",
                 "spark.databricks.notebook.path": f"/Workspace/Shop/etl/{name}", "callSite.short": "sql at <cell>:1", "user": "etl@shop.example"}
        plan = "== Physical Plan ==\nAdaptiveSparkPlan isFinalPlan=true\n+- " + "\n   ".join(q["root"].text(0))
        info = {"nodeName": "AdaptiveSparkPlan", "simpleString": "AdaptiveSparkPlan isFinalPlan=true", "children": [q["root"].info()],
                "metadata": {}, "metrics": []}
        emit(t_ready, {"Event": SQL + "SparkListenerSQLExecutionStart", "executionId": x, "rootExecutionId": x, "description": q["desc"],
                       "details": "", "physicalPlanDescription": plan, "sparkPlanInfo": info, "time": t_ready, "modifiedConfigs": {}, "jobTags": []})
        ids = {s["key"]: None for s in q["stages"]}
        ends: dict[str, int] = {}
        t = t_ready + 800
        # one Spark job per stage that writes (and its parents), one per scan otherwise: scans first, then the rest
        for s in q["stages"]:
            sid[0] += 1
            ids[s["key"]] = sid[0]
        for s in q["stages"]:
            st_id = ids[s["key"]]
            parents = [ids[p] for p in s["parents"]]
            submit = max([t] + [ends[p] + 300 for p in s["parents"]])
            jid[0] += 1
            scopes = ([s["scan"].name] if s["scan"] else []) + ["WholeStageCodegen (1)"] + (["Exchange"] if s["shuffle_out"] else []) + \
                     (["WriteFiles"] if s["write"] else [])
            sinfo = {"Stage ID": st_id, "Stage Attempt ID": 0, "Stage Name": f"{s['key']} at <cell>:1", "Number of Tasks": s["tasks"],
                     "RDD Info": [{"RDD ID": st_id * 10 + i, "Name": "MapPartitionsRDD", "Scope": json.dumps({"id": str(i), "name": n}),
                                   "Callsite": "sql at <cell>:1", "Parent IDs": [], "Storage Level": {"Use Disk": False, "Use Memory": False,
                                   "Deserialized": False, "Replication": 1}, "Barrier": False, "DeterministicLevel": "DETERMINATE",
                                   "Number of Partitions": s["tasks"], "Number of Cached Partitions": 0, "Memory Size": 0, "Disk Size": 0}
                                  for i, n in enumerate(scopes)],
                     "Parent IDs": parents, "Details": "", "Accumulables": [], "Resource Profile Id": 0}
            jprops = dict(props)
            emit(submit, {"Event": "SparkListenerJobStart", "Job ID": jid[0], "Submission Time": submit, "Stage Infos": [sinfo],
                          "Stage IDs": [st_id], "Properties": jprops})
            emit(submit, {"Event": "SparkListenerStageSubmitted", "Stage Info": dict(sinfo, **{"Submission Time": submit}), "Properties": jprops})
            n = s["tasks"]
            weights = [rng.uniform(0.7, 1.3) for _ in range(n)]
            if s["skew"] > 1:
                weights[rng.randrange(n)] = s["skew"]
            tot = sum(weights)
            end = submit
            parents_out = sum(next(p for p in q["stages"] if p["key"] == k)["shuffle_out"] for k in s["parents"])
            parents_rows = sum(next(p for p in q["stages"] if p["key"] == k)["rows_out"] for k in s["parents"])
            failed_idx = rng.randrange(n) if s["fail_one"] else None
            for i, w in enumerate(weights):
                share = w / tot
                ms = int(s["ms"] * w * rng.uniform(0.9, 1.1))
                attempts = [0, 1] if i == failed_idx else [0]
                for a in attempts:
                    free, e, c = heapq.heappop(slots)
                    launch = max(submit + 50, free)
                    run = ms // 3 if a == 0 and i == failed_idx else ms
                    finish = launch + run + 120
                    heapq.heappush(slots, (finish + 20, e, c))
                    tid[0] += 1
                    host = HOSTS[str(e)]
                    ok = not (a == 0 and i == failed_idx)
                    upd = []
                    if s["scan"] and ok:
                        m = s["scan"].metrics
                        upd += [(m["number of output rows"][0], int(s["rows"] * share)), (m["number of files read"][0], max(1, round((s["files"] - s["pruned_files"]) * share))),
                                (m["number of files pruned"][0], round(s["pruned_files"] * share)),
                                (m["size of files read"][0], int((s["table_bytes"] - s["pruned_bytes"]) * share)),
                                (m["number of bytes pruned"][0], int(s["pruned_bytes"] * share)), (m["scan time"][0], run * 6 // 10)]
                        if i == 0:  # recorded once per scan (on the driver), not summed over tasks
                            avg = s["table_bytes"] // max(1, s["files"])
                            upd += [(m["number of partition columns"][0], s["part_cols"]),
                                    (m["size of the smallest file read"][0], avg // 6), (m["size of the largest file read"][0], int(avg * 2.3))]
                    if s["write"] and ok:
                        for k2, v in s["write_rows"].items():
                            upd.append((s["write"].metrics[k2][0], int(v * share)))
                    in_bytes = int((s["table_bytes"] - s["pruned_bytes"]) * share * 0.55) if s["scan"] else 0
                    in_rows = int(s["rows"] * share) if s["scan"] else 0
                    sh_read = int(parents_out * share)
                    sh_rows = int(parents_rows * share)
                    sh_w = int(s["shuffle_out"] * share)
                    # rows the stage wrote to its table: the write node's output rows (a join can put out more than came in)
                    out_rows = int(s["write_rows"].get("number of output rows", parents_rows) * share)
                    reason = {"Reason": "Success"} if ok else {
                        "Reason": "ExceptionFailure", "Class Name": "org.apache.spark.api.python.PythonException",
                        "Description": "PythonException: IndexError: list index out of range (normalise_address)", "Stack Trace": [], "Full Stack Trace": "\n".join(TRACEBACK), "Accumulator Updates": []}
                    emit(launch, {"Event": "SparkListenerTaskStart", "Stage ID": st_id, "Stage Attempt ID": 0,
                                  "Task Info": {"Task ID": tid[0], "Index": i, "Attempt": a, "Partition ID": i, "Launch Time": launch,
                                                "Executor ID": str(e), "Host": host, "Locality": "PROCESS_LOCAL", "Speculative": False,
                                                "Getting Result Time": 0, "Finish Time": 0, "Failed": False, "Killed": False, "Accumulables": []}})
                    spill = int(s["spill"] * share) if s["spill"] and ok else 0
                    emit(finish, {"Event": "SparkListenerTaskEnd", "Stage ID": st_id, "Stage Attempt ID": 0,
                                  "Task Type": "ShuffleMapTask" if s["shuffle_out"] else "ResultTask", "Task End Reason": reason,
                                  "Task Info": {"Task ID": tid[0], "Index": i, "Attempt": a, "Partition ID": i, "Launch Time": launch,
                                                "Executor ID": str(e), "Host": host, "Locality": "PROCESS_LOCAL", "Speculative": False,
                                                "Getting Result Time": 0, "Finish Time": finish, "Failed": not ok, "Killed": False,
                                                "Accumulables": [{"ID": aid, "Update": str(v), "Value": str(v), "Internal": False,
                                                                  "Count Failed Values": False, "Metadata": "sql"} for aid, v in upd]},
                                  "Task Executor Metrics": {"JVMHeapMemory": 3 * GB, "OnHeapExecutionMemory": 2 * GB},
                                  "Task Metrics": {
                                      "Executor Deserialize Time": 15, "Executor Deserialize CPU Time": 9_000_000, "Executor Run Time": run,
                                      "Executor CPU Time": run * 700_000, "Peak Execution Memory": 2 * GB if spill else 512 * MB,
                                      "Result Size": 4_000, "JVM GC Time": run // 25, "Result Serialization Time": 1,
                                      "Memory Bytes Spilled": spill * 3, "Disk Bytes Spilled": spill,
                                      "Shuffle Read Metrics": {"Remote Blocks Fetched": 40 if sh_read else 0, "Local Blocks Fetched": 6 if sh_read else 0,
                                                               "Fetch Wait Time": run // 20 if sh_read else 0, "Remote Bytes Read": sh_read * 7 // 8,
                                                               "Remote Bytes Read To Disk": 0, "Local Bytes Read": sh_read // 8,
                                                               "Total Records Read": sh_rows, "Remote Requests Duration": 5, "Push Based Shuffle": {}},
                                      "Shuffle Write Metrics": {"Shuffle Bytes Written": sh_w, "Shuffle Write Time": run * 30_000,
                                                                "Shuffle Records Written": int(s["rows_out"] * share) if sh_w else 0},
                                      "Input Metrics": {"Bytes Read": in_bytes, "Records Read": in_rows},
                                      "Output Metrics": {"Bytes Written": int(out_rows * 40) if s["write"] and ok else 0,
                                                         "Records Written": out_rows if s["write"] and ok else 0},
                                      "Updated Blocks": []}})
                    end = max(end, finish)
            ends[s["key"]] = end
            emit(end + 10, {"Event": "SparkListenerStageCompleted", "Stage Info": dict(sinfo, **{"Submission Time": submit, "Completion Time": end + 10})})
            emit(end + 20, {"Event": "SparkListenerJobEnd", "Job ID": jid[0], "Completion Time": end + 20, "Job Result": {"Result": "JobSucceeded"}})
            if not s["parents"]:
                t = submit + 200  # scans of one query start together
        q_end = max(ends.values()) + 1_500
        emit(q_end, {"Event": SQL + "SparkListenerSQLExecutionEnd", "executionId": x, "time": q_end})
        if qi + 1 < len(queries):
            heapq.heappush(ready, (q_end + 2_000, ri * 100 + qi + 1, ri, qi + 1))
    last = max(e[0] for e in events)
    head = [{"Event": "SparkListenerLogStart", "Spark Version": "3.5.0"},
            {"Event": "SparkListenerResourceProfileAdded", "Resource Profile Id": 0,
             "Executor Resource Requests": {"cores": {"Resource Name": "cores", "Amount": CORES, "Discovery Script": "", "Vendor": ""}},
             "Task Resource Requests": {"cpus": {"Resource Name": "cpus", "Amount": 1.0}}},
            {"Event": "SparkListenerEnvironmentUpdate", "JVM Information": {"Java Version": "17.0.12", "Scala Version": "version 2.12.15"},
             "Spark Properties": {"spark.databricks.clusterUsageTags.clusterId": CLUSTER, "spark.databricks.clusterUsageTags.clusterName": "shop-nightly-etl",
                                  "spark.executor.memory": "24g", "spark.sql.adaptive.enabled": "true",
                                  "spark.databricks.clusterUsageTags.sparkVersion": "15.4.x-scala2.12"},
             "Hadoop Properties": {}, "System Properties": {}, "Classpath Entries": {}},
            {"Event": "SparkListenerApplicationStart", "App Name": "Databricks Shell", "App ID": APP, "Timestamp": T0, "User": "root"}]
    for e, host in HOSTS.items():
        ts = T0 + 45_000 + int(e) * 700
        head.append({"Event": "SparkListenerExecutorAdded", "Timestamp": ts, "Executor ID": e,
                     "Executor Info": {"Host": host, "Total Cores": CORES, "Log Urls": {}, "Attributes": {}, "Resources": {},
                                       "Resource Profile Id": 0, "Registration Time": ts, "Request Time": ts - 1000}})
        head.append({"Event": "SparkListenerBlockManagerAdded", "Block Manager ID": {"Executor ID": e, "Host": host, "Port": 41234},
                     "Maximum Memory": 12 * GB, "Timestamp": ts, "Maximum Onheap Memory": 12 * GB, "Maximum Offheap Memory": 0})
    tail = [{"Event": "SparkListenerApplicationEnd", "Timestamp": last + 600_000}]
    return head + [e for _, _, e in sorted(events, key=lambda x: (x[0], x[1]))] + tail


def _ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%y/%m/%d %H:%M:%S")


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}+0000"


TRACEBACK = [  # the one task that fails and is retried: a bad address in the job's own code
    "org.apache.spark.api.python.PythonException: Traceback (most recent call last):",
    '  File "/databricks/spark/python/pyspark/worker.py", line 1876, in main',
    "    process()",
    '  File "/Workspace/Shop/etl/build_order_line_item", line 42, in normalise_address',
    '    return addr.split(",")[1].strip().upper()',
    "IndexError: list index out of range",
    "\tat org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:642)",
    "\tat org.apache.spark.sql.execution.python.PythonArrowOutput$$anon$1.read(PythonArrowOutput.scala:118)",
    "\tat org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:621)",
]


def driver_log(evs: list[dict]) -> str:
    """The driver's log4j: start-up, each query, job and stage, the failed task with its Python traceback, shut-down."""
    end = evs[-1]["Timestamp"]
    out: list[tuple[int, str]] = [(T0, f"INFO DriverDaemon: Started Spark driver for cluster {CLUSTER}"),
                                  (T0 + 1_000, "INFO SparkContext: Running Spark version 3.5.0"),
                                  (T0 + 2_000, "INFO DatabricksILoop: Starting DatabricksILoop"),
                                  (T0 + 30_000, f"INFO DriverCorral: Cluster shop-nightly-etl: {EXECUTORS} workers requested")]
    for e in evs:
        k = e["Event"]
        if k.endswith("SQLExecutionStart"):
            out.append((e["time"], f"INFO SQLExecution: Execution {e['executionId']} started: {e['description'][:90]}"))
        elif k.endswith("SQLExecutionEnd"):
            out.append((e["time"], f"INFO SQLExecution: Execution {e['executionId']} finished"))
        elif k == "SparkListenerJobStart":
            out.append((e["Submission Time"], f"INFO DAGScheduler: Got job {e['Job ID']} (sql at <cell>:1) with stages {e['Stage IDs']}"))
        elif k == "SparkListenerStageSubmitted":
            s = e["Stage Info"]
            out.append((s["Submission Time"], f"INFO DAGScheduler: Submitting {s['Number of Tasks']} missing tasks from stage "
                                              f"{s['Stage ID']} ({s['Stage Name']})"))
        elif k == "SparkListenerStageCompleted":
            s = e["Stage Info"]
            secs = (s["Completion Time"] - s["Submission Time"]) / 1000
            out.append((s["Completion Time"], f"INFO DAGScheduler: Stage {s['Stage ID']} ({s['Stage Name']}) finished in {secs:.1f} s"))
        elif k == "SparkListenerJobEnd":
            out.append((e["Completion Time"], f"INFO DAGScheduler: Job {e['Job ID']} finished"))
        elif k == "SparkListenerTaskEnd" and e["Task Info"]["Failed"]:
            ti = e["Task Info"]
            out.append((ti["Finish Time"], f"WARN TaskSetManager: Lost task {ti['Index']}.0 in stage {e['Stage ID']}.0 (TID {ti['Task ID']}) "
                                           f"({ti['Host']} executor {ti['Executor ID']}): " + "\n".join(TRACEBACK)))
            out.append((ti["Finish Time"] + 5, f"INFO TaskSetManager: Starting task {ti['Index']}.1 in stage {e['Stage ID']}.0 (retry)"))
    out.append((end, "INFO DriverDaemon: Shutting down: the cluster is terminating (INACTIVITY)"))
    out.sort(key=lambda x: x[0])
    return "".join(f"{_ts(ms)} {line}\n" for ms, line in out)


def executor_logs(evs: list[dict]) -> dict[str, tuple[str, str]]:
    """Each executor's stderr (log4j: tasks run and finished, spills, the failed task) and stdout (JVM GC lines)."""
    err: dict[str, list[tuple[int, str]]] = {e: [] for e in HOSTS}
    gc: dict[str, list[tuple[int, int]]] = {e: [] for e in HOSTS}  # (time, pause ms)
    for e, host in HOSTS.items():
        t = T0 + 45_000 + int(e) * 700
        err[e] += [(t, f"INFO CoarseGrainedExecutorBackend: Started daemon with process name: executor {e} on {host}"),
                   (t + 400, f"INFO Executor: Starting executor ID {e} on host {host} with {CORES} cores"),
                   (t + 900, "INFO MemoryStore: MemoryStore started with capacity 12.0 GiB")]
    last = T0
    for ev in evs:
        if ev["Event"] == "SparkListenerTaskStart":
            ti = ev["Task Info"]
            err[ti["Executor ID"]].append((ti["Launch Time"], f"INFO Executor: Running task {ti['Index']}.{ti['Attempt']} in stage "
                                                             f"{ev['Stage ID']}.0 (TID {ti['Task ID']})"))
        elif ev["Event"] == "SparkListenerTaskEnd":
            ti, m = ev["Task Info"], ev["Task Metrics"]
            ex, fin = ti["Executor ID"], ti["Finish Time"]
            last = max(last, fin)
            if ti["Failed"]:
                err[ex].append((fin - 10, f"ERROR Executor: Exception in task {ti['Index']}.0 in stage {ev['Stage ID']}.0 "
                                          f"(TID {ti['Task ID']})\n" + "\n".join(TRACEBACK)))
                continue
            spill = m["Disk Bytes Spilled"]
            if spill:  # one line per ~1 GB spilled, spread over the task
                n = max(1, round(spill / GB))
                for i in range(n):
                    at = ti["Launch Time"] + (fin - ti["Launch Time"]) * (i + 1) // (n + 1)
                    err[ex].append((at, f"INFO UnsafeExternalSorter: Thread {60 + int(ex)} spilling sort data of "
                                        f"{spill * 3 / n / GB:.1f} GiB to disk ({i} times so far)"))
            err[ex].append((fin, f"INFO Executor: Finished task {ti['Index']}.{ti['Attempt']} in stage {ev['Stage ID']}.0 "
                                 f"(TID {ti['Task ID']}). {m['Result Size']} bytes result sent to driver"))
            gc[ex].append((fin - 5, max(4, m["JVM GC Time"] // 40)))
    out = {}
    for e in HOSTS:
        stdout, n = [], 0
        for at, pause in sorted(gc[e])[::6]:  # a young pause about every sixth task
            n += 1
            stdout.append(f"[{_iso(at)}][{(at - T0) / 1000:.3f}s][info][gc] GC({n}) Pause Young (Normal) (G1 Evacuation Pause) "
                          f"{rng.uniform(7.5, 10.5):.0f}G->{rng.uniform(3.0, 4.5):.1f}G(24G) {pause:.3f}ms")
        lines = sorted(err[e], key=lambda x: x[0]) + [(last + 1_000, "INFO CoarseGrainedExecutorBackend: Driver commanded a shutdown")]
        out[e] = ("".join(f"{_ts(ms)} {line}\n" for ms, line in lines), "\n".join(stdout) + "\n")
    return out


def main(out: Path) -> None:
    evs = build()
    d = out / CLUSTER
    (d / "eventlog" / f"{CLUSTER}_10_20_0_5" / CTX).mkdir(parents=True, exist_ok=True)
    (d / "eventlog" / f"{CLUSTER}_10_20_0_5" / CTX / "eventlog").write_text("\n".join(json.dumps(e) for e in evs) + "\n", encoding="utf-8")
    (d / "driver").mkdir(parents=True, exist_ok=True)
    (d / "driver" / "log4j-active.log").write_text(driver_log(evs), encoding="utf-8")
    for e, (stderr, stdout) in executor_logs(evs).items():
        (d / "executor" / APP / e).mkdir(parents=True, exist_ok=True)
        (d / "executor" / APP / e / "stderr").write_text(stderr, encoding="utf-8")
        (d / "executor" / APP / e / "stdout").write_text(stdout, encoding="utf-8")
    tasks = sum(1 for e in evs if e["Event"] == "SparkListenerTaskEnd")
    print(f"{d}: {len(evs):,} events, {tasks:,} tasks")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path("demo_logs"))
