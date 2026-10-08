"""Tasks queued on a full cluster, autoscaling, DataFrame caches, count-only queries, DDL loops, reads by source,
init scripts and names from the code (made-up numbers)."""

import pandas as pd

from databricks_cluster_log_analyzer.analysis.capacity import capacity_findings, core_use, makespan
from databricks_cluster_log_analyzer.analysis.initscripts import init_scripts
from databricks_cluster_log_analyzer.analysis.workload import (cache_size, count_only, ddl_shape, read_split,
                                                               workload_findings)
from databricks_cluster_log_analyzer.config import load_rules
from databricks_cluster_log_analyzer.parsing.eventlog import readable_description, statement_label

S = 1000
T0 = 1_790_000_000_000


def task(tid, sid, launch, dur, ex="1", run="r1", attempt=0, failed=False, reason="Success", error=None):
    return {"spark_context_id": "c", "stage_id": sid, "stage_attempt": 0, "task_id": tid, "task_attempt": attempt,
            "executor_id": ex, "launch_time": float(launch), "finish_time": float(launch + dur), "task_ms": float(dur),
            "failed": failed, "end_reason": reason, "error": error, "run_key": run}


def test_capacity_bound_counts_every_wave():
    rules = load_rules()
    # 4 cores; one stage of 40 tasks of 60 s each: ten waves, the first starts at once (no wait before the first task)
    tasks = [task(i, 1, T0 + (i // 4) * 60 * S, 60 * S, ex=str(i % 4 // 2)) for i in range(40)]
    stages = [{"spark_context_id": "c", "stage_id": 1, "stage_attempt": 0, "start_time": T0, "end_time": T0 + 600 * S,
               "status": "succeeded", "duration_ms": 600 * S, "run_key": "r1"}]
    ex = [{"spark_context_id": "c", "executor_id": str(i), "cores": 2, "added_time": T0 - 60 * S, "removed_time": None}
          for i in range(2)]
    runs = [{"run_key": "r1", "spark_context_id": "c", "start_time": T0, "end_time": T0 + 600 * S, "program": "load"}]
    f = capacity_findings("x", pd.DataFrame(tasks), stages, ex, runs, [], [], rules)
    cb = [r for r in f if r["category"] == "capacity_bound"]
    assert len(cb) == 1 and cb[0]["severity"] == "high"
    assert cb[0]["evidence"].startswith("Tasks queued on a full cluster for 9 m 0 s of its 10 m 0 s (90%): every core busy with 4-36")
    assert "behind its own tasks" in cb[0]["evidence"]


def test_autoscale_lag_and_removed_mid_work():
    rules = load_rules()
    # 2 cores from the start, 6 more arrive 5 minutes after 50 tasks queued
    tasks = [task(i, 1, T0 + (i // 2) * 30 * S, 30 * S, ex="1") for i in range(20)]
    tasks += [task(100 + i, 1, T0 + 300 * S + (i // 8) * 30 * S, 30 * S, ex=str(2 + i % 3)) for i in range(30)]
    stages = [{"spark_context_id": "c", "stage_id": 1, "stage_attempt": 0, "start_time": T0, "end_time": T0 + 450 * S,
               "status": "succeeded", "duration_ms": 450 * S, "run_key": "r1", "sql_execution_id": 3}]
    ex = [{"spark_context_id": "c", "executor_id": "1", "cores": 2, "added_time": T0 - S, "removed_time": None}]
    ex += [{"spark_context_id": "c", "executor_id": str(e), "cores": 2, "added_time": T0 + 300 * S,
            "removed_time": T0 + 440 * S, "removal_category": "autoscale"} for e in (2, 3, 4)]
    tasks += [task(200, 2, T0 + 450 * S, S, ex="1", failed=True, reason="FetchFailed",
                   error="FetchFailed(BlockManagerId(3, 10.0.0.9, 4048), shuffleId=1, mapIndex=4)")]
    stages += [{"spark_context_id": "c", "stage_id": 2, "stage_attempt": 0, "start_time": T0 + 445 * S,
                "end_time": T0 + 500 * S, "status": "succeeded", "duration_ms": 55 * S, "run_key": "r1"}]
    ci = [{"spark_context_id": "c", "min_workers": 1, "max_workers": 4}]
    signals = [{"signal": "cache_lost", "ts": T0 + 441 * S}] * 3
    f = {r["category"]: r for r in capacity_findings("x", pd.DataFrame(tasks), stages, ex, [], ci, signals, rules)}
    lag = f["autoscale_lag"]
    assert "the first new executor came 5 m 0 s later" in lag["evidence"] and "between 1 and 4 workers" in lag["evidence"]
    rm = f["autoscale_removed"]
    assert rm["severity"] == "high" and "3 cached blocks were lost" in rm["evidence"]
    assert "stage 2 hit a fetch failure: its shuffle data was on executor 3" in rm["evidence"]
    assert makespan([10, 10, 10, 10], 2) == 20 and makespan([30, 10, 10, 10], 2) == 30



def test_autoscale_lag_starts_at_the_real_queue_and_blocks_go_to_their_removal():
    rules = load_rules()
    # 2 cores; a 0.5 s blip of 3 queued tasks at T0, then 50 tasks queue from T0 + 60 s; 6 more cores 5 min later
    tasks = [task(i, 1, T0, 1_500, ex="1") for i in range(3)]
    tasks += [task(10 + i, 1, T0 + 60 * S + (i // 2) * 30 * S, 30 * S, ex="1") for i in range(20)]
    tasks += [task(100 + i, 1, T0 + 360 * S + (i // 8) * 30 * S, 30 * S, ex=str(2 + i % 3)) for i in range(30)]
    stages = [{"spark_context_id": "c", "stage_id": 1, "stage_attempt": 0, "start_time": T0, "end_time": T0 + 500 * S,
               "status": "succeeded", "duration_ms": 500 * S, "run_key": "r1"}]
    ex = [{"spark_context_id": "c", "executor_id": "1", "cores": 2, "added_time": T0 - S, "removed_time": None}]
    ex += [{"spark_context_id": "c", "executor_id": str(e), "cores": 2, "added_time": T0 + 360 * S,
            "removed_time": T0 + 480 * S + 400, "removal_category": "autoscale"} for e in (2, 3)]
    # executor 4 runs out of memory after the burst: the blocks lost then are its own, not autoscaling's
    ex += [{"spark_context_id": "c", "executor_id": "4", "cores": 2, "added_time": T0 + 360 * S,
            "removed_time": T0 + 485 * S, "removal_category": "oom"}]
    ci = [{"spark_context_id": "c", "min_workers": 1, "max_workers": 4}]
    # 2 lines in the removal's own second (log lines carry whole seconds), 4 after the OOM
    signals = [{"signal": "cache_lost", "ts": T0 + 480 * S}] * 2 + [{"signal": "cache_lost", "ts": T0 + 486 * S}] * 4
    f = {r["category"]: r for r in capacity_findings("x", pd.DataFrame(tasks), stages, ex, [], ci, signals, rules)}
    assert "the first new executor came 5 m 0 s later" in f["autoscale_lag"]["evidence"]
    assert "2 cached blocks were lost" in f["autoscale_removed"]["evidence"]


def test_core_use():
    tasks = pd.DataFrame([task(1, 1, T0, 60 * S), task(2, 1, T0, 60 * S, failed=True)])
    ex = [{"spark_context_id": "c", "executor_id": "1", "cores": 2, "added_time": T0, "removed_time": T0 + 60 * S}]
    assert core_use(tasks, ex, []) == {"worker_core_s": 120, "useful_task_s": 60, "core_s_per_useful": 2.0}


COUNT_PLAN = """== Physical Plan ==
AdaptiveSparkPlan (6)
+- == Final Plan ==
   ResultQueryStage (5), Statistics(sizeInBytes=16.0 B, rowCount=1)
   +- * HashAggregate (4)
      +- ShuffleQueryStage (3)
         +- Exchange (2)
            +- TableCacheQueryStage (1), Statistics(sizeInBytes=144.0 GiB, rowCount=1.02E+8)


(4) HashAggregate [codegen id : 2]
Input [1]: [count#10L]
Keys: []
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#9L]
Results [1]: [count(1)#9L AS count#11L]

(1) InMemoryRelation
Arguments: [id#1], StorageLevel(disk, memory, deserialized, 1 replicas)
"""


def test_count_only_cache_and_ddl_loop():
    rules = load_rules()
    assert count_only(COUNT_PLAN) and cache_size(COUNT_PLAN) == 144 << 30
    assert not count_only(COUNT_PLAN.replace("[count(1)]", "[max(load_ts#3)]"))
    assert not count_only("== Physical Plan ==\nAppendData (2)\n+- HashAggregate(keys=[], functions=[count(1)])\n")
    assert count_only("== Physical Plan ==\n*(2) HashAggregate(keys=[], functions=[count(1)])\n")
    assert ddl_shape("ALTER TABLE main.s.t ALTER COLUMN `a` COMMENT 'x'") == ddl_shape("ALTER TABLE main.s.t ALTER COLUMN b COMMENT 'y y'")
    assert ddl_shape("SELECT 1") is None

    qs = [{"spark_context_id": "c", "sql_execution_id": 104, "run_key": "r1", "duration_ms": 600 * S, "start_time": T0,
           "final_plan": COUNT_PLAN, "description": "count"}]
    qs += [{"spark_context_id": "c", "sql_execution_id": 200 + i, "run_key": "r1", "duration_ms": S, "start_time": T0,
            "final_plan": "", "description": f"ALTER TABLE main.s.t ALTER COLUMN c{i} COMMENT 'col {i}'"} for i in range(12)]
    runs = [{"run_key": "r1", "spark_context_id": "c", "duration_ms": 1000 * S, "program": "load"}]
    ex = [{"spark_context_id": "c", "executor_id": str(i), "storage_memory": 4 << 30, "added_time": T0, "removed_time": None}
          for i in range(4)]
    sig = [{"signal": "cache_not_fit", "line": "Not enough space to cache rdd_5_1 in memory! (computed 2.1 GiB so far)"}] * 3
    sig += [{"signal": "cache_lost", "line": "No more replicas available for rdd_5_2 !"}] * 2
    evc = [{"spark_context_id": "c", "event_type": "SparkListenerTaskEnd", "count": 5}]
    f = {r["category"]: r for r in workload_findings("x", qs, runs, ex, sig, evc, rules)}
    c = f["dataframe_cache"]
    assert c["severity"] == "high" and "the largest is 144 GB (query 104), 9.0× the 16.0 GB of storage memory" in c["evidence"]
    assert "3 blocks did not fit in memory (one reached 2.1 GB)" in c["evidence"] and "never released" in c["evidence"]
    assert f["count_only"]["evidence"].startswith("1 query only counted rows: 10 m 0 s of its 16 m 40 s (60%)")
    assert f["ddl_loop"]["evidence"].startswith("12 statements of the same shape")
    # the query stayed open 10 minutes but its stages ran 2 seconds: not a costly count
    st = [{"spark_context_id": "c", "sql_execution_id": 104, "start_time": T0 + 100 * S, "end_time": T0 + 102 * S}]
    assert "count_only" not in {r["category"] for r in workload_findings("x", qs, runs, ex, sig, evc, rules, st)}
    st = [{"spark_context_id": "c", "sql_execution_id": 104, "start_time": T0, "end_time": T0 + 500 * S},
          {"spark_context_id": "c", "sql_execution_id": 104, "start_time": T0 + 400 * S, "end_time": T0 + 560 * S}]
    f = {r["category"]: r for r in workload_findings("x", qs, runs, ex, sig, evc, rules, st)}
    assert f["count_only"]["evidence"].startswith("1 query only counted rows: 9 m 20 s of Spark work, of its 16 m 40 s")


def test_read_split():
    qs = [{"spark_context_id": "c", "sql_execution_id": 1}, {"spark_context_id": "c", "sql_execution_id": 2}]
    st = [{"spark_context_id": "c", "sql_execution_id": 1, "status": "succeeded", "input_bytes": 300, "rdd_scopes": ["InMemoryTableScan"]},
          {"spark_context_id": "c", "sql_execution_id": 1, "status": "succeeded", "input_bytes": 150, "rdd_scopes": ["Scan parquet t"]}]
    nodes = [{"spark_context_id": "c", "sql_execution_id": 2,
              "metrics_json": '[{"name": "cloud storage response size", "total": 70}, {"name": "cache hits size", "total": 5}]'}]
    read_split(qs, st, nodes)
    assert (qs[0]["storage_read"], qs[0]["cache_read"]) == (150, 300)
    assert qs[1]["storage_read"] == 70 and qs[1]["disk_cache_hit"] == 5


def test_init_scripts():
    files = [{"path": f"init_scripts/c_{n}/20261006_175900_00_setup.sh.{k}.log"} for n in ("a", "b") for k in ("stdout", "stderr")]
    text = {"init_scripts/c_a/20261006_175900_00_setup.sh.stderr.log": ["ERROR: pip install failed"]}
    ex = [{"executor_id": "1", "added_time": 1_791_309_600_000}]  # 70 s after 17:59:00 UTC... of a different day
    summary, f = init_scripts("x", files, lambda p: text.get(p, ["ok"]), ex)
    assert summary["node_starts"] == 2 and summary["with_errors"] == 1 and not summary["same_output_everywhere"]
    assert f[0]["evidence"].startswith("setup.sh wrote errors on 1 of 2 node starts. e.g. ERROR: pip install failed")


def test_names_from_the_code():
    assert statement_label("etl/load.py:42\ndf = spark.table('a').cache()\n") == "df = spark.table('a').cache() (etl/load.py:42)"
    assert statement_label("command-123-4:7\nspark.sql('x')") == "spark.sql('x') (command-123-4:7)"
    assert readable_description("# Read the orders\ndf = ...") == "Read the orders"
