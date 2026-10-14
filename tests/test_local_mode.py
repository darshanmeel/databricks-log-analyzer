"""Local mode (spark.master local[N]) and driver-bound runs: the no_spark_work and tiny_task_stages findings, the
driver counted as the executor, and the GC share over task time."""
from databricks_cluster_log_analyzer.analysis.workload import _minus, driver_findings
from databricks_cluster_log_analyzer.config import Rules
from databricks_cluster_log_analyzer.settings_catalog import local_contexts, local_threads
from databricks_cluster_log_analyzer.util import is_worker

S = 1000
MIN = 60 * S
CTX = "local-1700000000000"


def _stage(i, start, dur, tasks=1, p50=500, data=10 << 20, run_key=None, delta=False):
    return {"spark_context_id": CTX, "stage_id": i, "stage_attempt": 0, "start_time": start, "end_time": start + dur,
            "duration_ms": dur, "tasks": tasks, "p50_task_ms": p50, "p50_task_bytes_in": data, "run_key": run_key,
            "rdd_names": ["Delta Table State #0 - file:/tmp/t/_delta_log"] if delta else ["MapPartitionsRDD"]}


def _query(i, start, dur, desc, ops=(), run_key=None):
    return {"spark_context_id": CTX, "sql_execution_id": i, "start_time": start, "end_time": start + dur,
            "description": desc, "operators": list(ops), "run_key": run_key}


def test_minus_counts_what_is_outside():
    assert _minus([(0, 100)], [(10, 20), (50, 60)]) == 80
    assert _minus([(0, 10), (20, 30)], [(5, 25)]) == 10
    assert _minus([(0, 10)], []) == 10


def test_app_mostly_on_the_driver_is_one_high_finding():
    apps = [{"spark_context_id": CTX, "app_name": "tests", "start_time": 0, "end_time": 20 * MIN}]
    stages = [_stage(i, i * MIN, 10 * S) for i in range(5)]  # 50 s of stages in 20 min
    queries = [_query(100 + i, 6 * MIN + i * 30 * S, 20 * S, "sql at <unknown>:0", ("CommandResult", "DropTable"))
               for i in range(10)]
    settings = [{"spark_context_id": CTX, "key": "spark.master", "value": "local[1]"}]
    out = driver_findings("c", apps, [], queries, stages, settings, Rules())
    f = [x for x in out if x["category"] == "no_spark_work"]
    assert len(f) == 1 and f[0]["severity"] == "high"
    assert "19m 10s of the app's 20m (96%)" in f[0]["evidence"]
    assert "10 × DROP TABLE" in f[0]["evidence"]  # the plan's command, not "sql at <unknown>"


def test_busy_app_has_no_driver_finding():
    apps = [{"spark_context_id": CTX, "start_time": 0, "end_time": 10 * MIN}]
    stages = [_stage(0, 0, 10 * MIN)]
    assert driver_findings("c", apps, [], [], stages, [], Rules()) == []


def test_many_stages_of_50_tiny_tasks_reading_the_delta_log():
    apps = [{"spark_context_id": CTX, "start_time": 0, "end_time": 10 * MIN}]
    stages = [_stage(i, i * 5 * S, 3 * S, tasks=50, p50=15, data=46, delta=True) for i in range(40)]
    stages.append(_stage(99, 0, 10 * MIN, tasks=10))
    out = [x for x in driver_findings("c", apps, [], [], stages, [], Rules()) if x["category"] == "tiny_task_stages"]
    assert len(out) == 1
    assert out[0]["evidence"].startswith("40 stages ran exactly 50 tasks each (2,000 of the app's 2,010 tasks")
    assert "snapshotPartitions" in out[0]["fix"]


def test_driver_is_the_executor_only_in_local_mode():
    local = [{"spark_context_id": "a", "executor_id": "driver", "cores": 4}]
    cluster = [{"spark_context_id": "b", "executor_id": "driver", "cores": 8},
               {"spark_context_id": "b", "executor_id": "0", "cores": 8}]
    assert is_worker(local[0], local)
    assert not is_worker(cluster[0], cluster) and is_worker(cluster[1], cluster)


def test_local_master():
    rows = [{"spark_context_id": "a", "key": "spark.master", "value": "local[*]"},
            {"spark_context_id": "b", "key": "spark.master", "value": "spark://host:7077"}]
    assert local_contexts(rows) == {"a"}
    assert local_threads("local[4]") == 4 and local_threads("local") == 1 and local_threads("local[*]") is None
