"""Review of 2026-10-14: a stage holds cores from its first task, a run starts at its first query, job-less queries
take their run, data read counts the attempts that succeeded, and the findings from the operator trees."""
import pandas as pd

from databricks_cluster_log_analyzer.analysis.aggregate import dist_rows
from databricks_cluster_log_analyzer.analysis.runs import default_run, inherit_run_keys, widen_runs
from databricks_cluster_log_analyzer.analysis.workload import _spark_time, plan_findings
from databricks_cluster_log_analyzer.api.queries import _core_hog, _held_ms, _waited
from databricks_cluster_log_analyzer.config import load_rules

MIN = 60_000
H = 60 * MIN


def test_a_stage_holds_cores_from_its_first_task():
    queued = {"stage_id": 1, "start_time": 0, "first_task_time": 5 * H, "end_time": 8 * H, "num_tasks": 159, "tasks": 171}
    hog = {"stage_id": 2, "start_time": 0, "first_task_time": 1000, "end_time": 5 * H, "num_tasks": 2091, "tasks": 2091}
    assert _held_ms(queued) == 3 * H and _held_ms(hog) == 5 * H - 1000
    assert _held_ms({"start_time": 0, "end_time": H}) == H  # an older output: no first task time
    ex = [{"executor_id": str(i), "cores": 8, "added_time": 0, "removed_time": None} for i in range(3)]
    st, cores = _core_hog([queued, hog], ex)
    assert st["stage_id"] == 2 and cores == 24  # the stage that held the cores, not the one that waited longest


def test_a_run_waits_by_its_own_measure():
    assert _waited({"waiting_ms": 0, "queued_full_ms": 5 * H}) == 0  # none, not the queue of its waves
    assert _waited({"waiting_ms": None, "queued_full_ms": 5 * H}) == 5 * H


def test_spark_time_starts_at_the_first_task():
    stages = [{"spark_context_id": "c", "sql_execution_id": 1, "start_time": 0, "first_task_time": 5 * H, "end_time": 6 * H}]
    assert _spark_time(stages)[("c", 1)] == H


def test_a_run_starts_at_its_first_query():
    runs = [{"run_key": "r", "spark_context_id": "c", "start_time": 6 * H, "end_time": 6 * H + 9000, "duration_ms": 9000},
            {"run_key": "s", "spark_context_id": "c", "start_time": H, "end_time": 2 * H, "duration_ms": H,
             "overlapping_runs": None},
            {"run_key": "app", "spark_context_id": "c", "kind": "app", "label": "Outside any run (3 Spark jobs)",
             "start_time": 0, "end_time": 7 * H, "duration_ms": MIN}]
    queries = [{"run_key": "r", "start_time": 10_000, "end_time": 6 * H + 9000},
               {"run_key": "app", "start_time": 0, "end_time": 7 * H}]
    assert widen_runs(runs, queries) == 1
    r = runs[0]
    assert r["start_time"] == 10_000 and r["duration_ms"] == 6 * H + 9000 - 10_000
    assert r["overlapping_runs"] == ["s"] and runs[1]["overlapping_runs"] == ["r"]
    assert runs[2]["duration_ms"] == MIN  # the catch-all keeps its own time
    for x in runs:
        x["status"] = "succeeded"
    assert default_run(runs) == "r"  # never the catch-all


def test_a_query_with_no_job_takes_the_run_it_ran_in():
    qs = [{"spark_context_id": "c", "sql_execution_id": 644, "root_execution_id": 644, "run_key": None},
          {"spark_context_id": "c", "sql_execution_id": 648, "root_execution_id": 644, "run_key": "E"},
          {"spark_context_id": "c", "sql_execution_id": 847, "root_execution_id": 644, "run_key": None},
          {"spark_context_id": "c", "sql_execution_id": 900, "root_execution_id": None, "run_key": None}]
    assert inherit_run_keys(qs) == 2
    assert [q["run_key"] for q in qs] == ["E", "E", "E", None]


def test_data_read_counts_the_attempts_that_succeeded():
    t = pd.DataFrame({"k": ["a", "a", "a"], "failed": [True, False, False], "task_ms": [1.0, 2.0, 3.0],
                      "input_bytes": [0.0, 0.0, 0.0], "shuffle_read": [10.0, 20.0, 30.0],
                      "input_records": [100.0, 40.0, 60.0], "shuffle_read_records": [0.0, 0.0, 0.0],
                      "output_bytes": [5.0, 5.0, 5.0]})
    d = dist_rows(t, ["k"], ("input_records", "output_bytes", "shuffle_read"))["a"]
    assert d["input_records"] == 100 and d["output_bytes"] == 10  # the failed attempt's rows were read again
    assert d["shuffle_read"] == 60  # a cost: every attempt


def test_findings_from_the_operator_trees():
    rules = load_rules()
    queries = [{"spark_context_id": "c", "sql_execution_id": i, "root_execution_id": i, "run_key": f"r{i % 3}",
                "start_time": 0, "end_time": 20 * MIN} for i in range(1, 6)]
    nodes = [{"spark_context_id": "c", "sql_execution_id": 1, "name": "Scan JDBCRelation((SELECT * FROM T) Q) [numPartitions=1]"},
             {"spark_context_id": "c", "sql_execution_id": 2, "name": "Scan JDBCRelation((SELECT * FROM U) Q) [numPartitions=1]"},
             *({"spark_context_id": "c", "sql_execution_id": i, "name": "EmptyRelation"} for i in (3, 4, 5))]
    stages = [{"spark_context_id": "c", "sql_execution_id": 1, "num_tasks": 1, "input_records": 9, "input_bytes": 0,
               "max_task_ms": 2 * H},
              {"spark_context_id": "c", "sql_execution_id": 2, "num_tasks": 1, "input_records": 9, "input_bytes": 0,
               "max_task_ms": MIN}]  # short: not worth a connection pool
    out = {f["category"]: f for f in plan_findings("cid", queries, stages, nodes, [], rules)}
    j = out["jdbc_one_partition"]
    assert j["severity"] == "high" and j["sql_execution_id"] == 1 and j["evidence"].startswith("1 single-connection")
    e = out["empty_input"]
    assert e["evidence"].startswith("3 queries in 3 runs found no input") and "1h open" in e["evidence"]
