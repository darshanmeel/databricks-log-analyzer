"""Revision 7: per-operator SQL metrics from sparkPlanInfo + accumulator updates."""
import json

from databricks_cluster_log_analyzer.parsing.sqlplan import (
    add_driver_accums,
    add_stage_accums,
    add_task_accums,
    plan_from_text,
    plan_node_rows,
    walk_plan,
)


def _m(name, aid, t="sum"):
    return {"name": name, "accumulatorId": aid, "metricType": t}


PLAN = {
    "nodeName": "Exchange", "simpleString": "Exchange hashpartitioning(k, 200)",
    "metrics": [_m("shuffle bytes written", 10, "size"), _m("data size", 11, "size"), _m("shuffle write time", 12, "nsTiming")],
    "children": [{
        "nodeName": "WholeStageCodegen (1)", "metrics": [_m("duration", 20, "timing")],
        "children": [{
            "nodeName": "Filter", "simpleString": "Filter isnotnull(k)", "metrics": [_m("number of output rows", 30)],
            "children": [{"nodeName": "InputAdapter", "metrics": [], "children": [
                {"nodeName": "Scan parquet t", "metrics": [_m("number of output rows", 40), _m("size of files read", 41, "size")],
                 "children": []}]}],
        }],
    }],
}


def test_walk_plan_preorder_parents_and_codegen_clusters():
    nodes = walk_plan(PLAN)
    assert [n["name"] for n in nodes] == ["Exchange", "WholeStageCodegen (1)", "Filter", "InputAdapter", "Scan parquet t"]
    assert [n["parent_id"] for n in nodes] == [None, 0, 1, 2, 3]
    # inside the codegen cluster: the cluster, Filter and the InputAdapter; the scan below the adapter is outside
    assert [n["codegen_id"] for n in nodes] == [None, 1, 1, 1, None]
    assert nodes[1]["is_cluster"] and not nodes[2]["is_cluster"]


def test_task_stage_and_driver_values_join_by_accumulator_id():
    task, stage, driver = {}, {}, {}
    ok = {"Task End Reason": {"Reason": "Success"}}
    for rows, size in ((100, 1000), (300, 3000)):
        add_task_accums(task, "c", {**ok, "Task Info": {"Accumulables": [
            {"ID": 40, "Update": str(rows)}, {"ID": 41, "Update": size}, {"ID": 30, "Update": rows - 10},
            {"ID": 20, "Update": 7}, {"ID": 10, "Update": size // 2}, {"ID": 12, "Update": 2_000_000},
            {"ID": 999, "Name": "internal.metrics.executorRunTime", "Update": 5}]}})
    # failed tasks do not count
    add_task_accums(task, "c", {"Task End Reason": {"Reason": "ExceptionFailure"}, "Task Info": {"Accumulables": [{"ID": 40, "Update": 10**6}]}})
    add_stage_accums(stage, "c", {"Stage Info": {"Accumulables": [{"ID": 11, "Value": "5000"}]}})
    add_driver_accums(driver, "c", {"accumUpdates": [[30, 5]]})
    rows = {r["name"]: r for r in plan_node_rows("cl", "c", 7, walk_plan(PLAN), task, stage, driver)}
    scan = rows["Scan parquet t"]
    assert scan["rows_out"] == 400 and scan["data_bytes"] == 4000
    assert rows["Filter"]["rows_out"] == 380 + 5  # task updates plus the driver-side update
    assert rows["WholeStageCodegen (1)"]["time_ms"] == 14.0
    ex = rows["Exchange"]
    assert ex["data_bytes"] == 2000  # shuffle bytes written wins over data size (stage total 5000)
    assert ex["time_ms"] == 4.0  # 2 tasks x 2 ms, nsTiming converted
    m = {x["name"]: x for x in json.loads(ex["metrics_json"])}
    assert m["data size"]["total"] == 5000 and "max" not in m["data size"]  # stage-level fallback
    assert m["shuffle write time"]["max_ms"] == 2.0
    assert rows["InputAdapter"]["metrics_json"] is None  # defines no metrics
    failed = plan_node_rows("cl", "c", 7, walk_plan(PLAN), {}, {}, {})
    assert {r["name"]: r["metrics_json"] for r in failed}["Filter"] == "[]"  # defined, never reported


def test_fixture_query_plan_nodes(tmp_path):
    from databricks_cluster_log_analyzer.parsing.eventlog import EventTables, consume_events
    from tests.fixtures.make_fixtures import plan_q0, q0_task_accums

    t = EventTables("cl", "c")
    task = lambda i: ("SparkListenerTaskEnd", {"Task End Reason": {"Reason": "Success"}, "Task Info": {  # noqa: E731
        "Task ID": i, "Accumulables": [{"ID": a, "Update": v} for a, v in q0_task_accums(1000, 10, 64, 32)]}, "Task Metrics": {}})
    consume_events([("org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
                     {"executionId": 0, "sparkPlanInfo": plan_q0(), "time": 1}), task(1), task(2)], t)
    rows = {r["name"]: r for r in plan_node_rows("cl", "c", 0, t.sql_plans[("c", 0)], t.acc_task, t.acc_stage, t.acc_driver)}
    assert rows["Scan parquet sales.orders"]["rows_out"] == 20
    assert rows["Filter"]["rows_out"] == 18  # 97% of 10, twice
    assert rows["Exchange"]["data_bytes"] == 64


def test_plan_from_text_when_plan_info_is_a_stub():
    text = ("== Physical Plan ==\nAdaptiveSparkPlan isFinalPlan=true\n+- == Final Plan ==\n"
            "   InsertIntoHadoopFsRelationCommand dbfs:/x, Overwrite\n"
            "   +- *(3) SortMergeJoin(skew=true) [a], [b], Inner\n"
            "      :- *(3) Sort [a ASC NULLS FIRST], false, 0\n"
            "      :  +- AQEShuffleRead coalesced\n"
            "      :     +- ShuffleQueryStage 0\n"
            "      +- FileScan parquet db.t[a#1] Batched: true\n"
            "+- == Initial Plan ==\n   Exchange hashpartitioning(a, 200)\n")
    nodes = plan_from_text(text)
    by = {n["node_id"]: n for n in nodes}
    names = [n["name"] for n in nodes]
    assert names == ["AdaptiveSparkPlan", "InsertIntoHadoopFsRelationCommand", "WholeStageCodegen (3)", "SortMergeJoin",
                     "Sort", "AQEShuffleRead", "ShuffleQueryStage", "Scan parquet db.t"]  # initial plan left out
    smj = nodes[names.index("SortMergeJoin")]
    assert by[smj["parent_id"]]["is_cluster"] and smj["codegen_id"] == 3
    assert by[nodes[names.index("Sort")]["parent_id"]]["name"] == "SortMergeJoin"  # ":- " child, same codegen stage
    assert by[nodes[names.index("Scan parquet db.t")]["parent_id"]]["name"] == "SortMergeJoin"  # second child
    assert plan_from_text(None) == []


def test_fixture_query_with_stub_plan_info_gets_operators():
    from databricks_cluster_log_analyzer.analysis.aggregate import build_sql_plan_nodes
    from databricks_cluster_log_analyzer.parsing.eventlog import EventTables, consume_events

    t = EventTables("cl", "c")
    stub = {"nodeName": "AdaptiveSparkPlan", "simpleString": "AdaptiveSparkPlan", "children": [], "metadata": {}, "metrics": []}
    plan = ("== Physical Plan ==\nAdaptiveSparkPlan isFinalPlan=false\n+- HashAggregate(keys=[], functions=[count(1)])\n"
            "   +- FileScan json raw.events[] Batched: false, ReadSchema: struct<>\n")
    consume_events([("org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
                     {"executionId": 1, "physicalPlanDescription": plan, "sparkPlanInfo": stub, "time": 1})], t)
    rows = build_sql_plan_nodes(t)
    assert [r["name"] for r in rows] == ["AdaptiveSparkPlan", "HashAggregate", "Scan json raw.events"]
    assert all(r["metrics_json"] is None and r["from_text"] for r in rows)  # no numbers: the text carries none
