"""Revision 9: operator trees read from the plan text when the event log has no sparkPlanInfo tree."""
from databricks_cluster_log_analyzer.parsing.sqlplan import plan_from_text


def test_plan_from_text_formatted_explain():
    """Spark 3.1+ UI default: `* Op (n)` lines, codegen ids in the details after the tree."""
    text = "\n".join([
        "== Physical Plan ==",
        "AdaptiveSparkPlan (7)",
        "+- == Final Plan ==",
        "   * HashAggregate (4)",
        "   +- ShuffleQueryStage (3)",
        "      +- Exchange (2)",
        "         +- * ColumnarToRow (1)",
        "            +- Scan parquet db.t (0)",
        "+- == Initial Plan ==",
        "   HashAggregate (6)",
        "",
        "(0) Scan parquet db.t",
        "Output [1]: [a#1]",
        "",
        "(1) ColumnarToRow [codegen id : 1]",
        "",
        "(4) HashAggregate [codegen id : 2]",
    ])
    nodes = plan_from_text(text)
    real = [(n["name"], n["codegen_id"]) for n in nodes if not n["is_cluster"]]
    assert real == [("AdaptiveSparkPlan", None), ("HashAggregate", 2), ("ShuffleQueryStage", None),
                    ("Exchange", None), ("ColumnarToRow", 1), ("Scan parquet db.t", None)]
    assert sorted(n["name"] for n in nodes if n["is_cluster"]) == ["WholeStageCodegen (1)", "WholeStageCodegen (2)"]
    assert all(n["from_text"] for n in nodes)


def test_plan_from_text_subquery_initial_plan_and_logical_sections():
    text = "\n".join([
        "== Parsed Logical Plan ==",
        "'Project [*]",
        "+- 'UnresolvedRelation [t]",
        "",
        "== Physical Plan ==",
        "AdaptiveSparkPlan isFinalPlan=true",
        "+- == Final Plan ==",
        "   *(1) Filter (x#1 > Subquery subquery#5, [id=#40])",
        "   :  +- Subquery subquery#5, [id=#40]",
        "   :     +- AdaptiveSparkPlan isFinalPlan=true",
        "   :        +- == Final Plan ==",
        "   :           HashAggregate(keys=[], functions=[max(y#2)])",
        "   :        +- == Initial Plan ==",
        "   :           HashAggregate(keys=[], functions=[max(y#2)])",
        "   +- *(1) ColumnarToRow",
        "      +- Execute InsertIntoHadoopFsRelationCommand dbfs:/x",
        "+- == Initial Plan ==",
        "   Filter",
    ])
    nodes = plan_from_text(text)
    names = [n["name"] for n in nodes if not n["is_cluster"]]
    # the logical plan is ignored; the subquery's initial plan is skipped but the main tree after it is kept
    assert names == ["AdaptiveSparkPlan", "Filter", "Subquery", "AdaptiveSparkPlan", "HashAggregate", "ColumnarToRow",
                     "Execute InsertIntoHadoopFsRelationCommand"]
    by = {n["node_id"]: n for n in nodes}
    col = next(n for n in nodes if n["name"] == "ColumnarToRow")
    assert by[col["parent_id"]]["name"] == "Filter"  # second child of the Filter, after the subquery
