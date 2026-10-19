"""Revision 23: a physical plan back as SQL; windows that keep one row per key (and the ties rank / dense_rank keep);
the cause of a MERGE that failed on duplicate source rows. Plans are written as Spark prints them (formatted and simple
mode), with made-up tables and columns."""

from __future__ import annotations

from databricks_cluster_log_analyzer.api.queries import merge_dup_cause, plan_logic, query_logic, stage_what
from databricks_cluster_log_analyzer.parsing.plan_sql import and_parts, keeps_ties, plan_nodes, plan_sql, sql_expr, window_spec

# a streaming MERGE's source: the change feed of a batch, deduplicated with dense_rank (Spark turns "rank = 1" into a
# WindowGroupLimit, Databricks computes the rank in a RunningWindowFunction)
DENSE_RANK_SOURCE = """== Physical Plan ==
AdaptiveSparkPlan (12)
+- == Final Plan ==
   ResultQueryStage (11), Statistics(sizeInBytes=1.0 MiB, ColumnStat: N/A, isRuntime=true)
   +- Project (10)
      +- Filter (9)
         +- RunningWindowFunction (8)
            +- WindowGroupLimit (7)
               +- Sort (6)
                  +- AQEShuffleRead (5)
                     +- ShuffleQueryStage (4)
                        +- Exchange (3)
                           +- Filter (2)
                              +- Scan ExistingRDD (1)


(1) Scan ExistingRDD
Output [6]: [order_id#1, line_no#2, event_ts#3, amount#4, _change_type#5, _commit_version#6L]
Arguments: [order_id#1, line_no#2, event_ts#3, amount#4, _change_type#5, _commit_version#6L], MapPartitionsRDD[1]

(2) Filter
Input [6]: [order_id#1, line_no#2, event_ts#3, amount#4, _change_type#5, _commit_version#6L]
Condition : (isnotnull(_change_type#5) AND NOT (_change_type#5 = update_preimage))

(3) Exchange
Input [6]: [order_id#1, line_no#2, event_ts#3, amount#4, _change_type#5, _commit_version#6L]
Arguments: hashpartitioning(order_id#1, line_no#2, 200), ENSURE_REQUIREMENTS, [plan_id=10]

(4) ShuffleQueryStage
Output [6]: [order_id#1, line_no#2, event_ts#3, amount#4, _change_type#5, _commit_version#6L]
Arguments: 0, Statistics(sizeInBytes=1.0 MiB, ColumnStat: N/A, isRuntime=true)

(5) AQEShuffleRead
Input [6]: [order_id#1, line_no#2, event_ts#3, amount#4, _change_type#5, _commit_version#6L]
Arguments: coalesced

(6) Sort
Input [6]: [order_id#1, line_no#2, event_ts#3, amount#4, _change_type#5, _commit_version#6L]
Arguments: [order_id#1 ASC NULLS FIRST, line_no#2 ASC NULLS FIRST, event_ts#3 DESC NULLS LAST], false, 0

(7) WindowGroupLimit
Input [6]: [order_id#1, line_no#2, event_ts#3, amount#4, _change_type#5, _commit_version#6L]
Arguments: [order_id#1, line_no#2], [event_ts#3 DESC NULLS LAST], dense_rank(event_ts#3), 1, Final

(8) RunningWindowFunction
Input [6]: [order_id#1, line_no#2, event_ts#3, amount#4, _change_type#5, _commit_version#6L]
Arguments: [order_id#1, line_no#2, event_ts#3, amount#4, _change_type#5, _commit_version#6L]

(9) Filter
Input [7]: [order_id#1, line_no#2, event_ts#3, amount#4, _change_type#5, _commit_version#6L, _rnk#20]
Condition : ((_rnk#20 = 1) AND ((_change_type#5 = update_postimage) OR (_change_type#5 = insert)))

(10) Project
Output [4]: [order_id#1, line_no#2, event_ts#3, amount#4]
Input [7]: [order_id#1, line_no#2, event_ts#3, amount#4, _change_type#5, _commit_version#6L, _rnk#20]

(11) ResultQueryStage
Output [4]: [order_id#1, line_no#2, event_ts#3, amount#4]
Arguments: 1, Statistics(sizeInBytes=1.0 MiB, ColumnStat: N/A, isRuntime=true)

(12) AdaptiveSparkPlan
Output [4]: [order_id#1, line_no#2, event_ts#3, amount#4]
Arguments: isFinalPlan=true
"""


def window_plan(fn: str, filt: str = "(rn#9 = 1)") -> str:
    """A plain Window (no group limit): the latest row per customer, ranked with `fn`."""
    return f"""== Physical Plan ==
* Project (6)
+- * Filter (5)
   +- Window (4)
      +- * Sort (3)
         +- Exchange (2)
            +- * Scan parquet my_catalog.crm.customers (1)


(1) Scan parquet my_catalog.crm.customers
Output [3]: [customer_id#1, updated_at#2, email#3]
Batched: true
Location: PreparedDeltaFileIndex [dbfs:/x]
ReadSchema: struct<customer_id:string,updated_at:timestamp,email:string>

(2) Exchange
Input [3]: [customer_id#1, updated_at#2, email#3]
Arguments: hashpartitioning(customer_id#1, 200), ENSURE_REQUIREMENTS, [plan_id=5]

(3) Sort [codegen id : 2]
Input [3]: [customer_id#1, updated_at#2, email#3]
Arguments: [customer_id#1 ASC NULLS FIRST, updated_at#2 DESC NULLS LAST], false, 0

(4) Window
Input [3]: [customer_id#1, updated_at#2, email#3]
Arguments: [{fn}() windowspecdefinition(customer_id#1, updated_at#2 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#9], [customer_id#1], [updated_at#2 DESC NULLS LAST]

(5) Filter [codegen id : 3]
Input [4]: [customer_id#1, updated_at#2, email#3, rn#9]
Condition : {filt}

(6) Project [codegen id : 3]
Output [3]: [customer_id#1, updated_at#2, email#3]
Input [4]: [customer_id#1, updated_at#2, email#3, rn#9]
"""


AGG_JOIN = """== Physical Plan ==
AdaptiveSparkPlan (14)
+- == Final Plan ==
   * HashAggregate (13)
   +- AQEShuffleRead (12)
      +- ShuffleQueryStage (11)
         +- Exchange (10)
            +- * HashAggregate (9)
               +- * Project (8)
                  +- * SortMergeJoin Inner (7)
                     :- * Sort (3)
                     :  +- * Filter (2)
                     :     +- * Scan parquet my_catalog.sales.orders (1)
                     +- * Sort (6)
                        +- * Filter (5)
                           +- * Scan parquet my_catalog.sales.customers (4)
+- == Initial Plan ==
   HashAggregate (20)
   +- Exchange (19)
      +- SortMergeJoin Inner (18)
         :- Scan parquet my_catalog.sales.orders (15)
         +- Scan parquet my_catalog.sales.customers (16)


(1) Scan parquet my_catalog.sales.orders
Output [4]: [order_id#1, customer_id#2, amount#3, order_date#4]
PartitionFilters: [isnotnull(order_date#4), (order_date#4 >= 2026-10-01)]
PushedFilters: [IsNotNull(customer_id)]
ReadSchema: struct<order_id:string,customer_id:string,amount:double>

(2) Filter [codegen id : 1]
Input [4]: [order_id#1, customer_id#2, amount#3, order_date#4]
Condition : (isnotnull(customer_id#2) AND (amount#3 > 0.0))

(3) Sort [codegen id : 1]
Input [4]: [order_id#1, customer_id#2, amount#3, order_date#4]
Arguments: [customer_id#2 ASC NULLS FIRST], false, 0

(4) Scan parquet my_catalog.sales.customers
Output [2]: [customer_id#10, country#11]
ReadSchema: struct<customer_id:string,country:string>

(5) Filter [codegen id : 2]
Input [2]: [customer_id#10, country#11]
Condition : (isnotnull(customer_id#10) AND (country#11 = DE))

(6) Sort [codegen id : 2]
Input [2]: [customer_id#10, country#11]
Arguments: [customer_id#10 ASC NULLS FIRST], false, 0

(7) SortMergeJoin [codegen id : 3]
Left keys [1]: [customer_id#2]
Right keys [1]: [customer_id#10]
Join type: Inner
Join condition: None

(8) Project [codegen id : 3]
Output [2]: [amount#3, country#11]
Input [6]: [order_id#1, customer_id#2, amount#3, order_date#4, customer_id#10, country#11]

(9) HashAggregate [codegen id : 3]
Input [2]: [amount#3, country#11]
Keys [1]: [country#11]
Functions [1]: [partial_sum(amount#3)]
Aggregate Attributes [1]: [sum#30]
Results [2]: [country#11, sum#31]

(10) Exchange
Input [2]: [country#11, sum#31]
Arguments: hashpartitioning(country#11, 200), ENSURE_REQUIREMENTS, [plan_id=40]

(11) ShuffleQueryStage
Output [2]: [country#11, sum#31]
Arguments: 0

(12) AQEShuffleRead
Input [2]: [country#11, sum#31]
Arguments: coalesced

(13) HashAggregate [codegen id : 4]
Input [2]: [country#11, sum#31]
Keys [1]: [country#11]
Functions [1]: [sum(amount#3)]
Aggregate Attributes [1]: [sum(amount#3)#32]
Results [2]: [country#11, sum(amount#3)#32 AS revenue#33]

(14) AdaptiveSparkPlan
Output [2]: [country#11, revenue#33]
Arguments: isFinalPlan=true
"""


def test_dense_rank_source_reads_as_sql_with_its_filters():
    sql = plan_sql(DENSE_RANK_SOURCE)
    assert "dense_rank() OVER (PARTITION BY order_id, line_no ORDER BY event_ts DESC) AS _rnk" in sql
    # the first filter is under the window (a WHERE of the same SELECT); the rank filter is over it (a subquery)
    assert "WHERE NOT (_change_type = 'update_preimage')" in sql
    assert "WHERE _rnk = 1\n  AND ((_change_type = 'update_postimage') OR (_change_type = 'insert'))" in sql
    assert sql.index("dense_rank()") < sql.index("WHERE _rnk = 1")
    assert "FROM dataframe_rows" in sql and "IS NOT NULL" not in sql  # the inferred not-null went with its condition
    assert "Exchange" not in sql and "ShuffleQueryStage" not in sql
    assert sql.startswith("SELECT\n  order_id,\n  line_no,\n  event_ts,\n  amount\nFROM (")


def test_dense_rank_keeping_one_row_per_key_is_flagged_for_ties():
    lg = plan_logic(DENSE_RANK_SOURCE)
    (w,) = lg["windows"]
    assert (w["function"], w["partition_by"], w["limit"], w["ties"]) == ("dense_rank", ["order_id", "line_no"], 1, True)
    assert [f["condition"] for f in lg["filters"]] == [
        "(isnotnull(_change_type) AND NOT (_change_type = update_preimage))",
        "((_rnk = 1) AND ((_change_type = update_postimage) OR (_change_type = insert)))"]


def test_plain_window_row_number_rank_dense_rank():
    for fn, ties in (("row_number", False), ("rank", True), ("dense_rank", True)):
        p = window_plan(fn)
        sql = plan_sql(p)
        assert f"{fn}() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn" in sql, sql
        assert "WHERE rn = 1" in sql
        assert "FROM my_catalog.crm.customers" in sql
        (w,) = plan_logic(p)["windows"]
        assert (w["function"], w["alias"], w["partition_by"], w["ties"]) == (fn, "rn", ["customer_id"], ties)


def test_top_three_per_key_is_no_tie_risk_but_is_read():
    p = window_plan("rank", "(rn#9 <= 3)")
    assert "WHERE rn <= 3" in plan_sql(p)
    assert plan_logic(p)["windows"][0]["ties"] is False


def test_join_aggregate_and_partition_filter():
    sql = plan_sql(AGG_JOIN)
    # the partial and the final aggregate are one GROUP BY
    assert sql.count("GROUP BY") == 1 and "GROUP BY country" in sql
    assert "sum(amount)" in sql and "partial_" not in sql
    assert "JOIN (" in sql and "ON t1.customer_id = t2.customer_id" in sql
    # the partition filter that pruned files, and each side's filter; a string literal quoted
    assert "order_date >= '2026-10-01'" in sql
    assert sql.startswith("SELECT country, sum(amount) AS revenue")  # the aggregate keeps its alias
    assert "amount > 0.0" in sql and "country = 'DE'" in sql
    # the initial plan is not read twice
    assert sql.count("my_catalog.sales.orders") == 1
    lg = plan_logic(AGG_JOIN)
    assert lg["groups"] == [{"node": 9, "keys": ["country"]}]
    assert lg["joins"][0]["left_keys"] == ["customer_id"]


def test_plan_tree_parents_and_initial_plan_skipped():
    nodes, root = plan_nodes(AGG_JOIN)
    assert root == 14
    assert nodes[7]["children"] == [3, 6]
    assert 15 not in nodes and 20 not in nodes


NULL_SAFE_ANTI = """== Physical Plan ==
* BroadcastHashJoin LeftAnti BuildRight (5)
:- * Scan parquet my_catalog.s.new_rows (1)
+- BroadcastExchange (4)
   +- * Filter (3)
      +- * Scan parquet my_catalog.s.old_rows (2)


(1) Scan parquet my_catalog.s.new_rows
Output [2]: [k#1, v#2]

(2) Scan parquet my_catalog.s.old_rows
Output [2]: [k#3, v#4]

(3) Filter [codegen id : 1]
Input [2]: [k#3, v#4]
Condition : (v#4 > 10)

(4) BroadcastExchange
Input [2]: [k#3, v#4]

(5) BroadcastHashJoin [codegen id : 2]
Left keys [2]: [coalesce(k#1, ), isnull(k#1)]
Right keys [2]: [coalesce(k#3, ), isnull(k#3)]
Join type: LeftAnti
Join condition: None
"""


def test_null_safe_anti_join():
    sql = plan_sql(NULL_SAFE_ANTI)
    assert "FROM my_catalog.s.new_rows AS t1\nLEFT ANTI JOIN (" in sql
    assert "ON t1.k <=> t2.k" in sql
    assert "WHERE v > 10" in sql


UNION_EXPLODE_LIMIT = """== Physical Plan ==
TakeOrderedAndProject (6)
+- * Filter (5)
   +- * Generate (4)
      +- Union (3)
         :- * Scan parquet my_catalog.s.a (1)
         +- * Scan parquet my_catalog.s.b (2)


(1) Scan parquet my_catalog.s.a
Output [2]: [id#1, tags#2]

(2) Scan parquet my_catalog.s.b
Output [2]: [id#3, tags#4]

(3) Union

(4) Generate [codegen id : 3]
Input [2]: [id#1, tags#2]
Arguments: explode(tags#2), [id#1], false, [tag#5]

(5) Filter [codegen id : 3]
Input [2]: [id#1, tag#5]
Condition : (tag#5 = urgent)

(6) TakeOrderedAndProject
Input [2]: [id#1, tag#5]
Arguments: 10, [id#1 DESC NULLS LAST], [id#1, tag#5]
"""


def test_union_explode_filter_order_limit():
    sql = plan_sql(UNION_EXPLODE_LIMIT.replace("Arguments: 10, [id#1 DESC NULLS LAST], [id#1, tag#5]",
                                               "Arguments: limit=10, orderBy=[id#1 DESC NULLS LAST], output=[id#1,tag#5]"))
    assert "UNION ALL" in sql and "my_catalog.s.a" in sql and "my_catalog.s.b" in sql
    assert "LATERAL VIEW explode(tags)" in sql and "AS tag" in sql
    assert "WHERE tag = 'urgent'" in sql
    assert sql.rstrip().endswith("ORDER BY id DESC\nLIMIT 10")


PHOTON = """== Physical Plan ==
AdaptiveSparkPlan (7)
+- == Final Plan ==
   PhotonResultStage (6)
   +- PhotonGroupingAgg (5)
      +- PhotonShuffleExchangeSource (4)
         +- PhotonShuffleExchangeSink (3)
            +- PhotonFilter (2)
               +- PhotonScan parquet my_catalog.s.events (1)


(1) PhotonScan parquet my_catalog.s.events
Output [2]: [kind#1, user_id#2]

(2) PhotonFilter
Input [2]: [kind#1, user_id#2]
Condition : (kind#1 = click)

(5) PhotonGroupingAgg
Input [2]: [kind#1, user_id#2]
Keys [1]: [user_id#2]
Functions [1]: [count(1)]
"""


def test_photon_names_read_like_spark():
    sql = plan_sql(PHOTON)
    assert "FROM my_catalog.s.events" in sql and "WHERE kind = 'click'" in sql
    assert "GROUP BY user_id" in sql and "count(1)" in sql
    lg = plan_logic(PHOTON)
    assert lg["filters"] == [{"node": 2, "condition": "(kind = click)"}]
    assert lg["groups"] == [{"node": 5, "keys": ["user_id"]}]


SIMPLE = """== Physical Plan ==
*(2) HashAggregate(keys=[country#11], functions=[count(1)])
+- Exchange hashpartitioning(country#11, 200), ENSURE_REQUIREMENTS, [plan_id=12]
   +- *(1) HashAggregate(keys=[country#11], functions=[partial_count(1)])
      +- *(1) Project [country#11]
         +- *(1) Filter (isnotnull(age#10) AND (age#10 > 30))
            +- *(1) ColumnarToRow
               +- FileScan parquet my_catalog.s.people[age#10,country#11] Batched: true, DataFilters: [isnotnull(age#10), (age#10 > 30)], Format: Parquet, PartitionFilters: [], PushedFilters: [IsNotNull(age), GreaterThan(age,30)], ReadSchema: struct<age:int,country:string>
"""


def test_simple_mode_plan():
    sql = plan_sql(SIMPLE)
    assert "FROM my_catalog.s.people" in sql
    assert "WHERE age > 30" in sql
    assert "GROUP BY country" in sql and "count(1)" in sql


def test_a_command_is_a_comment_and_garbage_is_none():
    assert plan_sql("== Physical Plan ==\nExecute MergeIntoCommandEdge (1)\n\n(1) Execute MergeIntoCommandEdge\n") \
        .startswith("-- a command with no query of its own: Execute MergeIntoCommandEdge")
    assert plan_sql(None) is None and plan_sql("") is None and plan_sql("not a plan at all") is None


def test_expression_helpers():
    assert and_parts("((a = 1) AND ((b = 2) OR (c = 3)))") == ["a = 1", "(b = 2) OR (c = 3)"]
    assert sql_expr("isnotnull(x) AND (s = abc)", {"x", "s"}) == "x IS NOT NULL AND (s = 'abc')"
    assert sql_expr("(a = b)", {"a", "b"}) == "(a = b)"  # a column on both sides stays a column
    w = window_spec("WindowGroupLimit", "[k], [ts DESC NULLS LAST], rank(ts), 1, Partial")
    assert (w["function"], w["limit"], keeps_ties(w)) == ("rank", 1, True)
    assert keeps_ties({"function": "row_number", "limit": 1}) is False


def test_stage_names_say_window_and_filter():
    assert stage_what(["map", "Filter", "AQEShuffleRead", "Sort", "RunningWindowFunction", "WindowGroupLimit"], {}) \
        == "filter, keep the top rows per key"
    assert "window" in stage_what(["Exchange", "Window"], {})


# ---------------------------------------------------------------------------------------------------------------------
# the cause of a MERGE that failed on duplicate source rows
# ---------------------------------------------------------------------------------------------------------------------

MERGE_SCAN = """== Physical Plan ==
* BroadcastHashJoin Inner BuildLeft (3)
:- Scan ExistingRDD mergeMaterializedSource (1)
+- * Scan parquet my_catalog.s.order_lines (2)


(1) Scan ExistingRDD mergeMaterializedSource
Output [2]: [order_id#1, line_no#2]

(2) Scan parquet my_catalog.s.order_lines
Output [2]: [order_id#3, line_no#4]

(3) BroadcastHashJoin [codegen id : 1]
Left keys [4]: [coalesce(order_id#1, ), isnull(order_id#1), coalesce(line_no#2, ), isnull(line_no#2)]
Right keys [4]: [coalesce(order_id#3, ), isnull(order_id#3), coalesce(line_no#4, ), isnull(line_no#4)]
Join type: Inner
Join condition: None
"""


def _merge_queries(source_plan: str, error: str) -> list[dict]:
    b = "Streaming batch 4 · stream abcdef12"
    return [
        {"spark_context_id": "1", "sql_execution_id": 10, "description": b, "status": "failed", "error": error,
         "start_time": None, "end_time": None, "final_plan": None},
        {"spark_context_id": "1", "sql_execution_id": 11, "description": f"{b} · MERGE operation - materialize source",
         "status": "succeeded", "error": None, "final_plan": source_plan},
        {"spark_context_id": "1", "sql_execution_id": 12, "description": f"{b} · MERGE operation - scanning files for matches",
         "status": "succeeded", "error": None, "final_plan": MERGE_SCAN},
        # another stream's batch: not this MERGE
        {"spark_context_id": "1", "sql_execution_id": 13, "description": "Streaming batch 4 · stream 99999999 · MERGE operation - materialize source",
         "status": "succeeded", "error": None, "final_plan": window_plan("row_number")},
    ]


ERR = "[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched"


def _cause(source_plan: str, error: str = ERR) -> str | None:
    qs = _merge_queries(source_plan, error)
    logic = {(q["spark_context_id"], q["sql_execution_id"]): query_logic(q) for q in qs}
    return merge_dup_cause(qs[0], qs, logic)


def test_cause_names_the_dense_rank_that_kept_ties():
    c = _cause(DENSE_RANK_SOURCE)
    assert c.startswith("Query 11 keeps the first row per order_id, line_no with dense_rank() ordered by event_ts DESC")
    assert "The MERGE matches on order_id, line_no." in c and "row_number()" in c


def test_cause_when_the_dedup_keys_are_not_the_merge_keys():
    p = window_plan("row_number").replace("my_catalog.crm.customers", "x")
    c = _cause(p)
    assert "keeps one row per customer_id (row_number), but the MERGE matches on order_id, line_no" in c


def test_cause_when_nothing_deduplicates():
    c = _cause("== Physical Plan ==\nScan ExistingRDD (1)\n\n(1) Scan ExistingRDD\n")
    assert c.startswith("No query of this MERGE keeps one row per key")


def test_no_cause_for_another_error():
    assert _cause(DENSE_RANK_SOURCE, "java.lang.OutOfMemoryError") is None
