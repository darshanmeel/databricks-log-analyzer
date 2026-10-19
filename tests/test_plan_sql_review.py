"""Revision 23 review: the plan shapes an adversarial review found written wrong as SQL, and the MERGE cause's scope and
key checks. Plans in tests/fixtures/plan_sql_cases.py (made-up tables and columns)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "fixtures"))

import plan_sql_cases as c  # noqa: E402

from databricks_cluster_log_analyzer.api.queries import merge_dup_cause, plan_logic, query_logic  # noqa: E402
from databricks_cluster_log_analyzer.parsing.plan_sql import plan_sql  # noqa: E402


def test_aggregate_keeps_its_alias_for_the_having_above_it():
    sql = plan_sql(c.AGG_ALIAS_1)
    assert "sum(amount) AS total" in sql and "WHERE total > 1000.0" in sql


def test_take_ordered_and_project_formatted_and_simple():
    for p in (c.TAK_1, c.TAK_2):
        sql = plan_sql(p)
        assert "sum(amount) AS total" in sql, sql
        assert sql.rstrip().endswith("ORDER BY total DESC\nLIMIT 10")


def test_or_in_partition_filters_keeps_its_brackets():
    for p in (c.PF_OR_1, c.PF_OR_2):
        assert "WHERE ((order_date = '2024-01-01') OR (order_date = '2024-01-02'))\n  AND city = " in plan_sql(p)


def test_or_in_a_join_condition_keeps_its_brackets_and_sides():
    assert "ON t1.manager_id = t2.emp_id\n  AND ((t1.salary > t2.salary) OR t2.salary IS NULL)" in plan_sql(c.ORJOIN_1)


def test_literals_are_quoted_columns_are_not():
    sql = plan_sql(c.LIT_REPRO_1)
    assert "'src_a' AS source_system" in sql and "'2026-01-01 00:00:00' AS ingest_ts" in sql
    assert "WHERE ship_city = bill_city" in plan_sql(c.SIMPLE_EQ_1)


def test_reused_exchange_reads_the_rows_it_reuses():
    sql = plan_sql(c.REUSE_1)
    assert "reusedexchange" not in sql.lower() and sql.count("spark_catalog.default.emp") == 2
    assert "ON t1.mgr_id = t2.emp_id" in sql


def test_self_join_columns_keep_their_side():
    assert "ON ((t1.salary > t2.salary) OR (t1.mgr_id = t2.emp_id))" in plan_sql(c.SELFJOIN_1)
    sql = plan_sql(c.SELFCMP_1)
    assert "AND t1.ts >= t2.ts" in sql and "(t1.ts > t2.ts) AS newer" in sql


def test_two_windows_on_one_partition_both_stay():
    sql = plan_sql(c.TWO_WIN_1)
    assert "AS rn_latest" in sql and "AS rn_biggest" in sql


def test_not_null_next_to_an_or_is_kept():
    assert "WHERE a IS NOT NULL\n  AND ((a = 1) OR (b = 2))" in plan_sql(c.ISNN_1)


def test_jdbc_pushed_filters_and_table():
    for p in (c.JDBC_1, c.JDBC_2):
        sql = plan_sql(p)
        assert "FROM public.orders" in sql and "status = 'active'" in sql


def test_dynamic_partition_pruning_is_left_out():
    assert "dynamicpruning" not in plan_sql(c.DPP_1)


def test_existence_join_keeps_every_left_row():
    sql = plan_sql(c.EXIST_1)
    assert "SEMI JOIN" not in sql and "EXISTS (SELECT 1 FROM" in sql and "WHERE (exists OR vip)" in sql


def test_rollup_is_grouping_sets():
    sql = plan_sql(c.ROLLUP_1)
    assert sql.count("GROUP BY") == 1 and "GROUP BY GROUPING SETS ((region, city), (region), ())" in sql
    assert "spark_grouping_id" not in sql and "sum(amount) AS total" in sql


def test_null_safe_per_key():
    sql = plan_sql(c.NS_MIXED_1)
    assert "ON t1.id = t2.id\n  AND t1.part <=> t2.part" in sql


def test_named_struct():
    assert "named_struct('id', id, 'code_x', c.code, 'name', name)" in plan_sql(c.STRUCT_REPRO_1)


def test_an_initial_only_adaptive_plan_is_read():
    assert plan_sql(c.AQE_INIT_1) == "SELECT a\nFROM default.t\nWHERE a > 5"


def test_more_columns_note_leaves_no_trailing_comma():
    ins = ", ".join(f"c{i}#{i + 1}" for i in range(16))
    outs = ", ".join([ins] + [f"(c1#2 + {i}) AS a{i}#{100 + i}" for i in range(18)])
    plan = f"""== Physical Plan ==
Project (2)
+- Scan parquet db.t (1)


(1) Scan parquet db.t
Output [16]: [{ins}]

(2) Project
Output [34]: [{outs}]
Input [16]: [{ins}]
"""
    sql = plan_sql(plan)
    assert "(c1 + 11) AS a11 /* … 6 more */\nFROM db.t" in sql
    assert "collect_set(arr) AS s" in plan_sql(c.V_MORE_1)


def test_window_frame_is_written_when_not_the_default():
    p = c.TWO_WIN_1.replace("row_number()", "sum(amount#3)", 1) if "row_number()" in c.TWO_WIN_1 else None
    plan = """== Physical Plan ==
Window (2)
+- Scan parquet main.t (1)


(1) Scan parquet main.t
Output [3]: [acct#1, ts#2, amount#3]

(2) Window
Input [3]: [acct#1, ts#2, amount#3]
Arguments: [sum(amount#3) windowspecdefinition(acct#1, ts#2 ASC NULLS FIRST, specifiedwindowframe(RowFrame, -2, currentrow$())) AS sum3#11], [acct#1], [ts#2 ASC NULLS FIRST]
"""
    assert "sum(amount) OVER (PARTITION BY acct ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum3" in plan_sql(plan)
    assert p is None or isinstance(p, str)


def test_scalar_subquery_is_written_inline():
    plan = """== Physical Plan ==
* Filter (2)
+- Scan parquet main.s.sales (1)


(1) Scan parquet main.s.sales
Output [2]: [id#1, amount#2]

(2) Filter
Input [2]: [id#1, amount#2]
Condition : (amount#2 > Subquery scalar-subquery#30, [id=#40])

===== Subqueries =====

Subquery:1 Hosting operator id = 2 Hosting Expression = Subquery scalar-subquery#30, [id=#40]
* HashAggregate (4)
+- Scan parquet main.s.sales (3)


(3) Scan parquet main.s.sales
Output [1]: [amount#5]

(4) HashAggregate
Input [1]: [amount#5]
Keys: []
Functions [1]: [avg(amount#5)]
Results [1]: [avg(amount#5)#7 AS avg(amount)#8]
"""
    sql = plan_sql(plan)
    assert "WHERE amount > (\n  SELECT avg(amount)" in sql and "FROM main.s.sales\n)" in sql


# ---------------------------------------------------------------------------------------------------------------------
# merge_dup_cause: scope and key checks
# ---------------------------------------------------------------------------------------------------------------------

ERR = "[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched"


def _window(fn: str, part: str, filt: str = "(rn#9 = 1)", agg: bool = False) -> str:
    w = (f"sum(amount#3) windowspecdefinition({part}, ts#2 ASC NULLS FIRST, specifiedwindowframe(RangeFrame, "
         f"unboundedpreceding$(), currentrow$())) AS rn#9" if agg else
         f"{fn}() windowspecdefinition({part}, ts#2 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), "
         f"currentrow$())) AS rn#9")
    return f"""== Physical Plan ==
* Filter (3)
+- Window (2)
   +- Scan parquet main.s.src (1)


(1) Scan parquet main.s.src
Output [4]: [order_id#1, ts#2, amount#3, line_no#4]

(2) Window
Input [4]: [order_id#1, ts#2, amount#3, line_no#4]
Arguments: [{w}], [{part}], [ts#2 DESC NULLS LAST]

(3) Filter
Input [5]: [order_id#1, ts#2, amount#3, line_no#4, rn#9]
Condition : {filt}
"""


SCAN = """== Physical Plan ==
* SortMergeJoin Inner (3)
:- Scan parquet main.s.src (1)
+- Scan parquet main.s.tgt (2)


(1) Scan parquet main.s.src
Output [2]: [order_id#1, line_no#4]

(2) Scan parquet main.s.tgt
Output [2]: [order_id#11, line_no#14]

(3) SortMergeJoin
Left keys [2]: [order_id#1, line_no#4]
Right keys [2]: [order_id#11, line_no#14]
Join type: Inner
Join condition: None
"""


def _cause(qs: list[dict], failed: int) -> str | None:
    logic = {(q["spark_context_id"], q["sql_execution_id"]): query_logic(q) for q in qs}
    return merge_dup_cause(next(q for q in qs if q["sql_execution_id"] == failed), qs, logic)


def _q(i, desc, plan=None, err=None, root=None, t=(0, 10)):
    from datetime import datetime, timezone
    ts = lambda s: datetime.fromtimestamp(s, timezone.utc)  # noqa: E731
    return {"spark_context_id": "1", "sql_execution_id": i, "description": desc, "error": err, "final_plan": plan,
            "root_execution_id": root, "start_time": ts(t[0]), "end_time": ts(t[1]), "status": "failed" if err else "succeeded"}


def test_cause_does_not_reach_another_merge():
    qs = [_q(1, "MERGE INTO a", t=(0, 10)), _q(2, "MERGE operation - materialize source", _window("dense_rank", "order_id#1, line_no#4"),
                                                root=1, t=(1, 2)),
          _q(4, "MERGE INTO b", err=ERR, t=(20, 30)), _q(5, "MERGE operation - scanning files for matches", SCAN, root=4, t=(21, 22))]
    c_ = _cause(qs, 4)
    assert c_ is None or "Query 2" not in c_


def test_a_dedup_on_fewer_columns_than_the_merge_keys_is_safe():
    qs = [_q(4, "MERGE INTO b", err=ERR, t=(20, 30)),
          _q(5, "MERGE operation - materialize source", _window("row_number", "order_id#1"), root=4, t=(21, 22)),
          _q(6, "MERGE operation - scanning files for matches", SCAN, root=4, t=(22, 23))]
    assert _cause(qs, 4) is None


def test_a_dedup_on_more_columns_than_the_merge_keys_is_named():
    qs = [_q(4, "MERGE INTO b", err=ERR, t=(20, 30)),
          _q(5, "MERGE operation - materialize source", _window("row_number", "order_id#1, line_no#4, ts#2"), root=4, t=(21, 22)),
          _q(6, "MERGE operation - scanning files for matches", SCAN, root=4, t=(22, 23))]
    assert "but the MERGE matches on order_id, line_no only: rows that differ in ts" in _cause(qs, 4)


def test_a_running_sum_or_a_top_three_is_no_dedup():
    for plan in (_window("sum", "order_id#1", filt="(amount#3 > 0)", agg=True), _window("rank", "order_id#1", filt="(rn#9 <= 3)")):
        qs = [_q(4, "MERGE INTO b", err=ERR, t=(20, 30)),
              _q(5, "MERGE operation - materialize source", plan, root=4, t=(21, 22)),
              _q(6, "MERGE operation - scanning files for matches", SCAN, root=4, t=(22, 23))]
        assert _cause(qs, 4).startswith("No query of this MERGE keeps one row per key")


def test_plain_window_with_rank_filter_keeps_one_row():
    (w,) = plan_logic(_window("dense_rank", "order_id#1"))["windows"]
    assert (w["limit"], w["keeps"], w["ties"]) == (1, "the top 1 by dense_rank per key", True)
