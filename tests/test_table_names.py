"""Tables written by storage path get their names from the plans that name them (made-up ids and names)."""

from databricks_cluster_log_analyzer.parsing.plans import name_paths_in_text, name_tables, table_names

ROOT = "abfss://data@account0.dfs.core.windows.net/__unitystorage/catalogs/11111111-2222-3333-4444-555555555555"
T1 = "aaaaaaaa-0000-4000-8000-000000000001"
T2 = "bbbbbbbb-0000-4000-8000-000000000002"
T3 = "cccccccc-0000-4000-8000-000000000003"
T4 = "dddddddd-0000-4000-8000-000000000004"

MERGE_PLAN = f"""== Physical Plan ==
Execute MergeIntoCommandEdge (1)

(1) MergeIntoCommandEdge
Arguments: SubqueryAlias in, SubqueryAlias out, `my_catalog`.`sales`.`orders`, Delta[version=12, ... logs/11111111-2222-3333-4444-555555555555/entity_storage/tables/{T1}], (id#1 <=> id#2)
"""

SCAN_PLAN = f"""== Physical Plan ==
* ColumnarToRow (2)
+- Scan parquet my_catalog.sales.customers (1)


(1) Scan parquet my_catalog.sales.customers
Output [2]: [id#1, name#2]
Batched: true
Location: PreparedDeltaFileIndex [{ROOT}/entity_storage/tables/{T2}]
ReadSchema: struct<id:int,name:string>
"""

APPEND_PLAN = f"""== Physical Plan ==
AppendDataExecV1 (1)


(1) AppendDataExecV1
Arguments: [num_affected_rows#1L], DeltaTableV2(org.apache.spark.sql.SparkSession@1,{ROOT}/entity_storage/tables/{T3},Some(CatalogTable(
Catalog: my_catalog
Database: landing
Table: events
Owner: someone
Type: MANAGED
Provider: delta
Location: {ROOT}/entity_storage/tables/{T3}
Partition Provider: Catalog
))
"""


def test_names_from_merge_scan_and_append_plans():
    names = table_names([MERGE_PLAN, SCAN_PLAN, APPEND_PLAN, None, "no tables here"])
    assert names == {T1: "my_catalog.sales.orders", T2: "my_catalog.sales.customers", T3: "my_catalog.landing.events"}


def test_paths_are_renamed_and_delta_log_reads_fold_into_one():
    names = table_names([MERGE_PLAN])
    ts = [f"{ROOT}/entity_storage/tables/{T1}",
          f"{ROOT}/entity_storage/tables/{T1}/_delta_log/00000000000000000012.checkpoint.parquet",
          f"{ROOT}/entity_storage/tables/{T1}/_delta_log/_sidecars/00000000000000000012.checkpoint.0000000001.parquet",
          f"{ROOT}/entity_storage/tables/{T4}", "my_catalog.sales.items"]
    assert name_tables(ts, names) == ["my_catalog.sales.orders", "my_catalog.sales.orders/_delta_log",
                                      f"{ROOT}/entity_storage/tables/{T4}", "my_catalog.sales.items"]


def test_paths_in_free_text():
    names = table_names([MERGE_PLAN])
    text = f"The MERGE read 12 GB of {ROOT}/entity_storage/tables/{T1} and wrote 1 GB."
    assert name_paths_in_text(text, names) == "The MERGE read 12 GB of my_catalog.sales.orders and wrote 1 GB."
    assert name_paths_in_text(f"wrote {ROOT}/entity_storage/tables/{T4}", names).endswith(T4)


def test_causes_split_task_time():
    from databricks_cluster_log_analyzer.api.queries import _causes

    rows = [{"task_ms": 1000, "cpu_ms": 600, "gc_ms": 100, "fetch_wait_ms": 50, "shuffle_write_ms": 30, "failed_ms": 0,
             "disk_spill": 5},
            {"task_ms": 500, "cpu_ms": 100, "gc_ms": 0, "fetch_wait_ms": 0, "failed_ms": 200, "disk_spill": 0}]
    c = _causes(rows)
    # executor CPU time is the task thread's own: GC runs on other threads and is not taken off it
    assert c["cpu_ms"] == 700 and c["gc_ms"] == 100 and c["fetch_wait_ms"] == 50 and c["failed_ms"] == 200
    assert c["shuffle_write_ms"] == 30
    assert c["other_ms"] == 1500 - 200 - 700 - 100 - 50 - 30 and c["disk_spill"] == 5
    assert _causes([]) is None


def test_jdbc_source_kinds_and_steps():
    from databricks_cluster_log_analyzer.api.queries import jdbc_steps
    from databricks_cluster_log_analyzer.parsing.plans import jdbc_source, plan_summary

    def plan(sql, parts=1, extra=""):
        return f"== Physical Plan ==\n* Scan JDBCRelation(({sql}) SPARK_GEN_SUBQ_0) [numPartitions={parts}]{extra}  (1)\n"

    src = "SELECT * FROM sales.orders_v WHERE load_ts > '2026-01-01'"
    count = plan(f"SELECT COUNT(*) FROM ({src})", extra=" [limit=1]")
    probe = plan(f"SELECT * FROM ({src}) LIMIT 10")
    ranges = plan(f"SELECT load_date,\n  MIN(id) AS lo, MAX(id) AS hi, COUNT(*) AS n\nFROM ({src}) GROUP BY load_date")
    read = plan(src, 12)
    assert [jdbc_source(p)["kind"] for p in (count, probe, ranges, read)] == ["count", "probe", "key ranges", "read"]
    assert jdbc_source(read) == {"sql": src, "source": "sales.orders_v", "partitions": 12, "kind": "read"}
    assert "jdbc:sales.orders_v" in plan_summary(read)["tables_read"]
    qs = [{"sql_execution_id": i, "spark_context_id": "c", "start_time": i, "final_plan": p, "input_records": 5}
          for i, p in enumerate([count, probe, probe, ranges, read, read])]
    steps = jdbc_steps(qs)
    assert [s["same_as"] for s in steps] == [None, None, 1, None, None, 4]


def test_usual_from_earlier_runs_of_the_same_table(tmp_path):
    import pandas as pd

    from databricks_cluster_log_analyzer.api.queries import Store

    def runs(cid, rows):
        d = tmp_path / cid
        d.mkdir()
        pd.DataFrame(rows).to_parquet(d / "runs.parquet")

    day = 86_400_000
    old = [{"run_key": f"r{i}", "program": "load", "subject": "orders", "status": "succeeded",
            "start_time": pd.Timestamp(1_700_000_000_000 + i * day, unit="ms"), "duration_ms": 600_000 + i * 1000,
            "typical_duration_ms": None, "vs_typical": None} for i in range(3)]
    runs("0101-000000-abcd1234", old)
    runs("1006-120000-sample01", [
        {"run_key": "now", "program": "load", "subject": "orders", "status": "succeeded",
         "start_time": pd.Timestamp(1_700_000_000_000 + 5 * day, unit="ms"), "duration_ms": 1_800_000,
         "typical_duration_ms": 60_000, "vs_typical": 30.0},
        {"run_key": "other", "program": "load", "subject": "customers", "status": "succeeded",
         "start_time": pd.Timestamp(1_700_000_000_000 + 5 * day, unit="ms"), "duration_ms": 60_000,
         "typical_duration_ms": 60_000, "vs_typical": 1.0}])
    st = Store(tmp_path)
    with st.connect() as con:
        got = {r["run_key"]: r for r in st.select(con, "1006-120000-sample01", "runs")}
    assert got["now"]["usual_from"] == "history" and got["now"]["usual_runs"] == 3
    assert got["now"]["typical_duration_ms"] == 601_000 and got["now"]["vs_typical"] == round(1_800_000 / 601_000, 2)
    assert "usual_from" not in got["other"] and got["other"]["vs_typical"] == 1.0


def test_micro_batches_per_run():
    from databricks_cluster_log_analyzer.api.queries import _micro_batches

    g = lambda i, desc, a, b: {"id": i, "description": desc, "start": a, "end": b, "waiting_ms": 1, "running_ms": 2}  # noqa: E731
    bs = _micro_batches([g(1, "Streaming batch 7 · stream abcdef12 · MERGE operation", 0, 10),
                         g(2, "Streaming batch 7 · stream abcdef12 · MERGE operation - write", 5, 30),
                         g(3, "Streaming batch 8 · stream abcdef12", 40, 50), g(4, "a plain query", 0, 5)])
    assert [(b["batch"], b["queries"], b["start"], b["end"]) for b in bs] == [(7, [1, 2], 0, 30), (8, [3], 40, 50)]
