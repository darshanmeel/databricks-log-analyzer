"""End to end, shaped like the review's JDBC extraction example (all names and ids made up): one run asks the source
database to count, probes it twice with the same SQL, reads key ranges, extracts in partitions into a Unity Catalog
managed table written by path, then reads the source twice more. Built with the real pipeline; the API must name the
written table, list the database steps with the repeats flagged, show the source in what each query read, and split
the run's time by cause."""

from __future__ import annotations

from datetime import datetime

import pytest

import make_fixtures as mf

CID = "1008-070000-jdbcrun1"
CTX = "7770001112223334445"
APP = "app-20261008070000-0000"
T0 = datetime(2026, 10, 8, 7, 0, 0)
HOST = "10.0.0.21"
ROOT = "abfss://landing@account0.dfs.core.windows.net/__unitystorage/catalogs/11111111-2222-3333-4444-555555555555"
TABLE_ID = "aaaaaaaa-0000-4000-8000-0000000000aa"
SRC = "SELECT * FROM src.orders_v WHERE load_ts > '2026-10-01'"


def _jdbc_plan(sql: str, parts: int = 1, extra: str = "") -> str:
    return (f"== Physical Plan ==\n* Scan JDBCRelation(({sql}) SPARK_GEN_SUBQ_0) [numPartitions={parts}]{extra} (1)\n\n\n"
            f"(1) Scan JDBCRelation(({sql}) SPARK_GEN_SUBQ_0) [numPartitions={parts}]{extra}\nOutput [1]: [id#1]\n")


def _append_plan() -> str:
    path = f"{ROOT}/entity_storage/tables/{TABLE_ID}"
    return (f"== Physical Plan ==\nAppendDataExecV1 (2)\n+- * Scan JDBCRelation(({SRC}) SPARK_GEN_SUBQ_0) [numPartitions=4] (1)\n\n\n"
            f"(1) Scan JDBCRelation(({SRC}) SPARK_GEN_SUBQ_0) [numPartitions=4]\nOutput [1]: [id#1]\n\n"
            f"(2) AppendDataExecV1\nArguments: [num_affected_rows#9L], DeltaTableV2(org.apache.spark.sql.SparkSession@1,{path},"
            f"Some(CatalogTable(\nCatalog: my_catalog\nDatabase: landing\nTable: orders\nOwner: someone\nType: MANAGED\n"
            f"Provider: delta\nLocation: {path}\nPartition Provider: Catalog\n)))\n\n"
            f"(3) Execute WriteIntoDeltaCommand\nInput: []\nArguments: OutputSpec({path},Map(),List(id#1))\n")


MERGE_ID = "bbbbbbbb-0000-4000-8000-0000000000bb"


def _merge_plan() -> str:
    return ("== Physical Plan ==\nExecute MergeIntoCommandEdge (1)\n   +- MergeIntoCommandEdge (2)\n\n\n"
            "(1) Execute MergeIntoCommandEdge\nOutput [1]: [num_affected_rows#1L]\n\n"
            "(2) MergeIntoCommandEdge\nArguments: SubqueryAlias s, SubqueryAlias t, `my_catalog`.`sales`.`orders`, "
            f"Delta[version=12, ... logs/11111111-2222-3333-4444-555555555555/entity_storage/tables/{MERGE_ID}], (id#1 <=> id#2)\n")


def _rewrite_plan() -> str:
    path = f"{ROOT}/entity_storage/tables/{MERGE_ID}"
    return ("== Physical Plan ==\nExecute WriteIntoDeltaCommand (1)\n\n\n"
            f"(1) Execute WriteIntoDeltaCommand\nInput: []\nArguments: OutputSpec({path},Map(),List(id#1))\n")


STEPS = [  # (description, plan, tasks, rows per task, seconds)
    ("count the source", _jdbc_plan(f"SELECT COUNT(*) FROM ({SRC})", extra=" [limit=1]"), 1, 1, 1),
    ("probe", _jdbc_plan(f"SELECT * FROM ({SRC}) LIMIT 10"), 1, 10, 2),
    ("probe again", _jdbc_plan(f"SELECT * FROM ({SRC}) LIMIT 10"), 1, 10, 2),
    ("key ranges", _jdbc_plan(f"SELECT load_date, MIN(id) AS lo, MAX(id) AS hi FROM ({SRC}) GROUP BY load_date"), 1, 4, 2),
    ("extract", _append_plan(), 4, 25_000, 30),
    ("count after", _jdbc_plan(SRC, 4), 4, 25_000, 3),
    ("count again", _jdbc_plan(SRC, 4), 4, 25_000, 3),
    # worked example 1's shape: the MERGE names its target, its rewrite step only writes the path
    ("MERGE operation", _merge_plan(), 1, 10, 2),
    ("MERGE operation - Rewriting 4 files", _rewrite_plan(), 1, 10, 2),
]


def _write_cluster(root):
    t = lambda sec: mf.epoch_ms(T0) + int(round(sec * 1000))  # noqa: E731
    ev = mf.EventLog()
    ev.add(mf.ev_log_start())
    ev.add(mf.ev_app_start(APP, t(0)))
    ev.add(mf.ev_executor_added("0", HOST, t(2), cores=4))
    now, tid = 5.0, 0
    for q, (desc, plan, n, rows, secs) in enumerate(STEPS):
        ev.add(mf.ev_sql_start(q, t(now), desc, plan, desc))
        props = mf.job_properties(q, desc)
        info = mf.stage_info(q, 0, f"{desc} at extract.py:{q}", n)
        ev.add(mf.ev_job_start(q, t(now + 0.1), [q], props, [info]))
        ev.add(mf.ev_stage_submitted(mf.stage_info(q, 0, info["Stage Name"], n, submit=t(now + 0.1)), props))
        for i in range(n):
            ev.add(mf.ev_task_end(q, 0, tid, i, 0, "0", HOST, t(now + 0.2), t(now + 0.2 + secs), input_records=rows,
                                  output_bytes=(1 << 20) if desc == "extract" else 0))
            tid += 1
        ev.add(mf.ev_stage_completed(mf.stage_info(q, 0, info["Stage Name"], n, submit=t(now + 0.1), complete=t(now + secs + 0.3))))
        ev.add(mf.ev_job_end(q, t(now + secs + 0.4)))
        ev.add(mf.ev_sql_end(q, t(now + secs + 0.5)))
        now += secs + 1
    ev.add(mf.ev_app_end(t(now + 5)))
    c = root / CID
    mf._write(c / "eventlog" / f"{CID}_10_0_0_20" / CTX / "eventlog", ev.text())
    return c


@pytest.fixture(scope="module")
def built(tmp_path_factory, pipeline_mod):
    src = tmp_path_factory.mktemp("jdbc_logs")
    out = tmp_path_factory.mktemp("jdbc_out")
    pipeline_mod.build(_write_cluster(src), out)
    from databricks_cluster_log_analyzer.api.queries import Store

    return Store(out)


def test_written_table_is_named_and_jdbc_source_is_read(built):
    with built.connect() as con:
        qs = {q["sql_execution_id"]: q for q in built.select(con, CID, "sql_queries", exclude=("final_plan", "initial_plan"))}
    assert qs[4]["tables_written"] == ["my_catalog.landing.orders"]
    assert all("jdbc:src.orders_v" in (qs[i]["tables_read"] or []) for i in range(7))
    # the MERGE's rewrite step wrote by path; the MERGE's own plan names the table
    assert qs[8]["tables_written"] == ["my_catalog.sales.orders"]


def test_database_steps_and_repeats(built):
    from databricks_cluster_log_analyzer.api.queries import run_steps, run_tables

    with built.connect() as con:
        run = built.select(con, CID, "runs", limit=1)[0]["run_key"]
    js = run_tables(built, CID, run)["jdbc"]
    assert [j["kind"] for j in js] == ["count", "probe", "probe", "key ranges", "read", "read", "read"]
    assert [j["same_as"] for j in js] == [None, None, 1, None, None, 4, 4]
    assert js[4]["rows"] == 100_000 and js[4]["partitions"] == 4
    c = run_steps(built, CID, run)["causes"]
    assert c and c["task_ms"] > 0 and c["cpu_ms"] > 0
