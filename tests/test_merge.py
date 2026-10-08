"""One set of MERGE numbers (analysis.merge.merge_facts) for the finding, the Overview advice and the run tables. A
made-up streaming MERGE: a source copy, a scan for matching files and a write step, with deletion vectors and Change
Data Feed on, plus Photon-named and no-deletion-vector variants. All names, ids and numbers are made up."""

import copy
import json
from datetime import datetime, timezone

import pytest

from databricks_cluster_log_analyzer.analysis.contention import contention_findings
from databricks_cluster_log_analyzer.analysis.merge import merge_facts, merge_fix, scan_table, wrote_text
from databricks_cluster_log_analyzer.config import load_rules

CID = "0202-000000-mergetst"
CTX = "2222222222222222222"
RUN = "job_run:77-1"
GB = 1 << 30
T0 = 1_700_000_000_000
TARGET = "demo_cat.core.orders"
SOURCE = "demo_cat.raw.orders_feed"
B = "Streaming batch 4 · stream abc12345 · MERGE operation - MERGE operation - "

S, U = 4_000_000, 1_500_000  # source rows, matched (updated) rows
I = S - U  # inserted
OUT = S + 2 * U + I  # with CDF: data rows + a pre- and post-image per update + an insert row

SCAN_PLAN = """(1) Scan ExistingRDD mergeMaterializedSource
(2) Scan parquet demo_cat.core.orders
Output [3]: [order_id#1, _metadata#2, _target_row_file_dv_id_#3]
(3) Project
Output [2]: [order_id#1, _target_row_file_dv_id_#3 AS deletionVectorId#4]
(4) SortMergeJoin
Left keys [2]: [coalesce(order_id#9, ), isnull(order_id#9)]
Right keys [2]: [coalesce(order_id#1, ), isnull(order_id#1)]
Join type: Inner
"""
WRITE_PLAN = """(1) Scan ExistingRDD mergeMaterializedSource
(2) Scan parquet demo_cat.core.orders
(3) SortMergeJoin
Left keys [2]: [coalesce(order_id#9, ), isnull(order_id#9)]
Right keys [2]: [coalesce(order_id#1, ), isnull(order_id#1)]
Join type: LeftOuter
(4) Generate
Arguments: explode(packedCdc#20)
(5) Project
Output [2]: [order_id#9, __is_cdc#21]
"""


def q(qid, step, start, **kw):
    return {"spark_context_id": CTX, "sql_execution_id": qid, "run_key": RUN, "description": B + step,
            "start_time": T0 + start * 60_000, "end_time": T0 + (start + 1) * 60_000, **kw}


def st(sid, qid, scopes, status="succeeded", **kw):
    return {"spark_context_id": CTX, "stage_id": sid, "stage_attempt": 0, "sql_execution_id": qid, "run_key": RUN,
            "status": status, "rdd_scopes": scopes, "start_time": T0, "end_time": T0 + 60_000, "spark_job_id": sid, **kw}


def node(qid, name, files, pruned=0, size=0, parts=0):
    m = [{"name": "number of files read", "total": files}, {"name": "number of files pruned", "total": pruned},
         {"name": "size of files read", "total": size}, {"name": "number of partition columns", "total": parts}]
    return {"spark_context_id": CTX, "sql_execution_id": qid, "node_id": 1, "name": name, "metrics_json": json.dumps(m)}


def scenario(scan="Scan parquet"):
    """The upsert MERGE (queries 10, 20, 25, 30) and the start of the delete MERGE of the same batch (40, 50). Its write
    step (30) reads the source copy back (12 GB, not the target) and 300 GB of the target, and keeps the matched rows."""
    queries = [
        q(10, "materialize source", 0, input_bytes=6 * GB, final_plan=f"(1) Scan parquet {SOURCE}"),
        q(20, "scanning files for matches", 1, input_bytes=32 * GB, final_plan=SCAN_PLAN),
        q(25, "Rewriting 120 files and writing modified and inserted data", 2, input_bytes=0),  # AQE helper: reads nothing
        q(30, "Rewriting 120 files and writing modified and inserted data", 3, input_bytes=312 * GB,
          input_records=500_000_000 + S, output_records=OUT, output_bytes=20 * GB, disk_spill=90 * GB,
          final_plan=WRITE_PLAN, tables_read=[TARGET]),
        q(40, "materialize source", 4, input_bytes=6 * GB, final_plan=f"(1) Scan parquet {SOURCE}"),
        q(50, "scanning files for matches", 5, input_bytes=0, final_plan=SCAN_PLAN),
    ]
    stages = [
        st(100, 10, ["Exchange", f"{scan} {SOURCE}"], input_bytes=6 * GB, input_records=S, shuffle_write_records=S),
        st(200, 20, ["Exchange", "Scan ExistingRDD mergeMaterializedSource"], input_bytes=12 * GB, input_records=S),
        st(201, 20, ["Exchange", f"{scan} {TARGET}", "Project"], input_bytes=20 * GB, input_records=500_000_000),
        st(300, 30, ["Exchange", "Scan ExistingRDD mergeMaterializedSource", "Filter"], input_bytes=12 * GB,
           input_records=S, shuffle_write_records=S),
        st(301, 30, ["Exchange", f"{scan} {TARGET}", "Filter", "Project"], input_bytes=300 * GB,
           input_records=500_000_000, shuffle_write_records=U),
        st(302, 30, ["SortMergeJoin", "Generate", "Exchange"], shuffle_read_records=S + U, shuffle_write_records=OUT),
        st(303, 30, ["WriteFiles", "Sort"], output_records=OUT),
        # the delete MERGE's scan of the target: adaptive execution replanned it, it read nothing
        st(500, 50, ["Exchange", f"{scan} {TARGET}"], status="replanned", input_bytes=None, input_records=None),
    ]
    nodes = [node(20, f"{scan} {TARGET}", 120, 0, 300 * GB), node(30, f"{scan} {TARGET}", 120, 0, 300 * GB),
             node(50, f"{scan} {TARGET}", 130, 0, 330 * GB)]
    return queries, stages, nodes


def facts(queries, stages, nodes, step=30):
    me = next(x for x in queries if x["sql_execution_id"] == step)
    return merge_facts(me, [x for x in queries if x is not me], stages, nodes=nodes)


@pytest.mark.parametrize("scan", ["Scan parquet", "PhotonScan parquet"])
def test_merge_facts_with_dv_and_cdf(scan):
    f = facts(*scenario(scan))
    assert f["target"] == TARGET
    # the target only: not the 12 GB re-read of the source copy, not the replanned scan of the delete MERGE
    assert f["target_bytes"] == 300 * GB and f["target_rows"] == 500_000_000 and f["target_stages"] == [301]
    assert f["copy_bytes"] == 12 * GB and f["copy_rows"] == S
    assert f["scan_bytes"] == 20 * GB  # the scanning step's target scan, without its copy read
    assert (f["files_touched"], f["files_total"], f["per_file"]) == (120, 120, round(300 * GB / 120))
    assert f["dv_on"] and f["cdf_on"] and f["merge_key"] == "order_id" and f["partition_cols"] == 0
    assert (f["source_rows"], f["output_rows"], f["data_rows"], f["change_rows"]) == (S, OUT, S, OUT - S)
    assert (f["updated"], f["inserted"], f["derived"]) == (U, I, True)
    assert wrote_text(f).startswith(f"matched rows marked deleted (deletion vectors); {S:,} data rows written, "
                                    f"{OUT - S:,} change rows (CDF)")


def test_merge_facts_without_cdf():
    queries, stages, nodes = scenario()
    for x in queries:
        x["final_plan"] = (x.get("final_plan") or "").replace("packedCdc", "x").replace("__is_cdc", "y")
    me = next(x for x in queries if x["sql_execution_id"] == 30)
    me["output_records"] = S  # without CDF the write holds the source rows only
    f = facts(queries, stages, nodes)
    assert f["dv_on"] and not f["cdf_on"] and f["change_rows"] is None
    assert (f["updated"], f["inserted"]) == (U, I)
    assert "change rows" not in wrote_text(f) and f"{S:,} data rows written" in wrote_text(f)


def test_merge_facts_no_dv_and_broken_identity():
    queries, stages, nodes = scenario()
    for x in queries:
        x["final_plan"] = (x.get("final_plan") or "").replace("deletionVectorId", "z").replace("_target_row_file_dv_id_", "z")
    f = facts(queries, stages, nodes)
    # without deletion vectors the target scan passes on whole files: no updated / inserted split
    assert not f["dv_on"] and f["updated"] is None and f["inserted"] is None and not f["derived"]
    assert wrote_text(f).startswith("rewrote whole files (120)")
    assert any("deletion vectors are off" in x for x in merge_fix(f))
    # with deletion vectors but rows that do not add up: nothing derived
    queries, stages, nodes = scenario()
    next(x for x in queries if x["sql_execution_id"] == 30)["output_records"] = OUT + 1
    f = facts(queries, stages, nodes)
    assert f["dv_on"] and f["updated"] is None and f["inserted"] is None


def test_merge_fix_follows_the_evidence():
    f = facts(*scenario())
    fx = " ".join(merge_fix(f))
    assert "120 of 120 files touched" in fx
    assert "Liquid Clustering by order_id" in fx and "narrow range" in fx
    assert "deletion vectors" not in fx  # already on
    assert f"CDF on the target adds {OUT - S:,} change rows to the write shuffle" in fx
    assert "ON condition" not in fx  # no partition or clustering column to add
    queries, stages, nodes = scenario()
    nodes = [node(30, f"Scan parquet {TARGET}", 120, 30, 300 * GB, parts=1)]
    f = facts(queries, stages, nodes)
    assert f["files_total"] == 150
    fx = " ".join(merge_fix(f))
    assert "ON condition" in fx and "else NOT MATCHED inserts duplicates" in fx and "Liquid" not in fx


def test_merge_rewrite_finding_uses_merge_facts():
    queries, stages, nodes = scenario()
    runs = [{"run_key": RUN, "spark_context_id": CTX, "start_time": T0, "end_time": T0 + 10 * 60_000}]
    out = [x for x in contention_findings(CID, stages, {}, runs, [], copy.deepcopy(queries), [], load_rules(), nodes)
           if x["category"] == "merge_rewrite"]
    assert len(out) == 1
    f = out[0]
    assert f["entity"] == f"MERGE into {TARGET} (query 30)" and f["sql_execution_id"] == 30
    ev = f["evidence"]
    assert "read 300 GB of the target (500,000,000 rows) and wrote 20.0 GB" in ev
    assert f"Matched rows marked deleted (deletion vectors); {S:,} data rows written, {OUT - S:,} change rows (CDF)" in ev
    assert "rewrote" not in ev and "rewritten" not in ev
    assert "The source copy was read again: 12.0 GB" in ev and "scanned another 20.0 GB" in ev
    assert "Liquid Clustering by order_id" in f["fix"] and "deletion vectors are off" not in f["fix"]


def test_scan_names_photon_and_spark():
    from databricks_cluster_log_analyzer.api.queries import _scan_table
    assert scan_table(f"PhotonScan parquet {TARGET}") == TARGET == scan_table(f"Scan parquet {TARGET}")
    assert _scan_table(f"PhotonScan parquet {TARGET}")[:2] == (TARGET, "reads files")
    assert scan_table("Scan ExistingRDD mergeMaterializedSource") is None


def test_merge_cycles_skip_replanned_scans():
    from databricks_cluster_log_analyzer.api.queries import merge_cycles
    rd = [{"table": TARGET, "how": "reads files"}]
    qs = [
        {"ctx": CTX, "id": 1, "op": "MERGE: materialize source", "description": "batch 4 · MERGE", "start": 0, "end": 10,
         "reads": [], "writes": [], "input_bytes": 6 * GB, "logic": {"filters": [{"condition": "(_change_type = insert)"}]}},
        {"ctx": CTX, "id": 2, "op": "MERGE: scanning files for matches", "description": "", "start": 10, "end": 20, "reads": rd,
         "writes": [], "stages": [{"reads": [TARGET], "input_bytes": 20 * GB}], "logic": {"filters": []}},
        {"ctx": CTX, "id": 3, "op": "MERGE: materialize source", "description": "batch 4", "start": 30, "end": 40, "reads": [],
         "writes": [], "input_bytes": 6 * GB, "logic": {"filters": [{"condition": "(_change_type = delete)"}]}},
        # the delete MERGE's scan: replanned by adaptive execution, it read 0 bytes
        {"ctx": CTX, "id": 4, "op": "MERGE: scanning files for matches", "description": "", "start": 40, "end": 45, "reads": rd,
         "writes": [], "stages": [{"reads": [TARGET], "input_bytes": None, "status": "replanned"}], "logic": {"filters": []}},
    ]
    ms = merge_cycles(qs)
    assert [(m["kind"], m["target_scans"], m["source_copy_bytes"], m["ms"]) for m in ms] == [
        ("upserts", 1, 6 * GB, 20), ("deletes", 0, 6 * GB, 15)]


def _ts(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).replace(tzinfo=None)


@pytest.fixture()
def merge_store(tmp_path):
    """The scenario written as a built cluster: sql_queries, stages and sql_plan_nodes."""
    from databricks_cluster_log_analyzer.store import write_parquet
    queries, stages, nodes = scenario()
    d = tmp_path / CID
    for x in queries:
        x.update(cluster_id=CID, start_time=_ts(x["start_time"]), end_time=_ts(x["end_time"]))
    for x in stages:
        x.update(cluster_id=CID, start_time=_ts(x["start_time"]), end_time=_ts(x["end_time"]), num_tasks=10, tasks=10)
    for x in nodes:
        x.update(cluster_id=CID)
    write_parquet(queries, d / "sql_queries.parquet", "sql_queries")
    write_parquet(stages, d / "stages.parquet", "stages")
    write_parquet(nodes, d / "sql_plan_nodes.parquet", "sql_plan_nodes")
    return tmp_path


def test_table_stats_rows_per_file_from_one_scan(merge_store):
    """Rows per file from the scan that read rows: not the replanned scan (130 files, 0 rows), not all scans summed."""
    from databricks_cluster_log_analyzer.api.queries import Store, table_stats
    s = Store(merge_store)
    with s.connect() as con:
        t = table_stats(s, con, CID)[TARGET]
    assert t["files"] == 130  # the largest listing still sizes the table
    assert t["rows_per_file"] == round(500_000_000 / 120)


def test_run_tables_and_merge_facts_of_agree(merge_store):
    from databricks_cluster_log_analyzer.api.queries import Store, merge_facts_of, run_tables
    s = Store(merge_store)
    with s.connect() as con:
        f = merge_facts_of(s, con, CID, CTX, 30)
    assert f["target_bytes"] == 300 * GB and f["dv_on"] and f["cdf_on"] and f["updated"] == U
    rt = run_tables(s, CID, RUN)
    step = next(x for x in rt["queries"] if x["id"] == 30)
    assert step["merge"]["target_bytes"] == f["target_bytes"] and step["merge"]["updated"] == U
    up, de = rt["merges"]
    assert up["merge"]["target_bytes"] == 300 * GB and up["target_scans"] == 2
    assert de["target_scans"] == 0 and de["source_copy_bytes"] == 6 * GB
