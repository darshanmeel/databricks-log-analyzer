"""Revision 17: add the findings that need runs (analysis.contention) to an output built by an older version, from its
datasets alone (no raw logs needed). Rewrites findings.parquet and runs.parquet and the finding counts in
summary.json; running it again replaces what it added before."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .analysis.contention import CATEGORIES, add_findings, contention_findings, first_tasks, to_ms
from .config import Rules
from .schemas import SCHEMAS
from .store import write_parquet


def _rows(path: Path, columns: list[str] | None = None) -> list[dict]:
    if not path.exists():
        return []
    t = pq.read_table(path, columns=[c for c in columns if c in pq.read_schema(path).names] if columns else None)
    rows = t.to_pylist()
    ts = [f.name for f in t.schema if pa.types.is_timestamp(f.type)]
    for r in rows:
        for c in ts:
            r[c] = to_ms(r[c])
    return rows


def _merge_plans(path: Path) -> dict[tuple, str]:
    """(spark_context_id, sql_execution_id) -> final plan, for the MERGE queries only, read in batches."""
    if not path.exists() or "final_plan" not in pq.read_schema(path).names:
        return {}
    out: dict[tuple, str] = {}
    for b in pq.ParquetFile(path).iter_batches(columns=["spark_context_id", "sql_execution_id", "description", "final_plan"]):
        for r in b.to_pylist():
            if "MERGE" in (r.get("description") or "") and r.get("final_plan"):
                out[(r["spark_context_id"], r["sql_execution_id"])] = r["final_plan"]
    return out


def refresh_findings(out_dir: str | os.PathLike, rules: Rules) -> dict:
    """`out_dir` is <output>/<cluster_id>. Returns {"added": n, "by_category": {...}}."""
    d = Path(out_dir)
    cid = d.name
    stages = _rows(d / "stages.parquet")
    tasks = _rows(d / "tasks.parquet", ["spark_context_id", "stage_id", "stage_attempt", "launch_time"])
    runs = _rows(d / "runs.parquet")
    executors = _rows(d / "executors.parquet")
    queries = _rows(d / "sql_queries.parquet", [f.name for f in SCHEMAS["sql_queries"]
                                                 if f.name not in ("final_plan", "initial_plan", "details")])
    info = _rows(d / "cluster_info.parquet")
    findings = _rows(d / "findings.parquet")
    # the MERGE finding reads deletion vectors and CDF from the plans, and the files from the scan nodes: load them
    # for the MERGE queries only (plans are large)
    merges = {(q["spark_context_id"], q["sql_execution_id"]) for q in queries if "MERGE" in (q.get("description") or "")}
    plans = {k: p for k, p in _merge_plans(d / "sql_queries.parquet").items() if k in merges}
    for q in queries:
        q["final_plan"] = plans.get((q["spark_context_id"], q["sql_execution_id"]))
    nodes = [n for n in _rows(d / "sql_plan_nodes.parquet", ["spark_context_id", "sql_execution_id", "name", "metrics_json"])
             if (n["spark_context_id"], n["sql_execution_id"]) in merges and "Scan" in (n.get("name") or "")] if merges else []
    new = contention_findings(cid, stages, first_tasks(tasks), runs, executors, queries, info, rules, nodes)
    allf = add_findings(findings, runs, new, replace=CATEGORIES)
    write_parquet(allf, d / "findings.parquet", "findings")
    write_parquet(runs, d / "runs.parquet", "runs")
    sp = d / "summary.json"
    if sp.exists():
        s = json.loads(sp.read_text("utf-8"))
        s.setdefault("counts", {})["findings"] = len(allf)
        fbs = {"high": 0, "medium": 0, "low": 0}
        for f in allf:
            if f["severity"] in fbs:
                fbs[f["severity"]] += 1
        s["findings_by_severity"] = fbs
        tmp = d / "summary.json.tmp"
        tmp.write_text(json.dumps(s, indent=2, default=str), "utf-8")
        os.replace(tmp, sp)
    by: dict[str, int] = {}
    for f in new:
        by[f["category"]] = by.get(f["category"], 0) + 1
    return {"added": len(new), "by_category": by}
