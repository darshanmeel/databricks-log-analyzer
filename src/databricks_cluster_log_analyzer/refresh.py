"""Revision 17: add the findings that need runs (analysis.contention) to an output built by an older version, from its
datasets alone (no raw logs needed). Rewrites findings.parquet and runs.parquet and the finding counts in
summary.json; running it again replaces what it added before.

Revision 19 recomputes more from the datasets: the tasks adaptive execution cancelled (not failures), the stage
columns that come from the tasks (skew, data per task, the storage / DataFrame-cache split), the per-query read split,
the workload findings (DataFrame cache, count-only queries, DDL loops, disk cache), and the summary's status and
diagnosis. It writes the analyzer revision into summary.json. Not refreshable without the raw logs: the log signals,
the cloud storage metric of stages built before it was parsed, settings and init scripts."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import ANALYZER_REVISION
from .analysis.aggregate import mark_replanned, restage
from .analysis.contention import CATEGORIES, add_findings, contention_findings, first_tasks, to_ms
from .analysis.diagnosis import build_summary
from .analysis.findings import summarize_errors
from .analysis.workload import CATEGORIES as WORKLOAD_CATEGORIES, read_split, workload_findings
from .config import Rules
from .parsing.eventlog import _BENIGN_KILL
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


def _unflag_replanned_kills(path: Path) -> pd.DataFrame | None:
    """The task frame, with the tasks adaptive execution cancelled after re-planning marked not failed (older outputs
    counted them as failures). Rewrites tasks.parquet's `failed` column when one changed."""
    if not path.exists():
        return None
    t = pq.read_table(path)
    tdf = t.to_pandas()
    if tdf.empty or not {"failed", "end_reason", "error"} <= set(tdf.columns):
        return tdf
    kill = (tdf["failed"].fillna(False).astype(bool) & (tdf["end_reason"] == "TaskKilled")
            & tdf["error"].fillna("").map(lambda e: bool(_BENIGN_KILL.search(e))))
    if kill.any():
        tdf.loc[kill, "failed"] = False
        i = t.schema.get_field_index("failed")
        t = t.set_column(i, t.schema.field(i), pa.array(tdf["failed"].astype(bool).tolist(), type=pa.bool_()))
        tmp = path.with_name(path.name + ".tmp")
        pq.write_table(t, tmp, compression="zstd")
        os.replace(tmp, path)
    tdf["failed"] = tdf["failed"].fillna(False).astype(bool)
    return tdf


def refresh_findings(out_dir: str | os.PathLike, rules: Rules) -> dict:
    """`out_dir` is <output>/<cluster_id>. Returns {"added": n, "by_category": {...}}."""
    d = Path(out_dir)
    cid = d.name
    stages = _rows(d / "stages.parquet")
    jobs = _rows(d / "spark_jobs.parquet")
    tasks = _rows(d / "tasks.parquet", ["spark_context_id", "stage_id", "stage_attempt", "launch_time"])
    runs = _rows(d / "runs.parquet")
    executors = _rows(d / "executors.parquet")
    queries = _rows(d / "sql_queries.parquet")
    info = _rows(d / "cluster_info.parquet")
    findings = _rows(d / "findings.parquet")
    signals = _rows(d / "log_signals.parquet")

    # ---- what the tasks say: failures, then the stage columns built from them -------------------------------------
    tdf = _unflag_replanned_kills(d / "tasks.parquet")
    mark_replanned(jobs, stages, rules)
    restage(stages, tdf if tdf is not None else pd.DataFrame())
    read_split(queries, stages, _rows(d / "sql_plan_nodes.parquet", ["spark_context_id", "sql_execution_id", "metrics_json"]))

    # ---- findings that need runs, and the workload findings -----------------------------------------------------------
    # the MERGE finding reads deletion vectors and CDF from the plans (the queries are read whole, plans included) and
    # the files touched from the scan nodes: those of the MERGE queries only
    merges = {(q["spark_context_id"], q["sql_execution_id"]) for q in queries if "MERGE" in (q.get("description") or "")}
    nodes = [n for n in _rows(d / "sql_plan_nodes.parquet", ["spark_context_id", "sql_execution_id", "name", "metrics_json"])
             if (n["spark_context_id"], n["sql_execution_id"]) in merges and "Scan" in (n.get("name") or "")] if merges else []
    new = contention_findings(cid, stages, first_tasks(tasks), runs, executors, queries, info, rules, nodes)
    new += workload_findings(cid, queries, runs, executors, signals, _rows(d / "event_counts.parquet"), rules, stages)
    allf = add_findings(findings, runs, new, replace=(*CATEGORIES, *WORKLOAD_CATEGORIES))
    write_parquet(allf, d / "findings.parquet", "findings")
    write_parquet(runs, d / "runs.parquet", "runs")
    for name, data in (("stages", stages), ("spark_jobs", jobs), ("sql_queries", queries)):
        if (d / f"{name}.parquet").exists():
            write_parquet(data, d / f"{name}.parquet", name)

    # ---- summary: status, diagnosis and counts from the refreshed datasets ------------------------------------------
    sp = d / "summary.json"
    if sp.exists():
        s = json.loads(sp.read_text("utf-8"))
        counts = s.setdefault("counts", {})
        counts.update({
            "findings": len(allf), "failed_jobs": sum(j.get("result") == "JobFailed" for j in jobs),
            "failed_stages": sum(x.get("status") == "failed" for x in stages),
            "failed_queries": sum(q.get("status") == "failed" for q in queries)})
        if tdf is not None and "failed" in tdf:
            counts["failed_tasks"] = int(tdf["failed"].sum())
        log_errors = _rows(d / "log_errors.parquet")
        fresh = build_summary({
            "cluster_id": cid, "input_dir": s.get("input_dir"), "empty_reason": s.get("empty_reason"),
            "counts": counts, "totals": s.get("totals", {}), "rows": s.get("rows", {}), "apps": _rows(d / "apps.parquet"),
            "spark_jobs": jobs, "sql_queries": queries, "stages": stages, "executors": executors, "findings": allf,
            "log_signals": signals, "log_errors": log_errors, "error_summary": summarize_errors(log_errors),
            "run_story": _rows(d / "run_story.parquet"), "log_min_ts": None, "log_max_ts": None,
            "task_retries": _rows(d / "task_retries.parquet"), "cluster_info": info,
            "hotspots": _rows(d / "hotspots.parquet"), "incidents": _rows(d / "incidents.parquet"), "has_runs": bool(runs),
        }, rules)
        for k in ("status", "diagnosis", "findings_by_severity", "retries", "analyzer_revision"):
            s[k] = fresh[k]
        s["refreshed_at"] = fresh["built_at"]
        tmp = d / "summary.json.tmp"
        tmp.write_text(json.dumps(s, indent=2, default=str), "utf-8")
        os.replace(tmp, sp)
    by: dict[str, int] = {}
    for f in new:
        by[f["category"]] = by.get(f["category"], 0) + 1
    return {"added": len(new), "by_category": by}
