"""Revision 17: add the findings that need runs (analysis.contention) to an output built by an older version, from its
datasets alone (no raw logs needed). Rewrites findings.parquet and runs.parquet and the finding counts in
summary.json; running it again replaces what it added before.

Revision 19 recomputes more from the datasets: the tasks adaptive execution cancelled (not failures), the stage
columns that come from the tasks (skew, data per task, the storage / DataFrame-cache split), the per-query read split,
the workload findings (DataFrame cache, count-only queries, DDL loops, disk cache), and the summary's status and
diagnosis. It writes the analyzer revision into summary.json. Not refreshable without the raw logs: the log signals,
the cloud storage metric of stages built before it was parsed, settings and init scripts.

Revision 20 also rebuilds the capacity findings (tasks queued on a full cluster, autoscaling) with each run's queued
time, the incidents (now with the cache and autoscaling findings in them) and the per-minute task numbers (spread over
the minutes each task ran)."""

from __future__ import annotations

import json
import re
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import ANALYZER_REVISION
from .analysis.aggregate import mark_replanned, restage
from .analysis.capacity import CATEGORIES as CAPACITY_CATEGORIES, capacity_findings
from .analysis.combined import spill_shuffle_timeline
from .analysis.incidents import build_incidents
from .analysis.runs import default_run, inherit_run_keys, runs_note, set_typical, widen_runs
from .analysis.contention import CATEGORIES, add_findings, contention_findings, first_tasks, recount_queries, to_ms
from .analysis.diagnosis import build_summary
from .analysis.findings import (_real_losses, _signal_findings, calibrate, confirmed_signals, demote_orphan_failures,
                                recheck_exception_frames, recheck_gc, recheck_skew,
                                restore_attributed_failures, summarize_errors, summarize_signals)
from .analysis.workload import CATEGORIES as WORKLOAD_CATEGORIES, driver_findings, plan_findings, read_split, workload_findings
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


def _ms_frame(tdf: pd.DataFrame | None) -> pd.DataFrame | None:
    """The task frame with its times in epoch milliseconds, as the pipeline has them."""
    if tdf is None:
        return None
    out = tdf.copy()
    for c in ("launch_time", "finish_time"):
        if c in out and pd.api.types.is_datetime64_any_dtype(out[c]):
            s = out[c]
            s = s.dt.tz_localize(None) if getattr(s.dt, "tz", None) is not None else s
            out[c] = (s - pd.Timestamp(0)) // pd.Timedelta(milliseconds=1)  # any stored unit (ns, us, ms)
    return out


def _reclassify_retries(path: Path, findings: list[dict]) -> list[dict]:
    """Task attempts that lost their JDBC connection to a source database were "exception in task code" in older
    outputs: call them what they are, in task_retries.parquet and in the retry findings' text."""
    from collections import Counter

    from .analysis.retries import SOURCE_DB_TEXT_RE, category_text, stage_retry_summary

    rows = _rows(path)
    changed = False
    for r in rows:
        if r.get("first_failure_category") == "exception" and SOURCE_DB_TEXT_RE.match(r.get("first_failure_error") or ""):
            r["first_failure_category"] = "source_db"
            changed = True
    if not changed:
        return rows
    write_parquet(rows, path, "task_retries")
    summ = stage_retry_summary(rows)
    for f in findings:
        if f.get("category") != "task_retries":
            continue
        rs = summ.get((f.get("spark_context_id"), f.get("stage_id"), f.get("stage_attempt")))
        if rs and isinstance(rs.get("by_category"), Counter):
            f["evidence"] = re.sub(r"retried \([^)]*\)", f"retried ({category_text(rs['by_category'])})",
                                   f.get("evidence") or "", count=1)
    return rows


def _resignal(path: Path, rules: Rules) -> list[dict]:
    """Each log line's signal again with the current rules (the first that matches wins): a signal added before
    another, such as a SQL code error before a schema error, takes the lines that are its."""
    rows = _rows(path)
    changed = False
    for r in rows:
        line = r.get("line") or ""
        names = [s.name for s in rules.signals]
        if r.get("signal") not in names:
            continue
        # only a signal listed before the line's own takes it (the rest of the matching, multi-line or not, stays)
        hit = next((s for s in rules.signals[:names.index(r["signal"])] if s.regex.search(line)), None)
        if hit is not None:
            r["signal"], r["severity"], r["fix"] = hit.name, hit.severity, hit.fix
            changed = True
    if changed:
        write_parquet(rows, path, "log_signals")
    return rows


IO_SUMS = ("input_bytes", "input_records", "output_bytes", "output_records")


def _resum_io(rows: list[dict], stages: list[dict], key) -> int:
    """In place: the data read and written of each query or run as the sum of its restaged stages (attempts that
    succeeded only: a failed attempt's rows were read again by its retry). Returns how many rows changed."""
    tot: dict = {}
    for st in stages:
        k = key(st)
        if k is None:
            continue
        t = tot.setdefault(k, dict.fromkeys(IO_SUMS, 0))
        for c in IO_SUMS:
            t[c] += int(st.get(c) or 0)
    n = 0
    for r in rows:
        t = tot.get(key(r))
        if not t:
            continue
        new = {c: t[c] for c in IO_SUMS if c in r and r.get(c) is not None and t[c] != r.get(c)}
        if new:
            r.update(new)
            n += 1
    return n


def _refresh_user_frames(path: Path, rules: Rules) -> list[dict]:
    """"Where to fix" again with the current framework prefixes (JDBC drivers and tooling are not the user's code)."""
    from .parsing.loglines import is_framework_frame

    rows = _rows(path)
    pre = rules.framework_frame_prefixes
    changed = False
    for r in rows:
        uf = r.get("user_frame")
        if uf and is_framework_frame(uf, pre):
            r["user_frame"] = next((f for f in (r.get("top_frames") or []) if f and not is_framework_frame(f, pre)), None)
            changed = True
    if changed:
        write_parquet(rows, path, "log_errors")
    return rows


def _refresh_skew_tasks(d: Path, tms: pd.DataFrame | None, stages: list[dict], queries: list[dict], rules: Rules,
                        cid: str) -> None:
    """The slow-task hotspots again (their cause: a slow source database is now told from an unclear one)."""
    from .analysis.hotspots import _context, _skew_tasks

    path = d / "hotspots.parquet"
    if tms is None or tms.empty or not path.exists():
        return
    old = _rows(path)
    stuck = {(h["spark_context_id"], h["executor_id"]) for h in old if h.get("cause") == "gc_stuck"}
    stage_info, query_info = _context(stages, queries)
    new = _skew_tasks(tms, stage_info, query_info, rules, cid, frozenset(stuck))
    for h in new:
        st = stage_info.get((h["spark_context_id"], h["stage_id"], h["stage_attempt"])) or {}
        h["run_key"] = st.get("run_key")
        h["affected_runs"] = [h["run_key"]] if h["run_key"] else None
    keep = [h for h in old if h.get("kind") != "skew_task"]
    write_parquet(new + keep, path, "hotspots")


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
    # Revision 22: job-less queries take the run they ran in; a run starts at its first query, not its first job
    if inherit_run_keys(queries):
        failed = {q["run_key"] for q in queries if q.get("run_key") and q.get("status") == "failed"}
        for r in runs:
            if r["run_key"] in failed:
                r["status"] = "failed"  # as the pipeline has it: a run whose query failed failed
    widen_runs(runs, queries)
    # "usual" from runs on the same subject (one notebook over 30 tables is not the same work 30 times)
    set_typical(runs)
    info = _rows(d / "cluster_info.parquet")
    findings = _rows(d / "findings.parquet")
    restore_attributed_failures(findings, queries, rules)
    qkey = {(q["spark_context_id"], q["sql_execution_id"]): q["run_key"] for q in queries if q.get("run_key")}
    for f in findings:
        if not f.get("run_key") and f.get("sql_execution_id") is not None:
            f["run_key"] = qkey.get((f.get("spark_context_id"), int(f["sql_execution_id"])))
    signals = _resignal(d / "log_signals.parquet", rules)

    # ---- what the tasks say: failures, then the stage columns built from them -------------------------------------
    tdf = _unflag_replanned_kills(d / "tasks.parquet")
    mark_replanned(jobs, stages, rules)
    restage(stages, _ms_frame(tdf) if tdf is not None else pd.DataFrame())  # times in ms, as the pipeline has them
    read_split(queries, stages, _rows(d / "sql_plan_nodes.parquet", ["spark_context_id", "sql_execution_id", "metrics_json"]))
    qk = (lambda x: (x.get("spark_context_id"), x.get("sql_execution_id")) if x.get("sql_execution_id") is not None else None)
    _resum_io(queries, stages, qk)
    _resum_io(runs, stages, lambda x: x.get("run_key"))

    # ---- findings that need runs, and the workload findings -----------------------------------------------------------
    # the MERGE finding reads deletion vectors and CDF from the plans (the queries are read whole, plans included) and
    # the files touched from the scan nodes: those of the MERGE queries only
    merges = {(q["spark_context_id"], q["sql_execution_id"]) for q in queries if "MERGE" in (q.get("description") or "")}
    nodes = [n for n in _rows(d / "sql_plan_nodes.parquet", ["spark_context_id", "sql_execution_id", "name", "metrics_json"])
             if (n["spark_context_id"], n["sql_execution_id"]) in merges and "Scan" in (n.get("name") or "")] if merges else []
    tms = _ms_frame(tdf)
    _refresh_skew_tasks(d, tms, stages, queries, rules, cid)
    new = capacity_findings(cid, tms, stages, executors, runs, info, signals, rules,
                            settings=_rows(d / "settings.parquet")) if tms is not None else []
    bound = {f["run_key"] for f in new if f["category"] == "capacity_bound"}
    new += [f for f in contention_findings(cid, stages, first_tasks(tasks), runs, executors, queries, info, rules, nodes)
            if not (f["category"] == "waited_for_cores" and f.get("run_key") in bound)]
    new += workload_findings(cid, queries, runs, executors, signals, _rows(d / "event_counts.parquet"), rules, stages)
    new += driver_findings(cid, _rows(d / "apps.parquet"), runs, queries, stages, _rows(d / "settings.parquet"), rules)
    new += plan_findings(cid, queries, stages, _rows(d / "sql_plan_nodes.parquet",
                                                     ["spark_context_id", "sql_execution_id", "name", "detail"]), runs, rules)
    recheck_gc(findings, stages, rules)
    recheck_skew(findings, stages, rules)
    recheck_exception_frames(findings, rules)
    # older builds wrote binary sizes with decimal labels ("917.5 GB" for 917.5 GiB)
    for f in findings:
        for k in ("evidence", "entity"):
            if f.get(k):
                f[k] = re.sub(r"(\d) (K|M|G|T)B\b", r"\1 \2iB", f[k])
    # the log signal findings again, without the lines the event log does not back (a fetch failure no task had, an
    # executor lost as the cluster stopped)
    retries = _reclassify_retries(d / "task_retries.parquet", findings)
    sig_rows = confirmed_signals(signals, stages, retries, _rows(d / "apps.parquet"))
    new += _signal_findings(cid, summarize_signals(_real_losses(sig_rows, executors, rules)))
    log_cats = {f"log:{s.name}" for s in rules.signals}
    calibrate(findings, rules)
    demote_orphan_failures(findings, queries)
    allf = add_findings(findings, runs, new, replace=(*CATEGORIES, *WORKLOAD_CATEGORIES, *CAPACITY_CATEGORIES, *log_cats))
    demote_orphan_failures(allf, queries)  # the recomputed log findings too
    write_parquet(allf, d / "findings.parquet", "findings")
    if (d / "query_profile.parquet").exists():
        qp = _rows(d / "query_profile.parquet")
        recount_queries(qp, allf)
        _resum_io(qp, stages, qk)
        write_parquet(qp, d / "query_profile.parquet", "query_profile")

    # ---- incidents with the later findings in them, and the per-minute task numbers spread over each task's run --
    log_errors = _refresh_user_frames(d / "log_errors.parquet", rules)
    if (d / "incidents.parquet").exists():
        incidents = build_incidents(cid, allf, stages, executors, jobs, queries, signals, log_errors, tms,
                                    _rows(d / "task_retries.parquet"), _rows(d / "apps.parquet"))
        write_parquet(incidents, d / "incidents.parquet", "incidents")
    else:
        incidents = []
    if tms is not None and not tms.empty and (d / "spill_shuffle_timeline.parquet").exists():
        write_parquet(spill_shuffle_timeline(tms, cid, rules), d / "spill_shuffle_timeline.parquet", "spill_shuffle_timeline")
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
        fresh = build_summary({
            "cluster_id": cid, "input_dir": s.get("input_dir"), "empty_reason": s.get("empty_reason"),
            "counts": counts, "totals": s.get("totals", {}), "rows": s.get("rows", {}), "apps": _rows(d / "apps.parquet"),
            "spark_jobs": jobs, "sql_queries": queries, "stages": stages, "executors": executors, "findings": allf,
            "log_signals": signals, "log_errors": log_errors, "error_summary": summarize_errors(log_errors),
            "run_story": _rows(d / "run_story.parquet"), "log_min_ts": None, "log_max_ts": None,
            "task_retries": _rows(d / "task_retries.parquet"), "cluster_info": info,
            "hotspots": _rows(d / "hotspots.parquet"), "incidents": incidents, "has_runs": bool(runs),
        }, rules)
        for k in ("status", "diagnosis", "findings_by_severity", "retries", "analyzer_revision"):
            s[k] = fresh[k]
        if isinstance(s.get("runs"), dict) and runs:
            s["runs"].update({"count": len(runs), "default_run": default_run(runs), "note": runs_note(runs)})
        # the row counts the Raw data list shows, for the datasets rewritten here
        rows_ = s.setdefault("rows", {})
        for name in ("findings", "incidents", "runs", "stages", "spark_jobs", "sql_queries", "spill_shuffle_timeline",
                     "query_profile", "task_retries"):
            if (d / f"{name}.parquet").exists():
                rows_[name] = pq.read_metadata(d / f"{name}.parquet").num_rows
        s["refreshed_at"] = fresh["built_at"]
        tmp = d / "summary.json.tmp"
        tmp.write_text(json.dumps(s, indent=2, default=str), "utf-8")
        os.replace(tmp, sp)
    by: dict[str, int] = {}
    for f in new:
        by[f["category"]] = by.get(f["category"], 0) + 1
    return {"added": len(new), "by_category": by}
