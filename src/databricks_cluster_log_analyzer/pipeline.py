"""build(): one cluster folder -> every dataset as Parquet + summary.json under output/<cluster_id>/."""

from __future__ import annotations

import gc
import json
import os
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from .analysis.aggregate import build_event_datasets
from .analysis.combined import (StoryLogCollector, busy_wall, executor_busy, executor_profile, gc_profile, log_stats_add_gc, log_stats_new,
                                query_profile, run_story, spill_shuffle_timeline, stage_executor_profile)
from .analysis.diagnosis import EMPTY_REASON, build_summary
from .analysis.hotspots import build_hotspots
from .analysis.runs import attach_run_keys, default_run, runs_note
from .analysis.capacity import capacity_findings, core_use
from .analysis.contention import add_findings, contention_findings, first_tasks
from .analysis.initscripts import init_scripts, reader
from .analysis.workload import read_split, workload_findings
from .analysis.findings import build_findings_rows, build_timeline, gc_stuck, summarize_errors, task_oom_rows
from .analysis.incidents import build_incidents
from .config import Rules, load_rules
from .parsing.eventlog import EventTables, parse_eventlog_dir
from .parsing.loglines import SIGNAL_LINE_MAX, ExceptionGrouper, parse_gc_line, parse_log_file
from .readers import classify_files, folder_of, list_all_files, log_file_ids, open_text_lines, spark_context_of
from .schemas import DATASETS
from .store import BatchWriter, load_duckdb, write_parquet


@dataclass
class BuildReport:
    cluster_id: str
    input_dir: str
    output_dir: str
    summary: dict
    rows: dict[str, int] = field(default_factory=dict)
    seconds: float = 0.0
    files_read: int = 0
    duckdb_path: str | None = None

    def summary_lines(self) -> list[str]:
        s = self.summary
        c = s["counts"]
        f = s["findings_by_severity"]
        out = [f"cluster {self.cluster_id}: built in {self.seconds:.1f}s -> {self.output_dir}"]
        if s.get("empty_reason"):
            out.append(f"  EMPTY: {s['empty_reason']}")
        out.append(f"  status {s['status']}; files read {self.files_read} of {c['files']}; log lines {c['log_lines']:,}; "
                   f"events {c['events']:,} ({c['malformed_event_lines']} malformed lines)")
        out.append(f"  apps {c['apps']}, spark jobs {c['spark_jobs']} ({c['failed_jobs']} failed), stages {c['stages']} "
                   f"({c['failed_stages']} failed), tasks {c['tasks']:,} ({c['failed_tasks']} failed), queries "
                   f"{c['sql_queries']} ({c['failed_queries']} failed), executors {c['executors']} ({c['executors_lost']} lost)")
        out.append(f"  findings: {f.get('high', 0)} high, {f.get('medium', 0)} medium, {f.get('low', 0)} low")
        out.append("  rows: " + ", ".join(f"{k}={v:,}" for k, v in self.rows.items()))
        if self.duckdb_path:
            out.append(f"  duckdb: {self.duckdb_path}")
        return out


def _signal_row(r: dict) -> dict:
    return {"cluster_id": r["cluster_id"], "source": r["source"], "app_id": r["app_id"],
            "executor_id": r["executor_id"], "file_path": r["file_path"], "file_name": r["file_name"],
            "seq": r["seq"], "ts": r["ts"], "level": r["level"], "signal": r["signal"], "severity": r["severity"],
            "fix": r["fix"], "line": r["line"][:SIGNAL_LINE_MAX]}


def build(cluster_dir: str | os.PathLike, output_root: str | os.PathLike, *, cluster_id: str | None = None,
          rules: Rules | str | os.PathLike | None = None, duckdb: bool = False,
          progress: Callable[[str], None] | None = None) -> BuildReport:
    t0 = time.time()
    cluster_dir = Path(cluster_dir).expanduser().resolve()
    if not cluster_dir.is_dir():
        raise FileNotFoundError(f"Cluster folder not found: {cluster_dir}")
    cid = cluster_id or cluster_dir.name
    if not isinstance(rules, Rules):
        rules = load_rules(rules)
    out_dir = Path(output_root).expanduser().resolve() / cid
    out_dir.mkdir(parents=True, exist_ok=True)
    say = progress or (lambda m: None)
    warnings: list[str] = []

    # ---- files ---------------------------------------------------------------------------------------------
    files = [{"cluster_id": cid, "folder": folder_of(p), "path": p, "size": size, "modified": int(mtime * 1000)}
             for p, size, mtime in list_all_files(cluster_dir)]
    classified = classify_files(cluster_dir)
    log_files = classified["driver"] + classified["executor"]
    empty_reason = None if (log_files or classified["eventlog"]) else EMPTY_REASON.format(dir=cluster_dir)

    # ---- driver + executor logs (streamed) -----------------------------------------------------------------
    writer = BatchWriter(out_dir / "log_lines.parquet", "log_lines")
    grouper = ExceptionGrouper(rules, cid)
    story = StoryLogCollector(rules)
    log_signals: list[dict] = []
    log_errors: list[dict] = []
    gc_events: list[dict] = []
    file_lines: list[dict] = []
    sizes = {f["path"]: f["size"] for f in files}
    log_stats: dict[tuple, dict] = {}
    levels: Counter = Counter()
    seq = 0
    min_ts = max_ts = None
    gc_was_enabled = gc.isenabled()
    gc.disable()  # millions of small, acyclic objects: the cyclic GC only adds (super-linear) overhead here
    try:
        for path in log_files:
            rel = path.relative_to(cluster_dir).as_posix()
            source, app_id, ex = log_file_ids(rel)
            say(f"reading {rel}")
            st = log_stats.setdefault((source, app_id, ex), log_stats_new())
            n0 = seq
            for row in parse_log_file(open_text_lines(path, on_error=warnings.append), source=source,
                                      executor_id=ex, app_id=app_id, file_path=rel, file_name=path.name,
                                      start_seq=seq, rules=rules, cluster_id=cid):
                writer.add(row)
                seq = row["seq"] + 1
                lvl = row["level"]
                cont = row["continuation"]
                if lvl is not None and not cont:  # level counts are per log record, not per continuation line
                    levels[lvl] += 1
                    if lvl in ("ERROR", "FATAL"):
                        st["error_lines"] += 1
                    elif lvl == "WARN":
                        st["warn_lines"] += 1
                ts = row["ts"]
                if ts is not None:
                    if min_ts is None or ts < min_ts:
                        min_ts = ts
                    if max_ts is None or ts > max_ts:
                        max_ts = ts
                    if st["min_ts"] is None or ts < st["min_ts"]:
                        st["min_ts"] = ts
                    if st["max_ts"] is None or ts > st["max_ts"]:
                        st["max_ts"] = ts
                if row["signal"] is not None:
                    log_signals.append(_signal_row(row))
                    st["signals"][row["signal"]] += 1
                    story.feed(row)
                elif lvl in ("ERROR", "FATAL") and not cont:
                    story.feed(row)
                if row["logger"] is not None or "GC (" in row["line"]:
                    g = parse_gc_line(row)
                    if g is not None:
                        gc_events.append({"cluster_id": cid, "source": source, "app_id": app_id,
                                          "executor_id": ex, "file_path": rel, "seq": row["seq"], "ts": ts, **g})
                        log_stats_add_gc(st, g, row)
                found = grouper.feed(row)
                if found:
                    log_errors.extend(found)
            st["lines"] += seq - n0
            log_errors.extend(grouper.flush())
            file_lines.append({"cluster_id": cid, "folder": source, "file_path": rel, "lines": seq - n0,
                               "bytes": sizes.get(rel), "thread_dumps": grouper.dump_counts.get(rel, 0)})
        n_log_lines = writer.close()
    except BaseException:
        writer.abort()
        raise
    finally:
        if gc_was_enabled:
            gc.enable()

    # ---- event logs (one spark context at a time) ----------------------------------------------------------
    by_ctx: dict[str, list[Path]] = {}
    for p in classified["eventlog"]:
        by_ctx.setdefault(spark_context_of(p.relative_to(cluster_dir).as_posix()), []).append(p)
    ev = EventTables(cid, "")
    gc.disable()
    try:
        for ctx, paths in by_ctx.items():
            say(f"reading event log {ctx} ({len(paths)} files)")
            ev.extend(parse_eventlog_dir(paths, cluster_id=cid, spark_context_id=ctx, rules=rules))
    finally:
        if gc_was_enabled:
            gc.enable()
    warnings += ev.warnings
    for fl in ev.file_lines:
        rel = Path(fl["path"]).relative_to(cluster_dir).as_posix()
        file_lines.append({"cluster_id": cid, "folder": "eventlog", "file_path": rel, "lines": fl["lines"],
                           "bytes": sizes.get(rel), "thread_dumps": 0})
    ds = build_event_datasets(ev, rules)
    tdf, stages, jobs, queries, executors, apps = (ds["tasks"], ds["stages"], ds["spark_jobs"], ds["sql_queries"],
                                                   ds["executors"], ds["apps"])

    # ---- analysis ------------------------------------------------------------------------------------------
    say("analyzing")
    retries = ds["task_retries"]
    busy = executor_busy(tdf, cid)
    ep = executor_profile(executors, apps, tdf, log_stats, log_errors, rules, cid, wall=busy_wall(busy))
    gcp = gc_profile(ep, log_stats)
    task_ooms = task_oom_rows(tdf)
    findings = build_findings_rows({"cluster_id": cid, "stages": stages, "executors": executors,
                                    "sql_queries": queries, "log_signals": log_signals, "log_errors": log_errors,
                                    "task_retries": retries, "gc_profile": gcp, "task_ooms": task_ooms,
                                    "executor_profile": ep},
                                   rules)
    timeline = build_timeline(log_signals, cid)
    qp = query_profile(queries, jobs, stages, tdf, executors, findings, rules)
    sep = stage_executor_profile(tdf, cid)
    sst = spill_shuffle_timeline(tdf, cid, rules)
    hotspots = build_hotspots(tdf, stages, queries, sst, rules, cid,
                              stuck={(g["spark_context_id"], g["executor_id"]) for g in gcp if gc_stuck(g, rules)})
    story_rows, story_dropped = run_story(cid, apps, jobs, stages, queries, executors, findings, story.result(),
                                          rules, pruned=story.pruned, task_retries=retries)

    # ---- runs (Revision 6): run_key on run-scoped datasets + shared-resource attribution -----------------------
    run_tables = {"spark_jobs": jobs, "stages": stages, "sql_queries": queries, "query_profile": qp,
                  "connect_operations": ds["connect_operations"], "task_retries": retries, "hotspots": hotspots,
                  "findings": findings, "run_story": story_rows, "tasks": tdf, "stage_executor_profile": sep,
                  "executor_profile": ep}
    runs = attach_run_keys(cid, run_tables)
    # Revision 17: findings that need the runs (waiting for cores, cores full) and MERGE that reads too much
    # the queue on a full cluster (every wave of tasks), autoscaling, caches, counts, DDL loops, reads by source
    read_split(queries, stages, ds["sql_plan_nodes"])
    later = (capacity_findings(cid, tdf, stages, executors, runs, ds["cluster_info"], log_signals, rules)
             + workload_findings(cid, queries, runs, executors, log_signals, ds["event_counts"], rules, stages))
    init_summary, init_found = init_scripts(cid, files, reader(cluster_dir, open_text_lines), executors)
    later += init_found
    bound = {f["run_key"] for f in later if f["category"] == "capacity_bound"}
    later += [f for f in contention_findings(cid, stages, first_tasks(tdf), runs, executors, queries,
                                             ds["cluster_info"], rules, ds["sql_plan_nodes"])
              if not (f["category"] == "waited_for_cores" and f.get("run_key") in bound)]
    findings = add_findings(findings, runs, later)
    # incidents after the later findings: a cache bigger than memory and autoscaling that removed executors mid-work
    # are often the cause of the out of memory and fetch failures
    incidents = build_incidents(cid, findings, stages, executors, jobs, queries, log_signals, log_errors, tdf, retries, apps)

    # ---- write ---------------------------------------------------------------------------------------------
    datasets = {"files": files, "apps": apps, "log_signals": log_signals, "log_errors": log_errors, "tasks": tdf,
                "stages": stages, "spark_jobs": jobs, "sql_queries": queries, "executors": executors,
                "findings": findings, "timeline": timeline, "query_profile": qp, "stage_executor_profile": sep,
                "executor_profile": ep, "run_story": story_rows, "event_counts": ds["event_counts"],
                "file_lines": file_lines, "gc_events": gc_events, "connect_operations": ds["connect_operations"],
                "cluster_info": ds["cluster_info"], "task_retries": retries, "spill_shuffle_timeline": sst, "hotspots": hotspots,
                "sql_plan_nodes": ds["sql_plan_nodes"], "incidents": incidents,
                "runs": runs, "executor_busy": busy, "settings": ds["settings"]}
    rows = {"log_lines": n_log_lines}
    for name, data in datasets.items():
        rows[name] = write_parquet(data, out_dir / f"{name}.parquet", name)
    rows = {k: rows[k] for k in DATASETS}

    def tsum(c):
        return int(tdf[c].sum()) if not tdf.empty and tdf[c].notna().any() else 0

    run_sum = tsum("run_ms")
    counts = {
        "files": len(files), "log_lines": n_log_lines, "events": sum(a["events_read"] for a in apps),
        "apps": len(apps), "spark_jobs": len(jobs), "failed_jobs": sum(j["result"] == "JobFailed" for j in jobs),
        "stages": len(stages), "failed_stages": sum(s["status"] == "failed" for s in stages),
        "tasks": len(tdf), "failed_tasks": int(tdf["failed"].sum()) if not tdf.empty else 0,
        "sql_queries": len(queries), "failed_queries": sum(q["status"] == "failed" for q in queries),
        "executors": len(executors),
        # each removal class once: an executor that ran out of memory is not also counted as lost
        "executors_lost": sum(e["removal_category"] == "lost" for e in executors),
        "executors_oom": sum(e["removal_category"] == "oom" for e in executors),
        "executors_killed": sum(e["removal_category"] == "killed" for e in executors),
        "executors_autoscaled": sum(e["removal_category"] == "autoscale" for e in executors),
        "log_errors": len(log_errors), "error_lines": levels["ERROR"] + levels["FATAL"],
        "warn_lines": levels["WARN"], "signals": len(log_signals), "findings": len(findings),
        "story_rows": len(story_rows), "story_dropped": story_dropped,
        "malformed_event_lines": sum(a["malformed_lines"] for a in apps),
        "gc_events": len(gc_events),
        "full_gcs": sum(1 for g in gc_events if (g["kind"] or "").startswith("Pause Full")),
        "thread_dumps": sum(f["thread_dumps"] for f in file_lines),
        "connect_operations": len(ds["connect_operations"]),
        "failed_connect_operations": sum(o["status"] == "failed" for o in ds["connect_operations"]),
        "task_retries": len(retries),
        "retried_stages": len({(r["spark_context_id"], r["stage_id"], r["stage_attempt"]) for r in retries}),
        "stage_retries": sum(1 for s in stages if s["stage_attempt"]),
    }
    totals = {"mem_spill": tsum("mem_spill"), "disk_spill": tsum("disk_spill"),
              "gc_share": round(tsum("gc_ms") / run_sum, 3) if run_sum else 0.0,
              "input_bytes": tsum("input_bytes"), "shuffle_read": tsum("shuffle_read"),
              "shuffle_write": tsum("shuffle_write"), "output_bytes": tsum("output_bytes"), "task_ms": tsum("task_ms")}
    summary = build_summary({
        "cluster_id": cid, "input_dir": str(cluster_dir), "empty_reason": empty_reason, "counts": counts,
        "totals": totals, "rows": rows, "apps": apps, "spark_jobs": jobs, "sql_queries": queries, "stages": stages,
        "executors": executors, "findings": findings, "log_signals": log_signals, "log_errors": log_errors,
        "error_summary": summarize_errors(log_errors), "run_story": story_rows, "log_min_ts": min_ts,
        "log_max_ts": max_ts, "warnings": warnings, "task_retries": retries,
        "cluster_info": ds["cluster_info"], "hotspots": hotspots, "incidents": incidents, "has_runs": bool(runs),
    }, rules)
    summary["core_use"] = core_use(tdf, executors, apps)
    summary["init_scripts"] = init_summary
    summary["timeline_bucket_seconds"] = rules.timeline_bucket_seconds  # bucket of spill_shuffle_timeline
    summary["runs"] = {"count": len(runs), "default_run": default_run(runs), "note": runs_note(runs),
                       "overlapping": sum(1 for r in runs if r["overlapping_runs"])}
    tmp = out_dir / "summary.json.tmp"
    tmp.write_text(json.dumps(summary, indent=2, default=str), "utf-8")
    os.replace(tmp, out_dir / "summary.json")

    report = BuildReport(cid, str(cluster_dir), str(out_dir), summary, rows, round(time.time() - t0, 2),
                         files_read=len(log_files) + len(classified["eventlog"]))
    if duckdb:
        say("loading DuckDB")
        report.duckdb_path = load_duckdb(out_dir.parent, cid)
    return report
