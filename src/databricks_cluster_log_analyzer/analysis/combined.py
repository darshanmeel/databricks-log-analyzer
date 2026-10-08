"""Combined datasets: query_profile, stage_executor_profile, executor_profile, run_story."""

from __future__ import annotations

import re
from collections import Counter

import numpy as np
import pandas as pd

from ..config import FAILURE_REMOVALS, SEVERITY_RANK, Rules
from ..util import fmt_ms, fmt_words, to_int
from .retries import category_text, stage_retry_summary

DIGITS_RE = re.compile(r"\d+")
SEVERITIES = ("high", "medium", "low", "info")


def _max_sev(sevs) -> str | None:
    sevs = [s for s in sevs if s]
    return min(sevs, key=lambda s: SEVERITY_RANK.get(s, 9)) if sevs else None


def _sum(vals):
    vals = [v for v in vals if v is not None]
    return sum(vals) if vals else None


def _max(vals):
    vals = [v for v in vals if v is not None]
    return max(vals) if vals else None


# --------------------------------------------------------------------------------------------------------------
# query_profile
# --------------------------------------------------------------------------------------------------------------

def query_profile(queries: list[dict], jobs: list[dict], stages: list[dict], tdf: pd.DataFrame,
                  executors: list[dict], findings: list[dict], rules: Rules) -> list[dict]:
    jobs_by_q = Counter((j["spark_context_id"], j["sql_execution_id"]) for j in jobs
                        if j["sql_execution_id"] is not None)
    stages_by_q: dict[tuple, list[dict]] = {}
    for s in stages:
        if s["sql_execution_id"] is not None:
            stages_by_q.setdefault((s["spark_context_id"], s["sql_execution_id"]), []).append(s)
    # distinct executors running tasks of each query's stages
    execs_by_q: dict[tuple, int] = {}
    if not tdf.empty and stages_by_q:
        m = pd.DataFrame([{"spark_context_id": s["spark_context_id"], "stage_id": float(s["stage_id"]),
                           "stage_attempt": float(s["stage_attempt"]), "_q": s["sql_execution_id"]}
                          for ss in stages_by_q.values() for s in ss])
        t = tdf[["spark_context_id", "stage_id", "stage_attempt", "executor_id"]].copy()
        t["stage_attempt"] = t["stage_attempt"].fillna(0)
        j = t.merge(m, on=["spark_context_id", "stage_id", "stage_attempt"], how="inner")
        if not j.empty:
            for (ctx, q), n in j.groupby(["spark_context_id", "_q"])["executor_id"].nunique().items():
                execs_by_q[(ctx, int(q))] = int(n)
    find_by_q: dict[tuple, list[str]] = {}
    for f in findings:
        if f.get("sql_execution_id") is not None:
            find_by_q.setdefault((f["spark_context_id"], f["sql_execution_id"]), []).append(f["severity"])
    removed = [(e["spark_context_id"], e["removed_time"]) for e in executors
               if e["removed_time"] is not None and _removal_cat(e, rules) in FAILURE_REMOVALS]

    out = []
    for q in queries:
        k = (q["spark_context_id"], q["sql_execution_id"])
        ss = stages_by_q.get(k, [])
        gc, run = _sum(s.get("_gc_ms") for s in ss), _sum(s.get("_run_ms") for s in ss)
        start, end = q["start_time"], q["end_time"]
        lost = None
        if start is not None:
            lost = sum(1 for ctx, t in removed if ctx == k[0] and t >= start and (end is None or t <= end))
        sev = find_by_q.get(k, [])
        out.append({
            "cluster_id": q["cluster_id"], "spark_context_id": k[0], "sql_execution_id": k[1],
            "description": q["description"], "status": q["status"], "start_time": start, "end_time": end,
            "duration_ms": q["duration_ms"], "error": q["error"], "spark_jobs": jobs_by_q.get(k, 0),
            "stages": len(ss), "total_stage_ms": _sum(s["duration_ms"] for s in ss),
            "max_stage_ms": _max(s["duration_ms"] for s in ss), "tasks": _sum(s["tasks"] for s in ss),
            "failed_tasks": _sum(s["failed_tasks"] for s in ss), "mem_spill": _sum(s["mem_spill"] for s in ss),
            "disk_spill": _sum(s["disk_spill"] for s in ss),
            "gc_share": round(gc / run, 3) if gc is not None and run else None,
            "max_stage_skew": _max(s["skew"] for s in ss), "input_bytes": _sum(s["input_bytes"] for s in ss),
            "shuffle_read": _sum(s["shuffle_read"] for s in ss), "shuffle_write": _sum(s["shuffle_write"] for s in ss),
            "output_bytes": _sum(s["output_bytes"] for s in ss), "executors_used": execs_by_q.get(k, 0 if ss else None),
            "executors_lost": lost, "findings": len(sev), "max_severity": _max_sev(sev),
            "plan_hash": q["plan_hash"],
        })
    return out


# --------------------------------------------------------------------------------------------------------------
# stage_executor_profile
# --------------------------------------------------------------------------------------------------------------

def stage_executor_profile(tdf: pd.DataFrame, cluster_id: str) -> pd.DataFrame:
    cols = ["cluster_id", "spark_context_id", "stage_id", "stage_attempt", "executor_id", "host", "tasks",
            "failed_tasks", "task_ms_sum", "task_ms_max", "run_ms", "gc_ms", "mem_spill", "disk_spill",
            "input_bytes", "shuffle_read", "share_of_stage_ms"]
    if tdf.empty:
        return pd.DataFrame(columns=cols)
    t = tdf[tdf["stage_id"].notna()].copy()
    t["stage_attempt"] = t["stage_attempt"].fillna(0)
    t["executor_id"] = t["executor_id"].astype(object).where(t["executor_id"].notna(), None)
    keys = ["spark_context_id", "stage_id", "stage_attempt", "executor_id"]
    g = t.groupby(keys, dropna=False, sort=True)
    df = pd.DataFrame({
        "host": g["host"].first(),
        "tasks": g.size(),
        "failed_tasks": g["failed"].sum(),
        "task_ms_max": g["task_ms"].max(),
    })
    sums = g[["task_ms", "run_ms", "gc_ms", "mem_spill", "disk_spill", "input_bytes", "shuffle_read"]].sum(min_count=1)
    df = df.join(sums.rename(columns={"task_ms": "task_ms_sum"})).reset_index()
    stage_tot = df.groupby(["spark_context_id", "stage_id", "stage_attempt"])["task_ms_sum"].transform("sum")
    df["share_of_stage_ms"] = (df["task_ms_sum"] / stage_tot.where(stage_tot > 0)).round(4)
    df["cluster_id"] = cluster_id
    return df[cols]


# --------------------------------------------------------------------------------------------------------------
# spill_shuffle_timeline (Revision 4)
# --------------------------------------------------------------------------------------------------------------

SPILL_SHUFFLE_COLS = ["cluster_id", "spark_context_id", "minute", "executor_id", "stage_id", "stage_attempt",
                      "tasks", "mem_spill", "disk_spill", "shuffle_read", "shuffle_write", "input_bytes",
                      "output_bytes", "gc_ms", "run_ms"]
_SPILL_SHUFFLE_SUMS = ["mem_spill", "disk_spill", "shuffle_read", "shuffle_write", "input_bytes", "output_bytes",
                       "gc_ms", "run_ms"]


def spill_shuffle_timeline(tdf: pd.DataFrame, cluster_id: str, rules: Rules) -> pd.DataFrame:
    """Task bytes and times per (context, time bucket, executor, stage attempt). A task's numbers are spread over the
    buckets it ran in, by the share of its run in each (a 4-minute task is not all booked into the minute it ended);
    `tasks` counts a task once, in the bucket it finished. A task with only one of launch and finish time goes to that
    bucket; tasks with neither are left out."""
    if tdf.empty:
        return pd.DataFrame(columns=SPILL_SHUFFLE_COLS)
    bucket_ms = int(rules.timeline_bucket_seconds) * 1000
    end = tdf["finish_time"].where(tdf["finish_time"].notna(), tdf["launch_time"])
    start = tdf["launch_time"].where(tdf["launch_time"].notna(), end)
    keep = end.notna()
    t = tdf.loc[keep, ["spark_context_id", "executor_id", "stage_id", "stage_attempt"] + _SPILL_SHUFFLE_SUMS].copy()
    if t.empty:
        return pd.DataFrame(columns=SPILL_SHUFFLE_COLS)
    s = start[keep].astype("int64").to_numpy()
    e = end[keep].astype("int64").to_numpy()
    s = np.minimum(s, e)
    b0, b1 = s // bucket_ms, e // bucket_ms
    n = (b1 - b0 + 1).astype("int64")
    rep = np.repeat(np.arange(len(t)), n)
    # the bucket of each piece: b0, b0 + 1, ... b1
    first = np.repeat(np.cumsum(n) - n, n)
    b = np.repeat(b0, n) + (np.arange(len(rep)) - first)
    lo = np.maximum(np.repeat(s, n), b * bucket_ms)
    hi = np.minimum(np.repeat(e, n), (b + 1) * bucket_ms)
    span = np.repeat(e - s, n)
    frac = np.where(span > 0, (hi - lo) / np.where(span > 0, span, 1), 1.0)
    x = t.iloc[rep].reset_index(drop=True)
    for c in _SPILL_SHUFFLE_SUMS:
        x[c] = x[c] * frac
    x["minute"] = b * bucket_ms
    x["_task"] = (b == np.repeat(b1, n)).astype("int64")
    x["stage_attempt"] = x["stage_attempt"].fillna(0)
    x["executor_id"] = x["executor_id"].astype(object).where(x["executor_id"].notna(), None)
    keys = ["spark_context_id", "minute", "executor_id", "stage_id", "stage_attempt"]
    g = x.groupby(keys, dropna=False, sort=True)
    df = g[_SPILL_SHUFFLE_SUMS].sum(min_count=1).join(g["_task"].sum().rename("tasks")).reset_index()
    for c in _SPILL_SHUFFLE_SUMS:  # whole bytes and milliseconds, as before
        df[c] = df[c].round()
    df["cluster_id"] = cluster_id
    return df[SPILL_SHUFFLE_COLS]


# --------------------------------------------------------------------------------------------------------------
# executor_busy (Revision 12)
# --------------------------------------------------------------------------------------------------------------

BUSY_COLS = ["cluster_id", "spark_context_id", "executor_id", "busy_start", "busy_end", "tasks", "task_ms"]


def executor_busy(tdf: pd.DataFrame, cluster_id: str, merge_gap_ms: int = 2000) -> pd.DataFrame:
    """When each executor was running at least one task: task intervals merged per executor (a gap shorter than
    `merge_gap_ms` does not split a stretch). `tasks` / `task_ms` = tasks and summed task time in the stretch.
    Everything outside these stretches, between the executor's added and removed time, it was up and idle."""
    if tdf.empty:
        return pd.DataFrame(columns=BUSY_COLS)
    t = tdf[tdf["launch_time"].notna() & tdf["finish_time"].notna() & tdf["executor_id"].notna()
            & (tdf["executor_id"] != "driver")]
    if t.empty:
        return pd.DataFrame(columns=BUSY_COLS)
    t = t[["spark_context_id", "executor_id", "launch_time", "finish_time", "task_ms"]].sort_values(
        ["spark_context_id", "executor_id", "launch_time"], kind="mergesort")
    key = ["spark_context_id", "executor_id"]
    reach = t.groupby(key, sort=False)["finish_time"].cummax()
    prev = reach.groupby([t["spark_context_id"], t["executor_id"]], sort=False).shift()
    new = prev.isna() | (t["launch_time"] > prev + merge_gap_ms)
    t = t.assign(_grp=new.cumsum())
    g = t.groupby(key + ["_grp"], sort=False)
    df = pd.DataFrame({"busy_start": g["launch_time"].min(), "busy_end": g["finish_time"].max(),
                       "tasks": g.size(), "task_ms": g["task_ms"].sum(min_count=1)}).reset_index()
    df["cluster_id"] = cluster_id
    return df[BUSY_COLS]


def busy_wall(busy: pd.DataFrame) -> dict[tuple, int]:
    """(ctx, executor) -> wall-clock ms with at least one task running."""
    if busy.empty:
        return {}
    d = (busy["busy_end"] - busy["busy_start"]).groupby([busy["spark_context_id"], busy["executor_id"]]).sum()
    return {k: int(v) for k, v in d.items()}


# --------------------------------------------------------------------------------------------------------------
# executor_profile
# --------------------------------------------------------------------------------------------------------------

def _removal_cat(e: dict, rules: Rules):
    c = e.get("removal_category")
    return c if c is not None else rules.removal_category(e.get("removed_reason"))


def executor_profile(executors: list[dict], apps: list[dict], tdf: pd.DataFrame, log_stats: dict,
                     log_errors: list[dict], rules: Rules, cluster_id: str,
                     wall: dict[tuple, int] | None = None) -> list[dict]:
    """`log_stats`: {(source, app_id, executor_id): {"lines", "error_lines", "warn_lines", "signals": Counter}}."""
    ctx_app = {a["spark_context_id"]: a["app_id"] for a in apps}
    app_ctx = {a["app_id"]: a["spark_context_id"] for a in apps if a["app_id"]}
    ctx_end = {a["spark_context_id"]: a["end_time"] for a in apps}

    task_agg: dict[tuple, dict] = {}
    if not tdf.empty:
        g = tdf.groupby(["spark_context_id", "executor_id"], dropna=True, sort=False)
        agg = pd.DataFrame({"tasks": g.size(), "failed_tasks": g["failed"].sum(), "max_peak_mem": g["peak_mem"].max()})
        agg = agg.join(g[["task_ms", "run_ms", "gc_ms", "mem_spill", "disk_spill"]].sum(min_count=1))
        for (ctx, ex), row in agg.iterrows():
            task_agg[(ctx, ex)] = {k: to_int(v) for k, v in row.items()}

    exc_by: dict[tuple, Counter] = {}
    for e in log_errors:
        if e["source"] == "executor":
            exc_by.setdefault((e.get("app_id"), e.get("executor_id")), Counter())[e["exception_class"]] += 1

    exec_logs = {(k[1], k[2]): v for k, v in log_stats.items() if k[0] == "executor"}
    unmapped = {k for k in exec_logs if k[0] not in app_ctx}

    def logs_for(app_id, ex):
        keys = []
        if app_id is not None and (app_id, ex) in exec_logs:
            keys = [(app_id, ex)]
        else:  # fallback: executor_id only, among logs whose app_id matches no known app
            keys = [k for k in unmapped if k[1] == ex]
        return keys

    used_log_keys = set()
    out = []

    def make(ctx, app_id, ex, ev: dict | None):
        ev = ev or {}
        ta = task_agg.get((ctx, ex), {})
        keys = logs_for(app_id, ex)
        used_log_keys.update(keys)
        lines = err = warn = 0
        sig = Counter()
        exc = Counter()
        gcs = gc_full = gc_stuck = 0
        shares: list[float] = []
        gc_ms = 0.0
        heap_after = heap_total = None
        first_full = None
        for k in keys:
            st = exec_logs[k]
            lines += st["lines"]
            err += st["error_lines"]
            warn += st["warn_lines"]
            sig.update(st["signals"])
            exc.update(exc_by.get(k, Counter()))
            gcs += st["gc_pauses"]
            gc_full += st["full_gcs"]
            gc_stuck += st.get("stuck_full_gcs", 0)
            shares += st.get("full_after_shares", [])
            gc_ms += st["gc_pause_ms"]
            if st["max_heap_after_mb"] is not None:
                heap_after = max(heap_after or 0.0, st["max_heap_after_mb"])
            if st["heap_total_mb"] is not None:
                heap_total = max(heap_total or 0.0, st["heap_total_mb"])
            if st["first_full_gc"] and (first_full is None or (st["first_full_gc"][0] or 0) < (first_full[0] or 0)):
                first_full = st["first_full_gc"]
        added, removed = ev.get("added_time"), ev.get("removed_time")
        end = removed if removed is not None else ctx_end.get(ctx)
        lifetime = (end - added) if added is not None and end is not None else None
        cores = ev.get("cores")
        busy = ta.get("task_ms")
        busy_share = None
        if busy is not None and lifetime and cores:
            busy_share = round(max(0.0, min(1.0, busy / (lifetime * cores))), 4)
        run, gc = ta.get("run_ms"), ta.get("gc_ms")
        has_logs = bool(keys)
        # Revision 12: wall-clock time with a task running vs up and idle (paid for, doing nothing)
        bw = (wall or {}).get((ctx, ex))
        if bw is not None and lifetime is not None:
            bw = min(bw, lifetime)
        idle = (lifetime - (bw or 0)) if lifetime is not None else None
        out.append({
            "cluster_id": cluster_id, "spark_context_id": ctx, "app_id": app_id, "executor_id": ex,
            "host": ev.get("host"), "cores": cores, "added_time": added, "removed_time": removed,
            "lifetime_ms": lifetime, "removed_reason": ev.get("removed_reason"),
            "removal_category": _removal_cat(ev, rules) if ev else None,
            "removed_reason_raw": ev.get("removed_reason_raw"),
            "tasks": ta.get("tasks", 0), "failed_tasks": ta.get("failed_tasks", 0), "busy_ms": busy,
            "busy_share": busy_share, "run_ms": run, "gc_ms": gc,
            "busy_wall_ms": bw, "idle_ms": idle,
            "idle_core_ms": (lifetime * cores - (busy or 0)) if lifetime is not None and cores else None,
            **{c: ev.get(c) for c in ("resource_profile_id", "heap_mb", "overhead_mb", "offheap_mb",
                                      "unified_memory", "storage_memory", "task_cpus")},
            "gc_share": round(gc / run, 3) if gc is not None and run else None,
            "mem_spill": ta.get("mem_spill"), "disk_spill": ta.get("disk_spill"),
            "max_peak_mem": ta.get("max_peak_mem"),
            "log_lines": lines if has_logs else None, "log_errors_lines": err if has_logs else None,
            "log_warn_lines": warn if has_logs else None, "signals": sum(sig.values()) if has_logs else None,
            "top_signals": [s for s, _ in sig.most_common(5)], "exceptions": sum(exc.values()) if has_logs else None,
            "top_exception": exc.most_common(1)[0][0] if exc else None,
            "gc_pauses": gcs if has_logs else None, "gc_pause_ms": round(gc_ms, 3) if has_logs else None,
            "full_gcs": gc_full if has_logs else None, "max_heap_after_mb": heap_after,
            # GC from the GC log, per executor: task "JVM GC Time" counts one pause once per running task
            "gc_pause_share": round(gc_ms / lifetime, 4) if has_logs and gcs and lifetime else None,
            "full_gc_heap_after_p50": round(sorted(shares)[len(shares) // 2], 3) if shares else None,
            # internal (not written): for the jvm_full_gc finding
            "_heap_total_mb": heap_total, "_first_full_gc": first_full, "_stuck_full_gcs": gc_stuck,
        })

    for e in executors:
        make(e["spark_context_id"], ctx_app.get(e["spark_context_id"]), e["executor_id"], e)
    # executors seen only in tasks (no ExecutorAdded/Removed event)
    seen = {(e["spark_context_id"], e["executor_id"]) for e in executors}
    for (ctx, ex) in task_agg:
        if (ctx, ex) not in seen and ex != "driver":
            seen.add((ctx, ex))
            make(ctx, ctx_app.get(ctx), ex, None)
    # executor log folders with no matching executor in the event log
    for k in exec_logs:
        if k not in used_log_keys:
            app_id, ex = k
            ctx = app_ctx.get(app_id)
            if (ctx, ex) in seen:
                continue
            make(ctx, app_id, ex, None)
    return out


# --------------------------------------------------------------------------------------------------------------
# run_story
# --------------------------------------------------------------------------------------------------------------

class StoryLogCollector:
    """Collects ERROR/FATAL and signal-matched log lines while streaming, collapsing identical
    (source, app, executor, signal or logger, message without digits) rows within the same minute."""

    def __init__(self, rules: Rules, max_keys: int | None = None):
        self.rules = rules
        self.rows: dict[tuple, dict] = {}
        self.max_keys = max_keys or max(50_000, rules.story_max_log_rows * 20)
        self.pruned = 0

    def feed(self, r: dict) -> None:
        sig = r["signal"]
        level = r["level"]
        if sig is None and level not in ("ERROR", "FATAL"):
            return
        ts = r["ts"]
        msg = r["message"] or ""
        key = (None if ts is None else ts // 60000, r["source"], r["app_id"], r["executor_id"], sig or r["logger"],
               DIGITS_RE.sub("", msg[:300]))
        row = self.rows.get(key)
        if row is not None:
            row["count"] += 1
            return
        if sig is not None:
            kind, sev = "log_signal", r["severity"]
        else:
            kind, sev = "log_error", "high" if level == "FATAL" else "medium"
        self.rows[key] = {"ts": ts, "kind": kind, "severity": sev, "signal": sig, "level": level,
                          "logger": r["logger"], "message": msg[:1000], "line": r["line"][:1000],
                          "source": r["source"], "app_id": r["app_id"], "executor_id": r["executor_id"],
                          "file_path": r["file_path"], "seq": r["seq"], "count": 1}
        if len(self.rows) > self.max_keys:
            self._prune()

    def _prune(self) -> None:
        keep = max(self.rules.story_max_log_rows * 2, 1000)
        items = sorted(self.rows.items(), key=lambda kv: (SEVERITY_RANK.get(kv[1]["severity"], 9),
                                                          kv[1]["ts"] is None, kv[1]["ts"] or 0))
        self.pruned += len(items) - keep
        self.rows = dict(items[:keep])

    def result(self) -> list[dict]:
        return list(self.rows.values())


def _ctx_for_log(r, app_ctx, windows):
    if r["app_id"] and r["app_id"] in app_ctx:
        return app_ctx[r["app_id"]]
    if len(windows) == 1:
        return windows[0][0]
    ts = r["ts"]
    if ts is not None:
        for ctx, s, e in windows:
            if s is not None and ts >= s and (e is None or ts <= e):
                return ctx
    return None


def run_story(cluster_id: str, apps, jobs, stages, queries, executors, findings, log_rows: list[dict],
              rules: Rules, pruned: int = 0, task_retries: list[dict] | None = None) -> tuple[list[dict], int]:
    """Returns (rows, dropped_log_rows)."""
    ev: list[dict] = []

    def add(ctx, ts, kind, severity, title, detail=None, **links):
        r = {"cluster_id": cluster_id, "spark_context_id": ctx, "ts": ts, "kind": kind, "severity": severity,
             "title": title, "detail": detail, "count": 1, "spark_job_id": None, "stage_id": None,
             "stage_attempt": None, "sql_execution_id": None, "executor_id": None, "source": None,
             "log_file_path": None, "log_seq": None, "finding_id": None}
        r.update(links)
        ev.append(r)

    for a in apps:
        ctx = a["spark_context_id"]
        detail = ", ".join(x for x in (f"Spark {a['spark_version']}" if a.get("spark_version") else None,
                                       f"user {a['user']}" if a.get("user") else None,
                                       a.get("app_id")) if x)
        if a["start_time"] is not None:
            add(ctx, a["start_time"], "app_start", "info", f"Spark application started: {a.get('app_name') or ctx}",
                detail or None)
        if a.get("_app_end_seen") and a["end_time"] is not None:
            add(ctx, a["end_time"], "app_end", "info", f"Spark application ended after {fmt_ms(a['duration_ms'])}")
    for j in jobs:
        ctx, jid = j["spark_context_id"], j["spark_job_id"]
        links = {"spark_job_id": jid, "sql_execution_id": j["sql_execution_id"]}
        what = j["description"] or j["call_site"] or ""
        if j["start_time"] is not None:
            add(ctx, j["start_time"], "job_start", "info", f"Spark job {jid} started ({j['num_stages']} stages)",
                what[:500] or None, **links)
        if j["end_time"] is not None:
            failed = j["result"] == "JobFailed"
            verb = ("failed" if failed else "was cancelled by adaptive query execution (the query was re-planned; "
                    "not a failure)" if j["result"] == "JobReplanned" else "succeeded")
            add(ctx, j["end_time"], "job_end", "high" if failed else "info",
                f"Spark job {jid} {verb} after {fmt_ms(j['duration_ms'])}",
                (j["error"] if failed else what[:500]) or None, **links)
    for s in stages:
        ctx = s["spark_context_id"]
        links = {"stage_id": s["stage_id"], "stage_attempt": s["stage_attempt"], "spark_job_id": s["spark_job_id"],
                 "sql_execution_id": s["sql_execution_id"]}
        name = (s["stage_name"] or "")[:120]
        if s["start_time"] is not None:
            retry = s.get("retry_of_failure")
            rerun = bool(s["stage_attempt"])
            if retry:
                why = f"Retry because attempt {s['stage_attempt'] - 1} failed: {retry[:500]}"
            elif rerun:
                why = (f"Attempt {s['stage_attempt']} re-runs part of the stage: attempt {s['stage_attempt'] - 1} did not "
                       "fail itself, but Spark resubmitted it (usually its shuffle output was lost with an executor)")
            else:
                why = None
            add(ctx, s["start_time"], "stage_start", "low" if rerun else "info",
                f"Stage {s['stage_id']}.{s['stage_attempt']} started ({s['num_tasks'] or '?'} tasks)"
                + (f", re-running attempt {s['stage_attempt'] - 1}" if rerun else ""),
                ((why + (f" - {name}" if name else "")) if why else (name or None)), **links)
        if s["end_time"] is not None:
            if s["status"] == "failed":
                add(ctx, s["end_time"], "stage_failed", "high", f"Stage {s['stage_id']}.{s['stage_attempt']} failed",
                    (s["failure_reason"] or "")[:1000] or None, **links)
            elif s["status"] == "replanned":
                add(ctx, s["end_time"], "stage_end", "info",
                    f"Stage {s['stage_id']}.{s['stage_attempt']} was dropped: adaptive query execution re-planned the "
                    "query (not a failure)", name or None, **links)
            else:
                bits = [f"{s['tasks'] or 0} tasks"]
                if s["skew"] is not None:
                    bits.append(f"skew {s['skew']}x")
                if s["disk_spill"]:
                    bits.append(f"{s['disk_spill'] / (1 << 30):.1f} GB disk spill")
                if s["failed_tasks"]:
                    bits.append(f"{s['failed_tasks']} failed task attempts")
                add(ctx, s["end_time"], "stage_end", "info",
                    f"Stage {s['stage_id']}.{s['stage_attempt']} completed in {fmt_ms(s['duration_ms'])}",
                    ", ".join(bits) + (f" - {name}" if name else ""), **links)
    for q in queries:
        ctx, qid = q["spark_context_id"], q["sql_execution_id"]
        desc = (q["description"] or "")[:500]
        if q["start_time"] is not None:
            add(ctx, q["start_time"], "query_start", "info", f"Query {qid} started", desc or None,
                sql_execution_id=qid)
        if q["end_time"] is not None:
            if q["status"] == "failed":
                add(ctx, q["end_time"], "query_failed", "high", f"Query {qid} failed after {fmt_ms(q['duration_ms'])}",
                    q["error"], sql_execution_id=qid)
            else:
                add(ctx, q["end_time"], "query_end", "info", f"Query {qid} finished in {fmt_ms(q['duration_ms'])}",
                    desc or None, sql_execution_id=qid)
    for e in executors:
        ctx, ex = e["spark_context_id"], e["executor_id"]
        if e["added_time"] is not None:
            add(ctx, e["added_time"], "executor_added", "info",
                f"Executor {ex} added" + (f" on {e['host']}" if e["host"] else "")
                + (f" ({e['cores']} cores)" if e["cores"] else ""), executor_id=ex)
        if e["removed_time"] is not None:
            cat = _removal_cat(e, rules)
            sev = {"oom": "high", "lost": "medium", "killed": "medium"}.get(cat, "info")
            what = {"oom": " (out of memory)", "lost": " (lost)", "killed": " (killed by the OS)",
                    "autoscale": " (autoscaling)", "termination": " (cluster stopping)"}.get(cat, "")
            add(ctx, e["removed_time"], "executor_removed", sev, f"Executor {ex} removed{what}",
                e["removed_reason"], executor_id=ex)
    # task retries: one row per stage attempt, in plain English
    stage_by = {(s["spark_context_id"], s["stage_id"], s["stage_attempt"]): s for s in stages}
    for (ctx, sid, att), rs in stage_retry_summary(task_retries or []).items():
        st = stage_by.get((ctx, sid, att))
        n = rs["tasks"]
        title = (f"Stage {sid}.{att}: {n} task{'s' if n != 1 else ''} retried after failures"
                 f" ({category_text(rs['by_category'])})")
        detail = rs["example"]
        if n > 1:
            detail += f" ({n - 1} more like this"
            detail += f"; {fmt_words(rs['wasted_ms'])} of work lost in total)" if rs["wasted_ms"] else ")"
        add(ctx, rs["first_time"], "task_retry", "medium" if rs["failed_final"] else "low", title, detail,
            stage_id=sid, stage_attempt=att, spark_job_id=st["spark_job_id"] if st else None,
            sql_execution_id=st["sql_execution_id"] if st else None)
    n_sched = len(ev)

    # log rows: cap, keeping highest severity first (then earliest)
    logs = sorted(log_rows, key=lambda r: (SEVERITY_RANK.get(r["severity"], 9), r["ts"] is None, r["ts"] or 0,
                                           r["seq"]))
    cap = rules.story_max_log_rows
    dropped = max(0, len(logs) - cap) + pruned
    logs = logs[:cap]
    app_ctx = {a["app_id"]: a["spark_context_id"] for a in apps if a.get("app_id")}
    windows = [(a["spark_context_id"], a["start_time"], a["end_time"]) for a in apps]
    for r in logs:
        where = r["source"] if r["source"] == "driver" else f"executor {r['executor_id']}"
        if r["kind"] == "log_signal":
            title = f"{r['signal']} in {where} log"
        else:
            title = f"{r['level']} {r['logger'] or ''} ({where})".replace("  ", " ")
        add(_ctx_for_log(r, app_ctx, windows), r["ts"], r["kind"], r["severity"], title,
            r["message"] if r["kind"] == "log_error" else r["line"], count=r["count"],
            executor_id=r["executor_id"], source=r["source"], log_file_path=r["file_path"], log_seq=r["seq"])
    n_logs = len(ev)
    for f in findings:
        links = {k: f.get(k) for k in ("spark_job_id", "stage_id", "stage_attempt", "sql_execution_id",
                                         "executor_id")}
        add(f["spark_context_id"], f["ts"], "finding", f["severity"], f"{f['category']}: {f['entity']}",
            f["evidence"], finding_id=f["finding_id"], log_file_path=f.get("log_file_path"),
            log_seq=f.get("log_seq"), **links)

    order = sorted(range(len(ev)), key=lambda i: (ev[i]["ts"] is None, ev[i]["ts"] or 0,
                                                  0 if i < n_sched else 1 if i < n_logs else 2, i))
    rows = []
    for n, i in enumerate(order):
        r = ev[i]
        r["story_seq"] = n
        rows.append(r)
    return rows, dropped


def log_stats_new() -> dict:
    return {"lines": 0, "error_lines": 0, "warn_lines": 0, "signals": Counter(), "gc_pauses": 0, "full_gcs": 0, "stuck_full_gcs": 0, "full_after_shares": [],
            "gc_pause_ms": 0.0, "max_heap_after_mb": None, "heap_total_mb": None, "first_full_gc": None,
            "min_ts": None, "max_ts": None}


def log_stats_add_gc(st: dict, gc: dict, row: dict) -> None:
    """Accumulate one gc_events row into a log_stats entry (pauses only; concurrent phases are not pauses)."""
    kind = gc.get("kind") or ""
    if not kind.startswith("Pause"):
        return
    st["gc_pauses"] += 1
    st["gc_pause_ms"] += gc.get("pause_ms") or 0.0
    if kind.startswith("Pause Full"):
        st["full_gcs"] += 1
        # a Full GC that left the heap at least 90% full freed almost nothing: the JVM is stuck collecting
        after, total = gc.get("heap_after_mb"), gc.get("heap_total_mb")
        if after is not None and total:
            st["stuck_full_gcs"] += after / total >= 0.9
            st["full_after_shares"].append(after / total)
        if st["first_full_gc"] is None:
            st["first_full_gc"] = (row["ts"], row["file_path"], row["seq"])
    if gc.get("heap_after_mb") is not None:
        st["max_heap_after_mb"] = max(st["max_heap_after_mb"] or 0.0, gc["heap_after_mb"])
    if gc.get("heap_total_mb") is not None:
        st["heap_total_mb"] = max(st["heap_total_mb"] or 0.0, gc["heap_total_mb"])


def gc_profile(ep_rows: list[dict], log_stats: dict) -> list[dict]:
    """Inputs of the jvm_full_gc finding: one entry per executor_profile row with GC data, plus the driver."""
    out = []
    for r in ep_rows:
        if r.get("gc_pauses"):
            ff = r.get("_first_full_gc") or (None, None, None)
            out.append({"spark_context_id": r["spark_context_id"], "executor_id": r["executor_id"],
                        "full_gcs": r["full_gcs"], "gc_pause_ms": r["gc_pause_ms"], "lifetime_ms": r["lifetime_ms"],
                        "max_heap_after_mb": r["max_heap_after_mb"], "heap_total_mb": r.get("_heap_total_mb"),
                        "stuck_full_gcs": r.get("_stuck_full_gcs") or 0,
                        "heap_after_p50": r.get("full_gc_heap_after_p50"),
                        "first_full_gc_ts": ff[0], "file_path": ff[1], "seq": ff[2]})
    drv = [v for k, v in log_stats.items() if k[0] == "driver" and v["gc_pauses"]]
    if drv:
        lo = [v["min_ts"] for v in drv if v["min_ts"] is not None]
        hi = [v["max_ts"] for v in drv if v["max_ts"] is not None]
        ffs = [v["first_full_gc"] for v in drv if v["first_full_gc"]]
        ff = min(ffs, key=lambda x: x[0] or 0) if ffs else (None, None, None)
        heap = [v["max_heap_after_mb"] for v in drv if v["max_heap_after_mb"] is not None]
        tot = [v["heap_total_mb"] for v in drv if v["heap_total_mb"] is not None]
        out.append({"spark_context_id": None, "executor_id": None, "full_gcs": sum(v["full_gcs"] for v in drv),
                    "gc_pause_ms": sum(v["gc_pause_ms"] for v in drv),
                    "lifetime_ms": (max(hi) - min(lo)) if lo and hi and max(hi) > min(lo) else None,
                    "max_heap_after_mb": max(heap) if heap else None, "heap_total_mb": max(tot) if tot else None,
                    "first_full_gc_ts": ff[0], "file_path": ff[1], "seq": ff[2]})
    return out


__all__ = ["query_profile", "stage_executor_profile", "executor_profile", "run_story", "StoryLogCollector",
           "log_stats_new", "log_stats_add_gc", "gc_profile"]
