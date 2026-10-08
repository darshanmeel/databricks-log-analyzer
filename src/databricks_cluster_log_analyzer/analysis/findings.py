"""Ranked findings (notebook A2 + A3, plus the new `exception` category) and the signal timeline (A4)."""

from __future__ import annotations

import re
from collections.abc import Mapping

import pandas as pd

from ..config import SEVERITY_RANK, Rules
from ..util import fmt_words, round_half_up, spark_double_str
from ..parsing.oom import SITE_FIX, SITE_TEXT, is_oom, oom_site
from .retries import category_text, stage_retry_summary

FINDING_COLS = ["finding_id", "cluster_id", "spark_context_id", "severity", "category", "entity", "evidence", "fix",
                "ts", "stage_id", "stage_attempt", "spark_job_id", "sql_execution_id", "executor_id", "signal",
                "fingerprint", "log_file_path", "log_seq"]

FIX = {
    "stage_failed": "See the exception fingerprints and executor signals for the root cause.",
    "task_skew": "Salt or split hot keys, filter nulls before joins, enable AQE skew join.",
    "disk_spill": "Raise shuffle partitions, fix skew, or use memory-optimized nodes.",
    "gc_pressure": "More memory per core; avoid large broadcasts and caching.",
    "task_retries": "Retries cost time; check executor_lost, OOM and fetch failures.",
    "tiny_tasks": "Too many small files or partitions; compact (OPTIMIZE) or coalesce.",
    "executor_oom": "Use memory-optimized nodes or fewer cores per executor; check skew.",
    "executor_lost": "Spot reclaim or node loss; on-demand driver, spot fallback, decommissioning.",
    "query_failed": "See the exception fingerprints for the stack.",
    "executor_killed": ("The executor process was killed by the OS (exit code 9 / SIGKILL), often the Linux OOM "
                        "killer: use memory-optimized nodes or fewer cores per executor, and leave off-heap headroom."),
    "gc_stuck": ("The executor's heap is full of data it cannot free, usually a cache() or a broadcast bigger than "
                 "memory: cache less (or write an intermediate table), use memory-optimized nodes. More partitions "
                 "will not help."),
    "jvm_full_gc": ("The heap is too small for what it holds: use memory-optimized nodes or fewer cores per "
                    "executor, cache and broadcast less, raise shuffle partitions."),
    "large_tasks": ("Give each task less data: raise spark.sql.shuffle.partitions (on Databricks it can be 'auto'), "
                    "let AQE split big partitions (spark.sql.adaptive.advisoryPartitionSizeInBytes), lower "
                    "spark.sql.files.maxPartitionBytes for file scans, or use memory-optimized nodes. For MERGE, "
                    "filter the target on the partition or cluster keys so less of it is read."),
    "big_read": ("Read less of the table: filter on its partition or clustering columns so whole files are skipped "
                 "(the plan's PartitionFilters / data skipping), select only the columns you need, and cluster the "
                 "table on the columns you filter by (liquid clustering, or OPTIMIZE ... ZORDER BY). For a MERGE, put "
                 "the target's partition or cluster keys in the ON condition."),
    "executors_idle": ("Executors were up (and paid for) but ran nothing: usually they waited for a straggler or a "
                       "driver-side step. Fix the slow task (skew), enable autoscaling so idle workers are released, "
                       "or use fewer, smaller workers for this job."),
}


def _idle_findings(cid: str, profile: list[dict], rules: Rules) -> list[dict]:
    """Revision 12: per Spark context, executor core time paid for but not used by any task."""
    by_ctx: dict[str, list[dict]] = {}
    for e in profile:
        if e.get("executor_id") not in (None, "driver") and e.get("lifetime_ms") and e.get("cores"):
            by_ctx.setdefault(e["spark_context_id"], []).append(e)
    out = []
    for ctx, ex in by_ctx.items():
        paid = sum(e["lifetime_ms"] * e["cores"] for e in ex)
        idle = sum(max(0, e.get("idle_core_ms") or 0) for e in ex)
        if not paid or idle / paid < rules.idle_share_min or idle < rules.idle_core_min_ms:
            continue
        mostly = [e for e in ex if e.get("idle_ms") is not None and e["idle_ms"] >= 0.5 * e["lifetime_ms"]]
        mostly.sort(key=lambda e: -(e.get("idle_ms") or 0))
        who = ", ".join(f"exec {e['executor_id']} idle {fmt_words(e['idle_ms'])} of {fmt_words(e['lifetime_ms'])}"
                        for e in mostly[:4])
        ev = (f"{len(mostly)} of {len(ex)} executors were idle most of the time: "
              f"{idle / 3_600_000:.1f} of {paid / 3_600_000:.1f} paid core-hours ran no task ({idle / paid:.0%})")
        if who:
            ev += f". {who}"
        first = min((e["added_time"] for e in ex if e.get("added_time") is not None), default=None)
        out.append(_f(cid, ctx, "low", "executors_idle", f"{len(ex)} executors", ev, FIX["executors_idle"], first))
    return out


def _spill_ratio(s) -> str:
    """" (119.8 GB in memory first, 2.03 GB spilled per GB of shuffle read)": how hard memory was squeezed."""
    GB = 1 << 30
    mem, sr = s.get("mem_spill") or 0, s.get("shuffle_read") or 0
    bits = []
    if mem:
        bits.append(f"{mem / GB:,.1f} GB in memory first")
    if mem and sr >= GB:
        bits.append(f"{mem / sr:.2f} GB spilled per GB of shuffle read")
    return f" ({', '.join(bits)})" if bits else ""


def _large_task_findings(cid: str, stages: list[dict], rules: Rules) -> list[dict]:
    """Revision 13: stages whose typical task read far more than the ~128 MB a Spark task is sized for."""
    MB = 1 << 20
    out = []
    for s in stages:
        med = s.get("p50_task_bytes_in")
        if not med or med < rules.large_task_min_mb * MB or (s.get("duration_ms") or 0) < rules.large_task_min_ms:
            continue
        spill = s.get("disk_spill") or 0
        n = s.get("tasks") or 0
        ev = (f"The median task read {med / MB:,.0f} MB, {med / (128 * MB):.1f}x the 128 MB Spark sizes a task for"
              + (f" (p90 {s['p90_task_bytes_in'] / MB:,.0f} MB)" if s.get("p90_task_bytes_in") else "")
              + f", over {n} tasks in {fmt_words(s.get('duration_ms'))}")
        if n == 200:
            ev += ("; 200 tasks is the default spark.sql.shuffle.partitions, so the partition count did not grow "
                   "with the data")
        if s.get("shuffle_read") and s["shuffle_read"] >= 128 * MB:
            need = -(-s["shuffle_read"] // (128 * MB))
            if need > n:
                ev += f"; {s['shuffle_read'] / (1 << 30):,.1f} GB of shuffle needs ~{need:,} partitions of 128 MB, not {n:,}"
        if spill:
            ev += f"; it spilled {spill / (1 << 30):,.1f} GB to disk" + _spill_ratio(s)
        sev = "high" if spill >= (1 << 30) else "medium"
        out.append(_f(cid, s["spark_context_id"], sev, "large_tasks", f"Stage {s['stage_id']}.{s['stage_attempt']}",
                      ev, FIX["large_tasks"], s.get("start_time"), stage_id=s["stage_id"],
                      stage_attempt=s["stage_attempt"], spark_job_id=s.get("spark_job_id"),
                      sql_execution_id=s.get("sql_execution_id")))
    return out


_SCAN_RE = re.compile(r"Scan (?:parquet|delta|orc|csv|json|text|avro) (\S+)")


def gc_stuck(g: Mapping, rules: Rules) -> tuple[float, float] | None:
    """(Full GCs a minute, share of them that left the heap at least 90% full) when the executor is stuck in GC:
    it collects all the time and frees almost nothing. None otherwise."""
    full, life, stuck = g.get("full_gcs") or 0, g.get("lifetime_ms"), g.get("stuck_full_gcs") or 0
    if not full or not life:
        return None
    per_min, share = full / (life / 60_000), stuck / full
    return (per_min, share) if per_min > rules.gc_stuck_full_per_min and share > rules.gc_stuck_share else None


def _plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 's'}"


def _execs_text(execs) -> str:
    ex = sorted({str(e) for e in execs if e is not None}, key=lambda e: (len(e), e))
    return "" if not ex else f"executor {ex[0]}" if len(ex) == 1 else "executors " + ", ".join(ex[:6]) + (" …" if len(ex) > 6 else "")


def task_oom_rows(tdf: pd.DataFrame) -> list[dict]:
    """The failed task attempts that ran out of memory, with where (tasks.oom_site)."""
    if tdf is None or tdf.empty or "oom_site" not in tdf.columns:
        return []
    t = tdf[tdf["oom_site"].notna()]
    return [{"spark_context_id": r.spark_context_id, "stage_id": int(r.stage_id), "executor_id": r.executor_id,
             "oom_site": r.oom_site, "ts": None if pd.isna(r.finish_time) else int(r.finish_time)}
            for r in t.itertuples()]


def log_oom_sites(log_errors: list[dict]) -> dict[str, str]:
    """executor id -> where it ran out of memory, from the OutOfMemoryError stacks in its own log."""
    out: dict[str, str] = {}
    for r in log_errors:
        txt = f"{r.get('exception_class') or ''} {r.get('message') or ''}"
        if r.get("executor_id") is None or not is_oom(txt):
            continue
        site = oom_site(r.get("top_frames") or [], r.get("message"))
        if site and r["executor_id"] not in out:
            out[r["executor_id"]] = site
    return out


def _oom_site_findings(cid: str, task_ooms: list[dict], stages: list[dict]) -> list[dict]:
    """One finding per stage and site where tasks ran out of memory, saying where (building a cache, a broadcast,
    a sort ...), in which query, and on which executors."""
    qid = {(s["spark_context_id"], s["stage_id"]): s.get("sql_execution_id") for s in stages}
    groups: dict[tuple, list[dict]] = {}
    for t in task_ooms:
        groups.setdefault((t["spark_context_id"], t["stage_id"], t["oom_site"]), []).append(t)
    out = []
    for (ctx, sid, site), ts in groups.items():
        q = qid.get((ctx, sid))
        where = SITE_TEXT.get(site, "at an unknown place")
        ev = (f"{_plural(len(ts), 'task')} ran out of memory {where}"
              + (f" in query {q}" if q is not None else "") + f" (stage {sid}), on {_execs_text(t['executor_id'] for t in ts)}.")
        out.append(_f(cid, ctx, "high", "oom_site", f"stage {sid}: out of memory {where}", ev,
                      SITE_FIX.get(site, FIX["executor_oom"]), min((t["ts"] for t in ts if t["ts"] is not None), default=None),
                      stage_id=sid, sql_execution_id=q))
    return out


def _big_read_findings(cid: str, stages: list[dict], queries: list[dict], rules: Rules) -> list[dict]:
    """Stages that read a lot from storage, naming the table they scanned (from the stage's operators, else the one
    table its query read)."""
    GB = 1 << 30
    reads = {(q.get("spark_context_id"), q.get("sql_execution_id")): [t for t in (q.get("tables_read") or [])
                                                                        if not str(t).startswith("jdbc:")]
             for q in queries}
    out = []
    for s in stages:
        b = s.get("input_bytes") or 0
        if b < rules.big_read_min_bytes:
            continue
        scans = list(dict.fromkeys(m.group(1) for x in (s.get("rdd_scopes") or []) if (m := _SCAN_RE.match(str(x)))))
        q_tables = reads.get((s.get("spark_context_id"), s.get("sql_execution_id"))) or []
        tables = scans or (q_tables if len(q_tables) == 1 else [])
        what = tables[0] if len(tables) == 1 else (f"{tables[0]} and {len(tables) - 1} more" if tables else None)
        q_read = sum((x.get("input_bytes") or 0) for x in stages if x.get("sql_execution_id") is not None
                     and x.get("sql_execution_id") == s.get("sql_execution_id")
                     and x.get("spark_context_id") == s.get("spark_context_id"))
        ev = (f"read {b / GB:,.0f} GB from storage" + (f" scanning {what}" if what else "")
              + f" over {s.get('tasks') or 0:,} tasks in {fmt_words(s.get('duration_ms'))}")
        if q_read > b:
            ev += f"; {b / q_read:.0%} of what its query read"
        sev = "high" if b >= rules.big_read_high_bytes else "medium"
        entity = f"Stage {s['stage_id']}.{s['stage_attempt']}" + (f": {what}" if what else "")
        out.append(_f(cid, s["spark_context_id"], sev, "big_read", entity, ev, FIX["big_read"], s.get("start_time"),
                      stage_id=s["stage_id"], stage_attempt=s["stage_attempt"], spark_job_id=s.get("spark_job_id"),
                      sql_execution_id=s.get("sql_execution_id")))
    return out


def _earliest_key(r: Mapping):
    """notebook `earliest()`: by ts with null ts counted as last; ties by seq (file order)."""
    return (r.get("ts") is None, r.get("ts") or 0, r.get("seq") or 0)


def summarize_signals(log_signals: list[dict]) -> list[dict]:
    """Notebook summarize_signals: per signal occurrences, executors affected, first/last seen, sample line."""
    groups: dict[str, dict] = {}
    for r in log_signals:
        g = groups.get(r["signal"])
        if g is None:
            g = groups[r["signal"]] = {"cluster_id": r.get("cluster_id"), "signal": r["signal"],
                                       "severity": r["severity"], "fix": r["fix"], "occurrences": 0,
                                       "_execs": set(), "first_seen": None, "last_seen": None, "_sample": r}
        g["occurrences"] += 1
        if r.get("executor_id"):
            g["_execs"].add(r["executor_id"])
        ts = r.get("ts")
        if ts is not None:
            g["first_seen"] = ts if g["first_seen"] is None else min(g["first_seen"], ts)
            g["last_seen"] = ts if g["last_seen"] is None else max(g["last_seen"], ts)
        if _earliest_key(r) < _earliest_key(g["_sample"]):
            g["_sample"] = r
    out = []
    for g in groups.values():
        s = g.pop("_sample")
        g["executors_affected"] = len(g.pop("_execs"))
        g.update(sample_line=s["line"], sample_file_path=s["file_path"], sample_seq=s["seq"],
                 sample_executor_id=s.get("executor_id"))
        out.append(g)
    out.sort(key=lambda g: (SEVERITY_RANK.get(g["severity"], 9), -g["occurrences"]))
    return out


def summarize_errors(log_errors: list[dict]) -> list[dict]:
    """Notebook summarize_errors (+ user_frame, sources, sample location)."""
    groups: dict[str, dict] = {}
    for r in log_errors:
        g = groups.get(r["fingerprint"])
        if g is None:
            g = groups[r["fingerprint"]] = {"cluster_id": r.get("cluster_id"), "fingerprint": r["fingerprint"],
                                            "exception_class": r["exception_class"], "occurrences": 0,
                                            "_execs": set(), "_sources": set(), "first_seen": None,
                                            "last_seen": None, "_sample": r, "user_frame": None}
        g["occurrences"] += 1
        if r.get("executor_id"):
            g["_execs"].add(r["executor_id"])
        g["_sources"].add(r["source"])
        ts = r.get("ts")
        if ts is not None:
            g["first_seen"] = ts if g["first_seen"] is None else min(g["first_seen"], ts)
            g["last_seen"] = ts if g["last_seen"] is None else max(g["last_seen"], ts)
        if _earliest_key(r) < _earliest_key(g["_sample"]):
            g["_sample"] = r
        if g["user_frame"] is None and r.get("user_frame"):
            g["user_frame"] = r["user_frame"]
    out = []
    for g in groups.values():
        s = g.pop("_sample")
        g["executors_affected"] = len(g.pop("_execs"))
        g["sources"] = sorted(g.pop("_sources"))
        g.update(sample_message=s["message"], sample_stack=list(s["top_frames"]), sample_file_path=s["file_path"],
                 sample_seq=s["seq"], sample_executor_id=s.get("executor_id"))
        out.append(g)
    out.sort(key=lambda g: (-g["occurrences"], g["first_seen"] is None, g["first_seen"] or 0))
    return out


def frame_text(frame: str | None) -> str:
    """'at com.x.Y.z(Y.scala:1)' -> 'com.x.Y.z(Y.scala:1)'."""
    f = (frame or "").strip()
    return f[3:] if f.startswith("at ") else f


def _stage_entity(s) -> str:
    return f"stage {s['stage_id']}: {(s.get('stage_name') or '')[:80]}"


def _f(cluster_id, ctx, severity, category, entity, evidence, fix, ts, **links) -> dict:
    row = {"cluster_id": cluster_id, "spark_context_id": ctx, "severity": severity, "category": category,
           "entity": entity, "evidence": evidence, "fix": fix, "ts": ts, "stage_id": None, "stage_attempt": None,
           "spark_job_id": None, "sql_execution_id": None, "executor_id": None, "signal": None, "fingerprint": None,
           "log_file_path": None, "log_seq": None}
    row.update(links)
    return row


def _event_findings(cid: str, stages, executors, queries, rules: Rules, retries=None, gc_profile=None,
                    oom_sites=None) -> list[dict]:
    """Notebook A2, in the notebook's union order."""
    out: list[dict] = []
    GB = 1 << 30

    def stage_links(s):
        return {"stage_id": s["stage_id"], "stage_attempt": s["stage_attempt"], "spark_job_id": s["spark_job_id"],
                "sql_execution_id": s["sql_execution_id"]}

    def stage_rows(pred, severity, category, evidence):
        for s in stages:
            if pred(s):
                sev = severity(s) if callable(severity) else severity
                out.append(_f(cid, s["spark_context_id"], sev, category, _stage_entity(s), evidence(s),
                              FIX[category], s["end_time"], **stage_links(s)))

    stage_rows(lambda s: s["failure_reason"] is not None and s.get("status") != "replanned", "high", "stage_failed",
               lambda s: s["failure_reason"][:300])
    stage_rows(lambda s: s["skew"] is not None and s["max_task_ms"] is not None
               and s["skew"] >= rules.skew_ratio and s["max_task_ms"] >= rules.skew_min_task_ms,
               "high", "task_skew",
               lambda s: (f"slowest task {spark_double_str(round_half_up(s['max_task_ms'] / 1000))}s = "
                          f"{spark_double_str(s['skew'])}x the median"))
    stage_rows(lambda s: s["disk_spill"] is not None and s["disk_spill"] >= rules.spill_bytes,
               lambda s: "high" if s["disk_spill"] >= rules.spill_high_bytes else "medium", "disk_spill",
               lambda s: f"{s['disk_spill'] / GB:,.1f} GB spilled to disk" + _spill_ratio(s))
    stage_rows(lambda s: s["gc_share"] is not None and s["gc_share"] >= rules.gc_share and (s["tasks"] or 0) > 0,
               # a share alone is noise on a short stage: medium only when GC cost the stage a minute or more of its time
               lambda s: "medium" if s["gc_share"] * (s.get("duration_ms") or 0) >= 60_000 else "low", "gc_pressure",
               lambda s: f"{spark_double_str(round_half_up(s['gc_share'] * 100))}% of task time in GC")
    retry_sum = stage_retry_summary(retries or [])

    def retry_evidence(s):
        ev = f"{s['failed_tasks']} failed task attempts, stage still succeeded"
        rs = retry_sum.get((s["spark_context_id"], s["stage_id"], s["stage_attempt"]))
        if rs:
            n = rs["tasks"]
            ev += f": {n} task{'s' if n != 1 else ''} retried ({category_text(rs['by_category'])})"
            if rs["wasted_ms"]:
                ev += f", {fmt_words(rs['wasted_ms'])} of work lost"
            ev += f". e.g. {rs['example']}"
        return ev

    stage_rows(lambda s: s["failure_reason"] is None and (s["failed_tasks"] or 0) > 0, "medium", "task_retries",
               retry_evidence)
    stage_rows(lambda s: (s["tasks"] or 0) >= rules.tiny_tasks_min and s["p50_task_ms"] is not None
               and s["p50_task_ms"] < rules.tiny_tasks_p50_ms, "medium", "tiny_tasks",
               lambda s: f"{s['tasks']} tasks, median {s['p50_task_ms']} ms")
    def cat(e):
        c = e.get("removal_category")
        return c if c is not None else rules.removal_category(e["removed_reason"])

    # one category per executor (rules.toml [executors]); autoscale / termination / other produce no finding
    for category, sev, rc in (("executor_oom", "high", "oom"), ("executor_lost", "medium", "lost"),
                              ("executor_killed", "medium", "killed")):
        for e in executors:
            if e["removed_reason"] is not None and cat(e) == rc:
                site = (oom_sites or {}).get(e["executor_id"]) if rc in ("oom", "killed") else None
                ev = e["removed_reason"][:300] + (f": out of memory {SITE_TEXT[site]}" if site in SITE_TEXT else "")
                out.append(_f(cid, e["spark_context_id"], sev, category, f"executor {e['executor_id']}",
                              ev, SITE_FIX.get(site, FIX[category]), e["removed_time"],
                              executor_id=e["executor_id"]))
    for q in queries:
        if q["error"]:
            out.append(_f(cid, q["spark_context_id"], "high", "query_failed",
                          f"query {q['sql_execution_id']}: {(q['description'] or '')[:80]}", q["error"][:300],
                          FIX["query_failed"], q["end_time"], sql_execution_id=q["sql_execution_id"]))
    for g in gc_profile or []:
        full, pause, life = g.get("full_gcs") or 0, g.get("gc_pause_ms"), g.get("lifetime_ms")
        share = (pause / life) if pause and life else None
        who = "driver" if g.get("executor_id") is None else f"executor {g['executor_id']}"
        stuck = gc_stuck(g, rules)
        if stuck:
            # collecting all the time and freeing almost nothing: its long tasks are stuck in GC, not skewed
            ev = (f"stuck in GC: {stuck[0]:,.0f} Full GCs a minute, {stuck[1]:.0%} of them left the heap at least 90% full"
                  + (f"; {share:.0%} of its {fmt_words(life)} life in GC pauses" if share is not None else ""))
            out.append(_f(cid, g.get("spark_context_id"), "high", "gc_stuck", who, ev, FIX["gc_stuck"],
                          g.get("first_full_gc_ts"), executor_id=g.get("executor_id"),
                          log_file_path=g.get("file_path"), log_seq=g.get("seq")))
        elif full >= rules.full_gc_min or (share is not None and share >= rules.gc_pause_share_min):
            ev = f"{full} Full GC pause{'s' if full != 1 else ''}, {fmt_words(pause or 0)} total GC pause"
            if share is not None:
                ev += f" ({share * 100:.1f}% of its {fmt_words(life)} lifetime)"
            if g.get("heap_after_p50") is not None:
                ev += f"; after a Full GC the heap stayed {g['heap_after_p50']:.0%} full (median)"
            if g.get("max_heap_after_mb") is not None:
                ev += f"; max heap after GC {g['max_heap_after_mb']:,.0f} MB"
                if g.get("heap_total_mb"):
                    ev += f" of {g['heap_total_mb']:,.0f} MB"
            out.append(_f(cid, g.get("spark_context_id"), "medium", "jvm_full_gc", who, ev, FIX["jvm_full_gc"],
                          g.get("first_full_gc_ts"), executor_id=g.get("executor_id"),
                          log_file_path=g.get("file_path"), log_seq=g.get("seq")))
    return out


def _real_losses(log_signals: list[dict], executors: list[dict], rules: Rules) -> list[dict]:
    """Decommission lines are planned when every executor that went away was autoscaled away or stopped with the
    cluster: drop them, so a planned removal is not reported as an executor lost. A real loss line
    (ExecutorLostFailure, Lost executor) always stays."""
    def cat(e):
        c = e.get("removal_category")
        return c if c is not None else rules.removal_category(e.get("removed_reason"))
    gone = [cat(e) for e in executors if e.get("removed_reason") is not None]
    if any(c not in ("autoscale", "termination") for c in gone):
        return log_signals
    real = re.compile(r"ExecutorLostFailure|Lost executor")
    return [r for r in log_signals if r.get("signal") != "executor_lost" or real.search(r.get("line") or "")]


def _signal_findings(cid: str, signal_summary: list[dict]) -> list[dict]:
    return [_f(cid, None, g["severity"], f"log:{g['signal']}", "driver/executor logs",
               f"{g['occurrences']} lines on {g['executors_affected']} executors. e.g. {(g['sample_line'] or '')[:200]}",
               g["fix"], g["first_seen"], signal=g["signal"], log_file_path=g["sample_file_path"],
               log_seq=g["sample_seq"])
            for g in signal_summary]


def _exception_findings(cid: str, error_summary: list[dict], rules: Rules) -> list[dict]:
    high = re.compile(rules.exception_high_regex)
    benign = re.compile(rules.exception_benign_regex) if rules.exception_benign_regex else None
    out = []
    for g in error_summary:
        cls = g["exception_class"] or "Exception"
        uf = g["user_frame"]
        where = []
        if "driver" in g["sources"]:
            where.append("the driver")
        if g["executors_affected"]:
            where.append(f"{g['executors_affected']} executor{'s' if g['executors_affected'] != 1 else ''}")
        elif "executor" in g["sources"]:
            where.append("executors")
        ev = f"{g['occurrences']} occurrence{'s' if g['occurrences'] != 1 else ''} in {' and '.join(where)}"
        if uf:
            ev += f"; user code: {frame_text(uf)}"
        if g["sample_message"]:
            ev += f". e.g. {g['sample_message'][:200]}"
        fix = (f"Fix the code at {frame_text(uf)}" if uf else
               "No user frame in the stack: read the full stack trace and the first error before it.")
        site = oom_site(g.get("sample_stack") or [], g.get("sample_message")) if is_oom(f"{cls} {g['sample_message'] or ''}") else None
        if site:
            # where the memory ran out says more than a missing user frame
            ev = f"out of memory {SITE_TEXT[site]}: " + ev
            fix = SITE_FIX[site]
        sev = "high" if high.search(cls) else "medium"
        if benign and benign.search(f"{cls} {g['sample_message'] or ''}"):
            sev, fix = "low", "Logged by the platform on healthy clusters too; not the cause of a problem."
        out.append(_f(cid, None, sev, "exception", cls, ev, fix,
                      g["first_seen"], fingerprint=g["fingerprint"], log_file_path=g["sample_file_path"],
                      log_seq=g["sample_seq"],
                      executor_id=g["sample_executor_id"] if g["executors_affected"] == 1 else None))
    return out


def build_findings(tables: Mapping, rules: Rules) -> pd.DataFrame:
    """Ranked findings as a DataFrame (see build_findings_rows)."""
    return pd.DataFrame(build_findings_rows(tables, rules), columns=FINDING_COLS)


def build_findings_rows(tables: Mapping, rules: Rules) -> list[dict]:
    """`tables` needs cluster_id, stages, executors, sql_queries, log_signals, log_errors (lists of dicts).
    Returns the ranked findings (FINDING_COLS), finding_id F001.. by severity rank then ts (nulls first, like
    Spark's ascending sort)."""
    cid = tables["cluster_id"]
    parts = [
        _event_findings(cid, tables.get("stages") or [], tables.get("executors") or [],
                        tables.get("sql_queries") or [], rules, tables.get("task_retries") or [],
                        tables.get("gc_profile") or [],
                        {**log_oom_sites(tables.get("log_errors") or []),
                         **{t["executor_id"]: t["oom_site"] for t in tables.get("task_ooms") or [] if t.get("executor_id")}})
        + _oom_site_findings(cid, tables.get("task_ooms") or [], tables.get("stages") or [])
        + _idle_findings(cid, tables.get("executor_profile") or [], rules)
        + _large_task_findings(cid, tables.get("stages") or [], rules)
        + _big_read_findings(cid, tables.get("stages") or [], tables.get("sql_queries") or [], rules),
        _signal_findings(cid, summarize_signals(_real_losses(tables.get("log_signals") or [],
                                                             tables.get("executors") or [], rules))),
        _exception_findings(cid, summarize_errors(tables.get("log_errors") or []), rules),
    ]
    rows = []
    for pi, part in enumerate(parts):
        for i, r in enumerate(part):
            rows.append((SEVERITY_RANK.get(r["severity"], 9), r["ts"] is not None, r["ts"] or 0, pi, i, r))
    rows.sort(key=lambda t: t[:5])
    width = max(3, len(str(len(rows))))
    out = []
    for n, t in enumerate(rows, 1):
        r = t[-1]
        r["finding_id"] = f"F{n:0{width}d}"
        out.append(r)
    return out


def build_timeline(log_signals: list[dict], cluster_id: str) -> list[dict]:
    """Log signals per minute (notebook A4)."""
    counts: dict[tuple, int] = {}
    for r in log_signals:
        if r.get("ts") is None:
            continue
        k = (r["ts"] // 60000 * 60000, r["signal"])
        counts[k] = counts.get(k, 0) + 1
    return [{"cluster_id": cluster_id, "minute": m, "signal": s, "count": c} for (m, s), c in sorted(counts.items())]
