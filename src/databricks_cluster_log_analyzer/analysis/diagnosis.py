"""summary.json: counts, totals and the ordered plain-English "steps to debug" diagnosis."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone

from .. import __version__
from ..config import Rules
from .findings import frame_text
from .retries import CATEGORY_LABELS
from ..util import fmt_bytes, fmt_clock, fmt_ms, fmt_words

EMPTY_REASON = ("No driver/, executor/ or eventlog/ folders with log files in {dir}. Serverless compute produces no "
                "cluster logs (this tool covers classic clusters with cluster log delivery enabled); otherwise check "
                "that log delivery is configured for this cluster and that this is the right folder.")


def link(type_: str, label: str, ctx=None, id_=None, attempt=None, file_path=None, seq=None) -> dict:
    return {"type": type_, "label": label, "spark_context_id": ctx, "id": None if id_ is None else str(id_),
            "attempt": attempt, "file_path": file_path, "seq": seq}


def _story_links(r: Mapping) -> list[dict]:
    out = []
    ctx = r.get("spark_context_id")
    if r.get("finding_id"):
        out.append(link("finding", r["finding_id"], ctx, r["finding_id"]))
    if r.get("stage_id") is not None:
        out.append(link("stage", f"stage {r['stage_id']}.{r.get('stage_attempt') or 0}", ctx, r["stage_id"],
                        r.get("stage_attempt") or 0))
    if r.get("sql_execution_id") is not None:
        out.append(link("query", f"query {r['sql_execution_id']}", ctx, r["sql_execution_id"]))
    elif r.get("spark_job_id") is not None:
        out.append(link("job", f"job {r['spark_job_id']}", ctx, r["spark_job_id"]))
    if r.get("executor_id") is not None:
        out.append(link("executor", f"executor {r['executor_id']}", ctx, r["executor_id"]))
    if r.get("log_file_path"):
        out.append(link("log", f"{r['log_file_path']} #{r.get('log_seq')}", ctx, None, None, r["log_file_path"],
                        r.get("log_seq")))
    return out


def _outcome(t) -> dict | None:
    jobs, queries = t["spark_jobs"], t["sql_queries"]
    fj = [j for j in jobs if j["result"] == "JobFailed"]
    fq = [q for q in queries if q["status"] == "failed"]
    fs = [s for s in t["stages"] if s["status"] == "failed"]
    inc = [j for j in jobs if j["end_time"] is None]
    if not jobs and not queries:
        if t["counts"]["log_lines"]:
            return {"kind": "outcome", "severity": "info", "title": "No Spark jobs in the event log",
                    "text": "Only driver/executor logs were found (no event log, or no jobs ran). The log signals "
                            "and exceptions below are all there is to go on.", "links": []}
        return None
    links = []
    if fj or fq:
        parts = []
        if fj:
            parts.append(f"{len(fj)} of {len(jobs)} Spark jobs failed")
        if fq:
            parts.append(f"{len(fq)} of {len(queries)} SQL/DataFrame queries failed")
        if fs:
            parts.append(f"{len(fs)} stage attempts failed")
        text = "; ".join(parts) + "."
        first = sorted(fq, key=lambda q: q["end_time"] or 0)[:3]
        for q in first:
            text += f" Query {q['sql_execution_id']} ({(q['description'] or '').strip()[:80]}) failed: " \
                    f"{(q['error'] or '').splitlines()[0][:200] if q['error'] else ''}"
            links.append(link("query", f"query {q['sql_execution_id']}", q["spark_context_id"], q["sql_execution_id"]))
        for j in sorted(fj, key=lambda j: j["end_time"] or 0)[:3]:
            if not first:
                text += f" Job {j['spark_job_id']} failed: {(j['error'] or '').splitlines()[0][:200] if j['error'] else ''}"
            links.append(link("job", f"job {j['spark_job_id']}", j["spark_context_id"], j["spark_job_id"]))
        for s in fs[:3]:
            links.append(link("stage", f"stage {s['stage_id']}.{s['stage_attempt']}", s["spark_context_id"],
                              s["stage_id"], s["stage_attempt"]))
        return {"kind": "outcome", "severity": "high", "title": "The run failed", "text": text.strip(),
                "links": links}
    text = f"All {len(jobs)} Spark jobs" + (f" and {len(queries)} queries" if queries else "") + " that finished succeeded."
    if inc:
        text += f" {len(inc)} jobs never finished (the cluster may have stopped or still be running)."
    if fs:
        text += f" {len(fs)} stage attempts failed but were retried successfully."
    return {"kind": "outcome", "severity": "medium" if inc or fs else "info",
            "title": "The run succeeded" if not inc else "The run did not finish cleanly", "text": text,
            "links": [link("job", f"job {j['spark_job_id']}", j["spark_context_id"], j["spark_job_id"]) for j in inc[:3]]}


def _first_error(t) -> dict | None:
    cands = [r for r in t["run_story"] if r["severity"] == "high" and r["ts"] is not None]
    if not cands:
        cands = [r for r in t["run_story"] if r["severity"] == "medium" and r["ts"] is not None
                 and r["kind"] not in ("finding",)]
        if not cands:
            return None
    r = min(cands, key=lambda r: (r["ts"], r["story_seq"]))
    detail = (r.get("detail") or "").strip().splitlines()
    text = f"At {fmt_clock(r['ts'])} UTC: {r['title']}."
    if detail:
        text += f" {detail[0][:300]}"
    text += " The earliest error is usually the cause; what follows is often a consequence."
    return {"kind": "first_error", "severity": r["severity"], "title": f"First error at {fmt_clock(r['ts'])}",
            "text": text, "links": _story_links(r)}


CHAIN = [  # (key, label, signals, finding categories, removal category)
    ("spill", "disk spill", {"disk_spill"}, {"disk_spill", "log:disk_spill"}, None),
    ("gc", "GC pressure", {"gc_pressure"}, {"gc_pressure", "log:gc_pressure", "jvm_full_gc", "gc_stuck"}, None),
    ("oom", "out of memory", {"executor_oom"}, {"executor_oom", "log:executor_oom", "oom_site"}, "oom"),
    ("killed", "executor killed by the OS", set(), {"executor_killed"}, "killed"),
    ("disk_full", "disk full", {"disk_full"}, {"log:disk_full"}, None),
    ("lost", "executor lost", set(), {"executor_lost"}, "lost"),
    ("fetch", "shuffle fetch failure", {"fetch_failure"}, {"log:fetch_failure"}, None),
    ("stage_failed", "stage failed", set(), {"stage_failed"}, None),
    ("query_failed", "query/job failed", set(), {"query_failed"}, None),
]


_SLOW_KINDS = {"task skew", "disk spill", "GC pressure", "too many tiny tasks"}


def _incident_steps(inc_rows: list[dict], fby: Mapping) -> list[tuple]:
    """(ts, row, finding) along the incident's causal path: from its most likely root cause to the furthest effect,
    following caused_by (not every problem of the incident in time order: one cause can have two separate effects)."""
    from .incidents import KINDS
    level = {lbl: lv for lbl, lv in KINDS.values()}
    leads = {r["finding_id"]: r for r in inc_rows if r["role"] != "same"}
    if not leads:
        return []
    causes = {r["caused_by"] for r in leads.values() if r.get("caused_by")}

    def ts_of(r):
        ts = (fby.get(r["finding_id"]) or {}).get("ts")
        return r.get("incident_end") if ts is None and r["role"] == "effect" else ts

    ends = [r for r in leads.values() if r["finding_id"] not in causes and (r["role"] == "effect" or len(leads) == 1)]
    if not ends:
        ends = [r for r in leads.values() if r["role"] == "root"] or list(leads.values())
    end = max(ends, key=lambda r: (level.get(r["kind"], 0), ts_of(r) or 0))
    path, cur, seen = [], end, set()
    while cur is not None and cur["finding_id"] not in seen:
        seen.add(cur["finding_id"])
        path.append(cur)
        cur = leads.get(cur.get("caused_by")) if cur.get("caused_by") else None
    return [(ts_of(r), r, fby.get(r["finding_id"]) or {}) for r in reversed(path)]


def _queries(rows: list[dict]) -> set:
    return {(r.get("spark_context_id"), r["sql_execution_id"]) for r in rows if r.get("sql_execution_id") is not None}


def _root_cause(t, rules: Rules) -> dict | None:
    """From the top incident (findings chained cause -> effect by shared stage, executor or host), when there is one."""
    inc = t.get("incidents") or []
    fby = {f["finding_id"]: f for f in t["findings"]}
    ranks = sorted({r["incident_rank"] for r in inc})
    steps = []
    if ranks:
        rows = [r for r in inc if r["incident_rank"] == ranks[0]]
        steps = _incident_steps(rows, fby)
    if not steps:
        return _root_cause_by_time(t, rules)
    head = rows[0]
    chain = " -> ".join(f"{r['kind']} ({fmt_clock(ts)})" if ts is not None else r["kind"] for ts, r, _ in steps)
    if len(steps) == 1:
        text = f"{head['incident_title']}: {chain}."
    else:
        text = (f"{head['incident_title']}. In time order: {chain}. Read it left to right: the first item is the most "
                "likely root cause; each later one is linked to the one before by a shared stage, executor or host.")
    others = []
    for rank in ranks[1:4]:
        rows2 = [r for r in inc if r["incident_rank"] == rank]
        st2 = _incident_steps(rows2, fby)
        if st2 and rows2[0]["incident_severity"] == "high" and st2[0][1]["kind"] not in _SLOW_KINDS:
            same_q = bool(_queries(rows2) & _queries(rows))
            others.append(f"{rows2[0]['incident_title']}, starting from {st2[0][1]['kind']}"
                          + (" (an earlier failure in the same query that Spark retried)" if same_q else ""))
    if others:
        text += " Also: " + "; ".join(others) + "."
    links = [link("finding", r["finding_id"], r["spark_context_id"], r["finding_id"]) for _, r, _ in steps]
    return {"kind": "root_cause", "severity": head["incident_severity"], "title": "Likely root cause",
            "text": text, "links": links}


def _root_cause_by_time(t, rules: Rules) -> dict | None:
    first: dict[str, tuple[int, dict]] = {}

    def see(key, ts, lk):
        if ts is not None and (key not in first or ts < first[key][0]):
            first[key] = (ts, lk)

    for key, _label, sigs, cats, rem in CHAIN:
        for s in t["log_signals"]:
            if s["signal"] in sigs:
                see(key, s["ts"], link("log", f"{s['file_path']} #{s['seq']}", None, None, None, s["file_path"], s["seq"]))
        for f in t["findings"]:
            if f["category"] in cats:
                see(key, f["ts"], link("finding", f["finding_id"], f["spark_context_id"], f["finding_id"]))
        if rem:
            for e in t["executors"]:
                cat = e.get("removal_category") or rules.removal_category(e["removed_reason"])
                if cat == rem:
                    see(key, e["removed_time"], link("executor", f"executor {e['executor_id']}", e["spark_context_id"],
                                                     e["executor_id"]))
    for j in t["spark_jobs"]:
        if j["result"] == "JobFailed":
            see("query_failed", j["end_time"], link("job", f"job {j['spark_job_id']}", j["spark_context_id"],
                                                    j["spark_job_id"]))
    if not first:
        return None
    labels = {k: lbl for k, lbl, *_ in CHAIN}
    steps = sorted(first.items(), key=lambda kv: kv[1][0])
    chain = " -> ".join(f"{labels[k]} ({fmt_clock(ts)})" for k, (ts, _) in steps)
    serious = {"oom", "killed", "lost", "fetch", "stage_failed", "query_failed", "disk_full"} & set(first)
    if len(steps) == 1:
        text = f"Only one problem type shows up: {chain}."
    else:
        text = (f"In time order: {chain}. Read it left to right: the first item is the most likely root cause, "
                "the later ones are usually consequences (for example spill and GC lead to OOM, OOM kills executors, "
                "lost executors cause fetch failures, and fetch failures fail stages).")
    return {"kind": "root_cause", "severity": "high" if serious else "medium", "title": "Likely root cause chain",
            "text": text, "links": [lk for _, (_, lk) in steps]}


def _performance(t, rules: Rules) -> dict | None:
    st = [s for s in t["stages"] if s["duration_ms"] is not None]
    if not st:
        return None
    top = sorted(st, key=lambda s: -s["duration_ms"])[:3]
    total = sum(s["duration_ms"] for s in st)
    parts = []
    sev = "info"
    for s in top:
        bits = [fmt_ms(s["duration_ms"]), f"{s['tasks'] or 0} tasks"]
        if s["skew"] is not None:
            bits.append(f"skew {s['skew']}x")
            if s["skew"] >= rules.skew_ratio and (s["max_task_ms"] or 0) >= rules.skew_min_task_ms:
                sev = "medium"
        if s["disk_spill"]:
            bits.append(f"{fmt_bytes(s['disk_spill'])} disk spill")
            if s["disk_spill"] >= rules.spill_bytes:
                sev = "medium"
        if s["gc_share"] is not None:
            bits.append(f"GC {s['gc_share'] * 100:.0f}%")
            if s["gc_share"] >= rules.gc_share:
                sev = "medium"
        parts.append(f"stage {s['stage_id']}.{s['stage_attempt']} ({(s['stage_name'] or '')[:60]}): {', '.join(bits)}")
    share = sum(s["duration_ms"] for s in top) / total if total else 0
    text = (f"The slowest stages take {share * 100:.0f}% of total stage time. " + "; ".join(parts) + ".")
    links = [link("stage", f"stage {s['stage_id']}.{s['stage_attempt']}", s["spark_context_id"], s["stage_id"],
                  s["stage_attempt"]) for s in top]
    # Revision 5: the sharpest hotspots, as plain sentences (skewed tasks first, then the busiest shuffle/spill minute)
    hot = t.get("hotspots") or []
    picked = [h for h in hot if h["kind"] == "skew_task"][:2]
    for kind in ("shuffle_peak", "spill_peak"):
        picked += [h for h in hot if h["kind"] == kind][:1]
    if picked:
        text += " Peaks: " + " ".join(h["detail"] for h in picked)
        if any(h["kind"] == "skew_task" for h in picked):
            sev = "medium"
        for h in picked:
            lk = link("stage", f"stage {h['stage_id']}.{h['stage_attempt']}", h["spark_context_id"], h["stage_id"],
                      h["stage_attempt"])
            if lk not in links:
                links.append(lk)
    return {"kind": "performance", "severity": sev, "title": "Where the time went", "text": text, "links": links}


def _code_location(t) -> dict | None:
    parts, links = [], []
    seen_frames = set()
    for g in t["error_summary"]:
        if g["user_frame"] and g["user_frame"] not in seen_frames and len(seen_frames) < 3:
            seen_frames.add(g["user_frame"])
            n = sum(x["occurrences"] for x in t["error_summary"] if x["user_frame"] == g["user_frame"])
            parts.append(f"{g['exception_class']} ({n}x) is raised from {frame_text(g['user_frame'])}")
            links.append(link("error", g["exception_class"], None, g["fingerprint"], None, g["sample_file_path"],
                              g["sample_seq"]))
    jobs = [j for j in t["spark_jobs"] if j["result"] == "JobFailed"]
    if not jobs:
        jobs = sorted([j for j in t["spark_jobs"] if j["duration_ms"] is not None], key=lambda j: -j["duration_ms"])[:1]
    for j in jobs[:2]:
        where = j["notebook_path"] or j["call_site"]
        if where:
            what = "failed" if j["result"] == "JobFailed" else "slowest"
            parts.append(f"{what} Spark job {j['spark_job_id']} was started from {where.splitlines()[0][:200]}")
            links.append(link("job", f"job {j['spark_job_id']}", j["spark_context_id"], j["spark_job_id"]))
    for q in [q for q in t["sql_queries"] if q["status"] == "failed"][:2]:
        if q["details"]:
            parts.append(f"failed query {q['sql_execution_id']} came from: {q['details'].strip().splitlines()[0][:200]}")
            links.append(link("query", f"query {q['sql_execution_id']}", q["spark_context_id"], q["sql_execution_id"]))
    if not parts:
        return None
    return {"kind": "code_location", "severity": "info", "title": "Where to look in your code",
            "text": "; ".join(parts) + ".", "links": links}


def retries_block(retries: list[dict]) -> dict:
    """summary.json "retries": totals over task_retries plus up to 3 plain-English examples."""
    by_cat: dict[str, int] = {}
    stages = set()
    for r in retries:
        stages.add((r["spark_context_id"], r["stage_id"], r["stage_attempt"]))
        c = r.get("first_failure_category") or "other"
        by_cat[c] = by_cat.get(c, 0) + 1
    return {"tasks": len(retries), "stages": len(stages),
            "failed_attempts": sum(r.get("failed_attempts") or 0 for r in retries),
            "still_failed": sum(r.get("final_status") == "failed" for r in retries),
            "wasted_ms": sum(r.get("wasted_ms") or 0 for r in retries),
            "by_category": dict(sorted(by_cat.items(), key=lambda kv: -kv[1])),
            "examples": [r["explanation"] for r in retries[:3]]}


def _retries(t) -> dict | None:
    """'Retries (job still succeeded)': every job/query that finished succeeded, but tasks or stages were retried."""
    retries = t.get("task_retries") or []
    stage_retries = [s for s in t["stages"] if s.get("stage_attempt")]
    if not retries and not stage_retries:
        return None
    # only when every job really succeeded (an unfinished job is not a success) and no query failed
    if any(j["result"] != "JobSucceeded" for j in t["spark_jobs"]) or any(q["status"] in ("failed", "incomplete")
                                                                          for q in t["sql_queries"]):
        return None
    b = retries_block(retries)
    parts = []
    if retries:
        cats = ", ".join(f"{n} {CATEGORY_LABELS.get(k, k)}" for k, n in b["by_category"].items())
        parts.append(f"{b['tasks']} task{'s' if b['tasks'] != 1 else ''} in {b['stages']} stage"
                     f"{'s' if b['stages'] != 1 else ''} failed at least once and {'was' if b['tasks'] == 1 else 'were'} retried (first failure: {cats})")
        if b["wasted_ms"]:
            parts.append(f"{fmt_words(b['wasted_ms'])} of task time was lost to failed attempts")
    if stage_retries:
        parts.append(f"{len(stage_retries)} stage attempt{'s were' if len(stage_retries) != 1 else ' was'} re-run "
                     "after a failed attempt (usually a shuffle fetch failure after an executor was lost)")
    text = "Every job still succeeded, but " + "; ".join(parts) + "."
    if b["examples"]:
        text += " For example: " + b["examples"][0]
    text += " Retries are how Spark survives lost executors; if they keep happening, fix the cause (memory, spot "             "capacity) because they cost time."
    links, seen = [], set()
    for r in retries:
        k = (r["spark_context_id"], r["stage_id"], r["stage_attempt"])
        if k not in seen and len(seen) < 3:
            seen.add(k)
            links.append(link("stage", f"stage {k[1]}.{k[2]}", k[0], k[1], k[2]))
    for s in stage_retries[:3]:
        k = (s["spark_context_id"], s["stage_id"], s["stage_attempt"])
        if k not in seen:
            seen.add(k)
            links.append(link("stage", f"stage {k[1]}.{k[2]}", k[0], k[1], k[2]))
    return {"kind": "retries", "severity": "medium" if b["wasted_ms"] or stage_retries else "low",
            "title": "Retries (job still succeeded)", "text": text, "links": links}


def _next_steps(t) -> dict | None:
    seen, items, links = set(), [], []
    for f in t["findings"]:
        if f["fix"] and f["fix"] not in seen:
            seen.add(f["fix"])
            items.append(f"{f['category']}: {f['fix']}")
            links.append(link("finding", f["finding_id"], f["spark_context_id"], f["finding_id"]))
        if len(items) >= 5:
            break
    if not items:
        return None
    sev = t["findings"][0]["severity"] if t["findings"] else "info"
    return {"kind": "next_steps", "severity": sev, "title": "Next steps",
            "text": " ".join(f"{i + 1}. {s}" for i, s in enumerate(items)), "links": links}


def build_diagnosis(t: Mapping, rules: Rules) -> list[dict]:
    steps = []
    for fn in (_outcome, _first_error, lambda x: _root_cause(x, rules), _retries, lambda x: _performance(x, rules),
               _code_location,
               _next_steps):
        s = fn(t)
        if s:
            steps.append(s)
    return [{"step": i + 1, **s} for i, s in enumerate(steps)]


def cluster_info_block(rows: list[dict]) -> dict | None:
    """One merged object over the cluster_info rows (first non-null per field; cluster_creator left out)."""
    if not rows:
        return None
    out: dict = {}
    for r in rows:
        for k, v in r.items():
            if k in ("cluster_id", "cluster_creator") or v is None:
                continue
            out.setdefault(k, v)
    out["spark_contexts"] = [r["spark_context_id"] for r in rows]
    out.pop("spark_context_id", None)
    return out


def build_summary(t: Mapping, rules: Rules) -> dict:
    """`t`: mapping with cluster_id, input_dir, empty_reason, counts, totals, rows and the datasets as lists."""
    apps, jobs, queries = t["apps"], t["spark_jobs"], t["sql_queries"]
    if any(j["result"] == "JobFailed" for j in jobs) or any(q["status"] == "failed" for q in queries):
        status = "failed"
    elif jobs and all(j["result"] in ("JobSucceeded", "JobReplanned") for j in jobs):  # replanned by AQE: not a failure
        status = "succeeded"
    else:
        status = "unknown"
    starts = [a["start_time"] for a in apps if a["start_time"] is not None]
    ends = [a["end_time"] for a in apps if a["end_time"] is not None]
    start = min(starts) if starts else t.get("log_min_ts")
    end = max(ends) if ends else t.get("log_max_ts")
    fbs = {"high": 0, "medium": 0, "low": 0}
    for f in t["findings"]:
        fbs[f["severity"]] = fbs.get(f["severity"], 0) + 1
    return {
        "cluster_id": t["cluster_id"],
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "input_dir": t["input_dir"],
        "tool_version": __version__,
        "rules_file": rules.source_path,
        "empty_reason": t.get("empty_reason"),
        "status": status,
        "start_time": start, "end_time": end,
        "duration_ms": (end - start) if start is not None and end is not None else None,
        "spark_versions": sorted({a["spark_version"] for a in apps if a.get("spark_version")}),
        "counts": t["counts"],
        "totals": t["totals"],
        "findings_by_severity": fbs,
        "rows": t["rows"],
        "cluster_info": cluster_info_block(t.get("cluster_info") or []),
        "retries": retries_block(t.get("task_retries") or []),
        "diagnosis": [] if t.get("empty_reason") else build_diagnosis(t, rules),
        "warnings": t.get("warnings", [])[:50],
    }
