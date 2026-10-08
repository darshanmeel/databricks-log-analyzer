"""hotspots (Revision 5): when and where skew, shuffle, spill and GC peaked, which task, executor and SQL operator.

Kinds:
- skew_task     per stage attempt, tasks with task_ms >= skew_ratio x p50 and >= skew_min_task_ms (top N per stage),
                with a cause: data_skew (the task read far more bytes than the median), gc (the task spent a large
                share of its run time in GC), slow_executor (the same executor is slow in other stages too) or unknown.
- slowest_task  the N slowest tasks of each Spark context.
- shuffle_peak / spill_peak / gc_peak
                the top time buckets of spill_shuffle_timeline by shuffle bytes, disk+memory spill or GC time, with the
                stage and executor that contributed most to each bucket.
"""

from __future__ import annotations

import re

import pandas as pd

from ..config import Rules
from ..util import fmt_bytes, fmt_clock, fmt_words, is_null, lower_median

HOTSPOT_COLS = ["cluster_id", "spark_context_id", "kind", "ts_start", "ts_end", "stage_id", "stage_attempt",
                "spark_job_id", "sql_execution_id", "query_description", "operator", "task_id", "task_index",
                "executor_id", "host", "value", "baseline", "ratio", "share", "cause", "detail"]

# Operator most likely responsible, by hotspot kind: first operator in the query whose name matches, in this order.
_OPERATOR_PREFS = {
    "skew_task": [r"Join", r"Aggregate", r"Window", r"Sort\b", r"Exchange"],
    "slowest_task": [r"Join", r"Aggregate", r"Window", r"Sort\b", r"Exchange", r"Scan"],
    "shuffle_peak": [r"Exchange", r"AQEShuffleRead", r"Join", r"Aggregate"],
    "spill_peak": [r"Sort\b", r"SortMergeJoin", r"Aggregate", r"Window", r"Join"],
    "gc_peak": [r"Aggregate", r"Join", r"Sort\b", r"Window", r"BroadcastExchange"],
}


def _operator(kind: str, operators) -> str | None:
    ops = [o for o in (operators or []) if o]
    for pat in _OPERATOR_PREFS.get(kind, []):
        rx = re.compile(pat)
        for o in ops:
            if rx.search(o):
                return o
    return None


def _short(s, n: int = 90) -> str | None:
    if is_null(s) or not str(s).strip():
        return None
    s = " ".join(str(s).split())
    return s if len(s) <= n else s[: n - 1] + "…"


def _int(v):
    return None if is_null(v) else int(v)


def _stage_label(sid, att) -> str:
    sid = "?" if is_null(sid) else int(sid)
    att = 0 if is_null(att) else int(att)
    return f"stage {sid}" + (f".{att}" if att else "")


def _context(stages: list[dict], queries: list[dict]):
    stage_info = {(s["spark_context_id"], s["stage_id"], s["stage_attempt"] or 0): s for s in stages}
    query_info = {(q["spark_context_id"], q["sql_execution_id"]): q for q in queries}
    return stage_info, query_info


def _query_bits(kind: str, st: dict | None, query_info: dict):
    if not st:
        return None, None, None, None
    job = _int(st.get("spark_job_id"))
    qid = _int(st.get("sql_execution_id"))
    q = query_info.get((st["spark_context_id"], qid)) if qid is not None else None
    desc = _short(q.get("description")) if q else _short(st.get("job_description"))
    op = _operator(kind, q.get("operators")) if q else None
    if op is None:  # fall back to the stage's own operator scopes (RDD scopes)
        op = _operator(kind, st.get("rdd_scopes"))
    return job, qid, desc, op


def _where(op, qid, desc) -> str:
    parts = []
    if op:
        parts.append(op)
    if qid is not None:
        parts.append(f"in query {qid}" + (f" ({desc})" if desc else ""))
    elif desc:
        parts.append(f"in {desc}")
    return (" — " + " ".join(parts)) if parts else ""


def _executor_slowness(tdf: pd.DataFrame) -> dict[tuple, float]:
    """(ctx, executor) -> median over stages of (executor's median task_ms / stage median task_ms)."""
    t = tdf[tdf["task_ms"].notna() & tdf["executor_id"].notna()]
    if t.empty:
        return {}
    keys = ["spark_context_id", "stage_id", "stage_attempt"]
    stage_med = t.groupby(keys, dropna=False)["task_ms"].median().rename("stage_med")
    ex_med = t.groupby(keys + ["executor_id"], dropna=False)["task_ms"].median().rename("ex_med").reset_index()
    ex_med = ex_med.join(stage_med, on=keys)
    ex_med = ex_med[ex_med["stage_med"] > 0]
    ex_med["r"] = ex_med["ex_med"] / ex_med["stage_med"]
    return ex_med.groupby(["spark_context_id", "executor_id"])["r"].median().to_dict()


def _retry_text(g, r) -> str:
    """", its retry took 18 s on executor 2" when the same partition ran again in this stage attempt elsewhere."""
    if is_null(r.get("task_index")):
        return ""
    o = g[(g["task_index"] == r["task_index"]) & (g["task_id"] != r["task_id"]) & (g["executor_id"] != r["executor_id"])
          & ~g["failed"].astype(bool) & g["task_ms"].notna()]
    if o.empty:
        return ""
    b = o.sort_values("task_ms").iloc[0]
    return f", and its retry took {fmt_words(b['task_ms'])} on executor {b['executor_id']}"


def _skew_tasks(tdf, stage_info, query_info, rules: Rules, cid: str, stuck=frozenset()) -> list[dict]:
    out: list[dict] = []
    t = tdf[tdf["task_ms"].notna()]
    if t.empty:
        return out
    slowness = _executor_slowness(tdf)
    for (ctx, sid, att), g in t.groupby(["spark_context_id", "stage_id", "stage_attempt"], dropna=False, sort=True):
        att = 0 if is_null(att) else int(att)
        p50 = lower_median(g["task_ms"].tolist())
        if p50 is None:
            continue
        thr = max(rules.skew_ratio * max(p50, 1), rules.skew_min_task_ms)
        hot = g[g["task_ms"] >= thr].sort_values("task_ms", ascending=False).head(rules.hotspot_top_per_stage)
        if hot.empty:
            continue
        nbytes = g["shuffle_read"].fillna(0) + g["input_bytes"].fillna(0)
        med_bytes = lower_median(nbytes.tolist()) or 0
        st = stage_info.get((ctx, int(sid), att))
        job, qid, desc, op = _query_bits("skew_task", st, query_info)
        for _, r in hot.iterrows():
            b = (0 if is_null(r["shuffle_read"]) else r["shuffle_read"]) + (0 if is_null(r["input_bytes"]) else r["input_bytes"])
            bratio = b / med_bytes if med_bytes else (float("inf") if b > 0 else None)
            gc_share = (r["gc_ms"] / r["run_ms"]) if not is_null(r["gc_ms"]) and not is_null(r["run_ms"]) and r["run_ms"] else 0
            slow = slowness.get((ctx, r["executor_id"]))
            lost = bool(r.get("failed")) and r.get("end_reason") in ("ExecutorLostFailure", "TaskResultLost") and not b
            if (ctx, r["executor_id"]) in stuck:
                # the executor collected all the time and freed almost nothing: not skew, salting keys will not help
                cause = "gc_stuck"
                why = (f"it was stuck in GC on executor {r['executor_id']} (its heap stayed full after every collection)"
                       + _retry_text(g, r))
            elif lost:
                cause = "lost"
                why = f"no metrics: it was lost with executor {r['executor_id']}" + _retry_text(g, r)
            elif bratio is not None and bratio >= rules.hotspot_data_skew_ratio:
                cause = "data_skew"
                why = (f"it read {fmt_bytes(b)} vs a {fmt_bytes(med_bytes)} median"
                       + (f" ({bratio:.0f}×)" if bratio != float("inf") else "") + ", so this is data skew (a hot key or partition)")
            elif gc_share >= rules.gc_share:
                cause = "gc"
                why = f"it spent {gc_share:.0%} of its run time in garbage collection, so memory pressure slowed it"
            elif slow is not None and slow >= rules.hotspot_slow_executor_ratio:
                cause = "slow_executor"
                why = (f"its data size was normal but executor {r['executor_id']} is {slow:.1f}× slower than other "
                       f"executors across stages, so the node itself is slow")
            else:
                cause = "unknown"
                why = f"its input size was close to the median ({fmt_bytes(b)} vs {fmt_bytes(med_bytes)}), cause unclear"
            ratio = r["task_ms"] / max(p50, 1)
            host = None if is_null(r.get("host")) else r["host"]
            idx = _int(r.get("task_index"))
            detail = (f"Task {_int(r['task_id'])}" + (f" (partition {idx})" if idx is not None else "")
                      + f" on executor {r['executor_id']}" + (f" ({host})" if host else "")
                      + f" ran {fmt_words(r['task_ms'])} vs a {fmt_words(p50)} median ({ratio:.0f}×) in "
                      + _stage_label(sid, att) + _where(op, qid, desc) + f"; {why}.")
            out.append({"cluster_id": cid, "spark_context_id": ctx, "kind": "skew_task",
                        "ts_start": _int(r["launch_time"]), "ts_end": _int(r["finish_time"]),
                        "stage_id": int(sid), "stage_attempt": att, "spark_job_id": job, "sql_execution_id": qid,
                        "query_description": desc, "operator": op, "task_id": _int(r["task_id"]), "task_index": idx,
                        "executor_id": r["executor_id"], "host": host, "value": float(r["task_ms"]),
                        "baseline": float(p50), "ratio": round(float(ratio), 1), "share": None, "cause": cause,
                        "detail": detail})
    return out


def _slowest(tdf, stage_info, query_info, rules: Rules, cid: str) -> list[dict]:
    out: list[dict] = []
    t = tdf[tdf["task_ms"].notna()]
    for ctx, g in t.groupby("spark_context_id", sort=True):
        for _, r in g.sort_values("task_ms", ascending=False).head(rules.hotspot_slowest).iterrows():
            att = 0 if is_null(r["stage_attempt"]) else int(r["stage_attempt"])
            st = stage_info.get((ctx, int(r["stage_id"]), att))
            p50 = st.get("p50_task_ms") if st else None
            job, qid, desc, op = _query_bits("slowest_task", st, query_info)
            ratio = (r["task_ms"] / max(p50, 1)) if not is_null(p50) else None
            host = None if is_null(r.get("host")) else r["host"]
            detail = (f"Task {_int(r['task_id'])} in {_stage_label(r['stage_id'], att)} on executor {r['executor_id']} "
                      f"ran {fmt_words(r['task_ms'])}" + (f" ({ratio:.0f}× its stage median)" if ratio else "")
                      + _where(op, qid, desc) + (", and failed" if bool(r.get("failed")) else "") + ".")
            out.append({"cluster_id": cid, "spark_context_id": ctx, "kind": "slowest_task",
                        "ts_start": _int(r["launch_time"]), "ts_end": _int(r["finish_time"]),
                        "stage_id": int(r["stage_id"]), "stage_attempt": att, "spark_job_id": job,
                        "sql_execution_id": qid, "query_description": desc, "operator": op,
                        "task_id": _int(r["task_id"]), "task_index": _int(r.get("task_index")),
                        "executor_id": r["executor_id"], "host": host, "value": float(r["task_ms"]),
                        "baseline": None if is_null(p50) else float(p50),
                        "ratio": None if ratio is None else round(float(ratio), 1), "share": None, "cause": None,
                        "detail": detail})
    return out


_PEAKS = [
    ("shuffle_peak", ["shuffle_read", "shuffle_write"], "shuffle", fmt_bytes),
    ("spill_peak", ["disk_spill", "mem_spill"], "spill", fmt_bytes),
    ("gc_peak", ["gc_ms"], "GC time", fmt_words),
]


def _peaks(sst: pd.DataFrame, tdf: pd.DataFrame, stage_info, query_info, rules: Rules, cid: str) -> list[dict]:
    out: list[dict] = []
    if sst is None or len(sst) == 0:
        return out
    df = sst.copy()
    bucket = int(rules.timeline_bucket_seconds) * 1000
    hosts = {}
    if not tdf.empty:
        hosts = (tdf.dropna(subset=["executor_id", "host"]).drop_duplicates(["spark_context_id", "executor_id"])
                 .set_index(["spark_context_id", "executor_id"])["host"].to_dict())
    for kind, cols, word, fmt in _PEAKS:
        df["_v"] = df[cols].fillna(0).sum(axis=1)
        for ctx, g in df.groupby("spark_context_id", sort=True):
            per_bucket = g.groupby("minute")["_v"].sum()
            per_bucket = per_bucket[per_bucket > 0].sort_values(ascending=False).head(rules.hotspot_peaks)
            if per_bucket.empty:
                continue
            avg = float(g.groupby("minute")["_v"].sum().mean())
            for minute, total in per_bucket.items():
                b = g[g["minute"] == minute]
                by_stage = b.groupby(["stage_id", "stage_attempt"], dropna=False)["_v"].sum().sort_values(ascending=False)
                by_exec = b.groupby("executor_id", dropna=False)["_v"].sum().sort_values(ascending=False)
                (sid, att), sv = next(iter(by_stage.items()))
                ex, ev = next(iter(by_exec.items()))
                att = 0 if is_null(att) else int(att)
                st = stage_info.get((ctx, int(sid), att))
                job, qid, desc, op = _query_bits(kind, st, query_info)
                ex = None if is_null(ex) else str(ex)
                host = hosts.get((ctx, ex)) if ex else None
                s_share = sv / total if total else None
                e_share = ev / total if total else None
                detail = (f"{fmt(total)} of {word} between {fmt_clock(minute)} and {fmt_clock(minute + bucket)} UTC"
                          + (f" ({total / avg:.1f}× the average minute)"
                             if avg and total / avg >= 1.5 and g["minute"].nunique() > 1 else "")
                          + f"; {_stage_label(sid, att)} produced {s_share:.0%} of it" + _where(op, qid, desc)
                          + (f", and executor {ex}" + (f" ({host})" if host else "") + f" {e_share:.0%}" if ex else "")
                          + ".")
                out.append({"cluster_id": cid, "spark_context_id": ctx, "kind": kind, "ts_start": int(minute),
                            "ts_end": int(minute) + bucket, "stage_id": int(sid), "stage_attempt": att,
                            "spark_job_id": job, "sql_execution_id": qid, "query_description": desc, "operator": op,
                            "task_id": None, "task_index": None, "executor_id": ex, "host": host,
                            "value": float(total), "baseline": round(avg, 1) if avg else None,
                            "ratio": round(total / avg, 1) if avg else None,
                            "share": None if s_share is None else round(float(s_share), 3), "cause": None,
                            "detail": detail})
    df.drop(columns=["_v"], inplace=True, errors="ignore")
    return out


def build_hotspots(tdf: pd.DataFrame, stages: list[dict], queries: list[dict], sst: pd.DataFrame, rules: Rules,
                   cid: str, stuck=frozenset()) -> list[dict]:
    """`stuck`: (spark_context_id, executor_id) of executors stuck in GC (findings.gc_stuck)."""
    if tdf is None or tdf.empty:
        return []
    stage_info, query_info = _context(stages, queries)
    rows = (_skew_tasks(tdf, stage_info, query_info, rules, cid, stuck) + _peaks(sst, tdf, stage_info, query_info, rules, cid)
            + _slowest(tdf, stage_info, query_info, rules, cid))
    order = {"skew_task": 0, "shuffle_peak": 1, "spill_peak": 2, "gc_peak": 3, "slowest_task": 4}
    rows.sort(key=lambda r: (order[r["kind"]], r["spark_context_id"] or "", -(r["ratio"] or 0), r["ts_start"] or 0))
    return rows
