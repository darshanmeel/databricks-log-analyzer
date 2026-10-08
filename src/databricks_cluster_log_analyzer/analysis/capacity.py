"""Capacity: tasks queued on a full cluster, autoscaling that came late or removed workers mid-work, and what the
cores cost against the work done.

  capacity_bound      per run: its tasks waited for a core while every core was busy, counting every later wave of
                      tasks, not only the wait before a stage's first task (waited_for_cores).
  autoscale_lag       per Spark context: tasks queued on a full cluster and autoscaling added workers only minutes
                      later; with the core-seconds short, the seconds with no executor at all, and what the big stages
                      would have taken on all the cores (inferred).
  autoscale_removed   per removal burst: autoscaling removed executors while a stage ran, losing cached blocks or
                      shuffle data that the next stage then had to rebuild (a fetch failure naming the executor).

The sweep: every first task attempt is queued from its stage's submission until its launch, and busy from launch
to finish; the cores are the executors alive. The cluster is full while busy >= cores.
"""

from __future__ import annotations

import heapq
import re
from collections.abc import Mapping

import pandas as pd

from ..config import Rules
from ..util import fmt_words
from .contention import _run_name, to_ms

FIX = {
    "capacity_bound": (
        "The run needed more cores than the cluster had, so its tasks queued: more or bigger workers (raise the "
        "minimum workers so they are there when the run starts), or less work (cache less, drop count() calls that "
        "only log). More shuffle partitions will not help."),
    "autoscale_lag": (
        "Set the minimum workers to what the heavy step needs, or use fixed workers for this job: autoscaling added "
        "workers minutes after the tasks started to queue."),
    "autoscale_removed": (
        "Autoscaling removed workers that still held data: turn on graceful decommission with shuffle and cache "
        "migration (spark.decommission.enabled, spark.storage.decommission.shuffleBlocks.enabled and "
        "rddBlocks.enabled), raise the minimum workers for this job, or avoid caches that autoscaling can throw away."),
}

_EXEC_RE = re.compile(r"(?:executor[ :]+|BlockManagerId\()(\d+)", re.I)


def _f(cid, ctx, severity, category, entity, evidence, ts, **links) -> dict:
    row = {"cluster_id": cid, "spark_context_id": ctx, "severity": severity, "category": category, "entity": entity,
           "evidence": evidence, "fix": FIX[category], "ts": ts, "stage_id": None, "stage_attempt": None,
           "spark_job_id": None, "sql_execution_id": None, "executor_id": None, "signal": None, "fingerprint": None,
           "log_file_path": None, "log_seq": None, "run_key": None}
    row.update(links)
    return row


def _hms(ms: int) -> str:
    import datetime as _dt
    return _dt.datetime.fromtimestamp(ms / 1000, _dt.timezone.utc).strftime("%H:%M:%S")


def _wq(samples: list[tuple[int, int]], q: float) -> int:
    """Time-weighted quantile of (value, duration) samples."""
    if not samples:
        return 0
    s = sorted(samples)
    tot = sum(d for _, d in s)
    acc = 0
    for v, d in s:
        acc += d
        if acc >= q * tot:
            return v
    return s[-1][0]


def makespan(durations: list[float], slots: int) -> float:
    """Greedy list scheduling: the longest-first durations onto `slots` cores; the time the last one ends."""
    if not durations or slots <= 0:
        return 0.0
    h = [0.0] * min(slots, len(durations))
    for d in sorted(durations, reverse=True):
        heapq.heapreplace(h, h[0] + d)
    return max(h)


def sweep(tdf: pd.DataFrame, stages: list[dict], executors: list[dict]) -> dict[str, dict]:
    """Per Spark context: the queue, busy and cores over time (see the module doc)."""
    out: dict[str, dict] = {}
    if tdf is None or tdf.empty:
        return out
    sub = {(s["spark_context_id"], s["stage_id"], s["stage_attempt"]): to_ms(s.get("start_time")) for s in stages}
    has_run = "run_key" in tdf.columns
    t = tdf[tdf["launch_time"].notna()]
    for ctx, g in t.groupby("spark_context_id"):
        ev: list[tuple[int, int, int, int, str | None]] = []  # (t, d_queued, d_busy, d_cores, run)
        for r in g.itertuples(index=False):
            lt = int(r.launch_time)
            ft = int(r.finish_time) if not pd.isna(r.finish_time) else lt
            run = getattr(r, "run_key", None) if has_run else None
            run = None if run is None or (isinstance(run, float) and pd.isna(run)) else run
            if not r.task_attempt:  # a first attempt waited from the stage's submission
                s0 = sub.get((ctx, int(r.stage_id), int(r.stage_attempt)))
                if s0 is not None and s0 < lt:
                    ev.append((s0, 1, 0, 0, run))
                    ev.append((lt, -1, 0, 0, run))
            ev.append((lt, 0, 1, 0, run))
            ev.append((ft, 0, -1, 0, run))
        for e in executors:
            if e.get("spark_context_id") != ctx or e.get("executor_id") in (None, "driver"):
                continue
            a, b, c = to_ms(e.get("added_time")), to_ms(e.get("removed_time")), int(e.get("cores") or 0)
            if a is not None and c:
                ev.append((a, 0, 0, c, None))
                if b is not None:
                    ev.append((b, 0, 0, -c, None))
        ev.sort(key=lambda x: (x[0], -x[3], x[2]))  # cores arrive before tasks leave or start at the same ms
        q = busy = cores = 0
        q_run: dict = {}
        b_run: dict = {}
        st = {"queued_wall_ms": 0, "queued_task_ms": 0, "full_ms": 0, "zero_ms": 0, "max_cores": 0,
              "runs": {}, "series": []}
        prev = None
        for tm, dq, db, dc, run in ev:
            if prev is not None and tm > prev:
                dt = tm - prev
                full = busy >= cores
                if full and busy:
                    st["full_ms"] += dt
                if q > 0 and full:
                    st["queued_wall_ms"] += dt
                    st["queued_task_ms"] += q * dt
                    for rk, n in q_run.items():
                        if n <= 0:
                            continue
                        rs = st["runs"].setdefault(rk, {"queued_wall_ms": 0, "queued_task_ms": 0, "own_ms": 0,
                                                       "samples": []})
                        rs["queued_wall_ms"] += dt
                        rs["queued_task_ms"] += n * dt
                        rs["own_ms"] += n * dt * (b_run.get(rk, 0) / busy if busy else 0)
                        rs["samples"].append((n, dt))
                if q > 0 and cores == 0:
                    st["zero_ms"] += dt
                st["series"].append((prev, tm, q, busy, cores))
            prev = tm
            q += dq
            busy += db
            cores += dc
            st["max_cores"] = max(st["max_cores"], cores)
            if dq and run is not None:
                q_run[run] = q_run.get(run, 0) + dq
            if db and run is not None:
                b_run[run] = b_run.get(run, 0) + db
        out[ctx] = st
    return out


def capacity_findings(cid: str, tdf: pd.DataFrame, stages: list[dict], executors: list[dict], runs: list[dict],
                      cluster_info: list[dict] | None, log_signals: list[dict], rules: Rules,
                      sw: Mapping[str, dict] | None = None) -> list[dict]:
    out: list[dict] = []
    sw = sweep(tdf, stages, executors) if sw is None else sw

    # ---- per run: capacity-bound -----------------------------------------------------------------------------
    for r in runs:
        st = sw.get(r.get("spark_context_id")) or {}
        rs = (st.get("runs") or {}).get(r["run_key"])
        r0, r1 = to_ms(r.get("start_time")), to_ms(r.get("end_time"))
        if not rs or r0 is None or r1 is None or r1 <= r0:
            continue
        dur, w = r1 - r0, rs["queued_wall_ms"]
        if w < rules.wait_min_ms or w < rules.wait_share_min * dur:
            continue
        lo, hi = _wq(rs["samples"], 0.1), _wq(rs["samples"], 0.9)
        own = rs["own_ms"] / rs["queued_task_ms"] if rs["queued_task_ms"] else 1
        ev = (f"Tasks queued on a full cluster for {fmt_words(w)} of its {fmt_words(dur)} ({w / dur:.0%}): every core "
              f"busy with {lo if lo == hi else f'{lo}-{hi}'} of its tasks waiting "
              f"({rs['queued_task_ms'] / 1000:,.0f} task-seconds queued")
        ev += ", behind its own tasks)" if own >= 0.95 else f", {1 - own:.0%} of it behind other runs)"
        out.append(_f(cid, r.get("spark_context_id"), "high" if w >= 0.5 * dur else "medium", "capacity_bound",
                      f"run {_run_name(r)}", ev, r0, run_key=r["run_key"]))

    # ---- per Spark context: autoscaling ----------------------------------------------------------------------
    info = {c.get("spark_context_id"): c for c in cluster_info or []}
    for ctx, st in sw.items():
        ci = info.get(ctx) or {}
        ex = [e for e in executors if e.get("spark_context_id") == ctx and e.get("executor_id") not in (None, "driver")]
        per = max((int(e.get("cores") or 0) for e in ex), default=0)
        maxc = max(st["max_cores"], (int(ci["max_workers"]) * per) if ci.get("max_workers") and per else 0)
        if not maxc:
            continue
        # the first moment tasks queued on a full cluster below the most cores it could have
        t0 = q0 = c0 = None
        short = 0.0
        for a, b, q, busy, cores in st["series"]:
            if q > 0 and busy >= cores and cores < maxc:
                if t0 is None:
                    t0, q0, c0 = a, q, cores
                short += min(q, maxc - cores) * (b - a)
        adds = sorted(to_ms(e.get("added_time")) for e in ex if to_ms(e.get("added_time")) is not None)
        if t0 is not None:
            first_add = next((x for x in adds if x > t0), None)
            reach = next((a for a, b, q, busy, cores in st["series"] if a >= t0 and cores >= maxc), None)
            lag = (first_add - t0) if first_add is not None else None
            if lag is not None and lag >= 120_000 and short >= 600_000:
                ev = (f"{q0} tasks were queued at {_hms(t0)} on {c0} cores; the first new executor came "
                      f"{fmt_words(lag)} later")
                if reach is not None:
                    ev += f", and the {maxc} cores were there only {fmt_words(reach - t0)} after the queue started"
                ev += f". {short / 1000:,.0f} core-seconds short of the {maxc} cores"
                if st["zero_ms"] >= 10_000:
                    ev += f"; {fmt_words(st['zero_ms'])} with no executor at all while tasks waited"
                wi = _what_if(tdf, stages, ctx, t0, reach, maxc)
                if wi:
                    ev += (f". With the {maxc} cores from the start the stages that ran meanwhile would take about "
                           f"{fmt_words(wi[1])} instead of {fmt_words(wi[0])} (inferred)")
                if ci.get("min_workers") is not None and ci.get("max_workers") is not None:
                    ev += f". The cluster autoscales between {ci['min_workers']} and {ci['max_workers']} workers"
                out.append(_f(cid, ctx, "high" if lag >= 300_000 else "medium", "autoscale_lag",
                              f"autoscaling {fmt_words(lag)} late", ev, t0))
        out += _removed_mid_work(cid, ctx, ex, stages, tdf, log_signals)
    return out


def _what_if(tdf, stages, ctx, t0, t1, maxc) -> tuple[float, float] | None:
    """(actual, inferred) summed stage time of the successful stages that started while autoscaling caught up, with
    their task times scheduled onto all `maxc` cores."""
    if t1 is None:
        return None
    keys = {(s["stage_id"], s["stage_attempt"]): s for s in stages if s["spark_context_id"] == ctx
            and s.get("status") != "failed" and to_ms(s.get("start_time")) is not None
            and t0 <= to_ms(s["start_time"]) <= t1 and (s.get("duration_ms") or 0) >= 60_000}
    if not keys:
        return None
    t = tdf[(tdf["spark_context_id"] == ctx) & tdf["task_ms"].notna()]
    actual = inferred = 0.0
    for (sid, att), g in t.groupby(["stage_id", "stage_attempt"]):
        s = keys.get((int(sid), int(att)))
        if s is None:
            continue
        actual += s["duration_ms"]
        inferred += min(s["duration_ms"], makespan(g["task_ms"].tolist(), maxc))
    return (actual, inferred) if actual and actual - inferred >= 60_000 else None


def _removed_mid_work(cid, ctx, ex, stages, tdf, log_signals) -> list[dict]:
    gone = sorted(((to_ms(e["removed_time"]), e) for e in ex
                  if e.get("removal_category") == "autoscale" and to_ms(e.get("removed_time")) is not None), key=lambda x: x[0])
    if not gone:
        return []
    # bursts: removals within two minutes of each other
    bursts: list[list] = []
    for t, e in gone:
        if bursts and t - bursts[-1][-1][0] <= 120_000:
            bursts[-1].append((t, e))
        else:
            bursts.append([(t, e)])
    spans = [(to_ms(s.get("start_time")), to_ms(s.get("end_time")), s) for s in stages if s["spark_context_id"] == ctx]
    spans = [x for x in spans if x[0] is not None and x[1] is not None]
    lost_lines = sorted(r["ts"] for r in log_signals if r.get("signal") == "cache_lost" and r.get("ts") is not None)
    ff = tdf[(tdf["spark_context_id"] == ctx) & (tdf["end_reason"] == "FetchFailed")] if tdf is not None and not tdf.empty else None
    out = []
    for i, b in enumerate(bursts):
        t0, t1 = b[0][0], b[-1][0]
        nxt = bursts[i + 1][0][0] if i + 1 < len(bursts) else 1 << 62
        ids = {str(e["executor_id"]) for _, e in b}
        active = [s for a, z, s in spans if a <= t1 and z >= t0 - 60_000 and s.get("status") != "failed"]
        lost = sum(1 for x in lost_lines if t0 <= x < nxt)
        fetch = None
        if ff is not None and not ff.empty:
            for r in ff[(ff["finish_time"] >= t0) & (ff["finish_time"] < min(nxt, t1 + 1_800_000))].itertuples():
                named = set(_EXEC_RE.findall(str(r.error or "")))
                if named & ids:
                    fetch = (int(r.stage_id), sorted(named & ids))
                    break
        if not active and not lost and not fetch:
            continue
        ev = f"At {_hms(t0)} autoscaling removed {len(b)} executor{'s' if len(b) != 1 else ''} ({', '.join(sorted(ids))})"
        if active:
            ev += f" while stage {active[0]['stage_id']}" + (f" and {len(active) - 1} more" if len(active) > 1 else "") + " ran"
        if lost:
            ev += f"; {lost} cached block{'s were' if lost != 1 else ' was'} lost"
        if fetch:
            ev += (f"; then stage {fetch[0]} hit a fetch failure: its shuffle data was on executor "
                   f"{', '.join(fetch[1])}, so it was rebuilt")
        sev = "high" if fetch or lost >= 100 else "medium"
        top = active[0] if active else {}
        out.append(_f(cid, ctx, sev, "autoscale_removed", f"autoscaling removed {len(b)} executors mid-work", ev, t0,
                      stage_id=fetch[0] if fetch else top.get("stage_id"), run_key=top.get("run_key"),
                      sql_execution_id=top.get("sql_execution_id")))
    return out


def core_use(tdf: pd.DataFrame, executors: list[dict], apps: list[dict]) -> dict | None:
    """Worker core-seconds paid for against the core-seconds of successful tasks."""
    end = {a["spark_context_id"]: to_ms(a.get("end_time")) for a in apps}
    paid = 0.0
    for e in executors:
        if e.get("executor_id") in (None, "driver"):
            continue
        a = to_ms(e.get("added_time"))
        z = to_ms(e.get("removed_time")) or end.get(e.get("spark_context_id"))
        if a is not None and z is not None and z > a:
            paid += (z - a) * int(e.get("cores") or 0)
    if tdf is None or tdf.empty or not paid:
        return None
    useful = float(tdf.loc[~tdf["failed"].astype(bool), "task_ms"].sum())
    if not useful:
        return None
    return {"worker_core_s": round(paid / 1000), "useful_task_s": round(useful / 1000),
            "core_s_per_useful": round(paid / useful, 2)}


__all__ = ["capacity_findings", "sweep", "makespan", "core_use", "CATEGORIES"]
CATEGORIES = ("capacity_bound", "autoscale_lag", "autoscale_removed")
