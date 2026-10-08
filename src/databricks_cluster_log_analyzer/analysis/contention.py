"""Revision 17: findings about cores and MERGE, computed once runs are known.

  cores_full        per Spark context: runs spent a large share of their time waiting for a free core (more runs than
                    the executors could take), with how many ran at once, the cores there were, and autoscaling.
  waited_for_cores  per run: it spent a large share of its time with a stage submitted and no core free.
  merge_rewrite     per Delta MERGE that read far more of the target than its source, because the merge condition
                    does not let Delta skip files (numbers from analysis.merge.merge_facts).

Waiting for a core: a stage was submitted and its first task had not started yet, while none of the run's (or the
query's) other stages ran a task. Running: at least one of its stages ran tasks.
"""

from __future__ import annotations

import datetime as _dt
import re
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from ..config import SEVERITY_RANK, Rules
from ..util import fmt_bytes, fmt_words
from .merge import files_text, merge_facts, merge_fix, wrote_text

GB = 1 << 30

FIX = {
    "cores_full": (
        "The cores were the bottleneck, not the data: more runs were started than the executors had cores for, so "
        "their stages queued. Give the job more cores when the runs start (raise the job cluster's minimum workers: "
        "it starts at the minimum and autoscaling adds workers only minutes later; raise the maximum too), or start "
        "fewer at once (a for-each task's concurrency, or split the runs into groups that run one after another). "
        "Job compute bills per core-hour used, so more cores for the same work costs about the same and finishes "
        "sooner."),
    "waited_for_cores": (
        "Its stages waited for cores held by other runs on the same cluster: see the 'cores were full' finding. Give "
        "the cluster more cores when the runs start, or run fewer at once."),
    # the general fix; each finding carries its own, from what the logs show about that MERGE (analysis.merge)
    "merge_rewrite": (
        "Let Delta skip files: if the target has a partition or clustering column, add it to the MERGE ON condition "
        "(only if a key's rows never fall outside the bound, else NOT MATCHED inserts duplicates); otherwise cluster "
        "the target on the merge key (Liquid Clustering), which helps only if each batch's keys fall in a narrow range. "
        "If deletion vectors are off, turn them on so a matched row does not rewrite its whole file."),
}


def to_ms(v: Any) -> int | None:
    """Epoch ms from an int, a datetime or a pandas Timestamp (naive = UTC); None for missing."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return None if v != v else int(v)  # NaN
    if hasattr(v, "to_pydatetime"):
        v = v.to_pydatetime()
    if isinstance(v, _dt.datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=_dt.timezone.utc)
        return int(v.timestamp() * 1000)
    return None


def merge_spans(spans: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for a, b in sorted(x for x in spans if x[1] > x[0]):
        if out and a <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], b))
        else:
            out.append((a, b))
    return out


def wait_and_run(stages: list[dict], firsts: Mapping[tuple, int], key: Callable[[dict], Any]) -> dict[Any, tuple[list, list]]:
    """Per key (a query, a run): the stretches where one of its stages was submitted and waiting for a free core while
    none of its stages ran a task, and the merged stretches where at least one of its stages ran tasks."""
    wait: dict[Any, list] = {}
    run_: dict[Any, list] = {}
    for st in stages:
        k = key(st)
        s0, s1 = to_ms(st.get("start_time")), to_ms(st.get("end_time"))
        if k is None or s0 is None or s1 is None:
            continue
        f = firsts.get((st["spark_context_id"], st["stage_id"], st["stage_attempt"]))
        f = min(max(f, s0), s1) if f is not None else s0
        wait.setdefault(k, []).append((s0, f))
        run_.setdefault(k, []).append((f, s1))
    out: dict[Any, tuple[list, list]] = {}
    for k in set(wait) | set(run_):
        busy = merge_spans(run_.get(k, []))
        spans = []
        for a, b in merge_spans(wait.get(k, [])):
            for ra, rb in busy:  # cut out the time another of its stages was running
                if rb <= a or ra >= b:
                    continue
                if ra > a:
                    spans.append((a, ra))
                a = max(a, rb)
                if a >= b:
                    break
            if a < b:
                spans.append((a, b))
        out[k] = (spans, busy)
    return out


def first_tasks(tasks) -> dict[tuple, int]:
    """(spark_context_id, stage_id, stage_attempt) -> launch time (ms) of its first task, from the tasks table (a
    DataFrame or a list of dicts)."""
    if tasks is None:
        return {}
    if hasattr(tasks, "groupby"):
        if tasks.empty:
            return {}
        g = tasks.groupby(["spark_context_id", "stage_id", "stage_attempt"])["launch_time"].min()
        return {k: to_ms(v) for k, v in g.items() if to_ms(v) is not None}
    out: dict[tuple, int] = {}
    for t in tasks:
        k = (t["spark_context_id"], t["stage_id"], t["stage_attempt"])
        lt = to_ms(t.get("launch_time"))
        if lt is not None and (k not in out or lt < out[k]):
            out[k] = lt
    return out


def _f(cid, ctx, severity, category, entity, evidence, ts, **links) -> dict:
    row = {"cluster_id": cid, "spark_context_id": ctx, "severity": severity, "category": category, "entity": entity,
           "evidence": evidence, "fix": FIX[category], "ts": ts, "stage_id": None, "stage_attempt": None,
           "spark_job_id": None, "sql_execution_id": None, "executor_id": None, "signal": None, "fingerprint": None,
           "log_file_path": None, "log_seq": None, "run_key": None}
    row.update(links)
    return row


def _hhmm(ms: int) -> str:
    return _dt.datetime.fromtimestamp(ms / 1000, _dt.timezone.utc).strftime("%H:%M")


def _run_name(r: Mapping) -> str:
    return " · ".join(x for x in (r.get("program"), r.get("subject")) if x) or r.get("label") or r["run_key"]


def contention_findings(cid: str, stages: list[dict], firsts: Mapping[tuple, int], runs: list[dict],
                        executors: list[dict], queries: list[dict], cluster_info: list[dict] | None,
                        rules: Rules, plan_nodes: list[dict] | None = None) -> list[dict]:
    """`queries` should carry final_plan for the MERGE queries and `plan_nodes` their scan nodes (sql_plan_nodes):
    the MERGE finding reads deletion vectors, CDF and the files touched from them."""
    out: list[dict] = []
    per_run = wait_and_run(stages, firsts, lambda s: s.get("run_key"))
    stages_of: dict[str, list[dict]] = {}
    for s in stages:
        if s.get("run_key"):
            stages_of.setdefault(s["run_key"], []).append(s)

    # ---- per run: waited for cores --------------------------------------------------------------------------------
    waits: dict[str, int] = {}
    for r in runs:
        w, b = per_run.get(r["run_key"], ([], []))
        wait, ran = sum(e - a for a, e in w), sum(e - a for a, e in b)
        waits[r["run_key"]] = wait
        r0, r1 = to_ms(r.get("start_time")), to_ms(r.get("end_time"))
        dur = (r1 - r0) if r0 is not None and r1 is not None else None
        if not dur or wait < rules.wait_min_ms or wait < rules.wait_share_min * dur:
            continue
        # the stages that waited the longest, and how long they then ran
        ex = []
        for s in stages_of.get(r["run_key"], []):
            s0, s1 = to_ms(s.get("start_time")), to_ms(s.get("end_time"))
            f = firsts.get((s["spark_context_id"], s["stage_id"], s["stage_attempt"]))
            if s0 is None or s1 is None or f is None:
                continue
            f = min(max(f, s0), s1)
            ex.append((f - s0, s1 - f, s))
        ex.sort(key=lambda x: -x[0])
        longer = sum(1 for w_, r_, _ in ex if w_ >= 1000 and w_ > r_)
        ev = (f"Waited {fmt_words(wait)} for a free core during its {fmt_words(dur)} ({wait / dur:.0%}), while its "
              f"stages ran tasks for {fmt_words(ran)}")
        if longer:
            ev += f"; {longer} of its {len(ex)} stages waited longer than they ran"
        if ex and ex[0][0] >= 1000:
            ev += ". Longest waits: " + "; ".join(
                f"stage {s['stage_id']} waited {fmt_words(w_)}, then ran {fmt_words(r_)}" for w_, r_, s in ex[:3] if w_ >= 1000)
        slow = ((r.get("vs_typical") or 0) >= 2 and (r.get("same_job_runs") or 0) >= 3
                and (r.get("duration_ms") or 0) - (r.get("typical_duration_ms") or 0) >= 60_000)
        # a run that mostly waited is worth a look whatever its "x usual": when every run of a batch waits, the
        # usual is the same slow batch
        mostly_waited = wait >= 5 * 60_000 and wait >= 0.5 * dur
        if mostly_waited and not slow:
            ev += f". It mostly waited: {wait / max(1, ran):.0f}x longer waiting than running"
        if slow:
            ev += f". It took {r['vs_typical']:.1f}x its usual {fmt_words(r.get('typical_duration_ms'))}"
        top = ex[0][2] if ex else {}
        out.append(_f(cid, r.get("spark_context_id"), "medium" if slow or mostly_waited else "low", "waited_for_cores",
                      f"run {_run_name(r)}", ev, r0, run_key=r["run_key"], stage_id=top.get("stage_id"),
                      stage_attempt=top.get("stage_attempt"), spark_job_id=top.get("spark_job_id"),
                      sql_execution_id=top.get("sql_execution_id")))

    # ---- per Spark context: the cores were full -------------------------------------------------------------------
    info = {c.get("spark_context_id"): c for c in cluster_info or []}
    by_ctx: dict[str, list[dict]] = {}
    for r in runs:
        if to_ms(r.get("start_time")) is not None and to_ms(r.get("end_time")) is not None:
            by_ctx.setdefault(r.get("spark_context_id"), []).append(r)
    for ctx, rs in by_ctx.items():
        total = sum(to_ms(r["end_time"]) - to_ms(r["start_time"]) for r in rs)
        wait = sum(waits.get(r["run_key"], 0) for r in rs)
        if not total or wait < rules.cores_full_min_ms or wait < rules.cores_full_share_min * total:
            continue
        # how many runs at once, and when the most were running
        ev_ = sorted([(to_ms(r["start_time"]), 1) for r in rs] + [(to_ms(r["end_time"]), -1) for r in rs],
                     key=lambda x: (x[0], x[1]))
        n = peak = 0
        peak_at = ev_[0][0]
        for t, d in ev_:
            n += d
            if n > peak:
                peak, peak_at = n, t
        ex = [e for e in executors if e.get("spark_context_id") == ctx and e.get("executor_id") not in (None, "driver")]

        def cores_at(t: int) -> tuple[int, int]:
            up = [e for e in ex if (to_ms(e.get("added_time")) or 0) <= t and (to_ms(e.get("removed_time")) or 1 << 62) > t]
            return len(up), sum(int(e.get("total_cores") or e.get("cores") or 0) for e in up)

        times = sorted({to_ms(e.get("added_time")) for e in ex if to_ms(e.get("added_time")) is not None})
        max_c, max_n = 0, 0
        for t in times:
            k, c = cores_at(t)
            if c > max_c:
                max_c, max_n = c, k
        pk_n, pk_c = cores_at(peak_at)
        worst = max(rs, key=lambda r: waits.get(r["run_key"], 0))
        wd = to_ms(worst["end_time"]) - to_ms(worst["start_time"])
        ev = (f"Up to {peak} runs ran at once (at {_hhmm(peak_at)} UTC, on {pk_c} cores of {pk_n} "
              f"executor{'s' if pk_n != 1 else ''}); the "
              f"executors had at most {max_c} cores. Together the runs spent {fmt_words(wait)} waiting for a free core, "
              f"{wait / total:.0%} of their {fmt_words(total)} of run time. Worst: {_run_name(worst)} waited "
              f"{fmt_words(waits[worst['run_key']])} of its {fmt_words(wd)}")
        ci = info.get(ctx) or (cluster_info[0] if cluster_info else {})
        if ci.get("min_workers") is not None and ci.get("max_workers") is not None:
            ev += f". The cluster autoscales between {ci['min_workers']} and {ci['max_workers']} workers"
        first = min(to_ms(r["start_time"]) for r in rs)
        k0, c0 = cores_at(first)
        later = [t for t in times if t > first]
        if c0 < max_c and later:
            reach = next((t for t in later if cores_at(t)[1] >= max_c), later[-1])
            ev += (f"; when the first runs started at {_hhmm(first)} it had {c0} cores, and reached {max_c} only at "
                   f"{_hhmm(reach)}")
        out.append(_f(cid, ctx, "high" if wait >= 0.4 * total else "medium", "cores_full",
                      f"{len(rs)} runs on {max_c} cores", ev, first))

    # ---- per Delta MERGE that rewrote files ----------------------------------------------------------------------
    by_run_q: dict[Any, list[dict]] = {}
    for q in queries:
        by_run_q.setdefault(q.get("run_key"), []).append(q)
    nodes_of: dict[tuple, list[dict]] = {}
    for n in plan_nodes or []:
        nodes_of.setdefault((n.get("spark_context_id"), n.get("sql_execution_id")), []).append(n)
    stages_q: dict[tuple, list[dict]] = {}
    for s in stages:
        stages_q.setdefault((s.get("spark_context_id"), s.get("sql_execution_id")), []).append(s)
    for q in queries:
        desc = q.get("description") or ""
        if "MERGE" not in desc or not re.search(r"(?i)rewriting", desc) or (q.get("input_bytes") or 0) < rules.merge_read_min_bytes:
            continue
        sib = [x for x in by_run_q.get(q.get("run_key"), []) if x is not q]
        keys = [(x.get("spark_context_id"), x.get("sql_execution_id")) for x in [q, *sib]]
        mf = merge_facts(q, sib, [st for k in keys for st in stages_q.get(k, [])],
                         nodes=[n for k in keys for n in nodes_of.get(k, [])])
        # the target only: the step also reads its source copy back, which is not the target
        read = mf["target_bytes"]
        if read < rules.merge_read_min_bytes:
            continue
        src = max((x.get("input_bytes") or 0 for x in sib if re.search(r"(?i)materiali[sz]e source", x.get("description") or "")), default=0)
        scan = mf["scan_bytes"]
        if src and read < rules.merge_read_ratio * src:
            continue
        rows = mf["target_rows"]
        spill = q.get("disk_spill") or 0
        ft = files_text(mf)
        ev = (f"The MERGE {'rewrote ' + str(mf['files_touched']) + ' files: it ' if mf['files_touched'] and not mf['dv_on'] else ''}"
              f"read {fmt_bytes(read)} of the target"
              + (f" ({rows:,.0f} rows)" if rows else "")
              + f" and wrote {fmt_bytes(q.get('output_bytes') or 0)}"
              + (f", spilling {fmt_bytes(spill)} to disk" if spill >= GB else "")
              + (f", to merge a source of {fmt_bytes(src)} ({read / src:,.0f}x less)" if src else "")
              + (f". {ft[:1].upper() + ft[1:]}" if ft else "")
              + (f". Finding the matching files scanned another {fmt_bytes(scan)}" if scan >= GB else "")
              + (f". The source copy was read again: {fmt_bytes(mf['copy_bytes'])}, not counted as target"
                 if mf["copy_bytes"] >= GB else "")
              + (f". {wrote_text(mf)[:1].upper() + wrote_text(mf)[1:]}" if mf["dv_on"] else "")
              + (". The merge condition does not let Delta skip files, so most of the target is read."
                 if mf["dv_on"] else
                 ". The merge condition does not let Delta skip files, so most of the target is read and rewritten."))
        sev = "high" if read >= 100 * GB or spill >= rules.spill_high_bytes else "medium"
        name = f"MERGE into {mf['target']}" if mf["target"] else "MERGE"
        out.append(_f(cid, q.get("spark_context_id"), sev, "merge_rewrite",
                      f"{name} (query {q['sql_execution_id']})", ev, to_ms(q.get("start_time")),
                      run_key=q.get("run_key"), sql_execution_id=q["sql_execution_id"], fix=" ".join(merge_fix(mf))))
    return out


def add_findings(findings: list[dict], runs: list[dict], new: list[dict], replace: Iterable[str] = ()) -> list[dict]:
    """Append `new` findings (dropping existing ones of the `replace` categories first) with ids after the existing
    ones, and update each run's finding count and worst severity. Returns the new list."""
    drop = set(replace)
    kept = [f for f in findings if f.get("category") not in drop]
    width = max(3, len(str(len(kept) + len(new))))
    nums = [int(f["finding_id"][1:]) for f in kept if str(f.get("finding_id") or "")[1:].isdigit()]
    nxt = max(nums, default=0) + 1
    new = sorted(new, key=lambda r: (SEVERITY_RANK.get(r["severity"], 9), r["ts"] is None, r["ts"] or 0))
    for i, r in enumerate(new):
        r["finding_id"] = f"F{nxt + i:0{width}d}"
    allf = kept + new
    cnt: dict[str, int] = {}
    worst: dict[str, str] = {}
    for f in allf:
        k = f.get("run_key")
        if not k:
            continue
        cnt[k] = cnt.get(k, 0) + 1
        if k not in worst or SEVERITY_RANK.get(f["severity"], 9) < SEVERITY_RANK.get(worst[k], 9):
            worst[k] = f["severity"]
    for r in runs:
        r["findings"] = cnt.get(r["run_key"], 0)
        r["max_severity"] = worst.get(r["run_key"])
    return allf


CATEGORIES = ("cores_full", "waited_for_cores", "merge_rewrite")
