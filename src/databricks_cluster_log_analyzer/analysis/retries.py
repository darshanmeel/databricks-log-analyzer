"""task_retries: what failed on the first try, and why (Revision 3 item 12), with plain-English explanations."""

from __future__ import annotations

import re
from collections import Counter

import pandas as pd

from ..config import Rules
from ..util import fmt_words, nn, to_int

RETRY_COLS = ["cluster_id", "spark_context_id", "stage_id", "stage_attempt", "task_index", "attempts",
              "failed_attempts", "first_attempt_executor_id", "first_attempt_host", "first_failure_reason",
              "first_failure_error", "first_failure_category", "first_failure_time", "executor_removed_reason",
              "final_status", "final_executor_id", "retry_delay_ms", "wasted_ms", "spark_job_id",
              "sql_execution_id", "explanation"]

#: how a removal category reads after "executor ..."
REMOVAL_WORDS = {"oom": "out of memory", "killed": "killed", "lost": "lost", "autoscale": "removed by autoscaling",
                 "termination": "stopped (cluster terminating)", "other": "removed"}
#: plain-English labels for first_failure_category
CATEGORY_LABELS = {"executor_lost": "executor lost", "oom": "out of memory", "fetch_failed": "shuffle fetch failed",
                   "exception": "exception in task code", "killed": "killed", "other": "other"}


OOM_TEXT_RE = re.compile(r"OutOfMemoryError|Java heap space|GC overhead limit exceeded|"
                         r"Requested array size exceeds|unable to create (new )?native thread|MemoryError")


def failure_category(end_reason: str | None, error: str | None, removal_cat: str | None, rules: Rules) -> str:
    """executor_lost / oom / fetch_failed / exception / killed / other for one failed task attempt."""
    er = end_reason or ""
    txt = error or ""
    if er == "FetchFailed":
        return "fetch_failed"
    if er in ("ExecutorLostFailure", "TaskResultLost", "Resubmitted"):
        cat = removal_cat or (rules.removal_category(txt) if txt else None)
        if cat == "oom":
            return "oom"
        if cat == "killed":
            return "killed"
        return "executor_lost"
    if er == "ExceptionFailure":
        return "oom" if OOM_TEXT_RE.search(txt) else "exception"
    if er == "TaskKilled":
        return "other" if "another attempt succeeded" in txt.lower() else "killed"
    return "other"


_PY_ERR_LINE_RE = re.compile(r"^\s*[\w.]*(?:Error|Exception|Exit|Interrupt)\b.*")


def _first_line(s: str | None, n: int = 160) -> str:
    """First line of an error text; for a Python traceback the final `XxxError: message` line instead."""
    s = (s or "").strip()
    if not s:
        return ""
    lines = s.splitlines()
    if "Traceback (most recent call last)" in lines[0]:
        for ln in reversed(lines[1:]):
            if _PY_ERR_LINE_RE.match(ln):
                return ln.strip()[:n]
    return lines[0][:n]


def explain(r: dict) -> str:
    """'Stage 12 task 7 failed first on executor 3: ExecutorLostFailure (executor killed: Command exited with
    code 9). Retried on executor 1 after 42 s and succeeded; 3 m 10 s of work lost.'"""
    task = f"task {r['task_index']}" if r.get("task_index") is not None else "a task"
    stage = f"Stage {r['stage_id']}" + (f" (attempt {r['stage_attempt']})" if r.get("stage_attempt") else "")
    ex = r.get("_failed_executor_id") or r.get("first_attempt_executor_id")
    s = f"{stage} {task} failed first" + (f" on executor {ex}" if ex is not None else "")
    reason = r.get("first_failure_reason") or "failure"
    if r.get("executor_removed_reason"):
        detail = f"executor {REMOVAL_WORDS.get(r.get('_removal_category') or 'other', 'removed')}: " \
                 f"{_first_line(r['executor_removed_reason'], 150)}"
    else:
        detail = _first_line(r.get("first_failure_error"), 150)
    s += f": {reason}" + (f" ({detail})" if detail else "") + "."
    if r.get("final_status") == "succeeded":
        if (r.get("attempts") or 0) > 1:
            fe = r.get("final_executor_id")
            s += " Retried on the same executor" if fe is not None and fe == ex else (
                f" Retried on executor {fe}" if fe is not None else " Retried")
            if r.get("retry_delay_ms"):
                s += f" after {fmt_words(r['retry_delay_ms'])}"
            s += " and succeeded"
        else:
            s += " It still succeeded"
    elif r.get("final_status") == "succeeded later":
        s += f" Stage attempt {r.get('_later_attempt')} ran it again and the stage succeeded"
    else:
        n = r.get("attempts") or 1
        if n == 1:
            s += " It was not retried in this stage attempt"
        else:
            s += f" All {n} attempts failed" + (
                f"; the last on executor {r['final_executor_id']}" if r.get("final_executor_id") is not None else "")
    s += f"; {fmt_words(r['wasted_ms'])} of work lost." if r.get("wasted_ms") else "."
    return s


def build_task_retries(tdf: pd.DataFrame, stages: list[dict], executors: list[dict], rules: Rules,
                       cluster_id: str) -> list[dict]:
    """One row per (spark_context_id, stage_id, stage_attempt, task_index) with more than one attempt or any
    failed attempt."""
    if tdf.empty:
        return []
    t = tdf[tdf["stage_id"].notna()].copy()
    if t.empty:
        return []
    t["stage_attempt"] = t["stage_attempt"].fillna(0)
    # without an Index (very old logs) every task id is its own "partition"
    t["_idx"] = t["task_index"].where(t["task_index"].notna(), -(t["task_id"].fillna(0) + 1))
    keys = ["spark_context_id", "stage_id", "stage_attempt", "_idx"]
    g = t.groupby(keys, sort=False)["failed"].agg(["size", "sum"])
    sel = g[(g["size"] > 1) | (g["sum"] > 0)]
    if sel.empty:
        return []
    t = t.merge(sel.reset_index()[keys], on=keys, how="inner")
    t = t.sort_values(keys + ["task_attempt", "launch_time", "task_id"], kind="mergesort", na_position="last")

    removed = {(e["spark_context_id"], e["executor_id"]): e for e in executors}
    stage_link = {(s["spark_context_id"], s["stage_id"]): s for s in stages}
    # the stage attempts that completed: a partition that failed in attempt 0 was run again by a later attempt
    # (task indexes are renumbered there, so the stage, not the index, settles it)
    done = {}
    for s in stages:
        if s.get("status") == "succeeded":
            k = (s["spark_context_id"], s["stage_id"])
            done[k] = max(done.get(k, -1), s.get("stage_attempt") or 0)
    out = []
    for key, grp in t.groupby(keys, sort=False):
        ctx, sid, att, idx = key
        recs = grp.to_dict("records")
        first = recs[0]
        failed = [r for r in recs if r["failed"]]
        ok = [r for r in recs if not r["failed"]]
        ff = min(failed, key=lambda r: (nn(r["finish_time"]) is None, nn(r["finish_time"]) or 0)) if failed else None
        row = {"cluster_id": cluster_id, "spark_context_id": ctx, "stage_id": to_int(sid),
               "stage_attempt": to_int(att), "task_index": to_int(idx) if idx >= 0 else None,
               "attempts": len(recs), "failed_attempts": len(failed),
               "first_attempt_executor_id": first["executor_id"], "first_attempt_host": first["host"],
               "first_failure_reason": None, "first_failure_error": None, "first_failure_category": None,
               "first_failure_time": None, "executor_removed_reason": None, "_removal_category": None,
               "_failed_executor_id": None, "retry_delay_ms": None}
        if ff is not None:
            ex = removed.get((ctx, ff["executor_id"]))
            rem_cat = ex.get("removal_category") if ex else None
            if ex and rem_cat is None:
                rem_cat = rules.removal_category(ex.get("removed_reason"))
            row.update(first_failure_reason=ff["end_reason"], first_failure_error=ff["error"],
                       first_failure_time=to_int(ff["finish_time"]), _failed_executor_id=ff["executor_id"])
            if ff["end_reason"] in ("ExecutorLostFailure", "TaskResultLost", "Resubmitted") and ex:
                row["executor_removed_reason"] = ex.get("removed_reason")
                row["_removal_category"] = rem_cat
            row["first_failure_category"] = failure_category(ff["end_reason"], ff["error"],
                                                             rem_cat if row["executor_removed_reason"] else None,
                                                             rules)
            f_launch = nn(ff["launch_time"])
            f_end = nn(ff["finish_time"])
            nxt = [nn(r["launch_time"]) for r in recs if r is not ff and nn(r["launch_time"]) is not None
                   and f_launch is not None and nn(r["launch_time"]) >= f_launch]
            if nxt and f_end is not None:
                row["retry_delay_ms"] = max(0, int(min(nxt) - f_end))
        final = ok[-1] if ok else recs[-1]
        later = done.get((ctx, row["stage_id"]), -1)
        row["final_status"] = "succeeded" if ok else ("succeeded later" if later > row["stage_attempt"] else "failed")
        row["final_executor_id"] = final["executor_id"]
        row["_later_attempt"] = later if row["final_status"] == "succeeded later" else None
        wasted = [nn(r["task_ms"]) for r in failed if nn(r["task_ms"]) is not None]
        row["wasted_ms"] = int(sum(wasted)) if wasted else None
        st = stage_link.get((ctx, row["stage_id"]))
        row["spark_job_id"] = st["spark_job_id"] if st else None
        row["sql_execution_id"] = st["sql_execution_id"] if st else None
        row["explanation"] = explain(row)
        out.append(row)
    out.sort(key=lambda r: (r["spark_context_id"], r["first_failure_time"] is None, r["first_failure_time"] or 0,
                            r["stage_id"], r["stage_attempt"], r["task_index"] if r["task_index"] is not None else -1))
    return out


def stage_retry_summary(retries: list[dict]) -> dict[tuple, dict]:
    """(ctx, stage_id, stage_attempt) -> {tasks, failed_attempts, by_category Counter, wasted_ms, failed_final,
    example (explanation of the earliest retry)}."""
    out: dict[tuple, dict] = {}
    for r in retries:
        k = (r["spark_context_id"], r["stage_id"], r["stage_attempt"])
        s = out.setdefault(k, {"tasks": 0, "failed_attempts": 0, "by_category": Counter(), "wasted_ms": 0,
                               "failed_final": 0, "example": r["explanation"], "first_time": r["first_failure_time"]})
        s["tasks"] += 1
        s["failed_attempts"] += r["failed_attempts"] or 0
        if r["first_failure_category"]:
            s["by_category"][r["first_failure_category"]] += 1
        s["wasted_ms"] += r["wasted_ms"] or 0
        if r["final_status"] == "failed":
            s["failed_final"] += 1
    return out


def category_text(c: Counter) -> str:
    return ", ".join(f"{n} {CATEGORY_LABELS.get(k, k)}" for k, n in c.most_common())
