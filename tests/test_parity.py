"""Parity with the reference notebook's findings (cells A2 + A3 of the earlier Databricks notebook).

The notebook rules are re-implemented below in plain Python over the *built* stages / executors /
sql_queries / log_signals datasets and compared with the pipeline's ``findings`` dataset.

Allowed (intentional) differences:

1. A stage listed by several Spark jobs: the notebook's stage->job join duplicates the stage row (and so its
   findings); the pipeline keeps ONE row per stage attempt (lowest spark_job_id). The re-implementation runs on
   the de-duplicated stages dataset, so finding rows are compared as sets keyed per stage attempt.
2. Findings are keyed by spark_context_id (stage ids restart per context); the notebook does the same for
   event findings. Notebook log-signal findings have a null spark_context_id; the pipeline may fill it or not,
   so log-signal findings are compared on (category, severity) and occurrence count only.
3. New category ``exception`` (one per distinct fingerprint) does not exist in the notebook; excluded here.
4. Extra link columns (finding_id, stage_id, executor_id, ...) are new; not compared.
5. Thresholds come from rules.toml (defaults identical to the notebook).
"""

from __future__ import annotations

import re

import pandas as pd
import pytest

import make_fixtures as mf
from conftest import to_ms

GB = 1 << 30


def _nn(v) -> bool:
    """True when v is a real (non-null / non-NaN / non-NaT) value."""
    try:
        return not bool(pd.isna(v))
    except (TypeError, ValueError):
        return True


def _spark_round(x: float, nd: int = 0) -> float:
    """Spark F.round = HALF_UP."""
    from decimal import ROUND_HALF_UP, Decimal
    q = Decimal(1).scaleb(-nd)
    return float(Decimal(repr(float(x))).quantize(q, rounding=ROUND_HALF_UP))


def notebook_event_findings(stages: pd.DataFrame, executors: pd.DataFrame, sql: pd.DataFrame) -> list[dict]:
    out = []

    def stage_entity(r):
        return f"stage {r.stage_id}: {(r.stage_name or '')[:80]}"

    for r in stages.itertuples():
        base = {"spark_context_id": r.spark_context_id, "entity": stage_entity(r), "ts": to_ms(r.end_time),
                "stage_id": int(r.stage_id), "stage_attempt": int(r.stage_attempt)}
        fr = r.failure_reason if _nn(r.failure_reason) else None
        tasks = r.tasks if _nn(r.tasks) else None
        if fr is not None:
            out.append(dict(base, category="stage_failed", severity="high"))
        if _nn(r.skew) and _nn(r.max_task_ms) and r.skew >= 10 and r.max_task_ms >= 60000:
            out.append(dict(base, category="task_skew", severity="high"))
        if _nn(r.disk_spill) and r.disk_spill >= GB:
            out.append(dict(base, category="disk_spill", severity="high" if r.disk_spill >= 50 * GB else "medium"))
        if _nn(r.gc_share) and tasks and r.gc_share >= 0.2:
            # medium only when GC cost the stage a minute or more (the share alone is noise on a short stage)
            dur = r.duration_ms if _nn(r.duration_ms) else 0
            out.append(dict(base, category="gc_pressure", severity="medium" if r.gc_share * dur >= 60_000 else "low"))
        if fr is None and _nn(r.failed_tasks) and r.failed_tasks > 0:
            out.append(dict(base, category="task_retries", severity="medium"))
        if tasks and tasks >= 2000 and _nn(r.p50_task_ms) and r.p50_task_ms < 200:
            out.append(dict(base, category="tiny_tasks", severity="medium"))
    for r in executors.itertuples():
        reason = r.removed_reason if _nn(r.removed_reason) else None
        if reason is None:
            continue
        base = {"spark_context_id": r.spark_context_id, "entity": f"executor {r.executor_id}",
                "ts": to_ms(r.removed_time), "executor_id": r.executor_id}
        if re.search(r"(?i)memory|OOM|container killed|exit code 137", reason):
            out.append(dict(base, category="executor_oom", severity="high"))
        if re.search(r"(?i)decommission|spot|preempt|lost", reason):
            out.append(dict(base, category="executor_lost", severity="medium"))
    for r in sql.itertuples():
        if _nn(r.error) and r.error != "":
            out.append({"spark_context_id": r.spark_context_id, "category": "query_failed", "severity": "high",
                        "entity": f"query {r.sql_execution_id}: {(r.description or '')[:80]}",
                        "ts": to_ms(r.end_time)})
    return out


def notebook_log_findings(signals: pd.DataFrame) -> dict:
    """{category: (severity, occurrences)} like summarize_signals -> A3."""
    out = {}
    for (sig, sev), g in signals.groupby(["signal", "severity"]):
        out[f"log:{sig}"] = (sev, len(g))
    return out


def _key(d):
    return (d["spark_context_id"], d["category"], d["entity"], d["severity"], d["ts"])


@pytest.mark.parametrize("cid", [mf.MAIN, mf.HEALTHY, mf.EMPTY])
def test_event_findings_parity(ds, cid):
    expected = notebook_event_findings(ds(cid, "stages"), ds(cid, "executors"), ds(cid, "sql_queries"))
    f = ds(cid, "findings")
    # exception and executors_idle (Revision 12) are not notebook categories
    ev = f[~f.category.str.startswith("log:") & ~f.category.isin(["exception", "executors_idle", "large_tasks", "big_read", "oom_site"])]
    actual = [{"spark_context_id": r.spark_context_id, "category": r.category, "entity": r.entity,
               "severity": r.severity, "ts": to_ms(r.ts)} for r in ev.itertuples()]
    assert sorted(map(_key, actual), key=str) == sorted(map(_key, expected), key=str)


def test_event_findings_parity_main_is_not_trivial(ds):
    expected = notebook_event_findings(ds(mf.MAIN, "stages"), ds(mf.MAIN, "executors"), ds(mf.MAIN, "sql_queries"))
    cats = sorted(d["category"] for d in expected)
    assert cats == sorted(["stage_failed", "stage_failed", "task_skew", "disk_spill", "gc_pressure", "task_retries",
                           "tiny_tasks", "executor_oom", "executor_lost", "query_failed"])


@pytest.mark.parametrize("cid", [mf.MAIN, mf.HEALTHY, mf.EMPTY])
def test_log_signal_findings_parity(ds, cid):
    expected = notebook_log_findings(ds(cid, "log_signals"))
    f = ds(cid, "findings")
    lf = f[f.category.str.startswith("log:")]
    actual: dict = {}
    for r in lf.itertuples():
        sev, n = actual.get(r.category, (r.severity, 0))
        m = re.match(r"^(\d+) lines", r.evidence or "")
        actual[r.category] = (r.severity, n + (int(m.group(1)) if m else 0))
    assert {k: v[0] for k, v in actual.items()} == {k: v[0] for k, v in expected.items()}
    for k, (_, n) in actual.items():
        if n:  # evidence follows the notebook format "<n> lines on <m> executors. e.g. ..."
            assert n == expected[k][1], k


def test_evidence_formats_match_notebook(ds):
    """Spot-check the notebook's evidence strings for the main cluster."""
    f = ds(mf.MAIN, "findings")
    a = f[f.spark_context_id == mf.CTX_A]
    s = ds(mf.MAIN, "stages")
    s0 = s[(s.spark_context_id == mf.CTX_A) & (s.stage_id == 0)].iloc[0]
    skew = a[a.category == "task_skew"].iloc[0]
    secs = _spark_round(s0.max_task_ms / 1000)
    assert re.fullmatch(rf"slowest task {int(secs)}(\.0)?s = {re.escape(str(float(s0["skew"])))}x the median", skew.evidence), skew.evidence
    spill = a[a.category == "disk_spill"].iloc[0]
    assert spill.evidence.startswith("3.0 GiB spilled to disk")  # then how much went to memory first
    gc = a[a.category == "gc_pressure"].iloc[0]
    assert re.fullmatch(r"30(\.0)?% of task time in GC", gc.evidence), gc.evidence
    tiny = a[a.category == "tiny_tasks"].iloc[0]
    s2 = s[(s.spark_context_id == mf.CTX_A) & (s.stage_id == 2)].iloc[0]
    assert tiny.evidence == f"2000 tasks, median {int(s2.p50_task_ms)} ms"
    retries = a[a.category == "task_retries"].iloc[0]
    assert retries.evidence.startswith("2 failed task attempts, stage still succeeded")
    oom = a[a.category == "executor_oom"].iloc[0]
    assert oom.evidence.startswith(mf.OOM_REASON[:300])  # then where it ran out of memory
    qf = a[a.category == "query_failed"].iloc[0]
    assert qf.evidence == mf.JOB_ABORT[:300]
    sf = a[(a.category == "stage_failed")]
    assert set(sf.evidence) == {mf.FETCH_FAILURE[:300], mf.JOB_ABORT[:300]}
