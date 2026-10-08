"""What these logs do not hold: pieces of a cluster's logs that are missing, so the numbers that need them are
partial or unknown. Each item names the gap and what it affects; the pages show them above the numbers.

Checks (all from what was read, nothing guessed):

- no event log, or no driver / executor logs at all;
- rolled event-log files missing: the rolled files are named by the time they were rolled, at a steady interval;
  a step much longer than the usual one is a file that is not there;
- the start of the event log missing: no application start or no environment, or executors that ran tasks with no
  "executor added" event;
- the end of the event log missing: no application end while the other logs go on later;
- executors in the event log with no log folder (no GC, out-of-memory or error lines for them);
- executor stdout with no GC lines at all (GC logging off, or the stdout files gone);
- the driver log starting well after the application did (older rolled driver files gone).
"""
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path

from ..util import fmt_words

# eventlog-2026-10-06--18-10(.gz): the time the file was rolled
_ROLLED_RE = re.compile(r"eventlog-(\d{4}-\d{2}-\d{2})--(\d{2})-(\d{2})")
GAP_FACTOR = 1.8          # a step this many times the usual one between rolled files is a missing file
LATE_MS = 5 * 60_000      # a log that starts this much after the app started has lost its first files


def _ms(v) -> int | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if hasattr(v, "timestamp"):
        if getattr(v, "tzinfo", None) is None:
            v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp() * 1000)
    return None


def _clock(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M")


def _rolled_times(paths: Iterable[Path]) -> list[int]:
    out = []
    for p in paths:
        m = _ROLLED_RE.search(p.name)
        if m:
            d = datetime.strptime(f"{m.group(1)} {m.group(2)}:{m.group(3)}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            out.append(int(d.timestamp() * 1000))
    return sorted(out)


def event_log_gaps(paths: Iterable[Path]) -> list[tuple[int, int]]:
    """(from, to) where rolled event-log files are missing, from the times in their names."""
    ts = _rolled_times(paths)
    if len(ts) < 3:
        return []
    steps = sorted(b - a for a, b in zip(ts, ts[1:]) if b > a)
    if not steps:
        return []
    usual = steps[len(steps) // 2]
    return [(a, b) for a, b in zip(ts, ts[1:]) if usual and b - a >= GAP_FACTOR * usual]


def coverage(classified: Mapping[str, list[Path]], cluster_dir: Path, apps: list[dict], executors: list[dict],
             tasks_executors: set[tuple], settings_rows: int, log_stats: Mapping[tuple, Mapping],
             gc_events: int, log_max_ts: int | None) -> list[dict]:
    """[{"kind", "text", "affects"}] in the order a reader needs them."""
    out: list[dict] = []

    def add(kind: str, text: str, affects: str):
        out.append({"kind": kind, "text": text, "affects": affects})

    ev, drv, exe = classified.get("eventlog") or [], classified.get("driver") or [], classified.get("executor") or []
    if not ev:
        add("no_event_log", "No event log in these logs.",
            "Jobs, stages, tasks, queries, runs, executors' lives and every time and size: unknown, not zero.")
    if not drv and not exe:
        add("no_logs", "No driver or executor logs, only the event log.",
            "Errors, out-of-memory and GC lines: not in these logs, so \"no errors\" means not logged here.")
    elif not exe:
        add("no_executor_logs", "No executor logs.",
            "GC, out-of-memory and errors on the executors: not in these logs.")

    # ---- the event log ----------------------------------------------------------------------------------------
    for a, b in event_log_gaps(ev):
        add("event_log_gap", f"Event-log files are missing between {_clock(a)} and {_clock(b)} UTC.",
            "Jobs, tasks and executors of that stretch are not here: no idle time, queue or autoscaling delay is "
            "claimed across it.")
        out[-1].update({"from": a, "to": b})
    if ev:
        starts = [a for a in apps if a.get("start_time") is not None]
        if apps and not starts:
            add("event_log_no_start", "The start of the event log is missing (no application start).",
                "Executors added before the first file kept are unknown: core counts and capacity are too low.")
        elif not settings_rows:
            add("event_log_no_start", "The start of the event log is missing (no environment / settings).",
                "Settings, the cluster's node types and its memory per executor: unknown.")
        added = {(e.get("spark_context_id"), str(e.get("executor_id"))) for e in executors if e.get("added_time") is not None}
        orphan = sorted({ex for ctx, ex in tasks_executors if ex and ex != "driver" and (ctx, ex) not in added},
                        key=lambda x: (len(x), x))
        if orphan:
            add("executors_no_start", f"Executor{'s' if len(orphan) > 1 else ''} {', '.join(orphan[:8])} ran tasks "
                f"but {'their' if len(orphan) > 1 else 'its'} start is not in the event log.",
                "The cores up before that point are unknown: \"no executor\" or \"0 cores\" there is not true.")
        ends = [_ms(a.get("end_time")) for a in apps]
        if apps and all(e is None for e in ends):
            last = max((_ms(e.get("removed_time")) or _ms(e.get("added_time")) or 0 for e in executors), default=0)
            if log_max_ts and log_max_ts - max(last, 0) > LATE_MS:
                add("event_log_no_end", f"The end of the event log is missing: the other logs run on to {_clock(log_max_ts)} UTC.",
                    "Work and executor removals after the last event-log file are not here; the last runs may "
                    "look unfinished.")

    # ---- driver and executor logs ----------------------------------------------------------------------------
    if exe and executors:
        have = {(app, str(ex)) for (src, app, ex) in log_stats if src == "executor" and ex is not None}
        have_ids = {ex for _a, ex in have}
        missing = sorted({str(e.get("executor_id")) for e in executors
                          if e.get("executor_id") not in (None, "driver") and str(e.get("executor_id")) not in have_ids},
                         key=lambda x: (len(x), x))
        if missing:
            add("executor_logs_missing", f"No log folder for executor{'s' if len(missing) > 1 else ''} "
                f"{', '.join(missing[:10])}{' …' if len(missing) > 10 else ''}.",
                "Their GC, out-of-memory and error lines are not here; counts from the logs are of the other "
                "executors only.")
    if exe and not gc_events:
        add("no_gc_lines", "No GC lines in the executors' stdout (GC logging off, or the stdout files are gone).",
            "Full GCs, heap after GC and \"stuck in GC\": unknown, not zero.")
    if drv and apps:
        app0 = min((_ms(a.get("start_time")) for a in apps if a.get("start_time") is not None), default=None)
        d0 = min((st["min_ts"] for (src, _a, _e), st in log_stats.items() if src == "driver" and st.get("min_ts")),
                 default=None)
        d0 = _ms(d0)
        if app0 and d0 and d0 - app0 > LATE_MS:
            add("driver_log_late", f"The driver log starts at {_clock(d0)} UTC, {fmt_words(d0 - app0)} after the "
                f"application did: its older files are gone.",
                "Errors and signals from before then are not here.")
    return out
