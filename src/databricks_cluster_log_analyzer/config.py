"""Load the rules file (thresholds, signals, events, regexes) into a `Rules` dataclass."""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path

ENV_VAR = "DBX_LOG_ANALYZER_RULES"
SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2, "info": 3}


@dataclass(frozen=True)
class Signal:
    name: str
    pattern: str
    severity: str
    fix: str
    regex: re.Pattern = field(repr=False, compare=False)


@dataclass(frozen=True)
class Rules:
    # thresholds
    skew_ratio: float = 10.0
    skew_min_task_ms: int = 60000
    spill_bytes: int = 1 << 30
    spill_high_bytes: int = 50 << 30
    gc_share: float = 0.2
    tiny_tasks_min: int = 2000
    tiny_tasks_p50_ms: int = 200
    full_gc_min: int = 3
    idle_share_min: float = 0.6
    idle_core_min_ms: int = 1_800_000
    large_task_min_mb: int = 256
    large_task_min_ms: int = 60_000
    # big_read: a stage that read this much from storage (files, not shuffle); high from the second amount
    big_read_min_bytes: int = 20 << 30
    big_read_high_bytes: int = 100 << 30
    gc_pause_share_min: float = 0.1
    gc_stuck_full_per_min: float = 60.0
    gc_stuck_share: float = 0.5
    # Revision 17: waiting for cores, and MERGE that reads far more than it needs
    wait_min_ms: int = 300_000
    wait_share_min: float = 0.25
    cores_full_min_ms: int = 1_800_000
    cores_full_share_min: float = 0.2
    merge_read_min_bytes: int = 10 << 30
    merge_read_ratio: float = 5.0
    # Revision 4: graph + spill/shuffle timeline
    shuffle_heavy_bytes: int = 1 << 30
    graph_max_stages: int = 400
    graph_keep_longest: int = 200
    timeline_bucket_seconds: int = 60
    # Revision 5: hotspots
    hotspot_top_per_stage: int = 5
    hotspot_slowest: int = 10
    hotspot_peaks: int = 3
    hotspot_data_skew_ratio: float = 5.0
    hotspot_slow_executor_ratio: float = 2.0
    # story
    story_max_log_rows: int = 2000
    # executors
    executor_oom_regex: str = r"(?i)memory|OOM|container killed|exit code 137"
    executor_lost_regex: str = r"(?i)decommission|spot|preempt|lost"
    # ordered (category, regex) pairs; first match wins (see rules.toml [executors])
    removal_rules: tuple[tuple[str, str], ...] = ()
    # exceptions
    exception_high_regex: str = r"OutOfMemoryError|SparkException|Py4JJavaError|PythonException"
    exception_benign_regex: str = ""
    replanned_regex: str = r"Adaptive query execution has replanned|cancelled unused stages"
    framework_frame_prefixes: tuple[str, ...] = ()
    # events and signals
    events: tuple[str, ...] = ()
    signals: tuple[Signal, ...] = ()
    # a line matching this is never a signal (config / telemetry dumps); "" = no filter
    signal_ignore_regex: str = ""
    source_path: str = ""

    # --- compiled helpers -------------------------------------------------------------------------------------
    @property
    def signal_by_name(self) -> dict[str, Signal]:
        return {s.name: s for s in self.signals}

    def removal_category(self, reason: str | None) -> str | None:
        """oom / killed / termination / lost / autoscale / other, or None when there is no reason.

        The rules.toml [executors] regexes are tried in `removal_order`; the first match wins."""
        if reason is None:
            return None
        pairs = self.removal_rules or (("oom", self.executor_oom_regex), ("lost", self.executor_lost_regex))
        for cat, rx in _compiled(pairs):
            if rx.search(reason):
                return cat
        return "other"


_RX_CACHE: dict[tuple, tuple] = {}


def _compiled(pairs: tuple[tuple[str, str], ...]) -> tuple:
    hit = _RX_CACHE.get(pairs)
    if hit is None:
        hit = tuple((c, re.compile(r)) for c, r in pairs)
        _RX_CACHE[pairs] = hit
    return hit


REMOVAL_CATEGORIES = ("oom", "killed", "termination", "lost", "autoscale")
#: removal categories that are failures: they produce findings and count as "executors lost"
FAILURE_REMOVALS = ("oom", "lost", "killed")


def _read_toml(path: str | os.PathLike | None) -> tuple[dict, str]:
    if path is None:
        text = resources.files("databricks_cluster_log_analyzer").joinpath("rules.toml").read_text("utf-8")
        return tomllib.loads(text), "<packaged rules.toml>"
    p = Path(path)
    return tomllib.loads(p.read_text("utf-8")), str(p)


def _merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = {**out[k], **v}
        else:
            out[k] = v  # lists (signals, events) replace
    return out


def _checked(rx: str) -> str:
    if rx:
        re.compile(rx)  # fail early on a bad user regex
    return rx


def load_rules(path: str | os.PathLike | None = None) -> Rules:
    """Packaged rules.toml, overlaid with `path` (or $DBX_LOG_ANALYZER_RULES when path is None)."""
    data, src = _read_toml(None)
    user = path if path is not None else (os.environ.get(ENV_VAR) or None)
    if user:
        override, src = _read_toml(user)
        data = _merge(data, override)

    th = data.get("thresholds", {})
    ex = data.get("executors", {})
    exc = data.get("exceptions", {})
    removal_rules = []
    for cat in ex.get("removal_order") or list(REMOVAL_CATEGORIES):
        rx = ex.get(f"{cat}_regex")
        if rx:
            re.compile(rx)  # fail early on a bad user regex
            removal_rules.append((str(cat), rx))
    signals = []
    for s in data.get("signals", []):
        sev = str(s.get("severity", "medium")).lower()
        if sev not in SEVERITY_RANK:
            raise ValueError(f"signal {s.get('name')!r}: bad severity {sev!r}")
        signals.append(Signal(s["name"], s["pattern"], sev, s.get("fix", ""), re.compile(s["pattern"])))
    d = Rules()
    gr = data.get("graph", {})
    tl = data.get("timeline", {})
    hs = data.get("hotspots", {})

    def pick(sec: dict, key: str, default):
        return sec.get(key, th.get(key, default))

    bucket = int(pick(tl, "timeline_bucket_seconds", d.timeline_bucket_seconds))
    if bucket <= 0:
        raise ValueError(f"timeline_bucket_seconds must be > 0, got {bucket}")
    return Rules(
        shuffle_heavy_bytes=int(th.get("shuffle_heavy_bytes", d.shuffle_heavy_bytes)),
        graph_max_stages=max(1, int(pick(gr, "graph_max_stages", d.graph_max_stages))),
        graph_keep_longest=max(0, int(pick(gr, "graph_keep_longest", d.graph_keep_longest))),
        timeline_bucket_seconds=bucket,
        hotspot_top_per_stage=int(hs.get("top_per_stage", d.hotspot_top_per_stage)),
        hotspot_slowest=int(hs.get("slowest", d.hotspot_slowest)),
        hotspot_peaks=int(hs.get("peaks", d.hotspot_peaks)),
        hotspot_data_skew_ratio=float(hs.get("data_skew_ratio", d.hotspot_data_skew_ratio)),
        hotspot_slow_executor_ratio=float(hs.get("slow_executor_ratio", d.hotspot_slow_executor_ratio)),
        skew_ratio=float(th.get("skew_ratio", d.skew_ratio)),
        skew_min_task_ms=int(th.get("skew_min_task_ms", d.skew_min_task_ms)),
        spill_bytes=int(th.get("spill_bytes", d.spill_bytes)),
        spill_high_bytes=int(th.get("spill_high_bytes", d.spill_high_bytes)),
        gc_share=float(th.get("gc_share", d.gc_share)),
        tiny_tasks_min=int(th.get("tiny_tasks_min", d.tiny_tasks_min)),
        tiny_tasks_p50_ms=int(th.get("tiny_tasks_p50_ms", d.tiny_tasks_p50_ms)),
        story_max_log_rows=int(data.get("story", {}).get("story_max_log_rows", d.story_max_log_rows)),
        executor_oom_regex=ex.get("oom_regex", d.executor_oom_regex),
        executor_lost_regex=ex.get("lost_regex", d.executor_lost_regex),
        removal_rules=tuple(removal_rules),
        full_gc_min=int(th.get("full_gc_min", d.full_gc_min)),
        idle_share_min=float(th.get("idle_share_min", d.idle_share_min)),
        idle_core_min_ms=int(float(th.get("idle_core_min_minutes", d.idle_core_min_ms / 60_000)) * 60_000),
        large_task_min_mb=int(th.get("large_task_min_mb", d.large_task_min_mb)),
        large_task_min_ms=int(float(th.get("large_task_min_seconds", d.large_task_min_ms / 1000)) * 1000),
        big_read_min_bytes=int(float(th.get("big_read_min_gb", d.big_read_min_bytes / (1 << 30))) * (1 << 30)),
        big_read_high_bytes=int(float(th.get("big_read_high_gb", d.big_read_high_bytes / (1 << 30))) * (1 << 30)),
        gc_pause_share_min=float(th.get("gc_pause_share_min", d.gc_pause_share_min)),
        gc_stuck_full_per_min=float(th.get("gc_stuck_full_per_min", d.gc_stuck_full_per_min)),
        gc_stuck_share=float(th.get("gc_stuck_share", d.gc_stuck_share)),
        wait_min_ms=int(float(th.get("wait_min_minutes", d.wait_min_ms / 60_000)) * 60_000),
        wait_share_min=float(th.get("wait_share_min", d.wait_share_min)),
        cores_full_min_ms=int(float(th.get("cores_full_min_minutes", d.cores_full_min_ms / 60_000)) * 60_000),
        cores_full_share_min=float(th.get("cores_full_share_min", d.cores_full_share_min)),
        merge_read_min_bytes=int(float(th.get("merge_read_min_gb", d.merge_read_min_bytes / (1 << 30))) * (1 << 30)),
        merge_read_ratio=float(th.get("merge_read_ratio", d.merge_read_ratio)),
        exception_high_regex=exc.get("high_severity_regex", d.exception_high_regex),
        exception_benign_regex=exc.get("benign_regex", d.exception_benign_regex),
        replanned_regex=_checked(exc.get("replanned_regex", d.replanned_regex)),
        framework_frame_prefixes=tuple(exc.get("framework_frame_prefixes", [])),
        events=tuple(data.get("events", {}).get("names", [])),
        signals=tuple(signals),
        signal_ignore_regex=_checked(data.get("signal_filters", {}).get("ignore_line_regex", "")),
        source_path=src,
    )
