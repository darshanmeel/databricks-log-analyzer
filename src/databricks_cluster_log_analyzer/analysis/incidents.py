"""Incidents: findings tied back to where they happened and chained cause -> effect.

Findings are produced one detector at a time, so one real problem shows up several times (the OOM as an executor
removal, as a log signal and as an exception) and the links to stages and executors are often missing (a log
signal knows only its log lines). This module

1. resolves each finding's *scope*: stage attempts, executors, hosts and queries, from its own links, the log lines
   behind it (`stage 6.1`, `executor 3`, `Lost executor 1 on 10.0.0.1`, `Failed to connect to /10.0.0.1:...`) and the
   tasks that were running on its executor at that moment;
2. merges findings that are the same problem (same kind and the same stage, the same executor at about the same time,
   or the same message / user frame) into one *problem*. A log-signal finding covers every line of that signal in the
   cluster, so it joins at most one problem and never bridges two;
3. links problems cause -> effect by Spark's usual failure order (skew, spill, GC, out of memory, executor lost,
   fetch failure / task error, stage failed, job aborted, query failed, error reported to the notebook) when they share
   scope and the cause came first;
4. groups connected problems (and the performance findings of one query) into *incidents*, each with a most likely
   root cause, and writes one row per finding.

All of it is a heuristic over the logs: `because` says why two problems were linked so the reader can judge.
"""
from __future__ import annotations

import re
from collections.abc import Mapping

import pandas as pd

from ..util import fmt_words, is_null

INCIDENT_COLS = ["cluster_id", "finding_id", "incident_id", "incident_rank", "incident_title", "incident_severity",
                 "incident_impact", "incident_start", "incident_end", "problem_id", "kind", "role", "caused_by",
                 "because", "confidence", "spark_context_id", "stage_id", "stage_attempt", "spark_job_id", "sql_execution_id",
                 "executor_id", "stages", "executors"]

# kind -> (label, level). Lower levels come first in Spark's usual failure order and can cause higher ones.
KINDS = {
    "skew": ("task skew", 0), "tiny": ("too many tiny tasks", 0), "large": ("tasks too big", 0),
    "big_read": ("big table read", 0), "idle": ("executors idle", 0), "spill": ("disk spill", 1),
    "cache": ("DataFrame cache larger than memory", 1), "autoscale": ("autoscaling removed executors mid-work", 4),
    "gc": ("GC pressure", 2), "oom": ("out of memory", 3), "killed": ("executor killed by the OS", 3),
    "disk_full": ("disk full", 3), "lost": ("executor lost", 4), "fetch": ("shuffle fetch failure", 5),
    "task_error": ("error in task code", 5), "driver_error": ("error in driver code", 5), "other_error": ("error", 5),
    "retries": ("task retries", 6), "stage_failed": ("stage failed", 6), "job_aborted": ("job aborted", 7),
    "query_failed": ("query failed", 8), "reported": ("error reported to the notebook / job", 9),
}
CATEGORY_KIND = {
    "task_skew": "skew", "tiny_tasks": "tiny", "large_tasks": "large", "big_read": "big_read", "executors_idle": "idle",
    "disk_spill": "spill", "log:disk_spill": "spill",
    "gc_pressure": "gc", "log:gc_pressure": "gc", "jvm_full_gc": "gc", "gc_stuck": "gc", "executor_oom": "oom", "oom_site": "oom",
    "log:executor_oom": "oom", "executor_killed": "killed", "log:disk_full": "disk_full", "executor_lost": "lost",
    "log:executor_lost": "lost", "log:fetch_failure": "fetch", "log:python_error": "task_error",
    "task_retries": "retries", "stage_failed": "stage_failed", "query_failed": "query_failed",
    "dataframe_cache": "cache", "autoscale_removed": "autoscale",
}
# findings about the whole run or cluster (queueing, MERGE, counts, DDL loops, init scripts): no cause -> effect with
# a failure, so they stay outside incidents (the cache and autoscaling findings above do take part)
NOT_IN_INCIDENTS = {"capacity_bound", "autoscale_lag", "cores_full", "waited_for_cores", "merge_rewrite", "count_only",
                    "ddl_loop", "disk_cache", "init_script"}
AUTOSCALED_RE = re.compile(r"removed \d+ executors? \(([\w, ]+)\)")
PERF_KINDS = {"skew", "tiny", "large", "big_read", "idle", "spill", "gc"}
ERROR_KINDS = {"task_error", "driver_error", "other_error", "reported"}
FAILURE_KINDS = {"oom", "killed", "disk_full", "lost", "fetch", "task_error", "driver_error", "stage_failed",
                 "job_aborted", "query_failed", "reported"}

# which kinds can cause which: Spark's usual failure paths. An error in user code is a root: nothing above causes it.
CAUSES = {
    "spill": {"skew", "large", "cache"}, "gc": {"skew", "spill", "large", "cache"},
    "oom": {"skew", "spill", "gc", "large", "cache"},
    "killed": {"skew", "spill", "gc", "oom", "cache"}, "disk_full": {"spill"}, "lost": {"oom", "killed", "disk_full"},
    "fetch": {"oom", "killed", "lost", "disk_full", "autoscale"},
    "other_error": {"oom", "killed", "lost", "fetch", "disk_full", "autoscale"},
    "task_error": set(), "driver_error": set(),
    "retries": {"oom", "killed", "lost", "fetch", "task_error", "other_error", "disk_full", "autoscale"},
    "stage_failed": {"oom", "killed", "lost", "fetch", "task_error", "other_error", "disk_full", "autoscale"},
    "job_aborted": {"stage_failed", "task_error", "other_error"},
    "query_failed": {"stage_failed", "job_aborted", "task_error", "driver_error", "other_error"},
    "reported": {"query_failed", "job_aborted", "stage_failed", "driver_error"},
}
STAGE_RE = re.compile(r"\bstage (\d+)\.(\d+)", re.I)
EXEC_RE = re.compile(r"\bexecutor (\d+|driver)\b(?! exited caused by)", re.I)
LOST_EXEC_RE = re.compile(r"Lost executor (\d+)(?: on ([\w.\-]+))?", re.I)
# "connect to /10.0.0.1:4048", "connect to host-1.internal/10.0.0.1:4048", "from 10.0.0.1:4048"
FETCH_HOST_RE = re.compile(r"(?:connect to|from) (?:([\w.\-]+)/|/)?([\w.\-]+?):\d+", re.I)
CALL_RE = re.compile(r"calling o\d+\.(\w+)")
SEV_RANK = {"high": 0, "medium": 1, "low": 2, "info": 3}
SAME_TIME_MS = 120_000  # the same executor problem seen twice: an executor removal and its log line, seconds apart
ERR_SAME_MS = 2_000  # two errors on one executor this close together are one failure logged twice
LINK_GAP_MS = 30 * 60_000  # a cause linked only through an executor or host must be this close to its effect

BECAUSE = {
    ("skew", "spill"): "one partition is much bigger than the rest, so its task holds far more data than memory fits",
    ("skew", "oom"): "one partition is much bigger than the rest, so its task needed far more memory",
    ("skew", "gc"): "the oversized partition fills the heap, so the JVM keeps collecting",
    ("large", "spill"): "each task was given far more data than the ~128 MB a task is sized for, more than its memory holds",
    ("large", "gc"): "each task holds far more data than it is sized for, so the heap keeps filling",
    ("large", "oom"): "each task was given far more data than it is sized for, more than its memory holds",
    ("spill", "gc"): "memory was already full enough to spill; the same pressure shows up as GC time",
    ("spill", "oom"): "data did not fit in memory: spilling usually comes before running out of it",
    ("gc", "oom"): "the heap stayed full after collections, then ran out",
    ("gc", "killed"): "the executor's memory stayed full; the OS then killed the process",
    ("cache", "gc"): "the run cached more data than the executors' memory holds, so the heap stayed full",
    ("cache", "oom"): "the run cached more data than the executors' memory holds; building the cache ran out of memory",
    ("cache", "spill"): "the cache took the memory the tasks needed, so they spilled",
    ("cache", "killed"): "the run cached more data than the executors' memory holds",
    ("autoscale", "fetch"): "autoscaling removed executors that held shuffle files other tasks still needed",
    ("autoscale", "other_error"): "the error came from an executor while autoscaling removed it",
    ("autoscale", "retries"): "tasks on the executors autoscaling removed had to run again",
    ("autoscale", "stage_failed"): "the stage lost the shuffle files of the executors autoscaling removed",
    ("fetch", "retries"): "tasks that could not read their shuffle input were retried",
    ("other_error", "stage_failed"): "the error failed the stage's tasks until Spark gave up",
    ("oom", "lost"): "the executor ran out of memory, so its process was killed",
    ("killed", "lost"): "the OS killed the executor process",
    ("oom", "fetch"): "the executor that ran out of memory held shuffle files; once it died other tasks could not read them",
    ("killed", "fetch"): "the killed executor held shuffle files other tasks needed",
    ("lost", "fetch"): "the lost executor held shuffle files other tasks needed",
    ("lost", "retries"): "tasks running on the lost executor had to run again elsewhere",
    ("oom", "retries"): "tasks running on the executor that ran out of memory had to run again",
    ("fetch", "stage_failed"): "a stage fails as soon as its shuffle input cannot be read; Spark then re-runs the stage before it",
    ("task_error", "stage_failed"): "the same task kept failing (spark.task.maxFailures, 4 by default), so Spark gave up on the stage",
    ("task_error", "retries"): "the failing tasks were retried",
    ("oom", "stage_failed"): "tasks of this stage were running on the executor that ran out of memory",
    ("lost", "stage_failed"): "tasks of this stage were running on the executor that was lost",
    ("stage_failed", "job_aborted"): "a stage that fails for good aborts its job",
    ("stage_failed", "query_failed"): "the query's stage failed for good",
    ("job_aborted", "query_failed"): "the aborted job belongs to this query",
    ("driver_error", "query_failed"): "the error was raised in the driver while the query ran",
    ("query_failed", "reported"): "the query failure is what the notebook or job saw",
    ("job_aborted", "reported"): "the aborted job is what the notebook or job saw",
    ("driver_error", "reported"): "the driver's error is what the notebook or job saw",
}


def _exception_kind(f: Mapping, err_rows: list[dict]) -> str:
    cls = (f.get("entity") or "").lower()
    msg = " ".join(str(e.get("message") or "") for e in err_rows).lower()
    on_driver = bool(err_rows) and all(e.get("source") == "driver" and not e.get("executor_id") for e in err_rows)
    if "outofmemory" in cls:
        return "oom"
    # an IOException is a shuffle fetch only when its stack says so (a JDBC or storage connection is not)
    if "fetchfailed" in cls or ("ioexception" in cls and ("shuffleblockfetcher" in msg or "fetchfailed" in msg)):
        return "fetch"
    if "job aborted" in msg or "job aborted" in (f.get("evidence") or "").lower():
        return "job_aborted"
    if "py4jjavaerror" in cls:
        return "reported"
    if cls.startswith("pyspark.errors") and on_driver:
        return "reported"
    if any(e.get("user_frame") for e in err_rows) or "python" in cls or "error" in cls:
        # thrown only in the driver's log: the driver's own code, unless it is the driver's copy of a task error
        # (that copy merges with the executor's by message or frame and takes the task error's kind)
        return "driver_error" if on_driver else "task_error"
    return "other_error"


def _norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())[:120]


def _int(v):
    return None if is_null(v) else int(v)


def _str(v):
    return None if is_null(v) or v == "" else str(v)


class _Scope:
    __slots__ = ("stages", "inferred", "execs", "hosts", "fetch_hosts", "queries", "messages", "frames", "calls")

    def __init__(self):
        self.stages: set[tuple] = set()  # (ctx, stage_id, attempt)
        self.inferred: set[tuple] = set()  # the stages above known only from tasks running on the executor at the time
        self.execs: set[tuple] = set()  # (ctx or None, executor_id)
        self.hosts: set[str] = set()
        self.fetch_hosts: set[str] = set()
        self.queries: set[tuple] = set()  # (ctx, sql_execution_id)
        self.messages: set[str] = set()
        self.frames: set[str] = set()
        self.calls: set[str] = set()

    def update(self, other: "_Scope", attrs=None):
        for a in attrs or _Scope.__slots__:
            getattr(self, a).update(getattr(other, a))


def build_incidents(cid: str, findings: list[dict], stages: list[dict], executors: list[dict],
                    jobs: list[dict], queries: list[dict], log_signals: list[dict], log_errors: list[dict],
                    tasks: pd.DataFrame | None, task_retries: list[dict] | None = None,
                    apps: list[dict] | None = None) -> list[dict]:
    findings = [f for f in findings if f.get("category") not in NOT_IN_INCIDENTS]
    if not findings:
        return []
    ctxs = sorted({s["spark_context_id"] for s in stages if s.get("spark_context_id")} |
                  {e["spark_context_id"] for e in executors if e.get("spark_context_id")})
    app_ctx = {a["app_id"]: a["spark_context_id"] for a in apps or [] if a.get("app_id") and a.get("spark_context_id")}
    stage_by_key = {(s["spark_context_id"], s["stage_id"], s["stage_attempt"]): s for s in stages}
    stages_by_id: dict[int, list[dict]] = {}
    for s in stages:
        stages_by_id.setdefault(s["stage_id"], []).append(s)
    exec_host = {(e["spark_context_id"], str(e["executor_id"])): e.get("host") for e in executors}
    execs_by_id: dict[str, list[dict]] = {}
    for e in executors:
        execs_by_id.setdefault(str(e["executor_id"]), []).append(e)
    sig_rows: dict[str, list[dict]] = {}
    for r in log_signals:
        sig_rows.setdefault(r["signal"], []).append(r)
    err_rows: dict[str, list[dict]] = {}
    for r in log_errors:
        if r.get("fingerprint"):
            err_rows.setdefault(r["fingerprint"], []).append(r)
    # tasks per (ctx, executor): arrays for "what was running on executor X at time t"
    running: dict[tuple, tuple] = {}
    stage_execs: dict[tuple, set[str]] = {}
    if tasks is not None and not tasks.empty:
        t = tasks[["spark_context_id", "stage_id", "stage_attempt", "executor_id", "launch_time", "finish_time"]]
        t = t.dropna(subset=["executor_id", "launch_time"])
        for (ctx, ex), g in t.groupby(["spark_context_id", "executor_id"], sort=False):
            running[(ctx, str(ex))] = (g["launch_time"].astype("int64").to_numpy(),
                                       g["finish_time"].fillna(g["launch_time"]).astype("int64").to_numpy(),
                                       g["stage_id"].to_numpy(), g["stage_attempt"].to_numpy())
        for (ctx, sid, att), g in t.groupby(["spark_context_id", "stage_id", "stage_attempt"], sort=False):
            stage_execs[(ctx, int(sid), int(att))] = {str(x) for x in g["executor_id"].unique()}
    # the executor of each stage's slowest task, when an executor was stuck in GC: that task was not skewed
    stuck = {(_str(f.get("spark_context_id")), _str(f.get("executor_id"))) for f in findings if f["category"] == "gc_stuck"}
    slowest: dict[tuple, tuple] = {}
    if stuck and tasks is not None and not tasks.empty:
        d = t.assign(_ms=t["finish_time"].fillna(t["launch_time"]).astype("int64") - t["launch_time"].astype("int64"))
        for (ctx, sid, att), g in d.groupby(["spark_context_id", "stage_id", "stage_attempt"], sort=False):
            slowest[(ctx, int(sid), int(att))] = (ctx, str(g.loc[g["_ms"].idxmax(), "executor_id"]))
    # executors that really went away: out of memory, killed or lost (autoscaling and termination are planned)
    gone: dict[str, set] = {}
    for e in executors:
        if e.get("removal_category") in ("oom", "killed", "lost"):
            gone.setdefault(str(e["executor_id"]), set()).add(e["spark_context_id"])
    running_owners: dict[str, set] = {}
    for (c, ex) in running:
        running_owners.setdefault(ex, set()).add(c)
    stage_query = {k: (k[0], s["sql_execution_id"]) for k, s in stage_by_key.items() if s.get("sql_execution_id") is not None}

    def ctx_for(stage_id: int, ts) -> str | None:
        cands = stages_by_id.get(stage_id) or []
        if ts is not None:
            for s in cands:
                if s.get("start_time") is not None and s["start_time"] - 60_000 <= ts <= (s.get("end_time") or ts) + 60_000:
                    return s["spark_context_id"]
        if len({s["spark_context_id"] for s in cands}) == 1 and ts is None:
            return cands[0]["spark_context_id"]
        return ctxs[0] if len(ctxs) == 1 else None

    def exec_ctx(ex: str, ts) -> str | None:
        owners = running_owners.get(ex) or {e["spark_context_id"] for e in execs_by_id.get(ex, [])}
        if len(owners) == 1:
            return next(iter(owners))
        if ts is not None:
            for e in execs_by_id.get(ex, []):
                if (e.get("added_time") or 0) <= ts <= (e.get("removed_time") or ts):
                    return e["spark_context_id"]
        return ctxs[0] if len(ctxs) == 1 else None

    def stages_running(ctx, ex, ts) -> set[tuple]:
        r = running.get((ctx, ex))
        if r is None or ts is None:
            return set()
        ls, fs, sid, att = r
        m = (ls <= ts + 2_000) & (fs >= ts - 2_000)
        return {(ctx, int(a), int(b)) for a, b in zip(sid[m], att[m])}

    def scan_text(sc: _Scope, text: str, ts, ctx_hint, from_exec: str | None = None):
        for m in STAGE_RE.finditer(text):
            sid, att = int(m.group(1)), int(m.group(2))
            c = ctx_hint or ctx_for(sid, ts)
            if c is not None:
                sc.stages.add((c, sid, att))
        for m in LOST_EXEC_RE.finditer(text):
            sc.execs.add((ctx_hint or exec_ctx(m.group(1), ts), m.group(1)))
            if m.group(2):
                sc.hosts.add(m.group(2))
        for m in EXEC_RE.finditer(text):
            if m.group(1) != "driver":
                sc.execs.add((ctx_hint or exec_ctx(m.group(1), ts), m.group(1)))
        for m in FETCH_HOST_RE.finditer(text):
            sc.fetch_hosts.add(m.group(2))
            if m.group(1):
                sc.fetch_hosts.add(m.group(1))
        for m in CALL_RE.finditer(text):
            sc.calls.add(m.group(1).lower())
        if from_exec:
            sc.execs.add((ctx_hint or exec_ctx(from_exec, ts), from_exec))

    def add_running(sc: _Scope, stamps):
        for (c, ex) in list(sc.execs):
            for s in stamps:
                got = stages_running(c, ex, s) - sc.stages
                sc.stages |= got
                sc.inferred |= got

    def finish_scope(sc: _Scope):
        for (c, ex) in sc.execs:
            h = exec_host.get((c, ex))
            if h:
                sc.hosts.add(h)
        for k in sc.stages:
            if k in stage_query:
                sc.queries.add(stage_query[k])

    # ---- 1. kind and scope per finding ---------------------------------------------------------------------
    infos = []
    for f in findings:
        sc = _Scope()
        ctx = _str(f.get("spark_context_id"))
        ts = None if is_null(f.get("ts")) else f.get("ts")
        stage_id, attempt, exec_id, exec_q = (_int(f.get("stage_id")), _int(f.get("stage_attempt")),
                                              _str(f.get("executor_id")), _int(f.get("sql_execution_id")))
        errs = err_rows.get(f.get("fingerprint") or "", [])
        if f["category"] == "exception":
            kind = _exception_kind(f, errs)
        else:
            kind = CATEGORY_KIND.get(f["category"]) or "other_error"
        if stage_id is not None:
            sc.stages.add((ctx or ctx_for(stage_id, ts), stage_id, attempt or 0))
        if exec_id:
            sc.execs.add((ctx or exec_ctx(exec_id, ts), exec_id))
        if exec_q is not None and ctx:
            sc.queries.add((ctx, exec_q))
            if kind == "query_failed":
                sc.stages |= {k for k, q in stage_query.items() if q == (ctx, exec_q)}
        scan_text(sc, f.get("evidence") or "", ts, ctx)
        if kind == "autoscale":
            m = AUTOSCALED_RE.search(f.get("evidence") or "")
            for ex in (m.group(1).replace(" ", "").split(",") if m else []):
                sc.execs.add((ctx or exec_ctx(ex, ts), ex))
        # a log signal stands for every line of that signal: one event per line, so it can be matched line by line
        events = []
        for r in sig_rows.get(f.get("signal") or "", []) if f.get("signal") else []:
            ev = _Scope()
            scan_text(ev, r.get("line") or "", r.get("ts"), app_ctx.get(r.get("app_id")), _str(r.get("executor_id")))
            if kind not in PERF_KINDS:
                add_running(ev, [r.get("ts")])
            finish_scope(ev)
            events.append((r.get("ts"), ev))
            sc.update(ev)
        for r in errs:
            scan_text(sc, r.get("message") or "", r.get("ts"), app_ctx.get(r.get("app_id")), _str(r.get("executor_id")))
            sc.messages.add(_norm(r.get("message")))
            if r.get("user_frame"):
                sc.frames.add(_norm(r["user_frame"]))
        if f["category"] in ("stage_failed", "query_failed"):
            sc.messages.add(_norm(f.get("evidence")))
        sc.messages.discard("")
        # tasks running on the executor when it happened (exceptions and executor removals)
        if kind not in PERF_KINDS and kind not in ("stage_failed", "query_failed", "retries"):
            add_running(sc, [r.get("ts") for r in errs] or [ts])
        finish_scope(sc)
        label = KINDS[kind][0]
        if kind == "other_error":  # unmapped signal or exception: say what it is rather than just "error"
            if f["category"].startswith("log:"):
                label = f["category"][4:].replace("_", " ")
            elif f.get("entity"):
                label = f"error: {str(f['entity']).split('.')[-1]}"
        infos.append({"f": f, "kind": kind, "label": label, "sc": sc, "level": KINDS[kind][1], "ts": ts,
                      "events": events or [(ts, sc)], "multi": f["category"].startswith("log:") and len(events) > 1})

    # ---- 2. merge the same problem seen several ways ---------------------------------------------------------
    parent = list(range(len(infos)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    def shared_exec(a: _Scope, b: _Scope) -> bool:
        return any(x[1] == y[1] and (x[0] is None or y[0] is None or x[0] == y[0]) for x in a.execs for y in b.execs)

    def close(ta, tb) -> bool:
        return ta is None or tb is None or abs(ta - tb) <= SAME_TIME_MS

    def same_problem(a: dict, b: dict) -> bool:
        sa, sb = a["sc"], b["sc"]
        same_text = bool(sa.messages & sb.messages) or bool(sa.frames & sb.frames)
        ka, kb = a["kind"], b["kind"]
        if ka == kb and ka not in ("stage_failed", "query_failed", "retries"):
            if ka in PERF_KINDS:
                return bool(sa.stages & sb.stages)
            if ka in ERROR_KINDS:  # two different errors on one executor are two problems ...
                if same_text or (sa.stages & sb.stages) - (sa.inferred & sb.inferred):
                    return True
                # ... unless logged together: a Python traceback and the error inside it, the same second
                return any(ea.execs & eb.execs and ta is not None and tb is not None and abs(ta - tb) <= ERR_SAME_MS
                           for ta, ea in a["events"] for tb, eb in b["events"])
            # the same message on two executors is two problems ("Java heap space" on each of them)
            if same_text and (not sa.execs or not sb.execs or shared_exec(sa, sb)):
                return True
            if (sa.stages - sa.inferred) & (sb.stages - sb.inferred):
                return True
            # the same executor problem seen as a removal, a log line and an exception: same executor, same moment
            return any(ea.execs & eb.execs and close(ta, tb) for ta, ea in a["events"] for tb, eb in b["events"])
        if {ka, kb} <= ERROR_KINDS:
            # the executor's error and the driver's copy of it, or a wrapper and its cause logged together
            return same_text or any(ea.execs & eb.execs and ta is not None and tb is not None and abs(ta - tb) <= ERR_SAME_MS
                                    for ta, ea in a["events"] for tb, eb in b["events"])
        if {ka, kb} == {"stage_failed", "job_aborted"} and same_text:
            return True  # "Job aborted due to stage failure" is the stage failure, logged by the driver
        return False

    # candidate pairs from shared keys only (not every pair: thousands of stage findings on a long-running cluster)
    def keys(i: int, info: dict):
        sc = info["sc"]
        yield from (("st", k) for k in sc.stages)
        yield from (("ex", info["kind"], e) for e in sc.execs)
        yield from (("msg", m) for m in sc.messages)
        yield from (("fr", m) for m in sc.frames)

    single = [i for i, x in enumerate(infos) if not x["multi"]]
    buckets: dict[tuple, list[int]] = {}
    for i in single:
        for k in set(keys(i, infos[i])):
            buckets.setdefault(k, []).append(i)
    tried: set[tuple] = set()
    for members in buckets.values():
        for x in range(len(members)):
            for y in range(x + 1, len(members)):
                i, j = members[x], members[y]
                if (i, j) in tried or find(i) == find(j):
                    continue
                tried.add((i, j))
                if same_problem(infos[i], infos[j]):
                    union(i, j)
    # a cluster-wide log-signal finding joins the one problem it matches best (most matching lines, then earliest):
    # joining every match would chain unrelated failures together through it
    for i, x in enumerate(infos):
        if not x["multi"]:
            continue
        score: dict[int, int] = {}
        for k in set(keys(i, x)):
            for j in buckets.get(k, []):
                if same_problem(x, infos[j]):
                    score[find(j)] = score.get(find(j), 0) + 1
        if score:
            best = max(score, key=lambda r: (score[r], -(infos[r]["ts"] or 0)))
            union(i, best)
    groups: dict[int, list[int]] = {}
    for i in range(len(infos)):
        groups.setdefault(find(i), []).append(i)

    problems = []
    for members in groups.values():
        ms = [infos[i] for i in members]
        level = min(m["level"] for m in ms)
        # the problem's kind: the earliest in the failure order, preferring task errors over driver copies and reports
        km = min(ms, key=lambda m: (m["level"], m["kind"] == "reported", m["kind"] == "driver_error"))
        sc = _Scope()
        for m in ms:
            if not m["multi"]:
                sc.update(m["sc"])
                continue
            got = False
            if len(ms) > 1:  # a merged log signal adds only the lines that match this problem
                for ts_, ev in m["events"]:
                    if any(same_problem({**m, "events": [(ts_, ev)], "sc": ev}, o) for o in ms if o is not m):
                        sc.update(ev)
                        got = True
            if not got:
                # alone (or no line matched): only its earliest lines, so it never bridges problems cluster-wide
                stamps = [t_ for t_, _ in m["events"] if t_ is not None]
                if not stamps:
                    sc.update(m["events"][0][1])
                for ts_, ev in m["events"] if stamps else []:
                    if ts_ is not None and ts_ - min(stamps) <= ERR_SAME_MS:
                        sc.update(ev)
        tss = [m["ts"] for m in ms if m["ts"] is not None]
        # the finding that tells it best: user code first, then event-log facts, then exceptions, then log signals
        lead = min(ms, key=lambda m: (not m["sc"].frames or m["kind"] == "reported", SEV_RANK.get(m["f"]["severity"], 9),
                                      m["f"]["category"].startswith("log:"), m["f"]["category"] == "exception",
                                      m["f"]["finding_id"]))
        problems.append({"members": members, "kind": km["kind"], "label": km["label"], "level": level, "sc": sc,
                         "ts": min(tss) if tss else None, "end": max(tss) if tss else None, "lead": lead,
                         "sev": min(SEV_RANK.get(m["f"]["severity"], 9) for m in ms),
                         "runs": {m["f"].get("run_key") for m in ms} - {None}})

    # ---- 3. cause -> effect between problems ---------------------------------------------------------------
    def link_score(c: dict, e: dict) -> int:
        if c["level"] >= e["level"] or c["kind"] not in CAUSES.get(e["kind"], ()):
            return 0
        if c["ts"] is not None and e["ts"] is not None and c["ts"] > e["ts"] + 5_000:
            return 0
        a, b = c["sc"], e["sc"]
        # a cache bigger than memory fills the heap of every executor of its run, from when it was built
        if c["kind"] == "cache":
            return 2 if c["runs"] & e["runs"] else 0
        # GC on one executor does not make another run out of memory
        if c["kind"] == "gc" and e["kind"] in ("oom", "killed") and a.execs and b.execs and not shared_exec(a, b):
            return 0
        # a slow task stuck in GC on its executor was not skewed: the GC came first, not the other way round
        if c["kind"] == "skew" and e["kind"] == "gc" and any(slowest.get(k) in stuck for k in a.stages):
            return 0
        # a fetch failure names where the missing shuffle data was: only a problem on that host caused it (an
        # executor that died elsewhere, earlier, did not)
        if e["kind"] == "fetch" and b.fetch_hosts:
            near_ = c["end"] is None or e["ts"] is None or e["ts"] - c["end"] <= LINK_GAP_MS
            return 3 if (a.hosts & b.fetch_hosts) and near_ else 0
        shared = a.stages & b.stages
        if shared - (a.inferred & b.inferred):  # strong unless both sides only guessed the stage from running tasks
            return 3
        near = c["end"] is None or e["ts"] is None or e["ts"] - c["end"] <= LINK_GAP_MS
        if b.fetch_hosts and (a.hosts & b.fetch_hosts) and near:
            return 3
        if shared:
            return 2
        if not near:
            return 0
        # a dying executor hurts every stage it ran tasks for; memory pressure only its own executor or stage
        if c["kind"] in ("oom", "killed", "lost", "disk_full") and b.stages and any(
                ex in stage_execs.get(k, ()) for (_, ex) in a.execs for k in b.stages):
            return 2
        if a.execs & b.execs:
            return 2
        if e["kind"] == "reported" and c["kind"] in ("query_failed", "job_aborted") and b.calls:
            names = " ".join((stage_by_key.get(k, {}).get("stage_name") or "") for k in a.stages).lower()
            if any(cl in names for cl in b.calls):
                return 2
        return 0

    # candidate causes: problems sharing a stage, executor or host with the effect, plus the few failed queries/jobs
    by_stage: dict[tuple, list[dict]] = {}
    by_exec: dict[str, list[dict]] = {}
    by_host: dict[str, list[dict]] = {}
    ends = [p for p in problems if p["kind"] in ("query_failed", "job_aborted", "stage_failed", "driver_error")]
    caches = [p for p in problems if p["kind"] == "cache"]
    for p in problems:
        for k in p["sc"].stages:
            by_stage.setdefault(k, []).append(p)
        for _, ex in p["sc"].execs:
            by_exec.setdefault(ex, []).append(p)
        for h in p["sc"].hosts:
            by_host.setdefault(h, []).append(p)
    for e in problems:
        cands: dict[int, dict] = {}
        for k in e["sc"].stages:
            for c in by_stage.get(k, []):
                cands[id(c)] = c
            for ex in stage_execs.get(k, ()):
                for c in by_exec.get(ex, []):
                    cands[id(c)] = c
        for _, ex in e["sc"].execs:
            for c in by_exec.get(ex, []):
                cands[id(c)] = c
        for h in e["sc"].fetch_hosts:
            for c in by_host.get(h, []):
                cands[id(c)] = c
        if e["kind"] in ("reported", "query_failed"):
            for c in ends:
                cands[id(c)] = c
        if e["kind"] in ("gc", "oom", "spill", "killed"):
            for c in caches:
                cands[id(c)] = c
        best = None
        for c in cands.values():
            if c is e:
                continue
            s = link_score(c, e)
            if s == 0:
                continue
            # strongest evidence, then the nearest step in the failure order, then user-code errors, then latest
            key = (s, c["level"], c["kind"] == "task_error" and bool(c["sc"].frames), c["ts"] or 0)
            if best is None or key > best[0]:
                best = (key, c)
        e["cause"] = best[1] if best else None
        e["score"] = best[0][0] if best else 0
    # an error reported to the notebook with no other link: the last failed query or job before it
    for e in problems:
        if e["kind"] == "reported" and e["cause"] is None:
            fails = [c for c in problems if c["kind"] in ("query_failed", "job_aborted")
                     and (e["ts"] is None or c["ts"] is None or c["ts"] <= e["ts"] + 5_000)]
            if fails:
                e["cause"] = max(fails, key=lambda c: (c["ts"] or 0, c["level"]))
                e["weak"] = True
                e["score"] = 1

    # ---- 4. incidents: connected problems, plus one query's performance problems ---------------------------
    idx = {id(p): i for i, p in enumerate(problems)}
    pp = list(range(len(problems)))

    def pfind(i):
        while pp[i] != i:
            pp[i] = pp[pp[i]]
            i = pp[i]
        return i

    for i, p in enumerate(problems):
        if p.get("cause") is not None:
            a, b = pfind(i), pfind(idx[id(p["cause"])])
            if a != b:
                pp[max(a, b)] = min(a, b)
    causes = {id(p["cause"]) for p in problems if p.get("cause") is not None}
    # performance problems with no causal link: group by query
    by_query: dict[tuple, int] = {}
    for i, p in enumerate(problems):
        linked = p.get("cause") is not None or id(p) in causes
        if p["kind"] in PERF_KINDS and not linked and len(p["sc"].queries) == 1:
            q = next(iter(p["sc"].queries))
            if q in by_query:
                a, b = pfind(i), pfind(by_query[q])
                if a != b:
                    pp[max(a, b)] = min(a, b)
            else:
                by_query[q] = i
    comps: dict[int, list[dict]] = {}
    for i, p in enumerate(problems):
        comps.setdefault(pfind(i), []).append(p)

    q_desc = {(q["spark_context_id"], q["sql_execution_id"]): q.get("description") for q in queries}
    retries_wasted: dict[tuple, int] = {}
    for r in task_retries or []:
        k = (r["spark_context_id"], r["stage_id"], r["stage_attempt"])
        retries_wasted[k] = retries_wasted.get(k, 0) + int(r.get("wasted_ms") or 0)

    incidents = []
    for ps in comps.values():
        top = max(ps, key=lambda p: (p["level"] if p["kind"] != "reported" else 7.5, -(p["ts"] or 0)))
        # root: follow causes down from the top problem
        root, seen = top, set()
        while root.get("cause") is not None and id(root) not in seen:
            seen.add(id(root))
            root = root["cause"]
        if all(p.get("cause") is None for p in ps):  # unlinked (one query's performance problems): worst first
            root = min(ps, key=lambda p: (p["sev"], p["level"], p["ts"] or 0))
        path, cur = [], top
        while cur is not None and id(cur) not in {id(x) for x in path}:
            path.append(cur)
            cur = cur.get("cause")
        sc = _Scope()
        for p in ps:
            sc.update(p["sc"], ("stages", "execs", "queries"))
        # what this incident cost: the stages its failures touched (not every stage of a failed query)
        cost_stages = set()
        for p in ps:
            if p["kind"] not in ("query_failed", "reported"):
                cost_stages |= p["sc"].stages - p["sc"].inferred
        real = [p for p in ps if p["kind"] != "reported"]
        incidents.append({"ps": ps, "top": top, "root": root, "path": path, "sc": sc, "cost_stages": cost_stages,
                          "sev": min(p["sev"] for p in ps),
                          "failed": any(p["kind"] in FAILURE_KINDS for p in ps),
                          # an incident that is only the notebook's report of an error ranks below the real failures
                          "only_reported": not real,
                          "level": max((p["level"] for p in real), default=0),
                          "ts": min((p["ts"] for p in ps if p["ts"] is not None), default=None),
                          "end": max((p["end"] for p in ps if p["end"] is not None), default=None)})
    incidents.sort(key=lambda x: (not x["failed"], x["only_reported"], x["sev"], -x["level"], x["ts"] or 0))

    def query_of(p: dict, inc: dict):
        qs = sorted(p["sc"].queries) or sorted(inc["sc"].queries)
        return qs[-1] if qs else None

    def title(inc) -> str:
        top = inc["top"]
        kinds = []
        for p in sorted(inc["ps"], key=lambda p: p["level"]):
            if p["label"] not in kinds:
                kinds.append(p["label"])
        if top["kind"] in ("query_failed", "reported", "job_aborted"):
            q = query_of(top, inc)
            if q:
                d = (q_desc.get(q) or "")[:60]
                return f"Query {q[1]} failed" + (f": {d}" if d else "")
        if top["kind"] == "stage_failed" and top["sc"].stages:
            st = ", ".join(f"{k[1]}.{k[2]}" for k in sorted(top["sc"].stages, key=lambda k: (k[1], k[2])))
            q = sorted(top["sc"].queries)
            return f"Stage {st} failed" + (f" (query {q[-1][1]})" if len(q) == 1 else "")
        if top["kind"] in ("lost", "oom", "killed") and top["sc"].execs:
            ex = ", ".join(sorted({e for _, e in top["sc"].execs}))
            return f"Executor {ex}: {top['label']}"
        if all(p["kind"] in PERF_KINDS for p in inc["ps"]) and len(inc["sc"].queries) == 1:
            q = next(iter(inc["sc"].queries))
            return f"Query {q[1]} ran slow: {', '.join(kinds)}"
        if top["sc"].stages:
            sid = sorted({k[1] for k in top["sc"].stages})
            return f"Stage {', '.join(map(str, sid[:3]))}: {', '.join(kinds)}"
        if top["kind"] in ERROR_KINDS:
            ent = top["lead"]["f"].get("entity")
            lbl = top["label"]
            return lbl[0].upper() + lbl[1:] + (f": {ent}" if ent and ent not in lbl else "")
        if inc["sc"].execs:
            ex = ", ".join(sorted({e for _, e in inc["sc"].execs})[:3])
            return f"Executor {ex}: {', '.join(kinds)}"
        return kinds[-1][0].upper() + kinds[-1][1:] if kinds else "Problem"

    claimed: set[tuple] = set()  # stage attempts already counted by a higher-ranked incident

    def impact(inc) -> str:
        own = inc["cost_stages"] - claimed
        claimed.update(own)
        st = [stage_by_key[k] for k in own if k in stage_by_key]
        failed_stages = sum(1 for s in st if s.get("status") == "failed")
        failed_tasks = sum(int(s.get("failed_tasks") or 0) for s in st)
        # counted from the executor removals: out of memory, killed or lost (not autoscaled or stopped with the cluster)
        lost = {(c if c in gone[ex] else min(gone[ex]), ex) for p in inc["ps"] if p["kind"] in ("oom", "killed", "lost")
                for c, ex in p["sc"].execs if ex in gone and (c is None or c in gone[ex])}
        failed_q = {q for p in inc["ps"] if p["kind"] == "query_failed" for q in p["sc"].queries}
        wasted = sum(retries_wasted.get(k, 0) for k in own)
        parts = []
        if failed_q:
            parts.append(f"{len(failed_q)} quer{'y' if len(failed_q) == 1 else 'ies'} failed")
        if failed_stages:
            parts.append(f"{failed_stages} stage attempt{'s' if failed_stages != 1 else ''} failed")
        if failed_tasks:
            parts.append(f"{failed_tasks} task attempt{'s' if failed_tasks != 1 else ''} failed")
        if lost:
            parts.append(f"{len(lost)} executor{'s' if len(lost) != 1 else ''} lost")
        if wasted:
            parts.append(f"{fmt_words(wasted)} of task work thrown away")
        if not parts and st:
            slow = sum(int(s.get("duration_ms") or 0) for s in st)
            if slow:
                parts.append(f"{len(st)} stage{'s' if len(st) != 1 else ''}, {fmt_words(slow)} of stage time")
        return "; ".join(parts)

    out = []
    for n, inc in enumerate(incidents, 1):
        iid = f"I{n}"
        sev = next((k for k, v in SEV_RANK.items() if v == inc["sev"]), "medium")
        ttl, imp = title(inc), impact(inc)
        core = set()  # the stages / query on the causal path: what an ambiguous row should point at
        for p in inc["path"]:
            core |= p["sc"].stages - p["sc"].inferred
        tq = query_of(inc["top"], inc)
        pnum = {id(p): f"{iid}.{k}" for k, p in enumerate(sorted(inc["ps"], key=lambda p: (p["level"], p["ts"] or 0)), 1)}
        for p in inc["ps"]:
            cause = p.get("cause")
            if p is inc["root"]:
                role = "root"
            elif cause is not None:
                role = "effect"
            else:  # no cause of its own: another start point in the same incident
                role = "contributing" if p["kind"] not in PERF_KINDS or not inc["failed"] else "related"
            because = None
            if cause is not None:
                because = BECAUSE.get((cause["kind"], p["kind"]))
                if because is None:
                    shared = ("the same stage" if cause["sc"].stages & p["sc"].stages else
                              "the same executor" if cause["sc"].execs & p["sc"].execs else "the same query")
                    because = f"{cause['label']} came first on {shared}"
                if p.get("weak"):
                    because += " (matched by time only)"
            for i in sorted(p["members"], key=lambda i: infos[i]["f"]["finding_id"]):
                info = infos[i]
                f = info["f"]
                sc = info["sc"] if not info["multi"] else p["sc"]
                st = sorted(sc.stages, key=lambda k: (k[1], k[2]))
                exs = sorted({e for _, e in sc.execs}, key=lambda e: (len(e), e))
                # one stage / query for the row: its own when it has one, else the one on the incident's path
                prim = st[0] if len(st) == 1 else next((k for k in reversed(st) if k in core), None)
                s_row = stage_by_key.get(prim) if prim else None
                qs = sorted(sc.queries)
                q = qs[0] if len(qs) == 1 else (tq if tq in qs else None)
                f_stage = _int(f.get("stage_id"))
                out.append({
                    "cluster_id": cid, "finding_id": f["finding_id"], "incident_id": iid, "incident_rank": n,
                    "incident_title": ttl, "incident_severity": sev, "incident_impact": imp or None,
                    "incident_start": inc["ts"], "incident_end": inc["end"], "problem_id": pnum[id(p)],
                    "kind": p["label"], "role": role if info is p["lead"] else "same",
                    "caused_by": (cause["lead"]["f"]["finding_id"] if cause is not None else None)
                    if info is p["lead"] else p["lead"]["f"]["finding_id"],
                    "because": because if info is p["lead"] else "the same problem, seen another way",
                    # how sure the link to caused_by is: same stage or host (strong), same executor or a stage only
                    # inferred from running tasks (likely), time only (weak)
                    "confidence": ({3: "strong", 2: "likely", 1: "weak"}.get(p.get("score", 0)) if cause is not None
                                   else None) if info is p["lead"] else "strong",
                    "spark_context_id": _str(f.get("spark_context_id")) or (prim[0] if prim else None)
                    or (next(iter(sc.execs))[0] if sc.execs else None),
                    "stage_id": f_stage if f_stage is not None else (prim[1] if prim else None),
                    "stage_attempt": _int(f.get("stage_attempt")) if f_stage is not None else (prim[2] if prim else None),
                    "spark_job_id": _int(f.get("spark_job_id")) if _int(f.get("spark_job_id")) is not None
                    else (s_row.get("spark_job_id") if s_row else None),
                    "sql_execution_id": _int(f.get("sql_execution_id")) if _int(f.get("sql_execution_id")) is not None
                    else (q[1] if q else None),
                    "executor_id": _str(f.get("executor_id")) or (exs[0] if len(exs) == 1 else None),
                    "stages": ", ".join(f"{a}.{b}" for _, a, b in st) or None,
                    "executors": ", ".join(exs) or None,
                })
    order = {f["finding_id"]: i for i, f in enumerate(findings)}
    out.sort(key=lambda r: (r["incident_rank"], order.get(r["finding_id"], 0)))
    return out
