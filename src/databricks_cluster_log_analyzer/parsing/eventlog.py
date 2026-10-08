"""Spark event log parsing. Streams lines, filters by event name as a string BEFORE json.loads.

Ported from notebook part 3 (V1-V6), plus StageSubmitted / ApplicationStart / ApplicationEnd / LogStart,
EnvironmentUpdate (cluster_info) and the Spark Connect operation / session events (connect_operations).
Field names vary across Spark / DBR versions: everything is read with `.get` and a missing key never fails.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

from ..config import Rules
from ..readers import open_text_lines
from .oom import task_oom_site
from .sqlplan import add_driver_accums, add_stage_accums, add_task_accums, walk_plan

SQL_UI = "org.apache.spark.sql.execution.ui."
CONNECT = "org.apache.spark.sql.connect.service."
EV_TASK_END = "SparkListenerTaskEnd"
EV_STAGE_SUBMITTED = "SparkListenerStageSubmitted"
EV_STAGE_COMPLETED = "SparkListenerStageCompleted"
EV_JOB_START = "SparkListenerJobStart"
EV_JOB_END = "SparkListenerJobEnd"
EV_EXEC_ADDED = "SparkListenerExecutorAdded"
EV_EXEC_REMOVED = "SparkListenerExecutorRemoved"
EV_SQL_START = SQL_UI + "SparkListenerSQLExecutionStart"
EV_SQL_END = SQL_UI + "SparkListenerSQLExecutionEnd"
EV_SQL_AQE = SQL_UI + "SparkListenerSQLAdaptiveExecutionUpdate"
EV_DRIVER_ACCUM = SQL_UI + "SparkListenerDriverAccumUpdates"
# metrics of operators adaptive execution added after the plan was logged (their values come as accumulators)
EV_SQL_AQE_METRICS = SQL_UI + "SparkListenerSQLAdaptiveSQLMetricUpdates"
EV_APP_START = "SparkListenerApplicationStart"
EV_APP_END = "SparkListenerApplicationEnd"
EV_LOG_START = "SparkListenerLogStart"
# Databricks writes this first: it carries "Spark Version" when SparkListenerLogStart is missing
EV_DBC_META = "DBCEventLoggingListenerMetadata"
EV_ENV_UPDATE = "SparkListenerEnvironmentUpdate"
EV_BM_ADDED = "SparkListenerBlockManagerAdded"
EV_RP_ADDED = "SparkListenerResourceProfileAdded"
# Spark properties that say what each executor was given (memory, overhead, off-heap, task cpus)
EXEC_CONF_KEYS = ("spark.executor.memory", "spark.executor.memoryOverhead", "spark.executor.cores",
                  "spark.memory.offHeap.enabled", "spark.memory.offHeap.size", "spark.memory.fraction",
                  "spark.memory.storageFraction", "spark.task.cpus")
from ..settings_catalog import SETTING_KEYS  # noqa: E402

CONNECT_OP_PREFIX = "SparkListenerConnectOperation"
CONNECT_SESSION_PREFIX = "SparkListenerConnectSession"

TASK_COLS = ["cluster_id", "spark_context_id", "stage_id", "stage_attempt", "task_id", "task_attempt", "executor_id",
             "host", "launch_time", "finish_time", "task_ms", "run_ms", "gc_ms", "peak_mem", "mem_spill",
             "disk_spill", "input_bytes", "input_records", "output_bytes", "shuffle_read", "shuffle_write",
             "failed", "end_reason", "error", "task_index", "speculative", "cpu_ms", "fetch_wait_ms",
             "shuffle_read_records", "shuffle_write_records", "output_records", "shuffle_write_ms", "oom_site"]

# spark.databricks.clusterUsageTags.* (and job properties) -> cluster_info columns; first key found wins
TAGS = "spark.databricks.clusterUsageTags."
CLUSTER_INFO_KEYS: dict[str, tuple[str, ...]] = {
    "cluster_name": (TAGS + "clusterName",),
    "cluster_creator": (TAGS + "clusterCreator",),
    "spark_version": (TAGS + "effectiveSparkVersion", TAGS + "sparkVersion"),
    "driver_node_type": (TAGS + "driverNodeType",),
    "worker_node_type": (TAGS + "clusterNodeType", TAGS + "workerNodeType"),
    "min_workers": (TAGS + "clusterMinWorkers",),
    "max_workers": (TAGS + "clusterMaxWorkers",),
    "target_workers": (TAGS + "clusterTargetWorkers", TAGS + "clusterWorkers"),
    "cluster_scaling_type": (TAGS + "clusterScalingType",),
    "runtime_engine": (TAGS + "runtimeEngine",),
    "cloud_provider": (TAGS + "cloudProvider",),
    "region": (TAGS + "region", TAGS + "dataPlaneRegion", TAGS + "clusterRegion"),
    "workload_type": (TAGS + "clusterWorkloadType", TAGS + "workloadType", TAGS + "clusterSource"),
    "databricks_job_id": (TAGS + "jobId", "spark.databricks.job.id"),
    "job_run_id": ("spark.databricks.job.runId", TAGS + "jobRunId", TAGS + "idInJob"),
    "task_run_id": ("spark.databricks.job.taskRunId",),
    "parent_run_id": ("spark.databricks.job.parentRunId", TAGS + "parentRunId"),
}
CLUSTER_INFO_INT = ("min_workers", "max_workers", "target_workers")


class EventStats:
    def __init__(self):
        self.lines = 0
        self.matched = 0
        self.malformed = 0
        self.counts: dict[str, int] = {}  # every event type seen (read or not)


_EVENT_RE_CACHE: dict[frozenset, re.Pattern] = {}
# lenient about whitespace around the colon (real files have none, older ones a space)
ANY_EVENT_RE = re.compile(r'"Event"\s*:\s*"([^"]+)"')


def event_regex(wanted: Iterable[str]) -> re.Pattern:
    key = frozenset(wanted)
    rx = _EVENT_RE_CACHE.get(key)
    if rx is None:
        # notebook EVENT_RE, lenient about whitespace around the colon
        rx = re.compile(r'"Event"\s*:\s*"(' + "|".join(re.escape(e) for e in sorted(key, key=len, reverse=True))
                        + ')"')
        _EVENT_RE_CACHE[key] = rx
    return rx


def iter_events(lines: Iterable[str], wanted: set[str] | Iterable[str], stats: EventStats | None = None
                ) -> Iterator[tuple[str, dict]]:
    """(event_name, parsed dict) for each line whose "Event" is in `wanted`.

    The event name is extracted with a regex on the raw text first and checked against `wanted`; only matching
    lines are json-parsed. Every event type is counted in `stats.counts`. Malformed JSON lines are counted in
    `stats.malformed` and skipped."""
    wanted = wanted if isinstance(wanted, (set, frozenset)) else set(wanted)
    st = stats or EventStats()
    counts = st.counts
    search = ANY_EVENT_RE.search
    for line in lines:
        st.lines += 1
        i = line.find('"Event"')
        if i < 0:
            continue
        m = search(line, i)
        if m is None:
            continue
        name = m.group(1)
        counts[name] = counts.get(name, 0) + 1
        if name not in wanted:
            # a class-qualified name ("com.databricks....DBCEventLoggingListenerMetadata") matches by its last part
            short = name.rsplit(".", 1)[-1]
            if short == name or short not in wanted:
                continue
            name = short
        try:
            obj = json.loads(line)
        except ValueError:
            st.malformed += 1
            continue
        if not isinstance(obj, dict):
            st.malformed += 1
            continue
        st.matched += 1
        ev = obj.get("Event")
        yield (name if not ev or ev.rsplit(".", 1)[-1] == name else ev), obj


def _i(v):
    """int or None (JSON numbers can come as strings in some properties)."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


def _ts(v):
    v = _i(v)
    return v if v else None  # 0 / -1 mean "not set" in the event log


def _s(v, n: int | None = None):
    if v is None:
        return None
    s = v if isinstance(v, str) else str(v)
    return s[:n] if n else s


def _props(v) -> dict:
    """Properties as a dict ({k: v} or [[k, v], ...] depending on the Spark version)."""
    if isinstance(v, dict):
        return v
    if isinstance(v, list):
        out = {}
        for kv in v:
            if isinstance(kv, (list, tuple)) and len(kv) == 2:
                out[str(kv[0])] = kv[1]
        return out
    return {}


def parse_removed_reason(raw) -> tuple[str | None, str | None]:
    """(reason, raw). `Removed Reason` can be a JSON string like {"cause":"kill request from HTTP endpoint",...}:
    its `cause` (else `reason` / `message`) becomes the reason; the raw text is kept."""
    if raw is None:
        return None, None
    if not isinstance(raw, str):
        try:
            raw = json.dumps(raw)
        except (TypeError, ValueError):
            raw = str(raw)
    s = raw.strip()
    if s.startswith("{"):
        try:
            obj = json.loads(s)
        except ValueError:
            return raw, raw
        if isinstance(obj, dict):
            for k in ("cause", "reason", "message", "Cause", "Reason"):
                v = obj.get(k)
                if v not in (None, ""):
                    return str(v), raw
    return raw, raw


def cluster_info_from_props(props: dict) -> dict:
    out = {}
    for col, keys in CLUSTER_INFO_KEYS.items():
        for k in keys:
            v = props.get(k)
            if v not in (None, ""):
                out[col] = _i(v) if col in CLUSTER_INFO_INT else str(v)
                break
    return out


def split_tags(v) -> list[str]:
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return [str(x).strip() for x in v if str(x).strip()]
    return [t.strip() for t in str(v).split(",") if t.strip()]


@dataclass
class EventTables:
    cluster_id: str
    spark_context_id: str
    tasks: dict = field(default_factory=lambda: {c: [] for c in TASK_COLS})  # columnar (can be millions)
    stage_submitted: list[dict] = field(default_factory=list)
    stage_completed: list[dict] = field(default_factory=list)
    job_start: list[dict] = field(default_factory=list)
    job_end: list[dict] = field(default_factory=list)
    sql_start: list[dict] = field(default_factory=list)
    sql_end: list[dict] = field(default_factory=list)
    sql_aqe: dict = field(default_factory=dict)  # (ctx, execution_id) -> last physicalPlanDescription (file order)
    # Revision 7: operator tree of the latest sparkPlanInfo per query, and SQL metric values by accumulator id
    sql_plans: dict = field(default_factory=dict)  # (ctx, execution_id) -> walk_plan() nodes
    sql_aqe_metrics: dict = field(default_factory=dict)  # (ctx, execution_id) -> [(name, accumulator id, type)]
    acc_task: dict = field(default_factory=dict)   # (ctx, accumulator id) -> [sum, max, tasks]
    acc_stage: dict = field(default_factory=dict)  # (ctx, accumulator id) -> stage total
    acc_driver: dict = field(default_factory=dict)  # (ctx, accumulator id) -> driver total
    exec_added: list[dict] = field(default_factory=list)
    exec_removed: list[dict] = field(default_factory=list)
    # Revision 13: what each executor was given
    bm_added: dict = field(default_factory=dict)           # (ctx, executor_id) -> {max_mem, on_heap, off_heap}
    resource_profiles: dict = field(default_factory=dict)  # (ctx, profile id) -> {cores, memory_mb, ...}
    exec_conf: dict = field(default_factory=dict)          # ctx -> {spark property: value}
    # Revision 14: settings that decide speed and cost: ctx -> {key: {"cluster": value, "session": {values}}}
    settings: dict = field(default_factory=dict)
    apps: list[dict] = field(default_factory=list)
    connect_ops: dict = field(default_factory=dict)       # (ctx, operation_id) -> row
    connect_sessions: dict = field(default_factory=dict)  # (ctx, session_id) -> {user_id, user_name}
    cluster_info: dict = field(default_factory=dict)      # ctx -> merged cluster_info fields
    event_counts: dict = field(default_factory=dict)      # (ctx, event_type) -> count (every type)
    file_lines: list[dict] = field(default_factory=list)  # per event-log file: path, lines
    warnings: list[str] = field(default_factory=list)

    @property
    def n_tasks(self) -> int:
        return len(self.tasks["task_id"])

    def extend(self, other: "EventTables") -> None:
        for c in TASK_COLS:
            self.tasks[c].extend(other.tasks[c])
        for name in ("stage_submitted", "stage_completed", "job_start", "job_end", "sql_start", "sql_end",
                     "exec_added", "exec_removed", "apps", "warnings", "file_lines"):
            getattr(self, name).extend(getattr(other, name))
        self.sql_aqe.update(other.sql_aqe)
        self.sql_plans.update(other.sql_plans)
        self.acc_task.update(other.acc_task)
        self.acc_stage.update(other.acc_stage)
        self.acc_driver.update(other.acc_driver)
        self.connect_ops.update(other.connect_ops)
        self.connect_sessions.update(other.connect_sessions)
        self.bm_added.update(other.bm_added)
        self.resource_profiles.update(other.resource_profiles)
        self.exec_conf.update(other.exec_conf)
        for ctx, keys in other.settings.items():
            mine = self.settings.setdefault(ctx, {})
            for k, v in keys.items():
                m = mine.setdefault(k, {"cluster": None, "session": set()})
                m["cluster"] = m["cluster"] or v["cluster"]
                m["session"] |= v["session"]
        self.cluster_info.update(other.cluster_info)
        for k, v in other.event_counts.items():
            self.event_counts[k] = self.event_counts.get(k, 0) + v


def _ns_ms(v) -> int | None:
    """Task Metrics `Executor CPU Time` is in nanoseconds."""
    n = _i(v)
    return None if n is None else n // 1_000_000


_BENIGN_KILL = re.compile(r"Adaptive query execution has replanned", re.I)


def _add_task(t: dict, ctx: str, cid: str, e: dict) -> None:
    ti = e.get("Task Info") or {}
    tm = e.get("Task Metrics") or {}
    ter = e.get("Task End Reason") or {}
    reason = ter.get("Reason")
    launch, finish = _ts(ti.get("Launch Time")), _ts(ti.get("Finish Time"))
    im = tm.get("Input Metrics") or {}
    om = tm.get("Output Metrics") or {}
    srm = tm.get("Shuffle Read Metrics") or {}
    swm = tm.get("Shuffle Write Metrics") or {}
    failed = reason is not None and reason != "Success"
    error = None
    if failed:
        error = ter.get("Description") or ter.get("Loss Reason") or ter.get("Kill Reason") or ter.get("Message")
        if error is not None:
            error = str(error)[:1000]
        # killed on purpose, not a failure: adaptive execution replanned the query and cancelled the stage it no
        # longer needed. The reason and text are kept.
        if reason == "TaskKilled" and _BENIGN_KILL.search(error or ""):
            failed = False
    ex = ti.get("Executor ID")
    spec = ti.get("Speculative")
    vals = (cid, ctx, _i(e.get("Stage ID")), _i(e.get("Stage Attempt ID")), _i(ti.get("Task ID")),
            _i(ti.get("Attempt")), None if ex is None else str(ex), ti.get("Host"), launch, finish,
            (finish - launch) if (launch is not None and finish is not None) else None,
            _i(tm.get("Executor Run Time")), _i(tm.get("JVM GC Time")), _i(tm.get("Peak Execution Memory")),
            _i(tm.get("Memory Bytes Spilled")), _i(tm.get("Disk Bytes Spilled")),
            _i(im.get("Bytes Read")), _i(im.get("Records Read")), _i(om.get("Bytes Written")),
            (_i(srm.get("Remote Bytes Read")) or 0) + (_i(srm.get("Local Bytes Read")) or 0),
            _i(swm.get("Shuffle Bytes Written")), failed, reason, error,
            _i(ti.get("Index", ti.get("Partition ID"))), bool(spec) if isinstance(spec, bool) else None,
            _ns_ms(tm.get("Executor CPU Time")), _i(srm.get("Fetch Wait Time")),
            _i(srm.get("Total Records Read")), _i(swm.get("Shuffle Records Written")), _i(om.get("Records Written")),
            _ns_ms(swm.get("Shuffle Write Time")), task_oom_site(ter) if failed else None)
    for c, v in zip(TASK_COLS, vals):
        t[c].append(v)


STAGE_LIST_MAX = 20
STAGE_DETAILS_MAX = 2000


def _scope_name(scope) -> str | None:
    """Operator name of an `RDD Info[].Scope` value: a JSON string like {"id":"3","name":"Exchange"} (or a dict)."""
    if scope is None:
        return None
    obj = scope
    if isinstance(scope, str):
        s = scope.strip()
        if not s.startswith("{"):
            return s or None
        try:
            obj = json.loads(s)
        except ValueError:
            return None
    if isinstance(obj, dict):
        n = obj.get("name")
        return str(n).strip() or None if n not in (None, "") else None
    return None


def _distinct(values, n: int = STAGE_LIST_MAX) -> list[str]:
    out: list[str] = []
    seen = set()
    for v in values:
        if v is None:
            continue
        v = str(v).strip()
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
        if len(out) >= n:
            break
    return out


def _stage_info(e: dict, ctx: str, cid: str) -> dict:
    si = e.get("Stage Info") or {}
    if not isinstance(si, dict):
        si = {}
    rdds = si.get("RDD Info")
    rdds = [r for r in rdds if isinstance(r, dict)] if isinstance(rdds, list) else None
    parents = si.get("Parent IDs")
    details = si.get("Details")
    return {"cluster_id": cid, "spark_context_id": ctx, "stage_id": _i(si.get("Stage ID")),
            "stage_attempt": _i(si.get("Stage Attempt ID")) or 0, "stage_name": si.get("Stage Name"),
            "num_tasks": _i(si.get("Number of Tasks")), "start_time": _ts(si.get("Submission Time")),
            "end_time": _ts(si.get("Completion Time")), "failure_reason": si.get("Failure Reason"),
            # Revision 4 (None when the key is absent, so StageCompleted falls back to StageSubmitted)
            "parent_ids": sorted({p for p in (_i(x) for x in parents) if p is not None})
            if isinstance(parents, list) else None,
            "rdd_names": _distinct(r.get("Name") for r in rdds) if rdds is not None else None,
            "rdd_scopes": _distinct(_scope_name(r.get("Scope")) for r in rdds) if rdds is not None else None,
            "details": _s(details, STAGE_DETAILS_MAX) if details not in (None, "") else None}


def _connect_event(tables: EventTables, name: str, e: dict) -> None:
    ctx = tables.spark_context_id
    short = name.rsplit(".", 1)[-1]
    t = _ts(e.get("eventTime"))
    if short.startswith(CONNECT_SESSION_PREFIX):
        sid = _s(e.get("sessionId"))
        if sid is None:
            return
        sess = tables.connect_sessions.setdefault((ctx, sid), {})
        for src, dst in (("userId", "user_id"), ("userName", "user_name")):
            if e.get(src) not in (None, ""):
                sess.setdefault(dst, _s(e.get(src)))
        return
    op_id = _s(e.get("operationId"))
    if op_id is None:
        return
    op = tables.connect_ops.get((ctx, op_id))
    if op is None:
        op = tables.connect_ops[(ctx, op_id)] = {
            "cluster_id": tables.cluster_id, "spark_context_id": ctx, "operation_id": op_id, "session_id": None,
            "user_id": None, "user_name": None, "statement_text": None, "job_tag": None, "start_time": None,
            "analyzed_time": None, "ready_time": None, "finish_time": None, "closed_time": None,
            "_finished": False, "_failed": False, "_canceled": False, "error": None}
    for src, dst, n in (("sessionId", "session_id", None), ("userId", "user_id", None),
                        ("userName", "user_name", None), ("statementText", "statement_text", 4000),
                        ("jobTag", "job_tag", None)):
        if op[dst] is None and e.get(src) not in (None, ""):
            op[dst] = _s(e.get(src), n)
    what = short[len(CONNECT_OP_PREFIX):]
    if what == "Started":
        op["start_time"] = t
    elif what == "Analyzed":
        op["analyzed_time"] = t
    elif what == "ReadyForExecution":
        op["ready_time"] = t
    elif what == "Finished":
        op["finish_time"] = op["finish_time"] or t
        op["_finished"] = True
    elif what in ("Failed", "Canceled"):
        op["finish_time"] = op["finish_time"] or t
        op["_failed" if what == "Failed" else "_canceled"] = True
        err = e.get("errorMessage") or e.get("message") or e.get("error")
        if err not in (None, "") and op["error"] is None:
            op["error"] = _s(err, 1000)
    elif what == "Closed":
        op["closed_time"] = t


# Structured Streaming names each micro-batch "id = <stream uuid> / runId = <uuid> / batch = <n>": unreadable as a
# query name, so it becomes "Streaming batch <n> · stream <first 8 of the id>".
_STREAM_DESC = re.compile(r"^\s*id = ([0-9a-f-]{8,})\s*\n\s*runId = [0-9a-f-]+\s*\n\s*batch = (\d+)\s*(.*)$", re.S)


_LOC_RE = re.compile(r"^\s*((?:[\w./\-]+\.py|command-\d+-\d+|[\w\-]+):\d+)\s*$")


def statement_label(text) -> str | None:
    """A Spark Connect statement as "code line (file:line)": its first line is where the call is
    (`<file>.py:<line>` or `command-<id>-<n>:<line>`), the next the code itself."""
    if not isinstance(text, str):
        return None
    lines = [x for x in text.splitlines() if x.strip()]
    if not lines:
        return None
    m = _LOC_RE.match(lines[0])
    if not m:
        return lines[0].strip()[:120]
    code = lines[1].strip()[:120] if len(lines) > 1 else ""
    return f"{code} ({m.group(1)})" if code else m.group(1)


def is_connect_blob(d) -> bool:
    """The Spark Connect description that names a session, not the code."""
    return isinstance(d, str) and d.startswith("Spark Connect")


def readable_description(d):
    if not isinstance(d, str):
        return d
    m = _STREAM_DESC.match(d)
    if not m:
        # a cell's first comment line names the step ("# Read the orders")
        if d.startswith("#") and not d.startswith("#!"):
            first = d.splitlines()[0].lstrip("#").strip()
            return first or d
        return d
    rest = m.group(3).strip().lstrip("-").strip()  # e.g. "MERGE operation - materialize source"
    return f"Streaming batch {m.group(2)} · stream {m.group(1)[:8]}" + (f" · {rest}" if rest else "")


def _note_settings(tables: EventTables, ctx: str, props: dict, source: str) -> None:
    """Keep the SETTING_KEYS values: the cluster's configuration, or values set in code that differ from it."""
    for k in SETTING_KEYS.intersection(props):
        v = props[k]
        if v in (None, ""):
            continue
        m = tables.settings.setdefault(ctx, {}).setdefault(k, {"cluster": None, "session": set()})
        if source == "cluster":
            m["cluster"] = str(v)
        elif str(v) != m["cluster"]:
            m["session"].add(str(v))


def consume_events(events: Iterable[tuple[str, dict]], tables: EventTables) -> None:
    ctx, cid = tables.spark_context_id, tables.cluster_id
    app = None
    info = tables.cluster_info.setdefault(ctx, {})
    for name, e in events:
        if name == EV_TASK_END:
            _add_task(tables.tasks, ctx, cid, e)
            add_task_accums(tables.acc_task, ctx, e)
        elif name == EV_STAGE_COMPLETED:
            tables.stage_completed.append(_stage_info(e, ctx, cid))
            add_stage_accums(tables.acc_stage, ctx, e)
        elif name == EV_STAGE_SUBMITTED:
            tables.stage_submitted.append(_stage_info(e, ctx, cid))
        elif name == EV_JOB_START:
            props = _props(e.get("Properties"))
            stage_ids = [s for s in (_i(x) for x in (e.get("Stage IDs") or [])) if s is not None]
            for k, v in cluster_info_from_props(props).items():
                info.setdefault(k, v)
            connect_op = None
            for k, v in props.items():
                kl = k.lower()
                if kl.startswith("spark.connect") and "operation" in kl and kl.endswith("id") and v:
                    connect_op = str(v)
                    break
            _note_settings(tables, ctx, props, "session")
            tags = props.get("spark.job.tags")
            tables.job_start.append({
                "cluster_id": cid, "spark_context_id": ctx, "spark_job_id": _i(e.get("Job ID")),
                "start_time": _ts(e.get("Submission Time")), "stage_ids": stage_ids,
                "sql_execution_id": _i(props.get("spark.sql.execution.id")),
                "description": readable_description(props.get("spark.job.description")), "job_group": props.get("spark.jobGroup.id"),
                "call_site": props.get("callSite.long") or props.get("callSite.short"),
                "call_site_short": props.get("callSite.short"),
                "databricks_job_id": props.get("spark.databricks.job.id"),
                "databricks_run_id": props.get("spark.databricks.job.runId"),
                "notebook_path": props.get("spark.databricks.notebook.path"),
                "job_tags": _s(",".join(tags) if isinstance(tags, list) else tags),
                "databricks_task_run_id": _s(props.get("spark.databricks.job.taskRunId")),
                # Revision 13: the job run the task belongs to, the job's name and the task type
                "databricks_parent_run_id": _s(props.get("spark.databricks.job.parentRunId")),
                "databricks_job_name": _s(props.get("spark.databricks.workload.name"), 300),
                "databricks_task_type": _s(props.get("spark.databricks.job.type")),
                "_connect_op_prop": connect_op,
            })
        elif name == EV_JOB_END:
            jr = e.get("Job Result") or {}
            exc = jr.get("Exception") or {}
            msg = exc.get("Message") if isinstance(exc, dict) else None
            tables.job_end.append({"spark_context_id": ctx, "spark_job_id": _i(e.get("Job ID")),
                                   "end_time": _ts(e.get("Completion Time")), "result": jr.get("Result"),
                                   "error": msg[:500] if isinstance(msg, str) else None})
        elif name == EV_EXEC_ADDED:
            einfo = e.get("Executor Info") or {}
            ex = e.get("Executor ID")
            tables.exec_added.append({"cluster_id": cid, "spark_context_id": ctx,
                                      "executor_id": None if ex is None else str(ex),
                                      "added_time": _ts(e.get("Timestamp")), "host": einfo.get("Host"),
                                      "cores": _i(einfo.get("Total Cores")),
                                      "resource_profile_id": _i(einfo.get("Resource Profile Id"))})
        elif name == EV_BM_ADDED:
            bm = e.get("Block Manager ID") or {}
            ex = bm.get("Executor ID")
            if ex is not None:
                tables.bm_added[(ctx, str(ex))] = {"max_mem": _i(e.get("Maximum Memory")),
                                                   "on_heap": _i(e.get("Maximum Onheap Memory")),
                                                   "off_heap": _i(e.get("Maximum Offheap Memory"))}
        elif name == EV_RP_ADDED:
            req = e.get("Executor Resource Requests") or {}
            treq = e.get("Task Resource Requests") or {}

            def amt(d, k):
                v = d.get(k)
                return v.get("Amount") if isinstance(v, dict) else None

            tables.resource_profiles[(ctx, _i(e.get("Resource Profile Id")))] = {
                "cores": _i(amt(req, "cores")), "memory_mb": _i(amt(req, "memory")),
                "overhead_mb": _i(amt(req, "memoryOverhead")), "offheap_mb": _i(amt(req, "offHeap")),
                "task_cpus": amt(treq, "cpus")}
        elif name == EV_EXEC_REMOVED:
            ex = e.get("Executor ID")
            reason, raw = parse_removed_reason(e.get("Removed Reason"))
            tables.exec_removed.append({"spark_context_id": ctx, "executor_id": None if ex is None else str(ex),
                                        "removed_time": _ts(e.get("Timestamp")),
                                        "removed_reason": reason, "removed_reason_raw": raw})
        elif name == EV_SQL_START:
            details = e.get("details")
            tables.sql_start.append({
                "cluster_id": cid, "spark_context_id": ctx, "sql_execution_id": _i(e.get("executionId")),
                "description": readable_description(e.get("description")),
                "details": details[:4000] if isinstance(details, str) else None,
                "initial_plan": e.get("physicalPlanDescription"), "start_time": _ts(e.get("time")),
                "root_execution_id": _i(e.get("rootExecutionId"))})
            mc = e.get("modifiedConfigs")
            if isinstance(mc, dict):
                _note_settings(tables, ctx, mc, "session")
            nodes = walk_plan(e.get("sparkPlanInfo"))
            if nodes:
                tables.sql_plans[(ctx, _i(e.get("executionId")))] = nodes
        elif name == EV_SQL_END:
            err = e.get("errorMessage")
            tables.sql_end.append({"spark_context_id": ctx, "sql_execution_id": _i(e.get("executionId")),
                                   "end_time": _ts(e.get("time")),
                                   "error": err[:1000] if isinstance(err, str) else None})
        elif name == EV_SQL_AQE:
            plan = e.get("physicalPlanDescription")
            if plan is not None:
                tables.sql_aqe[(ctx, _i(e.get("executionId")))] = plan  # later lines overwrite: last wins
            nodes = walk_plan(e.get("sparkPlanInfo"))
            if nodes:
                tables.sql_plans[(ctx, _i(e.get("executionId")))] = nodes
        elif name == EV_SQL_AQE_METRICS:
            ms = [(str(m.get("name") or ""), _i(m.get("accumulatorId")), str(m.get("metricType") or "sum"))
                  for m in e.get("sqlPlanMetrics") or [] if isinstance(m, dict) and _i(m.get("accumulatorId")) is not None]
            if ms:
                tables.sql_aqe_metrics.setdefault((ctx, _i(e.get("executionId"))), []).extend(ms)
        elif name == EV_DRIVER_ACCUM:
            add_driver_accums(tables.acc_driver, ctx, e)
        elif name == EV_APP_START:
            app = app or {}
            app.update({"app_id": e.get("App ID"), "app_name": e.get("App Name"), "user": e.get("User"),
                        "start_time": _ts(e.get("Timestamp"))})
        elif name == EV_APP_END:
            app = app or {}
            app["end_time"] = _ts(e.get("Timestamp"))
        elif name == EV_LOG_START or name == EV_DBC_META:
            app = app or {}
            if e.get("Spark Version"):
                app.setdefault("spark_version", e.get("Spark Version"))
        elif name == EV_ENV_UPDATE:
            props = _props(e.get("Spark Properties"))
            for k, v in cluster_info_from_props(props).items():
                info.setdefault(k, v)
            conf = {k: props[k] for k in EXEC_CONF_KEYS if props.get(k) not in (None, "")}
            if conf:
                tables.exec_conf.setdefault(ctx, {}).update(conf)
            _note_settings(tables, ctx, props, "cluster")
        elif name.startswith(CONNECT):
            _connect_event(tables, name, e)
    if app is not None:
        tables.apps.append(app)
    if not info:
        tables.cluster_info.pop(ctx, None)


def parse_eventlog_dir(files: list[str | os.PathLike], *, cluster_id: str, spark_context_id: str, rules: Rules,
                       ) -> EventTables:
    """Parse the ordered event-log files of ONE spark context into EventTables (plus an `apps` row)."""
    tables = EventTables(cluster_id, spark_context_id)
    stats = EventStats()
    wanted = set(rules.events)

    def all_lines():
        for p in files:
            n0 = stats.lines
            yield from open_text_lines(p, on_error=tables.warnings.append)
            tables.file_lines.append({"path": p, "lines": stats.lines - n0})

    consume_events(iter_events(all_lines(), wanted, stats), tables)
    tables.event_counts = {(spark_context_id, k): v for k, v in stats.counts.items()}
    app = tables.apps[0] if tables.apps else {}
    tables.apps = [{
        "cluster_id": cluster_id, "spark_context_id": spark_context_id, "app_id": app.get("app_id"),
        "app_name": app.get("app_name"), "spark_version": app.get("spark_version"), "user": app.get("user"),
        "start_time": app.get("start_time"), "end_time": app.get("end_time"),
        "eventlog_files": len(files), "events_read": stats.matched, "malformed_lines": stats.malformed,
        "lines_read": stats.lines,
    }]
    return tables
