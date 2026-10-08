"""Data / Debug steps: the notebook's intermediate tables (S1-S2, D1-D5, E1-E6, V1-V8 (+ V7 spill & shuffle), A1-A4, C1-C4) as DuckDB
queries over the built Parquet files.

GET /api/clusters/{cid}/steps        -> [{id, group, step, title, description, rows}]
GET /api/clusters/{cid}/steps/{id}   -> generic table envelope (limit, offset, sort, desc, q)
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from . import queries as Q

NO_CONT = "NOT coalesce(continuation, FALSE)"


@dataclass(frozen=True)
class Step:
    id: str
    group: str
    step: str
    title: str
    description: str
    needs: tuple[str, ...]
    sql: Callable[[dict[str, str]], str]   # dataset name -> read_parquet(...) source
    order: tuple[str, ...] = ()            # default ORDER BY (result columns); "-col" = descending

    def info(self, rows: int | None) -> dict[str, Any]:
        return {"id": self.id, "group": self.group, "step": self.step, "title": self.title,
                "description": self.description, "rows": rows}


def _page(name: str, exclude: Iterable[str] = ()) -> Callable[[dict[str, str]], str]:
    ex = tuple(exclude)
    if ex:
        return lambda d: f"SELECT * EXCLUDE ({', '.join(Q.qi(c) for c in ex)}) FROM {d[name]}"
    return lambda d: f"SELECT * FROM {d[name]}"


def _signals(where: str = "TRUE") -> Callable[[dict[str, str]], str]:
    key = f"coalesce(ts, {Q._FAR_FUTURE}), seq"
    return lambda d: f"""
        SELECT signal, severity, fix, count(*) AS occurrences,
               count(DISTINCT nullif(executor_id, '')) AS executors_affected,
               min(ts) AS first_seen, max(ts) AS last_seen,
               first(line ORDER BY {key}) AS sample_line,
               first(file_path ORDER BY {key}) AS sample_file_path,
               first(seq ORDER BY {key}) AS sample_seq
        FROM {d['log_signals']} WHERE {where}
        GROUP BY signal, severity, fix"""


def _errors(where: str = "TRUE") -> Callable[[dict[str, str]], str]:
    key = f"coalesce(ts, {Q._FAR_FUTURE}), seq"
    return lambda d: f"""
        SELECT fingerprint, first(exception_class ORDER BY {key}) AS exception_class, count(*) AS occurrences,
               count(DISTINCT nullif(executor_id, '')) AS executors_affected,
               min(ts) AS first_seen, max(ts) AS last_seen,
               first(message ORDER BY {key}) AS sample_message,
               first(top_frames ORDER BY {key}) AS top_frames,
               first(user_frame ORDER BY {key}) FILTER (WHERE user_frame IS NOT NULL) AS user_frame,
               first(file_path ORDER BY {key}) AS sample_file_path,
               first(seq ORDER BY {key}) AS sample_seq
        FROM {d['log_errors']} WHERE {where}
        GROUP BY fingerprint"""


def _loggers(source: str) -> Callable[[dict[str, str]], str]:
    return lambda d: f"""
        SELECT level, logger, count(*) AS lines, min(ts) AS first_seen, max(ts) AS last_seen
        FROM {d['log_lines']}
        WHERE source = '{source}' AND level IN ('ERROR', 'WARN', 'FATAL') AND {NO_CONT}
        GROUP BY level, logger"""


def _first_errors(source: str) -> Callable[[dict[str, str]], str]:
    ex = "executor_id, " if source == "executor" else ""
    return lambda d: f"""
        SELECT ts, level, logger, {ex}message, file_path, seq FROM {d['log_lines']}
        WHERE source = '{source}' AND level IN ('ERROR', 'FATAL') AND {NO_CONT}
        ORDER BY ts NULLS LAST, seq LIMIT 50"""


def _file_lines(folder: str) -> Callable[[dict[str, str]], str]:
    return lambda d: (f"SELECT file_path, lines, bytes, thread_dumps FROM {d['file_lines']} "
                      f"WHERE folder = '{folder}'")


def _levels(source: str) -> Callable[[dict[str, str]], str]:
    return lambda d: (f"SELECT coalesce(level, '(no level)') AS level, count(*) AS lines FROM {d['log_lines']} "
                      f"WHERE source = '{source}' AND {NO_CONT} GROUP BY 1")


SETUP, DRIVER, EXECUTOR, EVENTS, ANALYSIS, COMBINED = (
    "Setup", "Driver logs", "Executor logs", "Event logs", "Analysis", "Combined")

STEPS: tuple[Step, ...] = (
    Step("s1_status", SETUP, "S1", "Folders read",
         "Status of the driver/, executor/ and eventlog/ folders: how many files each holds and their size. A "
         "missing folder means that part of the analysis has no input (serverless compute has none at all).",
         ("files",), lambda d: f"""
        SELECT f.folder, CASE WHEN count(x.path) > 0 THEN 'ok' ELSE 'missing' END AS status,
               count(x.path) AS files, round(coalesce(sum(x.size), 0) / 1e6, 2) AS total_mb
        FROM (VALUES ('driver'), ('executor'), ('eventlog')) AS f(folder)
        LEFT JOIN {d['files']} x ON x.folder = f.folder GROUP BY f.folder
        UNION ALL
        SELECT folder, 'ignored', count(*), round(sum(size) / 1e6, 2) FROM {d['files']} WHERE folder = 'other'
        GROUP BY folder""", ("folder",)),
    Step("s2_files", SETUP, "S2", "Files",
         "Every file in the cluster folder with its size, modification time and (for log files) its line and "
         "thread-dump counts.",
         ("files", "file_lines"), lambda d: f"""
        SELECT f.folder, f.path, round(f.size / 1e6, 3) AS size_mb, f.modified, l.lines, l.thread_dumps
        FROM {d['files']} f LEFT JOIN {d['file_lines']} l ON l.file_path = f.path""", ("folder", "path")),

    Step("d1_files", DRIVER, "D1", "Driver lines per file",
         "One row per driver log file (log4j-active.log, rolled log4j-*.log.gz, stdout, stderr, stacktrace.log). "
         "Check the per-file line counts look sensible.",
         ("file_lines",), _file_lines("driver"), ("file_path",)),
    Step("d1_raw", DRIVER, "D1", "Raw driver lines",
         "The driver lines as read, nothing parsed yet, in file order.",
         ("log_lines",), lambda d: (f"SELECT seq, file_path, line_no, line FROM {d['log_lines']} "
                                    f"WHERE source = 'driver'"), ("seq",)),
    Step("d2_levels", DRIVER, "D2", "Driver lines by level",
         "Each line split into timestamp, level, logger and message. The level breakdown tells you at a glance "
         "how noisy the run was: a handful of ERROR lines is normal; thousands usually means a retry loop.",
         ("log_lines",), _levels("driver"), ("-lines",)),
    Step("d3_loggers", DRIVER, "D3", "Driver errors and warnings by logger",
         "Which loggers complain most points to the area of the problem (TaskSetManager = task failures, "
         "DAGScheduler = stage failures, BlockManager = memory or shuffle).",
         ("log_lines",), _loggers("driver"), ("-lines",)),
    Step("d3_first_errors", DRIVER, "D3", "First 50 driver errors",
         "The first errors in time order: the earliest error is usually the cause and the rest follow from it.",
         ("log_lines",), _first_errors("driver"), ("ts", "seq")),
    Step("d4_signals", DRIVER, "D4", "Known problem signals in the driver",
         "Lines matching the SIGNALS patterns, counted per signal, with the suggested fix and a sample line.",
         ("log_signals",), _signals("source = 'driver'"), ("severity_rank", "-occurrences")),
    Step("d5_exceptions", DRIVER, "D5", "Driver exceptions by fingerprint",
         "Each exception with its top stack frames. The same fingerprint showing up many times is one problem, "
         "not many.",
         ("log_errors",), _errors("source = 'driver'"), ("-occurrences", "first_seen")),

    Step("e1_files", EXECUTOR, "E1", "Executor lines per file",
         "Executor logs sit at executor/<app-id>/<executor-id>/stderr (and stdout), one folder per executor.",
         ("file_lines",), _file_lines("executor"), ("file_path",)),
    Step("e2_executors", EXECUTOR, "E2", "Executors compared",
         "One executor with far more errors or warnings than the others usually means data skew (it got the hot "
         "partition) or a bad node.",
         ("log_lines",), lambda d: f"""
        SELECT app_id, executor_id, count(*) AS lines,
               count(*) FILTER (WHERE level IN ('ERROR', 'FATAL') AND {NO_CONT}) AS errors,
               count(*) FILTER (WHERE level = 'WARN' AND {NO_CONT}) AS warnings,
               min(ts) AS first_ts, max(ts) AS last_ts
        FROM {d['log_lines']} WHERE source = 'executor' GROUP BY app_id, executor_id""", ("-errors", "-warnings")),
    Step("e3_loggers", EXECUTOR, "E3", "Executor errors and warnings by logger",
         "Executor ERROR / WARN lines grouped by level and logger.",
         ("log_lines",), _loggers("executor"), ("-lines",)),
    Step("e3_first_errors", EXECUTOR, "E3", "First 50 executor errors",
         "The first executor errors in time order.",
         ("log_lines",), _first_errors("executor"), ("ts", "seq")),
    Step("e4_signals", EXECUTOR, "E4", "Known problem signals in the executors",
         "executors_affected matters here: an OOM on one executor suggests skew; on all of them, the executors "
         "are simply too small for the job.",
         ("log_signals",), _signals("source = 'executor'"), ("severity_rank", "-occurrences")),
    Step("e5_exceptions", EXECUTOR, "E5", "Executor exceptions by fingerprint",
         "Executor exceptions grouped by fingerprint, with the first user-code frame (where to fix).",
         ("log_errors",), _errors("source = 'executor'"), ("-occurrences", "first_seen")),
    Step("e6_gc_events", EXECUTOR, "E6", "JVM GC pauses (driver and executors)",
         "GC events parsed from JVM GC log lines in stdout: pause kind, cause, heap before/after and pause time. "
         "Repeated 'Pause Full' or a heap that stays near its maximum after GC means the JVM is short of memory.",
         ("gc_events",), _page("gc_events"), ("seq",)),

    Step("v1_event_counts", EVENTS, "V1", "Event types per Spark context",
         "Every event type in the event logs, counted, including the ones not read (e.g. SparkListenerTaskStart). "
         "A new spark_context_id appears each time the cluster restarts; job and stage ids restart with it.",
         ("event_counts",), _page("event_counts"), ("spark_context_id", "-count")),
    Step("v2_slowest_tasks", EVENTS, "V2", "Tasks, slowest first",
         "One row per task attempt. If one stage's slowest task is far slower than its others, that stage is "
         "skewed.",
         ("tasks",), _page("tasks"), ("-task_ms",)),
    Step("v2_failed_reasons", EVENTS, "V2", "Failed task reasons",
         "Failed task attempts grouped by end reason (ExecutorLostFailure, ExceptionFailure, FetchFailed, "
         "TaskKilled).",
         ("tasks",), lambda d: f"""
        SELECT end_reason, count(*) AS tasks, count(DISTINCT executor_id) AS executors,
               count(DISTINCT stage_id) AS stages, first(error ORDER BY launch_time) AS sample_error
        FROM {d['tasks']} WHERE failed GROUP BY end_reason""", ("-tasks",)),
    Step("v2_task_retries", EVENTS, "V2", "Task retries",
         "Tasks that failed at least once: what failed first, on which executor and why, and whether the retry "
         "succeeded. Retries can hide behind a job that still succeeded.",
         ("task_retries",), _page("task_retries"), ("first_failure_time",)),
    Step("v3_stages", EVENTS, "V3", "Stages",
         "Tasks rolled up per stage. Look for skew (slowest / median task above ~10), gc_share above ~0.2, large "
         "disk_spill, failed_tasks (retries) and failure_reason.",
         ("stages",), _page("stages"), ("spark_context_id", "start_time", "stage_id", "stage_attempt")),
    Step("v4_spark_jobs", EVENTS, "V4", "Spark jobs",
         "One row per Spark job (each action such as write, count or collect starts one). The properties tie a "
         "job back to its notebook, Databricks job run, SQL query or Spark Connect operation.",
         ("spark_jobs",), _page("spark_jobs"), ("spark_context_id", "spark_job_id")),
    Step("v5_sql_queries", EVENTS, "V5", "SQL and DataFrame queries",
         "Every SQL statement and DataFrame action with duration, error, totals from its stages and plan_hash "
         "(same hash = same plan across runs). Plans are on the query page.",
         ("sql_queries",), _page("sql_queries", ("final_plan", "initial_plan")),
         ("spark_context_id", "sql_execution_id")),
    Step("v6_executors", EVENTS, "V6", "Executors",
         "When each executor joined and left, and why it left: oom, killed (by the OS), lost (spot, heartbeat), "
         "autoscale or termination.",
         ("executors",), _page("executors"), ("spark_context_id", "added_time", "executor_id")),
    Step("v7_spill_shuffle_timeline", EVENTS, "V7", "Spill & shuffle over time",
         "Task spill, shuffle and I/O bytes per time bucket (rules.toml timeline_bucket_seconds), executor and "
         "stage attempt: when and where data spilled to disk or was shuffled. Each task's bytes are counted in the "
         "bucket of its finish time.",
         ("spill_shuffle_timeline",), _page("spill_shuffle_timeline"),
         ("spark_context_id", "minute", "executor_id", "stage_id", "stage_attempt")),
    Step("v7_connect_operations", EVENTS, "V7", "Spark Connect operations",
         "Statements sent through Spark Connect (shared clusters, serverless-style notebooks): who ran what, "
         "when, and how it ended. Spark jobs link to them through connect_operation_id.",
         ("connect_operations",), _page("connect_operations"), ("spark_context_id", "start_time")),
    Step("v8_cluster_info", EVENTS, "V8", "Cluster info",
         "Cluster name, runtime, node types, worker counts and job / run ids from the spark.databricks."
         "clusterUsageTags.* properties, per Spark context.",
         ("cluster_info",), _page("cluster_info"), ("spark_context_id",)),

    Step("c5_runs", COMBINED, "C5", "Runs on this cluster",
         "One row per run unit (Databricks job task run, job run, Spark Connect session, notebook, job group or the "
         "whole Spark app) with its status, size, retries, findings and which other runs overlapped it in time.",
         ("runs",), _page("runs"), ("start_time", "run_key")),
    Step("a5_hotspots", ANALYSIS, "A5", "Peaks and hotspots",
         "When and where skew, shuffle, spill and GC peaked: the skewed tasks of each stage with their cause "
         "(data skew, slow executor, GC), the busiest minutes for shuffle, spill and GC with the stage and executor "
         "behind them, and the slowest tasks. Each row has a plain-English detail sentence.",
         ("hotspots",), _page("hotspots"), ("spark_context_id", "kind", "ts_start")),

    Step("a1_signals", ANALYSIS, "A1", "Log signals, driver and executors together",
         "Signal summary over all driver and executor logs.",
         ("log_signals",), _signals(), ("severity_rank", "-occurrences")),
    Step("a1_errors", ANALYSIS, "A1", "Exceptions, driver and executors together",
         "Exception fingerprints over all driver and executor logs.",
         ("log_errors",), _errors(), ("-occurrences", "first_seen")),
    Step("a2_event_findings", ANALYSIS, "A2", "Findings from the event log",
         "Rules over the stage, executor and query tables (thresholds in rules.toml): stage_failed, task_skew, "
         "disk_spill, gc_pressure, task_retries, tiny_tasks, executor_oom / lost / killed, query_failed, "
         "jvm_full_gc.",
         ("findings",), lambda d: (f"SELECT * FROM {d['findings']} WHERE category NOT LIKE 'log:%' "
                                   f"AND category <> 'exception'"), ("finding_id",)),
    Step("a3_findings", ANALYSIS, "A3", "All findings, ranked",
         "The main output: event-log findings, log signals and exceptions in one list, high severity first, each "
         "with evidence and a fix.",
         ("findings",), _page("findings"), ("finding_id",)),
    Step("a4_timeline", ANALYSIS, "A4", "Timeline",
         "Log signals per minute: what happened first (for example spill, then GC, then OOM, then lost "
         "executors).",
         ("timeline",), _page("timeline"), ("minute", "signal")),

    Step("c1_query_profile", COMBINED, "C1", "Query profile",
         "One row per SQL query with its jobs, stages, spill, GC, skew, bytes, executors lost and findings.",
         ("query_profile",), _page("query_profile"), ("spark_context_id", "sql_execution_id")),
    Step("c2_stage_executor_profile", COMBINED, "C2", "Stage x executor",
         "Task time per stage per executor: did one executor carry a stage?",
         ("stage_executor_profile",), _page("stage_executor_profile"),
         ("spark_context_id", "stage_id", "stage_attempt", "executor_id")),
    Step("c3_executor_profile", COMBINED, "C3", "Executor profile",
         "Executor lifetime, busy share, spill, GC pauses, removal reason and its own log signals / exceptions.",
         ("executor_profile",), _page("executor_profile"), ("spark_context_id", "added_time", "executor_id")),
    Step("c4_run_story", COMBINED, "C4", "Run story",
         "Scheduler events, important log lines, retries and findings in one ordered stream.",
         ("run_story",), _page("run_story"), ("story_seq",)),
)

STEP_BY_ID = {s.id: s for s in STEPS}
_SEVERITY_RANK_SQL = "CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 ELSE 3 END"

_count_cache: dict[tuple, int | None] = {}
_cache_lock = threading.Lock()


def _sources(store: Q.Store, cid: str, step: Step) -> dict[str, str] | None:
    out = {}
    for n in step.needs:
        p = store.dataset_path(cid, n, required=False)
        if p is None:
            return None
        out[n] = store.src(p)
    return out


def _base_sql(step: Step, d: dict[str, str]) -> str:
    sql = step.sql(d)
    if "severity_rank" in step.order:
        sql = f"SELECT *, {_SEVERITY_RANK_SQL} AS severity_rank FROM ({sql})"
    return sql


def _stamp(store: Q.Store, cid: str) -> float:
    p = store.cluster_dir(cid) / "summary.json"
    try:
        return p.stat().st_mtime
    except OSError:
        return 0.0


def list_steps(store: Q.Store, cid: str) -> list[dict[str, Any]]:
    store.cluster_dir(cid)
    stamp = _stamp(store, cid)
    out = []
    with store.connect() as con:
        for step in STEPS:
            key = (str(store.output_root), cid, stamp, step.id)
            with _cache_lock:
                hit = _count_cache.get(key, "miss")
            if hit != "miss":
                out.append(step.info(hit))  # type: ignore[arg-type]
                continue
            d = _sources(store, cid, step)
            rows = None
            if d is not None:
                try:
                    rows = int(con.execute(f"SELECT count(*) FROM ({_base_sql(step, d)})").fetchone()[0])
                except Exception:  # noqa: BLE001  (older build without a column the step needs)
                    rows = None
            with _cache_lock:
                if len(_count_cache) > 5000:
                    _count_cache.clear()
                _count_cache[key] = rows
            out.append(step.info(rows))
    return out


def step_table(store: Q.Store, cid: str, step_id: str, query_items: Iterable[tuple[str, str]]) -> dict[str, Any]:
    store.cluster_dir(cid)
    step = STEP_BY_ID.get(step_id)
    if step is None:
        raise Q.NotFound(f"unknown step: {step_id!r}")
    single = {k: v for k, v in query_items}
    limit = max(0, min(int(Q._parse_int("limit", single.get("limit"), Q.DEFAULT_LIMIT)), Q.MAX_LIMIT))
    offset = max(0, int(Q._parse_int("offset", single.get("offset"), 0)))
    desc = Q._parse_bool(single.get("desc"), False)
    d = _sources(store, cid, step)
    if d is None:
        missing = [n for n in step.needs if store.dataset_path(cid, n, required=False) is None]
        return {"columns": [], "rows": [], "total": 0, "limit": limit, "offset": offset, "step": step.info(None),
                "missing": missing}
    base = _base_sql(step, d)
    with store.connect() as con:
        try:
            desc_rows = con.execute(f"DESCRIBE {base}").fetchall()
        except Exception as e:  # noqa: BLE001
            raise Q.ApiError(f"step {step_id} cannot run on this build (rebuild the cluster): {e}") from e
        sch = {r[0]: str(r[1]) for r in desc_rows}
        cols = [c for c in sch if c != "severity_rank"]
        params: list[Any] = []
        where = "TRUE"
        q = single.get("q")
        if q:
            ors = []
            for c, t in sch.items():
                if Q._is_string_type(t):
                    ors.append(f"strpos(lower({Q.qi(c)}), ?) > 0")
                    params.append(q.lower())
                elif t == "VARCHAR[]":
                    ors.append(f"strpos(lower(array_to_string({Q.qi(c)}, chr(10))), ?) > 0")
                    params.append(q.lower())
            where = "(" + (" OR ".join(ors) or "FALSE") + ")"
        sort = (single.get("sort") or "").strip()
        order_parts = []
        if sort:
            if sort not in sch:
                raise Q.BadRequest(f"unknown sort column for step {step_id}: {sort!r}")
            order_parts.append(f"{Q.qi(sort)} {'DESC' if desc else 'ASC'} NULLS LAST")
        for o in step.order:
            c, dsc = (o[1:], True) if o.startswith("-") else (o, False)
            if c in sch and c != sort:
                order_parts.append(f"{Q.qi(c)} {'DESC' if dsc else 'ASC'} NULLS LAST")
        order = f" ORDER BY {', '.join(order_parts)}" if order_parts else ""
        total = int(con.execute(f"SELECT count(*) FROM ({base}) WHERE {where}", params).fetchone()[0])
        rows = store.rows(con, f"SELECT {', '.join(Q.qi(c) for c in cols)} FROM ({base}) WHERE {where}{order} "
                               f"LIMIT {limit} OFFSET {offset}", params)
    return {"columns": cols, "rows": rows, "total": total, "limit": limit, "offset": offset,
            "step": step.info(total if not q else None)}
