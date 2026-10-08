// Typed client for the HTTP API (api/server.py).
// All timestamps are epoch milliseconds (number) or null.

export type Ms = number | null;
export type Severity = 'high' | 'medium' | 'low' | 'info';

export class ApiError extends Error {
  status: number;
  detail: string;
  constructor(status: number, detail: string) {
    super(detail);
    this.status = status;
    this.detail = detail;
  }
}

export type ParamValue = string | number | boolean | null | undefined | Array<string | number>;
export type Params = Record<string, ParamValue>;

export function qs(params: Params = {}): string {
  const sp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === null || v === undefined || v === '') continue;
    if (Array.isArray(v)) v.forEach((x) => sp.append(k, String(x)));
    else sp.append(k, String(v));
  }
  const s = sp.toString();
  return s ? `?${s}` : '';
}

async function parse<T>(res: Response): Promise<T> {
  const text = await res.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text;
  }
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    if (body && typeof body === 'object' && 'detail' in body) {
      const d = (body as { detail: unknown }).detail;
      detail = typeof d === 'string' ? d : JSON.stringify(d);
    } else if (typeof body === 'string' && body) {
      detail = body.slice(0, 500);
    }
    throw new ApiError(res.status, detail);
  }
  return body as T;
}

/* ------------------------------------------------------- run scope (Revision 6) */

let runScope: string | null = null;
/** The run the UI is scoped to (from the URL `?run=`); null = all runs. Set by the shell. */
export function setRunScope(run: string | null) {
  runScope = run || null;
}
export const getRunScope = () => runScope;
/** Endpoints that accept `run=`: datasets, graph, hotspots (others are cluster-wide). */
const RUN_AWARE = /^\/clusters\/[^/]+\/(datasets\/|graph$|hotspots$|gantt$|flow$|errors$|spill-shuffle$)/;

async function get<T>(path: string, params?: Params, signal?: AbortSignal): Promise<T> {
  let res: Response;
  if (runScope && RUN_AWARE.test(path) && !(params && 'run' in params)) params = { ...(params ?? {}), run: runScope };
  try {
    res = await fetch(`/api${path}${qs(params)}`, { signal });
  } catch (e) {
    if ((e as Error).name === 'AbortError') throw e;
    throw new ApiError(0, 'Cannot reach the analyzer API. Start it with `dbx-log-analyzer ui` (port 8765).');
  }
  return parse<T>(res);
}

async function post<T>(path: string, body: unknown): Promise<T> {
  let res: Response;
  try {
    res = await fetch(`/api${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch {
    throw new ApiError(0, 'Cannot reach the analyzer API. Start it with `dbx-log-analyzer ui` (port 8765).');
  }
  return parse<T>(res);
}

const enc = encodeURIComponent;

/* ------------------------------------------------------------------ types */

export interface Counts {
  files: number; log_lines: number; events: number; apps: number; spark_jobs: number; failed_jobs: number;
  stages: number; failed_stages: number; tasks: number; failed_tasks: number; sql_queries: number;
  failed_queries: number; executors: number; executors_lost: number; log_errors: number; error_lines: number;
  warn_lines: number; signals: number; findings: number; story_rows: number; story_dropped: number;
  malformed_event_lines: number;
}

export interface Totals {
  mem_spill: number; disk_spill: number; gc_share: number | null; input_bytes: number; shuffle_read: number;
  shuffle_write: number; output_bytes: number; task_ms: number;
}

export interface SeverityCounts { high: number; medium: number; low: number }

export type LinkType = 'stage' | 'query' | 'executor' | 'job' | 'finding' | 'log' | 'error';

export interface DiagLink {
  type: LinkType;
  label: string;
  spark_context_id?: string | null;
  id?: string | number | null;
  attempt?: number | null;
  file_path?: string | null;
  seq?: number | null;
}

export interface DiagnosisStep {
  step: number;
  kind: 'outcome' | 'first_error' | 'root_cause' | 'performance' | 'code_location' | 'next_steps' | string;
  severity: Severity;
  title: string;
  text: string;
  links: DiagLink[];
}

export type RunStatus = 'failed' | 'succeeded' | 'unknown';

/** A piece of the logs that is not there, and the numbers it makes partial or unknown (analysis.coverage). */
export interface CoverageItem { kind: string; text: string; affects: string; from?: Ms; to?: Ms }

export interface Summary {
  cluster_id: string;
  coverage?: CoverageItem[];
  built_at: string;
  input_dir: string;
  tool_version: string;
  empty_reason: string | null;
  status: RunStatus;
  start_time: Ms;
  end_time: Ms;
  duration_ms: number | null;
  spark_versions: string[];
  counts: Partial<Counts>;
  totals: Partial<Totals>;
  findings_by_severity: Partial<SeverityCounts>;
  rows: Record<string, number>;
  diagnosis: DiagnosisStep[];
  /** Revision 3: cluster_info block (one object, or one per Spark context). May be absent on older builds. */
  cluster_info?: Partial<ClusterInfoRow> | Partial<ClusterInfoRow>[] | null;
  /** Revision 3: retry summary (null/absent on older builds). */
  retries?: RetrySummary | null;
  /** Revision 6: runs on this cluster. */
  runs?: { count: number; default_run: string | null; note: string | null; overlapping: number } | null;
  /** worker core-seconds paid for against those of successful tasks */
  core_use?: { worker_core_s: number; useful_task_s: number; core_s_per_useful: number } | null;
  init_scripts?: { node_starts: number; scripts: string[]; with_errors: number; same_output_everywhere: boolean; start_to_executor_ms: [number, number] | null } | null;
}

/** Revision 5: a peak or hotspot (when and where skew, shuffle, spill or GC peaked). */
export type HotspotKind = 'skew_task' | 'slowest_task' | 'shuffle_peak' | 'spill_peak' | 'gc_peak';
export interface Hotspot {
  cluster_id: string; spark_context_id: string; kind: HotspotKind; ts_start: Ms; ts_end: Ms;
  stage_id: number | null; stage_attempt: number | null; spark_job_id: number | null; sql_execution_id: number | null;
  query_description: string | null; operator: string | null; task_id: number | null; task_index: number | null;
  executor_id: string | null; host: string | null; value: number | null; baseline: number | null; ratio: number | null;
  share: number | null; cause: 'data_skew' | 'slow_executor' | 'gc' | 'gc_stuck' | 'lost' | 'unknown' | null; detail: string;
  run_key?: string | null; affected_runs?: string[] | null;
}

/** Revision 5: plan compare candidates, grouped. */
export interface PlanCandidate {
  group: 'same_query_other_run' | 'same_query_this_run' | 'other_query';
  cluster_id: string; spark_context_id: string; sql_execution_id: number; description: string | null;
  status: string | null; start_time: Ms; duration_ms: number | null; plan_hash: string | null; same_plan_hash: boolean;
  run_status: string | null; run_start_time: Ms;
}

/** Revision 6: one run unit (job task run, job run, Connect session, notebook, job group or Spark app). */
export interface RunRow {
  /** "history": usual is the median of the same task on the same table in earlier runs (usual_runs of them) */
  usual_from?: 'history' | null; usual_runs?: number | null;
  cluster_id: string; spark_context_id: string; run_key: string;
  kind: 'task_run' | 'job_group' | 'connect_session' | 'notebook' | 'app' | string;
  label: string; databricks_job_id: string | null; databricks_run_id: string | null; task_run_id: string | null;
  notebook_path: string | null; user: string | null; start_time: Ms; end_time: Ms; duration_ms: number | null;
  status: 'succeeded' | 'failed' | 'incomplete' | string; spark_jobs: number; stages: number; tasks: number;
  failed_tasks: number; retried_tasks: number; disk_spill: number | null; mem_spill: number | null;
  shuffle_read: number | null; shuffle_write: number | null; findings: number; max_severity: Severity | null;
  overlapping_runs: string[] | null;
}
/** On the cluster's clock: time with at least one run waiting for a free core, running tasks, and both at once. */
export interface RunClock { waiting_ms: number; running_ms: number; both_ms: number }
export interface RunsResponse { runs: RunRow[]; default_run: string | null; note: string | null; clock?: RunClock | null }

export interface RetrySummary {
  tasks?: number; stages?: number; failed_attempts?: number; still_failed?: number; wasted_ms?: number;
  by_category?: Record<string, number>; examples?: unknown[];
}

export interface ClusterListItem {
  cluster_id: string;
  built_at: string;
  status: RunStatus;
  start_time: Ms;
  end_time: Ms;
  duration_ms: number | null;
  counts: Partial<Counts>;
  findings_by_severity: Partial<SeverityCounts>;
  empty_reason: string | null;
  /** the raw logs it was built from are still on disk, so it can be analyzed again */
  raw_available?: boolean;
}

export interface Table<R> {
  columns: string[];
  rows: R[];
  total: number;
  limit: number;
  offset: number;
}

export interface AppRow {
  cluster_id: string; spark_context_id: string; app_id: string | null; app_name: string | null;
  spark_version: string | null; user: string | null; start_time: Ms; end_time: Ms; duration_ms: number | null;
  eventlog_files: number | null; events_read: number | null; malformed_lines: number | null;
}

export interface LogLine {
  cluster_id: string; source: string; app_id: string | null; executor_id: string | null; file_path: string;
  file_name: string; seq: number; line_no: number; ts: Ms; level: string | null; logger: string | null;
  message: string | null; line: string; signal: string | null;
  /** Revision 3: multi-line log4j message continuation (inherits ts/level/logger). */
  continuation?: boolean | null;
}

export interface TaskRow {
  cluster_id: string; spark_context_id: string; stage_id: number; stage_attempt: number; task_id: number;
  task_attempt: number; executor_id: string | null; host: string | null; launch_time: Ms; finish_time: Ms;
  task_ms: number | null; run_ms: number | null; gc_ms: number | null; peak_mem: number | null;
  mem_spill: number | null; disk_spill: number | null; input_bytes: number | null; input_records: number | null;
  output_bytes: number | null; shuffle_read: number | null; shuffle_write: number | null; failed: boolean | null;
  end_reason: string | null; error: string | null;
  task_index?: number | null; speculative?: boolean | null;
}

export interface StageRow {
  cluster_id: string; spark_context_id: string; stage_id: number; stage_attempt: number; stage_name: string | null;
  num_tasks: number | null; status: 'succeeded' | 'failed' | 'incomplete' | string | null; start_time: Ms; end_time: Ms;
  duration_ms: number | null; failure_reason: string | null; tasks: number | null; failed_tasks: number | null;
  p50_task_ms: number | null; max_task_ms: number | null;
  /** fastest task of the stage attempt; absent on outputs built before it was added */
  min_task_ms?: number | null;
  /** Revision 11: rows read and written; data each task read (storage input + shuffle read) and its skew */
  shuffle_read_records?: number | null; shuffle_write_records?: number | null; output_records?: number | null;
  min_task_bytes_in?: number | null; p50_task_bytes_in?: number | null; max_task_bytes_in?: number | null;
  min_task_rows_in?: number | null; p50_task_rows_in?: number | null; max_task_rows_in?: number | null;
  data_skew?: number | null;
  skew: number | null; gc_share: number | null;
  max_peak_mem: number | null; mem_spill: number | null; disk_spill: number | null; input_bytes: number | null;
  input_records: number | null; output_bytes: number | null; shuffle_read: number | null; shuffle_write: number | null;
  executors_used: number | null; spark_job_id: number | null; sql_execution_id: number | null;
  job_description: string | null;
  /** Revision 3: failure_reason of the previous attempt (stage_attempt > 0). */
  retry_of_failure?: string | null;
  /** Revision 4: Stage Info `Parent IDs`, RDD names / operator scopes and the call-site `Details`. */
  parent_ids?: number[] | null;
  rdd_names?: string[] | null;
  rdd_scopes?: string[] | null;
  details?: string | null;
}

export interface SparkJobRow {
  cluster_id: string; spark_context_id: string; spark_job_id: number; start_time: Ms; end_time: Ms;
  duration_ms: number | null; result: string | null; error: string | null; stage_ids: number[] | null;
  num_stages: number | null; sql_execution_id: number | null; description: string | null; job_group: string | null;
  call_site: string | null; databricks_job_id: string | null; databricks_run_id: string | null;
  notebook_path: string | null;
  job_tags?: string | string[] | null; databricks_task_run_id?: string | null; connect_operation_id?: string | null;
}

export interface SqlQueryRow {
  cluster_id: string; spark_context_id: string; sql_execution_id: number; start_time: Ms; end_time: Ms;
  duration_ms: number | null; status: string | null; description: string | null; details: string | null;
  error: string | null; stages: number | null; tasks: number | null; disk_spill: number | null;
  max_stage_skew: number | null; input_bytes: number | null; shuffle_read: number | null; plan_hash: string | null;
  final_plan?: string | null; initial_plan?: string | null;
  /** Revision 4: operator summary and tables parsed from the final plan. */
  operators?: string[] | null; tables_read?: string[] | null; tables_written?: string[] | null;
}

export interface FindingRow {
  finding_id: string; cluster_id: string; spark_context_id: string | null; severity: Severity; category: string;
  entity: string | null; evidence: string | null; fix: string | null; ts: Ms; stage_id: number | null;
  stage_attempt: number | null; spark_job_id: number | null; sql_execution_id: number | null;
  executor_id: string | null; signal: string | null; fingerprint: string | null; log_file_path: string | null;
  log_seq: number | null;
}

export interface IncidentRow {
  cluster_id: string; finding_id: string; incident_id: string; incident_rank: number; incident_title: string;
  incident_severity: Severity; incident_impact: string | null; incident_start: Ms; incident_end: Ms; problem_id: string;
  kind: string; role: 'root' | 'contributing' | 'effect' | 'related' | 'same'; caused_by: string | null;
  because: string | null; confidence: 'strong' | 'likely' | 'weak' | null; spark_context_id: string | null;
  stage_id: number | null; stage_attempt: number | null; spark_job_id: number | null; sql_execution_id: number | null;
  executor_id: string | null; stages: string | null; executors: string | null;
}

export interface TimelineRow { cluster_id: string; minute: number; signal: string; count: number }

export interface QueryProfileRow {
  cluster_id: string; spark_context_id: string; sql_execution_id: number; description: string | null;
  status: string | null; start_time: Ms; end_time: Ms; duration_ms: number | null; error: string | null;
  spark_jobs: number | null; stages: number | null; total_stage_ms: number | null; max_stage_ms: number | null;
  tasks: number | null; failed_tasks: number | null; mem_spill: number | null; disk_spill: number | null;
  gc_share: number | null; max_stage_skew: number | null; input_bytes: number | null; shuffle_read: number | null;
  shuffle_write: number | null; output_bytes: number | null; executors_used: number | null;
  executors_lost: number | null; findings: number | null; max_severity: Severity | null; plan_hash: string | null;
}

export interface StageExecutorRow {
  cluster_id: string; spark_context_id: string; stage_id: number; stage_attempt: number; executor_id: string | null;
  host: string | null; tasks: number | null; failed_tasks: number | null; task_ms_sum: number | null;
  task_ms_max: number | null; run_ms: number | null; gc_ms: number | null; mem_spill: number | null;
  disk_spill: number | null; input_bytes: number | null; shuffle_read: number | null;
  share_of_stage_ms: number | null;
}

export interface ExecutorProfileRow {
  cluster_id: string; spark_context_id: string | null; app_id: string | null; executor_id: string; host: string | null;
  cores: number | null; added_time: Ms; removed_time: Ms; lifetime_ms: number | null; removed_reason: string | null;
  removal_category: RemovalCategory | null; removed_reason_raw?: string | null; tasks: number | null; failed_tasks: number | null;
  busy_ms: number | null; busy_share: number | null; run_ms: number | null; gc_ms: number | null;
  gc_share: number | null; mem_spill: number | null; disk_spill: number | null; max_peak_mem: number | null;
  log_lines: number | null; log_errors_lines: number | null; log_warn_lines: number | null; signals: number | null;
  top_signals: string[] | null; exceptions: number | null; top_exception: string | null;
  gc_pauses?: number | null; gc_pause_ms?: number | null; full_gcs?: number | null; max_heap_after_mb?: number | null;
}

export type RemovalCategory = 'autoscale' | 'termination' | 'oom' | 'killed' | 'lost' | 'other' | string;

export type StoryKind =
  | 'app_start' | 'app_end' | 'job_start' | 'job_end' | 'stage_start' | 'stage_end' | 'stage_failed'
  | 'query_start' | 'query_end' | 'query_failed' | 'executor_added' | 'executor_removed' | 'log_error'
  | 'log_signal' | 'finding' | 'task_retry' | 'stage_retry';

export const STORY_KINDS: StoryKind[] = [
  'app_start', 'app_end', 'job_start', 'job_end', 'stage_start', 'stage_end', 'stage_failed', 'query_start',
  'query_end', 'query_failed', 'executor_added', 'executor_removed', 'task_retry', 'stage_retry', 'log_error', 'log_signal', 'finding',
];

export interface StoryRow {
  cluster_id: string; spark_context_id: string | null; story_seq: number; ts: Ms; kind: StoryKind | string;
  severity: Severity; title: string; detail: string | null; count: number; spark_job_id: number | null;
  stage_id: number | null; stage_attempt: number | null; sql_execution_id: number | null;
  executor_id: string | null; source: string | null; log_file_path: string | null; log_seq: number | null;
  finding_id: string | null;
}

export interface HierarchyJob extends SparkJobRow {
  status: string | null;
  stages: StageRow[];
}
export interface HierarchyApp extends AppRow {
  jobs: HierarchyJob[];
  queries: QueryProfileRow[];
  orphan_stages: StageRow[];
}
export interface Hierarchy { apps: HierarchyApp[] }

export interface TaskSummary { min: number | null; p25: number | null; p50: number | null; p75: number | null; p90: number | null; p99: number | null; max: number | null }

export interface TaskColumnStat { p50: number | null; p95: number | null; max: number | null; sum: number | null; nonzero: number; min?: number | null; p10?: number | null; p90?: number | null }
/** Per-task column stats over every task of a stage attempt (task table summary rows). */
export type TaskColumnStats = Partial<Record<'task_ms' | 'gc_ms' | 'peak_mem' | 'mem_spill' | 'disk_spill' | 'input_bytes' | 'shuffle_read' | 'shuffle_write' | 'output_bytes' | 'cpu_ms' | 'run_ms', TaskColumnStat>>;

/** One task on the stage task timeline (stage detail, revision 10). */
export interface TaskTimelineRow {
  task_id: number; task_index: number | null; task_attempt: number | null; executor_id: string | null;
  launch_time: number | null; task_ms: number | null; failed: boolean | null; gc_ms: number | null; peak_mem: number | null;
  mem_spill: number | null; disk_spill: number | null; shuffle_read: number | null; input_bytes: number | null;
}

export interface StageDetail {
  stage: StageRow;
  tasks_summary: TaskSummary | null;
  task_columns?: TaskColumnStats;
  /** Revision 5: the stage's skewed tasks (hotspots kind skew_task). */
  hotspots?: Hotspot[];
  task_durations: number[];
  task_timeline?: TaskTimelineRow[];
  task_timeline_sampled?: boolean;
  sample?: boolean;
  sampled?: boolean;
  executors: StageExecutorRow[];
  findings: FindingRow[];
  job: SparkJobRow | null;
  query: SqlQueryRow | null;
}

export interface QueryDetail {
  query: SqlQueryRow;
  profile: QueryProfileRow | null;
  jobs: SparkJobRow[];
  stages: StageRow[];
  findings: FindingRow[];
  /** Revision 15: did it use adaptive execution and Photon, and if not, why */
  engine?: EngineUse | null;
  /** Revision 20: what it joins on and filters by */
  logic?: PlanLogic | null;
}
export interface PlanLogic {
  joins: { node: number; how: string; type: string | null; left_keys: string[]; right_keys: string[]; null_safe: boolean; condition: string | null }[];
  filters: { node: number; condition: string }[];
  scans: { node: number; table: string | null; partition_filters: string[]; data_filters: string[]; pushed_filters: string[] }[];
  groups: { node: number; keys: string[] }[];
  windows: { node: number; partition_by: string[]; order_by: string[]; keeps: string }[];
  /** plan signals in a sentence: Change Data Feed, optimized write, deletion vectors */
  facts?: string[];
  /** the joins come from the initial plan: adaptive execution replaced them (an empty side) */
  from_initial?: boolean;
}
export interface EngineState { state: 'used' | 'no' | 'na'; why: string; share?: number | null }
export interface EngineUse { aqe: EngineState; photon: EngineState }
export interface EngineCount { queries: number; could: number; used: number; why_not: [string, number][] }

export interface DiffLine { op: 'equal' | 'insert' | 'delete'; a_line: number | null; b_line: number | null; text: string }
export interface PlanDiff { a: Partial<SqlQueryRow>; b: Partial<SqlQueryRow>; diff: DiffLine[] }

export interface GanttJob { spark_job_id: number; start: Ms; end: Ms; result: string | null; description: string | null; stage_ids: number[] | null }
export interface GanttStage { stage_id: number; stage_attempt: number; start: Ms; end: Ms; status: string | null; name: string | null }
export interface GanttExecutor { executor_id: string; host: string | null; cores?: number | null; added: Ms; removed: Ms; removed_reason: string | null; removal_category: string | null }
export interface GanttTask {
  task_id: number; stage_id: number; stage_attempt: number; executor_id: string | null; start: Ms; end: Ms; failed: boolean | null;
  /** per-task metrics (absent on older builds) */
  run_ms?: number | null; cpu_ms?: number | null; gc_ms?: number | null; peak_mem?: number | null; mem_spill?: number | null;
  disk_spill?: number | null; input_bytes?: number | null; shuffle_read?: number | null; shuffle_write?: number | null; fetch_wait_ms?: number | null;
}
export interface GanttMarker {
  ts: number; executor_id: string | null;
  kind: 'oom' | 'executor_lost' | 'executor_added' | 'executor_removed' | 'executor_killed' | 'full_gc' | 'spill' | 'signal' | 'error' | string;
  label: string;
}
export interface Gantt {
  ctx: string; start: Ms; end: Ms; jobs: GanttJob[]; stages: GanttStage[]; executors: GanttExecutor[];
  tasks: GanttTask[]; tasks_total: number; sampled: boolean; markers: GanttMarker[];
  /** Revision 12: when each executor ran at least one task (exact, unlike a sampled task list) */
  busy?: { executor_id: string; busy_start: number; busy_end: number }[] | null;
}

export interface Facet { value: string | number | null; count: number }

export interface ErrorGroup {
  fingerprint: string; exception_class: string; occurrences: number; executors_affected: number;
  first_seen: Ms; last_seen: Ms; sample_message: string | null; sample_stack: string[] | string | null;
  user_frame: string | null; sources: string[] | string | null; sample_file_path: string | null; sample_seq: number | null;
  /** Revision 15: where its lines happened. Executor lines: the stage attempts with a task on that executor then
   * (the failed ones when a task failed); driver lines: the runs going at the time. */
  hit_runs?: { run_key: string; lines: number }[];
  hit_stages?: { run_key: string | null; spark_context_id: string; stage_id: number; stage_attempt: number; sql_execution_id: number | null; lines: number }[];
}

export interface SignalGroup {
  signal: string; severity: Severity; fix: string | null; occurrences: number; executors_affected: number;
  first_seen: Ms; last_seen: Ms; sample_line: string | null; sample_file_path: string | null; sample_seq: number | null;
}

export interface DownloadResult { download: Record<string, unknown>; summary: Summary | null }

/* ---------------------------------------------------- Revision 2: sources */

export interface SourceField { name: string; label: string; placeholder?: string | null; required?: boolean; secret?: boolean }
export interface SourceInfo { type: string; label: string; available: boolean; reason: string | null; fields: SourceField[] }
export interface SourceCluster { cluster_id: string; last_modified: Ms; analyzed: boolean }
export interface SourceRequest { type: string; root: string; options: Record<string, string> }
export interface IngestResult { download: Record<string, unknown> | null; summary: Summary }

/* ---------------------------------------------------- Raw data steps */

export interface StepInfo { id: string; group: string; step: string; title: string; description: string | null; rows: number | null }

/* -------------------------------------------------- Revision 3: datasets */

export interface ClusterInfoRow {
  cluster_id: string; spark_context_id: string | null; cluster_name: string | null; cluster_creator?: string | null;
  spark_version: string | null; driver_node_type: string | null; worker_node_type: string | null;
  min_workers: number | null; max_workers: number | null; target_workers: number | null;
  cluster_scaling_type: string | null; runtime_engine: string | null; cloud_provider: string | null; region: string | null;
  workload_type: string | null; databricks_job_id: string | null; job_run_id: string | null; task_run_id: string | null;
  parent_run_id: string | null;
}

export interface GcEventRow {
  cluster_id: string; source: string; app_id: string | null; executor_id: string | null; file_path: string; seq: number;
  ts: Ms; gc_id: number | null; kind: string | null; cause: string | null; heap_before_mb: number | null;
  heap_after_mb: number | null; heap_total_mb: number | null; pause_ms: number | null;
}

export interface ConnectOperationRow {
  cluster_id: string; spark_context_id: string | null; operation_id: string; session_id: string | null;
  user_id: string | null; user_name: string | null; statement_text: string | null; job_tag: string | null;
  start_time: Ms; analyzed_time: Ms; ready_time: Ms; finish_time: Ms; closed_time: Ms; duration_ms: number | null;
  status: 'finished' | 'failed' | 'canceled' | 'open' | string | null; error: string | null;
}

export type RetryCategory = 'executor_lost' | 'oom' | 'fetch_failed' | 'exception' | 'killed' | 'other' | string;

export interface TaskRetryRow {
  cluster_id: string; spark_context_id: string; stage_id: number; stage_attempt: number; task_index: number;
  attempts: number | null; first_attempt_executor_id: string | null; first_attempt_host: string | null;
  first_failure_reason: string | null; first_failure_error: string | null; first_failure_category: RetryCategory | null;
  first_failure_time: Ms; executor_removed_reason: string | null; final_status: 'succeeded' | 'failed' | string | null;
  final_executor_id: string | null; retry_delay_ms: number | null; wasted_ms: number | null;
  spark_job_id: number | null; sql_execution_id: number | null;
  failed_attempts?: number | null;
  /** backend's own plain-English sentence (preferred when present) */
  explanation?: string | null;
}

export interface EventCountRow { cluster_id: string; spark_context_id: string | null; event: string; count: number }

/* -------------------------------------------------- Revision 4: graph */

export type GraphNodeType = 'app' | 'job' | 'stage' | 'query' | 'connect';
export type GraphFlag = 'failed' | 'retried' | 'spill' | 'skew' | 'gc' | 'shuffle_heavy' | string;
export type GraphEdgeKind = 'contains' | 'depends' | 'retry' | 'runs_query' | 'from_connect' | string;

export interface GraphMetrics {
  tasks?: number | null; failed_tasks?: number | null; attempt?: number | null; attempts?: number | null;
  disk_spill?: number | null; mem_spill?: number | null; shuffle_read?: number | null; shuffle_write?: number | null;
  input_bytes?: number | null; output_bytes?: number | null; gc_share?: number | null; skew?: number | null;
  /** task durations behind the skew: fastest, median, slowest */
  min_task_ms?: number | null; p50_task_ms?: number | null; max_task_ms?: number | null;
  /** Revision 11: rows read and written, and the data each task read (min / median / max) behind data_skew */
  input_records?: number | null; shuffle_read_records?: number | null; shuffle_write_records?: number | null;
  output_records?: number | null; data_skew?: number | null;
  min_task_bytes_in?: number | null; p50_task_bytes_in?: number | null; max_task_bytes_in?: number | null;
  min_task_rows_in?: number | null; p50_task_rows_in?: number | null; max_task_rows_in?: number | null;
  /** task_retries rows for this stage attempt */
  retries?: number | null;
}

export interface GraphWhat {
  description?: string | null; call_site?: string | null; notebook_path?: string | null; sql_description?: string | null;
  operators?: string[] | null; tables_read?: string[] | null; tables_written?: string[] | null;
  statement_text?: string | null; rdd_scopes?: string[] | null;
}

export interface GraphNode {
  /** app:<ctx> | job:<ctx>:<job> | stage:<ctx>:<stage>:<attempt> | query:<ctx>:<exec> | connect:<ctx>:<op> */
  id: string;
  type: GraphNodeType;
  label: string;
  sublabel?: string | null;
  status?: string | null;
  start?: Ms; end?: Ms; duration_ms?: number | null;
  /** id of the containing job/app, or null */
  parent?: string | null;
  metrics?: GraphMetrics | null;
  flags?: GraphFlag[] | null;
  what?: GraphWhat | null;
  /** a stage: the parent stages whose shuffle output it read, with the rows and bytes each wrote */
  from_stages?: { stage_id: number; attempt: number; rows: number | null; bytes: number | null; reused: boolean }[];
}

/** depends edges: rows / bytes the parent stage wrote to the shuffle, which the child reads */
export interface GraphEdge { source: string; target: string; kind: GraphEdgeKind; label?: string | null; rows?: number | null; bytes?: number | null }

export interface Graph {
  ctx: string;
  start: Ms;
  end: Ms;
  nodes: GraphNode[];
  edges: GraphEdge[];
  truncated?: boolean;
  dropped_stages?: number;
  /** set by the UI when the graph was rebuilt from /hierarchy because /graph is missing */
  derived?: boolean;
}

/** One bucket of `spill_shuffle_timeline`, keyed by executor or stage. */
export interface SpillShuffleBucket {
  spark_context_id?: string | null;
  minute: number;
  /** backend bucket start (epoch ms); normalized into `minute` by the UI */
  ts?: number | null;
  executor_id?: string | null;
  stage_id?: number | null;
  stage_attempt?: number | null;
  /** UI-normalized lane key (executor id, or "stage.attempt") */
  key?: string;
  tasks?: number | null;
  mem_spill?: number | null; disk_spill?: number | null; shuffle_read?: number | null; shuffle_write?: number | null;
  input_bytes?: number | null; output_bytes?: number | null; gc_ms?: number | null; run_ms?: number | null;
}

/** Raw /spill-shuffle response: accepted as a bare list, `{buckets}`, `{rows}` (table envelope). */
export interface SpillShuffleSeries extends Omit<SpillShuffleBucket, 'minute'> {
  key: string; label?: string | null; first?: Ms; last?: Ms;
}
export type SpillShuffleRaw =
  | SpillShuffleBucket[]
  | {
      ctx?: string | null; contexts?: string[]; by?: string; bucket_ms?: number; bucket_seconds?: number; start?: Ms; end?: Ms;
      series?: SpillShuffleSeries[]; buckets?: SpillShuffleBucket[]; rows?: SpillShuffleBucket[]; totals?: Partial<SpillShuffleBucket>;
    };

/** Every dataset the Raw data tab lists (older backends may not have the Revision 3 ones). */
export const DATASETS: { name: string; note: string }[] = [
  { name: 'files', note: 'Every file found in the cluster folder' },
  { name: 'file_lines', note: 'Line, level and thread-dump counts per log file' },
  { name: 'apps', note: 'Spark applications, one per Spark context' },
  { name: 'cluster_info', note: 'Cluster name, runtime, node types and job ids per Spark context' },
  { name: 'event_counts', note: 'Count of every event type in the event logs, including skipped ones' },
  { name: 'log_lines', note: 'Every driver and executor log line' },
  { name: 'log_signals', note: 'Log lines that matched a known problem pattern' },
  { name: 'log_errors', note: 'Exceptions with their top stack frames' },
  { name: 'gc_events', note: 'JVM garbage-collection pauses parsed from stdout' },
  { name: 'tasks', note: 'Every task attempt' },
  { name: 'task_retries', note: 'Tasks that failed at least once, and what happened next' },
  { name: 'stages', note: 'Every stage attempt with task statistics' },
  { name: 'spark_jobs', note: 'Spark jobs with their stages and properties' },
  { name: 'sql_queries', note: 'SQL and DataFrame executions' },
  { name: 'connect_operations', note: 'Spark Connect statements (shared clusters)' },
  { name: 'executors', note: 'Executors added and removed' },
  { name: 'executor_profile', note: 'Executors joined with tasks, GC and logs' },
  { name: 'stage_executor_profile', note: 'Task time per stage, per executor' },
  { name: 'query_profile', note: 'Queries joined with jobs, stages and findings' },
  { name: 'findings', note: 'Detected problems with evidence and fix' },
  { name: 'incidents', note: 'Findings grouped into incidents: root cause, effects and why they are linked' },
  { name: 'timeline', note: 'Log signals per minute' },
  { name: 'run_story', note: 'Everything that happened, in time order' },
  { name: 'spill_shuffle_timeline', note: 'Spill and shuffle bytes per minute, executor and stage' },
  { name: 'hotspots', note: 'When and where skew, shuffle, spill and GC peaked, and why' },
  { name: 'runs', note: 'Runs on this cluster (job runs, notebooks, Connect sessions) and which overlapped' },
];

/** True when the backend does not have this endpoint/dataset yet (or the cluster/dataset is unknown). */
export function isMissing(e: unknown): boolean {
  return e instanceof ApiError && (e.status === 404 || e.status === 405 || e.status === 501);
}

/** Resolve to `null` instead of throwing when the endpoint or dataset is missing. */
export async function optional<T>(p: Promise<T>): Promise<T | null> {
  try {
    return await p;
  } catch (e) {
    if (isMissing(e)) return null;
    throw e;
  }
}

/* -------------------------------------------------------------- endpoints */

const c = (cid: string) => `/clusters/${enc(cid)}`;

export const api = {
  health: () => get<{ ok: boolean; version: string }>('/health'),
  clusters: () => get<ClusterListItem[]>('/clusters'),
  analyze: (body: { log_root: string; cluster_id: string } | { cluster_dir: string }) => post<Summary>('/analyze', body),
  reanalyze: (cid: string) => post<Summary>(`${c(cid)}/reanalyze`, {}),
  download: (body: { volume: string; cluster_id: string; profile: string | null; build: boolean }) =>
    post<DownloadResult>('/download', body),
  summary: (cid: string, s?: AbortSignal) => get<Summary>(`${c(cid)}/summary`, undefined, s),
  hierarchy: (cid: string, s?: AbortSignal) => get<Hierarchy>(`${c(cid)}/hierarchy`, undefined, s),
  dataset: <R>(cid: string, name: string, params?: Params, s?: AbortSignal) =>
    get<Table<R>>(`${c(cid)}/datasets/${enc(name)}`, params, s),
  stage: (cid: string, ctx: string, stageId: number | string, attempt: number | string, s?: AbortSignal) =>
    get<StageDetail>(`${c(cid)}/stages/${enc(ctx)}/${enc(String(stageId))}/${enc(String(attempt))}`, undefined, s),
  query: (cid: string, ctx: string, id: number | string, s?: AbortSignal) =>
    get<QueryDetail>(`${c(cid)}/queries/${enc(ctx)}/${enc(String(id))}`, undefined, s),
  planDiff: (cid: string, p: { a_ctx: string; a_id: number | string; b_ctx: string; b_id: number | string; which: 'final' | 'initial'; b_cid?: string }, s?: AbortSignal) =>
    get<PlanDiff>(`${c(cid)}/plan-diff`, p, s),
  // Revision 5
  planCandidates: (cid: string, ctx: string, id: number | string, s?: AbortSignal) =>
    get<PlanCandidate[]>(`${c(cid)}/queries/${enc(ctx)}/${enc(String(id))}/plan-candidates`, undefined, s),
  hotspots: (cid: string, p: { ctx?: string | null; kind?: HotspotKind | null } = {}, s?: AbortSignal) =>
    get<Hotspot[]>(`${c(cid)}/hotspots`, p, s),
  // Revision 6
  runs: (cid: string, s?: AbortSignal) => get<RunsResponse>(`${c(cid)}/runs`, undefined, s),
  flow: (cid: string, s?: AbortSignal) => get<Flow>(`${c(cid)}/flow`, undefined, s),
  clusterView: <T,>(cid: string, s?: AbortSignal) => get<T>(`${c(cid)}/cluster-view`, undefined, s),
  settings: (cid: string, s?: AbortSignal) => get<SettingsView>(`${c(cid)}/settings`, undefined, s),
  top: (cid: string, p: { kind: TopKind; by: TopBy; run?: string; limit?: number; q?: string }, s?: AbortSignal) => get<TopView>(`${c(cid)}/top`, p, s),
  queryTime: (cid: string, ctx: string, query: number, s?: AbortSignal) => get<QueryTime>(`${c(cid)}/query-time`, { ctx, query }, s),
  taskColumns: (cid: string, ctx: string, scope: { stage?: number; attempt?: number; job?: number; query?: number }, s?: AbortSignal) =>
    get<TaskColumns>(`${c(cid)}/task-columns`, { ctx, ...scope }, s),
  runSteps: (cid: string, run: string, s?: AbortSignal) => get<RunSteps>(`${c(cid)}/run-steps`, { run }, s),
  stageWhy: (cid: string, ctx: string, by: { query?: number; job?: number }, s?: AbortSignal) => get<{ stages: StageWhy[] }>(`${c(cid)}/stage-why`, { ctx, ...by }, s),
  runEnd: (cid: string, run: string, s?: AbortSignal) => get<RunEnd>(`${c(cid)}/run-end`, { run }, s),
  queryLogic: (cid: string, ctx: string, id: number, s?: AbortSignal) => get<PlanLogic>(`${c(cid)}/query-logic`, { ctx, id: String(id) }, s),
  tables: (cid: string, s?: AbortSignal, scope?: { run?: string; ctx?: string; query?: number }) => get<{ tables: TableStats[] }>(`${c(cid)}/tables`, scope, s),
  runTables: (cid: string, run: string, s?: AbortSignal) => get<RunTables>(`${c(cid)}/run-tables`, { run }, s),
  group: (cid: string, runs: string[], s?: AbortSignal) => get<GroupView>(`${c(cid)}/group`, { runs: runs.join(',') }, s),
  gantt: (cid: string, ctx: string | null, maxTasks = 20000, s?: AbortSignal, win?: { start: number; end: number }) =>
    // with a window: every run's tasks in it (run: '' turns the run scope off), to show what shared the executors
    get<Gantt>(`${c(cid)}/gantt`, win ? { ctx, max_tasks: maxTasks, run: '', start: win.start, end: win.end } : { ctx, max_tasks: maxTasks }, s),
  logContext: (cid: string, filePath: string, seq: number, before = 20, after = 40, s?: AbortSignal) =>
    get<Table<LogLine>>(`${c(cid)}/logs/context`, { file_path: filePath, seq, before, after }, s),
  facets: (cid: string, name: string, column: string, s?: AbortSignal) =>
    get<Facet[]>(`${c(cid)}/facets/${enc(name)}`, { column }, s),
  errors: (cid: string, s?: AbortSignal) => get<ErrorGroup[]>(`${c(cid)}/errors`, undefined, s),
  signals: (cid: string, s?: AbortSignal) => get<SignalGroup[]>(`${c(cid)}/signals`, undefined, s),
  // Revision 2: source picker → cluster picker → ingest
  sources: (s?: AbortSignal) => get<SourceInfo[]>('/sources', undefined, s),
  sourceClusters: (body: SourceRequest) => post<SourceCluster[]>('/sources/clusters', body),
  ingest: (body: SourceRequest & { cluster_id: string }) => post<IngestResult>('/ingest', body),
  // Raw data
  steps: (cid: string, s?: AbortSignal) => get<StepInfo[]>(`${c(cid)}/steps`, undefined, s),
  step: <R = Record<string, unknown>>(cid: string, id: string, params?: Params, s?: AbortSignal) =>
    get<Table<R>>(`${c(cid)}/steps/${enc(id)}`, params, s),
  // Revision 4
  graph: (cid: string, ctx: string | null, s?: AbortSignal) => get<Graph>(`${c(cid)}/graph`, { ctx }, s),
  spillShuffle: (cid: string, ctx: string | null, by: 'executor' | 'stage', s?: AbortSignal) =>
    get<SpillShuffleRaw>(`${c(cid)}/spill-shuffle`, { ctx, by }, s),
  /** Dataset that may not exist on an older backend: resolves to null on 404. */
  datasetOpt: <R>(cid: string, name: string, params?: Params, s?: AbortSignal) =>
    optional(get<Table<R>>(`${c(cid)}/datasets/${enc(name)}`, params, s)),
};

/* ------------------------------------------------------------ Revision 13 */

/** How the data (storage input + shuffle read of each successful task) and the task time were spread over the tasks. */
export interface DataDist {
  min_task_bytes_in?: number | null; p10_task_bytes_in?: number | null; p50_task_bytes_in?: number | null;
  p90_task_bytes_in?: number | null; max_task_bytes_in?: number | null; avg_task_bytes_in?: number | null;
  min_task_rows_in?: number | null; p10_task_rows_in?: number | null; p50_task_rows_in?: number | null;
  p90_task_rows_in?: number | null; max_task_rows_in?: number | null; avg_task_rows_in?: number | null;
  p10_task_ms?: number | null; p50_task_ms?: number | null; p90_task_ms?: number | null;
  /** half of all bytes were read by tasks at least this big */
  wmed_task_bytes_in?: number | null;
}
export interface DataIO {
  input_bytes?: number | null; input_records?: number | null; output_bytes?: number | null; output_records?: number | null;
  shuffle_read?: number | null; shuffle_write?: number | null;
}
/** What an executor was given: heap, overhead and off-heap (MiB), unified (execution + storage) and storage memory (bytes). */
export interface ExecRes {
  resource_profile_id?: number | null; heap_mb?: number | null; overhead_mb?: number | null; offheap_mb?: number | null;
  unified_memory?: number | null; storage_memory?: number | null; task_cpus?: number | null;
}
export interface StageRow extends DataDist {}
export interface GraphMetrics extends DataDist {}
export interface SparkJobRow extends DataDist, DataIO {}
export interface SqlQueryRow extends DataDist, DataIO { root_execution_id?: number | null; photon_share?: number | null }
export interface RunRow extends DataDist, DataIO {
  same_job_runs?: number | null; typical_duration_ms?: number | null; vs_typical?: number | null;
  program?: string | null; subject?: string | null;
  /** stage attempts that failed in this run (retried or not) */
  failed_stages?: number;
  /** waiting for a free core (a stage submitted, none of its stages running) and running tasks, from its stages */
  waiting_ms?: number | null; running_ms?: number | null;
  /** time with its tasks queued on a full cluster: every core busy, wave after wave (also behind its own tasks) */
  queued_full_ms?: number | null;
  /** the biggest file read and the biggest shuffle read of one task */
  max_task_input?: number | null; max_task_shuffle?: number | null;
  /** the Databricks job run this task run belongs to, the job's name and the task type (notebook, python, jar…) */
  parent_run_id?: string | null; job_name?: string | null; task_type?: string | null;
}
export interface ExecutorProfileRow extends ExecRes {}

export interface FlowNode extends DataDist, DataIO {
  key: string; kind: 'query' | 'job'; ctx: string; id: number; run_key?: string | null; start: Ms; end: Ms;
  status: string | null; what: string | null; error: string | null; jobs: number[]; tables_read: string[];
  tables_written: string[]; disk_spill?: number | null; tasks?: number | null;
  /** stretches where a stage of it waited for a free core and none of its stages ran */
  waits?: [number, number][];
}
/** rows / bytes: what passed along it (a table: the rows its writer wrote; a reused shuffle: the rows that stage wrote) */
export interface FlowEdge {
  from: string; to: string; kind: 'inside' | 'table' | 'shuffle'; labels: string[]; rows?: number | null; bytes?: number | null;
  /** written by a MERGE with Change Data Feed: the rows include the change rows written next to the data */
  cdf?: boolean;
}
export interface Flow { nodes: FlowNode[]; edges: FlowEdge[]; truncated: boolean; start: Ms; end: Ms; run: string | null }


/** What else ran on a stage's executors while it ran. */
export interface StageSharingRow {
  run_key: string | null; stage_id: number; stage_attempt: number; tasks: number; task_ms: number | null; executors: number;
  spark_job_id: number | null; sql_execution_id: number | null; stage_name: string | null; same_run: boolean;
}
export interface StageSharing {
  start: number; end: number; executors: string[]; cores: number | null; slot_ms: number | null; own_task_ms: number;
  other_task_ms: number; other_stages: number; other_runs: number; rows: StageSharingRow[];
  /** Revision 15: the other stages' tasks on these executors in the stage's time (longest first, capped) */
  other_tasks?: OtherTask[]; other_task_count?: number;
  /** Revision 17: per executor, this stage's tasks (mine) and the other stages' tasks in its window */
  by_exec?: SharingExec[];
  /** each other stage's typical task inside the window (to say why one of its tasks was slow), the executors, this run */
  typical?: { stage_id: number; stage_attempt: number; p50_task_ms: number | null; p50_read: number | null; run_key: string | null }[];
  exec_info?: TaskColumns['executors'];
  run_key?: string | null;
}
export interface TaskColumns {
  n: number; total: number; sampled: boolean;
  cols: Partial<Record<string, (number | null)[]>>;
  present: Partial<Record<string, boolean>>;
  executors: { executor_id: string; host: string | null; cores: number | null; added_time: number | null; removed_time: number | null; removal_category: string | null }[];
  stage_waits: { stage_id: number; stage_attempt: number; spark_job_id: number | null; submitted: number; first_task: number; wait_ms: number; tasks: number | null }[];
  /** Revision 18: for a job or a query, what else ran on its executors over its time */
  sharing?: StageSharing | null;
}
export interface SharingExec {
  executor_id: string | null; mine: boolean | null; tasks: number; task_ms: number | null; input_bytes: number | null;
  shuffle_read: number | null; shuffle_write: number | null; mem_spill: number | null; disk_spill: number | null;
  gc_ms: number | null; peak_mem: number | null; failed: number | null; stages: number; runs: number;
}
export interface OtherTask {
  executor_id: string | null; launch_time: number | null; task_ms: number | null; stage_id: number; stage_attempt: number;
  task_id?: number; failed?: boolean | null; disk_spill?: number | null; run_key?: string | null;
  input_bytes?: number | null; shuffle_read?: number | null; gc_ms?: number | null;
}
export interface StageDetail { sharing?: StageSharing | null }

/** Revision 14: successful tasks per size band (storage input + shuffle read) and the bytes each band read. */
export interface SizeBands {
  tasks_none?: number | null; tasks_lt10?: number | null; tasks_10_128?: number | null; tasks_128_256?: number | null; tasks_ge256?: number | null;
  bytes_lt10?: number | null; bytes_10_128?: number | null; bytes_128_256?: number | null; bytes_ge256?: number | null;
}
export interface DataDist extends SizeBands {}
export interface PlacementRow extends SizeBands {
  executor_id: string; host: string | null; tasks: number; bytes_in: number | null; max_bytes_in: number | null; task_ms: number | null; max_task_ms: number | null;
  /** Revision 15: the spread of task time and data on this executor, its spill and GC */
  min_task_ms?: number | null; p10_task_ms?: number | null; p50_task_ms?: number | null; p90_task_ms?: number | null;
  p50_bytes_in?: number | null; p90_bytes_in?: number | null; disk_spill?: number | null; gc_ms?: number | null;
}
export interface StageDetail { placement?: PlacementRow[] | null }

export interface IdleStretch { start: number; end: number; idle_core_ms: number; executors: number; cores_max: number }
export interface ComputeUse {
  core_ms_up: number; core_ms_used: number; idle_share: number | null; idle_core_ms_in_stretches: number; stretches: IdleStretch[];
  stretch_count: number; executors_from: number; executors_to: number; first_task: number | null; last_task: number | null;
  cluster_start: number | null; cluster_end: number | null;
}
export interface SettingRow {
  key: string; label: string; group: string; value: string | null; cluster_value: string | null; session_values: string[] | null;
  default: string | null; source: 'cluster' | 'code' | 'default'; what: string;
}
export interface Advice {
  severity: 'high' | 'medium' | 'info'; key: string | null; title: string; evidence: string; change: string;
  /** Revision 18: the stages it is about (biggest first, up to 50) and how many there are */
  stages?: AdviceStage[]; stage_count?: number;
  /** Revision 18: as points: what the data shows, the likely cause, what to change; and the runs and queries it is about */
  facts?: string[]; cause?: string | null; fixes?: string[];
  runs?: { run_key: string; label: string | null }[];
  queries?: { spark_context_id: string; sql_execution_id: number; run_key: string | null; label: string | null }[];
}
export interface AdviceStage {
  spark_context_id: string; stage_id: number; stage_attempt: number; spark_job_id: number | null; sql_execution_id: number | null;
  run_key: string | null; run_label: string | null; duration_ms: number | null; tasks: number | null; input_bytes: number | null;
  shuffle_read: number | null; shuffle_write: number | null; disk_spill: number | null; max_task_bytes_in: number | null; wmed_task_bytes_in: number | null;
}
export type TopKind = 'stages' | 'jobs' | 'queries' | 'tasks';
export type TopBy = 'duration' | 'ran' | 'wait' | 'spill' | 'shuffle' | 'read' | 'tasks' | 'skew' | 'task_read' | 'task_shuffle';
export interface TopRow {
  spark_context_id: string; stage_id?: number | null; stage_attempt?: number | null; task_id?: number | null; spark_job_id?: number | null;
  sql_execution_id?: number | null; run_key?: string | null; executor_id?: string | null; status?: string | null; result?: string | null;
  failed?: boolean | null; start_time?: number | null; launch_time?: number | null; duration_ms?: number | null; task_ms?: number | null;
  max_task_input?: number | null; max_task_shuffle?: number | null;
  tasks?: number | null; input_bytes?: number | null; shuffle_read?: number | null; shuffle_write?: number | null; disk_spill?: number | null;
  gc_ms?: number | null; data_skew?: number | null; skew?: number | null; max_stage_skew?: number | null; label?: string | null; wait_ms?: number | null; ran_ms?: number | null; max_task_bytes_in?: number | null;
}
export interface TopView { kind: TopKind; by: TopBy; rows: TopRow[]; total: number; wait_known: boolean }
export interface SettingsView { settings: SettingRow[]; advice: Advice[]; env_vars: number | null; has_settings: boolean; compute: ComputeUse | null }

/** A group of runs at a glance: which problems hit how many of its runs (details are on the one-run pages). */
export interface GroupProblem { runs: number; failed_runs: number; count: number; run_keys: string[] }
export interface GroupFinding extends GroupProblem { category: string; signal: string | null; severity: Severity; example: string | null; fix: string | null }
export interface GroupError extends GroupProblem { exception_class: string | null; message: string | null; source: string | null; first: Ms; last: Ms }
export interface GroupRun { run_key: string; label: string; status: string; start_time: Ms; duration_ms: number | null; vs_typical: number | null; findings: number; errors: number }
export interface GroupView { runs: GroupRun[]; findings: GroupFinding[]; errors: GroupError[]; more_findings: number; more_errors: number; note: string | null }

/** How one run ended (Revision 15): the story of its end and the errors logged just before it. */
export interface RunEndLine { tone: 'ok' | 'bad' | 'warn' | 'info'; text: string }
export interface RunEndError {
  exception_class: string | null; fingerprint: string; lines: number; first: Ms; last: Ms; message: string | null; user_frame: string | null; sources: string[] | null; executors: string[] | null;
  /** how many runs of the same code logged it near their end, of how many */
  runs_with?: number; family_runs?: number;
}
export interface RunEnd {
  run_key: string; status: string; start_time: Ms; end_time: Ms; lines: RunEndLine[];
  last_query: { spark_context_id: string; sql_execution_id: number; description: string | null; status: string; start_time: Ms; end_time: Ms; duration_ms: number | null } | null;
  queries: number; spark_jobs: number; failed_queries: number; failed_jobs: number; replanned_jobs: number;
  errors: RunEndError[]; executors_gone: { executor_id: string; removed_time: Ms; removal_category: string | null; removed_reason: string | null }[];
  cluster_end: Ms;
  /** a job cluster that stopped right after a run that finished: its normal end */
  normal_end?: boolean;
  engine?: { aqe: EngineCount; photon: EngineCount };
}

/** Revision 15: time facts per stage that explain slowness: waiting for cores, the tail, others on its executors. */
export interface StageWhy { stage_id: number; stage_attempt: number; wait_ms: number | null; tail_ms: number | null; other_ms: number | null; slot_ms: number | null; other_share: number | null;
  by_exec?: { executor_id: string; tasks: number; task_ms: number | null }[] }

/** Revision 15: where a query's time went, over its stages and those of the queries that ran inside it. */
export interface QueryTime {
  query: number; total_ms: number | null; waiting_ms: number; running_ms: number; outside_ms: number;
  inner: { sql_execution_id: number; start: number; end: number; duration_ms: number; description: string | null; linked: boolean; tasks: number | null }[];
  /** Revision 16: the query's window and each stage step by step (its own and those of the queries inside it) */
  start?: number; end?: number; steps?: QueryStep[];
  causes?: Causes | null;
}
/** A run step by step: its queries (and jobs without SQL) in order, each with waiting / running time and its stages. */
export interface RunStepStage {
  stage_id: number; stage_attempt: number; spark_job_id: number | null; status: string | null; tasks: number | null;
  submitted: number; first_task: number; end: number; wait_ms: number; run_ms: number; disk_spill: number | null; failed_tasks: number | null;
  /** rows in from storage / from the shuffle (its parent stages), out to the shuffle (the next stage) / to storage */
  rows_read?: number | null; rows_from_shuffle?: number | null; rows_to_shuffle?: number | null; rows_written?: number | null; parent_ids?: number[];
  min_task_ms?: number | null; p10_task_ms?: number | null; p50_task_ms?: number | null; p90_task_ms?: number | null; max_task_ms?: number | null;
  p10_task_bytes_in?: number | null; p50_task_bytes_in?: number | null; p90_task_bytes_in?: number | null; max_task_bytes_in?: number | null;
  input_bytes?: number | null; input_records?: number | null; shuffle_read?: number | null; shuffle_write?: number | null; output_bytes?: number | null;
  mem_spill?: number | null; max_peak_mem?: number | null; gc_share?: number | null; executors_used?: number | null; skew?: number | null;
  cpu_share?: number | null; fetch_wait_ms?: number | null;
  by_exec?: { executor_id: string; tasks: number; failed: number | null; task_ms: number | null; bytes_in: number | null; disk_spill: number | null }[];
}
export interface RunStepGroup {
  kind: 'query' | 'job'; ctx: string; id: number | null; description: string | null; status: string | null;
  start: number; end: number; waiting_ms: number; running_ms: number; stages: RunStepStage[];
  tables_read?: string[]; tables_written?: string[]; operators?: string[]; input_records?: number | null; output_records?: number | null;
  tasks?: number | null; failed_tasks?: number | null; input_bytes?: number | null; shuffle_read?: number | null; shuffle_write?: number | null;
  output_bytes?: number | null; mem_spill?: number | null; disk_spill?: number | null;
  causes?: Causes | null;
}
/** Where task time went, summed over tasks: CPU, GC, waiting for shuffle data, the rest (storage, network, Python),
 * failed or cancelled attempts; disk spill in bytes. */
export interface Causes {
  task_ms: number; cpu_ms: number; gc_ms: number; fetch_wait_ms: number; shuffle_write_ms?: number; other_ms: number; failed_ms: number; disk_spill: number; has_cpu: boolean;
}
export interface RunSteps {
  run_key: string; start: number | null; end: number | null; total_ms: number | null; waiting_ms: number; running_ms: number;
  outside_ms: number | null; groups: RunStepGroup[]; gaps: [number, number][]; causes?: Causes | null;
  /** time with its tasks queued on a full cluster (see RunRow) */
  queued_full_ms?: number | null;
  /** Structured Streaming micro-batches of the run (from the query descriptions) */
  batches?: { stream: string; batch: number; start: number; end: number; queries: (number | null)[]; waiting_ms: number; running_ms: number }[];
}
export interface QueryStep {
  sql_execution_id: number | null; spark_job_id: number | null; stage_id: number; stage_attempt: number; status: string | null;
  tasks: number | null; submitted: number; first_task: number; end: number; wait_ms: number; run_ms: number;
  rows_read?: number | null; rows_from_shuffle?: number | null; rows_to_shuffle?: number | null; rows_written?: number | null; parent_ids?: number[];
}

/** Revision 20: what one run read and wrote */
/** Rows a write reported: output rows (appends, overwrites); a Delta MERGE, UPDATE or DELETE also what it did to the
 * target, and copied = unchanged rows of the files it rewrote. */
export interface WriteRows { rows?: number; source_rows?: number; inserted?: number; updated?: number; deleted?: number; copied?: number }
export interface RunTableRead {
  ctx: string; id: number; op: string; table: string; how: string; filter?: string | null; from_files?: number | null;
  /** rows read from it: the plan's scan, or the input records of the stages that read only it */
  rows?: number | null;
  files_read?: number | null; files_pruned?: number | null; bytes_read?: number | null; bytes_pruned?: number | null;
  partitions_read?: number | null; partition_cols?: number | null; dpp_filters?: number | null; dfp_filters?: number | null;
  /** Databricks scans only: the smallest and the largest file read */
  min_file_bytes?: number | null; max_file_bytes?: number | null;
}
export interface RunTablesQuery {
  ctx: string; id: number; start: number | null; end: number | null; status: string | null; description: string; op: string;
  reads: RunTableRead[]; writes: { table: string; how: string }[]; fed_by: { ctx: string; id: number; table: string | null; why: string }[];
  jobs: number[]; logic: PlanLogic;
  stages: { stage_id: number; stage_attempt: number; spark_job_id: number | null; reads: string[]; writes: string[]; input_bytes: number | null;
    shuffle_read: number | null; shuffle_write: number | null; output_bytes: number | null; tasks: number | null; max_task_bytes_in: number | null }[];
  input_bytes: number | null; output_bytes: number | null;
}
/** One MERGE's numbers from one place (analysis/merge.py): the target read, files touched, deletion vectors, CDF and,
 * when they can be derived, the rows updated and inserted. */
export interface MergeFacts {
  target: string | null; target_bytes: number | null; target_rows: number | null; copy_bytes: number | null;
  files_touched: number | null; files_total: number | null; per_file: number | null; dv_on: boolean; cdf_on: boolean;
  source_rows: number | null; output_rows: number | null; data_rows: number | null; change_rows: number | null;
  updated: number | null; inserted: number | null; derived: boolean;
}
export interface RunTables {
  run_key: string;
  tables: { table: string; path: string | null; role: string; reads: RunTableRead[]; writes: { ctx: string; id: number; op: string; start?: number | null; step?: string; bytes?: number | null; read_bytes?: number | null; rows?: WriteRows | null; merge?: MergeFacts | null }[];
    first_read: number | null; first_write: number | null; last: number | null;
    merge?: { keys: string[]; null_safe: boolean; join: string | null; how: string; query: number; ctx: string; target_filters: string[] };
    stats?: TableStats | null }[];
  merges: MergeCycle[];
  /** queries that read a database over JDBC, in start order; same_as: an earlier query that sent the same SQL */
  jdbc?: JdbcStep[];
  queries: RunTablesQuery[];
}

/** Revision 20: how big a table is and what reading it cost */
export interface TableStats {
  table: string; size_bytes: number | null; files: number | null; avg_file_bytes: number | null; scans: number; runs: number;
  files_read: number; files_pruned: number; bytes_read: number; bytes_pruned: number; partition_cols: number;
  scan_stages: number; bytes_from_files: number; rows_from_files?: number; scan_wall_ms: number; scan_task_ms: number;
  /** Databricks scans only: the smallest and the largest file read; rows per file is an average (no per-file counts) */
  min_file_bytes?: number | null; max_file_bytes?: number | null; rows_per_file?: number | null;
  /** some query MERGEs into it; batches: read by one stream in this many micro-batches (sizes added up) */
  merged?: boolean; batches?: number;
}
export interface MergeCycle {
  batch: string | null; start: number | null; end: number | null; kind: 'upserts' | 'deletes' | null; target: string | null;
  target_scans: number; steps: { ctx: string; id: number; op: string }[];
}

export interface JdbcStep {
  query: number; ctx: string; kind: 'count' | 'probe' | 'key ranges' | 'read'; source: string | null; partitions: number; sql: string;
  rows: number | null; took_ms: number | null; wrote_rows: number | null; wrote_bytes: number | null; written: string[]; same_as: number | null;
  spread?: { p50_task_rows_in: number | null; max_task_rows_in: number | null; min_task_rows_in: number | null; p50_task_ms: number | null; p90_task_ms: number | null };
}

/** What a run waited for cores: before its stages could start, or with its tasks queued on a full cluster wave after
 * wave (behind other runs or its own tasks), whichever is longer. Older analyses have only the first. */
export const waitOf = (r: { waiting_ms?: number | null; queued_full_ms?: number | null }) =>
  Math.max(r.waiting_ms ?? 0, r.queued_full_ms ?? 0);

/** No driver or executor logs at all: "no errors" would be a claim the logs cannot make. */
export const noLogs = (s: { coverage?: CoverageItem[] }) => (s.coverage ?? []).some((c) => c.kind === 'no_logs');
