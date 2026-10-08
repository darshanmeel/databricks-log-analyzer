// Graph model for the Hierarchy view (CONTRACT Revision 4).
// The backend serves /graph; when it is missing (older analyzer) the same shape is rebuilt from /hierarchy.
import type {
  ConnectOperationRow,
  Graph,
  GraphEdge,
  GraphFlag,
  GraphNode,
  GraphNodeType,
  Hierarchy,
  HierarchyApp,
  HierarchyJob,
  StageRow,
} from '../api';
import { skewBreach, gcBreach } from '../thresholds';

export const SHUFFLE_HEAVY_BYTES = 1024 ** 3;

/** Visual status of a node: drives the stripe / glyph color. */
export type VisualStatus = 'failed' | 'retried' | 'ok' | 'incomplete' | 'unknown';

export const STATUS_META: Record<VisualStatus, { label: string; glyph: string; mark: string; ink: string; tint: string }> = {
  failed: { label: 'Failed', glyph: '✕', mark: 'var(--st-crit)', ink: 'var(--st-crit-ink)', tint: 'var(--st-crit-tint)' },
  retried: { label: 'Succeeded after retries', glyph: '↻', mark: 'var(--st-warn)', ink: 'var(--st-warn-ink)', tint: 'var(--st-warn-tint)' },
  ok: { label: 'Succeeded', glyph: '✓', mark: 'var(--st-ok)', ink: 'var(--st-ok-ink)', tint: 'var(--st-ok-tint)' },
  incomplete: { label: 'Incomplete', glyph: '◐', mark: 'var(--st-na)', ink: 'var(--st-na-ink)', tint: 'var(--st-na-tint)' },
  unknown: { label: 'Unknown', glyph: '?', mark: 'var(--st-ref)', ink: 'var(--st-ref-ink)', tint: 'var(--st-ref-tint)' },
};

/** Fixed metric identity colors (categorical, not status): same in the graph meters and the spill chart. */
export const METRIC_META = {
  disk_spill: { label: 'Disk spill', short: 'Disk', color: 'var(--c2)' },
  mem_spill: { label: 'Memory spill', short: 'Mem', color: 'var(--c5)' },
  shuffle_read: { label: 'Shuffle read', short: 'Read', color: 'var(--c1)' },
  shuffle_write: { label: 'Shuffle write', short: 'Write', color: 'var(--c3)' },
} as const;
export type MeterKey = keyof typeof METRIC_META;
export const METER_KEYS: MeterKey[] = ['disk_spill', 'mem_spill', 'shuffle_read', 'shuffle_write'];

export interface GNode extends GraphNode {
  ctx: string;
  /** numeric ids parsed from `id` */
  jobId: number | null;
  stageId: number | null;
  attempt: number;
  execId: number | null;
  opId: string | null;
  flagSet: Set<string>;
  vstatus: VisualStatus;
}

export interface GModel {
  ctx: string;
  start: number | null;
  end: number | null;
  nodes: GNode[];
  byId: Map<string, GNode>;
  edges: GraphEdge[];
  app: GNode | null;
  /** job id -> stage nodes (all attempts) in stage-id order */
  jobs: GNode[];
  stagesByJob: Map<string, GNode[]>;
  /** stages whose parent is not a job */
  orphans: GNode[];
  queries: GNode[];
  connects: GNode[];
  /** maxima across the context for the inline meters */
  max: Record<MeterKey, number>;
  truncated: boolean;
  dropped: number;
  derived: boolean;
  hasDepends: boolean;
}

const lc = (s: string | null | undefined) => (s ?? '').toLowerCase();

function parseIds(n: GraphNode, ctx: string) {
  const prefix = `${n.type}:${ctx}:`;
  const rest = n.id.startsWith(prefix) ? n.id.slice(prefix.length) : n.id.split(':').slice(2).join(':');
  const parts = rest.split(':');
  const num = (v: string | undefined) => (v === undefined || v === '' || Number.isNaN(Number(v)) ? null : Number(v));
  switch (n.type) {
    case 'job':
      return { jobId: num(parts[0]), stageId: null, attempt: 0, execId: null, opId: null };
    case 'stage':
      return { jobId: null, stageId: num(parts[0]), attempt: num(parts[1]) ?? n.metrics?.attempt ?? 0, execId: null, opId: null };
    case 'query':
      return { jobId: null, stageId: null, attempt: 0, execId: num(parts[0]), opId: null };
    case 'connect':
      return { jobId: null, stageId: null, attempt: 0, execId: null, opId: rest };
    default:
      return { jobId: null, stageId: null, attempt: 0, execId: null, opId: null };
  }
}

function visualStatus(n: GraphNode, flags: Set<string>): VisualStatus {
  const s = lc(n.status);
  if (s.includes('fail') || s === 'error' || flags.has('failed')) return 'failed';
  if (flags.has('retried')) return 'retried';
  if (['succeeded', 'jobsucceeded', 'success', 'finished', 'ok'].includes(s)) return 'ok';
  if (s === 'incomplete' || s === 'running' || s === 'open') return 'incomplete';
  if (s === 'canceled' || s === 'cancelled') return 'incomplete';
  return n.type === 'app' ? 'ok' : 'unknown';
}

/** Normalize a /graph response (or a derived one) into the model the views draw. */
export function buildModel(g: Graph): GModel {
  const ctx = g.ctx;
  const nodes: GNode[] = (g.nodes ?? []).map((n) => {
    const flagSet = new Set<string>((n.flags ?? []).filter(Boolean));
    const ids = parseIds(n, ctx);
    // stage labels are ours ("Stage 6"); the attempt is drawn as its own badge
    const label = n.type === 'stage' && ids.stageId !== null ? `Stage ${ids.stageId}` : n.label;
    return { ...n, label, ctx, ...ids, flagSet, vstatus: visualStatus(n, flagSet) };
  });
  const byId = new Map(nodes.map((n) => [n.id, n]));
  // retry labels: "org.apache.spark.shuffle.FetchFailedException: ..." -> "FetchFailedException"
  const shortReason = (l: string | null | undefined) => (l ? l.split('\n')[0].replace(/(?:[a-z_][\w$]*\.)+([A-Z][\w$]*)/g, '$1').split(':')[0].slice(0, 40) : l);
  const edges = (g.edges ?? []).filter((e) => byId.has(e.source) && byId.has(e.target)).map((e) => (e.kind === 'retry' ? { ...e, label: shortReason(e.label) } : e));

  // attempts: a stage with a later attempt was retried at the stage level
  const attemptsOf = new Map<string, number>();
  for (const n of nodes) if (n.type === 'stage' && n.stageId !== null) attemptsOf.set(`${n.stageId}`, Math.max(attemptsOf.get(`${n.stageId}`) ?? 0, n.attempt + 1));
  for (const n of nodes) {
    if (n.type !== 'stage' || n.stageId === null) continue;
    const total = Math.max(attemptsOf.get(`${n.stageId}`) ?? 1, n.metrics?.attempts ?? 1);
    n.metrics = { ...(n.metrics ?? {}), attempt: n.attempt, attempts: total };
  }

  const app = nodes.find((n) => n.type === 'app') ?? null;
  const jobs = nodes.filter((n) => n.type === 'job').sort((a, b) => (a.jobId ?? 0) - (b.jobId ?? 0));
  const jobIds = new Set(jobs.map((j) => j.id));
  const stagesByJob = new Map<string, GNode[]>(jobs.map((j) => [j.id, []]));
  const orphans: GNode[] = [];
  // parent may be absent: fall back to a contains edge job -> stage
  const containsParent = new Map<string, string>();
  for (const e of edges) if (e.kind === 'contains' && jobIds.has(e.source)) containsParent.set(e.target, e.source);
  for (const n of nodes) {
    if (n.type !== 'stage') continue;
    const p = n.parent && jobIds.has(n.parent) ? n.parent : containsParent.get(n.id);
    if (p) stagesByJob.get(p)!.push(n);
    else orphans.push(n);
  }
  const byStage = (a: GNode, b: GNode) => (a.stageId ?? 0) - (b.stageId ?? 0) || a.attempt - b.attempt;
  stagesByJob.forEach((arr) => arr.sort(byStage));
  orphans.sort(byStage);

  const max = { disk_spill: 0, mem_spill: 0, shuffle_read: 0, shuffle_write: 0 } as Record<MeterKey, number>;
  for (const n of nodes) if (n.type === 'stage') for (const k of METER_KEYS) max[k] = Math.max(max[k], n.metrics?.[k] ?? 0);

  return {
    ctx,
    start: g.start ?? null,
    end: g.end ?? null,
    nodes,
    byId,
    edges,
    app,
    jobs,
    stagesByJob,
    orphans,
    queries: nodes.filter((n) => n.type === 'query').sort((a, b) => (a.execId ?? 0) - (b.execId ?? 0)),
    connects: nodes.filter((n) => n.type === 'connect').sort((a, b) => (a.start ?? 0) - (b.start ?? 0)),
    max,
    truncated: !!g.truncated,
    dropped: g.dropped_stages ?? 0,
    derived: !!g.derived,
    hasDepends: edges.some((e) => e.kind === 'depends'),
  };
}

/* ------------------------------------------------------------------ fallback: derive from /hierarchy */

const jobFailed = (j: HierarchyJob) => lc(j.status ?? j.result).includes('fail');

function stageFlags(s: StageRow, attempts: number): GraphFlag[] {
  const f: GraphFlag[] = [];
  if (s.status === 'failed') f.push('failed');
  if (s.status !== 'failed' && (s.stage_attempt > 0 || (s.failed_tasks ?? 0) > 0)) f.push('retried');
  else if (s.status !== 'failed' && attempts > 1) f.push('retried');
  if ((s.disk_spill ?? 0) + (s.mem_spill ?? 0) > 0) f.push('spill');
  if (skewBreach(s.skew, s.max_task_ms)) f.push('skew');
  if (gcBreach(s.gc_share)) f.push('gc');
  if ((s.shuffle_read ?? 0) + (s.shuffle_write ?? 0) >= SHUFFLE_HEAVY_BYTES) f.push('shuffle_heavy');
  return f;
}

export function deriveGraph(h: Hierarchy, ops: ConnectOperationRow[], ctx: string | null): Graph | null {
  const app: HierarchyApp | undefined = h.apps.find((a) => a.spark_context_id === ctx) ?? h.apps[0];
  if (!app) return null;
  const c = app.spark_context_id;
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];
  const appId = `app:${c}`;
  nodes.push({
    id: appId,
    type: 'app',
    label: app.app_name || 'Spark application',
    sublabel: app.app_id,
    status: app.jobs.some(jobFailed) ? 'failed' : 'succeeded',
    start: app.start_time,
    end: app.end_time,
    duration_ms: app.duration_ms,
    parent: null,
    flags: app.jobs.some(jobFailed) ? ['failed'] : [],
    what: {},
  });
  const queries = new Map(app.queries.map((q) => [q.sql_execution_id, q]));
  const opById = new Map(ops.filter((o) => !o.spark_context_id || o.spark_context_id === c).map((o) => [o.operation_id, o]));
  const allStages: StageRow[] = [...app.jobs.flatMap((j) => j.stages), ...(app.orphan_stages ?? [])];
  const attempts = new Map<number, number>();
  for (const s of allStages) attempts.set(s.stage_id, Math.max(attempts.get(s.stage_id) ?? 0, s.stage_attempt + 1));
  const stageNode = new Map<number, string[]>();
  const stageJob = new Map<number, number | null>();

  const addStage = (s: StageRow, parent: string, jobId: number | null) => {
    const id = `stage:${c}:${s.stage_id}:${s.stage_attempt}`;
    const att = attempts.get(s.stage_id) ?? 1;
    nodes.push({
      id,
      type: 'stage',
      label: `Stage ${s.stage_id}`,
      sublabel: s.stage_name,
      status: s.status,
      start: s.start_time,
      end: s.end_time,
      duration_ms: s.duration_ms,
      parent,
      metrics: {
        tasks: s.tasks ?? s.num_tasks,
        failed_tasks: s.failed_tasks,
        attempt: s.stage_attempt,
        attempts: att,
        disk_spill: s.disk_spill,
        mem_spill: s.mem_spill,
        shuffle_read: s.shuffle_read,
        shuffle_write: s.shuffle_write,
        input_bytes: s.input_bytes,
        output_bytes: s.output_bytes,
        gc_share: s.gc_share,
        skew: s.skew,
        min_task_ms: s.min_task_ms ?? null,
        p50_task_ms: s.p50_task_ms,
        max_task_ms: s.max_task_ms,
        input_records: s.input_records ?? null,
        shuffle_read_records: s.shuffle_read_records ?? null,
        shuffle_write_records: s.shuffle_write_records ?? null,
        output_records: s.output_records ?? null,
        data_skew: s.data_skew ?? null,
        min_task_bytes_in: s.min_task_bytes_in ?? null,
        p50_task_bytes_in: s.p50_task_bytes_in ?? null,
        max_task_bytes_in: s.max_task_bytes_in ?? null,
        min_task_rows_in: s.min_task_rows_in ?? null,
        p50_task_rows_in: s.p50_task_rows_in ?? null,
        max_task_rows_in: s.max_task_rows_in ?? null,
        // Revision 13: p10 / p90 / average of the data per task, p10 / p90 of task time
        p10_task_bytes_in: s.p10_task_bytes_in ?? null,
        p90_task_bytes_in: s.p90_task_bytes_in ?? null,
        avg_task_bytes_in: s.avg_task_bytes_in ?? null,
        p10_task_rows_in: s.p10_task_rows_in ?? null,
        p90_task_rows_in: s.p90_task_rows_in ?? null,
        avg_task_rows_in: s.avg_task_rows_in ?? null,
        p10_task_ms: s.p10_task_ms ?? null,
        p90_task_ms: s.p90_task_ms ?? null,
        wmed_task_bytes_in: s.wmed_task_bytes_in ?? null,
      },
      flags: stageFlags(s, att),
      what: { description: s.stage_name, call_site: s.details ?? null, rdd_scopes: s.rdd_scopes ?? s.rdd_names ?? null },
    });
    edges.push({ source: parent, target: id, kind: 'contains' });
    stageNode.set(s.stage_id, [...(stageNode.get(s.stage_id) ?? []), id]);
    stageJob.set(s.stage_id, jobId);
  };

  for (const j of app.jobs) {
    const jid = `job:${c}:${j.spark_job_id}`;
    const q = j.sql_execution_id !== null ? queries.get(j.sql_execution_id) : undefined;
    const op = j.connect_operation_id ? opById.get(j.connect_operation_id) : undefined;
    const failed = jobFailed(j);
    const retried = !failed && j.stages.some((s) => s.stage_attempt > 0 || (s.failed_tasks ?? 0) > 0);
    nodes.push({
      id: jid,
      type: 'job',
      label: `Job ${j.spark_job_id}`,
      sublabel: j.description ?? j.call_site,
      status: failed ? 'failed' : j.result ? 'succeeded' : 'incomplete',
      start: j.start_time,
      end: j.end_time,
      duration_ms: j.duration_ms,
      parent: appId,
      metrics: {
        tasks: j.stages.reduce((a, s) => a + (s.tasks ?? 0), 0),
        failed_tasks: j.stages.reduce((a, s) => a + (s.failed_tasks ?? 0), 0),
      },
      flags: [...(failed ? ['failed'] : []), ...(retried ? ['retried'] : [])],
      what: {
        description: j.description,
        call_site: j.call_site,
        notebook_path: j.notebook_path,
        sql_description: q?.description ?? null,
        statement_text: op?.statement_text ?? null,
      },
    });
    edges.push({ source: appId, target: jid, kind: 'contains' });
    for (const s of j.stages) addStage(s, jid, j.spark_job_id);
  }
  for (const s of app.orphan_stages ?? []) addStage(s, appId, null);

  // dependencies (only when the backend already exposes parent_ids) and stage retries
  for (const s of allStages) {
    const tid = `stage:${c}:${s.stage_id}:${s.stage_attempt}`;
    for (const p of s.parent_ids ?? []) {
      const pids = stageNode.get(p);
      if (!pids?.length) continue;
      const reused = stageJob.get(p) !== stageJob.get(s.stage_id);
      edges.push({ source: pids[pids.length - 1], target: tid, kind: 'depends', label: reused ? 'reused' : null });
    }
    if (s.stage_attempt > 0) {
      const prev = `stage:${c}:${s.stage_id}:${s.stage_attempt - 1}`;
      if (stageNode.get(s.stage_id)?.includes(prev)) edges.push({ source: prev, target: tid, kind: 'retry', label: s.retry_of_failure ?? 'resubmitted' });
    }
  }

  for (const q of app.queries) {
    const qid = `query:${c}:${q.sql_execution_id}`;
    nodes.push({
      id: qid,
      type: 'query',
      label: `Query ${q.sql_execution_id}`,
      sublabel: q.description,
      status: q.status,
      start: q.start_time,
      end: q.end_time,
      duration_ms: q.duration_ms,
      parent: appId,
      metrics: { tasks: q.tasks, failed_tasks: q.failed_tasks, disk_spill: q.disk_spill, mem_spill: q.mem_spill, shuffle_read: q.shuffle_read, shuffle_write: q.shuffle_write, input_bytes: q.input_bytes, output_bytes: q.output_bytes, gc_share: q.gc_share, skew: q.max_stage_skew },
      flags: lc(q.status).includes('fail') ? ['failed'] : [],
      what: { sql_description: q.description },
    });
    for (const j of app.jobs) if (j.sql_execution_id === q.sql_execution_id) edges.push({ source: qid, target: `job:${c}:${j.spark_job_id}`, kind: 'runs_query' });
  }
  for (const o of opById.values()) {
    const linked = app.jobs.filter((j) => j.connect_operation_id === o.operation_id);
    if (!linked.length) continue;
    const oid = `connect:${c}:${o.operation_id}`;
    nodes.push({
      id: oid,
      type: 'connect',
      label: 'Connect statement',
      sublabel: o.user_name || o.user_id,
      status: o.status,
      start: o.start_time,
      end: o.finish_time,
      duration_ms: o.duration_ms,
      parent: appId,
      flags: o.status === 'failed' ? ['failed'] : [],
      what: { statement_text: o.statement_text },
    });
    for (const j of linked) edges.push({ source: oid, target: `job:${c}:${j.spark_job_id}`, kind: 'from_connect' });
  }
  return { ctx: c, start: app.start_time, end: app.end_time, nodes, edges, derived: true };
}

/* ------------------------------------------------------------------ filters */

export type FilterKey = 'problems' | 'spill' | 'shuffle' | 'join';
export const FILTERS: { key: FilterKey; label: string }[] = [
  { key: 'problems', label: 'Failed or retried' },
  { key: 'spill', label: 'Spilled' },
  { key: 'shuffle', label: 'Shuffle-heavy' },
  { key: 'join', label: 'Has a join' },
];
const JOIN_RE = /Join|Join[A-Z]|BroadcastHash|SortMerge|CartesianProduct/;
/** A join in the plan's operators or the stage's Spark scopes. */
const hasJoin = (n: GNode) => [...((n.what?.operators as string[] | undefined) ?? []), ...((n.what?.rdd_scopes as string[] | undefined) ?? [])].some((o) => JOIN_RE.test(String(o)));

export function matches(n: GNode, f: FilterKey[]): boolean {
  if (!f.length) return true;
  return f.every((k) => {
    if (k === 'problems') return n.vstatus === 'failed' || n.vstatus === 'retried' || n.flagSet.has('retried') || (n.metrics?.retries ?? 0) > 0;
    if (k === 'join') return hasJoin(n);
    if (k === 'spill') return n.flagSet.has('spill') || (n.metrics?.disk_spill ?? 0) + (n.metrics?.mem_spill ?? 0) > 0;
    return n.flagSet.has('shuffle_heavy') || (n.metrics?.shuffle_read ?? 0) + (n.metrics?.shuffle_write ?? 0) >= SHUFFLE_HEAVY_BYTES;
  });
}

export const typeLabel: Record<GraphNodeType, string> = {
  app: 'Spark application',
  job: 'Spark job',
  stage: 'Stage',
  query: 'SQL query',
  connect: 'Spark Connect statement',
};

export const FLAG_META: Record<string, { label: string; text: (n: GNode) => string }> = {
  skew: { label: 'Skew', text: (n) => `skew ${(n.metrics?.skew ?? 0).toFixed(0)}×` },
  gc: { label: 'GC', text: (n) => `GC ${Math.round((n.metrics?.gc_share ?? 0) * 100)}%` },
};


/* ------------------------------------------------------------------ work units: one query, Connect statement or job at a time */

/** What the Hierarchy page focuses on: a SQL query (all its jobs), a Connect statement without a query, or one job. */
export interface WorkUnit {
  id: string;
  kind: 'query' | 'connect' | 'job' | 'orphans';
  node: GNode | null;
  title: string;
  text: string;
  jobs: GNode[];
  stages: GNode[];
  vstatus: VisualStatus;
  start: number | null;
  end: number | null;
  duration_ms: number | null;
}

const RANK: Record<VisualStatus, number> = { failed: 4, retried: 3, incomplete: 2, unknown: 1, ok: 0 };

function unitOf(m: GModel, id: string, kind: WorkUnit['kind'], node: GNode | null, jobs: GNode[]): WorkUnit {
  const stages = kind === 'orphans' ? m.orphans : jobs.flatMap((j) => m.stagesByJob.get(j.id) ?? []);
  const all = [...(node ? [node] : []), ...jobs];
  let vstatus: VisualStatus = node?.vstatus ?? (jobs.length ? 'ok' : 'unknown');
  if (all.some((n) => n.vstatus === 'failed')) vstatus = 'failed';
  else if ([...all, ...stages].some((n) => n.vstatus === 'failed' || n.vstatus === 'retried')) vstatus = 'retried';
  else for (const n of all) if (RANK[n.vstatus] > RANK[vstatus]) vstatus = n.vstatus;
  const starts = [...all, ...stages].map((n) => n.start).filter((v): v is number => typeof v === 'number');
  const ends = [...all, ...stages].map((n) => n.end).filter((v): v is number => typeof v === 'number');
  const start = node?.start ?? (starts.length ? Math.min(...starts) : null);
  const end = node?.end ?? (ends.length ? Math.max(...ends) : null);
  const w = node?.what ?? jobs[0]?.what ?? {};
  const text =
    kind === 'orphans'
      ? 'Stages Spark ran outside any job'
      : node?.type === 'connect'
        ? w.statement_text ?? node.sublabel ?? ''
        : node?.sublabel ?? w.sql_description ?? w.description ?? w.call_site ?? '';
  return {
    id,
    kind,
    node,
    title: kind === 'orphans' ? 'Stages without a job' : node?.type === 'connect' ? 'Connect statement' : node?.label ?? id,
    text: text ?? '',
    jobs,
    stages,
    vstatus,
    start,
    end,
    duration_ms: node?.duration_ms ?? (start !== null && end !== null ? end - start : null),
  };
}

/** Queries first (each with all its jobs), then Connect statements whose jobs have no query, then jobs with neither. */
export function workUnits(m: GModel): WorkUnit[] {
  const jobsOf = new Map<string, GNode[]>();
  for (const e of m.edges) {
    if (e.kind !== 'runs_query' && e.kind !== 'from_connect') continue;
    const j = m.byId.get(e.target);
    if (j?.type === 'job') jobsOf.set(e.source, [...(jobsOf.get(e.source) ?? []), j]);
  }
  const byJobId = (a: GNode, b: GNode) => (a.jobId ?? 0) - (b.jobId ?? 0);
  const claimed = new Set<string>();
  const out: WorkUnit[] = [];
  for (const q of m.queries) {
    const jobs = (jobsOf.get(q.id) ?? []).sort(byJobId);
    jobs.forEach((j) => claimed.add(j.id));
    out.push(unitOf(m, q.id, 'query', q, jobs));
  }
  for (const c of m.connects) {
    const jobs = (jobsOf.get(c.id) ?? []).filter((j) => !claimed.has(j.id)).sort(byJobId);
    if (!jobs.length) continue;
    jobs.forEach((j) => claimed.add(j.id));
    out.push(unitOf(m, c.id, 'connect', c, jobs));
  }
  for (const j of m.jobs) if (!claimed.has(j.id)) out.push(unitOf(m, j.id, 'job', j, [j]));
  if (m.orphans.length) out.push(unitOf(m, 'orphans', 'orphans', null, []));
  return out.sort((a, b) => (a.start ?? Infinity) - (b.start ?? Infinity));
}

/** The unit for an id: a unit id itself, a single job (even one inside a query), or the unit that holds a stage. */
export function resolveUnit(m: GModel, units: WorkUnit[], id: string | null): WorkUnit | null {
  if (!id) return null;
  const direct = units.find((u) => u.id === id);
  if (direct) return direct;
  const n = m.byId.get(id);
  if (n?.type === 'job') return unitOf(m, n.id, 'job', n, [n]);
  if (n?.type === 'stage') return units.find((u) => u.stages.some((s) => s.id === n.id)) ?? null;
  return null;
}

/** Default focus: the first failed unit, else the first retried one, else the slowest. */
export function defaultUnit(units: WorkUnit[]): WorkUnit | null {
  const withJobs = units.filter((u) => u.jobs.length || u.kind === 'orphans');
  return (
    withJobs.find((u) => u.vstatus === 'failed') ??
    // then the slowest: a retried attempt is often adaptive execution cancelling work, not the problem
    [...withJobs].sort((a, b) => (b.duration_ms ?? 0) - (a.duration_ms ?? 0))[0] ??
    units[0] ??
    null
  );
}

export function unitMatches(u: WorkUnit, f: FilterKey[]): boolean {
  if (!f.length) return true;
  return [...(u.node ? [u.node] : []), ...u.jobs, ...u.stages].some((n) => matches(n, f));
}

/** A model holding only one unit: its caller(s), its jobs and their stages. Meter maxima stay app-wide so bars compare across units. */
export function focusModel(m: GModel, u: WorkUnit): GModel {
  const keep = new Set<string>([...u.jobs.map((j) => j.id), ...u.stages.map((s) => s.id)]);
  if (u.node) keep.add(u.node.id);
  // every query / Connect statement that ran these jobs gives context
  for (const e of m.edges) if ((e.kind === 'runs_query' || e.kind === 'from_connect') && keep.has(e.target) && u.jobs.some((j) => j.id === e.target)) keep.add(e.source);
  const nodes = m.nodes.filter((n) => keep.has(n.id));
  const times = nodes.flatMap((n) => [n.start, n.end]).filter((v): v is number => typeof v === 'number');
  return {
    ...m,
    start: times.length ? Math.min(...times) : null,
    end: times.length ? Math.max(...times) : null,
    nodes,
    byId: new Map(nodes.map((n) => [n.id, n])),
    edges: m.edges.filter((e) => keep.has(e.source) && keep.has(e.target)),
    app: null,
    jobs: u.jobs,
    stagesByJob: new Map(u.jobs.map((j) => [j.id, m.stagesByJob.get(j.id) ?? []])),
    orphans: u.kind === 'orphans' ? m.orphans : [],
    queries: m.queries.filter((n) => keep.has(n.id)),
    connects: m.connects.filter((n) => keep.has(n.id)),
    truncated: false,
    dropped: 0,
  };
}
