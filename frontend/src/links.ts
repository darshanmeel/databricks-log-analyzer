import { qs, type DiagLink } from './api';

// URL builders for cross-links.

const base = (cid: string) => `/c/${encodeURIComponent(cid)}`;

export const to = {
  home: () => '/',
  overview: (cid: string) => base(cid),
  story: (cid: string, p: Record<string, string | number | null | undefined> = {}) => `${base(cid)}/story${qs(p)}`,
  hierarchy: (
    cid: string,
    p: { ctx?: string | null; job?: number | null; stage?: number | null; attempt?: number | null; node?: string | null; mode?: 'time' | null; unit?: string | null; tab?: string | null } = {},
  ) =>
    `${base(cid)}/hierarchy${qs(p)}`,
  timeline: (cid: string, ctx?: string | null) => `${base(cid)}/timeline${qs({ ctx })}`,
  /** A stage opens inside its query or job on the Queries & jobs page. */
  stages: (cid: string, ctx?: string | null, stage?: number | string | null, attempt?: number | null) =>
    `${base(cid)}/hierarchy${qs({ ctx, stage, attempt: stage !== null && stage !== undefined ? attempt ?? 0 : undefined })}`,
  queries: (cid: string) => `${base(cid)}/hierarchy`,
  query: (cid: string, ctx: string, id: number | string, tab: 'plan' | null = null) =>
    `${base(cid)}/hierarchy${qs({ ctx, unit: `query:${ctx}:${id}`, tab })}`,
  executors: (cid: string, ctx?: string | null, executor?: string | null) => `${base(cid)}/executors${qs({ ctx, executor })}`,
  logLine: (cid: string, filePath: string, seq: number) => `${base(cid)}/logs${qs({ file_path: filePath, seq })}`,
  logs: (cid: string, filters: Record<string, string | number | null | undefined> = {}) => `${base(cid)}/logs${qs(filters)}`,
  findings: (cid: string, finding?: string | null) => `${base(cid)}/findings${qs({ finding })}`,
  errors: (cid: string, fingerprint?: string | null) => `${base(cid)}/errors${qs({ fingerprint })}`,
  /** Raw data tab: a notebook step (`step`) or a raw dataset (`dataset`), optionally pre-filtered by text. */
  data: (cid: string, p: { step?: string | null; dataset?: string | null; q?: string | null } = {}) => `${base(cid)}/data${qs(p)}`,
};

export function diagLinkHref(cid: string, l: DiagLink): string | null {
  const ctx = l.spark_context_id ?? null;
  switch (l.type) {
    case 'stage':
      return l.id === null || l.id === undefined ? null : to.stages(cid, ctx, l.id, l.attempt ?? 0);
    case 'query':
      return l.id === null || l.id === undefined || !ctx ? to.queries(cid) : to.query(cid, ctx, l.id);
    case 'executor':
      return to.executors(cid, ctx, l.id === null || l.id === undefined ? null : String(l.id));
    case 'job':
      return to.hierarchy(cid, { ctx, job: l.id === null || l.id === undefined ? null : Number(l.id) });
    case 'finding':
      return to.findings(cid, l.id === null || l.id === undefined ? null : String(l.id));
    case 'log':
      if (l.file_path && l.seq !== null && l.seq !== undefined) return to.logLine(cid, l.file_path, l.seq);
      return to.logs(cid);
    case 'error':
      return to.errors(cid, l.id === null || l.id === undefined ? null : String(l.id));
    default:
      return null;
  }
}

export interface EntityLink {
  type: DiagLink['type'];
  label: string;
  href: string;
}

/** Links derived from any row that carries the shared link columns (findings, story). */
export interface LinkCols {
  spark_context_id?: string | null;
  stage_id?: number | null;
  stage_attempt?: number | null;
  spark_job_id?: number | null;
  sql_execution_id?: number | null;
  executor_id?: string | null;
  log_file_path?: string | null;
  log_seq?: number | null;
  fingerprint?: string | null;
  finding_id?: string | null;
}

export function rowLinks(cid: string, r: LinkCols): EntityLink[] {
  const out: EntityLink[] = [];
  const ctx = r.spark_context_id ?? null;
  if (r.stage_id !== null && r.stage_id !== undefined)
    out.push({
      type: 'stage',
      label: `Stage ${r.stage_id}${r.stage_attempt ? `.${r.stage_attempt}` : ''}`,
      href: to.stages(cid, ctx, r.stage_id, r.stage_attempt ?? 0),
    });
  if (r.spark_job_id !== null && r.spark_job_id !== undefined)
    out.push({ type: 'job', label: `Job ${r.spark_job_id}`, href: to.hierarchy(cid, { ctx, job: r.spark_job_id }) });
  if (r.sql_execution_id !== null && r.sql_execution_id !== undefined && ctx)
    out.push({ type: 'query', label: `Query ${r.sql_execution_id}`, href: to.query(cid, ctx, r.sql_execution_id) });
  if (r.executor_id !== null && r.executor_id !== undefined && r.executor_id !== '')
    out.push({
      type: 'executor',
      label: r.executor_id === 'driver' ? 'Driver' : `Executor ${r.executor_id}`,
      href: to.executors(cid, ctx, r.executor_id),
    });
  if (r.log_file_path && r.log_seq !== null && r.log_seq !== undefined)
    out.push({ type: 'log', label: 'line', href: to.logLine(cid, r.log_file_path, r.log_seq) });
  if (r.fingerprint) out.push({ type: 'error', label: `Error ${r.fingerprint.slice(0, 7)}`, href: to.errors(cid, r.fingerprint) });
  if (r.finding_id) out.push({ type: 'finding', label: `Finding ${r.finding_id}`, href: to.findings(cid, r.finding_id) });
  return out;
}
