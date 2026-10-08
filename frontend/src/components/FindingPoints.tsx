// Revision 19: one finding as points, like the cluster's "what to change": what we saw (its facts), the likely cause,
// the fix, the tables its query reads and writes (empty for work without tables, such as a plain Spark job), and
// links to where it happened.
import type { ReactNode } from 'react';
import { Link } from 'react-router-dom';
import type { FindingRow } from '../api';
import { fmtTime } from '../format';
import { to } from '../links';
import { SeverityBadge } from './ui';

/** Why each kind of problem happens, when its evidence does not say. */
const CAUSE: Record<string, string> = {
  merge_rewrite: 'The MERGE condition does not let Delta skip files, so it reads far more of the target than the source touches (and, without deletion vectors, rewrites every file it touches).',
  disk_spill: 'Each task holds more data than fits in its share of memory: too few shuffle partitions, a skewed key, or small executors.',
  gc_pressure: 'Tasks create or hold more objects than the memory per core allows (large broadcasts, caching, wide rows).',
  jvm_full_gc: 'The executor heap is nearly full after each collection, so the JVM keeps stopping everything to free memory.',
  gc_stuck: 'The heap stayed full after every collection, so the executor spent its time collecting instead of running tasks; its slow tasks are stuck, not skewed.',
  oom_site: 'Tasks ran out of memory; where it happened says what holds the memory.',
  task_skew: 'A few partitions hold much more data than the rest, so a few tasks run long after the others finished.',
  data_skew: 'A few keys or partitions hold much more data than the rest.',
  tiny_tasks: 'The data is cut into many very small partitions, so scheduling costs more than the work.',
  executor_oom: 'An executor ran out of memory and was killed; its tasks and cached data were lost.',
  executor_lost: 'An executor went away (spot loss, node failure or a kill) while it ran tasks.',
  task_retries: 'Tasks failed and were run again; the time of the failed attempts was lost.',
  stage_failed: 'A stage ran out of retries.',
  query_failed: 'The query stopped with an error.',
  waited_for_cores: 'Other runs on the same cluster held the cores, so its stages queued before any task could start.',
  cores_full: 'More work was submitted than the cluster had cores for.',
  capacity_bound: 'The run had more tasks than the cluster had cores, wave after wave, so its tasks queued on a full cluster.',
  autoscale_lag: 'The cluster started small and autoscaling added workers only minutes after the tasks started to queue.',
  autoscale_removed: 'Autoscaling removed workers while their cached or shuffle data was still needed, so it was computed again.',
  dataframe_cache: 'cache() or persist() of more data than the executors can hold: blocks go to disk, are dropped, and are rebuilt.',
  count_only: 'count() calls run the whole query again just to return one number.',
  ddl_loop: 'One ALTER statement per column: each is its own Delta commit.',
  disk_cache: 'The Databricks disk cache copies files to local disk for later reads that never came.',
  init_script: 'A cluster init script wrote errors when a node started.',
};
const KIND: Record<string, string> = {
  merge_rewrite: 'code', data_skew: 'code', task_skew: 'code', tiny_tasks: 'code', exception: 'code', query_failed: 'code',
  waited_for_cores: 'cluster', cores_full: 'cluster', capacity_bound: 'cluster', autoscale_lag: 'cluster', autoscale_removed: 'cluster',
  dataframe_cache: 'code', count_only: 'code', ddl_loop: 'code', init_script: 'cluster', executor_lost: 'cluster', executor_oom: 'cluster', oom_site: 'code',
};
const base = (c: string) => c.replace(/^log:/, '');

/** How deep a finding goes: 0 = what the data plainly shows (big reads, spill, shuffle, slow or failed work), 1 = the
 * executor's memory and GC, 2 = log exceptions and retries. The plain ones come first; the deeper ones explain them. */
const MEMORY_GC = new Set(['gc_pressure', 'jvm_full_gc', 'gc_stuck', 'oom_site', 'disk_cache']);
const PLAIN_LOG = new Set(['disk_spill', 'executor_oom', 'executor_lost', 'disk_full', 'fetch_failure']);
export const depthOf = (c: string) =>
  MEMORY_GC.has(base(c)) ? 1
    : c === 'exception' || c === 'task_retries' || c === 'init_script' || (c.startsWith('log:') && !PLAIN_LOG.has(base(c))) || /^cache_/.test(base(c)) ? 2 : 0;
const SEV_N: Record<string, number> = { high: 0, medium: 1, low: 2, info: 3 };
/** Plain findings first, then by severity. */
export const plainFirst = (a: { category: string; severity: string }, b: { category: string; severity: string }) =>
  depthOf(a.category) - depthOf(b.category) || (SEV_N[a.severity] ?? 9) - (SEV_N[b.severity] ?? 9);

export const kindName = (c: string) => {
  const s = base(c).replace(/_/g, ' ');
  return (s[0].toUpperCase() + s.slice(1)).replace(/\bjvm\b/i, 'JVM').replace(/\bgc\b/ig, 'GC').replace(/\boom\b/i, 'out of memory').replace(/ site$/, '');
};

/** A finding's "where", short: "Stage 1884", "Query 1408", "executor 3". */
export const whereOf = (f: FindingRow) => {
  const e = f.entity ?? '';
  const m = e.match(/^(stage|query|job) (\d+(?:\.\d+)?)/);
  if (m) return `${m[1][0].toUpperCase()}${m[1].slice(1)} ${m[2]}`;
  if (e.startsWith('run ')) return 'this run';
  return e.replace('driver/executor logs', 'driver and executor logs');
};

/** Sentences, kept whole: "14.1 GB" and "e.g. 26/09" are not sentence ends. */
const sentences = (s: string | null | undefined) =>
  (s ?? '').split(/(?<=[.!?])\s+(?=[A-Z(])/).map((x) => x.trim().replace(/\.$/, '')).filter(Boolean);
const IS_CAUSE = /\b(does not|doesn't|because|too small|too few|too many|cannot|can't)\b/i;

/** Tables worth naming: not a stream's checkpoint files or a Delta log. */
export const realTables = (ts: string[] | null | undefined) =>
  (ts ?? []).filter((t) => !t.includes('_delta_log') && !/checkpoint|\.parquet$|\.json$/.test(t));

/** A table by name; a storage path only by its last part (the table id), the whole path on hover. */
const shortTable = (t: string) => (/^[a-z]+:\/\/|^\//.test(t) ? `…/${t.split('/').filter(Boolean).pop()}` : t);

function Tables({ label, ts }: { label: string; ts: string[] }) {
  return (
    <div className="adv-links">
      <span className="muted" style={{ minWidth: 44 }}>{label}</span>
      {ts.length ? ts.map((t) => <code key={t} className="fp-table" title={t}>{shortTable(t)}</code>) : <span className="muted">–</span>}
    </div>
  );
}

export function FindingPoints({ cid, f, reads, writes, links, time = true }: {
  cid: string; f: FindingRow; reads?: string[] | null; writes?: string[] | null; links?: ReactNode; time?: boolean;
}) {
  const all = sentences(f.evidence);
  const said = all.filter((x) => IS_CAUSE.test(x));
  const facts = all.filter((x) => !IS_CAUSE.test(x));
  const cause = said.length ? said.join('. ') + '.' : CAUSE[base(f.category)] ?? null;
  const fixes = sentences(f.fix);
  const onQuery = f.sql_execution_id !== null || f.stage_id !== null;
  const ctx = f.spark_context_id;
  return (
    <div className={`advice adv-${f.severity === 'high' ? 'high' : f.severity === 'medium' ? 'medium' : 'info'}`}>
      <div className="advice-head">
        <SeverityBadge sev={f.severity} />
        <Link className="ro-plain" to={to.findings(cid, f.finding_id)}><b>{kindName(f.category)}</b></Link>
        {f.entity ? <b>· {whereOf(f)}</b> : null}
        <span style={{ flex: 1 }} />
        {time ? <span className="mono small muted">{fmtTime(f.ts)}</span> : null}
      </div>
      <dl className="advice-grid small">
        {facts.length > 0 && <><dt>What we saw</dt><dd><ul>{facts.map((x, i) => <li key={i}>{x}</li>)}</ul></dd></>}
        {cause && <><dt>Likely cause</dt><dd>{cause}</dd></>}
        {fixes.length > 0 && (
          <>
            <dt>Fix <span className="ro-tag">{KIND[base(f.category)] ?? 'config'}</span></dt>
            <dd><ul className="adv-fix">{fixes.map((x, i) => <li key={i}>{x}</li>)}</ul></dd>
          </>
        )}
        {onQuery && (
          <>
            <dt>Tables</dt>
            <dd className="stack" style={{ gap: 3 }}>
              <Tables label="Reads" ts={realTables(reads)} />
              <Tables label="Writes" ts={realTables(writes)} />
            </dd>
          </>
        )}
        {(links || (ctx && (f.sql_execution_id !== null || f.stage_id !== null))) && (
          <>
            <dt>Open</dt>
            <dd className="adv-links">
              {ctx && f.sql_execution_id !== null && <Link className="tchip" to={to.query(cid, ctx, f.sql_execution_id)}>Query {f.sql_execution_id}</Link>}
              {ctx && f.stage_id !== null && <Link className="tchip" to={to.stages(cid, ctx, f.stage_id, f.stage_attempt ?? 0)}>Stage {f.stage_id}{f.stage_attempt ? `.${f.stage_attempt}` : ''}</Link>}
              {links}
            </dd>
          </>
        )}
      </dl>
    </div>
  );
}
