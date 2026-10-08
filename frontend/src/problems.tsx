import { Link } from 'react-router-dom';
import type { IncidentRow } from './api';

/** Shared by Findings and Errors: what each kind of problem means, and fixes in layers. */

export const CONF: Record<string, string> = { strong: 'strong link', likely: 'likely link', weak: 'matched by time only' };

export const FAILURE_KINDS = new Set([
  'out of memory', 'executor killed by the OS', 'disk full', 'executor lost', 'shuffle fetch failure', 'error in task code', 'stage failed',
  'job aborted', 'query failed', 'error reported to the notebook / job',
]);

/** One sentence per kind of problem, in plain words. */
export const MEANING: Record<string, string> = {
  'error in task code': 'Your code raised an error inside a task. Spark retried the task and, when every attempt failed the same way, gave up on the stage.',
  'out of memory': 'An executor ran out of JVM heap. Everything running on it fails, and the shuffle files it held are lost.',
  'executor killed by the OS': 'The operating system killed the executor process, usually because the container went over its memory limit.',
  'executor lost': 'An executor went away (spot reclaim, crash or decommission). Its running tasks are retried elsewhere.',
  'shuffle fetch failure': 'A task could not read shuffle output written by another executor, usually because that executor died. Spark reruns the stage that wrote it.',
  'stage failed': 'A stage gave up after its tasks failed too many times (spark.task.maxFailures, 4 by default).',
  'job aborted': 'Spark aborted the job because one of its stages failed.',
  'query failed': 'The query ended with an error.',
  'error reported to the notebook / job': 'How the failure reached your notebook or job: a wrapper around the error that really happened.',
  'disk full': 'Local disk on a node filled up, usually with shuffle or spill files.',
  error: 'An exception logged by the driver or an executor.',
  'tasks too big': 'Each task was given far more data than the ~128 MiB Spark sizes a task for: too few shuffle partitions, or files that cannot be split.',
  'big table read': 'A stage read a lot from storage, often a whole table where a filter on its partition or clustering columns would skip most files.',
  'executors idle': 'Executors were up, and paid for, but ran no task for a minute or more.',
};

/** Fixes in three layers (config / code / infrastructure), the way the tuning tools present them. */
export const TIPS: Record<string, { layer: 'Config' | 'Code' | 'Cluster'; text: string }[]> = {
  'task skew': [
    { layer: 'Config', text: 'spark.sql.adaptive.enabled=true and spark.sql.adaptive.skewJoin.enabled=true split skewed join partitions.' },
    { layer: 'Code', text: 'Salt or pre-aggregate the hot keys; drop null or default keys before the join.' },
  ],
  'disk spill': [
    { layer: 'Config', text: 'Raise spark.sql.shuffle.partitions (or let AQE pick: spark.sql.adaptive.coalescePartitions.enabled=true) so each task holds less.' },
    { layer: 'Cluster', text: 'Memory-optimized nodes, or fewer cores per executor so each task gets more memory.' },
  ],
  'GC pressure': [
    { layer: 'Code', text: 'Cache less, avoid collecting or broadcasting large tables, prefer built-in functions over Python UDFs.' },
    { layer: 'Cluster', text: 'More memory per core: memory-optimized nodes or fewer cores per executor.' },
  ],
  'out of memory': [
    { layer: 'Config', text: 'More, smaller partitions (spark.sql.shuffle.partitions); lower spark.sql.autoBroadcastJoinThreshold if a broadcast is large.' },
    { layer: 'Cluster', text: 'Memory-optimized nodes, or fewer cores per executor; leave off-heap headroom (spark.executor.memoryOverhead).' },
    { layer: 'Code', text: 'Look for skew in this stage first: one huge partition runs out of memory whatever the node size.' },
  ],
  'executor killed by the OS': [{ layer: 'Cluster', text: 'The OS killed the JVM (often the OOM killer): memory-optimized nodes, more spark.executor.memoryOverhead.' }],
  'executor lost': [
    { layer: 'Cluster', text: 'Spot capacity was reclaimed: use on-demand for the driver and first workers, with spot fallback to on-demand.' },
    { layer: 'Config', text: 'spark.decommission.enabled=true moves shuffle and cached blocks off a node before it goes.' },
  ],
  'shuffle fetch failure': [
    { layer: 'Config', text: 'Usually a symptom: fix why the executor holding the shuffle files died. spark.shuffle.io.maxRetries / retryWait only help with network blips.' },
  ],
  'too many tiny tasks': [
    { layer: 'Code', text: 'Compact small files (OPTIMIZE) or coalesce before writing.' },
    { layer: 'Config', text: 'Raise spark.sql.files.maxPartitionBytes so each read task gets more data.' },
  ],
  'tasks too big': [
    { layer: 'Config', text: "Raise spark.sql.shuffle.partitions (on Databricks it can be 'auto') or lower spark.sql.files.maxPartitionBytes so each task gets ~128 MiB." },
  ],
  'big table read': [
    { layer: 'Code', text: 'Filter on the partition or clustering columns so whole files are skipped; select only the columns you need.' },
    { layer: 'Cluster', text: 'OPTIMIZE with Z-ORDER or liquid clustering on the columns you filter by.' },
  ],
  'executors idle': [{ layer: 'Cluster', text: 'Let autoscaling shrink the cluster, or run fewer, bigger steps back to back.' }],
  'task retries': [{ layer: 'Config', text: 'Retries hide the cause: look at the first failure of each retried task.' }],
  'disk full': [{ layer: 'Cluster', text: 'Bigger local disks or autoscaling local storage; fewer, larger shuffle partitions.' }],
  'error in task code': [
    { layer: 'Code', text: 'Fix the line marked "Fix here". Bad input rows are the usual cause: validate or quarantine them (try/except returning null, or a filter) instead of failing the job.' },
    { layer: 'Config', text: 'Retrying will not help: the same row fails every attempt. Lower spark.task.maxFailures only to fail faster while you debug.' },
  ],
  'source database connection': [
    { layer: 'Cluster', text: 'The source database or the network dropped the connection; when every retry succeeded the job only lost time. Check the database load and the number of parallel connections (JDBC numPartitions).' },
    { layer: 'Config', text: 'Fewer JDBC partitions or a longer socket timeout on the connection; a separate job for the extraction keeps its retries off the other runs.' },
  ],
  'stage failed': [{ layer: 'Code', text: 'A symptom: fix the first error of the failing task (the root cause above it).' }],
  'error reported to the notebook / job': [{ layer: 'Code', text: 'A wrapper: the real error is the one it was caused by. Catch it in the notebook only to add context, not to retry blindly.' }],
};

/**
 * Incident rows as places to badge: a duplicate ('same') finding stands for its problem (the lead's kind, role and
 * finding) but keeps its own stage / executor scope, so a stage named only by a duplicate log line still gets the badge.
 * Leaves out what is not a place (query failed, the error the notebook saw). Root causes first.
 */
export function problemPlaces(rows: IncidentRow[]): IncidentRow[] {
  const lead = new Map<string, IncidentRow>();
  for (const r of rows) if (r.role !== 'same' && !lead.has(r.problem_id)) lead.set(r.problem_id, r);
  return rows
    .map((r) => {
      const l = r.role === 'same' ? lead.get(r.problem_id) : undefined;
      return l ? { ...r, role: l.role, kind: l.kind, finding_id: l.finding_id, incident_severity: l.incident_severity } : r;
    })
    .filter((r) => r.kind !== 'query failed' && !r.kind.startsWith('error reported'))
    .sort((a, b) => a.incident_rank - b.incident_rank || Number(b.role === 'root') - Number(a.role === 'root'));
}

/** key -> problems (one per problem_id), for every key a row maps to. */
export function indexProblems(rows: IncidentRow[], keys: (r: IncidentRow) => string[]): Map<string, IncidentRow[]> {
  const m = new Map<string, IncidentRow[]>();
  for (const r of problemPlaces(rows))
    for (const k of keys(r)) {
      const a = m.get(k) ?? [];
      if (!a.some((x) => x.problem_id === r.problem_id)) a.push(r);
      m.set(k, a);
    }
  return m;
}

export function FixTips({ kind, fix, stackHref, compact }: { kind: string; fix?: string | null; stackHref?: string | null; compact?: boolean }) {
  const tips = TIPS[kind] ?? [];
  if (!fix && !tips.length) return null;
  return (
    <div className={`fix-tips ${compact ? 'compact' : ''}`}>
      {fix && (
        <div className="fix-main">
          {!/^fix\b/i.test(fix) && <b>Fix: </b>}
          {fix}
          {stackHref && (
            <>
              {' '}
              <Link to={stackHref}>Full stack</Link>
            </>
          )}
        </div>
      )}
      {!compact &&
        tips.map((t, k) => (
          <div key={k} className="tip">
            <span className={`layer layer-${t.layer.toLowerCase()}`}>{t.layer}</span> {t.text}
          </div>
        ))}
    </div>
  );
}
