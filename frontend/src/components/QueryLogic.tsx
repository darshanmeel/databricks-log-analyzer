// Revision 20: what a query joins on and filters by, read from its physical plan: the joins (how, type, keys,
// null-safe), the filters, what each scan pushed down to the files, the group-by keys and the windows.
import type { PlanLogic } from '../api';

const short = (t: string | null) => (t && /^[a-z]+:\/\/|^\//.test(t) ? `…/${t.split('/').filter(Boolean).pop()}` : t ?? 'a source');
const HOW: Record<string, string> = {
  SortMergeJoin: 'sort-merge', BroadcastHashJoin: 'broadcast hash', ShuffledHashJoin: 'shuffled hash',
  BroadcastNestedLoopJoin: 'nested loop (row × row)', CartesianProduct: 'cartesian (row × row)',
};
const scanName = (t: string | null) => (t === 'ExistingRDD mergeMaterializedSource' ? 'the MERGE source' : t === 'ExistingRDD' ? 'the stream batch' : short(t));

/** Delta's own bookkeeping (joins on a file path or deletion vector) is not the user's logic. */
export const userJoins = (l: PlanLogic) => l.joins.filter((j) => !j.left_keys.some((k) => /deletionVector|^path$/.test(k)));

export function hasLogic(l: PlanLogic | null | undefined): l is PlanLogic {
  return !!l && (userJoins(l).length > 0 || (l.facts?.length ?? 0) > 0 || l.filters.length > 0 || l.groups.length > 0 || l.windows.length > 0 ||
    l.scans.some((s) => s.partition_filters.length || s.data_filters.length || s.pushed_filters.length));
}

const hasCost = (f: string) => /\d\s?(B|KB|MB|GB|TB|s|ms|min|h)\b/.test(f);
/** A plan note in two or three words (the sentence is its tooltip). */
const shortFact = (f: string) =>
  /Change Data Feed/.test(f) ? 'change data feed on' : /Optimized write/.test(f) ? 'optimized write: one more shuffle'
    : /Deletion vectors/.test(f) ? 'deletion vectors on' : /materialized its source/.test(f) ? 'MERGE copies its source' : f.split(':')[0];

/** The logic as short lines. */
export function QueryLogic({ l, compact }: { l: PlanLogic; compact?: boolean }) {
  const joins = userJoins(l);
  const scans = l.scans.filter((s) => s.partition_filters.length || s.data_filters.length || s.pushed_filters.length);
  const keys = (ks: string[]) => ks.map((k, i) => <span key={i}>{i ? ', ' : ''}<code className="fp-table">{k}</code></span>);
  return (
    <ul className={`qlogic ${compact ? 'compact' : ''}`}>
      {l.from_initial && <li className="muted small">Joins from the plan as it started: adaptive execution found an empty side and replaced the join with an empty result.</li>}
      {joins.map((j, i) => (
        <li key={`j${i}`}>
          <span className="ql-k">joins</span> {j.type ? <b>{j.type}</b> : null} <span className="muted">({HOW[j.how] ?? j.how})</span> on {keys(j.left_keys)}
          {j.null_safe ? <span className="muted" title="Spark writes a <=> b as coalesce(a, ''), isnull(a): NULL matches NULL"> · null-safe (&lt;=&gt;)</span> : null}
          {j.left_keys.join() !== j.right_keys.join() && j.right_keys.length ? <span className="muted"> = {j.right_keys.join(', ')}</span> : null}
          {j.condition ? <span className="muted"> · and {j.condition}</span> : null}
          {/(row × row)/.test(HOW[j.how] ?? '') ? <span className="bad"> · every row against every row</span> : null}
        </li>
      ))}
      {l.windows.map((w, i) => (
        <li key={`w${i}`}>
          <span className="ql-k">keeps</span> {w.keeps.replace('the top 1 by dense_rank per key', 'one row per key').replace('the top 1 by rank per key', 'one row per key')} {keys(w.partition_by)}
          <span className="muted"> · ordered by {w.order_by.join(', ')}</span>
        </li>
      ))}
      {l.groups.map((g, i) => <li key={`g${i}`}><span className="ql-k">groups by</span> {keys(g.keys)}</li>)}
      {l.filters.map((f, i) => <li key={`f${i}`}><span className="ql-k">filters</span> <code className="ql-expr">{f.condition}</code></li>)}
      {/* a note with a cost stays a line; the rest (table features with no number) fold into one line of chips */}
      {(l.facts ?? []).filter(hasCost).map((f, i) => <li key={`x${i}`}><span className="ql-k">cost</span> {f}</li>)}
      {(l.facts ?? []).some((f) => !hasCost(f)) && (
        <li><span className="ql-k">table</span> {(l.facts ?? []).filter((f) => !hasCost(f)).map((f, i) => <span key={i} className="ql-chip" title={f}>{shortFact(f)}</span>)}</li>
      )}
      {scans.map((s, i) => (
        <li key={`s${i}`}>
          <span className="ql-k">on read</span> {scanName(s.table)}:{' '}
          {s.partition_filters.length ? <><b>partition filter</b> <code className="ql-expr">{s.partition_filters.join(' AND ')}</code> </> : null}
          {s.data_filters.length ? <>data filter <code className="ql-expr">{s.data_filters.join(' AND ')}</code> </> : null}
          {s.pushed_filters.length ? <span className="muted">pushed to the files: {s.pushed_filters.join(', ')}</span> : null}
        </li>
      ))}
    </ul>
  );
}
