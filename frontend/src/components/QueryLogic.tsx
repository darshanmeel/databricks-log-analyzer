// Revision 20: what a query joins on and filters by, read from its physical plan: the joins (how, type, keys,
// null-safe), the filters, what each scan pushed down to the files, the group-by keys and the windows.
import { useState } from 'react';
import type { PlanLogic } from '../api';
import { truncate } from '../format';

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

const hasCost = (f: string) => /\d\s?(B|KiB|MiB|GiB|TiB|s|ms|min|h)\b/.test(f);
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
          {w.limit ? <><span className="ql-k">keeps</span> {w.limit === 1 ? 'one row' : `the top ${w.limit} rows`} {w.partition_by.length ? <>per {keys(w.partition_by)}</> : 'of the whole result'}</>
            : <><span className="ql-k">ranks</span> {fnName(w)} per {w.partition_by.length ? keys(w.partition_by) : 'the whole result'}</>}
          <span className="muted"> · {w.limit ? `${fnName(w)}, ` : ''}ordered by {orderText(w.order_by)}</span>
          {w.ties ? <div className="bad small">{tieText(w)}</div> : null}
        </li>
      ))}
      {l.groups.map((g, i) => <li key={`g${i}`}><span className="ql-k">groups by</span> {keys(g.keys)}</li>)}
      {l.filters.map((f, i) => <li key={`f${i}`}><span className="ql-k">filters</span> <code className="ql-expr" title={f.condition}>{condText(f.condition)}</code></li>)}
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

const orderText = (ob: string[]) => ob.map((o) => o.replace(/ ASC NULLS FIRST$/, '').replace(/ NULLS LAST$/, '')).join(', ') || 'nothing';
const fnName = (w: PlanLogic['windows'][number]) => `${w.function ?? 'a window function'}()`;
/** A rank or dense_rank that keeps the first row per key keeps every row tied for first. */
export const tieText = (w: PlanLogic['windows'][number]) =>
  `${fnName(w)} keeps every row tied for first: two rows with the same ${orderText(w.order_by)} both rank 1 and both stay. ` +
  'Use row_number() for exactly one row per key, or add a column unique per row to the order.';

/** A plan condition for a chip: Spark's inferred "isnotnull(x)" next to a condition on x goes, the outer brackets go. */
export function condText(c: string): string {
  const strip = (s: string) => { let x = s.trim(); while (/^\(.*\)$/.test(x) && balanced(x.slice(1, -1))) x = x.slice(1, -1).trim(); return x; };
  const parts = splitAnd(strip(c));
  const keep = parts.filter((p) => {
    const m = p.match(/^isnotnull\(([\w.`]+)\)$/);
    return !m || parts.filter((q) => q !== p && q.includes(m[1])).length === 0;
  });
  return keep.map((p) => strip(p).replace(/\bisnotnull\(([\w.`]+)\)/g, '$1 IS NOT NULL')).join(' AND ');
}
const balanced = (s: string) => { let d = 0; for (const ch of s) { d += ch === '(' ? 1 : ch === ')' ? -1 : 0; if (d < 0) return false; } return d === 0; };
/** Split on the ANDs outside brackets. */
function splitAnd(s: string): string[] {
  const out: string[] = []; let d = 0, cur = '';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    d += ch === '(' ? 1 : ch === ')' ? -1 : 0;
    if (d === 0 && s.startsWith(' AND ', i)) { out.push(cur); cur = ''; i += 4; continue; }
    cur += ch;
  }
  return [...out, cur].filter((x) => x.trim());
}

export interface LogicChip { k: string; text: string; bad?: boolean; title?: string }

/** The logic in a few chips, for a list row: what it keeps per key, groups by, filters and joins on. */
export function logicChips(l: PlanLogic | null | undefined): LogicChip[] {
  if (!l) return [];
  const out: LogicChip[] = [];
  for (const w of l.windows ?? []) {
    const per = w.partition_by.join(', ') || 'all rows';
    const text = w.limit ? `keeps ${w.limit === 1 ? '1 row' : `top ${w.limit}`} per ${per} · ${w.function ?? 'rank'} by ${orderText(w.order_by)}`
      : `${w.function ?? 'window'}() per ${per} by ${orderText(w.order_by)}`;
    out.push({ k: 'window', text, bad: !!w.ties, title: w.ties ? tieText(w) : undefined });
  }
  for (const g of l.groups ?? []) out.push({ k: 'group', text: `groups by ${g.keys.join(', ')}` });
  for (const f of l.filters ?? []) out.push({ k: 'filter', text: `where ${condText(f.condition)}`, title: f.condition });
  for (const s of l.scans ?? []) {
    if (s.partition_filters.length) out.push({ k: 'filter', text: `partition filter ${s.partition_filters.join(' AND ')}` });
  }
  for (const j of userJoins(l)) {
    const type = (j.type ?? 'Inner').replace(/(Outer|Semi|Anti)$/, ' $1').replace(/^LeftOuter$/, 'left').toLowerCase();
    out.push({ k: 'join', text: `${type} join on ${j.left_keys.join(', ')}${j.null_safe ? ' (null-safe)' : ''}` });
  }
  return out;
}

/** The logic chips of a row; each is cut short, its tooltip has it all. */
export function LogicChips({ l, max = 6 }: { l: PlanLogic | null | undefined; max?: number }) {
  const cs = logicChips(l);
  if (!cs.length) return null;
  return (
    <div className="logic-chips">
      {cs.slice(0, max).map((c, i) => (
        <span key={i} className={`lchip lchip-${c.k}${c.bad ? ' bad' : ''}`} title={c.title ?? c.text}>
          {c.bad ? '⚠ ' : ''}{truncate(c.text, 90)}
        </span>
      ))}
      {cs.length > max && <span className="muted small">+{cs.length - max} more</span>}
    </div>
  );
}

/** Said wherever SQL or a cause is rebuilt from the plans: they are our reading of them, not Spark's own words. */
export const REBUILT_NOTE = 'Rebuilt from the physical plan, to the best of our knowledge: it may not be 100% correct or match the original code.';

// a string literal first, so a "--" or a keyword inside one is not marked
const KW = /('(?:[^']|'')*'|--[^\n]*|\b(?:SELECT|FROM|WHERE|AND|OR|NOT|GROUP BY|ORDER BY|LIMIT|LEFT SEMI JOIN|LEFT ANTI JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|CROSS JOIN|JOIN|ON|AS|OVER|PARTITION BY|UNION ALL|LATERAL VIEW|OUTER|EXCEPT|IN|IS|NULL|DESC|CASE|WHEN|THEN|ELSE|END)\b)/g;

/** A query's SQL as rebuilt from its plan, with keywords and comments marked, and a copy button. */
export function SqlBlock({ sql, maxHeight = 420 }: { sql: string; maxHeight?: number }) {
  const [copied, setCopied] = useState(false);
  const copy = () => {
    navigator.clipboard?.writeText(sql).then(() => { setCopied(true); setTimeout(() => setCopied(false), 1500); }, () => undefined);
  };
  const parts = sql.split(KW);
  return (
    <div className="sql-wrap">
      <button className="btn small ghost sql-copy" onClick={copy}>{copied ? 'Copied' : 'Copy'}</button>
      <pre className="sql-block" style={{ maxHeight }}>
        <code>{parts.map((p, i) => (i % 2 ? <span key={i} className={p.startsWith('--') ? 'sql-c' : p.startsWith("'") ? 'sql-s' : 'sql-k'}>{p}</span> : p))}</code>
      </pre>
      <div className="muted small sql-note">{REBUILT_NOTE}</div>
    </div>
  );
}
