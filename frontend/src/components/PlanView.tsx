import { useMemo, useState } from 'react';
import type { DiffLine } from '../api';

function indentOf(line: string): number {
  if (/^\s*==.*==\s*$/.test(line)) return -1;
  if (!line.trim()) return 1e6; // blank lines belong to whatever is open
  const m = line.match(/^[\s:|+\-]*/);
  return m ? m[0].length : 0;
}

/** Operators that cost the most, by level: crit moves data between executors (or all of it to one task, or every
 * row against every row); warn holds data in memory and spills when it does not fit, or leaves the JVM. */
const RISK: [RegExp, 'crit' | 'warn', string][] = [
  [/^Exchange$/, 'crit', 'Shuffle: every row is written to disk and sent over the network to another executor'],
  [/^ShuffleQueryStage$/, 'crit', 'Shuffle stage: the data written by the shuffle below it'],
  [/^(CartesianProduct|BroadcastNestedLoopJoin)$/, 'crit', 'Every row against every row: grows as rows × rows'],
  [/^(Sort|SortMergeJoin|ShuffledHashJoin|SortAggregate|HashAggregate|ObjectHashAggregate)$/, 'warn', 'Holds rows in memory; spills to disk when they do not fit'],
  [/^(Window|RunningWindowFunction|WindowGroupLimit|WindowGroupLimitExec)$/, 'warn', 'Window: holds each partition key in memory, sorted'],
  [/^(BroadcastExchange|BroadcastQueryStage)$/, 'warn', 'Broadcast: the whole table goes to the driver and to every executor'],
  [/^(BatchEvalPython|ArrowEvalPython|FlatMapGroupsInPandas|MapInPandas|FlatMapCoGroupsInPandas|AggregateInPandas|PythonUDF)$/, 'warn', 'Python UDF: rows leave the JVM for a Python worker'],
  [/^(Generate|Expand)$/, 'warn', 'Multiplies rows (explode, rollup, cube, count distinct)'],
];
/** The operator on a plan line (tree prefix, then an optional "*(2) " or "(5) "), and how risky it is. */
function riskOf(line: string, args: Map<number, string>): { pre: string; op: string; rest: string; level: 'crit' | 'warn'; why: string; parts: number | null } | null {
  const m = line.match(/^([\s:|+\-]*(?:\*\(\d+\)\s*)?(?:\(\d+\)\s*)?)([A-Z][A-Za-z]+)(.*)$/);
  if (!m) return null;
  let hit = RISK.find(([re]) => re.test(m[2]));
  // a broadcast to the executors is not a shuffle; all rows to one task is worse than one
  if (m[2] === 'Exchange' && /SinglePartition/.test(m[3])) {
    hit = /BROADCAST/.test(m[3]) ? [/x/, 'warn', 'Broadcast: the whole table is sent to every executor']
      : [/x/, 'crit', 'Moves ALL rows to ONE task: no parallelism, and that task can run out of memory'];
  }
  // the tree line says "Exchange (20)"; how it partitions is on "Arguments:" under "(20) Exchange" further down
  const id = m[3].match(/^\s*\((\d+)\)/);
  const arg = m[2] === 'Exchange' ? `${m[3]} ${id ? args.get(Number(id[1])) ?? '' : ''}` : '';
  if (m[2] === 'Exchange' && /SinglePartition/.test(arg) && !/SinglePartition/.test(m[3])) {
    hit = /BROADCAST/.test(arg) ? [/x/, 'warn', 'Broadcast: the whole table is sent to every executor']
      : [/x/, 'crit', 'Moves ALL rows to ONE task: no parallelism, and that task can run out of memory'];
  }
  if (m[2] === 'Exchange' && /deltaoptimizedwrite/i.test(arg)) hit = [/x/, 'crit', 'Shuffle before the write (Delta optimized write), so each file gets about the target size'];
  // hashpartitioning(keys, 200) / rangepartitioning(keys, 200): how many pieces the shuffle cuts the data into
  const n = arg.match(/(?:hash|range)partitioning\(.*?, (\d+)\)/);
  const parts = n ? Number(n[1]) : null;
  if (hit && parts !== null) {
    hit = [hit[0], hit[1], `${hit[2]}. Cut into ${parts} partitions (tasks)` + (parts === 200
      ? ': 200 is the default spark.sql.shuffle.partitions. Adaptive execution can merge them when small but never split them, so a big shuffle stays at 200 big tasks.'
      : '.')];
  }
  return hit ? { pre: m[1], op: m[2], rest: m[3], level: hit[1], why: hit[2], parts } : null;
}

/** Monospace plan text with collapsible subtrees (by tree indentation) and section headers. */
export function PlanView({ text }: { text: string | null | undefined }) {
  const lines = useMemo(() => (text ?? '').replace(/\r\n/g, '\n').split('\n'), [text]);
  const indents = useMemo(() => lines.map(indentOf), [lines]);
  // "(20) Exchange" ... "Arguments: hashpartitioning(..., 200), ENSURE_REQUIREMENTS"
  const args = useMemo(() => {
    const m = new Map<number, string>();
    let cur: number | null = null;
    for (const l of lines) {
      const h = l.match(/^\((\d+)\) Exchange\b/);
      if (h) { cur = Number(h[1]); continue; }
      if (/^\(\d+\) /.test(l)) cur = null;
      if (cur !== null && l.startsWith('Arguments:')) { m.set(cur, l); cur = null; }
    }
    return m;
  }, [lines]);
  const [collapsed, setCollapsed] = useState<Set<number>>(new Set());

  const foldable = (i: number) => {
    for (let j = i + 1; j < lines.length; j++) {
      if (indents[j] === 1e6) continue;
      return indents[j] > indents[i];
    }
    return false;
  };

  const visible: { i: number; hidden: number }[] = [];
  for (let i = 0; i < lines.length; i++) {
    let hidden = 0;
    if (collapsed.has(i)) {
      let j = i + 1;
      while (j < lines.length && (indents[j] > indents[i])) j++;
      hidden = j - i - 1;
      visible.push({ i, hidden });
      i = j - 1;
      continue;
    }
    visible.push({ i, hidden });
  }

  if (!text) return <p className="muted">No plan was recorded for this query.</p>;

  const toggle = (i: number) =>
    setCollapsed((prev) => {
      const n = new Set(prev);
      if (n.has(i)) n.delete(i);
      else n.add(i);
      return n;
    });

  const collapseAll = () => {
    const n = new Set<number>();
    lines.forEach((_, i) => indents[i] >= 0 && indents[i] < 1e6 && foldable(i) && indents[i] > 0 && n.add(i));
    setCollapsed(n);
  };

  return (
    <div>
      <div className="row" style={{ gap: 8, marginBottom: 8, flexWrap: 'wrap' }}>
        <button className="btn small" onClick={() => setCollapsed(new Set())}>
          Expand all
        </button>
        <button className="btn small" onClick={collapseAll}>
          Collapse subtrees
        </button>
        <span className="muted small" style={{ alignSelf: 'center' }}>
          {lines.length} lines. Click ▸ to fold a subtree.
        </span>
        <span className="small plan-legend" style={{ alignSelf: 'center' }}>
          <span className="plan-op crit">Exchange</span> shuffle, all to one task, row × row ·{' '}
          <span className="plan-op warn">Sort</span> holds rows in memory, can spill · hover one for why
        </span>
      </div>
      <div className="plan">
        {visible.map(({ i, hidden }) => {
          const f = foldable(i);
          const isOp = indents[i] === -1 || /^[\s:|+\-]*(\*\(\d+\)\s*)?[A-Z][A-Za-z]+(Exec|Stage|Scan|Join|Aggregate|Exchange|Sort|Project|Filter|Window|Union|Limit)?\b/.test(lines[i]);
          return (
            <div key={i} className="plan-line">
              <span className="ln">{i + 1}</span>
              <span className={`fold ${f ? '' : 'none'}`} onClick={f ? () => toggle(i) : undefined} role={f ? 'button' : undefined} aria-label={f ? 'Toggle subtree' : undefined}>
                {f ? (collapsed.has(i) ? '▸' : '▾') : ''}
              </span>
              <span className={`txt ${isOp && indents[i] === -1 ? 'op' : ''}`}>{(() => {
                const r = indents[i] >= 0 ? riskOf(lines[i], args) : null;
                return r ? <>{r.pre}<span className={`plan-op ${r.level}`} title={r.why}>{r.op}</span>{r.rest}
                  {r.parts === 200 ? <span className="plan-note" title={r.why}>200 partitions: the default</span> : null}</> : lines[i] || ' ';
              })()}</span>
              {hidden > 0 && <span className="hidden-n">{hidden} lines folded</span>}
            </div>
          );
        })}
      </div>
    </div>
  );
}

/** Plan diff rendered unified or side by side. */
export function DiffView({ diff, mode }: { diff: DiffLine[]; mode: 'side' | 'unified' }) {
  const changed = diff.filter((d) => d.op !== 'equal').length;
  if (!diff.length) return <p className="muted">Both plans are empty.</p>;
  const head = (
    <p className="small ink2" style={{ marginBottom: 8 }}>
      {changed === 0 ? 'The plans are identical after normalizing expression ids.' : `${changed} changed lines.`}{' '}
      <span className="badge" style={{ background: 'var(--sev-high-soft)', color: 'var(--sev-high-text)' }}>− only in this query</span>{' '}
      <span className="badge good">+ only in the other query</span>
    </p>
  );
  if (mode === 'unified')
    return (
      <div>
        {head}
        <div className="plan">
          {diff.map((d, k) => (
            <div key={k} className={`diff-line ${d.op === 'insert' ? 'ins' : d.op === 'delete' ? 'del' : ''}`}>
              <span className="ln">{d.a_line ?? ''}</span>
              <span className="ln">{d.b_line ?? ''}</span>
              <span className="op">{d.op === 'insert' ? '+' : d.op === 'delete' ? '−' : ' '}</span>
              <span>{d.text || ' '}</span>
            </div>
          ))}
        </div>
      </div>
    );
  // side by side: pair consecutive delete/insert runs
  const rows: { a?: DiffLine; b?: DiffLine }[] = [];
  let i = 0;
  while (i < diff.length) {
    const d = diff[i];
    if (d.op === 'equal') {
      rows.push({ a: d, b: d });
      i++;
      continue;
    }
    const dels: DiffLine[] = [];
    const ins: DiffLine[] = [];
    while (i < diff.length && diff[i].op !== 'equal') {
      if (diff[i].op === 'delete') dels.push(diff[i]);
      else ins.push(diff[i]);
      i++;
    }
    for (let k = 0; k < Math.max(dels.length, ins.length); k++) rows.push({ a: dels[k], b: ins[k] });
  }
  const cell = (d: DiffLine | undefined, side: 'a' | 'b') => {
    if (!d) return <div className="diff-line blank"> </div>;
    const cls = d.op === 'insert' ? 'ins' : d.op === 'delete' ? 'del' : '';
    return (
      <div className={`diff-line ${cls}`}>
        <span className="ln">{side === 'a' ? d.a_line ?? '' : d.b_line ?? ''}</span>
        <span className="op">{d.op === 'insert' ? '+' : d.op === 'delete' ? '−' : ' '}</span>
        <span>{d.text || ' '}</span>
      </div>
    );
  };
  return (
    <div>
      {head}
      <div className="plan" style={{ padding: 0 }}>
        <div className="diff-side">
          <div style={{ borderRight: '1px solid var(--line)', overflowX: 'auto' }}>{rows.map((r, k) => <div key={k}>{cell(r.a, 'a')}</div>)}</div>
          <div style={{ overflowX: 'auto' }}>{rows.map((r, k) => <div key={k}>{cell(r.b, 'b')}</div>)}</div>
        </div>
      </div>
    </div>
  );
}
