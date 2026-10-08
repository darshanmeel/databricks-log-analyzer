// Interactive SVG for the Hierarchy graph: Flow (layered DAG per job) and Time (shared time axis).
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { fmtGrow, stageRows } from '../rows';
import { fmtBytes, fmtDuration, fmtNum, fmtPct, fmtRows, fmtSkew, fmtTs, tickLabel, timeTicks, truncate } from '../format';
import { FLOW, flowLayout, QBOX_HEAD, TIME, timeLayout, type FlowAround, type FlowCtx, type FlowLayout, type FoldSide, type Rect, type TimeLayout } from './layout';
import { METRIC_META, STATUS_META, typeLabel, type FilterKey, type GModel, type GNode } from './model';

export type Mode = 'flow' | 'time';

interface View {
  k: number;
  tx: number;
  ty: number;
}

const clamp = (v: number, a: number, b: number) => Math.max(a, Math.min(b, v));

function useBox<E extends HTMLElement>(): [(el: E | null) => void, { w: number; h: number }] {
  const [size, setSize] = useState({ w: 0, h: 0 });
  const ro = useRef<ResizeObserver | null>(null);
  const ref = useCallback((el: E | null) => {
    ro.current?.disconnect();
    if (!el) return;
    setSize({ w: el.clientWidth, h: el.clientHeight });
    ro.current = new ResizeObserver(([e]) => setSize({ w: Math.floor(e.contentRect.width), h: Math.floor(e.contentRect.height) }));
    ro.current.observe(el);
  }, []);
  useEffect(() => () => ro.current?.disconnect(), []);
  return [ref, size];
}

/* ------------------------------------------------------------------ small svg pieces */

function StatusPill({ n, x, y }: { n: GNode; x: number; y: number }) {
  const m = STATUS_META[n.vstatus];
  const word = n.vstatus === 'retried' ? 'Retried' : m.label;
  const w = 18 + word.length * 6.4;
  return (
    <g transform={`translate(${x - w},${y})`}>
      <rect width={w} height={18} rx={9} style={{ fill: m.tint }} />
      <text x={8} y={12.5} className="g-pill" style={{ fill: m.ink }}>
        {m.glyph} {word}
      </text>
    </g>
  );
}

/** Two thin meters (e.g. disk + memory spill) sharing one group max, with the group total. */
function MeterGroup({ x, y, w, label, a, b, max }: { x: number; y: number; w: number; label: string; a: [keyof typeof METRIC_META, number]; b: [keyof typeof METRIC_META, number]; max: number }) {
  const total = a[1] + b[1];
  const bar = (v: number) => (v > 0 && max > 0 ? Math.max(2, (v / max) * w) : 0);
  return (
    <g transform={`translate(${x},${y})`}>
      <text y={0} className="g-meter-label">
        {label}{' '}
        <tspan className={total > 0 ? 'g-meter-val' : 'g-faint'}>{total > 0 ? fmtBytes(total) : 'none'}</tspan>
      </text>
      {[a, b].map(([k, v], i) => (
        <g key={k} transform={`translate(0,${5 + i * 7})`}>
          <rect width={w} height={4} rx={2} className="g-track" />
          {v > 0 && <rect width={bar(v)} height={4} rx={2} style={{ fill: METRIC_META[k].color }} />}
        </g>
      ))}
    </g>
  );
}

function flagText(n: GNode): string {
  const out: string[] = [];
  if (n.flagSet.has('skew') && n.metrics?.skew) out.push(`skew ${fmtSkew(n.metrics.skew)}`);
  if (n.flagSet.has('gc') && n.metrics?.gc_share !== undefined && n.metrics?.gc_share !== null) out.push(`GC ${fmtPct(n.metrics.gc_share, 0)}`);
  return out.join(' · ');
}

function StageCard({ n, r, max, selected, dim, focusable, onKey }: { n: GNode; r: Rect; max: GModel['max']; selected: boolean; dim: boolean; focusable: boolean; onKey: (e: React.KeyboardEvent) => void }) {
  const m = STATUS_META[n.vstatus];
  const att = n.metrics?.attempts ?? 1;
  const tasks = n.metrics?.tasks;
  const failedTasks = n.metrics?.failed_tasks ?? 0;
  const left = `${fmtDuration(n.duration_ms ?? (n.start && n.end ? n.end - n.start : null))} · ${fmtNum(tasks)} tasks`;
  let flags = flagText(n);
  if (left.length + flags.length + (failedTasks ? 10 : 0) > 40) flags = [n.flagSet.has('skew') ? 'skew' : '', n.flagSet.has('gc') ? 'GC' : ''].filter(Boolean).join(' · ');
  const innerW = r.w - 24;
  const gw = (innerW - 14) / 2;
  const spillMax = Math.max(max.disk_spill, max.mem_spill);
  const shufMax = Math.max(max.shuffle_read, max.shuffle_write);
  const rf = stageRows(n.metrics);
  const rowsTitle = rf.in === null && rf.out === null ? 'Spark recorded no row counts for this stage' : [
    rf.read ? `${fmtRows(rf.read)} rows read from storage` : null, rf.fromShuffle ? `${fmtRows(rf.fromShuffle)} rows from the shuffle of earlier stages` : null,
    rf.out !== null ? `${fmtRows(rf.out)} rows out${rf.outTo === 'shuffle' ? ' to the shuffle' : rf.outTo === 'table' ? ' written' : ''}` : null,
    rf.grew ? `${fmtGrow(rf.grew)} more rows out than in: a join matched many rows per key, or a cross join` : null].filter(Boolean).join(' · ');
  return (
    <g
      className={`gnode stage ${selected ? 'sel' : ''} ${dim ? 'dim' : ''}`}
      transform={`translate(${r.x},${r.y})`}
      data-id={n.id}
      tabIndex={focusable ? 0 : -1}
      role="button"
      aria-label={`${n.label}${n.attempt ? ` attempt ${n.attempt + 1}` : ''}, ${m.label}`}
      onKeyDown={onKey}
    >
      <rect className="body" width={r.w} height={r.h} rx={8} />
      <rect width={4} height={r.h - 12} x={0} y={6} rx={2} style={{ fill: m.mark }} />
      <text x={14} y={20} className="g-title">
        {n.label}
      </text>
      {att > 1 && (
        <g transform={`translate(${14 + n.label.length * 7.4 + 8},7)`}>
          <rect width={78} height={17} rx={8.5} style={{ fill: n.vstatus === 'failed' ? 'var(--st-crit-tint)' : 'var(--st-warn-tint)' }} />
          <text x={39} y={12} textAnchor="middle" className="g-pill" style={{ fill: n.vstatus === 'failed' ? 'var(--st-crit-ink)' : 'var(--st-warn-ink)' }}>
            attempt {n.attempt + 1} of {att}
          </text>
        </g>
      )}
      <text x={r.w - 10} y={20} textAnchor="end" className="g-glyph" style={{ fill: m.ink }}>
        {m.glyph}
        <title>{m.label}</title>
      </text>
      <text x={14} y={38} className="g-sub">
        {truncate(n.sublabel ?? '', 36)}
      </text>
      <text x={14} y={55} className="g-meta">
        {left}
        {failedTasks > 0 && <tspan style={{ fill: 'var(--st-crit-ink)' }}>{` · ${fmtNum(failedTasks)} failed`}</tspan>}
      </text>
      {flags && (
        <text x={r.w - 10} y={55} textAnchor="end" className="g-flag">
          {flags}
        </text>
      )}
      <text x={14} y={73} className="g-meta">
        {rf.in === null && rf.out === null ? <tspan className="g-faint">rows not recorded</tspan> : <>
          rows {rf.in !== null ? fmtRows(rf.in) : '?'} in → {rf.out !== null ? fmtRows(rf.out) : '?'} out
          {rf.grew ? <tspan className="g-grew">{` ${fmtGrow(rf.grew)}`}</tspan> : null}</>}
        <title>{rowsTitle}</title>
      </text>
      <MeterGroup x={14} y={89} w={gw} label="Spill" a={['disk_spill', n.metrics?.disk_spill ?? 0]} b={['mem_spill', n.metrics?.mem_spill ?? 0]} max={spillMax} />
      <MeterGroup x={14 + gw + 14} y={89} w={gw} label="Shuffle" a={['shuffle_read', n.metrics?.shuffle_read ?? 0]} b={['shuffle_write', n.metrics?.shuffle_write ?? 0]} max={shufMax} />
    </g>
  );
}

function CallerCard({ n, r, selected, dim, focusable, onKey }: { n: GNode; r: Rect; selected: boolean; dim: boolean; focusable: boolean; onKey: (e: React.KeyboardEvent) => void }) {
  const m = STATUS_META[n.vstatus];
  const isConnect = n.type === 'connect';
  const text = isConnect ? (n.what?.statement_text ?? '').replace(/\s+/g, ' ') : n.sublabel ?? n.what?.sql_description ?? '';
  const ops = (n.what?.operators ?? []).slice(0, 4).join(' › ');
  return (
    <g
      className={`gnode caller ${selected ? 'sel' : ''} ${dim ? 'dim' : ''}`}
      transform={`translate(${r.x},${r.y})`}
      data-id={n.id}
      tabIndex={focusable ? 0 : -1}
      role="button"
      aria-label={`${typeLabel[n.type]} ${n.label}, ${m.label}`}
      onKeyDown={onKey}
    >
      <rect className="body" width={r.w} height={r.h} rx={8} />
      <text x={12} y={17} className="g-kicker">
        {isConnect ? `Spark Connect${n.sublabel && !text.startsWith(n.sublabel.slice(0, 12)) ? `, ${truncate(n.sublabel, 22)}` : ''}` : 'SQL query'}
      </text>
      <text x={r.w - 10} y={17} textAnchor="end" className="g-glyph" style={{ fill: m.ink }}>
        {m.glyph}
        <title>{m.label}</title>
      </text>
      <text x={12} y={36} className={isConnect ? 'g-code' : 'g-title'}>
        {isConnect ? truncate(text || 'statement', 34) : n.label}
      </text>
      <text x={12} y={54} className={isConnect ? 'g-meta' : 'g-sub'}>
        {isConnect ? fmtDuration(n.duration_ms) : truncate(ops || text, 36)}
      </text>
    </g>
  );
}

function JobBox({ b, n, selected, dim, focusable, onKey }: { b: FlowLayout['boxes'][number]; n: GNode | null; selected: boolean; dim: boolean; focusable: boolean; onKey: (e: React.KeyboardEvent) => void }) {
  const failed = n?.vstatus === 'failed';
  const descMax = Math.max(12, Math.floor((b.w - 150) / 6.2));
  return (
    <g className={`gbox ${selected ? 'sel' : ''} ${dim ? 'dim' : ''} ${failed ? 'failed' : ''}`} transform={`translate(${b.x},${b.y})`}>
      <rect className="box" width={b.w} height={b.h} rx={12} />
      <g
        className="gnode jobhead"
        data-id={n?.id}
        tabIndex={n && focusable ? 0 : -1}
        role={n ? 'button' : undefined}
        aria-label={n ? `${n.label}, ${STATUS_META[n.vstatus].label}` : b.label}
        onKeyDown={onKey}
      >
        <rect className="head" width={b.w} height={FLOW.head} rx={12} />
        <rect className="head" y={FLOW.head - 12} width={b.w} height={12} />
        <text x={16} y={20} className="g-title">
          {b.label}
        </text>
        <text x={16} y={37} className="g-sub">
          {b.collapsed
            ? `${fmtNum(b.stages)} ${b.stages === 1 ? 'stage' : 'stages'}, none match the filter`
            : truncate(n?.sublabel ?? (b.kind === 'orphans' ? 'Stages that no Spark job listed' : ''), descMax)}
        </text>
        {n && <StatusPill n={n} x={b.w - 12} y={8} />}
        <text x={b.w - 12} y={38} textAnchor="end" className="g-meta">
          {n ? `${fmtDuration(n.duration_ms)} · ${fmtNum(b.stages)} ${b.stages === 1 ? 'stage' : 'stages'}` : `${fmtNum(b.stages)} stages`}
        </text>
      </g>
      {!b.collapsed && b.stages === 0 && (
        <text x={16} y={FLOW.head + 20} className="g-faint">
          No stages ran. Spark skipped them because their output was already computed.
        </text>
      )}
    </g>
  );
}

/** The focused query as a box around all its jobs (Revision 15). */
function QueryBox({ c, sel, focusable, onKey }: { c: NonNullable<FlowLayout['container']>; sel: string | null; focusable: boolean; onKey: (e: React.KeyboardEvent) => void }) {
  const n = c.node;
  const m = STATUS_META[n.vstatus];
  const text = (n.sublabel ?? n.what?.sql_description ?? '').replace(/\s+/g, ' ');
  return (
    <g className={`gqbox ${sel === n.id ? 'sel' : ''} ${n.vstatus === 'failed' ? 'failed' : ''}`} transform={`translate(${c.x},${c.y})`}>
      <rect className="qbox" width={c.w} height={c.h} rx={14} />
      <g className="gnode qhead" data-id={n.id} tabIndex={focusable ? 0 : -1} role="button" aria-label={`${n.label}, ${m.label}`} onKeyDown={onKey}>
        <rect className="qhead-bg" width={c.w} height={QBOX_HEAD - 8} rx={14} />
        <g className="qfold" data-id="unit:fold:box" role="button" aria-label={c.collapsed ? 'Show its jobs' : 'Fold its jobs'}>
          <rect x={8} y={6} width={250} height={20} rx={4} className="qfold-bg" />
          <text x={16} y={20} className="g-kicker">
            {c.collapsed
              ? `▸ SQL query · ${c.jobs} Spark ${c.jobs === 1 ? 'job' : 'jobs'} folded: show`
              : `▾ SQL query · its ${c.jobs} Spark ${c.jobs === 1 ? 'job is' : 'jobs are'} inside`}
          </text>
          <title>{c.collapsed ? 'Show the jobs and stages inside this query' : 'Fold the jobs and stages to see only the query'}</title>
        </g>
        <text x={16} y={40} className="g-title">
          {n.label}
          <tspan className="g-sub" dx={10}>{truncate(text, Math.max(20, Math.floor((c.w - 260) / 6.5)))}</tspan>
        </text>
        <StatusPill n={n} x={c.w - 12} y={8} />
        <text x={c.w - 12} y={40} textAnchor="end" className="g-meta">{fmtDuration(n.duration_ms)}</text>
      </g>
    </g>
  );
}

const boxPortY = (box: Rect) => box.y + Math.min(box.h / 2, QBOX_HEAD / 2 + 4);

/** "+" / "−" on the query box edge: unfold or fold what came before (left) or after (right). */
function FoldButton({ side, f, box }: { side: 'before' | 'after'; f: FoldSide; box: Rect }) {
  const label = f.open ? '−' : `+${f.count}`;
  const w = f.open ? 22 : 16 + label.length * 7;
  // just outside the box edge, clear of its header text
  const x = side === 'before' ? box.x - w / 2 - 6 : box.x + box.w + w / 2 + 6;
  const y = boxPortY(box);
  const what = side === 'before' ? 'upstream' : 'downstream';
  return (
    <g className={`gfold ${f.open ? 'open' : ''}`} data-id={`unit:fold:${side}`} role="button" aria-label={f.open ? `Fold the ${what} side` : `Show ${f.count} ${what}`}
      transform={`translate(${x - w / 2},${y - 11})`}>
      <rect width={w} height={22} rx={11} />
      <text x={w / 2} y={15} textAnchor="middle">{label}</text>
      <title>{f.open ? `Fold the ${what} side` : `Show ${f.count} ${what}: ${f.names}`}</title>
    </g>
  );
}

/** A query or job linked to the focused one: before it (left) or after it (right); click opens it.
 * Its arrow goes to the box, or to the card one step closer to the box (the chain view). */
function CtxCard({ c, side, r, box, to }: { c: FlowCtx; side: 'before' | 'after'; r: Rect; box: Rect; to: Rect | null }) {
  const failed = c.status === 'failed';
  const y = r.y + r.h / 2;
  const by = boxPortY(box);
  const d = to
    ? side === 'before' ? curve(r.x + r.w, y, to.x - 2, to.y + to.h / 2) : curve(to.x + to.w, to.y + to.h / 2, r.x - 2, y)
    : side === 'before' ? curve(r.x + r.w, y, box.x - 34, by) : curve(box.x + box.w + 34, by, r.x - 2, y);
  const p = passed(c.rows, c.bytes);
  const [lx, ly] = to
    ? side === 'before' ? [(r.x + r.w + to.x) / 2, (y + to.y + to.h / 2) / 2] : [(to.x + to.w + r.x) / 2, (to.y + to.h / 2 + y) / 2]
    : side === 'before' ? [(r.x + r.w + box.x - 34) / 2, (y + by) / 2] : [(box.x + box.w + 34 + r.x) / 2, (by + y) / 2];
  return (
    <g>
      <path className="gedge ctx" d={d} markerEnd="url(#ga-link)"><title>{p ? `Passed along: ${p}` : c.why}</title></path>
      {c.rows != null && <text className="g-pass" x={lx} y={ly - 6} textAnchor="middle">{fmtRows(c.rows)} rows<title>{`Passed along: ${p}`}</title></text>}
      <g className={`gnode caller ctxcard ${failed ? 'failed' : ''}`} transform={`translate(${r.x},${r.y})`} data-id={`unit:${c.unit}`} role="button" aria-label={`${side}: ${c.label}`}>
        <rect className="body" width={r.w} height={r.h} rx={8} />
        <text x={12} y={17} className="g-kicker">{truncate(c.why, 36)}</text>
        <text x={12} y={36} className="g-title">{c.label}{failed ? ' ✕' : ''}</text>
        <text x={12} y={54} className="g-sub">{truncate(c.what.replace(/\s+/g, ' '), 36)}</text>
        <title>{`${side === 'before' ? 'Before' : 'After'}: ${c.label} ${c.why}${p ? ` (${p})` : ''}. Click to open it.`}</title>
      </g>
    </g>
  );
}

/* ------------------------------------------------------------------ edges */

/** "38.7M rows · 2.1 GB": what a link passed along (rows a stage wrote to the shuffle, or a query wrote to a table). */
const passed = (rows?: number | null, bytes?: number | null) =>
  [rows != null ? `${fmtRows(rows)} rows` : null, bytes ? fmtBytes(bytes) : null].filter(Boolean).join(' · ');


const curve = (sx: number, sy: number, tx: number, ty: number) => {
  const dx = Math.max(28, Math.abs(tx - sx) / 2);
  return `M${sx},${sy} C${sx + dx},${sy} ${tx - dx},${ty} ${tx},${ty}`;
};

function FlowEdges({ m, L, sel, dim }: { m: GModel; L: FlowLayout; sel: string | null; dim: boolean }) {
  const out: ReactNode[] = [];
  m.edges.forEach((e, i) => {
    const a = L.pos.get(e.source);
    const b = L.pos.get(e.target);
    if (!a || !b) return;
    const hot = sel !== null && (e.source === sel || e.target === sel);
    const faded = dim && (L.dimmed.has(e.source) || L.dimmed.has(e.target));
    const cls = `gedge ${e.kind} ${hot ? 'hot' : ''} ${faded ? 'dim' : ''}`;
    if (e.kind === 'depends') {
      const reused = e.label === 'reused' || m.byId.get(e.source)?.parent !== m.byId.get(e.target)?.parent;
      if (reused && !hot) {
        // a long edge across job boxes is noise: draw a short dashed stub into the stage instead
        const src = m.byId.get(e.source);
        const p = passed(e.rows, e.bytes);
        out.push(
          <g key={i}>
            <path className={`${cls} reused`} d={`M${b.x - 26},${b.y + b.h / 2 - 14} Q${b.x - 14},${b.y + b.h / 2} ${b.x - 2},${b.y + b.h / 2}`} markerEnd="url(#ga-dep)">
              <title>{`Reads the output of ${src?.label ?? 'a stage'} from an earlier job${p ? `: ${p}` : ''} (select either stage to see the link)`}</title>
            </path>
            {e.rows != null && <text className="g-pass" x={b.x - 28} y={b.y + b.h / 2 - 18} textAnchor="end">{fmtRows(e.rows)} rows<title>{`From ${src?.label ?? 'a stage'} of an earlier job: ${p}`}</title></text>}
          </g>,
        );
        return;
      }
      const p = passed(e.rows, e.bytes);
      const mx = (a.x + a.w + b.x) / 2, my = (a.y + a.h / 2 + b.y + b.h / 2) / 2;
      out.push(
        <g key={i}>
          <path className={`${cls} ${reused ? 'reused' : ''}`} d={curve(a.x + a.w, a.y + a.h / 2, b.x - 2, b.y + b.h / 2)} markerEnd={`url(#ga-${hot ? 'hot' : 'dep'})`}>
            <title>{`${reused ? 'Reads the output of a stage computed by an earlier job' : 'Reads the shuffle output of this stage'}${p ? `: ${p}` : ''}`}</title>
          </path>
          {e.rows != null && <text className={`g-pass ${faded ? 'dim' : ''}`} x={mx} y={my - 5} textAnchor="middle">{fmtRows(e.rows)}<title>{p}</title></text>}
        </g>,
      );
    } else if (e.kind === 'retry') {
      const x = a.x + a.w / 2;
      const y0 = a.y + a.h;
      const y1 = b.y - 2;
      const label = e.label ? `retry: ${e.label}` : 'retried';
      out.push(
        <g key={i} className={cls}>
          <path d={`M${x},${y0} L${x},${y1}`} markerEnd="url(#ga-retry)" />
          <text x={x + 8} y={(y0 + y1) / 2 + 4} className="g-retry">
            {truncate(label, 34)}
            <title>{label}</title>
          </text>
        </g>,
      );
    } else if (e.kind === 'runs_query' || e.kind === 'from_connect') {
      if (L.container && e.source === L.container.node.id) return; // its jobs are drawn inside it
      out.push(<path key={i} className={cls} d={curve(a.x + a.w, a.y + a.h / 2, b.x - 2, b.y + 22)} markerEnd={`url(#ga-${hot ? 'hot' : 'link'})`} />);
    }
  });
  return <g>{out}</g>;
}

/* ------------------------------------------------------------------ tooltip */

function Tip({ n, x, y }: { n: GNode; x: number; y: number }) {
  const rows: [string, string][] = [];
  rows.push(['Status', STATUS_META[n.vstatus].label]);
  if (n.start) rows.push(['Started', fmtTs(n.start)]);
  rows.push(['Duration', fmtDuration(n.duration_ms ?? (n.start && n.end ? n.end - n.start : null))]);
  const mt = n.metrics ?? {};
  if (mt.tasks !== undefined && mt.tasks !== null) rows.push(['Tasks', `${fmtNum(mt.tasks)}${mt.failed_tasks ? `, ${fmtNum(mt.failed_tasks)} failed` : ''}`]);
  if ((mt.attempts ?? 1) > 1) rows.push(['Attempt', `${(mt.attempt ?? 0) + 1} of ${mt.attempts}`]);
  for (const k of ['disk_spill', 'mem_spill', 'shuffle_read', 'shuffle_write'] as const) if (mt[k]) rows.push([METRIC_META[k].label, fmtBytes(mt[k])]);
  if (mt.skew) rows.push(['Skew (max / median task)', fmtSkew(mt.skew)]);
  if (mt.gc_share) rows.push(['GC share', fmtPct(mt.gc_share)]);
  return (
    <div className="tooltip" style={{ left: Math.min(x + 14, window.innerWidth - 340), top: y + 14 > window.innerHeight - 220 ? y - 200 : y + 14 }}>
      <div className="tt-title">
        {typeLabel[n.type]}: {n.label}
        {n.attempt ? `, attempt ${n.attempt + 1}` : ''}
      </div>
      {n.sublabel && <div className="small ink2" style={{ marginBottom: 4 }}>{truncate(n.sublabel, 90)}</div>}
      {rows.map(([k, v]) => (
        <div className="tt-row" key={k}>
          <span>{k}</span>
          <span>{v}</span>
        </div>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ main */

export interface GraphCanvasProps {
  model: GModel;
  mode: Mode;
  filters: FilterKey[];
  selected: string | null;
  onSelect: (id: string | null) => void;
  /** bump to center the selected node */
  focusTick: number;
  /** queries before / after the focused one (drawn left / right of its box) */
  around?: FlowAround;
  /** open another query or job (a before / after card was clicked) */
  onUnit?: (unit: string) => void;
}

const ZOOM_MIN = 0.12;
const ZOOM_MAX = 2.4;

export function GraphCanvas({ model, mode, filters, selected, onSelect, focusTick, around, onUnit }: GraphCanvasProps) {
  const [boxRef, size] = useBox<HTMLDivElement>();
  const svgRef = useRef<SVGSVGElement>(null);
  const flow = useMemo(() => (mode === 'flow' ? flowLayout(model, filters, around) : null), [model, filters, mode, around]);
  const time = useMemo(() => (mode === 'time' ? timeLayout(model, filters) : null), [model, filters, mode]);
  const [view, setView] = useState<View>({ k: 1, tx: 0, ty: 0 });
  const [hover, setHover] = useState<{ n: GNode; x: number; y: number } | null>(null);
  const drag = useRef<{ x: number; y: number; v: View; moved: boolean; id: string | null } | null>(null);
  const [grabbing, setGrabbing] = useState(false);
  const W = size.w;
  const H = size.h;
  const plotW = Math.max(200, W - TIME.label - 24);
  const px = time ? plotW / (time.t1 - time.t0) : 1;

  const fit = useCallback(() => {
    if (!W || !H) return;
    if (flow) {
      const k = clamp(Math.min((W - 32) / flow.width, (H - 32) / flow.height, 1), ZOOM_MIN, 1);
      setView({ k, tx: Math.max(16, (W - flow.width * k) / 2), ty: 16 });
    } else setView({ k: 1, tx: 0, ty: 0 });
  }, [flow, W, H]);
  const actual = useCallback(() => setView(flow ? { k: 1, tx: 16, ty: 16 } : { k: 1, tx: 0, ty: 0 }), [flow]);

  // refit when the layout or the mode changes (not on every filter flip for flow: keep the user's place)
  const fitKey = `${model.ctx}|${mode}|${W > 0}|${flow ? Math.round(flow.width) : ''}`;
  const lastFit = useRef('');
  useLayoutEffect(() => {
    if (!W || lastFit.current === fitKey) return;
    lastFit.current = fitKey;
    if (flow) {
      // start readable: fit the width when that keeps text legible, otherwise 75% from the top-left
      const k = clamp((W - 32) / flow.width, 0.75, 1);
      const c = flow.container;
      // too wide to show whole (a long chain): keep the focused query in the middle
      const tx = flow.width * k > W - 32 && c ? Math.min(16, W / 2 - (c.x + c.w / 2) * k) : Math.max(16, (W - flow.width * k) / 2);
      setView({ k, tx, ty: 16 });
    } else fit();
  }, [fitKey, fit, flow, W]);

  // center the selected node when asked (list / URL navigation)
  useEffect(() => {
    if (!selected || !focusTick || !W) return;
    if (flow && flow.width * view.k <= W - 16 && flow.height * view.k <= H - 16) return; // all of it is already on screen
    if (flow) {
      const r = flow.pos.get(selected);
      if (!r) return;
      setView((v) => {
        const k = Math.max(v.k, 0.75);
        const focusH = Math.min(r.h, H / k - 40);
        // center it, but never pull the graph's left/top edge into the middle of the canvas
        return { k, tx: Math.min(16, W / 2 - (r.x + Math.min(r.w, W / k - 40) / 2) * k), ty: Math.min(16, H / 2 - (r.y + focusH / 2) * k) };
      });
    } else if (time) {
      const it = time.items.get(selected);
      if (!it) return;
      setView((v) => {
        const sx = TIME.label + (it.s - time.t0) * px * v.k;
        const tx = sx + v.tx < TIME.label + 20 || sx + v.tx > W - 80 ? -(sx - TIME.label - plotW * 0.25) : v.tx;
        return { ...v, tx, ty: clamp(H / 2 - it.y - TIME.axis, -(time.height - H + TIME.axis + 40), 0) };
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [focusTick]);

  // wheel zoom (native, non-passive so the page does not scroll)
  useEffect(() => {
    const el = svgRef.current;
    if (!el) return;
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      const r = el.getBoundingClientRect();
      const cx = e.clientX - r.left;
      const cy = e.clientY - r.top;
      const f = Math.exp(-e.deltaY * (e.deltaMode === 1 ? 0.05 : 0.0016));
      setView((v) => {
        if (mode === 'flow') {
          const k = clamp(v.k * f, ZOOM_MIN, ZOOM_MAX);
          return { k, tx: cx - ((cx - v.tx) * k) / v.k, ty: cy - ((cy - v.ty) * k) / v.k };
        }
        if (e.shiftKey) return { ...v, ty: clamp(v.ty - e.deltaY, -(Math.max(0, (time?.height ?? 0) - H + TIME.axis + 40)), 0) };
        const k = clamp(v.k * f, 1, 5000);
        const L = TIME.label;
        const x = Math.max(L, cx);
        return { k, tx: x - L - ((x - L - v.tx) * k) / v.k, ty: v.ty };
      });
    };
    el.addEventListener('wheel', onWheel, { passive: false });
    return () => el.removeEventListener('wheel', onWheel);
  }, [mode, time, H]);

  const onPointerDown = (e: React.PointerEvent) => {
    if (e.button !== 0) return;
    const t = (e.target as Element).closest('[data-id]');
    drag.current = { x: e.clientX, y: e.clientY, v: view, moved: false, id: t?.getAttribute('data-id') ?? null };
  };
  const onPointerMove = (e: React.PointerEvent) => {
    const d = drag.current;
    if (d) {
      const dx = e.clientX - d.x;
      const dy = e.clientY - d.y;
      if (!d.moved && Math.hypot(dx, dy) > 4) {
        d.moved = true;
        setGrabbing(true);
        (e.currentTarget as Element).setPointerCapture(e.pointerId);
        setHover(null);
      }
      if (d.moved) {
        if (mode === 'flow') setView({ ...d.v, tx: d.v.tx + dx, ty: d.v.ty + dy });
        else setView({ ...d.v, tx: d.v.tx + dx, ty: clamp(d.v.ty + dy, -(Math.max(0, (time?.height ?? 0) - H + TIME.axis + 40)), 0) });
        return;
      }
    }
    const t = (e.target as Element).closest('[data-id]');
    const id = t?.getAttribute('data-id');
    const n = id ? model.byId.get(id) : undefined;
    setHover(n && n.type !== 'job' ? { n, x: e.clientX, y: e.clientY } : null);
  };
  const onPointerUp = () => {
    const d = drag.current;
    drag.current = null;
    setGrabbing(false);
    if (d && !d.moved) {
      setHover(null);
      if (d.id?.startsWith('unit:')) onUnit?.(d.id.slice(5));
      else onSelect(d.id);
    }
  };
  const keyFor = (id: string) => (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      onSelect(id);
    }
  };
  const focusable = model.nodes.length <= 300;
  const nudge = (f: number) =>
    setView((v) => {
      if (mode === 'flow') {
        const k = clamp(v.k * f, ZOOM_MIN, ZOOM_MAX);
        return { k, tx: W / 2 - ((W / 2 - v.tx) * k) / v.k, ty: H / 2 - ((H / 2 - v.ty) * k) / v.k };
      }
      const k = clamp(v.k * f, 1, 5000);
      const c = (W + TIME.label) / 2 - TIME.label;
      return { ...v, k, tx: c - ((c - v.tx) * k) / v.k };
    });

  // keyboard pan / zoom on the canvas itself
  const onCanvasKey = (e: React.KeyboardEvent) => {
    if ((e.target as Element) !== e.currentTarget) return;
    const step = 60;
    const map: Record<string, () => void> = {
      ArrowLeft: () => setView((v) => ({ ...v, tx: v.tx + step })),
      ArrowRight: () => setView((v) => ({ ...v, tx: v.tx - step })),
      ArrowUp: () => setView((v) => ({ ...v, ty: Math.min(mode === 'time' ? 0 : Infinity, v.ty + step) })),
      ArrowDown: () => setView((v) => ({ ...v, ty: v.ty - step })),
      '+': () => nudge(1.25),
      '=': () => nudge(1.25),
      '-': () => nudge(0.8),
      '0': fit,
    };
    const fn = map[e.key];
    if (fn) {
      e.preventDefault();
      fn();
    }
  };

  return (
    <div
      className="graph-canvas"
      ref={boxRef}
      // a small graph gets a box its own size, not a screen-tall one
      style={{ height: flow ? `min(${Math.round(flow.height + 40)}px, calc(100vh - var(--header-h) - 180px))` : time ? `min(${Math.round(time.height + TIME.axis + 40)}px, calc(100vh - var(--header-h) - 180px))` : undefined, minHeight: 220 }}
    >
      <div className="graph-tools" role="toolbar" aria-label="Zoom">
        <button className="btn small" onClick={() => nudge(1.25)} aria-label="Zoom in">
          +
        </button>
        <button className="btn small" onClick={() => nudge(0.8)} aria-label="Zoom out">
          −
        </button>
        <button className="btn small" onClick={fit}>
          Fit
        </button>
        <button className="btn small" onClick={actual}>
          {mode === 'flow' ? '100%' : 'Reset'}
        </button>
      </div>
      {W > 0 && (
        <svg
          ref={svgRef}
          width={W}
          height={H}
          className={`graph-svg ${grabbing ? 'grabbing' : ''}`}
          tabIndex={0}
          role="application"
          aria-label="Job and stage graph. Drag to pan, scroll or plus and minus to zoom, arrow keys to move, 0 to fit. Use the node list below to pick a node with the keyboard."
          onPointerDown={onPointerDown}
          onPointerMove={onPointerMove}
          onPointerUp={onPointerUp}
          onPointerLeave={() => setHover(null)}
          onKeyDown={onCanvasKey}
        >
          <defs>
            {(
              [
                ['dep', 'var(--border-strong)'],
                ['hot', 'var(--text-2)'],
                ['retry', 'var(--st-warn)'],
                ['link', 'var(--link)'],
              ] as const
            ).map(([k, c]) => (
              <marker key={k} id={`ga-${k}`} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
                <path d="M0,1 L10,5 L0,9 z" style={{ fill: c }} />
              </marker>
            ))}
          </defs>
          {flow ? (
            <FlowScene m={model} L={flow} view={view} W={W} H={H} sel={selected} filters={filters} focusable={focusable} keyFor={keyFor} />
          ) : time ? (
            <TimeScene m={model} L={time} view={view} W={W} H={H} px={px} sel={selected} focusable={focusable} keyFor={keyFor} />
          ) : null}
        </svg>
      )}
      {hover && !grabbing && <Tip n={hover.n} x={hover.x} y={hover.y} />}
    </div>
  );
}

/* ------------------------------------------------------------------ flow scene */

function FlowScene({ m, L, view, W, H, sel, filters, focusable, keyFor }: { m: GModel; L: FlowLayout; view: View; W: number; H: number; sel: string | null; filters: FilterKey[]; focusable: boolean; keyFor: (id: string) => (e: React.KeyboardEvent) => void }) {
  // visible world rect (cull what is far off-screen)
  const vx0 = -view.tx / view.k - 300;
  const vy0 = -view.ty / view.k - 300;
  const vx1 = (W - view.tx) / view.k + 300;
  const vy1 = (H - view.ty) / view.k + 300;
  const vis = (r: Rect) => r.x + r.w >= vx0 && r.x <= vx1 && r.y + r.h >= vy0 && r.y <= vy1;
  const active = filters.length > 0;
  const appR = m.app ? L.pos.get(m.app.id) : undefined;
  return (
    <g transform={`translate(${view.tx},${view.ty}) scale(${view.k})`} className={`g-scene ${view.k < 0.45 ? 'far' : ''}`}>
      {L.spine && (
        <g className="g-spine">
          <path d={`M${L.spine.x},${L.spine.y0} L${L.spine.x},${L.spine.ticks[L.spine.ticks.length - 1]}`} />
          {L.boxes.map((b, i) => (
            <path key={b.id} d={`M${L.spine!.x},${L.spine!.ticks[i]} L${b.x - 2},${L.spine!.ticks[i]}`} markerEnd="url(#ga-dep)" />
          ))}
        </g>
      )}
      {L.container && <QueryBox c={L.container} sel={sel} focusable={focusable} onKey={keyFor(L.container.node.id)} />}
      {L.container && L.ctx.map(({ c, side, r }) => (
        <CtxCard key={`${side}${c.unit}`} c={c} side={side} r={r} box={L.container!}
          to={c.link ? L.ctx.find((o) => o.side === side && o.c.unit === c.link)?.r ?? null : null} />
      ))}
      {L.container && L.fold?.before && <FoldButton side="before" f={L.fold.before} box={L.container} />}
      {L.container && L.fold?.after && <FoldButton side="after" f={L.fold.after} box={L.container} />}
      {L.boxes.filter(vis).map((b) => (
        <JobBox key={b.id} b={b} n={b.node} selected={!!b.node && b.node.id === sel} dim={active && !!b.node && L.dimmed.has(b.node.id)} focusable={focusable} onKey={b.node ? keyFor(b.node.id) : () => {}} />
      ))}
      <FlowEdges m={m} L={L} sel={sel} dim={active} />
      {m.app && appR && (
        <g className={`gnode app ${sel === m.app.id ? 'sel' : ''}`} transform={`translate(${appR.x},${appR.y})`} data-id={m.app.id} tabIndex={focusable ? 0 : -1} role="button" aria-label={`Application ${m.app.label}`} onKeyDown={keyFor(m.app.id)}>
          <rect className="body" width={appR.w} height={appR.h} rx={10} />
          <text x={16} y={23} className="g-app-title">
            {truncate(m.app.label, 30)}
          </text>
          <text x={16} y={42} className="g-app-sub">
            {truncate(`${m.jobs.length} jobs · ${fmtDuration(m.app.duration_ms ?? (m.app.start && m.app.end ? m.app.end - m.app.start : null))} · context ${m.ctx}`, 44)}
          </text>
          <text x={appR.w - 14} y={23} textAnchor="end" className="g-app-status">
            {STATUS_META[m.app.vstatus].glyph} {m.app.vstatus === 'retried' ? 'Retried' : STATUS_META[m.app.vstatus].label}
          </text>
        </g>
      )}
      {m.nodes.map((n) => {
        if (n.type !== 'stage' && n.type !== 'query' && n.type !== 'connect') return null;
        if (L.container && n.id === L.container.node.id) return null;
        const r = L.pos.get(n.id);
        if (!r || !vis(r)) return null;
        const dim = active && L.dimmed.has(n.id);
        return n.type === 'stage' ? (
          <StageCard key={n.id} n={n} r={r} max={m.max} selected={sel === n.id} dim={dim} focusable={focusable} onKey={keyFor(n.id)} />
        ) : (
          <CallerCard key={n.id} n={n} r={r} selected={sel === n.id} dim={dim} focusable={focusable} onKey={keyFor(n.id)} />
        );
      })}
    </g>
  );
}

/* ------------------------------------------------------------------ time scene */

function TimeScene({ m, L, view, W, H, px, sel, focusable, keyFor }: { m: GModel; L: TimeLayout; view: View; W: number; H: number; px: number; sel: string | null; focusable: boolean; keyFor: (id: string) => (e: React.KeyboardEvent) => void }) {
  const LBL = TIME.label;
  const A = TIME.axis;
  const xOf = (t: number) => LBL + (t - L.t0) * px * view.k + view.tx;
  const tOf = (x: number) => L.t0 + (x - LBL - view.tx) / (px * view.k);
  const yOf = (y: number) => y + view.ty + A;
  const v0 = tOf(LBL);
  const v1 = tOf(W);
  const ticks = timeTicks(v0, v1, Math.max(3, Math.floor((W - LBL) / 120)));
  const span = v1 - v0;
  const visRow = (y: number, h: number) => yOf(y) + h >= A && yOf(y) <= H;
  const items = [...L.items.values()];

  const edges: ReactNode[] = [];
  m.edges.forEach((e, i) => {
    if (e.kind === 'contains') return;
    const a = L.items.get(e.source);
    const b = L.items.get(e.target);
    if (!a || !b) return;
    const hot = sel !== null && (e.source === sel || e.target === sel);
    if ((e.kind === 'runs_query' || e.kind === 'from_connect') && !hot) return;
    const sx = Math.min(xOf(a.e), xOf(b.s) - 4);
    const sy = yOf(a.y + a.h / 2);
    const tx = xOf(b.s);
    const ty = yOf(b.y + b.h / 2);
    if (Math.max(sy, ty) < A || Math.min(sy, ty) > H) return;
    const ex = Math.max(sx + 6, tx - 8);
    const d = `M${xOf(a.e)},${sy} L${ex},${sy} L${ex},${ty} L${tx},${ty}`;
    const dim = L.dimmed.has(e.source) || L.dimmed.has(e.target);
    edges.push(
      <path key={i} className={`gedge t ${e.kind} ${hot ? 'hot' : ''} ${dim ? 'dim' : ''} ${e.label === 'reused' ? 'reused' : ''}`} d={d} markerEnd={`url(#ga-${hot ? 'hot' : e.kind === 'retry' ? 'retry' : e.kind === 'depends' ? 'dep' : 'link'})`}>
        <title>{e.kind === 'retry' ? `Retried: ${e.label ?? ''}` : e.kind === 'depends' ? `Reads the output of this stage${e.rows != null ? `: ${e.rows.toLocaleString()} rows` : ''}${e.bytes ? `, ${(e.bytes / 1024 ** 2).toFixed(1)} MB` : ''}` : ''}</title>
      </path>,
    );
  });

  return (
    <g className="g-scene time">
      {/* rows: bands and labels */}
      {L.rows.map((r, i) =>
        visRow(r.y, r.h) ? (
          <g key={i}>
            {r.kind === 'section' && <rect x={0} y={yOf(r.y)} width={W} height={r.h} className="t-section" />}
            {r.kind === 'job' && <rect x={0} y={yOf(r.y)} width={W} height={1} className="t-rule" />}
          </g>
        ) : null,
      )}
      {ticks.map((t) => (
        <line key={t} x1={xOf(t)} x2={xOf(t)} y1={A} y2={H} className="t-grid" />
      ))}
      <g clipPath="url(#t-plot)">
        <clipPath id="t-plot">
          <rect x={LBL} y={A} width={Math.max(0, W - LBL)} height={Math.max(0, H - A)} />
        </clipPath>
        {edges}
        {items.map((it) => {
          const n = it.node;
          const y = yOf(it.y);
          if (y + it.h < A || y > H) return null;
          const x0 = xOf(it.s);
          const x1 = Math.max(x0 + 2, xOf(it.e));
          if (x1 < LBL || x0 > W) return null;
          const w = x1 - x0;
          const st = STATUS_META[n.vstatus];
          const dim = L.dimmed.has(n.id);
          const label = n.type === 'stage' ? `${n.label}${n.attempt ? `.${n.attempt}` : ''}` : n.type === 'job' ? n.label : n.type === 'app' ? n.label : n.type === 'query' ? n.label : truncate((n.what?.statement_text ?? n.label).replace(/\s+/g, ' '), 40);
          const lx = Math.max(x0, LBL) + 7;
          const room = x1 - lx - 6;
          const spill = n.type === 'stage' && ((n.metrics?.disk_spill ?? 0) + (n.metrics?.mem_spill ?? 0) > 0);
          const shuffle = n.type === 'stage' && n.flagSet.has('shuffle_heavy');
          return (
            <g key={n.id} className={`gnode tbar ${n.type} ${sel === n.id ? 'sel' : ''} ${dim ? 'dim' : ''}`} data-id={n.id} tabIndex={focusable ? 0 : -1} role="button" aria-label={`${n.label}, ${st.label}`} onKeyDown={keyFor(n.id)}>
              <rect className="body" x={x0} y={y} width={w} height={it.h} rx={3} style={n.type === 'stage' || n.type === 'job' ? { fill: st.tint, stroke: st.mark } : undefined} />
              {n.type === 'stage' && <rect x={x0} y={y} width={Math.min(3, w)} height={it.h} style={{ fill: st.mark }} />}
              {spill && <rect x={x0} y={y + it.h - 3} width={w} height={3} style={{ fill: METRIC_META.disk_spill.color }} />}
              {shuffle && <rect x={x0} y={y} width={w} height={3} style={{ fill: METRIC_META.shuffle_read.color }} />}
              {room > 30 && (room >= label.length * 6.4 || Math.min(W, xOf(it.next)) - x1 - 10 < label.length * 6.4) ? (
                <text x={lx} y={y + it.h / 2 + 4} className={`t-label ${n.type}`}>
                  {truncate(label, Math.max(3, Math.floor(room / 6.6)))}
                </text>
              ) : (
                Math.min(W, xOf(it.next)) - x1 - 10 > label.length * 6.4 &&
                x1 > LBL && (
                  <text x={x1 + 5} y={y + it.h / 2 + 4} className="t-label out">
                    {label}
                  </text>
                )
              )}
            </g>
          );
        })}
      </g>
      {/* label column */}
      <rect x={0} y={A} width={LBL} height={H} className="t-labels" />
      {L.rows.map((r, i) =>
        visRow(r.y, r.h) && r.kind !== 'lane' ? (
          <g key={i} className={r.dim ? 't-dim' : ''}>
            <text x={12} y={yOf(r.y) + r.h / 2 + (r.sub && r.kind !== 'section' ? -1 : 4)} className={`t-row ${r.kind}`}>
              {truncate(r.label, 30)}
            </text>
            {r.sub && r.kind === 'job' && (
              <text x={12} y={yOf(r.y) + r.h / 2 + 12} className="t-rowsub">
                {truncate(r.sub, 32)}
              </text>
            )}
          </g>
        ) : null,
      )}
      <line x1={LBL} x2={LBL} y1={A} y2={H} className="t-axisline" />
      {/* axis on top, fixed */}
      <rect x={0} y={0} width={W} height={A} className="t-axis" />
      <text x={12} y={19} className="t-tick">
        UTC
      </text>
      {ticks.map((t) => (
        <text key={t} x={xOf(t)} y={19} textAnchor="middle" className="t-tick">
          {tickLabel(t, span)}
        </text>
      ))}
      <line x1={0} x2={W} y1={A - 0.5} y2={A - 0.5} className="t-axisline" />
    </g>
  );
}
