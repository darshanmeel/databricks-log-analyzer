// Revision 17: the tasks of a stage, a job or a query as statistics, not rows.
//   1. All tasks: one row per measure (min, p10, median, p90, max, distribution, total, tasks with any), per task or
//      per second of task time; a histogram of one measure (coloured by executor or by how much each task read). For a
//      job or a query, also each stage's wait for a free core.
//   2. Each executor: the same table, only its share of the tasks.
//   3. Executor by executor: these tasks, the other tasks on it in the same window, combined, against the executor's
//      capacity (cores x the window).
//   4. (stage) The slowest tasks on these executors while the stage ran, any stage, with why each took so long.
// Sections 2 and 3 of a stage, a job and a query alike; with more than 5 executors, each can be hidden.
import { Fragment, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, type SharingExec, type StageSharing, type TaskColumns } from '../api';
import { useAsync, useWidth } from '../hooks';
import { bare, fmtBytes, fmtDuration, fmtNum, fmtPct, fmtTime, unitFor, type Unit } from '../format';
import { to } from '../links';
import { Fold } from './ui';

type Kind = 'ms' | 'b';
type MetricKey = 'start_wait' | 'task_ms' | 'cpu_ms' | 'gc_ms' | 'input_bytes' | 'shuffle_read' | 'shuffle_write' | 'mem_spill' | 'disk_spill' | 'peak_mem' | 'output_bytes' | 'fetch_wait_ms';
const METRICS: { k: MetricKey; label: string; kind: Kind; peak?: boolean; missing?: string; noRate?: boolean }[] = [
  { k: 'start_wait', label: 'Each task waited', kind: 'ms', noRate: true, missing: 'no start times for these tasks' },
  { k: 'task_ms', label: 'Duration', kind: 'ms' },
  { k: 'cpu_ms', label: 'CPU time', kind: 'ms', missing: 'not kept in this output: re-run analyze with task metrics to get it' },
  { k: 'gc_ms', label: 'GC time', kind: 'ms' },
  { k: 'input_bytes', label: 'Read from files', kind: 'b' },
  { k: 'shuffle_read', label: 'Shuffle read', kind: 'b' },
  { k: 'shuffle_write', label: 'Shuffle write', kind: 'b' },
  { k: 'fetch_wait_ms', label: 'Shuffle fetch wait', kind: 'ms' },
  { k: 'mem_spill', label: 'Spill, memory', kind: 'b' },
  { k: 'disk_spill', label: 'Spill, disk', kind: 'b' },
  { k: 'peak_mem', label: 'Peak memory', kind: 'b', peak: true, missing: 'not recorded for these tasks' },
  { k: 'output_bytes', label: 'Output written (files)', kind: 'b' },
];
const MB = 1 << 20;
const BANDS: { label: string; lo: number; hi: number; color: string }[] = [
  { label: '< 10 MiB', lo: 0, hi: 10 * MB, color: 'var(--text-3)' },
  { label: '10 – 128 MiB', lo: 10 * MB, hi: 128 * MB, color: 'var(--series-1)' },
  { label: '128 – 256 MiB', lo: 128 * MB, hi: 256 * MB, color: 'var(--st-warn)' },
  { label: '256 MiB or more', lo: 256 * MB, hi: Infinity, color: 'var(--st-crit)' },
];
const EXEC_COLORS = ['var(--series-1)', 'var(--series-3)', 'var(--series-4)', 'var(--series-6)', 'var(--series-2)', 'var(--series-7)', 'var(--series-5)'];

type Stats = { n: number; min: number; p10: number; p50: number; p90: number; max: number; sum: number; nonzero: number; vals: number[] };
const q = (s: number[], p: number) => (s.length ? s[Math.min(s.length - 1, Math.max(0, Math.round(p * (s.length - 1))))] : 0);
function stats(vals: number[]): Stats | null {
  if (!vals.length) return null;
  const s = [...vals].sort((a, b) => a - b);
  return { n: s.length, min: s[0], p10: q(s, 0.1), p50: q(s, 0.5), p90: q(s, 0.9), max: s[s.length - 1], sum: s.reduce((a, x) => a + x, 0), nonzero: s.filter((x) => x > 0).length, vals: s };
}

type Mode = 'task' | 'rate';
/** The unit of one row: the share of task time in %, or bytes or time in the unit its typical value reaches. */
const rowUnit = (kind: Kind, mode: Mode, vals: number[]): Unit =>
  mode === 'rate' && kind === 'ms' ? { u: '%', f: (v) => (v === null || v === undefined ? '–' : bare(v * 100, true)) } : unitFor(kind, vals, mode === 'rate');
const U = ({ u }: { u: string }) => <span className="unit"> ({u})</span>;

/** A column of the task table as numbers (nulls kept as null). */
const col = (c: TaskColumns, k: string) => (c.cols as Record<string, (number | null)[] | undefined>)[k] ?? [];

/** The values of one measure for the tasks in `idx` (all when null): per task, or per second of the task's time. */
function valuesOf(c: TaskColumns, k: MetricKey, mode: Mode, idx: number[] | null): number[] {
  const v = col(c, k);
  const t = col(c, 'task_ms');
  const out: number[] = [];
  const it = idx ?? v.map((_, i) => i);
  for (const i of it) {
    const x = v[i];
    if (x === null || x === undefined) continue;
    if (mode === 'rate') {
      const ms = t[i];
      if (!ms || k === 'task_ms' || k === 'start_wait') continue;
      out.push(METRICS.find((m) => m.k === k)!.kind === 'ms' ? x / ms : x / (ms / 1000));
    } else out.push(x);
  }
  return out;
}

/** A small histogram of the values from 0 to `scale` (bar height square-root, so a few tasks in the tail still show). */
function Spark({ vals, scale, hot }: { vals: number[]; scale: number; hot?: number | null }) {
  const W = 96, H = 18, N = 20;
  if (!vals.length || !scale) return null;
  const bins = new Array(N).fill(0);
  for (const v of vals) bins[Math.min(N - 1, Math.floor((v / scale) * N))] += 1;
  const top = Math.sqrt(Math.max(...bins)) || 1;
  const hb = hot != null ? Math.min(N - 1, Math.floor((hot / scale) * N)) : -1;
  return (
    <svg width={W} height={H} aria-hidden style={{ display: 'block' }}>
      {bins.map((b, i) => (b ? <rect key={i} x={(i * W) / N} y={H - Math.max(1.5, (Math.sqrt(b) / top) * H)} width={W / N - 1} height={Math.max(1.5, (Math.sqrt(b) / top) * H)}
        fill={i === hb ? 'var(--st-warn)' : 'var(--series-1)'} /> : null))}
      <line x1={0} x2={W} y1={H - 0.5} y2={H - 0.5} stroke="var(--line)" />
    </svg>
  );
}

/** One row per measure: min, p10, median, p90, max (× the median), distribution, total, tasks with any. */
function MetricRows({ c, idx, mode, scaleOf, hideEmpty, waits, totalOf }: {
  c: TaskColumns; idx: number[] | null; mode: Mode; scaleOf: (k: MetricKey) => number; hideEmpty: boolean;
  waits?: number[] | null; totalOf?: (k: MetricKey) => number;
}) {
  const n = idx ? idx.length : (c.cols.task_ms?.length ?? 0);
  const rows = METRICS.map((m) => {
    if (!c.present[m.k]) return { m, st: null as Stats | null, why: m.missing ?? 'not kept in this output' };
    const st = stats(valuesOf(c, m.k, mode, idx));
    const allZero = !st || st.max === 0;
    let why = allZero ? 'none' : '';
    if (allZero && m.k === 'shuffle_read' && (stats(valuesOf(c, 'input_bytes', 'task', idx))?.max ?? 0) > 0) why = 'none: these tasks read files, not shuffle output';
    if (allZero && (m.k === 'mem_spill' || m.k === 'disk_spill')) why = 'none: every task fit in memory';
    return { m, st: allZero ? null : st, why };
  }).filter((r) => !(mode === 'rate' && (r.m.k === 'task_ms' || r.m.noRate))).filter((r) => !hideEmpty || r.st);
  const ws = waits && waits.length ? stats(waits) : null;
  const wu = ws ? unitFor('ms', ws.vals) : null;
  return (
    <>
      {ws && wu && mode === 'task' && (
        <tr className="wait-row">
          <td><b className="wait-text" title="One value per stage: from the stage being submitted to its first task starting. Small here and large below means the stage started at once but its tasks ran in waves.">Stage's first task waited</b><U u={wu.u} /><div className="muted small">one per stage: submitted → its first task started</div></td>
          <td className="num muted">{wu.f(ws.min)}</td><td className="num">{wu.f(ws.p10)}</td><td className="num">{wu.f(ws.p50)}</td>
          <td className="num">{wu.f(ws.p90)}</td><td className="num wait-text"><b>{wu.f(ws.max)}</b></td>
          <td><Spark vals={ws.vals} scale={ws.max} /></td>
          <td className="num wait-text">{wu.f(ws.sum)}</td>
          <td className="num">{fmtNum(ws.vals.filter((x) => x >= 1000).length)} of {fmtNum(ws.n)} stages over 1 s</td>
        </tr>
      )}
      {rows.map(({ m, st, why }) => {
        if (!st) {
          // every task 0: zeros; green for spill and shuffle (none is the good news); a measure not in the logs says so
          if (c.present[m.k])
            return (
              <tr key={m.k} className={`zero-metric ${/spill|shuffle|fetch/.test(m.k) ? 'zero-good' : ''}`} title={why === 'none' ? undefined : why.replace(/^none: /, '')}>
                <td>{m.label}</td>
                {[0, 1, 2, 3, 4].map((i) => <td key={i} className="num">0</td>)}
                <td />
                <td className="num">{m.peak || m.noRate || mode === 'rate' ? '–' : '0'}</td>
                <td className="num">0</td>
              </tr>
            );
          return (
            <tr key={m.k} className="empty-metric">
              <td>{m.label}</td>
              <td colSpan={6} className="muted small">{why}</td>
              <td className="num muted">–</td>
              <td className="num muted">–</td>
            </tr>
          );
        }
        const f = rowUnit(m.kind, mode, st.vals);
        const ratio = st.p50 ? st.max / st.p50 : null;
        const share = totalOf && mode === 'task' && !m.peak && !m.noRate ? (totalOf(m.k) ? st.sum / totalOf(m.k) : null) : null;
        return (
          <tr key={m.k}>
            <td className={m.k.includes('spill') ? 'spill-label' : m.k === 'start_wait' ? 'wait-text' : ''}>{m.label}<U u={f.u} />{m.k === 'start_wait' ? <div className="muted small" title="One value per task: tasks queue behind the stage's own earlier tasks (waves) and behind other work holding the cores.">one per task: stage submitted → this task started</div> : null}</td>
            <td className="num muted">{f.f(st.min)}</td>
            <td className="num">{f.f(st.p10)}</td>
            <td className="num">{f.f(st.p50)}</td>
            <td className="num">{f.f(st.p90)}</td>
            <td className="num">{f.f(st.max)}{ratio && ratio >= 3 ? <span className="ratio" title={`${ratio.toFixed(1)} times the median`}> ×{ratio.toFixed(ratio >= 10 ? 0 : 1)}</span> : null}</td>
            <td><Spark vals={st.vals} scale={scaleOf(m.k) || st.max} /></td>
            <td className="num" style={{ color: m.k.startsWith('shuffle_') ? 'var(--shuf)' : m.k.includes('spill') ? 'var(--spill)' : undefined }}>
              {m.peak || m.noRate || mode === 'rate' ? '–' : f.f(st.sum)}{share != null ? <span className="muted small"> {fmtPct(share, 0)}</span> : null}
            </td>
            <td className="num">{st.nonzero === n ? `all ${fmtNum(n)}` : `${fmtNum(st.nonzero)} of ${fmtNum(n)}`}</td>
          </tr>
        );
      })}
    </>
  );
}

function MetricHead({ first }: { first: string }) {
  return (
    <thead>
      <tr>
        <th>{first}</th><th className="num">Min</th><th className="num">p10</th><th className="num" title="The typical task: the ×N next to the max is against it">Median</th><th className="num">p90</th>
        <th className="num">Max</th><th style={{ width: 104 }}>Distribution</th><th className="num">Total</th><th className="num">Tasks with any</th>
      </tr>
    </thead>
  );
}

function ModePills({ mode, setMode, extra }: { mode: Mode; setMode: (m: Mode) => void; extra?: React.ReactNode }) {
  return (
    <div className="row small" style={{ gap: 6, alignItems: 'center', margin: '6px 0' }}>
      <div className="seg" role="group" aria-label="Per task or per second">
        <button className={mode === 'task' ? 'on' : ''} aria-pressed={mode === 'task'} onClick={() => setMode('task')}>Per task</button>
        <button className={mode === 'rate' ? 'on' : ''} aria-pressed={mode === 'rate'} onClick={() => setMode('rate')}>Per second of task time</button>
      </div>
      {extra}
    </div>
  );
}

/** Section 1: all the tasks. */
function AllTasks({ c, n, what, waits, allRowsLink }: { c: TaskColumns; n: number; what: string; waits?: number[] | null; allRowsLink?: string }) {
  const [mode, setMode] = useState<Mode>('task');
  const scale = (k: MetricKey) => stats(valuesOf(c, k, mode, null))?.max ?? 0;
  return (
    <div className="ts-section">
      <h3 className="ts-h"><span className="ts-num">1</span> All {fmtNum(n)} tasks{c.sampled ? <span className="muted small"> · statistics from {fmtNum(c.n)} of {fmtNum(c.total)} tasks (every failed one, the longest and an even sample)</span> : null}</h3>
      <p className="ts-legend small">
        <b>min</b> the smallest task · <b>p10</b> 10% of tasks are at or below this · <b>median</b> half are at or below this · <b>p90</b> 90% are at or below this, the last 10% are the tail · <b>max</b> the single largest task, with how many times the median it is · <b>distribution</b> how many tasks fall in each slice, from 0 to the max
      </p>
      <ModePills mode={mode} setMode={setMode} extra={allRowsLink ? <Link className="btn small ghost" to={allRowsLink}>All {fmtNum(n)} rows</Link> : null} />
      <div className="table-wrap">
        <table className="table compact ts-table">
          <MetricHead first={`Measure · ${what}`} />
          <tbody><MetricRows c={c} idx={null} mode={mode} scaleOf={scale} hideEmpty={false} waits={waits} /></tbody>
        </table>
      </div>
      <Histogram c={c} />
      <BigReads c={c} />
    </div>
  );
}

type HMetric = 'task_ms' | 'gc_ms' | 'read' | 'shuffle_write';
function niceStep(raw: number, kind: Kind): number {
  const base = kind === 'ms' ? [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10_000, 15_000, 30_000, 60_000, 120_000, 300_000, 600_000, 900_000, 1_800_000, 3_600_000] : [];
  if (base.length) return base.find((s) => s >= raw) ?? base[base.length - 1];
  const p = Math.pow(2, Math.ceil(Math.log2(Math.max(1, raw))));
  return p;
}

/** How long the tasks took (or how much they read / wrote / spent in GC), in slices, coloured by executor or by
 * how much each task read; lines mark p10, median and p90. */
function Histogram({ c }: { c: TaskColumns }) {
  const [metric, setMetric] = useState<HMetric>('task_ms');
  const [colour, setColour] = useState<'none' | 'executor' | 'read'>('none');
  const [ref, width] = useWidth<HTMLDivElement>();
  const n = c.cols.task_ms?.length ?? 0;
  const readOf = (i: number) => (col(c, 'input_bytes')[i] ?? 0) + (col(c, 'shuffle_read')[i] ?? 0);
  const valOf = (i: number): number | null => (metric === 'read' ? readOf(i) : (col(c, metric)[i] ?? null));
  const kind: Kind = metric === 'task_ms' || metric === 'gc_ms' ? 'ms' : 'b';
  const any = (k: HMetric) => { for (let i = 0; i < n; i++) if ((k === 'read' ? readOf(i) : (col(c, k)[i] ?? 0)) > 0) return true; return false; };
  const avail: [HMetric, string, boolean][] = ([['task_ms', 'Duration'], ['gc_ms', 'GC'], ['read', 'Read'], ['shuffle_write', 'Shuffle write']] as [HMetric, string][])
    .map(([k, l]) => [k, l, any(k)] as [HMetric, string, boolean]);
  const why0: Record<HMetric, string> = { task_ms: 'no task durations', gc_ms: 'no task spent time in GC', read: 'no task read data', shuffle_write: 'no task wrote shuffle data (nothing after these tasks needed a shuffle)' };
  const model = useMemo(() => {
    const idx: number[] = [];
    const vals: number[] = [];
    for (let i = 0; i < n; i++) { const v = valOf(i); if (v !== null) { idx.push(i); vals.push(v); } }
    const st = stats(vals);
    if (!st || st.max <= 0) return null;
    const step = niceStep(st.max / 22, kind);
    const N = Math.max(1, Math.ceil((st.max + 1) / step));
    const execs = [...new Set(idx.map((i) => String(col(c, 'executor_id')[i] ?? '?')))].sort((a, b) => Number(a) - Number(b) || a.localeCompare(b));
    const groups = colour === 'executor' ? execs : colour === 'read' ? BANDS.map((b) => b.label) : ['all'];
    const bins = Array.from({ length: N }, () => new Array(groups.length).fill(0));
    idx.forEach((i, j) => {
      const b = Math.min(N - 1, Math.floor(vals[j] / step));
      const g = colour === 'executor' ? execs.indexOf(String(col(c, 'executor_id')[i] ?? '?'))
        : colour === 'read' ? BANDS.findIndex((x) => readOf(i) >= x.lo && readOf(i) < x.hi) : 0;
      bins[b][Math.max(0, g)] += 1;
    });
    return { st, step, N, bins, groups };
  }, [c, metric, colour, n]); // eslint-disable-line react-hooks/exhaustive-deps
  const controls = (
    <div className="row small" style={{ gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
      <span className="muted">Measure</span>
      <div className="seg" role="group" aria-label="Measure">
        {avail.map(([k, l, has]) => (
          <button key={k} className={metric === k ? 'on' : ''} aria-pressed={metric === k} disabled={!has} title={has ? undefined : `0 for every task: ${why0[k]}`}
            style={has ? undefined : { opacity: 0.45, cursor: 'not-allowed' }} onClick={() => setMetric(k)}>{l}</button>
        ))}
      </div>
      <span className="muted">Colour by</span>
      <div className="seg" role="group" aria-label="Colour by">
        {([['none', 'none'], ['executor', 'executor'], ['read', 'data read']] as const).map(([k, l]) => (
          <button key={k} className={colour === k ? 'on' : ''} aria-pressed={colour === k} onClick={() => setColour(k)}>{l}</button>
        ))}
      </div>
    </div>
  );
  if (!model) {
    if (!avail.some((a) => a[2])) return null;
    return (
      <div className="ts-hist">
        {controls}
        <p className="muted small" style={{ margin: '8px 0' }}>{avail.find((a) => a[0] === metric)?.[1]}: 0 for every task, {why0[metric]}.</p>
      </div>
    );
  }
  const { st, step, N, bins, groups } = model;
  const full = Math.max(320, width || 800), H = 150, PAD = 26;
  const bw = Math.min(64, (full - 40) / N);
  const W = Math.min(full, N * bw + 40);
  const tops = bins.map((b) => b.reduce((a, x) => a + x, 0));
  const top = Math.sqrt(Math.max(...tops)) || 1;
  const x = (v: number) => 10 + (v / (N * step)) * (N * bw);
  const h = (k: number) => (Math.sqrt(k) / top) * (H - PAD - 14);
  // one unit for the axis, the lines and the tooltips: written once, the numbers bare
  const un = unitFor(kind, st.vals);
  const f = un.f;
  const colorOf = (g: number) => (colour === 'executor' ? EXEC_COLORS[g % EXEC_COLORS.length] : colour === 'read' ? BANDS[g].color : 'var(--series-1)');
  const tickEvery = Math.max(1, Math.round(N / 8));
  const label = avail.find(([k]) => k === metric)?.[1] ?? '';
  return (
    <div className="ts-hist">
      <div className="row small" style={{ gap: 8, alignItems: 'center', flexWrap: 'wrap', margin: '14px 0 4px' }}
        title={`Slices of ${f(step)} ${un.u}. Bar height is square-root, so the few tasks in the tail stay visible. The dashed lines mark p10, the median and p90.`}>
        <b>{label} per task</b><span className="muted">({un.u}) · {fmtNum(st.n)} tasks <span aria-hidden>ⓘ</span></span>
      </div>
      {controls}
      <div ref={ref} style={{ position: 'relative' }}>
        <svg width={W} height={H} role="img" aria-label={`Histogram of ${label}`} style={{ display: 'block' }}>
          {bins.map((b, i) => {
            let y = H - PAD;
            return (
              <g key={i}>
                {b.map((k, g) => {
                  if (!k) return null;
                  const hh = (h(tops[i]) * k) / tops[i];
                  y -= hh;
                  return <rect key={g} x={10 + i * bw + 1} y={y} width={Math.max(1, bw - 2)} height={hh} fill={colorOf(g)}><title>{`${f(i * step)} – ${f((i + 1) * step)} ${un.u}: ${fmtNum(k)} tasks${groups.length > 1 ? ` (${groups[g]})` : ''}`}</title></rect>;
                })}
                {tops[i] > 0 && bw >= 18 && <text x={10 + i * bw + bw / 2} y={H - PAD - h(tops[i]) - 3} fontSize={10} textAnchor="middle" fill="var(--text-2)">{fmtNum(tops[i])}</text>}
              </g>
            );
          })}
          {([['p10', st.p10, 'end'], ['median', st.p50, 'start'], ['p90', st.p90, 'start']] as [string, number, 'start' | 'end'][]).map(([l, v, anchor], j) => (
            <g key={l}>
              <line x1={x(v)} x2={x(v)} y1={4} y2={H - PAD} stroke="var(--text)" strokeOpacity={0.6} strokeDasharray="3 3" />
              <text x={x(v) + (anchor === 'end' ? -4 : 4)} y={12 + (j === 2 ? 0 : j * 12)} fontSize={10.5} textAnchor={anchor} fill="var(--text)"
                style={{ paintOrder: 'stroke', stroke: 'var(--surface)', strokeWidth: 3 }}>{l === 'median' ? 'med' : l} {f(v)}</text>
            </g>
          ))}
          <line x1={10} x2={10 + N * bw} y1={H - PAD} y2={H - PAD} stroke="var(--line)" />
          {Array.from({ length: N + 1 }, (_, i) => i).filter((i) => i % tickEvery === 0).map((i) => (
            <text key={i} x={10 + i * bw} y={H - PAD + 14} fontSize={10.5} textAnchor={i === 0 ? 'start' : i === N ? 'end' : 'middle'} fill="var(--text-2)">{i === 0 ? '0' : f(i * step)}</text>
          ))}
        </svg>
      </div>
      {groups.length > 1 && (
        <div className="vsplit-legend">{groups.map((g, i) => <span key={g}><i style={{ background: colorOf(i) }} />{colour === 'executor' ? `exec ${g}` : g}</span>)}</div>
      )}
    </div>
  );
}

/** One line when tasks read more than the ~128 MiB a task is sized for. */
function BigReads({ c }: { c: TaskColumns }) {
  const n = c.cols.task_ms?.length ?? 0;
  let big = 0, bigD = 0, all = 0;
  for (let i = 0; i < n; i++) {
    const r = (col(c, 'input_bytes')[i] ?? 0) + (col(c, 'shuffle_read')[i] ?? 0);
    all += r;
    if (r >= 128 * MB) { big += 1; bigD += r; }
  }
  if (!big || !all) return null;
  return (
    <p className="small" style={{ margin: '8px 0 0' }} title="More than the ~128 MiB a task is sized for: files that could not be split, or too few shuffle partitions.">
      <span className="why warn">{big === n ? 'every task ≥ 128 MiB' : `${fmtPct(big / n, 0)} of tasks ≥ 128 MiB · ${fmtPct(bigD / all, 0)} of the data`}</span>
      <span className="muted"> too big per task <span aria-hidden>ⓘ</span></span>
    </p>
  );
}

/** Which executors sections 2 and 3 show: all of them up to 5; beyond that the 5 with the most of these tasks' time,
 * and a chip per executor to show or hide it. */
type Pick = { order: string[]; shown: (id: string) => boolean; toggle: (id: string) => void; setAll: (on: boolean) => void; many: boolean };
const PICK_MAX = 5;
function usePick(c: TaskColumns): Pick {
  const order = useMemo(() => {
    const ex = col(c, 'executor_id') as unknown as (string | null)[];
    const t = col(c, 'task_ms');
    const m = new Map<string, number>();
    ex.forEach((e, i) => m.set(String(e ?? '?'), (m.get(String(e ?? '?')) ?? 0) + (t[i] ?? 0)));
    return [...m.entries()].sort((a, b) => b[1] - a[1]).map(([k]) => k);
  }, [c]);
  const [hidden, setHidden] = useState<Set<string> | null>(null);
  const hid = hidden ?? new Set(order.slice(PICK_MAX));
  return {
    order, many: order.length > PICK_MAX,
    shown: (id) => !hid.has(id),
    toggle: (id) => { const h = new Set(hid); if (h.has(id)) h.delete(id); else h.add(id); setHidden(h); },
    setAll: (on) => setHidden(on ? new Set() : new Set(order.slice(PICK_MAX))),
  };
}
function ExecPicker({ pick }: { pick: Pick }) {
  if (!pick.many) return null;
  const n = pick.order.filter(pick.shown).length;
  return (
    <div className="exec-pick small">
      <span className="muted">Showing {fmtNum(n)} of {fmtNum(pick.order.length)} executors (most work first):</span>
      {pick.order.map((id) => (
        <button key={id} className={`chip-btn ${pick.shown(id) ? 'on' : ''}`} aria-pressed={pick.shown(id)} onClick={() => pick.toggle(id)}>exec {id}</button>
      ))}
      <button className="btn small ghost" onClick={() => pick.setAll(n < pick.order.length)}>{n < pick.order.length ? 'Show all' : `Only the top ${PICK_MAX}`}</button>
    </div>
  );
}

/** Section 2: each executor, the same table over its share of the tasks. */
function PerExecutor({ c, pick, others }: { c: TaskColumns; pick: Pick; others: string }) {
  const [mode, setMode] = useState<Mode>('task');
  const [hideEmpty, setHideEmpty] = useState(true);
  const [open, setOpen] = useState<Set<string>>(new Set());
  const flip = (id: string) => setOpen((o) => { const n = new Set(o); if (n.has(id)) n.delete(id); else n.add(id); return n; });
  const ex = col(c, 'executor_id') as unknown as (string | null)[];
  const t = col(c, 'task_ms');
  const groups = useMemo(() => {
    const m = new Map<string, number[]>();
    ex.forEach((e, i) => { const k = String(e ?? '?'); m.set(k, [...(m.get(k) ?? []), i]); });
    return [...m.entries()].map(([id, idx]) => ({ id, idx, ms: idx.reduce((a, i) => a + (t[i] ?? 0), 0), max: Math.max(...idx.map((i) => t[i] ?? 0)) }))
      .sort((a, b) => b.ms - a.ms);
  }, [c]); // eslint-disable-line react-hooks/exhaustive-deps
  if (groups.length < 2) return null;
  const totMs = groups.reduce((a, g) => a + g.ms, 0) || 1;
  const slowest = groups.reduce((a, g) => (g.max > a.max ? g : a));
  const scale = (k: MetricKey) => stats(valuesOf(c, k, mode, null))?.max ?? 0;
  const total = (k: MetricKey) => stats(valuesOf(c, k, 'task', null))?.sum ?? 0;
  const info = new Map(c.executors.map((e) => [String(e.executor_id), e]));
  // the reading under the table
  const shares = groups.map((g) => g.ms / totMs);
  const meds = groups.map((g) => stats(g.idx.map((i) => t[i] ?? 0))?.p50 ?? 0);
  const all = stats(valuesOf(c, 'task_ms', 'task', null));
  const si = slowest.idx.find((i) => (t[i] ?? 0) === slowest.max) ?? slowest.idx[0];
  const read = (i: number) => (col(c, 'input_bytes')[i] ?? 0) + (col(c, 'shuffle_read')[i] ?? 0);
  const medRead = stats(groups.flatMap((g) => g.idx.map(read)))?.p50 ?? 0;
  const even = Math.max(...shares) <= 1.5 / groups.length;
  return (
    <div className="ts-section">
      <h3 className="ts-h"><span className="ts-num">2</span> Each executor: the same table, only its share of these tasks</h3>
      <p className="muted small" style={{ margin: 0 }}>Click an executor to unfold its table. Only these tasks; {others} on the same executors {others.endsWith('s') ? 'are' : 'is'} in section 3.</p>
      <ExecPicker pick={pick} />
      <ModePills mode={mode} setMode={setMode} extra={
        <>
          <button className="btn small ghost" onClick={() => setOpen(open.size ? new Set() : new Set(groups.map((g) => g.id)))}>{open.size ? 'Fold all' : 'Unfold all'}</button>
          <button className={`btn small ghost ${hideEmpty ? 'on' : ''}`} onClick={() => setHideEmpty((h) => !h)}>{hideEmpty ? 'Show empty rows' : 'Hide empty rows'}</button>
        </>} />
      <div className="table-wrap">
        <table className="table compact ts-table">
          <MetricHead first="Executor · measure" />
          <tbody>
            {groups.filter((g) => pick.shown(g.id)).map((g) => {
              const e = info.get(g.id);
              return (
                <Fragment key={g.id}>
                  <tr className="ts-group clickable" style={{ cursor: 'pointer' }} onClick={() => flip(g.id)} aria-expanded={open.has(g.id)}>
                    <td colSpan={9}>
                      <span aria-hidden className="muted">{open.has(g.id) ? '▾ ' : '▸ '}</span>
                      <b className="mono">exec {g.id}</b>
                      <span className="muted small"> {e?.host ? `${e.host} · ` : ''}{fmtNum(g.idx.length)} tasks · {fmtPct(g.ms / totMs, 0)} of the work · task time {fmtDuration(g.ms)}{e?.cores ? ` · ${e.cores} cores` : ''}</span>
                      {g === slowest && groups.length > 1 ? <span className="chip warn" style={{ marginLeft: 8 }}>holds the slowest task</span> : null}
                    </td>
                  </tr>
                  {open.has(g.id) && <MetricRows c={c} idx={g.idx} mode={mode} scaleOf={scale} hideEmpty={hideEmpty} totalOf={total} />}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="small ts-reading">
        <b>Reading:</b>{' '}
        {even ? `the work was spread evenly (${fmtPct(Math.min(...shares), 0)} – ${fmtPct(Math.max(...shares), 0)} each, medians within ${fmtPct((Math.max(...meds) - Math.min(...meds)) / Math.max(1, Math.min(...meds)), 0)}). `
          : `exec ${groups[0].id} did ${fmtPct(shares[0], 0)} of the work: more tasks or slower ones landed there. `}
        Exec {slowest.id} holds the slowest task: {fmtDuration(slowest.max)}{all?.p50 ? `, ${(slowest.max / all.p50).toFixed(1)}× the median` : ''}
        {medRead ? (read(si) <= 1.5 * medRead ? `, on normal data (${fmtBytes(read(si))}): a slow or busy executor at that time, not a data problem.` : `, on ${fmtBytes(read(si))} of data, ${(read(si) / medRead).toFixed(1)}× the median task: more data, not a slow executor.`) : '.'}
      </p>
    </div>
  );
}

/** Section 3: per executor, this stage's tasks, the other stages' tasks in the same window, and both together, against
 * what the executor could run (cores × the stage's time). */
function ExecCombined({ sh, label, otherLabel, c, pick }: { sh: StageSharing; label: string; otherLabel: string; c: TaskColumns; pick: Pick }) {
  const by = sh.by_exec ?? [];
  if (!by.length) return null;
  const span = Math.max(1, sh.end - sh.start);
  const info = new Map([...(sh.exec_info ?? []), ...c.executors].map((e) => [String(e.executor_id), e]));
  const ids = [...new Set(by.map((x) => String(x.executor_id)))]
    .sort((a, b) => (by.find((x) => String(x.executor_id) === b && x.mine)?.task_ms ?? 0) - (by.find((x) => String(x.executor_id) === a && x.mine)?.task_ms ?? 0));
  const cu = (k: keyof SharingExec, kind: Kind) => unitFor(kind, by.map((x) => x[k] as number | null));
  const u = { task: cu('task_ms', 'ms'), read: cu('input_bytes', 'b'), sr: cu('shuffle_read', 'b'), sw: cu('shuffle_write', 'b'), ms: cu('mem_spill', 'b'),
    ds: cu('disk_spill', 'b'), gc: cu('gc_ms', 'ms'), peak: cu('peak_mem', 'b') };
  const z = (un: Unit, v: number | null | undefined) => (v ? un.f(v) : '0');
  const row = (label: string, x: SharingExec | undefined, cap: number, kind: 'mine' | 'other' | 'all') => {
    const share = x && cap ? (x.task_ms ?? 0) / cap : 0;
    return (
      <tr className={`ec-${kind}`}>
        <td>{label}</td>
        <td className="num">{x ? fmtNum(x.tasks) : '–'}{x?.failed ? <div className="small bad">{fmtNum(x.failed)} failed</div> : null}</td>
        <td className="num">{x ? `${fmtNum(x.stages)} · ${fmtNum(x.runs)}` : '–'}</td>
        <td className="num">{x ? z(u.task, x.task_ms) : '–'}</td>
        <td style={{ minWidth: 130 }}>
          {x && cap ? (
            <>
              <span className="inline-bar"><span style={{ width: `${Math.min(100, share * 100)}%`, background: kind === 'other' ? 'var(--text-3)' : 'var(--series-1)' }} /></span>
              <span className="small"> {fmtPct(share, 0)}</span>
              {kind === 'all' && share >= 0.95 ? <div className="small muted">full the whole window</div> : null}
            </>
          ) : '–'}
        </td>
        <td className="num">{x ? z(u.read, x.input_bytes) : '–'}</td>
        <td className={`num ${x && !x.shuffle_read ? 'zero-good' : ''}`} style={{ color: x?.shuffle_read ? 'var(--shuf)' : undefined }}>{x ? z(u.sr, x.shuffle_read) : '–'}</td>
        <td className={`num ${x && !x.shuffle_write ? 'zero-good' : ''}`} style={{ color: x?.shuffle_write ? 'var(--shuf)' : undefined }}>{x ? z(u.sw, x.shuffle_write) : '–'}</td>
        <td className={`num ${x && !x.mem_spill ? 'zero-good' : ''}`}>{x ? z(u.ms, x.mem_spill) : '–'}</td>
        <td className={`num ${x && !x.disk_spill ? 'zero-good' : ''}`} style={{ color: x?.disk_spill ? 'var(--spill)' : undefined }}>{x ? z(u.ds, x.disk_spill) : '–'}</td>
        <td className="num">{x ? z(u.gc, x.gc_ms) : '–'}</td>
        <td className="num">{x?.peak_mem ? u.peak.f(x.peak_mem) : '–'}</td>
      </tr>
    );
  };
  const fulls: number[] = [], neigh: number[] = [];
  for (const id of ids) {
    const e = info.get(id);
    const cap = (e?.cores ?? 0) * span;
    const all = by.find((x) => String(x.executor_id) === id && x.mine === null);
    const oth = by.find((x) => String(x.executor_id) === id && x.mine === false);
    if (cap && all) fulls.push((all.task_ms ?? 0) / cap);
    if (cap && oth) neigh.push((oth.task_ms ?? 0) / cap);
  }
  const othSpill = by.filter((x) => x.mine === false).map((x) => x.disk_spill ?? 0);
  const ownSpill = by.filter((x) => x.mine === true).reduce((a, x) => a + (x.disk_spill ?? 0), 0);
  return (
    <div className="ts-section">
      <h3 className="ts-h"><span className="ts-num">3</span> Executor by executor: {label}'s tasks, the other tasks on it, and everything combined</h3>
      <p className="muted small" style={{ margin: 0 }}>
        {fmtTime(sh.start)} → {fmtTime(sh.end)}: "other" is {otherLabel}{sh.other_runs ? ` (from ${fmtNum(sh.other_runs)} other runs)` : ''} on the same executor in this window.
        Their time is the part inside the window; their data, shuffle and spill are whole tasks. Capacity is the executor's cores × {fmtDuration(span)}.
      </p>
      <ExecPicker pick={pick} />
      <div className="table-wrap">
        <table className="table compact level-table ts-table">
          <thead>
            <tr className="group-head">
              <th colSpan={3} /><th colSpan={2} className="grp">Time</th><th colSpan={3} className="grp">Data</th><th colSpan={4} className="grp">Spill · GC · memory</th>
            </tr>
            <tr>
              <th>Executor · what</th><th className="num">Tasks</th><th className="num">Stages · runs</th><th className="num">Task time<U u={u.task.u} /></th><th>Of its capacity</th>
              <th className="num">Read<U u={u.read.u} /></th><th className="num">Shuffle read<U u={u.sr.u} /></th><th className="num">Shuffle write<U u={u.sw.u} /></th>
              <th className="num">Spill, memory<U u={u.ms.u} /></th><th className="num">Spill, disk<U u={u.ds.u} /></th><th className="num">GC<U u={u.gc.u} /></th><th className="num">Peak per task<U u={u.peak.u} /></th>
            </tr>
          </thead>
          <tbody>
            {ids.filter(pick.shown).map((id) => {
              const e = info.get(id);
              const cap = (e?.cores ?? 0) * span;
              const get = (m: boolean | null) => by.find((x) => String(x.executor_id) === id && x.mine === m);
              return (
                <Fragment key={id}>
                  <tr className="ts-group"><td colSpan={12}>
                    <b className="mono">exec {id}</b>
                    <span className="muted small"> {e?.host ? `${e.host} · ` : ''}{e?.cores ? `${e.cores} cores · ` : ''}alive {fmtTime(e?.added_time ?? null)} → {e?.removed_time ? fmtTime(e.removed_time) : 'end'}</span>
                  </td></tr>
                  {row(`${label[0].toUpperCase()}${label.slice(1)}'s tasks`, get(true), cap, 'mine')}
                  {row(`${otherLabel[0].toUpperCase()}${otherLabel.slice(1)}`, get(false), cap, 'other')}
                  {row(`Combined on exec ${id}`, get(null), cap, 'all')}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
      {fulls.length > 0 && (
        <p className="small ts-reading">
          <b>Reading:</b> combined task time was {fmtPct(Math.min(...fulls), 0)} – {fmtPct(Math.max(...fulls), 0)} of each executor's capacity
          {Math.min(...fulls) >= 0.95 ? `: they were full the whole time ${label} ran` : ''}.
          {neigh.length ? ` The other tasks took ${fmtPct(Math.min(...neigh), 0)} – ${fmtPct(Math.max(...neigh), 0)} of it` : ''}
          {othSpill.some((x) => x > 0) ? ` and spilled ${fmtBytes(Math.min(...othSpill))} – ${fmtBytes(Math.max(...othSpill))} to disk while ${label} ran, on the same memory` : ''}.
          {ownSpill ? '' : ` ${label[0].toUpperCase()}${label.slice(1)} itself never spilled.`}
        </p>
      )}
    </div>
  );
}

type SlowRow = { task: number | null; stage: number; attempt: number; mine: boolean; sameRun: boolean; exec: string; start: number | null; ms: number; read: number | null; spill: number | null; gc: number | null; typMs: number | null; typRead: number | null };

/** Section 4: the slowest tasks on these executors while the stage ran, any stage, with why each took so long. */
function SlowestTasks({ c, sh, stage, cid, ctx }: { c: TaskColumns; sh: StageSharing | null; stage: { id: number; attempt: number }; cid: string; ctx: string }) {
  const [only, setOnly] = useState(false);
  const [all, setAll] = useState(false);
  const t = col(c, 'task_ms');
  const read = (i: number) => (col(c, 'input_bytes')[i] ?? 0) + (col(c, 'shuffle_read')[i] ?? 0);
  const ownMed = stats(t.filter((x): x is number => x !== null))?.p50 ?? null;
  const ownRead = stats(t.map((_, i) => read(i)))?.p50 ?? null;
  const rows: SlowRow[] = t.map((ms, i) => ({
    task: (col(c, 'task_id')[i] as number | null) ?? null, stage: stage.id, attempt: stage.attempt, mine: true, sameRun: true,
    exec: String(col(c, 'executor_id')[i] ?? '?'), start: col(c, 'launch_time')[i] ?? null, ms: ms ?? 0, read: read(i),
    spill: col(c, 'disk_spill')[i] ?? null, gc: col(c, 'gc_ms')[i] ?? null, typMs: ownMed, typRead: ownRead,
  }));
  const typ = new Map((sh?.typical ?? []).map((x) => [`${x.stage_id}.${x.stage_attempt}`, x]));
  if (!only)
    for (const o of sh?.other_tasks ?? []) {
      const ty = typ.get(`${o.stage_id}.${o.stage_attempt}`);
      rows.push({ task: o.task_id ?? null, stage: o.stage_id, attempt: o.stage_attempt, mine: false, sameRun: !!sh?.run_key && o.run_key === sh.run_key,
        exec: String(o.executor_id ?? '?'), start: o.launch_time, ms: o.task_ms ?? 0, read: (o.input_bytes ?? 0) + (o.shuffle_read ?? 0),
        spill: o.disk_spill ?? null, gc: o.gc_ms ?? null, typMs: ty?.p50_task_ms ?? null, typRead: ty?.p50_read ?? null });
    }
  // ranked by the time each task ran inside the stage's window: a task that started as the stage ended is not one of its slow ones
  const inWin = (r: SlowRow) => (sh && r.start !== null ? Math.max(0, Math.min(sh.end, r.start + r.ms) - Math.max(sh.start, r.start)) : r.ms);
  rows.sort((a, b) => inWin(b) - inWin(a) || b.ms - a.ms);
  const top = rows.slice(0, 20);
  const shown = all ? top : top.slice(0, 5);
  const why = (r: SlowRow) => {
    const x = r.typMs ? r.ms / r.typMs : null;
    if (x === null || x < 2) return { text: 'like the others', tone: 'ok' };
    const dataX = r.typRead && r.read ? r.read / r.typRead : null;
    if (dataX !== null && dataX >= 2) return { text: `${x.toFixed(1)}× the typical time, on ${dataX.toFixed(1)}× the typical data: more data (skew)`, tone: 'bad' };
    return { text: `${x.toFixed(1)}× the typical time on normal data: exec ${r.exec} slow or busy`, tone: 'warn' };
  };
  if (!top.length) return null;
  const su = { ms: unitFor('ms', shown.map((r) => r.ms)), read: unitFor('b', shown.map((r) => r.read)), spill: unitFor('b', shown.map((r) => r.spill)), gc: unitFor('ms', shown.map((r) => r.gc)) };
  return (
    <div className="ts-section">
      <h3 className="ts-h"><span className="ts-num">4</span> The 20 slowest tasks on these executors while the stage ran</h3>
      {sh && <p className="muted small" style={{ margin: 0 }}>{fmtTime(sh.start)} → {fmtTime(sh.end)} · any stage, any run, slowest first. "Why" compares each task with the typical task of its own stage.</p>}
      <div className="row small" style={{ gap: 6, margin: '6px 0' }}>
        <div className="seg" role="group" aria-label="Which tasks">
          <button className={!only ? 'on' : ''} aria-pressed={!only} onClick={() => setOnly(false)}>Everything on these executors</button>
          <button className={only ? 'on' : ''} aria-pressed={only} onClick={() => setOnly(true)}>This stage only</button>
        </div>
      </div>
      <div className="table-wrap">
        <table className="table compact ts-table">
          <thead><tr><th>Task</th><th>Stage · run</th><th>Executor</th><th>Started</th><th className="num">Took<U u={su.ms.u} /></th><th className="num">Read<U u={su.read.u} /></th><th className="num">Spilled to disk<U u={su.spill.u} /></th><th className="num">GC<U u={su.gc.u} /></th><th>Why it took this long</th></tr></thead>
          <tbody>
            {shown.map((r) => {
              const w = why(r);
              return (
                <tr key={`${r.stage}.${r.attempt}.${r.task}.${r.exec}.${r.start}`} className={r.mine ? '' : 'other-task'}>
                  <td className="mono">{r.task ?? '–'}</td>
                  <td><Link className={`tchip ${r.mine ? '' : 'other'}`} to={to.stages(cid, ctx, r.stage, r.attempt)}>Stage {r.stage}{r.attempt ? `.${r.attempt}` : ''} · {r.mine ? 'this stage' : r.sameRun ? 'this run' : 'other run'}</Link></td>
                  <td className="mono">exec {r.exec}</td>
                  <td className="mono small">{fmtTime(r.start)}</td>
                  <td className={`num ${w.tone !== 'ok' ? 'st-warn' : ''}`}><b>{su.ms.f(r.ms)}</b></td>
                  <td className="num">{r.read ? su.read.f(r.read) : '0'}</td>
                  <td className={`num ${r.spill ? '' : 'zero-good'}`} style={{ color: r.spill ? 'var(--spill)' : undefined }}>{r.spill ? su.spill.f(r.spill) : '0'}</td>
                  <td className="num">{r.gc ? su.gc.f(r.gc) : '0'}</td>
                  <td><span className={`why-chip ${w.tone}`}>{w.text}</span></td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {top.length > 5 && <button className="btn small ghost" onClick={() => setAll((a) => !a)}>{all ? 'Show 5' : `${top.length - 5} more ↓`}</button>}
    </div>
  );
}

/** CSV of every task of the scope (what the statistics were made from). */
export function exportCsv(c: TaskColumns, name: string) {
  const keys = Object.keys(c.cols);
  const n = c.cols.task_ms?.length ?? 0;
  const lines = [keys.join(',')];
  for (let i = 0; i < n; i++) lines.push(keys.map((k) => { const v = (c.cols as Record<string, unknown[]>)[k][i]; return v === null || v === undefined ? '' : String(v); }).join(','));
  const url = URL.createObjectURL(new Blob([lines.join('\n')], { type: 'text/csv' }));
  const a = document.createElement('a');
  a.href = url;
  a.download = `${name}.csv`;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/** Each task's wait to start: from its stage's submission to the task's launch (no free core, or queued behind the
 * stage's own earlier tasks). Added as the column `start_wait`. */
function withWait(c: TaskColumns): TaskColumns {
  const sub = new Map(c.stage_waits.map((w) => [`${w.stage_id}.${w.stage_attempt}`, w.submitted]));
  const st = col(c, 'stage_id'), at = col(c, 'stage_attempt'), lt = col(c, 'launch_time');
  const w = lt.map((l, i) => {
    const s0 = sub.get(`${st[i]}.${at[i]}`);
    return l === null || l === undefined || s0 === undefined ? null : Math.max(0, l - s0);
  });
  return { ...c, cols: { ...c.cols, start_wait: w }, present: { ...c.present, start_wait: w.some((x) => x !== null) } };
}

/** The stage page: sections 1 to 4. */
export function StageTaskSections({ cid, ctx, stageId, attempt, sharing, n }: { cid: string; ctx: string; stageId: number; attempt: number; sharing: StageSharing | null; n: number }) {
  const st = useAsync((s) => api.taskColumns(cid, ctx, { stage: stageId, attempt }, s), [cid, ctx, stageId, attempt]);
  if (!st.data) return <p className="muted small">{st.error ? `Could not load the tasks: ${st.error.message}` : 'Loading the tasks…'}</p>;
  return <StageBody cid={cid} ctx={ctx} stageId={stageId} attempt={attempt} sharing={sharing} n={n} c={withWait(st.data)} />;
}

function StageBody({ cid, ctx, stageId, attempt, sharing, n, c }: { cid: string; ctx: string; stageId: number; attempt: number; sharing: StageSharing | null; n: number; c: TaskColumns }) {
  const pick = usePick(c);
  const label = `stage ${stageId}${attempt ? `.${attempt}` : ''}`;
  return (
    <div className="stack ts-sections" style={{ gap: 18 }}>
      <div className="row small" style={{ justifyContent: 'flex-end' }}>
        <button className="btn small" onClick={() => exportCsv(c, `stage-${stageId}-${attempt}-tasks`)}>Export tasks (CSV)</button>
      </div>
      <AllTasks c={c} n={n} what="per task" allRowsLink={to.data(cid, { dataset: 'tasks', step: null, q: `stage_id = ${stageId} AND stage_attempt = ${attempt}` })} />
      <Fold name="tasks-by-executor" title="By executor" what={execWhat(c, 'the other stages')}>
        <PerExecutor c={c} pick={pick} others="the other stages' work" />
        {sharing && <ExecCombined sh={sharing} label={label} otherLabel="other stages' tasks" c={c} pick={pick} />}
      </Fold>
      <Fold name="tasks-slowest" title="The slowest tasks" what={[
        'The 20 slowest tasks on these executors while the stage ran, this stage or any other, with why each took long']}>
        <SlowestTasks c={c} sh={sharing} stage={{ id: stageId, attempt }} cid={cid} ctx={ctx} />
      </Fold>
    </div>
  );
}

/** What the folded executor view holds. */
const execWhat = (c: TaskColumns, others: string) => [
  `The same task numbers for each of the ${fmtNum(c.executors.length)} executors: which one was slow, read the most or spilled`,
  `Each executor's cores over time, with ${others} that ran there`,
];

/** A job or a query: sections 1 to 3 over all its tasks, with each stage's wait for a free core. */
export function ScopeTaskSection({ cid, ctx, job, query, what }: { cid: string; ctx: string; job?: number; query?: number; what: string }) {
  const st = useAsync((s) => api.taskColumns(cid, ctx, job !== undefined ? { job } : { query }, s), [cid, ctx, job, query]);
  if (!st.data) return <p className="muted small">{st.error ? `Could not load the tasks: ${st.error.message}` : 'Loading the tasks…'}</p>;
  if (!st.data.total) return null;
  return <ScopeBody c={withWait(st.data)} what={what} kind={job !== undefined ? 'job' : 'query'} />;
}

function ScopeBody({ c, what, kind }: { c: TaskColumns; what: string; kind: 'job' | 'query' }) {
  const pick = usePick(c);
  const other = kind === 'job' ? 'tasks of other jobs' : 'tasks of other queries';
  return (
    <div className="stack ts-sections" style={{ gap: 10 }}>
      <div className="row small" style={{ justifyContent: 'flex-end' }}>
        <button className="btn small" onClick={() => exportCsv(c, `${what.replace(/\s+/g, '-').toLowerCase()}-tasks`)}>Export tasks (CSV)</button>
      </div>
      <AllTasks c={c} n={c.total} what={`over its ${fmtNum(c.stage_waits.length)} stages`} waits={c.stage_waits.map((w) => w.wait_ms)} />
      <Fold name="tasks-by-executor" title="By executor" what={execWhat(c, `the ${other}`)}>
        <PerExecutor c={c} pick={pick} others={`the ${other}`} />
        {c.sharing && <ExecCombined sh={c.sharing} label={what} otherLabel={other} c={c} pick={pick} />}
      </Fold>
    </div>
  );
}
