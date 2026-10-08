// "Spill & shuffle over time" (CONTRACT Revision 4 item 5): when and where spill and shuffle happened.
import { useMemo, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { api, isMissing, optional, type Hotspot, type SpillShuffleBucket, type SpillShuffleRaw, type TaskRow } from '../api';
import { fmtBytes, fmtNum, fmtTime, fmtTs, tickLabel, timeTicks, truncate } from '../format';
import { METRIC_META, type MeterKey } from '../graph/model';
import { useAsync, useWidth } from '../hooks';
import { to } from '../links';
import { DataLink } from './ui';

/** Lanes: one per executor, per stage attempt, or per executor × stage attempt ('both'). */
export type SsBy = 'executor' | 'stage' | 'both';

export interface SsData {
  buckets: SpillShuffleBucket[];
  bucketMs: number;
  source: 'api' | 'dataset' | 'tasks';
  sampled: boolean;
}

const METRICS: MeterKey[] = ['disk_spill', 'mem_spill', 'shuffle_read', 'shuffle_write'];
const RAMP = ['var(--seq-1)', 'var(--seq-2)', 'var(--seq-3)', 'var(--seq-4)', 'var(--seq-5)'];

const toMinute = (v: unknown): number | null => {
  if (typeof v === 'number') return v;
  if (typeof v === 'string') {
    const t = Date.parse(v);
    return Number.isNaN(t) ? null : t;
  }
  return null;
};

/** Accepts a bare list, `{rows}` or `{series, buckets}` where buckets carry `ts` + `key` and series describe each key. */
function rowsOf(raw: SpillShuffleRaw | null | undefined): SpillShuffleBucket[] {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw;
  const rows = raw.buckets ?? raw.rows ?? [];
  const meta = new Map((raw.series ?? []).map((x) => [x.key, x]));
  return rows.map((r) => {
    const m = r.key !== undefined ? meta.get(r.key) : undefined;
    return {
      ...r,
      minute: toMinute(r.minute ?? r.ts) ?? 0,
      spark_context_id: r.spark_context_id ?? m?.spark_context_id ?? null,
      executor_id: r.executor_id ?? m?.executor_id ?? null,
      stage_id: r.stage_id ?? m?.stage_id ?? null,
      stage_attempt: r.stage_attempt ?? m?.stage_attempt ?? null,
    };
  });
}

function inferBucket(rows: SpillShuffleBucket[], hint?: number): number {
  if (hint && hint > 0) return hint;
  const ts = [...new Set(rows.map((r) => r.minute))].sort((a, b) => a - b);
  let best = Infinity;
  for (let i = 1; i < ts.length; i++) best = Math.min(best, ts[i] - ts[i - 1]);
  return Number.isFinite(best) && best > 0 ? Math.min(best, 3_600_000) : 60_000;
}


/** Load buckets: endpoint, then the raw dataset, then (older analyzers) a client-side rollup of tasks. */
export async function loadSpillShuffle(cid: string, ctx: string | null, by: SsBy, s: AbortSignal): Promise<SsData | null> {
  // the endpoint aggregates to one dimension; executor x stage needs the raw dataset
  if (by !== 'both')
  try {
    const raw = await api.spillShuffle(cid, ctx, by, s);
    const rows = rowsOf(raw);
    const hint = !Array.isArray(raw) ? raw.bucket_ms ?? (raw.bucket_seconds ? raw.bucket_seconds * 1000 : undefined) : undefined;
    return { buckets: rows, bucketMs: inferBucket(rows, hint), source: 'api', sampled: false };
  } catch (e) {
    if (!isMissing(e)) throw e;
  }
  const ds = await api.datasetOpt<SpillShuffleBucket>(cid, 'spill_shuffle_timeline', { spark_context_id: ctx, limit: 5000, sort: 'minute' }, s);
  if (ds) {
    const rows = ds.rows.map((r) => ({ ...r, minute: toMinute(r.minute) ?? 0 }));
    return { buckets: rows, bucketMs: inferBucket(rows), source: 'dataset', sampled: ds.total > ds.rows.length };
  }
  const t = await api.dataset<TaskRow>(
    cid,
    'tasks',
    { spark_context_id: ctx, limit: 5000, sort: 'finish_time', columns: 'finish_time,executor_id,stage_id,stage_attempt,mem_spill,disk_spill,shuffle_read,shuffle_write,spark_context_id' },
    s,
  );
  const m = new Map<string, SpillShuffleBucket>();
  for (const r of t.rows) {
    if (r.finish_time === null) continue;
    const minute = Math.floor(r.finish_time / 60_000) * 60_000;
    const k = `${minute}|${r.executor_id}|${r.stage_id}|${r.stage_attempt}`;
    const b = m.get(k) ?? { minute, executor_id: r.executor_id, stage_id: r.stage_id, stage_attempt: r.stage_attempt, tasks: 0, mem_spill: 0, disk_spill: 0, shuffle_read: 0, shuffle_write: 0 };
    b.tasks = (b.tasks ?? 0) + 1;
    for (const k2 of METRICS) b[k2] = (b[k2] ?? 0) + (r[k2] ?? 0);
    m.set(k, b);
  }
  return { buckets: [...m.values()], bucketMs: 60_000, source: 'tasks', sampled: t.total > t.rows.length };
}

const execSort = (a: string, b: string) => {
  if (a === 'driver') return -1;
  if (b === 'driver') return 1;
  const na = Number(a);
  const nb = Number(b);
  return !Number.isNaN(na) && !Number.isNaN(nb) ? na - nb : a.localeCompare(b);
};

interface Cell {
  t: number;
  tasks: number;
  v: Record<MeterKey, number>;
}
interface LaneRow {
  key: string;
  exec: string;
  label: string;
  total: Record<MeterKey, number>;
  cells: Map<number, Cell>;
  stage?: { id: number; attempt: number };
}

function useLanes(d: SsData, by: SsBy, cols: number) {
  return useMemo(() => {
    const ts = d.buckets.map((b) => b.minute).filter((t) => t > 0);
    const t0 = ts.length ? Math.min(...ts) : 0;
    const t1 = ts.length ? Math.max(...ts) + d.bucketMs : 1;
    const nb = Math.max(1, Math.round((t1 - t0) / d.bucketMs));
    const merge = Math.max(1, Math.ceil(nb / Math.max(1, cols)));
    const step = d.bucketMs * merge;
    const lanes = new Map<string, LaneRow>();
    const totals = new Map<number, Cell>();
    const zero = () => ({ disk_spill: 0, mem_spill: 0, shuffle_read: 0, shuffle_write: 0 });
    for (const b of d.buckets) {
      if (!(b.minute > 0)) continue;
      const ex = b.executor_id ?? 'driver';
      const stKey = b.stage_id === null || b.stage_id === undefined ? '?' : `${b.stage_id}.${b.stage_attempt ?? 0}`;
      const stLabel = `Stage ${b.stage_id ?? '?'}${b.stage_attempt ? `.${b.stage_attempt}` : ''}`;
      const key = by === 'executor' ? ex : by === 'stage' ? stKey : `${ex}|${stKey}`;
      const lane = lanes.get(key) ?? {
        key,
        exec: ex,
        label: by === 'executor' ? (ex === 'driver' ? 'driver' : `exec ${ex}`) : by === 'stage' ? stLabel : `${ex === 'driver' ? 'driver' : `exec ${ex}`} · ${stLabel.replace('Stage ', 'st ')}`,
        total: zero(),
        cells: new Map(),
        stage: by !== 'executor' && b.stage_id !== null && b.stage_id !== undefined ? { id: b.stage_id, attempt: b.stage_attempt ?? 0 } : undefined,
      };
      const t = t0 + Math.floor((b.minute - t0) / step) * step;
      const c = lane.cells.get(t) ?? { t, tasks: 0, v: zero() };
      const tot = totals.get(t) ?? { t, tasks: 0, v: zero() };
      c.tasks += b.tasks ?? 0;
      tot.tasks += b.tasks ?? 0;
      for (const k of METRICS) {
        const v = b[k] ?? 0;
        c.v[k] += v;
        lane.total[k] += v;
        tot.v[k] += v;
      }
      lane.cells.set(t, c);
      totals.set(t, tot);
      lanes.set(key, lane);
    }
    const byStage = (a: LaneRow, b: LaneRow) => (a.stage?.id ?? 0) - (b.stage?.id ?? 0) || (a.stage?.attempt ?? 0) - (b.stage?.attempt ?? 0);
    const arr = [...lanes.values()].sort((a, b) =>
      by === 'executor' ? execSort(a.key, b.key) : by === 'stage' ? byStage(a, b) : execSort(a.exec, b.exec) || byStage(a, b),
    );
    return { lanes: arr, totals, t0, t1: t0 + Math.ceil((t1 - t0) / step) * step, step };
  }, [d, by, cols]);
}

function rampIndex(v: number, max: number) {
  if (v <= 0 || max <= 0) return -1;
  return Math.min(RAMP.length - 1, Math.floor(Math.sqrt(v / max) * RAMP.length));
}

interface Hover {
  x: number;
  y: number;
  lane: string;
  cell: Cell;
}

const LABEL_W = 132;
const LANE_H = 20;
const TOTAL_H = 64;
const AXIS_H = 22;
const MAX_LANES = 40;

export function SpillShuffleChart({ cid, ctx }: { cid: string; ctx: string | null }) {
  const [by, setBy] = useState<SsBy>('executor');
  const [metric, setMetric] = useState<MeterKey | 'all'>('disk_spill');
  const st = useAsync((s) => loadSpillShuffle(cid, ctx, by, s), [cid, ctx, by]);
  const peaks = useAsync((s) => optional(api.hotspots(cid, { ctx }, s)).then((r) => r ?? []), [cid, ctx]);
  const [ref, width] = useWidth<HTMLDivElement>();
  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <h2>Spill and shuffle over time</h2>
          <div className="note">When and where data spilled to disk or memory, or moved between executors. Darker cells moved more bytes. Times in UTC.</div>
        </div>
        <div className="ss-controls">
          <div className="seg" role="group" aria-label="Measure">
            {METRICS.map((k) => (
              <button key={k} className={metric === k ? 'on' : ''} aria-pressed={metric === k} onClick={() => setMetric(k)}>
                {METRIC_META[k].label}
              </button>
            ))}
            <button className={metric === 'all' ? 'on' : ''} aria-pressed={metric === 'all'} onClick={() => setMetric('all')} title="Disk spill, memory spill, shuffle read and shuffle write together">
              All
            </button>
          </div>
          <div className="seg" role="group" aria-label="Lanes">
            <button className={by === 'executor' ? 'on' : ''} aria-pressed={by === 'executor'} onClick={() => setBy('executor')}>
              By executor
            </button>
            <button className={by === 'stage' ? 'on' : ''} aria-pressed={by === 'stage'} onClick={() => setBy('stage')}>
              By stage
            </button>
            <button className={by === 'both' ? 'on' : ''} aria-pressed={by === 'both'} onClick={() => setBy('both')} title="One lane per executor and stage: which stage spilled or shuffled on which executor">
              Both
            </button>
          </div>
        </div>
      </div>
      <div className="panel-body" ref={ref}>
        {st.error ? (
          <p className="muted small">Could not load spill and shuffle data: {st.error.message}</p>
        ) : !st.data ? (
          <p className="muted small">{st.loading ? 'Loading spill and shuffle…' : 'Not available.'}</p>
        ) : width > 0 ? (
          metric === 'all' ? (
            <AllLanes cid={cid} ctx={ctx} d={st.data} by={by} width={width} />
          ) : (
            <Lanes cid={cid} ctx={ctx} d={st.data} by={by} metric={metric} width={width} peaks={peaks.data ?? []} />
          )
        ) : null}
      </div>
    </section>
  );
}

function peakKind(metric: MeterKey) {
  return metric.endsWith('spill') ? 'spill_peak' : 'shuffle_peak';
}

function Lanes({ cid, ctx, d, by, metric, width, peaks }: { cid: string; ctx: string | null; d: SsData; by: SsBy; metric: MeterKey; width: number; peaks: Hotspot[] }) {
  const nav = useNavigate();
  const [sel, setSel] = useState<Hotspot | null>(null);
  const myPeaks = peaks.filter((h) => h.kind === peakKind(metric) && h.ts_start !== null);
  const plotW = Math.max(120, width - LABEL_W - 8);
  const { lanes, totals, t0, t1, step } = useLanes(d, by, Math.floor(plotW / 6));
  const [hover, setHover] = useState<Hover | null>(null);
  const withData = lanes.filter((l) => l.total[metric] > 0);
  const shown = [...withData].sort((a, b) => b.total[metric] - a.total[metric]).slice(0, MAX_LANES);
  const shownSet = new Set(shown.map((l) => l.key));
  const ordered = withData.filter((l) => shownSet.has(l.key));
  const maxCell = Math.max(0, ...ordered.flatMap((l) => [...l.cells.values()].map((c) => c.v[metric])));
  const maxTotal = Math.max(0, ...[...totals.values()].map((c) => c.v[metric]));
  const nCols = Math.max(1, Math.round((t1 - t0) / step));
  const cw = plotW / nCols;
  const x = (t: number) => LABEL_W + ((t - t0) / (t1 - t0)) * plotW;
  const ticks = timeTicks(t0, t1, Math.max(2, Math.floor(plotW / 110)));
  const H = TOTAL_H + AXIS_H + ordered.length * LANE_H + 6;
  const lanesY = TOTAL_H + AXIS_H;

  if (!d.buckets.length || !withData.length)
    return (
      <p className="ink2">
        No {METRIC_META[metric].label.toLowerCase()} in this Spark context.{' '}
        {metric.endsWith('spill') ? 'Every task fit in memory.' : 'No stage exchanged data between executors.'}
      </p>
    );

  const openLane = (l: LaneRow) => {
    if (l.stage) nav(to.stages(cid, ctx, l.stage.id, l.stage.attempt));
    else nav(to.executors(cid, ctx, l.key));
  };
  return (
    <div style={{ position: 'relative' }}>
      <svg className="svg-chart" width={width} height={H} role="img" aria-label={`${METRIC_META[metric].label} per ${by} over time`} onMouseLeave={() => setHover(null)}>
        {/* totals strip */}
        <text x={0} y={12} style={{ fill: 'var(--text-2)', fontSize: 11.5 }}>
          All {by === 'executor' ? 'executors' : 'stages'}
        </text>
        <text x={0} y={27} style={{ fill: 'var(--text-3)' }}>
          peak {fmtBytes(maxTotal)} / {Math.round(step / 1000) >= 60 ? `${Math.round(step / 60_000)} min` : `${Math.round(step / 1000)} s`}
        </text>
        <line className="baseline" x1={LABEL_W} x2={LABEL_W + plotW} y1={TOTAL_H - 2} y2={TOTAL_H - 2} />
        {[...totals.values()].map((c) => {
          const v = c.v[metric];
          if (v <= 0) return null;
          const h = Math.max(2, (v / maxTotal) * (TOTAL_H - 10));
          return (
            <rect
              key={c.t}
              x={x(c.t) + 0.5}
              width={Math.max(1, cw - 2)}
              y={TOTAL_H - 2 - h}
              height={h}
              rx={Math.min(2, cw / 3)}
              style={{ fill: 'var(--seq-3)' }}
              onMouseMove={(e) => setHover({ x: e.clientX, y: e.clientY, lane: `All ${by === 'executor' ? 'executors' : 'stages'}`, cell: c })}
            />
          );
        })}
        {/* peak markers (Revision 5 hotspots): click for the stage x executor breakdown of that minute */}
        {myPeaks.map((h, i) => {
          const px = x(h.ts_start as number) + Math.max(1, cw) / 2;
          // a label only when it has room: within 60 px of an earlier peak the marker alone says it (hover for which)
          const crowded = myPeaks.slice(0, i).some((o) => Math.abs(x(o.ts_start as number) + Math.max(1, cw) / 2 - px) < 60);
          return (
            <g key={`pk${i}`} className="ss-peak" style={{ cursor: 'pointer' }} onClick={() => setSel(sel === h ? null : h)}>
              <path d={`M${px - 6},4 L${px + 6},4 L${px},13 Z`} style={{ fill: sel === h ? 'var(--st-crit)' : 'var(--st-warn)' }} />
              {!crowded && <text x={px + 9} y={12} style={{ fill: 'var(--text-2)', fontSize: 11 }}>
                peak {i + 1}
              </text>}
              <title>{h.detail}</title>
            </g>
          );
        })}
        {/* axis */}
        {ticks.map((t) => (
          <g key={t}>
            <line className="grid" x1={x(t)} x2={x(t)} y1={TOTAL_H} y2={H} />
            <text x={x(t)} y={TOTAL_H + 15} textAnchor="middle">
              {tickLabel(t, t1 - t0)}
            </text>
          </g>
        ))}
        {/* lanes */}
        {ordered.map((l, i) => {
          const y = lanesY + i * LANE_H;
          return (
            <g key={l.key}>
              <text
                x={0}
                y={y + LANE_H / 2 + 4}
                style={{ fill: 'var(--link)', fontFamily: 'var(--f-mono)', fontSize: 11.5, cursor: 'pointer' }}
                onClick={() => openLane(l)}
              >
                {truncate(l.label, 14)}
                <title>{`${l.label}: ${fmtBytes(l.total[metric])} ${METRIC_META[metric].label.toLowerCase()} in total. Click to open.`}</title>
              </text>
              <rect x={LABEL_W} y={y + 1} width={plotW} height={LANE_H - 2} style={{ fill: 'var(--surface-2)' }} />
              {[...l.cells.values()].map((c) => {
                const ri = rampIndex(c.v[metric], maxCell);
                if (ri < 0) return null;
                const on = hover?.lane === l.label && hover.cell.t === c.t;
                return (
                  <rect
                    key={c.t}
                    className={`ss-cell ${on ? 'on' : ''}`}
                    x={x(c.t) + 0.5}
                    y={y + 2}
                    width={Math.max(2, cw - 1)}
                    height={LANE_H - 4}
                    style={{ fill: RAMP[ri], cursor: 'pointer' }}
                    onMouseMove={(e) => setHover({ x: e.clientX, y: e.clientY, lane: l.label, cell: c })}
                    onClick={() => openLane(l)}
                  />
                );
              })}
            </g>
          );
        })}
      </svg>
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center', marginTop: 8 }}>
        <div className="ss-scale">
          <span>less</span>
          <span className="ramp">
            {RAMP.map((c) => (
              <span key={c} style={{ background: c }} />
            ))}
          </span>
          <span>more {METRIC_META[metric].label.toLowerCase()} (largest cell {fmtBytes(maxCell)})</span>
        </div>
        <span className="muted small">
          {withData.length > ordered.length ? `Showing the ${ordered.length} ${by === 'executor' ? 'executors' : 'stages'} with the most bytes of ${withData.length}. ` : ''}
          {lanes.length > withData.length ? `${lanes.length - withData.length} with none are hidden. ` : ''}
          {d.sampled ? 'Based on a sample of tasks. ' : ''}
          Each task's bytes are spread over the minutes it ran. <DataLink cid={cid} dataset="spill_shuffle_timeline" label="Data" />
        </span>
      </div>
      {sel && <PeakBreakdown d={d} h={sel} metric={metric} onClose={() => setSel(null)} />}
      {hover && <SsTip h={hover} step={step} />}
    </div>
  );
}

function PeakBreakdown({ d, h, metric, onClose }: { d: SsData; h: Hotspot; metric: MeterKey; onClose: () => void }) {
  const t0 = h.ts_start as number;
  const t1 = (h.ts_end as number) ?? t0 + d.bucketMs;
  const inPeak = d.buckets.filter((b) => b.minute >= t0 && b.minute < t1);
  const sum = (key: (b: SpillShuffleBucket) => string) => {
    const m = new Map<string, number>();
    for (const b of inPeak) m.set(key(b), (m.get(key(b)) ?? 0) + (b[metric] ?? 0));
    const tot = [...m.values()].reduce((a, v) => a + v, 0) || 1;
    return [...m.entries()].filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1]).slice(0, 6).map(([k, v]) => ({ k, v, share: v / tot }));
  };
  const stages = sum((b) => `Stage ${b.stage_id ?? '?'}${b.stage_attempt ? `.${b.stage_attempt}` : ''}`);
  const execs = sum((b) => (b.executor_id ? `exec ${b.executor_id}` : 'driver'));
  const List = ({ title, rows }: { title: string; rows: { k: string; v: number; share: number }[] }) => (
    <div style={{ minWidth: 0 }}>
      <div className="small muted" style={{ marginBottom: 4 }}>{title}</div>
      {rows.length ? rows.map((r) => (
        <div key={r.k} className="tt-row small">
          <span className="mono">{r.k}</span>
          <span>{fmtBytes(r.v)} · {Math.round(r.share * 100)}%</span>
        </div>
      )) : <div className="small muted">none</div>}
    </div>
  );
  return (
    <div className="peak-box">
      <div className="row" style={{ justifyContent: 'space-between', gap: 8 }}>
        <b className="small">{fmtTs(t0)}–{fmtTime(t1)} UTC</b>
        <button className="btn small ghost" onClick={onClose}>Close</button>
      </div>
      <p className="small ink2 wrap-any" style={{ margin: '4px 0 8px' }}>{h.detail}</p>
      <div className="grid-2" style={{ gap: 16 }}>
        <List title={`${METRIC_META[metric].label} by stage`} rows={stages} />
        <List title={`${METRIC_META[metric].label} by executor`} rows={execs} />
      </div>
    </div>
  );
}

function SsTip({ h, step }: { h: Hover; step: number }) {
  return (
    <div className="tooltip" style={{ left: Math.min(h.x + 14, window.innerWidth - 300), top: h.y + 14 > window.innerHeight - 170 ? h.y - 160 : h.y + 14 }}>
      <div className="tt-title">{h.lane}</div>
      <div className="small ink2" style={{ marginBottom: 4 }}>
        {fmtTs(h.cell.t)} to {fmtTime(h.cell.t + step)} UTC
      </div>
      {METRICS.map((k) => (
        <div className="tt-row" key={k}>
          <span>{METRIC_META[k].label}</span>
          <span>{fmtBytes(h.cell.v[k])}</span>
        </div>
      ))}
      <div className="tt-row">
        <span>Tasks finished</span>
        <span>{fmtNum(h.cell.tasks)}</span>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ compact (Overview) */

function Strip({ label, cells, pick, t0, t1, width, color }: { label: string; cells: Cell[]; pick: (c: Cell) => number; t0: number; t1: number; width: number; color: string }) {
  const H = 46;
  const LW = 150;
  const plotW = Math.max(80, width - LW);
  const max = Math.max(0, ...cells.map(pick));
  const total = cells.reduce((a, c) => a + pick(c), 0);
  const x = (t: number) => LW + ((t - t0) / Math.max(1, t1 - t0)) * plotW;
  const n = Math.max(1, cells.length);
  const bw = Math.max(2, Math.min(18, plotW / Math.max(n, (t1 - t0) / 60_000) - 2));
  return (
    <svg className="svg-chart" width={width} height={H} role="img" aria-label={`${label}: ${fmtBytes(total)} in total, peak ${fmtBytes(max)} per bucket`}>
      <text x={0} y={18} style={{ fill: 'var(--text)', fontSize: 12.5, fontWeight: 600 }}>
        {label}
      </text>
      <text x={0} y={34} style={{ fill: 'var(--text-3)' }}>
        {total > 0 ? `${fmtBytes(total)} total` : 'none'}
      </text>
      <line className="baseline" x1={LW} x2={LW + plotW} y1={H - 4} y2={H - 4} />
      {max > 0 &&
        cells.map((c) => {
          const v = pick(c);
          if (v <= 0) return null;
          const h = Math.max(2, (v / max) * (H - 10));
          return (
            <rect key={c.t} x={x(c.t)} y={H - 4 - h} width={bw} height={h} rx={1.5} style={{ fill: color }}>
              <title>{`${fmtTime(c.t)} UTC: ${fmtBytes(v)}`}</title>
            </rect>
          );
        })}
    </svg>
  );
}

export function SpillShuffleMini({ cid, ctx }: { cid: string; ctx: string | null }) {
  const st = useAsync((s) => loadSpillShuffle(cid, ctx, 'executor', s).catch(() => null), [cid, ctx]);
  const [ref, width] = useWidth<HTMLDivElement>();
  const agg = useMemo(() => {
    const d = st.data;
    if (!d || !d.buckets.length) return null;
    const m = new Map<number, Cell>();
    for (const b of d.buckets) {
      const c = m.get(b.minute) ?? { t: b.minute, tasks: 0, v: { disk_spill: 0, mem_spill: 0, shuffle_read: 0, shuffle_write: 0 } };
      for (const k of METRICS) c.v[k] += b[k] ?? 0;
      m.set(b.minute, c);
    }
    const cells = [...m.values()].sort((a, b) => a.t - b.t);
    return { cells, t0: cells[0].t, t1: cells[cells.length - 1].t + d.bucketMs };
  }, [st.data]);
  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <h2>Spill and shuffle over time</h2>
          <div className="note">Bytes per minute across all executors, UTC</div>
        </div>
        <Link className="btn small" to={to.timeline(cid, ctx)}>
          See where on the timeline
        </Link>
      </div>
      <div className="panel-body" ref={ref}>
        {!agg ? (
          <p className="muted small">{st.loading ? 'Loading…' : 'No spill or shuffle data for this run.'}</p>
        ) : (
          width > 0 && (
            <div className="ss-mini">
              <Strip label="Spill, disk + memory" cells={agg.cells} pick={(c) => c.v.disk_spill + c.v.mem_spill} t0={agg.t0} t1={agg.t1} width={width} color="var(--spill)" />
              <Strip label="Shuffle, read + write" cells={agg.cells} pick={(c) => c.v.shuffle_read + c.v.shuffle_write} t0={agg.t0} t1={agg.t1} width={width} color="var(--seq-3)" />
              <div className="ss-mini-axis small muted">
                <span>{fmtTime(agg.t0)}</span>
                <span>{fmtTime(agg.t1)} UTC</span>
              </div>
            </div>
          )
        )}
      </div>
    </section>
  );
}

/* ------------------------------------------------------------------ all four measures at once */

/** One hue per measure in the "All" view, the same as everywhere else (red and amber are kept for status). */
const ALL_COLOR: Record<MeterKey, string> = {
  disk_spill: 'var(--spill)',
  mem_spill: 'var(--c5)',
  shuffle_read: 'var(--series-1)',
  shuffle_write: 'var(--series-3)',
};
const STRIP_H = 30;
const SUB_H = 6;
const ALL_LANE_H = METRICS.length * SUB_H + 8;

/** Disk spill, memory spill, shuffle read and shuffle write on one clock: a totals strip per measure, then each lane
 *  split into four thin rows (same order and colors), each shaded against its own measure's largest cell. */
function AllLanes({ cid, ctx, d, by, width }: { cid: string; ctx: string | null; d: SsData; by: SsBy; width: number }) {
  const nav = useNavigate();
  const plotW = Math.max(120, width - LABEL_W - 8);
  const { lanes, totals, t0, t1, step } = useLanes(d, by, Math.floor(plotW / 6));
  const [hover, setHover] = useState<Hover | null>(null);
  const has = METRICS.filter((k) => lanes.some((l) => l.total[k] > 0));
  const any = (l: LaneRow) => METRICS.some((k) => l.total[k] > 0);
  const withData = lanes.filter(any);
  if (!d.buckets.length || !withData.length)
    return <p className="ink2">No spill and no shuffle in this Spark context: every task fit in memory and no stage exchanged data between executors.</p>;
  // lanes with the most bytes of any measure, each measure scaled to its own largest lane
  const laneMax = Object.fromEntries(METRICS.map((k) => [k, Math.max(1, ...withData.map((l) => l.total[k]))])) as Record<MeterKey, number>;
  const weight = (l: LaneRow) => Math.max(...METRICS.map((k) => l.total[k] / laneMax[k]));
  const keep = new Set([...withData].sort((a, b) => weight(b) - weight(a)).slice(0, MAX_LANES).map((l) => l.key));
  const ordered = withData.filter((l) => keep.has(l.key));
  const maxCell = Object.fromEntries(METRICS.map((k) => [k, Math.max(0, ...ordered.flatMap((l) => [...l.cells.values()].map((c) => c.v[k])))])) as Record<MeterKey, number>;
  const maxTotal = Object.fromEntries(METRICS.map((k) => [k, Math.max(0, ...[...totals.values()].map((c) => c.v[k]))])) as Record<MeterKey, number>;
  const nCols = Math.max(1, Math.round((t1 - t0) / step));
  const cw = plotW / nCols;
  const x = (t: number) => LABEL_W + ((t - t0) / (t1 - t0)) * plotW;
  const ticks = timeTicks(t0, t1, Math.max(2, Math.floor(plotW / 110)));
  const stripsH = has.length * STRIP_H;
  const lanesY = stripsH + AXIS_H;
  const H = lanesY + ordered.length * ALL_LANE_H + 6;
  const per = Math.round(step / 1000) >= 60 ? `${Math.round(step / 60_000)} min` : `${Math.round(step / 1000)} s`;
  const allLabel = `All ${by === 'executor' ? 'executors' : 'stages'}`;
  const openLane = (l: LaneRow) => {
    if (l.stage) nav(to.stages(cid, ctx, l.stage.id, l.stage.attempt));
    else nav(to.executors(cid, ctx, l.key));
  };
  const shade = (v: number, max: number) => (v <= 0 || max <= 0 ? 0 : 0.25 + 0.75 * Math.sqrt(v / max));
  return (
    <div style={{ position: 'relative' }}>
      <svg className="svg-chart" width={width} height={H} role="img" aria-label={`Spill and shuffle per ${by} over time, all measures`} onMouseLeave={() => setHover(null)}>
        {has.map((k, i) => {
          const y0 = i * STRIP_H;
          return (
            <g key={k}>
              <rect x={0} y={y0 + 6} width={9} height={9} rx={2} style={{ fill: ALL_COLOR[k] }} />
              <text x={14} y={y0 + 14} style={{ fill: 'var(--text-2)', fontSize: 11.5 }}>
                {METRIC_META[k].label}
              </text>
              <text x={14} y={y0 + 26} style={{ fill: 'var(--text-3)', fontSize: 10.5 }}>
                peak {fmtBytes(maxTotal[k])} / {per}
              </text>
              <line className="baseline" x1={LABEL_W} x2={LABEL_W + plotW} y1={y0 + STRIP_H - 2} y2={y0 + STRIP_H - 2} />
              {[...totals.values()].map((c) => {
                const v = c.v[k];
                if (v <= 0 || maxTotal[k] <= 0) return null;
                const h = Math.max(2, (v / maxTotal[k]) * (STRIP_H - 6));
                return (
                  <rect
                    key={c.t}
                    x={x(c.t) + 0.5}
                    width={Math.max(1, cw - 2)}
                    y={y0 + STRIP_H - 2 - h}
                    height={h}
                    rx={Math.min(2, cw / 3)}
                    style={{ fill: ALL_COLOR[k] }}
                    onMouseMove={(e) => setHover({ x: e.clientX, y: e.clientY, lane: allLabel, cell: c })}
                  />
                );
              })}
            </g>
          );
        })}
        {ticks.map((t) => (
          <g key={t}>
            <line className="grid" x1={x(t)} x2={x(t)} y1={stripsH} y2={H} />
            <text x={x(t)} y={stripsH + 15} textAnchor="middle">
              {tickLabel(t, t1 - t0)}
            </text>
          </g>
        ))}
        {ordered.map((l, i) => {
          const y = lanesY + i * ALL_LANE_H;
          return (
            <g key={l.key}>
              <text
                x={0}
                y={y + ALL_LANE_H / 2 + 4}
                style={{ fill: 'var(--link)', fontFamily: 'var(--f-mono)', fontSize: 11.5, cursor: 'pointer' }}
                onClick={() => openLane(l)}
              >
                {truncate(l.label, 14)}
                <title>{`${l.label}: ${METRICS.map((k) => `${METRIC_META[k].label.toLowerCase()} ${fmtBytes(l.total[k])}`).join(', ')}. Click to open.`}</title>
              </text>
              <rect x={LABEL_W} y={y + 1} width={plotW} height={ALL_LANE_H - 2} style={{ fill: 'var(--surface-2)' }} />
              {[...l.cells.values()].map((c) => {
                const on = hover?.lane === l.label && hover.cell.t === c.t;
                return (
                  <g key={c.t} onMouseMove={(e) => setHover({ x: e.clientX, y: e.clientY, lane: l.label, cell: c })} onClick={() => openLane(l)} style={{ cursor: 'pointer' }}>
                    {METRICS.map((k, j) => {
                      const o = shade(c.v[k], maxCell[k]);
                      if (!o) return null;
                      return (
                        <rect
                          key={k}
                          className={`ss-cell ${on ? 'on' : ''}`}
                          x={x(c.t) + 0.5}
                          y={y + 4 + j * SUB_H}
                          width={Math.max(2, cw - 1)}
                          height={SUB_H - 1}
                          style={{ fill: ALL_COLOR[k], fillOpacity: o }}
                        />
                      );
                    })}
                  </g>
                );
              })}
            </g>
          );
        })}
      </svg>
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center', marginTop: 8, flexWrap: 'wrap', gap: 8 }}>
        <div className="ss-scale">
          {METRICS.map((k) => (
            <span key={k} className="row" style={{ gap: 4, alignItems: 'center' }}>
              <span style={{ width: 10, height: 10, borderRadius: 2, background: ALL_COLOR[k], display: 'inline-block' }} />
              <span>{METRIC_META[k].label}</span>
            </span>
          ))}
          <span className="muted">· each lane has one thin row per measure, in this order; stronger color moved more bytes</span>
        </div>
        <span className="muted small">
          {withData.length > ordered.length ? `Showing the ${ordered.length} ${by === 'executor' ? 'executors' : 'stages'} with the most bytes of ${withData.length}. ` : ''}
          {d.sampled ? 'Based on a sample of tasks. ' : ''}
          Each task's bytes are spread over the minutes it ran. <DataLink cid={cid} dataset="spill_shuffle_timeline" label="Data" />
        </span>
      </div>
      {hover && <SsTip h={hover} step={step} />}
    </div>
  );
}
