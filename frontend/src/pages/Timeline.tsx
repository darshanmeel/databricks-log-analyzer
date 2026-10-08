import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { api, type AppRow, type Gantt, type GanttExecutor, type GanttJob, type GanttMarker, type GanttStage, type GanttTask, type IncidentRow } from '../api';
import { useCluster, useRunScopeCtx, useThemeVersion } from '../components/Shell';
import { Async, Empty, ErrorState, Loading, DataLink } from '../components/ui';
import { SpillShuffleChart } from '../components/SpillShuffle';
import { fmtDuration, fmtNum, fmtTime, fmtTs, tickLabel, timeTicks, truncate } from '../format';
import { useAsync, useQueryState, useWidth } from '../hooks';
import { to } from '../links';

/* ------------------------------------------------------------------ layout model */

const LABEL_W = 156;
const AXIS_H = 28;
const HEADER_H = 22;
const JOB_H = 14;
const STAGE_H = 9;
const MAX_STAGE_ROWS = 14;
const MAX_SLOTS = 16;

type ItemRef = { t: 'job'; v: GanttJob } | { t: 'stage'; v: GanttStage } | { t: 'task'; v: GanttTask };
interface Item {
  s: number;
  e: number;
  failed: boolean;
  alt: boolean;
  ref: ItemRef;
}
interface Lane {
  kind: 'header' | 'jobs' | 'stages' | 'exec';
  label: string;
  sub?: string;
  y: number;
  h: number;
  barH: number;
  gap: number;
  slots: Item[][];
  exec?: GanttExecutor;
  markers: GanttMarker[];
  /** executor lanes: up but running no task (paid for, doing nothing) */
  idle?: { ms: number; life: number; gaps: [number, number][] };
}

const IDLE_GAP_MS = 60_000;

/** Idle time of one executor: its lifetime minus the union of its task intervals; stretches of a minute or more. */
function idleOf(items: { s: number; e: number }[], s: number, e: number): { ms: number; life: number; gaps: [number, number][] } {
  const life = Math.max(0, e - s);
  const iv = items.map((it) => [Math.max(s, it.s), Math.min(e, it.e)] as [number, number]).filter(([a, b]) => b > a).sort((a, b) => a[0] - b[0]);
  let busy = 0;
  let cur = s;
  const gaps: [number, number][] = [];
  let runEnd = -Infinity;
  let runStart = -Infinity;
  for (const [a, b] of iv) {
    if (a > runEnd) {
      if (runEnd > -Infinity) busy += runEnd - runStart;
      if (a - cur >= IDLE_GAP_MS) gaps.push([cur, a]);
      runStart = a;
      runEnd = b;
    } else runEnd = Math.max(runEnd, b);
    cur = Math.max(cur, runEnd);
  }
  if (runEnd > -Infinity) busy += runEnd - runStart;
  if (e - cur >= IDLE_GAP_MS) gaps.push([cur, e]);
  return { ms: Math.max(0, life - busy), life, gaps };
}
interface Model {
  lanes: Lane[];
  height: number;
  t0: number;
  t1: number;
  globalMarkers: GanttMarker[];
}

function pack(items: Item[], maxRows: number): Item[][] {
  const sorted = [...items].sort((a, b) => a.s - b.s || b.e - a.e);
  const rows: Item[][] = [];
  const ends: number[] = [];
  for (const it of sorted) {
    let r = ends.findIndex((e) => e <= it.s);
    if (r < 0) {
      if (rows.length < maxRows) {
        r = rows.length;
        rows.push([]);
        ends.push(0);
      } else {
        // overflow: put it on the row that frees up first
        r = ends.indexOf(Math.min(...ends));
      }
    }
    rows[r].push(it);
    ends[r] = Math.max(ends[r], it.e);
  }
  rows.forEach((row) => row.sort((a, b) => a.s - b.s));
  return rows;
}

function execSort(a: string, b: string): number {
  if (a === 'driver') return -1;
  if (b === 'driver') return 1;
  const na = Number(a);
  const nb = Number(b);
  if (!Number.isNaN(na) && !Number.isNaN(nb)) return na - nb;
  return a.localeCompare(b);
}

function buildModel(g: Gantt): Model {
  let t0 = g.start ?? Infinity;
  let t1 = g.end ?? -Infinity;
  const consider = (s: number | null, e: number | null) => {
    if (s !== null) t0 = Math.min(t0, s);
    if (e !== null) t1 = Math.max(t1, e);
    if (s !== null) t1 = Math.max(t1, s);
  };
  g.jobs.forEach((j) => consider(j.start, j.end));
  g.stages.forEach((s) => consider(s.start, s.end));
  g.tasks.forEach((t) => consider(t.start, t.end));
  if (!Number.isFinite(t0) || !Number.isFinite(t1)) {
    t0 = 0;
    t1 = 1;
  }
  if (t1 <= t0) t1 = t0 + 1000;
  const close = (e: number | null) => e ?? t1;

  const lanes: Lane[] = [];
  let y = 0;
  const push = (l: Omit<Lane, 'y'>) => {
    lanes.push({ ...l, y });
    y += l.h;
  };

  // jobs band
  const jobItems: Item[] = g.jobs
    .filter((j) => j.start !== null)
    .map((j) => ({ s: j.start!, e: close(j.end), failed: (j.result ?? '').toLowerCase().includes('fail'), alt: false, ref: { t: 'job', v: j } }));
  push({ kind: 'header', label: `Spark jobs (${fmtNum(g.jobs.length)})`, h: HEADER_H, barH: 0, gap: 0, slots: [], markers: [] });
  const jobRows = pack(jobItems, 6);
  push({ kind: 'jobs', label: 'Jobs', h: Math.max(1, jobRows.length) * (JOB_H + 2) + 6, barH: JOB_H, gap: 2, slots: jobRows, markers: [] });

  // stages band
  const stageItems: Item[] = g.stages
    .filter((s) => s.start !== null)
    .map((s) => ({ s: s.start!, e: close(s.end), failed: s.status === 'failed', alt: s.stage_id % 2 === 1, ref: { t: 'stage', v: s } }));
  push({ kind: 'header', label: `Stages (${fmtNum(g.stages.length)})`, h: HEADER_H, barH: 0, gap: 0, slots: [], markers: [] });
  const stageRows = pack(stageItems, MAX_STAGE_ROWS);
  push({ kind: 'stages', label: 'Stages', h: Math.max(1, stageRows.length) * (STAGE_H + 2) + 6, barH: STAGE_H, gap: 2, slots: stageRows, markers: [] });

  // executor lanes
  const byExec = new Map<string, Item[]>();
  for (const t of g.tasks) {
    if (t.start === null) continue;
    const k = t.executor_id ?? '?';
    const arr = byExec.get(k) ?? [];
    arr.push({ s: t.start, e: Math.max(close(t.end), t.start), failed: !!t.failed, alt: t.stage_id % 2 === 1, ref: { t: 'task', v: t } });
    byExec.set(k, arr);
  }
  const execMap = new Map(g.executors.map((e) => [e.executor_id, e]));
  const ids = [...new Set([...g.executors.map((e) => e.executor_id), ...byExec.keys()])].sort(execSort);
  const markersByExec = new Map<string, GanttMarker[]>();
  const globalMarkers: GanttMarker[] = [];
  for (const m of g.markers) {
    if (m.executor_id === null || m.executor_id === undefined || !ids.includes(m.executor_id)) globalMarkers.push(m);
    else markersByExec.set(m.executor_id, [...(markersByExec.get(m.executor_id) ?? []), m]);
  }
  push({ kind: 'header', label: `Executors (${fmtNum(ids.length)})`, h: HEADER_H, barH: 0, gap: 0, slots: [], markers: [] });
  for (const id of ids) {
    const slots = pack(byExec.get(id) ?? [], MAX_SLOTS);
    const n = Math.max(1, slots.length);
    const barH = n <= 2 ? 9 : n <= 4 ? 6 : n <= 8 ? 4 : 3;
    const gap = 1;
    const ex = execMap.get(id);
    const busyIv = g.busy?.filter((b) => b.executor_id === id).map((b) => ({ s: b.busy_start, e: b.busy_end }));
    const idle = id === 'driver' || (g.sampled && !g.busy) ? undefined : idleOf(busyIv ?? byExec.get(id) ?? [], ex?.added ?? t0, ex?.removed ?? t1);
    push({
      kind: 'exec',
      label: id === 'driver' ? 'driver' : `exec ${id}`,
      sub: ex?.host ?? undefined,
      idle,
      h: Math.max(idle ? 40 : 26, n * (barH + gap) + 10),
      barH,
      gap,
      slots,
      exec: ex,
      markers: markersByExec.get(id) ?? [],
    });
  }
  return { lanes, height: y + 8, t0, t1, globalMarkers };
}

/* ------------------------------------------------------------------ colors */

function cssColors() {
  const cs = getComputedStyle(document.documentElement);
  const v = (n: string) => cs.getPropertyValue(n).trim();
  return {
    task: v('--series-1'),
    taskAlt: v('--series-1-soft'),
    failed: v('--sev-high'),
    medium: v('--sev-medium'),
    good: v('--good'),
    info: v('--sev-info'),
    ink: v('--ink'),
    ink2: v('--ink-2'),
    muted: v('--muted'),
    grid: v('--grid'),
    line: v('--line-soft'),
    panel: v('--panel'),
    panel2: v('--panel-2'),
    life: v('--panel-3'),
    onBar: v('--on-series'),
    spill: v('--c2'),
    font: v('--font') || 'sans-serif',
    mono: v('--mono') || 'monospace',
  };
}

function markerColor(kind: string, c: ReturnType<typeof cssColors>) {
  switch (kind) {
    case 'oom':
    case 'executor_lost':
    case 'error':
      return c.failed;
    case 'executor_added':
      return c.good;
    case 'spill':
      return c.spill;
    case 'signal':
    case 'executor_killed':
    case 'full_gc':
      return c.medium;
    default:
      return c.info;
  }
}

const MARKER_LABEL: Record<string, string> = {
  oom: 'Out of memory',
  executor_lost: 'Executor lost',
  executor_added: 'Executor added',
  executor_removed: 'Executor removed',
  executor_killed: 'Executor killed by the OS',
  full_gc: 'Full GC pause',
  spill: 'Tasks spilled to disk',
  signal: 'Log signal',
  error: 'Error',
};

/* ------------------------------------------------------------------ canvas */

interface Hover {
  x: number;
  y: number;
  item?: Item;
  marker?: GanttMarker;
  lane?: Lane;
}

type Zoom = { t0: number; t1: number; n: number };

function GanttCanvas({ g, width, onOpenStage, zoomTo }: { g: Gantt; width: number; onOpenStage: (s: GanttStage | GanttTask) => void; zoomTo?: Zoom | null }) {
  const model = useMemo(() => buildModel(g), [g]);
  const [view, setView] = useState<[number, number]>([model.t0, model.t1]);
  useEffect(() => setView([model.t0, model.t1]), [model]);
  // zoom to a window picked outside the chart (an incident), with some context either side
  useEffect(() => {
    if (!zoomTo) return;
    const pad = Math.max(5000, (zoomTo.t1 - zoomTo.t0) * 0.4);
    const a = Math.max(model.t0, zoomTo.t0 - pad);
    const b = Math.min(model.t1, zoomTo.t1 + pad);
    if (b > a) setView([a, b]);
  }, [zoomTo, model]);
  const theme = useThemeVersion();
  const mainRef = useRef<HTMLCanvasElement>(null);
  const axisRef = useRef<HTMLCanvasElement>(null);
  const [hover, setHover] = useState<Hover | null>(null);
  const [drag, setDragState] = useState<{ x0: number; x1: number } | null>(null);
  const dragRef = useRef<{ x0: number; x1: number } | null>(null);
  const setDrag = (d: { x0: number; x1: number } | null) => {
    dragRef.current = d;
    setDragState(d);
  };
  const plotW = Math.max(100, width - LABEL_W - 8);

  const xOf = useCallback((t: number) => LABEL_W + ((t - view[0]) / (view[1] - view[0])) * plotW, [view, plotW]);
  const tOf = useCallback((x: number) => view[0] + ((x - LABEL_W) / plotW) * (view[1] - view[0]), [view, plotW]);

  // draw
  useLayoutEffect(() => {
    const cv = mainRef.current;
    const ax = axisRef.current;
    if (!cv || !ax || width <= 0) return;
    const dpr = window.devicePixelRatio || 1;
    const c = cssColors();
    const ticks = timeTicks(view[0], view[1], Math.max(3, Math.floor(plotW / 110)));
    const span = view[1] - view[0];

    // axis
    ax.width = width * dpr;
    ax.height = AXIS_H * dpr;
    ax.style.width = `${width}px`;
    ax.style.height = `${AXIS_H}px`;
    const a = ax.getContext('2d')!;
    a.setTransform(dpr, 0, 0, dpr, 0, 0);
    a.fillStyle = c.panel;
    a.fillRect(0, 0, width, AXIS_H);
    a.font = `11px ${c.font}`;
    a.fillStyle = c.muted;
    a.textBaseline = 'middle';
    a.fillText('UTC', 10, AXIS_H / 2);
    for (const t of ticks) {
      const x = xOf(t);
      a.fillStyle = c.grid;
      a.fillRect(Math.round(x), AXIS_H - 6, 1, 6);
      a.fillStyle = c.muted;
      a.textAlign = 'center';
      a.fillText(tickLabel(t, span), x, AXIS_H / 2 - 1);
    }
    a.textAlign = 'left';
    a.fillStyle = c.grid;
    a.fillRect(0, AXIS_H - 1, width, 1);

    // main
    const H = model.height;
    cv.width = width * dpr;
    cv.height = H * dpr;
    cv.style.width = `${width}px`;
    cv.style.height = `${H}px`;
    const ctx = cv.getContext('2d')!;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.fillStyle = c.panel;
    ctx.fillRect(0, 0, width, H);

    // gridlines
    ctx.fillStyle = c.grid;
    for (const t of ticks) ctx.fillRect(Math.round(xOf(t)), 0, 1, H);
    // the picked incident's window
    if (zoomTo) {
      const xa = Math.max(LABEL_W, xOf(zoomTo.t0));
      const xb = Math.min(LABEL_W + plotW, Math.max(xOf(zoomTo.t1), xa + 2));
      if (xb > xa) {
        ctx.globalAlpha = 0.1;
        ctx.fillStyle = c.failed;
        ctx.fillRect(xa, 0, xb - xa, H);
        ctx.globalAlpha = 1;
      }
    }

    const x0 = LABEL_W;
    const x1 = LABEL_W + plotW;
    const clipX = (x: number) => Math.max(x0, Math.min(x1, x));

    for (const lane of model.lanes) {
      if (lane.kind === 'header') {
        ctx.fillStyle = c.panel2;
        ctx.fillRect(0, lane.y, width, lane.h);
        ctx.fillStyle = c.ink2;
        ctx.font = `600 12px ${c.font}`;
        ctx.textBaseline = 'middle';
        ctx.fillText(lane.label, 10, lane.y + lane.h / 2);
        continue;
      }
      // separator
      ctx.fillStyle = c.line;
      ctx.fillRect(0, lane.y + lane.h - 1, width, 1);

      // executor lifetime shading
      if (lane.kind === 'exec' && lane.exec) {
        const s = lane.exec.added ?? view[0];
        const e = lane.exec.removed ?? model.t1;
        const xa = clipX(xOf(s));
        const xb = clipX(xOf(e));
        const top = lane.y + 2;
        const hh = lane.h - 5;
        // three states: not running (no fill), up and running tasks (bars on the up shade), up but idle (amber stripes)
        if (xb > xa) {
          ctx.fillStyle = c.life;
          ctx.fillRect(xa, top, xb - xa, hh);
        }
        const notRunning = (na: number, nb: number, why: string, right = false) => {
          if (nb - na < 2) return;
          ctx.strokeStyle = c.line;
          ctx.setLineDash([3, 3]);
          ctx.strokeRect(na + 0.5, top + 0.5, nb - na - 1, hh - 1);
          ctx.setLineDash([]);
          ctx.font = `italic 10.5px ${c.font}`;
          const w = ctx.measureText(why).width;
          if (w + 16 < nb - na) {
            ctx.fillStyle = c.muted;
            ctx.textBaseline = 'middle';
            ctx.fillText(why, right ? nb - w - 8 : na + 8, right ? lane.y + lane.h - 10 : lane.y + lane.h / 2);
          }
        };
        if (lane.exec.added !== null && lane.exec.added > view[0]) notRunning(clipX(xOf(view[0])), xa, 'not running yet');
        if (lane.exec.removed !== null && lane.exec.removed < view[1]) notRunning(xb, clipX(xOf(view[1])), 'not running (removed)', true);
        // up but running nothing: still paid for
        for (const [ga, gb] of lane.idle?.gaps ?? []) {
          const ia = clipX(xOf(ga));
          const ib = clipX(xOf(gb));
          if (ib - ia < 2) continue;
          ctx.save();
          ctx.beginPath();
          ctx.rect(ia, top, ib - ia, hh);
          ctx.clip();
          ctx.globalAlpha = 0.14;
          ctx.fillStyle = c.medium;
          ctx.fillRect(ia, top, ib - ia, hh);
          ctx.globalAlpha = 0.45;
          ctx.strokeStyle = c.medium;
          ctx.lineWidth = 1;
          ctx.beginPath();
          for (let sx = ia - hh; sx < ib; sx += 7) {
            ctx.moveTo(sx, top + hh);
            ctx.lineTo(sx + hh, top);
          }
          ctx.stroke();
          ctx.restore();
          const word = `up but idle ${fmtDuration(gb - ga)}`;
          ctx.font = `600 10.5px ${c.font}`;
          // a stretch that ends at the removal leaves its label to the removal ("lost 18:05:33")
          const endsAtRemoval = lane.exec?.removed != null && Math.abs(gb - lane.exec.removed) < 1000;
          const tw = ctx.measureText(word).width;
          if (!endsAtRemoval && tw + 16 < ib - ia) {
            const tx = (ia + ib) / 2 - tw / 2;
            ctx.fillStyle = c.panel;
            ctx.fillRect(tx - 5, lane.y + lane.h / 2 - 8, tw + 10, 16);
            ctx.fillStyle = c.medium;
            ctx.textBaseline = 'middle';
            ctx.fillText(word, tx, lane.y + lane.h / 2);
          }
        }
      }

      // labels
      ctx.textBaseline = 'middle';
      if (lane.kind === 'exec') {
        ctx.font = `12px ${c.mono}`;
        ctx.fillStyle = ['oom', 'lost', 'killed'].includes(lane.exec?.removal_category ?? '') ? c.failed : c.ink;
        const mid = lane.y + lane.h / 2;
        const idle = lane.idle && lane.idle.life > 0 ? lane.idle : null;
        const three = !!idle && lane.h >= 38;
        ctx.fillText(truncate(lane.label, 18), 10, three ? mid - 12 : lane.sub && lane.h >= 30 ? mid - 6 : mid);
        if (lane.sub && lane.h >= 30) {
          ctx.font = `10.5px ${c.font}`;
          ctx.fillStyle = c.muted;
          ctx.fillText(truncate(lane.sub, 22), 10, three ? mid + 1 : mid + 7);
        }
        if (three) {
          const share = idle.ms / idle.life;
          ctx.font = `10.5px ${c.font}`;
          ctx.fillStyle = share >= 0.5 ? c.medium : c.muted;
          ctx.fillText(`idle ${fmtDuration(idle.ms)} (${Math.round(share * 100)}%)`, 10, mid + 13);
        }
      }

      // bars
      const top = lane.y + 4;
      lane.slots.forEach((row, ri) => {
        const y = top + ri * (lane.barH + lane.gap);
        for (const it of row) {
          if (it.e < view[0] || it.s > view[1]) continue;
          const xa = clipX(xOf(it.s));
          const xb = clipX(xOf(it.e));
          const w = Math.max(1, xb - xa);
          ctx.fillStyle = it.failed ? c.failed : it.alt ? c.taskAlt : c.task;
          if (lane.kind === 'jobs' || lane.kind === 'stages') {
            ctx.fillRect(xa, y, w, lane.barH);
            if (lane.kind === 'jobs' && w > 40) {
              ctx.fillStyle = c.onBar;
              ctx.font = `600 10.5px ${c.font}`;
              const j = (it.ref as { t: 'job'; v: GanttJob }).v;
              ctx.save();
              ctx.beginPath();
              ctx.rect(xa, y, w, lane.barH);
              ctx.clip();
              ctx.fillText(`Job ${j.spark_job_id}${j.description ? `  ${j.description}` : ''}`, xa + 4, y + lane.barH / 2 + 0.5);
              ctx.restore();
            }
          } else {
            ctx.fillRect(xa, y, w > 2 ? w - 0.5 : w, lane.barH);
          }
        }
      });

      // per-lane markers, and a word for how an executor left (the bar alone does not say)
      for (const m of lane.markers) drawMarker(ctx, m, xOf(m.ts), lane.y + 1, lane.h - 3, c, x0, x1);
      if (lane.kind === 'exec') {
        const ends = lane.markers.filter((m) => END_WORD[m.kind]);
        const last = ends.length ? ends.reduce((a, b) => (b.ts > a.ts ? b : a)) : null;
        if (last) {
          const x = xOf(last.ts);
          if (x >= x0 && x <= x1) {
            const word = `${END_WORD[last.kind]} ${fmtTime(last.ts)}`;
            ctx.font = `600 11px ${c.font}`;
            const tw = ctx.measureText(word).width;
            const tx = x + 6 + tw < x1 ? x + 6 : x - 6 - tw;
            ctx.fillStyle = c.panel;
            ctx.fillRect(tx - 3, lane.y + 3, tw + 6, 15);
            ctx.fillStyle = last.kind === 'executor_removed' ? c.ink2 : c.failed;
            ctx.textBaseline = 'middle';
            ctx.fillText(word, tx, lane.y + 11);
          }
        }
      }
    }
    // driver-side errors (no executor): a faint dashed line across the plot. Driver log signals stay in the tooltip
    // only: drawn across every lane they hid the tasks.
    const execTop = model.lanes.find((l) => l.kind === 'exec')?.y ?? H;
    for (const m of model.globalMarkers) if (m.kind !== 'signal') drawMarker(ctx, m, xOf(m.ts), 0, Math.max(0, execTop - 22), c, x0, x1, true);

    // label column divider
    ctx.fillStyle = c.grid;
    ctx.fillRect(LABEL_W - 1, 0, 1, H);
  }, [model, view, width, plotW, xOf, theme]);

  // hit-test
  const hitTest = (x: number, y: number): Omit<Hover, 'x' | 'y'> | null => {
    const lane = model.lanes.find((l) => y >= l.y && y < l.y + l.h);
    if (!lane || lane.kind === 'header' || x < LABEL_W) return lane ? { lane } : null;
    const tol = (3 / plotW) * (view[1] - view[0]);
    const t = tOf(x);
    const markers = lane.kind === 'exec' ? [...lane.markers, ...model.globalMarkers] : model.globalMarkers;
    const m = markers.find((mk) => Math.abs(mk.ts - t) <= tol);
    if (m) return { lane, marker: m };
    const ri = Math.floor((y - lane.y - 4) / (lane.barH + lane.gap));
    const row = lane.slots[ri];
    if (!row) return { lane };
    // binary search last item with s <= t + tol
    let lo = 0;
    let hi = row.length - 1;
    let idx = -1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (row[mid].s <= t + tol) {
        idx = mid;
        lo = mid + 1;
      } else hi = mid - 1;
    }
    for (let i = idx; i >= 0 && i >= idx - 50; i--) {
      if (row[i].e + tol >= t) return { lane, item: row[i] };
    }
    return { lane };
  };

  const localXY = (e: React.MouseEvent) => {
    const r = mainRef.current!.getBoundingClientRect();
    return { x: e.clientX - r.left, y: e.clientY - r.top };
  };

  const onMove = (e: React.MouseEvent) => {
    const { x, y } = localXY(e);
    const dr = dragRef.current;
    if (dr) {
      setDrag({ ...dr, x1: Math.max(LABEL_W, Math.min(LABEL_W + plotW, x)) });
      setHover(null);
      return;
    }
    const h = hitTest(x, y);
    setHover(h && (h.item || h.marker) ? { ...h, x: e.clientX, y: e.clientY } : null);
  };
  const onDown = (e: React.MouseEvent) => {
    if (e.button !== 0) return;
    const { x } = localXY(e);
    if (x < LABEL_W) return;
    setDrag({ x0: x, x1: x });
  };
  const onUp = (e: React.MouseEvent) => {
    const dr = dragRef.current;
    if (!dr) return;
    const a = Math.min(dr.x0, dr.x1);
    const b = Math.max(dr.x0, dr.x1);
    setDrag(null);
    if (b - a > 6) {
      setView([tOf(a), tOf(b)]);
      return;
    }
    // click: open the stage of a clicked task/stage
    const { x, y } = localXY(e);
    const h = hitTest(x, y);
    if (h?.item && (h.item.ref.t === 'stage' || h.item.ref.t === 'task')) onOpenStage(h.item.ref.v);
  };

  const zoomAt = useCallback(
    (clientX: number, factor: number) => {
      const r = mainRef.current?.getBoundingClientRect();
      if (!r) return;
      const x = clientX - r.left;
      const t = tOf(Math.max(LABEL_W, Math.min(LABEL_W + plotW, x)));
      setView(([a, b]) => {
        const minSpan = 50;
        let na = t - (t - a) * factor;
        let nb = t + (b - t) * factor;
        if (nb - na < minSpan) return [a, b];
        const full = model.t1 - model.t0;
        if (nb - na > full) return [model.t0, model.t1];
        if (na < model.t0) {
          nb += model.t0 - na;
          na = model.t0;
        }
        if (nb > model.t1) {
          na -= nb - model.t1;
          nb = model.t1;
        }
        return [na, nb];
      });
    },
    [tOf, plotW, model],
  );

  // native wheel listeners (passive: false) so we can prevent page scroll while zooming
  useEffect(() => {
    const ax = axisRef.current;
    const cv = mainRef.current;
    if (!ax || !cv) return;
    const onAxis = (e: WheelEvent) => {
      e.preventDefault();
      zoomAt(e.clientX, e.deltaY > 0 ? 1.25 : 0.8);
    };
    const onMain = (e: WheelEvent) => {
      if (!(e.ctrlKey || e.metaKey || e.altKey)) return;
      e.preventDefault();
      zoomAt(e.clientX, e.deltaY > 0 ? 1.25 : 0.8);
    };
    ax.addEventListener('wheel', onAxis, { passive: false });
    cv.addEventListener('wheel', onMain, { passive: false });
    return () => {
      ax.removeEventListener('wheel', onAxis);
      cv.removeEventListener('wheel', onMain);
    };
  }, [zoomAt]);

  const zoomed = view[0] !== model.t0 || view[1] !== model.t1;

  return (
    <>
      <div className="row" style={{ alignItems: 'center', gap: 10, padding: '10px 16px' }}>
        <div className="legend">
          <span className="item">
            <span className="sw" style={{ background: 'var(--series-1)' }} />
            <span className="sw" style={{ background: 'var(--series-1-soft)', marginLeft: -4 }} />
            Task (two shades tell neighboring stages apart)
          </span>
          <span className="item">
            <span className="sw" style={{ background: 'var(--sev-high)' }} />
            Failed
          </span>
          <span className="item">
            <span className="sw" style={{ background: 'var(--panel-3)' }} />
            Executor up (bars: running tasks)
          </span>
          <span className="item">
            <span
              className="sw"
              style={{ background: 'repeating-linear-gradient(135deg, color-mix(in srgb, var(--sev-medium) 55%, transparent) 0 2px, color-mix(in srgb, var(--sev-medium) 15%, transparent) 2px 6px)' }}
            />
            Up but idle a minute or more: paid for, no task
          </span>
          <span className="item">
            <span className="sw" style={{ background: 'transparent', border: '1px dashed var(--line)' }} />
            Not running: before it was added or after it was removed
          </span>
          <span className="item">
            <span className="mk" style={{ borderColor: 'var(--good)' }} />
            Added
          </span>
          <span className="item">
            <span className="mk" style={{ borderColor: 'var(--sev-info)' }} />
            Removed
          </span>
          <span className="item">
            <span className="mk" style={{ borderColor: 'var(--sev-high)' }} />
            Out of memory, lost or error (dashed: on the driver)
          </span>
          <span className="item">
            <span className="mk" style={{ borderColor: 'var(--sev-medium)' }} />
            Problem line in its log
          </span>
          <span className="item">
            <svg width="10" height="10" aria-hidden>
              <path d="M5 0.5 L9.5 5 L5 9.5 L0.5 5 Z" style={{ fill: 'var(--c2)' }} />
            </svg>
            Disk spill
          </span>
        </div>
        <span className="grow" />
        <span className="muted small">
          Drag to zoom into a range. Scroll over the time axis (or Ctrl+scroll) to zoom. Click a task to open its stage.
        </span>
        <button className="btn small" disabled={!zoomed} onClick={() => setView([model.t0, model.t1])}>
          Reset zoom
        </button>
      </div>
      <div className="gantt-wrap">
        <canvas ref={axisRef} className="gantt-axis" style={{ cursor: 'ew-resize' }} />
        <div className="gantt-scroll">
          <div style={{ position: 'relative' }}>
            <canvas
              ref={mainRef}
              onMouseMove={onMove}
              onMouseDown={onDown}
              onMouseUp={onUp}
              onMouseLeave={() => {
                setHover(null);
                setDrag(null);
              }}
              style={{ cursor: drag ? 'col-resize' : hover?.item ? 'pointer' : 'crosshair' }}
            />
            {drag && Math.abs(drag.x1 - drag.x0) > 2 && (
              <div
                style={{
                  position: 'absolute',
                  top: 0,
                  bottom: 0,
                  left: Math.min(drag.x0, drag.x1),
                  width: Math.abs(drag.x1 - drag.x0),
                  background: 'color-mix(in srgb, var(--accent) 14%, transparent)',
                  borderLeft: '1px solid var(--accent)',
                  borderRight: '1px solid var(--accent)',
                  pointerEvents: 'none',
                }}
              />
            )}
          </div>
        </div>
      </div>
      {hover && <GanttTooltip h={hover} />}
    </>
  );
}

const END_WORD: Record<string, string> = { oom: 'out of memory', executor_lost: 'lost', executor_killed: 'killed', executor_removed: 'removed' };

function drawMarker(
  ctx: CanvasRenderingContext2D,
  m: GanttMarker,
  x: number,
  y: number,
  h: number,
  c: ReturnType<typeof cssColors>,
  x0: number,
  x1: number,
  dashed = false,
) {
  if (x < x0 || x > x1) return;
  const col = markerColor(m.kind, c);
  ctx.fillStyle = col;
  if (m.kind === 'spill') {
    // diamond at the bottom of the lane: spill is a volume event, not a lifecycle edge
    const cy = y + h - 5;
    ctx.beginPath();
    ctx.moveTo(x, cy - 4.5);
    ctx.lineTo(x + 4.5, cy);
    ctx.lineTo(x, cy + 4.5);
    ctx.lineTo(x - 4.5, cy);
    ctx.closePath();
    ctx.fill();
    ctx.strokeStyle = c.panel;
    ctx.lineWidth = 1;
    ctx.stroke();
    return;
  }
  if (dashed) {
    ctx.globalAlpha = 0.55;
    ctx.fillRect(Math.round(x) - 0.5, y, 1.5, h);
    ctx.globalAlpha = 1;
  } else {
    ctx.fillRect(Math.round(x) - 1, y, 2, h);
  }
  // cap triangle
  ctx.beginPath();
  ctx.moveTo(x - 4, y);
  ctx.lineTo(x + 4, y);
  ctx.lineTo(x, y + 5);
  ctx.closePath();
  ctx.fill();
}

function GanttTooltip({ h }: { h: Hover }) {
  const style: React.CSSProperties = {
    left: Math.min(h.x + 14, window.innerWidth - 400),
    top: h.y + 14 > window.innerHeight - 160 ? h.y - 150 : h.y + 14,
  };
  if (h.marker) {
    const m = h.marker;
    return (
      <div className="tooltip" style={style}>
        <div className="tt-title">{MARKER_LABEL[m.kind] ?? m.kind}</div>
        <div className="small" style={{ marginBottom: 4 }}>
          {truncate(m.label, 200)}
        </div>
        <div className="tt-row">
          <span>Time</span>
          <span>{fmtTs(m.ts, true)}</span>
        </div>
        {m.executor_id && (
          <div className="tt-row">
            <span>Executor</span>
            <span>{m.executor_id}</span>
          </div>
        )}
      </div>
    );
  }
  const it = h.item!;
  const rows: [string, string][] = [];
  let title = '';
  if (it.ref.t === 'job') {
    const j = it.ref.v;
    title = `Job ${j.spark_job_id}`;
    if (j.description) rows.push(['Description', truncate(j.description, 80)]);
    rows.push(['Result', j.result ?? 'running']);
    rows.push(['Stages', String(j.stage_ids?.length ?? 0)]);
  } else if (it.ref.t === 'stage') {
    const s = it.ref.v;
    title = `Stage ${s.stage_id}${s.stage_attempt ? `.${s.stage_attempt}` : ''}`;
    if (s.name) rows.push(['Name', truncate(s.name, 80)]);
    rows.push(['Status', s.status ?? '–']);
  } else {
    const t = it.ref.v;
    title = `Task ${t.task_id} (stage ${t.stage_id}${t.stage_attempt ? `.${t.stage_attempt}` : ''})`;
    rows.push(['Executor', t.executor_id ?? '–']);
    rows.push(['Status', t.failed ? 'Failed' : 'Succeeded']);
  }
  rows.push(['Start', fmtTs(it.s, true)]);
  rows.push(['Duration', fmtDuration(it.e - it.s)]);
  return (
    <div className="tooltip" style={style}>
      <div className="tt-title" style={it.failed ? { color: 'var(--sev-high-text)' } : undefined}>
        {title}
      </div>
      {rows.map(([k, v]) => (
        <div className="tt-row" key={k}>
          <span>{k}</span>
          <span style={{ textAlign: 'right' }}>{v}</span>
        </div>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ page */

export default function Timeline() {
  const { cid } = useCluster();
  const nav = useNavigate();
  const [sp, setQ] = useQueryState();
  const apps = useAsync((s) => api.dataset<AppRow>(cid, 'apps', { limit: 200, sort: 'start_time' }, s), [cid]);
  const ctxParam = sp.get('ctx');
  const ctx = ctxParam ?? apps.data?.rows[0]?.spark_context_id ?? null;
  const ready = !!apps.data || !!apps.error;
  const g = useAsync((s) => api.gantt(cid, ctx, 20000, s), [cid, ctx], ready);
  const [wrapRef, width] = useWidth<HTMLDivElement>();
  const incRows = useAsync((s) => api.datasetOpt<IncidentRow>(cid, 'incidents', { limit: 5000 }, s), [cid]);
  const [zoom, setZoom] = useState<(Zoom & { id: string }) | null>(null);
  useEffect(() => setZoom(null), [cid, ctx]);
  const bounds = useMemo(() => (g.data ? chartBounds(g.data) : null), [g.data]);
  const incs = useMemo(() => {
    const m = new Map<string, IncidentRow>();
    for (const r of incRows.data?.rows ?? []) {
      if (r.spark_context_id && g.data?.ctx && r.spark_context_id !== g.data.ctx) continue;
      if (!m.has(r.incident_id) || r.role === 'root') m.set(r.incident_id, r);
    }
    return [...m.values()].filter((r) => r.incident_start !== null).sort((a, b) => a.incident_rank - b.incident_rank);
  }, [incRows.data, g.data]);
  const pick = (r: IncidentRow) =>
    setZoom((z) => (z?.id === r.incident_id ? null : { id: r.incident_id, t0: r.incident_start!, t1: r.incident_end ?? r.incident_start!, n: Date.now() }));

  return (
    <div className="page wide">
      <div className="page-head">
        <div>
          <h1>When did things go wrong?</h1>
          <p className="sub">
            Every Spark job and stage as a bar over time, then one lane per executor: each small block is one task it ran, stacked when tasks ran side by side
            on its cores. Red blocks failed; a lane that ends with a red line lost its executor. Pick an incident to zoom to it, or drag across the chart.
          </p>
          <OtherRunsNote />
        </div>
        <div className="actions">
          {apps.data && apps.data.rows.length > 1 && (
            <label className="cluster-switch">
              <span>Spark context</span>
              <select className="select" value={ctx ?? ''} onChange={(e) => setQ({ ctx: e.target.value })}>
                {apps.data.rows.map((a) => (
                  <option key={a.spark_context_id} value={a.spark_context_id}>
                    {a.spark_context_id} {a.app_name ? `(${a.app_name})` : ''}
                  </option>
                ))}
              </select>
            </label>
          )}
          <DataLink cid={cid} dataset="tasks" />
        </div>
      </div>
      <div className="panel" ref={wrapRef}>
        {!ready ? (
          <Loading />
        ) : g.error ? (
          <ErrorState error={g.error} onRetry={g.reload} />
        ) : (
          <Async state={g} label="Loading tasks…">
            {(d) =>
              d.tasks.length === 0 && d.stages.length === 0 && d.jobs.length === 0 ? (
                <Empty title="Nothing to draw">No jobs, stages or tasks were found in the event log for this Spark context.</Empty>
              ) : (
                <>
                  {d.sampled && (
                    <div className="pager" style={{ borderTop: 0, borderBottom: '1px solid var(--line-soft)' }}>
                      Showing a sample of {fmtNum(d.tasks.length)} of {fmtNum(d.tasks_total)} tasks. Failed and slow tasks are kept.
                    </div>
                  )}
                  {incs.length > 0 && bounds && <WhereItBroke t0={bounds.t0} t1={bounds.t1} incs={incs} active={zoom?.id ?? null} onPick={pick} onReset={() => setZoom(null)} cid={cid} />}
                  {width > 0 && (
                    <GanttCanvas
                      g={d}
                      width={width - 2}
                      onOpenStage={(s) => nav(to.stages(cid, d.ctx, s.stage_id, s.stage_attempt))}
                      zoomTo={zoom}
                    />
                  )}
                </>
              )
            }
          </Async>
        )}
      </div>
      {ready && (
        <div style={{ marginTop: 16 }}>
          <SpillShuffleChart cid={cid} ctx={g.data?.ctx ?? ctx} />
        </div>
      )}
    </div>
  );
}

/** The incidents on the run's clock. Click one to zoom the task chart to it; the worst failure comes first. */
/** The chart's time range (same rule as buildModel), without building the lanes. */
function chartBounds(g: Gantt): { t0: number; t1: number } {
  let t0 = g.start ?? Infinity;
  let t1 = g.end ?? -Infinity;
  const consider = (s: number | null, e: number | null) => {
    if (s !== null) t0 = Math.min(t0, s);
    if (e !== null) t1 = Math.max(t1, e);
    if (s !== null) t1 = Math.max(t1, s);
  };
  g.jobs.forEach((j) => consider(j.start, j.end));
  g.stages.forEach((s) => consider(s.start, s.end));
  g.tasks.forEach((t) => consider(t.start, t.end));
  return Number.isFinite(t0) && Number.isFinite(t1) ? { t0, t1 } : { t0: 0, t1: 1 };
}

function WhereItBroke({ t0, t1, incs, active, onPick, onReset, cid }: { t0: number; t1: number; incs: IncidentRow[]; active: string | null; onPick: (r: IncidentRow) => void; onReset: () => void; cid: string }) {
  const pct = (t: number) => Math.max(0, Math.min(100, ((t - t0) / (t1 - t0 || 1)) * 100));
  const worst = incs[0];
  const sel = incs.find((i) => i.incident_id === active);
  return (
    <div className="where-broke">
      <div className="panel-head">
        <div>
          <h3>Where it broke</h3>
          <p className="note">The incidents found in this run, on the same clock as the chart below. Click one to zoom the chart to it.</p>
        </div>
        <div className="row" style={{ gap: 8 }}>
          {worst && worst.incident_id !== active && (
            <button className="btn small" onClick={() => onPick(worst)}>
              Zoom to {worst.incident_id}: {truncate(worst.incident_title, 40)}
            </button>
          )}
          {active && (
            <button className="btn small ghost" onClick={onReset}>
              Show the whole run
            </button>
          )}
        </div>
      </div>
      <div className="inc-strip" style={{ ['--lw' as string]: `${LABEL_W}px` }}>
        {incs.map((i) => {
          const s = i.incident_start!;
          const e = i.incident_end ?? s;
          return (
            <button key={i.incident_id} className={`inc-strip-row ${i.incident_id === active ? 'on' : ''}`} aria-pressed={i.incident_id === active} onClick={() => onPick(i)}>
              <span className="inc-strip-label" title={`${i.incident_id}: ${i.incident_title}`}>
                <b>{i.incident_id}</b> {i.incident_title}
              </span>
              <span className="inc-strip-track">
                <span className={`inc-strip-bar sev-${i.incident_severity}`} style={{ left: `${pct(s)}%`, width: `max(4px, ${pct(e) - pct(s)}%)` }} />
              </span>
            </button>
          );
        })}
        {sel && (
          <p className="small" style={{ margin: '6px 16px 0' }}>
            {sel.incident_id} ran {fmtTs(sel.incident_start)}
            {sel.incident_end && sel.incident_end !== sel.incident_start ? ` to ${fmtTs(sel.incident_end)} (${fmtDuration(sel.incident_end - sel.incident_start!)})` : ''}.{' '}
            {sel.incident_impact && <span className="muted">Cost: {sel.incident_impact}. </span>}
            <Link to={to.findings(cid, sel.finding_id)}>Root cause and how it spread</Link>
          </p>
        )}
      </div>
    </div>
  );
}

/** With one run picked: say that other runs used the same executors at the same time (their tasks are not drawn). */
function OtherRunsNote() {
  const { runs, run } = useRunScopeCtx();
  const cur = runs.find((r) => r.run_key === run);
  const n = cur?.overlapping_runs?.length ?? 0;
  if (!cur || !n) return null;
  return (
    <p className="small st-warn" style={{ margin: '4px 0 0' }}>
      Only this run's jobs, stages and tasks are drawn. {fmtNum(n)} other {n === 1 ? 'run was' : 'runs were'} running on the same executors at the same
      time: the gaps in its lanes, waiting for cores, the GC and lost-executor marks are partly theirs. Open a stage to see their tasks next to this run's.
    </p>
  );
}
