// Every task of one stage attempt on a time axis, one lane per executor (tasks running side by side on an
// executor's cores stack inside its lane), with a strip of how many tasks ran at each moment above it. A long
// tail where only a few tasks still run is a straggler or skew; a red bar is a failed task; colour by GC, spill,
// shuffle or memory shows which executor or which part of the stage did the heavy work.
import { useLayoutEffect, useMemo, useRef, useState } from 'react';
import type { OtherTask, TaskTimelineRow } from '../api';
import { fmtBytes, fmtDuration, fmtNum, fmtTime } from '../format';

export type Mode = 'share' | 'problems' | 'task_ms' | 'gc_ms' | 'spill' | 'shuffle_read' | 'peak_mem';
const MODES: { k: Mode; label: string }[] = [
  { k: 'share', label: 'This stage vs others' },
  { k: 'problems', label: 'Problems' },
  { k: 'task_ms', label: 'Duration' },
  { k: 'gc_ms', label: 'GC' },
  { k: 'spill', label: 'Spill' },
  { k: 'shuffle_read', label: 'Shuffle read' },
  { k: 'peak_mem', label: 'Peak memory' },
];
const LABEL_W = 78;
const STRIP_H = 34;
const AXIS_H = 18;

const val = (t: TaskTimelineRow, m: Mode): number => {
  if (m === 'spill') return (t.disk_spill ?? 0) + (t.mem_spill ?? 0);
  if (m === 'problems' || m === 'share') return 0;
  return (t[m] as number | null) ?? 0;
};
const fmtVal = (m: Mode, x: number) => (m === 'task_ms' || m === 'gc_ms' ? fmtDuration(x) : fmtBytes(x));

const execOrder = (a: string, b: string) => {
  const na = Number(a), nb = Number(b);
  if (Number.isFinite(na) && Number.isFinite(nb)) return na - nb;
  if (Number.isFinite(na)) return -1;
  if (Number.isFinite(nb)) return 1;
  return a.localeCompare(b);
};

function useWidth<T extends HTMLElement>(): [React.RefObject<T>, number] {
  const ref = useRef<T>(null);
  const [w, setW] = useState(900);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(() => setW(el.clientWidth || 900));
    ro.observe(el);
    setW(el.clientWidth || 900);
    return () => ro.disconnect();
  }, []);
  return [ref, w];
}

function niceStep(span: number, target: number): number {
  const raw = span / Math.max(1, target);
  const steps = [1e3, 2e3, 5e3, 10e3, 15e3, 30e3, 60e3, 120e3, 300e3, 600e3, 900e3, 1800e3, 3600e3, 7200e3];
  return steps.find((s) => s >= raw) ?? steps[steps.length - 1];
}

const PANEL_TITLE: Record<Mode, string> = {
  share: 'Who had the cores: this stage and the others',
  problems: 'Failed, slow and spilled tasks',
  task_ms: 'How long each task took',
  gc_ms: 'Time in garbage collection',
  spill: 'Spill per task',
  shuffle_read: 'Shuffle read per task',
  peak_mem: 'Peak memory per task',
};

type TimelineProps = {
  rows: TaskTimelineRow[]; start: number | null; end: number | null; p50: number | null; sampled: boolean; total: number | null;
  /** Revision 15: tasks of other stages on the same executors in this stage's time, drawn in orange next to its own */
  others?: OtherTask[]; othersTotal?: number;
};

/** Revision 16: the stage's tasks over time as separate panels, each shown only when it has something to say: who had
 * the cores (when other stages shared the executors), the problems (failed, slow, spilled tasks), and GC / spill /
 * shuffle / memory (opened by default when they are high, otherwise one click away). */
export function StageTaskPanels({ gcHigh, spilled, ...p }: TimelineProps & { gcHigh: boolean; spilled: boolean }) {
  const slowCut = p.p50 ? Math.max(3 * p.p50, 1000) : Infinity;
  const problems = p.rows.some((t) => t.failed || (t.task_ms ?? 0) >= slowCut || (t.disk_spill ?? 0) > 0);
  const hasAny = (k: Mode) => p.rows.some((t) => val(t, k) > 0);
  const extra = (['task_ms', 'gc_ms', 'spill', 'shuffle_read', 'peak_mem'] as Mode[]).filter(hasAny);
  const [open, setOpen] = useState<Set<Mode>>(() => new Set(extra.filter((k) => (k === 'gc_ms' && gcHigh) || (k === 'spill' && spilled))));
  const flip = (k: Mode) => setOpen((o) => {
    const n = new Set(o);
    if (n.has(k)) n.delete(k);
    else n.add(k);
    return n;
  });
  const own = { ...p, others: [], othersTotal: 0 };
  return (
    <div className="stack" style={{ gap: 14 }}>
      {p.others && p.others.length > 0 && <StageTaskTimeline {...p} mode="share" />}
      {problems ? <StageTaskTimeline {...own} mode="problems" /> : <p className="small muted" style={{ margin: 0 }}>✓ No failed, slow (3× the median) or spilled tasks.</p>}
      {extra.filter((k) => open.has(k)).map((k) => <StageTaskTimeline key={k} {...own} mode={k} onClose={() => flip(k)} />)}
      {extra.some((k) => !open.has(k)) && (
        <div className="row small" style={{ gap: 6, flexWrap: 'wrap', alignItems: 'center' }}>
          <span className="muted">Also show the tasks by:</span>
          {extra.filter((k) => !open.has(k)).map((k) => (
            <button key={k} className="btn small ghost" onClick={() => flip(k)}>＋ {MODES.find((m) => m.k === k)!.label}</button>
          ))}
        </div>
      )}
    </div>
  );
}

export function StageTaskTimeline({
  rows, start, end, p50, sampled, total, others = [], othersTotal = 0, mode, onClose,
}: TimelineProps & { mode: Mode; onClose?: () => void }) {
  const [ref, width] = useWidth<HTMLDivElement>();

  const model = useMemo(() => {
    const tasks = rows
      .filter((t) => t.launch_time !== null)
      .map((t) => ({ t, o: null as OtherTask | null, s: t.launch_time as number, e: (t.launch_time as number) + Math.max(0, t.task_ms ?? 0) }));
    if (!tasks.length) return null;
    const t0 = start ?? Math.min(...tasks.map((x) => x.s));
    const t1 = Math.max(end ?? 0, ...tasks.map((x) => x.e));
    const mine = new Set(tasks.map((x) => x.t.executor_id ?? '?'));
    const other = others
      .filter((o) => o.launch_time !== null && mine.has(o.executor_id ?? '?'))
      .map((o) => ({ t: { task_id: o.task_id ?? 0, executor_id: o.executor_id, task_ms: o.task_ms, failed: o.failed ?? null, disk_spill: o.disk_spill ?? null } as TaskTimelineRow, o,
        s: Math.max(t0, o.launch_time as number), e: Math.min(t1, (o.launch_time as number) + Math.max(0, o.task_ms ?? 0)) }))
      .filter((x) => x.e > x.s);
    // executors: lanes; tasks overlapping in time on one executor go to separate slots (its cores)
    const byExec = new Map<string, typeof tasks>();
    for (const x of tasks) {
      const k = x.t.executor_id ?? '?';
      byExec.set(k, [...(byExec.get(k) ?? []), x]);
    }
    // other stages' work per executor: one merged busy band, not their tasks
    const otherBusy = new Map<string, [number, number][]>();
    for (const x of other) {
      const k = x.t.executor_id ?? '?';
      if (!byExec.has(k)) byExec.set(k, []);
      otherBusy.set(k, [...(otherBusy.get(k) ?? []), [x.s, x.e]]);
    }
    for (const [k, v] of otherBusy) {
      const m: [number, number][] = [];
      for (const [a, b] of v.sort((p, q) => p[0] - q[0])) {
        if (m.length && a <= m[m.length - 1][1]) m[m.length - 1][1] = Math.max(m[m.length - 1][1], b);
        else m.push([a, b]);
      }
      otherBusy.set(k, m);
    }
    // the other stages, for the table under the chart
    const ostages = new Map<string, { stage: number; attempt: number; n: number; ms: number; first: number; last: number }>();
    for (const x of other) {
      const o = x.o!;
      const k = `${o.stage_id}.${o.stage_attempt ?? 0}`;
      const v = ostages.get(k) ?? { stage: o.stage_id, attempt: o.stage_attempt ?? 0, n: 0, ms: 0, first: Infinity, last: -Infinity };
      v.n += 1;
      v.ms += x.e - x.s;
      v.first = Math.min(v.first, x.s);
      v.last = Math.max(v.last, x.e);
      ostages.set(k, v);
    }
    const otherStages = [...ostages.values()].sort((a, b) => b.ms - a.ms);
    const lanes = [...byExec.keys()].sort(execOrder).map((id) => {
      const list = byExec.get(id)!.sort((a, b) => a.s - b.s);
      const slotEnd: number[] = [];
      const placed = list.map((x) => {
        let slot = slotEnd.findIndex((e) => e <= x.s);
        if (slot < 0) {
          slot = slotEnd.length;
          slotEnd.push(0);
        }
        slotEnd[slot] = x.e;
        return { ...x, slot };
      });
      return { id, placed, slots: Math.max(1, slotEnd.length), own: list.length, others: other.filter((x) => (x.t.executor_id ?? '?') === id).length, busy: list.reduce((a, x) => a + (x.e - x.s), 0) };
    });
    // other stages' tasks running at each moment, for the strip
    const oev = other.flatMap((x) => [[x.s, 1], [x.e, -1]] as [number, number][]).sort((a, b) => a[0] - b[0] || a[1] - b[1]);
    const oconc: [number, number][] = [[t0, 0]];
    let oc = 0, opeak = 0;
    for (const [at, d] of oev) {
      oc += d;
      opeak = Math.max(opeak, oc);
      oconc.push([at, oc]);
    }
    // running tasks over time (stepped, from start/end events)
    const ev = tasks.flatMap((x) => [[x.s, 1], [x.e, -1]] as [number, number][]).sort((a, b) => a[0] - b[0] || a[1] - b[1]);
    const conc: [number, number][] = [[t0, 0]];
    let cur = 0, peak = 0;
    for (const [at, d] of ev) {
      cur += d;
      peak = Math.max(peak, cur);
      conc.push([at, cur]);
    }
    // the tail: when 90% of the tasks were done, and how long the stage still ran after that
    const ends = tasks.map((x) => x.e).sort((a, b) => a - b);
    const p90end = ends[Math.max(0, Math.ceil(ends.length * 0.9) - 1)];
    const tail = t1 - p90end;
    const last = tasks.reduce((a, b) => (b.e > a.e ? b : a));
    const first = Math.min(...tasks.map((x) => x.s));
    return { tasks, t0, t1, lanes, conc, peak, p90end, tail, last, oconc, opeak, nOther: other.length, first, otherBusy, otherStages };
  }, [rows, start, end, others]);

  if (!model) return null;
  const { t0, t1, lanes, conc, peak, p90end, tail, last, oconc, opeak, nOther, first, otherBusy, otherStages } = model;
  const span = Math.max(1, t1 - t0);
  const plotW = Math.max(200, width - LABEL_W - 8);
  const x = (ms: number) => LABEL_W + ((ms - t0) / span) * plotW;
  const laneH = (slots: number) => Math.min(56, Math.max(14, slots * 4 + 4));
  let y = STRIP_H + 6;
  const laneY = lanes.map((l) => {
    const top = y;
    y += laneH(l.slots) + 3;
    return top;
  });
  const height = y + AXIS_H;
  const max = mode === 'problems' ? 0 : Math.max(0, ...rows.map((t) => val(t, mode)));
  const slowCut = p50 ? Math.max(3 * p50, 1000) : Infinity;
  const step = niceStep(span, Math.max(3, Math.floor(plotW / 110)));
  const ticks: number[] = [];
  for (let at = 0; at <= span; at += step) ticks.push(at);
  const tailShare = tail / span;
  // submitted, but no core free yet: shade it so the wait is seen, not just the work
  const waitMs = first - t0;
  const showWait = waitMs >= 2000 && waitMs / span >= 0.05;
  const failedN = rows.filter((t) => t.failed).length;
  const slowN = rows.filter((t) => (t.task_ms ?? 0) >= slowCut).length;

  const color = (t: TaskTimelineRow, o: OtherTask | null): { fill: string; op: number } => {
    if (o) return { fill: 'var(--text-faint, #999)', op: 0.25 };
    if (mode === 'share') return t.failed ? { fill: 'var(--st-crit)', op: 1 } : { fill: 'var(--series-1)', op: 0.9 };
    if (mode === 'problems') {
      if (t.failed) return { fill: 'var(--st-crit)', op: 1 };
      if ((t.task_ms ?? 0) >= slowCut) return { fill: 'var(--st-warn)', op: 1 };
      if ((t.disk_spill ?? 0) > 0) return { fill: 'var(--series-5)', op: 0.9 };
      return { fill: 'var(--series-1)', op: 0.5 };
    }
    const v = val(t, mode);
    return { fill: t.failed ? 'var(--st-crit)' : 'var(--series-1)', op: max > 0 ? 0.12 + 0.88 * (v / max) : 0.12 };
  };
  const top = Math.max(peak, opeak);
  const stepPath = (pts: [number, number][]) => {
    if (!top) return '';
    const yy = (n: number) => STRIP_H - (n / top) * (STRIP_H - 4);
    let d = `M${x(t0)},${STRIP_H}`;
    let prev = 0;
    for (const [at, n] of pts) {
      d += ` L${x(at)},${yy(prev)} L${x(at)},${yy(n)}`;
      prev = n;
    }
    return `${d} L${x(t1)},${yy(prev)} L${x(t1)},${STRIP_H} Z`;
  };
  const stripPath = stepPath(conc);
  const otherPath = nOther ? stepPath(oconc) : '';

  return (
    <div className="stage-tl">
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'baseline', flexWrap: 'wrap', gap: 8 }}>
        <h3 style={{ margin: 0 }}>{PANEL_TITLE[mode]}</h3>
        {onClose && <button className="btn small ghost" onClick={onClose} aria-label={`Hide ${PANEL_TITLE[mode]}`}>－ hide</button>}
      </div>
      <p className="muted small" style={{ margin: '4px 0 8px' }}>
        {showWait && (
          <><b className="wait-text">Waited {fmtDuration(waitMs)}</b> for a free core before its first task ({Math.round((waitMs / span) * 100)}% of its time). </>
        )}
        {tailShare >= 0.25 && tail >= 10_000 ? (
          <>
            <b className="st-warn">Long tail:</b> 90% of the tasks were done by {fmtTime(p90end)} (+{fmtDuration(p90end - t0)}); the stage then waited{' '}
            <b>{fmtDuration(tail)}</b> ({Math.round(tailShare * 100)}% of its time) for the last few. The last to finish was task {last.t.task_id} on exec {last.t.executor_id ?? '?'} (took{' '}
            {fmtDuration(last.t.task_ms)}{last.t.failed ? ', failed' : ''}).{' '}
          </>
        ) : (
          <>The work was spread evenly: 90% of the tasks were done by +{fmtDuration(p90end - t0)} of {fmtDuration(span)}. </>
        )}
        Up to {fmtNum(peak)} of its tasks ran at once on {fmtNum(lanes.length)} executor{lanes.length === 1 ? '' : 's'}
        {nOther ? <>, while other stages ran up to <b>{fmtNum(opeak)}</b> tasks at once on the same executors (grey)</> : null}.
        {sampled ? ` Drawn from ${fmtNum(rows.length)} of ${fmtNum(total)} tasks: every failed one, the longest, and an even sample of the rest.` : ''}
      </p>
      <div ref={ref} className="stage-tl-plot">
        <svg width={width} height={height} role="img" aria-label="Task timeline">
          {ticks.map((at) => (
            <g key={at}>
              <line x1={x(t0 + at)} x2={x(t0 + at)} y1={0} y2={height - AXIS_H} stroke="var(--line)" strokeWidth={1} />
              <text x={x(t0 + at)} y={height - 5} fontSize={10.5} fill="var(--muted)" textAnchor={at === 0 ? 'start' : x(t0 + at) > width - 40 ? 'end' : 'middle'}>
                {at === 0 ? 'start' : `+${fmtDuration(at)}`}
              </text>
            </g>
          ))}
          <text x={0} y={14} fontSize={10.5} fill="var(--muted)">running</text>
          <text x={0} y={27} fontSize={10.5} fill="var(--muted)">max {fmtNum(top)}</text>
          {otherPath && mode === 'share' && <path d={otherPath} fill="var(--text-3, #999)" fillOpacity={0.22} stroke="var(--text-3, #999)" strokeOpacity={0.6} strokeWidth={1} />}
          <path d={stripPath} fill="var(--series-1)" fillOpacity={0.35} stroke="var(--series-1)" strokeWidth={1} />
          {showWait && (
            <g>
              <rect x={x(t0)} y={0} width={Math.max(0, x(first) - x(t0))} height={height - AXIS_H} fill="var(--wait)" fillOpacity={0.08} />
              <line x1={x(first)} x2={x(first)} y1={0} y2={height - AXIS_H} stroke="var(--wait)" strokeDasharray="4 3" />
              {mode !== 'share' && (
                <text x={(x(t0) + x(first)) / 2} y={STRIP_H / 2 + 4} textAnchor="middle" fontSize={11} fill="var(--wait)">
                  waiting for cores · {fmtDuration(waitMs)}
                </text>
              )}
            </g>
          )}
          {tailShare >= 0.25 && tail >= 10_000 && (
            <g>
              <rect x={x(p90end)} y={0} width={Math.max(0, x(t1) - x(p90end))} height={height - AXIS_H} fill="var(--st-warn)" fillOpacity={0.07} />
              <line x1={x(p90end)} x2={x(p90end)} y1={0} y2={height - AXIS_H} stroke="var(--st-warn)" strokeDasharray="4 3" />
            </g>
          )}
          {lanes.map((l, i) => {
            const h = laneH(l.slots);
            const bh = Math.max(1.5, (h - 2) / l.slots - 1);
            return (
              <g key={l.id}>
                <rect x={LABEL_W} y={laneY[i]} width={plotW} height={h} fill="var(--panel-3)" fillOpacity={0.5} />
                {mode === 'share' && (otherBusy.get(l.id) ?? []).map(([a, b], k) => (
                  <rect key={`ob${k}`} x={x(a)} y={laneY[i]} width={Math.max(1, x(b) - x(a))} height={h} fill="var(--text-3, #999)" fillOpacity={0.22}>
                    <title>{`Other stages ran on exec ${l.id} ${fmtTime(a)} → ${fmtTime(b)} (${fmtDuration(b - a)})`}</title>
                  </rect>
                ))}
                <text x={0} y={laneY[i] + Math.min(h, 14) - 3} fontSize={11} fill="var(--text)">
                  exec {l.id}
                </text>
                {h >= 26 && (
                  <text x={0} y={laneY[i] + 24} fontSize={10} fill="var(--muted)">
                    {fmtNum(l.own)} {l.own === 1 ? 'task' : 'tasks'}
                  </text>
                )}
                {l.placed.map(({ t, o, s, e, slot }) => {
                  const c = color(t, o);
                  return (
                    <rect
                      key={o ? `o${o.stage_id}.${o.task_id}` : `${t.task_id}.${t.task_attempt}`}
                      x={x(s)}
                      y={laneY[i] + 1 + slot * (bh + 1)}
                      width={Math.max(1, x(e) - x(s))}
                      height={bh}
                      fill={c.fill}
                      fillOpacity={c.op}
                    >
                      <title>
                        {o ? `Stage ${o.stage_id}${o.stage_attempt ? `.${o.stage_attempt}` : ''} (another stage), task ${o.task_id ?? '?'} on exec ${o.executor_id ?? '?'}\n${fmtTime(o.launch_time)}, took ${fmtDuration(o.task_ms)}${o.disk_spill ? `\nspill to disk ${fmtBytes(o.disk_spill)}` : ''}` : `Task ${t.task_id} (#${t.task_index ?? '–'}) on exec ${t.executor_id ?? '?'}\n${fmtTime(s)} → ${fmtTime(e)}, took ${fmtDuration(t.task_ms)}${t.failed ? ' · FAILED' : ''}` +
                          `${t.gc_ms ? `\nGC ${fmtDuration(t.gc_ms)}` : ''}${t.disk_spill ? `\nspill to disk ${fmtBytes(t.disk_spill)}` : ''}${t.shuffle_read ? `\nshuffle read ${fmtBytes(t.shuffle_read)}` : ''}${t.peak_mem ? `\npeak memory ${fmtBytes(t.peak_mem)}` : ''}`}
                      </title>
                    </rect>
                  );
                })}
              </g>
            );
          })}
        </svg>
      </div>
      <div className="stage-tl-legend small muted">
        {mode === 'share' ? (
          <>
            <span><i style={{ background: 'var(--series-1)' }} /> this stage's tasks</span>
            <span><i style={{ background: 'var(--text-3, #999)', opacity: 0.4 }} /> other stages running on the same executors</span>
            <span><i style={{ background: 'var(--st-crit)' }} /> failed</span>
          </>
        ) : mode === 'problems' ? (
          <>
            <span><i style={{ background: 'var(--st-crit)' }} /> failed{failedN ? ` (${fmtNum(failedN)})` : ''}</span>
            <span><i style={{ background: 'var(--st-warn)' }} /> slow: 3× the median or more{slowN ? ` (${fmtNum(slowN)})` : ''}</span>
            <span><i style={{ background: 'var(--series-5)' }} /> spilled to disk</span>
            <span><i style={{ background: 'var(--series-1)', opacity: 0.5 }} /> other</span>
          </>
        ) : (
          <span>
            Darker = more {MODES.find((m) => m.k === mode)!.label.toLowerCase()}; the darkest is {fmtVal(mode, max)}. Failed tasks in red.
          </span>
        )}
        <span>Hover a bar for the task.</span>
      </div>
      {mode === 'share' && otherStages.length > 0 && (
        <details className="other-work" style={{ marginTop: 6 }}>
          <summary className="small">The other stages that ran next to it: {fmtNum(otherStages.length)} stages, {fmtNum(othersTotal || nOther)} tasks</summary>
          <div className="table-wrap" style={{ maxHeight: 320, overflowY: 'auto', marginTop: 6 }}>
            <table className="exec-table">
              <thead>
                <tr><th>Stage</th><th className="num">Tasks</th><th className="num">Task time in this stage's window</th><th>From → to</th></tr>
              </thead>
              <tbody>
                {otherStages.map((r) => (
                  <tr key={`${r.stage}.${r.attempt}`}>
                    <td className="mono">{r.stage}{r.attempt ? `.${r.attempt}` : ''}</td>
                    <td className="num">{fmtNum(r.n)}</td>
                    <td className="num">{fmtDuration(r.ms)}</td>
                    <td className="mono small">{fmtTime(r.first)} → {fmtTime(r.last)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {othersTotal > nOther && <p className="muted small" style={{ margin: '4px 0 0' }}>From the {fmtNum(nOther)} longest of {fmtNum(othersTotal)} tasks.</p>}
        </details>
      )}
    </div>
  );
}
