// How busy each executor was over time, from task metrics: each task's CPU, GC, bytes and wait time are
// spread evenly over its run time, then summed per executor per time bucket. Same time axis as the lanes above.
import { useMemo, useState } from 'react';
import type { Gantt, GanttTask } from '../api';
import { fmtBytes, fmtPct, fmtTime } from '../format';

type MetricKey = 'cpu' | 'busy' | 'mem' | 'shuffle_read' | 'shuffle_write' | 'spill' | 'gc' | 'wait';

const METRICS: { k: MetricKey; label: string; pct: boolean; explain: string }[] = [
  { k: 'cpu', label: 'CPU', pct: true, explain: 'CPU time of running tasks as a share of the executor’s cores. Low CPU while cores are busy means tasks were waiting (shuffle, I/O, GC).' },
  { k: 'busy', label: 'Cores busy', pct: true, explain: 'Running tasks as a share of the executor’s cores (task slots in use).' },
  { k: 'mem', label: 'Task memory', pct: false, explain: 'Sum of peak execution memory of the tasks running at that moment (an upper bound: peaks may not overlap).' },
  { k: 'shuffle_read', label: 'Shuffle read', pct: false, explain: 'Shuffle bytes read per second, from other executors and local disk.' },
  { k: 'shuffle_write', label: 'Shuffle write', pct: false, explain: 'Shuffle bytes written per second for the next stage.' },
  { k: 'spill', label: 'Spill', pct: false, explain: 'Bytes spilled to disk per second: data that did not fit in memory.' },
  { k: 'gc', label: 'GC', pct: true, explain: 'JVM garbage-collection time as a share of the executor’s cores.' },
  { k: 'wait', label: 'Shuffle wait', pct: true, explain: 'Time tasks spent waiting for shuffle blocks, as a share of the executor’s cores.' },
];

const BUCKETS = 120;
const ROW_H = 34;

export function ExecutorUsage({ g, tasks, execs, t0, t1, start, end, W, LABEL }: { g: Gantt; tasks: GanttTask[]; execs: string[]; t0: number; t1: number; start: number; end: number; W: number; LABEL: number }) {
  const [metric, setMetric] = useState<MetricKey>('cpu');
  const hasCpu = tasks.some((t) => t.cpu_ms !== null && t.cpu_ms !== undefined);
  const hasMetrics = tasks.some((t) => t.run_ms !== null && t.run_ms !== undefined);
  const series = useMemo(() => usageBuckets(g, tasks, execs, t0, t1, BUCKETS), [g, tasks, execs, t0, t1]);

  if (!hasMetrics) return null;
  const meta = METRICS.find((m) => m.k === metric)!;
  const peak = Math.max(...execs.map((ex) => Math.max(0, ...(series.get(ex)?.[metric] ?? []))));
  const top = meta.pct ? 1 : peak || 1;
  const fmt = (v: number) => (meta.pct ? fmtPct(v, 0) : metric === 'mem' ? fmtBytes(v) : `${fmtBytes(v)}/s`);
  const x = (ts: number) => LABEL + ((ts - t0) / (t1 - t0)) * (W - LABEL - 8);
  const bx = (b: number) => x(t0 + ((b + 0.5) * (t1 - t0)) / BUCKETS);
  const H = execs.length * (ROW_H + 4) + 6;
  // the focused window's average per executor, for the sentence above the chart
  const inWin = (b: number) => t0 + ((b + 0.5) * (t1 - t0)) / BUCKETS >= start && t0 + ((b + 0.5) * (t1 - t0)) / BUCKETS <= end;
  const avg = execs.map((ex) => {
    const arr = series.get(ex)?.[metric] ?? new Float64Array();
    let sum = 0;
    let n = 0;
    for (let b = 0; b < BUCKETS; b++)
      if (inWin(b)) {
        sum += arr[b];
        n++;
      }
    return { ex, v: n ? sum / n : 0 };
  });
  const cpuLow = metric === 'cpu' && hasCpu ? avg.filter((a) => {
    const busy = series.get(a.ex)?.busy ?? new Float64Array();
    let bs = 0;
    let n = 0;
    for (let b = 0; b < BUCKETS; b++) if (inWin(b)) { bs += busy[b]; n++; }
    return n > 0 && bs / n > 0.3 && a.v < (bs / n) * 0.6;
  }) : [];

  return (
    <div className="exec-usage">
      <div className="exec-usage-head">
        <h3 style={{ margin: 0 }}>How busy each executor was</h3>
        <div className="seg" role="group" aria-label="Usage metric">
          {METRICS.filter((m) => m.k !== 'cpu' || hasCpu).map((m) => (
            <button key={m.k} className={metric === m.k ? 'on' : ''} aria-pressed={metric === m.k} onClick={() => setMetric(m.k)}>
              {m.label}
            </button>
          ))}
        </div>
      </div>
      <p className="muted small" style={{ margin: '4px 0 6px' }}>
        {meta.explain} Average in the dashed window: {avg.map((a) => `exec ${a.ex} ${fmt(a.v)}`).join(', ')}.
        {cpuLow.length > 0 && <b className="st-warn"> {cpuLow.map((a) => `exec ${a.ex}`).join(', ')}: cores busy but CPU low, so tasks were mostly waiting.</b>}
        {g.sampled ? ' Based on a task sample, so totals are lower than real.' : ''}
      </p>
      <div style={{ overflowX: 'auto' }}>
        <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block', minWidth: 640 }} role="img" aria-label={`${meta.label} per executor over time`}>
          <rect x={x(start)} y={0} width={Math.max(1, x(end) - x(start))} height={H} style={{ fill: 'var(--accent-tint, rgba(35,86,160,0.07))', stroke: 'var(--accent, #2356a0)', strokeDasharray: '4 3', strokeWidth: 1 }} />
          {execs.map((ex, i) => {
            const y0 = 4 + i * (ROW_H + 4);
            const arr = series.get(ex)?.[metric] ?? new Float64Array(BUCKETS);
            const y = (v: number) => y0 + ROW_H - Math.min(1, v / top) * (ROW_H - 2);
            let d = `M${bx(0)},${y0 + ROW_H}`;
            for (let b = 0; b < BUCKETS; b++) d += ` L${bx(b)},${y(arr[b])}`;
            d += ` L${bx(BUCKETS - 1)},${y0 + ROW_H} Z`;
            const mx = Math.max(0, ...arr);
            const over = meta.pct && mx > 1;
            return (
              <g key={ex}>
                <text x={4} y={y0 + ROW_H / 2 + 4} style={{ fill: 'var(--text-1, var(--text))', fontSize: 12, fontFamily: 'var(--font-mono, monospace)' }}>
                  {ex === 'driver' ? 'driver' : `exec ${ex}`}
                </text>
                <rect x={LABEL} y={y0} width={W - LABEL - 8} height={ROW_H} rx={3} style={{ fill: 'var(--surface-2)' }} />
                {meta.pct && <line x1={LABEL} x2={W - 8} y1={y(1)} y2={y(1)} style={{ stroke: 'var(--border-subtle)', strokeDasharray: '2 3' }} />}
                <path d={d} style={{ fill: metric === 'spill' || metric === 'gc' || metric === 'wait' ? 'var(--st-warn)' : 'var(--series-1)', fillOpacity: 0.35, stroke: metric === 'spill' || metric === 'gc' || metric === 'wait' ? 'var(--st-warn)' : 'var(--series-1)', strokeWidth: 1.2 }} />
                <text x={W - 10} y={y0 + 11} textAnchor="end" style={{ fill: over ? 'var(--st-warn)' : 'var(--text-2)', fontSize: 10 }}>
                  peak {fmt(mx)}
                </text>
              </g>
            );
          })}
        </svg>
      </div>
      <div className="small muted">
        {meta.pct ? 'Dotted line: 100% of the executor’s cores.' : `Scale: 0 to ${fmt(top)}, the same for every executor.`} Times {fmtTime(t0)} to {fmtTime(t1)} UTC, in {BUCKETS} steps of {Math.max(1, Math.round((t1 - t0) / BUCKETS / 1000))} s.
      </div>
    </div>
  );
}

export type UsageKey = 'cpu' | 'busy' | 'mem' | 'shuffle_read' | 'shuffle_write' | 'spill' | 'gc' | 'wait';

/** Per executor, per time bucket: CPU / cores busy / GC / shuffle wait as a share of its cores, task memory in
 *  bytes, and shuffle / spill in bytes per second. Each task's metrics are spread evenly over its run time. */
export function usageBuckets(g: Gantt, tasks: GanttTask[], execs: string[], t0: number, t1: number, BUCKETS: number): Map<string, Record<UsageKey, Float64Array>> {
    const bms = (t1 - t0) / BUCKETS;
    const cores = new Map(g.executors.map((e) => [e.executor_id, Math.max(1, e.cores ?? 1)]));
    const out = new Map<string, Record<UsageKey, Float64Array>>();
    for (const ex of execs) {
      const z = () => new Float64Array(BUCKETS);
      out.set(ex, { cpu: z(), busy: z(), mem: z(), shuffle_read: z(), shuffle_write: z(), spill: z(), gc: z(), wait: z() });
    }
    for (const t of tasks) {
      const s = out.get(t.executor_id ?? '');
      if (!s || t.start === null) continue;
      const ts = t.start;
      const te = Math.max(ts + 1, t.end ?? ts + 1);
      const dur = te - ts;
      const b0 = Math.max(0, Math.floor((ts - t0) / bms));
      const b1 = Math.min(BUCKETS - 1, Math.floor((te - t0) / bms));
      for (let b = b0; b <= b1; b++) {
        const bs = t0 + b * bms;
        const ov = Math.min(te, bs + bms) - Math.max(ts, bs);
        if (ov <= 0) continue;
        const f = ov / dur;
        s.busy[b] += ov;
        s.cpu[b] += (t.cpu_ms ?? 0) * f;
        s.gc[b] += (t.gc_ms ?? 0) * f;
        s.wait[b] += (t.fetch_wait_ms ?? 0) * f;
        s.shuffle_read[b] += (t.shuffle_read ?? 0) * f;
        s.shuffle_write[b] += (t.shuffle_write ?? 0) * f;
        s.spill[b] += (t.disk_spill ?? 0) * f;
        s.mem[b] += t.peak_mem ?? 0;
      }
    }
    for (const [ex, s] of out) {
      const cap = (cores.get(ex) ?? 1) * bms;
      for (let b = 0; b < BUCKETS; b++) {
        s.busy[b] /= cap;
        s.cpu[b] /= cap;
        s.gc[b] /= cap;
        s.wait[b] /= cap;
        s.shuffle_read[b] /= bms / 1000;
        s.shuffle_write[b] /= bms / 1000;
        s.spill[b] /= bms / 1000;
      }
    }
    return out;
}
