// What every executor was doing while one query / job ran: its own tasks, other work sharing the executors,
// and executor events (lost, OOM, Full GC, spill) in the same window.
import { useMemo, useState } from 'react';
import { api, type Gantt, type GanttMarker, type GanttTask } from '../api';
import { fmtDuration, fmtNum, fmtPct, fmtTime } from '../format';
import { useAsync } from '../hooks';
import { Panel } from './ui';
import { ExecutorUsage } from './ExecutorUsage';

// one meaning per colour: red failed or lost, purple spill, light grey ticks for full GC, grey for a planned removal
const MARK: Record<string, { color: string; label: string }> = {
  oom: { color: 'var(--st-crit)', label: 'Out of memory' },
  executor_lost: { color: 'var(--st-crit)', label: 'Executor lost' },
  executor_killed: { color: 'var(--st-crit)', label: 'Executor killed' },
  executor_removed: { color: 'var(--wait)', label: 'Executor removed' },
  full_gc: { color: 'var(--wait)', label: 'Full GC' },
  spill: { color: 'var(--spill)', label: 'Spill' },
  error: { color: 'var(--st-crit)', label: 'Error' },
};

/** Merge time spans that touch or overlap. */
function mergeSpans(spans: [number, number][]): [number, number][] {
  const out: [number, number][] = [];
  for (const [a, b] of [...spans].sort((x, y) => x[0] - y[0])) {
    if (out.length && a <= out[out.length - 1][1]) out[out.length - 1][1] = Math.max(out[out.length - 1][1], b);
    else out.push([a, b]);
  }
  return out;
}

interface Props {
  cid: string;
  ctx: string;
  /** stage ids of the focused query / job (all attempts) */
  stageIds: Set<number>;
  start: number | null;
  end: number | null;
  title: string;
  onStage?: (stage: number, attempt: number) => void;
}

export function ExecutorsAtTime({ cid, ctx, stageIds, start, end, title, onStage }: Props) {
  // every run's tasks around this window (a minute either side), so other runs' work on the same executors shows in grey
  const pad = 60_000;
  const st = useAsync(
    (s) => api.gantt(cid, ctx, 20000, s, start !== null && end !== null ? { start: start - pad, end: end + pad } : undefined),
    [cid, ctx, start, end],
  );
  return (
    <Panel
      title="What the executors were doing at the same time"
      note={`Every executor during ${title}: its tasks in blue (red when failed), and grey where the executor was busy with other jobs.`}
    >
      {st.loading && !st.data ? (
        <p className="muted small">Loading executor activity…</p>
      ) : !st.data || start === null || end === null ? (
        <p className="muted small">No task timing for this window.</p>
      ) : (
        <Lanes g={st.data} stageIds={stageIds} start={start} end={end} title={title} onStage={onStage} />
      )}
    </Panel>
  );
}

function Lanes({ g, stageIds, start, end, title, onStage }: { g: Gantt; stageIds: Set<number>; start: number; end: number; title: string; onStage?: (s: number, a: number) => void }) {
  const [hover, setHover] = useState<{ x: number; y: number; text: string } | null>(null);
  const pad = Math.max(2000, (end - start) * 0.06);
  const t0 = start - pad;
  const t1 = end + pad;
  const model = useMemo(() => {
    const inWin = (s: number | null, e: number | null) => s !== null && (e ?? s) >= t0 && s <= t1;
    const tasks = g.tasks.filter((t) => inWin(t.start, t.end));
    const marks = g.markers.filter((m) => m.ts >= t0 && m.ts <= t1);
    const execIds = new Set<string>();
    for (const e of g.executors) if ((e.added ?? 0) <= t1 && (e.removed ?? Infinity) >= t0) execIds.add(e.executor_id);
    for (const t of tasks) if (t.executor_id) execIds.add(t.executor_id);
    for (const m of marks) if (m.executor_id) execIds.add(m.executor_id);
    const execs = [...execIds].sort((a, b) => (a === 'driver' ? -1 : b === 'driver' ? 1 : Number(a) - Number(b) || a.localeCompare(b)));
    // pack each executor's tasks into rows (one per concurrent slot)
    const rowsOf = new Map<string, GanttTask[][]>();
    const otherOf = new Map<string, [number, number][]>();
    for (const ex of execs) {
      otherOf.set(ex, mergeSpans(tasks.filter((t) => t.executor_id === ex && !stageIds.has(t.stage_id) && t.start !== null)
        .map((t) => [Math.max(t0, t.start!), Math.min(t1, t.end ?? t1)] as [number, number]).filter(([a, b]) => b > a)));
      const mine = tasks.filter((t) => t.executor_id === ex && stageIds.has(t.stage_id)).sort((a, b) => (a.start ?? 0) - (b.start ?? 0));
      const rows: GanttTask[][] = [];
      const ends: number[] = [];
      for (const t of mine) {
        let r = ends.findIndex((e) => e <= (t.start ?? 0));
        if (r < 0) {
          r = rows.length;
          rows.push([]);
          ends.push(0);
        }
        rows[r].push(t);
        ends[r] = t.end ?? t1;
      }
      rowsOf.set(ex, rows);
    }
    // time on each executor: focused work vs other work
    const dur = (t: GanttTask) => Math.max(0, Math.min(t.end ?? t1, end) - Math.max(t.start ?? t0, start));
    let mineMs = 0;
    let otherMs = 0;
    const otherStages = new Set<number>();
    for (const t of tasks) {
      if (stageIds.has(t.stage_id)) mineMs += dur(t);
      else if (dur(t) > 0) {
        otherMs += dur(t);
        otherStages.add(t.stage_id);
      }
    }
    // the other work, by stage, for the table under the chart
    const byStage = new Map<string, { stage: number; attempt: number; n: number; ms: number; first: number; last: number; execs: Set<string> }>();
    for (const t of tasks) {
      if (stageIds.has(t.stage_id) || dur(t) <= 0) continue;
      const k = `${t.stage_id}.${t.stage_attempt}`;
      const v = byStage.get(k) ?? { stage: t.stage_id, attempt: t.stage_attempt, n: 0, ms: 0, first: Infinity, last: -Infinity, execs: new Set<string>() };
      v.n += 1;
      v.ms += dur(t);
      v.first = Math.min(v.first, t.start ?? Infinity);
      v.last = Math.max(v.last, t.end ?? -Infinity);
      if (t.executor_id) v.execs.add(t.executor_id);
      byStage.set(k, v);
    }
    const jobOf = new Map<number, number>();
    for (const j of g.jobs) for (const s of j.stage_ids ?? []) jobOf.set(s, j.spark_job_id);
    const otherStageRows = [...byStage.values()].sort((a, b) => b.ms - a.ms).map((v) => ({ ...v, job: jobOf.get(v.stage) ?? null }));
    return { tasks, marks, execs, rowsOf, otherOf, mineMs, otherMs, otherStageRows };
  }, [g, stageIds, start, end, t0, t1]);

  const W = 1000;
  const LABEL = 92;
  const ROW = 7;
  const LANE_PAD = 6;
  const x = (ts: number) => LABEL + ((ts - t0) / (t1 - t0)) * (W - LABEL - 8);
  let y = 22;
  const lanes = model.execs.map((ex) => {
    const rows = Math.max(1, Math.min(model.rowsOf.get(ex)?.length ?? 1, 8));
    const h = rows * ROW + LANE_PAD * 2;
    const lane = { ex, y, h, rows };
    y += h + 4;
    return lane;
  });
  const H = y + 4;
  const ticks = niceTicks(t0, t1, 6);
  const lost = g.executors.filter((e) => e.removed !== null && e.removed >= t0 && e.removed <= t1);
  const kinds = new Map<string, number>();
  for (const m of model.marks) kinds.set(m.kind, (kinds.get(m.kind) ?? 0) + 1);
  const total = model.mineMs + model.otherMs;

  return (
    <div className="eat">
      <p className="ink2" style={{ margin: '0 0 8px' }}>
        {model.otherMs > 0 ? (
          <>
            While this ran, other jobs were running on the same executors: <b>{fmtPct(model.otherMs / Math.max(1, total), 0)}</b> of the task time in
            this window was theirs (grey).
          </>
        ) : (
          <>Nothing else ran on the executors in this window: the work here had them to itself.</>
        )}{' '}
        {lost.length > 0 && (
          <>
            {lost.map((e) => `exec ${e.executor_id} was ${e.removal_category === 'oom' ? 'killed for out of memory' : `removed (${e.removal_category ?? e.removed_reason ?? 'unknown'})`} at ${fmtTime(e.removed)}`).join('; ')}.{' '}
          </>
        )}
        {[...kinds.entries()]
          .filter(([k]) => MARK[k] && !k.startsWith('executor'))
          .map(([k, n]) => `${fmtNum(n)} ${MARK[k].label.toLowerCase()}${n === 1 ? '' : ' events'}`)
          .join(', ')}
        {kinds.size ? '.' : ''}
      </p>
      <details className="exec-more">
      <summary className="small">Show the executors over time, how busy they were and their tasks</summary>
      <div style={{ position: 'relative', overflowX: 'auto' }} onMouseLeave={() => setHover(null)}>
        <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block', minWidth: 640 }} role="img" aria-label="Executor activity in this window">
          {ticks.map((t) => (
            <g key={t}>
              <line x1={x(t)} x2={x(t)} y1={16} y2={H - 4} style={{ stroke: 'var(--border-subtle)' }} />
              <text x={x(t)} y={11} textAnchor="middle" style={{ fill: 'var(--text-2)', fontSize: 11 }}>
                {fmtTime(t)}
              </text>
            </g>
          ))}
          {/* the focused query / job window */}
          <rect x={x(start)} y={16} width={Math.max(1, x(end) - x(start))} height={H - 20} style={{ fill: 'var(--accent-tint, rgba(35,86,160,0.07))', stroke: 'var(--accent, #2356a0)', strokeDasharray: '4 3', strokeWidth: 1 }} />
          {lanes.map((l) => {
            const ex = g.executors.find((e) => e.executor_id === l.ex);
            const alive0 = Math.max(t0, ex?.added ?? t0);
            const alive1 = Math.min(t1, ex?.removed ?? t1);
            return (
              <g key={l.ex}>
                <text x={4} y={l.y + l.h / 2 + 4} style={{ fill: 'var(--text-1, var(--text))', fontSize: 12, fontFamily: 'var(--font-mono, monospace)' }}>
                  {l.ex === 'driver' ? 'driver' : `exec ${l.ex}`}
                </text>
                <rect x={x(alive0)} y={l.y} width={Math.max(0, x(alive1) - x(alive0))} height={l.h} rx={3} style={{ fill: 'var(--surface-2)' }} />
                {(model.otherOf.get(l.ex) ?? []).map(([a, b], i) => (
                  <rect key={`o${i}`} x={x(a)} y={l.y + 2} width={Math.max(1, x(b) - x(a))} height={l.h - 4} rx={2}
                    style={{ fill: 'var(--text-3, #9a9a9a)', opacity: 0.28 }}
                    onMouseMove={(ev) => {
                      const r = (ev.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect();
                      setHover({ x: ev.clientX - r.left, y: ev.clientY - r.top, text: `Other jobs ran here ${fmtTime(a)} → ${fmtTime(b)} (${fmtDuration(b - a)})` });
                    }} />
                ))}
                {(model.rowsOf.get(l.ex) ?? []).slice(0, l.rows).map((row, ri) =>
                  row.map((t) => {
                    const mine = stageIds.has(t.stage_id);
                    const s = Math.max(t0, t.start ?? t0);
                    const e = Math.min(t1, t.end ?? t1);
                    return (
                      <rect
                        key={t.task_id}
                        x={x(s)}
                        y={l.y + LANE_PAD + ri * ROW}
                        width={Math.max(1.2, x(e) - x(s))}
                        height={ROW - 1.5}
                        style={{ fill: t.failed ? 'var(--st-crit)' : 'var(--series-1)', cursor: onStage ? 'pointer' : 'default' }}
                        onClick={() => onStage?.(t.stage_id, t.stage_attempt)}
                        onMouseMove={(ev) => {
                          const r = (ev.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect();
                          setHover({
                            x: ev.clientX - r.left,
                            y: ev.clientY - r.top,
                            text: `Task ${t.task_id} · stage ${t.stage_id}${t.stage_attempt ? `.${t.stage_attempt}` : ''}${mine ? '' : ' (other work)'} · ${fmtDuration((t.end ?? t1) - (t.start ?? t0))}${t.failed ? ' · failed' : ''}`,
                          });
                        }}
                      />
                    );
                  }),
                )}
                {model.marks
                  .filter((m) => m.executor_id === l.ex)
                  .map((m, i) => (
                    <Marker key={i} m={m} x={x(m.ts)} y={l.y} h={l.h} onHover={(text, ev) => {
                      const r = (ev.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect();
                      setHover({ x: ev.clientX - r.left, y: ev.clientY - r.top, text });
                    }} />
                  ))}
              </g>
            );
          })}
        </svg>
        {hover && (
          <div className="tt" style={{ position: 'absolute', left: Math.min(hover.x + 12, 700), top: hover.y + 12, pointerEvents: 'none' }}>
            {hover.text}
          </div>
        )}
      </div>
      <div className="legend small" style={{ marginTop: 6 }}>
        <span className="item"><span className="sw" style={{ background: 'var(--series-1)' }} />{title}</span>
        <span className="item"><span className="sw" style={{ background: 'var(--text-3, #9a9a9a)', opacity: 0.35 }} />Other jobs running</span>
        <span className="item"><span className="sw" style={{ background: 'var(--st-crit)' }} />Failed task, out of memory, lost</span>
        <span className="item"><span className="sw" style={{ background: 'var(--wait)', width: 2 }} />Full GC</span>
        <span className="item"><span style={{ color: 'var(--spill)' }}>◆</span> Spill</span>
        <span className="item muted">Dashed box: when {title} ran</span>
      </div>
      {model.otherStageRows.length > 0 && (
        <details className="other-work" style={{ marginTop: 8 }}>
          <summary className="small">The other work on these executors: {fmtNum(model.otherStageRows.length)} stages, {fmtNum(model.otherStageRows.reduce((a, r) => a + r.n, 0))} tasks</summary>
          <div className="table-wrap" style={{ maxHeight: 320, overflowY: 'auto', marginTop: 6 }}>
            <table className="exec-table">
              <thead>
                <tr><th>Stage</th><th>Job</th><th className="num">Tasks here</th><th className="num">Task time in this window</th><th>From → to</th><th>Executors</th></tr>
              </thead>
              <tbody>
                {model.otherStageRows.map((r) => (
                  <tr key={`${r.stage}.${r.attempt}`}>
                    <td className="mono">{r.stage}{r.attempt ? `.${r.attempt}` : ''}</td>
                    <td className="mono">{r.job ?? '–'}</td>
                    <td className="num">{fmtNum(r.n)}</td>
                    <td className="num">{fmtDuration(r.ms)}</td>
                    <td className="mono small">{fmtTime(r.first)} → {fmtTime(r.last)}</td>
                    <td className="mono small">{[...r.execs].sort().map((e) => (e === 'driver' ? 'driver' : `exec ${e}`)).join(', ')}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      )}
      <ExecutorUsage g={g} tasks={model.tasks} execs={model.execs.filter((e) => e !== 'driver')} t0={t0} t1={t1} start={start} end={end} W={W} LABEL={LABEL} />
      <ExecTaskTable g={g} tasks={model.tasks} execs={model.execs} marks={model.marks} stageIds={stageIds} title={title} onStage={onStage} />
      </details>
    </div>
  );
}

/** Tasks per executor for the focused work: how many, how long, which stages, what failed, and what else ran there. */
function ExecTaskTable({ g, tasks, execs, marks, stageIds, title, onStage }: { g: Gantt; tasks: GanttTask[]; execs: string[]; marks: GanttMarker[]; stageIds: Set<number>; title: string; onStage?: (s: number, a: number) => void }) {
  const rows = execs
    .map((ex) => {
      const all = tasks.filter((t) => t.executor_id === ex);
      const mine = all.filter((t) => stageIds.has(t.stage_id));
      const d = (t: GanttTask) => Math.max(0, (t.end ?? t.start ?? 0) - (t.start ?? 0));
      const stages = new Map<string, { s: number; a: number; n: number; failed: number }>();
      for (const t of mine) {
        const k = `${t.stage_id}.${t.stage_attempt}`;
        const v = stages.get(k) ?? { s: t.stage_id, a: t.stage_attempt, n: 0, failed: 0 };
        v.n += 1;
        if (t.failed) v.failed += 1;
        stages.set(k, v);
      }
      const longest = mine.reduce<GanttTask | null>((b, t) => (!b || d(t) > d(b) ? t : b), null);
      const ev = marks.filter((m) => m.executor_id === ex && MARK[m.kind] && m.kind !== 'spill');
      const info = g.executors.find((e) => e.executor_id === ex);
      return {
        ex,
        n: mine.length,
        failed: mine.filter((t) => t.failed).length,
        ms: mine.reduce((a, t) => a + d(t), 0),
        other: all.filter((t) => !stageIds.has(t.stage_id)).reduce((a, t) => a + d(t), 0),
        first: Math.min(...mine.map((t) => t.start ?? Infinity)),
        last: Math.max(...mine.map((t) => t.end ?? -Infinity)),
        longest,
        longestMs: longest ? d(longest) : 0,
        stages: [...stages.values()].sort((a, b) => a.s - b.s || a.a - b.a),
        ev,
        info,
      };
    })
    .filter((r) => r.n > 0 || r.ev.length > 0);
  if (!rows.length) return null;
  const maxMs = Math.max(1, ...rows.map((r) => r.ms));
  const totalMs = rows.reduce((a, r) => a + r.ms, 0) || 1;
  return (
    <div style={{ marginTop: 14 }}>
      <h3 style={{ marginBottom: 4 }}>Tasks of {title} on each executor</h3>
      <p className="muted small" style={{ margin: '0 0 6px' }}>
        Task time is the sum of task durations. A row in red had failed tasks or lost the executor.{g.sampled ? ` Based on a sample of ${fmtNum(g.tasks.length)} of ${fmtNum(g.tasks_total)} tasks.` : ''}
      </p>
      <div style={{ overflowX: 'auto' }}>
        <table className="exec-table">
          <thead>
            <tr>
              <th>Executor</th>
              <th className="num">Tasks</th>
              <th>Task time</th>
              <th className="num">Longest task</th>
              <th>Busy</th>
              <th>Stages (tasks)</th>
              <th className="num">Other work</th>
              <th>Events</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const lostHere = r.info?.removed !== null && r.info?.removed !== undefined && r.ev.some((m) => m.kind !== 'full_gc');
              return (
                <tr key={r.ex} className={r.failed || lostHere ? 'row-bad' : ''}>
                  <td className="mono">
                    {r.ex === 'driver' ? 'driver' : `exec ${r.ex}`}
                    {r.info?.host ? <div className="muted small">{r.info.host}</div> : null}
                  </td>
                  <td className="num">
                    {fmtNum(r.n)}
                    {r.failed > 0 && <span className="st-crit"> · {fmtNum(r.failed)} failed</span>}
                  </td>
                  <td className="tt-time">
                    <div>
                      {fmtDuration(r.ms)} <span className="muted small">{fmtPct(r.ms / totalMs, 0)}</span>
                    </div>
                    <span className="mini-bar">
                      <span style={{ width: `${(r.ms / maxMs) * 100}%`, background: r.failed ? 'var(--st-crit)' : 'var(--series-1)' }} />
                    </span>
                  </td>
                  <td className="num">
                    {r.longest ? (
                      <>
                        {fmtDuration(r.longestMs)} <span className="muted small">task {r.longest.task_id}</span>
                      </>
                    ) : (
                      '–'
                    )}
                  </td>
                  <td className="mono small">{r.n ? `${fmtTime(r.first)} → ${fmtTime(r.last)}` : '–'}</td>
                  <td className="wrap">
                    {r.stages.map((st) => (
                      <button key={`${st.s}.${st.a}`} className="tchip" onClick={() => onStage?.(st.s, st.a)} title={`Open stage ${st.s}${st.a ? ` attempt ${st.a + 1}` : ''}`}>
                        {st.s}
                        {st.a ? `.${st.a + 1}` : ''} <span className="muted">({fmtNum(st.n)}{st.failed ? <span className="st-crit">, {st.failed} failed</span> : null})</span>
                      </button>
                    ))}
                  </td>
                  <td className="num">{r.other > 0 ? fmtDuration(r.other) : <span className="muted">none</span>}</td>
                  <td className="small wrap">
                    {r.ev.length ? (
                      <EventList ev={r.ev} />
                    ) : (
                      <span className="muted">–</span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

const EVENT_RANK = ['oom', 'executor_lost', 'executor_killed', 'executor_removed', 'full_gc', 'error'];

/** First time of each event kind on one executor, worst first: "Out of memory 18:07:23 · Error ×5 from 18:07:24". */
function EventList({ ev }: { ev: GanttMarker[] }) {
  const byKind = new Map<string, { first: number; n: number }>();
  for (const m of ev) {
    const v = byKind.get(m.kind) ?? { first: m.ts, n: 0 };
    v.n += 1;
    v.first = Math.min(v.first, m.ts);
    byKind.set(m.kind, v);
  }
  const rank = (k: string) => (EVENT_RANK.indexOf(k) + 1 || 99);
  return (
    <>
      {[...byKind.entries()]
        .sort((a, b) => rank(a[0]) - rank(b[0]))
        .map(([k, v]) => (
          <div key={k} style={{ color: MARK[k]?.color }}>
            {MARK[k]?.label ?? k}
            {v.n > 1 ? ` ×${v.n} from` : ''} {fmtTime(v.first)}
          </div>
        ))}
    </>
  );
}

function Marker({ m, x, y, h, onHover }: { m: GanttMarker; x: number; y: number; h: number; onHover: (text: string, ev: React.MouseEvent<SVGElement>) => void }) {
  const meta = MARK[m.kind] ?? { color: 'var(--st-ref)', label: m.kind };
  const text = `${fmtTime(m.ts)} · ${meta.label}: ${m.label}`;
  if (m.kind === 'spill' || m.kind === 'signal')
    return <path d={`M${x},${y + h - 8} l3.5,3.5 l-3.5,3.5 l-3.5,-3.5 Z`} style={{ fill: meta.color }} onMouseMove={(e) => onHover(text, e)} />;
  if (m.kind === 'full_gc')
    return <rect x={x - 0.6} y={y + 1} width={1.2} height={7} style={{ fill: meta.color }} onMouseMove={(e) => onHover(text, e)} />;
  if (m.kind === 'executor_removed')
    return <path d={`M${x - 4},${y - 1} h8 l-4,6 Z`} style={{ fill: meta.color }} onMouseMove={(e) => onHover(text, e)} />;
  return <rect x={x - 1} y={y - 2} width={2} height={h + 4} style={{ fill: meta.color }} onMouseMove={(e) => onHover(text, e)} />;
}

function niceTicks(t0: number, t1: number, n: number): number[] {
  const span = t1 - t0;
  const steps = [1e3, 5e3, 1e4, 3e4, 6e4, 3e5, 6e5, 9e5, 18e5, 36e5, 72e5];
  const step = steps.find((s) => span / s <= n) ?? 144e5;
  const out: number[] = [];
  for (let t = Math.ceil(t0 / step) * step; t <= t1; t += step) out.push(t);
  return out;
}
