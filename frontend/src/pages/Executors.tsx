import { useMemo } from 'react';
import { Link } from 'react-router-dom';
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis, ZAxis } from 'recharts';
import { api, optional, type AppRow, type ExecutorProfileRow, type Gantt, type GcEventRow, type IncidentRow } from '../api';
import { HBars } from '../components/charts';
import { usageBuckets } from '../components/ExecutorUsage';
import { useCluster, useRunScopeCtx } from '../components/Shell';
import { Async, DataLink, Empty, isProblemRemoval, Panel, REMOVAL, RemovalBadge } from '../components/ui';
import { fmtBytes, fmtDuration, fmtNum, fmtPct, fmtTime, fmtTs, tickLabel, timeTicks } from '../format';
import { useAsync, useQueryState, useWidth } from '../hooks';
import { to } from '../links';
import { gcBreach } from '../thresholds';
import { indexProblems } from '../problems';

const ekey = (r: ExecutorProfileRow) => `${r.spark_context_id ?? ''}|${r.executor_id}`;
const execLabel = (id: string) => (id === 'driver' ? 'driver' : `exec ${id}`);
const FULL_GC_MIN = 3; // rules.full_gc_min default (CONTRACT Revision 3 item 4)

const tooltipStyle = {
  contentStyle: { background: 'var(--panel)', border: '1px solid var(--line)', borderRadius: 6, color: 'var(--ink)', fontSize: 12.5 },
  itemStyle: { color: 'var(--ink)', padding: 0 },
  labelStyle: { color: 'var(--ink-2)', marginBottom: 4 },
};

const HEAT_BUCKETS = 160;

function Lifetimes({ rows, end, selected, onSelect, gantt }: { rows: ExecutorProfileRow[]; end: number | null; selected: string | null; onSelect: (r: ExecutorProfileRow) => void; gantt: Gantt | null }) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const { t0, t1 } = useMemo(() => {
    let a = Infinity;
    let b = -Infinity;
    for (const r of rows) {
      if (r.added_time !== null) a = Math.min(a, r.added_time);
      if (r.removed_time !== null) b = Math.max(b, r.removed_time);
      if (r.added_time !== null) b = Math.max(b, r.added_time);
    }
    // stop shortly after the last task: a cluster often idles long after the work is done
    for (const t of gantt?.tasks ?? []) if (t.end !== null && t.end > b) b = t.end;
    if (!Number.isFinite(a)) return { t0: 0, t1: 1 };
    if (!gantt?.tasks.length && end !== null) b = Math.max(b, end);
    b = b + Math.max(1000, (b - a) * 0.04);
    return { t0: a, t1: b > a ? b : a + 1000 };
  }, [rows, end, gantt]);
  const idleAfter = end !== null && end > t1 ? end - t1 : 0;
  const trackW = Math.max(100, width - 150);
  // how busy each executor was: CPU as a share of its cores, and task memory relative to the busiest moment
  const heat = useMemo(() => {
    if (!gantt || !gantt.tasks.some((t) => t.run_ms !== null && t.run_ms !== undefined)) return null;
    const ids = rows.map((r) => r.executor_id ?? '');
    const u = usageBuckets(gantt, gantt.tasks, ids, t0, t1, HEAT_BUCKETS);
    const hasCpu = gantt.tasks.some((t) => t.cpu_ms !== null && t.cpu_ms !== undefined);
    let memMax = 0;
    for (const s of u.values()) for (const v of s.mem) memMax = Math.max(memMax, v);
    return { u, hasCpu, memMax };
  }, [gantt, rows, t0, t1]);
  const marks = useMemo(() => (gantt?.markers ?? []).filter((m) => m.kind === 'full_gc' || m.kind === 'spill' || m.kind === 'oom'), [gantt]);
  const pct = (t: number) => ((t - t0) / (t1 - t0)) * 100;
  const ticks = timeTicks(t0, t1, Math.max(3, Math.floor(trackW / 120)));
  return (
    <div ref={ref} className="exec-lanes">
      <div className="exec-lane" style={{ cursor: 'default', height: 22 }}>
        <span className="name muted small" style={{ fontFamily: 'var(--font)' }}>
          UTC
        </span>
        <div style={{ position: 'relative', height: 22 }}>
          {ticks.map((t) => (
            <span key={t} className="muted" style={{ position: 'absolute', left: `${pct(t)}%`, transform: 'translateX(-50%)', fontSize: 11, top: 3, whiteSpace: 'nowrap' }}>
              {tickLabel(t, t1 - t0)}
            </span>
          ))}
        </div>
      </div>
      {rows.map((r) => {
        const a = r.added_time ?? t0;
        const b = r.removed_time ?? t1;
        const k = ekey(r);
        const bad = isProblemRemoval(r.removal_category);
        return (
          <div
            key={k}
            className={`exec-lane ${selected === k ? 'selected' : ''}`}
            onClick={() => onSelect(r)}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => (e.key === 'Enter' || e.key === ' ') && (e.preventDefault(), onSelect(r))}
            title={`${r.executor_id}: ${fmtTs(r.added_time)} → ${r.removed_time ? fmtTs(r.removed_time) : 'end of run'} (${fmtDuration(r.lifetime_ms)})${r.removed_reason ? `\n${r.removed_reason}` : ''}`}
          >
            <span className="name" style={bad ? { color: 'var(--sev-high-text)', fontWeight: 600 } : undefined}>
              {execLabel(r.executor_id)}
            </span>
            <div className="track" style={{ marginRight: 12 }}>
              {ticks.map((t) => (
                <span key={t} style={{ position: 'absolute', left: `${pct(t)}%`, top: -7, bottom: -7, width: 1, background: 'var(--grid)' }} />
              ))}
              <span className={`life ${heat ? "neutral" : ""}`} style={{ left: `${pct(a)}%`, width: `${Math.max(0.3, pct(b) - pct(a))}%` }} />
              {/* busy with any run's tasks (executors are shared): under this run's bands, so what is left empty was really idle */}
              {(gantt?.busy ?? []).filter((x) => x.executor_id === r.executor_id && x.busy_end > t0 && x.busy_start < t1).map((x, i) => (
                <span key={`b${i}`} className="busy-any" style={{ left: `${pct(Math.max(t0, x.busy_start))}%`, width: `${Math.max(0.2, pct(Math.min(t1, x.busy_end)) - pct(Math.max(t0, x.busy_start)))}%` }} />
              ))}
              {heat && (() => {
                const s = heat.u.get(r.executor_id ?? '');
                if (!s) return null;
                const cpu = heat.hasCpu ? s.cpu : s.busy;
                // one hard-stop gradient per band: no seams between buckets
                const band = (vals: ArrayLike<number>, color: string) => {
                  const stops: string[] = [];
                  for (let i = 0; i < HEAT_BUCKETS; i++) {
                    const v = Math.min(1, Math.max(0, vals[i]));
                    const c = v < 0.02 ? 'transparent' : `color-mix(in srgb, ${color} ${Math.round(20 + v * 80)}%, transparent)`;
                    stops.push(`${c} ${(i * 100) / HEAT_BUCKETS}%`, `${c} ${((i + 1) * 100) / HEAT_BUCKETS}%`);
                  }
                  return `linear-gradient(to right, ${stops.join(', ')})`;
                };
                const mem = heat.memMax ? Array.from(s.mem, (v) => v / heat.memMax) : [];
                return (
                  <>
                    <span className="heat cpu" style={{ background: band(cpu, 'var(--series-1)') }} />
                    {mem.length > 0 && <span className="heat mem" style={{ background: band(mem, 'var(--series-2)') }} />}
                  </>
                );
              })()}
              {marks
                .filter((m) => m.executor_id === r.executor_id && m.ts >= t0 && m.ts <= t1)
                .map((m, i) => (
                  <span key={`m${i}`} className={`lane-mark ${m.kind}`} style={{ left: `${pct(m.ts)}%` }} title={`${fmtTime(m.ts)} · ${m.label}`} />
                ))}
              {r.removed_time !== null && <span className={`end-mark ${bad ? 'problem' : ''}`} style={{ left: `calc(${pct(b)}% - 1px)` }} />}
              {r.removed_time !== null && (
                <span
                  className={`end-label ${bad ? 'problem' : ''}`}
                  style={pct(b) > 70 ? { right: `calc(${100 - pct(b)}% + 6px)` } : { left: `calc(${pct(b)}% + 6px)` }}
                >
                  {(REMOVAL[r.removal_category ?? 'other'] ?? REMOVAL.other).label.toLowerCase()} {fmtTime(r.removed_time)}
                </span>
              )}
            </div>
          </div>
        );
      })}
      {idleAfter > 60_000 && (
        <p className="muted small" style={{ margin: '6px 0 0 150px' }}>
          Executors still up kept running after the last task; the cluster stopped {fmtDuration(idleAfter)} later, at {fmtTime(end)} (not drawn).
        </p>
      )}
    </div>
  );
}

/** Who left early, when and why, in words; then one row per executor. */
function ExecSummary({ rows, selected, onPick, probs }: { rows: ExecutorProfileRow[]; selected: string | null; onPick: (r: ExecutorProfileRow) => void; probs: Map<string, IncidentRow[]> }) {
  const bad = rows.filter((r) => isProblemRemoval(r.removal_category)).sort((a, b) => (a.removed_time ?? 0) - (b.removed_time ?? 0));
  const sorted = [...rows].sort(
    (a, b) => Number(isProblemRemoval(b.removal_category)) - Number(isProblemRemoval(a.removal_category)) || a.executor_id.localeCompare(b.executor_id, undefined, { numeric: true }),
  );
  const failed = rows.reduce((n, r) => n + (r.failed_tasks ?? 0), 0);
  // executors are shared: on a run's page their counts still cover every run that used them
  const shared = useRunScopeCtx().run !== null;
  const all = shared ? ' (all runs)' : '';
  return (
    <Panel
      title="What happened to the executors"
      note="Executors are the worker JVMs that run tasks. Losing one mid-run throws away its running tasks and the shuffle files it held, so other tasks fail and stages are retried."
    >
      <ul className="exec-story">
        <li>
          <b>{fmtNum(rows.length)}</b> executor{rows.length === 1 ? '' : 's'} ran {fmtNum(rows.reduce((n, r) => n + (r.tasks ?? 0), 0))} tasks{shared ? ' for all the runs that shared them, not only this one' : ''}
          {failed ? (
            <>
              ; <b className="bad">{fmtNum(failed)}</b> task attempts failed
            </>
          ) : null}
          .
        </li>
        {(() => {
          // Revision 13: what each executor was given (the first one that says; they are usually all alike)
          const g = rows.find((r) => r.heap_mb || r.unified_memory);
          if (!g) return null;
          const MB = 1 << 20;
          const perCore = g.unified_memory && g.cores ? g.unified_memory / g.cores : null;
          return (
            <li>
              Each executor was given <b>{g.cores ?? '?'} cores</b>
              {g.heap_mb ? <>, <b>{fmtBytes(g.heap_mb * MB)}</b> of heap</> : null}
              {g.overhead_mb ? <> (+{fmtBytes(g.overhead_mb * MB)} overhead)</> : null}
              {g.offheap_mb ? <> and {fmtBytes(g.offheap_mb * MB)} off-heap</> : null}
              {g.unified_memory ? (
                <>
                  . Spark could use <b>{fmtBytes(g.unified_memory)}</b> of it for running tasks and cached data together
                  {g.storage_memory ? <>, {fmtBytes(g.storage_memory)} of that held back for cache when cache needs it</> : null}
                  {perCore ? <>: about <b>{fmtBytes(perCore)} per core</b>, so a task that holds more than that spills to disk</> : null}
                </>
              ) : null}
              .
            </li>
          );
        })()}
        {bad.length === 0 ? (
          <li>None was lost, killed or ran out of memory.</li>
        ) : (
          bad.map((r) => (
            <li key={ekey(r)}>
              <button className="linklike" onClick={() => onPick(r)}>
                {execLabel(r.executor_id)}
              </button>{' '}
              <span className="bad">{(REMOVAL[r.removal_category ?? 'other'] ?? REMOVAL.other).label.toLowerCase()}</span> at {fmtTime(r.removed_time)}, after{' '}
              {fmtDuration(r.lifetime_ms)}
              {r.removed_reason ? <span className="muted">: {r.removed_reason}</span> : '.'}
            </li>
          ))
        )}
      </ul>
      <div className="table-wrap">
        <table className="tbl exec-tbl">
          <thead>
            <tr>
              <th>Executor</th>
              <th title="Cores, heap (and overhead), and the unified memory Spark used for tasks and cache">Given</th>
              <th>Lived</th>
              <th>How it ended</th>
              <th className="num">Tasks{all}</th>
              <th className="num" title="Share of its cores' time spent running tasks (of any run) while it was alive">
                Busy{all}
              </th>
              <th className="num" title="Share of task time spent in garbage collection">
                GC
              </th>
              <th className="num">Disk spill</th>
              <th>Problems</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r) => {
              const k = ekey(r);
              return (
                <tr key={k} className={selected === k ? 'sel' : ''} onClick={() => onPick(r)}>
                  <td>
                    <button
                      className="linklike mono"
                      onClick={(e) => {
                        e.stopPropagation();
                        onPick(r);
                      }}
                    >
                      {execLabel(r.executor_id)}
                    </button>
                    {r.host && <div className="muted small">{r.host}</div>}
                  </td>
                  <td className="small nowrap">
                    {r.cores ?? '?'} cores
                    {r.heap_mb ? <div>{fmtBytes(r.heap_mb * 1048576)} heap{r.overhead_mb ? ` +${fmtBytes(r.overhead_mb * 1048576)}` : ''}</div> : null}
                    {r.unified_memory ? <div className="muted" title="Execution + storage memory (spark.memory.fraction of the heap)">{fmtBytes(r.unified_memory)} for tasks + cache</div> : null}
                  </td>
                  <td className="small nowrap">
                    {fmtTime(r.added_time)} → {r.removed_time ? fmtTime(r.removed_time) : 'end'}
                    <div className="muted">{fmtDuration(r.lifetime_ms)}</div>
                  </td>
                  <td>
                    {r.removed_time === null && !r.removal_category ? (
                      <span className="muted small">still running at the end</span>
                    ) : (
                      <RemovalBadge category={r.removal_category} reason={r.removed_reason} />
                    )}
                  </td>
                  <td className="num">
                    {fmtNum(r.tasks)}
                    {r.failed_tasks ? <div className="bad small">{fmtNum(r.failed_tasks)} failed</div> : null}
                  </td>
                  <td className="num">{fmtPct(r.busy_share, 0)}</td>
                  <td className={`num ${gcBreach(r.gc_share) ? 'bad' : ''}`}>{fmtPct(r.gc_share, 0)}</td>
                  <td className="num">{r.disk_spill ? fmtBytes(r.disk_spill) : '–'}</td>
                  <td>
                    <ExecPills cid={r.cluster_id} exec={r.executor_id} rows={probs.get(r.executor_id) ?? []} />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

/** Counts by removal category; autoscale / termination are neutral, oom / killed / lost are problems. */
function GcPerExecutor({ rows, selected, onPick }: { rows: ExecutorProfileRow[]; selected: string | null; onPick: (r: ExecutorProfileRow) => void }) {
  const jvm = rows.some((r) => (r.gc_pause_ms ?? 0) > 0 || (r.gc_pauses ?? 0) > 0);
  const data = [...rows]
    .map((r) => ({ r, v: jvm ? r.gc_pause_ms ?? 0 : r.gc_ms ?? 0 }))
    .filter((x) => x.v > 0)
    .sort((a, b) => b.v - a.v)
    .slice(0, 15);
  return (
    <Panel
      title={jvm ? 'GC pause time per executor' : 'GC time per executor'}
      note={
        jvm
          ? `Total JVM pause time from the GC log. Red names had ${FULL_GC_MIN} or more full GCs, which stop everything and usually mean the heap is too small.`
          : 'Task time spent in garbage collection (from the event log). Red names spend 20% or more of task time in GC.'
      }
    >
      {data.length === 0 ? (
        <p className="muted">No garbage-collection time recorded.</p>
      ) : (
        <HBars
          labelWidth={96}
          data={data.map(({ r, v }) => ({
            key: ekey(r),
            label: execLabel(r.executor_id),
            parts: [{ value: v, color: 'var(--series-1)', name: 'GC' }],
            display: jvm ? `${fmtDuration(v)}${r.full_gcs ? `, ${fmtNum(r.full_gcs)} full` : ''}` : `${fmtDuration(v)} (${fmtPct(r.gc_share, 0)})`,
            flagged: jvm ? (r.full_gcs ?? 0) >= FULL_GC_MIN : gcBreach(r.gc_share),
            selected: selected === ekey(r),
            title: jvm
              ? `${fmtNum(r.gc_pauses)} pauses, ${fmtNum(r.full_gcs)} full GCs, heap after GC up to ${r.max_heap_after_mb ? `${fmtNum(r.max_heap_after_mb)} MiB` : '–'}`
              : `GC ${fmtDuration(r.gc_ms)} of ${fmtDuration(r.run_ms)} task run time`,
            onClick: () => onPick(r),
          }))}
        />
      )}
    </Panel>
  );
}

export default function Executors() {
  const { cid, summary } = useCluster();
  const [sp, setQ] = useQueryState();
  const ctxParam = sp.get('ctx');
  const selExec = sp.get('executor');
  const apps = useAsync((s) => api.dataset<AppRow>(cid, 'apps', { limit: 200, sort: 'start_time' }, s), [cid]);
  const multi = (apps.data?.rows.length ?? 0) > 1;
  const ctx = ctxParam ?? (multi ? apps.data!.rows[0].spark_context_id : null);
  const gantt = useAsync((s) => optional(api.gantt(cid, ctx ?? (apps.data?.rows[0]?.spark_context_id ?? null), 20000, s)), [cid, ctx, apps.data], !!apps.data || !!apps.error);
  const st = useAsync(
    (s) => api.dataset<ExecutorProfileRow>(cid, 'executor_profile', { limit: 2000, sort: 'added_time', spark_context_id: ctx }, s),
    [cid, ctx],
    !!apps.data || !!apps.error,
  );
  const selRow = st.data?.rows.find((r) => r.executor_id === selExec && (!ctx || r.spark_context_id === ctx)) ?? null;
  const selKey = selRow ? ekey(selRow) : null;
  const pick = (r: ExecutorProfileRow) => setQ({ ctx: r.spark_context_id, executor: r.executor_id }, true);
  const inc = useAsync((s) => api.datasetOpt<IncidentRow>(cid, 'incidents', { limit: 5000 }, s), [cid]);
  const probsByExec = useMemo(() => problemsByExecutor(inc.data?.rows ?? [], ctx ?? apps.data?.rows[0]?.spark_context_id ?? null), [inc.data, ctx, apps.data]);

  return (
    <div className="page wide">
      <div className="page-head">
        <div>
          <h1>Executors</h1>
          <details className="sub"><summary>About this page</summary>
            When each executor lived, why it left, and how hard the JVM worked to free memory. Select an executor to see its garbage-collection
            pauses and heap over time.
          </details>
        </div>
        <div className="actions">
          {multi && (
            <label className="cluster-switch">
              <span>Spark context</span>
              <select className="select" value={ctx ?? ''} onChange={(e) => setQ({ ctx: e.target.value, executor: null })}>
                {apps.data!.rows.map((a) => (
                  <option key={a.spark_context_id} value={a.spark_context_id}>
                    {a.spark_context_id} {a.app_name ? `(${a.app_name})` : ''}
                  </option>
                ))}
              </select>
            </label>
          )}
          <DataLink cid={cid} dataset="executor_profile" />
        </div>
      </div>
      <Async state={st} label="Loading executors…">
        {(t) =>
          t.rows.length === 0 ? (
            <div className="panel">
              <Empty title="No executors">The event log recorded no executors for this cluster or context.</Empty>
            </div>
          ) : (
            <div className="stack">
              <ExecSummary rows={t.rows} selected={selKey} onPick={pick} probs={probsByExec} />
              {selRow && <ExecutorDetail cid={cid} r={selRow} onClose={() => setQ({ executor: null }, true)} />}
              <Panel
                title="When each executor was alive, and how hard it worked"
                note={gantt.data?.busy?.length
                  ? "One row per executor on the run's clock; the label at its end says how it left. Executors are shared: grey hatch is the executor running tasks of any run, and the bands on top are this run's own tasks (upper: CPU, lower: task memory; darker is busier). Hatch without a band means the cores were busy with other runs' work; only what is left empty was idle."
                  : "One row per executor on the run's clock. The bar spans its lifetime and the label at its end says how it left. Inside the bar, the upper band is CPU in use and the lower band is task memory in use: darker is busier, empty is idle."}
              >
                <Lifetimes rows={t.rows} end={summary.end_time} selected={selKey} onSelect={pick} gantt={gantt.data ?? null} />
                <div className="legend small" style={{ marginTop: 8 }}>
                  <span className="item"><span className="sw" style={{ background: 'var(--series-1)' }} />This run's CPU (upper band)</span>
                  <span className="item"><span className="sw" style={{ background: 'var(--series-2)' }} />This run's task memory (lower band)</span>
                  {gantt.data?.busy?.length ? <span className="item"><span className="sw busy-any-sw" />Busy with any run's tasks</span> : null}
                  <span className="item"><span className="sw" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }} />Alive, nothing running</span>
                  <span className="item">◆ Full GC (JVM paused)</span>
                  <span className="item">● Spilled to disk</span>
                  <span className="item" style={{ color: 'var(--st-crit)' }}>| Left because of a problem</span>
                </div>
              </Panel>
              <GcPerExecutor rows={t.rows} selected={selKey} onPick={pick} />
            </div>
          )
        }
      </Async>
    </div>
  );
}

function ExecutorDetail({ cid, r, onClose }: { cid: string; r: ExecutorProfileRow; onClose: () => void }) {
  const logFilter = r.executor_id === 'driver' ? { source: 'driver' } : { executor_id: r.executor_id, source: 'executor' };
  return (
    <Panel
      title={r.executor_id === 'driver' ? 'Driver' : `Executor ${r.executor_id}`}
      note={r.host ?? undefined}
      actions={
        <>
          <Link className="btn small" to={to.logs(cid, logFilter)}>
            Its logs
          </Link>
          <Link className="btn small" to={to.logs(cid, { ...logFilter, level: 'ERROR' })}>
            Its errors
          </Link>
          <Link className="btn small" to={to.logs(cid, { ...logFilter, logger: 'gc' })}>
            Its GC log
          </Link>
          <Link className="btn small" to={to.timeline(cid, r.spark_context_id)}>
            Timeline
          </Link>
          <button className="btn small ghost" onClick={onClose}>
            Close
          </button>
        </>
      }
    >
      <div className="stack">
        {r.removed_time !== null && (
          <p className={isProblemRemoval(r.removal_category) ? 'callout bad' : 'callout'}>
            <RemovalBadge category={r.removal_category} reason={r.removed_reason} />{' '}
            Removed at {fmtTs(r.removed_time)} after {fmtDuration(r.lifetime_ms)}
            {r.removed_reason ? <>: <span className="mono small">{r.removed_reason}</span></> : '.'}
          </p>
        )}
        <div className="grid-2">
          <dl className="kv">
            <dt>Added</dt>
            <dd>{fmtTs(r.added_time)}</dd>
            <dt>Removed</dt>
            <dd>{r.removed_time ? fmtTs(r.removed_time) : 'still running at the end of the logs'}</dd>
            <dt>Tasks</dt>
            <dd>
              {fmtNum(r.tasks)} ({fmtNum(r.failed_tasks)} failed), busy {fmtPct(r.busy_share, 0)} of {fmtNum(r.cores)} cores
            </dd>
            <dt>GC in tasks</dt>
            <dd>
              {fmtDuration(r.gc_ms)} of {fmtDuration(r.run_ms)} ({fmtPct(r.gc_share)})
            </dd>
            <dt>Spill</dt>
            <dd>
              {fmtBytes(r.mem_spill)} memory, {fmtBytes(r.disk_spill)} disk
            </dd>
            <dt>Peak memory</dt>
            <dd>{fmtBytes(r.max_peak_mem)}</dd>
          </dl>
          <dl className="kv">
            <dt>Log lines</dt>
            <dd>
              {fmtNum(r.log_lines)} ({fmtNum(r.log_errors_lines)} error, {fmtNum(r.log_warn_lines)} warn)
            </dd>
            <dt>Signals</dt>
            <dd>
              {fmtNum(r.signals)}
              {r.top_signals?.length ? (
                <div className="chips" style={{ marginTop: 4 }}>
                  {r.top_signals.map((s) => (
                    <Link key={s} className="chip" to={to.logs(cid, { ...logFilter, signal: s })}>
                      {s}
                    </Link>
                  ))}
                </div>
              ) : null}
            </dd>
            <dt>Exceptions</dt>
            <dd>
              {fmtNum(r.exceptions)}
              {r.top_exception && <div className="mono small wrap-any">{r.top_exception}</div>}
            </dd>
            <dt>App</dt>
            <dd className="mono small">{r.app_id ?? '–'}</dd>
          </dl>
        </div>
        <ExecutorProblems cid={cid} r={r} />
        <GcCharts cid={cid} r={r} />
      </div>
    </Panel>
  );
}

/** The incident problems this executor took part in (as the place it happened, or the one that went away). */
function ExecutorProblems({ cid, r }: { cid: string; r: ExecutorProfileRow }) {
  const inc = useAsync((s) => api.datasetOpt<IncidentRow>(cid, 'incidents', { limit: 5000 }, s), [cid]);
  const rows = useMemo(() => problemsByExecutor(inc.data?.rows ?? [], r.spark_context_id).get(r.executor_id) ?? [], [inc.data, r.executor_id, r.spark_context_id]);
  if (!rows.length) return null;
  return (
    <div>
      <div className="lbl-sm">Problems on this {r.executor_id === 'driver' ? 'driver' : 'executor'}</div>
      <ul className="exec-probs">
        {rows.map((x) => (
          <li key={x.problem_id}>
            <Link className={`stage-prob sev-${x.incident_severity} ${x.role === 'root' ? 'root' : ''}`} to={to.findings(cid, x.finding_id)}>
              {x.role === 'root' ? '● ' : ''}
              {x.kind}
            </Link>{' '}
            {x.stages && <span className="muted small">stage {x.stages} · </span>}
            <span className="small">
              {x.role === 'root' ? 'most likely root cause of ' : 'part of '}
              <Link to={to.findings(cid, x.finding_id)}>
                {x.incident_id}: {x.incident_title}
              </Link>
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function GcCharts({ cid, r }: { cid: string; r: ExecutorProfileRow }) {
  const params =
    r.executor_id === 'driver'
      ? { source: 'driver', limit: 5000, sort: 'ts' }
      : { source: 'executor', executor_id: r.executor_id, app_id: r.app_id ?? null, limit: 5000, sort: 'ts' };
  const st = useAsync((s) => api.datasetOpt<GcEventRow>(cid, 'gc_events', params, s), [cid, r.executor_id, r.app_id]);
  const model = useMemo(() => {
    const rows = (st.data?.rows ?? []).filter((g) => g.ts !== null);
    const isFull = (g: GcEventRow) => /full/i.test(g.kind ?? '');
    const pauses = rows.filter((g) => g.pause_ms !== null && /pause/i.test(g.kind ?? 'pause'));
    const young = pauses.filter((g) => !isFull(g)).map((g) => ({ ts: g.ts!, pause: g.pause_ms!, kind: g.kind, cause: g.cause }));
    const full = pauses.filter(isFull).map((g) => ({ ts: g.ts!, pause: g.pause_ms!, kind: g.kind, cause: g.cause }));
    const heap = rows.filter((g) => g.heap_after_mb !== null).map((g) => ({ ts: g.ts!, after: g.heap_after_mb, total: g.heap_total_mb }));
    const total = pauses.reduce((a, g) => a + (g.pause_ms ?? 0), 0);
    const longest = pauses.reduce<GcEventRow | null>((m, g) => (!m || (g.pause_ms ?? 0) > (m.pause_ms ?? 0) ? g : m), null);
    const maxAfter = heap.reduce((m, h) => Math.max(m, h.after ?? 0), 0);
    const maxTotal = heap.reduce((m, h) => Math.max(m, h.total ?? 0), 0);
    const t0 = rows.length ? rows[0].ts! : 0;
    const t1 = rows.length ? rows[rows.length - 1].ts! : 1;
    return { rows, young, full, heap, total, longest, maxAfter, maxTotal, domain: [t0, t1 > t0 ? t1 : t0 + 1000] as [number, number] };
  }, [st.data]);

  if (st.error) return <p className="muted small">Could not load the GC log: {st.error.message}</p>;
  if (st.data === undefined) return <p className="muted small">Loading garbage-collection log…</p>;
  if (st.data === null)
    return <p className="muted small">GC pause details need a newer analyzer (the gc_events dataset). Re-analyze this cluster to fill them in.</p>;
  if (model.rows.length === 0)
    return <p className="muted small">No JVM garbage-collection log lines were found for this {r.executor_id === 'driver' ? 'driver' : 'executor'}.</p>;

  const fullShare = model.maxTotal ? model.maxAfter / model.maxTotal : null;
  const xAxis = (
    <XAxis
      dataKey="ts"
      type="number"
      domain={model.domain}
      scale="time"
      tickFormatter={(v: number) => fmtTime(v).slice(0, 5)}
      tickLine={false}
      axisLine={{ stroke: 'var(--axis)' }}
      minTickGap={28}
    />
  );
  return (
    <div className="gc-block">
      <h3>Garbage collection</h3>
      <p className="ink2">
        {fmtNum(model.young.length + model.full.length)} pauses totalling {fmtDuration(model.total)}
        {model.full.length ? (
          <>
            , including <b className={model.full.length >= FULL_GC_MIN ? 'bad' : ''}>{fmtNum(model.full.length)} full GCs</b>
          </>
        ) : null}
        .{model.longest ? ` Longest pause ${fmtDuration(model.longest.pause_ms)} at ${fmtTime(model.longest.ts)} (${model.longest.kind ?? 'GC'}${model.longest.cause ? `, ${model.longest.cause}` : ''}).` : ''}
        {model.maxAfter ? ` Heap still in use after GC peaked at ${fmtNum(model.maxAfter)} MiB${model.maxTotal ? ` of ${fmtNum(model.maxTotal)} MiB (${fmtPct(fullShare, 0)})` : ''}.` : ''}
        {fullShare !== null && fullShare >= 0.8 ? ' Memory that GC cannot free is close to the heap size: give the executor more memory or process less data per task.' : ''}
      </p>
      <div className="grid-2">
        <div>
          <div className="chart-title">Heap in use after each GC (MB)</div>
          <div className="chart-box" style={{ height: 200 }}>
            <ResponsiveContainer>
              <LineChart data={model.heap} margin={{ top: 8, right: 12, bottom: 0, left: -6 }}>
                <CartesianGrid vertical={false} />
                {xAxis}
                <YAxis tickLine={false} axisLine={false} width={54} tickFormatter={(v: number) => fmtNum(v)} />
                <Tooltip {...tooltipStyle} labelFormatter={(v) => `${fmtTs(Number(v), true)} UTC`} formatter={(v: number, n: string) => [`${fmtNum(v)} MiB`, n]} />
                <Legend wrapperStyle={{ fontSize: 12.5, color: 'var(--ink-2)' }} iconSize={10} />
                <Line name="Heap size" dataKey="total" stroke="var(--axis)" strokeDasharray="4 3" strokeWidth={1.5} dot={false} isAnimationActive={false} />
                <Line name="In use after GC" dataKey="after" stroke="var(--series-1)" strokeWidth={2} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div>
          <div className="chart-title">Pause per GC (ms)</div>
          <div className="chart-box" style={{ height: 200 }}>
            <ResponsiveContainer>
              <ScatterChart margin={{ top: 8, right: 12, bottom: 0, left: -6 }}>
                <CartesianGrid vertical={false} />
                {xAxis}
                <YAxis dataKey="pause" type="number" tickLine={false} axisLine={false} width={54} tickFormatter={(v: number) => fmtNum(v)} />
                <ZAxis range={[36, 36]} />
                <Tooltip
                  {...tooltipStyle}
                  cursor={{ stroke: 'var(--axis)', strokeDasharray: '3 3' }}
                  formatter={(v: number, n: string) => (n === 'ts' ? [`${fmtTs(v, true)} UTC`, 'Time'] : [`${fmtNum(v, 1)} ms`, 'Pause'])}
                />
                <Legend wrapperStyle={{ fontSize: 12.5, color: 'var(--ink-2)' }} iconSize={10} />
                <Scatter name="Young / mixed" data={model.young} fill="var(--series-1)" stroke="var(--chart-surface)" strokeWidth={1} isAnimationActive={false} />
                <Scatter name="Full GC" data={model.full} fill="var(--series-2)" stroke="var(--chart-surface)" strokeWidth={1} shape="diamond" isAnimationActive={false} />
              </ScatterChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
      <DataLink cid={cid} dataset="gc_events" q={r.executor_id === 'driver' ? null : null} label="All GC events ↗" />
    </div>
  );
}

/** executor id -> the incident problems it took part in (one per problem, root causes first). */
function problemsByExecutor(rows: IncidentRow[], ctx: string | null): Map<string, IncidentRow[]> {
  return indexProblems(rows.filter((x) => !ctx || !x.spark_context_id || x.spark_context_id === ctx), (x) => (x.executors ?? '').split(', ').filter(Boolean));
}


/** An executor's problems as a few pills: one per kind with a count, root causes first, at most three, then "+N" (all
 * of them on hover). An executor lost, out of memory or killed is shown only on the executor it is about. */
const GONE_KINDS = new Set(['out of memory', 'executor lost', 'executor killed by the OS']);
function ExecPills({ cid, exec, rows }: { cid: string; exec: string; rows: IncidentRow[] }) {
  const mine = rows.filter((x) => !GONE_KINDS.has(x.kind) || (x.executor_id != null ? x.executor_id === exec : (x.executors ?? '').split(/[,\s]+/).includes(exec)));
  const by = new Map<string, IncidentRow[]>();
  for (const x of mine) by.set(x.kind, [...(by.get(x.kind) ?? []), x]);
  const groups = [...by.values()].sort((a, b) => Number(b.some((x) => x.role === 'root')) - Number(a.some((x) => x.role === 'root')));
  const shown = groups.slice(0, 3);
  return (
    <>
      {shown.map((g) => {
        const x = g.find((y) => y.role === 'root') ?? g[0];
        return (
          <Link key={x.kind} className={`stage-prob sev-${x.incident_severity} ${x.role === 'root' ? 'root' : ''}`} to={to.findings(cid, x.finding_id)}
            onClick={(e) => e.stopPropagation()} title={g.map((y) => `${y.incident_id}: ${y.incident_title}`).join('\n')}>
            {x.role === 'root' ? '● ' : ''}{x.kind}{g.length > 1 ? ` ×${g.length}` : ''}
          </Link>
        );
      })}
      {groups.length > 3 && <span className="muted small" title={groups.slice(3).map((g) => `${g[0].kind}${g.length > 1 ? ` ×${g.length}` : ''}`).join('\n')}>+{groups.length - 3}</span>}
    </>
  );
}
