import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import { hasLogic, QueryLogic } from '../components/QueryLogic';
import { Link } from 'react-router-dom';
import { api, isMissing, type AppRow, type ConnectOperationRow, type Flow, type FlowEdge, type FlowNode, type Graph, type IncidentRow, type TaskRetryRow } from '../api';
import type { FlowAround, FlowCtx } from '../graph/layout';
import { to } from '../links';
import { indexProblems } from '../problems';
import { ExecutorsAtTime } from '../components/ExecutorsAtTime';
import { QueryPlanTab } from './QueryDetail';
import { TraceWaterfall } from '../components/TraceWaterfall';
import { StagePanel } from './Stages';
import { useCluster } from '../components/Shell';
import { DataLink, Empty, ErrorState, Loading } from '../components/ui';
import { fmtBytes, fmtDuration, fmtNum, fmtRows, fmtTime, truncate, unitFor } from '../format';
import { dataSpread, Spread } from '../components/Spread';
import { skewBreach } from '../thresholds';
import { GraphCanvas, type Mode } from '../graph/GraphCanvas';
import {
  buildModel,
  defaultUnit,
  deriveGraph,
  FILTERS,
  focusModel,
  METRIC_META,
  resolveUnit,
  STATUS_META,
  unitMatches,
  workUnits,
  type FilterKey,
  type GModel,
  type GNode,
  type WorkUnit,
} from '../graph/model';
import { NodePanel } from '../graph/NodePanel';
import { JobSummary, QuerySummary, QueryTasks } from '../components/LevelSummary';
import { useAsync, useQueryState } from '../hooks';

async function loadGraph(cid: string, ctx: string | null, s: AbortSignal): Promise<Graph | null> {
  try {
    return await api.graph(cid, ctx, s);
  } catch (e) {
    if (!isMissing(e)) throw e;
    // older analyzer: rebuild the same shape from /hierarchy (no stage dependencies)
    const [h, ops] = await Promise.all([
      api.hierarchy(cid, s),
      api.datasetOpt<ConnectOperationRow>(cid, 'connect_operations', { limit: 5000, sort: 'start_time' }, s).catch(() => null),
    ]);
    return deriveGraph(h, ops?.rows ?? [], ctx);
  }
}

function Legend({ m, mode }: { m: GModel; mode: Mode }) {
  return (
    <div className="legend graph-legend" aria-label="Legend">
      {(['ok', 'retried', 'failed', 'incomplete'] as const).map((k) => (
        <span key={k} className="item">
          <span className="sw" style={{ background: STATUS_META[k].mark }} />
          <span aria-hidden style={{ color: STATUS_META[k].ink }}>
            {STATUS_META[k].glyph}
          </span>
          {STATUS_META[k].label}
        </span>
      ))}
      {mode === 'flow' ? (
        <>
          <span className="item">
            <span className="sw" style={{ background: METRIC_META.disk_spill.color }} />
            <span className="sw" style={{ background: METRIC_META.mem_spill.color, marginLeft: -3 }} />
            Spill: disk, memory
          </span>
          <span className="item">
            <span className="sw" style={{ background: METRIC_META.shuffle_read.color }} />
            <span className="sw" style={{ background: METRIC_META.shuffle_write.color, marginLeft: -3 }} />
            Shuffle: read, write
          </span>
          {m.hasDepends && (
            <span className="item">
              <svg width="26" height="8" aria-hidden>
                <path d="M1 4 H24" className="gedge depends" />
              </svg>
              Reads output of
            </span>
          )}
        </>
      ) : (
        <>
          <span className="item">
            <span className="sw" style={{ background: METRIC_META.disk_spill.color, height: 3 }} />
            Underline: stage spilled
          </span>
          <span className="item">
            <span className="sw" style={{ background: METRIC_META.shuffle_read.color, height: 3 }} />
            Overline: shuffle-heavy
          </span>
        </>
      )}
      <span className="item">
        <svg width="26" height="8" aria-hidden>
          <path d="M1 4 H24" className="gedge retry" />
        </svg>
        Retry of a failed attempt
      </span>
    </div>
  );
}

const KIND_LABEL: Record<WorkUnit['kind'], string> = { query: 'SQL', connect: 'Connect', job: 'job, no SQL', orphans: 'stages' };

/**
 * The line of user code in a recorded call site. Spark records the whole stack ("saveAsTable at
 * DataFrameWriter.scala:731 sun.reflect... py4j... /Workspace/x/main.py:18"): framework frames are noise, and Spark
 * Connect records "<unknown>:0", which says nothing.
 */
export function userCallSite(cs: string | null | undefined): string | null {
  if (!cs) return null;
  const parts = cs.split(/\s*\n\s*|\s+(?=(?:\/|[\w$.]+\())/).map((x) => x.trim()).filter(Boolean);
  const framework = /^(org\.apache\.|sun\.|java\.|scala\.|py4j\.|jdk\.|com\.databricks\.)/;
  const user = parts.find((x) => /\/(Workspace|Repos|Users)\//.test(x) || /\.py:\d+/.test(x)) ?? parts.find((x) => !framework.test(x));
  const out = (user ?? parts[0] ?? '').replace(/\s*at <unknown>:0\b/, '').replace(/^<unknown>:0$/, '');
  return out || null;
}

/** Left rail: every SQL query (with its jobs) and every job that ran without SQL, in run order. */
function UnitList({ units, current, filters, onPick }: { units: WorkUnit[]; current: WorkUnit | null; filters: FilterKey[]; onPick: (id: string) => void }) {
  const [q, setQ] = useState('');
  const [showEmpty, setShowEmpty] = useState(false);
  // Revision 15: which query or job had the most tasks (or took longest), not only the order they ran in
  const [order, setOrder] = useState<'start' | 'tasks' | 'time'>('start');
  const tasksOf = (u: WorkUnit) => u.stages.reduce((a, st) => a + (st.metrics?.tasks ?? 0), 0);
  const needle = q.trim().toLowerCase();
  const hasWork = (u: WorkUnit) => u.jobs.length > 0 || u.kind === 'orphans';
  const empty = units.filter((u) => !hasWork(u)).length;
  const shown = units.filter(
    (u) =>
      (showEmpty || hasWork(u)) &&
      unitMatches(u, filters) &&
      (!needle || `${u.title} ${u.text} ${u.jobs.map((j) => j.label).join(' ')}`.toLowerCase().includes(needle)),
  );
  if (order === 'tasks') shown.sort((a, b) => tasksOf(b) - tasksOf(a));
  else if (order === 'time') shown.sort((a, b) => (b.duration_ms ?? 0) - (a.duration_ms ?? 0));
  // One shared scale for every row, so the little bars show the order things ran in and what overlapped.
  const t0 = Math.min(...units.map((u) => u.start ?? Infinity));
  const t1 = Math.max(...units.map((u) => u.end ?? u.start ?? -Infinity));
  const span = Number.isFinite(t0) && Number.isFinite(t1) && t1 > t0 ? t1 - t0 : null;
  return (
    <div className="panel unit-list">
      <div className="unit-search">
        <input className="input" placeholder="Search queries and jobs" value={q} onChange={(e) => setQ(e.target.value)} aria-label="Search queries and jobs" />
        <span className="small muted">
          {fmtNum(shown.length)} of {fmtNum(units.length)}
        </span>
      </div>
      <div className="seg small" role="group" aria-label="Order" style={{ margin: '8px 10px' }}>
        {([['start', 'In order'], ['tasks', 'Most tasks'], ['time', 'Longest']] as const).map(([k, label]) => (
          <button key={k} className={order === k ? 'on' : ''} aria-pressed={order === k} onClick={() => setOrder(k)}>{label}</button>
        ))}
      </div>
      <ul>
        {shown.map((u) => {
          const inside = current?.kind === 'job' && u.kind !== 'job' && u.jobs.some((j) => j.id === current.id);
          const on = current?.id === u.id || inside;
          return (
            <li key={u.id} className={on ? 'on' : ''}>
              <button className="unit-main" onClick={() => onPick(u.id)} aria-current={on ? 'true' : undefined}>
                <span className="nl-glyph" style={{ color: STATUS_META[u.vstatus].ink }} aria-hidden>
                  {STATUS_META[u.vstatus].glyph}
                </span>
                <span className="unit-body">
                  <span className="unit-title">
                    {u.title}
                    <span className="muted small">
                      {' '}
                      · {KIND_LABEL[u.kind]}
                      {u.kind !== 'job' && u.jobs.length ? ` · ${u.jobs.length} ${u.jobs.length === 1 ? 'job' : 'jobs'}` : ''}
                      {tasksOf(u) ? ` · ${fmtNum(tasksOf(u))} ${tasksOf(u) === 1 ? 'task' : 'tasks'}` : ''}
                    </span>
                  </span>
                  <span className="unit-text">{truncate(u.text, 90) || <i className="muted">no description</i>}</span>
                  {u.start !== null && (
                    <span className="unit-when">
                      {fmtTime(u.start)} → {u.end !== null ? fmtTime(u.end) : 'not finished'}
                    </span>
                  )}
                  {span && u.start !== null && (
                    <span className="unit-span" title={`${fmtTime(u.start)} → ${fmtTime(u.end)} of the app's ${fmtTime(t0)} → ${fmtTime(t1)}`} aria-hidden>
                      <span
                        style={{
                          left: `${((u.start - t0) / span) * 100}%`,
                          width: `max(3px, ${(((u.end ?? t1) - u.start) / span) * 100}%)`,
                          background: STATUS_META[u.vstatus].ink,
                        }}
                      />
                    </span>
                  )}
                </span>
                <span className="nl-dur">{fmtDuration(u.duration_ms)}</span>
              </button>
              {u.kind !== 'job' && u.jobs.length > 1 && on && (
                <div className="unit-jobs">
                  {u.jobs.map((j) => (
                    <button key={j.id} className={`tchip ${current?.id === j.id ? 'on' : ''}`} onClick={() => onPick(j.id)} title={`Show only ${j.label}`}>
                      <span style={{ color: STATUS_META[j.vstatus].ink }} aria-hidden>
                        {STATUS_META[j.vstatus].glyph}
                      </span>{' '}
                      {j.label}
                    </button>
                  ))}
                </div>
              )}
            </li>
          );
        })}
        {!shown.length && (
          <li className="muted small" style={{ padding: 12 }}>
            Nothing matches.
          </li>
        )}
      </ul>
      {empty > 0 && (
        <button className="btn small ghost" style={{ margin: 8 }} onClick={() => setShowEmpty(!showEmpty)}>
          {showEmpty ? 'Hide' : 'Show'} {fmtNum(empty)} {empty === 1 ? 'query' : 'queries'} that ran no Spark job
        </button>
      )}
    </div>
  );
}

/** Every stage of the focused query or job, one row each, grouped by job: the numbers behind the graph. */
const sum = (a: number | null, b: number | null) => (a === null && b === null ? null : (a ?? 0) + (b ?? 0));

/** Data in or out of a stage: total size, rows under it, and how much of it went through the shuffle. */
function IO({ bytes, rows, shuffle }: { bytes: number | null; rows: number | null; shuffle: number | null }) {
  if (!bytes && !rows) return <span className="muted">–</span>;
  const viaShuffle = bytes && shuffle ? shuffle / bytes : 0;
  return (
    <>
      {bytes ? fmtBytes(bytes) : <span className="muted">0 B</span>}
      <div className="muted small nowrap">
        {rows ? `${fmtRows(rows)} rows` : 'no rows'}
        {viaShuffle >= 0.5 ? ' · shuffle' : ''}
      </div>
    </>
  );
}

function StageTable({ cid, u, selected, onPick }: { cid: string; u: WorkUnit; selected: string | null; onPick: (id: string) => void }) {
  // the problems found on each stage attempt (one per incident problem), linked to their incident on Findings
  const inc = useAsync((s) => api.datasetOpt<IncidentRow>(cid, 'incidents', { limit: 5000 }, s), [cid]);
  const probs = useMemo(
    () => indexProblems(inc.data?.rows ?? [], (r) => (r.stages ? r.stages.split(', ').map((sa) => `${r.spark_context_id ?? '*'}:${sa}`) : [])),
    [inc.data],
  );
  const probsAt = (ctx: string, sa: string) => [...(probs.get(`${ctx}:${sa}`) ?? []), ...(probs.get(`*:${sa}`) ?? [])];
  const groups = u.jobs.length
    ? u.jobs.map((j) => ({ job: j as GNode | null, stages: u.stages.filter((s) => s.parent === j.id) }))
    : [{ job: null as GNode | null, stages: u.stages }];
  const m = (s: GNode, k: keyof NonNullable<GNode['metrics']>) => s.metrics?.[k] ?? null;
  const bytes = (v: number | null) => (v ? fmtBytes(v) : <span className="muted">–</span>);
  return (
    <div className="panel stage-table">
      <h3>Stages in {u.title}</h3>
      {groups.map(({ job, stages }) => (
        <div key={job?.id ?? 'orphans'} className="stage-group">
          {job && u.jobs.length > 0 && (
            <div className="stage-group-head">
              <span style={{ color: STATUS_META[job.vstatus].ink }} aria-hidden>
                {STATUS_META[job.vstatus].glyph}
              </span>{' '}
              <b>{job.label}</b>
              {job.sublabel ? <span className="muted"> · {truncate(job.sublabel, 60)}</span> : null}
              <span className="muted small">
                {' '}
                · {fmtTime(job.start)} → {fmtTime(job.end)} · {fmtDuration(job.duration_ms)} · {fmtNum(stages.length)} stage {stages.length === 1 ? 'attempt' : 'attempts'}
              </span>
            </div>
          )}
          <div className="table-scroll">
            <table className="exec-table">
              <thead>
                <tr>
                  <th>Stage</th>
                  <th>What it ran</th>
                  <th>When</th>
                  <th className="num">Took</th>
                  <th className="num">Tasks</th>
                  <th className="num" title="Data the stage read: from storage (files, tables) plus from the shuffle of earlier stages">
                    Read
                    <div className="th-sub">data · rows</div>
                  </th>
                  <th className="num" title="Data the stage wrote: to storage plus to the shuffle for the next stage">
                    Wrote
                    <div className="th-sub">data · rows</div>
                  </th>
                  <th className="num" title="How long each task took. Skew = slowest ÷ median task">
                    Time per task
                    <div className="th-sub">min · med · max</div>
                  </th>
                  <th className="num" title="How much data each task read. Skew = biggest ÷ median task: a hot key or partition">
                    Data per task
                    <div className="th-sub">min · med · max</div>
                  </th>
                  <th className="num" title="Spilled to disk (memory spill on hover)">Spill</th>
                  <th className="num">GC</th>
                </tr>
              </thead>
              <tbody>
                {stages.map((s) => {
                  const failed = m(s, 'failed_tasks') ?? 0;
                  const skew = m(s, 'skew');
                  const gc = m(s, 'gc_share');
                  return (
                    <tr
                      key={s.id}
                      className={`${s.vstatus === 'failed' ? 'row-bad' : ''} ${selected === s.id ? 'row-on' : ''}`}
                      onClick={() => onPick(s.id)}
                      style={{ cursor: 'pointer' }}
                    >
                      <td>
                        <span style={{ color: STATUS_META[s.vstatus].ink }} aria-hidden>
                          {STATUS_META[s.vstatus].glyph}
                        </span>{' '}
                        <b>{s.label}</b>
                        {s.attempt > 0 && <span className="muted small"> attempt {s.attempt + 1}</span>}
                        {s.from_stages?.length ? (
                          <div className="muted small" title="Rows each parent stage wrote to the shuffle, which this stage read">
                            ← {s.from_stages.map((p) => `stage ${p.stage_id}${p.rows != null ? `: ${fmtRows(p.rows)} rows` : ''}${p.reused ? ' (reused)' : ''}`).join(' · ')}
                            {s.from_stages.some((p) => p.rows == null) && m(s, 'shuffle_read_records') != null ? ` · read ${fmtRows(m(s, 'shuffle_read_records'))} rows from the shuffle` : ''}
                          </div>
                        ) : null}
                        {probsAt(s.ctx, `${s.stageId}.${s.attempt}`).map((r) => (
                          <Link
                            key={r.problem_id}
                            to={to.findings(cid, r.finding_id)}
                            onClick={(e) => e.stopPropagation()}
                            className={`stage-prob sev-${r.incident_severity} ${r.role === 'root' ? 'root' : ''}`}
                            title={`${r.incident_id}: ${r.incident_title}${r.role === 'root' ? ' (root cause here)' : ''}`}
                          >
                            {r.role === 'root' ? '● ' : ''}
                            {r.kind}
                          </Link>
                        ))}
                      </td>
                      <td className="muted" title={s.what?.call_site ?? s.sublabel ?? ''}>
                        {truncate(s.sublabel ?? s.what?.description ?? s.what?.call_site ?? "", 48) || "–"}
                      </td>
                      <td className="mono nowrap small">
                        {fmtTime(s.start)} → {fmtTime(s.end)}
                      </td>
                      <td className="num">{fmtDuration(s.duration_ms)}</td>
                      <td className="num">
                        {fmtNum(m(s, 'tasks'))}
                        {failed > 0 && <div className="st-crit small">{fmtNum(failed)} failed</div>}
                      </td>
                      <td className="num" title={`From storage: ${fmtBytes(m(s, 'input_bytes'))}, ${fmtRows(m(s, 'input_records'))} rows
From shuffle: ${fmtBytes(m(s, 'shuffle_read'))}, ${fmtRows(m(s, 'shuffle_read_records'))} rows`}>
                        <IO bytes={sum(m(s, 'input_bytes'), m(s, 'shuffle_read'))} rows={sum(m(s, 'input_records'), m(s, 'shuffle_read_records'))} shuffle={m(s, 'shuffle_read')} />
                      </td>
                      <td className="num" title={`To storage: ${fmtBytes(m(s, 'output_bytes'))}, ${fmtRows(m(s, 'output_records'))} rows
To shuffle: ${fmtBytes(m(s, 'shuffle_write'))}, ${fmtRows(m(s, 'shuffle_write_records'))} rows`}>
                        <IO bytes={sum(m(s, 'output_bytes'), m(s, 'shuffle_write'))} rows={sum(m(s, 'output_records'), m(s, 'shuffle_write_records'))} shuffle={m(s, 'shuffle_write')} />
                      </td>
                      <td className="num">
                        <Spread lo={m(s, 'min_task_ms')} p10={m(s, 'p10_task_ms')} mid={m(s, 'p50_task_ms')} p90={m(s, 'p90_task_ms')} hi={m(s, 'max_task_ms')} ratio={skew} unit="ms" badAt={skewBreach(skew, m(s, 'max_task_ms'))} />
                      </td>
                      <td className="num">
                        <Spread {...dataSpread(s.metrics ?? {})} ratio={m(s, 'data_skew')} what="task (storage + shuffle input)" />
                      </td>
                      <td className="num" title={`Memory spill ${fmtBytes(m(s, 'mem_spill'))}`}>{bytes(m(s, 'disk_spill'))}</td>
                      <td className={`num ${gc !== null && gc >= 0.1 ? 'st-warn' : ''}`}>{gc !== null ? `${(gc * 100).toFixed(0)}%` : '–'}</td>
                    </tr>
                  );
                })}
                {!stages.length && (
                  <tr>
                    <td colSpan={11} className="muted">
                      This job ran no stages.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      ))}
      <p className="muted small">Click a row for its tasks, executors and why it was slow.</p>
    </div>
  );
}

/** What the focused query or job does, in plain words, above its graph. */
function UnitHeader({ u, cid }: { u: WorkUnit; cid: string }) {
  // a query: what it joins on, keeps and filters, from its plan (in place of the operator chain)
  const qk = u.kind === 'query' ? u.id.match(/^query:(.+):(\d+)$/) : null;
  const lg = useAsync((s) => (qk ? api.queryLogic(cid, qk[1], Number(qk[2]), s) : Promise.resolve(null)), [cid, u.id]);
  const w = { ...(u.jobs[0]?.what ?? {}), ...Object.fromEntries(Object.entries(u.node?.what ?? {}).filter(([, v]) => v !== null && v !== undefined)) };
  const failed = u.stages.filter((s) => s.vstatus === 'failed').length;
  const rerun = u.stages.filter((s) => s.attempt > 0).length;
  const sum = (k: 'disk_spill' | 'shuffle_read' | 'shuffle_write') => u.stages.reduce((a, s) => a + (s.metrics?.[k] ?? 0), 0);
  const ops = w.operators ?? [];
  const facts: [string, string][] = [];
  if (ops.length && !qk) facts.push(['Plan', `${ops.slice(0, 10).join(' → ')}${ops.length > 10 ? ' …' : ''}`]);
  // the tables it read: a small table of their own below, with what each read
  const reads = (w.tables_read ?? []) as string[];

  if (w.tables_written?.length) facts.push(['Writes', w.tables_written.join(', ')]);
  const code = userCallSite(w.call_site);
  if (code) facts.push(['Code', code]);
  if (w.notebook_path) facts.push(['Notebook', w.notebook_path]);
  return (
    <div className="panel unit-head">
      <div className="row" style={{ gap: 10, alignItems: 'baseline', flexWrap: 'wrap' }}>
        <h2 style={{ margin: 0 }}>{u.title}</h2>
        <span className="badge" style={{ color: STATUS_META[u.vstatus].ink, background: STATUS_META[u.vstatus].tint }}>
          {STATUS_META[u.vstatus].glyph} {STATUS_META[u.vstatus].label}
        </span>
        <span className="small ink2">
          {fmtDuration(u.duration_ms)} · {fmtNum(u.jobs.length)} {u.jobs.length === 1 ? 'job' : 'jobs'} · {fmtNum(u.stages.length)} stage attempts
          {failed ? (
            <>
              , <b className="bad">{fmtNum(failed)} failed</b>
            </>
          ) : null}
          {rerun ? <>, {fmtNum(rerun)} re-run</> : null}
          {sum('disk_spill') ? <>, spilled {fmtBytes(sum('disk_spill'))}</> : null}
          {sum('shuffle_read') + sum('shuffle_write') ? <>, shuffled {fmtBytes(sum('shuffle_read') + sum('shuffle_write'))}</> : null}
        </span>
        <span className="grow" />
      </div>
      {u.text && <p className="unit-desc wrap-any">{u.text}</p>}
      {facts.length > 0 && (
        <dl className="unit-facts">
          {facts.map(([k, v]) => (
            <div key={k}>
              <dt>{k}</dt>
              <dd className="mono small wrap-any">{v}</dd>
            </div>
          ))}
        </dl>
      )}
      {reads.length > 0 && <ReadsTable tables={reads} stages={u.stages} />}
      {hasLogic(lg.data) && <div className="unit-logic"><QueryLogic l={lg.data} compact /></div>}
    </div>
  );
}

const SCAN_RE = /^Scan (?:parquet|delta|orc|csv|json|text|avro) (\S+)/;
const BIG_READ = 20 * 1024 ** 3;

/** The tables a query read, one row each with what its scans read from storage, the biggest first. Built for many:
 * the top 8 show, the rest unfold, and a filter appears from 20 tables on (a join of a hundred tables still fits). */
function ReadsTable({ tables, stages }: { tables: string[]; stages: GNode[] }) {
  const [all, setAll] = useState(false);
  const [q, setQ] = useState('');
  const rows = useMemo(() => {
    const scansOf = (st: GNode) => ((st.what?.rdd_scopes as string[] | null | undefined) ?? []).map((x) => String(x).match(SCAN_RE)?.[1]).filter((x): x is string => !!x);
    const by = new Map<string, { bytes: number; stages: number; shared: boolean }>(tables.map((t) => [t, { bytes: 0, stages: 0, shared: false }]));
    for (const st of stages) {
      const sc = [...new Set(scansOf(st))].filter((t) => by.has(t));
      for (const t of sc) {
        const r = by.get(t)!;
        // a stage that scans several tables: its bytes cannot be split, each gets them and is marked shared
        r.bytes += st.metrics?.input_bytes ?? 0;
        r.stages += 1;
        r.shared ||= sc.length > 1;
      }
    }
    return tables.map((t) => ({ t, ...by.get(t)! })).sort((a, b) => b.bytes - a.bytes || a.t.localeCompare(b.t));
  }, [tables, stages]);
  // one definition of "read": files in storage, a DataFrame cache (InMemoryTableScan, whose input is cached batches),
  // and shuffle
  const src = useMemo(() => {
    let files = 0, cache = 0, shuffle = 0;
    for (const st of stages) {
      const cached = ((st.what?.rdd_scopes as string[] | null | undefined) ?? []).some((x) => String(x).startsWith('InMemoryTableScan'));
      if (cached) cache += st.metrics?.input_bytes ?? 0; else files += st.metrics?.input_bytes ?? 0;
      shuffle += st.metrics?.shuffle_read ?? 0;
    }
    return { files, cache, shuffle };
  }, [stages]);
  const u = unitFor('b', rows.map((r) => r.bytes));
  const su = unitFor('b', [src.files, src.cache, src.shuffle]);
  const hit = q ? rows.filter((r) => r.t.toLowerCase().includes(q.toLowerCase())) : rows;
  const shown = all || q ? hit : hit.slice(0, 8);
  return (
    <div className="reads-table">
      <div className="row small" style={{ gap: 8, alignItems: 'center' }}>
        <span className="muted">Reads {fmtNum(tables.length)} {tables.length === 1 ? 'table' : 'tables'}</span>
        {src.cache > 0 || src.shuffle > 0 ? (
          <span className="muted" title="Files: read from storage. DataFrame cache: read back from cache() or persist(), not from storage. Shuffle: data moved between stages.">
            · files {su.f(src.files)} {su.u}{src.cache > 0 ? ` · DataFrame cache ${su.f(src.cache)} ${su.u}` : ''}{src.shuffle > 0 ? ` · shuffle ${su.f(src.shuffle)} ${su.u}` : ''}
          </span>
        ) : null}
        {tables.length >= 20 && <input className="input small" placeholder="Filter tables" value={q} onChange={(e) => setQ(e.target.value)} aria-label="Filter the tables it read" />}
      </div>
      <div className="table-wrap" style={{ maxHeight: all ? 360 : undefined }}>
        <table className="table compact ts-table">
          <thead><tr><th>Table</th><th className="num" title="Read from storage by the stages that scanned it">Read<span className="unit"> ({u.u})</span></th><th className="num">Stages</th></tr></thead>
          <tbody>
            {shown.map((r) => (
              <tr key={r.t} className={r.bytes >= BIG_READ ? 'big-read' : ''}>
                <td className="mono small wrap-any">{r.t.startsWith('jdbc:') ? <>{r.t.slice(5)} <span className="muted">(database)</span></> : r.t}</td>
                <td className="num" title={r.shared ? 'Scanned together with other tables in the same stage: the bytes are the whole stage' : undefined}>{r.stages ? `${u.f(r.bytes)}${r.shared ? '*' : ''}` : '–'}</td>
                <td className="num">{r.stages || '–'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {!q && hit.length > 8 && <button className="btn small ghost" onClick={() => setAll((a) => !a)}>{all ? 'Show the top 8' : `${fmtNum(hit.length - 8)} more ↓`}</button>}
    </div>
  );
}

function initialSelection(sp: URLSearchParams, ctx: string): string | null {
  const node = sp.get('node');
  if (node) return node;
  const stage = sp.get('stage');
  if (stage) return `stage:${ctx}:${stage}:${sp.get('attempt') ?? 0}`;
  const job = sp.get('job');
  if (job) return `job:${ctx}:${job}`;
  return null;
}

export default function HierarchyPage() {
  const { cid } = useCluster();
  const [sp, setQ] = useQueryState();
  const apps = useAsync((s) => api.dataset<AppRow>(cid, 'apps', { limit: 200, sort: 'start_time' }, s), [cid]);
  const ctx = sp.get('ctx') ?? apps.data?.rows[0]?.spark_context_id ?? null;
  const ready = !!apps.data || !!apps.error;
  const g = useAsync((s) => loadGraph(cid, ctx, s), [cid, ctx], ready);
  const full = useMemo(() => (g.data ? buildModel(g.data) : null), [g.data]);
  const units = useMemo(() => (full ? workUnits(full) : []), [full]);
  const mode: Mode = sp.get('mode') === 'time' ? 'time' : 'flow';
  const fParam = sp.get('f') ?? '';
  const filters = useMemo(() => fParam.split(',').filter((f): f is FilterKey => FILTERS.some((x) => x.key === f)), [fParam]);
  const [selected, setSelected] = useState<string | null>(null);
  const [focusTick, setFocusTick] = useState(0);
  const retries = useAsync((s) => api.datasetOpt<TaskRetryRow>(cid, 'task_retries', { limit: 5000 }, s), [cid]);

  // focused unit: ?unit=, else the unit holding a cross-linked job/stage, else the failed / retried / slowest one
  const linked = full ? initialSelection(sp, full.ctx) : null;
  const unitParam = sp.get('unit');
  const unit = useMemo(() => {
    if (!full) return null;
    return resolveUnit(full, units, unitParam) ?? resolveUnit(full, units, linked) ?? defaultUnit(units);
  }, [full, units, unitParam, linked]);
  const model = useMemo(() => (full && unit ? focusModel(full, unit) : null), [full, unit]);

  // selection from the URL (cross-links: ?job= / ?stage= / ?node=)
  useEffect(() => {
    if (!model) return;
    if (linked && model.byId.has(linked)) {
      setSelected(linked);
      setFocusTick((t) => t + 1);
    } else if (selected && !model.byId.has(selected)) setSelected(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model]);

  const pickUnit = useCallback(
    (id: string) => {
      setSelected(null);
      setQ({ unit: id, node: null, job: null, stage: null, attempt: null }, true);
    },
    [setQ],
  );
  const select = useCallback(
    (id: string | null, focus = false) => {
      // a node outside the focused unit (a link in the side panel): switch to the unit that holds it
      if (id && full && model && !model.byId.has(id)) {
        const u = resolveUnit(full, units, id);
        if (u) {
          setQ({ unit: u.id, node: id, job: null, stage: null, attempt: null }, true);
          setSelected(id);
          return;
        }
      }
      setSelected(id);
      setQ({ node: id, job: null, stage: null, attempt: null }, true);
      if (focus && id) setFocusTick((t) => t + 1);
    },
    [setQ, full, model, units],
  );
  useEffect(() => {
    const h = (e: Event) => select((e as CustomEvent<string>).detail, true);
    document.addEventListener('graph-select', h);
    return () => document.removeEventListener('graph-select', h);
  }, [select]);

  const byFilter = useMemo(
    () =>
      Object.fromEntries(FILTERS.map((f) => [f.key, units.filter((u) => (u.jobs.length || u.kind === 'orphans') && unitMatches(u, [f.key])).length])) as Record<FilterKey, number>,
    [units],
  );
  const node = model && selected ? model.byId.get(selected) ?? null : null;
  // Revision 15: what came before and after the focused query, and the queries at its level, from the run's flow
  const flowSt = useAsync((s) => api.flow(cid, s), [cid]);
  const nb = useMemo(() => (unit ? neighbours(flowSt.data ?? null, unit.id) : null), [flowSt.data, unit]);
  // each side of the query box folds behind a "+" on its edge: data links start open, "ran just before / after" folded.
  // The box itself folds to its header; the chain view shows only queries: every one linked up- and downstream.
  const [openSide, setOpenSide] = useState<{ before: boolean | null; after: boolean | null }>({ before: null, after: null });
  const [jobsFolded, setJobsFolded] = useState(false);
  // the query with its jobs opens first; the chain of queries shows one step up and down, a "+" on each side the rest
  const [chain, setChain] = useState(false);
  const [chainAll, setChainAll] = useState<{ before: boolean; after: boolean }>({ before: false, after: false });
  useEffect(() => {
    setOpenSide({ before: null, after: null });
    setJobsFolded(false);
    setChainAll({ before: false, after: false });
  }, [unit?.id]);
  const chainNb = useMemo(() => (chain && unit ? chainOf(flowSt.data ?? null, unit.id) : null), [chain, flowSt.data, unit]);
  const around = useMemo((): (FlowAround & { siblings: FlowCtx[]; parentLabel: string | null }) | null => {
    if (!nb) return null;
    if (chainNb) {
      const cside = (k: 'before' | 'after') => {
        const all = chainNb[k];
        const deeper = all.filter((c) => (c.depth ?? 1) > 1);
        if (!deeper.length) return { cards: all, fold: null };
        const open = chainAll[k];
        return { cards: open ? all : all.filter((c) => (c.depth ?? 1) <= 1), fold: { open, count: deeper.length, names: deeper.map((c) => c.label).join(', ') } };
      };
      const b = cside('before');
      const a = cside('after');
      return { ...nb, before: b.cards, after: a.cards, collapsed: true, fold: { before: b.fold, after: a.fold } };
    }
    const side = (k: 'before' | 'after') => {
      const all = [...nb[k], ...(k === 'before' ? nb.seqBefore : nb.seqAfter)];
      if (!all.length) return { cards: [], fold: null };
      const open = openSide[k] ?? nb[k].length > 0;
      return { cards: open ? all : [], fold: { open, count: all.length, names: all.map((c) => c.label).join(', ') } };
    };
    const b = side('before');
    const a = side('after');
    return { ...nb, before: b.cards, after: a.cards, fold: { before: b.fold, after: a.fold }, collapsed: jobsFolded };
  }, [nb, chainNb, chainAll, openSide, jobsFolded]);
  const onUnitOrFold = useCallback((u: string) => {
    if (u === 'fold:box') {
      if (chain) {
        setChain(false);
        setJobsFolded(false);
      } else setJobsFolded((f) => !f);
    } else if (u.startsWith('fold:')) {
      const k = u.slice(5) as 'before' | 'after';
      if (chain) setChainAll((o) => ({ ...o, [k]: !o[k] }));
      else setOpenSide((o) => ({ ...o, [k]: !(o[k] ?? (nb ? nb[k].length > 0 : false)) }));
    } else pickUnit(u);
  }, [nb, chain, pickUnit]);
  const isQuery = unit?.node?.type === 'query' && unit.node.execId !== null;
  const tabQ = sp.get('tab');
  const tab = isQuery && tabQ === 'plan' ? 'plan' : tabQ === 'trace' && unit?.kind !== 'orphans' ? 'trace' : 'stages';
  const stageIds = useMemo(() => new Set((unit?.stages ?? []).map((s) => s.stageId).filter((v): v is number => v !== null)), [unit]);
  const parent = unit?.kind === 'job' ? units.find((u) => u.kind !== 'job' && u.jobs.some((j) => j.id === unit.id)) : undefined;
  const counts = useMemo(() => {
    if (!full) return null;
    const q = units.filter((u) => u.kind === 'query' && u.jobs.length).length;
    const conn = units.filter((u) => u.kind === 'connect' && u.jobs.length).length;
    const solo = units.filter((u) => u.kind === 'job').length;
    return { jobs: full.jobs.length, q, conn, solo };
  }, [full, units]);

  // the level: query → its jobs, job → its stages, stage → its tasks. A query's summary (why it took that long) goes
  // above the graph, so it is the first thing seen; a job's or stage's below it, next to where it was clicked
  const levelOf = (): { level: ReactNode; queryLevel: boolean; qNodeId: number | null } => {
    if (!unit || !model) return { level: null, queryLevel: false, qNodeId: null };
    const qNode = unit.kind === 'query' ? unit.node : null;
    const stageNode = node?.type === 'stage' ? node : null;
    const jobNode = node?.type === 'job' ? node : stageNode ? model.byId.get(stageNode.parent ?? '') ?? null : unit.kind === 'job' ? unit.jobs[0] ?? null : null;
    const crumbs: { label: string; id: string | null }[] = [
      ...(qNode ? [{ label: qNode.label, id: null }] : []),
      ...(jobNode && (qNode || stageNode) ? [{ label: jobNode.label, id: jobNode.id }] : []),
      ...(stageNode ? [{ label: `${stageNode.label}${stageNode.attempt ? ` attempt ${stageNode.attempt + 1}` : ''}`, id: stageNode.id }] : []),
    ];
    const level = (
      <>
        {crumbs.length > 1 && (
          <nav className="crumbs small" aria-label="Level">
            {crumbs.map((c, i) => (
              <span key={i}>
                {i ? ' › ' : ''}
                {i < crumbs.length - 1 ? <a href="#" onClick={(e) => { e.preventDefault(); select(c.id); }}>{c.label}</a> : <b>{c.label}</b>}
              </span>
            ))}
          </nav>
        )}
        {stageNode && stageNode.stageId !== null ? (
          <StagePanel key={stageNode.id} cid={cid} ctx={model.ctx} stage={String(stageNode.stageId)} attempt={String(stageNode.attempt)}
            retries={retries.data?.rows ?? null} onClose={() => select(jobNode?.id ?? null)} />
        ) : jobNode && jobNode.jobId !== null && (node?.type === 'job' || unit.kind === 'job') ? (
          <JobSummary key={jobNode.id} cid={cid} ctx={model.ctx} jobId={jobNode.jobId}
            onStage={(sid, att) => select(`stage:${model.ctx}:${sid}:${att}`, true)} />
        ) : qNode && qNode.execId !== null ? (
          <QuerySummary key={qNode.id} cid={cid} ctx={model.ctx} id={qNode.execId} title={`${qNode.label}${unit.text ? ` · ${truncate(unit.text, 60)}` : ''}`}
            onJob={(jid) => select(`job:${model.ctx}:${jid}`, true)} onQuery={(qid) => pickUnit(`query:${model.ctx}:${qid}`)} tasks={false} />
        ) : (
          <StageTable cid={cid} u={unit} selected={null} onPick={(id) => select(id)} />
        )}
      </>
    );
    return { level, queryLevel: !stageNode && !(jobNode && jobNode.jobId !== null && (node?.type === 'job' || unit.kind === 'job')) && !!qNode && qNode.execId !== null, qNodeId: qNode?.execId ?? null };
                
  };
  const { level, queryLevel, qNodeId } = levelOf();

  return (
    <div className="page wide">
      <div className="page-head">
        <div />
        <div className="actions">
          {apps.data && apps.data.rows.length > 1 && (
            <label className="cluster-switch">
              <span>Spark context</span>
              <select className="select" value={ctx ?? ''} onChange={(e) => setQ({ ctx: e.target.value, unit: null, node: null, job: null, stage: null })}>
                {apps.data.rows.map((a) => (
                  <option key={a.spark_context_id} value={a.spark_context_id}>
                    {a.spark_context_id} {a.app_name ? `(${a.app_name})` : ''}
                  </option>
                ))}
              </select>
            </label>
          )}
          <DataLink cid={cid} dataset="stages" />
        </div>
      </div>

      {full?.derived && (
        <div className="callout" style={{ marginBottom: 12 }}>
          This cluster was analyzed by an older version, so stage dependencies are not known: stages are listed in id order inside each job. Re-analyze the cluster to draw how
          data flows between stages.
        </div>
      )}


      {!ready || (g.loading && !g.data) ? (
        <div className="panel">
          <Loading label="Loading the job graph…" />
        </div>
      ) : g.error ? (
        <div className="panel">
          <ErrorState error={g.error} onRetry={g.reload} />
        </div>
      ) : !full || !unit || !model ? (
        <div className="panel">
          <Empty title="No Spark jobs">No event logs were found for this cluster, so there are no queries or jobs to draw.</Empty>
        </div>
      ) : (
        <div className="focus-layout">
          <div className="focus-side">
            {counts && (
              <p className="small ink2" style={{ margin: '0 0 8px' }}>
                {(apps.data?.rows.length ?? 0) > 1 ? 'In this Spark context, ' : ''}
                {fmtNum(counts.jobs)} Spark {counts.jobs === 1 ? 'job' : 'jobs'} came from{' '}
                {[
                  counts.q ? `${fmtNum(counts.q)} SQL ${counts.q === 1 ? 'query' : 'queries'}` : null,
                  counts.conn ? `${fmtNum(counts.conn)} Spark Connect ${counts.conn === 1 ? 'statement' : 'statements'}` : null,
                  counts.solo ? `${fmtNum(counts.solo)} ${counts.solo === 1 ? 'job' : 'jobs'} without SQL` : null,
                ]
                  .filter(Boolean)
                  .join(', ')}
                .
              </p>
            )}
            <div className="toggle-chips" role="group" aria-label="Show only" style={{ marginBottom: 8 }}>
              {FILTERS.map((f) => {
                const on = filters.includes(f.key);
                return (
                  <button
                    key={f.key}
                    type="button"
                    className={`tchip ${on ? 'on' : ''}`}
                    aria-pressed={on}
                    disabled={!on && byFilter[f.key] === 0}
                    onClick={() => setQ({ f: (on ? filters.filter((x) => x !== f.key) : [...filters, f.key]).join(',') || null })}
                  >
                    {f.label}
                    <span className="muted">{fmtNum(byFilter[f.key])}</span>
                  </button>
                );
              })}
            </div>
            <UnitList units={units} current={unit} filters={filters} onPick={pickUnit} />
          </div>
          <div className="focus-main">
            <UnitHeader u={unit} cid={cid} />
            {unit.kind !== 'orphans' && (
              <div className="seg tabs" role="tablist" aria-label="Query views">
                {(
                  [
                    ['stages', 'Stages and executors'],
                    ['trace', 'Trace'],
                    ...(isQuery ? ([['plan', 'SQL plan']] as const) : []),
                  ] as const
                ).map(([k, label]) => (
                  <button key={k} role="tab" aria-selected={tab === k} className={tab === k ? 'on' : ''} onClick={() => setQ({ tab: k === 'stages' ? null : k }, true)}>
                    {label}
                  </button>
                ))}
              </div>
            )}
            {tab === 'plan' && unit.node?.execId !== null && unit.node ? (
              <QueryPlanTab cid={cid} ctx={model.ctx} id={String(unit.node.execId)} />
            ) : tab === 'trace' ? (
              <TraceWaterfall
                cid={cid}
                ctx={model.ctx}
                unit={unit}
                stagesByJob={model.stagesByJob}
                onStage={(id) => {
                  // one update: two setQ calls in a row would each start from the same params and undo each other
                  setSelected(id);
                  setQ({ tab: null, node: id, job: null, stage: null, attempt: null }, true);
                }}
              />
            ) : (
              <>
                {queryLevel && level}
                <div className="graph-bar">
                  <div className="seg" role="group" aria-label="Layout">
                    <button className={mode === 'flow' ? 'on' : ''} aria-pressed={mode === 'flow'} onClick={() => setQ({ mode: null })}>
                      Flow
                    </button>
                    <button className={mode === 'time' ? 'on' : ''} aria-pressed={mode === 'time'} onClick={() => setQ({ mode: 'time' })}>
                      Time
                    </button>
                  </div>
                  {mode === 'flow' && around && (
                    <div className="seg" role="group" aria-label="What to show around it">
                      <button className={!chain ? 'on' : ''} aria-pressed={!chain} onClick={() => setChain(false)}
                        title="This query with its jobs and stages, and what came just before and after it">
                        Query with its jobs
                      </button>
                      <button className={chain ? 'on' : ''} aria-pressed={chain} onClick={() => setChain(true)}
                        title="Only queries: every query linked to this one, upstream and downstream, without their jobs and stages">
                        Chain of queries
                      </button>
                    </div>
                  )}
                  {parent && (
                    <button className="btn small ghost" onClick={() => pickUnit(parent.id)}>
                      ← All {fmtNum(parent.jobs.length)} jobs of {parent.title}
                    </button>
                  )}
                </div>
                <div className={`graph-layout ${node && !['stage', 'job', 'query'].includes(node.type) ? 'with-panel' : ''}`}>
                  <div className="panel graph-panel">
                    {around && around.siblings.length > 0 && <SameLevel sib={around.siblings} parent={around.parentLabel} onPick={pickUnit} />}
                    <GraphCanvas key={unit.id} model={model} mode={mode} filters={[]} selected={selected} onSelect={(id) => select(id)} focusTick={focusTick}
                      around={around ?? undefined} onUnit={onUnitOrFold} />
                    <div className="graph-foot">
                      <Legend m={model} mode={mode} />
                      <span className="muted small">Click a stage for its tasks and executors. Drag to move, scroll to zoom.</span>
                    </div>
                  </div>
                  {node && !['stage', 'job', 'query'].includes(node.type) && <NodePanel key={node.id} cid={cid} m={full} n={node} onClose={() => select(null)} />}
                </div>
                {queryLevel && qNodeId !== null && <QueryTasks cid={cid} ctx={model.ctx} id={qNodeId} />}
                {!queryLevel && level}
                {(node?.type as string | undefined) !== 'stage' && <ExecutorsAtTime
                  cid={cid}
                  ctx={model.ctx}
                  stageIds={node?.type === 'stage' && node.stageId !== null ? new Set([node.stageId]) : stageIds}
                  start={node?.type === 'stage' ? node.start ?? unit.start : unit.start}
                  end={node?.type === 'stage' ? node.end ?? unit.end : unit.end}
                  title={node?.type === 'stage' ? `${node.label}${node.attempt ? ` attempt ${node.attempt + 1}` : ''}` : unit.title}
                  onStage={(sid, att) => select(`stage:${model.ctx}:${sid}:${att}`)}
                />}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ before / after / same level (Revision 15) */

const unitOfFlow = (k: string) => k.replace(/^q:/, 'query:').replace(/^j:/, 'job:');
const flowOfUnit = (u: string) => u.replace(/^query:/, 'q:').replace(/^job:/, 'j:');

function ctxCard(n: FlowNode, why: string, depth?: number, link?: string): FlowCtx {
  return { unit: unitOfFlow(n.key), label: n.kind === 'query' ? `Query ${n.id}` : `Job ${n.id}`, what: n.what ?? '', status: n.status, why, depth, link };
}

/** Why an edge links two queries, said from the side of the one we look from. */
const whyBefore = (e: FlowEdge) =>
  e.kind === 'inside' ? 'ran this query inside it' : e.kind === 'table' ? `wrote ${e.labels.join(', ') || 'a table'} it reads` : 'computed shuffle output it reuses';
const whyAfter = (e: FlowEdge) =>
  e.kind === 'inside' ? 'ran inside this query' : e.kind === 'table' ? `reads ${e.labels.join(', ') || 'a table'} it wrote` : 'reuses its shuffle output';

const CHAIN_MAX = 30;
const CHAIN_SEQ = 6;

/** The chain view: every query linked to this one, upstream and downstream (walking the links step by step), each
 * card pointing at the one a step closer. A side with no links shows the top-level queries that ran just before /
 * after it in the run's order instead. */
function chainOf(f: Flow | null, unitId: string) {
  if (!f) return null;
  const me = flowOfUnit(unitId);
  const by = new Map(f.nodes.map((n) => [n.key, n]));
  const n0 = by.get(me);
  if (!n0) return null;
  const walk = (dir: 'before' | 'after') => {
    const out: FlowCtx[] = [];
    const seen = new Set([me]);
    let front = [me];
    for (let depth = 1; front.length && out.length < CHAIN_MAX; depth++) {
      const next: string[] = [];
      for (const k of front) {
        for (const e of f.edges) {
          const other = dir === 'before' ? (e.to === k ? e.from : null) : e.from === k ? e.to : null;
          if (!other || seen.has(other) || !by.has(other) || out.length >= CHAIN_MAX) continue;
          seen.add(other);
          next.push(other);
          out.push(ctxCard(by.get(other)!, dir === 'before' ? whyBefore(e) : whyAfter(e), depth, k === me ? undefined : unitOfFlow(k)));
        }
      }
      front = next;
    }
    return out;
  };
  let before = walk('before');
  let after = walk('after');
  if (!before.length || !after.length) {
    const inner = new Set(f.edges.filter((e) => e.kind === 'inside').map((e) => e.to));
    const top = f.nodes.filter((n) => n.key !== me && !inner.has(n.key) && n.start !== null).sort((a, b) => (a.start ?? 0) - (b.start ?? 0));
    const seq = (xs: FlowNode[], why: string) => xs.map((n, i) => ctxCard(n, why, i + 1, i ? unitOfFlow(xs[i - 1].key) : undefined));
    if (!before.length) before = seq(top.filter((n) => (n.end ?? Infinity) <= (n0.start ?? 0) + 500).reverse().slice(0, CHAIN_SEQ), 'ran before it');
    if (!after.length) after = seq(top.filter((n) => (n.start ?? 0) >= (n0.end ?? Infinity) - 500).slice(0, CHAIN_SEQ), 'ran after it');
  }
  return { before, after };
}

/** Before: the query it ran inside, the ones whose tables or shuffle output it used. After: the ones that used its
 * tables or shuffle output, and the queries that ran inside it. Same level: the other queries inside its parent. */
function neighbours(f: Flow | null, unitId: string) {
  if (!f) return null;
  const me = flowOfUnit(unitId);
  const by = new Map(f.nodes.map((n) => [n.key, n]));
  if (!by.has(me)) return null;
  const before: FlowCtx[] = [];
  const after: FlowCtx[] = [];
  let parent: string | null = null;
  for (const e of f.edges) {
    if (e.to === me && by.has(e.from)) {
      if (e.kind === 'inside') parent = e.from;
      before.push(ctxCard(by.get(e.from)!, whyBefore(e)));
    } else if (e.from === me && by.has(e.to)) {
      after.push(ctxCard(by.get(e.to)!, whyAfter(e)));
    }
  }
  // nothing links it on a side: offer what ran just before / just after it (the run's order), folded by default
  const n0 = by.get(me)!;
  const desc = new Set(f.edges.filter((e) => e.kind === 'inside' && e.from === me).map((e) => e.to));
  const pool = f.nodes.filter((n) => n.key !== me && n.key !== parent && !desc.has(n.key) && n.start !== null);
  const seqBefore = before.length ? [] : pool.filter((n) => (n.end ?? Infinity) <= (n0.start ?? 0) + 500)
    .sort((a, b) => (b.end ?? 0) - (a.end ?? 0)).slice(0, 1).map((n) => ctxCard(n, 'ran just before it'));
  const seqAfter = after.length ? [] : pool.filter((n) => (n.start ?? 0) >= (n0.end ?? Infinity) - 500)
    .sort((a, b) => (a.start ?? 0) - (b.start ?? 0)).slice(0, 1).map((n) => ctxCard(n, 'ran just after it'));
  const siblings = parent
    ? f.edges.filter((e) => e.kind === 'inside' && e.from === parent && e.to !== me && by.has(e.to)).map((e) => by.get(e.to)!)
        .sort((a, b) => (a.start ?? 0) - (b.start ?? 0)).map((n) => ctxCard(n, 'same level'))
    : [];
  const cap = (xs: FlowCtx[]) => xs.slice(0, 8);
  return { before: cap(before), after: cap(after), seqBefore, seqAfter, siblings, parentLabel: parent ? by.get(parent)!.kind === 'query' ? `Query ${by.get(parent)!.id}` : `Job ${by.get(parent)!.id}` : null };
}

function SameLevel({ sib, parent, onPick }: { sib: FlowCtx[]; parent: string | null; onPick: (unit: string) => void }) {
  return (
    <details className="same-level small" style={{ padding: '6px 12px', borderBottom: '1px solid var(--border)' }}>
      <summary style={{ cursor: 'pointer' }}>
        {fmtNum(sib.length)} other {sib.length === 1 ? 'query' : 'queries'} at the same level{parent ? ` (also inside ${parent})` : ''}
      </summary>
      <div className="row" style={{ gap: 6, flexWrap: 'wrap', marginTop: 6 }}>
        {sib.map((c) => (
          <button key={c.unit} className="btn small" onClick={() => onPick(c.unit)} title={c.what}
            style={{ borderColor: c.status === 'failed' ? 'var(--st-crit)' : undefined }}>
            {c.label} <span className="muted">{truncate(c.what.replace(/\s+/g, ' '), 40)}</span>
          </button>
        ))}
      </div>
    </details>
  );
}
