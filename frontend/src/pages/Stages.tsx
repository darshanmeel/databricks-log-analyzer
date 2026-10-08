import { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, type Hotspot, type StageDetail, type StageRow, type TaskRetryRow, type TaskRow } from '../api';
import { HBars } from '../components/charts';
import { StageTaskPanels } from '../components/StageTaskTimeline';
import { StageSharing } from '../components/StageSharing';
import { StageTaskSections } from '../components/TaskSections';
import { Bars, SIGN, Split, VCards, type VCard } from '../components/VCards';
import { useCluster } from '../components/Shell';
import { Async, DataLink, Empty, ErrorState, Loading, Panel, SeverityBadge, StatusBadge } from '../components/ui';
import { fmtBytes, fmtDuration, fmtNum, fmtPct, fmtSkew, fmtTs, truncate } from '../format';
import { useAsync, useDebounced, useQueryState } from '../hooks';
import { to } from '../links';
import { RETRY_CATS, RETRY_META, retriesByStage, retrySentence, retryCat } from '../retries';
import { gcBreach, skewBreach, spillBreach, TH } from '../thresholds';

const TOP = 20;

interface Metric {
  key: string;
  label: string;
  value: (r: StageRow) => number;
  display: (r: StageRow) => string;
  flag: (r: StageRow) => boolean;
  explain: string;
}

const METRICS: Metric[] = [
  { key: 'duration_ms', label: 'Duration', value: (r) => r.duration_ms ?? 0, display: (r) => fmtDuration(r.duration_ms), flag: (r) => r.status === 'failed', explain: 'The slowest stages are where the run spent its time. Red names failed.' },
  { key: 'failed_tasks', label: 'Failed tasks', value: (r) => r.failed_tasks ?? 0, display: (r) => `${fmtNum(r.failed_tasks)} of ${fmtNum(r.tasks)}`, flag: (r) => (r.failed_tasks ?? 0) > 0, explain: 'Stages whose tasks failed at least once. Failed tasks are retried; the retries panel says why they failed.' },
  { key: 'skew', label: 'Time skew', value: (r) => r.skew ?? 0, display: (r) => fmtSkew(r.skew), flag: (r) => skewBreach(r.skew, r.max_task_ms), explain: `Slowest task divided by the median task. Red at ${TH.skewRatio}× or more when the slowest task takes at least ${fmtDuration(TH.skewMinTaskMs)}: one partition is much bigger than the rest.` },
  { key: 'data_skew', label: 'Data skew', value: (r) => r.data_skew ?? 0, display: (r) => fmtSkew(r.data_skew), flag: (r) => (r.data_skew ?? 0) >= TH.skewRatio, explain: `Biggest task's input (storage + shuffle) divided by the median task's. Red at ${TH.skewRatio}× or more: one key or partition holds far more data than the rest.` },
  { key: 'disk_spill', label: 'Disk spill', value: (r) => r.disk_spill ?? 0, display: (r) => fmtBytes(r.disk_spill), flag: (r) => spillBreach(r.disk_spill), explain: 'Data written to disk because it did not fit in memory. Red at 1 GB or more.' },
  { key: 'gc_share', label: 'GC share', value: (r) => r.gc_share ?? 0, display: (r) => fmtPct(r.gc_share), flag: (r) => gcBreach(r.gc_share), explain: 'Share of task time spent in JVM garbage collection. Red at 20% or more.' },
  { key: 'mem_spill', label: 'Memory spill', value: (r) => r.mem_spill ?? 0, display: (r) => fmtBytes(r.mem_spill), flag: () => false, explain: 'Data Spark had to move out of execution memory (serialized, before it went to disk). Large values mean tasks needed more memory than they had.' },
  { key: 'shuffle_read', label: 'Shuffle read', value: (r) => r.shuffle_read ?? 0, display: (r) => fmtBytes(r.shuffle_read), flag: (r) => (r.shuffle_read ?? 0) >= TH.shuffleHeavyBytes, explain: 'Data read from other executors. Large shuffles are slow and fragile.' },
  { key: 'shuffle_write', label: 'Shuffle write', value: (r) => r.shuffle_write ?? 0, display: (r) => fmtBytes(r.shuffle_write), flag: (r) => (r.shuffle_write ?? 0) >= TH.shuffleHeavyBytes, explain: 'Data this stage wrote for the next stage to read (the map side of a shuffle: joins, aggregations, repartitions).' },
  { key: 'input_bytes', label: 'Input', value: (r) => r.input_bytes ?? 0, display: (r) => fmtBytes(r.input_bytes), flag: () => false, explain: 'Data read from files or tables. Large input with few tasks often means too few partitions or poor file pruning.' },
  { key: 'output_bytes', label: 'Output', value: (r) => r.output_bytes ?? 0, display: (r) => fmtBytes(r.output_bytes), flag: () => false, explain: 'Data written to files or tables.' },
  { key: 'max_peak_mem', label: 'Peak memory', value: (r) => r.max_peak_mem ?? 0, display: (r) => fmtBytes(r.max_peak_mem), flag: () => false, explain: 'The largest execution memory a single task used (sorts, aggregations, joins). Close to the executor memory per core means OOM risk.' },
  { key: 'tasks', label: 'Tasks', value: (r) => r.tasks ?? 0, display: (r) => fmtNum(r.tasks), flag: (r) => (r.tasks ?? 0) >= TH.tinyTasksMin && (r.p50_task_ms ?? Infinity) < TH.tinyTasksP50Ms, explain: 'Number of tasks. Thousands of tiny tasks (median under 200 ms) mean too many small files or partitions.' },
];

/** Unique stage label: attempt as 6.1, and a short Spark context suffix when the cluster has several contexts. */
const stageLabel = (r: { stage_id: number; stage_attempt: number; spark_context_id?: string }, multiCtx = false) =>
  `${multiCtx && r.spark_context_id ? `ctx …${r.spark_context_id.slice(-4)} · ` : ''}Stage ${r.stage_id}${r.stage_attempt ? `.${r.stage_attempt}` : ''}`;

export default function Stages() {
  const { cid, summary } = useCluster();
  const multiCtx = (summary.counts.apps ?? 1) > 1;
  const [sp, setQ] = useQueryState();
  const metric = METRICS.find((m) => m.key === sp.get('rank')) ?? METRICS[0];
  const status = sp.get('status') ?? '';
  const [text, setText] = useState(sp.get('q') ?? '');
  const q = useDebounced(text, 350);
  const selCtx = sp.get('ctx');
  const selStage = sp.get('stage');
  const selAttempt = sp.get('attempt') ?? '0';
  const selKey = selCtx && selStage !== null ? `${selCtx}|${selStage}|${selAttempt}` : null;

  const st = useAsync(
    (s) => api.dataset<StageRow>(cid, 'stages', { limit: TOP, sort: metric.key, desc: true, status: status || null, q: q || null }, s),
    [cid, metric.key, status, q],
  );
  const retries = useAsync((s) => api.datasetOpt<TaskRetryRow>(cid, 'task_retries', { limit: 5000 }, s), [cid]);
  const select = (ctx: string, stage: number, attempt: number) => setQ({ ctx, stage, attempt }, true);

  return (
    <div className="page wide">
      <div className="page-head">
        <div>
          <h1>Which stages took the longest, and why?</h1>
          <p className="sub">
            Which stages took the time, which ones struggled, and why. Pick what to rank by, then select a stage to see how its tasks were spread
            across executors and what was retried.
          </p>
        </div>
        <div className="actions">
          <DataLink cid={cid} dataset="stages" />
        </div>
      </div>
      <div className="stack">
        <div className="grid-overview">
          <Panel
            title={`Top ${TOP} stages by ${metric.label.toLowerCase()}`}
            note={metric.explain}
          >
            <div className="filters" style={{ marginBottom: 14 }}>
              <div className="group">
                <span className="lbl">Rank by</span>
                <div className="seg" role="group" aria-label="Rank stages by">
                  {METRICS.map((m) => (
                    <button
                      key={m.key}
                      className={m.key === metric.key ? 'on' : ''}
                      aria-pressed={m.key === metric.key}
                      onClick={() => setQ({ rank: m.key === 'duration_ms' ? null : m.key }, true)}
                    >
                      {m.label}
                    </button>
                  ))}
                </div>
              </div>
              <label className="group">
                <span className="lbl">Status</span>
                <select className="select" value={status} onChange={(e) => setQ({ status: e.target.value || null }, true)}>
                  <option value="">All</option>
                  <option value="failed">Failed</option>
                  <option value="succeeded">Succeeded</option>
                  <option value="incomplete">Incomplete</option>
                </select>
              </label>
              <label className="group grow" style={{ minWidth: 200 }}>
                <span className="lbl">Search</span>
                <input className="input" value={text} onChange={(e) => setText(e.target.value)} placeholder="Stage name, job description…" />
              </label>
            </div>
            {st.error ? (
              <ErrorState error={st.error} onRetry={st.reload} />
            ) : !st.data ? (
              <Loading label="Loading stages…" />
            ) : st.data.rows.length === 0 ? (
              <Empty title="No stages">{status || q ? 'No stages match these filters.' : 'The event log has no stages for this cluster.'}</Empty>
            ) : (
              <>
                <HBars
                  labelWidth={200}
                  data={st.data.rows.map((r) => ({
                    key: `${r.spark_context_id}|${r.stage_id}|${r.stage_attempt}`,
                    label: `${stageLabel(r, multiCtx)}${r.status === 'failed' ? ', failed' : ''}`,
                    sub: truncate(r.stage_name ?? r.job_description, 60),
                    parts: [{ value: metric.value(r), color: 'var(--series-1)', name: metric.label }],
                    display: metric.display(r),
                    flagged: metric.flag(r),
                    selected: selKey === `${r.spark_context_id}|${r.stage_id}|${r.stage_attempt}`,
                    title: `${r.stage_name ?? ''}\n${fmtDuration(r.duration_ms)}, ${fmtNum(r.tasks)} tasks${r.failed_tasks ? `, ${fmtNum(r.failed_tasks)} failed` : ''}`,
                    onClick: () => select(r.spark_context_id, r.stage_id, r.stage_attempt),
                  }))}
                />
                <p className="muted small" style={{ marginTop: 10 }}>
                  Showing {fmtNum(st.data.rows.length)} of {fmtNum(st.data.total)} stage attempts. Every column for every stage is in{' '}
                  <Link to={to.data(cid, { dataset: 'stages' })}>Data / Debug</Link>.
                </p>
              </>
            )}
          </Panel>
          <RetriesByStage
            cid={cid}
            rows={retries.data === undefined ? undefined : retries.data?.rows ?? null}
            error={retries.error}
            selKey={selKey}
            onSelect={select}
          />
        </div>
        {selCtx && selStage !== null ? (
          <StagePanel
            cid={cid}
            ctx={selCtx}
            stage={selStage}
            attempt={selAttempt}
            retries={retries.data?.rows ?? null}
            onClose={() => setQ({ ctx: null, stage: null, attempt: null }, true)}
          />
        ) : (
          <p className="muted">Select a stage to see its task-duration distribution, per-executor time, spill, GC and retries.</p>
        )}
      </div>
    </div>
  );
}

function RetriesByStage({
  cid,
  rows,
  error,
  selKey,
  onSelect,
}: {
  cid: string;
  rows: TaskRetryRow[] | null | undefined;
  error: Error | undefined;
  selKey: string | null;
  onSelect: (ctx: string, stage: number, attempt: number) => void;
}) {
  const by = useMemo(() => (rows ? retriesByStage(rows) : []), [rows]);
  const cats = RETRY_CATS.filter((c) => by.some((b) => b.byCat[c]));
  return (
    <Panel
      title="Retries by stage"
      note="Tasks that failed on the first try, by why they failed"
      actions={rows?.length ? <DataLink cid={cid} dataset="task_retries" label="Data" /> : undefined}
    >
      {error ? (
        <p className="muted small">Could not load retries: {error.message}</p>
      ) : rows === undefined ? (
        <Loading label="Loading retries…" />
      ) : rows === null ? (
        <p className="muted small">Retry details need a newer analyzer (the task_retries dataset). Re-analyze this cluster to fill them in.</p>
      ) : by.length === 0 ? (
        <p className="ink2">No task had to be retried.</p>
      ) : (
        <>
          <HBars
            labelWidth={96}
            legend={cats.map((c) => ({ name: RETRY_META[c].label, color: RETRY_META[c].color }))}
            data={by.slice(0, 15).map((b) => ({
              key: `${b.ctx}|${b.stage_id}|${b.stage_attempt}`,
              label: stageLabel(b),
              parts: cats.map((c) => ({ value: b.byCat[c] ?? 0, color: RETRY_META[c].color, name: `${RETRY_META[c].label}: ${b.byCat[c] ?? 0}` })),
              display: `${fmtNum(b.total)} ${b.total === 1 ? 'task' : 'tasks'}`,
              selected: selKey === `${b.ctx}|${b.stage_id}|${b.stage_attempt}`,
              title: `${cats
                .filter((c) => b.byCat[c])
                .map((c) => `${RETRY_META[c].label}: ${b.byCat[c]}`)
                .join(', ')}\n${fmtDuration(b.wasted)} of work lost`,
              onClick: () => onSelect(b.ctx, b.stage_id, b.stage_attempt),
            }))}
          />
          {cats.length === 1 && (
            <p className="muted small" style={{ marginTop: 8 }}>
              Every retry here was caused by: {RETRY_META[cats[0]].label.toLowerCase()}.
            </p>
          )}
          {by.length > 15 && (
            <p className="muted small" style={{ marginTop: 8 }}>
              {fmtNum(by.length - 15)} more stages had retries.
            </p>
          )}
        </>
      )}
    </Panel>
  );
}

function StageRetries({ s, rows }: { s: StageRow; rows: TaskRetryRow[] | null }) {
  const mine = useMemo(
    () =>
      (rows ?? [])
        .filter((r) => r.spark_context_id === s.spark_context_id && r.stage_id === s.stage_id && r.stage_attempt === s.stage_attempt)
        .sort((a, b) => (a.first_failure_time ?? 0) - (b.first_failure_time ?? 0)),
    [rows, s],
  );
  const [all, setAll] = useState(false);
  if (!s.retry_of_failure && s.stage_attempt === 0 && mine.length === 0) return null;
  const counts: Record<string, number> = {};
  mine.forEach((r) => (counts[retryCat(r)] = (counts[retryCat(r)] ?? 0) + 1));
  const wasted = mine.reduce((a, r) => a + (r.wasted_ms ?? 0), 0);
  const failed = mine.filter((r) => r.final_status === 'failed').length;
  const shown = all ? mine : mine.slice(0, 6);
  return (
    <div className="retries-block">
      <h3>What failed on the first try</h3>
      {s.stage_attempt > 0 && (
        <p className="ink2">
          This is attempt {s.stage_attempt + 1} of stage {s.stage_id}: Spark re-ran the stage because the previous attempt failed
          {s.retry_of_failure ? (
            <>
              : <span className="mono small wrap-any">{truncate(s.retry_of_failure, 400)}</span>
            </>
          ) : (
            '.'
          )}
        </p>
      )}
      {mine.length > 0 && (
        <>
          <p className="ink2">
            {fmtNum(mine.length)} {mine.length === 1 ? 'task' : 'tasks'} failed at least once (
            {RETRY_CATS.filter((c) => counts[c])
              .map((c) => `${counts[c]} ${RETRY_META[c].label.toLowerCase()}`)
              .join(', ')}
            ). {failed ? <b className="bad">{fmtNum(failed)} never succeeded.</b> : 'All of them succeeded on a retry.'}{' '}
            {wasted ? `${fmtDuration(wasted)} of task work was lost.` : ''}
          </p>
          <ul className="retry-list">
            {shown.map((r) => (
              <li key={r.task_index}>
                <span className="dot" style={{ background: RETRY_META[retryCat(r)].color }} aria-hidden />
                <span>{retrySentence(r)}</span>
              </li>
            ))}
          </ul>
          {mine.length > 6 && (
            <button className="btn small" onClick={() => setAll(!all)}>
              {all ? 'Show fewer' : `Show all ${fmtNum(mine.length)}`}
            </button>
          )}
        </>
      )}
    </div>
  );
}

export function StagePanel({ cid, ctx, stage, attempt, retries, onClose }: { cid: string; ctx: string; stage: string; attempt: string; retries: TaskRetryRow[] | null; onClose: () => void }) {
  const st = useAsync((s) => api.stage(cid, ctx, stage, attempt, s), [cid, ctx, stage, attempt]);
  return (
    <Async state={st} label={`Loading stage ${stage}…`}>
      {(d) => <StageDetailView cid={cid} d={d} retries={retries} onClose={onClose} />}
    </Async>
  );
}

function StageDetailView({ cid, d, retries, onClose }: { cid: string; d: StageDetail; retries: TaskRetryRow[] | null; onClose: () => void }) {
  const s = d.stage;
  const execs = [...d.executors].sort((a, b) => (b.task_ms_sum ?? 0) - (a.task_ms_sum ?? 0));
  const t = d.tasks_summary;
  const sh = d.sharing;
  return (
    <section className="panel">
      <div className="panel-head">
        <div style={{ minWidth: 0 }}>
          <div className="row" style={{ gap: 10, alignItems: 'center' }}>
            <h2>
              Stage {s.stage_id}
              {s.stage_attempt ? `.${s.stage_attempt}` : ''}
            </h2>
            <StatusBadge status={s.status} />
            {skewBreach(s.skew, s.max_task_ms) && <SeverityBadge sev="high" />}
          </div>
          <div className="note wrap-any">{s.stage_name}</div>
        </div>
        <div className="row" style={{ gap: 8 }}>
          {d.job && (
            <Link className="btn small" to={to.hierarchy(cid, { ctx: s.spark_context_id, job: d.job.spark_job_id })}>
              Job {d.job.spark_job_id}
            </Link>
          )}
          {d.query && (
            <Link className="btn small" to={to.query(cid, s.spark_context_id, d.query.sql_execution_id)}>
              Query {d.query.sql_execution_id}
            </Link>
          )}
          <button className="btn small ghost" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
      <div className="panel-body stack">
        {/* 1. what happened, in words */}
        <StageVerdict s={s} d={d} execs={execs} />
        {s.failure_reason && s.failure_reason.length > 200 && (
          <details className="small">
            <summary className="muted">Full failure message</summary>
            <pre className="mono small" style={{ whiteSpace: 'pre-wrap', maxHeight: 260, overflow: 'auto' }}>{truncate(s.failure_reason, 4000)}</pre>
          </details>
        )}

        <StageTime s={s} d={d} />

        {/* 2. the stage in six numbers */}
        <div className="kpis stage-kpis">
          {/* tiles with nothing in them (0 B written, no spill) are left out */}
          <Tile label="Duration" value={fmtDuration(s.duration_ms)} foot={`started ${fmtTs(s.start_time).slice(11, 19)} UTC`} />
          <Tile label="Tasks" value={fmtNum(s.tasks)} foot={<>on {fmtNum(s.executors_used)} executors{s.failed_tasks ? <span className="bad"> · {fmtNum(s.failed_tasks)} failed</span> : null}</>} tone={s.failed_tasks ? 'warn' : undefined} />
          {(s.input_bytes ?? 0) + (s.shuffle_read ?? 0) > 0 && <Tile label="Read" value={fmtBytes((s.input_bytes ?? 0) + (s.shuffle_read ?? 0))} foot={split2('files', s.input_bytes, 'shuffle', s.shuffle_read)} />}
          {(s.output_bytes ?? 0) > 0 && <Tile label="Wrote" value={fmtBytes(s.output_bytes)} foot="to storage" />}
          {(s.shuffle_write ?? 0) > 0 && <Tile label="Shuffled" value={fmtBytes(s.shuffle_write)} foot="to the next stage" />}
          {(s.disk_spill ?? 0) + (s.mem_spill ?? 0) > 0
            ? <Tile label="Spilled to disk" value={fmtBytes(s.disk_spill ?? 0)} foot={`from ${fmtBytes(s.mem_spill ?? 0)} in memory`} tone={spillBreach(s.disk_spill) ? 'warn' : undefined} />
            : <Tile label="Spilled" value="none" foot="every task fit in memory" />}
          {d.task_columns?.task_ms?.sum ? <Tile label="Task time" value={fmtDuration(d.task_columns.task_ms.sum)} foot={`GC ${fmtPct(s.gc_share, 0)} of it`} tone={gcBreach(s.gc_share) ? 'warn' : undefined} /> : null}
          {(s.max_peak_mem ?? 0) > 0 && <Tile label="Busiest task memory" value={fmtBytes(s.max_peak_mem ?? 0)} foot="peak of one task" />}
        </div>

        {(s.tasks ?? 0) <= 3 ? (
          <SmallStage cid={cid} s={s} execs={execs} />
        ) : (<>
        {/* 3. the tasks as statistics (all, per executor), the executors with what else ran on them, the slowest tasks */}
        <StageTaskSections cid={cid} ctx={s.spark_context_id} stageId={s.stage_id} attempt={s.stage_attempt} sharing={d.sharing ?? null} n={s.tasks ?? 0} />
        {d.hotspots && d.hotspots.length > 0 && <SkewedTasks cid={cid} rows={d.hotspots} />}
        <FailedTasks cid={cid} s={s} />
        </>)}

        {/* 6. on the executors over time, with whatever else ran there (folded) */}
        {d.task_timeline && d.task_timeline.length > 0 && (
          <details className="stage-more">
            <summary>
              <b>Tasks over time on these executors</b>{' '}
              <span className="muted small">
                {sh?.other_task_count
                  ? `${fmtNum(sh.other_task_count)} tasks of ${fmtNum(sh.other_stages)} other stages${sh.other_runs ? ` (${fmtNum(sh.other_runs)} other runs)` : ''} ran next to it` +
                    (sh.slot_ms ? `, ${fmtPct(sh.other_task_ms / sh.slot_ms, 0)} of the cores' time` : '')
                  : 'nothing else ran there'}
              </span>
            </summary>
            <StageTaskPanels rows={d.task_timeline} start={s.start_time} end={s.end_time} p50={t?.p50 ?? s.p50_task_ms} sampled={!!d.task_timeline_sampled} total={s.tasks ?? null}
              others={sh?.other_tasks ?? []} othersTotal={sh?.other_task_count ?? 0} gcHigh={gcBreach(s.gc_share)} spilled={(s.disk_spill ?? 0) > 0} />
            {sh && sh.other_stages > 0 && (
              <details className="stage-more" style={{ marginTop: 8 }}>
                <summary className="small">The {fmtNum(sh.other_stages)} other stages, by core time</summary>
                <StageSharing cid={cid} ctx={s.spark_context_id} sh={sh} />
              </details>
            )}
          </details>
        )}

        <StageRetries s={s} rows={retries} />

        {d.findings.length > 0 && (
          <div>
            <h3 style={{ marginBottom: 8 }}>Findings for this stage</h3>
            <div className="stack" style={{ gap: 8 }}>
              {d.findings.map((f) => (
                <div key={f.finding_id} className="row" style={{ gap: 10, alignItems: 'baseline' }}>
                  <SeverityBadge sev={f.severity} />
                  <Link to={to.findings(cid, f.finding_id)}>
                    <b>{f.category.replace(/_/g, ' ')}</b>
                  </Link>
                  <span className="ink2 grow wrap-any">{f.evidence}</span>
                </div>
              ))}
            </div>
          </div>
        )}
        {d.job?.error && (
          <p className="small ink2">
            <b>Job {d.job.spark_job_id} error:</b> {truncate(d.job.error, 500)}
          </p>
        )}
      </div>
    </section>
  );
}

const MB = 1 << 20;

const split2 = (a: string, x: number | null | undefined, b: string, y: number | null | undefined) =>
  [x ? `${a} ${fmtBytes(x)}` : null, y ? `${b} ${fmtBytes(y)}` : null].filter(Boolean).join(' · ') || null;

/** Where the stage's time went, as one bar: waiting for its first task (cores busy), running until 90% of the tasks
 * were done, and the tail of the last 10%. */
function StageTime({ s, d }: { s: StageRow; d: StageDetail }) {
  const tl = d.task_timeline ?? [];
  if (!tl.length || !s.start_time || !s.end_time || !s.duration_ms) return null;
  const first = Math.min(...tl.map((x) => x.launch_time ?? Infinity));
  const ends = tl.map((x) => (x.launch_time ?? 0) + (x.task_ms ?? 0)).sort((a, b) => a - b);
  const p90 = ends[Math.max(0, Math.ceil(ends.length * 0.9) - 1)];
  const wait = Math.max(0, first - s.start_time);
  const tail = (s.tasks ?? 0) >= 10 ? Math.max(0, s.end_time - p90) : 0;
  const run = Math.max(0, s.duration_ms - wait - tail);
  const tot = wait + run + tail || 1;
  // its own waves: most tasks at once, and how many rounds of that the stage needed (its own queue, not other work)
  const evs = tl.flatMap((x) => (x.launch_time == null ? [] : [[x.launch_time, 1], [x.launch_time + (x.task_ms ?? 0), -1]] as [number, number][]))
    .sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  let cur = 0, most = 0;
  for (const [, dlt] of evs) { cur += dlt; most = Math.max(most, cur); }
  const waves = most > 0 ? Math.ceil((s.tasks ?? tl.length) / most) : 0;
  const others = d.sharing?.other_runs ? `other runs (${fmtNum(d.sharing.other_runs)}) and stages held the cores` : 'other stages held the cores';
  const parts: [string, number, string, string][] = [
    ['waiting for cores', wait, 'var(--wait)', wait / tot >= 0.1 ? others : ''],
    ['running', run, 'var(--series-1)', waves > 1 ? `its own ${waves} waves of up to ${most} tasks at once` : ''],
    ['last 10% of tasks', tail, 'var(--series-3)', tail / tot >= 0.25 ? 'a few tasks held the stage up' : ''],
  ];
  return (
    <div className="stage-time">
      <div className="stage-time-head"><b><span aria-hidden>{SIGN.time} </span>Where its {fmtDuration(s.duration_ms)} went</b></div>
      <div className="vsplit-bar" style={{ height: 14 }}>
        {parts.map(([k, v, c]) => (v > 0 ? <span key={k} style={{ width: `${(v / tot) * 100}%`, background: c }} title={`${k}: ${fmtDuration(v)}`} /> : null))}
      </div>
      <div className="stage-time-legend">
        {parts.filter((p) => p[1] > 0).map(([k, v, c, why]) => (
          <span key={k}>
            <i style={{ background: c }} />
            {k.startsWith('waiting') && v / tot >= 0.1 ? <span aria-hidden>{SIGN.wait} </span> : null}<b>{fmtDuration(v)}</b> {k} <span className="muted">({fmtPct(v / tot, 0)})</span>
            {why ? <span className={k === 'running' ? 'muted' : 'st-warn'}> · {why}</span> : null}
          </span>
        ))}
      </div>
    </div>
  );
}

/** A stage of one to three tasks: one line per task instead of statistics and charts. */
function SmallStage({ cid, s, execs }: { cid: string; s: StageRow; execs: ExecRow[] }) {
  const st = useAsync(
    (sig) => api.dataset<TaskRow>(cid, 'tasks', { spark_context_id: s.spark_context_id, stage_id: s.stage_id, stage_attempt: s.stage_attempt, limit: 3 }, sig),
    [cid, s.spark_context_id, s.stage_id, s.stage_attempt],
  );
  return (
    <div className="stack" style={{ gap: 4 }}>
      <h3 className="h-sub" style={{ margin: 0 }}>{s.tasks === 1 ? 'Its one task' : `Its ${fmtNum(s.tasks)} tasks`}</h3>
      {(st.data?.rows ?? []).map((r) => (
        <div key={`${r.task_id}.${r.task_attempt}`} className={`small ${r.failed ? 'st-crit' : ''}`}>
          Task <span className="mono">{r.task_id}</span> on <b>exec {r.executor_id ?? '?'}</b>: took <b>{fmtDuration(r.task_ms)}</b>
          {(r.input_bytes ?? 0) + (r.shuffle_read ?? 0) ? <>, read {fmtBytes((r.input_bytes ?? 0) + (r.shuffle_read ?? 0))}</> : null}
          {r.disk_spill ? <>, spilled {fmtBytes(r.disk_spill)}</> : null}
          {r.gc_ms ? <>, GC {fmtDuration(r.gc_ms)}</> : null}
          {r.failed ? <>: failed, {truncate(r.error ?? r.end_reason ?? '', 120)}</> : null}
        </div>
      ))}
      {!st.data && execs.length ? <div className="small muted">on exec {execs.map((e) => e.executor_id).join(', ')}</div> : null}
    </div>
  );
}

function Tile({ label, value, foot, tone }: { label: string; value: string; foot?: React.ReactNode; tone?: 'bad' | 'warn' }) {
  return (
    <div className={`kpi ${tone ?? ''}`}>
      <div className="label">{label}</div>
      <div className="value">{value}</div>
      {foot ? <div className="foot">{foot}</div> : null}
    </div>
  );
}

/** Only the failed tasks get rows of their own: where they ran, how long, and why they failed. */
function FailedTasks({ cid, s }: { cid: string; s: StageRow }) {
  const st = useAsync(
    (sig) => api.dataset<TaskRow>(cid, 'tasks', { spark_context_id: s.spark_context_id, stage_id: s.stage_id, stage_attempt: s.stage_attempt, failed: 'true', sort: 'task_ms', desc: 'true', limit: 20 }, sig),
    [cid, s.spark_context_id, s.stage_id, s.stage_attempt],
  );
  const rows = st.data?.rows ?? [];
  if (!rows.length) return null;
  return (
    <div>
      <h3 className="h-sub" style={{ margin: '6px 0' }}>
        Failed tasks {(st.data?.total ?? 0) > rows.length ? <span className="muted small">(the {rows.length} longest of {fmtNum(st.data?.total)})</span> : null}
      </h3>
      <table className="table compact">
        <thead><tr><th>Task</th><th>Executor</th><th className="num">Ran</th><th>Why it failed</th></tr></thead>
        <tbody>
          {rows.map((r) => (
            <tr key={`${r.task_id}.${r.task_attempt}`} className="row-bad">
              <td className="mono">{r.task_id}{r.task_attempt ? <span className="muted small"> try {r.task_attempt + 1}</span> : null}</td>
              <td className="mono">exec {r.executor_id ?? '–'}</td>
              <td className="num">{fmtDuration(r.task_ms)}</td>
              <td className="small wrap-any">{truncate(r.error ?? r.end_reason ?? 'failed', 200)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const CAUSE: Record<string, { label: string; sev: 'high' | 'medium' | 'low'; fix: string }> = {
  data_skew: { label: 'Data skew', sev: 'high', fix: 'One key or partition holds far more data: salt or split the hot key, filter nulls before the join, or let AQE split skewed partitions (spark.sql.adaptive.skewJoin.enabled).' },
  slow_executor: { label: 'Slow executor', sev: 'medium', fix: 'The data was normal but this executor is slow everywhere: a bad or overloaded node. Check its GC and logs; spot reclaim or noisy neighbours are common causes.' },
  gc: { label: 'GC pressure', sev: 'medium', fix: 'The task spent much of its time in garbage collection: give executors more memory per core, or reduce cached and broadcast data.' },
  gc_stuck: { label: 'Stuck in GC', sev: 'high', fix: 'Not skew: the executor heap stayed full after every collection, so its tasks hung. Its retries on other executors were quick. Cache or broadcast less, or use bigger executors; salting keys will not help.' },
  lost: { label: 'Lost with executor', sev: 'medium', fix: 'No metrics: the task was lost when its executor went away. See why the executor was removed.' },
  unknown: { label: 'Cause unclear', sev: 'low', fix: 'Input size and GC look normal. Look at the task in the timeline and its executor logs around that time.' },
};

function SkewedTasks({ cid, rows }: { cid: string; rows: Hotspot[] }) {
  const causes = [...new Set(rows.map((r) => r.cause ?? 'unknown'))];
  return (
    <div>
      <h3 style={{ marginBottom: 4 }}>Skewed tasks: which task, where, and why</h3>
      <p className="muted small" style={{ marginBottom: 8 }}>
        Tasks at least {TH.skewRatio}× slower than the stage median and longer than {fmtDuration(TH.skewMinTaskMs)}.
      </p>
      <ul className="hot-list">
        {rows.map((h) => {
          const c = CAUSE[h.cause ?? 'unknown'] ?? CAUSE.unknown;
          return (
            <li key={`${h.task_id}-${h.ts_start}`}>
              <SeverityBadge sev={c.sev} label={c.label} />
              <div style={{ minWidth: 0 }}>
                <div className="wrap-any">{h.detail}</div>
                <div className="row small" style={{ gap: 10, marginTop: 3 }}>
                  {h.executor_id && <Link to={to.executors(cid, h.spark_context_id, h.executor_id)}>Executor {h.executor_id}</Link>}
                  {h.sql_execution_id !== null && <Link to={to.query(cid, h.spark_context_id, h.sql_execution_id)}>Query {h.sql_execution_id}</Link>}
                  <Link to={to.timeline(cid, h.spark_context_id)}>Timeline</Link>
                </div>
              </div>
            </li>
          );
        })}
      </ul>
      {causes.map((k) => (
        <p key={k} className="small ink2" style={{ marginTop: 6 }}>
          <b>{(CAUSE[k] ?? CAUSE.unknown).label}:</b> {(CAUSE[k] ?? CAUSE.unknown).fix}
        </p>
      ))}
    </div>
  );
}

type ExecRow = StageDetail['executors'][number];

/** What dominated this stage, as cards: a short title, one big number, a small chart and the change to make. */
function StageVerdict({ s, d, execs }: { s: StageRow; d: StageDetail; execs: ExecRow[] }) {
  const t = d.tasks_summary;
  const cards: VCard[] = [];
  if (s.status === 'failed')
    cards.push({ tone: 'bad', title: 'Failed', big: 'Failed', note: s.failure_reason ? truncate(s.failure_reason.split('\n')[0], 160) : 'The stage failed.' });
  // slow tasks: the slowest against the median and p90
  if (t?.max && t.p50 && skewBreach(s.skew, s.max_task_ms)) {
    const slow = d.task_durations.filter((x) => x >= Math.max(t.p50! * TH.skewRatio, TH.skewMinTaskMs)).length;
    const top = execs[0];
    const cause = d.hotspots?.[0]?.cause;
    const why = cause === 'data_skew' ? 'they read far more data (data skew)' : cause === 'slow_executor' ? 'their executor was slow' : cause === 'gc' ? 'garbage collection' : cause === 'gc_stuck' ? 'their executor was stuck in GC' : null;
    cards.push({
      tone: 'bad', title: 'A few tasks held it up', big: fmtSkew(s.skew), unit: 'slowest ÷ median',
      chart: <Bars rows={[['median', t.p50, false], ['p90', t.p90 ?? null, false], ['slowest', t.max, true]]} fmt={fmtDuration} />,
      note: `${fmtNum(slow)} of ${fmtNum(s.tasks)} tasks; the stage waited ${fmtDuration(t.max - (t.p90 ?? t.p50))} for them` +
        (top && execs.length > 1 && (top.share_of_stage_ms ?? 0) > 0.5 ? `, mostly on exec ${top.executor_id}` : '') + (why ? `. Likely ${why}.` : '.'),
    });
  }
  // tasks too big for Spark's ~128 MB target
  const wm = s.wmed_task_bytes_in ?? 0;
  if (wm >= 256 * MB) {
    const read = (s.input_bytes ?? 0) + (s.shuffle_read ?? 0);
    const want = Math.ceil(read / (128 * MB) / 100) * 100;
    cards.push({
      tone: 'warn', title: 'Tasks too big', big: fmtBytes(wm), unit: 'per task (half the data)',
      chart: <Bars rows={[['these tasks', wm, true], ['target', 128 * MB, false]]} fmt={fmtBytes} />,
      note: s.shuffle_read ? `${(wm / (128 * MB)).toFixed(1)}× the target. Use ~${fmtNum(want)} shuffle partitions instead of ${fmtNum(s.tasks)}.` : `${(wm / (128 * MB)).toFixed(1)}× the split size: the files cannot be split.`,
    });
  }
  // many tasks over the ~128 MB target, even when the median task is under it
  const bigTasks = (s.tasks_128_256 ?? 0) + (s.tasks_ge256 ?? 0);
  if (wm < 256 * MB && s.tasks && bigTasks >= 0.25 * s.tasks)
    cards.push({
      tone: 'warn', title: 'Many big tasks', big: fmtPct(bigTasks / s.tasks, 0), unit: 'of tasks read 128 MB or more',
      note: `${fmtNum(bigTasks)} of ${fmtNum(s.tasks)} tasks read more than a task is sized for (~128 MB): more partitions, or smaller files.`,
    });
  // a big read from storage: often the whole table, the query's findings say whether it could have skipped files
  if ((s.input_bytes ?? 0) >= 10 * 1024 * MB)
    cards.push({
      tone: 'warn', title: 'Big read from files', big: fmtBytes(s.input_bytes), unit: 'read from storage',
      note: `${s.stage_name && !s.stage_name.includes('<unknown>') ? `This stage: ${truncate(s.stage_name, 80)}. ` : ''}If that is most of a table, filter it on its partition or clustering columns so files are skipped; the query's findings say whether it skipped any.`,
    });
  // the CPU share of task time: low means the tasks waited on storage, the network or the shuffle, not on compute
  const cpu = d.task_columns?.cpu_ms?.sum ?? null;
  const tms = d.task_columns?.run_ms?.sum ?? d.task_columns?.task_ms?.sum ?? null;
  if (cpu !== null && tms && tms >= 10 * 60_000 && cpu / tms < 0.6)
    cards.push({
      tone: 'warn', title: 'Tasks mostly not computing', big: fmtPct(1 - cpu / tms, 0), unit: 'of task time off the CPU',
      chart: <Split parts={[['CPU', cpu, 'var(--series-1)'], ['waiting on I/O, shuffle or GC', tms - cpu, 'var(--wait)']]} fmt={fmtDuration} />,
      note: `CPU ${fmtDuration(cpu)} of ${fmtDuration(tms)} task time: the tasks spent the rest reading from storage, fetching shuffle data or in GC. ${(s.input_bytes ?? 0) > (s.shuffle_read ?? 0) ? 'Reading less from storage helps more than more CPU.' : 'Shuffling less helps more than more CPU.'}`,
    });
  if (spillBreach(s.disk_spill))
    cards.push({
      tone: 'warn', title: 'Ran out of memory', big: fmtBytes(s.disk_spill), unit: 'spilled to disk',
      chart: <Bars rows={[['read', (s.input_bytes ?? 0) + (s.shuffle_read ?? 0), false], ['spill (memory)', s.mem_spill ?? 0, false], ['spill (disk)', s.disk_spill ?? 0, true]]} fmt={fmtBytes} />,
      note: 'Smaller tasks (more partitions) or more memory per core.',
    });
  const sh = d.sharing;
  // waiting for cores is drawn in the time bar (StageTime)
  // executors shared with other stages
  const oshare = sh?.slot_ms ? sh.other_task_ms / sh.slot_ms : 0;
  const strained = spillBreach(s.disk_spill) || skewBreach(s.skew, s.max_task_ms) || gcBreach(s.gc_share);
  if (sh?.slot_ms && sh.other_stages && (sh.other_task_ms > sh.own_task_ms || (oshare >= 0.2 && strained)))
    cards.push({
      tone: 'warn', title: 'Shared executors', big: fmtPct(oshare, 0), unit: 'of the cores were busy with other stages',
      chart: <Split parts={[['this stage', sh.own_task_ms, 'var(--series-1)'], ['others', sh.other_task_ms, 'var(--text-3)'], ['idle', Math.max(0, sh.slot_ms - sh.own_task_ms - sh.other_task_ms), 'var(--line)']]} fmt={fmtDuration} pct />,
      note: `${sh.other_task_count ? `${fmtNum(sh.other_task_count)} tasks of ` : ''}${fmtNum(sh.other_stages)} stages${sh.other_runs ? `, ${fmtNum(sh.other_runs)} other runs` : ''}${strained ? '; they shared memory and disk too' : ''}.`,
    });
  if (gcBreach(s.gc_share))
    cards.push({
      tone: 'warn', title: 'Garbage collection', big: fmtPct(s.gc_share, 0), unit: 'of task time',
      chart: <Split parts={[['GC', s.gc_share ?? 0, 'var(--st-warn)'], ['work', 1 - (s.gc_share ?? 0), 'var(--series-1)']]} fmt={(x) => fmtPct(x, 0)} />,
      note: 'Executors are short of memory: more memory per core, less cached data.',
    });
  if (!cards.length) cards.push({ tone: 'ok', title: 'Nothing stands out', big: '✓', note: 'Tasks took similar time, nothing spilled, GC was low, no big read from storage.' });
  return <VCards cards={cards} />;
}

