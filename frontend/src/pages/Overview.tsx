import { useMemo } from 'react';
import { Link } from 'react-router-dom';
import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { api, getRunScope, noLogs, optional, type ClusterInfoRow, type ErrorGroup, type Hotspot, type IncidentRow, type RunRow, type Summary, type TaskRetryRow, type TimelineRow } from '../api';
import { useCluster, useRunScopeCtx } from '../components/Shell';
import { ClusterView } from './ClusterView';
import { RunOverview } from './RunOverview';
import { SettingsAdvice } from '../components/SettingsAdvice';
import { Async, DataLink, DiagChips, Empty, Panel, SeverityBadge, StatusBadge, Tile } from '../components/ui';
import { fmtBytes, fmtDuration, fmtNum, fmtPct, fmtTime, fmtTs, truncate } from '../format';
import { useAsync } from '../hooks';
import { to } from '../links';
import { SpillShuffleMini } from '../components/SpillShuffle';
import { CauseChain, FixHere } from '../components/CauseChain';
import { RETRY_CATS, RETRY_META, retrySentence, retryStats } from '../retries';
import { gcBreach, SERIES, SIGNAL_ORDER } from '../thresholds';

function verdict(s: Summary): string {
  const c = s.counts;
  if (s.status === 'failed') {
    const parts: string[] = [];
    if (c.failed_jobs) parts.push(`${fmtNum(c.failed_jobs)} of ${fmtNum(c.spark_jobs)} Spark jobs failed`);
    if (c.failed_queries) parts.push(`${fmtNum(c.failed_queries)} of ${fmtNum(c.sql_queries)} queries failed`);
    return parts.length ? `This run failed: ${parts.join(', ')}` : 'This run failed';
  }
  if (s.status === 'succeeded') return `This run succeeded: ${fmtNum(c.spark_jobs ?? 0)} Spark jobs completed`;
  return 'Outcome unknown: no job results were found in the event log';
}

function StatusBand({ s }: { s: Summary }) {
  const { cid } = useCluster();
  return (
    <div className={`status-band ${s.status}`}>
      <div className="stripe" />
      <div className="content">
        <div className="verdict">{verdict(s)}</div>
        <div className="facts">
          <span>
            Started <b>{fmtTs(s.start_time)}</b>
          </span>
          <span>
            Ended <b>{fmtTs(s.end_time)}</b>
          </span>
          <span>
            Ran for <b>{fmtDuration(s.duration_ms)}</b>
          </span>
          {s.spark_versions?.length > 0 && (
            <span>
              Spark <b>{s.spark_versions.join(', ')}</b>
            </span>
          )}
          <span className="muted">UTC</span>
        </div>
      </div>
      <div className="side">
        <StatusBadge status={s.status} />
        <Link className="btn small" to={to.story(cid)}>
          Read the story
        </Link>
      </div>
    </div>
  );
}

/** The shared tile, with "n / of" as its value. */
function Kpi({ label, value, of, foot, tone }: { label: string; value: string; of?: string; foot?: string; tone?: 'bad' | 'warn' }) {
  return <Tile label={label} value={of ? `${value} / ${of}` : value} foot={foot ?? (tone === 'bad' ? 'look at these first' : tone ? 'worth a look' : undefined)} tone={tone} />;
}

function Kpis({ s }: { s: Summary }) {
  const c = s.counts;
  const t = s.totals;
  const spill = (t.mem_spill ?? 0) + (t.disk_spill ?? 0);
  const x = c as typeof c & { executors_oom?: number; executors_killed?: number };
  const gone = (x.executors_lost ?? 0) + (x.executors_oom ?? 0) + (x.executors_killed ?? 0);
  return (
    <div className="kpis">
      <Kpi label="Duration" value={fmtDuration(s.duration_ms)} />
      <Kpi label="Spark applications" value={fmtNum(c.apps ?? 0)} />
      <Kpi label="Jobs failed" value={fmtNum(c.failed_jobs ?? 0)} of={fmtNum(c.spark_jobs ?? 0)} tone={c.failed_jobs ? 'bad' : undefined} />
      <Kpi label="Stages" value={fmtNum(c.stages ?? 0)} foot={c.failed_stages ? `${fmtNum(c.failed_stages)} failed` : 'none failed'} tone={c.failed_stages ? 'bad' : undefined} />
      <Kpi label="Task attempts retried" value={fmtNum(c.failed_tasks ?? 0)} of={fmtNum(c.tasks ?? 0)} tone={c.failed_tasks ? 'warn' : undefined} />
      <Kpi label="Spill" value={fmtBytes(spill)} foot={`disk ${fmtBytes(t.disk_spill ?? 0)}`} tone={(t.disk_spill ?? 0) >= 1024 ** 3 ? 'warn' : undefined} />
      <Kpi label="GC share of task time" value={fmtPct(t.gc_share)} tone={gcBreach(t.gc_share) ? 'warn' : undefined} />
      {/* lost, out of memory or killed: each removal counted once (autoscaling is not a loss) */}
      <Kpi label="Executors lost" value={fmtNum(gone)} of={fmtNum(c.executors ?? 0)} tone={gone ? 'bad' : undefined} />
      <Kpi label="Error log lines" value={fmtNum(c.error_lines ?? 0)} foot={`${fmtNum(c.log_errors ?? 0)} exceptions`} tone={c.error_lines ? 'warn' : undefined} />
    </div>
  );
}

function FindingsBySeverity({ s }: { s: Summary }) {
  const { cid } = useCluster();
  const f = s.findings_by_severity;
  const items = [
    { k: 'high' as const, n: f.high ?? 0, color: 'var(--sev-high)' },
    { k: 'medium' as const, n: f.medium ?? 0, color: 'var(--sev-medium)' },
    { k: 'low' as const, n: f.low ?? 0, color: 'var(--sev-low)' },
  ];
  const total = items.reduce((a, b) => a + b.n, 0);
  return (
    <Panel title="Findings by severity" actions={<Link className="btn small" to={to.findings(cid)}>All findings</Link>}>
      {total === 0 ? (
        <p className="muted">No findings. No threshold breaches or matched log signals in this run.</p>
      ) : (
        <>
          <div className="sevbar" role="img" aria-label={items.map((i) => `${i.k} ${i.n}`).join(', ')}>
            {items.map((i) => (i.n ? <span key={i.k} style={{ flex: i.n, background: i.color }} title={`${i.k}: ${i.n}`} /> : null))}
          </div>
          <div className="sev-legend">
            {items.map((i) => (
              <Link key={i.k} to={`${to.findings(cid)}`} className="item" style={{ color: 'var(--ink)' }}>
                <SeverityBadge sev={i.k} />
                <b>{fmtNum(i.n)}</b>
              </Link>
            ))}
          </div>
        </>
      )}
    </Panel>
  );
}

const KIND_LABEL: Record<string, string> = {
  outcome: 'Outcome',
  first_error: 'First error',
  root_cause: 'Likely root cause',
  performance: 'Where time went',
  code_location: 'Where to fix the code',
  next_steps: 'Next steps',
  retries: 'Retries (the job still succeeded)',
};

function Diagnosis({ s }: { s: Summary }) {
  const { cid } = useCluster();
  const steps = s.diagnosis ?? [];
  return (
    <Panel title="Steps to debug" note="Read in order: each step narrows down what went wrong" flush>
      {steps.length === 0 ? (
        <Empty title="No diagnosis steps">The pipeline produced no diagnosis for this run.</Empty>
      ) : (
        <ol className="steps">
          {steps.map((st, i) => (
            <li key={i} className={`step sev-${st.severity}`}>
              <div className="num" aria-hidden>
                {st.step ?? i + 1}
              </div>
              <div style={{ minWidth: 0 }}>
                <div className="kind">{KIND_LABEL[st.kind] ?? st.kind}</div>
                <h3>{st.title}</h3>
                {st.text && <p className="text">{st.text}</p>}
                <DiagChips cid={cid} links={st.links ?? []} />
              </div>
            </li>
          ))}
        </ol>
      )}
    </Panel>
  );
}

function SignalsChart() {
  const { cid } = useCluster();
  const st = useAsync((sig) => api.dataset<TimelineRow>(cid, 'timeline', { limit: 5000, sort: 'minute' }, sig), [cid]);
  return (
    <Panel title="Log signals per minute" note="Matched log lines (out of memory, GC, spill, fetch failures…) over time, UTC" actions={<DataLink cid={cid} dataset="timeline" label="Data" />}>
      <Async state={st} label="Loading timeline…">
        {(t) => <SignalsBars rows={t.rows} />}
      </Async>
    </Panel>
  );
}

function SignalsBars({ rows }: { rows: TimelineRow[] }) {
  const { summary } = useCluster();
  const { data, keys, colors } = useMemo(() => {
    const totals = new Map<string, number>();
    rows.forEach((r) => totals.set(r.signal, (totals.get(r.signal) ?? 0) + r.count));
    const ranked = [...totals.entries()].sort((a, b) => b[1] - a[1]).map((e) => e[0]);
    const kept = ranked.length > 7 ? ranked.slice(0, 6) : ranked; // --c1..--c7 at most, the rest folds into Other
    // stable color per signal: canonical rules order among the kept ones
    const ordered = [...kept].sort((a, b) => {
      const ia = SIGNAL_ORDER.indexOf(a);
      const ib = SIGNAL_ORDER.indexOf(b);
      return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib) || a.localeCompare(b);
    });
    const hasOther = ranked.length > kept.length;
    const keys = hasOther ? [...ordered, 'other'] : ordered;
    const colors: Record<string, string> = {};
    ordered.forEach((k, i) => (colors[k] = SERIES[i]));
    colors.other = 'var(--other)';

    const byMin = new Map<number, Record<string, number>>();
    for (const r of rows) {
      const k = kept.includes(r.signal) ? r.signal : 'other';
      const o = byMin.get(r.minute) ?? {};
      o[k] = (o[k] ?? 0) + r.count;
      byMin.set(r.minute, o);
    }
    let minutes = [...byMin.keys()].sort((a, b) => a - b);
    if (minutes.length > 1) {
      const span = (minutes[minutes.length - 1] - minutes[0]) / 60_000;
      if (span <= 720) {
        const full: number[] = [];
        for (let m = minutes[0]; m <= minutes[minutes.length - 1]; m += 60_000) full.push(m);
        minutes = full;
      }
    }
    const data = minutes.map((m) => ({ minute: m, ...(byMin.get(m) ?? {}) }));
    return { data, keys, colors };
  }, [rows]);

  if (!rows.length) return <p className="muted">{noLogs(summary) ? 'No driver or executor logs here, so OOM, GC, spill and failure lines are not known.' : 'No log signals matched. Logs contained no OOM, GC, spill or failure patterns.'}</p>;
  return (
    <div className="chart-box" style={{ height: 260 }}>
      <ResponsiveContainer>
        <BarChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: -12 }} barCategoryGap={1}>
          <CartesianGrid vertical={false} />
          <XAxis dataKey="minute" tickFormatter={(v: number) => fmtTime(v).slice(0, 5)} tickLine={false} axisLine={{ stroke: 'var(--axis)' }} minTickGap={24} />
          <YAxis allowDecimals={false} tickLine={false} axisLine={false} width={44} />
          <Tooltip
            labelFormatter={(v) => `${fmtTs(Number(v))} UTC`}
            contentStyle={{ background: 'var(--panel)', border: '1px solid var(--line)', borderRadius: 6, color: 'var(--ink)', fontSize: 12.5 }}
            itemStyle={{ color: 'var(--ink)', padding: 0 }}
            labelStyle={{ color: 'var(--ink-2)', marginBottom: 4 }}
          />
          <Legend wrapperStyle={{ fontSize: 12.5, color: 'var(--ink-2)' }} iconType="square" iconSize={10} />
          {keys.map((k) => (
            <Bar key={k} dataKey={k} stackId="s" fill={colors[k]} stroke="var(--chart-surface)" strokeWidth={1} isAnimationActive={false} maxBarSize={28} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

/* ------------------------------------------------------------ cluster info (Revision 3) */

function infoRows(s: Summary, ds: ClusterInfoRow[] | null | undefined): Partial<ClusterInfoRow>[] {
  const ci = s.cluster_info;
  if (Array.isArray(ci) && ci.length) return ci;
  if (ci && !Array.isArray(ci) && Object.keys(ci).length) return [ci];
  return ds ?? [];
}

function workersText(c: Partial<ClusterInfoRow>): string | null {
  const { min_workers: mn, max_workers: mx, target_workers: tg } = c;
  if (mn !== null && mn !== undefined && mx !== null && mx !== undefined && mn !== mx) {
    return `${fmtNum(mn)} to ${fmtNum(mx)} (autoscaling${tg !== null && tg !== undefined ? `, target ${fmtNum(tg)}` : ''})`;
  }
  const n = tg ?? mx ?? mn;
  return n === null || n === undefined ? null : `${fmtNum(n)} fixed`;
}

function ClusterInfoPanel({ s }: { s: Summary }) {
  const { cid } = useCluster();
  const hasBlock = !!s.cluster_info && (Array.isArray(s.cluster_info) ? s.cluster_info.length > 0 : Object.keys(s.cluster_info).length > 0);
  const st = useAsync((sig) => api.datasetOpt<ClusterInfoRow>(cid, 'cluster_info', { limit: 50 }, sig), [cid], !hasBlock);
  const rows = infoRows(s, st.data?.rows);
  const c = rows[0];
  if (!c) {
    if (!hasBlock && st.loading) return <Panel title="Cluster">{<p className="muted small">Loading cluster details…</p>}</Panel>;
    return (
      <Panel title="Cluster">
        <p className="muted small">
          No cluster details found. Cluster name, node types and job ids come from <span className="mono">spark.databricks.clusterUsageTags.*</span> in
          the event log; re-analyze with the latest version to read them.
        </p>
      </Panel>
    );
  }
  const facts: [string, string | null | undefined][] = [
    ['Databricks Runtime', c.spark_version ?? s.spark_versions?.join(', ')],
    ['Driver node', c.driver_node_type],
    ['Worker node', c.worker_node_type],
    ['Workers', workersText(c)],
    ['Engine', c.runtime_engine],
    ['Workload', c.workload_type],
    ['Cloud', [c.cloud_provider, c.region].filter(Boolean).join(', ') || null],
    ['Databricks job', c.databricks_job_id],
    ['Job run', c.job_run_id],
    ['Task run', c.task_run_id],
    ['Parent run', c.parent_run_id],
  ];
  const shown = facts.filter(([, v]) => v !== null && v !== undefined && v !== '');
  return (
    <Panel
      title="Cluster"
      note={rows.length > 1 ? `${rows.length} Spark contexts ran on this cluster; showing the first` : undefined}
      actions={<DataLink cid={cid} dataset="cluster_info" label="All properties" />}
    >
      <div className="cluster-name">{c.cluster_name || <span className="muted">Unnamed cluster</span>}</div>
      <dl className="kv cluster-facts">
        {shown.map(([k, v]) => (
          <div key={k} className="kv-pair">
            <dt>{k}</dt>
            <dd className={/job|run/i.test(k) ? 'mono' : undefined}>{v}</dd>
          </div>
        ))}
      </dl>
    </Panel>
  );
}

/* ------------------------------------------------------------ retries (Revision 3 item 12) */

function RetriesCard() {
  const { cid, summary } = useCluster();
  const st = useAsync((sig) => api.datasetOpt<TaskRetryRow>(cid, 'task_retries', { limit: 5000, sort: 'first_failure_time' }, sig), [cid]);
  const stats = useMemo(() => (st.data ? retryStats(st.data.rows) : null), [st.data]);
  if (st.error) return <Panel title="Retries">{<p className="muted small">Could not load retries: {st.error.message}</p>}</Panel>;
  if (st.data === null && summary.retries && (summary.retries.tasks ?? 0) > 0) {
    // dataset endpoint not available yet, but summary.json carries the totals
    const r = summary.retries;
    const cats = RETRY_CATS.filter((c) => r.by_category?.[c]);
    return (
      <Panel title="Retries" note="Tasks that failed at least once, and what happened next">
        <p className="retry-head">
          <b>{fmtNum(r.tasks ?? 0)}</b> tasks in {fmtNum(r.stages ?? 0)} stages failed on the first try
          {r.still_failed ? <>, <b className="bad">{fmtNum(r.still_failed)} never succeeded</b></> : null}
          {r.wasted_ms ? <>; {fmtDuration(r.wasted_ms)} of task work was lost</> : null}.
        </p>
        <div className="legend">
          {cats.map((c) => (
            <span key={c} className="item">
              <span className="sw" style={{ background: RETRY_META[c].color }} />
              {RETRY_META[c].label} <b>{fmtNum(r.by_category?.[c] ?? 0)}</b>
            </span>
          ))}
        </div>
        <ul className="retry-list">
          {(r.examples ?? [])
            .map((e) => (typeof e === 'string' ? e : e && typeof e === 'object' && 'stage_id' in e ? retrySentence(e as TaskRetryRow, true) : null))
            .filter((x): x is string => !!x)
            .map((t, i) => (
              <li key={i}>
                <span className="dot" style={{ background: 'var(--other)' }} aria-hidden />
                <span>{t}</span>
              </li>
            ))}
        </ul>
      </Panel>
    );
  }
  if (st.data === null)
    return (
      <Panel title="Retries">
        <p className="muted small">Retry details need a newer analyzer (the task_retries dataset). Re-analyze this cluster to see what failed on the first try.</p>
      </Panel>
    );
  if (!st.data || !stats) return <Panel title="Retries">{<p className="muted small">Loading retries…</p>}</Panel>;
  if (stats.total === 0)
    return (
      <Panel title="Retries">
        <p className="ink2">Every task succeeded on its first attempt. Nothing was retried.</p>
      </Panel>
    );
  const cats = RETRY_CATS.filter((c) => stats.byCat[c]);
  const top = [...st.data.rows].sort((a, b) => (b.wasted_ms ?? 0) - (a.wasted_ms ?? 0)).slice(0, 3);
  const stillOk = summary.status === 'succeeded';
  return (
    <Panel title="Retries" note="Tasks that failed at least once, and what happened next" actions={<DataLink cid={cid} dataset="task_retries" label="All retries" />}>
      <p className="retry-head">
        <b>{fmtNum(stats.total)}</b> {stats.total === 1 ? 'task' : 'tasks'} in {fmtNum(stats.stages)} {stats.stages === 1 ? 'stage' : 'stages'} failed
        on the first try. {fmtNum(stats.succeeded)} succeeded on a retry
        {stats.failed ? <>, <b className="bad">{fmtNum(stats.failed)} never did</b></> : null}
        {stats.wasted ? <>; {fmtDuration(stats.wasted)} of task work was lost</> : null}.
        {stillOk && !stats.failed ? ' The run still succeeded, but the retries cost time.' : ''}
      </p>
      <div className="catbar" role="img" aria-label={cats.map((c) => `${RETRY_META[c].label} ${stats.byCat[c]}`).join(', ')}>
        {cats.map((c) => (
          <span key={c} style={{ flex: stats.byCat[c], background: RETRY_META[c].color }} title={`${RETRY_META[c].label}: ${stats.byCat[c]}`} />
        ))}
      </div>
      <div className="legend" style={{ marginTop: 8 }}>
        {cats.map((c) => (
          <span key={c} className="item">
            <span className="sw" style={{ background: RETRY_META[c].color }} />
            {RETRY_META[c].label} <b>{fmtNum(stats.byCat[c])}</b>
          </span>
        ))}
      </div>
      <ul className="retry-list">
        {top.map((r) => (
          <li key={`${r.spark_context_id}|${r.stage_id}|${r.stage_attempt}|${r.task_index}`}>
            <span className={`dot ${r.final_status === 'failed' ? 'bad' : ''}`} style={{ background: RETRY_META[r.first_failure_category ?? 'other']?.color ?? 'var(--other)' }} aria-hidden />
            <span>
              {retrySentence(r, true)}{' '}
              <Link to={to.stages(cid, r.spark_context_id, r.stage_id, r.stage_attempt)}>Open stage</Link>
            </span>
          </li>
        ))}
      </ul>
    </Panel>
  );
}

const PEAK_META: Record<string, { label: string; sev: 'high' | 'medium' | 'low' }> = {
  skew_task: { label: 'Skewed task', sev: 'high' },
  shuffle_peak: { label: 'Shuffle peak', sev: 'medium' },
  spill_peak: { label: 'Spill peak', sev: 'medium' },
  gc_peak: { label: 'GC peak', sev: 'medium' },
  slowest_task: { label: 'Slowest task', sev: 'low' },
};
const CAUSE_LABEL: Record<string, string> = { data_skew: 'data skew', slow_executor: 'slow executor', gc: 'GC pressure', gc_stuck: 'stuck in GC', lost: 'lost with executor', unknown: 'cause unclear' };

/** Revision 5: the sharpest peaks as one-line sentences with links (top skewed tasks, then one shuffle, spill and GC peak). */
function PeaksPanel() {
  const { cid } = useCluster();
  const st = useAsync((sig) => optional(api.hotspots(cid, {}, sig)), [cid]);
  const picked = useMemo(() => {
    const rows = st.data ?? [];
    const out: Hotspot[] = rows.filter((h) => h.kind === 'skew_task').slice(0, 2);
    for (const k of ['shuffle_peak', 'spill_peak', 'gc_peak'] as const) {
      const h = rows.find((x) => x.kind === k && (x.ratio ?? 2) >= 1.5);
      if (h) out.push(h);
    }
    return out.slice(0, 4);
  }, [st.data]);
  if (st.data === null) return null; // older analyzer: no hotspots dataset
  return (
    <Panel
      title="Peaks and hotspots"
      note="When and where the run was hottest: which task, executor, stage and SQL operator, and the likely cause"
      actions={<DataLink cid={cid} dataset="hotspots" label="All hotspots" />}
    >
      {!st.data ? (
        <p className="muted small">{st.loading ? 'Loading hotspots…' : 'Not available.'}</p>
      ) : picked.length === 0 ? (
        <p className="ink2">No skewed tasks and no unusual spikes of shuffle, spill or GC. Work was spread evenly.</p>
      ) : (
        <ul className="hot-list">
          {picked.map((h, i) => {
            const m = PEAK_META[h.kind] ?? PEAK_META.slowest_task;
            return (
              <li key={i}>
                <SeverityBadge sev={m.sev} label={m.label} />
                <div style={{ minWidth: 0 }}>
                  <div className="wrap-any">{h.detail}</div>
                  <div className="row small" style={{ gap: 10, marginTop: 3 }}>
                    {h.cause && <span className="muted">Cause: {CAUSE_LABEL[h.cause] ?? h.cause}</span>}
                    {h.stage_id !== null && <Link to={to.stages(cid, h.spark_context_id, h.stage_id, h.stage_attempt ?? 0)}>Stage {h.stage_id}{h.stage_attempt ? `.${h.stage_attempt}` : ''}</Link>}
                    {h.executor_id && <Link to={to.executors(cid, h.spark_context_id, h.executor_id)}>Executor {h.executor_id}</Link>}
                    {h.sql_execution_id !== null && <Link to={to.query(cid, h.spark_context_id, h.sql_execution_id)}>Query {h.sql_execution_id}</Link>}
                    <Link to={to.timeline(cid, h.spark_context_id)}>Timeline</Link>
                  </div>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </Panel>
  );
}

/** Revision 6: runs on this cluster over time, when there is more than one. */
function RunsSwimlane() {
  const { cid } = useCluster();
  const st = useAsync((sig) => optional(api.runs(cid, sig)), [cid]);
  const runs: RunRow[] = (st.data?.runs ?? []).filter((r) => r.start_time !== null && r.end_time !== null);
  if (runs.length < 2) return null;
  const t0 = Math.min(...runs.map((r) => r.start_time as number));
  const t1 = Math.max(...runs.map((r) => r.end_time as number));
  const span = Math.max(1, t1 - t0);
  const cur = getRunScope();
  const tone = (r: RunRow) => (r.status === 'failed' ? 'var(--st-crit)' : r.status === 'incomplete' ? 'var(--st-warn)' : 'var(--c1)');
  return (
    <Panel title="Runs on this cluster" note={`${runs.length} runs between ${fmtTime(t0)} and ${fmtTime(t1)} UTC. Overlapping bars ran in parallel and shared executors, memory and shuffle.`}>
      <div className="runs-lane">
        {runs.map((r) => (
          <div key={r.run_key} className={`runs-row ${r.run_key === cur ? 'on' : ''}`}>
            <div className="runs-label wrap-any" title={r.run_key}>
              {r.label}
              <span className="muted small"> · {r.status}{r.overlapping_runs?.length ? ` · overlaps ${r.overlapping_runs.length}` : ''}</span>
            </div>
            <div className="runs-track">
              <span
                className="runs-bar"
                style={{
                  left: `${(((r.start_time as number) - t0) / span) * 100}%`,
                  width: `${Math.max(0.6, (((r.end_time as number) - (r.start_time as number)) / span) * 100)}%`,
                  background: tone(r),
                }}
                title={`${fmtTime(r.start_time)}–${fmtTime(r.end_time)} UTC, ${fmtDuration(r.duration_ms)}, ${fmtNum(r.tasks)} tasks, ${fmtNum(r.retried_tasks)} retried`}
              />
            </div>
          </div>
        ))}
      </div>
    </Panel>
  );
}

const TOP_PROBLEMS = 5;

/** One line that says what the exception was: for a Python traceback its last "XxxError: ..." line (none when it has only stack lines), else the first line. */
function errorLine(msg: string | null): string | null {
  if (!msg) return null;
  const lines = msg.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  if (!lines.length) return null;
  if (/^Traceback|^An error occurred while calling/.test(lines[0])) {
    const real = [...lines].reverse().find((l) => /^[\w.]+(Error|Exception|Exit|Interrupt)\b/.test(l));
    return real ?? null; // a bare stack says nothing; the code line is shown below it
  }
  return lines[0];
}

/** The two places to start, side by side: the problems the analyzer found (failures and slowness, ranked) and the
 *  exceptions the logs threw (grouped, most frequent first). Each row opens its page. */
function ProblemsAtGlance() {
  const { cid, summary: s } = useCluster();
  const st = useAsync(
    async (sig) => {
      const [inc, errs] = await Promise.all([api.datasetOpt<IncidentRow>(cid, 'incidents', { limit: 5000 }, sig), optional(api.errors(cid, sig))]);
      // one row per incident: its root cause row when there is one (that is the finding to open)
      const by = new Map<string, IncidentRow>();
      for (const r of inc?.rows ?? []) {
        const cur = by.get(r.incident_id);
        if (!cur || (r.role === 'root' && cur.role !== 'root')) by.set(r.incident_id, r);
      }
      return {
        incidents: [...by.values()].sort((a, b) => a.incident_rank - b.incident_rank),
        errors: [...(errs ?? [])].sort((a, b) => b.occurrences - a.occurrences),
      };
    },
    [cid],
  );
  const nFindings = s.counts.findings ?? 0;
  const nErrors = s.counts.log_errors ?? 0;
  if (!nFindings && !nErrors) return null;
  return (
    <div className="grid-2 problems-glance">
      <Panel
        title="Problems found"
        note="Failures and slowness, worst first. Each is one problem with everything that followed from it."
        actions={
          <Link className="btn small" to={to.findings(cid)}>
            All {fmtNum(nFindings)} findings
          </Link>
        }
      >
        {!st.data ? (
          <p className="muted small">{st.error ? `Could not load problems: ${st.error.message}` : 'Loading problems…'}</p>
        ) : st.data.incidents.length ? (
          <ol className="glance-list">
            {st.data.incidents.slice(0, TOP_PROBLEMS).map((r) => (
              <li key={r.incident_id}>
                <SeverityBadge sev={r.incident_severity} />
                <div className="glance-main">
                  <Link to={to.findings(cid, r.finding_id)}>
                    <b>{r.incident_title}</b>
                  </Link>
                  {r.incident_impact && <div className="muted small">{truncate(r.incident_impact, 140)}</div>}
                </div>
                <span className="muted small mono">{fmtTime(r.incident_start)}</span>
              </li>
            ))}
            {st.data.incidents.length > TOP_PROBLEMS && (
              <li className="glance-more">
                <Link to={to.findings(cid)}>{fmtNum(st.data.incidents.length - TOP_PROBLEMS)} more problems</Link>
              </li>
            )}
          </ol>
        ) : (
          <p className="muted small">No problems: nothing failed and nothing was unusually slow.</p>
        )}
      </Panel>
      <Panel
        title="Errors in the logs"
        note="Exceptions grouped by type and place, most frequent first."
        actions={
          <Link className="btn small" to={to.errors(cid)}>
            All {fmtNum(nErrors)} errors
          </Link>
        }
      >
        {!st.data ? (
          <p className="muted small">{st.error ? '' : 'Loading errors…'}</p>
        ) : st.data.errors.length ? (
          <ol className="glance-list">
            {st.data.errors.slice(0, TOP_PROBLEMS).map((e: ErrorGroup) => (
              <li key={e.fingerprint}>
                <span className="glance-count mono" title="Times logged">
                  {fmtNum(e.occurrences)}×
                </span>
                <div className="glance-main">
                  <Link to={to.errors(cid, e.fingerprint)}>
                    <b className="mono wrap-any">{e.exception_class.split('.').pop()}</b>
                  </Link>
                  {errorLine(e.sample_message) && <div className="muted small wrap-any">{truncate(errorLine(e.sample_message)!, 140)}</div>}
                  {e.user_frame && <div className="small mono wrap-any">at {truncate(e.user_frame, 90)}</div>}
                </div>
                <span className="muted small mono">{fmtTime(e.first_seen)}</span>
              </li>
            ))}
            {st.data.errors.length > TOP_PROBLEMS && (
              <li className="glance-more">
                <Link to={to.errors(cid)}>{fmtNum(st.data.errors.length - TOP_PROBLEMS)} more exception groups</Link>
              </li>
            )}
          </ol>
        ) : (
          <p className="muted small">{noLogs(s) ? 'No driver or executor logs here, so exceptions are not known.' : 'No exceptions in the logs.'}</p>
        )}
      </Panel>
    </div>
  );
}

export default function Overview() {
  const { summary: s, cid } = useCluster();
  const scope = useRunScopeCtx();
  if (scope.runs.length > 1 && !scope.run && !s.empty_reason) return <ClusterView />;
  const picked = scope.runs.length > 1 ? scope.runs.find((r) => r.run_key === scope.run) : undefined;
  if (picked && !s.empty_reason) return <RunOverview run={picked} />;
  if (s.empty_reason)
    return (
      <div className="page">
        <div className="page-head">
          <div>
            <h1>No cluster logs to analyze</h1>
            <p className="sub mono small">{cid}</p>
          </div>
        </div>
        <div className="panel panel-body stack" style={{ gap: 10, maxWidth: '72ch' }}>
          <p style={{ margin: 0 }}>The folder for this cluster has no driver, executor or event logs, so there is nothing to show on the other pages.</p>
          <ul className="ink2" style={{ margin: 0, paddingLeft: 18 }}>
            {/* init-script logs prove a classic cluster: then the logs were not delivered, it is not serverless */}
            {((s as typeof s & { init_scripts?: { node_starts?: number } }).init_scripts?.node_starts ?? 0) > 0
              ? <li>Init scripts ran on {fmtNum((s as typeof s & { init_scripts?: { node_starts?: number } }).init_scripts!.node_starts!)} nodes, so this was a classic cluster: its driver, executor and event logs were not delivered here (or were removed).</li>
              : <li>Serverless compute writes no cluster logs: this tool covers classic clusters.</li>}
            <li>For a classic cluster, check that cluster log delivery is turned on and points at this folder.</li>
            <li>Check the cluster id and the log root you picked.</li>
          </ul>
          {s.input_dir && (
            <p className="muted small" style={{ margin: 0 }}>
              Looked in <span className="mono wrap-any">{s.input_dir}</span>
            </p>
          )}
          <div>
            <Link className="btn" to="/">
              Analyze another folder
            </Link>
          </div>
        </div>
      </div>
    );
  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Run overview</h1>
          <p className="sub mono small">{cid}</p>
        </div>
        <div className="actions muted small">
          Analyzed {s.built_at ? fmtTs(Date.parse(s.built_at)) : '–'} UTC with version {s.tool_version}
        </div>
      </div>
      <div className="stack">
        <StatusBand s={s} />
        <div className="grid-cause">
          <CauseChain cid={cid} s={s} />
          <FixHere cid={cid} s={s} />
        </div>
        <ProblemsAtGlance />
        {scope.runs.length <= 1 && <SettingsAdvice cid={cid} />}
        <Kpis s={s} />
        <RunsSwimlane />
        <div className="grid-2">
          <ClusterInfoPanel s={s} />
          <RetriesCard />
        </div>
        <PeaksPanel />
        <div className="grid-overview">
          <Diagnosis s={s} />
          <div className="stack">
            <FindingsBySeverity s={s} />
            <SignalsChart />
            {(s.totals.mem_spill ?? 0) + (s.totals.disk_spill ?? 0) + (s.totals.shuffle_read ?? 0) + (s.totals.shuffle_write ?? 0) > 0 && <SpillShuffleMini cid={cid} ctx={null} />}
            {(s.counts.story_dropped ?? 0) > 0 && (
              <p className="muted small">
                The story keeps the {fmtNum(s.counts.story_rows ?? 0)} most important rows; {fmtNum(s.counts.story_dropped ?? 0)} lower-severity log rows were
                dropped. Use <Link to={to.logs(cid)}>Logs</Link> for everything.
              </p>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
