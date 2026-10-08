import { useState } from 'react';
import { hasLogic, QueryLogic } from '../components/QueryLogic';
import { Link, useParams } from 'react-router-dom';
import { api, optional, type PlanCandidate, type QueryDetail, type QueryProfileRow } from '../api';
import { DiffView, PlanView } from '../components/PlanView';
import { useCluster } from '../components/Shell';
import { Async, DataTable, EntityChips, Panel, SeverityBadge, StatusBadge, type Col } from '../components/ui';
import { fmtBytes, fmtDuration, fmtNum, fmtPct, fmtRows, fmtSkew, fmtTs, truncate } from '../format';
import { useAsync, useQueryState } from '../hooks';
import { rowLinks, to } from '../links';
import { gcBreach, skewBreach, spillBreach } from '../thresholds';
import type { StageRow } from '../api';
import { PlanGraph } from '../components/PlanGraph';
import { FindingPoints } from '../components/FindingPoints';
import { dataSpread, Spread } from '../components/Spread';

const sum = (a: number | null, b: number | null) => (a === null && b === null ? null : (a ?? 0) + (b ?? 0));
function IOCell({ bytes, rows }: { bytes: number | null; rows: number | null }) {
  if (!bytes && !rows) return <span className="muted">–</span>;
  return (
    <>
      {fmtBytes(bytes)}
      <div className="muted small nowrap">{rows ? `${fmtRows(rows)} rows` : 'no rows'}</div>
    </>
  );
}

export default function QueryDetailPage() {
  const { cid } = useCluster();
  const { ctx = '', id = '' } = useParams();
  const st = useAsync((s) => api.query(cid, ctx, id, s), [cid, ctx, id]);
  return (
    <div className="page wide">
      <p className="small" style={{ marginBottom: 10 }}>
        <Link to={to.queries(cid)}>All queries</Link>
      </p>
      <Async state={st} label={`Loading query ${id}…`}>
        {(d) => <QueryView cid={cid} ctx={ctx} id={id} d={d} />}
      </Async>
    </div>
  );
}

/** Plan tab of the Queries & jobs page: the error, where the query came from, its findings and the physical plan. */
export function QueryPlanTab({ cid, ctx, id }: { cid: string; ctx: string; id: string }) {
  const st = useAsync((s) => api.query(cid, ctx, id, s), [cid, ctx, id]);
  const [sp, setQ] = useQueryState();
  const which = (sp.get('plan') ?? 'final') as 'final' | 'initial';
  return (
    <Async state={st} label={`Loading query ${id}…`}>
      {(d) => (
        <div className="stack">
          {d.query.error && (
            <div className="inline-error">
              Query failed
              <pre>{d.query.error}</pre>
            </div>
          )}
          {d.findings.length > 0 && (
            <Panel title={`Findings (${d.findings.length})`}>
              <div className="stack" style={{ gap: 10 }}>
                {d.findings.map((f) => <FindingPoints key={f.finding_id} cid={cid} f={f} reads={d.query.tables_read} writes={d.query.tables_written} />)}
              </div>
            </Panel>
          )}
          {hasLogic(d.logic) && (
            <Panel title="Joins and filters" note="From the final plan: what it joins on, what it filters, what each read pushed down to the files, how it groups.">
              <QueryLogic l={d.logic} />
            </Panel>
          )}
          <PlanGraph cid={cid} ctx={ctx} id={id} />
          <Panel
            title="Physical plan"
            note={which === 'final' ? 'Final plan after adaptive query execution (last AQE update)' : 'Initial plan when the query started'}
            actions={
              <div className="seg">
                <button className={which === 'final' ? 'on' : ''} onClick={() => setQ({ plan: null }, true)}>
                  Final
                </button>
                <button className={which === 'initial' ? 'on' : ''} onClick={() => setQ({ plan: 'initial' }, true)}>
                  Initial
                </button>
              </div>
            }
          >
            <PlanView key={which} text={which === 'final' ? d.query.final_plan : d.query.initial_plan} />
          </Panel>
          {d.query.details && (
            <Panel title="Where it came from" note="Call site and user code stack recorded when the query started">
              <pre className="mono small" style={{ maxHeight: 260, overflow: 'auto' }}>{d.query.details}</pre>
            </Panel>
          )}
        </div>
      )}
    </Async>
  );
}

function Metric({ label, value, bad }: { label: string; value: string; bad?: boolean }) {
  return (
    <div className="kpi">
      <div className="label">{label}</div>
      <div className="value nowrap" style={{ fontSize: 17, color: bad ? 'var(--sev-high-text)' : undefined }}>
        {value}
      </div>
    </div>
  );
}

function QueryView({ cid, ctx, id, d }: { cid: string; ctx: string; id: string; d: QueryDetail }) {
  const q = d.query;
  const p = d.profile;
  const [sp, setQ] = useQueryState();
  const which = (sp.get('plan') ?? 'final') as 'final' | 'initial';
  const stageCols: Col<StageRow>[] = [
    { key: 'stage_id', label: 'Stage', render: (r) => <Link to={to.stages(cid, r.spark_context_id, r.stage_id, r.stage_attempt)}>{r.stage_id}{r.stage_attempt ? `.${r.stage_attempt}` : ''}</Link> },
    { key: 'stage_name', label: 'Name', render: (r) => <span className="clip">{truncate(r.stage_name, 70)}</span> },
    { key: 'status', label: 'Status', render: (r) => <StatusBadge status={r.status} /> },
    { key: 'duration_ms', label: 'Duration', num: true, render: (r) => fmtDuration(r.duration_ms) },
    { key: 'tasks', label: 'Tasks', num: true, render: (r) => fmtNum(r.tasks) },
    { key: 'failed_tasks', label: 'Failed', num: true, render: (r) => fmtNum(r.failed_tasks), breach: (r) => (r.failed_tasks ?? 0) > 0 },
    { key: 'input_bytes', label: 'Read', num: true, render: (r) => <IOCell bytes={sum(r.input_bytes, r.shuffle_read)} rows={sum(r.input_records, r.shuffle_read_records ?? null)} /> },
    { key: 'output_bytes', label: 'Wrote', num: true, render: (r) => <IOCell bytes={sum(r.output_bytes, r.shuffle_write)} rows={sum(r.output_records ?? null, r.shuffle_write_records ?? null)} /> },
    { key: 'skew', label: 'Time per task', num: true, render: (r) => <Spread lo={r.min_task_ms ?? null} p10={r.p10_task_ms} mid={r.p50_task_ms} p90={r.p90_task_ms} hi={r.max_task_ms} ratio={r.skew} unit="ms" badAt={skewBreach(r.skew, r.max_task_ms)} /> },
    { key: 'data_skew', label: 'Data per task', num: true, render: (r) => <Spread {...dataSpread(r)} ratio={r.data_skew ?? null} /> },
    { key: 'disk_spill', label: 'Disk spill', num: true, render: (r) => fmtBytes(r.disk_spill), breach: (r) => spillBreach(r.disk_spill) },
    { key: 'gc_share', label: 'GC', num: true, render: (r) => fmtPct(r.gc_share), breach: (r) => gcBreach(r.gc_share) },
  ];
  return (
    <div className="stack">
      <div className="page-head" style={{ marginBottom: 0 }}>
        <div style={{ minWidth: 0 }}>
          <div className="row" style={{ gap: 10, alignItems: 'center' }}>
            <h1>Query {q.sql_execution_id}</h1>
            <StatusBadge status={q.status} />
            {p?.max_severity && p.findings ? <SeverityBadge sev={p.max_severity} count={p.findings} /> : null}
          </div>
          <p className="sub wrap-any">{q.description || 'No description'}</p>
          <p className="muted small mono">
            context {ctx} · started {fmtTs(q.start_time)} UTC · plan hash {q.plan_hash ?? '–'}
          </p>
          {d.engine && (
            <p className="small" style={{ margin: '4px 0 0' }}>
              {(['aqe', 'photon'] as const).map((k) => {
                const e = d.engine![k];
                return (
                  <span key={k} style={{ marginRight: 16 }}>
                    <b>{k === 'aqe' ? 'Adaptive execution' : 'Photon'}:</b>{' '}
                    <span className={e.state === 'used' ? 'st-ok' : e.state === 'no' ? 'st-warn' : 'muted'}>
                      {e.state === 'used' ? 'used' : e.state === 'no' ? 'not used' : 'not applicable'}
                    </span>
                    <span className="muted"> · {e.why}</span>
                  </span>
                );
              })}
            </p>
          )}
        </div>
      </div>

      {q.error && (
        <div className="inline-error">
          Query failed
          <pre>{q.error}</pre>
        </div>
      )}

      <div className="kpis" style={{ gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))' }}>
        <Metric label="Duration" value={fmtDuration(q.duration_ms)} />
        <Metric label="Spark jobs" value={fmtNum(p?.spark_jobs ?? d.jobs.length)} />
        <Metric label="Stages" value={fmtNum(p?.stages ?? d.stages.length)} />
        <Metric label="Total stage time" value={fmtDuration(p?.total_stage_ms)} />
        <Metric label="Slowest stage" value={fmtDuration(p?.max_stage_ms)} />
        <Metric label="Tasks failed" value={`${fmtNum(p?.failed_tasks ?? 0)} / ${fmtNum(p?.tasks ?? q.tasks)}`} bad={(p?.failed_tasks ?? 0) > 0} />
        <Metric label="Spill (mem / disk)" value={`${fmtBytes(p?.mem_spill)} / ${fmtBytes(p?.disk_spill ?? q.disk_spill)}`} bad={spillBreach(p?.disk_spill ?? q.disk_spill)} />
        <Metric label="GC share" value={fmtPct(p?.gc_share)} bad={gcBreach(p?.gc_share)} />
        <Metric label="Max skew" value={fmtSkew(p?.max_stage_skew ?? q.max_stage_skew)} />
        <Metric label="Input" value={fmtBytes(p?.input_bytes ?? q.input_bytes)} />
        <Metric label="Shuffle read / write" value={`${fmtBytes(p?.shuffle_read ?? q.shuffle_read)} / ${fmtBytes(p?.shuffle_write)}`} />
        <Metric label="Output" value={fmtBytes(p?.output_bytes)} />
        <Metric label="Executors used / lost" value={`${fmtNum(p?.executors_used)} / ${fmtNum(p?.executors_lost)}`} bad={(p?.executors_lost ?? 0) > 0} />
      </div>

      {q.details && (
        <Panel title="Where it came from" note="Call site and user code stack recorded when the query started">
          <pre className="mono small" style={{ maxHeight: 260, overflow: 'auto' }}>{q.details}</pre>
        </Panel>
      )}

      {hasLogic(d.logic) && (
        <Panel title="Joins and filters" note="From the final plan: what it joins on, what it filters, what each read pushed down to the files, how it groups.">
          <QueryLogic l={d.logic} />
        </Panel>
      )}

      <div className="grid-2">
        <Panel title={`Stages (${d.stages.length})`} flush>
          {d.stages.length ? <DataTable cols={stageCols} rows={d.stages} rowKey={(r) => `${r.stage_id}.${r.stage_attempt}`} maxHeight={360} /> : <p className="muted panel-body">No stages.</p>}
        </Panel>
        <div className="stack">
          <Panel title={`Spark jobs (${d.jobs.length})`}>
            {d.jobs.length === 0 ? (
              <p className="muted">No jobs.</p>
            ) : (
              <div className="stack" style={{ gap: 6 }}>
                {d.jobs.map((j) => (
                  <div key={j.spark_job_id} className="row" style={{ gap: 10, alignItems: 'baseline' }}>
                    <Link to={to.hierarchy(cid, { ctx: j.spark_context_id, job: j.spark_job_id })}>Job {j.spark_job_id}</Link>
                    <StatusBadge status={(j.result ?? '').toLowerCase().includes('fail') ? 'failed' : j.result ? 'succeeded' : 'incomplete'} />
                    <span className="muted small">{fmtDuration(j.duration_ms)}</span>
                    <span className="small ink2 grow wrap-any">{truncate(j.error ?? j.description, 160)}</span>
                  </div>
                ))}
              </div>
            )}
          </Panel>
          <Panel title={`Findings (${d.findings.length})`}>
            {d.findings.length === 0 ? (
              <p className="muted">No findings for this query.</p>
            ) : (
              <div className="stack" style={{ gap: 10 }}>
                {d.findings.map((f) => <FindingPoints key={f.finding_id} cid={cid} f={f} reads={q.tables_read} writes={q.tables_written}
                  links={<EntityChips links={rowLinks(cid, { ...f, sql_execution_id: null, finding_id: null })} />} />)}
              </div>
            )}
          </Panel>
        </div>
      </div>

      <Panel
        title="Physical plan"
        note={which === 'final' ? 'Final plan after adaptive query execution (last AQE update)' : 'Initial plan when the query started'}
        actions={
          <div className="seg">
            <button className={which === 'final' ? 'on' : ''} onClick={() => setQ({ plan: null }, true)}>
              Final
            </button>
            <button className={which === 'initial' ? 'on' : ''} onClick={() => setQ({ plan: 'initial' }, true)}>
              Initial
            </button>
          </div>
        }
      >
        <PlanView key={which} text={which === 'final' ? q.final_plan : q.initial_plan} />
      </Panel>

      <PlanDiffPanel cid={cid} ctx={ctx} id={id} which={which} />
    </div>
  );
}

const GROUP_LABEL: Record<PlanCandidate['group'], string> = {
  same_query_other_run: 'Same query in other runs',
  same_query_this_run: 'Same query in this run',
  other_query: 'Different query (advanced)',
};

/** Revision 5: compare against the same query in other analyzed runs first, then in this run, then any other query. */
export function PlanDiffPanel({ cid, ctx, id, which }: { cid: string; ctx: string; id: string; which: 'final' | 'initial' }) {
  const [sp, setQ] = useQueryState();
  const other = sp.get('diff'); // "<cluster>|<ctx>|<id>" (older links: "<ctx>|<id>")
  const [mode, setMode] = useState<'side' | 'unified'>('side');
  const cands = useAsync(
    async (s) => {
      const c = await optional(api.planCandidates(cid, ctx, id, s));
      if (c) return c;
      const t = await api.dataset<QueryProfileRow>(cid, 'query_profile', { limit: 1000, sort: 'start_time', columns: 'spark_context_id,sql_execution_id,description,status,plan_hash,duration_ms,start_time' }, s);
      return t.rows
        .filter((r) => !(r.spark_context_id === ctx && String(r.sql_execution_id) === id))
        .map((r): PlanCandidate => ({ group: 'other_query', cluster_id: cid, spark_context_id: r.spark_context_id, sql_execution_id: r.sql_execution_id, description: r.description, status: r.status, start_time: r.start_time, duration_ms: r.duration_ms, plan_hash: r.plan_hash, same_plan_hash: false, run_status: null, run_start_time: null }));
    },
    [cid, ctx, id],
  );
  const parts = other ? other.split('|') : [];
  const [bCid, bCtx, bId] = parts.length === 3 ? parts : parts.length === 2 ? [cid, parts[0], parts[1]] : [null, null, null];
  const diff = useAsync(
    (s) => api.planDiff(cid, { a_ctx: ctx, a_id: id, b_ctx: bCtx!, b_id: bId!, which, b_cid: bCid! }, s),
    [cid, ctx, id, bCid, bCtx, bId, which],
    !!(bCtx && bId),
  );
  const groups = (['same_query_other_run', 'same_query_this_run', 'other_query'] as const).map((g) => ({
    g,
    rows: (cands.data ?? []).filter((c) => c.group === g),
  }));
  const picked = (cands.data ?? []).find((c) => c.cluster_id === bCid && c.spark_context_id === bCtx && String(c.sql_execution_id) === bId);
  const optLabel = (c: PlanCandidate) =>
    c.group === 'same_query_other_run'
      ? `${c.cluster_id} · ${c.run_start_time ? fmtTs(c.run_start_time).slice(0, 10) : '?'} · run ${c.run_status ?? '?'} · query ${c.sql_execution_id}${c.same_plan_hash ? ' · same plan' : ' · plan differs'}`
      : `Query ${c.sql_execution_id}${c.status === 'failed' ? ' (failed)' : ''}${c.same_plan_hash ? ' · same plan' : ''} · ${truncate(c.description, 60)}`;
  const noSameQuery = cands.data && !groups[0].rows.length && !groups[1].rows.length;
  return (
    <Panel
      title="Compare plans"
      note="Did the plan change? Compare this query with the same query in another analyzed run (a good run vs this one), or with another execution in this run. Expression ids like #123 are ignored."
      actions={
        <>
          <select className="select" value={other ?? ''} onChange={(e) => setQ({ diff: e.target.value || null }, true)} style={{ maxWidth: 460 }}>
            <option value="">Choose what to compare with…</option>
            {groups.map(({ g, rows }) =>
              rows.length ? (
                <optgroup key={g} label={GROUP_LABEL[g]}>
                  {rows.map((c) => (
                    <option key={`${c.cluster_id}|${c.spark_context_id}|${c.sql_execution_id}`} value={`${c.cluster_id}|${c.spark_context_id}|${c.sql_execution_id}`}>
                      {optLabel(c)}
                    </option>
                  ))}
                </optgroup>
              ) : null,
            )}
          </select>
          <div className="seg">
            <button className={mode === 'side' ? 'on' : ''} onClick={() => setMode('side')}>
              Side by side
            </button>
            <button className={mode === 'unified' ? 'on' : ''} onClick={() => setMode('unified')}>
              Unified
            </button>
          </div>
        </>
      }
    >
      {!other ? (
        <p className="muted">
          {noSameQuery
            ? 'No other run of this query has been analyzed yet. Analyze a good run of the same job (Home → pick its cluster) and it will appear here, so you can see whether the plan changed. You can still compare with a different query under "Different query (advanced)".'
            : 'Pick a run to compare with. "Plan differs" between a good run and a slow or failed run of the same query often explains the change: a different join strategy, lost partition pruning, or AQE choosing differently.'}
        </p>
      ) : (
        <Async state={diff} label="Computing diff…">
          {(d) => {
            const same = d.a.plan_hash && d.a.plan_hash === d.b.plan_hash;
            const other_ = picked?.group === 'same_query_other_run' ? `the same query in run ${bCid} (${picked.run_status ?? 'status unknown'})` : picked?.group === 'same_query_this_run' ? `another execution of the same query in this run (query ${bId})` : `a different query (query ${bId})`;
            return (
              <>
                <div className="row" style={{ gap: 10, alignItems: 'center', marginBottom: 8 }}>
                  <span className={`badge ${same ? 'good' : 'bad'}`}>{same ? 'Same plan shape' : 'Plan changed'}</span>
                  <span className="ink2 small">
                    Query {id} in this run ({d.a.status ?? '?'}) vs {other_}, {which} plan.
                  </span>
                </div>
                <div className="diff-side small ink2" style={{ marginBottom: 6, gap: 12 }}>
                  <div>
                    <b>This query {id}</b> {truncate(d.a.description, 80)} <span className="mono muted">{d.a.plan_hash}</span>
                  </div>
                  <div>
                    <b>
                      {bCid && bCid !== cid ? `${bCid} · ` : ''}Query <Link to={to.query(bCid ?? cid, bCtx!, bId!)}>{bId}</Link>
                    </b>{' '}
                    {truncate(d.b.description, 80)} <span className="mono muted">{d.b.plan_hash}</span>
                  </div>
                </div>
                <DiffView diff={d.diff} mode={mode} />
              </>
            );
          }}
        </Async>
      )}
    </Panel>
  );
}
