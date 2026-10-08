// Side panel for a selected graph node: what it is doing, metrics, retries, findings, links.
import { Link } from 'react-router-dom';
import { api, type FindingRow, type TaskRetryRow } from '../api';
import { SeverityBadge, StatusBadge } from '../components/ui';
import { fmtBytes, fmtDuration, fmtNum, fmtPct, fmtRows, fmtSkew, fmtTs, truncate } from '../format';
import { useAsync } from '../hooks';
import { to } from '../links';
import { retrySentence } from '../retries';
import { gcBreach, spillBreach } from '../thresholds';
import { METRIC_META, STATUS_META, typeLabel, type GModel, type GNode } from './model';

/** Scan parquet sales -> Filter -> HashAggregate -> Exchange -> Write */
export function Pipeline({ ops, label }: { ops: string[]; label: string }) {
  if (!ops.length) return null;
  return (
    <div className="pipe-wrap">
      <div className="pipe-label">{label}</div>
      <ol className="pipeline" aria-label={label}>
        {ops.map((o, i) => (
          <li key={i} className={/^(scan|filescan|batchscan|read)/i.test(o) ? 'src' : /write|insert|save|createtable|overwrite/i.test(o) ? 'sink' : /exchange|shuffle/i.test(o) ? 'xchg' : ''}>
            <span className="op" title={o}>
              {o}
            </span>
            {i < ops.length - 1 && (
              <span className="arrow" aria-hidden>
                →
              </span>
            )}
          </li>
        ))}
      </ol>
    </div>
  );
}

function Kv({ items }: { items: [string, React.ReactNode, boolean?][] }) {
  const shown = items.filter(([, v]) => v !== null && v !== undefined && v !== '' && v !== '–');
  if (!shown.length) return null;
  return (
    <dl className="kv np-kv">
      {shown.map(([k, v, bad]) => (
        <div key={k} className="np-pair">
          <dt>{k}</dt>
          <dd className={bad ? 'bad' : undefined}>{v}</dd>
        </div>
      ))}
    </dl>
  );
}

function sameCtxQuery(m: GModel, n: GNode): GNode | null {
  if (n.type === 'query') return n;
  const jobId = n.type === 'job' ? n.id : n.type === 'stage' ? n.parent : null;
  if (!jobId) return null;
  for (const e of m.edges) if (e.kind === 'runs_query' && e.target === jobId) return m.byId.get(e.source) ?? null;
  return null;
}

function linkedConnect(m: GModel, n: GNode): GNode | null {
  if (n.type === 'connect') return n;
  const jobId = n.type === 'job' ? n.id : n.type === 'stage' ? n.parent : null;
  if (!jobId) return null;
  for (const e of m.edges) if (e.kind === 'from_connect' && e.target === jobId) return m.byId.get(e.source) ?? null;
  return null;
}

function WhatItDoes({ m, n }: { m: GModel; n: GNode }) {
  const w = n.what ?? {};
  const q = sameCtxQuery(m, n);
  const c = linkedConnect(m, n);
  const ops = (w.operators?.length ? w.operators : q?.what?.operators) ?? [];
  const tablesRead = (w.tables_read?.length ? w.tables_read : q?.what?.tables_read) ?? [];
  const tablesWritten = (w.tables_written?.length ? w.tables_written : q?.what?.tables_written) ?? [];
  const sqlDesc = w.sql_description ?? q?.what?.sql_description ?? q?.sublabel ?? null;
  const stmt = w.statement_text ?? c?.what?.statement_text ?? null;
  const desc = w.description ?? (n.type === 'stage' ? n.sublabel : null);
  const scopes = w.rdd_scopes ?? [];
  const nothing = !desc && !sqlDesc && !ops.length && !stmt && !w.call_site && !w.notebook_path && !scopes.length && !tablesRead.length && !tablesWritten.length;
  return (
    <section className="np-sec">
      <h3>What it is doing</h3>
      {nothing && <p className="muted small">The event log carries no description, call site or plan for this {typeLabel[n.type].toLowerCase()}.</p>}
      {desc && <p className="np-desc">{desc}</p>}
      {sqlDesc && sqlDesc !== desc && (
        <p className="np-desc">
          <span className="muted">SQL: </span>
          {truncate(sqlDesc, 400)}
        </p>
      )}
      <Pipeline ops={ops} label={q && q !== n ? `Plan of ${q.label}` : 'Plan operators'} />
      {(tablesRead.length > 0 || tablesWritten.length > 0) && (
        <div className="np-tables">
          {tablesRead.length > 0 && (
            <div>
              <span className="muted small">Reads </span>
              {tablesRead.map((t) => (
                <code key={t} className="np-table">
                  {t}
                </code>
              ))}
            </div>
          )}
          {tablesWritten.length > 0 && (
            <div>
              <span className="muted small">Writes </span>
              {tablesWritten.map((t) => (
                <code key={t} className="np-table w">
                  {t}
                </code>
              ))}
            </div>
          )}
        </div>
      )}
      {stmt && (
        <div>
          <div className="pipe-label">Spark Connect statement{c?.sublabel && !stmt.startsWith(c.sublabel.slice(0, 12)) ? ` by ${c.sublabel}` : ''}</div>
          <pre className="stmt">{stmt}</pre>
        </div>
      )}
      {scopes.length > 0 && <Pipeline ops={scopes} label="Operators in this stage (RDD scopes)" />}
      {(w.call_site || w.notebook_path) && (
        <Kv
          items={[
            ['Notebook', w.notebook_path ? <span className="mono">{w.notebook_path}</span> : null],
            ['Call site', w.call_site ? <pre className="np-callsite">{truncate(w.call_site, 900)}</pre> : null],
          ]}
        />
      )}
    </section>
  );
}

function Metrics({ n }: { n: GNode }) {
  const mt = n.metrics ?? {};
  const dur = n.duration_ms ?? (n.start && n.end ? n.end - n.start : null);
  return (
    <section className="np-sec">
      <h3>Metrics</h3>
      <Kv
        items={[
          ['Started', n.start ? `${fmtTs(n.start)} UTC` : null],
          ['Duration', fmtDuration(dur)],
          ['Tasks', mt.tasks !== null && mt.tasks !== undefined ? fmtNum(mt.tasks) : null],
          ['Failed tasks', mt.failed_tasks ? fmtNum(mt.failed_tasks) : null, true],
          ['Attempt', (mt.attempts ?? 1) > 1 ? `${(mt.attempt ?? 0) + 1} of ${mt.attempts}` : null],
          ['Input', mt.input_bytes ? fmtBytes(mt.input_bytes) : null],
          ['Output', mt.output_bytes ? fmtBytes(mt.output_bytes) : null],
          [METRIC_META.shuffle_read.label, mt.shuffle_read ? fmtBytes(mt.shuffle_read) : null],
          [METRIC_META.shuffle_write.label, mt.shuffle_write ? fmtBytes(mt.shuffle_write) : null],
          [METRIC_META.mem_spill.label, mt.mem_spill ? fmtBytes(mt.mem_spill) : null],
          [METRIC_META.disk_spill.label, mt.disk_spill ? fmtBytes(mt.disk_spill) : null, spillBreach(mt.disk_spill)],
          ['GC share', mt.gc_share ? fmtPct(mt.gc_share) : null, gcBreach(mt.gc_share)],
          ['Skew (slowest / median task)', mt.skew ? fmtSkew(mt.skew) : null, n.flagSet.has('skew')],
          ['Rows read · written', mt.input_records != null || mt.shuffle_read_records != null || mt.output_records != null
            ? `${fmtRows((mt.input_records ?? 0) + (mt.shuffle_read_records ?? 0))} · ${fmtRows((mt.output_records ?? 0) + (mt.shuffle_write_records ?? 0))}`
            : null],
          ['Data skew (biggest / median task input)', mt.data_skew ? fmtSkew(mt.data_skew) : null, (mt.data_skew ?? 0) >= 10],
          ['Task time: min · median · max', mt.p50_task_ms != null || mt.max_task_ms != null
            ? [mt.min_task_ms, mt.p50_task_ms, mt.max_task_ms].map((v) => (v == null ? '–' : fmtDuration(v))).join(' · ')
            : null],
        ]}
      />
    </section>
  );
}

function Retries({ cid, m, n }: { cid: string; m: GModel; n: GNode }) {
  const isStage = n.type === 'stage';
  const st = useAsync(
    (s) =>
      isStage
        ? api.datasetOpt<TaskRetryRow>(cid, 'task_retries', { spark_context_id: n.ctx, stage_id: n.stageId, stage_attempt: n.attempt, limit: 20, sort: 'wasted_ms', desc: true }, s)
        : Promise.resolve(null),
    [cid, n.id],
  );
  const incoming = m.edges.find((e) => e.kind === 'retry' && e.target === n.id);
  const outgoing = m.edges.find((e) => e.kind === 'retry' && e.source === n.id);
  const jobStages = n.type === 'job' ? m.stagesByJob.get(n.id) ?? [] : [];
  const retriedStages = jobStages.filter((s) => s.attempt > 0);
  const taskRetries = jobStages.filter((s) => (s.metrics?.retries ?? 0) > 0 || (s.metrics?.failed_tasks ?? 0) > 0);
  const rows = st.data?.rows ?? [];
  if (!incoming && !outgoing && !rows.length && !retriedStages.length && !taskRetries.length && !(n.metrics?.retries ?? 0)) return null;
  return (
    <section className="np-sec">
      <h3>Retries</h3>
      <ul className="np-retries">
        {incoming && (
          <li>
            This is attempt {n.attempt + 1}. Spark ran the stage again because attempt {n.attempt} failed: <b>{truncate(incoming.label ?? 'resubmitted', 300)}</b>.
          </li>
        )}
        {outgoing && (
          <li>
            This attempt {n.vstatus === 'failed' ? 'failed' : 'was resubmitted'}, so Spark retried it as attempt {n.attempt + 2}.{' '}
            <button className="linkish" onClick={() => document.dispatchEvent(new CustomEvent('graph-select', { detail: outgoing.target }))}>
              Show attempt {n.attempt + 2}
            </button>
          </li>
        )}
        {retriedStages.map((s) => (
          <li key={s.id}>
            {s.label} needed attempt {s.attempt + 1}
            {m.edges.find((e) => e.kind === 'retry' && e.target === s.id)?.label ? `: ${truncate(m.edges.find((e) => e.kind === 'retry' && e.target === s.id)!.label!, 140)}` : ''}.
          </li>
        ))}
        {n.type === 'job' && taskRetries.length > 0 && (
          <li>
            {fmtNum(taskRetries.length)} {taskRetries.length === 1 ? 'stage had' : 'stages had'} tasks that failed and were retried:{' '}
            {taskRetries.map((s) => `${s.label}${s.attempt ? `.${s.attempt}` : ''}`).join(', ')}.
          </li>
        )}
        {rows.map((r) => (
          <li key={`${r.task_index}`}>{retrySentence(r)}</li>
        ))}
        {isStage && st.data === null && (n.metrics?.failed_tasks ?? 0) > 0 && (
          <li className="muted">{fmtNum(n.metrics?.failed_tasks)} task attempts failed. Re-analyze with the latest version to see why each was retried.</li>
        )}
      </ul>
    </section>
  );
}

function Findings({ cid, n }: { cid: string; n: GNode }) {
  const params: Record<string, string | number | null> =
    n.type === 'stage'
      ? { spark_context_id: n.ctx, stage_id: n.stageId }
      : n.type === 'job'
        ? { spark_context_id: n.ctx, spark_job_id: n.jobId }
        : n.type === 'query'
          ? { spark_context_id: n.ctx, sql_execution_id: n.execId }
          : n.type === 'app'
            ? { spark_context_id: n.ctx }
            : {};
  const enabled = n.type !== 'connect';
  const st = useAsync((s) => api.dataset<FindingRow>(cid, 'findings', { ...params, limit: 30, sort: 'finding_id' }, s), [cid, n.id], enabled);
  const rows = (st.data?.rows ?? []).filter((f) => n.type !== 'stage' || f.stage_attempt === null || f.stage_attempt === n.attempt);
  if (!enabled || (!st.loading && !rows.length)) return null;
  return (
    <section className="np-sec">
      <h3>Findings {rows.length > 0 && <span className="muted">({fmtNum(rows.length)})</span>}</h3>
      {st.loading && <p className="muted small">Loading findings…</p>}
      <ul className="np-findings">
        {rows.slice(0, 8).map((f) => (
          <li key={f.finding_id}>
            <div className="np-fhead">
              <SeverityBadge sev={f.severity} />
              <Link to={to.findings(cid, f.finding_id)} className="np-fcat">
                {f.category}
              </Link>
            </div>
            {f.evidence && <div className="small ink2">{truncate(f.evidence, 220)}</div>}
            {f.fix && (
              <div className="small">
                <b>Fix:</b> {truncate(f.fix, 200)}
              </div>
            )}
          </li>
        ))}
      </ul>
      {rows.length > 8 && (
        <Link className="small" to={to.findings(cid)}>
          All {fmtNum(rows.length)} findings
        </Link>
      )}
    </section>
  );
}

function Links({ cid, m, n }: { cid: string; m: GModel; n: GNode }) {
  const q = sameCtxQuery(m, n);
  const pad = 30_000;
  const logs = n.start ? to.logs(cid, { ts_from: n.start - pad, ts_to: (n.end ?? n.start) + pad }) : to.logs(cid);
  return (
    <div className="np-links">
      {n.type === 'stage' && (
        <Link className="btn small" to={to.stages(cid, n.ctx, n.stageId, n.attempt)}>
          Stage details
        </Link>
      )}
      {q && q.execId !== null && (
        <Link className="btn small" to={to.query(cid, n.ctx, q.execId)}>
          {n.type === 'query' ? 'Query details and plan' : `${q.label} and plan`}
        </Link>
      )}
      {n.type === 'connect' && (
        <Link className="btn small" to={to.queries(cid)}>
          Queries
        </Link>
      )}
      <Link className="btn small" to={to.timeline(cid, n.ctx)}>
        Timeline
      </Link>
      <Link className="btn small" to={logs} title="Driver and executor logs from 30 s before to 30 s after">
        Logs around this time
      </Link>
      {(n.type === 'job' || n.type === 'stage') && (
        <Link className="btn small" to={to.story(cid, { ctx: n.ctx, q: n.type === 'job' ? `job ${n.jobId}` : `stage ${n.stageId}` })}>
          Story
        </Link>
      )}
    </div>
  );
}

export function NodePanel({ cid, m, n, onClose }: { cid: string; m: GModel; n: GNode; onClose: () => void }) {
  const st = STATUS_META[n.vstatus];
  const parent = n.parent ? m.byId.get(n.parent) : null;
  return (
    <aside className="node-panel panel" aria-label={`${typeLabel[n.type]} details`}>
      <div className="np-head">
        <div style={{ minWidth: 0 }}>
          <div className="np-kicker">
            {typeLabel[n.type]}
            {parent && parent.type === 'job' ? (
              <>
                {' in '}
                <button className="linkish" onClick={() => document.dispatchEvent(new CustomEvent('graph-select', { detail: parent.id }))}>
                  {parent.label}
                </button>
              </>
            ) : null}
          </div>
          <h2 className="wrap-any">
            {n.type === 'connect' ? 'Spark Connect statement' : n.label}
            {n.type === 'stage' && (n.metrics?.attempts ?? 1) > 1 ? <span className="muted">, attempt {n.attempt + 1}</span> : null}
          </h2>
          <div className="np-status">
            {n.vstatus === 'retried' ? (
              <span className="badge sev-medium">
                <span aria-hidden>{st.glyph}</span>
                {st.label}
              </span>
            ) : (
              <StatusBadge status={n.vstatus === 'ok' ? 'succeeded' : n.vstatus === 'failed' ? 'failed' : n.vstatus === 'incomplete' ? 'incomplete' : n.status} />
            )}
            {[...n.flagSet]
              .filter((f) => f !== 'failed' && f !== 'retried')
              .map((f) => (
                <span key={f} className="badge plain">
                  {f === 'shuffle_heavy' ? 'Shuffle-heavy' : f === 'gc' ? 'High GC' : f === 'skew' ? 'Skewed' : f === 'spill' ? 'Spilled' : f}
                </span>
              ))}
          </div>
        </div>
        <button className="btn small ghost" onClick={onClose} aria-label="Close details">
          Close
        </button>
      </div>
      <div className="np-body">
        <WhatItDoes m={m} n={n} />
        <Metrics n={n} />
        <Retries cid={cid} m={m} n={n} />
        <Findings cid={cid} n={n} />
        <Links cid={cid} m={m} n={n} />
      </div>
    </aside>
  );
}
