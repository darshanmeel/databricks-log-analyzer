import { useState } from 'react';
import { Link } from 'react-router-dom';
import type { ConnectOperationRow } from '../api';
import { fmtDuration, fmtNum, fmtTs, truncate } from '../format';
import { to } from '../links';

/** Spark Connect operation status → the shared status badge vocabulary. */
export function ConnectStatus({ status }: { status: string | null | undefined }) {
  const s = (status ?? '').toLowerCase();
  if (s === 'failed')
    return (
      <span className="badge sev-high">
        <span aria-hidden>✕</span>Failed
      </span>
    );
  if (s === 'finished' || s === 'closed' || s === 'succeeded')
    return (
      <span className="badge good">
        <span aria-hidden>✓</span>Finished
      </span>
    );
  if (s === 'canceled' || s === 'cancelled')
    return (
      <span className="badge sev-medium">
        <span aria-hidden>■</span>Canceled
      </span>
    );
  return (
    <span className="badge sev-low">
      <span aria-hidden>◐</span>
      {s === 'open' ? 'Still open' : status ?? 'Unknown'}
    </span>
  );
}

export const opUser = (o: ConnectOperationRow) => o.user_name || o.user_id || null;

export interface OpJob {
  spark_context_id: string;
  spark_job_id: number;
  failed?: boolean;
}

/** One Spark Connect statement: who ran it, how long, status, the statement text, and the Spark jobs it caused. */
export function ConnectOpRow({ cid, op, jobs }: { cid: string; op: ConnectOperationRow; jobs: OpJob[] }) {
  const [open, setOpen] = useState(false);
  const text = op.statement_text ?? '';
  const long = text.length > 600;
  return (
    <div className="op-row">
      <div className="op-head">
        <ConnectStatus status={op.status} />
        <b style={{ color: 'var(--ink)' }}>{fmtDuration(op.duration_ms)}</b>
        {opUser(op) && <span>by {opUser(op)}</span>}
        <span className="muted">started {fmtTs(op.start_time)} UTC</span>
        <span className="grow" />
        <span className="mono small muted" title={`operation ${op.operation_id}${op.session_id ? `\nsession ${op.session_id}` : ''}`}>
          {op.operation_id.slice(0, 8)}
        </span>
      </div>
      {text ? (
        <pre className="stmt">{long && !open ? truncate(text, 600) : text}</pre>
      ) : (
        <span className="muted small">No statement text was recorded for this operation.</span>
      )}
      {long && (
        <button className="btn small ghost" style={{ alignSelf: 'flex-start' }} onClick={() => setOpen(!open)}>
          {open ? 'Show less' : 'Show the whole statement'}
        </button>
      )}
      {op.error && (
        <div className="small" style={{ color: 'var(--sev-high-text)' }}>
          {truncate(op.error, 600)}
        </div>
      )}
      <div className="chips">
        {jobs.length === 0 ? (
          <span className="muted small">No Spark job is linked to this statement.</span>
        ) : (
          <>
            <span className="muted small">Caused {fmtNum(jobs.length)} Spark {jobs.length === 1 ? 'job' : 'jobs'}:</span>
            {jobs.slice(0, 20).map((j) => (
              <Link key={`${j.spark_context_id}|${j.spark_job_id}`} className="chip" to={to.hierarchy(cid, { ctx: j.spark_context_id, job: j.spark_job_id })}>
                <span className="k">Job</span>
                {j.spark_job_id}
                {j.failed ? ' failed' : ''}
              </Link>
            ))}
            {jobs.length > 20 && <span className="muted small">and {fmtNum(jobs.length - 20)} more</span>}
          </>
        )}
      </div>
    </div>
  );
}
