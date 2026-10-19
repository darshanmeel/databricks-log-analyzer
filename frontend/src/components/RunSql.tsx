// Revision 23: a run as SQL. Every query of the run in the order it started, as the SQL its plan amounts to (rebuilt
// from the physical plan: the scans, the filters, the windows and what they keep per key, the groups, the joins), with
// its logic as chips; a failed query first, with its error and, when the plans say why (a MERGE whose source kept two
// rows per key), the cause.
import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, type RunSql, type RunSqlQuery } from '../api';
import { useAsync } from '../hooks';
import { fmtTime, truncate } from '../format';
import { to } from '../links';
import { hasLogic, LogicChips, REBUILT_NOTE, SqlBlock } from './QueryLogic';
import { Panel, StatusBadge } from './ui';

/** The run's queries as SQL, loaded once for the overview and its step list (none for no run). */
export function useRunSql(cid: string, run: string | null) {
  return useAsync((s) => (run ? api.runSql(cid, run, s).catch(() => null) : Promise.resolve(null)), [cid, run]);
}

/** Why the run failed, from the plans of its failed queries: one box per cause. */
export function FailureCause({ cid, d }: { cid: string; d: RunSql | null | undefined }) {
  const causes = (d?.queries ?? []).filter((q) => q.cause && !q.cause.startsWith('The same failure as'));
  if (!causes.length) return null;
  return (
    <div className="stack" style={{ gap: 8 }}>
      {causes.map((q) => (
        <div key={`${q.ctx}:${q.id}`} className="cause-box">
          <b>Likely why <Link to={to.query(cid, q.ctx, q.id)}>query {q.id}</Link> failed:</b> {q.cause}
          {q.error && <div className="muted small mono" style={{ marginTop: 4 }}>{truncate(q.error.split('\n')[0], 220)}</div>}
          <div className="muted small" style={{ marginTop: 4 }}>Read from the error and the plans of its queries, to the best of our knowledge: it may not be 100% correct. Check it against the code.</div>
        </div>
      ))}
    </div>
  );
}

const label = (q: RunSqlQuery) => {
  const m = q.description.match(/MERGE operation - (?:MERGE operation - )?(.+)$/);
  return m ? `MERGE: ${m[1].replace(/^Rewriting [\d,]+ files\b.*/i, 'write the changes')}` : q.op;
};

export function RunSqlPanel({ cid, d }: { cid: string; d: RunSql | null | undefined }) {
  const qs = useMemo(() => (d?.queries ?? []).filter((q) => q.sql), [d]);
  const key = (q: RunSqlQuery) => `${q.ctx}:${q.id}`;
  // a query with a known cause opens at first; the rest on a click
  const [open, setOpen] = useState<Set<string>>(() => new Set());
  useEffect(() => setOpen(new Set(qs.filter((q) => q.cause).map(key))), [qs]);
  const [onlyLogic, setOnlyLogic] = useState(true);
  const [copied, setCopied] = useState(false);
  if (!d || !qs.length) return null;
  // a query that filters, ranks, groups or joins is what the run's code decided; the rest only moves rows
  const shown = onlyLogic ? qs.filter((q) => hasLogic(q.logic) || !!q.error) : qs;
  const all = `-- ${REBUILT_NOTE}\n\n` + shown.map((q) => `-- Query ${q.id} · ${label(q)}${q.status && q.status !== 'succeeded' ? ` · ${q.status}` : ''}\n${q.sql}`).join('\n\n');
  const flip = (k: string) => setOpen((o) => { const s = new Set(o); if (s.has(k)) s.delete(k); else s.add(k); return s; });
  const copyAll = () => {
    if (!navigator.clipboard) return;
    navigator.clipboard.writeText(all).then(() => { setCopied(true); setTimeout(() => setCopied(false), 1500); }, () => undefined);
  };
  return (
    <Panel title="The run as SQL"
      note={`Each query of the run in the order it started, as the SQL its physical plan amounts to: what it read, filtered, ranked and kept per key, grouped and joined. Spark's own steps (shuffles, query stages) are left out; Delta's bookkeeping is a comment. ${REBUILT_NOTE}`}
      actions={
        <div className="row small" style={{ gap: 8 }}>
          <label className="row" style={{ gap: 4 }}>
            <input type="checkbox" checked={onlyLogic} onChange={(e) => setOnlyLogic(e.target.checked)} /> only queries that filter, rank, group or join
          </label>
          <button className="btn small ghost" onClick={() => setOpen(new Set(shown.map(key)))}>Open all</button>
          <button className="btn small ghost" onClick={() => setOpen(new Set())}>Close all</button>
          {navigator.clipboard && <button className="btn small ghost" onClick={copyAll}>{copied ? 'Copied' : 'Copy all'}</button>}
        </div>
      }>
      {shown.length === 0 && <p className="muted small">No query of this run filters, ranks, groups or joins. Untick the box to see them all.</p>}
      {shown.map((q) => {
        const k = key(q);
        const isOpen = open.has(k);
        return (
          <div key={k} className="run-sql-q">
            <div className="run-sql-head" onClick={() => flip(k)} role="button" aria-expanded={isOpen}>
              <span className="muted" aria-hidden>{isOpen ? '▾' : '▸'}</span>
              <Link to={to.query(cid, q.ctx, q.id)} onClick={(e) => e.stopPropagation()}><b>Query {q.id}</b></Link>
              <span>{label(q)}</span>
              {q.status && q.status !== 'succeeded' && q.status !== 'completed' ? <StatusBadge status={q.status} /> : null}
              <span className="muted small mono">{q.start != null ? fmtTime(q.start) : ''}</span>
            </div>
            <LogicChips l={q.logic} />
            {q.cause && <div className="cause-box small" style={{ marginTop: 6 }}>{q.cause}</div>}
            {isOpen && <div style={{ marginTop: 6 }}><SqlBlock sql={q.sql!} /></div>}
          </div>
        );
      })}
      {onlyLogic && shown.length < qs.length && (
        <p className="muted small" style={{ marginTop: 8 }}>{qs.length - shown.length} more queries only read, move or write rows: untick the box to see them.</p>
      )}
    </Panel>
  );
}
