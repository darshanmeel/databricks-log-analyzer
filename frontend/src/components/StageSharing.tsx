// Revision 13: was this stage slow, or short of cores? What else ran on its executors while it ran: other stages of
// the same run, and other runs (task runs of a job, notebooks) that shared the cluster, with their share of the cores.
import { Link } from 'react-router-dom';
import type { StageSharing as Sharing } from '../api';
import { fmtDuration, fmtNum, fmtPct, fmtTime } from '../format';
import { to } from '../links';
import { runName } from '../runName';
import { useRunScopeCtx } from './Shell';

export function StageSharing({ cid, ctx, sh }: { cid: string; ctx: string; sh: Sharing }) {
  const { runs } = useRunScopeCtx();
  const slot = sh.slot_ms ?? 0;
  const own = slot ? sh.own_task_ms / slot : null;
  const other = slot ? sh.other_task_ms / slot : null;
  const name = (rk: string | null) => {
    const r = runs.find((x) => x.run_key === rk);
    return r ? runName(r) : rk ?? 'no run';
  };
  const busy = other !== null && other >= 0.5 && (own ?? 0) < other;
  return (
    <div className="stack" style={{ gap: 8 }}>
      <p style={{ margin: 0 }}>
        While it ran ({fmtTime(sh.start)} → {fmtTime(sh.end)}, {fmtDuration(sh.end - sh.start)}), its {sh.executors.length} executor
        {sh.executors.length === 1 ? '' : 's'}
        {sh.cores ? ` (${sh.cores} cores)` : ''}{' '}
        {sh.other_stages ? (
          <>
            also ran <b>{fmtNum(sh.other_stages)} other stages</b>
            {sh.other_runs ? <> from <b>{sh.other_runs} other runs</b></> : ' of the same run'}.
          </>
        ) : (
          'ran nothing else: it had them to itself.'
        )}
        {own !== null && other !== null && sh.other_stages ? (
          <>
            {' '}This stage used <b>{fmtPct(own, 0)}</b> of their core time, the others <b className={busy ? 'st-warn' : undefined}>{fmtPct(other, 0)}</b>
            {busy ? '. It was short of cores more than slow: the other work held the executors.' : '.'}
          </>
        ) : null}
      </p>
      {own !== null && other !== null && sh.other_stages > 0 && (
        <div className="share-bar" title="Core time of the stage's executors while it ran" style={{ display: 'flex', height: 10, borderRadius: 3, overflow: 'hidden', background: 'var(--line)' }}>
          <span style={{ width: `${Math.min(100, own * 100)}%`, background: 'var(--series-1)' }} />
          <span style={{ width: `${Math.min(100 - Math.min(100, own * 100), other * 100)}%`, background: 'var(--st-warn)' }} />
        </div>
      )}
      {sh.rows.length > 0 && (
        <table className="tbl small">
          <thead>
            <tr>
              <th>Ran at the same time</th>
              <th>Run</th>
              <th className="num">Tasks</th>
              <th className="num">Core time</th>
              <th className="num">Share</th>
            </tr>
          </thead>
          <tbody>
            {sh.rows.map((r) => (
              <tr key={`${r.stage_id}.${r.stage_attempt}`}>
                <td>
                  <Link to={to.stages(cid, ctx, r.stage_id, r.stage_attempt)}>Stage {r.stage_id}{r.stage_attempt ? `.${r.stage_attempt}` : ''}</Link>
                  {r.sql_execution_id !== null ? <span className="muted"> · query {r.sql_execution_id}</span> : r.spark_job_id !== null ? <span className="muted"> · job {r.spark_job_id}</span> : null}
                </td>
                <td className={r.same_run ? 'muted' : undefined}>{r.same_run ? 'this run' : name(r.run_key)}</td>
                <td className="num">{fmtNum(r.tasks)}</td>
                <td className="num">{fmtDuration(r.task_ms)}</td>
                <td className="num">{slot && r.task_ms ? fmtPct(r.task_ms / slot, 0) : '–'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
