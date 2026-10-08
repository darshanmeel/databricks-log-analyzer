// Revision 14: what to change. Outliers the data shows (tasks too big for the partition count, spill, files that
// cannot be split, executors paid for but idle, runs sharing cores, no Photon), each with its evidence and the
// setting to change, then the settings themselves: what the cluster set, what code set, and the defaults.
import { useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';
import { api, type Advice, type AdviceStage, type SettingsView } from '../api';
import { useAsync } from '../hooks';
import { fmtBytes, fmtDuration, fmtNum, truncate } from '../format';
import { to } from '../links';
import { inRun } from './TopFinder';
import { Panel } from './ui';

const SEV_LABEL: Record<string, string> = { high: 'Fix', medium: 'Check', info: 'Note' };

export function SettingsAdvice({ cid }: { cid: string }) {
  const st = useAsync((s) => api.settings(cid, s), [cid]);
  if (st.loading || st.error || !st.data) return null;
  return <AdviceBody cid={cid} v={st.data} />;
}

/** One piece of advice as points: what we saw, the likely cause, the fix, and links to what it is about. */
/** Long tables show this many rows first; the rest on demand. */
export const TOP_ROWS = 10;

export function AdviceItem({ cid, a, children, open: open0 = false }: { cid: string; a: Advice; children?: ReactNode; open?: boolean }) {
  // folded to its title and headline fact: the reader picks which to open
  const [open, setOpen] = useState(open0);
  const [allRuns, setAllRuns] = useState(false);
  const [why, setWhy] = useState(false);
  const facts = a.facts?.length ? a.facts : [a.evidence];
  const fixes = a.fixes?.length ? a.fixes : [a.change];
  const runs = a.runs ?? [];
  const shownRuns = allRuns ? runs : runs.slice(0, 6);
  return (
    <div className={`advice adv-${a.severity} ${open ? 'open' : 'folded'}`}>
      <button type="button" className="advice-head advice-toggle" onClick={() => setOpen(!open)} aria-expanded={open}>
        <span className="fold-arrow" aria-hidden>{open ? '▾' : '▸'}</span>
        <span className={`adv-sev ${a.severity}`}>{SEV_LABEL[a.severity]}</span>
        <b>{a.title}</b>
        {a.key ? <code className="adv-key">{a.key.replace('spark.databricks.clusterUsageTags.', 'cluster: ')}</code> : null}
      </button>
      {!open && <div className="advice-peek small muted">{facts[0]}</div>}
      {open && <>
      <dl className="advice-grid small">
        {/* the headline fact and the fix; the rest of what we saw and the likely cause on demand */}
        <dt>What we saw</dt>
        <dd>
          <ul>{(why ? facts : facts.slice(0, 1)).map((f, i) => <li key={i}>{f}</li>)}</ul>
          {facts.length > 1 && !why && (
            <button className="linkish small" onClick={() => setWhy(true)}>
              {facts.length - 1} more {facts.length === 2 ? 'fact' : 'facts'} ▸
            </button>
          )}
        </dd>
        {a.cause ? <><dt>Likely cause</dt><dd>{a.cause}</dd></> : null}
        <dt>Fix</dt>
        <dd><ul className="adv-fix">{fixes.map((f, i) => <li key={i}>{f}</li>)}</ul></dd>
        {(a.stages?.length || runs.length || a.queries?.length) ? (
          <>
            <dt>Open</dt>
            <dd className="stack" style={{ gap: 4 }}>
              {a.queries && a.queries.length > 0 && (
                <div className="adv-links">
                  {a.queries.map((q) => (
                    <Link key={`${q.spark_context_id}.${q.sql_execution_id}`} className="tchip" to={inRun(to.query(cid, q.spark_context_id, q.sql_execution_id), q.run_key)}
                      title={q.label ?? undefined}>Query {q.sql_execution_id}{q.label ? <span className="muted"> · {truncate(q.label, 28)}</span> : null}</Link>
                  ))}
                </div>
              )}
              {runs.length > 0 && (
                <div className="adv-links">
                  <span className="muted">{fmtNum(runs.length)} {runs.length === 1 ? 'run' : 'runs'}:</span>
                  {shownRuns.map((r) => <Link key={r.run_key} className="tchip" to={inRun(to.overview(cid), r.run_key)}>{truncate(r.label ?? r.run_key, 36)}</Link>)}
                  {runs.length > shownRuns.length && <button className="btn small ghost" onClick={() => setAllRuns(true)}>+{runs.length - shownRuns.length} more</button>}
                </div>
              )}
              {a.stages && a.stages.length > 0 && <AdviceStages cid={cid} stages={a.stages} count={a.stage_count ?? a.stages.length} />}
            </dd>
          </>
        ) : null}
      </dl>
      {children}
      </>}
    </div>
  );
}

/** The stages a piece of advice is about, biggest first; each opens in its run. */
function AdviceStages({ cid, stages: every, count }: { cid: string; stages: AdviceStage[]; count: number }) {
  const b = (v: number | null | undefined) => (v ? fmtBytes(v) : '–');
  const [all, setAll] = useState(false);
  const stages = all ? every : every.slice(0, TOP_ROWS);
  return (
    <details className="small">
      <summary>Open {count === 1 ? 'the stage' : `the ${fmtNum(count)} stages`}{count > every.length ? ` (the ${every.length} biggest)` : ''}</summary>
      <div className="table-wrap" style={{ overflowX: 'auto', marginTop: 6 }}>
        <table className="table compact level-table">
          <thead><tr><th>Stage</th><th>Run</th><th className="num">Took</th><th className="num">Tasks</th><th className="num">Read</th><th className="num">Shuffle read</th><th className="num">Biggest task read</th><th className="num">Disk spill</th></tr></thead>
          <tbody>
            {stages.map((s) => (
              <tr key={`${s.spark_context_id}.${s.stage_id}.${s.stage_attempt}`}>
                <td>
                  <Link to={inRun(to.stages(cid, s.spark_context_id, s.stage_id, s.stage_attempt), s.run_key)}><b>Stage {s.stage_id}{s.stage_attempt ? `.${s.stage_attempt}` : ''}</b></Link>
                  <div className="muted">{[s.spark_job_id !== null ? `job ${s.spark_job_id}` : null, s.sql_execution_id !== null ? `query ${s.sql_execution_id}` : null].filter(Boolean).join(' · ')}</div>
                </td>
                <td>{s.run_key ? <Link to={inRun(to.overview(cid), s.run_key)}>{truncate(s.run_label ?? s.run_key, 40)}</Link> : <span className="muted">–</span>}</td>
                <td className="num">{fmtDuration(s.duration_ms)}</td>
                <td className="num">{fmtNum(s.tasks)}</td>
                <td className="num">{b(s.input_bytes)}</td>
                <td className="num" style={{ color: s.shuffle_read ? 'var(--shuf)' : undefined }}>{b(s.shuffle_read)}</td>
                <td className="num">{b(s.max_task_bytes_in)}</td>
                <td className="num" style={{ color: s.disk_spill ? 'var(--spill)' : undefined }}>{b(s.disk_spill)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {every.length > TOP_ROWS && <button className="linkish small" onClick={() => setAll(!all)}>{all ? `Only the top ${TOP_ROWS} ↑` : `All ${fmtNum(every.length)} ↓`}</button>}
    </details>
  );
}

function AdviceBody({ cid, v }: { cid: string; v: SettingsView }) {
  return (
    <Panel
      title="What to change"
      note="Outliers in this cluster's data, the evidence, and the setting that decides it. Highest impact first."
    >
      <div className="stack" style={{ gap: 10 }}>
        {v.advice.length ? (
          v.advice.map((a, i) => <AdviceItem key={i} cid={cid} a={a} />)
        ) : (
          <p className="muted small" style={{ margin: 0 }}>Nothing stood out: tasks were sized well, little spill, executors were busy.</p>
        )}
        <SettingsTable v={v} />
      </div>
    </Panel>
  );
}

/** The settings that decide speed and cost: what the cluster set, what code set, and the defaults. */
export function SettingsTable({ v }: { v: SettingsView }) {
  const [open, setOpen] = useState(false);
  const groups = [...new Set(v.settings.map((s) => s.group))];
  return (
    <>
        {v.settings.length > 0 && (
        <details open={open} onToggle={(e) => setOpen((e.target as HTMLDetailsElement).open)}>
          <summary className="small">
            Settings that decide speed and cost ({v.settings.filter((s) => s.source !== 'default').length} set, the rest default)
            {v.env_vars !== null ? ` · ${v.env_vars} environment variables (their values are not in the logs)` : ''}
          </summary>
          <div className="table-wrap" style={{ overflowX: 'auto', marginTop: 8 }}>
            <table className="table">
              <thead>
                <tr><th>Setting</th><th>Value</th><th>Where from</th><th>What it decides</th></tr>
              </thead>
              <tbody>
                {groups.flatMap((g) => [
                  <tr key={`g${g}`}><td colSpan={4} className="small" style={{ fontWeight: 700, paddingTop: 10 }}>{g}</td></tr>,
                  ...v.settings.filter((s) => s.group === g).map((s) => (
                    <tr key={s.key}>
                      <td className="mono small">{s.label}</td>
                      <td className="mono small">
                        {s.value ?? s.default ?? '–'}
                        {s.session_values && s.session_values.length > 0 && s.cluster_value !== null ? (
                          <div className="st-warn">set in code: {s.session_values.join(', ')}</div>
                        ) : null}
                      </td>
                      <td className="small">{s.source === 'code' ? 'set in code' : s.source === 'cluster' ? 'cluster config' : <span className="muted">default</span>}</td>
                      <td className="small muted">{s.what}</td>
                    </tr>
                  )),
                ])}
              </tbody>
            </table>
          </div>
        </details>
      )}
    </>
  );
}
