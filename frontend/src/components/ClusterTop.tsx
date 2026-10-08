// Revision 20: the cluster Overview laid out like a run's: its numbers, why runs were slow and how they ended (side by
// side), where the runs' time went (the few runs that matter), the causes and the fix, the programs and the errors,
// and its problems as points. The runs themselves are ranked in Find and listed in full below.
import { Fragment, useMemo, useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';
import { api, type Advice, type Causes, type ComputeUse, type ErrorGroup, type FindingRow, type RunRow } from '../api';
import { useAsync } from '../hooks';
import { fmtBytes, fmtDuration, fmtNum, fmtPct, fmtRows, fmtTime, truncate } from '../format';
import { to } from '../links';
import { runGroup, runId, runName, usualX } from '../runName';
import { isSlow, useCluster } from './Shell';
import { Fold } from './ui';
import { FindingPoints, depthOf, kindName, plainFirst } from './FindingPoints';
import { AdviceItem, SettingsTable } from './SettingsAdvice';
import { ClusterTables } from './TableStats';
import { inRun, WaitRanBar } from './TopFinder';
import { errorClass } from '../errorClass';
import { CauseBar, CauseHeadline, CauseLegend, causeOf, causeParts, type CauseKey } from './CauseBar';

const TOP = 5;
const GB = 1024 ** 3;
const BAD_GONE = ['oom', 'lost', 'killed'];

export interface TopExecutor { executor_id: string; cores: number | null; removal_category: string | null; removed_time: number | null }

interface Program {
  name: string; runs: RunRow[]; failed: number; slow: number; running: number; waiting: number; took: number;
  spill: number; read: number; usual: number | null; worst: RunRow;
}

const sum = (rs: RunRow[], f: (r: RunRow) => number | null | undefined) => rs.reduce((a, r) => a + (f(r) ?? 0), 0);


function programs(runs: RunRow[]): Program[] {
  const m = new Map<string, RunRow[]>();
  for (const r of runs) m.set(runGroup(r), [...(m.get(runGroup(r)) ?? []), r]);
  return [...m].map(([name, rs]) => ({
    name, runs: rs, failed: rs.filter((r) => r.status === 'failed').length, slow: rs.filter(isSlow).length,
    running: sum(rs, (r) => r.running_ms), waiting: sum(rs, (r) => r.waiting_ms), took: sum(rs, (r) => r.duration_ms),
    spill: sum(rs, (r) => r.disk_spill), read: sum(rs, (r) => (r.input_bytes ?? 0) + (r.shuffle_read ?? 0)),
    usual: rs.find((r) => r.typical_duration_ms)?.typical_duration_ms ?? null,
    worst: [...rs].sort((a, b) => (usualX(b) ?? 0) - (usualX(a) ?? 0) || (b.duration_ms ?? 0) - (a.duration_ms ?? 0))[0],
  })).sort((a, b) => b.running - a.running);
}

/** Most runs at once. */
function peakOf(runs: RunRow[]) {
  const ev = runs.flatMap((r) => [[r.start_time ?? 0, 1], [r.end_time ?? r.start_time ?? 0, -1]] as [number, number][]).sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  let n = 0, best = 0, at = 0;
  for (const [t, k] of ev) { n += k; if (n > best) { best = n; at = t; } }
  return { n: best, at };
}

function Tile({ label, value, foot, tone }: { label: string; value: string; foot?: ReactNode; tone?: 'bad' | 'warn' | 'spill' | 'shuf' }) {
  return (
    <div className={`kpi ${tone ?? ''}`}>
      <div className="label">{label}</div>
      <div className="value">{value}</div>
      {foot ? <div className="foot">{foot}</div> : null}
    </div>
  );
}

/** The cluster's first screen (why runs were slow, how they ended, the runs that took longest, the top changes) and,
 * folded under "Details", everything else: the numbers, programs, errors, tables, the ranking and `more` (the
 * timeline and the full runs table). */
export function ClusterTop({ cid, runs, executors, compute, onPick, find, more, atBox, causes, runCauses }: {
  cid: string; runs: RunRow[]; executors: TopExecutor[]; compute: ComputeUse | null; onPick: (r: RunRow) => void; find: ReactNode; more?: ReactNode; atBox?: ReactNode;
  causes?: Causes | null; runCauses?: Record<string, Causes | null>;
}) {
  const progs = useMemo(() => programs(runs), [runs]);
  const peak = useMemo(() => peakOf(runs), [runs]);
  const took = sum(runs, (r) => r.duration_ms);
  const waiting = sum(runs, (r) => r.waiting_ms);
  const running = sum(runs, (r) => r.running_ms);
  const waitShare = waiting / Math.max(1, took);
  const failed = runs.filter((r) => r.status === 'failed');
  const slow = runs.filter(isSlow);
  const retried = runs.filter((r) => r.status === 'succeeded' && ((r.failed_tasks ?? 0) > 0 || (r.failed_stages ?? 0) > 0));
  const spill = sum(runs, (r) => r.disk_spill);
  const shufR = sum(runs, (r) => r.shuffle_read), shufW = sum(runs, (r) => r.shuffle_write);
  const lost = executors.filter((e) => BAD_GONE.includes(e.removal_category ?? ''));
  const cores = Math.max(0, ...[executors.reduce((a, e) => a + (e.cores ?? 0), 0)]);
  const t0 = Math.min(...runs.map((r) => r.start_time ?? Infinity));
  const t1 = Math.max(...runs.map((r) => r.end_time ?? r.start_time ?? 0));

  const fs = useAsync((s) => api.datasetOpt<FindingRow & { run_key?: string | null }>(cid, 'findings', { limit: 500, run: '' }, s), [cid]);
  const use = useCluster().summary.core_use;
  const findings = useMemo(() => [...(fs.data?.rows ?? [])].sort(plainFirst), [fs.data]);
  const errs = useAsync((s) => api.errors(cid, s).catch(() => [] as ErrorGroup[]), [cid]);

  return (
    <>
      <div className="kpis">
        <Tile label="Runs" value={fmtNum(runs.length)} foot={failed.length ? `${fmtNum(failed.length)} failed` : retried.length ? `none failed · ${fmtNum(retried.length)} after retries` : 'none failed'}
          tone={failed.length ? 'bad' : undefined} />
        <Tile label="Slower than usual" value={fmtNum(slow.length)} foot="2× their usual or more" tone={slow.length ? 'warn' : undefined} />
        <Tile label="At once" value={fmtNum(peak.n)} foot={`at ${fmtTime(peak.at).slice(0, 5)} on ${fmtNum(executors.length)} executors`} tone={peak.n >= 10 ? 'warn' : undefined} />
        <Tile label="Waiting for cores" value={fmtDuration(waiting)} foot={`${fmtPct(waitShare, 0)} of all run time`} tone={waitShare >= 0.2 ? 'warn' : undefined} />
        <Tile label="Tasks" value={fmtNum(sum(runs, (r) => r.tasks))} foot={sum(runs, (r) => r.failed_tasks) ? `${fmtNum(sum(runs, (r) => r.failed_tasks))} attempts failed` : 'none failed'}
          tone={sum(runs, (r) => r.failed_tasks) ? 'warn' : undefined} />
        <Tile label="Spill" value={fmtBytes(spill)} foot="to disk, all runs" tone={spill >= GB ? 'spill' : undefined} />
        <Tile label="Shuffle" value={fmtBytes(Math.max(shufR, shufW))} foot={`read ${fmtBytes(shufR)} · write ${fmtBytes(shufW)}`} tone={shufR >= GB ? 'shuf' : undefined} />
        <Tile label="Idle compute" value={compute?.idle_share != null ? fmtPct(compute.idle_share, 0) : '–'}
          foot={<span title={use ? `Worker core-seconds paid for (${fmtNum(use.worker_core_s)}) per core-second of successful tasks (${fmtNum(use.useful_task_s)})` : undefined}>
            {compute ? `of ${(compute.core_ms_up / 3_600_000).toFixed(1)} core-hours up` : 'paid for, no tasks'}{use ? ` · ${use.core_s_per_useful} core-s per useful core-s` : ''}</span>} tone={(compute?.idle_share ?? 0) >= 0.3 ? 'warn' : undefined} />
      </div>

      <div className="ro-hero">
        <section className="ro-hero-box">
          <span className="ro-eyebrow warn">{slow.length || waitShare >= 0.2 ? 'Why runs were slow' : 'Where the time went'}</span>
          <WhySlow cid={cid} progs={progs} waiting={waiting} took={took} running={running} peak={peak.n} execs={executors.length} cores={cores} slow={slow} />
        </section>
        <section className="ro-hero-box">
          <span className={`ro-eyebrow ${failed.length || lost.length ? 'bad' : 'warn'}`}>How the runs ended</span>
          <HowEnded cid={cid} runs={runs} failed={failed} retried={retried} lost={lost} compute={compute} t1={t1} onPick={onPick} />
        </section>
      </div>

      {atBox}
      <TimeWent cid={cid} runs={runs} took={took} waiting={waiting} running={running} t0={t0} t1={t1} compute={compute} peak={peak.n} causes={causes ?? null} runCauses={runCauses ?? {}} />
      <ChangeList cid={cid} rows={findings} loading={fs.loading} runs={runs} />

      <Fold name="cluster" title="Details" ids={['cv-find', 'cv-runs', 'cv-errors']}
        what={[
          'Find the runs, queries, stages, jobs or tasks that took the most time, read or spilled the most, or were most skewed',
          `Programs (runs of the same code) and the ${errs.data?.length ? fmtNum(errs.data.length) + ' ' : ''}errors in the logs`,
          'Tables read and written',
          `Executors busy and idle, and all ${fmtNum(runs.length)} runs over time and in one table`,
        ]}>
        <div id="cv-find">{find}</div>
        <div className="ro-two">
          <Programs cid={cid} progs={progs} />
          <Errors cid={cid} rows={errs.data ?? null} runs={runs} />
        </div>
        <ClusterTables cid={cid} />
        {more}
      </Fold>
    </>
  );
}

function WhySlow({ cid, progs, waiting, took, running, peak, execs, cores, slow }: {
  cid: string; progs: Program[]; waiting: number; took: number; running: number; peak: number; execs: number; cores: number; slow: RunRow[];
}) {
  const share = waiting / Math.max(1, took);
  const top = progs[0];
  const topShare = top ? top.running / Math.max(1, running) : 0;
  const worst = [...slow].sort((a, b) => (usualX(b) ?? 0) - (usualX(a) ?? 0))[0];
  return (
    <>
      <ul className="ro-points">
        {share >= 0.1 && <li><b>{fmtDuration(waiting)} waiting for a free core</b> <span className="muted">· {fmtPct(share, 0)} of {fmtDuration(took)}</span></li>}
        {share >= 0.1 && <li>Up to <b>{fmtNum(peak)} runs at once</b> <span className="muted">on {fmtNum(execs)} executors{cores ? ` (${fmtNum(cores)} cores)` : ''}</span></li>}
        {top && <li><b>{top.name}</b> {topShare >= 0.4 ? 'did most of the work' : 'did the most work'} <span className="muted">· {fmtDuration(top.running)} over {fmtNum(top.runs.length)} runs{top.spill >= GB ? `, spilled ${fmtBytes(top.spill)}` : ''}</span></li>}
        {worst && <li><b>{slow.length === 1 ? 'One run' : `${fmtNum(slow.length)} runs`} 2× usual or more</b>; worst <Link to={inRun(to.overview(cid), worst.run_key)}>{runName(worst)}</Link> <span className="muted">· {fmtDuration(worst.duration_ms)}, {usualX(worst)!.toFixed(1)}×</span></li>}
        {!top && !worst && share < 0.1 ? <li>No run was slow: none waited long for cores, none took twice its usual time</li> : null}
      </ul>
      <div className="ro-links">
        <a href="#cv-time">Where the time went ↓</a>
        <a href="#cv-change">What to change ↓</a>
        <a href="#cv-runs">All runs ↓</a>
      </div>
    </>
  );
}

function HowEnded({ cid, runs, failed, retried, lost, compute, t1, onPick }: {
  cid: string; runs: RunRow[]; failed: RunRow[]; retried: RunRow[]; lost: TopExecutor[]; compute: ComputeUse | null; t1: number; onPick: (r: RunRow) => void;
}) {
  const end = compute?.cluster_end ?? null;
  const gap = end !== null && t1 ? end - t1 : null;
  const incomplete = runs.filter((r) => r.status !== 'failed' && r.status !== 'succeeded');
  return (
    <>
      <ul className="ro-points">
        {failed.length
          ? <li className="bad"><b>{fmtNum(failed.length)} of {fmtNum(runs.length)} runs failed in Spark</b>: {failed.slice(0, 3).map((r, i) => <Fragment key={r.run_key}>{i ? ', ' : ''}<Link to={inRun(to.overview(cid), r.run_key)}>{runName(r)}</Link> at {fmtTime(r.end_time).slice(0, 5)}</Fragment>)}{failed.length > 3 ? ` and ${failed.length - 3} more` : ''}</li>
          : <li><b>No failed run</b> <span className="muted">· all {fmtNum(runs.length - incomplete.length)} finished</span></li>}
        {retried.length ? <li>{fmtNum(retried.length)} finished only after failed tasks were retried</li> : null}
        {incomplete.length ? <li>{fmtNum(incomplete.length)} {incomplete.length === 1 ? 'run has' : 'runs have'} no end in the logs <span className="muted">(still going, or cut off)</span></li> : null}
        {lost.length ? <li className="bad"><b>{fmtNum(lost.length)} executors lost, killed or out of memory</b></li> : <li>No executor lost, killed or out of memory</li>}
        {gap !== null && gap >= 0 ? <li>Cluster stopped {gap >= 60_000 ? fmtDuration(gap) : `${Math.round(gap / 1000)} s`} after the last run, at <b className="mono">{fmtTime(end)}</b></li> : null}
      </ul>
      <div className="ro-links">
        {failed[0] && <button className="linkish" onClick={() => onPick(failed[0])}>Open the first failed run →</button>}
        <Link to={to.errors(cid)}>Errors →</Link>
        <Link to={to.executors(cid)}>Executors →</Link>
      </div>
    </>
  );
}

/** Wall-clock time with at least one run going. */
function busyClock(runs: RunRow[], t1: number) {
  const iv = runs.filter((r) => r.start_time != null).map((r) => [r.start_time!, r.end_time ?? t1] as [number, number]).sort((a, b) => a[0] - b[0]);
  let busy = 0, s = 0, e = -Infinity;
  for (const [a, b] of iv) {
    if (a > e) { if (e > s) busy += e - s; s = a; e = b; } else e = Math.max(e, b);
  }
  return e > s ? busy + e - s : busy;
}

/** Run time summed over runs against the cluster's own clock: runs overlap, so the sum can be days on a cluster up for hours. */
function ClockLine({ runs, took, t0, t1, compute, peak }: { runs: RunRow[]; took: number; t0: number; t1: number; compute: ComputeUse | null; peak: number }) {
  const up = compute?.cluster_start && compute?.cluster_end ? compute.cluster_end - compute.cluster_start : null;
  const busy = busyClock(runs, t1);
  if (!busy || runs.length < 2) return null;
  const avg = took / busy;
  return (
    <div className="note">
      <b>{fmtDuration(took)}</b> is run time added up over {fmtNum(runs.length)} runs. On the clock{up ? <>, the cluster was up <b>{fmtDuration(up)}</b> and</> : ','} runs were going
      for <b>{fmtDuration(busy)}</b>{up ? ` (${fmtPct(Math.min(1, busy / up), 0)} of it)` : ` (${fmtTime(t0).slice(0, 5)} to ${fmtTime(t1).slice(0, 5)})`}: {avg >= 1.05
        ? <>about <b>{avg.toFixed(1)} runs at once</b> on average, up to {fmtNum(peak)}.</>
        : <>they mostly ran one after another.</>}
    </div>
  );
}

function TimeWent({ cid, runs, took, waiting, running, t0, t1, compute, peak, causes, runCauses }: {
  cid: string; runs: RunRow[]; took: number; waiting: number; running: number; t0: number; t1: number; compute: ComputeUse | null; peak: number; causes: Causes | null; runCauses: Record<string, Causes | null>;
}) {
  const span = Math.max(1, t1 - t0);
  const pct = (v: number) => `${Math.min(100, Math.max(0, ((v - t0) / span) * 100))}%`;
  const width = (x: number, y: number) => `${Math.max(0.6, ((Math.min(y, t1) - Math.max(x, t0)) / span) * 100)}%`;
  const out = Math.max(0, took - waiting - running);
  const [by, setBy] = useState<CauseKey | null>(null);
  const parts = causeParts(waiting, running, out, causes);
  const rc = (r: RunRow, k: CauseKey) => causeOf(runCauses[r.run_key], r.waiting_ms ?? 0, k);
  const top = (by ? [...runs].sort((a, b) => rc(b, by) - rc(a, by)).filter((r) => rc(r, by) > 0)
    : [...runs].sort((a, b) => (b.duration_ms ?? 0) - (a.duration_ms ?? 0))).slice(0, TOP);
  const maxT = Math.max(1, ...top.map((r) => r.duration_ms ?? 0));
  return (
    <section className="panel" id="cv-time">
      <div className="panel-head ro-head">
        <div>
          <h2>Where the runs' {fmtDuration(took)} went</h2>
          <ClockLine runs={runs} took={took} t0={t0} t1={t1} compute={compute} peak={peak} />
          <div className="note"><CauseHeadline parts={parts} total={took} spill={causes?.disk_spill} /> Click a part to rank the runs by it.</div>
        </div>
        <div className="ro-legend small muted"><CauseLegend parts={parts} total={took} /></div>
      </div>
      <div className="panel-body">
        <CauseBar parts={parts} total={took} by={by} onBy={setBy} />
        {by && <p className="small" style={{ margin: '0 0 8px' }}>The runs with the most <b>{parts.find((p) => p.key === by)?.label}</b> · <button className="linkish" onClick={() => setBy(null)}>back to longest first</button></p>}
        <div className="table-wrap" style={{ overflowX: 'auto' }}>
          <table className="ro-find ro-time-table">
            <thead>
              <tr>
                <th>Run</th><th>Started</th><th style={{ width: '20%', minWidth: 160 }}>Took · waited | ran</th><th className="num">× usual</th><th className="num">Tasks</th>
                <th className="num" title="From storage (files and tables); shuffle on hover; rows for JDBC">Read from storage</th><th style={{ width: '18%', minWidth: 140 }}>On the cluster's clock</th><th>Flags</th>
              </tr>
            </thead>
            <tbody>
              {top.map((r) => {
                const t = r.duration_ms ?? 0;
                const w = Math.min(r.waiting_ms ?? 0, t);
                const x = usualX(r);
                return (
                  <tr key={r.run_key}>
                    <td style={{ whiteSpace: 'normal', minWidth: 220 }}>
                      <Link to={inRun(to.overview(cid), r.run_key)}><b>{truncate(runName(r), 60)}</b></Link>
                      <div className="muted small">{runId(r)}</div>
                    </td>
                    <td className="mono small">{fmtTime(r.start_time)}</td>
                    <td><WaitRanBar took={t} wait={w} ran={r.running_ms ?? Math.max(0, t - w)} max={maxT} /></td>
                    <td className={`num ${(x ?? 0) >= 2 ? 'st-warn' : 'muted'}`} style={{ fontWeight: (x ?? 0) >= 2 ? 600 : undefined }}>{x !== null ? `${x.toFixed(1)}×` : '–'}</td>
                    <td className="num">{fmtNum(r.tasks)}</td>
                    {/* a source without read bytes (JDBC) reports rows: show those, not its shuffle */}
                    <td className="num">{!r.input_bytes && (r.input_records ?? 0) > 0
                      ? <span title={r.shuffle_read ? `shuffle read ${fmtBytes(r.shuffle_read)}` : undefined}>{fmtRows(r.input_records)} rows</span>
                      : r.input_bytes ? <span title={r.shuffle_read ? `plus shuffle read ${fmtBytes(r.shuffle_read)}` : undefined}>{fmtBytes(r.input_bytes)}</span> : '–'}</td>
                    <td>
                      <div className="qs-lane">
                        <span className="qs-run" style={{ left: pct(r.start_time ?? t0), width: width(r.start_time ?? t0, r.end_time ?? t1) }} />
                      </div>
                    </td>
                    <td className="ro-flags">
                      {r.status === 'failed' && <span className="ro-flag bad">failed</span>}
                      {(r.disk_spill ?? 0) >= GB / 4 && <span className="ro-flag spill">spilled {fmtBytes(r.disk_spill)}</span>}
                      {w / Math.max(1, t) >= 0.3 && t >= 60_000 && <span className="ro-flag">{fmtPct(w / t, 0)} waiting</span>}
                      {(r.max_task_input ?? 0) > 256 * 1024 ** 2 && <span className="ro-flag bad">a task read {fmtBytes(r.max_task_input)} of files</span>}
                      {(r.max_task_shuffle ?? 0) > 256 * 1024 ** 2 && <span className="ro-flag bad">a task read {fmtBytes(r.max_task_shuffle)} of shuffle</span>}
                      {r.failed_tasks ? <span className="ro-flag warn">{fmtNum(r.failed_tasks)} {r.failed_tasks === 1 ? 'attempt' : 'attempts'} failed</span> : null}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <p className="small" style={{ margin: '8px 0 0' }}>
          <a href="#cv-find">Rank the runs another way ↓</a> · <a href="#cv-runs">All {fmtNum(runs.length)} runs ↓</a>
        </p>
      </div>
    </section>
  );
}

function Programs({ cid, progs }: { cid: string; progs: Program[] }) {
  return (
    <section className="panel">
      <div className="panel-head"><div><h2>Programs <span className="ro-count">{progs.length}</span></h2><div className="note">Runs of the same code, the one with the most work first.</div></div></div>
      <div className="table-wrap" style={{ overflowX: 'auto' }}>
        <table className="ro-find">
          <thead><tr><th>Program</th><th className="num">Runs</th><th className="num">Usual</th><th className="num">Ran</th><th className="num">Waited</th><th>Worst run</th></tr></thead>
          <tbody>
            {progs.map((p) => (
              <tr key={p.name}>
                <td style={{ whiteSpace: 'normal' }}><b>{p.name}</b>
                  {p.failed || p.slow ? <div className="small">{p.failed ? <span className="bad">{p.failed} failed</span> : null}{p.failed && p.slow ? ' · ' : ''}{p.slow ? <span className="st-warn">{p.slow} slow</span> : null}</div> : null}
                </td>
                <td className="num">{fmtNum(p.runs.length)}</td>
                <td className="num">{p.usual ? fmtDuration(p.usual) : '–'}</td>
                <td className="num">{fmtDuration(p.running)}</td>
                <td className="num">{p.waiting >= 1000 ? fmtDuration(p.waiting) : '–'}</td>
                <td className="small"><Link to={inRun(to.overview(cid), p.worst.run_key)}>{truncate(p.worst.subject ?? runName(p.worst), 28)}</Link>
                  <span className="muted"> · {fmtDuration(p.worst.duration_ms)}{usualX(p.worst) ? ` · ${usualX(p.worst)!.toFixed(1)}×` : ''}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

/** Rank errors by what they did before how often they were logged: a failed read, a denied credential or an out of
 * memory first; a notice such as "use X instead" or "deprecated" last, however many lines it has. */
const errorEffect = (e: ErrorGroup) => errorClass(e).effect;

function Errors({ cid, rows, runs }: { cid: string; rows: ErrorGroup[] | null; runs: RunRow[] }) {
  const names = new Map(runs.map((r) => [r.run_key, r]));
  const top = [...(rows ?? [])].sort((a, b) => errorEffect(b) - errorEffect(a) || b.occurrences - a.occurrences).slice(0, 6);
  return (
    <section className="panel">
      <div className="panel-head" id="cv-errors"><h2>Errors in the logs <span className="ro-count">{rows?.length ?? '…'}</span></h2><Link className="small" to={to.errors(cid)}>All errors</Link></div>
      <div className="table-wrap" style={{ overflowX: 'auto' }}>
        {rows === null ? <p className="muted small panel-body">Loading…</p> : !top.length ? <p className="muted small panel-body">No exceptions in the logs.</p> : (
          <table className="ro-find">
            <thead><tr><th>Error</th><th className="num">Lines</th><th className="num" title="Runs going when its lines were logged">Runs then</th><th>Last</th></tr></thead>
            <tbody>
              {top.map((e) => (
                <tr key={e.fingerprint}>
                  <td className="small" style={{ whiteSpace: 'normal' }}>
                    <ErrClassChip e={e} /> <Link to={to.errors(cid, e.fingerprint)}><b className="mono">{e.exception_class.split('.').pop()}</b></Link>
                    {e.sample_message ? <span className="muted"> · {truncate(e.sample_message.split(/\r?\n/)[0], 90)}</span> : null}
                  </td>
                  <td className="num">{fmtNum(e.occurrences)}</td>
                  <td className="num" title={(e.hit_runs ?? []).slice(0, 8).map((h) => names.get(h.run_key) ? runName(names.get(h.run_key)!) : h.run_key).join('\n')}>{e.hit_runs?.length ? fmtNum(e.hit_runs.length) : '–'}</td>
                  <td className="mono small">{fmtTime(e.last_seen).slice(0, 8)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}

/** The plain kind of an error ("credentials or access", "out of memory"), with what it usually means on hover. */
export function ErrClassChip({ e }: { e: Pick<ErrorGroup, 'exception_class' | 'sample_message'> }) {
  const c = errorClass(e);
  return <span className={`err-class eff-${c.effect}`} title={c.hint || undefined}>{c.label}</span>;
}

type Finding = FindingRow & { run_key?: string | null };
const RUN_LEVEL = new Set(['waited_for_cores', 'cores_full']);
const EXEC_MEMORY = new Set(['gc_pressure', 'jvm_full_gc', 'gc_stuck', 'oom_site', 'executor_oom', 'log:gc_pressure', 'log:jvm_full_gc', 'log:executor_oom', 'log:disk_spill']);
/** When no change names the finding's stage, query or run: the change about the same kind of problem. */
function sameKind(a: Advice, f: Finding): boolean {
  if (RUN_LEVEL.has(f.category)) return /waited for cores|shared the executors|runs started/i.test(a.title);
  if (f.category === 'merge_rewrite') return /MERGE/.test(a.title);
  if (f.category === 'disk_spill') return /spill/i.test(a.title);
  return false;
}

/** Is this finding part of the evidence for this change? The same stage, the same query, or (for a run-level
 * finding such as waiting for cores) the same run; memory findings on executors go with the memory change. */
function backs(a: Advice, f: Finding, qid: number | null): boolean {
  if (f.stage_id !== null && a.stages?.some((s) => s.spark_context_id === f.spark_context_id && s.stage_id === f.stage_id)) return true;
  if (qid !== null && a.queries?.some((q) => q.spark_context_id === f.spark_context_id && q.sql_execution_id === qid)) return true;
  if (f.stage_id === null && f.sql_execution_id === null && RUN_LEVEL.has(f.category) && f.run_key && a.runs?.some((r) => r.run_key === f.run_key)) return true;
  if (EXEC_MEMORY.has(f.category) && (a.key ?? '').includes('memory')) return true;
  return false;
}

/** One list of what to change, highest impact first; under each, the problems found that back it. The problems no
 * change covers come after, by kind. Then the settings themselves. */
function ChangeList({ cid, rows, loading, runs }: { cid: string; rows: Finding[]; loading: boolean; runs: RunRow[] }) {
  const sv = useAsync((s) => api.settings(cid, s), [cid]);
  const byRun = new Map(runs.map((r) => [r.run_key, r]));
  const need = rows.some((f) => f.sql_execution_id !== null || f.stage_id !== null);
  const qs = useAsync((s) => (need ? api.datasetOpt<{ spark_context_id: string; sql_execution_id: number; tables_read: string[] | null; tables_written: string[] | null }>(
    cid, 'sql_queries', { limit: 50000, run: '', columns: 'spark_context_id,sql_execution_id,tables_read,tables_written' }, s) : Promise.resolve(null)), [cid, need]);
  const sts = useAsync((s) => (need ? api.datasetOpt<{ spark_context_id: string; stage_id: number; sql_execution_id: number | null }>(
    cid, 'stages', { limit: 100000, run: '', columns: 'spark_context_id,stage_id,sql_execution_id' }, s) : Promise.resolve(null)), [cid, need]);
  const qmap = useMemo(() => new Map((qs.data?.rows ?? []).map((q) => [`${q.spark_context_id}|${q.sql_execution_id}`, q])), [qs.data]);
  const smap = useMemo(() => new Map((sts.data?.rows ?? []).map((x) => [`${x.spark_context_id}|${x.stage_id}`, x.sql_execution_id])), [sts.data]);
  const qidOf = (f: Finding) => f.sql_execution_id ?? (f.stage_id !== null ? smap.get(`${f.spark_context_id}|${f.stage_id}`) ?? null : null);
  const advice = sv.data?.advice ?? [];
  const { groups, other } = useMemo(() => {
    const groups = advice.map((a) => ({ a, fs: [] as Finding[] }));
    const other: Finding[] = [];
    for (const f of rows) {
      const g = groups.find((x) => backs(x.a, f, qidOf(f))) ?? groups.find((x) => sameKind(x.a, f));
      if (g) g.fs.push(f); else other.push(f);
    }
    return { groups, other };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [advice, rows, smap]);
  const card = (f: Finding) => {
    const qid = qidOf(f);
    const q = qid !== null ? qmap.get(`${f.spark_context_id}|${qid}`) : undefined;
    const run = f.run_key ? byRun.get(f.run_key) : undefined;
    const links = <>
      {f.sql_execution_id === null && qid !== null && f.spark_context_id ? <Link className="tchip" to={inRun(to.query(cid, f.spark_context_id, qid), f.run_key)}>Query {qid}</Link> : null}
      {run ? <Link className="tchip" to={inRun(to.overview(cid), run.run_key)}>Run · {truncate(runName(run), 40)}</Link> : null}
    </>;
    return <FindingPoints key={f.finding_id} cid={cid} f={f} reads={q?.tables_read} writes={q?.tables_written} links={links} />;
  };
  const [all, setAll] = useState(false);
  const TOP_CHANGES = 3;
  const kinds = useMemo(() => {
    const m = new Map<string, Finding[]>();
    for (const f of other) m.set(kindName(f.category), [...(m.get(kindName(f.category)) ?? []), f]);
    return [...m].map(([k, fs]) => ({ k, fs, deep: depthOf(fs[0].category) > 0 }));
  }, [other]);
  return (
    <section className="panel" id="cv-change">
      <div className="panel-head">
        <div>
          <h2>What to change <span className="ro-count">{advice.length}</span></h2>
          <div className="note">Highest impact first. Each: what the data shows across all runs, the likely cause and the fix; open it for the problems behind it.</div>
        </div>
        <Link className="btn small" to={to.findings(cid)}>All {fmtNum(rows.length)} problems</Link>
      </div>
      <div className="panel-body stack" style={{ gap: 10 }}>
        {sv.loading ? <p className="muted small">Loading…</p> : !advice.length ? <p className="muted small" style={{ margin: 0 }}>Nothing stood out: tasks were sized well, little spill, executors were busy.</p> : null}
        {(all ? groups : groups.slice(0, TOP_CHANGES)).map(({ a, fs }, i) => (
          <AdviceItem key={i} cid={cid} a={a}>
            {fs.length > 0 && <Evidence label={`The ${fmtNum(fs.length)} ${fs.length === 1 ? 'problem' : 'problems'} found behind it`} items={fs} card={card} />}
          </AdviceItem>
        ))}
        {!all && (groups.length > TOP_CHANGES || kinds.length > 0 || sv.data) && (
          <button className="linkish small" style={{ alignSelf: 'flex-start' }} onClick={() => setAll(true)}>
            {[groups.length > TOP_CHANGES ? `${groups.length - TOP_CHANGES} more ${groups.length - TOP_CHANGES === 1 ? 'change' : 'changes'}` : '',
              other.length ? `${fmtNum(other.length)} other problems` : '', 'the settings'].filter(Boolean).join(', ')} ↓
          </button>
        )}
        {all && !loading && kinds.length > 0 && (
          <div className="stack" style={{ gap: 6 }}>
            <h3 className="small" style={{ margin: '6px 0 0' }}>Other problems found <span className="ro-count">{other.length}</span> <span className="muted" style={{ fontWeight: 400 }}>· not covered by a change above, by kind, worst first</span></h3>
            {kinds.filter((x) => !x.deep).map(({ k, fs }) => <Evidence key={k} label={`${k} · ${fmtNum(fs.length)}`} sev={fs[0].severity} items={fs} card={card} />)}
            {kinds.some((x) => x.deep) && (
              <details className="evidence">
                <summary className="small">Detailed: GC, memory, logs · {fmtNum(kinds.filter((x) => x.deep).reduce((n, x) => n + x.fs.length, 0))} <span className="muted">· they explain the problems above</span></summary>
                <div className="stack" style={{ gap: 6, marginTop: 6 }}>
                  {kinds.filter((x) => x.deep).map(({ k, fs }) => <Evidence key={k} label={`${k} · ${fmtNum(fs.length)}`} sev={fs[0].severity} items={fs} card={card} />)}
                </div>
              </details>
            )}
          </div>
        )}
        {all && sv.data && <SettingsTable v={sv.data} />}
        {all && <button className="linkish small" style={{ alignSelf: 'flex-start' }} onClick={() => setAll(false)}>Only the top {TOP_CHANGES} ↑</button>}
      </div>
    </section>
  );
}

/** A folded list of findings: the first few, then the rest on demand. */
function Evidence({ label, items, card, sev }: { label: string; items: Finding[]; card: (f: Finding) => ReactNode; sev?: string }) {
  const [all, setAll] = useState(false);
  return (
    <details className="evidence">
      <summary className="small">{sev ? <span className={`adv-sev ${sev === 'high' ? 'high' : sev === 'medium' ? 'medium' : 'info'}`} style={{ marginRight: 6 }}>{sev}</span> : null}{label}</summary>
      <div className="stack" style={{ gap: 8, marginTop: 8 }}>
        {(all ? items : items.slice(0, 5)).map(card)}
        {items.length > 5 && <button className="linkish small" style={{ alignSelf: 'flex-start' }} onClick={() => setAll(!all)}>{all ? 'Fewer ↑' : `${items.length - 5} more ↓`}</button>}
      </div>
    </details>
  );
}
