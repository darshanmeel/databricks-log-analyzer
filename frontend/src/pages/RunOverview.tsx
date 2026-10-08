// Revision 19: the Overview when one run is picked, read top to bottom the way a 2 am engineer asks: why was it slow
// and how did it end (side by side), where the time went (the few queries that matter), the causes and the fix, its
// numbers, the end in time order, what to open, and its problems. Only that run; the whole cluster is the Overview
// with no run picked.
import { Fragment, useMemo, useState, type ReactNode } from 'react';
import { TablesRead } from '../components/TableStats';
import { ChangeList } from '../components/ClusterTop';
import { Link, useNavigate } from 'react-router-dom';
import { api, type EngineCount, type FindingRow, type RunEnd, type RunRow, type RunStepGroup, type RunSteps } from '../api';
import { useAsync } from '../hooks';
import { fmtBytes, fmtDuration, fmtNum, fmtPct, fmtRows, fmtTime, truncate } from '../format';
import { to } from '../links';
import { useCluster, useRunScopeCtx, isSlow } from '../components/Shell';
import { Fold, Tile } from '../components/ui';
import { FindingPoints, depthOf, plainFirst } from '../components/FindingPoints';
import { RunStepsPanel, whatItDid } from '../components/RunSteps';
import { TopFinder, WaitRanBar } from '../components/TopFinder';
import { RunTables } from '../components/RunTables';
import { ErrClassChip } from '../components/ClusterTop';
import { CauseBar, CauseHeadline, CauseLegend, causeOf, causeParts, type CauseKey } from '../components/CauseBar';
import { WhatBroke, useTopFailure, type TopFailure } from '../components/WhatBroke';

const TOP = 5; // queries listed in "where the time went"
const GB = 1024 ** 3;
const BAD_GONE = ['oom', 'lost', 'killed'];
const EXEC_FAILURES = new Set(['executor_oom', 'executor_lost', 'executor_killed', 'oom_site', 'log:executor_oom']);

/** The MERGE numbers from a merge_rewrite finding: what it read of the target, the rows, what it wrote and spilled. */
function mergeOf(f: FindingRow | undefined) {
  const m = f?.evidence?.match(/read ([\d.]+ \w+) of the target \(([\d,]+) rows\) and wrote ([\d.]+ \w+)(?:, spilling ([\d.]+ \w+))?/);
  const files = f?.evidence?.match(/rewrote (\d+) files/);
  return m ? { read: m[1], rows: Number(m[2].replace(/,/g, '')), wrote: m[3], spilled: m[4] ?? null, files: files ? Number(files[1]) : null } : null;
}
const toBytes = (s: string | null | undefined) => {
  const m = s?.match(/([\d.]+) (\w+)/);
  if (!m) return 0;
  const u: Record<string, number> = { B: 1, KB: 1024, MB: 1024 ** 2, GB, TB: 1024 ** 4 };
  return Number(m[1]) * (u[m[2]] ?? 1);
};
/** The first sentence or two of a fix, up to about 200 characters: the rest is on the finding. */
const firstSentences = (s: string) => truncate(s.split(/(?<=\.)\s+/)[0].replace(/ \(for example [^)]*\)/, ''), 160);
/** What a query did, for one line: a MERGE is named by its step, so what it read can go. */
const brief = (w: string) => {
  const bits = w.split(' · ');
  const m = bits.some((b) => b.startsWith('MERGE'));
  return bits.filter((b) => !(m && b.startsWith('reads ') && !b.includes('JDBC') && !b.includes('checkpoint')))
    // Spark names the MERGE's last step "rewriting N files…" even when deletion vectors write only the changed rows
    .map((b) => b.replace(/^MERGE:\s*rewriting [\d,]+ files\b.*/i, 'MERGE: write the changes'))
    .map((b) => b.replace(/^MERGE: (\w)/, (_x, c: string) => `MERGE: ${c.toLowerCase()}`)).join(' · ');
};
const gname = (g: RunStepGroup) => (g.kind === 'query' ? `Query ${g.id}` : `Job ${g.id ?? '?'}`);
const took = (g: RunStepGroup) => g.end - g.start;
const short = (ms: number) => fmtDuration(ms).replace(/ (\d+)s$/, ' $1s');
export function RunOverview({ run }: { run: RunRow }) {
  const { cid } = useCluster();
  const scope = useRunScopeCtx();
  const end = useAsync((s) => api.runEnd(cid, run.run_key, s), [cid, run.run_key]);
  const steps = useAsync((s) => api.runSteps(cid, run.run_key, s), [cid, run.run_key]);
  const fs = useAsync((s) => api.datasetOpt<FindingRow>(cid, 'findings', { limit: 500 }, s), [cid, run.run_key]);
  // executor findings about GC belong to the cluster: the executors are shared by every run, so they are on the
  // cluster's page, not counted as this run's problems. An executor lost or out of memory while the run went is kept.
  const findings = useMemo(() => [...(fs.data?.rows ?? [])]
    .filter((f) => !(f.executor_id != null && f.stage_id == null && f.sql_execution_id == null && !EXEC_FAILURES.has(f.category)))
    .sort(plainFirst), [fs.data]);
  const top = useTopFailure(cid, false, run.run_key);
  const [allSteps, setAllSteps] = useState(false);
  const e = end.data;
  const d = steps.data && steps.data.start !== null && steps.data.groups.length ? steps.data : null;
  const a = useMemo(() => (d ? analyse(d, findings) : null), [d, findings]);
  const spill = (run.disk_spill ?? 0) + (run.mem_spill ?? 0);
  const execs = useMemo(() => new Set((d?.groups ?? []).flatMap((g) => g.stages.flatMap((s) => (s.by_exec ?? []).map((x) => x.executor_id)))).size, [d]);
  // the runs that started within two minutes of it: a burst of runs fights for the same cores
  const burst = scope.runs.filter((r) => r.run_key !== run.run_key && r.start_time != null && run.start_time != null && Math.abs(r.start_time - run.start_time) <= 120_000).length;
  const gone = e?.executors_gone ?? [];
  const badGone = gone.filter((x) => BAD_GONE.includes(x.removal_category ?? ''));
  const goneTitle = gone.length ? Object.entries(gone.reduce<Record<string, number>>((m, x) => ({ ...m, [x.removal_category ?? 'other']: (m[x.removal_category ?? 'other'] ?? 0) + 1 }), {}))
    .map(([k, n]) => `${n} ${k}`).join(', ') : undefined;
  const others = run.overlapping_runs?.length ?? 0;
  // the numbers that say little on the first screen: under Details
  const moreTiles = (
    <div className="kpis">
      <Tile label="Tasks" value={fmtNum(run.tasks)} foot={run.failed_tasks ? `${fmtNum(run.failed_tasks)} attempts failed` : 'none failed'} tone={run.failed_tasks ? 'warn' : undefined} />
      <Tile label="Spill" value={fmtBytes(spill)} foot={`disk ${fmtBytes(run.disk_spill ?? 0)} · memory ${fmtBytes(run.mem_spill ?? 0)}`} />
      <Tile label="Shuffle" value={fmtBytes(Math.max(run.shuffle_read ?? 0, run.shuffle_write ?? 0))} foot={`read ${fmtBytes(run.shuffle_read ?? 0)} · write ${fmtBytes(run.shuffle_write ?? 0)}`} />
      <Tile label="Ran next to it" value={fmtNum(others)} foot="runs on the same executors" />
      <Tile label="Executors gone" value={e ? fmtNum(gone.length) : '…'} foot={goneTitle ?? 'while it ran'} />
    </div>
  );

  return (
    <div className="page wide">
      <div className="stack">
        {/* first screen: what broke, the verdict, the numbers, the causes, the fixes, the steps. The rest is under "Details" */}
        <WhatBroke cid={cid} top={top.data} run={run.run_key} />
        <div className="ro-hero">
          <section className="ro-hero-box">
            <span className={`ro-eyebrow ${a && (isSlow(run) || a.waitShare >= 0.3) ? 'warn' : ''}`}>{a && (isSlow(run) || a.waitShare >= 0.3) ? 'Why it was slow' : 'Where the time went'}</span>
            {a ? <WhySlow cid={cid} run={run} a={a} execs={execs} /> : <p className="muted small">{steps.loading ? 'Loading…' : 'No Spark work recorded for this run.'}</p>}
          </section>
          <section className="ro-hero-box">
            <span className={`ro-eyebrow ${e && (e.failed_queries || e.failed_jobs || badGone.length) ? 'bad' : ''}`}>How it ended</span>
            {e ? <HowEnded cid={cid} e={e} d={d} /> : <p className="muted small">{end.error ? `Could not load: ${end.error.message}` : 'Loading…'}</p>}
          </section>
        </div>

        {/* only the numbers that say something: an empty or normal one is under Details */}
        <div className="kpis">
          <Tile label="Duration" value={fmtDuration(run.duration_ms)}
            foot={run.typical_duration_ms && (run.usual_from === 'history' || (run.same_job_runs ?? 0) >= 3) ? `usual ${fmtDuration(run.typical_duration_ms)}` : 'no usual yet'}
            tone={isSlow(run) ? 'warn' : undefined}
            title={run.typical_duration_ms ? (run.usual_from === 'history' ? `Usual: the median of its last ${run.usual_runs} runs` : 'Usual: the median of this batch') : undefined} />
          <Tile label="Queries · jobs" value={e ? `${fmtNum(e.queries)} · ${fmtNum(run.spark_jobs)}` : '…'}
            foot={e ? (e.failed_jobs || e.failed_queries ? `${e.failed_queries} queries, ${e.failed_jobs} jobs failed` : e.replanned_jobs ? `none failed · ${e.replanned_jobs} replanned` : 'none failed') : null}
            tone={e && (e.failed_jobs || e.failed_queries) ? 'bad' : undefined} />
          {run.failed_tasks ? <Tile label="Tasks" value={fmtNum(run.tasks)} foot={`${fmtNum(run.failed_tasks)} ${run.failed_tasks === 1 ? 'attempt' : 'attempts'} failed`} tone="warn" title={`${fmtNum(run.retried_tasks)} retried`} /> : null}
          {(run.disk_spill ?? 0) > 0 && <Tile label="Disk spill" value={fmtBytes(run.disk_spill)} foot="to disk" tone={(run.disk_spill ?? 0) >= GB ? 'warn' : undefined}
            title={`Plus ${fmtBytes(run.mem_spill ?? 0)} spilled in memory (the data before it was written out)`} />}
          {others > 0 && <Tile label="Ran next to it" value={fmtNum(others)} foot="runs on its executors" tone={others >= 5 ? 'warn' : undefined} />}
          {badGone.length > 0 && <Tile label="Executors lost" value={fmtNum(badGone.length)} foot={<Link to={to.executors(cid)}>out of memory, lost or killed</Link>} tone="bad" title={goneTitle} />}
        </div>

        {a && <Causes cid={cid} a={a} run={run} execs={execs} burst={burst}
          toCluster={() => scope.choose(null)} />}

        {a && <Fixes run={run} a={a} spill={spill} findings={findings} failure={top.data} />}

        {a && d && <TimeWent cid={cid} d={d} a={a} allSteps={allSteps} setAllSteps={setAllSteps} />}
        {allSteps && <RunStepsPanel cid={cid} run={run.run_key} />}

        <TablesRead cid={cid} scope={{ run: run.run_key }} />

        {/* one fix list on the first screen (Fix, above); the cluster's changes this run is part of, with the evidence
            behind each, are under Details */}
        <Fold name="run" title="Details" ids={['ro-change', 'ro-find', 'ro-end', 'ro-problems']}
          hint={`more numbers, what to change, find, tables, the end in order${fs.loading ? '' : `, all ${fmtNum(findings.length)} problems`}`}>
          {moreTiles}
          <ChangeList cid={cid} rows={findings} loading={fs.loading} runs={scope.runs} run={run.run_key} />
          <div id="ro-find"><TopFinder cid={cid} run={run.run_key} /></div>
          <RunTables cid={cid} run={run.run_key} />

          {e && (
            <div className="ro-two">
              <EndInOrder cid={cid} e={e} d={d} />
              <div className="stack" style={{ gap: 12 }}>
                {e.engine && <Engine e={e.engine} />}
                <EndErrors cid={cid} e={e} />
              </div>
            </div>
          )}

          <Problems cid={cid} rows={findings} loading={fs.loading} d={d} />
        </Fold>
      </div>
    </div>
  );
}

interface Analysis {
  total: number; running: number; waitShare: number; byTook: RunStepGroup[]; top: RunStepGroup | null; topShare: number;
  merge: ReturnType<typeof mergeOf>; mergeFinding?: FindingRow; mostWaited: RunStepGroup | null;
  waitedFinding?: FindingRow; spillFindings: FindingRow[];
}

function analyse(d: RunSteps, fs: FindingRow[]): Analysis {
  const total = d.total_ms ?? (d.end! - d.start!);
  const byTook = [...d.groups].sort((x, y) => took(y) - took(x));
  const byRan = [...d.groups].sort((x, y) => y.running_ms - x.running_ms);
  const top = byRan[0] ?? null;
  const mergeFinding = fs.find((f) => f.category === 'merge_rewrite' && top && f.sql_execution_id === top.id);
  const mostWaited = [...d.groups].filter((g) => g.waiting_ms >= 60_000).sort((x, y) => y.waiting_ms / Math.max(1, y.running_ms) - x.waiting_ms / Math.max(1, x.running_ms))[0] ?? null;
  return {
    total, running: d.running_ms, waitShare: d.waiting_ms / Math.max(1, total), byTook, top, topShare: top ? top.running_ms / Math.max(1, d.running_ms) : 0,
    merge: mergeOf(mergeFinding), mergeFinding, mostWaited,
    waitedFinding: fs.find((f) => f.category === 'waited_for_cores'),
    spillFindings: fs.filter((f) => f.category === 'disk_spill'),
  };
}

function WhySlow({ cid, run, a, execs }: { cid: string; run: RunRow; a: Analysis; execs: number }) {
  const waited = a.waitShare >= 0.2;
  const d = a.total * a.waitShare;
  const t = a.top;
  const others = run.overlapping_runs?.length ?? 0;
  return (
    <>
      <ul className="ro-points">
        {waited && <li><b>{fmtDuration(d)} waiting for a free core</b> <span className="muted">· {fmtPct(a.waitShare, 0)} of {fmtDuration(a.total)}</span>
          {others ? <span className="muted"> · {fmtNum(others)} other runs on the same {execs || ''} executors</span> : null}</li>}
        {/* one line: the cause card below has the detail */}
        {t && <li><b>{gname(t)}{a.merge ? ' (MERGE)' : ''}</b> {a.topShare >= 0.4 ? 'did most of the work' : 'ran longest'}
          <span className="muted"> · {fmtDuration(t.running_ms)} of {fmtDuration(a.running)} running{a.merge ? `, read ${a.merge.read} of the target` : (t.input_bytes || t.shuffle_read) ? `, read ${fmtBytes((t.input_bytes ?? 0) + (t.shuffle_read ?? 0))}` : ''}{a.merge?.spilled || (t.disk_spill ?? 0) > 0 ? `, spilled ${a.merge?.spilled ?? fmtBytes(t.disk_spill)}` : ''}</span></li>}
      </ul>
      <div className="ro-links">
        <a href="#ro-time">Where the time went ↓</a>
        {t && t.kind === 'query' && t.id !== null && <Link to={to.query(cid, t.ctx, t.id)}>{gname(t)}'s stages →</Link>}
      </div>
    </>
  );
}

function HowEnded({ cid, e, d }: { cid: string; e: RunEnd; d: RunSteps | null }) {
  const bad = e.lines.find((l) => l.tone === 'bad');
  const lq = e.last_query;
  const lg = lq && d?.groups.find((g) => g.kind === 'query' && g.id === lq.sql_execution_id);
  const lastEnd = Math.max(lq?.end_time ?? 0, d?.end ?? 0) || e.end_time;
  const gap = e.cluster_end && lastEnd ? e.cluster_end - lastEnd : null;
  const stopped = gap !== null && gap >= 0 && gap <= 120_000;
  const gapText = gap === null ? '' : gap >= 60_000 ? fmtDuration(gap) : `${Math.round(gap / 1000)} s`;
  return (
    <>
      <ul className="ro-points">
        {bad ? <li className="bad"><b>{bad.text}</b></li>
          : <li><b>No failure in Spark</b> <span className="muted">· {fmtNum(e.queries)} queries, {fmtNum(e.spark_jobs - e.replanned_jobs)} jobs finished</span></li>}
        {lq && <li>Last: query {lq.sql_execution_id} {lq.status === 'failed' ? 'failed' : 'ended'} at <b className="mono">{fmtTime(lq.end_time)}</b>
          {lg && lg.failed_tasks ? <span className="muted"> · {fmtNum(lg.failed_tasks)} task attempts retried</span> : null}</li>}
        {stopped && <li>Cluster stopped <b>{gapText} later</b></li>}
        {!bad && stopped && <li>Databricks says failed? It was <b>cut off by the cluster ending</b> <span className="muted">(timeout, cancel or auto-termination)</span></li>}
      </ul>
      <div className="ro-links">
        <a href="#ro-end">The end, in order ↓</a>
        {e.errors.length > 0 && <Link to={to.errors(cid)}>Errors →</Link>}
      </div>
    </>
  );
}

/** Streaming runs: how many micro-batches, and whether one of them is the run. */
/** The run's own clock against its queries' times added up (they can overlap), and how the clock split. */
function RunClockLine({ d, total }: { d: RunSteps; total: number }) {
  const gs = d.groups;
  if (gs.length < 2 || !total) return null;
  const sumQ = gs.reduce((s, g) => s + took(g), 0);
  const ev = gs.flatMap((g) => [[g.start, 1], [g.end, -1]] as [number, number][]).sort((x, y) => x[0] - y[0] || x[1] - y[1]);
  let n = 0, peak = 0;
  for (const [, k] of ev) { n += k; peak = Math.max(peak, n); }
  return (
    <div className="note">
      On its clock <b>{fmtDuration(total)}</b>: waited for cores {fmtDuration(d.waiting_ms)}, ran tasks {fmtDuration(d.running_ms)}, no Spark work {fmtDuration(d.outside_ms ?? 0)}.{' '}
      Its {fmtNum(gs.length)} queries and jobs add up to <b>{fmtDuration(sumQ)}</b>{sumQ > total * 1.05
        ? <> because they overlapped (up to {fmtNum(peak)} at once)</> : sumQ < total * 0.95 ? <>; the rest of the clock is between them</> : null}.
    </div>
  );
}

function Batches({ d, total }: { d: RunSteps; total: number }) {
  const bs = d.batches ?? [];
  if (!bs.length) return null;
  const took = (b: (typeof bs)[number]) => b.end - b.start;
  const streams = new Set(bs.map((b) => b.stream)).size;
  const worst = [...bs].sort((x, y) => took(y) - took(x))[0];
  const med = [...bs].map(took).sort((x, y) => x - y)[Math.floor((bs.length - 1) / 2)];
  return (
    <div className="note">
      Streaming: {bs.length === 1
        ? <>one micro-batch (batch {worst.batch} of stream {worst.stream.slice(0, 8)}) is the run: {worst.queries.length} queries, {fmtDuration(took(worst))}. A run that processes one batch and stops is a trigger-once or available-now job: its time is this batch's work.</>
        : <>{fmtNum(bs.length)} micro-batches of {fmtNum(streams)} {streams === 1 ? 'stream' : 'streams'}; the longest, batch {worst.batch}, took {fmtDuration(took(worst))} ({fmtPct(took(worst) / Math.max(1, total), 0)} of the run), the median {fmtDuration(med)}.</>}
    </div>
  );
}

function TimeWent({ cid, d, a, allSteps, setAllSteps }: { cid: string; d: RunSteps; a: Analysis; allSteps: boolean; setAllSteps: (v: boolean) => void }) {
  const t0 = d.start!, t1 = d.end!;
  const span = Math.max(1, t1 - t0);
  const pct = (v: number) => `${Math.min(100, Math.max(0, ((v - t0) / span) * 100))}%`;
  const width = (x: number, y: number) => `${Math.max(0.6, ((Math.min(y, t1) - Math.max(x, t0)) / span) * 100)}%`;
  const [by, setBy] = useState<CauseKey | null>(null);
  const parts = causeParts(d.waiting_ms, d.running_ms, d.outside_ms ?? 0, d.causes ?? null);
  const gc = (g: RunStepGroup, k: CauseKey) => causeOf(g.causes, g.waiting_ms, k);
  const ranked = by ? [...d.groups].sort((x, y) => gc(y, by) - gc(x, by)).filter((g) => gc(g, by) > 0) : a.byTook;
  const top = ranked.slice(0, TOP);
  const rest = a.byTook.slice(TOP).sort((x, y) => x.start - y.start);
  const maxT = Math.max(1, ...top.map(took));
  return (
    <section className="panel" id="ro-time">
      <div className="panel-head ro-head">
        <div>
          <h2>Where the {fmtDuration(a.total)} went</h2>
          <div className="note"><CauseHeadline parts={parts} total={a.total} spill={d.causes?.disk_spill} /> Click a part to rank the queries by it.</div>
          <RunClockLine d={d} total={a.total} />
          <Batches d={d} total={a.total} />
        </div>
        <div className="ro-legend small muted"><CauseLegend parts={parts} total={a.total} /></div>
      </div>
      <div className="panel-body">
        <CauseBar parts={parts} total={a.total} by={by} onBy={setBy} />
        {by && <p className="small" style={{ margin: '0 0 8px' }}>Ranked by <b>{parts.find((p) => p.key === by)?.label}</b> · <button className="linkish" onClick={() => setBy(null)}>back to longest first</button></p>}
        <div className="table-wrap" style={{ overflowX: 'auto' }}>
          <table className="ro-find ro-time-table">
            <thead>
              <tr>
                <th>Query</th><th>Started</th><th style={{ width: '18%', minWidth: 150 }}>Took · waited | ran</th><th className="num">Stages</th><th className="num">Tasks</th>
                <th className="num">Rows in</th><th style={{ width: '16%', minWidth: 140 }}>On the run's clock</th><th>Flags</th>
              </tr>
            </thead>
            <tbody>
              {top.map((g) => {
                const what = brief(whatItDid(g));
                const merge = a.mergeFinding && g.id === a.mergeFinding.sql_execution_id ? a.merge : null;
                const ws = g.waiting_ms / Math.max(1, took(g));
                return (
                  <tr key={`${g.kind}${g.ctx}${g.id}`}>
                    <td style={{ whiteSpace: 'normal', minWidth: 220 }}>
                      {g.kind === 'query' && g.id !== null ? <Link to={to.query(cid, g.ctx, g.id)}><b>{gname(g)}</b></Link> : <b>{gname(g)}</b>}
                      {what ? <span className="muted"> · {truncate(what, 60)}</span> : null}
                    </td>
                    <td className="mono small">{fmtTime(g.start)}</td>
                    <td><WaitRanBar took={took(g)} wait={g.waiting_ms} ran={g.running_ms} max={maxT} /></td>
                    <td className="num">{fmtNum(g.stages.length)}</td>
                    <td className="num">{fmtNum(g.tasks ?? null)}</td>
                    <td className="num">{g.input_records ? fmtRows(g.input_records) : '–'}</td>
                    <td>
                      <div className="qs-lane">
                        <span className={`qs-span ${g === a.top ? 'ro-hot' : ''}`} style={{ left: pct(g.start), width: width(g.start, g.end) }} />
                        {g.stages.map((s) => <span key={`${s.stage_id}.${s.stage_attempt}`} className={g === a.top ? 'qs-run ro-hot' : 'qs-run'} style={{ left: pct(s.first_task), width: width(s.first_task, s.end) }} />)}
                      </div>
                    </td>
                    <td className="ro-flags">
                      {(g.disk_spill ?? 0) >= GB / 4 && <span className="ro-flag spill">spilled {fmtBytes(g.disk_spill)}</span>}
                      {merge && <span className="ro-flag warn">read {merge.read} of target</span>}
                      {ws >= 0.5 && took(g) >= 60_000 && <span className="ro-flag">{fmtPct(ws, 0)} waiting</span>}
                      {g.failed_tasks ? <span className="ro-flag bad">{fmtNum(g.failed_tasks)} attempts failed</span> : null}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {(rest.length > 0 || !allSteps) && (
          <p className="small" style={{ margin: '8px 0 0' }} hidden={!!by}>
            <button className="linkish" onClick={() => setAllSteps(!allSteps)}>
              {allSteps ? 'Hide the step-by-step list ↑' : rest.length ? `${rest.length} more ${rest.length === 1 ? 'query or job' : 'queries and jobs'} under ${short(Math.max(...rest.map(took)))}, as they ran →` : 'Every query step by step, with its stages →'}
            </button>
            {!allSteps && rest.length > 0 && <span className="muted"> · {rest.slice(0, 8).map((g) => gname(g) + (g.failed_tasks ? ` (${g.failed_tasks} task attempts failed, retried)` : '')).join(', ')}{rest.length > 8 ? '…' : ''}</span>}
          </p>
        )}
      </div>
    </section>
  );
}

/** At most three changes, in order, each with what it wins: the cluster's concurrency when the run mostly waited, the
 * query that did most of the work, then the worst other problems that carry a fix. */
function Fixes({ run, a, spill, findings, failure }: { run: RunRow; a: Analysis; spill: number; findings: FindingRow[]; failure: TopFailure | null }) {
  const [copied, setCopied] = useState(false);
  const waitMs = a.total * a.waitShare;
  const t = a.top;
  const fixes: { who: string; text: ReactNode; plain: string; wins?: string }[] = [];
  const done = new Set<string>();
  // what broke comes first: the fix of the failure's most likely root cause
  const rf = failure?.root.lead;
  if (rf?.fix) {
    const i = rf.inc!;
    const q = rf.sql_execution_id ?? i.sql_execution_id, st = rf.stage_id ?? i.stage_id;
    const who = q !== null ? `Query ${q}` : st !== null ? `Stage ${st}` : rf.executor_id ? `Executor ${rf.executor_id}` : 'Cluster';
    fixes.push({ who, plain: rf.fix, text: firstSentences(rf.fix) });
    done.add(rf.category);
  }
  if (a.waitShare >= 0.2) {
    const plain = `Lower the for-each concurrency to about the cores available, or raise min_workers before the ${fmtTime(run.start_time).slice(0, 5)} schedule.`;
    fixes.push({ who: 'Cluster', plain, text: plain, wins: `up to ${fmtDuration(waitMs)} of waiting` });
    done.add('waited_for_cores');
  }
  if (t && a.mergeFinding?.fix) {
    fixes.push({ who: gname(t), plain: a.mergeFinding.fix, text: "Add the target's partition or clustering column to the MERGE ON condition, so Delta skips the files it cannot match.", wins: `most of its ${fmtDuration(t.running_ms)} running` });
    done.add('merge_rewrite'); done.add('disk_spill');
  } else if (t && (t.disk_spill ?? 0) >= GB) {
    fixes.push({ who: gname(t), plain: 'More shuffle partitions (or let AQE split them) so each task holds less and stops spilling.', text: <>More shuffle partitions (<code>spark.sql.shuffle.partitions</code>, or let AQE split them) so each task holds less and stops spilling.</> });
    done.add('disk_spill');
  }
  // then the worst other problems that carry a fix, one per kind
  for (const f of findings) {
    if (fixes.length >= 3) break;
    if (!f.fix || done.has(f.category) || (f.severity !== 'high' && f.severity !== 'medium')) continue;
    done.add(f.category);
    const who = f.sql_execution_id !== null ? `Query ${f.sql_execution_id}` : f.stage_id !== null ? `Stage ${f.stage_id}` : 'Cluster';
    fixes.push({ who, plain: f.fix, text: firstSentences(f.fix) });
  }
  if (fixes.length < 3 && spill >= GB && !done.has('disk_spill')) fixes.push({ who: 'Still spilling', plain: 'Memory-optimised workers.', text: 'Memory-optimised workers.' });
  const copy = () => {
    const text = fixes.map((f, i) => `${i + 1}. ${f.who}: ${f.plain}`).join('\n');
    navigator.clipboard?.writeText(text).then(() => { setCopied(true); setTimeout(() => setCopied(false), 1500); }, () => {});
  };
  if (!fixes.length) return null;
  return (
    <section className="ro-card ro-fix" id="ro-fix">
      <div className="ro-card-head"><span className="ro-tag fix">Fix</span><button className="btn small" onClick={copy}>{copied ? 'Copied' : 'Copy'}</button></div>
      <h3>{fixes.length === 1 ? 'One change' : `${['', 'One', 'Two', 'Three'][fixes.length]} changes, in this order`}</h3>
      <ol className="small">
        {fixes.map((f, i) => <li key={i}><b>{f.who}:</b> {f.text}{f.wins ? <span className="muted"> Wins {f.wins}.</span> : null}</li>)}
      </ol>
    </section>
  );
}

function Causes({ cid, a, run, execs, burst, toCluster }: { cid: string; a: Analysis; run: RunRow; execs: number; burst: number; toCluster: () => void }) {
  const nav = useNavigate();
  const waitMs = a.total * a.waitShare;
  const t = a.top;
  const spilled = t ? [...t.stages].filter((s) => (s.disk_spill ?? 0) > 0).sort((x, y) => (y.disk_spill ?? 0) - (x.disk_spill ?? 0)).slice(0, 2) : [];
  const bars: [string, number, string][] = t ? (a.merge
    ? ([['read', toBytes(a.merge.read), 'ro-b-read'], ['spilled', toBytes(a.merge.spilled), 'ro-b-spill'], ['written', toBytes(a.merge.wrote), 'ro-b-write']] as [string, number, string][])
    : ([['read', (t.input_bytes ?? 0) + (t.shuffle_read ?? 0), 'ro-b-read'], ['spilled', t.disk_spill ?? 0, 'ro-b-spill'], ['shuffle write', t.shuffle_write ?? 0, 'ro-b-write'], ['written', t.output_bytes ?? 0, 'ro-b-write']] as [string, number, string][])
  ).filter((b) => b[1] > 0) : [];
  const bmax = Math.max(1, ...bars.map((b) => b[1]));
  if (a.waitShare < 0.2 && !t) return null;
  return (
    <div className="ro-causes" id="ro-causes">
      {a.waitShare >= 0.2 && (
        <section className="ro-card">
          <div className="ro-card-head"><span className="ro-tag">Cause 1 · cluster</span><span className="muted small">{fmtPct(a.waitShare, 0)} of the run</span></div>
          <h3>Cores held by other runs</h3>
          <div className="ro-big">{fmtDuration(waitMs)} <span>waiting for a free core</span></div>
          <p className="small muted">
            {burst ? `Started with ${fmtNum(burst)} other runs within two minutes of it; ` : ''}
            {run.overlapping_runs?.length ? `shared ${execs ? `${execs} executors` : 'its executors'} with ${fmtNum(run.overlapping_runs.length)} other runs.` : ''}
            {a.mostWaited ? ` ${gname(a.mostWaited)} waited ${fmtDuration(a.mostWaited.waiting_ms)} to run ${fmtDuration(a.mostWaited.running_ms)}.` : ''}
          </p>
          <button className="linkish small" style={{ alignSelf: 'flex-start' }} onClick={() => { toCluster(); nav(to.overview(cid)); }}>The cluster's runs and settings ↗</button>
        </section>
      )}
      {t && (
        <section className="ro-card">
          <div className="ro-card-head"><span className="ro-tag">Cause {a.waitShare >= 0.2 ? 2 : 1} · query</span><span className="muted small mono">{gname(t)} · {fmtTime(t.start).slice(0, 5)} – {fmtTime(t.end).slice(0, 5)}</span></div>
          <h3>{a.merge ? 'The MERGE reads the whole target' : a.topShare >= 0.4 ? `${gname(t)} did most of the work` : `The longest: ${gname(t)}`}</h3>
          {a.merge
            ? <div className="ro-big">{a.merge.read} <span>read of the target · {a.merge.wrote} written</span></div>
            : <div className="ro-big">{fmtDuration(t.running_ms)} <span>running tasks · {fmtPct(a.topShare, 0)} of the run's work</span></div>}
          {bars.length > 0 && (
            <div className="ro-bars small">
              {bars.map(([k, v, c]) => (
                <Fragment key={k}>
                  <span className="muted">{k}</span>
                  <span className="ro-bar"><i className={c} style={{ width: `${(v / bmax) * 100}%` }} /></span>
                  <span className="num">{fmtBytes(v)}</span>
                </Fragment>
              ))}
            </div>
          )}
          {spilled.length > 0 && (
            <p className="small muted">
              {spilled.map((s, i) => (
                <span key={s.stage_id}>{i ? '; ' : ''}<Link to={to.stages(cid, t.ctx, s.stage_id, s.stage_attempt)}>Stage {s.stage_id}</Link> ran {fmtNum(s.tasks ?? null)} tasks of ~{fmtBytes(s.p50_task_bytes_in ?? null)} each and spilled {fmtBytes(s.disk_spill ?? null)}</span>
              ))}.
            </p>
          )}
        </section>
      )}
    </div>
  );
}


type EndEvent = { t: number | null; tone: 'ok' | 'warn' | 'bad' | 'stop' | 'info'; text: ReactNode };

function EndInOrder({ cid, e, d }: { cid: string; e: RunEnd; d: RunSteps | null }) {
  const lq = e.last_query;
  const lg = lq && d?.groups.find((g) => g.kind === 'query' && g.id === lq.sql_execution_id);
  const stop = e.cluster_end && e.end_time && e.cluster_end - e.end_time <= 120_000 && e.cluster_end >= e.end_time ? e.cluster_end : null;
  const ev: EndEvent[] = [];
  const bad = e.lines.find((l) => l.tone === 'bad');
  if (bad) ev.push({ t: null, tone: 'bad', text: bad.text });
  if (lq) {
    ev.push({ t: lq.start_time, tone: lg?.failed_tasks ? 'warn' : 'info', text: <><Link to={to.query(cid, lq.spark_context_id, lq.sql_execution_id)}>Query {lq.sql_execution_id}</Link> starts{lg?.failed_tasks ? <>; <b>{fmtNum(lg.failed_tasks)} task attempts fail and are retried</b></> : null}</> });
    ev.push({ t: lq.end_time, tone: lq.status === 'failed' ? 'bad' : 'ok', text: <>Query {lq.sql_execution_id} {lq.status === 'failed' ? 'fails' : 'succeeds'} — <b>the run's last Spark work</b>.</> });
  }
  for (const x of e.errors) {
    const after = stop !== null && (x.last ?? 0) >= stop;
    const common = x.family_runs && x.runs_with && x.family_runs >= 3 && x.runs_with / x.family_runs >= 0.5;
    ev.push({
      t: x.last, tone: after ? 'info' : 'warn',
      text: <><Link to={to.errors(cid, x.fingerprint)} className="mono">{(x.exception_class ?? 'error').split('.').pop()}</Link> ×{x.lines} on the {(x.sources ?? ['logs']).join(', ')}
        {x.message ? <span className="muted"> — “{truncate(x.message, 60)}”</span> : null}
        {after ? <span className="muted"> · after the stop: a symptom of the shutdown</span> : common ? <span className="muted"> · not fatal · near the end of {x.runs_with} of {x.family_runs} runs</span> : null}</>,
    });
  }
  const gone = e.executors_gone;
  const withStop = stop ? gone.filter((x) => Math.abs((x.removed_time ?? 0) - stop) <= 5_000) : [];
  if (stop) {
    const bad = withStop.some((x) => ['oom', 'lost', 'killed'].includes(x.removal_category ?? ''));
    ev.push({ t: stop, tone: 'stop', text: <><b>Cluster stopped</b>{withStop.length ? `; exec ${withStop.map((x) => x.executor_id).join(', ')} removed with it` : ''}{bad ? '' : ' — none lost, killed or out of memory'}.</> });
  }
  for (const x of gone.filter((g) => !withStop.includes(g))) {
    ev.push({ t: x.removed_time, tone: ['oom', 'lost', 'killed'].includes(x.removal_category ?? '') ? 'bad' : 'info', text: <>Executor {x.executor_id} removed: {x.removed_reason ?? x.removal_category ?? 'no reason given'}</> });
  }
  ev.sort((x, y) => (x.t ?? 0) - (y.t ?? 0));
  if (e.replanned_jobs) ev.push({ t: null, tone: 'info', text: `${e.replanned_jobs} Spark job${e.replanned_jobs > 1 ? 's were' : ' was'} cancelled by adaptive execution after a re-plan — normal, not a failure.` });
  return (
    <section className="panel" id="ro-end">
      <div className="panel-head"><h2>The end, in order</h2><Link className="btn small" to={to.timeline(cid)}>On the timeline</Link></div>
      <div className="panel-body">
        <ol className="ro-events">
          {ev.map((x, i) => (
            <li key={i} className={`t-${x.tone}`}>
              <span className="mono ro-ev-t">{x.t ? fmtTime(x.t) : ''}</span>
              <span>{x.text}</span>
            </li>
          ))}
        </ol>
      </div>
    </section>
  );
}

function Engine({ e }: { e: NonNullable<RunEnd['engine']> }) {
  const row = (label: string, c: EngineCount, note: ReactNode) => {
    const on = c.could > 0 && c.used > 0;
    return (
      <div className="ro-engine-row">
        <span className={`ro-pill ${on ? 'on' : c.could ? 'off' : ''}`}>{on ? 'on' : c.could ? 'off' : 'n/a'}</span>
        <div>
          <b>{label}</b>
          <span className="muted"> · {c.could ? `used in ${fmtNum(c.used)} of the ${fmtNum(c.could)} queries that could use it` : 'no query here could use it'}</span>
          {c.why_not.length ? <span className="muted"> · {c.why_not.map(([w, n]) => `${w} (${fmtNum(n)})`).join('; ')}</span> : null}
          {note}
        </div>
      </div>
    );
  };
  return (
    <section className="panel">
      <div className="panel-head"><h2>Engine</h2><span className="muted small">per query · commands and local data cannot use either</span></div>
      <div className="panel-body stack small" style={{ gap: 8 }}>
        {row('Adaptive execution', e.aqe, null)}
        {row('Photon', e.photon, e.photon.could && !e.photon.used ? <span className="muted"> MERGE, sort and join usually run faster on a Photon runtime.</span> : null)}
      </div>
    </section>
  );
}

function EndErrors({ cid, e }: { cid: string; e: RunEnd }) {
  if (!e.errors.length) return null;
  const stop = e.cluster_end && e.end_time && e.cluster_end >= e.end_time && e.cluster_end - e.end_time <= 120_000 ? e.cluster_end : null;
  return (
    <section className="panel">
      <div className="panel-head"><h2>Errors logged before the end <span className="ro-count">{e.errors.length}</span></h2><Link className="small" to={to.logs(cid, { level: 'ERROR' })}>Search the logs</Link></div>
      <div className="table-wrap" style={{ overflowX: 'auto' }}>
        <table className="ro-find">
          <thead><tr><th>Error</th><th className="num">Lines</th><th>Last</th><th>Read as</th></tr></thead>
          <tbody>
            {e.errors.map((x) => {
              const after = stop !== null && (x.last ?? 0) >= stop;
              const common = x.family_runs && x.runs_with && x.family_runs >= 3 && x.runs_with / x.family_runs >= 0.5;
              return (
                <tr key={x.fingerprint}>
                  <td className="small" style={{ whiteSpace: 'normal' }}>
                    <ErrClassChip e={{ exception_class: x.exception_class ?? '', sample_message: x.message ?? null }} />{' '}
                    <Link to={to.errors(cid, x.fingerprint)}><b className="mono">{(x.exception_class ?? 'error').split('.').pop()}</b></Link>
                    <span className="muted"> · {[...(x.sources ?? []), ...(x.executors ?? []).map((id) => `exec ${id}`)].slice(0, 3).join(', ') || '–'}{x.message ? ` · ${truncate(x.message, 90)}` : ''}</span>
                  </td>
                  <td className="num">{fmtNum(x.lines)}</td>
                  <td className="mono small">{fmtTime(x.last).slice(0, 8)}</td>
                  <td>{after ? <span className="ro-flag">after the stop · symptom</span>
                    : common ? <span className="ro-flag">not fatal · {x.runs_with} of {x.family_runs} runs</span>
                    : <span className="ro-flag warn">before the end · check it</span>}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function Problems({ cid, rows, loading, d }: { cid: string; rows: FindingRow[]; loading: boolean; d: RunSteps | null }) {
  const [all, setAll] = useState(false);
  const [deep, setDeep] = useState(false);
  // the plain problems first; GC, memory and log details explain them and are opened on demand
  const plain = rows.filter((f) => depthOf(f.category) === 0);
  const detail = rows.filter((f) => depthOf(f.category) > 0);
  const shown = [...(all ? plain : plain.slice(0, 6)), ...(deep ? detail : [])];
  // the query a finding is about (itself, or the one its stage ran for): its tables
  const groupOf = (f: FindingRow) => d?.groups.find((g) => g.kind === 'query' && g.ctx === f.spark_context_id && (
    (f.sql_execution_id !== null && g.id === f.sql_execution_id) || (f.stage_id !== null && g.stages.some((x) => x.stage_id === f.stage_id))));
  return (
    <section className="panel" id="ro-problems">
      <div className="panel-head">
        <div><h2>Problems in this run {loading ? null : <span className="ro-count">{rows.length}</span>}</h2><div className="note">Worst first. Each: what we saw, the likely cause, the fix, and the tables its query reads and writes.</div></div>
        {loading ? null : <Link className="btn small" to={to.findings(cid)}>All {fmtNum(rows.length)} findings</Link>}
      </div>
      <div className="panel-body stack" style={{ gap: 10 }}>
        {loading ? <p className="muted small">Loading…</p> : rows.length ? shown.map((f) => {
          const g = groupOf(f);
          const q = g && f.sql_execution_id === null ? <Link className="tchip" to={to.query(cid, g.ctx, g.id!)}>Query {g.id}</Link> : null;
          return <FindingPoints key={f.finding_id} cid={cid} f={f} reads={g?.tables_read} writes={g?.tables_written} links={q} />;
        }) : <p className="muted small">No problems found in this run.</p>}
        <div className="row" style={{ gap: 14 }}>
          {plain.length > 6 && <button className="linkish small" onClick={() => setAll(!all)}>{all ? 'Fewer ↑' : `${plain.length - 6} more findings ↓`}</button>}
          {detail.length > 0 && <button className="linkish small" title="Garbage collection, memory, exceptions in the logs and retries: they explain the problems above" onClick={() => setDeep(!deep)}>
            {deep ? 'Hide the details ↑' : `${detail.length} detailed ${detail.length === 1 ? 'finding' : 'findings'} (GC, memory, logs) ↓`}</button>}
        </div>
      </div>
    </section>
  );
}
