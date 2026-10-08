import { useMemo, useState } from 'react';
import { ErrClassChip } from '../components/ClusterTop';
import { Link } from 'react-router-dom';
import { api, type ErrorGroup, type FindingRow, type IncidentRow, type Ms } from '../api';
import { useCluster, useRunScopeCtx } from '../components/Shell';
import { runName } from '../runName';
import { HBars } from '../components/charts';
import { Async, DataLink, Empty, EntityChips, Panel, SeverityBadge, useScrollTo } from '../components/ui';
import { asList, fmtNum, fmtTime, fmtTs } from '../format';
import { useAsync, useQueryState } from '../hooks';
import { rowLinks, to } from '../links';
import { CONF, FixTips, MEANING } from '../problems';

type Occ = { executor_id: string | null; ts: Ms; fingerprint: string };
/** One exception group, with the finding that reports it and that finding's place in an incident. */
type Ex = ErrorGroup & { f: FindingRow | null; inc: IncidentRow | null; where: string[] };
/** The exception groups that are one problem (an incident problem, or the group on its own). */
type Prob = { key: string; lead: IncidentRow | null; leadF: FindingRow | null; exs: Ex[]; first: number | null };

const ROLE_ORDER: Record<string, number> = { root: 0, contributing: 1, effect: 2, related: 3, same: 4 };

/** Exceptions grouped into the problems they are evidence of (same root cause, same incident), or one card per exception group. */
export default function Errors() {
  const { cid } = useCluster();
  const [sp, setQ] = useQueryState();
  const focus = sp.get('fingerprint');
  const scopedRun = useRunScopeCtx().run;
  const view = sp.get('view') === 'all' ? 'all' : 'problems';
  const st = useAsync(
    async (s) => {
      const [groups, f, inc, occ] = await Promise.all([
        api.errors(cid, s),
        api.datasetOpt<FindingRow>(cid, 'findings', { limit: 5000, sort: 'finding_id' }, s),
        api.datasetOpt<IncidentRow>(cid, 'incidents', { limit: 5000 }, s),
        api.datasetOpt<Occ>(cid, 'log_errors', { limit: 5000, columns: 'executor_id,ts,fingerprint' }, s),
      ]);
      return { ...join(groups, f?.rows ?? [], inc?.rows ?? [], occ?.rows ?? []), occTruncated: (occ?.total ?? 0) > (occ?.rows.length ?? 0) };
    },
    [cid],
  );
  const showProblems = view === 'problems' && !!st.data?.hasInc;
  useScrollTo(focus ? `err-${focus}` : null, !!st.data);

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>{scopedRun ? 'Errors in this run' : 'Errors'}</h1>
          {scopedRun && (
            <p className="sub small">
              Only the lines that hit this run: on an executor while one of its tasks ran there, or from the driver while it ran.
            </p>
          )}
          <p className="sub">
            Exceptions from driver and executor logs. The same failure is usually logged several times (thrown on an executor, reported again on the driver,
            wrapped for the notebook), so they are grouped into the problem they come from, with where it happened and how to fix it.
          </p>
        </div>
        <div className="actions">
          {st.data?.hasInc && (
            <div className="seg" role="group" aria-label="View">
              <button className={showProblems ? 'on' : ''} aria-pressed={showProblems} onClick={() => setQ({ view: null }, true)}>
                By problem
              </button>
              <button className={!showProblems ? 'on' : ''} aria-pressed={!showProblems} onClick={() => setQ({ view: 'all' }, true)}>
                Every exception group
              </button>
            </div>
          )}
          <DataLink cid={cid} dataset="log_errors" />
        </div>
      </div>
      <Async state={st} label="Loading exceptions…">
        {(d) =>
          d.exs.length === 0 ? (
            <div className="panel">
              <Empty title="No exceptions">No stack traces were found in the driver or executor logs.</Empty>
            </div>
          ) : showProblems ? (
            <Problems cid={cid} d={d} focus={focus} />
          ) : (
            <AllGroups cid={cid} d={d} focus={focus} />
          )
        }
      </Async>
    </div>
  );
}

type Joined = ReturnType<typeof join> & { occTruncated?: boolean };

function join(groups: ErrorGroup[], findings: FindingRow[], incidents: IncidentRow[], occ: Occ[]) {
  const incBy = new Map(incidents.map((r) => [r.finding_id, r]));
  const fBy = new Map(findings.map((r) => [r.finding_id, r]));
  const fByFp = new Map<string, FindingRow>();
  for (const f of findings) if (f.fingerprint && !fByFp.has(f.fingerprint)) fByFp.set(f.fingerprint, f);
  const where = new Map<string, Set<string>>();
  for (const o of occ) {
    const s = where.get(o.fingerprint) ?? new Set();
    s.add(o.executor_id ? o.executor_id : 'driver');
    where.set(o.fingerprint, s);
  }
  const exs: Ex[] = groups.map((g) => {
    const f = fByFp.get(g.fingerprint) ?? null;
    const w = [...(where.get(g.fingerprint) ?? [])].sort((a, b) => (a === 'driver' ? -1 : b === 'driver' ? 1 : a.localeCompare(b, undefined, { numeric: true })));
    return { ...g, f, inc: f ? (incBy.get(f.finding_id) ?? null) : null, where: w };
  });
  // the lead row of each incident problem (the finding that stands for it), and which problem each finding is in
  const leadOf = new Map<string, IncidentRow>();
  for (const r of incidents) if (!leadOf.has(r.problem_id) || r.role !== 'same') leadOf.set(r.problem_id, r);
  const probs = new Map<string, Prob>();
  for (const e of exs) {
    const key = e.inc?.problem_id ?? `fp:${e.fingerprint}`;
    let p = probs.get(key);
    if (!p) {
      const lead = e.inc ? (leadOf.get(e.inc.problem_id) ?? e.inc) : null;
      probs.set(key, (p = { key, lead, leadF: lead ? (fBy.get(lead.finding_id) ?? null) : null, exs: [], first: null }));
    }
    p.exs.push(e);
    if (e.first_seen !== null && (p.first === null || e.first_seen < p.first)) p.first = e.first_seen;
  }
  for (const p of probs.values()) p.exs.sort((a, b) => exOrder(a) - exOrder(b) || (a.first_seen ?? Infinity) - (b.first_seen ?? Infinity));
  const list = [...probs.values()].sort(
    (a, b) =>
      (a.lead?.incident_rank ?? 1e9) - (b.lead?.incident_rank ?? 1e9) ||
      (ROLE_ORDER[a.lead?.role ?? 'same'] ?? 9) - (ROLE_ORDER[b.lead?.role ?? 'same'] ?? 9) ||
      (a.first ?? Infinity) - (b.first ?? Infinity) ||
      sum(b) - sum(a),
  );
  const probOfFinding = new Map<string, Prob>();
  for (const r of incidents) {
    const p = probs.get(r.problem_id);
    if (p) probOfFinding.set(r.finding_id, p);
  }
  return { exs, probs: list, probOfFinding, incBy, leadOf, occ, hasInc: incidents.length > 0 && exs.some((e) => e.inc) };
}

const sum = (p: Prob) => p.exs.reduce((n, e) => n + e.occurrences, 0);
const short = (cls: string) => cls.split('.').pop() ?? cls;
const isWrapper = (e: ErrorGroup) => /Py4JJavaError$/.test(e.exception_class) || (/SparkException$/.test(e.exception_class) && /aborted/i.test(e.sample_message ?? ''));
const onDriver = (e: Ex) => e.where.length > 0 && e.where.every((w) => w === 'driver');
/** Where it was thrown first: in your code on an executor, then other executor errors, then driver copies, then wrappers. */
function exOrder(e: Ex): number {
  if (isWrapper(e)) return 3;
  if (onDriver(e)) return 2;
  return e.user_frame ? 0 : 1;
}

/** What this copy of the error is, in the story of the problem. */
function exRole(e: Ex, p: Prob): string {
  const c = e.exception_class;
  if (/Py4JJavaError$/.test(c)) return 'how the notebook saw it (Py4J wrapper)';
  if (isWrapper(e)) return 'Spark gave up on the job';
  if (onDriver(e)) {
    const thrownElsewhere = p.exs.some((x) => x !== e && !onDriver(x) && (short(x.exception_class) === short(c) || (!!x.user_frame && x.user_frame === e.user_frame)));
    return thrownElsewhere ? 'reported again on the driver' : 'logged on the driver';
  }
  if (/FetchFailedException$/.test(c)) return 'a task could not read shuffle data';
  if (/IOException$/.test(c) && /connect/i.test(e.sample_message ?? '')) return 'the network error underneath';
  if (/OutOfMemoryError$/.test(c)) return 'the JVM ran out of heap';
  if (e.user_frame) return 'thrown in your code';
  return 'thrown in a task';
}

const whereLabel = (w: string[]) => (w.length ? w.map((x) => (x === 'driver' ? 'driver' : `executor ${x}`)).join(', ') : '');

/** The exception text to lead with: the most specific message (not a bare "Traceback" line or a wrapper). */
function headline(p: Prob): { cls: string; full: string; msg: string | null } {
  const good = (e: Ex) => !!e.sample_message && !/^Traceback/.test(e.sample_message) && !/^An error occurred while calling/.test(e.sample_message);
  const e = p.exs.find((x) => !isWrapper(x) && good(x)) ?? p.exs.find(good) ?? p.exs[0];
  return { cls: short(e.exception_class), full: e.exception_class, msg: e.sample_message };
}

// ---------------------------------------------------------------------------------------------- by problem

function Problems({ cid, d, focus }: { cid: string; d: Joined; focus: string | null }) {
  const n = d.exs.reduce((k, e) => k + e.occurrences, 0);
  const roots = d.probs.filter((p) => p.lead?.role === 'root' && d.probs.length > 1);
  return (
    <>
      <ErrorLanes d={d} focus={focus} />
      <p className="muted small" style={{ margin: '0 0 12px' }}>
        {fmtNum(n)} exception{n !== 1 ? 's' : ''} in {fmtNum(d.exs.length)} group{d.exs.length !== 1 ? 's' : ''} come from {fmtNum(d.probs.length)} problem
        {d.probs.length !== 1 ? 's' : ''}
        {roots.length ? `; ${roots.length} of them ${roots.length === 1 ? 'is a' : 'are'} root cause${roots.length === 1 ? '' : 's'}` : ''}. Most likely root causes first, then
        what they caused.
      </p>
      <div className="incidents">
        {groupByIncident(d.probs).map((g) =>
          g.lead ? (
            <section key={g.key} id={`incident-${g.key}`} className={`err-incident sev-${g.lead.incident_severity}`}>
              <header className="incident-head">
                <span className="fid">{g.lead.incident_id}</span>
                <h2>{g.lead.incident_title}</h2>
                <span className="grow" />
                <Link className="small" to={to.findings(cid, g.lead.finding_id)}>
                  Open incident on Findings
                </Link>
              </header>
              {g.lead.incident_impact && <p className="incident-impact">Cost: {g.lead.incident_impact}</p>}
              <ProblemCard cid={cid} p={g.probs[0]} d={d} focus={focus} />
              {g.probs.length > 1 && (
                <details className="err-then" open={g.probs.slice(1).some((p) => p.exs.some((e) => e.fingerprint === focus))}>
                  <summary className="lbl">
                    Then, logged because of it: {g.probs.length - 1} more {g.probs.length === 2 ? 'error' : 'errors'}
                    <span className="muted"> · {[...new Set(g.probs.slice(1).map((p) => short(headline(p).cls)))].slice(0, 4).join(', ')}</span>
                  </summary>
                  {g.probs.slice(1).map((p) => (
                    <ProblemCard key={p.key} cid={cid} p={p} d={d} focus={focus} compact />
                  ))}
                </details>
              )}
            </section>
          ) : (
            <ProblemCard key={g.key} cid={cid} p={g.probs[0]} d={d} focus={focus} />
          ),
        )}
      </div>
    </>
  );
}

/** Problems of the same incident together (root cause first, then what it caused, in time order). */
function groupByIncident(probs: Prob[]): { key: string; lead: IncidentRow | null; probs: Prob[] }[] {
  const out: { key: string; lead: IncidentRow | null; probs: Prob[] }[] = [];
  for (const p of probs) {
    const id = p.lead?.incident_id;
    const g = id ? out.find((x) => x.key === id) : undefined;
    if (g) g.probs.push(p);
    else out.push({ key: id ?? p.key, lead: p.lead, probs: [p] });
  }
  const rank = (p: Prob) => (p.lead?.role === 'root' ? 0 : 1);
  for (const g of out) g.probs.sort((a, b) => rank(a) - rank(b) || (a.first ?? Infinity) - (b.first ?? Infinity));
  return out;
}

/** One lane per driver / executor: each dot is one exception, at the time it was logged. */
function ErrorLanes({ d, focus }: { d: Joined; focus: string | null }) {
  const [, setQ] = useQueryState();
  const probOf = new Map(d.probs.flatMap((p) => p.exs.map((e) => [e.fingerprint, p] as const)));
  const timed = d.occ.filter((o) => o.ts !== null);
  if (timed.length < 2) return null;
  const t0 = Math.min(...timed.map((o) => o.ts!));
  const t1 = Math.max(...timed.map((o) => o.ts!));
  if (t1 <= t0) return null;
  const pct = (t: number) => 3 + ((t - t0) / (t1 - t0)) * 94;
  const lanes = [...new Set(timed.map((o) => o.executor_id || 'driver'))].sort((a, b) =>
    a === 'driver' ? -1 : b === 'driver' ? 1 : a.localeCompare(b, undefined, { numeric: true }),
  );
  const untimed = d.occ.length - timed.length;
  return (
    <Panel
      title="Where and when exceptions were thrown"
      note={`One dot per exception, colored by problem. Click one to jump to its problem.${untimed ? ` ${untimed === 1 ? 'One exception has' : `${untimed} exceptions have`} no timestamp and ${untimed === 1 ? 'is' : 'are'} not shown.` : ''}${d.occTruncated ? ' Only the first 5,000 exceptions are drawn.' : ''}`}
    >
      <div className="inc-strip">
        {lanes.map((l) => (
          <div key={l} className="inc-strip-row">
            <span className="inc-strip-label">{l === 'driver' ? 'Driver' : `Executor ${l}`}</span>
            <span className="inc-strip-track">
              {timed
                .filter((o) => (o.executor_id || 'driver') === l)
                .map((o, k) => {
                  const p = probOf.get(o.fingerprint);
                  const e = p?.exs.find((x) => x.fingerprint === o.fingerprint);
                  const idx = p ? d.probs.indexOf(p) : -1;
                  return (
                    <button
                      key={k}
                      tabIndex={-1}
                      aria-hidden
                      className={`err-dot ${o.fingerprint === focus ? 'on' : ''}`}
                      style={{ left: `${pct(o.ts!)}%`, background: `var(--series-${(idx % 6) + 1})` }}
                      title={`${fmtTime(o.ts)} ${e ? short(e.exception_class) : o.fingerprint}${p?.lead ? ` (${p.lead.kind})` : ''}`}
                      onClick={() => setQ({ fingerprint: o.fingerprint }, true)}
                    />
                  );
                })}
            </span>
          </div>
        ))}
        <div className="inc-strip-row axis">
          <span />
          <span className="inc-strip-track">
            <span style={{ left: 0 }}>{fmtTime(t0)}</span>
            <span style={{ right: 0 }}>{fmtTime(t1)}</span>
          </span>
        </div>
      </div>
      <div className="err-legend">
        {d.probs.map((p, i) => (
          <a key={p.key} href={`#prob-${p.key}`}>
            <span className="sw" style={{ background: `var(--series-${(i % 6) + 1})` }} />
            {p.lead?.kind ?? short(p.exs[0].exception_class)}
          </a>
        ))}
      </div>
    </Panel>
  );
}

function ProblemCard({ cid, p, d, focus, compact }: { cid: string; p: Prob; d: Joined; focus: string | null; compact?: boolean }) {
  const [, setQ] = useQueryState();
  const focused = p.exs.find((e) => e.fingerprint === focus);
  const shown = focused ?? p.exs.find((e) => e.user_frame) ?? p.exs.find((e) => asList(e.sample_stack).length) ?? p.exs[0];
  const lead = p.lead;
  const kind = lead?.kind ?? 'error';
  const h = headline(p);
  const sev = p.leadF?.severity ?? lead?.incident_severity ?? 'medium';
  const cause = lead?.caused_by ? d.incBy.get(lead.caused_by) : null;
  const causeProb = lead?.caused_by ? d.probOfFinding.get(lead.caused_by) : null;
  const userFrame = p.exs.find((e) => e.user_frame)?.user_frame ?? null;
  const links = lead
    ? rowLinks(cid, {
        spark_context_id: lead.spark_context_id,
        stage_id: lead.stage_id,
        stage_attempt: lead.stage_attempt,
        spark_job_id: lead.spark_job_id,
        sql_execution_id: lead.sql_execution_id,
        executor_id: lead.executor_id,
      })
    : [];
  return (
    <section id={`prob-${p.key}`} className={`incident err-prob sev-${sev} ${compact ? 'compact' : ''}`}>
      <header className="incident-head">
        <SeverityBadge sev={sev} />
        <span className={`err-kind sev-${sev}`}>{kind}</span>
        <ErrClassChip e={{ exception_class: h.full, sample_message: h.msg }} />
        {lead?.stages && <span className="muted small">stage {lead.stages}</span>}
        <span className="grow" />
        {lead && <span className="muted small nowrap">{fmtTime(p.first)}</span>}
      </header>
      <h2 className="err-headline">
        <span className="cls">{h.cls}</span>
        {h.msg && <span className="msg">: {h.msg}</span>}
      </h2>
      {lead?.role === 'root' && d.probs.length > 1 && <div className="err-role root">Most likely root cause of {lead.incident_id}.</div>}
      {cause && (
        <div className="because">
          <span className="why">Caused by</span>{' '}
          {causeProb ? <a href={`#prob-${causeProb.key}`}>{cause.kind}</a> : <Link to={to.findings(cid, cause.finding_id)}>{cause.kind}</Link>}
          {lead?.because ? `: ${lead.because}` : ''}
          {lead?.confidence && <span className={`conf conf-${lead.confidence}`}>{CONF[lead.confidence]}</span>}
        </div>
      )}
      {!compact && MEANING[kind] && <p className="muted small" style={{ margin: 0 }}>{MEANING[kind]}</p>}
      {links.length > 0 && <EntityChips links={links} />}
      <div className="err-cols">
        <div className="err-seen">
          <div className="lbl">
            Seen as {p.exs.length} exception group{p.exs.length !== 1 ? 's' : ''} ({fmtNum(sum(p))} in the logs)
          </div>
          <table className="tbl err-seen-tbl">
            <thead>
              <tr>
                <th>Exception</th>
                <th>Where</th>
                <th className="num">Count</th>
                <th>First</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {p.exs.map((e) => (
                <tr
                  key={e.fingerprint}
                  id={`err-${e.fingerprint}`}
                  className={`${e === shown ? 'sel' : ''} ${e.fingerprint === focus ? 'flash' : ''}`}
                  onClick={() => setQ({ fingerprint: e.fingerprint }, true)}
                  tabIndex={0}
                  aria-selected={e === shown}
                  onKeyDown={(ev) => (ev.key === 'Enter' || ev.key === ' ') && (ev.preventDefault(), setQ({ fingerprint: e.fingerprint }, true))}
                >
                  <td>
                    <div className="mono cls" title={e.exception_class}>
                      {short(e.exception_class)}
                    </div>
                    <div className="muted small">{exRole(e, p)}</div>
                  </td>
                  <td className="small">{whereLabel(e.where)}</td>
                  <td className="num small">{fmtNum(e.occurrences)}×</td>
                  <td className="small nowrap">{fmtTime(e.first_seen)}</td>
                  <td className="small nowrap">
                    {e.sample_file_path && e.sample_seq !== null && (
                      <Link to={to.logLine(cid, e.sample_file_path, e.sample_seq)} onClick={(ev) => ev.stopPropagation()}>
                        Log line
                      </Link>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <details className="err-stack" open={!compact || shown === focused}>
          <summary className="lbl">
            Stack of {short(shown.exception_class)}
            {shown.where.length ? ` on ${whereLabel(shown.where)}` : ''}
          </summary>
          {shown.sample_message && <p className="msg mono small">{shown.sample_message}</p>}
          {asList(shown.sample_stack).length > 0 ? (
            <Frames frames={asList(shown.sample_stack)} user={shown.user_frame} />
          ) : (
            <p className="muted small">No stack frames were logged with this one{userFrame && !shown.user_frame ? '; select the row with your code in it' : ''}.</p>
          )}
        </details>
      </div>
      {!compact && <div className="fix-callout">
        {userFrame ? (
          <span>
            <b>Where to fix:</b> <span className="mono">{userFrame.trim()}</span>
          </span>
        ) : (
          <span className="muted">No frame from your code: all frames are Spark or JVM code. Start from the stage and query above.</span>
        )}
      </div>}
      <FixTips kind={kind} fix={p.leadF?.fix} compact={compact} />
    </section>
  );
}

function Frames({ frames, user }: { frames: string[]; user: string | null }) {
  const norm = (s: string) => s.trim();
  const hasUser = !!user && frames.some((f) => norm(f) === norm(user));
  return (
    <div className="frames">
      {user && !hasUser && (
        <>
          <div className="frame user">
            <span className="fixhere">Fix here</span>
            {user}
          </div>
          <div className="frame muted">(first frame in your code, deeper in the stack)</div>
        </>
      )}
      {frames.map((f, i) => {
        const isUser = !!user && norm(f) === norm(user);
        return (
          <div key={i} className={`frame ${isUser ? 'user' : ''}`}>
            {isUser && <span className="fixhere">Fix here</span>}
            {f}
          </div>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------------------------- every group

function AllGroups({ cid, d, focus }: { cid: string; d: Joined; focus: string | null }) {
  const [, setQ] = useQueryState();
  const [text, setText] = useState('');
  const probOf = useMemo(() => new Map(d.probs.flatMap((p) => p.exs.map((e) => [e.fingerprint, p] as const))), [d]);
  const rows = useMemo(() => {
    const t = text.trim().toLowerCase();
    const all = [...d.exs].sort((a, b) => b.occurrences - a.occurrences);
    return t ? all.filter((e) => [e.exception_class, e.sample_message, e.user_frame, e.inc?.kind].some((x) => x?.toLowerCase().includes(t))) : all;
  }, [d, text]);
  return (
    <>
      <div style={{ marginBottom: 16 }}>
        <Panel title="Exception groups by count" note="How many times each exception group was logged. Select one to jump to its stack and where to fix it.">
          <HBars
            labelWidth={260}
            data={[...d.exs]
              .sort((a, b) => b.occurrences - a.occurrences)
              .slice(0, 10)
              .map((e) => ({
                key: e.fingerprint,
                label: short(e.exception_class),
                sub: e.user_frame ? 'at ' + e.user_frame.trim().replace(/^at /, '') : (e.sample_message ?? undefined),
                parts: [{ value: e.occurrences, color: 'var(--series-1)', name: 'occurrences' }],
                display: `${fmtNum(e.occurrences)}×`,
                selected: focus === e.fingerprint,
                title: `${e.exception_class}${e.sample_message ? `\n${e.sample_message}` : ''}`,
                onClick: () => setQ({ fingerprint: e.fingerprint }, true),
              }))}
          />
        </Panel>
      </div>
      <div className="row" style={{ justifyContent: 'flex-end', marginBottom: 10 }}>
        <input className="input" style={{ width: 280 }} value={text} onChange={(e) => setText(e.target.value)} placeholder="Filter exceptions" aria-label="Filter by class, message, frame or problem" />
      </div>
      <div className="panel">
        {rows.length === 0 ? (
          <Empty title="No exceptions match">Clear the filter to see all {fmtNum(d.exs.length)} groups.</Empty>
        ) : (
          rows.map((e) => <ErrorCard key={e.fingerprint} cid={cid} e={e} p={probOf.get(e.fingerprint) ?? null} focus={focus === e.fingerprint} />)
        )}
      </div>
    </>
  );
}

/** Where an exception's lines happened: the stages (executor lines) and the runs. */
function ErrorHits({ cid, e }: { cid: string; e: Ex }) {
  const { runs, run, choose } = useRunScopeCtx();
  const stages = e.hit_stages ?? [];
  const hit = e.hit_runs ?? [];
  if (!stages.length && !hit.length) return null;
  const byKey = new Map(runs.map((r) => [r.run_key, r]));
  const name = (k: string) => {
    const r = byKey.get(k);
    return r ? `${runName(r)} (${fmtTime(r.start_time).slice(0, 5)})` : k;
  };
  return (
    <div className="small" style={{ margin: '6px 0' }}>
      {stages.length > 0 && (
        <div>
          <b>Hit:</b>{' '}
          {stages.map((s, i) => (
            <span key={i}>
              {i ? ', ' : ''}
              <Link to={to.stages(cid, s.spark_context_id, s.stage_id, s.stage_attempt)}>stage {s.stage_id}{s.stage_attempt ? `.${s.stage_attempt}` : ''}</Link>
              {s.sql_execution_id !== null ? <span className="muted"> (query {s.sql_execution_id})</span> : null}
              <span className="muted"> · {fmtNum(s.lines)} {s.lines === 1 ? 'line' : 'lines'}</span>
            </span>
          ))}
        </div>
      )}
      {!run && hit.length > 0 && (
        hit.length <= 3 ? (
          <div>
            <b>In {hit.length === 1 ? 'run' : 'runs'}:</b>{' '}
            {hit.map((h, i) => (
              <span key={h.run_key}>
                {i ? ', ' : ''}
                <a href="#" onClick={(ev) => { ev.preventDefault(); choose(h.run_key); }}>{name(h.run_key)}</a>
              </span>
            ))}
          </div>
        ) : (
          <div className="muted">
            Logged while {fmtNum(hit.length)} runs were going{stages.length ? '' : ' (a driver log line does not say which run it belongs to)'}.
          </div>
        )
      )}
    </div>
  );
}

function ErrorCard({ cid, e, p, focus }: { cid: string; e: Ex; p: Prob | null; focus: boolean }) {
  const [, setQ] = useQueryState();
  const frames = asList(e.sample_stack);
  const sources = asList(e.sources);
  const i = e.inc;
  const links = i
    ? rowLinks(cid, {
        spark_context_id: i.spark_context_id,
        stage_id: i.stage_id,
        stage_attempt: i.stage_attempt,
        spark_job_id: i.spark_job_id,
        sql_execution_id: i.sql_execution_id,
        executor_id: i.executor_id,
        finding_id: i.finding_id,
      })
    : [];
  return (
    <article id={`err-${e.fingerprint}`} className={`err-card ${focus ? 'flash' : ''}`}>
      <div className="row" style={{ gap: 10, alignItems: 'baseline', justifyContent: 'space-between' }}>
        <div className="cls">{e.exception_class}</div>
        <span className="badge mono" title="Fingerprint: class + top 3 frames">
          {e.fingerprint}
        </span>
      </div>
      {e.sample_message && <p className="msg">{e.sample_message}</p>}
      <div className="err-meta">
        <span>
          <b>{fmtNum(e.occurrences)}</b> {e.occurrences === 1 ? 'occurrence' : 'occurrences'}
        </span>
        {e.where.length > 0 && (
          <span>
            on <b>{whereLabel(e.where)}</b>
          </span>
        )}
        <span>
          first <b>{fmtTs(e.first_seen)}</b>
        </span>
        <span>
          last <b>{fmtTs(e.last_seen)}</b>
        </span>
        {sources.length > 0 && <span>in {sources.join(', ')}</span>}
      </div>
      {i && p && (
        <p className="inc-ref small">
          Part of{' '}
          <a
            href={`?fingerprint=${e.fingerprint}`}
            onClick={(ev) => {
              ev.preventDefault();
              setQ({ view: null, fingerprint: e.fingerprint }, true);
            }}
          >
            {p.lead?.kind ?? i.kind}
          </a>{' '}
          ({exRole(e, p)}) in {i.incident_id}: {i.incident_title}
        </p>
      )}
      <ErrorHits cid={cid} e={e} />
      {frames.length > 0 && <Frames frames={frames} user={e.user_frame} />}
      <div className="fix-callout">
        {e.user_frame ? (
          <span>
            <b>Where to fix:</b> <span className="mono">{e.user_frame.trim()}</span>
          </span>
        ) : (
          <span className="muted">All frames are framework code; look at the call site of the failing job or query.</span>
        )}
      </div>
      <div className="chips" style={{ marginTop: 10 }}>
        {e.sample_file_path && e.sample_seq !== null && (
          <Link className="chip" to={to.logLine(cid, e.sample_file_path, e.sample_seq)}>
            <span className="k">Log</span>First occurrence
          </Link>
        )}
        <Link className="chip" to={to.logs(cid, { q: e.exception_class })}>
          <span className="k">Logs</span>Search this class
        </Link>
      </div>
      {links.length > 0 && (
        <div style={{ marginTop: 8 }}>
          <EntityChips links={links} />
        </div>
      )}
    </article>
  );
}
