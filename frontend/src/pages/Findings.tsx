import { useMemo, useState } from 'react';
import { api, type FindingRow, type IncidentRow, type Severity } from '../api';
import { useCluster } from '../components/Shell';
import { HBars } from '../components/charts';
import { Async, DataLink, Empty, EntityChips, Panel, SeverityBadge, ToggleChips, sevColor, useScrollTo } from '../components/ui';
import { fmtDuration, fmtNum, fmtTime, fmtTs } from '../format';
import { useAsync, useQueryState } from '../hooks';
import { rowLinks, to } from '../links';
import { CONF, FAILURE_KINDS, FixTips } from '../problems';
import { plainFirst } from '../components/FindingPoints';
import { groupIncidents, type Incident, type Joined, type Problem } from '../incidents';

const SEVS: Severity[] = ['high', 'medium', 'low'];


/** Findings, grouped into incidents (root cause -> effects) by default, or the flat list. */
export default function Findings() {
  const { cid } = useCluster();
  const [sp, setQ] = useQueryState();
  const focus = sp.get('finding');
  const view = sp.get('view') === 'all' ? 'all' : 'incidents';
  const st = useAsync(
    async (s) => {
      const [f, inc] = await Promise.all([
        api.dataset<FindingRow>(cid, 'findings', { limit: 5000, sort: 'finding_id' }, s),
        api.datasetOpt<IncidentRow>(cid, 'incidents', { limit: 5000 }, s),
      ]);
      const byId = new Map((inc?.rows ?? []).map((r) => [r.finding_id, r]));
      return { total: f.total, rows: f.rows.map((r): Joined => ({ ...r, inc: byId.get(r.finding_id) ?? null })), hasInc: !!inc?.rows.length };
    },
    [cid],
  );
  const showIncidents = view === 'incidents' && st.data?.hasInc !== false;
  useScrollTo(focus ? `finding-${focus}` : null, !!st.data);

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Findings</h1>
          <p className="sub">
            What went wrong and why. Findings that are the same problem are merged, and problems are chained cause → effect, so each incident starts from its most
            likely root cause.
          </p>
        </div>
        <div className="actions">
          <div className="seg" role="group" aria-label="View">
            <button className={showIncidents ? 'on' : ''} aria-pressed={showIncidents} onClick={() => setQ({ view: null }, true)}>
              Incidents
            </button>
            <button className={!showIncidents ? 'on' : ''} aria-pressed={!showIncidents} onClick={() => setQ({ view: 'all' }, true)}>
              Every finding
            </button>
          </div>
          <DataLink cid={cid} dataset={showIncidents ? 'incidents' : 'findings'} />
        </div>
      </div>
      <Async state={st} label="Loading findings…">
        {(t) =>
          t.rows.length === 0 ? (
            <div className="panel">
              <Empty title="No problems found">
                Checked task skew, disk spill, GC time, tiny tasks, out-of-memory, lost or killed executors, task retries, failed stages and queries, and
                known problem lines in the driver and executor logs. Nothing crossed a threshold.
              </Empty>
            </div>
          ) : showIncidents ? (
            <Incidents cid={cid} rows={t.rows} focus={focus} />
          ) : (
            <AllFindings cid={cid} rows={t.rows} total={t.total} focus={focus} />
          )
        }
      </Async>
    </div>
  );
}

// ---------------------------------------------------------------------------------------------- incidents

function Incidents({ cid, rows, focus }: { cid: string; rows: Joined[]; focus: string | null }) {
  const incs = useMemo(() => groupIncidents(rows), [rows]);
  const failed = incs.filter((i) => i.problems.some((p) => FAILURE_KINDS.has(p.lead.inc!.kind)));
  return (
    <>
      <IncidentStrip incs={incs} />
      <p className="muted small" style={{ margin: '0 0 12px' }}>
        {fmtNum(rows.length)} findings make {fmtNum(incs.length)} incident{incs.length !== 1 ? 's' : ''}
        {failed.length ? `, ${failed.length} of them failures` : ''}. Failures first, then by severity. Links between problems are a best guess from shared
        stages, executors and timing; each one says why it was made.
      </p>
      <div className="incidents">
        {incs.map((inc) => (
          <IncidentCard key={inc.id} cid={cid} inc={inc} focus={focus} />
        ))}
      </div>
      <LooseFindings cid={cid} rows={rows.filter((r) => !r.inc)} focus={focus} />
    </>
  );
}


/** Findings that no incident took in (for example the ones added later from the runs: waiting for cores, the cores
 * were full, MERGE reading too much): listed under the incidents so nothing is hidden. */
function LooseFindings({ cid, rows, focus }: { cid: string; rows: Joined[]; focus: string | null }) {
  if (!rows.length) return null;
  const sorted = [...rows].sort((a, b) => plainFirst(a, b) || (a.ts ?? 0) - (b.ts ?? 0));
  return (
    <Panel title={`Not part of an incident · ${fmtNum(rows.length)}`} note="Findings that are not chained to another problem: each stands on its own.">
      <div className="stack" style={{ gap: 10 }}>
        {sorted.map((f) => (
          <div key={f.finding_id} className={`problem ${f.finding_id === focus ? 'flash' : ''}`}>
            <div className="problem-head">
              <span className={`kind sev-${f.severity}`}>{f.category}</span>
              {f.entity && <span className="entity">{f.entity}</span>}
            </div>
            {f.evidence && <p className="evidence">{cleanEvidence(f.evidence)}</p>}
            {f.fix && <p className="small" style={{ margin: '4px 0' }}><b>Fix:</b> {f.fix}</p>}
            <div className="problem-foot">
              <EntityChips links={rowLinks(cid, { ...f, finding_id: null })} />
              <span className="seen">Finding <a id={`finding-${f.finding_id}`} href={`#finding-${f.finding_id}`}>{f.finding_id}</a></span>
            </div>
          </div>
        ))}
      </div>
    </Panel>
  );
}

/** One row per incident on a shared clock: when each began and ended, so overlapping incidents are visible. */
function IncidentStrip({ incs }: { incs: Incident[] }) {
  const pts = incs.flatMap((i) => [i.head.incident_start, i.head.incident_end]).filter((x): x is number => x !== null);
  if (pts.length < 2 || incs.length < 2) return null;
  const t0 = Math.min(...pts);
  const t1 = Math.max(...pts);
  if (t1 <= t0) return null;
  const pct = (t: number) => ((t - t0) / (t1 - t0)) * 100;
  return (
    <Panel title="When each incident happened" note="Click an incident to jump to it. Dots are the problems inside it.">
      <div className="inc-strip">
        {incs.map((i) => {
          const s = i.head.incident_start ?? t0;
          const e = i.head.incident_end ?? s;
          return (
            <a key={i.id} className="inc-strip-row" href={`#incident-${i.id}`}>
              <span className="inc-strip-label">
                <b>{i.id}</b> {i.head.incident_title}
              </span>
              <span className="inc-strip-track">
                <span className={`inc-strip-bar sev-${i.head.incident_severity}`} style={{ left: `${pct(s)}%`, width: `max(4px, ${pct(e) - pct(s)}%)` }} />
                {i.problems
                  .filter((p) => p.ts !== null)
                  .map((p) => (
                    <span key={p.id} className={`inc-strip-dot sev-${p.lead.severity}`} style={{ left: `${pct(p.ts!)}%` }} title={`${fmtTime(p.ts)} ${p.lead.inc!.kind}`} />
                  ))}
              </span>
            </a>
          );
        })}
        <div className="inc-strip-row axis">
          <span />
          <span className="inc-strip-track">
            <span style={{ left: 0 }}>{fmtTime(t0)}</span>
            <span style={{ right: 0 }}>{fmtTime(t1)}</span>
          </span>
        </div>
      </div>
    </Panel>
  );
}

function IncidentCard({ cid, inc, focus }: { cid: string; inc: Incident; focus: string | null }) {
  const h = inc.head;
  const root = inc.problems.find((p) => p.lead.inc!.role === 'root') ?? inc.problems[0];
  const contributing = inc.problems.filter((p) => p !== root && (p.lead.inc!.role === 'contributing' || p.lead.inc!.role === 'related'));
  const chain = inc.problems.filter((p) => p !== root && !contributing.includes(p));
  const span = h.incident_start !== null && h.incident_end !== null ? h.incident_end - h.incident_start : null;
  const hasFocus = focus !== null && inc.byFinding.has(focus);
  return (
    <section id={`incident-${inc.id}`} className={`incident sev-${h.incident_severity}`}>
      <header className="incident-head">
        <SeverityBadge sev={h.incident_severity} />
        <span className="fid">{inc.id}</span>
        <h2>{h.incident_title}</h2>
        <span className="grow" />
        <span className="muted small nowrap">
          {fmtTime(h.incident_start)}
          {span ? ` → ${fmtTime(h.incident_end)} (${fmtDuration(span)})` : ''}
        </span>
      </header>
      {h.incident_impact && <p className="incident-impact">Cost: {h.incident_impact}</p>}
      <div className="incident-body">
        <div className="root-box">
          <div className="lbl">{inc.problems.length > 1 && root.lead.inc!.role === 'root' && chain.length ? 'Most likely root cause' : 'What was found'}</div>
          <ProblemView cid={cid} p={root} focus={focus} big />
          <FixTips kind={root.lead.inc!.kind} fix={root.lead.fix} stackHref={stackHref(cid, root)} />
        </div>
        {chain.length > 0 && (
          <div className="inc-spread">
            <div className="lbl">How it spread</div>
            <ol className="spread-steps">
              {chain.map((p) => (
                <li key={p.id}>
                  <span className="when">{p.ts !== null ? fmtTime(p.ts) : ''}</span>
                  <div className="what">
                    <ProblemView cid={cid} p={p} focus={focus} />
                    {p.lead.inc!.because && (
                      <div className="because">
                        <span className="why">Why linked:</span> {p.lead.inc!.because}
                        {p.lead.inc!.caused_by && inc.byFinding.get(p.lead.inc!.caused_by) && (
                          <> (after <b>{inc.byFinding.get(p.lead.inc!.caused_by)!.lead.inc!.kind}</b>)</>
                        )}
                        {p.lead.inc!.confidence && <span className={`conf conf-${p.lead.inc!.confidence}`}>{CONF[p.lead.inc!.confidence]}</span>}
                      </div>
                    )}
                  </div>
                </li>
              ))}
            </ol>
          </div>
        )}
        {contributing.length > 0 && (
          <div className="inc-spread">
            <div className="lbl">{chain.length ? 'Also found here' : 'Also in this query'}</div>
            {contributing.map((p) => (
              <div key={p.id} className="also">
                <ProblemView cid={cid} p={p} focus={focus} />
                <FixTips kind={p.lead.inc!.kind} fix={p.lead.fix} stackHref={stackHref(cid, p)} compact />
              </div>
            ))}
          </div>
        )}
      </div>
      {hasFocus && <div className="muted small">Finding {focus} is part of this incident.</div>}
    </section>
  );
}

/** The real error message among the merged findings (a Python traceback wrapper only says "Traceback ..."). */
function errorHeadline(fs: Joined[]): { cls: string; msg: string } | null {
  const vague = /^(Traceback \(most recent call last\):?|An error occurred while calling .*)$/;
  for (const f of fs) {
    if (f.category !== 'exception' || !f.entity) continue;
    const m = f.evidence?.match(/e\.g\. (.+)$/);
    if (m && !vague.test(m[1].trim())) return { cls: f.entity.split('.').pop() ?? f.entity, msg: m[1].trim() };
  }
  return null;
}

/** Drop a trailing "e.g. Traceback (most recent call last):": it says nothing. */
const cleanEvidence = (t: string) => t.replace(/\s*e\.g\. Traceback \(most recent call last\):?\s*$/, '');

/** The Errors page for the problem's own stack (the exception with your frame in it, when there is one). */
function stackHref(cid: string, p: Problem): string | null {
  const fp = [p.lead, ...p.same].find((x) => x.fingerprint)?.fingerprint;
  return fp ? to.errors(cid, fp) : null;
}

function ProblemView({ cid, p, focus, big }: { cid: string; p: Problem; focus: string | null; big?: boolean }) {
  const f = p.lead;
  const i = f.inc!;
  const all = [f, ...p.same];
  // the real error message, for error problems whose lead finding only shows a wrapper
  const errLine = /error/.test(i.kind) ? errorHeadline(all) : null;
  const links = rowLinks(cid, {
    ...f,
    spark_context_id: f.spark_context_id ?? i.spark_context_id,
    stage_id: f.stage_id ?? i.stage_id,
    stage_attempt: f.stage_id !== null ? f.stage_attempt : i.stage_attempt,
    spark_job_id: f.spark_job_id ?? i.spark_job_id,
    sql_execution_id: f.sql_execution_id ?? i.sql_execution_id,
    executor_id: f.executor_id ?? i.executor_id,
    finding_id: null,
  });
  // the log line and error links of the merged findings too: the evidence behind the conclusion
  // (one log line and up to two error groups: enough to check it, not every copy)
  for (const s of p.same)
    for (const l of rowLinks(cid, { ...s, finding_id: null })) {
      const n = links.filter((x) => x.type === l.type).length;
      if ((l.type === 'log' ? n < 1 : l.type === 'error' ? n < 2 : false) && !links.some((x) => x.href === l.href)) links.push(l);
    }
  return (
    <div className={`problem ${big ? 'big' : ''} ${all.some((x) => x.finding_id === focus) ? 'flash' : ''}`}>
      <div className="problem-head">
        <span className={`kind sev-${f.severity}`}>{i.kind}</span>
        {f.entity && <span className="entity">{f.entity}</span>}
        {(i.stages || i.executors) && (
          <span className="muted small">
            {i.stages ? `stage ${i.stages}` : ''}
            {i.stages && i.executors ? ' · ' : ''}
            {i.executors ? `executor ${i.executors}` : ''}
          </span>
        )}
      </div>
      {errLine && (
        <p className="err-line">
          <b className="mono">{errLine.cls}</b>: {errLine.msg}
        </p>
      )}
      {f.evidence && <p className="evidence">{cleanEvidence(f.evidence)}</p>}
      <div className="problem-foot">
        <EntityChips links={links} />
        <span className="seen">
          {all.length > 1 ? `Seen ${all.length} ways: ` : 'Finding '}
          {all.map((x, k) => (
            <span key={x.finding_id}>
              {k > 0 && ', '}
              <a id={`finding-${x.finding_id}`} href={`#finding-${x.finding_id}`} title={`${x.category}: ${x.evidence ?? ''}`}>
                {x.finding_id}
              </a>
            </span>
          ))}
        </span>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------------------------- flat list

function AllFindings({ cid, rows: all, total, focus }: { cid: string; rows: Joined[]; total: number; focus: string | null }) {
  const [sp, setQ] = useQueryState();
  const sevs = (sp.get('severity') ?? '').split(',').filter(Boolean) as Severity[];
  const cat = sp.get('category') ?? '';
  const [text, setText] = useState('');
  const cats = useMemo(() => {
    const m = new Map<string, number>();
    all.forEach((r) => m.set(r.category, (m.get(r.category) ?? 0) + 1));
    return [...m.entries()].sort((a, b) => b[1] - a[1]);
  }, [all]);
  const rows = useMemo(() => {
    const t = text.trim().toLowerCase();
    return all.filter(
      (r) =>
        (!sevs.length || sevs.includes(r.severity)) &&
        (!cat || r.category === cat) &&
        (!t || [r.category, r.entity, r.evidence, r.fix, r.inc?.incident_title].some((x) => x?.toLowerCase().includes(t))),
    ).sort(plainFirst);
  }, [all, sevs.join(), cat, text]);
  return (
    <>
      <div style={{ marginBottom: 16 }}>
        <FindingsByCategory rows={all} active={cat} onPick={(c) => setQ({ category: c === cat ? null : c }, true)} />
      </div>
      <div className="panel" style={{ marginBottom: 16 }}>
        <div className="panel-body filters">
          <div className="group">
            <span className="lbl">Severity</span>
            <ToggleChips<Severity>
              options={SEVS}
              value={sevs}
              onChange={(v) => setQ({ severity: v.join(',') || null }, true)}
              dot={(s) => sevColor(s)}
              render={(s) => s[0].toUpperCase() + s.slice(1)}
            />
          </div>
          <label className="group">
            <span className="lbl">Category</span>
            <select className="select" value={cat} onChange={(e) => setQ({ category: e.target.value || null }, true)}>
              <option value="">All categories</option>
              {cats.map(([c, n]) => (
                <option key={c} value={c}>
                  {c} ({n})
                </option>
              ))}
            </select>
          </label>
          <label className="group grow" style={{ minWidth: 220 }}>
            <span className="lbl">Text</span>
            <input className="input" value={text} onChange={(e) => setText(e.target.value)} placeholder="Entity, evidence, fix, incident…" />
          </label>
        </div>
      </div>
      <div className="panel">
        {rows.length === 0 ? (
          <Empty title="No findings match">Clear a filter to see more.</Empty>
        ) : (
          <>
            {rows.map((f) => {
              const i = f.inc;
              const links = rowLinks(cid, {
                ...f,
                spark_context_id: f.spark_context_id ?? i?.spark_context_id ?? null,
                stage_id: f.stage_id ?? i?.stage_id ?? null,
                stage_attempt: f.stage_id !== null ? f.stage_attempt : (i?.stage_attempt ?? null),
                spark_job_id: f.spark_job_id ?? i?.spark_job_id ?? null,
                sql_execution_id: f.sql_execution_id ?? i?.sql_execution_id ?? null,
                executor_id: f.executor_id ?? i?.executor_id ?? null,
                finding_id: null,
              });
              return (
                <article key={f.finding_id} id={`finding-${f.finding_id}`} className={`finding ${focus === f.finding_id ? 'flash' : ''}`}>
                  <div className="fid">{f.finding_id}</div>
                  <div style={{ minWidth: 0 }}>
                    <div className="headline">
                      <SeverityBadge sev={f.severity} />
                      <span className="cat">{f.category}</span>
                      {f.entity && <span className="entity">{f.entity}</span>}
                      <span className="grow" />
                      <span className="muted small nowrap">{fmtTs(f.ts)}</span>
                    </div>
                    {f.evidence && <p className="evidence">{f.evidence}</p>}
                    {f.fix && (
                      <p className="fix">
                        <b>Fix:</b> {f.fix}
                      </p>
                    )}
                    {i && (
                      <p className="inc-ref small">
                        <a href={`?view=incidents&finding=${f.finding_id}`} onClick={(e) => { e.preventDefault(); setQ({ view: null, finding: f.finding_id }, true); }}>
                          {i.incident_id}: {i.incident_title}
                        </a>
                        {' · '}
                        {i.role === 'root' ? 'most likely root cause' : i.role === 'same' ? `same problem as ${i.caused_by}` : i.role === 'effect' ? `caused by ${i.caused_by}` : i.role}
                        {i.role === 'effect' && i.because ? `: ${i.because}` : ''}
                      </p>
                    )}
                    <div style={{ marginTop: 8 }}>
                      <EntityChips links={links} />
                    </div>
                  </div>
                </article>
              );
            })}
            <div className="pager">
              {fmtNum(rows.length)} of {fmtNum(total)} findings
            </div>
          </>
        )}
      </div>
    </>
  );
}

const SEV_PARTS: { k: Severity; color: string; label: string }[] = [
  { k: 'high', color: 'var(--sev-high)', label: 'High' },
  { k: 'medium', color: 'var(--sev-medium)', label: 'Medium' },
  { k: 'low', color: 'var(--sev-low)', label: 'Low' },
];

function FindingsByCategory({ rows, active, onPick }: { rows: FindingRow[]; active: string; onPick: (c: string) => void }) {
  const data = useMemo(() => {
    const m = new Map<string, Record<string, number>>();
    for (const r of rows) {
      const o = m.get(r.category) ?? {};
      o[r.severity] = (o[r.severity] ?? 0) + 1;
      m.set(r.category, o);
    }
    const rank = (o: Record<string, number>) => (o.high ?? 0) * 1e6 + (o.medium ?? 0) * 1e3 + (o.low ?? 0);
    return [...m.entries()].sort((a, b) => rank(b[1]) - rank(a[1])).slice(0, 12);
  }, [rows]);
  return (
    <Panel title="What kind of problems" note="Findings per category, split by severity. Select a category to list only those.">
      <HBars
        labelWidth={200}
        legend={SEV_PARTS.map((p) => ({ name: p.label, color: p.color }))}
        data={data.map(([c, o]) => {
          const n = (o.high ?? 0) + (o.medium ?? 0) + (o.low ?? 0) + (o.info ?? 0);
          return {
            key: c,
            label: c,
            parts: SEV_PARTS.map((p) => ({ value: o[p.k] ?? 0, color: p.color, name: p.label + ': ' + (o[p.k] ?? 0) })),
            display: fmtNum(n) + (o.high ? ', ' + fmtNum(o.high) + ' high' : ''),
            selected: c === active,
            onClick: () => onPick(c),
          };
        })}
      />
    </Panel>
  );
}
