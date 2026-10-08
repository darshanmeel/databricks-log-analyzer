// The first thing on an overview when something failed: the top failure incident from Findings (its root problem, the
// first line of its fix, links to where it happened). Nothing is shown when nothing failed: slowness is the hero's.
import { useMemo } from 'react';
import { Link } from 'react-router-dom';
import { api, type FindingRow, type IncidentRow } from '../api';
import { useAsync } from '../hooks';
import { fmtTime, truncate } from '../format';
import { rowLinks, to } from '../links';
import { groupIncidents, isFailure, joinIncidents, rootOf, type Incident, type Problem } from '../incidents';
import { EntityChips } from './ui';
import { inRun } from './TopFinder';

export interface TopFailure { inc: Incident; root: Problem; count: number }

/** The worst failure incident of the cluster (`cluster`) or of the run in scope. */
export function useTopFailure(cid: string, cluster: boolean, run?: string): { data: TopFailure | null; loading: boolean } {
  const st = useAsync(async (s) => {
    const p = cluster ? { run: '' } : {};
    const [f, inc] = await Promise.all([
      api.datasetOpt<FindingRow>(cid, 'findings', { limit: 5000, sort: 'finding_id', ...p }, s),
      api.datasetOpt<IncidentRow>(cid, 'incidents', { limit: 5000, ...p }, s),
    ]);
    return joinIncidents(f?.rows ?? [], inc?.rows ?? []);
  }, [cid, cluster, run]);
  const data = useMemo(() => {
    const fails = groupIncidents(st.data ?? []).filter(isFailure);
    return fails.length ? { inc: fails[0], root: rootOf(fails[0]), count: fails.length } : null;
  }, [st.data]);
  return { data, loading: st.loading };
}

/** The first sentence of a fix. */
const firstSentence = (s: string) => truncate(s.split(/(?<=\.)\s+/)[0], 220);

export function WhatBroke({ cid, top, run }: { cid: string; top: TopFailure | null; run?: string }) {
  if (!top) return null;
  const { inc, root, count } = top;
  const f = root.lead;
  const i = f.inc!;
  const chained = inc.problems.length > 1 && i.role === 'root';
  const links = rowLinks(cid, {
    ...f, spark_context_id: f.spark_context_id ?? i.spark_context_id, stage_id: f.stage_id ?? i.stage_id,
    stage_attempt: f.stage_id !== null ? f.stage_attempt : i.stage_attempt, spark_job_id: f.spark_job_id ?? i.spark_job_id,
    sql_execution_id: f.sql_execution_id ?? i.sql_execution_id, executor_id: f.executor_id ?? i.executor_id, finding_id: null,
  }).filter((l) => l.type !== 'log');
  // the Findings page is one run's: open the run the failure happened in
  const href = inRun(to.findings(cid, f.finding_id), run ?? (f as FindingRow & { run_key?: string | null }).run_key);
  return (
    <section className="what-broke">
      <div className="wb-head">
        <span className="ro-eyebrow bad">What went wrong</span>
        <b>{inc.head.incident_title}</b>
        <span className="muted small mono">{fmtTime(inc.head.incident_start)}</span>
      </div>
      <p className="wb-cause"><span className="wb-lbl">{chained ? 'Most likely root cause' : 'Found'}</span> <b>{i.kind}</b>{f.entity ? ` · ${f.entity}` : ''}{f.evidence ? <span className="muted"> · {truncate(f.evidence.replace(/\s*e\.g\. Traceback \(most recent call last\):?\s*$/, ''), 200)}</span> : null}</p>
      {f.fix && <p className="wb-fix"><span className="wb-lbl">Fix</span> {firstSentence(f.fix)}</p>}
      <div className="wb-foot">
        <EntityChips links={links} />
        <Link to={href}>The incident{count > 1 ? ` (1 of ${count} failures)` : ''} →</Link>
      </div>
    </section>
  );
}
