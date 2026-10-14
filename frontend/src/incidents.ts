// Findings grouped into incidents (root cause -> effects), shared by the Findings page and the overviews' "What went
// wrong" block.
import type { FindingRow, IncidentRow } from './api';
import { FAILURE_KINDS } from './problems';

export type Joined = FindingRow & { inc: IncidentRow | null };

export type Problem = { id: string; lead: Joined; same: Joined[]; ts: number | null };
export type Incident = { id: string; head: IncidentRow; problems: Problem[]; byFinding: Map<string, Problem> };

export function groupIncidents(rows: Joined[]): Incident[] {
  const m = new Map<string, Incident>();
  for (const r of rows) {
    if (!r.inc) continue;
    let inc = m.get(r.inc.incident_id);
    if (!inc) m.set(r.inc.incident_id, (inc = { id: r.inc.incident_id, head: r.inc, problems: [], byFinding: new Map() }));
    let p = inc.problems.find((x) => x.id === r.inc!.problem_id);
    if (!p) inc.problems.push((p = { id: r.inc.problem_id, lead: r, same: [], ts: r.ts }));
    else if (r.inc.role === 'same') p.same.push(r);
    else {
      p.same.push(p.lead); // a 'same' row came first and stood in as the lead
      p.lead = r;
    }
    if (r.ts !== null && (p.ts === null || r.ts < p.ts)) p.ts = r.ts;
  }
  for (const inc of m.values()) {
    for (const p of inc.problems) for (const f of [p.lead, ...p.same]) inc.byFinding.set(f.finding_id, p);
    inc.problems.sort((a, b) => (a.ts ?? Infinity) - (b.ts ?? Infinity) || a.id.localeCompare(b.id));
  }
  // what touched a run before what touched none (a tooling query that failed on its own); then the analyzer's rank:
  // failures first, then by severity and how much they cost, so the numbers read I1, I2, … down the page
  const noRun = (i: Incident) => (i.problems.some((p) => [p.lead, ...p.same].some((f) => (f as { run_key?: string | null }).run_key)) ? 0 : 1);
  return [...m.values()].sort((a, b) => noRun(a) - noRun(b) || a.head.incident_rank - b.head.incident_rank);
}


/** Join findings to their incident rows. */
export function joinIncidents(findings: FindingRow[], incidents: IncidentRow[]): Joined[] {
  const byId = new Map(incidents.map((r) => [r.finding_id, r]));
  return findings.map((r) => ({ ...r, inc: byId.get(r.finding_id) ?? null }));
}

export const isFailure = (i: Incident) => i.problems.some((p) => FAILURE_KINDS.has(p.lead.inc!.kind));
/** The incident's root problem: the one marked root, else its first. */
export const rootOf = (i: Incident) => i.problems.find((p) => p.lead.inc!.role === 'root') ?? i.problems[0];
