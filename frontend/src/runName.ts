// How a run is named in the picker, the cluster view and the banners: what it ran (notebook or main class) and what
// it worked on (the table a per-table task loaded), then the Databricks ids, which all look alike.
import type { RunRow } from './api';
import { fmtDuration, fmtTime } from './format';

/** "load_table · sales_daily" or the run's own label when nothing better is known. */
export function runName(r: RunRow): string {
  const what = [r.program, r.subject].filter(Boolean).join(' · ');
  return what || r.label;
}

/** The short id part: "task run …1234", or the Spark app. */
export function runId(r: RunRow): string {
  const tail = (s: string | null) => (s ? `…${s.slice(-4)}` : '');
  if (r.task_run_id) return `task run ${tail(r.task_run_id)}`;
  // a job group's run id is the task's own run id (a job run's tasks each get one)
  if (r.databricks_run_id) return `task run ${tail(r.databricks_run_id)}`;
  return r.label;
}

/** "× usual" against the median of the same program's runs, when there are at least 3 of them. A ratio needs an
 * absolute floor: a 43 s run against a usual 541 ms is 79×, and means nothing, so a run under a minute slower than
 * usual has no ratio. */
export function usualX(r: { same_job_runs?: number | null; vs_typical?: number | null; duration_ms?: number | null; typical_duration_ms?: number | null; usual_from?: string | null }): number | null {
  // the usual comes from the same task on the same table earlier (2 or more runs), else from 3 or more runs of this batch
  if ((r.usual_from !== 'history' && (r.same_job_runs ?? 0) < 3) || r.vs_typical == null) return null;
  if (r.vs_typical > 1 && r.duration_ms != null && r.typical_duration_ms != null && r.duration_ms - r.typical_duration_ms < 60_000) return null;
  return r.vs_typical;
}

/** One line for a picker option: "04:02 · 30m 42s · failed · load_table · sales_daily · task run …1234". */
export function runOption(r: RunRow): string {
  const bits = [fmtTime(r.start_time).slice(0, 5), fmtDuration(r.duration_ms)];
  if (r.status !== 'succeeded') bits.push(r.status === 'failed' ? '✕ failed' : r.status);
  const ux = usualX(r);
  if (ux !== null && ux >= 2) bits.push(`${ux.toFixed(1)}× usual`);
  bits.push(runName(r));
  if (r.program || r.subject) bits.push(runId(r));
  return bits.join(' · ');
}

/** Group key for runs of the same code: the program, else the kind of run. */
export function runGroup(r: RunRow): string {
  return r.program ?? (r.kind === 'app' ? 'Spark apps' : r.kind === 'notebook' ? 'Notebooks' : r.kind === 'connect_session' ? 'Spark Connect sessions' : 'Other runs');
}
