// One verdict for the cluster and for a run, used by Home, the cluster header and the run header, so the first glance
// never gives two answers. Failed: a Spark job or query failed. Succeeded with problems: nothing failed in the end,
// but executors ran out of memory, were lost or killed, or stages and tasks failed and were retried. Succeeded: none
// of that. (Almost every finding is rated high, so findings do not decide it.)
import type { Counts, RunRow } from './api';
import { fmtNum } from './format';

export type VerdictKind = 'failed' | 'problems' | 'ok';
export interface Verdict { kind: VerdictKind; label: string; reason: string }

const n = (v: number | null | undefined) => v ?? 0;
const plural = (k: number, one: string, many: string) => `${fmtNum(k)} ${k === 1 ? one : many}`;

/** What went wrong without failing: executors gone badly, stages and tasks that failed and were retried. */
function problems(oom: number, lostOrKilled: number, stages: number, tasks: number): string[] {
  return [
    oom ? `${fmtNum(oom)} out of memory` : '',
    lostOrKilled ? plural(lostOrKilled, 'executor lost', 'executors lost') : '',
    stages ? plural(stages, 'stage failed', 'stages failed') : '',
    tasks ? plural(tasks, 'task retry', 'task retries') : '',
  ].filter(Boolean);
}

/** The cluster: from the analysis status and counts, and its runs when it has them. */
export function clusterVerdict(status: string | null | undefined, counts: Partial<Counts> | null | undefined, runs: RunRow[] = []): Verdict {
  const c = (counts ?? {}) as Partial<Counts> & { executors_oom?: number; executors_killed?: number };
  const fj = n(c.failed_jobs), fq = n(c.failed_queries);
  const failedRuns = runs.filter((r) => r.status === 'failed').length;
  if ((status ?? '').toLowerCase() === 'failed' || failedRuns) {
    const what = [failedRuns ? plural(failedRuns, 'run', 'runs') : '', fq ? plural(fq, 'query', 'queries') : '', fj ? plural(fj, 'Spark job', 'Spark jobs') : ''].filter(Boolean);
    const outside = runs.length && !failedRuns && (fq || fj) ? ' (outside any failed run)' : '';
    return { kind: 'failed', label: 'Failed', reason: what.length ? `${what.join(', ')} failed${outside}` : 'no failed job or query found in the logs' };
  }
  const p = problems(n(c.executors_oom), n(c.executors_lost) + n(c.executors_killed), n(c.failed_stages), n(c.failed_tasks));
  if (p.length) return { kind: 'problems', label: 'Succeeded with problems', reason: p.join(', ') };
  return { kind: 'ok', label: runs.length ? `All ${fmtNum(runs.length)} runs succeeded` : 'Succeeded', reason: '' };
}

/** One run: its status, its failed stages and tasks, and the executors it lost (from its end, when loaded). */
export function runVerdict(r: RunRow, gone?: { removal_category: string | null }[] | null): Verdict {
  if (r.status === 'failed') return { kind: 'failed', label: 'Failed', reason: '' };
  const cat = (k: string) => (gone ?? []).filter((x) => x.removal_category === k).length;
  const p = problems(cat('oom'), cat('lost') + cat('killed'), n(r.failed_stages), n(r.failed_tasks));
  if (p.length) return { kind: 'problems', label: 'Succeeded with problems', reason: p.join(', ') };
  return { kind: 'ok', label: 'Succeeded in Spark', reason: '' };
}
