// Plain-English helpers for the task_retries dataset (CONTRACT Revision 3 item 12).
import type { TaskRetryRow } from './api';
import { fmtDuration, truncate } from './format';

/** Fixed categorical order (identity, not status): colors never move when a filter hides a category. */
export const RETRY_CATS = ['executor_lost', 'oom', 'killed', 'fetch_failed', 'exception', 'other'] as const;

export const RETRY_META: Record<string, { label: string; color: string; why: string }> = {
  executor_lost: { label: 'Executor lost', color: 'var(--series-1)', why: 'the executor running it went away' },
  oom: { label: 'Out of memory', color: 'var(--series-2)', why: 'it ran out of memory' },
  killed: { label: 'Killed', color: 'var(--series-3)', why: 'it was killed' },
  fetch_failed: { label: 'Shuffle fetch failed', color: 'var(--series-4)', why: 'it could not fetch shuffle data from another executor' },
  exception: { label: 'Exception', color: 'var(--series-5)', why: 'the task code threw an exception' },
  other: { label: 'Other', color: 'var(--other)', why: 'of another failure' },
};

export const retryCat = (r: Pick<TaskRetryRow, 'first_failure_category'>) =>
  r.first_failure_category && RETRY_META[r.first_failure_category] ? r.first_failure_category : 'other';

const execName = (e: string | null | undefined) => (e === null || e === undefined || e === '' ? 'an unknown executor' : e === 'driver' ? 'the driver' : `executor ${e}`);

/**
 * "Task 7 failed first on executor 3: ExecutorLostFailure (executor killed: Command exited with code 9).
 *  Retried on executor 1 after 42 s and succeeded; 3m 10s of work lost."
 */
export function retrySentence(r: TaskRetryRow, withStage = false): string {
  if (r.explanation && withStage) return r.explanation;
  const who = `${withStage ? `Stage ${r.stage_id}${r.stage_attempt ? `.${r.stage_attempt}` : ''} task ${r.task_index}` : `Task ${r.task_index}`}`;
  const reason = r.first_failure_reason ?? RETRY_META[retryCat(r)].label;
  const detail = r.executor_removed_reason || r.first_failure_error;
  let s = `${who} failed first on ${execName(r.first_attempt_executor_id)}: ${reason}`;
  if (detail) s += ` (${truncate(detail.replace(/\s+/g, ' '), 160)})`;
  s += '.';
  const attempts = r.attempts ?? 0;
  if (r.final_status === 'succeeded') {
    const where = r.final_executor_id ? ` on ${execName(r.final_executor_id)}` : '';
    const after = r.retry_delay_ms !== null && r.retry_delay_ms !== undefined ? ` after ${fmtDuration(r.retry_delay_ms)}` : '';
    s += ` Retried${where}${after} and succeeded`;
    if (attempts > 2) s += ` on attempt ${attempts}`;
  } else if (r.final_status === 'failed') {
    s += ` Every retry failed${attempts ? ` (${attempts} attempts)` : ''}`;
  } else {
    s += ' The outcome of the retry is unknown';
  }
  if (r.wasted_ms) s += `; ${fmtDuration(r.wasted_ms)} of work lost`;
  return `${s}.`;
}

export interface RetryStats {
  total: number;
  succeeded: number;
  failed: number;
  wasted: number;
  stages: number;
  byCat: Record<string, number>;
}

export function retryStats(rows: TaskRetryRow[]): RetryStats {
  const byCat: Record<string, number> = {};
  const stages = new Set<string>();
  let succeeded = 0;
  let failed = 0;
  let wasted = 0;
  for (const r of rows) {
    const c = retryCat(r);
    byCat[c] = (byCat[c] ?? 0) + 1;
    stages.add(`${r.spark_context_id}|${r.stage_id}|${r.stage_attempt}`);
    if (r.final_status === 'succeeded') succeeded++;
    else if (r.final_status === 'failed') failed++;
    wasted += r.wasted_ms ?? 0;
  }
  return { total: rows.length, succeeded, failed, wasted, stages: stages.size, byCat };
}

/** One row per stage attempt with retries, counts per category, sorted by retry count. */
export function retriesByStage(rows: TaskRetryRow[]) {
  const m = new Map<string, { ctx: string; stage_id: number; stage_attempt: number; total: number; wasted: number; byCat: Record<string, number> }>();
  for (const r of rows) {
    const k = `${r.spark_context_id}|${r.stage_id}|${r.stage_attempt}`;
    const o = m.get(k) ?? { ctx: r.spark_context_id, stage_id: r.stage_id, stage_attempt: r.stage_attempt, total: 0, wasted: 0, byCat: {} };
    const c = retryCat(r);
    o.byCat[c] = (o.byCat[c] ?? 0) + 1;
    o.total++;
    o.wasted += r.wasted_ms ?? 0;
    m.set(k, o);
  }
  return [...m.values()].sort((a, b) => b.total - a.total || b.wasted - a.wasted);
}
