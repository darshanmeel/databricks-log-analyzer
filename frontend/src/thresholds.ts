// Mirrors [thresholds] in src/databricks_cluster_log_analyzer/rules.toml (the API does not expose rules,
// so the UI keeps a copy for highlighting breaches; findings themselves come from the backend).
export const TH = {
  skewRatio: 10,
  skewMinTaskMs: 60_000,
  spillBytes: 1024 ** 3,
  gcShare: 0.2,
  tinyTasksMin: 2000,
  tinyTasksP50Ms: 200,
  shuffleHeavyBytes: 1024 ** 3,
};

export const SIGNAL_ORDER = [
  'executor_oom',
  'driver_unresponsive',
  'gc_pressure',
  'disk_spill',
  'fetch_failure',
  'executor_lost',
  'broadcast_timeout',
  'disk_full',
  'storage_throttling',
  'schema_error',
  'python_error',
];

export const SERIES = [
  'var(--series-1)',
  'var(--series-2)',
  'var(--series-3)',
  'var(--series-4)',
  'var(--series-5)',
  'var(--series-6)',
  'var(--series-7)',
];

export function skewBreach(skew: number | null | undefined, maxTaskMs: number | null | undefined): boolean {
  return (skew ?? 0) >= TH.skewRatio && (maxTaskMs ?? 0) >= TH.skewMinTaskMs;
}
export function spillBreach(bytes: number | null | undefined): boolean {
  return (bytes ?? 0) >= TH.spillBytes;
}
export function gcBreach(share: number | null | undefined): boolean {
  return (share ?? 0) >= TH.gcShare;
}

/** Revision 19: what one task reads (input + shuffle read). Over 128 MiB is a warning, over 256 MiB critical: the task
 * holds too much at once (spill, GC, long tails); more partitions would split it. */
export const TASK_READ_WARN = 128 * 1024 ** 2;
export const TASK_READ_CRIT = 256 * 1024 ** 2;
export const taskReadLevel = (bytes: number | null | undefined): 'crit' | 'warn' | null =>
  bytes == null ? null : bytes > TASK_READ_CRIT ? 'crit' : bytes > TASK_READ_WARN ? 'warn' : null;
