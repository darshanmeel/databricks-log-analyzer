// The cause bar: where a run's (or all runs') time went by cause, not by query. Waiting for a free core and no Spark
// work come from the clock; the running time splits in the shares of the task time (CPU, storage and other, GC,
// waiting for shuffle data, failed attempts). Each part is a button that ranks the list below by it.
import type { Causes } from '../api';
import { fmtBytes, fmtDuration, fmtPct } from '../format';

export type CauseKey = 'wait' | 'cpu' | 'gc' | 'fetch' | 'write' | 'other' | 'failed' | 'out';
export interface CausePart { key: CauseKey; label: string; ms: number; cls: string; tip: string; rank: boolean }

const GB = 1024 ** 3;

/** The parts of the bar. Without CPU times (old logs) running stays one part. */
export function causeParts(waiting: number, running: number, outside: number, c: Causes | null | undefined): CausePart[] {
  const parts: CausePart[] = [{ key: 'wait', label: 'waiting for a core', ms: waiting, cls: 'ro-seg-wait', tip: 'A stage was ready but no core was free: other work held them.', rank: true }];
  if (c && c.has_cpu && c.task_ms > 0) {
    const share = (v: number) => (running * v) / c.task_ms;
    parts.push(
      { key: 'cpu', label: 'CPU', ms: share(c.cpu_ms), cls: 'ro-seg-cpu', tip: 'Executors computing: joins, sorts, aggregations, encoding.', rank: true },
      { key: 'other', label: 'storage and other', ms: share(c.other_ms), cls: 'ro-seg-io', tip: 'Task time off the CPU and not in GC or shuffle: reading and writing storage, network, Python workers, spilling.', rank: true },
      { key: 'gc', label: 'GC', ms: share(c.gc_ms), cls: 'ro-seg-gc', tip: 'JVM garbage collection inside tasks.', rank: true },
      { key: 'write', label: 'writing shuffle', ms: share(c.shuffle_write_ms ?? 0), cls: 'ro-seg-write', tip: 'Tasks writing their shuffle output to local disk for the next stage.', rank: true },
      { key: 'fetch', label: 'waiting for shuffle data', ms: share(c.fetch_wait_ms), cls: 'ro-seg-fetch', tip: 'Tasks waiting for shuffle blocks from other executors.', rank: true },
      { key: 'failed', label: 'failed attempts', ms: share(c.failed_ms), cls: 'ro-seg-fail', tip: 'Work of task attempts that failed or were cancelled.', rank: true },
    );
  } else {
    parts.push({ key: 'cpu', label: 'running tasks', ms: running, cls: 'ro-seg-run', tip: 'No CPU times in these logs, so running is not split.', rank: false });
  }
  parts.push({ key: 'out', label: 'no Spark work', ms: outside, cls: 'ro-seg-out', tip: 'No stage submitted: driver code, Python, JDBC calls, sleeps, or waiting outside Spark.', rank: false });
  return parts;
}

/** An amount of one cause in a Causes record (task time), with the waiting time given separately. */
export function causeOf(c: Causes | null | undefined, waiting: number, k: CauseKey): number {
  if (k === 'wait') return waiting;
  if (!c) return 0;
  return k === 'cpu' ? c.cpu_ms : k === 'gc' ? c.gc_ms : k === 'fetch' ? c.fetch_wait_ms : k === 'write' ? c.shuffle_write_ms ?? 0 : k === 'other' ? c.other_ms : k === 'failed' ? c.failed_ms : 0;
}

/** "57% waiting for a core · 24% CPU · spilled 108 GB to disk": the two biggest causes. */
export function CauseHeadline({ parts, total, spill }: { parts: CausePart[]; total: number; spill?: number | null }) {
  const big = [...parts].filter((p) => p.ms > 0).sort((x, y) => y.ms - x.ms).slice(0, 2);
  return (
    <>
      By cause: {big.map((p, i) => <span key={p.key}>{i ? ' · ' : ''}<b>{fmtPct(p.ms / Math.max(1, total), 0)} {p.label}</b></span>)}
      {(spill ?? 0) >= GB / 4 ? <> · spilled {fmtBytes(spill!)} to disk</> : null}.
    </>
  );
}

/** The key under (or beside) the bar; with `pct` each part also says its share, so no separate headline is needed. */
export function CauseLegend({ parts, total, pct = false }: { parts: CausePart[]; total: number; pct?: boolean }) {
  return (
    <>
      {visible(parts, total).map((p) => (
        <span key={p.key} title={`${fmtDuration(p.ms)}. ${p.tip}`}><i className={p.cls} /> {pct ? <b>{fmtPct(p.ms / Math.max(1, total), 0)}</b> : null} {p.label}</span>
      ))}
    </>
  );
}

/** Slivers under 0.2% of the time are noise. */
const visible = (parts: CausePart[], total: number) => parts.filter((p) => p.ms > 0 && p.ms >= total * 0.002);

export function CauseBar({ parts, total, by, onBy }: { parts: CausePart[]; total: number; by: CauseKey | null; onBy?: (k: CauseKey | null) => void }) {
  return (
    <div className="ro-split ro-causebar" role="group" aria-label="Where the time went, by cause">
      {visible(parts, total).map((p) => (
        <button key={p.key} className={`${p.cls} ${by === p.key ? 'on' : ''}`} style={{ width: `${(p.ms / Math.max(1, total)) * 100}%` }}
          title={`${p.label}: ${fmtDuration(p.ms)} (${fmtPct(p.ms / Math.max(1, total), 0)}). ${p.tip}`}
          aria-pressed={onBy ? by === p.key : undefined} disabled={!p.rank || !onBy} onClick={() => onBy?.(by === p.key ? null : p.key)}>
          {p.ms / total >= 0.12 ? <em>{p.label} {fmtDuration(p.ms)} · {fmtPct(p.ms / total, 0)}</em> : ''}
        </button>
      ))}
    </div>
  );
}
