// Formatting helpers. All timestamps are epoch ms and rendered in UTC
// (cluster logs are naive timestamps treated as UTC by the pipeline).

const DASH = '–';

export function fmtBytes(n: number | null | undefined, digits = 1): string {
  if (n === null || n === undefined || Number.isNaN(n)) return DASH;
  if (n === 0) return '0 B';
  // binary units, labelled as such (Spark's own UI does the same)
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  const neg = n < 0;
  let v = Math.abs(n);
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  const s = i === 0 ? String(Math.round(v)) : v.toFixed(v >= 100 ? 0 : digits);
  return `${neg ? '-' : ''}${s} ${units[i]}`;
}

export function fmtDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || Number.isNaN(ms)) return DASH;
  const neg = ms < 0;
  let v = Math.abs(ms);
  let out: string;
  // move up a unit when rounding would print the unit's own limit (999.6 ms, 59.96 s, 59m 59.6s, 23h 59.6m)
  if (v < 1000 && Math.round(v) >= 1000) v = 1000;
  if (v >= 1000 && v < 60_000 && Number((v / 1000).toFixed(v < 10_000 ? 2 : 1)) >= 60) v = 60_000;
  if (v >= 60_000 && v < 3_600_000 && Math.round(v / 1000) >= 3600) v = 3_600_000;
  if (v >= 3_600_000 && v < 86_400_000 && Math.round(v / 60_000) >= 1440) v = 86_400_000;
  if (v < 1000) out = `${Math.round(v)} ms`;
  else if (v < 60_000) out = `${(v / 1000).toFixed(v < 10_000 ? 2 : 1)} s`;
  else if (v < 3_600_000) {
    // round to the smallest unit shown first, so 5m 59.6s reads 6m, not 5m 60s
    const secs = Math.round(v / 1000);
    const m = Math.floor(secs / 60);
    const s = secs % 60;
    out = s ? `${m}m ${s}s` : `${m}m`;
  } else if (v < 86_400_000) {
    const mins = Math.round(v / 60_000);
    const h = Math.floor(mins / 60);
    const m = mins % 60;
    out = m ? `${h}h ${m}m` : `${h}h`;
  } else {
    const hrs = Math.round(v / 3_600_000);
    const d = Math.floor(hrs / 24);
    const h = hrs % 24;
    out = h ? `${d}d ${h}h` : `${d}d`;
  }
  return neg ? `-${out}` : out;
}

const pad = (n: number, w = 2) => String(n).padStart(w, '0');

/** 2024-10-06 17:48:10 (UTC) */
export function fmtTs(ms: number | null | undefined, withMs = false): string {
  if (ms === null || ms === undefined || Number.isNaN(ms)) return DASH;
  const d = new Date(ms);
  const base = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(
    d.getUTCMinutes(),
  )}:${pad(d.getUTCSeconds())}`;
  return withMs ? `${base}.${pad(d.getUTCMilliseconds(), 3)}` : base;
}

/** 17:48:10 (UTC) */
export function fmtTime(ms: number | null | undefined, withMs = false): string {
  if (ms === null || ms === undefined || Number.isNaN(ms)) return DASH;
  const d = new Date(ms);
  const base = `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
  return withMs ? `${base}.${pad(d.getUTCMilliseconds(), 3)}` : base;
}

export function fmtDate(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return DASH;
  const d = new Date(ms);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
}

export function fmtNum(n: number | null | undefined, digits = 0): string {
  if (n === null || n === undefined || Number.isNaN(n)) return DASH;
  return n.toLocaleString('en-US', { maximumFractionDigits: digits, minimumFractionDigits: 0 });
}

export function fmtPct(share: number | null | undefined, digits = 1): string {
  if (share === null || share === undefined || Number.isNaN(share)) return DASH;
  return `${(share * 100).toFixed(digits)}%`;
}

export function fmtSkew(n: number | null | undefined): string {
  if (n === null || n === undefined || Number.isNaN(n)) return DASH;
  return `${n.toFixed(1)}×`;
}

/** Relative offset from a reference time, like "+3m 12s". */
export function fmtOffset(ms: number | null | undefined, ref: number | null | undefined): string {
  if (ms === null || ms === undefined || ref === null || ref === undefined) return '';
  const d = ms - ref;
  return `${d < 0 ? '−' : '+'}${fmtDuration(Math.abs(d))}`;
}

/** ISO string or epoch → epoch ms */
export function toMs(v: string | number | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  if (typeof v === 'number') return v;
  const t = Date.parse(v);
  return Number.isNaN(t) ? null : t;
}

/** Parse a "YYYY-MM-DDTHH:mm" (datetime-local input, treated as UTC) to epoch ms. */
export function parseUtcInput(s: string): number | null {
  if (!s) return null;
  const t = Date.parse(s.length === 16 ? `${s}:00Z` : s.endsWith('Z') ? s : `${s}Z`);
  return Number.isNaN(t) ? null : t;
}

export function toUtcInput(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return '';
  const d = new Date(ms);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(
    d.getUTCMinutes(),
  )}`;
}

export function truncate(s: string | null | undefined, n: number): string {
  if (!s) return '';
  return s.length > n ? `${s.slice(0, n - 1)}…` : s;
}

export function asList(v: string[] | string | null | undefined): string[] {
  if (!v) return [];
  if (Array.isArray(v)) return v;
  return v.split('\n').filter(Boolean);
}

/** "Nice" tick values for a linear time axis in ms. */
export function timeTicks(t0: number, t1: number, approx = 8): number[] {
  const span = Math.max(1, t1 - t0);
  const steps = [
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10_000, 15_000, 30_000, 60_000, 120_000, 300_000, 600_000,
    900_000, 1_800_000, 3_600_000, 7_200_000, 10_800_000, 21_600_000, 43_200_000, 86_400_000,
  ];
  const raw = span / approx;
  const step = steps.find((s) => s >= raw) ?? Math.ceil(raw / 86_400_000) * 86_400_000;
  const first = Math.ceil(t0 / step) * step;
  const out: number[] = [];
  for (let t = first; t <= t1; t += step) out.push(t);
  return out;
}

export function tickLabel(t: number, span: number): string {
  if (span < 10_000) return fmtTime(t, true).slice(3);
  if (span > 2 * 86_400_000) return `${fmtDate(t).slice(5)} ${fmtTime(t).slice(0, 5)}`;
  return fmtTime(t);
}

/** Row counts, short: 950, 12.4K, 3.2M, 1.1B. */
export function fmtRows(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return '–';
  const a = Math.abs(n);
  if (a < 1000) return String(Math.round(n));
  const [d, u] = a >= 1e9 ? [1e9, 'B'] : a >= 1e6 ? [1e6, 'M'] : [1e3, 'K'];
  const v = n / d;
  return `${v >= 100 ? v.toFixed(0) : v.toFixed(1)}${u}`;
}

/** One unit for a whole row or column of a table, picked from its typical (median non-zero) value, so the unit is
 * written once in the heading and the cells are bare numbers: "Duration (s)" then 0.02, 1.75, 606. */
export type Unit = { u: string; f: (v: number | null | undefined) => string };
const T_UNITS: [string, number][] = [['ms', 1], ['s', 1000], ['min', 60_000], ['h', 3_600_000]];
const B_UNITS: [string, number][] = [['B', 1], ['KiB', 1024], ['MiB', 1024 ** 2], ['GiB', 1024 ** 3], ['TiB', 1024 ** 4]];
export function bare(x: number, whole = false): string {
  if (x === 0) return '0';
  const a = Math.abs(x);
  if (whole || a >= 100) return Math.round(x).toLocaleString('en-US');
  if (a < 0.01) return x < 0 ? '>-0.01' : '<0.01';
  return x.toFixed(a < 10 ? 2 : 1).replace(/\.?0+$/, '');
}
export function unitFor(kind: 'ms' | 'b', vals: (number | null | undefined)[], perSecond = false): Unit {
  const nz = vals.filter((v): v is number => v !== null && v !== undefined && Number.isFinite(v) && v !== 0).map(Math.abs).sort((a, b) => a - b);
  const ref = nz.length ? nz[Math.floor(nz.length / 2)] : 0;
  const table = kind === 'ms' ? T_UNITS : B_UNITS;
  // the largest unit the typical value reaches; seconds already from 1 s, minutes only from 2 min
  let [u, d] = table[0];
  for (const [uu, dd] of table) if (ref >= dd * (kind === 'ms' && uu !== 's' ? 2 : 1)) [u, d] = [uu, dd];
  const whole = d === 1;
  return {
    u: perSecond ? `${u}/s` : u,
    f: (v) => (v === null || v === undefined || Number.isNaN(v) ? DASH : bare(v / d, whole)),
  };
}
