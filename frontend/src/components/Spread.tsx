// How evenly work was spread over a stage's tasks: the skew ratio (biggest ÷ median), a small box plot on a 0..max
// scale (thin line from the smallest to the biggest task, a thick box from p10 to p90 when known, the tick is the
// median: a tick far left of the line's end means a few tasks did much more than the rest), and the numbers under it.
import { fmtBytes, fmtDuration, fmtRows } from '../format';

export type SpreadUnit = 'ms' | 'bytes' | 'rows';
const fmt = (u: SpreadUnit, v: number | null | undefined) =>
  v === null || v === undefined ? '–' : u === 'ms' ? fmtDuration(v) : u === 'bytes' ? fmtBytes(v) : fmtRows(v);

export function Spread({
  lo, p10, mid, p90, hi, avg, ratio, unit, warnAt = 3, badAt, what = 'task', full = false,
}: {
  lo: number | null; p10?: number | null; mid: number | null; p90?: number | null; hi: number | null; avg?: number | null;
  ratio: number | null; unit: SpreadUnit; warnAt?: number; badAt?: boolean; what?: string;
  /** all five numbers under the bar (detail panels); tables show median · p90 · biggest */
  full?: boolean;
}) {
  if (hi === null || (hi === 0 && unit !== 'ms')) return <span className="muted">–</span>;
  const W = 64;
  const pos = (v: number | null | undefined) => (hi > 0 && v !== null && v !== undefined ? Math.max(0, Math.min(W, (v / hi) * W)) : W);
  const cls = badAt ? 'bad' : ratio !== null && ratio >= warnAt ? 'st-warn' : '';
  const unitWord = unit === 'rows' ? ' rows' : '';
  const hasBox = p10 !== null && p10 !== undefined && p90 !== null && p90 !== undefined;
  const color = cls ? 'var(--st-warn)' : 'var(--series-1)';
  return (
    <div
      className="spread"
      title={
        `Per ${what}: smallest ${fmt(unit, lo)}${unitWord}` +
        (hasBox ? `, p10 ${fmt(unit, p10)}` : '') +
        `, median ${fmt(unit, mid)}` +
        (hasBox ? `, p90 ${fmt(unit, p90)}` : '') +
        `, biggest ${fmt(unit, hi)}${unitWord}` +
        (avg !== null && avg !== undefined ? `, average ${fmt(unit, avg)}${unitWord}` : '') +
        `. Skew = biggest ÷ median${ratio !== null ? ` = ${ratio.toFixed(1)}×` : ''}. ` +
        (hasBox ? 'The box runs from p10 to p90 (80% of the tasks), the tick is the median.' : 'The tick on the bar is the median.')
      }
    >
      <div className="spread-top">
        <svg width={W + 2} height={10} aria-hidden>
          <line x1={1} x2={W + 1} y1={5} y2={5} stroke="var(--line)" strokeWidth={2} />
          <line x1={1 + pos(lo)} x2={1 + Math.max(pos(lo) + 1, pos(hi))} y1={5} y2={5} stroke={color} strokeWidth={hasBox ? 1.5 : 4} strokeLinecap="round" />
          {hasBox && <rect x={1 + pos(p10)} y={2} width={Math.max(1.5, pos(p90) - pos(p10))} height={6} rx={1} fill={color} />}
          <line x1={1 + pos(mid)} x2={1 + pos(mid)} y1={0} y2={10} stroke="var(--text)" strokeWidth={1.5} />
        </svg>
        <b className={cls}>{ratio !== null ? `${ratio.toFixed(1)}×` : '–'}</b>
      </div>
      <div className="spread-nums">
        {full
          ? [lo, ...(hasBox ? [p10] : []), mid, ...(hasBox ? [p90] : []), hi].map((v) => fmt(unit, v)).join(' · ')
          : hasBox
            ? `${fmt(unit, mid)} · ${fmt(unit, p90)} · ${fmt(unit, hi)}`
            : `${fmt(unit, lo)} · ${fmt(unit, mid)} · ${fmt(unit, hi)}`}
        {unit === 'rows' ? <span className="muted"> rows</span> : null}
      </div>
      {full && (
        <div className="spread-nums muted">
          {hasBox ? 'min · p10 · median · p90 · max' : 'min · median · max'}
          {avg !== null && avg !== undefined ? ` · average ${fmt(unit, avg)}` : ''}
        </div>
      )}
    </div>
  );
}

type DataDistLike = {
  min_task_bytes_in?: number | null; p10_task_bytes_in?: number | null; p50_task_bytes_in?: number | null; p90_task_bytes_in?: number | null;
  max_task_bytes_in?: number | null; avg_task_bytes_in?: number | null;
  min_task_rows_in?: number | null; p10_task_rows_in?: number | null; p50_task_rows_in?: number | null; p90_task_rows_in?: number | null;
  max_task_rows_in?: number | null; avg_task_rows_in?: number | null;
};

/** Which unit to show data spread in: bytes when tasks read bytes, else rows. */
export function dataSpread(m: DataDistLike): {
  lo: number | null; p10: number | null; mid: number | null; p90: number | null; hi: number | null; avg: number | null; unit: SpreadUnit;
} {
  if (m.max_task_bytes_in)
    return {
      lo: m.min_task_bytes_in ?? null, p10: m.p10_task_bytes_in ?? null, mid: m.p50_task_bytes_in ?? null, p90: m.p90_task_bytes_in ?? null,
      hi: m.max_task_bytes_in, avg: m.avg_task_bytes_in ?? null, unit: 'bytes',
    };
  return {
    lo: m.min_task_rows_in ?? null, p10: m.p10_task_rows_in ?? null, mid: m.p50_task_rows_in ?? null, p90: m.p90_task_rows_in ?? null,
    hi: m.max_task_rows_in ?? null, avg: m.avg_task_rows_in ?? null, unit: 'rows',
  };
}
