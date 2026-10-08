import { useMemo, useState } from 'react';
import type { TaskSummary } from '../api';
import { fmtDuration, fmtNum } from '../format';
import { useWidth } from '../hooks';

/** Horizontal box plot + jittered strip of task durations (ms). */
export function BoxStrip({ values, summary, height = 160 }: { values: number[]; summary: TaskSummary | null; height?: number }) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [log, setLog] = useState(false);
  const [hover, setHover] = useState<{ x: number; v: number } | null>(null);
  const stats = useMemo(() => {
    const v = [...values].sort((a, b) => a - b);
    const q = (p: number) => (v.length ? v[Math.min(v.length - 1, Math.max(0, Math.ceil(p * v.length) - 1))] : null);
    return {
      min: summary?.min ?? v[0] ?? null,
      p25: summary?.p25 ?? q(0.25),
      p50: summary?.p50 ?? q(0.5),
      p75: summary?.p75 ?? q(0.75),
      max: summary?.max ?? v[v.length - 1] ?? null,
    };
  }, [values, summary]);
  // deterministic jitter
  const jitter = useMemo(() => values.map((_, i) => ((Math.sin(i * 12.9898) * 43758.5453) % 1 + 1) % 1), [values]);

  const padL = 12;
  const padR = 16;
  const w = Math.max(200, width);
  const plotW = w - padL - padR;
  const lo = Math.max(log ? 1 : 0, log ? Math.max(1, stats.min ?? 1) : 0);
  const hi = Math.max(lo + 1, stats.max ?? 1);
  const x = (v: number) => {
    if (log) {
      const a = Math.log10(Math.max(1, lo));
      const b = Math.log10(hi);
      return padL + ((Math.log10(Math.max(1, v)) - a) / Math.max(1e-9, b - a)) * plotW;
    }
    return padL + ((v - lo) / (hi - lo)) * plotW;
  };
  const ticks = useMemo(() => {
    if (log) {
      const out: number[] = [];
      for (let p = Math.floor(Math.log10(Math.max(1, lo))); p <= Math.ceil(Math.log10(hi)); p++) {
        const t = 10 ** p;
        if (t >= lo && t <= hi) out.push(t);
      }
      return out;
    }
    const n = Math.max(2, Math.floor(plotW / 110));
    return Array.from({ length: n + 1 }, (_, i) => lo + ((hi - lo) * i) / n);
  }, [log, lo, hi, plotW]);

  const boxY = 22;
  const boxH = 30;
  const stripY = 72;
  const stripH = height - stripY - 24;
  const axisY = height - 18;

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
        <span className="small ink2">
          {fmtNum(values.length)} task durations. Box = middle 50%, line = median, whiskers = min and max.
        </span>
        <div className="seg">
          <button className={!log ? 'on' : ''} onClick={() => setLog(false)}>
            Linear
          </button>
          <button className={log ? 'on' : ''} onClick={() => setLog(true)}>
            Log scale
          </button>
        </div>
      </div>
      {width > 0 && stats.min !== null && (
        <svg className="svg-chart" width={w} height={height} role="img" aria-label="Task duration distribution"
          onMouseLeave={() => setHover(null)}
          onMouseMove={(e) => {
            const r = (e.currentTarget as SVGSVGElement).getBoundingClientRect();
            const mx = e.clientX - r.left;
            if (mx < padL || mx > padL + plotW) return setHover(null);
            const frac = (mx - padL) / plotW;
            const v = log ? 10 ** (Math.log10(Math.max(1, lo)) + frac * (Math.log10(hi) - Math.log10(Math.max(1, lo)))) : lo + frac * (hi - lo);
            setHover({ x: mx, v });
          }}
        >
          {ticks.map((t, i) => (
            <g key={i}>
              <line className="grid" x1={x(t)} x2={x(t)} y1={10} y2={axisY - 4} />
              <text x={x(t)} y={axisY + 10} textAnchor="middle">
                {fmtDuration(t)}
              </text>
            </g>
          ))}
          {/* whiskers */}
          <line x1={x(stats.min!)} x2={x(stats.p25 ?? stats.min!)} y1={boxY + boxH / 2} y2={boxY + boxH / 2} stroke="var(--ink-2)" strokeWidth={1} />
          <line x1={x(stats.p75 ?? stats.max!)} x2={x(stats.max!)} y1={boxY + boxH / 2} y2={boxY + boxH / 2} stroke="var(--ink-2)" strokeWidth={1} />
          <line x1={x(stats.min!)} x2={x(stats.min!)} y1={boxY + 8} y2={boxY + boxH - 8} stroke="var(--ink-2)" />
          <line x1={x(stats.max!)} x2={x(stats.max!)} y1={boxY + 8} y2={boxY + boxH - 8} stroke="var(--ink-2)" />
          {/* box */}
          <rect
            x={x(stats.p25 ?? stats.min!)}
            y={boxY}
            width={Math.max(2, x(stats.p75 ?? stats.max!) - x(stats.p25 ?? stats.min!))}
            height={boxH}
            fill="var(--series-1-soft)"
            stroke="var(--series-1)"
            rx={3}
          />
          <line x1={x(stats.p50 ?? 0)} x2={x(stats.p50 ?? 0)} y1={boxY} y2={boxY + boxH} stroke="var(--ink)" strokeWidth={2} />
          {(() => {
            const mx = x(stats.p50 ?? 0);
            const anchor = mx < padL + 60 ? 'start' : mx > padL + plotW - 60 ? 'end' : 'middle';
            return (
              <text x={mx} y={boxY + boxH + 13} textAnchor={anchor} style={{ fill: 'var(--ink-2)' }}>
                median {fmtDuration(stats.p50)}
              </text>
            );
          })()}
          <text x={Math.min(x(stats.max!), padL + plotW)} y={boxY - 6} textAnchor={x(stats.max!) < padL + 60 ? 'start' : 'end'} style={{ fill: 'var(--ink-2)' }}>
            max {fmtDuration(stats.max)}
          </text>
          {/* strip */}
          {values.map((v, i) => (
            <circle key={i} cx={x(v)} cy={stripY + 4 + jitter[i] * Math.max(4, stripH - 8)} r={values.length > 1500 ? 1.6 : 2.6} fill="var(--series-1)" fillOpacity={values.length > 500 ? 0.35 : 0.6} />
          ))}
          <line className="baseline" x1={padL} x2={padL + plotW} y1={axisY - 4} y2={axisY - 4} />
          {hover && (
            <g pointerEvents="none">
              <line x1={hover.x} x2={hover.x} y1={10} y2={axisY - 4} stroke="var(--ink-2)" strokeWidth={1} />
              <text x={Math.min(hover.x + 6, padL + plotW - 60)} y={stripY - 2} style={{ fill: 'var(--ink)' }}>
                {fmtDuration(hover.v)}
              </text>
            </g>
          )}
        </svg>
      )}
    </div>
  );
}

export interface HBarDatum {
  key: string;
  label: string;
  sub?: string;
  selected?: boolean;
  parts: { value: number; color: string; name: string }[];
  display: string;
  flagged?: boolean;
  title?: string;
  onClick?: () => void;
}

/** Simple horizontal bar list (HTML) with optional stacked parts and a shared max. */
export function HBars({ data, max, threshold, legend, labelWidth }: { data: HBarDatum[]; max?: number; threshold?: { value: number; label: string }; legend?: { name: string; color: string }[]; labelWidth?: number }) {
  const m = max ?? Math.max(1, ...data.map((d) => d.parts.reduce((a, b) => a + b.value, 0)), threshold?.value ?? 0);
  return (
    <div>
      {legend && legend.length > 1 && (
        <div className="legend" style={{ marginBottom: 8 }}>
          {legend.map((l) => (
            <span key={l.name} className="item">
              <span className="sw" style={{ background: l.color }} />
              {l.name}
            </span>
          ))}
          {threshold && (
            <span className="item">
              <span className="mk" style={{ borderColor: 'var(--sev-high)' }} />
              {threshold.label}
            </span>
          )}
        </div>
      )}
      <div className="hbar-list">
        {data.map((d) => (
          <div
            key={d.key}
            className={`hbar ${d.flagged ? 'flagged' : ''} ${d.selected ? 'selected' : ''} ${d.sub ? 'has-sub' : ''}`}
            title={d.title}
            onClick={d.onClick}
            style={{ ...(d.onClick ? { cursor: 'pointer' } : {}), ...(labelWidth ? { gridTemplateColumns: `${labelWidth}px 1fr 110px` } : {}) }}
            role={d.onClick ? 'button' : undefined}
            tabIndex={d.onClick ? 0 : undefined}
            onKeyDown={d.onClick ? (e) => (e.key === 'Enter' || e.key === ' ') && (e.preventDefault(), d.onClick!()) : undefined}
          >
            <span className="name">
              {d.label}
              {d.sub && <span className="sub">{d.sub}</span>}
            </span>
            <span className="track">
              {d.parts.map((p, i) =>
                p.value > 0 ? (
                  <span key={i} style={{ width: `${(p.value / m) * 100}%`, background: p.color, borderRadius: i === d.parts.length - 1 ? '0 3px 3px 0' : 0 }} title={`${p.name}`} />
                ) : null,
              )}
              {threshold && (
                <span
                  aria-hidden
                  style={{ position: 'absolute', left: `${(threshold.value / m) * 100}%`, top: -2, bottom: -2, width: 0, borderLeft: '2px solid var(--sev-high)', background: 'none' }}
                />
              )}
            </span>
            <span className="val">{d.display}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
