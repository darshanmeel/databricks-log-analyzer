// Revision 15: problem cards shared by every drill-down level (query, job, stage): a short title, one big number,
// a small chart and the change to make.
import type { ReactNode } from 'react';
import { fmtPct } from '../format';

export type VCard = { tone: 'bad' | 'warn' | 'ok'; title: string; big: string; unit?: string; chart?: ReactNode; note?: string; icon?: string };

/** One sign per kind of problem, the same on cards and reason chips, so a page can be scanned before it is read. */
export const SIGN = {
  // plain marks only: pictures on every block were noise
  failed: '✕', slow: '', big: '', spill: '', gc: '', wait: '', shared: '', time: '', tiny: '', files: '', ok: '✓', tail: '',
} as const;
export const signFor = (title: string): string => {
  const t = title.toLowerCase();
  if (t.includes('fail')) return SIGN.failed;
  if (t.includes('wait')) return SIGN.wait;
  if (t.includes('shared') || t.includes('other stages')) return SIGN.shared;
  if (t.includes('too big') || t.includes('split')) return SIGN.big;
  if (t.includes('memory') || t.includes('spill')) return SIGN.spill;
  if (t.includes('garbage') || t.startsWith('gc')) return SIGN.gc;
  if (t.includes('tiny')) return SIGN.tiny;
  if (t.includes('held') || t.includes('slow') || t.includes('median') || t.includes('last 10%')) return SIGN.slow;
  if (t.includes('time went')) return SIGN.time;
  if (t.includes('nothing') || t.includes('no problem') || t === 'quick') return SIGN.ok;
  return '';
};

/** Horizontal bars on one scale, the highlighted one in the card's warning colour. */
export function Bars({ rows, fmt }: { rows: [string, number | null, boolean][]; fmt: (x: number | null) => string }) {
  const max = Math.max(1, ...rows.map((r) => r[1] ?? 0));
  return (
    <div className="vbars">
      {rows.map(([label, v, hot]) => (
        <div key={label} className="vbar-row">
          <span className="vbar-label">{label}</span>
          <span className="vbar-track">
            <span style={{ width: `${Math.max(1, ((v ?? 0) / max) * 100)}%`, background: hot ? 'var(--st-warn)' : 'var(--series-1)' }} />
          </span>
          <span className="vbar-val">{fmt(v)}</span>
        </div>
      ))}
    </div>
  );
}

/** One bar split into parts (waiting / running, this stage / others / idle), labelled under it. */
export function Split({ parts, fmt, pct }: { parts: [string, number, string][]; fmt: (x: number | null) => string; pct?: boolean }) {
  const tot = parts.reduce((a, p) => a + Math.max(0, p[1]), 0) || 1;
  return (
    <div className="vsplit">
      <div className="vsplit-bar">
        {parts.map(([label, v, color]) => (v > 0 ? <span key={label} style={{ width: `${(v / tot) * 100}%`, background: color }} title={`${label}: ${fmt(v)}`} /> : null))}
      </div>
      <div className="vsplit-legend">
        {parts.filter((p) => p[1] > 0).map(([label, v, color]) => (
          <span key={label}><i style={{ background: color }} />{label} {pct ? fmtPct(v / tot, 0) : fmt(v)}</span>
        ))}
      </div>
    </div>
  );
}


/** `compact`: the note moves into the card's tooltip (an ⓘ marks it), so a row of cards is numbers and bars only. */
export function VCards({ cards, compact = false }: { cards: VCard[]; compact?: boolean }) {
  return (
    <div className={`vcards ${compact ? 'compact' : ''}`}>
      {cards.map((c, i) => (
        <div key={i} className={`vcard ${c.tone}`} title={compact && c.note ? c.note : undefined}>
          <div className="vcard-title"><span className="vcard-sign" aria-hidden>{c.icon ?? signFor(c.title)}</span>{c.title}{compact && c.note ? <span className="vcard-info" aria-hidden> ⓘ</span> : null}</div>
          <div className="vcard-big">
            {c.big}
            {c.unit ? <span className="vcard-unit"> {c.unit}</span> : null}
          </div>
          {c.chart}
          {c.note && !compact ? <div className="vcard-note">{c.note}</div> : null}
        </div>
      ))}
    </div>
  );
}
