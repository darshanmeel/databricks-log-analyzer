// Revision 13: what led to what in a run. Every SQL query (that ran Spark jobs) and every Spark job without SQL, one
// row each in start order on the run's clock, and arrows for why one depends on another: it read a table an earlier
// query wrote, it reused shuffle output an earlier job computed, or it ran inside another query. Hover a row to see
// only its chain; click it to open it below.
import { useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { api, type FlowEdge, type FlowNode } from '../api';
import { fmtBytes, fmtDuration, fmtNum, fmtRows, fmtTime, tickLabel, timeTicks, truncate } from '../format';
import { useAsync, useWidth } from '../hooks';
import { useNavigate } from 'react-router-dom';
import { to } from '../links';

const LABEL_W = 250;
const RIGHT_W = 190;
const ROW = 18;
/** A stretch this long with no Spark work of the run is idle time (waiting outside Spark), not driver code to tune. */
const IDLE_MS = 30 * 60_000;
const MAX_ROWS = 160;

const EDGE: Record<FlowEdge['kind'], { color: string; dash?: string; what: string }> = {
  table: { color: 'var(--series-1)', what: 'read a table it wrote' },
  shuffle: { color: 'var(--shuf)', dash: '4 3', what: 'reused its shuffle output' },
  inside: { color: 'var(--text)', dash: '1 3', what: 'ran inside it' },
};

const tone = (n: FlowNode) => (n.status === 'failed' ? 'var(--st-crit)' : n.status === 'incomplete' || n.status === 'running' ? 'var(--st-warn)' : 'var(--series-1)');
const read = (n: FlowNode) => (n.input_bytes ?? 0) + (n.shuffle_read ?? 0);

export function RunFlow({ cid, onPick }: { cid: string; onPick: (n: FlowNode) => void }) {
  const st = useAsync((s) => api.flow(cid, s), [cid]);
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hot, setHot] = useState<string | null>(null);
  const [tip, setTip] = useState<{ x: number; y: number; text: string } | null>(null);
  // parents whose children are folded away (big parents start folded)
  const [folded, setFolded] = useState<Set<string> | null>(null);

  const m = useMemo(() => {
    const f = st.data;
    if (!f || !f.nodes.length) return null;
    let nodes = f.nodes.filter((n) => n.start !== null);
    let hidden = 0;
    if (nodes.length > MAX_ROWS) {
      // keep the failed ones, the ones with arrows, then the longest; in start order
      const linked = new Set(f.edges.flatMap((e) => [e.from, e.to]));
      const dur = (n: FlowNode) => (n.end ?? n.start ?? 0) - (n.start ?? 0);
      const keep = new Set(
        [...nodes.filter((n) => n.status === 'failed'), ...nodes.filter((n) => linked.has(n.key)), ...[...nodes].sort((a, b) => dur(b) - dur(a))]
          .slice(0, MAX_ROWS)
          .map((n) => n.key),
      );
      hidden = nodes.length - keep.size;
      nodes = nodes.filter((n) => keep.has(n.key));
    }
    // hierarchy: a query that ran inside another (root execution) sits under it, indented; the rest in start order
    const present = new Set(nodes.map((n) => n.key));
    const parent = new Map<string, string>();
    const parentKind = new Map<string, FlowEdge['kind']>();
    for (const e of f.edges)
      if (e.kind === 'inside' && present.has(e.from) && present.has(e.to) && !parent.has(e.to)) {
        parent.set(e.to, e.from);
        parentKind.set(e.to, 'inside');
      }
    const nodeBy = new Map(nodes.map((n) => [n.key, n]));
    // not inside anything: nest it under what it needed (read its table, reused its shuffle), the one that finished last
    for (const n of nodes) {
      if (parent.has(n.key)) continue;
      const ups = f.edges.filter((e) => e.to === n.key && e.kind !== 'inside' && present.has(e.from) && e.from !== n.key)
        .map((e) => ({ e, a: nodeBy.get(e.from)! }))
        .sort((x, y) => (y.a.end ?? 0) - (x.a.end ?? 0));
      // never under one of its own descendants
      const isDesc = (k: string) => { for (let q = parent.get(k); q; q = parent.get(q)) if (q === n.key) return true; return false; };
      const pick = ups.find((u) => !isDesc(u.a.key));
      if (pick) {
        parent.set(n.key, pick.a.key);
        parentKind.set(n.key, pick.e.kind);
      }
    }
    const kids = new Map<string, FlowNode[]>();
    for (const n of nodes) if (parent.has(n.key)) kids.set(parent.get(n.key)!, [...(kids.get(parent.get(n.key)!) ?? []), n]);
    for (const ks of kids.values()) ks.sort((a, b) => (a.start ?? 0) - (b.start ?? 0));
    const tree: { n: FlowNode; depth: number; last: boolean }[] = [];
    const seen = new Set<string>();
    const walk = (n: FlowNode, depth: number, last: boolean) => {
      if (seen.has(n.key)) return;
      seen.add(n.key);
      tree.push({ n, depth, last });
      const ks = kids.get(n.key) ?? [];
      ks.forEach((k, i) => walk(k, depth + 1, i === ks.length - 1));
    };
    for (const n of nodes) if (!parent.has(n.key)) walk(n, 0, true);
    for (const n of nodes) walk(n, 0, true); // a cycle or a lost parent: still shown
    const row = new Map(tree.map((t, i) => [t.n.key, i]));
    const edges = f.edges.filter((e) => row.has(e.from) && row.has(e.to));
    const t0 = Math.min(...nodes.map((n) => n.start!));
    const t1 = Math.max(...nodes.map((n) => n.end ?? n.start!), t0 + 1000);
    // the chain of the hovered row: everything upstream and downstream of it
    const up = new Map<string, string[]>(), down = new Map<string, string[]>();
    for (const e of edges) {
      up.set(e.to, [...(up.get(e.to) ?? []), e.from]);
      down.set(e.from, [...(down.get(e.from) ?? []), e.to]);
    }
    // the critical path: from what finished last, back through what each step needed (else what ran just before it)
    const crit: FlowNode[] = [];
    let cur: FlowNode | undefined = [...nodes].sort((a, b) => (b.end ?? 0) - (a.end ?? 0))[0];
    const used = new Set<string>();
    while (cur && !used.has(cur.key)) {
      used.add(cur.key);
      crit.push(cur);
      const c: FlowNode = cur;
      const before = (a: FlowNode) => a.key !== c.key && !used.has(a.key) && (a.end ?? Infinity) <= (c.start ?? 0) + 1000;
      const needed = edges.filter((e) => e.to === c.key && e.kind !== 'inside').map((e) => nodeBy.get(e.from)!).filter((a) => a && before(a));
      const pool = needed.length ? needed : nodes.filter(before);
      cur = pool.sort((a, b) => (b.end ?? 0) - (a.end ?? 0))[0];
    }
    crit.reverse();
    const busy = crit.reduce((a, n) => a + Math.max(0, (n.end ?? n.start ?? 0) - (n.start ?? 0)), 0);
    const gaps = crit.slice(1).reduce((a, n, i) => a + Math.max(0, (n.start ?? 0) - (crit[i].end ?? 0)), 0);
    // the longest stretch with no Spark work of this run: an hour-long one is idle time, not driver code to tune
    const idle = crit.slice(1).map((n, i) => ({ ms: Math.max(0, (n.start ?? 0) - (crit[i].end ?? 0)), from: crit[i], to: n }))
      .sort((a, b) => b.ms - a.ms)[0] ?? null;
    // inside the queries on the path: time a stage waited for a free core while none of the query's stages ran
    const waited = crit.reduce((a, n) => a + (n.waits ?? []).reduce((b, [ws, we]) => b + (we - ws), 0), 0);
    const critSet = new Set(crit.map((n) => n.key));
    const prevOnPath = new Map(crit.slice(1).map((n, i) => [n.key, crit[i]]));
    return { nodes, tree, kids, parent, parentKind, edges, row, t0, t1, hidden, up, down, truncated: f.truncated, crit, critSet, prevOnPath, busy, gaps, waited, idle };
  }, [st.data]);

  const chain = useMemo(() => {
    if (!m || !hot) return null;
    const seen = new Set([hot]);
    for (const g of [m.up, m.down]) {
      const todo: string[] = [hot];
      while (todo.length) for (const k of g.get(todo.pop()!) ?? []) if (!seen.has(k)) seen.add(k), todo.push(k);
    }
    return seen;
  }, [m, hot]);

  if (st.loading && !st.data) return <p className="muted small">Loading what led to what…</p>;
  if (st.error || !m) return null;
  const fold = folded ?? new Set([...m.kids].filter(([, ks]) => ks.length > 8).map(([k]) => k));
  const hiddenBy = (k: string): boolean => {
    for (let p = m.parent.get(k); p; p = m.parent.get(p)) if (fold.has(p)) return true;
    return false;
  };
  const vis = m.tree.filter((t) => !hiddenBy(t.n.key));
  const vrow = new Map(vis.map((t, i) => [t.n.key, i]));
  const toggle = (k: string) => {
    const nf = new Set(fold);
    if (nf.has(k)) nf.delete(k);
    else nf.add(k);
    setFolded(nf);
  };

  const W = Math.max(760, width || 1000);
  const x = (t: number) => LABEL_W + ((t - m.t0) / (m.t1 - m.t0)) * (W - LABEL_W - RIGHT_W);
  const H = 24 + vis.length * ROW + 6;
  const yOf = (k: string) => 24 + (vrow.get(k) ?? 0) * ROW + ROW / 2;
  const ticks = timeTicks(m.t0, m.t1, Math.max(3, Math.floor((W - LABEL_W - RIGHT_W) / 110)));
  const kinds = new Set(m.edges.map((e) => e.kind));
  const byKey = new Map(m.nodes.map((n) => [n.key, n]));
  const drawn = m.edges.filter((e) => e.kind !== 'inside' && m.parent.get(e.to) !== e.from && vrow.has(e.from) && vrow.has(e.to));
  const nestedBy = [...m.parentKind.values()];
  const inside = nestedBy.filter((k) => k === 'inside').length;
  const needs = nestedBy.length - inside;

  return (
    <div>
      {m.crit.length > 1 && (() => {
        const total = Math.max(1, (m.crit[m.crit.length - 1].end ?? 0) - (m.crit[0].start ?? 0));
        const busy = m.busy ?? 0;
        const ran = m.waited >= 1000 ? Math.max(0, busy - m.waited) : busy;
        const parts: [string, number, ReactNode][] = [
          ['running', ran, <>running Spark work</>],
          ...(m.waited >= 1000 ? [['waiting', m.waited, <>waiting for a free core inside queries <span className="muted">(grey hatch: other work held the cores)</span></>] as [string, number, ReactNode]] : []),
          ['gaps', m.gaps, <>between queries with no Spark work <span className="muted">(grey dots: its own driver code, Python, JDBC calls, sleeps, or waiting outside Spark)</span></>],
        ];
        const top = [...parts].sort((x, y) => y[1] - x[1])[0];
        return (
          <div className="flow-points small">
            <div><b>Critical path</b> (outlined): {fmtNum(m.crit.length)} steps, {fmtTime(m.crit[0].start)} → {fmtTime(m.crit[m.crit.length - 1].end)}, {fmtDuration(total)}. <span className="muted">Only the steps on that path count below; the cause bar on the Overview counts the whole run.</span></div>
            <ul>
              {parts.map(([k, v, label]) => (
                <li key={k}><b className={k === 'running' ? '' : 'wait-text'}>{fmtDuration(v)}</b> ({Math.round((v / total) * 100)}%) {label}</li>
              ))}
            </ul>
            <div className={top[0] === 'running' ? '' : 'st-warn'}>
              {top[0] === 'gaps' && m.idle && m.idle.ms >= IDLE_MS && m.idle.ms >= total / 2
                ? <><b>It sat idle for {fmtDuration(m.idle.ms)}</b> between {m.idle.from.kind === 'query' ? 'Query' : 'Job'} {m.idle.from.id} ({fmtTime(m.idle.from.end)}) and {m.idle.to.kind === 'query' ? 'Query' : 'Job'} {m.idle.to.id} ({fmtTime(m.idle.to.start)}): its own work took {fmtDuration(Math.max(0, total - m.idle.ms))}. <span className="muted">Nothing from this run reached Spark in that time: it waited on something outside Spark (a sleep, a poll, another job or table to be ready, a lock) or its driver code ran that long. Check the notebook between those two queries.</span></>
                : top[0] === 'gaps' ? <><b>Most of it is outside Spark</b>: look at the driver code between queries, not at the queries.</>
                : top[0] === 'waiting' ? <><b>Most of it is waiting for cores</b>: other runs held them; run fewer at once or add workers.</>
                : <><b>Most of it is Spark work</b>: the slowest steps on the path are where to look.</>}
            </div>
          </div>
        );
      })()}
      <ul className="flow-points small muted">
        <li>{fmtNum(m.nodes.length + m.hidden)} queries and jobs without SQL, each at its own time, left to right in the order they ran.</li>
        <li>Each sits under the one it ran inside ({fmtNum(inside)}) or needed: read its table or its shuffle output ({fmtNum(needs)}). ▸ / ▾ folds them.</li>
        {drawn.length ? <li>{drawn.length} more arrows: {[...kinds].filter((k) => k !== 'inside').map((k) => EDGE[k].what.replace('it ', 'an earlier one ')).join(', ')}.</li> : null}
        {m.hidden ? <li>{m.hidden} short ones without arrows are left out.</li> : null}
        <li>Hover a row for its chain; click it to open it below.</li>
      </ul>
      <div ref={ref} style={{ position: 'relative', overflowX: 'auto', maxHeight: 460, overflowY: 'auto' }}>
        <svg width={W} height={H} role="img" aria-label="Queries and jobs of the run over time, with what each needed from the others" style={{ display: 'block' }}>
          <defs>
            <pattern id="fw-hatch" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
              <rect width="5" height="5" fill="var(--surface-3)" />
              <line x1={0} y1={0} x2={0} y2={5} stroke="var(--wait)" strokeWidth={2} />
            </pattern>
            {Object.entries(EDGE).map(([k, v]) => (
              <marker key={k} id={`fa-${k}`} viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto">
                <path d="M0,0 L8,4 L0,8 Z" fill={v.color} />
              </marker>
            ))}
          </defs>
          {ticks.map((t) => (
            <g key={t}>
              <line x1={x(t)} x2={x(t)} y1={18} y2={H} stroke="var(--line)" strokeDasharray="2 3" />
              <text x={x(t)} y={12} fontSize={11} textAnchor="middle" fill="var(--text)" opacity={0.7}>{tickLabel(t, m.t1 - m.t0)}</text>
            </g>
          ))}
          {vis.map(({ n, depth, last }, i) => {
            const y = 24 + i * ROW;
            const ind = depth * 12;
            const nk = m.kids.get(n.key)?.length ?? 0;
            const s = n.start!, e = n.end ?? m.t1;
            const dim = chain && !chain.has(n.key);
            const data = read(n);
            const waitMs = (n.waits ?? []).reduce((a, [ws, we]) => a + (we - ws), 0);
            const right = [fmtDuration(n.end !== null && n.start !== null ? n.end - n.start : null),
              waitMs >= 2000 ? `waited ${fmtDuration(waitMs)} for cores` : null, data ? `read ${fmtBytes(data)}` : null,
              n.wmed_task_bytes_in ? `~${fmtBytes(n.wmed_task_bytes_in)}/task` : null].filter(Boolean).join(' · ');
            return (
              <g key={n.key} opacity={dim ? 0.25 : 1} style={{ cursor: 'pointer' }}
                onMouseEnter={() => setHot(n.key)}
                onMouseLeave={() => { setHot(null); setTip(null); }}
                onMouseMove={(ev) => {
                  const box = (ev.currentTarget.ownerSVGElement as SVGSVGElement).getBoundingClientRect();
                  setTip({ x: ev.clientX - box.left, y: ev.clientY - box.top, text:
                    `${n.kind === 'query' ? `Query ${n.id}` : `Job ${n.id} (no SQL)`}: ${truncate(n.what ?? '', 160)}\n${fmtTime(n.start)} → ${fmtTime(n.end)} UTC · ${n.status ?? ''}` +
                    (waitMs >= 1000 ? `\nwaited ${fmtDuration(waitMs)} for a free core (cores busy with other work), ran ${fmtDuration(Math.max(0, (n.end ?? 0) - (n.start ?? 0) - waitMs))}` : '') +
                    (n.jobs.length && n.kind === 'query' ? ` · Spark jobs ${n.jobs.slice(0, 6).join(', ')}${n.jobs.length > 6 ? '…' : ''}` : '') +
                    (data ? `\nread ${fmtBytes(data)}${n.output_bytes ? `, wrote ${fmtBytes(n.output_bytes)}` : ''}` : '') +
                    (n.p50_task_bytes_in !== undefined && n.max_task_bytes_in ? `\nper task: median ${fmtBytes(n.p50_task_bytes_in)}, p90 ${fmtBytes(n.p90_task_bytes_in)}, biggest ${fmtBytes(n.max_task_bytes_in)}` +
                      (n.wmed_task_bytes_in ? `; half the data in tasks ≥ ${fmtBytes(n.wmed_task_bytes_in)}` : '') : '') +
                    (n.disk_spill ? `\nspilled ${fmtBytes(n.disk_spill)} to disk` : '') +
                    (n.tables_read.length ? `\nreads ${n.tables_read.slice(0, 4).join(', ')}` : '') +
                    (n.tables_written.length ? `\nwrites ${n.tables_written.slice(0, 4).join(', ')}` : '') +
                    (n.error ? `\n${truncate(n.error, 200)}` : '') });
                }}
                onClick={() => onPick(n)}>
                <rect x={0} y={y} width={W} height={ROW} fill={hot === n.key ? 'var(--hl, rgba(0,0,0,0.04))' : 'transparent'} />
                {depth > 0 && (
                  <path d={`M${ind - 6},${y} V${last ? y + ROW / 2 : y + ROW} M${ind - 6},${y + ROW / 2} H${ind + 1}`} fill="none" stroke="var(--line)" strokeWidth={1} />
                )}
                {nk > 0 && (
                  <text x={ind + 2} y={y + 13} fontSize={10} fill="var(--text)" opacity={0.8} style={{ cursor: 'pointer' }}
                    onClick={(ev) => { ev.stopPropagation(); toggle(n.key); }}>
                    {fold.has(n.key) ? '▸' : '▾'}
                  </text>
                )}
                <text x={ind + (nk ? 13 : 4)} y={y + 13} fontSize={11.5} fill="var(--text)">
                  <tspan fontWeight={600}>{n.kind === 'query' ? `Query ${n.id}` : `Job ${n.id}`}</tspan>
                  <tspan opacity={0.65}> {truncate((n.what ?? '').replace(/\s+/g, ' '), Math.max(8, (n.kind === 'query' ? 26 : 28) - Math.round((ind + (nk ? 9 : 0)) / 6)))}</tspan>
                  {nk && fold.has(n.key) ? <tspan opacity={0.65}> (+{nk})</tspan> : null}
                </text>
                {(() => {
                  const pv = m.prevOnPath.get(n.key);
                  return pv && pv.end !== null && s - pv.end > 0 ? (
                    <g>
                      <line x1={x(pv.end)} x2={x(s)} y1={y + ROW / 2} y2={y + ROW / 2} stroke="var(--wait)" strokeWidth={2} strokeDasharray="1 3" strokeLinecap="round">
                        <title>{`${fmtDuration(s - pv.end)} after ${pv.kind === 'query' ? 'Query' : 'Job'} ${pv.id} with no Spark work of this run: code outside Spark on the driver, or waiting on something outside Spark`}</title>
                      </line>
                      {x(s) - x(pv.end) > 150 && (
                        <text x={(x(pv.end) + x(s)) / 2} y={y + ROW / 2 - 3} fontSize={10.5} textAnchor="middle" fill="var(--wait)">
                          {fmtDuration(s - pv.end)} {s - pv.end >= IDLE_MS ? 'idle: no Spark work from this run' : 'no Spark work (driver code)'}
                        </text>
                      )}
                    </g>
                  ) : null;
                })()}
                {depth > 0 && (() => {
                  const pk = m.parent.get(n.key);
                  const pr = pk ? vrow.get(pk) : undefined;
                  const pn = pk ? byKey.get(pk) : undefined;
                  if (pr === undefined || !pn || m.parentKind.get(n.key) === 'inside') return null;
                  const px = x(pn.end ?? pn.start!);
                  return <path d={`M${px},${24 + pr * ROW + ROW - 4} V${y + ROW / 2} H${x(s) - 1}`} fill="none" stroke="var(--series-1)" strokeOpacity={0.45} strokeWidth={1} strokeDasharray="2 2" />;
                })()}
                <rect x={x(s)} y={y + 4} width={Math.max(2, x(e) - x(s))} height={ROW - 8} rx={2} fill={tone(n)} fillOpacity={0.85}
                  stroke={m.critSet.has(n.key) ? 'var(--text)' : 'none'} strokeWidth={m.critSet.has(n.key) ? 1.5 : 0} />
                {(n.waits ?? []).map(([a, b], wi) => (
                  <rect key={`w${wi}`} x={x(Math.max(a, s))} y={y + 4} width={Math.max(1, x(Math.min(b, e)) - x(Math.max(a, s)))} height={ROW - 8}
                    fill="url(#fw-hatch)" pointerEvents="none" />
                ))}
                {n.disk_spill ? <rect x={x(s)} y={y + ROW - 5} width={Math.max(2, x(e) - x(s))} height={2} fill="var(--spill)" /> : null}
                <text x={x(e) + 5} y={y + 13} fontSize={10.5} fill="var(--text)" opacity={0.7}>{right}</text>
              </g>
            );
          })}
          {drawn.map((e, i) => {
            const a = byKey.get(e.from)!, b = byKey.get(e.to)!;
            const x1 = x(a.end ?? a.start!), y1 = yOf(e.from), x2 = x(b.start!), y2 = yOf(e.to);
            const on = !chain || (chain.has(e.from) && chain.has(e.to));
            const mid = Math.max(x1, x2) + 18;
            const v = EDGE[e.kind];
            const passed = (e.rows != null ? `${fmtRows(e.rows)} rows${e.bytes ? ` (${fmtBytes(e.bytes)})` : ''}` : e.bytes ? fmtBytes(e.bytes) : '')
              + (e.cdf && (e.rows != null || e.bytes) ? ' incl. CDF change rows' : '');
            return (
              <g key={i}>
              <path d={x2 > x1 + 12 ? `M${x1},${y1} C${(x1 + x2) / 2},${y1} ${(x1 + x2) / 2},${y2} ${x2 - 2},${y2}` : `M${x1},${y1} C${mid},${y1} ${mid},${y2} ${x2 - 2},${y2}`}
                fill="none" stroke={v.color} strokeWidth={on ? 1.4 : 0.8} strokeDasharray={v.dash} opacity={on ? 0.9 : 0.15} markerEnd={`url(#fa-${e.kind})`}>
                <title>{`${e.kind === 'table' ? `Wrote ${e.labels.join(', ')}, read later` : e.kind === 'shuffle' ? `Reused shuffle output (${e.labels.join(', ')})` : 'Ran inside it'}${passed ? `: ${passed} passed on` : ''}`}</title>
              </path>
              {passed && on && (chain || drawn.length <= 20) && (
                <text x={x2 - 6} y={y2 - 4} fontSize={9.5} textAnchor="end" fill={v.color}>{passed}</text>
              )}
              </g>
            );
          })}
        </svg>
        {tip && (
          <div className="chart-tip" style={{ position: 'absolute', left: Math.min(tip.x + 12, W - 380), top: tip.y + 14, whiteSpace: 'pre-line', pointerEvents: 'none', maxWidth: 420 }}>
            {tip.text}
          </div>
        )}
      </div>
      <div className="row small muted" style={{ gap: 16, marginTop: 6, flexWrap: 'wrap' }}>
        <span><span style={{ display: 'inline-block', width: 14, height: 8, background: 'var(--series-1)', borderRadius: 2 }} /> ran (red: failed)</span>
        <span><svg width={14} height={8} aria-hidden><rect width={14} height={8} fill="url(#fw-hatch)" /></svg> waiting for a free core</span>
        <span><span style={{ display: 'inline-block', width: 14, height: 2, background: 'var(--spill)', verticalAlign: 'middle' }} /> spilled to disk</span>
        <span><span style={{ display: 'inline-block', width: 14, height: 8, border: '1.5px solid var(--text)', borderRadius: 2 }} /> critical path</span>
        <span><svg width={22} height={8} aria-hidden><line x1={0} x2={22} y1={4} y2={4} stroke="var(--wait)" strokeWidth={2} strokeDasharray="1 3" strokeLinecap="round" /></svg> no Spark work of this run (driver code)</span>
        <span><svg width={22} height={8} aria-hidden><path d="M2,0 V4 H22" fill="none" stroke="var(--series-1)" strokeDasharray="2 2" /></svg> needed what the row above produced</span>
        {Object.entries(EDGE).filter(([k]) => k !== 'inside').map(([k, v]) => (
          <span key={k}>
            <svg width={22} height={8} aria-hidden><line x1={0} x2={22} y1={4} y2={4} stroke={v.color} strokeWidth={1.5} strokeDasharray={v.dash} /></svg> {v.what.replace('it', 'an earlier one')}
          </span>
        ))}
      </div>
    </div>
  );
}

/** "What happened when": every query and job of the run on one timeline, folded or open, on any page. A click opens
 * the query or job in Queries & stages unless the page handles it. */
export function FlowPanel({ cid, open = false, onPick }: { cid: string; open?: boolean; onPick?: (n: FlowNode) => void }) {
  const nav = useNavigate();
  const pick = onPick ?? ((n: FlowNode) => nav(n.kind === 'query' ? to.query(cid, n.ctx, n.id) : to.hierarchy(cid, { ctx: n.ctx, unit: `job:${n.ctx}:${n.id}` })));
  return (
    <details className="panel flow-panel" open={open} style={{ marginBottom: 12 }}>
      <summary className="panel-head" style={{ cursor: 'pointer' }}>
        <h2 style={{ display: 'inline' }}>What happened when</h2>{' '}
        <span className="muted small">every query of the run on one timeline: the critical path, waiting vs running, and which query needed which</span>
      </summary>
      <div className="panel-body">
        <RunFlow cid={cid} onPick={pick} />
      </div>
    </details>
  );
}
