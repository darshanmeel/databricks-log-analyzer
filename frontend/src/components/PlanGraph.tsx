// SQL plan as an operator graph with real per-operator metrics (Revision 7, `sql_plan_nodes`).
// Data flows left to right: scans on the left, the write or result on the right, rows on the edges.
// WholeStageCodegen stages are dashed boxes around the operators they fused; InputAdapter / AdaptiveSparkPlan
// wrappers are hidden, as in the Spark UI. Operators without numbers had no metrics recorded.
// Revision 18: the plan is cut at each shuffle (an Exchange and the ShuffleQueryStage that reads it) into the parts one
// stage runs; each part is matched to its stage by the operator names the stage's RDDs carry (rdd_scopes), coloured,
// and an operator's details say which stage ran it, which stage wrote what it reads and which stage reads its output.
import { useMemo, useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';
import { api, type IncidentRow, type StageWhy } from '../api';
import { to } from '../links';
import { fmtBytes, fmtDuration, fmtNum, truncate } from '../format';
import { useAsync } from '../hooks';
import { Panel } from './ui';

export interface PlanNodeRow {
  spark_context_id: string; sql_execution_id: number; node_id: number; parent_id: number | null; name: string;
  detail: string | null; codegen_id: number | null; is_cluster: boolean | null; rows_out: number | null;
  time_ms: number | null; peak_mem: number | null; spill_bytes: number | null; data_bytes: number | null;
  metrics_json: string | null;
  /** Revision 9: read from the plan text (the log had no operator tree); absent on older builds */
  from_text?: boolean | null;
}

type Heat = 'time' | 'rows' | 'size' | 'mem' | 'spill';
const HEAT: { k: Heat; label: string; get: (n: PlanNodeRow) => number | null; fmt: (v: number) => string }[] = [
  { k: 'time', label: 'Time', get: (n) => n.time_ms, fmt: (v) => fmtDuration(v) },
  { k: 'rows', label: 'Rows', get: (n) => n.rows_out, fmt: (v) => `${fmtCompact(v)} rows` },
  { k: 'size', label: 'Data', get: (n) => n.data_bytes, fmt: (v) => fmtBytes(v) },
  { k: 'mem', label: 'Peak memory', get: (n) => n.peak_mem, fmt: (v) => fmtBytes(v) },
  { k: 'spill', label: 'Spill', get: (n) => n.spill_bytes, fmt: (v) => fmtBytes(v) },
];

interface HotspotRow {
  kind: string; stage_id: number | null; stage_attempt: number | null; operator: string | null; executor_id: string | null;
  value: number | null; baseline: number | null; ratio: number | null; cause: string | null; detail: string | null;
}
type InsightKind = 'skew' | 'spill' | 'explode' | 'broadcast' | 'hot' | 'shuffle';
type Insight = { node: number; kind: InsightKind; label: string; text: string; sev: 'high' | 'medium' };
type Metric = { name: string; type: string; total: number; max?: number; tasks?: number; total_ms?: number; max_ms?: number };

/** Problems found on each operator, flagged on the node the way query profiles do: from the operator's own metrics
 *  (skew = biggest task far above the average task, spill, a join that multiplies rows, a big broadcast, most of the
 *  query's time) and from the analyzer's hotspots (which name the operator of the stage where they peaked). */
function findInsights(rows: PlanNodeRow[], edges: { from: number; to: number }[], hot: HotspotRow[]): Insight[] {
  const out: Insight[] = [];
  const add = (i: Insight) => {
    if (!out.some((o) => o.node === i.node && o.kind === i.kind)) out.push(i);
  };
  const timed = rows.filter((r) => !r.is_cluster && r.time_ms);
  const totalT = timed.reduce((a, r) => a + (r.time_ms ?? 0), 0);
  for (const r of rows) {
    if (r.is_cluster || HIDDEN.has(r.name)) continue;
    const ms: Metric[] = r.metrics_json ? JSON.parse(r.metrics_json) : [];
    for (const m of ms) {
      if (m.max === undefined || !m.tasks || m.tasks < 4 || !m.total) continue;
      const avg = m.total / m.tasks;
      const ratio = m.max / avg;
      const timing = m.type === 'timing' || m.type === 'nsTiming';
      const big = timing ? (m.max_ms ?? m.max) >= 1000 : m.type === 'size' ? m.max >= 64 * 2 ** 20 : m.max >= 1e6;
      if (ratio >= 4 && big) {
        const v = timing ? fmtDuration(m.max_ms ?? m.max) : m.type === 'size' ? fmtBytes(m.max) : fmtCompact(m.max);
        add({ node: r.node_id, kind: 'skew', label: `Skew ${ratio.toFixed(0)}×`, sev: ratio >= 10 ? 'high' : 'medium', text: `In this operator's ${m.name}, the busiest task had ${v}, ${ratio.toFixed(0)}× the average of its ${m.tasks} tasks: one partition holds much more data than the rest. (A stage's skew finding compares whole task times to the median, so its ratio differs.)` });
        break;
      }
    }
    if (r.spill_bytes) add({ node: r.node_id, kind: 'spill', label: 'Spill', sev: r.spill_bytes >= 2 ** 30 ? 'high' : 'medium', text: `Spilled ${fmtBytes(r.spill_bytes)} to disk: its data did not fit in execution memory.` });
    if (/Join|Generate/i.test(r.name) && r.rows_out) {
      const known = edges
        .filter((e) => e.to === r.node_id)
        .map((e) => rows.find((x) => x.node_id === e.from)?.rows_out ?? null)
        .filter((x): x is number => x !== null);
      const biggest = Math.max(0, ...known);
      if (known.length && biggest > 0 && r.rows_out >= 2 * biggest && r.rows_out >= 1e5)
        add({ node: r.node_id, kind: 'explode', label: `Rows ×${(r.rows_out / biggest).toFixed(0)}`, sev: r.rows_out >= 10 * biggest ? 'high' : 'medium', text: `Produces ${fmtCompact(r.rows_out)} rows from at most ${fmtCompact(biggest)} on one input: duplicate join keys multiply rows. Check the join condition.` });
    }
    if (/BroadcastExchange/.test(r.name) && (r.data_bytes ?? 0) >= 512 * 2 ** 20)
      add({ node: r.node_id, kind: 'broadcast', label: 'Big broadcast', sev: 'medium', text: `Broadcasts ${fmtBytes(r.data_bytes!)} to every executor; large broadcasts cost driver and executor memory.` });
    if (r.time_ms && totalT && timed.length > 2 && r.time_ms / totalT >= 0.5)
      add({ node: r.node_id, kind: 'hot', label: 'Most time', sev: 'medium', text: `${Math.round((r.time_ms / totalT) * 100)}% of the operator time in this query (${fmtDuration(r.time_ms)}).` });
  }
  for (const h of hot) {
    if (!h.operator || (h.ratio ?? 0) < 2) continue;
    const kind: InsightKind | null = h.kind === 'skew_task' ? 'skew' : h.kind === 'spill_peak' ? 'spill' : h.kind === 'shuffle_peak' ? 'shuffle' : null;
    if (!kind) continue;
    const op = h.operator;
    const r = rows.find((x) => !x.is_cluster && !HIDDEN.has(x.name) && (op.startsWith(x.name) || (x.detail ?? '').startsWith(op)));
    if (!r) continue;
    const where = `stage ${h.stage_id}${h.executor_id ? `, executor ${h.executor_id}` : ''}`;
    const ratio = h.ratio ?? 0;
    const text =
      kind === 'skew'
        ? `In ${where} one task took ${fmtDuration(h.value ?? 0)}, ${ratio.toFixed(0)}× the median task.`
        : kind === 'spill'
          ? `Spill peaked in ${where}: ${fmtBytes(h.value ?? 0)}, ${ratio.toFixed(1)}× the usual.`
          : `Shuffle peaked in ${where}: ${fmtBytes(h.value ?? 0)}, ${ratio.toFixed(1)}× the usual.`;
    add({ node: r.node_id, kind, label: kind === 'skew' ? `Skew ${ratio.toFixed(0)}×` : kind === 'spill' ? 'Spill' : 'Shuffle peak', sev: ratio >= 10 ? 'high' : 'medium', text });
  }
  return out.sort((a, b) => (a.sev === b.sev ? 0 : a.sev === 'high' ? -1 : 1));
}

const HIDDEN = new Set(['InputAdapter', 'AdaptiveSparkPlan']);
const NW = 210;
const NH = 80;

interface PStage {
  spark_context_id: string; stage_id: number; stage_attempt: number; spark_job_id: number | null; status: string | null;
  start_time: number | null; duration_ms: number | null; tasks: number | null; input_bytes: number | null; shuffle_read: number | null;
  shuffle_write: number | null; disk_spill: number | null; mem_spill: number | null; p50_task_ms: number | null; max_task_ms: number | null;
  rdd_scopes: string[] | string | null;
}
interface Seg { id: number; nodes: number[]; stages: PStage[]; color: string; label: string; parent: number | null }
const SEG_COLORS = ['var(--series-1)', 'var(--series-3)', 'var(--series-4)', 'var(--series-6)', 'var(--series-2)', 'var(--series-7)', 'var(--series-5)'];
const BOUNDARY = /^(Exchange|ShuffleExchange|BroadcastExchange|ReusedExchange)\b/;
const opName = (n: string) => n.replace(/^Execute\s+/, '').trim();

/** The plan cut at its shuffles into the parts one stage each runs, and the stages matched to them. */
function stageSegments(all: PlanNodeRow[], stages: PStage[]): { segOf: Map<number, number>; segs: Seg[] } {
  const byId = new Map(all.map((r) => [r.node_id, r]));
  const segOf = new Map<number, number>();
  const segIdOf = (r: PlanNodeRow): number => {
    const got = segOf.get(r.node_id);
    if (got !== undefined) return got;
    const parent = r.parent_id !== null ? byId.get(r.parent_id) : undefined;
    const v = !parent || BOUNDARY.test(r.name) ? r.node_id : segIdOf(parent);
    segOf.set(r.node_id, v);
    return v;
  };
  all.forEach(segIdOf);
  const ids = [...new Set(segOf.values())];
  const scopes = (st: PStage): string[] => {
    const v = st.rdd_scopes;
    if (!v) return [];
    if (Array.isArray(v)) return v;
    try { const j = JSON.parse(v); return Array.isArray(j) ? j : []; } catch { return []; }
  };
  const segs: Seg[] = ids.map((id) => {
    const head = byId.get(id)!;
    const parent = head.parent_id !== null ? segOf.get(head.parent_id) ?? null : null;
    return { id, nodes: all.filter((r) => segOf.get(r.node_id) === id).map((r) => r.node_id), stages: [], color: '', label: '', parent };
  });
  // each stage goes to the part whose operators its RDDs name most (a scan of the same table counts most)
  for (const st of stages) {
    const sc = new Set(scopes(st));
    if (!sc.size) continue;
    const cg = new Set([...sc].map((x) => /^WholeStageCodegen \((\d+)\)/.exec(x)?.[1]).filter(Boolean).map(Number));
    let best: Seg | null = null, bestScore = 0;
    for (const sg of segs) {
      let score = 0;
      for (const nid of sg.nodes) {
        const r = byId.get(nid)!;
        if (r.is_cluster || HIDDEN.has(r.name)) continue;
        const nm = opName(r.name);
        if (sc.has(nm)) score += nm.startsWith('Scan ') ? 6 : 1;
        if (r.codegen_id !== null && cg.has(r.codegen_id)) score += 2;
      }
      if (score > bestScore) { best = sg; bestScore = score; }
    }
    if (best) best.stages.push(st);
  }
  const PRI = [/^Write|InsertInto|SaveIntoDataSource/, /Join/, /^Scan /, /Aggregate/, /^Sort$/, /^Window/, /^Generate/, /^Project/];
  segs.sort((a, b) => (Math.min(...a.stages.map((x) => x.stage_id), 1e12) - Math.min(...b.stages.map((x) => x.stage_id), 1e12)) || b.id - a.id);
  segs.forEach((sg, i) => {
    sg.color = SEG_COLORS[i % SEG_COLORS.length];
    const names = sg.nodes.map((n) => byId.get(n)!).filter((r) => !r.is_cluster && !HIDDEN.has(r.name)).map((r) => opName(r.name));
    const pick = PRI.map((re) => names.find((x) => re.test(x))).find(Boolean) ?? names[0] ?? '';
    sg.label = pick.replace(/^Scan (\w+) .*/, 'Scan $1');
  });
  return { segOf, segs };
}

const stageWord = (st: PStage) => `Stage ${st.stage_id}${st.stage_attempt ? `.${st.stage_attempt}` : ''}`;
const COL = NW + 64;
const ROWH = NH + 22;

function fmtCompact(n: number): string {
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return fmtNum(n);
}

const shortName = (n: string) => n.replace(/^Execute\s+/, '').replace(/Command$/, '');

export function PlanGraph({ cid, ctx, id }: { cid: string; ctx: string; id: string }) {
  const st = useAsync((s) => api.datasetOpt<PlanNodeRow>(cid, 'sql_plan_nodes', { spark_context_id: ctx, sql_execution_id: id, limit: 2000 }, s), [cid, ctx, id]);
  const extra = useAsync(
    async (s) => {
      const [hot, inc] = await Promise.all([
        api.datasetOpt<HotspotRow>(cid, 'hotspots', { spark_context_id: ctx, sql_execution_id: id, limit: 500 }, s),
        api.datasetOpt<IncidentRow>(cid, 'incidents', { sql_execution_id: id, limit: 500 }, s),
      ]);
      return { hot: hot?.rows ?? [], inc: (inc?.rows ?? []).filter((r) => r.spark_context_id === ctx || r.spark_context_id === null) };
    },
    [cid, ctx, id],
  );
  const sst = useAsync(
    async (s) => {
      const [stg, why] = await Promise.all([
        api.datasetOpt<PStage>(cid, 'stages', { spark_context_id: ctx, sql_execution_id: id, limit: 500 }, s),
        api.stageWhy(cid, ctx, { query: Number(id) }, s).catch(() => ({ stages: [] as StageWhy[] })),
      ]);
      return { stages: stg?.rows ?? [], why: why.stages };
    },
    [cid, ctx, id],
  );
  const [heat, setHeat] = useState<Heat>('time');
  const [sel, setSel] = useState<number | null>(null);
  const [hiSeg, setHiSeg] = useState<number | null>(null);
  const [anyway, setAnyway] = useState(false);
  const rows = st.data?.rows ?? null;

  const g = useMemo(() => (rows && rows.length > 1 ? layout(rows) : null), [rows]);
  const parts = useMemo(() => (rows && rows.length > 1 ? stageSegments(rows, sst.data?.stages ?? []) : null), [rows, sst.data]);
  const segById = new Map((parts?.segs ?? []).map((x) => [x.id, x]));
  const segOfNode = (nid: number) => (parts ? segById.get(parts.segOf.get(nid) ?? -1) ?? null : null);
  const waitOf = new Map((sst.data?.why ?? []).map((w) => [`${w.stage_id}.${w.stage_attempt}`, w.wait_ms]));
  const staged = (parts?.segs ?? []).filter((x) => x.stages.length > 0);
  const insights = useMemo(() => (g && rows ? findInsights(rows, g.edges, extra.data?.hot ?? []) : []), [g, rows, extra.data]);
  const incidents = useMemo(() => {
    const m = new Map<string, IncidentRow>();
    for (const r of extra.data?.inc ?? []) if (!m.has(r.incident_id)) m.set(r.incident_id, r);
    return [...m.values()];
  }, [extra.data]);
  if (st.loading && !st.data) return <Panel title="Operators">{<p className="muted small">Loading operators…</p>}</Panel>;
  if (!g) return null; // older build, or a query whose plan has no operators
  const h = HEAT.find((x) => x.k === heat)!;
  const vis = g.nodes.filter((n) => !n.cluster);
  const max = Math.max(0, ...vis.map((n) => h.get(n.row) ?? 0));
  const withDefs = vis.filter((n) => n.row.metrics_json !== null);
  const measured = withDefs.filter((n) => n.row.metrics_json !== '[]').length;
  // the log had no operator tree, so the analyzer read the operators from the plan text: there are no numbers
  const fromText = vis.length > 0 && vis.every((n) => !!n.row.from_text);
  const top = [...vis].filter((n) => (h.get(n.row) ?? 0) > 0).sort((a, b) => (h.get(b.row) ?? 0) - (h.get(a.row) ?? 0)).slice(0, 6);
  const selected = g.nodes.find((n) => n.row.node_id === sel) ?? null;
  // operator times that are a sliver of the stages' time say nothing (53 ms in an hour-long query): hide them
  const stageMs = (sst.data?.stages ?? []).reduce((a, x) => a + ((x as { duration_ms?: number | null }).duration_ms ?? 0), 0);
  const topMs = heat === 'time' ? top.reduce((a, n) => a + (h.get(n.row) ?? 0), 0) : null;
  const tooSmall = topMs !== null && stageMs > 60_000 && topMs < 0.01 * stageMs;
  // most boxes would read "no numbers recorded": one line instead, the graph on request
  if (!anyway && !fromText && vis.length >= 2 && measured < vis.length / 2)
    return (
      <Panel title="Operators">
        <p className="muted small" style={{ margin: 0 }}>
          Spark recorded numbers for {measured} of {vis.length} operators, so the graph would say little; the plan below and the Stages tab have the timings.{' '}
          <button className="linkish" onClick={() => setAnyway(true)}>Show the graph anyway</button>
        </p>
      </Panel>
    );

  return (
    <Panel
      title="Operators"
      note={
        fromText
          ? "Each box is one step of the query plan. Data flows left to right. This query's log has no operator metrics, so the graph is drawn from the plan text and carries no numbers; the Stages and Trace tabs have the timings."
          : 'Each box is one step of the query plan, with the numbers Spark recorded for it. Data flows left to right; the number on an arrow is the rows passed on. Dashed boxes are codegen stages: operators fused into one loop, timed together.'
      }
      actions={
        !fromText && (
        <div className="seg" role="group" aria-label="Colour operators by">
          {HEAT.map((x) => (
            <button key={x.k} className={heat === x.k ? 'on' : ''} aria-pressed={heat === x.k} onClick={() => setHeat(x.k)}>
              {x.label}
            </button>
          ))}
        </div>
        )
      }
    >
      {staged.length > 1 && (
        <div className="plan-stages">
          <span className="muted small">Ran as {staged.length} stages, left to right · click one to pick out its operators:</span>
          {staged.map((sg) => {
            const st0 = sg.stages[0];
            const dur = Math.max(...sg.stages.map((x) => x.duration_ms ?? 0));
            const sp = sg.stages.reduce((a, x) => a + (x.disk_spill ?? 0), 0);
            return (
              <button key={sg.id} className={`plan-stage-chip ${hiSeg === sg.id ? 'on' : ''}`} style={{ ['--seg' as string]: sg.color }}
                aria-pressed={hiSeg === sg.id} onClick={() => setHiSeg(hiSeg === sg.id ? null : sg.id)}>
                <i />
                <b>{stageWord(st0)}{sg.stages.length > 1 ? ` +${sg.stages.length - 1}` : ''}</b>
                <span>{sg.label}</span>
                <span className="muted">{fmtDuration(dur)}{sp ? ` · spilled ${fmtBytes(sp)}` : ''}</span>
              </button>
            );
          })}
        </div>
      )}
      <div className="plan-graph-wrap">
        <div className="plan-graph-scroll">
          <svg viewBox={`0 0 ${g.W} ${g.H}`} width={g.W} height={g.H} role="img" aria-label="Query plan operators" style={{ display: 'block', maxWidth: 'none' }}>
            <defs>
              <marker id="pg-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
                <path d="M0,0 L10,5 L0,10 Z" style={{ fill: 'var(--text-2)' }} />
              </marker>
            </defs>
            {g.clusters.map((c) => (
              <g key={`cg${c.id}`}>
                <rect x={c.x} y={c.y} width={c.w} height={c.h} rx={8} style={{ fill: 'none', stroke: 'var(--accent, var(--series-1))', strokeDasharray: '5 4', strokeWidth: 1.2 }} />
                <text x={c.x + 8} y={c.y + 14} style={{ fill: 'var(--accent, var(--series-1))', fontSize: 11, fontWeight: 600 }}>
                  Codegen stage {c.id}
                  {c.row?.time_ms !== null && c.row?.time_ms !== undefined ? ` · ${fmtDuration(c.row.time_ms)}` : ''}
                </text>
              </g>
            ))}
            {g.edges.map((e, i) => {
              const a = g.pos.get(e.from)!;
              const b = g.pos.get(e.to)!;
              const x1 = a.x + NW;
              const y1 = a.y + NH / 2;
              const x2 = b.x;
              const y2 = b.y + NH / 2;
              const mx = (x1 + x2) / 2;
              const r = g.nodes.find((n) => n.row.node_id === e.from)?.row.rows_out;
              return (
                <g key={i}>
                  <path d={`M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2 - 2},${y2}`} markerEnd="url(#pg-arrow)" style={{ fill: 'none', stroke: 'var(--text-2)', strokeWidth: 1.2, opacity: 0.7 }} />
                  {r !== null && r !== undefined && (
                    <text x={mx} y={(y1 + y2) / 2 - 4} textAnchor="middle" style={{ fill: 'var(--text-2)', fontSize: 10.5, paintOrder: 'stroke', stroke: 'var(--surface)', strokeWidth: 3 }}>
                      {fmtCompact(r)}
                    </text>
                  )}
                </g>
              );
            })}
            {vis.map((n) => {
              const p = g.pos.get(n.row.node_id)!;
              const v = h.get(n.row);
              const share = v && max ? v / max : 0;
              const none = n.row.metrics_json === '[]';
              const noDefs = n.row.metrics_json === null;
              const on = sel === n.row.node_id;
              const facts = [
                n.row.time_ms !== null ? fmtDuration(n.row.time_ms) : null,
                n.row.rows_out !== null ? `${fmtCompact(n.row.rows_out)} rows` : null,
                n.row.data_bytes ? fmtBytes(n.row.data_bytes) : null,
                n.row.peak_mem ? `peak ${fmtBytes(n.row.peak_mem)}` : null,
                n.row.spill_bytes ? `spill ${fmtBytes(n.row.spill_bytes)}` : null,
              ].filter(Boolean) as string[];
              return (
                <g key={n.row.node_id} transform={`translate(${p.x},${p.y})`} style={{ cursor: 'pointer', opacity: hiSeg !== null && segOfNode(n.row.node_id)?.id !== hiSeg ? 0.3 : 1 }} onClick={() => setSel(on ? null : n.row.node_id)}>
                  <title>{n.row.detail ?? n.row.name}</title>
                  <rect width={NW} height={NH} rx={8} style={{ fill: 'var(--surface)', stroke: on ? 'var(--accent, var(--series-1))' : n.row.spill_bytes ? 'var(--st-warn)' : 'var(--border)', strokeWidth: on ? 2 : 1, strokeDasharray: none ? '3 3' : undefined, filter: on ? 'drop-shadow(0 2px 6px rgba(0,0,0,.18))' : undefined }} />
                  {(() => {
                    const sg = segOfNode(n.row.node_id);
                    if (!sg || !staged.length) return null;
                    const st0 = sg.stages[0];
                    return (
                      <>
                        <rect x={0} y={0} width={4} height={NH} rx={2} style={{ fill: sg.color }} />
                        <text x={12} y={68} style={{ fill: st0 ? sg.color : 'var(--text-3)', fontSize: 10.5, fontWeight: 600 }}>
                          {st0 ? `${stageWord(st0)}${st0.spark_job_id !== null ? ` · job ${st0.spark_job_id}` : ''}` : 'no stage of its own (reused or skipped)'}
                        </text>
                      </>
                    );
                  })()}
                  {share > 0 && <rect x={0} y={NH - 5} width={NW * share} height={5} rx={2} style={{ fill: heat === 'spill' || heat === 'mem' ? 'var(--spill)' : 'var(--series-1)' }} />}
                  {share > 0 && <rect width={NW} height={NH} rx={6} style={{ fill: heat === 'spill' || heat === 'mem' ? 'var(--spill)' : 'var(--series-1)', opacity: 0.05 + share * 0.2 }} />}
                  <text x={12} y={18} style={{ fill: 'var(--text-1, var(--text))', fontSize: 12.5, fontWeight: 600 }}>
                    {truncate(shortName(n.row.name), 28)}
                  </text>
                  <text x={12} y={34} style={{ fill: 'var(--text-2)', fontSize: 10.5 }}>
                    {truncate(detailOf(n.row), 36)}
                  </text>
                  {insights
                    .filter((x) => x.node === n.row.node_id)
                    .slice(0, 2)
                    .map((x, k, arr) => {
                      const w = x.label.length * 6.4 + 14;
                      const right = arr.slice(0, k).reduce((a, y) => a + y.label.length * 6.4 + 18, 0);
                      return (
                        <g key={x.kind} transform={`translate(${NW - w - 4 - right},-9)`}>
                          <title>{x.text}</title>
                          <rect width={w} height={17} rx={8.5} style={{ fill: x.sev === 'high' ? 'var(--st-crit)' : 'var(--st-warn)' }} />
                          <text x={w / 2} y={12} textAnchor="middle" style={{ fill: 'var(--surface)', fontSize: 10.5, fontWeight: 700 }}>
                            {x.label}
                          </text>
                        </g>
                      );
                    })}
                  <text x={12} y={51} style={{ fill: none ? 'var(--text-2)' : 'var(--text-1, var(--text))', fontSize: 11, fontStyle: none ? 'italic' : undefined }}>
                    {none ? 'no numbers recorded' : noDefs ? (fromText ? '' : 'Spark records nothing here') : truncate(facts.join(' · '), 38) || 'no numbers recorded'}
                  </text>
                </g>
              );
            })}
          </svg>
        </div>
        <div className="plan-side">
          {selected && (
            <NodeMetrics n={selected.row} insights={insights.filter((x) => x.node === selected.row.node_id)} onClose={() => setSel(null)}
              stage={<OperatorStage cid={cid} ctx={ctx} row={selected.row} rows={rows ?? []} seg={segOfNode(selected.row.node_id)} segs={parts?.segs ?? []} waitOf={waitOf} />} />
          )}
          {(insights.length > 0 || incidents.length > 0) && (
            <div className="plan-insights">
              <h3 style={{ margin: '0 0 6px' }}>What to look at</h3>
              <ul>
                {insights.map((x, k) => {
                  const node = rows?.find((r) => r.node_id === x.node);
                  return (
                    <li key={k}>
                      <button onClick={() => setSel(x.node)} className={sel === x.node ? 'on' : ''}>
                        <span className={`pi-badge sev-${x.sev}`}>{x.label}</span> <b>{node ? shortName(node.name) : ''}</b>
                        <span className="pi-text">{x.text}</span>
                      </button>
                    </li>
                  );
                })}
                {incidents.map((r) => (
                  <li key={r.incident_id}>
                    <Link to={to.findings(cid, r.finding_id)} className="pi-inc">
                      <span className={`pi-badge sev-${r.incident_severity}`}>{r.incident_id}</span> {r.incident_title}
                      {r.incident_impact && <span className="pi-text">{r.incident_impact}</span>}
                    </Link>
                  </li>
                ))}
              </ul>
            </div>
          )}
          {!fromText && <h3 style={{ margin: '0 0 6px' }}>Top operators by {h.label.toLowerCase()}</h3>}
          {fromText ? null : tooSmall ? (
            <p className="muted small">The operator times add up to {fmtDuration(topMs)} of the stages' {fmtDuration(stageMs)}: Spark timed only a sliver of the work here, so they are not shown. Use the stage times.</p>
          ) : top.length ? (
            <ol className="plan-top">
              {top.map((n) => {
                const v = h.get(n.row)!;
                return (
                  <li key={n.row.node_id} className={sel === n.row.node_id ? 'on' : ''}>
                    <button onClick={() => setSel(n.row.node_id)}>
                      <span className="plan-top-name">{shortName(n.row.name)}</span>
                      <span className="plan-top-val">{h.fmt(v)}</span>
                      <span className="mini-bar">
                        <span style={{ width: `${(v / max) * 100}%`, background: heat === 'spill' || heat === 'mem' ? 'var(--spill)' : 'var(--series-1)' }} />
                      </span>
                    </button>
                  </li>
                );
              })}
            </ol>
          ) : (
            <p className="muted small">No operator recorded {h.label.toLowerCase()}.</p>
          )}
          {fromText ? (
            <p className="muted small">Click a box to see its full plan line.</p>
          ) : (
          <p className="muted small">
            {measured} of {withDefs.length} measurable operators have numbers.
            {measured < withDefs.length ? ' Spark recorded no numbers for the rest (operators adaptive execution replaced, and work it did not measure): use the stage times instead.' : ''}
          </p>
          )}
        </div>
      </div>
    </Panel>
  );
}

function detailOf(r: PlanNodeRow): string {
  const d = r.detail ?? '';
  const name = r.name;
  return d.startsWith(name) ? d.slice(name.length).trim() || '' : d.replace(/^Execute\s+/, '');
}

/** Which stage ran an operator, with its numbers; for a shuffle, which stage wrote it and which read it. */
function OperatorStage({ cid, ctx, row, rows, seg, segs, waitOf }: { cid: string; ctx: string; row: PlanNodeRow; rows: PlanNodeRow[]; seg: Seg | null; segs: Seg[]; waitOf: Map<string, number | null> }) {
  if (!seg) return null;
  const b = (v: number | null | undefined) => (v ? fmtBytes(v) : '–');
  // the parts that feed this one (their Exchange's parent is in this part) and the part this one feeds
  const feeds = segs.filter((x) => x.parent === seg.id && x.stages.length);
  const into = seg.parent !== null ? segs.find((x) => x.id === seg.parent) ?? null : null;
  const isShuffleRead = /ShuffleQueryStage|AQEShuffleRead|BroadcastQueryStage|ShuffledRowRDD/.test(row.name);
  const reads = isShuffleRead ? feeds.filter((x) => {
    // the Exchange under this operator
    let r: PlanNodeRow | undefined = rows.find((y) => y.node_id === x.id);
    while (r && r.parent_id !== null) { if (r.parent_id === row.node_id) return true; r = rows.find((y) => y.node_id === r!.parent_id); }
    return false;
  }) : [];
  const link = (st: PStage) => <Link to={to.stages(cid, ctx, st.stage_id, st.stage_attempt)}>{stageWord(st)}</Link>;
  return (
    <div className="op-stage" style={{ ['--seg' as string]: seg.color }}>
      {seg.stages.length ? seg.stages.map((st) => {
        const w = waitOf.get(`${st.stage_id}.${st.stage_attempt}`) ?? null;
        return (
          <div key={`${st.stage_id}.${st.stage_attempt}`} className="op-stage-card">
            <div className="op-stage-head"><i />Ran in <b>{link(st)}</b>{st.spark_job_id !== null ? <span className="muted"> · job {st.spark_job_id}</span> : null}{st.status && st.status !== 'succeeded' && st.status !== 'complete' ? <span className="bad"> · {st.status}</span> : null}</div>
            <dl className="op-stage-grid">
              <dt>Took</dt><dd>{fmtDuration(st.duration_ms)}{w && w >= 1000 ? <span className="st-warn"> · waited {fmtDuration(w)} for cores</span> : null}</dd>
              <dt>Tasks</dt><dd>{fmtNum(st.tasks)}{st.p50_task_ms !== null ? <span className="muted"> · median {fmtDuration(st.p50_task_ms)}, slowest {fmtDuration(st.max_task_ms)}</span> : null}</dd>
              {st.input_bytes ? <><dt>Read files</dt><dd>{b(st.input_bytes)}</dd></> : null}
              {st.shuffle_read || st.shuffle_write ? <><dt>Shuffle</dt><dd><span style={{ color: 'var(--shuf)' }}>read {b(st.shuffle_read)} · wrote {b(st.shuffle_write)}</span></dd></> : null}
              {st.disk_spill ? <><dt>Spilled</dt><dd style={{ color: 'var(--spill)' }}>{b(st.disk_spill)} to disk</dd></> : null}
            </dl>
          </div>
        );
      }) : <p className="muted small" style={{ margin: 0 }}>No stage ran this part: Spark reused an earlier shuffle or skipped it.</p>}
      {BOUNDARY.test(row.name) && into?.stages.length ? <p className="small" style={{ margin: '4px 0 0' }}>Its output (the shuffle files) is read by {into.stages.map((x, i) => <span key={i}>{i ? ', ' : ''}{link(x)}</span>)}.</p> : null}
      {reads.length ? <p className="small" style={{ margin: '4px 0 0' }}>Reads the shuffle written by {reads.flatMap((x) => x.stages).map((x, i) => <span key={i}>{i ? ', ' : ''}{link(x)}</span>)}.</p> : null}
    </div>
  );
}

function NodeMetrics({ n, insights, onClose, stage }: { n: PlanNodeRow; insights: Insight[]; onClose: () => void; stage?: ReactNode }) {
  const ms: Metric[] = n.metrics_json ? JSON.parse(n.metrics_json) : [];
  const noDefs = n.metrics_json === null;
  const show = (m: (typeof ms)[number], k: 'total' | 'max') => {
    const v = k === 'total' ? m.total : m.max;
    if (v === undefined) return '–';
    if (m.type === 'size') return fmtBytes(v);
    if (m.type === 'timing' || m.type === 'nsTiming') return fmtDuration(k === 'total' ? m.total_ms ?? v : m.max_ms ?? v);
    return fmtNum(v);
  };
  return (
    <div className="plan-node-detail">
      <div className="row" style={{ justifyContent: 'space-between', alignItems: 'baseline' }}>
        <b>{shortName(n.name)}</b>
        <button className="btn small ghost" onClick={onClose} aria-label="Close operator details">
          ✕
        </button>
      </div>
      {n.detail && <div className="mono small wrap-any muted" style={{ margin: '4px 0 8px' }}>{n.detail}</div>}
      {stage}
      {insights.map((x) => (
        <p key={x.kind} className="small" style={{ margin: '0 0 6px' }}>
          <span className={`pi-badge sev-${x.sev}`}>{x.label}</span> {x.text}
        </p>
      ))}
      {ms.length ? (
        <table className="exec-table">
          <thead>
            <tr>
              <th>Metric</th>
              <th className="num">Total</th>
              <th className="num">Max per task</th>
            </tr>
          </thead>
          <tbody>
            {ms.map((m) => (
              <tr key={m.name}>
                <td>{m.name}</td>
                <td className="num">{show(m, 'total')}</td>
                <td className="num">{show(m, 'max')}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="muted small">{noDefs ? 'Spark records no metrics for this operator; its work is timed with the codegen stage around it.' : 'No numbers: the tasks that ran this operator did not finish successfully.'}</p>
      )}
    </div>
  );
}

interface LNode { row: PlanNodeRow; cluster: boolean }

function layout(all: PlanNodeRow[]) {
  const byId = new Map(all.map((r) => [r.node_id, r]));
  // hide wrappers: each visible node's parent is its nearest visible, non-cluster ancestor
  const hidden = (r: PlanNodeRow) => HIDDEN.has(r.name) || !!r.is_cluster;
  const visParent = (r: PlanNodeRow): number | null => {
    let p = r.parent_id;
    while (p !== null && p !== undefined) {
      const pr = byId.get(p);
      if (!pr) return null;
      if (!hidden(pr)) return p;
      p = pr.parent_id;
    }
    return null;
  };
  const vis = all.filter((r) => !hidden(r));
  const kids = new Map<number | null, number[]>();
  for (const r of vis) {
    const p = visParent(r);
    kids.set(p, [...(kids.get(p) ?? []), r.node_id]);
  }
  const roots = kids.get(null) ?? [];
  const depth = new Map<number, number>();
  const ypos = new Map<number, number>();
  let leaf = 0;
  const walk = (id: number, d: number): number => {
    depth.set(id, d);
    const ch = kids.get(id) ?? [];
    if (!ch.length) {
      const y = leaf++;
      ypos.set(id, y);
      return y;
    }
    const ys = ch.map((c) => walk(c, d + 1));
    const y = (Math.min(...ys) + Math.max(...ys)) / 2;
    ypos.set(id, y);
    return y;
  };
  for (const r of roots) walk(r, 0);
  const maxD = Math.max(0, ...depth.values());
  const PAD = 24;
  const pos = new Map<number, { x: number; y: number }>();
  for (const r of vis) pos.set(r.node_id, { x: PAD + (maxD - (depth.get(r.node_id) ?? 0)) * COL, y: PAD + 10 + (ypos.get(r.node_id) ?? 0) * ROWH });
  const edges = vis.filter((r) => visParent(r) !== null).map((r) => ({ from: r.node_id, to: visParent(r)! }));
  // codegen clusters: box around their visible members
  const clusters = all
    .filter((r) => r.is_cluster && r.codegen_id !== null)
    .map((c) => {
      const mem = vis.filter((r) => r.codegen_id === c.codegen_id && r.spark_context_id === c.spark_context_id).map((r) => pos.get(r.node_id)!);
      if (!mem.length) return null;
      const x0 = Math.min(...mem.map((p) => p.x)) - 10;
      const y0 = Math.min(...mem.map((p) => p.y)) - 22;
      const x1 = Math.max(...mem.map((p) => p.x)) + NW + 10;
      const y1 = Math.max(...mem.map((p) => p.y)) + NH + 10;
      return { id: c.codegen_id!, row: c, x: x0, y: y0, w: x1 - x0, h: y1 - y0 };
    })
    .filter(Boolean) as { id: number; row: PlanNodeRow; x: number; y: number; w: number; h: number }[];
  const nodes: LNode[] = vis.map((row) => ({ row, cluster: false }));
  const W = PAD * 2 + (maxD + 1) * COL - (COL - NW);
  const H = PAD * 2 + 20 + Math.max(1, leaf) * ROWH;
  return { nodes, pos, edges, clusters, W, H };
}
