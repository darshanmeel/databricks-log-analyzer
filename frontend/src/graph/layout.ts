// Layouts for the Hierarchy graph: a layered DAG per Spark job (Flow) and a shared time axis (Time).
import { matches, type FilterKey, type GModel, type GNode } from './model';

export interface Rect {
  x: number;
  y: number;
  w: number;
  h: number;
}

/* ------------------------------------------------------------------ flow */

export const FLOW = {
  stageW: 236,
  stageH: 100,
  gapX: 64,
  gapY: 18,
  retryGap: 30,
  pad: 16,
  head: 46,
  appW: 320,
  appH: 58,
  callerW: 236,
  callerH: 70,
  callerGap: 12,
  spineX: 22,
  colGap: 44,
  jobGap: 26,
  top: 16,
};

export interface FlowBox extends Rect {
  id: string;
  kind: 'job' | 'orphans';
  node: GNode | null;
  stages: number;
  hidden: number;
  collapsed: boolean;
  label: string;
}

/** A query or job before / after the focused one (Revision 15): why it is linked and where to go. */
export interface FlowCtx {
  unit: string;
  label: string;
  what: string;
  status: string | null;
  why: string;
  /** steps away from the focused query (1 = next to it); cards are drawn in one column per step */
  depth?: number;
  /** the card it links to, one step closer to the focused query (none: the focused query itself) */
  link?: string;
  /** what the link passed along: rows and bytes written to the table it reads, or to the shuffle it reuses */
  rows?: number | null;
  bytes?: number | null;
}

/** One side of the query box: folded behind a "+" on the box edge, or open. */
export interface FoldSide { open: boolean; count: number; names: string }

/** What to draw around the focused query (Revision 16). */
export interface FlowAround {
  before: FlowCtx[];
  after: FlowCtx[];
  fold?: { before: FoldSide | null; after: FoldSide | null };
  /** draw only the query's header, not its jobs and stages */
  collapsed?: boolean;
}

export interface FlowLayout {
  /** the focused query drawn as a box around all its jobs (null when the focus is not one query) */
  container: (Rect & { node: GNode; collapsed: boolean; jobs: number }) | null;
  fold: FlowAround['fold'] | null;
  /** the queries before (left) and after (right) the focused one */
  ctx: { c: FlowCtx; side: 'before' | 'after'; r: Rect }[];
  pos: Map<string, Rect>;
  boxes: FlowBox[];
  spine: { x: number; y0: number; ticks: number[] } | null;
  width: number;
  height: number;
  dimmed: Set<string>;
}

/** Longest-path layering on in-job `depends` edges, then barycentric ordering inside each layer. */
function layerJob(stages: GNode[], parentsOf: Map<number, Set<number>>) {
  const ids = [...new Set(stages.map((s) => s.stageId ?? -1))];
  const inJob = new Set(ids);
  const parents = (id: number) => [...(parentsOf.get(id) ?? [])].filter((p) => inJob.has(p) && p !== id);
  const layer = new Map<number, number>();
  const visiting = new Set<number>();
  const depth = (id: number): number => {
    const known = layer.get(id);
    if (known !== undefined) return known;
    if (visiting.has(id)) return 0; // cycle guard
    visiting.add(id);
    const ps = parents(id);
    const d = ps.length ? Math.max(...ps.map(depth)) + 1 : 0;
    visiting.delete(id);
    layer.set(id, d);
    return d;
  };
  ids.forEach(depth);
  const nLayers = Math.max(0, ...layer.values()) + 1;
  const layers: number[][] = Array.from({ length: nLayers }, () => []);
  ids.sort((a, b) => a - b).forEach((id) => layers[layer.get(id)!].push(id));

  // barycentric sweeps: down by parents, up by children, down again
  const children = new Map<number, number[]>();
  ids.forEach((id) => parents(id).forEach((p) => children.set(p, [...(children.get(p) ?? []), id])));
  const order = new Map<number, number>();
  const setOrder = () => layers.forEach((l) => l.forEach((id, i) => order.set(id, i)));
  setOrder();
  const bary = (nbrs: number[], fallback: number) => (nbrs.length ? nbrs.reduce((a, n) => a + (order.get(n) ?? 0), 0) / nbrs.length : fallback);
  for (let pass = 0; pass < 3; pass++) {
    const down = pass !== 1;
    const seq = down ? layers.slice(1) : layers.slice(0, -1).reverse();
    for (const l of seq) {
      const keyed = l.map((id, i) => ({ id, k: bary(down ? parents(id) : children.get(id) ?? [], i) }));
      keyed.sort((a, b) => a.k - b.k || a.id - b.id);
      l.splice(0, l.length, ...keyed.map((x) => x.id));
      setOrder();
    }
  }
  return layers;
}

export const QBOX_HEAD = 58;

export function flowLayout(m: GModel, filters: FilterKey[], around?: FlowAround): FlowLayout {
  const F = FLOW;
  // one query in focus: draw it as a box around its jobs, with what came before on the left and after on the right
  const q = m.queries.length === 1 && !m.app && m.jobs.length > 0 && !m.connects.length ? m.queries[0] : null;
  const before = q ? around?.before ?? [] : [];
  const after = q ? around?.after ?? [] : [];
  const colW = F.callerW + F.colGap + 20;
  const depthOf = (c: FlowCtx) => Math.max(1, c.depth ?? 1);
  const depthB = Math.max(0, ...before.map(depthOf));
  const depthA = Math.max(0, ...after.map(depthOf));
  const fold = q ? around?.fold ?? null : null;
  // room for the cards, or just for the "+" on the box edge when that side is folded
  const leftW = before.length ? depthB * colW : fold?.before ? 44 : 0;
  const hideJobs = !!q && !!around?.collapsed;
  const pos = new Map<string, Rect>();
  const boxes: FlowBox[] = [];
  const dimmed = new Set<string>();
  const active = filters.length > 0;

  // in-job dependencies by stage id
  const parentsOf = new Map<number, Set<number>>();
  for (const e of m.edges) {
    if (e.kind !== 'depends') continue;
    const s = m.byId.get(e.source);
    const t = m.byId.get(e.target);
    if (s?.stageId === null || s?.stageId === undefined || t?.stageId === null || t?.stageId === undefined) continue;
    const set = parentsOf.get(t.stageId) ?? new Set<number>();
    set.add(s.stageId);
    parentsOf.set(t.stageId, set);
  }

  // callers (Connect statements, then queries) per job
  const callersOf = new Map<string, GNode[]>();
  for (const e of m.edges) {
    if (e.kind !== 'runs_query' && e.kind !== 'from_connect') continue;
    const src = m.byId.get(e.source);
    if (!src) continue;
    callersOf.set(e.target, [...(callersOf.get(e.target) ?? []), src]);
  }
  const hasCallers = !q && m.queries.length + m.connects.length > 0;
  const callerX = F.spineX + 26;
  const qx = F.spineX + leftW;
  const boxX = q ? qx + F.pad : hasCallers ? callerX + F.callerW + F.colGap : F.spineX + 26;

  let y = q ? F.top + QBOX_HEAD : F.top;
  if (m.app) {
    pos.set(m.app.id, { x: 0, y, w: F.appW, h: F.appH });
    y += F.appH + 28;
  }
  const spineTicks: number[] = [];
  let callerY = y;
  const placed = new Set<string>(q ? [q.id] : []);
  let width = F.appW;

  const groups: { id: string; kind: 'job' | 'orphans'; node: GNode | null; stages: GNode[]; label: string }[] = (hideJobs ? [] : m.jobs).map((j) => ({
    id: j.id,
    kind: 'job',
    node: j,
    stages: m.stagesByJob.get(j.id) ?? [],
    label: j.label,
  }));
  if (m.orphans.length && !hideJobs) groups.push({ id: 'orphans', kind: 'orphans', node: null, stages: m.orphans, label: 'Stages without a Spark job' });

  // one query in focus: its jobs side by side, left to right, so the box reads across rather than down
  let jx = boxX;
  let rowBottom = y;
  for (const g of groups) {
    const shown = g.stages.filter((s) => matches(s, filters));
    g.stages.forEach((s) => !matches(s, filters) && dimmed.add(s.id));
    const jobMatches = g.node ? matches(g.node, filters) : false;
    const collapsed = active && shown.length === 0 && !jobMatches;
    if (g.node && active && !jobMatches && !shown.length) dimmed.add(g.node.id);

    // callers attached to this job, placed level with its box
    const top = q ? F.top + QBOX_HEAD : Math.max(y, callerY);
    const bx = q ? jx : boxX;
    let cy = top;
    for (const c of g.node ? callersOf.get(g.node.id) ?? [] : []) {
      if (placed.has(c.id)) continue;
      placed.add(c.id);
      pos.set(c.id, { x: callerX, y: cy, w: F.callerW, h: F.callerH });
      if (active && !matches(c, filters)) dimmed.add(c.id);
      cy += F.callerH + F.callerGap;
    }
    callerY = cy;

    let w = 340;
    let h = F.head;
    if (!collapsed) {
      const layers = layerJob(g.stages, parentsOf);
      const cellsByStage = new Map<number, GNode[]>();
      g.stages.forEach((s) => cellsByStage.set(s.stageId ?? -1, [...(cellsByStage.get(s.stageId ?? -1) ?? []), s]));
      const cellH = (id: number) => {
        const n = cellsByStage.get(id)?.length ?? 1;
        return n * F.stageH + (n - 1) * F.retryGap;
      };
      const layerH = layers.map((l) => l.reduce((a, id) => a + cellH(id), 0) + Math.max(0, l.length - 1) * F.gapY);
      const inner = Math.max(0, ...layerH);
      layers.forEach((l, li) => {
        let ly = top + F.head + F.pad + (inner - layerH[li]) / 2;
        const lx = bx + F.pad + li * (F.stageW + F.gapX);
        for (const id of l) {
          const cell = (cellsByStage.get(id) ?? []).sort((a, b) => a.attempt - b.attempt);
          cell.forEach((s, ai) => pos.set(s.id, { x: lx, y: ly + ai * (F.stageH + F.retryGap), w: F.stageW, h: F.stageH }));
          ly += cellH(id) + F.gapY;
        }
      });
      if (g.stages.length) {
        w = Math.max(w, F.pad * 2 + layers.length * F.stageW + (layers.length - 1) * F.gapX);
        h = F.head + F.pad * 2 + inner;
      } else h = F.head + 30;
    }
    const box: FlowBox = { id: g.id, kind: g.kind, node: g.node, x: bx, y: top, w, h, stages: g.stages.length, hidden: g.stages.length - shown.length, collapsed, label: g.label };
    boxes.push(box);
    if (g.node) pos.set(g.node.id, { x: bx, y: top, w, h });
    spineTicks.push(top + 22);
    width = Math.max(width, bx + w);
    if (q) {
      jx = bx + w + F.jobGap;
      rowBottom = Math.max(rowBottom, top + h);
      y = rowBottom + F.jobGap;
      continue;
    }
    y = Math.max(top + h, callerY - F.callerGap) + F.jobGap;
    callerY = Math.max(callerY, top);
  }
  let container: FlowLayout['container'] = null;
  const ctx: FlowLayout['ctx'] = [];
  if (q) {
    const right = Math.max(width, qx + 340) + F.pad;
    container = { node: q, x: qx, y: F.top, w: right - qx, h: hideJobs ? QBOX_HEAD + 6 : y - F.jobGap - F.top + F.pad, collapsed: hideJobs, jobs: m.jobs.length };
    pos.set(q.id, { x: container.x, y: container.y, w: container.w, h: container.h });
    // one column per step away from the query: nearest next to the box, further ones outward
    const card = (side: 'before' | 'after', list: FlowCtx[], xOf: (d: number) => number) => {
      const rows = new Map<number, number>();
      for (const c of list) {
        const d = depthOf(c);
        const i = rows.get(d) ?? 0;
        rows.set(d, i + 1);
        ctx.push({ c, side, r: { x: xOf(d), y: F.top + i * (F.callerH + F.callerGap), w: F.callerW, h: F.callerH } });
      }
    };
    const afterX = container.x + container.w + F.colGap + 12;
    card('before', before, (d) => (depthB - d) * colW);
    card('after', after, (d) => afterX + (d - 1) * colW);
    width = Math.max(width, container.x + container.w + (after.length ? F.colGap + 12 + F.callerW + (depthA - 1) * colW : fold?.after ? 44 : 0));
    y = Math.max(y, container.y + container.h, ...ctx.map((c) => c.r.y + c.r.h));
  }
  // callers that run no job in this context
  for (const c of [...m.connects, ...m.queries]) {
    if (placed.has(c.id)) continue;
    const yy = Math.max(y, callerY);
    pos.set(c.id, { x: callerX, y: yy, w: F.callerW, h: F.callerH });
    if (active && !matches(c, filters)) dimmed.add(c.id);
    callerY = yy + F.callerH + F.callerGap;
    y = callerY;
  }
  return {
    container,
    fold,
    ctx,
    pos,
    boxes,
    spine: m.app && spineTicks.length ? { x: F.spineX, y0: F.top + F.appH, ticks: spineTicks } : null,
    width: Math.max(width, q ? 0 : callerX + F.callerW) + 24,
    height: Math.max(y, callerY) + 24,
    dimmed,
  };
}

/* ------------------------------------------------------------------ time */

export const TIME = {
  label: 210,
  rowH: 26,
  barH: 18,
  jobRowH: 30,
  head: 26,
  axis: 30,
};

export interface TimeRow {
  y: number;
  h: number;
  kind: 'app' | 'section' | 'job' | 'lane';
  label: string;
  sub?: string;
  node?: GNode;
  dim?: boolean;
}

export interface TimeItem {
  node: GNode;
  y: number;
  h: number;
  s: number;
  e: number;
  /** start of the next bar in the same lane (room for an outside label) */
  next: number;
}

export interface TimeLayout {
  rows: TimeRow[];
  items: Map<string, TimeItem>;
  t0: number;
  t1: number;
  height: number;
  dimmed: Set<string>;
}

function packRows(nodes: GNode[], close: (n: GNode) => number): GNode[][] {
  const rows: GNode[][] = [];
  const ends: number[] = [];
  const sorted = [...nodes].filter((n) => n.start !== null && n.start !== undefined).sort((a, b) => a.start! - b.start! || (a.stageId ?? 0) - (b.stageId ?? 0));
  for (const n of sorted) {
    let r = ends.findIndex((e) => e <= n.start!);
    if (r < 0) {
      r = rows.length;
      rows.push([]);
      ends.push(0);
    }
    rows[r].push(n);
    ends[r] = Math.max(ends[r], close(n), n.start! + 1);
  }
  return rows;
}

export function timeLayout(m: GModel, filters: FilterKey[]): TimeLayout {
  const T = TIME;
  let t0 = m.start ?? Infinity;
  let t1 = m.end ?? -Infinity;
  for (const n of m.nodes) {
    if (n.start !== null && n.start !== undefined) {
      t0 = Math.min(t0, n.start);
      t1 = Math.max(t1, n.start);
    }
    if (n.end !== null && n.end !== undefined) t1 = Math.max(t1, n.end);
  }
  if (!Number.isFinite(t0) || !Number.isFinite(t1)) {
    t0 = 0;
    t1 = 1000;
  }
  if (t1 <= t0) t1 = t0 + 1000;
  const close = (n: GNode) => Math.max(n.end ?? t1, n.start ?? t0);
  const rows: TimeRow[] = [];
  const items = new Map<string, TimeItem>();
  const dimmed = new Set<string>();
  const active = filters.length > 0;
  let y = 0;
  const place = (n: GNode, row: number, h: number, next = Infinity) => {
    if (n.start === null || n.start === undefined) return;
    items.set(n.id, { node: n, y: row + (h - T.barH) / 2, h: T.barH, s: n.start, e: close(n), next });
  };
  const placeLane = (lane: GNode[], row: number) => lane.forEach((n, i) => place(n, row, T.rowH, lane[i + 1]?.start ?? Infinity));

  if (m.app) {
    rows.push({ y, h: T.jobRowH, kind: 'app', label: m.app.label, sub: 'application', node: m.app });
    place(m.app, y, T.jobRowH);
    y += T.jobRowH;
  }
  const callerSection = (label: string, nodes: GNode[]) => {
    if (!nodes.length) return;
    rows.push({ y, h: T.head, kind: 'section', label });
    y += T.head;
    for (const lane of packRows(nodes, close)) {
      rows.push({ y, h: T.rowH, kind: 'lane', label: '' });
      placeLane(lane, y);
      lane.forEach((n) => active && !matches(n, filters) && dimmed.add(n.id));
      y += T.rowH;
    }
  };
  callerSection(`Connect statements (${m.connects.length})`, m.connects);
  callerSection(`SQL queries (${m.queries.length})`, m.queries);

  const groups: { node: GNode | null; stages: GNode[]; label: string }[] = m.jobs.map((j) => ({ node: j, stages: m.stagesByJob.get(j.id) ?? [], label: j.label }));
  if (m.orphans.length) groups.push({ node: null, stages: m.orphans, label: 'Stages without a job' });
  rows.push({ y, h: T.head, kind: 'section', label: `Jobs and stages (${m.jobs.length} jobs)` });
  y += T.head;
  for (const g of groups) {
    const shown = g.stages.filter((s) => matches(s, filters));
    g.stages.forEach((s) => !matches(s, filters) && dimmed.add(s.id));
    const jobMatch = g.node ? matches(g.node, filters) : false;
    const collapsed = active && !shown.length && !jobMatch;
    if (g.node && collapsed) dimmed.add(g.node.id);
    rows.push({ y, h: T.jobRowH, kind: 'job', label: g.label, sub: g.node?.sublabel ?? undefined, node: g.node ?? undefined, dim: collapsed });
    if (g.node) place(g.node, y, T.jobRowH);
    y += T.jobRowH;
    if (collapsed) continue;
    for (const lane of packRows(g.stages, close)) {
      rows.push({ y, h: T.rowH, kind: 'lane', label: '' });
      placeLane(lane, y);
      y += T.rowH;
    }
  }
  return { rows, items, t0, t1, height: y + 12, dimmed };
}
