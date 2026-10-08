import { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, optional, type GanttTask, type IncidentRow } from '../api';
import { STATUS_META, type GNode, type WorkUnit } from '../graph/model';
import { fmtBytes, fmtDuration, fmtNum, fmtTs, tickLabel, timeTicks, truncate } from '../format';
import { useAsync } from '../hooks';
import { to } from '../links';
import { indexProblems } from '../problems';
import { Async } from './ui';

type Row = {
  key: string;
  depth: number;
  label: string;
  sub?: string | null;
  start: number | null;
  end: number | null;
  color: string;
  kids?: boolean;
  open?: boolean;
  stage?: GNode;
  task?: GanttTask;
  probs?: IncidentRow[];
  /** the stage's other tasks, clubbed into one row: their typical duration; clicking opens the stage's task table */
  rest?: { n: number; p50: number | null; stageId: string };
};

const TASKS_PER_STAGE = 5;
const plural = (n: number, w: string) => `${fmtNum(n)} ${w}${n === 1 ? '' : 's'}`;
const took = (t: GanttTask) => (t.start !== null && t.end !== null ? t.end - t.start : null);

/** One task's stand-out numbers, for its row: where it ran and anything heavy it did. */
function taskFacts(t: GanttTask): string {
  const f = [`executor ${t.executor_id ?? '?'}`];
  if (t.failed) f.push('failed');
  if (t.disk_spill) f.push(`spilled ${fmtBytes(t.disk_spill)}`);
  if (t.shuffle_read) f.push(`shuffle read ${fmtBytes(t.shuffle_read)}`);
  if (t.input_bytes) f.push(`read ${fmtBytes(t.input_bytes)}`);
  if (t.gc_ms && t.gc_ms >= 1000) f.push(`GC ${fmtDuration(t.gc_ms)}`);
  return f.join(' · ');
}

/**
 * A query or job as a trace, the way APM tools draw a request: the query span, then each Spark job, then each stage
 * attempt, then its notable tasks (every failed one, then the slowest), all on one clock.
 */
export function TraceWaterfall({ cid, ctx, unit, stagesByJob, onStage }: { cid: string; ctx: string; unit: WorkUnit; stagesByJob: Map<string, GNode[]>; onStage: (id: string) => void }) {
  const st = useAsync(
    async (s) => {
      const [g, inc] = await Promise.all([optional(api.gantt(cid, ctx, 20000, s)), api.datasetOpt<IncidentRow>(cid, 'incidents', { limit: 5000 }, s)]);
      return { tasks: g?.tasks ?? [], sampled: !!g?.sampled, inc: inc?.rows ?? [] };
    },
    [cid, ctx],
  );
  return (
    <div className="panel">
      <div className="panel-head">
        <div>
          <h3>Trace</h3>
          <p className="note">
            Read it like a request trace: each row runs inside the one above it. Long bars are where the time went, red ones failed. Under each stage are its 5 tasks
            that matter most (failed first, then the slowest) and one row for all the others; click that row to open the stage's full task table.
          </p>
        </div>
      </div>
      <Async state={st} label="Loading tasks…">
        {(d) => <Waterfall cid={cid} ctx={ctx} unit={unit} stagesByJob={stagesByJob} tasks={d.tasks} sampled={d.sampled} inc={d.inc} onStage={onStage} />}
      </Async>
    </div>
  );
}

function Waterfall({
  cid, ctx, unit, stagesByJob, tasks, sampled, inc, onStage,
}: { cid: string; ctx: string; unit: WorkUnit; stagesByJob: Map<string, GNode[]>; tasks: GanttTask[]; sampled: boolean; inc: IncidentRow[]; onStage: (id: string) => void }) {
  const [toggled, setToggled] = useState<Set<string>>(new Set());
  const flip = (k: string) => setToggled((s) => { const n = new Set(s); if (n.has(k)) n.delete(k); else n.add(k); return n; });

  const tasksBy = useMemo(() => {
    const m = new Map<string, GanttTask[]>();
    for (const t of tasks) {
      const k = `${t.stage_id}.${t.stage_attempt}`;
      const a = m.get(k) ?? [];
      a.push(t);
      m.set(k, a);
    }
    return m;
  }, [tasks]);
  const probsBy = useMemo(
    () => indexProblems(inc.filter((r) => !r.spark_context_id || r.spark_context_id === ctx), (r) => (r.stages ? r.stages.split(', ') : [])),
    [inc, ctx],
  );

  const rows: Row[] = [];
  const color = (n: GNode) => STATUS_META[n.vstatus].mark;
  // a job unit's node is its one job: draw it once, as the job row
  const ownRow = !!unit.node && unit.kind !== 'job';
  const base = ownRow ? 1 : 0;
  if (ownRow && unit.node) rows.push({ key: unit.node.id, depth: 0, label: unit.title, sub: unit.text, start: unit.start, end: unit.end, color: STATUS_META[unit.vstatus].mark });
  const jobs = unit.kind === 'orphans' ? [] : [...unit.jobs].sort((a, b) => (a.start ?? 0) - (b.start ?? 0));
  const stageRows = (stages: GNode[], depth: number) => {
    for (const s of [...stages].sort((a, b) => (a.start ?? 0) - (b.start ?? 0) || (a.stageId ?? 0) - (b.stageId ?? 0))) {
      const k = `${s.stageId}.${s.attempt}`;
      const ts = tasksBy.get(k) ?? [];
      // stages that failed or were retried open by default: that is where the story is
      const def = s.vstatus === 'failed' || s.vstatus === 'retried' || ts.some((t) => t.failed);
      const open = toggled.has(s.id) ? !def : def;
      rows.push({
        key: s.id, depth, label: `Stage ${s.stageId}${s.attempt ? `.${s.attempt}` : ''}`, sub: s.sublabel ?? s.label, start: s.start ?? null, end: s.end ?? null,
        color: color(s), kids: ts.length > 0, open, stage: s, probs: probsBy.get(k) ?? (s.attempt === 0 ? probsBy.get(String(s.stageId)) : undefined),
      });
      if (!open) continue;
      // the 5 that matter most: failed ones first, then the slowest; every other task is one clubbed row
      const failed = ts.filter((t) => t.failed).sort((a, b) => (a.start ?? 0) - (b.start ?? 0));
      const slow = ts.filter((t) => !t.failed && took(t) !== null).sort((a, b) => took(b)! - took(a)!);
      const top = [...failed, ...slow].slice(0, TASKS_PER_STAGE);
      const shown = new Set(top);
      for (const t of [...top].sort((a, b) => (a.start ?? 0) - (b.start ?? 0)))
        rows.push({
          key: `${s.id}:t${t.task_id}`, depth: depth + 1, label: `Task ${t.task_id}`, sub: taskFacts(t),
          start: t.start, end: t.end, color: t.failed ? 'var(--st-crit)' : 'var(--series-1)', task: t,
        });
      const others = ts.filter((t) => !shown.has(t));
      if (others.length) {
        const ds = others.map(took).filter((x): x is number => x !== null).sort((a, b) => a - b);
        const starts = others.map((t) => t.start).filter((x): x is number => x !== null);
        const ends = others.map((t) => t.end).filter((x): x is number => x !== null);
        const execs = new Set(others.map((t) => t.executor_id)).size;
        const otherFailed = others.filter((t) => t.failed).length;
        rows.push({
          key: `${s.id}:more`, depth: depth + 1, label: plural(others.length, top.length ? 'other task' : 'task'),
          sub: [
            ds.length ? `median ${fmtDuration(ds[Math.floor(ds.length / 2)])}, slowest ${fmtDuration(ds[ds.length - 1])}` : null,
            `on ${plural(execs, 'executor')}`,
            otherFailed ? `${fmtNum(otherFailed)} more failed` : null,
            sampled ? 'from a sample of the tasks' : null,
          ].filter(Boolean).join(' · '),
          start: starts.length ? Math.min(...starts) : null, end: ends.length ? Math.max(...ends) : null, color: 'var(--series-1)',
          rest: { n: others.length, p50: ds.length ? ds[Math.floor(ds.length / 2)] : null, stageId: s.id },
        });
      }
    }
  };
  if (unit.kind === 'orphans') stageRows(unit.stages, base);
  for (const j of jobs) {
    const open = !toggled.has(j.id);
    const stages = stagesByJob.get(j.id) ?? [];
    rows.push({ key: j.id, depth: base, label: `Job ${j.jobId}`, sub: j.sublabel ?? null, start: j.start ?? null, end: j.end ?? null, color: color(j), kids: stages.length > 0, open });
    if (open) stageRows(stages, base + 1);
  }

  const pts = rows.flatMap((r) => [r.start, r.end]).filter((x): x is number => x !== null);
  if (!pts.length) return <p className="panel-body muted">No timings were recorded for this {unit.kind}.</p>;
  const t0 = Math.min(...pts);
  const t1 = Math.max(...pts, t0 + 1);
  const span = t1 - t0;
  const pct = (t: number) => ((t - t0) / span) * 100;
  const ticks = timeTicks(t0, t1, 6);
  const total = unit.end !== null && unit.start !== null ? unit.end - unit.start : span;

  return (
    <div className="trace">
      <div className="trace-row trace-axis">
        <span className="trace-name muted small">Span</span>
        <span className="trace-track">
          {ticks.map((t) => (
            <span key={t} className="trace-tick" style={{ left: `${pct(t)}%` }}>
              {tickLabel(t, span)}
            </span>
          ))}
        </span>
        <span className="trace-dur muted small" title="Duration, and its share of the whole query or job">
          Duration, share
        </span>
      </div>
      {rows.map((r) => {
        const d = r.start !== null && r.end !== null ? r.end - r.start : null;
        const share = d !== null && total > 0 && r.depth > 0 && !r.task && !r.rest ? d / total : null;
        const target = r.stage?.id ?? r.rest?.stageId ?? null;
        return (
          <div
            key={r.key}
            className={`trace-row ${r.task ? 'task' : ''} ${r.rest ? 'task rest' : ''} ${target ? 'clickable' : ''}`}
            onClick={target ? () => onStage(target) : undefined}
            role={target ? 'button' : undefined}
            tabIndex={target ? 0 : undefined}
            aria-label={target ? (r.rest ? `Open the stage's task table: ${r.label}` : `Open ${r.label}`) : undefined}
            title={r.rest ? 'Open the stage to see every task, sortable, with spill, shuffle, GC and memory' : undefined}
            onKeyDown={target ? (e) => (e.key === 'Enter' || e.key === ' ') && (e.preventDefault(), onStage(target)) : undefined}
          >
            <span className="trace-name" style={{ paddingLeft: r.depth * 16 }}>
              {r.kids ? (
                <button
                  className="trace-toggle"
                  aria-label={`${r.open ? 'Collapse' : 'Expand'} ${r.label}`}
                  aria-expanded={!!r.open}
                  onClick={(e) => {
                    e.stopPropagation();
                    flip(r.key);
                  }}
                >
                  {r.open ? '▾' : '▸'}
                </button>
              ) : (
                <span className="trace-toggle" />
              )}
              <span className={r.rest ? 'trace-label trace-rest-label' : 'trace-label'}>{r.label}</span>
              {r.probs?.map((p) => (
                <Link
                  key={p.problem_id}
                  to={to.findings(cid, p.finding_id)}
                  onClick={(e) => e.stopPropagation()}
                  className={`stage-prob sev-${p.incident_severity} ${p.role === 'root' ? 'root' : ''}`}
                  title={`${p.incident_id}: ${p.incident_title}`}
                >
                  {p.role === 'root' ? '● ' : ''}
                  {p.kind}
                </Link>
              ))}
              {r.sub && <span className="trace-sub muted small" title={r.sub}>{truncate(r.sub, 70)}</span>}
            </span>
            <span className="trace-track">
              {ticks.map((t) => (
                <span key={t} className="trace-grid" style={{ left: `${pct(t)}%` }} />
              ))}
              {r.start !== null && (
                <span
                  className={r.rest ? 'trace-bar range' : 'trace-bar'}
                  style={{ left: `${pct(r.start)}%`, width: `max(2px, ${pct(r.end ?? r.start) - pct(r.start)}%)`, background: r.color }}
                  title={
                    r.rest
                      ? `${r.label} ran between ${fmtTs(r.start)} and ${r.end ? fmtTs(r.end) : '?'}`
                      : `${r.label}: ${fmtTs(r.start)} → ${r.end ? fmtTs(r.end) : '?'}${d !== null ? ` (${fmtDuration(d)})` : ''}`
                  }
                />
              )}
            </span>
            <span className="trace-dur small">
              {r.rest ? (r.rest.p50 !== null ? <span title="Median duration of these tasks">~{fmtDuration(r.rest.p50)} each</span> : '') : d !== null ? fmtDuration(d) : ''}
              {share !== null && share >= 0.01 && <span className="muted"> {Math.round(share * 100)}%</span>}
            </span>
          </div>
        );
      })}
      {sampled && <p className="muted small" style={{ padding: '6px 12px' }}>Tasks are a sample (failed and slow tasks are kept).</p>}
    </div>
  );
}
