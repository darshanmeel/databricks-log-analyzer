// Revision 13: the cluster level. Only which job, task or app ran when (runs of the same code share a block, each run a
// bar; overlapping runs stack), the executors, and per minute the CPU in use and the disk spill. Everything else is
// per run: pick one (a bar, a table row, or the picker on the left) and every page shows that run end to end.
// Revision 14: "my job failed at 1 am": type a time or click the chart, and a cursor shows what ran then, how busy the
// executors were, and the runs table narrows to it. Stretches where executors were up but idle are shaded.
import { useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api, type Causes, type ComputeUse, type ExecutorProfileRow, type RunRow } from '../api';
import { TopFinder } from '../components/TopFinder';
import { ClusterTop } from '../components/ClusterTop';
import { bandsLine } from '../components/TaskSizes';
import { useCluster, useRunScopeCtx } from '../components/Shell';
import { Async, Panel, StatusBadge } from '../components/ui';
import { fmtBytes, fmtDuration, fmtNum, fmtPct, fmtTime, tickLabel, timeTicks } from '../format';
import { useAsync, useWidth } from '../hooks';
import { runGroup, runId, runName, usualX } from '../runName';

interface CvExecutor {
  spark_context_id: string; executor_id: string; host: string | null; cores: number | null; added_time: number | null;
  removed_time: number | null; removed_reason: string | null; removal_category: string | null; heap_mb: number | null;
  unified_memory: number | null; tasks: number | null; disk_spill: number | null; idle_ms: number | null; lifetime_ms: number | null;
  /** the heap size came from the JVM's GC lines (the largest heap it reached), not from the event log */
  heap_from_gc?: boolean;
}
interface CvMinute { minute: number; run_ms: number | null; disk_spill: number | null; mem_spill: number | null; tasks: number | null; executors_busy: number; cores: number | null; cpu_share: number | null }
/** Heap in use after GC per minute: all executors, the fullest one, and the driver (shares of their heap). */
interface CvMem { minute: number; used_mb: number; up_mb: number; share: number | null; max_share: number | null; max_exec: string | null; driver_share: number | null }
interface CvApp { spark_context_id: string; app_id: string | null; app_name: string | null; start_time: number | null; end_time: number | null }
interface CvJob { cluster_name: string | null; workload_type: string | null; databricks_job_id: string | null; job_run_id: string | null; parent_run_id: string | null }
interface CvBusy { spark_context_id: string; executor_id: string; busy_start: number; busy_end: number }
interface ClusterViewData {
  job?: CvJob; apps?: CvApp[]; busy?: CvBusy[]; runs: RunRow[]; executors: CvExecutor[]; minutes: CvMinute[]; start: number | null; end: number | null;
  compute?: ComputeUse | null;
  memory?: CvMem[];
  causes?: Causes | null;
  run_causes?: Record<string, Causes | null>;
}

/** "13:05" -> the UTC time on the runs' day (after midnight: the next day); null when not a time. */
export function parseAt(text: string, runs: RunRow[]): number | null {
  const m = text.trim().match(/^(\d{1,2}):(\d{2})$/);
  const first = runs.find((r) => r.start_time !== null)?.start_time;
  if (!m || !first) return null;
  const d = new Date(first);
  let t = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), Number(m[1]), Number(m[2]));
  if (t < first - 12 * 3_600_000) t += 86_400_000;
  return t;
}
const hhmm = (t: number) => new Date(t).toISOString().slice(11, 16);
const runningAt = (r: RunRow, t: number) => (r.start_time ?? Infinity) <= t + 59_999 && (r.end_time ?? r.start_time ?? 0) >= t;

const LABEL_W = 210;
const LANE = 13;
const GAP = 3;
const BAD = /lost|oom|killed|fail|memory/i;

const runTone = (r: RunRow) => (r.status === 'failed' ? 'var(--st-crit)' : r.status === 'incomplete' ? 'var(--st-warn)' : 'var(--series-1)');

/** Overlapping runs of one group get their own lanes (first free lane, in start order). */
function packLanes(rs: RunRow[]): RunRow[][] {
  const lanes: { end: number; runs: RunRow[] }[] = [];
  for (const r of [...rs].sort((a, b) => (a.start_time ?? 0) - (b.start_time ?? 0))) {
    const s = r.start_time ?? 0;
    const lane = lanes.find((l) => l.end <= s);
    if (lane) {
      lane.runs.push(r);
      lane.end = r.end_time ?? s;
    } else lanes.push({ end: r.end_time ?? s, runs: [r] });
  }
  return lanes.map((l) => l.runs);
}

function useOpenRun() {
  const { choose } = useRunScopeCtx();
  const { cid } = useCluster();
  const nav = useNavigate();
  return (r: RunRow, page?: string) => {
    choose(r.run_key);
    if (page) nav(`/c/${encodeURIComponent(cid)}/${page}`);
  };
}

export function ClusterView() {
  const { cid } = useCluster();
  const st = useAsync((s) => api.clusterView<ClusterViewData>(cid, s), [cid]);
  return (
    <div className="page">
      <Async state={st}>{(d) => <ClusterBody d={d} />}</Async>
    </div>
  );
}

function ClusterBody({ d }: { d: ClusterViewData }) {
  // failed stages and waiting / processing time come with /runs (the run scope); add them to this page's rows
  const scoped = useRunScopeCtx().runs;
  const runs = useMemo(() => {
    const by = new Map(scoped.map((r) => [r.run_key, r]));
    return d.runs.filter((r) => r.start_time !== null).map((r) => {
      const x = by.get(r.run_key);
      return x ? { ...r, failed_stages: x.failed_stages, waiting_ms: x.waiting_ms, running_ms: x.running_ms, max_task_input: x.max_task_input, max_task_shuffle: x.max_task_shuffle } : r;
    });
  }, [d.runs, scoped]);
  const groups = useMemo(() => {
    // per app (Spark context), then per program
    const m = new Map<string, RunRow[]>();
    for (const r of runs) {
      const k = `${r.spark_context_id}|${runGroup(r)}`;
      m.set(k, [...(m.get(k) ?? []), r]);
    }
    return [...m].map(([k, rs]) => ({ ctx: k.split('|')[0], g: runGroup(rs[0]), rs, lanes: packLanes(rs), usual: rs.find((r) => r.typical_duration_ms)?.typical_duration_ms ?? null }));
  }, [runs]);
  const { cid } = useCluster();
  const [atText, setAtText] = useState('');
  const at = parseAt(atText, runs);
  const cu = d.compute;
  const open = useOpenRun();
  return (
    <div className="stack" style={{ gap: 16 }}>
      <ClusterTop cid={cid} runs={runs} executors={d.executors} compute={cu ?? null} onPick={(r) => open(r)}
        find={<TopFinder cid={cid} runs={runs} />} causes={d.causes} runCauses={d.run_causes}
        atBox={<>
          <div className="row" style={{ gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
            <label className="small">
              <b>What ran at (UTC)</b>{' '}
              <input className="input" style={{ width: 80 }} placeholder="01:00" value={atText} onChange={(e) => setAtText(e.target.value)} />
            </label>
            <span className="small muted">or click a time in the chart under Details</span>
            {atText && <button className="btn small ghost" onClick={() => setAtText('')}>Clear</button>}
          </div>
          {at !== null && <AtTime d={d} runs={runs} at={at} />}
        </>}
        more={<>
          {(() => {
            // Databricks jobs behind the runs: job (name) -> job run -> task runs
            const jobs = new Map<string, { name: string | null; id: string | null; parent: Set<string>; n: number }>();
            for (const r of runs.filter((x) => x.databricks_job_id)) {
              const k = r.databricks_job_id!;
              const j = jobs.get(k) ?? { name: r.job_name ?? null, id: k, parent: new Set<string>(), n: 0 };
              const p = r.parent_run_id ?? (d.job?.databricks_job_id === k ? d.job?.parent_run_id : null);
              if (p) j.parent.add(p);
              j.n += 1;
              jobs.set(k, j);
            }
            if (!jobs.size) return null;
            return (
              <p style={{ margin: 0 }}>
                {d.job?.workload_type === 'AUTOMATED' || d.job?.cluster_name?.startsWith('job-') ? 'A job cluster. ' : ''}
                {[...jobs.values()].map((j, i) => (
                  <span key={j.id}>
                    {i ? '; ' : ''}Databricks job <b>{j.name ?? j.id}</b>{j.name ? <span className="muted"> ({j.id})</span> : null}
                    {j.parent.size ? <>, {j.parent.size === 1 ? 'job run' : 'job runs'} <b>{[...j.parent].join(', ')}</b></> : null}: {j.n} task runs
                  </span>
                ))}
                . A task run is one task of the job run (or one iteration of a for-each task); they are grouped below by the code they ran.
                Task names are not in the logs, so each is named by its notebook or main class and the table it worked on.
              </p>
            );
          })()}
          <ExecutorsBusy cid={cid} compute={cu ?? null} heaps={d.executors} />
          <Panel title="Apps, runs, executors, CPU and spill" note="Per Spark app: its lifetime, one block per program it ran with a bar per run, and its executors (red: failed, amber outline: took at least twice its usual time). Hover for details, click a run to open its story, click anywhere else to see what ran at that time.">
            <ClusterChart d={d} groups={groups} at={at} onAt={(t) => setAtText(hhmm(t))} />
          </Panel>
          <div id="cv-runs"><Panel title="All runs" note="Click a row to open that run. Read per task is the storage input plus shuffle read of each task that read data; hover Biggest for the size bands.">
            <RunsTable runs={runs} atText={atText} onAtText={setAtText} />
          </Panel></div>
        </>} />
    </div>
  );
}

/** What ran at one minute: the runs (failed first), the executors up and busy, CPU and spill. */
function AtTime({ d, runs, at }: { d: ClusterViewData; runs: RunRow[]; at: number }) {
  const open = useOpenRun();
  const minute = Math.floor(at / 60_000) * 60_000;
  const m = d.minutes.find((x) => x.minute === minute);
  const live = runs.filter((r) => runningAt(r, at)).sort((a, b) => Number(b.status === 'failed') - Number(a.status === 'failed') || (a.start_time ?? 0) - (b.start_time ?? 0));
  // ended (failed) within 15 minutes before: "it failed at 1 am" is often the end time
  const endedNear = runs.filter((r) => !runningAt(r, at) && r.end_time !== null && Math.abs(r.end_time - at) <= 15 * 60_000);
  const up = d.executors.filter((e) => (e.added_time ?? 0) <= at && (e.removed_time ?? Infinity) >= at);
  const busy = new Set((d.busy ?? []).filter((b) => b.busy_start <= at + 59_999 && b.busy_end >= at).map((b) => `${b.spark_context_id}|${b.executor_id}`));
  const idle = (d.compute?.stretches ?? []).find((x) => x.start <= at && x.end > at);
  const mem = (d.memory ?? []).find((x) => x.minute === minute);
  const chip = (r: RunRow) => (
    <button key={r.run_key} className="btn small" onClick={() => open(r)} title={`${fmtTime(r.start_time)} → ${fmtTime(r.end_time)} UTC`}
      style={{ borderColor: r.status === 'failed' ? 'var(--st-crit)' : undefined }}>
      {r.status === 'failed' ? <b className="st-crit">✕ </b> : null}
      {runName(r)} <span className="muted">{fmtTime(r.start_time).slice(0, 5)}–{fmtTime(r.end_time).slice(0, 5)}</span>
    </button>
  );
  return (
    <div className="panel panel-body stack" style={{ gap: 8 }}>
      <div>
        <b>At {hhmm(at)} UTC</b>: {live.length ? `${live.length} ${live.length === 1 ? 'run was' : 'runs were'} running` : 'no run was running'}
        {live.filter((r) => r.status === 'failed').length ? <>, <b className="st-crit">{live.filter((r) => r.status === 'failed').length} of them failed</b></> : null}
        {' · '}{up.length} executors up, {busy.size} running tasks
        {m?.cpu_share != null ? ` · CPU ${fmtPct(m.cpu_share, 0)}` : ''}
        {mem?.share != null ? (
          <> · memory {fmtPct(mem.share, 0)}
            {mem.max_exec !== null && mem.max_share !== null ? (
              <span className={mem.max_share >= 0.9 ? 'st-crit' : ''}> (fullest exec {mem.max_exec} {fmtPct(mem.max_share, 0)})</span>
            ) : null}
          </>
        ) : null}
        {m?.disk_spill ? <> · <span className="st-warn">{fmtBytes(m.disk_spill)} spilled that minute</span></> : null}
        {idle ? <> · <span className="st-warn">executors idle {hhmm(idle.start)}–{hhmm(idle.end)}</span></> : null}
      </div>
      {live.length > 0 && <div className="row" style={{ gap: 6, flexWrap: 'wrap' }}>{live.slice(0, 40).map(chip)}{live.length > 40 ? <span className="muted small">+{live.length - 40} more in the table below</span> : null}</div>}
      {endedNear.length > 0 && (
        <div className="row small" style={{ gap: 6, flexWrap: 'wrap', alignItems: 'center' }}>
          <span className="muted">Ended within 15 minutes of it:</span>
          {endedNear.slice(0, 20).map(chip)}
        </div>
      )}
    </div>
  );
}

function ClusterChart({ d, groups, at, onAt }: {
  d: ClusterViewData; groups: { ctx: string; g: string; rs: RunRow[]; lanes: RunRow[][]; usual: number | null }[];
  at?: number | null; onAt?: (t: number) => void;
}) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [tip, setTip] = useState<{ x: number; y: number; text: string } | null>(null);
  const open = useOpenRun();
  const { run: cur } = useRunScopeCtx();
  const W = Math.max(640, width || 900);
  const times = [
    ...d.runs.flatMap((r) => [r.start_time, r.end_time]),
    ...(d.apps ?? []).flatMap((a) => [a.start_time, a.end_time]),
    ...d.executors.flatMap((e) => [e.added_time, e.removed_time]),
    ...d.minutes.map((m) => m.minute),
  ].filter((t): t is number => typeof t === 'number');
  if (!times.length) return <p className="muted">No times in this cluster's data.</p>;
  const t0 = Math.min(...times), t1 = Math.max(...times, t0 + 60_000);
  const x = (t: number) => LABEL_W + ((t - t0) / (t1 - t0)) * (W - LABEL_W - 12);
  const ticks = timeTicks(t0, t1, Math.max(3, Math.floor((W - LABEL_W) / 110)));

  type Row = { y: number; h: number; label: string; sub?: string; node: JSX.Element[] };
  const rows: Row[] = [];
  let y = 26;
  const hover = (text: string) => (ev: React.MouseEvent) => {
    const box = (ev.currentTarget as SVGElement).ownerSVGElement!.getBoundingClientRect();
    setTip({ x: ev.clientX - box.left, y: ev.clientY - box.top, text });
  };
  // apps in start order; a context with runs but no app row still gets a block
  const ctxs = [...(d.apps ?? []).map((a) => a.spark_context_id), ...groups.map((g) => g.ctx), ...d.executors.map((e) => e.spark_context_id)]
    .filter((c, i, all) => all.indexOf(c) === i);
  const exRows: Row[] = [];
  const appRows: Row[] = [];
  const exTop: number[] = [];
  let appNo = 0;
  for (const c of ctxs) {
  const app = (d.apps ?? []).find((a) => a.spark_context_id === c);
  const mine = groups.filter((g) => g.ctx === c);
  const myEx = d.executors.filter((e) => e.spark_context_id === c);
  const aS = app?.start_time ?? Math.min(...mine.flatMap((g) => g.rs.map((r) => r.start_time ?? t1)), t1);
  const lastRun = Math.max(...mine.flatMap((g) => g.rs.map((r) => r.end_time ?? 0)), aS);
  const aE = app?.end_time ?? lastRun;
  appNo += 1;
  appRows.push({
    y, h: 18, label: `App ${appNo}${app?.app_id ? ` · ${app.app_id}` : ''}`,
    node: [
      <rect key="a" x={x(aS)} y={y + 4} width={Math.max(2, x(aE) - x(aS))} height={8} rx={4} fill="var(--st-ok)" fillOpacity={0.35}
        onMouseMove={hover(`Spark app ${app?.app_id ?? c}${app?.app_name ? ` (${app.app_name})` : ''}
${fmtTime(aS)} → ${app?.end_time ? fmtTime(app.end_time) : 'no end event'} UTC, ${fmtDuration(aE - aS)}
${mine.reduce((n, g) => n + g.rs.length, 0)} runs, ${myEx.length} executors`)}
        onMouseLeave={() => setTip(null)} />,
    ],
  });
  y += 22;
  for (const g of mine) {
    // many runs in parallel: thinner lanes, so a block stays within about 130 px
    const lh = Math.max(3, Math.min(LANE, Math.floor(130 / g.lanes.length) - 1));
    const gap = lh >= 8 ? GAP : 1;
    const h = Math.max(34, g.lanes.length * (lh + gap) + 6);
    const node: JSX.Element[] = [];
    g.lanes.forEach((lane, li) =>
      lane.forEach((r) => {
        const s = r.start_time ?? t0, e = r.end_time ?? s;
        const slow = (usualX(r) ?? 0) >= 2;
        node.push(
          <rect
            key={r.run_key}
            x={x(s)}
            y={y + 3 + li * (lh + gap)}
            width={Math.max(2, x(e) - x(s))}
            height={lh}
            rx={2}
            fill={runTone(r)}
            fillOpacity={cur && cur !== r.run_key ? 0.35 : 0.85}
            stroke={r.run_key === cur ? 'var(--text)' : slow ? 'var(--st-warn)' : 'none'}
            strokeWidth={r.run_key === cur || slow ? (lh >= 8 ? 2 : 1) : 0}
            style={{ cursor: 'pointer' }}
            onMouseMove={hover(
              `${runName(r)} (${runId(r)})\n${fmtTime(r.start_time)} → ${fmtTime(r.end_time)} UTC, ${fmtDuration(r.duration_ms)}` +
                (r.typical_duration_ms ? `, usually ${fmtDuration(r.typical_duration_ms)}` : '') +
                `\n${r.status}; ${fmtNum(r.tasks)} tasks${r.failed_tasks ? `, ${fmtNum(r.failed_tasks)} failed` : ''}` +
                (r.input_bytes || r.shuffle_read ? `; read ${fmtBytes((r.input_bytes ?? 0) + (r.shuffle_read ?? 0))}` : '') +
                (r.disk_spill ? `; spilled ${fmtBytes(r.disk_spill)}` : ''),
            )}
            onMouseLeave={() => setTip(null)}
            onClick={(ev) => { ev.stopPropagation(); open(r); }}
          />,
        );
      }),
    );
    rows.push({ y, h, label: g.g, sub: `${g.rs.length} runs${g.usual ? ` · usually ${fmtDuration(g.usual)}` : ''}`, node });
    y += h + 4;
  }
  // its executors
  appRows.push({ y: y + 2, h: 14, label: 'Executors', node: [] });
  y += 18;
  exTop.push(y);
  for (const e of myEx) {
    const s = e.added_time ?? t0, en = e.removed_time ?? t1;
    const bad = BAD.test(`${e.removal_category ?? ''} ${e.removed_reason ?? ''}`);
    exRows.push({
      y, h: 12, label: `exec ${e.executor_id}`, sub: e.cores ? `${e.cores} cores` : undefined,
      node: [
        // up the whole time it lived: amber where it ran nothing (paid for, idle), blue where it ran tasks
        <rect key="b" x={x(s)} y={y + 2} width={Math.max(2, x(en) - x(s))} height={8} rx={2} fill="var(--text-3)" fillOpacity={0.18} stroke="var(--line)" strokeWidth={0.6}
          onMouseMove={hover(`exec ${e.executor_id}${e.host ? ` on ${e.host}` : ''}: ${e.cores ?? '?'} cores${e.heap_mb ? `, ${fmtBytes(e.heap_mb * 1048576)} heap` : ''}\n` +
            `${fmtTime(e.added_time)} → ${e.removed_time ? fmtTime(e.removed_time) : 'end'} UTC${e.removed_reason ? ` (${e.removed_reason})` : ''}` +
            (e.idle_ms && e.lifetime_ms ? `\nidle ${fmtDuration(e.idle_ms)} of ${fmtDuration(e.lifetime_ms)}` : ''))}
          onMouseLeave={() => setTip(null)} />,
        ...(d.busy ?? [])
          .filter((b) => b.executor_id === e.executor_id && b.spark_context_id === e.spark_context_id)
          .map((b, i) => (
            <rect key={`u${i}`} x={x(b.busy_start)} y={y + 2} width={Math.max(1, x(b.busy_end) - x(b.busy_start))} height={8} fill="var(--series-1)" pointerEvents="none" />
          )),
        ...(e.removed_time && bad ? [<text key="x" x={x(e.removed_time) + 4} y={y + 10} fontSize={10.5} fill="var(--st-crit)">✕ {e.removal_category ?? 'lost'}</text>] : []),
        ...(e.removed_time ? [<line key="r" x1={x(e.removed_time)} x2={x(e.removed_time)} y1={y} y2={y + 12} stroke={bad ? 'var(--st-crit)' : 'var(--line)'} strokeWidth={2} />] : []),
      ],
    });
    y += 14;
  }
  y += 10;
  }
  // CPU and spill per minute
  const cpuY = y + 24, cpuH = 44;
  const mem = d.memory ?? [];
  const memY = cpuY + cpuH + 18, memH = mem.length ? 44 : 0;
  const spY = memY + (mem.length ? memH + 18 : 0), spH = 34;
  const step = (pts: CvMem[], v: (m: CvMem) => number | null, y0: number, h: number, close: boolean) => {
    const ps = pts.filter((m) => v(m) !== null);
    if (!ps.length) return '';
    const seg = ps.map((m, i) => `${i || close ? 'L' : 'M'}${x(m.minute)},${y0 + h - v(m)! * h} L${x(m.minute + 60_000)},${y0 + h - v(m)! * h}`).join(' ');
    return close ? `M${x(ps[0].minute)},${y0 + h} ${seg} L${x(ps[ps.length - 1].minute + 60_000)},${y0 + h} Z` : seg;
  };
  const peakMem = mem.reduce<CvMem | null>((a, m) => ((m.max_share ?? 0) > (a?.max_share ?? -1) ? m : a), null);
  const maxSp = Math.max(1, ...d.minutes.map((m) => m.disk_spill ?? 0));
  const cpuPts = d.minutes.filter((m) => m.cpu_share !== null);
  const area = cpuPts.length
    ? `M${x(cpuPts[0].minute)},${cpuY + cpuH} ` + cpuPts.map((m) => `L${x(m.minute)},${cpuY + cpuH - (m.cpu_share ?? 0) * cpuH} L${x(m.minute + 60_000)},${cpuY + cpuH - (m.cpu_share ?? 0) * cpuH}`).join(' ') + ` L${x(cpuPts[cpuPts.length - 1].minute + 60_000)},${cpuY + cpuH} Z`
    : '';
  const H = spY + spH + 8;
  const bw = Math.max(1, x(t0 + 60_000) - x(t0) - 0.5);

  return (
    <div ref={ref} style={{ position: 'relative', overflowX: 'auto' }}>
      <svg width={W} height={H} role="img" aria-label="Runs, executors, CPU and spill over time" style={{ display: 'block', cursor: onAt ? 'crosshair' : undefined }}
        onClick={(ev) => {
          if (!onAt) return;
          const box = (ev.currentTarget as SVGSVGElement).getBoundingClientRect();
          const px = ev.clientX - box.left;
          if (px < LABEL_W) return;
          onAt(t0 + ((px - LABEL_W) / (W - LABEL_W - 12)) * (t1 - t0));
        }}>
        {(d.compute?.stretches ?? []).map((sx, i) => (
          <rect key={`idle${i}`} x={x(sx.start)} y={(exTop[0] ?? 26) - 2} width={Math.max(2, x(sx.end) - x(sx.start))} height={Math.max(4, cpuY + cpuH - (exTop[0] ?? 26) + 2)}
            fill="var(--wait)" fillOpacity={0.1} stroke="var(--wait)" strokeOpacity={0.5} strokeDasharray="3 3"
            onMouseMove={hover(`Idle ${fmtTime(sx.start).slice(0, 5)}–${fmtTime(sx.end).slice(0, 5)} UTC (${fmtDuration(sx.end - sx.start)}): ${sx.executors} executors, up to ${sx.cores_max} cores up, tasks used under 10% of them\n${(sx.idle_core_ms / 3_600_000).toFixed(1)} core-hours paid for nothing`)}
            onMouseLeave={() => setTip(null)} />
        ))}
        {ticks.map((t) => (
          <g key={t}>
            <line x1={x(t)} x2={x(t)} y1={18} y2={H} stroke="var(--line)" strokeDasharray="2 3" />
            <text x={x(t)} y={12} fontSize={11} textAnchor="middle" fill="var(--text)" opacity={0.7}>{tickLabel(t, t1 - t0)}</text>
          </g>
        ))}
        <text x={4} y={12} fontSize={11} fill="var(--text)" opacity={0.7}>UTC</text>
        {appRows.map((r, i) => (
          <g key={`a${i}`}>
            <text x={4} y={r.y + 11} fontSize={12} fontWeight={700} fill="var(--text)">{r.label.length > 30 ? `${r.label.slice(0, 29)}…` : r.label}</text>
            {r.node}
          </g>
        ))}
        {[...rows, ...exRows].map((r, i) => (
          <g key={i}>
            <text x={rows.includes(r) ? 14 : 24} y={r.y + Math.min(r.h, 14) - 2} fontSize={12} fill="var(--text)" fontWeight={rows.includes(r) ? 600 : 400}>
              {r.label.length > 26 ? `${r.label.slice(0, 25)}…` : r.label}
            </text>
            {r.sub && rows.includes(r) ? <text x={14} y={r.y + 26} fontSize={11} fill="var(--text)" opacity={0.6}>{r.sub}</text> : null}
            {r.sub && !rows.includes(r) ? <text x={LABEL_W - 8} y={r.y + 10} fontSize={10.5} textAnchor="end" fill="var(--text)" opacity={0.6}>{r.sub}</text> : null}
            {rows.includes(r) ? <line x1={0} x2={W} y1={r.y + r.h + 2} y2={r.y + r.h + 2} stroke="var(--line)" /> : null}
            {r.node}
          </g>
        ))}
        <g transform={`translate(${LABEL_W}, ${cpuY - 12})`} fontSize={10.5} fill="var(--text)">
          <rect x={0} y={-7} width={14} height={7} fill="var(--series-1)" /><text x={18} y={0}>executor ran tasks</text>
          <rect x={130} y={-7} width={14} height={7} fill="var(--text-3)" fillOpacity={0.18} stroke="var(--line)" /><text x={148} y={0}>up but idle (paid for)</text>
          <line x1={285} x2={285} y1={-9} y2={1} stroke="var(--line)" strokeWidth={2} /><text x={290} y={0}>removed</text>
          <line x1={350} x2={350} y1={-9} y2={1} stroke="var(--st-crit)" strokeWidth={2} /><text x={355} y={0} fill="var(--st-crit)">lost, killed or out of memory</text>
        </g>
        <text x={4} y={cpuY + 14} fontSize={12} fontWeight={600} fill="var(--text)">CPU in use</text>
        <text x={4} y={cpuY + 28} fontSize={11} fill="var(--text)" opacity={0.6}>share of the cores up</text>
        <rect x={LABEL_W} y={cpuY} width={W - LABEL_W - 12} height={cpuH} fill="var(--panel-3, transparent)" opacity={0.5} />
        {area && <path d={area} fill="var(--series-1)" fillOpacity={0.55} stroke="var(--series-1)" strokeWidth={1} />}
        {cpuPts.map((m) => (
          <rect key={m.minute} x={x(m.minute)} y={cpuY} width={bw} height={cpuH} fill="transparent"
            onMouseMove={hover(`${fmtTime(m.minute)} UTC: CPU ${fmtPct(m.cpu_share, 0)} of ${m.cores} cores, ${m.executors_busy} executors ran tasks`)} onMouseLeave={() => setTip(null)} />
        ))}
        {mem.length > 0 && (
          <g>
            <text x={4} y={memY + 14} fontSize={12} fontWeight={600} fill="var(--text)">Memory in use</text>
            <text x={4} y={memY + 28} fontSize={11} fill="var(--text)" opacity={0.6}>heap after GC, share of heap</text>
            <rect x={LABEL_W} y={memY} width={W - LABEL_W - 12} height={memH} fill="var(--panel-3, transparent)" opacity={0.5} />
            <line x1={LABEL_W} x2={W - 12} y1={memY + memH * 0.1} y2={memY + memH * 0.1} stroke="var(--st-crit)" strokeOpacity={0.5} strokeDasharray="4 3" />
            <text x={W - 14} y={memY + memH * 0.1 - 2} fontSize={9.5} textAnchor="end" fill="var(--st-crit)" opacity={0.8}>90%</text>
            <path d={step(mem, (m) => m.share, memY, memH, true)} fill="var(--series-2)" fillOpacity={0.45} stroke="var(--series-2)" strokeWidth={1} />
            <path d={step(mem, (m) => m.max_share, memY, memH, false)} fill="none" stroke="var(--st-crit)" strokeWidth={1.4} />
            <path d={step(mem, (m) => m.driver_share, memY, memH, false)} fill="none" stroke="var(--text)" strokeOpacity={0.6} strokeWidth={1} strokeDasharray="2 2" />
            {mem.map((m) => (
              <rect key={m.minute} x={x(m.minute)} y={memY} width={bw} height={memH} fill="transparent"
                onMouseMove={hover(`${fmtTime(m.minute)} UTC: executors use ${fmtPct(m.share, 0)} of their heap (${fmtBytes(m.used_mb * 1048576)} of ${fmtBytes(m.up_mb * 1048576)})` +
                  (m.max_exec !== null ? `\nfullest: exec ${m.max_exec} at ${fmtPct(m.max_share, 0)}` : '') +
                  (m.driver_share !== null ? `\ndriver: ${fmtPct(m.driver_share, 0)} of its heap` : '') +
                  '\n(heap left in use right after garbage collection: what the JVM could not free)')}
                onMouseLeave={() => setTip(null)} />
            ))}
            <g transform={`translate(${LABEL_W}, ${memY + memH + 12})`} fontSize={10.5} fill="var(--text)">
              <rect x={0} y={-7} width={14} height={7} fill="var(--series-2)" fillOpacity={0.45} /><text x={18} y={0}>all executors</text>
              <line x1={100} x2={114} y1={-3} y2={-3} stroke="var(--st-crit)" strokeWidth={1.4} /><text x={118} y={0}>fullest executor{peakMem?.max_share != null ? ` (peak ${fmtPct(peakMem.max_share, 0)}, exec ${peakMem.max_exec} at ${hhmm(peakMem.minute)})` : ''}</text>
              <line x1={420} x2={434} y1={-3} y2={-3} stroke="var(--text)" strokeOpacity={0.6} strokeDasharray="2 2" /><text x={438} y={0}>driver</text>
            </g>
          </g>
        )}
        <text x={4} y={spY + 14} fontSize={12} fontWeight={600} fill="var(--text)">Disk spill</text>
        <text x={4} y={spY + 28} fontSize={11} fill="var(--text)" opacity={0.6}>per minute, max {fmtBytes(maxSp)}</text>
        <line x1={LABEL_W} x2={W - 12} y1={spY + spH} y2={spY + spH} stroke="var(--line)" />
        {d.minutes.filter((m) => m.disk_spill).map((m) => {
          const h = Math.max(1.5, ((m.disk_spill ?? 0) / maxSp) * spH);
          return (
            <rect key={m.minute} x={x(m.minute)} y={spY + spH - h} width={bw} height={h} fill="var(--spill)"
              onMouseMove={hover(`${fmtTime(m.minute)} UTC: ${fmtBytes(m.disk_spill)} spilled to disk`)} onMouseLeave={() => setTip(null)} />
          );
        })}
        {at != null && at >= t0 && at <= t1 && (
          <g pointerEvents="none">
            <line x1={x(at)} x2={x(at)} y1={16} y2={H} stroke="var(--text)" strokeWidth={1.5} />
            <text x={x(at) + 4} y={24} fontSize={11} fontWeight={700} fill="var(--text)">{hhmm(at)}</text>
          </g>
        )}
      </svg>
      {tip && (
        <div className="chart-tip" style={{ position: 'absolute', left: Math.min(tip.x + 12, W - 320), top: tip.y + 12, whiteSpace: 'pre-line', pointerEvents: 'none', maxWidth: 360 }}>
          {tip.text}
        </div>
      )}
    </div>
  );
}

/** How busy each executor was over its whole life, for every run: the share of its cores' time running tasks, and the
 * cluster's core-hours up against those used. Revision 20: and its memory: the heap, what was still in use right
 * after garbage collection (near 100% the JVM spends its time collecting and can run out of memory), the GC pauses
 * and Full GCs, the most one task held for execution, and the spill. */
function ExecutorsBusy({ cid, compute, heaps }: { cid: string; compute: ComputeUse | null; heaps: CvExecutor[] }) {
  const st = useAsync((s) => api.dataset<ExecutorProfileRow>(cid, 'executor_profile', { limit: 2000, sort: 'added_time', run: '' }, s), [cid]);
  const rows = (st.data?.rows ?? []).filter((r) => r.executor_id !== 'driver');
  if (!rows.length) return null;
  const heapOf = (r: ExecutorProfileRow) => heaps.find((h) => h.executor_id === r.executor_id && h.spark_context_id === r.spark_context_id)
    ?? heaps.find((h) => h.executor_id === r.executor_id);
  const coreH = (ms: number) => `${(ms / 3_600_000).toFixed(1)} core-hours`;
  const MB = 1024 ** 2;
  // the median heap left after a Full GC: a young GC can leave a full heap for a moment without a problem
  const full = rows.filter((r) => ((r as ExecutorProfileRow & { full_gc_heap_after_p50?: number | null }).full_gc_heap_after_p50 ?? 0) >= 0.85);
  return (
    <Panel title="Executors: how busy, and their memory"
      note="Busy: the share of each executor's cores' time spent running tasks while it was up. Memory: its heap, the most still in use right after a garbage collection (what the JVM could not free), GC pauses, the most one task held, and spill.">
      {compute && compute.idle_share !== null && (
        <p style={{ margin: '0 0 6px' }}>
          Executors were up <b>{coreH(compute.core_ms_up)}</b> and ran tasks for <b>{coreH(compute.core_ms_used)}</b>:{' '}
          <b>{fmtPct(1 - compute.idle_share, 0)} busy</b>, {fmtPct(compute.idle_share, 0)} paid for but idle.
        </p>
      )}
      {full.length > 0 && (
        <p className="st-crit" style={{ margin: '0 0 10px' }}>
          <b>{full.length} of {rows.length} executors had their heap still 85% full or more after a Full GC (median)</b> (exec {full.map((r) => r.executor_id).join(', ')}):
          the heap held data the JVM could not free, so it kept pausing to collect{full.some((r) => (r.full_gcs ?? 0) > 0) ? ` (${fmtNum(full.reduce((a, r) => a + (r.full_gcs ?? 0), 0))} Full GCs)` : ''}.
          Look for a cache or a broadcast held in memory; then fewer cores per executor or more memory per core (memory-optimised workers).
        </p>
      )}
      <div className="table-wrap">
        <table className="table exec-mem">
          <thead>
            <tr className="group-head">
              <th colSpan={5} />
              <th colSpan={3} className="grp">Memory</th>
              <th colSpan={2} className="grp">Garbage collection</th>
              <th colSpan={2} className="grp">Spill</th>
              <th />
            </tr>
            <tr>
              <th>Executor</th><th className="num">Cores</th><th>Up</th><th style={{ width: '16%' }}>Busy</th><th className="num">Tasks</th>
              <th className="num" title="JVM heap of the executor">Heap</th>
              <th style={{ width: '14%' }} title="The most heap still in use right after a garbage collection: what the JVM could not free. Over 90% is critical.">Left after GC (peak)</th>
              <th className="num" title="The most execution memory one task used (sorts, joins, aggregations)">Most one task held</th>
              <th className="num" title="Time in GC pauses, and the share of task time spent in GC">Pauses</th>
              <th className="num" title="Full GCs stop everything and scan the whole heap">Full GCs</th>
              <th className="num" title="Spilled data as it was in memory">Memory</th><th className="num">Disk</th>
              <th>How it ended</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const h = heapOf(r);
              const heap = h?.heap_mb ?? null;
              const left = r.max_heap_after_mb ?? null;
              const share = heap && left !== null ? Math.min(1, left / heap) : null;
              const tone = share === null ? '' : share >= 0.9 ? 'crit' : share >= 0.7 ? 'warn' : '';
              const gcBad = (r.gc_share ?? 0) >= 0.1;
              return (
                <tr key={`${r.spark_context_id}|${r.executor_id}`}>
                  <td className="mono">exec {r.executor_id}{r.host ? <div className="muted small">{r.host}</div> : null}</td>
                  <td className="num">{r.cores ?? '–'}</td>
                  <td className="mono small">{fmtTime(r.added_time)} → {r.removed_time ? fmtTime(r.removed_time) : 'end'}<div className="muted">{fmtDuration(r.lifetime_ms)}</div></td>
                  <td>
                    <div className="busy-cell">
                      <span className="busy-track"><i style={{ width: `${Math.round((r.busy_share ?? 0) * 100)}%` }} /></span>
                      <b>{fmtPct(r.busy_share, 0)}</b>
                    </div>
                  </td>
                  <td className="num">{fmtNum(r.tasks)}{r.failed_tasks ? <div className="small bad">{fmtNum(r.failed_tasks)} failed</div> : null}</td>
                  <td className="num" title={h?.heap_from_gc ? 'The largest heap the JVM reached (from its GC lines): the event log did not give the size' : undefined}>
                    {heap ? fmtBytes(heap * MB) : '–'}{h?.heap_from_gc ? <span className="muted">*</span> : null}
                    {h?.unified_memory ? <div className="muted small">{fmtBytes(h.unified_memory)} for Spark</div> : null}
                  </td>
                  <td>
                    {left !== null ? (
                      <div className="busy-cell" title={`${fmtBytes(left * MB)} still in use right after a garbage collection${heap ? ` of ${fmtBytes(heap * MB)}` : ''}`}>
                        <span className={`busy-track mem ${tone}`}><i style={{ width: `${Math.round((share ?? 0) * 100)}%` }} /></span>
                        <b className={tone === 'crit' ? 'st-crit' : tone === 'warn' ? 'st-warn' : ''}>{share !== null ? fmtPct(share, 0) : fmtBytes(left * MB)}</b>
                      </div>
                    ) : <span className="muted">–</span>}
                  </td>
                  <td className="num">{r.max_peak_mem ? fmtBytes(r.max_peak_mem) : '–'}</td>
                  <td className="num">
                    {r.gc_pause_ms ? fmtDuration(r.gc_pause_ms) : '–'}
                    {r.gc_pauses ? <div className="muted small">{fmtNum(r.gc_pauses)} pauses</div> : null}
                    {r.gc_share != null ? <div className={`small ${gcBad ? 'st-warn' : 'muted'}`}>{fmtPct(r.gc_share, 0)} of task time</div> : null}
                  </td>
                  <td className={`num ${(r.full_gcs ?? 0) >= 10 ? 'st-crit' : ''}`}>{r.full_gcs ? fmtNum(r.full_gcs) : '–'}</td>
                  <td className="num muted">{r.mem_spill ? fmtBytes(r.mem_spill) : '–'}</td>
                  <td className="num">{r.disk_spill ? <span style={{ color: 'var(--spill)' }}>{fmtBytes(r.disk_spill)}</span> : '–'}</td>
                  <td className="small">{r.removed_reason ?? (r.removed_time ? '–' : 'still up at the end')}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {rows.some((r) => heapOf(r)?.heap_from_gc) && <p className="muted small" style={{ margin: '6px 0 0' }}>* Heap: the largest the JVM reached in its GC lines; the event log did not record the configured size.</p>}
    </Panel>
  );
}

type SortKey = 'start' | 'took' | 'usual' | 'data' | 'spill' | 'failed' | 'read' | 'shuffle' | 'tasks' | 'wait' | 'run';

/** How a run ended: failed, still running at the end, succeeded only after failed tasks or stages were retried, or clean. */
type RunState = 'failed' | 'incomplete' | 'retried' | 'ok';
const runState = (r: RunRow): RunState =>
  r.status === 'failed' ? 'failed' : r.status !== 'succeeded' ? 'incomplete'
    : (r.failed_tasks ?? 0) > 0 || (r.failed_stages ?? 0) > 0 || (r.retried_tasks ?? 0) > 0 ? 'retried' : 'ok';
const STATE_LABEL: Record<RunState, string> = { failed: 'Failed', incomplete: 'Incomplete', retried: 'Succeeded after retries', ok: 'Succeeded' };

export function RunsTable({ runs, goTo, atText, onAtText }: { runs: RunRow[]; goTo?: string; atText?: string; onAtText?: (s: string) => void }) {
  const open = useOpenRun();
  const [sort, setSort] = useState<SortKey>('start');
  const val: Record<SortKey, (r: RunRow) => number> = {
    start: (r) => -(r.start_time ?? 0),
    took: (r) => r.duration_ms ?? -1,
    usual: (r) => usualX(r) ?? -1,
    data: (r) => r.wmed_task_bytes_in ?? r.p90_task_bytes_in ?? -1,
    spill: (r) => r.disk_spill ?? -1,
    failed: (r) => (r.status === 'failed' ? 1e15 : 0) + (r.failed_stages ?? 0) * 1e6 + (r.failed_tasks ?? 0),
    read: (r) => (r.input_bytes ?? 0) + (r.shuffle_read ?? 0),
    shuffle: (r) => (r.shuffle_read ?? 0) + (r.shuffle_write ?? 0),
    tasks: (r) => r.tasks ?? -1,
    wait: (r) => r.waiting_ms ?? -1,
    run: (r) => r.running_ms ?? -1,
  };
  // thousands of runs: find the one that ran at a given time, or by name, table or id
  const [ownAt, setOwnAt] = useState('');
  const at = atText ?? ownAt;
  const setAt = onAtText ?? setOwnAt;
  const [q, setQ] = useState('');
  const [state, setState] = useState<RunState | null>(null);
  // runs under a minute (task values, setup steps) are noise next to the real work: hidden when there are many
  const isShort = (r: RunRow) => (r.duration_ms ?? 0) < 60_000 && r.status !== 'failed';
  const nShort = runs.filter(isShort).length;
  const [showShort, setShowShort] = useState(nShort <= 5);
  const atMs = parseAt(at, runs);
  const needle = q.trim().toLowerCase();
  const nState = (k: RunState) => runs.filter((r) => runState(r) === k).length;
  const shown = runs.filter(
    (r) =>
      (atMs === null || runningAt(r, atMs)) &&
      (!state || runState(r) === state) &&
      (showShort || !isShort(r)) &&
      (!needle || [runName(r), r.label, r.run_key, r.job_name, r.notebook_path].some((v) => v && v.toLowerCase().includes(needle))),
  );
  const rows = [...shown].sort((a, b) => val[sort](b) - val[sort](a));
  const th = (k: SortKey, label: string, title?: string) => (
    <th className="sortable" title={title} aria-sort={sort === k ? 'descending' : 'none'} onClick={() => setSort(k)} style={{ cursor: 'pointer' }}>
      {label}
      {sort === k ? ' ▾' : ''}
    </th>
  );
  return (
    <div className="stack" style={{ gap: 8 }}>
    <div className="row" style={{ gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
      <label className="small">
        Running at (UTC){' '}
        <input className="input" style={{ width: 80 }} placeholder="13:00" value={at} onChange={(e) => setAt(e.target.value)} />
      </label>
      <input className="input" style={{ minWidth: 200, flex: '1 1 200px' }} placeholder="Search name, table, notebook or id" value={q} onChange={(e) => setQ(e.target.value)} />
      <div className="seg" role="group" aria-label="Filter by status">
        <button className={state === null ? 'on' : ''} aria-pressed={state === null} onClick={() => setState(null)}>All {fmtNum(runs.length)}</button>
        {(['failed', 'incomplete', 'retried', 'ok'] as RunState[]).filter((k) => nState(k) > 0).map((k) => (
          <button key={k} className={state === k ? 'on' : ''} aria-pressed={state === k} onClick={() => setState(state === k ? null : k)}>
            <span className={`run-state-dot ${k}`} />{STATE_LABEL[k]} {fmtNum(nState(k))}
          </button>
        ))}
      </div>
      {nShort > 5 && (
        <label className="small">
          <input type="checkbox" checked={showShort} onChange={(e) => setShowShort(e.target.checked)} /> show the {nShort} runs under a minute
        </label>
      )}
      <span className="small muted">{shown.length === runs.length ? `${fmtNum(runs.length)} runs` : `${fmtNum(shown.length)} of ${fmtNum(runs.length)} runs`}</span>
    </div>
    <div className="table-wrap" style={{ overflowX: 'auto', maxHeight: 640, overflowY: 'auto' }}>
      <table className="table runs-table">
        <thead>
          <tr className="group-head">
            <th colSpan={2} />
            <th colSpan={3} className="grp" title="Total: start to end. Waiting: a stage was submitted but no core was free and none of its stages ran. Processing: at least one of its stages ran tasks. The rest of the total is its code outside Spark.">Time</th>
            <th colSpan={2} />
            <th colSpan={2} className="grp">Stages · tasks</th>
            <th colSpan={6} className="grp">Read per task (input + shuffle read)</th>
            <th colSpan={2} className="grp">Task time</th>
            <th colSpan={2} className="grp">Shuffle</th>
            <th colSpan={2} className="grp">Spill</th>
          </tr>
          <tr>
            <th>Run</th>
            {th('start', 'Started')}
            {th('took', 'Total')}
            {th('wait', 'Waiting', 'Waiting for a free core: a stage was submitted, none of its stages ran a task')}
            {th('run', 'Processing', 'At least one of its stages was running tasks')}
            {th('usual', '× usual', 'How long it took against the median of the runs of the same program')}
            {th('failed', 'Status')}
            <th className="num" title="Stages; failed stage attempts under it">Stages</th>
            {th('tasks', 'Tasks', 'Tasks; failed and retried under it')}
            {th('read', 'Total')}
            <th className="num">Avg</th>
            <th className="num">p10</th>
            <th className="num">Median</th>
            <th className="num">p90</th>
            {th('data', 'Biggest', 'The biggest task, and how many tasks read 256 MB or more (hover for the spread)')}
            <th className="num">Median</th>
            <th className="num">p90</th>
            {th('shuffle', 'Read')}
            <th className="num">Write</th>
            {th('spill', 'Disk')}
            <th className="num">Memory</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const x = usualX(r);
            const b = (v: number | null | undefined) => (v ? fmtBytes(v) : '–');
            return (
              <tr key={r.run_key} className="clickable" onClick={() => open(r, goTo)} style={{ cursor: 'pointer' }}>
                <td>
                  <div style={{ fontWeight: 600 }}>{runName(r)}</div>
                  <div className="muted small">{runId(r)}</div>
                </td>
                <td className="mono small">{fmtTime(r.start_time)}</td>
                <td className="num">{fmtDuration(r.duration_ms)}</td>
                <td className="num" title={r.waiting_ms != null && r.duration_ms ? `${Math.round((r.waiting_ms / r.duration_ms) * 100)}% of its time` : undefined}>
                  {r.waiting_ms ? <span className={r.duration_ms && r.waiting_ms / r.duration_ms >= 0.2 ? 'wait-text' : ''} style={{ fontWeight: r.duration_ms && r.waiting_ms / r.duration_ms >= 0.2 ? 600 : undefined }}>{fmtDuration(r.waiting_ms)}</span> : '–'}
                </td>
                <td className="num">{r.running_ms != null ? fmtDuration(r.running_ms) : '–'}</td>
                <td className={`num ${(x ?? 0) >= 2 ? 'st-warn' : 'muted'}`} style={{ fontWeight: (x ?? 0) >= 2 ? 600 : undefined }}>{x !== null ? `${x.toFixed(1)}×` : '–'}</td>
                <td>
                  {runState(r) === 'ok' ? <span className="muted small">Succeeded</span>
                    : runState(r) === 'retried' ? <span className="chip warn" title={`${fmtNum(r.failed_stages ?? 0)} stage attempts and ${fmtNum(r.failed_tasks ?? 0)} task attempts failed and were retried`}>Succeeded after retries</span>
                    : <StatusBadge status={r.status} />}
                </td>
                <td className="num">
                  {fmtNum(r.stages)}
                  {r.failed_stages ? <div className="small bad">{fmtNum(r.failed_stages)} failed</div> : null}
                </td>
                <td className="num">
                  {fmtNum(r.tasks)}
                  {r.failed_tasks ? <div className="small bad">{fmtNum(r.failed_tasks)} failed</div> : null}
                  {r.retried_tasks ? <div className="small muted">{fmtNum(r.retried_tasks)} retried</div> : null}
                </td>
                <td className="num">{b((r.input_bytes ?? 0) + (r.shuffle_read ?? 0))}</td>
                <td className="num muted">{b(r.avg_task_bytes_in)}</td>
                <td className="num muted">{b(r.p10_task_bytes_in)}</td>
                <td className="num">{b(r.p50_task_bytes_in)}</td>
                <td className="num">{b(r.p90_task_bytes_in)}</td>
                <td className="num">
                  {r.max_task_bytes_in ? (
                    <span title={`Per task: median ${fmtBytes(r.p50_task_bytes_in)}, p90 ${fmtBytes(r.p90_task_bytes_in)}, biggest ${fmtBytes(r.max_task_bytes_in)}` + (bandsLine(r) ? `
${bandsLine(r)}` : '')}>
                      {fmtBytes(r.max_task_bytes_in)}
                      {r.tasks_ge256 ? <div className="small st-crit">{fmtNum(r.tasks_ge256)} ≥ 256 MB</div> : null}
                    </span>
                  ) : '–'}
                </td>
                <td className="num">{r.p50_task_ms != null ? fmtDuration(r.p50_task_ms) : '–'}</td>
                <td className="num">{r.p90_task_ms != null ? fmtDuration(r.p90_task_ms) : '–'}</td>
                <td className="num" style={{ color: r.shuffle_read ? 'var(--shuf)' : undefined }}>{b(r.shuffle_read)}</td>
                <td className="num" style={{ color: r.shuffle_write ? 'var(--shuf)' : undefined }}>{b(r.shuffle_write)}</td>
                <td className="num" style={{ color: r.disk_spill ? 'var(--spill)' : undefined }}>{b(r.disk_spill)}</td>
                <td className="num muted">{b(r.mem_spill)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
    </div>
  );
}
