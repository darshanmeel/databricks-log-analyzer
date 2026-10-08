// Revision 15: the same summary at every level of the drill-down. A query: cards for what went wrong over all its
// jobs and stages, its numbers, then its jobs as a table (click one). A job: the same over its stages, then its
// stages (click one for the task level). Numbers and small charts, not sentences.
import { CauseBar, CauseLegend, causeParts } from './CauseBar';
import { TablesRead } from './TableStats';
import { useState } from 'react';
import { api, type SparkJobRow, type StageRow, type StageWhy } from '../api';
import { useAsync } from '../hooks';
import { fmtBytes, fmtDuration, fmtNum, fmtPct, fmtRows, fmtSkew, fmtTime, truncate, unitFor } from '../format';
import { gcBreach, skewBreach, spillBreach } from '../thresholds';
import { StatusBadge, Tile } from './ui';
import { ScopeTaskSection } from './TaskSections';
import { Bars, SIGN, Split, VCards, signFor, type VCard } from './VCards';
import type { QueryStep, QueryTime } from '../api';

const MB = 1 << 20;
type Stage = StageRow & { spark_job_id?: number | null; wmed_task_bytes_in?: number | null; tasks_ge256?: number | null };
type Job = SparkJobRow & { wmed_task_bytes_in?: number | null; p50_task_ms?: number | null; max_task_ms?: number | null; tasks_ge256?: number | null };

const sum = (xs: (number | null | undefined)[]) => xs.reduce<number>((a, x) => a + (x ?? 0), 0);
const read = (s: { input_bytes?: number | null; shuffle_read?: number | null }) => (s.input_bytes ?? 0) + (s.shuffle_read ?? 0);
const both = (a: string, x: number, b: string, y: number) => [x ? `${a} ${fmtBytes(x)}` : null, y ? `${b} ${fmtBytes(y)}` : null].filter(Boolean).join(' · ') || null;
const stageName = (s: Stage) => `Stage ${s.stage_id}${s.stage_attempt ? `.${s.stage_attempt}` : ''}`;

/** `short` is the chip in a table row (the full `text` is its tooltip). */
type Reason = { text: string; short: string; tone: 'bad' | 'warn' | 'ok'; weight: number };
const keyOf = (s: { stage_id: number; stage_attempt: number }) => `${s.stage_id}:${s.stage_attempt}`;

/** Why one stage took as long as it did, in plain words, the biggest reason first. */
function reasons(s: Stage, w: StageWhy | undefined): Reason[] {
  const dur = s.duration_ms ?? 0;
  const out: Reason[] = [];
  if (s.status === 'failed') out.push({ text: `failed: ${truncate((s.failure_reason ?? '').split('\n')[0], 60)}`, short: 'failed', tone: 'bad', weight: 1e15 });
  if (w?.wait_ms && w.wait_ms >= 2_000 && w.wait_ms >= 0.15 * dur) out.push({ text: `waited ${fmtDuration(w.wait_ms)} for cores`, short: `wait ${fmtDuration(w.wait_ms)}`, tone: 'warn', weight: w.wait_ms });
  const wm = s.wmed_task_bytes_in ?? 0;
  if (wm >= 256 * MB) {
    const want = Math.ceil(read(s) / (128 * MB) / 100) * 100;
    out.push({ text: s.shuffle_read ? `tasks too big: ${fmtBytes(wm)} each (~${fmtNum(want)} partitions, not ${fmtNum(s.tasks)})` : `files too big to split: ${fmtBytes(wm)} a task`, short: s.shuffle_read ? `${fmtBytes(wm)}/task → ${fmtNum(want)} parts` : `${fmtBytes(wm)}/task`, tone: 'warn', weight: dur * 0.6 });
  }
  if (skewBreach(s.skew, s.max_task_ms)) out.push({ text: `one task ${fmtSkew(s.skew)} the median (${fmtDuration(s.max_task_ms)})`, short: `skew ${fmtSkew(s.skew)}`, tone: 'bad', weight: (s.max_task_ms ?? 0) - (s.p50_task_ms ?? 0) });
  if (w?.other_share != null && w.other_share >= 0.2) out.push({ text: `${fmtPct(w.other_share, 0)} of its executors went to other stages`, short: `shared ${fmtPct(w.other_share, 0)}`, tone: 'warn', weight: w.other_share * dur });
  if (w?.tail_ms && w.tail_ms >= 10_000 && w.tail_ms >= 0.25 * dur) out.push({ text: `last 10% of tasks took ${fmtDuration(w.tail_ms)}`, short: `tail ${fmtDuration(w.tail_ms)}`, tone: 'warn', weight: w.tail_ms });
  if (spillBreach(s.disk_spill)) out.push({ text: `spilled ${fmtBytes(s.disk_spill)} to disk`, short: `spill ${fmtBytes(s.disk_spill)}`, tone: 'warn', weight: dur * 0.3 });
  if (gcBreach(s.gc_share)) out.push({ text: `GC ${fmtPct(s.gc_share, 0)} of task time`, short: `GC ${fmtPct(s.gc_share, 0)}`, tone: 'warn', weight: (s.gc_share ?? 0) * dur });
  if ((s.tasks ?? 0) >= 2000 && (s.p50_task_ms ?? 1e9) < 200) out.push({ text: `${fmtNum(s.tasks)} tiny tasks (${fmtDuration(s.p50_task_ms)} each)`, short: `${fmtNum(s.tasks)} tiny tasks`, tone: 'warn', weight: dur * 0.3 });
  if (!out.length) out.push({ text: dur < 5000 ? 'quick' : `no problem: ${fmtNum(s.tasks)} tasks of ~${fmtDuration(s.p50_task_ms)}`, short: dur < 5000 ? 'quick' : 'ok', tone: 'ok', weight: 0 });
  return out.sort((a, b) => b.weight - a.weight);
}

function ReasonChips({ rs, max = 4, short = false }: { rs: Reason[]; max?: number; short?: boolean }) {
  const rest = rs.slice(max);
  return (
    <span className={`why-chips ${short ? 'short' : ''}`}>
      {rs.slice(0, max).map((r) => (
        <span key={r.text} className={`why ${r.tone}`} title={short ? r.text : undefined}><span aria-hidden>{signFor(r.text)} </span>{short ? r.short : r.text}</span>
      ))}
      {rest.length ? <span className="why more" title={rest.map((r) => r.text).join('\n')}>+{rest.length}</span> : null}
    </span>
  );
}

/** One line at the top of a level: what its longest stages spent their time on. */
function WhyLine({ dur, stages, why }: { dur: number | null; stages: Stage[]; why: Map<string, StageWhy> }) {
  const top = [...stages].sort((a, b) => (b.duration_ms ?? 0) - (a.duration_ms ?? 0)).slice(0, 2).filter((s) => (s.duration_ms ?? 0) > 0);
  if (!top.length) return null;
  return (
    <div className="why-line" title="The two longest stages and why. Hover a chip for the full reason.">
      <b>Why {fmtDuration(dur)}</b>
      {top.map((s) => (
        <span key={keyOf(s)} className="why-line-item">
          <span className="muted">{stageName(s)} · {fmtDuration(s.duration_ms)}</span> <ReasonChips rs={reasons(s, why.get(keyOf(s)))} short />
        </span>
      ))}
    </div>
  );
}

/** Cards over a set of stages: failures, the worst slow-task stage, tasks too big, spill (by stage), GC, where the time went. */
function levelCards(stages: Stage[], what: string, why: Map<string, StageWhy>): VCard[] {
  const cards: VCard[] = [];
  const failed = stages.filter((s) => s.status === 'failed');
  if (failed.length)
    cards.push({ tone: 'bad', title: 'Failed', big: `${failed.length} of ${stages.length}`, unit: 'stages failed', note: truncate((failed[0].failure_reason ?? '').split('\n')[0], 150) });
  const slow = stages.filter((s) => skewBreach(s.skew, s.max_task_ms)).sort((a, b) => (b.max_task_ms ?? 0) - (a.max_task_ms ?? 0))[0];
  if (slow)
    cards.push({
      tone: 'bad', title: 'A few tasks held it up', big: fmtSkew(slow.skew), unit: `slowest ÷ median · ${stageName(slow)}`,
      chart: <Bars rows={[['median', slow.p50_task_ms, false], ['slowest', slow.max_task_ms, true]]} fmt={fmtDuration} />,
      note: `${stages.filter((s) => skewBreach(s.skew, s.max_task_ms)).length} of ${stages.length} stages had a task far slower than the rest.`,
    });
  const big = stages.filter((s) => (s.wmed_task_bytes_in ?? 0) >= 256 * MB).sort((a, b) => (b.wmed_task_bytes_in ?? 0) - (a.wmed_task_bytes_in ?? 0));
  if (big.length) {
    const b = big[0];
    const want = Math.ceil(read(b) / (128 * MB) / 100) * 100;
    cards.push({
      tone: 'warn', title: 'Tasks too big', big: fmtBytes(b.wmed_task_bytes_in ?? 0), unit: `per task · ${stageName(b)}`,
      chart: <Bars rows={[['biggest stage', b.wmed_task_bytes_in ?? 0, true], ['target', 128 * MB, false]]} fmt={fmtBytes} />,
      note: `${big.length} of ${stages.length} stages above 256 MiB a task.${b.shuffle_read ? ` ~${fmtNum(want)} partitions instead of ${fmtNum(b.tasks)} for ${stageName(b)}.` : ' Files that cannot be split.'}`,
    });
  }
  // waiting for cores: stages whose first task started long after they were submitted
  const waits = stages.map((s) => ({ s, w: why.get(keyOf(s))?.wait_ms ?? 0 })).filter((x) => x.w >= 10_000 && x.w >= 0.15 * (x.s.duration_ms ?? 0))
    .sort((a, b) => b.w - a.w);
  if (waits.length) {
    const top = waits[0];
    const dur = top.s.duration_ms ?? top.w;
    cards.push({
      tone: 'warn', title: 'Waited for cores', big: fmtDuration(sum(waits.map((x) => x.w))), unit: waits.length > 1 ? `${waits.length} stages` : stageName(top.s),
      chart: <Split parts={[['waiting', top.w, 'var(--wait)'], ['running', Math.max(0, dur - top.w), 'var(--series-1)']]} fmt={fmtDuration} />,
      note: `${stageName(top.s)} spent ${fmtPct(top.w / Math.max(1, dur), 0)} of its ${fmtDuration(dur)} waiting: the cores were busy with other work.`,
    });
  }
  // executors shared with other stages
  const shared = stages.map((s) => ({ s, w: why.get(keyOf(s)) })).filter((x) => (x.w?.other_share ?? 0) >= 0.2)
    .sort((a, b) => (b.w?.other_ms ?? 0) - (a.w?.other_ms ?? 0));
  if (shared.length) {
    const w = shared[0].w!;
    cards.push({
      tone: 'warn', title: 'Shared executors', big: fmtPct(w.other_share, 0), unit: `to other stages · ${stageName(shared[0].s)}`,
      chart: <Split parts={[['others', w.other_ms ?? 0, 'var(--text-3)'], ['free or its own', Math.max(0, (w.slot_ms ?? 0) - (w.other_ms ?? 0)), 'var(--series-1)']]} fmt={fmtDuration} pct />,
      note: `${shared.length} of ${stages.length} stages ran next to other stages' tasks on the same executors.`,
    });
  }
  const disk = sum(stages.map((s) => s.disk_spill));
  if (spillBreach(disk)) {
    const top = [...stages].sort((a, b) => (b.disk_spill ?? 0) - (a.disk_spill ?? 0)).filter((s) => (s.disk_spill ?? 0) > 0).slice(0, 3);
    cards.push({
      tone: 'warn', title: 'Ran out of memory', big: fmtBytes(disk), unit: 'spilled to disk',
      chart: <Bars rows={top.map((s) => [stageName(s), s.disk_spill ?? 0, true] as [string, number, boolean])} fmt={fmtBytes} />,
      note: `In ${stages.filter((s) => (s.disk_spill ?? 0) > 0).length} stages. Smaller tasks (more partitions) or more memory per core.`,
    });
  }
  const gcHot = stages.filter((s) => gcBreach(s.gc_share) && (s.tasks ?? 0) > 0);
  if (gcHot.length) {
    const g = gcHot.sort((a, b) => (b.gc_share ?? 0) - (a.gc_share ?? 0))[0];
    cards.push({
      tone: 'warn', title: 'Garbage collection', big: fmtPct(g.gc_share, 0), unit: `of task time · ${stageName(g)}`,
      chart: <Split parts={[['GC', g.gc_share ?? 0, 'var(--st-warn)'], ['work', 1 - (g.gc_share ?? 0), 'var(--series-1)']]} fmt={(x) => fmtPct(x, 0)} />,
      note: `${gcHot.length} stages short of memory.`,
    });
  }
  // where the time went: the longest stages against the rest
  const byTime = [...stages].filter((s) => (s.duration_ms ?? 0) > 0).sort((a, b) => (b.duration_ms ?? 0) - (a.duration_ms ?? 0));
  if (byTime.length > 1) {
    const tot = sum(byTime.map((s) => s.duration_ms));
    const top = byTime.slice(0, 2);
    const colors = ['var(--series-1)', 'var(--series-3)'];
    cards.push({
      tone: cards.length ? 'ok' : 'ok', title: 'Where the time went', big: fmtPct((top[0].duration_ms ?? 0) / tot, 0), unit: stageName(top[0]),
      chart: <Split parts={[...top.map((s, i) => [stageName(s), s.duration_ms ?? 0, colors[i]] as [string, number, string]), ['other ' + (byTime.length - top.length), tot - sum(top.map((s) => s.duration_ms)), 'var(--line)']]} fmt={fmtDuration} pct />,
      note: `Stage time summed over ${stages.length} stages of this ${what} (stages can overlap).`,
    });
  }
  // a big read from storage: the stage that is the problem must not read "nothing stands out"
  const files = [...stages].sort((a, b) => (b.input_bytes ?? 0) - (a.input_bytes ?? 0))[0];
  if (files && (files.input_bytes ?? 0) >= 10 * 1024 * MB)
    cards.splice(Math.min(cards.length, 1), 0, {
      tone: 'warn', title: 'Big read from files', big: fmtBytes(files.input_bytes), unit: stageName(files),
      note: `${files.stage_name && !files.stage_name.includes('<unknown>') ? `${truncate(files.stage_name, 80)}. ` : ''}If that is most of a table, the query's findings say whether it could skip files.`,
    });
  if (!cards.some((c) => c.tone !== 'ok'))
    cards.unshift({ tone: 'ok', title: 'Nothing stands out', big: '✓', note: 'No failures, no slow tasks, no big read, little spill and GC.' });
  return cards;
}

function Numbers({ dur, kids, kidWord, stages }: { dur: number | null; kids: number; kidWord: string; stages: Stage[] }) {
  const tasks = sum(stages.map((s) => s.tasks));
  const failedT = sum(stages.map((s) => s.failed_tasks));
  return (
    <div className="kpis stage-kpis">
      <Kpi label="Duration" value={fmtDuration(dur)} />
      <Kpi label={kidWord} value={fmtNum(kids)} foot={kidWord === 'Jobs' ? `${fmtNum(stages.length)} stages` : null} />
      <Kpi label="Tasks" value={fmtNum(tasks)} foot={failedT ? <span className="bad">{fmtNum(failedT)} failed attempts</span> : 'none failed'} tone={failedT ? 'warn' : undefined} />
      {sum(stages.map(read)) > 0 && <Kpi label="Read" value={fmtBytes(sum(stages.map(read)))} foot={both('files', sum(stages.map((s) => s.input_bytes)), 'shuffle', sum(stages.map((s) => s.shuffle_read)))} />}
      {/* shuffle is not output: what it wrote to storage, and what it shuffled to its next stages, apart */}
      {sum(stages.map((s) => s.output_bytes)) > 0 && <Kpi label="Wrote" value={fmtBytes(sum(stages.map((s) => s.output_bytes)))} foot="to storage" />}
      {sum(stages.map((s) => s.shuffle_write)) > 0 && <Kpi label="Shuffled" value={fmtBytes(sum(stages.map((s) => s.shuffle_write)))} foot="to the next stages" />}
      {sum(stages.map((s) => (s.disk_spill ?? 0) + (s.mem_spill ?? 0))) > 0 && <Kpi label="Spilled to disk" value={fmtBytes(sum(stages.map((s) => s.disk_spill)))} foot={`from ${fmtBytes(sum(stages.map((s) => s.mem_spill)))} in memory`} tone={spillBreach(sum(stages.map((s) => s.disk_spill))) ? 'warn' : undefined} />}
    </div>
  );
}

// one tile everywhere (ui.Tile): the tone colours the foot, the problem, not the total
const Kpi = Tile;

/** One row per child (job or stage): its time, stages, tasks, data, spill, which executors did the work, and why it
 * took that long. Every number column sorts; click a row to open it. */
type SortK = 'dur' | 'wait' | 'proc' | 'stages' | 'tasks' | 'read' | 'spill' | 'shuffle' | 'p90';
function ChildTable({ rows, onPick, word, why }: {
  rows: { id: string; label: string; sub: string; status: string | null; dur: number | null; stages: Stage[]; tasks: number | null }[];
  onPick: (id: string) => void; word: string; why: Map<string, StageWhy>;
}) {
  const [sort, setSort] = useState<SortK>('dur');
  // waiting for cores: each stage's time from submission to its first task, summed over the row's stages
  const waitOf = (st: Stage[]) => sum(st.map((x) => why.get(keyOf(x))?.wait_ms ?? 0));
  // processing: the time at least one of the row's stages was running tasks (from its first task to its end, merged)
  const procOf = (st: Stage[]) => {
    const spans = st.filter((x) => x.start_time !== null && x.end_time !== null)
      .map((x) => [Math.min(x.end_time!, x.start_time! + (why.get(keyOf(x))?.wait_ms ?? 0)), x.end_time!] as [number, number])
      .sort((a, b) => a[0] - b[0]);
    let tot = 0, cur: [number, number] | null = null;
    for (const [a, b] of spans) {
      if (cur && a <= cur[1]) cur[1] = Math.max(cur[1], b);
      else { if (cur) tot += cur[1] - cur[0]; cur = [a, b]; }
    }
    return tot + (cur ? cur[1] - cur[0] : 0);
  };
  const val: Record<SortK, (r: (typeof rows)[number]) => number> = {
    dur: (r) => r.dur ?? 0, wait: (r) => waitOf(r.stages), proc: (r) => procOf(r.stages), stages: (r) => r.stages.length, tasks: (r) => r.tasks ?? 0,
    read: (r) => sum(r.stages.map(read)), spill: (r) => sum(r.stages.map((s) => s.disk_spill)),
    shuffle: (r) => sum(r.stages.map((s) => (s.shuffle_read ?? 0) + (s.shuffle_write ?? 0))),
    p90: (r) => Math.max(0, ...r.stages.map((s) => s.p90_task_ms ?? 0)),
  };
  const sorted = [...rows].sort((a, b) => val[sort](b) - val[sort](a));
  const th = (k: SortK, label: string, num = true, unit?: string, tip?: string) => (
    <th className={`${num ? 'num' : ''} sortable`} title={tip} style={{ cursor: 'pointer' }} aria-sort={sort === k ? 'descending' : 'none'} onClick={() => setSort(k)}>
      {label}{unit ? <span className="unit"> ({unit})</span> : null}{sort === k ? ' ▾' : ''}
    </th>
  );
  const U = ({ u }: { u: string }) => <span className="unit"> ({u})</span>;
  const showStages = word === 'Job';
  // a stage row also gets its tasks' spread: time and data per task (p10, median, p90, biggest)
  const perTask = word === 'Stage';
  // one unit per column, written in its heading; the cells are bare numbers
  const ss = rows.map((r) => r.stages);
  // the three time columns share one unit, so they read side by side (took = waited + processed)
  const tu = unitFor('ms', [...rows.map((r) => r.dur), ...ss.map(waitOf), ...ss.map(procOf)]);
  const u = {
    dur: tu, wait: tu, proc: tu,
    pt: unitFor('ms', ss.flatMap((st) => [st[0]?.p50_task_ms, st[0]?.p90_task_ms])),
    read: unitFor('b', ss.map((st) => sum(st.map(read)))), pr: unitFor('b', ss.flatMap((st) => [st[0]?.p50_task_bytes_in, st[0]?.p90_task_bytes_in])),
    sr: unitFor('b', ss.map((st) => sum(st.map((x) => x.shuffle_read)))), sw: unitFor('b', ss.map((st) => sum(st.map((x) => x.shuffle_write)))),
    spill: unitFor('b', ss.map((st) => sum(st.map((x) => x.disk_spill)))),
  };
  const z = (f: (v: number) => string, v: number | null | undefined) => (v ? f(v) : '0');
  return (
    <div className="table-wrap" style={{ overflowX: 'auto' }}>
      <table className="table compact level-table">
        <thead>
          <tr className="group-head">
            <th colSpan={2} />
            <th colSpan={3} className="grp">Time</th>
            <th colSpan={showStages ? 2 : 1} className="grp">{showStages ? 'Stages · tasks' : 'Tasks'}</th>
            {perTask && <th colSpan={4} className="grp">Time per task</th>}
            <th colSpan={perTask ? 4 : 1} className="grp">Read{perTask ? ' per task' : ''}</th>
            <th colSpan={3} className="grp">Shuffle · spill</th>
            <th />
          </tr>
          <tr>
            <th>{word}</th>
            <th>Status</th>
            {th('dur', 'Took', true, u.dur.u)}
            {th('wait', 'Waited for cores', true, u.wait.u, 'Time its stages waited for a free core; the % is the share of its own time')}
            {th('proc', 'Processing', true, u.proc.u)}
            {showStages && th('stages', 'Stages')}
            {th('tasks', 'Tasks')}
            {perTask && <><th className="num">p10<U u={u.pt.u} /></th><th className="num">Median</th>{th('p90', 'p90')}<th className="num">Max</th></>}
            {th('read', perTask ? 'Total' : 'Read', true, u.read.u)}
            {perTask && <><th className="num">Median<U u={u.pr.u} /></th><th className="num">p90</th><th className="num">Max</th></>}
            {th('shuffle', 'Shuffle read', true, u.sr.u)}
            <th className="num">Shuffle write<U u={u.sw.u} /></th>
            {th('spill', 'Disk spill', true, u.spill.u)}
            <th style={{ minWidth: 200 }} title="Hover a chip for the full reason. shared: share of its executors' time that went to other stages · wait: waited for a free core · /task: data per task (→ the partitions it needs) · skew: slowest task × the median · tail: time of the last 10% of tasks · spill: spilled to disk">Why it took this long</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((r) => {
            const st = r.stages;
            const disk = sum(st.map((s) => s.disk_spill));
            const worstStage = [...st].sort((a, b) => (b.duration_ms ?? 0) - (a.duration_ms ?? 0))[0];
            return (
              <tr key={r.id} className="clickable" style={{ cursor: 'pointer' }} onClick={() => onPick(r.id)}>
                <td><b>{r.label}</b>{r.sub ? <div className="muted small">{truncate(r.sub, 60)}</div> : null}</td>
                <td>{r.status && r.status !== 'succeeded' && r.status !== 'completed' ? <StatusBadge status={r.status} /> : <span className="muted small">{r.status === 'completed' || r.status === 'succeeded' ? 'ok' : ''}</span>}</td>
                <td className="num"><b>{u.dur.f(r.dur)}</b></td>
                {(() => {
                  const w = waitOf(st);
                  const share = r.dur ? w / r.dur : 0;
                  return (
                    <td className={`num ${share >= 0.15 && w >= 1000 ? 'st-warn' : ''}`}>
                      {w > 0 ? <>{u.wait.f(w)} <span className="small muted">{fmtPct(Math.min(1, share), 0)}</span></> : <span className="muted">0</span>}
                    </td>
                  );
                })()}
                <td className="num">{u.proc.f(procOf(st))}</td>
                {showStages && <td className="num">{fmtNum(st.length)}</td>}
                <td className="num">
                  {fmtNum(r.tasks)}
                  {sum(st.map((x) => x.failed_tasks)) ? <div className="small bad">{fmtNum(sum(st.map((x) => x.failed_tasks)))} failed</div> : null}
                </td>
                {perTask && (
                  <>
                    <td className="num muted">{u.pt.f(st[0].p10_task_ms)}</td>
                    <td className="num">{u.pt.f(st[0].p50_task_ms)}</td>
                    <td className="num">{u.pt.f(st[0].p90_task_ms)}</td>
                    <td className={`num ${skewBreach(st[0].skew, st[0].max_task_ms) ? 'st-warn' : ''}`}>{u.pt.f(st[0].max_task_ms)}</td>
                  </>
                )}
                <td className="num">{z(u.read.f, sum(st.map(read)))}</td>
                {perTask && (
                  <>
                    <td className="num">{z(u.pr.f, st[0].p50_task_bytes_in)}</td>
                    <td className="num">{z(u.pr.f, st[0].p90_task_bytes_in)}</td>
                    <td className="num">{z(u.pr.f, st[0].max_task_bytes_in)}</td>
                  </>
                )}
                <td className={`num ${sum(st.map((x) => x.shuffle_read)) ? '' : 'zero-good'}`} style={{ color: sum(st.map((x) => x.shuffle_read)) ? 'var(--shuf)' : undefined }}>{z(u.sr.f, sum(st.map((x) => x.shuffle_read)))}</td>
                <td className={`num ${sum(st.map((x) => x.shuffle_write)) ? '' : 'zero-good'}`} style={{ color: sum(st.map((x) => x.shuffle_write)) ? 'var(--shuf)' : undefined }}>{z(u.sw.f, sum(st.map((x) => x.shuffle_write)))}</td>
                <td className={`num ${disk ? '' : 'zero-good'}`} style={{ color: disk ? 'var(--spill)' : undefined, fontWeight: spillBreach(disk) ? 600 : undefined }}>{z(u.spill.f, disk)}</td>
                <td>
                  {worstStage ? (
                    <>
                      {st.length > 1 ? <span className="muted small">{stageName(worstStage)}: </span> : null}
                      <ReasonChips rs={reasons(worstStage, why.get(keyOf(worstStage)))} max={2} short />
                    </>
                  ) : <span className="muted small">ran no stages</span>}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/** Query level: everything over its jobs and stages, then one row per job. */
/** Where a query's wall-clock time went: waiting for cores, running, or no Spark stage of it running (driver work),
 * over its own stages and those of the queries that ran inside it (listed, click one to open it). */
function QueryTimeBar({ t, onQuery }: { t: QueryTime; onQuery: (id: number) => void }) {
  if (!t.total_ms) return null;
  // the same cause bar as the run and the cluster: waiting, CPU, storage and other, GC, shuffle waits, no Spark work
  const parts = causeParts(t.waiting_ms, t.running_ms, t.outside_ms, t.causes);
  return (
    <div className="stage-time">
      <div className="stage-time-head cause-head"><b><span aria-hidden>{SIGN.time} </span>Where its {fmtDuration(t.total_ms)} went</b>
        <span className="ro-legend small"><CauseLegend parts={parts} total={t.total_ms} pct />
          {(t.causes?.disk_spill ?? 0) >= 256 * MB ? <span className="spill-label">spilled {fmtBytes(t.causes!.disk_spill)}</span> : null}</span></div>
      <CauseBar parts={parts} total={t.total_ms} by={null} />
      {t.inner.length > 0 && (
        <div className="small">
          <span className="muted">It ran {t.inner.length} {t.inner.length === 1 ? 'query' : 'queries'} inside it{t.inner.some((i) => !i.linked) ? ' (same run, same operation, within its time)' : ''}: </span>
          {t.inner.map((i, k) => (
            <span key={i.sql_execution_id}>
              {k ? ', ' : ''}
              <a href="#" onClick={(e) => { e.preventDefault(); onQuery(i.sql_execution_id); }}>Query {i.sql_execution_id}</a>
              <span className="muted"> {fmtDuration(i.duration_ms)}</span>
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

/** The query step by step: each stage (its own and those of the queries inside it) in the order they were submitted,
 * when it got its first core, how long it waited and ran, and a waterfall over the query's time. */
function QuerySteps({ t, id, onJob, onQuery }: { t: QueryTime; id: number; onJob: (jobId: number) => void; onQuery: (id: number) => void }) {
  const steps = t.steps ?? [];
  if (steps.length < 1 || t.start === undefined || t.end === undefined) return null;
  const t0 = t.start;
  const span = Math.max(1, t.end - t0);
  const pct = (v: number) => `${Math.min(100, Math.max(0, ((v - t0) / span) * 100))}%`;
  const width = (a: number, b: number) => `${Math.max(0.4, (Math.min(b, t.end!) - Math.max(a, t0)) / span * 100)}%`;
  const bigWait = (s: QueryStep) => s.wait_ms >= 2000 && s.wait_ms >= 0.2 * (s.wait_ms + s.run_ms);
  const stu = unitFor('ms', [...steps.map((s) => (s.wait_ms >= 500 ? s.wait_ms : 0)), ...steps.map((s) => s.run_ms)]);
  const su = { wait: stu, run: stu };  // waited and ran in one unit
  return (
    <div className="qsteps">
      <div className="stage-time-head"><b><span aria-hidden>{SIGN.time} </span>Step by step</b>
        <span className="muted small" title="Each stage from submitted to done"> <i className="qs-key wait" /> waiting <i className="qs-key run" /> running</span>
      </div>
      <div className="table-wrap">
        <table className="table compact qsteps-table">
          <thead>
            <tr>
              <th>Step</th><th className="num">Tasks</th><th>Submitted</th><th>First task</th><th className="num">Waited for cores<span className="unit"> ({su.wait.u})</span></th>
              <th className="num">Ran<span className="unit"> ({su.run.u})</span></th>
              <th title="Rows a stage got (from storage, or from its parent stages through the shuffle) and passed on (to the next stage through the shuffle, or written out)">Rows in → out</th><th style={{ width: '30%' }}>{fmtTime(t0)} → {fmtTime(t.end)}</th>
            </tr>
          </thead>
          <tbody>
            {steps.map((s) => {
              const own = s.sql_execution_id === id;
              return (
                <tr key={`${s.stage_id}.${s.stage_attempt}`}>
                  <td>
                    {own ? (
                      s.spark_job_id !== null ? <a href="#" onClick={(e) => { e.preventDefault(); onJob(s.spark_job_id!); }}>Job {s.spark_job_id}</a> : 'No job'
                    ) : (
                      <a href="#" onClick={(e) => { e.preventDefault(); if (s.sql_execution_id !== null) onQuery(s.sql_execution_id); }}>Query {s.sql_execution_id} inside it</a>
                    )}
                    <span className="muted"> · stage {s.stage_id}{s.stage_attempt ? ` (attempt ${s.stage_attempt})` : ''}{!own && s.spark_job_id !== null ? ` · job ${s.spark_job_id}` : ''}</span>
                    {s.status && s.status !== 'succeeded' && <> <StatusBadge status={s.status} /></>}
                  </td>
                  <td className="num">{fmtNum(s.tasks)}</td>
                  <td className="mono small">{fmtTime(s.submitted)}</td>
                  <td className="mono small">{fmtTime(s.first_task)}</td>
                  <td className={`num ${bigWait(s) ? 'st-warn' : 'muted'}`}>{bigWait(s) && <span aria-hidden>{SIGN.wait} </span>}{s.wait_ms >= 500 ? su.wait.f(s.wait_ms) : '0'}</td>
                  <td className="num">{su.run.f(s.run_ms)}</td>
                  <td className="small"><RowsFlow s={s} /></td>
                  <td>
                    <div className="qs-lane" title={`waited ${fmtDuration(s.wait_ms)}, ran ${fmtDuration(s.run_ms)}`}>
                      {s.wait_ms > 0 && <span className="qs-wait" style={{ left: pct(s.submitted), width: width(s.submitted, s.first_task) }} />}
                      <span className="qs-run" style={{ left: pct(s.first_task), width: width(s.first_task, s.end) }} />
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/** What a stage passed along: rows in (from storage, from its parent stages' shuffle) → rows out (to the next
 * stage's shuffle, written out). */
export function RowsFlow({ s }: { s: Pick<QueryStep, 'rows_read' | 'rows_from_shuffle' | 'rows_to_shuffle' | 'rows_written' | 'parent_ids'> }) {
  const part = (v: number | null | undefined, what: string) => (v ? `${fmtRows(v)} ${what}` : null);
  const from = s.parent_ids?.length ? `from stage ${s.parent_ids.join(', ')}` : 'from the shuffle';
  const inn = [part(s.rows_read, 'read'), part(s.rows_from_shuffle, from)].filter(Boolean);
  const out = [part(s.rows_to_shuffle, 'to the shuffle'), part(s.rows_written, 'written')].filter(Boolean);
  if (!inn.length && !out.length) return <span className="muted">–</span>;
  return <span className="nowrap">{inn.join(' + ') || '0'} <span className="muted">→</span> {out.join(' + ') || '0'}</span>;
}

/** A query's tasks on their own, for pages that draw the graph between the summary and the tasks. */
export function QueryTasks({ cid, ctx, id }: { cid: string; ctx: string; id: number }) {
  return (
    <section className="panel">
      <div className="panel-head"><h2>All its tasks</h2></div>
      <div className="panel-body"><ScopeTaskSection cid={cid} ctx={ctx} query={id} what={`query ${id}`} /></div>
    </section>
  );
}

export function QuerySummary({ cid, ctx, id, title, onJob, onQuery, tasks = true }: { cid: string; ctx: string; id: number; title: string; onJob: (jobId: number) => void; onQuery?: (id: number) => void; tasks?: boolean }) {
  const st = useAsync((s) => api.query(cid, ctx, id, s), [cid, ctx, id]);
  const tst = useAsync((s) => api.queryTime(cid, ctx, id, s), [cid, ctx, id]);
  const wst = useAsync((s) => api.stageWhy(cid, ctx, { query: id }, s), [cid, ctx, id]);
  const why = new Map((wst.data?.stages ?? []).map((w) => [keyOf(w), w]));
  if (!st.data) return <div className="panel panel-body muted small">{st.error ? `Could not load the query: ${st.error.message}` : 'Loading the query…'}</div>;
  const q = st.data.query;
  const stages = (st.data.stages ?? []) as Stage[];
  const jobs = (st.data.jobs ?? []) as Job[];
  const rows = jobs.map((j) => ({
    id: String(j.spark_job_id), label: `Job ${j.spark_job_id}`, sub: j.description ?? '', status: (j as unknown as { status?: string }).status ?? j.result,
    dur: j.duration_ms, stages: stages.filter((s) => s.spark_job_id === j.spark_job_id), tasks: sum(stages.filter((s) => s.spark_job_id === j.spark_job_id).map((s) => s.tasks)),
  })).sort((a, b) => (b.dur ?? 0) - (a.dur ?? 0));
  return (
    <section className="panel">
      <div className="panel-head"><h2>{title}</h2><span className="muted small">all {fmtNum(jobs.length)} Spark jobs and {fmtNum(stages.length)} stages together</span></div>
      <div className="panel-body stack">
        {tst.data && <QueryTimeBar t={tst.data} onQuery={(qid) => onQuery?.(qid)} />}
        {tst.data && <QuerySteps t={tst.data} id={id} onJob={onJob} onQuery={(qid) => onQuery?.(qid)} />}
        <WhyLine dur={q?.duration_ms ?? null} stages={stages} why={why} />
        <TablesRead cid={cid} scope={{ ctx, query: id }} embedded />
        <VCards cards={levelCards(stages, 'query', why)} compact />
        <Numbers dur={q?.duration_ms ?? null} kids={jobs.length} kidWord="Jobs" stages={stages} />
        <h3 className="h-sub" style={{ margin: 0 }}>Its {fmtNum(jobs.length)} {jobs.length === 1 ? 'job' : 'jobs'} <span className="muted small">each job's time, wait for cores, stages, tasks and shuffle · click a header to sort, a job for its stages and executors</span></h3>
        <ChildTable rows={rows} word="Job" why={why} onPick={(jid) => onJob(Number(jid))} />
        {tasks && <h3 className="h-sub" style={{ margin: '8px 0 0' }}>All its tasks</h3>}
        {tasks && <ScopeTaskSection cid={cid} ctx={ctx} query={id} what={`query ${id}`} />}
      </div>
    </section>
  );
}

/** Job level: everything over its stages, then one row per stage (click one for the task level). */
export function JobSummary({ cid, ctx, jobId, onStage }: { cid: string; ctx: string; jobId: number; onStage: (stageId: number, attempt: number) => void }) {
  const st = useAsync(
    async (s) => {
      const [stg, job] = await Promise.all([
        api.dataset<Stage>(cid, 'stages', { spark_context_id: ctx, spark_job_id: jobId, limit: 2000 }, s),
        api.dataset<Job>(cid, 'spark_jobs', { spark_context_id: ctx, spark_job_id: jobId, limit: 1 }, s),
      ]);
      return { stages: stg.rows, job: job.rows[0] ?? null };
    },
    [cid, ctx, jobId],
  );
  const wst = useAsync((s) => api.stageWhy(cid, ctx, { job: jobId }, s), [cid, ctx, jobId]);
  const why = new Map((wst.data?.stages ?? []).map((w) => [keyOf(w), w]));
  if (!st.data) return <div className="panel panel-body muted small">{st.error ? `Could not load the job: ${st.error.message}` : 'Loading the job…'}</div>;
  const { stages, job } = st.data;
  const rows = stages.map((s) => ({
    id: `${s.stage_id}:${s.stage_attempt}`, label: stageName(s), sub: s.stage_name ?? '', status: s.status, dur: s.duration_ms, stages: [s], tasks: s.tasks,
  })).sort((a, b) => (b.dur ?? 0) - (a.dur ?? 0));
  return (
    <section className="panel">
      <div className="panel-head"><h2>Job {jobId}</h2><span className="muted small">{job?.description ? truncate(job.description, 80) : `${fmtNum(stages.length)} stages`}</span></div>
      <div className="panel-body stack">
        <WhyLine dur={job?.duration_ms ?? null} stages={stages} why={why} />
        <VCards cards={levelCards(stages, 'job', why)} compact />
        <Numbers dur={job?.duration_ms ?? null} kids={stages.length} kidWord="Stages" stages={stages} />
        <h3 className="h-sub" style={{ margin: 0 }}>Its {fmtNum(stages.length)} {stages.length === 1 ? 'stage' : 'stages'} <span className="muted small">each stage's time, wait for cores, tasks, task spread and shuffle · click a header to sort, a stage for its tasks and executors</span></h3>
        <ChildTable rows={rows} word="Stage" why={why} onPick={(k) => { const [sid, att] = k.split(':').map(Number); onStage(sid, att); }} />
        <h3 className="h-sub" style={{ margin: '8px 0 0' }}>All its tasks</h3>
        <ScopeTaskSection cid={cid} ctx={ctx} job={jobId} what={`job ${jobId}`} />
      </div>
    </section>
  );
}
