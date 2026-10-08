// Revision 18: find the one to open. The slowest, longest-waiting, most spilling, biggest-shuffle or biggest-read
// stages, Spark jobs, SQL queries or tasks of the cluster (or of one run), with a search by id or text; each row opens
// it (in its own run).
import { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, waitOf, type RunRow, type TopKind, type TopBy, type TopRow } from '../api';
import { useAsync } from '../hooks';
import { fmtBytes, fmtDuration, fmtNum, fmtTime, truncate } from '../format';
import { to } from '../links';
import { runId, runName, usualX } from '../runName';
import { Panel } from './ui';
import { taskReadLevel } from '../thresholds';

/** Runs are ranked here from the runs list; the other kinds by /top. */
type Kind = TopKind | 'runs';
type By = TopBy | 'usual' | 'failed';
const KINDS: [Kind, string][] = [['runs', 'Runs'], ['stages', 'Stages'], ['jobs', 'Jobs'], ['queries', 'Queries'], ['tasks', 'Tasks']];
const ALL: Kind[] = ['runs', 'stages', 'jobs', 'queries', 'tasks'];
const BYS: [By, string, Kind[]][] = [
  ['duration', 'Slowest (waited + ran)', ALL],
  ['ran', 'Longest processing', ALL],
  ['usual', 'Slowest against usual', ['runs']],
  ['wait', 'Waited longest for cores', ALL],
  ['failed', 'Failed or retried', ['runs']],
  ['spill', 'Spilled most', ALL],
  ['shuffle', 'Most shuffle', ALL],
  ['read', 'Read most from files', ALL],
  ['task_read', 'Biggest task file read', ALL],
  ['task_shuffle', 'Biggest task shuffle read', ALL],
  ['tasks', 'Most tasks', ['runs', 'stages', 'jobs', 'queries']],
  ['skew', 'Most skewed', ['stages', 'jobs', 'queries']],
];
const RUN_VAL: Partial<Record<By, (r: RunRow) => number>> = {
  duration: (r) => r.duration_ms ?? -1,
  ran: (r) => r.running_ms ?? -1,
  usual: (r) => usualX(r) ?? -1,
  wait: (r) => (r.waiting_ms == null && r.queued_full_ms == null ? -1 : waitOf(r)),
  failed: (r) => (r.status === 'failed' ? 1e15 : 0) + (r.failed_stages ?? 0) * 1e6 + (r.failed_tasks ?? 0),
  spill: (r) => r.disk_spill ?? -1,
  shuffle: (r) => (r.shuffle_read ?? 0) + (r.shuffle_write ?? 0),
  read: (r) => r.input_bytes ?? -1,
  task_read: (r) => r.max_task_input ?? -1,
  task_shuffle: (r) => r.max_task_shuffle ?? -1,
  tasks: (r) => r.tasks ?? -1,
};

/** A link that also opens the item's run. */
export const inRun = (href: string, run: string | null | undefined) =>
  run ? `${href}${href.includes('?') ? '&' : '?'}run=${encodeURIComponent(run)}` : href;

export function TopFinder({ cid, run, runs }: { cid: string; run?: string | null; runs?: RunRow[] }) {
  const withRuns = !run && (runs?.length ?? 0) > 1;
  const [kind, setKind] = useState<Kind>(withRuns ? 'runs' : 'stages');
  const [by, setBy] = useState<By>('duration');
  const [text, setText] = useState('');
  const [q, setQ] = useState('');
  const [limit, setLimit] = useState(10);
  const isRuns = kind === 'runs';
  const st = useAsync((s) => (isRuns ? Promise.resolve(null) : api.top(cid, { kind: kind as TopKind, by: by as TopBy, run: run ?? undefined, limit, q: q || undefined }, s)),
    [cid, kind, by, run, limit, q]);
  const names = new Map((runs ?? []).map((r) => [r.run_key, runName(r)]));
  const kinds = KINDS.filter((k) => k[0] !== 'runs' || withRuns);
  const bys = BYS.filter((b) => b[2].includes(kind));
  const pickKind = (k: Kind) => { setKind(k); if (!BYS.find((b) => b[0] === by)?.[2].includes(k)) setBy('duration'); };
  // only rows of what is picked now: while the next list loads, the previous one (another kind) must not show under these headings
  const d = !isRuns && st.data && st.data.kind === kind && st.data.by === by ? st.data : null;
  const rows = d?.rows ?? [];
  const runRows = useMemo(() => {
    if (!isRuns) return { rows: [] as RunRow[], total: 0 };
    const needle = q.toLowerCase();
    const v = RUN_VAL[by] ?? RUN_VAL.duration!;
    const all = (runs ?? []).filter((r) => !needle || [runName(r), r.label, r.run_key, r.job_name, r.notebook_path, r.task_run_id, r.databricks_run_id]
      .some((x) => x && x.toLowerCase().includes(needle))).filter((r) => v(r) > 0);
    return { rows: all.sort((a, b) => v(b) - v(a)).slice(0, limit), total: all.length };
  }, [isRuns, runs, by, q, limit]);
  const b = (v: number | null | undefined) => (v ? fmtBytes(v) : '–');
  const showRun = !run;
  const task = kind === 'tasks';
  const tookOf = (r: TopRow) => (task ? r.task_ms : r.duration_ms) ?? 0;
  const maxT = Math.max(1, ...rows.map((r) => tookOf(r) + (task ? r.wait_ms ?? 0 : 0)));
  const what = KINDS.find((k) => k[0] === kind)![1].toLowerCase();
  const total = isRuns ? runRows.total : d?.total ?? 0;
  const shownN = isRuns ? runRows.rows.length : rows.length;
  const ready = isRuns || !!d;
  return (
    <Panel title={run ? 'Find in this run' : 'Find across all runs'} className="ro-find-panel"
      note={`The top ${what} by one measure${run ? ' in this run' : ', any run'}. Click one to open it.`}
      actions={
        <div className="ro-find-controls small">
          <div className="seg ro-pills" role="group" aria-label="What">
            {kinds.map(([k, l]) => <button key={k} className={kind === k ? 'on' : ''} aria-pressed={kind === k} onClick={() => pickKind(k)}>{l}</button>)}
          </div>
          <div className="seg ro-pills" role="group" aria-label="By">
            {bys.map(([k, l]) => <button key={k} className={by === k ? 'on' : ''} aria-pressed={by === k} onClick={() => setBy(k)}>{l}</button>)}
          </div>
          <form onSubmit={(e) => { e.preventDefault(); setQ(text.trim()); }} className="row" style={{ gap: 4 }}>
            <input className="input" style={{ width: 220, height: 28 }} placeholder={task ? 'stage id or executor' : isRuns ? 'name, table or run id' : 'id or text (name, table…)'} value={text}
              onChange={(e) => { setText(e.target.value); if (!e.target.value) setQ(''); }} aria-label="Search" />
            <button className="btn small" type="submit">Find</button>
          </form>
        </div>
      }>
      <div className="stack" style={{ gap: 8 }}>
        {st.error ? <p className="small bad">Could not load: {st.error.message}</p> : null}
        {isRuns ? <RunsFound cid={cid} rows={runRows.rows} q={q} /> : (
        <div style={{ overflowX: 'auto' }}>
          <table className="ro-find">
            <thead>
              <tr>
                <th>{task ? 'Task' : KINDS.find((k) => k[0] === kind)![1].slice(0, -1).replace('Querie', 'Query')}</th>
                {task && <th>Stage</th>}
                {showRun && <th>Run</th>}
                <th style={{ width: '30%', minWidth: 180 }}>{task ? 'Took · waited to start | ran' : 'Took · waited for cores | ran'}</th>
                {!task && <th className="num">Tasks</th>}
                {task && <th>Executor</th>}
                <th className="num" title={READ_TIP}>Read (files){task ? '' : <div className="th-sub">biggest task</div>}</th>
                <th className="num" title={READ_TIP}>Shuffle read{task ? '' : <div className="th-sub">biggest task</div>}</th>
                <th className="num">Shuffle write</th>
                <th className="num">Disk spill</th>
                {!task && kind !== 'queries' && <th className="num">Skew</th>}
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => <Row key={keyOf(kind, r)} r={r} kind={kind} cid={cid} showRun={showRun} runName={r.run_key ? names.get(r.run_key) ?? r.run_key : null} b={b} max={maxT} />)}
              {!rows.length && !st.loading && <tr><td colSpan={11} className="muted small">{q ? `Nothing matches “${q}”.` : 'Nothing here.'}</td></tr>}
            </tbody>
          </table>
        </div>
        )}
        <div className="row small" style={{ gap: 8, alignItems: 'center' }}>
          <span className="muted">{ready ? `${fmtNum(shownN)} of ${fmtNum(total)} ${what}` : st.loading ? 'Loading…' : ''}</span>
          {ready && total > shownN && limit < 100 && <button className="btn small ghost" onClick={() => setLimit(limit < 25 ? 25 : 100)}>Show {limit < 25 ? 25 : 100}</button>}
          {limit > 10 && <button className="btn small ghost" onClick={() => setLimit(10)}>Show 10</button>}
        </div>
      </div>
    </Panel>
  );
}

const shortD = (ms: number) => fmtDuration(ms);

/** Took as one bar: the hatched part waited for a free core, the solid part ran tasks. */
export function WaitRanBar({ took: t, wait, ran, max }: { took: number; wait: number; ran: number; max: number }) {
  const w = Math.max(4, (t / Math.max(1, max)) * 100);
  const ws = Math.min(1, wait / Math.max(1, t));
  const rs = Math.min(1 - ws, ran / Math.max(1, t));
  return (
    <div className="ro-wr">
      <div className="ro-wr-bar" style={{ width: `${w}%` }}>
        <span className="ro-wr-wait" style={{ width: `${ws * 100}%` }} />
        <span className="ro-wr-run" style={{ width: `${rs * 100}%` }} />
      </div>
      <div className="ro-wr-nums"><b>{shortD(t)}</b><span className="muted">{wait >= 500 ? shortD(wait) : '–'} | {shortD(ran)}</span></div>
    </div>
  );
}

const READ_TIP = 'Read: from files (tables, paths). Shuffle read: data another stage shuffled to this one. One task reading over 128 MB of either is a warning, over 256 MB critical.';

/** One task's read, marked over 128 MB (warning) and over 256 MB (critical). */
function Mark({ v }: { v: number | null | undefined }) {
  const lv = taskReadLevel(v);
  return (
    <>
      {v ? <span className={lv ? `task-read ${lv}` : ''} title={lv === 'crit' ? 'Over 256 MB in one task: critical' : lv === 'warn' ? 'Over 128 MB in one task: warning' : undefined}>{fmtBytes(v)}</span> : '–'}
      {lv ? <div className={`small task-read-note ${lv}`}>{lv === 'crit' ? 'over 256 MB' : 'over 128 MB'}</div> : null}
    </>
  );
}

/** A read: the total, and under it the biggest one task read, marked. A task's own read is marked itself
 * (task undefined). */
function ReadCell({ total, task, shuffle }: { total: number | null | undefined; task: number | null | undefined; shuffle?: boolean }) {
  const color = shuffle && total ? 'var(--shuf)' : undefined;
  if (task === undefined) return <td className="num" style={{ color }}><Mark v={total} /></td>;
  return (
    <td className="num">
      <span style={{ color }}>{total ? fmtBytes(total) : '–'}</span>
      {total && task ? <div className="small muted task-max">task <Mark v={task} /></div> : null}
    </td>
  );
}

const keyOf = (kind: TopKind, r: TopRow) =>
  `${r.spark_context_id}.${kind === 'jobs' ? r.spark_job_id : kind === 'queries' ? r.sql_execution_id : `${r.stage_id}.${r.stage_attempt}.${r.task_id ?? ''}`}`;

function Row({ r, kind, cid, showRun, runName, b, max }: { r: TopRow; kind: TopKind; cid: string; showRun: boolean; runName: string | null; b: (v: number | null | undefined) => string; max: number }) {
  const ctx = r.spark_context_id;
  const stageHref = inRun(to.stages(cid, ctx, r.stage_id ?? null, r.stage_attempt ?? 0), r.run_key);
  const href = kind === 'jobs' ? inRun(to.hierarchy(cid, { ctx, job: r.spark_job_id ?? null }), r.run_key)
    : kind === 'queries' ? inRun(to.query(cid, ctx, r.sql_execution_id ?? 0), r.run_key) : stageHref;
  const name = kind === 'jobs' ? `Job ${r.spark_job_id}` : kind === 'queries' ? `Query ${r.sql_execution_id}`
    : kind === 'stages' ? `Stage ${r.stage_id}${r.stage_attempt ? `.${r.stage_attempt}` : ''}` : `Task ${r.task_id}`;
  const bad = r.failed || r.status === 'failed' || r.result === 'failed' || (r.result ?? '').toLowerCase().includes('fail');
  const skew = r.data_skew ?? r.skew ?? r.max_stage_skew ?? null;
  const parent = [r.spark_job_id !== null && r.spark_job_id !== undefined && kind !== 'jobs' ? `job ${r.spark_job_id}` : null,
    r.sql_execution_id !== null && r.sql_execution_id !== undefined && kind !== 'queries' ? `query ${r.sql_execution_id}` : null].filter(Boolean).join(' · ');
  return (
    <tr>
      <td>
        <Link to={href}><b>{name}</b></Link>{bad ? <span className="small bad"> failed</span> : null}
        {r.label && !r.label.includes('<unknown>') ? <div className="muted small">{truncate(r.label, 70)}</div> : null}
        {parent && kind !== 'tasks' ? <div className="muted small">{parent}</div> : null}
      </td>
      {kind === 'tasks' && <td><Link to={stageHref}>Stage {r.stage_id}{r.stage_attempt ? `.${r.stage_attempt}` : ''}</Link><div className="muted small">{parent} · {fmtTime(r.launch_time ?? null)}</div></td>}
      {showRun && <td className="small">{runName ? <Link to={inRun(to.overview(cid), r.run_key)}>{truncate(runName, 40)}</Link> : <span className="muted">–</span>}</td>}
      <td>{kind === 'tasks'
        ? <WaitRanBar took={(r.task_ms ?? 0) + (r.wait_ms ?? 0)} wait={r.wait_ms ?? 0} ran={r.task_ms ?? 0} max={max} />
        : <WaitRanBar took={r.duration_ms ?? 0} wait={Math.min(r.wait_ms ?? 0, r.duration_ms ?? 0)} ran={r.ran_ms ?? Math.max(0, (r.duration_ms ?? 0) - (r.wait_ms ?? 0))} max={max} />}</td>
      {kind !== 'tasks' && <td className="num">{fmtNum(r.tasks ?? null)}</td>}
      {kind === 'tasks' && <td className="mono small">exec {r.executor_id ?? '?'}</td>}
      <ReadCell total={r.input_bytes} task={kind === 'tasks' ? undefined : r.max_task_input} />
      <ReadCell total={r.shuffle_read} task={kind === 'tasks' ? undefined : r.max_task_shuffle} shuffle />
      <td className="num" style={{ color: r.shuffle_write ? 'var(--shuf)' : undefined }}>{b(r.shuffle_write)}</td>
      <td className="num" style={{ color: r.disk_spill ? 'var(--spill)' : undefined }}>{b(r.disk_spill)}</td>
      {kind !== 'tasks' && kind !== 'queries' && <td className="num">{(r.input_bytes ?? 0) + (r.shuffle_read ?? 0) < 1024 ** 2 ? <span className="muted">– under 1 MB</span> : skew ? `${Number(skew).toFixed(1)}×` : '–'}</td>}
    </tr>
  );
}

/** The top runs: open one to see it end to end. */
function RunsFound({ cid, rows, q }: { cid: string; rows: RunRow[]; q: string }) {
  const max = Math.max(1, ...rows.map((r) => r.duration_ms ?? 0));
  const b = (v: number | null | undefined) => (v ? fmtBytes(v) : '–');
  return (
    <div style={{ overflowX: 'auto' }}>
      <table className="ro-find">
        <thead>
          <tr>
            <th>Run</th><th>Started</th><th style={{ width: '28%', minWidth: 180 }}>Took · waited for cores | ran</th><th className="num">× usual</th><th>Status</th>
            <th className="num">Tasks</th>
            <th className="num" title={READ_TIP}>Read (files)<div className="th-sub">biggest task</div></th>
            <th className="num" title={READ_TIP}>Shuffle read<div className="th-sub">biggest task</div></th>
            <th className="num">Shuffle write</th><th className="num">Disk spill</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const x = usualX(r);
            const t = r.duration_ms ?? 0;
            const wait = Math.min(waitOf(r), t);
            return (
              <tr key={r.run_key}>
                <td>
                  <Link to={inRun(to.overview(cid), r.run_key)}><b>{truncate(runName(r), 60)}</b></Link>
                  <div className="muted small">{runId(r)}{r.typical_duration_ms && x !== null ? ` · usually ${fmtDuration(r.typical_duration_ms)}` : ''}</div>
                </td>
                <td className="mono small">{fmtTime(r.start_time)}</td>
                <td><WaitRanBar took={t} wait={wait} ran={r.running_ms ?? Math.max(0, t - wait)} max={max} /></td>
                <td className={`num ${(x ?? 0) >= 2 ? 'st-warn' : 'muted'}`} style={{ fontWeight: (x ?? 0) >= 2 ? 600 : undefined }}>{x !== null ? `${x.toFixed(1)}×` : '–'}</td>
                <td className="small" style={{ whiteSpace: 'nowrap' }}>
                  {r.status === 'failed' ? <span className="bad">failed</span> : r.status !== 'succeeded' ? <span className="st-warn">{r.status}</span>
                    : (r.failed_tasks ?? 0) || (r.failed_stages ?? 0) ? <span className="st-warn" style={{ whiteSpace: 'nowrap' }}>after retries</span> : <span className="muted">succeeded</span>}
                </td>
                <td className="num">{fmtNum(r.tasks)}{r.failed_tasks ? <div className="small bad">{fmtNum(r.failed_tasks)} failed</div> : null}</td>
                <ReadCell total={r.input_bytes} task={r.max_task_input ?? null} />
                <ReadCell total={r.shuffle_read} task={r.max_task_shuffle ?? null} shuffle />
                <td className="num" style={{ color: r.shuffle_write ? 'var(--shuf)' : undefined }}>{b(r.shuffle_write)}</td>
                <td className="num" style={{ color: r.disk_spill ? 'var(--spill)' : undefined }}>{b(r.disk_spill)}</td>
              </tr>
            );
          })}
          {!rows.length && <tr><td colSpan={11} className="muted small">{q ? `No run matches “${q}”.` : 'Nothing here.'}</td></tr>}
        </tbody>
      </table>
    </div>
  );
}
