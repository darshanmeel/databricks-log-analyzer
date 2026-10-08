// A run step by step: its queries (and Spark jobs without SQL) in the order they started, what each did, how long a
// stage of it waited for a free core and how long its stages ran tasks, its data, on the run's own clock. Open one for
// its stages, each with the spread of its tasks (time and data per task), shuffle, spill, GC / CPU and how its tasks
// were split over the executors. Stretches of a minute or more with no Spark work of the run are rows of their own.
import { Fragment, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, type RunStepGroup, type RunStepStage, type RunSteps } from '../api';
import { useAsync } from '../hooks';
import { fmtBytes, fmtDuration, fmtNum, fmtPct, fmtTime, truncate } from '../format';
import { to } from '../links';
import { Panel, StatusBadge } from './ui';
import { SIGN } from './VCards';

type Sort = 'order' | 'wait' | 'took' | 'read' | 'spill';
type Row = { kind: 'group'; g: RunStepGroup } | { kind: 'gap'; a: number; b: number };

const b = (v: number | null | undefined) => (v ? fmtBytes(v) : '–');
const d = (v: number | null | undefined) => (v != null ? fmtDuration(v) : '–');
const n = (v: number | null | undefined) => (v != null ? fmtNum(v) : '–');
const bigWait = (w: number, of: number) => w >= 2000 && w >= 0.2 * of;

/** What a query did, from its plan: what it read and wrote, and which MERGE step it was. */
export function whatItDid(g: RunStepGroup): string {
  const ops = g.operators ?? [];
  const desc = (g.description ?? '').replace(/\s+/g, ' ');
  const bits: string[] = [];
  if (ops.some((o) => o.includes('JDBCRelation'))) bits.push('reads the source database over JDBC');
  // a stream's checkpoint files are not tables
  const ckpt = (g.tables_read ?? []).some((t) => /checkpoint|\.parquet$|\.json$/.test(t));
  const reads = (g.tables_read ?? []).filter((t) => !t.includes('_delta_log') && !/checkpoint|\.parquet$|\.json$/.test(t));
  if (ckpt && !reads.length) bits.push('reads the stream checkpoint');
  if (reads.length) bits.push(`reads ${reads.slice(0, 2).join(', ')}`);
  else if ((g.tables_read ?? []).length) bits.push("reads a table's Delta log");
  const merge = desc.match(/MERGE operation - ([^·]+)$/);
  if (merge) bits.push(`MERGE: ${merge[1].split(' - ').pop()!.trim()}`);
  else if (desc.includes('MERGE')) bits.push('MERGE');
  if ((g.tables_written ?? []).length || ops.some((o) => o.includes('WriteIntoDelta'))) bits.push(`writes ${(g.tables_written ?? []).slice(0, 2).join(', ') || 'a Delta table'}`);
  if (ops.some((o) => o.includes('SortMergeJoin'))) bits.push('sort-merge join');
  else if (ops.some((o) => o.includes('BroadcastHashJoin'))) bits.push('broadcast join');
  return bits.join(' · ');
}

export function RunStepsPanel({ cid, run }: { cid: string; run: string }) {
  const st = useAsync((s) => api.runSteps(cid, run, s), [cid, run]);
  if (st.loading && !st.data) return <div className="panel panel-body muted small">Loading the run step by step…</div>;
  if (st.error || !st.data || !st.data.groups.length || st.data.start === null || st.data.end === null) return null;
  return <Steps cid={cid} d={st.data} />;
}

function Steps({ cid, d: data }: { cid: string; d: RunSteps }) {
  const [sort, setSort] = useState<Sort>('order');
  const key = (g: RunStepGroup) => `${g.kind}:${g.ctx}:${g.id}`;
  // the long ones open at first: they are where the time went
  const [open, setOpen] = useState<Set<string>>(() => new Set(
    [...data.groups].sort((x, y) => y.end - y.start - (x.end - x.start)).filter((g) => g.end - g.start >= 60_000).slice(0, 3).map(key)));
  const t0 = data.start!, t1 = data.end!;
  const span = Math.max(1, t1 - t0);
  const pct = (v: number) => `${Math.min(100, Math.max(0, ((v - t0) / span) * 100))}%`;
  const width = (a: number, e: number) => `${Math.max(0.3, ((Math.min(e, t1) - Math.max(a, t0)) / span) * 100)}%`;
  const total = data.total_ms ?? span;
  const rows: Row[] = useMemo(() => {
    const val: Record<Sort, (g: RunStepGroup) => number> = {
      order: (g) => -g.start, wait: (g) => g.waiting_ms, took: (g) => g.end - g.start,
      read: (g) => (g.input_bytes ?? 0) + (g.shuffle_read ?? 0), spill: (g) => g.disk_spill ?? 0,
    };
    const out: Row[] = [...data.groups].sort((x, y) => val[sort](y) - val[sort](x)).map((g) => ({ kind: 'group', g }));
    if (sort === 'order') {
      for (const [a, e] of data.gaps) out.push({ kind: 'gap', a, b: e });
      out.sort((x, y) => (x.kind === 'group' ? x.g.start : x.a) - (y.kind === 'group' ? y.g.start : y.a));
    }
    return out;
  }, [data, sort]);
  const flip = (k: string) => setOpen((o) => { const s = new Set(o); if (s.has(k)) s.delete(k); else s.add(k); return s; });
  const parts: [string, number, string][] = [
    ['waiting for a free core', data.waiting_ms, 'var(--wait)'],
    ['running tasks', data.running_ms, 'var(--series-1)'],
    ['no Spark work (its code outside Spark)', data.outside_ms ?? 0, 'var(--series-3)'],
  ];
  const COLS = 13;
  const lane = (stages: RunStepStage[]) => stages.map((s) => (
    <Fragment key={`${s.stage_id}.${s.stage_attempt}`}>
      {s.wait_ms > 0 && <span className="qs-wait" style={{ left: pct(s.submitted), width: width(s.submitted, s.first_task) }} />}
      <span className="qs-run" style={{ left: pct(s.first_task), width: width(s.first_task, s.end) }} />
    </Fragment>
  ));

  return (
    <Panel title="What the run did, step by step"
      note="Each query of the run (and Spark job without SQL) in the order it started. Waiting: a stage of it was submitted but no core was free and none of its stages ran. Open a query for its stages and their tasks.">
      <div className="vsplit-bar" style={{ height: 14 }}>
        {parts.map(([k, v, c]) => (v > 0 ? <span key={k} style={{ width: `${(v / Math.max(1, total)) * 100}%`, background: c }} title={`${k}: ${fmtDuration(v)}`} /> : null))}
      </div>
      <div className="stage-time-legend" style={{ marginBottom: 10 }}>
        {parts.filter((p) => p[1] > 0).map(([k, v, c]) => (
          <span key={k}><i style={{ background: c }} /><b>{fmtDuration(v)}</b> {k} <span className="muted">({fmtPct(v / Math.max(1, total), 0)})</span></span>
        ))}
      </div>
      <div className="row small" style={{ gap: 8, alignItems: 'center', marginBottom: 6 }}>
        <span className="muted">Order:</span>
        <div className="seg" role="group" aria-label="Order">
          {([['order', 'As they ran'], ['wait', 'Most waiting'], ['took', 'Longest'], ['read', 'Most data'], ['spill', 'Most spill']] as [Sort, string][]).map(([k, l]) => (
            <button key={k} className={sort === k ? 'on' : ''} aria-pressed={sort === k} onClick={() => setSort(k)}>{l}</button>
          ))}
        </div>
        <span className="muted">{fmtNum(data.groups.length)} queries and jobs · <i className="qs-key wait" /> waiting <i className="qs-key run" /> running</span>
        <span className="spacer" style={{ flex: 1 }} />
        <button className="btn small ghost" onClick={() => setOpen(new Set(data.groups.map(key)))}>Open all</button>
        <button className="btn small ghost" onClick={() => setOpen(new Set())}>Close all</button>
      </div>
      <div className="table-wrap">
        <table className="data qsteps-table run-steps">
          <thead>
            <tr>
              <th>Step</th><th>Started</th><th className="num">Took</th><th className="num">Waited for cores</th><th className="num">Ran</th>
              <th className="num">Stages</th><th className="num">Tasks</th><th className="num">Rows in</th><th className="num">Read</th>
              <th className="num">Wrote</th><th className="num">Shuffle read</th><th className="num">Disk spill</th>
              <th style={{ minWidth: 220 }}>{fmtTime(t0)} → {fmtTime(t1)}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              if (r.kind === 'gap')
                return (
                  <tr key={`gap${r.a}`} className="gap-row">
                    <td className="wait-text">No Spark work of this run: its code outside Spark, or waiting on something outside it</td>
                    <td className="mono small">{fmtTime(r.a)}</td>
                    <td className="num wait-text">{fmtDuration(r.b - r.a)}</td><td colSpan={9} />
                    <td><div className="qs-lane"><span className="qs-gap" style={{ left: pct(r.a), width: width(r.a, r.b) }} /></div></td>
                  </tr>
                );
              const g = r.g;
              const k = key(g);
              const took = g.end - g.start;
              const isOpen = open.has(k);
              const what = whatItDid(g);
              return (
                <Fragment key={k}>
                  <tr className="clickable" onClick={() => flip(k)} aria-expanded={isOpen}>
                    <td style={{ minWidth: 300, whiteSpace: 'normal' }}>
                      <span className="muted" aria-hidden>{isOpen ? '▾ ' : '▸ '}</span>
                      {g.kind === 'query' && g.id !== null ? (
                        <Link to={to.query(cid, g.ctx, g.id)} onClick={(e) => e.stopPropagation()}><b>Query {g.id}</b></Link>
                      ) : <b>Job {g.id ?? '?'}</b>}
                      {g.status && g.status !== 'succeeded' && g.status !== 'completed' ? <> <StatusBadge status={g.status} /></> : null}
                      {what && <span> · {what}</span>}
                      <div className="muted small">{truncate((g.description ?? '').replace(/\s+/g, ' '), 110)}</div>
                    </td>
                    <td className="mono small">{fmtTime(g.start)}</td>
                    <td className="num">{fmtDuration(took)}</td>
                    <td className={`num ${bigWait(g.waiting_ms, took) ? 'wait-text' : 'muted'}`} style={{ fontWeight: bigWait(g.waiting_ms, took) ? 600 : undefined }}>
                      {bigWait(g.waiting_ms, took) && <span aria-hidden>{SIGN.wait} </span>}{g.waiting_ms >= 500 ? fmtDuration(g.waiting_ms) : '–'}
                    </td>
                    <td className="num">{fmtDuration(g.running_ms)}</td>
                    <td className="num">{fmtNum(g.stages.length)}</td>
                    <td className="num">{n(g.tasks)}{g.failed_tasks ? <div className="small bad">{fmtNum(g.failed_tasks)} failed</div> : null}</td>
                    <td className="num">{n(g.input_records)}</td>
                    <td className="num">{b((g.input_bytes ?? 0) + (g.shuffle_read ?? 0))}</td>
                    <td className="num">{b(g.output_bytes)}</td>
                    <td className="num" style={{ color: g.shuffle_read ? 'var(--shuf)' : undefined }}>{b(g.shuffle_read)}</td>
                    <td className="num" style={{ color: g.disk_spill ? 'var(--spill)' : undefined }}>{b(g.disk_spill)}</td>
                    <td><div className="qs-lane" title={`${fmtTime(g.start)} → ${fmtTime(g.end)}: waited ${fmtDuration(g.waiting_ms)}, ran ${fmtDuration(g.running_ms)}`}>
                      <span className="qs-span" style={{ left: pct(g.start), width: width(g.start, g.end) }} />{lane(g.stages)}
                    </div></td>
                  </tr>
                  {isOpen && (
                    <tr className="stage-detail-row">
                      <td colSpan={COLS}><StageTable cid={cid} g={g} /></td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

/** A query's stages, each from every one of its tasks: time and data per task, shuffle, spill, GC / CPU, executors. */
function StageTable({ cid, g }: { cid: string; g: RunStepGroup }) {
  return (
    <div className="table-wrap">
      <table className="table compact level-table stage-steps">
        <thead>
          <tr className="group-head">
            <th colSpan={5} />
            <th colSpan={5} className="grp">Time per task</th>
            <th colSpan={4} className="grp">Read per task (input + shuffle read)</th>
            <th colSpan={3} className="grp">Shuffle · output</th>
            <th colSpan={3} className="grp">Spill · memory</th>
            <th colSpan={3} className="grp">GC · CPU · shuffle wait</th>
          </tr>
          <tr>
            <th>Stage</th><th className="num">Tasks</th><th>Submitted</th><th className="num">Waited for cores</th><th className="num">Ran</th>
            <th className="num">Min</th><th className="num">p10</th><th className="num">Median</th><th className="num">p90</th><th className="num">Max</th>
            <th className="num">Total</th><th className="num">p10</th><th className="num">Median</th><th className="num">p90 · max</th>
            <th className="num">Shuffle read</th><th className="num">Shuffle write</th><th className="num">Output</th>
            <th className="num">Memory</th><th className="num">Disk</th><th className="num">Peak per task</th>
            <th className="num">GC share</th><th className="num">CPU share</th><th className="num">Shuffle wait</th>
          </tr>
        </thead>
        <tbody>
          {g.stages.map((s) => {
            const exTot = (s.by_exec ?? []).reduce((a, e) => a + (e.task_ms ?? 0), 0) || 1;
            return (
              <Fragment key={`${s.stage_id}.${s.stage_attempt}`}>
                <tr>
                  <td>
                    <Link to={to.stages(cid, g.ctx, s.stage_id, s.stage_attempt)}>Stage {s.stage_id}{s.stage_attempt ? `.${s.stage_attempt}` : ''}</Link>
                    <div className="muted small">job {s.spark_job_id ?? '–'}{s.status && s.status !== 'succeeded' ? <> · <StatusBadge status={s.status} /></> : null}</div>
                  </td>
                  <td className="num">{n(s.tasks)}{s.failed_tasks ? <div className="small bad">{fmtNum(s.failed_tasks)} failed</div> : null}</td>
                  <td className="mono small">{fmtTime(s.submitted)}</td>
                  <td className={`num ${bigWait(s.wait_ms, s.wait_ms + s.run_ms) ? 'wait-text' : 'muted'}`} style={{ fontWeight: bigWait(s.wait_ms, s.wait_ms + s.run_ms) ? 600 : undefined }}>{s.wait_ms >= 500 ? fmtDuration(s.wait_ms) : '–'}</td>
                  <td className="num">{fmtDuration(s.run_ms)}</td>
                  <td className="num muted">{d(s.min_task_ms)}</td>
                  <td className="num muted">{d(s.p10_task_ms)}</td>
                  <td className="num"><b>{d(s.p50_task_ms)}</b></td>
                  <td className="num">{d(s.p90_task_ms)}</td>
                  <td className="num">{d(s.max_task_ms)}</td>
                  <td className="num">{b((s.input_bytes ?? 0) + (s.shuffle_read ?? 0))}</td>
                  <td className="num muted">{b(s.p10_task_bytes_in)}</td>
                  <td className="num"><b>{b(s.p50_task_bytes_in)}</b></td>
                  <td className="num">{b(s.p90_task_bytes_in)} · {b(s.max_task_bytes_in)}</td>
                  <td className="num" style={{ color: s.shuffle_read ? 'var(--shuf)' : undefined }}>{b(s.shuffle_read)}</td>
                  <td className="num" style={{ color: s.shuffle_write ? 'var(--shuf)' : undefined }}>{b(s.shuffle_write)}</td>
                  <td className="num">{b(s.output_bytes)}</td>
                  <td className="num muted">{b(s.mem_spill)}</td>
                  <td className="num" style={{ color: s.disk_spill ? 'var(--spill)' : undefined }}>{b(s.disk_spill)}</td>
                  <td className="num">{b(s.max_peak_mem)}</td>
                  <td className="num">{s.gc_share != null ? fmtPct(s.gc_share, 0) : '–'}</td>
                  <td className="num">{s.cpu_share != null ? fmtPct(s.cpu_share, 0) : '–'}</td>
                  <td className="num">{s.fetch_wait_ms ? fmtDuration(s.fetch_wait_ms) : '–'}</td>
                </tr>
                {(s.by_exec ?? []).length > 0 && (
                  <tr className="exec-split-row">
                    <td />
                    <td colSpan={22} className="small muted">
                      {(s.by_exec ?? []).length} {(s.by_exec ?? []).length === 1 ? 'executor' : 'executors'}:{' '}
                      {(s.by_exec ?? []).map((e, i) => (
                        <span key={e.executor_id}>
                          {i ? ' · ' : ''}exec {e.executor_id}: {fmtNum(e.tasks)} tasks ({fmtPct((e.task_ms ?? 0) / exTot, 0)} of the work
                          {e.bytes_in ? `, ${fmtBytes(e.bytes_in)} read` : ''}{e.disk_spill ? `, ${fmtBytes(e.disk_spill)} spilled` : ''}
                          {e.failed ? <span className="bad">, {fmtNum(e.failed)} failed</span> : null})
                        </span>
                      ))}
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
