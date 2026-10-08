// Revision 14: how many tasks read how much. Two stacked bars over the same five size bands: the share of tasks and
// the share of the data. "12 tasks of 256 MB or more read 70% of the data while 9,800 read under 10 MB" is the skew
// a median hides, and it is what decides whether more shuffle partitions or a skew fix helps.
import type { PlacementRow, SizeBands } from '../api';
import { fmtBytes, fmtDuration, fmtNum, fmtPct } from '../format';

export const BANDS = [
  { k: 'none', label: 'no data', color: 'var(--line)' },
  { k: 'lt10', label: '< 10 MB', color: 'color-mix(in srgb, var(--series-1) 35%, transparent)' },
  { k: '10_128', label: '10–128 MB', color: 'var(--series-1)' },
  { k: '128_256', label: '128–256 MB', color: 'var(--st-warn)' },
  { k: 'ge256', label: '≥ 256 MB', color: 'var(--st-crit)' },
] as const;

const n = (v: number | null | undefined) => (typeof v === 'number' ? v : 0);
const taskCount = (b: SizeBands, k: string) => n((b as Record<string, number | null | undefined>)[`tasks_${k}`]);
const byteCount = (b: SizeBands, k: string) => n((b as Record<string, number | null | undefined>)[`bytes_${k}`]);

export function hasBands(b: SizeBands | null | undefined): boolean {
  return !!b && BANDS.some((x) => taskCount(b, x.k) > 0);
}

function Bar({ parts, height = 10, title }: { parts: { v: number; color: string; label: string }[]; height?: number; title?: string }) {
  const tot = parts.reduce((a, p) => a + p.v, 0);
  if (!tot) return null;
  return (
    <div title={title} style={{ display: 'flex', height, borderRadius: 3, overflow: 'hidden', background: 'var(--panel-3, transparent)', minWidth: 60 }}>
      {parts.filter((p) => p.v > 0).map((p) => (
        <div key={p.label} style={{ width: `${(p.v / tot) * 100}%`, minWidth: 2, background: p.color }} title={`${p.label}: ${fmtPct(p.v / tot, 0)}`} />
      ))}
    </div>
  );
}

/** One line for the skew: "12 tasks ≥ 256 MB read 70% of the data". */
export function bandsLine(b: SizeBands): string | null {
  const tot = BANDS.reduce((a, x) => a + byteCount(b, x.k), 0);
  const tasks = BANDS.reduce((a, x) => a + taskCount(b, x.k), 0);
  if (!tot || !tasks) return null;
  for (const top of ['ge256', '128_256'] as const) {
    const t = taskCount(b, top), by = byteCount(b, top);
    if (t && by / tot >= 0.25) {
      const label = BANDS.find((x) => x.k === top)!.label;
      return `${fmtNum(t)} of ${fmtNum(tasks)} tasks (${fmtPct(t / tasks, t / tasks < 0.01 ? 1 : 0)}) read ${label} each, ${fmtPct(by / tot, 0)} of the data`;
    }
  }
  return null;
}

/** Full view (stage, query, run panels): tasks bar, data bar, and the counts. */
export function TaskSizes({ b, compact = false }: { b: SizeBands; compact?: boolean }) {
  if (!hasBands(b)) return <span className="muted">–</span>;
  const tasks = BANDS.map((x) => ({ v: taskCount(b, x.k), color: x.color, label: x.label }));
  const data = BANDS.map((x) => ({ v: byteCount(b, x.k), color: x.color, label: x.label }));
  const line = bandsLine(b);
  if (compact)
    return (
      <div style={{ minWidth: 110 }}>
        <Bar parts={tasks} height={6} title={`Tasks: ${tasks.filter((p) => p.v).map((p) => `${fmtNum(p.v)} ${p.label}`).join(', ')}`} />
        <div style={{ height: 2 }} />
        <Bar parts={data} height={6} title={`Data: ${data.filter((p) => p.v).map((p) => `${fmtBytes(p.v)} in tasks ${p.label}`).join(', ')}`} />
      </div>
    );
  return (
    <div className="stack" style={{ gap: 4, minWidth: 0 }}>
      <div className="row small" style={{ gap: 8, alignItems: 'center' }}>
        <span className="muted" style={{ width: 40 }}>tasks</span>
        <div style={{ flex: 1 }}><Bar parts={tasks} /></div>
      </div>
      <div className="row small" style={{ gap: 8, alignItems: 'center' }}>
        <span className="muted" style={{ width: 40 }}>data</span>
        <div style={{ flex: 1 }}><Bar parts={data} /></div>
      </div>
      <div className="row small" style={{ gap: 12, flexWrap: 'wrap' }}>
        {BANDS.map((x) => {
          const t = taskCount(b, x.k);
          if (!t) return null;
          return (
            <span key={x.k}>
              <span style={{ display: 'inline-block', width: 9, height: 9, background: x.color, borderRadius: 2, marginRight: 4 }} />
              {x.label}: <b>{fmtNum(t)}</b> {t === 1 ? 'task' : 'tasks'}
              {x.k !== 'none' && byteCount(b, x.k) ? <span className="muted"> · {fmtBytes(byteCount(b, x.k))}</span> : null}
            </span>
          );
        })}
      </div>
      {line && <div className="small"><b className={line.includes('≥ 256') ? 'st-warn' : ''}>{line}.</b></div>}
    </div>
  );
}

/** Where a stage's tasks ran: per executor the size bands, the data and the time. */
export function TaskPlacement({ rows }: { rows: PlacementRow[] }) {
  if (!rows.length) return null;
  const big = rows.reduce((a, r) => a + n(r.tasks_ge256) + n(r.tasks_128_256), 0);
  const allBytes = rows.reduce((a, r) => a + n(r.bytes_in), 0);
  const top = rows[0];
  const bigOn = rows.filter((r) => n(r.tasks_ge256) + n(r.tasks_128_256) > 0);
  const timeMax = Math.max(...rows.map((r) => n(r.task_ms)));
  const timeMed = [...rows.map((r) => n(r.task_ms))].sort((a, b) => a - b)[Math.floor(rows.length / 2)];
  const say = [
    big
      ? bigOn.length === 1
        ? `All ${fmtNum(big)} tasks of 128 MB or more ran on executor ${bigOn[0].executor_id}.`
        : `The ${fmtNum(big)} tasks of 128 MB or more ran on ${bigOn.length} of ${rows.length} executors.`
      : null,
    allBytes && rows.length > 1 && n(top.bytes_in) / allBytes >= Math.max(0.4, 2 / rows.length)
      ? `Executor ${top.executor_id} read ${fmtPct(n(top.bytes_in) / allBytes, 0)} of the stage's data.`
      : null,
    rows.length > 1 && timeMed && timeMax >= 3 * timeMed ? `The busiest executor worked ${(timeMax / timeMed).toFixed(1)}× the median executor's task time.` : null,
  ].filter(Boolean);
  return (
    <div className="stack" style={{ gap: 6 }}>
      <p className="small" style={{ margin: 0 }}>{say.length ? say.join(' ') : `Tasks were spread evenly over ${rows.length} executors.`}</p>
      <div className="table-wrap" style={{ overflowX: 'auto', maxHeight: 320, overflowY: 'auto' }}>
        <table className="table">
          <thead>
            <tr>
              <th>Executor</th>
              <th className="num">Tasks</th>
              <th>Tasks · data by size</th>
              <th className="num">Read</th>
              <th className="num">Biggest task</th>
              <th className="num">Task time</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.executor_id}>
                <td>exec {r.executor_id}{r.host ? <div className="muted small mono">{r.host}</div> : null}</td>
                <td className="num">
                  {fmtNum(r.tasks)}
                  {n(r.tasks_ge256) ? <div className="small st-crit">{fmtNum(r.tasks_ge256)} ≥ 256 MB</div> : null}
                </td>
                <td style={{ minWidth: 130 }}><TaskSizes b={r} compact /></td>
                <td className="num">{fmtBytes(r.bytes_in)}{allBytes ? <div className="muted small">{fmtPct(n(r.bytes_in) / allBytes, 0)}</div> : null}</td>
                <td className="num">{fmtBytes(r.max_bytes_in)}</td>
                <td className="num">{fmtDuration(r.task_ms)}<div className="muted small">max {fmtDuration(r.max_task_ms)}</div></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
