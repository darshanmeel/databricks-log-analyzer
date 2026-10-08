// Revision 20: how big a table is and what reading it cost. The size and file count come from a scan that skipped
// nothing (files read + files skipped); a MERGE rewrites every file it touches, so files over 1 GB are flagged.
import { api, type TableStats as TS } from '../api';
import { useState } from 'react';
import { useAsync } from '../hooks';
import { TOP_ROWS } from './SettingsAdvice';
import { fmtBytes, fmtDuration, fmtNum, fmtRows } from '../format';

export const BIG_FILE = 1024 ** 3;
const short = (t: string) => (/^[a-z]+:\/\/|^\//.test(t) ? `…/${t.split('/').filter(Boolean).pop()}` : t);

/** A few lines under a table's name: size, files, average file, scans and their cost. */
export function TableStatsLines({ s }: { s: TS }) {
  const big = (s.avg_file_bytes ?? 0) >= BIG_FILE;
  return (
    <div className="small tstats">
      {s.size_bytes ? <div><b>~{fmtBytes(s.size_bytes)}</b> in {fmtNum(s.files)} {s.files === 1 ? 'file' : 'files'} · <span className={big ? 'st-warn' : ''}>{fmtBytes(s.avg_file_bytes)} per file</span></div> : null}
      <div className="muted">scanned {fmtNum(s.scans)}×{s.bytes_from_files ? <> · {fmtBytes(s.bytes_from_files)} pulled from the files</> : null}{s.scan_task_ms ? <> · {fmtDuration(s.scan_task_ms)} of task time</> : null}</div>
      {big && <div className="st-warn">Files of {fmtBytes(s.avg_file_bytes)}: data skipping works per file, so a filter can skip little, and without deletion vectors a MERGE rewrites every file with one matching row. Aim for 64–256 MB files and cluster by the merge keys.</div>}
    </div>
  );
}

const BIG_TABLE = 10 * 1024 ** 3;

type Scope = { run?: string; ctx?: string; query?: number };

/** What a table read says at a glance: files too big, nothing skipped on a big table, scanned again and again, or
 * most of the columns pulled. Worst first. */
function flagsOf(t: TS, scope: Scope): { text: string; bad?: boolean }[] {
  const f: { text: string; bad?: boolean }[] = [];
  if ((t.avg_file_bytes ?? 0) >= BIG_FILE) f.push({ text: `${fmtBytes(t.avg_file_bytes)} per file: a MERGE rewrites whole files`, bad: true });
  if ((t.size_bytes ?? 0) >= BIG_TABLE && !t.files_pruned) f.push({ text: `read whole: no file skipped${t.partition_cols ? '' : ', not partitioned'}` });
  const again = scope.query !== undefined ? t.scans : t.runs ? t.scans / t.runs : t.scans;
  if (again >= 2) f.push({ text: scope.query !== undefined ? `scanned ${fmtNum(t.scans)}× in this query` : t.runs > 1 ? `scanned ${fmtNum(t.scans)}× over ${fmtNum(t.runs)} runs` : `scanned ${fmtNum(t.scans)}× in one run` });
  return f;
}

/** Every table read from files (the cluster, one run or one query), the costliest first, with a highlighted line on
 * top that says in a second which read cost the most and what is wrong with it. */
export function TablesRead({ cid, scope = {}, embedded = false }: { cid: string; scope?: Scope; embedded?: boolean }) {
  const st = useAsync((s) => api.tables(cid, s, scope), [cid, scope.run, scope.ctx, scope.query]);
  const [all, setAll] = useState(false);
  const rows = (st.data?.tables ?? []).filter((t) => t.size_bytes || t.scan_task_ms);
  if (!rows.length) return null;
  const where = scope.query !== undefined ? 'this query' : scope.run ? 'this run' : 'this cluster';
  const allTask = rows.reduce((a, t) => a + (t.scan_task_ms || 0), 0);
  const pulled = rows.reduce((a, t) => a + (t.bytes_from_files || 0), 0);
  const rowsAll = rows.reduce((a, t) => a + (t.rows_from_files || 0), 0);
  const top = rows[0];
  const flagged = rows.map((t) => ({ t, f: flagsOf(t, scope) })).filter((x) => x.f.length);
  const head = (
    <div className="tr-head">
      <span className="ro-eyebrow warn">Tables read</span>
      <div className="tr-line">
        <b>{fmtNum(rows.length)} {rows.length === 1 ? 'table' : 'tables'}</b> read from files by {where}: <b>{fmtBytes(pulled)}</b>{rowsAll ? <> and <b>{fmtRows(rowsAll)} rows</b></> : null} pulled, <b>{fmtDuration(allTask)}</b> of task time.
        {' '}Most: <code className="fp-table" title={top.table}>{short(top.table)}</code>
        {top.size_bytes ? <> ~{fmtBytes(top.size_bytes)} in {fmtNum(top.files)} files</> : null}
        {top.scan_task_ms && allTask ? <>, {fmtDuration(top.scan_task_ms)} ({Math.round((100 * top.scan_task_ms) / allTask)}%)</> : null}
      </div>
      {flagged.length > 0 && (
        <ul className="tr-flags">
          {flagged.slice(0, 5).map(({ t, f }) => (
            <li key={t.table} className={f.some((x) => x.bad) ? 'bad' : ''}>
              <code className="fp-table" title={t.table}>{short(t.table)}</code> {f.map((x) => x.text).join(' · ')}
            </li>
          ))}
          {flagged.length > 5 && <li className="muted">{flagged.length - 5} more tables flagged below</li>}
        </ul>
      )}
    </div>
  );
  const table = (
    <div style={{ overflowX: 'auto' }}>
      <table className="ro-find">
        <thead>
          <tr>
            <th>Table</th><th className="num">Size</th><th className="num">Files</th><th className="num">Per file</th><th className="num">Scans</th>
            {scope.query === undefined && <th className="num">Runs</th>}
            <th className="num">Skipped</th><th className="num">Pulled from the files</th><th className="num">Rows read</th><th className="num">Scan time</th><th className="num">Task time</th>
          </tr>
        </thead>
        <tbody>
          {(all ? rows : rows.slice(0, TOP_ROWS)).map((t) => {
            const big = (t.avg_file_bytes ?? 0) >= BIG_FILE;
            return (
              <tr key={t.table}>
                <td><code className="fp-table" title={t.table}>{short(t.table)}</code>{t.partition_cols ? <div className="muted small">partitioned ({t.partition_cols} columns)</div> : <div className="muted small">not partitioned</div>}</td>
                <td className="num">{t.size_bytes ? `~${fmtBytes(t.size_bytes)}` : '–'}</td>
                <td className="num">{fmtNum(t.files)}</td>
                <td className={`num ${big ? 'st-warn' : ''}`} title={big ? 'Over 1 GB per file: a MERGE rewrites whole files' : undefined}>{fmtBytes(t.avg_file_bytes)}</td>
                <td className="num">{fmtNum(t.scans)}</td>
                {scope.query === undefined && <td className="num">{t.runs ? fmtNum(t.runs) : '–'}</td>}
                <td className="num">{t.files_pruned ? <>{fmtNum(t.files_pruned)} files<div className="muted small">{fmtBytes(t.bytes_pruned)}</div></> : <span className="muted">none</span>}</td>
                <td className="num">{t.bytes_from_files ? fmtBytes(t.bytes_from_files) : '–'}{t.bytes_read && t.bytes_from_files && t.bytes_from_files <= t.bytes_read ? <div className="muted small">of {fmtBytes(t.bytes_read)} in files scanned</div> : null}</td>
                <td className="num">{t.rows_from_files ? fmtRows(t.rows_from_files) : '–'}</td>
                <td className="num">{t.scan_wall_ms ? fmtDuration(t.scan_wall_ms) : '–'}</td>
                <td className="num">{t.scan_task_ms ? fmtDuration(t.scan_task_ms) : '–'}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {rows.length > TOP_ROWS && <button className="linkish small" style={{ marginTop: 6 }} onClick={() => setAll(!all)}>{all ? `Only the top ${TOP_ROWS} ↑` : `All ${fmtNum(rows.length)} tables ↓`}</button>}
    </div>
  );
  const note = 'Costliest reads first. Size and files from a scan that skipped nothing; pulled from the files is what the tasks actually read (only the columns they need: Parquet is columnar).';
  if (embedded)
    return (
      <div className="tables-read stack" style={{ gap: 8 }}>
        {head}
        <p className="muted small" style={{ margin: 0 }}>{note}</p>
        {table}
      </div>
    );
  return (
    <section className="panel tables-read">
      <div className="panel-body stack" style={{ gap: 10 }}>
        {head}
        <p className="muted small" style={{ margin: 0 }}>{note}{scope.run || scope.query !== undefined ? '' : ' Open a run to see which of its queries read and wrote each table.'}</p>
        {table}
      </div>
    </section>
  );
}

/** Every table the cluster read from files: biggest cost first. */
export function ClusterTables({ cid }: { cid: string }) {
  return <TablesRead cid={cid} />;
}
