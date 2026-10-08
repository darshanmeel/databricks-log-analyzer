// Revision 20: how big a table is and what reading it cost. The size and file count come from a scan that skipped
// nothing (files read + files skipped); a MERGE rewrites every file it touches, so files over 1 GB are flagged.
import { api, type TableStats as TS } from '../api';
import { useAsync } from '../hooks';
import { fmtBytes, fmtDuration, fmtNum } from '../format';
import { Panel } from './ui';

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

/** Every table the cluster read from files: biggest cost first. */
export function ClusterTables({ cid }: { cid: string }) {
  const st = useAsync((s) => api.tables(cid, s), [cid]);
  const rows = (st.data?.tables ?? []).filter((t) => t.size_bytes || t.scan_task_ms);
  if (!rows.length) return null;
  return (
    <Panel title="Tables" note="Every table read from files on this cluster, the costliest reads first. Size and files from a scan that skipped nothing; pulled from the files is what the tasks actually read (only the columns they need: Parquet is columnar).">
      <div style={{ overflowX: 'auto' }}>
        <table className="ro-find">
          <thead>
            <tr>
              <th>Table</th><th className="num">Size</th><th className="num">Files</th><th className="num">Per file</th><th className="num">Scans</th><th className="num">Runs</th>
              <th className="num">Skipped</th><th className="num">Pulled from the files</th><th className="num">Scan time</th><th className="num">Task time</th>
            </tr>
          </thead>
          <tbody>
            {rows.slice(0, 25).map((t) => {
              const big = (t.avg_file_bytes ?? 0) >= BIG_FILE;
              return (
                <tr key={t.table}>
                  <td><code className="fp-table" title={t.table}>{short(t.table)}</code>{t.partition_cols ? <div className="muted small">partitioned ({t.partition_cols} columns)</div> : <div className="muted small">not partitioned</div>}</td>
                  <td className="num">{t.size_bytes ? `~${fmtBytes(t.size_bytes)}` : '–'}</td>
                  <td className="num">{fmtNum(t.files)}</td>
                  <td className={`num ${big ? 'st-warn' : ''}`} title={big ? 'Over 1 GB per file: a MERGE rewrites whole files' : undefined}>{fmtBytes(t.avg_file_bytes)}</td>
                  <td className="num">{fmtNum(t.scans)}</td>
                  <td className="num">{t.runs ? fmtNum(t.runs) : '–'}</td>
                  <td className="num">{t.files_pruned ? <>{fmtNum(t.files_pruned)} files<div className="muted small">{fmtBytes(t.bytes_pruned)}</div></> : <span className="muted">none</span>}</td>
                  <td className="num">{t.bytes_from_files ? fmtBytes(t.bytes_from_files) : '–'}{t.bytes_read && t.bytes_from_files && t.bytes_from_files <= t.bytes_read ? <div className="muted small">of {fmtBytes(t.bytes_read)} in files scanned</div> : null}</td>
                  <td className="num">{t.scan_wall_ms ? fmtDuration(t.scan_wall_ms) : '–'}</td>
                  <td className="num">{t.scan_task_ms ? fmtDuration(t.scan_task_ms) : '–'}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {rows.length > 25 && <p className="muted small" style={{ margin: '6px 0 0' }}>{rows.length - 25} smaller tables not shown.</p>}
      <p className="muted small" style={{ margin: '6px 0 0' }}>Open a run to see which of its queries read and wrote each table. <a href="#cv-find">Rank the runs ↓</a></p>
    </Panel>
  );
}
