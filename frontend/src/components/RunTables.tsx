// Revision 20: what one run read and wrote. Per table: who read it (how: files, its Delta log, the source database),
// how much the read skipped (partitions, data skipping, dynamic pruning) and who wrote it (MERGE, write...). A query's
// joins, filters, jobs and stages are on its own page.
import { Link } from 'react-router-dom';
import { api, type RunTableRead, type RunTables as RT } from '../api';
import { useAsync } from '../hooks';
import { fmtBytes, fmtDuration, fmtNum, fmtRows, fmtTime, truncate } from '../format';
import { to } from '../links';
import { TableStatsLines } from './TableStats';

const short = (t: string) => (/^[a-z]+:\/\/|^\//.test(t) ? `…/${t.split('/').filter(Boolean).pop()}` : t);
const GB = 1024 ** 3;

/** How much of the table the read skipped, in words; tone 'warn' when a big read skipped nothing. */
function pruning(r: RunTableRead): { text: string; tone?: 'warn' | 'ok' } | null {
  if (r.how.includes('source database')) return r.filter ? { text: `filter pushed to the database: ${truncate(r.filter, 70)}`, tone: 'ok' } : { text: 'no filter: reads the whole source', tone: 'warn' };
  if (r.files_read == null && r.partition_cols == null) return null;
  const bits: string[] = [];
  let tone: 'warn' | 'ok' | undefined;
  if ((r.partition_cols ?? 0) > 0) { bits.push(`partitioned (${r.partition_cols} columns): ${fmtNum(r.partitions_read ?? 0)} partition${r.partitions_read === 1 ? '' : 's'} read`); tone = 'ok'; }
  else bits.push('not partitioned');
  if (r.files_read != null) {
    const pr = r.files_pruned ?? 0;
    bits.push(`${fmtNum(r.files_read)} file${r.files_read === 1 ? '' : 's'} in scope${r.bytes_read ? ` (${fmtBytes(r.bytes_read)})` : ''}`);
    if (pr > 0) { bits.push(`${fmtNum(pr)} skipped${r.bytes_pruned ? ` (${fmtBytes(r.bytes_pruned)})` : ''} by data skipping`); tone = 'ok'; }
    else if ((r.bytes_read ?? 0) >= 10 * GB && !(r.partition_cols ?? 0)) { bits.push('nothing skipped'); tone = 'warn'; }
  }
  if ((r.dpp_filters ?? 0) > 0 || (r.dfp_filters ?? 0) > 0) bits.push('dynamic pruning on');
  return { text: bits.join(' · '), tone };
}

export function RunTables({ cid, run }: { cid: string; run: string }) {
  const st = useAsync((s) => api.runTables(cid, run, s), [cid, run]);
  const d = st.data;
  if (st.error) return null;
  if (!d) return <section className="panel"><div className="panel-body muted small">Loading what it read and wrote…</div></section>;
  if (!d.tables.length && !d.queries.length) return null;
  const qname = (q: { ctx: string; id: number }) => <Link to={to.query(cid, q.ctx, q.id)}>Query {q.id}</Link>;
  const t0 = Math.min(...d.queries.map((q) => q.start ?? Infinity));
  const t1 = Math.max(...d.queries.map((q) => q.end ?? q.start ?? 0));
  const span = Math.max(1, t1 - t0);
  const pct = (v: number) => `${Math.min(100, Math.max(0, ((v - t0) / span) * 100))}%`;
  return (
    <section className="panel" id="ro-tables">
      <div className="panel-head">
        <div>
          <h2>What it read and wrote <span className="ro-count">{d.tables.length} tables</span></h2>
          <div className="note">In time order: each table from its first read or write to its last use, the queries that read it and how much they skipped, and the queries that wrote it. Open a query for its joins, filters, jobs and stages.</div>
        </div>
      </div>
      <div className="panel-body stack" style={{ gap: 14 }}>
        <Cycles cid={cid} d={d} />
        <Repeats cid={cid} d={d} />
        <JdbcSteps cid={cid} d={d} />
        <div style={{ overflowX: 'auto' }}>
          <table className="ro-find">
            <thead><tr><th>Table or path</th><th style={{ minWidth: 190 }}>When <span className="th-sub">on the run's clock</span></th><th>Read by</th><th>Pruning</th><th>Written by</th></tr></thead>
            <tbody>
              {d.tables.map((t) => (
                <tr key={t.table}>
                  <td style={{ whiteSpace: 'normal', minWidth: 200 }}>
                    <code className="fp-table" title={t.path ? `${t.table}\n${t.path}` : t.table}>{short(t.table)}</code>
                    <div className="muted small">{t.role}</div>
                    {t.stats ? <TableStatsLines s={t.stats} /> : null}
                  </td>
                  <td className="small">
                    <div className="qs-lane rt-lane" title="From its first use to its last; the mark is its first write">
                      {(t.first_read ?? t.first_write) != null && t.last != null && (
                        <span className="qs-run" style={{ left: pct(Math.min(t.first_read ?? Infinity, t.first_write ?? Infinity)), width: `${Math.max(1, ((t.last - Math.min(t.first_read ?? Infinity, t.first_write ?? Infinity)) / span) * 100)}%` }} />
                      )}
                      {t.first_write != null && <span className="rt-write" style={{ left: pct(t.first_write) }} />}
                    </div>
                    <div className="mono muted">
                      {t.first_read != null ? <div>read {fmtTime(t.first_read)}</div> : null}
                      {t.first_write != null ? <div className="rt-w">written {fmtTime(t.first_write)}</div> : null}
                      {t.last != null ? <div>last {fmtTime(t.last)}</div> : null}
                    </div>
                  </td>
                  <td className="small" style={{ whiteSpace: 'normal' }}>
                    {t.reads.length ? t.reads.map((r, i) => <div key={i}>{qname(r)} <span className="muted">· {r.op} · {r.how}</span></div>) : <span className="muted">–</span>}
                  </td>
                  <td className="small" style={{ whiteSpace: 'normal', maxWidth: 380 }}>
                    {t.merge && (
                      <div className="rt-merge">
                        <div>MERGE matches on{t.merge.null_safe ? <span className="muted"> (null-safe &lt;=&gt;)</span> : null}</div>
                        <div className="rt-keys">{t.merge.keys.map((k, i) => <code key={i} className="fp-table">{k}</code>)}</div>
                        {t.merge.target_filters.length
                          ? <div>filter on the target: <code className="ql-expr">{t.merge.target_filters.join(' AND ')}</code></div>
                          : <div className="st-warn">No filter on the target in the ON clause, so every file of it is read. Add one the source bounds (e.g. <code>t.{t.merge.keys[0]} &gt;= min of the source</code>), or cluster the table by {t.merge.keys.slice(0, 2).join(', ')} so files can be skipped.</div>}
                      </div>
                    )}
                    {t.reads.map((r, i) => { const p = pruning(r); return p ? <div key={i} className={p.tone === 'warn' ? 'st-warn' : p.tone === 'ok' ? '' : 'muted'}>Query {r.id}: {p.text}{pulled(r)}</div> : null; })}
                  </td>
                  <td className="small" style={{ whiteSpace: 'normal' }}>
                    {t.writes.length ? t.writes.map((w, i) => <div key={i}>{qname(w)} <span className="muted">· {w.op}{w.bytes ? <> · wrote {fmtBytes(w.bytes)}</> : null}</span>{writeNote(w)}</div>) : <span className="muted">–</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

/** What a read pulled from the files against the files it scanned: a MERGE's search for matches reads only the key
 * columns, its rewrite reads whole rows. */
function pulled(r: RunTableRead) {
  if (r.from_files == null || !r.bytes_read) return null;
  const share = r.from_files / r.bytes_read;
  const why = share < 0.001 ? ' (the scan barely ran: adaptive execution found nothing to join)' : share < 0.2 ? ' (only the columns it needs)' : share > 0.7 ? ' (whole rows)' : '';
  return <span className="muted"> · actually read {fmtBytes(r.from_files)}{why}</span>;
}

/** The MERGEs of the run: per batch, an upsert and a delete MERGE each scan the target again. */
function Cycles({ cid, d }: { cid: string; d: RT }) {
  const ms = d.merges ?? [];
  if (ms.length < 2) return null;
  const byTarget = new Map<string, typeof ms>();
  for (const m of ms) byTarget.set(m.target ?? '?', [...(byTarget.get(m.target ?? '?') ?? []), m]);
  const batches = new Set(ms.map((m) => m.batch ?? '?')).size;
  const scans = ms.reduce((a, m) => a + m.target_scans, 0);
  const both = ms.some((m) => m.kind === 'deletes') && ms.some((m) => m.kind === 'upserts');
  return (
    <div className="rt-cycles small">
      <div><b>The same cycle repeats:</b> {fmtNum(ms.length)} MERGEs in {fmtNum(batches)} {batches === 1 ? 'batch' : 'batches'}{byTarget.size === 1 ? <> into <code className="fp-table">{short([...byTarget.keys()][0])}</code></> : null}; the target was scanned <b>{fmtNum(scans)} times</b>.</div>
      <ol>
        {ms.map((m, i) => (
          <li key={i}>
            <span className="mono muted">{fmtTime(m.start)}</span> {m.batch ? <>batch {m.batch} · </> : null}<b>{m.kind ?? 'MERGE'}</b>: {m.steps.map((x, j) => (
              <span key={x.id}>{j ? ' → ' : ''}<Link to={to.query(cid, x.ctx, x.id)}>{x.id}</Link></span>
            ))} <span className="muted">· {m.target_scans} scan{m.target_scans === 1 ? '' : 's'} of the target</span>
          </li>
        ))}
      </ol>
      {both && <div className="st-warn">Upserts and deletes run as separate MERGEs, so each batch reads the target again. One MERGE with <code>WHEN MATCHED AND s._change_type = 'delete' THEN DELETE</code> next to the update and insert clauses reads it once.</div>}
    </div>
  );
}

/** Spark labels a MERGE's last step "rewriting N files"; with deletion vectors it writes only the changed rows, so when
 * it wrote far less than it read, the read is the cost, not the write. */
function writeNote(w: { step?: string; bytes?: number | null; read_bytes?: number | null }) {
  if (!w.step?.includes('rewriting') || !w.bytes || !w.read_bytes || w.bytes > 0.3 * w.read_bytes) return null;
  return <div className="muted small">read {fmtBytes(w.read_bytes)} to write {fmtBytes(w.bytes)}: it read every file but wrote only the changed rows (deletion vectors), so the read is the cost, not the "{w.step.replace(/^MERGE: /, '')}"</div>;
}

/** Generic, whatever the run does: a source read by several queries of the run (a re-materialized MERGE source, a
 * JDBC query run again to count, an uncached DataFrame reused by several actions), and how much it read from storage
 * per byte it wrote. */
/** What the run asked the source database, step by step, with the same SQL sent twice flagged. JDBC reports rows,
 * not bytes, so rows are the measure here. */
const KIND_WORD: Record<string, string> = { count: 'count the rows', probe: 'probe a few rows', 'key ranges': 'key ranges for the partitions', read: 'read' };
/** A read through one connection (numPartitions = 1) that took a minute or pulled a million rows. */
const oneLane = (j: { kind: string; partitions: number; took_ms: number | null; rows: number | null }) =>
  j.kind === 'read' && j.partitions === 1 && ((j.took_ms ?? 0) >= 60_000 || (j.rows ?? 0) >= 1_000_000);

function JdbcSteps({ cid, d }: { cid: string; d: RT }) {
  const js = d.jdbc ?? [];
  if (!js.length) return null;
  const again = js.filter((j) => j.same_as !== null);
  const sources = [...new Set(js.map((j) => j.source).filter(Boolean))];
  const big = js.find((j) => j.same_as === null && j.kind === 'read' && (j.rows ?? 0) > 0);
  return (
    <div className="rt-cycles small">
      <div>
        <b>The source database was queried {js.length} {js.length === 1 ? 'time' : 'times'}</b>
        {sources.length ? <> ({sources.map((s, i) => <span key={s!}>{i ? ', ' : ''}<code className="fp-table">{s}</code></span>)})</> : null}
        {again.length ? <span className="bad"> · {again.length} sent the same SQL again</span> : null}
        {big && big.took_ms ? <span className="muted"> · the read: {fmtRows(big.rows)} rows in {big.partitions} partitions, {fmtRows(Math.round((big.rows ?? 0) / Math.max(1, big.took_ms / 1000)))} rows/s</span> : null}
      </div>
      <table className="table compact" style={{ marginTop: 4 }}>
        <thead><tr><th>Query</th><th>Asked the database to</th><th className="num">Rows</th><th className="num">Partitions</th><th className="num">Took</th><th /></tr></thead>
        <tbody>
          {js.map((j) => (
            <tr key={j.query}>
              <td><Link to={to.query(cid, j.ctx, j.query)}>{j.query}</Link></td>
              <td title={j.sql}>{KIND_WORD[j.kind] ?? j.kind}{j.wrote_bytes ? <span className="muted"> · wrote {fmtBytes(j.wrote_bytes)}</span> : null}</td>
              <td className="num">{j.rows !== null ? fmtRows(j.rows) : '–'}</td>
              <td className="num">{j.partitions}</td>
              <td className="num">{j.took_ms !== null ? fmtDuration(j.took_ms) : '–'}</td>
              <td>
                {j.same_as !== null ? <span className="ro-flag warn">same SQL as {j.same_as}</span> : null}
                {oneLane(j) ? <span className="ro-flag warn" title="numPartitions = 1: one task, one connection, pulled every row. Give the read a partitionColumn with lowerBound, upperBound and numPartitions.">one connection</span> : null}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {big && big.partitions > 1 && big.spread?.p50_task_rows_in ? (() => {
        const sp = big.spread!;
        const x = (sp.max_task_rows_in ?? 0) / Math.max(1, sp.p50_task_rows_in ?? 0);
        return (
          <div>
            <b>Partitions {x <= 2 ? 'are even' : `are uneven: the biggest has ${x.toFixed(0)}× the median`}</b>
            <span className="muted"> · median {fmtRows(sp.p50_task_rows_in)} rows{sp.p50_task_ms ? ` in ${fmtDuration(sp.p50_task_ms)}` : ''}, biggest {fmtRows(sp.max_task_rows_in)}{sp.p90_task_ms ? `, p90 ${fmtDuration(sp.p90_task_ms)}` : ''}
              {x > 2 ? '. Split on a column with evenly spread values (partitionColumn, lowerBound, upperBound) or more partitions.' : ''}</span>
          </div>
        );
      })() : null}
      {js.some(oneLane) && <div><b>{js.filter(oneLane).length} {js.filter(oneLane).length === 1 ? 'read went' : 'reads went'} through one connection</b><span className="muted"> (numPartitions = 1): split it with partitionColumn, lowerBound, upperBound and numPartitions so several tasks read at once.</span></div>}
      {again.length > 0 && <div className="muted">The database answers each of these again. Count from the write's own metrics (rows written) or from the landed table, and send a probe once.</div>}
    </div>
  );
}

function Repeats({ cid, d }: { cid: string; d: RT }) {
  const source = (how: string) => /files|database|source/.test(how) && !/Delta log/.test(how);
  const again = d.tables
    .map((t) => ({ t, rs: t.reads.filter((r) => source(r.how)) }))
    .filter((x) => new Set(x.rs.map((r) => `${r.ctx}:${r.id}`)).size >= 2);
  const read = d.tables.reduce((a, t) => a + t.reads.reduce((b, r) => b + (r.from_files ?? 0), 0), 0);
  const wrote = d.tables.reduce((a, t) => a + t.writes.reduce((b, w) => b + (w.bytes ?? 0), 0), 0);
  const amp = wrote > 0 && read >= 1024 ** 3 ? read / wrote : null;
  if (!again.length && !(amp !== null && amp >= 5)) return null;
  return (
    <div className="rt-cycles small">
      {amp !== null && amp >= 5 && (
        <div><b>Read {amp.toFixed(0)}× what it wrote:</b> {fmtBytes(read)} read from storage to write {fmtBytes(wrote)}. The reads are the cost: skip files (filters on partition or clustering columns) before tuning the write.</div>
      )}
      {again.length > 0 && <div><b>Read more than once in this run:</b></div>}
      {again.length > 0 && (
        <ul style={{ margin: 0, paddingLeft: 20 }}>
          {again.map(({ t, rs }) => (
            <li key={t.table}>
              <code className="fp-table">{short(t.table)}</code> by {rs.length} queries:{' '}
              {rs.map((r, i) => (
                <span key={i}>{i ? ', ' : ''}<Link to={to.query(cid, r.ctx, r.id)}>{r.id}</Link>{r.from_files ? <span className="muted"> ({fmtBytes(r.from_files)})</span> : null}</span>
              ))}
            </li>
          ))}
        </ul>
      )}
      {again.length > 0 && <div className="muted">Each read goes back to storage (or the source database). Cache or checkpoint a source used by several steps, or count from the write's own metrics instead of reading again.</div>}
    </div>
  );
}
