import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, type Facet, type LogLine } from '../api';
import { LogLines } from '../components/LogLines';
import { useCluster } from '../components/Shell';
import { Async, Empty, ErrorState, Loading, Pager, Panel } from '../components/ui';
import { fmtNum, parseUtcInput, toUtcInput, truncate } from '../format';
import { useAsync, useDebounced, useQueryState } from '../hooks';
import { to } from '../links';

const LIMIT = 200;
const FILTERS = ['source', 'executor_id', 'level', 'logger', 'signal'] as const;
type FilterKey = (typeof FILTERS)[number];
const FILTER_LABEL: Record<FilterKey, string> = { source: 'Source', executor_id: 'Executor', level: 'Level', logger: 'Logger', signal: 'Signal' };

function FacetSelect({ cid, column, value, onChange, ready }: { cid: string; column: FilterKey; value: string; onChange: (v: string | null) => void; ready: boolean }) {
  // only once the lines loaded: without a log_lines table every facet request would fail
  const st = useAsync((s) => api.facets(cid, 'log_lines', column, s), [cid, column, ready], ready);
  const opts: Facet[] = st.data ?? [];
  const known = value && !opts.some((o) => String(o.value) === value);
  return (
    <label className="group">
      <span className="lbl">{FILTER_LABEL[column]}</span>
      <select className="select" value={value} onChange={(e) => onChange(e.target.value || null)} style={{ minWidth: 130, maxWidth: column === 'logger' ? 280 : 200 }} disabled={!!st.error}>
        <option value="">{st.loading ? 'Loading…' : 'Any'}</option>
        {known && <option value={value}>{value}</option>}
        {opts
          .filter((o) => o.value !== null && o.value !== '')
          .map((o) => (
            <option key={String(o.value)} value={String(o.value)}>
              {truncate(String(o.value), 48)} ({fmtNum(o.count)})
            </option>
          ))}
      </select>
    </label>
  );
}

/** One click to the lines that matter: the error and warning lines, with their counts (the full Level list stays in
 * the filters). All lines stay the default, so a Python traceback logged at INFO is never hidden. */
function LevelChips({ cid, value, onChange, ready }: { cid: string; value: string; onChange: (v: string | null) => void; ready: boolean }) {
  const st = useAsync((s) => api.facets(cid, 'log_lines', 'level', s), [cid, ready], ready);
  const count = (lv: string) => (st.data ?? []).filter((o) => String(o.value) === lv).reduce((a, o) => a + o.count, 0);
  const chips: [string | null, string, number | null][] = [[null, 'All lines', null], ['FATAL', 'Fatal', count('FATAL')], ['ERROR', 'Errors', count('ERROR')], ['WARN', 'Warnings', count('WARN')]];
  return (
    <div className="chips level-chips" role="group" aria-label="Level">
      {chips.filter(([lv, , n]) => lv === null || (n ?? 0) > 0 || value === lv).map(([lv, label, n]) => (
        <button key={label} className={`chip ${(value || null) === lv ? 'active' : ''}`} aria-pressed={(value || null) === lv} onClick={() => onChange(lv)}>
          {label}{n ? <span className="muted"> {fmtNum(n)}</span> : null}
        </button>
      ))}
    </div>
  );
}

export default function Logs() {
  const [sp] = useQueryState();
  const fp = sp.get('file_path');
  const seq = sp.get('seq');
  if (fp && seq !== null && seq !== '') return <ContextView filePath={fp} seq={Number(seq)} />;
  return <SearchView />;
}

function SearchView() {
  const { cid } = useCluster();
  const [sp, setQ] = useQueryState();
  const f = Object.fromEntries(FILTERS.map((k) => [k, sp.get(k) ?? ''])) as Record<FilterKey, string>;
  const tsFrom = sp.get('ts_from');
  const tsTo = sp.get('ts_to');
  const [text, setText] = useState(sp.get('q') ?? '');
  const q = useDebounced(text, 400);
  const [offset, setOffset] = useState(0);
  useEffect(() => {
    if ((sp.get('q') ?? '') !== q) setQ({ q }, true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q]);
  const fkey = [...FILTERS.map((k) => f[k]), tsFrom, tsTo, q].join('|');
  useEffect(() => setOffset(0), [fkey]);

  const st = useAsync(
    (s) =>
      api.dataset<LogLine>(
        cid,
        'log_lines',
        {
          limit: LIMIT,
          offset,
          sort: 'seq',
          ...Object.fromEntries(FILTERS.map((k) => [k, f[k] || null])),
          ts_from: tsFrom,
          ts_to: tsTo,
          q: q || null,
        },
        s,
      ),
    [cid, offset, fkey],
  );
  const active = FILTERS.some((k) => f[k]) || tsFrom || tsTo || q;

  return (
    <div className="page wide">
      <div className="page-head">
        <div>
          <h1>Logs</h1>
          <details className="sub"><summary>About this page</summary>Driver and executor log lines in file order. Click a line number to see the lines around it. Times are UTC.</details>
        </div>
      </div>
      <div className="panel" style={{ marginBottom: 16 }}>
        <div className="panel-body">
          <LevelChips cid={cid} value={f.level} onChange={(v) => setQ({ level: v }, true)} ready={!!st.data} />
          <div className="filters">
            {FILTERS.map((k) => (
              <FacetSelect key={k} cid={cid} column={k} value={f[k]} onChange={(v) => setQ({ [k]: v }, true)} ready={!!st.data} />
            ))}
            <label className="group">
              <span className="lbl">From (UTC)</span>
              <input className="input" type="datetime-local" value={toUtcInput(tsFrom ? Number(tsFrom) : null)} onChange={(e) => setQ({ ts_from: parseUtcInput(e.target.value) }, true)} />
            </label>
            <label className="group">
              <span className="lbl">To (UTC)</span>
              <input className="input" type="datetime-local" value={toUtcInput(tsTo ? Number(tsTo) : null)} onChange={(e) => setQ({ ts_to: parseUtcInput(e.target.value) }, true)} />
            </label>
            <label className="group grow" style={{ minWidth: 220 }}>
              <span className="lbl">Text</span>
              <input className="input mono" value={text} onChange={(e) => setText(e.target.value)} placeholder="Contains… (case-insensitive)" />
            </label>
            {active && (
              <button
                className="btn"
                onClick={() => {
                  setText('');
                  setQ({ source: null, executor_id: null, level: null, logger: null, signal: null, ts_from: null, ts_to: null, q: null }, true);
                }}
              >
                Clear filters
              </button>
            )}
          </div>
        </div>
      </div>
      <div className="panel">
        {st.error && /not built/.test(st.error.message) ? (
          <Empty title="No log lines in this output">
            This analysis kept no log_lines table (a copy without the raw lines, or an old version). Errors, signals and the
            story still come from the logs; re-run analyze on the raw logs to browse every line here.
          </Empty>
        ) : st.error ? (
          <ErrorState error={st.error} onRetry={st.reload} />
        ) : !st.data ? (
          <Loading label="Loading log lines…" />
        ) : st.data.rows.length === 0 ? (
          <Empty title="No log lines match">{active ? 'Loosen the filters or clear them.' : 'No driver or executor logs were found for this cluster.'}</Empty>
        ) : (
          <>
            <div style={{ opacity: st.loading ? 0.6 : 1, padding: '6px 0' }}>
              <LogLines rows={st.data.rows} wide cid={cid} linkToContext />
            </div>
            <Pager total={st.data.total} limit={LIMIT} offset={offset} onChange={setOffset} label="lines" />
          </>
        )}
      </div>
    </div>
  );
}

function ContextView({ filePath, seq }: { filePath: string; seq: number }) {
  const { cid } = useCluster();
  const [before, setBefore] = useState(40);
  const [after, setAfter] = useState(80);
  const st = useAsync((s) => api.logContext(cid, filePath, seq, before, after, s), [cid, filePath, seq, before, after]);
  const [scrolled, setScrolled] = useState(false);
  useEffect(() => setScrolled(false), [filePath, seq]);
  useEffect(() => {
    if (!st.data || scrolled) return;
    const t = setTimeout(() => {
      document.getElementById(`seq-${seq}`)?.scrollIntoView({ block: 'center' });
      setScrolled(true);
    }, 40);
    return () => clearTimeout(t);
  }, [st.data, seq, scrolled]);
  const first = st.data?.rows[0];
  return (
    <div className="page wide">
      <div className="page-head">
        <div style={{ minWidth: 0 }}>
          <h1>Log context</h1>
          <p className="sub mono small wrap-any">{filePath}</p>
          {first && (
            <p className="muted small">
              {first.source === 'driver' ? 'Driver' : `Executor ${first.executor_id}`} log. The highlighted line is the one you followed.
            </p>
          )}
        </div>
        <div className="actions">
          <Link className="btn" to={to.logs(cid)}>
            Search all logs
          </Link>
          {first && (
            <Link className="btn" to={to.logs(cid, first.source === 'driver' ? { source: 'driver' } : { source: 'executor', executor_id: first.executor_id })}>
              All lines from this {first.source === 'driver' ? 'driver' : 'executor'}
            </Link>
          )}
        </div>
      </div>
      <Panel flush>
        <div className="pager" style={{ borderTop: 0, borderBottom: '1px solid var(--line-soft)' }}>
          <button className="btn small" onClick={() => setBefore((b) => b + 100)}>
            Show 100 earlier lines
          </button>
          <span className="grow" />
          {st.data && <span>{fmtNum(st.data.rows.length)} lines</span>}
        </div>
        <Async state={st} label="Loading surrounding lines…">
          {(t) =>
            t.rows.length === 0 ? (
              <Empty title="Line not found">That log line is not in the dataset. The cluster may have been rebuilt since the link was made.</Empty>
            ) : (
              <div style={{ padding: '6px 0' }}>
                <LogLines rows={t.rows} targetSeq={seq} wide />
              </div>
            )
          }
        </Async>
        <div className="pager">
          <button className="btn small" onClick={() => setAfter((a) => a + 200)}>
            Show 200 later lines
          </button>
        </div>
      </Panel>
    </div>
  );
}
