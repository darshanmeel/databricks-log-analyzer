import { useEffect, useMemo, useRef, useState } from 'react';
import { FlowPanel } from '../components/RunFlow';
import { Link } from 'react-router-dom';
import { api, optional, STORY_KINDS, type AppRow, type Severity, type StoryRow } from '../api';
import { LogLines } from '../components/LogLines';
import { useCluster } from '../components/Shell';
import { DataLink, Drawer, Empty, EntityChips, ErrorState, Loading, Pager, SeverityBadge, ToggleChips, sevColor, Async } from '../components/ui';
import { fmtNum, fmtOffset, fmtTime, fmtTs } from '../format';
import { useAsync, useDebounced, useQueryState } from '../hooks';
import { rowLinks, to } from '../links';

const ROW_H = 58;
const PAGE = 5000;
const SEVS: Severity[] = ['high', 'medium', 'low', 'info'];

const KIND_LABEL: Record<string, string> = {
  app_start: 'App start',
  app_end: 'App end',
  job_start: 'Job start',
  job_end: 'Job end',
  stage_start: 'Stage start',
  stage_end: 'Stage end',
  stage_failed: 'Stage failed',
  query_start: 'Query start',
  query_end: 'Query end',
  query_failed: 'Query failed',
  executor_added: 'Executor added',
  executor_removed: 'Executor removed',
  log_error: 'Log error',
  log_signal: 'Log signal',
  finding: 'Finding',
  task_retry: 'Task retried',
  task_retries: 'Tasks retried',
  stage_retry: 'Stage retried',
  retry: 'Retry',
};

const KIND_PRESETS: { label: string; kinds: string[] }[] = [
  { label: 'Everything', kinds: [] },
  { label: 'Problems only', kinds: ['stage_failed', 'query_failed', 'executor_removed', 'task_retry', 'stage_retry', 'log_error', 'log_signal', 'finding'] },
  { label: 'Retries', kinds: ['task_retry', 'stage_retry'] },
  { label: 'Scheduler only', kinds: ['app_start', 'app_end', 'job_start', 'job_end', 'stage_start', 'stage_end', 'stage_failed', 'query_start', 'query_end', 'query_failed', 'executor_added', 'executor_removed'] },
];

function nodeClass(r: StoryRow): string {
  const sched = !['log_error', 'log_signal', 'finding'].includes(r.kind) && !r.kind.includes('retr');
  const parts = ['node'];
  if (r.severity !== 'info') parts.push(`sev-${r.severity}`);
  else if (sched) {
    parts.push('k-sched');
    if (r.kind.endsWith('_start') || r.kind === 'executor_added') parts.push('k-start');
    else if (r.kind.endsWith('_end')) parts.push('k-good');
  }
  return parts.join(' ');
}

function splitList(v: string | null): string[] {
  return v ? v.split(',').filter(Boolean) : [];
}

export default function Story() {
  const { cid, summary } = useCluster();
  const [sp, setQ] = useQueryState();
  const kinds = splitList(sp.get('kind'));
  const sevs = splitList(sp.get('severity')) as Severity[];
  const ctx = sp.get('ctx') ?? '';
  const selected = sp.get('step');
  const [text, setText] = useState(sp.get('q') ?? '');
  const q = useDebounced(text, 350);
  const [offset, setOffset] = useState(0);

  useEffect(() => {
    if ((sp.get('q') ?? '') !== q) setQ({ q }, true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q]);
  useEffect(() => setOffset(0), [cid, kinds.join(), sevs.join(), ctx, q]);

  const apps = useAsync((s) => api.dataset<AppRow>(cid, 'apps', { limit: 200, sort: 'start_time' }, s), [cid]);
  // kinds the backend actually emits (it may add new ones, e.g. retry rows) plus the contract list
  const kindFacet = useAsync((s) => optional(api.facets(cid, 'run_story', 'kind', s)), [cid]);
  const allKinds = useMemo(() => {
    const extra = (kindFacet.data ?? []).map((f) => String(f.value ?? '')).filter((k) => k && !(STORY_KINDS as string[]).includes(k));
    return [...(STORY_KINDS as string[]), ...extra];
  }, [kindFacet.data]);
  const presets = useMemo(() => {
    const retry = allKinds.filter((k) => k.includes('retr'));
    return KIND_PRESETS.map((p) => (p.label === 'Retries' ? { ...p, kinds: retry } : p.label === 'Problems only' ? { ...p, kinds: [...new Set([...p.kinds.filter((k) => !k.includes('retr')), ...retry])] } : p));
  }, [allKinds]);
  const st = useAsync(
    (s) =>
      api.dataset<StoryRow>(
        cid,
        'run_story',
        { limit: PAGE, offset, sort: 'story_seq', kind: kinds, severity: sevs, spark_context_id: ctx || null, q: q || null },
        s,
      ),
    [cid, offset, kinds.join(), sevs.join(), ctx, q],
  );

  const rows = st.data?.rows ?? [];
  const selRow = useMemo(() => rows.find((r) => String(r.story_seq) === selected) ?? null, [rows, selected]);

  return (
    <div className="page wide">
      <div className="page-head">
        <div />
        <div className="actions">
          <DataLink cid={cid} dataset="run_story" />
        </div>
      </div>

      <div className="panel" style={{ marginBottom: 16 }}>
        <div className="panel-body">
          <div className="filters">
            <div className="group">
              <span className="lbl">Show</span>
              <div className="seg">
                {presets.map((p) => {
                  const on = p.kinds.join() === kinds.join();
                  return (
                    <button key={p.label} className={on ? 'on' : ''} onClick={() => setQ({ kind: p.kinds.join(',') || null })}>
                      {p.label}
                    </button>
                  );
                })}
              </div>
            </div>
            <div className="group">
              <span className="lbl">Severity</span>
              <ToggleChips<Severity>
                options={SEVS}
                value={sevs}
                onChange={(v) => setQ({ severity: v.join(',') || null })}
                dot={(s) => sevColor(s)}
                render={(s) => s[0].toUpperCase() + s.slice(1)}
              />
            </div>
            {apps.data && apps.data.rows.length > 1 && (
              <label className="group">
                <span className="lbl">Spark context</span>
                <select className="select" value={ctx} onChange={(e) => setQ({ ctx: e.target.value || null })}>
                  <option value="">All contexts</option>
                  {apps.data.rows.map((a) => (
                    <option key={a.spark_context_id} value={a.spark_context_id}>
                      {a.spark_context_id} {a.app_name ? `(${a.app_name})` : ''}
                    </option>
                  ))}
                </select>
              </label>
            )}
            <label className="group grow" style={{ minWidth: 200 }}>
              <span className="lbl">Text</span>
              <input className="input" value={text} onChange={(e) => setText(e.target.value)} placeholder="Search titles and details" />
            </label>
          </div>
          <div style={{ marginTop: 10 }}>
            <details>
              <summary className="small ink2" style={{ cursor: 'pointer' }}>
                Event kinds {kinds.length ? `(${kinds.length} selected)` : '(all)'}
              </summary>
              <div style={{ marginTop: 8 }}>
                <ToggleChips<string>
                  options={allKinds}
                  value={kinds}
                  onChange={(v) => setQ({ kind: v.join(',') || null })}
                  render={(k) => KIND_LABEL[k] ?? k}
                />
              </div>
            </details>
          </div>
        </div>
      </div>

      <FlowPanel cid={cid} open />
      <div className="panel">
        {st.error ? (
          <ErrorState error={st.error} onRetry={st.reload} />
        ) : !st.data ? (
          <Loading label="Loading story…" />
        ) : rows.length === 0 ? (
          <Empty title="No story steps match">
            {kinds.length || sevs.length || q || ctx ? 'Clear some filters to see more of the run.' : 'The run story is empty for this cluster.'}
          </Empty>
        ) : (
          <>
            <VirtualStory
              rows={rows}
              start={summary.start_time}
              selected={selected}
              loading={st.loading}
              onSelect={(r) => setQ({ step: String(r.story_seq) }, true)}
            />
            {st.data.total > PAGE && <Pager total={st.data.total} limit={PAGE} offset={offset} onChange={setOffset} label="steps" />}
            {st.data.total <= PAGE && (
              <div className="pager">
                <span>{fmtNum(st.data.total)} steps</span>
              </div>
            )}
          </>
        )}
      </div>

      <StepDrawer row={selRow} cid={cid} start={summary.start_time} onClose={() => setQ({ step: null }, true)} />
    </div>
  );
}

function VirtualStory({
  rows,
  start,
  selected,
  onSelect,
  loading,
}: {
  rows: StoryRow[];
  start: number | null;
  selected: string | null;
  onSelect: (r: StoryRow) => void;
  loading: boolean;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [scroll, setScroll] = useState(0);
  const [height, setHeight] = useState(600);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    setHeight(el.clientHeight);
    const ro = new ResizeObserver(() => setHeight(el.clientHeight));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // keep the selected step visible when arriving from a link
  useEffect(() => {
    const el = ref.current;
    if (!el || !selected) return;
    const i = rows.findIndex((r) => String(r.story_seq) === selected);
    if (i < 0) return;
    const top = i * ROW_H;
    if (top < el.scrollTop || top > el.scrollTop + el.clientHeight - ROW_H) el.scrollTop = Math.max(0, top - el.clientHeight / 3);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rows]);

  const first = Math.max(0, Math.floor(scroll / ROW_H) - 8);
  const last = Math.min(rows.length, Math.ceil((scroll + height) / ROW_H) + 8);
  const slice = rows.slice(first, last);

  return (
    <div className="story-scroller" ref={ref} onScroll={(e) => setScroll(e.currentTarget.scrollTop)} style={{ opacity: loading ? 0.6 : 1 }}>
      <div className="story-inner" style={{ height: rows.length * ROW_H }}>
        {slice.map((r, j) => {
          const i = first + j;
          const prev = rows[i - 1];
          const showTime = !prev || prev.ts === null || r.ts === null || Math.floor(prev.ts / 1000) !== Math.floor(r.ts / 1000);
          const isLog = r.kind === 'log_error' || r.kind === 'log_signal';
          return (
            <div
              key={r.story_seq}
              className={`story-row ${selected === String(r.story_seq) ? 'selected' : ''}`}
              style={{ top: i * ROW_H, height: ROW_H }}
              onClick={() => onSelect(r)}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => (e.key === 'Enter' || e.key === ' ') && (e.preventDefault(), onSelect(r))}
              aria-label={`${fmtTs(r.ts)} ${r.title}`}
            >
              <div className="story-time">
                {showTime ? (
                  <>
                    {fmtTime(r.ts)}
                    <span className="off">{fmtOffset(r.ts, start)}</span>
                  </>
                ) : null}
              </div>
              <div className="story-spine">
                <span className={nodeClass(r)} />
              </div>
              <div className={`story-card sev-${r.severity}`}>
                <div className="top">
                  <span className="title" title={r.title}>
                    {r.title}
                  </span>
                  {r.count > 1 && <span className="times">×{fmtNum(r.count)}</span>}
                  <span className="grow" />
                  {r.severity !== 'info' && <SeverityBadge sev={r.severity} />}
                  <span className="kind-tag">{KIND_LABEL[r.kind] ?? r.kind}</span>
                </div>
                {r.detail && <div className={`detail ${isLog ? 'mono' : ''}`}>{shortDetail(r.detail)}</div>}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function StepDrawer({ row, cid, start, onClose }: { row: StoryRow | null; cid: string; start: number | null; onClose: () => void }) {
  const hasLog = !!(row?.log_file_path && row.log_seq !== null && row.log_seq !== undefined);
  const ctxState = useAsync(
    (s) => api.logContext(cid, row!.log_file_path!, row!.log_seq!, 15, 30, s),
    [cid, row?.log_file_path, row?.log_seq],
    hasLog,
  );
  useEffect(() => {
    if (!ctxState.data || !row) return;
    const t = setTimeout(() => document.getElementById(`seq-${row.log_seq}`)?.scrollIntoView({ block: 'center' }), 30);
    return () => clearTimeout(t);
  }, [ctxState.data, row]);

  if (!row) return null;
  const links = rowLinks(cid, row);
  return (
    <Drawer
      open
      onClose={onClose}
      title={row.title}
      head={
        <div className="row" style={{ gap: 8, marginTop: 6, alignItems: 'center' }}>
          <SeverityBadge sev={row.severity} />
          <span className="badge plain">{KIND_LABEL[row.kind] ?? row.kind}</span>
          {row.count > 1 && <span className="times">×{fmtNum(row.count)} in this minute</span>}
        </div>
      }
    >
      {row.detail && <FullDetail text={row.detail} />}
      <dl className="kv">
        <dt>Time (UTC)</dt>
        <dd>
          {fmtTs(row.ts, true)} <span className="muted">{fmtOffset(row.ts, start)} from start</span>
        </dd>
        {row.spark_context_id && (
          <>
            <dt>Spark context</dt>
            <dd className="mono">{row.spark_context_id}</dd>
          </>
        )}
        {row.source && (
          <>
            <dt>Source</dt>
            <dd>
              {row.source}
              {row.executor_id ? ` (executor ${row.executor_id})` : ''}
            </dd>
          </>
        )}
        {row.log_file_path && (
          <>
            <dt>Log file</dt>
            <dd className="mono small">
              {row.log_file_path}:{row.log_seq}
            </dd>
          </>
        )}
        <dt>Step</dt>
        <dd>#{row.story_seq}</dd>
      </dl>
      {links.length > 0 && (
        <div>
          <h3 style={{ marginBottom: 8 }}>Go to</h3>
          <EntityChips links={links} />
        </div>
      )}
      {hasLog && (
        <div>
          <div className="row" style={{ justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
            <h3>Surrounding log lines</h3>
            <Link className="btn small" to={to.logLine(cid, row.log_file_path!, row.log_seq!)}>
              Open in Logs
            </Link>
          </div>
          <div className="panel" style={{ maxHeight: 420, overflow: 'auto' }}>
            <Async state={ctxState} label="Loading log lines…">
              {(t) => (t.rows.length ? <LogLines rows={t.rows} targetSeq={row.log_seq} /> : <Empty title="No log lines found" />)}
            </Async>
          </div>
        </div>
      )}
    </Drawer>
  );
}

/** Configuration and telemetry dumps (one huge line of settings): shown as a one-line label, not the line. */
const DUMP = /UsageLogging|SaferConf\(|Read the dynamic config|"metric":"\w*[Cc]onfig/;
const SHORT = 240;

function shortDetail(d: string): string {
  if (DUMP.test(d)) return `Configuration / telemetry dump (${fmtNum(d.length)} characters): click for the full line`;
  return d.length > SHORT ? `${d.slice(0, SHORT)}… (${fmtNum(d.length)} characters: click for the full line)` : d;
}

function FullDetail({ text }: { text: string }) {
  const long = text.length > 2000;
  const [all, setAll] = useState(!long);
  return (
    <div>
      <pre className="mono small" style={{ background: 'var(--panel-2)', padding: 10, borderRadius: 6, whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: all ? 480 : undefined, overflow: 'auto' }}>
        {all ? text : `${text.slice(0, 2000)}…`}
      </pre>
      {long && (
        <button className="btn small" onClick={() => setAll(!all)}>
          {all ? 'Show less' : `Show all ${fmtNum(text.length)} characters`}
        </button>
      )}
    </div>
  );
}
