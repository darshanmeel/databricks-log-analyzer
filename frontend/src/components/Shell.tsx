import { Fragment, createContext, useContext, useEffect, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { Link, Outlet, useLocation, useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { api, optional, setRunScope, type ClusterListItem, type RunClock, type RunRow, type Summary } from '../api';
import { useAsync } from '../hooks';
import { fmtDuration, fmtNum, fmtTime } from '../format';
import { ErrorState, Loading, StatusBadge } from './ui';
import { runGroup, runName, runOption, usualX } from '../runName';

/* ------------------------------------------------------------ theme */

type Theme = 'light' | 'dark' | 'system';
function readTheme(): Theme {
  try {
    const t = localStorage.getItem('dbx-theme');
    if (t === 'light' || t === 'dark') return t;
  } catch {
    /* storage unavailable */
  }
  return 'system';
}

function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>(readTheme);
  useEffect(() => {
    const el = document.documentElement;
    if (theme === 'system') el.removeAttribute('data-theme');
    else el.setAttribute('data-theme', theme);
    try {
      if (theme === 'system') localStorage.removeItem('dbx-theme');
      else localStorage.setItem('dbx-theme', theme);
    } catch {
      /* ignore */
    }
    window.dispatchEvent(new Event('dbx-theme'));
  }, [theme]);
  return (
    <div className="seg" role="group" aria-label="Theme">
      {(['light', 'system', 'dark'] as Theme[]).map((t) => (
        <button key={t} className={theme === t ? 'on' : ''} onClick={() => setTheme(t)} aria-pressed={theme === t}>
          {t === 'system' ? 'Auto' : t === 'light' ? 'Light' : 'Dark'}
        </button>
      ))}
    </div>
  );
}

/** Re-render charts drawn with resolved CSS colors when the theme changes. */
export function useThemeVersion(): number {
  const [v, setV] = useState(0);
  useEffect(() => {
    const bump = () => setV((x) => x + 1);
    window.addEventListener('dbx-theme', bump);
    const mq = window.matchMedia('(prefers-color-scheme: dark)');
    mq.addEventListener('change', bump);
    return () => {
      window.removeEventListener('dbx-theme', bump);
      mq.removeEventListener('change', bump);
    };
  }, []);
  return v;
}

/* ------------------------------------------------------------ clusters context */

interface ClustersCtx {
  clusters: ClusterListItem[] | undefined;
  error: Error | undefined;
  reload: () => void;
}
const ClustersContext = createContext<ClustersCtx>({ clusters: undefined, error: undefined, reload: () => {} });
export const useClusters = () => useContext(ClustersContext);

interface ClusterCtx {
  cid: string;
  summary: Summary;
}
const ClusterContext = createContext<ClusterCtx | null>(null);
export function useCluster(): ClusterCtx {
  const c = useContext(ClusterContext);
  if (!c) throw new Error('useCluster outside a cluster route');
  return c;
}

/* ------------------------------------------------------------ shell */

function BrandMark() {
  return (
    <span className="brand-mark" aria-hidden="true">
      <svg width="12" height="12" viewBox="0 0 16 16" fill="none">
        <path d="M8 1V15M1 8H15" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    </span>
  );
}

/** The header's breadcrumb slot: the cluster layout fills it (it knows the run). */
const CrumbSlot = createContext<HTMLElement | null>(null);

export function Shell() {
  const st = useAsync(() => api.clusters(), []);
  const [slot, setSlot] = useState<HTMLElement | null>(null);
  return (
    <ClustersContext.Provider value={{ clusters: st.data, error: st.error, reload: st.reload }}>
      <CrumbSlot.Provider value={slot}>
        <div className="shell">
          <header className="header">
            <Link to="/" className="brand">
              <BrandMark />
              <span className="brand-title">Crosshire</span>
            </Link>
            <nav className="crumbs" aria-label="Where you are" ref={setSlot}>
              <Link to="/">Clusters</Link>
            </nav>
            <span className="spacer" />
            <span className="utc-chip" title="Every time on these pages is UTC">UTC</span>
            <ThemeToggle />
          </header>
          <Outlet />
        </div>
      </CrumbSlot.Provider>
    </ClustersContext.Provider>
  );
}

/* ------------------------------------------------------------ cluster layout */

/** The tabs of a run. Pairs of old pages share a tab and switch with a small toggle under it. */
interface Tab {
  label: string;
  routes: string[];
  sub?: [string, string][];
  count?: (r: RunRow | undefined, s: Summary) => number | undefined | null;
  alert?: (r: RunRow | undefined, s: Summary) => boolean;
}

const TABS: Tab[] = [
  { label: 'Overview', routes: [''] },
  { label: 'Findings & errors', routes: ['findings', 'errors'], sub: [['findings', 'Findings'], ['errors', 'Errors']],
    // one problem count: the run's own list says how many; the tab only counts for the whole cluster
    count: (r, s) => (r ? undefined : s.counts.findings), alert: (r, s) => (r ? r.max_severity === 'high' : (s.findings_by_severity.high ?? 0) > 0) },
  { label: 'Queries & stages', routes: ['hierarchy', 'stages', 'queries'], count: (r, s) => (r ? r.spark_jobs : s.counts.spark_jobs),
    alert: (r, s) => (r ? r.status === 'failed' : (s.counts.failed_jobs ?? 0) > 0) },
  { label: 'Events & timeline', routes: ['story', 'timeline'], sub: [['story', 'Events'], ['timeline', 'Timeline']] },
  { label: 'Executors', routes: ['executors'], count: (r, s) => (r ? undefined : s.counts.executors), alert: (r, s) => !r && (s.counts.executors_lost ?? 0) > 0 },
  { label: 'Logs & tables', routes: ['logs', 'data'], sub: [['logs', 'Logs'], ['data', 'Tables (debug)']] },
];

function compact(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(n >= 10_000_000 ? 0 : 1)}M`;
  if (n >= 10_000) return `${Math.round(n / 1000)}k`;
  return fmtNum(n);
}

/* ------------------------------------------------------------ run scope (Revision 6) */

const RUN_KEY = (cid: string) => `dbx-run:${cid}`;
const ALL_RUNS = '__all__';

function readStoredRun(cid: string): string | null {
  try {
    return sessionStorage.getItem(RUN_KEY(cid));
  } catch {
    return null;
  }
}
function storeRun(cid: string, run: string) {
  try {
    sessionStorage.setItem(RUN_KEY(cid), run);
  } catch {
    /* storage unavailable */
  }
}

/** Every page but the Overview, Logs and Tables looks at one run. The Overview with no run picked is the cluster; the
 * analysis pages always have a run (the worst one when none was picked), so there is no "whole cluster" or "group"
 * choice on them. Logs and Tables show the whole cluster when no run is picked. */
const RUN_PAGES = new Set(['story', 'timeline', 'hierarchy', 'findings', 'errors', 'stages', 'queries']);
/** Revision 18: executors are shared by every run on the cluster, so with several runs their page is the cluster's (a
 * run's own work per executor is on its stage, job and query pages). */
const CLUSTER_PAGES = new Set(['executors']);
/** Raw pages that work both ways: the whole cluster (from the cluster's tabs) or cut to one run (from a run's tabs). */
const EITHER_PAGES = new Set(['logs', 'data']);

export interface RunScope { runs: RunRow[]; run: string | null; choose: (r: string | null) => void; clock?: RunClock | null }
const RunScopeContext = createContext<RunScope>({ runs: [], run: null, choose: () => {} });
export const useRunScopeCtx = () => useContext(RunScopeContext);

const usualOf = (r: RunRow) => usualX(r);
/** What "usual" is the median of: earlier runs of the same task on the same table, or this batch's runs of the code. */
const usualWord = (r: RunRow) =>
  r.usual_from === 'history' ? `median of ${r.usual_runs ?? 'its'} earlier runs` : `median of the ${r.same_job_runs ?? ''} runs of this notebook`.replace('  ', ' ');

/** Time lost against its usual: 43 s at 79× a 0.5 s usual matters less than 1h 47m at 3.9× 27 minutes. */
const extraOf = (r: RunRow) => (usualOf(r) !== null && r.typical_duration_ms ? Math.max(0, (r.duration_ms ?? 0) - r.typical_duration_ms) : 0);
/** Slow: at least twice its usual time and at least a minute longer. */
export const isSlow = (r: RunRow) => (usualOf(r) ?? 0) >= 2 && extraOf(r) >= 60_000;

/** Worst first: failed, then incomplete, then the most time lost against its usual, then longest. */
export const worstFirst = (a: RunRow, b: RunRow) =>
  Number(b.status === 'failed') - Number(a.status === 'failed') ||
  Number(b.status === 'incomplete') - Number(a.status === 'incomplete') ||
  extraOf(b) - extraOf(a) ||
  (b.duration_ms ?? 0) - (a.duration_ms ?? 0);

/** The run this cluster's pages are scoped to: URL ?run= > this tab's last choice > none (the cluster). */
function useRunScope(cid: string, section: string) {
  const runsSt = useAsync((s) => optional(api.runs(cid, s)), [cid]);
  const [params] = useSearchParams();
  const urlRun = params.get('run');
  const [picked, setPicked] = useState<string | null>(null);
  useEffect(() => setPicked(null), [cid, urlRun]);
  const data = runsSt.data;
  const runs = data?.runs ?? [];
  let run: string | null = picked ?? urlRun ?? readStoredRun(cid) ?? null;
  if (run === ALL_RUNS || runs.length <= 1 || (run && !runs.some((r) => r.run_key === run))) run = null;
  // a run's page never shows the whole cluster: open the worst run
  if (!run && runs.length > 1 && RUN_PAGES.has(section)) run = [...runs].sort(worstFirst)[0].run_key;
  if (runs.length > 1 && CLUSTER_PAGES.has(section)) run = null;
  setRunScope(run); // read by the API client before the pages below fetch
  const choose = (next: string | null) => {
    storeRun(cid, next ?? ALL_RUNS);
    setPicked(next ?? ALL_RUNS);
  };
  return { runs, run, choose, loaded: !runsSt.loading, clock: data?.clock ?? null };
}

/** Everything a run can be searched by: its name, table, notebook, job name, ids (task run, job run, parent run) and start time. */
const runHaystack = (r: RunRow) =>
  [runName(r), r.label, r.run_key, r.program, r.subject, r.job_name, r.notebook_path, r.parent_run_id, r.databricks_run_id, r.task_run_id,
    r.databricks_job_id, (r as RunRow & { job_run_ids?: string | null }).job_run_ids, r.status, fmtTime(r.start_time).slice(0, 5)]
    .filter(Boolean).join(' ').toLowerCase();

const SHOW_IN_GROUP = 8;

/** How the rail orders runs: worst first, or by total, processing (total minus waiting for cores) or waiting time. */
type RailSort = 'worst' | 'total' | 'processing' | 'waiting';
const RAIL_SORTS: [RailSort, string][] = [['worst', 'Worst first'], ['total', 'Total time'], ['processing', 'Processing time'], ['waiting', 'Waiting time']];
const waitingOf = (r: RunRow) => r.waiting_ms ?? 0;
const processingOf = (r: RunRow) => Math.max(0, (r.duration_ms ?? 0) - waitingOf(r));
const railValue = (r: RunRow, by: RailSort) => (by === 'processing' ? processingOf(r) : by === 'waiting' ? waitingOf(r) : r.duration_ms ?? 0);
const railOrder = (by: RailSort) => (a: RunRow, b: RunRow) => (by === 'worst' ? worstFirst(a, b) : railValue(b, by) - railValue(a, by));
const readRailSort = (): RailSort => { try { const v = localStorage.getItem('rail:sort'); return RAIL_SORTS.some(([k]) => k === v) ? (v as RailSort) : 'worst'; } catch { return 'worst'; } };

function RailItem({ r, on, longest, choose, by }: { r: RunRow; on: boolean; longest: number; choose: (k: string) => void; by: RailSort }) {
  const x = usualOf(r);
  const slow = isSlow(r);
  const v = railValue(r, by);
  return (
    <button className={`rail-item ${on ? 'on' : ''}`} onClick={() => choose(r.run_key)} aria-current={on ? 'true' : undefined}
      title={`${runName(r)} · ${runOption(r)} · took ${fmtDuration(r.duration_ms)}, waited ${fmtDuration(waitingOf(r))}, processed ${fmtDuration(processingOf(r))}`}>
      <span className={`rail-name mono ${r.status === 'failed' ? 'st-crit' : ''}`}>{r.status === 'failed' ? '✕ ' : ''}{r.subject ?? fmtTime(r.start_time)}</span>
      <span className="rail-took">
        {by === 'processing' ? <>{fmtDuration(v)} <span className="rail-unit">ran</span></> : by === 'waiting' ? <>{fmtDuration(v)} <span className="rail-unit">waited</span></> : fmtDuration(r.duration_ms)}
        {x !== null && by !== 'processing' && by !== 'waiting' && <> · <b className={slow ? 'st-warn' : ''}>{x.toFixed(1)}×</b></>}
      </span>
      <span className="rail-bar"><i className={r.status === 'failed' ? 'crit' : slow ? 'warn' : ''} style={{ width: `${Math.max(3, (v / Math.max(1, longest)) * 100)}%` }} /></span>
    </button>
  );
}

/** Runs on this cluster, by program, worst first. Programs whose runs all took about their usual time fold to one line. */
function RunRail({ runs, run, choose }: { runs: RunRow[]; run: string | null; choose: (k: string) => void }) {
  const [q, setQ] = useState('');
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const [by, setBy] = useState<RailSort>(readRailSort);
  const pickSort = (v: RailSort) => { setBy(v); try { localStorage.setItem('rail:sort', v); } catch { /* private window */ } };
  const words = q.trim().toLowerCase().split(/\s+/).filter(Boolean);
  const shown = words.length ? runs.filter((r) => { const h = runHaystack(r); return words.every((w) => h.includes(w)); }) : runs;
  const groups = useMemo(() => {
    const m = new Map<string, RunRow[]>();
    for (const r of shown) m.set(runGroup(r), [...(m.get(runGroup(r)) ?? []), r]);
    const order = railOrder(by);
    return [...m].map(([g, rs]) => ({ g, rs: [...rs].sort(order) })).sort((a, b) => order(a.rs[0], b.rs[0]));
  }, [shown, by]);
  const longest = Math.max(1, ...runs.map((r) => railValue(r, by)));
  return (
    <aside className="rail" aria-label="Runs on this cluster">
      <div className="rail-head">
        <b>Runs on this cluster</b>
        <span className="cnt">{fmtNum(runs.length)}</span>
      </div>
      <input className="input rail-search" placeholder="Name, table, id or 13:05" value={q} aria-label="Search runs"
        onChange={(e) => setQ(e.target.value)}
        onKeyDown={(e) => { if (e.key === 'Enter' && shown[0]) choose(shown[0].run_key); if (e.key === 'Escape') setQ(''); }} />
      <div className="rail-sort">
        <label className="rail-note" htmlFor="rail-sort">Sort</label>
        <select id="rail-sort" className="input rail-sort-select" value={by} onChange={(e) => pickSort(e.target.value as RailSort)}>
          {RAIL_SORTS.map(([k, l]) => <option key={k} value={k}>{l}</option>)}
        </select>
      </div>
      <div className="rail-note">{by === 'processing' ? 'Processing = total time minus waiting for a free core' : by === 'waiting' ? 'Time waiting for a free core' : 'Amber = 2× its usual or more'}</div>
      {!shown.length && <div className="rail-note">No run matches.</div>}
      {groups.map(({ g, rs }) => {
        const slow = rs.filter(isSlow).length;
        const failed = rs.filter((r) => r.status === 'failed').length;
        const usual = rs.find((r) => r.typical_duration_ms)?.typical_duration_ms ?? null;
        const calm = !failed && !slow && !rs.some((r) => r.run_key === run) && rs.length > 1 && !words.length && by === 'worst';
        const isOpen = open[g] ?? !calm;
        const all = open[`${g}:all`] || words.length > 0;
        const top = all ? rs : rs.filter((r, i) => i < SHOW_IN_GROUP || r.run_key === run);
        return (
          <div key={g} className="rail-group">
            <div className="rail-group-head">
              <span className="rail-g">{g}</span>
              <span>{rs.length}{failed ? <b className="st-crit"> · {failed} failed</b> : slow ? <b className="st-warn"> · {slow} slow</b> : ''}</span>
            </div>
            {!isOpen ? (
              <button className="rail-more" onClick={() => setOpen((o) => ({ ...o, [g]: true }))}>
                all {rs.length} ≈ usual{usual ? ` · ${fmtDuration(usual)}` : ''} ▸
              </button>
            ) : (
              <>
                {top.map((r) => <RailItem key={r.run_key} r={r} on={r.run_key === run} longest={longest} choose={choose} by={by} />)}
                {top.length < rs.length && (
                  <button className="rail-more" onClick={() => setOpen((o) => ({ ...o, [`${g}:all`]: true }))}>
                    {rs.length - top.length} more ▸
                  </button>
                )}
              </>
            )}
          </div>
        );
      })}
    </aside>
  );
}

/** Run name, status (only when it did not succeed), × usual, and one meta line. */
function RunTitle({ r }: { r: RunRow }) {
  const x = usualOf(r);
  return (
    <div className="run-title">
      <div className="run-title-row">
        <h1 className="mono">{runName(r)}</h1>
        {r.status !== 'succeeded' ? <StatusBadge status={r.status} /> : <span className="chip ok" title="Spark saw no failure; a failure outside Spark (Python, timeout, cancel) is not in the event log">Succeeded in Spark</span>}
        {x !== null && isSlow(r) && <span className="chip warn">{x.toFixed(1)}× slower than usual</span>}
      </div>
      <div className="run-meta mono">
        {[
          r.databricks_job_id ? `job ${r.databricks_job_id}` : null,
          r.task_run_id || r.databricks_run_id ? `task run ${r.task_run_id ?? r.databricks_run_id}` : r.label,
          `${fmtTime(r.start_time)} → ${fmtTime(r.end_time)}`,
          fmtDuration(r.duration_ms) + (x !== null && r.typical_duration_ms ? ` · ${usualWord(r)} ${fmtDuration(r.typical_duration_ms)}` : ''),
          r.overlapping_runs?.length ? `${r.overlapping_runs.length} other runs overlapped it` : null,
        ].filter(Boolean).join('  ·  ')}
      </div>
    </div>
  );
}

/** The cluster's own tabs: its overview, and the raw pages for the whole cluster. */
function ClusterTabs({ base, section }: { base: string; section: string }) {
  const tabs: [string, string][] = [['', 'Overview'], ['executors', 'Executors (all runs)'], ['logs', 'Logs (whole cluster)'], ['data', 'Tables (debug)']];
  return (
    <div className="tabs" role="tablist">
      {tabs.map(([r, l]) => (
        <Link key={r} to={r ? `${base}/${r}` : base} role="tab" aria-selected={section === r} className={section === r ? 'active' : ''}>{l}</Link>
      ))}
    </div>
  );
}

function RunTabs({ base, section, cur, s, toCluster }: { base: string; section: string; cur: RunRow | undefined; s: Summary; toCluster?: (section: string) => void }) {
  const tab = TABS.find((t) => t.routes.includes(section)) ?? TABS[0];
  // keep the run across tabs (Logs and Tables would otherwise fall back to the whole cluster)
  const keep = cur ? `?run=${encodeURIComponent(cur.run_key)}` : '';
  const href = (r: string) => (r ? `${base}/${r}` : base) + (CLUSTER_PAGES.has(r) ? '' : keep);
  return (
    <>
      <div className="tabs" role="tablist">
        {TABS.filter((t) => !(toCluster && t.routes.some((r) => CLUSTER_PAGES.has(r)))).map((t) => {
          const c = t.count?.(cur, s);
          const alert = t.alert?.(cur, s) ?? false;
          return (
            <Link key={t.label} to={href(t.routes[0])} role="tab" aria-selected={t === tab} className={t === tab ? 'active' : ''}>
              {t.label}
              {c != null && c > 0 && <span className={`cnt ${alert ? 'cnt-crit' : ''}`}>{compact(c)}</span>}
            </Link>
          );
        })}
      </div>
      {tab.sub && (
        <div className="subtabs seg" role="group" aria-label={tab.label}>
          {tab.sub.map(([r, l]) => (
            <Link key={r} to={href(r)} className={section === r ? 'on' : ''}>{l}</Link>
          ))}
        </div>
      )}
      {cur && toCluster && EITHER_PAGES.has(section) && (
        <span className="small muted" style={{ marginLeft: 12 }}>
          Cut to this run's time and executors.{' '}
          <a href={href(section)} onClick={(e) => { e.preventDefault(); toCluster(section); }}>Whole cluster instead</a>
        </span>
      )}
    </>
  );
}

export function ClusterLayout() {
  const { cid = '' } = useParams();
  const st = useAsync((s) => api.summary(cid, s), [cid]);
  const base = `/c/${encodeURIComponent(cid)}`;
  const summary = st.data;
  const loc = useLocation();
  const nav = useNavigate();
  const section = loc.pathname.slice(base.length).split('/')[1] ?? '';
  const scope = useRunScope(cid, section);
  const slot = useContext(CrumbSlot);
  const many = scope.runs.length > 1;
  const cur = many ? scope.runs.find((r) => r.run_key === scope.run) : undefined;
  // the cluster: the Overview with no run picked (the only cluster-wide page)
  const clusterScope = many && !cur;
  const toCluster = (e: React.MouseEvent) => { e.preventDefault(); scope.choose(null); nav(base); };
  const sectionForCluster = (sec: string) => { scope.choose(null); nav(sec ? `${base}/${sec}` : base); };

  const crumbs = slot && createPortal(
    <>
      <span className="sep">/</span>
      {cur ? <a href={base} className="mono" onClick={toCluster}>{cid}</a> : <span className="mono cur">{cid}</span>}
      {cur && <><span className="sep">/</span><span className="mono cur">{runName(cur)}</span></>}
    </>,
    slot,
  );

  const page = !summary ? null : (
    <ClusterContext.Provider value={{ cid, summary }}>
      <RunScopeContext.Provider value={{ runs: scope.runs, run: scope.run, choose: scope.choose, clock: scope.clock }}>
        {summary.empty_reason && (
          <div className="page" style={{ paddingBottom: 0 }}>
            <EmptyCluster reason={summary.empty_reason} />
          </div>
        )}
        {clusterScope && !summary.empty_reason && (
          <div className="run-head">
            <div className="run-title">
              <div className="run-title-row"><h1 className="mono">{cid}</h1><ClusterEnd status={summary.status} runs={scope.runs} end={summary.end_time} /></div>
              <div className="run-meta mono">{fmtTime(summary.start_time)} → {fmtTime(summary.end_time)}  ·  {fmtDuration(summary.duration_ms)}  ·  {fmtNum(scope.runs.length)} runs</div>
            </div>
            <ClusterTabs base={base} section={EITHER_PAGES.has(section) || CLUSTER_PAGES.has(section) ? section : ''} />
          </div>
        )}
        {!clusterScope && !summary.empty_reason && (
          <div className="run-head">
            {cur ? <RunTitle r={cur} /> : (
              <div className="run-title">
                <div className="run-title-row"><h1 className="mono">{cid}</h1>{summary.status !== 'succeeded' && <StatusBadge status={summary.status} />}</div>
                <div className="run-meta mono">{fmtTime(summary.start_time)} → {fmtTime(summary.end_time)}  ·  {fmtDuration(summary.duration_ms)}</div>
              </div>
            )}
            <RunTabs base={base} section={section} cur={cur} s={summary} toCluster={many ? sectionForCluster : undefined} />
          </div>
        )}
        {/* remount the page when the run changes so every view refetches with the new scope */}
        <Fragment key={scope.run ?? ALL_RUNS}>
          {!scope.loaded ? <Loading label="Loading runs…" /> : <Outlet />}
        </Fragment>
      </RunScopeContext.Provider>
    </ClusterContext.Provider>
  );

  return (
    <div className={`body ${many ? 'with-rail' : 'no-nav'}`}>
      {crumbs}
      {many && <RunRail runs={scope.runs} run={scope.run} choose={(k) => scope.choose(k)} />}
      <main className="main">
        {st.error ? (
          <div className="page">
            <ErrorState error={st.error} onRetry={st.reload} />
            <p className="muted" style={{ textAlign: 'center' }}>
              <Link to="/">Back to all clusters</Link>
            </p>
          </div>
        ) : !summary ? (
          <div className="page">
            <Loading label="Loading cluster summary…" />
          </div>
        ) : page}
      </main>
    </div>
  );
}

export function EmptyCluster({ reason }: { reason: string }) {
  return (
    <div className="empty-banner" role="note">
      <div className="glyph" aria-hidden>
        ∅
      </div>
      <div>
        <h3>No cluster logs in this folder</h3>
        <p className="ink2" style={{ marginTop: 4, maxWidth: '80ch' }}>
          {reason}
        </p>
        {!/serverless/i.test(reason) && (
          <p className="muted small" style={{ marginTop: 6 }}>
            Serverless compute produces no cluster logs. This tool covers classic clusters with log delivery configured.
          </p>
        )}

      </div>
    </div>
  );
}

/** How the cluster's work ended: from its runs when it has them (a job cluster stops after its last run; that is not a
 * failure), else from Spark's jobs. */
function ClusterEnd({ status, runs, end }: { status: string; runs: RunRow[]; end: number | null }) {
  if (!runs.length) return status !== 'succeeded' ? <StatusBadge status={status} /> : null;
  const failed = runs.filter((r) => r.status === 'failed').length;
  const last = Math.max(...runs.map((r) => r.end_time ?? 0));
  const after = end && last ? end - last : null;
  return (
    <>
      {failed ? <StatusBadge status="failed" /> : <span className="chip ok">All {fmtNum(runs.length)} runs succeeded</span>}
      {failed ? <span className="muted small">{fmtNum(failed)} of {fmtNum(runs.length)} runs failed</span> : null}
      {after !== null && after >= 0 && after < 10 * 60_000 ? <span className="muted small">the cluster stopped {fmtDuration(after)} after the last run ended</span> : null}
    </>
  );
}
