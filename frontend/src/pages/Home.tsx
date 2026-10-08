import { useEffect, useMemo, useState, type FormEvent, type MouseEvent, type ReactNode } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { api, isMissing, type ClusterListItem, type SourceCluster, type SourceInfo, type Summary } from '../api';
import { useClusters } from '../components/Shell';
import { Empty, ErrorState, Loading, Panel, SeverityBadge, StatusBadge } from '../components/ui';
import { fmtDuration, fmtNum, fmtOffset, fmtTs, toMs } from '../format';
import { useAsync } from '../hooks';

/* ------------------------------------------------------------ analyzed clusters */

function FindingsCell({ c }: { c: ClusterListItem }) {
  const f = c.findings_by_severity ?? {};
  const any = (f.high ?? 0) + (f.medium ?? 0) + (f.low ?? 0);
  if (!any) return <span className="muted">None</span>;
  return (
    <div className="chips">
      {(['high', 'medium', 'low'] as const).map((s) => (f[s] ? <SeverityBadge key={s} sev={s} count={f[s]} /> : null))}
    </div>
  );
}

/** Why the cluster is failed (its jobs or queries failed), or that it succeeded after task retries: a failed task
 * attempt that was run again is not a failure. */
function StatusWhy({ c }: { c: ClusterListItem }) {
  const k = c.counts;
  if (!k) return null;
  const fj = k.failed_jobs ?? 0, fq = k.failed_queries ?? 0, ft = k.failed_tasks ?? 0;
  if ((c.status ?? '').toLowerCase() === 'failed') {
    const why = [fj ? `${fmtNum(fj)} ${fj === 1 ? 'job' : 'jobs'}` : null, fq ? `${fmtNum(fq)} ${fq === 1 ? 'query' : 'queries'}` : null].filter(Boolean).join(' and ');
    return why ? <div className="muted small">{why} failed</div> : null;
  }
  return ft ? <div className="muted small">after {fmtNum(ft)} task {ft === 1 ? 'retry' : 'retries'}</div> : null;
}

/** Analyze a cluster again from the raw logs it was built from (the local folder or the download cache), so new
 * analyzer versions apply to it. Disabled when those logs are gone. */
function ReanalyzeCell({ c, onDone }: { c: ClusterListItem; onDone: () => void }) {
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const elapsed = useElapsed(busy);
  const raw = c.raw_available !== false;
  const run = async (e: MouseEvent) => {
    e.stopPropagation();
    setBusy(true);
    setErr(null);
    try {
      await api.reanalyze(c.cluster_id);
      onDone();
    } catch (e2) {
      setErr((e2 as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div onClick={(e) => e.stopPropagation()}>
      <button
        className="btn small"
        type="button"
        disabled={busy || !raw}
        onClick={run}
        title={raw ? 'Analyze again from the raw logs this was built from, with this version of the analyzer' : 'The raw logs it was built from are no longer on disk. Analyze it again from its source above.'}
      >
        {busy && <span className="spinner sm" />}
        {busy ? `Analyzing… ${fmtDuration(elapsed)}` : 'Analyze again'}
      </button>
      {!raw && <div className="muted small">Raw logs gone</div>}
      {err && <div className="inline-error small" role="alert">{err}</div>}
    </div>
  );
}

function ClustersTable({ clusters, reload }: { clusters: ClusterListItem[]; reload: () => void }) {
  const nav = useNavigate();
  if (!clusters.length)
    return <Empty title="No clusters analyzed yet">Pick a source above and analyze a cluster. It shows up here when the analysis finishes.</Empty>;
  return (
    <div className="table-wrap">
      <table className="t">
        <thead>
          <tr>
            <th>Cluster</th>
            <th>Status</th>
            <th>Started (UTC)</th>
            <th className="num">Duration</th>
            <th className="num">Jobs failed</th>
            <th className="num" title="Task attempts that failed. Spark runs a failed task again, so these do not fail the cluster unless a job or query failed too.">Task attempts retried</th>
            <th>Findings</th>
            <th>Analyzed</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {clusters.map((c) => (
            <tr key={c.cluster_id} className="clickable" onClick={() => nav(`/c/${encodeURIComponent(c.cluster_id)}`)}>
              <td>
                <Link className="mono nowrap" to={`/c/${encodeURIComponent(c.cluster_id)}`} onClick={(e) => e.stopPropagation()}>
                  {c.cluster_id}
                </Link>
                {c.empty_reason && <div className="muted small">No cluster logs found</div>}
              </td>
              <td>
                <StatusBadge status={c.status} />
                <StatusWhy c={c} />
              </td>
              <td className="nowrap">{fmtTs(c.start_time)}</td>
              <td className="num">{fmtDuration(c.duration_ms)}</td>
              <td className="num">
                {fmtNum(c.counts?.failed_jobs ?? 0)} / {fmtNum(c.counts?.spark_jobs ?? 0)}
              </td>
              <td className="num">
                {fmtNum(c.counts?.failed_tasks ?? 0)} / {fmtNum(c.counts?.tasks ?? 0)}
              </td>
              <td>
                <FindingsCell c={c} />
              </td>
              <td className="nowrap muted small">{fmtTs(toMs(c.built_at))}</td>
              <td className="nowrap">
                <ReanalyzeCell c={c} onDone={reload} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ------------------------------------------------------------ source → cluster → analyze */

// Used only when the API predates GET /api/sources: maps onto the legacy /api/analyze and /api/download.
const LEGACY_SOURCES: SourceInfo[] = [
  { type: 'local', label: 'Local folder', available: true, reason: null, fields: [] },
  {
    type: 'volume',
    label: 'Databricks volume',
    available: true,
    reason: null,
    fields: [{ name: 'profile', label: 'Profile', placeholder: 'DEFAULT', required: false, secret: false }],
  },
];

const ROOT_HINT: Record<string, { label: string; placeholder: string; hint: string }> = {
  local: { label: 'Log root folder', placeholder: 'C:\\logs\\cluster_logs  or  ./cache', hint: 'The folder one level above the cluster-id folders: for C:\\logs\\0101-000000-abcd1234\\driver the root is C:\\logs. Analyzed in place.' },
  volume: { label: 'Volume path', placeholder: '/Volumes/catalog/schema/volume/cluster_logs', hint: 'New or changed files are copied to the local cache, then analyzed.' },
  adls: { label: 'ADLS path', placeholder: 'abfss://container@account.dfs.core.windows.net/cluster_logs', hint: 'New or changed files are copied to the local cache, then analyzed.' },
  s3: { label: 'S3 path', placeholder: 's3://bucket/cluster_logs', hint: 'New or changed files are copied to the local cache, then analyzed.' },
};

const SOURCE_BLURB: Record<string, string> = {
  local: 'Logs already on this machine',
  volume: 'Unity Catalog volume, read with the Databricks SDK',
  adls: 'Azure Data Lake Storage Gen2',
  s3: 'Amazon S3 bucket',
};

function store(key: string, v?: string): string {
  try {
    if (v === undefined) return localStorage.getItem(key) ?? '';
    localStorage.setItem(key, v);
  } catch {
    /* storage unavailable */
  }
  return v ?? '';
}

function FlowStep({ n, title, state, children, aside }: { n: number; title: string; state: 'done' | 'current' | 'todo'; children: ReactNode; aside?: ReactNode }) {
  return (
    <li className={`flow-step ${state}`}>
      <div className="flow-num" aria-hidden>
        {state === 'done' ? '✓' : n}
      </div>
      <div className="flow-body">
        <div className="flow-title">
          <h3>
            <span className="sr-only">Step {n}: </span>
            {title}
          </h3>
          {aside}
        </div>
        {children}
      </div>
    </li>
  );
}

function useElapsed(running: boolean): number {
  const [t0, setT0] = useState<number | null>(null);
  const [now, setNow] = useState(0);
  useEffect(() => {
    if (!running) {
      setT0(null);
      return;
    }
    const start = Date.now();
    setT0(start);
    setNow(start);
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [running]);
  return t0 === null ? 0 : now - t0;
}

function IngestFlow() {
  const { clusters, reload } = useClusters();
  const nav = useNavigate();
  const src = useAsync((s) => api.sources(s), []);
  const legacy = isMissing(src.error);
  const sources: SourceInfo[] | undefined = legacy ? LEGACY_SOURCES : src.data;

  const [type, setType] = useState<string>(() => store('dbx-src-type'));
  const [root, setRoot] = useState<string>(() => store(`dbx-src-root:${store('dbx-src-type')}`));
  const [options, setOptions] = useState<Record<string, string>>({});
  const [found, setFound] = useState<SourceCluster[] | null>(null);
  const [listing, setListing] = useState(false);
  const [listErr, setListErr] = useState<string | null>(null);
  const [listUnsupported, setListUnsupported] = useState(false);
  const [cid, setCid] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const elapsed = useElapsed(busy);

  const source = sources?.find((s) => s.type === type && s.available);
  // drop a remembered type that is no longer offered
  useEffect(() => {
    if (sources && type && !sources.some((s) => s.type === type && s.available)) setType('');
  }, [sources, type]);

  const chooseType = (t: string) => {
    if (t === type) return;
    setType(t);
    store('dbx-src-type', t);
    setRoot(store(`dbx-src-root:${t}`));
    setOptions({});
    setFound(null);
    setListErr(null);
    setListUnsupported(false);
    setErr(null);
  };

  const analyzedIds = useMemo(() => new Set((clusters ?? []).map((c) => c.cluster_id)), [clusters]);
  // the root has its own input above; the source's other fields are its options
  const optFields = (source?.fields ?? []).filter((f) => f.name !== 'root');
  const missingRequired = optFields.filter((f) => f.required && !options[f.name]?.trim());
  const rootOk = root.trim().length > 0;
  const step2Done = !!source && rootOk && missingRequired.length === 0;
  const remote = type !== 'local';
  const cleanOptions = () => Object.fromEntries(Object.entries(options).filter(([, v]) => v.trim() !== '').map(([k, v]) => [k, v.trim()]));

  const findClusters = async (e?: FormEvent) => {
    e?.preventDefault();
    if (!step2Done) return;
    store(`dbx-src-root:${type}`, root.trim());
    setListing(true);
    setListErr(null);
    setFound(null);
    try {
      const r = await api.sourceClusters({ type, root: root.trim(), options: cleanOptions() });
      setFound(r);
      setListUnsupported(false);
      if (r.length && !cid) setCid(r[0].cluster_id);
    } catch (e2) {
      if (isMissing(e2)) setListUnsupported(true);
      else setListErr((e2 as Error).message);
    } finally {
      setListing(false);
    }
  };

  const analyze = async () => {
    const id = cid.trim();
    if (!step2Done || !id) return;
    store(`dbx-src-root:${type}`, root.trim());
    setBusy(true);
    setErr(null);
    try {
      let summary: Summary | null = null;
      try {
        summary = (await api.ingest({ type, root: root.trim(), cluster_id: id, options: cleanOptions() })).summary;
      } catch (e2) {
        if (!isMissing(e2)) throw e2;
        // older API: fall back to the legacy endpoints
        if (type === 'local') summary = await api.analyze({ log_root: root.trim(), cluster_id: id });
        else if (type === 'volume')
          summary = (await api.download({ volume: root.trim(), cluster_id: id, profile: options.profile?.trim() || null, build: true })).summary;
        else throw e2;
      }
      reload();
      nav(`/c/${encodeURIComponent(summary?.cluster_id || id)}`);
    } catch (e2) {
      setErr((e2 as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const s1: 'done' | 'current' = source ? 'done' : 'current';
  const s2: 'done' | 'current' | 'todo' = !source ? 'todo' : step2Done ? 'done' : 'current';
  const s3: 'done' | 'current' | 'todo' = !step2Done ? 'todo' : cid.trim() ? 'done' : 'current';
  const s4: 'current' | 'todo' = step2Done && cid.trim() ? 'current' : 'todo';
  const rh = ROOT_HINT[type] ?? { label: 'Root path', placeholder: '', hint: 'The folder that holds one sub-folder per cluster id.' };

  return (
    <ol className="flow">
      <FlowStep n={1} title="Where are the logs?" state={s1}>
        {src.error && !legacy ? (
          <ErrorState error={src.error} onRetry={src.reload} />
        ) : !sources ? (
          <Loading label="Loading sources…" />
        ) : (
          <div className="source-cards" role="radiogroup" aria-label="Log source">
            {sources.map((s) => (
              <button
                key={s.type}
                type="button"
                role="radio"
                aria-checked={type === s.type}
                className={`source-card ${type === s.type ? 'on' : ''}`}
                disabled={!s.available}
                onClick={() => chooseType(s.type)}
              >
                <span className="src-label">{s.label}</span>
                <span className="src-blurb">{s.available ? SOURCE_BLURB[s.type] ?? s.type : s.reason ?? 'Not available on this machine'}</span>
              </button>
            ))}
          </div>
        )}
      </FlowStep>

      <FlowStep n={2} title="Root path and connection" state={s2}>
        {!source ? (
          <p className="muted small">Pick a source first.</p>
        ) : (
          <form className="stack" style={{ gap: 12 }} onSubmit={findClusters}>
            <label className="field">
              {rh.label}
              <input className="input mono" value={root} onChange={(e) => setRoot(e.target.value)} placeholder={rh.placeholder} spellCheck={false} />
              <span className="hint">{rh.hint}</span>
            </label>
            {optFields.length > 0 && (
              <div className="form-grid">
                {optFields.map((f) => (
                  <label key={f.name} className="field">
                    <span>
                      {f.label}
                      {!f.required && <span className="muted"> (optional)</span>}
                    </span>
                    <input
                      className="input mono"
                      type={f.secret ? 'password' : 'text'}
                      autoComplete={f.secret ? 'off' : undefined}
                      spellCheck={false}
                      value={options[f.name] ?? ''}
                      placeholder={f.placeholder ?? ''}
                      onChange={(e) => setOptions({ ...options, [f.name]: e.target.value })}
                    />
                  </label>
                ))}
              </div>
            )}
            {/* Enter in this form lists clusters */}
            <button type="submit" hidden />
          </form>
        )}
      </FlowStep>

      <FlowStep
        n={3}
        title="Pick a cluster"
        state={s3}
        aside={
          step2Done && !legacy ? (
            <button className="btn small" type="button" onClick={() => findClusters()} disabled={listing}>
              {listing && <span className="spinner sm" />}
              {listing ? 'Finding clusters…' : found ? 'Find again' : 'Find clusters'}
            </button>
          ) : null
        }
      >
        {!step2Done ? (
          <p className="muted small">Enter the root path first.</p>
        ) : (
          <div className="stack" style={{ gap: 10 }}>
            {listErr && (
              <div className="inline-error" role="alert">
                Could not list clusters
                <pre>{listErr}</pre>
              </div>
            )}
            {(legacy || listUnsupported) && <p className="muted small">This analyzer API cannot list clusters. Type the cluster id below.</p>}
            {found && found.length === 0 && <p className="muted small">No cluster folders found under this root. Check the path, or type a cluster id.</p>}
            {found && found.length > 0 && (
              <div className="cluster-pick" role="radiogroup" aria-label="Clusters found">
                {found.map((c) => {
                  const on = cid.trim() === c.cluster_id;
                  const analyzed = c.analyzed || analyzedIds.has(c.cluster_id);
                  return (
                    <button key={c.cluster_id} type="button" role="radio" aria-checked={on} className={`pick-row ${on ? 'on' : ''}`} onClick={() => setCid(c.cluster_id)}>
                      <span className="radio" aria-hidden />
                      <span className="mono">{c.cluster_id}</span>
                      <span className="muted small nowrap" title={c.last_modified ? `${fmtTs(c.last_modified)} UTC` : undefined}>
                        {c.last_modified ? `changed ${fmtOffset(Date.now(), c.last_modified).replace(/^\+/, '')} ago` : 'last change unknown'}
                      </span>
                      <span className="grow" />
                      {analyzed && <span className="badge good">Analyzed</span>}
                    </button>
                  );
                })}
              </div>
            )}
            <label className="field" style={{ maxWidth: 360 }}>
              {found?.length ? 'Or type a cluster id' : 'Cluster id'}
              <input className="input mono" value={cid} onChange={(e) => setCid(e.target.value)} placeholder="1006-120000-sample01" spellCheck={false} />
            </label>
          </div>
        )}
      </FlowStep>

      <FlowStep n={4} title="Analyze" state={s4}>
        <div className="stack" style={{ gap: 10 }}>
          <div className="row" style={{ alignItems: 'center', gap: 12 }}>
            <button className="btn primary" type="button" disabled={s4 !== 'current' || busy} onClick={analyze}>
              {busy && <span className="spinner sm" />}
              {busy ? (remote ? 'Downloading and analyzing…' : 'Analyzing…') : cid.trim() && analyzedIds.has(cid.trim()) ? 'Analyze again' : 'Analyze'}
            </button>
            {busy ? (
              <span className="muted small">
                {fmtDuration(elapsed)} so far. {remote ? 'Files already in the cache with the same size and time are skipped. ' : ''}Large clusters can take a few minutes.
              </span>
            ) : s4 === 'current' ? (
              <span className="muted small">
                {remote ? 'Copies new or changed log files to the local cache, then analyzes ' : 'Parses the logs and event logs of '}
                <span className="mono">{cid.trim()}</span>.
              </span>
            ) : null}
          </div>
          {err && (
            <div className="inline-error" role="alert">
              Analysis failed
              <pre>{err}</pre>
            </div>
          )}
        </div>
      </FlowStep>
    </ol>
  );
}

export default function Home() {
  const { clusters, error, reload } = useClusters();
  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>Analyze a cluster</h1>
          <p className="sub">
            Point at a folder of Databricks cluster logs, pick the cluster, and get what happened to its jobs, where time went, what failed and
            where to fix it. Times are UTC.
          </p>
        </div>
      </div>
      <div className="stack">
        <Panel>
          <IngestFlow />
        </Panel>
        <Panel
          title="Analyzed clusters"
          note="Newest analysis first"
          flush
          actions={
            <button className="btn small" onClick={reload}>
              Refresh
            </button>
          }
        >
          {error ? <ErrorState error={error} onRetry={reload} /> : clusters === undefined ? <Loading label="Loading clusters…" /> : <ClustersTable clusters={clusters} reload={reload} />}
        </Panel>
      </div>
    </div>
  );
}
