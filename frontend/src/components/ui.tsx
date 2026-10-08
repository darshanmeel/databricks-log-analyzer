import { useEffect, useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';
import type { DiagLink, Severity } from '../api';
import { ApiError } from '../api';
import { diagLinkHref, to, type EntityLink } from '../links';
import { fmtNum } from '../format';

const SEV_LABEL: Record<string, string> = { high: 'High', medium: 'Medium', low: 'Low', info: 'Info' };
export const SEV_RANK: Record<string, number> = { high: 0, medium: 1, low: 2, info: 3 };

export function SeverityBadge({ sev, count, label }: { sev: Severity | string | null | undefined; count?: number; label?: string }) {
  const s = (sev ?? 'info') as string;
  return (
    <span className={`badge sev-${s}`}>
      <span className="dot" aria-hidden />
      {label ?? SEV_LABEL[s] ?? s}
      {count !== undefined && <span style={{ fontWeight: 500 }}>{fmtNum(count)}</span>}
    </span>
  );
}

/** Run / job / stage / query status. Pairs an icon glyph with text so color never carries meaning alone. */
export function StatusBadge({ status }: { status: string | null | undefined }) {
  const s = (status ?? 'unknown').toLowerCase();
  if (['failed', 'jobfailed', 'error'].includes(s))
    return (
      <span className="badge sev-high">
        <span aria-hidden>✕</span>Failed
      </span>
    );
  if (['succeeded', 'jobsucceeded', 'success', 'ok'].includes(s))
    return (
      <span className="badge good">
        <span aria-hidden>✓</span>Succeeded
      </span>
    );
  if (s === 'replanned' || s === 'jobreplanned')
    return (
      <span className="badge" title="Adaptive query execution re-planned the query and dropped this work. Not a failure.">
        <span aria-hidden>↻</span>Replanned
      </span>
    );
  if (s === 'incomplete' || s === 'running')
    return (
      <span className="badge sev-low">
        <span aria-hidden>◐</span>Incomplete
      </span>
    );
  return <span className="badge">Unknown</span>;
}

/** Executor removal categories (Revision 3 item 9). Only oom / killed / lost are problems. */
export const REMOVAL: Record<string, { label: string; cls: string; problem: boolean; explain: string }> = {
  autoscale: { label: 'Autoscaled away', cls: '', problem: false, explain: 'Released by autoscaling (idle or downscale). Normal.' },
  termination: { label: 'Cluster stopped', cls: '', problem: false, explain: 'Removed because the application or cluster ended. Normal.' },
  oom: { label: 'Out of memory', cls: 'sev-high', problem: true, explain: 'The executor ran out of memory and was killed.' },
  killed: { label: 'Killed by the OS', cls: 'sev-medium', problem: true, explain: 'Killed with SIGKILL (exit code 9), often by the Linux OOM killer.' },
  lost: { label: 'Lost', cls: 'sev-medium', problem: true, explain: 'Lost without a clean shutdown: spot preemption, heartbeat timeout or a dead worker.' },
  other: { label: 'Removed', cls: '', problem: false, explain: 'Removed for another reason.' },
};
export const isProblemRemoval = (c: string | null | undefined) => !!c && !!REMOVAL[c]?.problem;

export function RemovalBadge({ category, reason }: { category: string | null | undefined; reason?: string | null }) {
  if (!category && !reason) return <span className="muted">–</span>;
  const c = category ?? 'other';
  const meta = REMOVAL[c] ?? { label: c, cls: '', problem: false, explain: '' };
  return (
    <span className={`badge ${meta.cls}`} title={reason ?? meta.explain}>
      {meta.problem ? <span aria-hidden>!</span> : null}
      {meta.label}
    </span>
  );
}

/** "Full table ↗" link to Raw data, shown under charts that summarize a dataset. */
export function DataLink({ cid, dataset, step, q, label }: { cid: string; dataset?: string; step?: string; q?: string | null; label?: string }) {
  return (
    <Link className="data-link" to={to.data(cid, { dataset: dataset ?? null, step: step ?? null, q: q ?? null })}>
      {label ?? 'Full table ↗'}
    </Link>
  );
}

/** Friendly empty state for an endpoint or dataset the backend does not provide yet. */
export function NotAvailable({ what, children }: { what: string; children?: ReactNode }) {
  return (
    <div className="state not-available">
      <h3>{what} is not available for this cluster</h3>
      <p>{children ?? 'The analyzer that built this cluster did not produce it. Re-analyze the cluster with the latest version to fill it in.'}</p>
    </div>
  );
}

export function Loading({ label = 'Loading…' }: { label?: string }) {
  return (
    <div className="loading-row" role="status">
      <span className="spinner" />
      {label}
    </div>
  );
}

export function ErrorState({ error, onRetry }: { error: Error | undefined; onRetry?: () => void }) {
  if (!error) return null;
  const notFound = error instanceof ApiError && error.status === 404;
  return (
    <div className="state error" role="alert">
      <h3>{notFound ? 'Not found' : 'Could not load this view'}</h3>
      <div className="detail">{error.message}</div>
      {notFound && <div style={{ marginTop: 12 }}><Link className="btn small" to="/">Back to the clusters</Link></div>}
      {onRetry && !notFound && (
        <div style={{ marginTop: 12 }}>
          <button className="btn small" onClick={onRetry}>
            Try again
          </button>
        </div>
      )}
    </div>
  );
}

export function Empty({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="state">
      <h3>{title}</h3>
      {children && <p>{children}</p>}
    </div>
  );
}

/** Wrap async content: loading → error → children. */
export function Async<T>({
  state,
  children,
  label,
}: {
  state: { data: T | undefined; error: Error | undefined; loading: boolean; reload: () => void };
  children: (d: T) => ReactNode;
  label?: string;
}) {
  if (state.error) return <ErrorState error={state.error} onRetry={state.reload} />;
  if (state.data === undefined) return <Loading label={label} />;
  return <>{children(state.data)}</>;
}

const LINK_KIND: Record<string, string> = {
  stage: 'Stage',
  query: 'Query',
  executor: 'Exec',
  job: 'Job',
  finding: 'Finding',
  log: 'Log',
  error: 'Error',
};

export function DiagChips({ cid, links }: { cid: string; links: DiagLink[] }) {
  if (!links?.length) return null;
  return (
    <div className="chips">
      {links.map((l, i) => {
        const href = diagLinkHref(cid, l);
        if (!href) return null;
        const kind = LINK_KIND[l.type] ?? l.type;
        const redundant = l.label.toLowerCase().startsWith(l.type) || l.label.toLowerCase().startsWith(kind.toLowerCase());
        const label = redundant ? l.label.charAt(0).toUpperCase() + l.label.slice(1) : l.label;
        return (
          <Link key={i} className="chip" to={href}>
            {!redundant && <span className="k">{kind}</span>}
            {label}
          </Link>
        );
      })}
    </div>
  );
}

export function EntityChips({ links }: { links: EntityLink[] }) {
  if (!links.length) return null;
  return (
    <div className="chips">
      {links.map((l, i) => (
        <Link key={i} className="chip" to={l.href}>
          <span className="k">{LINK_KIND[l.type] ?? l.type}</span>
          {l.label.replace(/^(Stage|Job|Query|Executor|Finding|Error) /, '')}
        </Link>
      ))}
    </div>
  );
}

export function Panel({
  title,
  note,
  actions,
  children,
  flush,
  className,
}: {
  title?: ReactNode;
  note?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  flush?: boolean;
  className?: string;
}) {
  return (
    <section className={`panel ${className ?? ''}`}>
      {(title || actions) && (
        <div className="panel-head">
          <div>
            {title && <h2>{title}</h2>}
            {note && <div className="note">{note}</div>}
          </div>
          {actions && <div className="row" style={{ gap: 8, alignItems: 'center' }}>{actions}</div>}
        </div>
      )}
      <div className={`panel-body ${flush ? 'flush' : ''}`}>{children}</div>
    </section>
  );
}

export function Pager({
  total,
  limit,
  offset,
  onChange,
  label = 'rows',
}: {
  total: number;
  limit: number;
  offset: number;
  onChange: (offset: number) => void;
  label?: string;
}) {
  const from = total === 0 ? 0 : offset + 1;
  const to = Math.min(total, offset + limit);
  const page = Math.floor(offset / limit) + 1;
  const pages = Math.max(1, Math.ceil(total / limit));
  return (
    <div className="pager">
      <span>
        {fmtNum(from)}–{fmtNum(to)} of {fmtNum(total)} {label}
      </span>
      <span className="grow" />
      <button className="btn small" disabled={offset === 0} onClick={() => onChange(0)}>
        First
      </button>
      <button className="btn small" disabled={offset === 0} onClick={() => onChange(Math.max(0, offset - limit))}>
        Previous
      </button>
      <span>
        Page {fmtNum(page)} of {fmtNum(pages)}
      </span>
      <button className="btn small" disabled={to >= total} onClick={() => onChange(offset + limit)}>
        Next
      </button>
      <button className="btn small" disabled={to >= total} onClick={() => onChange((pages - 1) * limit)}>
        Last
      </button>
    </div>
  );
}

export function Drawer({ open, onClose, title, children, head }: { open: boolean; onClose: () => void; title: ReactNode; head?: ReactNode; children: ReactNode }) {
  useEffect(() => {
    if (!open) return;
    const k = (e: KeyboardEvent) => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', k);
    return () => window.removeEventListener('keydown', k);
  }, [open, onClose]);
  if (!open) return null;
  return (
    <>
      <div className="drawer-backdrop" onClick={onClose} />
      <aside className="drawer" role="dialog" aria-modal="true">
        <div className="drawer-head">
          <div className="grow" style={{ minWidth: 0 }}>
            <h2 className="wrap-any">{title}</h2>
            {head}
          </div>
          <button className="btn small ghost" onClick={onClose} aria-label="Close">
            Close
          </button>
        </div>
        <div className="drawer-body">{children}</div>
      </aside>
    </>
  );
}

export interface Col<R> {
  key: string;
  label: ReactNode;
  num?: boolean;
  sortable?: boolean;
  render?: (r: R) => ReactNode;
  breach?: (r: R) => boolean;
  title?: string;
  width?: number | string;
}

export function DataTable<R>({
  cols,
  rows,
  sort,
  desc,
  onSort,
  rowKey,
  onRowClick,
  selectedKey,
  flashKey,
  maxHeight,
}: {
  cols: Col<R>[];
  rows: R[];
  sort?: string | null;
  desc?: boolean;
  onSort?: (key: string) => void;
  rowKey: (r: R) => string;
  onRowClick?: (r: R) => void;
  selectedKey?: string | null;
  flashKey?: string | null;
  maxHeight?: number | string;
}) {
  return (
    <div className="table-wrap" style={maxHeight ? { maxHeight } : undefined}>
      <table className="t">
        <thead>
          <tr>
            {cols.map((c) => (
              <th
                key={c.key}
                className={`${c.num ? 'num' : ''} ${c.sortable && onSort ? 'sortable' : ''}`}
                onClick={c.sortable && onSort ? () => onSort(c.key) : undefined}
                title={c.title}
                style={c.width ? { width: c.width } : undefined}
                aria-sort={sort === c.key ? (desc ? 'descending' : 'ascending') : undefined}
              >
                {c.label}
                {sort === c.key && <span className="arrow">{desc ? '↓' : '↑'}</span>}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const k = rowKey(r);
            return (
              <tr
                key={k}
                id={`row-${k}`}
                className={`${onRowClick ? 'clickable' : ''} ${selectedKey === k ? 'selected' : ''} ${flashKey === k ? 'flash' : ''}`}
                onClick={onRowClick ? () => onRowClick(r) : undefined}
              >
                {cols.map((c) => {
                  const v = c.render ? c.render(r) : String((r as Record<string, unknown>)[c.key] ?? '–');
                  const b = c.breach?.(r);
                  return (
                    <td key={c.key} className={`${c.num ? 'num' : ''} ${b ? 'breach' : ''}`}>
                      {v}
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/** Small toggleable chip group (multi-select). */
export function ToggleChips<T extends string>({
  options,
  value,
  onChange,
  render,
  dot,
}: {
  options: T[];
  value: T[];
  onChange: (v: T[]) => void;
  render?: (o: T) => ReactNode;
  dot?: (o: T) => string | undefined;
}) {
  return (
    <div className="toggle-chips">
      {options.map((o) => {
        const on = value.includes(o);
        const d = dot?.(o);
        return (
          <button
            key={o}
            type="button"
            className={`tchip ${on ? 'on' : ''}`}
            aria-pressed={on}
            onClick={() => onChange(on ? value.filter((x) => x !== o) : [...value, o])}
          >
            {d && <span className="dot" style={{ background: d }} />}
            {render ? render(o) : o}
          </button>
        );
      })}
    </div>
  );
}

export function sevColor(sev: string | null | undefined): string {
  switch (sev) {
    case 'high':
      return 'var(--sev-high)';
    case 'medium':
      return 'var(--sev-medium)';
    case 'low':
      return 'var(--sev-low)';
    default:
      return 'var(--sev-info)';
  }
}

/** Scroll an element with the given id into view once it exists. */
export function useScrollTo(id: string | null | undefined, ready: boolean) {
  useEffect(() => {
    if (!id || !ready) return;
    const t = setTimeout(() => {
      document.getElementById(id)?.scrollIntoView({ block: 'center', behavior: 'smooth' });
    }, 60);
    return () => clearTimeout(t);
  }, [id, ready]);
}

/** One number on an overview. The colour goes on the foot (what is wrong: "12 failed"), not on the total; the long
 * explanation is on hover. */
export function Tile({ label, value, foot, tone, title }: { label: string; value: string; foot?: ReactNode; tone?: 'bad' | 'warn'; title?: string }) {
  return (
    <div className="kpi" title={title}>
      <div className="label">{label}</div>
      <div className="value">{value}</div>
      {foot ? <div className={`foot ${tone ?? ''}`}>{foot}</div> : null}
    </div>
  );
}

/** Everything past the first screen: one folded block, named by what it holds. Its content mounts only when open
 * (so nothing inside fetches until asked); a link to one of its ids (#id) opens it and scrolls there. The open state
 * is remembered per page kind. */
export function Fold({ name, title, what, hint, ids = [], children }: { name: string; title: string; what?: string[]; hint?: string; ids?: string[]; children: ReactNode }) {
  const key = `fold:${name}`;
  const [open, setOpen] = useState(() => { try { return localStorage.getItem(key) === '1'; } catch { return false; } });
  const [goTo, setGoTo] = useState<string | null>(null);
  const toggle = (v: boolean) => { setOpen(v); try { localStorage.setItem(key, v ? '1' : '0'); } catch { /* private window */ } };
  useEffect(() => {
    const check = () => { const h = decodeURIComponent(window.location.hash.slice(1)); if (h && ids.includes(h)) { setOpen(true); setGoTo(h); } };
    check();
    window.addEventListener('hashchange', check);
    return () => window.removeEventListener('hashchange', check);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ids.join('|')]);
  useEffect(() => {
    if (!open || !goTo) return;
    const t = setTimeout(() => { document.getElementById(goTo)?.scrollIntoView({ block: 'start' }); setGoTo(null); }, 50);
    return () => clearTimeout(t);
  }, [open, goTo]);
  return (
    <section className={`fold ${open ? 'open' : ''}`}>
      <button className="fold-head" onClick={() => toggle(!open)} aria-expanded={open}>
        <span className="fold-arrow" aria-hidden>{open ? '▾' : '▸'}</span>
        <b>{title}</b>
        <span className="muted small">{open ? 'hide' : what?.length ? "what's inside:" : hint ?? ''}</span>
      </button>
      {!open && what?.length ? <ul className="fold-list small">{what.map((w) => <li key={w}>{w}</li>)}</ul> : null}
      {open && <div className="stack fold-body">{children}</div>}
    </section>
  );
}
