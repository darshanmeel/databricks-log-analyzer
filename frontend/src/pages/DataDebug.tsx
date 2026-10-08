import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, DATASETS, isMissing, type Params, type Table } from '../api';
import { useCluster } from '../components/Shell';
import { Empty, ErrorState, Loading, NotAvailable, Pager } from '../components/ui';
import { fmtNum, fmtTs } from '../format';
import { useAsync, useDebounced, useQueryState } from '../hooks';
import { to } from '../links';

type Row = Record<string, unknown>;

interface Source {
  kind: 'step' | 'dataset';
  id: string;
  title: string;
  code?: string;
  group: string;
  description: string | null;
  rows: number | null | undefined;
}

const RAW_GROUP = 'Raw datasets';

export default function DataDebug() {
  const { cid, summary } = useCluster();
  const [sp, setQ] = useQueryState();
  const steps = useAsync((s) => api.steps(cid, s), [cid]);
  const stepsMissing = isMissing(steps.error);

  const sources: Source[] = useMemo(() => {
    const out: Source[] = [];
    for (const st of steps.data ?? []) {
      out.push({ kind: 'step', id: st.id, title: st.title, code: st.step, group: st.group || 'Steps', description: st.description, rows: st.rows });
    }
    for (const d of DATASETS) {
      out.push({ kind: 'dataset', id: d.name, title: d.name, group: RAW_GROUP, description: d.note, rows: summary.rows?.[d.name] });
    }
    return out;
  }, [steps.data, summary.rows]);

  const stepParam = sp.get('step');
  const dsParam = sp.get('dataset');
  const fallback = steps.data?.length ? sources[0] : sources.find((s) => s.kind === 'dataset');
  let selected: Source | undefined;
  if (stepParam) selected = sources.find((s) => s.kind === 'step' && s.id === stepParam);
  else if (dsParam)
    selected = sources.find((s) => s.kind === 'dataset' && s.id === dsParam) ?? {
      kind: 'dataset',
      id: dsParam,
      title: dsParam,
      group: RAW_GROUP,
      description: null,
      rows: summary.rows?.[dsParam],
    };
  else if (!steps.loading) selected = fallback;
  // a step id from a link that this backend does not list: still try to load it
  if (!selected && stepParam && !steps.loading)
    selected = { kind: 'step', id: stepParam, title: stepParam, group: 'Steps', description: null, rows: undefined };

  const groups = useMemo(() => {
    const m = new Map<string, Source[]>();
    for (const s of sources) m.set(s.group, [...(m.get(s.group) ?? []), s]);
    return [...m.entries()];
  }, [sources]);

  const pick = (s: Source) => setQ(s.kind === 'step' ? { step: s.id, dataset: null, q: null } : { dataset: s.id, step: null, q: null });

  return (
    <div className="page wide">
      <div className="page-head">
        <div>
          <h1>Tables (debug)</h1>
          <details className="sub"><summary>About this page</summary>
            The intermediate tables behind every view, grouped by the notebook step that produces them. Use these to check a number or dig
            past what the charts show. Click a column to sort, click a row to unwrap long text.
          </details>
        </div>
      </div>
      <div className="data-layout">
        <nav className="panel data-list" aria-label="Steps and datasets">
          {steps.loading && !steps.data && <Loading label="Loading steps…" />}
          {steps.error && !stepsMissing && <ErrorState error={steps.error} onRetry={steps.reload} />}
          {stepsMissing && (
            <p className="muted small data-list-note">The notebook-step view needs a newer analyzer API. The raw datasets below still work.</p>
          )}
          {groups.map(([g, items]) => (
            <div key={g} className="data-group">
              <div className="data-group-title">{g}</div>
              {items.map((s) => {
                const on = selected?.kind === s.kind && selected.id === s.id;
                return (
                  <button key={`${s.kind}:${s.id}`} className={`data-item ${on ? 'on' : ''}`} onClick={() => pick(s)} aria-current={on ? 'true' : undefined}>
                    <span className="code">{s.code ?? ''}</span>
                    <span className={`ttl ${s.kind === 'dataset' ? 'mono' : ''}`}>{s.title}</span>
                    <span className="n">{s.rows === null || s.rows === undefined ? '' : compact(s.rows)}</span>
                  </button>
                );
              })}
            </div>
          ))}
        </nav>
        <section className="panel data-main">
          {!selected ? (
            steps.loading ? <Loading /> : <Empty title="Pick a step or dataset on the left" />
          ) : (
            <SourceView key={`${selected.kind}:${selected.id}`} cid={cid} src={selected} initialQ={sp.get('q') ?? ''} />
          )}
        </section>
      </div>
    </div>
  );
}

function compact(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 10_000) return `${Math.round(n / 1000)}k`;
  return fmtNum(n);
}

const LIMITS = [50, 100, 500];

function SourceView({ cid, src, initialQ }: { cid: string; src: Source; initialQ: string }) {
  const [sort, setSort] = useState<string | null>(null);
  const [desc, setDesc] = useState(false);
  const [text, setText] = useState(initialQ);
  const q = useDebounced(text, 350);
  const [offset, setOffset] = useState(0);
  const [limit, setLimit] = useState(100);
  const [open, setOpen] = useState<number | null>(null);
  useEffect(() => setOffset(0), [sort, desc, q, limit]);

  const st = useAsync(
    (s) => {
      const p: Params = { limit, offset, sort, desc: sort ? desc : null, q: q || null };
      return src.kind === 'step' ? api.step<Row>(cid, src.id, p, s) : api.dataset<Row>(cid, src.id, p, s);
    },
    [cid, src.kind, src.id, offset, sort, desc, q, limit],
  );

  const onSort = (c: string) => {
    if (sort === c) {
      if (!desc) setDesc(true);
      else {
        setSort(null);
        setDesc(false);
      }
    } else {
      setSort(c);
      setDesc(false);
    }
  };

  return (
    <>
      <div className="panel-head">
        <div style={{ minWidth: 0 }}>
          <h2>
            {src.code && <span className="step-code">{src.code}</span>}
            <span className={src.kind === 'dataset' ? 'mono' : ''}>{src.title}</span>
          </h2>
          {src.description && <div className="note data-desc">{src.description}</div>}
        </div>
        <div className="filters">
          <label className="group" style={{ minWidth: 220 }}>
            <span className="lbl">Search all text columns</span>
            <input className="input" value={text} onChange={(e) => setText(e.target.value)} placeholder="Contains…" />
          </label>
          <label className="group">
            <span className="lbl">Rows per page</span>
            <select className="select" value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
              {LIMITS.map((l) => (
                <option key={l} value={l}>
                  {l}
                </option>
              ))}
            </select>
          </label>
          {src.kind === 'dataset' && src.id === 'log_lines' && (
            <Link className="btn" to={to.logs(cid, { q: q || null })}>
              Open in Logs explorer
            </Link>
          )}
        </div>
      </div>
      {st.error ? (
        isMissing(st.error) ? (
          <NotAvailable what={src.kind === 'step' ? `Step ${src.code ?? src.id}` : `Dataset "${src.id}"`} />
        ) : (
          <ErrorState error={st.error} onRetry={st.reload} />
        )
      ) : !st.data ? (
        <Loading label="Loading rows…" />
      ) : (
        <GenericTable t={st.data} sort={sort} desc={desc} onSort={onSort} open={open} onToggle={(i) => setOpen(open === i ? null : i)} loading={st.loading} q={q} limit={limit} offset={offset} onPage={setOffset} />
      )}
    </>
  );
}

function GenericTable({
  t,
  sort,
  desc,
  onSort,
  open,
  onToggle,
  loading,
  q,
  limit,
  offset,
  onPage,
}: {
  t: Table<Row>;
  sort: string | null;
  desc: boolean;
  onSort: (c: string) => void;
  open: number | null;
  onToggle: (i: number) => void;
  loading: boolean;
  q: string;
  limit: number;
  offset: number;
  onPage: (o: number) => void;
}) {
  const cols = t.columns?.length ? t.columns : Object.keys(t.rows[0] ?? {});
  if (!t.rows.length)
    return <Empty title={q ? 'No rows match' : 'This table is empty'}>{q ? 'Clear the search to see every row.' : 'The analyzer produced no rows for this step.'}</Empty>;
  return (
    <>
      <div className="table-wrap generic" style={{ opacity: loading ? 0.6 : 1 }}>
        <table className="t">
          <thead>
            <tr>
              {cols.map((c) => (
                <th key={c} className="sortable" onClick={() => onSort(c)} aria-sort={sort === c ? (desc ? 'descending' : 'ascending') : undefined}>
                  {c}
                  {sort === c && <span className="arrow">{desc ? '↓' : '↑'}</span>}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {t.rows.map((r, i) => (
              <tr key={i} className={`clickable ${open === i ? 'expanded' : ''}`} onClick={() => onToggle(i)}>
                {cols.map((c) => {
                  const v = r[c];
                  const { text, title, num, dim } = fmtCell(c, v);
                  return (
                    <td key={c} className={`${num ? 'num' : ''} ${dim ? 'muted' : ''}`} title={title}>
                      <span className="cell">{text}</span>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Pager total={t.total} limit={limit} offset={offset} onChange={onPage} label="rows" />
    </>
  );
}

const TS_NAME = /(^ts$|^minute$|_time$|_seen$|^added$|^removed$|^start$|^end$|_ts$|^modified$|^built_at$)/;

function fmtCell(col: string, v: unknown): { text: string; title?: string; num?: boolean; dim?: boolean } {
  if (v === null || v === undefined) return { text: '–', dim: true };
  if (typeof v === 'boolean') return { text: v ? 'true' : 'false' };
  if (typeof v === 'number') {
    if (TS_NAME.test(col)) {
      if (v > 1e11) return { text: fmtTs(v, true), title: `${v} (epoch ms, UTC)` };
      if (col === 'modified' && v > 1e8) return { text: fmtTs(v * 1000), title: `${v} (epoch s, UTC)` };
    }
    if (/(^|_)id$|_seq$|^seq$|^line_no$|^gc_id$|attempt$|^task_index$/.test(col)) return { text: String(v), num: true };
    return { text: v.toLocaleString('en-US', { maximumFractionDigits: 3 }), title: String(v), num: true };
  }
  if (Array.isArray(v)) {
    const s = v.map((x) => (typeof x === 'object' ? JSON.stringify(x) : String(x))).join(v.some((x) => String(x).length > 40) ? '\n' : ', ');
    return { text: s, title: s.length > 80 ? s.slice(0, 2000) : undefined };
  }
  if (typeof v === 'object') {
    const s = JSON.stringify(v);
    return { text: s, title: s.slice(0, 2000) };
  }
  const s = String(v);
  return { text: s, title: s.length > 60 ? s.slice(0, 2000) : undefined };
}
