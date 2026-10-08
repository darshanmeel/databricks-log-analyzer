// Overview: the pipeline's "likely root cause chain" drawn as steps, and a "Fix here" card with the code line.
import { Link } from 'react-router-dom';
import { api, optional, type DiagLink, type ErrorGroup, type Summary } from '../api';
import { diagLinkHref, to } from '../links';
import { useAsync } from '../hooks';
import { Panel } from './ui';

interface Step {
  label: string;
  time: string | null;
}

/** "In time order: a (18:03:42) -> b (18:04:10) -> … . Read it left to right…" → steps. */
export function parseChain(text: string | null | undefined): Step[] {
  const m = (text ?? '').match(/In time order:\s*([\s\S]+?)(?:\.\s+Read|\.?$)/);
  if (!m) return [];
  return m[1]
    .split(/\s*->\s*/)
    .map((p) => {
      const t = p.trim().match(/^(.*?)\s*\((\d{1,2}:\d{2}(?::\d{2})?)\)$/);
      return t ? { label: t[1], time: t[2] } : { label: p.trim(), time: null };
    })
    .filter((s) => s.label);
}

const WHAT: { re: RegExp; tone: 'warn' | 'crit'; why: string; where: (cid: string, links: DiagLink[]) => string }[] = [
  { re: /task code/i, tone: 'crit', why: 'Your code raised an error inside a task, on every retry.', where: (cid) => to.errors(cid) },
  { re: /notebook/i, tone: 'crit', why: 'How the failure reached the notebook or job.', where: (cid) => to.errors(cid) },
  { re: /task skew/i, tone: 'warn', why: 'A few tasks ran far longer than the rest.', where: (cid) => to.findings(cid) },
  { re: /tasks too big/i, tone: 'warn', why: 'Each task was given far more data than it is sized for.', where: (cid) => to.findings(cid) },
  { re: /big table read/i, tone: 'warn', why: 'A stage read a lot of a table from storage.', where: (cid) => to.findings(cid) },
  { re: /spill/i, tone: 'warn', why: 'Data did not fit in memory and was written to disk.', where: (cid) => to.timeline(cid) },
  { re: /gc/i, tone: 'warn', why: 'The JVM spent long pauses freeing memory.', where: (cid) => to.executors(cid) },
  { re: /executor lost|decommission|preempt/i, tone: 'warn', why: 'An executor went away, with its shuffle files.', where: (cid) => to.executors(cid) },
  { re: /out of memory|oom/i, tone: 'crit', why: 'An executor ran out of memory and was killed.', where: (cid) => to.executors(cid) },
  { re: /fetch/i, tone: 'crit', why: 'Tasks could not read shuffle data from a lost executor.', where: (cid) => to.errors(cid) },
  { re: /stage/i, tone: 'crit', why: 'A stage ran out of retries.', where: (cid, l) => firstHref(cid, l, 'stage') ?? to.queries(cid) },
  { re: /query|job/i, tone: 'crit', why: 'The query or job stopped here.', where: (cid, l) => firstHref(cid, l, 'query') ?? firstHref(cid, l, 'job') ?? to.queries(cid) },
];

function firstHref(cid: string, links: DiagLink[], type: string): string | null {
  const l = links.find((x) => x.type === type);
  return l ? diagLinkHref(cid, l) : null;
}

export function CauseChain({ cid, s }: { cid: string; s: Summary }) {
  const root = s.diagnosis?.find((d) => d.kind === 'root_cause');
  const steps = parseChain(root?.text);
  if (steps.length < 2) return null;
  // links that name the failed query / job / stage live on the outcome step
  const links = [...(s.diagnosis?.find((d) => d.kind === 'outcome')?.links ?? []), ...(root?.links ?? [])];
  // built from an incident: one finding link per step, in step order, and the incident title before the chain
  const stepFindings = (root?.links ?? []).filter((l) => l.type === 'finding');
  const perStep = stepFindings.length === steps.length;
  const title = root?.text.match(/^(.+?)\.\s+In time order:/)?.[1] ?? null;
  const separately = root?.text.match(/(?:Also|Separately):\s*(.+?)\.?$/)?.[1] ?? null;
  return (
    <Panel
      title={title ? `${s.status === 'failed' ? 'Why it failed' : 'What went wrong'}: ${title}` : 'What led to what'}
      note={
        perStep
          ? 'The most likely root cause first, then what it caused, each linked to the one before by a shared stage, executor or host. Click a step for its evidence.'
          : 'In time order. The first step is the most likely cause; the later ones usually follow from it.'
      }
    >
      <ol className="chain" style={{ gridTemplateColumns: `repeat(${steps.length}, minmax(120px, 1fr))` }}>
        {steps.map((st, i) => {
          const w = WHAT.find((x) => x.re.test(st.label));
          const href = perStep ? to.findings(cid, String(stepFindings[i].id)) : w?.where(cid, links);
          const body = (
            <>
              <span className="chain-time mono">{st.time ?? '–'}</span>
              <span className="chain-label">{st.label.charAt(0).toUpperCase() + st.label.slice(1)}</span>
              {w && <span className="chain-why">{w.why}</span>}
              {i === 0 && <span className="chain-tag">{perStep ? 'Root cause' : 'Started here'}</span>}
              {i === steps.length - 1 && s.status === 'failed' && <span className="chain-tag crit">Ended here</span>}
            </>
          );
          return (
            <li key={i} className={`chain-step ${w?.tone ?? ''}`}>
              {href ? <Link to={href}>{body}</Link> : <div>{body}</div>}
            </li>
          );
        })}
      </ol>
      {separately && (
        <p className="muted small" style={{ margin: '10px 0 0' }}>
          Also: {separately}. <Link to={to.findings(cid)}>All incidents</Link>
        </p>
      )}
    </Panel>
  );
}

/** `File "/Workspace/x/transform.py", line 42, in parse_amount` or `a.b.C.m(File.scala:12)`. */
function parseFrame(f: string): { file: string; line: string | null; fn: string | null } {
  const py = f.match(/File "([^"]+)", line (\d+)(?:, in (\S+))?/);
  if (py) return { file: py[1], line: py[2], fn: py[3] ?? null };
  const jv = f.match(/([\w$.]+)\(([^:()]+):(\d+)\)/);
  if (jv) return { file: jv[2], line: jv[3], fn: jv[1] };
  return { file: f, line: null, fn: null };
}

export function FixHere({ cid, s }: { cid: string; s: Summary }) {
  const st = useAsync((sig) => optional(api.errors(cid, sig)), [cid]);
  const errs = st.data ?? [];
  const e = errs.filter((g) => g.user_frame).sort((a, b) => (a.first_seen ?? 0) - (b.first_seen ?? 0))[0];
  const code = s.diagnosis?.find((d) => d.kind === 'code_location');
  if (!e && !code) return null;
  // A Python wrapper ("Traceback …") hides the real message: use the plain exception logged next to it.
  const near = (g: ErrorGroup) => Math.abs((g.first_seen ?? 0) - (e?.first_seen ?? 0)) <= 2000;
  const plain = (g: ErrorGroup | undefined) => !!g?.sample_message && !/^Traceback/.test(g.sample_message);
  const msg = e ? (plain(e) ? e : errs.find((g) => g !== e && near(g) && !g.exception_class.includes('.') && plain(g)) ?? e) : null;
  const fr = e?.user_frame ? parseFrame(e.user_frame) : null;
  const rest = (code?.text ?? '')
    .split(/;\s*/)
    .filter((p) => !/raised from/.test(p))
    .map((p) => p.trim())
    .filter(Boolean);
  return (
    <Panel
      title={e ? 'Fix here' : 'Where the work came from'}
      note={e ? 'The first line of your own code in a failing stack, and where the failed work was started from.' : 'The notebook or code that started the jobs.'}
    >
      <div className="fix-here">
        {e && fr ? (
          <>
            <div className="fix-loc mono">
              {fr.file}
              {fr.line && <span className="fix-line">:{fr.line}</span>}
              {fr.fn && <span className="muted"> in {fr.fn}()</span>}
            </div>
            {msg && (
              <div className="fix-msg">
                <b>{msg.exception_class.split('.').pop()}</b>
                {plain(msg) ? <>: {msg.sample_message}</> : null}
              </div>
            )}
            <div className="small muted">
              Seen {e.occurrences}× on {e.executors_affected} {e.executors_affected === 1 ? 'executor' : 'executors'}. <Link to={to.errors(cid, e.fingerprint)}>Full stack</Link>
              {e.sample_file_path && e.sample_seq !== null && (
                <>
                  {' · '}
                  <Link to={to.logLine(cid, e.sample_file_path, e.sample_seq)}>Log line</Link>
                </>
              )}
            </div>
          </>
        ) : s.status === 'failed' ? (
          <p className="muted small">No line of your own code appears in the error stacks.</p>
        ) : null}
        {rest.length > 0 && (
          <ul className="fix-more small">
            {rest.map((r, i) => (
              <li key={i}>{r.charAt(0).toUpperCase() + r.slice(1)}</li>
            ))}
          </ul>
        )}
      </div>
    </Panel>
  );
}
