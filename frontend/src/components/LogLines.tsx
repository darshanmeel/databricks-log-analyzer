import { Link } from 'react-router-dom';
import type { LogLine } from '../api';
import { fmtTime, fmtTs } from '../format';
import { to } from '../links';

export function sourceLabel(l: Pick<LogLine, 'source' | 'executor_id'>): string {
  if (l.source === 'driver' || !l.executor_id) return 'driver';
  return `exec ${l.executor_id}`;
}

/** Monospace log lines with level colors; `targetSeq` is highlighted. */
export function LogLines({
  rows,
  targetSeq,
  wide,
  cid,
  linkToContext,
}: {
  rows: LogLine[];
  targetSeq?: number | null;
  wide?: boolean;
  cid?: string;
  linkToContext?: boolean;
}) {
  return (
    <div className="loglines" role="log">
      {rows.map((l) => {
        const lvl = l.level ?? '';
        const cont = !!l.continuation;
        const gc = l.logger === 'gc' || (l.logger ?? '').startsWith('gc,');
        const cls = `logline ${wide ? 'wide' : ''} lvl-${lvl} ${l.level ? '' : 'noprefix'} ${cont ? 'cont' : ''} ${gc ? 'gc' : ''} ${targetSeq === l.seq ? 'target' : ''}`;
        return (
          <div key={`${l.file_path}:${l.seq}`} id={`seq-${l.seq}`} className={cls}>
            <span className="seq" title={`${l.file_path} line ${l.line_no}`}>
              {linkToContext && cid ? (
                <Link to={to.logLine(cid, l.file_path, l.seq)} title="Show surrounding lines">
                  {l.line_no}
                </Link>
              ) : (
                l.line_no
              )}
            </span>
            {wide ? (
              <span className="ts" title={fmtTs(l.ts, true)}>
                {l.ts !== null ? fmtTs(l.ts).slice(5) : '–'}
              </span>
            ) : (
              <span className="ts" title={fmtTs(l.ts, true)}>
                {l.ts !== null ? fmtTime(l.ts) : '–'}
              </span>
            )}
            {wide && (
              <span className="src" title={l.file_path}>
                {sourceLabel(l)} {l.file_name}
              </span>
            )}
            <span className="lvl" title={cont ? `Continues the ${lvl || 'previous'} line above` : undefined}>
              {cont ? '' : lvl}
            </span>
            <span className="msg">
              {l.signal && <span className="sig">{l.signal}</span>}
              {cont ? (
                // continuation of a multi-line message: show the raw text, indented under its first line
                <span className="cont-text">{l.line}</span>
              ) : gc ? (
                <>
                  <span className="gc-tag" title="JVM garbage-collection log (unified logging)">
                    {l.logger}
                  </span>
                  {l.message ?? l.line}
                </>
              ) : l.level && l.logger ? (
                <>
                  <span className="muted">{l.logger}: </span>
                  {l.message}
                </>
              ) : (
                l.line
              )}
            </span>
          </div>
        );
      })}
    </div>
  );
}
