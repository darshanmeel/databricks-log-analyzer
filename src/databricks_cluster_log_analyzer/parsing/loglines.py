"""Driver / executor log lines: log4j parsing, signals, exception grouping. Pure functions over line iterators.

Ported from the notebook helpers `parse_log_lines`, `find_signals` and `find_errors`.
"""

from __future__ import annotations

import hashlib
import re
from calendar import timegm
from collections.abc import Iterable, Iterator

from ..config import Rules

LOG4J_RE = re.compile(r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) (TRACE|DEBUG|INFO|WARN|ERROR|FATAL) ([^:]+): ?(.*)$")
EXC_HEADER_RE = re.compile(r"^(?:Caused by: |\S+ \S+ \w+ [^:]+: )?([\w$.]*(?:Exception|Error))(?::\s*(.*))?$")
FRAME_RE = re.compile(r"^\s+(at |File \")")
_FRAME = re.compile(r"^\s+(at |app//)")
FINGERPRINT_STRIP_RE = re.compile(r":\d+\)|\d+|0x[0-9a-f]+")

LINE_MAX = 2000
SIGNAL_LINE_MAX = 500
MESSAGE_MAX = 500
TOP_FRAMES = 5

_TS_CACHE: dict[str, int | None] = {}


def parse_ts(s: str) -> int | None:
    """'yy/MM/dd HH:mm:ss' (naive, treated as UTC) -> epoch ms, None if invalid (like Spark try_to_timestamp)."""
    v = _TS_CACHE.get(s)
    if v is not None or s in _TS_CACHE:
        return v
    try:
        yy, mo, dd = int(s[0:2]), int(s[3:5]), int(s[6:8])
        hh, mi, ss = int(s[9:11]), int(s[12:14]), int(s[15:17])
        if not (1 <= mo <= 12 and 1 <= dd <= 31 and hh < 24 and mi < 60 and ss < 60):
            raise ValueError
        import datetime as _dt

        _dt.date(2000 + yy, mo, dd)  # validates day of month
        v = timegm((2000 + yy, mo, dd, hh, mi, ss, 0, 0, 0)) * 1000
    except ValueError:
        v = None
    if len(_TS_CACHE) > 200_000:
        _TS_CACHE.clear()
    _TS_CACHE[s] = v
    return v


# --------------------------------------------------------------------------------------------------------------
# Signals
# --------------------------------------------------------------------------------------------------------------

def _split_alternatives(pattern: str) -> list[str]:
    """Top-level '|' alternatives of a regex (respecting escapes, groups and character classes)."""
    out, cur, depth, i, in_class = [], [], 0, 0, False
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\" and i + 1 < len(pattern):
            cur.append(pattern[i:i + 2])
            i += 2
            continue
        if in_class:
            in_class = ch != "]"
        elif ch == "[":
            in_class = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "|" and depth == 0:
            out.append("".join(cur))
            cur = []
            i += 1
            continue
        cur.append(ch)
        i += 1
    out.append("".join(cur))
    return out


_LITERAL_ESCAPES = set(".()[]{}*+?|\\^$/-: \"'#&%!@=<>,;_~`")


def _longest_literal(alt: str) -> str:
    """Longest run of literal characters that every match of `alt` must contain ('' if unsure)."""
    runs, run, i = [], [], 0
    n = len(alt)

    def cut():
        if run:
            runs.append("".join(run))
            run.clear()

    while i < n:
        ch = alt[i]
        nxt = alt[i + 1] if i + 1 < n else ""
        if ch == "\\":
            if nxt in _LITERAL_ESCAPES and nxt:
                lit, step = nxt, 2
            else:
                cut()
                i += 2
                continue
        elif ch in "([":
            cut()
            # skip the whole group / class
            depth, j, in_class = 0, i, False
            while j < n:
                c = alt[j]
                if c == "\\":
                    j += 2
                    continue
                if in_class:
                    if c == "]":
                        in_class = False
                        if depth == 0:
                            break
                elif c == "[":
                    in_class = True
                elif c == "(":
                    depth += 1
                elif c == ")":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            i = j + 1
            if i < n and alt[i] in "?*+{":
                i += 1
            continue
        elif ch in ".^$)|]":
            cut()
            i += 1
            continue
        elif ch in "?*+{":
            if run:
                run.pop()  # previous char is optional / repeated
            cut()
            if ch == "{":
                i = alt.find("}", i) + 1 or n
            else:
                i += 1
            continue
        else:
            lit, step = ch, 1
        after = alt[i + step] if i + step < n else ""
        if after in ("?", "*", "{"):
            cut()  # this char is optional
            i += step
            continue
        run.append(lit)
        i += step
    cut()
    return max(runs, key=len, default="")


def signal_keywords(patterns: list[str]) -> list[str] | None:
    """Lower-cased literals such that every line matching any pattern contains at least one of them (as a
    case-insensitive substring). None when a pattern has no usable literal (then no prefilter is used)."""
    kws = []
    for p in patterns:
        if p.startswith("(?") and not p.startswith(("(?:", "(?i)")):
            return None
        body = p[4:] if p.startswith("(?i)") else p
        for alt in _split_alternatives(body):
            lit = _longest_literal(alt)
            if len(lit) < 3:
                return None
            kws.append(lit.lower())
    return sorted(set(kws), key=len)


class _SignalMatcher:
    def __init__(self, rules: Rules):
        self.signals = [(s.regex, (s.name, s.severity, s.fix)) for s in rules.signals]
        self.ignore = re.compile(rules.signal_ignore_regex) if rules.signal_ignore_regex else None
        try:
            self.keywords = signal_keywords([s.pattern for s in rules.signals])
        except Exception:  # never let the optimisation break matching
            self.keywords = None
        # One combined search rejects the (vast majority of) lines that match no signal; the per-signal loop
        # then applies the notebook's "first signal in list order wins" rule.
        self.any = None
        if rules.signals:
            try:
                self.any = re.compile("|".join(f"(?:{s.pattern})" for s in rules.signals))
            except re.error:  # e.g. a user pattern with a global inline flag; fall back to the plain loop
                self.any = None

    def find(self, line: str):
        if not self.signals:
            return None
        # a stack frame names the classes on the path, not what happened: the exception's own line does that
        if _FRAME.match(line):
            return None
        if self.keywords is not None:
            low = line.lower()
            for k in self.keywords:
                if k in low:
                    break
            else:
                return None
        elif self.any is not None and self.any.search(line) is None:
            return None
        for rx, result in self.signals:
            if rx.search(line):
                # checked only for lines that matched: config and telemetry dumps are never a signal
                return None if self.ignore is not None and self.ignore.search(line) else result
        return None


_MATCHERS: dict[int, tuple[Rules, _SignalMatcher]] = {}


def _matcher(rules: Rules) -> _SignalMatcher:
    hit = _MATCHERS.get(id(rules))
    if hit is None or hit[0] is not rules:
        hit = (rules, _SignalMatcher(rules))
        _MATCHERS[id(rules)] = hit
    return hit[1]


def find_signal(line: str, rules: Rules) -> tuple[str, str, str] | None:
    """(signal, severity, fix) of the first SIGNALS entry whose regex is found in the raw line, else None."""
    return _matcher(rules).find(line)


# --------------------------------------------------------------------------------------------------------------
# Lines
# --------------------------------------------------------------------------------------------------------------

# JVM unified logging: [2026-10-06T18:00:01.123+0000][1.234s][info][gc] GC(12) Pause Young ...
JVM_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\]((?:\[[^\]]*\])*)\s?(.*)$")
JVM_BRACKET_RE = re.compile(r"\[([^\]]*)\]")
JVM_LEVELS = {"trace": "TRACE", "debug": "DEBUG", "info": "INFO", "warning": "WARN", "warn": "WARN",
              "error": "ERROR"}
# ISO-8601 at line start (Python logging, JVM -XX:+PrintGCDateStamps, ...): 2026-10-06T18:00:01.123456Z ... /
# 2026-10-06 18:00:01,123 ...
ISO_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:[.,](\d{1,9}))?(Z|[+-]\d{2}:?\d{2})?"
                    r"(?=[\s\]|:,-]|$)[\s:|\-]*(.*)$")
ISO_LEVEL_RE = re.compile(r"\b(TRACE|DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL|FATAL)\b")
ISO_LOGGER_RE = re.compile(r"\s+([\w.$\-]+):\s?")
ISO_LEVEL_MAP = {"WARNING": "WARN", "CRITICAL": "FATAL"}


def _offset_ms(off: str | None) -> int:
    if not off or off == "Z":
        return 0
    sign = -1 if off[0] == "-" else 1
    d = off[1:].replace(":", "")
    return sign * (int(d[:2]) * 60 + int(d[2:4] or 0)) * 60000


def parse_iso_ts(s: str) -> int | None:
    """'2026-10-06T18:00:01.123+0000' / '2026-10-06 18:00:01,123' -> epoch ms UTC (naive = UTC); None if bad."""
    m = ISO_RE.match(s)
    if not m:
        return None
    return _iso_from_match(m)


def _iso_from_match(m: re.Match) -> int | None:
    try:
        y, mo, d, h, mi, sec = (int(m.group(i)) for i in range(1, 7))
        if not (1 <= mo <= 12 and 1 <= d <= 31 and h < 24 and mi < 60 and sec < 61):
            return None
        frac = m.group(7)
        ms = int((frac + "00")[:3]) if frac else 0
        return timegm((y, mo, d, h, mi, min(sec, 59), 0, 0, 0)) * 1000 + ms - _offset_ms(m.group(8))
    except (ValueError, OverflowError):
        return None


def parse_line_prefix(line: str):
    """(ts, level, logger, message, kind) of a line with its own timestamp, else None.

    kind = 'log4j' | 'jvm' | 'iso'. Tried in that order (cheap first-character checks first)."""
    c0 = line[:1]
    if c0.isdigit():
        if line[2:3] == "/":
            mm = LOG4J_RE.match(line)
            if mm:
                return parse_ts(mm.group(1)), mm.group(2), mm.group(3), mm.group(4), "log4j"
        elif line[4:5] == "-":
            mm = ISO_RE.match(line)
            if mm:
                ts = _iso_from_match(mm)
                if ts is not None:
                    rest = mm.group(9)
                    level = logger = None
                    lm = ISO_LEVEL_RE.search(rest, 0, 60)
                    if lm:
                        level = ISO_LEVEL_MAP.get(lm.group(1), lm.group(1))
                        gm = ISO_LOGGER_RE.match(rest, lm.end())
                        if gm:
                            logger = gm.group(1)
                    return ts, level, logger, rest, "iso"
    elif c0 == "[" and line[1:2].isdigit():
        mm = JVM_RE.match(line)
        if mm:
            m2 = ISO_RE.match(mm.group(1))
            ts = _iso_from_match(m2) if m2 else None
            if ts is not None:
                level = logger = None
                decos = JVM_BRACKET_RE.findall(mm.group(2))
                for i, d in enumerate(decos):
                    lv = JVM_LEVELS.get(d.strip().lower())
                    if lv:
                        level = lv
                        if i + 1 < len(decos):
                            logger = decos[i + 1].strip() or None
                        break
                return ts, level, logger, mm.group(3), "jvm"
    return None


def _is_continuation(line: str) -> bool:
    """A line without its own timestamp that continues the previous log record (multi-line log4j message,
    stack trace under an ERROR line)."""
    c0 = line[:1]
    if c0 == " " or c0 == "\t":
        return line.strip() != ""
    if line.startswith("Caused by:"):
        return True
    return ("Exception" in line or "Error" in line) and EXC_HEADER_RE.match(line) is not None


def parse_log_file(lines: Iterable[str], *, source: str, executor_id: str | None, app_id: str | None,
                   file_path: str, file_name: str, start_seq: int, rules: Rules,
                   cluster_id: str | None = None) -> Iterator[dict]:
    """One row dict per line, in file order.

    Keys: cluster_id, source, app_id, executor_id, file_path, file_name, seq, line_no, ts (epoch ms or None),
    level, logger, message, line (<= 2000 chars), signal, continuation (bool), plus severity/fix of the signal
    (None when no signal) and the internal `_own_ts` (the line carries its own timestamp).

    Timestamps: log4j `yy/MM/dd HH:mm:ss`, JVM unified logging `[ISO][...][level][tag]`, or ISO-8601 at line
    start. Lines without one inherit the previous timestamp of the same file. A continuation line (indented,
    `Caused by:` or an exception header right after a log record) also inherits level and logger.
    """
    m = _matcher(rules)
    last_ts = None
    prev_level = prev_logger = None
    seq = start_seq
    for line_no, line in enumerate(lines, 1):
        p = parse_line_prefix(line) if line[:1] in "0123456789[" else None
        cont = False
        if p is not None:
            ts, level, logger, message, _kind = p
            if ts is None:
                ts = last_ts
            else:
                last_ts = ts
            own = True
            prev_level, prev_logger = level, logger
        else:
            own = False
            ts, message = last_ts, line
            if prev_level is not None and _is_continuation(line):
                level, logger, cont = prev_level, prev_logger, True
            else:
                level = logger = None
                prev_level = prev_logger = None
        sig = m.find(line)
        yield {
            "cluster_id": cluster_id, "source": source, "app_id": app_id, "executor_id": executor_id,
            "file_path": file_path, "file_name": file_name, "seq": seq, "line_no": line_no, "ts": ts,
            "level": level, "logger": logger,
            "message": message if len(message) <= LINE_MAX else message[:LINE_MAX],
            "line": line if len(line) <= LINE_MAX else line[:LINE_MAX],
            "signal": sig[0] if sig else None,
            "severity": sig[1] if sig else None,
            "fix": sig[2] if sig else None,
            "continuation": cont,
            "_own_ts": own,
        }
        seq += 1


# --------------------------------------------------------------------------------------------------------------
# JVM GC log lines -> gc_events
# --------------------------------------------------------------------------------------------------------------

UNIFIED_GC_RE = re.compile(
    r"^GC\((\d+)\)\s+((?:Pause|Concurrent)[\w ]*?)\s*((?:\((?:[^()]|\([^()]*\))*\)\s*)*)"
    r"(?:(\d+(?:\.\d+)?)([KMG])->(\d+(?:\.\d+)?)([KMG])\((\d+(?:\.\d+)?)([KMG])\)\s+)?(\d+(?:\.\d+)?)ms\s*$")
LEGACY_GC_RE = re.compile(r"\[(Full GC|GC) \(([^)]*)\).*?(\d+)K->(\d+)K\((\d+)K\)\]?,\s*(?:\[[^\]]*\],\s*)*([\d.]+) secs\]")
GC_CAUSE_RE = re.compile(r"\(((?:[^()]|\([^()]*\))*)\)")
_UNIT_MB = {"K": 1 / 1024, "M": 1.0, "G": 1024.0}


def _mb(v, unit):
    return None if v is None else round(float(v) * _UNIT_MB[unit], 3)


def parse_gc_line(row: dict) -> dict | None:
    """GC event fields (gc_id, kind, cause, heap_before_mb, heap_after_mb, heap_total_mb, pause_ms) from a parsed
    log row: JVM unified logging `[gc]` summary lines (JDK 9+) or legacy `-XX:+PrintGCDetails` lines (JDK 8)."""
    logger = row.get("logger")
    if logger is not None:
        if logger != "gc":
            return None
        mm = UNIFIED_GC_RE.match(row["message"] or "")
        if not mm:
            return None
        causes = [c.strip() for c in GC_CAUSE_RE.findall(mm.group(3) or "")]
        return {"gc_id": int(mm.group(1)), "kind": mm.group(2).strip(), "cause": ", ".join(causes) or None,
                "heap_before_mb": _mb(mm.group(4), mm.group(5)), "heap_after_mb": _mb(mm.group(6), mm.group(7)),
                "heap_total_mb": _mb(mm.group(8), mm.group(9)), "pause_ms": float(mm.group(10))}
    line = row["line"]
    if "GC (" not in line:
        return None
    mm = LEGACY_GC_RE.search(line)
    if not mm:
        return None
    return {"gc_id": None, "kind": "Pause Full" if mm.group(1) == "Full GC" else "Pause Young",
            "cause": mm.group(2) or None, "heap_before_mb": _mb(mm.group(3), "K"),
            "heap_after_mb": _mb(mm.group(4), "K"), "heap_total_mb": _mb(mm.group(5), "K"),
            "pause_ms": round(float(mm.group(6)) * 1000, 3)}


# --------------------------------------------------------------------------------------------------------------
# Exceptions
# --------------------------------------------------------------------------------------------------------------

def fingerprint(exception_class: str, frames: list[str]) -> str:
    """sha256 of class + top 3 frames with line numbers, digits and hex addresses removed; first 12 hex chars.
    Same as Spark sha2(concat_ws("|", class, transform(slice(top_frames,1,3), regexp_replace(...))), 256)."""
    parts = [exception_class or ""] + [FINGERPRINT_STRIP_RE.sub("", f) for f in frames[:3]]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:12]


def is_framework_frame(frame: str, prefixes: tuple[str, ...]) -> bool:
    s = frame.strip()
    if s.startswith("at "):
        s = s[3:]
    return s.startswith(prefixes) if prefixes else False


THREAD_DUMP_HEADER_RE = re.compile(r'^"[^"]*".*\b(?:prio|tid|nid)=')
MODULE_FRAME_RE = re.compile(r"^\s*[\w.$]+@[\w.\-]*/")


def is_thread_dump_header(line: str) -> bool:
    return line.startswith("Full thread dump") or (line[:1] == '"' and THREAD_DUMP_HEADER_RE.match(line) is not None)


class ExceptionGrouper:
    """Streaming exception grouping (notebook find_errors, with the Revision 3 contiguity rule). Feed rows in file
    order (several files allowed, each file contiguous); completed exception rows are returned by feed()/flush().

    is_header = line matches EXC_HEADER and not FRAME. A header starts a group that collects the frame lines
    **contiguous** to it: the group stays open over frame lines, `... N more` and any other non-blank line without
    its own timestamp (e.g. a multi-line message), and is closed by a blank line, a line with its own timestamp,
    a JVM thread-dump header or the next exception header (`Caused by:` is a header). Frames after a closed group,
    or before the first header of a file, are dropped. JVM thread dumps (`"name" #12 daemon prio=5 ...` headers,
    or `Full thread dump ...`) start a dump block whose frames are skipped; dumps are counted per file in
    `dump_counts`.
    """

    def __init__(self, rules: Rules, cluster_id: str | None = None):
        self.prefixes = tuple(rules.framework_frame_prefixes)
        self.cluster_id = cluster_id
        self.file_path = None
        self.cur: dict | None = None
        self.in_dump = False
        self.dump_counts: dict[str, int] = {}
        self.record = None  # the last line with its own timestamp: what logged an exception that follows it

    def _emit(self) -> list[dict]:
        g, self.cur = self.cur, None
        if g is None:
            return []
        frames = g.pop("_frames")
        g["top_frames"] = frames
        g["fingerprint"] = fingerprint(g["exception_class"], frames)
        return [g]

    def feed(self, row: dict) -> list[dict]:
        out: list[dict] = []
        if row["file_path"] != self.file_path:
            out = self._emit()
            self.file_path = row["file_path"]
            self.in_dump = False
            self.record = None
        line = row["line"]
        own_ts = row.get("_own_ts")
        if own_ts is None:
            own_ts = row.get("level") is not None and not row.get("continuation")
        c0 = line[:1]
        if (c0 == '"' or c0 == "F") and is_thread_dump_header(line):
            out += self._emit()
            if not self.in_dump:
                self.dump_counts[self.file_path] = self.dump_counts.get(self.file_path, 0) + 1
                self.in_dump = True
            return out
        is_frame = FRAME_RE.match(line) is not None
        if self.in_dump:
            if (is_frame or c0 in (" ", "\t", "") or line.startswith(("java.lang.Thread.State", "- ", "JNI global"))
                    or MODULE_FRAME_RE.match(line) or not line.strip()):
                return out  # thread-dump body: never part of an exception
            self.in_dump = False
        hm = None
        if not is_frame and ("Exception" in line or "Error" in line):
            hm = EXC_HEADER_RE.match(line)
        if hm is not None:
            out += self._emit()
            msg = hm.group(2) or ""
            self.cur = {
                "cluster_id": row.get("cluster_id", self.cluster_id) or self.cluster_id,
                "source": row["source"], "app_id": row.get("app_id"), "executor_id": row.get("executor_id"),
                "file_path": row["file_path"], "file_name": row["file_name"], "seq": row["seq"], "ts": row["ts"],
                "exception_class": hm.group(1), "message": msg[:MESSAGE_MAX],
                "_frames": [], "user_frame": None,
                # not written: the log line that reported it (a listener, a logger), for the benign-by-stack rules
                "logged_by": (line if own_ts else self.record or "")[:300],
            }
            if own_ts:
                self.record = line
            return out
        if own_ts:
            self.record = line
        g = self.cur
        if g is None:
            return out
        if own_ts or not line.strip():
            out += self._emit()  # the group ends: later frames never attach to it
        elif is_frame:
            ts = row["ts"]
            if ts is not None and (g["ts"] is None or ts < g["ts"]):
                g["ts"] = ts
            f = line.strip()
            if len(g["_frames"]) < TOP_FRAMES:
                g["_frames"].append(f)
            if g["user_frame"] is None and not is_framework_frame(f, self.prefixes):
                g["user_frame"] = f
        return out

    def flush(self) -> list[dict]:
        out = self._emit()
        self.file_path = None
        self.in_dump = False
        return out


def extract_errors(rows: Iterable[dict], rules: Rules) -> list[dict]:
    """Exceptions (header + up to 5 frames, fingerprint, user_frame) from parsed rows in file order."""
    g = ExceptionGrouper(rules)
    out: list[dict] = []
    for r in rows:
        out += g.feed(r)
    out += g.flush()
    return out
