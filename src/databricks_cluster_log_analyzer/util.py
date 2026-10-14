"""Small shared helpers."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal


def is_null(v) -> bool:
    if v is None:
        return True
    try:
        return isinstance(v, float) and math.isnan(v)
    except TypeError:
        return False


def nn(v):
    """NaN -> None."""
    return None if is_null(v) else v


def to_int(v):
    v = nn(v)
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def round_half_up(x, nd: int = 0):
    """Spark F.round semantics (HALF_UP), not Python's banker's rounding."""
    if is_null(x):
        return None
    q = Decimal(1).scaleb(-nd)
    return float(Decimal(repr(float(x))).quantize(q, rounding=ROUND_HALF_UP))


def spark_double_str(x) -> str:
    """How Spark casts a double to string in concat (61.0 -> '61.0')."""
    if is_null(x):
        return ""
    return repr(float(x))


def fmt_ts(ms) -> str:
    if is_null(ms):
        return "?"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fmt_clock(ms) -> str:
    if is_null(ms):
        return "?"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S")


def fmt_ms(ms) -> str:
    if is_null(ms):
        return "?"
    s = ms / 1000
    if s < 60:
        return f"{s:.1f}s"
    if s < 3600:
        return f"{int(s // 60)}m {int(s % 60)}s"
    return f"{int(s // 3600)}h {int(s % 3600 // 60)}m"


def fmt_words(ms) -> str:
    """Duration for sentences, as the UI writes it: '0.4 s', '42 s', '3m 10s', '1h 5m'."""
    if is_null(ms):
        return "?"
    s = ms / 1000
    if s < 10:
        return f"{s:.1f} s"
    if s < 60:
        return f"{round(s)} s"
    s = int(round(s))
    if s < 3600:
        return f"{s // 60}m {s % 60}s" if s % 60 else f"{s // 60}m"
    m = int(round(s / 60))  # rounded to the minute, as the pages show it (12h 16m 40s is 12h 17m)
    return f"{m // 60}h {m % 60}m" if m % 60 else f"{m // 60}h"


def fmt_bytes(b) -> str:
    if is_null(b):
        return "?"
    b = float(b)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(b) < 1024 or unit == "TiB":
            # one rule on every screen (the UI's fmtBytes): whole numbers from 100 up, one decimal below
            return f"{b:.0f} {unit}" if unit == "B" or abs(b) >= 100 else f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TiB"


def lower_median(values) -> int | None:
    """Spark percentile_approx(x, 0.5) on small data: sorted(v)[ceil(0.5*n)-1]."""
    v = sorted(x for x in values if not is_null(x))
    if not v:
        return None
    return v[math.ceil(0.5 * len(v)) - 1]


LONG_PREFIX = "\\\\?\\"  # the Windows extended-length path prefix (backslash backslash ? backslash)


def fs_path(p) -> str:
    """Path string for file I/O. On Windows, absolute paths of 240+ chars get the extended-length prefix so deep
    cache folders work without the LongPathsEnabled registry setting."""
    import os

    s = os.fspath(p)
    if os.name != "nt" or s.startswith(LONG_PREFIX):
        return s
    a = os.path.abspath(s)
    if len(a) < 240:
        return s
    return LONG_PREFIX + "UNC\\" + a[2:] if a.startswith("\\\\") else LONG_PREFIX + a


def walk_files(root):
    """Yield (relative posix path, file name) for every file under root (long-path safe on Windows)."""
    import os

    base = os.path.abspath(os.fspath(root))
    walk_root = LONG_PREFIX + base if os.name == "nt" and not base.startswith("\\\\") else base
    for dirpath, _dirs, files in os.walk(walk_root):
        rel_dir = os.path.relpath(dirpath, walk_root)
        for fn in files:
            rel = fn if rel_dir in (".", "") else os.path.join(rel_dir, fn)
            yield rel.replace(os.sep, "/"), fn


def is_worker(e: dict, executors: list[dict]) -> bool:
    """An executor that runs tasks: every one but the driver, and the driver too in local mode (local[N]), where it is
    the only executor of its Spark context and runs every task."""
    ex = e.get("executor_id")
    if ex is None:
        return False
    if str(ex) != "driver":
        return True
    ctx = e.get("spark_context_id")
    return not any(o is not e and o.get("spark_context_id") == ctx and o.get("executor_id") not in (None, "driver")
                   for o in executors)
