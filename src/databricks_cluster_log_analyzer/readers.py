"""Streaming file readers and the cluster-folder file classifier/orderer.

Nothing here loads a whole file: `open_text_lines` yields one decoded line at a time, gunzipping transparently
when the file starts with the gzip magic bytes (whatever its name).
"""

from __future__ import annotations

import gzip
import io
import os
import re
import zlib
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import BinaryIO

from .util import fs_path, walk_files

GZIP_MAGIC = b"\x1f\x8b"

# notebook: LOG_FILES = r"/(log4j[^/]*|stdout[^/]*|stderr[^/]*|[^/]*stacktrace[^/]*)$"
LOG_FILE_RE = re.compile(r"^(log4j[^/]*|stdout[^/]*|stderr[^/]*|[^/]*stacktrace[^/]*)$")
EVENTLOG_FILE_RE = re.compile(r"^eventlog[^/]*$")

LOG_FOLDERS = ("driver", "executor", "eventlog")
ACTIVE_NAMES = {"log4j-active.log", "stdout", "stderr", "stacktrace.log", "eventlog"}


def open_binary(path: str | os.PathLike) -> BinaryIO:
    """Open a file for binary reading; gzip-decompress when it starts with the gzip magic bytes."""
    f = open(fs_path(path), "rb")
    head = f.read(2)
    f.seek(0)
    if head == GZIP_MAGIC:
        return gzip.GzipFile(fileobj=f, mode="rb")  # closing the GzipFile does not close f; see open_text_lines
    return f


def open_text_lines(path: str | os.PathLike, on_error: Callable[[str], None] | None = None) -> Iterator[str]:
    """Yield the lines of a (possibly gzipped) text file, utf-8 with errors='replace', without trailing \\r\\n.

    A truncated gzip stream (e.g. a rolled file still being written) ends the iteration instead of raising;
    `on_error` (if given) receives a message.
    """
    raw = open(fs_path(path), "rb")
    try:
        head = raw.read(2)
        raw.seek(0)
        stream: BinaryIO = gzip.GzipFile(fileobj=raw, mode="rb") if head == GZIP_MAGIC else raw
        text = io.TextIOWrapper(stream, encoding="utf-8", errors="replace", newline="\n")
        try:
            for line in text:
                yield line.rstrip("\r\n")
        except (EOFError, gzip.BadGzipFile, zlib.error, OSError) as e:
            if on_error:
                on_error(f"{path}: stopped reading early ({type(e).__name__}: {e})")
    finally:
        raw.close()


# --------------------------------------------------------------------------------------------------------------
# File classification and ordering
# --------------------------------------------------------------------------------------------------------------

def natural_key(s: str) -> list:
    return [(0, int(t), "") if t.isdigit() else (1, 0, t) for t in re.split(r"(\d+)", s) if t != ""]


def _family(name: str) -> int:
    if name.startswith("log4j"):
        return 0
    if name.startswith("stdout"):
        return 1
    if name.startswith("stderr"):
        return 2
    if "stacktrace" in name:
        return 3
    if name.startswith("eventlog"):
        return 4
    return 5


_DATE_IN_NAME_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})(?:-{1,2}(\d{1,2}))?(?:-(\d{1,2}))?")


def rolled_date_key(name: str) -> tuple:
    """(Y, M, D, H, M) found in a rolled file name, missing parts 0; () when the name has no date.

    Names seen: log4j-2026-10-06-18.log.gz, stdout--2026-10-06--18-00, stderr--2026-10-06--18.gz,
    eventlog-2026-10-06--18-00.gz, 2026-10-06-18.stacktrace.log.gz."""
    m = _DATE_IN_NAME_RE.search(name)
    if not m:
        return ()
    return tuple(int(g) if g else 0 for g in m.groups())


def file_sort_key(name: str) -> tuple:
    """Order inside one folder: by family (log4j, stdout, stderr, stacktrace, eventlog); rolled files (by the
    date in their name, then natural sort) before the active file."""
    return (_family(name), 1 if name in ACTIVE_NAMES else 0, rolled_date_key(name), natural_key(name))


def _exec_key(app_id: str, executor_id: str) -> tuple:
    return (app_id, 0 if executor_id.isdigit() else 1, int(executor_id) if executor_id.isdigit() else 0, executor_id)


def folder_of(rel_posix: str) -> str:
    top = rel_posix.split("/", 1)[0]
    return top if top in LOG_FOLDERS else "other"


def log_file_ids(rel_posix: str) -> tuple[str, str | None, str | None]:
    """(source, app_id, executor_id) for a log file path relative to the cluster dir."""
    parts = rel_posix.split("/")
    if parts[0] == "executor":
        app = parts[1] if len(parts) >= 3 else None
        ex = parts[2] if len(parts) >= 4 else None
        return "executor", app, ex
    return "driver", None, None


def spark_context_of(rel_posix: str) -> str:
    """eventlog/<cluster>_<hash>/<spark_context_id>/eventlog* -> spark_context_id (best effort otherwise)."""
    parts = rel_posix.split("/")
    if len(parts) >= 4:
        return parts[2]
    if len(parts) == 3:
        return parts[1]
    return "unknown"


def classify_files(cluster_dir: str | os.PathLike) -> dict[str, list[Path]]:
    """Log and event-log files of a cluster folder, each list in processing (seq) order.

    Returns {"driver": [...], "executor": [...], "eventlog": [...]}. Driver files first; executor files grouped
    by (app_id, numeric executor_id); event logs grouped by spark_context folder; inside a folder rolled files
    come before the active one.
    """
    root = Path(cluster_dir)
    out: dict[str, list[Path]] = {k: [] for k in LOG_FOLDERS}
    for folder in LOG_FOLDERS:
        base = root / folder
        if not base.is_dir():
            continue
        found = []
        for sub_rel, fn in walk_files(base):
            rel = f"{folder}/{sub_rel}"
            p = root / rel
            if True:
                if folder == "eventlog":
                    if EVENTLOG_FILE_RE.match(fn):
                        found.append((rel, p))
                elif LOG_FILE_RE.match(fn):
                    found.append((rel, p))

        def key(item):
            rel, p = item
            parts = rel.split("/")
            name = parts[-1]
            if folder == "executor":
                _, app, ex = log_file_ids(rel)
                return (_exec_key(app or "", ex or ""), "/".join(parts[3:-1]), file_sort_key(name))
            if folder == "eventlog":
                return (natural_key("/".join(parts[1:-1])), file_sort_key(name))
            return ("/".join(parts[1:-1]), file_sort_key(name))

        out[folder] = [p for _, p in sorted(found, key=key)]
    return out


def list_all_files(cluster_dir: str | os.PathLike) -> list[tuple[str, int, float]]:
    """(relative posix path, size, mtime) of every file under the cluster folder, sorted by path."""
    root = Path(cluster_dir)
    rows = []
    for rel, fn in walk_files(root):
        p = root / rel
        if True:
            if fn.startswith(".dbx_"):  # download manifest / partial downloads
                continue
            try:
                st = os.stat(fs_path(p))
            except OSError:
                continue
            rows.append((rel, st.st_size, st.st_mtime))
    rows.sort(key=lambda r: natural_key(r[0]))
    return rows
