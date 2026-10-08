"""Log source interface: where a cluster's log folder lives (local disk, UC volume, ADLS Gen2, S3)."""

from __future__ import annotations

import io
import re
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import BinaryIO

#: cluster folder names: 0101-000000-abcd1234
CLUSTER_ID_RE = re.compile(r"\d{4}-\d{6}-[a-z0-9]+")


@dataclass(frozen=True)
class FileInfo:
    path: str        # posix, relative to the cluster dir, e.g. "driver/log4j-active.log"
    size: int
    modified: float  # epoch seconds


@dataclass(frozen=True)
class ClusterInfo:
    cluster_id: str
    last_log_time: float | None  # epoch seconds of the newest file directly under driver/ (None if unknown)

    @property
    def last_modified(self) -> float | None:
        return self.last_log_time

    def to_dict(self) -> dict:
        """{cluster_id, last_modified (epoch ms | None)} as the API returns it."""
        return {"cluster_id": self.cluster_id,
                "last_modified": None if self.last_log_time is None else int(self.last_log_time * 1000)}


class MissingDependencyError(ImportError):
    """An optional SDK for a source is not installed; the message says which extra to install."""


def require(module: str, extra: str, what: str):
    """Import `module` or raise MissingDependencyError naming the pip extra."""
    import importlib

    try:
        return importlib.import_module(module)
    except ImportError as e:
        raise MissingDependencyError(
            f"{what} needs the optional package {module.split('.')[0]!r}: "
            f"pip install \"databricks-cluster-log-analyzer[{extra}]\"") from e


class LogSource(ABC):
    """A root that contains one folder per cluster: <root>/<cluster_id>/{driver,executor,eventlog}/..."""

    #: remote sources are downloaded into the cache before building; local ones are built in place
    remote: bool = True

    @abstractmethod
    def list_files(self, cluster_id: str) -> list[FileInfo]:
        """Every file under <root>/<cluster_id>/ (recursive)."""

    @abstractmethod
    def open(self, cluster_id: str, path: str) -> BinaryIO:
        """Binary stream of <root>/<cluster_id>/<path>. Caller closes it."""

    def list_cluster_ids(self) -> list[str]:
        raise NotImplementedError(f"{type(self).__name__} cannot list clusters")

    def last_log_time(self, cluster_id: str) -> float | None:
        """Newest modification time under driver/ (cheap proxy for 'last active', as in notebook step 1a)."""
        return None

    def list_clusters(self, limit: int | None = None) -> list[ClusterInfo]:
        """Cluster folders (names matching CLUSTER_ID_RE), newest driver/ activity first."""
        from concurrent.futures import ThreadPoolExecutor

        ids = self.list_cluster_ids()
        with ThreadPoolExecutor(16) as pool:
            times = list(pool.map(self.last_log_time, ids))
        rows = [ClusterInfo(c, t) for c, t in zip(ids, times)]
        rows.sort(key=lambda r: (r.last_log_time is None, -(r.last_log_time or 0), r.cluster_id))
        return rows[:limit] if limit else rows

    def describe(self) -> str:
        return type(self).__name__


class IterStream(io.RawIOBase):
    """Read-only binary stream over an iterator of byte chunks (SDK download iterators)."""

    def __init__(self, chunks: Iterator[bytes], on_close=None):
        self._it = iter(chunks)
        self._buf = b""
        self._on_close = on_close

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        while not self._buf:
            try:
                self._buf = next(self._it)
            except StopIteration:
                return 0
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n

    def close(self) -> None:
        if not self.closed and self._on_close:
            try:
                self._on_close()
            except Exception:  # noqa: BLE001
                pass
        super().close()
