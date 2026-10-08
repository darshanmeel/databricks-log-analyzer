"""LocalSource: cluster folders already on local disk."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import BinaryIO

from ..util import fs_path, walk_files
from .base import CLUSTER_ID_RE, FileInfo, LogSource

__all__ = ["LocalSource", "CLUSTER_ID_RE"]


class LocalSource(LogSource):
    remote = False

    def __init__(self, root: str | os.PathLike):
        self.root = Path(os.path.expanduser(str(root)))

    def describe(self) -> str:
        return f"local folder {self.root}"

    def _cluster_dir(self, cluster_id: str) -> Path:
        return self.root / cluster_id

    def list_files(self, cluster_id: str) -> list[FileInfo]:
        base = self._cluster_dir(cluster_id)
        if not base.is_dir():
            raise FileNotFoundError(f"No cluster folder {base}")
        out = []
        for rel, _fn in walk_files(base):
            st = os.stat(fs_path(base / rel))
            out.append(FileInfo(rel, st.st_size, st.st_mtime))
        return sorted(out, key=lambda f: f.path)

    def open(self, cluster_id: str, path: str) -> BinaryIO:
        return open(fs_path(self._cluster_dir(cluster_id) / path), "rb")

    def list_cluster_ids(self) -> list[str]:
        if not self.root.is_dir():
            raise FileNotFoundError(f"No folder {self.root}")
        return sorted(d.name for d in self.root.iterdir() if d.is_dir() and CLUSTER_ID_RE.fullmatch(d.name))

    def last_log_time(self, cluster_id: str) -> float | None:
        d = self._cluster_dir(cluster_id) / "driver"
        try:
            return max((p.stat().st_mtime for p in d.iterdir() if p.is_file()), default=None)
        except OSError:
            return None
