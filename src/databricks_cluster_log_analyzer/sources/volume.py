"""VolumeSource: a Unity Catalog volume path read through the Databricks SDK Files API.

`databricks-sdk` is imported lazily so the rest of the tool works without it. Auth comes from
~/.databrickscfg (`profile`) or the usual DATABRICKS_* environment variables.
"""

from __future__ import annotations

from typing import BinaryIO

from .base import CLUSTER_ID_RE, FileInfo, LogSource, require


class VolumeSource(LogSource):
    def __init__(self, volume_root: str, profile: str | None = None, client=None, host: str | None = None,
                 token: str | None = None):
        root = volume_root.strip().rstrip("/")
        if root.startswith("dbfs:"):
            root = root[len("dbfs:"):]
        if not root.startswith("/Volumes/"):
            raise ValueError(
                f"Expected a UC volume path like /Volumes/<catalog>/<schema>/<volume>/..., got {volume_root!r}")
        self.root = root
        self.profile = profile or None
        self.host = host or None
        self.token = token or None
        self._client = client

    def describe(self) -> str:
        return f"volume {self.root}" + (f" (profile {self.profile})" if self.profile else "")

    @property
    def files_api(self):
        if self._client is None:
            sdk = require("databricks.sdk", "databricks", "The Databricks volume source")  # lazy: optional
            kw = {}
            if self.profile:
                kw["profile"] = self.profile
            if self.host:
                kw["host"] = self.host
            if self.token:
                kw["token"] = self.token
            self._client = sdk.WorkspaceClient(**kw)
        return self._client.files

    def _walk(self, directory: str):
        for entry in self.files_api.list_directory_contents(directory):
            path = entry.path.rstrip("/")
            if entry.is_directory:
                yield from self._walk(path)
            else:
                yield entry

    def list_files(self, cluster_id: str) -> list[FileInfo]:
        base = f"{self.root}/{cluster_id}"
        out = []
        for e in self._walk(base):
            rel = e.path[len(base):].lstrip("/")
            modified = (e.last_modified or 0) / 1000.0  # SDK gives epoch ms
            out.append(FileInfo(rel, int(e.file_size or 0), modified))
        return sorted(out, key=lambda f: f.path)

    def open(self, cluster_id: str, path: str) -> BinaryIO:
        resp = self.files_api.download(f"{self.root}/{cluster_id}/{path}")
        return resp.contents

    def list_cluster_ids(self) -> list[str]:
        ids = []
        for e in self.files_api.list_directory_contents(self.root):
            name = (e.name or e.path.rstrip("/").rsplit("/", 1)[-1]).rstrip("/")
            if e.is_directory and CLUSTER_ID_RE.fullmatch(name):
                ids.append(name)
        return sorted(ids)

    def last_log_time(self, cluster_id: str) -> float | None:
        try:
            times = [e.last_modified or 0
                     for e in self.files_api.list_directory_contents(f"{self.root}/{cluster_id}/driver")
                     if not e.is_directory]
        except Exception:
            return None
        return max(times) / 1000.0 if times else None


def split_volume_cluster_path(path: str) -> tuple[str, str | None]:
    """'/Volumes/c/s/v/cluster_logs/0101-000000-abc' -> ('/Volumes/c/s/v/cluster_logs', '0101-000000-abc')."""
    p = path.rstrip("/")
    last = p.rsplit("/", 1)[-1]
    if CLUSTER_ID_RE.fullmatch(last):
        return p.rsplit("/", 1)[0], last
    return p, None
