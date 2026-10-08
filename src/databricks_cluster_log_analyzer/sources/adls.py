"""ADLSSource: cluster log folders in Azure Data Lake Storage Gen2.

Needs the optional extra: pip install "databricks-cluster-log-analyzer[adls]" (azure-storage-file-datalake +
azure-identity), imported lazily. Credentials: `account_key` or `sas_token` when given, else
DefaultAzureCredential (az login, managed identity, environment variables ...).

Root forms:
  abfss://<container>@<account>.dfs.core.windows.net/<path>
  https://<account>.dfs.core.windows.net/<container>/<path>   (blob.core.windows.net is accepted too)
  <path>  together with the account_name and container options
"""

from __future__ import annotations

import re
from typing import BinaryIO
from urllib.parse import urlparse

from .base import CLUSTER_ID_RE, FileInfo, IterStream, LogSource, require

EXTRA = "adls"


def parse_adls_root(root: str, account_name: str | None = None, container: str | None = None
                    ) -> tuple[str, str, str]:
    """(account, container, path prefix without leading/trailing '/')."""
    r = (root or "").strip()
    acct, cont, path = account_name or None, container or None, r
    if r.startswith(("abfss://", "abfs://")):
        u = urlparse(r)
        if "@" not in u.netloc:
            raise ValueError(f"Expected abfss://<container>@<account>.dfs.core.windows.net/<path>, got {root!r}")
        cont_u, host = u.netloc.split("@", 1)
        acct, cont, path = acct or host.split(".", 1)[0], cont or cont_u, u.path
    elif r.startswith(("https://", "http://")):
        u = urlparse(r)
        parts = u.path.strip("/").split("/", 1)
        acct = acct or u.netloc.split(".", 1)[0]
        cont = cont or (parts[0] if parts and parts[0] else None)
        path = parts[1] if len(parts) > 1 else ""
    if not acct or not cont:
        raise ValueError("ADLS root needs an account and a container: use abfss://<container>@<account>"
                         ".dfs.core.windows.net/<path>, or set the account_name and container options")
    if not re.fullmatch(r"[a-z0-9]{3,24}", acct):
        raise ValueError(f"Not a valid storage account name: {acct!r}")
    return acct, cont, path.strip("/")


class ADLSSource(LogSource):
    def __init__(self, root: str, account_name: str | None = None, container: str | None = None,
                 sas_token: str | None = None, account_key: str | None = None, client=None):
        self.account, self.container, self.prefix = parse_adls_root(root, account_name, container)
        self.sas_token = (sas_token or "").lstrip("?") or None
        self.account_key = account_key or None
        self._fs = client  # a FileSystemClient (tests can inject a fake)

    def describe(self) -> str:
        return f"ADLS abfss://{self.container}@{self.account}.dfs.core.windows.net/{self.prefix}"

    @property
    def fs(self):
        if self._fs is None:
            dl = require("azure.storage.filedatalake", EXTRA, "The ADLS source")
            if self.account_key:
                cred = self.account_key
            elif self.sas_token:
                cred = self.sas_token
            else:
                ident = require("azure.identity", EXTRA, "The ADLS source")
                cred = ident.DefaultAzureCredential()
            svc = dl.DataLakeServiceClient(account_url=f"https://{self.account}.dfs.core.windows.net",
                                           credential=cred)
            self._fs = svc.get_file_system_client(self.container)
        return self._fs

    def _p(self, *parts: str) -> str:
        return "/".join(x.strip("/") for x in (self.prefix, *parts) if x and x.strip("/"))

    def list_files(self, cluster_id: str) -> list[FileInfo]:
        base = self._p(cluster_id)
        out = []
        for e in self.fs.get_paths(path=base, recursive=True):
            if getattr(e, "is_directory", False):
                continue
            rel = e.name[len(base):].lstrip("/")
            lm = getattr(e, "last_modified", None)
            out.append(FileInfo(rel, int(getattr(e, "content_length", 0) or 0), lm.timestamp() if lm else 0.0))
        if not out:
            raise FileNotFoundError(f"No files under {self.describe()}/{cluster_id}")
        return sorted(out, key=lambda f: f.path)

    def open(self, cluster_id: str, path: str) -> BinaryIO:
        dl = self.fs.get_file_client(self._p(cluster_id, path)).download_file()
        return IterStream(dl.chunks())

    def list_cluster_ids(self) -> list[str]:
        ids = []
        for e in self.fs.get_paths(path=self.prefix or None, recursive=False):
            name = e.name.rstrip("/").rsplit("/", 1)[-1]
            if getattr(e, "is_directory", False) and CLUSTER_ID_RE.fullmatch(name):
                ids.append(name)
        return sorted(ids)

    def last_log_time(self, cluster_id: str) -> float | None:
        try:
            times = [e.last_modified.timestamp() for e in self.fs.get_paths(path=self._p(cluster_id, "driver"),
                                                                           recursive=False)
                     if not getattr(e, "is_directory", False) and getattr(e, "last_modified", None)]
        except Exception:  # noqa: BLE001
            return None
        return max(times) if times else None
