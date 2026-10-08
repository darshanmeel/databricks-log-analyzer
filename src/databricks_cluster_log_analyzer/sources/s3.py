"""S3Source: cluster log folders in Amazon S3 (or an S3-compatible store via endpoint_url).

Needs the optional extra: pip install "databricks-cluster-log-analyzer[s3]" (boto3), imported lazily.
Credentials: the default AWS credential chain, or a named `profile`. Root: s3://<bucket>/<prefix>.
"""

from __future__ import annotations

from typing import BinaryIO
from urllib.parse import urlparse

from .base import CLUSTER_ID_RE, FileInfo, LogSource, require

EXTRA = "s3"


def parse_s3_root(root: str) -> tuple[str, str]:
    r = (root or "").strip()
    if not r.startswith(("s3://", "s3a://", "s3n://")):
        raise ValueError(f"Expected an S3 root like s3://<bucket>/<prefix>, got {root!r}")
    u = urlparse(r)
    if not u.netloc:
        raise ValueError(f"No bucket in {root!r}")
    return u.netloc, u.path.strip("/")


class S3Source(LogSource):
    def __init__(self, root: str, profile: str | None = None, region: str | None = None,
                 endpoint_url: str | None = None, client=None):
        self.bucket, self.prefix = parse_s3_root(root)
        self.profile = profile or None
        self.region = region or None
        self.endpoint_url = endpoint_url or None
        self._client = client

    def describe(self) -> str:
        return f"S3 s3://{self.bucket}/{self.prefix}" + (f" (profile {self.profile})" if self.profile else "")

    @property
    def s3(self):
        if self._client is None:
            boto3 = require("boto3", EXTRA, "The S3 source")
            session = boto3.Session(profile_name=self.profile, region_name=self.region)
            self._client = session.client("s3", endpoint_url=self.endpoint_url)
        return self._client

    def _key(self, *parts: str) -> str:
        return "/".join(x.strip("/") for x in (self.prefix, *parts) if x and x.strip("/"))

    def _objects(self, prefix: str, delimiter: str | None = None):
        kw = {"Bucket": self.bucket, "Prefix": prefix}
        if delimiter:
            kw["Delimiter"] = delimiter
        for page in self.s3.get_paginator("list_objects_v2").paginate(**kw):
            yield page

    def list_files(self, cluster_id: str) -> list[FileInfo]:
        base = self._key(cluster_id) + "/"
        out = []
        for page in self._objects(base):
            for o in page.get("Contents", []) or []:
                key = o["Key"]
                if key.endswith("/"):
                    continue
                lm = o.get("LastModified")
                out.append(FileInfo(key[len(base):], int(o.get("Size") or 0), lm.timestamp() if lm else 0.0))
        if not out:
            raise FileNotFoundError(f"No files under s3://{self.bucket}/{base}")
        return sorted(out, key=lambda f: f.path)

    def open(self, cluster_id: str, path: str) -> BinaryIO:
        return self.s3.get_object(Bucket=self.bucket, Key=self._key(cluster_id, path))["Body"]

    def list_cluster_ids(self) -> list[str]:
        base = self._key() + "/" if self.prefix else ""
        ids = []
        for page in self._objects(base, "/"):
            for cp in page.get("CommonPrefixes", []) or []:
                name = cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
                if CLUSTER_ID_RE.fullmatch(name):
                    ids.append(name)
        return sorted(ids)

    def last_log_time(self, cluster_id: str) -> float | None:
        try:
            times = [o["LastModified"].timestamp()
                     for page in self._objects(self._key(cluster_id, "driver") + "/", "/")
                     for o in page.get("Contents", []) or [] if o.get("LastModified")]
        except Exception:  # noqa: BLE001
            return None
        return max(times) if times else None
