"""Source registry: source type -> class, label, input fields and an availability check (used by CLI and API)."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from dataclasses import dataclass, field

from .base import LogSource


@dataclass(frozen=True)
class Field:
    name: str
    label: str
    placeholder: str = ""
    required: bool = False
    secret: bool = False

    def to_dict(self) -> dict:
        return {"name": self.name, "label": self.label, "placeholder": self.placeholder or None,
                "required": self.required, "secret": self.secret}


@dataclass(frozen=True)
class SourceType:
    type: str
    label: str
    cls_path: str                      # "module:Class" (imported lazily)
    fields: tuple[Field, ...]          # the first field is always "root"
    modules: tuple[str, ...] = ()      # modules that must be importable
    extra: str | None = None           # pip extra providing them
    options: tuple[str, ...] = field(default=())  # constructor keyword options (besides root)

    def availability(self) -> tuple[bool, str | None]:
        missing = []
        for m in self.modules:
            if sys.modules.get(m) is not None:  # already imported (or injected), even without a module spec
                continue
            try:
                if importlib.util.find_spec(m) is None:
                    missing.append(m)
            except (ImportError, ValueError):
                missing.append(m)
        if missing:
            return False, (f"Needs {', '.join(missing)}: pip install "
                           f"\"databricks-cluster-log-analyzer[{self.extra}]\"")
        return True, None

    def to_dict(self) -> dict:
        ok, reason = self.availability()
        return {"type": self.type, "label": self.label, "available": ok, "reason": reason,
                "fields": [f.to_dict() for f in self.fields]}


SOURCES: dict[str, SourceType] = {
    "local": SourceType(
        "local", "Local folder", "databricks_cluster_log_analyzer.sources.local:LocalSource",
        (Field("root", "Log root folder (contains one folder per cluster id)", "C:/logs/cluster-logs", True),)),
    "volume": SourceType(
        "volume", "Databricks Unity Catalog volume", "databricks_cluster_log_analyzer.sources.volume:VolumeSource",
        (Field("root", "Volume log root", "/Volumes/<catalog>/<schema>/<volume>/cluster_logs", True),
         Field("profile", "Profile in ~/.databrickscfg (blank = SDK default auth)", "DEFAULT"),
         Field("host", "Workspace URL (optional, instead of a profile)", "https://adb-123.4.azuredatabricks.net"),
         Field("token", "Personal access token (optional)", "dapi...", secret=True)),
        modules=("databricks.sdk",), extra="databricks", options=("profile", "host", "token")),
    "adls": SourceType(
        "adls", "Azure Data Lake Storage Gen2", "databricks_cluster_log_analyzer.sources.adls:ADLSSource",
        (Field("root", "Log root", "abfss://<container>@<account>.dfs.core.windows.net/cluster-logs", True),
         Field("account_name", "Storage account (if not in the root)", "mystorageacct"),
         Field("container", "Container (if not in the root)", "logs"),
         Field("sas_token", "SAS token (optional; default: Azure login / DefaultAzureCredential)", "sv=...",
               secret=True),
         Field("account_key", "Account key (optional)", "", secret=True)),
        modules=("azure.storage.filedatalake", "azure.identity"), extra="adls",
        options=("account_name", "container", "sas_token", "account_key")),
    "s3": SourceType(
        "s3", "Amazon S3", "databricks_cluster_log_analyzer.sources.s3:S3Source",
        (Field("root", "Log root", "s3://<bucket>/cluster-logs", True),
         Field("profile", "AWS profile (blank = default credential chain)", "default"),
         Field("region", "Region (optional)", "eu-west-1"),
         Field("endpoint_url", "Endpoint URL (S3-compatible stores only)", "")),
        modules=("boto3",), extra="s3", options=("profile", "region", "endpoint_url")),
}


def describe_sources() -> list[dict]:
    """GET /api/sources payload."""
    return [s.to_dict() for s in SOURCES.values()]


def make_source(type_: str, root: str, options: dict | None = None) -> LogSource:
    """Instantiate a source. Raises ValueError for an unknown type / bad root, MissingDependencyError when the
    optional SDK is missing (the message names the pip extra)."""
    st = SOURCES.get((type_ or "").strip().lower())
    if st is None:
        raise ValueError(f"Unknown source type {type_!r}; choose one of {', '.join(SOURCES)}")
    if not root or not str(root).strip():
        raise ValueError(f"{st.label}: root is required")
    ok, reason = st.availability()
    if not ok:
        from .base import MissingDependencyError

        raise MissingDependencyError(f"{st.label}: {reason}")
    opts = {}
    for k, v in (options or {}).items():
        if k not in st.options:
            raise ValueError(f"{st.label}: unknown option {k!r} (allowed: {', '.join(st.options) or 'none'})")
        if v not in (None, ""):
            opts[k] = str(v).strip()
    mod, cls = st.cls_path.split(":")
    klass = getattr(importlib.import_module(mod), cls)
    return klass(str(root).strip(), **opts)
