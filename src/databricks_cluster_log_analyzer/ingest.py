"""Incremental download of a cluster's log folder into the local cache, and the 'latest N clusters' finder."""

from __future__ import annotations

import json
import os
import shutil
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .sources.base import ClusterInfo, FileInfo, LogSource
from .sources.local import LocalSource
from .util import fs_path

MANIFEST = ".dbx_manifest.json"
DEFAULT_FOLDERS = ("driver", "executor", "eventlog")


@dataclass
class DownloadReport:
    cluster_id: str
    source: str
    cache_dir: str
    files_listed: int = 0
    downloaded: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[dict] = field(default_factory=list)
    bytes_downloaded: int = 0
    seconds: float = 0.0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["downloaded_count"] = len(self.downloaded)
        d["skipped_count"] = len(self.skipped)
        d["failed_count"] = len(self.failed)
        return d

    def summary_line(self) -> str:
        return (f"{self.cluster_id}: {len(self.downloaded)} downloaded ({self.bytes_downloaded / 1e6:.1f} MB), "
                f"{len(self.skipped)} skipped (unchanged), {len(self.failed)} failed -> {self.cache_dir}")


def _load_manifest(path: Path) -> dict:
    try:
        return json.loads(path.read_text("utf-8")).get("files", {})
    except (OSError, ValueError):
        return {}


def _save_manifest(path: Path, cluster_id: str, source: str, files: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"cluster_id": cluster_id, "source": source, "updated_at": time.time(),
                               "files": files}, indent=1), "utf-8")
    os.replace(tmp, path)


def download(source: LogSource, cluster_id: str, cache_root: str | os.PathLike,
             folders: tuple[str, ...] | None = DEFAULT_FOLDERS, progress=None) -> DownloadReport:
    """Copy <source>/<cluster_id>/ into <cache_root>/<cluster_id>/, skipping files already cached with the same
    size and modified time (tracked in .dbx_manifest.json). `folders` limits the top-level folders copied
    (default driver, executor, eventlog; None = everything). `progress(msg)` is called per file if given."""
    t0 = time.time()
    dest = Path(cache_root) / cluster_id
    dest.mkdir(parents=True, exist_ok=True)
    manifest_path = dest / MANIFEST
    manifest = _load_manifest(manifest_path)
    report = DownloadReport(cluster_id, source.describe(), str(dest))

    files: list[FileInfo] = source.list_files(cluster_id)
    if folders is not None:
        files = [f for f in files if f.path.split("/", 1)[0] in folders]
    report.files_listed = len(files)

    for i, f in enumerate(files):
        local = dest / f.path
        known = manifest.get(f.path)
        if (known and known.get("size") == f.size and abs(float(known.get("modified", -1)) - f.modified) < 1e-3
                and os.path.isfile(fs_path(local)) and os.path.getsize(fs_path(local)) == f.size):
            report.skipped.append(f.path)
            continue
        os.makedirs(fs_path(local.parent), exist_ok=True)
        tmp = local.with_name(".dbx_part_" + local.name)
        try:
            src = source.open(cluster_id, f.path)
            try:
                with open(fs_path(tmp), "wb") as out:
                    shutil.copyfileobj(src, out, 1 << 20)
            finally:
                try:
                    src.close()
                except Exception:
                    pass
            os.replace(fs_path(tmp), fs_path(local))
            if f.modified:
                os.utime(fs_path(local), (f.modified, f.modified))
            manifest[f.path] = {"size": f.size, "modified": f.modified}
            report.downloaded.append(f.path)
            report.bytes_downloaded += os.path.getsize(fs_path(local))
            if progress:
                progress(f"[{i + 1}/{len(files)}] {f.path} ({f.size / 1e6:.1f} MB)")
            if len(report.downloaded) % 50 == 0:  # checkpoint so an interrupted download resumes
                _save_manifest(manifest_path, cluster_id, report.source, manifest)
        except Exception as e:  # keep going; report the file
            report.failed.append({"path": f.path, "error": f"{type(e).__name__}: {e}"})
            Path(fs_path(tmp)).unlink(missing_ok=True)
    _save_manifest(manifest_path, cluster_id, report.source, manifest)
    report.seconds = round(time.time() - t0, 2)
    return report


def list_clusters(source_root: LogSource | str | os.PathLike, limit: int | None = None) -> list[ClusterInfo]:
    """Cluster folders under a log root, newest first by the latest file time under driver/ (notebook step 1a).

    `source_root` is a LogSource or a local folder path."""
    source = source_root if isinstance(source_root, LogSource) else LocalSource(source_root)
    return source.list_clusters(limit)


@dataclass
class IngestResult:
    download: DownloadReport | None
    build: object  # pipeline.BuildReport
    cluster_dir: str


def ingest(source: LogSource, cluster_id: str, cache_root: str | os.PathLike, output_root: str | os.PathLike, *,
           rules=None, duckdb: bool = False, progress=None) -> IngestResult:
    """Remote source: incremental download into <cache_root>/<cluster_id>/, then build. Local source: build the
    folder in place (no copy)."""
    from .pipeline import build

    report = None
    if getattr(source, "remote", True):
        report = download(source, cluster_id, cache_root, progress=progress)
        if report.files_listed and len(report.failed) == report.files_listed:
            first = report.failed[0]["error"] if report.failed else "?"
            raise RuntimeError(f"{cluster_id}: every file failed to download (first error: {first})")
        cluster_dir = Path(cache_root) / cluster_id
    else:
        cluster_dir = Path(source.root) / cluster_id  # type: ignore[attr-defined]
        if not cluster_dir.is_dir():
            raise FileNotFoundError(f"No cluster folder {cluster_dir}")
    b = build(cluster_dir, output_root, cluster_id=cluster_id, rules=rules, duckdb=duckdb, progress=progress)
    return IngestResult(report, b, str(cluster_dir))
