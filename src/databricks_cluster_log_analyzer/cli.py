"""dbx-log-analyzer command line: download, build, analyze, clusters, ui.

Sources (Revision 2): --source local|volume|adls|s3 --root <...> [source options]. The old --volume <path> flag still
works and means --source volume --root <path>.
"""

from __future__ import annotations

import json
import logging
import sys
import time
import threading
import webbrowser
from datetime import datetime, timezone
from pathlib import Path

import click

from . import __version__

# CLI flag -> source option name (only passed to sources that accept it)
SOURCE_FLAGS = ("profile", "host", "token", "account_name", "container", "sas_token", "account_key", "region",
                "endpoint_url")


def _echo_lines(lines):
    for line in lines:
        click.echo(line)


def source_options(f):
    """--source/--root/--volume plus the per-source options."""
    opts = [
        click.option("--source", "source_type", type=click.Choice(["local", "volume", "adls", "s3"]),
                     help="Where the cluster log folders are (default: local, or volume with --volume)."),
        click.option("--root", help="Log root containing one folder per cluster id (local folder, "
                                    "/Volumes/..., abfss://..., s3://...)."),
        click.option("--volume", help="Alias for --source volume --root <path> (a path ending in a cluster id also "
                                      "selects that cluster)."),
        click.option("--profile", default=None, help="volume: ~/.databrickscfg profile; s3: AWS profile."),
        click.option("--host", default=None, help="volume: workspace URL (instead of a profile)."),
        click.option("--token", default=None, envvar="DBX_LOG_ANALYZER_TOKEN", help="volume: personal access token."),
        click.option("--account-name", default=None, help="adls: storage account (if not in the root)."),
        click.option("--container", default=None, help="adls: container (if not in the root)."),
        click.option("--sas-token", default=None, envvar="DBX_LOG_ANALYZER_SAS_TOKEN", help="adls: SAS token."),
        click.option("--account-key", default=None, envvar="DBX_LOG_ANALYZER_ACCOUNT_KEY", help="adls: account key."),
        click.option("--region", default=None, help="s3: region."),
        click.option("--endpoint-url", default=None, help="s3: endpoint URL (S3-compatible stores)."),
    ]
    for o in reversed(opts):
        f = o(f)
    return f


class SourceSpec:
    def __init__(self, type_, root, options, cluster_in_path=None):
        self.type, self.root, self.options, self.cluster_in_path = type_, root, options, cluster_in_path

    def make(self):
        from .sources import MissingDependencyError, make_source

        try:
            return make_source(self.type, self.root, self.options)
        except (ValueError, MissingDependencyError) as e:
            raise click.ClickException(str(e)) from e


def _source_spec(kw, *, default_local=True) -> SourceSpec | None:
    """SourceSpec from the common options (pops them from kw); None when neither --root nor --volume is given."""
    from .sources import SOURCES

    type_ = kw.pop("source_type", None)
    root = kw.pop("root", None)
    volume = kw.pop("volume", None)
    raw = {k: kw.pop(k, None) for k in SOURCE_FLAGS}
    cluster_in_path = None
    if volume:
        from .sources.volume import split_volume_cluster_path

        if type_ not in (None, "volume"):
            raise click.UsageError("--volume implies --source volume")
        type_ = "volume"
        root, cluster_in_path = split_volume_cluster_path(volume)
    if not root:
        if type_ and type_ != "local":
            raise click.UsageError(f"--source {type_} needs --root")
        return None
    type_ = type_ or ("local" if default_local else None)
    allowed = SOURCES[type_].options
    ignored = [k for k, v in raw.items() if v and k not in allowed]
    if ignored:
        click.echo(f"note: ignoring {', '.join('--' + k.replace('_', '-') for k in ignored)} for --source {type_}",
                   err=True)
    return SourceSpec(type_, root, {k: v for k, v in raw.items() if v and k in allowed}, cluster_in_path)


def _resolve_input(input_dir, root, cluster):
    if input_dir:
        p = Path(input_dir)
        return p, cluster or p.resolve().name
    if root and cluster:
        return Path(root) / cluster, cluster
    raise click.UsageError("Give --input <dir>/<cluster_id>, or --root <dir> and --cluster <id>.")


def _build(cluster_dir, cluster_id, output, use_duckdb, rules, quiet=False):
    from .pipeline import build

    progress = None if quiet else (lambda m: click.echo(f"  {m}", err=True))
    try:
        report = build(cluster_dir, output, cluster_id=cluster_id, rules=rules, duckdb=use_duckdb, progress=progress)
    except FileNotFoundError as e:
        raise click.ClickException(str(e)) from e
    _echo_lines(report.summary_lines())
    return report


def _cluster_ids(source, spec: SourceSpec, clusters, latest) -> list[str]:
    from .ingest import list_clusters

    ids = list(clusters) or ([spec.cluster_in_path] if spec.cluster_in_path else [])
    if latest:
        try:
            found = list_clusters(source, limit=latest)
        except Exception as e:  # noqa: BLE001  (auth, permissions, path)
            raise click.ClickException(f"listing clusters failed: {type(e).__name__}: {e}") from e
        click.echo(f"latest {len(found)} clusters in {source.describe()}:")
        for c in found:
            click.echo(f"  {c.cluster_id}  last log {_fmt_epoch(c.last_log_time)}")
        ids += [c.cluster_id for c in found if c.cluster_id not in ids]
    if not ids:
        raise click.UsageError("Give --cluster <id> (repeatable) or --latest N.")
    return ids


def _download(spec: SourceSpec, clusters, latest, cache):
    from .ingest import download

    source = spec.make()
    if not source.remote:
        raise click.UsageError("download needs a remote source (--source volume|adls|s3, or --volume); "
                               "a local folder is built in place: use build --root <dir> --cluster <id>")
    reports = []
    for cid in _cluster_ids(source, spec, clusters, latest):
        click.echo(f"downloading {cid} from {source.describe()} ...")
        try:
            rep = download(source, cid, cache, progress=lambda m: click.echo(f"  {m}", err=True))
        except Exception as e:  # show the real error (auth, permissions, path)
            raise click.ClickException(f"{cid}: {type(e).__name__}: {e}") from e
        click.echo(rep.summary_line())
        for f in rep.failed[:10]:
            click.echo(f"  FAILED {f['path']}: {f['error']}")
        reports.append(rep)
    return reports


def _fmt_epoch(t):
    if not t:
        return "?"
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="dbx-log-analyzer")
def main():
    """Databricks Cluster Log Analyzer: cluster logs -> Parquet datasets -> local UI."""


def _common_build_opts(f):
    for o in reversed([
        click.option("--output", default="./output", show_default=True, type=click.Path(file_okay=False)),
        click.option("--duckdb", "use_duckdb", is_flag=True, help="Also load the datasets into <output>/insights.duckdb."),
        click.option("--rules", type=click.Path(dir_okay=False, exists=True), help="Rules TOML overriding the defaults."),
        click.option("--quiet", "-q", is_flag=True, help="No per-file progress."),
    ]):
        f = o(f)
    return f


@main.command()
@click.option("--input", "input_dir", type=click.Path(file_okay=False), help="Cluster folder (<root>/<cluster_id>).")
@source_options
@click.option("--cluster", "clusters", multiple=True, help="Cluster ID (with --root; repeatable).")
@click.option("--latest", type=int, default=0, help="Also build the latest N clusters in the source.")
@click.option("--cache", default="./cache", show_default=True, type=click.Path(file_okay=False),
              help="Download cache for remote sources.")
@_common_build_opts
def build(input_dir, clusters, latest, cache, output, use_duckdb, rules, quiet, **kw):
    """Parse a cluster folder into datasets under <output>/<cluster_id>/.

    Local: --input <dir>/<cluster_id>, or --root <dir> --cluster <id> (built in place). A remote --source is
    downloaded into --cache first (same as analyze)."""
    spec = _source_spec(kw)
    if input_dir or spec is None:
        if spec is not None and spec.type != "local":
            raise click.UsageError("--input is a local folder; do not combine it with a remote --source")
        cluster_dir, cid = _resolve_input(input_dir, None, clusters[0] if clusters else None)
        _build(cluster_dir, cid, output, use_duckdb, rules, quiet)
        return
    if spec.type == "local" and len(clusters) == 1 and not latest:
        _build(Path(spec.root).expanduser() / clusters[0], clusters[0], output, use_duckdb, rules, quiet)
        return
    _analyze(spec, clusters, latest, cache, output, use_duckdb, rules, quiet)


@main.command()
@source_options
@click.option("--cluster", "clusters", multiple=True, help="Cluster ID (repeatable).")
@click.option("--latest", type=int, default=0, help="Also download the latest N clusters (by driver/ file time).")
@click.option("--cache", default="./cache", show_default=True, type=click.Path(file_okay=False))
def download(clusters, latest, cache, **kw):
    """Incrementally copy cluster log folders from a remote source into <cache>/<cluster_id>/."""
    spec = _source_spec(kw, default_local=False)
    if spec is None:
        raise click.UsageError("Give --source volume|adls|s3 --root <...> (or --volume /Volumes/...).")
    _download(spec, clusters, latest, cache)


def _analyze(spec, clusters, latest, cache, output, use_duckdb, rules, quiet):
    from .ingest import ingest

    source = spec.make()
    progress = None if quiet else (lambda m: click.echo(f"  {m}", err=True))
    for cid in _cluster_ids(source, spec, clusters, latest):
        click.echo(f"{'downloading and ' if source.remote else ''}building {cid} from {source.describe()} ...")
        try:
            res = ingest(source, cid, cache, output, rules=rules, duckdb=use_duckdb, progress=progress)
        except click.ClickException:
            raise
        except Exception as e:  # noqa: BLE001
            raise click.ClickException(f"{cid}: {type(e).__name__}: {e}") from e
        if res.download is not None:
            click.echo(res.download.summary_line())
            for f in res.download.failed[:10]:
                click.echo(f"  FAILED {f['path']}: {f['error']}")
        _echo_lines(res.build.summary_lines())


@main.command()
@source_options
@click.option("--cluster", "clusters", multiple=True, help="Cluster ID (repeatable).")
@click.option("--latest", type=int, default=0, help="Also analyze the latest N clusters.")
@click.option("--cache", default="./cache", show_default=True, type=click.Path(file_okay=False))
@_common_build_opts
def analyze(clusters, latest, cache, output, use_duckdb, rules, quiet, **kw):
    """Download (remote sources, incremental) then build. Without a source, builds <cache>/<cluster_id>."""
    spec = _source_spec(kw)
    if spec is None:
        if not clusters:
            raise click.UsageError("Give --source/--root (or --volume) and/or --cluster.")
        for cid in clusters:
            _build(Path(cache) / cid, cid, output, use_duckdb, rules, quiet)
        return
    _analyze(spec, clusters, latest, cache, output, use_duckdb, rules, quiet)


@main.command()
@click.option("--output", default="./output", show_default=True, type=click.Path(file_okay=False))
@source_options
@click.option("--latest", type=int, default=20, show_default=True, help="How many to show (with a source).")
@click.option("--json", "as_json", is_flag=True, help="JSON output.")
def clusters(output, latest, as_json, **kw):
    """List built clusters (or, with --source/--root or --volume, the latest cluster folders in a log root)."""
    spec = _source_spec(kw)
    if spec is not None:
        from .ingest import list_clusters

        source = spec.make()
        try:
            found = list_clusters(source, limit=latest)
        except Exception as e:  # noqa: BLE001
            raise click.ClickException(f"{type(e).__name__}: {e}") from e
        built = {p.parent.name for p in Path(output).glob("*/summary.json")} if Path(output).is_dir() else set()
        if as_json:
            click.echo(json.dumps([{**c.to_dict(), "last_log_time": c.last_log_time,
                                    "analyzed": c.cluster_id in built} for c in found]))
            return
        click.echo(f"{len(found)} clusters in {source.describe()} (newest first):")
        for c in found:
            click.echo(f"  {c.cluster_id}  last log {_fmt_epoch(c.last_log_time)}"
                       + ("  [analyzed]" if c.cluster_id in built else ""))
        return
    out = Path(output)
    rows = []
    for p in sorted(out.glob("*/summary.json")) if out.is_dir() else []:
        try:
            rows.append(json.loads(p.read_text("utf-8")))
        except (OSError, ValueError):
            continue
    rows.sort(key=lambda s: s.get("built_at") or "", reverse=True)
    if as_json:
        click.echo(json.dumps([{k: s.get(k) for k in ("cluster_id", "built_at", "status", "start_time", "end_time",
                                                       "duration_ms", "findings_by_severity", "empty_reason")}
                               for s in rows]))
        return
    if not rows:
        click.echo(f"No built clusters in {out}. Run: dbx-log-analyzer build --input <dir>/<cluster_id>")
        return
    click.echo(f"{len(rows)} built clusters in {out}:")
    for s in rows:
        f = s.get("findings_by_severity", {})
        c = s.get("counts", {})
        extra = "  (empty: no cluster logs)" if s.get("empty_reason") else ""
        click.echo(f"  {s['cluster_id']:<28} {s.get('status', '?'):<9} built {s.get('built_at', '?')}  "
                   f"jobs {c.get('spark_jobs', 0)}  findings H{f.get('high', 0)}/M{f.get('medium', 0)}/L{f.get('low', 0)}"
                   f"{extra}")


@main.command("refresh-findings")
@click.option("--output", default="./output", show_default=True, type=click.Path(file_okay=False, exists=True))
@click.option("--cluster", "clusters", multiple=True, help="Cluster ID (repeatable; default: every cluster in --output).")
@click.option("--rules", type=click.Path(dir_okay=False, exists=True), help="Rules TOML overriding the defaults.")
def refresh_findings_cmd(output, clusters, rules):
    """Add the newer findings (waiting for cores, cores full, MERGE reading too much, DataFrame caches, count-only
    queries) to already-built outputs, and recompute the stage columns, the status and the diagnosis, from their
    datasets alone: no raw logs needed. Running it again replaces what it added."""
    from .config import load_rules
    from .refresh import refresh_findings

    r = load_rules(rules)
    root = Path(output)
    ids = list(clusters) or sorted(p.name for p in root.iterdir() if (p / "summary.json").exists())
    if not ids:
        raise click.UsageError(f"no built clusters under {root}")
    for cid in ids:
        res = refresh_findings(root / cid, r)
        by = ", ".join(f"{k} {v}" for k, v in sorted(res["by_category"].items())) or "none"
        click.echo(f"  {cid}: {res['added']} findings ({by})")


@main.command()
def sources():
    """List the source types and whether their optional SDK is installed."""
    from .sources import describe_sources

    for s in describe_sources():
        status = "available" if s["available"] else f"NOT available: {s['reason']}"
        click.echo(f"  {s['type']:<7} {s['label']:<34} {status}")
        click.echo("          options: " + ", ".join(f["name"] for f in s["fields"]))


@main.command()
@click.option("--output", default="./output", show_default=True, type=click.Path(file_okay=False))
@click.option("--cache", default="./cache", show_default=True, type=click.Path(file_okay=False))
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=8765, show_default=True, type=int)
@click.option("--no-browser", is_flag=True, help="Do not open a browser tab.")
def ui(output, cache, host, port, no_browser):
    """Start the local web UI (FastAPI + the built React app)."""
    try:
        import uvicorn

        from .api.server import create_app
    except ImportError as e:
        raise click.ClickException(f"UI not available: {e}") from e
    out, cache_p = Path(output).resolve(), Path(cache).resolve()
    out.mkdir(parents=True, exist_ok=True)
    log = _ui_log(out, port)
    app = create_app(out, cache_p)
    url = f"http://{'127.0.0.1' if host in ('0.0.0.0', '::') else host}:{port}/"
    click.echo(f"Databricks Cluster Log Analyzer UI at {url}  (output {out}, cache {cache_p}); Ctrl+C to stop")
    click.echo(f"Keep this window open: closing it or pressing Ctrl+C in it stops the UI. Log: {log}")
    if not no_browser:
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()
    import copy

    from uvicorn.config import LOGGING_CONFIG

    # uvicorn sets its logging up when it starts: the log file goes in through its config (request errors only)
    cfg = copy.deepcopy(LOGGING_CONFIG)
    cfg["formatters"]["file"] = {"()": "databricks_cluster_log_analyzer.cli._UtcFormatter"}
    cfg["handlers"]["file"] = {"class": "logging.FileHandler", "filename": str(log), "encoding": "utf-8",
                               "level": "ERROR", "formatter": "file"}
    # on "uvicorn" only: "uvicorn.error" (startup errors, request tracebacks) passes its records up to it
    cfg["loggers"]["uvicorn"]["handlers"] = [*cfg["loggers"]["uvicorn"].get("handlers", []), "file"]
    # Ctrl+C (or a shutdown signal) is handled inside uvicorn: run() then returns normally
    reason = "stopped: Ctrl+C in its window (or a shutdown signal)"
    try:
        uvicorn.run(app, host=host, port=port, log_level="warning", log_config=cfg)
    except KeyboardInterrupt:
        reason = "stopped: Ctrl+C in its window"
    except SystemExit as e:
        reason = f"could not start or stopped early (exit code {e.code}; is port {port} already in use?)"
        raise
    except BaseException as e:
        reason = f"stopped by an error: {type(e).__name__}: {e}"
        raise
    finally:
        _ui_log_line(log, reason)


UI_LOG = ".dbx_ui.log"


class _UtcFormatter(logging.Formatter):
    """The log file's lines in UTC, like the UI and the start / stop lines."""

    converter = time.gmtime

    def __init__(self):
        super().__init__("%(asctime)s UTC  %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")


def _ui_log_line(path: Path, text: str) -> None:
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC  {text}\n")
    except OSError:
        pass


def _ui_log(out: Path, port: int) -> Path:
    """<output>/.dbx_ui.log: when the UI started and stopped and why, request errors and, through faulthandler, the
    stack of a crash inside a native library. A start with no stop line before it means the process was killed (its
    window closed, the machine slept or ran out of memory); that is said in the console at the next start."""
    import faulthandler

    path = out / UI_LOG
    try:
        last = [ln for ln in path.read_text("utf-8", errors="replace").splitlines() if ln.strip()][-1]
    except (OSError, IndexError):
        last = ""
    if " started on port " in last:
        click.echo(f"Note: the UI that started at {last[:23]} ended without stopping cleanly (its window was closed, "
                   f"or the process was killed or crashed). Details, if any, are in {path}.")
        _ui_log_line(path, "the previous run ended without a clean stop")
    _ui_log_line(path, f"started on port {port}, version {__version__}")
    try:
        f = open(path, "a", encoding="utf-8")  # kept open for the life of the process: faulthandler writes to it
        faulthandler.enable(file=f)
    except OSError:
        pass
    return path


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
