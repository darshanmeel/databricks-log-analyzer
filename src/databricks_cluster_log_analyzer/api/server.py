"""FastAPI application: JSON API over the built datasets + the static React UI (frontend/dist)."""

from __future__ import annotations

import dataclasses
import json
import os
from pathlib import Path
from typing import Any, Optional

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response

from . import queries as Q
from . import graph as GRAPH
from . import steps as STEPS

DEV_ORIGINS = ["http://localhost:5173", "http://127.0.0.1:5173"]


def _version() -> str:
    try:
        from databricks_cluster_log_analyzer import __version__

        return str(__version__)
    except Exception:  # noqa: BLE001
        return "0.0.0"


class CleanJSONResponse(JSONResponse):
    """JSON response that never fails on NaN / numpy / datetime values (they are cleaned first)."""

    def render(self, content: Any) -> bytes:
        return json.dumps(
            Q.clean(content), ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")


def _to_dict(obj: Any) -> Any:
    """Best-effort conversion of a report object (dataclass / pydantic / plain object) to JSON data."""
    if obj is None or isinstance(obj, (str, int, float, bool, list, dict)):
        return Q.clean(obj)
    for meth in ("to_dict", "as_dict", "model_dump", "dict"):
        f = getattr(obj, meth, None)
        if callable(f):
            try:
                return Q.clean(_stringify_paths(f()))
            except Exception:  # noqa: BLE001
                pass
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return Q.clean(_stringify_paths(dataclasses.asdict(obj)))
    if hasattr(obj, "__dict__"):
        return Q.clean(_stringify_paths({k: v for k, v in vars(obj).items() if not k.startswith("_")}))
    return str(obj)


def _stringify_paths(v: Any) -> Any:
    if isinstance(v, Path):
        return v.as_posix()
    if isinstance(v, dict):
        return {k: _stringify_paths(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_stringify_paths(x) for x in v]
    return v


def _is_client_error(e: BaseException) -> bool:
    """Errors caused by bad input / config / credentials -> 400; everything else -> 500."""
    if isinstance(e, (ValueError, FileNotFoundError, NotADirectoryError, PermissionError, KeyError, NotImplementedError)):
        return True
    try:
        from databricks_cluster_log_analyzer.sources.base import MissingDependencyError

        if isinstance(e, MissingDependencyError):  # optional SDK not installed: tell the user which extra
            return True
    except Exception:  # noqa: BLE001
        pass
    mod = type(e).__module__ or ""
    # cloud SDK errors: auth, not found, permission denied ...
    return mod.startswith(("databricks", "azure", "botocore", "boto3", "google.auth"))


def _err_msg(e: BaseException) -> str:
    msg = str(e).strip()
    return f"{type(e).__name__}: {msg}" if msg else type(e).__name__


def _find_frontend_dist(explicit: Optional[Path] = None) -> Optional[Path]:
    candidates: list[Path] = []
    if explicit is not None:
        candidates.append(Path(explicit))
    env = os.environ.get("DBX_LOG_ANALYZER_FRONTEND")
    if env:
        candidates.append(Path(env))
    here = Path(__file__).resolve()
    # a fresh `npm run build` in a repo checkout wins; else the build committed into the package (no Node needed)
    if len(here.parents) > 3:
        candidates.append(here.parents[3] / "frontend" / "dist")  # repo checkout: <repo>/src/<pkg>/api/server.py
    candidates.append(here.parent / "static")  # packaged build: frontend/scripts/copy-static.mjs
    candidates.append(Path.cwd() / "frontend" / "dist")
    for c in candidates:
        if (c / "index.html").is_file():
            return c.resolve()
    return None


def create_app(output_root: Path, cache_root: Path, *, frontend_dist: Optional[Path] = None) -> FastAPI:
    output_root = Path(output_root)
    cache_root = Path(cache_root)
    store = Q.Store(output_root)

    app = FastAPI(
        title="Databricks Cluster Log Analyzer",
        version=_version(),
        default_response_class=CleanJSONResponse,
    )
    app.state.output_root = output_root
    app.state.cache_root = cache_root
    app.state.store = store

    app.add_middleware(
        CORSMiddleware,
        allow_origins=DEV_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(Q.ApiError)
    async def _api_error(_request: Request, exc: Q.ApiError):
        return CleanJSONResponse({"detail": exc.detail}, status_code=exc.status_code)

    # ---- meta --------------------------------------------------------------------------------------------------

    @app.get("/api/health")
    def health():
        return {"ok": True, "version": _version()}

    @app.get("/api/clusters")
    def clusters():
        return Q.list_clusters(store)

    # ---- build / download --------------------------------------------------------------------------------------

    def _build(cluster_dir: Path, cluster_id: Optional[str]) -> dict[str, Any]:
        try:
            from databricks_cluster_log_analyzer import pipeline
        except Exception as e:  # noqa: BLE001
            raise HTTPException(500, f"pipeline is not available: {_err_msg(e)}") from e
        cid = cluster_id or cluster_dir.name
        try:
            report = pipeline.build(cluster_dir, output_root, cluster_id=cid)
        except HTTPException:
            raise
        except Exception as e:  # noqa: BLE001
            raise HTTPException(400 if _is_client_error(e) else 500, _err_msg(e)) from e
        cid = getattr(report, "cluster_id", None) or cid
        try:
            return Q.read_summary(store, cid)
        except Q.ApiError:
            # pipeline did not write summary.json where expected: return what the build reported
            return {"cluster_id": cid, "build_report": _to_dict(report)}

    @app.post("/api/analyze")
    def analyze(body: dict = Body(...)):
        cluster_dir_s = (body.get("cluster_dir") or "").strip() if isinstance(body.get("cluster_dir"), str) else ""
        log_root = (body.get("log_root") or "").strip() if isinstance(body.get("log_root"), str) else ""
        cluster_id = (body.get("cluster_id") or "").strip() if isinstance(body.get("cluster_id"), str) else ""
        if cluster_dir_s:
            cluster_dir = Path(os.path.expanduser(cluster_dir_s))
            cid = cluster_id or None
        elif log_root and cluster_id:
            cluster_dir = Path(os.path.expanduser(log_root)) / cluster_id
            cid = cluster_id
        else:
            raise HTTPException(400, 'body must be {"log_root": str, "cluster_id": str} or {"cluster_dir": str}')
        if not cluster_dir.exists():
            raise HTTPException(400, f"folder does not exist: {cluster_dir}")
        if not cluster_dir.is_dir():
            raise HTTPException(400, f"not a folder: {cluster_dir}")
        if cid is not None and not Q._CLUSTER_ID_RE.match(cid):
            raise HTTPException(400, f"invalid cluster id: {cid!r}")
        if cid is None and not Q._CLUSTER_ID_RE.match(cluster_dir.resolve().name):
            raise HTTPException(400, f"cannot derive a cluster id from folder name {cluster_dir.resolve().name!r}")
        return _build(cluster_dir.resolve(), cid)

    @app.post("/api/clusters/{cid}/reanalyze")
    def reanalyze(cid: str):
        """Build an analyzed cluster again from the raw logs it was built from, when they are still there."""
        summary = Q.read_summary(store, cid)
        raw = summary.get("input_dir")
        if not raw:
            raise HTTPException(409, "this analysis does not record where its raw logs were; analyze it again from the source")
        cluster_dir = Path(raw)
        if not cluster_dir.is_dir() or not Q._has_files(cluster_dir):
            raise HTTPException(409, f"the raw logs are no longer at {cluster_dir}; analyze it again from the source")
        return _build(cluster_dir.resolve(), summary.get("cluster_id") or cid)

    @app.post("/api/download")
    def download(body: dict = Body(...)):
        volume = body.get("volume")
        cluster_id = body.get("cluster_id")
        profile = body.get("profile") or None
        do_build = bool(body.get("build", True))
        if not isinstance(volume, str) or not volume.strip():
            raise HTTPException(400, "volume is required (e.g. /Volumes/catalog/schema/volume/cluster_logs)")
        if not isinstance(cluster_id, str) or not Q._CLUSTER_ID_RE.match(cluster_id.strip()):
            raise HTTPException(400, f"invalid cluster_id: {cluster_id!r}")
        cluster_id = cluster_id.strip()
        try:
            from databricks_cluster_log_analyzer.ingest import download as ingest_download
            from databricks_cluster_log_analyzer.sources import make_source
        except Exception as e:  # noqa: BLE001
            raise HTTPException(500, f"download support is not available: {_err_msg(e)}") from e
        try:
            source = make_source("volume", volume.strip(), {"profile": profile})
            report = ingest_download(source, cluster_id, cache_root)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(400 if _is_client_error(e) else 500, _err_msg(e)) from e
        summary = _build((cache_root / cluster_id).resolve(), cluster_id) if do_build else None
        return {"download": _to_dict(report), "summary": summary}

    # ---- sources (Revision 2): source picker -> cluster picker -> ingest ------------------------------------------

    def _source_body(body: dict) -> tuple[str, str, dict]:
        type_ = body.get("type")
        root = body.get("root")
        options = body.get("options") or {}
        if not isinstance(type_, str) or not type_.strip():
            raise HTTPException(400, "type is required (local, volume, adls or s3)")
        if not isinstance(root, str) or not root.strip():
            raise HTTPException(400, "root is required")
        if not isinstance(options, dict):
            raise HTTPException(400, "options must be an object")
        # the root travels in "root"; an older UI also sent it among the options
        return type_.strip().lower(), root.strip(), {str(k): v for k, v in options.items() if k != "root"}

    def _make_source(type_: str, root: str, options: dict):
        try:
            from databricks_cluster_log_analyzer.sources import make_source
        except Exception as e:  # noqa: BLE001
            raise HTTPException(500, f"sources are not available: {_err_msg(e)}") from e
        try:
            return make_source(type_, root, options)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(400 if _is_client_error(e) else 500, _err_msg(e)) from e

    @app.get("/api/sources")
    def sources():
        from databricks_cluster_log_analyzer.sources import describe_sources

        return describe_sources()

    @app.post("/api/sources/clusters")
    def source_clusters(body: dict = Body(...)):
        type_, root, options = _source_body(body)
        limit = body.get("limit")
        limit = int(limit) if isinstance(limit, (int, float, str)) and str(limit).strip().isdigit() else 200
        source = _make_source(type_, root, options)
        try:
            found = source.list_clusters(limit)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(400 if _is_client_error(e) else 500, _err_msg(e)) from e
        out = []
        for c in found:
            row = c.to_dict()
            row["analyzed"] = (output_root / c.cluster_id / "summary.json").is_file()
            out.append(row)
        return out

    @app.post("/api/ingest")
    def ingest(body: dict = Body(...)):
        type_, root, options = _source_body(body)
        cluster_id = body.get("cluster_id")
        if not isinstance(cluster_id, str) or not Q._CLUSTER_ID_RE.match(cluster_id.strip()) or ".." in cluster_id:
            raise HTTPException(400, f"invalid cluster_id: {cluster_id!r}")
        cluster_id = cluster_id.strip()
        source = _make_source(type_, root, options)
        report = None
        if getattr(source, "remote", True):
            try:
                from databricks_cluster_log_analyzer.ingest import download as ingest_download

                report = ingest_download(source, cluster_id, cache_root)
            except Exception as e:  # noqa: BLE001
                raise HTTPException(400 if _is_client_error(e) else 500, _err_msg(e)) from e
            if report.files_listed and len(report.failed) == report.files_listed:
                raise HTTPException(400, f"every file failed to download; first error: {report.failed[0]['error']}")
            cluster_dir = (cache_root / cluster_id).resolve()
        else:
            cluster_dir = (Path(os.path.expanduser(root)) / cluster_id).resolve()
            if not cluster_dir.is_dir():
                raise HTTPException(400, f"folder does not exist: {cluster_dir}")
        return {"download": _to_dict(report) if report is not None else None, "summary": _build(cluster_dir, cluster_id)}

    # ---- per cluster -------------------------------------------------------------------------------------------

    @app.get("/api/clusters/{cid}/steps")
    def steps(cid: str):
        return STEPS.list_steps(store, cid)

    @app.get("/api/clusters/{cid}/steps/{step_id}")
    def step(cid: str, step_id: str, request: Request):
        return STEPS.step_table(store, cid, step_id, request.query_params.multi_items())


    @app.get("/api/clusters/{cid}/summary")
    def summary(cid: str):
        return Q.read_summary(store, cid)

    @app.get("/api/clusters/{cid}/hierarchy")
    def hierarchy(cid: str):
        return Q.hierarchy(store, cid)

    @app.get("/api/clusters/{cid}/datasets/{name}")
    def dataset(cid: str, name: str, request: Request):
        return Q.table_query(store, cid, name, request.query_params.multi_items())

    @app.get("/api/clusters/{cid}/task-columns")
    def task_columns(cid: str, ctx: str, stage: Optional[int] = None, attempt: Optional[int] = None,
                     job: Optional[int] = None, query: Optional[int] = None):
        return Q.task_columns(store, cid, ctx, stage, attempt, job, query)

    @app.get("/api/clusters/{cid}/stages/{ctx}/{stage_id}/{attempt}")
    def stage(cid: str, ctx: str, stage_id: int, attempt: int):
        return Q.stage_detail(store, cid, ctx, stage_id, attempt)

    @app.get("/api/clusters/{cid}/queries/{ctx}/{exec_id}")
    def query(cid: str, ctx: str, exec_id: int):
        return Q.query_detail(store, cid, ctx, exec_id)

    @app.get("/api/clusters/{cid}/plan-diff")
    def plan_diff(
        cid: str,
        a_ctx: str = Query(...),
        a_id: int = Query(...),
        b_ctx: str = Query(...),
        b_id: int = Query(...),
        which: str = Query("final"),
        b_cid: Optional[str] = None,
    ):
        return Q.plan_diff(store, cid, a_ctx, a_id, b_ctx, b_id, which, b_cid)

    @app.get("/api/clusters/{cid}/queries/{ctx}/{exec_id}/plan-candidates")
    def plan_candidates(cid: str, ctx: str, exec_id: int):
        return Q.plan_candidates(store, cid, ctx, exec_id)

    @app.get("/api/clusters/{cid}/hotspots")
    def hotspots(cid: str, ctx: Optional[str] = None, kind: Optional[str] = None, run: Optional[str] = None):
        return Q.hotspots(store, cid, ctx, kind, run)

    @app.get("/api/clusters/{cid}/runs")
    def runs(cid: str):
        return Q.runs(store, cid)

    @app.get("/api/clusters/{cid}/cluster-view")
    def cluster_view(cid: str):
        return Q.cluster_view(store, cid)

    @app.get("/api/clusters/{cid}/run-steps")
    def run_steps(cid: str, run: str):
        return Q.run_steps(store, cid, run)

    @app.get("/api/clusters/{cid}/query-time")
    def query_time(cid: str, ctx: str, query: int):
        return Q.query_time(store, cid, ctx, query)

    @app.get("/api/clusters/{cid}/stage-why")
    def stage_why(cid: str, ctx: str, query: Optional[int] = None, job: Optional[int] = None):
        return Q.stage_why(store, cid, ctx, query, job)

    @app.get("/api/clusters/{cid}/run-end")
    def run_end(cid: str, run: str):
        return Q.run_end(store, cid, run)

    @app.get("/api/clusters/{cid}/query-logic")
    def query_logic(cid: str, ctx: str, id: int):
        return Q.query_logic_view(store, cid, ctx, id)

    @app.get("/api/clusters/{cid}/tables")
    def tables(cid: str):
        return Q.cluster_tables(store, cid)

    @app.get("/api/clusters/{cid}/run-tables")
    def run_tables(cid: str, run: str):
        return Q.run_tables(store, cid, run)

    @app.get("/api/clusters/{cid}/group")
    def group(cid: str, runs: str = ""):
        return Q.group_view(store, cid, [r for r in runs.split(",") if r])

    @app.get("/api/clusters/{cid}/top")
    def top(cid: str, kind: str = "stages", by: str = "duration", run: Optional[str] = None, limit: int = 25,
            q: Optional[str] = None):
        return Q.top(store, cid, kind, by, run, limit, q)

    @app.get("/api/clusters/{cid}/settings")
    def settings(cid: str):
        return Q.settings_view(store, cid)

    @app.get("/api/clusters/{cid}/flow")
    def flow(cid: str, run: Optional[str] = None):
        return Q.flow(store, cid, run)

    @app.get("/api/clusters/{cid}/gantt")
    def gantt(cid: str, ctx: Optional[str] = None, max_tasks: int = Query(20000, ge=1, le=1_000_000),
              run: Optional[str] = None, start: Optional[int] = None, end: Optional[int] = None):
        return Q.gantt(store, cid, ctx, max_tasks, run, start, end)

    @app.get("/api/clusters/{cid}/graph")
    def graph(cid: str, ctx: Optional[str] = None, run: Optional[str] = None):
        return GRAPH.graph(store, cid, ctx, run)

    @app.get("/api/clusters/{cid}/spill-shuffle")
    def spill_shuffle(cid: str, ctx: Optional[str] = None, by: str = Query("executor"), run: Optional[str] = None):
        return GRAPH.spill_shuffle(store, cid, ctx, by, run)

    @app.get("/api/clusters/{cid}/logs/context")
    def logs_context(
        cid: str,
        file_path: str = Query(...),
        seq: int = Query(...),
        before: int = Query(20, ge=0, le=Q.MAX_LIMIT),
        after: int = Query(40, ge=0, le=Q.MAX_LIMIT),
    ):
        return Q.log_context(store, cid, file_path, seq, before, after)

    @app.get("/api/clusters/{cid}/facets/{name}")
    def facets(cid: str, name: str, column: str = Query(...)):
        return Q.facets(store, cid, name, column)

    @app.get("/api/clusters/{cid}/errors")
    def errors(cid: str, run: Optional[str] = None):
        store.cluster_dir(cid)
        return Q.error_groups(store, cid, run)

    @app.get("/api/clusters/{cid}/signals")
    def signals(cid: str):
        store.cluster_dir(cid)
        return Q.signal_groups(store, cid)

    @app.api_route("/api/{rest:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
    def api_not_found(rest: str):
        raise HTTPException(404, f"unknown API route: /api/{rest}")

    # ---- static frontend (SPA) ---------------------------------------------------------------------------------

    dist = _find_frontend_dist(frontend_dist)
    app.state.frontend_dist = dist
    if dist is not None:
        index = dist / "index.html"

        @app.get("/{full_path:path}", include_in_schema=False)
        def spa(full_path: str):
            if full_path == "api" or full_path.startswith("api/"):
                raise HTTPException(404, f"unknown API route: /{full_path}")
            if full_path:
                try:
                    candidate = (dist / full_path).resolve()
                    candidate.relative_to(dist)
                except (ValueError, OSError):
                    candidate = None
                if candidate is not None and candidate.is_file():
                    headers = {"Cache-Control": "public, max-age=31536000, immutable"} if "/assets/" in (
                        "/" + full_path
                    ) else None
                    return FileResponse(candidate, headers=headers)
            return FileResponse(index, headers={"Cache-Control": "no-cache"})
    else:

        @app.get("/", include_in_schema=False)
        def no_ui():
            return Response(
                "<!doctype html><title>Databricks Cluster Log Analyzer</title>"
                "<p>The API is running at <code>/api</code>. The UI is not built: run "
                "<code>npm install &amp;&amp; npm run build</code> in <code>frontend/</code>.</p>",
                media_type="text/html",
            )

    return app
