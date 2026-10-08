"""Revision 4: the Hierarchy graph and the spill & shuffle timeline.

GET /api/clusters/{cid}/graph?ctx=                         -> graph(store, cid, ctx)
GET /api/clusters/{cid}/spill-shuffle?ctx=&by=executor|stage -> spill_shuffle(store, cid, ctx, by)

Node ids: ``app:<ctx>``, ``job:<ctx>:<job>``, ``stage:<ctx>:<stage>:<attempt>``, ``query:<ctx>:<exec>``,
``connect:<ctx>:<op>``. Edge kinds: contains, depends, retry, runs_query, from_connect.
"""

from __future__ import annotations

from typing import Any

from . import queries as Q

GRAPH_FLAGS = ("failed", "retried", "spill", "skew", "gc", "shuffle_heavy")
# Revision 11: rows read and written, and the per-task data spread behind data_skew
STAGE_DATA_KEYS = ("input_records", "shuffle_read_records", "shuffle_write_records", "output_records", "data_skew",
                   "min_task_bytes_in", "p50_task_bytes_in", "max_task_bytes_in", "min_task_rows_in",
                   "p50_task_rows_in", "max_task_rows_in", "p10_task_bytes_in", "p90_task_bytes_in",
                   "avg_task_bytes_in", "p10_task_rows_in", "p90_task_rows_in", "avg_task_rows_in", "p10_task_ms",
                   "p90_task_ms", "wmed_task_bytes_in", "tasks_none", "tasks_lt10", "tasks_10_128",
                   "tasks_128_256", "tasks_ge256", "bytes_lt10", "bytes_10_128", "bytes_128_256", "bytes_ge256")
STAGE_METRICS = ("disk_spill", "mem_spill", "shuffle_read", "shuffle_write", "input_bytes", "output_bytes")
WHAT_KEYS = ("description", "call_site", "notebook_path", "sql_description", "operators", "tables_read",
             "tables_written", "statement_text", "rdd_scopes")
SHORT = 160
LIST_MAX = 20


def _rules():
    if not Q._RULES:
        from ..config import load_rules

        Q._RULES.append(load_rules())
    return Q._RULES[0]


def _short(v: Any, n: int = SHORT) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.splitlines()[0].strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _num(v: Any) -> float:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else 0.0


def _add_sum(acc: dict, key: str, v: Any) -> None:
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        acc[key] = (acc.get(key) or 0) + v


def _merge_list(dst: list, src: Any, cap: int = LIST_MAX) -> None:
    for x in src or []:
        if x not in dst and len(dst) < cap:
            dst.append(x)


def _what(**kw: Any) -> dict[str, Any]:
    out: dict[str, Any] = {k: None for k in WHAT_KEYS}
    for k in ("operators", "tables_read", "tables_written", "rdd_scopes"):
        out[k] = []
    for k, v in kw.items():
        out[k] = v if v is not None or k not in out else out[k]
    return out


def resolve_ctx(store: Q.Store, con, cid: str, ctx: str | None, *, allow_all: bool = False) -> tuple[str | None, list[str]]:
    """(ctx, contexts). Like the Gantt: ctx is required when the cluster has several Spark contexts (unless
    `allow_all`, then None means "every context")."""
    ctxs = Q.contexts(store, con, cid)
    if not ctx:
        if len(ctxs) == 1:
            return ctxs[0], ctxs
        if not ctxs or allow_all:
            return None, ctxs
        raise Q.BadRequest(f"ctx is required: this cluster has {len(ctxs)} Spark contexts: {', '.join(ctxs)}")
    if ctx not in ctxs:
        raise Q.NotFound(f"unknown spark context {ctx!r}")
    return ctx, ctxs


# ---------------------------------------------------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------------------------------------------------


def _stage_flags(s: dict, attempts: int, retries: int, rules) -> list[str]:
    flags = []
    if s.get("status") == "failed":
        flags.append("failed")
    if (s.get("stage_attempt") or 0) > 0 or attempts > 1 or retries > 0 or (s.get("failed_tasks") or 0) > 0:
        flags.append("retried")
    if _num(s.get("disk_spill")) > 0:
        flags.append("spill")
    skew, mx = s.get("skew"), s.get("max_task_ms")
    if skew is not None and _num(skew) >= rules.skew_ratio and _num(mx) >= rules.skew_min_task_ms:
        flags.append("skew")
    if s.get("gc_share") is not None and _num(s.get("gc_share")) >= rules.gc_share:
        flags.append("gc")
    if _num(s.get("shuffle_read")) + _num(s.get("shuffle_write")) >= rules.shuffle_heavy_bytes:
        flags.append("shuffle_heavy")
    return flags


def _ordered_flags(flags) -> list[str]:
    fs = set(flags)
    return [f for f in GRAPH_FLAGS if f in fs]


def _rolled_up(child_flags, own_failed: bool) -> set[str]:
    """Flags of a job / query / statement / app from its children: a failed stage attempt inside a container that
    still succeeded reads as "retried", not "failed"."""
    out = set(child_flags)
    if "failed" in out and not own_failed:
        out.discard("failed")
        out.add("retried")
    if own_failed:
        out.add("failed")
    return out


def _empty_graph(ctxs: list[str]) -> dict[str, Any]:
    return {"ctx": None, "contexts": ctxs, "start": None, "end": None, "nodes": [], "edges": [], "truncated": False,
            "dropped_stages": 0, "stages_total": 0}


def graph(store: Q.Store, cid: str, ctx: str | None, run: str | None = None) -> dict[str, Any]:
    store.cluster_dir(cid)
    rules = _rules()
    with store.connect() as con:
        ctx, ctxs = resolve_ctx(store, con, cid, ctx)
        if ctx is None:
            return _empty_graph(ctxs)
        cw, cp = "spark_context_id = ?", [ctx]
        apps = store.select(con, cid, "apps", cw, cp, limit=1)
        jobs = store.select(con, cid, "spark_jobs", cw, cp, order="spark_job_id NULLS LAST",
                            truncate={"call_site": 2000, "description": 2000})
        stages = store.select(con, cid, "stages", cw, cp, order="stage_id, stage_attempt",
                              truncate={"stage_name": 500, "failure_reason": 2000, "job_description": 2000,
                                        "details": 2000, "retry_of_failure": 2000})
        queries = store.select(con, cid, "sql_queries", cw, cp, exclude=("final_plan", "initial_plan"),
                               order="sql_execution_id", truncate={"description": 2000, "details": 2000,
                                                                   "error": 1000})
        connect = store.select(con, cid, "connect_operations", cw, cp, order="start_time NULLS LAST, operation_id",
                               truncate={"statement_text": 2000, "error": 1000})
        retries: dict[tuple, int] = {}
        rpath = store.dataset_path(cid, "task_retries", required=False)
        if rpath is not None:
            for sid, att, n in con.execute(
                f"SELECT stage_id, stage_attempt, count(*) FROM {store.src(rpath)} WHERE {cw} GROUP BY 1, 2", cp
            ).fetchall():
                retries[(sid, att)] = int(n)

    if run:  # Revision 6: only this run's jobs, stages, queries and Connect operations
        jobs = [j for j in jobs if j.get("run_key") == run]
        stages = [x for x in stages if x.get("run_key") == run]
        queries = [x for x in queries if x.get("run_key") == run]
        connect = [x for x in connect if x.get("run_key") == run]
    app_id = f"app:{ctx}"
    job_by_id = {j.get("spark_job_id"): j for j in jobs}
    job_ids = set(job_by_id)
    q_by_id = {q.get("sql_execution_id"): q for q in queries}
    op_by_id = {o.get("operation_id"): o for o in connect}
    attempts_of: dict[Any, int] = {}
    for s in stages:
        attempts_of[s.get("stage_id")] = attempts_of.get(s.get("stage_id"), 0) + 1

    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    times: list[int] = []

    def see(*vals):
        times.extend(v for v in vals if isinstance(v, int))

    # ---- stages (first, so jobs / app can aggregate them) -------------------------------------------------------
    stage_nodes: list[dict[str, Any]] = []
    for s in stages:
        sid, att = s.get("stage_id"), s.get("stage_attempt") or 0
        job = s.get("spark_job_id")
        parent = f"job:{ctx}:{job}" if job is not None and job in job_ids else app_id
        n_retries = retries.get((sid, att), 0)
        attempts = attempts_of.get(sid, 1)
        flags = _stage_flags(s, attempts, n_retries, rules)
        jrow = job_by_id.get(job) if job is not None else None
        q = q_by_id.get(s.get("sql_execution_id")) if s.get("sql_execution_id") is not None else None
        op = op_by_id.get(jrow.get("connect_operation_id")) if jrow and jrow.get("connect_operation_id") else None
        see(s.get("start_time"), s.get("end_time"))
        label = f"Stage {sid}" + (f" (attempt {att})" if att else "")
        stage_nodes.append({
            "id": f"stage:{ctx}:{sid}:{att}", "type": "stage", "label": label, "sublabel": _short(s.get("stage_name")),
            "status": s.get("status"), "start": s.get("start_time"), "end": s.get("end_time"),
            "duration_ms": s.get("duration_ms"), "parent": parent,
            "metrics": {"tasks": s.get("tasks"), "failed_tasks": s.get("failed_tasks"), "attempt": att,
                        "attempts": attempts, **{k: s.get(k) for k in STAGE_METRICS}, "gc_share": s.get("gc_share"),
                        "skew": s.get("skew"), "retries": n_retries, "num_tasks": s.get("num_tasks"),
                        "p50_task_ms": s.get("p50_task_ms"), "max_task_ms": s.get("max_task_ms"), "min_task_ms": s.get("min_task_ms"),
                        **{k: s.get(k) for k in STAGE_DATA_KEYS},
                        "executors_used": s.get("executors_used")},
            "flags": flags,
            "what": _what(description=s.get("job_description"), call_site=(jrow or {}).get("call_site"),
                          notebook_path=(jrow or {}).get("notebook_path"),
                          sql_description=(q or {}).get("description"), operators=(q or {}).get("operators"),
                          tables_read=(q or {}).get("tables_read"), tables_written=(q or {}).get("tables_written"),
                          statement_text=(op or {}).get("statement_text"), rdd_scopes=s.get("rdd_scopes") or [],
                          rdd_names=s.get("rdd_names") or [], details=s.get("details"),
                          stage_name=s.get("stage_name"), failure_reason=s.get("failure_reason"),
                          retry_of_failure=s.get("retry_of_failure")),
            # internal, removed below
            "_sid": sid, "_att": att, "_job": job, "_parents": s.get("parent_ids") or [],
        })

    # ---- truncation ---------------------------------------------------------------------------------------------
    stages_total = len(stage_nodes)
    dropped = 0
    truncated = False
    if stages_total > rules.graph_max_stages:
        truncated = True
        keep = {n["id"] for n in stage_nodes if n["flags"]}
        longest = sorted(stage_nodes, key=lambda n: -(n["duration_ms"] or 0))[: rules.graph_keep_longest]
        keep.update(n["id"] for n in longest)
        kept = [n for n in stage_nodes if n["id"] in keep]
        dropped = stages_total - len(kept)
        all_stage_nodes, stage_nodes = stage_nodes, kept
    else:
        all_stage_nodes = stage_nodes

    # ---- jobs ---------------------------------------------------------------------------------------------------
    by_job: dict[Any, list[dict]] = {}
    for n in all_stage_nodes:
        by_job.setdefault(n["_job"], []).append(n)
    shown_by_job: dict[Any, int] = {}
    for n in stage_nodes:
        shown_by_job[n["_job"]] = shown_by_job.get(n["_job"], 0) + 1
    job_nodes = []
    for j in jobs:
        jid = j.get("spark_job_id")
        sts = by_job.get(jid, [])
        status = Q._job_status(j.get("result"), j.get("end_time"))
        m: dict[str, Any] = {"tasks": None, "failed_tasks": None, "attempt": None, "attempts": None,
                             **{k: None for k in STAGE_METRICS}, "gc_share": None, "skew": None, "retries": 0}
        flags = set()
        scopes: list[str] = []
        for n in sts:
            sm = n["metrics"]
            for k in ("tasks", "failed_tasks", *STAGE_METRICS):
                _add_sum(m, k, sm.get(k))
            m["retries"] += sm.get("retries") or 0
            if sm.get("skew") is not None:
                m["skew"] = max(m["skew"] or 0, sm["skew"])
            if sm.get("gc_share") is not None:  # job / query / app: the worst stage
                m["gc_share"] = max(m["gc_share"] or 0, sm["gc_share"])
            flags.update(n["flags"])
            _merge_list(scopes, n["what"].get("rdd_scopes"))
        m["stages"] = len(sts)
        m["stage_attempts_failed"] = sum(1 for n in sts if n["status"] == "failed")
        if truncated:
            m["stages_shown"] = shown_by_job.get(jid, 0)
        flags = _rolled_up(flags, status == "failed")
        q = q_by_id.get(j.get("sql_execution_id")) if j.get("sql_execution_id") is not None else None
        op = op_by_id.get(j.get("connect_operation_id")) if j.get("connect_operation_id") else None
        see(j.get("start_time"), j.get("end_time"))
        job_nodes.append({
            "id": f"job:{ctx}:{jid}", "type": "job", "label": f"Job {jid}",
            "sublabel": _short(j.get("description")) or _short(j.get("call_site")),
            "status": status, "start": j.get("start_time"), "end": j.get("end_time"),
            "duration_ms": j.get("duration_ms"), "parent": app_id, "metrics": m, "flags": _ordered_flags(flags),
            "what": _what(description=j.get("description"), call_site=j.get("call_site"),
                          notebook_path=j.get("notebook_path"), sql_description=(q or {}).get("description"),
                          operators=(q or {}).get("operators"), tables_read=(q or {}).get("tables_read"),
                          tables_written=(q or {}).get("tables_written"),
                          statement_text=(op or {}).get("statement_text"), rdd_scopes=scopes,
                          job_group=j.get("job_group"), error=j.get("error"),
                          databricks_job_id=j.get("databricks_job_id"), databricks_run_id=j.get("databricks_run_id")),
            "_flags": flags,
        })

    # ---- queries ------------------------------------------------------------------------------------------------
    jobs_of_query: dict[Any, list[dict]] = {}
    for jn, j in zip(job_nodes, jobs):
        if j.get("sql_execution_id") is not None:
            jobs_of_query.setdefault(j.get("sql_execution_id"), []).append(jn)
    query_nodes = []
    for q in queries:
        eid = q.get("sql_execution_id")
        js = jobs_of_query.get(eid, [])
        flags = set()
        m = {"tasks": q.get("tasks"), "failed_tasks": None, "attempt": None, "attempts": None,
             **{k: None for k in STAGE_METRICS}, "gc_share": None, "skew": q.get("max_stage_skew"), "retries": 0,
             "spark_jobs": len(js), "stages": q.get("stages")}
        for jn in js:
            for k in ("failed_tasks", *STAGE_METRICS):
                _add_sum(m, k, jn["metrics"].get(k))
            m["retries"] += jn["metrics"].get("retries") or 0
            if jn["metrics"].get("gc_share") is not None:
                m["gc_share"] = max(m["gc_share"] or 0, jn["metrics"]["gc_share"])
            flags.update(jn["_flags"])
        flags = _rolled_up(flags, q.get("status") == "failed")
        see(q.get("start_time"), q.get("end_time"))
        query_nodes.append({
            "id": f"query:{ctx}:{eid}", "type": "query", "label": f"Query {eid}",
            "sublabel": _short(q.get("description")), "status": q.get("status"), "start": q.get("start_time"),
            "end": q.get("end_time"), "duration_ms": q.get("duration_ms"), "parent": app_id, "metrics": m,
            "flags": _ordered_flags(flags),
            "what": _what(sql_description=q.get("description"), call_site=q.get("details"),
                          operators=q.get("operators") or [], tables_read=q.get("tables_read") or [],
                          tables_written=q.get("tables_written") or [], error=q.get("error"),
                          plan_hash=q.get("plan_hash")),
        })

    # ---- connect operations -------------------------------------------------------------------------------------
    jobs_of_op: dict[Any, list[dict]] = {}
    for jn, j in zip(job_nodes, jobs):
        if j.get("connect_operation_id"):
            jobs_of_op.setdefault(j.get("connect_operation_id"), []).append(jn)
    connect_nodes = []
    for o in connect:
        oid = o.get("operation_id")
        js = jobs_of_op.get(oid, [])
        flags = set()
        for jn in js:
            flags.update(jn["_flags"])
        flags = _rolled_up(flags, o.get("status") == "failed")
        see(o.get("start_time"), o.get("finish_time"))
        connect_nodes.append({
            "id": f"connect:{ctx}:{oid}", "type": "connect", "label": "Statement " + str(oid)[:8],
            "sublabel": _short(o.get("statement_text")), "status": o.get("status"), "start": o.get("start_time"),
            "end": o.get("finish_time"), "duration_ms": o.get("duration_ms"), "parent": app_id,
            "metrics": {"tasks": None, "failed_tasks": None, "attempt": None, "attempts": None,
                        **{k: None for k in STAGE_METRICS}, "gc_share": None, "skew": None,
                        "retries": sum(jn["metrics"].get("retries") or 0 for jn in js), "spark_jobs": len(js)},
            "flags": _ordered_flags(flags),
            "what": _what(statement_text=o.get("statement_text"), user_name=o.get("user_name"),
                          session_id=o.get("session_id"), error=o.get("error"), operation_id=oid),
        })

    # ---- app ----------------------------------------------------------------------------------------------------
    a = apps[0] if apps else {}
    see(a.get("start_time"), a.get("end_time"))
    app_flags = set()
    am: dict[str, Any] = {"tasks": None, "failed_tasks": None, "attempt": None, "attempts": None,
                          **{k: None for k in STAGE_METRICS}, "gc_share": None, "skew": None, "retries": 0,
                          "spark_jobs": len(jobs), "stages": stages_total, "queries": len(queries),
                          "connect_operations": len(connect)}
    for n in all_stage_nodes:
        for k in ("tasks", "failed_tasks", *STAGE_METRICS):
            _add_sum(am, k, n["metrics"].get(k))
        am["retries"] += n["metrics"].get("retries") or 0
        if n["metrics"].get("gc_share") is not None:
            am["gc_share"] = max(am["gc_share"] or 0, n["metrics"]["gc_share"])
        if n["metrics"].get("skew") is not None:
            am["skew"] = max(am["skew"] or 0, n["metrics"]["skew"])
        app_flags.update(n["flags"])
    failed_jobs = [jn for jn in job_nodes if jn["status"] == "failed"]
    if failed_jobs or any(q.get("status") == "failed" for q in queries):
        app_status = "failed"
    elif jobs and all(jn["status"] == "succeeded" for jn in job_nodes):
        app_status = "succeeded"
    elif not jobs and a.get("end_time") is not None:
        app_status = "succeeded"
    else:
        app_status = "incomplete" if any(jn["status"] == "incomplete" for jn in job_nodes) else "succeeded"
    app_node = {
        "id": app_id, "type": "app", "label": a.get("app_name") or f"Spark context {ctx}",
        "sublabel": " · ".join(str(x) for x in (a.get("app_id"), a.get("spark_version")) if x) or None,
        "status": app_status, "start": a.get("start_time"), "end": a.get("end_time"),
        "duration_ms": a.get("duration_ms"), "parent": None, "metrics": am,
        "flags": _ordered_flags(_rolled_up(app_flags, app_status == "failed")),
        "what": _what(user=a.get("user"), app_id=a.get("app_id"), spark_version=a.get("spark_version")),
    }

    # ---- edges --------------------------------------------------------------------------------------------------
    shown = {n["id"] for n in stage_nodes}
    for jn in job_nodes:
        edges.append({"source": app_id, "target": jn["id"], "kind": "contains", "label": None})
    for n in stage_nodes:
        edges.append({"source": n["parent"], "target": n["id"], "kind": "contains", "label": None})
    for qn in query_nodes:
        edges.append({"source": app_id, "target": qn["id"], "kind": "contains", "label": None})
    for cn in connect_nodes:
        edges.append({"source": app_id, "target": cn["id"], "kind": "contains", "label": None})

    # depends: parent stage -> child stage attempt. The parent attempt is the latest one that started before the
    # child (else its latest attempt); a parent assigned to an earlier job = a reused shuffle ("reused").
    attempts_by_sid: dict[Any, list[dict]] = {}
    for n in stage_nodes:
        attempts_by_sid.setdefault(n["_sid"], []).append(n)
    for n in stage_nodes:
        for pid in n["_parents"]:
            cands = attempts_by_sid.get(pid)
            if not cands:
                continue
            st = n["start"]
            before = [c for c in cands if st is None or c["start"] is None or c["start"] <= st]
            p = max(before or cands, key=lambda c: c["_att"])
            same_job = p["_job"] == n["_job"]
            edges.append({"source": p["id"], "target": n["id"], "kind": "depends",
                          "label": None if same_job else "reused"})
    # retry: attempt N -> N+1
    for sid, cands in attempts_by_sid.items():
        cands = sorted(cands, key=lambda c: c["_att"])
        for prev, nxt in zip(cands, cands[1:]):
            edges.append({"source": prev["id"], "target": nxt["id"], "kind": "retry",
                          "label": _short(nxt["what"].get("retry_of_failure"), 300) or "resubmitted"})
    for qn, q in zip(query_nodes, queries):
        for jn in jobs_of_query.get(q.get("sql_execution_id"), []):
            edges.append({"source": qn["id"], "target": jn["id"], "kind": "runs_query", "label": None})
    for cn in connect_nodes:
        oid = cn["what"].get("operation_id")
        for jn in jobs_of_op.get(oid, []):
            edges.append({"source": cn["id"], "target": jn["id"], "kind": "from_connect", "label": None})
    edges = [e for e in edges if not (e["target"].startswith("stage:") and e["target"] not in shown)]

    for n in stage_nodes:
        for k in ("_sid", "_att", "_job", "_parents"):
            n.pop(k, None)
    for jn in job_nodes:
        jn.pop("_flags", None)
    nodes = [app_node, *job_nodes, *stage_nodes, *query_nodes, *connect_nodes]
    return Q.clean({
        "ctx": ctx, "contexts": ctxs, "start": min(times) if times else None, "end": max(times) if times else None,
        "nodes": nodes, "edges": edges, "truncated": truncated, "dropped_stages": dropped,
        "stages_total": stages_total,
    })


# ---------------------------------------------------------------------------------------------------------------------
# Spill & shuffle over time
# ---------------------------------------------------------------------------------------------------------------------

SPILL_SUM_COLS = ("tasks", "mem_spill", "disk_spill", "shuffle_read", "shuffle_write", "input_bytes", "output_bytes",
                  "gc_ms", "run_ms")


def bucket_ms(store: Q.Store, cid: str) -> int:
    """Bucket width the cluster was built with (summary.json), else the current rules."""
    try:
        s = Q.read_summary(store, cid)
        v = s.get("timeline_bucket_seconds")
        if isinstance(v, int) and v > 0:
            return v * 1000
    except Q.ApiError:
        pass
    return int(_rules().timeline_bucket_seconds) * 1000


def spill_shuffle(store: Q.Store, cid: str, ctx: str | None, by: str = "executor", run: str | None = None) -> dict[str, Any]:
    if by not in ("executor", "stage"):
        raise Q.BadRequest("by must be 'executor' or 'stage'")
    store.cluster_dir(cid)
    bms = bucket_ms(store, cid)
    with store.connect() as con:
        ctx, ctxs = resolve_ctx(store, con, cid, ctx, allow_all=True)
        out: dict[str, Any] = {"ctx": ctx, "contexts": ctxs, "by": by, "bucket_ms": bms, "start": None, "end": None,
                               "series": [], "buckets": [], "totals": {c: 0 for c in SPILL_SUM_COLS}}
        path = store.dataset_path(cid, "spill_shuffle_timeline", required=False)
        if path is None:
            out["missing"] = ["spill_shuffle_timeline"]
            return out
        multi = ctx is None and len(ctxs) > 1
        where, params = ("spark_context_id = ?", [ctx]) if ctx else ("TRUE", [])
        spath = store.dataset_path(cid, "stages", required=False)
        if run and spath is not None and "run_key" in store.schema(con, spath):
            # Revision 15: one run's stages only (the cluster's other runs are left out)
            where += (f" AND (spark_context_id, stage_id, coalesce(stage_attempt, 0)) IN (SELECT spark_context_id, stage_id, "
                      f"stage_attempt FROM {store.src(spath)} WHERE run_key = ?)")
            params = params + [run]
        if by == "executor":
            key_sql = "coalesce(executor_id, '?')"
            extra = "min(executor_id) AS executor_id, NULL::BIGINT AS stage_id, NULL::BIGINT AS stage_attempt"
        else:
            key_sql = "CAST(stage_id AS VARCHAR) || '.' || CAST(coalesce(stage_attempt, 0) AS VARCHAR)"
            extra = "NULL::VARCHAR AS executor_id, min(stage_id) AS stage_id, min(coalesce(stage_attempt, 0)) AS stage_attempt"
        if multi:
            key_sql = f"spark_context_id || '/' || {key_sql}"
        sums = ", ".join(f"sum({Q.qi(c)})::BIGINT AS {Q.qi(c)}" for c in SPILL_SUM_COLS)
        src = store.src(path)
        rows = store.rows(
            con,
            f"SELECT minute AS ts, {key_sql} AS key, min(spark_context_id) AS spark_context_id, {extra}, {sums} "
            f"FROM {src} WHERE {where} GROUP BY minute, {key_sql} ORDER BY minute, key",
            params,
        )
    series: dict[str, dict[str, Any]] = {}
    for r in rows:
        k = r["key"]
        s = series.get(k)
        if s is None:
            if by == "executor":
                ex = r.get("executor_id")
                label = "driver" if ex == "driver" else f"executor {ex if ex is not None else '?'}"
            else:
                label = f"stage {r.get('stage_id')}" + (f" (attempt {r['stage_attempt']})" if r.get("stage_attempt") else "")
            if multi:
                label = f"{r.get('spark_context_id')} {label}"
            s = series[k] = {"key": k, "label": label, "spark_context_id": r.get("spark_context_id"),
                             "executor_id": r.get("executor_id"), "stage_id": r.get("stage_id"),
                             "stage_attempt": r.get("stage_attempt"), **{c: 0 for c in SPILL_SUM_COLS},
                             "first": r["ts"], "last": r["ts"]}
        for c in SPILL_SUM_COLS:
            v = r.get(c) or 0
            s[c] += v
            out["totals"][c] += v
        s["last"] = r["ts"]
    out["series"] = sorted(series.values(), key=lambda s: -(s["disk_spill"] + s["mem_spill"] + s["shuffle_read"]
                                                            + s["shuffle_write"]))
    out["buckets"] = [{"ts": r["ts"], "key": r["key"], **{c: r.get(c) or 0 for c in SPILL_SUM_COLS}} for r in rows]
    if rows:
        out["start"] = rows[0]["ts"]
        out["end"] = max(r["ts"] for r in rows) + bms
    return Q.clean(out)


def spill_markers(store: Q.Store, con, cid: str, ctx: str | None, limit: int) -> list[dict[str, Any]]:
    """Gantt `spill` markers: tasks that spilled to disk, at most one marker per executor per time bucket
    (ts = the earliest spilling task's finish time in that bucket)."""
    from ..util import fmt_bytes

    tpath = store.dataset_path(cid, "tasks", required=False)
    if tpath is None or ctx is None:
        return []
    sch = store.schema(con, tpath)
    if "disk_spill" not in sch or "finish_time" not in sch:
        return []
    bms = bucket_ms(store, cid)
    rows = store.rows(
        con,
        f"SELECT executor_id, min(finish_time) AS ts, sum(disk_spill)::BIGINT AS disk_spill, "
        f"sum(coalesce(mem_spill, 0))::BIGINT AS mem_spill, count(*) AS tasks, "
        f"list(DISTINCT stage_id ORDER BY stage_id) AS stage_ids "
        f"FROM {store.src(tpath)} WHERE spark_context_id = ? AND disk_spill > 0 AND finish_time IS NOT NULL "
        f"GROUP BY executor_id, epoch_ms(finish_time) // {int(bms)} ORDER BY ts LIMIT {int(limit)}",
        [ctx],
    )
    out = []
    for r in rows:
        ex = r.get("executor_id")
        stages = r.get("stage_ids") or []
        st = ", ".join(str(s) for s in stages[:5]) + (" ..." if len(stages) > 5 else "")
        out.append({
            "ts": r["ts"], "executor_id": ex, "kind": "spill",
            "label": f"disk spill {fmt_bytes(r['disk_spill'])} on executor {ex} ({r['tasks']} task"
                     f"{'s' if r['tasks'] != 1 else ''}, stage{'s' if len(stages) != 1 else ''} {st})",
            "disk_spill": r["disk_spill"], "mem_spill": r["mem_spill"], "tasks": r["tasks"], "stage_ids": stages,
        })
    return out
