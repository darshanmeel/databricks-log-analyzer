"""runs (Revision 6): one row per run unit, a run_key on every run-scoped dataset, and shared-resource attribution.

A run unit is chosen per Spark job, first that applies:
  task_run:<taskRunId>         a Databricks job task run (spark.databricks.job.taskRunId)
  job_run:<jobId>-<runId>      a Databricks job run found in the job group (".._job-<id>-run-<id>-action-..")
  connect:<sessionId>          a Spark Connect session (through the job's Connect operation)
  notebook:<path>              an interactive notebook
  job_group:<group>            any other job group (a trailing "-action-<n>" suffix is dropped)
  app:<spark_context_id>       everything else in the Spark application
Stages, tasks, queries, retries, hotspots, findings and story rows inherit the run_key of their Spark job. Executors,
GC, log lines and time-bucket peaks are shared by the cluster; they get `affected_runs` (runs that had tasks on that
executor / in that bucket) and `top_run_key` (the run with the most task time there).
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict

import pandas as pd

from ..parsing.eventlog import is_connect_blob
from ..util import fmt_words, is_null

RUN_COLS = ["cluster_id", "spark_context_id", "run_key", "kind", "label", "databricks_job_id", "databricks_run_id",
            "task_run_id", "notebook_path", "user", "start_time", "end_time", "duration_ms", "status", "spark_jobs",
            "stages", "tasks", "failed_tasks", "retried_tasks", "disk_spill", "mem_spill", "shuffle_read",
            "shuffle_write", "findings", "max_severity", "overlapping_runs", "parent_run_id", "job_name", "task_type"]
# Revision 13: data in/out, its spread over tasks, and how long the same Databricks job usually takes

_JOB_RUN = re.compile(r"job-(\d+)-run-(\d+)")
_ACTION = re.compile(r"[-_]action-[\w-]+$")
_SEV = {"high": 0, "medium": 1, "low": 2}


def _ms(v) -> int | None:
    """Epoch milliseconds from an int, a float, or a (pandas) datetime; None when missing."""
    if is_null(v):
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if hasattr(v, "timestamp"):
        if getattr(v, "tzinfo", None) is None:
            return int(pd.Timestamp(v).tz_localize("UTC").timestamp() * 1000)
        return int(v.timestamp() * 1000)
    return None


def _s(v):
    return None if is_null(v) or str(v).strip() == "" else str(v)


def job_run_key(job: dict, connect_sessions: dict) -> tuple[str, str, str]:
    """(run_key, kind, label) for one spark_jobs row."""
    ctx = job["spark_context_id"]
    tr = _s(job.get("databricks_task_run_id"))
    jid, rid = _s(job.get("databricks_job_id")), _s(job.get("databricks_run_id"))
    if tr:
        lbl = "Job " + (jid or "?") + (f" · run {rid}" if rid else "") + f" · task run {tr}"
        return f"task_run:{tr}", "task_run", lbl
    grp = _s(job.get("job_group"))
    m = _JOB_RUN.search(grp or "")
    if m:
        return f"job_run:{m.group(1)}-{m.group(2)}", "task_run", f"Job {m.group(1)} · run {m.group(2)}"
    if jid and rid:
        return f"job_run:{jid}-{rid}", "task_run", f"Job {jid} · run {rid}"
    op = _s(job.get("connect_operation_id"))
    if op and connect_sessions.get((ctx, op)):
        sess, user = connect_sessions[(ctx, op)]
        return f"connect:{sess}", "connect_session", "Spark Connect session " + sess[:8] + (f" · {user}" if user else "")
    nb = _s(job.get("notebook_path"))
    if nb:
        return f"notebook:{nb}", "notebook", "Notebook " + nb.rsplit("/", 1)[-1]
    if grp:
        g = _ACTION.sub("", grp)
        return f"job_group:{g}", "job_group", f"Job group {g[:40]}"
    return f"app:{ctx}", "app", f"Spark app {ctx[-6:]}"


def attach_run_keys(cid: str, d: dict) -> list[dict]:
    """Add run_key to the run-scoped datasets in `d` (in place) and return the runs rows."""
    jobs, stages, queries = d["spark_jobs"], d["stages"], d["sql_queries"]
    sessions = {(o["spark_context_id"], o["operation_id"]): (o["session_id"], _s(o.get("user_name")))
                for o in d["connect_operations"] if o.get("session_id")}
    meta: dict[str, dict] = {}
    job_key: dict[tuple, str] = {}
    for j in jobs:
        key, kind, label = job_run_key(j, sessions)
        j["run_key"] = key
        job_key[(j["spark_context_id"], j["spark_job_id"])] = key
        m = meta.setdefault(key, {"cluster_id": cid, "spark_context_id": j["spark_context_id"], "run_key": key,
                                  "kind": kind, "label": label, "databricks_job_id": None, "databricks_run_id": None,
                                  "task_run_id": None, "notebook_path": None, "user": None,
                                  "parent_run_id": None, "job_name": None, "task_type": None})
        for src, dst in (("databricks_job_id", "databricks_job_id"), ("databricks_run_id", "databricks_run_id"),
                         ("databricks_task_run_id", "task_run_id"), ("notebook_path", "notebook_path"),
                         ("databricks_parent_run_id", "parent_run_id"), ("databricks_job_name", "job_name"),
                         ("databricks_task_type", "task_type")):
            if m[dst] is None and _s(j.get(src)):
                m[dst] = _s(j.get(src))
        op = _s(j.get("connect_operation_id"))
        if m["user"] is None and op and sessions.get((j["spark_context_id"], op)):
            m["user"] = sessions[(j["spark_context_id"], op)][1]

    def by_job(ctx, job):
        return job_key.get((ctx, None if is_null(job) else int(job)))

    stage_key: dict[tuple, str] = {}
    for s in stages:
        s["run_key"] = by_job(s["spark_context_id"], s.get("spark_job_id"))
        if s["run_key"]:
            stage_key[(s["spark_context_id"], s["stage_id"])] = s["run_key"]
    query_key: dict[tuple, str] = {}
    for j in jobs:  # a query belongs to the run of its first Spark job
        q = j.get("sql_execution_id")
        if not is_null(q):
            query_key.setdefault((j["spark_context_id"], int(q)), j["run_key"])
    for q in queries:
        q["run_key"] = query_key.get((q["spark_context_id"], q["sql_execution_id"]))
    for q in queries:  # a query that ran no Spark job belongs to the run of the query it ran inside, if any
        root = q.get("root_execution_id")
        if q["run_key"] is None and not is_null(root):
            q["run_key"] = query_key.get((q["spark_context_id"], int(root)))
            if q["run_key"]:
                query_key[(q["spark_context_id"], q["sql_execution_id"])] = q["run_key"]
    # a streaming micro-batch's root query runs no job itself: it belongs to the run of the queries inside it
    for q in queries:
        root = q.get("root_execution_id")
        if q["run_key"] and not is_null(root) and int(root) != q["sql_execution_id"]:
            query_key.setdefault((q["spark_context_id"], int(root)), q["run_key"])
    for q in queries:  # the root, then the other job-less queries under it
        if q["run_key"] is None:
            q["run_key"] = query_key.get((q["spark_context_id"], q["sql_execution_id"]))
    for q in queries:
        root = q.get("root_execution_id")
        if q["run_key"] is None and not is_null(root):
            q["run_key"] = query_key.get((q["spark_context_id"], int(root)))
    op_key = {(j["spark_context_id"], j.get("connect_operation_id")): j["run_key"] for j in jobs
              if j.get("connect_operation_id")}
    for o in d["connect_operations"]:
        o["run_key"] = op_key.get((o["spark_context_id"], o["operation_id"]))
        if o["run_key"] is None and o.get("session_id"):
            o["run_key"] = f"connect:{o['session_id']}"
    # driver-only SQL of a Spark Connect run (DDL, ALTER, metadata): no job, so no run from the jobs; it ran during one
    # of the run's Connect operations, and that session's other operations name the run
    sess_run: dict[tuple, str] = {}
    for o in d["connect_operations"]:
        if o.get("session_id") and o["run_key"] in meta:
            sess_run.setdefault((o["spark_context_id"], o["session_id"]), o["run_key"])
    windows: dict[str, list[tuple]] = defaultdict(list)
    for o in d["connect_operations"]:
        rk = sess_run.get((o["spark_context_id"], o.get("session_id")))
        a, z = _ms(o.get("start_time")), _ms(o.get("finish_time") or o.get("closed_time"))
        if rk and a is not None:
            windows[o["spark_context_id"]].append((a, z if z is not None else a, rk))
    for q in queries:
        if q["run_key"] is not None or not windows.get(q["spark_context_id"]):
            continue
        t0 = _ms(q.get("start_time"))
        hit = {rk for a, z, rk in windows[q["spark_context_id"]] if t0 is not None and a <= t0 <= z}
        if len(hit) == 1:
            q["run_key"] = hit.pop()
            query_key[(q["spark_context_id"], q["sql_execution_id"])] = q["run_key"]
    for r in d["query_profile"]:
        r["run_key"] = query_key.get((r["spark_context_id"], r["sql_execution_id"]))

    def row_key(r):
        ctx = r.get("spark_context_id")
        k = by_job(ctx, r.get("spark_job_id"))
        if k is None and not is_null(r.get("stage_id")):
            k = stage_key.get((ctx, int(r["stage_id"])))
        if k is None and not is_null(r.get("sql_execution_id")):
            k = query_key.get((ctx, int(r["sql_execution_id"])))
        return k

    for name in ("task_retries", "hotspots", "findings", "run_story"):
        for r in d[name]:
            r["run_key"] = row_key(r)

    tdf: pd.DataFrame = d["tasks"]
    if not tdf.empty:
        tdf["run_key"] = [stage_key.get((c, None if is_null(s) else int(s)))
                          for c, s in zip(tdf["spark_context_id"], tdf["stage_id"])]
    else:
        tdf["run_key"] = pd.Series(dtype=object)
    sep = d["stage_executor_profile"]
    if isinstance(sep, pd.DataFrame):
        sep["run_key"] = [stage_key.get((c, None if is_null(s) else int(s)))
                          for c, s in zip(sep["spark_context_id"], sep["stage_id"])] if not sep.empty else None
    else:
        for r in sep:
            r["run_key"] = stage_key.get((r["spark_context_id"], r["stage_id"]))

    _attribute_shared(d, tdf)
    return _runs_rows(cid, meta, d, tdf)


def _attribute_shared(d: dict, tdf: pd.DataFrame) -> None:
    """affected_runs / top_run_key on executor_profile, and on hotspots rows that describe a shared time bucket."""
    ex_runs: dict[tuple, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    bucket_runs: dict[tuple, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    if not tdf.empty:
        t = tdf[tdf["run_key"].notna()]
        for (ctx, ex, rk), ms in t.groupby(["spark_context_id", "executor_id", "run_key"])["task_ms"].sum().items():
            ex_runs[(ctx, ex)][rk] += float(ms or 0)
        if any(h["kind"].endswith("_peak") for h in d["hotspots"]):
            ts = t["finish_time"].where(t["finish_time"].notna(), t["launch_time"])
            for h in d["hotspots"]:
                if not h["kind"].endswith("_peak"):
                    continue
                m = t[(t["spark_context_id"] == h["spark_context_id"]) & (ts >= h["ts_start"]) & (ts < h["ts_end"])]
                for rk, ms in m.groupby("run_key")["task_ms"].sum().items():
                    bucket_runs[(h["spark_context_id"], h["ts_start"], h["kind"])][rk] += float(ms or 0)
    for e in d["executor_profile"]:
        runs = ex_runs.get((e["spark_context_id"], e["executor_id"]), {})
        e["affected_runs"] = sorted(runs, key=lambda k: -runs[k]) or None
        e["top_run_key"] = max(runs, key=runs.get) if runs else None
    for h in d["hotspots"]:
        if h["kind"].endswith("_peak"):
            runs = bucket_runs.get((h["spark_context_id"], h["ts_start"], h["kind"]), {})
            h["affected_runs"] = sorted(runs, key=lambda k: -runs[k]) or None
            if len(runs) > 1 and h.get("detail"):
                tot = sum(runs.values()) or 1
                top = max(runs, key=runs.get)
                h["detail"] += (f" {len(runs)} runs were active in that minute; {top} used "
                                f"{runs[top] / tot:.0%} of the task time.")
        else:
            h["affected_runs"] = [h["run_key"]] if h.get("run_key") else None


def _runs_rows(cid: str, meta: dict, d: dict, tdf: pd.DataFrame) -> list[dict]:
    agg: dict[str, dict] = {k: {"spark_jobs": 0, "stages": 0, "tasks": 0, "failed_tasks": 0, "retried_tasks": 0,
                                "disk_spill": 0, "mem_spill": 0, "shuffle_read": 0, "shuffle_write": 0,
                                "findings": 0, "max_severity": None, "start": None, "end": None,
                                "failed": False, "open": False} for k in meta}

    def span(a, s, e):
        if not is_null(s):
            a["start"] = s if a["start"] is None else min(a["start"], s)
        if not is_null(e):
            a["end"] = e if a["end"] is None else max(a["end"], e)

    for j in d["spark_jobs"]:
        a = agg[j["run_key"]]
        a["spark_jobs"] += 1
        span(a, j.get("start_time"), j.get("end_time"))
        if j.get("result") == "JobFailed":
            a["failed"] = True
        elif j.get("result") is None:
            a["open"] = True
    for s in d["stages"]:
        if s.get("run_key") in agg:
            agg[s["run_key"]]["stages"] += 1
    for q in d["sql_queries"]:
        if q.get("run_key") in agg and q.get("status") == "failed":
            agg[q["run_key"]]["failed"] = True
    if not tdf.empty:
        g = tdf[tdf["run_key"].notna()].groupby("run_key")
        sums = g[["disk_spill", "mem_spill", "shuffle_read", "shuffle_write"]].sum(min_count=1)
        for rk, n in g.size().items():
            a = agg.get(rk)
            if a is None:
                continue
            a["tasks"] = int(n)
            a["failed_tasks"] = int(g.get_group(rk)["failed"].sum())
            for c in ("disk_spill", "mem_spill", "shuffle_read", "shuffle_write"):
                v = sums.loc[rk, c]
                a[c] = 0 if is_null(v) else int(v)
    for r in d["task_retries"]:
        if r.get("run_key") in agg:
            agg[r["run_key"]]["retried_tasks"] += 1
    for f in d["findings"]:
        a = agg.get(f.get("run_key"))
        if a is None:
            continue
        a["findings"] += 1
        if a["max_severity"] is None or _SEV.get(f["severity"], 9) < _SEV.get(a["max_severity"], 9):
            a["max_severity"] = f["severity"]

    rows = []
    for k, m in meta.items():
        a = agg[k]
        status = "failed" if a["failed"] else ("incomplete" if a["open"] else "succeeded")
        dur = a["end"] - a["start"] if a["start"] is not None and a["end"] is not None else None
        rows.append({**m, "start_time": a["start"], "end_time": a["end"], "duration_ms": dur, "status": status,
                     **{c: a[c] for c in ("spark_jobs", "stages", "tasks", "failed_tasks", "retried_tasks",
                                          "disk_spill", "mem_spill", "shuffle_read", "shuffle_write", "findings",
                                          "max_severity")},
                     "overlapping_runs": None})
    for r in rows:
        ov = [o["run_key"] for o in rows if o is not r and o["spark_context_id"] == r["spark_context_id"]
              and None not in (r["start_time"], r["end_time"], o["start_time"], o["end_time"])
              and o["start_time"] < r["end_time"] and r["start_time"] < o["end_time"]]
        r["overlapping_runs"] = ov or None
    rows.sort(key=lambda r: (r["start_time"] is None, r["start_time"] or 0, r["run_key"]))
    names = run_names(d)
    for r in rows:
        r["program"], r["subject"] = names.get(r["run_key"], (None, None))
    from .aggregate import DIST_COLS, dist_rows

    data = dist_rows(tdf, ["run_key"], ("input_bytes", "input_records", "output_bytes", "output_records")) \
        if not tdf.empty and "run_key" in tdf else {}
    for r in rows:
        got = data.get(r["run_key"], {})
        r.update({c: got.get(c) for c in (*DIST_COLS, "input_bytes", "input_records", "output_bytes",
                                          "output_records")})
    # the same job's other runs: is this one unusually slow?
    # the same code over different tables (one notebook loading 30 tables) is not the same work: within a family,
    # runs are compared only with runs on the same subject; with fewer than 3 of those there is no usual yet
    fam: dict = {}
    for r in rows:
        fam.setdefault(run_family(r), []).append(r)
    groups: list[list[dict]] = []
    for rs in fam.values():
        subj = {x.get("subject") for x in rs}
        if len(subj) > 1:
            by: dict = {}
            for x in rs:
                by.setdefault(x.get("subject"), []).append(x)
            groups += list(by.values())
        else:
            groups.append(rs)
    for rs in groups:
        durs = sorted(x["duration_ms"] for x in rs if x["duration_ms"] is not None)
        typ = durs[(len(durs) - 1) // 2] if len(durs) >= 3 else None
        for r in rs:
            r["same_job_runs"] = len(rs)
            r["typical_duration_ms"] = typ
            r["vs_typical"] = round(r["duration_ms"] / typ, 2) if typ and r["duration_ms"] is not None else None
    return rows


def run_family(r: dict) -> str:
    """Runs of the same thing: the same program (notebook or main class) of the same Databricks job, else the
    notebook, else the run kind and label. Tasks of one multi-task job that run different code are not compared."""
    prog = r.get("program") or r.get("notebook_path")
    if r.get("databricks_job_id"):
        return f"job:{r['databricks_job_id']}:{prog or ''}"
    if prog:
        return f"program:{prog}"
    return f"{r.get('kind')}:{r.get('label')}"


_CLASS = re.compile(r"^\s*((?:[a-z_][\w$]*\.){2,}[A-Z][\w$]*)")
_IDENT = re.compile(r"\b[A-Za-z][A-Za-z0-9]*_[A-Za-z0-9_]{2,}\b")
_NOISE = {"run_id", "batch_id", "job_id", "task_values", "spark_catalog", "session_id", "user_id", "operation_id"}


def run_names(d: dict) -> dict[str, tuple[str | None, str | None]]:
    """run_key -> (program, subject). program: the notebook's name, else the main class (a JAR or Python wheel task);
    subject: what the run worked on, the name that comes up most in its job descriptions and named tables
    (catalog.schema.table, not storage paths), e.g. the table a per-table task loads."""
    prog: dict[str, Counter] = defaultdict(Counter)
    subj: dict[str, Counter] = defaultdict(Counter)
    for j in d["spark_jobs"]:
        rk = j.get("run_key")
        nb = _s(j.get("notebook_path"))
        if nb:
            prog[rk][nb.rstrip("/").rsplit("/", 1)[-1]] += 1
        desc = _s(j.get("description")) or ""
        if is_connect_blob(desc):
            continue  # "Spark Connect - session_id ...": names the session, not what the run worked on
        m = _CLASS.match(desc)
        if m:
            prog[rk][m.group(1).rsplit(".", 1)[-1].rstrip(".")] += 1
        elif desc:
            for w in _IDENT.findall(desc[:300]):
                if w.lower() not in _NOISE:
                    subj[rk][w] += 1
    for q in d["sql_queries"]:
        rk = q.get("run_key")
        for col, wt in (("tables_written", 3), ("tables_read", 2)):
            for t in q.get(col) or []:
                t = str(t)
                if "/" in t or ":" in t:
                    continue  # a storage path says little
                name = t.rsplit(".", 1)[-1]
                if name.lower() not in _NOISE:
                    subj[rk][name] += wt
    out = {}
    for rk in set(prog) | set(subj):
        p = prog[rk].most_common(1)[0][0] if prog[rk] else None
        s = subj[rk].most_common(1)[0][0] if subj[rk] else None
        out[rk] = (p, s)
    return out


def default_run(runs: list[dict]) -> str | None:
    """The run the UI opens first: the only one, else a failed one (the longest failed), else the longest."""
    if not runs:
        return None
    if len(runs) == 1:
        return runs[0]["run_key"]
    failed = [r for r in runs if r["status"] == "failed"]
    pool = failed or runs
    return max(pool, key=lambda r: r["duration_ms"] or 0)["run_key"]


def runs_note(runs: list[dict]) -> str | None:
    if len(runs) <= 1:
        return None
    k = default_run(runs)
    r = next(x for x in runs if x["run_key"] == k)
    par = sum(1 for x in runs if x["overlapping_runs"])
    return (f"This cluster ran {len(runs)} runs" + (f" ({par} overlapped in time)" if par else "")
            + f"; showing {r['label']} ({r['status']}, {fmt_words(r['duration_ms'])}) by default.")
