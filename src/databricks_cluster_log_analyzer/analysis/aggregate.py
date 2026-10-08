"""Event tables -> tasks, stages, spark_jobs, sql_queries, executors, apps (notebook V2-V6)."""

from __future__ import annotations

import hashlib
import math
import re
from collections import Counter

import numpy as np
import pandas as pd

from ..config import Rules
from ..parsing.eventlog import TASK_COLS, EventTables, is_connect_blob, split_tags, statement_label
from ..parsing.sqlplan import plan_from_text, plan_node_rows
from ..parsing.plans import name_tables, plan_summary, table_names
from ..util import nn, round_half_up, to_int

PLAN_HASH_STRIP_RE = re.compile(r"#\d+|\[plan_id=\d+\]|\(\d+\)|\d+")
STAGE_KEY = ["spark_context_id", "stage_id", "stage_attempt"]

# Revision 13: how the data and the time were spread over the tasks of a stage, job, query or run. Data in = storage
# input + shuffle read of each successful task (bytes, and rows); percentiles are lower (rank ceil(q * n)), like the
# median the notebook uses.
DIST_COLS = ("min_task_bytes_in", "p10_task_bytes_in", "p50_task_bytes_in", "p90_task_bytes_in", "max_task_bytes_in",
             "avg_task_bytes_in", "min_task_rows_in", "p10_task_rows_in", "p50_task_rows_in", "p90_task_rows_in",
             "max_task_rows_in", "avg_task_rows_in", "p10_task_ms", "p50_task_ms", "p90_task_ms", "wmed_task_bytes_in")
# How many successful tasks read how much (storage input + shuffle read), and how much of the data each band read:
# none, under 10 MiB, 10-128 MiB (128 MiB is Spark's default file split), 128-256 MiB, 256 MiB or more. A few tasks in
# the top band holding most of the data is the skew to fix.
SIZE_EDGES_MB = (10, 128, 256)
SIZE_BANDS = ("none", "lt10", "10_128", "128_256", "ge256")
SIZE_COLS = tuple(f"tasks_{b}" for b in SIZE_BANDS) + tuple(f"bytes_{b}" for b in SIZE_BANDS[1:])
DIST_COLS = DIST_COLS + SIZE_COLS
IO_COLS = ("input_bytes", "input_records", "output_bytes", "output_records", "shuffle_read", "shuffle_write")
EXEC_RES = ("resource_profile_id", "heap_mb", "overhead_mb", "offheap_mb", "unified_memory", "storage_memory",
            "task_cpus")


def _ranked(t: pd.DataFrame, keys: list[str], col: str) -> pd.DataFrame:
    """min, p10, p50, p90, max, avg of `col` per `keys` (lower percentiles)."""
    d = t[t[col].notna()].sort_values(keys + [col], kind="mergesort")
    if d.empty:
        # keep the key index (named), or joining it with the other parts loses the keys: a cluster where no task
        # read any bytes (only JDBC sources) would fail to build
        idx = (pd.MultiIndex.from_arrays([[] for _ in keys], names=keys) if len(keys) > 1
               else pd.Index([], name=keys[0]))
        return pd.DataFrame(columns=["min", "p10", "p50", "p90", "max", "avg"], index=idx)
    g = d.groupby(keys, sort=False, dropna=False)
    out = pd.DataFrame({"min": g[col].min(), "max": g[col].max(), "avg": g[col].mean()})
    d = d.assign(_rk=g.cumcount(), _n=g[col].transform("size"))
    for q in (10, 50, 90):
        pick = d[d["_rk"] == np.ceil(q / 100 * d["_n"]).astype(int) - 1].set_index(keys)[col]
        out = out.join(pick.rename(f"p{q}"))
    return out[["min", "p10", "p50", "p90", "max", "avg"]]


def task_dist(tdf: pd.DataFrame, keys: list[str], nonzero: bool = False) -> pd.DataFrame:
    """DIST_COLS per `keys` from the task frame (rows with a null key are left out). `nonzero`: the data spread over
    the tasks that read something (a job or query mixes stages, and its tiny driver-side stages would pull the median
    to zero); a stage keeps its empty tasks, they are part of its skew."""
    t = tdf[[*keys, "failed", "task_ms", "input_bytes", "shuffle_read", "input_records", "shuffle_read_records"]]
    t = t[t[keys].notna().all(axis=1)].copy()
    if t.empty:
        return pd.DataFrame(columns=list(DIST_COLS))
    t["_bytes_in"] = t[["input_bytes", "shuffle_read"]].sum(axis=1, min_count=1)
    t["_rows_in"] = t[["input_records", "shuffle_read_records"]].sum(axis=1, min_count=1)
    ok = t[~t["failed"].astype(bool)]
    parts = []
    for unit, col in (("bytes", "_bytes_in"), ("rows", "_rows_in")):
        r = _ranked(ok[ok[col] > 0] if nonzero else ok, keys, col)
        parts.append(r.rename(columns={c: f"{c}_task_{unit}_in" for c in r.columns}))
    r = _ranked(t, keys, "task_ms")[["p10", "p50", "p90"]]
    parts.append(r.rename(columns={c: f"{c}_task_ms" for c in r.columns}))
    # data-weighted median: half of all bytes were read by tasks at least this big (the size of the tasks that did
    # the work, even when most tasks read next to nothing)
    w = ok[ok["_bytes_in"] > 0].sort_values(keys + ["_bytes_in"], kind="mergesort")
    if not w.empty:
        g = w.groupby(keys, sort=False, dropna=False)["_bytes_in"]
        w = w.assign(_cum=g.cumsum(), _tot=g.transform("sum"))
        parts.append(w[w["_cum"] >= w["_tot"] / 2].groupby(keys, sort=False, dropna=False)["_bytes_in"].first()
                     .rename("wmed_task_bytes_in").to_frame())
    parts.append(_size_bands(ok, keys))
    out = pd.concat(parts, axis=1)
    for c in DIST_COLS:
        if c not in out.columns:
            out[c] = np.nan
    return out[list(DIST_COLS)]


def _size_bands(ok: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """SIZE_COLS per `keys`: task counts per size band (all successful tasks, the empty ones too) and bytes read."""
    b = ok["_bytes_in"].fillna(0)
    mb = 1 << 20
    edges = [0, *[e * mb for e in SIZE_EDGES_MB]]
    band = np.select([b <= 0, b < edges[1], b < edges[2], b < edges[3]], list(SIZE_BANDS[:4]), SIZE_BANDS[4])
    d = ok[keys].assign(_band=band, _b=b)
    n = d.groupby([*keys, "_band"], dropna=False).size().unstack("_band")
    by = d.groupby([*keys, "_band"], dropna=False)["_b"].sum().unstack("_band")
    out = pd.DataFrame(index=n.index)
    for x in SIZE_BANDS:
        out[f"tasks_{x}"] = n[x].fillna(0) if x in n else 0
        if x != "none":
            out[f"bytes_{x}"] = by[x].fillna(0) if x in by else 0
    return out


def dist_rows(tdf: pd.DataFrame, keys: list[str], sums: tuple[str, ...] = ()) -> dict:
    """{key tuple (or scalar for one key): {DIST_COLS..., sums...}} as ints; data spread over tasks that read data."""
    if tdf.empty:
        return {}
    d = task_dist(tdf, keys, nonzero=True)
    if sums:
        t = tdf[tdf[keys].notna().all(axis=1)]
        tot = t.groupby(keys, sort=False, dropna=False)[list(sums)].sum(min_count=1)
        d = tot.assign(**{c: np.nan for c in DIST_COLS}) if d.empty else d.join(tot, how="outer")
    out = {}
    for k, row in d.iterrows():
        out[k] = {c: to_int(nn(v)) for c, v in row.items()}
    return out


def _mib(v, default_unit: str = "m") -> int | None:
    """A Spark size ("7284m", "4g", "512", "2147483648b") in MiB; a bare number is `default_unit`."""
    if v is None:
        return None
    m = re.fullmatch(r"\s*([\d.]+)\s*([kmgtp]?)b?\s*", str(v).lower())
    if not m:
        return None
    n, u = float(m.group(1)), m.group(2) or default_unit
    if u == "b" or (not m.group(2) and default_unit == "b"):
        return int(n / (1 << 20))
    return int(n * {"k": 1 / 1024, "m": 1, "g": 1024, "t": 1 << 20, "p": 1 << 30}[u])


def plan_hash(plan: str | None) -> str | None:
    if plan is None:
        return None
    return hashlib.sha256(PLAN_HASH_STRIP_RE.sub("", plan).encode("utf-8")).hexdigest()[:12]


def tasks_frame(tables: EventTables) -> pd.DataFrame:
    df = pd.DataFrame({c: tables.tasks[c] for c in TASK_COLS}, columns=TASK_COLS)
    for c in ("stage_id", "stage_attempt", "task_id", "task_attempt", "launch_time", "finish_time", "task_ms",
              "run_ms", "gc_ms", "peak_mem", "mem_spill", "disk_spill", "input_bytes", "input_records",
              "output_bytes", "shuffle_read", "shuffle_write", "task_index", "cpu_ms", "fetch_wait_ms",
              "shuffle_read_records", "shuffle_write_records", "output_records", "shuffle_write_ms"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    df["failed"] = df["failed"].astype(bool)
    df["speculative"] = df["speculative"].astype(object)
    return df


def _stage_metrics(tdf: pd.DataFrame) -> dict[tuple, dict]:
    """Per (ctx, stage_id, attempt): notebook stage_metrics + internal gc/run sums."""
    if tdf.empty:
        return {}
    t = tdf[tdf["stage_id"].notna()].copy()
    t["stage_attempt"] = t["stage_attempt"].fillna(0)
    g = t.groupby(STAGE_KEY, sort=False, dropna=False)
    # task times and the distributions come from the successful attempts, as in Spark's own stage summary: a failed
    # attempt (stuck in GC, killed with its executor) is not a skewed task. A stage with no success keeps them all.
    ok = t[~t["failed"]]
    ok = pd.concat([ok, t[~t.set_index(STAGE_KEY).index.isin(ok.set_index(STAGE_KEY).index)]])
    go = ok.groupby(STAGE_KEY, sort=False, dropna=False)
    agg = pd.DataFrame({
        "tasks": g.size(),
        "failed_tasks": g["failed"].sum(),
        "max_task_ms": go["task_ms"].max(),
        "min_task_ms": go["task_ms"].min(),
        "max_peak_mem": g["peak_mem"].max(),
        "executors_used": g["executor_id"].nunique(),
    })
    sums = g[["gc_ms", "run_ms", "mem_spill", "disk_spill", "input_bytes", "input_records", "output_bytes",
              "shuffle_read", "shuffle_write", "task_ms", "shuffle_read_records", "shuffle_write_records",
              "output_records"]].sum(min_count=1)
    agg = agg.join(sums.rename(columns={"gc_ms": "_gc_ms", "run_ms": "_run_ms", "task_ms": "_task_ms_sum"}))
    # p50 = lower median (Spark percentile_approx(., 0.5) on small data); Revision 11/13: the data each task read
    # (storage input + shuffle read) as bytes and rows, and task time: min, p10, median, p90, max, average
    agg = agg.join(task_dist(ok, STAGE_KEY))
    out = {}
    for key, row in agg.iterrows():
        ctx, sid, att = key
        d = {k: nn(v) for k, v in row.items()}
        for k in ("tasks", "failed_tasks", "max_task_ms", "max_peak_mem", "executors_used", "_gc_ms", "_run_ms",
                  "mem_spill", "disk_spill", "input_bytes", "input_records", "output_bytes", "shuffle_read",
                  "shuffle_write", "_task_ms_sum", "p50_task_ms", "min_task_ms", "shuffle_read_records",
                  "shuffle_write_records", "output_records", *DIST_COLS):
            d[k] = to_int(d.get(k))
        out[(ctx, int(sid), int(att))] = d
    return out


OPERATION_IN_TAG_RE = re.compile(r"_Operation_([0-9a-fA-F-]{8,})")


def _connect_link(j: dict, by_tag: dict, ops_in_ctx: set) -> str | None:
    """Connect operation id of a Spark job: its spark.job.tags contains the operation's jobTag (or a tag that
    embeds `_Operation_<id>`), or a spark.connect.*operation*id property."""
    ctx = j["spark_context_id"]
    for t in split_tags(j.get("job_tags")):
        hit = by_tag.get((ctx, t))
        if hit:
            return hit
        m = OPERATION_IN_TAG_RE.search(t)
        if m and (ctx, m.group(1)) in ops_in_ctx:
            return m.group(1)
    prop = j.get("_connect_op_prop")
    if prop:
        return prop
    for t in split_tags(j.get("job_tags")):
        m = OPERATION_IN_TAG_RE.search(t)
        if m:
            return m.group(1)
    return None


def build_spark_jobs(tables: EventTables) -> list[dict]:
    ends = {(e["spark_context_id"], e["spark_job_id"]): e for e in tables.job_end}
    by_tag = {(op["spark_context_id"], op["job_tag"]): op["operation_id"] for op in tables.connect_ops.values()
              if op.get("job_tag")}
    ops_in_ctx = set(tables.connect_ops)
    rows = []
    for j in tables.job_start:
        e = ends.get((j["spark_context_id"], j["spark_job_id"]), {})
        start, end = j["start_time"], e.get("end_time")
        rows.append({
            "cluster_id": j["cluster_id"], "spark_context_id": j["spark_context_id"], "spark_job_id": j["spark_job_id"],
            "start_time": start, "end_time": end,
            "duration_ms": (end - start) if start is not None and end is not None else None,
            "result": e.get("result"), "error": e.get("error"), "stage_ids": list(j["stage_ids"]),
            "num_stages": len(j["stage_ids"]), "sql_execution_id": j["sql_execution_id"],
            "description": j["description"], "job_group": j["job_group"], "call_site": j["call_site"],
            "databricks_job_id": j["databricks_job_id"], "databricks_run_id": j["databricks_run_id"],
            "notebook_path": j["notebook_path"], "job_tags": j.get("job_tags"),
            "databricks_task_run_id": j.get("databricks_task_run_id"),
            **{k: j.get(k) for k in ("databricks_parent_run_id", "databricks_job_name", "databricks_task_type")},
            "connect_operation_id": _connect_link(j, by_tag, ops_in_ctx),
        })
    rows.sort(key=lambda r: (r["spark_context_id"], r["spark_job_id"] if r["spark_job_id"] is not None else -1))
    return rows


_UUID_TAIL = re.compile(r"(?:^|[./])[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


def stage_metric_cols(m: dict) -> dict:
    """The stage columns that come from its tasks (`m`: one value of _stage_metrics), with skew, data skew and GC
    share. Also used to recompute the stages of an older output from its tasks (refresh)."""
    p50, mx = m.get("p50_task_ms"), m.get("max_task_ms")
    skew = round_half_up(mx / max(p50, 1), 1) if (mx is not None and p50 is not None) else None
    # data skew: the biggest task's input over the median task's (bytes, else rows); null when tasks read nothing
    data_skew = None
    for unit in ("bytes", "rows"):
        dmx, dmed = m.get(f"max_task_{unit}_in"), m.get(f"p50_task_{unit}_in")
        if dmx:
            data_skew = round_half_up(dmx / max(dmed or 0, 1), 1)
            break
    gc_share = None
    if m.get("_gc_ms") is not None and m.get("_run_ms"):
        gc_share = round_half_up(m["_gc_ms"] / m["_run_ms"], 3)
    return {
        "tasks": m.get("tasks"), "failed_tasks": m.get("failed_tasks"),
        "p50_task_ms": p50, "max_task_ms": mx, "min_task_ms": m.get("min_task_ms"), "skew": skew, "data_skew": data_skew,
        **{k: m.get(k) for k in ("shuffle_read_records", "shuffle_write_records", "output_records",
                                 *DIST_COLS) if k != "p50_task_ms"}, "gc_share": gc_share,
        "max_peak_mem": m.get("max_peak_mem"), "mem_spill": m.get("mem_spill"),
        "disk_spill": m.get("disk_spill"), "input_bytes": m.get("input_bytes"),
        "input_records": m.get("input_records"), "output_bytes": m.get("output_bytes"),
        "shuffle_read": m.get("shuffle_read"), "shuffle_write": m.get("shuffle_write"),
        "executors_used": m.get("executors_used"),
        # internal (not written): used by query_profile
        "_gc_ms": m.get("_gc_ms"), "_run_ms": m.get("_run_ms"), "_task_ms_sum": m.get("_task_ms_sum"),
    }


def restage(stages: list[dict], tdf: pd.DataFrame) -> int:
    """In place: the task-derived columns of these stage rows recomputed from the task frame (rows without tasks
    are left as they are), then the storage split. Returns how many stages were recomputed."""
    metrics = _stage_metrics(tdf)
    n = 0
    for s in stages:
        m = metrics.get((s.get("spark_context_id"), s.get("stage_id"), s.get("stage_attempt") or 0))
        if m:
            s.update(stage_metric_cols(m))
            n += 1
    add_storage_reads(stages)
    return n


def build_stages(tables: EventTables, tdf: pd.DataFrame, jobs: list[dict]) -> list[dict]:
    metrics = _stage_metrics(tdf)
    # stage -> lowest Spark job id listing it (one row per stage attempt)
    stage_job: dict[tuple, dict] = {}
    for j in sorted(jobs, key=lambda r: (r["spark_job_id"] is None, r["spark_job_id"] or 0)):
        for sid in j["stage_ids"]:
            stage_job.setdefault((j["spark_context_id"], sid), j)

    info: dict[tuple, dict] = {}
    for s in tables.stage_submitted:
        info.setdefault((s["spark_context_id"], s["stage_id"], s["stage_attempt"]), dict(s, _completed=False))
    for s in tables.stage_completed:
        k = (s["spark_context_id"], s["stage_id"], s["stage_attempt"])
        prev = info.get(k, {})
        merged = dict(prev)
        merged.update({kk: v for kk, v in s.items() if v is not None or kk not in prev})
        merged["_completed"] = True
        info[k] = merged

    rows = []
    for k, s in info.items():
        m = metrics.get(k, {})
        j = stage_job.get((k[0], k[1]))
        start, end = s.get("start_time"), s.get("end_time") if s.get("_completed") else None
        fr = s.get("failure_reason")
        status = "failed" if fr is not None else ("succeeded" if s.get("_completed") else "incomplete")
        rows.append({
            "cluster_id": s["cluster_id"], "spark_context_id": k[0], "stage_id": k[1], "stage_attempt": k[2],
            "stage_name": s.get("stage_name"), "num_tasks": s.get("num_tasks"), "status": status,
            "start_time": start, "end_time": end,
            "duration_ms": (end - start) if start is not None and end is not None else None,
            "failure_reason": fr, **stage_metric_cols(m),
            "spark_job_id": j["spark_job_id"] if j else None,
            "sql_execution_id": j["sql_execution_id"] if j else None,
            "job_description": j["description"] if j else None,
            # Revision 4: DAG + "what is this stage doing"
            "parent_ids": list(s.get("parent_ids") or []), "rdd_names": list(s.get("rdd_names") or []),
            "rdd_scopes": list(s.get("rdd_scopes") or []), "details": s.get("details"),
            "cloud_bytes": s.get("cloud_bytes"), "disk_cache_bytes": s.get("disk_cache_bytes"),
        })
    # stage retries: attempt N > 0 records why attempt N-1 failed (typically FetchFailed)
    by_key = {(r["spark_context_id"], r["stage_id"], r["stage_attempt"]): r for r in rows}
    for r in rows:
        r["retry_of_failure"] = None
        if r["stage_attempt"]:
            prev = by_key.get((r["spark_context_id"], r["stage_id"], r["stage_attempt"] - 1))
            if prev is not None:
                # the previous attempt's recorded failure reason, or null (it may have been resubmitted without failing)
                r["retry_of_failure"] = prev["failure_reason"] or None
    rows.sort(key=lambda r: (r["spark_context_id"], r["start_time"] is None, r["start_time"] or 0,
                             r["stage_id"], r["stage_attempt"]))
    add_storage_reads(rows)
    return rows


def add_storage_reads(stages: list[dict]) -> None:
    """In place, per stage: storage_bytes (read from cloud storage or the Databricks disk cache) and df_cache_bytes
    (the rest of its input: read from a DataFrame cache). Spark counts a cached-block read as task input, so the
    input alone overstates what came from storage.

    The cloud storage metric decides. A finished stage without it, in a Spark context whose other stages report it,
    read no files. Without the metric anywhere (older runtimes, open-source Spark) the input is all there is."""
    known = {s.get("spark_context_id") for s in stages
             if s.get("cloud_bytes") is not None or s.get("disk_cache_bytes") is not None}
    for s in stages:
        cloud, hits, inp = s.get("cloud_bytes"), s.get("disk_cache_bytes"), s.get("input_bytes")
        if cloud is not None or hits is not None:
            s["storage_bytes"] = (cloud or 0) + (hits or 0)
        elif s.get("spark_context_id") in known and s.get("status") in ("succeeded", "failed", "replanned"):
            s["storage_bytes"] = 0
        else:
            s["storage_bytes"] = inp
            s["df_cache_bytes"] = None
            continue
        s["df_cache_bytes"] = max(0, (inp or 0) - s["storage_bytes"]) if inp is not None else None


def storage_bytes(s: dict) -> int:
    """What a stage read from storage: storage_bytes when the output has it, else its input (older outputs)."""
    v = s.get("storage_bytes")
    return (v if v is not None else s.get("input_bytes")) or 0


def build_sql_queries(tables: EventTables, stages: list[dict]) -> list[dict]:
    ends = {(e["spark_context_id"], e["sql_execution_id"]): e for e in tables.sql_end}
    totals: dict[tuple, dict] = {}
    for s in stages:
        if s["sql_execution_id"] is None:
            continue
        t = totals.setdefault((s["spark_context_id"], s["sql_execution_id"]),
                              {"stages": 0, "tasks": None, "disk_spill": None, "max_stage_skew": None,
                               "input_bytes": None, "shuffle_read": None})
        t["stages"] += 1
        for c in ("tasks", "disk_spill", "input_bytes", "shuffle_read"):
            if s[c] is not None:
                t[c] = (t[c] or 0) + s[c]
        if s["skew"] is not None:
            t["max_stage_skew"] = s["skew"] if t["max_stage_skew"] is None else max(t["max_stage_skew"], s["skew"])
    rows = []
    seen = set()
    for q in tables.sql_start:
        k = (q["spark_context_id"], q["sql_execution_id"])
        if k in seen:
            continue
        seen.add(k)
        e = ends.get(k, {})
        err = e.get("error")
        err = err if err else None
        final = tables.sql_aqe.get(k) or q["initial_plan"]
        start, end = q["start_time"], e.get("end_time")
        t = totals.get(k, {})
        rows.append({
            "cluster_id": q["cluster_id"], "spark_context_id": k[0], "sql_execution_id": k[1],
            "start_time": start, "end_time": end,
            "duration_ms": (end - start) if start is not None and end is not None else None,
            "status": "failed" if err else ("succeeded" if e else "incomplete"),
            "description": q["description"], "details": q["details"], "error": err,
            "stages": t.get("stages"), "tasks": t.get("tasks"), "disk_spill": t.get("disk_spill"),
            "max_stage_skew": t.get("max_stage_skew"), "input_bytes": t.get("input_bytes"),
            "shuffle_read": t.get("shuffle_read"), "plan_hash": plan_hash(final), "final_plan": final,
            "initial_plan": q["initial_plan"], "root_execution_id": q.get("root_execution_id"),
            **plan_summary(final, q["initial_plan"]),
        })
    # tables written by storage path: name them from the plans that name them (anywhere on the cluster)
    names = table_names(p for r in rows for p in (r["final_plan"], r["initial_plan"]))
    for r in rows:
        r["photon_share"] = photon_share(r.get("operators"))
        r["tables_read"] = name_tables(r["tables_read"], names)
        # a managed table's storage folder that no plan names (a bare id, as a CTAS writes it) is not a name a
        # reader knows: left out
        r["tables_written"] = [x for x in name_tables(r["tables_written"], names) if not _UUID_TAIL.search(str(x))]
    rows.sort(key=lambda r: (r["spark_context_id"], r["sql_execution_id"] if r["sql_execution_id"] is not None else -1))
    return rows


def photon_share(ops) -> float | None:
    """Revision 13: share of the plan's operators that ran in Photon (null: no plan); below 1 = some fell back."""
    ops = [str(o) for o in (ops or []) if o and not str(o).startswith(("AdaptiveSparkPlan", "ResultQueryStage",
                                                                         "ShuffleQueryStage", "BroadcastQueryStage",
                                                                         "WholeStageCodegen", "InputAdapter"))]
    if not ops:
        return None
    return round(sum(1 for o in ops if o.startswith("Photon")) / len(ops), 3)


def build_executors(tables: EventTables, cluster_id: str, rules: Rules | None = None) -> list[dict]:
    rows: dict[tuple, dict] = {}
    for a in tables.exec_added:
        k = (a["spark_context_id"], a["executor_id"])
        if k not in rows:
            rows[k] = {"cluster_id": cluster_id, "spark_context_id": k[0], "executor_id": k[1], "host": a["host"],
                       "cores": a["cores"], "added_time": a["added_time"], "removed_time": None,
                       "removed_reason": None, "removed_reason_raw": None,
                       "resource_profile_id": a.get("resource_profile_id")}
    for r in tables.exec_removed:
        k = (r["spark_context_id"], r["executor_id"])
        row = rows.setdefault(k, {"cluster_id": cluster_id, "spark_context_id": k[0], "executor_id": k[1],
                                  "host": None, "cores": None, "added_time": None})
        row["removed_time"] = r["removed_time"]
        row["removed_reason"] = r["removed_reason"]
        row["removed_reason_raw"] = r.get("removed_reason_raw")
    for row in rows.values():
        row["removal_category"] = rules.removal_category(row["removed_reason"]) if rules else None
        row.update(executor_resources(tables, row))
    out = list(rows.values())
    out.sort(key=lambda r: (r["added_time"] is None, r["added_time"] or 0, r["spark_context_id"],
                            _num(r["executor_id"])))
    return out


def executor_resources(tables: EventTables, row: dict) -> dict:
    """Revision 13: what the executor was given. Heap, overhead and off-heap come from its resource profile, else
    the Spark properties; unified memory (execution + storage, what Spark can use for tasks and cache) from its block
    manager; storage memory is the part of it kept for cached data (spark.memory.storageFraction, default 0.5)."""
    ctx = row["spark_context_id"]
    conf = tables.exec_conf.get(ctx, {})
    rp = tables.resource_profiles.get((ctx, row.get("resource_profile_id") or 0), {})
    bm = tables.bm_added.get((ctx, row["executor_id"]), {})
    off_on = str(conf.get("spark.memory.offHeap.enabled", "")).lower() == "true"
    heap = rp.get("memory_mb") or _mib(conf.get("spark.executor.memory"))
    over = rp.get("overhead_mb") or _mib(conf.get("spark.executor.memoryOverhead"))
    off = rp.get("offheap_mb") or (_mib(conf.get("spark.memory.offHeap.size"), "b") if off_on else None)
    unified = bm.get("max_mem")
    try:
        sf = float(conf.get("spark.memory.storageFraction", 0.5))
    except (TypeError, ValueError):
        sf = 0.5
    try:
        tc = rp.get("task_cpus") or float(conf.get("spark.task.cpus", 1))
    except (TypeError, ValueError):
        tc = 1
    if row.get("cores") is None and rp.get("cores"):
        row["cores"] = rp["cores"]
    return {"heap_mb": heap, "overhead_mb": over, "offheap_mb": off or None, "unified_memory": unified,
            "storage_memory": int(unified * sf) if unified else None, "task_cpus": to_int(tc) or 1}


def attach_task_data(tdf: pd.DataFrame, stages: list[dict], jobs: list[dict], queries: list[dict]) -> None:
    """Revision 13: per Spark job and per SQL query, data in/out totals and how the data was spread over tasks."""
    if tdf.empty:
        for r in jobs + queries:
            r.update({c: None for c in (*DIST_COLS, *IO_COLS) if c not in r})
        return
    sj = {(s["spark_context_id"], s["stage_id"]): (s["spark_job_id"], s["sql_execution_id"]) for s in stages}
    keys = [sj.get((c, None if pd.isna(s) else int(s)), (None, None))
            for c, s in zip(tdf["spark_context_id"], tdf["stage_id"])]
    t = tdf.assign(_job=pd.array([k[0] for k in keys], dtype="Int64"), _sql=pd.array([k[1] for k in keys], dtype="Int64"))
    for rows, col, idc in ((jobs, "_job", "spark_job_id"), (queries, "_sql", "sql_execution_id")):
        by = dist_rows(t, ["spark_context_id", col], IO_COLS)
        for r in rows:
            got = by.get((r["spark_context_id"], r[idc]), {})
            for c in (*DIST_COLS, *IO_COLS):
                if r.get(c) is None:
                    r[c] = got.get(c)


def _num(x):
    return (0, int(x), "") if isinstance(x, str) and x.isdigit() else (1, 0, x or "")


def build_apps(tables: EventTables, tdf: pd.DataFrame, stages, jobs, queries, executors) -> list[dict]:
    """One row per spark context; start/end fall back to the earliest/latest event time when the
    ApplicationStart/End events are missing (e.g. a context still running)."""
    lo: dict[str, int] = {}
    hi: dict[str, int] = {}

    def see(ctx, *vals):
        for v in vals:
            if v is None or (isinstance(v, float) and math.isnan(v)):
                continue
            v = int(v)
            lo[ctx] = min(lo.get(ctx, v), v)
            hi[ctx] = max(hi.get(ctx, v), v)

    for r in jobs:
        see(r["spark_context_id"], r["start_time"], r["end_time"])
    for r in stages:
        see(r["spark_context_id"], r["start_time"], r["end_time"])
    for r in queries:
        see(r["spark_context_id"], r["start_time"], r["end_time"])
    for r in executors:
        see(r["spark_context_id"], r["added_time"], r["removed_time"])
    if not tdf.empty:
        g = tdf.groupby("spark_context_id")
        for ctx, v in g["launch_time"].min().items():
            see(ctx, v)
        for ctx, v in g["finish_time"].max().items():
            see(ctx, v)
    rows = []
    for a in tables.apps:
        ctx = a["spark_context_id"]
        start = a["start_time"] if a["start_time"] is not None else lo.get(ctx)
        end = a["end_time"] if a["end_time"] is not None else hi.get(ctx)
        rows.append({**a, "start_time": start, "end_time": end,
                     "duration_ms": (end - start) if start is not None and end is not None else None,
                     "_app_end_seen": a["end_time"] is not None})
    rows.sort(key=lambda r: (r["start_time"] is None, r["start_time"] or 0, r["spark_context_id"]))
    return rows


def build_sql_plan_nodes(tables: EventTables) -> list[dict]:
    """Revision 7: one row per operator of each query's latest plan, with its SQL metric values."""
    out: list[dict] = []
    plans = dict(tables.sql_plans)
    # a missing or childless sparkPlanInfo (some runtimes log only the AdaptiveSparkPlan root): read the operators
    # from the final plan text instead, without metrics
    for q in tables.sql_start:
        k = (q["spark_context_id"], q["sql_execution_id"])
        if len(plans.get(k) or []) <= 1:
            parsed = plan_from_text(tables.sql_aqe.get(k) or q["initial_plan"])
            if len(parsed) > 1:
                plans[k] = parsed
    # metrics of operators adaptive execution added later: on the plan's root, so the query's totals are complete
    for k, ms in tables.sql_aqe_metrics.items():
        if plans.get(k):
            root = dict(plans[k][0])
            have = {m[1] for n in plans[k] for m in n.get("metrics") or []}
            root["metrics"] = list(root.get("metrics") or []) + [m for m in ms if m[1] not in have]
            plans[k] = [root] + list(plans[k][1:])
    for (ctx, exec_id), nodes in sorted(plans.items(), key=lambda kv: (str(kv[0][0]), kv[0][1] if kv[0][1] is not None else -1)):
        out += plan_node_rows(tables.cluster_id, ctx, exec_id, nodes, tables.acc_task, tables.acc_stage,
                              tables.acc_driver)
    return out


def build_connect_operations(tables: EventTables, jobs: list[dict]) -> list[dict]:
    n_jobs = Counter((j["spark_context_id"], j["connect_operation_id"]) for j in jobs if j["connect_operation_id"])
    rows = []
    for (ctx, op_id), op in tables.connect_ops.items():
        sess = tables.connect_sessions.get((ctx, op.get("session_id")), {})
        status = ("failed" if op["_failed"] else "canceled" if op["_canceled"] else
                  "finished" if op["_finished"] else "open")
        start, end = op["start_time"], op["finish_time"]
        row = {k: v for k, v in op.items() if not k.startswith("_")}
        row.update(user_id=op["user_id"] or sess.get("user_id"), user_name=op["user_name"] or sess.get("user_name"),
                   status=status, duration_ms=(end - start) if start is not None and end is not None else None,
                   spark_jobs=n_jobs.get((ctx, op_id), 0))
        rows.append(row)
    rows.sort(key=lambda r: (r["spark_context_id"], r["start_time"] is None, r["start_time"] or 0,
                             r["operation_id"]))
    return rows


CLUSTER_INFO_COLS = ["cluster_name", "cluster_creator", "spark_version", "driver_node_type", "worker_node_type",
                     "min_workers", "max_workers", "target_workers", "cluster_scaling_type", "runtime_engine",
                     "cloud_provider", "region", "workload_type", "databricks_job_id", "job_run_id", "task_run_id",
                     "parent_run_id"]


def build_cluster_info(tables: EventTables, apps: list[dict]) -> list[dict]:
    """One row per spark context that carried spark.databricks.clusterUsageTags.* / job properties."""
    rows = []
    order = [a["spark_context_id"] for a in apps] + sorted(set(tables.cluster_info) - {a["spark_context_id"]
                                                                                        for a in apps})
    for ctx in order:
        info = tables.cluster_info.get(ctx)
        if not info:
            continue
        rows.append({"cluster_id": tables.cluster_id, "spark_context_id": ctx,
                     **{c: info.get(c) for c in CLUSTER_INFO_COLS}})
    return rows


def build_settings(tables: EventTables) -> list[dict]:
    """Revision 14: one row per context and setting in the catalogue: the cluster's value, the values set in code,
    and the default when neither was seen."""
    from ..settings_catalog import SETTINGS

    rows = []
    ctxs = sorted(set(tables.settings) | set(tables.cluster_info)) or []
    for ctx in ctxs:
        got = tables.settings.get(ctx, {})
        for key, group, default, what in SETTINGS:
            g = got.get(key)
            cluster = g["cluster"] if g else None
            session = sorted(g["session"]) if g else []
            if cluster is None and not session and default is None:
                continue
            rows.append({"cluster_id": tables.cluster_id, "spark_context_id": ctx, "key": key, "group": group,
                         "value": session[-1] if session and cluster is None and len(session) == 1 else cluster,
                         "cluster_value": cluster, "session_values": session, "default": default,
                         "source": "code" if session else "cluster" if cluster is not None else "default",
                         "what": what})
    return rows


def build_event_counts(tables: EventTables) -> list[dict]:
    rows = [{"cluster_id": tables.cluster_id, "spark_context_id": ctx, "event_type": ev, "count": n}
            for (ctx, ev), n in tables.event_counts.items()]
    rows.sort(key=lambda r: (r["spark_context_id"], -r["count"], r["event_type"]))
    return rows


def mark_replanned(jobs: list[dict], stages: list[dict], rules: Rules) -> None:
    """Jobs and stages that adaptive query execution cancelled after re-planning the query are not failures:
    job result JobReplanned, stage status "replanned" (the message stays in error / failure_reason)."""
    if not rules.replanned_regex:
        return
    rx = re.compile(rules.replanned_regex)
    for j in jobs:
        if j.get("result") == "JobFailed" and j.get("error") and rx.search(j["error"]):
            j["result"] = "JobReplanned"
    for s in stages:
        if s.get("status") == "failed" and s.get("failure_reason") and rx.search(s["failure_reason"]):
            s["status"] = "replanned"


def written_by_root(queries: list[dict]) -> None:
    """A query run for another (a CREATE OR REPLACE TABLE AS SELECT writes in a child query) often knows its table
    only by its storage path, while the root query names it: take the root's name."""
    by_id = {(q["spark_context_id"], q["sql_execution_id"]): q for q in queries}
    for q in queries:
        rid = q.get("root_execution_id")
        tw = q.get("tables_written") or []
        if rid is None or rid == q["sql_execution_id"] or not any("/" in t for t in tw):
            continue
        root = by_id.get((q["spark_context_id"], rid)) or {}
        named = [t for t in root.get("tables_written") or [] if "/" not in t]
        if named:
            q["tables_written"] = list(dict.fromkeys([t for t in tw if "/" not in t] + named))


def name_by_code(tables: EventTables, jobs: list[dict], stages: list[dict], queries: list[dict]) -> None:
    """Spark Connect names jobs, stages and queries by its session ("Spark Connect - session_id ..."): name them by
    the code instead, from the statement of the Connect operation that started them."""
    label = {}
    for (ctx, op_id), op in tables.connect_ops.items():
        lb = statement_label(op.get("statement_text"))
        if lb:
            label[(ctx, op_id)] = lb
    if not label:
        return
    by_job, by_query = {}, {}
    for j in jobs:
        lb = label.get((j["spark_context_id"], j.get("connect_operation_id")))
        if lb is None:
            continue
        if j.get("description") is None or is_connect_blob(j["description"]):
            j["description"] = lb
        by_job[(j["spark_context_id"], j["spark_job_id"])] = lb
        if j.get("sql_execution_id") is not None:
            by_query.setdefault((j["spark_context_id"], j["sql_execution_id"]), lb)
    for s in stages:
        lb = by_job.get((s["spark_context_id"], s.get("spark_job_id")))
        if lb and (s.get("job_description") is None or is_connect_blob(s["job_description"])):
            s["job_description"] = lb
    for q in queries:
        lb = by_query.get((q["spark_context_id"], q["sql_execution_id"]))
        if lb and (q.get("description") is None or is_connect_blob(q["description"])):
            q["description"] = lb


def build_event_datasets(tables: EventTables, rules: Rules) -> dict:
    from .retries import build_task_retries

    tdf = tasks_frame(tables)
    jobs = build_spark_jobs(tables)
    stages = build_stages(tables, tdf, jobs)
    mark_replanned(jobs, stages, rules)
    queries = build_sql_queries(tables, stages)
    name_by_code(tables, jobs, stages, queries)
    written_by_root(queries)
    attach_task_data(tdf, stages, jobs, queries)
    executors = build_executors(tables, tables.cluster_id, rules)
    apps = build_apps(tables, tdf, stages, jobs, queries, executors)
    return {"tasks": tdf, "stages": stages, "spark_jobs": jobs, "sql_queries": queries, "executors": executors,
            "apps": apps, "connect_operations": build_connect_operations(tables, jobs),
            "sql_plan_nodes": build_sql_plan_nodes(tables),
            "cluster_info": build_cluster_info(tables, apps), "settings": build_settings(tables),
            "event_counts": build_event_counts(tables),
            "task_retries": build_task_retries(tdf, stages, executors, rules, tables.cluster_id)}
