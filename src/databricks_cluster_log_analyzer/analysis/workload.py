"""What the code asked for: DataFrame caches, queries that only count, DDL loops, and where reads came from.

  dataframe_cache   per Spark context: queries read a DataFrame cache (cache() / persist()) bigger than the
                    executors' storage memory, or whose blocks did not fit, were dropped or were lost with an
                    executor; and whether it was ever released (UnpersistRDD).
  count_only        per run: queries that only counted rows (an ungrouped count, no write) took a large share of it.
  ddl_loop          per run: the same ALTER statement ran many times, one Delta commit each.
  disk_cache        per Spark context: the Databricks disk cache wrote much more than it served.

read_split() adds per query: storage_read (files), cache_read (a DataFrame cache) and the disk cache numbers.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping

from ..config import Rules
from ..util import fmt_bytes, fmt_words
from .contention import _run_name, to_ms

GB = 1 << 30

FIX = {
    "dataframe_cache": (
        "Do not cache a table that is many times the storage memory: cache only the narrow columns that are reused, "
        "or write an intermediate Delta table and read it back. Unpersist when done, and drop count() calls that "
        "exist only to fill the cache or to log."),
    "count_only": (
        "Drop count() calls that only log or check: each one runs the whole query again. When you need the number, "
        "take it from the write (the Delta commit's numOutputRows) or count the written table."),
    "ddl_loop": (
        "Set the comments or properties in one statement (or in the CREATE TABLE): each ALTER is a separate Delta "
        "commit, and every tenth one writes a checkpoint."),
    "disk_cache": (
        "The disk cache wrote far more than it served: this job reads each file about once. Turn it off for the job "
        "(spark.databricks.io.cache.enabled false) so the local disks keep their room for spill and shuffle."),
}

_CACHE_RE = re.compile(r"InMemoryRelation|InMemoryTableScan|TableCacheQueryStage")
_CACHE_SIZE_RE = re.compile(r"TableCacheQueryStage[^\n]*?sizeInBytes=([\d.]+)\s*([KMGTPE]i?B|B)")
_UNIT = {"B": 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40, "PiB": 1 << 50, "EiB": 1 << 60,
         "KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30, "TB": 1 << 40, "PB": 1 << 50, "EB": 1 << 60}
_WRITE_RE = re.compile(r"Insert|Write(?!Files)|Save|AppendData|Overwrite|CreateTable|CreateDelta|ReplaceTable|"
                       r"TableAsSelect|MergeInto|DeleteFrom|UpdateTable|Merge(?:Into)?Command")
_COMPUTED_RE = re.compile(r"computed ([\d.]+)\s*([KMGTPE]i?B|B) so far")
_DDL_RE = re.compile(r"(?is)^\s*(ALTER\s+TABLE|COMMENT\s+ON|ALTER\s+VIEW)\b")


def _f(cid, ctx, severity, category, entity, evidence, ts, **links) -> dict:
    row = {"cluster_id": cid, "spark_context_id": ctx, "severity": severity, "category": category, "entity": entity,
           "evidence": evidence, "fix": FIX[category], "ts": ts, "stage_id": None, "stage_attempt": None,
           "spark_job_id": None, "sql_execution_id": None, "executor_id": None, "signal": None, "fingerprint": None,
           "log_file_path": None, "log_seq": None, "run_key": None}
    row.update(links)
    return row


def _plan(q: Mapping) -> str:
    return q.get("final_plan") or q.get("initial_plan") or ""


def cache_size(plan: str) -> int:
    """The largest cached relation adaptive execution measured in this plan (TableCacheQueryStage statistics)."""
    return int(max((float(v) * _UNIT.get(u, 1) for v, u in _CACHE_SIZE_RE.findall(plan or "")), default=0))


def count_only(plan: str | None) -> bool:
    """The query returns one row from an ungrouped count and writes nothing: a df.count()."""
    if not plan or _WRITE_RE.search(plan.split("\n\n", 1)[0]):
        return False
    # the first aggregate in the details is the final one: no keys, only count functions
    m = re.search(r"\(\d+\) (?:Photon)?(?:Hash|Sort|Object)?Aggregate[^\n]*\n(?:(?!\(\d+\) ).*\n){0,6}?Keys(?: \[0\])?: \[\]\n"
                  r"Functions \[\d+\]: \[([^\n]*)\]", plan)
    if m:
        return all(f.strip().startswith(("count(", "partial_count(", "merge_count(")) for f in re.split(r",\s*(?![^()]*\))", m.group(1)))
    # simple explain: HashAggregate(keys=[], functions=[count(1)])
    m = re.search(r"Aggregate\(keys=\[\], functions=\[([^\]]*)\]", plan)
    return bool(m) and all(f.strip().startswith(("count(", "partial_count(", "finalmerge_count(", "merge_count("))
                           for f in re.split(r",\s*(?![^()]*\))", m.group(1)))


def ddl_shape(text: str | None) -> str | None:
    """An ALTER statement with its names and literals taken out: the same shape repeated is a loop."""
    if not text or not _DDL_RE.match(text):
        return None
    s = re.sub(r"`[^`]*`|'(?:[^'\\]|\\.)*'|\"[^\"]*\"", "?", text.strip())
    s = re.sub(r"(?i)\b(TABLE|COLUMN|VIEW|ON)\s+[\w.?]+", r"\1 ?", s)
    s = re.sub(r"\s+", " ", s).upper()
    return s[:80]


def read_split(queries: list[dict], stages: list[dict], plan_nodes: list[dict]) -> None:
    """Per query, in place: storage_read (from files: the cloud storage metric when the scans report it, else the
    input of stages that do not read a DataFrame cache), cache_read (input from a DataFrame cache), and the Databricks
    disk cache hits, misses and writes."""
    metric = {}
    for n in plan_nodes or []:
        mj = n.get("metrics_json")
        if not mj:
            continue
        try:
            ms = json.loads(mj)
        except ValueError:
            continue
        k = (n["spark_context_id"], n["sql_execution_id"])
        d = metric.setdefault(k, {})
        for m in ms if isinstance(ms, list) else []:
            name = m.get("name")
            if name in ("cloud storage response size", "cache hits size", "cache misses size", "cache writes size"):
                d[name] = d.get(name, 0) + (m.get("total") or 0)
    by_q: dict[tuple, list[dict]] = {}
    for s in stages:
        if s.get("sql_execution_id") is not None and s.get("status") != "failed":
            by_q.setdefault((s["spark_context_id"], s["sql_execution_id"]), []).append(s)
    for q in queries:
        k = (q["spark_context_id"], q["sql_execution_id"])
        ss = by_q.get(k, [])
        cached = sum(s.get("input_bytes") or 0 for s in ss if "InMemoryTableScan" in "|".join(s.get("rdd_scopes") or []))
        files = sum(s.get("input_bytes") or 0 for s in ss) - cached
        m = metric.get(k, {})
        q["storage_read"] = int(m["cloud storage response size"]) if m.get("cloud storage response size") else (files if ss else None)
        q["cache_read"] = cached if ss else None
        q["disk_cache_hit"] = m.get("cache hits size")
        q["disk_cache_miss"] = m.get("cache misses size")
        q["disk_cache_write"] = m.get("cache writes size")


def workload_findings(cid: str, queries: list[dict], runs: list[dict], executors: list[dict],
                      log_signals: list[dict], event_counts: list[dict], rules: Rules) -> list[dict]:
    out: list[dict] = []
    runs_by = {r["run_key"]: r for r in runs}

    # ---- DataFrame cache ------------------------------------------------------------------------------------------
    by_ctx: dict = {}
    for q in queries:
        if _CACHE_RE.search(_plan(q)):
            by_ctx.setdefault(q["spark_context_id"], []).append(q)
    unpersist = {}
    for e in event_counts or []:
        if str(e.get("event_type") or e.get("event") or "").endswith("UnpersistRDD"):
            unpersist[e["spark_context_id"]] = unpersist.get(e["spark_context_id"], 0) + (e.get("count") or 0)
    sig = {}
    for r in log_signals:
        if r.get("signal") in ("cache_not_fit", "cache_dropped", "cache_lost"):
            sig.setdefault(r["signal"], []).append(r)
    for ctx, qs in by_ctx.items():
        sizes = [(cache_size(_plan(q)), q) for q in qs]
        big, bq = max(sizes, key=lambda x: x[0])
        ex = [e for e in executors if e.get("spark_context_id") == ctx and e.get("executor_id") not in (None, "driver")]
        per = max((e.get("storage_memory") or 0 for e in ex), default=0)
        alive = _max_alive(ex)
        cap = per * alive
        not_fit = len(sig.get("cache_not_fit", []))
        dropped = len(sig.get("cache_dropped", []))
        lost = len(sig.get("cache_lost", []))
        computed = max((float(v) * _UNIT.get(u, 1) for r in sig.get("cache_not_fit", [])
                        for v, u in _COMPUTED_RE.findall(r.get("line") or "")), default=0)
        released = unpersist.get(ctx, 0)
        too_big = cap and big > cap
        if not (too_big or not_fit or lost or dropped):
            continue
        ev = f"{len(qs)} quer{'ies' if len(qs) != 1 else 'y'} read a DataFrame cache (cache() or persist())"
        if big:
            ev += f"; the largest is {fmt_bytes(big)} (query {bq['sql_execution_id']})"
            if cap:
                ev += f", {big / cap:.1f}× the {fmt_bytes(cap)} of storage memory ({fmt_bytes(per)} on each of {alive} executors)"
        elif cap:
            ev += f"; the executors had {fmt_bytes(cap)} of storage memory ({fmt_bytes(per)} each)"
        parts = []
        if not_fit:
            parts.append(f"{not_fit:,} blocks did not fit in memory" + (f" (one reached {fmt_bytes(computed)})" if computed else ""))
        if dropped:
            parts.append(f"{dropped:,} were dropped from memory")
        if lost:
            parts.append(f"{lost:,} were lost when executors went away and had to be rebuilt")
        if parts:
            ev += ". " + "; ".join(parts)[0].upper() + "; ".join(parts)[1:]
        ev += ". " + ("It was never released (no unpersist)." if not released else f"{released} unpersist call{'s' if released != 1 else ''}.")
        sev = "high" if (cap and big > 2 * cap) or lost >= 100 or not_fit >= 100 else "medium"
        out.append(_f(cid, ctx, sev, "dataframe_cache", f"query {bq['sql_execution_id']}: DataFrame cache", ev,
                      to_ms(bq.get("start_time")), sql_execution_id=bq["sql_execution_id"], run_key=bq.get("run_key")))

    # ---- per run: counts and DDL loops -----------------------------------------------------------------------------
    per_run: dict = {}
    for q in queries:
        if q.get("run_key"):
            per_run.setdefault(q["run_key"], []).append(q)
    for rk, qs in per_run.items():
        r = runs_by.get(rk) or {}
        dur = r.get("duration_ms") or 0
        counts = [q for q in qs if (q.get("duration_ms") or 0) > 0 and count_only(_plan(q))]
        tot = sum(q["duration_ms"] for q in counts)
        if counts and tot >= 60_000 and (not dur or tot >= 0.1 * dur):
            counts.sort(key=lambda q: -q["duration_ms"])
            cached = sum(1 for q in counts if _CACHE_RE.search(_plan(q)))
            ev = (f"{len(counts)} quer{'ies' if len(counts) != 1 else 'y'} only counted rows: {fmt_words(tot)}"
                  + (f" of its {fmt_words(dur)} ({tot / dur:.0%})" if dur else "") + ". "
                  + ", ".join(f"query {q['sql_execution_id']} {fmt_words(q['duration_ms'])}" for q in counts[:4])
                  + (f"; {cached} of them filled a DataFrame cache" if cached else ""))
            out.append(_f(cid, counts[0]["spark_context_id"], "high" if dur and tot >= 0.3 * dur else "medium",
                          "count_only", f"run {_run_name(r) if r else rk}", ev, to_ms(counts[0].get("start_time")),
                          run_key=rk, sql_execution_id=counts[0]["sql_execution_id"]))
        shapes: dict = {}
        for q in qs:
            sh = ddl_shape(q.get("description"))
            if sh:
                shapes.setdefault(sh, []).append(q)
        for sh, ds in shapes.items():
            if len(ds) <= 10:
                continue
            t = sum(q.get("duration_ms") or 0 for q in ds)
            ev = f"{len(ds)} statements of the same shape ({sh[:60]}) took {fmt_words(t)}, one Delta commit each"
            out.append(_f(cid, ds[0]["spark_context_id"], "medium" if t >= 60_000 else "low", "ddl_loop",
                          f"run {_run_name(r) if r else rk}: {len(ds)} ALTER statements", ev, to_ms(ds[0].get("start_time")),
                          run_key=rk, sql_execution_id=ds[0]["sql_execution_id"]))

    # ---- Databricks disk cache ---------------------------------------------------------------------------------
    dc: dict = {}
    for q in queries:
        d = dc.setdefault(q["spark_context_id"], [0, 0, 0])
        d[0] += q.get("disk_cache_hit") or 0
        d[1] += q.get("disk_cache_miss") or 0
        d[2] += q.get("disk_cache_write") or 0
    for ctx, (hit, miss, wr) in dc.items():
        if hit + miss and wr >= 10 * GB and hit / (hit + miss) < 0.1:
            ev = (f"The Databricks disk cache wrote {fmt_bytes(wr)} and served {fmt_bytes(hit)} "
                  f"({hit / (hit + miss):.1%} of the reads): the files were read about once")
            out.append(_f(cid, ctx, "low", "disk_cache", "Databricks disk cache", ev, None))
    return out


def _max_alive(ex: list[dict]) -> int:
    ev = []
    for e in ex:
        a, b = to_ms(e.get("added_time")), to_ms(e.get("removed_time"))
        if a is not None:
            ev.append((a, 1))
            if b is not None:
                ev.append((b, -1))
    n = best = 0
    for _, d in sorted(ev, key=lambda x: (x[0], -x[1])):
        n += d
        best = max(best, n)
    return best or len(ex)


CATEGORIES = ("dataframe_cache", "count_only", "ddl_loop", "disk_cache")
