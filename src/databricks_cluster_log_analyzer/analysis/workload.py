"""What the code asked for: DataFrame caches, queries that only count, DDL loops, and where reads came from.

  dataframe_cache   per Spark context: queries read a DataFrame cache (cache() / persist()) bigger than the
                    executors' storage memory, or whose blocks did not fit, were dropped or were lost with an
                    executor; and whether it was ever released (UnpersistRDD).
  count_only        per run: queries that only counted rows (an ungrouped count, no write) took a large share of it.
  ddl_loop          per run: the same ALTER statement ran many times, one Delta commit each.
  disk_cache        per Spark context: the Databricks disk cache wrote much more than it served.
  no_spark_work     per run (and per Spark app): most of its time no stage ran, so the time went to the driver
                    (statements that run no tasks, Python, waiting), which more cores cannot shorten.
  tiny_task_stages  per Spark app: many stages cut almost no data into the same number of tasks (Delta's 50 log
                    partitions, a shuffle partition count set for big data), so task overhead is most of their time.

read_split() adds per query: storage_read (files), cache_read (a DataFrame cache) and the disk cache numbers.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping

from ..config import Rules
from ..util import fmt_bytes, fmt_words, is_worker
from .aggregate import storage_bytes
from .contention import _run_name, to_ms

GB = 1 << 30

FIX = {
    "dedup_noop": (
        "The window dedup (row_number() per key, keep the first) removed no rows: the keys were already unique. Drop "
        "it for this load, or dedup only the new rows (a MERGE or an anti-join against the keys already loaded)."),
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
    "no_spark_work": (
        "This time is on the driver, not in Spark tasks: more cores or workers will not shorten it. Look at what ran "
        "between and inside the statements: DDL and table re-creation (CREATE, REPLACE, DROP: each one a Delta "
        "commit), Delta log reads, collect() and Python code, sleeps and waits on other systems. Do them once instead "
        "of per table or per test, or keep the tables between runs."),
    "tiny_task_stages": (
        "Give these stages fewer partitions: lower spark.sql.shuffle.partitions (or let adaptive execution coalesce "
        "them), or coalesce() small DataFrames before the action. Each task costs a few milliseconds of scheduling "
        "even when it has nothing to do."),
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
    """Per query, in place: storage_read (from cloud storage and the disk cache: the stages' storage_bytes),
    cache_read (input from a DataFrame cache: their df_cache_bytes), and the Databricks disk cache hits, misses and
    writes. Stages built before storage_bytes existed count their whole input as storage."""
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
            if name in ("cache hits size", "cache misses size", "cache writes size"):
                d[name] = d.get(name, 0) + (m.get("total") or 0)
    by_q: dict[tuple, list[dict]] = {}
    for s in stages:
        if s.get("sql_execution_id") is not None and s.get("status") != "failed":
            by_q.setdefault((s["spark_context_id"], s["sql_execution_id"]), []).append(s)
    for q in queries:
        k = (q["spark_context_id"], q["sql_execution_id"])
        ss = by_q.get(k, [])
        m = metric.get(k, {})
        q["storage_read"] = sum(storage_bytes(s) for s in ss) if ss else None
        cached = [s.get("df_cache_bytes") for s in ss if s.get("df_cache_bytes") is not None]
        q["cache_read"] = sum(cached) if cached else None  # unknown without the cloud storage metric
        # disk cache hits: from the stages that ran (a plan can repeat the same scan), else the plan's metric
        hits = [s.get("disk_cache_bytes") for s in ss if s.get("disk_cache_bytes") is not None]
        q["disk_cache_hit"] = sum(hits) if hits else m.get("cache hits size")
        q["disk_cache_miss"] = m.get("cache misses size")
        q["disk_cache_write"] = m.get("cache writes size")


def _spark_time(stages: list[dict] | None) -> dict[tuple, float]:
    """Per (ctx, query): the wall time with at least one of its stages running (the union of their spans). A query can
    sit open for hours (a stream, a lock, the driver) while its Spark work takes a second."""
    spans: dict[tuple, list] = {}
    for s in stages or []:
        a, z = to_ms(s.get("start_time")), to_ms(s.get("end_time"))
        if s.get("sql_execution_id") is not None and a is not None and z is not None and z > a:
            spans.setdefault((s["spark_context_id"], s["sql_execution_id"]), []).append((a, z))
    out = {}
    for k, iv in spans.items():
        iv.sort()
        tot, s0, e0 = 0, iv[0][0], iv[0][1]
        for a, z in iv[1:]:
            if a > e0:
                tot += e0 - s0
                s0, e0 = a, z
            else:
                e0 = max(e0, z)
        out[k] = tot + e0 - s0
    return out


def workload_findings(cid: str, queries: list[dict], runs: list[dict], executors: list[dict],
                      log_signals: list[dict], event_counts: list[dict], rules: Rules,
                      stages: list[dict] | None = None) -> list[dict]:
    out: list[dict] = []
    runs_by = {r["run_key"]: r for r in runs}
    busy = _spark_time(stages)

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
        ex = [e for e in executors if e.get("spark_context_id") == ctx and is_worker(e, executors)]
        # a cache can take all of the unified memory (Spark's MemoryStore capacity) while tasks do not need it, not only
        # the storage fraction of it
        per = max((e.get("unified_memory") or e.get("storage_memory") or 0 for e in ex), default=0)
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
                ev += f", {big / cap:.1f}× the {fmt_bytes(cap)} of memory a cache can use ({fmt_bytes(per)} on each of {alive} executors)"
        elif cap:
            ev += f"; the executors had {fmt_bytes(cap)} of memory a cache can use ({fmt_bytes(per)} each)"
        parts = []
        if not_fit:
            parts.append(f"{not_fit:,} blocks did not fit in memory" + (f" (one reached {fmt_bytes(computed)})" if computed else ""))
        if dropped:
            parts.append(f"{dropped:,} were dropped from memory")
        if lost:
            parts.append(f"{lost:,} were lost when executors went away and had to be rebuilt")
        if parts:
            ev += ". " + "; ".join(parts)[0].upper() + "; ".join(parts)[1:]
        # the event log counts unpersist calls but does not say which cache they released
        ev += ". " + ("Nothing was released (no unpersist)." if not released else
                      f"The app called unpersist {released} time{'s' if released != 1 else ''}; the logs do not say whether this cache was one of them.")
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
        # what a count cost: the time its stages ran, not how long the query stayed open (when the stages are known)
        cost = lambda q: min(q.get("duration_ms") or 0, busy.get((q["spark_context_id"], q["sql_execution_id"]), 0)) if busy else (q.get("duration_ms") or 0)  # noqa: E731
        counts = [q for q in qs if cost(q) > 0 and count_only(_plan(q))]
        tot = sum(cost(q) for q in counts)
        if counts and tot >= 60_000 and (not dur or tot >= 0.1 * dur):
            counts.sort(key=lambda q: -cost(q))
            # a count over a cache that is used for the first time fills it (its cost moves to the next action that reads
            # the cache if the count is dropped); later counts only read it
            seen, filled, read = set(), 0, 0
            for q in sorted((q for q in qs if _CACHE_RE.search(_plan(q))), key=lambda q: to_ms(q.get("start_time")) or 0):
                sz = cache_size(_plan(q))
                if q in counts:
                    if sz in seen:
                        read += 1
                    else:
                        filled += 1
                seen.add(sz)
            ev = (f"{len(counts)} quer{'ies' if len(counts) != 1 else 'y'} only counted rows: {fmt_words(tot)}" + (" of Spark work," if busy else "")
                  + (f" of its {fmt_words(dur)} ({tot / dur:.0%})" if dur else "") + ". "
                  + ", ".join(f"query {q['sql_execution_id']} {fmt_words(cost(q))}" for q in counts[:4])
                  + (f"; {filled} of them filled a DataFrame cache (dropping that count moves its work to the next "
                       "action on the cache)" if filled else "")
                  + (f"; {read} only read a cache" if read else ""))
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

    # ---- a window dedup that kept every row -------------------------------------------------------------------------
    # row_number() over a key, keep the first: when as many rows come out as went in, the shuffle and sort bought nothing
    for q in queries:
        p = _plan(q)
        n_in, n_out = q.get("input_records") or 0, q.get("output_records") or 0
        if n_in < 10_000_000 or n_out != n_in or "Window" not in p or "row_number" not in p.lower():
            continue
        sh = q.get("shuffle_read") or 0
        ev = (f"query {q['sql_execution_id']} deduplicated {n_in:,} rows with a window (row_number) and wrote all "
              f"{n_out:,} of them" + (f", after shuffling {fmt_bytes(sh)}" if sh else "")
              + (f" and spilling {fmt_bytes(q['disk_spill'])}" if q.get("disk_spill") else ""))
        out.append(_f(cid, q["spark_context_id"], "medium" if sh >= 100 * GB else "low", "dedup_noop",
                      f"query {q['sql_execution_id']}: dedup that removed nothing", ev, to_ms(q.get("start_time")),
                      run_key=q.get("run_key"), sql_execution_id=q["sql_execution_id"]))

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


CATEGORIES = ("dataframe_cache", "count_only", "ddl_loop", "disk_cache", "dedup_noop", "no_spark_work",
              "tiny_task_stages")

DELTA_LOG_FIX = (
    "These are Delta reading a table's transaction log (its state), split into spark.databricks.delta.snapshotPartitions "
    "parts (open source Delta: 50). For small tables (tests, local runs, a laptop) set it to 1 or 2 in the session: "
    "each part is a task of a few milliseconds, and on few cores they run one after another.")


def _merge(iv: list[tuple[int, int]]) -> list[tuple[int, int]]:
    out: list[list[int]] = []
    for a, z in sorted(iv):
        if out and a <= out[-1][1]:
            out[-1][1] = max(out[-1][1], z)
        else:
            out.append([a, z])
    return [(a, z) for a, z in out]


def _within(iv: list[tuple[int, int]], lo: int, hi: int) -> list[tuple[int, int]]:
    return [(max(a, lo), min(z, hi)) for a, z in iv if min(z, hi) > max(a, lo)]


def _length(iv: list[tuple[int, int]]) -> int:
    return sum(z - a for a, z in iv)


def _minus(a_iv: list[tuple[int, int]], b_iv: list[tuple[int, int]]) -> int:
    """Length of the merged intervals a_iv outside the merged intervals b_iv."""
    tot = 0
    for a, z in a_iv:
        cur = a
        for b0, b1 in b_iv:
            if b1 <= cur:
                continue
            if b0 >= z:
                break
            if b0 > cur:
                tot += b0 - cur
            cur = max(cur, b1)
            if cur >= z:
                break
        if cur < z:
            tot += z - cur
    return tot


_DIGITS_RE = re.compile(r"\b(?=[0-9a-f]*\d)[0-9a-f]{6,}\b|\d+")  # numbers and hex ids


_CALL_SITE_RE = re.compile(r"^\S+ at \S+:\d+$")
_WRAPPERS = {"CommandResult", "AdaptiveSparkPlan", "ExecutedCommand", "ResultQueryStage", "WholeStageCodegen"}


def _shape(q: Mapping) -> str:
    """What a statement was, for a list: its description, or its plan's command (DROP TABLE, CREATE TABLE) when the
    description is only a call site (sql at <unknown>:0, from PySpark)."""
    d = (q.get("description") or "").strip().splitlines()
    d = d[0].strip() if d else ""
    if not d or _CALL_SITE_RE.match(d):
        op = next((str(o) for o in (q.get("operators") if q.get("operators") is not None else []) if str(o) not in _WRAPPERS), None)
        if op:
            return re.sub(r"([a-z])([A-Z])", r"\1 \2", op).upper() if op.endswith("Table") else op
    return _DIGITS_RE.sub("N", d)[:60] or "no description"


def _clip(iv: list[tuple[int, int]], up: list[tuple[int, int]]) -> list[tuple[int, int]]:
    return _merge([x for a, z in up for x in _within(iv, a, z)])


def _no_spark(spans_q: list[dict], stage_iv: list[tuple[int, int]], up: list[tuple[int, int]]):
    """Over the merged intervals `up` (when its Spark app was running): (wall ms, ms with no stage running, of it inside
    SQL statements, the statement shapes with the most of that time)."""
    wall = _length(up)
    st = _clip(stage_iv, up)
    none = wall - _length(st)
    q_iv = _clip([(to_ms(q["start_time"]), to_ms(q["end_time"])) for q in spans_q], up)
    in_q = _minus(q_iv, st)
    shapes: dict[str, list] = {}
    for q in spans_q:
        own = _minus(_clip([(to_ms(q["start_time"]), to_ms(q["end_time"]))], up), st)
        if own >= 1000:
            s = shapes.setdefault(_shape(q), [0, 0])
            s[0] += 1
            s[1] += own
    top = sorted(shapes.items(), key=lambda kv: -kv[1][1])[:3]
    return wall, none, in_q, top


def driver_findings(cid: str, apps: list[dict], runs: list[dict], queries: list[dict], stages: list[dict],
                    settings: list[dict], rules: Rules) -> list[dict]:
    """no_spark_work (per run, and per Spark app when no single run covers most of it) and tiny_task_stages."""
    from ..settings_catalog import local_contexts

    out: list[dict] = []
    local = local_contexts(settings)
    stage_iv: dict = {}
    run_stage_iv: dict = {}
    for s in stages:
        a, z = to_ms(s.get("start_time")), to_ms(s.get("end_time"))
        if a is None or z is None or z <= a:
            continue
        stage_iv.setdefault(s["spark_context_id"], []).append((a, z))
        if s.get("run_key"):
            run_stage_iv.setdefault(s["run_key"], []).append((a, z))
    q_ok = [q for q in queries if to_ms(q.get("start_time")) is not None and to_ms(q.get("end_time")) is not None
            and to_ms(q["end_time"]) > to_ms(q["start_time"])]

    def text(none, wall, in_q, top, what):
        ev = f"No Spark stage ran for {fmt_words(none)} of {what} {fmt_words(wall)} ({none / wall:.0%})"
        bits = []
        if in_q >= 1000:
            bits.append(f"{fmt_words(in_q)} inside SQL statements while none of their stages ran"
                        + (" (" + ", ".join(f"{n} × {sh}" for sh, (n, _ms) in top) + ")" if top else ""))
        if none - in_q >= 1000:
            bits.append(f"{fmt_words(none - in_q)} between statements (driver or Python code, session start, waiting)")
        return ev + (": " + "; ".join(bits) if bits else "") + "."

    # a run counts only while a Spark app of it was up: the gap between two apps (a restart) is not driver code
    app_span = {a["spark_context_id"]: (to_ms(a.get("start_time")), to_ms(a.get("end_time"))) for a in apps}
    run_ctx: dict = {}
    for x in [*stages, *queries]:
        if x.get("run_key"):
            run_ctx.setdefault(x["run_key"], set()).add(x["spark_context_id"])
    covered: dict = {}
    for r in runs:
        if r.get("kind") == "app":
            continue
        lo, hi = to_ms(r.get("start_time")), to_ms(r.get("end_time"))
        if lo is None or hi is None or hi - lo < rules.no_spark_min_ms:
            continue
        spans = [app_span.get(c) for c in run_ctx.get(r["run_key"], {r["spark_context_id"]})]
        up = _clip([(a, z) for a, z in (s for s in spans if s) if a is not None and z is not None] or [(lo, hi)], [(lo, hi)])
        qs = [q for q in q_ok if q.get("run_key") == r["run_key"]]
        wall, none, in_q, top = _no_spark(qs, run_stage_iv.get(r["run_key"], []), up)
        if none < rules.no_spark_min_ms or none < rules.no_spark_share_min * wall:
            continue
        covered[r["spark_context_id"]] = max(covered.get(r["spark_context_id"], 0), wall)
        sev = "high" if none >= 0.7 * wall and none >= 600_000 else "medium"
        out.append(_f(cid, r["spark_context_id"], sev, "no_spark_work", f"run {_run_name(r)}",
                      text(none, wall, in_q, top, "its"), lo, run_key=r["run_key"]))
    app_run = {r["spark_context_id"]: r["run_key"] for r in runs if r.get("kind") == "app"}
    for a in apps:
        ctx = a["spark_context_id"]
        lo, hi = to_ms(a.get("start_time")), to_ms(a.get("end_time"))
        if lo is None or hi is None or hi - lo < rules.no_spark_min_ms or covered.get(ctx, 0) >= 0.8 * (hi - lo):
            continue
        qs = [q for q in q_ok if q["spark_context_id"] == ctx]
        _wall, none, in_q, top = _no_spark(qs, stage_iv.get(ctx, []), [(lo, hi)])
        if none < rules.no_spark_min_ms or none < rules.no_spark_share_min * (hi - lo):
            continue
        sev = "high" if none >= 0.7 * (hi - lo) and (none >= 600_000 or ctx in local) else "medium"
        out.append(_f(cid, ctx, sev, "no_spark_work", f"Spark app {a.get('app_name') or ctx}",
                      text(none, hi - lo, in_q, top, "the app's"), lo, run_key=app_run.get(ctx)))

    # ---- many stages that cut almost nothing into the same number of tasks ------------------------------------------
    by_ctx: dict = {}
    for s in stages:
        by_ctx.setdefault(s["spark_context_id"], []).append(s)
    for ctx, ss in by_ctx.items():
        all_tasks = sum(s.get("tasks") or 0 for s in ss)
        groups: dict = {}
        for s in ss:
            n, p50 = s.get("tasks") or 0, s.get("p50_task_ms")
            if n >= 20 and p50 is not None and p50 <= 100 and (s.get("p50_task_bytes_in") or 0) <= 64 << 10:
                groups.setdefault(n, []).append(s)
        for n, g in groups.items():
            tasks = sum(s.get("tasks") or 0 for s in g)
            if len(g) < 10 or tasks < max(1000, 0.3 * all_tasks):
                continue
            ms = sum(s.get("duration_ms") or 0 for s in g)
            delta = sum(1 for s in g if any("Delta Table State" in str(x) for x in (s.get("rdd_names") or [])))
            p50s = sorted(s["p50_task_ms"] for s in g)
            ev = (f"{len(g)} stages ran exactly {n} tasks each ({tasks:,} of the app's {all_tasks:,} tasks, "
                  f"{tasks / all_tasks:.0%}); their median task took {p50s[len(p50s) // 2]} ms and read almost nothing. "
                  f"Together they took {fmt_words(ms)} of stage time")
            if delta:
                ev += f". {delta} of them read a Delta table's transaction log"
            app = next((a for a in apps if a["spark_context_id"] == ctx), {})
            dur = (to_ms(app.get("end_time")) or 0) - (to_ms(app.get("start_time")) or 0)
            sev = "high" if dur > 0 and ms >= 0.2 * dur else "medium" if ms >= 60_000 else "low"
            f = _f(cid, ctx, sev, "tiny_task_stages", f"{len(g)} stages of {n} tiny tasks", ev + ".",
                   to_ms(g[0].get("start_time")), stage_id=g[0]["stage_id"], stage_attempt=g[0]["stage_attempt"],
                   run_key=app_run.get(ctx))
            if delta >= len(g) / 2:
                f["fix"] = DELTA_LOG_FIX
            out.append(f)
    return out
