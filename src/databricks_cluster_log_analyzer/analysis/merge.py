"""One source of truth for the numbers of a Delta MERGE step: what it read of the target (not the re-read of its
source copy), the files it touched, whether deletion vectors and Change Data Feed are on, and the rows it updated and
inserted when the stage rows prove them.

A streaming MERGE runs as 3 or more SQL queries: "materialize source" (a copy of the source rows), "scanning files for
matches" (which target files hold a match), and the write ("Rewriting N files and writing modified and inserted
data"). Adaptive execution adds helper queries that read nothing, and can cancel a scan it replanned (0 bytes read).
Photon names its scans "PhotonScan parquet <table>" where Spark says "Scan parquet <table>".
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

from ..util import fmt_bytes

#: a scan of a table's files, Spark's or Photon's
SCAN_FILES = re.compile(r"^(?:Photon)?Scan (?:parquet|delta|json|csv|orc|text)\s+(\S+)")
#: the scan of a Delta table (what a MERGE target is)
_SCAN_TARGET = re.compile(r"^(?:Photon)?Scan (?:parquet|delta)\s+(\S+)")
_COPY = "mergeMaterializedSource"
_STEP = re.compile(r"MERGE operation - (?:MERGE operation - )?(.+)$")
_BATCH = re.compile(r"^(.*?[Bb]atch \d+.*?) · MERGE")
_KEY = re.compile(r"^Left keys \[\d+\]: \[(.+)\]$", re.M)


def scan_table(name: str | None) -> str | None:
    """The table a scan of files reads ("Scan parquet t", "PhotonScan parquet t"), else None."""
    m = SCAN_FILES.match(" ".join((name or "").split()))
    return m.group(1) if m else None


def step_of(q: Mapping[str, Any]) -> str:
    """The MERGE step a query runs, lower case ("materialize source", "scanning files for matches", "rewriting 200
    files and ..."), or "" when it is not a named MERGE step."""
    m = _STEP.search(" ".join((q.get("description") or "").split()))
    return m.group(1).strip().lower() if m else ""


def _num(v) -> int:
    try:
        return 0 if v is None or v != v else int(v)
    except (TypeError, ValueError):
        return 0


def _scopes(st: Mapping[str, Any]) -> list[str]:
    s = st.get("rdd_scopes")
    return [] if s is None else [" ".join(str(x).split()) for x in s]


def _ok(st: Mapping[str, Any]) -> bool:
    return (st.get("status") or "succeeded") == "succeeded"


def _targets_in(st: Mapping[str, Any]) -> list[str]:
    return [m.group(1) for m in (_SCAN_TARGET.match(x) for x in _scopes(st)) if m]


def _holds_copy(st: Mapping[str, Any]) -> bool:
    return any(_COPY in x for x in _scopes(st))


def cycle_of(step: Mapping[str, Any], siblings: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """The queries of the same MERGE as `step`, before it: the same streaming batch (when the description names one),
    from the last "materialize source" that started before it. Later queries belong to the next MERGE."""
    me = (step.get("spark_context_id"), step.get("sql_execution_id"))
    b = _BATCH.match(step.get("description") or "")
    sib = [x for x in siblings if (x.get("spark_context_id"), x.get("sql_execution_id")) != me
           and x.get("spark_context_id") == step.get("spark_context_id")
           and (x.get("sql_execution_id") or 0) < (step.get("sql_execution_id") or 0)]
    if b:
        sib = [x for x in sib if (x.get("description") or "").startswith(b.group(1) + " · MERGE")]
    sib.sort(key=lambda x: x.get("sql_execution_id") or 0)
    mat = [i for i, x in enumerate(sib) if step_of(x).startswith("materiali")]
    return sib[mat[-1]:] if mat else sib


def merge_facts(step_query: Mapping[str, Any], sibling_queries: Iterable[Mapping[str, Any]], stages: Iterable[Mapping[str, Any]],
                plan_text: str | None = None, nodes: Iterable[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    """The numbers of one MERGE write step (`step_query`), from its stages, the stages of the earlier steps of the same
    MERGE (found among `sibling_queries`: the same run or batch), their plans (`plan_text`; by default the final plans
    of the step and its MERGE's earlier steps) and the scan nodes of the plans (`nodes`, sql_plan_nodes rows, optional).

    target_bytes / target_rows: what the step's target scan stages read (succeeded stages whose operators scan
    `(Photon)?Scan parquet|delta <target>`, not the stages that read the materialized source back); copy_bytes /
    copy_rows: that re-read of the source copy, apart. scan_bytes: what the "scanning files for matches" step read of
    the target. files_touched / files_total / file_bytes: from the plan's scan metrics. dv_on / cdf_on: deletion vectors
    and Change Data Feed, seen in the plans. source_rows: rows of the source copy. updated / inserted: derived from the
    stage rows, only with deletion vectors and when output = source + 2 x updated + inserted (CDF) or output = source
    (no CDF) holds; else None."""
    step_query = dict(step_query)
    cyc = cycle_of(step_query, sibling_queries)
    keys = {(q.get("spark_context_id"), q.get("sql_execution_id")) for q in [step_query, *cyc]}
    mine = (step_query.get("spark_context_id"), step_query.get("sql_execution_id"))
    by_q: dict[tuple, list[Mapping]] = {}
    for st in stages:
        k = (st.get("spark_context_id"), st.get("sql_execution_id"))
        if k in keys:
            by_q.setdefault(k, []).append(st)
    own = by_q.get(mine, [])
    scanning = [q for q in cyc if step_of(q).startswith("scanning files")]

    # the target: the Delta table the step scans most (the one the "scanning files" step also scans, first)
    bytes_by: dict[str, int] = {}
    for st in own:
        if not _holds_copy(st):
            for t in _targets_in(st):
                bytes_by[t] = bytes_by.get(t, 0) + _num(st.get("input_bytes")) + 1
    seen_in_scan = {t for q in scanning for st in by_q.get((q.get("spark_context_id"), q.get("sql_execution_id")), [])
                    for t in _targets_in(st)}
    target = None
    if bytes_by:
        target = max(bytes_by, key=lambda t: (t in seen_in_scan, bytes_by[t]))
    else:
        named = [scan_table(n.get("name")) for n in nodes or [] if (n.get("spark_context_id"), n.get("sql_execution_id")) == mine]
        named = [t for t in named if t] or [t for t in step_query.get("tables_read") or [] if "/" not in t]
        target = named[0] if named else None

    def target_stages(k: tuple) -> list[Mapping]:
        return [st for st in by_q.get(k, []) if _ok(st) and not _holds_copy(st) and target in _targets_in(st)]

    tst = target_stages(mine) if target else []
    copy = [st for st in own if _ok(st) and _holds_copy(st)]
    copy_bytes = sum(_num(st.get("input_bytes")) for st in copy)
    copy_rows = sum(_num(st.get("input_records")) for st in copy)
    if tst:
        target_bytes = sum(_num(st.get("input_bytes")) for st in tst)
        target_rows = sum(_num(st.get("input_records")) for st in tst)
        target_from = "stages"
    else:  # no stage names its scans (an older build): the query's input, less the source copy
        target_bytes = max(0, _num(step_query.get("input_bytes")) - copy_bytes)
        target_rows = max(0, _num(step_query.get("input_records")) - copy_rows)
        target_from = "query"
    scan_bytes = 0
    for q in scanning:
        k = (q.get("spark_context_id"), q.get("sql_execution_id"))
        ts = target_stages(k) if target else []
        scan_bytes += (sum(_num(st.get("input_bytes")) for st in ts) if ts else
                       max(0, _num(q.get("input_bytes")) - sum(_num(st.get("input_bytes")) for st in by_q.get(k, []) if _holds_copy(st))))

    # files: the step's scan of the target (files read = touched; read + pruned = the table), else its description
    touched = pruned = file_bytes = parts = total = 0
    for n in nodes or []:
        k = (n.get("spark_context_id"), n.get("sql_execution_id"))
        if k not in keys or not target or scan_table(n.get("name")) != target:
            continue
        try:
            mt = {x.get("name"): _num(x.get("total")) for x in json.loads(n.get("metrics_json") or "[]")}
        except (ValueError, TypeError, AttributeError):
            continue
        parts = max(parts, mt.get("number of partition columns", 0))
        fr, fp = mt.get("number of files read", 0), mt.get("number of files pruned", 0)
        total = max(total, fr + fp)
        if k == mine:
            touched += fr
            pruned += fp
            file_bytes += mt.get("size of files read", 0)
    if not touched:
        m = re.search(r"(?i)rewriting (\d+) files", step_query.get("description") or "")
        touched = int(m.group(1)) if m else 0

    if plan_text is None:
        plan_text = "\n".join(q.get("final_plan") or "" for q in [step_query, *cyc])
    p = plan_text or ""
    dv_on = "deletionVectorId" in p or "_target_row_file_dv_id_" in p
    cdf_on = "packedCdc" in p or "__is_cdc" in p
    clustered = bool(re.search(r"clusteringColumns|\bCLUSTER BY\b", p))
    km = _KEY.search(p)
    key = None
    if km:
        k0 = km.group(1).split(", ")[0]
        k0 = re.sub(r"#\d+L?", "", k0)
        key = re.sub(r"^coalesce\((.+?)$", r"\1", k0).strip() or None

    # source rows: the stage that holds the source copy in this step, else the materialize step's last shuffle
    source_rows = sum(_num(st.get("shuffle_write_records")) or _num(st.get("input_records")) for st in copy) or None
    if source_rows is None:
        mat = [q for q in cyc if step_of(q).startswith("materiali")]
        if mat:
            ms = [st for st in by_q.get((mat[-1].get("spark_context_id"), mat[-1].get("sql_execution_id")), [])
                  if _ok(st) and _num(st.get("shuffle_write_records"))]
            if ms:
                source_rows = _num(max(ms, key=lambda st: st.get("stage_id") or 0).get("shuffle_write_records"))
    output = _num(step_query.get("output_records")) or None
    updated = inserted = None
    matched = sum(_num(st.get("shuffle_write_records")) for st in tst) if tst else None
    if dv_on and source_rows and output and matched is not None and matched <= source_rows:
        ins = source_rows - matched
        if (cdf_on and output == source_rows + 2 * matched + ins) or (not cdf_on and output == source_rows):
            updated, inserted = matched, ins
    data_rows = source_rows if dv_on and source_rows and output and output >= source_rows else output
    change_rows = (output - source_rows) if cdf_on and source_rows and output and output > source_rows else None
    return {
        "target": target, "target_bytes": target_bytes, "target_rows": target_rows, "target_from": target_from,
        "target_stages": [st.get("stage_id") for st in tst],
        "copy_bytes": copy_bytes, "copy_rows": copy_rows, "scan_bytes": scan_bytes,
        "files_touched": touched or None, "files_total": max(total, touched + pruned) or None,
        "file_bytes": file_bytes or None, "per_file": round(file_bytes / touched) if file_bytes and touched else None,
        "partition_cols": parts, "clustered": clustered, "merge_key": key,
        "dv_on": dv_on, "cdf_on": cdf_on,
        "source_rows": source_rows, "output_rows": output, "data_rows": data_rows, "change_rows": change_rows,
        "updated": updated, "inserted": inserted, "derived": updated is not None,
        "spill": _num(step_query.get("disk_spill")),
    }


def wrote_text(f: Mapping[str, Any]) -> str:
    """What the write did to the target, in one clause."""
    if f.get("dv_on"):
        s = "matched rows marked deleted (deletion vectors)"
        if f.get("data_rows"):
            s += f"; {f['data_rows']:,} data rows written"
        if f.get("change_rows"):
            s += f", {f['change_rows']:,} change rows (CDF)"
        if f.get("derived"):
            s += f" (derived: {f['updated']:,} updated, {f['inserted']:,} inserted)"
        return s
    n = f.get("files_touched")
    return f"rewrote whole files{f' ({n:,})' if n else ''}: every file with a matched row is copied with the change"


def files_text(f: Mapping[str, Any]) -> str | None:
    """"200 of 200 files touched, 2.2 GiB per file"."""
    n, t = f.get("files_touched"), f.get("files_total")
    if not n:
        return None
    s = f"{n:,} of {t:,} files touched" if t else f"{n:,} files touched"
    if f.get("per_file"):
        s += f", {fmt_bytes(f['per_file'])} per file"
    return s


def merge_fix(f: Mapping[str, Any], numbers: bool = True) -> list[str]:
    """What to change, from what the logs show about this MERGE. With `numbers`, the sentences carry its numbers."""
    out = []
    ft = files_text(f) if numbers else None
    if ft:
        out.append(f"The write read every file with a match: {ft}.")
    bound = ("only if a key's rows never fall outside the bound, else NOT MATCHED inserts duplicates")
    if f.get("partition_cols") or f.get("clustered"):
        out.append("Add the target's partition or clustering column to the MERGE ON condition so Delta skips the files "
                   f"it cannot match (for example t.<column> >= the oldest value in the source): {bound}.")
    else:
        key = f.get("merge_key") if numbers else None
        out.append(f"The target has no partition or clustering column: cluster it on the merge key (Liquid Clustering"
                   f"{' by ' + key if key else ''}); this helps only if each batch's keys fall in a narrow range.")
    if not f.get("dv_on"):
        out.append("If deletion vectors are off, turn them on so a matched row does not rewrite its whole file.")
    if f.get("cdf_on"):
        n = f.get("change_rows")
        out.append(f"CDF on the target adds {f'{n:,} ' if numbers and n else ''}change rows to the write shuffle; turn it "
                   "off only if nothing reads the table's change feed.")
    if f.get("spill", 0) >= 1 << 30:
        out.append("The join and the write spilled"
                   + (f" {fmt_bytes(f['spill'])}" if numbers else "")
                   + ": give them more shuffle partitions (spark.sql.shuffle.partitions, about 128 MiB of shuffle each).")
    return out
