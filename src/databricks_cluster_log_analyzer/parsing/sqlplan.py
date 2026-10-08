"""Per-operator SQL metrics (Revision 7), the data behind the Spark UI SQL tab and the Databricks query profile.

`SparkListenerSQLExecutionStart` / `SparkListenerSQLAdaptiveExecutionUpdate` carry `sparkPlanInfo`: a tree of
`{nodeName, simpleString, children[], metrics[{name, accumulatorId, metricType}]}`. The metric values arrive
separately, keyed by the same accumulator id:

* `SparkListenerTaskEnd` → `Task Info.Accumulables[{ID, Update, ...}]` (one update per task; failed tasks skipped),
* `SparkListenerStageCompleted` → `Stage Info.Accumulables[{ID, Value}]` (stage totals; used when tasks have none),
* `SparkListenerDriverAccumUpdates` → `accumUpdates [[id, value], ...]` (driver-side metrics, e.g. broadcast).

`walk_plan` flattens the tree (`plan_from_text` reads it from the plan text when the log has none); `plan_node_rows` joins it with the accumulated values into one row per operator.
"""
from __future__ import annotations

import json
import re
from typing import Any

CODEGEN_RE = re.compile(r"^WholeStageCodegen\s*\((\d+)\)")
DETAIL_MAX = 400
# the one size shown per operator: what it read, shuffled or wrote, else its output size
SIZE_PRIORITY = ("size of files read", "shuffle bytes written", "written output", "remote bytes read", "data size")


def _int(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None


def walk_plan(info: dict | None) -> list[dict]:
    """Pre-order list of operators: node_id, parent_id, name, detail, codegen_id, is_cluster, metrics.

    `codegen_id` is the WholeStageCodegen cluster an operator runs inside (the cluster node itself carries it too);
    an InputAdapter ends the cluster for its children, as in the Spark UI."""
    nodes: list[dict] = []
    if not isinstance(info, dict):
        return nodes
    stack: list[tuple[dict, int | None, int | None]] = [(info, None, None)]
    while stack:
        n, parent, cg = stack.pop()
        name = str(n.get("nodeName") or "")
        m = CODEGEN_RE.match(name)
        own_cg = int(m.group(1)) if m else cg
        metrics = []
        for mm in n.get("metrics") or []:
            if isinstance(mm, dict) and _int(mm.get("accumulatorId")) is not None:
                metrics.append((str(mm.get("name") or ""), _int(mm.get("accumulatorId")), str(mm.get("metricType") or "sum")))
        idx = len(nodes)
        detail = n.get("simpleString")
        nodes.append({"node_id": idx, "parent_id": parent, "name": name,
                      "detail": detail[:DETAIL_MAX] if isinstance(detail, str) else None,
                      "codegen_id": own_cg, "is_cluster": bool(m), "metrics": metrics})
        child_cg = None if name == "InputAdapter" else own_cg
        for c in reversed([c for c in (n.get("children") or []) if isinstance(c, dict)]):
            stack.append((c, idx, child_cg))
    return nodes


TREE_PREFIX_RE = re.compile(r"^((?:[ |]|:(?!- ))*)(?:[+:]- )?")
CODEGEN_TEXT_RE = re.compile(r"^\*\((\d+)\)\s+")       # simple / extended explain: `*(2) Project [...]`
CODEGEN_STAR_RE = re.compile(r"^\*\s+")                # formatted explain (Spark 3.1+ UI default): `* Project (4)`
FORMATTED_NUM_RE = re.compile(r"\s+\((\d+)\)$")       # formatted explain: operator number at the end of the line
FORMATTED_DETAIL_RE = re.compile(r"^\((\d+)\)\s.*?\[codegen id : (\d+)\]")
FILESCAN_RE = re.compile(r"^(?:FileScan|Scan)\s+(\w+)\s+([\w.`$-]+)")
TWO_WORD = {"Execute", "Scan"}  # sparkPlanInfo names these with their second word


def _text_name(op: str) -> str:
    """Operator name as sparkPlanInfo spells it: `FileScan parquet db.t[cols]` -> `Scan parquet db.t`,
    `Execute InsertIntoHadoopFsRelationCommand dbfs:/x` -> `Execute InsertIntoHadoopFsRelationCommand`."""
    m = FILESCAN_RE.match(op)
    if m:
        return f"Scan {m.group(1)} {m.group(2)}"
    words = re.split(r"[\s(\[]", op, maxsplit=2)
    if words[0] in TWO_WORD and len(words) > 1 and words[1]:
        return f"{words[0]} {words[1]}"
    return words[0]


def plan_from_text(text: str | None) -> list[dict]:
    """Operator tree parsed from a `physicalPlanDescription`, for queries whose `sparkPlanInfo` is missing or a
    childless stub. Same shape as `walk_plan` (no metrics, `from_text` set): one node per tree line of the physical
    plan (logical sections of an extended explain are skipped), the final AQE plan only (an `== Initial Plan ==`
    section is skipped, also inside a subquery), `*(n)` / formatted `*` operators wrapped in a `WholeStageCodegen (n)`
    cluster node as sparkPlanInfo does (formatted explain takes the id from its `[codegen id : n]` details)."""
    nodes: list[dict] = []
    if not isinstance(text, str):
        return nodes
    lines = text.splitlines()
    phys = next((i for i, ln in enumerate(lines) if ln.strip() == "== Physical Plan =="), None)
    if phys is not None:
        lines = lines[phys + 1:]
    # formatted explain: "(6) HashAggregate [codegen id : 2]" in the details after the tree
    cg_of = {int(m.group(1)): int(m.group(2)) for m in (FORMATTED_DETAIL_RE.match(ln.strip()) for ln in lines) if m}
    stack: list[tuple[int, int]] = []  # (depth, node index) of the open ancestors
    started = False
    skip_from: int | None = None  # inside a subquery's `== Initial Plan ==` section: skip lines this deep or deeper
    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            if started:
                break  # the tree ends at the first blank line (formatted explain lists operator details after it)
            continue
        pre = TREE_PREFIX_RE.match(line)
        col = pre.end() if pre else 0
        depth = col // 3
        op = line[col:]
        if skip_from is not None:
            if depth >= skip_from:  # a section's operators sit at its header's own indent
                continue
            skip_from = None
        if op.startswith("=="):
            if "Initial Plan" in op:
                if depth <= 1 and ":" not in line[:col]:
                    break  # the query's own initial plan: the final plan above is complete
                skip_from = depth  # a subquery's initial plan: skip its lines, keep the rest of the tree
            continue
        if not op:
            continue
        started = True
        while stack and stack[-1][0] >= depth:
            stack.pop()
        parent = stack[-1][1] if stack else None
        num_m = FORMATTED_NUM_RE.search(op)
        num = int(num_m.group(1)) if num_m else None
        if num_m:
            op = op[:num_m.start()]
        cg = None
        m = CODEGEN_TEXT_RE.match(op)
        if m:
            cg = int(m.group(1))
            op = op[m.end():]
        else:
            m = CODEGEN_STAR_RE.match(op)
            if m:
                op = op[m.end():]
                cg = cg_of.get(num) if num is not None else None
        if cg is not None and (parent is None or nodes[parent]["codegen_id"] != cg):
            nodes.append({"node_id": len(nodes), "parent_id": parent, "name": f"WholeStageCodegen ({cg})",
                          "detail": None, "codegen_id": cg, "is_cluster": True, "metrics": [], "from_text": True})
            parent = len(nodes) - 1
        nodes.append({"node_id": len(nodes), "parent_id": parent, "name": _text_name(op),
                      "detail": op[:DETAIL_MAX], "codegen_id": cg, "is_cluster": False, "metrics": [],
                      "from_text": True})
        stack.append((depth, len(nodes) - 1))
    return nodes


def add_task_accums(acc: dict, ctx: str, e: dict) -> None:
    """Fold one TaskEnd's SQL accumulator updates into acc[(ctx, id)] = [sum, max, count]."""
    ti = e.get("Task Info") or {}
    items = ti.get("Accumulables")
    if not items:
        return
    reason = (e.get("Task End Reason") or {}).get("Reason")
    if reason not in (None, "Success"):
        return  # Spark's SQL metrics only count successful tasks
    for a in items:
        if not isinstance(a, dict) or a.get("Internal") or str(a.get("Name") or "").startswith("internal."):
            continue
        aid = _int(a.get("ID"))
        v = _int(a.get("Update"))
        if aid is None or v is None:
            continue
        cur = acc.get((ctx, aid))
        if cur is None:
            acc[(ctx, aid)] = [v, v, 1]
        else:
            cur[0] += v
            cur[1] = max(cur[1], v)
            cur[2] += 1


def add_stage_accums(acc: dict, ctx: str, e: dict) -> None:
    """Stage totals: acc[(ctx, id)] += Value (several stages may update one metric)."""
    si = e.get("Stage Info") or {}
    for a in si.get("Accumulables") or []:
        if not isinstance(a, dict) or a.get("Internal") or str(a.get("Name") or "").startswith("internal."):
            continue
        aid = _int(a.get("ID"))
        v = _int(a.get("Value"))
        if aid is not None and v is not None:
            acc[(ctx, aid)] = acc.get((ctx, aid), 0) + v


def add_driver_accums(acc: dict, ctx: str, e: dict) -> None:
    for pair in e.get("accumUpdates") or []:
        if isinstance(pair, (list, tuple)) and len(pair) == 2:
            aid, v = _int(pair[0]), _int(pair[1])
            if aid is not None and v is not None:
                acc[(ctx, aid)] = acc.get((ctx, aid), 0) + v


def _value(ctx: str, aid: int, task: dict, stage: dict, driver: dict) -> tuple[int | None, int | None, int]:
    """(total, max per task, task count) for one accumulator."""
    t = task.get((ctx, aid))
    if t is not None:
        tot, mx, n = t
        return tot + driver.get((ctx, aid), 0), mx, n
    if (ctx, aid) in stage:
        return stage[(ctx, aid)] + driver.get((ctx, aid), 0), None, 0
    if (ctx, aid) in driver:
        return driver[(ctx, aid)], None, 0
    return None, None, 0


def _ms(v: int | None, mtype: str) -> float | None:
    if v is None:
        return None
    return v / 1e6 if mtype == "nsTiming" else float(v)


def plan_node_rows(cid: str, ctx: str, exec_id: int | None, nodes: list[dict], task: dict, stage: dict,
                   driver: dict) -> list[dict]:
    """One row per operator with the headline metrics pulled out and every metric in `metrics_json`."""
    rows = []
    for n in nodes:
        allm = []
        rows_out = time_ms = duration = peak = spill = None
        sizes: dict[str, int] = {}
        for name, aid, mtype in n["metrics"]:
            tot, mx, cnt = _value(ctx, aid, task, stage, driver)
            if tot is None:
                continue
            ln = name.lower()
            entry: dict[str, Any] = {"name": name, "type": mtype, "total": tot}
            if mx is not None:
                entry["max"] = mx
                entry["tasks"] = cnt
            if mtype in ("timing", "nsTiming"):
                entry["total_ms"] = _ms(tot, mtype)
                if mx is not None:
                    entry["max_ms"] = _ms(mx, mtype)
            allm.append(entry)
            if ln == "number of output rows":
                rows_out = (rows_out or 0) + tot
            elif mtype in ("timing", "nsTiming"):
                # "duration" is the operator's own clock (codegen clusters); otherwise sum its timings
                ms = _ms(tot, mtype) or 0.0
                if ln == "duration":
                    duration = (duration or 0.0) + ms
                else:
                    time_ms = (time_ms or 0.0) + ms
            elif "peak memory" in ln:
                peak = max(peak or 0, mx if mx is not None else tot)
            elif "spill size" in ln or ln == "spill size":
                spill = (spill or 0) + tot
            if mtype == "size" and ln in SIZE_PRIORITY:
                sizes[ln] = tot
        if duration is not None:
            time_ms = duration
        size = next((sizes[k] for k in SIZE_PRIORITY if k in sizes), None)
        rows.append({
            "cluster_id": cid, "spark_context_id": ctx, "sql_execution_id": exec_id, "node_id": n["node_id"],
            "parent_id": n["parent_id"], "name": n["name"], "detail": n["detail"], "codegen_id": n["codegen_id"],
            "is_cluster": n["is_cluster"], "rows_out": rows_out,
            "time_ms": None if time_ms is None else round(time_ms, 3), "peak_mem": peak, "spill_bytes": spill,
            # metrics_json: null = Spark defines no metrics for this operator; "[]" = defined, but no task reported any
            "data_bytes": size, "metrics_json": json.dumps(allm, separators=(",", ":")) if n["metrics"] else None,
            # Revision 9: true when the operator was read from the plan text (the log had no sparkPlanInfo tree)
            "from_text": bool(n.get("from_text")),
        })
    return rows
