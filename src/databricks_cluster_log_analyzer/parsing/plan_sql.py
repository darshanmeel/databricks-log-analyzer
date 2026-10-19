"""Revision 23: a physical plan back as the SQL it amounts to, and its windows (which ranking function keeps which rows).

Spark's plan is what ran; reading 80 lines of it at 2am to find the one `dense_rank` that kept two rows per key is
slow. `plan_sql` walks the plan's tree (bottom up) and writes one SELECT per layer: the scan is the FROM, a Filter
the WHERE, a Window a `fn() OVER (PARTITION BY .. ORDER BY ..)` column, an aggregate the GROUP BY (GROUPING SETS for
a rollup or cube), a join a JOIN .. ON with its keys (null-safe keys as <=>), a Union a UNION ALL, a global Sort and a
limit ORDER BY and LIMIT, a scalar subquery its own SELECT. A layer that filters on a window or a group wraps the one
below as a subquery, as SQL needs. Exchanges, query stages and codegen are how Spark ran it, not what it computed:
they are left out. The result is an equivalent query, not the original text: Spark's own rewrites (inferred
`IS NOT NULL`, pushed filters, a partial and a final aggregate) are folded back where that is safe and kept otherwise.

Spark names every column with an expression id (`amount#12`). They are kept while the SQL is built and dropped when
it is written: an id says which side of a join a column comes from (`t1.ts >= t2.ts`, not `ts >= ts`), and a word
with no id is a literal the plan printed without quotes (`status = active` -> `status = 'active'`).

Both shapes `physicalPlanDescription` comes in work: formatted (tree with node ids, then one detail section per
node) and simple (the arguments on the tree lines). Best effort: an odd plan yields None, never an exception.
"""

from __future__ import annotations

import contextvars
import re
from typing import Any

EXPR_ID = re.compile(r"#\d+L?")
#: Spark's and Delta's own columns and conditions: bookkeeping, not the user's logic
INTERNAL = re.compile(r"_databricks_internal|_source_row_present_|_target_row_present_|_row_dropped_|fileInScanId|"
                      r"deletionVector|raise_error\(|__is_cdc|packedCdc|packedData|^isnotnull\((add|remove|path)\)$|"
                      r"partitionValuesStr|sidecar\.|^true$|_metadata\b|_tmp_metadata_row_index|row_index_filter|"
                      r"_target_row_|_source_row_|_row_id_|_row_commit_version|_delta_internal|dynamicpruning")
JOINS = ("SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin", "BroadcastNestedLoopJoin", "CartesianProduct")
RANKING = ("row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "ntile")
SQL_MAX = 12_000
CAP = 400

_DETAIL_HEAD = re.compile(r"^\((\d+)\) (.+?)(?: \[codegen id : \d+\])?$")
_TREE = re.compile(r"^(?P<lead>[\s:|]*)(?P<branch>[+:]- )?(?P<star>\*(?:\(\d+\))? )?(?P<name>[A-Za-z].*?)"
                   r"(?: \((?P<id>\d+)\))?(?P<rest>, Statistics\(.*)?$")
_AGGS = ("HashAggregate", "ObjectHashAggregate", "SortAggregate", "GroupingAgg", "HashAggregateExec")
_WINDOWS = ("Window", "WindowGroupLimit", "RunningWindowFunction")
#: how Spark ran it, not what it computed
_QUIET = {"Exchange", "ShuffleExchange", "ShuffleQueryStage", "AQEShuffleRead", "CustomShuffleReader", "BroadcastExchange",
          "BroadcastQueryStage", "ColumnarToRow", "RowToColumnar", "InputAdapter", "WholeStageCodegen", "AdaptiveSparkPlan",
          "ResultQueryStage", "ResultStage", "SerializeFromObject", "DeserializeToObject", "Coalesce", "TableCacheQueryStage",
          "ShuffleExchangeSink", "ShuffleExchangeSource", "BroadcastExchangeSink", "BroadcastExchangeSource", "ToSpark",
          "Adapter", "ColumnarToRowExec", "Sort", "Project", "LocalLimit", "CollectLimit", "GlobalLimit", "Subquery",
          "SubqueryBroadcast", "ReusedSubquery", "DeltaInvariantChecker", "DeltaOptimizedWriter", "Unknown", "Expand",
          "Union", "Generate", "Filter", "Window", "WindowGroupLimit", "RunningWindowFunction", "TakeOrderedAndProject",
          "CommandResult", "LogicalQueryStage", "MapElements", "MapPartitions", "ReusedExchange", "QueryStage"}
_WRITES = re.compile(r"Write|InsertInto|AppendData|OverwriteByExpression|OverwritePartitionsDynamic|CreateTable|"
                     r"ReplaceTable|MergeInto|SaveIntoDataSource|SaveIntoDelta|CreateDataSourceTable")
_PYTHON = re.compile(r"EvalPython|MapInPandas|MapInArrow|FlatMapGroupsInPandas|FlatMapCoGroupsInPandas|"
                     r"AggregateInPandas|WindowInPandas|PythonUDTF")
#: a column with its expression id: amount#12, ts#3L
_REF = re.compile(r"(?<![\w'#$])([A-Za-z_][\w.]*)#(\d+)L?")
_NUM = re.compile(r"-?\d+(\.\d+)?([eE][+-]?\d+)?[LDSBY]?|-?\d+(\.\d+)?BD")
#: a comparison of a column with a value the plan printed without quotes ("status#3 = active", "d#2 >= 2024-01-01")
_CMP = re.compile(r"(?<![\w.])([A-Za-z_][\w.]*#\d+L?) (=|<>|!=|<=>|>=|<=|>|<) ([^()#]*?)(?=\)|$| AND | OR )")
_IN = re.compile(r"(?<![\w.])([A-Za-z_][\w.]*#\d+L?) IN \(([^()#]*)\)")
_SUBQ = re.compile(r"(?:ReusedSubquery )?Subquery (scalar-subquery#\d+), \[id=#\d+\]")
#: the scalar subqueries of the plan being written (its "===== Subqueries =====" section), by name
_SUBS: contextvars.ContextVar[dict[str, str]] = contextvars.ContextVar("plan_sql_subqueries", default={})


# ---------------------------------------------------------------------------------------------------------------------
# small text helpers
# ---------------------------------------------------------------------------------------------------------------------

def split_top(s: str, sep: str = ",") -> list[str]:
    """Split "a, f(b, c), d" on the separators that are not inside brackets."""
    out, depth, cur, i, n = [], 0, [], 0, len(sep)
    while i < len(s):
        ch = s[i]
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        if depth == 0 and s.startswith(sep, i):
            out.append("".join(cur).strip())
            cur, i = [], i + n
            continue
        cur.append(ch)
        i += 1
    if "".join(cur).strip():
        out.append("".join(cur).strip())
    return out


def _closes_at_end(v: str) -> bool:
    """Is the bracket that opens `v` the one that closes it ("[a], [b]" is two lists, not one)?"""
    depth = 0
    for i, ch in enumerate(v):
        depth += ch in "([{"
        depth -= ch in ")]}"
        if depth == 0:
            return i == len(v) - 1
    return False


def list_of(v: str | None) -> list[str]:
    v = (v or "").strip()
    if v.startswith("[") and v.endswith("]") and _closes_at_end(v):
        v = v[1:-1]
    return [x for x in split_top(v) if x]


def lists_in(v: str) -> list[list[str]]:
    """The bracketed lists among the top-level parts of an argument string: "[a, b], [c], f(x), 1" -> [[a, b], [c]]."""
    return [list_of(x) for x in split_top(v) if x.startswith("[") and _closes_at_end(x)]


def _strip_parens(s: str) -> str:
    s = s.strip()
    while s.startswith("(") and s.endswith(")") and _closes_at_end(s):
        s = s[1:-1].strip()
    return s


def and_parts(cond: str) -> list[str]:
    """A condition split on its top-level ANDs: "((a AND b) AND c)" -> [a, b, c]."""
    s = _strip_parens(cond)
    parts = split_top(s, " AND ")
    if len(parts) == 1:
        return [s]
    return [p for x in parts for p in and_parts(x)]


def _or_safe(e: str) -> str:
    """An expression to AND with others: in brackets when it has a top-level OR (AND binds tighter)."""
    return f"({e})" if len(split_top(e, " OR ")) > 1 else e


def strip_ids(s: str) -> str:
    return EXPR_ID.sub("", s or "")


def null_safe_pairs(items: list[str]) -> list[tuple[str, bool]]:
    """Join keys, each with whether it is null-safe: Spark writes a <=> b as coalesce(a, <default>), isnull(a)."""
    out, i = [], 0
    while i < len(items):
        m = re.fullmatch(r"coalesce\((.+?), [^)]*\)", items[i])
        if m and i + 1 < len(items) and strip_ids(items[i + 1]) == f"isnull({strip_ids(m.group(1))})":
            out.append((m.group(1), True))
            i += 2
            continue
        out.append((items[i], False))
        i += 1
    return out


def null_safe_keys(items: list[str]) -> tuple[list[str], bool]:
    """Join keys with null-safe ones folded back, and whether any was null-safe."""
    ps = null_safe_pairs(items)
    return [k for k, _ in ps], any(f for _, f in ps)


_COL_NAME = re.compile(r"(?:.* AS )?`?([\w.]+)`?$")


def col_names(items: list[str]) -> set[str]:
    """The names a list of output expressions gives its columns ("a", "x + 1 AS b" -> {a, b})."""
    out = set()
    for x in items:
        m = _COL_NAME.match(strip_ids(x).strip())
        if m:
            out.add(m.group(1))
    return out


def ref_map(items: list[str]) -> dict[str, str]:
    """Expression id -> column name of a list of output expressions with ids ("a#1", "x#2 + 1 AS b#7")."""
    out = {}
    for x in items:
        m = re.search(r"(?:^| AS )`?([\w.]+)`?#(\d+)L?$", x.strip())
        if m:
            out[m.group(2)] = m.group(1)
    return out


def _lit(v: str) -> str:
    """A value the plan printed bare, as a SQL literal: numbers, booleans and NULL as they are, the rest quoted."""
    v = v.strip()
    if not v or _NUM.fullmatch(v) or v.lower() in ("true", "false", "null") or v.startswith("'"):
        return v
    return "'" + v.replace("'", "''") + "'"


def _named_structs(s: str) -> str:
    """struct(a, a#1, b, col#2.b) - how a plan prints named_struct - as named_struct('a', a#1, 'b', col#2.b)."""
    out, i = [], 0
    while True:
        m = re.compile(r"(?<![\w])struct\(").search(s, i)
        if not m:
            out.append(s[i:])
            return "".join(out)
        j, depth = m.end(), 1
        while j < len(s) and depth:
            depth += s[j] == "("
            depth -= s[j] == ")"
            j += 1
        inner = s[m.end():j - 1]
        args = split_top(inner)
        if len(args) >= 2 and len(args) % 2 == 0 and all(re.fullmatch(r"[A-Za-z_]\w*", a) for a in args[::2]) \
                and any("#" in a for a in args[1::2]):
            body = ", ".join(f"'{n}', {_named_structs(v)}" for n, v in zip(args[::2], args[1::2]))
            out.append(s[i:m.start()] + f"named_struct({body})")
        else:
            out.append(s[i:m.end()] + _named_structs(inner) + ")")
        i = j


def sql_expr(e: str, cols: set[str] | None = None, q: "_Sel | None" = None, all_sides: bool = False) -> str:
    """A plan expression as SQL. With expression ids: a value with no id is a literal (quoted), a column takes its
    join side when it is ambiguous (or always, `all_sides`), a scalar subquery its SELECT. Without ids (an older or
    hand-written plan): a bare word compared with a known column is a literal. Then isnotnull(x) -> x IS NOT NULL."""
    s = e or ""
    subs = _SUBS.get()
    if subs:
        s = _SUBQ.sub(lambda m: "(\n" + _indent(subs[m.group(1)]) + "\n)" if m.group(1) in subs else "(a scalar subquery)", s)
    if "#" in s:
        s = _CMP.sub(lambda m: f"{m.group(1)} {m.group(2)} {_lit(m.group(3))}" if m.group(3).strip() else m.group(0), s)
        s = _IN.sub(lambda m: f"{m.group(1)} IN ({', '.join(_lit(x) for x in split_top(m.group(2)))})", s)
        s = _named_structs(s)
        # a symbol the plan printed bare as a function argument (concat(a#1, @, b#2)) is a string
        s = re.sub(r"(?:(?<=\()|(?<=, ))([@:;|/%&!~^$=]+)(?=[,)])", lambda m: _lit(m.group(1)), s)
        s = re.sub(r", 0, 0\)(?=#\d+)", ")", s)  # an aggregate's buffer offsets (collect_set(x#1, 0, 0)#5)
        s = _REF.sub(lambda m: _ref(m.group(1), m.group(2), q, all_sides), s)
        s = EXPR_ID.sub("", s)
    elif cols:
        def lit(w: str) -> str:
            return w if w in cols else _lit(w)
        s = re.sub(r"(?<![\w.'])([\w.]+) (=|<>|!=|<=>) ([A-Za-z_][\w\-]*)(?=\)|$| AND| OR)",
                   lambda m: f"{m.group(1)} {m.group(2)} {lit(m.group(3)) if m.group(1) in cols else m.group(3)}", s)
        s = re.sub(r"\b([\w.]+) IN \(([^()]*)\)",
                   lambda m: f"{m.group(1)} IN ({', '.join(lit(x.strip()) for x in m.group(2).split(','))})"
                   if m.group(1) in cols else m.group(0), s)
    s = re.sub(r"\bisnotnull\(([\w.`]+)\)", r"\1 IS NOT NULL", s)
    s = re.sub(r"\bisnull\(([\w.`]+)\)", r"\1 IS NULL", s)
    s = re.sub(r"\bsize\(([^()]*), (?:true|false)\)", r"size(\1)", s)
    return re.sub(r"\b(\d+)L\b", r"\1", s)


def _ref(name: str, i: str, q: "_Sel | None", all_sides: bool) -> str:
    if q is None:
        return name
    base = q.rename.get(i, name)
    a = q.qual.get(i)
    if a and (all_sides or base in q.amb):
        return f"{a}.{base}"
    return base


def _item(raw: str, q: "_Sel | None") -> str:
    """One select-list item: "expr AS alias", a bare value as a literal (lit("x") prints as x AS col)."""
    parts = split_top(raw, " AS ")
    if len(parts) > 1:
        expr, alias = " AS ".join(parts[:-1]), strip_ids(parts[-1])
        if "#" not in expr and "(" not in expr:
            expr = _lit(expr)
        e = sql_expr(expr, q=q)
        return alias if e == alias else f"{e} AS {alias}"
    return sql_expr(raw, q=q)


def _cap(s: str) -> str:
    """One expression at most CAP characters (Delta's own, a MERGE's per-column CASE, can run to pages): the body is
    cut and its brackets closed; the alias stays."""
    if len(s) <= CAP:
        return s
    parts = split_top(s, " AS ")
    body, alias = (" AS ".join(parts[:-1]), parts[-1]) if len(parts) > 1 and len(parts[-1]) < 120 else (s, None)
    cut = body[:CAP]
    depth = max(0, cut.count("(") - cut.count(")"))
    return f"{cut} …{')' * depth}" + (f" AS {alias}" if alias else "")


# ---------------------------------------------------------------------------------------------------------------------
# the plan as nodes: id, name, kind, attrs (with expression ids), children
# ---------------------------------------------------------------------------------------------------------------------

def _kind(name: str) -> str:
    k = name.split(" ")[0].split("(")[0].split("[")[0]
    k = k.removesuffix("Exec")
    if k.startswith("Photon") and len(k) > 6:
        k = k[6:]
    return {"FileScan": "Scan", "BatchScan": "Scan", "RowDataSourceScan": "Scan"}.get(k, k)


def _simple_attrs(kind: str, text: str) -> dict[str, str]:
    """A simple-mode tree line's arguments as the attributes a formatted plan's detail section has."""
    args = text[len(text.split(" ")[0]):].strip() if " " in text else ""
    if kind == "Filter":
        return {"Condition": args}
    if kind == "Project":
        return {"Output": args}
    if kind in _AGGS:
        m = re.match(r"\(keys=(\[.*?\]), functions=(\[.*\])", text[text.find("("):]) if "(" in text else None
        if m:
            out = {"Keys": m.group(1), "Functions": m.group(2)}
            # "..., output=[k#1, total#5])": the aggregates' aliases, after the keys
            fo = re.match(r"(\[.*?\]), output=(\[.*\])\)?$", m.group(2))
            if fo:
                out["Functions"] = fo.group(1)
                keys, fns, res = list_of(m.group(1)), list_of(fo.group(1)), list_of(fo.group(2))
                if len(res) == len(keys) + len(fns):
                    out["Results"] = "[" + ", ".join(keys + [f"{f} AS {r}" for f, r in zip(fns, res[len(keys):])]) + "]"
            return out
        return {}
    if kind in JOINS:
        parts = split_top(args)
        out = {}
        if len(parts) >= 2 and parts[0].startswith("["):
            out["Left keys"], out["Right keys"] = parts[0], parts[1]
            rest = parts[2:]
        else:
            rest = parts
        if rest:
            out["Join type"] = rest[0]
        cond = [p for p in rest[1:] if p not in ("false", "true") and not p.startswith("Build")]
        if cond:
            out["Join condition"] = cond[-1]
        return out
    if kind == "TakeOrderedAndProject":
        return {"Arguments": text[text.find("(") + 1:-1] if "(" in text else args}
    if kind == "Scan":
        out = {}
        for k in ("PartitionFilters", "PushedFilters", "DataFilters"):
            m = re.search(rf"{k}: (\[[^\]]*\])", text)
            if m:
                out[k] = m.group(1)
        m = re.match(r"^\S+ \S+ [^\s\[]+\[([^\]]*)\]", text)
        if m:
            out["Output"] = f"[{m.group(1)}]"
        return out
    return {"Arguments": args}


def plan_nodes(plan: str | None) -> tuple[dict[int, dict[str, Any]], int | None]:
    """The plan's tree as {id: node} and the root id. A node: name, kind (Photon's names folded to Spark's), attrs
    (from its detail section, or its tree line in a simple-mode plan; expression ids kept) and children (left
    first). An adaptive plan's initial plan is skipped when it has a final (or current) one."""
    lines = (plan or "").splitlines()
    try:
        start = next(i for i, x in enumerate(lines) if x.strip() == "== Physical Plan ==") + 1
    except StopIteration:
        start = 0
    nodes: dict[int, dict[str, Any]] = {}
    stack: list[tuple[int, int]] = []  # (name column, id) of the nodes printed so far
    root: int | None = None
    auto = 100_000  # ids for simple-mode nodes, which have none
    skip_col: int | None = None
    formatted = any(_DETAIL_HEAD.match(x) for x in lines[start:])
    has_final = any("== Final Plan ==" in x or "== Current Plan ==" in x for x in lines)
    i = start
    while i < len(lines):
        line = lines[i].rstrip()
        i += 1
        if not line.strip():
            if nodes:
                break
            continue
        if "== Initial Plan ==" in line and has_final:
            skip_col = line.index("==")
            continue
        if "== Final Plan ==" in line or "== Current Plan ==" in line or "== Initial Plan ==" in line:
            skip_col = None
            continue
        m = _TREE.match(line)
        if not m:
            continue
        col = m.start("star") if m.group("star") else m.start("name")
        if skip_col is not None:
            if col >= skip_col:
                continue
            skip_col = None
        name = m.group("name").strip()
        if not formatted and m.group("id") is not None:
            name = f"{name} ({m.group('id')})"
        if formatted and m.group("id") is not None:
            nid, simple = int(m.group("id")), False
        else:
            nid, auto, simple = auto, auto + 1, True
        kind = _kind(name)
        node = {"id": nid, "name": strip_ids(name) if simple else name, "kind": kind, "children": [],
                "attrs": _simple_attrs(kind, name) if simple else {}}
        if simple:
            node["line"] = name
        if m.group("branch"):
            mcol = m.start("branch")
            while stack and stack[-1][0] > mcol:
                stack.pop()
            parent = next((p for c, p in reversed(stack) if c == mcol), None)
            if parent is None:
                parent = next((p for c, p in reversed(stack) if c < col), None)
        else:
            while stack and stack[-1][0] >= col:
                stack.pop()
            parent = stack[-1][1] if stack else None
        if nid in nodes:  # printed twice: keep the first
            continue
        nodes[nid] = node
        if parent is None and root is None:
            root = nid
        elif parent is not None:
            nodes[parent]["children"].append(nid)
        stack.append((col, nid))
    # the detail sections of a formatted plan
    cur: dict[str, Any] | None = None
    for raw in lines[i:]:
        line = raw.rstrip()
        if line.startswith("===== Subqueries"):
            break
        h = _DETAIL_HEAD.match(line)
        if h:
            cur = nodes.get(int(h.group(1)))
            if cur is not None:
                cur["name"] = h.group(2).strip()
            continue
        if cur is not None and ": " in line and not line.startswith(" "):
            k, v = line.split(": ", 1)
            k = re.sub(r"\s*\[\d+\]$", "", k.strip())
            cur["attrs"].setdefault(k, v.strip())
    return nodes, root


def _subqueries(plan: str) -> dict[str, str]:
    """The scalar subqueries of a formatted plan's "===== Subqueries =====" section as SQL, by name."""
    i = plan.find("===== Subqueries =====")
    if i < 0:
        return {}
    out: dict[str, str] = {}
    blocks = re.split(r"^Subquery:\d+ Hosting operator id = \d+ Hosting Expression = ", plan[i:], flags=re.M)[1:]
    for b in blocks:
        head, _, body = b.partition("\n")
        m = re.search(r"(scalar-subquery#\d+)", head)
        if not m or m.group(1) in out:
            continue
        nodes, root = plan_nodes(body)
        if root is not None:
            try:
                out[m.group(1)] = _render(_build(root, _Ctx(nodes)))
            except (RecursionError, ValueError, IndexError, KeyError, StopIteration, re.error):
                continue
    return out


# ---------------------------------------------------------------------------------------------------------------------
# windows
# ---------------------------------------------------------------------------------------------------------------------

_WIN_FN = re.compile(r"^(\w+)\((.*?)\)\s+windowspecdefinition\((.*)\)(?:\s+AS\s+`?(\w+)`?)?$")


def _frame(spec: str) -> tuple[str, str, str] | None:
    m = re.search(r"specifiedwindowframe\((.*)\)$", spec)
    if not m:
        return None
    parts = split_top(m.group(1))
    return (parts[0], parts[1], parts[2]) if len(parts) == 3 else None


def window_spec(kind: str, args: str) -> dict[str, Any] | None:
    """A Window / WindowGroupLimit / RunningWindowFunction node's arguments (expression ids stripped) -> {function,
    args, partition_by, order_by, alias, limit, frame}. WindowGroupLimit ([partition], [order], dense_rank(ts), 1,
    Final) is Spark keeping the top `limit` rows per partition before the window runs; a plain Window lists
    [fn(..) windowspecdefinition(.., specifiedwindowframe(..)) AS alias, ..], [partition], [order]."""
    if not args:
        return None
    parts = split_top(args)
    if kind == "WindowGroupLimit":
        lists = lists_in(args)
        if len(lists) < 2:
            return None
        fn = next((p for p in parts if re.match(r"^(\w+)\(", p) and not p.startswith("[")), None)
        lim = next((p for p in parts if re.fullmatch(r"\d+", p)), None)
        m = re.match(r"^(\w+)\((.*)\)$", fn or "")
        return {"function": m.group(1) if m else None, "args": m.group(2) if m else "", "partition_by": lists[0],
                "order_by": lists[1], "alias": None, "limit": int(lim) if lim else None, "frame": None}
    lists = [list_of(p) for p in parts if p.startswith("[") and _closes_at_end(p)]
    fns = [x for xs in lists for x in xs if "windowspecdefinition(" in x]
    if not fns:
        return None
    m = _WIN_FN.match(fns[0])
    first = next(xs for xs in lists if any("windowspecdefinition(" in x for x in xs))
    # a RunningWindowFunction lists its output columns first: the partition and order are the two lists after the
    # functions
    after = lists[lists.index(first) + 1:]
    pb, ob = (after + [[], []])[:2]
    out = {"function": m.group(1) if m else None, "args": m.group(2) if m else "", "partition_by": pb,
           "order_by": ob, "alias": m.group(4) if m else None, "limit": None,
           "frame": _frame(m.group(3)) if m else None,
           "all": [(g[0], g[1], g[3], _frame(g[2])) for g in (_WIN_FN.match(f).groups() for f in fns if _WIN_FN.match(f))]}
    if not pb and not ob and m:
        # no separate lists: read them off the spec (the order columns carry a direction)
        spec = [x for x in split_top(m.group(3)) if not x.startswith("specifiedwindowframe")]
        out["order_by"] = [x for x in spec if re.search(r" (ASC|DESC)( NULLS (FIRST|LAST))?$", x)]
        out["partition_by"] = [x for x in spec if x not in out["order_by"]]
    return out


def keeps_ties(w: dict[str, Any], filters: list[str] | None = None) -> bool:
    """A rank or dense_rank that keeps the first row per key keeps every row tied for first: two rows with the same
    order values both rank 1. Keeping first: a WindowGroupLimit of 1, or a filter `alias = 1` / `<= 1` / `< 2`."""
    if w.get("function") not in ("rank", "dense_rank"):
        return False
    if w.get("limit") == 1:
        return True
    a = w.get("alias")
    return bool(a) and any(re.search(rf"\b{re.escape(a)} (= 1|<= 1|< 2)\b", f) for f in filters or [])


# ---------------------------------------------------------------------------------------------------------------------
# the plan as SQL
# ---------------------------------------------------------------------------------------------------------------------

class _Sel:
    """One SELECT being built from the bottom up."""

    def __init__(self, src: str, bare: bool = True):
        self.src = src
        self.bare = bare  # src is a plain name, usable as a FROM item as it is
        self.cols: list[str] | None = None
        self.except_cols: list[str] = []
        self.adds: list[str] = []  # computed columns next to * (a wide row's new ones)
        self.wins: list[dict[str, Any]] = []
        self.laterals: list[str] = []
        self.where: list[str] = []
        self.group: list[str] | None = None
        self.gsets: list[list[str]] | None = None
        self.aggs: list[str] = []
        self.order: list[str] | None = None
        self.limit: int | None = None
        self.union: list[_Sel] | None = None
        self.notes: list[str] = []
        self.known: set[str] = set()
        self.outs: dict[str, str] = {}  # expression id -> column name of what it outputs
        self.qual: dict[str, str] = {}  # expression id -> the join alias it comes from
        self.amb: set[str] = set()  # names more than one side of a join has
        self.rename: dict[str, str] = {}  # expression id -> the name a subquery gave it (t2.ts AS ts_t2)
        self.joined = False  # src is a chain of joins another join can extend
        self.expand: tuple[list[list[str]], list[str]] | None = None  # an Expand's projections and output, for a rollup

    def plain(self) -> bool:
        return (self.bare and not (self.cols or self.except_cols or self.adds or self.wins or self.laterals or self.where
                                   or self.group is not None or self.order or self.limit is not None or self.union
                                   or self.notes))

    def open_join(self) -> bool:
        """A join chain with nothing on top yet: another join can extend it."""
        return self.joined and not (self.cols or self.except_cols or self.adds or self.wins or self.laterals or self.where
                                    or self.group is not None or self.order or self.limit is not None)


class _Ctx:
    def __init__(self, nodes: dict[int, dict[str, Any]]):
        self.nodes = nodes
        self.n = 0

    def alias(self, base: str = "t") -> str:
        self.n += 1
        return f"{base}{self.n}"


def _indent(s: str, by: str = "  ") -> str:
    return "\n".join(by + x if x else x for x in s.splitlines())


def _render(q: _Sel) -> str:
    out = [f"-- {n}" for n in dict.fromkeys(q.notes)]
    if q.union is not None and not (q.cols or q.where or q.wins or q.group is not None or q.order or q.limit is not None
                                    or q.laterals or q.except_cols or q.adds):
        return "\n".join(out + ["\nUNION ALL\n".join(_render(u) for u in q.union)])
    sel = list(q.cols) if q.cols else (list(q.group or []) + q.aggs if q.group is not None else ["*"])
    if q.except_cols and sel == ["*"]:
        sel = [f"* EXCEPT ({', '.join(q.except_cols)})"]
    if q.adds and not q.cols and q.group is None:
        sel += q.adds
    sel = [_cap(re.sub(r"^([\w.]+) AS \1$", r"\1", x)) for x in sel]
    sel += [_win_sql(w) for w in q.wins]
    more = [x for x in sel if x.startswith("/*")]
    sel = [x for x in sel if not x.startswith("/*")]
    if len(sel) <= 3 and sum(len(x) for x in sel) <= 90 and not more:
        out.append("SELECT " + ", ".join(sel))
    else:
        out.append("SELECT\n" + ",\n".join(_indent(x) for x in sel) + "".join(f" {m}" for m in more))
    src = q.src
    if q.union is not None:
        src = "(\n" + _indent("\nUNION ALL\n".join(_render(u) for u in q.union)) + "\n) AS u"
    out.append("FROM " + src)
    out.extend(q.laterals)
    if q.where:
        out.append("WHERE " + "\n  AND ".join(q.where))
    if q.gsets is not None:
        out.append("GROUP BY GROUPING SETS (" + ", ".join(f"({', '.join(s)})" for s in q.gsets) + ")")
    elif q.group:
        out.append("GROUP BY " + ", ".join(q.group))
    if q.order:
        out.append("ORDER BY " + ", ".join(q.order))
    if q.limit is not None:
        out.append(f"LIMIT {q.limit}")
    return "\n".join(out)


_BOUND = {"unboundedpreceding$()": "UNBOUNDED PRECEDING", "unboundedfollowing$()": "UNBOUNDED FOLLOWING",
          "currentrow$()": "CURRENT ROW"}


def _bound(b: str) -> str:
    b = b.strip()
    if b in _BOUND:
        return _BOUND[b]
    return f"{b[1:]} PRECEDING" if b.startswith("-") else f"{b} FOLLOWING"


def _order_sql(o: str) -> str:
    return re.sub(r" ASC NULLS FIRST$| DESC NULLS LAST$", lambda m: "" if "ASC" in m.group(0) else " DESC", o)


def _win_sql(w: dict[str, Any]) -> str:
    fn = w.get("function") or "window_function"
    args = "" if fn in RANKING and fn != "ntile" else (w.get("args") or "")
    over = []
    if w.get("partition_by"):
        over.append("PARTITION BY " + ", ".join(w["partition_by"]))
    if w.get("order_by"):
        over.append("ORDER BY " + ", ".join(_order_sql(o) for o in w["order_by"]))
    fr = w.get("frame")
    if fr and fn not in RANKING and fn not in ("lag", "lead"):
        lo, hi = fr[1].strip(), fr[2].strip()
        default = ((fr[0] == "RangeFrame" and lo == "unboundedpreceding$()" and hi == "currentrow$()") if w.get("order_by")
                   else (lo == "unboundedpreceding$()" and hi == "unboundedfollowing$()"))
        if not default:
            over.append(f"{'ROWS' if fr[0] == 'RowFrame' else 'RANGE'} BETWEEN {_bound(lo)} AND {_bound(hi)}")
    return f"{fn}({args}) OVER ({' '.join(over)}) AS {w.get('alias') or 'rnk'}"


def _disambiguate(q: _Sel) -> None:
    """A join chain read as a subquery: SELECT * would give two columns the same name. The first side keeps its
    names; another side's same-named column is renamed after its alias (t2.ts AS ts_t2), and the layers above follow."""
    if not (q.joined and q.amb and not q.cols and q.group is None and q.union is None):
        return
    order = list(dict.fromkeys(q.qual.values()))
    seen: set[str] = set()
    cols = []
    for a in order:
        ids = [i for i, x in q.qual.items() if x == a]
        names = {i: q.rename.get(i, q.outs.get(i, "")) for i in ids}
        clash = sorted({n for n in names.values() if n in seen})
        cols.append(f"{a}.* EXCEPT ({', '.join(clash)})" if clash else f"{a}.*")
        for i, n in names.items():
            if n in clash:
                q.rename[i] = f"{n}_{a}"
                cols.append(f"{a}.{n} AS {n}_{a}")
        seen |= set(names.values())
    q.cols = cols


def _wrap(q: _Sel, ctx: _Ctx) -> _Sel:
    if q.plain():
        return q
    _disambiguate(q)
    n = _Sel("(\n" + _indent(_render(q)) + f"\n) AS {ctx.alias()}", bare=False)
    n.known = set(q.known) | {w.get("alias") for w in q.wins if w.get("alias")}
    if q.cols:
        n.known |= col_names(q.cols)
    n.outs, n.rename = dict(q.outs), dict(q.rename)
    return n


def _from_item(q: _Sel, ctx: _Ctx) -> tuple[str, str]:
    """A side of a join as a FROM item and its alias."""
    a = ctx.alias()
    if q.plain():
        return f"{q.src} AS {a}", a
    _disambiguate(q)
    return "(\n" + _indent(_render(q)) + f"\n) AS {a}", a


_PUSHED = {"EqualTo": "=", "EqualNullSafe": "<=>", "GreaterThan": ">", "GreaterThanOrEqual": ">=", "LessThan": "<",
           "LessThanOrEqual": "<="}


def _pushed(f: str) -> str | None:
    """A data source filter (JDBC, DSv2: PushedFilters [*EqualTo(status,active)]) as SQL."""
    m = re.fullmatch(r"(\w+)\((.*)\)", f.strip().lstrip("*"))
    if not m:
        return None
    op, args = m.group(1), split_top(m.group(2))
    if op in _PUSHED and len(args) == 2:
        return f"{args[0]} {_PUSHED[op]} {_lit(args[1])}"
    if op in ("IsNotNull", "IsNull") and len(args) == 1:
        return f"{args[0]} IS {'NOT ' if op == 'IsNotNull' else ''}NULL"
    if op == "In" and len(args) == 2:
        return f"{args[0]} IN ({', '.join(_lit(x) for x in list_of(args[1]))})"
    if op in ("StringStartsWith", "StringEndsWith", "StringContains") and len(args) == 2:
        v = args[1].replace("'", "''")
        pat = {"StringStartsWith": f"{v}%", "StringEndsWith": f"%{v}", "StringContains": f"%{v}%"}[op]
        return f"{args[0]} LIKE '{pat}'"
    if op in ("And", "Or") and len(args) == 2:
        a, b = _pushed(args[0]), _pushed(args[1])
        return f"({a} {op.upper()} {b})" if a and b else None
    if op == "Not" and len(args) == 1:
        a = _pushed(args[0])
        return f"NOT ({a})" if a else None
    return None


def _source(name: str) -> tuple[str, str | None]:
    """What a leaf node reads, as a FROM item, and a note."""
    n = " ".join(strip_ids(name).split())
    m = re.match(r"^(?:Photon)?(?:Scan|FileScan|BatchScan) (?:parquet|delta|orc|csv|json|text|avro)\s+([^\s\[]+)", n, re.I)
    if m:
        return m.group(1), None
    if "mergeMaterializedSource" in n:
        return "merge_source_copy", "merge_source_copy: the MERGE's copy of its source rows"
    if re.match(r"^(?:Photon)?Scan ExistingRDD|^Scan RDD|^(?:Photon)?RDDScan|^ExistingRDD", n):
        return "dataframe_rows", "dataframe_rows: rows of a DataFrame made in code (e.g. a stream micro-batch in foreachBatch)"
    if "JDBCRelation" in n:
        m = re.search(r"JDBCRelation\((.*?)\)\s*\[", n)
        src = (m.group(1) if m else "the source database").strip()
        if re.fullmatch(r"[\w.\"`\[\]]+", src):
            return src, f"{src}: read from the source database over JDBC"
        return "jdbc_source", f"jdbc_source: read over JDBC from {src[:200]}"
    if n.startswith(("InMemoryTableScan", "InMemoryRelation")):
        return "cached_dataframe", "cached_dataframe: a DataFrame .cache() / .persist()"
    if n.startswith("LocalTableScan"):
        return "local_rows", "local_rows: rows held in the driver (a small DataFrame or a literal list)"
    if n.startswith("Range"):
        return "range_rows", None
    if n.startswith(("LogicalQueryStage", "QueryStage", "TableCacheQueryStage")):
        return "earlier_stage_rows", "earlier_stage_rows: the output of a stage adaptive execution had already run"
    if n.startswith(("EmptyRelation", "LocalRelation")):
        return "empty_rows", "empty_rows: no rows (adaptive execution found an empty side)"
    m = re.match(r"^(?:Photon)?Scan (\S+)", n)
    if m:
        return re.sub(r"\W+", "_", m.group(1)).strip("_").lower() or "source", None
    return re.sub(r"\W+", "_", n.split(" ")[0]).lower() or "source", None


def _agg_fn(f: str) -> str:
    """An aggregate function as written: partial_count(1) -> count(1); Databricks' final form
    "finalmerge_count(merge count) AS count(1)" -> count(1); the buffer offsets Spark adds (collect_set(x, 0, 0)) go."""
    m = re.match(r"^(?:finalmerge_|merge_)\w+\(.*?\) AS (.+)$", f)
    f = m.group(1) if m else re.sub(r"^(partial_|merge_|finalmerge_)", "", f)
    return re.sub(r", 0, 0\)$", ")", f)


def _cols_of(node: dict[str, Any]) -> set[str]:
    a = node["attrs"]
    return col_names(list_of(a.get("Input") or a.get("Output") or ""))


def _outs_of(node: dict[str, Any]) -> dict[str, str]:
    a = node["attrs"]
    return ref_map(list_of(a.get("Output") or a.get("Input") or ""))


def _alias_aggs(q: _Sel, out: list[str]) -> bool:
    """An aggregate with no aliases (a simple-mode plan) under a node that names its output ([k#1, total#5]): the
    names after the keys are the aggregates', in order. True when that named them."""
    names = [strip_ids(x) for x in out]
    if q.group is None or q.cols is not None or len(names) != len(q.group) + len(q.aggs) or names[:len(q.group)] != q.group \
            or any(" AS " in x or "(" in x for x in names):
        return False
    q.cols = list(q.group) + [f"{f} AS {n}" for f, n in zip(q.aggs, names[len(q.group):])]
    return True


def _is_final_agg(fns: list[str]) -> bool:
    return not any(strip_ids(f).startswith(("partial_", "merge_")) for f in fns)


def _build(nid: int, ctx: _Ctx, depth: int = 0) -> _Sel:
    node = ctx.nodes[nid]
    kind, a, name = node["kind"], node["attrs"], node["name"]
    kids = [k for k in node["children"] if k in ctx.nodes]
    if depth > 200:
        return _Sel("deep_plan")
    if kind == "ReusedExchange" or name.startswith("ReusedExchange"):
        m = re.search(r"Reuses operator id: (\d+)", name + " " + " ".join(f"{k}: {v}" for k, v in a.items()))
        if m and int(m.group(1)) in ctx.nodes and int(m.group(1)) != nid:
            q = _build(int(m.group(1)), ctx, depth + 1)
            # the reused rows under the ids this node outputs
            outs = _outs_of(node)
            if outs:
                q.rename.update({i: n for i, n in outs.items()})
                q.outs = outs
            return q
    if not kids:
        src, note = _source(node.get("line") or name)
        q = _Sel(src)
        q.known = _cols_of(node)
        q.outs = _outs_of(node)
        if note:
            q.notes.append(note)
        pf = [x for x in list_of(a.get("PartitionFilters")) if not INTERNAL.search(strip_ids(x))]
        for x in pf:
            m = re.fullmatch(r"isnotnull\(([\w.`]+#\d+L?)\)", x)
            if m and any(m.group(1) in y for y in pf if y != x):
                continue  # inferred from the condition next to it
            e = _or_safe(sql_expr(_strip_parens(x), q.known, q))
            if e not in q.where:
                q.where.append(e)
        for f in list_of(a.get("PushedFilters")):
            if f.startswith("*"):  # handled by the source itself: no Filter above it says so
                e = _pushed(strip_ids(f))
                if e and e not in q.where:
                    q.where.append(_or_safe(e))
        return q
    if kind in JOINS:
        return _join(node, kids, ctx, depth)
    if kind == "Union":
        parts = [_build(k, ctx, depth + 1) for k in kids]
        q = _Sel("", bare=False)
        q.union = parts
        q.known = parts[0].known if parts else set()
        q.outs = _outs_of(node) or (parts[0].outs if parts else {})
        return q
    q = _build(kids[0], ctx, depth + 1)
    if len(kids) > 1:
        q.notes.append(f"{name.split(' ')[0]} also reads {len(kids) - 1} more input(s), left out here")
    mine = _cols_of(node)
    # a window's alias is the column the layer above it adds
    for w in q.wins:
        if not w.get("alias") and w.get("_base") is not None:
            new = [c for c in (col_names(list_of(a.get("Input") or "")) - w["_base"]) if not INTERNAL.search(c)]
            if len(new) == 1:
                w["alias"] = new[0]
    q.known |= mine
    out = _apply(node, q, ctx)
    if out is not q or kind not in _AGGS:
        ids = _outs_of(node)
        if ids and kind not in ("Generate", "Expand"):
            out.outs = ids if not out.qual else {**out.outs, **ids}
    return out


def _join(node: dict[str, Any], kids: list[int], ctx: _Ctx, depth: int) -> _Sel:
    a, name, kind = node["attrs"], node["name"], node["kind"]
    sides = [_build(k, ctx, depth + 1) for k in kids[:2]]
    if len(sides) < 2:
        return sides[0]
    left, right = sides
    if left.open_join():
        lf, lqual = left.src, dict(left.qual)
    else:
        lf, la = _from_item(left, ctx)
        lqual = {i: la for i in left.outs}
    rf, ra = _from_item(right, ctx)
    j = _Sel("", bare=False)
    j.qual = {**lqual, **{i: ra for i in right.outs}}
    j.outs = {**left.outs, **right.outs}
    j.rename = {**left.rename, **right.rename}
    by_alias: dict[str, set[str]] = {}
    for i, al in j.qual.items():
        by_alias.setdefault(j.rename.get(i, j.outs.get(i, "")), set()).add(al)
    j.amb = {n for n, als in by_alias.items() if n and len(als) > 1}
    j.known = left.known | right.known
    jt_raw = (a.get("Join type") or (name.split(" ")[1] if len(name.split(" ")) > 1 else "Inner")).strip()
    jt = strip_ids(jt_raw)
    on = []
    for (lk, f1), (rk, f2) in zip(null_safe_pairs(list_of(a.get("Left keys"))), null_safe_pairs(list_of(a.get("Right keys")))):
        on.append(f"{sql_expr(lk, q=j, all_sides=True)} {'<=>' if f1 or f2 else '='} {sql_expr(rk, q=j, all_sides=True)}")
    cond = a.get("Join condition")
    if cond and cond != "None":
        on.extend(_or_safe(sql_expr(p, q=j, all_sides=True)) for p in and_parts(cond) if not INTERNAL.search(strip_ids(p)))
    if jt.startswith("ExistenceJoin"):
        # EXISTS (..) OR ..: every left row stays, with a flag
        m = re.search(r"ExistenceJoin\((\w+)#?(\d*)", jt_raw)
        flag = m.group(1) if m else "exists"
        q = _Sel(lf if left.open_join() else lf, bare=False)
        la_ = next(iter(dict.fromkeys(lqual.values())), "t")
        q.cols = [f"{la_}.*" if not left.open_join() else "*",
                  f"EXISTS (SELECT 1 FROM {rf} WHERE {' AND '.join(on) or 'TRUE'}) AS {flag}"]
        q.outs = dict(left.outs)
        if m and m.group(2):
            q.outs[m.group(2)] = flag
        q.qual, q.rename, q.known = lqual, dict(left.rename), left.known | {flag}
        return q
    jsql = {"Inner": "JOIN", "LeftOuter": "LEFT JOIN", "RightOuter": "RIGHT JOIN", "FullOuter": "FULL JOIN",
            "LeftSemi": "LEFT SEMI JOIN", "LeftAnti": "LEFT ANTI JOIN", "Cross": "CROSS JOIN"}.get(jt, "JOIN")
    if jsql in ("LEFT SEMI JOIN", "LEFT ANTI JOIN"):
        # the right side's columns are not in the result
        j.qual, j.outs = lqual, dict(left.outs)
        j.amb = set()
    if kind in ("BroadcastNestedLoopJoin", "CartesianProduct") and not on:
        jsql = "CROSS JOIN"
    j.src = f"{lf}\n{jsql} {rf}" + (("\n  ON " + "\n  AND ".join(on)) if on and jsql != "CROSS JOIN" else "")
    j.joined = True
    return j


def _apply(node: dict[str, Any], q: _Sel, ctx: _Ctx) -> _Sel:
    """One operator on top of the SELECT built so far."""
    kind, a, name = node["kind"], node["attrs"], node["name"]
    mine = _cols_of(node)
    busy = lambda: q.group is not None or q.limit is not None or q.cols or q.adds or q.union is not None or q.order  # noqa: E731
    if kind == "Filter":
        raw = [p for p in and_parts(a.get("Condition") or "") if p and not INTERNAL.search(strip_ids(p))]
        # Spark's inferred "x IS NOT NULL" next to a null-rejecting condition on x says nothing more
        parts = []
        for p in raw:
            m = re.fullmatch(r"isnotnull\(([\w.`]+(?:#\d+L?)?)\)", p)
            if m and any(m.group(1) in o and len(split_top(_strip_parens(o), " OR ")) == 1 for o in raw if o != p) \
                    and ("#" in m.group(1) or len(re.findall(rf"\b{re.escape(m.group(1))}\b", " ".join(raw))) > 1):
                continue
            parts.append(p)
        if not parts:
            return q
        refs = strip_ids(" ".join(parts))
        wins = {w.get("alias") for w in q.wins}
        if busy() or (q.wins and any(re.search(rf"\b{re.escape(x)}\b", refs) for x in wins if x)):
            q = _wrap(q, ctx)
        for p in parts:
            e = _or_safe(sql_expr(p, q.known | mine, q))
            if e not in q.where:
                q.where.append(e)
        return q
    if kind in _WINDOWS:
        w = window_spec(kind, strip_ids(a.get("Arguments") or ""))
        if w is None:
            if kind == "RunningWindowFunction" and not q.wins:
                q.notes.append("a running window function (its expression is not in the plan)")
            return q
        # the limit that keeps the top rows (Partial, then Final) and the Window that computes the rank: one window
        same = next((x for x in q.wins if x.get("function") == w["function"] and x.get("partition_by") == w["partition_by"]
                     and x.get("order_by") == w["order_by"]
                     and (not x.get("alias") or not w.get("alias") or x["alias"] == w["alias"])), None)
        if same:
            same["alias"] = same.get("alias") or w.get("alias")
            same["limit"] = same.get("limit") or w.get("limit")
            same["frame"] = same.get("frame") or w.get("frame")
            return q
        if busy():
            q = _wrap(q, ctx)
        w["_base"] = col_names(list_of(a.get("Input") or a.get("Output") or "")) or None
        q.wins.append(w)
        for f in (w.get("all") or [])[1:]:
            q.wins.append({"function": f[0], "args": f[1], "partition_by": w["partition_by"], "order_by": w["order_by"],
                           "alias": f[2], "limit": None, "frame": f[3]})
        return q
    if kind in _AGGS:
        keys_raw = list_of(a.get("Keys"))
        fns_raw = list_of(a.get("Functions"))
        keys = [sql_expr(k, q=q) for k in keys_raw]
        fns = [_agg_fn(sql_expr(f, q=q)) for f in fns_raw]
        results = [r for r in list_of(a.get("Results")) if not INTERNAL.search(strip_ids(r))]
        final = _is_final_agg(fns_raw)
        gid = [k for k in keys if k == "spark_grouping_id"]
        plain_keys = [k for k in keys if k != "spark_grouping_id"]
        if not (q.group is not None and (q.group == keys or (gid and q.group == plain_keys))):
            if q.group is not None or q.limit is not None or q.cols or q.adds or q.wins or q.union is not None or q.order:
                q = _wrap(q, ctx)
            q.except_cols = []
            q.group = keys
            if gid and q.expand:
                # a rollup / cube / grouping sets: Spark copies each row once per set (Expand), nulls the keys a set
                # leaves out, and groups by the keys and the set's id
                projs, outcols = q.expand
                ks = [k for k in keys if k != "spark_grouping_id"]
                pos = {k: outcols.index(k) for k in ks if k in outcols}
                q.gsets = [[k for k in ks if k in pos and pos[k] < len(p) and p[pos[k]] != "null"] for p in projs]
                q.group = ks
        q.aggs = fns or q.aggs
        if final and results:
            # the select list as Spark computed it: the keys and each aggregate with its alias (sum(amount) AS total)
            q.cols = [x for x in (_item(r, q) for r in results) if x != "spark_grouping_id"]
            q.outs = ref_map(list_of(a.get("Results")))
            q.known |= col_names(results)
        if not keys and not fns:
            q.group = None
        return q
    if kind == "Project":
        out = list_of(a.get("Output") or a.get("Arguments") or "")
        if not out or all(INTERNAL.search(strip_ids(x)) for x in out) or any("(unknown)" in x for x in out):
            return q  # nothing, or Delta's bookkeeping columns only
        if len(out) <= 15 and not any(INTERNAL.search(strip_ids(x)) for x in out):
            if q.group is not None and q.cols is not None and col_names(out) <= (col_names(q.cols) | set(q.group)) \
                    and all(" AS " not in x for x in out):
                # only picks or reorders what the aggregate gave
                by = {c: x for x in q.cols for c in col_names([x])}
                q.cols = [by.get(strip_ids(x), strip_ids(x)) for x in out]
                return q
            if q.group is not None and not q.cols:
                if not _alias_aggs(q, out):
                    q.cols = [_item(x, q) for x in out]  # the aggregate's own select list
                return q
            if q.cols or q.wins or q.except_cols or q.union is not None or q.group is not None:
                q = _wrap(q, ctx)
            q.cols = [_item(x, q) for x in out]
            return q
        # a wide row: name only the columns it drops (a window's rank once it filtered on it) and the ones it computes
        ins = col_names(list_of(a.get("Input")))
        dropped = sorted(c for c in ins - col_names(out) if not INTERNAL.search(c))
        adds = [x for x in out if " AS " in x and not INTERNAL.search(strip_ids(x)) and col_names([x]) - ins
                and not re.fullmatch(r"([\w.]+) AS \1", strip_ids(x))]
        if (0 < len(dropped) <= 5 or adds) and q.group is None and ins:
            aliases = {w.get("alias") for w in q.wins}
            if q.cols or q.except_cols or q.adds or q.union is not None or aliases & (set(dropped) | col_names(adds)):
                q = _wrap(q, ctx)
            q.except_cols = dropped if len(dropped) <= 5 else []
            q.adds = [_item(x, q) for x in adds[:12]] + ([f"/* … {len(adds) - 12} more */"] if len(adds) > 12 else [])
        return q
    if kind == "Sort":
        parts = split_top(strip_ids(a.get("Arguments") or ""))
        if len(parts) >= 2 and parts[1].strip() == "true":
            if q.limit is not None:
                q = _wrap(q, ctx)
            q.order = [_order_sql(x) for x in list_of(parts[0])]
        return q
    if kind == "TakeOrderedAndProject":
        args = strip_ids(a.get("Arguments") or "")
        lim = re.search(r"limit=(\d+)", args)
        ob = re.search(r"orderBy=(\[.*?\])(?:, output=|$)", args)
        output = re.search(r"output=(\[.*\])$", a.get("Arguments") or "")
        if lim:
            n, order = int(lim.group(1)), list_of(ob.group(1)) if ob else []
            if output:
                _alias_aggs(q, list_of(output.group(1)))
        else:
            # formatted mode: limit, [order], [output] (Spark 3.4+ puts an offset after the limit)
            parts = split_top(args)
            nums = [p for p in parts if re.fullmatch(r"\d+", p)]
            lists = [list_of(p) for p in parts if p.startswith("[")]
            n, order = (int(nums[0]) if nums else None), (lists[0] if lists else [])
            if len(lists) > 1:
                _alias_aggs(q, lists[1])
        if q.limit is not None or (q.order and order):
            q = _wrap(q, ctx)
        if order:
            q.order = [_order_sql(x) for x in order]
        if n is not None:
            q.limit = n
        return q
    if kind in ("GlobalLimit", "CollectLimit", "LocalLimit"):
        m = re.match(r"^\s*(\d+)", a.get("Arguments") or "")
        if m:
            if q.limit is not None and q.limit != int(m.group(1)):
                q = _wrap(q, ctx)
            q.limit = int(m.group(1))
        return q
    if kind == "Generate":
        parts = split_top(a.get("Arguments") or "")
        gen = parts[0] if parts else "explode(..)"
        outer = len(parts) > 2 and parts[2].strip() == "true"
        cols = [strip_ids(c) for c in list_of(parts[3])] if len(parts) > 3 else []
        if INTERNAL.search(strip_ids(gen)) or "_target_row_present_" in gen or "packedCdc" in gen:
            note = "Delta MERGE: each joined row becomes the row to insert, update, delete or keep"
            if "packedCdc" in gen:
                q.notes = [n for n in q.notes if n != note]
                note += " (and its change data feed rows)"
            if not any(n.startswith(note) for n in q.notes):
                q.notes.append(note)
            return q
        if q.where or q.group is not None or q.wins or q.cols or q.adds or q.limit is not None or q.union is not None \
                or q.except_cols:
            q = _wrap(q, ctx)
        q.laterals.append(f"LATERAL VIEW {'OUTER ' if outer else ''}{_cap(sql_expr(gen, q=q))} {ctx.alias('g')} "
                          f"AS {', '.join(cols) or 'col'}")
        q.known |= set(cols)
        if len(parts) > 3:
            q.outs = {**q.outs, **ref_map(list_of(parts[3]))}
        return q
    if kind == "Expand":
        lists = lists_in(strip_ids(a.get("Arguments") or ""))
        if len(lists) >= 2 and lists[1] and lists[1][-1] == "spark_grouping_id":
            q.expand = ([list_of(p) for p in lists[0]], lists[1])
        else:
            q.notes.append("Expand: each row is copied once per DISTINCT aggregate")
        return q
    if _PYTHON.search(kind):
        fns = re.findall(r"(\w+)\(", strip_ids(a.get("Arguments") or node.get("line") or ""))
        q.notes.append(f"{kind}: runs Python code" + (f" ({', '.join(dict.fromkeys(fns[:3]))})" if fns else ""))
        return q
    if kind in ("MapPartitions", "MapElements", "MapGroups", "FlatMapGroupsWithState", "AppendColumns"):
        q.notes.append(f"{kind}: runs Scala / Java code on the rows (a Dataset map)")
        return q
    if kind == "WriteFiles":
        return q  # the command above it says what it writes
    if _WRITES.search(name):
        q.notes.append(f"then writes the rows: {name.split(',')[0][:160]}")
        return q
    if kind == "EmptyRelation":
        q.notes.append("adaptive execution found no rows to read: the query returned nothing")
        return q
    if kind not in _QUIET:
        q.notes.append(f"also: {name.split(',')[0][:100]}")
    return q


def _is_command(name: str) -> bool:
    n = name.strip()
    return n.startswith("Execute ") or n.startswith("CommandResult") or bool(re.match(r"^(?:Photon)?(CreateTable|"
                                                                                   r"ReplaceTable|MergeIntoCommand)", n))


def plan_sql(plan: str | None) -> str | None:
    """The SQL a physical plan amounts to (see the module notes), or None when the plan has no tree to read."""
    if not plan or "== Physical Plan ==" not in plan:
        return None
    token = _SUBS.set({})
    try:
        nodes, root = plan_nodes(plan)
        if root is None or not nodes:
            return None
        # a command with no query under it (CREATE TABLE, the MERGE command that runs its steps as other queries)
        cmd = next((n["name"] for n in nodes.values() if _is_command(n["name"])), None)
        reads = [n for n in nodes.values() if not n["children"] and not _is_command(n["name"])
                 and n["kind"] not in ("LocalTableScan", "CommandResult")]
        if cmd and not reads:
            return f"-- a command with no query of its own: {cmd.split(',')[0][:200]}"
        if all(n["kind"] in _QUIET and not n["children"] for n in nodes.values()) and len(nodes) == 1:
            return None  # only the adaptive plan's wrapper
        _SUBS.set(_subqueries(plan))
        q = _build(root, _Ctx(nodes))
        if q.plain() and q.src in ("adaptivesparkplan", "source"):
            return None
        sql = _render(q)
    except (RecursionError, ValueError, IndexError, KeyError, StopIteration, re.error):
        return None
    finally:
        _SUBS.reset(token)
    return sql if len(sql) <= SQL_MAX else sql[:SQL_MAX] + "\n-- ... (cut)"
