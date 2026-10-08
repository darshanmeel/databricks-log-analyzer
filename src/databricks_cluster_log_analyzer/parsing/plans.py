"""Physical plan text -> operator summary, tables read and tables written (Revision 4, `sql_queries`).

Handles the two shapes `physicalPlanDescription` comes in:

* simple mode (tree only)::

      AdaptiveSparkPlan isFinalPlan=true
      +- == Final Plan ==
         Exchange hashpartitioning(customer_id#12, 200), ENSURE_REQUIREMENTS, [plan_id=45]
         +- *(1) Project [customer_id#12, amount#13]
            +- FileScan parquet sales.orders[customer_id#12,amount#13] Batched: true, ...

* formatted mode (tree with node ids, a blank line, then one detail section per node)::

      AdaptiveSparkPlan (16)
      +- == Final Plan ==
         * HashAggregate (10)
         ...
      +- == Initial Plan ==
         ...

      (1) Scan parquet spark_catalog.default.t
      Location: InMemoryFileIndex [dbfs:/user/hive/warehouse/t]
      ...

Operators come from the tree only (the `== Initial Plan ==` part is skipped when a `== Final Plan ==` exists), in
data-flow order (bottom of the printed tree first), de-duplicated. Tables come from the tree lines and the detail
sections. Everything is best effort: an odd plan yields fewer names, never an exception.
"""

from __future__ import annotations

import re

OPERATORS_MAX = 40
LABEL_MAX = 200
TABLES_MAX = 20

_PREFIX_RE = re.compile(r"^([\s:|+\-]*)(.*)$")
_CONNECTOR_RE = re.compile(r"[+:]-")
_CODEGEN_RE = re.compile(r"^\*\s*(?:\(\d+\)\s*)?")
_NODE_ID_RE = re.compile(r"\s+\((\d+)\)(?=$|[\s,])")
_HEAD_RE = re.compile(r"^([A-Za-z_][\w$]*)")
_DETAIL_HEADER_RE = re.compile(r"^\((\d+)\)\s+(.+)$")
_SECTION_RE = re.compile(r"^==\s*(.+?)\s*==\s*$")
_QUALIFIED_RE = re.compile(r"^[A-Za-z_][\w\-]*(?:\.[A-Za-z_][\w\-]*){1,3}$")
_BACKTICK_ID_RE = re.compile(r"`[^`]+`(?:\.`[^`]+`)+")
_PATH_RE = re.compile(r"(?:dbfs|s3a?|s3n|abfss?|wasbs?|gs|file):/+[^\s,\]\)]+|/(?:Volumes|mnt|user|Workspace)/[^\s,\]\)]+")
_LOCATION_RE = re.compile(r"Location:\s*\w+(?:\([^)]*\))?\s*\[([^\],\s]+)")
_WAREHOUSE_RE = re.compile(r"/([^/]+)\.db/([^/]+?)/?$")
_PARTITIONING_RE = re.compile(r"^([A-Za-z]*(?:[Pp]artitioning|SinglePartition))")

_SCAN_HEADS = {"Scan", "FileScan", "PhotonScan", "RowDataSourceScan", "DataSourceScan"}
_NON_TABLE_FORMATS = {"ExistingRDD", "OneRowRelation", "LocalTableScan", "JDBCRelation", "RDD"}
_EXCHANGE_HEADS = {"Exchange", "ShuffleExchange", "PhotonShuffleExchangeSink", "PhotonShuffleExchangeSource"}
_WRITE_RE = re.compile(
    r"Insert|Write(?!Files)|Save|AppendData|OverwriteByExpression|OverwritePartitions|CreateTable|CreateDeltaTable|"
    r"ReplaceTable|TableAsSelect|MergeInto|DeleteFrom|UpdateTable|Merge(?:Into)?Command")


def _clean_table(t: str) -> str:
    t = t.strip().rstrip(",;")
    if "[" in t:
        t = t.split("[", 1)[0]
    return t.strip("`")


def _path_table(path: str) -> str:
    """Hive warehouse paths `.../<db>.db/<table>` read better as `db.table`; other paths stay as they are."""
    m = _WAREHOUSE_RE.search(path)
    return f"{m.group(1)}.{m.group(2)}" if m else path.rstrip("/")


def _node_text(line: str) -> tuple[int, str]:
    """(tree position, node text without tree drawing, codegen marker and node id)."""
    m = _PREFIX_RE.match(line)
    prefix, text = m.group(1), m.group(2).strip()
    conns = list(_CONNECTOR_RE.finditer(prefix))
    pos = conns[-1].start() if conns else len(prefix)
    text = _CODEGEN_RE.sub("", text)
    dm = _DETAIL_HEADER_RE.match(text)  # "(3) Name ..." (detail-style line where a tree line was expected)
    if dm:
        return pos, dm.group(2).strip()[:LABEL_MAX], dm.group(1)
    im = _NODE_ID_RE.search(text)
    nid = im.group(1) if im else None
    if im:
        text = text[:im.start()] + text[im.end():]
    return pos, text.strip()[:LABEL_MAX], nid


def _operator(text: str) -> tuple[str | None, str | None]:
    """(operator label, head word) for one node text."""
    text = _NODE_ID_RE.sub("", text, count=1)
    m = _HEAD_RE.match(text)
    if not m:
        return None, None
    head = m.group(1)
    rest = text[m.end():].strip()
    toks = rest.split()
    if head in _SCAN_HEADS:
        label = "Scan" if head == "FileScan" else head
        if toks:
            fmt = toks[0].split("[", 1)[0].split("(", 1)[0].rstrip(",")
            if fmt:
                label += " " + fmt
            if fmt not in _NON_TABLE_FORMATS and len(toks) > 1:
                t = _clean_table(toks[1])
                if t and (_QUALIFIED_RE.match(t) or t.isidentifier()):
                    label += " " + t
        return label, head
    if head == "BatchScan" and toks:
        t = _clean_table(toks[0])
        return (f"BatchScan {t}" if t else "BatchScan"), head
    if head in _EXCHANGE_HEADS and toks:
        pm = _PARTITIONING_RE.match(toks[0])
        if pm:
            return f"{head} {pm.group(1)}", head
        return head, head
    if head == "Execute" and toks:
        cm = _HEAD_RE.match(toks[0])
        return (f"Execute {cm.group(1)}" if cm else "Execute"), head
    return head, head


def _scan_table(text: str, body: str = "") -> str | None:
    m = _HEAD_RE.match(text)
    if not m:
        return None
    head = m.group(1)
    toks = text[m.end():].split()
    if head == "BatchScan":
        if toks:
            t = _clean_table(toks[0])
            if _QUALIFIED_RE.match(t) or t.isidentifier():
                return t
    elif head in _SCAN_HEADS:
        if toks and toks[0].split("[", 1)[0] in _NON_TABLE_FORMATS:
            return None
        if len(toks) > 1:
            t = _clean_table(toks[1])
            if t and (_QUALIFIED_RE.match(t) or t.isidentifier()):
                return t
    else:
        return None
    lm = _LOCATION_RE.search(text + "\n" + body)
    if lm:
        return _path_table(lm.group(1))
    return None


_UUID_RE = re.compile(r"(?:^|[./])[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


def _write_table(text: str, body: str = "") -> str | None:
    name = _write_table_raw(text, body)
    # a managed table's storage folder (a bare id, as a CTAS writes it) is not a name a reader knows
    return None if name and _UUID_RE.search(name) else name


def _write_table_raw(text: str, body: str = "") -> str | None:
    m = _HEAD_RE.match(text)
    if not m:
        return None
    head = m.group(1)
    rest = text[m.end():].strip()
    name = head
    if head == "Execute":
        cm = _HEAD_RE.match(rest)
        if not cm:
            return None
        name = cm.group(1)
        rest = rest[cm.end():].strip()
    if not _WRITE_RE.search(name):
        return None
    hay = rest + "\n" + body
    bm = _BACKTICK_ID_RE.search(hay)
    if bm:
        parts = [p.strip("`") for p in bm.group(0).split("`.`")]
        if len(parts) == 2 and parts[0].lower() in ("delta", "parquet") and "/" in parts[1]:
            return _path_table(parts[1])
        return ".".join(parts)
    first = rest.split(None, 1)[0].rstrip(",") if rest else ""
    first = first.split("[", 1)[0]
    if first and _QUALIFIED_RE.match(first):
        return first
    pm = _PATH_RE.search(hay)
    if pm:
        return _path_table(pm.group(0))
    return None


def _add(out: list[str], seen: set, v: str | None, cap: int) -> None:
    if v and v not in seen and len(out) < cap:
        seen.add(v)
        out.append(v)


def _split(plan: str) -> tuple[list[str], list[tuple[str | None, str, str]]]:
    """(tree lines, [(detail node id, detail header, detail body)])."""
    lines = plan.splitlines()
    i = 0
    while i < len(lines) and not lines[i].strip():
        i += 1
    if i < len(lines) and lines[i].strip().startswith("== Physical Plan"):
        i += 1
    tree: list[str] = []
    while i < len(lines) and lines[i].strip():
        s = lines[i].strip()
        if s.startswith("=====") or (_SECTION_RE.match(s) and "Plan" not in s):
            break  # e.g. "===== Subqueries =====" in formatted mode
        tree.append(lines[i])
        i += 1
    details: list[tuple[str | None, str, str]] = []
    nid, header, body = None, None, []
    for ln in lines[i:]:
        s = ln.strip()
        if s.startswith("====="):  # "===== Subqueries =====" etc.: stop
            break
        m = _DETAIL_HEADER_RE.match(s)
        if m:
            if header is not None:
                details.append((nid, header, "\n".join(body)))
            nid, header, body = m.group(1), m.group(2)[:LABEL_MAX], []
        elif header is not None and len(body) < 50:
            body.append(ln[:2000])
    if header is not None:
        details.append((nid, header, "\n".join(body)))
    # a plan with blank lines but no detail sections: keep reading the rest as tree lines
    if not details and i < len(lines):
        tree += [ln for ln in lines[i:] if ln.strip() and not ln.strip().startswith("=====")]
    return tree, details


def _tree_nodes(tree: list[str]) -> tuple[list[str], set[str], bool]:
    """(node texts, their formatted-mode node ids, whether a part was skipped). The `== Initial Plan ==` part is
    skipped when a `== Final Plan ==` exists."""
    has_final = any("== Final Plan ==" in ln for ln in tree)
    out: list[str] = []
    ids: set[str] = set()
    skipped = False
    skip_at: int | None = None
    for ln in tree:
        pos, text, nid = _node_text(ln)
        if skip_at is not None:
            if pos > skip_at:
                skipped = True
                continue
            skip_at = None
        sm = _SECTION_RE.match(text)
        if sm:
            if has_final and "Initial Plan" in sm.group(1):
                skip_at = pos
            continue
        if text:
            out.append(text)
            if nid is not None:
                ids.add(nid)
    return out, ids, skipped


def parse_plan(plan: str | None) -> dict[str, list[str]]:
    """{"operators", "tables_read", "tables_written"} for one plan text. Never raises."""
    ops: list[str] = []
    reads: list[str] = []
    writes: list[str] = []
    if not plan or not isinstance(plan, str):
        return {"operators": ops, "tables_read": reads, "tables_written": writes}
    try:
        tree, details = _split(plan)
        nodes, kept_ids, skipped = _tree_nodes(tree)
        if skipped and kept_ids:  # formatted mode: only the detail sections of the nodes that were kept
            details = [d for d in details if d[0] in kept_ids]
        so, sr, sw = set(), set(), set()
        for text in reversed(nodes):  # data-flow order: leaves (scans) first
            label, _ = _operator(text)
            _add(ops, so, label, OPERATORS_MAX)
            _add(reads, sr, _scan_table(text), TABLES_MAX)
            _add(writes, sw, _write_table(text), TABLES_MAX)
        if not nodes:  # detail sections only
            for _, header, _ in reversed(details):
                _add(ops, so, _operator(header)[0], OPERATORS_MAX)
        for _, header, body in details:
            _add(reads, sr, _scan_table(header, body), TABLES_MAX)
            _add(writes, sw, _write_table(header, body), TABLES_MAX)
    except Exception:  # noqa: BLE001  (odd plans give partial results, never a failed build)
        pass
    return {"operators": ops, "tables_read": reads, "tables_written": writes}


def plan_summary(final_plan: str | None, initial_plan: str | None = None) -> dict[str, list[str]]:
    """Operators from the final plan (initial when there is no final); tables from both (an AQE final plan can
    show only `ShuffleQueryStage n` where the initial plan still names the scans)."""
    fin = parse_plan(final_plan)
    if not fin["operators"] and initial_plan and initial_plan != final_plan:
        fin["operators"] = parse_plan(initial_plan)["operators"]
    if initial_plan and initial_plan != final_plan:
        ini = parse_plan(initial_plan)
        for k in ("tables_read", "tables_written"):
            for t in ini[k]:
                if t not in fin[k] and len(fin[k]) < TABLES_MAX:
                    fin[k].append(t)
    # a JDBC read names its source only inside the query it sends to the database
    j = jdbc_source(final_plan) or jdbc_source(initial_plan)
    if j and j["source"] and f"jdbc:{j['source']}" not in fin["tables_read"]:
        fin["tables_read"].append(f"jdbc:{j['source']}")
    return fin


# ---- table names for storage paths ----------------------------------------------------------------------------------
# Unity Catalog managed tables are written by path (".../__unitystorage/.../tables/<uuid>"), so a query that writes one
# names only the path. The plans of the cluster still tie the two together in three places; the table id (the uuid at
# the end of the path) is the key, because plans shorten long paths with "...".
_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_TABLE_ID_RE = re.compile(r"tables/(" + _UUID + r")(/_delta_log(?:/.*)?)?/?$")
# MERGE / DELETE / UPDATE arguments: `catalog`.`schema`.`table`, Delta[version=12, ...tables/<uuid>]
_NAMED_DELTA_RE = re.compile(r"((?:`[^`]+`\.){1,2}`[^`]+`),\s*Delta\[version=\d+,[^\]]*?tables/(" + _UUID + r")\]")
# a scan's detail block: "(1) Scan parquet catalog.schema.table" ... "Location: PreparedDeltaFileIndex [...tables/<uuid>]"
_NAMED_SCAN_RE = re.compile(r"^\(\d+\) Scan \w+ ([\w.\-]+)\n(?:(?!\(\d+\) ).*\n){0,40}?Location: \w+ ?(?:\([^)]*\))? ?\["
                            r"[^\]]*?tables/(" + _UUID + r")", re.M)
# an append / overwrite carries the catalog entry: "Catalog: c\nDatabase: s\nTable: t" ... "Location: ...tables/<uuid>"
_NAMED_CATALOG_RE = re.compile(r"Catalog: (\S+)\nDatabase: (\S+)\nTable: (\S+)\n(?:.*\n){0,60}?Location: \S*?tables/("
                               + _UUID + r")", re.M)


def table_names(plans) -> dict[str, str]:
    """{table id: catalog.schema.table} from plan texts (any number; None is skipped)."""
    out: dict[str, str] = {}
    for p in plans:
        if not p or not isinstance(p, str) or "tables/" not in p:
            continue
        try:
            for m in _NAMED_DELTA_RE.finditer(p):
                out.setdefault(m.group(2), m.group(1).replace("`", ""))
            for m in _NAMED_SCAN_RE.finditer(p):
                if "." in m.group(1):
                    out.setdefault(m.group(2), m.group(1))
            for m in _NAMED_CATALOG_RE.finditer(p):
                out.setdefault(m.group(4), f"{m.group(1)}.{m.group(2)}.{m.group(3)}")
        except Exception:  # noqa: BLE001  (an odd plan names nothing, never fails the build)
            continue
    return out


def name_table(t: str, names: dict[str, str]) -> str:
    """A storage path of a table the plans named -> its name (a read of its Delta log, any of its checkpoint files ->
    "<name>/_delta_log"); anything else unchanged."""
    if not names or not t or "tables/" not in t:
        return t
    m = _TABLE_ID_RE.search(t.rstrip("/"))
    if not m or m.group(1) not in names:
        return t
    return names[m.group(1)] + ("/_delta_log" if m.group(2) else "")


def name_tables(ts, names: dict[str, str]):
    """The same list with named paths replaced (duplicates dropped, order kept)."""
    if not ts or not names:
        return ts
    out: list[str] = []
    for t in ts:
        n = name_table(t, names)
        if n not in out:
            out.append(n)
    return out


_PATH_IN_TEXT_RE = re.compile(r"[\w:/@.\-]*?tables/(" + _UUID + r")(/_delta_log[^\s,\]\)'\"]*)?")


def name_paths_in_text(text, names: dict[str, str]):
    """Free text (evidence, a story line) with the storage paths of named tables replaced by the names."""
    if not names or not isinstance(text, str) or "tables/" not in text:
        return text
    return _PATH_IN_TEXT_RE.sub(lambda m: (names[m.group(1)] + ("/_delta_log" if m.group(2) else ""))
                                if m.group(1) in names else m.group(0), text)


# ---- JDBC sources ---------------------------------------------------------------------------------------------------
# "Scan JDBCRelation((<the query sent to the database>) <alias>) [numPartitions=77] [limit=1]": the source query, its
# partitions and the object it reads. JDBC reports rows, not bytes.
_JDBC_RE = re.compile(r"JDBCRelation\((.*?)\)\s*\[numPartitions=(\d+)\]((?:\s*\[[^\]\n]*\])*)", re.S)
_FROM_RE = re.compile(r"\bFROM\s+((?:[\"`\[]?[A-Za-z_][\w$#]*[\"`\]]?\.)+[\"`\[]?[A-Za-z_][\w$#]*[\"`\]]?)", re.I)


def jdbc_source(plan) -> dict | None:
    """{"sql", "source", "partitions", "kind"} of the first JDBC scan in a plan text, or None. kind: "count" (a
    COUNT(*) only), "key ranges" (GROUP BY with MIN/MAX: bounds for partitioning), "probe" (LIMIT / a limit pushed
    down), else "read"."""
    if not plan or not isinstance(plan, str) or "JDBCRelation" not in plan:
        return None
    m = _JDBC_RE.search(plan)
    if not m:
        return None
    inner = m.group(1).strip()
    # "(<sql>) ALIAS" -> <sql>; a bare table name stays as it is
    am = re.match(r"^\((.*)\)\s*[\w$]*$", inner, re.S)
    sql = re.sub(r"\s+", " ", am.group(1) if am else inner).strip()
    fm = _FROM_RE.findall(sql)
    source = re.sub(r"[\"`\[\]]", "", fm[-1]) if fm else (sql if re.fullmatch(r"[\w$.#\"`\[\]]+", sql) else None)
    up = sql.upper()
    extra = m.group(3) or ""
    if "GROUP BY" in up and ("MIN(" in up or "MAX(" in up):
        kind = "key ranges"
    elif re.search(r"\bCOUNT\s*\(", up) and "GROUP BY" not in up and up.count("SELECT") <= 2:
        kind = "count"
    elif "[limit=" in extra or re.search(r"\bLIMIT\s+\d+|\bFETCH\s+FIRST\b|\bTOP\s+\d+", up):
        kind = "probe"
    else:
        kind = "read"
    return {"sql": sql[:2000], "source": source, "partitions": int(m.group(2)), "kind": kind}
