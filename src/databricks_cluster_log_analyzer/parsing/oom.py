"""Where an OutOfMemoryError happened, from its stack: the first frame that names a known memory user wins.

The site tells the on-call engineer what to change (a cache that does not fit, a broadcast that is too big, a sort
or aggregate with too little memory per task), where a bare "out of memory" only says "add memory"."""

from __future__ import annotations

import re

OOM_RE = re.compile(r"OutOfMemoryError|Java heap space|GC overhead limit exceeded|Requested array size exceeds|"
                    r"OUT_OF_MEMORY")

#: site -> frame pattern, tried on each frame in stack order (the innermost frame first)
SITES: tuple[tuple[str, re.Pattern], ...] = (
    ("cache", re.compile(r"CachedRDDBuilder|DefaultCachedBatchSerializer|MemoryStore\.putIterator|"
                         r"BlockManager\.doPutIterator|DiskStore\.put|InMemoryRelation")),
    ("block_fetch", re.compile(r"BlockTransferService\S*onBlockFetchSuccess|OneForOneBlockFetcher|"
                               r"ShuffleBlockFetcherIterator|fetchRemoteManagedBuffer")),
    ("broadcast", re.compile(r"TorrentBroadcast|HashedRelation|BroadcastExchangeExec|BroadcastHashJoin")),
    ("sort_aggregate", re.compile(r"UnsafeExternalSorter|UnsafeInMemorySorter|BytesToBytesMap|HashAggregate|"
                                  r"ObjectAggregat|UnsafeFixedWidthAggregationMap|ExternalAppendOnlyMap")),
    ("driver_result", re.compile(r"maxResultSize|\.collect\b|collectFromPlan|toPandas|executeCollect|"
                                 r"collectAsArrowToPython")),
)

#: how a site reads in a finding, and what to do about it
SITE_TEXT = {
    "cache": "while building a DataFrame cache",
    "block_fetch": "while fetching a block from another executor",
    "broadcast": "while building a broadcast table",
    "sort_aggregate": "while sorting or aggregating",
    "driver_result": "while collecting results to the driver",
}
SITE_FIX = {
    "cache": ("A cache() or persist() of more data than storage memory: cache only the reused columns, write an "
              "intermediate Delta table instead, and unpersist when done."),
    "block_fetch": "A large cached or shuffle block was fetched in one piece: smaller partitions, or no cache.",
    "broadcast": ("The broadcast side is too big for memory: lower spark.sql.autoBroadcastJoinThreshold or drop the "
                  "broadcast hint."),
    "sort_aggregate": "Each task holds too much: more shuffle partitions, fewer cores per executor, or fix skew.",
    "driver_result": "Too much data comes back to the driver: write it out instead of collect() or toPandas().",
}


def is_oom(text: str | None) -> bool:
    return bool(text) and bool(OOM_RE.search(text))


def oom_site(frames, text: str | None = None) -> str | None:
    """The site of an OOM from its frames (strings, innermost first), else from the free text (message or full
    trace). None when nothing matches."""
    for f in frames or ():
        for site, rx in SITES:
            if rx.search(f):
                return site
    if text:
        best = None
        for site, rx in SITES:
            m = rx.search(text)
            if m and (best is None or m.start() < best[0]):
                best = (m.start(), site)
        if best:
            return best[1]
    return None


def task_oom_site(ter: dict) -> str | None:
    """The OOM site of a failed task, from its Task End Reason (Class Name, Description, Stack Trace, Full Stack
    Trace); None when the task did not run out of memory or the site is unknown."""
    desc = " ".join(str(ter.get(k) or "") for k in ("Class Name", "Description"))
    full = ter.get("Full Stack Trace")
    full = full if isinstance(full, str) else ""
    if not (is_oom(desc) or is_oom(full[:2000])):
        return None
    st = ter.get("Stack Trace")
    frames = [f"{f.get('Declaring Class', '')}.{f.get('Method Name', '')}" for f in st if isinstance(f, dict)] \
        if isinstance(st, list) else []
    return oom_site(frames, full or desc) or "unknown"
