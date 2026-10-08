"""Init scripts: init_scripts/<container>/<yyyyMMdd_HHmmss>_<NN>_<script>.{stdout,stderr}.log, one folder per node
start. A script that wrote an error line, or whose output differs between nodes, is worth a look; the time from the
script's start to the next executor registering is how long a node took to join."""

from __future__ import annotations

import datetime as _dt
import re
from collections.abc import Callable, Iterable
from pathlib import Path

from ..util import fmt_words
from .contention import to_ms

_NAME_RE = re.compile(r"(?:^|/)init_scripts/([^/]+)/(\d{8}_\d{6})_(\d+)_(.+?)\.(stdout|stderr)\.log(?:\.gz)?$")
_ERR_RE = re.compile(r"(?i)\berror\b|\bfailed\b|exit (?:code|status) [1-9]|traceback|command not found|"
                     r"permission denied|no such file")
# noise that installers print on success
_BENIGN_RE = re.compile(r"(?i)WARNING: Running pip as the 'root' user|DEPRECATION:|notice\]")

FIX = ("An init script wrote errors: check the script (a package that failed to install leaves the job without it), "
       "and make it fail loudly (set -e) so a broken node does not join the cluster.")


def init_scripts(cid: str, files: list[dict], read: Callable[[str], Iterable[str]], executors: list[dict]) -> tuple[dict | None, list[dict]]:
    """(summary, findings). `read(path)` yields a file's lines."""
    runs: dict[tuple, dict] = {}
    for f in files:
        m = _NAME_RE.search(f["path"].replace("\\", "/"))
        if not m:
            continue
        node, ts, _, script, kind = m.groups()
        r = runs.setdefault((node, script), {"node": node, "script": script, "start": ts, "stderr": None, "stdout": None})
        r[kind] = f["path"]
    if not runs:
        return None, []
    nodes = {k[0] for k in runs}
    # the node's own executor: the one on the same host (the folder is named after the node's address); the driver's
    # node runs the scripts too but starts no executor, so it has no join time
    adds: dict[str, list[int]] = {}
    for e in executors:
        t_ = to_ms(e.get("added_time"))
        if e.get("executor_id") in (None, "driver") or t_ is None or not e.get("host"):
            continue
        adds.setdefault(re.sub(r"[^0-9a-z]", "", str(e["host"]).lower()), []).append(t_)
    errors: dict[str, list] = {}
    outputs: dict[str, set] = {}
    joins = []
    for (node, script), r in runs.items():
        start = int(_dt.datetime.strptime(r["start"], "%Y%m%d_%H%M%S").replace(tzinfo=_dt.timezone.utc).timestamp() * 1000)
        key = re.sub(r"[^0-9a-z]", "", node.lower())
        mine = sorted(t_ for h, ts_ in adds.items() if h and h in key for t_ in ts_)
        nxt = next((t_ for t_ in mine if t_ >= start), None)
        if nxt is not None and nxt - start <= 900_000:
            joins.append(nxt - start)
        text = []
        for kind in ("stderr", "stdout"):
            if r[kind]:
                try:
                    text += [ln for ln in read(r[kind]) if ln.strip()]
                except OSError:
                    pass
        outputs.setdefault(script, set()).add("\n".join(re.sub(r"\d", "0", ln) for ln in text))
        bad = [ln.strip() for ln in text if _ERR_RE.search(ln) and not _BENIGN_RE.search(ln)]
        if bad:
            errors.setdefault(script, []).append((node, bad[0]))
    summary = {"node_starts": len(nodes), "scripts": sorted({k[1] for k in runs}),
               "with_errors": sum(len(v) for v in errors.values()),
               "same_output_everywhere": all(len(v) == 1 for v in outputs.values()),
               "start_to_executor_ms": [min(joins), max(joins)] if joins else None}
    out = []
    for script, bad in errors.items():
        n = len({b[0] for b in bad})
        ev = f"{script} wrote errors on {n} of {len(nodes)} node starts. e.g. {bad[0][1][:200]}"
        if joins:
            ev += f". Nodes took {fmt_words(min(joins))}-{fmt_words(max(joins))} from the script's start to their executor joining"
        out.append({"cluster_id": cid, "spark_context_id": None, "severity": "medium" if n == len(nodes) else "low",
                    "category": "init_script", "entity": f"init script {script}", "evidence": ev, "fix": FIX, "ts": None,
                    "stage_id": None, "stage_attempt": None, "spark_job_id": None, "sql_execution_id": None,
                    "executor_id": None, "signal": None, "fingerprint": None, "log_file_path": None,
                    "log_seq": None, "run_key": None})
    return summary, out


def reader(cluster_dir: Path, open_lines) -> Callable[[str], Iterable[str]]:
    return lambda rel: open_lines(cluster_dir / rel)
