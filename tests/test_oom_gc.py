"""Out-of-memory sites and executors stuck in GC (made-up stacks and numbers)."""

import pandas as pd

from databricks_cluster_log_analyzer.analysis.findings import _oom_site_findings, build_findings_rows, gc_stuck, log_oom_sites
from databricks_cluster_log_analyzer.analysis.hotspots import _skew_tasks
from databricks_cluster_log_analyzer.config import load_rules
from databricks_cluster_log_analyzer.parsing.oom import oom_site, task_oom_site


def frame(cls, m):
    return {"Declaring Class": cls, "Method Name": m, "File Name": "x.scala", "Line Number": 1}


def test_task_oom_site_from_the_stack():
    cache = {"Reason": "ExceptionFailure", "Class Name": "java.lang.OutOfMemoryError", "Description": "Java heap space",
             "Stack Trace": [frame("java.nio.HeapByteBuffer", "<init>"), frame("org.apache.spark.storage.DiskStore", "put"),
                             frame("org.apache.spark.sql.execution.columnar.CachedRDDBuilder", "buildBuffers")]}
    assert task_oom_site(cache) == "cache"
    sort = {"Class Name": "java.lang.OutOfMemoryError", "Description": "",
            "Full Stack Trace": "java.lang.OutOfMemoryError\n\tat org.apache.spark.unsafe.map.BytesToBytesMap.grow"}
    assert task_oom_site(sort) == "sort_aggregate"
    assert task_oom_site({"Class Name": "java.lang.OutOfMemoryError", "Description": "x"}) == "unknown"
    assert task_oom_site({"Class Name": "java.io.IOException", "Description": "disk"}) is None
    assert oom_site(["org.apache.spark.network.BlockTransferService$$anon$1.onBlockFetchSuccess"]) == "block_fetch"
    assert oom_site([], "Total size of serialized results is bigger than spark.driver.maxResultSize") == "driver_result"


def test_oom_findings_say_where():
    rules = load_rules()
    stages = [{"spark_context_id": "c", "stage_id": 106, "stage_attempt": 0, "sql_execution_id": 104}]
    ooms = [{"spark_context_id": "c", "stage_id": 106, "executor_id": e, "oom_site": "cache", "ts": 5} for e in ("2", "4")]
    f = _oom_site_findings("x", ooms, stages)
    assert len(f) == 1 and f[0]["severity"] == "high"
    assert f[0]["evidence"] == "2 tasks ran out of memory while building a DataFrame cache in query 104 (stage 106), on executors 2, 4."
    assert "unpersist" in f[0]["fix"]
    # the executor removed with exit code 52 says where too, from its own log
    logs = [{"executor_id": "3", "exception_class": "java.lang.OutOfMemoryError", "message": "Java heap space",
             "top_frames": ["org.apache.spark.broadcast.TorrentBroadcast.readBroadcastBlock"]}]
    assert log_oom_sites(logs) == {"3": "broadcast"}
    ex = [{"spark_context_id": "c", "executor_id": "3", "removed_reason": "Command exited with code 52",
           "removed_time": 9, "removal_category": "oom"}]
    rows = build_findings_rows({"cluster_id": "x", "executors": ex, "log_errors": []}, rules)
    assert [r["evidence"] for r in rows if r["category"] == "executor_oom"] == ["Command exited with code 52"]
    rows = build_findings_rows({"cluster_id": "x", "executors": ex, "task_ooms": [{**ooms[0], "executor_id": "3"}]}, rules)
    oom = next(r for r in rows if r["category"] == "executor_oom")
    assert oom["evidence"].endswith("out of memory while building a DataFrame cache")


def test_stuck_in_gc_not_skew():
    rules = load_rules()
    life = 284_000  # 4 min 44 s
    g = {"spark_context_id": "c", "executor_id": "4", "full_gcs": 2191, "stuck_full_gcs": 2062, "gc_pause_ms": 250_000,
         "lifetime_ms": life, "max_heap_after_mb": 8800, "heap_total_mb": 8874}
    assert gc_stuck(g, rules) is not None
    assert gc_stuck({**g, "stuck_full_gcs": 100}, rules) is None
    rows = build_findings_rows({"cluster_id": "x", "gc_profile": [g]}, rules)
    f = [r for r in rows if r["executor_id"] == "4"]
    assert [r["category"] for r in f] == ["gc_stuck"] and f[0]["severity"] == "high"
    assert f[0]["evidence"].startswith("stuck in GC: 463 Full GCs a minute, 94% of them")

    # four tasks hung on executor 4; their retries on executor 2 were quick
    def t(tid, idx, ex, ms, failed=False):
        return {"spark_context_id": "c", "stage_id": 1, "stage_attempt": 0, "task_id": tid, "task_index": idx,
                "executor_id": ex, "host": None, "task_ms": ms, "run_ms": ms, "gc_ms": 0, "launch_time": 0,
                "finish_time": ms, "shuffle_read": 0, "input_bytes": 100 if not failed else 0, "failed": failed,
                "end_reason": "ExecutorLostFailure" if failed else "Success"}
    tasks = [t(i, i, "1", 10_000) for i in range(20)] + [t(100, 50, "4", 247_000, True), t(101, 50, "2", 18_500)]
    hs = _skew_tasks(pd.DataFrame(tasks), {}, {}, rules, "x", stuck={("c", "4")})
    assert hs[0]["cause"] == "gc_stuck"
    assert "stuck in GC on executor 4" in hs[0]["detail"] and "its retry took 18 s on executor 2" in hs[0]["detail"]
    # without the GC log: lost with the executor, not "input close to the median"
    hs = _skew_tasks(pd.DataFrame(tasks), {}, {}, rules, "x")
    assert hs[0]["cause"] == "lost" and "no metrics: it was lost with executor 4" in hs[0]["detail"]
