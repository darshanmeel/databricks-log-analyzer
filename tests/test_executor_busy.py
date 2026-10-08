"""Revision 12: when each executor had a task running, and how long it was up but idle."""
import pandas as pd

from databricks_cluster_log_analyzer.analysis.combined import busy_wall, executor_busy


def test_executor_busy_merges_overlapping_and_close_tasks():
    tdf = pd.DataFrame({
        "spark_context_id": ["x"] * 6,
        "executor_id": ["0", "0", "0", "0", "1", "driver"],
        "launch_time": [0.0, 500.0, 2_500.0, 60_000.0, 10_000.0, 0.0],
        "finish_time": [1_000.0, 2_000.0, 3_000.0, 61_000.0, 70_000.0, 5.0],
        "task_ms": [1_000.0, 1_500.0, 500.0, 1_000.0, 60_000.0, 5.0],
    })
    b = executor_busy(tdf, "c")
    e0 = b[b.executor_id == "0"].sort_values("busy_start")
    # 0-1000 and 500-2000 overlap, 2500 starts within the 2 s gap: one stretch; 60 s later a second one
    assert list(zip(e0.busy_start, e0.busy_end, e0.tasks)) == [(0, 3_000, 3), (60_000, 61_000, 1)]
    assert "driver" not in set(b.executor_id)
    assert busy_wall(b) == {("x", "0"): 4_000, ("x", "1"): 60_000}
