"""Revision 11: rows read and written per stage, and the data skew (biggest task input / median task input)."""
import numpy as np
import pandas as pd

from databricks_cluster_log_analyzer.analysis.aggregate import _stage_metrics
from databricks_cluster_log_analyzer.parsing.eventlog import TASK_COLS


def _tasks(rows: list[dict]) -> pd.DataFrame:
    base = {c: np.nan for c in TASK_COLS}
    base.update({"cluster_id": "c", "spark_context_id": "x", "stage_attempt": 0, "failed": False, "executor_id": "0"})
    df = pd.DataFrame([{**base, **r} for r in rows], columns=TASK_COLS)
    for c in TASK_COLS:
        if c not in ("cluster_id", "spark_context_id", "executor_id", "host", "failed", "end_reason", "error",
                     "speculative"):
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    df["failed"] = df["failed"].astype(bool)
    return df


def test_stage_data_spread_and_rows():
    # stage 1: one hot partition (shuffle read 10x the others); stage 2: storage reads only, even
    rows = [{"stage_id": 1, "task_id": i, "task_ms": 1000, "input_bytes": 0, "input_records": 0,
             "shuffle_read": 100 if i else 1000, "shuffle_read_records": 10 if i else 100,
             "shuffle_write": 5, "shuffle_write_records": 1, "output_bytes": 0, "output_records": 0} for i in range(5)]
    rows += [{"stage_id": 2, "task_id": 10 + i, "task_ms": 500, "input_bytes": 64, "input_records": 8,
              "shuffle_read": None, "shuffle_read_records": None, "output_bytes": 32, "output_records": 4}
             for i in range(4)]
    # a failed task reading nothing does not drag the data median down
    rows.append({"stage_id": 2, "task_id": 99, "task_ms": 10, "failed": True, "input_bytes": 0, "input_records": 0})
    m = _stage_metrics(_tasks(rows))
    s1, s2 = m[("x", 1, 0)], m[("x", 2, 0)]
    assert (s1["min_task_bytes_in"], s1["p50_task_bytes_in"], s1["max_task_bytes_in"]) == (100, 100, 1000)
    assert (s1["min_task_rows_in"], s1["p50_task_rows_in"], s1["max_task_rows_in"]) == (10, 10, 100)
    assert s1["shuffle_read_records"] == 140 and s1["shuffle_write_records"] == 5 and s1["output_records"] == 0
    assert (s2["min_task_bytes_in"], s2["p50_task_bytes_in"], s2["max_task_bytes_in"]) == (64, 64, 64)
    assert s2["output_records"] == 16 and s2["input_records"] == 32
