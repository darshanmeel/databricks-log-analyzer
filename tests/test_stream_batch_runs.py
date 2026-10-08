"""A streaming batch is a root query (it holds the file scan, runs no job) with queries under it; one of them runs the
jobs. The root and its other job-less queries take that query's run, so a table the cluster lists is in its run too."""
import collections

import pandas as pd

from databricks_cluster_log_analyzer.analysis.runs import attach_run_keys


def test_root_and_sibling_queries_of_a_streaming_batch_take_the_run():
    d = collections.defaultdict(list)
    d["spark_jobs"] = [{"spark_context_id": "c", "spark_job_id": 1, "sql_execution_id": 13,
                        "databricks_job_id": "7", "databricks_run_id": "70"}]
    d["tasks"] = pd.DataFrame()
    d["sql_queries"] = [{"spark_context_id": "c", "sql_execution_id": i, "root_execution_id": 10} for i in (10, 11, 13)]
    attach_run_keys("demo", d)
    keys = [q["run_key"] for q in d["sql_queries"]]
    assert keys[2] is not None and keys == [keys[2]] * 3
