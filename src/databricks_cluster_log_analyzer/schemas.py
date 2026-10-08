"""Explicit Arrow schemas for every dataset. Empty datasets keep these schemas."""

from __future__ import annotations

import pyarrow as pa

S = pa.string()
I = pa.int64()
I32 = pa.int32()
F = pa.float64()
B = pa.bool_()
TS = pa.timestamp("ms")
LS = pa.list_(pa.string())
LI = pa.list_(pa.int64())


def _schema(*cols: tuple[str, pa.DataType]) -> pa.Schema:
    return pa.schema([pa.field(n, t) for n, t in cols])


SCHEMAS: dict[str, pa.Schema] = {
    "files": _schema(("cluster_id", S), ("folder", S), ("path", S), ("size", I), ("modified", TS)),
    "apps": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("app_id", S), ("app_name", S), ("spark_version", S),
        ("user", S), ("start_time", TS), ("end_time", TS), ("duration_ms", I), ("eventlog_files", I32),
        ("events_read", I), ("malformed_lines", I),
    ),
    "log_lines": _schema(
        ("cluster_id", S), ("source", S), ("app_id", S), ("executor_id", S), ("file_path", S), ("file_name", S),
        ("seq", I), ("line_no", I32), ("ts", TS), ("level", S), ("logger", S), ("message", S), ("line", S),
        ("signal", S), ("continuation", B),
    ),
    "log_signals": _schema(
        ("cluster_id", S), ("source", S), ("app_id", S), ("executor_id", S), ("file_path", S), ("file_name", S),
        ("seq", I), ("ts", TS), ("level", S), ("signal", S), ("severity", S), ("fix", S), ("line", S),
    ),
    "log_errors": _schema(
        ("cluster_id", S), ("source", S), ("app_id", S), ("executor_id", S), ("file_path", S), ("file_name", S),
        ("seq", I), ("ts", TS), ("exception_class", S), ("message", S), ("top_frames", LS), ("fingerprint", S),
        ("user_frame", S),
    ),
    "tasks": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("stage_id", I), ("stage_attempt", I), ("task_id", I),
        ("task_attempt", I), ("executor_id", S), ("host", S), ("launch_time", TS), ("finish_time", TS),
        ("task_ms", I), ("run_ms", I), ("gc_ms", I), ("peak_mem", I), ("mem_spill", I), ("disk_spill", I),
        ("input_bytes", I), ("input_records", I), ("output_bytes", I), ("shuffle_read", I), ("shuffle_write", I),
        ("failed", B), ("end_reason", S), ("error", S), ("task_index", I), ("speculative", B), ("cpu_ms", I), ("fetch_wait_ms", I), ("run_key", S),
        ("shuffle_read_records", I), ("shuffle_write_records", I), ("output_records", I), ("shuffle_write_ms", I),
        ("oom_site", S),
    ),
    "stages": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("stage_id", I), ("stage_attempt", I), ("stage_name", S),
        ("num_tasks", I), ("status", S), ("start_time", TS), ("end_time", TS), ("duration_ms", I),
        ("failure_reason", S), ("tasks", I), ("failed_tasks", I), ("p50_task_ms", I), ("max_task_ms", I),
        ("skew", F), ("gc_share", F), ("max_peak_mem", I), ("mem_spill", I), ("disk_spill", I),
        ("input_bytes", I), ("input_records", I), ("output_bytes", I), ("shuffle_read", I), ("shuffle_write", I),
        ("executors_used", I), ("spark_job_id", I), ("sql_execution_id", I), ("job_description", S),
        ("retry_of_failure", S), ("parent_ids", LI), ("rdd_names", LS), ("rdd_scopes", LS), ("details", S), ("run_key", S), ("min_task_ms", I),
        # Revision 11: rows read and written, and how evenly the data was spread over the tasks
        ("shuffle_read_records", I), ("shuffle_write_records", I), ("output_records", I),
        ("min_task_bytes_in", I), ("p50_task_bytes_in", I), ("max_task_bytes_in", I),
        ("min_task_rows_in", I), ("p50_task_rows_in", I), ("max_task_rows_in", I), ("data_skew", F),
        # Revision 13: p10 / p90 / average of the data per task and p10 / p90 of task time
        ("p10_task_bytes_in", I), ("p90_task_bytes_in", I), ("avg_task_bytes_in", I),
        ("p10_task_rows_in", I), ("p90_task_rows_in", I), ("avg_task_rows_in", I),
        ("p10_task_ms", I), ("p90_task_ms", I), ("wmed_task_bytes_in", I),
        # tasks per size band and the bytes each band read
        ("tasks_none", I), ("tasks_lt10", I), ("tasks_10_128", I), ("tasks_128_256", I), ("tasks_ge256", I),
        ("bytes_lt10", I), ("bytes_10_128", I), ("bytes_128_256", I), ("bytes_ge256", I),
        # where the input came from: cloud storage (and the Databricks disk cache), or a DataFrame cache
        ("cloud_bytes", I), ("disk_cache_bytes", I), ("storage_bytes", I), ("df_cache_bytes", I),
    ),
    "spark_jobs": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("spark_job_id", I), ("start_time", TS), ("end_time", TS),
        ("duration_ms", I), ("result", S), ("error", S), ("stage_ids", LI), ("num_stages", I),
        ("sql_execution_id", I), ("description", S), ("job_group", S), ("call_site", S),
        ("databricks_job_id", S), ("databricks_run_id", S), ("notebook_path", S), ("job_tags", S),
        ("databricks_task_run_id", S), ("connect_operation_id", S), ("run_key", S),
        ("databricks_parent_run_id", S), ("databricks_job_name", S), ("databricks_task_type", S),
        # Revision 13: data in and out, and how it was spread over the job's tasks
        ("input_bytes", I), ("input_records", I), ("output_bytes", I), ("output_records", I), ("shuffle_read", I),
        ("shuffle_write", I), ("min_task_bytes_in", I), ("p10_task_bytes_in", I), ("p50_task_bytes_in", I), ("p90_task_bytes_in", I),
        ("max_task_bytes_in", I), ("avg_task_bytes_in", I), ("min_task_rows_in", I), ("p10_task_rows_in", I),
        ("p50_task_rows_in", I), ("p90_task_rows_in", I), ("max_task_rows_in", I), ("avg_task_rows_in", I),
        ("p10_task_ms", I), ("p50_task_ms", I), ("p90_task_ms", I), ("wmed_task_bytes_in", I),
        # tasks per size band and the bytes each band read
        ("tasks_none", I), ("tasks_lt10", I), ("tasks_10_128", I), ("tasks_128_256", I), ("tasks_ge256", I),
        ("bytes_lt10", I), ("bytes_10_128", I), ("bytes_128_256", I), ("bytes_ge256", I),
    ),
    "sql_queries": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("sql_execution_id", I), ("start_time", TS), ("end_time", TS),
        ("duration_ms", I), ("status", S), ("description", S), ("details", S), ("error", S), ("stages", I),
        ("tasks", I), ("disk_spill", I), ("max_stage_skew", F), ("input_bytes", I), ("shuffle_read", I),
        ("plan_hash", S), ("final_plan", S), ("initial_plan", S), ("operators", LS), ("tables_read", LS),
        ("tables_written", LS), ("run_key", S),
        # Revision 13: the query this one runs under (Spark 3.4+), data in and out, its spread over tasks
        ("root_execution_id", I), ("photon_share", F), ("input_records", I), ("output_bytes", I), ("output_records", I),
        ("shuffle_write", I), ("min_task_bytes_in", I), ("p10_task_bytes_in", I), ("p50_task_bytes_in", I), ("p90_task_bytes_in", I),
        ("max_task_bytes_in", I), ("avg_task_bytes_in", I), ("min_task_rows_in", I), ("p10_task_rows_in", I),
        ("p50_task_rows_in", I), ("p90_task_rows_in", I), ("max_task_rows_in", I), ("avg_task_rows_in", I),
        ("p10_task_ms", I), ("p50_task_ms", I), ("p90_task_ms", I), ("wmed_task_bytes_in", I),
        # tasks per size band and the bytes each band read
        ("tasks_none", I), ("tasks_lt10", I), ("tasks_10_128", I), ("tasks_128_256", I), ("tasks_ge256", I),
        ("bytes_lt10", I), ("bytes_10_128", I), ("bytes_128_256", I), ("bytes_ge256", I),
        # where the reads came from: files in storage, a DataFrame cache; and the Databricks disk cache
        ("storage_read", I), ("cache_read", I), ("disk_cache_hit", I), ("disk_cache_miss", I), ("disk_cache_write", I),
    ),
    "executors": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("executor_id", S), ("host", S), ("cores", I),
        ("added_time", TS), ("removed_time", TS), ("removed_reason", S), ("removed_reason_raw", S),
        ("removal_category", S),
        # Revision 13: what the executor was given
        ("resource_profile_id", I), ("heap_mb", I), ("overhead_mb", I), ("offheap_mb", I), ("unified_memory", I),
        ("storage_memory", I), ("task_cpus", I),
    ),
    "findings": _schema(
        ("finding_id", S), ("cluster_id", S), ("spark_context_id", S), ("severity", S), ("category", S),
        ("entity", S), ("evidence", S), ("fix", S), ("ts", TS), ("stage_id", I), ("stage_attempt", I),
        ("spark_job_id", I), ("sql_execution_id", I), ("executor_id", S), ("signal", S), ("fingerprint", S),
        ("log_file_path", S), ("log_seq", I), ("run_key", S),
    ),
    "timeline": _schema(("cluster_id", S), ("minute", TS), ("signal", S), ("count", I)),
    "query_profile": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("sql_execution_id", I), ("description", S), ("status", S),
        ("start_time", TS), ("end_time", TS), ("duration_ms", I), ("error", S), ("spark_jobs", I), ("stages", I),
        ("total_stage_ms", I), ("max_stage_ms", I), ("tasks", I), ("failed_tasks", I), ("mem_spill", I),
        ("disk_spill", I), ("gc_share", F), ("max_stage_skew", F), ("input_bytes", I), ("shuffle_read", I),
        ("shuffle_write", I), ("output_bytes", I), ("executors_used", I), ("executors_lost", I), ("findings", I),
        ("max_severity", S), ("plan_hash", S), ("run_key", S),
    ),
    "stage_executor_profile": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("stage_id", I), ("stage_attempt", I), ("executor_id", S),
        ("host", S), ("tasks", I), ("failed_tasks", I), ("task_ms_sum", I), ("task_ms_max", I), ("run_ms", I),
        ("gc_ms", I), ("mem_spill", I), ("disk_spill", I), ("input_bytes", I), ("shuffle_read", I),
        ("share_of_stage_ms", F), ("run_key", S),
    ),
    "executor_profile": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("app_id", S), ("executor_id", S), ("host", S), ("cores", I),
        ("added_time", TS), ("removed_time", TS), ("lifetime_ms", I), ("removed_reason", S),
        ("removal_category", S), ("tasks", I), ("failed_tasks", I), ("busy_ms", I), ("busy_share", F),
        ("run_ms", I), ("gc_ms", I), ("gc_share", F), ("mem_spill", I), ("disk_spill", I), ("max_peak_mem", I),
        ("log_lines", I), ("log_errors_lines", I), ("log_warn_lines", I), ("signals", I), ("top_signals", LS),
        ("exceptions", I), ("top_exception", S), ("removed_reason_raw", S), ("gc_pauses", I), ("gc_pause_ms", F),
        ("full_gcs", I), ("max_heap_after_mb", F), ("affected_runs", LS),
        ("top_run_key", S),
        # Revision 12: wall time with a task running, up but idle, and idle core time (lifetime x cores - task time)
        ("busy_wall_ms", I), ("idle_ms", I), ("idle_core_ms", I),
        # Revision 13: what the executor was given
        ("resource_profile_id", I), ("heap_mb", I), ("overhead_mb", I), ("offheap_mb", I), ("unified_memory", I),
        ("storage_memory", I), ("task_cpus", I),
        # from the GC log: share of its life in GC pauses, and how full the heap stayed after a Full GC (median)
        ("gc_pause_share", F), ("full_gc_heap_after_p50", F),
    ),
    "executor_busy": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("executor_id", S), ("busy_start", TS), ("busy_end", TS),
        ("tasks", I), ("task_ms", I),
    ),
    "run_story": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("story_seq", I), ("ts", TS), ("kind", S), ("severity", S),
        ("title", S), ("detail", S), ("count", I), ("spark_job_id", I), ("stage_id", I), ("stage_attempt", I),
        ("sql_execution_id", I), ("executor_id", S), ("source", S), ("log_file_path", S), ("log_seq", I),
        ("finding_id", S),
        # Revision 13: the run of the row's job, stage or query (null: shared by the cluster, e.g. executors)
        ("run_key", S),
    ),
    # ---- Revision 2 / 3 datasets ----------------------------------------------------------------------------
    "event_counts": _schema(("cluster_id", S), ("spark_context_id", S), ("event_type", S), ("count", I)),
    "file_lines": _schema(
        ("cluster_id", S), ("folder", S), ("file_path", S), ("lines", I), ("bytes", I), ("thread_dumps", I),
    ),
    "gc_events": _schema(
        ("cluster_id", S), ("source", S), ("app_id", S), ("executor_id", S), ("file_path", S), ("seq", I),
        ("ts", TS), ("gc_id", I), ("kind", S), ("cause", S), ("heap_before_mb", F), ("heap_after_mb", F),
        ("heap_total_mb", F), ("pause_ms", F),
    ),
    "sql_plan_nodes": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("sql_execution_id", I), ("node_id", I), ("parent_id", I),
        ("name", S), ("detail", S), ("codegen_id", I), ("is_cluster", B), ("rows_out", I), ("time_ms", F),
        ("peak_mem", I), ("spill_bytes", I), ("data_bytes", I), ("metrics_json", S), ("from_text", B),
    ),
    "incidents": _schema(
        ("cluster_id", S), ("finding_id", S), ("incident_id", S), ("incident_rank", I), ("incident_title", S),
        ("incident_severity", S), ("incident_impact", S), ("incident_start", TS), ("incident_end", TS),
        ("problem_id", S), ("kind", S), ("role", S), ("caused_by", S), ("because", S), ("confidence", S),
        ("spark_context_id", S),
        ("stage_id", I), ("stage_attempt", I), ("spark_job_id", I), ("sql_execution_id", I), ("executor_id", S),
        ("stages", S), ("executors", S),
    ),
    "connect_operations": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("operation_id", S), ("session_id", S), ("user_id", S),
        ("user_name", S), ("statement_text", S), ("job_tag", S), ("start_time", TS), ("analyzed_time", TS),
        ("ready_time", TS), ("finish_time", TS), ("closed_time", TS), ("duration_ms", I), ("status", S),
        ("error", S), ("spark_jobs", I),
    ),
    "cluster_info": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("cluster_name", S), ("cluster_creator", S),
        ("spark_version", S), ("driver_node_type", S), ("worker_node_type", S), ("min_workers", I),
        ("max_workers", I), ("target_workers", I), ("cluster_scaling_type", S), ("runtime_engine", S),
        ("cloud_provider", S), ("region", S), ("workload_type", S), ("databricks_job_id", S), ("job_run_id", S),
        ("task_run_id", S), ("parent_run_id", S),
    ),
    # Revision 14: the settings that decide speed and cost (settings_catalog.SETTINGS)
    "settings": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("key", S), ("group", S), ("value", S), ("cluster_value", S),
        ("session_values", LS), ("default", S), ("source", S), ("what", S),
    ),
    "task_retries": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("stage_id", I), ("stage_attempt", I), ("task_index", I),
        ("attempts", I), ("failed_attempts", I), ("first_attempt_executor_id", S), ("first_attempt_host", S),
        ("first_failure_reason", S), ("first_failure_error", S), ("first_failure_category", S),
        ("first_failure_time", TS), ("executor_removed_reason", S), ("final_status", S),
        ("final_executor_id", S), ("retry_delay_ms", I), ("wasted_ms", I), ("spark_job_id", I),
        ("sql_execution_id", I), ("explanation", S), ("run_key", S),
    ),
    # ---- Revision 4 -------------------------------------------------------------------------------------------
    "spill_shuffle_timeline": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("minute", TS), ("executor_id", S), ("stage_id", I),
        ("stage_attempt", I), ("tasks", I), ("mem_spill", I), ("disk_spill", I), ("shuffle_read", I),
        ("shuffle_write", I), ("input_bytes", I), ("output_bytes", I), ("gc_ms", I), ("run_ms", I),
    ),
    # ---- Revision 5 -------------------------------------------------------------------------------------------
    "hotspots": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("kind", S), ("ts_start", TS), ("ts_end", TS), ("stage_id", I),
        ("stage_attempt", I), ("spark_job_id", I), ("sql_execution_id", I), ("query_description", S),
        ("operator", S), ("task_id", I), ("task_index", I), ("executor_id", S), ("host", S), ("value", F),
        ("baseline", F), ("ratio", F), ("share", F), ("cause", S), ("detail", S), ("run_key", S), ("affected_runs", LS),
    ),
    # ---- Revision 6 -------------------------------------------------------------------------------------------
    "runs": _schema(
        ("cluster_id", S), ("spark_context_id", S), ("run_key", S), ("kind", S), ("label", S),
        ("databricks_job_id", S), ("databricks_run_id", S), ("task_run_id", S), ("notebook_path", S), ("user", S),
        ("start_time", TS), ("end_time", TS), ("duration_ms", I), ("status", S), ("spark_jobs", I), ("stages", I),
        ("tasks", I), ("failed_tasks", I), ("retried_tasks", I), ("disk_spill", I), ("mem_spill", I),
        ("shuffle_read", I), ("shuffle_write", I), ("findings", I), ("max_severity", S), ("overlapping_runs", LS),
        # Revision 13: data in and out, its spread over tasks, and the same job's usual duration
        ("input_bytes", I), ("input_records", I), ("output_bytes", I), ("output_records", I), ("min_task_bytes_in", I), ("p10_task_bytes_in", I), ("p50_task_bytes_in", I), ("p90_task_bytes_in", I),
        ("max_task_bytes_in", I), ("avg_task_bytes_in", I), ("min_task_rows_in", I), ("p10_task_rows_in", I),
        ("p50_task_rows_in", I), ("p90_task_rows_in", I), ("max_task_rows_in", I), ("avg_task_rows_in", I),
        ("p10_task_ms", I), ("p50_task_ms", I), ("p90_task_ms", I), ("wmed_task_bytes_in", I),
        # tasks per size band and the bytes each band read
        ("tasks_none", I), ("tasks_lt10", I), ("tasks_10_128", I), ("tasks_128_256", I), ("tasks_ge256", I),
        ("bytes_lt10", I), ("bytes_10_128", I), ("bytes_128_256", I), ("bytes_ge256", I),
        ("same_job_runs", I), ("typical_duration_ms", I), ("vs_typical", F), ("program", S), ("subject", S),
        ("parent_run_id", S), ("job_name", S), ("task_type", S),
    ),
}

DATASETS: tuple[str, ...] = tuple(SCHEMAS)

# Main timestamp column per dataset (used by the API for ts_from / ts_to).
MAIN_TS = {
    "log_lines": "ts", "log_signals": "ts", "log_errors": "ts", "findings": "ts", "run_story": "ts",
    "tasks": "launch_time", "stages": "start_time", "sql_queries": "start_time", "spark_jobs": "start_time",
    "query_profile": "start_time", "executors": "added_time", "executor_profile": "added_time", "executor_busy": "busy_start",
    "timeline": "minute", "apps": "start_time", "files": "modified", "gc_events": "ts",
    "connect_operations": "start_time", "task_retries": "first_failure_time", "spill_shuffle_timeline": "minute",
    "hotspots": "ts_start", "incidents": "incident_start",
    "runs": "start_time",
}
