"""Revision 14: the Spark and cluster settings that decide speed and cost, read from the event log.

The cluster's own configuration comes from SparkListenerEnvironmentUpdate; a value set in code (spark.conf.set,
SET in SQL) shows up in the properties of the Spark jobs and in the modifiedConfigs of the SQL queries after it.
A setting that was never set runs with its default. Environment variables are not in the event log: only how many
there were (clusterNumSparkEnvVars).
"""

from __future__ import annotations

TAGS = "spark.databricks.clusterUsageTags."

# (key, group, Spark / Databricks default or None when it depends, what it decides)
SETTINGS: tuple[tuple[str, str, str | None, str], ...] = (
    ("spark.sql.shuffle.partitions", "Partitions", "200", "how many tasks every shuffle (join, group by, MERGE) is cut into"),
    ("spark.databricks.adaptive.autoOptimizeShuffle.enabled", "Partitions", "false",
     "let Databricks pick the shuffle partitions from the data size (same as shuffle.partitions = auto)"),
    ("spark.sql.adaptive.enabled", "Partitions", "true", "adaptive query execution: re-plans each stage with the real sizes"),
    ("spark.sql.adaptive.coalescePartitions.enabled", "Partitions", "true", "merge small shuffle partitions after a shuffle"),
    ("spark.sql.adaptive.advisoryPartitionSizeInBytes", "Partitions", "64MB", "the size adaptive execution aims for per shuffle partition"),
    ("spark.sql.adaptive.skewJoin.enabled", "Partitions", "true", "split very large join partitions into several tasks"),
    ("spark.sql.files.maxPartitionBytes", "Partitions", "128MB", "the most a task reads from files"),
    ("spark.sql.autoBroadcastJoinThreshold", "Partitions", "10MB", "tables up to this size are copied to every executor instead of shuffled"),
    ("spark.executor.memory", "Memory and cores", None, "JVM heap per executor"),
    ("spark.executor.memoryOverhead", "Memory and cores", None, "memory per executor outside the heap"),
    ("spark.executor.cores", "Memory and cores", None, "tasks an executor runs at once"),
    ("spark.task.cpus", "Memory and cores", "1", "cores each task takes"),
    ("spark.memory.fraction", "Memory and cores", "0.6", "share of the heap for running tasks and cache"),
    ("spark.memory.storageFraction", "Memory and cores", "0.5", "part of that kept for cache"),
    ("spark.memory.offHeap.enabled", "Memory and cores", "false", "memory outside the JVM heap for Spark"),
    ("spark.memory.offHeap.size", "Memory and cores", None, "its size"),
    ("spark.driver.maxResultSize", "Memory and cores", None, "the most a collect() may bring to the driver"),
    ("spark.scheduler.mode", "Sharing and scheduling", "FIFO", "FAIR lets runs on the same cluster share the cores"),
    ("spark.databricks.preemption.enabled", "Sharing and scheduling", None, "take cores back from a run that holds too many"),
    ("spark.speculation", "Sharing and scheduling", "false", "start a copy of a very slow task elsewhere"),
    ("spark.dynamicAllocation.enabled", "Sharing and scheduling", "false", "open source autoscaling (Databricks scales workers instead)"),
    ("spark.databricks.aggressiveWindowDownS", "Sharing and scheduling", None, "how fast an autoscaling cluster gives idle workers back"),
    ("spark.databricks.photon.enabled", "Engine and storage", None, "Photon, the vectorised engine"),
    ("spark.databricks.io.cache.enabled", "Engine and storage", None, "the local disk cache for files read again"),
    ("spark.databricks.delta.optimizeWrite.enabled", "Engine and storage", None, "write fewer, bigger files"),
    ("spark.databricks.delta.autoCompact.enabled", "Engine and storage", None, "compact small files after a write"),
    (TAGS + "runtimeEngine", "Cluster", None, "STANDARD or PHOTON"),
    (TAGS + "effectiveSparkVersion", "Cluster", None, "Databricks runtime"),
    (TAGS + "clusterNodeType", "Cluster", None, "worker VM type"),
    (TAGS + "driverNodeType", "Cluster", None, "driver VM type"),
    (TAGS + "clusterWorkers", "Cluster", None, "workers asked for"),
    (TAGS + "clusterMinWorkers", "Cluster", None, "autoscaling minimum"),
    (TAGS + "clusterMaxWorkers", "Cluster", None, "autoscaling maximum"),
    (TAGS + "clusterScalingType", "Cluster", None, "fixed size or autoscaling"),
    (TAGS + "autoTerminationMinutes", "Cluster", None, "idle minutes before the cluster stops (0: never)"),
    (TAGS + "clusterAvailability", "Cluster", None, "on-demand or spot VMs"),
    (TAGS + "clusterFirstOnDemand", "Cluster", None, "how many VMs are on-demand before spot"),
    (TAGS + "enableElasticDisk", "Cluster", None, "grow local disk when it fills (spill)"),
    (TAGS + "workloadType", "Cluster", None, "job cluster or all-purpose"),
    (TAGS + "clusterNumSparkConfs", "Cluster", None, "Spark settings in the cluster's configuration"),
    (TAGS + "clusterNumSparkEnvVars", "Cluster", None, "environment variables set on the cluster (their values are not logged)"),
    (TAGS + "numPerClusterInitScriptsV2", "Cluster", None, "init scripts run when a node starts"),
)
SETTING_KEYS = frozenset(k for k, *_ in SETTINGS)


def short_key(key: str) -> str:
    return key.replace(TAGS, "cluster: ")


def size_bytes(v: str | None) -> int | None:
    """'128MB', '64m', '10485760', '1g' -> bytes; None when unreadable (-1 and 'auto' included)."""
    import re

    if v is None:
        return None
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kmgt]?)(?:i?b)?\s*", str(v).lower())
    if not m:
        return None
    return int(float(m.group(1)) * {"": 1, "k": 1 << 10, "m": 1 << 20, "g": 1 << 30, "t": 1 << 40}[m.group(2)])
