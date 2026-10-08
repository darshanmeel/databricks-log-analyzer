"""Databricks Cluster Log Analyzer: cluster logs -> Parquet datasets -> local UI."""

__version__ = "0.1.0"

# Bumped when the analysis changes what an output holds, so an output built by an older version can be told apart
# (summary.json "analyzer_revision"; outputs built before it have none). refresh-findings writes it too.
ANALYZER_REVISION = 20
