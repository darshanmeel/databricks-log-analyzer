"""`python -m databricks_cluster_log_analyzer ...`: the same CLI as `dbx-log-analyzer` (for when the Scripts folder
is not on PATH)."""
import sys

from .cli import main

sys.exit(main())
