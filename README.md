# Databricks Cluster Log Analyzer

> **Not a perfect tool, but a fast one.** Most of the time (about 95% of cases) it finds the real problem, or more
> than 95% of the issues, without much effort from you. For the rest it gives you more than enough data to find the
> issue yourself, and many of its findings help you **rule things out** or show you **where to dive in**. For a
> developer it answers the 2 a.m. questions: what to fix, and where to look.

Point it at the logs Databricks delivers for a cluster (driver and executor logs, Spark event logs). It turns them into
Parquet datasets and opens a **local web UI** that answers:

- **What ran and how long it took:** the cluster's time on the clock next to run time added up over runs, and how much
  was spent waiting for a free core.
- **What failed and why:** failed jobs and queries, task retries that still succeeded, lost or out-of-memory
  executors, and errors grouped with the first line of your own code.
- **What to change:** grouped fixes, highest impact first, each with what the data shows, the likely cause and the
  setting or code change. This is shown for the whole cluster and for each run.
- **Which tables cost the most:** size, files, file size, files skipped, bytes and rows pulled, and scan time, for the
  cluster, a run or one query. It flags files over 1 GB, big tables read whole, and tables scanned again and again.
- **Rows passed along:** rows read and written per table; rows in and out of every stage, per parent stage. It flags a
  join that puts out more rows than came in. It also shows the rows passed from one job or query to the next.
- **Drill down** from cluster to run, query, job, stage and task, with skew, spill, shuffle, GC and executor charts.

Everything runs on your laptop: no Spark, no cluster, nothing leaves the machine.

| Cluster overview | A query: step by step, rows in → out |
|---|---|
| ![Cluster overview](docs/screenshots/cluster-overview.png) | ![Query](docs/screenshots/query-graph.png) |

**The full tour, with every screen explained: open [README.html](README.html)** in a browser (clone or download the
repo and double-click it, or view it online through
[htmlpreview](https://htmlpreview.github.io/?https://github.com/darshanmeel/databricks-log-analyzer/blob/main/README.html)).
Every screenshot is from a synthetic demo cluster (a made-up shop's nightly ETL), not from a real customer.

## Install and run

Only **Python 3.11+** is needed: the web UI is already built and ships in the package, so you don't need Node.js.

**Windows (PowerShell)**

```powershell
git clone https://github.com/darshanmeel/databricks-log-analyzer.git   # or unzip the download
cd databricks-log-analyzer
python -m pip install -e .
dbx-log-analyzer ui --output C:\dbx\output --cache C:\dbx\cache
```

**macOS / Linux**

```bash
git clone https://github.com/darshanmeel/databricks-log-analyzer.git
cd databricks-log-analyzer
python3 -m pip install -e .
dbx-log-analyzer ui --output ~/dbx/output --cache ~/dbx/cache
```

A browser tab opens at http://127.0.0.1:8765. On **Home**:

1. Pick **Local folder** (or a Unity Catalog volume, ADLS or S3).
2. Paste the folder that *contains* the cluster-id folder.
3. Pick the cluster and click **Analyze**.

Each analyzed cluster has an **Analyze again** button. It rebuilds the analysis from the same raw logs with the newest
version of the tool, as long as those logs are still on disk.

### Without installing the package

Install the libraries once, then point Python at `src` each time:

```powershell
python -m pip install pandas pyarrow duckdb click fastapi uvicorn   # once
$env:PYTHONPATH = "src"                                            # in every new PowerShell window
python -m databricks_cluster_log_analyzer ui --output C:\dbx\output --cache C:\dbx\cache
```

```bash
python3 -m pip install pandas pyarrow duckdb click fastapi uvicorn   # once
PYTHONPATH=src python3 -m databricks_cluster_log_analyzer ui --output ~/dbx/output --cache ~/dbx/cache
```

Cloud sources need their extra: `pip install -e ".[databricks]"` (volumes), `".[adls]"`, `".[s3]"` or `".[all]"`.

### Try it on the demo cluster

```bash
python scripts/make_demo_cluster.py ./demo_logs        # writes a synthetic cluster: 0112-020000-demo0001
dbx-log-analyzer ui --output ./demo_out --cache ./demo_cache
```

Then on Home pick **Local folder**, enter `./demo_logs` and analyze `0112-020000-demo0001`.

### Good to know

- **To update:** `git pull`, then start the UI again. Old analyses still open. Click **Analyze again** for the newest
  findings.
- **"No module named databricks_cluster_log_analyzer":** run `python -m pip install -e .` in this folder, or set
  `PYTHONPATH=src`.
- **"Command not found":** use `python -m databricks_cluster_log_analyzer ui ...` instead.
- **Port in use:** add `--port 8766`.
- **Keep `--output` and `--cache` outside the repo.** That way nothing from a real cluster can be committed by accident.

## Command line

```bash
dbx-log-analyzer clusters --root ./my-logs                                   # clusters in a folder (or --source volume|adls|s3)
dbx-log-analyzer analyze --root ./my-logs --cluster 0101-000000-abcd1234     # download if remote, then parse
dbx-log-analyzer build --input ./cache/0101-000000-abcd1234 [--duckdb]       # parse one local cluster folder
dbx-log-analyzer ui                                                          # the web UI
```

Sources, the log folder layout, every dataset, the rules file and how the tool is built are all described in
[README.html](README.html).
