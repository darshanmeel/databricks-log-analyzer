# Databricks Cluster Log Analyzer

Turn raw Databricks / Spark cluster logs (driver log4j/stdout/stderr, executor logs and Spark event logs) into
structured **Parquet datasets**, then explore them in a **local web UI** that shows, step by step, what happened
to a job: what ran, where the time went, what failed and why, and what to fix.

Everything runs on your laptop: no Spark, no cluster.

```
log source  ──►  ingest (download / point at)  ──►  parse  ──►  datasets (Parquet, optional DuckDB)  ──►  UI
(local | volume | adls | s3)                                                                      (FastAPI + React)
```

## Get started

Only **Python 3.11+** is needed (the web UI is already built and ships in the package; no Node.js).

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

### Or run it without installing the package

The app runs straight from the folder: install only the libraries it uses once, then point Python at `src` each
time. Nothing is registered with Python, so a fresh download or a moved folder just works.

**Windows (PowerShell)**, from the repo folder:

```powershell
python -m pip install pandas pyarrow duckdb click fastapi uvicorn   # once
$env:PYTHONPATH = "src"                                            # in every new PowerShell window
python -m databricks_cluster_log_analyzer ui --output C:\dbx\output --cache C:\dbx\cache
```

**macOS / Linux**, from the repo folder:

```bash
python3 -m pip install pandas pyarrow duckdb click fastapi uvicorn   # once
PYTHONPATH=src python3 -m databricks_cluster_log_analyzer ui --output ~/dbx/output --cache ~/dbx/cache
```

Every command in this README works this way: write `python -m databricks_cluster_log_analyzer` where it says
`dbx-log-analyzer`, for example `python -m databricks_cluster_log_analyzer analyze --root ... --cluster ...`. For a
cloud source, also install its libraries: `databricks-sdk` (Unity Catalog volumes), `azure-storage-file-datalake
azure-identity` (ADLS) or `boto3` (S3).

What the libraries are for: `pandas` and `pyarrow` read the logs and write the Parquet datasets, `duckdb` queries them
for the UI, `fastapi` and `uvicorn` are the local web server (it listens on your machine only), `click` is the
command line.

### Then

A browser tab opens at http://127.0.0.1:8765. On **Home**: pick **Local folder**, paste the folder that *contains*
the cluster-id folder (e.g. `C:\logs\cluster_logs` for `C:\logs\cluster_logs\1006-120000-sample01\`), pick the
cluster, then **Analyze**. Results are written to `<output>\<cluster_id>\`; local log folders are read in place.

- **Update**: `git pull` (or download the zip again), then start the UI again with the same command. With the
  package installed, run `pip install -e .` again only if you moved the folder or use a new download; without it,
  nothing to redo.
- **No module named databricks_cluster_log_analyzer**: the package is not installed for this folder (or was
  installed from a folder that moved). Run `python -m pip install -e .` here, or set `PYTHONPATH` to `src` as above. Clusters analyzed before an update still open; click **Analyze again** for the newest charts.
- **Command not found**: use `python -m databricks_cluster_log_analyzer ui --output ... --cache ...` (Python's
  Scripts folder is not on PATH).
- **Port in use**: add `--port 8766` and open http://127.0.0.1:8766.
- **Defaults**: without `--output` / `--cache` the UI uses `./output` and `./cache` under the folder you start it in.
  Keep them outside the repo for real logs, so nothing from a customer cluster can be committed by accident.
- **Cloud sources** (Unity Catalog volume, ADLS, S3): install the extra first, see [Install](#install).

## Install

```bash
pip install -e .                       # Python 3.11+ (local folders only)
pip install -e ".[databricks]"         # + Unity Catalog volumes (databricks-sdk)
pip install -e ".[adls]"               # + Azure Data Lake Storage Gen2 (azure-storage-file-datalake, azure-identity)
pip install -e ".[s3]"                 # + Amazon S3 (boto3)
pip install -e ".[all]"                # all of the above
```

The cloud SDKs are optional and imported only when that source is used; without them the tool says which extra to
install (`dbx-log-analyzer sources` shows what is available).

Only Python is needed to run it: the built web UI is committed in `src/databricks_cluster_log_analyzer/api/static`,
so after `git pull` (or unzipping) run `pip install -e .` once and then `dbx-log-analyzer ui`. Node.js is only for
changing the UI: `cd frontend && npm ci && npm run build` rebuilds `frontend/dist` and copies it into `api/static`
(commit both the source change and `api/static`).

## Sources

| `--source` | `--root` | options | credentials |
|---|---|---|---|
| `local` | a folder containing cluster folders | | |
| `volume` | `/Volumes/<catalog>/<schema>/<volume>/<path>` | `--profile`, `--host`, `--token` | `~/.databrickscfg` profile or `DATABRICKS_*` env |
| `adls` | `abfss://<container>@<account>.dfs.core.windows.net/<path>` | `--account-name`, `--container`, `--sas-token`, `--account-key` | SAS / key, else `DefaultAzureCredential` (az login, managed identity) |
| `s3` | `s3://<bucket>/<prefix>` | `--profile`, `--region`, `--endpoint-url` | default AWS credential chain or a named profile |

Remote sources are copied into `./cache/<cluster_id>/` incrementally (files with the same size and modification
time are skipped); local folders are built in place.

## Quick start

```bash
# which clusters are there (newest driver activity first)?
dbx-log-analyzer clusters --source volume --root /Volumes/cat/sch/vol/cluster_logs --profile DEFAULT
dbx-log-analyzer clusters --source adls --root abfss://logs@myacct.dfs.core.windows.net/cluster-logs
dbx-log-analyzer clusters --source s3 --root s3://my-bucket/cluster-logs --profile prod
dbx-log-analyzer clusters --root ./my-logs                                  # local

# download (remote, incremental) + parse into ./output/<cluster_id>/
dbx-log-analyzer analyze --source volume --root /Volumes/cat/sch/vol/cluster_logs --cluster 1006-120000-sample01
dbx-log-analyzer analyze --source s3 --root s3://my-bucket/cluster-logs --latest 5
dbx-log-analyzer download --volume /Volumes/cat/sch/vol/cluster_logs --cluster 1006-120000-sample01   # old flag still works

# parse a local folder
dbx-log-analyzer build --input ./cache/0101-000000-abcd1234 [--output ./output] [--duckdb]
dbx-log-analyzer build --root ./my-logs --cluster 0101-000000-abcd1234

# outputs built by an older version: add the newer findings (waiting for cores, cores full, MERGE reading too
# much) from their datasets, without the raw logs
dbx-log-analyzer refresh-findings --output ./output [--cluster <id>]

# look at it
dbx-log-analyzer clusters                 # built clusters
dbx-log-analyzer ui                       # http://127.0.0.1:8765
```

In the UI the home page does the same: pick a source type, enter the root (and options), pick a cluster from the
list (with its last-modified time and an "analyzed" badge), then Analyze.

Any folder in the Databricks log-delivery layout works:

```
<root>/<cluster_id>/
  driver/     log4j-active.log, log4j-<yyyy-mm-dd-HH>.log.gz, stdout, stdout--<date>, stderr, stderr--<date>,
              stacktrace.log, <date>.stacktrace.log.gz
  executor/   <app-id>/<executor-id>/stdout, stderr (+ rolled stdout--<date>.gz, stderr--<date>.gz)
  eventlog/   <cluster_id>_<hash>/<spark_context_id>/eventlog (+ rolled eventlog-<date>.gz)
  init_scripts/ (ignored)
```

Serverless compute produces no cluster logs; an empty folder is built anyway and its `summary.json` says why.

## What is parsed

- Log lines with log4j (`26/10/06 18:00:01 INFO ...`), JVM unified-logging (`[2026-10-06T18:00:01.123+0000][..][info][gc] ...`)
  and ISO-8601 timestamps; multi-line messages and stack traces under a log record are marked as continuation
  lines and keep its level.
- Known problem signals (rules.toml), exceptions grouped by fingerprint with the first user-code frame; JVM
  thread dumps are recognised and skipped.
- JVM GC pauses (JDK 9+ unified `[gc]` lines and JDK 8 `[GC (...)` lines) into `gc_events`.
- The Spark event log: tasks, stages, jobs, SQL queries, executors (removal reason categorised as oom, killed,
  lost, autoscale, termination), Spark Connect operations, cluster info (`clusterUsageTags`) and every event type
  counted.
- Retries: which tasks failed first, on which executor, why, and whether the retry worked, in plain English
  ("Stage 12 task 7 failed first on executor 3: ExecutorLostFailure (executor killed: Command exited with code 9).
  Retried on executor 1 after 42 s and succeeded; 3 m 10 s of work lost.").
- What each job / stage / query is doing: stage DAG parents, RDD and operator names, the call-site stack, and per
  SQL query an operator summary (Scan parquet sales.orders, Filter, Exchange hashpartitioning, SortMergeJoin, ...)
  with the tables read and written, parsed from the physical plan.

## Datasets

Written to `output/<cluster_id>/<name>.parquet` with explicit schemas (empty datasets keep their columns), plus
`summary.json` (counts, totals, cluster info, retries, findings by severity and an ordered plain-English
diagnosis). `schemas.py` lists every column.

| dataset | grain |
|---|---|
| `files`, `file_lines` | file in the cluster folder; lines, bytes and thread dumps per log file |
| `apps`, `cluster_info` | Spark context (one per cluster start); cluster name, runtime, node types, workers, job/run ids |
| `event_counts` | event type per Spark context (including types not read) |
| `log_lines` | driver + executor log line (timestamp, level, logger, message, signal, continuation) |
| `log_signals` | line matching a known problem pattern |
| `log_errors` | exception with top frames, fingerprint and `user_frame` (first non-framework frame) |
| `gc_events` | JVM GC pause / cycle with heap before/after |
| `tasks`, `stages`, `spark_jobs`, `sql_queries`, `executors` | from the Spark event log |
| `connect_operations` | Spark Connect statement (user, text, timing, status), linked from `spark_jobs.connect_operation_id` |
| `task_retries` | task (partition) that failed at least once: first failure, cause, retry outcome, wasted time |
| `spill_shuffle_timeline` | time bucket (default 1 min) x executor x stage attempt: spill, shuffle, I/O bytes, GC and run time |
| `findings` | ranked problems with evidence, fix and links |
| `incidents` | one row per finding: its incident, problem, role (root cause / effect / same problem), cause, why linked, confidence, resolved stage / executor / query |
| `sql_plan_nodes` | physical plan operator per SQL query (initial and final AQE plan) with its metrics and stages |
| `hotspots` | peaks: skewed tasks, shuffle and spill peaks, slowest stages and operators, with the likely cause |
| `timeline` | log signals per minute |
| `query_profile` | one row per SQL query with its stages, spill, GC, skew, bytes, executors lost, findings |
| `stage_executor_profile` | stage x executor: did one executor carry a stage? |
| `executor_profile` | executor lifetime, busy share, spill, GC (task GC time and JVM pauses), removal reason, its own log signals/exceptions |
| `run_story` | one ordered stream of scheduler events, retries, important log lines and findings |

`--duckdb` also loads them into `output/insights.duckdb` (one schema per cluster: `c_<cluster_id>`).

The UI's **Data / Debug** tab shows the notebook's intermediate tables step by step (Setup S1–S2, Driver D1–D5,
Executor E1–E6, Event log V1–V8, Analysis A1–A4, Combined C1–C4), served by `/api/clusters/<id>/steps`.

The **Hierarchy** view is a graph (app, Spark jobs, stages, SQL queries, Spark Connect statements) with stage
dependencies, attempts and retries, spill / shuffle / skew / GC flags and a "what is this doing" panel, served by
`/api/clusters/<id>/graph?ctx=`; `/api/clusters/<id>/spill-shuffle?ctx=&by=executor|stage` feeds the
"Spill & shuffle over time" chart.

## Rules

Thresholds (skew, spill, GC, tiny tasks, Full GC, shuffle-heavy), the graph size cap, the spill/shuffle timeline
bucket, the log `SIGNALS` patterns, the event types read, the executor
removal-reason categories (regexes and their order) and the framework stack-frame prefixes all live in one file:
[`src/databricks_cluster_log_analyzer/rules.toml`](src/databricks_cluster_log_analyzer/rules.toml).
Override with `--rules my_rules.toml` or `DBX_LOG_ANALYZER_RULES=my_rules.toml` (merged over the defaults).

## How it is built

- Parsing is streaming: files are read line by line (gzip detected by magic bytes), event-log lines are filtered
  by event name as a string before `json.loads`, and `log_lines` is written to Parquet in batches. Transforms use
  pandas; DuckDB serves the UI queries.
- Parsing rules, regexes, signals, thresholds and finding rules are ported from an earlier Databricks notebook;
  `tests/test_parity.py` checks the findings match and lists the intentional differences.
- Sources implement a small interface (`list_clusters`, `list_files`, `open`) and are registered in
  `sources/registry.py` (type, label, input fields, availability), which the CLI and the API share.
- UI: FastAPI serving the datasets through DuckDB, React + Vite front end (`frontend/`).
