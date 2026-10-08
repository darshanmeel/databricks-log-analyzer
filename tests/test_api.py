"""HTTP API (FastAPI TestClient against create_app(output_root, cache_root))."""

from __future__ import annotations

import re
from datetime import timedelta

import pytest

import make_fixtures as mf
from conftest import ms

pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

server = pytest.importorskip("databricks_cluster_log_analyzer.api.server")

A, B = mf.CTX_A, mf.CTX_B
MAIN = mf.MAIN
ENVELOPE = {"columns", "rows", "total", "limit", "offset"}


def ta(sec):
    return ms(mf.T0A + timedelta(seconds=sec))


@pytest.fixture(scope="module")
def client(output_root, cache_root):
    app = server.create_app(output_root, cache_root)
    with TestClient(app) as c:
        yield c


def get(client, url, status=200, **params):
    r = client.get(url, params=params or None)
    assert r.status_code == status, (url, r.status_code, r.text[:500])
    return r.json()


def assert_epoch_ms(v, nullable=True):
    if v is None:
        assert nullable
        return
    assert isinstance(v, int) and not isinstance(v, bool), v
    assert 1_500_000_000_000 < v < 2_500_000_000_000, v  # epoch ms, not s / ns


def assert_envelope(body):
    assert set(body) >= ENVELOPE
    assert isinstance(body["columns"], list) and isinstance(body["rows"], list)
    assert isinstance(body["total"], int) and isinstance(body["limit"], int) and isinstance(body["offset"], int)
    for r in body["rows"]:
        assert isinstance(r, dict)


# ---------------------------------------------------------------------------------------------- basics
def test_health(client):
    body = get(client, "/api/health")
    assert body["ok"] is True and isinstance(body["version"], str)


def test_clusters(client):
    body = get(client, "/api/clusters")
    assert isinstance(body, list)
    by_id = {c["cluster_id"]: c for c in body}
    assert {MAIN, mf.HEALTHY, mf.EMPTY} <= set(by_id)
    for c in body:
        for k in ("cluster_id", "built_at", "status", "start_time", "end_time", "duration_ms", "counts",
                  "findings_by_severity", "empty_reason"):
            assert k in c, k
        assert_epoch_ms(c["start_time"])
        assert_epoch_ms(c["end_time"])
    assert by_id[MAIN]["status"] == "failed"
    assert by_id[MAIN]["start_time"] == ta(0)
    assert by_id[mf.EMPTY]["empty_reason"]
    built = [c["built_at"] for c in body]
    assert built == sorted(built, reverse=True)  # newest built first


def test_summary(client):
    s = get(client, f"/api/clusters/{MAIN}/summary")
    assert s["cluster_id"] == MAIN and s["status"] == "failed"
    assert isinstance(s["diagnosis"], list) and s["diagnosis"]
    get(client, "/api/clusters/9999-999999-nothere1/summary", status=404)


# ---------------------------------------------------------------------------------------------- datasets
def test_dataset_envelope_and_paging(client):
    body = get(client, f"/api/clusters/{MAIN}/datasets/stages", limit=5)
    assert_envelope(body)
    assert body["total"] == 12 and body["limit"] == 5 and body["offset"] == 0 and len(body["rows"]) == 5
    assert "stage_id" in body["columns"]
    for r in body["rows"]:
        assert_epoch_ms(r["start_time"])
        assert_epoch_ms(r["end_time"])
    page2 = get(client, f"/api/clusters/{MAIN}/datasets/stages", limit=5, offset=10)
    assert page2["offset"] == 10 and len(page2["rows"]) == 2


def test_dataset_default_and_max_limit(client):
    body = get(client, f"/api/clusters/{MAIN}/datasets/tasks")
    assert body["limit"] == 200 and len(body["rows"]) == 200 and body["total"] == 2067
    r = client.get(f"/api/clusters/{MAIN}/datasets/tasks", params={"limit": 100000})
    if r.status_code == 200:
        assert r.json()["limit"] <= 5000 and len(r.json()["rows"]) <= 5000
    else:
        assert r.status_code in (400, 422)


def test_dataset_filters_sort_search(client):
    url = f"/api/clusters/{MAIN}/datasets/stages"
    a = get(client, url, spark_context_id=A, limit=100)
    assert a["total"] == 9 and all(r["spark_context_id"] == A for r in a["rows"])
    top = get(client, url, sort="duration_ms", desc="true", limit=1)["rows"][0]
    assert top["stage_id"] == 0 and top["spark_context_id"] == A and top["duration_ms"] == 902_000
    asc = get(client, url, sort="duration_ms", desc="false", limit=100)["rows"]
    d = [r["duration_ms"] for r in asc if r["duration_ms"] is not None]
    assert d == sorted(d)
    q = get(client, url, q="MAPPARTITIONS", limit=100)  # case-insensitive substring over string cols
    # matches any string or list-of-string column (stage_name, rdd_names, details)
    assert q["total"] >= 2 and all("mappartitions" in str(r).lower() for r in q["rows"])
    assert sum("mapPartitions" in r["stage_name"] for r in q["rows"]) == 2
    cols = get(client, url, columns="stage_id,status", limit=3)
    assert set(cols["columns"]) == {"stage_id", "status"}
    assert all(set(r) == {"stage_id", "status"} for r in cols["rows"])


def test_dataset_repeatable_filter_is_in(client):
    r = client.get(f"/api/clusters/{MAIN}/datasets/stages",
                   params=[("stage_id", "0"), ("stage_id", "6"), ("spark_context_id", A), ("limit", "100")])
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert {(x["stage_id"], x["stage_attempt"]) for x in rows} == {(0, 0), (6, 0), (6, 1)}


def test_dataset_ts_range(client):
    url = f"/api/clusters/{MAIN}/datasets/stages"
    body = get(client, url, ts_from=ta(1100), ts_to=ta(1200), spark_context_id=A, limit=100)
    starts = [r["start_time"] for r in body["rows"]]
    assert starts and all(ta(1100) <= s <= ta(1200) for s in starts)
    assert {r["stage_id"] for r in body["rows"]} == {5, 6}


def test_dataset_lists_and_nulls_serialize(client):
    body = get(client, f"/api/clusters/{MAIN}/datasets/spark_jobs", spark_context_id=A, spark_job_id=1)
    row = body["rows"][0]
    assert row["stage_ids"] == [1, 2]
    errs = get(client, f"/api/clusters/{MAIN}/datasets/log_errors", limit=500)["rows"]
    assert all(isinstance(e["top_frames"], list) for e in errs)
    assert any(e["user_frame"] is None for e in errs)
    for r in get(client, f"/api/clusters/{MAIN}/datasets/log_lines", limit=500)["rows"]:
        assert_epoch_ms(r["ts"])


@pytest.mark.parametrize("name", ["files", "apps", "log_lines", "log_signals", "log_errors", "tasks", "stages",
                                  "spark_jobs", "sql_queries", "executors", "findings", "timeline",
                                  "query_profile", "stage_executor_profile", "executor_profile", "run_story"])
def test_every_dataset_served(client, name):
    body = get(client, f"/api/clusters/{MAIN}/datasets/{name}", limit=3)
    assert_envelope(body)
    assert body["total"] > 0
    empty = get(client, f"/api/clusters/{mf.EMPTY}/datasets/{name}", limit=3)
    assert empty["total"] == 0 and empty["rows"] == []


def test_dataset_404s(client):
    get(client, f"/api/clusters/{MAIN}/datasets/not_a_dataset", status=404)
    get(client, "/api/clusters/9999-999999-nothere1/datasets/stages", status=404)


# ---------------------------------------------------------------------------------------------- hierarchy
def test_hierarchy(client):
    h = get(client, f"/api/clusters/{MAIN}/hierarchy")
    apps = {a["spark_context_id"]: a for a in h["apps"]}
    assert set(apps) == {A, B}
    a = apps[A]
    assert a["app_id"] == mf.APP_A
    assert_epoch_ms(a["start_time"])
    jobs = {j["spark_job_id"]: j for j in a["jobs"]}
    assert set(jobs) == {0, 1, 2, 3, 4}
    assert "status" in jobs[4]
    assert {(s["stage_id"], s["stage_attempt"]) for s in jobs[0]["stages"]} >= {(0, 0), (1, 0)}
    assert {(s["stage_id"], s["stage_attempt"]) for s in jobs[4]["stages"]} == {(5, 0), (6, 0), (5, 1), (6, 1)}
    for s in jobs[4]["stages"]:
        assert_epoch_ms(s["start_time"])
    assert isinstance(a["queries"], list) and len(a["queries"]) == 4
    assert isinstance(a["orphan_stages"], list)
    assert len(apps[B]["jobs"]) == 2


# ---------------------------------------------------------------------------------------------- stage detail
def test_stage_detail(client):
    body = get(client, f"/api/clusters/{MAIN}/stages/{A}/0/0")
    assert body["stage"]["stage_id"] == 0 and body["stage"]["skew"] == pytest.approx(180.0)
    ts = body["tasks_summary"]
    for k in ("min", "p25", "p50", "p75", "p90", "p99", "max"):
        assert k in ts
    assert ts["max"] == 900_000 and ts["min"] == 5000
    assert len(body["task_durations"]) == 20
    assert {e["executor_id"] for e in body["executors"]} == {"0", "1", "2", "3"}
    assert "task_skew" in {f["category"] for f in body["findings"]}
    assert body["job"]["spark_job_id"] == 0
    assert body["query"]["sql_execution_id"] == 0
    assert "final_plan" not in body["query"] and "initial_plan" not in body["query"]
    tl = body["task_timeline"]
    assert len(tl) == 20 and body["task_timeline_sampled"] is False
    assert max(t["task_ms"] for t in tl) == 900_000
    assert [t["launch_time"] for t in tl] == sorted(t["launch_time"] for t in tl)
    assert body["stage"]["min_task_ms"] == 5000


def test_stage_task_timeline_sampled(client, monkeypatch):
    from databricks_cluster_log_analyzer.api import queries as q
    monkeypatch.setattr(q, "MAX_TIMELINE_TASKS", 300)
    body = get(client, f"/api/clusters/{MAIN}/stages/{A}/2/0")
    tl = body["task_timeline"]
    assert body["task_timeline_sampled"] is True and 100 < len(tl) < 2000
    longest = max(body["task_durations"])
    assert max(t["task_ms"] for t in tl) == longest  # the longest task is always drawn


def test_stage_detail_404(client):
    get(client, f"/api/clusters/{MAIN}/stages/{A}/99/0", status=404)


def test_stage_detail_without_query(client):
    body = get(client, f"/api/clusters/{MAIN}/stages/{A}/4/0")
    assert body["stage"]["failed_tasks"] == 2
    assert body["query"] is None
    assert "task_retries" in {f["category"] for f in body["findings"]}


# ---------------------------------------------------------------------------------------------- query detail
def test_query_detail(client):
    body = get(client, f"/api/clusters/{MAIN}/queries/{A}/3")
    q = body["query"]
    assert q["status"] == "failed"
    assert "AQEShuffleRead" in q["final_plan"] and q["initial_plan"].strip() == mf.PLAN_Q3_INITIAL.strip()
    assert_epoch_ms(q["start_time"], nullable=False)
    assert body["profile"]["sql_execution_id"] == 3
    assert [j["spark_job_id"] for j in body["jobs"]] == [4]
    assert len(body["stages"]) == 4
    assert "query_failed" in {f["category"] for f in body["findings"]}
    get(client, f"/api/clusters/{MAIN}/queries/{A}/42", status=404)


def test_plan_diff(client):
    url = f"/api/clusters/{MAIN}/plan-diff"
    same = get(client, url, a_ctx=A, a_id=0, b_ctx=B, b_id=0, which="final")
    assert set(same) >= {"a", "b", "diff"}
    # the two plans differ only in expression ids on the FileScan / header lines -> those lines are "equal"
    eq = [d["text"] for d in same["diff"] if d["op"] == "equal"]
    assert any("FileScan parquet sales.orders" in t for t in eq)
    assert any("== Physical Plan ==" in t for t in eq)
    diff = get(client, url, a_ctx=A, a_id=3, b_ctx=A, b_id=3, which="initial")
    assert all(d["op"] == "equal" for d in diff["diff"])
    other = get(client, url, a_ctx=A, a_id=0, b_ctx=A, b_id=3, which="final")
    ops = {d["op"] for d in other["diff"]}
    assert ops <= {"equal", "insert", "delete"} and ("insert" in ops or "delete" in ops)
    for d in other["diff"]:
        assert set(d) >= {"op", "a_line", "b_line", "text"}
        assert not re.search(r"#\d", d["text"])  # expression ids stripped


# ---------------------------------------------------------------------------------------------- gantt
def test_gantt(client):
    g = get(client, f"/api/clusters/{MAIN}/gantt", ctx=A)
    for k in ("ctx", "start", "end", "jobs", "stages", "executors", "tasks", "tasks_total", "sampled", "markers"):
        assert k in g, k
    assert g["ctx"] == A
    assert_epoch_ms(g["start"], nullable=False)
    assert_epoch_ms(g["end"], nullable=False)
    assert g["tasks_total"] == 2057 and g["sampled"] is False and len(g["tasks"]) == 2057
    t = g["tasks"][0]
    assert set(t) >= {"task_id", "stage_id", "stage_attempt", "executor_id", "start", "end", "failed"}
    assert_epoch_ms(t["start"])
    assert {j["spark_job_id"] for j in g["jobs"]} == {0, 1, 2, 3, 4}
    assert len(g["stages"]) == 9
    ex = {e["executor_id"]: e for e in g["executors"]}
    assert ex["1"]["removal_category"] == "oom" and ex["2"]["removal_category"] == "lost"
    kinds = {m["kind"] for m in g["markers"]}
    assert kinds <= {"oom", "executor_lost", "executor_added", "executor_removed", "signal", "error", "spill", "full_gc"}
    assert "oom" in kinds
    for m in g["markers"]:
        assert_epoch_ms(m["ts"], nullable=False)


def test_gantt_sampling(client):
    g = get(client, f"/api/clusters/{MAIN}/gantt", ctx=A, max_tasks=100)
    assert g["sampled"] is True and g["tasks_total"] == 2057
    # cap is max_tasks; implementations may additionally keep every failed task attempt (12 in context A)
    assert len(g["tasks"]) <= 100 + 12


def test_gantt_requires_ctx_when_several(client):
    r = client.get(f"/api/clusters/{MAIN}/gantt")
    assert r.status_code in (400, 422)
    one = get(client, f"/api/clusters/{mf.HEALTHY}/gantt")  # only one context -> ctx optional
    assert one["ctx"] == mf.CTX_H


# ---------------------------------------------------------------------------------------------- logs / facets
def test_logs_context(client):
    errs = get(client, f"/api/clusters/{MAIN}/datasets/log_errors",
               exception_class="java.lang.OutOfMemoryError", limit=10)["rows"]
    e = errs[0]
    body = get(client, f"/api/clusters/{MAIN}/logs/context", file_path=e["file_path"], seq=e["seq"],
               before=2, after=3)
    assert_envelope(body)
    seqs = [r["seq"] for r in body["rows"]]
    assert e["seq"] in seqs and len(seqs) <= 6
    assert seqs == sorted(seqs)
    assert all(r["file_path"] == e["file_path"] for r in body["rows"])


def test_facets(client):
    body = get(client, f"/api/clusters/{MAIN}/facets/log_lines", column="level")
    assert isinstance(body, list)
    vals = {f["value"]: f["count"] for f in body}
    assert {"INFO", "WARN", "ERROR"} <= set(vals)
    assert all(isinstance(c, int) for c in vals.values())
    counts = [f["count"] for f in body]
    assert counts == sorted(counts, reverse=True)


def test_errors_grouped(client):
    body = get(client, f"/api/clusters/{MAIN}/errors")
    assert isinstance(body, list) and body
    for e in body:
        for k in ("fingerprint", "exception_class", "occurrences", "executors_affected", "first_seen", "last_seen",
                  "sample_message", "sample_stack", "user_frame", "sources", "sample_file_path", "sample_seq"):
            assert k in e, k
        assert_epoch_ms(e["first_seen"])
    oom = [e for e in body if e["exception_class"] == "java.lang.OutOfMemoryError"]
    assert len(oom) == 1 and oom[0]["occurrences"] == 2 and oom[0]["executors_affected"] == 1
    assert isinstance(oom[0]["sample_stack"], list)
    assert any(e["user_frame"] == mf.USER_FRAME for e in body)


def test_signals_grouped(client):
    body = get(client, f"/api/clusters/{MAIN}/signals")
    assert isinstance(body, list) and body
    for s in body:
        for k in ("signal", "severity", "fix", "occurrences", "executors_affected", "first_seen", "last_seen",
                  "sample_line", "sample_file_path", "sample_seq"):
            assert k in s, k
    sev = [{"high": 0, "medium": 1, "low": 2}[s["severity"]] for s in body]
    assert sev == sorted(sev)
    spill = [s for s in body if s["signal"] == "disk_spill"][0]
    assert spill["occurrences"] == 5 and spill["executors_affected"] == 1


# ---------------------------------------------------------------------------------------------- analyze
def test_analyze_endpoint(tmp_path, fixture_root, cache_root):
    app = server.create_app(tmp_path / "out", cache_root)
    with TestClient(app) as c:
        r = c.post("/api/analyze", json={"log_root": str(fixture_root), "cluster_id": mf.HEALTHY})
        assert r.status_code == 200, r.text
        assert r.json()["cluster_id"] == mf.HEALTHY and r.json()["status"] == "succeeded"
        r = c.post("/api/analyze", json={"cluster_dir": str(fixture_root / mf.HEALTHY)})
        assert r.status_code == 200
        r = c.post("/api/analyze", json={"cluster_dir": str(tmp_path / "does-not-exist")})
        assert r.status_code == 400 and "detail" in r.json()
        ids = [x["cluster_id"] for x in c.get("/api/clusters").json()]
        assert ids == [mf.HEALTHY]


def test_reanalyze_from_raw_logs(tmp_path, fixture_root, cache_root):
    """A cluster can be analyzed again from the raw logs it was built from, while they are still on disk."""
    import shutil

    raw = tmp_path / "raw"
    shutil.copytree(fixture_root / mf.HEALTHY, raw / mf.HEALTHY)
    app = server.create_app(tmp_path / "out", cache_root)
    with TestClient(app) as c:
        assert c.post("/api/analyze", json={"log_root": str(raw), "cluster_id": mf.HEALTHY}).status_code == 200
        [row] = c.get("/api/clusters").json()
        assert row["raw_available"] is True
        r = c.post(f"/api/clusters/{mf.HEALTHY}/reanalyze")
        assert r.status_code == 200, r.text
        assert r.json()["cluster_id"] == mf.HEALTHY
        shutil.rmtree(raw / mf.HEALTHY)
        [row] = c.get("/api/clusters").json()
        assert row["raw_available"] is False
        r = c.post(f"/api/clusters/{mf.HEALTHY}/reanalyze")
        assert r.status_code == 409 and "no longer" in r.json()["detail"]
        assert c.post("/api/clusters/nope-not-there/reanalyze").status_code == 404


def test_source_clusters_ignores_root_sent_as_option(tmp_path, fixture_root, cache_root):
    """The UI once sent the root among the options too; that must not fail with 'unknown option root'."""
    app = server.create_app(tmp_path / "out", cache_root)
    with TestClient(app) as c:
        r = c.post("/api/sources/clusters", json={"type": "local", "root": str(fixture_root), "options": {"root": str(fixture_root)}})
        assert r.status_code == 200, r.text
        assert mf.HEALTHY in r.text
