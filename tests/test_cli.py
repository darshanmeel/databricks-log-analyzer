"""CLI (click CliRunner): build / clusters with the Revision 2 `--source local --root --cluster` options."""

from __future__ import annotations

import json
import sys

import pytest

import make_fixtures as mf

click_testing = pytest.importorskip("click.testing")
cli = pytest.importorskip("databricks_cluster_log_analyzer.cli")
main = getattr(cli, "main", None) or getattr(cli, "cli")


def run(args, ok=True):
    res = click_testing.CliRunner().invoke(main, [str(a) for a in args], catch_exceptions=True)
    out = (res.output or "") + (str(res.exception) if res.exception and not isinstance(res.exception, SystemExit) else "")
    if ok:
        assert res.exit_code == 0, (args, res.exit_code, out[-2000:])
    return res, out


def test_help_lists_commands():
    _, out = run(["--help"])
    for cmd in ("build", "download", "analyze", "clusters", "ui"):
        assert cmd in out, cmd
    _, out = run(["build", "--help"])
    for opt in ("--source", "--root", "--cluster", "--output", "--input"):
        assert opt in out, opt
    _, out = run(["clusters", "--help"])
    assert "--source" in out and "--root" in out


def test_build_source_local(fixture_root, tmp_path):
    out_dir = tmp_path / "out"
    _, out = run(["build", "--source", "local", "--root", fixture_root, "--cluster", mf.HEALTHY,
                  "--output", out_dir])
    s = json.loads((out_dir / mf.HEALTHY / "summary.json").read_text("utf-8"))
    assert s["cluster_id"] == mf.HEALTHY and s["status"] == "succeeded"
    assert (out_dir / mf.HEALTHY / "log_lines.parquet").is_file()
    assert mf.HEALTHY in out  # prints a short summary


def test_build_rev3_writes_revision3_datasets(fixture_root, tmp_path):
    out_dir = tmp_path / "out"
    run(["build", "--source", "local", "--root", fixture_root, "--cluster", mf.REV3, "--output", out_dir])
    for name in ("gc_events", "connect_operations", "cluster_info", "task_retries", "event_counts", "file_lines"):
        assert (out_dir / mf.REV3 / f"{name}.parquet").is_file(), name
    assert json.loads((out_dir / mf.REV3 / "summary.json").read_text("utf-8"))["status"] == "succeeded"


def test_build_root_defaults_to_local_and_input(fixture_root, tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    run(["build", "--root", fixture_root, "--cluster", mf.HEALTHY, "--output", a])
    run(["build", "--input", fixture_root / mf.HEALTHY, "--output", b])
    assert (a / mf.HEALTHY / "summary.json").is_file()
    assert (b / mf.HEALTHY / "summary.json").is_file()


def test_build_empty_cluster(fixture_root, tmp_path):
    out_dir = tmp_path / "out"
    run(["build", "--source", "local", "--root", fixture_root, "--cluster", mf.EMPTY, "--output", out_dir])
    s = json.loads((out_dir / mf.EMPTY / "summary.json").read_text("utf-8"))
    assert s["empty_reason"]


def test_build_missing_cluster_fails(fixture_root, tmp_path):
    res, out = run(["build", "--source", "local", "--root", fixture_root, "--cluster", "9999-999999-nothere1",
                    "--output", tmp_path / "out"], ok=False)
    assert res.exit_code != 0
    assert not (tmp_path / "out" / "9999-999999-nothere1" / "summary.json").exists()


def test_clusters_source_local_lists_folders(fixture_root):
    _, out = run(["clusters", "--source", "local", "--root", fixture_root])
    for cid in (mf.MAIN, mf.HEALTHY, mf.REV3):
        assert cid in out, cid


def test_clusters_lists_built(fixture_root, tmp_path):
    out_dir = tmp_path / "out"
    run(["build", "--source", "local", "--root", fixture_root, "--cluster", mf.HEALTHY, "--output", out_dir])
    _, out = run(["clusters", "--output", out_dir])
    assert mf.HEALTHY in out and mf.MAIN not in out


def test_clusters_s3_missing_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", None)
    res, out = run(["clusters", "--source", "s3", "--root", "s3://example-bucket/cluster-logs"], ok=False)
    assert res.exit_code != 0
    low = out.lower()
    assert ("s3" in low or "boto3" in low) and ("install" in low or "extra" in low), out[-1000:]


def test_unknown_source_rejected(fixture_root):
    res, _ = run(["clusters", "--source", "ftp", "--root", fixture_root], ok=False)
    assert res.exit_code != 0
