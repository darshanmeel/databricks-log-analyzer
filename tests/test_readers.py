"""readers.open_text_lines / classify_files, and the fixture generator itself."""

from __future__ import annotations

import gzip
import hashlib
from pathlib import Path

import pytest

import make_fixtures as mf

readers = pytest.importorskip("databricks_cluster_log_analyzer.readers")


def _tree_hash(root: Path) -> dict:
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


# ---------------------------------------------------------------------------------------------- generator
def test_generator_is_deterministic(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    ids_a = mf.generate(a)
    ids_b = mf.generate(b)
    assert ids_a == ids_b == {"main": mf.MAIN, "healthy": mf.HEALTHY, "empty": mf.EMPTY, "rev3": mf.REV3}
    assert _tree_hash(a) == _tree_hash(b)


def test_generator_layout(fixture_root):
    c = fixture_root / mf.MAIN
    assert (c / "driver" / "log4j-active.log").is_file()
    assert (c / "driver" / "log4j-2026-10-06-17.log.gz").read_bytes()[:2] == b"\x1f\x8b"
    assert (c / "driver" / "stderr--2026-10-06--17-00").is_file()
    assert (c / "driver" / "stacktrace.log").is_file()
    assert (c / "executor" / mf.APP_A / "1" / "stderr").is_file()
    ev = c / "eventlog" / mf.HASH_A / mf.CTX_A
    assert (ev / "eventlog").is_file()
    assert (ev / "eventlog-2026-10-06--18-00.gz").read_bytes()[:2] == b"\x1f\x8b"
    assert (c / "eventlog" / mf.HASH_B / mf.CTX_B / "eventlog").is_file()
    assert (fixture_root / mf.EMPTY).is_dir()
    assert not any((fixture_root / mf.EMPTY).iterdir())


# ---------------------------------------------------------------------------------------------- open_text_lines
def test_open_text_lines_plain_strips_crlf_and_replaces_bad_utf8(tmp_path):
    p = tmp_path / "stdout"
    p.write_bytes(b"first\r\nsecond \xff\xfe bad\nthird")
    lines = list(readers.open_text_lines(p))
    assert lines[0] == "first"
    assert lines[1].startswith("second ") and lines[1].endswith(" bad") and "�" in lines[1]
    assert lines[2] == "third"
    assert len(lines) == 3


def test_open_text_lines_gzip(tmp_path):
    p = tmp_path / "log4j-2026-10-06-17.log.gz"
    p.write_bytes(gzip.compress(b"26/10/06 17:48:12 INFO A: x\r\n26/10/06 17:48:13 WARN B: y\n", mtime=0))
    assert list(readers.open_text_lines(p)) == ["26/10/06 17:48:12 INFO A: x", "26/10/06 17:48:13 WARN B: y"]


def test_open_text_lines_rolled_eventlog_gz(fixture_root):
    p = fixture_root / mf.MAIN / "eventlog" / mf.HASH_A / mf.CTX_A / "eventlog-2026-10-06--18-00.gz"
    lines = list(readers.open_text_lines(p))
    assert lines[0].startswith("{") and "SparkListenerLogStart" in lines[0]
    assert sum('"SparkListenerTaskEnd"' in ln for ln in lines) >= 2000


# ---------------------------------------------------------------------------------------------- classify_files
def _names(paths):
    return [Path(p).as_posix() for p in paths]


def test_classify_files_ordering(fixture_root):
    out = readers.classify_files(fixture_root / mf.MAIN)
    for k in ("driver", "executor", "eventlog"):
        assert k in out
    drv = [Path(p).name for p in out["driver"]]
    assert drv.index("log4j-2026-10-06-17.log.gz") < drv.index("log4j-active.log")
    assert drv.index("stderr--2026-10-06--17-00") < drv.index("stderr")
    assert set(drv) == {"log4j-2026-10-06-17.log.gz", "log4j-active.log", "stdout", "stderr",
                        "stderr--2026-10-06--17-00", "stacktrace.log"}

    ex = _names(out["executor"])
    # executor folders sorted by (app_id, numeric executor_id); rolled stderr before stderr
    order = [(p.split("/")[-3], p.split("/")[-2]) for p in ex]
    keys = [(app, int(e)) for app, e in order]
    assert keys == sorted(keys)
    e0 = [p.split("/")[-1] for p in ex if f"{mf.APP_A}/0/" in p]
    assert e0.index("stderr--2026-10-06--17-00") < e0.index("stderr")

    ev = _names(out["eventlog"])
    a = [p.split("/")[-1] for p in ev if f"/{mf.CTX_A}/" in p]
    assert a == ["eventlog-2026-10-06--18-00.gz", "eventlog"]
    assert any(f"/{mf.CTX_B}/" in p for p in ev)


def test_classify_files_empty_cluster(fixture_root):
    out = readers.classify_files(fixture_root / mf.EMPTY)
    assert all(len(v) == 0 for v in out.values())
