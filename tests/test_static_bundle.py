"""The built UI is committed into the package (api/static) so a machine without Node can run it with only Python."""
import re
from pathlib import Path

STATIC = Path(__file__).resolve().parents[1] / "src" / "databricks_cluster_log_analyzer" / "api" / "static"


def test_static_bundle_is_complete():
    index = STATIC / "index.html"
    assert index.is_file(), "run `npm run build` in frontend/ (it copies dist/ to api/static)"
    refs = re.findall(r'(?:src|href)="/?(assets/[^"]+)"', index.read_text(encoding="utf-8"))
    assert any(r.endswith(".js") for r in refs)
    for r in refs:
        assert (STATIC / r).is_file(), r
