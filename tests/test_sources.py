"""Log sources (CONTRACT Revision 2): source registry, LocalSource, VolumeSource / S3Source / ADLSSource with their
SDKs replaced by in-memory fakes (no network), the missing-extra error, incremental ingest skip logic and the
HTTP endpoints driven through a fake remote source.

The contract fixes the source *behaviour* (list_clusters(limit) -> [{cluster_id, last_modified}], list_files, open,
lazy optional SDK imports, clear "install the extra" errors) but not the Python factory name, so the registry is
discovered by a few plausible names and the class constructors are used as a fallback."""

from __future__ import annotations

import importlib
import io
import os
import sys
import types
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

import make_fixtures as mf

src_pkg = pytest.importorskip("databricks_cluster_log_analyzer.sources")
ingest = pytest.importorskip("databricks_cluster_log_analyzer.ingest")
from databricks_cluster_log_analyzer.sources.base import FileInfo, LogSource  # noqa: E402

CLUSTERS = (mf.REV3, mf.HEALTHY)
BUCKET = "example-bucket"
S3_ROOT = f"s3://{BUCKET}/cluster-logs"
ADLS_ACCOUNT = "exampleacct"
ADLS_FS = "logs"
ADLS_ROOT = f"abfss://{ADLS_FS}@{ADLS_ACCOUNT}.dfs.core.windows.net/cluster-logs"
VOLUME_ROOT = "/Volumes/main/ops/logs/cluster_logs"
NEWER = 3600  # HEALTHY's driver files are made 1 h newer in the fake remotes -> listed first


# ============================================================================================ helpers
FACTORY_NAMES = ("make_source", "get_source", "create_source", "build_source", "source_from_type", "open_source",
                 "source_for")
REGISTRY_NAMES = ("SOURCE_TYPES", "SOURCES", "REGISTRY", "source_types", "list_source_types", "available_sources",
                  "describe_sources", "list_sources")


def _modules():
    mods = [src_pkg]
    for sub in ("registry", "factory"):
        try:
            mods.append(importlib.import_module(f"databricks_cluster_log_analyzer.sources.{sub}"))
        except ImportError:
            pass
    return mods


def _factory():
    for m in _modules():
        for n in FACTORY_NAMES:
            f = getattr(m, n, None)
            if callable(f):
                return f
    return None


def _registry_types():
    for m in _modules():
        for n in REGISTRY_NAMES:
            reg = getattr(m, n, None)
            if reg is None:
                continue
            if callable(reg) and not isinstance(reg, type):
                reg = reg()
            if isinstance(reg, dict):
                return set(reg)
            out = set()
            for x in reg:
                if isinstance(x, str):
                    out.add(x)
                elif isinstance(x, dict):
                    out.add(x.get("type"))
                else:
                    out.add(getattr(x, "type", None))
            return out
    return None


CLASSES = {"local": "LocalSource", "volume": "VolumeSource", "s3": "S3Source", "adls": "ADLSSource"}


def make(type_: str, root, options: dict | None = None):
    """Build a source through the registry when there is one, else through the class constructor."""
    options = options or {}
    f = _factory()
    if f is not None:
        for call in (lambda: f(type_, root, options), lambda: f(type_, root, **options),
                     lambda: f(type=type_, root=root, options=options)):
            try:
                return call()
            except TypeError:
                continue
    cls = getattr(src_pkg, CLASSES[type_])
    try:
        return cls(root, **options)
    except TypeError:
        return cls(root)


def _get(x, *names):
    for n in names:
        if isinstance(x, dict) and n in x:
            return x[n]
        if hasattr(x, n):
            return getattr(x, n)
    return None


def cluster_rows(source, limit=None):
    rows = source.list_clusters(limit) if limit is not None else source.list_clusters()
    return [(_get(r, "cluster_id"), _get(r, "last_modified", "last_log_time")) for r in rows]


def report_dict(rep) -> dict:
    if hasattr(rep, "to_dict"):
        return rep.to_dict()
    if is_dataclass(rep):
        return asdict(rep)
    return dict(rep)


def n_of(rep: dict, key: str) -> int:
    v = rep.get(key)
    if isinstance(v, (list, tuple)):
        return len(v)
    if isinstance(v, int):
        return v
    return int(rep.get(f"{key}_count", 0))


def local_tree(root: Path, cid: str) -> dict[str, Path]:
    base = root / cid
    return {p.relative_to(base).as_posix(): p for p in sorted(base.rglob("*")) if p.is_file()}


def remote_store(fixture_root: Path) -> dict[str, tuple[bytes, float]]:
    """{"<cluster_id>/<rel path>": (bytes, mtime epoch s)} for the REV3 and HEALTHY fixture clusters."""
    store = {}
    for cid in CLUSTERS:
        for rel, p in local_tree(fixture_root, cid).items():
            mt = mf.FIXED_MTIME + (NEWER if cid == mf.HEALTHY else 0)
            store[f"{cid}/{rel}"] = (p.read_bytes(), float(mt))
    return store


def assert_lists_fixture_files(source, fixture_root, cid, expected_mtime):
    files = source.list_files(cid)
    by = {f.path: f for f in files}
    tree = local_tree(fixture_root, cid)
    core = {k for k in tree if not k.startswith("init_scripts/")}
    assert core <= set(by), sorted(core - set(by))
    for rel in core:
        assert "\\" not in rel and not by[rel].path.startswith("/")
        assert by[rel].size == tree[rel].stat().st_size, rel
        assert abs(float(by[rel].modified) - expected_mtime) < 2, (rel, by[rel].modified)
    for rel in ("driver/stdout", "eventlog/{}/{}/eventlog".format(mf.HASH_R, mf.CTX_R) if cid == mf.REV3
                else "driver/log4j-active.log"):
        fh = source.open(cid, rel)
        try:
            assert fh.read() == tree[rel].read_bytes(), rel
        finally:
            try:
                fh.close()
            except Exception:
                pass


# ============================================================================================ registry
def test_registry_lists_all_source_types():
    types_ = _registry_types()
    if types_ is None:
        pytest.skip("no Python-level source registry exposed (the HTTP /api/sources test covers the list)")
    assert {"local", "volume", "adls", "s3"} <= types_


def test_registry_builds_local_source(fixture_root):
    if _factory() is None:
        pytest.skip("no Python-level source factory exposed")
    s = make("local", str(fixture_root))
    assert isinstance(s, LogSource)
    assert mf.REV3 in [c for c, _ in cluster_rows(s)]


def test_sources_package_exports():
    for name in ("LogSource", "FileInfo", "LocalSource", "VolumeSource", "S3Source", "ADLSSource"):
        assert hasattr(src_pkg, name), name


# ============================================================================================ LocalSource
def _touch(p: Path, text: str, mtime: float):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    os.utime(p, (mtime, mtime))


def test_local_list_clusters_newest_first(tmp_path):
    old, new, nodriver = "1101-000000-aaaa1111", "1102-000000-bbbb2222", "1103-000000-cccc3333"
    _touch(tmp_path / old / "driver" / "log4j-active.log", "x\n", 1_700_000_000)
    _touch(tmp_path / new / "driver" / "log4j-active.log", "x\n", 1_700_100_000)
    _touch(tmp_path / new / "driver" / "stdout", "x\n", 1_700_000_500)
    _touch(tmp_path / nodriver / "eventlog" / "x" / "eventlog", "{}\n", 1_800_000_000)
    _touch(tmp_path / "not-a-cluster" / "driver" / "stdout", "x\n", 1_900_000_000)
    _touch(tmp_path / "README.txt", "hello\n", 1_900_000_000)
    s = src_pkg.LocalSource(tmp_path)
    rows = cluster_rows(s)
    assert [c for c, _ in rows] == [new, old, nodriver]
    lm = dict(rows)
    assert lm[nodriver] is None
    # newest driver/ activity; seconds or epoch ms are both accepted here (the API normalizes to ms)
    assert lm[new] in (1_700_100_000, 1_700_100_000_000) or abs(lm[new] - 1_700_100_000) < 1
    assert [c for c, _ in cluster_rows(s, 1)] == [new]


def test_local_list_files_and_open(fixture_root):
    s = src_pkg.LocalSource(fixture_root)
    assert_lists_fixture_files(s, fixture_root, mf.REV3, mf.FIXED_MTIME)
    files = s.list_files(mf.REV3)
    assert all(isinstance(f, FileInfo) for f in files)
    assert [f.path for f in files] == sorted(f.path for f in files)


def test_local_missing_cluster_raises(tmp_path):
    s = src_pkg.LocalSource(tmp_path)
    with pytest.raises(Exception):
        s.list_files("1101-000000-missing1")


# ============================================================================================ fake SDKs
class FakeBody(io.BytesIO):
    def iter_chunks(self, chunk_size=1024):
        while True:
            b = self.read(chunk_size)
            if not b:
                return
            yield b


class FakeS3Client:
    PAGE = 7  # small pages so pagination is exercised

    def __init__(self, store: dict):
        self.objects = {f"cluster-logs/{k}": v for k, v in store.items()}
        self.objects["cluster-logs/README.txt"] = (b"not a cluster\n", float(mf.FIXED_MTIME))
        self.objects["cluster-logs/not-a-cluster/driver/stdout"] = (b"x\n", float(mf.FIXED_MTIME + 99999))
        self.calls: list[str] = []

    @staticmethod
    def _dt(epoch: float) -> datetime:
        return datetime.fromtimestamp(epoch, tz=timezone.utc)

    def list_objects_v2(self, Bucket, Prefix="", Delimiter=None, ContinuationToken=None, MaxKeys=1000,
                        StartAfter=None, **kw):
        self.calls.append("list_objects_v2")
        assert Bucket == BUCKET, Bucket
        items, seen = [], set()
        for k in sorted(self.objects):
            if not k.startswith(Prefix) or (StartAfter and k <= StartAfter):
                continue
            rest = k[len(Prefix):]
            if Delimiter and Delimiter in rest:
                p = Prefix + rest.split(Delimiter, 1)[0] + Delimiter
                if p not in seen:
                    seen.add(p)
                    items.append(("p", p))
            else:
                items.append(("k", k))
        start = int(ContinuationToken or 0)
        page = items[start:start + min(MaxKeys, self.PAGE)]
        more = start + len(page) < len(items)
        resp = {"Name": Bucket, "Prefix": Prefix, "KeyCount": len(page), "IsTruncated": more, "MaxKeys": MaxKeys}
        contents = [{"Key": k, "Size": len(self.objects[k][0]), "LastModified": self._dt(self.objects[k][1]),
                     "ETag": '"0"', "StorageClass": "STANDARD"} for t, k in page if t == "k"]
        prefixes = [{"Prefix": p} for t, p in page if t == "p"]
        if contents:
            resp["Contents"] = contents
        if prefixes:
            resp["CommonPrefixes"] = prefixes
        if Delimiter:
            resp["Delimiter"] = Delimiter
        if more:
            resp["NextContinuationToken"] = str(start + len(page))
        return resp

    def get_paginator(self, op):
        assert op == "list_objects_v2", op
        client = self

        class _P:
            def paginate(self, **kw):
                kw.pop("PaginationConfig", None)
                token = None
                while True:
                    resp = client.list_objects_v2(**kw, **({"ContinuationToken": token} if token else {}))
                    yield resp
                    if not resp.get("IsTruncated"):
                        return
                    token = resp["NextContinuationToken"]

        return _P()

    def _obj(self, Bucket, Key):
        assert Bucket == BUCKET
        if Key not in self.objects:
            raise sys.modules["botocore.exceptions"].ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": Key}}, "GetObject")
        return self.objects[Key]

    def get_object(self, Bucket, Key, **kw):
        self.calls.append("get_object")
        data, mt = self._obj(Bucket, Key)
        return {"Body": FakeBody(data), "ContentLength": len(data), "LastModified": self._dt(mt)}

    def head_object(self, Bucket, Key, **kw):
        data, mt = self._obj(Bucket, Key)
        return {"ContentLength": len(data), "LastModified": self._dt(mt)}

    def download_fileobj(self, Bucket, Key, Fileobj, **kw):
        self.calls.append("download_fileobj")
        Fileobj.write(self._obj(Bucket, Key)[0])

    def download_file(self, Bucket, Key, Filename, **kw):
        self.calls.append("download_file")
        Path(Filename).write_bytes(self._obj(Bucket, Key)[0])


def _module(name: str, **attrs) -> types.ModuleType:
    m = types.ModuleType(name)
    m.__path__ = []  # behave like a package so submodule imports resolve from sys.modules
    for k, v in attrs.items():
        setattr(m, k, v)
    return m


@pytest.fixture
def fake_boto3(monkeypatch, fixture_root):
    client = FakeS3Client(remote_store(fixture_root))
    sessions = []

    class ClientError(Exception):
        def __init__(self, error_response=None, operation_name=None):
            super().__init__(str(error_response))
            self.response = error_response or {}

    class Session:
        def __init__(self, *a, **kw):
            sessions.append(kw)

        def client(self, name, *a, **kw):
            assert name == "s3"
            return client

        def resource(self, *a, **kw):  # pragma: no cover
            raise NotImplementedError("fake boto3: use the client API")

    class Config:
        def __init__(self, *a, **kw):
            self.kw = kw

    boto3 = _module("boto3", Session=Session, client=lambda name, *a, **kw: Session().client(name),
                    __version__="0.0-fake")
    boto3.session = _module("boto3.session", Session=Session)
    exc = _module("botocore.exceptions", ClientError=ClientError, BotoCoreError=Exception,
                  NoCredentialsError=Exception, ProfileNotFound=Exception, EndpointConnectionError=Exception)
    cfg = _module("botocore.config", Config=Config)
    botocore = _module("botocore", exceptions=exc, config=cfg)
    for name, mod in (("boto3", boto3), ("boto3.session", boto3.session), ("botocore", botocore),
                      ("botocore.exceptions", exc), ("botocore.config", cfg)):
        monkeypatch.setitem(sys.modules, name, mod)
    client.sessions = sessions
    return client


class _PathProps:
    def __init__(self, name, is_directory, size, mtime):
        self.name = name
        self.is_directory = is_directory
        self.content_length = size
        self.last_modified = datetime.fromtimestamp(mtime, tz=timezone.utc)

    def __getitem__(self, k):  # PathProperties is also dict-like in the SDK
        return getattr(self, k)

    def get(self, k, default=None):
        return getattr(self, k, default)


class _Downloader:
    def __init__(self, data: bytes):
        self._data = data
        self.size = len(data)

    def readall(self):
        return self._data

    def content_as_bytes(self, *a, **kw):
        return self._data

    def readinto(self, stream):
        stream.write(self._data)
        return len(self._data)

    def chunks(self):
        yield self._data


class _FileClient:
    def __init__(self, fs, path):
        self.fs, self.path = fs, path.strip("/")

    def download_file(self, offset=None, length=None, **kw):
        self.fs.calls.append("download_file")
        if self.path not in self.fs.objects:
            raise sys.modules["azure.core.exceptions"].ResourceNotFoundError(self.path)
        return _Downloader(self.fs.objects[self.path][0])

    def get_file_properties(self, **kw):
        data, mt = self.fs.objects[self.path]
        return _PathProps(self.path, False, len(data), mt)

    def exists(self, **kw):
        return self.path in self.fs.objects


class FakeFileSystem:
    def __init__(self, objects: dict, name: str = ADLS_FS):
        self.objects = objects
        self.name = self.file_system_name = name
        self.calls: list[str] = []

    def get_paths(self, path=None, recursive=True, max_results=None, **kw):
        self.calls.append("get_paths")
        prefix = (path or "").strip("/")
        prefix = prefix + "/" if prefix else ""
        if prefix and not any(k.startswith(prefix) for k in self.objects):
            raise sys.modules["azure.core.exceptions"].ResourceNotFoundError(path)
        out, dirs = [], set()
        for k in sorted(self.objects):
            if not k.startswith(prefix):
                continue
            parts = k[len(prefix):].split("/")
            for i in range(1, len(parts)):
                d = prefix + "/".join(parts[:i])
                if (recursive or i == 1) and d not in dirs:
                    dirs.add(d)
                    out.append(_PathProps(d, True, 0, self.objects[k][1]))
            if recursive or len(parts) == 1:
                data, mt = self.objects[k]
                out.append(_PathProps(k, False, len(data), mt))
        return iter(sorted(out, key=lambda p: p.name))

    def get_file_client(self, file_path):
        return _FileClient(self, file_path)

    def get_directory_client(self, directory):
        fs, base = self, directory.strip("/")

        class _Dir:
            def get_file_client(self, name):
                return _FileClient(fs, f"{base}/{name}")

            def get_paths(self, recursive=True, **kw):
                return fs.get_paths(path=base, recursive=recursive, **kw)

            def exists(self, **kw):
                return any(k.startswith(base + "/") for k in fs.objects)

        return _Dir()

    def exists(self, **kw):
        return True


@pytest.fixture
def fake_adls(monkeypatch, fixture_root):
    objects = {f"cluster-logs/{k}": v for k, v in remote_store(fixture_root).items()}
    objects["cluster-logs/not-a-cluster/driver/stdout"] = (b"x\n", float(mf.FIXED_MTIME + 99999))
    fs = FakeFileSystem(objects)
    created = []

    class DataLakeServiceClient:
        def __init__(self, account_url=None, credential=None, **kw):
            created.append({"account_url": account_url, "credential": credential, **kw})
            self.account_url = account_url

        @classmethod
        def from_connection_string(cls, conn_str, credential=None, **kw):
            return cls("https://%s.dfs.core.windows.net" % ADLS_ACCOUNT, credential)

        def get_file_system_client(self, file_system):
            assert file_system == ADLS_FS, file_system
            return fs

        def close(self):
            pass

    class FileSystemClient(FakeFileSystem):
        def __init__(self, account_url=None, file_system_name=None, credential=None, **kw):
            created.append({"account_url": account_url, "credential": credential, **kw})
            assert file_system_name == ADLS_FS, file_system_name
            self.__dict__ = fs.__dict__

        @classmethod
        def from_connection_string(cls, conn_str, file_system_name, credential=None, **kw):
            return cls("https://%s.dfs.core.windows.net" % ADLS_ACCOUNT, file_system_name, credential)

    class DefaultAzureCredential:
        def __init__(self, *a, **kw):
            pass

        def get_token(self, *a, **kw):  # pragma: no cover
            return types.SimpleNamespace(token="fake", expires_on=4_000_000_000)

    class ResourceNotFoundError(Exception):
        pass

    dl = _module("azure.storage.filedatalake", DataLakeServiceClient=DataLakeServiceClient,
                 FileSystemClient=FileSystemClient)
    ident = _module("azure.identity", DefaultAzureCredential=DefaultAzureCredential,
                    AzureCliCredential=DefaultAzureCredential, ClientSecretCredential=DefaultAzureCredential)
    exc = _module("azure.core.exceptions", ResourceNotFoundError=ResourceNotFoundError, AzureError=Exception,
                  HttpResponseError=Exception, ClientAuthenticationError=Exception)
    cred = _module("azure.core.credentials", AzureNamedKeyCredential=lambda *a, **kw: ("key", a),
                   AzureSasCredential=lambda *a, **kw: ("sas", a))
    core = _module("azure.core", exceptions=exc, credentials=cred)
    storage = _module("azure.storage", filedatalake=dl)
    azure = _module("azure", storage=storage, identity=ident, core=core)
    for name, mod in (("azure", azure), ("azure.storage", storage), ("azure.storage.filedatalake", dl),
                      ("azure.identity", ident), ("azure.core", core), ("azure.core.exceptions", exc),
                      ("azure.core.credentials", cred)):
        monkeypatch.setitem(sys.modules, name, mod)
    fs.created = created
    return fs


class FakeFilesAPI:
    """databricks-sdk WorkspaceClient().files stand-in (list_directory_contents / download)."""

    def __init__(self, store):
        self.objects = {f"{VOLUME_ROOT}/{k}": v for k, v in store.items()}

    def list_directory_contents(self, directory_path, **kw):
        base = directory_path.rstrip("/") + "/"
        if not any(k.startswith(base) for k in self.objects):
            raise FileNotFoundError(directory_path)
        seen = set()
        for k in sorted(self.objects):
            if not k.startswith(base):
                continue
            rest = k[len(base):]
            name = rest.split("/", 1)[0]
            if "/" in rest:
                if name not in seen:
                    seen.add(name)
                    yield types.SimpleNamespace(path=base + name + "/", name=name, is_directory=True,
                                                file_size=None, last_modified=None)
            else:
                data, mt = self.objects[k]
                yield types.SimpleNamespace(path=k, name=name, is_directory=False, file_size=len(data),
                                            last_modified=int(mt * 1000))

    def download(self, file_path, **kw):
        return types.SimpleNamespace(contents=io.BytesIO(self.objects[file_path][0]))


# ============================================================================================ S3 / ADLS / volume
def test_s3_source_with_fake_boto3(fake_boto3, fixture_root):
    s = make("s3", S3_ROOT)
    rows = cluster_rows(s)
    assert [c for c, _ in rows] == [mf.HEALTHY, mf.REV3]  # newest driver/ activity first; non-cluster dirs skipped
    assert [c for c, _ in cluster_rows(s, 1)] == [mf.HEALTHY]
    assert_lists_fixture_files(s, fixture_root, mf.REV3, mf.FIXED_MTIME)
    assert_lists_fixture_files(s, fixture_root, mf.HEALTHY, mf.FIXED_MTIME + NEWER)


def test_s3_profile_option_reaches_session(fake_boto3):
    s = make("s3", S3_ROOT, {"profile": "dev"})
    cluster_rows(s)
    assert fake_boto3.calls  # the fake client was used: no network
    if fake_boto3.sessions:
        assert any(kw.get("profile_name") == "dev" for kw in fake_boto3.sessions), fake_boto3.sessions


def test_adls_source_with_fake_sdk(fake_adls, fixture_root):
    s = make("adls", ADLS_ROOT)
    rows = cluster_rows(s)
    assert [c for c, _ in rows] == [mf.HEALTHY, mf.REV3]
    assert_lists_fixture_files(s, fixture_root, mf.REV3, mf.FIXED_MTIME)
    assert fake_adls.calls
    assert any(ADLS_ACCOUNT in str(c.get("account_url")) for c in fake_adls.created), fake_adls.created


def test_volume_source_with_fake_client(fixture_root):
    api = FakeFilesAPI(remote_store(fixture_root))
    try:
        s = src_pkg.VolumeSource(VOLUME_ROOT, client=types.SimpleNamespace(files=api))
    except TypeError:
        pytest.skip("VolumeSource does not accept an injected client")
    assert [c for c, _ in cluster_rows(s)] == [mf.HEALTHY, mf.REV3]
    assert_lists_fixture_files(s, fixture_root, mf.REV3, mf.FIXED_MTIME)


def test_s3_download_into_cache_is_incremental(fake_boto3, fixture_root, tmp_path):
    s = make("s3", S3_ROOT)
    rep = report_dict(ingest.download(s, mf.REV3, tmp_path))
    tree = local_tree(fixture_root, mf.REV3)
    core = {k: p for k, p in tree.items() if not k.startswith("init_scripts/")}
    assert n_of(rep, "downloaded") == len(core)
    for rel, p in core.items():
        assert (tmp_path / mf.REV3 / rel).read_bytes() == p.read_bytes(), rel
    assert not (tmp_path / mf.REV3 / "init_scripts").exists()
    fake_boto3.calls.clear()
    rep2 = report_dict(ingest.download(s, mf.REV3, tmp_path))
    assert n_of(rep2, "downloaded") == 0 and n_of(rep2, "skipped") == len(core)
    assert not [c for c in fake_boto3.calls if c.startswith(("get_object", "download"))]


# ============================================================================================ missing extras
def test_s3_missing_extra_message(monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", None)
    with pytest.raises(Exception) as ei:
        s = make("s3", S3_ROOT)
        cluster_rows(s)
    msg = str(ei.value).lower()
    assert ("s3" in msg or "boto3" in msg) and ("install" in msg or "extra" in msg), msg


def test_adls_missing_extra_message(monkeypatch):
    monkeypatch.setitem(sys.modules, "azure.storage.filedatalake", None)
    monkeypatch.setitem(sys.modules, "azure.identity", None)
    with pytest.raises(Exception) as ei:
        s = make("adls", ADLS_ROOT)
        cluster_rows(s)
    msg = str(ei.value).lower()
    assert ("adls" in msg or "azure" in msg) and ("install" in msg or "extra" in msg), msg


# ============================================================================================ incremental ingest
class FakeRemote(LogSource):
    """A remote-like source over an in-memory dict, recording every open()."""

    def __init__(self, cluster_id: str, files: dict[str, tuple[bytes, float]]):
        self.cid = cluster_id
        self.files = dict(files)
        self.opened: list[str] = []

    def list_files(self, cluster_id):
        assert cluster_id == self.cid
        return [FileInfo(path=p, size=len(b), modified=m) for p, (b, m) in sorted(self.files.items())]

    def open(self, cluster_id, path):
        self.opened.append(path)
        return io.BytesIO(self.files[path][0])

    def list_clusters(self, limit=None):
        return [{"cluster_id": self.cid, "last_modified": None}]

    def list_cluster_ids(self):
        return [self.cid]

    def last_log_time(self, cluster_id):
        return None

    def describe(self):
        return "fake remote"


FakeRemote.__abstractmethods__ = frozenset()  # tolerate abstract methods added to LogSource later


def test_incremental_ingest_skip_logic(tmp_path):
    cid = "1101-000000-fakeremo"
    ev = f"eventlog/{cid}_10_0_0_5/1234567890/eventlog"
    files = {
        "driver/log4j-active.log": (b"26/10/09 18:00:12 INFO SparkContext: Running Spark version 3.5.0\n", 1_791_000_000.0),
        "driver/stdout": (b"2026-10-09T18:00:13Z hello\n", 1_791_000_000.0),
        "executor/app-20261009180012-0000/0/stderr": (b"26/10/09 18:00:20 INFO Executor: hi\n", 1_791_000_000.0),
        ev: (b'{"Event":"SparkListenerLogStart","Spark Version":"3.5.0"}\n', 1_791_000_000.0),
        "init_scripts/x_10_0_0_5/setup.sh.stderr.log": (b"ignored\n", 1_791_000_000.0),
    }
    src = FakeRemote(cid, files)
    dest = tmp_path / cid

    rep = report_dict(ingest.download(src, cid, tmp_path))
    assert n_of(rep, "downloaded") == 4 and n_of(rep, "failed") == 0
    assert sorted(src.opened) == sorted(k for k in files if not k.startswith("init_scripts/"))
    assert not (dest / "init_scripts").exists()
    assert (dest / ".dbx_manifest.json").is_file()
    assert (dest / "driver" / "stdout").read_bytes() == files["driver/stdout"][0]

    # 2nd run: nothing changed -> everything skipped, nothing opened
    src.opened.clear()
    rep = report_dict(ingest.download(src, cid, tmp_path))
    assert n_of(rep, "downloaded") == 0 and n_of(rep, "skipped") == 4 and src.opened == []

    # size change -> only that file again
    src.files["driver/log4j-active.log"] = (files["driver/log4j-active.log"][0] + b"26/10/09 18:00:30 INFO X: y\n",
                                             1_791_000_100.0)
    rep = report_dict(ingest.download(src, cid, tmp_path))
    assert src.opened == ["driver/log4j-active.log"] and n_of(rep, "downloaded") == 1
    assert (dest / "driver" / "log4j-active.log").read_bytes() == src.files["driver/log4j-active.log"][0]

    # same size, new mtime -> downloaded again
    src.opened.clear()
    src.files["driver/stdout"] = (b"2026-10-09T18:00:14Z hellO\n", 1_791_000_200.0)
    rep = report_dict(ingest.download(src, cid, tmp_path))
    assert src.opened == ["driver/stdout"] and n_of(rep, "downloaded") == 1
    assert (dest / "driver" / "stdout").read_bytes() == b"2026-10-09T18:00:14Z hellO\n"

    # local copy deleted -> downloaded again even though the manifest knows it
    src.opened.clear()
    (dest / "executor" / "app-20261009180012-0000" / "0" / "stderr").unlink()
    rep = report_dict(ingest.download(src, cid, tmp_path))
    assert src.opened == ["executor/app-20261009180012-0000/0/stderr"] and n_of(rep, "downloaded") == 1


def test_incremental_ingest_failed_file_is_reported(tmp_path):
    cid = "1101-000000-fakefail"
    src = FakeRemote(cid, {"driver/stdout": (b"x\n", 1_791_000_000.0), "driver/stderr": (b"y\n", 1_791_000_000.0)})
    orig = src.open

    def flaky(cluster_id, path):
        if path == "driver/stderr":
            raise OSError("connection reset")
        return orig(cluster_id, path)

    src.open = flaky
    rep = report_dict(ingest.download(src, cid, tmp_path))
    assert n_of(rep, "downloaded") == 1 and n_of(rep, "failed") == 1
    assert not (tmp_path / cid / "driver" / "stderr").exists()
    src.open = orig  # next run retries the failed file only
    rep = report_dict(ingest.download(src, cid, tmp_path))
    assert n_of(rep, "downloaded") == 1 and n_of(rep, "skipped") == 1


# ============================================================================================ HTTP with a fake remote
def test_api_s3_clusters_and_ingest(fake_boto3, tmp_path):
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    server = pytest.importorskip("databricks_cluster_log_analyzer.api.server")
    out, cache = tmp_path / "out", tmp_path / "cache"
    with TestClient(server.create_app(out, cache)) as c:
        srcs = {s["type"]: s for s in c.get("/api/sources").json()}
        assert srcs["s3"]["available"] is True
        r = c.post("/api/sources/clusters", json={"type": "s3", "root": S3_ROOT, "options": {}})
        assert r.status_code == 200, r.text
        rows = r.json()
        assert [x["cluster_id"] for x in rows] == [mf.HEALTHY, mf.REV3]
        assert rows[0]["last_modified"] == (mf.FIXED_MTIME + NEWER) * 1000
        assert all(x["analyzed"] is False for x in rows)
        r = c.post("/api/ingest", json={"type": "s3", "root": S3_ROOT, "cluster_id": mf.HEALTHY, "options": {}})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["download"] is not None
        assert body["summary"]["cluster_id"] == mf.HEALTHY and body["summary"]["status"] == "succeeded"
        assert (cache / mf.HEALTHY / "driver" / "log4j-active.log").is_file()
        assert (out / mf.HEALTHY / "summary.json").is_file()
        rows = c.post("/api/sources/clusters", json={"type": "s3", "root": S3_ROOT, "options": {}}).json()
        assert {x["cluster_id"]: x["analyzed"] for x in rows} == {mf.HEALTHY: True, mf.REV3: False}
