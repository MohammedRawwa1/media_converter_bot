"""One source, one store: every producer goes through the same S3 helper.

The bucket is not one thing per pipeline. A source is stored by whoever happens to
hold it - the big-file pipeline, the Bot API download behind the bot's buttons, the
fetcher service, the web uploader - and each of them used to carry its own copy of
the decision: which key, whole file or probe header, what to tell the job about it.
That is how ``PIPELINE_SOURCE_UPLOAD=header`` came to be honoured by exactly one of
them, and how the same media ended up stored three different ways.

These tests are the gate on that. The mechanics live in ``utils/source_store.py``,
no producer re-implements them, the mode is parsed in exactly one place, and the
worker still refuses to encode a probe header.
"""

import ast
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_helpers import parse_source, read_source, source_text  # noqa: E402

from utils.source_store import (  # noqa: E402
    DEFAULT_HEADER_BYTES,
    SourceRef,
    header_object_key,
    read_head_bytes,
    record_source,
    remember_fetched_source,
    source_library_key,
    source_upload_mode,
    store_source,
)

MiB = 1024 * 1024

#: Every module that hands a source to the bucket.
PRODUCERS = [
    ("handlers.py",),
    ("fetcher", "service.py"),
    ("web", "webapp.py"),
    ("utils", "bigfile_pipeline.py"),
]

PRODUCER_IDS = ["handlers", "fetcher", "webapp", "pipeline"]


# ─────────────────────────────────────────────────────────────────────────────
# The gate on repetition
# ─────────────────────────────────────────────────────────────────────────────


def _upload_calls(tree: ast.Module) -> list[tuple[int, str]]:
    """Every ``something.upload_file(...)`` / ``upload_bytes(...)`` call in a module."""
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr in ("upload_file", "upload_bytes", "upload_file_streaming"):
            found.append((node.lineno, node.func.attr))
    return found


@pytest.mark.parametrize("parts", PRODUCERS, ids=PRODUCER_IDS)
def test_a_producer_stores_a_source_only_through_the_shared_helper(parts):
    """A raw upload here is a second implementation of the store decision."""
    calls = _upload_calls(parse_source(*parts))
    assert calls == [], (
        f"{'/'.join(parts)} uploads a source directly at {calls}; it must go through "
        "utils.source_store.store_source(), or this path will not follow "
        "PIPELINE_SOURCE_UPLOAD the way every other one does"
    )


@pytest.mark.parametrize("parts", PRODUCERS, ids=PRODUCER_IDS)
def test_a_producer_does_not_re_implement_the_mode(parts):
    """The rules are read, not restated: no producer re-derives header vs whole."""
    src = read_source(*parts)
    assert 'PIPELINE_SOURCE_UPLOAD"' not in src, (
        f"{'/'.join(parts)} parses the mode itself; the environment name belongs to utils/source_store.py"
    )
    assert '"/source"' not in src, f"{'/'.join(parts)} derives the header key itself instead of using the helper"


def test_the_mode_is_read_from_the_environment_in_exactly_one_module():
    """One parser, so a typo'd value cannot mean two different things."""
    readers = set()
    for root, _dirs, names in os.walk("."):
        if any(part in root for part in ("tests", ".git", "__pycache__", ".ruff_cache")):
            continue
        for name in names:
            if not name.endswith(".py"):
                continue
            path = os.path.join(root, name)
            with open(path, encoding="utf-8") as fh:
                text = fh.read()
            if '"PIPELINE_SOURCE_UPLOAD"' in text:
                readers.add(os.path.relpath(path, ".").replace("\\", "/"))
    assert readers == {"utils/source_store.py"}


def test_the_header_mechanics_are_delegations_not_a_second_copy():
    """The pipeline keeps the names its tests read, backed by the one implementation."""
    src = read_source("utils", "bigfile_pipeline.py")
    assert "return header_object_key(" in src
    assert "return read_head_bytes(" in src
    # ...and the constants it exposes are the shared ones.
    assert "PIPELINE_SOURCE_UPLOAD = source_upload_mode()" in src
    assert "PIPELINE_HEADER_BYTES = header_bytes()" in src


def test_only_the_helper_says_a_header_key_must_never_be_the_source_key():
    src = read_source("utils", "source_store.py")
    assert 'if input_s3_key.endswith("/source")' in src
    assert "job_key=None, header_only=True" in src, "a header must never be offered as a source"


@pytest.mark.parametrize("parts", PRODUCERS, ids=PRODUCER_IDS)
def test_every_producer_uses_the_helper(parts):
    src = read_source(*parts)
    assert "store_source(" in src, f"{'/'.join(parts)} does not use the shared store helper"


@pytest.mark.parametrize("parts", PRODUCERS, ids=PRODUCER_IDS)
def test_a_producer_that_can_store_a_header_flags_the_job(parts):
    """A probe reference on a job means the media has to be reachable elsewhere."""
    src = read_source(*parts)
    if "telegram_fallback=True" not in src:
        return
    assert "input_header_only" in src
    assert "header_only" in src


# ─────────────────────────────────────────────────────────────────────────────
# The mode
# ─────────────────────────────────────────────────────────────────────────────


def test_the_default_mode_is_the_one_that_keeps_a_whole_copy_out_of_the_bucket(monkeypatch):
    monkeypatch.delenv("PIPELINE_SOURCE_UPLOAD", raising=False)
    assert source_upload_mode() == "header"


def test_a_garbage_mode_falls_back_rather_than_leaving_a_source_unstored(monkeypatch):
    monkeypatch.setenv("PIPELINE_SOURCE_UPLOAD", "yolo")
    assert source_upload_mode() == "header"


@pytest.mark.parametrize("mode", ["HEADER", " header ", "Full", "STREAM"])
def test_the_mode_is_normalised(monkeypatch, mode):
    monkeypatch.setenv("PIPELINE_SOURCE_UPLOAD", mode)
    assert source_upload_mode() == mode.strip().lower()


# ─────────────────────────────────────────────────────────────────────────────
# Header mechanics
# ─────────────────────────────────────────────────────────────────────────────


def test_a_header_object_never_lands_on_the_source_object():
    assert header_object_key("inputs/library/abc/source") == "inputs/library/abc/header"
    assert header_object_key("inputs/job-1/source") == "inputs/job-1/header"
    assert header_object_key("uploads/video.mp4") == "uploads/video.mp4/header"


def test_only_the_head_is_read(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_HEADER_BYTES", "2048")
    payload = b"x" * 5000
    path = tmp_path / "src.bin"
    path.write_bytes(payload)
    assert read_head_bytes(str(path)) == payload[:2048]
    assert read_head_bytes(str(path), len(payload) + 10) == payload


def test_the_header_size_defaults_when_the_environment_is_empty(monkeypatch):
    monkeypatch.setenv("PIPELINE_HEADER_BYTES", "")
    from utils.source_store import header_bytes

    assert header_bytes() == DEFAULT_HEADER_BYTES


# ─────────────────────────────────────────────────────────────────────────────
# store_source: one decision, every producer
# ─────────────────────────────────────────────────────────────────────────────


class _FullBackend:
    """A backend with both a whole-object upload and partial uploads."""

    def __init__(self):
        self.file_uploads = []
        self.bytes_uploads = []

    async def upload_file(self, src_path, dest_key):
        self.file_uploads.append((dest_key, os.path.getsize(src_path)))
        return dest_key

    async def upload_bytes(self, data, dest_key):
        self.bytes_uploads.append((dest_key, len(data)))
        return dest_key


class _WholeOnlyBackend:
    """No ``upload_bytes``: the local backend's shape."""

    def __init__(self):
        self.file_uploads = []

    async def upload_file(self, src_path, dest_key):
        self.file_uploads.append((dest_key, os.path.getsize(src_path)))
        return dest_key


def _run(coro):
    import asyncio

    return asyncio.run(coro)


@pytest.mark.parametrize("mode", ["full", "stream"])
def test_a_whole_object_mode_hands_the_key_to_the_job(tmp_path, mode):
    payload = b"m" * 4096
    path = tmp_path / "src.mp4"
    path.write_bytes(payload)
    backend = _FullBackend()

    ref = _run(store_source(backend, str(path), key="inputs/job/source", mode=mode))

    assert ref.stored and ref.job_key == "inputs/job/source"
    assert ref.header_only is False
    assert ref.bytes == len(payload)
    assert backend.file_uploads == [("inputs/job/source", len(payload))]
    assert backend.bytes_uploads == []


def test_header_mode_stores_a_probe_reference_when_the_media_is_still_reachable(tmp_path):
    payload = b"m" * (3 * MiB)
    path = tmp_path / "src.mp4"
    path.write_bytes(payload)
    backend = _FullBackend()

    ref = _run(
        store_source(
            backend,
            str(path),
            key="inputs/library/abc/source",
            mode="header",
            telegram_fallback=True,
            head_limit=2048,
        )
    )

    assert ref.header_only is True
    # The header is never offered as this job's source.
    assert ref.job_key is None
    assert backend.file_uploads == []
    assert backend.bytes_uploads == [("inputs/library/abc/header", 2048)]


def test_header_mode_stores_the_media_when_there_is_no_fallback(tmp_path):
    """A probe header with nothing else to read from is a lost source, not a small one."""
    payload = b"m" * 4096
    path = tmp_path / "src.mp4"
    path.write_bytes(payload)
    backend = _FullBackend()

    ref = _run(store_source(backend, str(path), key="uploads/job_source.mp4", mode="header", telegram_fallback=False))

    assert ref.header_only is False
    assert ref.job_key == "uploads/job_source.mp4"
    assert backend.file_uploads == [("uploads/job_source.mp4", len(payload))]
    assert backend.bytes_uploads == []


def test_a_backend_without_partial_uploads_still_gets_a_usable_source(tmp_path):
    payload = b"m" * 512
    path = tmp_path / "src.mp4"
    path.write_bytes(payload)
    backend = _WholeOnlyBackend()

    ref = _run(store_source(backend, str(path), key="k/source", mode="header", telegram_fallback=True))

    assert ref.header_only is False
    assert backend.file_uploads == [("k/source", len(payload))]


def test_local_mode_stores_nothing(tmp_path):
    path = tmp_path / "src.mp4"
    path.write_bytes(b"m" * 16)
    backend = _FullBackend()

    ref = _run(store_source(backend, str(path), key="inputs/job/source", mode="local", telegram_fallback=True))

    assert ref.stored is False
    assert ref.job_key is None
    assert backend.file_uploads == [] and backend.bytes_uploads == []


@pytest.mark.parametrize(
    "backend,path,key",
    [(None, "somewhere.mp4", "k"), (_FullBackend(), None, "k"), (_FullBackend(), "somewhere.mp4", None)],
    ids=["no-backend", "no-file", "no-key"],
)
def test_nothing_to_store_is_not_an_error(backend, path, key):
    ref = _run(store_source(backend, path, key=key, mode="full"))
    assert ref.stored is False


# ─────────────────────────────────────────────────────────────────────────────
# record_source: one descriptor shape
# ─────────────────────────────────────────────────────────────────────────────


def _capture_remember(monkeypatch):
    """Capture what the descriptor writer passes to media_cache.remember."""
    calls = []

    async def _remember(file_unique_id, **kwargs):
        calls.append((file_unique_id, kwargs))
        return True

    import utils.media_cache as media_cache

    monkeypatch.setattr(media_cache, "remember", _remember)
    return calls


def test_a_probe_header_goes_in_the_header_slot_and_never_in_the_source_slot(monkeypatch):
    calls = _capture_remember(monkeypatch)

    async def _check():
        return await record_source(
            "AgAD-uid",
            ref=SourceRef(mode="header", key="inputs/library/abc/header", header_only=True, bytes=2048),
            size=900,
            name="movie.mp4",
            source_meta={"duration": 12.5},
        )

    assert _run(_check()) is True
    _uid, kwargs = calls[-1]
    assert kwargs["header_key"] == "inputs/library/abc/header"
    assert kwargs["header_only"] is True
    assert not kwargs.get("input_key")
    assert kwargs["storage"] == "s3"
    assert kwargs["source_meta"] == {"duration": 12.5}


def test_a_whole_object_goes_in_the_source_slot(monkeypatch):
    calls = _capture_remember(monkeypatch)

    async def _check():
        return await record_source(
            "AgAD-uid",
            ref=SourceRef(mode="full", key="inputs/library/abc/source", job_key="inputs/library/abc/source"),
            size=900,
            name="movie.mp4",
        )

    _run(_check())
    _uid, kwargs = calls[-1]
    assert kwargs["input_key"] == "inputs/library/abc/source"
    assert kwargs.get("header_only") is not True
    assert not kwargs.get("header_key")


def test_nothing_stored_is_not_remembered_as_a_source(monkeypatch):
    calls = _capture_remember(monkeypatch)

    async def _check():
        return await record_source("AgAD-uid", ref=SourceRef(mode="local"), size=900)

    _run(_check())
    _uid, kwargs = calls[-1]
    # A local-mode run has no object; the descriptor must not claim one.
    assert not kwargs.get("input_key")
    assert not kwargs.get("header_key")


def test_a_media_without_an_identity_is_not_remembered(monkeypatch):
    calls = _capture_remember(monkeypatch)

    async def _check():
        return await record_source(None, ref=SourceRef(mode="full", key="k"), size=1)

    assert _run(_check()) is False
    assert calls == []


def test_the_disk_copy_is_kept_beside_the_key(monkeypatch):
    """``path`` is the reuse tier a stored key cannot replace: the local file."""
    calls = _capture_remember(monkeypatch)

    async def _check():
        return await record_source(
            "AgAD-uid",
            ref=SourceRef(mode="full", key="inputs/library/abc/source", job_key="inputs/library/abc/source"),
            size=900,
            local_path="/storage/temp/src_7_abc.mp4",
        )

    _run(_check())
    _uid, kwargs = calls[-1]
    assert kwargs["input_key"] == "inputs/library/abc/source"
    assert kwargs["path"] == "/storage/temp/src_7_abc.mp4"
    assert kwargs["storage"] == "s3"


# ──────────────────────────────────────────────────────────────────────
# remember_fetched_source: a fetch leaves a record, not only a file
# ──────────────────────────────────────────────────────────────────────


def _fetch_plumbing(monkeypatch, *, mode="header", store_name="s3", backend=None, previous=None):
    """What a fetch needs: a backend, a mode, and a captured descriptor."""
    calls = _capture_remember(monkeypatch)
    backed = backend if backend is not None else _FullBackend()

    async def _backend():
        return backed

    import config as config_module
    import utils.media_cache as media_cache
    import utils.storage as storage

    monkeypatch.setattr(config_module, "get_storage_backend_name", lambda: store_name)
    monkeypatch.setattr(storage, "get_storage_backend", _backend)
    monkeypatch.setenv("PIPELINE_SOURCE_UPLOAD", mode)

    async def _lookup(*_args, **_kwargs):
        return previous

    monkeypatch.setattr(media_cache, "lookup", _lookup)
    return calls, backed


def test_a_fetched_media_is_stored_under_its_own_identity(tmp_path, monkeypatch):
    """One media is one object: the key every producer derives for it."""
    payload = b"m" * 900
    path = tmp_path / "movie.mp4"
    path.write_bytes(payload)
    calls, backend = _fetch_plumbing(monkeypatch, mode="full")

    key = _run(
        remember_fetched_source(
            {"file_unique_id": "AgAD-uid", "name": "movie.mp4", "id": "file-id-1"},
            str(path),
        )
    )

    assert key == source_library_key("AgAD-uid")
    assert backend.file_uploads == [(key, len(payload))]
    _uid, kwargs = calls[-1]
    assert _uid == "AgAD-uid"
    assert kwargs["input_key"] == key
    # The disk copy travels with it, as the second tier a repeat can read.
    assert kwargs["path"] == str(path)
    assert kwargs["storage"] == "s3"
    assert kwargs["size"] == len(payload)
    assert kwargs["data"] == payload


def test_a_header_mode_fetch_keeps_a_probe_reference(tmp_path, monkeypatch):
    """A media still reachable in Telegram does not need a whole second copy."""
    path = tmp_path / "movie.mp4"
    path.write_bytes(b"m" * 900)
    calls, backend = _fetch_plumbing(monkeypatch, mode="header")

    key = _run(
        remember_fetched_source(
            {"file_unique_id": "AgAD-uid", "name": "movie.mp4"},
            str(path),
            telegram_fallback=True,
        )
    )

    # A header is a reference, never a job's source.
    assert key is None
    _uid, kwargs = calls[-1]
    assert kwargs["header_only"] is True
    assert kwargs["header_key"] == header_object_key(source_library_key("AgAD-uid"))
    assert not kwargs.get("input_key")
    assert backend.file_uploads == []


def test_a_fetch_with_nothing_to_store_still_records_the_disk_copy(tmp_path, monkeypatch):
    """``local`` mode: no object, but the fetch is not forgotten either."""
    path = tmp_path / "movie.mp4"
    path.write_bytes(b"m" * 900)
    calls, backend = _fetch_plumbing(monkeypatch, mode="local")

    key = _run(remember_fetched_source({"file_unique_id": "AgAD-uid", "name": "movie.mp4"}, str(path)))

    assert key is None
    assert backend.file_uploads == [] and backend.bytes_uploads == []
    _uid, kwargs = calls[-1]
    assert kwargs.get("input_key") is None and not kwargs.get("header_key")
    assert kwargs["path"] == str(path)
    assert kwargs["storage"] == "local"


def test_a_deployment_with_no_object_store_records_the_fetch_without_asking_for_one(tmp_path, monkeypatch):
    """A local backend *is* this disk: nothing is uploaded, the copy is recorded."""
    path = tmp_path / "movie.mp4"
    path.write_bytes(b"m" * 900)
    calls, _backend = _fetch_plumbing(monkeypatch, mode="full", store_name="local")

    key = _run(remember_fetched_source({"file_unique_id": "AgAD-uid", "name": "movie.mp4"}, str(path)))

    assert key is None
    _uid, kwargs = calls[-1]
    assert kwargs["path"] == str(path)
    assert kwargs["storage"] == "local"
    assert not kwargs.get("input_key")


def test_a_fetch_that_stored_nothing_keeps_the_key_an_earlier_producer_wrote(tmp_path, monkeypatch):
    """Blank the descriptor and the object a previous run stored becomes invisible."""
    path = tmp_path / "movie.mp4"
    path.write_bytes(b"m" * 900)
    calls, _backend = _fetch_plumbing(
        monkeypatch,
        mode="local",
        previous={"input_key": "inputs/library/old/source", "size": 900},
    )

    key = _run(remember_fetched_source({"file_unique_id": "AgAD-uid", "name": "movie.mp4"}, str(path)))

    assert key == "inputs/library/old/source"
    _uid, kwargs = calls[-1]
    assert kwargs["input_key"] == "inputs/library/old/source"
    assert kwargs["storage"] == "s3"


def test_a_fetch_that_saved_nothing_new_says_so(tmp_path, monkeypatch):
    """An unreadable path is not a fetch: no descriptor, no key."""
    calls, _backend = _fetch_plumbing(monkeypatch, mode="full")

    key = _run(remember_fetched_source({"file_unique_id": "AgAD-uid"}, str(tmp_path / "gone.mp4")))

    assert key is None
    assert calls == []


# ─────────────────────────────────────────────────────────────────────────────
# The worker: one read-back, one URL fetch
# ─────────────────────────────────────────────────────────────────────────────


def test_the_worker_fetches_a_url_source_one_way():
    """The initial acquisition and the mid-run re-download share one fetch."""
    src = read_source("workers", "ffmpeg_worker.py")
    assert src.count("async def _fetch_source_url_to_path(") == 1
    assert src.count("_fetch_source_url_to_path(") >= 3
    # The no-redirect rule and the SSRF re-check are stated once, not twice.
    assert src.count("allow_redirects=False") == 1
    assert src.count("_validate_url_safe(") == 1
    assert "iter_chunked" in src


def test_the_worker_never_encodes_a_probe_header():
    """The read-back side of the contract the producers write."""
    src = read_source("workers", "ffmpeg_worker.py")
    assert "input_header_only" in src
    # The object it names is refused, and the worker says so: a header is not a
    # source, so the bytes have to come from a whole stored copy or Telegram.
    assert "is only the probe header, not the media" in src
    assert "input_key = None" in src
    # And it is refused *before* anything reads that object - including the
    # bucket-first check, which would otherwise call the header readable.
    assert src.index("input_header_only") < src.index("_stored_source_available(input_key)")


def test_the_producer_and_the_worker_agree_on_the_flag():
    """Whoever sets ``input_header_only`` reports what was actually stored."""
    flagged = 0
    for parts in PRODUCERS:
        text = source_text(*parts)
        if "input_header_only" not in text:
            continue
        flagged += 1
        # Whatever the ref is called locally, the flag has to come from it.
        assert re.search(r"\w*ref\w*\.header_only", text), (
            f"{'/'.join(parts)} must mark the job from what the helper stored, not from the configured mode"
        )
    assert flagged >= 3, "the producers that can hand over a header have to say so on the job"


# ─────────────────────────────────────────────────────────────────────────────
# The docs
# ─────────────────────────────────────────────────────────────────────────────


def test_the_readme_says_the_mode_applies_to_every_producer():
    readme = source_text("README.md")
    assert "PIPELINE_SOURCE_UPLOAD" in readme
    assert "utils/source_store.py" in readme or "source_store" in readme
