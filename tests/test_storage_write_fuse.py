"""A bucket that refuses writes is a state, not a flake.

When the plan is full (iDrive e2 answers 403 ``AccessDenied``), or the key lost
its write permission, every source the bot fetches paid three refused PutObjects
plus backoff, then reported the download as failed - for bytes that were already
on local disk. These tests pin the fuse that turns that into one refusal and a
fallback: writers fail fast while it is open, the producers that hold the bytes
skip the bucket entirely, and local storage keeps working throughout.
"""

import asyncio

import pytest

from utils import source_store, storage

#: What iDrive e2 / S3 answer when the plan is full or the key cannot write.
ACCESS_DENIED = "An error occurred (AccessDenied) when calling the PutObject operation: Access Denied."


@pytest.fixture(autouse=True)
def _no_fuse_leaks():
    storage.reset_storage_write_fuse()
    yield
    storage.reset_storage_write_fuse()


async def _instant(*_args, **_kwargs):
    return None


class _UploadClient:
    """A boto3-shaped client whose uploads all fail the same way."""

    def __init__(self, error=None):
        self.error = error
        self.uploads: list[str] = []

    def upload_file(self, src, bucket, key):
        self.uploads.append(key)
        if self.error is not None:
            raise self.error

    def put_object(self, Bucket=None, Key=None, Body=None):  # noqa: N803 - mirrors boto3
        self.uploads.append(Key)
        if self.error is not None:
            raise self.error
        return {}


class _SinkClient(_UploadClient):
    def create_multipart_upload(self, **_kwargs):
        self.uploads.append("create_multipart_upload")
        if self.error is not None:
            raise self.error
        return {"UploadId": "u-1"}


def _s3_backend(monkeypatch, client):
    fake_boto3 = type("FakeBoto3", (), {"client": staticmethod(lambda *a, **k: client)})
    monkeypatch.setattr(storage, "boto3", fake_boto3)
    backend = storage.S3AsyncBackend(bucket="a-bucket", endpoint_url="https://example.invalid")
    backend._use_aioboto3 = False
    backend._boto_config = None
    return backend


class _CountingBackend:
    """The smallest storage backend a producer can be given."""

    def __init__(self):
        self.uploads: list[str] = []

    async def upload_file(self, src_path, dest_key):
        self.uploads.append(dest_key)
        return dest_key


# ── what counts as a refusal ────────────────────────────────────────────


def test_a_refusal_is_recognised_whatever_the_provider_calls_it():
    assert storage.storage_write_refused(RuntimeError(ACCESS_DENIED)) is True
    assert storage.storage_write_refused(RuntimeError("QuotaExceeded: storage full")) is True
    assert storage.storage_write_refused(RuntimeError("403 Forbidden")) is True


def test_a_flaky_connection_is_not_a_refusal():
    """A reset is worth retrying; a refused write can never succeed by retrying."""
    assert storage.storage_write_refused(RuntimeError("connection reset by peer")) is False
    assert storage.storage_write_refused(OSError("no route to the bucket")) is False


# ── the upload paths ────────────────────────────────────────────────────


def test_a_refused_upload_is_not_retried_and_opens_the_fuse(monkeypatch, tmp_path):
    src = tmp_path / "source.mp4"
    src.write_bytes(b"video")
    client = _UploadClient(error=RuntimeError(ACCESS_DENIED))
    backend = _s3_backend(monkeypatch, client)

    with pytest.raises(RuntimeError, match="AccessDenied"):
        asyncio.run(backend.upload_file(str(src), "inputs/library/abc/source"))

    assert client.uploads == ["inputs/library/abc/source"], "a refusal must not be retried three times"
    assert storage.storage_writes_fused() is True
    assert "AccessDenied" in storage.storage_fuse_reason()


def test_every_later_write_fails_fast_without_touching_the_bucket(monkeypatch, tmp_path):
    client = _UploadClient()
    backend = _s3_backend(monkeypatch, client)
    storage.note_storage_write_failure(RuntimeError(ACCESS_DENIED))

    src = tmp_path / "source.mp4"
    src.write_bytes(b"video")

    with pytest.raises(storage.StorageWriteUnavailableError):
        asyncio.run(backend.upload_bytes(b"bytes", "uploads/job/source"))
    with pytest.raises(storage.StorageWriteUnavailableError):
        asyncio.run(backend.upload_file(str(src), "uploads/job/other"))

    assert client.uploads == []


def test_the_streaming_sink_is_refused_before_the_upload_is_opened(monkeypatch):
    """The pipeline's ``stream`` mode has to be able to fall back to disk."""
    client = _SinkClient()
    backend = _s3_backend(monkeypatch, client)
    storage.note_storage_write_failure(RuntimeError(ACCESS_DENIED))

    with pytest.raises(storage.StorageWriteUnavailableError):
        asyncio.run(backend.open_upload_sink("inputs/job/source"))

    assert client.uploads == [], "nothing may reach the bucket while the fuse is open"


def test_a_transient_error_is_still_retried_and_opens_nothing(monkeypatch, tmp_path):
    src = tmp_path / "source.mp4"
    src.write_bytes(b"video")
    client = _UploadClient(error=OSError("no route to the bucket"))
    backend = _s3_backend(monkeypatch, client)
    monkeypatch.setattr(storage.asyncio, "sleep", _instant)

    with pytest.raises(OSError):
        asyncio.run(backend.upload_file(str(src), "inputs/job/source"))

    assert len(client.uploads) == 3, "a flake keeps its retries"
    assert storage.storage_writes_fused() is False


# ── the fuse's lifetime ─────────────────────────────────────────────────


def test_the_fuse_closes_by_itself(monkeypatch):
    monkeypatch.setenv("STORAGE_WRITE_FUSE_SECONDS", "60")
    clock = [1000.0]
    monkeypatch.setattr(storage.time, "monotonic", lambda: clock[0])

    assert storage.note_storage_write_failure(RuntimeError(ACCESS_DENIED)) is True
    assert storage.storage_writes_fused() is True

    clock[0] += 61

    assert storage.storage_writes_fused() is False, "a paused bucket must not stay paused forever"
    assert storage.storage_fuse_reason() == ""


def test_the_fuse_can_be_switched_off(monkeypatch, tmp_path):
    """``0`` means "always try": an operator who wants the old behaviour gets it."""
    monkeypatch.setenv("STORAGE_WRITE_FUSE_SECONDS", "0")
    monkeypatch.setattr(storage.asyncio, "sleep", _instant)
    src = tmp_path / "source.mp4"
    src.write_bytes(b"video")
    client = _UploadClient(error=RuntimeError(ACCESS_DENIED))
    backend = _s3_backend(monkeypatch, client)

    with pytest.raises(RuntimeError):
        asyncio.run(backend.upload_file(str(src), "inputs/job/source"))

    assert storage.storage_writes_fused() is False
    first = len(client.uploads)
    assert first > 0, "with no fuse the request still tries the bucket"

    with pytest.raises(RuntimeError):
        asyncio.run(backend.upload_file(str(src), "inputs/job/source"))
    assert len(client.uploads) > first, "and so does the one after it"


# ── what the producers do with it ───────────────────────────────────────


def test_store_source_keeps_the_bytes_local_while_the_bucket_refuses(monkeypatch, tmp_path):
    """The stopgap itself: no object, no error, and the caller keeps its file."""
    src = tmp_path / "source.mp4"
    src.write_bytes(b"video")
    backend = _CountingBackend()
    storage.note_storage_write_failure(RuntimeError(ACCESS_DENIED))

    ref = asyncio.run(source_store.store_source(backend, str(src), key="inputs/library/abc/source"))

    assert backend.uploads == [], "a fused bucket must not be asked to store anything"
    assert ref.stored is False
    assert ref.job_key is None, "the job is fed from the local copy instead"


def test_store_source_still_stores_when_the_bucket_is_healthy(tmp_path):
    src = tmp_path / "source.mp4"
    src.write_bytes(b"video")
    backend = _CountingBackend()

    ref = asyncio.run(source_store.store_source(backend, str(src), key="inputs/library/abc/source"))

    assert backend.uploads == ["inputs/library/abc/source"]
    assert ref.stored is True and ref.job_key == "inputs/library/abc/source"


def test_a_local_writer_is_never_held_back_by_the_fuse(monkeypatch, tmp_path):
    """Local storage is not the thing refusing writes, so it keeps working."""
    src = tmp_path / "source.mp4"
    src.write_bytes(b"video")
    local = storage.LocalStorageBackend(base_path=str(tmp_path / "store"))
    storage.note_storage_write_failure(RuntimeError(ACCESS_DENIED))

    asyncio.run(local.upload_file(str(src), "inputs/library/abc/source"))

    assert (tmp_path / "store" / "inputs" / "library" / "abc" / "source").read_bytes() == b"video"
