"""The streaming upload sink: bytes reach storage while they are produced.

Before this existed the only ways into the bucket were a finished local file or
a complete in-memory buffer, so an MTProto download had to be written to disk in
full before S3 saw any of it. These tests pin the sink that removes that copy:
multipart parts uploaded in order as they fill, an aborted upload on failure, and
a backend-agnostic fallback for everything that is not S3.
"""

import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils import storage  # noqa: E402

MiB = 1024 * 1024


class _FakeS3:
    """A recording stand-in for a boto3 S3 client's multipart calls."""

    def __init__(self, *, fail_upload_part: bool = False):
        self.created = []
        self.parts = []
        self.completed = []
        self.aborted = []
        self.fail_upload_part = fail_upload_part

    def create_multipart_upload(self, **kw):
        self.created.append(kw)
        return {"UploadId": f"upload-{len(self.created)}"}

    def upload_part(self, **kw):
        if self.fail_upload_part:
            raise RuntimeError("S3 said no")
        self.parts.append(kw)
        return {"ETag": f'"etag-{kw["PartNumber"]}"'}

    def complete_multipart_upload(self, **kw):
        self.completed.append(kw)
        return {"Key": kw["Key"]}

    def abort_multipart_upload(self, **kw):
        self.aborted.append(kw)
        return {}


def _backend(monkeypatch, s3: _FakeS3) -> storage.S3AsyncBackend:
    """An S3 backend whose client is the recorder (boto3 path, run in threads)."""
    monkeypatch.setattr(storage, "boto3", SimpleNamespace(client=lambda *a, **kw: s3))
    backend = storage.S3AsyncBackend(bucket="a-bucket", endpoint_url="https://example.invalid")
    backend._use_aioboto3 = False
    return backend


def _run(coro):
    return asyncio.run(coro)


def test_s3_backend_hands_out_a_multipart_sink(monkeypatch):
    backend = _backend(monkeypatch, _FakeS3())

    async def _check():
        sink = await backend.open_upload_sink("inputs/job-1/source")
        assert isinstance(sink, storage.S3UploadSink)
        assert sink.key == "inputs/job-1/source"

    _run(_check())


def test_chunks_are_uploaded_as_they_fill(monkeypatch):
    s3 = _FakeS3()
    backend = _backend(monkeypatch, s3)

    async def _run_stream():
        sink = await backend.open_upload_sink("inputs/job-1/source", part_size=5 * MiB)
        await sink.open()
        # 12 MiB in 1 MiB chunks -> two full 5 MiB parts and a 2 MiB tail.
        for _ in range(12):
            await sink.awrite(b"x" * MiB)
        assert sink.tell() == 12 * MiB
        return await sink.close()

    key = _run(_run_stream())

    assert key == "inputs/job-1/source"
    assert len(s3.created) == 1
    assert s3.created[0]["Key"] == "inputs/job-1/source"
    sizes = [len(p["Body"]) for p in s3.parts]
    assert sizes == [5 * MiB, 5 * MiB, 2 * MiB]
    assert [p["PartNumber"] for p in s3.parts] == [1, 2, 3]
    # Parts are completed in ascending order, which S3 requires.
    assert s3.completed[0]["MultipartUpload"]["Parts"] == [
        {"PartNumber": 1, "ETag": '"etag-1"'},
        {"PartNumber": 2, "ETag": '"etag-2"'},
        {"PartNumber": 3, "ETag": '"etag-3"'},
    ]
    assert s3.aborted == []


def test_write_returns_something_awaitable_only_when_the_caller_must_slow_down(monkeypatch):
    """Back-pressure has to be awaitable, because the producer is async."""
    backend = _backend(monkeypatch, _FakeS3())

    async def _check():
        sink = await backend.open_upload_sink("k", part_size=5 * MiB, max_inflight_bytes=1)
        await sink.open()
        first = sink.write(b"y" * MiB)
        assert first is None  # nothing in flight yet, no reason to wait
        pending = sink.write(b"y" * (5 * MiB))
        assert pending is not None, "the sink must ask the producer to wait once it is full"
        await pending
        await sink.close()

    _run(_check())


def test_a_failed_upload_is_aborted_not_left_half_written(monkeypatch):
    s3 = _FakeS3(fail_upload_part=True)
    backend = _backend(monkeypatch, s3)
    monkeypatch.setenv("S3_OP_RETRIES", "1")

    async def _check():
        sink = await backend.open_upload_sink("inputs/job-1/source", part_size=5 * MiB)
        await sink.open()
        await sink.awrite(b"z" * (6 * MiB))
        with pytest.raises(RuntimeError):
            await sink.close()

    _run(_check())

    assert s3.completed == []
    assert len(s3.aborted) == 1
    assert s3.aborted[0]["Key"] == "inputs/job-1/source"


def test_an_empty_stream_never_completes_an_empty_object(monkeypatch):
    s3 = _FakeS3()
    backend = _backend(monkeypatch, s3)

    async def _check():
        sink = await backend.open_upload_sink("inputs/job-1/source")
        await sink.open()
        with pytest.raises(RuntimeError):
            await sink.close()

    _run(_check())
    assert s3.completed == []


def test_other_backends_stage_then_upload(tmp_path):
    """The fallback sink works everywhere, so any backend can accept a stream."""
    backend = storage.LocalStorageBackend(base_path=str(tmp_path))

    async def _check():
        sink = await backend.open_upload_sink("uploads/job-1/source.mp4")
        assert isinstance(sink, storage.BufferedUploadSink)
        await sink.open()
        await sink.awrite(b"hello ")
        await sink.awrite(b"world")
        assert sink.tell() == 11
        return await sink.close()

    key = _run(_check())

    assert key == "uploads/job-1/source.mp4"
    written = os.path.join(str(tmp_path), "uploads", "job-1", "source.mp4")
    assert os.path.exists(written)
    with open(written, "rb") as fh:
        assert fh.read() == b"hello world"
    # The staging file does not survive the upload.
    leftovers = [n for n in os.listdir(os.path.dirname(written)) if n.startswith("uploadsink_")]
    assert leftovers == []


def test_the_fallback_sink_removes_its_staging_file_when_aborted(tmp_path, monkeypatch):
    backend = storage.LocalStorageBackend(base_path=str(tmp_path))

    async def _check():
        sink = await backend.open_upload_sink("uploads/job-2/source.mp4")
        await sink.open()
        await sink.awrite(b"partial")
        await sink.abort()

    _run(_check())
    assert not os.path.exists(os.path.join(str(tmp_path), "uploads", "job-2", "source.mp4"))


def test_telethon_can_write_into_a_sink():
    """The contract Telethon calls: write(chunk), tell(), flush()."""
    backend = storage.LocalStorageBackend(base_path="storage")
    sink = storage.BufferedUploadSink(backend, "uploads/x")
    assert callable(sink.write)
    assert callable(sink.tell)
    assert callable(sink.flush)
