"""The batch apply's "Fetching source from storage" line must show real movement.

A source that lives in the bucket is the first thing a job does, and for a large
video that used to be minutes of a frozen 0%: the worker's managed transfer has
no hook between "started" and "done", so the batch message could only ever say
``Fetching source from storage``. These tests cover both halves of the fix - a
streaming download that reports the bytes as they arrive, and the throttled
callback that writes those bytes into the job hash the batch message polls.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_helpers import read_object_source  # noqa: E402

from utils import storage  # noqa: E402
from workers import ffmpeg_worker  # noqa: E402

PAYLOAD = b"a" * (200 * 1024)


# ─────────────────────────────────────────────────────────────────────────────
# The streaming download itself
# ─────────────────────────────────────────────────────────────────────────────


class _SyncBody:
    def __init__(self, payload):
        self._payload = payload
        self._pos = 0

    def read(self, n=-1):
        if n is None or n < 0:
            chunk = self._payload[self._pos :]
            self._pos = len(self._payload)
            return chunk
        chunk = self._payload[self._pos : self._pos + n]
        self._pos += len(chunk)
        return chunk


class _AsyncBody:
    def __init__(self, payload):
        self._inner = _SyncBody(payload)

    async def read(self, n=-1):
        return self._inner.read(n)


def _range_bounds(range_header, length: int):
    """``(start, end)`` inclusive for a Range header, or ``None`` when absent."""
    if not range_header:
        return None
    spec = str(range_header).split("=", 1)[-1]
    first, _, last = spec.partition("-")
    start = int(first or 0)
    end = int(last) if last else (length - 1)
    return start, end


def _object_response(payload, range_header, *, honor_range, body_cls):
    """A ``get_object`` response, ranged or whole depending on *honor_range*."""
    bounds = _range_bounds(range_header, len(payload)) if honor_range else None
    start, end = bounds if bounds else (0, len(payload) - 1)
    body = payload[start : end + 1]
    resp = {"Body": body_cls(body), "ContentLength": len(body)}
    if range_header and bounds is not None:
        resp["ContentRange"] = f"bytes {start}-{end}/{len(payload)}"
        resp["ResponseMetadata"] = {"HTTPStatusCode": 206}
    elif range_header and not honor_range:
        # A backend that ignores Range answers 200 with the whole object.
        resp["ResponseMetadata"] = {"HTTPStatusCode": 200}
    return resp


class _SyncClient:
    """A boto3-shaped client that serves *payload*, honouring Range if asked."""

    def __init__(self, payload, *, honor_range=True):
        self._payload = payload
        self._honor_range = honor_range
        self.ranges = []

    def head_object(self, Bucket=None, Key=None):
        return {"ContentLength": len(self._payload), "ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_object(self, Bucket=None, Key=None, Range=None):
        self.ranges.append(Range)
        return _object_response(self._payload, Range, honor_range=self._honor_range, body_cls=_SyncBody)


class _AsyncClient:
    def __init__(self, payload, *, honor_range=True):
        self._payload = payload
        self._honor_range = honor_range
        self.ranges = []

    async def head_object(self, Bucket=None, Key=None):
        return {"ContentLength": len(self._payload), "ResponseMetadata": {"HTTPStatusCode": 200}}

    async def get_object(self, Bucket=None, Key=None, Range=None):
        self.ranges.append(Range)
        return _object_response(self._payload, Range, honor_range=self._honor_range, body_cls=_AsyncBody)


class _ClientCtx:
    def __init__(self, client):
        self._client = client

    async def __aenter__(self):
        return self._client

    async def __aexit__(self, *_exc):
        return False


class _FakeSession:
    def __init__(self, client):
        self._client = client

    def client(self, *_args, **_kwargs):
        return _ClientCtx(self._client)


def _s3_backend(monkeypatch, payload=PAYLOAD, *, aioboto3=False, honor_range=True):
    if aioboto3:
        client = _AsyncClient(payload, honor_range=honor_range)
        backend = storage.S3AsyncBackend(bucket="a-bucket", endpoint_url="https://example.invalid")
        backend._use_aioboto3 = True
        backend._session = _FakeSession(client)
    else:
        client = _SyncClient(payload, honor_range=honor_range)
        fake_boto3 = type("FakeBoto3", (), {"client": staticmethod(lambda *a, **k: client)})
        monkeypatch.setattr(storage, "boto3", fake_boto3)
        backend = storage.S3AsyncBackend(bucket="a-bucket", endpoint_url="https://example.invalid")
        backend._use_aioboto3 = False
        backend._boto_config = None
    backend._test_client = client
    return backend


def test_the_s3_backend_advertises_streaming_and_reports_it(monkeypatch, tmp_path):
    """The flag is what the worker keys off, so it has to be true for S3."""
    backend = _s3_backend(monkeypatch)
    assert backend.supports_download_progress is True


def test_a_streaming_download_reports_the_bytes_as_they_arrive(monkeypatch, tmp_path):
    monkeypatch.setenv("S3_DOWNLOAD_CHUNK_KB", "64")
    backend = _s3_backend(monkeypatch)
    dest = tmp_path / "source.bin"
    seen = []

    ok = asyncio.run(
        backend.download_file_with_progress("inputs/a.mp4", str(dest), lambda done, total: seen.append((done, total)))
    )

    assert ok is True
    assert dest.read_bytes() == PAYLOAD
    # The point of the streaming read: progress exists *during* the transfer, not
    # only as the single "done" the managed downloader used to expose.
    assert seen[0][0] < len(PAYLOAD)
    assert seen[-1] == (len(PAYLOAD), len(PAYLOAD))
    assert [done for done, _ in seen] == sorted(done for done, _ in seen)
    assert all(total == len(PAYLOAD) for _, total in seen)


def test_the_aioboto3_path_streams_with_the_same_progress(monkeypatch, tmp_path):
    monkeypatch.setenv("S3_DOWNLOAD_CHUNK_KB", "64")
    backend = _s3_backend(monkeypatch, aioboto3=True)
    dest = tmp_path / "source.bin"
    seen = []

    ok = asyncio.run(
        backend.download_file_with_progress("inputs/a.mp4", str(dest), lambda done, total: seen.append((done, total)))
    )

    assert ok is True
    assert dest.read_bytes() == PAYLOAD
    assert seen[0][0] < len(PAYLOAD)
    assert seen[-1] == (len(PAYLOAD), len(PAYLOAD))


def test_a_backend_that_cannot_stream_does_not_claim_to(tmp_path):
    """Local paths copy in one go; the worker must fall back rather than ask for progress."""
    store = tmp_path / "store" / "inputs"
    store.mkdir(parents=True)
    (store / "a.mp4").write_bytes(b"x" * 32)
    backend = storage.LocalStorageBackend(str(tmp_path / "store"))
    dest = tmp_path / "out.bin"
    seen = []

    assert backend.supports_download_progress is False
    ok = asyncio.run(backend.download_file_with_progress("inputs/a.mp4", str(dest), lambda d, t: seen.append((d, t))))

    assert ok is True
    # One report, at the end - honest about what it knows.
    assert seen == [(32, 32)]


# ─────────────────────────────────────────────────────────────────────────────
# Resuming an interrupted transfer
# ─────────────────────────────────────────────────────────────────────────────


def test_resume_is_advertised_by_the_streaming_backend_only(monkeypatch, tmp_path):
    assert _s3_backend(monkeypatch).supports_resume_download is True
    store = tmp_path / "store"
    store.mkdir()
    assert storage.LocalStorageBackend(str(store)).supports_resume_download is False


def test_an_interrupted_download_resumes_from_the_bytes_already_on_disk(monkeypatch, tmp_path):
    monkeypatch.setenv("S3_DOWNLOAD_CHUNK_KB", "64")
    backend = _s3_backend(monkeypatch)
    dest = tmp_path / "source.bin"
    prefix = PAYLOAD[: 96 * 1024]
    dest.write_bytes(prefix)
    seen = []

    ok = asyncio.run(
        backend.download_file_with_progress(
            "inputs/a.mp4", str(dest), lambda done, total: seen.append((done, total)), resume_from=len(prefix)
        )
    )

    assert ok is True
    # The prefix was kept and the remainder appended - no duplicated bytes.
    assert dest.read_bytes() == PAYLOAD
    # Only the remainder was requested, not the whole object again.
    assert backend._test_client.ranges[0] == f"bytes={len(prefix)}-"
    # Progress counts from the start of the object, not from the resume point.
    assert seen[0][0] == len(prefix) + 64 * 1024
    assert seen[-1] == (len(PAYLOAD), len(PAYLOAD))


def test_the_aioboto3_path_resumes_too(monkeypatch, tmp_path):
    monkeypatch.setenv("S3_DOWNLOAD_CHUNK_KB", "64")
    backend = _s3_backend(monkeypatch, aioboto3=True)
    dest = tmp_path / "source.bin"
    prefix = PAYLOAD[: 96 * 1024]
    dest.write_bytes(prefix)

    ok = asyncio.run(backend.download_file_with_progress("inputs/a.mp4", str(dest), None, resume_from=len(prefix)))

    assert ok is True
    assert dest.read_bytes() == PAYLOAD
    assert backend._test_client.ranges[0] == f"bytes={len(prefix)}-"


def test_a_bucket_that_ignores_the_range_does_not_duplicate_the_prefix(monkeypatch, tmp_path):
    """A 200 where a 206 was asked for is a whole object: replace, don't append."""
    monkeypatch.setenv("S3_DOWNLOAD_CHUNK_KB", "64")
    backend = _s3_backend(monkeypatch, honor_range=False)
    dest = tmp_path / "source.bin"
    prefix = PAYLOAD[: 96 * 1024]
    dest.write_bytes(prefix)

    ok = asyncio.run(backend.download_file_with_progress("inputs/a.mp4", str(dest), None, resume_from=len(prefix)))

    assert ok is True
    assert dest.read_bytes() == PAYLOAD


def test_iter_range_streams_just_the_requested_window(monkeypatch):
    monkeypatch.setenv("S3_DOWNLOAD_CHUNK_KB", "64")
    backend = _s3_backend(monkeypatch)
    chunks = []

    async def _collect():
        async for chunk in backend.iter_range("inputs/a.mp4", start=100, end=299):
            chunks.append(chunk)

    asyncio.run(_collect())

    assert b"".join(chunks) == PAYLOAD[100:300]
    assert backend.supports_range_streaming is True
    assert backend._test_client.ranges[0] == "bytes=100-299"


def test_iter_range_uses_the_aioboto3_path_too(monkeypatch):
    monkeypatch.setenv("S3_DOWNLOAD_CHUNK_KB", "64")
    backend = _s3_backend(monkeypatch, aioboto3=True)
    chunks = []

    async def _collect():
        async for chunk in backend.iter_range("inputs/a.mp4", start=8, end=999):
            chunks.append(chunk)

    asyncio.run(_collect())

    assert b"".join(chunks) == PAYLOAD[8:1000]
    assert backend._test_client.ranges[0] == "bytes=8-999"


def test_a_backend_that_cannot_stream_a_range_says_so(tmp_path):
    store = tmp_path / "store"
    store.mkdir()
    assert storage.LocalStorageBackend(str(store)).supports_range_streaming is False


def test_a_partial_that_is_already_the_whole_object_starts_over(monkeypatch, tmp_path):
    """A Range from the end would be a 416; a HEAD shows it and we re-pull clean."""
    monkeypatch.setenv("S3_DOWNLOAD_CHUNK_KB", "64")
    backend = _s3_backend(monkeypatch)
    dest = tmp_path / "source.bin"
    dest.write_bytes(PAYLOAD)

    ok = asyncio.run(backend.download_file_with_progress("inputs/a.mp4", str(dest), None, resume_from=len(PAYLOAD)))

    assert ok is True
    assert dest.read_bytes() == PAYLOAD
    # No Range at all: the size check turned the stale partial into a fresh GET.
    assert backend._test_client.ranges[0] is None


# ─────────────────────────────────────────────────────────────────────────────
# The callback that writes the percentage where the batch message reads it
# ─────────────────────────────────────────────────────────────────────────────


def test_the_fetch_callback_writes_the_percentage_into_the_job_hash(monkeypatch):
    writes = []

    async def _fake_set(job_id, status, message, *, progress=None, channel=None):
        writes.append((job_id, status, message, progress, channel))

    monkeypatch.setattr(ffmpeg_worker, "_set_job_state", _fake_set)
    clock = [1000.0]
    monkeypatch.setattr(ffmpeg_worker.time, "time", lambda: clock[0])

    async def _drive():
        cb = ffmpeg_worker._make_fetch_progress_callback("job-1", "chan-1", "fetching source from storage (12 MB)")
        cb(0, 100)
        cb(50, 100)  # inside the pacing window: coalesced away
        clock[0] += 2
        cb(50, 100)
        clock[0] += 2
        cb(100, 100)  # a finished fetch must not paint 100% before the encode resets it
        await asyncio.sleep(0.05)
        cb(10, 0)  # an unknown total is not a percentage

    asyncio.run(_drive())

    assert writes[0][:2] == ("job-1", "processing")
    assert writes[0][2] == "fetching source from storage (12 MB)"
    assert writes[0][4] == "chan-1"
    assert [w[3] for w in writes] == [0, 50, 99]


# ─────────────────────────────────────────────────────────────────────────────
# The two ends of the rope
# ─────────────────────────────────────────────────────────────────────────────


def test_the_worker_prefers_the_streaming_download_when_available():
    src = read_object_source(ffmpeg_worker.handle_job)
    assert (
        "backend.download_file_with_progress(input_key, _part_path, _attempt_progress, resume_from=_resume_from)" in src
    )
    # The managed transfer stays as the fallback, and the atomic swap is untouched.
    assert "backend.download_file(input_key, _part_path)" in src
    assert "os.replace(_part_path, temp_input_path)" in src
    # The capability flag - not a bare attribute probe - decides which path runs,
    # so a test double with auto-created attributes is never mistaken for a stream.
    assert 'getattr(backend, "supports_download_progress", False) is True' in src


def test_the_worker_resumes_and_keeps_a_resumable_partial():
    src = read_object_source(ffmpeg_worker.handle_job)
    assert 'getattr(backend, "supports_resume_download", False) is True' in src
    assert "resume_from=_resume_from" in src
    assert "_resume_from = os.path.getsize(_part_path)" in src
    # A backend that cannot resume still gets a clean slate each attempt.
    assert "if not download_success and not _resumes_fetch:" in src


def test_the_worker_names_the_resume_point_for_the_batch_line():
    src = read_object_source(ffmpeg_worker.handle_job)
    assert 'f"fetching source from storage (resuming from {_resume_pct}%)"' in src
    assert "_attempt_progress = _make_fetch_progress_callback(job_id, progress_channel, _attempt_note)" in src
    # The wording is written before the transfer so the line announces the resume
    # even if the first chunk is slow to arrive.
    assert 'await _set_job_state(job_id, "processing", _attempt_note, progress=0, channel=progress_channel)' in src


def test_the_batch_line_renders_the_percentage_once_it_moves():
    import handlers

    emoji, stage = handlers._batch_member_stage(
        {"status": "processing", "message": "fetching source from storage (12 MB)", "progress": "42"}
    )

    assert emoji == "⬇️"
    assert stage == "Fetching source from storage — 42%"


def test_a_resumed_fetch_line_names_where_it_picked_up():
    import handlers

    emoji, stage = handlers._batch_member_stage(
        {"status": "processing", "message": "fetching source from storage (resuming from 62%)", "progress": "0"}
    )

    assert emoji == "⬇️"
    assert stage == "Fetching source from storage — resuming from 62%"


def test_a_resumed_fetch_keeps_the_live_percentage_and_the_origin():
    import handlers

    _emoji, stage = handlers._batch_member_stage(
        {"status": "processing", "message": "fetching source from storage (resuming from 62%)", "progress": "80"}
    )

    assert stage == "Fetching source from storage — 80% (resumed from 62%)"


def test_the_userbot_fallback_renders_the_batch_shape_and_honours_stop():
    """The Pyrogram fallback is another fetch onto the apply's one message.

    It used to replace the batch's id-and-Stop line with a bare
    ``Downloading: N%`` one and watch only its own Cancel button, so a batch
    Stop left the transfer running and the message out of shape.
    """
    import handlers

    src = read_object_source(handlers.EnhancedMediaHandler._ensure_current_file_downloaded)
    assert "_batch_member_text(" in src
    assert "_batch_stop_markup(_dl_batch_id)" in src
    assert "_watch_userbot_batch_cancel" in src
    # Stop flags the batch in Redis, so the download callback checks that too.
    assert "_dl_batch_cancelled[0]" in src
