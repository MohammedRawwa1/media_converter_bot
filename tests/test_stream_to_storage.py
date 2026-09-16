"""Storage as the source of truth: the bytes reach the bucket while they download.

Three behaviours are pinned here.

* A multipart sink opens **one** upload per sink. Parts upload concurrently, and
  each one has to ask for the upload id - so the ask is single-flight, or a
  single sink scatters its parts across several uploads and only the last can
  ever be completed.
* Streaming a source into storage keeps only the container header in memory, so
  the job still carries real ``source_*`` metadata without a second read.
* The MTProto download writes into that sink itself: Telethon hands each chunk
  to the sink (and awaits the back-pressure it returns), which is the only way
  the media is ever in flight rather than staged on disk.
"""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_helpers import defined_functions, find_function, parse_source, read_source  # noqa: E402

from utils import bigfile_pipeline, media_cache, storage, userbot_downloader  # noqa: E402
from utils.bigfile_pipeline import BigFilePipeline  # noqa: E402

MiB = 1024 * 1024
WORKER = ("workers", "ffmpeg_worker.py")


def _run(coro):
    return asyncio.run(coro)


# ─────────────────────────────────────────────────────────────────────────────
# The head-capture tap
# ─────────────────────────────────────────────────────────────────────────────


class _Recorder(storage.UploadSink):
    """A sink that records what it was handed, and nothing else."""

    def __init__(self, key="inputs/job/source"):
        self.key = key
        self.chunks = []
        self.opened = 0
        self.closed = 0
        self.aborted = 0
        self.awaitable = None
        self._written = 0

    async def open(self):
        self.opened += 1

    def write(self, chunk):
        self.chunks.append(chunk)
        self._written += len(chunk)
        return self.awaitable

    def tell(self):
        return self._written

    async def close(self):
        self.closed += 1
        return self.key

    async def abort(self):
        self.aborted += 1


def test_head_capture_keeps_the_head_and_forwards_every_byte():
    inner = _Recorder()
    sink = storage.HeadCaptureSink(inner, 10)

    assert sink.write(b"abcdefgh") is None
    assert sink.write(b"ijklmnop") is None

    # The header is exactly the first `limit` bytes, in order...
    assert sink.head == b"abcdefghij"
    # ...and the wrapped sink still saw every byte, in order, untouched.
    assert inner.chunks == [b"abcdefgh", b"ijklmnop"]
    assert sink.tell() == 16
    assert sink.key == inner.key


def test_head_capture_never_grows_past_the_limit():
    inner = _Recorder()
    sink = storage.HeadCaptureSink(inner, 4)
    sink.write(b"ab")
    sink.write(b"cd")
    sink.write(b"ef")
    assert sink.head == b"abcd"


def test_head_capture_passes_back_pressure_through():
    """The producer throttles on the wrapped sink's answer, not the tap's."""
    inner = _Recorder()
    sentinel = object()
    inner.awaitable = sentinel
    sink = storage.HeadCaptureSink(inner, 4)
    assert sink.write(b"data") is sentinel


def test_head_capture_delegates_the_lifecycle():
    inner = _Recorder()
    sink = storage.HeadCaptureSink(inner, 4)

    async def _check():
        await sink.open()
        sink.write(b"xyz")
        await sink.close()

    _run(_check())
    assert inner.opened == 1
    assert inner.closed == 1
    assert inner.aborted == 0
    assert sink.wrapped is inner


def test_head_capture_aborts_the_wrapped_sink():
    inner = _Recorder()
    sink = storage.HeadCaptureSink(inner, 4)

    async def _check():
        await sink.open()
        await sink.abort()

    _run(_check())
    assert inner.aborted == 1
    assert inner.closed == 0


def test_a_local_backend_hands_out_a_working_sink(tmp_path):
    """Every backend can accept a stream, even without multipart support."""
    backend = storage.LocalStorageBackend(base_path=str(tmp_path))

    async def _check():
        sink = await backend.open_upload_sink("inputs/job-1/source.mp4")
        tapped = storage.HeadCaptureSink(sink, 5)
        await tapped.open()
        await tapped.awrite(b"hello ")
        await tapped.awrite(b"world")
        await tapped.close()
        return tapped.head

    assert _run(_check()) == b"hello"
    written = os.path.join(str(tmp_path), "inputs", "job-1", "source.mp4")
    with open(written, "rb") as fh:
        assert fh.read() == b"hello world"


# ─────────────────────────────────────────────────────────────────────────────
# The multipart sink: one upload per sink
# ─────────────────────────────────────────────────────────────────────────────


class _FakeS3:
    """A recording stand-in whose every multipart call yields to the loop.

    The yield is the point: it is what put two parts inside the upload-id race at
    the same time, which is how a sink used to end up with one multipart upload
    per part instead of one per sink.
    """

    def __init__(self):
        self.created = []
        self.parts = []
        self.completed = []

    async def create_multipart_upload(self, **kw):
        await asyncio.sleep(0)
        self.created.append(kw)
        return {"UploadId": f"upload-{len(self.created)}"}

    async def upload_part(self, **kw):
        await asyncio.sleep(0)
        self.parts.append(kw)
        return {"ETag": f'"etag-{kw["PartNumber"]}"'}

    async def complete_multipart_upload(self, **kw):
        self.completed.append(kw)
        return {"Key": kw["Key"]}

    async def abort_multipart_upload(self, **kw):
        return {}


def _s3_backend(fake):
    """An S3 backend whose client is the fake (aioboto3 path, so no threads)."""

    class _Ctx:
        async def __aenter__(self):
            return fake

        async def __aexit__(self, *exc):
            return False

    backend = storage.S3AsyncBackend(bucket="a-bucket", endpoint_url="https://example.invalid")
    backend._use_aioboto3 = True
    backend._session = type("S", (), {"client": lambda self, *a, **k: _Ctx()})()
    return backend


def test_concurrent_parts_share_one_multipart_upload():
    """Every part asks for the upload id; only the first may create one."""
    fake = _FakeS3()
    backend = _s3_backend(fake)

    async def _check():
        # Four parts' worth in one write, so every part task races for the id.
        sink = await backend.open_upload_sink("inputs/job-1/source", part_size=5 * MiB)
        await sink.open()
        await sink.awrite(b"z" * (4 * 5 * MiB))
        await sink.close()

    _run(_check())

    assert len(fake.created) == 1, "a sink must open exactly one multipart upload"
    assert len(fake.parts) == 4
    assert sorted(p["PartNumber"] for p in fake.parts) == [1, 2, 3, 4]
    # Every part must belong to the upload that was actually completed.
    assert {p["UploadId"] for p in fake.parts} == {"upload-1"}
    assert fake.completed[0]["UploadId"] == "upload-1"
    assert len(fake.completed[0]["MultipartUpload"]["Parts"]) == 4


# ─────────────────────────────────────────────────────────────────────────────
# The pipeline in `stream` mode
# ─────────────────────────────────────────────────────────────────────────────

SOURCE_BYTES = b"A" * 4096
CHUNK = 1024


class _StreamStorage:
    """Storage whose sink records the bytes the download pushed into it."""

    def __init__(self):
        self.file_uploads = []
        self.bytes_uploads = []
        self.sinks = []

    async def open_upload_sink(self, key, **kwargs):
        sink = _Recorder(key)
        self.sinks.append(sink)
        return sink

    async def upload_file(self, src_path, dest_key):
        with open(src_path, "rb") as fh:
            self.file_uploads.append((dest_key, len(fh.read())))
        return dest_key

    async def upload_bytes(self, data, dest_key):
        self.bytes_uploads.append((dest_key, len(data)))
        return dest_key

    async def exists(self, key):
        return True

    async def get_file_size(self, key):
        return None


def _install(monkeypatch, tmp_path, *, stream_ok=True, mode="stream", cancel_midway=False):
    """A pipeline whose download, storage, cache and queue are all fakes."""
    backend = _StreamStorage()
    jobs = []
    streamed = []

    async def _get_storage():
        return backend

    async def _remember(*_args, **_kwargs):
        return True

    async def _lookup(*_args, **_kwargs):
        return None

    async def _probe(_path):
        return {"duration": 12.5, "format_name": "mov,mp4,m4a", "width": 1920}

    async def _enqueue(job):
        jobs.append(job)
        return True

    async def _download_to_sink(chat_id, message_id, sink, **kwargs):
        streamed.append((chat_id, message_id, sink, kwargs))
        if not stream_ok:
            return False
        await sink.open()
        for start in range(0, len(SOURCE_BYTES), CHUNK):
            sink.write(SOURCE_BYTES[start : start + CHUNK])
            if cancel_midway:
                callback = kwargs.get("progress_callback")
                if callback is not None:
                    callback(sink.tell(), len(SOURCE_BYTES))
        return True

    async def _download(self, chat_id, message_id, dest_path, progress_callback=None, user_id=None):
        with open(dest_path, "wb") as fh:
            fh.write(SOURCE_BYTES)
        return True

    monkeypatch.setattr(bigfile_pipeline, "PIPELINE_SOURCE_UPLOAD", mode)
    monkeypatch.setattr(bigfile_pipeline, "get_storage_backend", _get_storage, raising=False)
    monkeypatch.setattr(bigfile_pipeline, "get_cache", None, raising=False)
    monkeypatch.setattr(bigfile_pipeline.media_cache, "cache_enabled", lambda: True)
    monkeypatch.setattr(bigfile_pipeline.media_cache, "remember", _remember)
    monkeypatch.setattr(bigfile_pipeline.media_cache, "lookup", _lookup)
    monkeypatch.setattr(BigFilePipeline, "_download_via_pyrogram", _download)
    monkeypatch.setattr(userbot_downloader, "download_media_to_sink", _download_to_sink)

    import utils.ffmpeg_runner as ffmpeg_runner
    import utils.job_queue as job_queue

    # The probe verdict travels in the job's Redis hash, not the job dict.
    hashes = []

    class _FakeRedis:
        async def hset(self, key, mapping=None):
            hashes.append(dict(mapping or {}))
            return len(mapping or {})

        async def close(self):
            return None

    async def _get_redis():
        return _FakeRedis()

    monkeypatch.setattr(ffmpeg_runner, "probe_media", _probe)
    monkeypatch.setattr(job_queue, "enqueue_job", _enqueue)
    monkeypatch.setattr(job_queue, "get_redis", _get_redis)
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path))
    monkeypatch.setenv("REUSE_LOCAL_INPUT", "1")
    return backend, jobs, streamed, hashes


def _ingest(monkeypatch, tmp_path, **kwargs):
    # A cancel check belongs to the ingest call, not to the fakes.
    cancel_check = kwargs.pop("cancel_check", None)
    backend, jobs, streamed, hashes = _install(monkeypatch, tmp_path, **kwargs)

    async def _run_coro():
        pipeline = BigFilePipeline()
        return await pipeline.ingest_large_file(
            chat_id=-1001234567890,
            message_id=311,
            file_size=len(SOURCE_BYTES),
            file_unique_id="AgADCSIAAtbSIVE",
            user_id=1405333465,
            original_filename="movie.mp4",
            cancel_check=cancel_check,
        )

    return _run(_run_coro()), backend, jobs, streamed, hashes


def test_stream_mode_puts_the_media_in_the_bucket_without_a_local_copy(monkeypatch, tmp_path):
    result, backend, jobs, streamed, hashes = _ingest(monkeypatch, tmp_path)

    assert result.ok
    # One media is one shared object, because stream mode stores the whole file.
    expected_key = media_cache.media_library_key("AgADCSIAAtbSIVE")
    assert result.s3_key == expected_key

    # The download wrote the bucket; nothing was uploaded a second time.
    assert backend.file_uploads == []
    assert backend.bytes_uploads == []
    assert len(backend.sinks) == 1
    assert backend.sinks[0].closed == 1
    assert backend.sinks[0].aborted == 0
    assert backend.sinks[0].tell() == len(SOURCE_BYTES)

    # The download was asked for the sink itself, not for a path.
    assert len(streamed) == 1
    _, _, sink, _ = streamed[0]
    assert isinstance(sink, storage.HeadCaptureSink)

    job = jobs[-1]
    assert job["input_key"] == expected_key
    # The object is the media, not a probe header.
    assert job["input_header_only"] == 0
    # No local source exists for this run, so the worker reads the object.
    assert job["input_path"] is None
    assert job["source_chat_id"] == -1001234567890
    assert job["source_message_id"] == 311
    # The tapped header was probed, so the job still carries real metadata.
    assert hashes[-1]["source_duration"] == "12.5"
    assert hashes[-1]["source_width"] == "1920"


def test_the_streamed_head_is_probed_from_a_throwaway_file(monkeypatch, tmp_path):
    """The probe needs a file on disk; the pipeline must not leave one behind."""
    _result, _backend, _jobs, _streamed, _hashes = _ingest(monkeypatch, tmp_path)
    temp_dir = os.path.join(str(tmp_path), "temp")
    leftovers = [name for name in os.listdir(temp_dir)] if os.path.isdir(temp_dir) else []
    assert [name for name in leftovers if name.startswith("stream_head_")] == []


def test_a_failed_stream_aborts_the_upload_and_uses_the_disk(monkeypatch, tmp_path):
    result, backend, jobs, streamed, hashes = _ingest(monkeypatch, tmp_path, stream_ok=False)

    assert result.ok
    assert len(streamed) == 1
    # Nothing half-written may be left for a later job to mistake for a source.
    assert backend.sinks[0].aborted == 1
    assert backend.sinks[0].closed == 0

    # The disk path took over and uploaded the whole file...
    assert len(backend.file_uploads) == 1
    assert backend.file_uploads[0][1] == len(SOURCE_BYTES)

    # ...and the job is a normal full-object job, with the local copy handed over.
    job = jobs[-1]
    assert job["input_key"] == backend.file_uploads[0][0]
    assert job["input_header_only"] == 0
    assert os.path.exists(job["input_path"])


def test_a_cancelled_batch_aborts_the_stream(monkeypatch, tmp_path):
    """A cancel has to interrupt the transfer, not wait for the whole file."""
    result, backend, jobs, _streamed, _hashes = _ingest(
        monkeypatch, tmp_path, cancel_midway=True, cancel_check=lambda: True
    )

    assert result.ok is False
    assert result.error == "batch cancelled"
    # Nothing half-written is left in the bucket and nothing was queued.
    assert backend.sinks[0].aborted == 1
    assert backend.sinks[0].closed == 0
    assert jobs == []


def test_stream_mode_is_a_valid_setting():
    assert bigfile_pipeline.PIPELINE_SOURCE_UPLOAD in ("header", "full", "local", "stream")
    assert "stream" in read_source("utils", "bigfile_pipeline.py")
    # `stream` stores a whole object, so it takes the shared library key too.
    src = read_source("utils", "bigfile_pipeline.py")
    assert 'PIPELINE_SOURCE_UPLOAD in ("full", "stream")' in src


def test_both_paths_that_probe_a_source_map_it_the_same_way():
    """One mapper, so the worker never sees two shapes of ``source_*``."""
    tree = parse_source("utils", "bigfile_pipeline.py")
    assert "_flatten_source_meta" in defined_functions(tree)
    uses = read_source("utils", "bigfile_pipeline.py")
    # The definition plus the disk path and the streaming path.
    assert uses.count("_flatten_source_meta(") >= 3
    # The inline copy of that mapping must be gone, not duplicated.
    assert uses.count('"source_video_bitrate"') == 1


# ─────────────────────────────────────────────────────────────────────────────
# The download itself: Telethon writes into the sink
# ─────────────────────────────────────────────────────────────────────────────


class _FakeTelethonClient:
    """The slice of Telethon this path uses: start, download_media, disconnect."""

    def __init__(self, *, chunks=(b"x" * 10,), msg=True):
        self.chunks = chunks
        self.msg = msg
        self.download_calls = []
        self.started = 0
        self.disconnected = 0

    async def start(self):
        self.started += 1

    async def get_messages(self, *args, **kwargs):
        return None

    async def download_media(self, message, **kwargs):
        self.download_calls.append(kwargs)
        sink = kwargs["file"]
        for chunk in self.chunks:
            pending = sink.write(chunk)
            if pending is not None:
                await pending
        return "done"

    async def disconnect(self):
        self.disconnected += 1


class _FakeMessage:
    media = object()


def _patch_telethon(monkeypatch, client):
    import utils.telethon_session as telethon_session

    async def _session_str(**_kwargs):
        return "session"

    async def _normalize(chat_id, _client=None):
        return chat_id

    async def _resolve(_client, _chat_id, _message_id, target=None):
        return [_FakeMessage()]

    monkeypatch.setattr(telethon_session, "build_telethon_client", lambda *a, **k: client)
    monkeypatch.setattr(telethon_session, "get_telethon_session_string_for_user", _session_str)
    monkeypatch.setattr(telethon_session, "get_db_model", lambda: None)
    monkeypatch.setattr(telethon_session, "get_userbot_credentials", lambda: (1, "hash"))
    monkeypatch.setattr(userbot_downloader, "_normalize_target", _normalize)
    monkeypatch.setattr(userbot_downloader, "_resolve_message_via_telethon", _resolve)


def test_the_download_writes_into_the_sink_itself(monkeypatch):
    client = _FakeTelethonClient(chunks=(b"a" * 100, b"b" * 100, b"c" * 56))
    _patch_telethon(monkeypatch, client)
    sink = _Recorder()

    ok = _run(userbot_downloader.download_media_to_sink(-1001, 5, sink, expected_size=256))

    assert ok is True
    assert len(client.download_calls) == 1
    # The sink is handed to Telethon as the output file: this identity is the
    # whole feature - a path here means a full local copy first.
    assert client.download_calls[0]["file"] is sink
    assert sink.tell() == 256
    assert client.disconnected == 1


def test_a_longer_stream_than_announced_is_still_accepted(monkeypatch):
    """A size Telegram merely rounded is no reason to drop a good copy."""
    client = _FakeTelethonClient(chunks=(b"a" * 120,))
    _patch_telethon(monkeypatch, client)
    sink = _Recorder()

    assert _run(userbot_downloader.download_media_to_sink(-1001, 5, sink, expected_size=100)) is True


def test_a_short_stream_is_refused(monkeypatch):
    client = _FakeTelethonClient(chunks=(b"a" * 100,))
    _patch_telethon(monkeypatch, client)
    sink = _Recorder()

    ok = _run(userbot_downloader.download_media_to_sink(-1001, 5, sink, expected_size=999))

    assert ok is False
    # The caller owns the lifecycle, so the sink is still theirs to abort.
    assert sink.closed == 0
    assert sink.aborted == 0
    assert client.disconnected == 1


def test_streaming_without_a_session_is_a_clean_no(monkeypatch):
    import utils.telethon_session as telethon_session

    monkeypatch.setattr(telethon_session, "build_telethon_client", lambda *a, **k: None)
    sink = _Recorder()
    assert _run(userbot_downloader.download_media_to_sink(-1001, 5, sink)) is False
    assert sink.chunks == []


def test_streaming_is_offered_only_by_telethon():
    """Pyrogram's download_media takes a path; the sink path must be Telethon's."""
    src = read_source("utils", "userbot_downloader.py")
    assert "Telethon-only" in src
    # The sink is what Telethon is told to write into. Handing it a path here
    # would put the whole media on disk first, which is the thing being removed.
    assert '"file": sink' in src


def test_both_telethon_paths_share_one_peer_resolver():
    tree = parse_source("utils", "userbot_downloader.py")
    resolver = find_function(tree, "_resolve_message_via_telethon")
    assert resolver is not None
    dst = read_source("utils", "userbot_downloader.py")
    assert dst.count("_resolve_message_via_telethon(") >= 3


# ─────────────────────────────────────────────────────────────────────────────
# The worker still understands a streamed job
# ─────────────────────────────────────────────────────────────────────────────


def test_a_streamed_job_is_a_normal_full_object_job():
    """The worker must treat it as the media, not as a probe reference."""
    src = read_source(*WORKER)
    assert "input_header_only" in src
    assert "fetching the source over Telegram" in src


@pytest.mark.parametrize("name", ["HeadCaptureSink", "UploadSink", "S3UploadSink", "BufferedUploadSink"])
def test_the_sink_types_are_exported(name):
    assert hasattr(storage, name)
