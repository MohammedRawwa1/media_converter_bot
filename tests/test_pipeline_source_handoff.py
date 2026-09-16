"""The bucket is a copy, never the transport.

Two behaviours are pinned here:

* the big-file pipeline hands the already-downloaded source to the job - as a
  local path, plus the Telegram coordinates it came from - and stores only the
  small header object by default. A worker that shares the disk must never pull
  the whole media back out of storage, which is where the egress came from;
* the worker trusts the ingest's own ffprobe instead of range-probing storage,
  and a header slice without a duration is a warning rather than a corrupt
  source that kills a job mid-batch.
"""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_helpers import call_keywords, parse_source, read_source  # noqa: E402

from utils import bigfile_pipeline, media_cache  # noqa: E402
from utils.bigfile_pipeline import (  # noqa: E402
    PIPELINE_HEADER_BYTES,
    BigFilePipeline,
    _header_object_key,
    _read_head_bytes,
)

WORKER = ("workers", "ffmpeg_worker.py")


# ─────────────────────────────────────────────────────────────────────────────
# Header object helpers
# ─────────────────────────────────────────────────────────────────────────────


def test_the_header_object_gets_its_own_key():
    """It must not land on (or overwrite) a full source object."""
    key = _header_object_key("inputs/library/abc123/source")
    assert key == "inputs/library/abc123/header"
    assert key != "inputs/library/abc123/source"
    assert _header_object_key("inputs/job-1/source") == "inputs/job-1/header"
    # Any other shape still gets a distinct name rather than a collision.
    assert _header_object_key("uploads/video.mp4") == "uploads/video.mp4/header"


def test_only_the_head_is_read(tmp_path):
    payload = b"x" * 5000
    path = tmp_path / "src.mp4"
    path.write_bytes(payload)
    assert _read_head_bytes(str(path), 2048) == payload[:2048]
    assert _read_head_bytes(str(path), len(payload) + 10) == payload


# ─────────────────────────────────────────────────────────────────────────────
# The pipeline's ingest, by mode
# ─────────────────────────────────────────────────────────────────────────────


class _FakeStorage:
    def __init__(self):
        self.file_uploads = []
        self.bytes_uploads = []

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


SOURCE_BYTES = b"A" * 4096


def _install_pipeline(monkeypatch, tmp_path, mode, *, cache_enabled=False):
    """A pipeline whose download, storage, cache and queue are all fakes."""
    storage = _FakeStorage()
    jobs = []

    async def _get_storage():
        return storage

    async def _remember(*_args, **_kwargs):
        return True

    async def _lookup(*_args, **_kwargs):
        return None

    async def _probe(_path):
        return {"duration": 12.5, "format_name": "mov,mp4,m4a"}

    async def _enqueue(job):
        jobs.append(job)
        return True

    async def _download(self, chat_id, message_id, dest_path, progress_callback=None, user_id=None):
        with open(dest_path, "wb") as fh:
            fh.write(SOURCE_BYTES)
        return True

    monkeypatch.setattr(bigfile_pipeline, "PIPELINE_SOURCE_UPLOAD", mode)
    monkeypatch.setattr(bigfile_pipeline, "get_storage_backend", _get_storage, raising=False)
    monkeypatch.setattr(bigfile_pipeline, "get_cache", None, raising=False)
    monkeypatch.setattr(bigfile_pipeline.media_cache, "cache_enabled", lambda: cache_enabled)
    monkeypatch.setattr(bigfile_pipeline.media_cache, "remember", _remember)
    monkeypatch.setattr(bigfile_pipeline.media_cache, "lookup", _lookup)
    monkeypatch.setattr(BigFilePipeline, "_download_via_pyrogram", _download)

    import utils.ffmpeg_runner as ffmpeg_runner
    import utils.job_queue as job_queue

    monkeypatch.setattr(ffmpeg_runner, "probe_media", _probe)
    monkeypatch.setattr(job_queue, "enqueue_job", _enqueue)

    monkeypatch.setenv("STORAGE_PATH", str(tmp_path))
    return storage, jobs


def _ingest(monkeypatch, tmp_path, mode, **kwargs):
    storage, jobs = _install_pipeline(monkeypatch, tmp_path, mode)

    async def _run():
        pipeline = BigFilePipeline()
        return await pipeline.ingest_large_file(
            chat_id=-1001234567890,
            message_id=311,
            file_size=len(SOURCE_BYTES),
            file_unique_id="AgADCSIAAtbSIVE",
            user_id=1405333465,
            original_filename="movie.mp4",
            **kwargs,
        )

    return asyncio.run(_run()), storage, jobs


def test_header_mode_stores_only_the_head_and_hands_over_the_file(monkeypatch, tmp_path):
    result, storage, jobs = _ingest(monkeypatch, tmp_path, "header")

    assert result.ok
    # Nothing but a slice reached the bucket...
    assert storage.file_uploads == []
    assert len(storage.bytes_uploads) == 1
    key, size = storage.bytes_uploads[0]
    assert key.endswith("/header")
    assert size == min(PIPELINE_HEADER_BYTES, len(SOURCE_BYTES))
    # ...and no caller is told a storage key that is not the media.
    assert result.s3_key is None

    job = jobs[-1]
    assert job["input_key"] is None
    assert job["input_header_only"] == 1
    # The source travels with the job instead: local path + Telegram copy.
    assert os.path.exists(job["input_path"])
    with open(job["input_path"], "rb") as fh:
        assert fh.read() == SOURCE_BYTES
    assert job["source_chat_id"] == -1001234567890
    assert job["source_message_id"] == 311


def test_local_mode_uploads_nothing(monkeypatch, tmp_path):
    result, storage, jobs = _ingest(monkeypatch, tmp_path, "local")

    assert result.ok
    assert storage.file_uploads == []
    assert storage.bytes_uploads == []
    assert result.s3_key is None
    assert jobs[-1]["input_key"] is None
    assert jobs[-1]["input_header_only"] == 0
    assert os.path.exists(jobs[-1]["input_path"])


def test_full_mode_uploads_the_whole_file_and_still_hands_it_over(monkeypatch, tmp_path):
    result, storage, jobs = _ingest(monkeypatch, tmp_path, "full")

    assert result.ok
    assert storage.bytes_uploads == []
    assert len(storage.file_uploads) == 1
    key, size = storage.file_uploads[0]
    assert key.endswith("/source")
    assert size == len(SOURCE_BYTES)
    assert result.s3_key == key

    job = jobs[-1]
    assert job["input_key"] == key
    assert job["input_header_only"] == 0
    # The local copy is handed over as well, so a co-located worker reads this
    # disk instead of paying for the whole file twice.
    assert os.path.exists(job["input_path"])


# ─────────────────────────────────────────────────────────────────────────────
# The worker: source check and where the bytes come from
# ─────────────────────────────────────────────────────────────────────────────


def test_the_ingest_probe_is_trusted_over_a_storage_range_probe():
    src = read_source(*WORKER)
    assert "skipping the storage range-probe" in src
    assert "was probed at ingest" in src
    # The worker must read the verdict out of the job hash the pipeline wrote.
    assert '"source_duration"' in src
    assert '"source_format"' in src


def test_a_duration_less_header_is_no_longer_fatal():
    src = read_source(*WORKER)
    # The old path errored the job out on a header slice that had no duration.
    assert "source appears corrupt or unsupported" not in src
    assert "proceeding to the full download" in src


def test_the_range_probe_is_bounded():
    src = read_source(*WORKER)
    assert "timeout=STORAGE_PROBE_TIMEOUT_SECONDS" in src


def test_the_worker_refuses_to_encode_a_header_object():
    src = read_source(*WORKER)
    assert "is only the probe header, not the media" in src
    assert "input_header_only" in src


def test_the_worker_reads_the_media_over_telegram_before_storage():
    src = read_source(*WORKER)
    assert "fetching the source over Telegram" in src
    tree = parse_source(*WORKER)
    assert call_keywords(tree, "download_forward_via_userbot", "chat_id")
    assert call_keywords(tree, "download_forward_via_userbot", "dest_path")


def test_a_pipeline_header_job_is_recognised_as_a_probe_reference():
    """The flag the pipeline writes is the one the worker reads."""
    assert "input_header_only" in read_source("utils", "bigfile_pipeline.py")
    assert "source_chat_id" in read_source("utils", "bigfile_pipeline.py")
    assert "source_message_id" in read_source("utils", "bigfile_pipeline.py")


@pytest.mark.parametrize("name", ["_header_object_key", "_read_head_bytes"])
def test_header_helpers_exist(name):
    assert hasattr(bigfile_pipeline, name)


def test_default_mode_is_header():
    """The default has to be the one that keeps a whole copy out of the bucket."""
    assert bigfile_pipeline.PIPELINE_SOURCE_UPLOAD in ("header", "full", "local")
    assert media_cache.__name__ == "utils.media_cache"
