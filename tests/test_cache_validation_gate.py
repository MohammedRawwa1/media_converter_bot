"""Validate the cache against storage before downloading anything.

Re-submitting a media must be answered from the media cache and the object the
descriptor points at - never by pulling the file out of Telegram again. These
tests pin the gate that decides that:

* :func:`stored_object_is_intact` - the existence + size check a descriptor has
  to pass before it is trusted, and the rule that a backend hiccup must not read
  as a miss (a false negative here *is* the re-download the gate exists for);
* the big-file pipeline - a validated descriptor skips the Pyrogram download
  entirely, whichever tier answered (whole object, local copy, probe header),
  while one whose object is gone or the wrong size does not;
* the shared probe-header key - ``inputs/library/<hash>/header`` is what makes
  "this media was already ingested" provable in the default ``header`` mode,
  where nothing at all used to be remembered;
* promote-on-repeat - the second request for a media is the point where a whole
  object starts paying for itself, so the run that proves the media was seen
  before is the run that stores it whole;
* the worker's recovery fallback - when a header job's Telegram copy is gone, a
  stored copy of the same media (from a promotion, ``full``/``stream`` mode or
  the Bot-API path) is used instead of failing the job.
"""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_helpers import read_object_source, read_source  # noqa: E402

from utils import bigfile_pipeline, media_cache, storage  # noqa: E402
from workers import ffmpeg_worker  # noqa: E402

UNIQUE_ID = "AgADCSIAAtbSIVE"
LIBRARY_KEY = media_cache.media_library_key(UNIQUE_ID)
HEADER_KEY = f"{LIBRARY_KEY.rsplit('/', 1)[0]}/header"
SOURCE_BYTES = b"A" * 4096


# ─────────────────────────────────────────────────────────────────────────────
# The evidence check itself
# ─────────────────────────────────────────────────────────────────────────────


class _Head:
    """A backend that only answers the two metadata questions the gate asks."""

    def __init__(self, *, exists=True, size=None, boom=False):
        self._exists = exists
        self._size = size
        self._boom = boom
        self.heads = 0

    async def exists(self, key):
        self.heads += 1
        if self._boom:
            raise RuntimeError("s3 is having a moment")
        return self._exists

    async def get_file_size(self, key):
        if self._boom:
            raise RuntimeError("s3 is having a moment")
        return self._size


def _intact(backend, key="inputs/library/abc/source", expected_size=1000):
    return asyncio.run(storage.stored_object_is_intact(backend, key, expected_size=expected_size))


def test_a_missing_object_is_not_evidence():
    assert _intact(_Head(exists=False)) is False


def test_an_empty_key_is_never_a_hit():
    assert _intact(_Head(), key=None) is False
    assert _intact(_Head(), key="") is False


def test_the_stored_size_has_to_agree_with_what_arrived():
    # Same file_unique_id, different media: the descriptor must not be reused.
    assert _intact(_Head(size=999), expected_size=1000) is False
    assert _intact(_Head(size=1000), expected_size=1000) is True


def test_a_backend_without_a_size_still_counts_as_present():
    assert _intact(_Head(size=None)) is True


def test_a_backend_error_is_not_a_cache_miss():
    """A transient failure must not cost a re-download."""
    assert _intact(_Head(boom=True)) is True


def test_the_probe_is_metadata_only():
    """The whole point: validating a cached object costs no egress."""
    backend = _Head(size=1000)
    _intact(backend)
    assert backend.heads == 1


# ─────────────────────────────────────────────────────────────────────────────
# The pipeline gate
# ─────────────────────────────────────────────────────────────────────────────


class _FakeStorage:
    def __init__(self, objects):
        # key -> stored size; absence means the object is gone.
        self.objects = dict(objects)
        self.bytes_uploads = []
        self.file_uploads = []

    async def upload_file(self, src_path, dest_key):
        with open(src_path, "rb") as fh:
            self.file_uploads.append((dest_key, len(fh.read())))
        return dest_key

    async def upload_bytes(self, data, dest_key):
        self.bytes_uploads.append((dest_key, len(data)))
        return dest_key

    async def exists(self, key):
        return key in self.objects

    async def get_file_size(self, key):
        return self.objects.get(key)


def _install(monkeypatch, tmp_path, mode, *, entry=None, objects=None):
    """A pipeline whose download, storage, cache and queue are all fakes."""
    backend = _FakeStorage(objects or {})
    jobs = []
    hashes = []
    downloads = []
    remembered = []

    async def _get_storage():
        return backend

    async def _lookup(*_args, **_kwargs):
        return entry

    async def _remember(*args, **kwargs):
        remembered.append({"args": args, "kwargs": kwargs})
        return True

    async def _probe(_path):
        return {"duration": 12.5, "format_name": "mov,mp4,m4a", "width": 1920}

    async def _enqueue(job):
        jobs.append(job)
        return True

    async def _download(self, chat_id, message_id, dest_path, progress_callback=None, user_id=None):
        downloads.append(dest_path)
        with open(dest_path, "wb") as fh:
            fh.write(SOURCE_BYTES)
        return True

    class _FakeRedis:
        async def hset(self, key, mapping=None):
            hashes.append(dict(mapping or {}))
            return len(mapping or {})

        async def close(self):
            return None

    async def _get_redis():
        return _FakeRedis()

    monkeypatch.setattr(bigfile_pipeline, "PIPELINE_SOURCE_UPLOAD", mode)
    monkeypatch.setattr(bigfile_pipeline, "get_storage_backend", _get_storage, raising=False)
    monkeypatch.setattr(bigfile_pipeline, "get_cache", None, raising=False)
    monkeypatch.setattr(bigfile_pipeline.media_cache, "cache_enabled", lambda: True)
    monkeypatch.setattr(bigfile_pipeline.media_cache, "lookup", _lookup)
    monkeypatch.setattr(bigfile_pipeline.media_cache, "remember", _remember)
    monkeypatch.setattr(bigfile_pipeline.BigFilePipeline, "_download_via_pyrogram", _download)

    import utils.ffmpeg_runner as ffmpeg_runner
    import utils.job_queue as job_queue

    monkeypatch.setattr(ffmpeg_runner, "probe_media", _probe)
    monkeypatch.setattr(job_queue, "enqueue_job", _enqueue)
    monkeypatch.setattr(job_queue, "get_redis", _get_redis)
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path))
    monkeypatch.setenv("REUSE_LOCAL_INPUT", "1")

    return backend, jobs, hashes, downloads, remembered


def _ingest(monkeypatch, tmp_path, mode, **kwargs):
    backend, jobs, hashes, downloads, remembered = _install(monkeypatch, tmp_path, mode, **kwargs)

    async def _run():
        pipeline = bigfile_pipeline.BigFilePipeline()
        return await pipeline.ingest_large_file(
            chat_id=-1001234567890,
            message_id=311,
            file_size=len(SOURCE_BYTES),
            file_unique_id=UNIQUE_ID,
            user_id=1405333465,
            original_filename="movie.mp4",
        )

    return asyncio.run(_run()), backend, jobs, hashes, downloads, remembered


def _descriptor(**overrides):
    entry = {
        "file_unique_id": UNIQUE_ID,
        "size": len(SOURCE_BYTES),
        "input_key": None,
        "header_key": None,
        "header_only": False,
        "path": None,
        "source_meta": None,
    }
    entry.update(overrides)
    return entry


def _base_meta():
    return {"duration": "12.5", "format_name": "mov,mp4,m4a", "width": "1920"}


def test_header_mode_stores_the_probe_header_under_the_shared_library_key(monkeypatch, tmp_path):
    """Nothing was remembered before, so no repeat could ever be a hit."""
    result, backend, jobs, _hashes, _downloads, remembered = _ingest(monkeypatch, tmp_path, "header")

    assert result.ok
    assert backend.file_uploads == []
    assert len(backend.bytes_uploads) == 1
    key, size = backend.bytes_uploads[0]
    assert key == HEADER_KEY
    assert size == min(bigfile_pipeline.PIPELINE_HEADER_BYTES, len(SOURCE_BYTES))

    # The descriptor is written, and the header is *not* offered as a source.
    assert remembered, "the ingest has to leave evidence behind"
    call = remembered[-1]["kwargs"]
    assert call["header_key"] == HEADER_KEY
    assert call["header_only"] is True
    assert not call.get("input_key")
    assert jobs[-1]["input_key"] is None
    assert jobs[-1]["input_header_only"] == 1


def test_a_validated_probe_header_skips_the_pyrogram_download(monkeypatch, tmp_path):
    """With promotion off, the header is evidence and nothing else."""
    monkeypatch.setenv(bigfile_pipeline.PIPELINE_PROMOTE_ON_REPEAT_ENV, "0")
    entry = _descriptor(header_key=HEADER_KEY, header_only=True, source_meta={"duration": "12.5", "width": "1920"})
    result, _backend, jobs, hashes, downloads, _remembered = _ingest(
        monkeypatch, tmp_path, "header", entry=entry, objects={HEADER_KEY: 2048}
    )

    assert result.ok
    assert downloads == [], "a validated descriptor must not be downloaded again"
    job = jobs[-1]
    # The media itself is still on Telegram, so the worker reads it there.
    assert job["input_key"] is None
    assert job["input_path"] is None
    assert job["source_chat_id"] == -1001234567890
    assert job["source_message_id"] == 311
    # ...and the caption still has the metadata the first ingest probed.
    assert hashes[-1]["source_duration"] == "12.5"
    assert hashes[-1]["source_width"] == "1920"
    assert result.source_metadata == {"duration": "12.5", "width": "1920"}


def test_a_swept_probe_header_does_not_short_circuit(monkeypatch, tmp_path):
    """Evidence that is gone is worth nothing: the download has to happen."""
    entry = _descriptor(header_key=HEADER_KEY, header_only=True)
    result, _backend, _jobs, _hashes, downloads, _remembered = _ingest(
        monkeypatch, tmp_path, "header", entry=entry, objects={}
    )

    assert result.ok
    assert len(downloads) == 1


def test_a_whole_object_of_the_wrong_size_is_not_reused(monkeypatch, tmp_path):
    entry = _descriptor(input_key=LIBRARY_KEY)
    result, _backend, _jobs, _hashes, downloads, _remembered = _ingest(
        monkeypatch, tmp_path, "full", entry=entry, objects={LIBRARY_KEY: len(SOURCE_BYTES) + 1}
    )

    assert result.ok
    assert len(downloads) == 1


def test_a_second_request_promotes_the_media_to_a_whole_object(monkeypatch, tmp_path):
    """The repeat re-fetches the media once - and stores it whole this time.

    That is the trade promote-on-repeat exists for: a media asked for twice then
    stops being read over Telegram for every job, while media asked for once
    never cost more than their 2 MB header.
    """
    monkeypatch.delenv(bigfile_pipeline.PIPELINE_PROMOTE_ON_REPEAT_ENV, raising=False)
    entry = _descriptor(header_key=HEADER_KEY, header_only=True, source_meta={"duration": "12.5"})
    result, backend, jobs, _hashes, downloads, remembered = _ingest(
        monkeypatch, tmp_path, "header", entry=entry, objects={HEADER_KEY: 2048}
    )

    assert result.ok
    # It did download the media - that is what promotion costs...
    assert len(downloads) == 1
    # ...and the whole file went to the shared library key, not a slice of it.
    assert backend.bytes_uploads == []
    assert backend.file_uploads == [(LIBRARY_KEY, len(SOURCE_BYTES))]

    call = remembered[-1]["kwargs"]
    assert call["input_key"] == LIBRARY_KEY
    # The whole-object descriptor supersedes the header: nothing marks this entry
    # as a probe reference any more.
    assert not call.get("header_only")
    assert not call.get("header_key")

    # The job is now a normal whole-object job: no Telegram read for the worker.
    job = jobs[-1]
    assert job["input_key"] == LIBRARY_KEY
    assert job["input_header_only"] == 0
    assert result.s3_key == LIBRARY_KEY


@pytest.mark.parametrize("mode", ["header", "full", "stream"])
def test_a_whole_object_of_the_right_size_is_never_re_downloaded(monkeypatch, tmp_path, mode):
    """Whichever mode produced it, a validated object ends the Telegram traffic."""
    entry = _descriptor(input_key=LIBRARY_KEY)
    _result, _backend, _jobs, _hashes, downloads, _remembered = _ingest(
        monkeypatch, tmp_path, mode, entry=entry, objects={LIBRARY_KEY: len(SOURCE_BYTES)}
    )

    assert downloads == []


@pytest.mark.parametrize("mode", ["full", "stream"])
def test_a_validated_whole_object_is_served_from_the_bucket(monkeypatch, tmp_path, mode):
    """The evidence is the object itself: no Telegram traffic at all.

    Both modes that store a whole object resolve to the same shared key, so the
    repeat path is the same one regardless of which of them produced it.
    """
    entry = _descriptor(input_key=LIBRARY_KEY, source_meta={"duration": "12.5", "width": "1920"})
    result, _backend, jobs, hashes, downloads, _remembered = _ingest(
        monkeypatch, tmp_path, mode, entry=entry, objects={LIBRARY_KEY: len(SOURCE_BYTES)}
    )

    assert result.ok
    assert downloads == []
    assert result.s3_key == LIBRARY_KEY
    job = jobs[-1]
    assert job["input_key"] == LIBRARY_KEY
    assert job["input_header_only"] == 0
    assert job["input_path"] is None
    assert hashes[-1]["source_duration"] == "12.5"


def test_a_surviving_local_copy_is_preferred_over_telegram(monkeypatch, tmp_path):
    """A local file needs no bucket *and* no MTProto read."""
    local = tmp_path / "earlier_source.mp4"
    local.write_bytes(SOURCE_BYTES)
    entry = _descriptor(path=str(local))
    result, _backend, jobs, _hashes, downloads, _remembered = _ingest(
        monkeypatch, tmp_path, "header", entry=entry, objects={}
    )

    assert result.ok
    assert downloads == []
    job = jobs[-1]
    assert job["input_path"] == str(local)
    # No validated stored object backs the run, so no key is claimed.
    assert job["input_key"] is None


def test_a_descriptor_for_another_media_is_never_reused(monkeypatch, tmp_path):
    """The size validation happens in ``lookup``; here it is a plain miss."""
    result, _backend, _jobs, _hashes, downloads, remembered = _ingest(
        monkeypatch, tmp_path, "header", entry=None, objects={HEADER_KEY: 2048}
    )

    assert result.ok
    assert len(downloads) == 1
    assert remembered[-1]["kwargs"]["header_key"] == HEADER_KEY


def test_the_gate_is_wired_into_the_ingest():
    src = read_source("utils", "bigfile_pipeline.py")
    assert "stored_object_is_intact" in src
    assert "media_cache.lookup(" in src
    # The evidence is the header object's own key, not the job-scoped fallback.
    assert "_header_object_key(_library_key)" in src


@pytest.mark.parametrize("name", ["stored_object_is_intact"])
def test_the_storage_helper_is_public(name):
    assert callable(getattr(storage, name))


def test_remember_carries_the_header_evidence_and_the_probe_verdict(monkeypatch):
    """The descriptor has to be able to prove *and* describe a repeat."""
    stored = {}

    class _Cache:
        async def cache_file_info(self, key, entry, ttl=None):
            stored[key] = dict(entry)
            return True

    async def _get_cache():
        return _Cache()

    import utils.cache as cache_mod

    monkeypatch.setattr(cache_mod, "get_cache", _get_cache)
    # The durable tier would need MongoDB; the Redis tier is the one under test.
    monkeypatch.setenv("MEDIA_REGISTRY_ENABLED", "0")

    ok = asyncio.run(
        media_cache.remember(
            UNIQUE_ID,
            size=len(SOURCE_BYTES),
            header_key=HEADER_KEY,
            header_only=True,
            storage="s3",
            source_meta={"duration": "12.5"},
        )
    )

    assert ok is True
    entry = stored[UNIQUE_ID]
    assert entry["header_key"] == HEADER_KEY
    assert entry["header_only"] is True
    assert entry["input_key"] is None
    assert entry["source_meta"] == {"duration": "12.5"}


# ─────────────────────────────────────────────────────────────────────────────
# The worker's recovery fallback
# ─────────────────────────────────────────────────────────────────────────────


def _recover(monkeypatch, *, entry, backend=None, file_size=None, uid=UNIQUE_ID):
    """Run the worker's stored-copy lookup against fakes."""
    if file_size is None:
        file_size = len(SOURCE_BYTES)

    async def _lookup(*_args, **_kwargs):
        return entry

    async def _get_backend():
        return backend if backend is not None else _Head(exists=True, size=file_size)

    monkeypatch.setattr(media_cache, "lookup", _lookup)
    monkeypatch.setattr(ffmpeg_worker, "get_storage_backend", _get_backend)
    return asyncio.run(
        ffmpeg_worker._stored_source_for_job({"job_id": "job-1", "file_unique_id": uid, "file_size": file_size})
    )


def test_a_stored_copy_recovers_a_header_job(monkeypatch):
    """The relay copy is gone, but the media was stored whole somewhere else."""
    assert _recover(monkeypatch, entry=_descriptor(input_key=LIBRARY_KEY)) == LIBRARY_KEY


def test_a_probe_header_cannot_recover_anything(monkeypatch):
    """A header is evidence, not a source: it must never be adopted as one."""
    entry = _descriptor(header_key=HEADER_KEY, header_only=True)
    assert _recover(monkeypatch, entry=entry) is None


def test_a_swept_object_cannot_recover_anything(monkeypatch):
    assert _recover(monkeypatch, entry=_descriptor(input_key=LIBRARY_KEY), backend=_Head(exists=False)) is None


def test_a_stored_copy_of_the_wrong_size_cannot_recover_anything(monkeypatch):
    """The media changed under the same id: adopting it would feed a wrong file."""
    entry = _descriptor(input_key=LIBRARY_KEY)
    backend = _Head(exists=True, size=len(SOURCE_BYTES) + 1)
    assert _recover(monkeypatch, entry=entry, backend=backend) is None


def test_a_job_without_an_identity_cannot_recover_anything(monkeypatch):
    assert _recover(monkeypatch, entry=_descriptor(input_key=LIBRARY_KEY), uid=None) is None


def test_no_storage_backend_is_not_a_crash(monkeypatch):
    monkeypatch.setattr(ffmpeg_worker, "get_storage_backend", None)
    assert asyncio.run(ffmpeg_worker._stored_source_for_job({"file_unique_id": UNIQUE_ID})) is None


def test_the_recovery_runs_before_the_storage_download():
    """It has to set the key the download path then reads, not run after it."""
    src = read_object_source(ffmpeg_worker.handle_job)
    assert "_stored_source_for_job(job)" in src
    assert src.index("_stored_source_for_job(job)") < src.index(
        "_shared_cache_path = _find_library_source_cache(input_key"
    )
    # The give-up message has to say a stored copy was looked for too.
    assert "and no stored copy to recover it from" in src
