"""One media = one stored object, cached locally and reused by every operation.

Covers the three pieces that make that true:

* the local cache path derived from a shared library key (and only from one),
* the storage janitor no longer deleting the library object out from under it,
* the audio tags the player shows surviving the encode and the upload.
"""

import asyncio
import inspect
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_helpers import read_object_source, read_source  # noqa: E402

from tasks import cleanup_tasks as cleanup_mod  # noqa: E402
from utils import media_cache  # noqa: E402
from workers import ffmpeg_worker  # noqa: E402

LIBRARY_KEY = media_cache.media_library_key("AgADBAADy6cxG4testFileUniqueId")


# ─────────────────────────────────────────────────────────────────────────────
# Local cache path derivation
# ─────────────────────────────────────────────────────────────────────────────


def test_library_key_maps_to_a_stable_cache_path():
    first = ffmpeg_worker._library_source_cache_path(LIBRARY_KEY)
    second = ffmpeg_worker._library_source_cache_path(LIBRARY_KEY)
    assert first and first == second
    assert "library" in first.replace("\\", "/")
    # Same media, every operation: identical path, so the second style reuses it.
    assert ffmpeg_worker._library_source_cache_path(LIBRARY_KEY) == first


def test_extension_is_added_when_the_key_has_none():
    """media_cache writes `source` with no suffix; ffmpeg still needs one."""
    bare = ffmpeg_worker._library_source_cache_path(LIBRARY_KEY, ".mp4")
    assert bare.endswith("source.mp4")
    # An existing suffix is never doubled up.
    with_ext = ffmpeg_worker._library_source_cache_path("inputs/library/abc123/source.mkv", ".mp4")
    assert with_ext.endswith("source.mkv")


def test_per_job_keys_are_never_cached_under_a_shared_name():
    """A per-job object belongs to one job and must not be handed to another."""
    assert ffmpeg_worker._library_source_cache_path("inputs/6f2b/source.mp4") is None
    assert ffmpeg_worker._library_source_cache_path("outputs/job/out.mp4") is None
    assert ffmpeg_worker._library_source_cache_path("") is None
    assert ffmpeg_worker._library_source_cache_path(None) is None


def test_traversal_and_odd_shapes_are_refused():
    for key in (
        "inputs/library/../../etc/source.mp4",
        "inputs/library/ab/../../../x/source.mp4",
        "inputs/library/ab/nested/source.mp4",
        "inputs/library/ab",
        "inputs/library/ab/",
    ):
        assert ffmpeg_worker._library_source_cache_path(key) is None, key


def test_shared_cache_detection_matches_only_the_library_copy():
    cached = ffmpeg_worker._library_source_cache_path(LIBRARY_KEY)
    assert ffmpeg_worker._is_shared_source_cache(cached, LIBRARY_KEY) is True
    # A per-job temp copy is disposable.
    assert ffmpeg_worker._is_shared_source_cache("storage/temp/job1_src.mp4", LIBRARY_KEY) is False
    assert ffmpeg_worker._is_shared_source_cache(None, LIBRARY_KEY) is False
    assert ffmpeg_worker._is_shared_source_cache(cached, "inputs/6f2b/source.mp4") is False


def test_cached_copy_is_found_whether_or_not_it_carries_the_key_extension(tmp_path, monkeypatch):
    """The write names the cache file with an extension; the key has none.

    If lookup and identity only consider the bare key name, a repeat never sees
    the copy the previous job wrote, so it pays the full object out of the
    bucket again. Both must accept the extension-bearing file.
    """
    monkeypatch.setattr(ffmpeg_worker.config, "TEMP_PATH", str(tmp_path))
    cached = ffmpeg_worker._library_source_cache_path(LIBRARY_KEY, ".aac")
    os.makedirs(os.path.dirname(cached), exist_ok=True)
    with open(cached, "wb") as fh:
        fh.write(b"x" * 16)

    # Found with the same extension, with none, and by the cleanup guard.
    assert ffmpeg_worker._find_library_source_cache(LIBRARY_KEY, ".aac") == cached
    assert ffmpeg_worker._find_library_source_cache(LIBRARY_KEY) == cached
    assert ffmpeg_worker._is_shared_source_cache(cached, LIBRARY_KEY) is True


def test_empty_and_partial_files_are_never_served_as_a_cache_hit(tmp_path, monkeypatch):
    monkeypatch.setattr(ffmpeg_worker.config, "TEMP_PATH", str(tmp_path))
    cached = ffmpeg_worker._library_source_cache_path(LIBRARY_KEY, ".aac")
    os.makedirs(os.path.dirname(cached), exist_ok=True)
    open(cached, "wb").close()  # zero bytes is not a usable source
    assert ffmpeg_worker._find_library_source_cache(LIBRARY_KEY, ".aac") is None

    with open(cached, "wb") as fh:
        fh.write(b"partial")
    part = cached + ".part"
    with open(part, "wb") as fh:
        fh.write(b"y" * 16)
    os.remove(cached)
    # A leftover download fragment must not be handed to ffmpeg.
    assert ffmpeg_worker._find_library_source_cache(LIBRARY_KEY, ".aac") is None


def test_job_reuses_the_cache_and_keeps_it_after_the_job():
    """The source is downloaded into the cache, then deliberately not deleted."""
    src = read_object_source(ffmpeg_worker.handle_job)
    # The reuse check runs before the download and short-circuits it, matching
    # the copy by any extension rather than by the bare key name alone.
    assert "_shared_cache_path = _find_library_source_cache(input_key" in src
    assert "reusing shared local source cache" in src
    # Bytes that do arrive are written to the shared location, not a job-only one.
    assert "_library_source_cache_path(input_key, ext)" in src
    # And the post-job cleanup is guarded against removing it.
    assert "_is_shared_source_cache(input_path" in src
    assert "keeping shared source cache" in src


def test_downloads_land_atomically_so_the_cache_cannot_hold_half_a_media():
    """A cut connection must not leave a truncated file for later jobs to reuse."""
    src = read_object_source(ffmpeg_worker.handle_job)
    assert "os.replace(_part_path, temp_input_path)" in src
    assert "backend.download_file(input_key, _part_path)" in src


# ─────────────────────────────────────────────────────────────────────────────
# Storage janitor: the library object survives the inputs sweep
# ─────────────────────────────────────────────────────────────────────────────


class _FakeBackend:
    def __init__(self, objects):
        self.objects = objects
        self.deleted = []

    async def list_keys(self, prefix):
        return [o for o in self.objects if o["key"].startswith(prefix)]

    async def delete_keys(self, keys):
        self.deleted.extend(keys)
        return len(keys)


def _install_backend(monkeypatch, objects):
    backend = _FakeBackend(objects)

    async def _get():
        return backend

    import utils.storage as storage_mod

    monkeypatch.setattr(storage_mod, "get_storage_backend", _get)
    monkeypatch.setattr(cleanup_mod.logger, "info", lambda *a, **k: None)
    monkeypatch.setattr(cleanup_mod.config, "get_storage_backend_name", lambda: "s3")
    return backend


OLD = time.time() - 30 * 24 * 3600


def test_inputs_sweep_skips_the_library(monkeypatch):
    objects = [
        {"key": "inputs/library/abc123/source", "last_modified": OLD},
        {"key": "inputs/job1/source.mp4", "last_modified": OLD},
    ]
    backend = _install_backend(monkeypatch, objects)
    manager = cleanup_mod.CleanupManager()

    asyncio.run(manager.cleanup_s3_inputs())

    assert backend.deleted == ["inputs/job1/source.mp4"]


def test_library_objects_expire_on_their_own_longer_ttl(monkeypatch):
    fresh = time.time() - 3600  # inside S3_LIBRARY_TTL, outside S3_INPUT_TTL
    older = time.time() - 40 * 24 * 3600
    objects = [
        {"key": "inputs/library/fresh1/source", "last_modified": fresh},
        {"key": "inputs/library/stale1/source", "last_modified": older},
    ]
    backend = _install_backend(monkeypatch, objects)
    manager = cleanup_mod.CleanupManager()

    asyncio.run(manager.cleanup_s3_library())

    assert backend.deleted == ["inputs/library/stale1/source"]


def test_library_prefix_is_shared_with_the_cache_module():
    assert cleanup_mod.LIBRARY_KEY_PREFIX == media_cache.LIBRARY_KEY_PREFIX


def test_the_library_outlives_a_week_by_default(monkeypatch):
    """A media re-submitted weeks later must still be a hit, not a re-download.

    Expiring the shared object is the expensive side of the trade: the next
    request then pays a full copy of the media in egress plus the upload, while
    keeping it is one stored copy.
    """
    monkeypatch.delenv("S3_LIBRARY_TTL", raising=False)
    manager = cleanup_mod.CleanupManager()
    assert manager.s3_library_ttl >= 30 * 24 * 3600


# ─────────────────────────────────────────────────────────────────────────────
# Audio tags on delivery
# ─────────────────────────────────────────────────────────────────────────────


def test_audio_probe_falls_back_to_the_media_name_for_the_title(tmp_path, monkeypatch):
    src = tmp_path / "song.mp3"
    src.write_bytes(b"not really audio")

    async def _no_tags(path):
        return {"duration": 12}

    import utils.userbot_uploader as uploader

    monkeypatch.setattr(uploader, "_probe_audio_metadata", _no_tags)
    meta = asyncio.run(ffmpeg_worker._probe_audio_delivery(str(src), "My Song.mp3"))

    assert meta is not None
    assert meta["title"] == "My Song"
    assert meta["duration"] == 12


def test_audio_probe_keeps_real_tags(tmp_path, monkeypatch):
    src = tmp_path / "song.mp3"
    src.write_bytes(b"not really audio")

    async def _tags(path):
        return {"title": "Real Title", "performer": "Artist", "duration": 5}

    import utils.userbot_uploader as uploader

    monkeypatch.setattr(uploader, "_probe_audio_metadata", _tags)
    meta = asyncio.run(ffmpeg_worker._probe_audio_delivery(str(src), "other.mp3"))

    assert meta == {"title": "Real Title", "performer": "Artist", "duration": 5}


def test_audio_probe_returns_none_rather_than_a_partial_dict(tmp_path, monkeypatch):
    """A stub dict would suppress the uploader's own probe and lose its fields."""
    missing = tmp_path / "gone.mp3"
    assert asyncio.run(ffmpeg_worker._probe_audio_delivery(str(missing), "x.mp3")) is None

    src = tmp_path / "song.mp3"
    src.write_bytes(b"x")

    async def _boom(path):
        raise RuntimeError("ffprobe exploded")

    import utils.userbot_uploader as uploader

    monkeypatch.setattr(uploader, "_probe_audio_metadata", _boom)
    assert asyncio.run(ffmpeg_worker._probe_audio_delivery(str(src), "x.mp3")) is None


def test_audio_probe_hands_the_work_back_when_it_could_read_nothing(tmp_path, monkeypatch):
    """An empty probe must not become a half-filled dict.

    ``probe_audio_metadata`` returns ``{}`` when ffprobe is missing or refuses the
    file. Building the title fallback on top of that produced a title-only dict,
    which *suppresses* the uploader's own probe - the one that fills in the
    duration and the performer - so the send lost the very fields this exists to
    carry. The probe that raised already hands the work back; an empty one does
    too.
    """
    src = tmp_path / "song.mp3"
    src.write_bytes(b"not really audio")

    async def _nothing(path):
        return {}

    import utils.userbot_uploader as uploader

    monkeypatch.setattr(uploader, "_probe_audio_metadata", _nothing)

    assert asyncio.run(ffmpeg_worker._probe_audio_delivery(str(src), "My Song.mp3")) is None


def test_delivery_sites_pass_the_audio_tags_through():
    src = read_object_source(ffmpeg_worker.handle_job)
    assert src.count("audio_meta=_pre_am") == 2
    # Every place that sends the produced audio reads the tags off it: the two
    # MTProto uploads and the inline Bot-API send, which used to hard-code a
    # filename title and an empty performer instead.
    assert src.count("_probe_audio_delivery(out, _delivery_name)") == 3


def test_uploader_exposes_a_public_probe_entry_point():
    import utils.userbot_uploader as uploader

    assert inspect.iscoroutinefunction(uploader.probe_audio_metadata)


@pytest.mark.parametrize("name", ["thumb.jpg", "cover.png"])
def test_public_probe_delegates(name, tmp_path, monkeypatch):
    import utils.userbot_uploader as uploader

    src = tmp_path / name
    src.write_bytes(b"x")

    async def _fake(path):
        return {"seen": path}

    monkeypatch.setattr(uploader, "_probe_audio_metadata", _fake)
    assert asyncio.run(uploader.probe_audio_metadata(str(src))) == {"seen": str(src)}


# ─────────────────────────────────────────────────────────────────────────────
# Which copy the worker reads: the bucket before Telegram
# ─────────────────────────────────────────────────────────────────────────────


class _ExistsBackend:
    """A backend that only answers the HEAD the fetch order turns on."""

    def __init__(self, readable):
        self.readable = set(readable)
        self.checked = []

    async def exists(self, key):
        self.checked.append(key)
        return key in self.readable


def _install_exists_backend(monkeypatch, readable=()):
    backend = _ExistsBackend(readable)

    async def _get():
        return backend

    monkeypatch.setattr(ffmpeg_worker, "get_storage_backend", _get)
    return backend


def test_a_readable_stored_object_needs_no_telegram_at_all(monkeypatch):
    backend = _install_exists_backend(monkeypatch, [LIBRARY_KEY])

    assert asyncio.run(ffmpeg_worker._stored_source_available(LIBRARY_KEY)) is True
    assert backend.checked == [LIBRARY_KEY]


def test_a_missing_object_leaves_telegram_as_the_copy_that_is_there(monkeypatch):
    _install_exists_backend(monkeypatch, [])

    assert asyncio.run(ffmpeg_worker._stored_source_available(LIBRARY_KEY)) is False
    # No key to read, and no key to read it with: both mean Telegram.
    assert asyncio.run(ffmpeg_worker._stored_source_available(None)) is False
    assert asyncio.run(ffmpeg_worker._stored_source_available("")) is False


def test_a_backend_that_cannot_answer_does_not_break_the_fetch(monkeypatch):
    """A HEAD that fails must not fail the job - it decides the order, nothing else."""

    async def _boom():
        raise RuntimeError("storage is down")

    monkeypatch.setattr(ffmpeg_worker, "get_storage_backend", _boom)

    assert asyncio.run(ffmpeg_worker._stored_source_available(LIBRARY_KEY)) is False


def test_the_worker_asks_the_bucket_before_it_opens_telegram():
    """The fetch order is the point, so it is asserted on the source itself."""
    body = read_source("workers", "ffmpeg_worker.py")

    assert "_stored_readable = await _stored_source_available(input_key)" in body
    assert "and not _stored_readable" in body
    # Telegram is what the guard above leaves for the case the bucket cannot
    # serve: it may not be reached before the stored copy has been ruled out.
    assert body.index("_stored_source_available(input_key)") < body.index("download_forward_via_userbot(")
    # And the registry is consulted before Telegram too, not only after a failed
    # Telegram fetch: a whole copy another producer wrote is reachable without
    # asking Telegram for the media.
    assert body.index("_stored_source_for_job(job)") < body.index("download_forward_via_userbot(")
