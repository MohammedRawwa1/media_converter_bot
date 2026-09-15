"""The media cache that stops the bot re-downloading a file it already has.

The behaviour that matters is the *validation*: the cache may only ever hand back
the same media, which is why an id match with a different byte size is treated as
a miss and the stale entry is dropped. A fake cache stands in for Redis here, so
these run without any service.
"""

import asyncio

import pytest

from utils import media_cache
from utils.cache import PREFIX_FILE


def _info_key(uid):
    return f"{PREFIX_FILE}{uid}"


def _bytes_key(uid):
    return f"{PREFIX_FILE}bytes:{uid}"


class FakeCache:
    """Only the lookups media_cache performs, keyed the way RedisCache keys them.

    The prefixes matter: ``forget`` deletes the *fully qualified* keys, so a fake
    that stored bare ids would silently never delete anything.
    """

    def __init__(self):
        self.info: dict[str, dict] = {}
        self.blobs: dict[str, bytes] = {}
        self.deleted: list[str] = []

    async def get_file_info(self, key):
        return self.info.get(_info_key(key))

    async def cache_file_info(self, key, info, ttl=None):
        self.info[_info_key(key)] = info
        return True

    async def get_cached_file_bytes(self, key):
        return self.blobs.get(_bytes_key(key))

    async def cache_file_bytes(self, key, data, ttl=None):
        self.blobs[_bytes_key(key)] = data
        return True

    async def delete(self, key):
        self.info.pop(key, None)
        self.blobs.pop(key, None)
        self.deleted.append(key)
        return 1


@pytest.fixture
def fake_cache(monkeypatch):
    cache = FakeCache()

    async def _get_cache():
        return cache

    monkeypatch.setattr(media_cache, "_cache", _get_cache)
    monkeypatch.setenv("MEDIA_CACHE_ENABLED", "1")
    return cache


# ── pure helpers ────────────────────────────────────────────────────────


def test_sizes_agree_only_on_an_exact_match():
    assert media_cache.sizes_agree({"size": 100}, 100) is True
    assert media_cache.sizes_agree({"size": "100"}, 100) is True
    assert media_cache.sizes_agree({"size": 100}, 101) is False
    assert media_cache.sizes_agree({}, 100) is False
    assert media_cache.sizes_agree(None, 100) is False


def test_library_key_is_stable_and_safe():
    first = media_cache.media_library_key("AgADsome-id")
    assert first == media_cache.media_library_key("AgADsome-id")
    assert first.startswith(media_cache.LIBRARY_KEY_PREFIX)
    assert "/" in first[len(media_cache.LIBRARY_KEY_PREFIX) :]
    # The raw Telegram id must not leak into the path.
    assert "AgADsome-id" not in first
    assert media_cache.media_library_key("") is None
    assert media_cache.media_library_key(None) is None


# ── remember / lookup ───────────────────────────────────────────────────


def test_remember_then_lookup_round_trips(fake_cache):
    asyncio.run(media_cache.remember("uid-1", size=1024, input_key="inputs/library/x/source", storage="s3"))

    entry = asyncio.run(media_cache.lookup("uid-1", expected_size=1024))
    assert entry["input_key"] == "inputs/library/x/source"
    assert entry["size"] == 1024


def test_lookup_rejects_a_size_mismatch_and_drops_the_entry(fake_cache):
    asyncio.run(media_cache.remember("uid-2", size=1024, input_key="k", storage="s3"))

    assert asyncio.run(media_cache.lookup("uid-2", expected_size=2048)) is None
    # The stale descriptor must not survive to mislead the next request.
    assert fake_cache.info == {}


def test_lookup_without_an_expected_size_accepts_the_entry(fake_cache):
    asyncio.run(media_cache.remember("uid-3", size=10, input_key="k"))
    assert asyncio.run(media_cache.lookup("uid-3")) is not None


def test_lookup_is_a_miss_for_an_unknown_media(fake_cache):
    assert asyncio.run(media_cache.lookup("never-seen", expected_size=1)) is None


def test_cache_disabled_short_circuits(monkeypatch, fake_cache):
    monkeypatch.setenv("MEDIA_CACHE_ENABLED", "0")
    assert media_cache.cache_enabled() is False
    assert asyncio.run(media_cache.lookup("uid", expected_size=1)) is None
    assert asyncio.run(media_cache.remember("uid", size=1, input_key="k")) is False


# ── bytes tier ──────────────────────────────────────────────────────────


def test_remember_stores_small_bodies(fake_cache, monkeypatch):
    monkeypatch.setattr(media_cache, "bytes_cache_limit", lambda: 8)
    asyncio.run(media_cache.remember("uid-4", size=4, data=b"data"))

    assert fake_cache.blobs[_bytes_key("uid-4")] == b"data"
    assert asyncio.run(media_cache.get_bytes("uid-4", expected_size=4)) == b"data"


def test_remember_skips_bodies_over_the_limit(fake_cache, monkeypatch):
    monkeypatch.setattr(media_cache, "bytes_cache_limit", lambda: 2)
    asyncio.run(media_cache.remember("uid-5", size=100, data=b"way-too-big"))

    assert _bytes_key("uid-5") not in fake_cache.blobs
    assert asyncio.run(media_cache.get_bytes("uid-5")) is None


def test_get_bytes_rejects_a_body_of_the_wrong_size(fake_cache):
    fake_cache.blobs[_bytes_key("uid-6")] = b"twelve bytes"
    assert asyncio.run(media_cache.get_bytes("uid-6", expected_size=999)) is None
    # A mismatched body is dropped so it cannot be reused by mistake.
    assert _bytes_key("uid-6") not in fake_cache.blobs


def test_forget_removes_both_tiers(fake_cache):
    asyncio.run(media_cache.remember("uid-7", size=4, input_key="k", data=b"data"))

    asyncio.run(media_cache.forget("uid-7"))

    assert _info_key("uid-7") not in fake_cache.info
    assert _bytes_key("uid-7") not in fake_cache.blobs


def test_lookup_never_raises_without_redis(monkeypatch):
    """get_cache() blowing up must read as a miss, not an exception."""
    import utils.cache as cache_module

    async def _boom():
        raise RuntimeError("no redis")

    monkeypatch.setattr(cache_module, "get_cache", _boom)

    assert asyncio.run(media_cache.lookup("uid", expected_size=1)) is None
    assert asyncio.run(media_cache.remember("uid", size=1)) is False


# ── pipeline wiring ─────────────────────────────────────────────────────


def _handlers_source():
    import os

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "handlers.py"), encoding="utf-8") as fh:
        return fh.read()


def test_pipeline_reuses_the_cached_input_and_keeps_it():
    """The big-file path must look up before downloading and not delete a shared input."""
    import os

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "utils", "bigfile_pipeline.py"), encoding="utf-8") as fh:
        src = fh.read()

    assert "media_cache.lookup(" in src
    assert "media_cache.remember(" in src
    # Only the local shared file must survive cleanup; an S3 object is never
    # deleted by the worker, so its temp copy must still be removed.
    assert '"cleanup_input": not (_shared_input and self._storage is None)' in src
    assert "media_library_key" in src


def test_pipeline_reuses_a_bare_byte_hit_without_remember():
    """A media the pipeline only saw through the byte tier still skips Pyrogram."""
    import os

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "utils", "bigfile_pipeline.py"), encoding="utf-8") as fh:
        src = fh.read()

    assert "_bytes_hit" in src
    assert "if not _reused and not _bytes_hit:" in src


def test_bot_api_path_reuses_every_tier():
    """The bot-API download must consult the descriptor, not only the byte tier."""
    src = _handlers_source()

    assert "_media_cache.lookup(_uid, expected_size=_expected)" in src
    # 1) remote key, 2) local path, 3) raw bytes
    assert 'current_file["input_key"] = _stored_key' in src
    assert 'current_file["path"] = _stored_path' in src
    assert "_media_cache.get_bytes(_uid, expected_size=_expected)" in src
    # The local fallback records where the file is, not just its bytes, so a
    # file too large for the byte tier can still be reused.
    assert "path=file_path," in src


def test_photo_path_uses_the_cache_too():
    """Photos are media as well: a repeat must not re-fetch them from Telegram."""
    src = _handlers_source()

    assert "_photo_reused" in src
    assert "_media_cache.lookup(_photo_uid, expected_size=_photo_size)" in src
    assert "if not _photo_reused:" in src
    assert "path=photo_path," in src


def test_remote_s3_streaming_path_uses_the_shared_library_key():
    """The S3/R2 streaming branch must reuse and remember under the library key."""
    src = _handlers_source()

    assert "media_library_key(_cache_uid)" in src
    assert 'f"inputs/{_job_id}/source{ext}"' in src
    assert '_input_key = _library_key or f"inputs/{_job_id}/source{ext}"' in src
    assert "_media_cache.remember(" in src
    assert 'storage="s3",' in src


def test_both_download_pipes_share_one_key_scheme():
    """bot-API and userbot must derive the same library key for one media."""
    uid = "AgADshared-id"
    key = media_cache.media_library_key(uid)

    assert key is not None
    assert key.startswith("inputs/library/")
    # Same input for the same identity is what makes a cross-pipe hit possible.
    assert media_cache.media_library_key(uid) == key
