"""The media cache that stops the bot re-downloading a file it already has.

The behaviour that matters is the *validation*: the cache may only ever hand back
the same media, which is why an id match with a different byte size is treated as
a miss and the stale entry is dropped. A fake cache stands in for Redis here, so
these run without any service.
"""

import asyncio
from datetime import UTC, datetime

import pytest
from source_helpers import read_source

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


# ── the durable tier: MongoDB holds what Redis can lose ─────────────────


class FakeRegistryModel:
    """The only two methods media_cache is allowed to call on the Mongo model."""

    def __init__(self):
        self.docs: dict[str, dict] = {}
        self.forgotten: list[str] = []

    async def lookup_media(self, file_unique_id):
        doc = self.docs.get(file_unique_id)
        return dict(doc) if doc else None

    async def remember_media(self, file_unique_id, entry):
        merged = {k: v for k, v in dict(entry).items() if v is not None}
        merged["file_unique_id"] = file_unique_id
        self.docs[file_unique_id] = merged
        return True

    async def forget_media(self, file_unique_id):
        self.docs.pop(file_unique_id, None)
        self.forgotten.append(file_unique_id)
        return True


@pytest.fixture
def durable(monkeypatch):
    """A registered Mongo model: the tier Redis cannot lose."""
    model = FakeRegistryModel()

    async def _db_model():
        return model

    monkeypatch.setattr(media_cache, "_db_model", _db_model)
    monkeypatch.setenv("MEDIA_REGISTRY_ENABLED", "1")
    return model


def test_a_redis_miss_is_not_believed_when_mongo_knows_the_media(fake_cache, durable):
    durable.docs["uid-8"] = {"size": 99, "input_key": "inputs/library/x/source", "storage": "s3"}

    entry = asyncio.run(media_cache.lookup("uid-8", expected_size=99))

    assert entry["input_key"] == "inputs/library/x/source"
    # The hot tier is refilled, so the next lookup is one round trip again.
    assert fake_cache.info[_info_key("uid-8")]["size"] == 99


def test_an_empty_redis_descriptor_is_treated_as_a_miss(fake_cache, durable):
    fake_cache.info[_info_key("uid-11")] = {}
    durable.docs["uid-11"] = {"size": 7, "input_key": "k"}
    assert asyncio.run(media_cache.lookup("uid-11", expected_size=7))["input_key"] == "k"


def test_a_durable_hit_needs_no_redis_at_all(monkeypatch, durable):
    """Redis being unreachable is exactly when the durable tier matters."""

    async def _no_redis():
        return None

    monkeypatch.setattr(media_cache, "_cache", _no_redis)
    durable.docs["uid-9"] = {"size": 5, "input_key": "inputs/library/y/source"}

    entry = asyncio.run(media_cache.lookup("uid-9", expected_size=5))

    assert entry["input_key"] == "inputs/library/y/source"


def test_remember_writes_both_tiers(fake_cache, durable):
    asyncio.run(media_cache.remember("uid-10", size=2048, input_key="inputs/library/z/source", storage="s3"))
    assert durable.docs["uid-10"]["input_key"] == "inputs/library/z/source"
    assert fake_cache.info[_info_key("uid-10")]["size"] == 2048


def test_remember_succeeds_when_only_mongo_took_it(monkeypatch, durable):
    async def _no_redis():
        return None

    monkeypatch.setattr(media_cache, "_cache", _no_redis)

    stored = asyncio.run(media_cache.remember("uid-12", size=64, input_key="k"))

    assert stored is True
    assert durable.docs["uid-12"]["size"] == 64


def test_a_durable_size_mismatch_drops_both_copies(fake_cache, durable):
    durable.docs["uid-13"] = {"size": 5, "input_key": "stale"}

    assert asyncio.run(media_cache.lookup("uid-13", expected_size=6)) is None

    assert "uid-13" not in durable.docs
    assert durable.forgotten == ["uid-13"]


def test_forget_clears_the_durable_copy(fake_cache, durable):
    asyncio.run(media_cache.remember("uid-14", size=4, input_key="k"))
    asyncio.run(media_cache.forget("uid-14"))

    assert "uid-14" not in durable.docs
    assert durable.forgotten == ["uid-14"]


def test_the_registry_can_be_switched_off(fake_cache, durable, monkeypatch):
    monkeypatch.setenv("MEDIA_REGISTRY_ENABLED", "0")
    assert media_cache.registry_enabled() is False
    durable.docs["uid-15"] = {"size": 3, "input_key": "k"}
    # Redis is empty, and the durable tier was told not to answer.
    assert asyncio.run(media_cache.lookup("uid-15", expected_size=3)) is None


def test_the_telegram_token_and_probe_duration_are_kept(fake_cache, durable):
    """The token a file arrived under, and its probed duration, both survive."""
    asyncio.run(
        media_cache.remember(
            "uid-16",
            size=1024,
            input_key="inputs/library/w/source",
            storage="s3",
            file_id="BAACAgQAAxkBAAIQ",
            duration=2994.58,
        )
    )

    assert durable.docs["uid-16"]["file_id"] == "BAACAgQAAxkBAAIQ"
    assert durable.docs["uid-16"]["duration"] == 2994.58
    assert fake_cache.info[_info_key("uid-16")]["duration"] == 2994.58


# ── the model behind the durable tier ───────────────────────────────────


class _FakeUpdateResult:
    matched_count = 1
    modified_count = 1
    deleted_count = 1


class _FakeCursor:
    def __init__(self, docs):
        self._docs = docs
        self._limit = None

    def sort(self, *args):
        return self

    def skip(self, *args):
        return self

    def limit(self, n):
        self._limit = n
        return self

    def allow_disk_use(self):
        return self

    async def to_list(self, length=None):
        return self._docs[: (self._limit or length or len(self._docs))]


class _FakeCollection:
    def __init__(self, name):
        self.name = name
        self.docs: dict[str, dict] = {}
        self.indexes: list[tuple] = []
        self.fail_update = False

    @staticmethod
    def _doc_key(filters) -> str:
        """Whichever key field this collection is addressed by.

        The media registry is keyed by ``file_unique_id`` and the file_id
        registry by ``cache_key``, so the fake has to read the key the caller
        actually used instead of assuming one.
        """
        for field in ("file_unique_id", "cache_key"):
            if field in filters:
                return filters[field]
        raise KeyError(f"no known key field in {filters}")

    @staticmethod
    def _as_bson(value):
        """What the driver hands back for a value that went into BSON.

        A datetime is stored as the instant plus a zone *in the index*, but
        PyMongo decodes it to a **naive** datetime by default. Keeping the aware
        value here would let these tests compare two aware datetimes - a
        comparison the real driver never gets to make, and one that hides a zone
        mismatch rather than exposing it.
        """
        if isinstance(value, datetime) and value.tzinfo is not None:
            return value.astimezone(UTC).replace(tzinfo=None)
        return value

    async def update_one(self, flt, upd, upsert=False):
        if self.fail_update:
            raise RuntimeError("mongo down")
        stored = {field: self._as_bson(value) for field, value in upd["$set"].items()}
        self.docs.setdefault(self._doc_key(flt), {}).update(stored)
        return _FakeUpdateResult()

    def find(self, flt, projection=None):
        doc = self.docs.get(self._doc_key(flt))
        return _FakeCursor([dict(doc)] if doc else [])

    async def delete_one(self, flt):
        self.docs.pop(self._doc_key(flt), None)
        return _FakeUpdateResult()

    async def create_index(self, *args, **kwargs):
        self.indexes.append((args, kwargs))
        return "index"


class _FakeDB:
    def __init__(self):
        self.colls: dict[str, _FakeCollection] = {}

    def __getitem__(self, name):
        return self.colls.setdefault(name, _FakeCollection(name))


class _FakeMongoClient:
    def __init__(self):
        self.dbs: dict[str, _FakeDB] = {}

    def __getitem__(self, name):
        return self.dbs.setdefault(name, _FakeDB())


def _registry_model(monkeypatch):
    from models import MediaConversionModel

    for var in ("BOT_ID", "BOT_USERNAME", "BOT_NAME"):
        monkeypatch.delenv(var, raising=False)
    return MediaConversionModel(_FakeMongoClient(), db_name="test", bot_id=None)


def test_the_registry_upserts_one_document_per_media(monkeypatch):
    model = _registry_model(monkeypatch)

    asyncio.run(model.remember_media("uid-1", {"size": 10, "input_key": "k"}))
    asyncio.run(model.remember_media("uid-1", {"file_id": "BAAC"}))

    doc = asyncio.run(model.lookup_media("uid-1"))
    # The second write adds to the first, it does not replace it...
    assert doc["input_key"] == "k"
    assert doc["file_id"] == "BAAC"
    # ...and neither write may blank a field it does not know about.
    assert doc["size"] == 10
    assert doc["updated_at"] is not None


def test_lookup_strips_the_bson_object_id(monkeypatch):
    """An ObjectId would not survive the trip into the Redis tier."""
    model = _registry_model(monkeypatch)
    model._media_registry_coll.docs["uid-2"] = {"input_key": "k", "_id": object()}

    doc = asyncio.run(model.lookup_media("uid-2"))

    assert "_id" not in doc
    assert doc["input_key"] == "k"


def test_an_unknown_media_is_no_document(monkeypatch):
    model = _registry_model(monkeypatch)
    assert asyncio.run(model.lookup_media("never-seen")) is None
    assert asyncio.run(model.lookup_media("")) is None
    assert asyncio.run(model.remember_media("", {"size": 1})) is False


def test_forget_removes_the_document(monkeypatch):
    model = _registry_model(monkeypatch)
    asyncio.run(model.remember_media("uid-3", {"size": 1}))
    assert asyncio.run(model.forget_media("uid-3")) is True
    assert asyncio.run(model.lookup_media("uid-3")) is None


class _BrokenCollection:
    """A collection whose every operation fails, as an unreachable Mongo does."""

    name = "media_registry"

    def find(self, *args, **kwargs):
        raise RuntimeError("mongo unreachable")

    async def update_one(self, *args, **kwargs):
        raise RuntimeError("mongo unreachable")

    async def delete_one(self, *args, **kwargs):
        raise RuntimeError("mongo unreachable")


def test_a_mongo_failure_is_never_fatal(monkeypatch):
    """The registry only ever saves a download; it must not cost a job."""
    model = _registry_model(monkeypatch)
    model._media_registry_coll.fail_update = True
    assert asyncio.run(model.remember_media("uid-4", {"size": 1})) is False

    model.media_registry.collection = _BrokenCollection()
    assert asyncio.run(model.lookup_media("uid-5")) is None
    assert asyncio.run(model.forget_media("uid-5")) is False


def test_the_registry_is_indexed_and_pruned(monkeypatch):
    model = _registry_model(monkeypatch)
    asyncio.run(model.ensure_indexes())

    indexes = model._media_registry_coll.indexes
    assert (("file_unique_id",), {"unique": True}) in indexes
    ttl = [kwargs for args, kwargs in indexes if args == ("updated_at",)]
    assert ttl and ttl[0]["expireAfterSeconds"] > 0


# ── pipeline wiring ─────────────────────────────────────────────────────


def _handlers_source():
    return read_source("handlers.py")


def test_pipeline_reuses_the_cached_input_and_keeps_it():
    """The big-file path must look up before downloading and not delete a shared input."""
    src = read_source("utils", "bigfile_pipeline.py")

    assert "media_cache.lookup(" in src
    assert "media_cache.remember(" in src
    # Only the local shared file must survive cleanup; an S3 object is never
    # deleted by the worker, so its temp copy must still be removed.
    assert '"cleanup_input": not (_shared_input and self._storage is None)' in src
    assert "media_library_key" in src


def test_pipeline_reuses_a_bare_byte_hit_without_remember():
    """A media the pipeline only saw through the byte tier still skips Pyrogram."""
    src = read_source("utils", "bigfile_pipeline.py")

    assert "_bytes_hit" in src
    # The disk download is the fallback for every path that did not already
    # produce a source: not reused, not served from the byte tier, and not
    # streamed straight into storage.
    assert "if not _reused and not _bytes_hit and not _streamed:" in src


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
