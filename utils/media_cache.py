"""Remember media that already entered the processing pipe.

The bot used to re-download the same video (or audio) from Telegram every time it
was submitted again - once per job, sometimes twice on the large-file path. This
module is the lookup that stops that: it keys on the Telegram ``file_unique_id``
and treats the *byte size* as the validation, so a repeat only reuses the earlier
download when it is provably the same media.

Two tiers, because media sizes are wildly different:

* **bytes** - small files are kept verbatim in Redis (``cache:file:bytes:<uid>``)
  so not even the storage object is needed on the repeat.
* **library input** - large files are stored once under a key derived from the
  media identity (``inputs/library/<hash>/source``) and that key is reused. The
  job that consumes it is marked ``cleanup_input=False`` so the shared object
  survives for the next request.

A size that does not match is a *miss*, and the stale entry is dropped: reusing a
different file that merely shared an id would be worse than re-downloading.

Everything here is best-effort and returns ``None``/``False`` when Redis is
unavailable, so a cache problem costs a download, never a job.

The descriptor is kept twice: Redis for latency, and the ``media_registry``
MongoDB collection for durability. Redis is a cache - it can be flushed,
evicted, or be unreachable - and a bare Redis miss used to mean fetching a
byte-identical file from Telegram again. The Mongo tier is what makes
"already have it" survive a restart.
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import os
import time

from utils.cache import PREFIX_FILE

logger = logging.getLogger(__name__)

# Keys live in storage, so this is the in-Redis descriptor's TTL, not a storage
# lifecycle rule. Pair it with a bucket lifecycle policy for large media.
CACHE_TTL_SECONDS = int(os.getenv("MEDIA_CACHE_TTL_SECONDS", "86400"))
# Bytes are only cached below this size: a multi-gigabyte video does not belong
# in Redis, and for those the shared library key is what saves the re-download.
BYTES_CACHE_MAX_MB = float(os.getenv("MEDIA_CACHE_BYTES_MAX_MB", "32"))
LIBRARY_KEY_PREFIX = "inputs/library/"


def cache_enabled() -> bool:
    """Whether media reuse is on (``MEDIA_CACHE_ENABLED=0`` turns it off)."""
    return os.getenv("MEDIA_CACHE_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")


def registry_enabled() -> bool:
    """Whether the durable MongoDB tier is on (``MEDIA_REGISTRY_ENABLED=0`` off).

    Only the *durable* half is switched here: turning it off leaves every
    Redis path exactly as it was, which is what makes it safe to disable when
    MongoDB is unavailable.
    """
    return os.getenv("MEDIA_REGISTRY_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")


def bytes_cache_limit() -> int:
    """Largest media body that is stored in Redis, in bytes."""
    try:
        return int(float(os.getenv("MEDIA_CACHE_BYTES_MAX_MB", str(BYTES_CACHE_MAX_MB))) * 1024 * 1024)
    except Exception:
        return 32 * 1024 * 1024


def shared_library_key(file_unique_id) -> str | None:
    """The shared storage key for a media identity, or ``None`` when there is none.

    The one place that decides whether a media gets a shared object: every
    producer (the bot's handlers, the fetcher, the Telethon ingest) derives the
    same key from the same ``file_unique_id``, so a file arriving through two
    routes still ends up as **one** object instead of two. Returns ``None`` when
    reuse is switched off, which leaves each caller on its per-job key.
    """
    if not cache_enabled():
        return None
    return media_library_key(file_unique_id)


def media_library_key(file_unique_id) -> str | None:
    """The shared storage key for a media identity, or None when there is none.

    Hashed rather than used raw: ``file_unique_id`` is an opaque Telegram value
    and has no business being a path segment.
    """
    uid = str(file_unique_id or "").strip()
    if not uid:
        return None
    digest = hashlib.sha256(uid.encode("utf-8")).hexdigest()[:32]
    return f"{LIBRARY_KEY_PREFIX}{digest}/source"


def sizes_agree(entry, expected_size) -> bool:
    """Pure size check: True only when the cached size equals the incoming one."""
    if not isinstance(entry, dict):
        return False
    try:
        cached = int(entry.get("size"))
    except (TypeError, ValueError):
        return False
    if expected_size is None:
        return True
    try:
        return cached == int(expected_size)
    except (TypeError, ValueError):
        return False


def _entry_key(file_unique_id) -> str | None:
    uid = str(file_unique_id or "").strip()
    return uid or None


async def _cache():
    try:
        from utils.cache import get_cache

        return await get_cache()
    except Exception:
        return None


async def _db_model():
    """The bot's MongoDB model, when one has been registered at startup.

    Imported lazily and looked up through the registry the same way session
    resolution does it, so the download paths that only hold a ``user_id`` (or
    nothing at all) can still reach the durable tier.
    """
    try:
        from utils.telethon_session import get_db_model

        return get_db_model()
    except Exception:
        return None


async def _durable_lookup(file_unique_id) -> dict | None:
    """Read a descriptor from MongoDB - the tier Redis cannot lose."""
    if not registry_enabled() or not file_unique_id:
        return None
    model = await _db_model()
    if model is None or not hasattr(model, "lookup_media"):
        return None
    try:
        doc = await model.lookup_media(str(file_unique_id))
    except Exception:
        logger.debug("media_cache: durable lookup failed for %s", file_unique_id)
        return None
    if not isinstance(doc, dict):
        return None
    doc.pop("_id", None)
    return doc or None


async def _durable_remember(file_unique_id, entry: dict) -> bool:
    """Mirror a descriptor into MongoDB (best-effort)."""
    if not registry_enabled() or not file_unique_id:
        return False
    model = await _db_model()
    if model is None or not hasattr(model, "remember_media"):
        return False
    try:
        return bool(await model.remember_media(str(file_unique_id), entry))
    except Exception:
        logger.debug("media_cache: durable remember failed for %s", file_unique_id)
        return False


async def _durable_forget(file_unique_id) -> None:
    """Drop a descriptor from MongoDB (best-effort)."""
    if not registry_enabled() or not file_unique_id:
        return
    model = await _db_model()
    if model is None or not hasattr(model, "forget_media"):
        return
    with contextlib.suppress(Exception):
        await model.forget_media(str(file_unique_id))


async def lookup(file_unique_id, *, expected_size=None) -> dict | None:
    """Return the cached descriptor for this media, or None on miss/mismatch."""
    key = _entry_key(file_unique_id)
    if not key or not cache_enabled():
        return None
    client = await _cache()
    entry = None
    if client is not None:
        try:
            entry = await client.get_file_info(key)
        except Exception:
            entry = None
        if not isinstance(entry, dict) or not entry:
            entry = None

    if entry is None:
        # A Redis miss is only evidence that Redis forgot - it may have been
        # flushed, evicted the key, or be unreachable. The durable tier is the
        # record of whether this media is already in storage, so it is asked
        # before the miss is believed, and a hit re-seeds Redis so the next
        # lookup is one round trip again.
        entry = await _durable_lookup(file_unique_id)
        if entry is None:
            return None
        if client is not None:
            with contextlib.suppress(Exception):
                await client.cache_file_info(key, entry, ttl=CACHE_TTL_SECONDS)
        logger.info("media_cache: durable registry HIT for %s - reseeded the Redis tier", key)

    if expected_size is not None and not sizes_agree(entry, expected_size):
        logger.info(
            "media_cache: size mismatch for %s (stored=%s expected=%s) - dropping stale entry",
            key,
            entry.get("size"),
            expected_size,
        )
        await forget(file_unique_id)
        return None
    return entry


async def get_bytes(file_unique_id, *, expected_size=None) -> bytes | None:
    """Return the cached body for a small media, size-validated."""
    key = _entry_key(file_unique_id)
    if not key or not cache_enabled():
        return None
    client = await _cache()
    if client is None:
        return None
    try:
        data = await client.get_cached_file_bytes(key)
    except Exception:
        return None
    if not data:
        return None
    if expected_size is not None and not sizes_agree({"size": len(data)}, expected_size):
        logger.info(
            "media_cache: cached bytes for %s are %d but %s were expected - dropping",
            key,
            len(data),
            expected_size,
        )
        await forget(file_unique_id)
        return None
    return data


async def remember(
    file_unique_id,
    *,
    size,
    input_key=None,
    path=None,
    name=None,
    storage=None,
    data: bytes | None = None,
    file_id=None,
    duration=None,
    header_key=None,
    header_only: bool = False,
    source_meta: dict | None = None,
) -> bool:
    """Record where this media already lives so a repeat skips the download.

    ``input_key`` is a whole object - a source. When only a probe header was
    stored (``PIPELINE_SOURCE_UPLOAD=header``), the object goes in
    ``header_key`` instead and ``header_only`` marks the entry: that is enough
    evidence to prove the media was already ingested (existence + size), while
    never being mistaken for something a job may encode from.

    ``source_meta`` is the ingest's own ffprobe verdict. It travels with the
    descriptor so a repeat that skips the download still has the duration /
    codec / title the caption and the audio tags are built from.
    """
    key = _entry_key(file_unique_id)
    if not key or not cache_enabled():
        return False
    try:
        size_int = int(size)
    except (TypeError, ValueError):
        size_int = None

    client = await _cache()

    entry = {
        "file_unique_id": key,
        "size": size_int,
        "input_key": input_key,
        # A probe header is not a source: it is kept apart from ``input_key``
        # on purpose, so no reuse path can hand it to a job as media.
        "header_key": header_key,
        "header_only": bool(header_only),
        "path": path,
        "name": name,
        "storage": storage,
        # The Telegram token the media arrived under, when the caller has it:
        # reusing it is what lets a later step forward or process the media
        # inside Telegram's ecosystem instead of downloading it to this box
        # again (tokens expire, so this is a hint and every consumer has to be
        # ready for Telegram to refuse it).
        "file_id": file_id,
        "duration": duration,
        "source_meta": dict(source_meta) if isinstance(source_meta, dict) else None,
        "cached_at": time.time(),
    }
    stored = False
    if client is not None:
        try:
            stored = bool(await client.cache_file_info(key, entry, ttl=CACHE_TTL_SECONDS))
        except Exception:
            logger.debug("media_cache: failed to store descriptor for %s", key)

        if data and len(data) <= bytes_cache_limit():
            try:
                stored = bool(await client.cache_file_bytes(key, data)) or stored
            except Exception:
                logger.debug("media_cache: failed to store bytes for %s", key)

    # The durable tier is written whether or not Redis answered: Redis being
    # down is precisely the case it exists for. If *either* tier took the
    # descriptor the media is remembered, so that alone counts.
    durable = await _durable_remember(key, entry)
    return stored or durable


async def forget(file_unique_id) -> None:
    """Drop the descriptor (both tiers) and any cached bytes for this media."""
    key = _entry_key(file_unique_id)
    if not key:
        return
    client = await _cache()
    if client is not None:
        for redis_key in (f"{PREFIX_FILE}{key}", f"{PREFIX_FILE}bytes:{key}"):
            with contextlib.suppress(Exception):
                await client.delete(redis_key)
    # The durable copy has to go too: leaving it behind would answer the next
    # lookup with a source that was deliberately dropped (a stale object, or a
    # media whose size changed).
    await _durable_forget(key)
