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


def bytes_cache_limit() -> int:
    """Largest media body that is stored in Redis, in bytes."""
    try:
        return int(float(os.getenv("MEDIA_CACHE_BYTES_MAX_MB", str(BYTES_CACHE_MAX_MB))) * 1024 * 1024)
    except Exception:
        return 32 * 1024 * 1024


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


async def lookup(file_unique_id, *, expected_size=None) -> dict | None:
    """Return the cached descriptor for this media, or None on miss/mismatch."""
    key = _entry_key(file_unique_id)
    if not key or not cache_enabled():
        return None
    client = await _cache()
    if client is None:
        return None
    try:
        entry = await client.get_file_info(key)
    except Exception:
        return None
    if not isinstance(entry, dict):
        return None
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
) -> bool:
    """Record where this media already lives so a repeat skips the download."""
    key = _entry_key(file_unique_id)
    if not key or not cache_enabled():
        return False
    try:
        size_int = int(size)
    except (TypeError, ValueError):
        size_int = None

    client = await _cache()
    if client is None:
        return False

    entry = {
        "file_unique_id": key,
        "size": size_int,
        "input_key": input_key,
        "path": path,
        "name": name,
        "storage": storage,
        "cached_at": time.time(),
    }
    stored = False
    try:
        stored = bool(await client.cache_file_info(key, entry, ttl=CACHE_TTL_SECONDS))
    except Exception:
        logger.debug("media_cache: failed to store descriptor for %s", key)

    if data and len(data) <= bytes_cache_limit():
        try:
            stored = bool(await client.cache_file_bytes(key, data)) or stored
        except Exception:
            logger.debug("media_cache: failed to store bytes for %s", key)
    return stored


async def forget(file_unique_id) -> None:
    """Drop both the descriptor and any cached bytes for this media."""
    key = _entry_key(file_unique_id)
    if not key:
        return
    client = await _cache()
    if client is None:
        return
    for redis_key in (f"{PREFIX_FILE}{key}", f"{PREFIX_FILE}bytes:{key}"):
        with contextlib.suppress(Exception):
            await client.delete(redis_key)
