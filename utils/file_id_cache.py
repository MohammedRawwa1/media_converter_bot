"""Telegram file_id caching for repeated media delivery.

When a bot sends the same media file (welcome video, standard photo, etc.)
to multiple users, Telegram returns a unique `file_id` the first time. On
subsequent sends, using that `file_id` instead of re-uploading the file:

1. Saves bandwidth from IDrive/object storage (egress drops to ~zero)
2. Is faster (Telegram serves from their cache)
3. Is free (Telegram caches files on their servers)

This module caches file_ids keyed by the media's content identity (file_unique_id
or a hash of the file content), so the same media sent to different users
reuses the cached file_id.

Key design decisions:
- file_ids are per-bot (a file_id from bot A won't work with bot B)
- file_ids expire eventually (Telegram may invalidate them), so we use a TTL
- The cache is keyed by content hash/file_unique_id, not by filename
- Falls back gracefully when Redis is unavailable

The entry is kept in two tiers, the same way the media registry is: Redis for
latency, and the ``file_id_registry`` MongoDB collection for durability. Redis
is a cache - it can be flushed, evicted, or be unreachable - and losing a
file_id means uploading the very same bytes to Telegram all over again.

Reuse is **self-healing**. Telegram can refuse a token it handed out earlier
(``wrong file identifier``, an expired file reference), so every consumer of a
cached file_id must be ready for it to be rejected: :func:`is_stale_file_id`
says whether a failure was about the token, :func:`invalidate_file_id` drops it,
and the send is retried from the file. Caching a token is only a saving while a
dead one costs a retry rather than a failed delivery.

Usage:
    from utils import file_id_cache

    # After sending media for the first time:
    await file_id_cache.store_file_id(media_type, file_unique_id, file_id, chat_id)

    # Before sending media:
    cached_file_id = await file_id_cache.get_file_id(media_type, file_unique_id)
    if cached_file_id:
        await bot.send_video(chat_id=chat_id, video=cached_file_id, ...)
    else:
        # Upload the file fresh
        ...
"""

from __future__ import annotations

import contextlib
import datetime

_NOW_UTC = datetime.datetime.now(tz=datetime.timezone.utc)
import hashlib
import logging
import os

logger = logging.getLogger(__name__)

# Default TTL for cached file_ids (Telegram may invalidate them, so don't
# cache forever). 24 hours is a reasonable default.
FILE_ID_CACHE_TTL_SECONDS = int(os.getenv("FILE_ID_CACHE_TTL_SECONDS", "86400"))

# Media types we cache file_ids for
SUPPORTED_MEDIA_TYPES = frozenset({"photo", "video", "audio", "document", "voice", "sticker"})


def _cache_prefix(media_type: str) -> str:
    """Redis key prefix for a media type's file_id cache."""
    return f"file_id:{media_type}:"


def _file_id_key(media_type: str, content_identity: str) -> str:
    """Redis key for a specific media's cached file_id."""
    return f"{_cache_prefix(media_type)}{content_identity}"


def registry_key(media_type: str, content_identity: str) -> str:
    """The durable registry's key for the same pair the Redis key encodes."""
    return f"{media_type}:{content_identity}"


async def _get_cache():
    """Get the Redis cache client, or None if unavailable."""
    try:
        from utils.cache import get_cache

        return await get_cache()
    except Exception:
        return None


def registry_enabled() -> bool:
    """Whether the durable MongoDB tier is on (``FILE_ID_REGISTRY_ENABLED=0`` off).

    Only the durable half is switched here; every Redis path behaves exactly as
    it did before, which is what makes it safe to turn off when Mongo is
    unavailable.
    """
    return os.getenv("FILE_ID_REGISTRY_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")


async def _db_model():
    """The bot's MongoDB model, when one has been registered at startup."""
    try:
        from utils.telethon_session import get_db_model

        return get_db_model()
    except Exception:
        return None


async def _durable_get(media_type: str, content_identity: str) -> str | None:
    """Read a file_id from MongoDB - the tier Redis cannot lose."""
    if not registry_enabled():
        return None
    model = await _db_model()
    if model is None or not hasattr(model, "lookup_file_id"):
        return None
    try:
        doc = await model.lookup_file_id(registry_key(media_type, content_identity))
    except Exception:
        logger.debug("file_id_cache: durable lookup failed for %s/%s", media_type, content_identity[:16])
        return None
    if not isinstance(doc, dict):
        return None
    file_id = doc.get("file_id")
    return str(file_id) if file_id else None


async def _durable_set(media_type: str, content_identity: str, file_id: str, ttl: int) -> bool:
    """Mirror a file_id into MongoDB with the caller's own deadline."""
    if not registry_enabled():
        return False
    model = await _db_model()
    if model is None or not hasattr(model, "remember_file_id"):
        return False
    entry = {
        "media_type": media_type,
        "content_identity": content_identity,
        "file_id": file_id,
        # The document's own expiry, so the registry honors the TTL the caller
        # passed instead of a single collection-wide one.
        "expires_at": _NOW_UTC + datetime.timedelta(seconds=max(1, int(ttl))),
    }
    try:
        return bool(await model.remember_file_id(registry_key(media_type, content_identity), entry))
    except Exception:
        logger.debug("file_id_cache: durable store failed for %s/%s", media_type, content_identity[:16])
        return False


async def _durable_del(media_type: str, content_identity: str) -> bool:
    """Drop a file_id from MongoDB (best-effort); True when it is gone."""
    if not registry_enabled():
        return False
    model = await _db_model()
    if model is None or not hasattr(model, "forget_file_id"):
        return False
    try:
        return bool(await model.forget_file_id(registry_key(media_type, content_identity)))
    except Exception:
        logger.debug("file_id_cache: durable invalidate failed for %s/%s", media_type, content_identity[:16])
        return False


# Telegram's wording when it will not accept a token it handed out earlier: the
# file_id was revoked, or the file reference it decodes to has expired. Anything
# else - a flood wait, a network error, a closed file handle - is a reason to
# retry, not a reason to throw away a good token.
_STALE_FILE_ID_MARKERS = (
    "wrong file identifier",
    "file identifier",
    "file_reference",
    "file reference",
    "file not found",
    "invalid file",
    "file is not found",
)


def is_stale_file_id(error) -> bool:
    """Whether *error* means Telegram refused the file_id itself.

    Callers use this to decide between two very different reactions: drop the
    token and upload fresh (it is dead), or leave the cache alone and retry (it
    was never the problem).
    """
    try:
        text = str(error).lower()
    except Exception:
        return False
    return any(marker in text for marker in _STALE_FILE_ID_MARKERS)


def _compute_content_hash(file_path: str | None = None, file_unique_id: str | None = None) -> str | None:
    """Compute a content identity for caching.

    Prefers file_unique_id (Telegram's stable identifier) when available,
    falls back to a SHA256 hash of the file content.
    """
    if file_unique_id:
        return f"uid:{file_unique_id}"

    if file_path and os.path.exists(file_path):
        try:
            hasher = hashlib.sha256()
            with open(file_path, "rb") as f:
                # Only hash the first 64KB for speed; collisions are extremely
                # unlikely for media files and false positives just cause a
                # re-upload (not data corruption).
                for chunk in iter(lambda: f.read(8192), b""):
                    hasher.update(chunk)
                    if hasher.digest_size >= 65536:  # 64KB worth of hashing
                        break
            return f"hash:{hasher.hexdigest()[:32]}"
        except Exception:
            logger.debug("file_id_cache: failed to hash file %s", file_path)

    return None


async def get_file_id(
    media_type: str,
    *,
    file_unique_id: str | None = None,
    file_path: str | None = None,
    content_identity: str | None = None,
) -> str | None:
    """Retrieve a cached file_id for the given media.

    Args:
        media_type: One of 'photo', 'video', 'audio', 'document', 'voice', 'sticker'.
        file_unique_id: Telegram's stable file_unique_id (preferred).
        file_path: Path to the local file (used to compute content hash fallback).
        content_identity: Pre-computed content identity (bypasses computation).

    Returns:
        The cached file_id string, or None if not found/expired.
    """
    if media_type not in SUPPORTED_MEDIA_TYPES:
        logger.debug("file_id_cache: unsupported media type %s", media_type)
        return None

    if not content_identity:
        content_identity = _compute_content_hash(file_path=file_path, file_unique_id=file_unique_id)

    if not content_identity:
        return None

    client = await _get_cache()
    key = _file_id_key(media_type, content_identity)
    if client is None:
        # Redis being unreachable is precisely when the durable tier matters.
        return await _durable_get(media_type, content_identity)

    try:
        cached = await client.get(key)
        if cached:
            logger.debug("file_id_cache: HIT for %s/%s", media_type, content_identity[:16])
            return cached
        logger.debug("file_id_cache: MISS for %s/%s", media_type, content_identity[:16])
    except Exception:
        logger.debug("file_id_cache: failed to read cache for %s", key)

    # A Redis miss only proves Redis forgot. The durable tier is the record of
    # what Telegram already has, and a hit there is re-seeded into Redis so the
    # next delivery is one round trip again.
    durable = await _durable_get(media_type, content_identity)
    if durable:
        logger.info(
            "file_id_cache: durable registry HIT for %s/%s - reseeded the Redis tier",
            media_type,
            content_identity[:16],
        )
        with contextlib.suppress(Exception):
            await client.set(key, durable, ttl=FILE_ID_CACHE_TTL_SECONDS)
    return durable


async def store_file_id(
    media_type: str,
    file_id: str,
    *,
    file_unique_id: str | None = None,
    file_path: str | None = None,
    content_identity: str | None = None,
    ttl: int | None = None,
) -> bool:
    """Store a file_id in the cache for future reuse.

    Args:
        media_type: One of 'photo', 'video', 'audio', 'document', 'voice', 'sticker'.
        file_id: The Telegram file_id to cache.
        file_unique_id: Telegram's stable file_unique_id (preferred).
        file_path: Path to the local file (used to compute content hash fallback).
        content_identity: Pre-computed content identity (bypasses computation).
        ttl: Cache TTL in seconds (defaults to FILE_ID_CACHE_TTL_SECONDS).

    Returns:
        True if stored successfully, False otherwise.
    """
    if media_type not in SUPPORTED_MEDIA_TYPES:
        logger.debug("file_id_cache: unsupported media type %s", media_type)
        return False

    if not content_identity:
        content_identity = _compute_content_hash(file_path=file_path, file_unique_id=file_unique_id)

    if not content_identity:
        logger.debug("file_id_cache: cannot compute content identity for file_id cache")
        return False

    client = await _get_cache()
    key = _file_id_key(media_type, content_identity)
    use_ttl = ttl or FILE_ID_CACHE_TTL_SECONDS

    stored = False
    if client is not None:
        try:
            await client.set(key, file_id, ttl=use_ttl)
            logger.debug("file_id_cache: stored file_id for %s/%s (TTL=%ss)", media_type, content_identity[:16], use_ttl)
            stored = True
        except Exception:
            logger.debug("file_id_cache: failed to store file_id for %s", key)

    # The durable tier is written whether or not Redis answered. If either tier
    # took the token the media is reusable, so that alone counts.
    durable = await _durable_set(media_type, content_identity, file_id, use_ttl)
    return stored or durable


async def invalidate_file_id(
    media_type: str,
    *,
    file_unique_id: str | None = None,
    file_path: str | None = None,
    content_identity: str | None = None,
) -> bool:
    """Invalidate (delete) a cached file_id.

    Useful when you know the file_id is stale or the media has changed.

    Args:
        media_type: One of 'photo', 'video', 'audio', 'document', 'voice', 'sticker'.
        file_unique_id: Telegram's stable file_unique_id (preferred).
        file_path: Path to the local file (used to compute content hash fallback).
        content_identity: Pre-computed content identity (bypasses computation).

    Returns:
        True if deleted successfully, False otherwise.
    """
    if media_type not in SUPPORTED_MEDIA_TYPES:
        return False

    if not content_identity:
        content_identity = _compute_content_hash(file_path=file_path, file_unique_id=file_unique_id)

    if not content_identity:
        return False

    client = await _get_cache()
    key = _file_id_key(media_type, content_identity)
    dropped = False
    if client is not None:
        try:
            await client.delete(key)
            logger.debug("file_id_cache: invalidated %s/%s", media_type, content_identity[:16])
            dropped = True
        except Exception:
            logger.debug("file_id_cache: failed to invalidate %s", key)

    # The durable copy has to go too: leaving it behind would answer the next
    # delivery with the very token that was just refused, so the healing would
    # undo itself on the following send.
    durable = await _durable_del(media_type, content_identity)
    return dropped or durable


async def send_cached_media(
    bot,
    chat_id: int,
    media_type: str,
    send_method: str,
    *,
    file_unique_id: str | None = None,
    file_path: str | None = None,
    content_identity: str | None = None,
    **send_kwargs,
) -> dict:
    """Send media using a cached file_id if available, otherwise upload fresh.

    This is the main convenience function that handles the "check cache, send,
    store result" pattern.

    Args:
        bot: The Telegram Bot instance.
        chat_id: Target chat ID.
        media_type: One of 'photo', 'video', 'audio', 'document', 'voice', 'sticker'.
        send_method: The bot method name to call (e.g., 'send_video', 'send_photo').
        file_unique_id: Telegram's stable file_unique_id from the source message.
        file_path: Path to the local file (for content hash and fresh upload).
        content_identity: Pre-computed content identity.
        **send_kwargs: Additional kwargs passed to the send method.

    Returns:
        A dict with:
        - 'success': bool
        - 'used_cached': bool (True if the cached file_id was used)
        - 'file_id': str | None (the file_id that was used, if available)
        - 'error': str | None (error message if failed)
    """
    result = {"success": False, "used_cached": False, "file_id": None, "error": None}

    if media_type not in SUPPORTED_MEDIA_TYPES:
        result["error"] = f"Unsupported media type: {media_type}"
        return result

    # Try to get cached file_id
    cached_file_id = await get_file_id(
        media_type,
        file_unique_id=file_unique_id,
        file_path=file_path,
        content_identity=content_identity,
    )

    send_method_func = getattr(bot, send_method, None)
    if send_method_func is None:
        result["error"] = f"Unknown send method: {send_method}"
        return result

    try:
        if cached_file_id:
            # Use cached file_id - Telegram serves from their cache
            logger.info("file_id_cache: sending %s using cached file_id for %s", media_type, chat_id)
            send_kwargs["chat_id"] = chat_id

            # For the file parameter, pass the cached file_id directly
            file_param = _get_file_param_name(media_type)
            if file_param:
                send_kwargs[file_param] = cached_file_id

            try:
                message = await send_method_func(**send_kwargs)
            except Exception as cached_exc:
                if not is_stale_file_id(cached_exc):
                    # A flood wait or a network error is not the token's fault:
                    # leave the cache alone and let the caller decide what to do.
                    raise
                logger.info(
                    "file_id_cache: Telegram refused the cached %s file_id (%s); dropping it and uploading fresh",
                    media_type,
                    cached_exc,
                )
                await invalidate_file_id(
                    media_type,
                    file_unique_id=file_unique_id,
                    file_path=file_path,
                    content_identity=content_identity,
                )
                cached_file_id = None
            else:
                result["success"] = True
                result["used_cached"] = True
                result["file_id"] = getattr(message, "video", None) or getattr(message, "photo", None)
                if hasattr(result["file_id"], "file_id"):
                    result["file_id"] = result["file_id"].file_id
                elif hasattr(message, "document") and hasattr(message.document, "file_id"):
                    result["file_id"] = message.document.file_id
                elif hasattr(message, "audio") and hasattr(message.audio, "file_id"):
                    result["file_id"] = message.audio.file_id
                elif hasattr(message, "voice") and hasattr(message.voice, "file_id"):
                    result["file_id"] = message.voice.file_id
        if not cached_file_id:
            # No cache hit - upload the file fresh
            logger.info("file_id_cache: uploading fresh %s for %s", media_type, chat_id)

            # If file_path is provided, open and send it
            if file_path and os.path.exists(file_path):
                send_kwargs["chat_id"] = chat_id
                file_param = _get_file_param_name(media_type)
                if file_param:
                    with open(file_path, "rb") as f:
                        send_kwargs[file_param] = f
                        message = await send_method_func(**send_kwargs)

                # Extract the file_id from the response and cache it
                new_file_id = _extract_file_id(message, media_type)
                if new_file_id:
                    await store_file_id(
                        media_type,
                        new_file_id,
                        file_unique_id=file_unique_id,
                        file_path=file_path,
                        content_identity=content_identity,
                    )
                    result["file_id"] = new_file_id

                result["success"] = True
            else:
                # No file to upload
                result["error"] = f"No file available for {media_type}"
    except Exception as e:
        logger.exception("file_id_cache: send_cached_media failed for %s", chat_id)
        result["error"] = str(e)

    return result


def _get_file_param_name(media_type: str) -> str | None:
    """Get the parameter name for the file argument of each media type's send method."""
    return {
        "photo": "photo",
        "video": "video",
        "audio": "audio",
        "document": "document",
        "voice": "voice",
        "sticker": "sticker",
    }.get(media_type)


def _extract_file_id(message, media_type: str) -> str | None:
    """Extract the file_id from a Telegram message object."""
    try:
        if media_type == "photo":
            # Photo messages have a photo list; the last one is the highest resolution
            if hasattr(message, "photo") and message.photo:
                return message.photo[-1].file_id
        elif media_type == "video":
            if hasattr(message, "video") and message.video:
                return message.video.file_id
        elif media_type == "audio":
            if hasattr(message, "audio") and message.audio:
                return message.audio.file_id
        elif media_type == "document":
            if hasattr(message, "document") and message.document:
                return message.document.file_id
        elif media_type == "voice":
            if hasattr(message, "voice") and message.voice:
                return message.voice.file_id
        elif media_type == "sticker" and hasattr(message, "sticker") and message.sticker:
            return message.sticker.file_id
    except Exception:
        logger.debug("file_id_cache: failed to extract file_id from message")
    return None
