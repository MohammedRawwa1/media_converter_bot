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
import hashlib
import logging
import os
import time

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


async def _get_cache():
    """Get the Redis cache client, or None if unavailable."""
    try:
        from utils.cache import get_cache

        return await get_cache()
    except Exception:
        return None


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
    if client is None:
        return None

    key = _file_id_key(media_type, content_identity)
    try:
        cached = await client.get(key)
        if cached:
            logger.debug("file_id_cache: HIT for %s/%s", media_type, content_identity[:16])
            return cached
        logger.debug("file_id_cache: MISS for %s/%s", media_type, content_identity[:16])
    except Exception:
        logger.debug("file_id_cache: failed to read cache for %s", key)

    return None


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
    if client is None:
        return False

    key = _file_id_key(media_type, content_identity)
    use_ttl = ttl or FILE_ID_CACHE_TTL_SECONDS

    try:
        await client.set(key, file_id, ttl=use_ttl)
        logger.debug("file_id_cache: stored file_id for %s/%s (TTL=%ss)", media_type, content_identity[:16], use_ttl)
        return True
    except Exception:
        logger.debug("file_id_cache: failed to store file_id for %s", key)
        return False


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
    if client is None:
        return False

    key = _file_id_key(media_type, content_identity)
    try:
        await client.delete(key)
        logger.debug("file_id_cache: invalidated %s/%s", media_type, content_identity[:16])
        return True
    except Exception:
        logger.debug("file_id_cache: failed to invalidate %s", key)
        return False


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

            message = await send_method_func(**send_kwargs)
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
        else:
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
        elif media_type == "sticker":
            if hasattr(message, "sticker") and message.sticker:
                return message.sticker.file_id
    except Exception:
        logger.debug("file_id_cache: failed to extract file_id from message")
    return None
