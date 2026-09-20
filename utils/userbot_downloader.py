import asyncio
import contextlib
import io
import json
import logging
import os
import shutil
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime

try:
    import config
except Exception:

    class _FallbackConfig:
        RELAY_CHAT_ID = ""

    config = _FallbackConfig()

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except Exception:  # pragma: no cover - optional dependency
    TelegramClient = None
    StringSession = None

try:
    from pyrogram import Client as PyrogramClient
except Exception:  # pragma: no cover - optional dependency
    PyrogramClient = None

logger = logging.getLogger(__name__)


def _safe_path_token(value) -> str:
    """Coerce a value into a filesystem-safe single path token.

    Used when embedding chat/message ids into temp filenames, so an
    attacker-influenced string can never introduce path separators.
    """
    if value is None:
        return "none"
    import re as _re

    token = _re.sub(r"[^A-Za-z0-9_-]", "_", str(value))
    return token[:80] or "none"


# Module-level default timeouts for download operations.
# TELETHON_DOWNLOAD_TIMEOUT/PYROGRAM_DOWNLOAD_TIMEOUT are legacy wall-clock caps
# kept for compatibility; downloads are now governed by STALL detection below -
# a slow-but-flowing transfer is alive, a silent one is dead.
TELETHON_DOWNLOAD_TIMEOUT = int(os.getenv("TELETHON_DOWNLOAD_TIMEOUT", "600"))
PYROGRAM_DOWNLOAD_TIMEOUT = int(os.getenv("PYROGRAM_DOWNLOAD_TIMEOUT", "600"))

# A download is only timed out when NO bytes have arrived for this long. It has
# to comfortably exceed Telegram's flood sleeps (PYROGRAM_SLEEP_THRESHOLD=30s:
# pyrogram dozes inside the RPC without progress callbacks firing), so 120s.
# The old fixed 600s wall clock killed flood-throttled big files and restarted
# them from byte zero - the exact "second video crawls" symptom.
DOWNLOAD_STALL_SECONDS = float(os.getenv("DOWNLOAD_STALL_SECONDS", "120"))
# Absolute ceiling for one download attempt (bytes may trickle forever without
# ever stalling). Generous: a 2GB file at 200KB/s takes ~2.8h.
DOWNLOAD_HARD_SECONDS = float(os.getenv("DOWNLOAD_HARD_SECONDS", "7200"))
_STALL_POLL_SECONDS = 10.0


class _ProgressWatch:
    """Track a download's last-byte time and wrap progress callbacks.

    The wrapper forwards to the caller's callback untouched - including its
    async-ness, which Pyrogram checks with iscoroutinefunction on the callback
    it is handed, so an async user callback must stay async.
    """

    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.started = self.loop.time()
        self.last_activity = self.started

    def wrap(self, user_progress):
        if user_progress is not None and asyncio.iscoroutinefunction(user_progress):

            async def _progress(current, total, *args):
                self.last_activity = self.loop.time()
                return await user_progress(current, total, *args)
        else:

            def _progress(current, total, *args):
                self.last_activity = self.loop.time()
                if user_progress is not None:
                    return user_progress(current, total, *args)

        return _progress


async def _wait_download_or_stall(dl_task, watch) -> object:
    """Await a download task, raising TimeoutError only when it has stalled.

    Progress callbacks keep ``watch.last_activity`` fresh, so any transfer that
    is still receiving bytes runs to completion no matter how slow Telegram
    serves it. Only DOWNLOAD_STALL_SECONDS of total silence (or the hard cap)
    cancels the task and raises - the caller's retry loop then treats it like
    the old wall-clock timeout.
    """
    hard_deadline = watch.loop.time() + DOWNLOAD_HARD_SECONDS if DOWNLOAD_HARD_SECONDS > 0 else None
    while True:
        try:
            # shield: an expired poll must not cancel the still-running download
            return await asyncio.wait_for(asyncio.shield(dl_task), timeout=_STALL_POLL_SECONDS)
        except TimeoutError:
            now = watch.loop.time()
            if dl_task.done():
                return dl_task.result()
            if hard_deadline is not None and now >= hard_deadline:
                dl_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await dl_task
                raise
            if now - watch.last_activity >= DOWNLOAD_STALL_SECONDS:
                dl_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await dl_task
                raise


def _get_bot_user_id() -> int | None:
    """Extract the bot's user ID from the BOT_TOKEN environment variable.

    When the Bot API reports chat_id == user_id (i.e. the user's ID in a DM),
    MTProto clients (Telethon/Pyrogram) need the **bot's user ID** to access
    those same messages from the bot's chat.  This helper extracts the bot's
    numeric ID from the first segment of the BOT_TOKEN.
    """
    token = os.getenv("BOT_TOKEN", "")
    if ":" in token:
        try:
            return int(token.split(":")[0])
        except (ValueError, IndexError):
            pass
    return None


def _is_user_dm_chat(chat_id: int | str) -> bool:
    """Return True if chat_id looks like a user-to-bot DM chat.

    In the Bot API, DMs use the user's Telegram ID as the chat_id,
    which is always a positive integer.  Negative IDs are groups/channels.
    """
    try:
        cid = int(chat_id)
        return cid > 0
    except (TypeError, ValueError):
        return False


# Media types Pyrogram's download_media() accepts. Everything else Telegram can
# attach to a message - a poll, a location, a link preview - is reported by
# ``msg.media`` too, but downloading it raises ValueError.
_DOWNLOADABLE_MEDIA_NAMES = frozenset(
    {"PHOTO", "VIDEO", "AUDIO", "DOCUMENT", "ANIMATION", "VOICE", "VIDEO_NOTE", "STICKER"}
)


def _has_downloadable_media(msg) -> bool:
    """Whether *msg* carries media Pyrogram can actually fetch.

    ``msg.media`` is truthy for anything Telegram attaches, so testing it alone
    picks up messages that only carry a link preview / poll / location. Downloading
    one of those raises "This message doesn't contain any downloadable media",
    which - in the candidate loops - used to be read as "the download failed"
    and stopped the search before the peer that actually holds the file was
    tried. Ask for a document or a photo instead.
    """
    if msg is None or getattr(msg, "empty", False):
        return False

    media = getattr(msg, "media", None)
    if not media:
        return False

    try:
        from pyrogram.enums import MessageMediaType
    except Exception:  # pragma: no cover - pyrogram missing or older layout
        MessageMediaType = None

    if MessageMediaType is not None and isinstance(media, MessageMediaType):
        return getattr(media, "name", "").upper() in _DOWNLOADABLE_MEDIA_NAMES

    # Not the parsed enum: fall back to the payload download_media() consumes.
    return bool(getattr(msg, "document", None) or getattr(msg, "photo", None))


async def _resolve_pyrogram_peer(client, peer_id: int | str) -> int:
    """Resolve a peer ID to get Pyrogram's cached entity (with access_hash).

    Pyrogram needs the access_hash for a peer before it can call get_messages().
    For user IDs the userbot has never interacted with, Pyrogram raises
    [400 PEER_ID_INVALID] because it lacks the hash.  This function resolves
    the peer via get_chat() / get_users(), which fetches and caches the hash.
    """
    if not isinstance(peer_id, int):
        return peer_id

    # Try get_chat first (covers groups, channels, and users)
    try:
        resolved = await client.get_chat(peer_id)
        if resolved is not None:
            cached_id = getattr(resolved, "id", None)
            if cached_id is not None:
                logger.debug(
                    "userbot: resolve_pyrogram_peer: get_chat(%s) -> id=%s type=%s",
                    peer_id,
                    cached_id,
                    getattr(resolved, "_", type(resolved).__name__),
                )
                return cached_id
    except Exception as e:
        logger.debug(
            "userbot: resolve_pyrogram_peer: get_chat(%s) failed: %s",
            peer_id,
            e,
        )

    # Fall back to get_users (only works for users, not groups/channels)
    try:
        resolved = await client.get_users(peer_id)
        if resolved is not None:
            cached_id = getattr(resolved, "id", None)
            if cached_id is not None:
                logger.debug(
                    "userbot: resolve_pyrogram_peer: get_users(%s) -> id=%s",
                    peer_id,
                    cached_id,
                )
                return cached_id
    except Exception as e:
        logger.debug(
            "userbot: resolve_pyrogram_peer: get_users(%s) failed: %s",
            peer_id,
            e,
        )

    # Final fallback: scan recent dialogs for the peer
    try:
        async for dialog in client.get_dialogs(limit=200):
            chat = getattr(dialog, "chat", None)
            if chat and getattr(chat, "id", None) == peer_id:
                cached_id = getattr(chat, "id", None)
                logger.info(
                    "userbot: resolve_pyrogram_peer: resolved %s via dialog scan -> id=%s type=%s",
                    peer_id,
                    cached_id,
                    type(chat).__name__,
                )
                return cached_id
    except Exception as e:
        logger.debug(
            "userbot: resolve_pyrogram_peer: dialog scan for %s failed: %s",
            peer_id,
            e,
        )

    logger.info(
        "userbot: resolve_pyrogram_peer: could not resolve %s, will try as-is",
        peer_id,
    )
    return peer_id


async def _resolve_telethon_entity(client, chat_id: int | str):
    """Resolve a chat/peer entity for Telethon with multiple fallback strategies.

    Telethon needs a cached entity (from ``get_entity`` or dialog iteration)
    to download messages from a chat. This function tries several approaches:
    1. Direct ``client.get_entity()`` with the original ID
    2. For channel IDs, try with raw API (GetChannelsRequest)
    3. Iterate through recent dialogs and match by ID

    Args:
        client: An active Telethon client.
        chat_id: Numeric chat ID or @username.

    Returns:
        Resolved entity on success, or None on failure.
    """
    if isinstance(chat_id, str) and chat_id.startswith("@"):
        try:
            return await client.get_entity(chat_id)
        except Exception as e:
            logger.debug("userbot: get_entity(@) failed for %s: %s", chat_id, e)
            return None

    # Strategy 1: Try direct get_entity with the raw ID
    try:
        return await client.get_entity(chat_id)
    except ValueError as e:
        err_str = str(e)
        if "Could not find the input entity" in err_str:
            logger.debug(
                "userbot: get_entity(%s) entity not found, trying alternative strategies",
                chat_id,
            )
        else:
            logger.debug("userbot: get_entity(%s) failed: %s", chat_id, e)
    except Exception as e:
        logger.debug("userbot: get_entity(%s) failed: %s", chat_id, e)

    # Strategy 2: For Bot API channel IDs (e.g. -100xxxxxxxxx), try resolving
    # by constructing the canonical peer and using raw API
    if isinstance(chat_id, int) and chat_id < 0:
        s = str(chat_id)
        if s.startswith("-100"):
            raw_id = abs(chat_id) - 1000000000000
            try:
                from telethon import types as t_types
                from telethon.tl.functions.channels import GetChannelsRequest

                peer = t_types.InputChannel(channel_id=raw_id, access_hash=0)
                result = await client(GetChannelsRequest(id=[peer]))
                if result and result.chats:
                    entity = result.chats[0]
                    logger.info(
                        "userbot: resolved channel via raw API: %s (id=%s)",
                        type(entity).__name__,
                        getattr(entity, "id", None),
                    )
                    return entity
            except Exception as e2:
                logger.debug(
                    "userbot: raw channel resolution failed for %s: %s",
                    chat_id,
                    e2,
                )

    # Strategy 3: Scan recent dialogs for a matching entity
    try:
        async for dialog in client.iter_dialogs(limit=200):
            if dialog and dialog.entity:
                eid = getattr(dialog.entity, "id", None)
                if eid and eid == abs(chat_id):
                    logger.info(
                        "userbot: resolved entity via dialog scan: %s (id=%s)",
                        type(dialog.entity).__name__,
                        eid,
                    )
                    return dialog.entity
    except Exception as e3:
        logger.debug("userbot: dialog scan failed: %s", e3)

    logger.warning("userbot: could not resolve entity for chat_id=%s", chat_id)
    return None


async def _normalize_target(chat_id: int | str, client=None):
    """Return a compatible target entity for `chat_id`."""
    if isinstance(chat_id, str) and chat_id.startswith("@"):
        return chat_id
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return chat_id


# How far a downloaded file's size may differ from the one Telegram announced for
# the requested media before it is treated as a *different* file. Telegram rounds
# sizes in some message shapes, so equality alone would refuse the right file; 1%
# or 4 KB is far tighter than the gap between two unrelated media (a 1600x800
# photo against a 49 MB module, in the case this exists for).
_MEDIA_SIZE_TOLERANCE = 0.01


def _message_media_size(message) -> int | None:
    """Bytes Telegram says a message's media is, or ``None`` when it has no file.

    ``None`` is the answer for a photo, a poll, a location - anything that is not
    a downloadable file - and for a message with no media at all.
    """
    for field in ("document", "audio", "video", "voice", "video_note", "animation", "sticker"):
        media = getattr(message, field, None)
        size = getattr(media, "size", None) if media is not None else None
        if size:
            with contextlib.suppress(TypeError, ValueError):
                return int(size)
    return None


def _message_has_audio(message) -> bool:
    """Whether a message's media actually carries an audio stream."""
    if getattr(message, "audio", None) is not None or getattr(message, "voice", None) is not None:
        return True
    document = getattr(message, "document", None)
    for attribute in getattr(document, "attributes", None) or ():
        if attribute.__class__.__name__ == "DocumentAttributeAudio" or hasattr(attribute, "voice"):
            return True
    return False


def _size_matches(size, expected_size) -> bool:
    """Whether a candidate's size can be the requested media's size."""
    if size is None or not expected_size:
        return True
    with contextlib.suppress(TypeError, ValueError):
        expected = int(expected_size)
        return abs(int(size) - expected) <= max(4096, int(expected * _MEDIA_SIZE_TOLERANCE))
    return True


def _scan_candidate_matches(message, *, expected_size=None, want_audio: bool = False) -> bool:
    """Whether a message found by a *scan* can be the media that was asked for.

    The date and recent-history scans do not look at the requested message: they
    walk the chat and take the first thing with media near the given instant. Near
    a forwarded audio that can perfectly well be a photo the user sent in the same
    minute - and a photo downloaded into the audio's own path satisfied every check
    the old code had (a file exists; ffprobe can read it), so the fallback returned
    success, the chat got "✅ Download complete!", and the encode that followed
    failed on a source with no audio track at all.

    A scan is therefore only allowed to take a candidate that is a *file* (never a
    photo), whose announced size is the one the caller expected, and - when an
    audio was requested - that actually carries audio. Everything else is skipped
    so the scan can carry on to the message that really holds the media.
    """
    size = _message_media_size(message)
    if size is None:
        return False
    if not _size_matches(size, expected_size):
        return False
    return not want_audio or _message_has_audio(message)


async def _ffprobe_ok(path: str) -> bool:
    """Run ffprobe (in a thread) to verify the media file appears valid."""
    cmd = [os.getenv("FFPROBE_PATH", "ffprobe"), "-v", "error", "-show_entries", "format=size", "-of", "json", path]
    try:
        proc = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, timeout=15)
    except Exception:
        return False
    if proc.returncode != 0:
        return False
    try:
        out = json.loads(proc.stdout)
        if out and out.get("format") and out["format"].get("size"):
            try:
                size = int(out["format"]["size"])
                return size > 0
            except Exception:
                return False
    except Exception:
        return False
    return False


# ---------------------------------------------------------------------------
# Helper: read DOWNLOAD_CHUNK_SIZE_KB env var (default 256 KB)
# ---------------------------------------------------------------------------
def get_download_chunk_size_kb() -> int:
    """Return the configured download chunk size in KB.

    Reads the ``DOWNLOAD_CHUNK_SIZE_KB`` env var (default 256).
    Smaller values (e.g. 64) reduce per-request data and may help
    avoid ``-503 Timeout`` errors on unreliable networks; larger
    values (e.g. 512 or 1024) improve throughput on stable connections.

    For Telethon this is passed directly as ``part_size_kb`` to
    ``download_media()``.  For Pyrogram a raw-MTProto chunked download
    is used when the env var is set (see ``_download_bytes_via_raw_api``).
    """
    try:
        return int(os.getenv("DOWNLOAD_CHUNK_SIZE_KB", "256"))
    except (TypeError, ValueError):
        return 256


# ---------------------------------------------------------------------------
# Helper: reconcile Telethon/Pyrogram download path to expected destination
# ---------------------------------------------------------------------------
def _reconcile_download_path(dl_result, dest_path: str) -> None:
    """Move file from Telethon/Pyrogram's actual save path to the expected dest_path.

    Both Telethon and Pyrogram may save to a path different from *dest_path*
    (e.g. by appending a file extension or resolving a relative path against
    an internal working directory).  This helper reconciles the two so the
    caller can check ``dest_path`` directly.

    Args:
        dl_result: The return value of ``client.download_media()``, or None.
        dest_path: The path the caller expected the file to be saved at.
    """
    if dl_result is None:
        return
    _dl_path = str(dl_result)
    _abs_dest = os.path.abspath(dest_path)
    if _dl_path != _abs_dest and not os.path.exists(dest_path):
        if os.path.exists(_dl_path):
            logger.info(
                "userbot: reconciling download path %s -> %s",
                _dl_path,
                _abs_dest,
            )
            shutil.move(_dl_path, _abs_dest)
        else:
            logger.warning(
                "userbot: download_media returned %s but file does not exist",
                _dl_path,
            )


# ---------------------------------------------------------------------------
# Helper: detect -503 Timeout / InternalServerError from any MTProto client
# ---------------------------------------------------------------------------
def _is_503_timeout(exc: Exception) -> bool:
    """Return True when *exc* is a Telegram -503 (internal timeout) error."""
    err_str = str(exc)
    if "503" in err_str and "Timeout" in err_str:
        return True
    if "-503" in err_str:
        return True
    return "InternalServerError" in type(exc).__name__


# ---------------------------------------------------------------------------
# Retry wrapper: calls a download coroutine with exponential backoff on -503
# ---------------------------------------------------------------------------
async def _download_media_with_retry(
    client,
    msg,
    max_retries: int = 5,
    **dl_kwargs,
):
    """Call ``client.download_media(msg, **dl_kwargs)`` retrying on
    ``-503 Timeout`` with exponential backoff.

    Retry delays: 5s, 15s, 45s, 120s, 300s (capped at 300s).

    Returns the download_media result on success.
    Raises the last exception if all retries are exhausted.
    """
    delays = [5, 15, 45, 120, 300]

    for attempt in range(max_retries):
        # Stall-aware wait: inject our counting progress callback (forwarding to
        # the caller's, if any) so silence and slowness are distinguishable.
        _watch = _ProgressWatch()
        _kwargs = dict(dl_kwargs)
        _kwargs["progress"] = _watch.wrap(_kwargs.get("progress"))
        try:
            return await _wait_download_or_stall(
                asyncio.create_task(client.download_media(msg, **_kwargs)),
                _watch,
            )
        except TimeoutError:
            logger.warning(
                "userbot: download_media attempt %d/%d stalled (%ds without bytes, elapsed %ds), retrying",
                attempt + 1,
                max_retries,
                int(DOWNLOAD_STALL_SECONDS),
                int(_watch.loop.time() - _watch.started),
            )
            if attempt < max_retries - 1:
                wait = delays[min(attempt, len(delays) - 1)]
                await asyncio.sleep(wait)
                continue
            raise
        except Exception as exc:
            if not _is_503_timeout(exc):
                raise  # non-timeout error, propagate immediately

            if attempt >= max_retries - 1:
                logger.warning(
                    "userbot: download_media exhausted %d retries (-503): %s",
                    max_retries,
                    exc,
                )
                raise  # last retry exhausted

            wait = delays[min(attempt, len(delays) - 1)]
            logger.warning(
                "userbot: download_media attempt %d/%d failed (-503), retrying in %ds: %s",
                attempt + 1,
                max_retries,
                wait,
                exc,
            )
            await asyncio.sleep(wait)


# ---------------------------------------------------------------------------
# Raw-MTProto chunked download for Pyrogram (supports configurable chunk size)
# ---------------------------------------------------------------------------
async def _get_raw_file_location(msg):
    """Extract the file ``InputFileLocation`` and total size from a Pyrogram
    message's media, so we can call ``upload.GetFile`` directly.

    Returns ``(location, total_size)`` or ``(None, 0)`` if the message
    does not carry downloadable media (video, audio, photo, document).
    """
    from pyrogram import raw

    media = getattr(msg, "media", None)
    if media is None:
        return None, 0

    # Document media (video, audio, document files)
    doc = getattr(media, "document", None)
    if doc is not None:
        try:
            loc = raw.types.InputDocumentFileLocation(
                id=doc.id,
                access_hash=doc.access_hash,
                file_reference=doc.file_reference,
                thumb_size="",  # empty = full file
            )
            return loc, doc.size
        except Exception:
            logger.debug("userbot: failed to get raw file location for document")

    # Photo media
    photo = getattr(media, "photo", None)
    if photo is not None:
        try:
            thumb_size = "m"
            sizes = getattr(photo, "sizes", [])
            if sizes:
                thumb_size = getattr(sizes[-1], "type", "m")
            loc = raw.types.InputPhotoFileLocation(
                id=photo.id,
                access_hash=photo.access_hash,
                file_reference=photo.file_reference,
                thumb_size=thumb_size,
            )
            return loc, 0  # photo size not known upfront
        except Exception:
            logger.debug("userbot: failed to get raw file location for photo")

    return None, 0


async def _download_bytes_via_raw_api(
    client,
    msg,
    chunk_size_kb: int | None = None,
    progress_callback=None,
) -> bytes | None:
    """Download media bytes using raw ``upload.GetFile`` with configurable
    chunk size and per-chunk retry with exponential backoff.

    Each chunk is retried independently so a ``-503`` mid-download does
    not lose already-transferred data.

    Args:
        client: An active Pyrogram client.
        msg: A Pyrogram ``Message`` with media.
        chunk_size_kb: Chunk size in KB (default from env var or 256).
        progress_callback: Optional ``(current, total)`` callback.

    Returns:
        Complete file bytes, or ``None`` on failure.
    """
    from pyrogram import raw

    if chunk_size_kb is None:
        chunk_size_kb = get_download_chunk_size_kb()
    chunk_size = chunk_size_kb * 1024

    location, total_size = await _get_raw_file_location(msg)
    if location is None:
        logger.warning("userbot: raw API download skipped (no extractable file location)")
        return None

    logger.info(
        "userbot: raw API chunked download starting (chunk_size=%dKB, total_size=%d)",
        chunk_size_kb,
        total_size,
    )

    chunks = []
    offset = 0
    max_chunk_retries = 5
    chunk_delays = [1, 3, 10, 30, 60]

    while True:
        chunk_data = None
        for chunk_attempt in range(max_chunk_retries):
            try:
                result = await client.invoke(
                    raw.functions.upload.GetFile(
                        location=location,
                        offset=offset,
                        limit=chunk_size,
                    )
                )
                # result.bytes carries the chunk payload
                chunk_data = result.bytes if hasattr(result, "bytes") else None
                break
            except Exception as exc:
                if not _is_503_timeout(exc):
                    logger.warning(
                        "userbot: raw API chunk at offset %d failed with non-retryable error: %s",
                        offset,
                        exc,
                    )
                    return None  # non-retryable, bail out

                if chunk_attempt >= max_chunk_retries - 1:
                    logger.warning(
                        "userbot: raw API chunk at offset %d exhausted %d retries (-503): %s",
                        offset,
                        max_chunk_retries,
                        exc,
                    )
                    return None

                wait = chunk_delays[min(chunk_attempt, len(chunk_delays) - 1)]
                logger.warning(
                    "userbot: raw API chunk at offset %d attempt %d/%d, retrying in %ds",
                    offset,
                    chunk_attempt + 1,
                    max_chunk_retries,
                    wait,
                )
                await asyncio.sleep(wait)

        if chunk_data is None:
            break

        chunks.append(chunk_data)
        offset += len(chunk_data)

        if progress_callback and total_size > 0:
            with contextlib.suppress(Exception):
                progress_callback(offset, total_size)

        # If we received less than the requested limit, it's the last chunk
        if len(chunk_data) < chunk_size:
            break

    if not chunks:
        logger.warning("userbot: raw API download returned no chunks")
        return None

    data = b"".join(chunks)
    logger.info(
        "userbot: raw API download complete: %d bytes in %d chunks (chunk_size=%dKB)",
        len(data),
        len(chunks),
        chunk_size_kb,
    )
    return data


# ---------------------------------------------------------------------------
# Recovery helpers: forward to Saved Messages, 1-byte probe, session recycle
# ---------------------------------------------------------------------------


async def _forward_to_saved_messages(client, chat_id, message_id: int, client_type: str = "pyrogram") -> int | None:
    """Forward a message to Saved Messages to get a fresh ``file_reference``.

    Telegram assigns a new ``file_reference`` to the forwarded copy, which
    may route to a different (healthy) storage node and bypass persistent
    ``-503 Timeout`` errors on the original.

    Handles both Telethon and Pyrogram forward_messages API differences:
    - Pyrogram: forward_messages(chat_id=..., from_chat_id=..., message_ids=[...])
    - Telethon: forward_messages(entity, messages=[...], from_peer=...)

    Returns the forwarded message ID, or ``None`` on failure.
    """
    try:
        me = await client.get_me()
        if client_type == "telethon":
            forwarded = await client.forward_messages(
                me.id,
                messages=[message_id],
                from_peer=chat_id,
            )
        else:
            forwarded = await client.forward_messages(
                chat_id=me.id,
                from_chat_id=chat_id,
                message_ids=[message_id],
            )
        if forwarded:
            fwd_msg = forwarded[0] if isinstance(forwarded, list) else forwarded
            logger.info(
                "userbot: forwarded %s/%s to Saved Messages -> msg %s",
                chat_id,
                message_id,
                getattr(fwd_msg, "id", None),
            )
            return fwd_msg.id
    except Exception as exc:
        logger.warning(
            "userbot: forward to Saved Messages failed for %s/%s: %s",
            chat_id,
            message_id,
            exc,
        )
    return None


async def _probe_file_wakeup(client, msg) -> bool:
    """Try ``upload.GetFile`` with **1 byte** at offset 0 to "wake up"
    a sluggish storage node.

    A tiny request is more likely to get through than a full chunk.  If
    it succeeds, the node is responsive and a subsequent full download may
    work while the node is "warm".

    Returns ``True`` if the probe succeeded.
    """
    from pyrogram import raw

    try:
        location, _ = await _get_raw_file_location(msg)
        if location is None:
            return False

        result = await client.invoke(
            raw.functions.upload.GetFile(
                location=location,
                offset=0,
                limit=1,  # just 1 byte
            )
        )
        got_bytes = len(result.bytes) if hasattr(result, "bytes") else 0
        logger.info(
            "userbot: 1-byte probe succeeded (%d bytes) — storage node is responsive",
            got_bytes,
        )
        return True
    except Exception as exc:
        logger.warning(
            "userbot: 1-byte probe failed (node unresponsive): %s",
            exc,
        )
        return False


async def _recycle_client_session(client) -> bool:
    """Disconnect and reconnect the Pyrogram client to potentially land on a
    different Telegram DC or a different storage node within the same DC.

    Returns ``True`` if the session was recycled successfully.
    """
    try:
        logger.info("userbot: recycling Pyrogram session (stop \u2192 start)")
        await client.stop()
        await asyncio.sleep(3)
        await client.start()
        logger.info("userbot: Pyrogram session recycled successfully")
        return True
    except Exception as exc:
        logger.warning("userbot: session recycling failed: %s", exc)
        return False


async def _discard_relay_copy(client, client_type: str | None, relay_chat_id, relay_msg_id) -> bool:
    """Delete a relay copy the fallback forwarded but could not download.

    The forward exists only so this userbot session can reach the media. Once the
    download has failed the copy has no further use, and a failed 700 MB video
    left in a shared relay chat is pure clutter. Best-effort: never raises.
    """
    if not relay_chat_id or not relay_msg_id:
        return False
    try:
        if client_type == "telethon":
            await client.delete_messages(relay_chat_id, [relay_msg_id])
        else:
            await client.delete_messages(chat_id=relay_chat_id, message_ids=[relay_msg_id])
        logger.info("userbot: removed the relay copy at %s/%s", relay_chat_id, relay_msg_id)
        return True
    except Exception:
        logger.debug("userbot: could not remove the relay copy at %s/%s", relay_chat_id, relay_msg_id)
        return False


async def _try_relay_fallback(
    client,
    chat_id: int | str,
    message_id: int,
    dest_path: str,
    relay_chat_id: int | str | None = None,
    client_type: str = "pyrogram",
    progress_callback=None,
) -> bool:
    """Try a relay-group fallback by forwarding the original message to a trusted chat
    and retrying the download from the forwarded copy.

    This is used when direct peer/message resolution fails for the original
    source chat, which is common for large channels and stale peer state.

    Args:
        progress_callback: Optional ``(current, total)`` callback for download progress.
    """
    if not relay_chat_id:
        return False

    try:
        relay_chat_id = int(relay_chat_id)
    except (TypeError, ValueError):
        relay_chat_id = str(relay_chat_id)

    # Bound before the try so the failure paths below can clean up the forward.
    relay_msg_id = None
    try:
        logger.info(
            "userbot: relay fallback trying %s/%s -> relay %s",
            chat_id,
            message_id,
            relay_chat_id,
        )
        # Handle both Telethon and Pyrogram forward_messages API differences:
        # - Pyrogram: forward_messages(chat_id=..., from_chat_id=..., message_ids=[...])
        # - Telethon: forward_messages(entity, messages=[...], from_peer=...)
        if client_type == "telethon":
            forwarded = await client.forward_messages(
                relay_chat_id,
                messages=[message_id],
                from_peer=chat_id,
            )
        else:
            forwarded = await client.forward_messages(
                chat_id=relay_chat_id,
                from_chat_id=chat_id,
                message_ids=[message_id],
            )
        forwarded_msg = None
        if forwarded:
            forwarded_msg = forwarded[0] if isinstance(forwarded, list) else forwarded
        if forwarded_msg is None:
            logger.warning("userbot: relay fallback forward returned no message")
            return False

        relay_msg_id = getattr(forwarded_msg, "id", None)
        if relay_msg_id is None:
            logger.warning("userbot: relay fallback forward message has no id")
            return False

        logger.info(
            "userbot: relay fallback forwarded to %s/%s, retrying download",
            relay_chat_id,
            relay_msg_id,
        )
        if client_type == "telethon":
            relay_msgs = await client.get_messages(relay_chat_id, ids=[relay_msg_id])
        else:
            relay_msgs = await client.get_messages(relay_chat_id, message_ids=[relay_msg_id])
        if relay_msgs:
            relay_msg = relay_msgs[0] if isinstance(relay_msgs, list) else relay_msgs
            if (
                relay_msg
                and getattr(relay_msg, "media", None)
                and await _download_and_ensure_path(client, relay_msg, dest_path, progress_callback=progress_callback)
            ):
                return True
        logger.warning(
            "userbot: relay fallback download from %s/%s failed",
            relay_chat_id,
            relay_msg_id,
        )
        await _discard_relay_copy(client, client_type, relay_chat_id, relay_msg_id)
    except Exception as exc:
        logger.warning("userbot: relay fallback failed for %s/%s: %s", chat_id, message_id, exc)
        await _discard_relay_copy(client, client_type, relay_chat_id, relay_msg_id)
    return False


async def _attempt_recovery_download(
    client,
    chat_id,
    message_id: int,
    dest_path: str,
    progress_callback=None,
) -> bool:
    """Attempt to recover a download that failed with persistent ``-503 Timeout``.

    Recovery sequence:
        1. Get the message again (needed for probe / forward).
        2. Try a **1-byte probe** \u2014 if it succeeds immediately retry full download.
        3. ~~Forward to Saved Messages~~ (disabled — creates unwanted copies in DM).
        4. Try **session recycling** \u2014 disconnect/reconnect, then retry one more
           time on the original message.

    Args:
        progress_callback: Optional ``(current, total)`` callback for download progress.

    Returns ``True`` on success, ``False`` if all recovery methods failed.
    """

    logger.info(
        "userbot: starting recovery download for %s/%s -> %s",
        chat_id,
        message_id,
        dest_path,
    )

    # Resolve the peer (same approach as _download_with_pyrogram)
    target = await _normalize_target(chat_id)
    candidates = [await _resolve_pyrogram_peer(client, target)]

    # DM fallback: if chat_id looks like a user ID (Bot API DM),
    # also try the bot's user ID so Pyrogram can access the bot's chat.
    if _is_user_dm_chat(chat_id):
        bot_user_id = _get_bot_user_id()
        if bot_user_id is not None and bot_user_id != abs(int(chat_id)):
            bot_resolved = await _resolve_pyrogram_peer(client, bot_user_id)
            if bot_resolved not in candidates:
                candidates.append(bot_resolved)

    # ---- Step 1: Find the message ----
    msg = None
    for _peer in candidates:
        try:
            messages = await client.get_messages(_peer, message_ids=[message_id])
            if messages:
                _m = messages[0] if isinstance(messages, list) else messages
                if _m and getattr(_m, "media", None):
                    msg = _m
                    break
        except ValueError as e:
            if "Peer id invalid" in str(e) and isinstance(_peer, int) and _is_large_bot_api_channel(_peer):
                channel_peer = await _resolve_bot_api_channel_raw(client, _peer)
                if channel_peer is not None:
                    _m = await _get_messages_via_raw_channel_api(
                        client,
                        channel_peer,
                        message_id,
                    )
                    if _m is not None and getattr(_m, "media", None):
                        msg = _m
                    break
        except Exception:
            continue

    if msg is None:
        logger.warning(
            "userbot: recovery could not find message %s/%s",
            chat_id,
            message_id,
        )
        return False

    # ---- Step 2: 1-byte probe + retry ----
    probe_ok = await _probe_file_wakeup(client, msg)
    if probe_ok:
        logger.info("userbot: recovery \u2014 probe succeeded, retrying download immediately")
        if await _download_and_ensure_path(client, msg, dest_path, progress_callback=progress_callback):
            return True
        logger.info("userbot: recovery \u2014 probe retry still failed, continuing...")
    else:
        logger.info("userbot: recovery \u2014 probe failed, trying forward + retry")

    # ---- Step 3: Forward to Saved Messages + retry with fresh file_reference ----
    # ── DISABLED: forwarding to the userbot's Saved Messages creates an
    #    unwanted copy in the user's DM.  The file is already accessible
    #    via the relay group, so this extra forward is unnecessary.
    #    We skip straight to session recycling (Step 4). ──
    logger.info(
        "userbot: recovery \u2014 skipping forward to Saved Messages (disabled), trying session recycle instead"
    )

    # ---- Step 4: Session recycling + final retry ----
    recycled = await _recycle_client_session(client)
    if recycled:
        logger.info("userbot: recovery \u2014 session recycled, final retry on original msg")
        if await _download_and_ensure_path(client, msg, dest_path, progress_callback=progress_callback):
            return True

    logger.warning(
        "userbot: all recovery methods exhausted for %s/%s",
        chat_id,
        message_id,
    )
    return False


async def _resolve_message_via_telethon(client, chat_id: int | str, message_id: int, *, target=None):
    """Resolve the message carrying the media for ``chat_id``/``message_id``.

    The one implementation of Telethon peer resolution: the smart entity
    resolver first, the raw target second, then the DM fallback - a Bot API DM
    ``chat_id`` is the *user's* id, while MTProto needs the bot's own id to see
    that conversation at all. Returns the messages when the result actually
    holds media, otherwise ``None``.
    """
    if target is None:
        target = await _normalize_target(chat_id, client)

    # Use smart entity resolution for better channel/chat handling
    resolved_entity = await _resolve_telethon_entity(client, chat_id)
    msgs = None
    if resolved_entity is not None:
        try:
            logger.info(
                "userbot: Telethon trying get_messages via resolved entity (id=%s, ids=%s)",
                getattr(resolved_entity, "id", None),
                message_id,
            )
            msgs = await client.get_messages(resolved_entity, ids=message_id)
        except Exception as e:
            logger.warning(
                "userbot: get_messages via resolved entity failed: %s; trying raw target",
                e,
            )
            msgs = None

    # If entity resolution didn't work, fall back to direct get_messages
    if msgs is None:
        try:
            logger.info(
                "userbot: Telethon trying get_messages(target=%s, ids=%s)",
                target,
                message_id,
            )
            msgs = await client.get_messages(target, ids=message_id)
        except Exception as e:
            logger.exception("userbot: get_messages direct by id failed: %s", e)
            msgs = None

    _telethon_msgs = None
    if msgs:
        msg = msgs[0] if isinstance(msgs, (list, tuple)) else msgs
        logger.info(
            "userbot: Telethon direct lookup resolved target=%s msg_id=%s media=%s",
            target,
            getattr(msg, "id", None),
            bool(getattr(msg, "media", None)),
        )
        if getattr(msg, "media", None):
            _telethon_msgs = msgs
        else:
            logger.debug("userbot: message found but no media: %s/%s", target, message_id)

    # ── DM fallback: Bot API chat_id maps to user ID in DMs, but MTProto
    # needs the **bot's** user ID.  Try resolving the bot from BOT_TOKEN.
    if _telethon_msgs is None and _is_user_dm_chat(chat_id):
        bot_user_id = _get_bot_user_id()
        if bot_user_id is not None and bot_user_id != abs(int(chat_id)):
            try:
                logger.info(
                    "userbot: Telethon DM chat detected (chat_id=%s), trying bot entity (bot_id=%s)",
                    chat_id,
                    bot_user_id,
                )
                # Use _resolve_telethon_entity which has built-in dialog scan fallback
                bot_entity = await _resolve_telethon_entity(client, bot_user_id)
                if bot_entity is not None:
                    logger.info("userbot: Telethon resolved bot entity, trying get_messages from bot DM")
                    bot_msgs = await client.get_messages(bot_entity, ids=message_id)
                    if bot_msgs:
                        bot_msg = bot_msgs[0] if isinstance(bot_msgs, (list, tuple)) else bot_msgs
                        if getattr(bot_msg, "media", None):
                            _telethon_msgs = bot_msgs
                            logger.info(
                                "userbot: Telethon DM fallback resolved msg %s/%s with media",
                                bot_user_id,
                                message_id,
                            )
            except Exception as e:
                logger.warning("userbot: Telethon bot entity resolution failed: %s", e)

    return _telethon_msgs


async def download_media_to_sink(
    chat_id: int | str,
    message_id: int,
    sink,
    *,
    expected_size: int | None = None,
    progress_callback=None,
    user_id: int | None = None,
) -> bool:
    """Stream a Telegram file straight into *sink*, with no local copy.

    The sink is what storage handed back from ``open_upload_sink``: Telethon
    writes each MTProto chunk into it as the chunk arrives, and it awaits a
    write whose return value is awaitable - which is how a multipart sink
    applies back-pressure to the download instead of buffering the media.

    The **caller owns the sink's lifecycle**: open it before calling, ``close``
    it on ``True``, ``abort`` it on ``False``. This path is Telethon-only -
    Pyrogram's ``download_media`` accepts a path and can only ever write to disk
    - so a ``False`` means "fall back to the disk download", never "the upload
    failed for good".
    """
    if TelegramClient is None or sink is None:
        logger.debug("userbot: Telethon unavailable; cannot stream into a sink")
        return False

    from utils.telethon_session import (
        build_telethon_client,
        get_db_model,
        get_telethon_session_string_for_user,
        get_userbot_credentials,
        operating_user_id,
    )

    # Streaming is still a user's transfer: their session when they have one,
    # else the deployment's (see ``operating_user_id``).
    user_id = await operating_user_id(user_id, get_db_model())

    chunk_size_kb = get_download_chunk_size_kb()
    try:
        api_id, api_hash = get_userbot_credentials()
    except Exception as e:
        # No userbot configured at all: a normal state for a deploy that only
        # uses the Bot API. It is a "no" from this path, never an error.
        logger.info("userbot: no userbot credentials (%s); cannot stream into a sink", e)
        return False
    try:
        session_str = await get_telethon_session_string_for_user(user_id=user_id, db_model=get_db_model())
    except Exception:
        session_str = None

    client = build_telethon_client(api_id, api_hash, session_str=session_str)
    if client is None:
        logger.debug("userbot: no Telethon session for streaming")
        return False

    try:
        logger.info("userbot: starting Telethon client to stream %s/%s into storage", chat_id, message_id)
        await client.start()
        target = await _normalize_target(chat_id, client)
        msgs = await _resolve_message_via_telethon(client, chat_id, message_id, target=target)
        if not msgs:
            logger.info("userbot: could not resolve %s/%s for streaming", chat_id, message_id)
            return False

        await sink.open()
        logger.info(
            "userbot: streaming %s/%s into %s",
            target,
            message_id,
            getattr(sink, "key", "?"),
        )
        watch = _ProgressWatch()
        dl_kwargs = {
            "file": sink,
            "progress_callback": watch.wrap(progress_callback),
        }
        try:
            await _wait_download_or_stall(
                asyncio.create_task(client.download_media(msgs[0], **dl_kwargs, part_size_kb=chunk_size_kb)),
                watch,
            )
        except TypeError:
            # part_size_kb was removed in Telethon v1.35+.
            logger.debug("userbot: Telethon does not support part_size_kb, retrying without")
            await _wait_download_or_stall(
                asyncio.create_task(client.download_media(msgs[0], **dl_kwargs)),
                watch,
            )

        written = int(sink.tell() or 0)
        if written <= 0:
            logger.warning("userbot: streaming %s/%s wrote nothing", chat_id, message_id)
            return False
        if expected_size and written < int(expected_size):
            # A short read is a truncated transfer, and the whole point of the
            # bucket object is that it is the complete source: refuse it so the
            # caller can abort the upload and retry from disk.
            logger.warning(
                "userbot: streaming %s/%s wrote %d bytes but %s were expected - truncated, refusing",
                chat_id,
                message_id,
                written,
                expected_size,
            )
            return False
        if expected_size and written != int(expected_size):
            # Longer than Telegram announced: the bytes are all there, and a size
            # Telegram merely rounded is not a reason to throw away a good copy.
            logger.warning(
                "userbot: streaming %s/%s wrote %d bytes against %s announced (accepting)",
                chat_id,
                message_id,
                written,
                expected_size,
            )
        logger.info(
            "userbot: streamed %dMB for %s/%s into storage",
            written // (1024 * 1024),
            chat_id,
            message_id,
        )
        return True
    except Exception as e:
        logger.warning("userbot: streaming download failed for %s/%s: %s", chat_id, message_id, e)
        return False
    finally:
        with contextlib.suppress(Exception):
            await client.disconnect()


async def download_head_via_userbot(
    chat_id: int | str,
    message_id: int,
    dest_path: str,
    *,
    max_bytes: int = 262144,
    user_id: int | None = None,
    timeout: float = 60.0,
) -> bool:
    """Read only the **first** ``max_bytes`` of a Telegram media into *dest_path*.

    Some questions about a media can be answered from its opening bytes - what
    bitrate does this audio carry? - and the download that answers them is the
    one worth avoiding: a 47MB audio cannot be read over the Bot API at all, so
    the only way to learn its bitrate used to be fetching the whole file through
    the userbot pipeline. Telethon's ``iter_download`` takes a byte offset and a
    limit, so this is a genuine partial read: a quarter of a megabyte instead of
    the media.

    Deliberately best-effort. Returns ``False`` - never raises - for a missing
    credential, an unresolvable message, a timeout or a transport error, because
    every caller uses the answer only to decide whether it can skip work it would
    otherwise do.
    """
    if TelegramClient is None or not dest_path or max_bytes <= 0:
        return False

    from utils.telethon_session import (
        build_telethon_client,
        get_db_model,
        get_telethon_session_string_for_user,
        get_userbot_credentials,
        operating_user_id,
    )

    # Best-effort read, so it asks the same question as every other userbot path:
    # a user with no session of their own is served by the deployment's.
    user_id = await operating_user_id(user_id, get_db_model())

    try:
        api_id, api_hash = get_userbot_credentials()
    except Exception as e:
        # No userbot configured at all: a normal state for a Bot-API-only
        # deploy, and a "no" from this path rather than an error.
        logger.debug("userbot: no userbot credentials (%s); cannot read a header", e)
        return False

    try:
        session_str = await get_telethon_session_string_for_user(user_id=user_id, db_model=get_db_model())
    except Exception:
        session_str = None

    client = build_telethon_client(api_id, api_hash, session_str=session_str)
    if client is None:
        logger.debug("userbot: no Telethon session for a header read")
        return False

    # Written to a sibling and swapped in only once the bytes are there. An
    # aborted read must never leave a file at the destination: every caller asks
    # nothing more than "is there something at this path", and a leftover empty
    # one answers yes - the same reason every download in this project stages
    # through a ``.part`` file.
    part_path = f"{dest_path}.part"

    async def _fetch() -> bool:
        try:
            await client.start()
            target = await _normalize_target(chat_id, client)
            msgs = await _resolve_message_via_telethon(client, chat_id, message_id, target=target)
            if not msgs:
                logger.debug("userbot: could not resolve %s/%s for a header read", chat_id, message_id)
                return False
            written = 0
            with open(part_path, "wb") as fh:
                async for chunk in client.iter_download(msgs[0], offset=0, limit=max_bytes):
                    if not chunk:
                        continue
                    # Only the bytes still missing are written, so the file holds
                    # the header and not a whole chunk past it: ``limit`` is
                    # honoured by every version of Telethon this project
                    # supports, but a version that read it as a *chunk* count
                    # would otherwise turn a header read back into a full
                    # download of the very media this exists to avoid.
                    slice_ = chunk[: max_bytes - written]
                    fh.write(slice_)
                    written += len(slice_)
                    if written >= max_bytes:
                        break
            if written <= 0:
                return False
            os.replace(part_path, dest_path)
            return True
        finally:
            with contextlib.suppress(OSError):
                if os.path.exists(part_path):
                    os.remove(part_path)
            with contextlib.suppress(Exception):
                await client.disconnect()

    try:
        return bool(await asyncio.wait_for(_fetch(), timeout=timeout))
    except TimeoutError:
        logger.debug("userbot: header read for %s/%s timed out after %.0fs", chat_id, message_id, timeout)
        return False
    except Exception as exc:
        logger.debug("userbot: header read for %s/%s failed: %s", chat_id, message_id, exc)
        return False


async def _download_with_telethon(
    chat_id: int | str,
    message_id: int,
    dest_path: str,
    msg_date: str | None = None,
    file_unique_id: str | None = None,
    progress_callback=None,
    user_id: int | None = None,
    *,
    expected_size: int | None = None,
    want_audio: bool = False,
) -> bool:
    """Download using Telethon client.

    Args:
        progress_callback: Optional ``(current, total)`` callback for download progress.
        user_id: Optional Telegram user ID for per-user session resolution.
        expected_size: Size Telegram announced for the requested media. The scans
            use it to refuse a nearby message's media - see
            :func:`_scan_candidate_matches`.
        want_audio: The requested media is an audio, so a scan candidate without
            an audio stream is not it.
    """
    if TelegramClient is None:
        logger.debug("Telethon not installed; skipping Telethon download")
        return False

    from utils.telethon_session import (
        build_telethon_client,
        get_db_model,
        get_telethon_session_string_for_user,
        get_userbot_credentials,
    )

    chunk_size_kb = get_download_chunk_size_kb()
    api_id, api_hash = get_userbot_credentials()

    session_str = None
    try:
        # Pass the registered MongoDB model so a session stored only in Mongo is
        # still usable here (these call paths never see ``application.bot_data``).
        session_str = await get_telethon_session_string_for_user(user_id=user_id, db_model=get_db_model())
    except Exception:
        session_str = None

    client = build_telethon_client(api_id, api_hash, session_str=session_str)
    try:
        logger.info("userbot: starting Telethon client for download")
        await client.start()
        logger.info("userbot: Telethon client started successfully")
    except Exception as e:
        logger.exception("userbot: failed to start Telethon client: %s", e)
        return False

    try:
        target = await _normalize_target(chat_id, client)
        logger.info(
            "userbot: Telethon direct lookup started for chat=%s msg=%s target=%s",
            chat_id,
            message_id,
            target,
        )

        # Peer resolution is shared with the streaming path: one implementation,
        # so a fix here (or there) fixes both.
        _telethon_msgs = await _resolve_message_via_telethon(client, chat_id, message_id, target=target)

        if _telethon_msgs:
            msg = _telethon_msgs[0] if isinstance(_telethon_msgs, (list, tuple)) else _telethon_msgs
            if getattr(msg, "media", None):
                logger.info("userbot: message found; downloading %s/%s to %s", target, message_id, dest_path)
                for attempt in range(3):
                    try:
                        logger.debug(
                            "userbot: download attempt %s for %s/%s -> %s",
                            attempt + 1,
                            target,
                            getattr(msg, "id", None),
                            dest_path,
                        )
                        # The stall guard only ever learns that a download is
                        # alive through the watch, so the callback is injected and
                        # wrapped rather than left to the caller: Telethon's own
                        # progress callback is what keeps the transfer from
                        # looking silent from byte one, and a call that passed no
                        # callback would otherwise be killed as "stalled" at
                        # DOWNLOAD_STALL_SECONDS while running perfectly well.
                        _watch = _ProgressWatch()
                        dl_kwargs = {
                            "file": dest_path,
                            "progress_callback": _watch.wrap(progress_callback),
                        }
                        # part_size_kb removed in Telethon v1.35+; catch TypeError and retry without
                        try:
                            _dl_result = await _wait_download_or_stall(
                                asyncio.create_task(
                                    client.download_media(msg, **dl_kwargs, part_size_kb=chunk_size_kb)
                                ),
                                _watch,
                            )
                        except TypeError:
                            logger.debug("userbot: Telethon does not support part_size_kb, retrying without")
                            _dl_result = await _wait_download_or_stall(
                                asyncio.create_task(client.download_media(msg, **dl_kwargs)),
                                _watch,
                            )
                        dl_result = _dl_result
                        _reconcile_download_path(dl_result, dest_path)
                        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                            ok = await _ffprobe_ok(dest_path)
                            if ok:
                                return True
                        logger.warning(
                            "userbot: downloaded file failed validation (attempt %s) %s", attempt + 1, dest_path
                        )
                        with contextlib.suppress(Exception):
                            os.remove(dest_path)
                    except TimeoutError:
                        logger.warning(
                            "userbot: Telethon download attempt %s timed out after %ds",
                            attempt + 1,
                            TELETHON_DOWNLOAD_TIMEOUT,
                        )
                    except Exception as e:
                        logger.exception("userbot: download attempt %s failed: %s", attempt + 1, e)
                logger.debug("userbot: message found but downloads failed validation: %s/%s", target, message_id)

        # Search by date if provided
        search_done = False
        if msg_date:
            try:
                dt = datetime.fromisoformat(msg_date)
                # Stored dates exist in two shapes: records written before the
                # timezone cleanup are naive (``utcnow().isoformat()``) and newer
                # ones are zone-suffixed. Both mean UTC, so pin the zone here
                # instead of leaning on Telethon's own "naive means UTC" rule for
                # ``offset_date`` - the instant must not depend on the host's
                # local zone.
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
            except Exception:
                # Without this the date-scan fallback would just look unneeded.
                logger.warning("userbot: unparseable msg_date %r; skipping the date-scan fallback", msg_date)
                dt = None
            if dt is not None:
                logger.debug("userbot: searching around date %s in %s", msg_date, target)
                try:
                    logger.info(
                        "userbot: Telethon date-scan started for target=%s around=%s",
                        target,
                        msg_date,
                    )
                    async for m in client.iter_messages(target, limit=100, offset_date=dt):
                        if not _scan_candidate_matches(m, expected_size=expected_size, want_audio=want_audio):
                            # Not the media that was asked for: a photo, another
                            # file, or something with no audio when audio was
                            # requested. Skip it instead of downloading it into the
                            # requested file's path - that is how a 1600x800 JPEG
                            # came to be reported as the module it was asked for.
                            if getattr(m, "media", None):
                                logger.info(
                                    "userbot: date scan skipping message %s (media is %s bytes%s) - not the source that was requested",
                                    getattr(m, "id", "?"),
                                    _message_media_size(m),
                                    ", no audio" if want_audio and not _message_has_audio(m) else "",
                                )
                            continue
                        if getattr(m, "media", None):
                            for _ in range(3):
                                try:
                                    _dl_kwargs = {"file": dest_path}
                                    if progress_callback is not None:
                                        _dl_kwargs["progress_callback"] = progress_callback
                                    try:
                                        dl_result = await asyncio.wait_for(
                                            client.download_media(m, **_dl_kwargs, part_size_kb=chunk_size_kb),
                                            timeout=TELETHON_DOWNLOAD_TIMEOUT,
                                        )
                                    except TypeError:
                                        logger.debug("userbot: Telethon no part_size_kb (date scan), retrying without")
                                        dl_result = await asyncio.wait_for(
                                            client.download_media(m, **_dl_kwargs),
                                            timeout=TELETHON_DOWNLOAD_TIMEOUT,
                                        )
                                    _reconcile_download_path(dl_result, dest_path)
                                    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                                        written = os.path.getsize(dest_path)
                                        # The candidate looked right before the
                                        # transfer; check what actually landed. A
                                        # file of the wrong size is not the source,
                                        # and keeping it is what let the fallback
                                        # report success for media it never fetched.
                                        if not _size_matches(written, expected_size or _message_media_size(m)):
                                            logger.warning(
                                                "userbot: date scan message %s downloaded %d bytes but %s were expected; discarding it",
                                                getattr(m, "id", "?"),
                                                written,
                                                expected_size or _message_media_size(m),
                                            )
                                            with contextlib.suppress(OSError):
                                                os.remove(dest_path)
                                            break
                                        ok = await _ffprobe_ok(dest_path)
                                        if ok:
                                            logger.info("userbot: downloaded via date search to %s", dest_path)
                                            return True
                                except TimeoutError:
                                    logger.debug(
                                        "userbot: date scan download timed out after %ds", TELETHON_DOWNLOAD_TIMEOUT
                                    )
                                except Exception:
                                    logger.debug("userbot: date scan download attempt failed")
                    search_done = True
                except Exception:
                    logger.debug("userbot: date scan iteration failed for %s", target)

        # Scan recent messages
        if not search_done:
            try:
                logger.info(
                    "userbot: Telethon recent-history scan started for target=%s",
                    target,
                )
                async for m in client.iter_messages(target, limit=200):
                    if not _scan_candidate_matches(m, expected_size=expected_size, want_audio=want_audio):
                        # Same gate as the date scan, same reason: a recent message
                        # with *some* media is not the requested one.
                        if getattr(m, "media", None):
                            logger.info(
                                "userbot: recent scan skipping message %s (media is %s bytes) - not the source that was requested",
                                getattr(m, "id", "?"),
                                _message_media_size(m),
                            )
                        continue
                    if getattr(m, "media", None):
                        for _ in range(3):
                            try:
                                _scan_kwargs = {"file": dest_path}
                                if progress_callback is not None:
                                    _scan_kwargs["progress_callback"] = progress_callback
                                try:
                                    dl_result = await asyncio.wait_for(
                                        client.download_media(m, **_scan_kwargs, part_size_kb=chunk_size_kb),
                                        timeout=TELETHON_DOWNLOAD_TIMEOUT,
                                    )
                                except TypeError:
                                    logger.debug("userbot: Telethon no part_size_kb (recent scan), retrying without")
                                    dl_result = await asyncio.wait_for(
                                        client.download_media(m, **_scan_kwargs),
                                        timeout=TELETHON_DOWNLOAD_TIMEOUT,
                                    )
                                _reconcile_download_path(dl_result, dest_path)
                                if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                                    written = os.path.getsize(dest_path)
                                    if not _size_matches(written, expected_size or _message_media_size(m)):
                                        logger.warning(
                                            "userbot: recent scan message %s downloaded %d bytes but %s were expected; discarding it",
                                            getattr(m, "id", "?"),
                                            written,
                                            expected_size or _message_media_size(m),
                                        )
                                        with contextlib.suppress(OSError):
                                            os.remove(dest_path)
                                        break
                                    ok = await _ffprobe_ok(dest_path)
                                    if ok:
                                        return True
                            except TimeoutError:
                                logger.debug(
                                    "userbot: recent scan download timed out after %ds", TELETHON_DOWNLOAD_TIMEOUT
                                )
                            except Exception:
                                logger.debug("userbot: recent scan download attempt failed")
            except Exception:
                logger.debug("userbot: recent scan iteration failed for %s", target)

        relay_chat_id = config.RELAY_CHAT_ID
        if relay_chat_id:
            logger.info(
                "userbot: Telethon direct resolution failed; trying relay fallback for %s/%s",
                chat_id,
                message_id,
            )
            if await _try_relay_fallback(
                client,
                chat_id,
                message_id,
                dest_path,
                relay_chat_id=relay_chat_id,
                client_type="telethon",
                progress_callback=progress_callback,
            ):
                return True

        return False
    finally:
        with contextlib.suppress(Exception):
            await client.disconnect()


async def _resolve_bot_api_channel_raw(client, bot_api_chat_id: int):
    """Resolve a Bot API channel ID (-100xxxxx...) using raw MTProto API.

    Pyrogram 2.0.106's ``get_peer_type()`` has a hardcoded range check that
    only accepts channel IDs whose raw ``channel_id <= 2147483647``.
    Channels with larger IDs (e.g. ``4367325292``) are rejected with
    ``Peer id invalid`` **before** any network request is made.

    This function bypasses the range check by invoking
    ``channels.GetChannels`` directly with ``access_hash=0``, allowing the
    server to respond with the correct access_hash.
    """
    from pyrogram import raw

    raw_channel_id = abs(bot_api_chat_id) - 1000000000000
    try:
        result = await client.invoke(
            raw.functions.channels.GetChannels(
                id=[
                    raw.types.InputChannel(
                        channel_id=raw_channel_id,
                        access_hash=0,
                    )
                ]
            )
        )
        if result and result.chats:
            chat = result.chats[0]
            access_hash = getattr(chat, "access_hash", 0)
            logger.info(
                "userbot: resolved large channel %s -> channel_id=%s access_hash=%s",
                bot_api_chat_id,
                raw_channel_id,
                access_hash,
            )
            return raw.types.InputPeerChannel(
                channel_id=raw_channel_id,
                access_hash=access_hash,
            )
    except Exception as e:
        logger.warning(
            "userbot: failed to resolve large channel %s via raw API: %s",
            bot_api_chat_id,
            e,
        )
    return None


def _is_large_bot_api_channel(peer_id) -> bool:
    """Return True if ``peer_id`` is a Bot API channel ID with a raw
    channel_id that Pyrogram 2.0.106's range check can not handle.
    """
    if not isinstance(peer_id, int) or peer_id >= 0:
        return False
    s = str(peer_id)
    if not s.startswith("-100"):
        return False
    raw_id = abs(peer_id) - 1000000000000
    # Pyrogram's MIN_CHANNEL_ID = -1002147483647, which corresponds to
    # a max raw channel_id of 2147483647 (2^31-1, 32-bit signed int).
    return raw_id > 2147483647


async def _get_messages_via_raw_channel_api(
    client,
    channel_peer,
    message_id: int,
):
    """Get a single message from a channel using raw MTProto API.

    Returns the first :class:`Message` from the response, or None.
    """
    from pyrogram import raw
    from pyrogram import types as pyro_types

    try:
        r = await client.invoke(
            raw.functions.channels.GetMessages(
                channel=channel_peer,
                id=[raw.types.InputMessageID(id=message_id)],
            )
        )
        if r and r.messages:
            users = {i.id: i for i in r.users}
            chats = {i.id: i for i in r.chats}
            msg = await pyro_types.Message._parse(
                client,
                r.messages[0],
                users,
                chats,
                replies=0,
            )
            return msg
    except Exception as e:
        logger.warning(
            "userbot: GetMessages via raw API failed for msg %s: %s",
            message_id,
            e,
        )
    return None


async def _download_and_ensure_path(client, msg, dest_path, progress_callback=None):
    """Download media from *msg* and ensure the file ends up at *dest_path*.

    Pyrogram 2.0.106's ``download_media`` resolves relative paths against
    ``self.PARENT_DIR`` and returns an absolute path.  The caller's ``dest_path``
    is often relative.  This helper reconciles the two.

    Uses ``_download_media_with_retry`` so that transient ``-503 Timeout``
    errors are automatically retried with exponential backoff.

    Args:
        client: An active Pyrogram client.
        msg: A Pyrogram ``Message`` with media.
        dest_path: Destination file path.
        progress_callback: Optional ``(current, total)`` callback for download progress.

    Returns ``True`` on success, ``False`` otherwise.
    """
    # ── CRITICAL: Convert relative path to absolute before passing to Pyrogram.
    #    Pyrogram 2.0.106's ``download_media`` resolves relative ``file_name``
    #    against its own ``PARENT_DIR`` (which is the Python install directory,
    #    e.g. ``/usr/local/bin/``), NOT the process current working directory.
    #    This causes ``[Errno 13] Permission denied: '/usr/local/bin/storage'``
    #    when it tries to create subdirectories there. ──
    _abs_dest = os.path.abspath(dest_path) if dest_path else dest_path
    try:
        _dl_kwargs = {"file_name": _abs_dest}
        if progress_callback is not None:
            _dl_kwargs["progress"] = progress_callback
        _dl = await _download_media_with_retry(client, msg, **_dl_kwargs)
    except Exception as exc:
        logger.warning(
            "userbot: download_media_with_retry failed for %s (absolute=%s): %s",
            dest_path,
            _abs_dest,
            exc,
        )
        return False

    logger.info(
        "userbot: download_media dest_path=%s returned=%s",
        _abs_dest,
        _dl,
    )
    if not _dl:
        logger.warning("userbot: download_media returned None")
        return False

    # Reconcile Pyrogram's download path to the expected destination
    _reconcile_download_path(_dl, _abs_dest)

    # Check at the absolute destination path (where the file should be)
    if os.path.exists(_abs_dest) and os.path.getsize(_abs_dest) > 0:
        ok = await _ffprobe_ok(_abs_dest)
        if ok:
            return True
        logger.warning(
            "userbot: download succeeded but ffprobe validation failed: %s",
            _abs_dest,
        )
    else:
        logger.warning(
            "userbot: download_media produced empty/missing file at %s",
            _abs_dest,
        )
    return False


async def _download_bytes_with_pyrogram(
    chat_id: int | str,
    message_id: int,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
) -> bytes | None:
    """Download a message's media into memory (bytes) using Pyrogram.

    Uses ``download_media(..., in_memory=True)`` (with retry on ``-503``)
    to get raw bytes without writing to disk.  Falls back to raw-MTProto
    chunked download when ``DOWNLOAD_CHUNK_SIZE_KB`` is explicitly set
    (see ``_download_bytes_via_raw_api``).

    Returns ``None`` on any failure.

    If ``progress_callback`` is provided, it will be called with
    ``(current_bytes, total_bytes)`` during download.

    Args:
        user_id: Optional Telegram user ID for per-user session resolution.
    """
    if PyrogramClient is None:
        logger.info("userbot: Pyrogram not installed; cannot do in-memory download")
        return None

    from utils.telethon_session import (
        build_pyrogram_client,
        get_db_model,
        get_pyrogram_session_string_for_user,
        get_userbot_credentials,
    )

    api_id, api_hash = get_userbot_credentials()
    # Resolve via per-user JSON -> MongoDB -> env so a session stored only in
    # MongoDB is usable outside the healthcheck (the sync variant cannot await).
    pyro_session = await get_pyrogram_session_string_for_user(user_id=user_id, db_model=get_db_model())
    client = build_pyrogram_client(api_id, api_hash, session_str=pyro_session)
    if client is None:
        logger.info("userbot: Pyrogram session string not configured; cannot do in-memory download")
        return None

    # Decide whether to use the raw-MTProto chunked path (preferred when
    # DOWNLOAD_CHUNK_SIZE_KB is set explicitly)
    _explicit_chunk_size = None
    _raw_chunk_override = os.getenv("DOWNLOAD_CHUNK_SIZE_KB", "")
    if _raw_chunk_override:
        with contextlib.suppress(TypeError, ValueError):
            _explicit_chunk_size = int(_raw_chunk_override)

    try:
        await client.start()
        logger.info("userbot: Pyrogram client started for in-memory download")

        target = await _normalize_target(chat_id)

        # Resolve peers to cache access_hash (prevents PEER_ID_INVALID).
        # For user-to-bot DMs, Bot API chat_id = user_id, but MTProto needs
        # the bot's user ID to access the bot-user conversation.
        _candidates = [await _resolve_pyrogram_peer(client, target)]
        logger.info(
            "userbot: in-memory download started for chat=%s msg=%s candidates=%s",
            chat_id,
            message_id,
            _candidates,
        )

        # DM fallback: if chat_id looks like a user ID (Bot API DM),
        # also try the bot's user ID so Pyrogram can access the bot's chat.
        if _is_user_dm_chat(chat_id):
            bot_user_id = _get_bot_user_id()
            if bot_user_id is not None and bot_user_id != abs(int(chat_id)):
                bot_resolved = await _resolve_pyrogram_peer(client, bot_user_id)
                if bot_resolved not in _candidates:
                    _candidates.append(bot_resolved)
                    logger.info(
                        "userbot: added bot user ID %s as candidate for in-memory DM download",
                        bot_user_id,
                    )

        for _peer in _candidates:
            try:
                logger.info(
                    "userbot: Pyrogram in-memory get_messages(peer=%s, msg=%s)",
                    _peer,
                    message_id,
                )
                messages = await client.get_messages(_peer, message_ids=[message_id])

                if messages:
                    msg = messages[0] if isinstance(messages, list) else messages
                    if _has_downloadable_media(msg):
                        logger.info(
                            "userbot: Pyrogram in-memory downloading %s/%s (peer=%s, media=%s)",
                            _peer,
                            message_id,
                            _peer,
                            bool(getattr(msg, "media", None)),
                        )
                        dl_kwargs = {"in_memory": True}
                        if progress_callback is not None:
                            dl_kwargs["progress"] = progress_callback

                        # ── Try raw-MTProto chunked download when chunk size is configured ──
                        if _explicit_chunk_size is not None:
                            raw_data = await _download_bytes_via_raw_api(
                                client,
                                msg,
                                chunk_size_kb=_explicit_chunk_size,
                                progress_callback=progress_callback,
                            )
                            if raw_data is not None:
                                logger.info(
                                    "userbot: raw API chunked download succeeded: %d bytes from %s/%s",
                                    len(raw_data),
                                    _peer,
                                    message_id,
                                )
                                return raw_data
                            logger.info(
                                "userbot: raw API chunked download failed for %s/%s, falling back to download_media",
                                _peer,
                                message_id,
                            )

                        # ── Fallback: download_media with -503 retry ──
                        try:
                            data = await _download_media_with_retry(
                                client,
                                msg,
                                **dl_kwargs,
                            )
                        except Exception as exc:
                            logger.warning(
                                "userbot: in-memory download_media with retry failed for %s/%s: %s",
                                _peer,
                                message_id,
                                exc,
                            )
                            data = None

                        if data is not None and isinstance(data, bytes) and len(data) > 0:
                            logger.info(
                                "userbot: Pyrogram in-memory download succeeded: %d bytes from %s/%s",
                                len(data),
                                _peer,
                                message_id,
                            )
                            return data
                        logger.warning(
                            "userbot: Pyrogram in-memory returned empty/invalid data for %s/%s",
                            _peer,
                            message_id,
                        )
                    else:
                        # Media but nothing downloadable: this peer is not the chat
                        # that holds the file, so let the loop try the next one.
                        logger.info(
                            "userbot: Pyrogram in-memory msg %s/%s has no downloadable media (peer=%s)",
                            _peer,
                            message_id,
                            _peer,
                        )
                else:
                    logger.info(
                        "userbot: Pyrogram in-memory get_messages(peer=%s) returned None for msg %s",
                        _peer,
                        message_id,
                    )
            except ValueError as e:
                if "Peer id invalid" in str(e) and isinstance(_peer, int) and _is_large_bot_api_channel(_peer):
                    logger.info(
                        "userbot: large channel ID %s for in-memory, trying raw API",
                        _peer,
                    )
                    channel_peer = await _resolve_bot_api_channel_raw(client, _peer)
                    if channel_peer is not None:
                        msg = await _get_messages_via_raw_channel_api(
                            client,
                            channel_peer,
                            message_id,
                        )
                        if _has_downloadable_media(msg):
                            dl_kwargs = {"in_memory": True}
                            if progress_callback is not None:
                                dl_kwargs["progress"] = progress_callback

                            # ── Raw-MTProto chunked download for raw-API resolved messages ──
                            if _explicit_chunk_size is not None:
                                raw_data = await _download_bytes_via_raw_api(
                                    client,
                                    msg,
                                    chunk_size_kb=_explicit_chunk_size,
                                    progress_callback=progress_callback,
                                )
                                if raw_data is not None:
                                    logger.info(
                                        "userbot: raw API (large channel) chunked download succeeded: %d bytes",
                                        len(raw_data),
                                    )
                                    return raw_data

                            try:
                                data = await _download_media_with_retry(
                                    client,
                                    msg,
                                    **dl_kwargs,
                                )
                            except Exception as exc:
                                logger.warning(
                                    "userbot: raw-API resolved download_media failed: %s",
                                    exc,
                                )
                                data = None

                            if data is not None and isinstance(data, bytes) and len(data) > 0:
                                logger.info(
                                    "userbot: raw API in-memory download succeeded: %d bytes",
                                    len(data),
                                )
                                return data
                else:
                    logger.warning(
                        "userbot: Pyrogram in-memory error with peer=%s msg=%s: %s",
                        _peer,
                        message_id,
                        e,
                    )
            except Exception as e:
                logger.warning(
                    "userbot: Pyrogram in-memory error with peer=%s msg=%s: %s",
                    _peer,
                    message_id,
                    e,
                )

        # ---- Final recovery: try recovery for in-memory path via temp file ----
        logger.warning(
            "userbot: Pyrogram in-memory download failed for %s/%s, trying recovery via temp file...",
            chat_id,
            message_id,
        )
        import tempfile as _tempfile

        _tmp_path = os.path.join(
            os.getenv("TEMP_PATH", _tempfile.gettempdir()),
            f"recovery_{_safe_path_token(chat_id)}_{_safe_path_token(message_id)}.tmp",
        )
        try:
            if await _attempt_recovery_download(
                client, chat_id, message_id, _tmp_path, progress_callback=progress_callback
            ):
                with open(_tmp_path, "rb") as _fh:
                    data = _fh.read()
                logger.info(
                    "userbot: recovery in-memory download succeeded: %d bytes",
                    len(data),
                )
                return data
        except Exception as exc:
            logger.warning("userbot: recovery in-memory download failed: %s", exc)
        finally:
            try:
                if os.path.exists(_tmp_path):
                    os.remove(_tmp_path)
            except Exception:
                logger.debug("userbot: failed to remove temp recovery file %s", _tmp_path)

        return None
    finally:
        # Give the internal dispatcher a moment to finish processing any
        # pending updates before closing the SQLite storage, otherwise we get
        # ``sqlite3.ProgrammingError: Cannot operate on a closed database``
        # when ``handle_updates -> fetch_peers -> storage.update_peers``
        # is still in flight.
        with contextlib.suppress(Exception):
            await asyncio.sleep(1)
        with contextlib.suppress(Exception):
            await client.stop()


async def _download_with_pyrogram(
    chat_id: int | str,
    message_id: int,
    dest_path: str,
    progress_callback=None,
    user_id: int | None = None,
) -> bool:
    """Download using Pyrogram client (session string fallback).

    Args:
        chat_id: Origin chat or bot chat ID.
        message_id: Message ID in that chat.
        dest_path: Destination file path.
        progress_callback: Optional ``(current, total)`` callback for download progress.
        user_id: Optional Telegram user ID for per-user session resolution.
    """
    if PyrogramClient is None:
        logger.info("userbot: Pyrogram not installed; skipping")
        return False

    from utils.telethon_session import (
        build_pyrogram_client,
        get_db_model,
        get_pyrogram_session_string_for_user,
        get_userbot_credentials,
    )

    api_id, api_hash = get_userbot_credentials()
    # Resolve via per-user JSON -> MongoDB -> env so a session stored only in
    # MongoDB is usable outside the healthcheck (the sync variant cannot await).
    pyro_session = await get_pyrogram_session_string_for_user(user_id=user_id, db_model=get_db_model())
    client = build_pyrogram_client(api_id, api_hash, session_str=pyro_session)
    if client is None:
        logger.info("userbot: Pyrogram session string not configured")
        return False

    # Ensure dest dir exists
    _dest_dir = os.path.dirname(dest_path)
    if _dest_dir:
        try:
            os.makedirs(_dest_dir, exist_ok=True)
        except Exception as e:
            logger.warning(
                "userbot: could not create dest dir %s (absolute=%s): %s",
                _dest_dir,
                os.path.abspath(_dest_dir),
                e,
            )

    try:
        await client.start()
        logger.info("userbot: Pyrogram client started for download")

        target = await _normalize_target(chat_id)
        logger.info(
            "userbot: disk download started for chat=%s msg=%s target=%s",
            chat_id,
            message_id,
            target,
        )

        # Resolve peers to cache access_hash (prevents PEER_ID_INVALID).
        # For user-to-bot DMs, Bot API chat_id = user_id, but MTProto needs
        # the bot's user ID to access the bot-user conversation.
        _candidates = [await _resolve_pyrogram_peer(client, target)]

        # DM fallback: if chat_id looks like a user ID (Bot API DM),
        # also try the bot's user ID so Pyrogram can access the bot's chat.
        if _is_user_dm_chat(chat_id):
            bot_user_id = _get_bot_user_id()
            if bot_user_id is not None and bot_user_id != abs(int(chat_id)):
                bot_resolved = await _resolve_pyrogram_peer(client, bot_user_id)
                if bot_resolved not in _candidates:
                    _candidates.append(bot_resolved)
                    logger.info(
                        "userbot: added bot user ID %s as candidate for DM download",
                        bot_user_id,
                    )

        _found_msg = False
        for _peer in _candidates:
            try:
                logger.info(
                    "userbot: Pyrogram trying get_messages(peer=%s, msg=%s)",
                    _peer,
                    message_id,
                )
                messages = await client.get_messages(_peer, message_ids=[message_id])

                if messages:
                    msg = messages[0] if isinstance(messages, list) else messages
                    if msg:
                        _has_media = _has_downloadable_media(msg)
                        logger.info(
                            "userbot: Pyrogram get_messages returned msg id=%s peer=%s media=%s downloadable=%s",
                            getattr(msg, "id", None),
                            _peer,
                            bool(getattr(msg, "media", None)),
                            _has_media,
                        )
                        if _has_media:
                            _found_msg = True
                    if msg and _has_media:
                        logger.info(
                            "userbot: Pyrogram downloading %s/%s -> %s (peer=%s)",
                            _peer,
                            message_id,
                            dest_path,
                            _peer,
                        )
                        logger.info(
                            "userbot: resolved message payload for disk download: peer=%s msg_id=%s media=%s",
                            _peer,
                            getattr(msg, "id", None),
                            bool(getattr(msg, "media", None)),
                        )
                        if await _download_and_ensure_path(client, msg, dest_path, progress_callback=progress_callback):
                            return True
                        logger.warning(
                            "userbot: Pyrogram download failed for %s/%s (peer=%s)",
                            _peer,
                            message_id,
                            _peer,
                        )
                        # The message does carry a file, so this peer is the right
                        # one and the download itself failed - break out to avoid
                        # re-downloading the same file from another peer.
                        break
                    else:
                        # A message with media but nothing downloadable means this
                        # peer resolved to the wrong chat (the Bot API user id maps
                        # to the account's own peer, not the DM with the bot), so
                        # keep going - the next candidate holds the real file.
                        logger.info(
                            "userbot: Pyrogram message %s/%s has no downloadable media (peer=%s); trying the next peer",
                            _peer,
                            message_id,
                            _peer,
                        )
                else:
                    logger.info(
                        "userbot: Pyrogram get_messages(peer=%s) returned None/empty for msg %s",
                        _peer,
                        message_id,
                    )
            except ValueError as e:
                err_str = str(e)
                if "Peer id invalid" in err_str and isinstance(_peer, int) and _is_large_bot_api_channel(_peer):
                    # Pyrogram's get_peer_type range check rejects this channel ID.
                    # Retry using raw MTProto API.
                    logger.info(
                        "userbot: large channel ID %s, retrying via raw API",
                        _peer,
                    )
                    channel_peer = await _resolve_bot_api_channel_raw(client, _peer)
                    if channel_peer is not None:
                        msg = await _get_messages_via_raw_channel_api(
                            client,
                            channel_peer,
                            message_id,
                        )
                        if msg is not None:
                            _found_msg = True
                            if _has_downloadable_media(msg):
                                logger.info(
                                    "userbot: raw API got msg %s with media, downloading...",
                                    message_id,
                                )
                                if await _download_and_ensure_path(
                                    client, msg, dest_path, progress_callback=progress_callback
                                ):
                                    return True
                                logger.warning(
                                    "userbot: raw API download failed validation for %s/%s",
                                    _peer,
                                    message_id,
                                )
                            else:
                                logger.info(
                                    "userbot: raw API msg %s/%s has no media",
                                    _peer,
                                    message_id,
                                )
                        else:
                            logger.warning(
                                "userbot: raw API returned no message for %s/%s",
                                _peer,
                                message_id,
                            )
                else:
                    logger.warning(
                        "userbot: Pyrogram error with peer=%s msg=%s: %s",
                        _peer,
                        message_id,
                        e,
                    )
            except Exception as e:
                logger.warning(
                    "userbot: Pyrogram error with peer=%s msg=%s: %s",
                    _peer,
                    message_id,
                    e,
                )

        async def _try_large_channel(peer):
            """Try downloading from a large Bot API channel ID using raw MTProto.
            Returns True on success, False if peer not applicable, or None."""
            if not _is_large_bot_api_channel(peer):
                return False
            logger.info(
                "userbot: large channel ID %s, trying raw API",
                peer,
            )
            channel_peer = await _resolve_bot_api_channel_raw(client, peer)
            if channel_peer is None:
                return None
            msg = await _get_messages_via_raw_channel_api(
                client,
                channel_peer,
                message_id,
            )
            if not _has_downloadable_media(msg):
                return None
            if await _download_and_ensure_path(client, msg, dest_path, progress_callback=progress_callback):
                return True
            return None

        # Fallback: scan the recent history of each candidate peer for a matching media message.
        for _peer in _candidates:
            try:
                logger.info(
                    "userbot: Pyrogram scanning history of peer=%s for msg=%s (fallback)",
                    _peer,
                    message_id,
                )
                async for msg in client.get_chat_history(_peer, limit=50):
                    if getattr(msg, "id", None) == message_id and _has_downloadable_media(msg):
                        logger.info(
                            "userbot: Pyrogram found msg %s/%s in history (peer=%s)",
                            _peer,
                            message_id,
                            _peer,
                        )
                        if await _download_and_ensure_path(client, msg, dest_path, progress_callback=progress_callback):
                            return True
                        break
            except ValueError as e:
                if "Peer id invalid" in str(e):
                    result = await _try_large_channel(_peer)
                    if result is True:
                        return True
                    if result is not None:
                        _found_msg = True
            except Exception as e:
                logger.warning(
                    "userbot: Pyrogram history scan(peer=%s) failed: %s",
                    _peer,
                    e,
                )

        # Final attempt: try get_chat to resolve peer properly, then retry get_messages
        if not _found_msg:
            for _peer in _candidates:
                try:
                    logger.info(
                        "userbot: Pyrogram resolving peer=%s via get_chat() for msg %s",
                        _peer,
                        message_id,
                    )
                    _chat = await client.get_chat(_peer)
                    if _chat:
                        _resolved_id = getattr(_chat, "id", _peer)
                        logger.info(
                            "userbot: Pyrogram resolved chat peer=%s -> id=%s",
                            _peer,
                            _resolved_id,
                        )
                        messages = await client.get_messages(_resolved_id, message_ids=[message_id])
                        if messages:
                            msg = messages[0] if isinstance(messages, list) else messages
                            if _has_downloadable_media(msg):
                                _found_msg = True
                                if await _download_and_ensure_path(
                                    client, msg, dest_path, progress_callback=progress_callback
                                ):
                                    return True
                except ValueError as e:
                    if "Peer id invalid" in str(e):
                        result = await _try_large_channel(_peer)
                        if result is True:
                            return True
                        if result is not None:
                            _found_msg = True
                except Exception as e:
                    logger.warning(
                        "userbot: Pyrogram get_chat(peer=%s) or retry failed: %s",
                        _peer,
                        e,
                    )

        if not _found_msg:
            logger.warning(
                "userbot: Pyrogram could not find message %s in any candidate peer (%s)",
                message_id,
                _candidates,
            )
            logger.info(
                "userbot: disk download failed to resolve any message for chat=%s msg=%s",
                chat_id,
                message_id,
            )

        _abs_dest = os.path.abspath(dest_path)
        relay_chat_id = config.RELAY_CHAT_ID
        if not os.path.exists(_abs_dest) or os.path.getsize(_abs_dest) == 0:
            if relay_chat_id:
                logger.info(
                    "userbot: trying relay fallback for %s/%s via %s",
                    chat_id,
                    message_id,
                    relay_chat_id,
                )
                if await _try_relay_fallback(
                    client,
                    chat_id,
                    message_id,
                    dest_path,
                    relay_chat_id=relay_chat_id,
                    client_type="pyrogram",
                    progress_callback=progress_callback,
                ):
                    logger.info(
                        "userbot: relay fallback download succeeded for %s/%s",
                        chat_id,
                        message_id,
                    )
                    return True

                    # ---- Final recovery: attempt advanced techniques for persistent -503 ----
            logger.info(
                "userbot: all standard download methods failed for %s/%s, trying recovery (forward+probe+recycle)...",
                chat_id,
                message_id,
            )
            if await _attempt_recovery_download(
                client, chat_id, message_id, dest_path, progress_callback=progress_callback
            ):
                logger.info(
                    "userbot: recovery download succeeded for %s/%s",
                    chat_id,
                    message_id,
                )
                return True

        return False
    finally:
        # Give the internal dispatcher a moment to finish processing any
        # pending updates before closing the SQLite storage, otherwise we get
        # ``sqlite3.ProgrammingError: Cannot operate on a closed database``
        # when ``handle_updates -> fetch_peers -> storage.update_peers``
        # is still in flight.
        with contextlib.suppress(Exception):
            await asyncio.sleep(1)
        with contextlib.suppress(Exception):
            await client.stop()


async def download_forward_via_userbot(
    chat_id: int | str,
    message_id: int,
    dest_path: str,
    msg_date: str | None = None,
    file_unique_id: str | None = None,
    progress_callback=None,
    user_id: int | None = None,
    *,
    expected_size: int | None = None,
    want_audio: bool = False,
) -> bool:
    """Download a message media using a user account.

    Tries Telethon first (with string session or file-based session),
    then falls back to Pyrogram if a session string is configured.

    Args:
      chat_id: origin chat or bot chat id (int or @username)
      message_id: message id in that chat
      dest_path: destination file path to save to
      msg_date: ISO datetime string of the original message (optional)
      file_unique_id: Telegram Bot API file_unique_id (optional)
      progress_callback: Optional ``(current, total)`` callback for download progress.
      user_id: Optional Telegram user ID for per-user session resolution.
      expected_size: The size Telegram announced for the requested media. Passed to
        the scans so they cannot hand back a *different* message's media.
      want_audio: The requested media is an audio, so a scan candidate without an
        audio stream is not it.

    Returns True on success, False on failure. Raises RuntimeError for missing config.
    """
    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError(
            "Neither Telethon nor Pyrogram are installed. "
            "Install at least one: pip install telethon or pip install pyrogram"
        )

    from utils.telethon_session import (
        get_db_model,
        get_pyrogram_session_string_for_user,
        has_usable_telethon_session_async,
        operating_user_id,
    )

    # A user who never logged in has no session of their own; this fetch is then
    # carried by the deployment's session instead of failing (see
    # ``operating_user_id``).
    user_id = await operating_user_id(user_id, get_db_model())

    pyrogram_session_configured = bool(
        await get_pyrogram_session_string_for_user(user_id=user_id, db_model=get_db_model())
    )

    # Prefer a pre-configured Pyrogram session when available; it avoids
    # interactive Telethon login prompts on server environments.
    if PyrogramClient is not None and pyrogram_session_configured:
        try:
            result = await _download_with_pyrogram(
                chat_id, message_id, dest_path, progress_callback=progress_callback, user_id=user_id
            )
            if result:
                return True
            logger.info("userbot: Pyrogram download failed; trying Telethon fallback")
        except Exception as e:
            logger.warning("userbot: Pyrogram download error (%s); trying Telethon fallback", e)

    # Try Telethon only when a usable session exists (MongoDB included).
    if TelegramClient is not None and await has_usable_telethon_session_async(user_id=user_id, db_model=get_db_model()):
        try:
            result = await _download_with_telethon(
                chat_id,
                message_id,
                dest_path,
                msg_date,
                file_unique_id,
                progress_callback=progress_callback,
                user_id=user_id,
                expected_size=expected_size,
                want_audio=want_audio,
            )
            if result:
                return True
            logger.info("userbot: Telethon download failed; no further fallback")
        except Exception as e:
            logger.warning("userbot: Telethon download error (%s)", e)
    elif TelegramClient is not None:
        logger.info("userbot: Telethon session not configured; skipping Telethon download")

    logger.warning(
        "userbot: all download methods failed for %s/%s (dest=%s)",
        chat_id,
        message_id,
        dest_path,
    )
    return False


async def download_bytes_via_userbot(
    chat_id: int | str,
    message_id: int,
    progress_callback: Callable[[int, int], None] | None = None,
    user_id: int | None = None,
) -> bytes | None:
    """Download a message media into memory (bytes) using userbot.

    Tries Pyrogram with ``in_memory=True`` first to avoid any disk I/O.
    Falls back to Telethon (file-like object) if Pyrogram fails.

    If ``progress_callback`` is provided, it will be called with
    ``(current_bytes, total_bytes)`` during download.

    Returns the file contents as ``bytes`` on success, or ``None`` on failure.

    Args:
        user_id: Optional Telegram user ID for per-user session resolution.
    """
    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError(
            "Neither Telethon nor Pyrogram are installed. "
            "Install at least one: pip install telethon or pip install pyrogram"
        )

    from utils.telethon_session import (
        get_db_model,
        get_pyrogram_session_string,
        get_pyrogram_session_string_for_user,
        has_usable_telethon_session,
        has_usable_telethon_session_async,
        operating_user_id,
    )

    # No session of the user's own: the deployment's session carries the read.
    user_id = await operating_user_id(user_id, get_db_model())

    pyrogram_session_configured = bool(
        await get_pyrogram_session_string_for_user(user_id=user_id, db_model=get_db_model())
    )

    # Try Pyrogram in-memory first
    if PyrogramClient is not None and pyrogram_session_configured:
        try:
            data = await _download_bytes_with_pyrogram(
                chat_id, message_id, progress_callback=progress_callback, user_id=user_id
            )
            if data is not None:
                logger.info(
                    "userbot: in-memory download via Pyrogram succeeded: %d bytes",
                    len(data),
                )
                return data
            logger.info("userbot: Pyrogram in-memory download failed; trying Telethon fallback")
        except Exception as e:
            logger.warning(
                "userbot: Pyrogram in-memory download error (%s); trying Telethon fallback",
                e,
            )

    # Try Telethon with BytesIO as fallback (MongoDB included)
    if TelegramClient is not None and await has_usable_telethon_session_async(user_id=user_id, db_model=get_db_model()):
        try:
            from utils.telethon_session import build_telethon_client
            from utils.telethon_session import get_userbot_credentials as _get_creds

            chunk_size_kb = get_download_chunk_size_kb()
            _api_id, _api_hash = _get_creds()
            _client = build_telethon_client(_api_id, _api_hash)
            if _client is not None:
                await _client.start()
                target = await _normalize_target(chat_id, _client)
                msgs = await _client.get_messages(target, ids=message_id)
                if msgs:
                    msg = msgs[0] if isinstance(msgs, (list, tuple)) else msgs
                    if getattr(msg, "media", None):
                        buf = io.BytesIO()
                        dl_kwargs = {"file": buf, "part_size_kb": chunk_size_kb}
                        if progress_callback is not None:
                            dl_kwargs["progress_callback"] = progress_callback
                        await asyncio.wait_for(
                            _client.download_media(msg, **dl_kwargs),
                            timeout=TELETHON_DOWNLOAD_TIMEOUT,
                        )
                        data = buf.getvalue()
                        if data and len(data) > 0:
                            logger.info(
                                "userbot: in-memory download via Telethon succeeded: %d bytes (chunk_size=%dKB)",
                                len(data),
                                chunk_size_kb,
                            )
                            return data
                await _client.disconnect()
        except Exception as e:
            logger.warning(
                "userbot: Telethon in-memory download error (%s)",
                e,
            )

    logger.warning(
        "userbot: all in-memory download methods failed for %s/%s (Pyrogram session=%s Telethon session=%s)",
        chat_id,
        message_id,
        bool(get_pyrogram_session_string()),
        has_usable_telethon_session(),
    )
    return None
