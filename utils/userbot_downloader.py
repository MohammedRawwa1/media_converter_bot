import io
import os
import logging
import shutil
from typing import Union, Optional, Callable
from datetime import datetime
import asyncio
import subprocess
import json

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


async def _normalize_target(chat_id: Union[int, str], client=None):
    """Return a compatible target entity for `chat_id`."""
    if isinstance(chat_id, str) and chat_id.startswith("@"):
        return chat_id
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return chat_id


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
# Helper: detect -503 Timeout / InternalServerError from any MTProto client
# ---------------------------------------------------------------------------
def _is_503_timeout(exc: Exception) -> bool:
    """Return True when *exc* is a Telegram -503 (internal timeout) error."""
    err_str = str(exc)
    if "503" in err_str and "Timeout" in err_str:
        return True
    if "-503" in err_str:
        return True
    if "InternalServerError" in type(exc).__name__:
        return True
    return False


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
        try:
            return await client.download_media(msg, **dl_kwargs)
        except Exception as exc:
            if not _is_503_timeout(exc):
                raise  # non-timeout error, propagate immediately

            if attempt >= max_retries - 1:
                logger.warning(
                    "userbot: download_media exhausted %d retries (-503): %s",
                    max_retries, exc,
                )
                raise  # last retry exhausted

            wait = delays[min(attempt, len(delays) - 1)]
            logger.warning(
                "userbot: download_media attempt %d/%d failed (-503), "
                "retrying in %ds: %s",
                attempt + 1, max_retries, wait, exc,
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
            pass

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
            pass

    return None, 0


async def _download_bytes_via_raw_api(
    client,
    msg,
    chunk_size_kb: Optional[int] = None,
    progress_callback=None,
) -> Optional[bytes]:
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
        chunk_size_kb, total_size,
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
                        "userbot: raw API chunk at offset %d failed with "
                        "non-retryable error: %s",
                        offset, exc,
                    )
                    return None  # non-retryable, bail out

                if chunk_attempt >= max_chunk_retries - 1:
                    logger.warning(
                        "userbot: raw API chunk at offset %d exhausted %d "
                        "retries (-503): %s",
                        offset, max_chunk_retries, exc,
                    )
                    return None

                wait = chunk_delays[min(chunk_attempt, len(chunk_delays) - 1)]
                logger.warning(
                    "userbot: raw API chunk at offset %d attempt %d/%d, "
                    "retrying in %ds",
                    offset, chunk_attempt + 1, max_chunk_retries, wait,
                )
                await asyncio.sleep(wait)

        if chunk_data is None:
            break

        chunks.append(chunk_data)
        offset += len(chunk_data)

        if progress_callback and total_size > 0:
            try:
                progress_callback(offset, total_size)
            except Exception:
                pass

        # If we received less than the requested limit, it's the last chunk
        if len(chunk_data) < chunk_size:
            break

    if not chunks:
        logger.warning("userbot: raw API download returned no chunks")
        return None

    data = b"".join(chunks)
    logger.info(
        "userbot: raw API download complete: %d bytes in %d chunks (chunk_size=%dKB)",
        len(data), len(chunks), chunk_size_kb,
    )
    return data


# ---------------------------------------------------------------------------
# Recovery helpers: forward to Saved Messages, 1-byte probe, session recycle
# ---------------------------------------------------------------------------

async def _forward_to_saved_messages(client, chat_id, message_id: int) -> Optional[int]:
    """Forward a message to Saved Messages to get a fresh ``file_reference``.

    Telegram assigns a new ``file_reference`` to the forwarded copy, which
    may route to a different (healthy) storage node and bypass persistent
    ``-503 Timeout`` errors on the original.

    Returns the forwarded message ID, or ``None`` on failure.
    """
    try:
        me = await client.get_me()
        forwarded = await client.forward_messages(
            chat_id=me.id,
            from_chat_id=chat_id,
            message_ids=[message_id],
        )
        if forwarded:
            fwd_msg = forwarded[0] if isinstance(forwarded, list) else forwarded
            logger.info(
                "userbot: forwarded %s/%s to Saved Messages -> msg %s",
                chat_id, message_id, getattr(fwd_msg, "id", None),
            )
            return fwd_msg.id
    except Exception as exc:
        logger.warning(
            "userbot: forward to Saved Messages failed for %s/%s: %s",
            chat_id, message_id, exc,
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
            "userbot: 1-byte probe failed (node unresponsive): %s", exc,
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


async def _attempt_recovery_download(
    client,
    chat_id,
    message_id: int,
    dest_path: str,
) -> bool:
    """Attempt to recover a download that failed with persistent ``-503 Timeout``.

    Recovery sequence:
        1. Get the message again (needed for probe / forward).
        2. Try a **1-byte probe** \u2014 if it succeeds immediately retry full download.
        3. Try **forward to Saved Messages** \u2014 gets a fresh ``file_reference``
           that may route to a healthy storage node, then retry download.
        4. Try **session recycling** \u2014 disconnect/reconnect, then retry one more
           time on the original message.

    Returns ``True`` on success, ``False`` if all recovery methods failed.
    """
    from pyrogram import raw

    logger.info(
        "userbot: starting recovery download for %s/%s -> %s",
        chat_id, message_id, dest_path,
    )

    # Resolve the peer (same approach as _download_with_pyrogram)
    target = await _normalize_target(chat_id)
    candidates = [target]
    _bot_token = os.getenv("BOT_TOKEN", "")
    if _bot_token and ":" in _bot_token:
        try:
            _bot_id = int(_bot_token.split(":")[0])
            if _bot_id != target:
                candidates.append(_bot_id)
        except (ValueError, IndexError):
            pass

    # ---- Step 1: Find the message ----
    msg = None
    used_raw_peer = None
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
                        client, channel_peer, message_id,
                    )
                    if _m is not None and getattr(_m, "media", None):
                        msg = _m
                        used_raw_peer = channel_peer
                        break
        except Exception:
            continue

    if msg is None:
        logger.warning(
            "userbot: recovery could not find message %s/%s", chat_id, message_id,
        )
        return False

    # ---- Step 2: 1-byte probe + retry ----
    probe_ok = await _probe_file_wakeup(client, msg)
    if probe_ok:
        logger.info("userbot: recovery \u2014 probe succeeded, retrying download immediately")
        if await _download_and_ensure_path(client, msg, dest_path):
            return True
        logger.info("userbot: recovery \u2014 probe retry still failed, continuing...")
    else:
        logger.info("userbot: recovery \u2014 probe failed, trying forward + retry")

    # ---- Step 3: Forward to Saved Messages + retry with fresh file_reference ----
    fwd_msg_id = None
    if used_raw_peer is not None:
        # Large channel: use raw MTProto forward
        try:
            r = await client.invoke(
                raw.functions.messages.ForwardMessages(
                    from_peer=used_raw_peer,
                    id=[message_id],
                    to_peer=raw.types.InputPeerSelf(),
                    random_id=[client.rnd_id()],
                )
            )
            if r and r.updates:
                for update in r.updates:
                    if hasattr(update, "message") and hasattr(update.message, "id"):
                        fwd_msg_id = update.message.id
                        break
                    if hasattr(update, "id"):
                        fwd_msg_id = update.id
                        break
            if fwd_msg_id is not None:
                # Brief delay so Telegram indexes the forwarded message
                await asyncio.sleep(1)
                logger.info(
                    "userbot: recovery \u2014 raw API forwarded to Saved Messages -> msg %s",
                    fwd_msg_id,
                )
        except Exception as exc:
            logger.warning("userbot: recovery \u2014 raw API forward failed: %s", exc)
    else:
        # Normal peer: use Pyrogram's high-level forward
        fwd_msg_id = await _forward_to_saved_messages(client, target, message_id)
        if fwd_msg_id is not None:
            # Brief delay so Telegram indexes the forwarded message
            await asyncio.sleep(1)

    if fwd_msg_id is not None:
        me = await client.get_me()
        try:
            fwd_msgs = await client.get_messages(me.id, message_ids=[fwd_msg_id])
            if fwd_msgs:
                fwd_msg = fwd_msgs[0] if isinstance(fwd_msgs, list) else fwd_msgs
                if fwd_msg and getattr(fwd_msg, "media", None):
                    logger.info(
                        "userbot: recovery \u2014 retrying download from forwarded copy (%s/%s)",
                        me.id, fwd_msg_id,
                    )
                    if await _download_and_ensure_path(client, fwd_msg, dest_path):
                        return True
        except Exception as exc:
            logger.warning(
                "userbot: recovery \u2014 forward+retry download failed: %s", exc,
            )

    # ---- Step 4: Session recycling + final retry ----
    recycled = await _recycle_client_session(client)
    if recycled:
        logger.info("userbot: recovery \u2014 session recycled, final retry on original msg")
        if await _download_and_ensure_path(client, msg, dest_path):
            return True

    logger.warning(
        "userbot: all recovery methods exhausted for %s/%s", chat_id, message_id,
    )
    return False


async def _download_with_telethon(
    chat_id: Union[int, str],
    message_id: int,
    dest_path: str,
    msg_date: Optional[str] = None,
    file_unique_id: Optional[str] = None,
) -> bool:
    """Download using Telethon client."""
    if TelegramClient is None:
        logger.debug("Telethon not installed; skipping Telethon download")
        return False

    from utils.telethon_session import build_telethon_client, get_userbot_credentials

    chunk_size_kb = get_download_chunk_size_kb()
    api_id, api_hash = get_userbot_credentials()

    client = build_telethon_client(api_id, api_hash)
    try:
        logger.info("userbot: starting Telethon client for download")
        await client.start()
        logger.info("userbot: Telethon client started successfully")
    except Exception as e:
        logger.exception("userbot: failed to start Telethon client: %s", e)
        return False

    try:
        target = await _normalize_target(chat_id, client)

        # Try direct fetch by id first
        try:
            msgs = await client.get_messages(target, ids=message_id)
        except Exception as e:
            logger.exception("userbot: get_messages direct by id failed: %s", e)
            msgs = None

        if msgs:
            msg = msgs[0] if isinstance(msgs, (list, tuple)) else msgs
            if getattr(msg, "media", None):
                logger.info("userbot: message found; downloading %s/%s to %s", target, message_id, dest_path)
                for attempt in range(3):
                    try:
                        logger.debug("userbot: download attempt %s for %s/%s -> %s", attempt + 1, target, getattr(msg, 'id', None), dest_path)
                        await client.download_media(msg, file=dest_path, part_size_kb=chunk_size_kb)
                        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                            ok = await _ffprobe_ok(dest_path)
                            if ok:
                                return True
                        logger.warning("userbot: downloaded file failed validation (attempt %s) %s", attempt + 1, dest_path)
                        try:
                            os.remove(dest_path)
                        except Exception:
                            pass
                    except Exception as e:
                        logger.exception("userbot: download attempt %s failed: %s", attempt + 1, e)
                logger.debug("userbot: message found but downloads failed validation: %s/%s", target, message_id)
            else:
                logger.debug("userbot: message found but no media: %s/%s", target, message_id)

        # Search by date if provided
        search_done = False
        if msg_date:
            try:
                dt = datetime.fromisoformat(msg_date)
            except Exception:
                dt = None
            if dt is not None:
                logger.debug("userbot: searching around date %s in %s", msg_date, target)
                try:
                    async for m in client.iter_messages(target, limit=100, offset_date=dt):
                        if getattr(m, "media", None):
                            for attempt in range(3):
                                try:
                                    await client.download_media(m, file=dest_path, part_size_kb=chunk_size_kb)
                                    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                                        ok = await _ffprobe_ok(dest_path)
                                        if ok:
                                            logger.info("userbot: downloaded via date search to %s", dest_path)
                                            return True
                                except Exception:
                                    pass
                    search_done = True
                except Exception:
                    pass

        # Scan recent messages
        if not search_done:
            try:
                async for m in client.iter_messages(target, limit=200):
                    if getattr(m, "media", None):
                        for attempt in range(3):
                            try:
                                await client.download_media(m, file=dest_path, part_size_kb=chunk_size_kb)
                                if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                                    ok = await _ffprobe_ok(dest_path)
                                    if ok:
                                        return True
                            except Exception:
                                pass
            except Exception:
                pass

        return False
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


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
                id=[raw.types.InputChannel(
                    channel_id=raw_channel_id,
                    access_hash=0,
                )]
            )
        )
        if result and result.chats:
            chat = result.chats[0]
            access_hash = getattr(chat, "access_hash", 0)
            logger.info(
                "userbot: resolved large channel %s -> channel_id=%s access_hash=%s",
                bot_api_chat_id, raw_channel_id, access_hash,
            )
            return raw.types.InputPeerChannel(
                channel_id=raw_channel_id,
                access_hash=access_hash,
            )
    except Exception as e:
        logger.warning(
            "userbot: failed to resolve large channel %s via raw API: %s",
            bot_api_chat_id, e,
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
    client, channel_peer, message_id: int,
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
                client, r.messages[0], users, chats, replies=0,
            )
            return msg
    except Exception as e:
        logger.warning(
            "userbot: GetMessages via raw API failed for msg %s: %s",
            message_id, e,
        )
    return None


async def _download_and_ensure_path(client, msg, dest_path):
    """Download media from *msg* and ensure the file ends up at *dest_path*.

    Pyrogram 2.0.106's ``download_media`` resolves relative paths against
    ``self.PARENT_DIR`` and returns an absolute path.  The caller's ``dest_path``
    is often relative.  This helper reconciles the two.

    Uses ``_download_media_with_retry`` so that transient ``-503 Timeout``
    errors are automatically retried with exponential backoff.

    Returns ``True`` on success, ``False`` otherwise.
    """
    try:
        _dl = await _download_media_with_retry(client, msg, file_name=dest_path)
    except Exception as exc:
        logger.warning(
            "userbot: download_media_with_retry failed for %s: %s",
            dest_path, exc,
        )
        return False

    logger.info(
        "userbot: download_media dest_path=%s returned=%s",
        dest_path, _dl,
    )
    if not _dl:
        logger.warning("userbot: download_media returned None")
        return False

    # If the file was saved to a different path, move it to the expected destination
    _dl_path = str(_dl)
    _abs_dest = os.path.abspath(dest_path)
    if _dl_path != _abs_dest and not os.path.exists(dest_path):
        if os.path.exists(_dl_path):
            logger.info(
                "userbot: moving downloaded file %s -> %s",
                _dl_path, _abs_dest,
            )
            shutil.move(_dl_path, _abs_dest)
        else:
            logger.warning(
                "userbot: download_media returned %s but file does not exist", _dl_path,
            )

    # Check at the absolute destination path (where the file should be)
    if os.path.exists(_abs_dest) and os.path.getsize(_abs_dest) > 0:
        ok = await _ffprobe_ok(_abs_dest)
        if ok:
            return True
        logger.warning(
            "userbot: download succeeded but ffprobe validation failed: %s", _abs_dest,
        )
    else:
        logger.warning(
            "userbot: download_media produced empty/missing file at %s", _abs_dest,
        )
    return False


async def _download_bytes_with_pyrogram(
    chat_id: Union[int, str],
    message_id: int,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Optional[bytes]:
    """Download a message's media into memory (bytes) using Pyrogram.

    Uses ``download_media(..., in_memory=True)`` (with retry on ``-503``)
    to get raw bytes without writing to disk.  Falls back to raw-MTProto
    chunked download when ``DOWNLOAD_CHUNK_SIZE_KB`` is explicitly set
    (see ``_download_bytes_via_raw_api``).

    Returns ``None`` on any failure.

    If ``progress_callback`` is provided, it will be called with
    ``(current_bytes, total_bytes)`` during download.
    """
    if PyrogramClient is None:
        logger.info("userbot: Pyrogram not installed; cannot do in-memory download")
        return None

    from utils.telethon_session import build_pyrogram_client, get_userbot_credentials
    api_id, api_hash = get_userbot_credentials()

    client = build_pyrogram_client(api_id, api_hash)
    if client is None:
        logger.info("userbot: Pyrogram session string not configured; cannot do in-memory download")
        return None

    # Decide whether to use the raw-MTProto chunked path (preferred when
    # DOWNLOAD_CHUNK_SIZE_KB is set explicitly)
    _explicit_chunk_size = None
    _raw_chunk_override = os.getenv("DOWNLOAD_CHUNK_SIZE_KB", "")
    if _raw_chunk_override:
        try:
            _explicit_chunk_size = int(_raw_chunk_override)
        except (TypeError, ValueError):
            pass

    try:
        await client.start()
        logger.info("userbot: Pyrogram client started for in-memory download")

        target = await _normalize_target(chat_id)

        # Collect candidate peers: provided chat_id first, then bot's user ID
        _candidates = [target]
        _bot_token = os.getenv("BOT_TOKEN", "")
        if _bot_token and ":" in _bot_token:
            try:
                _bot_id = int(_bot_token.split(":")[0])
                if _bot_id != target:
                    _candidates.append(_bot_id)
            except (ValueError, IndexError):
                pass

        for _peer in _candidates:
            try:
                logger.info(
                    "userbot: Pyrogram in-memory get_messages(peer=%s, msg=%s)",
                    _peer, message_id,
                )
                messages = await client.get_messages(_peer, message_ids=[message_id])

                if messages:
                    msg = messages[0] if isinstance(messages, list) else messages
                    if msg and getattr(msg, "media", None):
                        logger.info(
                            "userbot: Pyrogram in-memory downloading %s/%s (peer=%s)",
                            _peer, message_id, _peer,
                        )
                        dl_kwargs = {"in_memory": True}
                        if progress_callback is not None:
                            dl_kwargs["progress"] = progress_callback

                        # ── Try raw-MTProto chunked download when chunk size is configured ──
                        if _explicit_chunk_size is not None:
                            raw_data = await _download_bytes_via_raw_api(
                                client, msg,
                                chunk_size_kb=_explicit_chunk_size,
                                progress_callback=progress_callback,
                            )
                            if raw_data is not None:
                                logger.info(
                                    "userbot: raw API chunked download succeeded: %d bytes from %s/%s",
                                    len(raw_data), _peer, message_id,
                                )
                                return raw_data
                            logger.info(
                                "userbot: raw API chunked download failed for %s/%s, "
                                "falling back to download_media",
                                _peer, message_id,
                            )

                        # ── Fallback: download_media with -503 retry ──
                        try:
                            data = await _download_media_with_retry(
                                client, msg, **dl_kwargs,
                            )
                        except Exception as exc:
                            logger.warning(
                                "userbot: in-memory download_media with retry failed "
                                "for %s/%s: %s",
                                _peer, message_id, exc,
                            )
                            data = None

                        if data is not None and isinstance(data, bytes) and len(data) > 0:
                            logger.info(
                                "userbot: Pyrogram in-memory download succeeded: %d bytes from %s/%s",
                                len(data), _peer, message_id,
                            )
                            return data
                        logger.warning(
                            "userbot: Pyrogram in-memory returned empty/invalid data for %s/%s",
                            _peer, message_id,
                        )
                    else:
                        logger.info(
                            "userbot: Pyrogram in-memory msg %s/%s no media (peer=%s)",
                            _peer, message_id, _peer,
                        )
                else:
                    logger.info(
                        "userbot: Pyrogram in-memory get_messages(peer=%s) returned None for msg %s",
                        _peer, message_id,
                    )
            except ValueError as e:
                if "Peer id invalid" in str(e) and isinstance(_peer, int) and _is_large_bot_api_channel(_peer):
                    logger.info(
                        "userbot: large channel ID %s for in-memory, trying raw API", _peer,
                    )
                    channel_peer = await _resolve_bot_api_channel_raw(client, _peer)
                    if channel_peer is not None:
                        msg = await _get_messages_via_raw_channel_api(
                            client, channel_peer, message_id,
                        )
                        if msg is not None and getattr(msg, "media", None):
                            dl_kwargs = {"in_memory": True}
                            if progress_callback is not None:
                                dl_kwargs["progress"] = progress_callback

                            # ── Raw-MTProto chunked download for raw-API resolved messages ──
                            if _explicit_chunk_size is not None:
                                raw_data = await _download_bytes_via_raw_api(
                                    client, msg,
                                    chunk_size_kb=_explicit_chunk_size,
                                    progress_callback=progress_callback,
                                )
                                if raw_data is not None:
                                    logger.info(
                                        "userbot: raw API (large channel) chunked download "
                                        "succeeded: %d bytes",
                                        len(raw_data),
                                    )
                                    return raw_data

                            try:
                                data = await _download_media_with_retry(
                                    client, msg, **dl_kwargs,
                                )
                            except Exception as exc:
                                logger.warning(
                                    "userbot: raw-API resolved download_media failed: %s", exc,
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
                        _peer, message_id, e,
                    )
            except Exception as e:
                logger.warning(
                    "userbot: Pyrogram in-memory error with peer=%s msg=%s: %s",
                    _peer, message_id, e,
                )

        # ---- Final recovery: try recovery for in-memory path via temp file ----
        logger.warning(
            "userbot: Pyrogram in-memory download failed for %s/%s, "
            "trying recovery via temp file...",
            chat_id, message_id,
        )
        _tmp_path = os.path.join(
            os.getenv("TEMP_PATH", "/tmp"),
            f"recovery_{chat_id}_{message_id}.tmp",
        )
        try:
            if await _attempt_recovery_download(client, chat_id, message_id, _tmp_path):
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
                pass

        return None
    finally:
        # Give the internal dispatcher a moment to finish processing any
        # pending updates before closing the SQLite storage, otherwise we get
        # ``sqlite3.ProgrammingError: Cannot operate on a closed database``
        # when ``handle_updates -> fetch_peers -> storage.update_peers``
        # is still in flight.
        try:
            await asyncio.sleep(1)
        except Exception:
            pass
        try:
            await client.stop()
        except Exception:
            pass


async def _download_with_pyrogram(
    chat_id: Union[int, str],
    message_id: int,
    dest_path: str,
) -> bool:
    """Download using Pyrogram client (session string fallback)."""
    if PyrogramClient is None:
        logger.info("userbot: Pyrogram not installed; skipping")
        return False

    from utils.telethon_session import build_pyrogram_client, get_userbot_credentials
    api_id, api_hash = get_userbot_credentials()

    client = build_pyrogram_client(api_id, api_hash)
    if client is None:
        logger.info("userbot: Pyrogram session string not configured")
        return False

    # Ensure dest dir exists
    _dest_dir = os.path.dirname(dest_path)
    if _dest_dir:
        try:
            os.makedirs(_dest_dir, exist_ok=True)
        except Exception as e:
            logger.warning("userbot: could not create dest dir %s: %s", _dest_dir, e)

    try:
        await client.start()
        logger.info("userbot: Pyrogram client started for download")

        target = await _normalize_target(chat_id)

        # Collect candidate peers to try: the provided chat_id first, plus the bot's
        # own user ID (from BOT_TOKEN) as a fallback. This covers the common case
        # where the Bot API reports chat_id = user_id for private chats, but Pyrogram
        # needs the bot's peer ID to resolve the conversation.
        _candidates = [target]
        _bot_token = os.getenv("BOT_TOKEN", "")
        if _bot_token and ":" in _bot_token:
            try:
                _bot_id = int(_bot_token.split(":")[0])
                if _bot_id != target:
                    _candidates.append(_bot_id)
            except (ValueError, IndexError):
                pass

        _found_msg = False
        for _peer in _candidates:
            try:
                logger.info(
                    "userbot: Pyrogram trying get_messages(peer=%s, msg=%s)",
                    _peer, message_id,
                )
                messages = await client.get_messages(_peer, message_ids=[message_id])

                if messages:
                    msg = messages[0] if isinstance(messages, list) else messages
                    if msg:
                        _found_msg = True
                        logger.info(
                            "userbot: Pyrogram get_messages returned msg id=%s peer=%s has_media=%s",
                            getattr(msg, "id", None),
                            _peer,
                            bool(getattr(msg, "media", None)),
                        )
                    if msg and getattr(msg, "media", None):
                        logger.info(
                            "userbot: Pyrogram downloading %s/%s -> %s (peer=%s)",
                            _peer, message_id, dest_path, _peer,
                        )
                        if await _download_and_ensure_path(client, msg, dest_path):
                            return True
                        logger.warning(
                            "userbot: Pyrogram download failed for %s/%s (peer=%s)",
                            _peer, message_id, _peer,
                        )
                        # File was found but download failed — break out to avoid re-downloading
                        # from another peer (the message is correct, download itself failed)
                        break
                    else:
                        logger.info(
                            "userbot: Pyrogram message %s/%s found but has no media (peer=%s)",
                            _peer, message_id, _peer,
                        )
                else:
                    logger.info(
                        "userbot: Pyrogram get_messages(peer=%s) returned None/empty for msg %s",
                        _peer, message_id,
                    )
            except ValueError as e:
                err_str = str(e)
                if "Peer id invalid" in err_str and isinstance(_peer, int) and _is_large_bot_api_channel(_peer):
                    # Pyrogram's get_peer_type range check rejects this channel ID.
                    # Retry using raw MTProto API.
                    logger.info(
                        "userbot: large channel ID %s, retrying via raw API", _peer,
                    )
                    channel_peer = await _resolve_bot_api_channel_raw(client, _peer)
                    if channel_peer is not None:
                        msg = await _get_messages_via_raw_channel_api(
                            client, channel_peer, message_id,
                        )
                        if msg is not None:
                            _found_msg = True
                            if getattr(msg, "media", None):
                                logger.info(
                                    "userbot: raw API got msg %s with media, downloading...",
                                    message_id,
                                )
                                if await _download_and_ensure_path(client, msg, dest_path):
                                    return True
                                logger.warning(
                                    "userbot: raw API download failed validation for %s/%s",
                                    _peer, message_id,
                                )
                            else:
                                logger.info(
                                    "userbot: raw API msg %s/%s has no media",
                                    _peer, message_id,
                                )
                        else:
                            logger.warning(
                                "userbot: raw API returned no message for %s/%s",
                                _peer, message_id,
                            )
                else:
                    logger.warning(
                        "userbot: Pyrogram error with peer=%s msg=%s: %s",
                        _peer, message_id, e,
                    )
            except Exception as e:
                logger.warning(
                    "userbot: Pyrogram error with peer=%s msg=%s: %s",
                    _peer, message_id, e,
                )

        async def _try_large_channel(peer):
            """Try downloading from a large Bot API channel ID using raw MTProto.
            Returns True on success, False if peer not applicable, or None."""
            if not _is_large_bot_api_channel(peer):
                return False
            logger.info(
                "userbot: large channel ID %s, trying raw API", peer,
            )
            channel_peer = await _resolve_bot_api_channel_raw(client, peer)
            if channel_peer is None:
                return None
            msg = await _get_messages_via_raw_channel_api(
                client, channel_peer, message_id,
            )
            if msg is None or not getattr(msg, "media", None):
                return None
            if await _download_and_ensure_path(client, msg, dest_path):
                return True
            return None

        # Fallback: scan the recent history of each candidate peer for a matching media message.
        for _peer in _candidates:
            try:
                logger.info(
                    "userbot: Pyrogram scanning history of peer=%s for msg=%s (fallback)",
                    _peer, message_id,
                )
                async for msg in client.get_chat_history(_peer, limit=50):
                    if getattr(msg, "id", None) == message_id and getattr(msg, "media", None):
                        logger.info(
                            "userbot: Pyrogram found msg %s/%s in history (peer=%s)",
                            _peer, message_id, _peer,
                        )
                        if await _download_and_ensure_path(client, msg, dest_path):
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
                    "userbot: Pyrogram history scan(peer=%s) failed: %s", _peer, e,
                )

        # Final attempt: try get_chat to resolve peer properly, then retry get_messages
        if not _found_msg:
            for _peer in _candidates:
                try:
                    logger.info(
                        "userbot: Pyrogram resolving peer=%s via get_chat() for msg %s",
                        _peer, message_id,
                    )
                    _chat = await client.get_chat(_peer)
                    if _chat:
                        _resolved_id = getattr(_chat, "id", _peer)
                        logger.info(
                            "userbot: Pyrogram resolved chat peer=%s -> id=%s",
                            _peer, _resolved_id,
                        )
                        messages = await client.get_messages(_resolved_id, message_ids=[message_id])
                        if messages:
                            msg = messages[0] if isinstance(messages, list) else messages
                            if msg and getattr(msg, "media", None):
                                _found_msg = True
                                if await _download_and_ensure_path(client, msg, dest_path):
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
                        _peer, e,
                    )

        if not _found_msg:
            logger.warning(
                "userbot: Pyrogram could not find message %s in any candidate peer (%s)",
                message_id, _candidates,
            )

        # ---- Final recovery: attempt advanced techniques for persistent -503 ----
        _abs_dest = os.path.abspath(dest_path)
        if not os.path.exists(_abs_dest) or os.path.getsize(_abs_dest) == 0:
            logger.info(
                "userbot: all standard download methods failed for %s/%s, "
                "trying recovery (forward+probe+recycle)...",
                chat_id, message_id,
            )
            if await _attempt_recovery_download(client, chat_id, message_id, dest_path):
                logger.info(
                    "userbot: recovery download succeeded for %s/%s",
                    chat_id, message_id,
                )
                return True

        return False
    finally:
        # Give the internal dispatcher a moment to finish processing any
        # pending updates before closing the SQLite storage, otherwise we get
        # ``sqlite3.ProgrammingError: Cannot operate on a closed database``
        # when ``handle_updates -> fetch_peers -> storage.update_peers``
        # is still in flight.
        try:
            await asyncio.sleep(1)
        except Exception:
            pass
        try:
            await client.stop()
        except Exception:
            pass


async def download_forward_via_userbot(
    chat_id: Union[int, str],
    message_id: int,
    dest_path: str,
    msg_date: Optional[str] = None,
    file_unique_id: Optional[str] = None,
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

    Returns True on success, False on failure. Raises RuntimeError for missing config.
    """
    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError(
            "Neither Telethon nor Pyrogram are installed. "
            "Install at least one: pip install telethon or pip install pyrogram"
        )

    from utils.telethon_session import (
        get_pyrogram_session_string,
        has_usable_telethon_session,
    )

    pyrogram_session_configured = bool(get_pyrogram_session_string())

    # Prefer a pre-configured Pyrogram session when available; it avoids
    # interactive Telethon login prompts on server environments.
    if PyrogramClient is not None and pyrogram_session_configured:
        try:
            result = await _download_with_pyrogram(chat_id, message_id, dest_path)
            if result:
                return True
            logger.info("userbot: Pyrogram download failed; trying Telethon fallback")
        except Exception as e:
            logger.warning("userbot: Pyrogram download error (%s); trying Telethon fallback", e)

    # Try Telethon only when a usable session exists.
    if TelegramClient is not None and has_usable_telethon_session():
        try:
            result = await _download_with_telethon(
                chat_id, message_id, dest_path, msg_date, file_unique_id
            )
            if result:
                return True
            logger.info("userbot: Telethon download failed; no further fallback")
        except Exception as e:
            logger.warning("userbot: Telethon download error (%s)", e)
    elif TelegramClient is not None:
        logger.info("userbot: Telethon session not configured; skipping Telethon download")

    logger.warning("userbot: all download methods failed for %s/%s", chat_id, message_id)
    return False


async def download_bytes_via_userbot(
    chat_id: Union[int, str],
    message_id: int,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Optional[bytes]:
    """Download a message media into memory (bytes) using userbot.

    Tries Pyrogram with ``in_memory=True`` first to avoid any disk I/O.
    Falls back to Telethon (file-like object) if Pyrogram fails.

    If ``progress_callback`` is provided, it will be called with
    ``(current_bytes, total_bytes)`` during download.

    Returns the file contents as ``bytes`` on success, or ``None`` on failure.
    """
    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError(
            "Neither Telethon nor Pyrogram are installed. "
            "Install at least one: pip install telethon or pip install pyrogram"
        )

    from utils.telethon_session import (
        get_pyrogram_session_string,
        has_usable_telethon_session,
    )

    pyrogram_session_configured = bool(get_pyrogram_session_string())

    # Try Pyrogram in-memory first
    if PyrogramClient is not None and pyrogram_session_configured:
        try:
            data = await _download_bytes_with_pyrogram(chat_id, message_id, progress_callback=progress_callback)
            if data is not None:
                logger.info(
                    "userbot: in-memory download via Pyrogram succeeded: %d bytes",
                    len(data),
                )
                return data
            logger.info("userbot: Pyrogram in-memory download failed; trying Telethon fallback")
        except Exception as e:
            logger.warning(
                "userbot: Pyrogram in-memory download error (%s); trying Telethon fallback", e,
            )

    # Try Telethon with BytesIO as fallback
    if TelegramClient is not None and has_usable_telethon_session():
        try:
            from utils.telethon_session import build_telethon_client, get_userbot_credentials as _get_creds

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
                        await _client.download_media(msg, **dl_kwargs)
                        data = buf.getvalue()
                        if data and len(data) > 0:
                            logger.info(
                                "userbot: in-memory download via Telethon succeeded: %d bytes (chunk_size=%dKB)",
                                len(data), chunk_size_kb,
                            )
                            return data
                await _client.disconnect()
        except Exception as e:
            logger.warning(
                "userbot: Telethon in-memory download error (%s)", e,
            )

    logger.warning(
        "userbot: all in-memory download methods failed for %s/%s",
        chat_id, message_id,
    )
    return None
