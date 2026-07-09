import os
import logging
from typing import Union, Optional
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
                        await client.download_media(msg, file=dest_path)
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
                                    await client.download_media(m, file=dest_path)
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
                                await client.download_media(m, file=dest_path)
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


async def _download_with_pyrogram(
    chat_id: Union[int, str],
    message_id: int,
    dest_path: str,
) -> bool:
    """Download using Pyrogram client (session string fallback)."""
    if PyrogramClient is None:
        logger.debug("Pyrogram not installed; skipping Pyrogram download")
        return False

    from utils.telethon_session import build_pyrogram_client, get_userbot_credentials
    api_id, api_hash = get_userbot_credentials()

    client = build_pyrogram_client(api_id, api_hash)
    if client is None:
        logger.debug("Pyrogram session string not configured")
        return False

    try:
        await client.start()
        logger.info("userbot: Pyrogram client started for download")

        target = await _normalize_target(chat_id)
        try:
            # Pyrogram uses get_messages differently
            messages = await client.get_messages(target, ids=message_id)
            if messages:
                msg = messages[0] if isinstance(messages, list) else messages
                if msg and getattr(msg, "media", None):
                    logger.info("userbot: Pyrogram downloading %s/%s to %s", target, message_id, dest_path)
                    await client.download_media(msg, file=dest_path)
                    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                        ok = await _ffprobe_ok(dest_path)
                        if ok:
                            return True
        except Exception as e:
            logger.exception("userbot: Pyrogram download failed: %s", e)

        return False
    finally:
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

    # Try Telethon first (traditional)
    if TelegramClient is not None:
        try:
            result = await _download_with_telethon(
                chat_id, message_id, dest_path, msg_date, file_unique_id
            )
            if result:
                return True
            logger.info("userbot: Telethon download failed; trying Pyrogram fallback")
        except Exception as e:
            logger.warning("userbot: Telethon download error (%s); trying Pyrogram fallback", e)

    # Fall back to Pyrogram
    if PyrogramClient is not None:
        result = await _download_with_pyrogram(chat_id, message_id, dest_path)
        if result:
            return True

    logger.warning("userbot: all download methods failed for %s/%s", chat_id, message_id)
    return False
