import os
import logging
from typing import Union, Optional, Callable

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
    try:
        if isinstance(chat_id, str) and chat_id.startswith("@"):
            return chat_id
        try:
            return int(chat_id)
        except Exception:
            return chat_id
    except Exception:
        return chat_id


async def _send_with_telethon(
    chat_id: Union[int, str], file_path: str, caption: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> bool:
    """Send a file using Telethon.

    Args:
        chat_id: Target chat ID or username.
        file_path: Path to the file to send.
        caption: Optional caption text.
        progress_callback: Optional callable(sent_bytes, total_bytes) for upload progress.
    """
    if TelegramClient is None:
        return False

    from utils.telethon_session import build_telethon_client, get_userbot_credentials, has_usable_telethon_session

    # Fail fast if no usable Telethon session is available — avoids
    # client.start() prompting for a phone number on stdin (EOFError).
    if not has_usable_telethon_session():
        logger.info("userbot: Telethon session not configured; skipping Telethon upload")
        return False

    api_id, api_hash = get_userbot_credentials()

    client = build_telethon_client(api_id, api_hash)
    try:
        # Pass a phone callback that raises instead of prompting stdin.
        async def _no_phone():
            raise RuntimeError("Telethon phone prompt unexpectedly triggered")
        await client.start(phone=_no_phone)
        target = await _normalize_target(chat_id, client)
        kwargs = {"file": file_path, "caption": caption}
        if progress_callback is not None:
            kwargs["progress_callback"] = progress_callback
        await client.send_file(target, **kwargs)
        logger.info("userbot: Telethon sent file %s to %s", file_path, target)
        return True
    except Exception:
        logger.exception("userbot: Telethon failed to send file %s", file_path)
        return False
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def _send_with_pyrogram(
    chat_id: Union[int, str], file_path: str, caption: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> bool:
    """Send a file using Pyrogram (session string fallback).

    Args:
        chat_id: Target chat ID or username.
        file_path: Path to the file to send.
        caption: Optional caption text.
        progress_callback: Optional callable(current, total) for upload progress.
                           Pyrogram progress callback is synchronous.
    """
    if PyrogramClient is None:
        return False

    from utils.telethon_session import build_pyrogram_client, get_userbot_credentials
    api_id, api_hash = get_userbot_credentials()

    client = build_pyrogram_client(api_id, api_hash)
    if client is None:
        return False

    try:
        await client.start()
        target = await _normalize_target(chat_id)
        kwargs = {"caption": caption or "", "supports_streaming": True}
        if progress_callback is not None:
            kwargs["progress"] = progress_callback
        await client.send_video(target, file_path, **kwargs)
        logger.info("userbot: Pyrogram sent video %s to %s", file_path, target)
        return True
    except Exception:
        logger.exception("userbot: Pyrogram failed to send file %s", file_path)
        return False
    finally:
        try:
            await client.stop()
        except Exception:
            pass


async def send_file_via_userbot(
    chat_id: Union[int, str], file_path: str, caption: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> bool:
    """Send a file using a user account.

    Tries Telethon first (when a session is available), then falls back to
    Pyrogram if a session string is configured. Fails fast without connecting
    to Telegram when no session is configured.

    Args:
        chat_id: Target chat ID or username.
        file_path: Path to the file to send.
        caption: Optional caption text.
        progress_callback: Optional callable(sent_bytes, total_bytes) for upload progress.
                           Both Telethon and Pyrogram callbacks follow this signature.

    Returns True on success, False on failure. Raises RuntimeError for missing config.
    """
    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError(
            "Neither Telethon nor Pyrogram are installed. "
            "Install at least one: pip install telethon or pip install pyrogram"
        )

    from utils.telethon_session import has_usable_telethon_session

    # Try Telethon first only when a usable session exists.
    if TelegramClient is not None and has_usable_telethon_session():
        try:
            result = await _send_with_telethon(chat_id, file_path, caption, progress_callback=progress_callback)
            if result:
                return True
            logger.info("userbot: Telethon send failed; trying Pyrogram fallback")
        except Exception as e:
            logger.warning("userbot: Telethon send error (%s); trying Pyrogram fallback", e)
    elif TelegramClient is not None:
        logger.info("userbot: Telethon session not configured; skipping Telethon upload")

    # Fall back to Pyrogram (requires PYROGRAM_SESSION env var)
    if PyrogramClient is not None:
        result = await _send_with_pyrogram(chat_id, file_path, caption, progress_callback=progress_callback)
        if result:
            return True

    logger.warning("userbot: all send methods failed for %s", chat_id)
    return False
