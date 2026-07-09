import os
import logging
from typing import Union, Optional

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
    chat_id: Union[int, str], file_path: str, caption: Optional[str] = None
) -> bool:
    """Send a file using Telethon."""
    if TelegramClient is None:
        return False

    from utils.telethon_session import build_telethon_client, get_userbot_credentials
    api_id, api_hash = get_userbot_credentials()

    client = build_telethon_client(api_id, api_hash)
    try:
        await client.start()
        target = await _normalize_target(chat_id, client)
        await client.send_file(target, file=file_path, caption=caption)
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
    chat_id: Union[int, str], file_path: str, caption: Optional[str] = None
) -> bool:
    """Send a file using Pyrogram (session string fallback)."""
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
        await client.send_document(target, file_path, caption=caption or "")
        logger.info("userbot: Pyrogram sent file %s to %s", file_path, target)
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
    chat_id: Union[int, str], file_path: str, caption: Optional[str] = None
) -> bool:
    """Send a file using a user account.

    Tries Telethon first, then falls back to Pyrogram if a session string is configured.

    Returns True on success, False on failure. Raises RuntimeError for missing config.
    """
    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError(
            "Neither Telethon nor Pyrogram are installed. "
            "Install at least one: pip install telethon or pip install pyrogram"
        )

    # Try Telethon first
    if TelegramClient is not None:
        try:
            result = await _send_with_telethon(chat_id, file_path, caption)
            if result:
                return True
            logger.info("userbot: Telethon send failed; trying Pyrogram fallback")
        except Exception as e:
            logger.warning("userbot: Telethon send error (%s); trying Pyrogram fallback", e)

    # Fall back to Pyrogram
    if PyrogramClient is not None:
        result = await _send_with_pyrogram(chat_id, file_path, caption)
        if result:
            return True

    logger.warning("userbot: all send methods failed for %s", chat_id)
    return False
