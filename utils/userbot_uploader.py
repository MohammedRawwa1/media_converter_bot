import os
import logging
from typing import Union, Optional

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except Exception:  # pragma: no cover - optional dependency
    TelegramClient = None
    StringSession = None

logger = logging.getLogger(__name__)


async def _normalize_target(chat_id: Union[int, str], client: TelegramClient):
    try:
        if isinstance(chat_id, str) and chat_id.startswith("@"):
            return chat_id
        try:
            return int(chat_id)
        except Exception:
            return chat_id
    except Exception:
        return chat_id


async def send_file_via_userbot(
    chat_id: Union[int, str], file_path: str, caption: Optional[str] = None
) -> bool:
    """Send a file using a user account (Telethon).

    Returns True on success, False on failure. Raises RuntimeError for missing config.
    """
    if TelegramClient is None:
        raise RuntimeError("Telethon is not installed. Add telethon to requirements and install it.")

    api_id = os.getenv("API_ID") or os.getenv("api_id") or os.getenv("USERBOT_API_ID") or os.getenv("userbot_api_id")
    api_hash = os.getenv("API_HASH") or os.getenv("api_hash") or os.getenv("USERBOT_API_HASH") or os.getenv("userbot_api_hash")
    session_str = (
        os.getenv("API_SESSION")
        or os.getenv("SESSION")
        or os.getenv("api_session")
        or os.getenv("USERBOT_SESSION")
        or os.getenv("userbot_session")
        or os.getenv("TELETHON_SESSION")
        or os.getenv("telethon_session")
    )

    if not api_id or not api_hash:
        raise RuntimeError("API_ID and API_HASH must be set to use userbot fallback")

    try:
        api_id = int(api_id)
    except Exception:
        raise RuntimeError("API_ID must be an integer")

    if session_str and StringSession is not None:
        client = TelegramClient(StringSession(session_str), api_id, api_hash)
    else:
        session_name = (
            os.getenv("API_SESSION_NAME")
            or os.getenv("SESSION_NAME")
            or os.getenv("USERBOT_SESSION_NAME")
            or os.getenv("TELETHON_SESSION_NAME")
            or "userbot_session"
        )
        client = TelegramClient(session_name, api_id, api_hash)

    await client.start()
    try:
        target = await _normalize_target(chat_id, client)
        try:
            await client.send_file(target, file=file_path, caption=caption)
            logger.info("userbot: sent file %s to %s", file_path, target)
            return True
        except Exception:
            logger.exception("userbot: failed to send file %s to %s", file_path, target)
            return False
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
