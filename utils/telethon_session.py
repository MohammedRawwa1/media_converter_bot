import os
import logging
from typing import Optional

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except Exception:  # pragma: no cover - optional dependency
    TelegramClient = None
    StringSession = None

logger = logging.getLogger(__name__)


def _get_env_value(*names: str) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value:
            return value.strip()
    return None


def get_telethon_session_name() -> str:
    return _get_env_value(
        "API_SESSION_NAME",
        "SESSION_NAME",
        "USERBOT_SESSION_NAME",
        "TELETHON_SESSION_NAME",
    ) or "userbot_session"


def get_telethon_session_dir() -> str:
    return _get_env_value("TELETHON_SESSION_DIR") or os.getenv("TEMP_PATH") or os.getcwd()


def get_telethon_session_path() -> str:
    session_dir = get_telethon_session_dir()
    try:
        os.makedirs(session_dir, exist_ok=True)
    except Exception:
        pass
    return os.path.join(session_dir, get_telethon_session_name())


def build_telethon_client(api_id: int, api_hash: str):
    if TelegramClient is None:
        raise RuntimeError("Telethon is not installed. Install telethon to use userbot fallback.")

    session_str = _get_env_value(
        "API_SESSION",
        "SESSION",
        "api_session",
        "USERBOT_SESSION",
        "userbot_session",
        "TELETHON_SESSION",
        "telethon_session",
    )

    if session_str and StringSession is not None:
        try:
            return TelegramClient(StringSession(session_str), api_id, api_hash)
        except Exception:
            logger.exception("Failed to load StringSession from env; falling back to file-based session")

    session_path = get_telethon_session_path()
    return TelegramClient(session_path, api_id, api_hash)
