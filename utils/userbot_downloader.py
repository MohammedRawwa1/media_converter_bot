import os
from typing import Union

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except Exception:  # pragma: no cover - optional dependency
    TelegramClient = None
    StringSession = None


async def download_forward_via_userbot(chat_id: Union[int, str], message_id: int, dest_path: str) -> bool:
    """Download a message media using a user account (Telethon).

    Preferred env vars: `API_ID`, `API_HASH`. Optional: `API_SESSION` or `SESSION` (string session).
    Backwards-compatible with legacy `USERBOT_API_ID`, `USERBOT_API_HASH`, `USERBOT_SESSION`.
    Returns True on success, False on failure.
    """
    if TelegramClient is None:
        raise RuntimeError("Telethon is not installed. Add telethon to requirements and install it.")

    # Prefer concise env names; fall back to legacy names
    api_id = os.getenv("API_ID") or os.getenv("api_id") or os.getenv("USERBOT_API_ID") or os.getenv("userbot_api_id")
    api_hash = os.getenv("API_HASH") or os.getenv("api_hash") or os.getenv("USERBOT_API_HASH") or os.getenv("userbot_api_hash")
    session_str = (
        os.getenv("API_SESSION")
        or os.getenv("SESSION")
        or os.getenv("api_session")
        or os.getenv("USERBOT_SESSION")
        or os.getenv("userbot_session")
    )

    if not api_id or not api_hash:
        raise RuntimeError("API_ID and API_HASH must be set to use userbot fallback")

    try:
        api_id = int(api_id)
    except Exception:
        raise RuntimeError("API_ID must be an integer")

    # Build client (prefer string session if provided)
    if session_str and StringSession is not None:
        client = TelegramClient(StringSession(session_str), api_id, api_hash)
    else:
        # Session name envs: prefer generic names first, then legacy
        session_name = (
            os.getenv("API_SESSION_NAME")
            or os.getenv("SESSION_NAME")
            or os.getenv("USERBOT_SESSION_NAME")
            or "userbot_session"
        )
        client = TelegramClient(session_name, api_id, api_hash)

    await client.start()
    try:
        # Normalize chat id (allow @username or integer)
        target = chat_id
        try:
            if isinstance(chat_id, str) and chat_id.startswith("@"):
                target = chat_id
            else:
                target = int(chat_id)
        except Exception:
            target = chat_id

        msgs = await client.get_messages(target, ids=message_id)
        if not msgs:
            return False

        # Telethon may return a list-like or single Message; normalize
        msg = msgs[0] if isinstance(msgs, (list, tuple)) else msgs
        await client.download_media(msg, file=dest_path)
        return os.path.exists(dest_path)
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
