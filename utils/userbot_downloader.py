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

logger = logging.getLogger(__name__)


async def _normalize_target(chat_id: Union[int, str], client: TelegramClient):
    """Return a Telethon-compatible target entity for `chat_id`."""
    try:
        if isinstance(chat_id, str) and chat_id.startswith("@"):
            return chat_id
        try:
            return int(chat_id)
        except Exception:
            return chat_id
    except Exception:
        return chat_id


async def download_forward_via_userbot(
    chat_id: Union[int, str],
    message_id: int,
    dest_path: str,
    msg_date: Optional[str] = None,
    file_unique_id: Optional[str] = None,
) -> bool:
    """Download a message media using a user account (Telethon).

    Args:
      chat_id: origin chat or bot chat id (int or @username)
      message_id: message id in that chat
      dest_path: destination file path to save to
      msg_date: ISO datetime string of the original message (optional)
      file_unique_id: Telegram Bot API file_unique_id (optional)

    Returns True on success, False on failure. Raises RuntimeError for missing config.
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
        session_name = (
            os.getenv("API_SESSION_NAME")
            or os.getenv("SESSION_NAME")
            or os.getenv("USERBOT_SESSION_NAME")
            or "userbot_session"
        )
        client = TelegramClient(session_name, api_id, api_hash)

    # Start client
    try:
        # Log session details (do not log secrets)
        logger.info("userbot: starting Telethon client session_name=%s string_session_provided=%s", session_name if 'session_name' in locals() else None, bool(session_str))
        await client.start()
        logger.info("userbot: Telethon client started successfully")
    except Exception as e:
        logger.exception("userbot: failed to start Telethon client: %s", e)
        raise
    try:
        target = await _normalize_target(chat_id, client)

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
                # Attempt download with basic retry and verification
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

        # If direct id lookup failed, try searching around the message date if provided
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
                                    logger.debug("userbot: failed validation during date search download (attempt %s)", attempt + 1)
                                    try:
                                        os.remove(dest_path)
                                    except Exception:
                                        pass
                                except Exception:
                                    logger.debug("userbot: failed download during date search for msg %s", getattr(m, 'id', None))
                    search_done = True
                except Exception:
                    logger.debug("userbot: date-based search failed for %s", target)

        # Finally, try scanning recent messages in target for first media-containing message
        if not search_done:
            try:
                logger.debug("userbot: scanning recent messages in %s for media", target)
                async for m in client.iter_messages(target, limit=200):
                    if getattr(m, "media", None):
                        for attempt in range(3):
                            try:
                                logger.debug("userbot: recent-scan download attempt %s for msg %s -> %s", attempt + 1, getattr(m, 'id', None), dest_path)
                                await client.download_media(m, file=dest_path)
                                if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
                                    ok = await _ffprobe_ok(dest_path)
                                    if ok:
                                        logger.info("userbot: downloaded during recent-scan to %s", dest_path)
                                        return True
                                logger.debug("userbot: download failed validation for recent message %s (attempt %s)", getattr(m, 'id', None), attempt + 1)
                                try:
                                    os.remove(dest_path)
                                except Exception:
                                    pass
                            except Exception:
                                logger.debug("userbot: download failed for recent message %s", getattr(m, 'id', None))
            except Exception:
                logger.debug("userbot: recent-scan failed for %s", target)

        logger.debug("userbot: no media found or valid download for %s/%s", target, message_id)
        return False
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
