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
                        # download_media returns the file path on success, None on failure
                        _dl_result = await client.download_media(msg, file=dest_path)
                        logger.info(
                            "userbot: Pyrogram download_media returned: %s",
                            _dl_result,
                        )
                        if _dl_result and os.path.exists(_dl_result) and os.path.getsize(_dl_result) > 0:
                            ok = await _ffprobe_ok(_dl_result)
                            if ok:
                                return True
                            logger.warning(
                                "userbot: Pyrogram download_media succeeded but ffprobe validation failed: %s",
                                _dl_result,
                            )
                        else:
                            logger.warning(
                                "userbot: Pyrogram download_media failed or produced empty file for %s/%s (peer=%s): result=%s",
                                _peer, message_id, _peer, _dl_result,
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
            except Exception as e:
                logger.warning(
                    "userbot: Pyrogram error with peer=%s msg=%s: %s",
                    _peer, message_id, e,
                )

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
                        _dl_result = await client.download_media(msg, file=dest_path)
                        if _dl_result and os.path.exists(_dl_result) and os.path.getsize(_dl_result) > 0:
                            ok = await _ffprobe_ok(_dl_result)
                            if ok:
                                return True
                        break
            except Exception as e:
                logger.warning(
                    "userbot: Pyrogram history scan(peer=%s) failed: %s", _peer, e,
                )

        # Final attempt: try get_chat to resolve peer properly, then retry get_messages
        if not _found_msg:
            # The message wasn't found by any method. Try resolving the peer via get_chat first.
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
                                _dl_result = await client.download_media(msg, file=dest_path)
                                if _dl_result and os.path.exists(_dl_result) and os.path.getsize(_dl_result) > 0:
                                    ok = await _ffprobe_ok(_dl_result)
                                    if ok:
                                        return True
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
