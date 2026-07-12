import os
import json
import asyncio
import logging
import shutil
import tempfile
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


async def _probe_video_metadata(path: str) -> dict:
    """Probe a video file with ffprobe and return parsed metadata dict.

    Returns a dict with keys: duration (int seconds), width (int), height (int).
    Missing or unreadable keys are omitted. Returns empty dict on any failure.
    """
    ffprobe_bin = "ffprobe"
    try:
        proc = await asyncio.create_subprocess_exec(
            ffprobe_bin, "-v", "quiet", "-print_format", "json",
            "-show_entries", "stream=width,height,codec_type:format=duration",
            path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        if proc.returncode != 0:
            return {}
        data = json.loads(stdout.decode())
    except Exception:
        return {}

    meta = {}
    # First video stream carries the dimensions
    streams = data.get("streams", [])
    for s in streams:
        if s.get("codec_type") == "video":
            if "width" in s:
                meta["width"] = s["width"]
            if "height" in s:
                meta["height"] = s["height"]
            break
    # Duration from format section
    fmt = data.get("format", {})
    if fmt.get("duration"):
        try:
            meta["duration"] = int(float(fmt["duration"]))
        except (ValueError, TypeError):
            pass
    return meta


async def _generate_video_thumbnail(path: str) -> Optional[str]:
    """Extract a single frame thumbnail from the video at ~1 second mark.

    Returns the path to a JPEG thumbnail file, or None on failure.
    The caller is responsible for cleaning up the returned file.
    """
    ffmpeg_bin = "ffmpeg"
    # Use a named temp file so we can return the path
    tmp_dir = tempfile.mkdtemp(prefix="pyro_thumb_")
    thumb_path = os.path.join(tmp_dir, "thumb.jpg")
    try:
        proc = await asyncio.create_subprocess_exec(
            ffmpeg_bin, "-y", "-ss", "00:00:01", "-i", path,
            "-vframes", "1", "-q:v", "2", thumb_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(proc.communicate(), timeout=30)
        if proc.returncode == 0 and os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
            # Keep the temp dir; caller must clean up
            return thumb_path
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None


async def _send_with_pyrogram(
    chat_id: Union[int, str], file_path: str, caption: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> bool:
    """Send a file using Pyrogram (session string fallback).

    Probes the video for duration / dimensions and extracts a thumbnail
    frame so the resulting Telegram message shows proper metadata instead
    of a "violet" unknown-video placeholder.

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

    # Pre-fetch video metadata and thumbnail before connecting to Telegram
    # so we can bail early if the file is problematic.
    video_meta = await _probe_video_metadata(file_path)
    thumb_path = await _generate_video_thumbnail(file_path)

    _temp_cleanup = None
    if thumb_path:
        _temp_cleanup = os.path.dirname(thumb_path)

    try:
        await client.start()
        target = await _normalize_target(chat_id)
        kwargs = {
            "caption": caption or "",
            "supports_streaming": True,
        }
        if progress_callback is not None:
            kwargs["progress"] = progress_callback

        # Pass probed metadata so Telegram displays proper video info
        if "duration" in video_meta:
            kwargs["duration"] = video_meta["duration"]
        if "width" in video_meta:
            kwargs["width"] = video_meta["width"]
        if "height" in video_meta:
            kwargs["height"] = video_meta["height"]
        if thumb_path is not None:
            kwargs["thumb"] = thumb_path

        await client.send_video(target, file_path, **kwargs)
        logger.info(
            "userbot: Pyrogram sent video %s to %s (meta=%s, thumb=%s)",
            file_path, target, video_meta, bool(thumb_path),
        )
        return True
    except Exception:
        logger.exception("userbot: Pyrogram failed to send file %s", file_path)
        return False
    finally:
        # Clean up temp thumbnail directory
        if _temp_cleanup:
            try:
                shutil.rmtree(_temp_cleanup, ignore_errors=True)
            except Exception:
                pass
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
