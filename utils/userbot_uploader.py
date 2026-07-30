import asyncio
import contextlib
import json
import logging
import os
import tempfile
from collections.abc import Callable

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

from utils.file_utils import safe_rmtree

logger = logging.getLogger(__name__)

# ── Cached Pyrogram bot client (reused across sends to avoid reconnect overhead) ──
_BOT_CACHE_LOCK: asyncio.Lock | None = None
_BOT_CACHED_CLIENT: tuple | None = None  # (client, api_id, api_hash, bot_token)


async def _get_cached_bot_client(api_id: int, api_hash: str, bot_token: str):
    """Return a cached Pyrogram bot client, creating or reconnecting if needed.

    The client is created once and reused for all subsequent ``_send_with_pyrogram_bot``
    calls, avoiding the ~5s connection/auth overhead per file.

    Thread-safe via ``_BOT_CACHE_LOCK`` (``asyncio.Lock``).
    """
    global _BOT_CACHE_LOCK, _BOT_CACHED_CLIENT

    # Lazily initialise the lock on first call (module import may not have a running loop)
    if _BOT_CACHE_LOCK is None:
        _BOT_CACHE_LOCK = asyncio.Lock()

    # Fast path: cached client is still connected
    cached = _BOT_CACHED_CLIENT
    if cached is not None:
        client, c_id, c_hash, c_token = cached
        if c_id == api_id and c_hash == api_hash and c_token == bot_token:
            if client.is_connected:
                return client
            logger.info("userbot: Pyrogram bot client disconnected; recreating")

    # Slow path: create a new client under the lock
    async with _BOT_CACHE_LOCK:
        # Double-check after acquiring lock
        cached = _BOT_CACHED_CLIENT
        if cached is not None:
            client, c_id, c_hash, c_token = cached
            if c_id == api_id and c_hash == api_hash and c_token == bot_token and client.is_connected:
                return client

        # Stop any previous orphaned client before replacing it
        if _BOT_CACHED_CLIENT is not None:
            old_client = _BOT_CACHED_CLIENT[0]
            with contextlib.suppress(Exception):
                await old_client.stop()

        client = PyrogramClient(
            "bot_sender",
            api_id=api_id,
            api_hash=api_hash,
            bot_token=bot_token,
            in_memory=True,
        )
        await client.start()
        _BOT_CACHED_CLIENT = (client, api_id, api_hash, bot_token)
        logger.info("userbot: Created new cached Pyrogram bot client")
        return client


# ── Cached Telethon client (reused across sends) ──
_TELETHON_CACHE_LOCK: asyncio.Lock | None = None
_TELETHON_CACHED_CLIENT: tuple | None = None  # (client, api_id, api_hash)


async def _get_cached_telethon_client(api_id: int, api_hash: str):
    """Return a cached Telethon client, creating or reconnecting if needed."""
    from utils.telethon_session import build_telethon_client

    global _TELETHON_CACHE_LOCK, _TELETHON_CACHED_CLIENT

    if _TELETHON_CACHE_LOCK is None:
        _TELETHON_CACHE_LOCK = asyncio.Lock()

    # Fast path: cached client still connected
    cached = _TELETHON_CACHED_CLIENT
    if cached is not None:
        client, c_id, c_hash = cached
        if c_id == api_id and c_hash == api_hash:
            if client.is_connected():
                return client
            logger.info("userbot: Telethon client disconnected; recreating")

    # Slow path: create new client under lock
    async with _TELETHON_CACHE_LOCK:
        cached = _TELETHON_CACHED_CLIENT
        if cached is not None:
            client, c_id, c_hash = cached
            if c_id == api_id and c_hash == api_hash and client.is_connected():
                return client

        # Stop old orphaned client before replacing
        if _TELETHON_CACHED_CLIENT is not None:
            old = _TELETHON_CACHED_CLIENT[0]
            with contextlib.suppress(Exception):
                await old.disconnect()

        client = build_telethon_client(api_id, api_hash)

        async def _no_phone():
            raise RuntimeError("Telethon phone prompt unexpectedly triggered")

        await client.start(phone=_no_phone)
        _TELETHON_CACHED_CLIENT = (client, api_id, api_hash)
        logger.info("userbot: Created new cached Telethon client")
        return client


# ── Cached Pyrogram user client (reused across sends) ──
_PYRO_USER_CACHE_LOCK: asyncio.Lock | None = None
_PYRO_USER_CACHED_CLIENT: tuple | None = None  # (client, api_id, api_hash)


async def _get_cached_pyrogram_user_client(api_id: int, api_hash: str):
    """Return a cached Pyrogram user client, creating or reconnecting if needed."""
    from utils.telethon_session import build_pyrogram_client

    global _PYRO_USER_CACHE_LOCK, _PYRO_USER_CACHED_CLIENT

    if _PYRO_USER_CACHE_LOCK is None:
        _PYRO_USER_CACHE_LOCK = asyncio.Lock()

    # Fast path: cached client still connected
    cached = _PYRO_USER_CACHED_CLIENT
    if cached is not None:
        client, c_id, c_hash = cached
        if c_id == api_id and c_hash == api_hash:
            if client.is_connected:
                return client
            logger.info("userbot: Pyrogram user client disconnected; recreating")

    # Slow path: create new client under lock
    async with _PYRO_USER_CACHE_LOCK:
        cached = _PYRO_USER_CACHED_CLIENT
        if cached is not None:
            client, c_id, c_hash = cached
            if c_id == api_id and c_hash == api_hash and client.is_connected:
                return client

        # Stop old orphaned client before replacing
        if _PYRO_USER_CACHED_CLIENT is not None:
            old = _PYRO_USER_CACHED_CLIENT[0]
            with contextlib.suppress(Exception):
                await old.stop()

        client = build_pyrogram_client(api_id, api_hash)
        if client is None:
            return None
        await client.start()
        _PYRO_USER_CACHED_CLIENT = (client, api_id, api_hash)
        logger.info("userbot: Created new cached Pyrogram user client")
        return client


# ── Parallel upload constants (FastTelethon-style) ──
# Part size for Telegram file uploads: 512 KB (standard for big files)
_PARALLEL_PART_SIZE: int = 512 * 1024
# Max concurrent chunk uploads — 4-8 is the sweet spot; more triggers FloodWait
_PARALLEL_WORKERS: int = 6
# Files larger than this use SaveBigFilePart (vs SaveFilePart)
_PARALLEL_BIG_FILE_THRESHOLD: int = 10 * 1024 * 1024
# Max file size for in-memory parallel upload; beyond this we fall back to
# sequential upload to avoid OOM from reading the entire file into memory.
# Set to 0 or negative to disable the guard.
_PARALLEL_MAX_MEMORY_BYTES: int = 500 * 1024 * 1024  # 500 MB


async def _parallel_upload_file(
    client,
    file_path: str,
    file_size: int,
    progress_callback: Callable[[int, int], None] | None = None,
    part_size: int = _PARALLEL_PART_SIZE,
    workers: int = _PARALLEL_WORKERS,
):
    """Upload a file to Telegram using parallel chunked upload (FastTelethon-style).

    Splits the file into fixed-size parts and uploads them concurrently via
    Telethon's low-level ``SaveBigFilePartRequest`` (or ``SaveFilePartRequest``
    for small files). This is significantly faster than the sequential upload
    used by ``client.send_file()`` because it saturates the connection with
    multiple in-flight parts.

    Args:
        client: Connected Telethon client.
        file_path: Path to the file to upload.
        file_size: Total file size in bytes.
        progress_callback: Optional ``callable(sent_bytes, total_bytes)``.
        part_size: Chunk size in bytes (default 512 KB).
        workers: Max concurrent uploads (default 6).

    Returns:
        ``InputFileBig`` for large files or ``InputFile`` for small files,
        ready to pass to ``client.send_file()`` as the ``file`` argument.
    """
    # ── Memory guard: avoid loading huge files entirely into RAM ──
    if _PARALLEL_MAX_MEMORY_BYTES > 0 and file_size > _PARALLEL_MAX_MEMORY_BYTES:
        logger.warning(
            "parallel_upload: file %s size %d exceeds memory guard %d — falling back to sequential",
            file_path,
            file_size,
            _PARALLEL_MAX_MEMORY_BYTES,
        )
        return None

    import random

    from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
    from telethon.tl.types import InputFile, InputFileBig

    file_id = random.randrange(1 << 63)  # noqa: S311
    total_parts = max(1, (file_size + part_size - 1) // part_size)
    is_big = total_parts > 1024 or file_size > _PARALLEL_BIG_FILE_THRESHOLD
    sem = asyncio.Semaphore(workers)
    sent_bytes = 0

    async def _upload_part(part_index: int, data: bytes) -> None:
        nonlocal sent_bytes
        async with sem:
            for attempt in range(3):
                try:
                    if is_big:
                        await client(
                            SaveBigFilePartRequest(
                                file_id=file_id,
                                file_part=part_index,
                                file_total_parts=total_parts,
                                bytes=data,
                            )
                        )
                    else:
                        await client(
                            SaveFilePartRequest(
                                file_id=file_id,
                                file_part=part_index,
                                bytes=data,
                            )
                        )
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(1 * (attempt + 1))

            sent_bytes += len(data)
            if progress_callback:
                progress_callback(sent_bytes, file_size)

    # Read entire file into memory for chunking
    with open(file_path, "rb") as f:
        file_data = f.read()

    chunks = []
    for i in range(total_parts):
        start = i * part_size
        end = min(start + part_size, len(file_data))
        chunks.append((i, file_data[start:end]))

    logger.info(
        "parallel_upload: file=%s size=%d parts=%d workers=%d is_big=%s",
        file_path,
        file_size,
        total_parts,
        workers,
        is_big,
    )

    await asyncio.gather(*[_upload_part(i, d) for i, d in chunks])

    filename = os.path.basename(file_path)
    if is_big:
        return InputFileBig(id=file_id, parts=total_parts, name=filename)
    else:
        return InputFile(id=file_id, parts=total_parts, name=filename)


async def _parallel_upload_file_pyrogram(
    client,
    file_path: str,
    file_size: int,
    progress_callback: Callable[[int, int], None] | None = None,
    part_size: int = _PARALLEL_PART_SIZE,
    workers: int = _PARALLEL_WORKERS,
):
    """Upload a file to Telegram via Pyrogram using parallel chunked upload.

    Uses Pyrogram's low-level ``raw.functions.upload.SaveBigFilePart`` / ``SaveFilePart``
    to upload file parts concurrently, then returns an ``InputFileBig`` / ``InputFile``
    that can be passed directly to ``client.send_video()`` as the ``video`` argument.

    Args:
        client: Connected Pyrogram client.
        file_path: Path to the file to upload.
        file_size: Total file size in bytes.
        progress_callback: Optional ``callable(sent_bytes, total_bytes)``.
        part_size: Chunk size in bytes (default 512 KB).
        workers: Max concurrent uploads (default 6).

    Returns:
        ``InputFileBig`` for large files or ``InputFile`` for small files,
        ready to pass to ``client.send_video()``.
    """
    # ── Memory guard: avoid loading huge files entirely into RAM ──
    if _PARALLEL_MAX_MEMORY_BYTES > 0 and file_size > _PARALLEL_MAX_MEMORY_BYTES:
        logger.warning(
            "pyrogram_parallel_upload: file %s size %d exceeds memory guard %d — falling back to sequential",
            file_path,
            file_size,
            _PARALLEL_MAX_MEMORY_BYTES,
        )
        return None

    import random as _random

    from pyrogram.raw.functions.upload import SaveBigFilePart as _SaveBig
    from pyrogram.raw.functions.upload import SaveFilePart as _SaveSmall
    from pyrogram.raw.types import InputFile as _InputFile
    from pyrogram.raw.types import InputFileBig as _InputBig

    file_id = _random.randrange(1 << 63)  # noqa: S311
    total_parts = max(1, (file_size + part_size - 1) // part_size)
    is_big = total_parts > 1024 or file_size > _PARALLEL_BIG_FILE_THRESHOLD
    sem = asyncio.Semaphore(workers)
    sent_bytes = 0

    async def _upload_part(part_index: int, data: bytes) -> None:
        nonlocal sent_bytes
        async with sem:
            for attempt in range(3):
                try:
                    if is_big:
                        await client.invoke(
                            _SaveBig(
                                file_id=file_id,
                                file_part=part_index,
                                file_total_parts=total_parts,
                                bytes=data,
                            )
                        )
                    else:
                        await client.invoke(
                            _SaveSmall(
                                file_id=file_id,
                                file_part=part_index,
                                bytes=data,
                            )
                        )
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(1 * (attempt + 1))

            sent_bytes += len(data)
            if progress_callback:
                progress_callback(sent_bytes, file_size)

    # Read entire file into memory for chunking
    with open(file_path, "rb") as f:
        file_data = f.read()

    chunks = []
    for i in range(total_parts):
        start = i * part_size
        end = min(start + part_size, len(file_data))
        chunks.append((i, file_data[start:end]))

    logger.info(
        "pyrogram_parallel_upload: file=%s size=%d parts=%d workers=%d is_big=%s",
        file_path,
        file_size,
        total_parts,
        workers,
        is_big,
    )

    await asyncio.gather(*[_upload_part(i, d) for i, d in chunks])

    filename = os.path.basename(file_path)
    if is_big:
        return _InputBig(id=file_id, parts=total_parts, name=filename)
    else:
        return _InputFile(id=file_id, parts=total_parts, name=filename)


async def _normalize_target(chat_id: int | str, client=None):
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
    chat_id: int | str,
    file_path: str,
    caption: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    video_meta: dict | None = None,
    thumb_path: str | None = None,
) -> int | None:
    """Send a file using Telethon.

    When ``video_meta`` is provided (e.g. from a pre-probe in the worker), the
    internal ffprobe+thumbnail generation is skipped entirely and the supplied
    metadata is used directly. This ensures the video always arrives with
    duration/timestamps even if ffprobe would fail in an isolated environment.

    Args:
        chat_id: Target chat ID or username.
        file_path: Path to the file to send.
        caption: Optional caption text.
        progress_callback: Optional callable(sent_bytes, total_bytes) for upload progress.
        video_meta: Pre-probed metadata dict with keys ``duration``, ``width``, ``height``.
                    If provided, skips internal ffprobe.
        thumb_path: Pre-generated thumbnail path. If provided, skips internal thumbnail generation.

    Returns:
        The sent message ID on success, or None on failure.
    """
    if TelegramClient is None:
        return None

    from utils.telethon_session import get_userbot_credentials, has_usable_telethon_session

    # Fail fast if no usable Telethon session is available — avoids
    # client.start() prompting for a phone number on stdin (EOFError).
    if not has_usable_telethon_session():
        logger.info("userbot: Telethon session not configured; skipping Telethon upload")
        return None

    api_id, api_hash = get_userbot_credentials()

    # Pre-fetch video metadata and thumbnail before connecting.
    # Gracefully fall back to a generic send if ffprobe isn't available.
    # If video_meta/thumb_path were provided externally (pre-probed in the
    # worker), skip the internal probe entirely.
    _thumb_dir = None
    if video_meta is None:
        try:
            video_meta = await _probe_video_metadata(file_path) or {}
        except Exception:
            video_meta = {}
    if thumb_path is None:
        try:
            thumb_path = await _generate_video_thumbnail(file_path)
            if thumb_path:
                # Track the containing directory explicitly (not dirname(), which
                # could resolve to /tmp if the file is placed directly in the system
                # temp directory).  _generate_video_thumbnail and
                # probe_video_for_delivery both create a mkdtemp subdirectory, so
                # dirname() will return the private subdirectory — but this explicit
                # tracking is more robust against future changes.
                _thumb_dir = os.path.dirname(thumb_path)
        except Exception:
            thumb_path = None
    # NOTE: if thumb_path was provided externally (by the worker), we do NOT
    # track it for cleanup here — the caller (send_file_via_userbot or the
    # worker) owns it and will clean it up after ALL send methods have been
    # tried.  Cleaning it up early would break fallback send methods.

    client = await _get_cached_telethon_client(api_id, api_hash)
    if client is None:
        return None
    try:
        target = await _normalize_target(chat_id, client)

        # ── Upload file data using parallel chunked transfer (FastTelethon-style) ──
        file_size = os.path.getsize(file_path)
        uploaded_file = await _parallel_upload_file(
            client,
            file_path,
            file_size,
            progress_callback=progress_callback,
        )

        # ── Send the file with metadata ──
        # If parallel upload returned None (memory guard triggered), use
        # the raw file_path instead and let Telethon upload sequentially.
        _file_arg = uploaded_file if uploaded_file is not None else file_path
        if video_meta.get("duration"):
            from telethon.tl.types import DocumentAttributeVideo

            kwargs = {
                "caption": caption or "",
                "supports_streaming": True,
                "attributes": [
                    DocumentAttributeVideo(
                        duration=int(video_meta["duration"]),
                        w=int(video_meta.get("width", 0)),
                        h=int(video_meta.get("height", 0)),
                        supports_streaming=True,
                    )
                ],
            }
            if thumb_path is not None:
                kwargs["thumb"] = thumb_path
            # Only pass progress_callback for sequential upload (parallel handles its own)
            if uploaded_file is None and progress_callback is not None:
                kwargs["progress_callback"] = progress_callback
            msg = await client.send_file(target, _file_arg, **kwargs)
            logger.info(
                "userbot: Telethon sent video %s to %s (meta=%s, thumb=%s, msg_id=%s)",
                file_path,
                target,
                video_meta,
                bool(thumb_path),
                getattr(msg, "id", None),
            )
        else:
            # Fallback: generic file send (no video metadata)
            kwargs = {"caption": caption}
            if uploaded_file is None and progress_callback is not None:
                kwargs["progress_callback"] = progress_callback
            msg = await client.send_file(target, _file_arg, **kwargs)
            logger.info("userbot: Telethon sent file %s to %s (msg_id=%s)", file_path, target, getattr(msg, "id", None))
        return getattr(msg, "id", None)
    except Exception:
        logger.exception("userbot: Telethon failed to send file %s", file_path)
        return None
    finally:
        # Clean up temp thumbnail directory using safe_rmtree
        if _thumb_dir:
            with contextlib.suppress(Exception):
                safe_rmtree(_thumb_dir)
        # NOTE: the Telethon client is cached and NOT disconnected here.
        # _get_cached_telethon_client() recycles it for the next send.


async def _probe_video_metadata(path: str) -> dict:
    """Probe a video file with ffprobe and return parsed metadata dict.

    Returns a dict with keys: duration (int seconds), width (int), height (int).
    Missing or unreadable keys are omitted. Returns empty dict on any failure.
    """
    ffprobe_bin = "ffprobe"
    try:
        proc = await asyncio.create_subprocess_exec(
            ffprobe_bin,
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_entries",
            "stream=width,height,codec_type:format=duration",
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
        with contextlib.suppress(ValueError, TypeError):
            meta["duration"] = int(float(fmt["duration"]))
    return meta


async def _generate_video_thumbnail(path: str) -> str | None:
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
            ffmpeg_bin,
            "-y",
            "-ss",
            "00:00:01",
            "-i",
            path,
            "-vframes",
            "1",
            "-q:v",
            "2",
            thumb_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(proc.communicate(), timeout=30)
        if proc.returncode == 0 and os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
            # Keep the temp dir; caller must clean up
            return thumb_path
        safe_rmtree(tmp_dir)
        return None
    except Exception:
        safe_rmtree(tmp_dir)
        return None


async def _send_with_pyrogram(
    chat_id: int | str,
    file_path: str,
    caption: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    video_meta: dict | None = None,
    thumb_path: str | None = None,
) -> int | None:
    """Send a file using Pyrogram (session string fallback).

    Probes the video for duration / dimensions and extracts a thumbnail
    frame so the resulting Telegram message shows proper metadata instead
    of a "violet" unknown-video placeholder.

    When ``video_meta`` is provided (e.g. from a pre-probe in the worker),
    the internal ffprobe+thumbnail generation is skipped and the supplied
    metadata is used directly.

    Args:
        chat_id: Target chat ID or username.
        file_path: Path to the file to send.
        caption: Optional caption text.
        progress_callback: Optional callable(current, total) for upload progress.
                           Pyrogram progress callback is synchronous.
        video_meta: Pre-probed metadata dict with keys ``duration``, ``width``, ``height``.
        thumb_path: Pre-generated thumbnail path.

    Returns:
        The sent message ID on success, or None on failure.
    """
    if PyrogramClient is None:
        return None

    from utils.telethon_session import get_userbot_credentials

    api_id, api_hash = get_userbot_credentials()

    client = await _get_cached_pyrogram_user_client(api_id, api_hash)
    if client is None:
        return None

    # Pre-fetch video metadata and thumbnail before connecting to Telegram.
    # If video_meta/thumb_path were provided externally, skip internal probe.
    _thumb_dir = None
    if video_meta is None:
        try:
            video_meta = await _probe_video_metadata(file_path) or {}
        except Exception:
            video_meta = {}
    if thumb_path is None:
        try:
            thumb_path = await _generate_video_thumbnail(file_path)
            if thumb_path:
                _thumb_dir = os.path.dirname(thumb_path)
        except Exception:
            thumb_path = None
    # NOTE: if thumb_path was provided externally (by the worker), we do NOT
    # track it for cleanup here — the caller (send_file_via_userbot or the
    # worker) owns it and will clean it up after ALL send methods have been
    # tried.  Cleaning it up early would break fallback send methods.

    try:
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

        # ── Parallel upload then send ──
        file_size = os.path.getsize(file_path)
        uploaded_file = await _parallel_upload_file_pyrogram(
            client,
            file_path,
            file_size,
            progress_callback=progress_callback,
        )
        # If parallel upload returned None (memory guard), remove progress
        # so Pyrogram uses file_path and reports progress itself.
        if uploaded_file is None:
            logger.info("userbot: Pyrogram falling back to sequential upload for %s", file_path)
        else:
            kwargs.pop("progress", None)  # parallel upload already handled progress
        _file_arg = uploaded_file if uploaded_file is not None else file_path
        msg = await client.send_video(target, _file_arg, **kwargs)
        logger.info(
            "userbot: Pyrogram sent video %s to %s (meta=%s, thumb=%s, msg_id=%s)",
            file_path,
            target,
            video_meta,
            bool(thumb_path),
            getattr(msg, "id", None),
        )
        return getattr(msg, "id", None)
    except Exception:
        logger.exception("userbot: Pyrogram failed to send file %s", file_path)
        return None
    finally:
        # Clean up temp thumbnail directory using safe_rmtree
        if _thumb_dir:
            with contextlib.suppress(Exception):
                safe_rmtree(_thumb_dir)
        # NOTE: the Pyrogram user client is cached and NOT stopped here.
        # _get_cached_pyrogram_user_client() recycles it for the next send.


async def _send_with_pyrogram_bot(
    chat_id: int | str,
    file_path: str,
    caption: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    video_meta: dict | None = None,
    thumb_path: str | None = None,
) -> int | None:
    """Send a video using Pyrogram authenticated as the bot (bot token).

    The video appears as sent by the bot (not a user account), with full
    metadata (duration, width, height, supports_streaming, thumbnail).
    Sends via MTProto directly — no Bot API 50MB limit.

    Returns:
        The sent message ID on success, or None on failure/missing BOT_TOKEN.
    """
    if PyrogramClient is None:
        return None

    bot_token = os.environ.get("BOT_TOKEN")
    if not bot_token:
        logger.info("userbot: BOT_TOKEN not set; skipping Pyrogram (bot) send")
        return None

    from utils.telethon_session import get_userbot_credentials

    try:
        api_id, api_hash = get_userbot_credentials()
    except RuntimeError:
        logger.info(
            "userbot: API_ID/API_HASH not set; skipping Pyrogram (bot) send "
            "(bot accounts still need API credentials for MTProto connection)"
        )
        return None

    # Pre-fetch video metadata and thumbnail before connecting.
    _thumb_dir = None
    if video_meta is None:
        try:
            video_meta = await _probe_video_metadata(file_path) or {}
        except Exception:
            video_meta = {}
    if thumb_path is None:
        try:
            thumb_path = await _generate_video_thumbnail(file_path)
            if thumb_path:
                _thumb_dir = os.path.dirname(thumb_path)
        except Exception:
            thumb_path = None
    # NOTE: if thumb_path was provided externally (by the worker), we do NOT
    # track it for cleanup here — the caller (send_file_via_userbot or the
    # worker) owns it and will clean it up after ALL send methods have been
    # tried.  Cleaning it up early would break fallback send methods.

    bot = await _get_cached_bot_client(api_id, api_hash, bot_token)
    try:
        target = await _normalize_target(chat_id)

        kwargs = {
            "caption": caption or "",
            "supports_streaming": True,
        }
        if progress_callback is not None:
            kwargs["progress"] = progress_callback
        if "duration" in video_meta:
            kwargs["duration"] = video_meta["duration"]
        if "width" in video_meta:
            kwargs["width"] = video_meta["width"]
        if "height" in video_meta:
            kwargs["height"] = video_meta["height"]
        if thumb_path is not None:
            kwargs["thumb"] = thumb_path

        # ── Parallel upload then send ──
        file_size = os.path.getsize(file_path)
        uploaded_file = await _parallel_upload_file_pyrogram(
            bot,
            file_path,
            file_size,
            progress_callback=progress_callback,
        )
        # If parallel upload returned None (memory guard), remove progress
        # so Pyrogram uses file_path and reports progress itself.
        if uploaded_file is None:
            logger.info("userbot: Pyrogram (bot) falling back to sequential upload for %s", file_path)
        else:
            kwargs.pop("progress", None)  # parallel upload already handled progress
        _file_arg = uploaded_file if uploaded_file is not None else file_path
        msg = await bot.send_video(target, _file_arg, **kwargs)
        logger.info(
            "userbot: Pyrogram (bot) sent video %s to %s (meta=%s, thumb=%s, msg_id=%s)",
            file_path,
            target,
            video_meta,
            bool(thumb_path),
            getattr(msg, "id", None),
        )
        return getattr(msg, "id", None)
    except Exception:
        logger.exception("userbot: Pyrogram (bot) failed to send file %s", file_path)
        return None
    finally:
        # Clean up temp thumbnail directory using safe_rmtree
        if _thumb_dir:
            with contextlib.suppress(Exception):
                safe_rmtree(_thumb_dir)
        # NOTE: the bot client is cached and NOT stopped here.
        # _get_cached_bot_client() recycles it for the next send.
        # If the connection drops, the next call automatically creates a fresh one.


async def send_file_via_userbot(
    chat_id: int | str,
    file_path: str,
    caption: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    video_meta: dict | None = None,
    thumb_path: str | None = None,
) -> int | None:
    """Send a file using a user account or bot.

    Priority order:
    1. Pyrogram with bot token — video appears as sent by the bot (preferred)
    2. Telethon user account — appears as sent by the Telethon phone number
    3. Pyrogram user account — appears as sent by the Pyrogram phone number

    When ``video_meta`` is provided (e.g. pre-probed in the worker), the
    internal ffprobe is skipped and the supplied metadata is used, ensuring
    the video always arrives with duration/timestamps even if ffprobe would
    fail in an isolated environment.

    Args:
        chat_id: Target chat ID or username.
        file_path: Path to the file to send.
        caption: Optional caption text.
        progress_callback: Optional callable(sent_bytes, total_bytes) for upload progress.
                           Both Telethon and Pyrogram callbacks follow this signature.
        video_meta: Pre-probed metadata dict with keys ``duration``, ``width``, ``height``.
        thumb_path: Pre-generated thumbnail path.

    Returns:
        The sent message ID on success, or None on failure.
        Raises RuntimeError for missing config.
    """
    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError(
            "Neither Telethon nor Pyrogram are installed. "
            "Install at least one: pip install telethon or pip install pyrogram"
        )

    # ── Priority 1: Pyrogram with bot token (video appears as sent by bot) ──
    if PyrogramClient is not None and os.environ.get("BOT_TOKEN"):
        try:
            msg_id = await _send_with_pyrogram_bot(
                chat_id,
                file_path,
                caption,
                progress_callback=progress_callback,
                video_meta=video_meta,
                thumb_path=thumb_path,
            )
            if msg_id is not None:
                return msg_id
            logger.info("userbot: Pyrogram bot send failed; trying Telethon fallback")
        except Exception as e:
            logger.warning("userbot: Pyrogram bot error (%s); trying Telethon fallback", e)

    # ── Priority 2: Telethon user account ──
    from utils.telethon_session import has_usable_telethon_session

    if TelegramClient is not None and has_usable_telethon_session():
        try:
            msg_id = await _send_with_telethon(
                chat_id,
                file_path,
                caption,
                progress_callback=progress_callback,
                video_meta=video_meta,
                thumb_path=thumb_path,
            )
            if msg_id is not None:
                return msg_id
            logger.info("userbot: Telethon send failed; trying Pyrogram fallback")
        except Exception as e:
            logger.warning("userbot: Telethon send error (%s); trying Pyrogram fallback", e)
    elif TelegramClient is not None:
        logger.info("userbot: Telethon session not configured; skipping Telethon upload")

    # ── Priority 3: Pyrogram user account (session string) ──
    if PyrogramClient is not None:
        msg_id = await _send_with_pyrogram(
            chat_id,
            file_path,
            caption,
            progress_callback=progress_callback,
            video_meta=video_meta,
            thumb_path=thumb_path,
        )
        if msg_id is not None:
            return msg_id

    logger.warning("userbot: all send methods failed for %s", chat_id)
    return None
