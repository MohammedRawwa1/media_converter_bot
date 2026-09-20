import asyncio
import contextlib
import hashlib
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


async def _get_cached_telethon_client(api_id: int, api_hash: str, session_str: str | None = None):
    """Return a cached Telethon client, creating or reconnecting if needed.

    When ``session_str`` is provided (per-user session), a fresh non-cached
    client is created and returned.  When ``session_str`` is ``None`` (default),
    the global cached client is reused, avoiding reconnect overhead.
    """
    from utils.telethon_session import build_telethon_client

    global _TELETHON_CACHE_LOCK, _TELETHON_CACHED_CLIENT

    # ── Per-user session: create a fresh client, no caching ──
    if session_str is not None:
        client = build_telethon_client(api_id, api_hash, session_str=session_str)

        async def _no_phone_explicit():
            raise RuntimeError("Telethon phone prompt unexpectedly triggered")

        await client.start(phone=_no_phone_explicit)
        logger.info("userbot: Created new Telethon client for per-user session")
        return client

    # ── Global session: use cached client ──
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


async def _get_cached_pyrogram_user_client(api_id: int, api_hash: str, session_str: str | None = None):
    """Return a cached Pyrogram user client, creating or reconnecting if needed.

    When ``session_str`` is provided (per-user session), a fresh non-cached
    client is created and returned.  When ``session_str`` is ``None`` (default),
    the global cached client is reused, avoiding reconnect overhead.
    """
    from utils.telethon_session import build_pyrogram_client

    global _PYRO_USER_CACHE_LOCK, _PYRO_USER_CACHED_CLIENT

    # ── Per-user session: create a fresh client, no caching ──
    if session_str is not None:
        client = build_pyrogram_client(api_id, api_hash, session_str=session_str)
        if client is None:
            return None
        await client.start()
        logger.info("userbot: Created new Pyrogram client for per-user session")
        return client

    # ── Global session: use cached client ──
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
# Above this size the parallel path is skipped and the file is handed to the
# client's own streaming uploader instead. The parallel path reads one part at a
# time straight off disk, so this is a policy threshold (a path known good on
# very large files), not a memory guard.
# Set to 0 or negative to always use the parallel path.
_PARALLEL_MAX_MEMORY_BYTES: int = 500 * 1024 * 1024  # 500 MB

# ── Audio delivery ──
# Extracted audio (video -> MP3, format conversions) must be uploaded as a
# Telegram *audio* document, i.e. carrying ``DocumentAttributeAudio``. Without
# that attribute the client shows a plain downloadable file instead of the
# streamable music player, and Bot API-sized files behave differently from the
# MTProto (userbot) path.
_AUDIO_OUTPUT_EXTS: frozenset[str] = frozenset(
    {".mp3", ".m4a", ".aac", ".flac", ".ogg", ".oga", ".opus", ".wav", ".wma"}
)
_AUDIO_MIME_TYPES: dict[str, str] = {
    ".mp3": "audio/mpeg",
    ".m4a": "audio/mp4",
    ".aac": "audio/aac",
    ".flac": "audio/flac",
    ".ogg": "audio/ogg",
    ".oga": "audio/ogg",
    ".opus": "audio/opus",
    ".wav": "audio/wav",
    ".wma": "audio/x-ms-wma",
}


def is_audio_delivery_output(file_path: str, media_kind: str | None = None) -> bool:
    """Return True when ``file_path`` should be delivered as Telegram audio.

    ``media_kind`` (``"audio"``/``"video"``) from the job metadata wins when
    present; otherwise the file extension decides, so existing video-only
    callers keep their behaviour unchanged.
    """
    if media_kind:
        return str(media_kind).lower() == "audio"
    return os.path.splitext(file_path or "")[1].lower() in _AUDIO_OUTPUT_EXTS


def _audio_mime_type(file_path: str) -> str:
    ext = os.path.splitext(file_path or "")[1].lower()
    return _AUDIO_MIME_TYPES.get(ext, "audio/mpeg")


def _audio_title(name: str | None) -> str:
    """Track title shown in Telegram's player: the name without extension."""
    return os.path.splitext(os.path.basename(name or ""))[0][:64]


async def _probe_audio_metadata(path: str) -> dict:
    """Probe an audio file for the metadata Telegram needs to show a player.

    Returns a dict with optional keys ``duration`` (int seconds), ``title`` and
    ``performer``. Empty dict when ffprobe is unavailable or fails.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_entries",
            "format=duration:format_tags=title,artist,artists,album_artist,performer,author",
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

    meta: dict = {}
    fmt = data.get("format", {}) or {}
    if fmt.get("duration"):
        with contextlib.suppress(ValueError, TypeError):
            # Telegram needs a non-zero duration for the player to seek.
            meta["duration"] = max(1, int(float(fmt["duration"])))
    tags = fmt.get("tags", {}) or {}
    for key, value in tags.items():
        lowered = str(key).lower()
        if lowered == "title" and value:
            meta["title"] = str(value)[:64]
        elif lowered in ("artist", "artists", "album_artist", "performer", "author") and value:
            meta.setdefault("performer", str(value)[:64])
    return meta


# Public alias: the ffmpeg worker pre-probes the encoded output so the player
# tags are read once, from the file it just produced, and passed in explicitly.
async def probe_audio_metadata(path: str) -> dict:
    """Public entry point for :func:`_probe_audio_metadata`."""
    return await _probe_audio_metadata(path)


def _part_count(file_size: int, part_size: int) -> int:
    """Number of Telegram upload parts for a file of ``file_size`` bytes."""
    part_size = max(1, int(part_size))
    return max(1, (int(file_size) + part_size - 1) // part_size)


def _file_parts(file_size: int, part_size: int) -> list[tuple[int, int, int]]:
    """``(index, offset, length)`` for every part of a file of ``file_size`` bytes.

    Together the parts cover the file exactly once: no gap, no overlap, and the
    final part carries the remainder.
    """
    part_size = max(1, int(part_size))
    return [
        (i, i * part_size, min(part_size, file_size - i * part_size)) for i in range(_part_count(file_size, part_size))
    ]


def _md5_hex(file_path: str) -> str:
    """MD5 of a file, read in blocks so it never lands in RAM whole.

    Small files go up with ``SaveFilePart``, which wants the checksum; the big
    file path uses ``InputFileBig``, which has no such field.
    """
    # usedforsecurity=False states the intent in the call itself: this is the
    # protocol checksum SaveFilePart requires, not a security primitive. Without
    # it the call reads as a weak-hash mistake to both ruff (S324) and bandit
    # (B324), the latter at HIGH severity.
    digest = hashlib.md5(usedforsecurity=False)
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_file_part(file_path: str, offset: int, length: int) -> bytes:
    """Read exactly one part from disk, straight off the file.

    Parts are read one at a time so only the parts actually in flight are ever
    resident. Reading the whole file up front (and then slicing it into a list
    of parts) held two full copies in RAM, which is what killed large deliveries
    mid-upload on memory-tight containers: the process was OOM-killed with no
    error and the job had to be re-run from the start.
    """
    with open(file_path, "rb") as f:
        f.seek(offset)
        return f.read(length)


async def _parallel_upload_file(
    client,
    file_path: str,
    file_size: int,
    progress_callback: Callable[[int, int], None] | None = None,
    part_size: int = _PARALLEL_PART_SIZE,
    workers: int = _PARALLEL_WORKERS,
    name: str | None = None,
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
    # ── Size policy: very large files use the client's own uploader ──
    if _PARALLEL_MAX_MEMORY_BYTES > 0 and file_size > _PARALLEL_MAX_MEMORY_BYTES:
        logger.warning(
            "parallel_upload: file %s size %d is above the parallel threshold %d — falling back to sequential",
            file_path,
            file_size,
            _PARALLEL_MAX_MEMORY_BYTES,
        )
        return None

    import secrets

    from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
    from telethon.tl.types import InputFile, InputFileBig

    # Telegram's upload protocol wants a random int64 file_id. It has to be unique;
    # it does not have to be secret. `secrets` is the same call with an OS-grade
    # generator behind it, so there is no reason to reach for the Mersenne twister.
    file_id = secrets.randbits(63)
    total_parts = _part_count(file_size, part_size)
    is_big = total_parts > 1024 or file_size > _PARALLEL_BIG_FILE_THRESHOLD
    sem = asyncio.Semaphore(workers)
    sent_bytes = 0

    async def _upload_part(part_index: int, offset: int, length: int) -> None:
        nonlocal sent_bytes
        async with sem:
            # Read this part only, and only once the slot is held: the bytes of
            # the parts still queued behind the semaphore stay on disk.
            data = _read_file_part(file_path, offset, length)
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

    logger.info(
        "parallel_upload: file=%s size=%d parts=%d workers=%d is_big=%s",
        file_path,
        file_size,
        total_parts,
        workers,
        is_big,
    )

    await asyncio.gather(*[_upload_part(i, offset, length) for i, offset, length in _file_parts(file_size, part_size)])

    # The uploaded file's name drives mime-type detection downstream, so use
    # the delivery name (original media name) when the caller supplied one.
    filename = name or os.path.basename(file_path)
    if is_big:
        return InputFileBig(id=file_id, parts=total_parts, name=filename)
    else:
        # InputFile (small files) requires the checksum; omitting it raised a
        # TypeError that made every small delivery fall back to a slower path.
        return InputFile(id=file_id, parts=total_parts, name=filename, md5_checksum=_md5_hex(file_path))


async def _parallel_upload_file_pyrogram(
    client,
    file_path: str,
    file_size: int,
    progress_callback: Callable[[int, int], None] | None = None,
    part_size: int = _PARALLEL_PART_SIZE,
    workers: int = _PARALLEL_WORKERS,
    name: str | None = None,
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
    # ── Size policy: very large files use the client's own uploader ──
    if _PARALLEL_MAX_MEMORY_BYTES > 0 and file_size > _PARALLEL_MAX_MEMORY_BYTES:
        logger.warning(
            "pyrogram_parallel_upload: file %s size %d is above the parallel threshold %d — falling back to sequential",
            file_path,
            file_size,
            _PARALLEL_MAX_MEMORY_BYTES,
        )
        return None

    import secrets as _secrets

    from pyrogram.raw.functions.upload import SaveBigFilePart as _SaveBig
    from pyrogram.raw.functions.upload import SaveFilePart as _SaveSmall
    from pyrogram.raw.types import InputFile as _InputFile
    from pyrogram.raw.types import InputFileBig as _InputBig

    # Random int64 upload id, exactly as in the Telethon path above.
    file_id = _secrets.randbits(63)
    total_parts = _part_count(file_size, part_size)
    is_big = total_parts > 1024 or file_size > _PARALLEL_BIG_FILE_THRESHOLD
    sem = asyncio.Semaphore(workers)
    sent_bytes = 0

    async def _upload_part(part_index: int, offset: int, length: int) -> None:
        nonlocal sent_bytes
        async with sem:
            # Read this part only, and only once the slot is held: the bytes of
            # the parts still queued behind the semaphore stay on disk.
            data = _read_file_part(file_path, offset, length)
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

    logger.info(
        "pyrogram_parallel_upload: file=%s size=%d parts=%d workers=%d is_big=%s",
        file_path,
        file_size,
        total_parts,
        workers,
        is_big,
    )

    await asyncio.gather(*[_upload_part(i, offset, length) for i, offset, length in _file_parts(file_size, part_size)])

    # The uploaded file's name drives mime-type detection downstream, so use
    # the delivery name (original media name) when the caller supplied one.
    filename = name or os.path.basename(file_path)
    if is_big:
        return _InputBig(id=file_id, parts=total_parts, name=filename)
    else:
        # InputFile (small files) requires the checksum; omitting it raised a
        # TypeError that made every small delivery fall back to a slower path.
        return _InputFile(id=file_id, parts=total_parts, name=filename, md5_checksum=_md5_hex(file_path))


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
    user_id: int | None = None,
    media_kind: str | None = None,
    delivery_name: str | None = None,
    audio_meta: dict | None = None,
    as_document: bool = False,
) -> int | None:
    """Send a file using Telethon.

    When ``video_meta`` is provided (e.g. from a pre-probe in the worker), the
    internal ffprobe+thumbnail generation is skipped entirely and the supplied
    metadata is used directly. This ensures the video always arrives with
    duration/timestamps even if ffprobe would fail in an isolated environment.

    When the output is audio (``media_kind``/file extension), the file is sent
    with ``DocumentAttributeAudio`` so Telegram shows the streamable music
    player instead of a plain downloadable file.

    Args:
        chat_id: Target chat ID or username.
        file_path: Path to the file to send.
        caption: Optional caption text.
        progress_callback: Optional callable(sent_bytes, total_bytes) for upload progress.
        video_meta: Pre-probed metadata dict with keys ``duration``, ``width``, ``height``.
                    If provided, skips internal ffprobe.
        thumb_path: Pre-generated thumbnail path. If provided, skips internal thumbnail generation.
        user_id: Optional Telegram user ID for per-user session resolution.
        media_kind: ``"audio"`` to force audio delivery; ``None`` infers it from
                    the file extension.
        delivery_name: Filename shown in Telegram (defaults to the file's name).
        audio_meta: Pre-probed audio metadata (``duration``/``title``/``performer``).
        as_document: Send non-audio outputs as a Telegram document instead of
                    playable media (the ``upload_mode`` preference).

    Returns:
        The sent message ID on success, or None on failure.
    """
    if TelegramClient is None:
        return None

    from utils.telethon_session import (
        get_db_model,
        get_telethon_session_string_for_user,
        get_userbot_credentials,
        has_usable_telethon_session_async,
    )

    # Fail fast if no usable Telethon session is available — avoids
    # client.start() prompting for a phone number on stdin (EOFError).
    # The async variant also consults MongoDB, which this path cannot reach
    # through ``application.bot_data``.
    if not await has_usable_telethon_session_async(user_id=user_id, db_model=get_db_model()):
        logger.info("userbot: Telethon session not configured; skipping Telethon upload")
        return None

    api_id, api_hash = get_userbot_credentials()

    # Resolve per-user session string if user_id is provided
    session_str = None
    if user_id is not None:
        try:
            session_str = await get_telethon_session_string_for_user(user_id=user_id, db_model=get_db_model())
        except Exception:
            session_str = None

    _is_audio = is_audio_delivery_output(file_path, media_kind)
    _delivery_name = delivery_name or os.path.basename(file_path)
    # Audio is never forced into the document view: a music file rendered as a
    # plain download has lost the thing that made it useful. Everything else
    # can be, when the user asked for documents in /usersettings.
    _as_document = bool(as_document) and not _is_audio

    # Pre-fetch video metadata and thumbnail before connecting.
    # Gracefully fall back to a generic send if ffprobe isn't available.
    # If video_meta/thumb_path were provided externally (pre-probed in the
    # worker), skip the internal probe entirely.
    _thumb_dir = None
    if _is_audio:
        # Audio carries no video metadata and needs no thumbnail; only its
        # duration/tags matter for the player.
        if audio_meta is None:
            try:
                audio_meta = await _probe_audio_metadata(file_path) or {}
            except Exception:
                audio_meta = {}
        video_meta = video_meta or {}
    elif _as_document:
        # A document needs no duration, dimensions or preview frame, so the
        # probe and the thumbnail generation are skipped entirely - worth doing
        # for a large file that only had to be re-uploaded as its own bytes.
        video_meta = {}
    elif video_meta is None:
        try:
            video_meta = await _probe_video_metadata(file_path) or {}
        except Exception:
            video_meta = {}
    if not _is_audio and not _as_document and thumb_path is None:
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

    client = await _get_cached_telethon_client(api_id, api_hash, session_str=session_str)
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
            name=_delivery_name,
        )

        # ── Send the file with metadata ──
        # If parallel upload returned None (memory guard triggered), use
        # the raw file_path instead and let Telethon upload sequentially.
        _file_arg = uploaded_file if uploaded_file is not None else file_path
        if _as_document:
            from telethon.tl.types import DocumentAttributeFilename

            # Only the filename attribute: without a video/audio attribute
            # Telegram files this as a document, and the explicit filename
            # keeps the on-disk name (with its collision suffix) out of the chat.
            kwargs = {
                "caption": caption or "",
                "attributes": [DocumentAttributeFilename(file_name=_delivery_name)],
                # A filename-only attribute already files this as a document, but
                # ``force_document`` makes it explicit: an archive volume (a
                # ``.001`` part, or a ``.zip``) must never be pulled into a media
                # view by its extension, and the bytes have to arrive untouched.
                "force_document": True,
            }
            # Only pass progress_callback for sequential upload (parallel handles its own)
            if uploaded_file is None and progress_callback is not None:
                kwargs["progress_callback"] = progress_callback
            msg = await client.send_file(target, _file_arg, **kwargs)
            logger.info(
                "userbot: Telethon sent document %s to %s as %s (msg_id=%s)",
                file_path,
                target,
                _delivery_name,
                getattr(msg, "id", None),
            )
        elif _is_audio:
            from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeFilename

            audio_meta = audio_meta or {}
            _audio_attributes = [
                DocumentAttributeAudio(
                    duration=int(audio_meta.get("duration") or 0),
                    title=(audio_meta.get("title") or _audio_title(_delivery_name))[:64],
                    performer=(audio_meta.get("performer") or "")[:64],
                    voice=False,
                ),
                DocumentAttributeFilename(file_name=_delivery_name),
            ]
            kwargs = {
                "caption": caption or "",
                # Explicit attributes are required: Telethon only adds
                # DocumentAttributeAudio when it can read the file's tags with
                # hachoir, which is not installed in most deployments.
                "attributes": _audio_attributes,
                "mime_type": _audio_mime_type(_delivery_name),
            }
            # Only pass progress_callback for sequential upload (parallel handles its own)
            if uploaded_file is None and progress_callback is not None:
                kwargs["progress_callback"] = progress_callback
            msg = await client.send_file(target, _file_arg, **kwargs)
            logger.info(
                "userbot: Telethon sent audio %s to %s as %s (meta=%s, msg_id=%s)",
                file_path,
                target,
                _delivery_name,
                audio_meta,
                getattr(msg, "id", None),
            )
        elif video_meta.get("duration"):
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


async def _send_video_raw(
    client,  # pyrogram.Client — noqa: F821
    target: int | str,
    file_path: str,
    uploaded_file,  # raw.types.InputFile or InputFileBig
    caption: str = "",
    video_meta: dict | None = None,
    thumb_path: str | None = None,
) -> int | None:
    """Send a pre-uploaded video via raw Telegram API.

    Pyrogram's ``send_video()`` has no code path for pre-uploaded
    ``raw.types.InputFile`` / ``InputFileBig`` objects — it always calls
    ``save_file()`` which only accepts file paths or binary streams and
    raises ``ValueError`` for TL types.

    This helper builds the raw ``messages.SendMedia`` call directly with
    the already-uploaded file, avoiding a redundant re-upload.
    """
    from pyrogram import raw

    video_meta = video_meta or {}

    attributes = [
        raw.types.DocumentAttributeVideo(
            supports_streaming=True,
            duration=int(video_meta.get("duration", 0)),
            w=int(video_meta.get("width", 0)),
            h=int(video_meta.get("height", 0)),
        ),
        raw.types.DocumentAttributeFilename(file_name=os.path.basename(file_path)),
    ]

    thumb = None
    if thumb_path:
        try:
            thumb = await client.save_file(thumb_path)
        except Exception:
            logger.warning("userbot: failed to upload thumbnail %s", thumb_path)

    media = raw.types.InputMediaUploadedDocument(
        mime_type=client.guess_mime_type(file_path) or "video/mp4",
        file=uploaded_file,
        thumb=thumb,
        attributes=attributes,
    )
    return await _send_media_raw(client, target, media, caption=caption, file_path=file_path)


async def _send_document_raw(
    client,  # pyrogram.Client — noqa: F821
    target: int | str,
    file_path: str,
    uploaded_file,  # raw.types.InputFile or InputFileBig
    caption: str = "",
    delivery_name: str | None = None,
) -> int | None:
    """Send a pre-uploaded file as a plain document via the raw Telegram API.

    Same reason as ``_send_video_raw``: Pyrogram's ``send_document`` cannot take
    an already-uploaded ``InputFile`` and would upload the file a second time.

    Only ``DocumentAttributeFilename`` is attached, and deliberately no video
    attribute: that is what makes Telegram show the file in the document view
    (downloadable, named) instead of as playable media with a preview. It is
    also what carries the user-facing name, which the raw message would
    otherwise take from the on-disk path.
    """
    from pyrogram import raw

    name = delivery_name or os.path.basename(file_path)
    media = raw.types.InputMediaUploadedDocument(
        mime_type=client.guess_mime_type(file_path) or "application/octet-stream",
        file=uploaded_file,
        attributes=[raw.types.DocumentAttributeFilename(file_name=name)],
    )
    return await _send_media_raw(client, target, media, caption=caption, file_path=file_path)


async def _send_audio_raw(
    client,  # pyrogram.Client — noqa: F821
    target: int | str,
    file_path: str,
    uploaded_file,  # raw.types.InputFile or InputFileBig
    caption: str = "",
    audio_meta: dict | None = None,
    delivery_name: str | None = None,
) -> int | None:
    """Send a pre-uploaded audio file via the raw Telegram API.

    Mirrors what ``Client.send_audio`` builds (``DocumentAttributeAudio`` +
    ``DocumentAttributeFilename``) but reuses the already-uploaded parallel
    chunk upload.  The audio attribute is what makes Telegram render the
    message as streamable audio with a player rather than a plain document.
    """
    from pyrogram import raw

    audio_meta = audio_meta or {}
    name = delivery_name or os.path.basename(file_path)

    attributes = [
        raw.types.DocumentAttributeAudio(
            duration=int(audio_meta.get("duration") or 0),
            title=(audio_meta.get("title") or _audio_title(name)),
            performer=(audio_meta.get("performer") or "")[:64],
            voice=False,
        ),
        raw.types.DocumentAttributeFilename(file_name=name),
    ]

    media = raw.types.InputMediaUploadedDocument(
        mime_type=_audio_mime_type(name),
        file=uploaded_file,
        attributes=attributes,
    )
    return await _send_media_raw(client, target, media, caption=caption, file_path=file_path)


async def _send_media_raw(
    client,  # pyrogram.Client — noqa: F821
    target: int | str,
    media,
    caption: str = "",
    file_path: str = "",
) -> int | None:
    """Invoke ``messages.SendMedia`` with pre-built media and return the msg id."""
    from pyrogram import raw

    try:
        r = await client.invoke(
            raw.functions.messages.SendMedia(
                peer=await client.resolve_peer(target),
                media=media,
                message=caption or "",
                random_id=client.rnd_id(),
            )
        )
        for update in r.updates:
            if isinstance(
                update,
                (
                    raw.types.UpdateNewMessage,
                    raw.types.UpdateNewChannelMessage,
                    raw.types.UpdateNewScheduledMessage,
                ),
            ):
                from pyrogram import types as pyro_types

                msg = await pyro_types.Message._parse(
                    client,
                    update.message,
                    {i.id: i for i in r.users},
                    {i.id: i for i in r.chats},
                    is_scheduled=isinstance(update, raw.types.UpdateNewScheduledMessage),
                )
                return getattr(msg, "id", None)
        return None
    except Exception:
        logger.exception("userbot: raw API send failed for %s", file_path)
        return None


async def _send_with_pyrogram(
    chat_id: int | str,
    file_path: str,
    caption: str | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    video_meta: dict | None = None,
    thumb_path: str | None = None,
    user_id: int | None = None,
    media_kind: str | None = None,
    delivery_name: str | None = None,
    audio_meta: dict | None = None,
    as_document: bool = False,
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
        user_id: Optional Telegram user ID for per-user session resolution.
        media_kind: ``"audio"`` to force audio delivery; otherwise inferred from
                    the file extension.
        delivery_name: Filename shown in Telegram (defaults to the file's name).
        audio_meta: Pre-probed audio metadata (``duration``/``title``/``performer``).
        as_document: Send non-audio outputs as a Telegram document instead of
                    playable media (the ``upload_mode`` preference).

    Returns:
        The sent message ID on success, or None on failure.
    """
    if PyrogramClient is None:
        return None

    from utils.telethon_session import (
        get_db_model,
        get_pyrogram_session_string_for_user,
        get_userbot_credentials,
    )

    api_id, api_hash = get_userbot_credentials()

    # Resolve per-user session string if user_id is provided
    session_str = None
    if user_id is not None:
        # JSON -> MongoDB -> env, so a session stored only in MongoDB works here.
        session_str = await get_pyrogram_session_string_for_user(user_id=user_id, db_model=get_db_model())
        if not session_str:
            logger.info("userbot: Pyrogram session not configured for user %s; skipping Pyrogram upload", user_id)
            return None

    client = await _get_cached_pyrogram_user_client(api_id, api_hash, session_str=session_str)
    if client is None:
        return None

    _is_audio = is_audio_delivery_output(file_path, media_kind)
    _delivery_name = delivery_name or os.path.basename(file_path)
    # Audio is never forced into the document view: a music file rendered as a
    # plain download has lost the thing that made it useful. Everything else
    # can be, when the user asked for documents in /usersettings.
    _as_document = bool(as_document) and not _is_audio

    # Pre-fetch video metadata and thumbnail before connecting to Telegram.
    # If video_meta/thumb_path were provided externally, skip internal probe.
    _thumb_dir = None
    if _is_audio:
        if audio_meta is None:
            try:
                audio_meta = await _probe_audio_metadata(file_path) or {}
            except Exception:
                audio_meta = {}
        video_meta = video_meta or {}
    elif _as_document:
        # A document needs no duration, dimensions or preview frame, so the
        # probe and the thumbnail generation are skipped entirely - worth doing
        # for a large file that only had to be re-uploaded as its own bytes.
        video_meta = {}
    elif video_meta is None:
        try:
            video_meta = await _probe_video_metadata(file_path) or {}
        except Exception:
            video_meta = {}
    if not _is_audio and not _as_document and thumb_path is None:
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

        # ── Parallel upload then send ──
        # Pyrogram's send_video() does NOT accept pre-uploaded InputFile
        # objects (it calls save_file() which only handles strings/IO).
        # When parallel upload succeeds, we use the raw API instead.
        file_size = os.path.getsize(file_path)
        uploaded_file = await _parallel_upload_file_pyrogram(
            client,
            file_path,
            file_size,
            progress_callback=progress_callback,
            name=_delivery_name,
        )
        if uploaded_file is not None:
            if _is_audio:
                msg_id = await _send_audio_raw(
                    client,
                    target,
                    file_path,
                    uploaded_file,
                    caption=caption or "",
                    audio_meta=audio_meta,
                    delivery_name=_delivery_name,
                )
            elif _as_document:
                msg_id = await _send_document_raw(
                    client,
                    target,
                    file_path,
                    uploaded_file,
                    caption=caption or "",
                    delivery_name=_delivery_name,
                )
            else:
                msg_id = await _send_video_raw(
                    client,
                    target,
                    file_path,
                    uploaded_file,
                    caption=caption or "",
                    video_meta=video_meta,
                    thumb_path=thumb_path,
                )
            if msg_id is not None:
                logger.info(
                    "userbot: Pyrogram sent %s %s to %s (meta=%s, thumb=%s, msg_id=%s)",
                    "audio" if _is_audio else "video",
                    file_path,
                    target,
                    audio_meta if _is_audio else video_meta,
                    bool(thumb_path),
                    msg_id,
                )
                return msg_id
            logger.warning("userbot: Pyrogram raw API send returned None; falling back")

        if _is_audio:
            # Fallback: let Pyrogram build the audio document (it sets
            # DocumentAttributeAudio, which keeps the message streamable).
            logger.info("userbot: Pyrogram falling back to send_audio for %s", file_path)
            audio_meta = audio_meta or {}
            kwargs = {
                "caption": caption or "",
                "file_name": _delivery_name,
                "title": audio_meta.get("title") or _audio_title(_delivery_name),
                "performer": audio_meta.get("performer") or "",
                "duration": int(audio_meta.get("duration") or 0),
            }
            if progress_callback is not None:
                kwargs["progress"] = progress_callback
            msg = await client.send_audio(target, file_path, **kwargs)
            logger.info(
                "userbot: Pyrogram sent audio %s to %s as %s (msg_id=%s)",
                file_path,
                target,
                _delivery_name,
                getattr(msg, "id", None),
            )
            return getattr(msg, "id", None)

        if _as_document:
            # Fallback: let Pyrogram handle upload + send via send_document,
            # which forces the document view and names the file explicitly.
            logger.info("userbot: Pyrogram falling back to send_document for %s", file_path)
            kwargs = {"caption": caption or "", "file_name": _delivery_name}
            if progress_callback is not None:
                kwargs["progress"] = progress_callback
            msg = await client.send_document(target, file_path, **kwargs)
            logger.info(
                "userbot: Pyrogram sent document %s to %s as %s (msg_id=%s)",
                file_path,
                target,
                _delivery_name,
                getattr(msg, "id", None),
            )
            return getattr(msg, "id", None)

        # Fallback: let Pyrogram handle upload + send via send_video
        logger.info("userbot: Pyrogram falling back to send_video for %s", file_path)
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

        msg = await client.send_video(target, file_path, **kwargs)
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
    media_kind: str | None = None,
    delivery_name: str | None = None,
    audio_meta: dict | None = None,
    as_document: bool = False,
) -> int | None:
    """Send a video/audio using Pyrogram authenticated as the bot (bot token).

    The file appears as sent by the bot (not a user account), with full
    metadata (duration, dimensions, supports_streaming, thumbnail for videos;
    DocumentAttributeAudio for audio). Sends via MTProto directly — no Bot API
    50MB limit.

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

    _is_audio = is_audio_delivery_output(file_path, media_kind)
    _delivery_name = delivery_name or os.path.basename(file_path)
    # Audio is never forced into the document view: a music file rendered as a
    # plain download has lost the thing that made it useful. Everything else
    # can be, when the user asked for documents in /usersettings.
    _as_document = bool(as_document) and not _is_audio

    # Pre-fetch video metadata and thumbnail before connecting.
    _thumb_dir = None
    if _is_audio:
        if audio_meta is None:
            try:
                audio_meta = await _probe_audio_metadata(file_path) or {}
            except Exception:
                audio_meta = {}
        video_meta = video_meta or {}
    elif _as_document:
        # A document needs no duration, dimensions or preview frame, so the
        # probe and the thumbnail generation are skipped entirely - worth doing
        # for a large file that only had to be re-uploaded as its own bytes.
        video_meta = {}
    elif video_meta is None:
        try:
            video_meta = await _probe_video_metadata(file_path) or {}
        except Exception:
            video_meta = {}
    if not _is_audio and not _as_document and thumb_path is None:
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

        # ── Parallel upload then send ──
        # Pyrogram's send_video() does NOT accept pre-uploaded InputFile
        # objects (it calls save_file() which only handles strings/IO).
        # When parallel upload succeeds, we use the raw API instead.
        file_size = os.path.getsize(file_path)
        uploaded_file = await _parallel_upload_file_pyrogram(
            bot,
            file_path,
            file_size,
            progress_callback=progress_callback,
            name=_delivery_name,
        )
        if uploaded_file is not None:
            if _is_audio:
                msg_id = await _send_audio_raw(
                    bot,
                    target,
                    file_path,
                    uploaded_file,
                    caption=caption or "",
                    audio_meta=audio_meta,
                    delivery_name=_delivery_name,
                )
            elif _as_document:
                msg_id = await _send_document_raw(
                    bot,
                    target,
                    file_path,
                    uploaded_file,
                    caption=caption or "",
                    delivery_name=_delivery_name,
                )
            else:
                msg_id = await _send_video_raw(
                    bot,
                    target,
                    file_path,
                    uploaded_file,
                    caption=caption or "",
                    video_meta=video_meta,
                    thumb_path=thumb_path,
                )
            if msg_id is not None:
                logger.info(
                    "userbot: Pyrogram (bot) sent %s %s to %s (meta=%s, thumb=%s, msg_id=%s)",
                    "audio" if _is_audio else "video",
                    file_path,
                    target,
                    audio_meta if _is_audio else video_meta,
                    bool(thumb_path),
                    msg_id,
                )
                return msg_id
            logger.warning("userbot: Pyrogram (bot) raw API send failed; falling back")

        if _is_audio:
            # Fallback: let Pyrogram build the audio document (it sets
            # DocumentAttributeAudio, which keeps the message streamable).
            logger.info("userbot: Pyrogram (bot) falling back to send_audio for %s", file_path)
            audio_meta = audio_meta or {}
            kwargs = {
                "caption": caption or "",
                "file_name": _delivery_name,
                "title": audio_meta.get("title") or _audio_title(_delivery_name),
                "performer": audio_meta.get("performer") or "",
                "duration": int(audio_meta.get("duration") or 0),
            }
            if progress_callback is not None:
                kwargs["progress"] = progress_callback
            msg = await bot.send_audio(target, file_path, **kwargs)
            logger.info(
                "userbot: Pyrogram (bot) sent audio %s to %s as %s (msg_id=%s)",
                file_path,
                target,
                _delivery_name,
                getattr(msg, "id", None),
            )
            return getattr(msg, "id", None)

        if _as_document:
            # Fallback: let Pyrogram handle upload + send via send_document,
            # which forces the document view and names the file explicitly.
            logger.info("userbot: Pyrogram (bot) falling back to send_document for %s", file_path)
            kwargs = {"caption": caption or "", "file_name": _delivery_name}
            if progress_callback is not None:
                kwargs["progress"] = progress_callback
            msg = await bot.send_document(target, file_path, **kwargs)
            logger.info(
                "userbot: Pyrogram (bot) sent document %s to %s as %s (msg_id=%s)",
                file_path,
                target,
                _delivery_name,
                getattr(msg, "id", None),
            )
            return getattr(msg, "id", None)

        # Fallback: let Pyrogram handle upload + send via send_video
        logger.info("userbot: Pyrogram (bot) falling back to send_video for %s", file_path)
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

        msg = await bot.send_video(target, file_path, **kwargs)
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
    user_id: int | None = None,
    media_kind: str | None = None,
    delivery_name: str | None = None,
    audio_meta: dict | None = None,
    as_document: bool = False,
) -> int | None:
    """Send a file using a user account or bot.

    Priority order:
    1. Pyrogram with bot token — file appears as sent by the bot (preferred)
    2. Telethon user account — appears as sent by the Telethon phone number
    3. Pyrogram user account — appears as sent by the Pyrogram phone number

    When ``video_meta`` is provided (e.g. pre-probed in the worker), the
    internal ffprobe is skipped and the supplied metadata is used, ensuring
    the video always arrives with duration/timestamps even if ffprobe would
    fail in an isolated environment.

    Audio outputs (``media_kind="audio"`` or an audio file extension) are sent
    as Telegram audio with ``DocumentAttributeAudio``, which is what makes the
    client show the streamable music player instead of a plain downloadable
    document.  ``delivery_name`` sets the filename shown to the user, so the
    original media name survives the conversion pipeline.

    Args:
        chat_id: Target chat ID or username.
        file_path: Path to the file to send.
        caption: Optional caption text.
        progress_callback: Optional callable(sent_bytes, total_bytes) for upload progress.
                           Both Telethon and Pyrogram callbacks follow this signature.
        video_meta: Pre-probed metadata dict with keys ``duration``, ``width``, ``height``.
        thumb_path: Pre-generated thumbnail path.
        user_id: Optional Telegram user ID for per-user session resolution.
        media_kind: ``"audio"``/``"video"`` from the job metadata; ``None`` lets
                    the file extension decide.
        delivery_name: Filename shown in Telegram (defaults to the file's name).
        audio_meta: Pre-probed audio metadata (``duration``/``title``/``performer``).
        as_document: Send non-audio outputs as a Telegram document instead of
                    playable media, matching the ``upload_mode`` preference from
                    /usersettings. Set on the job by ``enqueue_job``.

    Returns:
        The sent message ID on success, or None on failure.
        Raises RuntimeError for missing config.
    """
    if TelegramClient is None and PyrogramClient is None:
        raise RuntimeError(
            "Neither Telethon nor Pyrogram are installed. "
            "Install at least one: pip install telethon or pip install pyrogram"
        )

    if not delivery_name:
        # The worker names its output after the original media, so this keeps
        # the user's filename even when the caller forgot to pass one.
        delivery_name = os.path.basename(file_path)

    # ── Priority 1: Pyrogram with bot token (file appears as sent by bot) ──
    if PyrogramClient is not None and os.environ.get("BOT_TOKEN"):
        try:
            msg_id = await _send_with_pyrogram_bot(
                chat_id,
                file_path,
                caption,
                progress_callback=progress_callback,
                video_meta=video_meta,
                thumb_path=thumb_path,
                media_kind=media_kind,
                delivery_name=delivery_name,
                audio_meta=audio_meta,
                as_document=as_document,
            )
            if msg_id is not None:
                return msg_id
            logger.info("userbot: Pyrogram bot send failed; trying Telethon fallback")
        except Exception as e:
            logger.warning("userbot: Pyrogram bot error (%s); trying Telethon fallback", e)

    # ── Priority 2: Telethon user account ──
    from utils.telethon_session import get_db_model, has_usable_telethon_session_async, operating_user_id

    # The delivery belongs to a user, but a user who never logged in has no
    # session of their own - and a result that cannot be delivered is worse than
    # one delivered by the deployment's account, so that session carries it (see
    # ``operating_user_id``).
    user_id = await operating_user_id(user_id, get_db_model())

    if TelegramClient is not None and await has_usable_telethon_session_async(user_id=user_id, db_model=get_db_model()):
        try:
            msg_id = await _send_with_telethon(
                chat_id,
                file_path,
                caption,
                progress_callback=progress_callback,
                video_meta=video_meta,
                thumb_path=thumb_path,
                user_id=user_id,
                media_kind=media_kind,
                delivery_name=delivery_name,
                audio_meta=audio_meta,
                as_document=as_document,
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
            user_id=user_id,
            media_kind=media_kind,
            delivery_name=delivery_name,
            audio_meta=audio_meta,
            as_document=as_document,
        )
        if msg_id is not None:
            return msg_id

    logger.warning("userbot: all send methods failed for %s", chat_id)
    return None
