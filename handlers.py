# handlers.py
import asyncio
import contextlib
import html
import json
import logging
import os
import time
from datetime import UTC, datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, Update
from telegram.error import BadRequest
from telegram.ext import ContextTypes, ConversationHandler

from utils.time_utils import utc_iso

# Try to import from local modules
try:
    from media_converter import ExtendedMediaConverter
except ImportError:
    ExtendedMediaConverter = None

try:
    import uuid

    from utils.job_queue import enqueue_job
    from utils.keyboard_utils import MediaMenuBuilder
except ImportError:
    MediaMenuBuilder = None
    try:
        enqueue_job
    except NameError:
        enqueue_job = None
    try:
        uuid
    except NameError:
        uuid = None

try:
    from utils.file_utils import AsyncFileLock, detect_filename, filename_from_url, sanitize_filename
except ImportError:
    AsyncFileLock = None
    sanitize_filename = None

    def filename_from_url(url: str, default_ext: str = ".mp4", fallback_stem: str = "media") -> str:
        """Fallback used only when utils.file_utils cannot be imported."""
        return f"{fallback_stem}{default_ext}"


# Import config module if available (some code references `config.<NAME>`)
try:
    import config
except Exception:
    import os as _cfg_os

    class _FallbackConfig:
        """Minimal config with absolute paths when config.py import fails."""

        ROOT_DIR = _cfg_os.path.dirname(_cfg_os.path.abspath(__file__))
        STORAGE_PATH = _cfg_os.getenv("STORAGE_PATH", _cfg_os.path.join(ROOT_DIR, "storage"))
        INPUT_PATH = _cfg_os.path.join(STORAGE_PATH, "input")
        OUTPUT_PATH = _cfg_os.path.join(STORAGE_PATH, "output")
        TEMP_PATH = _cfg_os.path.join(STORAGE_PATH, "temp")
        THUMBNAIL_PATH = _cfg_os.path.join(STORAGE_PATH, "thumbnails")
        FFMPEG_PATH = _cfg_os.getenv("FFMPEG_PATH", "ffmpeg")
        MAX_FILE_SIZE = 4 * 1024**3
        STORAGE_BACKEND = "local"
        BOT_API_MAX_MB = 50
        BOT_API_MAX_BYTES = BOT_API_MAX_MB * 1024 * 1024
        ENABLE_USERBOT = _cfg_os.getenv("ENABLE_USERBOT", "").lower() in ("1", "true", "yes")
        ENABLE_LINK_SEND = _cfg_os.getenv("ENABLE_LINK_SEND", "").lower() in ("1", "true", "yes")
        RELAY_CHAT_ID = _cfg_os.getenv("RELAY_CHAT_ID", "")

        @staticmethod
        def get_storage_backend_name() -> str:
            return (_cfg_os.getenv("STORAGE_BACKEND") or "local").lower()

    config = _FallbackConfig()

# Import ACL helper
try:
    from config import MAX_FILE_SIZE, is_user_allowed

    try:
        from utils.bigfile_pipeline import BigFilePipeline

        _bigfile_pipeline = BigFilePipeline()
    except Exception:
        _bigfile_pipeline = None
except Exception:

    def is_user_allowed(_):
        return True

    MAX_FILE_SIZE = 4 * 1024**3

# Optional user settings helper
try:
    from utils import user_settings

    try:
        from utils.cache import get_cache
    except Exception:
        get_cache = None
except Exception:
    user_settings = None

logger = logging.getLogger(__name__)

# Registry for cancel-download flags: key="chat_id:msg_id" -> [bool]
# Used by _try_userbot_download() progress callback and the cancel_dl: callback handler.
_download_cancel_flags: dict[str, list] = {}

# How often the apply's message is refreshed with the stage of the file it is
# currently working on. Above the single-file watcher's 2s floor (which exists to
# stay clear of Telegram's edit rate) and coarse enough that a long conversion is
# not a stream of edits.
_BATCH_MEMBER_POLL_SECONDS = 3.0

# Job statuses that mean the worker is done with a file, one way or another.
# Anything else - including a status this build has never heard of - keeps the
# stage watcher polling, so an unfamiliar state is never mistaken for an ending.
_TERMINAL_JOB_STATUSES = frozenset({"done", "completed", "error", "failed", "cancelled", "canceled"})


# How long a watcher keeps trying to write a *terminal* status the flood gate
# swallowed before it gives up. The watcher is a detached task, not a handler, so
# waiting is safe - but it is still bounded, because a day-long penalty must not
# pin a task for a message nobody is watching any more.
_FLOOD_TERMINAL_MAX_WAIT_SECONDS = float(os.getenv("TELEGRAM_FLOOD_TERMINAL_MAX_WAIT_SECONDS", "3600"))
# How long a progress watcher keeps polling after ``ffmpeg:job:<id>`` stops
# existing. The hash is deliberately kept until the watcher reads the terminal
# status, so an empty read means it is never coming back.
_WATCH_JOB_MISSING_MAX_SECONDS = float(os.getenv("WATCH_JOB_MISSING_MAX_SECONDS", "600"))


class _EditUnchanged:
    """Returned by ``safe_edit`` when the message already showed that exact text.

    Telegram answers a repeated edit with a 400, and the edit has effectively
    succeeded: the message is showing what the caller wanted. Saying so (instead
    of reporting the same ``None`` as a dropped edit) is what lets a progress
    watcher tell "nothing to do" from "that write never happened".
    """

    __slots__ = ()

    def __bool__(self) -> bool:
        return True

    def __repr__(self) -> str:
        return "<edit: message already showed this text>"


EDIT_UNCHANGED = _EditUnchanged()


def _edit_target_ids(query, progress_msg=None) -> tuple[int | None, int | None]:
    """Return (chat_id, message_id) of the message a progress edit renders onto.

    Progress watchers can be started from a callback query or handed an explicit
    message; either way the pair identifies the one message they share, which is
    what the edit coalescer and the flood gate key on.
    """
    for source in (progress_msg, getattr(query, "message", None)):
        if source is None:
            continue
        chat_id = getattr(getattr(source, "chat", None), "id", None)
        if chat_id is None:
            chat_id = getattr(source, "chat_id", None)
        message_id = getattr(source, "message_id", None)
        if chat_id is not None and message_id is not None:
            return chat_id, message_id
    return None, None


def _extract_large_file_source(current_file: dict | None) -> tuple[int | None, int | None]:
    """Return (chat_id, message_id) for the source message used by the big-file pipeline.

    The bot stores this metadata in several shapes depending on the incoming file type:
    - current_file["chat_id"] / current_file["msg_id"] for regular messages
    - current_file["forward"]["chat_id"] / current_file["forward"]["message_id"]
      when the incoming file was forwarded
    """
    if not current_file:
        return None, None

    try:
        forward = current_file.get("forward") or {}
        forward_chat = forward.get("chat_id")
        forward_msg = forward.get("message_id")
        if forward_chat and forward_msg:
            return forward_chat, forward_msg
    except Exception:
        logger.debug("handlers: in _extract_large_file_source()")

    chat_id = current_file.get("chat_id") or current_file.get("forward_chat_id")
    message_id = current_file.get("msg_id") or current_file.get("message_id") or current_file.get("forward_message_id")
    return chat_id, message_id


def _parse_time_to_seconds(tstr: str) -> float:
    """Parse time strings like HH:MM:SS(.ms), MM:SS(.ms) or plain seconds -> seconds (float)."""
    try:
        parts = tstr.strip().split(":")
        if len(parts) == 3:
            h = int(parts[0])
            m = int(parts[1])
            s = float(parts[2])
            return h * 3600 + m * 60 + s
        elif len(parts) == 2:
            m = int(parts[0])
            s = float(parts[1])
            return m * 60 + s
        else:
            return float(parts[0])
    except Exception as e:
        raise ValueError(f"Invalid time format: {tstr}") from e


# Fallback bitrate for every video -> MP3 extraction when the user has not
# picked one explicitly. Mirrors utils.callbacks.MP3_DEFAULT_BITRATE.
_DEFAULT_AUDIO_BITRATE = "128k"

# Accepted bitrate range. Anything outside it (or non-numeric) falls back to
# the default so a user-supplied string can never reach the ffmpeg command line.
_AUDIO_BITRATE_MIN_KBPS = 32
_AUDIO_BITRATE_MAX_KBPS = 320

# A bulk apply holds the collected list until it finishes, so pressing Apply
# again while the first run is still going would process every file twice. The
# guard is time-bounded so a crashed apply can never lock a user out of their own
# batch until it expires.
_BULK_APPLY_GUARD_SECONDS = 12 * 3600

# How long a bulk apply waits for one job to reach a terminal state before it
# gives up and says so. Generous - a 900 MB conversion legitimately takes hours -
# but not infinite: an unresponsive worker used to hang the whole apply silently,
# which the user experiences as a batch frozen forever with no explanation.
_BULK_JOB_WAIT_SECONDS = float(os.environ.get("BULK_JOB_WAIT_SECONDS", str(6 * 3600)))

# How long the apply waits for ONE file to become available locally before it
# gives up on that file and moves to the next.
#
# Every wait in the fetch path is bounded on its own (the Pyrogram download has
# PIPELINE_DOWNLOAD_TIMEOUT_SECONDS, the pipeline's cancel watch polls), but this
# is the backstop that makes the loop itself unable to hang: a fetch that outlives
# this is abandoned, reported per-file, and the batch carries on instead of
# sitting on file 7 of 30 forever with the remaining files never queued. It is
# deliberately longer than the download timeout, so a slow-but-working download
# is never cut short by it. It bounds the *fetch* only: the conversion job a
# pipeline fetch queues is waited out by _await_bulk_pipeline_job, outside this
# bound, under _BULK_JOB_WAIT_SECONDS like every other job the apply queues.
_BULK_FETCH_TIMEOUT_SECONDS = float(os.environ.get("BULK_FETCH_TIMEOUT_SECONDS", str(45 * 60)))


# ── Bulk mode ────────────────────────────────────────────────────────────────
# Each toggle maps to the same encoding the matching single-file action uses, so
# bulk results look like the ones produced from the per-file menus.
#
# Compress quality and the Optimize preset are user picks (the bulk menu's quality
# row), stored per user as `bulk_crf` / `bulk_optimize_preset`. The values below
# are only the fallbacks used until the user changes them; keep the choices in
# sync with utils.callbacks.BULK_CRF_* and BULK_PRESET_*.

# Compress quality: libx264 CRF, lower is better quality. Mirrors
# utils.callbacks.BULK_CRF_DEFAULT.
_BULK_COMPRESS_CRF_DEFAULT = 28
_BULK_COMPRESS_CRF_MIN = 18
_BULK_COMPRESS_CRF_MAX = 51
_BULK_COMPRESS_AUDIO_ARGS = ["-c:a", "aac", "-b:a", "128k"]

# Optimize presets, mirroring the preset_map in `optimize_video()`:
# preset -> (encoder preset, crf, audio bitrate). The keys must match
# utils.callbacks.BULK_PRESET_CHOICES.
_BULK_OPTIMIZE_PRESETS: dict[str, tuple[str, int, str]] = {
    "web": ("slow", 23, "128k"),
    "mobile": ("medium", 28, "96k"),
    "tv": ("slow", 20, "192k"),
    "storage": ("veryfast", 35, "64k"),
}
_BULK_OPTIMIZE_DEFAULT = "web"

# Extract Audio bitrate, mirroring utils.callbacks.BULK_BITRATE_DEFAULT.
# Validated through `_sanitize_audio_bitrate` so the pick can never reach the
# ffmpeg command line as free text.
_BULK_EXTRACT_BITRATE_DEFAULT = _DEFAULT_AUDIO_BITRATE

# Convert to MP4 — see convert_video_format(). Values are (video_args, audio_args);
# audio args are replaced by -an when the Remove Audio toggle is also on.
_BULK_CONVERT_ARGS: tuple[list[str], list[str]] = (
    ["-c:v", "libx264", "-movflags", "+faststart"],
    ["-c:a", "aac", "-strict", "experimental"],
)

# Precedence when several video toggles are on: only one encode pass is possible.
_BULK_VIDEO_PRECEDENCE = ("bulk_compress", "bulk_optimize", "bulk_convert_mp4")

_BULK_ACTION_LABELS = {
    "bulk_convert_mp4": "Convert to MP4",
    "bulk_compress": "Compress",
    "bulk_extract_audio": "Extract Audio",
    "bulk_remove_audio": "Remove Audio",
    "bulk_rename": "Rename",
    "bulk_optimize": "Optimize",
}

# Cap on the files auto-collected for the next "Apply Bulk" so a long-lived
# session cannot grow without bound. Oldest entries are dropped first.
_BULK_LIST_LIMIT = 30

# How many per-file result lines the Apply summary lists before truncating.
_BULK_SUMMARY_MAX_LINES = 20

# Seconds each queued photo is shown in a generated slideshow. The value in
# utils.callbacks.BULK_SLIDESHOW_DEFAULT is the menu default; these bounds keep a
# stored pick inside a sane range so it can never reach ffmpeg as free text.
_BULK_SLIDESHOW_SECONDS = 3.0
_BULK_SLIDESHOW_MIN = 0.5
_BULK_SLIDESHOW_MAX = 30.0

# Longest filename shown in the per-file Apply summary before it is elided.
_BULK_NAME_MAX = 32

# Image extensions: a photo sent uncompressed arrives as a document, and these
# are the ones ffmpeg can read back out of the slideshow pipeline.
_IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".bmp")

# "Video Only" extraction: keep the first video stream, drop audio/subtitles.
# Re-encoding to H.264 makes the result playable regardless of the source codec.
_EXTRACT_VIDEO_FFMPEG_ARGS = [
    "-map",
    "0:v:0",
    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    "-crf",
    "23",
    "-an",
    "-movflags",
    "+faststart",
]


def _sanitize_audio_bitrate(value, default: str = _DEFAULT_AUDIO_BITRATE) -> str:
    """Normalize a user-supplied audio bitrate to a safe ``"<kbps>k"`` string."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        value = f"{int(value)}k"
    text = str(value or "").strip().lower()
    if text.endswith("k"):
        text = text[:-1].strip()
    if text.endswith("kbps"):
        text = text[:-4].strip()
    if not text.isdigit():
        return default
    kbps = int(text)
    if not _AUDIO_BITRATE_MIN_KBPS <= kbps <= _AUDIO_BITRATE_MAX_KBPS:
        return default
    return f"{kbps}k"


def _audio_delivery_name(name: str | None, fallback_id=None, extension: str = ".mp3") -> str:
    """Build the filename used when delivering an extracted audio file.

    The original media name is preserved (only the extension is swapped) so the
    user receives ``My Video.mp3`` instead of an opaque storage key.
    """
    stem = os.path.basename((name or "").strip())
    stem = os.path.splitext(stem)[0]
    if not stem:
        stem = f"audio_{fallback_id}" if fallback_id else "audio"
    return f"{stem}{extension}"


def _metadata_caption(current_file: dict | None, fallback: str | None = None) -> str:
    """Build a metadata-derived caption from the captured source metadata.

    Prefer title/performer tags when present, otherwise fall back to the
    original media filename stem, then the supplied fallback string.
    """
    metadata = {}
    if current_file:
        metadata = current_file.get("_source_metadata") or current_file.get("source_metadata") or {}

    def _first(*candidates):
        for candidate in candidates:
            value = metadata.get(candidate)
            if value is None:
                continue
            value = str(value).strip()
            if value:
                return value
        return ""

    title = _first("title", "source_title")
    performer = _first("performer", "artist", "artists", "album_artist", "author")

    if title and performer:
        return f"{title} — {performer}"
    if title:
        return title
    if performer:
        return performer

    if fallback:
        return fallback

    name = (
        current_file.get("name") or current_file.get("original_filename") or current_file.get("output_filename") or ""
    )
    stem = os.path.splitext(os.path.basename(name))[0].strip()
    if stem:
        return stem

    return "media"


def _bulk_rename_filename(filename: str | None, settings: dict | None) -> tuple[str, bool]:
    """Rename ``filename`` using the user's prefix/suffix/words-to-remove settings.

    Returns ``(new_name, changed)``. The extension is always preserved.
    """
    name = os.path.basename(filename or "")
    stem, ext = os.path.splitext(name)
    if not stem:
        return name, False

    settings = settings or {}
    new_stem = stem
    for word in settings.get("words_remove") or []:
        if word:
            new_stem = new_stem.replace(str(word), "")
    new_stem = new_stem.strip() or stem
    renamed = f"{settings.get('prefix') or ''}{new_stem}{settings.get('suffix') or ''}".strip() or new_stem
    new_name = f"{renamed}{ext}"
    return new_name, new_name != name


def _parse_bulk_crf(value) -> int | None:
    """Parse a user-supplied bulk CRF, returning ``None`` when out of range.

    Separate from the sanitizer so the text-input path can tell "invalid" apart
    from "unset", which the sanitizer deliberately collapses into the default.
    """
    if isinstance(value, bool):
        return None
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    if _BULK_COMPRESS_CRF_MIN <= number <= _BULK_COMPRESS_CRF_MAX:
        return number
    return None


def _sanitize_bulk_crf(value, default: int = _BULK_COMPRESS_CRF_DEFAULT) -> int:
    """Coerce a stored bulk CRF to a valid libx264 value.

    Anything unusable (missing, non-numeric, out of range) falls back to the
    default so a stale setting can never reach the ffmpeg command line.
    """
    parsed = _parse_bulk_crf(value)
    return default if parsed is None else parsed


def _sanitize_bulk_preset(value, default: str = _BULK_OPTIMIZE_DEFAULT) -> str:
    """Coerce a stored bulk optimize preset to a known preset name."""
    name = str(value or "").strip().lower()
    return name if name in _BULK_OPTIMIZE_PRESETS else default


def _sanitize_bulk_extract_bitrate(value, default: str = _BULK_EXTRACT_BITRATE_DEFAULT) -> str:
    """Coerce a stored bulk Extract Audio bitrate to a safe ``"<kbps>k"`` string.

    Shares the single-file sanitizer, so the accepted range and the normalising
    of ``128`` / ``128kbps`` / `` 128K `` are identical to the MP3 picker.
    """
    return _sanitize_audio_bitrate(value, default=default)


def _sanitize_bulk_slideshow_seconds(value, default: float = _BULK_SLIDESHOW_SECONDS) -> float:
    """Coerce a stored slideshow seconds-per-photo value into the valid range.

    Anything unusable (missing, non-numeric, out of range) falls back to the
    default so a stale setting can never reach the ffmpeg command line.
    """
    if isinstance(value, bool):
        return default
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return default
    if not (_BULK_SLIDESHOW_MIN <= seconds <= _BULK_SLIDESHOW_MAX):
        return default
    return seconds


def _bulk_video_recipe(key: str, settings: dict | None = None) -> tuple[list[str], list[str]]:
    """Video and audio args for one video toggle, honoring the quality picks.

    Returns ``(video_args, audio_args)``; the caller replaces the audio args with
    ``-an`` when Remove Audio is also on.
    """
    settings = settings or {}

    if key == "bulk_compress":
        crf = _sanitize_bulk_crf(settings.get("bulk_crf"))
        return (
            ["-c:v", "libx264", "-preset", "medium", "-crf", str(crf), "-movflags", "+faststart"],
            list(_BULK_COMPRESS_AUDIO_ARGS),
        )

    if key == "bulk_optimize":
        preset = _sanitize_bulk_preset(settings.get("bulk_optimize_preset"))
        encoder, crf, bitrate = _BULK_OPTIMIZE_PRESETS[preset]
        return (
            ["-c:v", "libx264", "-preset", encoder, "-crf", str(crf), "-movflags", "+faststart"],
            ["-c:a", "aac", "-b:a", bitrate],
        )

    return list(_BULK_CONVERT_ARGS[0]), list(_BULK_CONVERT_ARGS[1])


def _bulk_quality_label(plan: dict) -> str:
    """Describe the encoding quality a plan will use, for the Apply summary."""
    if "bulk_compress" in plan.get("applied", []):
        return f"CRF {plan.get('crf')}"
    if "bulk_optimize" in plan.get("applied", []):
        preset = plan.get("optimize_preset") or _BULK_OPTIMIZE_DEFAULT
        return f"Optimize preset: {preset}"
    if "bulk_extract_audio" in plan.get("applied", []):
        return f"MP3 {plan.get('extract_bitrate') or _BULK_EXTRACT_BITRATE_DEFAULT}"
    return ""


def _resolve_bulk_plan(settings: dict | None) -> dict:
    """Resolve the bulk toggles into the single ffmpeg pass applied to each file.

    A worker job is one ffmpeg invocation, so contradictory toggles cannot all
    run. Extraction wins over video work (it discards video anyway), Compress
    wins over Optimize over Convert, and Remove Audio / Rename act as
    modifiers. Dropped toggles are reported in ``ignored`` so the user is told
    instead of silently getting something else.

    Compress uses the user's ``bulk_crf`` and Optimize the user's
    ``bulk_optimize_preset`` (both defaulted and validated here).

    Returns a dict with ``ffmpeg_args``, ``output_ext``, ``convert_type``,
    ``applied``, ``ignored``, ``rename``, ``crf`` and ``optimize_preset``.
    """
    settings = settings or {}

    extract_audio = bool(settings.get("bulk_extract_audio"))
    remove_audio = bool(settings.get("bulk_remove_audio"))
    rename = bool(settings.get("bulk_rename"))
    video_keys = [key for key in _BULK_VIDEO_PRECEDENCE if settings.get(key)]

    crf = _sanitize_bulk_crf(settings.get("bulk_crf"))
    optimize_preset = _sanitize_bulk_preset(settings.get("bulk_optimize_preset"))
    extract_bitrate = _sanitize_bulk_extract_bitrate(settings.get("bulk_extract_bitrate"))

    ignored: list[str] = []
    if extract_audio:
        ffmpeg_args = ["-vn", "-acodec", "libmp3lame", "-ab", extract_bitrate]
        output_ext, convert_type = ".mp3", "extract_audio"
        applied = ["bulk_extract_audio"]
        # Video work and audio removal are meaningless once the audio is the
        # only thing being delivered.
        ignored = list(video_keys) + (["bulk_remove_audio"] if remove_audio else [])
    elif video_keys:
        chosen = video_keys[0]
        video_args, audio_args = _bulk_video_recipe(chosen, settings)
        applied = [chosen]
        ignored = list(video_keys[1:])
        if remove_audio:
            ffmpeg_args = list(video_args) + ["-an"]
            applied.append("bulk_remove_audio")
        else:
            ffmpeg_args = list(video_args) + list(audio_args)
        output_ext, convert_type = ".mp4", "ffmpeg"
    elif remove_audio:
        # Stream copy keeps this lossless and fast.
        ffmpeg_args = ["-an", "-c:v", "copy"]
        output_ext, convert_type = ".mp4", "ffmpeg"
        applied = ["bulk_remove_audio"]
    else:
        # Nothing selected: keep the historic default (MP4 conversion).
        video_args, audio_args = _bulk_video_recipe("bulk_convert_mp4", settings)
        ffmpeg_args = list(video_args) + list(audio_args)
        output_ext, convert_type = ".mp4", "ffmpeg"
        applied = ["bulk_convert_mp4"]

    if rename:
        applied.append("bulk_rename")

    return {
        "ffmpeg_args": ffmpeg_args,
        "output_ext": output_ext,
        "convert_type": convert_type,
        "applied": applied,
        "ignored": ignored,
        "rename": rename,
        "crf": crf,
        "optimize_preset": optimize_preset,
        "extract_bitrate": extract_bitrate,
    }


_BULK_PHOTO_ACTIONS = ("bulk_convert_mp4", "bulk_compress", "bulk_optimize")


def _bulk_photo_supported(plan: dict | None) -> bool:
    """Whether an image input can run this resolved plan.

    A photo has no audio or video stream, so only an actual video encode
    (Convert / Compress / Optimize) can produce something from it. Extract
    Audio (MP3 out) and remove-audio-only (a ``-c:v copy`` stream copy) cannot.
    """
    plan = plan or {}
    if plan.get("output_ext") != ".mp4":
        return False
    applied = plan.get("applied") or []
    return any(key in applied for key in _BULK_PHOTO_ACTIONS)


def _read_bulk_settings(user_id, session: dict | None) -> dict:
    """Read the bulk settings the same way Apply does (settings store, else session).

    Reading and writing through one pair keeps the menu, the toggles, the quality
    pickers and Apply from ever looking at different stores.
    """
    try:
        if user_settings:
            return user_settings.get_user_settings(user_id) or {}
        return (session or {}).get("bulk_settings") or {}
    except Exception:
        logger.exception("Failed to read bulk settings for %s", user_id)
        return {}


def _bulk_item_key(item: dict) -> object:
    """Stable identity for a collected bulk file (Telegram file id preferred)."""
    return item.get("id") or item.get("file_unique_id") or item.get("path")


def _bulk_entry_key(entry) -> str | None:
    """Identity for one collected entry, as a string, for resume bookkeeping.

    Survives a restart, because it is the Telegram file id rather than anything
    process-local: that is what lets the next Apply tell which files an
    interrupted run had already finished.
    """
    try:
        if isinstance(entry, dict):
            key = _bulk_item_key(entry)
            return str(key) if key else None
        text = str(entry or "").strip()
        return text or None
    except Exception:
        return None


def _bulk_display_name(file_info: dict | None) -> str:
    """Short, human label for one batch entry in the per-file Apply summary."""
    info = file_info or {}
    name = info.get("name") or os.path.basename(str(info.get("path") or "")) or str(info.get("id") or "file")
    name = str(name)
    if len(name) > _BULK_NAME_MAX:
        stem, ext = os.path.splitext(name)
        keep = max(1, _BULK_NAME_MAX - len(ext) - 1)
        name = f"{stem[:keep]}…{ext}" if ext else f"{name[: _BULK_NAME_MAX - 1]}…"
    return name


def _batch_stop_markup(batch_id):
    """The one button a running apply needs, or None when there is no batch."""
    if not batch_id:
        return None
    try:
        return InlineKeyboardMarkup([[InlineKeyboardButton("⏹️ Stop batch", callback_data=f"batch_cancel:{batch_id}")]])
    except Exception:
        return None


# A job status/message pair as the stage it represents in the chat. Keyed off the
# worker's own vocabulary (see workers/ffmpeg_worker.py and run_ffmpeg) so the
# apply never has to know which worker is running the file.
def _batch_member_stage(info: dict | None) -> tuple[str, str]:
    """``(emoji, stage)`` for one member, from its job hash."""
    data = info or {}
    status = str(data.get("status") or "queued").strip().lower()
    message = str(data.get("message") or "").strip()
    progress = str(data.get("progress") or "0").strip()
    low = f"{status} {message}".lower()
    if status in ("done", "completed"):
        return "✅", "delivered"
    if status in ("error", "failed"):
        return "❌", message or "failed"
    if status in ("cancelled", "canceled"):
        return "⏹️", "stopped"
    if "upload" in low or "sending" in low:
        return "📤", f"Sending to Telegram — {progress}%"
    if "encod" in low:
        return "🎬", f"Encoding — {progress}%"
    if "waiting" in low:
        return "⏳", message or "waiting for the worker"
    if "fetch" in low or "download" in low or "storag" in low:
        if progress not in ("", "0", "0.0"):
            return "⬇️", f"Fetching source from storage — {progress}%"
        return "⬇️", "Fetching source from storage"
    if status == "queued":
        return "⏳", "queued"
    return "🔄", message or status


def _batch_member_text(batch_id, index, total, name, info: dict | None = None) -> str:
    """The apply's whole message while one file is being fetched, coded and sent.

    One apply shows one message. The pipeline's download progress, the worker's
    queueing, its encode percentage and the delivery all render here, so a 29-file
    batch adds no per-file messages to the chat: the result the worker delivers is
    the only new message a member produces.
    """
    emoji, stage = _batch_member_stage(info)
    head = f"▶️ Batch `{batch_id}`\n" if batch_id else ""
    try:
        label = f"File {int(index)} of {int(total)} — {name}" if index and total else str(name or "")
    except (TypeError, ValueError):
        label = str(name or "")
    body = f"{label}\n{emoji} {stage}" if label else f"{emoji} {stage}"
    return f"{head}{body}\nConversions run one file at a time — each result arrives as its job finishes."


def _bulk_batch_lines(entries, limit: int = 12) -> list[str]:
    """Numbered ``name · type`` lines describing the queued batch.

    Names are HTML-escaped because the menu is rendered with ``parse_mode=HTML``
    and a filename is user-supplied text.
    """
    lines = []
    for index, entry in enumerate(list(entries or [])[:limit], start=1):
        item = _normalize_bulk_item(entry)
        if item is None:
            continue
        kind = str(item.get("type") or "file")
        lines.append(f"{index}. {html.escape(_bulk_display_name(item))} · {html.escape(kind)}")
    remaining = max(0, len(entries or []) - limit)
    if remaining:
        lines.append(f"… +{remaining} more")
    return lines


def _bulk_slideshow_music(entries):
    """The first queued audio entry — used as the slideshow's background music."""
    for entry in entries or []:
        item = _normalize_bulk_item(entry)
        if item is not None and item.get("type") == "audio":
            return item
    return None


def _normalize_bulk_item(item):
    """Coerce a bulk/merge list entry to the file-dict shape Apply expects.

    Entries reach the list two ways: auto-collected sends and album photos store
    dicts, while the merge menu's "Add File" button stores a bare path string.
    Normalising here keeps the bulk loop from tripping over either shape.
    """
    if isinstance(item, dict):
        return item
    if isinstance(item, str) and item:
        name = os.path.basename(item)
        return {"path": item, "id": name, "name": name}
    return None


def _bulk_failure_reason(exc: BaseException) -> str:
    """Condense a bulk fetch failure into the few words the user needs.

    The per-file summary used to say only "could not fetch" for every outcome,
    which hid whether the file was too large for the Bot API, whether the
    userbot or relay fallback failed, or whether Telegram throttled us. Those
    have completely different fixes, so the reason belongs in the message the
    user already reads.
    """
    text = str(exc or "").strip()
    lowered = text.lower()
    if "too big" in lowered or "exceeds" in lowered:
        return "too large for the Bot API"
    if "cancel" in lowered:
        return "stopped"
    if "relay" in lowered:
        return "relay group failed"
    if "userbot" in lowered:
        return "userbot download failed"
    if "flood" in lowered or "too many requests" in lowered or "retryafter" in lowered:
        return "Telegram rate limit"
    if "timeout" in lowered or "timed out" in lowered:
        return "timed out"
    return (text[:60] or "download failed").replace("\n", " ")


def _register_bulk_file(session: dict | None, file_info: dict | None) -> bool:
    """Auto-collect a just-sent file for the next bulk Apply.

    Idempotent per file id and capped at ``_BULK_LIST_LIMIT`` so re-sending the
    same file does not process it twice. Returns True when newly collected.
    """
    if session is None or not isinstance(file_info, dict):
        return False
    try:
        queue = session.setdefault("bulk_list", [])
        key = _bulk_item_key(file_info)
        if not key:
            return False
        for item in queue:
            if isinstance(item, dict) and _bulk_item_key(item) == key:
                return False
        if len(queue) >= _BULK_LIST_LIMIT:
            # The cap is a hard limit on one apply, and silently dropping the
            # oldest file is the sort of thing a user only discovers by counting.
            # Name what was dropped; the bulk menu warns about the cap as well.
            _dropped = queue.pop(0)
            logger.warning(
                "bulk: batch is full (%d files); dropped the oldest entry %s",
                _BULK_LIST_LIMIT,
                _bulk_display_name(_dropped) if isinstance(_dropped, dict) else _dropped,
            )
        queue.append(file_info)
        return True
    except Exception:
        logger.exception("Failed to register file for bulk processing")
        return False


def _write_bulk_setting(user_id, session: dict | None, key: str, value) -> None:
    """Persist one bulk setting next to the toggles (settings store or session)."""
    try:
        if user_settings:
            user_settings.set_user_setting(user_id, key, value)
        elif session is not None:
            session.setdefault("bulk_settings", {})[key] = value
    except Exception:
        logger.exception("Failed to persist bulk setting %s", key)


def _format_seconds_to_hhmmss(sec: float) -> str:
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec % 60
    # keep millisecond precision when present
    if abs(s - int(s)) > 0:
        return f"{h:02d}:{m:02d}:{s:06.3f}"
    else:
        return f"{h:02d}:{m:02d}:{int(s):02d}"


# Optional ffmpeg-python binding (best-effort)
try:
    import ffmpeg
except Exception:
    ffmpeg = None

# Probe helper for durations
try:
    from utils.ffmpeg_runner import probe_duration
except Exception:
    probe_duration = None

# Optional MongoDB model (best-effort import)
try:
    from models import MediaConversionModel
except Exception:
    MediaConversionModel = None

# Conversation states
SELECT_TIME, SELECT_RESOLUTION, SELECT_BITRATE, MERGE_FILES, CUSTOM_INPUT = range(5)


class EnhancedMediaHandler:
    def __init__(self, max_concurrent_conversions: int = 5):
        if ExtendedMediaConverter is None:
            raise ImportError("ExtendedMediaConverter not available")
        self.converter = ExtendedMediaConverter()
        self.user_sessions: dict[int, dict] = {}
        self.session_timeouts: dict[int, asyncio.TimerHandle] = {}
        self.db_model = None  # Optional MongoDB model
        self._session_timeout_seconds = 3600  # 1 hour inactivity timeout

        # Concurrency limiter for conversions
        self.conversion_semaphore = asyncio.Semaphore(max_concurrent_conversions)
        self._max_conversions = max_concurrent_conversions
        self.active_conversions: dict[int, str] = {}  # user_id -> task_name
        self._active_conversion_count: dict[int, int] = {}  # user_id -> running count
        # Telemetry for malformed callbacks
        self.bad_callback_counts: dict[str, int] = {}

        # Ensure session persistence directory exists for multi-worker setups
        self._session_store_dir = os.path.join(os.path.dirname(__file__), "storage", "temp_sessions")
        try:
            os.makedirs(self._session_store_dir, exist_ok=True)
        except Exception:
            # Best-effort; continue if cannot create
            logger.debug("Could not create session store dir: %s", self._session_store_dir)

        # Redis cache for media analysis, user preferences, and file metadata
        self._cache = None
        try:
            if get_cache is not None:
                # asyncio is already imported at module level
                loop = None
                with contextlib.suppress(RuntimeError):
                    loop = asyncio.get_running_loop()
                if loop and loop.is_running():
                    asyncio.ensure_future(self._init_cache())
        except Exception:
            logger.debug("handlers: Redis cache for media analysis, user preferences, and file metadata")

    async def _cleanup_session(self, user_id: int):
        """Cleanup user session asynchronously.

        With concurrent_updates, a handler may still be using this session
        when the inactivity timer fires.  We reschedule if the user has an
        active conversion — the conversion's own timeout handles the real
        cleanup.
        """
        if user_id not in self.user_sessions:
            return

        # A conversion is still running for this user — don't tear down the
        # session out from under it.
        if user_id in self.active_conversions:
            try:
                loop = asyncio.get_running_loop()
                handle = loop.call_later(
                    self._session_timeout_seconds,
                    lambda: asyncio.create_task(self._cleanup_session(user_id)),
                )
                self.session_timeouts[user_id] = handle
            except RuntimeError:
                pass
            return

        session = self.user_sessions[user_id]

        try:
            # Clean temp files
            if "current_file" in session:
                temp_path = session["current_file"].get("path")
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                        logger.info(f"Cleaned up temp file: {temp_path}")
                    except Exception as e:
                        logger.error(f"Failed to cleanup {temp_path}: {e}")

            # Clean merge list files (entries may be paths, not dicts)
            if "merge_list" in session:
                for file_info in session["merge_list"]:
                    temp_path = file_info.get("path") if isinstance(file_info, dict) else file_info
                    if temp_path and os.path.exists(temp_path):
                        with contextlib.suppress(OSError):
                            os.remove(temp_path)

            # Clean auto-collected bulk files
            if "bulk_list" in session:
                for file_info in session["bulk_list"]:
                    temp_path = file_info.get("path") if isinstance(file_info, dict) else file_info
                    if temp_path and os.path.exists(temp_path):
                        with contextlib.suppress(OSError):
                            os.remove(temp_path)
        finally:
            # Remove session
            if user_id in self.user_sessions:
                del self.user_sessions[user_id]
            if user_id in self.session_timeouts:
                del self.session_timeouts[user_id]

            logger.info(f"Cleaned up session for user {user_id}")

    def _schedule_session_cleanup(self, user_id: int):
        """Schedule automatic cleanup for session."""
        # Cancel existing timer
        if user_id in self.session_timeouts:
            self.session_timeouts[user_id].cancel()

        # Schedule new cleanup
        try:
            loop = asyncio.get_running_loop()
            handle = loop.call_later(
                self._session_timeout_seconds,
                lambda: asyncio.create_task(self._cleanup_session(user_id)),
            )
            self.session_timeouts[user_id] = handle
        except RuntimeError:
            logger.error("Failed to schedule session cleanup - no event loop")

    async def _finalize_media_group(self, user_id: int, media_group_id: str):
        """Called after a short delay to finalize a media_group (album) and add
        its items to the user's merge_list with a single confirmation message.
        """
        try:
            session = self.user_sessions.get(user_id)
            if not session:
                return
            groups = session.setdefault("media_groups", {})
            items = groups.pop(media_group_id, [])
            if not items:
                return

            # Ensure merge_list exists
            if "merge_list" not in session:
                session["merge_list"] = []
            for it in items:
                session["merge_list"].append(it)

            # Persist session
            try:
                self._persist_session(user_id)
            except Exception:
                logger.debug("Could not persist session after finalizing media group")

            # Send a confirmation to the user via direct chat if available
            # We can't access the Update here; best-effort: log the result
            logger.info("Finalized media_group %s for user %s: %d items", media_group_id, user_id, len(items))
        except Exception:
            logger.exception("Failed to finalize media_group %s for user %s", media_group_id, user_id)

    async def _buffer_album_item(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: dict,
        user_id: int,
        kind: str,
    ) -> bool:
        """Collect an album (media group) item instead of showing a per-file menu.

        Telegram delivers an album as one update per item, so without this a
        10-video album produced ten "registered - choose an action" menus. The
        item is already in the bulk batch (the caller registers every sent file),
        so this only counts it and schedules the album's single announcement.

        Returns True when the item belongs to an album and the caller should skip
        the per-file menu.
        """
        message = getattr(update, "message", None)
        media_group_id = getattr(message, "media_group_id", None)
        if not media_group_id:
            return False

        entry = session.setdefault("album_batch", {}).setdefault(
            media_group_id, {"count": 0, "kinds": set(), "chat_id": None, "bot": None}
        )
        entry["count"] += 1
        entry["kinds"].add(kind)
        entry["chat_id"] = getattr(getattr(message, "chat", None), "id", None) or entry["chat_id"]
        entry["bot"] = getattr(context, "bot", None) or entry["bot"]

        timers = session.setdefault("album_batch_timers", {})
        if media_group_id not in timers:
            try:
                loop = asyncio.get_running_loop()
                timers[media_group_id] = loop.call_later(
                    1.5,
                    lambda: asyncio.create_task(self._flush_album_batch(user_id, media_group_id)),
                )
            except RuntimeError:
                await self._flush_album_batch(user_id, media_group_id)
        return True

    async def _flush_album_batch(self, user_id: int, media_group_id: str):
        """Announce an album once, after Telegram has delivered every item."""
        session = self.user_sessions.get(user_id)
        if not session:
            return
        (session.get("album_batch_timers") or {}).pop(media_group_id, None)
        entry = (session.get("album_batch") or {}).pop(media_group_id, None)
        if not entry or not entry.get("count"):
            return

        try:
            self._persist_session(user_id)
        except Exception:
            logger.debug("Could not persist session after an album batch")

        bot = entry.get("bot")
        chat_id = entry.get("chat_id")
        if bot is None or chat_id is None:
            logger.debug("album batch: no bot/chat to announce %s", media_group_id)
            return

        queued = len(session.get("bulk_list") or [])
        kinds = ", ".join(sorted(entry.get("kinds") or []))
        text = (
            f"➕ Added {entry['count']} {kinds or 'file'} from the album to the batch.\n"
            f"📦 Batch size: {queued}\n"
            "Open /bulkmenu and press ▶️ Apply Bulk when you are ready."
        )
        try:
            await bot.send_message(chat_id=chat_id, text=text)
        except Exception:
            logger.debug("album batch: could not announce %s", media_group_id)

    async def _watch_job_progress(
        self,
        query,
        job_id: str,
        poll_interval: float = 1.0,
        progress_msg=None,
        bot=None,
    ):
        """Background task: poll Redis job hash for progress and update the progress message.

        Args:
            query: A CallbackQuery to edit (required for Cancel button reply_markup).
            job_id: The job ID to poll.
            poll_interval: Seconds between polls.
            progress_msg: Optional Message object to edit instead of the query's message.
                          When provided, the query is still used for the Cancel button reply_markup
                          but the main progress text is edited on progress_msg instead.
        """
        try:
            try:
                from utils.job_queue import get_redis
            except Exception:
                get_redis = None

            if not get_redis:
                logger.debug("_watch_job_progress disabled: get_redis not available")
                return

            try:
                r = await get_redis()
            except Exception as e:
                logger.debug("_watch_job_progress could not connect to redis: %s", e)
                return
            last_text = None
            _hash_missing_since = None  # set while ffmpeg:job:<id> reads back empty

            # ── T14: Subscribe to Redis pub/sub for instant progress updates ──
            #    Falls back to polling when pub/sub is unavailable.
            _pubsub_conn = None
            _pubsub_obj = None
            try:
                _pubsub_conn = await get_redis()
                _pubsub_obj = _pubsub_conn.pubsub()
                await _pubsub_obj.subscribe(f"ffmpeg:progress:{job_id}")
                logger.debug("_watch_job_progress: subscribed to ffmpeg:progress:%s", job_id)
            except Exception:
                _pubsub_obj = None
                logger.debug("_watch_job_progress: pubsub unavailable for %s, using polling", job_id)

            _last_edit_time = [0.0]
            _min_edit_interval = 2.0  # Minimum seconds between Telegram edits
            _terminal_pending_since = None  # set once a terminal status is being retried

            # Several watchers (this one per job, the batch member watcher, the
            # big-file pipeline) can render onto the same message. The coalescer
            # keeps them from stacking their edits into Telegram's per-chat limit;
            # the gate stops us calling at all while a flood window is open.
            from utils.rate_limiter import telegram_edit_coalescer, telegram_flood_gate

            _edit_chat_id, _edit_message_id = _edit_target_ids(query, progress_msg)
            _flood_scope = telegram_flood_gate.scope_for_chat(_edit_chat_id)

            async def _edit(text, force: bool = False, **kwargs) -> bool:
                """Edit either progress_msg or the callback query message.

                Throttled to avoid hitting Telegram's429 rate limit.
                Handles RetryAfter (429), BadRequest, and network errors.

                ``force`` bypasses the pacing so a terminal status always lands.

                Returns True when the message is showing ``text`` afterwards - a
                dropped edit has to be distinguishable, because a terminal status
                nobody wrote is the difference between a finished job and a
                progress bar frozen at 40%.
                """
                from telegram.error import RetryAfter as _RetryAfter

                if telegram_edit_coalescer.should_skip(
                    _edit_chat_id, _edit_message_id, text, _min_edit_interval, force=force
                ):
                    # Either this message was just edited, or it already shows this
                    # text; only the second one means nothing is left to do.
                    return telegram_edit_coalescer.shows(_edit_chat_id, _edit_message_id, text)
                if await telegram_flood_gate.should_drop_inline(_flood_scope):
                    return False  # Telegram has stopped accepting writes to this chat

                now = time.time()
                if not force and (now - _last_edit_time[0]) < _min_edit_interval:
                    return False  # Skip this edit to avoid429
                _last_edit_time[0] = now
                if progress_msg:
                    try:
                        await progress_msg.edit_text(text, **kwargs)
                        telegram_edit_coalescer.record(_edit_chat_id, _edit_message_id, text)
                        return True
                    except _RetryAfter as e:
                        _left = await telegram_flood_gate.note(getattr(e, "retry_after", None) or 5, _flood_scope)
                        logger.warning("_edit:429 on progress_msg, window=%.0fs", _left)
                        if _left <= telegram_flood_gate.inline_max:
                            await asyncio.sleep(_left + 0.5)
                        _last_edit_time[0] = time.time()  # reset throttle after wait
                        return False
                    except BadRequest:
                        # "Message is not modified" and friends: the text is there.
                        return True
                    except Exception:
                        logger.debug("_edit: progress_msg edit failed")
                        return False
                else:
                    _result = await self.safe_edit(query, text, **kwargs)
                    if _result is not None:
                        telegram_edit_coalescer.record(_edit_chat_id, _edit_message_id, text)
                        return True
                    return False

            while True:
                try:
                    # T14: Try pubsub first for near-instant wake-up; fall back to polling sleep
                    if _pubsub_obj is not None:
                        try:
                            _ps_msg = await _pubsub_obj.get_message(
                                ignore_subscribe_messages=True, timeout=poll_interval
                            )
                            # If a pubsub message arrives, poll the hash for fresh data immediately
                            # (the message itself is just a progress update; the hash has full state)
                            if _ps_msg and _ps_msg.get("data"):
                                pass  # Fall through to hgetall below
                        except Exception:
                            pass
                    else:
                        await asyncio.sleep(poll_interval)

                    data = await r.hgetall(f"ffmpeg:job:{job_id}")
                    # hgetall returns bytes keys/values when using aioredis
                    if not data:
                        # The hash is the only thing this watcher reads, so once it
                        # is gone there is nothing left to render and polling it
                        # forever just holds a Redis connection per user. That is
                        # reachable now that a deferred delivery keeps a job
                        # ``processing`` for hours: the hash can expire underneath
                        # the watcher, and it must give up rather than spin.
                        if _hash_missing_since is None:
                            _hash_missing_since = time.time()
                        elif (time.time() - _hash_missing_since) > _WATCH_JOB_MISSING_MAX_SECONDS:
                            logger.debug(
                                "_watch_job_progress: job %s hash is gone; stopping the watcher",
                                job_id,
                            )
                            break
                        await asyncio.sleep(poll_interval)
                        continue
                    _hash_missing_since = None
                    # decode
                    info = {
                        k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
                        for k, v in data.items()
                    }
                    status = info.get("status")
                    progress = info.get("progress")
                    message = info.get("message") or ""
                    batch_id = info.get("batch_id")

                    # Phase-aware emoji prefix based on the message content
                    _emoji = "🔄"
                    _msg_lower = (message or "").lower()
                    if "upload" in _msg_lower or "sending" in _msg_lower:
                        _emoji = "📤"
                    elif "convert" in _msg_lower or "process" in _msg_lower or "ffmpeg" in _msg_lower:
                        _emoji = "🎬"
                    elif "done" in _msg_lower or "delivered" in _msg_lower:
                        _emoji = "✅"
                    elif "error" in _msg_lower or "fail" in _msg_lower:
                        _emoji = "❌"
                    elif "cancel" in _msg_lower or _msg_lower == "cancelled":
                        _emoji = "⏹️"
                    batch_line = f"\nBatch: `{batch_id}` (use /cancelbatch to stop)" if batch_id else ""
                    text = f"{_emoji} Job {job_id} — {status or 'processing'}\nProgress: {progress or '0'}%\n{message}{batch_line}"
                    # Build an inline keyboard with Cancel and an optional Progress (web) link
                    status_url = None
                    try:
                        web_base = os.environ.get("WEB_UPLOAD_URL") or os.environ.get("WEBAPP_URL")
                        if web_base:
                            # strip common upload suffixes if present
                            for suf in ("/upload", "/upload/", "/flask/upload", "/flask/upload/"):
                                if web_base.endswith(suf):
                                    web_base = web_base[: -len(suf)]
                                    break
                            web_base = web_base.rstrip("/")
                            status_url = f"{web_base}/status/{job_id}"
                    except Exception:
                        status_url = None

                    if status_url:
                        kb = InlineKeyboardMarkup(
                            [
                                [
                                    InlineKeyboardButton("📊 Progress", url=status_url),
                                    InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}"),
                                ]
                            ]
                        )
                    else:
                        kb = InlineKeyboardMarkup(
                            [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}")]]
                        )
                    _terminal = status in ("done", "error", "cancelled")
                    # Only claim the text once the message really shows it: a dropped
                    # edit that advanced last_text would never be retried, and the
                    # user would keep the stale one.
                    if text != last_text and await _edit(text, force=_terminal, reply_markup=kb):
                        last_text = text

                    if _terminal:
                        if last_text == text:
                            break
                        # The terminal status was swallowed (a flood window is
                        # open on this chat). Keep polling so it lands when the
                        # window closes, bounded so a day-long penalty cannot pin
                        # this task forever.
                        if _terminal_pending_since is None:
                            _terminal_pending_since = time.time()
                        elif (time.time() - _terminal_pending_since) > _FLOOD_TERMINAL_MAX_WAIT_SECONDS:
                            logger.warning(
                                "Job %s finished but its final message could not be written "
                                "(Telegram flood control on this chat); leaving the last one",
                                job_id,
                            )
                            break

                except Exception:
                    logger.debug("Error polling job hash for %s", job_id)
                await asyncio.sleep(poll_interval)

            # final fetch for output or error
            try:
                data = await r.hgetall(f"ffmpeg:job:{job_id}")
                info = (
                    {
                        k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
                        for k, v in data.items()
                    }
                    if data
                    else {}
                )
                status = info.get("status")
                output = info.get("output")
                if status == "done" and output:
                    display_output = output
                    try:
                        # Only generate and display a presigned URL if explicit link delivery is enabled.
                        send_link = config.ENABLE_LINK_SEND
                        if send_link and not (str(output).startswith("http://") or str(output).startswith("https://")):
                            try:
                                from utils.storage import get_storage_backend

                                backend = await get_storage_backend()
                            except Exception:
                                backend = None
                            if backend is not None:
                                try:
                                    presigned = await backend.generate_presigned_get(output)
                                    if presigned:
                                        display_output = presigned
                                except Exception:
                                    logger.debug("handlers: operation failed")
                    except Exception:
                        logger.debug("handlers: operation failed")

                    # ── Build a rich result message with video metadata ──
                    _result_parts = ["✅ **Conversion complete!**"]

                    # Try to extract metadata from the Redis job hash first
                    _in_bytes = info.get("in_bytes")
                    _out_bytes = info.get("out_bytes")

                    if _in_bytes:
                        try:
                            _in_mb = int(_in_bytes) / (1024 * 1024)
                            _result_parts.append(f"📥 Input: `{_in_mb:.1f} MB`")
                        except (ValueError, TypeError):
                            pass
                    if _out_bytes:
                        try:
                            _out_mb = int(_out_bytes) / (1024 * 1024)
                            _result_parts.append(f"📤 Output: `{_out_mb:.1f} MB`")
                            # Show size ratio if both sizes are available
                            if _in_bytes:
                                try:
                                    _ratio = int(_out_bytes) / max(int(_in_bytes), 1)
                                    _result_parts.append(f"📊 Ratio: `{_ratio:.2f}x`")
                                except (ValueError, TypeError, ZeroDivisionError):
                                    pass
                        except (ValueError, TypeError):
                            pass

                    # ── T10/T15: Use Redis-stored output metadata (avoids re-running ffprobe) ──
                    _redis_dur = info.get("output_duration")
                    _redis_w = info.get("output_width")
                    _redis_h = info.get("output_height")
                    _redis_vcodec = info.get("output_video_codec")
                    _redis_acodec = info.get("output_audio_codec")
                    _redis_vbitrate = info.get("output_video_bitrate")
                    _redis_fps = info.get("output_fps")

                    _has_redis_meta = bool(_redis_dur and _redis_w and _redis_h)

                    if _has_redis_meta:
                        # Use Redis-stored metadata (no subprocess needed)
                        if _redis_w and _redis_h:
                            _result_parts.append(f"🖥️ Resolution: `{_redis_w}×{_redis_h}`")
                        if _redis_dur:
                            try:
                                _dur_str = _format_seconds_to_hhmmss(float(_redis_dur))
                                _result_parts.append(f"⏱️ Duration: `{_dur_str}`")
                            except (ValueError, TypeError):
                                pass
                        if _redis_vcodec:
                            _result_parts.append(f"🎞️ Video: `{_redis_vcodec}`")
                        if _redis_acodec:
                            _result_parts.append(f"🔊 Audio: `{_redis_acodec}`")
                        if _redis_fps:
                            _result_parts.append(f"⚡ FPS: `{_redis_fps}`")
                        if _redis_vbitrate:
                            try:
                                _vbit_mbps = int(_redis_vbitrate) / 1_000_000
                                _result_parts.append(f"📊 Bitrate: `{_vbit_mbps:.1f} Mbps`")
                            except (ValueError, TypeError):
                                pass
                    else:
                        # ── Fallback: ffprobe the output file for rich metadata ──
                        # Only probe if output is a local path (not a URL)
                        if not str(display_output).startswith("http") and os.path.exists(str(output)):
                            try:
                                import json as _rj
                                import subprocess as _rsp

                                _ffprobe_bin = getattr(config, "FFMPEG_PATH", "ffmpeg").replace("ffmpeg", "ffprobe")
                                _rp = await asyncio.to_thread(
                                    lambda: _rsp.run(  # noqa: S603
                                        [
                                            _ffprobe_bin,
                                            "-v",
                                            "quiet",
                                            "-print_format",
                                            "json",
                                            "-show_streams",
                                            "-show_format",
                                            str(output),
                                        ],
                                        capture_output=True,
                                        timeout=15,
                                    )
                                )
                                if _rp.returncode == 0:
                                    _probe = _rj.loads(_rp.stdout.decode() or "{}")
                                    _streams = _probe.get("streams", [])
                                    _vcodec = None
                                    _acodec = None
                                    _width = None
                                    _height = None
                                    _duration = None
                                    for s in _streams:
                                        if s.get("codec_type") == "video":
                                            _vcodec = s.get("codec_name", "unknown")
                                            _width = s.get("width")
                                            _height = s.get("height")
                                        elif s.get("codec_type") == "audio":
                                            _acodec = s.get("codec_name", "unknown")
                                    _fmt = _probe.get("format", {})
                                    if _fmt.get("duration"):
                                        with contextlib.suppress(ValueError, TypeError):
                                            _duration = float(_fmt["duration"])

                                    if _width and _height:
                                        _result_parts.append(f"🖥️ Resolution: `{_width}×{_height}`")
                                    if _duration:
                                        _dur_str = _format_seconds_to_hhmmss(_duration)
                                        _result_parts.append(f"⏱️ Duration: `{_dur_str}`")
                                    if _vcodec:
                                        _result_parts.append(f"🎞️ Video: `{_vcodec}`")
                                    if _acodec:
                                        _result_parts.append(f"🔊 Audio: `{_acodec}`")
                            except Exception:
                                logger.debug("handlers: ffprobe metadata extraction failed for result")

                    _result_parts.append(f"📎 Job: `{job_id[:12]}...`")
                    _result_text = "\n".join(_result_parts)
                    await _edit(_result_text)
                elif status == "cancelled":
                    if info.get("cancel_notified"):
                        pass
                    else:
                        await _edit(f"⏹️ Job {job_id} was cancelled.")
                else:
                    await _edit(f"⚠️ Job {job_id} finished with status: {status}")

                try:
                    _queued_chat_id = info.get("queued_message_chat_id")
                    _queued_message_id = info.get("queued_message_id")
                    if _queued_chat_id and _queued_message_id:
                        _queue_bot = bot
                        _msg = getattr(query, "message", None)
                        for _candidate in (
                            _queue_bot,
                            getattr(query, "bot", None),
                            getattr(_msg, "bot", None),
                            getattr(_msg, "_bot", None),
                            getattr(_msg, "_client", None),
                            getattr(getattr(_msg, "chat", None), "bot", None),
                        ):
                            if _candidate is not None:
                                _queue_bot = _candidate
                                break
                        if _queue_bot is None:
                            logger.debug(
                                "handlers: no bot object available to delete queued pipeline notification for %s",
                                job_id,
                            )
                        else:
                            with contextlib.suppress(Exception):
                                await _queue_bot.delete_message(
                                    chat_id=int(_queued_chat_id),
                                    message_id=int(_queued_message_id),
                                )
                except Exception:
                    logger.debug("handlers: failed to delete queued pipeline notification for %s", job_id)
            except Exception:
                logger.debug("handlers: final fetch for output or error")

            # Terminal jobs no longer need their temporary status messages. Delete
            # both the dedicated progress message and the callback message when
            # they are distinct; Telegram BadRequest is harmless if one is gone.
            _messages_to_delete = []
            if progress_msg is not None:
                _messages_to_delete.append(progress_msg)
            _query_message = getattr(query, "message", None)
            if _query_message is not None and _query_message is not progress_msg:
                _messages_to_delete.append(_query_message)
            for _message in _messages_to_delete:
                with contextlib.suppress(Exception):
                    await _message.delete()

            # ── T19: Cleanup pubsub subscription and connections ──
            if _pubsub_obj is not None:
                with contextlib.suppress(Exception):
                    await _pubsub_obj.unsubscribe(f"ffmpeg:progress:{job_id}")
            if _pubsub_conn is not None:
                try:
                    aclose = getattr(_pubsub_conn, "aclose", None)
                    if aclose is not None:
                        await aclose()
                    else:
                        await _pubsub_conn.close()
                except Exception:
                    logger.debug("handlers: pubsub connection cleanup failed")
            try:
                try:
                    aclose = getattr(r, "aclose", None)
                    if aclose is not None:
                        await aclose()
                    else:
                        await r.close()
                except Exception:
                    logger.debug("handlers: operation failed")
            except Exception:
                logger.debug("handlers: operation failed")
        except Exception:
            logger.exception("_watch_job_progress failed for %s", job_id)

    async def _await_job_finished(self, job_id: str, poll_interval: float = 2.0, timeout: float = 0.0) -> str | None:
        """Wait for a queued job to reach a terminal state **without editing Telegram**.

        The bulk pipeline has to know when a file is done before it fetches the
        next source (30 x ~500 MB must never land on disk at once), but it must
        not own the user's message while it waits. :meth:`_watch_job_progress`
        edits the message it is handed and then *deletes* it on terminal status -
        and the batch progress bar used to live on that same message, which is why
        a batch could sit frozen at "0 of N" while its bar was deleted and
        reposted underneath it.

        This polls Redis only. The worker stays the single writer of the batch's
        progress message. Returns the terminal status, or ``None`` when Redis is
        unreachable or the wait times out. The wait is always bounded
        (:data:`_BULK_JOB_WAIT_SECONDS`): an unresponsive worker used to hang the
        apply silently and for good, which the user just sees as a batch frozen
        forever with nobody told anything.
        """
        timeout = _BULK_JOB_WAIT_SECONDS if not timeout else float(timeout)
        if not job_id:
            # Nothing to track. Returning straight away matters: the wait is
            # bounded but long, and a job with no id would otherwise hold the
            # apply for the whole timeout for no reason.
            return None
        try:
            from utils.job_queue import get_redis

            r = await get_redis()
        except Exception:
            logger.debug("_await_job_finished: no redis for %s", job_id)
            return None
        try:
            deadline = time.time() + timeout
            while True:
                with contextlib.suppress(Exception):
                    raw = await r.hget(f"ffmpeg:job:{job_id}", "status")
                    status = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
                    if status in ("done", "error", "cancelled"):
                        return status
                if deadline is not None and time.time() >= deadline:
                    logger.warning("_await_job_finished: timed out waiting for %s", job_id)
                    return None
                await asyncio.sleep(poll_interval)
        finally:
            with contextlib.suppress(Exception):
                aclose = getattr(r, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await r.close()

    async def _bulk_show_fetch_progress(self, query, batch_id, index: int, total: int, file_info: dict) -> None:
        """Render one file's stage onto the message the handler owns.

        This is the apply's only message for the whole batch: the fetch, the
        queueing, the encode percentage and the delivery are all written here by
        whoever knows them at the time (the apply, the pipeline's download
        callback, the member stage watcher), always with the batch id and the Stop
        button kept. The worker's batch bar carries the aggregate view.
        """
        if query is None:
            return
        try:
            # The same message every stage of this file renders onto, so the batch
            # keeps one id, one Stop button and one shape from fetch to delivery.
            await self.safe_edit(
                query,
                _batch_member_text(
                    batch_id,
                    index,
                    total,
                    _bulk_display_name(file_info),
                    {"status": "fetching", "message": "fetching the source", "progress": 0},
                ),
                reply_markup=_batch_stop_markup(batch_id),
            )
        except Exception:
            logger.debug("bulk apply: could not show fetch progress")

    async def _batch_worker_counts_job(self, batch_id, job_id) -> bool:
        """Whether the worker will count this job toward the batch's progress.

        A job stamped with the batch id advances the batch counter when the
        worker finishes it running. A job created outside the batch (a pipeline
        job the user already had in flight) carries no such tag and stays
        invisible to that counter, so the handler has to count it instead.
        Counting a job both ways finished the batch at half its files and took
        its progress message down while files were still queued.
        """
        if not batch_id or not job_id:
            return False
        try:
            from utils.job_queue import get_redis

            r = await get_redis()
        except Exception:
            # Saved by the caller's own failure handling: with no Redis the
            # counter cannot be advanced either way.
            return False
        try:
            raw = await r.hget(f"ffmpeg:job:{job_id}", "batch_id")
        except Exception:
            logger.debug("bulk apply: could not read the batch tag of job %s", job_id)
            # Fails towards the worker: it is the primary counter, and assuming
            # otherwise would leave the bar one short forever.
            return True
        finally:
            with contextlib.suppress(Exception):
                aclose = getattr(r, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await r.close()
        value = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
        return bool(value) and str(value) == str(batch_id)

    async def _close_batch_message(self, context, batch_id: str) -> None:
        """Delete a finished batch's progress message and stop tracking it.

        The worker takes the message down itself whenever its counter matches the
        batch total. When files were skipped at enqueue time the total it saw was
        the higher estimate, so this closes the batch out once the handler has
        waited for every job it queued.
        """
        from utils.batch_pipeline import batch_message_key, parse_batch_message_ref
        from utils.job_queue import get_redis

        r = await get_redis()
        try:
            stored = await r.get(batch_message_key(batch_id))
            if stored:
                ref = parse_batch_message_ref(stored)
                if ref:
                    with contextlib.suppress(Exception):
                        await context.bot.delete_message(chat_id=ref[0], message_id=ref[1])
                await r.delete(batch_message_key(batch_id))
        finally:
            with contextlib.suppress(Exception):
                aclose = getattr(r, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await r.close()

    # ---------- Session persistence helpers (simple JSON store) ----------
    def _session_file(self, user_id: int) -> str:
        return os.path.join(self._session_store_dir, f"session_{user_id}.json")

    def _persist_session(self, user_id: int) -> None:
        """Persist minimal session info to disk for cross-worker retrieval."""
        try:
            session = self.user_sessions.get(user_id)
            if not session:
                # remove existing file if session cleared
                path = self._session_file(user_id)
                if os.path.exists(path):
                    with contextlib.suppress(OSError):
                        os.remove(path)
                return
            minimal = {
                "current_file": session.get("current_file"),
                "merge_list": session.get("merge_list", []),
                "bulk_list": session.get("bulk_list", []),
                "_bulk_apply_started_at": session.get("_bulk_apply_started_at"),
            }
            # Write locally for fast local recovery
            try:
                with open(self._session_file(user_id), "w", encoding="utf-8") as fh:
                    json.dump(minimal, fh, ensure_ascii=False)
            except Exception:
                logger.exception("Failed to write local session file for %s", user_id)

            # Persist to MongoDB asynchronously when available (best-effort)
            try:
                if getattr(self, "db_model", None):
                    try:
                        try:
                            loop = asyncio.get_running_loop()
                            asyncio.create_task(self.db_model.save_session(user_id, minimal))
                        except RuntimeError:
                            # No running loop — create one and run synchronously
                            loop = asyncio.new_event_loop()
                            with contextlib.suppress(Exception):
                                loop.run_until_complete(self.db_model.save_session(user_id, minimal))
                            loop.close()
                    except Exception:
                        logger.exception("Failed scheduling DB session save for %s", user_id)
            except Exception:
                logger.debug("No db_model available to persist session for %s", user_id)
        except Exception:
            logger.exception("Failed to persist session for user %s", user_id)

    def _load_persisted_session(self, user_id: int) -> dict | None:
        """Load persisted session if available. Returns session dict or None."""
        try:
            path = self._session_file(user_id)
            if not os.path.exists(path):
                # Try loading from MongoDB when available (best-effort)
                try:
                    if getattr(self, "db_model", None):
                        try:
                            import asyncio as _asyncio
                            import queue
                            import threading

                            q = queue.Queue()

                            def _runner():
                                try:
                                    # Wrap in wait_for to prevent thread pile-up
                                    # when MongoDB is unreachable (connection timeout).
                                    _timeout_coro = _asyncio.wait_for(
                                        self.db_model.load_session(user_id),
                                        timeout=2.0,
                                    )
                                    res = _asyncio.run(_timeout_coro)
                                    q.put(res)
                                except TimeoutError:
                                    q.put(None)
                                except Exception:
                                    q.put(None)

                            t = threading.Thread(target=_runner, daemon=True)
                            t.start()
                            try:
                                res = q.get(timeout=3)
                            except Exception:
                                res = None
                            return res
                        except Exception:
                            return None
                except Exception:
                    return None
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            # Ensure merge_list present
            if "merge_list" not in data:
                data["merge_list"] = []
            if "bulk_list" not in data:
                data["bulk_list"] = []
            return data
        except Exception:
            logger.exception("Failed to load persisted session for user %s", user_id)
            return None

    async def _run_with_concurrency_limit(self, user_id: int, task_name: str, coroutine):
        """Run a conversion task with concurrency limiting.

        Overlapping conversions for the same user are tracked with a per-user
        counter so that one file finishing does not erase another's entry.
        """
        async with self.conversion_semaphore:
            self.active_conversions[user_id] = task_name
            self._active_conversion_count[user_id] = self._active_conversion_count.get(user_id, 0) + 1
            try:
                return await coroutine
            finally:
                count = self._active_conversion_count.get(user_id, 0) - 1
                if count <= 0:
                    self.active_conversions.pop(user_id, None)
                    self._active_conversion_count.pop(user_id, None)
                else:
                    self._active_conversion_count[user_id] = count

    def get_active_conversions(self) -> dict[int, str]:
        """Get all active conversions."""
        return self.active_conversions.copy()

    async def safe_edit(self, query, text, **kwargs):
        """Safely edit a callback-query message, ignoring 'Message is not modified'
        and handling Telegram429 rate limits with exponential backoff.

        A flood window longer than the gate's inline maximum is never slept off: a
        long 429 means Telegram has stopped accepting writes to this chat, so
        blocking the handler for hours only makes the bot look dead. The edit is
        dropped instead, and the gate keeps the next calls from firing.

        Returns the API result, ``EDIT_UNCHANGED`` when the message already showed
        that text, or None when the edit was dropped.
        """
        from telegram.error import NetworkError, RetryAfter, TimedOut

        from utils.rate_limiter import telegram_flood_gate

        _chat_id, _message_id = _edit_target_ids(query)
        _scope = telegram_flood_gate.scope_for_chat(_chat_id)
        if await telegram_flood_gate.should_drop_inline(_scope):
            logger.debug(
                "safe_edit: skipped, Telegram flood control open for %.0fs more (scope=%s)",
                await telegram_flood_gate.remaining(_scope),
                _scope,
            )
            return None

        _max_retries = 3
        for _attempt in range(_max_retries):
            try:
                return await query.edit_message_text(text, **kwargs)
            except RetryAfter as e:
                _left = await telegram_flood_gate.note(getattr(e, "retry_after", None) or 5, _scope)
                if _left > telegram_flood_gate.inline_max:
                    logger.warning(
                        "safe_edit: Telegram flood control for %.0fs; dropping the edit instead of "
                        "blocking the handler (scope=%s)",
                        _left,
                        _scope,
                    )
                    return None
                # Short window — worth waiting out, then retrying the edit.
                logger.warning(
                    "safe_edit: Telegram rate limit (429), waiting %ss before retry (attempt %d/%d)",
                    _left,
                    _attempt + 1,
                    _max_retries,
                )
                await asyncio.sleep(_left + 0.5)  # small buffer
                continue
            except BadRequest as e:
                msg = str(e)
                # Log full BadRequest details for debugging
                try:
                    msg_obj = getattr(query, "message", None)
                    chat_obj = getattr(msg_obj, "chat", None) if msg_obj else None
                    chat_id = getattr(chat_obj, "id", None) if chat_obj else None

                    await self._log_bad_callback(
                        "BadRequest_edit",
                        {
                            "error": msg,
                            "callback_data": getattr(query, "data", None),
                        },
                        getattr(getattr(query, "from_user", None), "id", None),
                        chat_id,
                        getattr(msg_obj, "message_id", None),
                    )
                except Exception:
                    logger.exception("Failed to log BadRequest in safe_edit")

                if "Message is not modified" in msg or "specified new message content" in msg:
                    logger.debug("Ignored MessageNotModified error during edit")
                    return EDIT_UNCHANGED

                # Fall back to sending a new message if editing fails for other reasons
                try:
                    if getattr(query, "message", None):
                        return await query.message.reply_text(text, **kwargs)
                except Exception:
                    logger.exception("Fallback reply_text failed after edit BadRequest")

                # If fallback not possible, re-raise the original exception
                raise
            except (TimedOut, NetworkError) as e:
                if _attempt < _max_retries - 1:
                    _wait = 2 ** (_attempt + 1)
                    logger.warning("safe_edit: network error, retrying in %ss: %s", _wait, e)
                    await asyncio.sleep(_wait)
                    continue
                raise

        # All retries exhausted
        logger.debug("safe_edit: all %d retries exhausted", _max_retries)
        return None

    async def _require_callback(self, update) -> bool:
        """Ensure the update contains a callback_query. Return True if present."""
        if getattr(update, "callback_query", None) is None:
            logger.warning("Handler invoked without callback_query")
            return False
        return True

    async def _log_bad_callback(
        self,
        reason: str,
        data,
        user_id: int | None = None,
        chat_id: int | None = None,
        message_id: int | None = None,
    ):
        """Log malformed or unexpected callback events for later inspection.

        Appends a JSON line to `logs/bad_callbacks.log` and increments an in-memory counter.
        """
        try:
            # Increment in-memory counter
            self.bad_callback_counts[reason] = self.bad_callback_counts.get(reason, 0) + 1

            # Ensure logs dir exists
            log_dir = os.path.join(os.path.dirname(__file__), "logs")
            os.makedirs(log_dir, exist_ok=True)
            log_path = os.path.join(log_dir, "bad_callbacks.log")

            entry = {
                "timestamp": utc_iso(),
                "reason": reason,
                "data": repr(data),
                "user_id": user_id,
                "chat_id": chat_id,
                "message_id": message_id,
            }

            with open(log_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            logger.exception("Failed to log bad callback event")

    async def _check_conversion_quota(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Enforce per-user conversion rate limits if configured.

        Returns True if the user may proceed, False if they are rate-limited
        (and an informational message has been sent).
        """
        try:
            user_id = update.effective_user.id
        except Exception:
            user_id = None

        try:
            conversion_limiter = None
            if context and getattr(context, "application", None):
                conversion_limiter = context.application.bot_data.get("conversion_rate_limiter")
            if conversion_limiter and user_id is not None:
                allowed, message = await conversion_limiter.can_convert(str(user_id))
                if not allowed:
                    try:
                        if getattr(update, "callback_query", None):
                            await self.safe_edit(update.callback_query, message)
                        elif getattr(update, "message", None):
                            await update.message.reply_text(message)
                    except Exception:
                        logger.debug("Failed to notify user about conversion rate limit")
                return allowed
        except Exception:
            # On any error, allow the conversion to proceed (fail-open)
            logger.debug("Conversion quota check failed, allowing conversion")
        return True

    async def _discard_relay_copy(self, context, chat_id, message_id, reason: str = "") -> bool:
        """Delete a relay-group copy we forwarded but are not going to use.

        The relay group exists so the userbot can reach a file the bot itself
        cannot fetch. Once the reason for forwarding is gone - the batch was
        stopped, or the download that the copy was for failed - the copy is dead
        weight in a shared chat, and for a long video a lot of it. Best-effort:
        a failure here is logged and never fails the caller.
        """
        try:
            chat_id = int(chat_id)
            message_id = int(message_id)
        except (TypeError, ValueError):
            return False
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.info(
                "Relay: removed the copy at %s/%s%s",
                chat_id,
                message_id,
                f" ({reason})" if reason else "",
            )
            return True
        except Exception:
            logger.debug("Relay: could not remove the copy at %s/%s", chat_id, message_id)
            return False

    async def _cleanup_dedup_key(self, dedup_key: str | None) -> None:
        """Delete a pipeline dedup key from Redis (best-effort, fire-and-forget).

        Used to clean up the "pending" placeholder when ingest fails so future
        retries aren't blocked.
        """
        if not dedup_key:
            return
        try:
            from utils.job_queue import get_redis as _get_dedup_redis

            _r = await _get_dedup_redis()
            try:
                await _r.delete(dedup_key)
            finally:
                with contextlib.suppress(Exception):
                    await _r.close()
        except Exception:
            pass

    async def _cancel_stale_pipeline_job(self, session: dict, handler_name: str, user_id: int | None = None) -> bool:
        """Cancel any stale pipeline job that may have been started, so the user's specific settings take effect.

        Also clears the pipeline dedup Redis key so the same file can be re-processed.

        Args:
            session: The user session dict.
            handler_name: Name of the calling handler (for logging).
            user_id: Optional user ID. If provided, also deletes the pipeline dedup
                     Redis key to prevent stale dedup blocking.

        Returns True if a job was cancelled, False otherwise.
        """
        current_file = session.get("current_file")
        if not current_file:
            return False
        _existing_id = current_file.get("_pipeline_job_id")
        if not _existing_id:
            return False

        try:
            from utils.job_queue import get_redis as _cancel_r

            _r_cancel = await _cancel_r()
            try:
                # Mark job as cancelled (both the cancel flag and the status)
                await _r_cancel.hset(
                    f"ffmpeg:job:{_existing_id}",
                    mapping={"cancel": "1", "status": "cancelled"},
                )
                # Also clear the pipeline dedup key so future requests for the same
                # file aren't blocked by a stale dedup reference.
                _file_uid = current_file.get("file_unique_id")
                if user_id and _file_uid:
                    _dedup_key = f"ffmpeg:pipeline_dedup:{user_id}:{_file_uid}"
                    with contextlib.suppress(Exception):
                        await _r_cancel.delete(_dedup_key)
                logger.info(
                    "%s: cancelled stale pipeline job %s to apply specific settings",
                    handler_name,
                    _existing_id,
                )
            finally:
                with contextlib.suppress(Exception):
                    await _r_cancel.close()
        except Exception:
            logger.debug("%s: failed to cancel stale pipeline job %s", handler_name, _existing_id)

        current_file.pop("_pipeline_job_id", None)
        session["current_file"] = current_file
        return True

    # ── Media file_id caching helpers ─────────────────────────────────────
    async def _get_cached_file_id(
        self,
        media_type: str,
        *,
        file_unique_id: str | None = None,
        file_path: str | None = None,
    ) -> str | None:
        """Get a cached Telegram file_id for the given media, if available.

        Uses the file_id_cache module to retrieve a previously stored file_id.
        Falls back gracefully when the cache is unavailable.
        """
        try:
            from utils.file_id_cache import get_file_id

            return await get_file_id(
                media_type,
                file_unique_id=file_unique_id,
                file_path=file_path,
            )
        except Exception:
            logger.debug("handlers: file_id cache lookup failed, will upload fresh")
            return None

    async def _cache_file_id(
        self,
        media_type: str,
        file_id: str,
        *,
        file_unique_id: str | None = None,
        file_path: str | None = None,
    ) -> bool:
        """Cache a Telegram file_id for future reuse.

        Uses the file_id_cache module to store the file_id.
        Falls back gracefully when the cache is unavailable.
        """
        try:
            from utils.file_id_cache import store_file_id

            return await store_file_id(
                media_type,
                file_id,
                file_unique_id=file_unique_id,
                file_path=file_path,
            )
        except Exception:
            logger.debug("handlers: failed to cache file_id for %s", media_type)
            return False

    @staticmethod
    def _is_stale_file_id(error) -> bool:
        """Whether *error* means Telegram refused the file_id itself.

        Tells the two reactions apart: drop the token and upload fresh (it is
        dead), or leave the cache alone and let the failure surface (a flood wait
        or a network error was never the token's fault).
        """
        try:
            from utils.file_id_cache import is_stale_file_id

            return bool(is_stale_file_id(error))
        except Exception:
            return False

    async def _forget_cached_file_id(
        self,
        media_type: str,
        *,
        file_unique_id: str | None = None,
        file_path: str | None = None,
    ) -> None:
        """Drop a file_id Telegram refused, so the next send uploads fresh.

        A cached file_id is only a saving while a dead one costs a retry: without
        this, a token Telegram has revoked makes every delivery of that media
        fail, and the more durable the cache the more chances there are to hit
        one.
        """
        try:
            from utils.file_id_cache import invalidate_file_id

            await invalidate_file_id(
                media_type,
                file_unique_id=file_unique_id,
                file_path=file_path,
            )
            logger.info(
                "handlers: dropped the refused %s file_id; the next send uploads fresh",
                media_type,
            )
        except Exception:
            logger.debug("handlers: could not invalidate a refused %s file_id", media_type)

    # ── Video delivery helper: send_video with rich metadata ──────────────
    async def _send_video_result(
        self,
        bot,
        chat_id: int,
        file_path: str,
        caption: str = "",
        thumb_path: str | None = None,
        file_unique_id: str | None = None,
    ) -> str | None:
        """Send a video file with probed metadata (duration, width, height, thumbnail).

        Uses the shared ``probe_video_for_delivery`` utility to extract video
        metadata and generate a thumbnail, then calls send_video with all
        available info so Telegram shows the video's duration, dimensions,
        and thumbnail.

        Returns the Telegram file_id of the sent video, or None on failure.
        The file_id is cached for reuse on subsequent sends of the same media
        to avoid repeated egress from IDrive/object storage.

        Args:
            file_unique_id: Optional stable identifier for the video content.
                If provided, the resulting file_id will be cached for reuse.
        """
        _vid_duration = None
        _vid_width = None
        _vid_height = None
        _thumb_path = thumb_path
        _cleanup_thumb = False

        # ── Use shared probe+thumbnail utility ──
        try:
            from utils.ffmpeg_runner import probe_video_for_delivery

            _meta, _auto_thumb = await probe_video_for_delivery(file_path)
            if _meta:
                _vid_duration = _meta.get("duration")
                _vid_width = _meta.get("width")
                _vid_height = _meta.get("height")
            if not _thumb_path and _auto_thumb and os.path.exists(_auto_thumb):
                _thumb_path = _auto_thumb
                _cleanup_thumb = True
            elif _auto_thumb and os.path.exists(_auto_thumb):
                with contextlib.suppress(Exception):
                    os.remove(_auto_thumb)
        except Exception:
            logger.debug("handlers: probe_video_for_delivery failed")

        # ── Try to use cached file_id first ──
        _sent_file_id = None
        if file_unique_id:
            _cached_file_id = await self._get_cached_file_id(
                "video",
                file_unique_id=file_unique_id,
            )
            if _cached_file_id:
                logger.info(
                    "handlers: using cached file_id for video (chat_id=%s), avoiding re-upload",
                    chat_id,
                )
                _send_kwargs = {
                    "chat_id": chat_id,
                    "video": _cached_file_id,
                    "caption": caption,
                    "supports_streaming": True,
                }
                if _vid_duration is not None:
                    _send_kwargs["duration"] = _vid_duration
                if _vid_width is not None:
                    _send_kwargs["width"] = _vid_width
                if _vid_height is not None:
                    _send_kwargs["height"] = _vid_height
                try:
                    if _thumb_path:
                        try:
                            with open(_thumb_path, "rb") as _tf:
                                _send_kwargs["thumb"] = _tf
                                _msg = await bot.send_video(**_send_kwargs)
                        except Exception:
                            # The thumb may be what Telegram objected to, not the
                            # token: retry without it and keep the retry's message
                            # (this used to drop the result, leaving _msg unbound).
                            _send_kwargs.pop("thumb", None)
                            _msg = await bot.send_video(**_send_kwargs)
                    else:
                        _msg = await bot.send_video(**_send_kwargs)
                except Exception as _cached_exc:
                    if not self._is_stale_file_id(_cached_exc):
                        raise
                    # The token is dead: drop it and fall through to a real
                    # upload, so caching can never cost a delivery.
                    await self._forget_cached_file_id("video", file_unique_id=file_unique_id)
                else:
                    _sent_file_id = getattr(_msg, "video", None)
                    if _sent_file_id and hasattr(_sent_file_id, "file_id"):
                        _sent_file_id = _sent_file_id.file_id
                    return _sent_file_id

        # ── Send with all available metadata (fresh upload) ──
        try:
            with open(file_path, "rb") as _fh:
                _send_kwargs = {
                    "chat_id": chat_id,
                    "video": _fh,
                    "caption": caption,
                    "supports_streaming": True,
                }
                if _vid_duration is not None:
                    _send_kwargs["duration"] = _vid_duration
                if _vid_width is not None:
                    _send_kwargs["width"] = _vid_width
                if _vid_height is not None:
                    _send_kwargs["height"] = _vid_height
                if _thumb_path:
                    try:
                        with open(_thumb_path, "rb") as _tf:
                            _send_kwargs["thumb"] = _tf
                            _msg = await bot.send_video(**_send_kwargs)
                    except Exception:
                        _msg = await bot.send_video(**_send_kwargs)
                else:
                    _msg = await bot.send_video(**_send_kwargs)
                _sent_file_id = getattr(_msg, "video", None)
                if _sent_file_id and hasattr(_sent_file_id, "file_id"):
                    _sent_file_id = _sent_file_id.file_id

                # Cache the file_id for future reuse
                if _sent_file_id and file_unique_id:
                    await self._cache_file_id(
                        "video",
                        _sent_file_id,
                        file_unique_id=file_unique_id,
                    )

                return _sent_file_id
        finally:
            if _cleanup_thumb and _thumb_path and os.path.exists(_thumb_path):
                with contextlib.suppress(Exception):
                    os.remove(_thumb_path)

    # ── Photo delivery helper with file_id caching ───────────────────────
    async def _send_photo_result(
        self,
        bot,
        chat_id: int,
        file_path: str,
        caption: str = "",
        file_unique_id: str | None = None,
    ) -> str | None:
        """Send a photo file with file_id caching.

        Returns the Telegram file_id of the sent photo, or None on failure.
        The file_id is cached for reuse on subsequent sends of the same photo
        to avoid repeated egress from IDrive/object storage.
        """
        # ── Try to use cached file_id first ──
        if file_unique_id:
            _cached_file_id = await self._get_cached_file_id(
                "photo",
                file_unique_id=file_unique_id,
            )
            if _cached_file_id:
                logger.info(
                    "handlers: using cached file_id for photo (chat_id=%s), avoiding re-upload",
                    chat_id,
                )
                try:
                    _msg = await bot.send_photo(
                        chat_id=chat_id,
                        photo=_cached_file_id,
                        caption=caption,
                    )
                except Exception as _cached_exc:
                    if not self._is_stale_file_id(_cached_exc):
                        raise
                    await self._forget_cached_file_id("photo", file_unique_id=file_unique_id)
                else:
                    _sent_file_id = getattr(_msg, "photo", None)
                    if _sent_file_id and hasattr(_sent_file_id, "file_id"):
                        _sent_file_id = _sent_file_id[-1].file_id  # Last photo is highest res
                    return _sent_file_id

        # ── Fresh upload ──
        try:
            with open(file_path, "rb") as _fh:
                _msg = await bot.send_photo(
                    chat_id=chat_id,
                    photo=_fh,
                    caption=caption,
                )
            _sent_file_id = getattr(_msg, "photo", None)
            if _sent_file_id and hasattr(_sent_file_id, "file_id"):
                _sent_file_id = _sent_file_id[-1].file_id  # Last photo is highest res

            # Cache the file_id for future reuse
            if _sent_file_id and file_unique_id:
                await self._cache_file_id(
                    "photo",
                    _sent_file_id,
                    file_unique_id=file_unique_id,
                )

            return _sent_file_id
        except Exception:
            logger.exception("handlers: failed to send photo")
            return None

    # ── Audio delivery helper with file_id caching ───────────────────────
    async def _send_audio_result(
        self,
        bot,
        chat_id: int,
        file_path: str,
        caption: str = "",
        title: str | None = None,
        performer: str | None = None,
        filename: str | None = None,
        file_unique_id: str | None = None,
    ) -> str | None:
        """Send an audio file with file_id caching.

        Returns the Telegram file_id of the sent audio, or None on failure.
        The file_id is cached for reuse on subsequent sends of the same audio
        to avoid repeated egress from IDrive/object storage.
        """
        # ── Try to use cached file_id first ──
        if file_unique_id:
            _cached_file_id = await self._get_cached_file_id(
                "audio",
                file_unique_id=file_unique_id,
            )
            if _cached_file_id:
                logger.info(
                    "handlers: using cached file_id for audio (chat_id=%s), avoiding re-upload",
                    chat_id,
                )
                _send_kwargs = {
                    "chat_id": chat_id,
                    "audio": _cached_file_id,
                    "caption": caption,
                }
                if title:
                    _send_kwargs["title"] = title
                if performer:
                    _send_kwargs["performer"] = performer
                if filename:
                    _send_kwargs["filename"] = filename
                try:
                    _msg = await bot.send_audio(**_send_kwargs)
                except Exception as _cached_exc:
                    if not self._is_stale_file_id(_cached_exc):
                        raise
                    await self._forget_cached_file_id("audio", file_unique_id=file_unique_id)
                else:
                    _sent_file_id = getattr(_msg, "audio", None)
                    if _sent_file_id and hasattr(_sent_file_id, "file_id"):
                        _sent_file_id = _sent_file_id.file_id
                    return _sent_file_id

        # ── Fresh upload ──
        try:
            with open(file_path, "rb") as _fh:
                _send_kwargs = {
                    "chat_id": chat_id,
                    "audio": _fh,
                    "caption": caption,
                }
                if title:
                    _send_kwargs["title"] = title
                if performer:
                    _send_kwargs["performer"] = performer
                if filename:
                    _send_kwargs["filename"] = filename
                _msg = await bot.send_audio(**_send_kwargs)
            _sent_file_id = getattr(_msg, "audio", None)
            if _sent_file_id and hasattr(_sent_file_id, "file_id"):
                _sent_file_id = _sent_file_id.file_id

            # Cache the file_id for future reuse
            if _sent_file_id and file_unique_id:
                await self._cache_file_id(
                    "audio",
                    _sent_file_id,
                    file_unique_id=file_unique_id,
                )

            return _sent_file_id
        except Exception:
            logger.exception("handlers: failed to send audio")
            return None

    # ── Document delivery helper with file_id caching ────────────────────
    async def _send_document_result(
        self,
        bot,
        chat_id: int,
        file_path: str,
        caption: str = "",
        filename: str | None = None,
        file_unique_id: str | None = None,
    ) -> str | None:
        """Send a document file with file_id caching.

        Returns the Telegram file_id of the sent document, or None on failure.
        The file_id is cached for reuse on subsequent sends of the same document
        to avoid repeated egress from IDrive/object storage.
        """
        # ── Try to use cached file_id first ──
        if file_unique_id:
            _cached_file_id = await self._get_cached_file_id(
                "document",
                file_unique_id=file_unique_id,
            )
            if _cached_file_id:
                logger.info(
                    "handlers: using cached file_id for document (chat_id=%s), avoiding re-upload",
                    chat_id,
                )
                _send_kwargs = {
                    "chat_id": chat_id,
                    "document": _cached_file_id,
                    "caption": caption,
                }
                if filename:
                    _send_kwargs["filename"] = filename
                try:
                    _msg = await bot.send_document(**_send_kwargs)
                except Exception as _cached_exc:
                    if not self._is_stale_file_id(_cached_exc):
                        raise
                    await self._forget_cached_file_id("document", file_unique_id=file_unique_id)
                else:
                    _sent_file_id = getattr(_msg, "document", None)
                    if _sent_file_id and hasattr(_sent_file_id, "file_id"):
                        _sent_file_id = _sent_file_id.file_id
                    return _sent_file_id

        # ── Fresh upload ──
        try:
            with open(file_path, "rb") as _fh:
                _send_kwargs = {
                    "chat_id": chat_id,
                    "document": _fh,
                    "caption": caption,
                }
                if filename:
                    _send_kwargs["filename"] = filename
                _msg = await bot.send_document(**_send_kwargs)
            _sent_file_id = getattr(_msg, "document", None)
            if _sent_file_id and hasattr(_sent_file_id, "file_id"):
                _sent_file_id = _sent_file_id.file_id

            # Cache the file_id for future reuse
            if _sent_file_id and file_unique_id:
                await self._cache_file_id(
                    "document",
                    _sent_file_id,
                    file_unique_id=file_unique_id,
                )

            return _sent_file_id
        except Exception:
            logger.exception("handlers: failed to send document")
            return None

    async def _ensure_bulk_file_downloaded(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: dict,
        file_info: dict,
    ):
        """Make one bulk-list entry available locally and return its usable path.

        ``_ensure_current_file_downloaded`` only knows about ``session["current_file"]``,
        so point the session at each entry in turn and restore the previous value
        afterwards. Files already streamed to storage return their input key.
        """
        path = (file_info or {}).get("path")
        if path and os.path.exists(path):
            return path
        if (file_info or {}).get("input_key"):
            return file_info.get("input_key")

        had_current = "current_file" in session
        previous = session.get("current_file")
        pipeline_job_before = file_info.get("_pipeline_job_id")
        session["current_file"] = file_info
        try:
            await self._ensure_current_file_downloaded(update, context, session)
            pipeline_job_id = file_info.get("_pipeline_job_id")
            if pipeline_job_id and pipeline_job_id != pipeline_job_before:
                # Queued, not waited on here. The wait is not a fetch, so it must
                # not run under the caller's fetch timeout: it used to, and that
                # bound (45 min) cut a live conversion off mid-encode and reported
                # a file the worker was still converting as "could not fetch".
                # :meth:`_await_bulk_pipeline_job` owns the wait instead.
                file_info["_bulk_pipeline_job_pending"] = pipeline_job_id
        finally:
            if had_current:
                session["current_file"] = previous
            else:
                session.pop("current_file", None)
        return file_info.get("path")

    async def _watch_batch_member(
        self, query, *, batch_id, job_id, index: int = 0, total: int = 0, name: str = ""
    ) -> None:
        """Show one member's stage on the apply's message, start to delivery.

        The apply owns exactly one message per batch, so this renders onto that
        message instead of posting a second one: the fetch, the queueing, the live
        encode percentage and the delivery all appear in place, with the batch id
        and the Stop button kept. Returns when the job reaches a terminal state - a
        member that finished, failed or was stopped - and never touches anything
        else, so the apply is free to rewrite the message once the wait is over.
        """
        if query is None or not job_id:
            return
        try:
            from utils.job_queue import get_redis as _watch_redis

            r = await _watch_redis()
        except Exception:
            return

        from utils.rate_limiter import telegram_edit_coalescer, telegram_flood_gate

        _chat_id, _message_id = _edit_target_ids(query)
        _flood_scope = telegram_flood_gate.scope_for_chat(_chat_id)

        last_text = None
        _terminal_pending_since = None  # set once a terminal status is being retried
        try:
            while True:
                info: dict = {}
                with contextlib.suppress(Exception):
                    data = await r.hgetall(f"ffmpeg:job:{job_id}")
                    info = {
                        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
                        for k, v in (data or {}).items()
                    }
                text = _batch_member_text(batch_id, index, total, name, info)
                _terminal = str(info.get("status") or "").lower() in _TERMINAL_JOB_STATUSES
                # The job watcher and the big-file pipeline render onto this same
                # message, so go through the shared coalescer and the flood gate
                # rather than editing on our own schedule.
                _sent = False
                if text != last_text:
                    if telegram_edit_coalescer.should_skip(
                        _chat_id, _message_id, text, _BATCH_MEMBER_POLL_SECONDS, force=_terminal
                    ):
                        # Nothing to send only when that text is already on screen.
                        _sent = telegram_edit_coalescer.shows(_chat_id, _message_id, text)
                    elif not await telegram_flood_gate.should_drop_inline(_flood_scope):
                        with contextlib.suppress(Exception):
                            _sent = (
                                await self.safe_edit(query, text, reply_markup=_batch_stop_markup(batch_id))
                            ) is not None
                        if _sent:
                            telegram_edit_coalescer.record(_chat_id, _message_id, text)
                    if _sent:
                        # Only claim a text the message is actually showing, so a
                        # status the gate swallowed is retried next poll.
                        last_text = text
                if _terminal:
                    if last_text == text:
                        return
                    # A finished file whose final line could not be written keeps
                    # this watcher alive until the flood window closes; the apply
                    # rewrites the message anyway, so the wait is bounded.
                    if _terminal_pending_since is None:
                        _terminal_pending_since = time.time()
                    elif (time.time() - _terminal_pending_since) > _FLOOD_TERMINAL_MAX_WAIT_SECONDS:
                        logger.warning(
                            "Batch %s: file %s finished but its stage could not be written "
                            "(Telegram flood control on this chat)",
                            batch_id,
                            job_id,
                        )
                        return
                await asyncio.sleep(_BATCH_MEMBER_POLL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("bulk apply: member stage watcher stopped for %s", job_id)
        finally:
            with contextlib.suppress(Exception):
                aclose = getattr(r, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await r.close()

    async def _await_member_job(
        self, query, job_id: str, *, batch_id=None, index: int = 0, total: int = 0, name: str = ""
    ) -> str | None:
        """Wait for one member's job while the apply's message shows its stage.

        Returns the terminal status, or ``None`` when the wait gave up - the same
        contract as :meth:`_await_job_finished`, which this wraps. Every member the
        apply waits on goes through here (a pipeline job and a job the apply
        queued itself alike), so the batch's one message tracks all of them the
        same way instead of freezing on the last text the apply wrote.
        """
        member_task = None
        if query is not None and batch_id:
            try:
                member_task = asyncio.create_task(
                    self._watch_batch_member(
                        query, batch_id=batch_id, job_id=job_id, index=index, total=total, name=name
                    )
                )
            except Exception:
                logger.debug("bulk apply: could not start the stage watcher for %s", job_id)
        try:
            return await self._await_job_finished(job_id)
        finally:
            # The watcher stops itself on a terminal state; this only bounds how
            # long the apply waits for its last line before moving on.
            if member_task is not None:
                try:
                    await asyncio.wait_for(member_task, timeout=_BATCH_MEMBER_POLL_SECONDS + 5)
                except (TimeoutError, asyncio.CancelledError):
                    member_task.cancel()
                    with contextlib.suppress(Exception):
                        await member_task

    async def _await_bulk_pipeline_job(self, file_info: dict, *, query=None, index: int = 0, total: int = 0) -> None:
        """Wait out a pipeline job a bulk fetch just queued - and watch it live.

        The fetch that queued this job is bounded by
        :data:`_BULK_FETCH_TIMEOUT_SECONDS`; the job it queued is not a fetch. The
        wait used to live inside that bound, so a conversion that legitimately ran
        longer than 45 minutes was abandoned mid-encode - the apply reported the
        file as unfetchable and moved on while the worker finished it and delivered
        anyway. Here the wait is bounded only by :data:`_BULK_JOB_WAIT_SECONDS`,
        the same budget the jobs the apply enqueues itself get.

        While it waits, :meth:`_watch_batch_member` keeps the apply's own message
        showing which stage the file is in. Nothing watched these jobs before, so a
        batch file went from "queued" to delivered with nothing in between.
        """
        job_id = (file_info or {}).get("_bulk_pipeline_job_pending")
        if not job_id:
            return
        file_info.pop("_bulk_pipeline_job_pending", None)

        _pipeline_status = await self._await_member_job(
            query,
            job_id,
            batch_id=file_info.get("_pipeline_batch_id"),
            index=index,
            total=total,
            name=file_info.get("name") or "",
        )
        if _pipeline_status == "done":
            file_info["_bulk_pipeline_completed"] = True
        elif _pipeline_status == "cancelled":
            # Stopped, not finished. Reporting it as completed would count a
            # cancelled file as done and keep the batch running.
            file_info["_batch_cancelled"] = True
        elif _pipeline_status is None:
            # The job still exists and will deliver, so the file stays marked
            # completed - enqueuing a second job for it would be worse than
            # waiting on the one already queued.
            logger.warning(
                "bulk apply: gave up waiting for pipeline job %s; its result arrives on its own",
                job_id,
            )
            file_info["_bulk_pipeline_completed"] = True
        else:
            # Errored for real: say so, and never enqueue a second job for a file
            # the pipeline already tried and failed to convert.
            file_info["_pipeline_failed"] = True

    async def _ensure_current_file_downloaded(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Ensure the session's current_file is downloaded locally. Raises Exception on failure."""
        user_id = update.effective_user.id if update and update.effective_user else None
        current_file = session.get("current_file") if session else None
        if not current_file:
            raise Exception("No file in session")

        # If already downloaded (local) or already streamed to S3, nothing to do
        path = current_file.get("path")
        input_key = current_file.get("input_key")
        # ── Stale-key guard for pipeline-restored sessions: verify the S3 key
        #    actually exists before reusing it.  Only check when the key came from
        #    an earlier pipeline run (has _pipeline_job_id).  Freshly-streamed keys
        #    (set seconds ago in the S3 stream path) skip this check to avoid an
        #    unnecessary S3 head-object call on every conversion. ──
        if input_key and current_file.get("_pipeline_job_id"):
            try:
                from utils.storage import get_storage_backend as _gsb

                _bn = config.get_storage_backend_name()
                if _bn in ("s3", "r2") and _gsb is not None:
                    _backend_check = await _gsb()
                    _exists = await _backend_check.exists(input_key)
                    if not _exists:
                        # Key is stale — clear it and proceed with fresh download
                        logger.warning(
                            "Stale input_key=%s for user=%s does not exist in S3; clearing and re-downloading",
                            input_key,
                            user_id,
                        )
                        current_file.pop("input_key", None)
                        session["current_file"] = current_file
                        input_key = None
            except Exception:
                # If the check fails, conservatively assume the key is valid
                pass
        if input_key or (path and os.path.exists(path)):
            return

        file_id = current_file.get("id") or current_file.get("file_id")
        if not file_id:
            raise Exception("No file identifier available to download")

        # Check size against MAX_FILE_SIZE if present
        try:
            max_size = int(MAX_FILE_SIZE)
        except Exception:
            max_size = 4 * 1024**3
        size = current_file.get("size")
        if size and size > max_size:
            raise Exception(f"File too large ({size // 1024 // 1024}MB). Max allowed: {max_size // 1024 // 1024}MB")

        # Preserve the original source identifiers for userbot fallback and the
        # big-file pipeline so we can reuse the real chat/message pair when the
        # bot API download fails.
        #
        # NOTE: current_file["forward"] may be explicitly None (stored as such by
        # handle_document/handle_video when the message is not a forward), so we
        # MUST use ``current_file.get("forward") or {}`` instead of the seemingly
        # equivalent ``current_file.get("forward", {})`` — the latter returns None
        # when the key exists but its value is None, which would cause a
        # ``'NoneType' object has no attribute 'get'`` crash on the next .get() call.
        _forward = current_file.get("forward") or {}
        current_file.setdefault("chat_id", _forward.get("chat_id"))
        current_file.setdefault("msg_id", current_file.get("msg_id") or current_file.get("message_id"))
        current_file.setdefault("message_id", current_file.get("msg_id") or current_file.get("message_id"))
        current_file.setdefault("forward_chat_id", _forward.get("chat_id"))
        current_file.setdefault("forward_message_id", _forward.get("message_id"))

        # Prepare extension and paths early so fallback download (userbot) can use them
        ext = ""
        t = current_file.get("type")
        name = current_file.get("name") or ""
        if t == "video":
            ext = ".mp4"
        elif t == "audio":
            ext = os.path.splitext(name)[1] or ".mp3"
        else:
            ext = os.path.splitext(name)[1] or ""

        input_dir = getattr(
            config, "INPUT_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage", "input")
        )
        with contextlib.suppress(OSError):
            os.makedirs(input_dir, exist_ok=True)
        file_path = os.path.join(input_dir, f"{user_id}_{file_id}{ext}")

        # Track whether the BigFilePipeline already forwarded this file to the relay
        # group, so the error handler below can avoid a duplicate forward.
        _relay_forwarded_already = False

        # ── Media cache short-circuit: a media that already entered the pipe
        #    (same file_unique_id AND same byte size) is reused instead of being
        #    fetched from Telegram again. Three tiers, cheapest first:
        #      1. a stored remote key  (already uploaded to S3/R2)
        #      2. a local file path    (already downloaded to disk)
        #      3. the raw bytes        (small media kept verbatim in Redis)
        #    A size mismatch makes ``lookup`` return None, so a different file
        #    that merely shared an id is never reused.
        try:
            from utils import media_cache as _media_cache

            _uid = current_file.get("file_unique_id")
            if _uid and _media_cache.cache_enabled():
                _expected = current_file.get("size")
                _entry = await _media_cache.lookup(_uid, expected_size=_expected)

                # 1) Remote copy already in object storage — reuse the key.
                _stored_key = (_entry or {}).get("input_key")
                if _stored_key and config.get_storage_backend_name() in ("s3", "r2"):
                    _key_ok = True
                    try:
                        from utils.storage import get_storage_backend as _gsb_check

                        _check_backend = await _gsb_check()
                        if _check_backend is not None:
                            _key_ok = await _check_backend.exists(_stored_key)
                    except Exception:
                        # Conservatively treat an unchecked key as valid, matching
                        # the stale-key guard elsewhere in this function.
                        _key_ok = True
                    if _key_ok:
                        current_file["input_key"] = _stored_key
                        current_file["path"] = None
                        session["current_file"] = current_file
                        with contextlib.suppress(Exception):
                            self._persist_session(user_id)
                        logger.info(
                            "media cache: reused stored input_key for user %s (file_unique_id=%s)",
                            user_id,
                            _uid,
                        )
                        return

                # 2) Local copy still on disk — reuse the file.
                _stored_path = (_entry or {}).get("path")
                if _stored_path and os.path.exists(_stored_path):
                    current_file["path"] = _stored_path
                    session["current_file"] = current_file
                    with contextlib.suppress(Exception):
                        self._persist_session(user_id)
                    logger.info(
                        "media cache: reused local file for user %s (file_unique_id=%s)",
                        user_id,
                        _uid,
                    )
                    return

                # 3) Small media held verbatim in Redis.
                _cached = await _media_cache.get_bytes(_uid, expected_size=_expected)
                if _cached:
                    with open(file_path, "wb") as _fh:
                        _fh.write(_cached)
                    current_file["path"] = file_path
                    session["current_file"] = current_file
                    with contextlib.suppress(Exception):
                        self._persist_session(user_id)
                    logger.info(
                        "media cache: reused %d bytes for user %s (file_unique_id=%s)",
                        len(_cached),
                        user_id,
                        _uid,
                    )
                    return
        except Exception:
            logger.debug("handlers: media cache lookup failed; continuing to download")

        # Attempt to fetch file via Telegram API (bot). If Telegram refuses due to
        # file size or access rules, prefer a user-account (userbot) fallback when
        # configured via env (`ENABLE_USERBOT` + API_ID/API_HASH).
        try:
            # -- Big files pipeline: route files > BOT_API_MAX_MB through Pyrogram->S3->Worker
            bot_api_max_mb = config.BOT_API_MAX_MB
            file_size = current_file.get("size") or 0
            if file_size and file_size > bot_api_max_mb * 1024 * 1024 and _bigfile_pipeline is not None:
                _bot_chat, _bot_msg = _extract_large_file_source(current_file)
                if _bot_chat and _bot_msg:
                    # A batch that is already stopped must not reach the relay
                    # forward further down. Order matters: that forward happens
                    # *before* the pipeline ingests, so without this check pressing
                    # Stop mid-file leaves a copy of that file in the relay group
                    # and the ingest is then refused as "batch cancelled" - i.e. a
                    # forward with nothing processed.
                    _pre_batch_id = current_file.get("_pipeline_batch_id")
                    if _pre_batch_id:
                        with contextlib.suppress(Exception):
                            from utils.batch_pipeline import is_batch_cancelled as _is_cancelled_now

                            if await _is_cancelled_now(batch_id=_pre_batch_id):
                                current_file["_batch_cancelled"] = True
                                session["current_file"] = current_file
                                logger.info(
                                    "Batch %s is stopped: skipping fetch of %s",
                                    _pre_batch_id,
                                    current_file.get("name"),
                                )
                                return

                    # ── Redis-based pipeline dedup: if this file_unique_id was already
                    #    pipelined (e.g. from a previous callback where the session didn't
                    #    persist _pipeline_job_id), skip the pipeline entirely. This prevents
                    #    double Pyrogram downloads even when the session is reloaded. ──
                    _dedup_key = None
                    _file_uid = current_file.get("file_unique_id")
                    if user_id and _file_uid:
                        _dedup_key = f"ffmpeg:pipeline_dedup:{user_id}:{_file_uid}"
                        # ── Atomic dedup: use SET NX to claim the dedup key.
                        #    If the key already exists AND the job is active, skip.
                        #    If the key exists but the job is stale, overwrite it.
                        #    If the key doesn't exist, claim it atomically. ──
                        try:
                            from utils.job_queue import get_redis

                            _r_dedup = await get_redis()
                            try:
                                _already = await _r_dedup.get(_dedup_key)
                                if _already:
                                    _stored_job_id = _already.decode() if isinstance(_already, bytes) else _already
                                    # ── Active-job guard: skip if the old job is still
                                    #    actively processing or if another request is
                                    #    currently claiming the key ("pending" placeholder). ──
                                    _active = False
                                    if _stored_job_id == "pending":
                                        # Another concurrent request is mid-ingest — skip
                                        _active = True
                                    else:
                                        try:
                                            _old_hash = await _r_dedup.hgetall(f"ffmpeg:job:{_stored_job_id}")
                                            if _old_hash:
                                                # If the job has cancel=1 set (even if status still
                                                # shows "queued"/"waiting"), treat it as inactive so
                                                # the user can reprocess the same file immediately.
                                                _cancel_val = _old_hash.get(b"cancel") or _old_hash.get("cancel")
                                                _is_cancelled = False
                                                if _cancel_val:
                                                    _cv = (
                                                        _cancel_val.decode()
                                                        if isinstance(_cancel_val, bytes)
                                                        else str(_cancel_val)
                                                    )
                                                    _is_cancelled = _cv == "1"
                                                if not _is_cancelled:
                                                    _status = _old_hash.get(b"status") or _old_hash.get("status")
                                                    if _status:
                                                        _s = (
                                                            _status.decode()
                                                            if isinstance(_status, bytes)
                                                            else str(_status)
                                                        )
                                                        _active = _s in (
                                                            "processing",
                                                            "queued",
                                                            "waiting",
                                                            "started",
                                                            "uploading",
                                                            "sending",
                                                        )
                                                # else: _active stays False (cancelled)
                                        except Exception:
                                            _active = False

                                    if _active:
                                        logger.info(
                                            "Pipeline dedup: file %s is still being processed "
                                            "by job %s; skipping duplicate for user %s",
                                            _file_uid,
                                            _stored_job_id,
                                            user_id,
                                        )
                                        if current_file is not None:
                                            current_file["_pipeline_job_id"] = _stored_job_id
                                            session["current_file"] = current_file
                                        return
                                    else:
                                        # Job is done, cancelled, errored, or hash doesn't exist
                                        # → clear dedup and allow re-processing
                                        logger.info(
                                            "Pipeline dedup: job %s for file %s is no longer "
                                            "active (stale/done); clearing dedup and reprocessing",
                                            _stored_job_id,
                                            _file_uid,
                                        )
                                        await _r_dedup.delete(_dedup_key)
                                        # Do NOT return — fall through to normal pipeline processing
                                # ── Try to claim the dedup key atomically (SET NX). ──
                                #    We'll set the value to a placeholder "pending" and
                                #    overwrite it with the real job_id after ingest succeeds.
                                #    If another concurrent call already claimed it, skip. ──
                                _claimed = await _r_dedup.set(_dedup_key, "pending", nx=True, ex=86400)
                                if not _claimed:
                                    # Another concurrent call claimed it — skip
                                    logger.info(
                                        "Pipeline dedup: concurrent claim for file %s by "
                                        "another request; skipping for user %s",
                                        _file_uid,
                                        user_id,
                                    )
                                    return
                            finally:
                                with contextlib.suppress(Exception):
                                    await _r_dedup.close()
                        except Exception:
                            pass
                    # ── Relay-forward for pipeline: userbot may not have access to the
                    #    original chat (direct bot-user chat). If RELAY_CHAT_ID is configured,
                    #    forward the message there first so the userbot can download it.
                    _pipeline_chat = _bot_chat
                    _pipeline_msg = _bot_msg
                    _relay_chat_id = config.RELAY_CHAT_ID
                    if _relay_chat_id:
                        try:
                            _rid = int(_relay_chat_id)
                            logger.info(
                                "Big files pipeline: forwarding %s/%s to relay %s for pipeline download",
                                _bot_chat,
                                _bot_msg,
                                _rid,
                            )
                            _forwarded = await context.bot.forward_message(
                                chat_id=_rid,
                                from_chat_id=_bot_chat,
                                message_id=_bot_msg,
                            )
                            if _forwarded and getattr(_forwarded, "message_id", None):
                                _pipeline_chat = _rid
                                _pipeline_msg = _forwarded.message_id
                                # Mark this file as already forwarded to prevent double relay
                                _relay_forwarded_already = True
                                logger.info(
                                    "Big files pipeline: relay forwarded to %s/%s",
                                    _rid,
                                    _pipeline_msg,
                                )
                            else:
                                logger.warning(
                                    "Big files pipeline: relay forward returned no message_id; using original source"
                                )
                        except Exception as _relay_exc:
                            logger.warning(
                                "Big files pipeline: relay forward failed (%s); using original source", _relay_exc
                            )

                    # ── Show progress before pipeline download (no Cancel button —
                    #    pipeline download is fast and job_id isn't known yet) ──
                    _pipeline_progress_msg = None
                    _pipeline_loop = None
                    try:
                        _dl_text = f"⬇️ Downloading via pipeline ({file_size // (1024 * 1024)} MB)..."
                        # The caller (a menu press) owns the message progress belongs
                        # on. `query` is read from the update rather than the closure,
                        # because this path is reached from several handlers.
                        _pipeline_batch_id = current_file.get("_pipeline_batch_id")
                        if _pipeline_batch_id:
                            # In a batch the apply's message is the only message
                            # about this file, so the pipeline edits that one - with
                            # the batch id and the Stop button kept, and the fetch
                            # line naming the file. (Editing it *without* those used
                            # to be how the batch id and the button vanished behind
                            # "Large file (N MB) queued for processing".)
                            _dl_text = _batch_member_text(
                                _pipeline_batch_id,
                                current_file.get("_pipeline_file_index"),
                                current_file.get("_pipeline_file_total"),
                                current_file.get("name"),
                                {"status": "downloading", "message": "downloading from storage", "progress": 0},
                            )
                        query = getattr(update, "callback_query", None)
                        _batch_message = getattr(query, "message", None) if query else None
                        if _batch_message is not None:
                            # ``reply_markup=None`` leaves the existing keyboard in
                            # place, so a batch's Stop button survives every edit.
                            _pipeline_progress_msg = _batch_message
                            await _pipeline_progress_msg.edit_text(
                                _dl_text, reply_markup=_batch_stop_markup(_pipeline_batch_id)
                            )
                        elif update and update.message:
                            _pipeline_progress_msg = await update.message.reply_text(_dl_text)
                        elif update and update.effective_user and context and context.bot:
                            _pipeline_progress_msg = await context.bot.send_message(
                                chat_id=update.effective_user.id,
                                text=_dl_text,
                            )
                        _pipeline_loop = asyncio.get_running_loop()
                    except Exception:
                        pass

                    _pl_last_pct = [-1]
                    _pl_last_time = [0.0]
                    _pipeline_cancelled = [False]
                    _pipeline_cancel_task = None

                    async def _watch_pipeline_cancel():
                        batch_id = current_file.get("_pipeline_batch_id")
                        if not batch_id:
                            return
                        try:
                            from utils.job_queue import get_redis

                            while True:
                                redis = await get_redis()
                                try:
                                    from utils.batch_pipeline import is_batch_cancelled

                                    if await is_batch_cancelled(redis, batch_id):
                                        _pipeline_cancelled[0] = True
                                        return
                                finally:
                                    with contextlib.suppress(Exception):
                                        await redis.close()
                                await asyncio.sleep(0.5)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            logger.debug("bulk: pipeline cancellation watcher stopped")

                    def _pipeline_progress_cb(sent: int, total: int):
                        """Sync callback for pipeline download progress."""
                        if _pipeline_cancelled[0]:
                            raise asyncio.CancelledError("batch cancelled during pipeline download")
                        if not _pipeline_progress_msg or not _pipeline_loop or total <= 0:
                            return
                        try:
                            pct = min(int(sent * 100 / total), 100)
                            now = time.time()
                            if pct == _pl_last_pct[0] and (now - _pl_last_time[0]) < 1.0:
                                return
                            _pl_last_pct[0] = pct
                            _pl_last_time[0] = now
                            mb_sent = sent // (1024 * 1024)
                            mb_total = total // (1024 * 1024)
                            batch_label = current_file.get("_pipeline_batch_id")
                            if batch_label:
                                # Same single message, same shape as every other
                                # stage of this file - only the state changes.
                                text = _batch_member_text(
                                    batch_label,
                                    current_file.get("_pipeline_file_index"),
                                    current_file.get("_pipeline_file_total"),
                                    current_file.get("name"),
                                    {"status": "downloading", "message": "downloading", "progress": pct},
                                )
                            else:
                                text = f"⬇️ Pipeline download: {pct}% ({mb_sent}MB / {mb_total}MB)"
                            asyncio.run_coroutine_threadsafe(
                                _pipeline_progress_msg.edit_text(text, reply_markup=_batch_stop_markup(batch_label)),
                                _pipeline_loop,
                            )
                        except Exception:
                            pass

                    try:
                        _pipeline_cancel_task = asyncio.create_task(_watch_pipeline_cancel())
                        _ingest = await _bigfile_pipeline.ingest_large_file(
                            chat_id=_pipeline_chat,
                            message_id=_pipeline_msg,
                            file_size=file_size,
                            file_unique_id=current_file.get("file_unique_id"),
                            user_id=user_id,
                            original_filename=current_file.get("name"),
                            ffmpeg_args=current_file.get("_pipeline_ffmpeg_args"),
                            conversion_type=current_file.get("_pipeline_conversion_type") or "ffmpeg",
                            output_ext=current_file.get("_pipeline_output_ext"),
                            caption=current_file.get("_pipeline_caption"),
                            progress_callback=_pipeline_progress_cb,
                            batch_id=current_file.get("_pipeline_batch_id"),
                            batch_seq=int(current_file.get("_pipeline_batch_seq") or 0),
                            batch_total=int(current_file.get("_pipeline_batch_total") or 0),
                            cancel_check=lambda: _pipeline_cancelled[0],
                        )
                        if _ingest.error == "batch cancelled":
                            with contextlib.suppress(Exception):
                                if _pipeline_progress_msg:
                                    await _pipeline_progress_msg.edit_text("⏹️ Batch download cancelled.")
                            # The relay forward for this file ran *before* the
                            # pipeline could refuse it, so the copy is now dead
                            # weight - remove it instead of leaving the batch's
                            # in-flight file sitting in the relay group.
                            if _relay_forwarded_already:
                                await self._discard_relay_copy(
                                    context, _pipeline_chat, _pipeline_msg, "batch cancelled"
                                )
                            await self._cleanup_dedup_key(_dedup_key)
                            return
                        if _ingest.ok:
                            logger.info(
                                "Big files pipeline: job %s queued for %s/%s (%dMB)",
                                _ingest.job_id,
                                _pipeline_chat,
                                _pipeline_msg,
                                file_size // (1024 * 1024),
                            )
                            # ── Flag the session so convert_video_format (and other
                            #    callback handlers) know a pipeline job is already
                            #    queued and should NOT enqueue a duplicate. ──
                            if current_file is not None:
                                current_file["_pipeline_job_id"] = _ingest.job_id
                                # The ingest ffprobed the source itself, and its
                                # verdict is the only place a large file's title
                                # and performer can come from: this path never ran
                                # the disk probe that fills ``_source_metadata``,
                                # so every big file was delivered with a
                                # filename-derived caption. Keep whatever the
                                # session already had when the ingest had nothing.
                                if _ingest.source_metadata:
                                    current_file["_source_metadata"] = dict(_ingest.source_metadata)
                                # Also persist the S3 input_key so that
                                # _ensure_current_file_downloaded's early-return
                                # check (input_key or path.exists) catches this
                                # on subsequent calls from ANY handler, preventing
                                # a second pipeline run entirely.
                                if _ingest.s3_key:
                                    current_file["input_key"] = _ingest.s3_key
                                session["current_file"] = current_file
                                try:
                                    self._persist_session(user_id)
                                except Exception:
                                    logger.debug("Could not persist pipeline job flag")

                            # ── Set Redis dedup flag so the pipeline won't re-run
                            #    even if the session is lost or reloaded. ──
                            if _dedup_key:
                                try:
                                    _r_dedup_save = await get_redis()
                                    try:
                                        await _r_dedup_save.set(_dedup_key, _ingest.job_id, ex=86400)
                                        logger.info(
                                            "Pipeline dedup: set redis flag %s = %s (TTL 2h)",
                                            _dedup_key,
                                            _ingest.job_id,
                                        )
                                    finally:
                                        with contextlib.suppress(Exception):
                                            await _r_dedup_save.close()
                                except Exception:
                                    pass

                            # Return BEFORE the notification so pipeline success
                            # always short-circuits even if reply_text fails
                            # (e.g. when update.message is None for relay-originated files).
                            _notify_text = (
                                f"Large file ({file_size // (1024 * 1024)} MB) queued for processing.\n"
                                f"Job: {_ingest.job_id[:8]}... You will receive the result shortly."
                            )
                            _queued_message = None
                            if _pipeline_batch_id:
                                # The apply's message already names this file and now
                                # says it is queued, so a batch posts nothing else -
                                # which also means there is no per-file message left
                                # for a later watcher to delete out from under it.
                                if _pipeline_progress_msg:
                                    with contextlib.suppress(BadRequest):
                                        await _pipeline_progress_msg.edit_text(
                                            _batch_member_text(
                                                _pipeline_batch_id,
                                                current_file.get("_pipeline_file_index"),
                                                current_file.get("_pipeline_file_total"),
                                                current_file.get("name"),
                                                {"status": "queued", "message": "queued", "progress": 0},
                                            ),
                                            reply_markup=_batch_stop_markup(_pipeline_batch_id),
                                        )
                            elif _pipeline_progress_msg:
                                with contextlib.suppress(BadRequest):
                                    await _pipeline_progress_msg.edit_text(_notify_text)
                                _queued_message = _pipeline_progress_msg
                            elif update and update.message:
                                _queued_message = await update.message.reply_text(_notify_text)
                            elif update and update.effective_user and context and context.bot:
                                with contextlib.suppress(Exception):
                                    _queued_message = await context.bot.send_message(
                                        chat_id=update.effective_user.id,
                                        text=_notify_text,
                                    )

                            # Persist the queued notification message metadata so the
                            # background job watcher can remove it automatically once the
                            # processing job finishes.
                            if _queued_message is not None:
                                try:
                                    _queued_chat_id = getattr(_queued_message, "chat_id", None)
                                    if _queued_chat_id is None:
                                        _queued_chat = getattr(_queued_message, "chat", None)
                                        _queued_chat_id = getattr(_queued_chat, "id", None)
                                    _queued_message_id = getattr(_queued_message, "message_id", None)
                                    if _queued_chat_id and _queued_message_id:
                                        from utils.job_queue import get_redis as _get_r

                                        _r_queue_meta = await _get_r()
                                        try:
                                            await _r_queue_meta.hset(
                                                f"ffmpeg:job:{_ingest.job_id}",
                                                mapping={
                                                    "queued_message_chat_id": str(_queued_chat_id),
                                                    "queued_message_id": str(_queued_message_id),
                                                },
                                            )
                                        finally:
                                            with contextlib.suppress(Exception):
                                                await _r_queue_meta.close()
                                except Exception:
                                    logger.debug(
                                        "Big files pipeline: failed to store queued notification metadata for job %s",
                                        _ingest.job_id,
                                    )
                            # ── NOTE: We do NOT start _watch_job_progress here because the
                            #    caller (convert_video_format, optimize_video, etc.) will
                            #    start its own watcher on the callback message after detecting
                            #    _pipeline_job_id. Starting a second watcher would create
                            #    duplicate progress messages and make it look like two jobs.
                            #
                            #    A *batch* caller does start one, but from
                            #    _await_bulk_pipeline_job and bound to the file's own
                            #    message instead of the apply's - because while nothing
                            #    watched those jobs, a batch file showed nothing at all
                            #    between download and delivery. ──
                            return
                        else:
                            logger.warning(
                                "Big files pipeline failed: %s (chat=%s msg=%s); falling back to Bot API",
                                _ingest.error,
                                _pipeline_chat,
                                _pipeline_msg,
                            )
                            # ── Clean up the "pending" dedup key so future retries aren't blocked ──
                            await self._cleanup_dedup_key(_dedup_key)
                    except Exception as pipeline_exc:
                        logger.warning(
                            "Big files pipeline error: %s (chat=%s msg=%s); falling back to Bot API",
                            pipeline_exc,
                            _pipeline_chat,
                            _pipeline_msg,
                        )
                        # ── Clean up the "pending" dedup key so future retries aren't blocked ──
                        await self._cleanup_dedup_key(_dedup_key)
                    finally:
                        if _pipeline_cancel_task is not None:
                            _pipeline_cancel_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError, Exception):
                                await _pipeline_cancel_task

            # ── Clean early error: file > Bot API download limit but no fallback available ──
            if file_size and file_size > bot_api_max_mb * 1024 * 1024 and _bigfile_pipeline is None:
                _userbot_enabled = config.ENABLE_USERBOT
                if not _userbot_enabled:
                    _upload_url_early = (
                        os.environ.get("WEB_UPLOAD_URL") or os.environ.get("WEBAPP_URL") or "<your-server>/upload"
                    )
                    raise Exception(
                        f"File is {file_size // (1024 * 1024)} MB, which exceeds the "
                        f"{bot_api_max_mb} MB download limit.\n\n"
                        "To process files larger than this size, configure one of these:\n"
                        "• Set ENABLE_USERBOT=true and configure PYROGRAM_SESSION "
                        "(or API_ID/API_HASH + API_SESSION) so the bot can use a user account.\n"
                        f"• Upload the file via the web uploader at {_upload_url_early}."
                    )
                logger.info(
                    "BigFilePipeline unavailable but userbot fallback is enabled — "
                    "will attempt userbot download for %dMB file",
                    file_size // (1024 * 1024),
                )

            file = await context.bot.get_file(file_id)
        except Exception as e:
            logger.exception("get_file failed for %s: %s", file_id, e)
            err_text = str(e) or ""
            upload_url = os.environ.get("WEB_UPLOAD_URL") or os.environ.get("WEBAPP_URL") or "<your-server>/upload"
            enable_userbot = config.ENABLE_USERBOT

            async def _try_userbot_download(chat_id, message_id, reason):
                if not enable_userbot or not chat_id or not message_id:
                    return False
                try:
                    from utils.userbot_downloader import download_forward_via_userbot

                    logger.info(
                        "Attempting userbot download fallback (%s) for %s/%s -> %s",
                        reason,
                        chat_id,
                        message_id,
                        file_path,
                    )

                    # ── Download progress bar: edit callback message with Cancel button ──
                    _dl_progress_msg = None
                    _dl_loop = None
                    _cancel_key = f"{chat_id}:{message_id}"
                    _cancel_flag = [False]  # thread-safe via list mutation
                    _download_cancel_flags[_cancel_key] = _cancel_flag
                    try:
                        # Prefer editing the callback query message for continuity
                        _cq = getattr(update, "callback_query", None)
                        _kb_cancel = InlineKeyboardMarkup(
                            [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_dl:{_cancel_key}")]]
                        )
                        if _cq:
                            _dl_progress_msg = _cq.message
                            await _dl_progress_msg.edit_text("⬇️ Starting download...", reply_markup=_kb_cancel)
                        else:
                            _dl_progress_msg = await update.effective_message.reply_text(
                                "⬇️ Starting download...", reply_markup=_kb_cancel
                            )
                        _dl_loop = asyncio.get_running_loop()
                    except Exception:
                        pass

                    _dl_last_pct = [-1]
                    _dl_last_time = [0.0]

                    def _dl_progress_cb(sent, total):
                        """Sync callback called by Pyrogram/Telethon download_media.

                        Raises an exception when the user presses the Cancel button
                        so the download aborts and propagates up.
                        """
                        if not _dl_progress_msg or not _dl_loop or total <= 0:
                            return
                        try:
                            # Check if user pressed Cancel
                            if _cancel_flag[0]:
                                raise asyncio.CancelledError("Download cancelled by user")
                            pct = min(int(sent * 100 / total), 100)
                            now = time.time()
                            if pct == _dl_last_pct[0] and (now - _dl_last_time[0]) < 1.0:
                                return
                            _dl_last_pct[0] = pct
                            _dl_last_time[0] = now
                            mb_sent = sent // (1024 * 1024)
                            mb_total = total // (1024 * 1024)
                            text = f"⬇️ Downloading: {pct}% ({mb_sent}MB / {mb_total}MB)"
                            asyncio.run_coroutine_threadsafe(
                                _dl_progress_msg.edit_text(text, reply_markup=_kb_cancel),
                                _dl_loop,
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            pass

                    ok = await download_forward_via_userbot(
                        chat_id,
                        message_id,
                        file_path,
                        msg_date=current_file.get("msg_date")
                        or current_file.get("date")
                        or current_file.get("registered_at"),
                        file_unique_id=current_file.get("file_unique_id"),
                        progress_callback=_dl_progress_cb,
                        user_id=update.effective_user.id,
                    )
                    logger.info(
                        "Userbot download fallback (%s) result for %s/%s: ok=%s exists=%s",
                        reason,
                        chat_id,
                        message_id,
                        bool(ok),
                        os.path.exists(file_path),
                    )
                    # Update progress message to reflect final state
                    if _dl_progress_msg:
                        try:
                            if ok and os.path.exists(file_path):
                                await _dl_progress_msg.edit_text("✅ Download complete!")
                            else:
                                await _dl_progress_msg.edit_text("❌ Download failed")
                        except Exception:
                            pass
                    if ok and os.path.exists(file_path):
                        current_file["path"] = file_path
                        session["current_file"] = current_file
                        try:
                            self._persist_session(user_id)
                        except Exception:
                            logger.debug("Could not persist session after userbot download (%s)", reason)
                        _download_cancel_flags.pop(_cancel_key, None)
                        return True
                except asyncio.CancelledError:
                    # User cancelled the download — update message and return False
                    # NOTE: do NOT re-raise — `CancelledError` is a `BaseException` in
                    # Python 3.12+ so it would blast through all `except Exception:`
                    # blocks in the call stack and crash the PTB handler.
                    if _dl_progress_msg:
                        with contextlib.suppress(BadRequest):
                            await _dl_progress_msg.edit_text("⏹️ Download cancelled.")
                    return False
                except Exception:
                    logger.exception("Userbot download fallback failed (%s) for %s/%s", reason, chat_id, message_id)
                finally:
                    _download_cancel_flags.pop(_cancel_key, None)
                return False

            # For large-file bot API failures, try the userbot fallback first.
            # Guard: skip relay group fallback if the bigfile pipeline already
            # forwarded this file to the relay group.
            if enable_userbot and ("file is too big" in err_text.lower() or "too big" in err_text.lower()):
                _relay_already_done = _relay_forwarded_already
                forward = current_file.get("forward") if current_file else None
                if (
                    forward
                    and forward.get("chat_id")
                    and forward.get("message_id")
                    and await _try_userbot_download(forward.get("chat_id"), forward.get("message_id"), "origin_forward")
                ):
                    return

                bot_chat = current_file.get("chat_id")
                bot_msg = current_file.get("msg_id") or current_file.get("message_id")
                if bot_chat and bot_msg and await _try_userbot_download(bot_chat, bot_msg, "bot_chat_large_file"):
                    return

                # Relay group fallback: forward the file to a shared group/channel
                # where the userbot account has access, then download from there.
                # Guard: skip if the bigfile pipeline already forwarded to the relay group
                # for this file (prevents double forwarding).
                _relay_chat_id = config.RELAY_CHAT_ID
                if _relay_chat_id and bot_chat and bot_msg and not _relay_already_done:
                    try:
                        _relay_chat_id = int(_relay_chat_id)
                        logger.info(
                            "Relay: forwarding message %s/%s to relay group %s",
                            bot_chat,
                            bot_msg,
                            _relay_chat_id,
                        )
                        _forwarded = await context.bot.forward_message(
                            chat_id=_relay_chat_id,
                            from_chat_id=bot_chat,
                            message_id=bot_msg,
                        )
                        if _forwarded and getattr(_forwarded, "message_id", None):
                            _relay_msg_id = _forwarded.message_id
                            logger.info(
                                "Relay: forwarded to %s/%s, trying userbot download",
                                _relay_chat_id,
                                _relay_msg_id,
                            )
                            if await _try_userbot_download(_relay_chat_id, _relay_msg_id, "relay_group"):
                                return
                            logger.warning(
                                "Relay: userbot download from %s/%s failed",
                                _relay_chat_id,
                                _relay_msg_id,
                            )
                            # The copy was forwarded for that download only.
                            await self._discard_relay_copy(context, _relay_chat_id, _relay_msg_id, "download failed")
                    except Exception as _relay_exc:
                        logger.exception("Relay: forwarding/download failed: %s", _relay_exc)

            # If userbot fallback did not produce a file, keep the existing large-forward
            # handling behavior so the bot can persist metadata or provide upload guidance.
            if "file is too big" in err_text.lower() or "too big" in err_text.lower():
                try:
                    fh = await self._handle_large_forward(update, context, current_file, err_text, upload_url)
                    if fh:
                        raise Exception(
                            "Telegram reports the file is too big to download via the bot. "
                            f"Forward saved (id={fh}). The server will attempt to fetch it; or upload via {upload_url}?forward_hash={fh}"
                        )
                    else:
                        raise Exception(
                            "Telegram reports the file is too big to download via the bot. "
                            f"Please either upload the file via the web uploader (POST to {upload_url}) or provide a direct public URL to the file."
                            + (
                                " Configure PYROGRAM_SESSION (preferred) or API_ID/API_HASH + API_SESSION if you want automatic userbot fallback."
                                if enable_userbot
                                else ""
                            )
                        )
                except Exception:
                    raise Exception(
                        "Telegram reports the file is too big to download via the bot. "
                        f"Please either upload the file via the web uploader (POST to {upload_url}) or provide a direct public URL to the file."
                        + (
                            " Configure PYROGRAM_SESSION (preferred) or API_ID/API_HASH + API_SESSION if you want automatic userbot fallback."
                            if enable_userbot
                            else ""
                        )
                    ) from None

            # All userbot download attempts have been exhausted above.
            # Re-raise the original get_file/API error without retrying
            # the same dead-end fallbacks again.
            raise

        # Check if we should stream directly to remote storage (S3/R2), skipping local disk
        _backend = None
        _use_remote = False
        try:
            from utils.storage import get_storage_backend as _gsb

            _bn = config.get_storage_backend_name()
            if _bn in ("s3", "r2") and _gsb is not None:
                _backend = await _gsb()
                _use_remote = True
        except Exception:
            logger.debug("handlers: Check if we should stream directly to remote storage (S3/R2), skipp...")

        if _use_remote and _backend is not None:
            # ── Pipeline flow: download to temp → ffprobe → upload with job_id key → metadata in Redis ──
            _job_id = uuid.uuid4().hex
            _temp_dir = getattr(config, "TEMP_PATH", "storage/temp")
            with contextlib.suppress(Exception):
                os.makedirs(_temp_dir, exist_ok=True)
            # Keyed by job id rather than the upload second: the file is kept
            # for the worker to reuse (see below), so two uploads must never
            # share a name or one job could read the other's bytes.
            _temp_path = os.path.join(_temp_dir, f"src_{user_id}_{_job_id}{ext}")

            # ── Media cache: reuse a copy already in object storage so a repeat of
            #    the same media skips both the Telegram download and the upload.
            #    This is the piped (S3/R2) branch, which the local-disk cache
            #    short-circuit earlier never reaches. ──
            _cache_uid = current_file.get("file_unique_id")
            _library_key = None
            try:
                from utils import media_cache as _media_cache

                if _cache_uid and _media_cache.cache_enabled():
                    _library_key = _media_cache.media_library_key(_cache_uid)
                    _entry = await _media_cache.lookup(_cache_uid, expected_size=current_file.get("size"))
                    _stored_key = (_entry or {}).get("input_key")
                    if _stored_key:
                        _stored_ok = True
                        try:
                            _stored_ok = await _backend.exists(_stored_key)
                        except Exception:
                            _stored_ok = True
                        if _stored_ok:
                            current_file["input_key"] = _stored_key
                            current_file["path"] = None
                            # The metadata captured when this media was ingested
                            # belongs to *this* media, and it is what the caption
                            # and the audio tags are built from. This path used to
                            # blank it, so every repeat - the second style applied
                            # to a file, and every batch file after the first -
                            # delivered with the filename instead of the title and
                            # performer the file actually carries.
                            current_file.setdefault("_source_metadata", {})
                            session["current_file"] = current_file
                            with contextlib.suppress(Exception):
                                self._persist_session(user_id)
                            logger.info(
                                "media cache: reused stored input_key=%s for user %s (file_unique_id=%s)",
                                _stored_key,
                                user_id,
                                _cache_uid,
                            )
                            return
            except Exception:
                logger.debug("handlers: remote media-cache lookup failed for %s", file_id)

            # Download to temp file (disk, not bytearray — ffprobe needs a local file)
            await file.download_to_drive(_temp_path)

            # ── T4: ffprobe source analysis ──
            _source_meta = {}
            try:
                from utils.ffmpeg_runner import probe_media as _probe_media

                _source_meta = await _probe_media(_temp_path)
            except Exception:
                logger.debug("handlers: source ffprobe failed for %s", file_id)

            # Upload to storage. With the media cache on the key is derived from
            # the media identity, so the object is reused by the next request for
            # this file instead of being downloaded and uploaded again.
            _input_key = _library_key or f"inputs/{_job_id}/source{ext}"
            await _backend.upload_file(_temp_path, _input_key)

            # Record where this media now lives (and its bytes when small enough
            # for Redis) so the reuse paths can find it. Read the body before the
            # temp file is removed below.
            try:
                from utils import media_cache as _media_cache

                if _cache_uid:
                    _cached_size = current_file.get("size")
                    with contextlib.suppress(Exception):
                        _cached_size = os.path.getsize(_temp_path)
                    _payload = None
                    if _cached_size and _cached_size <= _media_cache.bytes_cache_limit():
                        with contextlib.suppress(Exception), open(_temp_path, "rb") as _fh:
                            _payload = _fh.read()
                    await _media_cache.remember(
                        _cache_uid,
                        size=_cached_size,
                        input_key=_input_key,
                        name=current_file.get("name"),
                        storage="s3",
                        data=_payload,
                        # The location token the upload came from: it is what lets
                        # a later step forward the media inside Telegram instead
                        # of fetching it down to this box again.
                        file_id=current_file.get("id"),
                    )
            except Exception:
                logger.debug("handlers: failed to remember remote media in cache")

            # ── Source metadata stored on current_file; the callback handler
            #    will write it to Redis when the user picks an action. ──

            # The uploaded copy is deliberately left on disk when local reuse is
            # on (the default). The worker usually runs in this very container,
            # so a local copy means it can read the source instead of pulling
            # the same bytes back out of storage - a full extra copy of the
            # media out of the bucket for every job, which is pure egress and,
            # on a large video, minutes of transfer before ffmpeg starts.
            #
            # `_local_input_path` is only a hint: the worker prefers it and falls
            # straight back to `input_key` whenever the file is not there (a
            # separate worker container, a restart, or the temp sweep), so
            # nothing depends on it surviving. The worker deletes it after the
            # job and the temp sweep reclaims any that are abandoned.
            _keep_local_input = os.getenv("REUSE_LOCAL_INPUT", "1").strip().lower() not in (
                "0",
                "false",
                "no",
                "off",
            )
            try:
                _local_path = _temp_path if _keep_local_input and os.path.exists(_temp_path) else None
            except Exception:
                _local_path = None
            if _local_path:
                logger.debug("handlers: kept local source %s for reuse by the worker", _local_path)
            else:
                try:
                    os.remove(_temp_path)
                    logger.debug("handlers: cleaned up temp file %s", _temp_path)
                except Exception:
                    pass

            current_file["input_key"] = _input_key
            current_file["_source_job_id"] = _job_id
            current_file["_source_metadata"] = _source_meta
            current_file["path"] = None
            current_file["_local_input_path"] = _local_path

            logger.info(
                "Source ffprobe → S3: %s/%s → %s (dur=%s codec=%s %sx%s fps=%s rot=%s audio=%s)",
                user_id,
                file_id,
                _input_key,
                _source_meta.get("duration", "?"),
                _source_meta.get("video_codec", "?"),
                _source_meta.get("width", "?"),
                _source_meta.get("height", "?"),
                _source_meta.get("fps", "?"),
                _source_meta.get("rotation", "?"),
                _source_meta.get("audio_codec", "?"),
            )
        else:
            # Fallback: download to local disk
            await file.download_to_drive(file_path)
            try:
                final_name = await detect_filename(file_path, getattr(update, "message", None))
                if final_name:
                    current_file["name"] = final_name
            except Exception:
                logger.debug("detect_filename failed after download")
            current_file["path"] = file_path

            # Remember the body so a repeat of this media skips the download.
            # Only small media are stored in Redis; larger ones are covered by
            # the shared library key on the big-file pipeline instead.
            try:
                from utils import media_cache as _media_cache

                _uid = current_file.get("file_unique_id")
                if _uid:
                    _size = os.path.getsize(file_path)
                    _payload = None
                    if _size <= _media_cache.bytes_cache_limit():
                        with open(file_path, "rb") as _fh:
                            _payload = _fh.read()
                    await _media_cache.remember(
                        _uid,
                        size=_size,
                        # Record the on-disk location too: it lets a repeat reuse
                        # the file directly when it is too large for the bytes
                        # tier, instead of re-fetching it from Telegram.
                        path=file_path,
                        name=current_file.get("name"),
                        storage="local",
                        data=_payload,
                        file_id=current_file.get("id"),
                    )
            except Exception:
                logger.debug("handlers: failed to remember media in cache")

        session["current_file"] = current_file
        try:
            self._persist_session(user_id)
        except Exception:
            logger.debug("Could not persist session after download")

    async def _handle_large_forward(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        current_file: dict,
        err_text: str,
        upload_url: str,
    ):
        """Persist forward metadata, optionally auto-fetch and enqueue, or raise an instruction.

        This centralizes the previous inline logic for handling "file is too big" errors
        so handlers can call it non-blockingly and keep the download flow readable.
        """
        try:
            from utils.forward_store import delete_forward_metadata, save_forward_metadata

            metadata = {
                "chat_id": current_file.get("chat_id"),
                "message_id": current_file.get("msg_id") or current_file.get("message_id"),
                "file_id": current_file.get("id") or current_file.get("file_id"),
                "file_unique_id": current_file.get("file_unique_id"),
                "name": current_file.get("name"),
                "size": current_file.get("size"),
                "type": current_file.get("type"),
                "registered_at": utc_iso(),
            }
            fh = await save_forward_metadata(metadata)
            logger.info("Saved forward metadata id=%s for file_id=%s", fh, metadata.get("file_id"))

            auto_fetch = os.environ.get("AUTO_FETCH_FORWARDS", "").lower() in ("1", "true", "yes")
            web_upload_url = os.environ.get("WEB_UPLOAD_URL") or os.environ.get("WEBAPP_URL")

            # Diagnostic logging to help trace fallback behavior in production
            logger.info("_handle_large_forward: fh=%s auto_fetch=%s web_upload_url=%s", fh, auto_fetch, web_upload_url)

            # Extra debug: record which fetch paths we will try
            try:
                enable_userbot_env = config.ENABLE_USERBOT
                logger.info(
                    "_handle_large_forward debug: fh=%s enable_userbot_env=%s AUTO_FETCH_FORWARDS=%s PREFER_USERBOT=%s",
                    fh,
                    enable_userbot_env,
                    auto_fetch,
                    os.environ.get("PREFER_USERBOT"),
                )
            except Exception:
                logger.debug("handlers: Extra debug: record which fetch paths we will try")

            if auto_fetch:
                # Prepare local paths
                jid = str(uuid.uuid4())

                input_dir = getattr(
                    config, "INPUT_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage", "input")
                )
                with contextlib.suppress(OSError):
                    os.makedirs(input_dir, exist_ok=True)

                _meta_name = metadata.get("name") or ""
                if _meta_name and sanitize_filename is not None:
                    _safe_name = await sanitize_filename(_meta_name)
                else:
                    _safe_name = _meta_name
                ext = os.path.splitext(_safe_name)[1] or ".mp4"
                input_path = os.path.join(input_dir, f"{jid}{ext}") if jid else os.path.join(input_dir, f"{fh}{ext}")

                fetched = False

                # Try local userbot downloader first when enabled
                enable_userbot = config.ENABLE_USERBOT
                logger.info("_handle_large_forward: enable_userbot=%s for fh=%s", enable_userbot, fh)

                if enable_userbot:
                    try:
                        from utils.userbot_downloader import download_forward_via_userbot
                    except Exception as e:
                        logger.exception("Failed to import userbot_downloader: %s", e)
                        download_forward_via_userbot = None

                    if download_forward_via_userbot is not None:
                        # ── Cancel-download setup for this fallback path ──
                        _chat_id = metadata.get("chat_id")
                        _msg_id = metadata.get("message_id") or metadata.get("msg_id")
                        _cancel_key = f"{_chat_id}:{_msg_id}" if _chat_id and _msg_id else f"fh:{fh}"
                        _cancel_flag = [False]
                        _download_cancel_flags[_cancel_key] = _cancel_flag
                        _dl_progress_msg = None
                        _dl_loop = None
                        try:
                            _cq = getattr(update, "callback_query", None)
                            _kb_cancel = InlineKeyboardMarkup(
                                [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_dl:{_cancel_key}")]]
                            )
                            if _cq:
                                _dl_progress_msg = _cq.message
                                await _dl_progress_msg.edit_text("⬇️ Fetching via userbot...", reply_markup=_kb_cancel)
                            else:
                                _dl_progress_msg = await update.effective_message.reply_text(
                                    "⬇️ Fetching via userbot...", reply_markup=_kb_cancel
                                )
                            _dl_loop = asyncio.get_running_loop()
                        except Exception:
                            pass

                        _dl_last_pct = [-1]
                        _dl_last_time = [0.0]

                        def _dl_progress_cb(sent, total):
                            """Sync callback called by Pyrogram/Telethon download_media."""
                            if not _dl_progress_msg or not _dl_loop or total <= 0:
                                return
                            try:
                                if _cancel_flag[0]:
                                    raise asyncio.CancelledError("Download cancelled by user")
                                pct = min(int(sent * 100 / total), 100)
                                now = time.time()
                                if pct == _dl_last_pct[0] and (now - _dl_last_time[0]) < 1.0:
                                    return
                                _dl_last_pct[0] = pct
                                _dl_last_time[0] = now
                                mb_sent = sent // (1024 * 1024)
                                mb_total = total // (1024 * 1024)
                                text = f"⬇️ Fetching: {pct}% ({mb_sent}MB / {mb_total}MB)"
                                asyncio.run_coroutine_threadsafe(
                                    _dl_progress_msg.edit_text(text, reply_markup=_kb_cancel),
                                    _dl_loop,
                                )
                            except asyncio.CancelledError:
                                raise
                            except Exception:
                                pass

                        ok = False
                        try:
                            logger.info(
                                "Attempting userbot download for fh=%s chat=%s msg=%s -> %s",
                                fh,
                                _chat_id,
                                _msg_id,
                                input_path,
                            )
                            ok = await download_forward_via_userbot(
                                _chat_id,
                                _msg_id,
                                input_path,
                                msg_date=metadata.get("registered_at") or metadata.get("created_at"),
                                file_unique_id=metadata.get("file_unique_id"),
                                progress_callback=_dl_progress_cb,
                                user_id=update.effective_user.id,
                            )
                            logger.info(
                                "userbot download result for fh=%s: ok=%s exists=%s",
                                fh,
                                bool(ok),
                                os.path.exists(input_path),
                            )
                            if ok and os.path.exists(input_path):
                                fetched = True
                        except asyncio.CancelledError:
                            # User cancelled — update message and do NOT re-raise (see _try_userbot_download)
                            if _dl_progress_msg:
                                with contextlib.suppress(BadRequest):
                                    await _dl_progress_msg.edit_text("⏹️ Fetch cancelled.")
                        except Exception:
                            logger.exception("auto-fetch via userbot failed for %s", fh)
                        finally:
                            _download_cancel_flags.pop(_cancel_key, None)
                            # Update progress message to reflect final state
                            if _dl_progress_msg and not _cancel_flag[0]:
                                try:
                                    if ok and os.path.exists(input_path):
                                        await _dl_progress_msg.edit_text("✅ Fetch complete!")
                                    else:
                                        await _dl_progress_msg.edit_text("❌ Fetch failed")
                                except Exception:
                                    pass

                # If local fetch failed and web upload endpoint is configured, ask webapp to fetch
                if not fetched and web_upload_url:
                    logger.info("Attempting server fetch via web upload URL %s for fh=%s", web_upload_url, fh)

                    def _post_fetch():
                        headers = {}
                        import requests

                        upload_secret = os.environ.get("UPLOAD_SECRET")
                        if upload_secret:
                            headers["X-Upload-Token"] = upload_secret
                        try:
                            resp = requests.post(web_upload_url, data={"forward_hash": fh}, headers=headers, timeout=60)
                            return resp
                        except requests.RequestException as e:
                            logger.exception("Webapp fetch POST failed: %s", e)
                            return None

                    resp = await asyncio.get_running_loop().run_in_executor(None, _post_fetch)
                    logger.info("Webapp fetch response for fh=%s: resp=%s", fh, getattr(resp, "status_code", None))

                    if resp is not None and getattr(resp, "status_code", None) == 200:
                        try:
                            j = resp.json()
                            queued_job = j.get("job_id")
                            # notify user
                            try:
                                if getattr(update, "callback_query", None):
                                    await self.safe_edit(
                                        update.callback_query,
                                        f"✅ Server fetched and queued conversion (job {queued_job}).",
                                    )
                                elif getattr(update, "message", None):
                                    await update.message.reply_text(
                                        f"✅ Server fetched and queued conversion (job {queued_job})."
                                    )
                            except Exception:
                                logger.debug("handlers: notify user")
                            # delete saved forward metadata to avoid duplicates
                            # Best-effort cleanup: storage backends may raise OSError or RuntimeError
                            with contextlib.suppress(OSError, RuntimeError):
                                await delete_forward_metadata(fh)
                            return fh
                        except Exception:
                            logger.exception("Failed to parse webapp enqueue response for %s", fh)

                # If we successfully fetched locally, enqueue the job directly
                if fetched:
                    job_id = str(uuid.uuid4())
                    output_dir = getattr(
                        config,
                        "OUTPUT_PATH",
                        os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage", "output"),
                    )
                    with contextlib.suppress(OSError):
                        os.makedirs(output_dir, exist_ok=True)
                    _raw_name = metadata.get("name") or os.path.basename(input_path)
                    _safe_name = (
                        await sanitize_filename(_raw_name)
                        if sanitize_filename is not None
                        else os.path.basename(_raw_name)
                    )
                    base_name = os.path.splitext(_safe_name)[0]
                    output_path = os.path.join(output_dir, f"{base_name}_{job_id}.mp4")
                    job = {
                        "job_id": job_id,
                        "input_path": input_path,
                        "output_path": output_path,
                        "original_filename": _safe_name,
                        "output_filename": os.path.basename(output_path),
                        "ffmpeg_args": [
                            "-c:v",
                            "libx264",
                            "-preset",
                            "veryfast",
                            "-crf",
                            "23",
                            "-c:a",
                            "aac",
                            "-b:a",
                            "128k",
                            "-maxrate",
                            "2M",
                            "-bufsize",
                            "4M",
                        ],
                        "progress_channel": f"ffmpeg:progress:{job_id}",
                        "chat_id": update.effective_chat.id
                        if update and getattr(update, "effective_chat", None)
                        else None,
                        "thumbnail": metadata.get("thumbnail"),
                        "cleanup_input": True,
                        "cleanup_output": False,
                    }
                    # enqueue_job is already imported at module level
                    await enqueue_job(job)
                    # notify user (prefer editing the callback message when available)
                    try:
                        q = getattr(update, "callback_query", None)
                        if q is not None:
                            await self.safe_edit(q, f"✅ Fetched forwarded media and queued conversion (job {job_id}).")
                            with contextlib.suppress(RuntimeError):
                                asyncio.create_task(self._watch_job_progress(q, job_id, bot=context.bot))
                        else:
                            # fallback to replying in chat when no callback_query
                            if getattr(update, "message", None):
                                with contextlib.suppress(BadRequest):
                                    await update.message.reply_text(
                                        f"✅ Fetched forwarded media and queued conversion (job {job_id})."
                                    )
                    except Exception:
                        logger.exception("Failed to notify user after enqueue for %s", fh)
                    # cleanup saved forward metadata
                    # Best-effort cleanup: storage backends may raise OSError or RuntimeError
                    with contextlib.suppress(OSError, RuntimeError):
                        await delete_forward_metadata(fh)
                    return fh

            # Fallback: return instruction to user with forward-hash and web upload link
            raise Exception(
                "Telegram reports the file is too big to download via the bot. "
                f"You can either upload the file via the web uploader, or use this forward-hash to let the server fetch it via an opt-in userbot: {fh} -- visit {upload_url}?forward_hash={fh}"
            )
        except Exception:
            # Re-raise to allow caller to provide a simpler fallback message
            raise

    async def show_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show user settings with Redis cache for preferences."""
        user_id = update.effective_user.id
        # Try loading preferences from Redis cache first
        _cached_prefs = None
        try:
            if self._cache:
                _cached_prefs = await self._cache.get_user_session(str(user_id))
        except Exception:
            logger.debug("handlers: Try loading preferences from Redis cache first")
        if _cached_prefs and context.user_data is not None:
            # Merge cached prefs into user_data for fast access
            for k, v in _cached_prefs.items():
                if k not in context.user_data or context.user_data.get(k) is None:
                    context.user_data[k] = v
        user_id = update.effective_user.id
        if user_settings is None:
            await update.message.reply_text("⚠️ Settings not available (missing module).")
            return

        # Build a two-page settings keyboard with toggle switches
        s = user_settings.get_user_settings(user_id)

        # If called via callback with page param, the caller will handle; default to page 1
        # Build text and keyboard to match the requested control panel style
        text = "⚙️ <b>Config Bot Settings</b>\n\n"
        text += f"• Thumbnail : {'Yes' if s.get('use_custom_thumbnail') else 'No'}\n"
        text += f"• Rename File : {'Yes' if s.get('prefix') or s.get('suffix') else 'No'}\n"

        kb_page1 = [
            [
                InlineKeyboardButton(
                    f"Thumbnail : {'Yes' if s.get('use_custom_thumbnail') else 'No'}", callback_data="settings_page:2"
                )
            ],
            [
                InlineKeyboardButton(
                    f"Rename File : {'Yes' if s.get('prefix') or s.get('suffix') else 'No'}",
                    callback_data="video_renamer",
                )
            ],
            [InlineKeyboardButton("Upload as Audio", callback_data="menu_audio")],
            [InlineKeyboardButton("Upload as Video", callback_data="menu_video")],
            [InlineKeyboardButton("Stream Mapper", callback_data="menu_advanced")],
            [InlineKeyboardButton("Video Metadata", callback_data="full_info")],
            [InlineKeyboardButton("Mp3 Tag Setting", callback_data="mp3_tag_editor")],
            [InlineKeyboardButton("Audio Settings", callback_data="menu_audio")],
            [InlineKeyboardButton("Reset Settings", callback_data="reset_settings")],
            [InlineKeyboardButton("Close Settings", callback_data="menu_main")],
        ]

        await update.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb_page1))
        context.user_data.clear()
        context.user_data["settings_page"] = 1

    async def show_bulk_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show the bulk-mode action menu (either as reply or edit)."""
        user_id = update.effective_user.id if update and update.effective_user else None
        s = _read_bulk_settings(user_id, self.user_sessions.get(user_id))
        crf = _sanitize_bulk_crf(s.get("bulk_crf"))
        preset = _sanitize_bulk_preset(s.get("bulk_optimize_preset"))
        bitrate = _sanitize_bulk_extract_bitrate(s.get("bulk_extract_bitrate"))
        sess = self.user_sessions.get(user_id) or {}
        # Subtract anything an interrupted earlier run already finished, so the
        # count here and the next Apply agree and neither redoes completed work.
        _recovered = 0
        try:
            from utils.batch_pipeline import forget_finished_entries, read_finished_entries
            from utils.job_queue import get_redis as _get_redis_here

            _r_here = await _get_redis_here()
            _finished_here = await read_finished_entries(_r_here, user_id)
            if _finished_here:
                _done_here = set().union(*_finished_here.values())
                _entries_here = sess.get("bulk_list") or []
                _kept_here = [e for e in _entries_here if _bulk_entry_key(e) not in _done_here]
                _recovered = len(_entries_here) - len(_kept_here)
                if _recovered:
                    sess["bulk_list"] = _kept_here
                    await forget_finished_entries(_r_here, _finished_here, _done_here)
                    with contextlib.suppress(Exception):
                        self._persist_session(user_id)
                    logger.info(
                        "bulk menu: recovered %d already-finished file(s) for user %s",
                        _recovered,
                        user_id,
                    )
        except Exception:
            logger.debug("show_bulk_menu: could not reconcile the resume record")
        entries = sess.get("bulk_list") or sess.get("merge_list") or []
        queued = len(entries)
        seconds = _sanitize_bulk_slideshow_seconds(s.get("bulk_slideshow_seconds"))
        photos = [e for e in entries if isinstance(e, dict) and e.get("type") == "photo"]
        music = _bulk_slideshow_music(entries)
        _slideshow_line = f"Slideshow : {seconds:g}s per photo"
        if len(photos) >= 2:
            _slideshow_line += f" → 1 video from {len(photos)} photos"
        _music_line = html.escape(_bulk_display_name(music)) if music else "none"
        _batch_block = ""
        if entries:
            _batch_block = "🗂 <b>Batch</b>\n" + "\n".join(_bulk_batch_lines(entries)) + "\n\n"
        if _recovered:
            _batch_block += (
                f"↩️ {_recovered} file(s) here were already finished by an interrupted run "
                "and were removed from this list.\n\n"
            )
        if queued >= _BULK_LIST_LIMIT:
            _batch_block += (
                f"⚠️ The batch holds its maximum of {_BULK_LIST_LIMIT} files — "
                "sending another drops the oldest. Apply or Clear the list before adding more.\n\n"
            )
        text = (
            f"📦 <b>Bulk Mode Actions</b>\n\n"
            f"Files queued : {queued}\n"
            f"Quality >> Compress CRF : {crf} · Optimize preset : {preset} · Extract Audio : {bitrate}\n"
            f"{_slideshow_line}\n"
            f"🎵 Slideshow music : {_music_line}\n\n"
            f"{_batch_block}"
            "Toggle one or more actions, then press ▶️ Apply Bulk.\n"
            "<i>Every file you send is collected here automatically, and Apply runs "
            "on all of them. One encoding pass per file: Extract Audio replaces the "
            "video actions, Compress wins over Optimize over Convert, and Remove "
            "Audio / Rename combine with them. Two or more photos become one "
            "slideshow video, scored with the first queued audio file.</i>\n\n"
            "Please select your preferred action below 👇"
        )
        # Build keyboard defensively. MediaMenuBuilder may be missing or raise,
        # so capture failures and still show a helpful message.
        kb = None
        try:
            if MediaMenuBuilder and hasattr(MediaMenuBuilder, "get_bulk_menu"):
                try:
                    kb = MediaMenuBuilder.get_bulk_menu(s)
                except Exception:
                    logger.exception("MediaMenuBuilder.get_bulk_menu() raised an exception")
                    kb = None
        except Exception:
            # If the import resolved to None or something unexpected, continue
            kb = None

        try:
            if getattr(update, "callback_query", None):
                if kb:
                    await self.safe_edit(update.callback_query, text, reply_markup=kb, parse_mode="HTML")
                else:
                    await self.safe_edit(update.callback_query, text, parse_mode="HTML")
            elif getattr(update, "message", None):
                if kb:
                    await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")
                else:
                    await update.message.reply_text(text, parse_mode="HTML")
            else:
                logger.warning("show_bulk_menu called without message or callback")
                return
        except Exception as e:
            logger.exception("Failed to show bulk menu: %s", e)
            # Try a resilient fallback: notify the user directly via any available channel
            try:
                chat_id = None
                if getattr(update, "callback_query", None):
                    with contextlib.suppress(BadRequest):
                        await update.callback_query.answer()
                    if getattr(update.callback_query, "message", None) and getattr(
                        update.callback_query.message, "chat", None
                    ):
                        chat_id = update.callback_query.message.chat.id
                elif getattr(update, "message", None) and getattr(update.message, "chat", None):
                    chat_id = update.message.chat.id

                # Prefer sending through context.bot if available
                if chat_id and getattr(context, "bot", None):
                    try:
                        await context.bot.send_message(chat_id=chat_id, text="⚠️ Failed to open bulk menu.")
                    except Exception:
                        logger.exception("Fallback send_message failed for bulk menu")
                else:
                    # Try to message the user directly
                    try:
                        if getattr(update, "effective_user", None) and getattr(context, "bot", None):
                            await context.bot.send_message(
                                chat_id=update.effective_user.id, text="⚠️ Failed to open bulk menu."
                            )
                    except Exception:
                        logger.exception("Secondary fallback for bulk menu failed")
            except Exception:
                logger.exception("Secondary fallback for bulk menu failed")

    async def bulk_url_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enqueue one or more URLs provided as command arguments for processing."""
        if user_settings is None:
            await update.message.reply_text("⚠️ Settings not available (missing module).")
            return

        args = context.args if hasattr(context, "args") else []
        if not args:
            await update.message.reply_text(
                "Usage: /bulk_url <url1> [url2 ...]\nYou can also paste multiple URLs in a message."
            )
            return

        enqueued = 0
        for url in args:
            if not isinstance(url, str) or not url.startswith("http"):
                continue
            job_id = str(uuid.uuid4())
            job = {
                "job_id": job_id,
                "source_url": url,
                # Keeps the delivered filename derived from the URL instead of the job id.
                "original_filename": filename_from_url(url),
                "progress_channel": f"ffmpeg:progress:{job_id}",
                "chat_id": update.effective_chat.id if update and update.effective_chat else None,
                "cleanup_input": True,
            }
            try:
                # Propagate per-update request_id (if present) for end-to-end tracing
                try:
                    job["request_id"] = getattr(update, "request_id", None)
                except Exception:
                    job["request_id"] = None
                await enqueue_job(job)
                enqueued += 1
            except Exception:
                logger.exception("Failed to enqueue bulk URL %s", url)

        await update.message.reply_text(f"✅ Enqueued {enqueued} URL(s) for processing.")

    async def convert_video_format(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: dict,
        target_format: str,
    ):
        """Convert video to different format."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")
        user_id = update.effective_user.id

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        # ── Cancel any stale pipeline job so user's specific settings take effect ──
        if current_file.get("_pipeline_job_id"):
            await self._cancel_stale_pipeline_job(session, "convert_video_format", user_id)

        await self.safe_edit(query, f"🎬 Queuing conversion to {target_format.upper()}...")

        # ── Store conversion metadata so the BigFilePipeline knows what to produce ──
        _format_ffmpeg_args = {
            "mp4": ["-c:v", "libx264", "-c:a", "aac", "-strict", "experimental", "-movflags", "+faststart"],
            "mkv": ["-c:v", "libx264", "-c:a", "aac", "-movflags", "+faststart"],
            "avi": ["-c:v", "libx264", "-c:a", "mp3"],
            "mov": ["-c:v", "libx264", "-c:a", "aac", "-movflags", "+faststart"],
            "webm": ["-c:v", "libvpx-vp9", "-c:a", "libvorbis"],
            "flv": ["-c:v", "libx264", "-c:a", "aac"],
            "m4v": ["-c:v", "libx264", "-c:a", "aac", "-strict", "experimental", "-movflags", "+faststart"],
        }
        current_file["_pipeline_ffmpeg_args"] = _format_ffmpeg_args.get(target_format)
        current_file["_pipeline_output_ext"] = f".{target_format}"
        current_file["_pipeline_conversion_type"] = "format_video"
        current_file["_pipeline_caption"] = _metadata_caption(current_file)
        session["current_file"] = current_file

        # Ensure file is available locally (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
                # If pipeline queued a job (big file), watch it and return.
                # Don't create a duplicate local job or cancel the pipeline job.
                if current_file and current_file.get("_pipeline_job_id"):
                    _pipeline_job_id = current_file["_pipeline_job_id"]
                    kb = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{_pipeline_job_id}")]]
                    )
                    await self.safe_edit(
                        query,
                        f"✅ Large file queued (Job: {_pipeline_job_id[:8]}...). I'll send the result when ready.",
                        reply_markup=kb,
                    )
                    with contextlib.suppress(RuntimeError):
                        asyncio.create_task(self._watch_job_progress(query, _pipeline_job_id, bot=context.bot))
                    return
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        # Enqueue conversion job to Redis so a worker handles heavy lifting
        # (Only reached for small files downloaded via Bot API, not pipeline jobs)
        input_path = current_file["path"] or current_file.get("_local_input_path")
        output_ext = f".{target_format}"
        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{current_file['id']}_converted{output_ext}")
        job_id = str(uuid.uuid4())
        job = {
            "job_id": job_id,
            "input_path": input_path,
            "input_key": current_file.get("input_key"),
            "output_path": output_path,
            # Keeps the delivered filename derived from the original name.
            "original_filename": current_file.get("name") or os.path.basename(output_path),
            "ffmpeg_args": current_file.get("_pipeline_ffmpeg_args") or _format_ffmpeg_args.get(target_format),
            "output_ext": f".{target_format}",
            "progress_channel": f"ffmpeg:progress:{job_id}",
            "chat_id": update.effective_chat.id if update and update.effective_chat else None,
            "thumbnail": current_file.get("thumbnail"),
            "caption": f"Conversion to {target_format.upper()} finished",
            "cleanup_input": True,
            "cleanup_output": False,
        }

        try:
            try:
                job["request_id"] = getattr(update, "request_id", None)
            except Exception:
                job["request_id"] = None
            await enqueue_job(job)
        except Exception:
            logger.exception("Failed to enqueue job")
            await self.safe_edit(query, "❌ Failed to queue conversion.")
            return

        # Inform user job queued and provide a cancel button
        try:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}")]])
            await self.safe_edit(
                query, f"✅ Job queued (ID: {job_id}). I'll send the file when ready.", reply_markup=kb
            )
            with contextlib.suppress(RuntimeError):
                asyncio.create_task(self._watch_job_progress(query, job_id, bot=context.bot))
        except Exception:
            await self.safe_edit(query, f"✅ Job queued (ID: {job_id}). I'll send the file when ready.")
        return

    async def handle_media_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Main entry point for media messages."""
        # ── Bot-self detection: skip messages sent by the bot's own Telethon userbot ──
        #    When the userbot sends output to the relay group, the bot would otherwise
        #    pick it up as a new input file, creating an infinite processing loop.
        try:
            _msg_obj = getattr(update, "message", None)
            _from_user = getattr(_msg_obj, "from_user", None) if _msg_obj else None
            _sender_id = getattr(_from_user, "id", None) if _from_user else None
            if _sender_id is not None:
                try:
                    from utils.userbot_downloader import _get_bot_user_id

                    _bot_id = _get_bot_user_id()
                    if _bot_id is not None and _sender_id == _bot_id:
                        logger.info(
                            "handle_media_message: skipping message from bot itself (sender=%s == bot_id=%s) to prevent processing loop",
                            _sender_id,
                            _bot_id,
                        )
                        return
                except Exception:
                    pass
        except Exception:
            pass

        # Log incoming update for debugging dispatch issues
        try:
            user_id = update.effective_user.id
        except Exception:
            user_id = None

        try:
            update_id = getattr(update, "update_id", None)
            msg_id = getattr(getattr(update, "message", None), "message_id", None)
            has_video = bool(getattr(getattr(update, "message", None), "video", None))
            has_document = bool(getattr(getattr(update, "message", None), "document", None))
            has_audio = bool(getattr(getattr(update, "message", None), "audio", None))
            text_preview = (getattr(getattr(update, "message", None), "text", None) or "")[:200]

            fmt = (
                "Incoming message update: user_id=%s update_id=%s msg_id=%s "
                "has_video=%s has_document=%s has_audio=%s text=%s"
            )
            logger.info(
                fmt,
                user_id,
                update_id,
                msg_id,
                has_video,
                has_document,
                has_audio,
                text_preview,
            )
        except Exception:
            logger.exception("Failed to log incoming message update")
        # Enforce access control for private bots
        try:
            if not is_user_allowed(user_id):
                await update.message.reply_text("Access denied. This bot is private.")
                return
        except Exception:
            # If ACL check fails for any reason, default to deny-safe
            with contextlib.suppress(BadRequest):
                await update.message.reply_text("Access denied. (ACL check failed)")
            return

        # Initialize user session
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = {
                "files": {},
                "current_file": None,
                "merge_list": [],
                "bulk_list": [],
                "processing": False,
            }

        session = self.user_sessions[user_id]

        # Schedule session cleanup (resets timer on each interaction)
        self._schedule_session_cleanup(user_id)

        # Respect Telegram API rate limits if a limiter is provided in bot_data
        try:
            api_limiter = None
            if context and getattr(context, "bot_data", None) is not None:
                api_limiter = context.bot_data.get("api_rate_limiter")
            if api_limiter and user_id is not None:
                try:
                    await api_limiter.wait_if_needed(str(user_id))
                except Exception:
                    # If rate limiter fails, continue but log
                    logger.debug("API rate limiter wait failed or was skipped")
        except Exception:
            logger.debug("handlers: Respect Telegram API rate limits if a limiter is provided in bot_data")

        # Clear awaiting_custom_resolution if user sends non-text
        if getattr(context, "user_data", {}).get("awaiting_custom_resolution") and not getattr(
            update.message, "text", None
        ):
            context.user_data.pop("awaiting_custom_resolution", None)
            await update.message.reply_text(
                "❌ Cancelled custom resolution. Send text like 1280x720 or send a file.",
                reply_markup=MediaMenuBuilder.get_back_button(),
            )
            return

        # If user is in settings flow and sends a photo, treat as thumbnail upload
        try:
            if getattr(context, "user_data", {}).get("awaiting_settings") and getattr(update.message, "photo", None):
                photos = update.message.photo
                if photos:
                    # choose largest
                    file_obj = photos[-1]
                    await update.message.reply_text("📥 Downloading thumbnail photo and saving as default...")
                    file = await context.bot.get_file(file_obj.file_id)
                    thumb_dir = config.THUMBNAIL_PATH if hasattr(config, "THUMBNAIL_PATH") else "storage/thumbnails"
                    with contextlib.suppress(OSError):
                        os.makedirs(thumb_dir, exist_ok=True)
                    thumb_path = os.path.join(thumb_dir, f"{user_id}_{file_obj.file_id}.jpg")
                    await file.download_to_drive(thumb_path)
                    if user_settings:
                        user_settings.set_user_setting(user_id, "default_thumbnail", thumb_path)
                        user_settings.set_user_setting(user_id, "save_thumbnail", True)
                    await update.message.reply_text("✅ Default thumbnail saved.")
                    # clear awaiting flag
                    for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
                    return
        except Exception:
            logger.exception("Failed to handle thumbnail photo upload")

        # If user is providing mp3 tag JSON while in mp3 tag editor flow
        try:
            if getattr(context, "user_data", {}).get("awaiting_mp3_tags") and getattr(update.message, "text", None):
                text = update.message.text.strip()
                try:
                    import json as _json

                    tags = _json.loads(text)
                except Exception:
                    await update.message.reply_text("❌ Invalid JSON. Send a JSON object with tag keys and values.")
                    return

                # Apply tags to current file if available
                session = self.user_sessions.get(update.effective_user.id, {})
                current_file = session.get("current_file") if session else None
                if not current_file or current_file.get("type") != "audio":
                    await update.message.reply_text("❌ No audio file selected to apply tags.")
                    # clear awaiting flag
                    context.user_data.pop("awaiting_mp3_tags", None)
                    return

                input_path = current_file.get("path")
                output_path = input_path + ".tagged" + os.path.splitext(input_path)[1]
                try:
                    ok = await self.converter.edit_metadata(input_path, output_path, tags)
                    if ok and os.path.exists(output_path):
                        # replace current file path
                        current_file["path"] = output_path
                        current_file["_source_metadata"] = dict(tags)
                        # Deliver the tagged file right away as streamable audio,
                        # otherwise the user never sees the result of this action.
                        delivery_name = _audio_delivery_name(
                            current_file.get("name"),
                            current_file.get("id"),
                            extension=os.path.splitext(output_path)[1] or ".mp3",
                        )
                        with open(output_path, "rb") as audio_file:
                            await update.message.reply_audio(
                                audio=audio_file,
                                caption=_metadata_caption(current_file),
                                title=tags.get("title") or os.path.splitext(delivery_name)[0],
                                performer=tags.get("artist") or tags.get("performer") or "",
                                filename=delivery_name,
                            )
                    else:
                        await update.message.reply_text("❌ Failed to apply tags.")
                except Exception:
                    logger.exception("Failed to apply mp3 tags")
                    await update.message.reply_text("❌ Error while applying tags.")

                # clear awaiting flag
                context.user_data.pop("awaiting_mp3_tags", None)
                return
        except Exception:
            logger.exception("Failed to handle awaiting_mp3_tags message")

        # If message contains a photo (normal incoming photo, not settings thumbnail),
        # save it to storage, queue it in the bulk batch, and keep it in the merge
        # list so multiple pasted photos are collected automatically.
        try:
            if getattr(update.message, "photo", None) and not getattr(context, "user_data", {}).get(
                "awaiting_settings"
            ):
                photos = update.message.photo
                if photos:
                    # choose largest size variant
                    file_obj = photos[-1]
                    input_dir = getattr(config, "INPUT_PATH", "storage/input")
                    with contextlib.suppress(OSError):
                        os.makedirs(input_dir, exist_ok=True)
                    photo_path = os.path.join(input_dir, f"{user_id}_{file_obj.file_id}.jpg")
                    _photo_uid = getattr(file_obj, "file_unique_id", None)
                    _photo_size = getattr(file_obj, "file_size", None)

                    # Reuse a photo that already entered the pipe (same id AND
                    # size) instead of fetching it from Telegram again.
                    _photo_reused = False
                    try:
                        from utils import media_cache as _media_cache

                        if _photo_uid and _media_cache.cache_enabled():
                            _entry = await _media_cache.lookup(_photo_uid, expected_size=_photo_size)
                            _stored_path = (_entry or {}).get("path")
                            if _stored_path and os.path.exists(_stored_path):
                                photo_path = _stored_path
                                _photo_reused = True
                            else:
                                _cached = await _media_cache.get_bytes(_photo_uid, expected_size=_photo_size)
                                if _cached:
                                    with open(photo_path, "wb") as _fh:
                                        _fh.write(_cached)
                                    _photo_reused = True
                    except Exception:
                        logger.debug("handlers: photo media-cache lookup failed")

                    if not _photo_reused:
                        file = await context.bot.get_file(file_obj.file_id)
                        await file.download_to_drive(photo_path)
                        try:
                            from utils import media_cache as _media_cache

                            if _photo_uid:
                                _download_size = os.path.getsize(photo_path)
                                _payload = None
                                if _download_size <= _media_cache.bytes_cache_limit():
                                    with open(photo_path, "rb") as _fh:
                                        _payload = _fh.read()
                                await _media_cache.remember(
                                    _photo_uid,
                                    size=_download_size,
                                    path=photo_path,
                                    name=os.path.basename(photo_path),
                                    storage="local",
                                    data=_payload,
                                    file_id=getattr(file_obj, "file_id", None),
                                )
                        except Exception:
                            logger.debug("handlers: failed to remember photo in cache")

                    # Every photo also joins the bulk batch — with videos and
                    # audio — so a single "Apply Bulk" covers everything sent.
                    # The bytes are already on disk, so Apply reuses this path
                    # instead of downloading the photo again.
                    _photo_entry = {
                        "id": getattr(file_obj, "file_id", None) or photo_path,
                        "file_unique_id": getattr(file_obj, "file_unique_id", None),
                        "name": os.path.basename(photo_path),
                        "path": photo_path,
                        "type": "photo",
                        "size": getattr(file_obj, "file_size", None),
                    }
                    _register_bulk_file(session, _photo_entry)

                    # If part of an album (media_group_id), collect into temporary group
                    mgid = getattr(update.message, "media_group_id", None)
                    if mgid:
                        groups = session.setdefault("media_groups", {})
                        lst = groups.setdefault(mgid, [])
                        lst.append({"path": photo_path, "type": "photo"})
                        # schedule a finalize in 1s if not scheduled
                        timers = session.setdefault("media_group_timers", {})
                        if mgid not in timers:
                            try:
                                loop = asyncio.get_running_loop()
                                handle = loop.call_later(
                                    1.0, lambda: asyncio.create_task(self._finalize_media_group(user_id, mgid))
                                )
                                timers[mgid] = handle
                            except Exception:
                                # best-effort: finalize immediately
                                await self._finalize_media_group(user_id, mgid)
                        # One announcement for the whole album, not one per photo.
                        await self._buffer_album_item(update, context, session, user_id, "photo")
                        return

                    # Non-album single photo: keep it for the merger too, but the
                    # batch is what “Apply Bulk” reads.
                    if "merge_list" not in session:
                        session["merge_list"] = []
                    session["merge_list"].append({"path": photo_path, "type": "photo"})
                    try:
                        self._persist_session(user_id)
                    except Exception:
                        logger.debug("Could not persist session after photo download")
                    await update.message.reply_text(
                        f"✅ Photo queued. Batch size: {len(session.get('bulk_list') or [])}\n"
                        "Open /bulkmenu and press ▶️ Apply Bulk when you are ready."
                    )
                    return
        except Exception:
            logger.exception("Failed to auto-handle incoming photo")

        # Check if message has video
        if update.message.video:
            await self.handle_video(update, context, session)
        elif update.message.document:
            await self.handle_document(update, context, session)
        elif update.message.audio:
            await self.handle_audio(update, context, session)
        else:
            # Detect URLs in text and support bulk URL conversion
            text = getattr(update.message, "text", "") or ""
            import re

            urls = re.findall(r"(https?://\S+)", text)
            if urls:
                queued = 0
                for url in urls:
                    job_id = str(uuid.uuid4())
                    output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
                    with contextlib.suppress(OSError):
                        os.makedirs(output_dir, exist_ok=True)
                    output_path = os.path.join(output_dir, f"{job_id}.mp4")
                    job = {
                        "job_id": job_id,
                        "source_url": url,
                        "output_path": output_path,
                        # Keeps the delivered filename derived from the URL instead of the job id.
                        "original_filename": filename_from_url(url),
                        "ffmpeg_args": [
                            "-c:v",
                            "libx264",
                            "-preset",
                            "veryfast",
                            "-crf",
                            "23",
                            "-c:a",
                            "aac",
                            "-b:a",
                            "128k",
                            "-maxrate",
                            "2M",
                            "-bufsize",
                            "4M",
                        ],
                        "progress_channel": f"ffmpeg:progress:{job_id}",
                        "chat_id": update.effective_chat.id if update and update.effective_chat else None,
                        "caption": f"✅ Converted from URL: {url}",
                        "cleanup_input": True,
                        "cleanup_output": False,
                    }
                    try:
                        try:
                            job["request_id"] = getattr(update, "request_id", None)
                        except Exception:
                            job["request_id"] = None
                        await enqueue_job(job)
                        queued += 1
                    except Exception:
                        logger.exception("Failed to enqueue URL job: %s", url)
                await update.message.reply_text(f"✅ Queued {queued} URL job(s).")
                return

            await update.message.reply_text(
                "Please send a video, audio, or document file. You can also paste one or more URLs (http/https) to enqueue conversions."
            )

    async def handle_video(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Handle incoming video files."""
        video = update.message.video
        user_id = update.effective_user.id

        # Check file size (configurable)
        try:
            max_size = int(MAX_FILE_SIZE)
        except Exception:
            max_size = 4 * 1024**3
        if video.file_size > max_size:
            await update.message.reply_text("❌ File too large (max 4GB).\nFor larger files, use the /upload command.")
            return

        # Register file lazily (do not download yet). We'll download on-demand
        # Preserve the filename Telegram gives us (videos sent as files carry
        # ``video.file_name``). Only fall back to a generated name when the
        # client did not send one, otherwise converted audio/video would be
        # delivered under an opaque ``{user_id}_{file_id}`` name.
        file_id = video.file_id
        ext = ".mp4"
        _original_name = (getattr(video, "file_name", None) or "").strip()
        if _original_name:
            _orig_ext = os.path.splitext(_original_name)[1].lower()
            if _orig_ext in self.converter.supported_formats["video"]:
                ext = _orig_ext
            default_name = _original_name if _orig_ext else f"{_original_name}{ext}"
        else:
            # Telegram did not send a filename (common for gallery videos), so
            # use a readable timestamp instead of the old "<user>_<file_id>" blob.
            # UTC, so the name does not shift with the host's zone.
            default_name = f"video_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}{ext}"
        final_name = default_name
        thumb = None
        try:
            if user_settings:
                s = user_settings.get_user_settings(user_id)
                prefix = s.get("prefix") or ""
                suffix = s.get("suffix") or ""
                final_name = f"{prefix}{final_name}{suffix}"
                if s.get("save_thumbnail") and s.get("default_thumbnail"):
                    thumb = s.get("default_thumbnail")
        except Exception:
            logger.exception("Failed to apply user settings to video name")

        # Capture forward metadata when available (useful for userbot fallback)
        forward_info = None
        try:
            fch = getattr(update.message, "forward_from_chat", None)
            f_msg_id = getattr(update.message, "forward_from_message_id", None)
            if fch or f_msg_id:
                tmp = {}
                if fch:
                    tmp["chat_id"] = getattr(fch, "id", None) or getattr(fch, "username", None)
                if f_msg_id:
                    tmp["message_id"] = f_msg_id
                forward_info = tmp
        except Exception:
            forward_info = None

        # capture message date and file unique id for better userbot fallback
        msg_date = None
        try:
            if getattr(update, "message", None) and getattr(update.message, "date", None):
                msg_date = update.message.date.isoformat()
        except Exception:
            msg_date = None

        file_unique_id = getattr(video, "file_unique_id", None)

        session["current_file"] = {
            "path": None,
            "type": "video",
            "id": file_id,
            "size": video.file_size,
            "name": final_name,
            "thumbnail": thumb,
            "forward": forward_info,
            "chat_id": getattr(update.message, "chat", None) and getattr(update.message.chat, "id", None),
            "msg_id": getattr(update.message, "message_id", None),
            "msg_date": msg_date,
            "file_unique_id": file_unique_id,
        }

        logger.info(
            "registered current_file for user %s id=%s forward=%s size=%s",
            user_id,
            file_id,
            forward_info,
            video.file_size,
        )
        # Collect every sent file so "Apply Bulk" can run on the whole batch.
        _register_bulk_file(session, session["current_file"])
        try:
            self._persist_session(user_id)
        except Exception:
            logger.debug("Could not persist session after registering video")

        # Log to MongoDB if needed
        await self.log_media_to_db(user_id, session["current_file"])

        # An album arrives as one update per video, so show a menu only for a
        # standalone send; an album is collected and announced as a batch.
        if await self._buffer_album_item(update, context, session, user_id, "video"):
            return

        # ── Show action menu — let user choose what to do with the video ──
        await update.message.reply_text(
            f"✅ Video registered!\n📦 Size: {video.file_size // 1024 // 1024} MB\nChoose an action:",
            reply_markup=MediaMenuBuilder.get_main_menu("video"),
        )

    async def handle_audio(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Handle incoming audio files."""
        audio = update.message.audio
        user_id = update.effective_user.id

        ext = ".mp3"
        if audio.mime_type:
            ext_map = {
                "audio/mpeg": ".mp3",
                "audio/wav": ".wav",
                "audio/x-wav": ".wav",
                "audio/aac": ".aac",
                "audio/flac": ".flac",
                "audio/ogg": ".ogg",
            }
            ext = ext_map.get(audio.mime_type, ".mp3")

        # Prefer the sender's own filename, then the audio title, and only then a
        # generated name — so the delivered audio keeps the original name.
        _audio_original = (getattr(audio, "file_name", None) or "").strip()
        if _audio_original:
            default_name = _audio_original
        elif audio.title:
            default_name = audio.title
        else:
            # UTC, so the name does not shift with the host's zone.
            default_name = f"audio_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}{ext}"
        final_name = default_name
        thumb = None
        try:
            if user_settings:
                s = user_settings.get_user_settings(user_id)
                for w in s.get("words_remove") or []:
                    final_name = final_name.replace(w, "")
                final_name = final_name.strip()
                if not os.path.splitext(final_name)[1]:
                    final_name += ext
                final_name = f"{s.get('prefix') or ''}{final_name}{s.get('suffix') or ''}"
                if s.get("save_thumbnail") and s.get("default_thumbnail"):
                    thumb = s.get("default_thumbnail")
        except Exception:
            logger.exception("Failed to apply user settings to audio name")

        # Capture forward metadata when available (useful for userbot fallback)
        forward_info = None
        try:
            fch = getattr(update.message, "forward_from_chat", None)
            f_msg_id = getattr(update.message, "forward_from_message_id", None)
            if fch or f_msg_id:
                tmp = {}
                if fch:
                    tmp["chat_id"] = getattr(fch, "id", None) or getattr(fch, "username", None)
                if f_msg_id:
                    tmp["message_id"] = f_msg_id
                forward_info = tmp
        except Exception:
            forward_info = None

        msg_date = None
        try:
            if getattr(update, "message", None) and getattr(update.message, "date", None):
                msg_date = update.message.date.isoformat()
        except Exception:
            msg_date = None

        file_unique_id = getattr(audio, "file_unique_id", None)

        session["current_file"] = {
            "path": None,
            "type": "audio",
            "id": audio.file_id,
            "size": audio.file_size,
            "name": final_name,
            "thumbnail": thumb,
            "forward": forward_info,
            "chat_id": getattr(update.message, "chat", None) and getattr(update.message.chat, "id", None),
            "msg_id": getattr(update.message, "message_id", None),
            "msg_date": msg_date,
            "file_unique_id": file_unique_id,
        }

        # Collect every sent file so "Apply Bulk" can run on the whole batch.
        _register_bulk_file(session, session["current_file"])

        logger.info(
            "registered current_file for user %s id=%s forward=%s size=%s",
            user_id,
            audio.file_id,
            forward_info,
            audio.file_size,
        )

        # An album arrives as one update per track: collect it instead of
        # showing a menu per track.
        if await self._buffer_album_item(update, context, session, user_id, "audio"):
            return

        # ── Show action menu — let user choose what to do with the audio ──
        await update.message.reply_text(
            f"✅ Audio registered!\n🎵 {audio.title or 'Unknown title'}\nChoose an action:",
            reply_markup=MediaMenuBuilder.get_main_menu("audio"),
        )

    async def handle_document(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Handle document files (could be video/audio)."""
        document = update.message.document
        user_id = update.effective_user.id

        # Check file extension
        file_name = document.file_name or f"file_{document.file_id}"
        file_ext = os.path.splitext(file_name)[1].lower()

        # If the user was asked to send a subtitle file, handle specially
        awaiting_sub = context.user_data.pop("awaiting_subtitle_file", False)
        awaiting_burn = context.user_data.pop("awaiting_burn_subtitle", False)

        subtitle_exts = {".srt", ".ass", ".vtt"}
        if (awaiting_sub or awaiting_burn) and file_ext in subtitle_exts:
            await update.message.reply_text("📥 Downloading subtitle file...")
            file = await context.bot.get_file(document.file_id)
            input_dir = getattr(config, "INPUT_PATH", "storage/input") if config else "storage/input"
            with contextlib.suppress(OSError):
                os.makedirs(input_dir, exist_ok=True)
            subtitle_path = os.path.join(input_dir, f"{user_id}_{document.file_id}{file_ext}")
            await file.download_to_drive(subtitle_path)

            # Ensure we have a current video for burning/adding
            current = session.get("current_file")
            if not current or current.get("type") != "video":
                await update.message.reply_text("❌ No video available in session to apply subtitles.")
                return

            video_path = current["path"]
            output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
            with contextlib.suppress(OSError):
                os.makedirs(output_dir, exist_ok=True)
            out_path = os.path.join(output_dir, f"{user_id}_subtitled_{os.path.basename(video_path)}")

            if awaiting_burn:
                await update.message.reply_text("🔧 Burning subtitles into video (this may take a while)...")
                ok = await self.converter.burn_subtitles(video_path, subtitle_path, out_path)
            else:
                await update.message.reply_text("🔧 Adding subtitles as a separate stream (soft subtitles)...")
                ok = await self.converter.add_subtitles(video_path, subtitle_path, out_path)

            if ok and os.path.exists(out_path):
                await update.message.reply_text("✅ Subtitles applied. Sending file...")
                try:
                    with open(out_path, "rb") as doc_file:
                        await context.bot.send_document(chat_id=update.effective_chat.id, document=doc_file)
                except Exception:
                    await update.message.reply_text("⚠️ Failed to send file; try downloading from the server.")
            else:
                await update.message.reply_text("❌ Failed to apply subtitles. See logs for details.")

            return

        # Determine file type
        if file_ext in self.converter.supported_formats["video"]:
            file_type = "video"
        elif file_ext in self.converter.supported_formats["audio"]:
            file_type = "audio"
        else:
            await update.message.reply_text(
                f"❌ Unsupported file format: {file_ext}\n"
                f"Supported formats:\n"
                f"Video: {', '.join(self.converter.supported_formats['video'][:5])}\n"
                f"Audio: {', '.join(self.converter.supported_formats['audio'][:5])}"
            )
            return

        # Check file size (if provided) to avoid calling get_file on huge files
        try:
            max_size = int(MAX_FILE_SIZE)
        except Exception:
            max_size = 4 * 1024**3

        doc_size = getattr(document, "file_size", None)
        if doc_size and doc_size > max_size:
            await update.message.reply_text(
                f"❌ File too large ({doc_size // 1024 // 1024} MB). "
                f"Maximum allowed is {max_size // 1024 // 1024} MB.\n"
                "For large files please provide a direct download URL or use the web upload endpoint."
            )
            return

        # For subtitle flows we still need to download immediately (handled above).
        # Otherwise register the document lazily and show the menu.
        final_name = file_name
        thumb = None
        try:
            if user_settings:
                s = user_settings.get_user_settings(user_id)
                for w in s.get("words_remove") or []:
                    final_name = final_name.replace(w, "")
                final_name = final_name.strip()
                if not os.path.splitext(final_name)[1] and file_ext:
                    final_name += file_ext
                final_name = f"{s.get('prefix') or ''}{final_name}{s.get('suffix') or ''}"
                if s.get("save_thumbnail") and s.get("default_thumbnail"):
                    thumb = s.get("default_thumbnail")
        except Exception:
            logger.exception("Failed to apply user settings to document name")

        # Capture forward metadata when available (useful for userbot fallback)
        forward_info = None
        try:
            fch = getattr(update.message, "forward_from_chat", None)
            f_msg_id = getattr(update.message, "forward_from_message_id", None)
            if fch or f_msg_id:
                tmp = {}
                if fch:
                    tmp["chat_id"] = getattr(fch, "id", None) or getattr(fch, "username", None)
                if f_msg_id:
                    tmp["message_id"] = f_msg_id
                forward_info = tmp
        except Exception:
            forward_info = None

        msg_date = None
        try:
            if getattr(update, "message", None) and getattr(update.message, "date", None):
                msg_date = update.message.date.isoformat()
        except Exception:
            msg_date = None

        # document may be a photo, file or other; attempt to extract unique id
        file_unique_id = None
        try:
            if getattr(document, "file_unique_id", None):
                file_unique_id = document.file_unique_id
            else:
                # photos stored in message.photo list
                photos = getattr(update.message, "photo", None)
                if photos:
                    file_unique_id = getattr(photos[-1], "file_unique_id", None)
        except Exception:
            file_unique_id = None

        session["current_file"] = {
            "path": None,
            "type": file_type,
            "id": document.file_id,
            "size": document.file_size,
            "name": final_name,
            "thumbnail": thumb,
            "forward": forward_info,
            "chat_id": getattr(update.message, "chat", None) and getattr(update.message.chat, "id", None),
            "msg_id": getattr(update.message, "message_id", None),
            "msg_date": msg_date,
            "file_unique_id": file_unique_id,
        }

        # Collect every sent file so "Apply Bulk" can run on the whole batch.
        # A photo sent uncompressed arrives as a document; queue it as a photo so
        # it joins the slideshow instead of being handled as a generic file.
        if file_ext in _IMAGE_EXTS:
            _register_bulk_file(session, {**session["current_file"], "type": "photo"})
        else:
            _register_bulk_file(session, session["current_file"])
        try:
            self._persist_session(user_id)
        except Exception:
            logger.debug("Could not persist session after registering document")

        logger.info(
            "registered current_file for user %s id=%s forward=%s size=%s",
            user_id,
            document.file_id,
            forward_info,
            document.file_size,
        )

        # An album arrives as one update per file: collect it instead of showing
        # a menu per file.
        if await self._buffer_album_item(update, context, session, user_id, file_type):
            return

        # ── Show action menu — let user choose what to do with the file ──
        await update.message.reply_text(
            f"✅ {file_type.capitalize()} registered!\n📁 {file_name}\nChoose an action:",
            reply_markup=MediaMenuBuilder.get_main_menu(file_type),
        )

    async def _apply_fade(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: dict,
        fade_in: float = 0.0,
        fade_out: float = 0.0,
    ):
        """Apply audio fade-in and/or fade-out to the current file."""
        query = getattr(update, "callback_query", None)
        if query is None:
            return

        current_file = session.get("current_file") if session else None
        if not current_file:
            await self.safe_edit(
                query,
                "❌ No file registered. Send a media file first.",
                reply_markup=MediaMenuBuilder.get_back_button(),
            )
            return

        if not await self._check_conversion_quota(update, context):
            return

        await self.safe_edit(query, "📈 Applying fade effect...", reply_markup=MediaMenuBuilder.get_back_button())

        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        input_path = current_file.get("path")
        if not input_path or not os.path.exists(input_path):
            await self.safe_edit(query, "❌ File not found on disk.", reply_markup=MediaMenuBuilder.get_back_button())
            return
        ext = os.path.splitext(input_path)[1] or ".mp3"
        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{current_file.get('id', 'unknown')}_faded{ext}")

        success = await self.converter.apply_fade(input_path, output_path, fade_in, fade_out)
        if success and os.path.exists(output_path):
            current_file["path"] = output_path
            await self.safe_edit(
                query, "✅ Fade applied! Sending file...", reply_markup=MediaMenuBuilder.get_back_button()
            )
            try:
                if current_file.get("type") == "audio":
                    # Deliver faded audio as streamable audio, not as a document.
                    delivery_name = _audio_delivery_name(
                        current_file.get("name"), current_file.get("id"), extension=ext or ".mp3"
                    )
                    with open(output_path, "rb") as audio_file:
                        await context.bot.send_audio(
                            chat_id=update.effective_chat.id,
                            audio=audio_file,
                            caption=_metadata_caption(current_file),
                            title=os.path.splitext(delivery_name)[0],
                            filename=delivery_name,
                            performer="",
                        )
                else:
                    with open(output_path, "rb") as f:
                        await context.bot.send_document(chat_id=update.effective_chat.id, document=f)
            except Exception:
                await self.safe_edit(
                    query,
                    "✅ Fade applied but failed to send. Check the output folder.",
                    reply_markup=MediaMenuBuilder.get_back_button(),
                )
        else:
            await self.safe_edit(
                query, "❌ Failed to apply fade effect.", reply_markup=MediaMenuBuilder.get_back_button()
            )

    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all callback queries with enhanced features."""
        query = update.callback_query
        logger.info(
            "Callback received: user=%s data=%s message_id=%s",
            getattr(update.effective_user, "id", None),
            getattr(query, "data", None),
            getattr(getattr(query, "message", None), "message_id", None),
        )
        # Defensive: ensure we have a callback_query
        if query is None:
            logger.warning("callback_handler called without callback_query")
            # Log and persist this event
            await self._log_bad_callback(
                "missing_query",
                None,
                getattr(update.effective_user, "id", None),
                getattr(update.effective_chat, "id", None),
                None,
            )
            return
        await query.answer()

        user_id = update.effective_user.id
        data = query.data

        # Validate callback payload
        if not isinstance(data, str):
            with contextlib.suppress(BadRequest):
                await query.answer()
            await self.safe_edit(query, "⚠️ Invalid button payload.")
            logger.warning(f"Invalid callback data type: {type(data)} data={data}")
            # Persist bad callback event
            await self._log_bad_callback(
                "invalid_payload",
                data,
                user_id,
                getattr(update.effective_chat, "id", None),
                getattr(getattr(query, "message", None), "message_id", None),
            )
            return
        # Accept older/alternate callback names from `MediaMenuBuilder` by mapping
        # them to the canonical names expected by this handler. This keeps
        # `utils/keyboard_utils.py` unchanged while ensuring callbacks are handled.
        aliases = {
            # Main/menu aliases
            "video_tools": "menu_video",
            "back_to_main": "menu_main",
            "media_info": "info",
            "send_file": "menu_main",
            "help": "menu_main",
            "quick_start": "menu_main",
            # Conversion / format aliases
            "convert_audio": "convert_format_menu",
            "convert_video": "convert_format_menu",
            "audio_mp3": "format_mp3",
            "audio_wav": "format_wav",
            "audio_aac": "format_aac",
            "audio_flac": "format_flac",
            "audio_ogg": "format_ogg",
            "audio_m4a": "format_m4a",
            # Merge aliases
            "merge_audio": "merge_audios_menu",
            "merge_start": "merge_videos_start",
            # individual merge menu actions are handled via UI flow; map sensible
            "merge_add": "merge_add",
            "merge_view": "merge_view",
            "merge_clear": "merge_clear",
            # Resolution presets: map explicit WxH to handler-friendly keys
            "res_3840_2160": "res_4k",
            "res_1920_1080": "res_1080",
            "res_1280_720": "res_720",
            "res_854_480": "res_480",
            "res_640_360": "res_360",
            # Screenshot menu differences
            "screenshot_grid_3": "screenshot_9grid",
            "screenshot_grid_4": "screenshot_multiple",
            # Extraction aliases (see MediaMenuBuilder.get_extraction_menu)
            "extract_audio_only": "extract_audio",
            "extract_video_only": "extract_video",
            "extract_all": "extract_all_streams",
            # Misc small mappings
            "add_audio": "merge_av_menu",
            # UI-friendly names mapping to canonical handler keys
            "thumbnail_grid": "thumbnail_grid",
            "thumbnail_extractor": "thumbnail_grid",
            "caption_editor": "caption_editor",
            "media_forwarder": "media_forwarder",
            "stream_remover": "remove_audio",
            "stream_extractor": "extract_streams",
            "video_splitter": "video_splitter",
            "manual_shots": "screenshot_custom",
            "video_to_audio": "convert_mp3",
            "subtitle_merger": "add_subtitles",
            "video_renamer": "video_renamer",
            "video_converter": "convert_format_menu",
            # Fade aliases
            "fade_in": "fade_in",
            "fade_out": "fade_out",
            "fade_both": "fade_both",
        }

        # Remap data if an alias exists
        data = aliases.get(data, data)

        # Wrap handler dispatch in try/except to catch unexpected errors
        # Import canonical callback names for comparison when needed
        # canonical callback names (if ever needed) are provided by `utils.callbacks`.
        # We don't import them here to avoid unused-name noise from linters.
        try:
            # Map video bitrate shortcuts to generic bitrate handler
            if isinstance(data, str) and data.startswith("vbitrate_"):
                data = "bitrate_" + data.split("_", 1)[1]

            # Ensure session exists
            if user_id not in self.user_sessions:
                # Try to load persisted session (useful when running multiple workers)
                persisted = self._load_persisted_session(user_id)
                if persisted:
                    # If the persisted apply guard is older than the guard window,
                    # treat it as expired and clear it to avoid a stale lock.
                    _persisted_guard = persisted.get("_bulk_apply_started_at")
                    if _persisted_guard:
                        try:
                            _guard_age = time.time() - float(_persisted_guard)
                            if _guard_age >= _BULK_APPLY_GUARD_SECONDS:
                                _persisted_guard = None
                        except (TypeError, ValueError):
                            _persisted_guard = None
                    self.user_sessions[user_id] = {
                        "files": {},
                        "current_file": persisted.get("current_file"),
                        "merge_list": persisted.get("merge_list", []),
                        "bulk_list": persisted.get("bulk_list", []),
                        "_bulk_apply_started_at": _persisted_guard,
                    }
                else:
                    self.user_sessions[user_id] = {"files": {}, "current_file": None}

            session = self.user_sessions[user_id]
            current_file = session.get("current_file")

            # Main menu navigation
            if data == "menu_main":
                await self.safe_edit(
                    query,
                    "🎬 **Media Conversion Bot**\nSelect a category:",
                    reply_markup=MediaMenuBuilder.get_main_menu(current_file["type"] if current_file else None),
                )

            elif data == "menu_video":
                await self.safe_edit(
                    query,
                    "🎬 **Video Tools**\nChoose an action:",
                    reply_markup=MediaMenuBuilder.get_video_tools_menu(),
                )

            elif data == "menu_audio":
                await self.safe_edit(
                    query,
                    "🎧 **Audio Tools**\nChoose an action:",
                    reply_markup=MediaMenuBuilder.get_audio_tools_menu(),
                )

            elif data == "menu_advanced":
                await self.safe_edit(
                    query,
                    "🔧 **Advanced Tools**\nChoose an action:",
                    reply_markup=MediaMenuBuilder.get_advanced_tools_menu(),
                )

            # Video tools
            elif data == "convert_mp3":
                # Let the user pick the MP3 quality before extracting audio.
                await self.show_mp3_quality_menu(update, context, session)

            elif isinstance(data, str) and data.startswith("mp3q_"):
                quality = data.split("_", 1)[1]
                if quality == "custom":
                    await self.safe_edit(query, "✏️ Send the MP3 bitrate (32k-320k, e.g. 128k):")
                    for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
                    context.user_data["awaiting_mp3_bitrate"] = True
                    return
                await self.convert_to_mp3(update, context, session, bitrate=quality)

            elif data == "compress_menu":
                await self.safe_edit(
                    query,
                    "📉 **Compression Options**\nSelect quality preset:",
                    reply_markup=MediaMenuBuilder.get_compression_menu(),
                )

            elif isinstance(data, str) and data.startswith("compress_"):
                crf = data.split("_")[1]
                await self.compress_video(update, context, session, crf)

            elif data == "trim_video":
                # Open trimmer selection menu with two dynamic modes
                await self.safe_edit(
                    query,
                    "✂️ **Video Trimming**\nChoose a trimmer mode:",
                    reply_markup=MediaMenuBuilder.get_trimmer_menu(),
                )

            elif data == "trimmer_1":
                # Trimmer 1: ask for start then end time
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_trimmer"] = "trimmer1_start"
                await self.safe_edit(
                    query,
                    "✂️ Trimmer 1 selected.\nSend START time (HH:MM:SS[.ms])\nExample: 00:01:00",
                )
                return

            elif data == "trimmer_2":
                # Trimmer 2: ask for start then duration
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_trimmer"] = "trimmer2_start"
                await self.safe_edit(
                    query,
                    "✂️ Trimmer 2 selected.\nSend START time (HH:MM:SS[.ms])\nExample: 00:05:00",
                )
                return

            elif data == "merge_videos_menu":
                await self.safe_edit(
                    query,
                    "🔀 **Merge Videos**\nSend multiple video files, then click 'Start Merge':",
                    reply_markup=MediaMenuBuilder.get_merge_menu("video"),
                )

            elif data == "merge_videos_start":
                await self.merge_videos(update, context, session)

            elif data == "remove_audio":
                await self.remove_audio(update, context, session)

            elif data == "merge_av_menu":
                await self.safe_edit(
                    query,
                    "🎵 **Merge Audio with Video**\nFirst send the audio file, then select this option again.",
                )

            elif data == "resolution_menu":
                await self.safe_edit(
                    query,
                    "📐 **Change Resolution**\nSelect preset:",
                    reply_markup=MediaMenuBuilder.get_resolution_menu(),
                )

            elif data == "res_custom":
                if not current_file:
                    await self.safe_edit(query, "❌ No file registered. Send a file first.")
                else:
                    await self.safe_edit(
                        query,
                        "📐 Enter custom resolution (widthxheight, e.g. 1280x720):",
                    )
                    context.user_data["awaiting_custom_resolution"] = True

            elif isinstance(data, str) and data.startswith("res_"):
                resolution = data.split("_")[1]
                await self.change_resolution(update, context, session, resolution)

            elif data == "optimize_menu":
                await self.safe_edit(
                    query,
                    "⚡ **Optimize Video**\nSelect optimization preset:",
                    reply_markup=MediaMenuBuilder.get_optimize_menu(),
                )

            elif data == "optimize_custom":
                if not current_file:
                    await self.safe_edit(query, "\u274c No file registered. Send a file first.")
                else:
                    await self.safe_edit(
                        query, "\u2699\ufe0f Custom optimization: compressing with CRF 23, preset medium, faststart..."
                    )
                    if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
                        try:
                            await self._ensure_current_file_downloaded(update, context, session)
                            current_file = session.get("current_file")
                        except Exception as e:
                            await self.safe_edit(query, f"\u274c Failed to download file: {e}")
                            return
                    input_path = current_file["path"]
                    ext = os.path.splitext(input_path)[1] or ".mp4"
                    output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
                    with contextlib.suppress(OSError):
                        os.makedirs(output_dir, exist_ok=True)
                    output_path = os.path.join(output_dir, f"{current_file.get('id', 'unknown')}_optimized{ext}")
                    success = await self.converter.optimize_video(input_path, output_path, preset="medium", crf=23)
                    if success and os.path.exists(output_path):
                        current_file["path"] = output_path
                        await self.safe_edit(
                            query,
                            "\u2705 Custom optimization complete!",
                            reply_markup=MediaMenuBuilder.get_back_button(),
                        )
                        try:
                            with open(output_path, "rb") as f:
                                await context.bot.send_document(chat_id=update.effective_chat.id, document=f)
                        except Exception:
                            await self.safe_edit(
                                query,
                                "\u2705 Optimized but failed to send.",
                                reply_markup=MediaMenuBuilder.get_back_button(),
                            )
                    else:
                        await self.safe_edit(
                            query, "\u274c Failed to optimize.", reply_markup=MediaMenuBuilder.get_back_button()
                        )
            elif isinstance(data, str) and data.startswith("optimize_"):
                preset = data.split("_")[1]
                await self.optimize_video(update, context, session, preset)

            elif data == "repair_video":
                await self.repair_video(update, context, session)

            elif data == "screenshots_menu":
                await self.safe_edit(
                    query,
                    "🖼️ **Screenshot Options**\nChoose an option:",
                    reply_markup=MediaMenuBuilder.get_screenshots_menu(),
                )

            elif data == "screenshot_start":
                await self._quick_screenshot(update, context, session, time_str="00:00:00.500")

            elif data == "screenshot_middle":
                # Will be resolved in the handler using video duration
                await self._quick_screenshot(update, context, session, time_str="__middle__")

            elif data == "screenshot_end":
                # Will be resolved in the handler using video duration
                await self._quick_screenshot(update, context, session, time_str="__end__")

            elif isinstance(data, str) and data.startswith("screenshot_"):
                option = data.split("_")[1]
                await self.take_screenshot(update, context, session, option)

            elif data == "extraction_menu":
                # Opened from the video tools menu; the individual actions are
                # handled by extract_audio / extract_streams / extract_subtitles.
                _has_video = bool(current_file) and current_file.get("type") == "video"
                _hint = "" if _has_video else "\n\nℹ️ Video Only, Subtitles and All Streams need a video file."
                await self.safe_edit(
                    query,
                    f"🗂️ **Extract Streams**\nChoose what to extract:{_hint}",
                    reply_markup=MediaMenuBuilder.get_extraction_menu(),
                )

            elif data == "extract_streams":
                await self.extract_streams(update, context, session)

            elif data == "extract_audio":
                await self.extract_audio(update, context, session)

            elif data == "extract_video":
                await self.extract_video(update, context, session)

            # Audio tools
            elif data == "convert_format_menu":
                # Determine appropriate media type for format menu (video vs audio)
                media_type = "audio"
                try:
                    if current_file and current_file.get("type") == "video":
                        media_type = "video"
                except Exception:
                    media_type = "audio"

                await self.safe_edit(
                    query,
                    "🔄 **Convert Format**\nSelect target format:",
                    reply_markup=MediaMenuBuilder.get_format_menu(media_type),
                )

            elif isinstance(data, str) and data.startswith("format_"):
                format_type = data.split("_")[1]
                if format_type in ("audio", "video"):
                    # "🔄 Convert" entry points use format_audio/format_video to
                    # only open the picker — they are not conversion targets.
                    media_type = "video" if format_type == "video" else "audio"
                    await self.safe_edit(
                        query,
                        f"🔄 **Convert {media_type.title()} Format**\nSelect target format:",
                        reply_markup=MediaMenuBuilder.get_format_menu(media_type),
                    )
                    return
                # Route to video or audio conversion depending on current file type
                try:
                    if current_file and current_file.get("type") == "video":
                        await self.convert_video_format(update, context, session, format_type)
                    else:
                        await self.convert_audio_format(update, context, session, format_type)
                except Exception:
                    # Fallback to audio conversion to preserve previous behavior
                    await self.convert_audio_format(update, context, session, format_type)

            elif data == "bitrate_menu":
                await self.safe_edit(
                    query,
                    "🎚️ **Adjust Bitrate**\nSelect bitrate:",
                    reply_markup=MediaMenuBuilder.get_bitrate_menu(),
                )

            # Merge list interactions
            elif data == "merge_add":
                # Add the current file to the merge list
                current_file = session.get("current_file")
                if not current_file:
                    await self.safe_edit(query, "❌ No current file to add. Send a file first.")
                    return
                path = current_file.get("path")
                if not path or not os.path.exists(path):
                    await self.safe_edit(query, "❌ File not available to add.")
                    return
                # Ensure merge_list stores file paths
                if "merge_list" not in session:
                    session["merge_list"] = []
                session["merge_list"].append(path)
                # Persist session after update
                try:
                    self._persist_session(user_id)
                except Exception:
                    logger.debug("Could not persist session after merge_add")
                await self.safe_edit(
                    query,
                    f"➕ Added to merge list. Total files: {len(session['merge_list'])}",
                )
                with contextlib.suppress(BadRequest):
                    await query.answer("Added to merge list")

            elif isinstance(data, str) and data.startswith("merge_view"):
                # Support pagination: callback forms: 'merge_view' or 'merge_view:2'
                try:
                    parts = data.split(":")
                    page = int(parts[1]) if len(parts) > 1 else 1
                except Exception:
                    page = 1

                merge_list = session.get("merge_list") or []
                if not merge_list:
                    await self.safe_edit(query, "🗒️ Merge list is empty.")
                else:
                    per_page = 5
                    total = len(merge_list)
                    last_page = max(1, (total + per_page - 1) // per_page)
                    page = max(1, min(page, last_page))
                    start = (page - 1) * per_page
                    end = start + per_page
                    slice_items = merge_list[start:end]

                    text_lines = [f"🗒️ Merge list ({page}/{last_page}):\n"]
                    for idx, p in enumerate(slice_items, start=start + 1):
                        text_lines.append(f"{idx}. {os.path.basename(p)}")

                    # Build navigation buttons
                    nav_buttons = []
                    if page > 1:
                        nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"merge_view:{page - 1}"))
                    if page < last_page:
                        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"merge_view:{page + 1}"))

                    control_row = [
                        InlineKeyboardButton("🗑️ Clear", callback_data="merge_clear"),
                        InlineKeyboardButton("↩️ Back", callback_data="menu_main"),
                    ]

                    kb = [[InlineKeyboardButton(os.path.basename(p), callback_data="noop")] for p in slice_items]
                    if nav_buttons:
                        kb.append(nav_buttons)
                    kb.append(control_row)

                    await self.safe_edit(query, "\n".join(text_lines), reply_markup=InlineKeyboardMarkup(kb))

            elif data == "merge_clear":
                session["merge_list"] = []
                try:
                    self._persist_session(user_id)
                except Exception:
                    logger.debug("Could not persist session after merge_clear")
                await self.safe_edit(query, "🗑️ Merge list cleared.")
                with contextlib.suppress(BadRequest):
                    await query.answer("Merge list cleared")

            elif data == "framerate_menu":
                await self.safe_edit(
                    query,
                    "⏱️ **Change Framerate**\nEnter target FPS (e.g., 24, 30, 60).",
                )
                # Clear previous prompts *before* arming this one, otherwise the
                # loop below would delete the flag we just set.
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_framerate"] = True

            elif data == "fade_menu":
                await self.safe_edit(
                    query,
                    "📈 Fade In/Out\nChoose a fade type:",
                    reply_markup=MediaMenuBuilder.get_fade_menu(),
                )

            elif data == "fade_in":
                await self._apply_fade(update, context, session, fade_in=3.0)

            elif data == "fade_out":
                await self._apply_fade(update, context, session, fade_out=3.0)

            elif data == "fade_both":
                await self._apply_fade(update, context, session, fade_in=3.0, fade_out=3.0)
            elif data == "cancel":
                # Clear any awaiting inputs and notify user, close the menu
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                await self.safe_edit(
                    query,
                    "❌ Operation cancelled.",
                )

            elif data == "confirm":
                await self.safe_edit(
                    query,
                    "✅ Confirmed.",
                    reply_markup=MediaMenuBuilder.get_main_menu(current_file["type"] if current_file else None),
                )

            elif isinstance(data, str) and data.startswith("bitrate_"):
                bitrate = data.split("_")[1]
                await self.adjust_bitrate(update, context, session, bitrate)

            elif data == "trim_audio":
                # Clear previous prompts *before* arming this one, otherwise the
                # loop below would delete the flag we just set.
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_trim"] = "start"
                await self.safe_edit(query, "✂️ **Trim Audio**\nSend start time (HH:MM:SS):")

            elif data == "caption_editor":
                # Ask user to send a new caption for the current file
                current_file = session.get("current_file")
                if not current_file:
                    await self.safe_edit(query, "❌ No file found to caption.")
                    return
                await self.safe_edit(query, "✏️ Send the new caption text:")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_caption"] = True

            elif data == "video_renamer":
                current_file = session.get("current_file")
                if not current_file:
                    await self.safe_edit(query, "❌ No file found to rename.")
                    return
                await self.safe_edit(query, "✏️ Send new filename (include extension):")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_rename"] = True

            elif data == "video_splitter":
                current_file = session.get("current_file")
                if not current_file or current_file.get("type") != "video":
                    await self.safe_edit(query, "❌ No video file found to split.")
                    return
                await self.safe_edit(
                    query,
                    "📌 Send split as either 'start-end' in seconds (e.g. 10-30) or 'n' for number of equal parts:",
                )
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_split"] = True

            elif data == "media_forwarder":
                current_file = session.get("current_file")
                if not current_file:
                    await self.safe_edit(query, "❌ No file to forward.")
                    return
                await self.safe_edit(query, "➡️ Send target chat id or @username to forward the file to:")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_forward_to"] = True

            elif data == "merge_audios_menu":
                await self.safe_edit(
                    query,
                    "🔀 **Merge Audio Files**\nSend multiple audio files, then click 'Start Merge':",
                    reply_markup=MediaMenuBuilder.get_merge_menu("audio"),
                )

            elif data == "merge_audios_start":
                await self.merge_audios(update, context, session)

            elif data == "normalize_audio":
                await self.normalize_audio(update, context, session)

            # Advanced tools
            elif data == "extract_all_streams":
                await self.extract_all_streams(update, context, session)

            elif data == "extract_subtitles":
                await self.extract_subtitles(update, context, session)

            elif data == "edit_metadata":
                await self.safe_edit(
                    query,
                    "🏷️ Edit metadata: send JSON (example in README).",
                )
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_metadata"] = True

            elif data == "full_info":
                await self.show_full_info(update, context, session)

            elif data == "create_archive":
                await self.create_archive(update, context, session)

            elif data == "bulk_menu":
                # Open the bulk action menu
                await self.show_bulk_menu(update, context)

            elif isinstance(data, str) and data.startswith("bulk_toggle:"):
                # Toggle a boolean bulk setting for the user
                try:
                    key = data.split(":", 1)[1]
                except Exception:
                    key = None
                if not key:
                    await self.safe_edit(query, "⚠️ Invalid toggle request.")
                    return

                try:
                    sess = session or self.user_sessions.setdefault(user_id, {})
                    new = not bool(_read_bulk_settings(user_id, sess).get(key))
                    _write_bulk_setting(user_id, sess, key, new)

                    await self.safe_edit(query, f"✅ {key.replace('_', ' ').title()}: {'On' if new else 'Off'}")
                    # re-render the bulk menu to show updated status
                    await self.show_bulk_menu(update, context)
                except Exception:
                    logger.exception("Failed to toggle bulk setting %s", key)
                    await self.safe_edit(query, "⚠️ Failed to toggle setting")

            elif data == "bulk_crf_menu":
                # Compress quality picker for the next Apply
                sess = session or self.user_sessions.setdefault(user_id, {})
                current = _sanitize_bulk_crf(_read_bulk_settings(user_id, sess).get("bulk_crf"))
                await self.safe_edit(
                    query,
                    f"🎚️ <b>Bulk compress quality</b>\n\nCurrent: CRF {current}\n"
                    "<i>Lower CRF means better quality and a larger file.</i>",
                    reply_markup=MediaMenuBuilder.get_bulk_crf_menu(current),
                )

            elif data == "bulk_preset_menu":
                # Optimize preset picker for the next Apply
                sess = session or self.user_sessions.setdefault(user_id, {})
                current = _sanitize_bulk_preset(_read_bulk_settings(user_id, sess).get("bulk_optimize_preset"))
                await self.safe_edit(
                    query,
                    f"⚡ <b>Bulk optimize preset</b>\n\nCurrent: {current}\n"
                    "<i>Applied when the Optimize toggle is on.</i>",
                    reply_markup=MediaMenuBuilder.get_bulk_preset_menu(current),
                )

            elif data == "bulk_bitrate_menu":
                # Extract Audio bitrate picker for the next Apply
                sess = session or self.user_sessions.setdefault(user_id, {})
                current = _sanitize_bulk_extract_bitrate(_read_bulk_settings(user_id, sess).get("bulk_extract_bitrate"))
                await self.safe_edit(
                    query,
                    f"🎵 <b>Bulk Extract Audio bitrate</b>\n\nCurrent: {current}\n"
                    "<i>Applied when the Extract Audio toggle is on — higher is better quality and a bigger file.</i>",
                    reply_markup=MediaMenuBuilder.get_bulk_bitrate_menu(current),
                )

            elif data == "bulk_slideshow_menu":
                # Slideshow seconds-per-photo picker for the next Apply
                sess = session or self.user_sessions.setdefault(user_id, {})
                current = _sanitize_bulk_slideshow_seconds(
                    _read_bulk_settings(user_id, sess).get("bulk_slideshow_seconds")
                )
                await self.safe_edit(
                    query,
                    f"🎞️ <b>Slideshow seconds per photo</b>\n\nCurrent: {current:g}s\n"
                    "<i>Applied when two or more photos are queued — they become one "
                    "slideshow video.</i>",
                    reply_markup=MediaMenuBuilder.get_bulk_slideshow_menu(current),
                )

            elif isinstance(data, str) and data.startswith("bulk_set_slideshow:"):
                # Store the chosen seconds-per-photo
                value = data.split(":", 1)[1]
                sess = session or self.user_sessions.setdefault(user_id, {})
                seconds = _sanitize_bulk_slideshow_seconds(value, default=0)
                if not seconds:
                    await self.safe_edit(query, "⚠️ Invalid slideshow option.")
                else:
                    _write_bulk_setting(user_id, sess, "bulk_slideshow_seconds", seconds)
                    await self.safe_edit(query, f"✅ Slideshow set to {seconds:g}s per photo.")
                    await self.show_bulk_menu(update, context)

            elif isinstance(data, str) and data.startswith("bulk_set_bitrate:"):
                # Store the chosen Extract Audio bitrate (or arm the custom prompt)
                value = data.split(":", 1)[1]
                sess = session or self.user_sessions.setdefault(user_id, {})
                if value == "custom":
                    for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
                    context.user_data["awaiting_bulk_bitrate"] = True
                    await self.safe_edit(
                        query,
                        f"✏️ Send the MP3 bitrate ({_AUDIO_BITRATE_MIN_KBPS}k-{_AUDIO_BITRATE_MAX_KBPS}k, e.g. 128k):",
                    )
                else:
                    bitrate = _sanitize_audio_bitrate(value, default="")
                    if not bitrate:
                        await self.safe_edit(query, "⚠️ Invalid bitrate option.")
                    else:
                        _write_bulk_setting(user_id, sess, "bulk_extract_bitrate", bitrate)
                        await self.safe_edit(query, f"✅ Bulk Extract Audio bitrate set to {bitrate}.")
                        await self.show_bulk_menu(update, context)

            elif isinstance(data, str) and data.startswith("bulk_set_crf:"):
                # Store the chosen CRF (or arm the custom-input prompt)
                value = data.split(":", 1)[1]
                sess = session or self.user_sessions.setdefault(user_id, {})
                if value == "custom":
                    for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
                    context.user_data["awaiting_bulk_crf"] = True
                    await self.safe_edit(
                        query,
                        f"✏️ Enter a CRF between {_BULK_COMPRESS_CRF_MIN} and {_BULK_COMPRESS_CRF_MAX}"
                        " (e.g. 23). Lower means better quality.",
                    )
                else:
                    crf = _parse_bulk_crf(value)
                    if crf is None:
                        await self.safe_edit(query, "⚠️ Invalid CRF option.")
                    else:
                        _write_bulk_setting(user_id, sess, "bulk_crf", crf)
                        await self.safe_edit(query, f"✅ Bulk compress CRF set to {crf}.")
                        await self.show_bulk_menu(update, context)

            elif isinstance(data, str) and data.startswith("bulk_set_preset:"):
                # Store the chosen optimize preset
                value = data.split(":", 1)[1]
                sess = session or self.user_sessions.setdefault(user_id, {})
                preset = str(value or "").strip().lower()
                if preset not in _BULK_OPTIMIZE_PRESETS:
                    await self.safe_edit(query, "⚠️ Invalid preset option.")
                else:
                    _write_bulk_setting(user_id, sess, "bulk_optimize_preset", preset)
                    await self.safe_edit(query, f"✅ Bulk optimize preset set to {preset}.")
                    await self.show_bulk_menu(update, context)

            elif data == "bulk_apply":
                # Apply bulk actions to the files collected for this batch. Sent
                # files land in bulk_list automatically; older sessions may still
                # only have a merge list or a single current file.
                try:
                    sess = session or self.user_sessions.get(user_id, {})
                    _source = sess.get("bulk_list") or sess.get("merge_list") or []
                    files = []
                    for item in _source:
                        entry = _normalize_bulk_item(item)
                        if entry is not None and entry not in files:
                            files.append(entry)
                    if not files and sess.get("current_file"):
                        files = [sess.get("current_file")]

                    if not files:
                        await self.safe_edit(
                            query,
                            "❌ No files to process.\nSend the file(s) first — they are collected "
                            "automatically — then press ▶️ Apply Bulk.",
                        )
                        return

                    # One apply at a time per account: the list is only cleared when
                    # a run finishes, so a second press would queue the whole batch
                    # again while the first is still working through it.
                    try:
                        _apply_started = float(sess.get("_bulk_apply_started_at") or 0)
                    except (TypeError, ValueError):
                        _apply_started = 0
                    if _apply_started and (time.time() - _apply_started) < _BULK_APPLY_GUARD_SECONDS:
                        await self.safe_edit(
                            query,
                            "⏳ A bulk apply is already running for your account.\n"
                            "Open its progress message and press ⏹️ Stop batch to end it first.",
                        )
                        return
                    sess["_bulk_apply_started_at"] = time.time()

                    # Ask the resume record which of these files an interrupted
                    # earlier run already finished, and drop them. Without this a
                    # restart mid-batch would silently redo every file it had
                    # already converted. Runs here rather than in the menu so a
                    # direct Apply gets it too.
                    _reclaimed = 0
                    try:
                        from utils.batch_pipeline import forget_finished_entries, read_finished_entries
                        from utils.job_queue import get_redis as _get_redis_resume

                        _r_resume = await _get_redis_resume()
                        _finished = await read_finished_entries(_r_resume, user_id)
                        if _finished:
                            _done_keys = set().union(*_finished.values())
                            _kept = [e for e in files if _bulk_entry_key(e) not in _done_keys]
                            _reclaimed = len(files) - len(_kept)
                            if _reclaimed:
                                files = _kept
                                await forget_finished_entries(_r_resume, _finished, _done_keys)
                                # Keep the stored collection in step with what is
                                # actually left, now that those keys are consumed
                                # and would no longer be pruned by a later resume.
                                sess["bulk_list"] = _kept
                                with contextlib.suppress(Exception):
                                    self._persist_session(user_id)
                                logger.info(
                                    "bulk apply: recovered %d already-finished file(s) for user %s",
                                    _reclaimed,
                                    user_id,
                                )
                    except Exception:
                        logger.debug("bulk apply: could not read the resume record")

                    if not files:
                        await self.safe_edit(
                            query,
                            "✅ Nothing left to do — every file collected here was already finished by an earlier run.",
                        )
                        with contextlib.suppress(Exception):
                            sess.pop("_bulk_apply_started_at", None)
                        return

                    # Honor every bulk toggle (convert / compress / extract audio /
                    # remove audio / rename / optimize) and tell the user which
                    # ones could not fit into the single pass, so nothing is
                    # silently ignored.
                    _bulk_settings = _read_bulk_settings(user_id, sess)

                    _plan = _resolve_bulk_plan(_bulk_settings)
                    _bulk_args = _plan["ffmpeg_args"]
                    _bulk_ext = _plan["output_ext"]

                    # A photo has no audio/video stream, so audio-only plans skip
                    # it (and are reported) instead of enqueuing a job that fails.
                    _photo_ok = _bulk_photo_supported(_plan)
                    _slideshow_seconds = _sanitize_bulk_slideshow_seconds(_bulk_settings.get("bulk_slideshow_seconds"))

                    enqueued = 0
                    skipped = 0
                    photo_skipped = 0
                    failed = 0
                    # (label, status) pairs — one per queued file/group — shown below.
                    results: list[tuple[str, str]] = []

                    # One identity for the whole apply. The worker uses it to run
                    # the batch strictly one file at a time (a per-batch Redis
                    # lock) and to force a memory cleanup between jobs, so 30
                    # videos never overlap in RAM. Redis holds the backlog; only
                    # one member of this batch runs at a time no matter how many
                    # worker replicas are up.
                    _batch_id = None
                    _batch_total = 0
                    _batch_seq = 0
                    try:
                        from utils.batch_pipeline import new_batch_id, tag_batch_job

                        _batch_id = new_batch_id()
                        _batch_total = len(files)
                    except Exception:
                        logger.debug("bulk apply: sequential batch tagging unavailable")

                    if _batch_id:
                        # This message stays the handler's: it carries the batch
                        # id and the Stop button for the duration of the apply
                        # and becomes the per-file summary at the end. The batch's
                        # own progress message is posted and owned by the worker,
                        # so exactly one writer ever touches either message. (The
                        # handler used to hand this message to the worker as the
                        # batch message as well, and the single-file progress
                        # watcher deleted it out from under the batch bar.)
                        with contextlib.suppress(Exception):
                            await self.safe_edit(
                                query,
                                f"▶️ Batch started: `{_batch_id}`\n"
                                "Progress for this batch, and for every other batch you "
                                "have running, is in the message below.\n"
                                f"`/cancelbatch {_batch_id}` also works.",
                                reply_markup=InlineKeyboardMarkup(
                                    [
                                        [
                                            InlineKeyboardButton(
                                                "⏹️ Stop batch",
                                                callback_data=f"batch_cancel:{_batch_id}",
                                            )
                                        ]
                                    ]
                                ),
                            )
                        # Make the batch visible to the aggregate view, and publish
                        # the expected count before the first job is queued so the
                        # worker can render its bar from the first second.
                        with contextlib.suppress(Exception):
                            from utils.batch_pipeline import (
                                open_batch_resume,
                                register_active_batch,
                                set_batch_total,
                            )

                            await set_batch_total(batch_id=_batch_id, total=len(files))
                            await register_active_batch(batch_id=_batch_id)
                            # Make the apply resumable before the first file starts,
                            # so a restart cannot leave finished work to be redone.
                            await open_batch_resume(batch_id=_batch_id, user_id=user_id)

                    # Photos in the batch become ONE slideshow video instead of a
                    # per-photo still-image encode. A lone photo keeps the normal
                    # single-file path in the loop below.
                    _photos = [f for f in files if f.get("type") == "photo"]
                    _slideshow_photos = _photos if len(_photos) >= 2 else []
                    if _slideshow_photos:
                        _photo_paths: list[str] = []
                        for _pf in _slideshow_photos:
                            try:
                                _p = await self._ensure_bulk_file_downloaded(update, context, sess, _pf)
                            except Exception:
                                logger.debug("bulk: slideshow download failed for %s", _pf.get("id"))
                                _p = None
                            if _p and os.path.exists(_p):
                                _photo_paths.append(_p)
                            else:
                                skipped += 1

                        # Background music: the first queued audio file, looped to
                        # cover the slideshow and cut at the video's end.
                        _music_path = None
                        _music_entry = _bulk_slideshow_music(files)
                        if _music_entry is not None:
                            try:
                                _mp = await self._ensure_bulk_file_downloaded(update, context, sess, _music_entry)
                            except Exception:
                                logger.debug("bulk: slideshow music download failed for %s", _music_entry.get("id"))
                                _mp = None
                            if _mp and os.path.exists(_mp):
                                _music_path = _mp

                        if _photo_paths:
                            _label = f"🎞 slideshow ({len(_photo_paths)} photos)"
                            job_id = str(uuid.uuid4()) if uuid else None
                            output_dir = (
                                getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
                            )
                            with contextlib.suppress(OSError):
                                os.makedirs(output_dir, exist_ok=True)
                            _ss_name = "slideshow.mp4"
                            if _plan["rename"]:
                                _renamed, _renamed_ok = _bulk_rename_filename(_ss_name, _bulk_settings)
                                if _renamed_ok:
                                    _ss_name = _renamed
                            _ss_job = {
                                "job_id": job_id,
                                "type": "slideshow",
                                "files": _photo_paths,
                                "output_path": os.path.join(output_dir, f"{job_id}_slideshow.mp4"),
                                "output_ext": ".mp4",
                                "original_filename": _ss_name,
                                "seconds_per_image": _slideshow_seconds,
                                "music_path": _music_path,
                                "progress_channel": f"ffmpeg:progress:{job_id}",
                                "chat_id": update.effective_chat.id
                                if update and getattr(update, "effective_chat", None)
                                else None,
                                "user_id": user_id,
                                "caption": f"🎞 Slideshow from {len(_photo_paths)} photo(s)",
                                "cleanup_input": True,
                                "cleanup_output": False,
                            }
                            if _batch_id:
                                try:
                                    tag_batch_job(_ss_job, _batch_id, _batch_seq, _batch_total)
                                    _batch_seq += 1
                                except Exception:
                                    logger.debug("bulk apply: could not tag slideshow job")
                            if enqueue_job:
                                try:
                                    try:
                                        _ss_job["request_id"] = getattr(update, "request_id", None)
                                    except Exception:
                                        _ss_job["request_id"] = None
                                    await enqueue_job(_ss_job)
                                    enqueued += 1
                                    results.append((_label, f"📋 queued · {job_id}"))
                                    # This loop is what paces the batch, so it has to
                                    # wait for the slideshow too. Without that the
                                    # loop could finish (and the batch be closed out)
                                    # while the slideshow was still converting, and
                                    # the batch's counts could never line up.
                                    if (
                                        await self._await_member_job(
                                            query,
                                            job_id,
                                            batch_id=_batch_id,
                                            index=_batch_seq,
                                            total=_batch_total,
                                            name=_label,
                                        )
                                        == "done"
                                    ):
                                        # The photos became this one video, so every
                                        # photo that went into it is done - otherwise a
                                        # resume would rebuild the slideshow.
                                        with contextlib.suppress(Exception):
                                            from utils.batch_pipeline import mark_batch_entry_finished
                                            from utils.job_queue import get_redis as _get_redis_ss

                                            _r_ss = await _get_redis_ss()
                                            for _photo_entry in _slideshow_photos:
                                                await mark_batch_entry_finished(
                                                    _r_ss, _batch_id, _bulk_entry_key(_photo_entry)
                                                )
                                except Exception:
                                    logger.exception("Failed to enqueue bulk slideshow")
                                    failed += 1
                                    results.append((_label, "❌ enqueue failed"))
                            else:
                                sess.setdefault("queued_bulk_jobs", []).append(_ss_job)
                                enqueued += 1
                                results.append((_label, f"📋 queued · {job_id}"))

                        # Slideshow photos are handled — keep them out of the loop.
                        _slideshow_ids = {id(f) for f in _slideshow_photos}
                        files = [f for f in files if id(f) not in _slideshow_ids]

                    stopped = False
                    stalled = False
                    _bulk_files = list(files)
                    for _idx, f in enumerate(_bulk_files):
                        try:
                            # Honour Stop between files, not just between jobs. The
                            # worker checks this marker before starting a job, but
                            # nothing used to check it here - so after pressing Stop
                            # the apply carried on fetching, which for a file over the
                            # Bot API limit means forwarding it to the relay group and
                            # downloading it with the userbot, only for the job it was
                            # for to be discarded. Stop should stop the fetching too.
                            if _batch_id:
                                try:
                                    from utils.batch_pipeline import is_batch_cancelled

                                    if await is_batch_cancelled(batch_id=_batch_id):
                                        stopped = True
                                        _remaining = len(_bulk_files) - _idx - 1
                                        if _remaining:
                                            results.append((f"+{_remaining} remaining file(s)", "⏹️ batch stopped"))
                                        break
                                except Exception:
                                    logger.debug("bulk apply: could not read the cancel marker")

                            if f.get("type") == "photo" and not _photo_ok:
                                photo_skipped += 1
                                results.append((_bulk_display_name(f), "⏭️ skipped — needs audio/video"))
                                continue

                            # Say which file is being fetched, on the handler's
                            # own message. Downloading a 500 MB source takes
                            # minutes and the worker's bar only appears once a job
                            # is running, so without this the one thing the user
                            # stares at while a batch is being fed is a message
                            # that has not changed since they pressed Apply.
                            await self._bulk_show_fetch_progress(query, _batch_id, _idx + 1, len(_bulk_files), f)

                            # Each entry may need its own download — the session's
                            # current_file is not necessarily this file.
                            try:
                                f["_pipeline_ffmpeg_args"] = list(_bulk_args)
                                f["_pipeline_conversion_type"] = _plan["convert_type"]
                                f["_pipeline_output_ext"] = _bulk_ext
                                _bulk_fallback_caption = (
                                    f"✅ Audio extracted ({_plan['extract_bitrate']})"
                                    if _bulk_ext == ".mp3"
                                    else f"Bulk conversion finished for {f.get('name') or f.get('id')}"
                                )
                                # A file that carries tags keeps its metadriven
                                # caption in a batch too - the same one the
                                # single-file actions build - so the title and
                                # performer survive Apply Bulk instead of every
                                # result arriving with one generic line. A file
                                # with no tags at all keeps the batch wording.
                                _bulk_tag_caption = (
                                    _metadata_caption(f)
                                    if (f.get("_source_metadata") or f.get("source_metadata"))
                                    else ""
                                )
                                f["_pipeline_caption"] = _bulk_tag_caption or _bulk_fallback_caption
                                f["_pipeline_batch_id"] = _batch_id
                                f["_pipeline_batch_seq"] = _batch_seq
                                f["_pipeline_batch_total"] = _batch_total
                                # What the apply's message shows while this file is
                                # fetched, queued, encoded and sent: the same one
                                # message for the whole batch.
                                f["_pipeline_file_index"] = _idx + 1
                                f["_pipeline_file_total"] = len(_bulk_files)
                                # Bounded on purpose: every step inside has its own
                                # timeout, but this is what guarantees the loop
                                # advances even if one of them is added later
                                # without one. Without it a single dead transfer
                                # stopped the apply dead - no next file queued, no
                                # bar moving, nothing said.
                                _file_path = await asyncio.wait_for(
                                    self._ensure_bulk_file_downloaded(update, context, sess, f),
                                    timeout=_BULK_FETCH_TIMEOUT_SECONDS,
                                )
                            except TimeoutError:
                                # ``asyncio.wait_for`` raises the builtin (the alias
                                # of ``asyncio.TimeoutError`` on 3.11+).
                                logger.warning(
                                    "bulk apply: fetching %s exceeded %.0fs; skipping it",
                                    _bulk_display_name(f),
                                    _BULK_FETCH_TIMEOUT_SECONDS,
                                )
                                skipped += 1
                                results.append(
                                    (
                                        _bulk_display_name(f),
                                        f"❌ could not fetch — timed out after "
                                        f"{int(_BULK_FETCH_TIMEOUT_SECONDS // 60)}m",
                                    )
                                )
                                continue
                            except Exception as _fetch_exc:
                                logger.warning(
                                    "bulk apply: could not fetch %s: %s",
                                    _bulk_display_name(f),
                                    _fetch_exc,
                                )
                                skipped += 1
                                results.append(
                                    (
                                        _bulk_display_name(f),
                                        f"❌ could not fetch — {_bulk_failure_reason(_fetch_exc)}",
                                    )
                                )
                                continue

                            # A pipeline job this file just queued is what fetches,
                            # converts and delivers it, so it is waited out here -
                            # after the fetch bound, which must never cut a live
                            # conversion short - with its own watchdog showing the
                            # encode instead of a silent poll.
                            await self._await_bulk_pipeline_job(f, query=query, index=_idx + 1, total=len(_bulk_files))

                            # A file the batch stopped before touching reports no
                            # path and no storage key - identical to a real fetch
                            # failure - which is why a stopped batch summarised as
                            # "Could not fetch N file(s)". Say what actually happened.
                            if f.get("_batch_cancelled"):
                                # Not a skipped file: the batch was stopped before
                                # this one was ever attempted, so it must not be
                                # counted or reported as a fetch failure.
                                stopped = True
                                results.append((_bulk_display_name(f), "⏹️ stopped with the batch"))
                                break

                            if f.get("_pipeline_failed"):
                                # The pipeline already had this file and could not
                                # convert it, so queueing another job for it would
                                # just fail twice.
                                failed += 1
                                results.append((_bulk_display_name(f), "❌ conversion failed"))
                                continue

                            # Check completion *before* looking for a local file: a
                            # conversion the pipeline already queued has neither a
                            # path nor a key of its own yet - its job is what fetches
                            # it. Checking for a path first reported those files as
                            # "could not fetch" and left them out of the batch count.
                            if f.pop("_bulk_pipeline_completed", False):
                                enqueued += 1
                                _batch_seq += 1
                                # The pipeline already queued this file's conversion,
                                # and that job is what fetches, converts and
                                # delivers - so it is a real worker job, not an
                                # inline one. Count it here *only* when the job is
                                # not tagged with this batch, because a tagged job
                                # is counted by the worker that runs it: counting
                                # both ways finished the batch at half its files
                                # and took the progress message down with files
                                # still queued.
                                if _batch_id and not await self._batch_worker_counts_job(
                                    _batch_id, f.get("_pipeline_job_id")
                                ):
                                    with contextlib.suppress(Exception):
                                        from utils.batch_pipeline import mark_batch_file_done

                                        await mark_batch_file_done(batch_id=_batch_id)
                                if _batch_id:
                                    with contextlib.suppress(Exception):
                                        from utils.batch_pipeline import mark_batch_entry_finished
                                        from utils.job_queue import get_redis as _get_redis_mark

                                        await mark_batch_entry_finished(
                                            await _get_redis_mark(), _batch_id, _bulk_entry_key(f)
                                        )
                                results.append((_bulk_display_name(f), f"✅ completed · {f.get('_pipeline_job_id')}"))
                                continue

                            if not _file_path and not f.get("input_key"):
                                skipped += 1
                                results.append((_bulk_display_name(f), "❌ could not fetch — no file or storage key"))
                                continue

                            job_id = str(uuid.uuid4()) if uuid else None
                            output_dir = (
                                getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
                            )
                            with contextlib.suppress(OSError):
                                os.makedirs(output_dir, exist_ok=True)
                            out_path = os.path.join(output_dir, f"{f.get('id')}_bulk{_bulk_ext}")
                            # Keep the delivered filename derived from the original
                            # name, applying the Rename toggle when it is on.
                            _bulk_name = f.get("name") or os.path.basename(out_path)
                            if _plan["rename"]:
                                _renamed, _renamed_ok = _bulk_rename_filename(_bulk_name, _bulk_settings)
                                if _renamed_ok:
                                    _bulk_name = _renamed
                            job = {
                                "job_id": job_id,
                                "input_path": f.get("path"),
                                "input_key": f.get("input_key"),
                                "output_path": out_path,
                                "original_filename": _bulk_name,
                                "ffmpeg_args": list(_bulk_args),
                                "output_ext": _bulk_ext,
                                "type": _plan["convert_type"],
                                "progress_channel": f"ffmpeg:progress:{job_id}",
                                "chat_id": update.effective_chat.id
                                if update and getattr(update, "effective_chat", None)
                                else None,
                                "thumbnail": None if _bulk_ext == ".mp3" else f.get("thumbnail"),
                                "caption": (
                                    f"✅ Audio extracted ({_plan['extract_bitrate']})"
                                    if _bulk_ext == ".mp3"
                                    else f"Bulk conversion finished for {_bulk_name}"
                                ),
                                "cleanup_input": True,
                                "cleanup_output": False,
                            }
                            if _batch_id:
                                try:
                                    tag_batch_job(job, _batch_id, _batch_seq, _batch_total)
                                    _batch_seq += 1
                                except Exception:
                                    logger.debug("bulk apply: could not tag job %s", job_id)

                            if enqueue_job:
                                try:
                                    try:
                                        job["request_id"] = getattr(update, "request_id", None)
                                    except Exception:
                                        job["request_id"] = None
                                    await enqueue_job(job)
                                    enqueued += 1
                                    results.append((_bulk_display_name(f), f"📋 queued · {job_id}"))
                                    # Keep bulk ingestion serial: do not download the next
                                    # source until this job has reached a terminal state,
                                    # so 30 large sources never pile up on disk or in RAM.
                                    # The batch's own message shows this member's stage
                                    # while we wait - the wait itself never writes code
                                    # into the chat (see _await_job_finished).
                                    _job_status = await self._await_member_job(
                                        query,
                                        job_id,
                                        batch_id=_batch_id,
                                        index=_idx + 1,
                                        total=len(_bulk_files),
                                        name=_bulk_name,
                                    )
                                    if _job_status is None:
                                        stalled = True
                                        results.append((_bulk_display_name(f), "⏱️ worker did not finish this job"))
                                        break
                                    if _job_status == "done" and _batch_id:
                                        # This file is genuinely finished, so a later
                                        # resume must not convert it again.
                                        with contextlib.suppress(Exception):
                                            from utils.batch_pipeline import mark_batch_entry_finished
                                            from utils.job_queue import get_redis as _get_redis_mark

                                            await mark_batch_entry_finished(
                                                await _get_redis_mark(), _batch_id, _bulk_entry_key(f)
                                            )
                                except Exception:
                                    logger.exception("Failed to enqueue bulk job for %s", f.get("id"))
                                    failed += 1
                                    results.append((_bulk_display_name(f), "❌ enqueue failed"))
                            else:
                                sess.setdefault("queued_bulk_jobs", []).append(job)
                                enqueued += 1
                                results.append((_bulk_display_name(f), f"📋 queued · {job_id}"))
                        except Exception:
                            logger.exception("Failed processing bulk file %s", f.get("id"))
                            failed += 1
                            results.append((_bulk_display_name(f), "❌ failed"))

                    # Record how many jobs this apply really enqueued. The worker
                    # uses it to know when the batch is finished and to take its
                    # single progress message down; the payload total is only the
                    # number of collected files, and some can be skipped above.
                    if _batch_id:
                        try:
                            from utils.batch_pipeline import set_batch_total

                            await set_batch_total(batch_id=_batch_id, total=enqueued)
                        except Exception:
                            logger.debug("bulk apply: could not record the batch total")
                        # The loop above waited on every job it queued, so the
                        # batch is over. The worker already removed its message
                        # whenever its counter matched the total; this covers the
                        # case where files were skipped at enqueue and the total
                        # the worker saw was the higher estimate.
                        with contextlib.suppress(Exception):
                            await self._close_batch_message(context, _batch_id)
                        with contextlib.suppress(Exception):
                            from utils.batch_pipeline import unregister_active_batch

                            await unregister_active_batch(batch_id=_batch_id)
                        # A run that finished keeps no resume record: its collection
                        # is cleared, so there is nothing left to skip. A stopped or
                        # stalled run keeps its record deliberately - the finished
                        # files are still in the collection, and pressing Apply again
                        # should continue from where it stopped rather than convert
                        # them a second time.
                        if not (stopped or stalled):
                            with contextlib.suppress(Exception):
                                from utils.batch_pipeline import close_batch_resume

                                await close_batch_resume(batch_id=_batch_id, user_id=user_id)

                    # Batch consumed — start fresh for the next round.
                    sess["bulk_list"] = []
                    with contextlib.suppress(Exception):
                        sess.pop("_bulk_apply_started_at", None)
                    try:
                        self._persist_session(user_id)
                    except Exception:
                        logger.debug("Could not persist session after bulk apply")

                    _applied = ", ".join(_BULK_ACTION_LABELS[key] for key in _plan["applied"])
                    _quality = _bulk_quality_label(_plan)
                    if _quality:
                        _applied = f"{_applied} ({_quality})"
                    if stalled:
                        _head = (
                            f"⏱️ Bulk apply stalled — {enqueued} file(s) were handled, then a job "
                            f"did not finish in time.\n• Applied: {_applied}"
                        )
                    elif stopped:
                        # You stopped this batch, so "finished" and "could not
                        # fetch" are both wrong: the files the stop prevented were
                        # never attempted. Say so plainly instead of dressing an
                        # abandoned batch up as a completed one with failures.
                        _head = (
                            f"⏹️ Bulk apply stopped — {enqueued} file(s) were already handled "
                            f"before the stop.\n• Applied: {_applied}"
                        )
                    else:
                        _head = f"✅ Bulk apply finished — queued {enqueued} file(s).\n• Applied: {_applied}"
                    _halted = stopped or stalled
                    if _batch_id and not _halted:
                        _head += (
                            f"\n• Batch ID: `{_batch_id}`\nUse `/cancelbatch {_batch_id}` to stop the remaining jobs."
                        )
                    if enqueued and not _halted:
                        _head += (
                            "\n🐢 Processing one file at a time (memory-safe) — "
                            "each result arrives as its job finishes."
                        )
                    if stopped:
                        _head += "\n⏹️ The remaining files were not fetched or queued."
                    if stalled:
                        _head += (
                            "\n⏱️ Gave up waiting after "
                            f"{int(_BULK_JOB_WAIT_SECONDS // 3600)}h — the worker may be down. "
                            "Check `/session_status`. The remaining files were not queued."
                        )
                    if skipped:
                        # Only genuine failures land here now; stopped files have
                        # their own line and are counted separately.
                        _head += f"\n⚠️ Could not fetch {skipped} file(s)."
                    if failed:
                        # Covers both outcomes: a file that could not be queued and
                        # one the pipeline already tried and failed to convert.
                        _head += f"\n❗ {failed} file(s) failed."
                    if _reclaimed:
                        _head += f"\n↩️ Skipped {_reclaimed} file(s) an earlier run had already finished."
                    if photo_skipped:
                        _head += f"\n⚠️ Skipped {photo_skipped} photo(s) — “{_applied}” needs an audio/video stream."
                    if _plan["ignored"]:
                        _skipped = ", ".join(_BULK_ACTION_LABELS[key] for key in _plan["ignored"])
                        _head += f"\n⚠️ Skipped — cannot run in the same pass: {_skipped}"

                    if results:
                        _lines = [f"• {name} → {status}" for name, status in results[:_BULK_SUMMARY_MAX_LINES]]
                        if len(results) > _BULK_SUMMARY_MAX_LINES:
                            _lines.append(f"… +{len(results) - _BULK_SUMMARY_MAX_LINES} more")
                        _head += "\n\n🗂 Per-file:\n" + "\n".join(_lines)
                    # The batch is over, so its Stop button goes with it.
                    await self.safe_edit(query, _head, reply_markup=None)
                except Exception:
                    logger.exception("bulk_apply failed")
                    # Release the apply guard, or a failed run would lock the user
                    # out of their own batch until the guard's window expires.
                    with contextlib.suppress(Exception):
                        (session or self.user_sessions.get(user_id, {})).pop("_bulk_apply_started_at", None)
                    await self.safe_edit(query, "⚠️ Failed to apply bulk actions.")

            elif data == "bulk_clear":
                # Drop the collected batch without processing it.
                #
                # This has to clear the merge list too. Apply Bulk reads
                # `bulk_list or merge_list`, and the menu counts the same way, so
                # clearing only `bulk_list` left everything in `merge_list` still
                # queued and still counted - which is why the button looked like it
                # did nothing. It also confirms what it removed, because a silent
                # re-render of an identical menu is indistinguishable from a
                # broken button.
                sess = session or self.user_sessions.get(user_id, {})
                # Count distinct entries: the same file can sit in both lists
                # (photos are added to each), and reporting it twice would be as
                # confusing as the silent button was.
                _seen = set()
                for _item in (sess.get("bulk_list") or []) + (sess.get("merge_list") or []):
                    _key = _bulk_item_key(_item) if isinstance(_item, dict) else str(_item)
                    if _key:
                        _seen.add(_key)
                total_cleared = len(_seen)
                sess["bulk_list"] = []
                sess["merge_list"] = []
                # Also clear the apply guard in case it's stuck
                sess.pop("_bulk_apply_started_at", None)
                try:
                    self._persist_session(user_id)
                except Exception:
                    logger.debug("Could not persist session after bulk_clear")

                if total_cleared:
                    _toast = f"🗑️ Cleared {total_cleared} file(s)"
                    logger.info("bulk_clear: cleared %s file(s) for user %s", total_cleared, user_id)
                else:
                    _toast = "List was already empty"
                with contextlib.suppress(BadRequest):
                    await query.answer(_toast, show_alert=False)
                await self.show_bulk_menu(update, context)

            elif data == "video_reorder":
                # Placeholder for video reorder feature
                await self.safe_edit(
                    query,
                    "🔁 Video Reorder\n\nThis feature is coming soon — you can manage order via the merge list for now.",
                )

            elif data == "mp3_tag_editor":
                # Simple entry point for mp3 tag edits (advanced editor may be added later)
                try:
                    # Clear stale prompts before arming this one (see add_subtitles).
                    for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
                    context.user_data["awaiting_mp3_tags"] = True
                    await self.safe_edit(
                        query,
                        '✏️ Mp3 Tag Editor\n\nSend a JSON object with tag keys and values (example: {"title":"Song"}).',
                    )
                except Exception:
                    await self.safe_edit(query, "⚠️ Failed to open Mp3 Tag Editor.")

            elif data == "convert_to_video":
                # Show video format menu
                await self.safe_edit(
                    query,
                    "🔄 Convert To Video\nSelect target container:",
                    reply_markup=MediaMenuBuilder.get_format_menu("video"),
                )

            elif data == "convert_to_file":
                # Generic convert menu
                await self.safe_edit(
                    query, "🔄 Convert To File\nSelect target format:", reply_markup=MediaMenuBuilder.get_format_menu()
                )

            elif data == "batch_process":
                await self.show_bulk_menu(update, context)

            # Settings pagination and toggle handlers
            elif isinstance(data, str) and data.startswith("settings_page:"):
                # Show a specific settings page
                try:
                    page = int(data.split(":", 1)[1])
                except Exception:
                    page = 1

                s = user_settings.get_user_settings(user_id) if user_settings else {}
                if page == 1:
                    text = "⚙️ <b>Your Settings — Page 1</b>\n\n"
                    text += f"• Upload mode: {s.get('upload_mode')}\n"
                    text += f"• Prefix: {s.get('prefix')!s}\n"
                    text += f"• Suffix: {s.get('suffix')!s}\n"
                    kb = [
                        [
                            InlineKeyboardButton(
                                f"Toggle Save Thumb: {'On' if s.get('save_thumbnail') else 'Off'}",
                                callback_data="toggle_save_thumbnail",
                            )
                        ],
                        [InlineKeyboardButton("Next ➡️", callback_data="settings_page:2")],
                        [InlineKeyboardButton("Close", callback_data="menu_main")],
                    ]
                    await self.safe_edit(query, text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
                else:
                    text = "⚙️ <b>Your Settings — Page 2</b>\n\n"
                    _words = html.escape(", ".join(s.get("words_remove") or []))
                    text += f"• Words to remove: {_words}\n"
                    _thumb = html.escape(str(s.get("default_thumbnail") or ""))
                    text += f"• Default thumbnail: {_thumb}\n"
                    kb = [
                        [InlineKeyboardButton("⬅️ Prev", callback_data="settings_page:1")],
                        [InlineKeyboardButton("Close", callback_data="menu_main")],
                    ]
                    await self.safe_edit(query, text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")

            elif isinstance(data, str) and data.startswith("toggle_"):
                # toggle_<key>
                key = data.split("_", 1)[1]
                if user_settings is None:
                    await self.safe_edit(query, "⚠️ Settings not available.")
                else:
                    try:
                        new = user_settings.toggle_user_setting(user_id, key)
                        await self.safe_edit(query, f"✅ `{key}` set to {new}", parse_mode="Markdown")
                        with contextlib.suppress(BadRequest):
                            await query.answer("Setting updated")
                    except Exception:
                        logger.exception("Failed to toggle setting %s for user %s", key, user_id)
                        await self.safe_edit(query, "⚠️ Failed to change setting.")

            elif data == "reset_settings":
                # Reset user's settings to defaults (confirmation shown)
                if user_settings is None:
                    await self.safe_edit(query, "⚠️ Settings not available.")
                else:
                    try:
                        user_settings.clear_user_settings(user_id)
                        await self.safe_edit(query, "✅ Your settings have been reset to defaults.")
                        with contextlib.suppress(BadRequest):
                            await query.answer("Settings reset")
                    except Exception:
                        logger.exception("Failed to reset settings for user %s", user_id)
                        await self.safe_edit(query, "⚠️ Failed to reset settings.")

            elif isinstance(data, str) and data.startswith("cancel_job:"):
                # User pressed a Cancel button for a queued job
                try:
                    job_id = data.split(":", 1)[1]
                except Exception:
                    await self.safe_edit(query, "⚠️ Invalid cancel request.")
                    return

                try:
                    from utils.job_queue import cancel_job, get_redis

                    # Set cancel_notified=1 first so _watch_job_progress() sees it
                    # before cancel_job() sets status=cancelled (eliminates race window).
                    try:
                        _r = await get_redis()
                        try:
                            await _r.hset(f"ffmpeg:job:{job_id}", "cancel_notified", "1")
                        finally:
                            _aclose = getattr(_r, "aclose", None)
                            if _aclose is not None:
                                await _aclose()
                            else:
                                await _r.close()
                    except Exception:
                        pass

                    await cancel_job(job_id)
                    await self.safe_edit(query, f"⏹️ Job {job_id} cancelled and removed from queue.")
                    with contextlib.suppress(BadRequest):
                        await query.answer("Job removed")
                except Exception:
                    logger.exception("Failed to cancel job %s", job_id)
                    await self.safe_edit(query, "⚠️ Failed to cancel job.")

            elif isinstance(data, str) and data.startswith("batch_cancel:"):
                # Stop button on a batch's progress message. The batch id rides in
                # the callback data, so the user never has to read it off a
                # message or type /cancelbatch to stop a batch.
                try:
                    batch_id = data.split(":", 1)[1]
                except Exception:
                    await self.safe_edit(query, "⚠️ Invalid batch cancel request.")
                    return

                try:
                    from utils.batch_pipeline import cancel_batch

                    # The stop takes the whole batch down itself: members flagged,
                    # queue and delayed entries pruned, the members' dedup keys
                    # dropped, and every ``ffmpeg:batch:<id>:*`` key removed apart
                    # from the tombstone that keeps a still-finishing worker from
                    # reposting the bar. The progress message's location comes back
                    # in ``report['message']`` so the bar can be deleted here.
                    report = await cancel_batch(batch_id=batch_id, requested_by=user_id)

                    # A batch can show two stoppable messages - the bot's summary and
                    # the worker's progress bar - and each carries its own Stop
                    # button. Stopping from one used to leave the other behind as a
                    # stale bar that nothing would ever remove, which reads exactly
                    # like a batch frozen at its last percentage. Take the batch's
                    # own progress message down as well, unless it is the one the
                    # user just pressed (that becomes the confirmation below).
                    _ref = report.get("message")
                    _pressed_id = getattr(getattr(query, "message", None), "message_id", None)
                    if _ref and _ref[1] != _pressed_id:
                        with contextlib.suppress(Exception):
                            await context.bot.delete_message(chat_id=_ref[0], message_id=_ref[1])

                    await self.safe_edit(
                        query,
                        "⏹️ Batch stopped.\n"
                        f"• Flagged {report['jobs']} job(s)\n"
                        f"• Removed {report['queued']} queued and {report['delayed']} waiting job(s)\n"
                        f"• Dropped {report.get('dedup', 0)} dedup key(s) and cleared the batch state",
                    )
                    with contextlib.suppress(BadRequest):
                        await query.answer("Batch stopped")
                except Exception:
                    logger.exception("Failed to cancel batch %s", batch_id)
                    await self.safe_edit(query, "⚠️ Failed to stop the batch.")

            elif isinstance(data, str) and data.startswith("cancel_dl:"):
                # User pressed the Cancel button during a userbot download
                try:
                    key = data.split(":", 1)[1]
                except Exception:
                    await self.safe_edit(query, "⚠️ Invalid cancel request.")
                    return

                cancel_flag = _download_cancel_flags.get(key)
                if cancel_flag is not None:
                    cancel_flag[0] = True
                    await self.safe_edit(query, "⏹️ Cancelling download...")
                    with contextlib.suppress(BadRequest):
                        await query.answer("Cancellation requested")
                else:
                    await self.safe_edit(query, "⏳ Download already completed or not found.")

            elif data == "add_thumb_instruction":
                # Provide instructions to the user for adding a custom thumbnail
                await self.safe_edit(
                    query,
                    "To add a custom thumbnail: reply to a photo with the command /addthumb",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Close", callback_data="menu_main")]]),
                )

            elif data == "delete_custom_thumb":
                # Delete per-user thumbnail file if present
                try:
                    thumb_path = os.path.join(os.path.dirname(__file__), "thumbnails", f"{user_id}.jpg")
                    if os.path.exists(thumb_path):
                        os.remove(thumb_path)
                        await self.safe_edit(query, "🗑️ Custom thumbnail deleted.")
                    else:
                        await self.safe_edit(query, "⚠️ No custom thumbnail set.")
                except Exception:
                    logger.exception("Failed to delete custom thumbnail for user %s", user_id)
                    await self.safe_edit(query, "⚠️ Failed to delete thumbnail.")

            elif data == "thumbnail_grid":
                await self.create_thumbnail_grid(update, context, session)

            elif data == "add_subtitles":
                # Clear stale prompts before arming this one so the next upload is
                # routed here rather than into a previously armed flow.
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_subtitle_file"] = True
                await self.safe_edit(query, "➕ **Add Subtitles**\nSend subtitle file (.srt, .ass):")

            elif data == "burn_subtitles":
                # Ask user to send subtitle file to burn into current video
                current_file = session.get("current_file")
                if not current_file or current_file.get("type") != "video":
                    await self.safe_edit(query, "❌ No video file found to burn subtitles into.")
                else:
                    for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
                    context.user_data["awaiting_burn_subtitle"] = True
                    await self.safe_edit(
                        query,
                        ("✏️ **Burn Subtitles**\nSend subtitle file (.srt, .ass) to burn into the current video:"),
                    )

            # Information
            elif data == "info":
                await self.show_media_info(update, context, session)

            elif data == "noop":
                # Non-actionable placeholder button pressed; acknowledge silently.
                with contextlib.suppress(BadRequest):
                    await query.answer()
                return

            else:
                await self.safe_edit(query, f"Unknown command: {data}")
        except Exception as e:
            # Log unexpected exceptions along with callback metadata
            try:
                await self._log_bad_callback(
                    "callback_handler_exception",
                    {
                        "exception": repr(e),
                        "callback_data": data,
                    },
                    getattr(update.effective_user, "id", None),
                    getattr(update.effective_chat, "id", None),
                    getattr(getattr(query, "message", None), "message_id", None),
                )
            except Exception:
                logger.exception("Failed to persist callback_handler exception")

            # Optional debug dump of full Update JSON when enabled via env
            try:
                if os.environ.get("DEBUG_DUMP_UPDATES", "0").lower() in ("1", "true", "yes"):
                    dump_dir = os.path.join(os.path.dirname(__file__), "logs")
                    os.makedirs(dump_dir, exist_ok=True)
                    dump_path = os.path.join(dump_dir, "update_dumps.log")
                    try:
                        # Prefer structured dict if Update supports it
                        if hasattr(update, "to_dict"):
                            update_data = update.to_dict()
                        elif hasattr(update, "to_json"):
                            # to_json may return a JSON string
                            try:
                                update_data = json.loads(update.to_json())
                            except Exception:
                                update_data = {"repr": repr(update)}
                        else:
                            update_data = {"repr": repr(update)}

                        entry = {
                            "timestamp": utc_iso(),
                            "update_id": getattr(update, "update_id", None),
                            "callback_data": data,
                            "exception": repr(e),
                            "update": update_data,
                        }
                        with open(dump_path, "a", encoding="utf-8") as fh:
                            fh.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
                        logger.info("Wrote update dump to %s", dump_path)
                    except Exception:
                        logger.exception("Failed writing update dump for callback exception")
            except Exception:
                logger.exception("Failed to evaluate DEBUG_DUMP_UPDATES")

            logger.exception("Unhandled exception in callback_handler: %s", e)
            try:
                await self.safe_edit(query, "⚠️ Internal error while handling the button. Try again later.")
            except Exception:
                # Best-effort only
                logger.exception("Failed to notify user after callback_handler exception")
            return

    # ========== IMPLEMENTATION METHODS ==========

    async def show_mp3_quality_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Show the MP3 quality picker used before extracting audio from a video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file.get("type") != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        current = _sanitize_audio_bitrate(current_file.get("audio_bitrate"))
        await self.safe_edit(
            query,
            "🎵 **Extract Audio (MP3)**\n"
            f"Current quality: **{current}**\n"
            f"{current} keeps the file small and still sounds good.",
            reply_markup=MediaMenuBuilder.get_mp3_quality_menu(current),
        )

    async def extract_audio(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Extract the audio track of the current media ("Audio Only" button).

        Videos go through the MP3 quality picker; audio files go through the
        bitrate picker, which re-encodes them at the chosen quality.
        """
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file:
            await self.safe_edit(query, "❌ No file found.")
            return

        if current_file.get("type") == "video":
            await self.show_mp3_quality_menu(update, context, session)
            return

        if current_file.get("type") == "audio":
            await self.safe_edit(
                query,
                "🎚️ Choose the target bitrate for this audio:",
                reply_markup=MediaMenuBuilder.get_bitrate_menu("audio"),
            )
            return

        await self.safe_edit(query, "❌ Audio can only be extracted from a video or audio file.")

    async def extract_video(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Extract the video track only (audio/subtitles dropped) into an MP4.

        This is the "🎬 Video Only" action of the extraction menu; "All Streams"
        (``extract_streams``) keeps every track and delivers a ZIP instead.
        """
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")
        user_id = update.effective_user.id

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        # ── Cancel any stale pipeline job so user's specific settings take effect ──
        if current_file.get("_pipeline_job_id"):
            await self._cancel_stale_pipeline_job(session, "extract_video", user_id)

        await self.safe_edit(query, "🎬 Extracting video (without audio)...")

        # ── Store conversion metadata so the BigFilePipeline knows what to produce ──
        current_file["_pipeline_ffmpeg_args"] = list(_EXTRACT_VIDEO_FFMPEG_ARGS)
        current_file["_pipeline_output_ext"] = ".mp4"
        current_file["_pipeline_conversion_type"] = "extract_video"
        current_file["_pipeline_caption"] = _metadata_caption(current_file)
        session["current_file"] = current_file

        # Ensure file is available locally (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
                # If the pipeline queued a job (big file), watch it and return.
                if current_file and current_file.get("_pipeline_job_id"):
                    _pipeline_job_id = current_file["_pipeline_job_id"]
                    kb = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{_pipeline_job_id}")]]
                    )
                    await self.safe_edit(
                        query,
                        f"✅ Large file queued (Job: {_pipeline_job_id[:8]}...). I'll send the video when ready.",
                        reply_markup=kb,
                    )
                    with contextlib.suppress(RuntimeError):
                        asyncio.create_task(self._watch_job_progress(query, _pipeline_job_id, bot=context.bot))
                    return
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        # Enqueue the job so a worker handles the encoding, progress and delivery
        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{current_file['id']}_video_only.mp4")
        job_id = str(uuid.uuid4())
        job = {
            "job_id": job_id,
            "input_path": current_file["path"] or current_file.get("_local_input_path"),
            "input_key": current_file.get("input_key"),
            "output_path": output_path,
            # Keeps the delivered filename derived from the original name.
            "original_filename": current_file.get("name") or os.path.basename(output_path),
            "ffmpeg_args": list(_EXTRACT_VIDEO_FFMPEG_ARGS),
            "output_ext": ".mp4",
            "type": "extract_video",
            "progress_channel": f"ffmpeg:progress:{job_id}",
            "chat_id": update.effective_chat.id if update and update.effective_chat else None,
            "thumbnail": current_file.get("thumbnail"),
            "caption": "✅ Video extracted (audio removed)",
            "cleanup_input": True,
            "cleanup_output": False,
        }

        try:
            try:
                job["request_id"] = getattr(update, "request_id", None)
            except Exception:
                job["request_id"] = None
            await enqueue_job(job)
        except Exception:
            logger.exception("Failed to enqueue extract_video job")
            await self.safe_edit(query, "❌ Failed to queue extraction.")
            return

        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}")]])
        await self.safe_edit(query, f"✅ Job queued (ID: {job_id}). I'll send the video when ready.", reply_markup=kb)
        with contextlib.suppress(RuntimeError):
            asyncio.create_task(self._watch_job_progress(query, job_id, bot=context.bot))

    async def convert_to_mp3(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: dict,
        bitrate: str | None = None,
    ):
        """Extract MP3 audio from the current video and deliver it as playable audio.

        ``bitrate`` is the requested audio bitrate (e.g. ``"128k"``). When it is
        omitted the bitrate previously chosen for this file — or the 128k
        default — is used. The original media name is preserved for delivery.
        """
        query = getattr(update, "callback_query", None)
        message = getattr(update, "message", None)

        if query is None and message is None:
            logger.warning("convert_to_mp3 invoked without callback_query or message")
            return

        async def notify(text, **kwargs):
            """Report progress through whichever update context triggered this run."""
            if query is not None:
                await self.safe_edit(query, text, **kwargs)
            else:
                await message.reply_text(text)

        current_file = session.get("current_file")
        user_id = update.effective_user.id

        if not current_file or current_file.get("type") != "video":
            await notify("❌ No video file found.")
            return

        audio_bitrate = _sanitize_audio_bitrate(bitrate or current_file.get("audio_bitrate") or _DEFAULT_AUDIO_BITRATE)
        current_file["audio_bitrate"] = audio_bitrate
        session["current_file"] = current_file
        delivery_name = _audio_delivery_name(current_file.get("name"), current_file.get("id"))
        caption = _metadata_caption(current_file)

        # Conversion quota enforcement
        if not await self._check_conversion_quota(update, context):
            return

        # Check rate limiting
        conversion_limiter = context.application.bot_data.get("conversion_rate_limiter")
        if conversion_limiter:
            allowed, limit_message = await conversion_limiter.can_convert(str(user_id))
            if not allowed:
                await notify(limit_message)
                return

        # Check queue status
        active_count = len(self.active_conversions)
        max_conversions = getattr(self, "_max_conversions", 1)

        if active_count >= max_conversions:
            queue_position = active_count - max_conversions + 1
            await notify(
                f"⏳ Queue position: #{queue_position}\n"
                f"Active conversions: {active_count}/{max_conversions}\n"
                f"Your conversion will start soon...",
            )
        else:
            await notify(f"🎵 Converting to MP3 ({audio_bitrate})...")

        async def do_conversion():
            # Lock the input file to prevent concurrent access
            output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
            with contextlib.suppress(OSError):
                os.makedirs(output_dir, exist_ok=True)
            # Transient on-disk name; ``delivery_name`` carries the original name.
            output_path = os.path.join(output_dir, f"{current_file['id']}_audio.mp3")

            async def _deliver():
                """Send the extracted MP3 as streamable Telegram audio (music player)."""
                file_size = os.path.getsize(output_path)
                if file_size > config.BOT_API_MAX_BYTES:
                    await notify(
                        f"❌ File too large ({file_size // 1024 // 1024}MB).\nTry compression first.",
                    )
                    return
                # send_audio keeps the file in Telegram's music player (streamable)
                # instead of delivering it as an opaque downloadable document.
                with open(output_path, "rb") as audio_file:
                    await context.bot.send_audio(
                        chat_id=update.effective_chat.id,
                        audio=audio_file,
                        caption=caption,
                        title=os.path.splitext(delivery_name)[0],
                        filename=delivery_name,
                        performer="",
                    )

            if AsyncFileLock:
                # Defensive: ensure we have a concrete file path before attempting locks
                path = current_file.get("path")
                if not path:
                    await notify("❌ Local file missing. Try re-downloading or use the web uploader.")
                    return

                lock = await AsyncFileLock.acquire(path)
                async with lock:
                    success = await self.converter.extract_audio_from_video(path, output_path, "mp3", audio_bitrate)

                    if success and os.path.exists(output_path):
                        await _deliver()
                        os.remove(output_path)
                    else:
                        await notify("❌ Conversion failed.")

                await AsyncFileLock.release(path)
            else:
                # Fallback without locking
                success = await self.converter.extract_audio_from_video(
                    current_file["path"], output_path, "mp3", audio_bitrate
                )

                if success and os.path.exists(output_path):
                    await _deliver()
                    os.remove(output_path)
                else:
                    await notify("❌ Conversion failed.")

        # ── Store conversion metadata so the BigFilePipeline knows what to produce ──
        # The pipeline derives the delivered filename from ``original_filename``
        # (i.e. ``current_file["name"]``), so the original name is preserved.
        current_file["_pipeline_ffmpeg_args"] = ["-vn", "-acodec", "libmp3lame", "-ab", audio_bitrate]
        current_file["_pipeline_output_ext"] = ".mp3"
        current_file["_pipeline_conversion_type"] = "extract_audio"
        current_file["_pipeline_caption"] = caption
        session["current_file"] = current_file

        # Ensure file downloaded before conversion (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
                if current_file and current_file.get("_pipeline_job_id"):
                    _pipeline_job_id = current_file["_pipeline_job_id"]
                    kb = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{_pipeline_job_id}")]]
                    )
                    await notify(
                        f"✅ Large file queued (Job: {_pipeline_job_id[:8]}...). I'll send the MP3 when ready.",
                        reply_markup=kb,
                    )
                    # Progress can only be watched when we own the callback message.
                    if query is not None:
                        with contextlib.suppress(RuntimeError):
                            asyncio.create_task(self._watch_job_progress(query, _pipeline_job_id, bot=context.bot))
                    return
            except Exception as e:
                await notify(f"❌ Failed to download file: {e}")
                return

        # ── Cancel any stale pipeline job so user's specific settings take effect ──
        if current_file and current_file.get("_pipeline_job_id"):
            await self._cancel_stale_pipeline_job(session, "convert_to_mp3", user_id)

        await self._run_with_concurrency_limit(user_id, "mp3_conversion", do_conversion())

    async def compress_video(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: dict,
        crf: str,
    ):
        """Compress video with specified CRF."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        user_id = update.effective_user.id

        if not await self._check_conversion_quota(update, context):
            return

        if crf == "custom":
            # Clear previous prompts *before* arming this one (the loop would
            # otherwise delete the flag we just set) and stop here so the value
            # typed by the user is the one that gets used.
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
            context.user_data["awaiting_crf"] = True
            await self.safe_edit(query, "✏️ Enter CRF value (18-51, lower=better quality):")
            return

        current_file = session.get("current_file")
        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        active_count = len(self.active_conversions)
        max_conversions = getattr(self, "_max_conversions", 1)

        if active_count >= max_conversions:
            queue_position = active_count - max_conversions + 1
            await self.safe_edit(
                query,
                f"⏳ Queue position: #{queue_position}\n"
                f"Active conversions: {active_count}/{max_conversions}\n"
                f"Your compression will start soon...",
            )
        else:
            await self.safe_edit(query, f"📉 Compressing with CRF {crf}...")

        async def do_compression():
            output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
            with contextlib.suppress(OSError):
                os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{current_file['id']}_compressed.mp4")

            # Map resolution presets
            resolution_map = {
                "4k_to_1080": ("1920", "1080"),
                "1080_to_720": ("1280", "720"),
            }

            if crf in resolution_map:
                width, height = resolution_map[crf]
                success = await self.converter.change_resolution(
                    current_file["path"], output_path, int(width), int(height)
                )
            else:
                # default optimize path: treat crf as an integer when possible
                crf_value = int(crf) if isinstance(crf, str) and crf.isdigit() else 28
                success = await self.converter.optimize_video(current_file["path"], output_path, "medium", crf_value)

            if success and os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                if file_size > 2 * 1024**3:  # 2GB
                    await self.safe_edit(
                        query,
                        f"❌ Compressed file still too large ({file_size // 1024 // 1024}MB).\nTry higher compression.",
                    )
                    os.remove(output_path)
                else:
                    await self._send_video_result(
                        context.bot,
                        update.effective_chat.id,
                        output_path,
                        caption=_metadata_caption(current_file),
                    )
                    os.remove(output_path)
            else:
                await self.safe_edit(query, "❌ Compression failed.")

        # ── Store conversion metadata so the BigFilePipeline knows what to produce ──
        _crf_value = int(crf) if isinstance(crf, str) and crf.isdigit() else 28
        current_file["_pipeline_ffmpeg_args"] = [
            "-c:v",
            "libx264",
            "-preset",
            "medium",
            "-crf",
            str(_crf_value),
            "-movflags",
            "+faststart",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
        ]
        current_file["_pipeline_output_ext"] = ".mp4"
        current_file["_pipeline_conversion_type"] = "compress_video"
        current_file["_pipeline_caption"] = _metadata_caption(current_file)
        session["current_file"] = current_file

        # Ensure file downloaded before compression (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
                if current_file and current_file.get("_pipeline_job_id"):
                    _pipeline_job_id = current_file["_pipeline_job_id"]
                    kb = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{_pipeline_job_id}")]]
                    )
                    await self.safe_edit(
                        query,
                        f"✅ Large file queued (Job: {_pipeline_job_id[:8]}...). I'll send the compressed video when ready.",
                        reply_markup=kb,
                    )
                    with contextlib.suppress(RuntimeError):
                        asyncio.create_task(self._watch_job_progress(query, _pipeline_job_id, bot=context.bot))
                    return
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        # ── Cancel any stale pipeline job so user's specific settings take effect ──
        if current_file and current_file.get("_pipeline_job_id"):
            await self._cancel_stale_pipeline_job(session, "compress_video", user_id)

        await self._run_with_concurrency_limit(user_id, "compression", do_compression())

    async def merge_videos(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Merge multiple videos."""
        if not await self._require_callback(update):
            return
        query = update.callback_query

        if not await self._check_conversion_quota(update, context):
            return

        if "merge_list" not in session or len(session["merge_list"]) < 2:
            await self.safe_edit(
                query,
                "❌ Need at least 2 videos to merge.\nSend video files first, then click 'Start Merge'.",
            )
            return

        await self.safe_edit(query, f"🔀 Merging {len(session['merge_list'])} videos...")

        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"merged_{int(datetime.now(UTC).timestamp())}.mp4")
        success = await self.converter.merge_videos(session["merge_list"], output_path)
        if success and os.path.exists(output_path):
            await self._send_video_result(
                context.bot,
                update.effective_chat.id,
                output_path,
                caption=_metadata_caption(session.get("current_file")),
            )

            # Cleanup
            os.remove(output_path)
            for file_path in session["merge_list"]:
                if os.path.exists(file_path):
                    os.remove(file_path)
            session["merge_list"] = []
        else:
            await self.safe_edit(query, "❌ Merge failed.")

    async def merge_audios(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Merge multiple audio files."""
        if not await self._require_callback(update):
            return
        query = update.callback_query

        if not await self._check_conversion_quota(update, context):
            return

        if "merge_list" not in session or len(session["merge_list"]) < 2:
            await self.safe_edit(
                query,
                "❌ Need at least 2 audio files to merge.\nSend audio files first, then click 'Start Merge'.",
            )
            return

        await self.safe_edit(query, f"🔀 Merging {len(session['merge_list'])} audio files...")

        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"merged_{int(datetime.now(UTC).timestamp())}.mp3")
        success = await self.converter.merge_audios(session["merge_list"], output_path)

        if success and os.path.exists(output_path):
            # Use the first input's name as the base so the merge keeps a
            # recognisable (and streamable) audio filename.
            _first_name = session["current_file"].get("name") if session.get("current_file") else None
            # ``or "Merged Audio"`` already guarantees a non-empty name, so there is
            # no empty stem for a fallback id to fill in.
            delivery_name = _audio_delivery_name(_first_name or "Merged Audio", extension=".mp3")
            with open(output_path, "rb") as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption=_metadata_caption(session.get("current_file")),
                    title=os.path.splitext(delivery_name)[0],
                    filename=delivery_name,
                    performer="",
                )

            # Cleanup
            os.remove(output_path)
            for file_path in session["merge_list"]:
                if os.path.exists(file_path):
                    os.remove(file_path)
            session["merge_list"] = []
        else:
            await self.safe_edit(query, "❌ Merge failed.")

    async def remove_audio(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Remove audio from video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        await self.safe_edit(query, "🔉 Removing audio...")

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{current_file['id']}_no_audio.mp4")
        success = await self.converter.remove_audio(current_file["path"], output_path)

        if success and os.path.exists(output_path):
            await self._send_video_result(
                context.bot,
                update.effective_chat.id,
                output_path,
                caption=_metadata_caption(current_file),
            )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Failed to remove audio.")

    async def change_resolution(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: dict,
        resolution: str,
    ):
        """Change video resolution."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        # Resolution mapping
        res_map = {
            "4k": (3840, 2160),
            "2k": (2560, 1440),
            "1080": (1920, 1080),
            "720": (1280, 720),
            "480": (854, 480),
            "360": (640, 360),
            "mobile": (480, 854),  # Portrait mobile
        }

        if resolution == "custom":
            # Clear previous prompts before arming this one, then stop so the
            # resolution typed by the user is the one that gets encoded.
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
            context.user_data["awaiting_resolution"] = True
            await self.safe_edit(query, "📐 Enter resolution (WIDTHxHEIGHT):\nExample: 1280x720")
            return

        if not await self._check_conversion_quota(update, context):
            return

        if resolution not in res_map:
            await self.safe_edit(query, "❌ Invalid resolution.")
            return

        width, height = res_map[resolution]
        await self.safe_edit(query, f"📐 Changing resolution to {width}x{height}...")

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{current_file['id']}_{width}x{height}.mp4")
        success = await self.converter.change_resolution(current_file["path"], output_path, width, height)

        if success and os.path.exists(output_path):
            await self._send_video_result(
                context.bot,
                update.effective_chat.id,
                output_path,
                caption=_metadata_caption(current_file),
            )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Failed to change resolution.")

    async def optimize_video(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: dict,
        preset: str,
    ):
        """Optimize video for specific use case."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        # Preset mapping
        preset_map = {
            "web": ("slow", 23, "128k"),
            "mobile": ("medium", 28, "96k"),
            "tv": ("slow", 20, "192k"),
            "storage": ("veryfast", 35, "64k"),
            "fast": ("veryfast", 28, "128k"),
        }

        if preset == "custom":
            # Clear previous prompts before arming this one, then stop so the
            # settings typed by the user are the ones that get encoded.
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
            context.user_data["awaiting_optimize"] = True
            await self.safe_edit(
                query,
                "⚡ Enter optimization settings:\nFormat: preset,crf,bitrate\nExample: slow,23,128k",
            )
            return

        if preset not in preset_map:
            await self.safe_edit(query, "❌ Invalid preset.")
            return

        encoder_preset, crf, bitrate = preset_map[preset]
        # ── Store conversion metadata so the BigFilePipeline knows what to produce ──
        current_file["_pipeline_ffmpeg_args"] = [
            "-c:v",
            "libx264",
            "-preset",
            encoder_preset,
            "-crf",
            str(crf),
            "-movflags",
            "+faststart",
            "-c:a",
            "aac",
            "-b:a",
            bitrate,
        ]
        current_file["_pipeline_output_ext"] = ".mp4"
        current_file["_pipeline_conversion_type"] = "optimize_video"
        current_file["_pipeline_caption"] = _metadata_caption(current_file)
        session["current_file"] = current_file

        await self.safe_edit(query, f"⚡ Optimizing for {preset}...")

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
                # If pipeline queued a job (big file), watch it and return.
                if current_file and current_file.get("_pipeline_job_id"):
                    _pipeline_job_id = current_file["_pipeline_job_id"]
                    kb = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{_pipeline_job_id}")]]
                    )
                    await self.safe_edit(
                        query,
                        f"✅ Large file queued (Job: {_pipeline_job_id[:8]}...). I'll send the result when ready.",
                        reply_markup=kb,
                    )
                    with contextlib.suppress(RuntimeError):
                        asyncio.create_task(self._watch_job_progress(query, _pipeline_job_id, bot=context.bot))
                    return
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        # ── Cancel any stale pipeline job so user's specific settings take effect ──
        # (Only reached for small files downloaded via Bot API)
        if current_file and current_file.get("_pipeline_job_id"):
            await self._cancel_stale_pipeline_job(session, "optimize_video", update.effective_user.id)

        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{current_file['id']}_optimized.mp4")

        # Use FFmpeg command for optimization
        cmd = [
            "-c:v",
            "libx264",
            "-preset",
            encoder_preset,
            "-crf",
            str(crf),
            "-c:a",
            "aac",
            "-b:a",
            bitrate,
            "-movflags",
            "+faststart",
        ]

        # Try to enqueue as a background job to get progress + cancel support
        if enqueue_job and uuid:
            job_id = str(uuid.uuid4())
            job = {
                "job_id": job_id,
                "input_path": current_file["path"] or current_file.get("_local_input_path"),
                "input_key": current_file.get("input_key"),
                "output_path": output_path,
                # Keeps the delivered filename derived from the original name.
                "original_filename": current_file.get("name") or os.path.basename(output_path),
                "ffmpeg_args": cmd,
                "progress_channel": f"ffmpeg:progress:{job_id}",
                "chat_id": update.effective_chat.id if update and update.effective_chat else None,
                "thumbnail": current_file.get("thumbnail"),
                "caption": f"✅ Optimized for {preset}",
                "cleanup_input": True,
                "cleanup_output": True,
            }
            try:
                try:
                    job["request_id"] = getattr(update, "request_id", None)
                except Exception:
                    job["request_id"] = None
                await enqueue_job(job)
            except Exception:
                logger.exception("Failed to enqueue optimization job")
                await self.safe_edit(query, "❌ Failed to queue optimization job.")
                return

            # Inform user and provide cancel button
            try:
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}")]])
                await self.safe_edit(
                    query, f"✅ Optimization job queued (ID: {job_id}). I'll update you with progress.", reply_markup=kb
                )
                with contextlib.suppress(RuntimeError):
                    asyncio.create_task(self._watch_job_progress(query, job_id, bot=context.bot))
            except Exception:
                await self.safe_edit(query, f"✅ Optimization job queued (ID: {job_id}).")
            return

        # Fallback: inline execution if no job queue available
        success, _ = await self.converter.execute_ffmpeg(cmd, current_file["path"], output_path)

        if success and os.path.exists(output_path):
            await self._send_video_result(
                context.bot,
                update.effective_chat.id,
                output_path,
                caption=_metadata_caption(current_file),
            )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Optimization failed.")

    async def repair_video(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Attempt to repair corrupted video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        # Conversion quota enforcement
        if not await self._check_conversion_quota(update, context):
            return

        # ── Store conversion metadata so the BigFilePipeline knows what to produce ──
        current_file["_pipeline_ffmpeg_args"] = ["-c", "copy"]
        current_file["_pipeline_output_ext"] = ".mp4"
        current_file["_pipeline_conversion_type"] = "repair_video"
        current_file["_pipeline_caption"] = _metadata_caption(current_file)
        session["current_file"] = current_file

        await self.safe_edit(query, "🔧 Attempting to repair video...")
        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
                # If pipeline queued a job (big file), watch it and return.
                if current_file and current_file.get("_pipeline_job_id"):
                    _pipeline_job_id = current_file["_pipeline_job_id"]
                    kb = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{_pipeline_job_id}")]]
                    )
                    await self.safe_edit(
                        query,
                        f"✅ Large file queued (Job: {_pipeline_job_id[:8]}...). I'll send the result when ready.",
                        reply_markup=kb,
                    )
                    with contextlib.suppress(RuntimeError):
                        asyncio.create_task(self._watch_job_progress(query, _pipeline_job_id, bot=context.bot))
                    return
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        # ── Cancel any stale pipeline job so user's specific settings take effect ──
        # (Only reached for small files downloaded via Bot API)
        if current_file and current_file.get("_pipeline_job_id"):
            await self._cancel_stale_pipeline_job(session, "repair_video", update.effective_user.id)

        # enqueue repair job
        job_id = str(uuid.uuid4())
        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{current_file['id']}_repaired.mp4")
        job = {
            "job_id": job_id,
            "input_path": current_file["path"] or current_file.get("_local_input_path"),
            "input_key": current_file.get("input_key"),
            "output_path": output_path,
            # Keeps the delivered filename derived from the original name.
            "original_filename": current_file.get("name") or os.path.basename(output_path),
            "ffmpeg_args": ["-c", "copy"],
            "progress_channel": f"ffmpeg:progress:{job_id}",
            "chat_id": update.effective_chat.id if update and update.effective_chat else None,
            "thumbnail": current_file.get("thumbnail"),
            "caption": "✅ Video repaired (if possible)",
            "cleanup_input": True,
            "cleanup_output": True,
        }

        try:
            try:
                job["request_id"] = getattr(update, "request_id", None)
            except Exception:
                job["request_id"] = None
            await enqueue_job(job)
            await self.safe_edit(query, f"✅ Repair job queued (ID: {job_id}). I'll send the file when ready.")
            with contextlib.suppress(RuntimeError):
                asyncio.create_task(self._watch_job_progress(query, job_id, bot=context.bot))
        except Exception:
            logger.exception("Failed to enqueue repair job")
            await self.safe_edit(query, "❌ Failed to queue repair job.")
        return

    async def _quick_screenshot(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: dict,
        time_str: str,
    ):
        """Take a single screenshot for the Start/Middle/End shortcuts.

        ``time_str`` may be ``"__middle__"`` or ``"__end__"``, which are resolved
        against the media duration; any other value is passed to ffmpeg as-is.
        """
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file.get("type") != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        input_path = current_file.get("path")
        if not input_path or not os.path.exists(input_path):
            await self.safe_edit(query, "❌ File not available on disk.")
            return

        if time_str in ("__middle__", "__end__"):
            duration = None
            try:
                from utils.ffmpeg_runner import probe_media

                _meta = await probe_media(input_path)
                duration = _meta.get("duration")
            except Exception:
                duration = None
            if duration:
                if time_str == "__middle__":
                    time_str = f"{float(duration) / 2:.3f}"
                else:
                    time_str = f"{max(0.0, float(duration) - 1):.3f}"
            else:
                logger.warning("Could not read duration of %s; using 00:00:01", input_path)
                time_str = "00:00:01"

        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{current_file.get('id', 'unknown')}_screenshot.jpg")

        await self.safe_edit(query, f"🖼️ Taking screenshot at {time_str}...")
        success = await self.converter.take_screenshot_at_time(input_path, output_path, time_str)

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as photo_file:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=photo_file,
                    caption=_metadata_caption(current_file),
                )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Failed to take screenshot.")

    async def take_screenshot(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: dict,
        option: str,
    ):
        """Take screenshot(s) from video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        # Conversion quota enforcement
        if not await self._check_conversion_quota(update, context):
            return

        # Get video duration for calculations (try ffmpeg-python binding, fallback to error)
        ffmpeg_mod = ffmpeg
        if ffmpeg_mod is None:
            try:
                import importlib

                ffmpeg_mod = importlib.import_module("ffmpeg")
            except Exception:
                ffmpeg_mod = None

        if not ffmpeg_mod:
            await self.safe_edit(
                query,
                "FFmpeg-python binding is not available on the server. This operation requires ffmpeg-python.",
            )
            logger.info("ffmpeg-python not available for take_screenshot; falling back to CLI where possible")
            return

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        try:
            probe = ffmpeg_mod.probe(current_file["path"])
            duration = float(probe["format"]["duration"])
        except Exception as e:
            logger.warning(f"ffmpeg.probe failed: {e}")
            await self.safe_edit(query, "Failed to read media info for screenshot operation.")
            return

        if option == "custom":
            # Clear previous prompts before arming this one and wait for the time
            # instead of falling through and grabbing a frame at 00:00:01.
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
            context.user_data["awaiting_screenshot_time"] = True
            await self.safe_edit(query, "✏️ Enter time (HH:MM:SS or seconds):")
            return

        # Calculate time based on option
        def _fmt_time(seconds: float) -> str:
            secs = max(0.0, float(seconds))
            hours = int(secs // 3600)
            mins = int((secs % 3600) // 60)
            rem = secs % 60
            return f"{hours:02d}:{mins:02d}:{rem:06.3f}"

        middle = duration / 2
        end_time = max(0.0, duration - 1)

        time_map = {
            "start": "00:00:01",
            "middle": _fmt_time(middle),
            "end": _fmt_time(end_time),
        }

        if option == "9grid":
            await self.create_thumbnail_grid(update, context, session)
            return
        elif option == "multiple":
            # Clear previous prompts before arming this one and wait for the count
            # instead of falling through and taking a single screenshot.
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
            context.user_data["awaiting_screenshot_count"] = True
            await self.safe_edit(query, "✏️ How many screenshots? (2-20)")
            return

        time_str = time_map.get(option, "00:00:01")
        await self.safe_edit(query, f"🖼️ Taking screenshot at {time_str}...")

        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{current_file.get('id', 'unknown')}_screenshot.jpg")
        success = await self.converter.take_screenshot_at_time(current_file["path"], output_path, time_str)

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as photo_file:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=photo_file,
                    caption=_metadata_caption(current_file),
                )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Failed to take screenshot.")

    async def create_thumbnail_grid(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Create thumbnail grid from video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        await self.safe_edit(query, "🖼️ Creating thumbnail grid...")

        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{current_file['id']}_grid.jpg")
        success = await self.converter.extract_thumbnail_grid(current_file["path"], output_path, 3, 3)

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as photo_file:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=photo_file,
                    caption=_metadata_caption(current_file),
                )
            os.remove(output_path)
        else:
            # Fallback to single screenshot
            await self.take_screenshot(update, context, session, "middle")

    async def extract_streams(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Extract all streams from video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        # ── Store conversion metadata so the BigFilePipeline knows what to produce ──
        current_file["_pipeline_ffmpeg_args"] = None
        current_file["_pipeline_output_ext"] = ".zip"
        current_file["_pipeline_conversion_type"] = "extract_streams"
        current_file["_pipeline_caption"] = _metadata_caption(current_file)
        session["current_file"] = current_file

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
                # If pipeline queued a job (big file), watch it and return.
                if current_file and current_file.get("_pipeline_job_id"):
                    _pipeline_job_id = current_file["_pipeline_job_id"]
                    kb = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{_pipeline_job_id}")]]
                    )
                    await self.safe_edit(
                        query,
                        f"✅ Large file queued (Job: {_pipeline_job_id[:8]}...). I'll send the result when ready.",
                        reply_markup=kb,
                    )
                    with contextlib.suppress(RuntimeError):
                        asyncio.create_task(self._watch_job_progress(query, _pipeline_job_id, bot=context.bot))
                    return
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        # ── Cancel any stale pipeline job so user's specific settings take effect ──
        # (Only reached for small files downloaded via Bot API)
        if current_file and current_file.get("_pipeline_job_id"):
            await self._cancel_stale_pipeline_job(session, "extract_streams", update.effective_user.id)

        await self.safe_edit(query, "🎞️ Extracting streams...")

        # enqueue extract_streams job to worker for progress/cancel support
        job_id = str(uuid.uuid4())
        out_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        out_dir = os.path.join(out_base, f"{current_file['id']}_streams")
        with contextlib.suppress(OSError):
            os.makedirs(out_dir, exist_ok=True)
        archive_path = f"{out_dir}.zip"
        # The worker delivers `archive_path`, not `output_path`, so the archive
        # name has to be carried explicitly or the zip arrives as `{job_id}_streams.zip`.
        _source_stem = os.path.splitext(current_file.get("name") or "")[0]
        _streams_name = f"{_source_stem}_streams.zip" if _source_stem else os.path.basename(archive_path)
        job = {
            "job_id": job_id,
            "type": "extract_streams",
            "input_path": current_file["path"] or current_file.get("_local_input_path"),
            "input_key": current_file.get("input_key"),
            "output_dir": out_dir,
            "archive_path": archive_path,
            "original_filename": current_file.get("name") or os.path.basename(archive_path),
            "output_filename": _streams_name,
            "progress_channel": f"ffmpeg:progress:{job_id}",
            "chat_id": update.effective_chat.id if update and update.effective_chat else None,
            "thumbnail": current_file.get("thumbnail"),
            "cleanup_input": True,
        }

        try:
            job["request_id"] = getattr(update, "request_id", None)
        except Exception:
            job["request_id"] = None
        try:
            job["request_id"] = getattr(update, "request_id", None)
        except Exception:
            job["request_id"] = None
        await enqueue_job(job)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}")]])
        await self.safe_edit(query, f"⏳ Job queued: {job_id} — extracting streams", reply_markup=kb)
        with contextlib.suppress(RuntimeError):
            asyncio.create_task(self._watch_job_progress(query, job_id, bot=context.bot))
        return

    async def convert_audio_format(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: dict,
        format_type: str,
    ):
        """Convert audio to different format."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "audio":
            await self.safe_edit(query, "❌ No audio file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        # ── Store conversion metadata so the BigFilePipeline knows what to produce ──
        # Every target gets an explicit codec: anything missing here must not
        # silently fall back to ``-c:a copy`` (that produces a file whose
        # contents do not match its extension).
        _format_ffmpeg_args = {
            "mp3": ["-c:a", "libmp3lame", "-b:a", _DEFAULT_AUDIO_BITRATE],
            "wav": ["-c:a", "pcm_s16le"],
            "aac": ["-c:a", "aac", "-b:a", "128k"],
            "m4a": ["-c:a", "aac", "-b:a", "128k"],
            "flac": ["-c:a", "flac"],
            "ogg": ["-c:a", "libvorbis", "-b:a", "128k"],
            "opus": ["-c:a", "libopus", "-b:a", "96k"],
        }
        current_file["_pipeline_ffmpeg_args"] = _format_ffmpeg_args.get(format_type, ["-c:a", "copy"])
        current_file["_pipeline_output_ext"] = f".{format_type}"
        current_file["_pipeline_conversion_type"] = "format_audio"
        current_file["_pipeline_caption"] = _metadata_caption(current_file)
        session["current_file"] = current_file

        await self.safe_edit(query, f"🔄 Converting to {format_type.upper()}...")

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
                # If pipeline queued a job (big file), watch it and return.
                if current_file and current_file.get("_pipeline_job_id"):
                    _pipeline_job_id = current_file["_pipeline_job_id"]
                    kb = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{_pipeline_job_id}")]]
                    )
                    await self.safe_edit(
                        query,
                        f"✅ Large file queued (Job: {_pipeline_job_id[:8]}...). I'll send the result when ready.",
                        reply_markup=kb,
                    )
                    with contextlib.suppress(RuntimeError):
                        asyncio.create_task(self._watch_job_progress(query, _pipeline_job_id, bot=context.bot))
                    return
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        # ── Cancel any stale pipeline job so user's specific settings take effect ──
        # (Only reached for small files downloaded via Bot API)
        if current_file and current_file.get("_pipeline_job_id"):
            await self._cancel_stale_pipeline_job(session, "convert_audio_format", update.effective_user.id)

        output_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_base, exist_ok=True)
        output_path = os.path.join(output_base, f"{current_file['id']}_converted.{format_type}")
        success = await self.converter.convert_audio_format(current_file["path"], output_path, format_type)

        if success and os.path.exists(output_path):
            # NOTE: Bot API infers the MIME type from the filename, and
            # ``send_audio`` has no mime_type parameter — passing one raises
            # TypeError and would silently downgrade delivery to a document.
            delivery_name = _audio_delivery_name(
                current_file.get("name"), current_file.get("id"), extension=f".{format_type}"
            )
            with open(output_path, "rb") as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption=_metadata_caption(current_file),
                    title=os.path.splitext(delivery_name)[0],
                    filename=delivery_name,
                    performer="",
                )
            os.remove(output_path)
        else:
            await self.safe_edit(query, f"❌ Failed to convert to {format_type}.")

    async def adjust_bitrate(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: dict,
        bitrate: str,
    ):
        """Re-encode the current audio file at a specific bitrate."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file:
            await self.safe_edit(query, "❌ No audio file found.")
            return

        if current_file.get("type") != "audio":
            # Videos are handled by the Video -> Audio flow so the user also
            # gets to pick the container/quality before anything is encoded.
            await self.safe_edit(query, "❌ No audio file found. Use 🎵 Video To Audio for videos.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        if bitrate == "custom":
            # Clear previous prompts *before* arming this one, otherwise the
            # loop would delete the flag we just set.
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
            context.user_data["awaiting_bitrate"] = True
            await self.safe_edit(query, "✏️ Enter bitrate (32k-320k, e.g. 128k, 320k):")
            return

        audio_bitrate = _sanitize_audio_bitrate(bitrate)
        current_file["audio_bitrate"] = audio_bitrate
        session["current_file"] = current_file

        await self.safe_edit(query, f"🎚️ Setting bitrate to {audio_bitrate}...")

        output_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_base, exist_ok=True)
        output_path = os.path.join(output_base, f"{current_file['id']}_{audio_bitrate}.mp3")

        # Convert with specific bitrate
        cmd = ["-c:a", "libmp3lame", "-b:a", audio_bitrate]

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        success, _ = await self.converter.execute_ffmpeg(cmd, current_file["path"], output_path)

        if success and os.path.exists(output_path):
            delivery_name = _audio_delivery_name(current_file.get("name"), current_file.get("id"))
            with open(output_path, "rb") as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption=_metadata_caption(current_file),
                    title=os.path.splitext(delivery_name)[0],
                    filename=delivery_name,
                    performer="",
                )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Failed to adjust bitrate.")

    async def normalize_audio(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Normalize audio volume."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "audio":
            await self.safe_edit(query, "❌ No audio file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        await self.safe_edit(query, "🔊 Normalizing audio...")

        output_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_base, exist_ok=True)
        output_path = os.path.join(output_base, f"{current_file['id']}_normalized.mp3")
        audio_bitrate = _sanitize_audio_bitrate(current_file.get("audio_bitrate"))

        # Use loudnorm filter for normalization
        cmd = [
            "-filter:a",
            "loudnorm=I=-16:TP=-1.5:LRA=11",
            "-c:a",
            "libmp3lame",
            "-b:a",
            audio_bitrate,
        ]

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        success, _ = await self.converter.execute_ffmpeg(cmd, current_file["path"], output_path)

        if success and os.path.exists(output_path):
            delivery_name = _audio_delivery_name(current_file.get("name"), current_file.get("id"))
            with open(output_path, "rb") as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption=_metadata_caption(current_file),
                    title=os.path.splitext(delivery_name)[0],
                    filename=delivery_name,
                    performer="",
                )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Failed to normalize audio.")

    async def extract_all_streams(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Extract all streams (video, audio, subtitles)."""
        if not await self._require_callback(update):
            return
        await self.extract_streams(update, context, session)

    async def extract_subtitles(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Extract subtitles from video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        await self.safe_edit(query, "📝 Extracting subtitles...")

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        output_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_base, exist_ok=True)
        output_path = os.path.join(output_base, f"{current_file['id']}_subtitles.srt")
        success = await self.converter.extract_subtitles(current_file["path"], output_path)

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as sub_file:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=sub_file,
                    caption=_metadata_caption(current_file),
                    filename=f"{current_file['name']}_subtitles.srt",
                )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ No subtitles found or extraction failed.")

    async def show_full_info(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Show full media information."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file:
            await self.safe_edit(query, "❌ No file found.")
            return

        await self.safe_edit(query, "📊 Analyzing media...")

        ffmpeg_mod = ffmpeg
        if ffmpeg_mod is None:
            try:
                import importlib

                ffmpeg_mod = importlib.import_module("ffmpeg")
            except Exception:
                ffmpeg_mod = None

        if not ffmpeg_mod:
            await self.safe_edit(
                query,
                "FFmpeg-python binding is not available on the server. Full media analysis requires ffmpeg-python.",
            )
            logger.warning("ffmpeg-python not available for media analysis")
            return

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        try:
            probe = ffmpeg_mod.probe(current_file["path"])

            # Format information
            format_info = probe.get("format", {})
            streams = probe.get("streams", [])

            info_text = "📊 **Full Media Analysis**\n\n"
            info_text += f"📁 **File:** {current_file['name']}\n"
            info_text += f"📦 **Size:** {format_info.get('size', 0) // 1024 // 1024} MB\n"
            info_text += f"🎞️ **Format:** {format_info.get('format_name', 'N/A')}\n"
            info_text += f"⏱️ **Duration:** {float(format_info.get('duration', 0)):.2f}s\n"
            bitrate_kbps = int(format_info.get("bit_rate", 0)) // 1000
            info_text += f"📈 **Bitrate:** {bitrate_kbps} kbps\n\n"

            # Streams information
            info_text += f"🎬 **Streams ({len(streams)}):**\n"

            for i, stream in enumerate(streams):
                codec_type = stream.get("codec_type", "unknown")
                info_text += f"\n**Stream {i + 1} ({codec_type}):**\n"

                if codec_type == "video":
                    info_text += f"  Codec: {stream.get('codec_name', 'N/A')}\n"
                    info_text += f"  Resolution: {stream.get('width', 'N/A')}x{stream.get('height', 'N/A')}\n"
                    num, den = stream.get("avg_frame_rate", "0/1").split("/")
                    fps = float(num) / float(den) if float(den) != 0 else 0
                    info_text += f"  FPS: {fps:.2f}\n"
                    sb = stream.get("bit_rate")
                    sb_kbps = f"{int(sb) // 1000} kbps" if sb else "N/A"
                    info_text += f"  Bitrate: {sb_kbps}\n"

                elif codec_type == "audio":
                    info_text += f"  Codec: {stream.get('codec_name', 'N/A')}\n"
                    info_text += f"  Channels: {stream.get('channels', 'N/A')}\n"
                    info_text += f"  Sample Rate: {stream.get('sample_rate', 'N/A')} Hz\n"
                    sb = stream.get("bit_rate")
                    sb_kbps = f"{int(sb) // 1000} kbps" if sb else "N/A"
                    info_text += f"  Bitrate: {sb_kbps}\n"

                elif codec_type == "subtitle":
                    info_text += f"  Codec: {stream.get('codec_name', 'N/A')}\n"
                    info_text += f"  Language: {stream.get('tags', {}).get('language', 'N/A')}\n"

            await self.safe_edit(query, info_text[:4000])  # Telegram message limit

        except Exception as e:
            logger.error(f"Error analyzing media: {e}")
            await self.safe_edit(query, "❌ Failed to analyze media.")

    async def create_archive(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Create archive of processed files."""
        if not await self._require_callback(update):
            return
        query = update.callback_query

        # Get all files in output directory for this user
        user_id = update.effective_user.id
        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
        with contextlib.suppress(OSError):
            os.makedirs(output_dir, exist_ok=True)
        user_files = [f for f in os.listdir(output_dir) if f.startswith(str(user_id))]

        if not user_files:
            await self.safe_edit(query, "❌ No files to archive.")
            return

        await self.safe_edit(query, "📦 Creating archive...")

        # enqueue create_archive job so worker handles packaging and progress
        file_paths = [os.path.join(output_dir, f) for f in user_files]
        archive_path = os.path.join(output_dir, f"{user_id}_archive.zip")
        job_id = str(uuid.uuid4())
        job = {
            "job_id": job_id,
            "type": "create_archive",
            "files": file_paths,
            "output_path": archive_path,
            # Name the delivered archive after the first selected file.
            "original_filename": f"{os.path.splitext(os.path.basename(file_paths[0]))[0]}_archive.zip"
            if file_paths
            else os.path.basename(archive_path),
            "progress_channel": f"ffmpeg:progress:{job_id}",
            "chat_id": update.effective_chat.id if update and update.effective_chat else None,
        }

        try:
            job["request_id"] = getattr(update, "request_id", None)
        except Exception:
            job["request_id"] = None
        await enqueue_job(job)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}")]])
        await self.safe_edit(query, f"⏳ Job queued: {job_id} — creating archive", reply_markup=kb)
        with contextlib.suppress(RuntimeError):
            asyncio.create_task(self._watch_job_progress(query, job_id, bot=context.bot))

    async def show_media_info(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict):
        """Show basic media information."""
        if not await self._require_callback(update):
            return
        await self.show_full_info(update, context, session)

    async def log_media_to_db(self, user_id: int, file_info: dict):
        """Log media processing to MongoDB."""
        try:
            # Prefer an existing, cached model created during application startup
            if getattr(self, "db_model", None) is not None:
                model = self.db_model
            else:
                # If the optional top-level model class isn't available, skip
                if MediaConversionModel is None:
                    logger.info("MongoDB model class not available; skipping DB logging")
                    return

                # Resolve canonical Mongo URI (prefer config if imported)
                mongo_uri = None
                try:
                    if "config" in globals() and config is not None:
                        mongo_uri = (
                            getattr(config, "MONGO_URI", None)
                            or os.environ.get("MONGO_URI")
                            or os.environ.get("MONGODB_URL")
                        )
                    else:
                        mongo_uri = os.environ.get("MONGO_URI") or os.environ.get("MONGODB_URL")
                except Exception:
                    mongo_uri = os.environ.get("MONGO_URI") or os.environ.get("MONGODB_URL")

                if not mongo_uri:
                    logger.info("No MongoDB URI configured; skipping DB logging")
                    return

                # Create and cache a Motor client + model for reuse
                try:
                    from motor.motor_asyncio import AsyncIOMotorClient

                    client = AsyncIOMotorClient(mongo_uri)
                    model = MediaConversionModel(
                        client,
                        db_name=os.environ.get("MONGODB_NAME", None) or "media_conversion_bot",
                    )
                    # Cache on the handler so subsequent calls reuse the same model
                    self.db_model = model
                    # Register globally too, so the userbot downloader/uploader
                    # (which only receive a user_id) can reach MongoDB.
                    from utils.telethon_session import set_db_model

                    set_db_model(model)

                    # Ensure indexes asynchronously (best-effort)
                    try:
                        import asyncio as _asyncio

                        try:
                            _asyncio.get_running_loop()
                            _asyncio.create_task(model.ensure_indexes())
                        except RuntimeError:
                            pass
                    except Exception:
                        logger.debug("Could not schedule async index creation for Mongo model")

                except Exception as e:
                    logger.error(f"Failed to initialize MongoDB client/model: {e}")
                    return

            # Build log entry and persist via the model
            try:
                log_entry = {
                    "user_id": user_id,
                    "file_name": file_info.get("name"),
                    "file_type": file_info.get("type"),
                    "file_size": file_info.get("size"),
                    "timestamp": datetime.now(UTC),
                    "action": "upload",
                }

                await model.log_conversion(log_entry)
                logger.info("Logged media upload for user %s", user_id)
            except Exception as e:
                logger.error(f"Failed to log to MongoDB: {e}")

            else:
                # Catch-all for any unhandled callback (guarded: no update/query/data available here)
                logger.debug("log_media_to_db: unhandled path for user %s", user_id)

        except Exception as e:
            logger.error(f"Failed to log to MongoDB: {e}")

    async def handle_custom_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle custom user input for various operations."""
        user_input = update.message.text.strip()
        user_id = update.effective_user.id

        if user_id not in self.user_sessions:
            await update.message.reply_text("❌ Session expired. Please send a file first.")
            return ConversationHandler.END

        session = self.user_sessions[user_id]
        current_file = session.get("current_file")

        # --- Dynamic trimmer flow (Trimmer 1 & 2) ---
        if context.user_data.get("awaiting_trimmer"):
            mode = context.user_data.get("awaiting_trimmer")
            try:
                if mode == "trimmer1_start":
                    # Validate start time
                    _ = _parse_time_to_seconds(user_input)
                    context.user_data["trimmer_start"] = user_input.strip()
                    context.user_data["awaiting_trimmer"] = "trimmer1_end"
                    await update.message.reply_text(
                        "📥 Start time saved. Now send END time (HH:MM:SS[.ms])\nExample: 00:10:00"
                    )
                    return

                elif mode == "trimmer1_end":
                    start = context.user_data.get("trimmer_start")
                    if not start:
                        await update.message.reply_text("❌ Missing start time. Please restart Trimmer.")
                        for k in list(context.user_data.keys()):
                            if k.startswith("awaiting_") or k.startswith("trimmer_"):
                                del context.user_data[k]
                        return

                    # Parse times
                    start_s = _parse_time_to_seconds(start)
                    end_s = _parse_time_to_seconds(user_input)
                    if end_s <= start_s:
                        await update.message.reply_text("❌ End time must be after start time. Send END time again.")
                        return

                    # Perform trim
                    await update.message.reply_text(f"✂️ Trimming from {start} to {user_input}...")
                    output_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
                    with contextlib.suppress(OSError):
                        os.makedirs(output_base, exist_ok=True)
                    output_path = os.path.join(
                        output_base, f"{current_file['id']}_trim_{int(start_s)}_{int(end_s)}.mp4"
                    )
                    success = await self.converter.trim_video(
                        current_file["path"], output_path, start, user_input.strip()
                    )

                    if success and os.path.exists(output_path):
                        await self._send_video_result(
                            context.bot,
                            update.effective_chat.id,
                            output_path,
                            caption=_metadata_caption(current_file),
                        )
                        os.remove(output_path)
                    else:
                        await update.message.reply_text("❌ Failed to trim video.")

                    # Cleanup state
                    for k in list(context.user_data.keys()):
                        if k.startswith("awaiting_") or k.startswith("trimmer_") or k.startswith("trimmer"):
                            del context.user_data[k]
                    return

                elif mode == "trimmer2_start":
                    # Save start and ask for duration
                    _ = _parse_time_to_seconds(user_input)
                    context.user_data["trimmer_start"] = user_input.strip()
                    context.user_data["awaiting_trimmer"] = "trimmer2_duration"
                    await update.message.reply_text(
                        "📥 Start time saved. Now send DURATION (HH:MM:SS[.ms] or seconds)\nExample: 00:10:00"
                    )
                    return

                elif mode == "trimmer2_duration":
                    start = context.user_data.get("trimmer_start")
                    if not start:
                        await update.message.reply_text("❌ Missing start time. Please restart Trimmer.")
                        for k in list(context.user_data.keys()):
                            if k.startswith("awaiting_") or k.startswith("trimmer_"):
                                del context.user_data[k]
                        return

                    try:
                        start_s = _parse_time_to_seconds(start)
                        dur_s = _parse_time_to_seconds(user_input)
                        end_s = start_s + dur_s
                        end_str = _format_seconds_to_hhmmss(end_s)
                    except ValueError:
                        await update.message.reply_text("❌ Invalid duration format. Use HH:MM:SS or seconds.")
                        return

                    await update.message.reply_text(
                        f"✂️ Trimming from {start} for duration {user_input} (to {end_str})..."
                    )
                    output_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
                    with contextlib.suppress(OSError):
                        os.makedirs(output_base, exist_ok=True)
                    output_path = os.path.join(
                        output_base, f"{current_file['id']}_trim_{int(start_s)}_{int(end_s)}.mp4"
                    )
                    success = await self.converter.trim_video(current_file["path"], output_path, start, end_str)

                    if success and os.path.exists(output_path):
                        await self._send_video_result(
                            context.bot,
                            update.effective_chat.id,
                            output_path,
                            caption=_metadata_caption(current_file),
                        )
                        os.remove(output_path)
                    else:
                        await update.message.reply_text("❌ Failed to trim video.")

                    # Cleanup
                    for k in list(context.user_data.keys()):
                        if k.startswith("awaiting_") or k.startswith("trimmer_") or k.startswith("trimmer"):
                            del context.user_data[k]
                    return
            except ValueError as e:
                await update.message.reply_text(str(e))
                return

        # Check what we're waiting for
        if context.user_data.get("awaiting_settings"):
            if user_settings is None:
                await update.message.reply_text("⚠️ Settings backend not available.")
            else:
                cmd = user_input.strip()
                lower = cmd.lower()
                try:
                    if lower.startswith("set prefix:"):
                        val = cmd.split(":", 1)[1].strip()
                        user_settings.set_user_setting(user_id, "prefix", val)
                        await update.message.reply_text(f"✅ Prefix set to: {val}")
                    elif lower.startswith("set suffix:"):
                        val = cmd.split(":", 1)[1].strip()
                        user_settings.set_user_setting(user_id, "suffix", val)
                        await update.message.reply_text(f"✅ Suffix set to: {val}")
                    elif lower.startswith("set upload_mode:"):
                        val = cmd.split(":", 1)[1].strip().lower()
                        if val in ("video", "file", "zip"):
                            user_settings.set_user_setting(user_id, "upload_mode", val)
                            await update.message.reply_text(f"✅ Upload mode set to: {val}")
                        else:
                            await update.message.reply_text("❌ Invalid upload_mode. Choose video|file|zip")
                    elif lower == "toggle save_thumbnail":
                        cur = user_settings.get_user_settings(user_id).get("save_thumbnail", False)
                        user_settings.set_user_setting(user_id, "save_thumbnail", not cur)
                        await update.message.reply_text(f"✅ save_thumbnail set to: {not cur}")
                    elif lower.startswith("set thumb_url:"):
                        val = cmd.split(":", 1)[1].strip()
                        user_settings.set_user_setting(user_id, "default_thumbnail", val)
                        user_settings.set_user_setting(user_id, "save_thumbnail", True)
                        await update.message.reply_text("✅ Default thumbnail saved.")
                    elif lower == "clear_thumb":
                        user_settings.set_user_setting(user_id, "default_thumbnail", None)
                        user_settings.set_user_setting(user_id, "save_thumbnail", False)
                        await update.message.reply_text("✅ Default thumbnail cleared.")
                    elif lower.startswith("add_word:"):
                        word = cmd.split(":", 1)[1].strip()
                        s = user_settings.get_user_settings(user_id)
                        words = list(s.get("words_remove") or [])
                        if word and word not in words:
                            words.append(word)
                            user_settings.set_user_setting(user_id, "words_remove", words)
                            await update.message.reply_text(f"✅ Added word to remove: {word}")
                        else:
                            await update.message.reply_text("⚠️ Word empty or already present.")
                    elif lower.startswith("remove_word:"):
                        word = cmd.split(":", 1)[1].strip()
                        s = user_settings.get_user_settings(user_id)
                        words = list(s.get("words_remove") or [])
                        if word in words:
                            words.remove(word)
                            user_settings.set_user_setting(user_id, "words_remove", words)
                            await update.message.reply_text(f"✅ Removed word: {word}")
                        else:
                            await update.message.reply_text("⚠️ Word not found in list.")
                    elif lower == "list_words":
                        s = user_settings.get_user_settings(user_id)
                        words = s.get("words_remove") or []
                        await update.message.reply_text("Words to remove: " + (", ".join(words) if words else "(none)"))
                    elif lower == "clear_words":
                        user_settings.set_user_setting(user_id, "words_remove", [])
                        await update.message.reply_text("✅ Cleared words remover list.")
                    else:
                        await update.message.reply_text(
                            "❓ Unknown settings command. Send /usersettings for instructions."
                        )
                except Exception:
                    await update.message.reply_text("⚠️ Failed to update settings.")

            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        elif context.user_data.get("awaiting_crf"):
            if user_input.isdigit() and 18 <= int(user_input) <= 51:
                await self.compress_video(update, context, session, user_input)
            else:
                await update.message.reply_text("❌ Invalid CRF. Enter 18-51.")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_bulk_bitrate"):
            # Custom Extract Audio bitrate for the next ▶️ Apply Bulk.
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
            _bitrate = _sanitize_audio_bitrate(user_input, default="")
            if not _bitrate:
                await update.message.reply_text(
                    f"❌ Invalid bitrate. Use a value between {_AUDIO_BITRATE_MIN_KBPS}k and"
                    f" {_AUDIO_BITRATE_MAX_KBPS}k (e.g. 128k)."
                )
            else:
                _write_bulk_setting(user_id, session, "bulk_extract_bitrate", _bitrate)
                await update.message.reply_text(f"✅ Bulk Extract Audio bitrate set to {_bitrate}.")

        elif context.user_data.get("awaiting_bulk_crf"):
            # Custom compress quality for the next ▶️ Apply Bulk.
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
            _crf = _parse_bulk_crf(user_input)
            if _crf is None:
                await update.message.reply_text(
                    f"❌ Invalid CRF. Enter {_BULK_COMPRESS_CRF_MIN}-{_BULK_COMPRESS_CRF_MAX}."
                )
            else:
                _write_bulk_setting(user_id, session, "bulk_crf", _crf)
                await update.message.reply_text(f"✅ Bulk compress CRF set to {_crf}.")

        elif context.user_data.get("awaiting_resolution"):
            if "x" in user_input:
                try:
                    width, height = map(int, user_input.split("x"))
                    await update.message.reply_text(f"📐 Changing resolution to {width}x{height}...")

                    output_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
                    with contextlib.suppress(OSError):
                        os.makedirs(output_base, exist_ok=True)
                    output_path = os.path.join(output_base, f"{current_file['id']}_{width}x{height}.mp4")
                    success = await self.converter.change_resolution(current_file["path"], output_path, width, height)

                    if success and os.path.exists(output_path):
                        await self._send_video_result(
                            context.bot,
                            update.effective_chat.id,
                            output_path,
                            caption=_metadata_caption(current_file),
                        )
                        os.remove(output_path)
                    else:
                        await update.message.reply_text("❌ Failed to change resolution.")
                except Exception:
                    logger.exception("Invalid resolution input while parsing WIDTHxHEIGHT")
                    await update.message.reply_text("❌ Invalid format. Use WIDTHxHEIGHT.")
            else:
                await update.message.reply_text("❌ Invalid format. Use WIDTHxHEIGHT.")

            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        elif context.user_data.get("awaiting_caption"):
            # Store caption in session and confirm
            if not current_file:
                await update.message.reply_text("❌ No file in session.")
            else:
                session["current_file"]["caption"] = user_input
                await update.message.reply_text("✅ Caption saved.")
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        elif context.user_data.get("awaiting_rename"):
            if not current_file:
                await update.message.reply_text("❌ No file in session.")
            else:
                # Only change stored name, do not move files on disk here
                session["current_file"]["name"] = user_input
                await update.message.reply_text(f"✅ Filename set to: {user_input}")
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        elif context.user_data.get("awaiting_split"):
            if not current_file or current_file.get("type") != "video":
                await update.message.reply_text("❌ No video available to split.")
            else:
                # Basic placeholder: accept 'start-end' or integer parts
                try:
                    if "-" in user_input:
                        start_s, end_s = user_input.split("-", 1)
                        start = float(start_s.strip())
                        end = float(end_s.strip())
                        await update.message.reply_text(
                            f"✅ Split request queued for {start}s to {end}s. Processing..."
                        )
                        # Try to call converter.split if available
                        try:
                            output_base = (
                                getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
                            )
                            with contextlib.suppress(OSError):
                                os.makedirs(output_base, exist_ok=True)
                            out = os.path.join(output_base, f"{current_file['id']}_split_{int(start)}_{int(end)}.mp4")
                            if hasattr(self.converter, "split_video"):
                                success = await self.converter.split_video(current_file["path"], start, end, out)
                                if success and os.path.exists(out):
                                    await self._send_video_result(
                                        context.bot,
                                        update.effective_chat.id,
                                        out,
                                        caption=_metadata_caption(current_file),
                                    )
                                    os.remove(out)
                                else:
                                    await update.message.reply_text("⚠️ Split finished but no file produced.")
                        except Exception:
                            logger.exception("split_video failed")
                    else:
                        await update.message.reply_text(
                            "⚠️ Split-into-equal-parts is not yet implemented. "
                            "Use a range like `00:10-00:20` (start-end in HH:MM:SS) instead."
                        )
                except Exception:
                    await update.message.reply_text(
                        "❌ Invalid split format. Use 'start-end' or an integer number of parts."
                    )
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        elif context.user_data.get("awaiting_forward_to"):
            if not current_file:
                await update.message.reply_text("❌ No file to forward.")
            else:
                target = user_input.strip()
                path = current_file.get("path")
                if not path or not os.path.exists(path):
                    await update.message.reply_text("❌ Source file not available on disk.")
                else:
                    # Resolve & validate target chat (username or id)
                    try:
                        # Normalize username (allow with or without @)
                        if target.startswith("@"):
                            lookup = target
                        else:
                            # try integer id first
                            try:
                                lookup = int(target)
                            except Exception:
                                lookup = target

                        # This will raise if bot cannot access the chat or it's invalid
                        dest_chat = await context.bot.get_chat(lookup)
                    except Exception as e:
                        logger.warning("Invalid forward target or inaccessible chat: %s", e)
                        await update.message.reply_text(
                            "❌ Invalid target or bot cannot access that chat/user. "
                            "Provide a numeric chat id or ensure the user has started the bot (use @username)."
                        )
                        for key in list(context.user_data.keys()):
                            if key.startswith("awaiting_"):
                                del context.user_data[key]
                        return

                    # Try sending with validation and robust error handling
                    try:
                        # Choose send method; document is a safer fallback for large files
                        caption = current_file.get("caption", "")

                        if current_file.get("type") == "video":
                            # Prefer send_video; fallback to send_document on failure
                            try:
                                await self._send_video_result(
                                    context.bot,
                                    dest_chat.id,
                                    path,
                                    caption=caption,
                                )
                            except Exception:
                                logger.exception("send_video failed, trying send_document as fallback")
                                with open(path, "rb") as f:
                                    await context.bot.send_document(chat_id=dest_chat.id, document=f, caption=caption)

                        elif current_file.get("type") == "audio":
                            try:
                                with open(path, "rb") as f:
                                    await context.bot.send_audio(chat_id=dest_chat.id, audio=f, caption=caption)
                            except Exception:
                                logger.exception("send_audio failed, trying send_document as fallback")
                                with open(path, "rb") as f:
                                    await context.bot.send_document(chat_id=dest_chat.id, document=f, caption=caption)

                        else:
                            with open(path, "rb") as f:
                                await context.bot.send_document(chat_id=dest_chat.id, document=f, caption=caption)

                        await update.message.reply_text("✅ Forwarded file successfully.")
                    except Exception as e:
                        logger.exception("Failed to forward file to %s: %s", getattr(dest_chat, "id", lookup), e)
                        await update.message.reply_text(f"❌ Failed to forward: {e}")

            # Clear awaiting flag regardless of outcome to avoid stuck state
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
            else:
                await update.message.reply_text("❌ Invalid format. Use WIDTHxHEIGHT.")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_trim"):
            # Handle trim time input (audio or video).
            # NOTE: the awaiting flag must survive the start -> end transition,
            # otherwise the second reply never reaches this branch.
            context.user_data["trim_time"] = user_input
            if context.user_data["awaiting_trim"] == "start":
                context.user_data["start_time"] = user_input.strip()
                context.user_data["awaiting_trim"] = "end"
                await update.message.reply_text(
                    "📥 Start time saved. Now send the END time (HH:MM:SS[.ms])\nExample: 00:01:30"
                )
                return

            # Perform trim
            start_time = context.user_data.get("start_time", "00:00:00")
            end_time = user_input

            if not current_file or not current_file.get("path"):
                await update.message.reply_text("❌ No file available to trim.")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                return

            await update.message.reply_text(f"✂️ Trimming from {start_time} to {end_time}...")

            if not await self._check_conversion_quota(update, context):
                return

            _is_audio_trim = current_file.get("type") == "audio"
            _trim_ext = os.path.splitext(current_file.get("name") or "")[1].lower()
            if _trim_ext not in (self.converter.supported_formats["audio"] if _is_audio_trim else []):
                _trim_ext = ".mp3" if _is_audio_trim else ".mp4"

            output_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
            with contextlib.suppress(OSError):
                os.makedirs(output_base, exist_ok=True)
            output_path = os.path.join(output_base, f"{current_file['id']}_trimmed{_trim_ext}")
            # trim_video delegates to the shared trim_media implementation, which
            # stream-copies either container, so it is safe for audio too.
            success = await self.converter.trim_video(current_file["path"], output_path, start_time, end_time)

            if success and os.path.exists(output_path):
                if _is_audio_trim:
                    delivery_name = _audio_delivery_name(
                        current_file.get("name"), current_file.get("id"), extension=_trim_ext
                    )
                    with open(output_path, "rb") as audio_file:
                        await context.bot.send_audio(
                            chat_id=update.effective_chat.id,
                            audio=audio_file,
                            caption=_metadata_caption(current_file),
                            title=os.path.splitext(delivery_name)[0],
                            filename=delivery_name,
                            performer="",
                        )
                else:
                    await self._send_video_result(
                        context.bot,
                        update.effective_chat.id,
                        output_path,
                        caption=_metadata_caption(current_file),
                    )
                os.remove(output_path)
            else:
                await update.message.reply_text("❌ Failed to trim media.")
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        elif context.user_data.get("awaiting_screenshot_time"):
            # Handle screenshot time
            await update.message.reply_text(f"🖼️ Taking screenshot at {user_input}...")

            output_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
            with contextlib.suppress(OSError):
                os.makedirs(output_base, exist_ok=True)
            output_path = os.path.join(output_base, f"{current_file['id']}_screenshot.jpg")
            success = await self.converter.take_screenshot_at_time(current_file["path"], output_path, user_input)

            if success and os.path.exists(output_path):
                with open(output_path, "rb") as photo_file:
                    await context.bot.send_photo(
                        chat_id=update.effective_chat.id,
                        photo=photo_file,
                        caption=_metadata_caption(current_file),
                    )
                os.remove(output_path)
            else:
                await update.message.reply_text("❌ Failed to take screenshot.")

        elif context.user_data.get("awaiting_screenshot_count"):
            # Handle multiple screenshots
            if user_input.isdigit() and 2 <= int(user_input) <= 20:
                count = int(user_input)
                await update.message.reply_text(f"🖼️ Taking {count} screenshots...")

                screenshots = await self.converter.take_screenshot_grid(
                    current_file["path"],
                    os.path.join(output_base, f"{current_file['id']}_grid"),
                    count,
                )

                if screenshots:
                    # Send as album
                    media_group = []
                    for i, screenshot_path in enumerate(screenshots):
                        with open(screenshot_path, "rb") as photo_file:
                            media_group.append(
                                InputMediaPhoto(
                                    photo_file,
                                    caption=(f"Screenshot {i + 1}" if i == 0 else ""),
                                )
                            )

                    await context.bot.send_media_group(chat_id=update.effective_chat.id, media=media_group)

                    # Cleanup
                    for screenshot_path in screenshots:
                        if os.path.exists(screenshot_path):
                            os.remove(screenshot_path)
                else:
                    await update.message.reply_text("❌ Failed to take screenshots.")
            else:
                await update.message.reply_text("❌ Enter number 2-20.")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_framerate"):
            # Handle custom framerate input
            try:
                fps = float(user_input)
                await update.message.reply_text(f"⏱️ Changing framerate to {fps} fps...")
                output_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
                with contextlib.suppress(OSError):
                    os.makedirs(output_base, exist_ok=True)
                output_path = os.path.join(output_base, f"{current_file['id']}_fr_{int(fps)}.mp4")
                success = await self.converter.change_framerate(current_file["path"], output_path, fps)

                if success and os.path.exists(output_path):
                    await self._send_video_result(
                        context.bot,
                        update.effective_chat.id,
                        output_path,
                        caption=_metadata_caption(current_file),
                    )
                    os.remove(output_path)
                else:
                    await update.message.reply_text("❌ Failed to change framerate.")
            except Exception:
                await update.message.reply_text("❌ Invalid FPS value. Use a number like 24 or 29.97.")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_mp3_bitrate"):
            # Handle a custom bitrate for video -> MP3 extraction
            parsed = _sanitize_audio_bitrate(user_input, default="")
            if parsed:
                await self.convert_to_mp3(update, context, session, bitrate=parsed)
            else:
                await update.message.reply_text("❌ Invalid bitrate. Use a value between 32k and 320k (e.g. 128k).")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_bitrate"):
            # Handle custom bitrate
            parsed = _sanitize_audio_bitrate(user_input, default="")
            if parsed:
                await self.adjust_bitrate(update, context, session, parsed)
            else:
                await update.message.reply_text("❌ Invalid bitrate. Use a value between 32k and 320k (e.g. 128k).")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_optimize"):
            # Handle custom optimization
            try:
                preset, crf, bitrate = user_input.split(",")
                await update.message.reply_text(f"⚡ Optimizing with preset={preset}, crf={crf}, bitrate={bitrate}...")

                output_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
                with contextlib.suppress(OSError):
                    os.makedirs(output_base, exist_ok=True)
                output_path = os.path.join(output_base, f"{current_file['id']}_optimized.mp4")
                cmd = [
                    "-c:v",
                    "libx264",
                    "-preset",
                    preset.strip(),
                    "-crf",
                    crf.strip(),
                    "-c:a",
                    "aac",
                    "-b:a",
                    bitrate.strip(),
                    "-movflags",
                    "+faststart",
                ]

                success, _ = await self.converter.execute_ffmpeg(cmd, current_file["path"], output_path)

                if success and os.path.exists(output_path):
                    await self._send_video_result(
                        context.bot,
                        update.effective_chat.id,
                        output_path,
                        caption=_metadata_caption(current_file),
                    )
                    os.remove(output_path)
                else:
                    await update.message.reply_text("❌ Optimization failed.")
            except Exception:
                logger.exception("Invalid custom optimize input; expected preset,crf,bitrate")
                await update.message.reply_text("❌ Invalid format. Use: preset,crf,bitrate")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_metadata"):
            # Handle metadata JSON
            try:
                metadata = json.loads(user_input)
                output_base = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
                with contextlib.suppress(OSError):
                    os.makedirs(output_base, exist_ok=True)
                output_path = os.path.join(output_base, f"{current_file['id']}_with_metadata.mp4")

                success = await self.converter.edit_metadata(current_file["path"], output_path, metadata)

                if success and os.path.exists(output_path):
                    await self._send_video_result(
                        context.bot,
                        update.effective_chat.id,
                        output_path,
                        caption=_metadata_caption(current_file),
                    )
                    os.remove(output_path)
                else:
                    await update.message.reply_text("❌ Failed to update metadata.")
            except json.JSONDecodeError:
                await update.message.reply_text("❌ Invalid JSON format.")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        # Clear context
        for key in list(context.user_data.keys()):
            if key.startswith("awaiting_"):
                del context.user_data[key]

        return ConversationHandler.END
