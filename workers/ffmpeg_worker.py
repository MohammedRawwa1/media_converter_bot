"""FFmpeg worker: consumes Redis job queue, runs ffmpeg via ffmpeg_runner,
persists job state to MongoDB (if available), and exposes Prometheus metrics.
"""

import asyncio
import logging
import os
import signal
import subprocess
import sys
import time

from utils.ffmpeg_runner import run_ffmpeg
from utils.job_queue import JOB_LIST, close_redis, get_redis, pop_job, publish_update, release_input_lock

try:
    from utils.cache import get_cache
except Exception:
    get_cache = None
import contextlib
import hashlib
import json
import shutil
import tempfile

import aiohttp
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from telegram import Bot

import config
from tasks import (
    create_archive,
    create_slideshow,
    extract_streams,
    generate_sample,
    merge_audios,
    merge_videos,
    trim_media,
)
from utils import batch_pipeline, deferred_delivery, eventbus, file_utils, job_store
from utils.eventbus import (
    JOB_CANCELLED,
    JOB_COMPLETED,
    JOB_FAILED,
    JOB_STARTED,
    emit_event,
    is_rabbitmq_job,
)
from utils.file_utils import safe_rmtree
from utils.rate_limiter import telegram_edit_coalescer, telegram_flood_gate

logger = logging.getLogger(__name__)

# Every Bot this worker builds sends through the shared flood gate, so a window
# earned here (or by the bot process) stops the write before it costs a 429.
try:
    from utils.telegram_flood_request import flood_gated_request
except Exception:  # pragma: no cover - PTB request layer unavailable

    def flood_gated_request(**kwargs):
        return None


try:
    from utils.storage import get_storage_backend
except Exception:
    get_storage_backend = None

try:
    from utils.storage import record_source_cache
except Exception:  # pragma: no cover - accounting is optional, never fatal

    async def record_source_cache(*_args, **_kwargs):
        return {}


try:
    from utils.rate_limiter import ConversionRateLimiterRedis

    _conv_limiter = ConversionRateLimiterRedis(conversions_per_hour=int(os.environ.get("CONVERSIONS_PER_HOUR", "360")))
except Exception:
    _conv_limiter = None

# Prometheus metrics
METRICS_PORT = int(os.environ.get("PROMETHEUS_METRICS_PORT", "8000"))
JOBS_TOTAL = Counter("media_jobs_total", "Total ffmpeg jobs processed")
JOBS_FAILED = Counter("media_jobs_failed", "Total ffmpeg jobs failed")
JOBS_SUCCEEDED = Counter("media_jobs_succeeded", "Total ffmpeg jobs succeeded")
JOB_DURATION = Histogram("media_job_duration_seconds", "Duration of ffmpeg jobs")
ACTIVE_JOBS = Gauge("media_jobs_active", "Number of active ffmpeg jobs")
LOCKS_CLEANED = Counter("media_locks_cleaned_total", "Input locks released (cleaned up) after job completion")
# Resident memory sampled after each job's cleanup. The drop between jobs is the
# signal that the "finish -> clean -> next" contract is actually working.
WORKER_RSS = Gauge("media_worker_rss_bytes", "Worker resident memory after the last job cleanup")

# How often an idle worker republishes its RSS to Redis for the dashboard's
# capacity view. Tied to the heartbeat TTL so a live worker never looks stale.
_RSS_HEARTBEAT_SECONDS = max(5.0, batch_pipeline.WORKER_RSS_TTL_SECONDS / 3.0)
_last_rss_publish = 0.0

# Jobs this process is processing right now. A restart must not cut one short,
# so the loop only exits when this is zero - which also covers a job running on
# the RabbitMQ consumer while the Redis loop sits idle.
_jobs_in_flight = 0
# How often an idle worker looks for an admin-requested restart. Cheap, but not
# on every 0.2s poll: a restart is not a sub-second operation.
_RESTART_PROBE_SECONDS = 10.0
_last_restart_probe = 0.0

# Forward notification event (set by background pubsub listener)
FORWARD_NOTIFY_EVENT: asyncio.Event | None = None
# Redis cache instance shared between worker_loop and handle_job
_cache = None
LAST_FORWARD_NOTIFICATION: dict | None = None


async def _get_cached_file_id(
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
        from utils import file_id_cache

        return await file_id_cache.get_file_id(
            media_type,
            file_unique_id=file_unique_id,
            file_path=file_path,
        )
    except Exception:
        logger.debug("ffmpeg_worker: file_id cache lookup failed, will upload fresh")
        return None


async def _cache_file_id(
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
        from utils import file_id_cache

        return await file_id_cache.store_file_id(
            media_type,
            file_id,
            file_unique_id=file_unique_id,
            file_path=file_path,
        )
    except Exception:
        logger.debug("ffmpeg_worker: file_id cache store failed, will re-upload next time")
        return False


def _is_stale_file_id(error) -> bool:
    """Whether *error* means Telegram refused the file_id itself.

    Tells the two reactions apart: drop the token and upload fresh (it is dead),
    or leave the cache alone and let the failure surface (a flood wait or a
    network error was never the token's fault).
    """
    try:
        from utils import file_id_cache

        return bool(file_id_cache.is_stale_file_id(error))
    except Exception:
        return False


async def _forget_cached_file_id(
    media_type: str,
    *,
    file_unique_id: str | None = None,
    file_path: str | None = None,
) -> None:
    """Drop a file_id Telegram refused, so the next delivery uploads fresh.

    A cached file_id is only a saving while a dead one costs a retry: without
    this, a revoked token makes every delivery of that media fail, and the more
    durable the cache the more chances there are to hit one.
    """
    try:
        from utils import file_id_cache

        await file_id_cache.invalidate_file_id(
            media_type,
            file_unique_id=file_unique_id,
            file_path=file_path,
        )
        logger.info(
            "Worker: dropped the refused %s file_id; the next delivery uploads fresh",
            media_type,
        )
    except Exception:
        logger.debug("ffmpeg_worker: could not invalidate a refused %s file_id", media_type)


# Cache for output probe results to avoid double ffprobe/thumbnail generation.
# Keyed by output file path; values are (video_meta, thumb_path).
_output_probe_cache: dict[str, tuple[dict | None, str | None]] = {}

# How long one source download from storage may take before it is abandoned.
#
# A single fixed number cannot fit both a 20 MB clip and a 2 GB video, so the
# bound grows with the size the job already records: at least
# STORAGE_DOWNLOAD_TIMEOUT_SECONDS, or one second per
# STORAGE_DOWNLOAD_MIN_BYTES_PER_SECOND bytes, capped at
# STORAGE_DOWNLOAD_MAX_SECONDS. Bounded on purpose - this transfer had no bound
# at all, so a stalled one held the batch lock and the only conversion slot for
# good, freezing every later member of its batch with nothing reported anywhere.
STORAGE_DOWNLOAD_TIMEOUT_SECONDS = float(os.environ.get("STORAGE_DOWNLOAD_TIMEOUT_SECONDS", "900"))
STORAGE_DOWNLOAD_MIN_BYTES_PER_SECOND = float(os.environ.get("STORAGE_DOWNLOAD_MIN_BYTES_PER_SECOND", str(256 * 1024)))
STORAGE_DOWNLOAD_MAX_SECONDS = float(os.environ.get("STORAGE_DOWNLOAD_MAX_SECONDS", str(6 * 3600)))
# How long the header-only range GET may take before it is abandoned. The probe
# is advisory - it can only ever produce a warning - so it must never be able to
# hold the worker's one conversion slot (and its batch's lock) open.
STORAGE_PROBE_TIMEOUT_SECONDS = float(os.environ.get("STORAGE_PROBE_TIMEOUT_SECONDS", "60"))


def _storage_download_timeout_seconds(size_bytes) -> float:
    """The bound for one source download, derived from the size it should carry."""
    try:
        size = float(size_bytes or 0)
    except (TypeError, ValueError):
        size = 0.0
    if size <= 0 or STORAGE_DOWNLOAD_MIN_BYTES_PER_SECOND <= 0:
        return STORAGE_DOWNLOAD_TIMEOUT_SECONDS
    return min(
        STORAGE_DOWNLOAD_MAX_SECONDS,
        max(STORAGE_DOWNLOAD_TIMEOUT_SECONDS, size / STORAGE_DOWNLOAD_MIN_BYTES_PER_SECOND),
    )


def _job_source_bytes(job: dict) -> int:
    """The source size a job recorded, or 0 when it did not record one."""
    try:
        return max(0, int(job.get("file_size") or 0))
    except (TypeError, ValueError):
        return 0


async def _stored_source_for_job(job: dict) -> str | None:
    """A validated whole-object key for this job's media, when one exists.

    A ``header``-mode job is handed no object at all - the media is meant to come
    from Telegram - so when that copy is unreadable (a relay copy that was pruned,
    or a forward that failed and left coordinates a userbot cannot reach) there is
    nothing on the job itself left to fall back to. The media registry is keyed on
    the Telegram identity, which the job does carry, so it can still name a stored
    copy: any producer may have written this media whole - the Bot-API path,
    ``full``/``stream`` mode, or a promote-on-repeat.

    Only an object that still exists *and* matches the job's recorded size is
    adopted. A stale descriptor must not turn into a job fed the wrong bytes, so
    the same evidence rules apply here as in the ingest gate.
    """
    file_unique_id = job.get("file_unique_id")
    if not file_unique_id or get_storage_backend is None:
        return None
    expected = _job_source_bytes(job) or None
    try:
        from utils import media_cache
        from utils.storage import stored_object_is_intact

        entry = await media_cache.lookup(file_unique_id, expected_size=expected)
        key = (entry or {}).get("input_key")
        if not key:
            return None
        backend = await get_storage_backend()
        if backend is None:
            return None
        if not await stored_object_is_intact(backend, key, expected_size=expected):
            return None
    except Exception:
        logger.debug("ffmpeg worker: no stored copy to recover for job %s", job.get("job_id"))
        return None
    return key


async def _check_upload_cancelled(job_id: str) -> bool:
    """Quick Redis check: return True if this job has been cancelled."""
    if not job_id:
        return False
    try:
        r = await get_redis()
        try:
            cancel_val = await r.hget(f"ffmpeg:job:{job_id}", "cancel")
            return cancel_val is not None and cancel_val in (b"1", "1")
        finally:
            with contextlib.suppress(Exception):
                await r.close()
    except Exception:
        return False


async def _update_upload_progress(job_id: str, progress_channel: str, pct: int, message: str) -> None:
    """Update Redis job hash and publish progress for Telegram upload."""
    try:
        r = await get_redis()
        try:
            await r.hset(
                f"ffmpeg:job:{job_id}",
                mapping={
                    "progress": str(pct),
                    "message": message,
                    "status": "uploading" if pct < 100 else "sending",
                },
            )
            await publish_update(
                progress_channel,
                {
                    "job_id": job_id,
                    "progress": pct,
                    "message": message,
                },
            )
        finally:
            with contextlib.suppress(Exception):
                await r.close()
    except Exception:
        logger.debug("ffmpeg worker: operation failed")


async def _set_job_state(
    job_id: str, status: str, message: str, *, progress: int | None = None, channel: str | None = None
) -> None:
    """Write a job's state where the bot's watchers actually read it: the hash.

    ``_watch_job_progress`` in the bot and the bulk apply's wait both poll
    ``ffmpeg:job:<id>`` in Redis. A state that is only published on the progress
    channel, or only written to Mongo, is invisible to them - which is how a job
    could end in failure while its watchdog kept showing the last non-terminal
    text and its batch waited out the full ``BULK_JOB_WAIT_SECONDS`` budget for
    work that was already over.
    """
    if not job_id:
        return
    mapping = {"status": str(status), "message": str(message)}
    if progress is not None:
        mapping["progress"] = str(progress)
    try:
        r = await get_redis()
        try:
            await r.hset(f"ffmpeg:job:{job_id}", mapping=mapping)
        finally:
            with contextlib.suppress(Exception):
                await r.close()
    except Exception:
        logger.debug("ffmpeg worker: could not write the status of job %s", job_id)
    if channel:
        with contextlib.suppress(Exception):
            await publish_update(
                channel,
                {
                    "job_id": job_id,
                    "progress": progress if progress is not None else 0,
                    "status": str(status),
                    "message": str(message),
                },
            )


def _make_upload_progress_callback(job_id: str, progress_channel: str):
    """Create a throttled sync progress callback for userbot uploads.

    Returns a callable(sent_bytes, total_bytes) suitable for both
    Telethon's progress_callback and Pyrogram's progress parameter.
    Updates are throttled to at most once per second or when the
    percentage changes.
    """
    _last_pct = [-1]
    _last_update = [0.0]
    _interval = 1.0

    def _progress(sent_bytes: int, total_bytes: int) -> None:
        try:
            if total_bytes <= 0:
                return
            pct = min(int(sent_bytes * 100 / total_bytes), 100)
            now = time.time()
            if pct != _last_pct[0] or (now - _last_update[0]) >= _interval:
                _last_pct[0] = pct
                _last_update[0] = now
                mb_sent = sent_bytes // (1024 * 1024)
                mb_total = total_bytes // (1024 * 1024)
                msg = f"Uploading to Telegram: {pct}% ({mb_sent}MB / {mb_total}MB)"

                # Cancel check: every ~5% bucket, check Redis cancel flag
                # Sync redis.from_url() call in this sync callback.
                if pct % 5 == 0 or pct == 100:
                    try:
                        import redis as _redis

                        _redis_url = os.environ.get("REDIS_URL")
                        if _redis_url:
                            _rr = _redis.from_url(_redis_url, socket_timeout=2)
                            try:
                                _cancel_val = _rr.hget(f"ffmpeg:job:{job_id}", "cancel")
                                if _cancel_val == b"1":
                                    raise asyncio.CancelledError("Upload cancelled by user")
                            finally:
                                _rr.close()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass

                try:
                    loop = asyncio.get_running_loop()
                    asyncio.run_coroutine_threadsafe(
                        _update_upload_progress(job_id, progress_channel, pct, msg),
                        loop,
                    )
                except Exception:
                    logger.debug("ffmpeg worker: operation failed")
        except Exception:
            logger.debug("ffmpeg worker: in _progress()")

    return _progress


class _ProgressFileWrapper:
    """Wraps a file-like object and calls a progress callback as bytes are read.

    The Bot API (python-telegram-bot via httpx) reads from the file handle in
    chunks during multipart upload. This wrapper intercepts those reads and
    tracks progress so we can show upload status in the Telegram progress message.
    """

    def __init__(self, fh, total_size: int, progress_callback):
        self._fh = fh
        self._total = total_size
        self._sent = 0
        self._progress_callback = progress_callback

    def read(self, size: int = -1):
        chunk = self._fh.read(size)
        if chunk:
            self._sent += len(chunk)
            if self._progress_callback:
                with contextlib.suppress(Exception):
                    self._progress_callback(self._sent, self._total)
        return chunk

    def __getattr__(self, name):
        return getattr(self._fh, name)


async def _send_video_result(
    bot,
    chat_id: int,
    file_path: str,
    caption: str = "",
    *,
    file_size: int = 0,
    progress_channel: str | None = None,
    job_id: str | None = None,
    thumb_path: str | None = None,
    _temp_thumb: str | None = None,
    vid_duration: int | None = None,
    vid_width: int | None = None,
    vid_height: int | None = None,
    file_unique_id: str | None = None,
) -> str | None:
    """Open a video file, wrap with upload progress, and send_video with metadata.

    Returns the Telegram file_id of the sent video, or None on failure.
    The file_id is cached for reuse on subsequent sends of the same media.

    Args:
        file_unique_id: Optional stable identifier for the video content.
            If provided, the resulting file_id will be cached for reuse,
            avoiding repeated uploads to Telegram (and the associated IDrive egress).
    """
    _bot_up_cb = _make_upload_progress_callback(job_id, progress_channel) if job_id and progress_channel else None
    _cleanup_thumb = _temp_thumb
    _sent_file_id = None
    try:
        # Try to use cached file_id first (if we have a content identity)
        if file_unique_id:
            _cached_file_id = await _get_cached_file_id("video", file_unique_id=file_unique_id)
            if _cached_file_id:
                logger.info(
                    "Worker: using cached file_id for video (chat_id=%s), avoiding re-upload",
                    chat_id,
                )
                _send_kwargs = {
                    "chat_id": chat_id,
                    "video": _cached_file_id,
                    "caption": caption,
                    "supports_streaming": True,
                }
                if vid_duration is not None:
                    _send_kwargs["duration"] = vid_duration
                if vid_width is not None:
                    _send_kwargs["width"] = vid_width
                if vid_height is not None:
                    _send_kwargs["height"] = vid_height
                try:
                    if thumb_path:
                        try:
                            with open(thumb_path, "rb") as _tf:
                                _send_kwargs["thumbnail"] = _tf
                                _msg = await bot.send_video(**_send_kwargs)
                        except Exception:
                            _send_kwargs.pop("thumbnail", None)
                            _msg = await bot.send_video(**_send_kwargs)
                    else:
                        _msg = await bot.send_video(**_send_kwargs)
                except Exception as _cached_exc:
                    if not _is_stale_file_id(_cached_exc):
                        raise
                    # The token is dead: drop it and fall through to a real
                    # upload, so caching can never cost a delivery.
                    await _forget_cached_file_id("video", file_unique_id=file_unique_id)
                else:
                    _sent_file_id = getattr(_msg, "video", None)
                    if _sent_file_id and hasattr(_sent_file_id, "file_id"):
                        _sent_file_id = _sent_file_id.file_id
                    return _sent_file_id

        # No cached file_id - upload the file fresh
        with open(file_path, "rb") as _fh:
            _fh = _ProgressFileWrapper(_fh, file_size, _bot_up_cb) if file_size and _bot_up_cb else _fh
            _send_kwargs = {
                "chat_id": chat_id,
                "video": _fh,
                "caption": caption,
                "supports_streaming": True,
            }
            if vid_duration is not None:
                _send_kwargs["duration"] = vid_duration
            if vid_width is not None:
                _send_kwargs["width"] = vid_width
            if vid_height is not None:
                _send_kwargs["height"] = vid_height
            logger.info(
                "Worker: sending video file=%s size=%s thumb=%s duration=%s supports_streaming=True",
                file_path,
                file_size,
                bool(thumb_path),
                _send_kwargs.get("duration"),
            )
            if thumb_path:
                try:
                    with open(thumb_path, "rb") as _tf:
                        _send_kwargs["thumbnail"] = _tf
                        _msg = await bot.send_video(**_send_kwargs)
                except Exception:
                    _send_kwargs.pop("thumbnail", None)  # Remove closed file handle before retry
                    _msg = await bot.send_video(**_send_kwargs)
            else:
                _msg = await bot.send_video(**_send_kwargs)
            _sent_file_id = getattr(_msg, "video", None)
            if _sent_file_id and hasattr(_sent_file_id, "file_id"):
                _sent_file_id = _sent_file_id.file_id

            # Cache the file_id for future reuse
            if _sent_file_id and file_unique_id:
                await _cache_file_id("video", _sent_file_id, file_unique_id=file_unique_id)

            return _sent_file_id
    finally:
        if _cleanup_thumb and os.path.exists(_cleanup_thumb):
            try:
                _d = os.path.dirname(_cleanup_thumb)
                safe_rmtree(_d)
            except RuntimeError:
                # If dirname resolved to a protected directory (shouldn't happen
                # now that thumbnails live in mkdtemp subdirectories), fall back
                # to removing only the file itself.
                with contextlib.suppress(Exception):
                    os.remove(_cleanup_thumb)
            except Exception:
                with contextlib.suppress(Exception):
                    os.remove(_cleanup_thumb)


async def _probe_output_metadata(out_path: str) -> tuple[dict | None, str | None]:
    """Probe a video file for metadata and generate a thumbnail.

    Delegates to the shared ``probe_video_for_delivery`` utility which
    extracts all metadata (dimensions, duration, codec, bitrate, fps) and
    generates a thumbnail in a single ffprobe + ffmpeg call.

    Adds caching layers: in-memory ``_output_probe_cache`` and Redis.

    Returns:
        Tuple of (video_meta dict or None, thumb_path str or None).
        ``video_meta`` has keys ``duration`` (int), ``width`` (int), ``height`` (int)
        plus optional ``video_codec``, ``video_bitrate``, ``fps``, ``audio_codec``,
        ``audio_bitrate``.
        ``thumb_path`` is a path to a JPEG thumbnail file (caller cleans up via os.remove).
    """
    if not out_path or not os.path.exists(out_path):
        return None, None

    # Return cached result immediately if available (avoids double ffprobe)
    _cached = _output_probe_cache.get(out_path)
    if _cached is not None:
        logger.debug("Worker: _probe_output_metadata cache HIT for %s", out_path)
        _cached_meta, _cached_thumb = _cached
        if _cached_thumb and not os.path.exists(_cached_thumb):
            # The caller that consumed this probe removed its thumbnail directory,
            # so hand back the metadata without a path to a file that is gone
            # rather than a dead path a delivery would try to upload.
            _cached = (_cached_meta, None)
            _output_probe_cache[out_path] = _cached
        return _cached

    # ── Delegate to shared utility (single ffprobe + thumbnail call) ──
    from utils.ffmpeg_runner import probe_video_for_delivery

    video_meta, thumb_path = await probe_video_for_delivery(out_path)

    # ── Cache result ──
    _output_probe_cache[out_path] = (video_meta, thumb_path)
    try:
        if _cache and video_meta:
            _fhash = hashlib.sha256(f"{out_path}:{os.path.getsize(out_path)}".encode()).hexdigest()[:16]
            await _cache.set(f"cache:probe_meta:{_fhash}", video_meta, ttl=86400)
    except Exception:
        logger.debug("Worker: failed to cache probe result for %s", out_path)

    return video_meta, thumb_path


async def _probe_audio_delivery(out_path: str, delivery_name: str) -> dict | None:
    """Audio tags for the Telegram music player, or ``None`` to let the uploader probe.

    Built once here, from the file the worker just encoded and the media's own
    name, so the player shows a real title/performer/duration instead of whatever
    a fresh encode happened to leave behind. The uploader only probes when
    ``audio_meta`` is ``None``, so a *partial* dict would suppress that probe and
    lose the fields it fills in - hence a complete dict or ``None``, never a stub.
    """
    if not out_path or not os.path.exists(out_path):
        return None
    try:
        from utils.userbot_uploader import probe_audio_metadata

        meta = dict(await probe_audio_metadata(out_path) or {})
    except Exception:
        # Probing is best-effort; None hands the work back to the uploader.
        return None
    # A converted file usually carries no title tag of its own, and Telegram shows
    # the media name in the player, so fall back to it exactly as the uploader does.
    if not meta.get("title"):
        meta["title"] = os.path.splitext(os.path.basename(delivery_name or out_path))[0][:64]
    return meta


# ── Deferred delivery ────────────────────────────────────────────────────────
# Delivery is the last step of a job, so a Telegram flood window landing on it
# used to cost the user the file outright: the job was finalized as "delivery
# failed" and nothing ever retried it, even though the window closes on its own
# within hours and the converted output is still on disk. A refused delivery on
# a chat whose window is open is now handed to the deferred queue
# (``utils.deferred_delivery``) and retried from here - no re-download, no second
# encode, and no re-send once somebody else has delivered it.

_DEFERRED_MESSAGE = (
    "📦 Delivery queued — Telegram is rate limiting this chat. "
    "The file will be sent automatically as soon as that clears."
)
_DEFERRED_SWEEP_SECONDS = float(os.getenv("DEFERRED_DELIVERY_SWEEP_SECONDS", "30"))
_DEFERRED_RETRY_BACKOFF_SECONDS = float(os.getenv("DEFERRED_DELIVERY_RETRY_BACKOFF_SECONDS", "120"))
_DEFERRED_SWEEP_LIMIT = int(os.getenv("DEFERRED_DELIVERY_SWEEP_LIMIT", "5"))


def _deferred_media_kind(record: dict) -> str:
    """How a deferred output has to be sent: audio, video, or a plain file."""
    name = record.get("delivery_name") or record.get("output") or ""
    try:
        from utils.userbot_uploader import is_audio_delivery_output

        if is_audio_delivery_output(name, media_kind=record.get("media_kind")):
            return "audio"
    except Exception:
        logger.debug("deferred delivery: audio detection unavailable for %s", name)
    if os.path.splitext(name)[1].lower() in (".mp4", ".mkv", ".mov", ".webm", ".avi"):
        return "video"
    return "document"


async def _defer_delivery(
    job,
    *,
    chat_id,
    output,
    delivery_name,
    media_kind,
    caption,
    reason,
    get_url=None,
    output_key=None,
) -> float:
    """Queue a refused delivery for later. Returns the window it is waiting on.

    A failure with no open window for the chat is a *real* failure - a forbidden
    chat, a file Telegram will not take - so it keeps the old
    ``delivery failed`` outcome. Only a window this bot is actually serving is
    worth waiting out, because that one closes by itself.
    """
    remaining = await telegram_flood_gate.remaining(telegram_flood_gate.scope_for_chat(chat_id))
    if remaining <= 0:
        return 0.0

    horizon = deferred_delivery.DEFAULT_HORIZON_SECONDS
    record = {
        "job_id": job.get("job_id"),
        "chat_id": chat_id,
        "output": os.path.abspath(output) if output else None,
        "output_key": output_key,
        "delivery_name": delivery_name,
        "media_kind": media_kind,
        "caption": caption,
        "link_url": get_url,
        "cleanup_output": bool(job.get("cleanup_output", False)),
        "reason": reason,
    }
    if not await deferred_delivery.defer(record, due_in=min(remaining + 1.0, horizon), horizon=horizon):
        return 0.0
    return remaining


async def _resolve_deferred_output(record: dict) -> str | None:
    """The file to send: the local output, else the copy in storage.

    A result that was going to be delivered from this container was never
    uploaded - nothing else would ever read it - so the file on disk is the only
    copy that exists and it has to still be there. The storage fallback covers
    the cases where a copy does exist (link delivery on, or a job with no chat).
    """
    path = record.get("output")
    if path and os.path.exists(path) and os.path.getsize(path) > 0:
        return path

    key = record.get("output_key")
    if not key or get_storage_backend is None:
        return None
    try:
        backend = await get_storage_backend()
        dest = os.path.join(config.TEMP_PATH, f"deferred_{record.get('job_id')}_{os.path.basename(str(key))}")
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        if await backend.download_file(key, dest) and os.path.exists(dest) and os.path.getsize(dest) > 0:
            record["_downloaded"] = dest
            return dest
    except Exception:
        logger.warning("deferred delivery: could not fetch %s for job %s", key, record.get("job_id"))
    return None


async def _send_deferred_output(record: dict, path: str) -> bool:
    """Send one deferred result. True when Telegram accepted it."""
    bot_token = getattr(config, "BOT_TOKEN", None)
    chat_id = record.get("chat_id")
    if not bot_token or not chat_id or not path:
        return False

    try:
        size = os.path.getsize(path)
    except Exception:
        size = 0
    limit = int(getattr(config, "BOT_API_MAX_BYTES", 0) or 0)
    if limit and size > limit:
        logger.warning(
            "deferred delivery: job %s output is %.1fMB, past the Bot API limit (%dMB)",
            record.get("job_id"),
            size / (1024 * 1024),
            limit // (1024 * 1024),
        )
        return False

    name = record.get("delivery_name") or os.path.basename(path)
    caption = record.get("caption") or None
    kind = _deferred_media_kind(record)
    logger.info(
        "deferred delivery: sending %s for job %s (%s, %.1fMB)",
        name,
        record.get("job_id"),
        kind,
        size / (1024 * 1024),
    )

    async with Bot(token=bot_token, request=flood_gated_request()) as bot:
        if kind == "audio":
            meta = await _probe_audio_delivery(path, name) or {}
            with open(path, "rb") as fh:
                await bot.send_audio(
                    chat_id=chat_id,
                    audio=fh,
                    caption=caption,
                    filename=name,
                    title=(meta.get("title") or os.path.splitext(name)[0])[:64],
                    performer=(meta.get("performer") or "")[:64],
                    duration=int(meta["duration"]) if meta.get("duration") else None,
                )
        elif kind == "video":
            meta, thumb = await _probe_output_metadata(path)
            kwargs: dict = {}
            for field in ("duration", "width", "height"):
                value = (meta or {}).get(field)
                if value:
                    kwargs[field] = int(value)
            if thumb:
                kwargs["thumbnail"] = thumb
            try:
                with open(path, "rb") as fh:
                    await bot.send_video(
                        chat_id=chat_id,
                        video=fh,
                        caption=caption,
                        filename=name,
                        supports_streaming=True,
                        **kwargs,
                    )
            finally:
                if thumb:
                    with contextlib.suppress(Exception):
                        os.remove(thumb)
        else:
            with open(path, "rb") as fh:
                await bot.send_document(chat_id=chat_id, document=fh, caption=caption, filename=name)
    return True


async def _finish_deferred(record: dict, *, ok: bool, message: str) -> None:
    """Record a deferred delivery's outcome and drop it from the queue.

    The job hash is what the bot's progress watcher polls, so writing the
    terminal status here is what finally renders the result: the ``processing``
    status the deferral left behind is what kept that watcher alive.
    """
    job_id = record.get("job_id")
    if not job_id:
        return

    status = "done" if ok else "error"
    with contextlib.suppress(Exception):
        r = await get_redis()
        try:
            await r.hset(
                f"ffmpeg:job:{job_id}",
                mapping={
                    "status": status,
                    "progress": "100",
                    "message": message,
                    "delivered": "1" if ok else "0",
                    "delivery_deferred": "0",
                },
            )
        finally:
            with contextlib.suppress(Exception):
                await r.close()
    with contextlib.suppress(Exception):
        await publish_update(
            f"ffmpeg:progress:{job_id}",
            {"job_id": job_id, "progress": 100, "message": message, "status": status},
        )
    with contextlib.suppress(Exception):
        await emit_event(
            JOB_COMPLETED if ok else JOB_FAILED,
            job={"job_id": job_id},
            payload={"status": status, "message": message, "progress": 100},
            source="worker",
        )

    # Free the disk the attempt was holding. A *delivered* result is removed when
    # the job asked for that; an abandoned one is deliberately kept, because a
    # failed delivery means this file is the user's only copy and deleting it is
    # what would make the failure permanent.
    if record.get("_downloaded"):
        with contextlib.suppress(Exception):
            os.remove(record["_downloaded"])
    if ok and record.get("cleanup_output") and record.get("output"):
        with contextlib.suppress(Exception):
            os.remove(record["output"])

    await deferred_delivery.clear(job_id)


async def _deliver_deferred_record(record: dict) -> str:
    """One deferred attempt: "delivered", "deferred" or "abandoned"."""
    from telegram.error import RetryAfter

    job_id = record.get("job_id")
    remaining = await telegram_flood_gate.remaining(telegram_flood_gate.scope_for_chat(record.get("chat_id")))
    if remaining > 0:
        # The window is still open - Telegram told us so on the last attempt, and
        # it may even have been extended by the other container since.
        await deferred_delivery.update(job_id, due_in=remaining + 1.0, reason="still rate limited")
        return "deferred"

    if await _job_already_delivered({"job_id": job_id}):
        # Something else got the file through (a broker redelivery, an operator's
        # requeue, a stop/resume): never send a second copy.
        await _finish_deferred(record, ok=True, message="delivered to Telegram")
        return "delivered"

    created_at = float(record.get("created_at") or 0)
    horizon = float(record.get("horizon") or deferred_delivery.DEFAULT_HORIZON_SECONDS)
    if created_at and (time.time() - created_at) > horizon:
        logger.warning(
            "deferred delivery: job %s waited past its %.0fh window; giving up (output kept at %s)",
            job_id,
            horizon / 3600,
            record.get("output"),
        )
        await _finish_deferred(
            record,
            ok=False,
            message="delivery failed: the Telegram rate limit outlasted the retry window",
        )
        return "abandoned"

    attempts = int(record.get("attempts") or 0) + 1
    path = await _resolve_deferred_output(record)
    ok = False
    if path:
        try:
            ok = await _send_deferred_output(record, path)
        except RetryAfter as exc:
            # The window reopened (or the other container found one) between the
            # check above and the send. This attempt cost nothing and the file is
            # still fine, so wait the window out instead of spending one of the
            # few attempts we allow.
            _left = await telegram_flood_gate.note(
                getattr(exc, "retry_after", None) or 5,
                telegram_flood_gate.scope_for_chat(record.get("chat_id")),
            )
            logger.info(
                "deferred delivery: job %s re-gated for %.0fs; not counting an attempt",
                job_id,
                _left,
            )
            await deferred_delivery.update(job_id, due_in=_left + 1.0, reason="re-gated")
            return "deferred"
        except Exception:
            logger.exception("deferred delivery: send failed for job %s (attempt %s)", job_id, attempts)
    if ok:
        await _finish_deferred(record, ok=True, message="delivered to Telegram (retried after rate limiting)")
        return "delivered"

    if attempts >= deferred_delivery.MAX_ATTEMPTS:
        await _finish_deferred(record, ok=False, message="delivery failed after retrying past the rate limit")
        return "abandoned"

    logger.warning(
        "deferred delivery: attempt %s/%s for job %s did not land; retrying in %.0fs",
        attempts,
        deferred_delivery.MAX_ATTEMPTS,
        job_id,
        _DEFERRED_RETRY_BACKOFF_SECONDS,
    )
    await deferred_delivery.update(
        job_id,
        due_in=_DEFERRED_RETRY_BACKOFF_SECONDS,
        attempts=attempts,
        reason="retry after a failed attempt",
    )
    return "deferred"


async def _sweep_deferred_deliveries(*, limit: int | None = None) -> dict:
    """Retry the deliveries rate limiting pushed past their send.

    Skipped while a conversion is running: this container's memory ceiling is
    about a conversion *plus* an upload, and the delivery is not going anywhere -
    the queue keeps it, and the next pass picks it up.
    """
    counts = {"delivered": 0, "deferred": 0, "abandoned": 0, "skipped": 0}
    if _jobs_in_flight > 0:
        counts["skipped"] = 1
        return counts

    records = await deferred_delivery.claim_due(limit=limit or _DEFERRED_SWEEP_LIMIT)
    for record in records:
        try:
            outcome = await _deliver_deferred_record(record)
        except Exception:
            logger.exception("deferred delivery: attempt for job %s raised", record.get("job_id"))
            outcome = "deferred"
        counts[outcome] = counts.get(outcome, 0) + 1
    return counts


async def _deferred_delivery_sweeper(stop_event: asyncio.Event | None = None) -> None:
    """Background task: keep resolving deferred deliveries while the worker runs.

    The first pass runs immediately, so a redeploy resumes whatever a previous
    container had to hand off rather than waiting for the queue to be touched.
    """
    while True:
        try:
            counts = await _sweep_deferred_deliveries()
            if counts["delivered"] or counts["abandoned"]:
                logger.info("deferred delivery sweep: %s", counts)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("deferred delivery sweep failed")
        try:
            await asyncio.sleep(_DEFERRED_SWEEP_SECONDS)
        except asyncio.CancelledError:
            raise


async def _forward_pubsub_listener(stop_event: asyncio.Event | None, event: asyncio.Event) -> None:
    """Background task: subscribe to forward publish channel and set `event` when a notification arrives.

    This is best-effort: failures are logged and the task exits without raising.
    """
    try:
        import redis.asyncio as aioredis
    except Exception:
        logger.warning("forward listener: redis.asyncio not available; listener disabled")
        return

    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        logger.info("forward listener: REDIS_URL not configured; listener disabled")
        return

    channel = os.environ.get("FORWARD_PUBLISH_CHANNEL", "ffmpeg:forwards")

    try:
        client = aioredis.from_url(redis_url, decode_responses=True)
        pub = client.pubsub()
        await pub.subscribe(channel)
        logger.info("Subscribed to forward publish channel %s", channel)
    except Exception:
        logger.exception("Failed to subscribe to forward publish channel; listener disabled")
        try:
            aclose = getattr(client, "aclose", None)
            if aclose is not None:
                await aclose()
            else:
                await client.close()
        except Exception:
            logger.debug("ffmpeg worker: operation failed")
        return

    try:
        async for msg in pub.listen():
            if stop_event and stop_event.is_set():
                break
            if not msg:
                continue
            if msg.get("type") != "message":
                continue
            data = msg.get("data")
            if isinstance(data, bytes):
                try:
                    data = data.decode("utf-8")
                except Exception:
                    data = str(data)
            try:
                payload = json.loads(data)
            except Exception:
                payload = {"fid": data}

            # store last payload and notify waiter(s)
            global LAST_FORWARD_NOTIFICATION
            LAST_FORWARD_NOTIFICATION = payload
            with contextlib.suppress(Exception):
                event.set()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("Exception in forward pubsub listener")
    finally:
        with contextlib.suppress(Exception):
            await pub.unsubscribe(channel)
        try:
            aclose = getattr(client, "aclose", None)
            if aclose is not None:
                await aclose()
            else:
                await client.close()
        except Exception:
            logger.debug("ffmpeg worker: operation failed")


# The size a thumbnail is stored at is a policy, not an accident of which call
# site ran: the copy that goes into the bucket (web results, presigned links, the
# Bot API path) has to stay inside the 320px the Bot API allows, and a frame the
# worker generates is written at that same size. The user's own /addthumb file is
# a deliberate choice of cover, so it is delivered raw when it is on disk instead
# of being downsampled to the storage size.
_THUMB_MAX_EDGE = 320

_LIBRARY_KEY_PREFIX_FALLBACK = "inputs/library/"


def _library_source_cache_path(input_key: str | None, ext: str = "") -> str | None:
    """Local cache path for a shared library object, or ``None`` for any other key.

    Library keys are content-addressed and shared: every operation on one media
    (stream capture, audio extract, compress, any button) resolves to the same
    ``inputs/library/<hash>/source`` object. Caching the bytes of that object on
    local disk once therefore serves all of those operations, so the media only
    travels out of the bucket the first time instead of once per style.

    Returns ``None`` for every other key shape: a per-job object must never be
    handed to a different job, and a crafted ``<hash>/<name>`` pair must not be
    able to walk out of the cache directory.
    """
    if not input_key or not isinstance(input_key, str):
        return None
    try:
        from utils import media_cache as _media_cache_mod

        _prefix = _media_cache_mod.LIBRARY_KEY_PREFIX
    except Exception:
        _prefix = _LIBRARY_KEY_PREFIX_FALLBACK

    key = input_key.replace("\\", "/").lstrip("/")
    if not key.startswith(_prefix):
        return None
    parts = [p for p in key[len(_prefix) :].split("/") if p not in ("", ".")]
    if len(parts) != 2:
        return None
    _hash, _name = parts
    # Hash segment: hex from media_cache.media_library_key, but accept any plain
    # token so a key written by an older/newer producer still hits the cache.
    if not _hash or len(_hash) > 64 or ".." in _hash or not all(c.isalnum() or c in "_-" for c in _hash):
        return None
    if ".." in _name or os.path.basename(_name) != _name:
        return None
    # The stored key may carry no extension (media_cache writes `source`); the
    # caller's extension keeps the cached file usable by ffmpeg and friends.
    if ext and not os.path.splitext(_name)[1]:
        _name = f"{_name}{ext}"
    return os.path.join(getattr(config, "TEMP_PATH", "storage/temp"), "library", _hash, _name)


def _is_shared_source_cache(input_path: str | None, input_key: str | None) -> bool:
    """Whether *input_path* is the shared cache copy of the *input_key* object.

    Used before deleting an input after a job: the per-job copy is disposable,
    but the library copy is the whole point of the cache and has to survive.
    """
    cached = _library_source_cache_path(input_key)
    if not cached or not input_path:
        return False
    return os.path.normpath(str(input_path)) == os.path.normpath(cached)


def _cleanup_local_thumb(job: dict) -> None:
    """Remove the private directory holding a job's local thumbnail copy.

    Only a directory this worker created is ever removed (``worker_thumb_``),
    and only when it is not a path the user's own files live in, so a custom
    thumbnail kept in ``storage/thumbnails`` is never touched.
    """
    path = job.get("_local_thumb")
    if not path or not isinstance(path, str):
        return
    try:
        directory = os.path.dirname(os.path.abspath(path))
        if os.path.basename(directory).startswith("worker_thumb_"):
            safe_rmtree(directory)
            job.pop("_local_thumb", None)
    except Exception:
        logger.debug("worker: could not remove the local thumbnail copy %s", path)


def _local_thumb_candidate(job: dict) -> str | None:
    """A thumbnail that is already on local disk, or ``None``.

    Delivery only needs the bytes to attach to the Telegram upload, and the
    worker normally produced this thumbnail itself moments earlier in the same
    call, so the local copy is preferred over fetching the object back out of
    storage. ``None`` deliberately leaves the caller's storage fallback in
    place, which is what keeps a retry that lands in another container working.

    The user's own thumbnail is checked before the worker's normalised copy: the
    raw file is what they picked, and the stored variant has already been capped
    to ``_THUMB_MAX_EDGE``. Entries that are not paths (an object key from an
    earlier container) fail the ``os.path.exists`` test and fall through.
    """
    for cand in (job.get("thumbnail"), job.get("thumb"), job.get("_local_thumb")):
        try:
            if cand and os.path.exists(str(cand)):
                return str(cand)
        except Exception:
            continue
    return None


async def handle_job(job: dict):
    # Clear per-job probe cache to prevent unbounded memory growth across jobs.
    # Each job starts with a fresh cache; re-probes within the same job are still
    # served from the cache because _probe_output_metadata is called multiple
    # times during a single handle_job invocation (e.g. for delivery).
    _output_probe_cache.clear()
    job_id = job.get("job_id")
    input_path = job.get("input_path")
    # Early resolve commonly-used fields so error handlers can report progress
    output_path = job.get("output_path")
    progress_channel = job.get("progress_channel") or f"ffmpeg:progress:{job_id}"

    # Normalize path separators in incoming job payloads (handle Windows-origin paths)
    try:
        if isinstance(input_path, str) and input_path:
            input_path = input_path.replace("\\", os.sep)
            input_path = os.path.normpath(input_path)
            job["input_path"] = input_path
        if isinstance(output_path, str) and output_path:
            output_path = output_path.replace("\\", os.sep)
            output_path = os.path.normpath(output_path)
            job["output_path"] = output_path
    except Exception:
        logger.debug("ffmpeg worker: Normalize path separators in incoming job payloads (handle Win...")

    # Enrich job payload from Redis-stored job hash when fields are missing.
    # Some producers write extra metadata into the job hash (hset) but push
    # a minimal JSON onto the queue; read the hash to fill any missing fields
    # before we decide there is "no input".
    try:
        try:
            r = await get_redis()
        except Exception:
            r = None
        if r is not None and job_id:
            try:
                stored = await r.hgetall(f"ffmpeg:job:{job_id}")
                if stored:
                    # stored values may be bytes or str depending on client
                    def _sval(key):
                        v = stored.get(key)
                        if isinstance(v, bytes):
                            try:
                                return v.decode()
                            except Exception:
                                return v
                        return v

                    # fill missing fields conservatively
                    if not job.get("input_path") and _sval("input"):
                        job["input_path"] = _sval("input")
                        input_path = job["input_path"]
                    if not job.get("input_key") and _sval("input_key"):
                        job["input_key"] = _sval("input_key")
                    if not job.get("source_url") and _sval("source_url"):
                        job["source_url"] = _sval("source_url")
                    if not job.get("output_path") and _sval("output"):
                        job["output_path"] = _sval("output")
                    # Naming fields are persisted by enqueue_job; restore them so a
                    # requeued/retried job still delivers under the original name.
                    if not job.get("original_filename") and _sval("original_filename"):
                        job["original_filename"] = _sval("original_filename")
                    if not job.get("output_filename") and _sval("output_filename"):
                        job["output_filename"] = _sval("output_filename")
                    # Source provenance written by the big-file pipeline: the
                    # ingest already probed the whole media before it stored
                    # anything, and it knows the Telegram copy the userbot
                    # fetched from. Both are read here so the worker neither
                    # repeats the probe against storage nor assumes the bucket is
                    # the only place the bytes exist.
                    for _field in (
                        "source_duration",
                        "source_format",
                        "source_chat_id",
                        "source_message_id",
                        "file_unique_id",
                    ):
                        if not job.get(_field) and _sval(_field):
                            job[_field] = _sval(_field)
                    if _sval("input_header_only") is not None:
                        job["input_header_only"] = _sval("input_header_only")
            except Exception:
                logger.debug("ffmpeg worker: operation failed")
            try:
                aclose = getattr(r, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await r.close()
            except Exception:
                logger.debug("ffmpeg worker: in _sval()")
    except Exception:
        logger.debug('ffmpeg worker: before we decide there is "no input".')

    # Early cancel check: if the job was cancelled (hash has cancel=1), bail out now.
    if job_id and await _check_upload_cancelled(job_id):
        logger.info("Job %s was cancelled — skipping processing entirely", job_id)
        try:
            r = await get_redis()
            try:
                await r.hset(
                    f"ffmpeg:job:{job_id}",
                    mapping={"status": "cancelled", "progress": "0", "message": "cancelled by user"},
                )
                await publish_update(
                    progress_channel,
                    {
                        "job_id": job_id,
                        "progress": 0,
                        "status": "cancelled",
                        "message": "cancelled by user",
                    },
                )
            finally:
                with contextlib.suppress(Exception):
                    await r.close()
        except Exception:
            logger.debug("ffmpeg worker: could not publish early cancellation for %s", job_id)
        return

    # If job references a remote storage key (S3/MinIO), prefer to download it
    # when the local `input_path` is missing or the file is not present on disk.
    input_key = job.get("input_key") or job.get("s3_key") or job.get("remote_key")
    if input_key and input_path and os.path.exists(input_path):
        # A usable local copy beats the bucket: this is the same object the
        # producer uploaded, so reading it here saves a whole copy of the media
        # of egress. It only ever holds when the worker shares a filesystem with
        # that producer; otherwise `input_path` is absent or already gone and
        # the download below is the only way to get the source.
        logger.info(
            "Job %s: reading source from local disk (%s); skipping storage download of %s",
            job_id,
            input_path,
            input_key,
        )
        # Count it: a shared object served from disk is a fetch that never left
        # the bucket, which is the whole point of one object per media.
        if _library_source_cache_path(input_key):
            with contextlib.suppress(Exception):
                await record_source_cache(True, nbytes=os.path.getsize(input_path))
    # ── The bucket may hold only a header ──
    # With ``PIPELINE_SOURCE_UPLOAD=header`` the stored object is a couple of
    # megabytes of container, and the media itself is on Telegram. Encoding the
    # header would produce a two-megabyte "video", so it is never downloaded as
    # a source: the bytes are read from local disk or over MTProto.
    if (
        str(job.get("input_header_only") or "").strip().lower() in ("1", "true", "yes", "on")
        and input_key
        and (not input_path or not os.path.exists(input_path))
    ):
        logger.info(
            "Job %s: %s is only the probe header, not the media; reading the source from Telegram",
            job_id,
            input_key,
        )
        input_key = None

    # ── Telegram first: the media is already there ──
    # The userbot fetched this file through the relay chat, and that copy is
    # still in it. Reading it over MTProto costs no bucket egress, so it is
    # preferred over pulling a whole stored copy back out; a stored object stays
    # the last resort for a worker with no session to read it with.
    if not input_path or not os.path.exists(input_path):
        _tg_chat = job.get("source_chat_id")
        _tg_msg = job.get("source_message_id")
        if _tg_chat and _tg_msg:
            try:
                from utils.userbot_downloader import download_forward_via_userbot

                _tg_dir = os.path.join(getattr(config, "TEMP_PATH", "storage/temp"))
                os.makedirs(_tg_dir, exist_ok=True)
                _tg_dest = os.path.join(
                    _tg_dir,
                    f"{job_id}_src{file_utils.safe_extension(job.get('original_filename') or '', '.mp4')}",
                )
                logger.info(
                    "Job %s: fetching the source over Telegram (%s/%s) instead of storage",
                    job_id,
                    _tg_chat,
                    _tg_msg,
                )
                _tg_ok = await download_forward_via_userbot(
                    chat_id=_tg_chat,
                    message_id=int(_tg_msg),
                    dest_path=_tg_dest,
                    file_unique_id=job.get("file_unique_id"),
                    user_id=job.get("user_id"),
                )
                if _tg_ok and os.path.exists(_tg_dest) and os.path.getsize(_tg_dest) > 0:
                    input_path = _tg_dest
                    job["input_path"] = input_path
                    logger.info(
                        "Job %s: source fetched over Telegram (%dMB)",
                        job_id,
                        os.path.getsize(_tg_dest) // (1024 * 1024),
                    )
                else:
                    logger.warning(
                        "Job %s: the Telegram fetch returned nothing; falling back to storage",
                        job_id,
                    )
            except Exception:
                logger.warning(
                    "Job %s: the Telegram fetch failed; falling back to storage",
                    job_id,
                    exc_info=True,
                )

    # ── Recover from the cache: the media may live under another name ──
    # A header job carries no source object, so an unreadable Telegram copy used
    # to be the end of it even when a stored copy of the same media existed - one
    # written by a promote-on-repeat, by ``full``/``stream`` mode, or by the
    # Bot-API path. The registry is keyed on the Telegram identity rather than on
    # the job, which is what lets this run find those bytes. Adopting the key is
    # all that is needed: the storage path below does the rest (HEAD pre-flight,
    # shared local cache, atomic download).
    if not input_key and (not input_path or not os.path.exists(input_path)):
        _recovered_key = await _stored_source_for_job(job)
        if _recovered_key:
            input_key = _recovered_key
            logger.info(
                "Job %s: the Telegram copy was not readable; recovering the media from storage (%s)",
                job_id,
                _recovered_key,
            )

    if input_key and (not input_path or not os.path.exists(input_path)):
        # ── Shared source cache ──
        # One media is one library object, and every operation on it resolves to
        # that same key, so the bytes only have to leave the bucket once: the
        # second style, the third button press and every repeat then read the
        # source off local disk. Only library keys are cached - a per-job object
        # belongs to that job alone and must never be served to another.
        _shared_cache_path = _library_source_cache_path(input_key)
        if _shared_cache_path and os.path.exists(_shared_cache_path) and os.path.getsize(_shared_cache_path) > 0:
            logger.info(
                "Job %s: reusing shared local source cache (%s); skipping storage download of %s",
                job_id,
                _shared_cache_path,
                input_key,
            )
            input_path = _shared_cache_path
            job["input_path"] = input_path
            with contextlib.suppress(Exception):
                await record_source_cache(True, nbytes=os.path.getsize(_shared_cache_path))
            with contextlib.suppress(Exception):
                # Keep it warm: the temp sweeps prune by mtime, and an in-use
                # cache entry must not be pruned out from under the next job.
                os.utime(_shared_cache_path, None)
    if input_key and (not input_path or not os.path.exists(input_path)):
        # prepare temp path
        temp_dir = os.path.join(getattr(config, "TEMP_PATH", "storage/temp"))
        os.makedirs(temp_dir, exist_ok=True)
        _, ext = os.path.splitext(input_key)
        if not ext:
            # Allowlist the fallback ext (shared with the BigFile pipeline) so
            # attacker-supplied original_filenames can't inject odd suffixes
            # into the on-disk temp path.
            ext = file_utils.safe_extension(job.get("original_filename") or "", "")
        temp_input_path = _library_source_cache_path(input_key, ext) or os.path.join(temp_dir, f"{job_id}_src{ext}")
        # A cache location lives in its own per-hash directory; the per-job path
        # shares the existing temp dir. create the one that's missing.
        with contextlib.suppress(Exception):
            os.makedirs(os.path.dirname(temp_input_path), exist_ok=True)

        if get_storage_backend is None:
            raise RuntimeError("storage backend helper not available")

        backend = await get_storage_backend()

        # If backend supports existence checks, verify the remote key exists
        exists_remote = True
        try:
            if hasattr(backend, "exists") and job.get("input_key"):
                exists_remote = await backend.exists(job.get("input_key"))
        except Exception:
            # conservatively assume it exists if the check fails
            exists_remote = True

        if not exists_remote:
            # Requeue with exponential backoff for transient remote-key availability
            try:
                r2 = await get_redis()
            except Exception:
                r2 = None

            attempts = 0
            try:
                if r2 is not None:
                    cur = await r2.hget(f"ffmpeg:job:{job_id}", "remote_missing_attempts")
                    if cur:
                        if isinstance(cur, bytes):
                            cur = cur.decode()
                        attempts = int(cur or 0)
            except Exception:
                attempts = 0

            max_attempts = int(os.environ.get("MAX_REMOTE_MISSING_ATTEMPTS", "3"))
            attempts += 1
            if attempts <= max_attempts:
                backoff_base = float(os.environ.get("REMOTE_MISSING_BACKOFF_BASE", "30"))
                backoff = backoff_base * (2 ** (attempts - 1))
                try:
                    if r2 is not None:
                        await r2.hset(f"ffmpeg:job:{job_id}", mapping={"remote_missing_attempts": str(attempts)})
                        try:
                            await r2.zadd("ffmpeg:delayed", {json.dumps(job): time.time() + backoff})
                        except Exception:
                            await r2.lpush(JOB_LIST, json.dumps(job))
                except Exception:
                    logger.debug("ffmpeg worker: operation failed")

                with contextlib.suppress(Exception):
                    await publish_update(
                        progress_channel,
                        {
                            "job_id": job_id,
                            "progress": 0,
                            "message": "requeued_missing_input",
                            "attempts": attempts,
                            "backoff": backoff,
                        },
                    )

                if r2 is not None:
                    try:
                        aclose = getattr(r2, "aclose", None)
                        if aclose is not None:
                            await aclose()
                        else:
                            await r2.close()
                    except Exception:
                        logger.debug("ffmpeg worker: operation failed")
                return
            else:
                with contextlib.suppress(Exception):
                    await publish_update(
                        progress_channel,
                        {
                            "job_id": job_id,
                            "progress": 0,
                            "message": "download_failed",
                            "error": "remote_key_missing_permanent",
                        },
                    )
                with contextlib.suppress(Exception):
                    await job_store.update_job(job_id, {"status": "error", "error": "remote_key_missing_permanent"})
                # Terminal state in the hash too: it is what every watcher reads.
                await _set_job_state(job_id, "error", "the source is missing from storage", progress=0)
                if r2 is not None:
                    try:
                        aclose = getattr(r2, "aclose", None)
                        if aclose is not None:
                            await aclose()
                        else:
                            await r2.close()
                    except Exception:
                        logger.debug("ffmpeg worker: operation failed")
                return

        # A source held in storage is the first thing this job does, and for a
        # large video that is minutes of transfer before ffmpeg ever starts. Both
        # facts are handled here because of how a stuck member presents itself:
        # the job hash kept the "queued" that enqueue_job wrote until ffmpeg
        # started, so a file fetching its source looked exactly like one nobody had
        # picked up - the watchdog sat on "queued / Progress: 0%" - and the
        # transfer had no bound, so a stalled one held the batch lock and the only
        # conversion slot for good.
        source_bytes = _job_source_bytes(job)
        _fetch_note = (
            f"fetching source from storage ({source_bytes // (1024 * 1024)} MB)"
            if source_bytes
            else "fetching source from storage"
        )
        await _set_job_state(job_id, "processing", _fetch_note, progress=0, channel=progress_channel)

        # ── HEAD pre-flight: short-circuit when the key is already gone ──
        # Without this, a deleted source burns every download attempt (each
        # pulling bytes from the wire) before the error is surfaced.
        try:
            if not await backend.exists(input_key):
                logger.warning(
                    "Source pre-flight: key %s no longer exists in storage (job %s)",
                    input_key,
                    job_id,
                )
                await _set_job_state(
                    job_id,
                    "error",
                    "the source is missing from storage",
                    progress=0,
                )
                return
        except Exception:
            # exists() may not be implemented by every backend; a failure
            # here is non-fatal — fall through to the download loop.
            logger.debug(
                "Source pre-flight head_object failed for %s (job %s); proceeding to download",
                input_key,
                job_id,
            )

        # ── HEAD size: fill in the timeout budget when the job hash lacks it ──
        # The job hash's ``file_size`` field is the size Telegram reported at
        # upload; if it was 0 or missing the download timeout falls back to the
        # floor (1800 s).  A second HEAD (zero egress) lets us scale the
        # timeout to the real object size.
        if source_bytes <= 0:
            try:
                real_size = await backend.get_file_size(input_key)
                if real_size and real_size > 0:
                    source_bytes = real_size
            except Exception:
                pass

        # ── Source check before pulling the whole file ──
        # The ingest ffprobed the entire media *before* it stored anything and
        # left that verdict in this job's hash, so it is what is trusted here: it
        # costs no egress and it is exact.
        #
        # The storage range-probe therefore only runs when there is no verdict.
        # When it does run, a slice without a duration is *not* a failure: an MP4
        # whose moov atom sits at the end (not faststart) legitimately reports no
        # duration in its first 2 MB while being perfectly playable, and treating
        # that as "corrupt" killed good jobs mid-batch - the apply saw a terminal
        # error and went straight on to fetch the next file. The full download
        # below validates whatever it actually fetched.
        _declared_duration = job.get("source_duration")
        _declared_format = job.get("source_format")
        _probe_slice_path = f"{temp_input_path}.probe"
        if _declared_duration or _declared_format:
            logger.info(
                "Source check: %s was probed at ingest (dur=%s, format=%s, job %s); skipping the storage range-probe",
                input_key,
                _declared_duration or "?",
                _declared_format or "?",
                job_id,
            )
        else:
            try:
                range_ok = await asyncio.wait_for(
                    backend.download_range(
                        input_key,
                        _probe_slice_path,
                        end=2_097_151,
                    ),
                    timeout=STORAGE_PROBE_TIMEOUT_SECONDS,
                )
                if range_ok and os.path.exists(_probe_slice_path) and os.path.getsize(_probe_slice_path) > 0:
                    from utils.ffmpeg_runner import probe_media

                    probe = await probe_media(_probe_slice_path)
                    if not probe or not probe.get("duration"):
                        logger.warning(
                            "Range-probe: no duration in the first 2MB of %s (job %s) - "
                            "this is normal for a container whose header sits at the end; "
                            "proceeding to the full download",
                            input_key,
                            job_id,
                        )
                    else:
                        logger.debug(
                            "Range-probe: source %s looks valid (dur=%.1fs, job %s)",
                            input_key,
                            probe.get("duration", 0),
                            job_id,
                        )
            except TimeoutError:
                logger.warning(
                    "Range-probe timed out after %.0fs for %s (job %s); proceeding to the full download",
                    STORAGE_PROBE_TIMEOUT_SECONDS,
                    input_key,
                    job_id,
                )
            except Exception:
                # Range GET or ffprobe failure is non-fatal — fall through to the
                # full download which has its own ffprobe validation.
                logger.debug(
                    "Range-probe failed for %s (job %s); proceeding to full download",
                    input_key,
                    job_id,
                )
            finally:
                with contextlib.suppress(OSError):
                    os.remove(_probe_slice_path)

        # retry/backoff for transient storage/download issues
        download_retries = int(os.environ.get("DOWNLOAD_RETRIES", "3"))
        backoff_base = float(os.environ.get("DOWNLOAD_BACKOFF_BASE", "1"))
        download_timeout = _storage_download_timeout_seconds(source_bytes)
        download_success = False
        last_exc = None
        # Download through a sibling `.part` file and swap it in only once the
        # bytes are all there. A cut connection or a crash must never leave a
        # truncated file at the destination: for a shared library object that
        # file is the cache later jobs read, and half a video would be processed
        # as if it were the whole thing.
        _part_path = f"{temp_input_path}.part"
        with contextlib.suppress(Exception):
            if os.path.exists(_part_path):
                os.remove(_part_path)
        for attempt in range(1, download_retries + 1):
            try:
                await asyncio.wait_for(backend.download_file(input_key, _part_path), timeout=download_timeout)
                # confirm file exists and has data
                if os.path.exists(_part_path) and (os.path.getsize(_part_path) > 0):
                    os.replace(_part_path, temp_input_path)
                    download_success = True
                    break
            except TimeoutError:
                # ``asyncio.wait_for`` raises the builtin (the alias of
                # ``asyncio.TimeoutError`` on 3.11+).
                last_exc = TimeoutError(f"storage download exceeded {download_timeout:.0f}s")
                logger.warning(
                    "Source download for job %s timed out after %.0fs (%s); attempt %d/%d",
                    job_id,
                    download_timeout,
                    input_key,
                    attempt,
                    download_retries,
                )
            except Exception as e:
                last_exc = e
            if not download_success:
                with contextlib.suppress(Exception):
                    os.remove(_part_path)
            # backoff before next attempt
            if attempt < download_retries:
                await asyncio.sleep(backoff_base * (2 ** (attempt - 1)))

        if download_success:
            input_path = temp_input_path
            # A shared object whose local copy was missing (first use, another
            # container, or a swept cache): these are the bytes that had to leave
            # the bucket, so they count as a miss against the cache.
            if _is_shared_source_cache(temp_input_path, job.get("input_key") or input_key):
                with contextlib.suppress(Exception):
                    await record_source_cache(False)
            job["input_path"] = input_path
            job["_input_from_remote"] = True
            # persist indicator into Redis job hash for observability
            try:
                r2 = None
                try:
                    r2 = await get_redis()
                except Exception:
                    r2 = None
                if r2 is not None:
                    with contextlib.suppress(Exception):
                        await r2.hset(
                            f"ffmpeg:job:{job_id}",
                            mapping={"input": str(job.get("input_path") or ""), "input_from_remote": "1"},
                        )
            except Exception:
                logger.debug("ffmpeg worker: persist indicator into Redis job hash for observability")
            finally:
                if r2 is not None:
                    try:
                        aclose = getattr(r2, "aclose", None)
                        if aclose is not None:
                            await aclose()
                        else:
                            await r2.close()
                    except Exception:
                        logger.debug("ffmpeg worker: operation failed")
        else:
            logger.exception("Failed to download input from storage for job %s: %s", job_id, last_exc)
            with contextlib.suppress(Exception):
                await publish_update(
                    progress_channel,
                    {"job_id": job_id, "progress": 0, "message": "download_failed", "error": "remote_download_failed"},
                )
            with contextlib.suppress(Exception):
                await job_store.update_job(job_id, {"status": "error", "error": "remote_download_failed"})
            # The watchers read the job hash; the publish above and Mongo are not
            # enough. Without this the job stayed "processing" for the rest of its
            # TTL, so its watchdog never showed the failure and a bulk apply waited
            # out its whole job budget for a member that was already over.
            _fetch_error = (
                "the source could not be found: no local copy, unreadable on Telegram, "
                "and no stored copy to recover it from"
                if str(job.get("input_header_only") or "").strip().lower() in ("1", "true", "yes", "on")
                else "could not fetch the source from storage"
            )
            await _set_job_state(job_id, "error", _fetch_error, progress=0)
            return
    # (re)use any job-provided retry count
    retries = int(job.get("retries", 0))
    max_runtime = int(os.environ.get("JOB_MAX_SECONDS", str(6 * 3600)))

    # download source_url into temp_input if provided
    temp_input = None
    source_url = job.get("source_url")
    if source_url:
        try:
            temp_dir = os.path.join(getattr(config, "TEMP_PATH", "storage/temp"))
            os.makedirs(temp_dir, exist_ok=True)
            temp_input = os.path.join(temp_dir, f"{job_id}_src")
            async with aiohttp.ClientSession() as session:
                async with session.get(source_url, timeout=60, allow_redirects=False) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"Failed to download source URL: {resp.status}")
                    with open(temp_input, "wb") as fh:
                        async for chunk in resp.content.iter_chunked(1024 * 64):
                            fh.write(chunk)
            if not job.get("input_path"):
                job["input_path"] = temp_input
            input_path = job.get("input_path")
        except Exception as e:
            logger.exception("Failed to download source URL for job %s: %s", job_id, e)
            with contextlib.suppress(Exception):
                await publish_update(
                    progress_channel,
                    {
                        "job_id": job_id,
                        "progress": 0,
                        "message": "download_failed",
                        "error": "source_url_download_failed",
                    },
                )
            return

    # Acquire per-input lock to avoid duplicate processing
    redis_lock_client = None
    lock_key = None
    lock_acquired = True
    job_type = job.get("type", "ffmpeg")
    if job_type in ("ffmpeg", None) or job.get("ffmpeg_args"):
        try:
            redis_lock_client = await get_redis()
            # Use the canonical remote key (input_key) as the lock name when available,
            # so BigFilePipeline jobs sharing the same S3 input collide on the same lock.
            # This prevents duplicate processing of the same remote file.
            lock_name = (job.get("input_key") or input_path or job.get("source_url") or job_id) or job_id
            lock_hash = hashlib.sha256(str(lock_name).encode()).hexdigest()
            lock_key = f"ffmpeg:lock:{lock_hash}"
            lock_ttl = int(os.environ.get("JOB_LOCK_SECONDS", str(3600)))
            lock_acquired = await redis_lock_client.set(lock_key, job_id, nx=True, ex=lock_ttl)
        except Exception:
            lock_acquired = True

    if not lock_acquired:
        # Check if we already own this lock from a previous attempt.
        # If the existing lock value matches our job_id, the lock was set
        # by an earlier run of this same job and is still valid — treat as
        # acquired rather than entering an infinite requeue loop.
        try:
            current_val = await redis_lock_client.get(lock_key)
            if current_val:
                if isinstance(current_val, bytes):
                    current_val = current_val.decode()
                if current_val == job_id:
                    lock_acquired = True
                    logger.info("Input lock already owned by this job %s, reusing", job_id)
                    # Refresh the TTL so the lock doesn't expire during processing
                    with contextlib.suppress(Exception):
                        await redis_lock_client.expire(lock_key, lock_ttl)
        except Exception:
            logger.debug("ffmpeg worker: acquired rather than entering an infinite requeue loop.")

    if not lock_acquired:
        logger.info("Input already locked for job %s, requeueing", job_id)
        with contextlib.suppress(Exception):
            await publish_update(
                progress_channel, {"job_id": job_id, "progress": 0, "message": "locked", "note": "input_locked"}
            )
        # A job delivered by RabbitMQ goes back to RabbitMQ through its retry
        # (delay) queue, so it keeps the acknowledgement semantics and the retry
        # counter it arrived with. Jobs from the Redis list keep using the
        # delayed set exactly as before - this is what stops a lock collision
        # from moving a job between the two queues.
        requeued_on_broker = False
        if is_rabbitmq_job(job):
            try:
                requeued_on_broker = await eventbus.requeue_job(job)
            except Exception:
                requeued_on_broker = False
        if not requeued_on_broker:
            try:
                # Push into delayed set with a small backoff to avoid tight requeue loop
                backoff = int(os.environ.get("JOB_LOCK_BACKOFF", "5"))
                try:
                    # zadd mapping: {member: score}
                    await redis_lock_client.zadd("ffmpeg:delayed", {json.dumps(job): time.time() + backoff})
                except Exception:
                    # fallback to lpush if zadd not supported
                    await redis_lock_client.lpush(JOB_LIST, json.dumps(job))
            except Exception:
                logger.warning("Failed to requeue locked job %s", job_id)
        try:
            try:
                aclose = getattr(redis_lock_client, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await redis_lock_client.close()
            except Exception:
                logger.debug("ffmpeg worker: operation failed")
        except Exception:
            logger.debug("ffmpeg worker: operation failed")
        return

    # mark processing start
    try:
        await job_store.update_job(job_id, {"status": "processing", "started_at": time.time()})
        # Cache job start for fast status queries
        try:
            if _cache:
                await _cache.cache_job_metadata(
                    job_id,
                    {
                        "status": "processing",
                        "started_at": time.time(),
                    },
                    ttl=3600,
                )
        except Exception:
            logger.debug("ffmpeg worker: Cache job start for fast status queries")
    except Exception:
        logger.debug("ffmpeg worker: mark processing start")
    # Mirror the start into the Redis job hash. The hash stays "queued" from
    # enqueue until the upload phase otherwise, so a status view cannot tell a
    # job that is actually being worked on from one still waiting in the list.
    try:
        _start_r = await get_redis()
        try:
            await _start_r.hset(
                f"ffmpeg:job:{job_id}",
                mapping={"status": "processing", "started_at": str(time.time())},
            )
        finally:
            with contextlib.suppress(Exception):
                await _start_r.close()
    except Exception:
        logger.debug("ffmpeg worker: could not mark job %s as processing", job_id)
    with contextlib.suppress(Exception):
        await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "started"})

    # prefer original filename for output if provided
    try:
        orig = job.get("original_filename") or job.get("original_name")
        if orig:
            sanitized = await file_utils.sanitize_filename(orig)
            base, ext = os.path.splitext(sanitized)
            out_ext = job.get("output_ext") or (
                ".mp4"
                if (job.get("ffmpeg_args") or job.get("type") in ("ffmpeg", None, "generate_sample"))
                else (ext or ".mp4")
            )
            out_dir = os.path.dirname(output_path) if output_path else getattr(config, "OUTPUT_PATH", "storage/output")
            os.makedirs(out_dir, exist_ok=True)
            candidate = os.path.join(out_dir, f"{base}{out_ext}")
            counter = 1
            while os.path.exists(candidate):
                candidate = os.path.join(out_dir, f"{base}_{counter}{out_ext}")
                counter += 1
            output_path = candidate
            job["output_path"] = output_path
    except Exception:
        logger.exception("Failed to compute output_path from original_filename")

    attempt = 0
    # optional memory sampler task (helpful for remote debugging)
    memory_sampler_task = None

    async def _get_rss_bytes() -> int:
        try:
            import psutil

            p = psutil.Process(os.getpid())
            return int(getattr(p.memory_info(), "rss", 0))
        except Exception:
            try:
                out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(os.getpid())], text=True)
                return int(out.strip()) * 1024
            except Exception:
                return 0

    async def _memory_sampler(channel: str, interval: float = 5.0):
        while True:
            try:
                rss = await _get_rss_bytes()
                with contextlib.suppress(Exception):
                    await publish_update(channel, {"job_id": job_id, "memory_rss": rss})
            except Exception:
                logger.debug("ffmpeg worker: operation failed")
            await asyncio.sleep(interval)

    try:
        while True:
            attempt += 1
            ACTIVE_JOBS.inc()
            try:
                with JOB_DURATION.time():
                    success = False
                    info = None
                    job_type = job.get("type", "ffmpeg")

                    if job_type in ("ffmpeg", None) or job.get("ffmpeg_args"):
                        ffmpeg_args = job.get("ffmpeg_args") if isinstance(job.get("ffmpeg_args"), list) else None
                        redis_url = job.get("redis_url") or os.environ.get("REDIS_URL")
                        # Enforce conversion rate limit at actual start of processing
                        try:
                            if _conv_limiter is not None:
                                user_key = str(job.get("user_id") or job.get("chat_id") or "global")
                                ok = await _conv_limiter.mark_conversion_started(user_key)
                                if not ok:
                                    # inform progress channel and mark job as errored due to rate limit
                                    with contextlib.suppress(Exception):
                                        await publish_update(
                                            progress_channel,
                                            {
                                                "job_id": job_id,
                                                "progress": 0,
                                                "message": "rate_limited",
                                                "error": "user rate limit reached",
                                            },
                                        )
                                    with contextlib.suppress(Exception):
                                        await job_store.update_job(job_id, {"status": "error", "error": "rate_limited"})
                                    return
                        except Exception:
                            # on limiter failures, allow processing to continue
                            logger.debug("ffmpeg worker: on limiter failures, allow processing to continue")
                        # Ensure there is some form of input before starting ffmpeg: a local path (that exists),
                        # a remote storage key, or a source URL. Prefer remote key download when present.
                        has_local_file = bool(input_path and os.path.exists(input_path))

                        # If no local file and no remote key/source_url available, allow a short
                        # grace period for producers to populate the job hash (input_key/input).
                        # This avoids transient race conditions where a producer pushes a
                        # minimal job JSON then writes the richer metadata into the hash.
                        if not has_local_file and not job.get("input_key") and not job.get("source_url"):
                            wait_seconds = int(os.environ.get("JOB_WAIT_SECONDS", "10"))
                            # notify once that we're waiting
                            with contextlib.suppress(Exception):
                                await publish_update(
                                    progress_channel,
                                    {
                                        "job_id": job_id,
                                        "progress": 0,
                                        "message": "waiting_for_input",
                                        "wait_seconds": wait_seconds,
                                    },
                                )

                            try:
                                rr = await get_redis()
                            except Exception:
                                rr = None

                            if rr is not None and job_id:
                                try:
                                    for _ in range(wait_seconds):
                                        try:
                                            stored = await rr.hgetall(f"ffmpeg:job:{job_id}")
                                            if stored:

                                                def _sval(key):
                                                    v = stored.get(key)
                                                    if isinstance(v, bytes):
                                                        try:
                                                            return v.decode()
                                                        except Exception:
                                                            return v
                                                    return v

                                                if not job.get("input_key") and _sval("input_key"):
                                                    job["input_key"] = _sval("input_key")
                                                if not job.get("input_path") and _sval("input"):
                                                    job["input_path"] = _sval("input")
                                                if not job.get("source_url") and _sval("source_url"):
                                                    job["source_url"] = _sval("source_url")

                                                # recompute local-file presence
                                                input_path = job.get("input_path")
                                                has_local_file = bool(input_path and os.path.exists(input_path))
                                                if has_local_file or job.get("input_key") or job.get("source_url"):
                                                    break
                                        except Exception:
                                            # swallow per-iteration errors and continue waiting
                                            logger.debug(
                                                "ffmpeg worker: swallow per-iteration errors and continue waiting"
                                            )
                                        # Wait up to 1s, but wake early if a forward notification arrives
                                        try:
                                            if FORWARD_NOTIFY_EVENT is not None:
                                                try:
                                                    await asyncio.wait_for(FORWARD_NOTIFY_EVENT.wait(), timeout=1)
                                                    with contextlib.suppress(Exception):
                                                        FORWARD_NOTIFY_EVENT.clear()
                                                except TimeoutError:
                                                    pass
                                            else:
                                                await asyncio.sleep(1)
                                        except Exception:
                                            # on any error, fall back to sleeping briefly
                                            with contextlib.suppress(Exception):
                                                await asyncio.sleep(0.5)
                                finally:
                                    try:
                                        aclose = getattr(rr, "aclose", None)
                                        if aclose is not None:
                                            await aclose()
                                        else:
                                            await rr.close()
                                    except Exception:
                                        logger.debug("ffmpeg worker: operation failed")

                            # final check after waiting
                            if not has_local_file and not job.get("input_key") and not job.get("source_url"):
                                logger.error("No input available for job %s after waiting; marking as error", job_id)
                                with contextlib.suppress(Exception):
                                    await publish_update(
                                        progress_channel,
                                        {
                                            "job_id": job_id,
                                            "progress": 0,
                                            "message": "error",
                                            "error": "no_input_provided",
                                        },
                                    )
                                with contextlib.suppress(Exception):
                                    await job_store.update_job(
                                        job_id, {"status": "error", "error": "no_input_provided"}
                                    )
                                return

                        coro = run_ffmpeg(
                            input_path,
                            output_path,
                            job_id,
                            ffmpeg_args=ffmpeg_args,
                            redis_url=redis_url,
                            progress_channel=progress_channel,
                        )
                        # Start optional memory sampler
                        try:
                            if os.environ.get("ENABLE_MEMORY_SAMPLER", "").lower() in ("1", "true", "yes"):
                                memory_sampler_task = asyncio.create_task(
                                    _memory_sampler(
                                        progress_channel, float(os.environ.get("MEMORY_SAMPLER_INTERVAL", "5.0"))
                                    )
                                )
                        except Exception:
                            memory_sampler_task = None
                        try:
                            success, info = await asyncio.wait_for(coro, timeout=max_runtime)
                        except TimeoutError:
                            success, info = False, "timeout"

                    elif job_type in ("create_archive", "archive"):
                        await publish_update(
                            progress_channel, {"job_id": job_id, "progress": 5, "message": "creating archive"}
                        )
                        files = job.get("files") or []
                        ok, msg = await create_archive(files, output_path)
                        success = ok
                        info = output_path if ok else msg
                        await publish_update(
                            progress_channel,
                            {
                                "job_id": job_id,
                                "progress": 100 if ok else 0,
                                "message": "done" if ok else "error",
                                "output": output_path if ok else None,
                            },
                        )

                    elif job_type == "merge_videos":
                        await publish_update(
                            progress_channel, {"job_id": job_id, "progress": 5, "message": "merging videos"}
                        )
                        files = job.get("files") or []
                        ok, msg = await merge_videos(files, output_path)
                        success = ok
                        info = output_path if ok else msg
                        await publish_update(
                            progress_channel,
                            {
                                "job_id": job_id,
                                "progress": 100 if ok else 0,
                                "message": "done" if ok else "error",
                                "output": output_path if ok else None,
                            },
                        )

                    elif job_type == "merge_audios":
                        await publish_update(
                            progress_channel, {"job_id": job_id, "progress": 5, "message": "merging audios"}
                        )
                        files = job.get("files") or []
                        ok, msg = await merge_audios(files, output_path)
                        success = ok
                        info = output_path if ok else msg
                        await publish_update(
                            progress_channel,
                            {
                                "job_id": job_id,
                                "progress": 100 if ok else 0,
                                "message": "done" if ok else "error",
                                "output": output_path if ok else None,
                            },
                        )

                    elif job_type == "slideshow":
                        await publish_update(
                            progress_channel, {"job_id": job_id, "progress": 5, "message": "building slideshow"}
                        )
                        files = job.get("files") or []
                        try:
                            seconds = float(job.get("seconds_per_image") or 3.0)
                        except (TypeError, ValueError):
                            seconds = 3.0
                        music_path = job.get("music_path")
                        if music_path and not os.path.isfile(str(music_path)):
                            music_path = None
                        ok, msg = await create_slideshow(
                            files, output_path, seconds_per_image=seconds, music_path=music_path
                        )
                        success = ok
                        info = output_path if ok else msg
                        await publish_update(
                            progress_channel,
                            {
                                "job_id": job_id,
                                "progress": 100 if ok else 0,
                                "message": "done" if ok else "error",
                                "output": output_path if ok else None,
                            },
                        )

                    elif job_type == "extract_streams":
                        await publish_update(
                            progress_channel, {"job_id": job_id, "progress": 5, "message": "extracting streams"}
                        )
                        out_dir = job.get("output_dir") or os.path.join(
                            getattr(config, "OUTPUT_PATH", "storage/output"), f"{job_id}_streams"
                        )
                        os.makedirs(out_dir, exist_ok=True)
                        ok, extracted = await extract_streams(input_path, out_dir)
                        if ok and extracted:
                            archive_path = job.get("archive_path") or f"{out_dir}.zip"
                            ok2, msg2 = await create_archive(list(extracted.values()), archive_path)
                            success = ok2
                            info = archive_path if ok2 else msg2
                            await publish_update(
                                progress_channel,
                                {
                                    "job_id": job_id,
                                    "progress": 100 if ok2 else 0,
                                    "message": "done" if ok2 else "error",
                                    "output": archive_path if ok2 else None,
                                },
                            )
                        else:
                            success = False
                            info = "no_streams" if ok else "extract_failed"
                            await publish_update(
                                progress_channel, {"job_id": job_id, "progress": 0, "message": "error", "error": info}
                            )

                    elif job_type == "generate_sample":
                        await publish_update(
                            progress_channel, {"job_id": job_id, "progress": 5, "message": "generating sample"}
                        )
                        dur = int(job.get("duration", 30))
                        ok, msg = await generate_sample(input_path, output_path, dur)
                        success = ok
                        info = output_path if ok else msg
                        await publish_update(
                            progress_channel,
                            {
                                "job_id": job_id,
                                "progress": 100 if ok else 0,
                                "message": "done" if ok else "error",
                                "output": output_path if ok else None,
                            },
                        )

                    elif job_type == "trim":
                        await publish_update(progress_channel, {"job_id": job_id, "progress": 5, "message": "trimming"})
                        start_time = job.get("start_time")
                        end_time = job.get("end_time")
                        ok, msg = await trim_media(input_path, output_path, start_time, end_time)
                        success = ok
                        info = output_path if ok else msg
                        await publish_update(
                            progress_channel,
                            {
                                "job_id": job_id,
                                "progress": 100 if ok else 0,
                                "message": "done" if ok else "error",
                                "output": output_path if ok else None,
                            },
                        )

                    elif job_type == "rename":
                        new_name = job.get("new_name")
                        try:
                            new_path = job.get("output_path") or os.path.join(os.path.dirname(input_path), new_name)
                            os.rename(input_path, new_path)
                            success = True
                            info = new_path
                            await publish_update(
                                progress_channel,
                                {"job_id": job_id, "progress": 100, "message": "renamed", "output": new_path},
                            )
                        except Exception as e:
                            success = False
                            info = str(e)
                            await publish_update(
                                progress_channel, {"job_id": job_id, "progress": 0, "message": "error", "error": info}
                            )

                    else:
                        ffmpeg_args = job.get("ffmpeg_args") if isinstance(job.get("ffmpeg_args"), list) else None
                        redis_url = job.get("redis_url") or os.environ.get("REDIS_URL")
                        try:
                            success, info = await asyncio.wait_for(
                                run_ffmpeg(
                                    input_path,
                                    output_path,
                                    job_id,
                                    ffmpeg_args=ffmpeg_args,
                                    redis_url=redis_url,
                                    progress_channel=progress_channel,
                                ),
                                timeout=max_runtime,
                            )
                        except TimeoutError:
                            success, info = False, "timeout"

                # end with JOB_DURATION

                JOBS_TOTAL.inc()

                if success:
                    JOBS_SUCCEEDED.inc()
                    out = info if isinstance(info, str) else output_path
                    await publish_update(
                        progress_channel, {"job_id": job_id, "progress": 100, "message": "done", "output": out}
                    )
                    try:
                        await job_store.update_job(
                            job_id, {"status": "done", "finished_at": time.time(), "output": out}
                        )
                        # Cache job result for fast status queries
                        try:
                            if _cache:
                                await _cache.cache_job_metadata(
                                    job_id,
                                    {
                                        "status": "done",
                                        "output": out,
                                        "finished_at": time.time(),
                                    },
                                    ttl=3600,
                                )
                        except Exception:
                            logger.debug("ffmpeg worker: Cache job result for fast status queries")
                    except Exception:
                        logger.debug("ffmpeg worker: operation failed")

                    # Attempt to upload processed output to configured storage backend
                    upload_success = False
                    dest = None
                    get_url = None
                    # Whether a copy has to exist in the bucket at all. Set for real
                    # below; the default is the conservative "yes".
                    _needs_remote_copy = True
                    try:
                        # Only attempt when a storage backend helper is available
                        if get_storage_backend is not None:
                            try:
                                backend = await get_storage_backend()
                            except Exception:
                                backend = None
                        else:
                            backend = None

                        # A result only needs a copy in the bucket when something
                        # other than this container will read it: a job with no chat
                        # has to be collected through a URL, and link delivery hands
                        # the user a presigned GET. A result that is about to be sent
                        # to Telegram from here is uploaded for nobody - and every
                        # object we never create is egress nobody can spend later.
                        _needs_remote_copy = bool(config.ENABLE_LINK_SEND or not job.get("chat_id"))
                        if backend is not None and out and os.path.exists(out) and _needs_remote_copy:
                            try:
                                # Choose a sensible destination key/path for outputs
                                base = os.path.basename(out)
                                dest_key = f"outputs/{job_id}/{base}"
                                # Upload the file (local backend will copy to storage path)
                                dest = await backend.upload_file(out, dest_key)
                                # Try to produce a presigned GET URL when supported. Do not expose it
                                # as the default output unless link delivery is explicitly enabled.
                                try:
                                    get_url = await backend.generate_presigned_get(dest)
                                except Exception:
                                    get_url = None

                                # Update Redis job hash with output metadata for the web UI
                                try:
                                    send_link = config.ENABLE_LINK_SEND
                                    r = await get_redis()
                                    mapping = {"output_key": dest}
                                    if get_url:
                                        mapping["output_get_url"] = get_url
                                    mapping["output"] = get_url if get_url and send_link else dest
                                    with contextlib.suppress(Exception):
                                        mapping["out_bytes"] = str(os.path.getsize(out))

                                    # ── T11: Preserve original thumbnail if available (Pillow, no ffmpeg),
                                    #    otherwise generate new frame (ffmpeg) & upload to S3 ──
                                    _thumb_path = None
                                    _probe_meta = None
                                    try:
                                        try:
                                            # Check job for existing thumbnail data (thumb_key from S3 upload
                                            # or 'thumbnail'/'thumb' from Telegram metadata). If it exists
                                            # as a local file, use Pillow to resize/save — no ffmpeg needed.
                                            _existing_thumb = (
                                                job.get("thumbnail") or job.get("thumb") or job.get("thumb_key")
                                            )
                                            # ── T11: Keep original — if thumb is an S3 key, download it first ──
                                            _existing_thumb_dir = None
                                            if _existing_thumb and not os.path.exists(str(_existing_thumb)):
                                                _td_thumb = None
                                                try:
                                                    from utils.storage import get_storage_backend as _gsb_thumb

                                                    _backend_thumb = await _gsb_thumb()
                                                    if _backend_thumb:
                                                        _td_thumb = tempfile.mkdtemp(prefix="worker_thumb_s3_")
                                                        _dl_thumb = os.path.join(_td_thumb, "original_thumb.jpg")
                                                        await _backend_thumb.download_file(
                                                            str(_existing_thumb), _dl_thumb
                                                        )
                                                        if os.path.exists(_dl_thumb) and os.path.getsize(_dl_thumb) > 0:
                                                            _existing_thumb = _dl_thumb
                                                            _existing_thumb_dir = _td_thumb
                                                            logger.debug(
                                                                "Worker: T11 downloaded existing thumb from S3: %s -> %s",
                                                                job.get("thumb_key"),
                                                                _dl_thumb,
                                                            )
                                                        else:
                                                            shutil.rmtree(_td_thumb, ignore_errors=True)
                                                except Exception:
                                                    _existing_thumb = None
                                                    if _td_thumb is not None and os.path.exists(_td_thumb):
                                                        shutil.rmtree(_td_thumb, ignore_errors=True)
                                                    logger.debug(
                                                        "Worker: T11 failed to download existing thumb from S3"
                                                    )
                                            if _existing_thumb and os.path.exists(str(_existing_thumb)):
                                                # ── T11: Keep original — Pillow resize/save ──
                                                # The user's own cover is only ever capped, never
                                                # upscaled: this is the copy other consumers
                                                # (web/links/Bot API) get, so it must respect the
                                                # Bot API limit while local delivery still prefers
                                                # the untouched original.
                                                try:
                                                    from PIL import Image as _PILImg

                                                    _td = tempfile.mkdtemp(prefix="worker_thumb_")
                                                    _tp = os.path.join(_td, "thumb.jpg")
                                                    _img = _PILImg.open(str(_existing_thumb))
                                                    _img.thumbnail(
                                                        (_THUMB_MAX_EDGE, _THUMB_MAX_EDGE), _PILImg.Resampling.LANCZOS
                                                    )
                                                    _img.save(_tp, "JPEG", quality=85, optimize=True)
                                                    if os.path.exists(_tp) and os.path.getsize(_tp) > 0:
                                                        _thumb_path = _tp
                                                        logger.debug(
                                                            "Worker: T11 preserved original thumbnail via Pillow -> %s",
                                                            _tp,
                                                        )
                                                except Exception:
                                                    _thumb_path = None
                                                    logger.debug(
                                                        "Worker: Pillow thumbnail preservation failed, falling back to ffmpeg"
                                                    )
                                                finally:
                                                    # Cleanup S3-downloaded thumb temp dir after Pillow is done with it
                                                    if _existing_thumb_dir is not None and os.path.exists(
                                                        _existing_thumb_dir
                                                    ):
                                                        shutil.rmtree(_existing_thumb_dir, ignore_errors=True)
                                                        _existing_thumb_dir = None
                                        except Exception:
                                            _thumb_path = None

                                        # If no existing thumbnail, generate one via ffmpeg
                                        if not _thumb_path:
                                            _probe_meta, _thumb_path = await _probe_output_metadata(out)
                                            if _thumb_path and os.path.exists(_thumb_path):
                                                # ── T11: Pillow post-process ──
                                                try:
                                                    from PIL import Image as _PILImg

                                                    _img = _PILImg.open(_thumb_path)
                                                    _img.thumbnail(
                                                        (_THUMB_MAX_EDGE, _THUMB_MAX_EDGE), _PILImg.Resampling.LANCZOS
                                                    )
                                                    _img.save(_thumb_path, "JPEG", quality=85, optimize=True)
                                                except ImportError:
                                                    pass  # Pillow not available; use ffmpeg output as-is
                                                except Exception:
                                                    pass  # Best-effort; keep original ffmpeg thumb

                                                _thumb_s3_key = f"outputs/{job_id}/thumb.jpg"
                                                try:
                                                    await backend.upload_file(_thumb_path, _thumb_s3_key)
                                                    # `thumb_key` is the durable pointer for a delivery
                                                    # that happens in another container. `thumbnail` is
                                                    # deliberately NOT overwritten with the key: it is
                                                    # the field delivery reads first, and storing a key
                                                    # there is what made every send fetch the object
                                                    # back out of storage for bytes already on disk.
                                                    mapping["thumb_key"] = _thumb_s3_key
                                                    with contextlib.suppress(Exception):
                                                        job["_local_thumb"] = _thumb_path
                                                    logger.info("Worker: uploaded thumbnail to S3: %s", _thumb_s3_key)
                                                except Exception as _thumb_err:
                                                    logger.warning(
                                                        "Worker: failed to upload thumbnail to S3: %s", _thumb_err
                                                    )

                                            # ── T10: Store output ffprobe metadata in Redis for delivery ──
                                            if _probe_meta:
                                                raw_ffprobe = _probe_meta.get("raw_ffprobe")
                                                if raw_ffprobe is not None:
                                                    mapping["output_metadata"] = json.dumps(raw_ffprobe)
                                                if _probe_meta.get("duration"):
                                                    mapping["output_duration"] = str(_probe_meta["duration"])
                                                if _probe_meta.get("width"):
                                                    mapping["output_width"] = str(_probe_meta["width"])
                                                if _probe_meta.get("height"):
                                                    mapping["output_height"] = str(_probe_meta["height"])
                                                # Extended output metadata (bitrate, fps, codecs)
                                                if _probe_meta.get("video_bitrate"):
                                                    mapping["output_video_bitrate"] = str(_probe_meta["video_bitrate"])
                                                if _probe_meta.get("fps"):
                                                    mapping["output_fps"] = str(_probe_meta["fps"])
                                                if _probe_meta.get("video_codec"):
                                                    mapping["output_video_codec"] = _probe_meta["video_codec"]
                                                if _probe_meta.get("audio_codec"):
                                                    mapping["output_audio_codec"] = _probe_meta["audio_codec"]
                                    except Exception as _thumb_gen_err:
                                        logger.debug("Worker: thumbnail generation/upload failed: %s", _thumb_gen_err)

                                    await r.hset(f"ffmpeg:job:{job_id}", mapping=mapping)
                                    upload_success = True
                                    await r.close()
                                except Exception:
                                    logger.debug(
                                        "ffmpeg worker: Update Redis job hash with output metadata for the web UI"
                                    )
                            except Exception:
                                logger.exception("Failed to upload output for job %s", job_id)
                        elif backend is not None and out and os.path.exists(out):
                            logger.info(
                                "Job %s: result kept out of storage - delivered to chat %s from this "
                                "container and link delivery is disabled",
                                job_id,
                                job.get("chat_id"),
                            )
                            # The result message is still due, and the watcher keys it
                            # off a truthy `output` (a `done` job with no output reads
                            # as a failure). The local path is the honest answer here:
                            # it is what delivery used, and when the bot shares this
                            # container the watcher can probe it for the same metadata
                            # the stored copy used to provide.
                            try:
                                r = await get_redis()
                                try:
                                    await r.hset(
                                        f"ffmpeg:job:{job_id}",
                                        mapping={
                                            "output": str(out),
                                            "out_bytes": str(os.path.getsize(out)),
                                        },
                                    )
                                finally:
                                    await r.close()
                            except Exception:
                                logger.debug("ffmpeg worker: could not record the local result path for %s", job_id)
                    except Exception:
                        logger.debug("ffmpeg worker: operation failed")

                    try:
                        # Only remove the input when:
                        # - cleanup_input is requested, AND
                        # - an input_path exists on disk, AND
                        # - either there is no remote backend (local-only) OR the output upload succeeded.
                        # Respect global override via KEEP_LOCAL_UPLOADS: when set to 1/true/yes,
                        # preserve local uploads regardless of per-job flags.
                        keep_local_uploads = os.environ.get("KEEP_LOCAL_UPLOADS", "").lower() in ("1", "true", "yes")
                        if input_path and os.path.exists(input_path):
                            if keep_local_uploads:
                                # user requested to keep local uploads — do not delete
                                pass
                            else:
                                if job.get("cleanup_input", True):
                                    should_delete = False
                                    try:
                                        if backend is not None:
                                            # Safe to drop the local input when the result is
                                            # safe somewhere: either it is in the bucket, or
                                            # this job deliberately keeps no remote copy
                                            # (delivered to Telegram inline, links off).
                                            if upload_success or not _needs_remote_copy:
                                                should_delete = True
                                        else:
                                            # no remote backend configured; safe to delete local input after processing
                                            should_delete = True
                                    except Exception:
                                        # conservative default: don't delete if uncertain
                                        should_delete = False

                                    if _is_shared_source_cache(input_path, job.get("input_key") or input_key):
                                        # Shared cache, not this job's copy: later
                                        # operations on the same media still want
                                        # these bytes, and re-fetching them is the
                                        # egress this cache exists to avoid.
                                        logger.info(
                                            "Job %s: keeping shared source cache %s for reuse",
                                            job_id,
                                            input_path,
                                        )
                                    elif should_delete:
                                        os.remove(input_path)
                    except Exception as e:
                        logger.warning(f"Failed to cleanup input file: {e}")

                    try:
                        chat_id = job.get("chat_id")
                        caption = job.get("caption")
                        sent = False
                        # Set when a flood window forces this delivery to be
                        # retried later instead of finalized as failed.
                        _deferred_window = None
                        enable_userbot = config.ENABLE_USERBOT
                        bot_token = getattr(config, "BOT_TOKEN", None)

                        # Determine file size and Bot API threshold (MB)
                        file_size = 0
                        try:
                            if out and os.path.exists(out):
                                file_size = os.path.getsize(out)
                        except Exception:
                            file_size = 0

                        # Delivery name = the filename shown in Telegram. It is derived
                        # from the original media name (never from the storage key), and
                        # for audio outputs the file must be sent as Telegram audio so
                        # the client shows the streamable music player.
                        _delivery_name = job.get("output_filename") or os.path.basename(out or "output")
                        try:
                            from utils.userbot_uploader import is_audio_delivery_output

                            _media_kind = "audio" if is_audio_delivery_output(out or "") else None
                        except Exception:
                            _media_kind = None

                        bot_api_max_bytes = config.BOT_API_MAX_BYTES

                        send_link = config.ENABLE_LINK_SEND
                        # If we uploaded the output and generated a presigned GET URL, only send it if
                        # explicit link delivery is enabled. Otherwise keep delivery inside Telegram.
                        if send_link and upload_success and get_url and chat_id and bot_token:
                            if await _check_upload_cancelled(job_id):
                                logger.info("Upload cancelled by user — skipping presigned URL send for job %s", job_id)
                            else:
                                try:
                                    job_type = job.get("type") if isinstance(job, dict) else None
                                    if file_size > bot_api_max_bytes or (job_type and job_type != "generate_sample"):
                                        async with Bot(token=bot_token, request=flood_gated_request()) as bot:
                                            text = f"Your video is ready: {get_url}"
                                            await bot.send_message(chat_id=chat_id, text=text)
                                        logger.info("Sent presigned URL to chat %s for job %s", chat_id, job_id)
                                        sent = True
                                except Exception:
                                    logger.exception("Failed to send presigned URL for job %s", job_id)
                                    sent = False

                        # If output is large and userbot is enabled, prefer userbot for delivery
                        if chat_id and file_size > bot_api_max_bytes and enable_userbot:
                            if await _check_upload_cancelled(job_id):
                                logger.info(
                                    "Upload cancelled by user — skipping preferred userbot send for job %s", job_id
                                )
                                sent = False
                            else:
                                try:
                                    from utils.userbot_uploader import send_file_via_userbot

                                    _up_cb = _make_upload_progress_callback(job_id, progress_channel)
                                    # Direct userbot delivery: sends directly to user's DM via MTProto
                                    # (Telethon/Pyrogram). This preserves all video metadata (duration,
                                    # dimensions, thumbnail, codecs, streaming support) and works for
                                    # files of any size (no Bot API 50MB limit).
                                    _pre_am = None
                                    if _media_kind == "audio":
                                        # Audio has no video metadata/thumbnail; its player
                                        # tags are probed here so they come from the file
                                        # this job just produced, tagged with the media's
                                        # own name.
                                        _pre_vm, _pre_tp = (None, None)
                                        _pre_am = await _probe_audio_delivery(out, _delivery_name)
                                    else:
                                        _pre_vm, _pre_tp = await _probe_output_metadata(out)
                                    try:
                                        ok = await send_file_via_userbot(
                                            chat_id,
                                            out,
                                            caption=caption,
                                            progress_callback=_up_cb,
                                            video_meta=_pre_vm,
                                            thumb_path=_pre_tp,
                                            audio_meta=_pre_am,
                                            user_id=job.get("user_id"),
                                            media_kind=_media_kind,
                                            delivery_name=_delivery_name,
                                        )
                                    finally:
                                        if _pre_tp:
                                            with contextlib.suppress(Exception):
                                                safe_rmtree(os.path.dirname(_pre_tp))
                                    if ok:
                                        logger.info("Sent output via userbot (direct) for job %s", job_id)
                                        sent = True
                                    else:
                                        logger.error("Userbot send failed for job %s", job_id)
                                        sent = False
                                except Exception:
                                    logger.exception("Preferred userbot send raised exception for job %s", job_id)
                                    sent = False
                        else:
                            # Try Bot API first if configured
                            if chat_id and bot_token:
                                try:
                                    # Cancel check before Bot API send
                                    if await _check_upload_cancelled(job_id):
                                        logger.info(
                                            "Upload cancelled by user — skipping Bot API send for job %s", job_id
                                        )
                                        sent = False
                                        raise asyncio.CancelledError()

                                    # Use _probe_output_metadata for metadata + thumbnail
                                    # (replaces separate ffprobe + auto-thumbnail generation)
                                    _probe_vm, _probe_tp = (
                                        await _probe_output_metadata(out)
                                        if out and os.path.exists(out)
                                        else (None, None)
                                    )
                                    kind = "doc"
                                    _vid_duration = _probe_vm.get("duration") if _probe_vm else None
                                    _vid_width = _probe_vm.get("width") if _probe_vm else None
                                    _vid_height = _probe_vm.get("height") if _probe_vm else None
                                    _output_ext = os.path.splitext(out)[1].lower() if out else ""
                                    _delivery_name = job.get("output_filename") or os.path.basename(out or "output")
                                    if _output_ext in (".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".opus"):
                                        kind = "audio"
                                    elif _vid_width is not None or _probe_vm:
                                        kind = "video"
                                    elif out and str(out).lower().endswith(".zip"):
                                        kind = "zip"
                                    elif out and str(out).lower().endswith((".mp4", ".mov", ".mkv")):
                                        kind = "video"
                                    else:
                                        kind = "doc"
                                    logger.info(
                                        "Worker: Bot API probe for %s: kind=%s duration=%s width=%s height=%s",
                                        out,
                                        kind,
                                        _vid_duration,
                                        _vid_width,
                                        _vid_height,
                                    )
                                    try:
                                        # Use async Bot API methods directly and close the client when done
                                        async with Bot(token=bot_token, request=flood_gated_request()) as bot:
                                            if kind == "zip":
                                                # Attempt to attach a thumbnail when available
                                                # Local copy first: this run produced the thumbnail
                                                # itself, so the object in the bucket would be a
                                                # download of bytes we are already holding.
                                                thumb_path = _local_thumb_candidate(job)
                                                _temp_thumb = None
                                                try:
                                                    # Prefer explicit job field
                                                    cand = None if thumb_path else job.get("thumbnail")
                                                    if cand:
                                                        if os.path.exists(cand):
                                                            thumb_path = cand
                                                        else:
                                                            try:
                                                                backend = (
                                                                    await get_storage_backend()
                                                                    if get_storage_backend is not None
                                                                    else None
                                                                )
                                                            except Exception:
                                                                backend = None
                                                            if backend:
                                                                temp_dir = os.path.join(
                                                                    getattr(config, "TEMP_PATH", "storage/temp")
                                                                )
                                                                os.makedirs(temp_dir, exist_ok=True)
                                                                _temp_thumb = os.path.join(
                                                                    temp_dir,
                                                                    f"{job_id}_thumb{os.path.splitext(cand)[1] or '.jpg'}",
                                                                )
                                                                try:
                                                                    ok = await backend.download_file(cand, _temp_thumb)
                                                                    if ok:
                                                                        thumb_path = _temp_thumb
                                                                except Exception:
                                                                    _temp_thumb = None
                                                    # Fallback: check Redis-stored job hash for thumbnail
                                                    if not thumb_path:
                                                        try:
                                                            r = await get_redis()
                                                            stored = await r.hgetall(f"ffmpeg:job:{job_id}")
                                                            sval = (
                                                                stored.get("thumbnail")
                                                                or stored.get("thumb")
                                                                or stored.get("thumb_key")
                                                            )
                                                            if sval:
                                                                cand = sval
                                                                if os.path.exists(cand):
                                                                    thumb_path = cand
                                                                else:
                                                                    try:
                                                                        backend = (
                                                                            await get_storage_backend()
                                                                            if get_storage_backend is not None
                                                                            else None
                                                                        )
                                                                    except Exception:
                                                                        backend = None
                                                                    if backend:
                                                                        temp_dir = os.path.join(
                                                                            getattr(config, "TEMP_PATH", "storage/temp")
                                                                        )
                                                                        os.makedirs(temp_dir, exist_ok=True)
                                                                        _temp_thumb = os.path.join(
                                                                            temp_dir,
                                                                            f"{job_id}_thumb{os.path.splitext(cand)[1] or '.jpg'}",
                                                                        )
                                                                        try:
                                                                            ok = await backend.download_file(
                                                                                cand, _temp_thumb
                                                                            )
                                                                            if ok:
                                                                                thumb_path = _temp_thumb
                                                                        except Exception:
                                                                            _temp_thumb = None
                                                            aclose = getattr(r, "aclose", None)
                                                            if aclose is not None:
                                                                await aclose()
                                                            else:
                                                                await r.close()
                                                        except Exception:
                                                            logger.debug(
                                                                "ffmpeg worker: Fallback: check Redis-stored job hash for thumbnail"
                                                            )

                                                except Exception:
                                                    thumb_path = None

                                                _bot_up_cb = _make_upload_progress_callback(job_id, progress_channel)
                                                try:
                                                    with open(out, "rb") as fh:
                                                        fh = (
                                                            _ProgressFileWrapper(fh, file_size, _bot_up_cb)
                                                            if file_size
                                                            else fh
                                                        )
                                                        if thumb_path:
                                                            try:
                                                                with open(thumb_path, "rb") as tf:
                                                                    await bot.send_document(
                                                                        chat_id=chat_id,
                                                                        document=fh,
                                                                        caption=caption,
                                                                        thumbnail=tf,
                                                                    )
                                                            except Exception:
                                                                await bot.send_document(
                                                                    chat_id=chat_id, document=fh, caption=caption
                                                                )
                                                        else:
                                                            await bot.send_document(
                                                                chat_id=chat_id, document=fh, caption=caption
                                                            )
                                                finally:
                                                    try:
                                                        if _temp_thumb and os.path.exists(_temp_thumb):
                                                            os.remove(_temp_thumb)
                                                    except Exception:
                                                        logger.debug("ffmpeg worker: operation failed")
                                            elif kind == "audio":
                                                # Sent via ``send_audio`` so it arrives as
                                                # streamable Telegram audio. Bot API infers
                                                # the MIME type from the filename, so no
                                                # mime_type argument is passed (send_audio
                                                # does not accept one).
                                                _bot_up_cb = _make_upload_progress_callback(job_id, progress_channel)
                                                with open(out, "rb") as fh:
                                                    fh = (
                                                        _ProgressFileWrapper(fh, file_size, _bot_up_cb)
                                                        if file_size
                                                        else fh
                                                    )
                                                    await bot.send_audio(
                                                        chat_id=chat_id,
                                                        audio=fh,
                                                        caption=caption,
                                                        title=os.path.splitext(_delivery_name)[0],
                                                        filename=_delivery_name,
                                                        performer="",
                                                        duration=int(_vid_duration)
                                                        if _vid_duration is not None
                                                        else None,
                                                    )
                                            elif kind == "video":
                                                # Try to attach thumbnail (thumb) when available
                                                # Local copy first - see _local_thumb_candidate.
                                                thumb_path = _local_thumb_candidate(job)
                                                _temp_thumb = None
                                                try:
                                                    cand = None if thumb_path else job.get("thumbnail")
                                                    if cand:
                                                        if os.path.exists(cand):
                                                            thumb_path = cand
                                                        else:
                                                            try:
                                                                backend = (
                                                                    await get_storage_backend()
                                                                    if get_storage_backend is not None
                                                                    else None
                                                                )
                                                            except Exception:
                                                                backend = None
                                                            if backend:
                                                                temp_dir = os.path.join(
                                                                    getattr(config, "TEMP_PATH", "storage/temp")
                                                                )
                                                                os.makedirs(temp_dir, exist_ok=True)
                                                                _temp_thumb = os.path.join(
                                                                    temp_dir,
                                                                    f"{job_id}_thumb{os.path.splitext(cand)[1] or '.jpg'}",
                                                                )
                                                                try:
                                                                    ok = await backend.download_file(cand, _temp_thumb)
                                                                    if ok:
                                                                        thumb_path = _temp_thumb
                                                                except Exception:
                                                                    _temp_thumb = None
                                                    if not thumb_path:
                                                        try:
                                                            r = await get_redis()
                                                            stored = await r.hgetall(f"ffmpeg:job:{job_id}")
                                                            sval = (
                                                                stored.get("thumbnail")
                                                                or stored.get("thumb")
                                                                or stored.get("thumb_key")
                                                            )
                                                            if sval:
                                                                cand = sval
                                                                if os.path.exists(cand):
                                                                    thumb_path = cand
                                                                else:
                                                                    try:
                                                                        backend = (
                                                                            await get_storage_backend()
                                                                            if get_storage_backend is not None
                                                                            else None
                                                                        )
                                                                    except Exception:
                                                                        backend = None
                                                                    if backend:
                                                                        temp_dir = os.path.join(
                                                                            getattr(config, "TEMP_PATH", "storage/temp")
                                                                        )
                                                                        os.makedirs(temp_dir, exist_ok=True)
                                                                        _temp_thumb = os.path.join(
                                                                            temp_dir,
                                                                            f"{job_id}_thumb{os.path.splitext(cand)[1] or '.jpg'}",
                                                                        )
                                                                        try:
                                                                            ok = await backend.download_file(
                                                                                cand, _temp_thumb
                                                                            )
                                                                            if ok:
                                                                                thumb_path = _temp_thumb
                                                                        except Exception:
                                                                            _temp_thumb = None
                                                            aclose = getattr(r, "aclose", None)
                                                            if aclose is not None:
                                                                await aclose()
                                                            else:
                                                                await r.close()
                                                        except Exception:
                                                            logger.debug("ffmpeg worker: operation failed")

                                                    # Use thumbnail from _probe_output_metadata if not already set
                                                    if not thumb_path and _probe_tp:
                                                        thumb_path = _probe_tp
                                                        _temp_thumb = _probe_tp
                                                except Exception:
                                                    thumb_path = None

                                                await _send_video_result(
                                                    bot,
                                                    chat_id,
                                                    out,
                                                    caption=caption,
                                                    file_size=file_size,
                                                    progress_channel=progress_channel,
                                                    job_id=job_id,
                                                    thumb_path=thumb_path,
                                                    _temp_thumb=_temp_thumb,
                                                    vid_duration=_vid_duration,
                                                    vid_width=_vid_width,
                                                    vid_height=_vid_height,
                                                )
                                            else:
                                                # non-video non-zip fallback
                                                # Local copy first - see _local_thumb_candidate.
                                                thumb_path = _local_thumb_candidate(job)
                                                _temp_thumb = None
                                                try:
                                                    cand = None if thumb_path else job.get("thumbnail")
                                                    if cand and os.path.exists(cand):
                                                        thumb_path = cand
                                                    elif cand:
                                                        try:
                                                            backend = (
                                                                await get_storage_backend()
                                                                if get_storage_backend is not None
                                                                else None
                                                            )
                                                        except Exception:
                                                            backend = None
                                                        if backend:
                                                            temp_dir = os.path.join(
                                                                getattr(config, "TEMP_PATH", "storage/temp")
                                                            )
                                                            os.makedirs(temp_dir, exist_ok=True)
                                                            _temp_thumb = os.path.join(
                                                                temp_dir,
                                                                f"{job_id}_thumb{os.path.splitext(cand)[1] or '.jpg'}",
                                                            )
                                                            try:
                                                                ok = await backend.download_file(cand, _temp_thumb)
                                                                if ok:
                                                                    thumb_path = _temp_thumb
                                                            except Exception:
                                                                _temp_thumb = None
                                                except Exception:
                                                    thumb_path = None

                                                _bot_up_cb = _make_upload_progress_callback(job_id, progress_channel)
                                                try:
                                                    with open(out, "rb") as fh:
                                                        fh = (
                                                            _ProgressFileWrapper(fh, file_size, _bot_up_cb)
                                                            if file_size
                                                            else fh
                                                        )
                                                        if thumb_path:
                                                            try:
                                                                with open(thumb_path, "rb") as tf:
                                                                    await bot.send_document(
                                                                        chat_id=chat_id,
                                                                        document=fh,
                                                                        caption=caption,
                                                                        thumbnail=tf,
                                                                    )
                                                            except Exception:
                                                                await bot.send_document(
                                                                    chat_id=chat_id, document=fh, caption=caption
                                                                )
                                                        else:
                                                            await bot.send_document(
                                                                chat_id=chat_id, document=fh, caption=caption
                                                            )
                                                finally:
                                                    try:
                                                        if _temp_thumb and os.path.exists(_temp_thumb):
                                                            os.remove(_temp_thumb)
                                                    except Exception:
                                                        logger.debug("ffmpeg worker: operation failed")
                                        sent = True
                                    except asyncio.CancelledError:
                                        pass  # cancelled, sent already False
                                    except Exception as e:
                                        logger.warning("Bot API send failed for job %s: %s", job_id, e)
                                        sent = False
                                except asyncio.CancelledError:
                                    pass  # cancelled, sent already False
                                except Exception as e:
                                    logger.warning("Bot init failed for job %s: %s", job_id, e)
                                    sent = False

                        # Fallback: if not sent and userbot is enabled, attempt userbot.
                        # Only runs when the preferred userbot path above did NOT already run
                        # (i.e. for files small enough for Bot API).  Without this guard the
                        # fallback would re-call send_file_via_userbot after the preferred path
                        # already tried all 3 methods — causing a duplicate full upload.
                        if not sent and chat_id and enable_userbot and file_size <= bot_api_max_bytes:
                            if await _check_upload_cancelled(job_id):
                                logger.info(
                                    "Upload cancelled by user — skipping fallback userbot send for job %s", job_id
                                )
                            else:
                                try:
                                    from utils.userbot_uploader import send_file_via_userbot

                                    # Direct userbot delivery (fallback): sends directly to user's DM via MTProto.
                                    # This preserves all video metadata and works for files of any size.
                                    _up_cb = _make_upload_progress_callback(job_id, progress_channel)
                                    _pre_am = None
                                    if _media_kind == "audio":
                                        _pre_vm, _pre_tp = (None, None)
                                        _pre_am = await _probe_audio_delivery(out, _delivery_name)
                                    else:
                                        _pre_vm, _pre_tp = await _probe_output_metadata(out)
                                    try:
                                        ok = await send_file_via_userbot(
                                            chat_id,
                                            out,
                                            caption=caption,
                                            progress_callback=_up_cb,
                                            video_meta=_pre_vm,
                                            thumb_path=_pre_tp,
                                            audio_meta=_pre_am,
                                            user_id=job.get("user_id"),
                                            media_kind=_media_kind,
                                            delivery_name=_delivery_name,
                                        )
                                    finally:
                                        if _pre_tp:
                                            with contextlib.suppress(Exception):
                                                safe_rmtree(os.path.dirname(_pre_tp))
                                    if ok:
                                        logger.info("Sent output via userbot (direct) for job %s", job_id)
                                        sent = True
                                    else:
                                        logger.error("Userbot send failed for job %s", job_id)
                                except Exception:
                                    logger.exception("Userbot fallback raised exception for job %s", job_id)

                        if not sent and chat_id:
                            logger.warning(
                                "Could not deliver output for job %s — neither Bot API nor userbot succeeded", job_id
                            )

                        # The local thumbnail copy has served its purpose: it was
                        # only ever an optimisation for this delivery. Remove the
                        # private directory it lives in so a job cannot leave one
                        # behind (the temp sweep would get it eventually, but a
                        # busy hour would hold one per job until then).
                        _cleanup_local_thumb(job)

                        # A delivery only Telegram's flood window refused is not a
                        # lost delivery: hand it to the deferred queue, which sends
                        # it once the window closes. Reported as still "processing"
                        # so the progress watcher stays alive and renders the result
                        # when the file actually goes out.
                        if not sent and chat_id:
                            _deferred_window = await _defer_delivery(
                                job,
                                chat_id=chat_id,
                                output=out,
                                output_key=dest,
                                delivery_name=_delivery_name,
                                media_kind=_media_kind,
                                caption=caption,
                                get_url=get_url,
                                reason="telegram flood control",
                            )

                        # Set final job status on Redis hash so _watch_job_progress (and web UI) can see it.
                        try:
                            if _deferred_window:
                                _final_status = "processing"
                                _final_msg = _DEFERRED_MESSAGE
                            else:
                                _final_status = "done" if sent else "error"
                                _final_msg = "delivered to Telegram" if sent else "delivery failed"
                            # Lifecycle event: the job reached its end. Published
                            # before the Redis hash is deleted, and best-effort, so
                            # the log records the outcome even if the UI state goes.
                            # A deferred delivery has not ended, so it emits nothing
                            # here - the sweep emits the terminal event it reaches.
                            if _deferred_window is None:
                                await emit_event(
                                    JOB_COMPLETED if sent else JOB_FAILED,
                                    job=job,
                                    payload={"status": _final_status, "message": _final_msg, "progress": 100},
                                    source="worker",
                                )
                            _r = await get_redis()
                            try:
                                await _r.hset(
                                    f"ffmpeg:job:{job_id}",
                                    mapping={
                                        "status": _final_status,
                                        "progress": "100",
                                        "message": _final_msg,
                                        # The user has the file. A broker redelivery
                                        # after this point must not convert and send
                                        # it a second time (see _job_already_delivered).
                                        "delivered": "1" if sent else "0",
                                        # Tells the sweeper (and an operator reading the
                                        # hash) that a retry is queued for this job.
                                        "delivery_deferred": "1" if _deferred_window else "0",
                                    },
                                )
                                await publish_update(
                                    progress_channel,
                                    {
                                        "job_id": job_id,
                                        "progress": 100,
                                        "message": _final_msg,
                                        "status": _final_status,
                                    },
                                )
                                # Keep the terminal hash until the watcher observes it. The
                                # watcher owns the Telegram progress-message lifecycle; deleting
                                # this hash here makes it wait forever and leaves the message stuck.
                                # JOB_METADATA_TTL and periodic cleanup remove it later.
                            finally:
                                with contextlib.suppress(Exception):
                                    await _r.close()
                            if _deferred_window:
                                logger.info(
                                    "Job %s: delivery deferred for ~%.0fs (Telegram flood control); "
                                    "the worker will retry the send",
                                    job_id,
                                    _deferred_window,
                                )
                            else:
                                logger.info(
                                    "Job %s: final status=%s message=%s (Redis hash deleted)",
                                    job_id,
                                    _final_status,
                                    _final_msg,
                                )
                        except Exception:
                            logger.debug("ffmpeg worker: operation failed")

                    except Exception:
                        logger.exception("Failed to send result via Telegram")

                    try:
                        # A deferred delivery still needs this file: it is the only
                        # copy when nothing else reads the result. The sweeper
                        # removes it once the send actually lands.
                        if not _deferred_window and job.get("cleanup_output", False) and out and os.path.exists(out):
                            os.remove(out)
                    except Exception:
                        logger.debug("ffmpeg worker: in _probe()")

                    try:
                        if temp_input and os.path.exists(temp_input):
                            os.remove(temp_input)
                    except Exception:
                        logger.debug("ffmpeg worker: in _probe()")

                        # cancel memory sampler if running
                        try:
                            if memory_sampler_task:
                                memory_sampler_task.cancel()
                                with contextlib.suppress(Exception):
                                    await memory_sampler_task
                        except Exception:
                            logger.debug("ffmpeg worker: cancel memory sampler if running")

                    return

                else:
                    # Detect likely truncated/corrupt input errors and attempt a re-download
                    lower_err = (str(info) or "").lower()
                    # Detect container/codec related errors that can often be worked
                    # around by remuxing into a more permissive container (MKV)
                    container_indicators = (
                        "could not find tag for codec",
                        "codec not currently supported in container",
                        "could not write header",
                        "incorrect codec parameters",
                        "nothing was written into output file",
                        "error sending frames to consumers",
                    )
                    corruption_indicators = (
                        "moov atom not found",
                        "invalid data found",
                        "error opening input",
                        "truncated",
                        "premature eof",
                        "could not find codec parameters",
                    )

                    try:
                        # 1) Container/codec mismatch -> try remuxing to MKV and retry
                        if any(k in lower_err for k in container_indicators) and input_path:
                            try:
                                r = await get_redis()
                            except Exception:
                                r = None

                            remux_attempts = 0
                            try:
                                if r:
                                    cur = await r.hget(f"ffmpeg:job:{job_id}", "remux_attempts")
                                    if cur:
                                        try:
                                            if isinstance(cur, bytes):
                                                cur = cur.decode()
                                            remux_attempts = int(cur or 0)
                                        except Exception:
                                            remux_attempts = 0
                            except Exception:
                                remux_attempts = 0

                            max_remux = int(os.environ.get("MAX_REMUX_ATTEMPTS", "1"))
                            if remux_attempts < max_remux:
                                try:
                                    temp_dir = os.path.join(getattr(config, "TEMP_PATH", "storage/temp"))
                                    os.makedirs(temp_dir, exist_ok=True)
                                    remux_path = os.path.join(temp_dir, f"{job_id}_remux.mkv")
                                    ffmpeg_bin = getattr(config, "FFMPEG_PATH", "ffmpeg")
                                    cmd = [
                                        ffmpeg_bin,
                                        "-y",
                                        "-hide_banner",
                                        "-loglevel",
                                        "error",
                                        "-i",
                                        input_path,
                                        "-c",
                                        "copy",
                                        remux_path,
                                    ]
                                    try:
                                        proc = await asyncio.to_thread(
                                            subprocess.run, cmd, capture_output=True, text=True, timeout=600
                                        )
                                    except Exception:
                                        proc = None

                                    ok = False
                                    if (
                                        proc
                                        and getattr(proc, "returncode", 1) == 0
                                        and os.path.exists(remux_path)
                                        and os.path.getsize(remux_path) > 0
                                    ):
                                        ok = True

                                    # Persist attempt count
                                    try:
                                        if r:
                                            await r.hset(
                                                f"ffmpeg:job:{job_id}",
                                                mapping={"remux_attempts": str(remux_attempts + 1)},
                                            )
                                    except Exception:
                                        logger.debug("ffmpeg worker: Persist attempt count")

                                    if ok:
                                        with contextlib.suppress(Exception):
                                            await publish_update(
                                                progress_channel,
                                                {
                                                    "job_id": job_id,
                                                    "progress": 0,
                                                    "message": "remuxed",
                                                    "note": "remux succeeded; retrying",
                                                },
                                            )
                                        logger.info(
                                            "Remux succeeded for job %s, retrying ffmpeg against %s", job_id, remux_path
                                        )
                                        # Switch to remuxed input and retry
                                        input_path = remux_path
                                        job["input_path"] = remux_path
                                        # close redis client if opened
                                        try:
                                            if r:
                                                await r.close()
                                        except Exception:
                                            logger.debug("ffmpeg worker: close redis client if opened")
                                        continue
                                    else:
                                        with contextlib.suppress(Exception):
                                            await publish_update(
                                                progress_channel,
                                                {
                                                    "job_id": job_id,
                                                    "progress": 0,
                                                    "message": "remux_failed",
                                                    "note": "remux attempted and failed",
                                                },
                                            )
                                except Exception:
                                    logger.exception("Remux attempt failed for job %s", job_id)

                            try:
                                if r:
                                    await r.close()
                            except Exception:
                                logger.debug("ffmpeg worker: operation failed")

                        # 2) Truncated/corrupt input -> try re-download (existing logic)
                        if any(k in lower_err for k in corruption_indicators) and input_path:
                            try:
                                r = await get_redis()
                            except Exception:
                                r = None

                            redownload_attempts = 0
                            try:
                                if r:
                                    cur = await r.hget(f"ffmpeg:job:{job_id}", "redownload_attempts")
                                    if cur:
                                        try:
                                            if isinstance(cur, bytes):
                                                cur = cur.decode()
                                            redownload_attempts = int(cur or 0)
                                        except Exception:
                                            redownload_attempts = 0
                            except Exception:
                                redownload_attempts = 0

                            max_redownload = int(os.environ.get("MAX_REDOWNLOAD_ATTEMPTS", "1"))
                            if redownload_attempts < max_redownload:
                                # Remove possibly-corrupt file and attempt to re-fetch using available metadata
                                try:
                                    if os.path.exists(input_path):
                                        with contextlib.suppress(Exception):
                                            os.remove(input_path)
                                except Exception:
                                    logger.debug(
                                        "ffmpeg worker: Remove possibly-corrupt file and attempt to re-fetch using ava..."
                                    )

                                ok = False
                                tried = False
                                # Try forward_hash if present
                                try:
                                    fh = job.get("forward_hash") or job.get("fh")
                                    if fh:
                                        try:
                                            from utils.forward_store import load_forward_metadata

                                            meta = await load_forward_metadata(fh)
                                            if meta:
                                                try:
                                                    from utils.userbot_downloader import download_forward_via_userbot

                                                    tried = True
                                                    ok = await download_forward_via_userbot(
                                                        meta.get("chat_id"),
                                                        meta.get("message_id") or meta.get("msg_id"),
                                                        input_path,
                                                        msg_date=meta.get("registered_at") or meta.get("created_at"),
                                                        file_unique_id=meta.get("file_unique_id"),
                                                        user_id=job.get("user_id"),
                                                    )
                                                except Exception:
                                                    ok = False
                                        except Exception:
                                            logger.debug("ffmpeg worker: operation failed")
                                except Exception:
                                    logger.debug("ffmpeg worker: Try forward_hash if present")

                                # Try direct chat/message metadata if available
                                if not tried and job.get("chat_id") and (job.get("message_id") or job.get("msg_id")):
                                    try:
                                        from utils.userbot_downloader import download_forward_via_userbot

                                        tried = True
                                        ok = await download_forward_via_userbot(
                                            job.get("chat_id"),
                                            job.get("message_id") or job.get("msg_id"),
                                            input_path,
                                            user_id=job.get("user_id"),
                                        )
                                    except Exception:
                                        ok = False

                                # Try HTTP source_url if available
                                if not tried and job.get("source_url"):
                                    try:
                                        tried = True
                                        async with aiohttp.ClientSession() as session:
                                            async with session.get(
                                                # SSRF hardening: never follow redirects. The URL was
                                                # pre-validated by webapp.py; a redirect to an internal
                                                # address would bypass that validation.
                                                job.get("source_url"),
                                                timeout=aiohttp.ClientTimeout(total=60),
                                                allow_redirects=False,
                                            ) as resp:
                                                if resp.status == 200:
                                                    with open(input_path, "wb") as fh:
                                                        async for chunk in resp.content.iter_chunked(1024 * 64):
                                                            fh.write(chunk)
                                                    ok = os.path.exists(input_path) and os.path.getsize(input_path) > 0
                                                else:
                                                    ok = False
                                    except Exception:
                                        ok = False

                                # Persist redownload attempts
                                try:
                                    if r:
                                        await r.hset(
                                            f"ffmpeg:job:{job_id}",
                                            mapping={"redownload_attempts": str(redownload_attempts + 1)},
                                        )
                                except Exception:
                                    logger.debug("ffmpeg worker: Persist redownload attempts")
                                try:
                                    if r:
                                        await r.close()
                                except Exception:
                                    logger.debug("ffmpeg worker: operation failed")

                                if ok:
                                    with contextlib.suppress(Exception):
                                        await publish_update(
                                            progress_channel,
                                            {
                                                "job_id": job_id,
                                                "progress": 0,
                                                "message": "redownloaded",
                                                "note": "re-download succeeded; retrying",
                                            },
                                        )
                                    logger.info("Redownload succeeded for job %s, retrying ffmpeg", job_id)
                                    # Retry immediately
                                    continue
                                else:
                                    with contextlib.suppress(Exception):
                                        await publish_update(
                                            progress_channel,
                                            {
                                                "job_id": job_id,
                                                "progress": 0,
                                                "message": "redownload_failed",
                                                "note": "re-download attempted and failed",
                                            },
                                        )

                    except Exception:
                        logger.exception("Error during re-download attempt for job %s", job_id)

                    # If we attempted re-download and it failed, fall through to normal failure handling
                    JOBS_FAILED.inc()
                    await publish_update(
                        progress_channel, {"job_id": job_id, "progress": 0, "message": "error", "error": info}
                    )
                    try:
                        await job_store.update_job(job_id, {"status": "error", "error": info, "attempt": attempt})
                        try:
                            if _cache:
                                await _cache.cache_job_metadata(
                                    job_id,
                                    {
                                        "status": "error",
                                        "error": info,
                                        "attempt": attempt,
                                    },
                                    ttl=1800,
                                )
                        except Exception:
                            logger.debug("ffmpeg worker: operation failed")
                    except Exception:
                        logger.debug("ffmpeg worker: operation failed")

                    if attempt <= retries:
                        backoff = min(30, 2**attempt)
                        logger.info(f"Retrying job {job_id} in {backoff}s (attempt {attempt})")
                        await asyncio.sleep(backoff)
                        continue
                    else:
                        # Only the last attempt ends the job's life; intermediate
                        # attempts are retried just above.
                        await emit_event(
                            JOB_FAILED,
                            job=job,
                            payload={"status": "error", "error": info, "attempt": attempt},
                            source="worker",
                        )
                        # Terminal in the hash as well, or the bot's watchdog keeps
                        # polling a job the worker has already given up on.
                        await _set_job_state(
                            job_id,
                            "error",
                            str(info or "conversion failed"),
                            progress=0,
                            channel=progress_channel,
                        )
                        return

            except asyncio.CancelledError:
                logger.info("Job cancelled via worker shutdown")
                with contextlib.suppress(Exception):
                    await job_store.update_job(job_id, {"status": "cancelled", "message": "shutdown"})
                await emit_event(
                    JOB_CANCELLED,
                    job=job,
                    payload={"status": "cancelled", "message": "shutdown"},
                    source="worker",
                )
                raise
            except Exception as e:
                JOBS_FAILED.inc()
                logger.exception("Unhandled exception while processing job: %s", e)
                with contextlib.suppress(Exception):
                    await job_store.update_job(
                        job_id, {"status": "error", "error": "processing_failed", "attempt": attempt}
                    )
                if attempt <= retries:
                    await asyncio.sleep(2**attempt)
                    continue
                else:
                    await emit_event(
                        JOB_FAILED,
                        job=job,
                        payload={"status": "error", "error": "processing_failed", "attempt": attempt},
                        source="worker",
                    )
                    # Same reason as above: the hash is what the bot reads.
                    await _set_job_state(job_id, "error", "processing failed", progress=0, channel=progress_channel)
                    return
            finally:
                ACTIVE_JOBS.dec()
    finally:
        # release the input lock if we acquired one
        try:
            if lock_key and lock_acquired and job_id:
                _released = await release_input_lock(lock_key, job_id, redis_client=redis_lock_client)
                if _released:
                    LOCKS_CLEANED.inc()
        except Exception:
            logger.warning("Failed to release input lock %s for job %s", lock_key, job_id)
        finally:
            try:
                if redis_lock_client:
                    await redis_lock_client.close()
            except Exception:
                logger.debug("ffmpeg worker: operation failed")


async def _start_healthcheck_server():
    """Start a minimal aiohttp server for Railway healthchecks on $PORT."""
    try:
        from aiohttp import web

        port = int(os.environ.get("HEALTHCHECK_PORT", os.environ.get("PORT", "8000")))

        async def _handle_health(request):
            return web.json_response({"service": "ffmpeg_worker", "healthy": True, "ok": True})

        app = web.Application()
        app.router.add_get("/health", _handle_health)
        runner = web.AppRunner(app)
        await runner.setup()
        host = os.environ.get("HEALTHCHECK_HOST", "0.0.0.0")  # nosec  # noqa: S104
        site = web.TCPSite(runner, host, port)
        await site.start()
        logger.info("Healthcheck server started on %s:%s/health", host, port)
        return runner
    except Exception:
        logger.exception("Failed to start healthcheck server")
        return None


async def _publish_worker_rss(rss=None, *, force: bool = False) -> None:
    """Record this worker's RSS for the dashboard's capacity view.

    Called with a fresh, post-cleanup number after every job, and on a slow timer
    while idle, so the dashboard shows a live figure rather than a stale one.
    """
    global _last_rss_publish
    now = time.time()
    if not force and now - _last_rss_publish < _RSS_HEARTBEAT_SECONDS:
        return
    _last_rss_publish = now
    with contextlib.suppress(Exception):
        await batch_pipeline.publish_worker_rss(rss=rss)


async def _consume_forced_restart(*, force: bool = False) -> bool:
    """Honour an admin-requested restart, if one is pending.

    Consumes the Redis request, so it is acted on once and a worker that comes
    back up does not restart again in a loop. Only the standalone worker ever
    calls this with ``allow_restart`` set - the bot-hosted worker must not exit.
    """
    global _last_restart_probe
    now = time.time()
    if not force and now - _last_restart_probe < _RESTART_PROBE_SECONDS:
        return False
    _last_restart_probe = now
    try:
        r = await get_redis()
    except Exception:
        return False
    try:
        token = await batch_pipeline.consume_worker_restart(r)
    except Exception:
        token = None
    finally:
        with contextlib.suppress(Exception):
            await r.close()
    if token is None:
        return False
    batch_pipeline.request_restart(f"requested by {token}")
    return True


async def _maybe_stop_for_restart(allow_restart: bool, *, force: bool = False) -> bool:
    """Check for a restart at a safe point and report whether to exit now.

    Guarded on :data:`_jobs_in_flight` so a planned restart never truncates work
    that is already running; the next idle moment picks the request up instead.
    """
    if not allow_restart or _jobs_in_flight:
        return False
    await _consume_forced_restart(force=force)
    if batch_pipeline.restart_requested():
        logger.warning("Exiting so the container restarts with a clean heap (no job in flight)")
        return True
    return False


async def _claim_execution_slot(job: dict):
    """Reserve the one global ffmpeg slot (and the batch lock) for this job.

    Returns the slot index to hand to
    :func:`~utils.batch_pipeline.finalize_job` when this worker may run the job
    (``-1`` when Redis could not be checked and the job runs unfenced), or
    ``None`` when it may not - in which case the job has been returned to the
    delayed set and the worker moves on to other work.

    This is the gate the "only one ffmpeg ever runs" guarantee rests on:
    however many replicas or services are up, Redis hands out one conversion
    slot, so two ffmpeg processes never overlap and their memory peaks cannot
    compound. A job that belongs to a bulk batch additionally has to hold that
    batch's lock, which keeps a 30-file Apply Bulk strictly sequential.
    """
    try:
        r = await get_redis()
    except Exception:
        # No Redis means no fencing is possible; running beats dropping.
        return -1
    try:
        batch_id = batch_pipeline.job_batch_id(job)
        if batch_id and await batch_pipeline.is_batch_cancelled(r, batch_id):
            logger.info(
                "Batch %s already cancelled: dropping deferred job %s",
                batch_id,
                job.get("job_id"),
            )
            with contextlib.suppress(Exception):
                await r.hset(
                    f"ffmpeg:job:{job.get('job_id')}",
                    mapping={"cancel": "1", "status": "cancelled", "message": "batch cancelled"},
                )
            return None
        # 1. Memory ceiling: do not start a conversion on a process that is
        #    still holding a previous job's memory. A bounded number of defers
        #    keeps a mis-set ceiling from stalling the queue forever.
        if batch_pipeline.over_memory_ceiling():
            defers = int(job.get(batch_pipeline.CEILING_DEFER_FIELD) or 0)
            if defers < batch_pipeline.MEMORY_CEILING_MAX_DEFERS:
                job[batch_pipeline.CEILING_DEFER_FIELD] = defers + 1
                batch_pipeline.reclaim_memory("memory ceiling")
                deferred = await batch_pipeline.defer_batch_job(r, job)
                with contextlib.suppress(Exception):
                    await r.hset(
                        f"ffmpeg:job:{job.get('job_id')}",
                        mapping={"status": "waiting", "progress": "0", "message": "waiting for memory cleanup"},
                    )
                logger.warning(
                    "Memory ceiling reached (RSS %.1fMB >= %.1fMB): deferring job %s (%s)",
                    batch_pipeline.rss_bytes() / 1024 / 1024,
                    batch_pipeline.MEMORY_CEILING_BYTES / 1024 / 1024,
                    job.get("job_id"),
                    "requeued" if deferred else "defer failed",
                )
                return None
            batch_pipeline.request_restart_if_pressured()
            logger.warning(
                "Memory ceiling still exceeded after %d deferrals; running job %s anyway",
                defers,
                job.get("job_id"),
            )

        # 2. The global conversion slot - one ffmpeg at a time, everywhere.
        slot = await batch_pipeline.acquire_ffmpeg_slot(r, job.get("job_id"))
        if slot is None:
            deferred = await batch_pipeline.defer_batch_job(r, job)
            with contextlib.suppress(Exception):
                await r.hset(
                    f"ffmpeg:job:{job.get('job_id')}",
                    mapping={"status": "waiting", "progress": "0", "message": "waiting for another conversion"},
                )
            logger.info(
                "ffmpeg slot busy: deferred job %s%s",
                job.get("job_id"),
                "" if deferred else " (defer failed)",
            )
            return None

        # 3. One job per batch, so an Apply Bulk stays in order and never runs
        #    two of its own files at once.
        if batch_id and not await batch_pipeline.try_acquire_batch_lock(r, batch_id, job.get("job_id")):
            # This job must not hold the global slot while it waits on its batch,
            # or a single busy batch would freeze every other conversion.
            await batch_pipeline.release_ffmpeg_slot(r, slot, job.get("job_id"))
            deferred = await batch_pipeline.defer_batch_job(r, job)
            with contextlib.suppress(Exception):
                await r.hset(
                    f"ffmpeg:job:{job.get('job_id')}",
                    mapping={"status": "waiting", "progress": "0", "message": "waiting for batch lock"},
                )
            logger.info(
                "Batch %s busy: deferred job %s%s",
                batch_id,
                job.get("job_id"),
                "" if deferred else " (defer failed)",
            )
            return None
        # Stamp which worker owns this claim. The ghost-claim checks read it with
        # this worker's heartbeat to tell a claim whose owner died mid-encode
        # (OOM kill, redeploy) from one that is genuinely still converting, so a
        # claim left by a dead worker is freed as soon as its heartbeat expires
        # instead of holding the one conversion slot for its full TTL.
        with contextlib.suppress(Exception):
            await r.hset(
                f"ffmpeg:job:{job.get('job_id')}",
                mapping={"worker": batch_pipeline.worker_identity()},
            )
        return slot
    finally:
        with contextlib.suppress(Exception):
            await r.close()


# How often the batch message refreshes with the running file's progress. Kept
# above Telegram's comfortable edit rate (and the 2s the single-file watcher
# uses) so a long conversion does not trip a 429.
BATCH_PROGRESS_INTERVAL = float(os.environ.get("BATCH_PROGRESS_INTERVAL", "3.0"))


def _batch_view_rows(batches) -> list[str]:
    """One row per running batch, least-finished first, for the aggregate view.

    This is the whole of "show every batch on one bar": a render over the
    counters that already exist. Nothing schedules, aggregates or caches it.
    """
    rows = []
    for row in batches or []:
        try:
            done, total = int(row.get("done", 0)), int(row.get("total", 0))
        except (TypeError, ValueError):
            continue
        if total <= 0:
            continue
        rows.append(f"{batch_pipeline.progress_bar(done, total)} {done}/{total}  #{str(row.get('batch_id', ''))[:8]}")
        if len(rows) >= batch_pipeline.BATCH_VIEW_MAX_ROWS:
            break
    return rows


def _batch_progress_text(done: int, total: int, *, name: str = "", pct=None, batches=None) -> str:
    """The batch's one message: how far this batch is, plus every other batch.

    ``batches`` is the aggregate view of every batch still running, so a user
    with several applies in flight reads them from one message instead of
    juggling one message per batch.
    """
    text = f"📊 Processing one at a time — {done} of {total} finished"
    if name:
        if pct is None:
            text += f"\n✅ {name}"
        else:
            text += f"\n🔄 {name} — {int(pct)}%"
    rows = _batch_view_rows(batches)
    if len(rows) > 1:
        text += "\n\n🗂 Batches\n" + "\n".join(rows)
    return text


def _batch_cancel_keyboard(batch_id):
    """The one button a batch needs: stop the rest without typing an id.

    The ``batch_id`` travels in the callback data, so the user never has to read
    it off the message or copy it into ``/cancelbatch``.
    """
    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        return InlineKeyboardMarkup([[InlineKeyboardButton("⏹️ Stop batch", callback_data=f"batch_cancel:{batch_id}")]])
    except Exception:
        return None


async def _read_batch_view(r) -> list[dict]:
    """The aggregate view's input, or an empty list if it cannot be read."""
    try:
        return await batch_pipeline.read_active_batches(r)
    except Exception:
        return []


async def _batch_state(r, batch_id, job) -> dict:
    """Read a batch's counters and the location of its single progress message."""
    try:
        total = int(job.get(batch_pipeline.BATCH_TOTAL_FIELD) or 0)
    except (TypeError, ValueError):
        total = 0
    # Prefer the exact enqueued count the bot recorded: the payload total is the
    # number of collected files, and files skipped at enqueue time would
    # otherwise leave the batch looking unfinished forever.
    with contextlib.suppress(Exception):
        recorded = await r.get(batch_pipeline.batch_total_key(batch_id))
        if recorded is not None:
            total = int(recorded)
    state = {"total": total, "done": 0, "stored": None, "batches": []}
    with contextlib.suppress(Exception):
        state["done"] = int(await r.get(batch_pipeline.batch_progress_key(batch_id)) or 0)
    with contextlib.suppress(Exception):
        state["stored"] = await r.get(batch_pipeline.batch_message_key(batch_id))
    state["batches"] = await _read_batch_view(r)
    return state


async def _set_batch_message(bot, r, batch_id, chat_id, stored, text):
    """Set the batch's single progress message, editing it in place.

    Returns the ``chat_id:message_id`` reference now holding the text. A message
    that has been deleted (or is too old to edit) is replaced with a fresh one,
    but a rate limit or transient error keeps the existing message instead of
    spraying duplicates.
    """
    from telegram.error import BadRequest, RetryAfter

    keyboard = _batch_cancel_keyboard(batch_id)
    parsed = _parse_batch_message_ref(stored) if stored else None
    if parsed is not None:
        _edit_chat_id, _edit_message_id = parsed[0], parsed[1]
        _flood_scope = telegram_flood_gate.scope_for_chat(_edit_chat_id)
        # The bot's own watchers edit this same bar, so stay off the wire entirely
        # while a flood window is open. min_interval=0 because every report here is
        # a discrete event (file n of m finished): dropping one would leave the bar
        # a file behind. Only an exact repeat of the current text is skipped.
        if telegram_edit_coalescer.should_skip(_edit_chat_id, _edit_message_id, text, min_interval=0.0):
            return stored
        if await telegram_flood_gate.should_drop_inline(_flood_scope):
            return stored
        try:
            await bot.edit_message_text(
                chat_id=_edit_chat_id, message_id=_edit_message_id, text=text, reply_markup=keyboard
            )
            telegram_edit_coalescer.record(_edit_chat_id, _edit_message_id, text)
            return stored
        except RetryAfter as exc:
            # Never sleep off a long window here: the bar simply stays at its last
            # text until Telegram lets the bot write again.
            _left = await telegram_flood_gate.note(getattr(exc, "retry_after", None) or 5, _flood_scope)
            logger.warning(
                "ffmpeg worker: Telegram flood control for %.0fs; keeping the batch bar for %s as is",
                _left,
                batch_id,
            )
            return stored
        except BadRequest:
            # Gone (deleted, or past the edit window) - post a replacement below,
            # and drop what we knew about the id since it is about to be reused for
            # a message with no relation to the text that used to be there.
            telegram_edit_coalescer.forget(_edit_chat_id, _edit_message_id)
            logger.debug("ffmpeg worker: batch message for %s is gone, reposting", batch_id)
        except Exception:
            logger.debug("ffmpeg worker: could not edit batch message for %s", batch_id)
            return stored

    # A batch's message is removed on purpose once the batch is over: the bot
    # deletes it after waiting for every job it queued, and a stop deletes it
    # immediately. A report that lands just after that must not post a fresh bar
    # that nothing will ever take down - so check before creating one.
    if await batch_pipeline.is_batch_cancelled(r, batch_id):
        logger.debug("ffmpeg worker: batch %s is over; not reposting its message", batch_id)
        return stored

    sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
    ref = f"{chat_id}:{getattr(sent, 'message_id', '')}"
    with contextlib.suppress(Exception):
        await r.set(
            batch_pipeline.batch_message_key(batch_id),
            ref,
            ex=int(batch_pipeline.BATCH_STATE_TTL_SECONDS),
        )
    return ref


async def _batch_live_progress(job: dict) -> None:
    """Keep the batch's message showing the file currently being converted.

    Runs for as long as the job does (the caller cancels it when the job ends)
    and reads the live percentage ``run_ffmpeg`` writes into the job hash - the
    same number the single-file progress watcher shows. This is why the batch
    message can carry per-file progress without a second message: the worker is
    already the one place that knows both the batch count and the running file.
    """
    batch_id = batch_pipeline.job_batch_id(job)
    chat_id = job.get("chat_id")
    bot_token = getattr(config, "BOT_TOKEN", None)
    if not batch_id or not chat_id or not bot_token:
        return
    job_id = job.get("job_id")
    name = str(job.get("original_filename") or "").strip()
    try:
        r = await get_redis()
    except Exception:
        return
    last_text = None
    try:
        async with Bot(token=bot_token, request=flood_gated_request()) as bot:
            while True:
                try:
                    state = await _batch_state(r, batch_id, job)
                    if state["total"] <= 0:
                        return
                    pct = None
                    with contextlib.suppress(Exception):
                        raw = await r.hget(f"ffmpeg:job:{job_id}", "progress")
                        if raw is not None:
                            pct = float(raw)
                    text = _batch_progress_text(
                        state["done"], state["total"], name=name, pct=pct, batches=state["batches"]
                    )
                    if text != last_text:
                        state["stored"] = await _set_batch_message(bot, r, batch_id, chat_id, state["stored"], text)
                        last_text = text
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.debug("ffmpeg worker: batch live progress tick failed for %s", batch_id)
                await asyncio.sleep(BATCH_PROGRESS_INTERVAL)
    finally:
        with contextlib.suppress(Exception):
            await r.close()


async def _report_batch_progress(job: dict) -> None:
    """Advance the batch's one message when a file finishes, or take it down.

    A batch shows exactly **one** message, edited in place while files run and
    as they finish, so a 30-file apply cannot flood the chat. The message is
    deleted as soon as the last file finishes - whether it finished well or
    badly - and the counter is written before the reply is touched, so a
    Telegram failure never loses the batch's position.

    Every step is best-effort: progress reporting must never fail a job.
    """
    batch_id = batch_pipeline.job_batch_id(job)
    chat_id = job.get("chat_id")
    if not batch_id or not chat_id:
        return

    done_key = batch_pipeline.batch_progress_key(batch_id)
    msg_key = batch_pipeline.batch_message_key(batch_id)
    try:
        r = await get_redis()
    except Exception:
        return
    try:
        # A batch the user stopped has already had its state taken down
        # (``cancel_batch``); only its tombstone survives. Re-writing the done
        # counter here would resurrect it, and the next dry-run of
        # ``scripts/cleanup_stale_redis.py`` would show the same batch again.
        if await batch_pipeline.is_batch_cancelled(r, batch_id):
            return
        # The same question for a batch that was taken down without a marker: a
        # cancel-all tombstones only the batches that still had something to
        # stop, so one that finished and was then swept as stale is gone with no
        # marker at all. The INCR below would write its counter straight back,
        # and the message after it would post a bar for a batch nothing owns.
        if not await batch_pipeline.batch_owns_state(r, batch_id):
            return
        # Count each job exactly once. A retried delivery runs this block again,
        # and a plain INCR would then count one file twice - finishing the batch
        # early and removing its bar while files were still queued.
        if await batch_pipeline.claim_batch_progress_slot(r, batch_id, job.get("job_id")):
            done = int(await r.incr(done_key))
            with contextlib.suppress(Exception):
                await r.expire(done_key, int(batch_pipeline.BATCH_STATE_TTL_SECONDS))
        else:
            logger.debug("ffmpeg worker: job %s already counted toward batch %s", job.get("job_id"), batch_id)
            done = int(await r.get(done_key) or 0)
        state = await _batch_state(r, batch_id, job)
        total = state["total"]

        bot_token = getattr(config, "BOT_TOKEN", None)
        if not bot_token or total <= 0:
            return

        async with Bot(token=bot_token, request=flood_gated_request()) as bot:
            # Last file (or a replayed counter): the batch is over, so the
            # message has served its purpose and comes down.
            if done >= total:
                # Only ever deletes here, never posts: a finished batch must not
                # leave a fresh message behind if the bot already closed it out.
                await _delete_batch_message(bot, r, msg_key, state["stored"])
                with contextlib.suppress(Exception):
                    await batch_pipeline.unregister_active_batch(r, batch_id=batch_id)
                return
            name = str(job.get("original_filename") or "").strip()
            await _set_batch_message(
                bot,
                r,
                batch_id,
                chat_id,
                state["stored"],
                _batch_progress_text(done, total, name=name, batches=state["batches"]),
            )
    except Exception:
        logger.debug("ffmpeg worker: batch progress update failed for %s", batch_id)
    finally:
        with contextlib.suppress(Exception):
            await r.close()


def _parse_batch_message_ref(stored) -> tuple[int, int] | None:
    """Decode the ``chat_id:message_id`` a batch progress message is stored as."""
    return batch_pipeline.parse_batch_message_ref(stored)


async def _delete_batch_message(bot, redis, msg_key: str, stored) -> None:
    """Remove a batch's progress message and stop tracking it.

    The reference is only forgotten once the message is confirmed gone. A
    transient Telegram failure keeps it, so a later report (a redelivered job,
    or /cancelall's sweep, which reads the same key) retries the delete -
    otherwise a failed final delete would leave a bar claiming a finished batch
    forever, with nothing left that would ever take it down.
    """
    parsed = _parse_batch_message_ref(stored) if stored else None
    deleted = True
    if parsed is not None:
        with contextlib.suppress(Exception):
            telegram_edit_coalescer.forget(parsed[0], parsed[1])
        try:
            from telegram.error import BadRequest
        except Exception:
            BadRequest = None
        try:
            await bot.delete_message(chat_id=parsed[0], message_id=parsed[1])
        except Exception as exc:
            # Already gone (the stop path deleted it, or it aged out of the
            # edit window): done, forget the reference as planned.
            if BadRequest is not None and isinstance(exc, BadRequest):
                pass
            else:
                deleted = False
    if deleted:
        with contextlib.suppress(Exception):
            await redis.delete(msg_key)


async def _keep_claims_alive(job: dict, slot) -> None:
    """Re-arm the TTL on this job's conversion slot and batch lock while it runs.

    The TTLs are deliberately short so a worker that dies releases its claims in
    minutes instead of hours (that short TTL is what stops a redeploy from
    freezing a batch at 0%). This heartbeat is the other half of that bargain:
    it is what keeps a legitimately long conversion's slot and batch lock alive
    for as long as the job actually runs.

    It publishes this worker's *own* heartbeat on its own, faster timer. The
    ghost-claim checks ask whether the claiming worker is still heartbeating to
    tell a claim whose owner died mid-encode from one that is still converting,
    and the only other publisher is the idle loop - which is not running while a
    job is. Without this, a conversion longer than the heartbeat's TTL made its
    own worker look dead: the next job stole the live claim and started a second
    ffmpeg beside it, on a box whose whole design is one conversion at a time.
    """
    job_id = job.get("job_id")
    batch_id = batch_pipeline.job_batch_id(job)
    # Both default to half a minute or more; the floors only stop a mis-set
    # configuration from turning this into a hot loop.
    heartbeat_every = max(0.01, _RSS_HEARTBEAT_SECONDS)
    claim_every = max(0.01, batch_pipeline.CLAIM_HEARTBEAT_SECONDS)
    next_claim_refresh = time.time() + claim_every
    try:
        r = await get_redis()
    except Exception:
        return
    try:
        while True:
            await asyncio.sleep(heartbeat_every)
            with contextlib.suppress(Exception):
                await _publish_worker_rss()
            if time.time() < next_claim_refresh:
                continue
            next_claim_refresh = time.time() + claim_every
            with contextlib.suppress(Exception):
                await batch_pipeline.refresh_ffmpeg_slot(r, slot, job_id)
            if batch_id:
                with contextlib.suppress(Exception):
                    await batch_pipeline.refresh_batch_lock(r, batch_id, job_id)
    except asyncio.CancelledError:
        raise
    finally:
        with contextlib.suppress(Exception):
            await r.close()


async def _job_already_delivered(job: dict) -> bool:
    """Whether a previous attempt at this job already delivered its result.

    The broker redelivers a job whose handler raised. If the failure came *after*
    the output reached the user - a failed confirmation message, a Redis hiccup
    while recording the result - then re-running the job would download the source
    again, convert it again and send a second copy. The delivery is recorded on
    the job hash, and this is what turns that duplicate into a no-op.
    """
    job_id = job.get("job_id")
    if not job_id:
        return False
    try:
        r = await get_redis()
    except Exception:
        return False
    try:
        raw = await r.hget(f"ffmpeg:job:{job_id}", "delivered")
    except Exception:
        return False
    value = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
    return value in ("1", 1, "true")


async def _run_queued_job(job: dict, source: str) -> None:
    """Process one job that came off a queue, whichever queue that was.

    Kept separate so the Redis list and the RabbitMQ consumer share exactly the
    same job path: the difference between them is where a job is taken from and
    what happens when processing raises, not what processing does.

    A job for a batch that is already running elsewhere is deferred rather than
    processed, and every job that does run is followed by an explicit memory
    cleanup before the worker picks up anything else.
    """
    global _jobs_in_flight
    # A redelivery of work that is already done is not work. Checked before the
    # conversion slot is taken, so a duplicate attempt costs nothing at all.
    if await _job_already_delivered(job):
        logger.warning(
            "Job %s was already delivered; skipping the duplicate attempt (via %s)",
            job.get("job_id"),
            source,
        )
        # If the first attempt delivered the file but died before its batch
        # progress landed, this redelivery is the last chance to take the
        # batch's bar down. Best-effort and idempotent (claim_batch_progress_slot
        # counts each job once), so running it here is harmless when the counter
        # is already correct.
        with contextlib.suppress(Exception):
            await _report_batch_progress(job)
        return
    slot = await _claim_execution_slot(job)
    if slot is None:
        return
    logger.info("Picked job: %s (via %s)", job.get("job_id"), source)
    # ensure persisted
    with contextlib.suppress(Exception):
        await job_store.save_job(job)
    await emit_event(JOB_STARTED, job=job, source=source)
    _jobs_in_flight += 1
    # Show the file being converted inside the batch's single message. Started
    # before the job so progress is visible from the first second, not only once
    # a file has finished.
    batch_progress_task = None
    if batch_pipeline.job_batch_id(job):
        with contextlib.suppress(Exception):
            batch_progress_task = asyncio.create_task(_batch_live_progress(job))
    # Keep the slot (and the batch lock) this job holds from expiring under it.
    # Started for every job, batch or not: a single 900 MB conversion can run far
    # longer than BATCH_LOCK_TTL_SECONDS.
    claims_task = None
    with contextlib.suppress(Exception):
        claims_task = asyncio.create_task(_keep_claims_alive(job, slot))
    try:
        await handle_job(job)
    finally:
        _jobs_in_flight = max(0, _jobs_in_flight - 1)
        # Stop the live ticker before writing the final figure, so two writers
        # never race on the same message.
        if batch_progress_task is not None:
            batch_progress_task.cancel()
            # ``await`` re-raises CancelledError and that is a BaseException, so
            # suppress(Exception) would let it escape and - on the RabbitMQ path
            # - kill the consumer for good, skipping the progress report and the
            # slot/batch-lock release below. Swallow it here; a real shutdown
            # cancellation re-enters through handle_job's own await point.
            with contextlib.suppress(asyncio.CancelledError):
                await batch_progress_task
        # Stop the heartbeat before the claims are released, so a refresh can
        # never land after finalize_job handed the slot back.
        if claims_task is not None:
            claims_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await claims_task
        # How far into its batch this file got, for the user's progress message.
        with contextlib.suppress(Exception):
            await _report_batch_progress(job)
        # Finish -> clean -> next: release the ffmpeg slot and batch lock, drop caches, gc and
        # return freed heap pages to the OS so the next job does not pile onto
        # the previous one's memory. Never raises.
        try:
            summary = await batch_pipeline.finalize_job(job, source=source, ffmpeg_slot=slot)
            memory = summary.get("memory") or {}
            rss = memory.get("after") or batch_pipeline.rss_bytes()
            if rss:
                WORKER_RSS.set(rss)
            logger.info(
                "Job %s cleaned up: RSS %.1fMB, %d object(s) collected, allocator %s",
                job.get("job_id"),
                (rss or 0) / 1024 / 1024,
                memory.get("collected", 0),
                "trimmed" if memory.get("trimmed") else "not trimmed",
            )
            # Refresh the heartbeat with the number we just measured, so the
            # dashboard's headroom figure matches this job's cleanup.
            await _publish_worker_rss(rss, force=True)
            batch_pipeline.request_restart_if_pressured()
        except Exception:
            logger.debug("ffmpeg worker: post-job cleanup failed for %s", job.get("job_id"))


async def _rabbitmq_consumer_task(stop_event: asyncio.Event | None = None) -> None:
    """Consume jobs from RabbitMQ (messages are acked only after processing)."""
    queue = eventbus.get_queue()
    await queue.consume(lambda job: _run_queued_job(job, "rabbitmq"), stop_event=stop_event)


async def _rabbitmq_consumer_supervisor(stop_event: asyncio.Event | None = None) -> None:
    """Keep the RabbitMQ consumer alive for the life of the worker.

    A consumer that dies used to take its queue out of service until the next
    deploy: jobs kept landing in the broker, nothing consumed them, and every
    file sat "queued" while the Redis loop drained an empty list. Restarts are
    spaced out so a persistently broken broker cannot spin the process.
    """
    backoff = 2.0
    while not (stop_event is not None and stop_event.is_set()):
        try:
            await _rabbitmq_consumer_task(stop_event)
            if stop_event is not None and stop_event.is_set():
                return
            logger.warning("eventbus: RabbitMQ consumer returned early; restarting it")
            backoff = 2.0
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("eventbus: RabbitMQ consumer crashed; restarting in %.0fs", backoff)
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                raise
            backoff = min(backoff * 2.0, 60.0)


async def worker_loop(stop_event: asyncio.Event | None = None, *, allow_restart: bool = False):
    """Main worker loop: pop jobs from the queue(s), process them, deliver results.

    The Redis list is always drained - it holds jobs queued before a broker
    rollout as well as the share the rollout keeps on Redis - and the RabbitMQ
    consumer runs alongside it whenever the broker carries any traffic. A
    consumer that dies takes only its own queue out of service.

    Args:
        stop_event: When set, the worker loop exits gracefully.
    """
    logger.info("FFmpeg worker starting, waiting for jobs...")

    # Start healthcheck server for Railway
    await _start_healthcheck_server()

    # This process just booted, so it cannot own any claim yet - and on Railway
    # the old process is already gone, so nothing else holds one either. Any
    # slot, batch lock or dedup key a previous deploy left behind is a ghost:
    # release them now so the first job after a redeploy never waits out a
    # 15-minute TTL on a claim whose owner died with the old container, and the
    # first file never reads as "still being processed" by a job that is gone.
    try:
        _redis_for_sweep = await get_redis()
        freed = await batch_pipeline.sweep_ghost_claims(_redis_for_sweep)
    except Exception as _exc:  # never block startup on Redis trouble
        logger.debug("Ghost-claim sweep skipped at startup: %s", _exc)
    else:
        if freed:
            logger.warning("Ghost-claim sweep at startup: released %s", freed)

    # Announce this worker's memory so the dashboard has a figure from the start.
    await _publish_worker_rss(force=True)

    # Prove the event log is writable before accepting work. A misconfigured
    # Kafka previously only showed up as a debug line per dropped event, so the
    # first symptom was an empty topic. This logs an ERROR on failure, and raises
    # when EVENTBUS_REQUIRE_BROKERS is set - which turns "silently no events"
    # into a process that refuses to start.
    await eventbus.verify_events_startup()

    # init job store if MONGO_URI available
    try:
        mongo_uri = os.environ.get("MONGO_URI")
        if mongo_uri:
            await job_store.init(mongo_uri)
    except Exception:
        logger.exception("Failed to init job_store (Mongo)")

    # Initialize Redis cache for ffprobe results and job progress
    global _cache
    try:
        if get_cache is not None:
            _cache = await get_cache()
            logger.info("Redis cache initialized for worker")
    except Exception as e:
        logger.debug("Worker cache init failed (non-fatal): %s", e)

    # Start background forward pubsub listener to wake waiting jobs early
    forward_task = None
    try:
        forward_event = asyncio.Event()
        global FORWARD_NOTIFY_EVENT
        FORWARD_NOTIFY_EVENT = forward_event
        forward_task = asyncio.create_task(_forward_pubsub_listener(stop_event, forward_event))
    except Exception:
        forward_task = None

    # Retry the deliveries a Telegram flood window refused. A separate task, so a
    # busy queue (which defers the sweep) never loses them: the queue keeps them.
    deferred_task = None
    try:
        deferred_task = asyncio.create_task(_deferred_delivery_sweeper(stop_event))
    except Exception:
        deferred_task = None
        logger.debug("ffmpeg worker: deferred delivery sweeper not started")

    # Optional RabbitMQ consumer. Enabled by EVENTBUS_QUEUE_BACKEND=rabbitmq with
    # a non-zero rollout; the Redis loop below keeps running either way, because
    # during a rollout both queues hold jobs.
    rabbit_task = None
    try:
        if eventbus.get_settings().consumes_rabbitmq:
            rabbit_task = asyncio.create_task(_rabbitmq_consumer_supervisor(stop_event))
            logger.info("eventbus: RabbitMQ job consumer started alongside the Redis queue")
    except Exception:
        rabbit_task = None
        logger.debug("ffmpeg worker: RabbitMQ consumer not started")

    try:
        while True:
            if stop_event and stop_event.is_set():
                logger.info("Stop event set, exiting worker loop")
                break
            try:
                job = await pop_job(timeout=5)
                if not job:
                    # Keep the dashboard's capacity view fresh while idle, and pick
                    # up an admin restart request that arrived in the meantime.
                    await _publish_worker_rss()
                    if await _maybe_stop_for_restart(allow_restart):
                        break
                    await asyncio.sleep(0.2)
                    continue
                await _run_queued_job(job, "redis")
                # A finished job is the best moment to act on a pending restart:
                # memory was just cleaned up, and nothing is in flight. Covers
                # both the memory ceiling and an admin request.
                if await _maybe_stop_for_restart(allow_restart, force=True):
                    break
            except asyncio.CancelledError:
                logger.info("Worker cancelled, exiting")
                break
            except Exception:
                logger.exception("Exception in worker loop")
                await asyncio.sleep(1)
    finally:
        # ensure forward listener is cancelled
        try:
            if forward_task:
                forward_task.cancel()
                # CancelledError is a BaseException: suppress(Exception) would
                # let it escape and mask the rest of the shutdown.
                with contextlib.suppress(asyncio.CancelledError):
                    await forward_task
        except Exception:
            logger.debug("ffmpeg worker: ensure forward listener is cancelled")
        try:
            if deferred_task:
                deferred_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await deferred_task
        except Exception:
            logger.debug("ffmpeg worker: ensure the deferred delivery sweeper is cancelled")
        # Stop the broker consumer and close both adapters so an in-flight
        # message is redelivered instead of being acked by a dying process.
        try:
            if rabbit_task:
                rabbit_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await rabbit_task
        except Exception:
            logger.debug("ffmpeg worker: RabbitMQ consumer shutdown failed")
        with contextlib.suppress(Exception):
            await eventbus.close_eventbus()


def create_worker_task(stop_event: asyncio.Event | None = None) -> asyncio.Task:
    """Create and return a background asyncio Task that runs the worker loop.

    This is designed to be called from another async application (e.g. the
    FastAPI/uvicorn process) so that the worker processes jobs in the same
    event loop. The task respects the provided stop_event for graceful
    shutdown.

    Usage inside main.py (background mode):
        from workers.ffmpeg_worker import create_worker_task
        worker_task = create_worker_task(shutdown_event)
        app.bot_data["worker_task"] = worker_task

    Returns:
        An asyncio.Task that runs :func:`worker_loop`. The caller should
        ensure the task is cancelled during application shutdown.
    """
    # Initialize job store if MONGO_URI is configured
    try:
        mongo_uri = os.environ.get("MONGO_URI")
        if mongo_uri:
            try:
                # Fire-and-forget init into the running loop
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    asyncio.create_task(job_store.init(mongo_uri))
            except Exception:
                logger.debug("ffmpeg worker: operation failed")
    except Exception:
        logger.debug("ffmpeg worker: Initialize job store if MONGO_URI is configured")

    # Initialize cache if helper available
    try:
        if get_cache is not None:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                asyncio.create_task(_init_cache())
    except Exception:
        logger.debug("ffmpeg worker: Initialize cache if helper available")

    # Wrap worker_loop to log top-level exceptions
    async def _wrapped():
        try:
            await worker_loop(stop_event)
        except asyncio.CancelledError:
            logger.info("Background worker task cancelled")
        except Exception:
            logger.exception("Background worker task exited with unhandled exception")
            raise

    task = asyncio.create_task(_wrapped())
    logger.info("Created background worker task")
    return task


async def _init_cache():
    """Initialize the shared Redis cache instance."""
    global _cache
    try:
        if get_cache is not None:
            _cache = await get_cache()
            logger.info("Worker cache initialized")
    except Exception as e:
        logger.debug("Worker cache init failed (non-fatal): %s", e)


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Share Telegram flood-control windows with the bot process (same bot token,
    # same Redis): a penalty this worker earns in a chat has to stop the bot from
    # writing into that chat too, and the reverse.
    from utils.rate_limiter import telegram_flood_gate

    telegram_flood_gate.attach_redis()

    stop_event = asyncio.Event()

    def _signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, stopping worker...")
        loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # start prometheus metrics server in background thread
    try:
        # bind metrics server to loopback so platform agents
        # do not detect an additional open public port
        start_http_server(METRICS_PORT, addr="127.0.0.1")
        logger.info(f"Prometheus metrics available on 127.0.0.1:{METRICS_PORT}")
    except Exception:
        logger.exception("Failed to start Prometheus metrics server")

    try:
        loop.run_until_complete(worker_loop(stop_event, allow_restart=True))
    finally:
        with contextlib.suppress(Exception):
            # Close job store (Mongo) if used
            loop.run_until_complete(job_store.close())
        try:
            # Close cache if initialized
            if _cache is not None:
                loop.run_until_complete(_cache.close())
        except Exception:
            logger.debug("ffmpeg worker: Close job store (Mongo) if used")
        try:
            # Close shared Redis client used across utils
            loop.run_until_complete(close_redis())
        except Exception:
            logger.debug("ffmpeg worker: failed to close the shared Redis client")
        loop.close()

    # A clean-slate restart is requested when a finished job left memory above
    # the configured ceiling. Exit non-zero so the platform's restart policy
    # (Railway: ON_FAILURE) brings the container back with an empty heap.
    if batch_pipeline.restart_requested():
        logger.warning("Exiting (1) for a memory-clean worker restart")
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
