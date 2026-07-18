"""FFmpeg worker: consumes Redis job queue, runs ffmpeg via ffmpeg_runner,
persists job state to MongoDB (if available), and exposes Prometheus metrics.
"""
import asyncio
import os
import signal
import logging
import time
import subprocess
from typing import Optional

from utils.job_queue import pop_job, publish_update, get_redis, JOB_LIST, close_redis, release_input_lock
from utils.ffmpeg_runner import run_ffmpeg
try:
    from utils.cache import get_cache
except Exception:
    get_cache = None
from utils import job_store
from utils import file_utils
from telegram import Bot
import config
import aiohttp
import shutil
import json
import hashlib
import tempfile
from media_converter import ExtendedMediaConverter
from tasks import (
    create_archive,
    merge_videos,
    merge_audios,
    extract_streams,
    generate_sample,
    trim_media,
)

from prometheus_client import Counter, Histogram, Gauge, start_http_server

logger = logging.getLogger(__name__)

try:
    from utils.storage import get_storage_backend
except Exception:
    get_storage_backend = None
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

# Forward notification event (set by background pubsub listener)
FORWARD_NOTIFY_EVENT: Optional[asyncio.Event] = None
# Redis cache instance shared between worker_loop and handle_job
_cache = None
LAST_FORWARD_NOTIFICATION: Optional[dict] = None


async def _update_upload_progress(job_id: str, progress_channel: str, pct: int, message: str) -> None:
    """Update Redis job hash and publish progress for Telegram upload."""
    try:
        r = await get_redis()
        try:
            await r.hset(f"ffmpeg:job:{job_id}", mapping={
                "progress": str(pct),
                "message": message,
                "status": "uploading" if pct < 100 else "sending",
            })
            await publish_update(progress_channel, {
                "job_id": job_id,
                "progress": pct,
                "message": message,
            })
        finally:
            try:
                await r.close()
            except Exception:
                pass
    except Exception:
        pass


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
                try:
                    loop = asyncio.get_running_loop()
                    asyncio.run_coroutine_threadsafe(
                        _update_upload_progress(job_id, progress_channel, pct, msg),
                        loop,
                    )
                except Exception:
                    pass
        except Exception:
            pass

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
                try:
                    self._progress_callback(self._sent, self._total)
                except Exception:
                    pass
        return chunk

    def __getattr__(self, name):
        return getattr(self._fh, name)


async def _forward_pubsub_listener(stop_event: Optional[asyncio.Event], event: asyncio.Event) -> None:
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
            pass
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
            try:
                event.set()
            except Exception:
                pass
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("Exception in forward pubsub listener")
    finally:
        try:
            await pub.unsubscribe(channel)
        except Exception:
            pass
        try:
            aclose = getattr(client, "aclose", None)
            if aclose is not None:
                await aclose()
            else:
                await client.close()
        except Exception:
            pass


async def handle_job(job: dict):
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
        pass

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
            except Exception:
                pass
            try:
                aclose = getattr(r, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await r.close()
            except Exception:
                pass
    except Exception:
        pass

    # If job references a remote storage key (S3/MinIO), prefer to download it
    # when the local `input_path` is missing or the file is not present on disk.
    input_key = job.get("input_key") or job.get("s3_key") or job.get("remote_key")
    if input_key and (not input_path or not os.path.exists(input_path)):
        # prepare temp path
        temp_dir = os.path.join(getattr(config, "TEMP_PATH", "storage/temp"))
        os.makedirs(temp_dir, exist_ok=True)
        _, ext = os.path.splitext(input_key)
        if not ext:
            ext = os.path.splitext(job.get("original_filename") or "")[1] or ""
        temp_input_path = os.path.join(temp_dir, f"{job_id}_src{ext}")

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
                    pass

                try:
                    await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "requeued_missing_input", "attempts": attempts, "backoff": backoff})
                except Exception:
                    pass

                if r2 is not None:
                    try:
                        aclose = getattr(r2, "aclose", None)
                        if aclose is not None:
                            await aclose()
                        else:
                            await r2.close()
                    except Exception:
                        pass
                return
            else:
                try:
                    await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "download_failed", "error": "remote_key_missing_permanent"})
                except Exception:
                    pass
                try:
                    await job_store.update_job(job_id, {"status": "error", "error": "remote_key_missing_permanent"})
                except Exception:
                    pass
                if r2 is not None:
                    try:
                        aclose = getattr(r2, "aclose", None)
                        if aclose is not None:
                            await aclose()
                        else:
                            await r2.close()
                    except Exception:
                        pass
                return

        # retry/backoff for transient storage/download issues
        download_retries = int(os.environ.get("DOWNLOAD_RETRIES", "3"))
        backoff_base = float(os.environ.get("DOWNLOAD_BACKOFF_BASE", "1"))
        download_success = False
        last_exc = None
        for attempt in range(1, download_retries + 1):
            try:
                await backend.download_file(input_key, temp_input_path)
                # confirm file exists and has data
                if os.path.exists(temp_input_path) and (os.path.getsize(temp_input_path) > 0):
                    download_success = True
                    break
            except Exception as e:
                last_exc = e
            # backoff before next attempt
            if attempt < download_retries:
                await asyncio.sleep(backoff_base * (2 ** (attempt - 1)))

        if download_success:
            input_path = temp_input_path
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
                    try:
                        await r2.hset(f"ffmpeg:job:{job_id}", mapping={"input": str(job.get("input_path") or ""), "input_from_remote": "1"})
                    except Exception:
                        pass
            except Exception:
                pass
            finally:
                if r2 is not None:
                    try:
                        aclose = getattr(r2, "aclose", None)
                        if aclose is not None:
                            await aclose()
                        else:
                            await r2.close()
                    except Exception:
                        pass
        else:
            logger.exception("Failed to download input from storage for job %s: %s", job_id, last_exc)
            try:
                await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "download_failed", "error": str(last_exc)})
            except Exception:
                pass
            try:
                await job_store.update_job(job_id, {"status": "error", "error": "remote_download_failed"})
            except Exception:
                pass
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
                async with session.get(source_url, timeout=60) as resp:
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
            try:
                await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "download_failed", "error": str(e)})
            except Exception:
                pass
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
            lock_ttl = int(os.environ.get("JOB_LOCK_SECONDS", str(6 * 3600)))
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
                    try:
                        await redis_lock_client.expire(lock_key, lock_ttl)
                    except Exception:
                        pass
        except Exception:
            pass

    if not lock_acquired:
        logger.info("Input already locked for job %s, requeueing", job_id)
        try:
            await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "locked", "note": "input_locked"})
        except Exception:
            pass
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
                pass
        except Exception:
            pass
        return

    # mark processing start
    try:
        await job_store.update_job(job_id, {"status": "processing", "started_at": time.time()})
        # Cache job start for fast status queries
        try:
            if _cache:
                await _cache.cache_job_metadata(job_id, {
                    "status": "processing",
                    "started_at": time.time(),
                }, ttl=3600)
        except Exception:
            pass
    except Exception:
        pass
    try:
        await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "started"})
    except Exception:
        pass

    # prefer original filename for output if provided
    try:
        orig = job.get("original_filename") or job.get("original_name")
        if orig:
            sanitized = await file_utils.sanitize_filename(orig)
            base, ext = os.path.splitext(sanitized)
            out_ext = ".mp4" if (job.get("ffmpeg_args") or job.get("type") in ("ffmpeg", None, "generate_sample")) else (ext or ".mp4")
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
    converter = ExtendedMediaConverter() if ExtendedMediaConverter else None
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
                try:
                    await publish_update(channel, {"job_id": job_id, "memory_rss": rss})
                except Exception:
                    pass
            except Exception:
                pass
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
                                    try:
                                        await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "rate_limited", "error": "user rate limit reached"})
                                    except Exception:
                                        pass
                                    try:
                                        await job_store.update_job(job_id, {"status": "error", "error": "rate_limited"})
                                    except Exception:
                                        pass
                                    return
                        except Exception:
                            # on limiter failures, allow processing to continue
                            pass
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
                            try:
                                await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "waiting_for_input", "wait_seconds": wait_seconds})
                            except Exception:
                                pass

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
                                            pass
                                        # Wait up to 1s, but wake early if a forward notification arrives
                                        try:
                                            if FORWARD_NOTIFY_EVENT is not None:
                                                try:
                                                    await asyncio.wait_for(FORWARD_NOTIFY_EVENT.wait(), timeout=1)
                                                    try:
                                                        FORWARD_NOTIFY_EVENT.clear()
                                                    except Exception:
                                                        pass
                                                except asyncio.TimeoutError:
                                                    pass
                                            else:
                                                await asyncio.sleep(1)
                                        except Exception:
                                            # on any error, fall back to sleeping briefly
                                            try:
                                                await asyncio.sleep(0.5)
                                            except Exception:
                                                pass
                                finally:
                                    try:
                                        aclose = getattr(rr, "aclose", None)
                                        if aclose is not None:
                                            await aclose()
                                        else:
                                            await rr.close()
                                    except Exception:
                                        pass

                            # final check after waiting
                            if not has_local_file and not job.get("input_key") and not job.get("source_url"):
                                logger.error("No input available for job %s after waiting; marking as error", job_id)
                                try:
                                    await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "error", "error": "no_input_provided"})
                                except Exception:
                                    pass
                                try:
                                    await job_store.update_job(job_id, {"status": "error", "error": "no_input_provided"})
                                except Exception:
                                    pass
                                return

                        coro = run_ffmpeg(input_path, output_path, job_id, ffmpeg_args=ffmpeg_args, redis_url=redis_url, progress_channel=progress_channel)
                        # Start optional memory sampler
                        try:
                            if os.environ.get("ENABLE_MEMORY_SAMPLER", "").lower() in ("1", "true", "yes"):
                                memory_sampler_task = asyncio.create_task(_memory_sampler(progress_channel, float(os.environ.get("MEMORY_SAMPLER_INTERVAL", "5.0"))))
                        except Exception:
                            memory_sampler_task = None
                        try:
                            success, info = await asyncio.wait_for(coro, timeout=max_runtime)
                        except asyncio.TimeoutError:
                            success, info = False, "timeout"

                    elif job_type in ("create_archive", "archive"):
                        await publish_update(progress_channel, {"job_id": job_id, "progress": 5, "message": "creating archive"})
                        files = job.get("files") or []
                        ok, msg = await create_archive(files, output_path)
                        success = ok
                        info = output_path if ok else msg
                        await publish_update(progress_channel, {"job_id": job_id, "progress": 100 if ok else 0, "message": "done" if ok else "error", "output": output_path if ok else None})

                    elif job_type == "merge_videos":
                        await publish_update(progress_channel, {"job_id": job_id, "progress": 5, "message": "merging videos"})
                        files = job.get("files") or []
                        ok, msg = await merge_videos(files, output_path)
                        success = ok
                        info = output_path if ok else msg
                        await publish_update(progress_channel, {"job_id": job_id, "progress": 100 if ok else 0, "message": "done" if ok else "error", "output": output_path if ok else None})

                    elif job_type == "merge_audios":
                        await publish_update(progress_channel, {"job_id": job_id, "progress": 5, "message": "merging audios"})
                        files = job.get("files") or []
                        ok, msg = await merge_audios(files, output_path)
                        success = ok
                        info = output_path if ok else msg
                        await publish_update(progress_channel, {"job_id": job_id, "progress": 100 if ok else 0, "message": "done" if ok else "error", "output": output_path if ok else None})

                    elif job_type == "extract_streams":
                        await publish_update(progress_channel, {"job_id": job_id, "progress": 5, "message": "extracting streams"})
                        out_dir = job.get("output_dir") or os.path.join(getattr(config, "OUTPUT_PATH", "storage/output"), f"{job_id}_streams")
                        os.makedirs(out_dir, exist_ok=True)
                        ok, extracted = await extract_streams(input_path, out_dir)
                        if ok and extracted:
                            archive_path = job.get("archive_path") or f"{out_dir}.zip"
                            ok2, msg2 = await create_archive(list(extracted.values()), archive_path)
                            success = ok2
                            info = archive_path if ok2 else msg2
                            await publish_update(progress_channel, {"job_id": job_id, "progress": 100 if ok2 else 0, "message": "done" if ok2 else "error", "output": archive_path if ok2 else None})
                        else:
                            success = False
                            info = "no_streams" if ok else "extract_failed"
                            await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "error", "error": info})

                    elif job_type == "generate_sample":
                        await publish_update(progress_channel, {"job_id": job_id, "progress": 5, "message": "generating sample"})
                        dur = int(job.get("duration", 30))
                        ok, msg = await generate_sample(input_path, output_path, dur)
                        success = ok
                        info = output_path if ok else msg
                        await publish_update(progress_channel, {"job_id": job_id, "progress": 100 if ok else 0, "message": "done" if ok else "error", "output": output_path if ok else None})

                    elif job_type == "trim":
                        await publish_update(progress_channel, {"job_id": job_id, "progress": 5, "message": "trimming"})
                        start_time = job.get("start_time")
                        end_time = job.get("end_time")
                        ok, msg = await trim_media(input_path, output_path, start_time, end_time)
                        success = ok
                        info = output_path if ok else msg
                        await publish_update(progress_channel, {"job_id": job_id, "progress": 100 if ok else 0, "message": "done" if ok else "error", "output": output_path if ok else None})

                    elif job_type == "rename":
                        new_name = job.get("new_name")
                        try:
                            new_path = job.get("output_path") or os.path.join(os.path.dirname(input_path), new_name)
                            os.rename(input_path, new_path)
                            success = True
                            info = new_path
                            await publish_update(progress_channel, {"job_id": job_id, "progress": 100, "message": "renamed", "output": new_path})
                        except Exception as e:
                            success = False
                            info = str(e)
                            await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "error", "error": info})

                    else:
                        ffmpeg_args = job.get("ffmpeg_args") if isinstance(job.get("ffmpeg_args"), list) else None
                        redis_url = job.get("redis_url") or os.environ.get("REDIS_URL")
                        try:
                            success, info = await asyncio.wait_for(run_ffmpeg(input_path, output_path, job_id, ffmpeg_args=ffmpeg_args, redis_url=redis_url, progress_channel=progress_channel), timeout=max_runtime)
                        except asyncio.TimeoutError:
                            success, info = False, "timeout"

                # end with JOB_DURATION

                JOBS_TOTAL.inc()

                if success:
                    JOBS_SUCCEEDED.inc()
                    out = info if isinstance(info, str) else output_path
                    await publish_update(progress_channel, {"job_id": job_id, "progress": 100, "message": "done", "output": out})
                    try:
                        await job_store.update_job(job_id, {"status": "done", "finished_at": time.time(), "output": out})
                        # Cache job result for fast status queries
                        try:
                            if _cache:
                                await _cache.cache_job_metadata(job_id, {
                                    "status": "done",
                                    "output": out,
                                    "finished_at": time.time(),
                                }, ttl=3600)
                        except Exception:
                            pass
                    except Exception:
                        pass

                    # Attempt to upload processed output to configured storage backend
                    upload_success = False
                    dest = None
                    get_url = None
                    try:
                        # Only attempt when a storage backend helper is available
                        if get_storage_backend is not None:
                            try:
                                backend = await get_storage_backend()
                            except Exception:
                                backend = None
                        else:
                            backend = None

                        if backend is not None and out and os.path.exists(out):
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
                                    send_link = os.environ.get("ENABLE_LINK_SEND", "").lower() in ("1", "true", "yes")
                                    r = await get_redis()
                                    mapping = {"output_key": dest}
                                    if get_url:
                                        mapping["output_get_url"] = get_url
                                    mapping["output"] = get_url if get_url and send_link else dest
                                    try:
                                        mapping["out_bytes"] = str(os.path.getsize(out))
                                    except Exception:
                                        pass
                                    await r.hset(f"ffmpeg:job:{job_id}", mapping=mapping)
                                    upload_success = True
                                    await r.close()
                                except Exception:
                                    pass
                            except Exception:
                                logger.exception("Failed to upload output for job %s", job_id)
                    except Exception:
                        pass

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
                                            if upload_success:
                                                should_delete = True
                                        else:
                                            # no remote backend configured; safe to delete local input after processing
                                            should_delete = True
                                    except Exception:
                                        # conservative default: don't delete if uncertain
                                        should_delete = False

                                    if should_delete:
                                        os.remove(input_path)
                    except Exception as e:
                        logger.warning(f"Failed to cleanup input file: {e}")

                    try:
                        chat_id = job.get("chat_id")
                        caption = job.get("caption")
                        sent = False
                        enable_userbot = os.environ.get("ENABLE_USERBOT", "").lower() in ("1", "true", "yes")
                        bot_token = getattr(config, "BOT_TOKEN", None)

                        # Determine file size and Bot API threshold (MB)
                        file_size = 0
                        try:
                            if out and os.path.exists(out):
                                file_size = os.path.getsize(out)
                        except Exception:
                            file_size = 0

                        bot_api_max_mb = int(os.environ.get("BOT_API_MAX_SIZE_MB", "50"))
                        bot_api_max_bytes = bot_api_max_mb * 1024 * 1024

                        send_link = os.environ.get("ENABLE_LINK_SEND", "").lower() in ("1", "true", "yes")
                        # If we uploaded the output and generated a presigned GET URL, only send it if
                        # explicit link delivery is enabled. Otherwise keep delivery inside Telegram.
                        if send_link and upload_success and get_url and chat_id and bot_token:
                            try:
                                job_type = job.get("type") if isinstance(job, dict) else None
                                if file_size > bot_api_max_bytes or (job_type and job_type != "generate_sample"):
                                    async with Bot(token=bot_token) as bot:
                                        text = f"Your video is ready: {get_url}"
                                        await bot.send_message(chat_id=chat_id, text=text)
                                    logger.info("Sent presigned URL to chat %s for job %s", chat_id, job_id)
                                    sent = True
                            except Exception:
                                logger.exception("Failed to send presigned URL for job %s", job_id)
                                sent = False

                        # If output is large and userbot is enabled, prefer userbot for delivery
                        if chat_id and file_size > bot_api_max_bytes and enable_userbot:
                            try:
                                from utils.userbot_uploader import send_file_via_userbot

                                _up_cb = _make_upload_progress_callback(job_id, progress_channel)
                                ok = await send_file_via_userbot(chat_id, out, caption=caption, progress_callback=_up_cb)
                                if ok:
                                    logger.info("Sent output via Telethon userbot (preferred) for job %s", job_id)
                                    sent = True
                                else:
                                    logger.error("Preferred userbot send failed for job %s", job_id)
                                    sent = False
                            except Exception:
                                logger.exception("Preferred userbot send raised exception for job %s", job_id)
                                sent = False
                        else:
                            # Try Bot API first if configured
                            if chat_id and bot_token:
                                try:
                                    # Determine media kind. Prefer probing the output file for a video stream
                                    kind = "doc"
                                    # Always-bounded sentinel for video metadata extracted during probe
                                    _vid_duration = None
                                    _vid_width = None
                                    _vid_height = None
                                    try:
                                        if out and os.path.exists(out):
                                            # prefer ffprobe if available to detect real video streams
                                            ffprobe_bin = getattr(config, "FFPROBE_PATH", None) or getattr(config, "FFMPEG_PATH", "ffmpeg").replace("ffmpeg", "ffprobe")
                                            ffmpeg_bin = getattr(config, "FFMPEG_PATH", "ffmpeg")

                                            # Try cache first for ffprobe results
                                            probe_info = None
                                            try:
                                                if _cache and out and os.path.exists(out):
                                                    import hashlib as _hl
                                                    _fhash = _hl.sha256(f"{out}:{os.path.getsize(out)}".encode()).hexdigest()[:16]
                                                    probe_info = await _cache.get(f"cache:probe:{_fhash}")
                                                    if probe_info:
                                                        logger.debug("Worker: ffprobe cache HIT for %s", out)
                                            except Exception:
                                                pass

                                            if probe_info is None:  # cache miss
                                                def _probe():
                                                    try:
                                                        return subprocess.run([ffprobe_bin, "-v", "quiet", "-print_format", "json", "-show_streams", out], capture_output=True)
                                                    except Exception:
                                                        return None

                                                p = await asyncio.to_thread(_probe)
                                            if p and getattr(p, "returncode", 1) == 0:
                                                try:
                                                    probe_info = json.loads(p.stdout.decode() or "{}")
                                                    # Cache ffprobe result
                                                    try:
                                                        if _cache and out and os.path.exists(out) and probe_info:
                                                            _fhash = _hl.sha256(f"{out}:{os.path.getsize(out)}".encode()).hexdigest()[:16]
                                                            await _cache.set(f"cache:probe:{_fhash}", probe_info, ttl=86400)
                                                            logger.debug("Worker: cached ffprobe result for %s", out)
                                                    except Exception:
                                                        pass
                                                    streams = probe_info.get("streams", [])
                                                    if any(s.get("codec_type") == "video" for s in streams):
                                                        kind = "video"
                                                    else:
                                                        # not a video stream; fall back to extension-based
                                                        if str(out).lower().endswith(".zip"):
                                                            kind = "zip"
                                                        elif str(out).lower().endswith((".mp4", ".mov", ".mkv")):
                                                            kind = "video"
                                                        else:
                                                            kind = "doc"
                                                    # Extract video metadata for richer send_video payload
                                                    for s in streams:
                                                        if s.get("codec_type") == "video":
                                                            if "width" in s:
                                                                _vid_width = s["width"]
                                                            if "height" in s:
                                                                _vid_height = s["height"]
                                                            break
                                                    fmt = probe_info.get("format", {})
                                                    if fmt.get("duration"):
                                                        try:
                                                            _vid_duration = int(float(fmt["duration"]))
                                                        except (ValueError, TypeError):
                                                            pass
                                                except Exception:
                                                    # probe parse failed -> extension fallback
                                                    if str(out).lower().endswith(".zip"):
                                                        kind = "zip"
                                                    elif str(out).lower().endswith((".mp4", ".mov", ".mkv")):
                                                        kind = "video"
                                                    else:
                                                        kind = "doc"
                                            else:
                                                # probe failed -> extension fallback
                                                if str(out).lower().endswith(".zip"):
                                                    kind = "zip"
                                                elif str(out).lower().endswith((".mp4", ".mov", ".mkv")):
                                                    kind = "video"
                                                else:
                                                    kind = "doc"
                                        else:
                                            kind = "doc"
                                    except Exception:
                                        kind = "zip" if out and str(out).lower().endswith(".zip") else ("video" if out and str(out).lower().endswith((".mp4", ".mov", ".mkv")) else "doc")
                                    try:
                                        # Use async Bot API methods directly and close the client when done
                                        async with Bot(token=bot_token) as bot:
                                            if kind == "zip":
                                                # Attempt to attach a thumbnail when available
                                                thumb_path = None
                                                _temp_thumb = None
                                                try:
                                                    # Prefer explicit job field
                                                    cand = job.get("thumbnail")
                                                    if cand:
                                                        if os.path.exists(cand):
                                                            thumb_path = cand
                                                        else:
                                                            try:
                                                                backend = await get_storage_backend() if get_storage_backend is not None else None
                                                            except Exception:
                                                                backend = None
                                                            if backend:
                                                                temp_dir = os.path.join(getattr(config, "TEMP_PATH", "storage/temp"))
                                                                os.makedirs(temp_dir, exist_ok=True)
                                                                _temp_thumb = os.path.join(temp_dir, f"{job_id}_thumb{os.path.splitext(cand)[1] or '.jpg'}")
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
                                                            sval = stored.get("thumbnail") or stored.get("thumb")
                                                            if sval:
                                                                cand = sval
                                                                if os.path.exists(cand):
                                                                    thumb_path = cand
                                                                else:
                                                                    try:
                                                                        backend = await get_storage_backend() if get_storage_backend is not None else None
                                                                    except Exception:
                                                                        backend = None
                                                                    if backend:
                                                                        temp_dir = os.path.join(getattr(config, "TEMP_PATH", "storage/temp"))
                                                                        os.makedirs(temp_dir, exist_ok=True)
                                                                        _temp_thumb = os.path.join(temp_dir, f"{job_id}_thumb{os.path.splitext(cand)[1] or '.jpg'}")
                                                                        try:
                                                                            ok = await backend.download_file(cand, _temp_thumb)
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
                                                            pass

                                                except Exception:
                                                    thumb_path = None

                                                _bot_up_cb = _make_upload_progress_callback(job_id, progress_channel)
                                                try:
                                                    with open(out, "rb") as fh:
                                                        fh = _ProgressFileWrapper(fh, file_size, _bot_up_cb) if file_size else fh
                                                        if thumb_path:
                                                            try:
                                                                with open(thumb_path, "rb") as tf:
                                                                    await bot.send_document(chat_id=chat_id, document=fh, caption=caption, thumb=tf)
                                                            except Exception:
                                                                await bot.send_document(chat_id=chat_id, document=fh, caption=caption)
                                                        else:
                                                            await bot.send_document(chat_id=chat_id, document=fh, caption=caption)
                                                finally:
                                                    try:
                                                        if _temp_thumb and os.path.exists(_temp_thumb):
                                                            os.remove(_temp_thumb)
                                                    except Exception:
                                                        pass
                                            elif kind == "video":
                                                # Try to attach thumbnail (thumb) when available
                                                thumb_path = None
                                                _temp_thumb = None
                                                try:
                                                    cand = job.get("thumbnail")
                                                    if cand:
                                                        if os.path.exists(cand):
                                                            thumb_path = cand
                                                        else:
                                                            try:
                                                                backend = await get_storage_backend() if get_storage_backend is not None else None
                                                            except Exception:
                                                                backend = None
                                                            if backend:
                                                                temp_dir = os.path.join(getattr(config, "TEMP_PATH", "storage/temp"))
                                                                os.makedirs(temp_dir, exist_ok=True)
                                                                _temp_thumb = os.path.join(temp_dir, f"{job_id}_thumb{os.path.splitext(cand)[1] or '.jpg'}")
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
                                                            sval = stored.get("thumbnail") or stored.get("thumb")
                                                            if sval:
                                                                cand = sval
                                                                if os.path.exists(cand):
                                                                    thumb_path = cand
                                                                else:
                                                                    try:
                                                                        backend = await get_storage_backend() if get_storage_backend is not None else None
                                                                    except Exception:
                                                                        backend = None
                                                                    if backend:
                                                                        temp_dir = os.path.join(getattr(config, "TEMP_PATH", "storage/temp"))
                                                                        os.makedirs(temp_dir, exist_ok=True)
                                                                        _temp_thumb = os.path.join(temp_dir, f"{job_id}_thumb{os.path.splitext(cand)[1] or '.jpg'}")
                                                                        try:
                                                                            ok = await backend.download_file(cand, _temp_thumb)
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
                                                            pass

                                                    # Auto-generate thumbnail from video if none provided
                                                    if not thumb_path:
                                                        try:
                                                            _auto_thumb_dir = tempfile.mkdtemp(prefix="auto_thumb_")
                                                            _auto_thumb_path = os.path.join(_auto_thumb_dir, "thumb.jpg")
                                                            _thumb_proc = await asyncio.create_subprocess_exec(
                                                                ffmpeg_bin, "-y", "-ss", "00:00:01", "-i", out,
                                                                "-vframes", "1", "-q:v", "2", _auto_thumb_path,
                                                                stdout=asyncio.subprocess.PIPE,
                                                                stderr=asyncio.subprocess.PIPE,
                                                            )
                                                            await asyncio.wait_for(_thumb_proc.communicate(), timeout=30)
                                                            if _thumb_proc.returncode == 0 and os.path.exists(_auto_thumb_path) and os.path.getsize(_auto_thumb_path) > 0:
                                                                thumb_path = _auto_thumb_path
                                                                _temp_thumb = _auto_thumb_path
                                                            else:
                                                                shutil.rmtree(_auto_thumb_dir, ignore_errors=True)
                                                        except Exception:
                                                            pass
                                                except Exception:
                                                    thumb_path = None

                                                _bot_up_cb = _make_upload_progress_callback(job_id, progress_channel)
                                                try:
                                                    with open(out, "rb") as fh:
                                                        fh = _ProgressFileWrapper(fh, file_size, _bot_up_cb) if file_size else fh
                                                        # Build send_video kwargs with available metadata
                                                        _send_kwargs = {
                                                            "chat_id": chat_id,
                                                            "video": fh,
                                                            "caption": caption,
                                                            "supports_streaming": True,
                                                        }
                                                        if _vid_duration is not None:
                                                            _send_kwargs["duration"] = _vid_duration
                                                        if _vid_width is not None:
                                                            _send_kwargs["width"] = _vid_width
                                                        if _vid_height is not None:
                                                            _send_kwargs["height"] = _vid_height
                                                        if thumb_path:
                                                            try:
                                                                with open(thumb_path, "rb") as tf:
                                                                    _send_kwargs["thumb"] = tf
                                                                    await bot.send_video(**_send_kwargs)
                                                            except Exception:
                                                                await bot.send_video(**_send_kwargs)
                                                        else:
                                                            await bot.send_video(**_send_kwargs)
                                                finally:
                                                    try:
                                                        if _temp_thumb and os.path.exists(_temp_thumb):
                                                            try:
                                                                _d = os.path.dirname(_temp_thumb)
                                                                shutil.rmtree(_d, ignore_errors=True)
                                                            except Exception:
                                                                os.remove(_temp_thumb)
                                                    except Exception:
                                                        pass
                                            else:
                                                # non-video non-zip fallback
                                                thumb_path = None
                                                _temp_thumb = None
                                                try:
                                                    cand = job.get("thumbnail")
                                                    if cand and os.path.exists(cand):
                                                        thumb_path = cand
                                                    elif cand:
                                                        try:
                                                            backend = await get_storage_backend() if get_storage_backend is not None else None
                                                        except Exception:
                                                            backend = None
                                                        if backend:
                                                            temp_dir = os.path.join(getattr(config, "TEMP_PATH", "storage/temp"))
                                                            os.makedirs(temp_dir, exist_ok=True)
                                                            _temp_thumb = os.path.join(temp_dir, f"{job_id}_thumb{os.path.splitext(cand)[1] or '.jpg'}")
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
                                                        fh = _ProgressFileWrapper(fh, file_size, _bot_up_cb) if file_size else fh
                                                        if thumb_path:
                                                            try:
                                                                with open(thumb_path, "rb") as tf:
                                                                    await bot.send_document(chat_id=chat_id, document=fh, caption=caption, thumb=tf)
                                                            except Exception:
                                                                await bot.send_document(chat_id=chat_id, document=fh, caption=caption)
                                                        else:
                                                            await bot.send_document(chat_id=chat_id, document=fh, caption=caption)
                                                finally:
                                                    try:
                                                        if _temp_thumb and os.path.exists(_temp_thumb):
                                                            os.remove(_temp_thumb)
                                                    except Exception:
                                                        pass
                                        sent = True
                                    except Exception as e:
                                        logger.warning("Bot API send failed for job %s: %s", job_id, e)
                                        sent = False
                                except Exception as e:
                                    logger.warning("Bot init failed for job %s: %s", job_id, e)
                                    sent = False

                        # Fallback: if not sent and Telethon userbot is enabled, attempt userbot
                        if not sent and chat_id and enable_userbot:
                            try:
                                from utils.userbot_uploader import send_file_via_userbot

                                _up_cb = _make_upload_progress_callback(job_id, progress_channel)
                                ok = await send_file_via_userbot(chat_id, out, caption=caption, progress_callback=_up_cb)
                                if ok:
                                    logger.info("Sent output via Telethon userbot fallback for job %s", job_id)
                                    sent = True
                                else:
                                    logger.error("Userbot fallback failed for job %s", job_id)
                            except Exception:
                                logger.exception("Userbot fallback raised exception for job %s", job_id)

                        if not sent and chat_id:
                            logger.warning("Could not deliver output for job %s — neither Bot API nor userbot succeeded", job_id)

                        # Set final job status on Redis hash so _watch_job_progress (and web UI) can see it.
                        try:
                            _final_status = "done" if sent else "error"
                            _final_msg = "delivered to Telegram" if sent else "delivery failed"
                            _r = await get_redis()
                            try:
                                await _r.hset(f"ffmpeg:job:{job_id}", mapping={
                                    "status": _final_status,
                                    "progress": "100",
                                    "message": _final_msg,
                                })
                                await publish_update(progress_channel, {
                                    "job_id": job_id,
                                    "progress": 100,
                                    "message": _final_msg,
                                    "status": _final_status,
                                })
                            finally:
                                try:
                                    await _r.close()
                                except Exception:
                                    pass
                        except Exception:
                            pass

                    except Exception:
                        logger.exception("Failed to send result via Telegram")

                    try:
                        if job.get("cleanup_output", False) and out and os.path.exists(out):
                            os.remove(out)
                    except Exception:
                        pass

                    try:
                        if temp_input and os.path.exists(temp_input):
                            os.remove(temp_input)
                    except Exception:
                        pass

                        # cancel memory sampler if running
                        try:
                            if memory_sampler_task:
                                memory_sampler_task.cancel()
                                try:
                                    await memory_sampler_task
                                except Exception:
                                    pass
                        except Exception:
                            pass

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

                    attempted_redownload = False
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
                                    cmd = [ffmpeg_bin, "-y", "-hide_banner", "-loglevel", "error", "-i", input_path, "-c", "copy", remux_path]
                                    try:
                                        proc = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, timeout=600)
                                    except Exception as e:
                                        proc = None

                                    ok = False
                                    if proc and getattr(proc, "returncode", 1) == 0 and os.path.exists(remux_path) and os.path.getsize(remux_path) > 0:
                                        ok = True

                                    # Persist attempt count
                                    try:
                                        if r:
                                            await r.hset(f"ffmpeg:job:{job_id}", mapping={"remux_attempts": str(remux_attempts + 1)})
                                    except Exception:
                                        pass

                                    if ok:
                                        try:
                                            await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "remuxed", "note": "remux succeeded; retrying"})
                                        except Exception:
                                            pass
                                        logger.info("Remux succeeded for job %s, retrying ffmpeg against %s", job_id, remux_path)
                                        # Switch to remuxed input and retry
                                        input_path = remux_path
                                        job["input_path"] = remux_path
                                        # close redis client if opened
                                        try:
                                            if r:
                                                await r.close()
                                        except Exception:
                                            pass
                                        continue
                                    else:
                                        try:
                                            await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "remux_failed", "note": "remux attempted and failed"})
                                        except Exception:
                                            pass
                                except Exception:
                                    logger.exception("Remux attempt failed for job %s", job_id)

                            try:
                                if r:
                                    await r.close()
                            except Exception:
                                pass

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
                                        try:
                                            os.remove(input_path)
                                        except Exception:
                                            pass
                                except Exception:
                                    pass

                                ok = False
                                tried = False
                                # Try forward_hash if present
                                try:
                                    fh = job.get("forward_hash") or job.get("fh")
                                    if fh:
                                        try:
                                            from utils.forward_store import load_forward_metadata

                                            meta = load_forward_metadata(fh)
                                            if meta:
                                                try:
                                                    from utils.userbot_downloader import download_forward_via_userbot

                                                    tried = True
                                                    ok = await download_forward_via_userbot(
                                                        meta.get("chat_id"), meta.get("message_id") or meta.get("msg_id"), input_path, msg_date=meta.get("registered_at") or meta.get("created_at"), file_unique_id=meta.get("file_unique_id")
                                                    )
                                                except Exception:
                                                    ok = False
                                        except Exception:
                                            pass
                                except Exception:
                                    pass

                                # Try direct chat/message metadata if available
                                if not tried and job.get("chat_id") and (job.get("message_id") or job.get("msg_id")):
                                    try:
                                        from utils.userbot_downloader import download_forward_via_userbot

                                        tried = True
                                        ok = await download_forward_via_userbot(job.get("chat_id"), job.get("message_id") or job.get("msg_id"), input_path)
                                    except Exception:
                                        ok = False

                                # Try HTTP source_url if available
                                if not tried and job.get("source_url"):
                                    try:
                                        tried = True
                                        async with aiohttp.ClientSession() as session:
                                            async with session.get(job.get("source_url"), timeout=aiohttp.ClientTimeout(total=60)) as resp:
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
                                        await r.hset(f"ffmpeg:job:{job_id}", mapping={"redownload_attempts": str(redownload_attempts + 1)})
                                except Exception:
                                    pass
                                try:
                                    if r:
                                        await r.close()
                                except Exception:
                                    pass

                                attempted_redownload = True
                                if ok:
                                    try:
                                        await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "redownloaded", "note": "re-download succeeded; retrying"})
                                    except Exception:
                                        pass
                                    logger.info("Redownload succeeded for job %s, retrying ffmpeg", job_id)
                                    # Retry immediately
                                    continue
                                else:
                                    try:
                                        await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "redownload_failed", "note": "re-download attempted and failed"})
                                    except Exception:
                                        pass

                    except Exception:
                        logger.exception("Error during re-download attempt for job %s", job_id)

                    # If we attempted re-download and it failed, fall through to normal failure handling
                    JOBS_FAILED.inc()
                    await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "error", "error": info})
                    try:
                        await job_store.update_job(job_id, {"status": "error", "error": info, "attempt": attempt})
                        try:
                            if _cache:
                                await _cache.cache_job_metadata(job_id, {
                                    "status": "error",
                                    "error": info,
                                    "attempt": attempt,
                                }, ttl=1800)
                        except Exception:
                            pass
                    except Exception:
                        pass

                    if attempt <= retries:
                        backoff = min(30, 2 ** attempt)
                        logger.info(f"Retrying job {job_id} in {backoff}s (attempt {attempt})")
                        await asyncio.sleep(backoff)
                        continue
                    else:
                        return

            except asyncio.CancelledError:
                logger.info("Job cancelled via worker shutdown")
                try:
                    await job_store.update_job(job_id, {"status": "cancelled", "message": "shutdown"})
                except Exception:
                    pass
                raise
            except Exception as e:
                JOBS_FAILED.inc()
                logger.exception("Unhandled exception while processing job")
                try:
                    await job_store.update_job(job_id, {"status": "error", "error": str(e), "attempt": attempt})
                except Exception:
                    pass
                if attempt <= retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    return
            finally:
                ACTIVE_JOBS.dec()
    finally:
        # release the input lock if we acquired one
        try:
            if lock_key and lock_acquired and job_id:
                await release_input_lock(lock_key, job_id, redis_client=redis_lock_client)
        except Exception:
            logger.warning("Failed to release input lock %s for job %s", lock_key, job_id)
        finally:
            try:
                if redis_lock_client:
                    await redis_lock_client.close()
            except Exception:
                pass


async def worker_loop(stop_event: Optional[asyncio.Event] = None):
    """Main worker loop: pop jobs from Redis, process them, deliver results.

    Args:
        stop_event: When set, the worker loop exits gracefully.
    """
    logger.info("FFmpeg worker starting, waiting for jobs...")
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

    try:
        while True:
            if stop_event and stop_event.is_set():
                logger.info("Stop event set, exiting worker loop")
                break
            try:
                job = await pop_job(timeout=5)
                if not job:
                    await asyncio.sleep(0.2)
                    continue
                logger.info(f"Picked job: {job.get('job_id')}")
                # ensure persisted
                try:
                    await job_store.save_job(job)
                except Exception:
                    pass
                await handle_job(job)
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
                try:
                    await forward_task
                except Exception:
                    pass
        except Exception:
            pass

def create_worker_task(stop_event: Optional[asyncio.Event] = None) -> asyncio.Task:
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
                pass
    except Exception:
        pass

    # Initialize cache if helper available
    global _cache
    try:
        if get_cache is not None:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                asyncio.create_task(_init_cache())
    except Exception:
        pass

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

    stop_event = asyncio.Event()

    def _signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, stopping worker...")
        loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # start prometheus metrics server in background thread
    try:
        # bind metrics server to loopback so platform agents (e.g. Render)
        # do not detect an additional open public port
        start_http_server(METRICS_PORT, addr='127.0.0.1')
        logger.info(f"Prometheus metrics available on 127.0.0.1:{METRICS_PORT}")
    except Exception:
        logger.exception("Failed to start Prometheus metrics server")

    try:
        loop.run_until_complete(worker_loop(stop_event))
    finally:
        try:
            # Close job store (Mongo) if used
            loop.run_until_complete(job_store.close())
        except Exception:
            pass
        try:
            # Close cache if initialized
            if _cache is not None:
                loop.run_until_complete(_cache.close())
        except Exception:
            pass
        try:
            # Close shared Redis client used across utils
            loop.run_until_complete(close_redis())
        except Exception:
            pass
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
