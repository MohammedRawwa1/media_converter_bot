"""FFmpeg worker: consumes Redis job queue, runs ffmpeg via ffmpeg_runner,
persists job state to MongoDB (if available), and exposes Prometheus metrics.
"""
import asyncio
import os
import signal
import logging
import time
from typing import Optional

from utils.job_queue import pop_job, publish_update, get_redis, JOB_LIST
from utils.ffmpeg_runner import run_ffmpeg
from utils import job_store
from utils import file_utils
from telegram import Bot
import config
import aiohttp
import shutil
import json
import hashlib
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

# Prometheus metrics
METRICS_PORT = int(os.environ.get("PROMETHEUS_METRICS_PORT", "8000"))
JOBS_TOTAL = Counter("media_jobs_total", "Total ffmpeg jobs processed")
JOBS_FAILED = Counter("media_jobs_failed", "Total ffmpeg jobs failed")
JOBS_SUCCEEDED = Counter("media_jobs_succeeded", "Total ffmpeg jobs succeeded")
JOB_DURATION = Histogram("media_job_duration_seconds", "Duration of ffmpeg jobs")
ACTIVE_JOBS = Gauge("media_jobs_active", "Number of active ffmpeg jobs")


async def handle_job(job: dict):
    job_id = job.get("job_id")
    input_path = job.get("input_path")
    output_path = job.get("output_path")
    progress_channel = job.get("progress_channel") or f"ffmpeg:progress:{job_id}"
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
            lock_name = (input_path or job.get("source_url") or job_id) or job_id
            lock_hash = hashlib.sha256(str(lock_name).encode()).hexdigest()
            lock_key = f"ffmpeg:lock:{lock_hash}"
            lock_ttl = int(os.environ.get("JOB_LOCK_SECONDS", str(6 * 3600)))
            lock_acquired = await redis_lock_client.set(lock_key, job_id, nx=True, ex=lock_ttl)
        except Exception:
            lock_acquired = True

    if not lock_acquired:
        logger.info("Input already locked for job %s, requeueing", job_id)
        try:
            await publish_update(progress_channel, {"job_id": job_id, "progress": 0, "message": "locked", "note": "input_locked"})
        except Exception:
            pass
        try:
            await redis_lock_client.lpush(JOB_LIST, json.dumps(job))
        except Exception:
            logger.warning("Failed to requeue locked job %s", job_id)
        try:
            await redis_lock_client.close()
        except Exception:
            pass
        return

    # mark processing start
    try:
        await job_store.update_job(job_id, {"status": "processing", "started_at": time.time()})
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
                        coro = run_ffmpeg(input_path, output_path, job_id, ffmpeg_args=ffmpeg_args, redis_url=redis_url, progress_channel=progress_channel)
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
                        out_dir = job.get("output_dir") or f"storage/output/{job_id}_streams"
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
                    except Exception:
                        pass

                    try:
                        if job.get("cleanup_input", True) and input_path and os.path.exists(input_path):
                            os.remove(input_path)
                    except Exception as e:
                        logger.warning(f"Failed to cleanup input file: {e}")

                    try:
                        chat_id = job.get("chat_id")
                        caption = job.get("caption")
                        sent = False
                        enable_userbot = os.environ.get("ENABLE_USERBOT", "").lower() in ("1", "true", "yes")
                        bot_token = getattr(config, "BOT_TOKEN", None)

                        # Try Bot API first if configured
                        if chat_id and bot_token:
                            try:
                                bot = Bot(token=bot_token)

                                # Run blocking Bot API calls off the event loop to avoid blocking
                                def _send_sync(kind: str, file_path: str, caption_text: str):
                                    if kind == "zip":
                                        with open(file_path, "rb") as fh:
                                            bot.send_document(chat_id=chat_id, document=fh, caption=caption_text)
                                    elif kind == "video":
                                        with open(file_path, "rb") as fh:
                                            bot.send_video(chat_id=chat_id, video=fh, caption=caption_text, supports_streaming=True)
                                    else:
                                        with open(file_path, "rb") as fh:
                                            bot.send_document(chat_id=chat_id, document=fh, caption=caption_text)

                                kind = "zip" if out and str(out).lower().endswith(".zip") else ("video" if out and str(out).lower().endswith((".mp4", ".mov", ".mkv")) else "doc")
                                try:
                                    await asyncio.to_thread(_send_sync, kind, out, caption)
                                    sent = True
                                except Exception as e:
                                    logger.warning("Bot API send failed for job %s: %s", job_id, e)
                                    sent = False
                            except Exception as e:
                                logger.warning("Bot init failed for job %s: %s", job_id, e)
                                sent = False

                        # Fallback: use Telethon userbot if enabled and Bot API failed/not present
                        if not sent and chat_id and enable_userbot:
                            try:
                                from utils.userbot_uploader import send_file_via_userbot

                                ok = await send_file_via_userbot(chat_id, out, caption=caption)
                                if ok:
                                    logger.info("Sent output via Telethon userbot fallback for job %s", job_id)
                                    sent = True
                                else:
                                    logger.error("Userbot fallback failed for job %s", job_id)
                            except Exception:
                                logger.exception("Userbot fallback raised exception for job %s", job_id)

                        if not sent and chat_id:
                            logger.warning("Could not deliver output for job %s — neither Bot API nor userbot succeeded", job_id)
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

                    return

                else:
                    # Detect likely truncated/corrupt input errors and attempt a re-download
                    lower_err = (str(info) or "").lower()
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
            if lock_key and redis_lock_client:
                try:
                    cur = await redis_lock_client.get(lock_key)
                    if cur:
                        try:
                            if isinstance(cur, bytes):
                                cur = cur.decode()
                        except Exception:
                            pass
                        if cur == job_id:
                            await redis_lock_client.delete(lock_key)
                except Exception:
                    pass
        finally:
            try:
                if redis_lock_client:
                    await redis_lock_client.close()
            except Exception:
                pass


async def worker_loop(stop_event: Optional[asyncio.Event] = None):
    logger.info("FFmpeg worker starting, waiting for jobs...")
    # init job store if MONGO_URI available
    try:
        mongo_uri = os.environ.get("MONGO_URI")
        if mongo_uri:
            await job_store.init(mongo_uri)
    except Exception:
        logger.exception("Failed to init job_store (Mongo)")

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
            loop.run_until_complete(job_store.close())
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
