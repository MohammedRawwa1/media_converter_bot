"""Big files pipeline: Pyrogram download → S3 upload → Redis queue → Worker → S3 → Pyrogram delivery.

Handles files that exceed the Telegram Bot API 50MB limit by routing them
through a userbot-based download, S3 storage, and worker processing pipeline.

Usage from handlers.py:
    from utils.bigfile_pipeline import BigFilePipeline

    pipeline = BigFilePipeline()
    result = await pipeline.ingest_large_file(
        chat_id=chat_id,
        message_id=message_id,
        file_size=file_size,
        file_unique_id=file_unique_id,
        user_id=user_id,
    )
    if result.ok:
        # File is queued — inform user and return
    else:
        # Fallback to normal Bot API download
"""

import asyncio
import contextlib
import logging
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass

import config

logger = logging.getLogger(__name__)

# Default thresholds
DEFAULT_BOT_API_MAX_MB = int(os.getenv("BOT_API_MAX_MB", "20"))
DEFAULT_BOT_API_MAX_BYTES = DEFAULT_BOT_API_MAX_MB * 1024 * 1024

# Files up to this size (200MB) get streamed through memory instead of temp disk
# Imports — all guarded for optional dependencies
try:
    from utils.storage import get_storage_backend
except Exception:
    get_storage_backend = None

try:
    from utils.cache import get_cache
except Exception:
    get_cache = None


@dataclass
class IngestResult:
    """Result of a big file ingestion attempt."""

    ok: bool
    job_id: str | None = None
    s3_key: str | None = None
    error: str | None = None


class BigFilePipeline:
    """Orchestrates the large file ingestion pipeline."""

    def __init__(self):
        self._storage = None
        self._cache = None
        self._init_lock = asyncio.Lock()

    async def _ensure_initialized(self):
        """Lazy-init storage and cache backends."""
        if self._storage is not None:
            return
        async with self._init_lock:
            if self._storage is not None:
                return
            try:
                if get_storage_backend is not None:
                    self._storage = await get_storage_backend()
            except Exception as e:
                logger.warning("BigFilePipeline: storage init failed: %s", e)
            try:
                if get_cache is not None:
                    self._cache = await get_cache()
            except Exception:
                logger.debug("BigFilePipeline: cache init failed", exc_info=True)

    async def ingest_large_file(
        self,
        chat_id: int,
        message_id: int,
        file_size: int,
        file_unique_id: str | None = None,
        user_id: int | None = None,
        original_filename: str | None = None,
        ffmpeg_args: list | None = None,
        conversion_type: str | None = None,
        output_ext: str | None = None,
        caption: str | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> IngestResult:
        """Download a large file via Pyrogram userbot, upload to S3, enqueue a processing job.

        This path uses a single disk-based download (no in-memory streaming) to avoid
        double-Pyrogram downloads. For files up to 200MB the previous in-memory path
        often failed silently, causing a fallback disk download that looked like two
        separate Pyrogram calls.

        Args:
            chat_id: Telegram chat ID where the file was sent.
            message_id: Telegram message ID of the file.
            file_size: Size of the file in bytes.
            file_unique_id: Telegram file_unique_id for caching/dedup.
            user_id: User who sent the file.
            original_filename: Original filename if known.
            ffmpeg_args: Custom FFmpeg arguments for processing.
            conversion_type: Type of conversion (e.g., "ffmpeg", "compress", "to_mp3").
            output_ext: Output file extension (e.g., ".mp4", ".mkv") when different from original.
            caption: Caption text for the output message.
            progress_callback: Optional callable(current_bytes, total_bytes) for download progress.

        Returns:
            IngestResult with job_id and s3_key on success.
        """
        await self._ensure_initialized()

        job_id = uuid.uuid4().hex
        input_s3_key = f"inputs/{job_id}/source"

        # Determine extension from filename
        ext = ""
        if original_filename:
            _, ext = os.path.splitext(original_filename)
        if not ext:
            ext = ".bin"

        actual_size = 0
        s3_key = input_s3_key

        # ── Check Redis byte cache before downloading (fast path) ──
        _cache_hit = False
        if self._cache and file_unique_id:
            try:
                cached_data = await self._cache.get_cached_file_bytes(file_unique_id)
                if cached_data is not None and len(cached_data) > 0:
                    logger.info(
                        "BigFilePipeline: cache HIT for file_unique_id=%s (%dMB), uploading to S3",
                        file_unique_id,
                        len(cached_data) // (1024 * 1024),
                    )
                    actual_size = len(cached_data)
                    if self._storage is not None:
                        await self._storage.upload_bytes(cached_data, s3_key)
                    _cache_hit = True
                    if self._cache:
                        with contextlib.suppress(Exception):
                            await self._cache.cache_file_info(
                                file_unique_id,
                                {
                                    "job_id": job_id,
                                    "size": actual_size,
                                    "path": s3_key,
                                    "chat_id": chat_id,
                                    "message_id": message_id,
                                },
                                ttl=86400,
                            )
                    logger.info(
                        "BigFilePipeline: cache pipeline succeeded for %s/%s (%d bytes)",
                        chat_id,
                        message_id,
                        actual_size,
                    )
            except Exception as e:
                logger.debug("BigFilePipeline: cache check failed: %s", e)

        # ── Disk-based download (single Pyrogram call, always used) ──
        # The previous in-memory path (download_bytes_via_userbot) often failed for files
        # 20-200MB, causing a fallback disk download that looked like two Pyrogram calls.
        # Now we always use the single disk-based path with progress callback support.
        if not _cache_hit:
            try:
                temp_dir = os.path.abspath(os.path.join(os.getenv("STORAGE_PATH", "storage"), "temp"))
                os.makedirs(temp_dir, exist_ok=True)

                temp_path = os.path.join(temp_dir, f"{job_id}_src{ext}")
                logger.info(
                    "BigFilePipeline: downloading via Pyrogram chat=%s msg=%s size=%dMB -> %s",
                    chat_id,
                    message_id,
                    file_size // (1024 * 1024),
                    temp_path,
                )

                download_ok = await self._download_via_pyrogram(
                    chat_id,
                    message_id,
                    temp_path,
                    progress_callback=progress_callback,
                )
                if not download_ok or not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
                    return IngestResult(
                        ok=False,
                        error="Pyrogram download failed",
                    )

                actual_size = os.path.getsize(temp_path)
                logger.info("BigFilePipeline: disk download complete, actual_size=%dMB", actual_size // (1024 * 1024))

                # Cache file info in Redis
                if self._cache and file_unique_id:
                    with contextlib.suppress(Exception):
                        await self._cache.cache_file_info(
                            file_unique_id,
                            {
                                "job_id": job_id,
                                "size": actual_size,
                                "path": input_s3_key if self._storage is not None else temp_path,
                                "chat_id": chat_id,
                                "message_id": message_id,
                            },
                            ttl=86400,
                        )

            except Exception as e:
                logger.exception("BigFilePipeline: Pyrogram download error: %s", e)
                return IngestResult(ok=False, error=f"Download error: {e}")

            # Upload to S3
            try:
                if self._storage is not None:
                    logger.info("BigFilePipeline: uploading to S3 key=%s", s3_key)
                    await self._storage.upload_file(temp_path, s3_key)
                    logger.info("BigFilePipeline: S3 upload complete")
                    # Immediately clean up temp file
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                            logger.info("BigFilePipeline: cleaned up temp file %s", temp_path)
                    except Exception as cleanup_err:
                        logger.warning("BigFilePipeline: failed to clean up temp file %s: %s", temp_path, cleanup_err)
                else:
                    # No S3 — keep the file locally
                    s3_key = temp_path
                    logger.info("BigFilePipeline: no S3 backend, using local path: %s", temp_path)
            except Exception as e:
                logger.exception("BigFilePipeline: S3 upload failed: %s", e)
                s3_key = temp_path

        # Step 3: Enqueue processing job
        try:
            from utils.job_queue import enqueue_job

            # Determine output extension: prefer caller-supplied output_ext,
            # otherwise fall back to original filename extension or .mp4
            _out_ext = output_ext or ".mp4"
            if not output_ext and original_filename:
                _, _orig_ext = os.path.splitext(original_filename)
                if _orig_ext:
                    _out_ext = _orig_ext

            # Build the job payload
            job = {
                "job_id": job_id,
                "input_key": s3_key if self._storage is not None else None,
                "input_path": s3_key if self._storage is None else None,
                # chat_id for delivery = user_id (the person who should receive
                # the processed result). The original chat_id was used for download
                # (may be a relay group) but the result must go to the user's DM.
                "chat_id": user_id or chat_id,
                "user_id": user_id,
                "message_id": message_id,
                "original_filename": original_filename or f"file_{job_id}{ext}",
                "file_unique_id": file_unique_id,
                "file_size": actual_size,
                "progress_channel": f"ffmpeg:progress:{job_id}",
                "cleanup_input": True,
                "type": conversion_type or "ffmpeg",
                "created_at": time.time(),
            }
            # Include conversion-specific metadata when provided by the caller
            if ffmpeg_args:
                job["ffmpeg_args"] = ffmpeg_args
            if caption:
                job["caption"] = caption
            # Include output_ext so the worker knows what format extension to use
            job["output_ext"] = _out_ext

            # For extract_streams, include output_dir and archive_path so the
            # worker's extract_streams branch can place extracted files and create the zip.
            if conversion_type == "extract_streams":
                _out_base = getattr(config, "OUTPUT_PATH", "storage/output")
                _streams_dir = os.path.join(_out_base, f"{job_id}_streams")
                job["output_dir"] = _streams_dir
                job["archive_path"] = f"{_streams_dir}.zip"

            await enqueue_job(job)
            logger.info("BigFilePipeline: job %s enqueued (input_key=%s)", job_id, s3_key)

            return IngestResult(ok=True, job_id=job_id, s3_key=s3_key)

        except Exception as e:
            logger.exception("BigFilePipeline: enqueue failed: %s", e)
            return IngestResult(ok=False, error=f"Enqueue error: {e}")

    async def _download_via_pyrogram(
        self,
        chat_id: int,
        message_id: int,
        dest_path: str,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> bool:
        """Download a message using Pyrogram userbot.

        Args:
            chat_id: Telegram chat ID where the file is.
            message_id: Telegram message ID of the file.
            dest_path: Local path to save the downloaded file.
            progress_callback: Optional callable(current_bytes, total_bytes) for progress.

        Returns True on success, False on failure.
        """
        try:
            from utils.userbot_downloader import download_forward_via_userbot

            ok = await download_forward_via_userbot(
                chat_id=chat_id,
                message_id=message_id,
                dest_path=dest_path,
                progress_callback=progress_callback,
            )
            return ok
        except Exception as e:
            logger.exception("BigFilePipeline: Pyrogram download failed: %s", e)
            return False
