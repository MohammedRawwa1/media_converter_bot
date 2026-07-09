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
import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Default thresholds
DEFAULT_BOT_API_MAX_MB = int(os.getenv("BOT_API_MAX_MB", "50"))
DEFAULT_BOT_API_MAX_BYTES = DEFAULT_BOT_API_MAX_MB * 1024 * 1024

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
    job_id: Optional[str] = None
    s3_key: Optional[str] = None
    error: Optional[str] = None


class BigFilePipeline:
    """Orchestrates the large file ingestion pipeline."""

    def __init__(self):
        self._storage = None
        self._cache = None
        self._init_lock = None  # lazy-init to avoid Windows deprecation

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
                pass

    async def ingest_large_file(
        self,
        chat_id: int,
        message_id: int,
        file_size: int,
        file_unique_id: Optional[str] = None,
        user_id: Optional[int] = None,
        original_filename: Optional[str] = None,
        ffmpeg_args: Optional[list] = None,
        conversion_type: Optional[str] = None,
    ) -> IngestResult:
        """Download a large file via Pyrogram userbot, upload to S3, enqueue a processing job.

        Args:
            chat_id: Telegram chat ID where the file was sent.
            message_id: Telegram message ID of the file.
            file_size: Size of the file in bytes.
            file_unique_id: Telegram file_unique_id for caching/dedup.
            user_id: User who sent the file.
            original_filename: Original filename if known.
            ffmpeg_args: Custom FFmpeg arguments for processing.
            conversion_type: Type of conversion (e.g., "ffmpeg", "compress", "to_mp3").

        Returns:
            IngestResult with job_id and s3_key on success.
        """
        await self._ensure_initialized()

        job_id = uuid.uuid4().hex
        input_s3_key = f"inputs/{job_id}/source"

        # Step 1: Download via Pyrogram userbot to temp
        try:
            temp_dir = os.path.join(
                os.getenv("STORAGE_PATH", "storage"), "temp"
            )
            os.makedirs(temp_dir, exist_ok=True)

            # Determine extension from filename
            ext = ""
            if original_filename:
                _, ext = os.path.splitext(original_filename)
            if not ext:
                ext = ".bin"

            temp_path = os.path.join(temp_dir, f"{job_id}_src{ext}")
            logger.info(
                "BigFilePipeline: downloading via Pyrogram chat=%s msg=%s size=%dMB -> %s",
                chat_id, message_id, file_size // (1024 * 1024), temp_path,
            )

            download_ok = await self._download_via_pyrogram(
                chat_id, message_id, temp_path
            )
            if not download_ok or not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
                return IngestResult(
                    ok=False,
                    error="Pyrogram download failed",
                )

            actual_size = os.path.getsize(temp_path)
            logger.info(
                "BigFilePipeline: download complete, actual_size=%dMB", actual_size // (1024 * 1024)
            )

            # Cache file info
            if self._cache and file_unique_id:
                try:
                    await self._cache.cache_file_info(
                        file_unique_id,
                        {
                            "job_id": job_id,
                            "size": actual_size,
                            "path": temp_path,
                            "chat_id": chat_id,
                            "message_id": message_id,
                        },
                        ttl=86400,
                    )
                except Exception:
                    pass

        except Exception as e:
            logger.exception("BigFilePipeline: Pyrogram download error: %s", e)
            return IngestResult(ok=False, error=f"Download error: {e}")

        # Step 2: Upload to S3
        s3_key = input_s3_key
        try:
            if self._storage is not None:
                logger.info("BigFilePipeline: uploading to S3 key=%s", s3_key)
                await self._storage.upload_file(temp_path, s3_key)
                logger.info("BigFilePipeline: S3 upload complete")
            else:
                # No S3 — keep the file locally
                s3_key = temp_path
                logger.info("BigFilePipeline: no S3 backend, using local path: %s", temp_path)
        except Exception as e:
            logger.exception("BigFilePipeline: S3 upload failed: %s", e)
            # Fall back to local path
            s3_key = temp_path

        # Step 3: Enqueue processing job
        try:
            from utils.job_queue import enqueue_job

            # Build the job payload
            job = {
                "job_id": job_id,
                "input_key": s3_key if self._storage is not None else None,
                "input_path": s3_key if self._storage is None else None,
                "chat_id": chat_id,
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
            if ffmpeg_args:
                job["ffmpeg_args"] = ffmpeg_args

            await enqueue_job(job)
            logger.info("BigFilePipeline: job %s enqueued (input_key=%s)", job_id, s3_key)

            return IngestResult(ok=True, job_id=job_id, s3_key=s3_key)

        except Exception as e:
            logger.exception("BigFilePipeline: enqueue failed: %s", e)
            return IngestResult(ok=False, error=f"Enqueue error: {e}")

    async def _download_via_pyrogram(
        self, chat_id: int, message_id: int, dest_path: str
    ) -> bool:
        """Download a message using Pyrogram userbot."""
        try:
            from utils.userbot_downloader import download_forward_via_userbot

            ok = await download_forward_via_userbot(
                chat_id=chat_id,
                message_id=message_id,
                dest_path=dest_path,
            )
            return ok
        except Exception as e:
            logger.exception("BigFilePipeline: Pyrogram download failed: %s", e)
            return False
