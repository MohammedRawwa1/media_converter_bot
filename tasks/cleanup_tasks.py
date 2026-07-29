# tasks/cleanup_tasks.py
import asyncio
import logging
import os
import time

try:
    import config
except Exception:
    config = None

logger = logging.getLogger(__name__)


class CleanupManager:
    """Manages cleanup of temporary files and old data."""

    def __init__(self):
        self.cleanup_interval = 3600  # 1 hour
        self.max_file_age = 24 * 3600  # 24 hours
        self.max_temp_age = 1 * 3600  # 1 hour
        # S3 / R2 TTLs (overridable via env vars, values in seconds)
        self.s3_input_ttl = int(os.getenv("S3_INPUT_TTL", str(24 * 3600)))
        self.s3_upload_ttl = int(os.getenv("S3_UPLOADS_TTL", str(24 * 3600)))
        self.s3_forward_ttl = int(os.getenv("S3_FORWARDS_TTL", str(48 * 3600)))
        # Redis job hash cleanup interval (30 minutes)
        self.redis_cleanup_interval = int(os.getenv("REDIS_CLEANUP_INTERVAL", str(30 * 60)))
        # Stale Redis job hash max age (24 hours)
        self.redis_job_max_age = int(os.getenv("REDIS_JOB_MAX_AGE", str(24 * 3600)))
        self.is_running = False
        self._redis_cleanup_task = None

    async def start(self):
        """Start periodic cleanup tasks (hourly file cleanup + 30-min Redis cleanup)."""
        self.is_running = True
        # Start the Redis cleanup loop as a separate background task
        self._redis_cleanup_task = asyncio.create_task(self._redis_cleanup_loop())
        logger.info(
            "Cleanup manager started (file cleanup every %ds, Redis cleanup every %ds)",
            self.cleanup_interval,
            self.redis_cleanup_interval,
        )

        while self.is_running:
            try:
                await self.cleanup_all()
                await asyncio.sleep(self.cleanup_interval)
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error

    def stop(self):
        """Stop cleanup tasks."""
        self.is_running = False
        if self._redis_cleanup_task and not self._redis_cleanup_task.done():
            self._redis_cleanup_task.cancel()
        logger.info("Cleanup manager stopped")

    async def cleanup_all(self) -> dict:
        """Run all cleanup operations (local + remote)."""
        results = {
            "input_files": await self.cleanup_input_files(),
            "output_files": await self.cleanup_output_files(),
            "temp_files": await self.cleanup_temp_files(),
            "thumbnails": await self.cleanup_thumbnails(),
            "redis_jobs": await self.cleanup_stale_redis_jobs(),
            "redis_dedup_keys": await self.cleanup_stale_dedup_keys(),
            "empty_dirs": await self.cleanup_empty_directories(),
            # ── S3 / R2 remote cleanup ──
            "s3_inputs": await self.cleanup_s3_inputs(),
            "s3_uploads": await self.cleanup_s3_uploads(),
            "s3_forwards": await self.cleanup_s3_forwards(),
        }

        total_cleaned = sum(results.values())
        if total_cleaned > 0:
            logger.info(f"Cleanup completed: {results}")

        return results

    async def cleanup_input_files(self) -> int:
        """Clean up old input files."""
        return await self._cleanup_directory(getattr(config, "INPUT_PATH", "storage/input"), self.max_file_age)

    async def cleanup_output_files(self) -> int:
        """Clean up old output files."""
        return await self._cleanup_directory(getattr(config, "OUTPUT_PATH", "storage/output"), self.max_file_age)

    async def cleanup_temp_files(self) -> int:
        """Clean up temporary files."""
        return await self._cleanup_directory(getattr(config, "TEMP_PATH", "storage/temp"), self.max_temp_age)

    async def cleanup_thumbnails(self) -> int:
        """Clean up old thumbnails."""
        return await self._cleanup_directory(getattr(config, "THUMBNAIL_PATH", "storage/thumbnails"), self.max_file_age)

    async def _cleanup_directory(self, directory: str, max_age: int) -> int:
        """Clean up files in a directory older than max_age."""
        try:
            if not os.path.exists(directory):
                return 0

            current_time = time.time()
            files_removed = 0

            for item in os.listdir(directory):
                item_path = os.path.join(directory, item)

                if os.path.isfile(item_path):
                    file_age = current_time - os.path.getmtime(item_path)
                    if file_age > max_age:
                        try:
                            os.remove(item_path)
                            files_removed += 1
                            logger.debug(f"Removed old file: {item_path}")
                        except Exception as e:
                            logger.error(f"Error removing file {item_path}: {e}")

                elif os.path.isdir(item_path):
                    # Recursively cleanup subdirectories
                    sub_removed = await self._cleanup_directory(item_path, max_age)
                    files_removed += sub_removed

            return files_removed

        except Exception as e:
            logger.error(f"Error cleaning directory {directory}: {e}")
            return 0

    async def cleanup_empty_directories(self) -> int:
        """Remove empty directories."""
        directories = [
            getattr(config, "INPUT_PATH", "storage/input"),
            getattr(config, "OUTPUT_PATH", "storage/output"),
            getattr(config, "TEMP_PATH", "storage/temp"),
            getattr(config, "THUMBNAIL_PATH", "storage/thumbnails"),
        ]

        removed_count = 0

        for directory in directories:
            try:
                if os.path.exists(directory):
                    for root, dirs, _files in os.walk(directory, topdown=False):
                        for dir_name in dirs:
                            dir_path = os.path.join(root, dir_name)
                            try:
                                if not os.listdir(dir_path):
                                    os.rmdir(dir_path)
                                    removed_count += 1
                                    logger.debug(f"Removed empty directory: {dir_path}")
                            except Exception as e:
                                logger.error(f"Error checking directory {dir_path}: {e}")
            except Exception as e:
                logger.error(f"Error cleaning empty directories in {directory}: {e}")

        return removed_count

    # ─────────────────────────────────────────────────────────────────────
    # S3 / R2 remote cleanup helpers
    # ─────────────────────────────────────────────────────────────────────

    async def cleanup_s3_inputs(self) -> int:
        """Clean old input files from S3 under the ``inputs/`` prefix."""
        return await self._cleanup_s3_prefix("inputs/", self.s3_input_ttl)

    async def cleanup_s3_uploads(self) -> int:
        """Clean old uploaded files from S3 under the ``uploads/`` prefix."""
        return await self._cleanup_s3_prefix("uploads/", self.s3_upload_ttl)

    async def cleanup_s3_forwards(self) -> int:
        """Clean old forward metadata files from S3 under the ``forwards/`` prefix."""
        return await self._cleanup_s3_prefix("forwards/", self.s3_forward_ttl)

    async def _cleanup_s3_prefix(self, prefix: str, max_age_seconds: int) -> int:
        """List objects under an S3 *prefix* and delete those older than *max_age_seconds*."""
        try:
            from utils.storage import get_storage_backend

            backend = await get_storage_backend()

            # Only attempt S3 / R2 cleanup when the active backend is actually remote
            _bn = config.get_storage_backend_name() if config else (os.getenv("STORAGE_BACKEND") or "local").lower()
            if _bn not in ("s3", "r2"):
                return 0

            objects = await backend.list_keys(prefix)
            if not objects:
                return 0

            now = time.time()
            to_delete = [obj["key"] for obj in objects if (now - obj["last_modified"]) > max_age_seconds]

            if not to_delete:
                return 0

            deleted = await backend.delete_keys(to_delete)
            logger.info(
                "S3 cleanup: prefix=%s deleted=%d/%d candidates=%d (TTL=%ds)",
                prefix,
                deleted,
                len(to_delete),
                len(objects),
                max_age_seconds,
            )
            return deleted
        except Exception as e:
            logger.error("S3 cleanup failed for prefix=%s: %s", prefix, e)
            return 0

    # ─────────────────────────────────────────────────────────────────────
    # Redis job hash cleanup
    # ─────────────────────────────────────────────────────────────────────

    async def cleanup_stale_dedup_keys(self) -> int:
        """Delete stale pipeline dedup keys (``ffmpeg:pipeline_dedup:*``).

        A dedup key is stale if its value is "pending" (failed ingest) or
        references a job that is no longer active (done/error/cancelled/missing).
        Keys with an active job are preserved.

        Returns the number of keys deleted.
        """
        try:
            from utils.job_queue import get_redis

            r = await get_redis()
        except Exception as e:
            logger.debug("redis_dedup_cleanup: cannot connect to Redis: %s", e)
            return 0

        deleted = 0
        try:
            cursor = 0
            while True:
                try:
                    cursor, keys = await r.scan(cursor, match="ffmpeg:pipeline_dedup:*", count=100)
                except Exception:
                    break

                for key in keys:
                    try:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        val = await r.get(key)
                        if not val:
                            # Already expired or empty — delete
                            await r.delete(key)
                            deleted += 1
                            continue

                        val_str = val.decode() if isinstance(val, bytes) else str(val)

                        # "pending" placeholder from a failed ingest — stale
                        if val_str == "pending":
                            await r.delete(key)
                            deleted += 1
                            logger.debug("redis_dedup_cleanup: deleted pending key %s", key_str)
                            continue

                        # Check if the referenced job is still active
                        job_hash = await r.hgetall(f"ffmpeg:job:{val_str}")
                        if not job_hash:
                            # Job hash doesn't exist — stale
                            await r.delete(key)
                            deleted += 1
                            logger.debug("redis_dedup_cleanup: deleted orphaned key %s (job %s gone)", key_str, val_str)
                            continue

                        status = job_hash.get(b"status") or job_hash.get("status")
                        if status:
                            status = status.decode() if isinstance(status, bytes) else str(status)
                        else:
                            status = ""

                        if status in ("done", "error", "cancelled", ""):
                            await r.delete(key)
                            deleted += 1
                            logger.debug("redis_dedup_cleanup: deleted key %s (job %s status=%s)", key_str, val_str, status)

                    except Exception as e:
                        logger.debug("redis_dedup_cleanup: error processing key %s: %s", key, e)

                if cursor == 0:
                    break

            if deleted > 0:
                logger.info("redis_dedup_cleanup: deleted %d stale dedup keys", deleted)
        except Exception as e:
            logger.error("redis_dedup_cleanup: unexpected error: %s", e)
        finally:
            try:
                aclose = getattr(r, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await r.close()
            except Exception:
                pass

        return deleted

    async def cleanup_stale_redis_jobs(self) -> int:
        """Delete Redis job hashes for completed/errored/cancelled jobs older than max age.

        Scans all ``ffmpeg:job:*`` keys, checks their ``status`` and
        ``finished_at`` (or ``started_at``) timestamp, and removes hashes
        that are older than ``self.redis_job_max_age`` (default 24 hours).

        Returns the number of hashes deleted.
        """
        try:
            from utils.job_queue import get_redis

            r = await get_redis()
        except Exception as e:
            logger.debug("redis_job_cleanup: cannot connect to Redis: %s", e)
            return 0

        deleted = 0
        now = time.time()
        try:
            # Scan for all job hash keys
            cursor = 0
            while True:
                try:
                    cursor, keys = await r.scan(cursor, match="ffmpeg:job:*", count=100)
                except Exception:
                    break

                for key in keys:
                    try:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        # Only delete job hashes (not dedup keys, progress keys, etc.)
                        job_id = key_str.replace("ffmpeg:job:", "")
                        if not job_id or len(job_id) < 8:
                            continue

                        data = await r.hgetall(key)
                        if not data:
                            # Empty hash — safe to delete
                            await r.delete(key)
                            deleted += 1
                            continue

                        status = data.get(b"status") or data.get("status")
                        if status:
                            status = status.decode() if isinstance(status, bytes) else str(status)
                        else:
                            status = ""

                        # Only clean up terminal states
                        if status not in ("done", "error", "cancelled"):
                            continue

                        # Check timestamp: prefer finished_at, fallback to started_at
                        ts_raw = data.get(b"finished_at") or data.get("finished_at")
                        if not ts_raw:
                            ts_raw = data.get(b"started_at") or data.get("started_at")
                        if not ts_raw:
                            # No timestamp — if status is terminal, delete it
                            await r.delete(key)
                            deleted += 1
                            continue

                        try:
                            ts = float(ts_raw.decode() if isinstance(ts_raw, bytes) else ts_raw)
                        except (ValueError, TypeError):
                            await r.delete(key)
                            deleted += 1
                            continue

                        age = now - ts
                        if age > self.redis_job_max_age:
                            await r.delete(key)
                            deleted += 1
                            logger.debug(
                                "redis_job_cleanup: deleted %s (status=%s, age=%.0fs)",
                                key_str,
                                status,
                                age,
                            )
                    except Exception as e:
                        logger.debug("redis_job_cleanup: error processing key %s: %s", key, e)

                if cursor == 0:
                    break

            if deleted > 0:
                logger.info(
                    "redis_job_cleanup: deleted %d stale job hashes (max_age=%ds)",
                    deleted,
                    self.redis_job_max_age,
                )
        except Exception as e:
            logger.error("redis_job_cleanup: unexpected error: %s", e)
        finally:
            try:
                aclose = getattr(r, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await r.close()
            except Exception:
                pass

        return deleted

    async def _redis_cleanup_loop(self):
        """Periodic loop that runs Redis job hash cleanup every 30 minutes."""
        # Wait a bit before first run to avoid startup thundering herd
        await asyncio.sleep(60)
        while self.is_running:
            try:
                deleted = await self.cleanup_stale_redis_jobs()
                if deleted > 0:
                    logger.info("Redis cleanup loop: removed %d stale job hashes", deleted)
            except Exception as e:
                logger.error("Redis cleanup loop error: %s", e)
            await asyncio.sleep(self.redis_cleanup_interval)

    async def startup_temp_cleanup(self, max_age: int = 1800) -> int:
        """Clean stale temp files on startup (default: files older than 30 minutes).

        This prevents stale files from previous runs (e.g. crashed workers) from
        being picked up by the pipeline or consuming disk space.
        """
        temp_dir = getattr(config, "TEMP_PATH", "storage/temp") if config else "storage/temp"
        if not os.path.exists(temp_dir):
            logger.info("startup_temp_cleanup: temp dir %s does not exist; skipping", temp_dir)
            return 0

        current_time = time.time()
        files_removed = 0
        bytes_freed = 0

        for item in os.listdir(temp_dir):
            item_path = os.path.join(temp_dir, item)
            if os.path.isfile(item_path):
                try:
                    file_age = current_time - os.path.getmtime(item_path)
                    if file_age > max_age:
                        file_size = os.path.getsize(item_path)
                        os.remove(item_path)
                        files_removed += 1
                        bytes_freed += file_size
                        logger.info(
                            "startup_temp_cleanup: removed stale file %s (age=%.0fs, size=%dMB)",
                            item,
                            file_age,
                            file_size // (1024 * 1024),
                        )
                except Exception as e:
                    logger.warning("startup_temp_cleanup: failed to remove %s: %s", item_path, e)

        if files_removed > 0:
            logger.info(
                "startup_temp_cleanup: removed %d stale files (%dMB freed) from %s",
                files_removed,
                bytes_freed // (1024 * 1024),
                temp_dir,
            )
        else:
            logger.info("startup_temp_cleanup: no stale files found in %s", temp_dir)

        return files_removed

    async def force_cleanup(self, directory: str = None) -> int:
        """Force cleanup of specific directory or all."""
        if directory and os.path.exists(directory):
            return await self._cleanup_directory(directory, 0)  # Clean all files
        else:
            results = await self.cleanup_all()
            return sum(results.values())

    async def get_storage_stats(self) -> dict:
        """Get storage usage statistics."""
        stats = {}
        directories = [
            getattr(config, "INPUT_PATH", "storage/input"),
            getattr(config, "OUTPUT_PATH", "storage/output"),
            getattr(config, "TEMP_PATH", "storage/temp"),
            getattr(config, "THUMBNAIL_PATH", "storage/thumbnails"),
        ]

        for directory in directories:
            size_bytes = 0
            file_count = 0

            try:
                if os.path.exists(directory):
                    for root, _dirs, files in os.walk(directory):
                        for file in files:
                            file_path = os.path.join(root, file)
                            if os.path.exists(file_path):
                                size_bytes += os.path.getsize(file_path)
                                file_count += 1
            except Exception as e:
                logger.error(f"Error getting stats for {directory}: {e}")

            dir_name = directory.split("/")[-1]
            stats[dir_name] = {"size_mb": size_bytes / (1024 * 1024), "file_count": file_count}

        stats["total"] = {
            "size_mb": sum(d["size_mb"] for d in stats.values()),
            "file_count": sum(d["file_count"] for d in stats.values()),
        }

        return stats


# Global cleanup manager instance
cleanup_manager = CleanupManager()


async def start_cleanup_task():
    """Start the cleanup manager as a background task."""
    await cleanup_manager.start()


def stop_cleanup_task():
    """Stop the cleanup manager."""
    cleanup_manager.stop()
