# tasks/cleanup_tasks.py
import asyncio
import logging
import os
import time

try:
    import config
except Exception:
    config = None

# Shared single source of truth for the library prefix. Falls back to the literal
# so a partially importable tree cannot silently stop protecting the cache.
try:
    from utils.media_cache import LIBRARY_KEY_PREFIX
except Exception:
    LIBRARY_KEY_PREFIX = "inputs/library/"

# How long a finished batch's leftover state may sit in Redis before the sweep
# takes it down. Read from the module that owns the value, so the sweep and the
# apply that retires its own batch cannot drift apart.
try:
    from utils.batch_pipeline import BATCH_STALE_TTL_SECONDS as _BATCH_STALE_TTL_SECONDS
except Exception:  # pragma: no cover - the module is always present in-tree
    _BATCH_STALE_TTL_SECONDS = int(os.getenv("BATCH_STALE_TTL_SECONDS", "60"))

logger = logging.getLogger(__name__)

# Directory placeholders that must never be treated as stale data.
_PLACEHOLDER_FILES = frozenset({"README.md", ".gitkeep", ".gitignore"})


def _env_ttl(name: str) -> int | None:
    """A seconds TTL from the environment, or ``None`` when it says nothing.

    ``0`` is a real answer - "no grace period at all" - so it is kept apart from
    unset rather than being read as one: only an empty value falls back to the
    caller's default.
    """
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return None
    try:
        return max(0, int(float(raw)))
    except (TypeError, ValueError):
        logger.warning("cleanup: %s=%r is not a number; ignoring it", name, raw)
        return None


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
        self.s3_output_ttl = int(os.getenv("S3_OUTPUTS_TTL", str(24 * 3600)))
        # Shared media library (``inputs/library/<hash>/source``): one object per
        # media, reused by every operation on it. Deliberately much longer than
        # the per-job input TTL and exempt from that sweep - deleting it is what
        # forced the next request for the same media to download and upload a
        # fresh copy again, and it never went through the inputs/ janitor.
        #
        # 30 days, because a re-download is the expensive side of the trade: it is
        # a full copy of the media in egress (the thing that gets a free-egress
        # account suspended) plus the upload, while keeping the object is one
        # stored copy - which also raises the allowance itself, since the free
        # egress is a multiple of what is stored. The sweep still bounds the
        # bucket, so it stays a cache rather than a permanent archive.
        self.s3_library_ttl = int(os.getenv("S3_LIBRARY_TTL", str(30 * 24 * 3600)))
        # How long a *superseded* version of an object may stay. Every S3 bucket
        # keeps versions, and a plain delete on one only writes a marker: the
        # bytes stay and keep counting against the plan, which is how a bucket
        # holding seven objects filled an account. Unset keeps the prefix's own
        # TTL, the same window S3's NoncurrentVersionExpiration would use; ``0``
        # drops a superseded version the moment a sweep sees it.
        self.s3_noncurrent_ttl = _env_ttl("S3_NONCURRENT_TTL_SECONDS")
        # Redis job hash cleanup interval (30 minutes)
        self.redis_cleanup_interval = int(os.getenv("REDIS_CLEANUP_INTERVAL", str(30 * 60)))
        # Stale Redis job hash max age (24 hours)
        self.redis_job_max_age = int(os.getenv("REDIS_JOB_MAX_AGE", str(24 * 3600)))
        # How often finished batches are swept out of Redis. A finished Apply
        # retires its own batch; this is the backstop for one it never got to,
        # and it is what a manual scripts/cleanup_stale_redis.py run used to do.
        self.batch_stale_ttl = max(1, int(os.getenv("BATCH_STALE_TTL_SECONDS", str(_BATCH_STALE_TTL_SECONDS))))
        self.is_running = False
        self._redis_cleanup_task = None
        self._batch_sweep_task = None

    async def start(self):
        """Start periodic cleanup tasks (hourly file cleanup + 30-min Redis cleanup)."""
        self.is_running = True
        # Start the Redis cleanup loop as a separate background task
        self._redis_cleanup_task = asyncio.create_task(self._redis_cleanup_loop())
        # The finished-batch sweep runs on its own short clock, so the traces a
        # finished Apply leaves are gone in about a minute rather than after the
        # 30-day state TTL (or a manual cleanup script).
        self._batch_sweep_task = asyncio.create_task(self._batch_sweep_loop())
        logger.info(
            "Cleanup manager started (file cleanup every %ds, Redis cleanup every %ds, batch sweep every %ds)",
            self.cleanup_interval,
            self.redis_cleanup_interval,
            self.batch_stale_ttl,
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
        if self._batch_sweep_task and not self._batch_sweep_task.done():
            self._batch_sweep_task.cancel()
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
            "redis_lock_keys": await self.cleanup_stale_locks(),
            "finished_batches": await self.cleanup_finished_batches(),
            "empty_dirs": await self.cleanup_empty_directories(),
            "rate_limit_buckets": self.cleanup_rate_limit_buckets(),
            "web_job_store": self.cleanup_web_job_store(),
            # ── S3 / R2 remote cleanup ──
            "s3_inputs": await self.cleanup_s3_inputs(),
            "s3_uploads": await self.cleanup_s3_uploads(),
            "s3_forwards": await self.cleanup_s3_forwards(),
            "s3_outputs": await self.cleanup_s3_outputs(),
            "s3_library": await self.cleanup_s3_library(),
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
                    if item in _PLACEHOLDER_FILES:
                        # ``README.md``/``.gitkeep`` are how an empty storage
                        # directory survives a checkout; they are not stale data
                        # and deleting them is what removes a tracked file from
                        # a working tree.
                        continue
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

    def cleanup_rate_limit_buckets(self) -> int:
        """Drop in-memory rate-limiter buckets for clients that have gone quiet.

        The limiter is keyed by client IP (which the caller supplies through
        ``X-Forwarded-For``), so without a periodic pruning pass it holds an entry
        per address that ever called a public endpoint until it next hits its cap.
        """
        try:
            from utils.web_rate_limiter import web_rate_limiter

            pruned = int(web_rate_limiter.prune())
            if pruned:
                logger.info("rate limiter cleanup: dropped %d idle bucket(s)", pruned)
            return pruned
        except Exception:
            logger.debug("cleanup: rate limiter prune unavailable")
            return 0

    def cleanup_web_job_store(self) -> int:
        """Prune the web UI's in-memory fallback job store.

        It prunes on write as well; this is the backstop for a process that has
        gone quiet. Only touched when the module is already loaded - importing a
        Flask app into a service that does not serve HTTP would be pure cost.
        """
        try:
            import sys

            webapp = sys.modules.get("web.webapp")
            if webapp is None:
                return 0
            pruned = int(webapp._job_store_prune())
            if pruned:
                logger.info("web job store cleanup: dropped %d stale entr(ies)", pruned)
            return pruned
        except Exception:
            logger.debug("cleanup: web job store prune unavailable")
            return 0

    async def cleanup_s3_inputs(self) -> int:
        """Clean old per-job input files from S3 under the ``inputs/`` prefix.

        The shared library (``inputs/library/``) is excluded: it is a cache with
        its own, longer TTL, and sweeping it here is what silently turned every
        repeat of the same media back into a fresh download plus upload.
        """
        return await self._cleanup_s3_prefix("inputs/", self.s3_input_ttl, exclude_prefixes=(LIBRARY_KEY_PREFIX,))

    async def cleanup_s3_library(self) -> int:
        """Clean cached library objects older than ``S3_LIBRARY_TTL``.

        One object per media, shared by every style/button applied to it. The
        window is measured so the entry survives the normal burst of operations
        on a media, then drops so the bucket is not a permanent archive.
        """
        return await self._cleanup_s3_prefix(LIBRARY_KEY_PREFIX, self.s3_library_ttl)

    async def cleanup_s3_uploads(self) -> int:
        """Clean old uploaded files from S3 under the ``uploads/`` prefix."""
        return await self._cleanup_s3_prefix("uploads/", self.s3_upload_ttl)

    async def cleanup_s3_forwards(self) -> int:
        """Clean old forward metadata files from S3 under the ``forwards/`` prefix."""
        return await self._cleanup_s3_prefix("forwards/", self.s3_forward_ttl)

    async def cleanup_s3_outputs(self) -> int:
        """Clean delivered results from S3 under the ``outputs/`` prefix.

        Kept long enough to cover a job the user comes back for and any presigned
        URL still in flight (``PRESIGN_EXPIRES`` is an hour by default), then
        dropped. Without this the prefix only ever grew, so every result the bot
        ever produced stayed in the bucket as an object something could later
        download - which is exactly the egress this account was suspended over.
        """
        return await self._cleanup_s3_prefix("outputs/", self.s3_output_ttl)

    async def _cleanup_s3_prefix(
        self, prefix: str, max_age_seconds: int, exclude_prefixes: tuple[str, ...] = ()
    ) -> int:
        """List objects under an S3 *prefix* and delete those older than *max_age_seconds*.

        Objects under any of *exclude_prefixes* are skipped: a caller sweeping a
        broad prefix (``inputs/``) must not remove a nested prefix that has its
        own lifecycle (``inputs/library/``).

        On a bucket that keeps versions the delete is only half the job - it writes
        a marker and the bytes stay - so the sweep ends by purging what a plain
        delete leaves behind (:meth:`_purge_version_residue`). The count it returns
        is what the sweep actually took out of the bucket: expired keys *and* the
        versions and markers that went with them.
        """
        try:
            from utils.storage import get_storage_backend

            backend = await get_storage_backend()

            # Only attempt S3 / R2 cleanup when the active backend is actually remote
            _bn = config.get_storage_backend_name() if config else (os.getenv("STORAGE_BACKEND") or "local").lower()
            if _bn not in ("s3", "r2"):
                return 0

            objects = await backend.list_keys(prefix)

            now = time.time()
            to_delete = [
                obj["key"]
                for obj in objects
                if (now - obj["last_modified"]) > max_age_seconds and not obj["key"].startswith(tuple(exclude_prefixes))
            ]

            deleted = 0
            if to_delete:
                deleted = await backend.delete_keys(to_delete)
                logger.info(
                    "S3 cleanup: prefix=%s deleted=%d/%d candidates=%d (TTL=%ds)",
                    prefix,
                    deleted,
                    len(to_delete),
                    len(objects),
                    max_age_seconds,
                )

            # What the delete above left behind when the bucket keeps versions.
            # It runs even when nothing expired this time: the residue of earlier
            # sweeps is invisible to the listing above and is exactly what fills
            # a plan up.
            purged = await self._purge_version_residue(
                backend,
                prefix,
                max_age_seconds,
                exclude_prefixes,
                expired=set(to_delete),
            )
            return deleted + purged
        except Exception as e:
            logger.error("S3 cleanup failed for prefix=%s: %s", prefix, e)
            return 0

    async def _purge_version_residue(
        self,
        backend,
        prefix: str,
        max_age_seconds: int,
        exclude_prefixes: tuple[str, ...],
        *,
        expired: set[str],
    ) -> int:
        """Remove the versions and delete markers a plain delete leaves behind.

        A versioned bucket - and every S3 bucket is one unless somebody turned it
        off - answers a plain delete with a *delete marker*: the object is hidden,
        the bytes stay, and the marker becomes the key's newest version, so the
        next sweep sees a fresh object and leaves the whole history alone. Seven
        live objects plus twenty-three invisible versions is a full plan and a
        bucket that refuses every write, while every sweep reports success.

        Three kinds of entry go, none of them visible to ``list_keys``:

        * whatever is left of a key this sweep just expired - markers included,
          which is what makes the expiry stick instead of adding a tombstone;
        * a superseded version older than the prefix's own TTL: nobody reads it
          (every reader resolves the newest version) and it only costs space;
        * a delete marker that has outlived its TTL. A marker is not an object, it
          is the tombstone of one, and nothing ever removed it.

        Backends without :attr:`supports_object_versions` (a local filesystem, a
        test double) are left exactly as they were.
        """
        if not getattr(backend, "supports_object_versions", False):
            return 0
        try:
            versions = await backend.list_versions(prefix)
        except NotImplementedError:
            return 0
        if not versions:
            return 0

        max_age = int(self.s3_noncurrent_ttl) if self.s3_noncurrent_ttl is not None else int(max_age_seconds)
        excluded = tuple(exclude_prefixes)
        now = time.time()
        doomed: list[tuple[str, str]] = []
        for row in versions:
            key = row.get("key")
            version_id = row.get("version_id")
            if not key or not version_id or (excluded and key.startswith(excluded)):
                continue
            if key in expired:
                # The TTL expired this key: every copy of it goes, whatever the
                # listing says about which one is current.
                doomed.append((key, version_id))
                continue
            age = now - float(row.get("last_modified") or 0)
            stale = max_age <= 0 or age > max_age
            if not stale:
                continue
            if row.get("is_delete_marker") or (not row.get("is_latest") and version_id != "null"):
                doomed.append((key, version_id))

        if not doomed:
            return 0

        purged = await backend.delete_versions(doomed)
        logger.info(
            "S3 cleanup: prefix=%s purged=%d/%d version(s) and marker(s) (superseded TTL=%ds)",
            prefix,
            purged,
            len(doomed),
            max_age,
        )
        return purged

    # ─────────────────────────────────────────────────────────────────────
    # Redis job hash cleanup
    # ─────────────────────────────────────────────────────────────────────

    async def cleanup_stale_locks(self) -> int:
        """Delete stale input lock keys (``ffmpeg:lock:*``).

        A lock is stale if its associated job hash no longer exists
        (the worker deletes job hashes after delivery), or the job
        status is terminal (done/error/cancelled).

        Returns the number of locks deleted.
        """
        try:
            from utils.job_queue import get_redis

            r = await get_redis()
        except Exception as e:
            logger.debug("lock_cleanup: cannot connect to Redis: %s", e)
            return 0

        deleted = 0
        try:
            cursor = 0
            while True:
                try:
                    cursor, keys = await r.scan(cursor, match="ffmpeg:lock:*", count=100)
                except Exception:
                    break

                for key in keys:
                    try:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        val = await r.get(key)
                        if not val:
                            # Already expired — delete
                            await r.delete(key)
                            deleted += 1
                            continue

                        val_str = val.decode() if isinstance(val, bytes) else str(val)

                        # Check if the owning job is still active
                        job_hash = await r.hgetall(f"ffmpeg:job:{val_str}")
                        if not job_hash:
                            # Job hash doesn't exist — stale lock
                            await r.delete(key)
                            deleted += 1
                            logger.debug("lock_cleanup: deleted orphaned lock %s (job %s gone)", key_str, val_str)
                            continue

                        status = job_hash.get(b"status") or job_hash.get("status")
                        if status:
                            status = status.decode() if isinstance(status, bytes) else str(status)
                        else:
                            status = ""

                        if status in ("done", "error", "cancelled", ""):
                            await r.delete(key)
                            deleted += 1
                            logger.debug("lock_cleanup: deleted lock %s (job %s status=%s)", key_str, val_str, status)

                    except Exception as e:
                        logger.debug("lock_cleanup: error processing key %s: %s", key, e)

                if cursor == 0:
                    break

            if deleted > 0:
                logger.info("lock_cleanup: deleted %d stale lock keys", deleted)
        except Exception as e:
            logger.error("lock_cleanup: unexpected error: %s", e)
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
                            logger.debug(
                                "redis_dedup_cleanup: deleted key %s (job %s status=%s)", key_str, val_str, status
                            )

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

    async def cleanup_finished_batches(self) -> int:
        """Retire batches whose members have all reported but whose keys linger.

        The bot's own answer to running ``scripts/cleanup_stale_redis.py`` by hand
        for the traces a finished Apply leaves behind. Safe by construction: only a
        batch whose ``:done`` counter has reached its ``:total`` is touched, so a
        batch that is still being fed one file at a time is never torn down.

        Returns the number of batches retired.
        """
        try:
            from utils.batch_pipeline import sweep_finished_batches
        except Exception:
            logger.debug("finished-batch sweep unavailable")
            return 0
        try:
            summary = await sweep_finished_batches()
            return len(summary.get("batches") or [])
        except Exception as e:
            logger.debug("finished-batch sweep failed: %s", e)
            return 0

    async def _batch_sweep_loop(self):
        """Sweep finished batches on their own short clock (BATCH_STALE_TTL_SECONDS)."""
        # A short delay before the first run, so startup is not the busiest moment.
        await asyncio.sleep(min(30, self.batch_stale_ttl))
        while self.is_running:
            try:
                retired = await self.cleanup_finished_batches()
                if retired > 0:
                    logger.info("Batch sweep: retired %d finished batch(es)", retired)
            except Exception as e:
                logger.error("Batch sweep loop error: %s", e)
            await asyncio.sleep(self.batch_stale_ttl)

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
            if os.path.isfile(item_path) and item in _PLACEHOLDER_FILES:
                continue
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
