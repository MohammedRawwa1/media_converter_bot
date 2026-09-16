"""Asynchronous storage backend abstraction.

Provides a small async-friendly wrapper for local filesystem storage and
S3/S3-compatible (e.g. Cloudflare R2) using `aioboto3`.

Usage example:
    from utils.storage import get_storage_backend

    storage = await get_storage_backend()
    await storage.upload_file("/tmp/video.mp4", "uploads/job123/video.mp4")
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from abc import ABC, abstractmethod
from typing import Any

try:
    import aioboto3
except Exception:  # pragma: no cover - aioboto3 may be optional
    aioboto3 = None

try:
    import boto3
except Exception:
    boto3 = None

try:
    from botocore.config import Config as BotoConfig
except Exception:
    BotoConfig = None

import config

logger = logging.getLogger(__name__)


class AsyncStorageBackend(ABC):
    @abstractmethod
    async def upload_file(self, src_path: str, dest_key: str) -> str:
        """Upload a local file at `src_path` to storage and return the storage key or path."""

    @abstractmethod
    async def download_file(self, key: str, dest_path: str) -> bool:
        """Download a storage object `key` to local `dest_path`. Return True on success."""

    @abstractmethod
    async def generate_presigned_post(self, key: str, expires: int | None = None) -> dict[str, Any]:
        """Return a dict with presigned POST upload info (url/fields) or raise when unsupported."""

    @abstractmethod
    async def generate_presigned_get(self, key: str, expires: int | None = None) -> str:
        """Return a presigned GET URL for `key` or raise when unsupported."""

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete object at `key` from storage. Return True if deleted or not found."""

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Return True if object `key` exists in storage, False otherwise."""

    @abstractmethod
    async def list_keys(self, prefix: str = "") -> list[dict[str, Any]]:
        """List keys under a prefix, returning key name, last_modified, and size.

        Each entry in the returned list is a dict with:
            - "key": the full storage key
            - "last_modified": a float UNIX timestamp (seconds since epoch)
            - "size": file size in bytes
        Returns an empty list when the prefix yields no objects or when the
        backend does not support listing.
        """

    @abstractmethod
    async def delete_keys(self, keys: list[str]) -> int:
        """Bulk-delete the given keys. Returns the count of successfully deleted keys.

        Backends that do not support bulk-delete may fall back to calling
        `delete()` in a loop.  Returns 0 when no keys are provided.
        """

    async def usage(self, *, max_objects: int = 5000, group_depth: int = 1) -> dict[str, Any]:
        """Count objects and total bytes held by the backend.

        Returns a dict with ``backend``, ``location``, ``objects``, ``bytes``,
        ``truncated`` and ``groups`` - where ``groups`` attributes the totals to
        the first ``group_depth`` path segments of each key (``uploads/``,
        ``inputs/``, ...) so a full bucket can be explained without a second
        listing. The scan stops after ``max_objects`` so it stays bounded no
        matter how large the store grows, setting ``truncated`` when it did.

        Backends that cannot list their contents raise ``NotImplementedError`` -
        callers are expected to catch that rather than treat it as zero usage.
        """
        raise NotImplementedError("storage usage is not supported by this backend")


def _usage_group(key: str, depth: int) -> str:
    """The group a key's bytes are attributed to (``uploads/``, ``(root)``)."""
    parts = str(key).split("/")
    if len(parts) <= 1:
        return "(root)"
    return "/".join(parts[:depth]) + "/"


def _usage_add(
    groups: dict[str, dict[str, int]], key: str, size: int, depth: int
) -> None:
    """Fold one object into the group totals."""
    name = _usage_group(key, depth)
    row = groups.setdefault(name, {"objects": 0, "bytes": 0})
    row["objects"] += 1
    row["bytes"] += size


# ── Egress accounting ───────────────────────────────────────────────────────
#
# IDrive e2 - and the "free egress" policies in this class generally - grant
# free egress worth a *multiple of what you store* per billing cycle (e2's is
# 3x). The ratio is what gets an account suspended, not the absolute total, so
# counting the bytes that leave the bucket is what turns "we were disabled for
# an egress violation" into a number somebody can watch before it recurs.
#
# Two kinds are tracked because they are not the same measurement:
#   * ``object`` - bytes this process pulled *out* of the bucket. Measured.
#   * ``link``   - presigned GET URLs handed out. A *count*, not bytes: whether
#     (or how often) a URL is fetched happens outside this process, so each one
#     is at least one more copy of that object's bytes as egress.
EGRESS_REDIS_PREFIX = "storage:egress"
# The provider's free-egress allowance, as a multiple of stored bytes.
EGRESS_FREE_MULTIPLIER = float(os.getenv("EGRESS_FREE_MULTIPLIER", "3"))
# Log a warning every time this much egress accumulates within the cycle.
EGRESS_WARN_STEP_BYTES = int(float(os.getenv("EGRESS_WARN_STEP_GB", "50")) * 1024**3)
# The ratio of the allowance at which the dashboard starts flagging it.
EGRESS_WATCH_PERCENT = float(os.getenv("EGRESS_WATCH_PERCENT", "80"))
# Counters are written per cycle and read back by the dashboard, so they outlive
# the cycle by a margin - then expire. Without this, one key per cycle per counter
# would accumulate for the life of the bucket.
COUNTER_TTL_SECONDS = int(float(os.getenv("STORAGE_COUNTER_TTL_DAYS", "60")) * 86400)

# Kept in-process so the counter still moves when Redis is unreachable, and so
# callers never depend on Redis to get a reading.
_EGRESS_LOCAL: dict[str, int] = {}


def egress_period(now: float | None = None) -> str:
    """The billing-cycle bucket key for *now* - UTC, ``YYYY-MM``."""
    return time.strftime("%Y-%m", time.gmtime(time.time() if now is None else now))


def _egress_key(period: str, kind: str) -> str:
    return f"{EGRESS_REDIS_PREFIX}:{period}:{kind}"


def _egress_int(value) -> int:
    """A counter read back from Redis, as an int (``None`` becomes 0)."""
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


async def record_egress(nbytes, *, kind: str = "object", period: str | None = None) -> int:
    """Add to this cycle's egress counter, and return the new total.

    Best-effort by design: metering must never be able to break a download, so
    every failure here degrades to the process-local count.
    """
    try:
        nbytes = int(nbytes)
    except (TypeError, ValueError):
        return 0
    if nbytes <= 0:
        return 0

    period = period or egress_period()
    key = _egress_key(period, kind)
    total = _EGRESS_LOCAL.get(key, 0) + nbytes
    _EGRESS_LOCAL[key] = total

    try:
        from utils.job_queue import get_redis

        client = await get_redis()
        total = int(await client.incrby(key, nbytes))
        if COUNTER_TTL_SECONDS > 0:
            try:
                await client.expire(key, COUNTER_TTL_SECONDS)
            except Exception:
                # A counter without a TTL is untidy, not fatal.
                logger.debug("egress: could not set a TTL on the %s counter", key)
    except Exception:
        logger.debug("egress: Redis counter unavailable; keeping the process-local total")

    # One line per step, not per byte: a busy cycle must not turn the egress
    # counter itself into a log flood.
    if EGRESS_WARN_STEP_BYTES > 0 and (total // EGRESS_WARN_STEP_BYTES) != (
        (total - nbytes) // EGRESS_WARN_STEP_BYTES
    ):
        logger.warning(
            "egress: %s bytes have left storage this cycle (%s): %.1f GB against an "
            "allowance of x%g stored bytes - check the free-egress policy before it is "
            "exceeded again",
            kind,
            period,
            total / 1024**3,
            EGRESS_FREE_MULTIPLIER,
        )
    return total


async def egress_snapshot(*, period: str | None = None, stored_bytes=None) -> dict:
    """This cycle's egress, the free allowance, and how close we are to it.

    ``stored_bytes`` comes from the caller's usage scan: the allowance is a
    multiple of it, so without a reading the ratio is reported as unknown
    rather than guessed.
    """
    period = period or egress_period()
    snapshot: dict[str, Any] = {
        "period": period,
        "object_bytes": _EGRESS_LOCAL.get(_egress_key(period, "object"), 0),
        "links_issued": _EGRESS_LOCAL.get(_egress_key(period, "link"), 0),
        "stored_bytes": None,
        "allowance_bytes": None,
        "percent": None,
        "status": "unknown",
        "shared": False,
    }

    try:
        from utils.job_queue import get_redis

        client = await get_redis()
        values = await client.mget(_egress_key(period, "object"), _egress_key(period, "link"))
        if isinstance(values, (list, tuple)) and len(values) >= 2:
            snapshot["object_bytes"] = _egress_int(values[0])
            snapshot["links_issued"] = _egress_int(values[1])
            snapshot["shared"] = True
    except Exception:
        logger.debug("egress: could not read the shared counter; reporting the local total")

    try:
        stored = int(stored_bytes) if stored_bytes is not None else None
    except (TypeError, ValueError):
        stored = None

    if stored is not None and stored >= 0:
        allowance = int(stored * EGRESS_FREE_MULTIPLIER)
        snapshot["stored_bytes"] = stored
        snapshot["allowance_bytes"] = allowance
        if allowance > 0:
            percent = snapshot["object_bytes"] * 100.0 / allowance
            snapshot["percent"] = percent
            if percent >= 100:
                snapshot["status"] = "over"
            elif percent >= EGRESS_WATCH_PERCENT:
                snapshot["status"] = "watch"
            else:
                snapshot["status"] = "ok"
        else:
            # Nothing stored means no free egress at all, so any traffic at all
            # is already over the allowance.
            snapshot["status"] = "over" if snapshot["object_bytes"] else "ok"
    return snapshot


# ── Shared source cache accounting ───────────────────────────────────────────
# One media is one library object, and every operation on it may read that object
# again. A "hit" is a job served from the local copy of that object, so the bytes
# never left the bucket; a "miss" is a job that had to pull them out. The ratio is
# what tells you whether the cache is actually doing anything on a given workload,
# and ``bytes_saved`` is the egress it avoided this cycle.
SOURCE_CACHE_REDIS_PREFIX = "storage:source_cache"
_SOURCE_CACHE_LOCAL: dict[str, int] = {}


def _source_cache_key(period: str, field: str) -> str:
    return f"{SOURCE_CACHE_REDIS_PREFIX}:{period}:{field}"


async def record_source_cache(hit: bool, *, nbytes=0, period: str | None = None) -> dict:
    """Count one source fetch as a cache *hit* or *miss*; return the new totals.

    Best-effort like the egress counters: bookkeeping must never fail a job.
    """
    period = period or egress_period()
    try:
        nbytes = max(0, int(nbytes))
    except (TypeError, ValueError):
        nbytes = 0
    field = "hits" if hit else "misses"

    local_hits = _SOURCE_CACHE_LOCAL.get(_source_cache_key(period, "hits"), 0) + (1 if hit else 0)
    local_misses = _SOURCE_CACHE_LOCAL.get(_source_cache_key(period, "misses"), 0) + (0 if hit else 1)
    local_bytes = _SOURCE_CACHE_LOCAL.get(_source_cache_key(period, "bytes"), 0) + (nbytes if hit else 0)
    _SOURCE_CACHE_LOCAL[_source_cache_key(period, "hits")] = local_hits
    _SOURCE_CACHE_LOCAL[_source_cache_key(period, "misses")] = local_misses
    _SOURCE_CACHE_LOCAL[_source_cache_key(period, "bytes")] = local_bytes

    try:
        from utils.job_queue import get_redis

        client = await get_redis()
        counter_key = _source_cache_key(period, field)
        await client.incr(counter_key)
        if hit and nbytes > 0:
            await client.incrby(_source_cache_key(period, "bytes"), nbytes)
        if COUNTER_TTL_SECONDS > 0:
            for key in (
                counter_key,
                _source_cache_key(period, "hits"),
                _source_cache_key(period, "misses"),
                _source_cache_key(period, "bytes"),
            ):
                try:
                    await client.expire(key, COUNTER_TTL_SECONDS)
                except Exception:
                    logger.debug("source cache: could not set a TTL on %s", key)
    except Exception:
        logger.debug("source cache: Redis counter unavailable; keeping the process-local totals")

    return {"hits": local_hits, "misses": local_misses, "bytes": local_bytes}


async def source_cache_snapshot(*, period: str | None = None) -> dict:
    """This cycle's source-cache hits/misses and the bytes kept out of the bucket."""
    period = period or egress_period()
    snapshot: dict[str, Any] = {
        "period": period,
        "hits": _SOURCE_CACHE_LOCAL.get(_source_cache_key(period, "hits"), 0),
        "misses": _SOURCE_CACHE_LOCAL.get(_source_cache_key(period, "misses"), 0),
        "bytes_saved": _SOURCE_CACHE_LOCAL.get(_source_cache_key(period, "bytes"), 0),
        "shared": False,
    }

    try:
        from utils.job_queue import get_redis

        client = await get_redis()
        values = await client.mget(
            _source_cache_key(period, "hits"),
            _source_cache_key(period, "misses"),
            _source_cache_key(period, "bytes"),
        )
        if isinstance(values, (list, tuple)) and len(values) >= 3:
            snapshot["hits"] = _egress_int(values[0])
            snapshot["misses"] = _egress_int(values[1])
            snapshot["bytes_saved"] = _egress_int(values[2])
            snapshot["shared"] = True
    except Exception:
        logger.debug("source cache: could not read the shared counters; reporting the local totals")

    total = snapshot["hits"] + snapshot["misses"]
    snapshot["fetches"] = total
    snapshot["hit_percent"] = (snapshot["hits"] * 100.0 / total) if total else None
    return snapshot


class LocalStorageBackend(AsyncStorageBackend):
    def __init__(self, base_path: str | None = None):
        self.base = base_path or config.STORAGE_PATH

    def _abs_path(self, key: str) -> str:
        return os.path.join(self.base, key)

    async def upload_file(self, src_path: str, dest_key: str) -> str:
        dest = self._abs_path(dest_key)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        await asyncio.to_thread(shutil.copy2, src_path, dest)
        return dest

    async def download_file(self, key: str, dest_path: str) -> bool:
        src = self._abs_path(key)
        if not os.path.exists(src):
            return False
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        await asyncio.to_thread(shutil.copy2, src, dest_path)
        return True

    async def generate_presigned_post(self, key: str, expires: int | None = None) -> dict[str, Any]:
        raise NotImplementedError("Presigned uploads are not supported for local backend")

    async def generate_presigned_get(self, key: str, expires: int | None = None) -> str:
        # Provide a file:// URL for convenience (may not be usable remotely)
        return "file://" + os.path.abspath(self._abs_path(key))

    async def delete(self, key: str) -> bool:
        p = self._abs_path(key)
        try:
            if os.path.exists(p):
                await asyncio.to_thread(os.remove, p)
                return True
            return True
        except Exception:
            return False

    async def exists(self, key: str) -> bool:
        p = self._abs_path(key)
        try:
            return os.path.exists(p)
        except Exception:
            return False

    async def list_keys(self, prefix: str = "") -> list[dict[str, Any]]:
        """List local files under a prefix directory.

        The prefix is treated as a relative directory path.  Returns an
        empty dict if the directory does not exist.
        """
        base = self._abs_path(prefix)
        if not os.path.isdir(base):
            return []
        results: list[dict[str, Any]] = []
        try:
            for entry in os.scandir(base):
                if entry.is_file():
                    st = entry.stat()
                    results.append(
                        {
                            "key": os.path.join(prefix, entry.name).replace("\\", "/"),
                            "last_modified": st.st_mtime,
                            "size": st.st_size,
                        }
                    )
        except Exception:
            pass
        return results

    async def delete_keys(self, keys: list[str]) -> int:
        """Delete multiple local files. Calls `asyncio.to_thread` for each."""
        if not keys:
            return 0
        deleted = 0
        for key in keys:
            try:
                p = self._abs_path(key)
                if os.path.exists(p):
                    await asyncio.to_thread(os.remove, p)
                deleted += 1
            except Exception:
                pass
        return deleted

    async def usage(self, *, max_objects: int = 5000, group_depth: int = 1) -> dict[str, Any]:
        """Count files and bytes under the storage root, bounded by *max_objects*."""
        cap = max(1, int(max_objects))
        depth = max(1, int(group_depth))
        tally = {"objects": 0, "bytes": 0, "truncated": False}
        groups: dict[str, dict[str, int]] = {}

        def _scan() -> None:
            # Not following symlinks keeps a stray link from walking the box.
            for root, _dirs, files in os.walk(self.base, followlinks=False):
                for name in files:
                    path = os.path.join(root, name)
                    try:
                        size = int(os.path.getsize(path))
                    except OSError:
                        continue
                    key = os.path.relpath(path, self.base).replace("\\", "/")
                    tally["objects"] += 1
                    tally["bytes"] += size
                    _usage_add(groups, key, size, depth)
                    if tally["objects"] >= cap:
                        tally["truncated"] = True
                        return

        await asyncio.to_thread(_scan)
        return {
            "backend": "local",
            "location": self.base,
            "objects": tally["objects"],
            "bytes": tally["bytes"],
            "truncated": tally["truncated"],
            "groups": groups,
        }


class S3AsyncBackend(AsyncStorageBackend):
    def __init__(
        self,
        bucket: str | None = None,
        endpoint_url: str | None = None,
        region: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        use_ssl: bool = True,
    ):
        # Support both async aioboto3 (preferred) and sync boto3 (fallback).
        # If aioboto3 is present we will use it for non-blocking IO. Otherwise
        # we will call synchronous boto3 functions inside `asyncio.to_thread`.
        self._use_aioboto3 = aioboto3 is not None

        self.bucket = bucket or config.S3_BUCKET
        self.endpoint_url = endpoint_url or (config.S3_ENDPOINT or None)
        self.region = region or (config.S3_REGION or None)
        self.aws_access_key_id = aws_access_key_id or config.AWS_ACCESS_KEY_ID or None
        self.aws_secret_access_key = aws_secret_access_key or config.AWS_SECRET_ACCESS_KEY or None
        # Support temporary session tokens (AWS STS / assumed-role / R2 variants)
        self.aws_session_token = aws_session_token or os.getenv("AWS_SESSION_TOKEN") or None
        self.use_ssl = use_ssl

        # async session only when aioboto3 is available
        self._session = aioboto3.Session() if self._use_aioboto3 else None

        # optional botocore config (used for both aioboto3 and boto3 clients)
        self._boto_config = None
        if BotoConfig is not None:
            try:
                # Allow forcing path-style addressing for S3-compatible endpoints
                force_path = str(os.getenv("S3_FORCE_PATH_STYLE", "")).lower() in ("1", "true", "yes")
                if force_path:
                    try:
                        # prefer explicit addressing style when requested
                        self._boto_config = BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"})
                    except Exception:
                        # fallback to default config object
                        self._boto_config = BotoConfig(signature_version="s3v4")
                else:
                    self._boto_config = BotoConfig(signature_version="s3v4")
            except Exception:
                self._boto_config = None

    def _client_kwargs(self) -> dict[str, Any]:
        kw = {}
        if self.region:
            kw["region_name"] = self.region
        if self.endpoint_url:
            # Ensure endpoint_url has a scheme (boto3 requires https:// or http://)
            ep = str(self.endpoint_url).strip()
            if ep and not ep.startswith("http://") and not ep.startswith("https://"):
                scheme = "https" if self.use_ssl else "http"
                ep = f"{scheme}://{ep}"
            # Strip any trailing slash
            ep = ep.rstrip("/")
            kw["endpoint_url"] = ep
        if self.aws_access_key_id:
            kw["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            kw["aws_secret_access_key"] = self.aws_secret_access_key
        if self._boto_config is not None:
            kw["config"] = self._boto_config
        if self.aws_session_token:
            kw["aws_session_token"] = self.aws_session_token
        return kw

    async def upload_file(self, src_path: str, dest_key: str) -> str:
        if not src_path:
            raise ValueError(f"Invalid src_path: {src_path}")
        src_path = os.path.abspath(src_path)
        if not os.path.exists(src_path):
            raise ValueError(f"Invalid src_path: {src_path}")

        if not dest_key:
            raise ValueError("dest_key must not be empty")

        if not self.bucket:
            raise ValueError(f"Invalid S3 bucket name: {self.bucket}")

        # Ensure the configured S3 bucket is a bucket name, not a URL
        if "http://" in str(self.bucket) or "https://" in str(self.bucket):
            raise ValueError(f"S3_BUCKET must be a bucket name, not a URL: {self.bucket}")

        # Retry/backoff parameters
        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))

        for attempt in range(1, retries + 1):
            try:
                # Masked diagnostics (do not log secrets). Show partial key and endpoint for debugging.
                masked_key = None
                if self.aws_access_key_id:
                    ak = str(self.aws_access_key_id)
                    masked_key = f"{ak[:4]}...{ak[-4:]}" if len(ak) > 8 else ak
                else:
                    masked_key = "(env)"

                logger.info(
                    "Uploading file → bucket=%s key=%s (attempt %s/%s) [ak=%s endpoint=%s]",
                    self.bucket,
                    dest_key,
                    attempt,
                    retries,
                    masked_key,
                    (self.endpoint_url or "default"),
                )

                # Async path (aioboto3)
                if self._use_aioboto3:
                    async with self._session.client("s3", **self._client_kwargs()) as client:
                        await client.upload_file(src_path, self.bucket, dest_key)
                    return dest_key

                # Sync fallback (boto3)
                if boto3 is None:
                    raise RuntimeError("boto3 is required when aioboto3 is not installed")

                def _sync_upload():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.upload_file(src_path, self.bucket, dest_key)

                await asyncio.to_thread(_sync_upload)
                return dest_key

            except Exception as e:
                logger.warning(
                    "S3 upload failed (attempt %s/%s): %s",
                    attempt,
                    retries,
                    e,
                )

                if attempt == retries:
                    logger.exception("S3 upload failed permanently for key=%s", dest_key)
                    raise

                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                # Use deterministic jitter (based on attempt number) to avoid S311 insecure-random warning
                _jitter = (attempt * 9973) % 1000 / 1000  # deterministic fractional jitter
                await asyncio.sleep(backoff + _jitter)

    async def upload_file_streaming(self, src_path: str, dest_key: str) -> str:
        src_path = os.path.abspath(src_path)
        if not os.path.exists(src_path):
            raise ValueError(f"File not found: {src_path}")

        # Async path (aioboto3)
        if self._use_aioboto3:
            async with self._session.client("s3", **self._client_kwargs()) as client:
                with open(src_path, "rb") as f:
                    await client.put_object(
                        Bucket=self.bucket,
                        Key=dest_key,
                        Body=f,
                    )
            return dest_key

        # Sync fallback
        if boto3 is None:
            raise RuntimeError("boto3 is required when aioboto3 is not installed")

        def _sync():
            client = boto3.client("s3", **self._client_kwargs())
            with open(src_path, "rb") as f:
                client.put_object(Bucket=self.bucket, Key=dest_key, Body=f)

        await asyncio.to_thread(_sync)
        return dest_key

    async def upload_bytes(self, data: bytes, dest_key: str) -> str:
        """Upload bytes directly to S3 without writing to local disk first."""
        if not dest_key:
            raise ValueError("dest_key must not be empty")
        if not self.bucket:
            raise ValueError(f"Invalid S3 bucket name: {self.bucket}")
        if "http://" in str(self.bucket) or "https://" in str(self.bucket):
            raise ValueError(f"S3_BUCKET must be a bucket name, not a URL: {self.bucket}")

        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))
        for attempt in range(1, retries + 1):
            try:
                logger.info(
                    "Uploading bytes → bucket=%s key=%s (attempt %s/%s) size=%d",
                    self.bucket,
                    dest_key,
                    attempt,
                    retries,
                    len(data),
                )
                if self._use_aioboto3:
                    async with self._session.client("s3", **self._client_kwargs()) as client:
                        await client.put_object(Bucket=self.bucket, Key=dest_key, Body=data)
                    return dest_key
                if boto3 is None:
                    raise RuntimeError("boto3 is required when aioboto3 is not installed")

                def _sync():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.put_object(Bucket=self.bucket, Key=dest_key, Body=data)

                await asyncio.to_thread(_sync)
                return dest_key
            except Exception as e:
                logger.warning("S3 bytes upload failed (attempt %s/%s): %s", attempt, retries, e)
                if attempt == retries:
                    logger.exception("S3 bytes upload failed permanently for key=%s", dest_key)
                    raise
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                # Use deterministic jitter (based on attempt number) to avoid S311 insecure-random warning
                _jitter = (attempt * 9973) % 1000 / 1000  # deterministic fractional jitter
                await asyncio.sleep(backoff + _jitter)

    async def download_file(self, key: str, dest_path: str) -> bool:
        # Retry/backoff parameters
        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))

        for attempt in range(1, retries + 1):
            try:
                if self._use_aioboto3:
                    async with self._session.client("s3", **self._client_kwargs()) as client:
                        await client.download_file(self.bucket, key, dest_path)
                    await self._record_download_egress(dest_path)
                    return True

                if boto3 is None:
                    raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")

                def _sync_download():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.download_file(self.bucket, key, dest_path)

                await asyncio.to_thread(_sync_download)
                await self._record_download_egress(dest_path)
                return True

            except Exception as e:
                logger.warning("S3 download attempt %s/%s failed for key %s: %s", attempt, retries, key, e)
                if attempt == retries:
                    logger.exception("S3 download failed after %s attempts for key %s", retries, key)
                    raise
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                # Use deterministic jitter (based on attempt number) to avoid S311 insecure-random warning
                _jitter = (attempt * 9973) % 1000 / 1000  # deterministic fractional jitter
                await asyncio.sleep(backoff + _jitter)

    async def _record_download_egress(self, dest_path: str) -> None:
        """Count the bytes a download just pulled out of the bucket.

        The object is on local disk by this point, so its size *is* the egress -
        measured rather than inferred from the key.
        """
        try:
            await record_egress(os.path.getsize(dest_path))
        except Exception:
            logger.debug("egress: could not measure a download of %s", dest_path)

    async def _record_link_issued(self) -> None:
        """Count one presigned GET URL handed to a caller.

        Not bytes: the fetch happens outside this process. It is recorded so a
        cycle where delivery was handed to links is visible, since each fetch is
        one more full copy of that object as egress.
        """
        try:
            await record_egress(1, kind="link")
        except Exception:
            logger.debug("egress: could not count a presigned link")

    async def generate_presigned_post(self, key: str, expires: int | None = None) -> dict[str, Any]:
        expires = expires or config.PRESIGN_EXPIRES
        if self._use_aioboto3:
            async with self._session.client("s3", **self._client_kwargs()) as client:
                # generate_presigned_post is a local signing operation (no network)
                # but aioboto3 still wraps it as a coroutine — await it.
                post = await client.generate_presigned_post(Bucket=self.bucket, Key=key, ExpiresIn=expires)
                get_url = await client.generate_presigned_url(
                    "get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires * 24
                )
            return {"url": post["url"], "fields": post["fields"], "key": key, "get_url": get_url}

        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")

        def _sync_post():
            client = boto3.client("s3", **self._client_kwargs())
            post = client.generate_presigned_post(Bucket=self.bucket, Key=key, ExpiresIn=expires)
            get_url = client.generate_presigned_url(
                "get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires * 24
            )
            return {"url": post["url"], "fields": post["fields"], "key": key, "get_url": get_url}

        return await asyncio.to_thread(_sync_post)

    async def generate_presigned_get(self, key: str, expires: int | None = None) -> str:
        expires = expires or config.PRESIGN_EXPIRES
        if self._use_aioboto3:
            async with self._session.client("s3", **self._client_kwargs()) as client:
                url = await client.generate_presigned_url(
                    "get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires
                )
            await self._record_link_issued()
            return url

        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")

        def _sync_get():
            client = boto3.client("s3", **self._client_kwargs())
            return client.generate_presigned_url(
                "get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires
            )

        url = await asyncio.to_thread(_sync_get)
        await self._record_link_issued()
        return url

    async def delete(self, key: str) -> bool:
        # Retry/backoff for deletes, but do not raise to avoid unhandled
        # exceptions when deletes are scheduled as fire-and-forget.
        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))

        for attempt in range(1, retries + 1):
            try:
                if self._use_aioboto3:
                    async with self._session.client("s3", **self._client_kwargs()) as client:
                        await client.delete_object(Bucket=self.bucket, Key=key)
                    return True

                if boto3 is None:
                    logger.error("boto3 is required for S3 operations when aioboto3 is not installed")
                    return False

                def _sync_delete():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.delete_object(Bucket=self.bucket, Key=key)

                await asyncio.to_thread(_sync_delete)
                return True

            except Exception as e:
                logger.warning("S3 delete attempt %s/%s failed for key %s: %s", attempt, retries, key, e)
                if attempt == retries:
                    logger.exception("S3 delete failed after %s attempts for key %s", retries, key)
                    return False
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                # Use deterministic jitter (based on attempt number) to avoid S311 insecure-random warning
                _jitter = (attempt * 9973) % 1000 / 1000  # deterministic fractional jitter
                await asyncio.sleep(backoff + _jitter)

    async def exists(self, key: str) -> bool:
        # Use head_object on S3 to check existence with the same retry/backoff strategy
        if not key:
            return False

        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))
        for attempt in range(1, retries + 1):
            try:
                if self._use_aioboto3:
                    async with self._session.client("s3", **self._client_kwargs()) as client:
                        await client.head_object(Bucket=self.bucket, Key=key)
                    return True

                if boto3 is None:
                    raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")

                def _sync_head():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.head_object(Bucket=self.bucket, Key=key)

                await asyncio.to_thread(_sync_head)
                return True
            except Exception as e:
                logger.debug("S3 head_object attempt %s/%s failed for key %s: %s", attempt, retries, key, e)
                if attempt == retries:
                    return False
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                # Use deterministic jitter (based on attempt number) to avoid S311 insecure-random warning
                _jitter = (attempt * 9973) % 1000 / 1000  # deterministic fractional jitter
                await asyncio.sleep(backoff + _jitter)

    # ─────────────────────────────────────────────────────────────────────────
    # Bulk key listing & deletion (used by periodic S3 cleanup tasks)
    # ─────────────────────────────────────────────────────────────────────────

    async def usage(self, *, max_objects: int = 5000, group_depth: int = 1) -> dict[str, Any]:
        """Count objects and bytes in the bucket, bounded by *max_objects*.

        Uses the shared paginator (1000 keys per request) and stops as soon as
        the cap is reached, so a large bucket costs a few requests, not a full
        walk.
        """
        cap = max(1, int(max_objects))
        depth = max(1, int(group_depth))
        tally = {"objects": 0, "bytes": 0, "truncated": False}
        groups: dict[str, dict[str, int]] = {}
        kwargs: dict[str, Any] = {"Bucket": self.bucket}

        def _accumulate(contents) -> bool:
            """Fold a page in; return True once the cap is reached."""
            for obj in contents or []:
                size = int(obj.get("Size") or 0)
                tally["objects"] += 1
                tally["bytes"] += size
                _usage_add(groups, obj.get("Key") or "", size, depth)
                if tally["objects"] >= cap:
                    tally["truncated"] = True
                    return True
            return False

        if self._use_aioboto3:
            async with self._session.client("s3", **self._client_kwargs()) as client:
                paginator = client.get_paginator("list_objects_v2")
                async for page in paginator.paginate(**kwargs):
                    if _accumulate(page.get("Contents")):
                        break
        else:
            if boto3 is None:
                raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")

            def _sync_usage():
                client = boto3.client("s3", **self._client_kwargs())
                paginator = client.get_paginator("list_objects_v2")
                for page in paginator.paginate(**kwargs):
                    if _accumulate(page.get("Contents")):
                        break

            await asyncio.to_thread(_sync_usage)

        return {
            "backend": "s3",
            "location": self.bucket,
            "objects": tally["objects"],
            "bytes": tally["bytes"],
            "truncated": tally["truncated"],
            "groups": groups,
        }

    async def list_keys(self, prefix: str = "") -> list[dict[str, Any]]:
        """List S3 keys under *prefix*, with pagination.

        Returns a list of dicts with keys:
            - "key": the full S3 object key
            - "last_modified": UNIX timestamp (float)
            - "size": object size in bytes
        """
        keys: list[dict[str, Any]] = []
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix}

        if self._use_aioboto3:
            async with self._session.client("s3", **self._client_kwargs()) as client:
                paginator = client.get_paginator("list_objects_v2")
                async for page in paginator.paginate(**kwargs):
                    for obj in page.get("Contents", []):
                        keys.append(
                            {
                                "key": obj["Key"],
                                "last_modified": obj["LastModified"].timestamp(),
                                "size": obj["Size"],
                            }
                        )
        else:
            if boto3 is None:
                raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")

            def _sync_list():
                client = boto3.client("s3", **self._client_kwargs())
                paginator = client.get_paginator("list_objects_v2")
                out: list[dict[str, Any]] = []
                for page in paginator.paginate(**kwargs):
                    for obj in page.get("Contents", []):
                        out.append(
                            {
                                "key": obj["Key"],
                                "last_modified": obj["LastModified"].timestamp(),
                                "size": obj["Size"],
                            }
                        )
                return out

            keys = await asyncio.to_thread(_sync_list)

        return keys

    async def delete_keys(self, keys: list[str]) -> int:
        """Bulk-delete S3 keys using delete_objects (batching up to 1000)."""
        if not keys:
            return 0

        deleted = 0
        batch_size = 1000

        for i in range(0, len(keys), batch_size):
            batch = keys[i : i + batch_size]
            delete_dict = {"Objects": [{"Key": k} for k in batch]}

            if self._use_aioboto3:
                async with self._session.client("s3", **self._client_kwargs()) as client:
                    resp = await client.delete_objects(Bucket=self.bucket, Delete=delete_dict)
                    _batch_deleted = len(resp.get("Deleted", []))
                    _errors = resp.get("Errors", [])
                    if _errors:
                        logger.warning(
                            "S3 bulk delete: %d errors in batch: %s",
                            len(_errors),
                            _errors[:3],
                        )
                    deleted += _batch_deleted
            else:
                if boto3 is None:
                    raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")

                def _sync_delete(delete_dict=delete_dict):
                    client = boto3.client("s3", **self._client_kwargs())
                    resp = client.delete_objects(Bucket=self.bucket, Delete=delete_dict)
                    _batch_del = len(resp.get("Deleted", []))
                    _errs = resp.get("Errors", [])
                    if _errs:
                        logger.warning(
                            "S3 bulk delete: %d errors in batch: %s",
                            len(_errs),
                            _errs[:3],
                        )
                    return _batch_del

                deleted += await asyncio.to_thread(_sync_delete)

        logger.info("S3 bulk delete: requested=%d succeeded=%d/%d", len(keys), deleted, len(keys))
        return deleted


_STORAGE_SINGLETON: AsyncStorageBackend | None = None


async def get_storage_backend() -> AsyncStorageBackend:
    """Return a shared AsyncStorageBackend instance based on configuration.

    This factory chooses between `local` and `s3`/`r2` backends depending on
    `config.STORAGE_BACKEND`. The result is cached for the lifetime of the
    process.
    """
    global _STORAGE_SINGLETON
    if _STORAGE_SINGLETON is not None:
        return _STORAGE_SINGLETON

    backend = (os.getenv("STORAGE_BACKEND") or config.STORAGE_BACKEND or "local").lower()
    if backend in ("s3", "r2"):
        _STORAGE_SINGLETON = S3AsyncBackend(
            bucket=config.S3_BUCKET,
            endpoint_url=(os.getenv("S3_ENDPOINT") or config.S3_ENDPOINT or None),
            region=(os.getenv("S3_REGION") or config.S3_REGION or None),
            aws_access_key_id=(os.getenv("AWS_ACCESS_KEY_ID") or config.AWS_ACCESS_KEY_ID or None),
            aws_secret_access_key=(os.getenv("AWS_SECRET_ACCESS_KEY") or config.AWS_SECRET_ACCESS_KEY or None),
            use_ssl=config.S3_USE_SSL,
        )
    else:
        _STORAGE_SINGLETON = LocalStorageBackend(base_path=(os.getenv("STORAGE_PATH") or config.STORAGE_PATH))

    return _STORAGE_SINGLETON


def get_storage_backend_sync() -> AsyncStorageBackend:
    """Synchronous convenience wrapper to obtain a backend without awaiting.

    Note: callers should prefer `await get_storage_backend()` where possible.
    This helper will create the same singleton but will raise if S3 backend
    requires `aioboto3` and it's not installed.
    """
    global _STORAGE_SINGLETON
    if _STORAGE_SINGLETON is not None:
        return _STORAGE_SINGLETON

    backend = (os.getenv("STORAGE_BACKEND") or config.STORAGE_BACKEND or "local").lower()
    if backend in ("s3", "r2"):
        # create synchronously (may raise if aioboto3 missing)
        _STORAGE_SINGLETON = S3AsyncBackend(
            bucket=config.S3_BUCKET,
            endpoint_url=config.S3_ENDPOINT or None,
            region=config.S3_REGION or None,
            aws_access_key_id=config.AWS_ACCESS_KEY_ID or None,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY or None,
            use_ssl=config.S3_USE_SSL,
        )
    else:
        _STORAGE_SINGLETON = LocalStorageBackend(base_path=(os.getenv("STORAGE_PATH") or config.STORAGE_PATH))

    return _STORAGE_SINGLETON
