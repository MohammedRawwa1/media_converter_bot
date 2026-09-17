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
import contextlib
import logging
import os
import shutil
import tempfile
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


# ── Streaming uploads ───────────────────────────────────────────────────────
#
# Until this existed, the only way bytes could reach the bucket was from a path
# (a file that had finished downloading to local disk) or from a complete
# in-memory buffer. That made a full local copy of every media unavoidable, and
# put S3 in the position of a mirror written *after* the fact rather than the
# place the source lives. A sink is the other half: a writable target a producer
# can hand its chunks to as they arrive.
#
# The sink interface is shaped by what the producers actually call:
# Telethon's ``download_media(file=<sink>)`` writes with ``f.write(chunk)``,
# awaits the result **if it is awaitable**, calls ``f.tell()`` for its progress
# callback and ``f.flush()`` at the end. So ``write()`` returning a coroutine is
# how back-pressure is applied without blocking the event loop, and ``tell()``
# has to report the bytes written so far.


class UploadSink(ABC):
    """A writable target for bytes produced elsewhere (e.g. an MTProto download)."""

    #: The storage key this sink completes to.
    key: str

    @abstractmethod
    async def open(self) -> None:
        """Prepare the target (create the multipart upload, open the temp file)."""

    @abstractmethod
    def write(self, chunk: bytes) -> Any | None:
        """Accept a chunk. May return an awaitable the caller should await."""

    @abstractmethod
    def tell(self) -> int:
        """Bytes accepted so far (Telethon's progress callback reads this)."""

    def flush(self) -> None:  # noqa: B027 - an optional hook, not a required override
        """No-op hook: Telethon calls this when it is done writing."""

    @abstractmethod
    async def close(self) -> str:
        """Finish the upload and return the completed key."""

    @abstractmethod
    async def abort(self) -> None:
        """Discard everything written; never leaves a partial object behind."""

    async def awrite(self, chunk: bytes) -> None:
        """Accept a chunk from async code, applying back-pressure."""
        pending = self.write(chunk)
        if pending is not None:
            await pending


class BufferedUploadSink(UploadSink):
    """Fallback sink: write to a temp file, upload it whole on close.

    Every backend gets streaming *support* this way - the bytes are staged on
    disk locally instead of in RAM - while backends that can do better (S3, via
    :class:`S3UploadSink`) override :meth:`AsyncStorageBackend.open_upload_sink`.
    """

    def __init__(self, backend: AsyncStorageBackend, key: str, suffix: str = ""):
        self._backend = backend
        self.key = key
        self._suffix = suffix
        self._path: str | None = None
        self._written = 0
        self._closed = False

    async def open(self) -> None:
        if self._path is None:
            fd, path = tempfile.mkstemp(prefix="uploadsink_", suffix=self._suffix or ".part")
            os.close(fd)
            self._path = path

    def write(self, chunk: bytes) -> Any | None:
        if not chunk:
            return None
        if self._path is None:
            raise RuntimeError("UploadSink.open() must be awaited before writing")
        with open(self._path, "ab") as fh:
            fh.write(chunk)
        self._written += len(chunk)
        return None

    def tell(self) -> int:
        return self._written

    async def close(self) -> str:
        if self._closed:
            return self.key
        if self._path is None or not os.path.exists(self._path):
            raise RuntimeError(f"nothing was written for {self.key}")
        if self._written <= 0:
            raise RuntimeError(f"nothing was written for {self.key}")
        path = self._path
        try:
            await self._backend.upload_file(path, self.key)
        finally:
            with contextlib.suppress(OSError):
                os.remove(path)
            self._closed = True
        return self.key

    async def abort(self) -> None:
        if self._path:
            with contextlib.suppress(OSError):
                os.remove(self._path)
        self._closed = True


class HeadCaptureSink(UploadSink):
    """Wrap a sink and keep the first ``limit`` bytes for a local probe.

    Streaming a source straight into storage means there is no local file left
    to ffprobe. ffprobe only needs the container header for duration/codec
    questions on the common formats, so this tap keeps exactly that much in
    memory while still forwarding every byte to the wrapped sink. The captured
    head is what the ingest writes to disk as the *probe* copy - it is never
    treated as the media itself.
    """

    def __init__(self, sink: UploadSink, limit: int):
        self._sink = sink
        self.key = sink.key
        self._limit = max(0, int(limit))
        self._head = bytearray()

    @property
    def head(self) -> bytes:
        """The first ``limit`` bytes that passed through, in order."""
        return bytes(self._head)

    @property
    def wrapped(self) -> UploadSink:
        """The sink the bytes actually go to."""
        return self._sink

    async def open(self) -> None:
        await self._sink.open()

    def write(self, chunk: bytes) -> Any | None:
        if chunk and len(self._head) < self._limit:
            self._head += chunk[: self._limit - len(self._head)]
        return self._sink.write(chunk)

    def tell(self) -> int:
        return self._sink.tell()

    def flush(self) -> None:
        self._sink.flush()

    async def close(self) -> str:
        return await self._sink.close()

    async def abort(self) -> None:
        await self._sink.abort()


class _BlockingS3Client:
    """Run a synchronous boto3 client's calls in worker threads.

    Lets one multipart sink implementation serve both the aioboto3 and the
    boto3-only deployments instead of maintaining two.
    """

    def __init__(self, client):
        self._client = client

    def __getattr__(self, name):
        target = getattr(self._client, name)

        async def _call(**kwargs):
            return await asyncio.to_thread(target, **kwargs)

        return _call


class S3UploadSink(UploadSink):
    """Multipart-upload sink: Telegram chunks land in S3 as they arrive.

    An S3 multipart upload is opened on the first write, each filled part is
    uploaded in parallel (up to ``S3_UPLOAD_PARTS_IN_FLIGHT``) with the same
    retry/backoff knobs as every other S3 operation here, and the object is
    completed on :meth:`close`. Bytes held in memory are bounded by
    ``max_inflight_bytes``: once that much is queued, :meth:`write` returns an
    awaitable that the caller awaits before producing more, so a download that
    outruns the bucket throttles the download instead of exhausting the box.

    On any failure the multipart upload is aborted, so a failed download leaves
    no half-object behind for a later job to pick up as if it were complete.
    """

    #: S3's minimum size for every part except the last.
    MIN_PART_BYTES = 5 * 1024 * 1024
    #: boto3's own default threshold, so behaviour matches ``upload_file``.
    DEFAULT_PART_BYTES = 8 * 1024 * 1024

    def __init__(
        self,
        backend: S3AsyncBackend,
        key: str,
        *,
        part_size: int | None = None,
        content_type: str | None = None,
        max_inflight_bytes: int | None = None,
    ):
        self._backend = backend
        self.key = key
        self._part_size = max(self.MIN_PART_BYTES, int(part_size or S3UploadSink.DEFAULT_PART_BYTES))
        self._content_type = content_type
        self._max_inflight = int(
            max_inflight_bytes
            if max_inflight_bytes is not None
            else _env_int("S3_UPLOAD_MAX_BUFFERED_MB", 256) * 1024 * 1024
        )
        self._parallel = max(1, _env_int("S3_UPLOAD_PARTS_IN_FLIGHT", 4))
        self._buffer = bytearray()
        self._written = 0
        self._inflight = 0
        self._upload_id: str | None = None
        self._parts: dict[int, str] = {}
        self._pending: set[asyncio.Task] = set()
        self._error: BaseException | None = None
        self._client_ctx = None
        self._client = None
        self._sem: asyncio.Semaphore | None = None
        self._upload_lock: asyncio.Lock | None = None
        self._closed = False
        # Part numbers are claimed in ``write``/``close``, which only ever run on
        # the event loop thread, so a plain counter cannot hand the same number
        # to two parts.
        self._next_part = 1

    # ── lifecycle ────────────────────────────────────────────────────────

    async def open(self) -> None:
        if self._client is not None:
            return
        self._sem = asyncio.Semaphore(self._parallel)
        self._client_ctx = self._backend._multipart_client()
        self._client = await self._client_ctx.__aenter__()

    async def _ensure_upload(self) -> str:
        """Open the multipart upload once, no matter how many parts race for it.

        Parts upload concurrently, and every one of them calls this before its
        ``upload_part``. Without the lock each racer saw ``_upload_id is None``
        while awaiting and opened its *own* multipart upload, so a single sink
        scattered its parts across several uploads and only the last one could
        ever be completed - the rest were left behind. The double check inside
        the lock keeps it to exactly one create per sink.
        """
        if self._upload_id:
            return self._upload_id
        if self._upload_lock is None:
            self._upload_lock = asyncio.Lock()
        async with self._upload_lock:
            if self._upload_id:
                return self._upload_id
            await self.open()
            kwargs: dict[str, Any] = {"Bucket": self._backend.bucket, "Key": self.key}
            if self._content_type:
                kwargs["ContentType"] = self._content_type
            resp = await self._client.create_multipart_upload(**kwargs)
            self._upload_id = resp["UploadId"]
            logger.info(
                "s3 sink: multipart upload opened for %s (part=%dMB)", self.key, self._part_size // (1024 * 1024)
            )
            return self._upload_id

    def write(self, chunk: bytes) -> Any | None:
        """Buffer a chunk; return an awaitable when the caller must slow down."""
        if not chunk:
            return None
        self._buffer += chunk
        self._written += len(chunk)
        while len(self._buffer) >= self._part_size:
            self._submit_part(bytes(self._buffer[: self._part_size]))
            del self._buffer[: self._part_size]
        if self._inflight >= self._max_inflight:
            return self._wait_for_capacity()
        return None

    async def _wait_for_capacity(self) -> None:
        while self._inflight >= self._max_inflight and self._pending:
            await asyncio.sleep(0.05)
        if self._error is not None:
            raise self._error

    def tell(self) -> int:
        return self._written

    def _submit_part(self, data: bytes) -> None:
        """Queue one part; the upload runs as its own task."""
        self._inflight += len(data)
        part_number = self._next_part
        self._next_part += 1
        task = asyncio.get_running_loop().create_task(self._upload_part(part_number, data))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def _upload_part(self, part_number: int, data: bytes) -> None:
        try:
            async with self._sem:
                upload_id = await self._ensure_upload()
                resp = await self._retry(
                    lambda: self._client.upload_part(
                        Bucket=self._backend.bucket,
                        Key=self.key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=data,
                    ),
                    what=f"upload_part {part_number}",
                )
                self._parts[part_number] = resp["ETag"]
                logger.debug("s3 sink: part %d uploaded (%dMB)", part_number, len(data) // (1024 * 1024))
        except BaseException as exc:  # noqa: BLE001 - recorded and re-raised at close
            self._error = exc
            logger.warning("s3 sink: part %d failed for %s: %s", part_number, self.key, exc)
        finally:
            self._inflight = max(0, self._inflight - len(data))

    async def _retry(self, call, *, what: str):
        retries = _env_int("S3_OP_RETRIES", 3)
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1") or 1)
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60") or 60)
        last: BaseException | None = None
        for attempt in range(1, retries + 1):
            try:
                return await call()
            except Exception as exc:  # noqa: PERF203 - the retry loop is the point
                last = exc
                if attempt == retries:
                    break
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                logger.warning(
                    "s3 sink: %s attempt %d/%d failed (%s); retrying in %.1fs", what, attempt, retries, exc, backoff
                )
                await asyncio.sleep(backoff)
        raise last if last is not None else RuntimeError(f"{what} failed")

    async def close(self) -> str:
        if self._closed:
            return self.key
        try:
            if self._buffer:
                self._submit_part(bytes(self._buffer))
                self._buffer.clear()
            if self._pending:
                await asyncio.gather(*list(self._pending), return_exceptions=True)
            if self._error is not None:
                raise self._error
            if not self._parts:
                raise RuntimeError(f"nothing was written for {self.key}")
            upload_id = await self._ensure_upload()
            await self._retry(
                lambda: self._client.complete_multipart_upload(
                    Bucket=self._backend.bucket,
                    Key=self.key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": [{"PartNumber": n, "ETag": e} for n, e in sorted(self._parts.items())]},
                ),
                what="complete_multipart_upload",
            )
            self._closed = True
            logger.info(
                "s3 sink: completed %s (%dMB in %d part(s))",
                self.key,
                self._written // (1024 * 1024),
                len(self._parts),
            )
            return self.key
        except BaseException:
            await self.abort()
            raise
        finally:
            await self._release_client()

    async def abort(self) -> None:
        for task in list(self._pending):
            task.cancel()
        if self._pending:
            with contextlib.suppress(Exception):
                await asyncio.gather(*list(self._pending), return_exceptions=True)
        if self._upload_id and self._client is not None:
            with contextlib.suppress(Exception):
                await self._client.abort_multipart_upload(
                    Bucket=self._backend.bucket,
                    Key=self.key,
                    UploadId=self._upload_id,
                )
                logger.info("s3 sink: aborted the upload of %s; nothing was left behind", self.key)
        self._closed = True
        await self._release_client()

    async def _release_client(self) -> None:
        ctx, self._client_ctx, self._client = self._client_ctx, None, None
        if ctx is not None:
            with contextlib.suppress(Exception):
                await ctx.__aexit__(None, None, None)


class AsyncStorageBackend(ABC):
    @abstractmethod
    async def upload_file(self, src_path: str, dest_key: str) -> str:
        """Upload a local file at `src_path` to storage and return the storage key or path."""

    @abstractmethod
    async def download_file(self, key: str, dest_path: str) -> bool:
        """Download a storage object `key` to local `dest_path`. Return True on success."""

    async def download_range(self, key: str, dest_path: str, end: int = 2_097_151) -> bool:
        """Download only the first ``end + 1`` bytes of ``key`` to ``dest_path``.

        Used for Range-probe pre-flights: inspect container headers with
        ffprobe without pulling the full object.  Returns ``True`` on
        success, ``False`` when the backend does not support range GETs.
        """
        return False

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

    async def get_file_size(self, key: str) -> int | None:
        """Return the object size in bytes via a HEAD request, or ``None``.

        The default implementation falls back to ``None``; backends that
        support ``head_object`` (S3, IDrive e2) override this to return
        the real ``ContentLength`` without downloading the object.
        """
        return None

    async def open_upload_sink(
        self,
        key: str,
        *,
        part_size: int | None = None,
        content_type: str | None = None,
        max_inflight_bytes: int | None = None,
    ) -> UploadSink:
        """Open a writable target that uploads to *key* as bytes arrive.

        The default stages the stream in a local temp file and uploads it whole
        on close, which is correct everywhere and good enough for a local path.
        S3 overrides this with a real multipart sink so a producer (an MTProto
        download, a URL fetch) never needs a full local copy first.

        Callers must ``await sink.open()`` before writing and either
        ``await sink.close()`` or ``await sink.abort()`` when finished.
        """
        return BufferedUploadSink(self, key)

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


async def stored_object_is_intact(
    backend: AsyncStorageBackend, key: str | None, *, expected_size: int | None = None
) -> bool:
    """Cache evidence: is *key* actually in storage and the media we expect?

    The two questions a reuse decision has to answer before it can skip a
    Telegram download, and both are answered from metadata - a HEAD, never a
    body read - so validating a cached object costs no egress:

    * does the object still exist (a lifecycle rule or a sweep may have taken it),
    * and when the expected size is known, is it the *same* media (a stored size
      that disagrees is a different file that merely shared an id).

    Deliberately forgiving: a backend that cannot report a size, or one that
    errors on the probe, is treated as intact. The consequence of being wrong
    here is one download; the consequence of a false negative is exactly the
    re-download this gate exists to prevent, so the unknown must not read as a
    miss.
    """
    if not key:
        return False
    try:
        if not await backend.exists(key):
            return False
    except Exception:
        return True
    if not expected_size:
        return True
    try:
        stored_size = await backend.get_file_size(key)
    except Exception:
        stored_size = None
    if stored_size is None:
        return True
    try:
        return int(stored_size) == int(expected_size)
    except (TypeError, ValueError):
        return True


def _env_int(name: str, default: int) -> int:
    """Read an int env var, tolerating unset/empty/garbage values."""
    try:
        return int(float(str(os.getenv(name) or "").strip()))
    except (TypeError, ValueError):
        return default


def _usage_group(key: str, depth: int) -> str:
    """The group a key's bytes are attributed to (``uploads/``, ``(root)``)."""
    parts = str(key).split("/")
    if len(parts) <= 1:
        return "(root)"
    return "/".join(parts[:depth]) + "/"


def _usage_add(groups: dict[str, dict[str, int]], key: str, size: int, depth: int) -> None:
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
    if EGRESS_WARN_STEP_BYTES > 0 and (total // EGRESS_WARN_STEP_BYTES) != ((total - nbytes) // EGRESS_WARN_STEP_BYTES):
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

    async def open_upload_sink(
        self,
        key: str,
        *,
        part_size: int | None = None,
        content_type: str | None = None,
        max_inflight_bytes: int | None = None,
    ) -> UploadSink:
        """A multipart sink, so bytes reach the bucket while they are produced."""
        if not key:
            raise ValueError("key must not be empty")
        if not self.bucket:
            raise ValueError(f"Invalid S3 bucket name: {self.bucket}")
        return S3UploadSink(
            self,
            key,
            part_size=part_size,
            content_type=content_type,
            max_inflight_bytes=max_inflight_bytes,
        )

    @contextlib.asynccontextmanager
    async def _multipart_client(self):
        """An S3 client for a long-lived multipart upload.

        aioboto3 clients are async context managers, so one is held open for the
        whole upload rather than per part; the boto3-only path is wrapped so its
        blocking calls run in a thread.
        """
        if self._use_aioboto3:
            async with self._session.client("s3", **self._client_kwargs()) as client:
                yield client
            return
        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")
        yield _BlockingS3Client(boto3.client("s3", **self._client_kwargs()))

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

    async def download_range(self, key: str, dest_path: str, end: int = 2_097_151) -> bool:
        """Download only bytes 0–*end* of *key* (a Range GET).

        The first 2 MB is enough for every common container header (MP4
        ``moov``, MKV ``SegmentInfo``, AVI ``RIFF`` header).  Returns True
        on success; the caller runs ffprobe on the slice.
        """
        retries = int(os.getenv("S3_OP_RETRIES", "2"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "30"))

        for attempt in range(1, retries + 1):
            try:
                range_header = f"bytes=0-{end}"
                if self._use_aioboto3:
                    async with self._session.client("s3", **self._client_kwargs()) as client:
                        resp = await client.get_object(
                            Bucket=self.bucket,
                            Key=key,
                            Range=range_header,
                        )
                        body = await resp["Body"].read()
                    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
                    with open(dest_path, "wb") as fh:
                        fh.write(body)
                    return True

                if boto3 is None:
                    raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")

                # ``range_header`` is bound as a default so the thread reads this
                # attempt's range rather than whatever the next iteration set.
                def _sync_range(range_header=range_header):
                    client = boto3.client("s3", **self._client_kwargs())
                    resp = client.get_object(Bucket=self.bucket, Key=key, Range=range_header)
                    return resp["Body"].read()

                body = await asyncio.to_thread(_sync_range)
                os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
                with open(dest_path, "wb") as fh:
                    fh.write(body)
                return True

            except Exception as e:
                logger.debug("S3 range-GET attempt %s/%s failed for key %s: %s", attempt, retries, key, e)
                if attempt == retries:
                    return False
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                _jitter = (attempt * 9973) % 1000 / 1000
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

    async def get_file_size(self, key: str) -> int | None:
        """Return the object size in bytes via ``head_object``, or ``None``.

        One HEAD request replaces a full download when only the size is
        needed (e.g. timeout scaling, egress budget checks).
        """
        if not key:
            return None

        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))
        for attempt in range(1, retries + 1):
            try:
                if self._use_aioboto3:
                    async with self._session.client("s3", **self._client_kwargs()) as client:
                        resp = await client.head_object(Bucket=self.bucket, Key=key)
                    return resp.get("ContentLength")

                if boto3 is None:
                    raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")

                def _sync_head_size():
                    client = boto3.client("s3", **self._client_kwargs())
                    resp = client.head_object(Bucket=self.bucket, Key=key)
                    return resp.get("ContentLength")

                return await asyncio.to_thread(_sync_head_size)
            except Exception as e:
                logger.debug("S3 head_object size attempt %s/%s failed for key %s: %s", attempt, retries, key, e)
                if attempt == retries:
                    return None
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                _jitter = (attempt * 9973) % 1000 / 1000
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
