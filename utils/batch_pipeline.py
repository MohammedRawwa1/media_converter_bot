"""Sequential, memory-safe execution for bulk ("Apply Bulk") batches.

A bulk menu apply queues every collected file into Redis, which is where the
backlog belongs - it must not be 30 file handles and 30 ffmpeg buffers in RAM.
This module makes the other half of that bargain explicit:

* **One job at a time, per batch.** Every job of an apply carries the same
  ``batch_id``, and a worker must hold that batch's Redis lock before it runs
  one. With more than one worker replica this is what keeps a 30-file batch on
  a single file at a time instead of letting replicas chew through it in
  parallel - which is exactly how N concurrent ffmpeg processes compound a
  memory spike into an OOM. A worker that cannot take the lock defers its job
  (Redis-backed, see :data:`utils.job_queue.DELAYED_SET`) and picks up other
  work, so nothing is lost and nothing overlaps.

* **Finish -> clean -> next.** :func:`finalize_job` releases the batch lock and
  then forces the process back to a clean slate: drop the probe caches, sweep
  leftover temp artifacts, ``gc.collect()`` and (on glibc) ``malloc_trim(0)``
  so freed heap pages actually go back to the OS rather than sitting in the
  allocator's free lists until the container is killed.

The module is deliberately dependency-light (stdlib + the existing job queue)
so both the worker and tests can import it without pulling in ffmpeg.
"""

import contextlib
import gc
import json
import logging
import os
import tempfile
import time
import uuid

logger = logging.getLogger(__name__)


def _env_number(name: str, default):
    """Read a numeric env var, tolerating an unset *or empty* value.

    ``int(os.getenv("X", default))`` raises on ``X=`` (common in ``.env`` files),
    which would take the whole worker down at import time. Falls back to the
    default and warns instead.
    """
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return type(default)(str(raw).strip())
    except (TypeError, ValueError):
        logger.warning("batch_pipeline: invalid %s=%r, using %r", name, raw, default)
        return default


# ---------------------------------------------------------------------------
# Batch identity
# ---------------------------------------------------------------------------

# Job payload fields written by the bulk menu and read by the worker. Kept as
# module constants so the handler, the worker and the tests cannot drift.
BATCH_ID_FIELD = "batch_id"
BATCH_SEQ_FIELD = "batch_seq"
BATCH_TOTAL_FIELD = "batch_total"

# The bulk menu caps a batch at 30 entries; this is the same ceiling used when
# sanity-checking a total that arrives off the queue.
BATCH_MAX_JOBS = 30

# ---------------------------------------------------------------------------
# Per-batch lock (one job of a batch at a time, across replicas)
# ---------------------------------------------------------------------------

# Namespace shared by everything keyed on a batch: the lock (`<id>`) and the
# finished-file counter (`<id>:done`).
BATCH_KEY_PREFIX = "ffmpeg:batch:"
# How long a worker may hold a batch's lock without refreshing it. Defaults to
# the worker's own maximum job runtime (JOB_MAX_SECONDS, 6h) so a legitimately
# slow 700 MB+ conversion can never outlive its lock and let a second replica
# start the next file of the same batch. The lock is released explicitly after
# every job; the TTL only matters if a worker dies mid-job, and then it is what
# stops the batch from wedging forever.
BATCH_LOCK_TTL_SECONDS = _env_number("BATCH_LOCK_TTL_SECONDS", _env_number("JOB_MAX_SECONDS", 6 * 3600))
# How long a job waits before it is offered to the queue again when its batch
# is busy. Short enough to keep a batch moving, long enough not to spin.
BATCH_DEFER_SECONDS = _env_number("BATCH_DEFER_SECONDS", 10.0)

# Compare-and-delete so a worker can only release a lock it still owns. Mirrors
# the input-lock release in utils.job_queue.
_RELEASE_LOCK_SCRIPT = """
local current = redis.call('get', KEYS[1])
if current == ARGV[1] then
    redis.call('del', KEYS[1])
    return 1
end
return 0
"""


def new_batch_id() -> str:
    """Opaque identity shared by every job of one Apply Bulk run."""
    return uuid.uuid4().hex


def batch_lock_key(batch_id) -> str:
    """Redis key holding the lock for one bulk batch."""
    return f"{BATCH_KEY_PREFIX}{batch_id}"


def batch_progress_key(batch_id) -> str:
    """Redis key counting the finished files of one bulk batch."""
    return f"{BATCH_KEY_PREFIX}{batch_id}:done"


def batch_total_key(batch_id) -> str:
    """Redis key holding how many jobs a batch actually enqueued."""
    return f"{BATCH_KEY_PREFIX}{batch_id}:total"


async def set_batch_total(redis=None, *, batch_id, total) -> bool:
    """Record the number of jobs an apply really enqueued.

    The total on the job payload is the number of *collected* files, and a few
    can be skipped at enqueue time (unfetchable, or a photo an audio-only plan
    cannot use). Without this exact count a batch whose last file was skipped
    would never look finished, and its single progress message would be left in
    the chat. The worker prefers this figure when it is present.
    """
    try:
        if redis is None:
            from utils.job_queue import get_redis

            redis = await get_redis()
        await redis.set(
            batch_total_key(batch_id), int(total), ex=max(1, int(BATCH_LOCK_TTL_SECONDS))
        )
        return True
    except Exception:
        logger.debug("batch_pipeline: could not record batch total for %s", batch_id)
        return False


def batch_message_key(batch_id) -> str:
    """Redis key holding ``chat_id:message_id`` of a batch's progress message.

    A batch shows exactly one progress message, edited in place and deleted when
    the batch ends. Keeping its location in Redis is what lets a worker that
    restarts mid-batch carry on editing the same message instead of posting a
    second one.
    """
    return f"{BATCH_KEY_PREFIX}{batch_id}:msg"


def tag_batch_job(job: dict, batch_id, seq: int = 0, total: int = 0) -> dict:
    """Stamp a job as a member of ``batch_id`` and return it.

    ``seq`` is the position in the apply (0-based) and ``total`` the batch size,
    both used for progress reporting; neither affects ordering, which the lock
    enforces by simply allowing one job of the batch to run at a time.
    """
    job[BATCH_ID_FIELD] = str(batch_id)
    job[BATCH_SEQ_FIELD] = int(seq)
    job[BATCH_TOTAL_FIELD] = int(total)
    return job


def job_batch_id(job: dict | None):
    """The ``batch_id`` on a job payload, or ``None`` for a non-batch job."""
    if not job:
        return None
    value = job.get(BATCH_ID_FIELD)
    return str(value) if value else None


async def try_acquire_batch_lock(redis, batch_id, job_id) -> bool:
    """Take the batch lock if it is free.

    Returns ``True`` when this worker now owns the batch and may run the job.
    Returns ``False`` when another job of the same batch is already running.
    On a Redis error the call **fails open** (returns ``True``): a queue
    hiccup should cost us a serialization guarantee for one job, never stall a
    user's batch indefinitely.
    """
    key = batch_lock_key(batch_id)
    owner = str(job_id or "")
    try:
        acquired = await redis.set(
            key, owner, nx=True, px=int(BATCH_LOCK_TTL_SECONDS * 1000)
        )
        return bool(acquired)
    except Exception:
        logger.warning(
            "batch_pipeline: could not acquire lock for batch %s (running anyway)", batch_id
        )
        return True


async def release_batch_lock(redis, batch_id, job_id) -> bool:
    """Release the batch lock, but only if this job still owns it."""
    key = batch_lock_key(batch_id)
    owner = str(job_id or "")
    try:
        result = await redis.eval(_RELEASE_LOCK_SCRIPT, 1, key, owner)
        return bool(result)
    except Exception:
        logger.debug("batch_pipeline: failed to release lock %s", key)
        return False


async def defer_batch_job(redis, job: dict, delay: float | None = None) -> bool:
    """Put a job back for later, in Redis, so a busy batch never blocks the worker.

    Uses the existing delayed-job zset that :func:`utils.job_queue.pop_job`
    promotes when due, so a deferred batch job re-enters the normal queue path.
    """
    if not job:
        return False
    delay = BATCH_DEFER_SECONDS if delay is None else float(delay)
    try:
        from utils.job_queue import DELAYED_SET

        await redis.zadd(DELAYED_SET, {json.dumps(job): time.time() + delay})
        return True
    except Exception:
        logger.debug("batch_pipeline: failed to defer job %s", job.get("job_id"))
        return False


# ---------------------------------------------------------------------------
# Global ffmpeg slot: at most MAX_CONCURRENT_FFMPEG conversions, anywhere
# ---------------------------------------------------------------------------

# Slot keys are claimed with SET NX PX and released with a compare-and-delete,
# exactly like the batch lock, so a worker that dies mid-conversion cannot leak
# a slot permanently - its key simply expires. Because the slots live in Redis
# and not in a process, the guarantee holds across replicas *and* across
# services: the bot hosts a background worker too, and both draw from this pool.
FFMPEG_SLOT_PREFIX = "ffmpeg:slot:"
# 1 means "one ffmpeg process ever", which is the point on a 1 GB box.
MAX_CONCURRENT_FFMPEG = max(1, _env_number("MAX_CONCURRENT_FFMPEG", 1))
FFMPEG_SLOT_TTL_SECONDS = BATCH_LOCK_TTL_SECONDS


def ffmpeg_slot_key(index: int) -> str:
    """Redis key for one conversion slot."""
    return f"{FFMPEG_SLOT_PREFIX}{int(index)}"


async def acquire_ffmpeg_slot(redis, job_id, *, slots=None, ttl_seconds=None):
    """Claim one global conversion slot.

    Returns the slot index (>= 0) when a slot was taken, ``None`` when every
    slot is busy (the caller should defer the job), or ``-1`` when Redis errored
    so no slot could be checked - in that case the caller runs the job rather
    than stall the queue, and release is a no-op.
    """
    total = MAX_CONCURRENT_FFMPEG if slots is None else max(1, int(slots))
    ttl_ms = int((FFMPEG_SLOT_TTL_SECONDS if ttl_seconds is None else ttl_seconds) * 1000)
    owner = str(job_id or "")
    for index in range(total):
        try:
            if await redis.set(ffmpeg_slot_key(index), owner, nx=True, px=ttl_ms):
                return index
        except Exception:
            logger.warning("batch_pipeline: ffmpeg slot check failed; running job without a slot")
            return -1
    return None


async def release_ffmpeg_slot(redis, index, job_id) -> bool:
    """Release a conversion slot, but only if this job still owns it."""
    try:
        index = int(index)
    except (TypeError, ValueError):
        return False
    if index < 0:
        return False  # ran without a slot (Redis was unavailable) - nothing to free
    key = ffmpeg_slot_key(index)
    try:
        return bool(await redis.eval(_RELEASE_LOCK_SCRIPT, 1, key, str(job_id or "")))
    except Exception:
        logger.debug("batch_pipeline: failed to release ffmpeg slot %s", index)
        return False


# ---------------------------------------------------------------------------
# Capacity telemetry (for the /session_status monitoring dashboard)
# ---------------------------------------------------------------------------

# Each worker records its own RSS here, keyed by host:pid and carrying a TTL, so
# the dashboard can show how close the busiest worker is to the memory ceiling
# without reaching into any worker's process.
WORKER_RSS_KEY_PREFIX = "ffmpeg:worker:rss:"
# Three times the worker's heartbeat interval, so a single missed beat is not
# enough to make a live worker look offline.
WORKER_RSS_TTL_SECONDS = _env_number("WORKER_RSS_TTL_SECONDS", 90)


def worker_identity() -> str:
    """A stable id for this worker process, used to key its heartbeat."""
    try:
        import socket

        return f"{socket.gethostname()}:{os.getpid()}"
    except Exception:
        return f"worker:{os.getpid()}"


async def publish_worker_rss(redis=None, *, worker_id=None, rss=None) -> bool:
    """Record this process's RSS in Redis for the dashboard's capacity view."""
    try:
        if redis is None:
            from utils.job_queue import get_redis

            redis = await get_redis()
        value = rss_bytes() if rss is None else int(rss)
        key = f"{WORKER_RSS_KEY_PREFIX}{worker_id or worker_identity()}"
        await redis.set(key, int(value), ex=max(1, int(WORKER_RSS_TTL_SECONDS)))
        return True
    except Exception:
        logger.debug("batch_pipeline: could not publish worker RSS")
        return False


async def read_worker_rss(redis) -> dict:
    """Return ``{worker_id: rss_bytes}`` for every worker reporting right now."""
    out: dict = {}
    try:
        async for key in redis.scan_iter(match=f"{WORKER_RSS_KEY_PREFIX}*", count=200):
            text = key.decode() if isinstance(key, (bytes, bytearray)) else str(key)
            try:
                out[text[len(WORKER_RSS_KEY_PREFIX) :]] = int(await redis.get(text))
            except (TypeError, ValueError):
                continue
    except Exception:
        logger.debug("batch_pipeline: could not read worker RSS heartbeats")
    return out


async def read_used_slots(redis, *, slots=None):
    """How many of the global conversion slots are taken now (``None`` on error)."""
    total = MAX_CONCURRENT_FFMPEG if slots is None else max(1, int(slots))
    try:
        return int(await redis.exists(*[ffmpeg_slot_key(index) for index in range(total)]))
    except Exception:
        logger.debug("batch_pipeline: could not count busy ffmpeg slots")
        return None


# ---------------------------------------------------------------------------
# Memory reclamation
# ---------------------------------------------------------------------------


def rss_bytes() -> int:
    """Resident set size of this process in bytes (0 when unavailable)."""
    try:
        import psutil

        return int(psutil.Process(os.getpid()).memory_info().rss)
    except Exception:
        pass
    try:
        with open("/proc/self/statm", encoding="ascii") as fh:
            pages = int(fh.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Memory ceiling: never start a conversion on an already-dirty process
# ---------------------------------------------------------------------------

# Bytes of RSS above which the worker refuses to start another job until the
# process is clean (0 disables). Set it below the container limit - e.g. ~75%
# of a 1 GB box - so the *next* ffmpeg never adds its peak on top of the
# previous file's leftovers. Distinct from WORKER_RESTART_AFTER_JOB_BYTES,
# which restarts instead of gating.
MEMORY_CEILING_BYTES = _env_number("WORKER_MEMORY_CEILING_BYTES", 0)
# How many times a job may be deferred for the ceiling before it is run anyway
# (so a mis-set ceiling degrades throughput instead of stalling the queue).
MEMORY_CEILING_MAX_DEFERS = _env_number("WORKER_MEMORY_CEILING_MAX_DEFERS", 3)
# Transient job field counting how often this job has been held back.
CEILING_DEFER_FIELD = "_ceiling_defers"


def over_memory_ceiling() -> bool:
    """True when the process is already at or above the configured ceiling."""
    if MEMORY_CEILING_BYTES <= 0:
        return False
    current = rss_bytes()
    return bool(current) and current >= MEMORY_CEILING_BYTES


def _malloc_trim() -> bool:
    """Ask glibc to return free heap pages to the OS.

    ``gc.collect()`` frees Python objects, but the glibc allocator often keeps
    the underlying pages, which is why RSS can stay high after a large job and
    the *next* job OOMs on top of it. No-op on non-glibc platforms.
    """
    if os.name != "posix":
        return False
    try:
        import ctypes

        libc = ctypes.CDLL("libc.so.6")
        return bool(libc.malloc_trim(0))
    except Exception:
        return False


def reclaim_memory(reason: str = "", *, force: bool = True) -> dict:
    """Force a clean slate after a job and report what it did.

    Steps, in order: clear the worker's probe caches, drop the gc generation
    leftovers, ``malloc_trim`` the allocator, then measure RSS. Never raises -
    a failed cleanup must not fail a job that already produced its output.
    """
    before = rss_bytes()
    trimmed = False
    try:
        if force:
            # ffmpeg probe results and thumbnail paths are the largest caches the
            # worker keeps across jobs; clearing them here frees them immediately
            # instead of at the start of the next job.
            try:
                from workers import ffmpeg_worker

                ffmpeg_worker._output_probe_cache.clear()
            except Exception:
                pass
            collected = gc.collect()
            trimmed = _malloc_trim()
        else:
            collected = 0
    except Exception:
        logger.debug("batch_pipeline: reclaim_memory(%s) failed", reason)
        collected = 0

    after = rss_bytes()
    if before and after and after > before:
        # Not fatal - allocator warmth - but worth seeing next to the drop.
        logger.debug(
            "batch_pipeline: RSS grew during cleanup (%s): %.1fMB -> %.1fMB",
            reason,
            before / 1024 / 1024,
            after / 1024 / 1024,
        )
    return {"before": before, "after": after, "collected": collected, "trimmed": trimmed}


def _temp_roots() -> list[str]:
    """Directories the worker may have dropped temp artifacts into."""
    roots = [tempfile.gettempdir()]
    try:
        storage_path = os.getenv("STORAGE_PATH", "storage")
        roots.append(os.path.join(storage_path, "temp"))
    except Exception:
        pass
    seen, ordered = set(), []
    for root in roots:
        if root and root not in seen:
            seen.add(root)
            ordered.append(root)
    return ordered


# Prefixes of throwaway artifacts the worker creates per job. Anything else in
# temp is left alone - a missing cleanup is cheaper than deleting a file that
# is still in use.
_TEMP_PREFIXES = ("worker_thumb_", "delivery_thumb_", "worker_tmp_")


def sweep_temp_artifacts(grace_seconds: float = 300.0) -> dict:
    """Delete leftover per-job temp artifacts older than ``grace_seconds``.

    Only files/dirs carrying a known worker temp prefix are touched, and only
    once they are old enough that no in-flight job can own them. Returns a
    small summary for logging/metrics.
    """
    removed = 0
    freed = 0
    cutoff = time.time() - max(0.0, float(grace_seconds))
    for root in _temp_roots():
        try:
            names = os.listdir(root)
        except Exception:
            continue
        for name in names:
            if not name.startswith(_TEMP_PREFIXES):
                continue
            path = os.path.join(root, name)
            try:
                if os.path.getmtime(path) > cutoff:
                    continue
                if os.path.isdir(path):
                    freed += _dir_size(path)
                    _rmtree(path)
                else:
                    freed += os.path.getsize(path)
                    os.remove(path)
                removed += 1
            except Exception:
                logger.debug("batch_pipeline: could not remove temp artifact %s", path)
    return {"removed": removed, "freed": freed}


def _dir_size(path: str) -> int:
    total = 0
    with contextlib.suppress(Exception):
        for dirpath, _dirnames, filenames in os.walk(path):
            for filename in filenames:
                with contextlib.suppress(OSError):
                    total += os.path.getsize(os.path.join(dirpath, filename))
    return total


def _rmtree(path: str) -> None:
    try:
        from utils.file_utils import safe_rmtree

        safe_rmtree(path)
    except Exception:
        logger.debug("batch_pipeline: _rmtree failed for %s", path)


async def finalize_job(
    job: dict | None, *, source: str = "", redis=None, sweep: bool = True, ffmpeg_slot=None
) -> dict:
    """End-of-job cleanup: free the ffmpeg slot and batch lock, reclaim memory.

    ``ffmpeg_slot`` is the slot index the caller took from
    :func:`acquire_ffmpeg_slot`. It is passed in rather than stored on the job so
    a transient internal marker never ends up in the persisted payload. Called
    from the worker after **every** queued job, batch or not, so the "finish ->
    clean -> next" contract holds for the whole queue, not just the bulk menu.
    Never raises.
    """
    job = job or {}
    job_id = job.get("job_id")
    batch_id = job_batch_id(job)
    summary: dict = {}

    # 1. Hand the global ffmpeg slot back first: that is the token other
    #    workers/services are waiting on, so it must be free before any cleanup
    #    work below.
    if ffmpeg_slot is not None:
        try:
            if redis is None:
                from utils.job_queue import get_redis

                redis = await get_redis()
            summary["ffmpeg_slot_released"] = await release_ffmpeg_slot(redis, ffmpeg_slot, job_id)
        except Exception:
            logger.debug("batch_pipeline: ffmpeg slot release failed for %s", job_id)

    # 2. Free the batch so the next job of this apply can run.
    if batch_id:
        try:
            if redis is None:
                from utils.job_queue import get_redis

                redis = await get_redis()
            unlocked = await release_batch_lock(redis, batch_id, job_id)
            summary["batch_released"] = unlocked
        except Exception:
            logger.debug("batch_pipeline: batch lock release failed for %s", batch_id)

    # 3. Finish -> clean -> next.
    try:
        summary["memory"] = reclaim_memory(f"job {job_id} ({source})")
        if sweep:
            summary["temp"] = sweep_temp_artifacts()
    except Exception:
        logger.debug("batch_pipeline: post-job cleanup failed for %s", job_id)

    return summary


# ---------------------------------------------------------------------------
# Memory-pressure monitoring / optional worker restart
# ---------------------------------------------------------------------------

# Set when the RSS after a finished job is still above the configured ceiling,
# or when an admin asked for a restart. The standalone worker reads it after the
# loop exits and restarts the container; the in-process (bot-hosted) worker only
# logs it, because exiting there would take the bot down with it.
_restart_requested = False

# Bytes of resident memory above which a finished job is considered to have
# left the process dirty enough that a restart is safer than another job.
# 0 disables the check entirely (the safe default).
RESTART_RSS_THRESHOLD = _env_number("WORKER_RESTART_AFTER_JOB_BYTES", 0)


def memory_pressure() -> bool:
    """True when RSS exceeds the configured restart threshold (0 => never)."""
    if RESTART_RSS_THRESHOLD <= 0:
        return False
    current = rss_bytes()
    return bool(current) and current >= RESTART_RSS_THRESHOLD


def request_restart(reason: str = "") -> bool:
    """Flag a clean restart of this worker process.

    The standalone worker finishes the job in hand and then exits non-zero, so
    the platform brings it back with an empty heap. Honoured only where the
    worker loop was started with ``allow_restart``, so calling this from the bot
    process cannot take the bot down.
    """
    global _restart_requested
    _restart_requested = True
    logger.warning(
        "batch_pipeline: worker restart requested%s", f" ({reason})" if reason else ""
    )
    return True


def request_restart_if_pressured() -> bool:
    """Flag a restart when memory is still high after a cleaned-up job.

    Restarting guarantees a truly empty heap for the next file, which matters
    most for very large inputs; because it costs a container boot, it is opt-in
    via ``WORKER_RESTART_AFTER_JOB_BYTES``.
    """
    if not memory_pressure():
        return False
    logger.warning(
        "batch_pipeline: RSS %.1fMB is still above the restart threshold after cleanup",
        rss_bytes() / 1024 / 1024,
    )
    return request_restart("memory pressure")


# ---------------------------------------------------------------------------
# Admin-requested restart
# ---------------------------------------------------------------------------

# An admin can ask the worker to recycle at a safe point - after the job it is
# running, or while idle. The request rides in Redis so it reaches the worker
# process however the deployment is split; the TTL stops a request lingering
# forever when no worker is up to honour it.
WORKER_RESTART_KEY = "ffmpeg:worker:restart"
WORKER_RESTART_TTL_SECONDS = _env_number("WORKER_RESTART_REQUEST_TTL_SECONDS", 300)


async def request_worker_restart(redis=None, *, requested_by=None) -> bool:
    """Ask the standalone worker to restart cleanly, via Redis."""
    try:
        if redis is None:
            from utils.job_queue import get_redis

            redis = await get_redis()
        who = "admin" if requested_by in (None, "") else str(requested_by)
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        await redis.set(
            WORKER_RESTART_KEY,
            f"{who}@{stamp}",
            ex=max(1, int(WORKER_RESTART_TTL_SECONDS)),
        )
        logger.info("batch_pipeline: restart requested by %s", who)
        return True
    except Exception:
        logger.debug("batch_pipeline: could not request a worker restart")
        return False


async def consume_worker_restart(redis) -> str | None:
    """Take a pending restart request, if any, so it is honoured exactly once.

    Returns the request token (who asked and when) or ``None``. Consuming the
    key is what makes this safe: a worker that restarts and comes back must not
    find the same request waiting and restart again in a loop.
    """
    try:
        # GETDEL is the atomic form; fall back to GET+DEL on older servers.
        getdel = getattr(redis, "getdel", None)
        if getdel is not None:
            value = await getdel(WORKER_RESTART_KEY)
        else:
            value = await redis.get(WORKER_RESTART_KEY)
            if value is not None:
                await redis.delete(WORKER_RESTART_KEY)
    except Exception:
        logger.debug("batch_pipeline: could not check for a pending worker restart")
        return None
    if value is None:
        return None
    return value.decode() if isinstance(value, (bytes, bytearray)) else str(value)


def restart_requested() -> bool:
    """Whether :func:`request_restart_if_pressured` has flagged a restart."""
    return _restart_requested


def reset_restart_request() -> None:
    """Clear the restart flag (tests / callers that manage their own lifecycle)."""
    global _restart_requested
    _restart_requested = False
