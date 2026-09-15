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
BATCH_CANCELLED_FIELD = "batch_cancelled"

# The bulk menu caps a batch at 30 entries; this is the same ceiling used when
# sanity-checking a total that arrives off the queue.
BATCH_MAX_JOBS = 30

# ---------------------------------------------------------------------------
# Per-batch lock (one job of a batch at a time, across replicas)
# ---------------------------------------------------------------------------

# Namespace shared by everything keyed on a batch: the lock (`<id>`) and the
# finished-file counter (`<id>:done`).
BATCH_KEY_PREFIX = "ffmpeg:batch:"
# How long a worker may hold a batch's lock without refreshing it, in seconds.
#
# This is deliberately *bounded* and short, and refreshed while the job runs (see
# :func:`refresh_batch_lock`) - so it has to outlive a heartbeat gap, not a whole
# conversion. It used to inherit the worker's 6h job ceiling, and that was the
# bug behind "batch frozen at 0%": a worker that died (or was redeployed) while
# holding `ffmpeg:batch:<id>` fenced that batch for six hours - every remaining
# job could not take the lock, so it deferred, got promoted, deferred again, and
# the progress message sat at "0 of N" the whole time. Fifteen minutes is still
# comfortably longer than any heartbeat gap, and short enough that an orphaned
# lock cannot outlive the container that dropped it.
BATCH_LOCK_TTL_SECONDS = max(
    1,
    min(
        _env_number("BATCH_LOCK_TTL_SECONDS", 900),
        _env_number("JOB_MAX_SECONDS", 6 * 3600),
    ),
)
# How often the owning worker re-arms the TTL on the claims it holds while a job
# runs. Must stay a small fraction of BATCH_LOCK_TTL_SECONDS.
CLAIM_HEARTBEAT_SECONDS = max(1.0, _env_number("CLAIM_HEARTBEAT_SECONDS", 60.0))
# Lifetime of a batch's *data*: its counters, its total, the location of its
# message, its cancellation marker and its job set. Deliberately independent of
# BATCH_LOCK_TTL_SECONDS above - the lock is a short-lived claim that a worker
# refreshes, while these keys have to survive however long the batch really
# takes. A 30-file Apply Bulk on a 1 GB box can legitimately run for hours, and
# letting the `:done` counter expire mid-batch would silently restart its
# progress at zero. This is only a backstop against keys outliving the feature.
BATCH_STATE_TTL_SECONDS = max(1, _env_number("BATCH_STATE_TTL_SECONDS", 30 * 24 * 3600))
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

# Compare-and-pexpire: re-arm the TTL, but only for the owner that still holds
# the claim. This is what lets the TTL above stay short (so a dead worker's
# claim expires quickly) without a live worker ever losing a long conversion's
# slot or batch lock mid-job.
_REFRESH_LOCK_SCRIPT = """
local current = redis.call('get', KEYS[1])
if current == ARGV[1] then
    redis.call('pexpire', KEYS[1], ARGV[2])
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


def batch_cancel_key(batch_id) -> str:
    """Redis marker that prevents any remaining member from starting."""
    return f"{BATCH_KEY_PREFIX}{batch_id}:cancelled"


def batch_jobs_key(batch_id) -> str:
    """Redis set containing the job IDs belonging to one batch."""
    return f"{BATCH_KEY_PREFIX}{batch_id}:jobs"


async def is_batch_cancelled(redis=None, batch_id=None) -> bool:
    """Return whether a batch has been cancelled.

    ``redis`` is optional so the *bot* can ask the same question as the worker:
    the bulk apply checks this between files, which is what stops a stopped batch
    from carrying on fetching (and relay-forwarding) its remaining sources. The
    marker is only ever set, never unset, so a second read costs nothing.
    """
    if not batch_id:
        return False
    own = redis is None
    try:
        if own:
            from utils.job_queue import get_redis

            redis = await get_redis()
        return bool(await redis.exists(batch_cancel_key(batch_id)))
    except Exception:
        logger.debug("batch_pipeline: could not read cancellation marker for %s", batch_id)
        return False
    finally:
        if own and redis is not None:
            with contextlib.suppress(Exception):
                await redis.close()


def batch_done_jobs_key(batch_id) -> str:
    """Redis set of the job ids already counted toward a batch's progress."""
    return f"{BATCH_KEY_PREFIX}{batch_id}:done_jobs"


async def claim_batch_progress_slot(redis, batch_id, job_id) -> bool:
    """True the first time this job is counted toward its batch, False after that.

    A job can be delivered more than once - the broker retries a job whose handler
    raised - and every attempt runs the same end-of-job bookkeeping. Counting with
    a plain INCR would then count one file twice, finish the batch a file early and
    take its progress message down while work was still queued. Set membership
    makes the count exactly-once per job instead.

    Fails open (returns True): under-counting would stall a batch forever, while
    counting a duplicate once more only misreports by one.
    """
    if not batch_id or not job_id:
        return True
    try:
        added = await redis.sadd(batch_done_jobs_key(batch_id), str(job_id))
        with contextlib.suppress(Exception):
            await redis.expire(batch_done_jobs_key(batch_id), int(BATCH_STATE_TTL_SECONDS))
        return bool(added)
    except Exception:
        logger.debug("batch_pipeline: could not claim a progress slot for job %s", job_id)
        return True


# ---------------------------------------------------------------------------
# Resuming a batch that was interrupted
# ---------------------------------------------------------------------------

# The bulk apply feeds the queue one file at a time, so a restart mid-batch
# abandons whatever it had not reached yet. Those files are still in the user's
# persisted collection - but so are the ones that already finished, so on its own
# a restart makes the next Apply redo completed work. Every finished file is
# therefore recorded here against its batch, and the bulk menu subtracts those
# entries the next time it is opened: what is left is exactly what was never
# processed.
#
# The record is keyed per *user* rather than per batch, because that is the
# question the menu asks: "which of my collected files are already done?".

def batch_finished_keys(batch_id) -> str:
    """Redis set of the collection entries a batch has already finished."""
    return f"{BATCH_KEY_PREFIX}{batch_id}:finished"


def user_batches_key(user_id) -> str:
    """Redis set of the batches a user has left unfinished or unclosed."""
    return f"{BATCH_KEY_PREFIX}resume:{user_id}"


async def open_batch_resume(redis=None, *, batch_id, user_id) -> bool:
    """Register a batch as resumable, before its first file starts."""
    if not batch_id or not user_id:
        return False
    own = redis is None
    try:
        if own:
            from utils.job_queue import get_redis

            redis = await get_redis()
        await redis.sadd(user_batches_key(user_id), str(batch_id))
        with contextlib.suppress(Exception):
            await redis.expire(user_batches_key(user_id), int(BATCH_STATE_TTL_SECONDS))
        # Stamp the start. An apply spends minutes fetching its first file before
        # it has a job to point at, so age is the only thing that can tell a
        # batch that is still starting up from one abandoned mid-setup.
        with contextlib.suppress(Exception):
            await redis.set(batch_started_key(batch_id), time.time(), ex=int(BATCH_STATE_TTL_SECONDS))
        return True
    except Exception:
        logger.debug("batch_pipeline: could not open a resume record for %s", batch_id)
        return False
    finally:
        if own and redis is not None:
            with contextlib.suppress(Exception):
                await redis.close()


async def mark_batch_entry_finished(redis, batch_id, entry_key) -> bool:
    """Record one collection entry as done, so a later resume will skip it."""
    if not batch_id or not entry_key:
        return False
    try:
        await redis.sadd(batch_finished_keys(batch_id), str(entry_key))
        with contextlib.suppress(Exception):
            await redis.expire(batch_finished_keys(batch_id), int(BATCH_STATE_TTL_SECONDS))
        return True
    except Exception:
        logger.debug("batch_pipeline: could not record a finished entry for %s", batch_id)
        return False


async def read_finished_entries(redis, user_id) -> dict:
    """``{batch_id: {entry_key, ...}}`` for every batch this user left unfinished."""
    out: dict = {}
    if not user_id:
        return out
    try:
        batch_ids = await redis.smembers(user_batches_key(user_id))
    except Exception:
        logger.debug("batch_pipeline: could not list unfinished batches for %s", user_id)
        return out
    for raw in batch_ids:
        batch_id = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
        try:
            keys = await redis.smembers(batch_finished_keys(batch_id))
        except Exception:
            continue
        decoded = {k.decode() if isinstance(k, (bytes, bytearray)) else str(k) for k in keys}
        if decoded:
            out[batch_id] = decoded
    return out


async def forget_finished_entries(redis, finished: dict, consumed) -> None:
    """Drop recorded entries once they have been subtracted from a collection.

    Forgetting them is what keeps a *re-sent* file from being skipped later: an
    entry is only ever removed once it has been accounted for.
    """
    for batch_id, keys in (finished or {}).items():
        victims = [key for key in keys if key in consumed]
        if not victims:
            continue
        with contextlib.suppress(Exception):
            await redis.srem(batch_finished_keys(batch_id), *victims)


async def close_batch_resume(redis=None, *, batch_id, user_id=None) -> bool:
    """Discard a batch's resume record - it finished, or was stopped."""
    if not batch_id:
        return False
    own = redis is None
    try:
        if own:
            from utils.job_queue import get_redis

            redis = await get_redis()
        if user_id:
            with contextlib.suppress(Exception):
                await redis.srem(user_batches_key(user_id), str(batch_id))
        with contextlib.suppress(Exception):
            await redis.delete(batch_finished_keys(batch_id))
        return True
    except Exception:
        logger.debug("batch_pipeline: could not close the resume record for %s", batch_id)
        return False
    finally:
        if own and redis is not None:
            with contextlib.suppress(Exception):
                await redis.close()


async def mark_batch_file_done(redis=None, *, batch_id) -> int | None:
    """Count one file of a batch as finished, without a worker job reporting it.

    A file the bot finishes *inside the handler* (the inline pipeline path) never
    becomes a queued job, so no worker would ever advance the batch's ``:done``
    counter - yet it is still counted toward the batch total. That left the batch
    permanently one short: its bar could only ever read "0 of 1" and was never
    taken down. Returns the new count, or ``None`` if it could not be recorded.
    """
    if not batch_id:
        return None
    own = redis is None
    try:
        if own:
            from utils.job_queue import get_redis

            redis = await get_redis()
        done = int(await redis.incr(batch_progress_key(batch_id)))
        with contextlib.suppress(Exception):
            await redis.expire(batch_progress_key(batch_id), int(BATCH_STATE_TTL_SECONDS))
        return done
    except Exception:
        logger.debug("batch_pipeline: could not mark a file done for %s", batch_id)
        return None
    finally:
        if own and redis is not None:
            with contextlib.suppress(Exception):
                await redis.close()


async def cancel_batch(redis=None, *, batch_id, requested_by=None, ttl_seconds=None) -> dict:
    """Cancel one batch and remove its queued or delayed members.

    The marker is written first. A worker that already owns one job observes the
    per-job cancel flag; every later member is discarded before it can acquire a
    conversion slot or be deferred again.
    """
    if not batch_id:
        return {"batch_id": batch_id, "jobs": 0, "queued": 0, "delayed": 0}
    if redis is None:
        from utils.job_queue import get_redis

        redis = await get_redis()
    ttl = max(1, int(ttl_seconds or BATCH_STATE_TTL_SECONDS))
    job_ids = []
    queued = delayed = 0
    try:
        await redis.set(batch_cancel_key(batch_id), str(requested_by or "user"), ex=ttl)
        from utils.job_queue import DELAYED_SET, JOB_LIST

        members = await redis.smembers(batch_jobs_key(batch_id))
        for member in members:
            job_id = member.decode() if isinstance(member, bytes) else str(member)
            key = f"ffmpeg:job:{job_id}"
            job_ids.append(job_id)
            await redis.hset(
                key,
                mapping={
                    "cancel": "1",
                    "status": "cancelled",
                    "message": "batch cancelled",
                },
            )

        prune_script = """
        local removed = 0
        local items = redis.call('lrange', KEYS[1], 0, -1)
        for _, item in ipairs(items) do
            local ok, parsed = pcall(cjson.decode, item)
            if ok and type(parsed) == 'table' and tostring(parsed.batch_id) == ARGV[1] then
                redis.call('lrem', KEYS[1], 1, item)
                removed = removed + 1
            end
        end
        return removed
        """
        queued = int(await redis.eval(prune_script, 1, JOB_LIST, str(batch_id)))
        delayed_items = await redis.zrange(DELAYED_SET, 0, -1)
        for item in delayed_items:
            raw = item.decode() if isinstance(item, bytes) else item
            try:
                if str(json.loads(raw).get(BATCH_ID_FIELD, "")) == str(batch_id):
                    delayed += int(await redis.zrem(DELAYED_SET, item))
            except Exception:
                continue
        with contextlib.suppress(Exception):
            await redis.delete(batch_jobs_key(batch_id))
        return {"batch_id": str(batch_id), "jobs": len(job_ids), "queued": queued, "delayed": delayed}
    finally:
        if redis is not None:
            with contextlib.suppress(Exception):
                await redis.close()


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
            batch_total_key(batch_id), int(total), ex=max(1, int(BATCH_STATE_TTL_SECONDS))
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


def parse_batch_message_ref(stored) -> tuple[int, int] | None:
    """Decode the ``chat_id:message_id`` a batch message location is stored as.

    Shared by the worker (which edits and removes the message) and the bot
    (which closes the batch out once every job it queued has finished), so the
    encoding lives in exactly one place.
    """
    try:
        text = stored.decode() if isinstance(stored, (bytes, bytearray)) else str(stored)
        chat, _, message = text.partition(":")
        return int(chat), int(message)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# "All my batches" view
# ---------------------------------------------------------------------------

# The set of batch ids still running, so one message can show every batch at
# once instead of the user juggling a message per batch. This is a *render*
# input, not a second state machine: the per-batch ``:done`` and ``:total``
# counters already hold the truth, and this set only says which ones are worth
# drawing. Members whose counters have expired are pruned as they are read, so a
# batch whose worker died can never leave a phantom row behind.
ACTIVE_BATCHES_KEY = f"{BATCH_KEY_PREFIX}active"
# Rows the aggregate view draws before it summarises the remainder.
BATCH_VIEW_MAX_ROWS = max(1, _env_number("BATCH_VIEW_MAX_ROWS", 6))


def progress_bar(done, total, width: int = 10) -> str:
    """A fixed-width text bar, e.g. ``██████░░░░``.

    Width is constant regardless of the batch size so several batches stack into
    a readable column in the aggregate view.
    """
    try:
        total = int(total)
        done = int(done)
    except (TypeError, ValueError):
        return "░" * width
    if total <= 0:
        return "░" * width
    filled = int(round(max(0, min(done, total)) / total * width))
    return "█" * filled + "░" * (width - filled)


async def register_active_batch(redis=None, *, batch_id, ttl_seconds=None) -> bool:
    """Mark a batch as worth drawing in the aggregate view."""
    if not batch_id:
        return False
    own = redis is None
    try:
        if own:
            from utils.job_queue import get_redis

            redis = await get_redis()
        await redis.sadd(ACTIVE_BATCHES_KEY, str(batch_id))
        with contextlib.suppress(Exception):
            await redis.expire(ACTIVE_BATCHES_KEY, int(ttl_seconds or BATCH_STATE_TTL_SECONDS))
        return True
    except Exception:
        logger.debug("batch_pipeline: could not register active batch %s", batch_id)
        return False
    finally:
        if own and redis is not None:
            with contextlib.suppress(Exception):
                await redis.close()


async def unregister_active_batch(redis=None, *, batch_id) -> bool:
    """Stop drawing a batch - it finished, or was cancelled."""
    if not batch_id:
        return False
    own = redis is None
    try:
        if own:
            from utils.job_queue import get_redis

            redis = await get_redis()
        await redis.srem(ACTIVE_BATCHES_KEY, str(batch_id))
        return True
    except Exception:
        logger.debug("batch_pipeline: could not unregister active batch %s", batch_id)
        return False
    finally:
        if own and redis is not None:
            with contextlib.suppress(Exception):
                await redis.close()


async def read_batch_counters(redis, batch_id) -> dict:
    """``{total, done}`` for one batch, with ``total`` preferring the recorded
    exact enqueued count (see :func:`set_batch_total`)."""
    out = {"batch_id": str(batch_id), "total": 0, "done": 0}
    with contextlib.suppress(Exception):
        recorded = await redis.get(batch_total_key(batch_id))
        if recorded is not None:
            out["total"] = int(recorded)
    with contextlib.suppress(Exception):
        out["done"] = int(await redis.get(batch_progress_key(batch_id)) or 0)
    return out


async def read_active_batches(redis) -> list[dict]:
    """Every batch still worth drawing, with its counters.

    Prunes members as a side effect: a batch whose counters are gone (expired,
    or never written) is removed from the set rather than rendered as ``0/0``.
    """
    rows: list[dict] = []
    try:
        members = await redis.smembers(ACTIVE_BATCHES_KEY)
    except Exception:
        logger.debug("batch_pipeline: could not list active batches")
        return rows
    for member in members:
        batch_id = member.decode() if isinstance(member, (bytes, bytearray)) else str(member)
        counters = await read_batch_counters(redis, batch_id)
        if counters["total"] <= 0:
            with contextlib.suppress(Exception):
                await redis.srem(ACTIVE_BATCHES_KEY, batch_id)
            continue
        rows.append(counters)
    rows.sort(key=lambda row: row["done"] / max(row["total"], 1))
    return rows



# ---------------------------------------------------------------------------
# Tearing a batch down
# ---------------------------------------------------------------------------

# A batch with no members yet is protected by age instead, because a fresh apply
# legitimately spends minutes fetching its first file before the job that owns it
# exists. Members are the real signal; this only covers that window.
BATCH_PURGE_GRACE_SECONDS = max(0, _env_number("BATCH_PURGE_GRACE_SECONDS", 900))

# The tombstone written when a batch is taken down. Kept, not deleted: a worker
# that is still finishing one of its members asks about it before it edits or
# reposts the progress message, so a batch that is over cannot put its bar back.
BATCH_TOMBSTONE_REASON = "cancelled by admin"

# Suffixes under ``ffmpeg:batch:`` that are not batch ids themselves.
_NON_BATCH_SUFFIXES = frozenset({"active", "resume"})

# A job hash that says one of these is not running any more. Anything else -
# including a status this build has never heard of - counts as live, so a cleanup
# never tears down work it does not understand.
_TERMINAL_JOB_STATUSES = frozenset({"done", "completed", "error", "failed", "cancelled", "canceled"})

_JOB_HASH_PREFIX = "ffmpeg:job:"
_RESUME_KEY_PREFIX = f"{BATCH_KEY_PREFIX}resume:"


def batch_started_key(batch_id) -> str:
    """When a batch began, so a memberless one is not mistaken for stale."""
    return f"{BATCH_KEY_PREFIX}{batch_id}:started"


def batch_state_keys(batch_id) -> tuple[str, ...]:
    """Every key a batch owns - apart from its lock and its tombstone."""
    return (
        batch_jobs_key(batch_id),
        batch_total_key(batch_id),
        batch_progress_key(batch_id),
        batch_done_jobs_key(batch_id),
        batch_message_key(batch_id),
        batch_finished_keys(batch_id),
        batch_started_key(batch_id),
    )


async def purge_batch(redis=None, *, batch_id, reason=BATCH_TOMBSTONE_REASON, ttl_seconds=None) -> dict:
    """Take one batch down: tombstone it first, then drop everything it owns.

    Order matters. The tombstone stops a worker that is still finishing a member
    from reposting the progress message, so it has to be written before the
    message's location is forgotten. The location itself is *returned* rather
    than deleted from the chat, because removing a Telegram message needs a bot
    and this module deliberately has none.
    """
    result = {"batch_id": str(batch_id or ""), "keys": 0, "message": None}
    if not batch_id:
        return result
    own = redis is None
    try:
        if own:
            from utils.job_queue import get_redis

            redis = await get_redis()
        ttl = max(1, int(ttl_seconds or BATCH_STATE_TTL_SECONDS))
        with contextlib.suppress(Exception):
            await redis.set(batch_cancel_key(batch_id), str(reason), ex=ttl)
        with contextlib.suppress(Exception):
            stock = await redis.get(batch_message_key(batch_id))
            result["message"] = parse_batch_message_ref(stock) if stock else None
        result["keys"] = int(await redis.delete(*batch_state_keys(batch_id)) or 0)
        with contextlib.suppress(Exception):
            await redis.srem(ACTIVE_BATCHES_KEY, str(batch_id))
        return result
    except Exception:
        logger.debug("batch_pipeline: could not purge batch %s", batch_id)
        return result
    finally:
        if own and redis is not None:
            with contextlib.suppress(Exception):
                await redis.close()


async def purge_stale_batches(
    redis=None, *, cancelled_job_ids=None, reason=BATCH_TOMBSTONE_REASON, ttl_seconds=None
) -> dict:
    """Take down every batch that has no live member left.

    This is what ``/cancelall`` runs, so a stale batch no longer needs
    ``scripts/cleanup_stale_redis.py``: cancelling everything leaves no job of
    any batch alive, and every batch therefore looks stale. A batch is *kept*
    when any member's job hash still looks active (or when its state cannot be
    read at all), so a batch that is genuinely running - including one started
    while this ran - is never torn down.

    ``cancelled_job_ids`` is the caller saying "I cancelled these myself": a
    member on that list cannot keep its batch alive even if its hash still reads
    active, which is what covers a cancel whose flag write failed.

    Returns ``{batches, keys, messages, kept}``, where ``messages`` are the
    ``(chat_id, message_id)`` pairs the caller should delete from the chat.
    """
    summary: dict = {"batches": [], "keys": 0, "messages": [], "kept": 0}
    own = redis is None
    try:
        if own:
            from utils.job_queue import get_redis

            redis = await get_redis()
        for batch_id in sorted(await _known_batch_ids(redis)):
            if await _batch_is_live(redis, batch_id, cancelled_job_ids):
                summary["kept"] += 1
                continue
            purged = await purge_batch(
                redis, batch_id=batch_id, reason=reason, ttl_seconds=ttl_seconds
            )
            summary["batches"].append(batch_id)
            summary["keys"] += purged["keys"]
            if purged["message"]:
                summary["messages"].append(purged["message"])
            await _forget_resume_membership(redis, batch_id)
        return summary
    except Exception:
        logger.debug("batch_pipeline: stale batch sweep failed")
        return summary
    finally:
        if own and redis is not None:
            with contextlib.suppress(Exception):
                await redis.close()


async def _known_batch_ids(redis) -> set[str]:
    """Every batch id Redis knows about, from its keys and the active set."""
    ids: set[str] = set()
    try:
        async for key in redis.scan_iter(match=f"{BATCH_KEY_PREFIX}*", count=500):
            text = key.decode() if isinstance(key, (bytes, bytearray)) else str(key)
            suffix = text[len(BATCH_KEY_PREFIX) :]
            batch_id = suffix.split(":", 1)[0]
            if batch_id and batch_id not in _NON_BATCH_SUFFIXES:
                ids.add(batch_id)
    except Exception:
        logger.debug("batch_pipeline: could not scan for batch keys")
    with contextlib.suppress(Exception):
        for member in await redis.smembers(ACTIVE_BATCHES_KEY):
            value = member.decode() if isinstance(member, (bytes, bytearray)) else str(member)
            if value:
                ids.add(value)
    return ids


async def _batch_is_live(redis, batch_id, cancelled_job_ids=None) -> bool:
    """Whether any of a batch's jobs is still running (or might be)."""
    known_dead = {str(value) for value in (cancelled_job_ids or ())}
    try:
        members = {_job_text(value) for value in await redis.smembers(batch_jobs_key(batch_id))}
    except Exception:
        # Unreadable membership: keep it. Tearing down a possibly-running batch
        # is worse than leaving metadata behind for the next sweep.
        return True
    if not members:
        return await _batch_is_fresh(redis, batch_id)
    for job_id in members:
        if job_id in known_dead:
            continue
        if await _member_is_active(redis, job_id):
            return True
    return False


async def _member_is_active(redis, job_id) -> bool:
    """Whether one job hash still looks like running work."""
    try:
        status = await redis.hget(f"{_JOB_HASH_PREFIX}{job_id}", "status")
    except Exception:
        return True
    value = _job_text(status)
    if not value:
        # No hash at all: the job is gone (expired, or never written), so it is
        # not running and cannot keep its batch alive.
        return False
    return value not in _TERMINAL_JOB_STATUSES


async def _batch_is_fresh(redis, batch_id) -> bool:
    """Whether a memberless batch is young enough to still be starting up."""
    if BATCH_PURGE_GRACE_SECONDS <= 0:
        return False
    try:
        raw = await redis.get(batch_started_key(batch_id))
    except Exception:
        return True
    if not raw:
        # No timestamp: a batch from before this bookkeeping existed. Nothing to
        # age against, and its members would have protected it - so it is stale.
        return False
    try:
        started = float(_job_text(raw))
    except (TypeError, ValueError):
        return False
    return (time.time() - started) < BATCH_PURGE_GRACE_SECONDS


async def _forget_resume_membership(redis, batch_id) -> None:
    """Drop a taken-down batch from every user's resume set."""
    try:
        async for key in redis.scan_iter(match=f"{_RESUME_KEY_PREFIX}*", count=200):
            with contextlib.suppress(Exception):
                await redis.srem(key, str(batch_id))
    except Exception:
        logger.debug("batch_pipeline: could not clear the resume record for %s", batch_id)


def _job_text(value) -> str:
    """A Redis value as ``str``, whether the client decodes or not."""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    return "" if value is None else str(value)


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


async def refresh_lock(redis, key, owner, ttl_seconds=None) -> bool:
    """Re-arm the TTL on a claim we still own; no-op once we no longer hold it.

    Called periodically by the worker for as long as its job runs. Without it
    the TTL would have to cover an entire conversion (hence the old 6h value and
    hence the freeze); with it, the TTL only has to cover the gap between two
    heartbeats and an orphaned claim self-heals in minutes.
    """
    ttl = BATCH_LOCK_TTL_SECONDS if ttl_seconds is None else ttl_seconds
    try:
        ttl_ms = int(max(1.0, float(ttl)) * 1000)
    except (TypeError, ValueError):
        ttl_ms = int(BATCH_LOCK_TTL_SECONDS * 1000)
    try:
        return bool(await redis.eval(_REFRESH_LOCK_SCRIPT, 1, key, str(owner or ""), ttl_ms))
    except Exception:
        logger.debug("batch_pipeline: could not refresh %s", key)
        return False


async def refresh_batch_lock(redis, batch_id, job_id, ttl_seconds=None) -> bool:
    """Keep this job's claim on its batch from expiring while the job runs."""
    return await refresh_lock(redis, batch_lock_key(batch_id), job_id, ttl_seconds)


async def refresh_ffmpeg_slot(redis, index, job_id, ttl_seconds=None) -> bool:
    """Keep this job's conversion slot from expiring while the job runs."""
    if index is None:
        return False
    try:
        index = int(index)
    except (TypeError, ValueError):
        return False
    if index < 0:
        return False  # ran without a slot (Redis was unavailable) - nothing to keep
    return await refresh_lock(redis, ffmpeg_slot_key(index), job_id, ttl_seconds)


async def defer_batch_job(redis, job: dict, delay: float | None = None) -> bool:
    """Put a job back for later, in Redis, so a busy batch never blocks the worker.

    Uses the existing delayed-job zset that :func:`utils.job_queue.pop_job`
    promotes when due, so a deferred batch job re-enters the normal queue path.
    """
    if not job:
        return False
    if await is_batch_cancelled(redis, job_batch_id(job)):
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
