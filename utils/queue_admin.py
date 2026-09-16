"""Admin maintenance for both background pipes and the Redis cache.

media_conversion_bot queues work on two independent pipes, and admin maintenance
has to reason about both or it silently leaves jobs behind:

1. Redis pipe (``utils/job_queue.py``)
     - Queue list    ``ffmpeg:jobs``              JSON job dicts, popped by workers
     - Delayed set   ``ffmpeg:delayed``           zset promoted onto the list by ``pop_job``
     - Job hashes    ``ffmpeg:job:<id>``          status/progress, read by /status
     - Progress      ``ffmpeg:progress:<id>``     live progress mirrors
     - Input locks   ``ffmpeg:lock:<sha256>``     one owner per input
     - Dedup keys    ``ffmpeg:pipeline_dedup:*``  -> job id that owns the input
2. RabbitMQ pipe (``utils/eventbus/rabbit.py``)
     - ``media.jobs.run`` / ``media.jobs.retry`` / ``media.jobs.dead``

It also takes down the batch state those jobs belong to (``ffmpeg:batch:*``:
counters, membership, the progress message's location, the resume record and the
active set). Cancelling every job leaves no batch with a live member, so every
batch is stale on the way out - which is why this replaces running
``scripts/cleanup_stale_redis.py`` by hand after a cancel. Each batch is
tombstoned as it goes so a worker still finishing one member cannot put its
progress bar back, and the bar itself is deleted from the chat when a bot is
passed in.

Ghosted work goes with it: dedup keys of every kind (including the ``pending``
placeholder an interrupted ingest leaves), tombstones old enough that no member
can still be running, and the conversion slots, batch locks and dedup keys left
behind by workers that are gone - which are what make the queue refuse new
conversions, and a single file refuse to be processed again.

``cancel_all_jobs`` drains both. It purges the broker even when
``EVENTBUS_QUEUE_ROLLOUT_PERCENT`` is 0, because that setting only stops *new*
jobs from being routed there - anything already queued must still be drained, or
a rollback would strand in-flight jobs (see ``EventBusSettings.consumes_rabbitmq``).

Both entry points return a report object instead of printing, so the Telegram
commands and any future CLI can render the same outcome.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from dataclasses import dataclass, field

from utils import job_queue
from utils.cache import PREFIX_FILE, PREFIX_JOB, PREFIX_META, PREFIX_RESPONSE, PREFIX_USER

logger = logging.getLogger(__name__)

JOB_HASH_PREFIX = "ffmpeg:job:"
PROGRESS_PREFIX = "ffmpeg:progress:"
LOCK_PREFIX = "ffmpeg:lock:"
DEDUP_PREFIX = "ffmpeg:pipeline_dedup:"
ROUTE_CACHE_PREFIX = "routecache:"

# Statuses the worker writes to ``ffmpeg:job:<id>`` that mean "no longer running".
TERMINAL_STATUSES = frozenset({"done", "completed", "error", "failed", "cancelled", "canceled"})

# Statuses that mean "this job is still the live owner of its input". Kept in step
# with the dedup check in ``scripts/cleanup_stale_dedup_keys.py`` and with
# ``_ensure_current_file_downloaded``.
ACTIVE_STATUSES = frozenset({"processing", "queued", "waiting", "started", "uploading", "sending"})

# ``pending`` is the placeholder the BigFilePipeline writes while a file is still
# being ingested, before the real job id exists. It has no job hash to look up and
# must be treated as active so a cancel-all never un-dedups an in-progress ingest.
PENDING_PLACEHOLDER = "pending"

CANCEL_REASON = "cancelled by admin"

# A broker that is configured but unreachable must not hold the command open: the
# Redis pipe is already drained by then, so the purge is reported as an error and
# the admin still gets an answer. Same spirit as ``publish_job`` falling back to
# the Redis list when the broker declines a job.
BROKER_PURGE_TIMEOUT_SECONDS = float(os.getenv("ADMIN_BROKER_PURGE_TIMEOUT", "20"))


def _text(value) -> str:
    """Return a Redis value as ``str`` whether the client decodes or not."""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    return "" if value is None else str(value)


async def _scan_keys(redis, pattern: str, count: int = 500) -> list[str]:
    keys: list[str] = []
    async for key in redis.scan_iter(match=pattern, count=count):
        keys.append(_text(key))
    return keys


@dataclass
class QueueReport:
    """Outcome of a ``/cancelall`` run, one counter per thing that was cleared."""

    queued: int = 0
    delayed: int = 0
    in_flight: int = 0
    progress_keys: int = 0
    locks_released: int = 0
    dedup_keys: int = 0
    batches: int = 0
    batch_messages: int = 0
    batch_keys: int = 0
    batch_tombstones: int = 0
    batches_kept: int = 0
    slots_freed: int = 0
    locks_freed: int = 0
    job_ids: list[str] = field(default_factory=list)
    broker: dict[str, int | str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def cancelled_jobs(self) -> int:
        """Jobs removed from the queue plus jobs flagged while already running."""
        return self.queued + self.delayed + self.in_flight

    def as_lines(self) -> list[str]:
        lines = ["🧹 Cancelled jobs on both pipes"]
        lines.append(f"• Queued removed:      {self.queued}")
        lines.append(f"• Delayed removed:     {self.delayed}")
        lines.append(f"• Running flagged:     {self.in_flight}")
        lines.append(f"• Progress keys:       {self.progress_keys}")
        lines.append(f"• Locks released:      {self.locks_released}")
        lines.append(f"• Dedup keys dropped:  {self.dedup_keys}")
        lines.append(f"• Batches cleared:     {self.batches}")
        if self.batches:
            lines.append(f"• Batch keys removed:  {self.batch_keys}")
            if self.batch_messages:
                lines.append(f"• Batch bars deleted:  {self.batch_messages}")
        if self.batch_tombstones:
            lines.append(f"• Old tombstones:      {self.batch_tombstones}")
        if self.slots_freed or self.locks_freed:
            claims = f"{self.slots_freed} slot(s), {self.locks_freed} batch lock(s)"
            lines.append(f"• Ghost claims freed:  {claims}")
        if self.broker:
            purged = ", ".join(f"{name}={count}" for name, count in sorted(self.broker.items()))
            lines.append(f"• Broker queues:       {purged}")
        else:
            lines.append("• Broker queues:       not configured")
        lines.append(f"\nTotal jobs affected: {self.cancelled_jobs}")
        if self.errors:
            lines.append("\n⚠️ Some steps failed:")
            lines.extend(f"• {err}" for err in self.errors)
        return lines


@dataclass
class CacheReport:
    """Outcome of a ``/clear_cache`` run."""

    prefixes: dict[str, int] = field(default_factory=dict)
    # ``cache:file:*`` is reported on its own because it is the media cache: the
    # descriptors and the cached bodies that let a repeat skip a download.
    media_cache: int = 0
    # Shared media-library objects in storage, only removed when explicitly asked.
    media_storage_keys: int = 0
    route_cache_keys: int = 0
    route_cache_memory: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def total_keys(self) -> int:
        return sum(self.prefixes.values()) + self.media_cache + self.route_cache_keys

    def as_lines(self) -> list[str]:
        lines = ["🧼 Redis cache cleared"]
        for prefix, count in sorted(self.prefixes.items()):
            lines.append(f"• {prefix}*  →  {count}")
        lines.append(f"• media cache ({PREFIX_FILE}*)  →  {self.media_cache}")
        lines.append(f"• {ROUTE_CACHE_PREFIX}*  →  {self.route_cache_keys}")
        if self.route_cache_memory:
            lines.append(f"• in-memory route cache  →  {self.route_cache_memory}")
        if self.media_storage_keys:
            lines.append(f"• media library objects in storage  →  {self.media_storage_keys}")
        lines.append(f"\nTotal keys deleted: {self.total_keys}")
        if self.errors:
            lines.append("\n⚠️ Some steps failed:")
            lines.extend(f"• {err}" for err in self.errors)
        return lines


async def _flag_job_cancelled(redis, job_id: str) -> None:
    """Set the same flags ``job_queue.cancel_job`` sets, so a running worker stops.

    The hash is deliberately kept (not deleted): a worker that is already
    mid-job only stops when it reads ``cancel=1`` from it.
    """
    if not job_id:
        return
    with contextlib.suppress(Exception):
        await redis.hset(
            f"{JOB_HASH_PREFIX}{job_id}",
            mapping={
                "cancel": "1",
                "status": "cancelled",
                "message": CANCEL_REASON,
                "progress": "0",
            },
        )


async def _drain_job_list(redis, report: QueueReport, cancelled: set[str]) -> None:
    """Remove every queued job from ``ffmpeg:jobs``.

    Flag each job's hash before deleting the list: a worker may pop an entry
    between the read and the delete, and the flag is what stops that job.
    """
    entries = await redis.lrange(job_queue.JOB_LIST, 0, -1)
    for entry in entries:
        job_id = _job_id_of(entry)
        if job_id:
            cancelled.add(job_id)
            report.job_ids.append(job_id)
            await _flag_job_cancelled(redis, job_id)
    if entries:
        await redis.delete(job_queue.JOB_LIST)
    report.queued = len(entries)


async def _drain_delayed_set(redis, report: QueueReport, cancelled: set[str]) -> None:
    """Remove every delayed job from ``ffmpeg:delayed``.

    Delayed entries are cancelled by removal - ``pop_job`` promotes them onto the
    live list, so anything left here would be re-queued later.
    """
    entries = await redis.zrange(job_queue.DELAYED_SET, 0, -1)
    for entry in entries:
        job_id = _job_id_of(entry)
        if job_id:
            cancelled.add(job_id)
            report.job_ids.append(job_id)
            await _flag_job_cancelled(redis, job_id)
    if entries:
        await redis.delete(job_queue.DELAYED_SET)
    report.delayed = len(entries)


async def _cancel_in_flight(redis, report: QueueReport, cancelled: set[str]) -> None:
    """Flag every non-terminal job hash, including jobs the workers already popped."""
    for key in await _scan_keys(redis, f"{JOB_HASH_PREFIX}*"):
        job_id = key[len(JOB_HASH_PREFIX) :]
        if not job_id:
            continue
        try:
            data = await redis.hgetall(key) or {}
        except Exception as exc:
            report.errors.append(f"{key}: {exc}")
            continue
        if not data:
            continue
        status = _text(data.get("status") or data.get(b"status") or "")
        if status in TERMINAL_STATUSES:
            continue
        cancelled.add(job_id)
        report.job_ids.append(job_id)
        await _flag_job_cancelled(redis, job_id)
        report.in_flight += 1


async def _drop_progress_keys(redis, report: QueueReport) -> None:
    """Drop the live-progress mirrors; they are derived state for cancelled jobs."""
    keys = await _scan_keys(redis, f"{PROGRESS_PREFIX}*")
    for start in range(0, len(keys), 500):
        batch = keys[start : start + 500]
        with contextlib.suppress(Exception):
            await redis.delete(*batch)
    report.progress_keys = len(keys)


async def _drop_stale_dedup_keys(redis, report: QueueReport) -> None:
    """Drop dedup keys whose job is gone or no longer active, so files can re-run.

    Mirrors ``scripts/cleanup_stale_dedup_keys.py``: a key is only removed when the
    job hash says the job is finished, cancelled or errored, and an unreadable
    state keeps the key (conservative - never un-dedup something mid-ingest).

    ``pending`` goes too. It is the placeholder an ingest writes while it is still
    deciding, and ``cancel_batch`` already drops it for its own batch's files; left
    behind by a cancel-all it keeps that file from ever being converted again until
    the key's own 24h TTL ran out, which is the "ghosted, refuses to re-run" trap
    this command exists to clear. An ingest that is genuinely mid-flight rewrites
    the key with its job id as soon as it has one, so it loses nothing real.
    """
    for key in await _scan_keys(redis, f"{DEDUP_PREFIX}*"):
        try:
            owner = _text(await redis.get(key))
        except Exception as exc:
            report.errors.append(f"{key}: {exc}")
            continue
        if not owner:
            continue
        if owner != PENDING_PLACEHOLDER and await _job_is_active(redis, owner):
            continue
        with contextlib.suppress(Exception):
            await redis.delete(key)
            report.dedup_keys += 1


async def _job_is_active(redis, job_id: str) -> bool:
    """True when ``job_id`` still owns its input. Unknown state counts as active."""
    if job_id == PENDING_PLACEHOLDER:
        return True
    try:
        data = await redis.hgetall(f"{JOB_HASH_PREFIX}{job_id}") or {}
    except Exception:
        return True
    if not data:
        return False
    status = _text(data.get("status") or data.get(b"status") or "")
    if not status:
        return False
    return status in ACTIVE_STATUSES


async def _release_stale_locks(redis, report: QueueReport, cancelled: set[str]) -> None:
    """Release input locks held by cancelled jobs or by jobs that no longer exist.

    Lock keys are derived from a hash of the input, so they cannot be enumerated
    from a job id - the owner is read out of the lock value instead.
    """
    for key in await _scan_keys(redis, f"{LOCK_PREFIX}*"):
        try:
            owner = _text(await redis.get(key))
        except Exception as exc:
            report.errors.append(f"{key}: {exc}")
            continue
        if not owner:
            continue
        if owner not in cancelled and await _job_is_active(redis, owner):
            continue
        try:
            if await job_queue.release_input_lock(key, owner, redis_client=redis):
                report.locks_released += 1
        except Exception as exc:
            report.errors.append(f"{key}: {exc}")


async def _purge_broker_queues(report: QueueReport) -> None:
    """Purge the RabbitMQ pipe, including the dead-letter queue.

    ``consumes_rabbitmq`` (not ``queue_enabled``) is the gate: it stays true when
    the rollout is 0, because a rollback still has to drain what is in the broker.
    """
    try:
        from utils.eventbus import get_settings

        settings = get_settings()
        if not settings.consumes_rabbitmq:
            return
        from utils.eventbus.rabbit import get_queue

        purged = await asyncio.wait_for(get_queue().purge_queues(), timeout=BROKER_PURGE_TIMEOUT_SECONDS)
        report.broker = {name: count for name, count in (purged or {}).items()}
    except TimeoutError:
        logger.error("cancelall: broker purge timed out after %ss", BROKER_PURGE_TIMEOUT_SECONDS)
        report.errors.append(f"broker purge timed out after {BROKER_PURGE_TIMEOUT_SECONDS:g}s")
    except Exception as exc:
        logger.exception("cancelall: broker purge failed")
        report.errors.append(f"broker purge: {exc}")


async def _delete_batch_message(bot, ref) -> bool:
    """Remove a cancelled batch's progress message from the chat.

    Best-effort on purpose: the message may already be gone (the worker deletes
    it when a batch finishes), and a failure there must not fail the cancel.
    """
    if bot is None or not ref:
        return False
    try:
        chat_id, message_id = ref
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        return True
    except Exception:
        logger.debug("cancelall: could not delete a batch progress message %s", ref)
        return False


async def _drop_stale_batches(redis, report: QueueReport, bot=None) -> None:
    """Take down every batch that no live job belongs to, and its bar.

    Run last: the steps before this one are what make a batch stale - they flag
    every queued, delayed and in-flight job. A batch whose members are all
    terminal (or gone) has nothing left to report and would otherwise sit in
    Redis, and in the chat, until its keys expired.
    """
    from utils import batch_pipeline

    summary = await batch_pipeline.purge_stale_batches(
        redis, cancelled_job_ids=report.job_ids, reason=CANCEL_REASON
    )
    report.batches = len(summary["batches"])
    report.batch_keys = int(summary["keys"])
    report.batch_tombstones = int(summary.get("tombstones", 0))
    report.batches_kept = int(summary["kept"])
    for ref in summary["messages"]:
        if await _delete_batch_message(bot, ref):
            report.batch_messages += 1


async def _sweep_ghost_work(redis, report: QueueReport) -> None:
    """Free what dead workers left behind: conversion slots, batch locks, dedups.

    Cancelling a job does not free the claim its worker took - the worker that owns
    it releases it when the job ends, and one that was killed (OOM, redeploy) never
    gets to. Until then the single global ffmpeg slot reads "busy" for every later
    job, which is the "ghost in an ffmpeg slot, refuses new work" failure, and a
    dedup key whose owner is gone is the same thing one level down: that particular
    file will not be processed again.

    A claim or dedup key whose worker still heartbeats is deliberately left alone:
    that worker is about to release it itself, and stealing the slot early would let
    a second conversion start beside the one still winding down.
    """
    from utils import batch_pipeline

    freed = await batch_pipeline.sweep_ghost_claims(redis)
    report.slots_freed = int(freed.get("slots", 0))
    report.locks_freed = int(freed.get("locks", 0))
    # The dedicated dedup pass above drops everything it can see; folding this
    # sweep's findings in means the report never under-reports what was removed.
    report.dedup_keys += int(freed.get("dedup", 0))


async def cancel_all_jobs(*, purge_broker: bool = True, bot=None) -> QueueReport:
    """Cancel every queued, delayed and in-flight job for every user on both pipes.

    Each step is isolated: one failing step is recorded in ``report.errors`` and
    the remaining steps still run, so a cancel-all never half-completes because a
    single key misbehaved.

    ``bot`` (optional) is used for the one thing Redis cannot do: deleting the
    cancelled batches' progress messages from the chat. Without it the batch
    state is still cleared, and the bars are left for Telegram to keep showing.
    """
    report = QueueReport()
    redis = None
    try:
        redis = await job_queue.get_redis()
    except Exception as exc:
        logger.exception("cancelall: Redis unavailable")
        report.errors.append(f"Redis unavailable: {exc}")

    if redis is not None:
        cancelled: set[str] = set()
        steps = (
            ("queued", _drain_job_list(redis, report, cancelled)),
            ("delayed", _drain_delayed_set(redis, report, cancelled)),
            ("in-flight", _cancel_in_flight(redis, report, cancelled)),
            ("progress keys", _drop_progress_keys(redis, report)),
            ("stale locks", _release_stale_locks(redis, report, cancelled)),
            ("dedup keys", _drop_stale_dedup_keys(redis, report)),
            # Last, because the steps above are what make every batch stale and
            # every claim's owner over.
            ("stale batches", _drop_stale_batches(redis, report, bot)),
            ("ghost work", _sweep_ghost_work(redis, report)),
        )
        for name, step in steps:
            try:
                await step
            except Exception as exc:
                logger.exception("cancelall: %s step failed", name)
                report.errors.append(f"{name}: {exc}")

    if purge_broker:
        await _purge_broker_queues(report)

    logger.info(
        "cancelall: queued=%s delayed=%s in_flight=%s batches=%s kept=%s broker=%s errors=%s",
        report.queued,
        report.delayed,
        report.in_flight,
        report.batches,
        report.batches_kept,
        report.broker,
        len(report.errors),
    )
    return report


async def _delete_prefix(redis, prefix: str, report: CacheReport) -> int:
    keys = await _scan_keys(redis, f"{prefix}*")
    removed = 0
    for start in range(0, len(keys), 500):
        batch = keys[start : start + 500]
        try:
            removed += int(await redis.delete(*batch) or 0)
        except Exception as exc:
            report.errors.append(f"{prefix}*: {exc}")
    report.prefixes[prefix] = removed
    return removed


async def _purge_media_library(report: CacheReport) -> None:
    """Delete the shared media-library objects from storage.

    Opt-in, because these are *not* cache keys: they are the uploaded inputs that
    later jobs reuse so the file is never downloaded from Telegram twice. The
    per-job ``cleanup_input`` leaves them in place on purpose, so removing them is
    an explicit cleanup rather than part of a routine cache wipe.
    """
    try:
        from utils import media_cache
        from utils.storage import get_storage_backend

        backend = await get_storage_backend()
        if backend is None:
            report.errors.append("media library purge: no storage backend configured")
            return

        prefix = media_cache.LIBRARY_KEY_PREFIX
        names = [entry.get("key") for entry in (await backend.list_keys(prefix)) or [] if entry.get("key")]

        # The local backend lists a single directory level, but the library nests
        # objects one directory deep (``<hash>/source``), so walk it too. The local
        # backend exposes its root as ``base`` (not ``base_path``).
        base = getattr(backend, "base_path", None) or getattr(backend, "base", None)
        if base:
            root = os.path.join(base, prefix.rstrip("/"))
            if os.path.isdir(root):
                for dirpath, _dirs, files in os.walk(root):
                    for name in files:
                        full = os.path.join(dirpath, name)
                        names.append(os.path.relpath(full, base).replace("\\", "/"))

        names = sorted({name for name in names if name})
        if not names:
            return
        report.media_storage_keys = int(await backend.delete_keys(names) or 0)
    except Exception as exc:
        logger.exception("clear_cache: media library purge failed")
        report.errors.append(f"media library purge: {exc}")


async def clear_cache_keys(*, clear_route_cache: bool = True, clear_media_storage: bool = False) -> CacheReport:
    """Delete every Redis cache key the app writes, optionally including route cache.

    Covers the ``utils.cache`` prefixes (job/user/meta/response and the whole
    ``cache:file:*`` media cache, descriptors and cached bodies alike) and
    ``utils.route_cache`` keys. Job state (``ffmpeg:job:*``) is never touched here
    - that is ``/cancelall``.

    With ``clear_media_storage`` the shared media-library objects in storage are
    deleted as well, so the cached media really does stop existing instead of
    just losing its Redis descriptor.
    """
    report = CacheReport()
    prefixes = (PREFIX_JOB, PREFIX_USER, PREFIX_META, PREFIX_RESPONSE)

    try:
        redis = await job_queue.get_redis()
    except Exception as exc:
        logger.exception("clear_cache: Redis unavailable")
        report.errors.append(f"Redis unavailable: {exc}")
        return report

    for prefix in prefixes:
        try:
            await _delete_prefix(redis, prefix, report)
        except Exception as exc:
            logger.exception("clear_cache: prefix %s failed", prefix)
            report.errors.append(f"{prefix}*: {exc}")

    # The media cache is `cache:file:*` (descriptors) plus `cache:file:bytes:*`
    # (cached bodies). Scanned together, reported on their own line.
    try:
        await _delete_prefix(redis, PREFIX_FILE, report)
        report.media_cache = report.prefixes.pop(PREFIX_FILE, 0)
    except Exception as exc:
        logger.exception("clear_cache: media cache wipe failed")
        report.errors.append(f"{PREFIX_FILE}*: {exc}")

    if clear_route_cache:
        try:
            await _delete_prefix(redis, ROUTE_CACHE_PREFIX, report)
            report.route_cache_keys = report.prefixes.pop(ROUTE_CACHE_PREFIX, 0)
        except Exception as exc:
            report.errors.append(f"{ROUTE_CACHE_PREFIX}*: {exc}")
        # The route cache also keeps a process-local fallback dict; with no Redis
        # client injected it only touches that dict, so one call covers both.
        try:
            from utils.route_cache import route_cache

            report.route_cache_memory = route_cache.invalidate_prefix("")
        except Exception as exc:
            report.errors.append(f"in-memory route cache: {exc}")

    if clear_media_storage:
        await _purge_media_library(report)

    logger.info(
        "clear_cache: prefixes=%s media_cache=%s media_storage=%s route_cache=%s memory=%s errors=%s",
        report.prefixes,
        report.media_cache,
        report.media_storage_keys,
        report.route_cache_keys,
        report.route_cache_memory,
        len(report.errors),
    )
    return report


def _job_id_of(raw) -> str:
    """Extract ``job_id`` from a raw ``ffmpeg:jobs`` / ``ffmpeg:delayed`` entry."""
    text = _text(raw)
    if not text:
        return ""
    try:
        parsed = json.loads(text)
    except Exception:
        return ""
    if not isinstance(parsed, dict):
        return ""
    return str(parsed.get("job_id") or "")
