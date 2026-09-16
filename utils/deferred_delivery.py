# utils/deferred_delivery.py
"""A Redis-backed queue for Telegram deliveries a flood window pushed past.

A conversion can finish while Telegram is refusing writes to the chat it has to
be delivered to. Sending is the *last* step, so before this existed the worker
marked the job ``error``/``delivery failed`` and walked away - the user never got
a file that had already been downloaded, converted and paid for, even though the
window was going to close on its own within the hour.

The record here is deliberately dumb: it holds everything a later, delivery-only
retry needs (where the output is, where it goes, what it is called) plus the
moment it becomes eligible. The worker owns the sending; this module only stores
and hands back. Nothing is deleted until an attempt has actually resolved -
succeeded, or run out of attempts - so a crash mid-send loses at most the lease,
not the delivery.

``output`` is a local path and ``output_key`` an optional storage key. Local is
the one that matters in the common case: a result that is about to be sent to
Telegram from the same container is never uploaded (see ``_needs_remote_copy`` in
the worker), so the file on disk is the only copy there is.
"""

import contextlib
import json
import logging
import os
import time

try:
    from utils.job_queue import get_redis
except Exception:  # pragma: no cover - only when the job queue is unavailable
    get_redis = None

logger = logging.getLogger(__name__)

# job_id -> due time (unix seconds). A member is only removed once its delivery
# has been resolved, so a sweep that dies leaves the delivery in place.
DUE_KEY = "ffmpeg:delivery_deferred"
RECORD_PREFIX = "ffmpeg:delivery_deferred:"

# How long a deferred delivery may sit before it is abandoned. The local output
# is swept after 24h (``cleanup_tasks.max_file_age``) and objects under
# ``outputs/`` expire on the same clock, so waiting past that would only retry a
# file that no longer exists.
DEFAULT_HORIZON_SECONDS = float(os.getenv("DEFERRED_DELIVERY_HORIZON_SECONDS", "43200"))
MAX_ATTEMPTS = int(os.getenv("DEFERRED_DELIVERY_MAX_ATTEMPTS", "5"))
# Pushed forward when a record is claimed, so a second sweep (or a second worker)
# cannot send the same file while the first attempt is still running.
CLAIM_LEASE_SECONDS = float(os.getenv("DEFERRED_DELIVERY_LEASE_SECONDS", "600"))
RECORD_TTL_SECONDS = float(os.getenv("DEFERRED_DELIVERY_TTL_SECONDS", str(int(DEFAULT_HORIZON_SECONDS + 3600))))

# Never wake earlier than this, even if a window says so: retrying into a window
# that is about to close is just a wasted round trip.
MIN_DUE_DELAY_SECONDS = 5.0


def _record_key(job_id: str) -> str:
    return f"{RECORD_PREFIX}{job_id}"


async def _client(client=None):
    """The Redis client to use, or None when Redis is not reachable."""
    if client is not None:
        return client
    factory = globals().get("get_redis")
    if factory is None:
        return None
    try:
        return await factory()
    except Exception:
        logger.debug("deferred delivery: no Redis client available")
        return None


async def defer(
    record: dict,
    *,
    due_in: float,
    horizon: float | None = None,
    client=None,
) -> bool:
    """Store *record* and make it eligible again in ``due_in`` seconds.

    Returns True when the record was persisted. An unreachable Redis returns
    False and the caller keeps its old (failed) outcome - a delivery that cannot
    be recorded is not a delivery this process may promise.
    """
    job_id = record.get("job_id")
    if not job_id:
        return False

    delay = max(MIN_DUE_DELAY_SECONDS, float(due_in or 0))
    now = time.time()
    payload = dict(record)
    payload.setdefault("attempts", 0)
    payload.setdefault("created_at", now)
    payload["deferred_at"] = now
    payload["horizon"] = float(horizon if horizon is not None else DEFAULT_HORIZON_SECONDS)
    payload["due_at"] = now + delay

    client = await _client(client)
    if client is None:
        return False
    try:
        await client.set(_record_key(job_id), json.dumps(payload, default=str), ex=int(RECORD_TTL_SECONDS))
        await client.zadd(DUE_KEY, {job_id: payload["due_at"]})
        logger.info(
            "deferred delivery: job %s will retry in %.0fs (attempt %s/%s, reason=%s)",
            job_id,
            delay,
            payload["attempts"],
            MAX_ATTEMPTS,
            payload.get("reason") or "unknown",
        )
        return True
    except Exception:
        logger.exception("deferred delivery: could not record job %s", job_id)
        return False
    finally:
        await _release(client)


async def claim_due(*, limit: int = 5, lease: float | None = None, client=None) -> list[dict]:
    """Records that are due, each pushed out by a lease so it is not re-sent.

    Returns the decoded payloads. A record whose payload has expired is dropped
    from the queue rather than retried forever.
    """
    client = await _client(client)
    if client is None:
        return []
    try:
        now = time.time()
        raw_ids = await client.zrangebyscore(DUE_KEY, "-inf", now, start=0, num=max(1, int(limit)))
        if not raw_ids:
            return []
        lease_until = now + (CLAIM_LEASE_SECONDS if lease is None else float(lease))
        records: list[dict] = []
        for raw_id in raw_ids:
            job_id = raw_id.decode() if isinstance(raw_id, (bytes, bytearray)) else str(raw_id)
            payload_raw = await client.get(_record_key(job_id))
            if not payload_raw:
                # TTL ran out: the payload is gone, so the queue entry is noise.
                await client.zrem(DUE_KEY, job_id)
                continue
            try:
                payload = json.loads(payload_raw)
            except Exception:
                logger.warning("deferred delivery: unreadable record for job %s; dropping", job_id)
                await _forget(client, job_id)
                continue
            payload["job_id"] = payload.get("job_id") or job_id
            await client.zadd(DUE_KEY, {job_id: lease_until})
            records.append(payload)
        return records
    except Exception:
        logger.exception("deferred delivery: could not read the queue")
        return []
    finally:
        await _release(client)


async def update(job_id: str, *, due_in: float | None = None, **fields) -> bool:
    """Merge ``fields`` into a record, optionally moving its due time."""
    client = await _client()
    if client is None or not job_id:
        return False
    try:
        raw = await client.get(_record_key(job_id))
        if not raw:
            return False
        try:
            payload = json.loads(raw)
        except Exception:
            payload = {"job_id": job_id}
        payload.update(fields)
        payload["job_id"] = job_id
        due_at = None
        if due_in is not None:
            due_at = time.time() + max(MIN_DUE_DELAY_SECONDS, float(due_in))
            payload["due_at"] = due_at
        await client.set(_record_key(job_id), json.dumps(payload, default=str), ex=int(RECORD_TTL_SECONDS))
        if due_at is not None:
            await client.zadd(DUE_KEY, {job_id: due_at})
        return True
    except Exception:
        logger.exception("deferred delivery: could not update job %s", job_id)
        return False
    finally:
        await _release(client)


async def clear(job_id: str, client=None) -> None:
    """Drop a record: the delivery resolved one way or the other."""
    client = await _client(client)
    if client is None or not job_id:
        return
    try:
        await _forget(client, job_id)
    finally:
        await _release(client)


async def pending(client=None) -> int:
    """How many deliveries are waiting (for health/metrics output)."""
    client = await _client(client)
    if client is None:
        return 0
    try:
        return int(await client.zcard(DUE_KEY) or 0)
    except Exception:
        return 0
    finally:
        await _release(client)


async def _forget(client, job_id: str) -> None:
    with contextlib.suppress(Exception):
        await client.delete(_record_key(job_id))
    with contextlib.suppress(Exception):
        await client.zrem(DUE_KEY, job_id)


async def _release(client) -> None:
    """Return the shared client (``job_queue``'s proxy close is a no-op)."""
    with contextlib.suppress(Exception):
        await client.close()
