"""Who is using the bot right now.

The bot reports "users online" from a Redis heartbeat instead of the in-memory
session map, because that map only ever knows about the worker that happens to
serve the request: with two processes behind one bot, each one would claim to
see a different (and smaller) set of users.

Contract, deliberately the same spirit as ``utils.health``:

* ``touch`` is best-effort and never raises - it runs on the hot path for every
  update, so a Redis problem must cost a debug log, never a reply.
* ``snapshot`` degrades to whatever this process has seen locally when Redis is
  unreachable, and says so via ``source``, instead of pretending nobody is
  online. Counts read from a fallback are explicitly marked so the caller can
  label them as approximate.
"""

from __future__ import annotations

import contextlib
import logging
import os
import time

logger = logging.getLogger(__name__)

# Presence marker per user: ``ffmpeg:online:<user_id>`` -> last-seen epoch.
# The TTL is what makes "online" mean "active recently" rather than "ever seen":
# a user drops out of the count once they stop interacting for this long.
PRESENCE_PREFIX = "ffmpeg:online:"
PRESENCE_TTL_SECONDS = int(os.getenv("PRESENCE_TTL_SECONDS", "300"))
# ``touch`` runs on every update, so writes are throttled per user: a chatty user
# costs one Redis SET per interval, not one per message. The marker's TTL is far
# longer than this, so the heartbeat never lapses between writes.
PRESENCE_TOUCH_INTERVAL = float(os.getenv("PRESENCE_TOUCH_INTERVAL", "30"))

# Fallback store used only while Redis cannot be reached. Process-local, so it
# under-reports behind multiple workers - which is exactly why ``snapshot``
# reports ``source="memory"`` when it answers from here.
_memory_seen: dict[int, float] = {}
# Last time each user's heartbeat was written to Redis (throttle bookkeeping).
_last_written: dict[int, float] = {}


def _now() -> float:
    return time.time()


def _int_or_none(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _prune_memory(now: float) -> None:
    """Drop fallback entries older than the presence TTL."""
    cutoff = now - PRESENCE_TTL_SECONDS
    for user_id in [uid for uid, seen in _memory_seen.items() if seen < cutoff]:
        _memory_seen.pop(user_id, None)


async def touch(user_id) -> None:
    """Record that ``user_id`` is active now. Best-effort; never raises."""
    uid = _int_or_none(user_id)
    if uid is None:
        return

    now = _now()
    _memory_seen[uid] = now
    if len(_memory_seen) > 1000:
        _prune_memory(now)

    if now - _last_written.get(uid, 0.0) < PRESENCE_TOUCH_INTERVAL:
        return
    _last_written[uid] = now

    try:
        from utils.job_queue import get_redis

        client = await get_redis()
        try:
            await client.setex(f"{PRESENCE_PREFIX}{uid}", PRESENCE_TTL_SECONDS, str(now))
        finally:
            with contextlib.suppress(Exception):
                await client.close()
    except Exception:
        # Redis unavailable: the in-memory fallback above still answers.
        logger.debug("presence: could not record heartbeat for %s", uid)


async def snapshot() -> dict:
    """Return ``{"count", "user_ids", "source", "error"}`` for the live heartbeat.

    ``source`` is ``"redis"`` when the shared client answered (counts span every
    worker) or ``"memory"`` when only this process could be consulted.
    """
    try:
        from utils.job_queue import get_redis

        client = await get_redis()
    except Exception as exc:
        return _memory_snapshot(error=f"redis unavailable: {exc}")

    try:
        user_ids: list[int] = []
        try:
            async for key in client.scan_iter(match=f"{PRESENCE_PREFIX}*", count=500):
                uid = _int_or_none(_text(key)[len(PRESENCE_PREFIX) :])
                if uid is not None:
                    user_ids.append(uid)
        finally:
            with contextlib.suppress(Exception):
                await client.close()
        user_ids.sort()
        return {"count": len(user_ids), "user_ids": user_ids, "source": "redis", "error": None}
    except Exception as exc:
        logger.debug("presence: redis scan failed: %s", exc)
        return _memory_snapshot(error=f"{type(exc).__name__}: {exc}")


def _memory_snapshot(*, error: str | None) -> dict:
    now = _now()
    _prune_memory(now)
    user_ids = sorted(_memory_seen)
    return {"count": len(user_ids), "user_ids": user_ids, "source": "memory", "error": error}


def _text(value) -> str:
    """Return a Redis value as ``str`` whether the client decodes or not."""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    return "" if value is None else str(value)


def reset_memory() -> None:
    """Clear the fallback heartbeat store (tests and process restarts)."""
    _memory_seen.clear()
    _last_written.clear()
