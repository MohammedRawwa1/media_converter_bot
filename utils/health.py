"""Health probes for both background pipes, in the shape both bots now report.

The two deployments queue work on different pairs of pipes, so a health payload
that only proves "the process is up" hides the failure that actually matters: a
reachable Redis but an unreachable broker (this bot), or a stuck RQ queue (the
reference bot). This module answers the same four questions in both bots:

1. Is Redis reachable, and how fast does it answer? -> ``redis.ping_ms``
2. How much work is waiting on the Redis pipe?      -> ``redis.queue_depth`` / ``.delayed``
3. How much work is waiting on the broker pipe?     -> ``broker.queues``
4. How is the event bus configured?                 -> ``eventbus`` (describe())

Contract, deliberately the same as ``utils/queue_admin``:

* ``collect_health`` **never raises** and every probe is bounded by a timeout, so a
  dead Redis or a hung broker degrades one field instead of hanging the request.
* The overall ``status`` is ``degraded`` when a pipe that should be serving is
  unusable, and ``ok`` otherwise. Callers always answer HTTP 200: this endpoint
  backs the platform healthcheck and a keep-alive ping, so turning a partially
  degraded bot into a non-200 would restart a container that is still converting
  files, or let a free-tier instance be spun down.
"""

from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger(__name__)

# Bounded so /health always answers quickly even when a dependency is hung. The
# broker probe is the slower one because it re-declares its queues over the
# network before reading their depths.
PROBE_TIMEOUT_SECONDS = 2.0
BROKER_TIMEOUT_SECONDS = 3.0


async def collect_health() -> dict:
    """Probe both pipes and return the shared health payload.

    Returns ``{"status", "redis", "broker", "eventbus"}``. ``status`` is
    ``"degraded"`` when Redis is unreachable or the broker pipe (when the bot is
    configured to use one) failed to answer.
    """
    redis_probe = await _redis_probe()
    broker_probe = await _broker_probe()
    return {
        "status": _overall_status(redis_probe, broker_probe),
        "redis": redis_probe,
        "broker": broker_probe,
        "eventbus": _eventbus_describe(),
    }


def _overall_status(redis_probe: dict, broker_probe: dict) -> str:
    """``degraded`` when a pipe that should be serving cannot, else ``ok``.

    ``broker["connected"] is None`` means "not configured", which is healthy: a
    Redis-only deployment has no broker to be degraded about.
    """
    if not redis_probe.get("connected"):
        return "degraded"
    if broker_probe.get("connected") is False:
        return "degraded"
    return "ok"


async def _redis_probe() -> dict:
    """Ping Redis and read the depths of the two Redis-side queue keys."""
    from utils.job_queue import DELAYED_SET, JOB_LIST, get_redis

    probe: dict = {"connected": False, "ping_ms": None, "queue_depth": None, "delayed": None, "error": None}
    try:
        # The shared client is a process-wide proxy whose close() is a no-op, so
        # there is deliberately nothing to close here.
        client = get_redis()
        client = await asyncio.wait_for(client, timeout=PROBE_TIMEOUT_SECONDS)
    except Exception as exc:
        probe["error"] = f"redis unavailable: {exc}"
        return probe

    try:
        started = time.perf_counter()
        await asyncio.wait_for(client.ping(), timeout=PROBE_TIMEOUT_SECONDS)
        probe["ping_ms"] = round((time.perf_counter() - started) * 1000, 2)
        probe["connected"] = True
        probe["queue_depth"] = int(await asyncio.wait_for(client.llen(JOB_LIST), timeout=PROBE_TIMEOUT_SECONDS))
        probe["delayed"] = int(await asyncio.wait_for(client.zcard(DELAYED_SET), timeout=PROBE_TIMEOUT_SECONDS))
    except Exception as exc:
        probe["error"] = f"{type(exc).__name__}: {exc}"
        logger.warning("health: redis probe failed: %s", exc)
    return probe


async def _broker_probe() -> dict:
    """Report the broker pipe: which backend, reachable or not, and queue depths.

    ``backend`` is ``"rabbitmq"`` when jobs are routed through the broker,
    ``"redis"`` when every job uses the Redis list, and the probe then reports
    ``connected: None`` - "no broker configured" is not a degradation.
    """
    from utils.eventbus import get_settings

    probe: dict = {"backend": "disabled", "connected": None, "queues": {}, "error": None}
    try:
        settings = get_settings()
    except Exception as exc:
        probe["error"] = f"event bus settings unavailable: {exc}"
        return probe

    # consumes_rabbitmq (not queue_enabled) is the gate: during a rollout rollback
    # nothing new goes to the broker, but whatever is already queued there still
    # has to be visible - and drainable - so /health must report it.
    if not settings.consumes_rabbitmq:
        probe["backend"] = "redis"
        return probe

    probe["backend"] = "rabbitmq"
    try:
        from utils.eventbus.rabbit import get_queue

        queues = await asyncio.wait_for(get_queue().stats(), timeout=BROKER_TIMEOUT_SECONDS)
        probe["queues"] = queues
        # stats() reports a string ("unavailable: ...") for a queue it could not
        # read, so accept the pipe only when every depth came back as a number.
        probe["connected"] = bool(queues) and all(isinstance(depth, int) for depth in queues.values())
    except Exception as exc:
        probe["connected"] = False
        probe["error"] = f"broker unavailable: {exc}"
        logger.warning("health: broker probe failed: %s", exc)
    return probe


def _eventbus_describe() -> dict | None:
    """Resolved event-bus configuration (never secrets), or None when unusable."""
    try:
        from utils.eventbus import describe

        return describe()
    except Exception as exc:
        logger.warning("health: eventbus describe failed: %s", exc)
        return None
