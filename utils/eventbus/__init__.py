"""Optional event-bus layer: RabbitMQ for job execution, Kafka for job events.

Everything the application needs is in this module, and every function is safe to
call when the layer is disabled - that is the point. Callers do not branch on
configuration; they call ``publish_job`` / ``emit_event`` and the layer either
does the work or does nothing:

* ``publish_job(job)``            -> ``True`` only when RabbitMQ took the job.
  A ``False`` return means "queue it the way you always did" (the Redis list),
  so a broker outage degrades into the existing path instead of losing a job.
* ``emit_event(...)``             -> best-effort log write, never raises.
* ``consume_jobs(handler, ...)``  -> the worker's RabbitMQ consumer.
* ``requeue_job(job)``            -> sends a job back for another attempt on
  whichever backend it came from.

Rollout is per job and decided by a stable hash (see ``config.queue_backend_for_job``),
so a deployment can send 5% of jobs through the broker, watch them, then raise
``EVENTBUS_QUEUE_ROLLOUT_PERCENT`` - with both queues drained meanwhile.
"""

from __future__ import annotations

import logging

from utils.eventbus.config import (
    EventBusSettings,
    get_settings,
    kafka_available,
    load_settings,
    queue_backend_for_job,
    rabbitmq_available,
    reset_settings_cache,
)
from utils.eventbus.kafka import get_bus
from utils.eventbus.messages import (
    EVENTS_PROBE,
    JOB_CANCELLED,
    JOB_COMPLETED,
    JOB_FAILED,
    JOB_PROGRESS,
    JOB_QUEUED,
    JOB_STARTED,
    TERMINAL_EVENTS,
    job_bucket,
    job_summary,
    new_event,
)
from utils.eventbus.rabbit import get_queue, is_rabbitmq_job

logger = logging.getLogger(__name__)

__all__ = [
    "JOB_CANCELLED",
    "JOB_COMPLETED",
    "JOB_FAILED",
    "JOB_PROGRESS",
    "JOB_QUEUED",
    "EVENTS_PROBE",
    "JOB_STARTED",
    "TERMINAL_EVENTS",
    "EventBusSettings",
    "close_eventbus",
    "consume_jobs",
    "describe",
    "emit_event",
    "get_settings",
    "job_bucket",
    "job_summary",
    "load_settings",
    "publish_job",
    "queue_backend_for_job",
    "requeue_job",
    "reset_settings_cache",
    "verify_events_startup",
]


async def publish_job(job: dict) -> bool:
    """Queue a job on RabbitMQ when its rollout bucket says so.

    Returns ``True`` when the broker accepted the message, so the caller must
    **not** also push it to Redis. Returns ``False`` in every other case
    (rollout, disabled, unavailable, publish failure) and the caller queues it
    as before.
    """
    try:
        if queue_backend_for_job(job) != "rabbitmq":
            return False
        return await get_queue().publish_job(job)
    except Exception:
        logger.exception("eventbus: publish_job failed; caller should use the Redis queue")
        return False


async def requeue_job(job: dict) -> bool:
    """Put a job back for another attempt on the backend it came from.

    Returns ``True`` when it was requeued on RabbitMQ, ``False`` when the caller
    has to use the Redis delayed set (its original behaviour).
    """
    try:
        if not is_rabbitmq_job(job):
            return False
        return await get_queue().requeue_job(job)
    except Exception:
        logger.exception("eventbus: requeue_job failed")
        return False


async def emit_event(
    event_type: str,
    *,
    job: dict | None = None,
    job_id: str = "",
    payload: dict | None = None,
    source: str = "",
) -> bool:
    """Publish one lifecycle event. Never raises; returns True when Kafka took it."""
    try:
        settings = get_settings()
        if not settings.events_enabled:
            return False
        resolved_job_id = str(job_id or (job or {}).get("job_id") or "")
        event = new_event(
            event_type,
            job_id=resolved_job_id,
            request_id=str((job or {}).get("request_id") or ""),
            source=source,
            payload={**job_summary(job), **(payload or {})},
        )
        bus = get_bus()
        if event_type == JOB_PROGRESS:
            return await bus.emit_progress(event)
        sent = await bus.emit(event)
        if sent and event_type in TERMINAL_EVENTS:
            bus.forget_progress(resolved_job_id)
        return sent
    except Exception:
        logger.debug("eventbus: emit_event failed", exc_info=True)
        return False


async def verify_events_startup() -> bool:
    """Prove at startup that an enabled Kafka event log can actually be written to.

    A broken event log used to be invisible: the layer reported
    ``events_enabled: true`` and then dropped every event from a debug-level log,
    so the first symptom was "the topic is empty". This starts the producer and
    publishes one probe event, waiting for the broker's ack, which surfaces a
    missing topic, a wrong SASL password, an untrusted CA or an empty bootstrap
    list at boot instead.

    Returns ``True`` when events are disabled or the probe was accepted, ``False``
    (after an ERROR log) when they are enabled but unusable. Raises
    :class:`RuntimeError` instead when ``EVENTBUS_REQUIRE_BROKERS`` is set - the
    one configuration in which running without the event log is not acceptable.
    """
    settings = get_settings()
    if not settings.events_enabled:
        return True
    probe = new_event(EVENTS_PROBE, source="startup")
    ok, detail = await get_bus().verify_publish(probe)
    if ok:
        logger.info("eventbus: Kafka event log verified (%s)", detail)
        return True
    message = (
        f"Kafka events are enabled but cannot be published, so every event will be dropped: {detail}. "
        f"Check KAFKA_BOOTSTRAP_SERVERS, KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_*, KAFKA_SSL_CAFILE, "
        f"and that the topic {settings.kafka_topic!r} exists."
    )
    if settings.require_brokers:
        raise RuntimeError(f"eventbus: {message}")
    logger.error("eventbus: %s", message)
    return False


async def consume_jobs(handler, stop_event=None) -> None:
    """Consume jobs from RabbitMQ, calling ``handler(job)`` per message.

    The handler's outcome is what the broker acts on: returning normally acks the
    message, raising retries it and eventually dead-letters it.
    """
    await get_queue().consume(handler, stop_event=stop_event)


async def close_eventbus() -> None:
    """Close both adapters (call at process shutdown)."""
    from utils.eventbus.kafka import close_bus
    from utils.eventbus.rabbit import close_queue

    for closer in (close_queue, close_bus):
        try:
            await closer()
        except Exception:
            logger.debug("eventbus: shutdown step failed", exc_info=True)


def describe() -> dict:
    """Return a log-safe summary of the resolved configuration and client libs."""
    settings = get_settings()
    return {
        **settings.describe(),
        "aio_pika_installed": rabbitmq_available(),
        "aiokafka_installed": kafka_available(),
        "consumes_redis": settings.consumes_redis,
        "consumes_rabbitmq": settings.consumes_rabbitmq,
    }
