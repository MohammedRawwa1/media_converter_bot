"""Prometheus counters for the event-bus layer.

Metrics are the only honest way to claim anything about delivery reliability, so
the layer counts what actually happened: which queue a job was routed to, whether
the broker accepted it, and how often a message was retried or dead-lettered.
Registration is wrapped so that a metrics mishap can never break job processing.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Counter

    JOBS_ROUTED = Counter("eventbus_jobs_routed_total", "Jobs routed to a queue backend", ["backend"])
    JOBS_ROUTED_FALLBACK = Counter(
        "eventbus_jobs_routed_fallback_total", "Jobs that fell back to Redis after a broker problem", ["reason"]
    )
    EVENTS_PUBLISHED = Counter("eventbus_events_published_total", "Lifecycle events accepted by Kafka", ["type"])
    EVENTS_FAILED = Counter("eventbus_events_publish_failed_total", "Lifecycle events Kafka refused", ["type"])
    QUEUE_RETRIES = Counter("eventbus_queue_retries_total", "Messages re-published for another attempt")
    QUEUE_DEAD_LETTERED = Counter("eventbus_queue_dead_lettered_total", "Messages moved to the dead-letter queue")
except Exception:  # pragma: no cover - prometheus_client is a hard dependency, kept optional anyway
    Counter = None  # type: ignore[assignment]

    class _Noop:
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

    JOBS_ROUTED = JOBS_ROUTED_FALLBACK = EVENTS_PUBLISHED = EVENTS_FAILED = _Noop()  # type: ignore[assignment]
    QUEUE_RETRIES = QUEUE_DEAD_LETTERED = _Noop()  # type: ignore[assignment]


def inc(counter, *label_values: str) -> None:
    """Increment a counter, never raising into the caller's hot path."""
    try:
        target = counter.labels(*label_values) if label_values else counter
        target.inc()
    except Exception:
        logger.debug("eventbus: metric increment failed")
