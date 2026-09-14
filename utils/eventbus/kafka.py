"""Kafka adapter: the durable, replayable job-lifecycle event log.

Why the existing pub/sub is not the same thing

Job progress is currently published with Redis ``PUBLISH``. That is
fire-and-forget: a message published while nobody is subscribed is gone, and a
subscriber that disconnects mid-copy misses everything until it reconnects. It
is a fine notification mechanism and a poor record.

Kafka here is the **record**: one topic, one event per lifecycle transition
(queued, started, optional progress, completed/failed/cancelled), keyed by job id
so a job's events stay in order within a partition. That gives:

* **replay** - a consumer can re-read a job's history from the log, which the
  Redis channel cannot do;
* **independent consumers** - analytics, alerting or an audit trail can read the
  same events without the worker knowing they exist;
* **a bounded gap** - retention is the log's, not a subscriber's uptime.

Publishing never blocks the caller: a broker problem is counted and logged, and
the job path continues exactly as it did before. Delivery reliability is claimed
only where it is real - Kafka acks + idempotent producer - and the counters in
``utils.eventbus.metrics`` are what would substantiate any number.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import tempfile
import time

from utils.eventbus import metrics
from utils.eventbus.config import EventBusSettings, get_settings
from utils.eventbus.messages import encode

logger = logging.getLogger(__name__)

try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from aiokafka.helpers import create_ssl_context
except Exception:  # pragma: no cover - optional dependency
    AIOKafkaConsumer = None
    AIOKafkaProducer = None
    create_ssl_context = None


def ssl_context_for(settings: EventBusSettings):
    """Return a verifying SSLContext for SSL/SASL_SSL, or ``None`` otherwise.

    aiokafka refuses the TLS protocols without an explicit context - it raises
    "`ssl_context` is mandatory if security_protocol=='SSL'" - and every managed
    broker requires TLS, so without this the producer never started and all
    events were dropped while its config looked correct.

    ``KAFKA_SSL_CAFILE`` supplies a private CA file (e.g. Aiven's project CA).
    Deployments can instead provide the PEM contents in ``AIVEN_CA_CERT``;
    that value is written to a temporary file only while the SSL context loads.
    With both unset the system trust store is used. Hostname checking stays on.
    """
    if settings.kafka_security_protocol not in ("SSL", "SASL_SSL"):
        return None
    if create_ssl_context is None:  # pragma: no cover - aiokafka not installed
        return None
    temporary_cafile = None
    cafile = settings.kafka_ssl_cafile or None
    if cafile is None:
        certificate = os.getenv("AIVEN_CA_CERT", "").strip()
        if certificate:
            with tempfile.NamedTemporaryFile("w", suffix=".pem", encoding="utf-8", delete=False) as handle:
                handle.write(certificate.replace("\\n", "\n"))
                temporary_cafile = handle.name
            cafile = temporary_cafile
    try:
        return create_ssl_context(cafile=cafile)
    finally:
        if temporary_cafile:
            with contextlib.suppress(OSError):
                os.unlink(temporary_cafile)


def security_kwargs(settings: EventBusSettings) -> dict:
    """TLS/SASL kwargs shared by the producer and the reader."""
    kwargs: dict = {}
    if settings.kafka_security_protocol:
        kwargs["security_protocol"] = settings.kafka_security_protocol
    context = ssl_context_for(settings)
    if context is not None:
        kwargs["ssl_context"] = context
    if settings.kafka_sasl_mechanism:
        kwargs["sasl_mechanism"] = settings.kafka_sasl_mechanism
        kwargs["sasl_plain_username"] = settings.kafka_sasl_username
        kwargs["sasl_plain_password"] = settings.kafka_sasl_password
    return kwargs


def producer_kwargs(settings: EventBusSettings) -> dict:
    """Keyword arguments for the producer (testable without a live broker)."""
    return {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "client_id": settings.kafka_client_id,
        "acks": settings.kafka_acks,
        # Idempotent producer: retries cannot reorder or duplicate within a
        # partition, which is what makes "at least once" usable downstream.
        "enable_idempotence": True,
        "linger_ms": settings.kafka_linger_ms,
        **security_kwargs(settings),
    }


def consumer_kwargs(settings: EventBusSettings) -> dict:
    """Keyword arguments for the reader, with the same TLS/SASL rules."""
    return {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "client_id": f"{settings.kafka_client_id}-reader",
        "auto_offset_reset": "earliest",
        **security_kwargs(settings),
    }


def progress_allowed(last_sent: float | None, now: float, min_interval_ms: int) -> bool:
    """Pure throttle decision for progress events (attempted at most once per interval)."""
    if min_interval_ms <= 0:
        return True
    if last_sent is None:
        return True
    return (now - last_sent) * 1000.0 >= min_interval_ms


class KafkaEventBus:
    """Producer (and optional reader) for the job-event log."""

    def __init__(self, settings: EventBusSettings | None = None, producer_factory=None):
        self._settings = settings
        self._producer = None
        self._producer_factory = producer_factory
        self._lock = asyncio.Lock()
        self._progress_seen: dict[str, float] = {}
        # Counters for diagnostics (surfaced by scripts/check_eventbus.py).
        self.published = 0
        self.failed = 0
        self.suppressed = 0

    @property
    def settings(self) -> EventBusSettings:
        return self._settings or get_settings()

    def available(self) -> bool:
        return (self._producer_factory is not None or AIOKafkaProducer is not None) and bool(
            self.settings.kafka_bootstrap_servers
        )

    def _build_producer(self):
        if self._producer_factory is not None:
            return self._producer_factory(self.settings)
        return AIOKafkaProducer(**producer_kwargs(self.settings))

    async def start(self) -> bool:
        """Start the producer (idempotent). Returns True when it is usable."""
        if not self.available():
            return False
        async with self._lock:
            if self._producer is not None:
                return True
            try:
                producer = self._build_producer()
                await producer.start()
                self._producer = producer
                logger.info(
                    "eventbus: Kafka producer ready (client_id=%s topic=%s)",
                    self.settings.kafka_client_id,
                    self.settings.kafka_topic,
                )
                return True
            except Exception as exc:
                logger.warning("eventbus: Kafka producer unavailable (%s); events are dropped", exc)
                self._producer = None
                return False

    async def verify_publish(self, event: dict, timeout_s: float = 10.0) -> tuple[bool, str]:
        """Start the producer if needed, then send ``event`` and wait for the ack.

        ``start()`` alone only proves the bootstrap connection works: a missing
        topic or a write ACL denial shows up only when a message is actually
        sent, which is the failure this exists to catch. Returns ``(ok, detail)``
        and never raises, so the caller decides whether to log or abort.
        """
        if not self.available():
            return False, "aiokafka is not installed or KAFKA_BOOTSTRAP_SERVERS is empty"
        try:
            started = await asyncio.wait_for(self.start(), timeout=timeout_s)
        except TimeoutError:
            return False, f"producer did not start within {timeout_s:.0f}s"
        except Exception as exc:
            return False, f"producer start failed: {type(exc).__name__}: {exc}"
        if not started or self._producer is None:
            return False, "producer could not start (check bootstrap servers, SASL and TLS settings)"
        try:
            await asyncio.wait_for(
                self._producer.send_and_wait(self.settings.kafka_topic, value=encode(event), key=None),
                timeout=timeout_s,
            )
        except TimeoutError:
            return False, f"publishing to {self.settings.kafka_topic!r} timed out after {timeout_s:.0f}s"
        except Exception as exc:
            return False, f"publishing to {self.settings.kafka_topic!r} failed: {type(exc).__name__}: {exc}"
        self.published += 1
        return True, f"probe accepted by {self.settings.kafka_topic!r}"

    async def close(self) -> None:
        with contextlib.suppress(Exception):
            if self._producer is not None:
                await self._producer.stop()
        self._producer = None

    async def emit(self, event: dict) -> bool:
        """Publish one event. Never raises - a logging failure must not fail a job."""
        event_type = str(event.get("type") or "?")
        if not self.settings.events_enabled:
            return False
        try:
            if self._producer is None and not await self.start():
                return False
            key = str(event.get("job_id") or "").encode("utf-8") or None
            await self._producer.send_and_wait(self.settings.kafka_topic, value=encode(event), key=key)
            self.published += 1
            metrics.inc(metrics.EVENTS_PUBLISHED, event_type)
            return True
        except Exception as exc:
            self.failed += 1
            metrics.inc(metrics.EVENTS_FAILED, event_type)
            logger.warning("eventbus: could not publish %s event (%s)", event_type, exc)
            return False

    async def emit_progress(self, event: dict, now: float | None = None) -> bool:
        """Publish a progress event, throttled to one per configured interval per job.

        Progress is high-volume by nature, so it is capped rather than turned
        off: the log still shows progress moving, at a bounded rate.
        """
        if not self.settings.events_enabled or not self.settings.emit_progress_events:
            self.suppressed += 1
            return False
        job_id = str(event.get("job_id") or "")
        moment = time.time() if now is None else now
        last = self._progress_seen.get(job_id)
        if not progress_allowed(last, moment, self.settings.progress_min_interval_ms):
            self.suppressed += 1
            return False
        self._progress_seen[job_id] = moment
        # Keep the map from growing without bound during a long uptime.
        if len(self._progress_seen) > 5000:
            cutoff = moment - 3600
            self._progress_seen = {k: v for k, v in self._progress_seen.items() if v >= cutoff}
        return await self.emit(event)

    def forget_progress(self, job_id: str) -> None:
        """Drop throttle state for a finished job."""
        self._progress_seen.pop(str(job_id or ""), None)

    async def read_events(self, job_id: str = "", limit: int = 200, timeout_ms: int = 5000) -> list[dict]:
        """Read events back from the log (audit/replay). Used by the check script."""
        if AIOKafkaConsumer is None or not self.settings.kafka_bootstrap_servers:
            return []
        from utils.eventbus.messages import decode

        consumer = AIOKafkaConsumer(self.settings.kafka_topic, **consumer_kwargs(self.settings))
        found: list[dict] = []
        await consumer.start()
        try:
            # One bounded pass with a deadline, so an empty or short topic ends
            # the read instead of looping on a consumer timeout.
            deadline = time.monotonic() + max(0.1, timeout_ms / 1000.0)
            while len(found) < limit:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    message = await asyncio.wait_for(consumer.getone(), timeout=remaining)
                except (TimeoutError, StopAsyncIteration):
                    break
                try:
                    event = decode(message.value)
                except Exception:
                    continue
                if job_id and event.get("job_id") != job_id:
                    continue
                found.append(event)
        except Exception as exc:
            logger.warning("eventbus: reading the log failed (%s)", exc)
        finally:
            with contextlib.suppress(Exception):
                await consumer.stop()
        return found


_BUS: KafkaEventBus | None = None


def get_bus(settings: EventBusSettings | None = None) -> KafkaEventBus:
    """Return the process-wide event bus."""
    global _BUS
    if _BUS is None:
        _BUS = KafkaEventBus(settings)
    return _BUS


async def close_bus() -> None:
    """Stop the shared producer (call at shutdown)."""
    global _BUS
    if _BUS is not None:
        await _BUS.close()
        _BUS = None
