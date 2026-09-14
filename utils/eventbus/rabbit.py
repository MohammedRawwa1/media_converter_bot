"""RabbitMQ adapter: the durable work queue that runs FFmpeg jobs.

Why a broker replaces the Redis list for job *execution*

The existing queue is ``LPUSH`` + ``BRPOP``. A job is removed from the list at
the moment it is handed to a worker, so a worker that is killed mid-encode (a
deploy, an OOM, a container restart) takes that job with it and nothing retries
it — the job simply disappears. Delivery is also invisible: there is no ack, no
retry counter and no place for a message that fails repeatedly.

This adapter adds exactly those three things, and nothing else:

* **Manual acknowledgement.** A message is acked only after the handler returns;
  if the worker dies first, RabbitMQ redelivers it.
* **Bounded retries through a delay queue.** A failed attempt is re-published to
  a retry queue whose message TTL dead-letters it back onto the work queue after
  a backoff, with the attempt count carried in a header - no in-process sleeping
  and no lost job.
* **A dead-letter queue.** After ``RABBITMQ_MAX_RETRIES`` the message lands in
  ``RABBITMQ_DEAD_QUEUE`` where it can be inspected and replayed by hand instead
  of vanishing.

Topology (all declared idempotently on connect):

    exchange media.jobs  --jobs.run-->  queue media.jobs.run
                                            |  (failed, retries left)
                                            v
                                        queue media.jobs.retry  --TTL--> back to media.jobs.run
                                            |  (failed, retries exhausted)
                                            v
    exchange media.jobs.dlx --jobs.dead--> queue media.jobs.dead
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import timedelta

from utils.eventbus import metrics
from utils.eventbus.config import EventBusSettings, get_settings

logger = logging.getLogger(__name__)

try:
    import aio_pika
    from aio_pika import DeliveryMode, ExchangeType, Message
except Exception:  # pragma: no cover - optional dependency
    aio_pika = None
    DeliveryMode = None
    ExchangeType = None

RETRY_HEADER = "x-retry-count"
ORIGIN_KEY = "_eventbus_origin"
RABBITMQ_ORIGIN = "rabbitmq"


def retry_count(headers: dict | None) -> int:
    """Read the retry counter from message headers, tolerating odd types."""
    try:
        value = (headers or {}).get(RETRY_HEADER, 0)
        return max(0, int(value))
    except Exception:
        return 0


def should_retry(headers: dict | None, max_retries: int) -> bool:
    """True while the message still has attempts left."""
    return retry_count(headers) < max(0, int(max_retries))


def retry_headers(headers: dict | None) -> dict:
    """Headers for the next attempt (previous headers preserved, counter bumped)."""
    merged = dict(headers or {})
    merged[RETRY_HEADER] = retry_count(headers) + 1
    return merged


def backoff_ms(attempt: int, base_ms: int) -> int:
    """Delay before attempt ``attempt`` (1-based): base, 2x, 4x... capped at 10x."""
    step = max(1, int(attempt))
    return min(int(base_ms) * (2 ** (step - 1)), int(base_ms) * 10)


def retry_expiration(delay_ms: int) -> timedelta:
    """Wrap a millisecond backoff as the duration aio-pika's ``Message`` wants.

    ``Message.expiration`` is expressed in **seconds**: aio-pika multiplies it by
    1000 to build the AMQP ``expiration`` property. Passing ``backoff_ms``'s
    milliseconds straight through therefore made every retry 1000x too long - the
    default 30s backoff parked a failed job for 8+ hours, so it never came back.
    A ``timedelta`` carries the unit explicitly instead of relying on a comment.
    """
    return timedelta(milliseconds=max(0, int(delay_ms)))


def is_rabbitmq_job(job: dict | None) -> bool:
    """True when a job dict was delivered by this adapter rather than the Redis list."""
    try:
        return (job or {}).get(ORIGIN_KEY) == RABBITMQ_ORIGIN
    except Exception:
        return False


def mark_origin(job: dict) -> dict:
    """Tag an inbound job so later code (e.g. the lock requeue path) can send it home."""
    try:
        job[ORIGIN_KEY] = RABBITMQ_ORIGIN
    except Exception:
        logger.debug("eventbus: could not tag job origin")
    return job


def jobs_queue_arguments(settings: EventBusSettings) -> dict:
    """Declaration arguments for the work queue.

    The dead-letter pair is what routes an exhausted message to the dead queue
    instead of RabbitMQ silently discarding it. Pinned by a unit test, because
    leaving it out is invisible until a job fails for the last time.
    """
    return {
        "x-dead-letter-exchange": settings.dlx,
        "x-dead-letter-routing-key": settings.dead_routing_key,
    }


def retry_queue_arguments(settings: EventBusSettings) -> dict:
    """Declaration arguments for the delay queue.

    Deliberately **no** ``x-message-ttl``: the delay is carried per message (see
    ``_handle_failure``), so changing ``RABBITMQ_RETRY_TTL_MS`` does not change
    the queue's declaration. A queue-level TTL would have to be re-declared with
    different arguments when the setting changes, and RabbitMQ rejects that with
    PRECONDITION_FAILED - i.e. tuning the backoff would break a running cluster
    until someone deleted the queue by hand.
    """
    return {
        "x-dead-letter-exchange": settings.exchange,
        "x-dead-letter-routing-key": settings.jobs_routing_key,
    }


def topology(settings: EventBusSettings) -> dict:
    """Return the queue names and arguments the consumer declares."""
    return {
        "exchange": settings.exchange,
        "jobs_queue": settings.jobs_queue,
        "jobs_routing_key": settings.jobs_routing_key,
        "retry_queue": settings.retry_queue,
        "dead_queue": settings.dead_queue,
        "dead_routing_key": settings.dead_routing_key,
        "dlx": settings.dlx,
        "retry_ttl_ms": settings.retry_ttl_ms,
    }


class RabbitJobQueue:
    """Thin aio-pika wrapper used by the producer (enqueue) and the worker (consume)."""

    def __init__(self, settings: EventBusSettings | None = None):
        self._settings = settings
        self._connection = None
        self._channel = None
        self._exchange = None
        self._jobs_queue = None
        self._retry_queue = None
        self._dead_queue = None
        self._lock = asyncio.Lock()

    # ── lifecycle ────────────────────────────────────────────────────────────
    @property
    def settings(self) -> EventBusSettings:
        return self._settings or get_settings()

    def available(self) -> bool:
        return aio_pika is not None and bool(self.settings.rabbitmq_url)

    async def connect(self):
        """Open a robust connection, declare the topology and enable confirms."""
        if not self.available():
            raise RuntimeError("RabbitMQ client unavailable (aio-pika missing or RABBITMQ_URL unset)")
        async with self._lock:
            if self._connection is not None and not self._connection.is_closed:
                return self._connection
            self._connection = await aio_pika.connect_robust(self.settings.rabbitmq_url)
            # publisher_confirms makes publish() wait for the broker to take
            # responsibility for the message: a job is only considered queued
            # once RabbitMQ has persisted it.
            self._channel = await self._connection.channel(publisher_confirms=True)
            await self._channel.set_qos(prefetch_count=self.settings.prefetch)
            self._exchange = await self._channel.declare_exchange(
                self.settings.exchange, ExchangeType.DIRECT, durable=True
            )
            dlx = await self._channel.declare_exchange(self.settings.dlx, ExchangeType.DIRECT, durable=True)
            # The work queue needs its own dead-letter arguments, or a rejected
            # message is discarded instead of being routed to the dead queue -
            # that is the difference between "retried and then parked" and
            # "retried and then silently lost".
            self._jobs_queue = await self._channel.declare_queue(
                self.settings.jobs_queue,
                durable=True,
                arguments=jobs_queue_arguments(self.settings),
            )
            await self._jobs_queue.bind(self._exchange, routing_key=self.settings.jobs_routing_key)
            # Dead-letter queue: nothing consumes it either. It is bound to the
            # DLX so exhausted messages have somewhere to sit for inspection.
            self._dead_queue = await self._channel.declare_queue(self.settings.dead_queue, durable=True)
            await self._dead_queue.bind(dlx, routing_key=self.settings.dead_routing_key)
            # Retry queue: nothing consumes it. Each message's own expiration
            # TTL expires it and the dead-letter routing sends it back to the work
            # queue, so the backoff happens in the broker instead of a worker
            # holding a job while it sleeps.
            self._retry_queue = await self._channel.declare_queue(
                self.settings.retry_queue,
                durable=True,
                arguments=retry_queue_arguments(self.settings),
            )
            logger.info(
                "eventbus: RabbitMQ ready (exchange=%s queue=%s prefetch=%s)",
                self.settings.exchange,
                self.settings.jobs_queue,
                self.settings.prefetch,
            )
            return self._connection

    async def close(self) -> None:
        with contextlib.suppress(Exception):
            if self._connection is not None and not self._connection.is_closed:
                await self._connection.close()
        self._connection = self._channel = self._exchange = None
        self._jobs_queue = self._retry_queue = self._dead_queue = None

    # ── producing ────────────────────────────────────────────────────────────
    async def publish_job(self, job: dict, *, retry: bool = False) -> bool:
        """Publish a job durably. Returns False when the broker did not take it."""
        if not self.available():
            return False
        job_id = str(job.get("job_id") or "")
        try:
            await self.connect()
            headers = retry_headers({RETRY_HEADER: 0}) if retry else {RETRY_HEADER: 0}
            message = Message(
                body=json.dumps(job).encode("utf-8"),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
                # The job id is the message id, which makes a duplicate delivery
                # identifiable to consumers (and to RabbitMQ's own UI).
                message_id=job_id,
                correlation_id=str(job.get("request_id") or ""),
                headers=headers,
            )
            target = self._retry_queue if retry else None
            if target is not None:
                await self._channel.default_exchange.publish(
                    message, routing_key=self.settings.retry_queue, timeout=self.settings.publisher_timeout
                )
            else:
                await self._exchange.publish(
                    message, routing_key=self.settings.jobs_routing_key, timeout=self.settings.publisher_timeout
                )
            metrics.inc(metrics.JOBS_ROUTED, "rabbitmq")
            logger.info("eventbus: queued job %s on RabbitMQ (%s)", job_id, self.settings.jobs_queue)
            return True
        except Exception as exc:
            # The caller falls back to the Redis list, so a broker outage slows
            # jobs down instead of losing them.
            metrics.inc(metrics.JOBS_ROUTED_FALLBACK, type(exc).__name__)
            logger.warning("eventbus: RabbitMQ publish failed for job %s (%s); falling back to Redis", job_id, exc)
            return False

    async def requeue_job(self, job: dict) -> bool:
        """Put a job back for another attempt through the retry (delayed) queue."""
        return await self.publish_job(job, retry=True)

    # ── consuming ────────────────────────────────────────────────────────────
    async def consume(self, handler, stop_event=None) -> None:
        """Consume jobs with manual ack, bounded retries and a dead-letter path.

        ``handler`` is awaited per job; returning normally acks the message, and
        raising sends it through the retry queue or to the dead-letter queue.
        """
        if not self.available():
            raise RuntimeError("RabbitMQ client unavailable (aio-pika missing or RABBITMQ_URL unset)")
        await self.connect()
        # Deliveries are pushed into a local queue by the broker callback rather
        # than iterated, so stop_event is honoured within a second even when the
        # queue is empty. Anything handed over but not yet processed stays
        # unacked and the broker redelivers it.
        inbound: asyncio.Queue = asyncio.Queue()
        consumer_tag = await self._jobs_queue.consume(inbound.put_nowait, no_ack=False)
        logger.info("eventbus: consuming jobs from %s", self.settings.jobs_queue)
        try:
            while True:
                if stop_event is not None and stop_event.is_set():
                    logger.info("eventbus: consumer stopping; unprocessed deliveries stay unacked")
                    break
                try:
                    message = await asyncio.wait_for(inbound.get(), timeout=1.0)
                except TimeoutError:
                    continue
                await self._process_message(message, handler)
        finally:
            with contextlib.suppress(Exception):
                await self._jobs_queue.cancel(consumer_tag)

    async def _process_message(self, message, handler) -> None:
        """Handle one delivery: parse it, run the handler, then ack / retry / dead-letter."""
        try:
            job = json.loads(message.body.decode("utf-8"))
        except Exception:
            logger.warning("eventbus: dropping unparseable message %s", message.message_id)
            metrics.inc(metrics.QUEUE_DEAD_LETTERED)
            with contextlib.suppress(Exception):
                await message.reject(requeue=False)
            return
        if not isinstance(job, dict):
            with contextlib.suppress(Exception):
                await message.reject(requeue=False)
            return
        mark_origin(job)
        try:
            await handler(job)
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await message.nack(requeue=True)
            raise
        except Exception as exc:
            await self._handle_failure(message, job, exc)
        else:
            with contextlib.suppress(Exception):
                await message.ack()

    async def _handle_failure(self, message, job: dict, exc: Exception) -> None:
        """Retry a failed job, or dead-letter it once the attempts are used up."""
        job_id = str(job.get("job_id") or message.message_id or "?")
        headers = getattr(message, "headers", None) or {}
        if should_retry(headers, self.settings.max_retries):
            attempt = retry_count(headers) + 1
            try:
                if self._channel is None:
                    await self.connect()
                delay_ms = backoff_ms(attempt, self.settings.retry_ttl_ms)
                retried = Message(
                    body=message.body,
                    content_type="application/json",
                    delivery_mode=DeliveryMode.PERSISTENT,
                    message_id=message.message_id,
                    correlation_id=job.get("request_id") or "",
                    headers=retry_headers(headers),
                    # The broker holds the message for this long, then dead-letters
                    # it back onto the work queue. aio-pika reads ``expiration`` in
                    # *seconds*, so the millisecond backoff has to be wrapped (see
                    # ``retry_expiration``) or every delay is 1000x too long.
                    expiration=retry_expiration(delay_ms),
                )
                await self._channel.default_exchange.publish(
                    retried, routing_key=self.settings.retry_queue, timeout=self.settings.publisher_timeout
                )
                metrics.inc(metrics.QUEUE_RETRIES)
                logger.warning(
                    "eventbus: job %s failed (attempt %s/%s), retrying in %sms: %s",
                    job_id,
                    attempt,
                    self.settings.max_retries,
                    delay_ms,
                    exc,
                )
                with contextlib.suppress(Exception):
                    await message.ack()
                return
            except Exception as requeue_exc:
                logger.error("eventbus: could not queue a retry for job %s (%s)", job_id, requeue_exc)
                # Putting the delivery straight back without a pause turns a
                # broken retry path into a hot loop: the broker redelivers
                # immediately, the handler fails again, and the pair spins at full
                # speed. A short pause keeps the job (nothing is lost) while
                # leaving room for the broker to recover.
                await asyncio.sleep(1.0)
                with contextlib.suppress(Exception):
                    await message.nack(requeue=True)
                return
        metrics.inc(metrics.QUEUE_DEAD_LETTERED)
        logger.error(
            "eventbus: job %s exhausted %s attempt(s), dead-lettering to %s: %s",
            job_id,
            self.settings.max_retries,
            self.settings.dead_queue,
            exc,
        )
        with contextlib.suppress(Exception):
            await message.reject(requeue=False)

    async def stats(self) -> dict:
        """Report ready-message counts for the three queues by name.

        Re-declaring is how a queue depth is read with aio-pika 10 (its
        ``Queue.declare`` has no ``passive`` argument); declaring with the same
        arguments is idempotent, so this is a read, not a reconfiguration.
        """
        await self.connect()
        out = {}
        for name, queue in (
            (self.settings.jobs_queue, self._jobs_queue),
            (self.settings.retry_queue, self._retry_queue),
            (self.settings.dead_queue, self._dead_queue),
        ):
            try:
                declared = await queue.declare()
                out[name] = getattr(declared, "message_count", None)
            except Exception as exc:
                out[name] = f"unavailable: {exc}"
        return out

    async def queue_depth(self, queue_name: str) -> int | None:
        """Ready-message count for one of the three declared queues, or None."""
        depths = await self.stats()
        value = depths.get(queue_name)
        return value if isinstance(value, int) else None

    async def purge_queues(self, include_dead: bool = True) -> dict:
        """Discard every message in the declared queues. **Destructive.**

        Only for local verification (``scripts/check_eventbus_integration.py``)
        and manual recovery: purging the work queue deletes jobs that have not
        run yet, so it is never called from the application path.
        """
        await self.connect()
        targets = [(self.settings.jobs_queue, self._jobs_queue), (self.settings.retry_queue, self._retry_queue)]
        if include_dead:
            targets.append((self.settings.dead_queue, self._dead_queue))
        purged = {}
        for name, queue in targets:
            try:
                result = await queue.purge()
                purged[name] = getattr(result, "message_count", 0)
            except Exception as exc:
                purged[name] = f"unavailable: {exc}"
        return purged


_QUEUE: RabbitJobQueue | None = None


def get_queue(settings: EventBusSettings | None = None) -> RabbitJobQueue:
    """Return the process-wide queue adapter."""
    global _QUEUE
    if _QUEUE is None:
        _QUEUE = RabbitJobQueue(settings)
    return _QUEUE


async def close_queue() -> None:
    """Close the shared adapter (call at shutdown)."""
    global _QUEUE
    if _QUEUE is not None:
        await _QUEUE.close()
        _QUEUE = None
