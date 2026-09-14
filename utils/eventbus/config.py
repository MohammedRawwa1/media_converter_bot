"""Configuration for the optional event-bus layer (RabbitMQ work queue + Kafka event log).

Two independent switches, both off by default so that deploying this module
cannot change existing behaviour:

* ``EVENTBUS_QUEUE_BACKEND`` chooses where *jobs* are queued: ``redis`` (the
  existing list) or ``rabbitmq``. When ``rabbitmq`` is selected,
  ``EVENTBUS_QUEUE_ROLLOUT_PERCENT`` decides what share of jobs actually goes
  there — the rest keep using Redis, so a broker can be introduced on a small
  slice of traffic first and expanded once it has been watched.
* ``EVENTBUS_EVENTS_BACKEND`` chooses whether *lifecycle events* are also
  written to Kafka (``off`` or ``kafka``).

Nothing here raises when a setting is missing or wrong: an unusable
configuration is reported and degrades to the previous behaviour (Redis queue,
events off) rather than breaking job processing. Callers do not need to check
whether the layer is enabled — they call the helpers in
``utils.eventbus``, which no-op when the relevant switch is off.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)

QUEUE_BACKENDS = ("redis", "rabbitmq")
EVENT_BACKENDS = ("off", "kafka")

# Defaults for the broker topology. All three names are declared by the consumer
# on startup, so changing them only requires changing them everywhere.
DEFAULT_EXCHANGE = "media.jobs"
DEFAULT_JOBS_QUEUE = "media.jobs.run"
DEFAULT_JOBS_ROUTING_KEY = "jobs.run"
DEFAULT_RETRY_QUEUE = "media.jobs.retry"
DEFAULT_DEAD_QUEUE = "media.jobs.dead"
DEFAULT_DLX = "media.jobs.dlx"
DEFAULT_DEAD_ROUTING_KEY = "jobs.dead"

DEFAULT_KAFKA_TOPIC = "media.job.events"


def _env(name: str, default: str = "") -> str:
    try:
        return (os.getenv(name) or default).strip()
    except Exception:  # pragma: no cover - os.getenv does not fail in practice
        return default


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if not raw:
        return default
    try:
        return int(float(raw))
    except Exception:
        logger.warning("eventbus: %s is not a number, using default %s", name, default)
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name).lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


@dataclass(frozen=True)
class EventBusSettings:
    """Resolved configuration for both brokers (never raises, never secret-echoing)."""

    queue_backend: str
    queue_rollout_percent: int
    events_backend: str
    # Jobs routed to Redis instead of the broker for reasons other than the
    # rollout share (broker unreachable, client library missing). Reported by
    # ``validate`` so a silent degrade is visible in the logs.
    degraded_reason: str | None
    # When true, an enabled-but-unusable event log aborts startup instead of
    # degrading to "events dropped" (see ``verify_events_startup``).
    require_brokers: bool

    # ── RabbitMQ ──
    rabbitmq_url: str
    exchange: str
    jobs_queue: str
    jobs_routing_key: str
    retry_queue: str
    dead_queue: str
    dlx: str
    dead_routing_key: str
    prefetch: int
    max_retries: int
    retry_ttl_ms: int
    publisher_timeout: float

    # ── Kafka ──
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_client_id: str
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    # Path to a PEM CA bundle for SSL/SASL_SSL. Managed brokers that present a
    # publicly-signed certificate (Confluent, Redpanda) need nothing here; a
    # broker with its own CA (e.g. Aiven's project CA) needs the file.
    kafka_ssl_cafile: str
    kafka_acks: str
    kafka_linger_ms: int
    emit_progress_events: bool
    progress_min_interval_ms: int

    @property
    def queue_enabled(self) -> bool:
        """True when at least some jobs are meant to go through RabbitMQ."""
        return self.queue_backend == "rabbitmq" and self.queue_rollout_percent > 0 and not self.degraded_reason

    @property
    def events_enabled(self) -> bool:
        return self.events_backend == "kafka" and bool(self.kafka_bootstrap_servers)

    @property
    def consumes_redis(self) -> bool:
        """True when the Redis list can still hold jobs for this deployment.

        With a partial rollout both queues are live at once, so the worker has to
        keep draining Redis as well as the broker.
        """
        return (not self.queue_enabled) or self.queue_rollout_percent < 100

    @property
    def consumes_rabbitmq(self) -> bool:
        """True when a worker should consume from the broker.

        Deliberately *not* tied to the rollout share: `..._ROLLOUT_PERCENT=0`
        means "send nothing new there", but whatever is already queued still has
        to be drained, or a rollback would strand in-flight jobs in the broker.
        """
        return self.queue_backend == "rabbitmq" and not self.degraded_reason

    def describe(self) -> dict:
        """Return a log-safe summary (no URLs, credentials or passwords)."""
        return {
            "queue_backend": self.queue_backend,
            "queue_rollout_percent": self.queue_rollout_percent,
            "rabbitmq_configured": bool(self.rabbitmq_url),
            "queue_enabled": self.queue_enabled,
            "events_backend": self.events_backend,
            "events_enabled": self.events_enabled,
            "kafka_configured": bool(self.kafka_bootstrap_servers),
            "kafka_topic": self.kafka_topic,
            "emit_progress_events": self.emit_progress_events,
            "degraded_reason": self.degraded_reason,
            "require_brokers": self.require_brokers,
        }


def load_settings() -> EventBusSettings:
    """Read the settings from the environment and resolve the effective backend."""
    queue_backend = _env("EVENTBUS_QUEUE_BACKEND", "redis").lower() or "redis"
    events_backend = _env("EVENTBUS_EVENTS_BACKEND", "off").lower() or "off"
    rollout = _clamp(_env_int("EVENTBUS_QUEUE_ROLLOUT_PERCENT", 0), 0, 100)

    degraded_reason = None
    if queue_backend not in QUEUE_BACKENDS:
        degraded_reason = f"unknown EVENTBUS_QUEUE_BACKEND={queue_backend!r}"
        queue_backend = "redis"
    elif queue_backend == "rabbitmq":
        # An enabled-but-unconfigured broker is a configuration error, not a
        # reason to stop processing jobs: report it and keep using Redis.
        if not _env("RABBITMQ_URL"):
            degraded_reason = "RABBITMQ_URL is not set"
            queue_backend = "redis"
        elif not rabbitmq_available():
            degraded_reason = "aio-pika is not installed"
            queue_backend = "redis"

    if events_backend not in EVENT_BACKENDS:
        logger.warning("eventbus: unknown EVENTBUS_EVENTS_BACKEND=%r, disabling events", events_backend)
        events_backend = "off"

    settings = EventBusSettings(
        queue_backend=queue_backend,
        queue_rollout_percent=rollout,
        events_backend=events_backend,
        degraded_reason=degraded_reason,
        require_brokers=_env_bool("EVENTBUS_REQUIRE_BROKERS", False),
        rabbitmq_url=_env("RABBITMQ_URL"),
        exchange=_env("RABBITMQ_EXCHANGE", DEFAULT_EXCHANGE),
        jobs_queue=_env("RABBITMQ_JOBS_QUEUE", DEFAULT_JOBS_QUEUE),
        jobs_routing_key=_env("RABBITMQ_JOBS_ROUTING_KEY", DEFAULT_JOBS_ROUTING_KEY),
        retry_queue=_env("RABBITMQ_RETRY_QUEUE", DEFAULT_RETRY_QUEUE),
        dead_queue=_env("RABBITMQ_DEAD_QUEUE", DEFAULT_DEAD_QUEUE),
        dlx=_env("RABBITMQ_DLX", DEFAULT_DLX),
        dead_routing_key=_env("RABBITMQ_DEAD_ROUTING_KEY", DEFAULT_DEAD_ROUTING_KEY),
        # Prefetch 1 matches the existing worker, which processes one job at a
        # time. Raising it is a deliberate throughput change and needs the worker
        # to hold several jobs at once (MAX_CONCURRENT_TASKS is a separate cap).
        prefetch=_clamp(_env_int("RABBITMQ_PREFETCH", 1), 1, 100),
        max_retries=_clamp(_env_int("RABBITMQ_MAX_RETRIES", 3), 0, 20),
        retry_ttl_ms=_clamp(_env_int("RABBITMQ_RETRY_TTL_MS", 30000), 1000, 3600000),
        publisher_timeout=float(_env_int("RABBITMQ_PUBLISH_TIMEOUT", 10)),
        kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS"),
        kafka_topic=_env("KAFKA_EVENTS_TOPIC", DEFAULT_KAFKA_TOPIC),
        kafka_client_id=_env("KAFKA_CLIENT_ID", "media-bot"),
        kafka_security_protocol=_env("KAFKA_SECURITY_PROTOCOL"),
        kafka_sasl_mechanism=_env("KAFKA_SASL_MECHANISM"),
        kafka_sasl_username=_env("KAFKA_SASL_USERNAME"),
        kafka_sasl_password=_env("KAFKA_SASL_PASSWORD"),
        kafka_ssl_cafile=_env("KAFKA_SSL_CAFILE"),
        kafka_acks=_env("KAFKA_ACKS", "all"),
        kafka_linger_ms=_clamp(_env_int("KAFKA_LINGER_MS", 20), 0, 1000),
        # Progress is chatty (one event per ffmpeg tick), so it is opt-in and
        # throttled; lifecycle events are always emitted.
        emit_progress_events=_env_bool("KAFKA_EMIT_PROGRESS_EVENTS", False),
        progress_min_interval_ms=_clamp(_env_int("KAFKA_PROGRESS_MIN_INTERVAL_MS", 2000), 0, 60000),
    )

    if settings.degraded_reason:
        logger.warning(
            "eventbus: RabbitMQ queue backend requested but unusable (%s); jobs keep using Redis",
            settings.degraded_reason,
        )
    if events_backend == "kafka" and not settings.kafka_bootstrap_servers:
        logger.warning("eventbus: KAFKA_BOOTSTRAP_SERVERS is not set, event publishing stays off")

    return settings


def rabbitmq_available() -> bool:
    """True when the aio-pika client library can be imported."""
    try:
        import aio_pika  # noqa: F401
    except Exception:
        return False
    return True


def kafka_available() -> bool:
    """True when the aiokafka client library can be imported."""
    try:
        import aiokafka  # noqa: F401
    except Exception:
        return False
    return True


_SETTINGS_CACHE: EventBusSettings | None = None


def get_settings() -> EventBusSettings:
    """Return the cached settings, loading them from the environment on first use."""
    global _SETTINGS_CACHE
    if _SETTINGS_CACHE is None:
        _SETTINGS_CACHE = load_settings()
    return _SETTINGS_CACHE


def reset_settings_cache() -> None:
    """Drop the cached settings (used by tests and by tooling that re-reads env)."""
    global _SETTINGS_CACHE
    _SETTINGS_CACHE = None


def queue_backend_for_job(job: dict | None) -> str:
    """Return which backend a single job must be queued on: ``redis`` or ``rabbitmq``.

    The decision is a stable hash of the job id, so every producer (Telegram
    handlers, the fetcher service, the Telethon ingest tool) agrees on which
    queue a given job belongs to, and a job id always lands on the same side of
    the rollout without any shared state.
    """
    settings = get_settings()
    if not settings.queue_enabled:
        return "redis"
    if settings.queue_rollout_percent >= 100:
        return "rabbitmq"
    job_id = ""
    try:
        job_id = str((job or {}).get("job_id") or "")
    except Exception:
        job_id = ""
    if not job_id:
        # Without an id there is nothing to hash; keep it on the proven path.
        return "redis"
    # Imported here to keep config importable on its own (messages has no deps).
    from utils.eventbus.messages import job_bucket

    return "rabbitmq" if job_bucket(job_id) < settings.queue_rollout_percent else "redis"
