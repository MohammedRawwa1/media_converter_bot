"""Kafka producer behaviour, with a fake producer standing in for the broker.

The fake is what makes these tests meaningful without a cluster: they pin the
envelope that is written, the key that keeps a job's events in order, the
progress throttle, and - most importantly - that a broker failure is counted and
swallowed rather than propagated into a job.
"""

import asyncio
import dataclasses
import logging
import ssl

from utils import eventbus
from utils.eventbus import kafka, messages
from utils.eventbus.kafka import KafkaEventBus, progress_allowed


class FakeProducer:
    """Records what would have been sent; can be told to fail like a dead broker."""

    def __init__(self, fail: bool = False):
        self.fail = fail
        self.started = False
        self.stopped = False
        self.sent: list[dict] = []

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def send_and_wait(self, topic, value=None, key=None):
        if self.fail:
            raise RuntimeError("kafka is down")
        self.sent.append({"topic": topic, "value": value, "key": key})


def _bus(eventbus_env, producer: FakeProducer, **env) -> KafkaEventBus:
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_BOOTSTRAP_SERVERS="localhost:9092", **env)
    return KafkaEventBus(settings, producer_factory=lambda _settings: producer)


def test_disabled_events_do_not_touch_the_broker(eventbus_env):
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="off")
    producer = FakeProducer()
    bus = KafkaEventBus(settings, producer_factory=lambda _settings: producer)
    event = messages.new_event(messages.JOB_QUEUED, job_id="j1")
    assert asyncio.run(bus.emit(event)) is False
    assert producer.sent == []
    assert producer.started is False


def test_emit_writes_the_envelope_keyed_by_job(eventbus_env):
    producer = FakeProducer()
    bus = _bus(eventbus_env, producer)
    event = messages.new_event(messages.JOB_COMPLETED, job_id="job-abc", payload={"status": "done"})
    assert asyncio.run(bus.emit(event)) is True

    assert len(producer.sent) == 1
    sent = producer.sent[0]
    # Same topic as configured, keyed by job id so one job's events stay ordered.
    assert sent["topic"] == bus.settings.kafka_topic
    assert sent["key"] == b"job-abc"
    assert messages.decode(sent["value"]) == event
    assert bus.published == 1 and bus.failed == 0


def test_broker_failure_is_counted_not_raised(eventbus_env):
    producer = FakeProducer(fail=True)
    bus = _bus(eventbus_env, producer)
    event = messages.new_event(messages.JOB_QUEUED, job_id="j1")
    # A logging failure must never surface in the job path.
    assert asyncio.run(bus.emit(event)) is False
    assert bus.failed == 1
    assert producer.sent == []


def test_start_failure_is_handled(eventbus_env):
    class BrokenStart(FakeProducer):
        async def start(self):
            raise RuntimeError("no route to broker")

    bus = _bus(eventbus_env, BrokenStart())
    assert asyncio.run(bus.start()) is False
    assert asyncio.run(bus.emit(messages.new_event(messages.JOB_QUEUED, job_id="j1"))) is False


def test_progress_throttle_decision():
    assert progress_allowed(None, 100.0, 2000) is True
    assert progress_allowed(100.0, 101.0, 2000) is False
    assert progress_allowed(100.0, 102.0, 2000) is True
    # Interval 0 means "no throttle" rather than "block everything".
    assert progress_allowed(100.0, 100.0, 0) is True


def test_progress_events_are_opt_in(eventbus_env):
    producer = FakeProducer()
    bus = _bus(eventbus_env, producer)
    event = messages.new_event(messages.JOB_PROGRESS, job_id="j1", payload={"progress": 10})
    assert asyncio.run(bus.emit_progress(event)) is False
    assert producer.sent == []
    assert bus.suppressed == 1


def test_progress_events_are_throttled_per_job(eventbus_env):
    producer = FakeProducer()
    bus = _bus(eventbus_env, producer, KAFKA_EMIT_PROGRESS_EVENTS="true", KAFKA_PROGRESS_MIN_INTERVAL_MS=2000)
    first = messages.new_event(messages.JOB_PROGRESS, job_id="j1", payload={"progress": 10})
    second = messages.new_event(messages.JOB_PROGRESS, job_id="j1", payload={"progress": 20})
    other = messages.new_event(messages.JOB_PROGRESS, job_id="j2", payload={"progress": 20})

    assert asyncio.run(bus.emit_progress(first, now=1000.0)) is True
    assert asyncio.run(bus.emit_progress(second, now=1001.0)) is False
    # A different job is throttled independently.
    assert asyncio.run(bus.emit_progress(other, now=1001.0)) is True
    # Once the interval has passed the same job is allowed through again.
    assert asyncio.run(bus.emit_progress(second, now=1003.0)) is True
    assert len(producer.sent) == 3
    assert bus.suppressed == 1


def test_a_finished_job_forgets_its_throttle_state(eventbus_env):
    producer = FakeProducer()
    bus = _bus(eventbus_env, producer, KAFKA_EMIT_PROGRESS_EVENTS="true", KAFKA_PROGRESS_MIN_INTERVAL_MS=60000)
    assert asyncio.run(bus.emit_progress(messages.new_event(messages.JOB_PROGRESS, job_id="j1"), now=10.0)) is True
    assert asyncio.run(bus.emit_progress(messages.new_event(messages.JOB_PROGRESS, job_id="j1"), now=11.0)) is False
    bus.forget_progress("j1")
    assert asyncio.run(bus.emit_progress(messages.new_event(messages.JOB_PROGRESS, job_id="j1"), now=12.0)) is True


def test_throttle_state_does_not_grow_without_bound(eventbus_env):
    producer = FakeProducer()
    bus = _bus(eventbus_env, producer, KAFKA_EMIT_PROGRESS_EVENTS="true", KAFKA_PROGRESS_MIN_INTERVAL_MS=0)
    for index in range(5200):
        # Clock advances 10s per job, so old entries become prunable.
        asyncio.run(
            bus.emit_progress(messages.new_event(messages.JOB_PROGRESS, job_id=f"job-{index}"), now=1000.0 + index * 10)
        )
    # The map is pruned once it grows past its cap, keeping a long-running
    # worker's memory flat instead of accumulating one entry per job it has seen.
    assert len(bus._progress_seen) < 1000, len(bus._progress_seen)


def test_tls_protocols_get_a_verifying_ssl_context(eventbus_env):
    # aiokafka raises "`ssl_context` is mandatory if security_protocol=='SSL'"
    # when one is not passed, and every managed broker requires TLS - so the
    # producer never started and all events were dropped while the config still
    # reported "events_enabled: true".
    plain = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka")
    assert kafka.ssl_context_for(plain) is None

    context = kafka.ssl_context_for(eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_SECURITY_PROTOCOL="SASL_SSL"))
    assert context is not None
    # Verification must stay on; this must never become an unverified connection.
    assert context.check_hostname is True
    assert context.verify_mode == ssl.CERT_REQUIRED

    assert (
        kafka.ssl_context_for(eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_SECURITY_PROTOCOL="SSL")) is not None
    )
    # PLAINTEXT must not get one, or aiokafka rejects the combination.
    assert (
        kafka.ssl_context_for(eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_SECURITY_PROTOCOL="PLAINTEXT"))
        is None
    )


def test_producer_kwargs_carry_the_security_settings(eventbus_env):
    settings = eventbus_env(
        EVENTBUS_EVENTS_BACKEND="kafka",
        KAFKA_BOOTSTRAP_SERVERS="broker:9094",
        KAFKA_SECURITY_PROTOCOL="SASL_SSL",
        KAFKA_SASL_MECHANISM="SCRAM-SHA-256",
        KAFKA_SASL_USERNAME="user",
        KAFKA_SASL_PASSWORD="secret",  # noqa: S106 - a throwaway value, not a credential
    )
    kwargs = kafka.producer_kwargs(settings)
    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["ssl_context"] is not None
    assert kwargs["sasl_mechanism"] == "SCRAM-SHA-256"
    assert kwargs["sasl_plain_username"] == "user"
    assert kwargs["enable_idempotence"] is True
    assert kwargs["acks"] == "all"


def test_plaintext_kwargs_are_left_alone(eventbus_env):
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_BOOTSTRAP_SERVERS="localhost:9092")
    kwargs = kafka.producer_kwargs(settings)
    assert "ssl_context" not in kwargs
    assert "security_protocol" not in kwargs
    assert "sasl_mechanism" not in kwargs
    # The reader path follows the same rules, or replay fails on a TLS cluster.
    assert "ssl_context" not in kafka.consumer_kwargs(settings)
    assert kafka.consumer_kwargs(settings)["client_id"].endswith("-reader")


def test_cafile_reaches_the_ssl_context(eventbus_env, monkeypatch):
    # A broker presenting its own CA (Aiven's project CA) needs the bundle; a
    # publicly-signed one (Confluent, Redpanda) needs nothing and falls back to
    # the system trust store.
    seen: dict = {}

    def fake_create_ssl_context(*, cafile=None):
        seen["cafile"] = cafile
        return "context"

    monkeypatch.setattr(kafka, "create_ssl_context", fake_create_ssl_context)

    base = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_SECURITY_PROTOCOL="SASL_SSL")
    # Publicly-signed broker: nothing supplied, so the system trust store is used.
    assert kafka.ssl_context_for(base) == "context"
    assert seen["cafile"] is None

    # Private CA (e.g. Aiven's project CA): the bundle is passed through.
    with_ca = dataclasses.replace(base, kafka_ssl_cafile="/etc/ssl/aiven-ca.pem")
    assert kafka.ssl_context_for(with_ca) == "context"
    assert seen["cafile"] == "/etc/ssl/aiven-ca.pem"


def test_aiven_ca_cert_environment_value_reaches_ssl_context(eventbus_env, monkeypatch):
    seen: dict = {}

    def fake_create_ssl_context(*, cafile=None):
        seen["cafile"] = cafile
        assert cafile is not None
        with open(cafile, encoding="utf-8") as handle:
            assert handle.read() == "-----BEGIN CERTIFICATE-----\nvalue\n-----END CERTIFICATE-----"
        return "context"

    monkeypatch.setattr(kafka, "create_ssl_context", fake_create_ssl_context)
    monkeypatch.setenv("AIVEN_CA_CERT", "-----BEGIN CERTIFICATE-----\\nvalue\\n-----END CERTIFICATE-----")

    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_SECURITY_PROTOCOL="SASL_SSL")
    assert kafka.ssl_context_for(settings) == "context"
    assert seen["cafile"]


def test_verify_publish_confirms_a_write_is_accepted(eventbus_env):
    producer = FakeProducer()
    bus = _bus(eventbus_env, producer)
    ok, detail = asyncio.run(bus.verify_publish(messages.new_event(messages.EVENTS_PROBE)))
    assert ok is True
    assert bus.settings.kafka_topic in detail
    assert len(producer.sent) == 1

    # The probe is operational, not a job event: no job id, so a per-job reader
    # never picks it up.
    body = messages.decode(producer.sent[0]["value"])
    assert body["type"] == messages.EVENTS_PROBE
    assert body["job_id"] == ""


def test_verify_publish_reports_why_it_failed(eventbus_env):
    # The reason is the point: "events are dropped" without a cause is what made
    # a misconfigured broker take hours to find.
    bus = _bus(eventbus_env, FakeProducer(fail=True))
    ok, detail = asyncio.run(bus.verify_publish(messages.new_event(messages.EVENTS_PROBE)))
    assert ok is False
    assert "kafka is down" in detail


def test_verify_publish_reports_a_missing_bootstrap_list(eventbus_env):
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka")  # no KAFKA_BOOTSTRAP_SERVERS
    bus = KafkaEventBus(settings, producer_factory=lambda _settings: FakeProducer())
    ok, detail = asyncio.run(bus.verify_publish(messages.new_event(messages.EVENTS_PROBE)))
    assert ok is False
    assert "KAFKA_BOOTSTRAP_SERVERS" in detail


def test_startup_verification_does_nothing_when_events_are_off(eventbus_env, monkeypatch):
    eventbus_env(EVENTBUS_EVENTS_BACKEND="off")

    def explode():
        raise AssertionError("the bus must not be touched when events are disabled")

    monkeypatch.setattr(eventbus, "get_bus", explode)
    assert asyncio.run(eventbus.verify_events_startup()) is True


def test_startup_verification_logs_an_error_but_keeps_going(eventbus_env, monkeypatch, caplog):
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_BOOTSTRAP_SERVERS="localhost:9092")
    bus = KafkaEventBus(settings, producer_factory=lambda _settings: FakeProducer(fail=True))
    monkeypatch.setattr(eventbus, "get_bus", lambda: bus)

    with caplog.at_level(logging.ERROR):
        ok = asyncio.run(eventbus.verify_events_startup())

    # Loud, but not fatal by default: a broken event log must not stop job
    # processing, which is the contract the whole layer is built on.
    assert ok is False
    errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors, "a broken event log must be reported at ERROR, not debug"
    assert "cannot be published" in errors[0].getMessage()


def test_startup_verification_raises_when_brokers_are_required(eventbus_env, monkeypatch):
    settings = eventbus_env(
        EVENTBUS_EVENTS_BACKEND="kafka",
        KAFKA_BOOTSTRAP_SERVERS="localhost:9092",
        EVENTBUS_REQUIRE_BROKERS="true",
    )
    assert settings.require_brokers is True
    bus = KafkaEventBus(settings, producer_factory=lambda _settings: FakeProducer(fail=True))
    monkeypatch.setattr(eventbus, "get_bus", lambda: bus)

    raised = None
    try:
        asyncio.run(eventbus.verify_events_startup())
    except RuntimeError as exc:
        raised = exc
    assert raised is not None
    assert "cannot be published" in str(raised)
