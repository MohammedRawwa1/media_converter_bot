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
from types import SimpleNamespace

import pytest
from aiokafka import errors as kafka_errors

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

    async def _emit_many():
        # One event loop for the whole run. This used to be one asyncio.run() per
        # event, so most of the test's time went into 5,200 loop setups rather
        # than into the pruning it is about.
        for index in range(5200):
            # Clock advances 10s per job, so old entries become prunable.
            await bus.emit_progress(
                messages.new_event(messages.JOB_PROGRESS, job_id=f"job-{index}"), now=1000.0 + index * 10
            )

    asyncio.run(_emit_many())
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
        KAFKA_SASL_PASSWORD="secret",  # noqa: S106  # nosec B106 - a throwaway value, not a credential
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


def test_cafile_reaches_the_ssl_context(eventbus_env, monkeypatch, tmp_path):
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

    # Private CA (e.g. Aiven's project CA): the bundle is passed through.  The
    # path has to exist, or it is treated as unset (see the fallback test below).
    bundle = tmp_path / "aiven-ca.pem"
    bundle.write_text("-----BEGIN CERTIFICATE-----\nvalue\n-----END CERTIFICATE-----\n", encoding="utf-8")
    with_ca = dataclasses.replace(base, kafka_ssl_cafile=str(bundle))
    assert kafka.ssl_context_for(with_ca) == "context"
    assert seen["cafile"] == str(bundle)


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


def test_missing_cafile_falls_back_to_aiven_ca_cert(eventbus_env, monkeypatch, tmp_path):
    """A KAFKA_SSL_CAFILE that is not deployed must not hide AIVEN_CA_CERT.

    ``certs/`` is gitignored, so the path is commonly set while the file is
    absent on the host.  Preferring it made AIVEN_CA_CERT look ignored and the
    producer die with a bare FileNotFoundError instead of using the PEM that was
    actually provided.
    """
    seen: dict = {}

    def fake_create_ssl_context(*, cafile=None):
        seen["cafile"] = cafile
        with open(cafile, encoding="utf-8") as handle:
            assert handle.read() == "-----BEGIN CERTIFICATE-----\nvalue\n-----END CERTIFICATE-----"
        return "context"

    monkeypatch.setattr(kafka, "create_ssl_context", fake_create_ssl_context)
    monkeypatch.setenv("AIVEN_CA_CERT", "-----BEGIN CERTIFICATE-----\\nvalue\\n-----END CERTIFICATE-----")

    missing = str(tmp_path / "not-deployed.pem")
    settings = eventbus_env(
        EVENTBUS_EVENTS_BACKEND="kafka",
        KAFKA_SECURITY_PROTOCOL="SASL_SSL",
        KAFKA_SSL_CAFILE=missing,
    )
    # The setting is still recorded, it just does not win over a usable PEM.
    assert settings.kafka_ssl_cafile == missing
    assert kafka.ssl_context_for(settings) == "context"
    assert seen["cafile"] != missing


def test_missing_cafile_without_a_pem_names_the_variable(eventbus_env, monkeypatch, tmp_path):
    monkeypatch.setattr(kafka, "create_ssl_context", lambda *, cafile=None: "context")
    monkeypatch.delenv("AIVEN_CA_CERT", raising=False)

    settings = eventbus_env(
        EVENTBUS_EVENTS_BACKEND="kafka",
        KAFKA_SECURITY_PROTOCOL="SASL_SSL",
        KAFKA_SSL_CAFILE=str(tmp_path / "not-deployed.pem"),
    )
    with pytest.raises(RuntimeError, match="KAFKA_SSL_CAFILE"):
        kafka.ssl_context_for(settings)


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


# ── Failure diagnosis (preflight) ────────────────────────────────────
#
# Each cause below maps to a *different* fix, so they must not collapse into one
# "events are dropped" line: a CA, a password, a topic, or an ACL.


@pytest.mark.parametrize(
    ("exc", "expected_cause"),
    [
        (ssl.SSLError("certificate verify failed"), "tls"),
        (kafka_errors.AuthenticationFailedError(), "auth"),
        (kafka_errors.SaslAuthenticationFailed(), "auth"),
        (kafka_errors.UnsupportedSaslMechanismError(), "auth"),
        (kafka_errors.TopicAuthorizationFailedError(), "write_denied"),
        (kafka_errors.GroupAuthorizationFailedError(), "write_denied"),
        (kafka_errors.ClusterAuthorizationFailedError(), "cluster_acl"),
        (kafka_errors.UnknownTopicOrPartitionError(), "topic_missing"),
        (kafka_errors.KafkaConnectionError(), "unreachable"),
        (TimeoutError(), "timeout"),
        (RuntimeError("something else"), "unknown"),
    ],
)
def test_classify_kafka_failure_names_the_cause(exc, expected_cause):
    cause, remedy = kafka.classify_kafka_failure(exc)
    assert cause == expected_cause
    assert remedy, "every cause carries the fix, not just a label"


def test_classify_kafka_failure_handles_a_missing_exception():
    cause, remedy = kafka.classify_kafka_failure(None)
    assert cause == "unknown"
    assert remedy


def test_preflight_classifies_a_captured_failure_without_reconnecting(eventbus_env, monkeypatch):
    """The probe already failed: classify it instead of paying for a second round trip."""
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_BOOTSTRAP_SERVERS="localhost:9092")

    def explode(*_args, **_kwargs):
        raise AssertionError("preflight must not open a producer when a failure is supplied")

    monkeypatch.setattr(kafka, "AIOKafkaProducer", explode)

    result = asyncio.run(kafka.preflight(settings, failure=kafka_errors.TopicAuthorizationFailedError()))

    assert result["ok"] is False
    assert result["cause"] == "write_denied"
    assert "Write" in result["remedy"]
    assert result["topic"] == settings.kafka_topic


def test_preflight_reports_a_forgotten_bootstrap_list(eventbus_env):
    """An enabled backend without a bootstrap list must not be reported as "disabled"."""
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka")
    assert settings.events_enabled is False  # the naive check that used to answer first

    result = asyncio.run(kafka.preflight(settings))

    assert result["ok"] is False
    assert result["cause"] == "not_configured"
    assert "KAFKA_BOOTSTRAP_SERVERS" in result["detail"]


def test_preflight_reports_a_disabled_backend_as_ok(eventbus_env):
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="off")

    result = asyncio.run(kafka.preflight(settings))

    assert result["ok"] is True
    assert result["cause"] == "disabled"


def test_offline_problems_name_the_missing_ca_and_credentials(eventbus_env, tmp_path, monkeypatch):
    monkeypatch.delenv("AIVEN_CA_CERT", raising=False)
    settings = eventbus_env(
        EVENTBUS_EVENTS_BACKEND="kafka",
        KAFKA_BOOTSTRAP_SERVERS="broker:9092",
        KAFKA_SECURITY_PROTOCOL="SASL_SSL",
        KAFKA_SSL_CAFILE=str(tmp_path / "not-deployed.pem"),
    )

    problems = kafka._offline_problems(settings)

    assert any("KAFKA_SSL_CAFILE" in problem for problem in problems)
    assert any("SASL" in problem for problem in problems)


def test_preflight_names_a_topic_the_broker_does_not_have(eventbus_env, monkeypatch):
    """The real-world symptom: the send retries until it times out, but the cause is the topic.

    ``Topic X not found in cluster metadata`` is only an aiokafka log line, and
    the resulting ``TimeoutError`` reads like a network problem, so the metadata is
    checked explicitly before the write is attempted.
    """
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_BOOTSTRAP_SERVERS="localhost:9092")

    class NoTopicProducer(FakeProducer):
        def __init__(self):
            super().__init__()
            self.client = SimpleNamespace(
                cluster=SimpleNamespace(topics=lambda exclude_internal_topics=True: {"some-other-topic"})
            )

        async def partitions_for(self, topic):
            raise kafka_errors.KafkaTimeoutError()

    monkeypatch.setattr(kafka, "AIOKafkaProducer", lambda **kwargs: NoTopicProducer())

    result = asyncio.run(kafka.preflight(settings))

    assert result["ok"] is False
    assert result["cause"] == "topic_missing"
    assert settings.kafka_topic in result["detail"]
    assert "Create the topic" in result["remedy"]


def test_preflight_confirms_a_working_event_log(eventbus_env, monkeypatch):
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_BOOTSTRAP_SERVERS="localhost:9092")
    topic = settings.kafka_topic

    class GoodProducer(FakeProducer):
        def __init__(self):
            super().__init__()
            self.client = SimpleNamespace(cluster=SimpleNamespace(topics=lambda exclude_internal_topics=True: {topic}))

        async def partitions_for(self, _topic):
            return [0]

    producer = GoodProducer()
    monkeypatch.setattr(kafka, "AIOKafkaProducer", lambda **kwargs: producer)

    result = asyncio.run(kafka.preflight(settings))

    assert result["ok"] is True
    assert result["cause"] == "ok"
    assert topic in result["detail"]
    # The probe is operational, not a job event: no job id, so a reader ignores it.
    assert producer.sent
    assert messages.decode(producer.sent[0]["value"])["type"] == messages.EVENTS_PROBE


def test_startup_verification_reports_the_cause(eventbus_env, monkeypatch, caplog):
    """A denial must be named at startup, not left as "events are dropped"."""

    class DeniedProducer(FakeProducer):
        async def send_and_wait(self, topic, value=None, key=None):
            raise kafka_errors.TopicAuthorizationFailedError()

    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_BOOTSTRAP_SERVERS="localhost:9092")
    bus = KafkaEventBus(settings, producer_factory=lambda _settings: DeniedProducer())
    monkeypatch.setattr(eventbus, "get_bus", lambda: bus)

    with caplog.at_level(logging.ERROR):
        ok = asyncio.run(eventbus.verify_events_startup())

    assert ok is False
    message = next(record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR)
    assert "cannot be published" in message
    assert "write_denied" in message
    # The remedy must be the ACL fix, not the cluster one.
    assert "Write" in message
    assert "IdempotentWrite" not in message
