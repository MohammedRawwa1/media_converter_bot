"""Tests for the shared /health payload.

What is covered: that a healthy pair of pipes reports ``ok`` with real depths, that
each way a pipe can fail (no Redis, silent Redis, unreadable broker queue) degrades
the payload instead of raising or hanging, and that the event-bus description is
included when it can be resolved.

The failures matter more than the happy path: /health backs the platform
healthcheck and the keep-alive ping, so it has to answer even when everything it
watches is broken.
"""

import asyncio

import pytest
from test_queue_admin import FakeRedis, _job

import utils.eventbus as eventbus
from utils import health, job_queue
from utils.eventbus import rabbit as rabbit_module


@pytest.fixture
def fake_redis(monkeypatch, eventbus_env):
    """Route the probe at a fake client and keep the broker stubbed out.

    ``eventbus_env`` clears the event-bus env vars, so a developer machine with a
    real RABBITMQ_URL cannot make the probe reach for a live broker.
    """
    client = FakeRedis()

    async def _get_redis():
        return client

    monkeypatch.setattr(job_queue, "get_redis", _get_redis)
    return client


def _broker(monkeypatch, *, consumes_rabbitmq, stats=None, raises=None):
    class StubSettings:
        def __init__(self):
            self.consumes_rabbitmq = consumes_rabbitmq

    class StubQueue:
        async def stats(self):
            if raises is not None:
                raise raises
            return stats

    monkeypatch.setattr(eventbus, "get_settings", lambda: StubSettings())
    monkeypatch.setattr(rabbit_module, "get_queue", lambda: StubQueue())


def _payload(**kwargs):
    return asyncio.run(health.collect_health(**kwargs))


def test_healthy_pipes_report_ok_with_real_depths(fake_redis, monkeypatch):
    fake_redis.lists[job_queue.JOB_LIST] = [_job("queued-1"), _job("queued-2")]
    fake_redis.zsets[job_queue.DELAYED_SET] = [_job("delayed-1")]
    _broker(monkeypatch, consumes_rabbitmq=False)
    monkeypatch.setattr(eventbus, "describe", lambda: {"queue_backend": "redis"})

    payload = _payload()

    assert payload["status"] == "ok"
    assert payload["redis"] == {
        "connected": True,
        "ping_ms": payload["redis"]["ping_ms"],
        "queue_depth": 2,
        "delayed": 1,
        "error": None,
    }
    assert payload["redis"]["ping_ms"] is not None

    # A Redis-only deployment has no broker to be degraded about.
    assert payload["broker"] == {"backend": "redis", "connected": None, "queues": {}, "error": None}
    assert payload["eventbus"] == {"queue_backend": "redis"}


def test_unreachable_redis_degrades_instead_of_raising(monkeypatch, eventbus_env):
    async def _boom():
        raise RuntimeError("REDIS_URL is not set")

    monkeypatch.setattr(job_queue, "get_redis", _boom)
    _broker(monkeypatch, consumes_rabbitmq=False)

    payload = _payload()

    assert payload["status"] == "degraded"
    assert payload["redis"]["connected"] is False
    assert payload["redis"]["queue_depth"] is None
    assert "redis unavailable" in payload["redis"]["error"]


def test_silent_redis_degrades_via_the_probe_timeout(fake_redis, monkeypatch):
    async def _hang():
        await asyncio.sleep(30)

    monkeypatch.setattr(health, "PROBE_TIMEOUT_SECONDS", 0.01)
    fake_redis.ping = _hang
    _broker(monkeypatch, consumes_rabbitmq=False)

    payload = _payload()

    assert payload["status"] == "degraded"
    assert payload["redis"]["connected"] is False
    assert payload["redis"]["error"]


def test_broker_depths_are_reported_when_the_broker_answers(fake_redis, monkeypatch):
    depths = {"media.jobs.run": 3, "media.jobs.retry": 0, "media.jobs.dead": 1}
    _broker(monkeypatch, consumes_rabbitmq=True, stats=depths)

    payload = _payload()

    assert payload["status"] == "ok"
    assert payload["broker"]["backend"] == "rabbitmq"
    assert payload["broker"]["connected"] is True
    assert payload["broker"]["queues"] == depths


def test_unreachable_broker_degrades_while_redis_stays_ok(fake_redis, monkeypatch):
    _broker(monkeypatch, consumes_rabbitmq=True, raises=TimeoutError("broker did not answer"))

    payload = _payload()

    assert payload["redis"]["connected"] is True
    assert payload["broker"]["connected"] is False
    assert payload["status"] == "degraded"
    assert "broker unavailable" in payload["broker"]["error"]


def test_broker_queue_that_cannot_be_read_counts_as_degraded(fake_redis, monkeypatch):
    """stats() reports a string for a queue it could not read; that is not healthy."""
    _broker(
        monkeypatch,
        consumes_rabbitmq=True,
        stats={"media.jobs.run": "unavailable: channel closed", "media.jobs.retry": 0, "media.jobs.dead": 0},
    )

    payload = _payload()

    assert payload["broker"]["connected"] is False
    assert payload["status"] == "degraded"


def test_eventbus_description_failure_is_reported_as_none(fake_redis, monkeypatch):
    def _boom():
        raise RuntimeError("settings exploded")

    _broker(monkeypatch, consumes_rabbitmq=False)
    monkeypatch.setattr(eventbus, "describe", _boom)

    payload = _payload()

    assert payload["eventbus"] is None
    assert payload["status"] == "ok"


def test_payload_survives_everything_being_broken(monkeypatch, eventbus_env):
    """Redis, settings and describe all failing must still yield one payload."""

    async def _redis_boom():
        raise RuntimeError("no redis")

    def _settings_boom():
        raise RuntimeError("no settings")

    def _describe_boom():
        raise RuntimeError("no describe")

    monkeypatch.setattr(job_queue, "get_redis", _redis_boom)
    monkeypatch.setattr(eventbus, "get_settings", _settings_boom)
    monkeypatch.setattr(eventbus, "describe", _describe_boom)

    payload = _payload()

    assert payload["status"] == "degraded"
    assert payload["redis"]["connected"] is False
    assert payload["broker"]["connected"] is None
    assert "event bus settings unavailable" in payload["broker"]["error"]
    assert payload["eventbus"] is None
