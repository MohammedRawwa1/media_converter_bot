"""Shared pytest fixtures for the test suite."""

import os
import sys

import pytest

# Allow `from utils import ...` no matter which directory pytest is invoked from.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.eventbus import config  # noqa: E402

EVENTBUS_ENV_KEYS = (
    "EVENTBUS_QUEUE_BACKEND",
    "EVENTBUS_QUEUE_ROLLOUT_PERCENT",
    "EVENTBUS_EVENTS_BACKEND",
    "EVENTBUS_REQUIRE_BROKERS",
    "RABBITMQ_URL",
    "RABBITMQ_MAX_RETRIES",
    "RABBITMQ_PREFETCH",
    "RABBITMQ_RETRY_TTL_MS",
    "RABBITMQ_JOBS_QUEUE",
    "RABBITMQ_EXCHANGE",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_EVENTS_TOPIC",
    "KAFKA_EMIT_PROGRESS_EVENTS",
    "KAFKA_PROGRESS_MIN_INTERVAL_MS",
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
    "KAFKA_SSL_CAFILE",
)


@pytest.fixture
def eventbus_env(monkeypatch):
    """Clear the event-bus env vars, then apply the ones a test needs.

    Returns a callable: ``settings = eventbus_env(EVENTBUS_QUEUE_BACKEND="rabbitmq")``.
    The settings cache is reset before and after so tests never see each other's
    configuration.
    """
    for key in EVENTBUS_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    config.reset_settings_cache()

    def _apply(**values):
        for key, value in values.items():
            monkeypatch.setenv(key, str(value))
        config.reset_settings_cache()
        return config.get_settings()

    yield _apply
    config.reset_settings_cache()
