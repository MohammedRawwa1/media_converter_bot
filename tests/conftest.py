"""Shared pytest fixtures for the test suite."""

import contextlib
import os
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Allow `from utils import ...` no matter which directory pytest is invoked from,
# and `from source_helpers import ...` for the shared test helpers.
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# A value has to be long enough to look like a credential or a URL before it is
# treated as the developer's; short ones ("1", "true", "128k") are flags that
# unrelated variables may legitimately share.
_MIN_DOTENV_VALUE_LENGTH = 8

# The loader as it was before the suite denied it, so the session can put it back.
_ORIGINAL_LOAD_DOTENV = None


def _developer_dotenv_paths() -> list[str]:
    """The ``.env`` files ``config._load_environment_file`` reads, in its order."""
    return [
        os.path.join(PROJECT_ROOT, ".env"),
        os.path.join(os.getcwd(), ".env"),
        os.path.join(PROJECT_ROOT, ".env.local"),
    ]


def _read_developer_dotenv() -> tuple[frozenset[str], frozenset[str]]:
    """What a developer's ``.env`` defines: its names, and the values behind them.

    Both are empty on CI, and on a machine without the file. The values matter as
    well as the names, because one entry can reach the environment under another
    name: ``config`` copies ``MONGO_URI`` into ``MONGODB_URL``, ``MONGO_URL`` and
    ``MONGODB_URI`` while it is imported, so reading any of those is reading the
    developer's file.
    """
    try:
        from dotenv import dotenv_values
    except Exception:  # python-dotenv is optional in production
        return frozenset(), frozenset()

    names: set[str] = set()
    values: set[str] = set()
    for path in _developer_dotenv_paths():
        if not os.path.exists(path):
            continue
        with contextlib.suppress(Exception):
            for name, value in dotenv_values(path).items():
                if not name:
                    continue
                names.add(name)
                if value and len(value) >= _MIN_DOTENV_VALUE_LENGTH:
                    values.add(value)
    return frozenset(names), frozenset(values)


_DOTENV_NAMES, _DOTENV_VALUES = _read_developer_dotenv()


def _hide_developer_dotenv() -> None:
    """Remove every trace of the developer's ``.env`` from the environment.

    ``config`` runs ``load_dotenv`` every time it is imported and reads the values
    with ``os.getenv``, so a developer's ``.env`` otherwise reaches the whole suite
    while CI - which has no such file - sees none of it. That is how three Kafka
    tests came to fail locally while passing in CI: the file's ``AIVEN_CA_CERT``
    reached the TLS helper, which materialised it into a temporary CA file.

    Tests that want a variable set it themselves, which is what the fixtures here
    do. Hidden are the names the file defines and any variable already carrying one
    of its values, so an alias cannot smuggle a value back in.
    """
    hidden = set(_DOTENV_NAMES)
    hidden.update(name for name, value in os.environ.items() if value in _DOTENV_VALUES)
    for name in hidden:
        os.environ.pop(name, None)


def _deny_loading_dotenv() -> None:
    """Stop the session from reading a ``.env`` file back into the environment.

    pytest re-imports ``config`` while it collects test modules, and each import
    looks for the file again, so the denial has to outlive this import.
    """
    global _ORIGINAL_LOAD_DOTENV

    try:
        import dotenv
    except Exception:  # python-dotenv is optional in production
        return

    def _deny(*_args, **_kwargs) -> bool:
        return False

    _ORIGINAL_LOAD_DOTENV = getattr(dotenv, "load_dotenv", None)
    dotenv.load_dotenv = _deny


# Before any test module is imported: this is the first import that would read a
# developer's ``.env`` into the environment for the rest of the session.
_deny_loading_dotenv()
_hide_developer_dotenv()

# Read the constants once, now, so they are built from the cleaned environment.
with contextlib.suppress(Exception):
    import config  # noqa: F401

# Importing ``config`` puts the Mongo URL back into the environment under the four
# names it answers to, so hide the file's values again - and once per test below.
_hide_developer_dotenv()


@pytest.fixture(autouse=True)
def _hermetic_environment() -> None:
    """Re-hide the developer's ``.env`` before every test.

    A module imported during collection can write a value straight into
    ``os.environ`` after the pop above; hiding it per test means no test ever
    observes one. Tests that need a value set it after this, in their own fixture
    or body.
    """
    _hide_developer_dotenv()
    yield


def pytest_unconfigure(config) -> None:
    """Give the real ``load_dotenv`` back once the suite is done."""
    if _ORIGINAL_LOAD_DOTENV is None:
        return
    with contextlib.suppress(Exception):
        import dotenv

        dotenv.load_dotenv = _ORIGINAL_LOAD_DOTENV


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
    # Read straight from the environment by the Kafka adapter (not through
    # ``EventBusSettings``), so it has to be cleared here too: a value from a
    # developer's ``.env`` or shell would otherwise make the "no CA configured"
    # cases fail, since it is materialised into a temporary file. Tests that need
    # it set it themselves.
    "AIVEN_CA_CERT",
)


@pytest.fixture
def developer_dotenv_keys() -> set[str]:
    """Names a developer's ``.env`` file defines (empty on CI or without one)."""
    return set(_DOTENV_NAMES)


@pytest.fixture
def developer_dotenv_values() -> set[str]:
    """The values a developer's ``.env`` file defines (empty on CI or without one)."""
    return set(_DOTENV_VALUES)


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
