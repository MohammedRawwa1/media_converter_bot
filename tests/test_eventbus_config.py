"""Backend selection, safe degradation and the rollout decision.

No broker is involved: these tests pin the *configuration* behaviour, which is
what decides whether the layer can affect production at all. The property that
matters most is that a wrong or missing setting leaves the existing Redis path in
charge rather than breaking job processing.
"""

from utils.eventbus import config


def test_defaults_leave_the_existing_path_in_charge(eventbus_env):
    settings = eventbus_env()
    assert settings.queue_backend == "redis"
    assert settings.queue_rollout_percent == 0
    assert settings.events_backend == "off"
    assert not settings.queue_enabled
    assert not settings.events_enabled
    assert settings.consumes_redis is True
    assert settings.consumes_rabbitmq is False
    assert config.queue_backend_for_job({"job_id": "anything"}) == "redis"


def test_unknown_queue_backend_degrades_to_redis(eventbus_env):
    settings = eventbus_env(EVENTBUS_QUEUE_BACKEND="amqp-ish")
    assert settings.queue_backend == "redis"
    assert settings.degraded_reason and "unknown" in settings.degraded_reason


def test_enabled_without_a_url_degrades_instead_of_breaking(eventbus_env):
    settings = eventbus_env(EVENTBUS_QUEUE_BACKEND="rabbitmq", EVENTBUS_QUEUE_ROLLOUT_PERCENT=100)
    assert settings.queue_backend == "redis"
    assert settings.degraded_reason == "RABBITMQ_URL is not set"
    assert config.queue_backend_for_job({"job_id": "j"}) == "redis"
    # The reason is reported rather than silent, and no URL/secret is echoed.
    assert "amqp" not in str(settings.describe())


def test_enabled_without_the_client_library_degrades(eventbus_env, monkeypatch):
    monkeypatch.setattr(config, "rabbitmq_available", lambda: False)
    settings = eventbus_env(
        EVENTBUS_QUEUE_BACKEND="rabbitmq",
        EVENTBUS_QUEUE_ROLLOUT_PERCENT=100,
        RABBITMQ_URL="amqp://guest:guest@localhost:5672/",
    )
    assert settings.queue_backend == "redis"
    assert "aio-pika" in (settings.degraded_reason or "")
    # The URL is configured, so the only thing standing in the way is the client
    # library - and that is reported rather than guessed at.
    assert settings.describe()["rabbitmq_configured"] is True
    assert settings.queue_enabled is False


def test_full_rollout_routes_every_job_to_the_broker(eventbus_env, monkeypatch):
    monkeypatch.setattr(config, "rabbitmq_available", lambda: True)
    settings = eventbus_env(
        EVENTBUS_QUEUE_BACKEND="rabbitmq",
        EVENTBUS_QUEUE_ROLLOUT_PERCENT=100,
        RABBITMQ_URL="amqp://guest:guest@localhost:5672/",
    )
    assert settings.queue_enabled is True
    assert all(config.queue_backend_for_job({"job_id": f"j{i}"}) == "rabbitmq" for i in range(50))
    assert settings.consumes_rabbitmq is True
    # Nothing new goes to Redis, but any job left in the list still has to be
    # drained - so the worker keeps polling it.
    assert settings.consumes_redis is False


def test_zero_rollout_keeps_every_job_on_redis(eventbus_env, monkeypatch):
    monkeypatch.setattr(config, "rabbitmq_available", lambda: True)
    eventbus_env(
        EVENTBUS_QUEUE_BACKEND="rabbitmq",
        EVENTBUS_QUEUE_ROLLOUT_PERCENT=0,
        RABBITMQ_URL="amqp://guest:guest@localhost:5672/",
    )
    assert config.get_settings().queue_enabled is False
    assert all(config.queue_backend_for_job({"job_id": f"j{i}"}) == "redis" for i in range(50))
    # The consumer still runs: a rollback must drain what is already queued in the
    # broker instead of stranding it there.
    assert config.get_settings().consumes_rabbitmq is True
    assert config.get_settings().consumes_redis is True


def test_the_consumer_runs_even_at_a_zero_rollout(eventbus_env, monkeypatch):
    monkeypatch.setattr(config, "rabbitmq_available", lambda: True)
    settings = eventbus_env(
        EVENTBUS_QUEUE_BACKEND="rabbitmq",
        EVENTBUS_QUEUE_ROLLOUT_PERCENT=0,
        RABBITMQ_URL="amqp://guest:guest@localhost:5672/",
    )
    assert settings.consumes_rabbitmq is True


def test_a_degraded_backend_does_not_start_a_consumer(eventbus_env, monkeypatch):
    monkeypatch.setattr(config, "rabbitmq_available", lambda: True)
    # No RABBITMQ_URL: there is nothing to connect to and nothing could have been
    # queued, so no consumer is started either.
    degraded = eventbus_env(EVENTBUS_QUEUE_BACKEND="rabbitmq", EVENTBUS_QUEUE_ROLLOUT_PERCENT=100)
    assert degraded.queue_backend == "redis"
    assert degraded.consumes_rabbitmq is False
    assert degraded.consumes_redis is True


def test_partial_rollout_splits_traffic_deterministically(eventbus_env, monkeypatch):
    monkeypatch.setattr(config, "rabbitmq_available", lambda: True)
    settings = eventbus_env(
        EVENTBUS_QUEUE_BACKEND="rabbitmq",
        EVENTBUS_QUEUE_ROLLOUT_PERCENT=25,
        RABBITMQ_URL="amqp://guest:guest@localhost:5672/",
    )
    assert settings.queue_enabled is True
    assert settings.consumes_redis is True and settings.consumes_rabbitmq is True

    job_ids = [f"job-{i}" for i in range(1000)]
    decisions = [config.queue_backend_for_job({"job_id": job_id}) for job_id in job_ids]
    share = decisions.count("rabbitmq") / len(decisions)

    # A quarter of the traffic, within the noise a 1000-sample hash can produce.
    assert 0.20 <= share <= 0.30, share
    # Every producer (bot, fetcher, ingest tool) has to agree on the same job id,
    # so the decision must not depend on process-local state.
    assert decisions == [config.queue_backend_for_job({"job_id": job_id}) for job_id in job_ids]


def test_job_without_an_id_keeps_using_redis_during_a_partial_rollout(eventbus_env, monkeypatch):
    monkeypatch.setattr(config, "rabbitmq_available", lambda: True)
    eventbus_env(
        EVENTBUS_QUEUE_BACKEND="rabbitmq",
        EVENTBUS_QUEUE_ROLLOUT_PERCENT=50,
        RABBITMQ_URL="amqp://guest:guest@localhost:5672/",
    )
    # There is nothing to hash, so there is no way to be deterministic about it:
    # such a job stays on the path that is already known to work.
    assert config.queue_backend_for_job({}) == "redis"
    assert config.queue_backend_for_job(None) == "redis"


def test_full_rollout_does_not_depend_on_an_id(eventbus_env, monkeypatch):
    monkeypatch.setattr(config, "rabbitmq_available", lambda: True)
    eventbus_env(
        EVENTBUS_QUEUE_BACKEND="rabbitmq",
        EVENTBUS_QUEUE_ROLLOUT_PERCENT=100,
        RABBITMQ_URL="amqp://guest:guest@localhost:5672/",
    )
    # At 100% the share is the whole traffic, so no bucketing happens and an
    # id-less job is not treated as a special case.
    assert config.queue_backend_for_job({}) == "rabbitmq"
    assert config.queue_backend_for_job(None) == "rabbitmq"


def test_rollout_is_clamped(eventbus_env, monkeypatch):
    monkeypatch.setattr(config, "rabbitmq_available", lambda: True)
    settings = eventbus_env(
        EVENTBUS_QUEUE_BACKEND="rabbitmq",
        EVENTBUS_QUEUE_ROLLOUT_PERCENT=250,
        RABBITMQ_URL="amqp://guest:guest@localhost:5672/",
    )
    assert settings.queue_rollout_percent == 100
    settings = eventbus_env(
        EVENTBUS_QUEUE_BACKEND="rabbitmq",
        EVENTBUS_QUEUE_ROLLOUT_PERCENT=-5,
        RABBITMQ_URL="amqp://guest:guest@localhost:5672/",
    )
    assert settings.queue_rollout_percent == 0


def test_events_need_a_bootstrap_server(eventbus_env):
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka")
    assert settings.events_enabled is False
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="kafka", KAFKA_BOOTSTRAP_SERVERS="localhost:9092")
    assert settings.events_enabled is True
    assert settings.kafka_topic == "media.job.events"


def test_unknown_events_backend_disables_events(eventbus_env):
    settings = eventbus_env(EVENTBUS_EVENTS_BACKEND="rabbitmq", KAFKA_BOOTSTRAP_SERVERS="localhost:9092")
    assert settings.events_backend == "off"
    assert settings.events_enabled is False


def test_describe_never_prints_credentials(eventbus_env):
    settings = eventbus_env(
        EVENTBUS_QUEUE_BACKEND="rabbitmq",
        EVENTBUS_QUEUE_ROLLOUT_PERCENT=10,
        RABBITMQ_URL="amqps://user:sup3rsecret@broker.example.com:5671/vhost",
        EVENTBUS_EVENTS_BACKEND="kafka",
        KAFKA_BOOTSTRAP_SERVERS="broker:9092",
        KAFKA_SASL_MECHANISM="SCRAM-SHA-256",
    )
    blob = str(settings.describe())
    assert "sup3rsecret" not in blob
    assert "amqps://" not in blob
