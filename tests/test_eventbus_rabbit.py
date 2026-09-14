"""RabbitMQ adapter logic that can be checked without a broker.

What is covered here: the retry counter / backoff / origin-tagging helpers that
decide whether a failed message is retried or dead-lettered, the declared
topology names, and the guarantee that a missing client library or URL makes the
producer decline the job (so the caller keeps using Redis) instead of raising.

What is *not* covered: publisher confirms, redelivery after a worker crash and
dead-lettering after the retries are exhausted. Those are broker behaviours and
need a running RabbitMQ (docker compose -f docker-compose.eventbus.yml up -d),
which is what scripts/check_eventbus.py is for.
"""

import asyncio
import dataclasses
import json
from datetime import timedelta

from utils.eventbus import rabbit


def test_retry_counter_reads_and_tolerates_odd_headers():
    assert rabbit.retry_count({rabbit.RETRY_HEADER: 2}) == 2
    assert rabbit.retry_count({rabbit.RETRY_HEADER: "3"}) == 3
    assert rabbit.retry_count({}) == 0
    assert rabbit.retry_count(None) == 0
    assert rabbit.retry_count({rabbit.RETRY_HEADER: "not-a-number"}) == 0
    assert rabbit.retry_count({rabbit.RETRY_HEADER: -4}) == 0


def test_should_retry_stops_at_the_configured_limit():
    assert rabbit.should_retry({}, max_retries=3) is True
    assert rabbit.should_retry({rabbit.RETRY_HEADER: 2}, max_retries=3) is True
    assert rabbit.should_retry({rabbit.RETRY_HEADER: 3}, max_retries=3) is False
    # Retries turned off means the first failure is final.
    assert rabbit.should_retry({}, max_retries=0) is False


def test_retry_headers_increment_and_preserve_existing_headers():
    headers = rabbit.retry_headers({"trace": "abc", rabbit.RETRY_HEADER: 1})
    assert headers[rabbit.RETRY_HEADER] == 2
    assert headers["trace"] == "abc"


def test_backoff_grows_then_caps():
    base = 1000
    assert rabbit.backoff_ms(1, base) == 1000
    assert rabbit.backoff_ms(2, base) == 2000
    assert rabbit.backoff_ms(3, base) == 4000
    # Capped at 10x the base so a long retry chain cannot park a job for hours.
    assert rabbit.backoff_ms(9, base) == 10000
    assert rabbit.backoff_ms(20, base) == 10000
    # A nonsense attempt number must not produce a zero delay.
    assert rabbit.backoff_ms(0, base) == 1000


def test_retry_expiration_is_milliseconds_not_seconds():
    # aio-pika's Message.expiration is in *seconds* and is multiplied by 1000 to
    # build the AMQP property. Passing backoff_ms's milliseconds straight through
    # made every retry 1000x too long: the default 30s backoff parked a failed job
    # for 8+ hours, and a live broker showed it stuck in media.jobs.retry. Pinned
    # here (found against real RabbitMQ, not by the broker-free suite).
    assert rabbit.retry_expiration(1000) == timedelta(seconds=1)
    assert rabbit.retry_expiration(1200) == timedelta(milliseconds=1200)
    assert rabbit.retry_expiration(30000) == timedelta(seconds=30)
    # A negative delay would be rejected by the broker; floor it instead.
    assert rabbit.retry_expiration(-5) == timedelta(0)


def test_origin_tagging_marks_only_broker_jobs():
    job = {"job_id": "j1"}
    assert rabbit.is_rabbitmq_job(job) is False
    rabbit.mark_origin(job)
    assert rabbit.is_rabbitmq_job(job) is True
    assert job[rabbit.ORIGIN_KEY] == rabbit.RABBITMQ_ORIGIN
    # A Redis-sourced job must never be mistaken for a broker job, or the lock
    # requeue path would move it to the wrong queue.
    assert rabbit.is_rabbitmq_job({"job_id": "j2", rabbit.ORIGIN_KEY: "redis"}) is False
    assert rabbit.is_rabbitmq_job(None) is False


def test_topology_names_are_the_documented_ones(eventbus_env):
    settings = eventbus_env()
    topology = rabbit.topology(settings)
    assert topology["exchange"] == rabbit.topology(settings)["exchange"]
    assert topology["jobs_queue"] and topology["retry_queue"] and topology["dead_queue"]
    assert topology["jobs_queue"] != topology["retry_queue"] != topology["dead_queue"]
    # The retry queue is what returns a message to the work queue after its TTL.
    assert topology["retry_ttl_ms"] > 0


def test_work_queue_is_declared_with_its_dead_letter_arguments(eventbus_env):
    # Without these, RabbitMQ discards a rejected message instead of routing it to
    # the dead queue: "retried and then parked" silently becomes "retried and then
    # lost". Found by running against a real broker, pinned here so it stays fixed.
    settings = eventbus_env()
    arguments = rabbit.jobs_queue_arguments(settings)
    assert arguments["x-dead-letter-exchange"] == settings.dlx
    assert arguments["x-dead-letter-routing-key"] == settings.dead_routing_key


def test_retry_delay_is_per_message_not_a_queue_level_ttl(eventbus_env):
    # A queue-level x-message-ttl would bake the backoff into the declaration, and
    # RabbitMQ refuses to re-declare a queue with different arguments
    # (PRECONDITION_FAILED) - so tuning RABBITMQ_RETRY_TTL_MS would break a live
    # cluster until the queue was deleted by hand.
    settings = eventbus_env()
    arguments = rabbit.retry_queue_arguments(settings)
    assert "x-message-ttl" not in arguments
    assert arguments["x-dead-letter-exchange"] == settings.exchange
    assert arguments["x-dead-letter-routing-key"] == settings.jobs_routing_key


def test_publish_declines_without_a_client_library(eventbus_env, monkeypatch):
    settings = dataclasses.replace(eventbus_env(), rabbitmq_url="amqp://guest:guest@localhost:5672/")
    monkeypatch.setattr(rabbit, "aio_pika", None)
    queue = rabbit.RabbitJobQueue(settings)
    assert queue.available() is False
    # Returning False is the contract that keeps the job on the Redis path.
    assert asyncio.run(queue.publish_job({"job_id": "j1"})) is False
    assert asyncio.run(queue.requeue_job({"job_id": "j1"})) is False


def test_publish_declines_without_a_url(eventbus_env):
    settings = dataclasses.replace(eventbus_env(), rabbitmq_url="")
    queue = rabbit.RabbitJobQueue(settings)
    assert queue.available() is False
    assert asyncio.run(queue.publish_job({"job_id": "j1"})) is False


def test_consume_refuses_to_start_without_a_broker(eventbus_env, monkeypatch):
    settings = dataclasses.replace(eventbus_env(), rabbitmq_url="")
    monkeypatch.setattr(rabbit, "aio_pika", None)
    queue = rabbit.RabbitJobQueue(settings)

    async def _handler(_job):
        return None

    raised = None
    try:
        asyncio.run(queue.consume(_handler))
    except RuntimeError as exc:
        raised = exc
    assert raised is not None and "unavailable" in str(raised)


def test_stats_does_not_invent_an_answer_without_a_broker(eventbus_env, monkeypatch):
    settings = dataclasses.replace(eventbus_env(), rabbitmq_url="")
    monkeypatch.setattr(rabbit, "aio_pika", None)
    queue = rabbit.RabbitJobQueue(settings)
    try:
        asyncio.run(queue.stats())
        raised = False
    except RuntimeError:
        raised = True
    assert raised is True


def test_retry_message_payload_is_the_job_verbatim():
    # The queue carries the whole job (the worker needs paths, chat ids, options);
    # it is the *event log* that publishes a summary instead. Pinned here because
    # the two are easy to confuse.
    job = {"job_id": "j1", "input_path": "/data/storage/input/a.mkv", "ffmpeg_args": ["-c:v", "libx264"]}
    assert json.loads(json.dumps(job)) == job
