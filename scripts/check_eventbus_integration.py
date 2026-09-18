"""Real-broker verification of the RabbitMQ work queue and the Kafka event log.

Unlike tests/ (which is broker-free), this drives a live stack:

    docker compose -f docker-compose.eventbus.yml up -d
    python scripts/check_eventbus_integration.py

What it proves, with the brokers actually running:

  A. a job is published durably and the broker *confirms* it (publisher confirms)
  B. the message is acked only after the handler returns, and is not redelivered
  C. a failing handler is retried through the delay queue, the attempt counter in
     the header goes up, and the same job comes back
  D. a job that keeps failing is dead-lettered to media.jobs.dead
  E. lifecycle events reach Kafka and can be read back, in order, keyed by job id
  F. a stop event ends consumption promptly, leaving an unprocessed delivery in
     the queue rather than losing it
  G. the worker's own consumer task (workers.ffmpeg_worker) processes a job end
     to end with only the handler stubbed, and emits its started event

Configuration: .env is loaded the same way the application loads it, so topic
names and topology overrides come from the real config. The values these checks
depend on - a localhost broker, short retry timings, a 100% rollout - are applied
on top of it unless you export them yourself, so a production-sized
RABBITMQ_RETRY_TTL_MS cannot make the retry scenarios time out.

**Destructive:** it purges the configured queues first. It refuses to run unless
the broker is on localhost/127.0.0.1, so it cannot be pointed at production by
accident (override with --force only if you mean it).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
import uuid
from contextlib import suppress

# Snapshot the real environment before .env is loaded, so an explicit export is
# still distinguishable from a value that came out of the file.
_SHELL_ENV = dict(os.environ)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Imported for its side effect: ``config`` loads .env the way the application
# does (root/.env, cwd/.env, .env.local), without overriding an exported value.
import config  # noqa: E402,F401


def _default(key: str, value: str) -> None:
    """Apply a harness default unless the caller exported ``key`` themselves.

    These are the values the checks depend on - localhost brokers, so the
    destructive purge cannot reach a managed instance, and short retry timings,
    so the retry/dead-letter scenarios finish in seconds. They therefore take
    precedence over .env; exporting the variable is how you point the script at
    another stack (which the localhost guard still vets).
    """
    if not _SHELL_ENV.get(key):
        os.environ[key] = value


_default("EVENTBUS_QUEUE_BACKEND", "rabbitmq")
_default("EVENTBUS_QUEUE_ROLLOUT_PERCENT", "100")
_default("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
_default("RABBITMQ_MAX_RETRIES", "1")
_default("RABBITMQ_RETRY_TTL_MS", "1200")
_default("EVENTBUS_EVENTS_BACKEND", "kafka")
_default("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_default("KAFKA_EMIT_PROGRESS_EVENTS", "true")

from utils import eventbus  # noqa: E402
from utils.eventbus import messages  # noqa: E402
from utils.eventbus.config import get_settings, reset_settings_cache  # noqa: E402
from utils.eventbus.rabbit import RETRY_HEADER  # noqa: E402

RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    RESULTS.append((name, bool(ok), detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {name}{'' if ok else '  -> ' + str(detail)}")
    return bool(ok)


def job(job_id: str, **extra) -> dict:
    return {
        "job_id": job_id,
        "request_id": f"req-{job_id}",
        "type": "ffmpeg",
        "chat_id": 424242,
        "input_path": "/data/storage/input/clip.mkv",
        "output_path": "/data/storage/output/clip.mp4",
        **extra,
    }


async def _drain_until(queue, queue_name, expected_ready, timeout=15.0):
    """Poll a queue depth until it reaches the expected value (or the timeout)."""
    deadline = time.monotonic() + timeout
    depth = None
    while time.monotonic() < deadline:
        depth = await queue.queue_depth(queue_name)
        if depth == expected_ready:
            return True, depth
        await asyncio.sleep(0.2)
    return False, depth


async def scenario_a_publish_confirmed(queue) -> None:
    print("\n--- A. publisher confirms ---")
    jid = f"itest-a-{uuid.uuid4().hex[:8]}"
    routed = eventbus.queue_backend_for_job({"job_id": jid})
    check("rollout routes this job to RabbitMQ", routed == "rabbitmq", routed)

    # The application's own entry point, not the adapter directly.
    ok = await eventbus.publish_job(job(jid))
    check("publish_job returned True (broker confirmed the write)", ok is True, repr(ok))
    reached, depth = await _drain_until(queue, get_settings().jobs_queue, 1)
    check("the message is in media.jobs.run", reached, f"depth={depth}")


async def scenario_b_ack_after_handler(queue) -> None:
    print("\n--- B. ack only after the handler returns ---")
    await queue.purge_queues()  # each scenario starts from an empty broker
    settings = get_settings()
    jid = f"itest-b-{uuid.uuid4().hex[:8]}"
    seen: list[dict] = []
    stop = asyncio.Event()

    async def handler(inbound: dict) -> None:
        seen.append(inbound)
        stop.set()

    await queue.publish_job(job(jid))
    await asyncio.wait_for(queue.consume(handler, stop_event=stop), timeout=15)

    check("the handler received the job", len(seen) == 1 and seen[0].get("job_id") == jid, str(seen)[:120])
    check(
        "the delivered job is tagged as broker-sourced",
        eventbus.is_rabbitmq_job(seen[0]),
        str(seen[0].get("_eventbus_origin")),
    )
    check(
        "the local paths survive the round trip",
        seen[0].get("input_path") == "/data/storage/input/clip.mkv",
        str(seen[0].get("input_path")),
    )
    empty, depth = await _drain_until(queue, settings.jobs_queue, 0, timeout=5)
    check("the message was acked (queue is empty)", empty, f"depth={depth}")
    await asyncio.sleep(1.5)
    replayed, depth2 = await _drain_until(queue, settings.jobs_queue, 0, timeout=3)
    check("nothing was redelivered afterwards", replayed, f"depth={depth2}")


async def scenario_c_retry(queue) -> None:
    print("\n--- C. a failed attempt is retried with a bumped counter ---")
    await queue.purge_queues()
    settings = get_settings()
    jid = f"itest-c-{uuid.uuid4().hex[:8]}"
    attempts: list[int] = []
    stop = asyncio.Event()

    async def handler(inbound: dict) -> None:
        attempts.append(len(attempts) + 1)
        if len(attempts) == 1:
            raise RuntimeError("simulated ffmpeg failure")
        stop.set()

    await queue.publish_job(job(jid))
    started = time.monotonic()
    # While the first attempt is failing, the message should be sitting in the
    # retry queue with the counter already at 1.
    task = asyncio.create_task(queue.consume(handler, stop_event=stop))
    ok, retry_depth = await _drain_until(queue, settings.retry_queue, 1, timeout=10)
    check("the failed message is parked in the retry queue", ok, f"depth={retry_depth}")

    retry_headers_seen = None
    if ok and queue._retry_queue is not None:  # noqa: SLF001 - deliberate: read the parked message
        parked = await queue._retry_queue.get(no_ack=False, fail=False)  # noqa: SLF001
        if parked is not None:
            retry_headers_seen = dict(parked.headers or {})
            check(
                "the parked message carries the incremented attempt counter",
                retry_headers_seen.get(RETRY_HEADER) == 1,
                str(retry_headers_seen),
            )
            # Assert the *value*, not just that one is set: a delay 1000x too long
            # (aio-pika reads seconds, the code used to hand it milliseconds) still
            # "has a delay set". aio-pika decodes expiration back to seconds.
            expected_delay_s = settings.retry_ttl_ms / 1000.0
            check(
                "the retry delay matches RABBITMQ_RETRY_TTL_MS (not 1000x longer)",
                parked.expiration is not None and abs(float(parked.expiration) - expected_delay_s) < 0.5,
                f"expiration={parked.expiration!r}s expected~{expected_delay_s}s",
            )
            await parked.nack(requeue=True)  # put it back so the delay queue can release it

    await asyncio.wait_for(task, timeout=25)
    elapsed = time.monotonic() - started
    check("the handler ran twice (one failure, one success)", len(attempts) == 2, str(attempts))
    check("the retry came back through the delay queue, not immediately", elapsed >= 1.0, f"{elapsed:.2f}s")
    drained, depth = await _drain_until(queue, settings.retry_queue, 0, timeout=5)
    check("the retry queue is empty again", drained, f"depth={depth}")
    work_empty, work_depth = await _drain_until(queue, settings.jobs_queue, 0, timeout=5)
    check("the successful attempt was acked", work_empty, f"depth={work_depth}")


async def scenario_d_dead_letter(queue) -> None:
    print("\n--- D. exhausting the retries dead-letters the job ---")
    await queue.purge_queues()
    settings = get_settings()
    jid = f"itest-d-{uuid.uuid4().hex[:8]}"
    attempts: list[int] = []
    stop = asyncio.Event()

    async def handler(inbound: dict) -> None:
        attempts.append(len(attempts) + 1)
        if len(attempts) >= 2:
            stop.set()
        raise RuntimeError("simulated permanent failure")

    await queue.publish_job(job(jid))
    await asyncio.wait_for(queue.consume(handler, stop_event=stop), timeout=25)

    check("the handler was tried max_retries+1 times", len(attempts) == 2, str(attempts))
    landed, depth = await _drain_until(queue, settings.dead_queue, 1, timeout=10)
    check("the job landed in the dead-letter queue", landed, f"depth={depth}")

    if landed and queue._dead_queue is not None:  # noqa: SLF001
        dead = await queue._dead_queue.get(no_ack=False, fail=False)  # noqa: SLF001
        if dead is not None:
            import json

            body = json.loads(dead.body.decode("utf-8"))
            check("the dead-lettered message is the original job", body.get("job_id") == jid, str(body)[:140])
            check(
                "the dead-lettered message left media.jobs.run",
                await queue.queue_depth(settings.jobs_queue) == 0,
            )
            await dead.ack()  # consumed for inspection


async def scenario_e_kafka(queue) -> None:
    print("\n--- E. lifecycle events reach Kafka and read back ---")
    settings = get_settings()
    if not settings.events_enabled:
        check("Kafka events are enabled", False, "EVENTBUS_EVENTS_BACKEND/KAFKA_BOOTSTRAP_SERVERS not set")
        return
    jid = f"itest-e-{uuid.uuid4().hex[:8]}"
    # A job carrying things that must NOT reach the log: a signed URL and a fake
    # credential, both checked against the events below.
    leaky = job(
        jid,
        source_url="https://cdn.example.com/x.mp4?token=leaked-token",
        # Not a credential: a canary value used below to assert the projection
        # keeps secrets out of the event log (bandit flags the argument name).
        aws_secret_access_key="leaked-secret",  # noqa: S106  # nosec B106
    )

    for event_type in (messages.JOB_QUEUED, messages.JOB_STARTED, messages.JOB_COMPLETED):
        payload = {"progress": 100, "status": "done"} if event_type == messages.JOB_COMPLETED else {}
        sent = await eventbus.emit_event(event_type, job=leaky, payload=payload, source="integration-check")
        check(f"{event_type} was accepted by Kafka", sent is True, repr(sent))

    # Progress is throttled and opt-in; the first one for a job is always allowed.
    progress_sent = await eventbus.emit_event(
        messages.JOB_PROGRESS, job=leaky, payload={"progress": 55, "message": "encoding"}, source="integration-check"
    )
    check("a progress event was accepted (KAFKA_EMIT_PROGRESS_EVENTS=true)", progress_sent is True, repr(progress_sent))

    from utils.eventbus.kafka import get_bus

    events = await get_bus().read_events(job_id=jid, limit=20, timeout_ms=8000)
    types = [event.get("type") for event in events]
    check("all four events were read back from the topic", len(events) == 4, str(types))
    check(
        "the events are in lifecycle order",
        types == [messages.JOB_QUEUED, messages.JOB_STARTED, messages.JOB_COMPLETED, messages.JOB_PROGRESS],
        str(types),
    )
    blob = str([event.get("payload") for event in events])
    check(
        "the log carries the job summary",
        all(event.get("payload", {}).get("job_id") == jid for event in events),
        blob[:160],
    )
    check(
        "no credential or token leaked into the log",
        "leaked-token" not in blob and "leaked-secret" not in blob,
        blob[:160],
    )
    check("file paths are not part of the event payload", "/data/storage" not in blob, blob[:160])


async def scenario_f_graceful_stop(queue) -> None:
    print("\n--- F. stopping the consumer loses nothing ---")
    await queue.purge_queues()
    settings = get_settings()
    first, second = (f"itest-f-{uuid.uuid4().hex[:8]}" for _ in range(2))
    handled: list[str] = []
    stop = asyncio.Event()

    async def handler(inbound: dict) -> None:
        handled.append(inbound.get("job_id", ""))
        stop.set()  # stop as soon as one job is processed

    check("the first test job was published", await queue.publish_job(job(first)) is True)
    check("the second test job was published", await queue.publish_job(job(second)) is True)
    started = time.monotonic()
    await asyncio.wait_for(queue.consume(handler, stop_event=stop), timeout=15)
    elapsed = time.monotonic() - started
    check("the consumer returned promptly after the stop event", elapsed < 5.0, f"{elapsed:.2f}s")
    check("exactly one job was processed", len(handled) == 1, str(handled))

    # Cancelling the consumer stops new deliveries, but a message already handed
    # over stays unacked on the *channel* until the channel closes - so it is not
    # "ready" yet. Both halves of that are worth pinning down, because the worker
    # relies on the second half: close_eventbus() runs on shutdown and is what
    # returns the in-flight job to the queue.
    check(
        "the unprocessed delivery is unacked, not ready (prefetch holds it)",
        await queue.queue_depth(settings.jobs_queue) == 0,
        f"ready={await queue.queue_depth(settings.jobs_queue)}",
    )

    await queue.close()  # what the worker's shutdown does
    survivors = []
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        await queue.connect()
        message = await queue._jobs_queue.get(no_ack=False, fail=False)  # noqa: SLF001 - deliberate probe
        if message is not None:
            import json

            survivors.append(json.loads(message.body.decode("utf-8")).get("job_id"))
            await message.nack(requeue=True)
            break
        await asyncio.sleep(0.3)
    check("closing the connection requeued it, so nothing was lost", survivors == [second], str(survivors))


async def scenario_g_worker_path(queue) -> None:
    """Drive the worker's real consumer task, with only the job handler stubbed.

    This is the wiring the bot actually runs: publish_job (producer side) →
    workers.ffmpeg_worker._rabbitmq_consumer_task → _run_queued_job → handler →
    ack, plus the started event the worker emits on the way in. Only handle_job
    and the Mongo write are replaced, so no Redis/ffmpeg/Mongo is needed.
    """
    print("\n--- G. the worker's own consumer path ---")
    await queue.purge_queues()
    settings = get_settings()
    jid = f"itest-g-{uuid.uuid4().hex[:8]}"
    processed: list[dict] = []

    import workers.ffmpeg_worker as worker

    async def fake_handle_job(inbound: dict) -> None:
        processed.append(inbound)

    async def noop_save(*args, **kwargs):
        return None

    original_handler = worker.handle_job
    original_save = worker.job_store.save_job
    worker.handle_job = fake_handle_job
    worker.job_store.save_job = noop_save
    stop = asyncio.Event()

    async def stop_once_processed():
        while not processed:
            await asyncio.sleep(0.05)
        stop.set()

    stopper = asyncio.create_task(stop_once_processed())
    try:
        await queue.publish_job(job(jid))
        await asyncio.wait_for(worker._rabbitmq_consumer_task(stop), timeout=20)  # noqa: SLF001
    finally:
        worker.handle_job = original_handler
        worker.job_store.save_job = original_save
        stopper.cancel()
        with suppress(Exception):
            await stopper

    check(
        "the worker consumed the job through its own consumer task",
        [entry.get("job_id") for entry in processed] == [jid],
        str(processed)[:140],
    )
    check(
        "the worker tagged the job as broker-sourced (lock requeue goes back to the broker)",
        bool(processed) and eventbus.is_rabbitmq_job(processed[0]),
        str(processed[0].get("_eventbus_origin")) if processed else "no job",
    )
    check("the worker acked the message", await queue.queue_depth(settings.jobs_queue) == 0)

    from utils.eventbus.kafka import get_bus

    events = await get_bus().read_events(job_id=jid, limit=10, timeout_ms=6000)
    types = [event.get("type") for event in events]
    check("the worker's started event reached Kafka", messages.JOB_STARTED in types, str(types))


async def main(force: bool) -> int:
    reset_settings_cache()
    settings = get_settings()
    print("event bus integration check")
    print(f"  settings: {settings.describe()}")

    host = settings.rabbitmq_url.split("@")[-1].split(":")[0].lower()
    if host not in ("localhost", "127.0.0.1", "::1") and not force:
        print(f"\nREFUSING TO RUN: RABBITMQ_URL host is {host!r}, not localhost. Use --force to override.")
        return 2

    # Scenario A asserts that a *specific* job is routed to the broker, which is
    # only true at a full rollout. With a partial one the job legitimately goes to
    # Redis, so say that instead of failing three checks with a confusing reason.
    if settings.queue_rollout_percent < 100 and not force:
        print(
            f"\nREFUSING TO RUN: EVENTBUS_QUEUE_ROLLOUT_PERCENT is {settings.queue_rollout_percent}, "
            "but these checks require 100 (a partial rollout would route some test jobs to Redis by design)."
            "\nUnset it or set 100, then run again."
        )
        return 2

    queue = eventbus.get_queue()
    try:
        await queue.connect()
        print(
            f"\ntopology: {settings.exchange} --{settings.jobs_routing_key}--> {settings.jobs_queue}"
            f"  (retry: {settings.retry_queue}, dead: {settings.dead_queue})"
        )
        purged = await queue.purge_queues()
        print(f"queues purged before the run: {purged}")

        scenarios = (
            scenario_a_publish_confirmed,
            scenario_b_ack_after_handler,
            scenario_c_retry,
            scenario_d_dead_letter,
            scenario_e_kafka,
            scenario_f_graceful_stop,
            scenario_g_worker_path,
        )
        for scenario in scenarios:
            # One scenario blowing up must not hide the rest of the report: a
            # timeout or a broker refusal in A used to abort the whole run before
            # anything was summarised.
            try:
                await scenario(queue)
            except Exception as exc:  # noqa: BLE001 - the point is to keep going and report it
                check(f"{scenario.__name__} raised before completing", False, f"{type(exc).__name__}: {exc}")

        print("\nqueue depths at the end:", await queue.stats())
    finally:
        with suppress(Exception):
            await eventbus.close_eventbus()

    failures = [name for name, ok, _ in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failures)}/{len(RESULTS)} checks passed")
    if failures:
        print("failed:")
        for name in failures:
            print(f"  - {name}")
    print("RESULT:", "OK" if not failures else "FAILED")
    return 0 if not failures else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify the event bus against real brokers.")
    parser.add_argument(
        "--force", action="store_true", help="allow running against a non-localhost broker (destructive)"
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(args.force)))
