"""Smoke check for the optional event-bus layer.

Prints the resolved configuration, then - only for the parts that are enabled -
connects to the broker, declares/looks at the topology and reports what it found.
Nothing here mutates jobs. Two writes are offered, neither of which is a job:
the Kafka check always publishes one probe event (``eventbus.probe``, no job id)
so a missing topic or a write-ACL denial is reported instead of a clean-looking
start, and ``--publish-test`` additionally publishes a throwaway message with a
``check.`` job id so RabbitMQ can be confirmed end to end.

    python scripts/check_eventbus.py                     # config + connectivity
    python scripts/check_eventbus.py --publish-test      # also send one test job
    python scripts/check_eventbus.py --replay <job_id>   # read a job's events back

Exit codes: 0 when every *enabled* component is reachable - and, for Kafka,
writable - 1 otherwise. A disabled component is not a failure: it is reported as
disabled.

Configuration is read from the process environment, falling back to ``.env``
(loaded here exactly the way the application loads it). An exported variable
always wins over the file, so ``KAFKA_BOOTSTRAP_SERVERS=... python ...`` still
overrides ``.env``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Imported for its side effect: ``config`` loads .env (root/.env, cwd/.env,
# .env.local) without overriding anything already exported - the same loader the
# application uses. Without it this check only saw the variables that happened to
# be exported, and reported a configured layer as "disabled".
import config  # noqa: E402,F401
from utils import eventbus  # noqa: E402
from utils.eventbus.config import kafka_available, reset_settings_cache  # noqa: E402


def _print_config(info: dict) -> None:
    print("--- resolved configuration ---")
    for key in sorted(info):
        print(f"  {key}: {info[key]}")
    if info.get("degraded_reason"):
        print(f"\n  ! RabbitMQ was requested but is unusable: {info['degraded_reason']}")
        print("    Jobs keep using the Redis list. Check RABBITMQ_URL / pip install aio-pika.")


async def _check_rabbitmq(publish_test: bool) -> bool:
    if not eventbus.get_settings().queue_enabled:
        print("\n--- RabbitMQ ---\n  disabled (EVENTBUS_QUEUE_BACKEND/RABBITMQ_URL/rollout)\n")
        return True
    from utils.eventbus.rabbit import get_queue

    queue = get_queue()
    try:
        await queue.connect()
    except Exception as exc:
        print(f"\n--- RabbitMQ ---\n  UNREACHABLE: {exc}\n")
        return False
    stats = await queue.stats()
    print("\n--- RabbitMQ ---")
    print(f"  exchange: {queue.settings.exchange}")
    for name, count in stats.items():
        print(f"  queue {name}: {count} message(s) ready")
    if publish_test:
        job_id = f"check.{uuid.uuid4().hex[:12]}"
        # A throwaway path in the OS temp dir: the worker acking this message will
        # fail on the missing input, which is itself a useful retry/dead-letter
        # demonstration rather than a real job.
        scratch = os.path.join(tempfile.gettempdir(), f"eventbus-check-{job_id}.txt")
        ok = await queue.publish_job(
            {
                "job_id": job_id,
                "request_id": job_id,
                "type": "check",
                "input_path": scratch,
                "output_path": f"{scratch}.out",
            }
        )
        print(f"  published test job {job_id}: {'confirmed by broker' if ok else 'REJECTED'}")
        if not ok:
            return False
    await queue.close()
    return True


async def _check_kafka(replay_job_id: str | None) -> bool:
    settings = eventbus.get_settings()
    if not settings.events_enabled:
        print("\n--- Kafka ---\n  disabled (EVENTBUS_EVENTS_BACKEND/KAFKA_BOOTSTRAP_SERVERS)\n")
        return True
    if not kafka_available():
        print("\n--- Kafka ---\n  aiokafka is not installed: pip install -r requirements.txt\n")
        return False
    from utils.eventbus.kafka import get_bus

    bus = get_bus()
    print("\n--- Kafka ---")
    print(f"  bootstrap: {settings.kafka_bootstrap_servers}")
    print(f"  topic: {settings.kafka_topic} (partitions/keyed by job id)")
    try:
        started = await bus.start()
    except Exception as exc:
        print(f"  UNREACHABLE: {exc}\n")
        return False
    if not started:
        print("  producer could not be started")
        print("  ! check KAFKA_BOOTSTRAP_SERVERS, KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_*,")
        print("    and the CA (KAFKA_SSL_CAFILE or AIVEN_CA_CERT)\n")
        return False
    print("  producer: started (acks=all, idempotent)")

    # A started producer only proves the bootstrap/TLS/SASL handshake went
    # through. A topic that does not exist or a write-ACL denial shows up only
    # when a message is actually sent and acked, so confirm the write here -
    # otherwise this check reports a clean start while every event is dropped.
    ok = True
    probe = eventbus.new_event(eventbus.EVENTS_PROBE, source="check")
    write_ok, detail = await bus.verify_publish(probe)
    print(f"  write probe: {detail}")
    if not write_ok:
        ok = False
        # Name the exact cause (TLS / auth / topic missing / write denied) rather
        # than leaving it to be inferred from the error string.  The probe's own
        # exception is classified, so this does not contact the broker again.
        from utils.eventbus.kafka import preflight

        diagnosis = await preflight(settings, failure=bus.last_failure)
        print(f"  ! write rejected - {diagnosis['cause']}: {diagnosis['detail']}")
        if diagnosis["remedy"]:
            print(f"    fix: {diagnosis['remedy']}")
        for problem in diagnosis["problems"]:
            print(f"    config: {problem}")

    if replay_job_id:
        events = await bus.read_events(job_id=replay_job_id, limit=50)
        print(f"  replay for job {replay_job_id}: {len(events)} event(s)")
        for event in events:
            print(f"    {event.get('type')} @ {event.get('ts')} payload={json.dumps(event.get('payload'))[:120]}")
    await bus.close()
    return ok


async def _main(args) -> int:
    reset_settings_cache()
    info = eventbus.describe()
    _print_config(info)
    rabbit_ok = await _check_rabbitmq(args.publish_test)
    kafka_ok = await _check_kafka(args.replay)

    print("\n--- routing sanity ---")
    sample = [f"job-{i}" for i in range(200)]
    settings = eventbus.get_settings()
    if settings.queue_enabled and settings.queue_rollout_percent < 100:
        routed = sum(1 for job_id in sample if eventbus.queue_backend_for_job({"job_id": job_id}) == "rabbitmq")
        print(f"  rollout {settings.queue_rollout_percent}%: {routed}/200 sample jobs would use RabbitMQ")
        first = eventbus.queue_backend_for_job({"job_id": sample[0]})
        again = eventbus.queue_backend_for_job({"job_id": sample[0]})
        print(f"  decision is stable across calls: {first == again} ({first})")
    elif settings.queue_enabled:
        print("  rollout 100%: every job uses RabbitMQ")
    else:
        print("  rollout not active: every job uses the Redis list")

    await eventbus.close_eventbus()
    ok = rabbit_ok and kafka_ok
    print("\nRESULT:", "OK" if ok else "FAILED (an enabled component was unreachable)")
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Check the media bot event bus (RabbitMQ + Kafka).")
    parser.add_argument("--publish-test", action="store_true", help="publish one throwaway job to RabbitMQ")
    parser.add_argument("--replay", metavar="JOB_ID", default=None, help="read a job's events back from Kafka")
    args = parser.parse_args()
    return asyncio.run(_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
