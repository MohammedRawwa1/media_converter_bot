#!/usr/bin/env python3
"""Preview or remove stale Redis state for the media conversion pipeline.

The bot now does this itself: ``/cancelall`` cancels every job and then takes
down every batch left without a live member, deleting the batches' progress
messages from the chat as well (see ``utils.queue_admin`` and
``utils.batch_pipeline.purge_stale_batches``). Run this script only to *preview*
what is stale, or to clean up offline when the bot is not running.

Dry-run is the default. Use --apply to delete only state that is no longer
owned by an active job. Active jobs and their input/ffmpeg locks are preserved.

Usage:
  python scripts/cleanup_stale_redis.py
  python scripts/cleanup_stale_redis.py --apply
  python scripts/cleanup_stale_redis.py --apply --batch BATCH_ID

The REDIS_URL environment variable must be set.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter

try:
    import redis
except ImportError:
    print("ERROR: redis package is required")
    raise SystemExit(2) from None

ACTIVE_STATUSES = frozenset({
    "queued", "processing", "waiting", "started", "uploading", "sending",
})
TERMINAL_STATUSES = frozenset({
    "done", "completed", "error", "failed", "cancelled", "canceled",
})
JOB_PREFIX = "ffmpeg:job:"
BATCH_PREFIX = "ffmpeg:batch:"
# Keys under the batch prefix that are not a batch id themselves.
BATCH_NON_ID_SUFFIXES = frozenset({"active", "resume"})
BATCH_ACTIVE_SET = f"{BATCH_PREFIX}active"
BATCH_RESUME_PREFIX = f"{BATCH_PREFIX}resume:"
# Everything one batch owns, apart from its lock. Must stay in step with
# ``utils.batch_pipeline.batch_state_keys``.
BATCH_STATE_SUFFIXES = (
    ":jobs",
    ":total",
    ":done",
    ":done_jobs",
    ":msg",
    ":finished",
    ":started",
    ":cancelled",
)
DEDUPE_PREFIX = "ffmpeg:pipeline_dedup:"
LOCK_PREFIX = "ffmpeg:lock:"
# The global conversion slots. One of these left behind by a worker that is gone
# is what makes the queue defer every new job ("slot busy") until its TTL runs
# out, so it belongs in here as much as the input locks do.
SLOT_PREFIX = "ffmpeg:slot:"
JOB_LIST = "ffmpeg:jobs"
DELAYED_SET = "ffmpeg:delayed"


def text(value) -> str:
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return str(value)


def job_hash(r, job_id: str) -> dict:
    return {text(k): text(v) for k, v in r.hgetall(f"{JOB_PREFIX}{job_id}").items()}


def scan(r, pattern: str):
    yield from r.scan_iter(match=pattern, count=500)


def active_job_ids(r, selected_batch: str | None = None) -> tuple[set[str], dict[str, dict]]:
    active: set[str] = set()
    hashes: dict[str, dict] = {}
    for raw_key in scan(r, f"{JOB_PREFIX}*"):
        key = text(raw_key)
        job_id = key[len(JOB_PREFIX):]
        stored = job_hash(r, job_id)
        hashes[job_id] = stored
        if selected_batch and stored.get("batch_id") != selected_batch:
            continue
        if stored.get("status") in ACTIVE_STATUSES:
            active.add(job_id)
    return active, hashes


def queue_job_id(raw) -> str | None:
    try:
        payload = json.loads(text(raw))
        return str(payload.get("job_id")) if payload.get("job_id") else None
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def is_stale_job(job_id: str | None, hashes: dict[str, dict], selected_batch: str | None) -> bool:
    if not job_id:
        return False
    stored = hashes.get(job_id)
    if not stored:
        return True
    if selected_batch and stored.get("batch_id") != selected_batch:
        return False
    return stored.get("status") in TERMINAL_STATUSES


def _drop_member(r, key: str, member: str, *, apply: bool) -> bool:
    """Whether ``key`` holds ``member`` - removing it only when applying."""
    try:
        if not apply:
            return bool(r.sismember(key, member))
        return bool(r.srem(key, member))
    except Exception:
        return False


def report_or_delete(r, key: str, *, apply: bool, reason: str, count: Counter) -> None:
    count[reason] += 1
    print(f"  {'DELETE' if apply else 'STALE'} {key} ({reason})")
    if apply:
        r.delete(key)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Perform deletions; default is dry-run")
    parser.add_argument("--batch", help="Limit cleanup to one batch ID")
    args = parser.parse_args()

    url = os.environ.get("REDIS_URL")
    if not url:
        print("ERROR: REDIS_URL environment variable is not set")
        return 1

    r = redis.from_url(url, decode_responses=False)
    r.ping()
    counts = Counter()
    active, hashes = active_job_ids(r, args.batch)
    print(f"Mode: {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"Active jobs preserved: {len(active)}")

    # Remove only queue entries whose job hash is missing or terminal. Active
    # queued jobs are preserved so this never drains a live pipeline.
    for raw in list(r.lrange(JOB_LIST, 0, -1)):
        job_id = queue_job_id(raw)
        if is_stale_job(job_id, hashes, args.batch):
            if args.apply:
                r.lrem(JOB_LIST, 1, raw)
            counts["queue"] += 1
            print(f"  {'DELETE' if args.apply else 'STALE'} queue member {job_id}")

    for raw in list(r.zrange(DELAYED_SET, 0, -1)):
        job_id = queue_job_id(raw)
        if is_stale_job(job_id, hashes, args.batch):
            if args.apply:
                r.zrem(DELAYED_SET, raw)
            counts["delayed"] += 1
            print(f"  {'DELETE' if args.apply else 'STALE'} delayed member {job_id}")

    # Preserve locks owned by active jobs. A lock value is a job id; unknown or
    # terminal owners are stale and safe to remove.
    for raw_key in scan(r, f"{LOCK_PREFIX}*"):
        key = text(raw_key)
        owner = text(r.get(raw_key) or "")
        if owner not in active:
            report_or_delete(r, key, apply=args.apply, reason="lock", count=counts)

    # Conversion slots hold the same kind of value as an input lock, so the same
    # rule applies: an owner that is not an active job is a ghost claim.
    for raw_key in scan(r, f"{SLOT_PREFIX}*"):
        key = text(raw_key)
        owner = text(r.get(raw_key) or "")
        if owner not in active:
            report_or_delete(r, key, apply=args.apply, reason="slot", count=counts)

    # Dedup keys are safe to remove when their referenced job is absent or
    # terminal. The special pending value is always preserved.
    for raw_key in scan(r, f"{DEDUPE_PREFIX}*"):
        key = text(raw_key)
        owner = text(r.get(raw_key) or "")
        if owner != "pending" and owner not in active:
            report_or_delete(r, key, apply=args.apply, reason="dedup", count=counts)

    # Batch metadata is removed only when its job set has no active member. The
    # job set itself is the authoritative membership index for /cancelbatch.
    batch_ids: set[str] = set()
    for raw_key in scan(r, f"{BATCH_PREFIX}*"):
        key = text(raw_key)
        suffix = key[len(BATCH_PREFIX):]
        batch_id = suffix.split(":", 1)[0]
        if batch_id and batch_id not in BATCH_NON_ID_SUFFIXES:
            batch_ids.add(batch_id)
    # A batch whose keys expired but which is still in the aggregate view.
    for raw in r.smembers(BATCH_ACTIVE_SET):
        if text(raw):
            batch_ids.add(text(raw))

    for batch_id in sorted(batch_ids):
        if args.batch and batch_id != args.batch:
            continue
        members = {text(x) for x in r.smembers(f"{BATCH_PREFIX}{batch_id}:jobs")}
        if members & active:
            continue
        # The batch's own lock is the bare ``ffmpeg:batch:<id>`` key, which is why
        # it is not one of BATCH_STATE_SUFFIXES. It was held by one of the members,
        # and no member is active, so it is stale like everything else here.
        lock_key = f"{BATCH_PREFIX}{batch_id}"
        if r.exists(lock_key):
            report_or_delete(r, lock_key, apply=args.apply, reason="batch lock", count=counts)
        for suffix in BATCH_STATE_SUFFIXES:
            key = f"{BATCH_PREFIX}{batch_id}{suffix}"
            if r.exists(key):
                report_or_delete(r, key, apply=args.apply, reason="batch", count=counts)
        # Membership outside the batch's own keys: the aggregate view, and every
        # user's resume record. Left behind, both keep pointing at a batch whose
        # state is gone. (/cancelall keeps the ``:cancelled`` tombstone instead of
        # deleting it, because a worker may still be finishing a member; this
        # script only runs where nothing is.)
        # Checked with SISMEMBER and only removed under --apply: a dry run used to
        # call SREM outright, so previewing quietly took the batch out of the
        # aggregate view and out of every user's resume record while printing
        # "STALE" and then promising that no data was deleted.
        if _drop_member(r, BATCH_ACTIVE_SET, batch_id, apply=args.apply):
            counts["batch"] += 1
            print(f"  {'DELETE' if args.apply else 'STALE'} {BATCH_ACTIVE_SET} member {batch_id}")
        for resume_key in scan(r, f"{BATCH_RESUME_PREFIX}*"):
            if _drop_member(r, resume_key, batch_id, apply=args.apply):
                counts["batch"] += 1
                print(f"  {'DELETE' if args.apply else 'STALE'} resume member {batch_id} of {text(resume_key)}")

    print("Summary:")
    for name in ("queue", "delayed", "lock", "slot", "dedup", "batch", "batch lock"):
        print(f"  {name}: {counts[name]}")
    if not args.apply:
        print("No data was deleted. Re-run with --apply to remove these stale entries.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
