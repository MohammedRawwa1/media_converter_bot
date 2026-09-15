#!/usr/bin/env python3
"""Preview or remove stale Redis state for the media conversion pipeline.

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
import hashlib
import json
import os
import sys
from collections import Counter

try:
    import redis
except ImportError:
    print("ERROR: redis package is required")
    raise SystemExit(2)

ACTIVE_STATUSES = frozenset({
    "queued", "processing", "waiting", "started", "uploading", "sending",
})
TERMINAL_STATUSES = frozenset({
    "done", "completed", "error", "failed", "cancelled", "canceled",
})
JOB_PREFIX = "ffmpeg:job:"
BATCH_PREFIX = "ffmpeg:batch:"
DEDUPE_PREFIX = "ffmpeg:pipeline_dedup:"
LOCK_PREFIX = "ffmpeg:lock:"
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
        if batch_id:
            batch_ids.add(batch_id)

    for batch_id in sorted(batch_ids):
        if args.batch and batch_id != args.batch:
            continue
        members = {text(x) for x in r.smembers(f"{BATCH_PREFIX}{batch_id}:jobs")}
        if members & active:
            continue
        for suffix in (":jobs", ":total", ":done", ":msg", ":cancelled"):
            key = f"{BATCH_PREFIX}{batch_id}{suffix}"
            if r.exists(key):
                report_or_delete(r, key, apply=args.apply, reason="batch", count=counts)

    print("Summary:")
    for name in ("queue", "delayed", "lock", "dedup", "batch"):
        print(f"  {name}: {counts[name]}")
    if not args.apply:
        print("No data was deleted. Re-run with --apply to remove these stale entries.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
