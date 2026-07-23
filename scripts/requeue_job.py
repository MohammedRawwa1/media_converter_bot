#!/usr/bin/env python3
"""Safely requeue an ffmpeg job after verifying input exists locally or in storage.

Usage:
  python3 scripts/requeue_job.py --job JOB_ID [--input-key KEY | --input-path PATH] [--dry-run] [--force]

This helper will check whether a local file exists or whether the configured
storage backend contains the provided remote key (via `exists()`). When checks
pass (or when `--force` is used), it calls `enqueue_job()` which atomically
HSETs the job metadata then LPUSHes the job JSON onto the queue.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import pathlib
import sys
from pathlib import Path

# Ensure repository root is importable when running as a script
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    from utils.job_queue import enqueue_job, get_redis
    from utils.storage import get_storage_backend_sync
except Exception:
    print("Failed to import project helpers (utils.*). Ensure you're running this script from the repository root and have installed dependencies.")
    import traceback

    traceback.print_exc()
    sys.exit(2)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("requeue_job")


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", "-j", required=True, help="Job ID to requeue")
    parser.add_argument("--input-key", help="Storage key (remote) to set as input")
    parser.add_argument("--input-path", help="Local file path to set as input")
    parser.add_argument("--force", action="store_true", help="Force requeue without verifying remote existence")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without performing them")
    args = parser.parse_args(argv)

    job_id = args.job

    # Read existing job hash (best-effort) to discover candidates
    try:
        r = await get_redis()
    except Exception as e:
        logger.error("Failed to connect to Redis: %s", e)
        return 2

    stored = {}
    try:
        stored = await r.hgetall(f"ffmpeg:job:{job_id}") or {}
    except Exception:
        stored = {}
    finally:
        with contextlib.suppress(Exception):
            await r.close()

    cand_input_path = args.input_path or stored.get("input") or None
    cand_input_key = args.input_key or stored.get("input_key") or None

    use_local = False
    use_remote = False
    final_input_path = None
    final_input_key = None

    # Prefer explicit CLI args
    if args.input_path:
        if os.path.exists(args.input_path):
            use_local = True
            final_input_path = args.input_path
        else:
            logger.error("Local input path does not exist: %s", args.input_path)
            return 3

    elif args.input_key:
        use_remote = True
        final_input_key = args.input_key

    else:
        # Fall back to stored values
        if cand_input_path and os.path.exists(cand_input_path):
            use_local = True
            final_input_path = cand_input_path
        elif cand_input_key:
            use_remote = True
            final_input_key = cand_input_key
        elif cand_input_path:
            # stored input present but not on disk; treat as remote candidate
            use_remote = True
            final_input_key = cand_input_path
        else:
            logger.error("No input info available in job hash and no CLI override provided")
            return 4

    # If remote, verify existence via storage backend (unless forced)
    if use_remote and not args.force:
        try:
            backend = get_storage_backend_sync()
        except Exception:
            # try async factory as fallback
            backend = None
            try:
                from utils.storage import get_storage_backend

                backend = await get_storage_backend()
            except Exception:
                backend = None

        if backend is None:
            logger.warning("No storage backend available to verify remote key; use --force to bypass")
            print("No storage backend available to verify remote key; use --force to bypass")
            return 5

        try:
            exists = await backend.exists(final_input_key)
        except Exception as e:
            logger.exception("Storage existence check failed: %s", e)
            exists = False

        if not exists and not args.force:
            logger.error("Remote key not found: %s", final_input_key)
            print(f"Remote key not found: {final_input_key}")
            return 6

    # Dry-run - summarize and exit
    if args.dry_run:
        print("Dry run: would requeue job", job_id)
        if use_local:
            print(" - using local input path:", final_input_path)
        elif use_remote:
            print(" - using remote input_key:", final_input_key)
        return 0

    # Construct a minimal job payload and call enqueue_job which performs HSET then LPUSH
    job = {"job_id": job_id}
    if use_local:
        # normalize to POSIX-style for storage in the job hash
        job["input_path"] = pathlib.PurePath(final_input_path).as_posix()
    else:
        job["input_key"] = final_input_key

    try:
        await enqueue_job(job)
        print("Requeued job", job_id)
        return 0
    except Exception as e:
        logger.exception("Failed to enqueue job: %s", e)
        print("Failed to enqueue job:", e)
        return 7


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
