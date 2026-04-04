#!/usr/bin/env python3
"""One-shot requeue for jobs missing input

Scans Redis `ffmpeg:job:*` hashes for jobs without `input`/`input_key` and
attempts to re-populate them using forward metadata (`forwards/<fid>.json`).
If a remote key is present in the forward metadata this script calls
`enqueue_job()` to atomically HSET + LPUSH the job. Otherwise it publishes a
fetch request to `ffmpeg:fetch` so the fetcher can try to obtain the input.

Usage:
  python scripts/requeue_missing_jobs_once.py

Environment:
  REDIS_URL - required (or set in project env)

This is best-effort and safe to run multiple times: it uses a per-job
lock `ffmpeg:requeue_lock:<job_id>` to avoid duplicates.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

# Ensure project root is importable when running this script directly
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("requeue_missing_jobs_once")

REDIS_URL = os.environ.get("REDIS_URL")
REQUEUE_LOCK_TTL = int(os.environ.get("REQUEUE_LOCK_TTL", "300"))


async def _run_once():
    if not aioredis:
        logger.error("redis.asyncio not available; install redis>=4.6.0")
        return 2
    if not REDIS_URL:
        logger.error("REDIS_URL must be set")
        return 2

    client = aioredis.from_url(REDIS_URL, decode_responses=True)
    count = 0

    async def _process_key(key) -> int:
        """Process a single Redis job key. Returns 1 if an action was taken, else 0."""
        try:
            if isinstance(key, (bytes, bytearray)):
                key = key.decode("utf-8")
            job_id = key.rsplit(":", 1)[-1]
            stored = await client.hgetall(key)
            if not stored:
                return 0

            def _sval(k):
                v = stored.get(k)
                if isinstance(v, (bytes, bytearray)):
                    try:
                        return v.decode()
                    except Exception:
                        return v
                return v

            inp = _sval("input") or ""
            ik = _sval("input_key") or ""
            source_url = _sval("source_url") or ""
            if inp or ik or source_url:
                return 0

            fh = _sval("forward_hash") or _sval("fh")
            if not fh:
                if isinstance(inp, str) and "forwards/" in inp and inp.endswith('.json'):
                    try:
                        fh = os.path.basename(inp).replace('.json', '')
                    except Exception:
                        fh = None

            if not fh:
                fwd = _sval("forward")
                if fwd:
                    try:
                        if isinstance(fwd, (bytes, bytearray)):
                            fwd = fwd.decode("utf-8")
                        fj = json.loads(fwd) if isinstance(fwd, str) else fwd
                        if isinstance(fj, dict):
                            fh = str(fj.get("fid") or fj.get("forward_hash") or fj.get("file_id") or "")
                            if not fh:
                                fh = None
                    except Exception:
                        fh = None

            if not fh:
                return 0

            lock_key = f"ffmpeg:requeue_lock:{job_id}"
            try:
                set_ok = await client.set(lock_key, "1", nx=True, ex=REQUEUE_LOCK_TTL)
            except Exception:
                set_ok = True
            if not set_ok:
                return 0

            meta = None
            try:
                from utils.forward_store import load_forward_metadata

                meta = load_forward_metadata(fh)
            except Exception:
                meta = None

            remote_key = None
            if meta and isinstance(meta, dict):
                remote_key = meta.get("remote_key") or meta.get("input_key") or meta.get("s3_key") or meta.get("key")

            if remote_key:
                try:
                    from utils.job_queue import enqueue_job
                except Exception:
                    enqueue_job = None

                if enqueue_job:
                    job = {"job_id": job_id, "input_key": remote_key}
                    out = _sval("output")
                    if out:
                        job["output_path"] = out
                    orig = _sval("original_filename")
                    if orig:
                        job["original_filename"] = orig
                    try:
                        await enqueue_job(job)
                        logger.info("Re-enqueued job %s with input_key %s", job_id, remote_key)
                        return 1
                    except Exception:
                        logger.exception("enqueue_job failed for %s; falling back to LPUSH", job_id)

                try:
                    await client.lpush("ffmpeg:jobs", json.dumps({"job_id": job_id, "input_key": remote_key}))
                    return 1
                except Exception:
                    logger.exception("Fallback LPUSH failed for %s", job_id)
                    return 0
            else:
                try:
                    await client.publish("ffmpeg:fetch", json.dumps({"forward_hash": fh}))
                    logger.info("Published fetch request for forward %s", fh)
                    return 1
                except Exception:
                    logger.exception("Failed to publish fetch request for %s", fh)
                    return 0

        except Exception:
            logger.exception("Error processing key %s", key)
            return 0

    try:
        async for key in client.scan_iter(match="ffmpeg:job:*", count=200):
            inc = await _process_key(key)
            try:
                count += int(inc)
            except Exception:
                pass
    except Exception:
        logger.exception("Error scanning Redis keys")
    finally:
        try:
            aclose = getattr(client, "aclose", None)
            if aclose is not None:
                await aclose()
            else:
                await client.close()
        except Exception:
            pass

    logger.info("Done scanning; actions taken: %s", count)
    return 0


if __name__ == "__main__":
    res = None
    try:
        res = asyncio.run(_run_once())
    except KeyboardInterrupt:
        logger.info("Interrupted")
        res = 0
    sys.exit(res or 0)
