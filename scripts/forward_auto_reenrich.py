#!/usr/bin/env python3
"""Forward auto re-enricher

Listens for forward notifications (FORWARD_PUBLISH_CHANNEL) and attempts to
re-populate Redis job hashes when a remote input key becomes available.

Behavior:
 - When a forward notification with `fid` and optional `remote_key` arrives,
   the service scans `ffmpeg:job:*` for job hashes referencing that forward id
   (via `forward_hash`, `fh`, or `input` pointing to `forwards/<fid>.json`).
 - For each matching job it sets a short requeue lock to avoid duplicates,
   then calls `enqueue_job({'job_id': ..., 'input_key': <remote_key>})` to
   atomically update the job hash and push a job JSON onto the queue.
 - If no remote key is available, the service will publish a fetch request to
   `ffmpeg:fetch` so the fetcher service can attempt to obtain the input.

Run:
  python scripts/forward_auto_reenrich.py

Environment:
  REDIS_URL - required
  FORWARD_PUBLISH_CHANNEL - default: ffmpeg:forwards
  FETCH_CHANNEL - default: ffmpeg:fetch
  REQUEUE_LOCK_TTL - seconds to avoid duplicate requeues (default 300)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

# allow importing project modules when run from scripts dir
sys.path.insert(0, os.getcwd())

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("forward_auto_reenrich")

FORWARD_CHANNEL = os.environ.get("FORWARD_PUBLISH_CHANNEL", "ffmpeg:forwards")
FETCH_CHANNEL = os.environ.get("FETCH_CHANNEL", "ffmpeg:fetch")
REDIS_URL = os.environ.get("REDIS_URL")
REQUEUE_LOCK_TTL = int(os.environ.get("REQUEUE_LOCK_TTL", "300"))


async def _process_forward(fid, payload: dict, client) -> None:
    # Attempt to determine a remote key from payload or forward metadata
    remote_key = None
    try:
        # payload may include remote_key when saved to S3
        remote_key = payload.get("remote_key") or payload.get("input_key")
    except Exception:
        remote_key = None

    # Load forward metadata (sync function provided by utils.forward_store)
    try:
        from utils.forward_store import load_forward_metadata

        meta = load_forward_metadata(fid)
        if not remote_key and meta:
            # common fields that may indicate a remote key
            remote_key = meta.get("remote_key") or meta.get("input_key") or meta.get("s3_key") or meta.get("key")
    except Exception:
        meta = None

    # If no remote key available, ask fetcher to try (best-effort)
    if not remote_key:
        try:
            # publish lightweight fetch request
            await client.publish(FETCH_CHANNEL, json.dumps({"forward_hash": fid}))
            logger.info("Published fetch request for forward %s to %s", fid, FETCH_CHANNEL)
        except Exception:
            logger.exception("Failed to publish fetch request for %s", fid)
        return

    logger.info("Found remote_key=%s for forward %s; scanning jobs...", remote_key, fid)

    # Scan job hashes and re-enqueue matching jobs
    try:
        async for key in client.scan_iter(match="ffmpeg:job:*", count=200):
            try:
                if isinstance(key, (bytes, bytearray)):  # decode if needed
                    key = key.decode("utf-8")
                job_id = key.rsplit(":", 1)[-1]
                stored = await client.hgetall(key)
                if not stored:
                    continue
                # normalize stored values to strings
                def _sval(k):
                    v = stored.get(k)
                    if isinstance(v, (bytes, bytearray)):
                        try:
                            return v.decode()
                        except Exception:
                            return v
                    return v

                matched = False
                if _sval("forward_hash") and str(_sval("forward_hash")) == str(fid):
                    matched = True
                if not matched and _sval("fh") and str(_sval("fh")) == str(fid):
                    matched = True
                if not matched:
                    inp = _sval("input") or ""
                    if isinstance(inp, str) and inp.endswith(f"forwards/{fid}.json"):
                        matched = True
                if not matched:
                    # sometimes forward metadata is stored as JSON in a field called 'forward'
                    fwd = _sval("forward")
                    if fwd:
                        try:
                            if isinstance(fwd, (bytes, bytearray)):
                                fwd = fwd.decode("utf-8")
                            fj = json.loads(fwd) if isinstance(fwd, str) else fwd
                            if isinstance(fj, dict) and (str(fj.get("fid") or fj.get("forward_hash") or fj.get("file_id")) == str(fid)):
                                matched = True
                        except Exception:
                            pass

                if not matched:
                    continue

                # attempt requeue lock (avoid repeated enqueues)
                lock_key = f"ffmpeg:requeue_lock:{job_id}"
                try:
                    set_ok = await client.set(lock_key, "1", nx=True, ex=REQUEUE_LOCK_TTL)
                    if not set_ok:
                        # already requeued recently
                        continue
                except Exception:
                    # best-effort: if set fails, proceed (may duplicate)
                    pass

                # Build minimal job payload and call enqueue_job which HSETs then LPUSHes
                try:
                    from utils.job_queue import enqueue_job
                except Exception:
                    enqueue_job = None

                job = {"job_id": job_id, "input_key": remote_key}
                try:
                    # attempt to preserve original output_path or original_filename if available
                    out = _sval("output")
                    if out:
                        job["output_path"] = out
                    orig = _sval("original_filename") or _sval("original_name")
                    if orig:
                        job["original_filename"] = orig
                except Exception:
                    pass

                if enqueue_job:
                    try:
                        await enqueue_job(job)
                        logger.info("Re-enqueued job %s with input_key %s", job_id, remote_key)
                    except Exception:
                        logger.exception("Failed to enqueue job %s", job_id)
                else:
                    # Fallback: perform direct LPUSH of a minimal job JSON
                    try:
                        await client.lpush("ffmpeg:jobs", json.dumps(job))
                        logger.info("LPUSH fallback re-enqueued job %s", job_id)
                    except Exception:
                        logger.exception("Fallback LPUSH failed for job %s", job_id)

            except Exception:
                logger.exception("Error while scanning job key %s", key)
    except Exception:
        logger.exception("Failed scanning Redis for job hashes")


async def _async_run():
    if not aioredis:
        logger.error("redis.asyncio not available; cannot run auto reenricher")
        return 2
    if not REDIS_URL:
        logger.error("REDIS_URL is required")
        return 2

    client = aioredis.from_url(REDIS_URL, decode_responses=True)
    pub = client.pubsub()
    await pub.subscribe(FORWARD_CHANNEL)
    logger.info("Subscribed to %s", FORWARD_CHANNEL)
    try:
        async for msg in pub.listen():
            if not msg:
                continue
            if msg.get("type") != "message":
                continue
            data = msg.get("data")
            if not data:
                continue
            try:
                if isinstance(data, (bytes, bytearray)):
                    data = data.decode("utf-8")
                payload = json.loads(data)
            except Exception:
                payload = {"fid": data}
            fid = payload.get("fid") or payload.get("forward_hash")
            if not fid:
                try:
                    fid = str(data)
                except Exception:
                    fid = None
            if fid:
                await _process_forward(fid, payload, client)
    finally:
        try:
            await pub.unsubscribe(FORWARD_CHANNEL)
        except Exception:
            pass
        try:
            aclose = getattr(client, "aclose", None)
            if aclose:
                await aclose()
            else:
                await client.close()
        except Exception:
            pass
    return 0


def main():
    try:
        return asyncio.run(_async_run())
    except KeyboardInterrupt:
        logger.info("Interrupted")
        return 0


if __name__ == "__main__":
    sys.exit(main())
