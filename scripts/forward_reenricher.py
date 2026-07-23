#!/usr/bin/env python3
"""Forward reenricher service

Listens on `FORWARD_PUBLISH_CHANNEL` (default `ffmpeg:forwards`) and republishes a
lightweight fetch request to `ffmpeg:fetch` (payload: {"forward_hash": <fid>}).

This is useful when forward metadata is saved but fetcher is running in a
separate process and should be triggered to fetch/enqueue the forwarded input.

Run: `python scripts/forward_reenricher.py`
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import sys

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

try:
    import redis as redis_sync
except Exception:
    redis_sync = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("forward_reenricher")

FORWARD_CHANNEL = os.environ.get("FORWARD_PUBLISH_CHANNEL", "ffmpeg:forwards")
FETCH_CHANNEL = os.environ.get("FETCH_CHANNEL", "ffmpeg:fetch")
REDIS_URL = os.environ.get("REDIS_URL")


async def _async_run():
    if not aioredis:
        logger.error("aioredis not available; cannot run async reenricher")
        return 2
    client = aioredis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else aioredis.Redis(decode_responses=True)
    pub = client.pubsub()
    await pub.subscribe(FORWARD_CHANNEL)
    logger.info("Subscribed to %s (async)", FORWARD_CHANNEL)
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
                # not JSON? treat as raw fid
                payload = {"fid": data}
            fid = payload.get("fid") or payload.get("forward_hash")
            if not fid:
                try:
                    fid = str(data)
                except Exception:
                    fid = None
            if fid:
                try:
                    await client.publish(FETCH_CHANNEL, json.dumps({"forward_hash": fid}))
                    logger.info("Republished forward_hash=%s -> %s", fid, FETCH_CHANNEL)
                except Exception:
                    logger.exception("Failed to publish fetch request for %s", fid)
    finally:
        with contextlib.suppress(Exception):
            await pub.unsubscribe(FORWARD_CHANNEL)
        with contextlib.suppress(Exception):
            await client.close()
    return 0


def _sync_run():
    if not redis_sync:
        logger.error("redis (sync) not available; cannot run reenricher")
        return 2
    client = redis_sync.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else redis_sync.Redis(decode_responses=True)
    pub = client.pubsub()
    pub.subscribe(FORWARD_CHANNEL)
    logger.info("Subscribed to %s (sync)", FORWARD_CHANNEL)
    try:
        for msg in pub.listen():
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
                try:
                    client.publish(FETCH_CHANNEL, json.dumps({"forward_hash": fid}))
                    logger.info("Republished forward_hash=%s -> %s", fid, FETCH_CHANNEL)
                except Exception:
                    logger.exception("Failed to publish fetch request for %s", fid)
    finally:
        with contextlib.suppress(Exception):
            pub.unsubscribe(FORWARD_CHANNEL)
        with contextlib.suppress(Exception):
            client.close()
    return 0


def main():
    # Prefer async mode when available
    if aioredis:
        try:
            return asyncio.run(_async_run())
        except KeyboardInterrupt:
            logger.info("Interrupted")
            return 0
    else:
        try:
            return _sync_run()
        except KeyboardInterrupt:
            logger.info("Interrupted")
            return 0


if __name__ == "__main__":
    sys.exit(main())
