import json
import os
import asyncio
import logging
from typing import Optional
from urllib.parse import urlparse

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

# Do not hard-code localhost defaults. Require REDIS_URL to be set in environment
DEFAULT_REDIS_URL = None
JOB_LIST = "ffmpeg:jobs"


async def get_redis():
    if not aioredis:
        raise RuntimeError("redis.asyncio is required for job queue")
    # read the env var at call-time so runtime env changes or late injection work
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        raise RuntimeError("REDIS_URL environment variable is not set")
    # Log a masked host:port for diagnostics (do not print credentials)
    try:
        parsed = urlparse(redis_url)
        hostport = parsed.hostname or ""
        if parsed.port:
            hostport = f"{hostport}:{parsed.port}"
        logging.getLogger(__name__).debug("Connecting to Redis at %s (scheme=%s)", hostport, parsed.scheme)
    except Exception:
        pass

    return aioredis.from_url(redis_url)


async def enqueue_job(job: dict) -> None:
    """Push a job dict to the Redis job list."""
    r = await get_redis()
    await r.lpush(JOB_LIST, json.dumps(job))
    # persist to Mongo if available (best-effort)
    try:
        from .job_store import init as _init_store, save_job

        # Fire-and-forget init if env provided
        mongo_uri = os.environ.get("MONGO_URI")
        if mongo_uri:
            try:
                # ensure client available
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(save_job(job))
                else:
                    loop.run_until_complete(save_job(job))
            except Exception:
                pass
    except Exception:
        pass

    await r.close()


async def pop_job(timeout: int = 5) -> Optional[dict]:
    """Blocking pop a job from the Redis job list (BRPOP semantics)."""
    r = await get_redis()
    try:
        item = await r.brpop(JOB_LIST, timeout=timeout)
        if not item:
            return None
        # item is (list_name, data)
        raw = item[1].decode() if isinstance(item[1], bytes) else item[1]
        return json.loads(raw)
    finally:
        await r.close()


async def publish_update(channel: str, payload: dict) -> None:
    r = await get_redis()
    try:
        await r.publish(channel, json.dumps(payload))
    finally:
        await r.close()


async def cancel_job(job_id: str) -> None:
    """Set cancel flag for a job."""
    r = await get_redis()
    try:
        await r.hset(f"ffmpeg:job:{job_id}", mapping={"cancel": "1"})
    finally:
        await r.close()
