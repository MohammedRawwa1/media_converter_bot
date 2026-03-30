import json
import os
from typing import Optional

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
JOB_LIST = "ffmpeg:jobs"


async def get_redis():
    if not aioredis:
        raise RuntimeError("redis.asyncio is required for job queue")
    return aioredis.from_url(REDIS_URL)


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
