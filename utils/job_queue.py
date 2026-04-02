import json
import os
import asyncio
import logging
import time
from typing import Optional
from urllib.parse import urlparse

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

# Do not hard-code localhost defaults. Require REDIS_URL to be set in environment
DEFAULT_REDIS_URL = None
JOB_LIST = "ffmpeg:jobs"
DELAYED_SET = "ffmpeg:delayed"
# Optional TTL (seconds) for job metadata hashes created at enqueue time.
# Default to 1 day (86400 seconds) so job metadata does not persist indefinitely.
# Set JOB_METADATA_TTL=0 to disable automatic expiry.
JOB_METADATA_TTL = int(os.getenv("JOB_METADATA_TTL", "86400"))


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
    # Normalize path separators for any local paths to a portable POSIX style
    try:
        import pathlib
        if job.get("input_path"):
            try:
                job["input_path"] = pathlib.PurePath(job["input_path"]).as_posix()
            except Exception:
                job["input_path"] = job["input_path"].replace("\\", "/")
        if job.get("output_path"):
            try:
                job["output_path"] = pathlib.PurePath(job["output_path"]).as_posix()
            except Exception:
                job["output_path"] = job["output_path"].replace("\\", "/")
    except Exception:
        # best-effort normalization; ignore failures
        try:
            if job.get("input_path"):
                job["input_path"] = job["input_path"].replace("\\", "/")
            if job.get("output_path"):
                job["output_path"] = job["output_path"].replace("\\", "/")
        except Exception:
            pass
    await r.lpush(JOB_LIST, json.dumps(job))
    # Initialize a Redis job hash so status endpoints see the job immediately.
    try:
        job_id = job.get("job_id")
        if job_id:
            mapping = {
                "status": "queued",
                "progress": 0,
                "message": "queued",
                # Prefer an explicit remote key when available so web UIs show where
                # the input lives even when local temp files are removed.
                "input": job.get("input_path") or job.get("input_key") or job.get("source_url") or "",
                "input_key": job.get("input_key") or "",
                "output": job.get("output_path") or job.get("output") or "",
                "created_at": str(time.time()),
            }
            # carry optional request_id for tracing (may be None)
            try:
                mapping["request_id"] = job.get("request_id") or ""
            except Exception:
                mapping["request_id"] = ""
            try:
                await r.hset(f"ffmpeg:job:{job_id}", mapping=mapping)
                # set optional TTL so job metadata does not live forever
                try:
                    if JOB_METADATA_TTL and JOB_METADATA_TTL > 0:
                        await r.expire(f"ffmpeg:job:{job_id}", JOB_METADATA_TTL)
                except Exception:
                    pass
            except Exception:
                # best-effort - do not fail enqueue if hset fails
                pass
            try:
                logging.getLogger(__name__).info("Enqueued job %s request_id=%s", job_id, job.get("request_id"))
            except Exception:
                pass
    except Exception:
        pass
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
        # Move any due delayed jobs back onto the active job list (best-effort)
        try:
            now = int(time.time())
            due = await r.zrangebyscore(DELAYED_SET, "-inf", now, 0, 50)
            if due:
                for item in due:
                    raw = item.decode() if isinstance(item, bytes) else item
                    try:
                        # remove then push to front of queue so it will be picked in order
                        await r.zrem(DELAYED_SET, raw)
                    except Exception:
                        pass
                    try:
                        await r.lpush(JOB_LIST, raw)
                    except Exception:
                        pass
        except Exception:
            # best-effort; don't fail pop if this step errors
            pass
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
