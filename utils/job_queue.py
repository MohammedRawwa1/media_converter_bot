import asyncio
import contextlib
import json
import logging
import os
import time
import uuid
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
    # Use a shared aioredis client for the process to avoid exhausting
    # Redis server client slots. We return a lightweight proxy whose
    # `close()` is a no-op so existing call sites that `await r.close()`
    # remain safe; call `close_redis()` at shutdown to close the real
    # client.
    global _redis_client, _redis_proxy
    if not aioredis:
        raise RuntimeError("redis.asyncio is required for job queue")
    # read the env var at call-time so runtime env changes or late injection work
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        raise RuntimeError("REDIS_URL environment variable is not set")

    # If already created, return proxy
    try:
        if _redis_proxy is not None:
            return _redis_proxy
    except NameError:
        # fall through to create
        pass

    # Log a masked host:port for diagnostics (do not print credentials)
    try:
        parsed = urlparse(redis_url)
        hostport = parsed.hostname or ""
        if parsed.port:
            hostport = f"{hostport}:{parsed.port}"
        logging.getLogger(__name__).debug("Connecting to Redis at %s (scheme=%s)", hostport, parsed.scheme)
    except Exception:
        pass

    # module-level storage for the real client and proxy
    _redis_client = aioredis.from_url(redis_url, decode_responses=True, max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", "50")))

    class _RedisProxy:
        def __init__(self, client):
            self._client = client

        def __getattr__(self, name):
            return getattr(self._client, name)

        async def close(self):
            # no-op: callers may `await r.close()` safely; call close_redis()
            # at shutdown to close the real client.
            return

    _redis_proxy = _RedisProxy(_redis_client)
    return _redis_proxy


async def close_redis():
    """Close the shared Redis client (call at process shutdown)."""
    global _redis_client, _redis_proxy
    try:
        if _redis_client is not None:
            try:
                aclose = getattr(_redis_client, "aclose", None)
                if aclose is not None:
                    await aclose()
                else:
                    await _redis_client.close()
            except Exception:
                pass
    finally:
        _redis_client = None
        _redis_proxy = None


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
    # If no request_id provided, generate one for end-to-end tracing
    try:
        if not job.get("request_id"):
            job["request_id"] = str(uuid.uuid4())
    except Exception:
        pass

    # Initialize a Redis job hash so status endpoints see the job immediately.
    # Write the job hash before pushing to the list to avoid a race where a
    # worker pops the job before the metadata has been created.
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
                "request_id": job.get("request_id") or "",
            }
            # Attempt to set the hash first
            try:
                await r.hset(f"ffmpeg:job:{job_id}", mapping=mapping)
                if JOB_METADATA_TTL and JOB_METADATA_TTL > 0:
                    with contextlib.suppress(Exception):
                        await r.expire(f"ffmpeg:job:{job_id}", JOB_METADATA_TTL)
            except Exception:
                # best-effort - proceed to push the job even if hset fails
                pass

            try:
                src = mapping.get("input")
                out = mapping.get("output")
                logging.getLogger(__name__).info("Prepared job %s request_id=%s input=%s output=%s", job_id, mapping.get("request_id"), src, out)
            except Exception:
                pass

    except Exception:
        pass

    # Finally push the job into the queue
    try:
        await r.lpush(JOB_LIST, json.dumps(job))
    except Exception:
        # If push fails, there's not much we can do here - leave the hash as-is
        with contextlib.suppress(Exception):
            logging.getLogger(__name__).exception("Failed to push job onto Redis list for job %s", job.get("job_id"))
    except Exception:
        pass
    # persist to Mongo if available (best-effort)
    try:
        from .job_store import save_job

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


async def pop_job(timeout: int = 5) -> dict | None:
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
                    with contextlib.suppress(Exception):
                        # remove then push to front of queue so it will be picked in order
                        await r.zrem(DELAYED_SET, raw)
                    with contextlib.suppress(Exception):
                        await r.lpush(JOB_LIST, raw)
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


async def release_input_lock(lock_key: str, owner_job_id: str, redis_client=None) -> bool:
    """Delete a Redis input lock if it is still owned by the provided job.

    This is intentionally defensive: it uses a Lua script when possible and falls
    back to a simple get/delete sequence if the client does not support eval.
    """
    if not lock_key or not owner_job_id:
        return False

    close_client = False
    client = redis_client
    if client is None:
        try:
            client = await get_redis()
            close_client = True
        except Exception:
            return False

    try:
        script = """
        local current = redis.call('get', KEYS[1])
        if current == ARGV[1] then
            redis.call('del', KEYS[1])
            return 1
        end
        return 0
        """
        try:
            result = await client.eval(script, 1, lock_key, owner_job_id)
            if result:
                logging.getLogger(__name__).info("Released input lock %s for job %s", lock_key, owner_job_id)
                return True
        except Exception:
            pass

        try:
            current = await client.get(lock_key)
            if isinstance(current, bytes):
                current = current.decode()
            if current == owner_job_id:
                await client.delete(lock_key)
                logging.getLogger(__name__).info("Released input lock %s for job %s", lock_key, owner_job_id)
                return True
        except Exception:
            pass

        return False
    finally:
        if close_client and client is not None:
            with contextlib.suppress(Exception):
                await client.close()
