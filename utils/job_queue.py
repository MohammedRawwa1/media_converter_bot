import asyncio
import contextlib
import json
import logging
import os
import time
import uuid
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

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

# Job-hash fields that describe the media's name. A requeue rebuilds the payload
# from the stored hash (often carrying only the id, input and output paths), so
# these have to be carried over explicitly or the redelivered file comes back
# named after the job id.
NAMING_JOB_FIELDS = ("original_filename", "output_filename")

# Older hashes stored the name under `original_name`; the worker accepts both.
_NAMING_FIELD_SOURCES = {
    "original_filename": ("original_filename", "original_name"),
    "output_filename": ("output_filename",),
}


def _as_hash_str(value):
    """Normalize a value out of a Redis hash, which may arrive as bytes."""
    if isinstance(value, bytes):
        try:
            value = value.decode()
        except Exception:
            return None
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    return str(value) or None


# How a queued video output should reach the user: playable media (Telegram
# shows a preview) or a document. Mirrors utils.callbacks.UPLOAD_MODE_*.
_UPLOAD_MODES = ("video", "file")


def _stamp_upload_mode(job: dict) -> None:
    """Copy the owner's video delivery preference onto the job payload.

    Done at the one place every job passes through, so a conversion queued by
    the bot, the web UI or a script is delivered the way /usersettings asks
    without each call site having to remember to pass it. An explicit
    ``upload_mode`` already on the payload always wins.
    """
    if job.get("upload_mode"):
        return
    owner = job.get("user_id") or job.get("chat_id")
    if not owner:
        return
    try:
        # Imported lazily: this module is used by the worker too, where the
        # settings store may not exist and the default is the right answer.
        from utils.user_settings import get_user_setting

        mode = str(get_user_setting(owner, "upload_mode") or "").strip().lower()
        if mode in _UPLOAD_MODES:
            job["upload_mode"] = mode
    except Exception:
        logger.debug("job_queue: could not read upload_mode for %s", owner)


def _normalized_hash(stored: dict | None) -> dict:
    """Normalize a hash read without ``decode_responses`` (bytes keys) to str keys.

    Every client in this project decodes responses, but a stray sync client or a
    raw pipeline can still hand back bytes, and looking up the name then silently
    fails.
    """
    normalized = {}
    for key, value in (stored or {}).items():
        if isinstance(key, (bytes, bytearray)):
            try:
                key = key.decode()
            except Exception:
                continue
        normalized[str(key)] = value
    return normalized


def stored_job_naming(stored: dict | None) -> dict:
    """Extract the usable naming fields from a stored job hash.

    Bytes-safe and alias-aware, so a hash written by any past version still
    yields a name.
    """
    stored = _normalized_hash(stored)
    naming = {}
    for field in NAMING_JOB_FIELDS:
        for source in _NAMING_FIELD_SOURCES.get(field, (field,)):
            try:
                value = _as_hash_str(stored.get(source))
            except Exception:
                value = None
            if value:
                naming[field] = value
                break
    return naming


def carry_over_job_naming(job: dict, stored: dict | None, *, overwrite: bool = False) -> dict:
    """Copy the media name recorded in a stored job hash onto a rebuilt payload.

    Use this when requeueing a job: the name lives in the hash, not in the
    payload the requeue builds. With ``overwrite=False`` (the default) an
    explicit name already on the payload wins, so a caller-supplied override is
    never silently replaced.
    """
    for field, value in stored_job_naming(stored).items():
        if overwrite or not job.get(field):
            job[field] = value
    return job


# Job-hash fields that identify the job's owner. A requeue rebuilds the payload
# from the stored hash, so the owner has to be carried over explicitly - without
# it the worker has no chat_id to deliver to and the user who queued the job
# never receives their file.
OWNER_JOB_FIELDS = ("chat_id", "user_id")


def stored_job_owners(stored: dict | None) -> dict:
    """Extract the owner fields from a stored job hash, bytes-safe."""
    stored = _normalized_hash(stored)
    owners = {}
    for field in OWNER_JOB_FIELDS:
        value = _as_hash_str(stored.get(field))
        if value:
            owners[field] = value
    return owners


def carry_over_job_owners(job: dict, stored: dict | None, *, overwrite: bool = False) -> dict:
    """Copy the owner recorded in a stored job hash onto a rebuilt payload.

    Mirrors :func:`carry_over_job_naming` for the delivery target. Numeric ids
    are converted back to ``int`` because the hash stores them as strings and
    the worker's Telegram calls expect a chat id, not a string.
    """
    for field, value in stored_job_owners(stored).items():
        if overwrite or not job.get(field):
            try:
                job[field] = int(value)
            except (TypeError, ValueError):
                job[field] = value
    return job


# ── One Redis client per event loop ─────────────────────────────────────────
# redis.asyncio binds every connection to the event loop that opened it, so one
# client per *process* is only usable from the loop that created it. This process
# runs more than one loop: the bot's, a persistent loop per WSGI thread for the
# Flask uploader (web/webapp.py), and short-lived loops a few paths spin up in a
# thread. Sharing a single client across them fails exactly the way an unreadable
# session did - "got Future <Future pending> attached to a different loop" - on a
# command that then never returns.
#
# The cache is keyed by ``id(loop)``, and the loop itself is kept as the value's
# first element: an id reused by a *new* loop therefore cannot be handed the dead
# loop's client. Entries whose loop has been closed are dropped on the next call,
# and close_redis() closes whatever is left at shutdown.
_redis_clients: dict[int, tuple] = {}


def _drop_closed_redis_clients() -> None:
    """Forget the clients of loops that have been closed since the last call."""
    for key, (loop, _proxy) in list(_redis_clients.items()):
        if loop.is_closed():
            _redis_clients.pop(key, None)


def _loop_redis_proxy(redis_url: str):
    """The client for one loop, wrapped so ``await r.close()`` stays safe.

    The proxy's ``close()`` is a no-op because dozens of call sites end with
    ``await r.close()`` and they are right to: the connection is the process's,
    not theirs. The real client is closed by ``close_redis()`` at shutdown, or
    when its loop is closed and the entry is dropped.
    """

    class _RedisProxy:
        def __init__(self, client):
            self._client = client

        def __getattr__(self, name):
            return getattr(self._client, name)

        async def close(self):
            return

    client = aioredis.from_url(
        redis_url, decode_responses=True, max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))
    )
    return _RedisProxy(client)


async def get_redis():
    """The Redis client belonging to the running loop, created on first use.

    One client per loop rather than one per process - see ``_redis_clients`` for
    why, and note that every existing caller keeps working unchanged: it is the
    *client*, not the caller, that had to be given the right loop.
    """
    if not aioredis:
        raise RuntimeError("redis.asyncio is required for job queue")
    # read the env var at call-time so runtime env changes or late injection work
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        raise RuntimeError("REDIS_URL environment variable is not set")

    loop = asyncio.get_running_loop()
    cached = _redis_clients.get(id(loop))
    if cached is not None and cached[0] is loop and not loop.is_closed():
        return cached[1]

    _drop_closed_redis_clients()

    # Log a masked host:port for diagnostics (do not print credentials)
    try:
        parsed = urlparse(redis_url)
        hostport = parsed.hostname or ""
        if parsed.port:
            hostport = f"{hostport}:{parsed.port}"
        logger.debug("Connecting to Redis at %s (scheme=%s)", hostport, parsed.scheme)
    except Exception:
        logger.debug("job_queue: failed to parse REDIS_URL for diagnostic logging")

    proxy = _loop_redis_proxy(redis_url)
    _redis_clients[id(loop)] = (loop, proxy)
    return proxy


async def close_redis():
    """Close every loop's Redis client (call at process shutdown)."""
    clients = [(loop, proxy) for loop, proxy in _redis_clients.values()]
    _redis_clients.clear()
    for _loop, proxy in clients:
        client = getattr(proxy, "_client", None)
        if client is None:
            continue
        try:
            aclose = getattr(client, "aclose", None)
            if aclose is not None:
                await aclose()
            else:
                await client.close()
        except Exception:
            # A client whose loop is not this one can refuse to close from here;
            # its sockets go when the process does.
            logger.debug("job_queue: failed to close a Redis client during shutdown")


# ── The Mongo write that follows a queue push ───────────────────────────────
# Kept referenced until it finishes: an unreferenced task can be collected
# mid-flight, which loses the job document, and its exception is retrieved so a
# Mongo that is down is a debug line instead of a silent failure. The write stays
# on the caller's loop, which is the loop job_store's client belongs to - a loop
# of this module's own (the old ``run_until_complete`` fallback) would fail the
# same way the session load did, "got Future ... attached to a different loop".
_job_writes: set = set()


def _schedule_job_write(save_job, job: dict) -> None:
    """Persist one job document in the background, on the running loop."""
    coro = save_job(job)
    try:
        task = asyncio.create_task(coro)
    except RuntimeError:
        # No running loop to bind the write to; nothing awaited it, so close it.
        coro.close()
        logger.debug("job_queue: no running loop to persist job %s", job.get("job_id", "?"))
        return
    _job_writes.add(task)

    def _finished(done) -> None:
        _job_writes.discard(done)
        with contextlib.suppress(Exception):
            error = done.exception()
            if error is not None:
                logger.debug("job_queue: failed to persist job %s to Mongo: %s", job.get("job_id", "?"), error)

    task.add_done_callback(_finished)


async def enqueue_job(job: dict) -> None:
    """Push a job dict to the Redis job list."""
    r = await get_redis()
    batch_id = job.get("batch_id")
    if batch_id:
        try:
            from utils.batch_pipeline import batch_cancel_key, batch_jobs_key

            if await r.exists(batch_cancel_key(batch_id)):
                job_id = job.get("job_id")
                if job_id:
                    await r.hset(
                        f"ffmpeg:job:{job_id}",
                        mapping={"status": "cancelled", "cancel": "1", "message": "batch cancelled"},
                    )
                return
        except Exception:
            logger.debug("job_queue: batch cancellation check failed")
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
            logger.debug("job_queue: path normalization failed for job")

    # If no request_id provided, generate one for end-to-end tracing
    try:
        if not job.get("request_id"):
            job["request_id"] = str(uuid.uuid4())
    except Exception:
        logger.debug("job_queue: failed to set request_id for job")

    _stamp_upload_mode(job)

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
                "created_at": str(time.time()),
                "request_id": job.get("request_id") or "",
            }
            for _batch_field in ("batch_id", "batch_seq", "batch_total"):
                if job.get(_batch_field) not in (None, ""):
                    mapping[_batch_field] = str(job[_batch_field])
            output_value = job.get("output_path") or job.get("output") or ""
            if output_value:
                mapping["output"] = output_value
            # Record the owner so monitoring can attribute a running/queued job
            # to a user without re-reading the queue payload (which a worker has
            # already popped by the time a status command looks at the hashes).
            for _owner_field in ("chat_id", "user_id"):
                _owner_value = job.get(_owner_field)
                if _owner_value not in (None, ""):
                    mapping[_owner_field] = str(_owner_value)
            # Persist the naming fields so a requeued job (or a retry that only
            # reads the hash back) still delivers under the original media name.
            # These are written only when the payload actually carries them: a
            # requeue payload has no name, and writing "" here would erase the
            # name already stored for this job id.
            for _field, _value in stored_job_naming(job).items():
                mapping[_field] = _value
            # Kept in the hash as well as the payload so a status view can show
            # how the result will be delivered.
            if job.get("upload_mode"):
                mapping["upload_mode"] = str(job["upload_mode"])
            # Attempt to set the hash first
            try:
                await r.hset(f"ffmpeg:job:{job_id}", mapping=mapping)
                if JOB_METADATA_TTL and JOB_METADATA_TTL > 0:
                    with contextlib.suppress(Exception):
                        await r.expire(f"ffmpeg:job:{job_id}", JOB_METADATA_TTL)
            except Exception:
                logger.debug("job_queue: hset failed for job %s (best-effort)", job_id)
                # best-effort - proceed to push the job even if hset fails

            try:
                src = mapping.get("input")
                out = mapping.get("output")
                logging.getLogger(__name__).info(
                    "Prepared job %s request_id=%s input=%s output=%s", job_id, mapping.get("request_id"), src, out
                )
            except Exception:
                logger.debug("job_queue: failed to log job preparation for %s", job_id)

            if batch_id:
                try:
                    from utils.batch_pipeline import batch_jobs_key

                    await r.sadd(batch_jobs_key(batch_id), str(job_id))
                    await r.expire(batch_jobs_key(batch_id), JOB_METADATA_TTL or 86400)
                except Exception:
                    logger.debug("job_queue: failed to index batch job %s", job_id)

    except Exception:
        logger.debug("job_queue: failed to prepare job hash for %s", job_id)

    if batch_id:
        try:
            from utils.batch_pipeline import batch_cancel_key, batch_jobs_key

            if await r.exists(batch_cancel_key(batch_id)):
                if job_id:
                    await r.hset(
                        f"ffmpeg:job:{job_id}",
                        mapping={"status": "cancelled", "cancel": "1", "message": "batch cancelled"},
                    )
                    await r.srem(batch_jobs_key(batch_id), str(job_id))
                return
        except Exception:
            logger.debug("job_queue: final batch cancellation check failed")

    # Push the job onto its queue. Which queue is decided per job: with the
    # RabbitMQ backend enabled and a rollout below 100%, only a share of jobs
    # goes to the broker and every other job keeps using the Redis list (see
    # utils.eventbus). A broker that is unreachable or refuses the publish also
    # falls back to the list, so a broker problem costs latency, never a job.
    routed_to_broker = False
    try:
        from utils.eventbus import publish_job

        routed_to_broker = await publish_job(job)
    except Exception:
        logger.debug("job_queue: event bus unavailable for job %s", job.get("job_id"))

    if not routed_to_broker:
        try:
            await r.lpush(JOB_LIST, json.dumps(job))
        except Exception:
            # If push fails, there's not much we can do here - leave the hash as-is
            with contextlib.suppress(Exception):
                logging.getLogger(__name__).exception(
                    "Failed to push job onto Redis list for job %s", job.get("job_id")
                )
            logger.debug("job_queue: lpush failed for job %s", job.get("job_id"))

    # Lifecycle event: the job has been accepted by a queue (best-effort; the
    # event log is a record of what happened, not a second queue).
    try:
        from utils.eventbus import JOB_QUEUED, emit_event

        await emit_event(
            JOB_QUEUED,
            job=job,
            payload={"queue": "rabbitmq" if routed_to_broker else "redis"},
            source="enqueue",
        )
    except Exception:
        logger.debug("job_queue: event mirror failed for job %s", job.get("job_id"))
    # persist to Mongo if available (best-effort)
    try:
        from .job_store import save_job

        # Fire-and-forget init if env provided
        if os.environ.get("MONGO_URI"):
            _schedule_job_write(save_job, job)
    except Exception:
        logger.debug("job_queue: failed to import save_job")

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
            logger.debug("job_queue: failed to promote delayed jobs")
            # best-effort; don't fail pop if this step errors
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

    # Mirror progress into the event log. This is the single choke point every
    # progress update already goes through, which is why the mirroring lives
    # here instead of at the ~30 call sites. Throttled and best-effort: the
    # Redis publish above has already happened, and a Kafka problem must never
    # affect a progress update.
    try:
        if isinstance(payload, dict) and payload.get("job_id"):
            from utils.eventbus import JOB_PROGRESS, emit_event

            await emit_event(
                JOB_PROGRESS,
                job_id=str(payload.get("job_id")),
                payload={
                    key: value
                    for key, value in payload.items()
                    if key in ("progress", "message", "status", "note", "error")
                },
                source=channel,
            )
    except Exception:
        logger.debug("job_queue: progress event mirror failed")


async def cancel_job(job_id: str) -> str:
    """Cancel a job: set cancel=1 flag in the hash and release the input lock.

    Running workers detect cancel=1 in the hash and stop processing. The hash
    is preserved (with cancel=1) so the worker sees it; cleanup is handled by
    the hash's TTL (default 1 day).

    Returns what actually happened, because "cancelled" is not the only honest
    answer: a job that already delivered its file is ``"done"``, one that
    failed is ``"error"``, one this deployment has no record of is
    ``"missing"``, and anything else is ``"cancelled"``. Every caller that
    reports to a user needs those apart - the terminal hash is deliberately
    kept so a watcher can render the outcome, which means an old Cancel button
    can outlive its job, and blindly overwriting that hash reported a delivered
    file as a cancellation the user never got.
    """
    import hashlib as _hl

    r = await get_redis()
    try:
        key = f"ffmpeg:job:{job_id}"

        # 1. Read the job hash before deleting to extract lock inputs
        _hash_read = True
        try:
            stored = await r.hgetall(key)
        except Exception:
            stored = {}
            _hash_read = False

        _stored_status = str((stored or {}).get("status") or "").strip().lower()

        # A job that already reached its end is not cancelled: its hash is the
        # record of what it did, and a file the user has in hand cannot be
        # un-delivered by a button pressed afterwards.
        if _stored_status in ("done", "error"):
            logger.info("cancel_job: job %s already finished (status=%s); nothing to cancel", job_id, _stored_status)
            return _stored_status
        if _stored_status == "cancelled":
            logger.info("cancel_job: job %s was already cancelled", job_id)
            return "cancelled"
        # No hash at all - and the read succeeded, so this is a fact rather than
        # a Redis outage: nothing is queued or running under this id.
        if _hash_read and not stored:
            logger.info("cancel_job: no job hash for %s; nothing to cancel", job_id)
            return "missing"

        # 2. Set cancel flag in the hash so running workers detect it
        #    and stop processing. The hash will be cleaned up by TTL.
        #
        #    ``cancel_notified`` rides in the same write as ``status``: the
        #    progress watcher reads the hash in one go, so it can never see the
        #    cancellation without also seeing that the user was already told
        #    about it - which is what the separate pre-write this replaces
        #    existed to approximate.
        try:
            await r.hset(
                key,
                mapping={
                    "cancel": "1",
                    "status": "cancelled",
                    "message": "cancelled by user",
                    "progress": "0",
                    "cancel_notified": "1",
                },
            )
        except Exception:
            logger.warning("cancel_job: failed to set cancel flag for %s", job_id)

        # 3. Remove the job from the queue list so a worker doesn't pop it later.
        #    Uses a Lua script to atomically scan and remove matching entries.
        try:
            _prune_script = """
                local key = KEYS[1]
                local target_job_id = ARGV[1]
                local removed = 0
                local limit = 500
                -- Scan the list for matching entries (limited to 500 to avoid blocking)
                local items = redis.call('lrange', key, 0, limit)
                for _, item in ipairs(items) do
                    local ok, parsed = pcall(cjson.decode, item)
                    if ok and type(parsed) == 'table' and parsed.job_id == target_job_id then
                        redis.call('lrem', key, 1, item)
                        removed = removed + 1
                    end
                end
                return removed
            """
            _removed = await r.eval(_prune_script, 1, JOB_LIST, job_id)
            if _removed and int(_removed) > 0:
                logger.info("cancel_job: pruned %s entries from queue list for job %s", _removed, job_id)
        except Exception:
            logger.debug("cancel_job: failed to prune queue list for %s (best-effort)", job_id)

        # 4. Compute and release the input lock so new jobs with the same
        #    input can acquire it.
        #    Matches the worker's lock key computation:
        #      lock_name = (input_key or input_path or source_url or job_id)
        #      lock_hash = sha256(lock_name).hexdigest()
        #      lock_key  = f"ffmpeg:lock:{lock_hash}"
        lock_name = (
            (stored or {}).get("input_key") or (stored or {}).get("input") or (stored or {}).get("source_url") or job_id
        )
        lock_hash = _hl.sha256(str(lock_name).encode()).hexdigest()
        lock_key = f"ffmpeg:lock:{lock_hash}"
        try:
            await release_input_lock(lock_key, job_id, redis_client=r)
            logger.info("cancel_job: released lock %s for job %s", lock_key, job_id)
        except Exception:
            logger.debug("cancel_job: lock release failed for %s", job_id)

        logger.info("cancel_job: cancelled job %s (cancel=1 set in hash)", job_id)
        return "cancelled"
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
            logger.debug("job_queue: Lua eval failed for lock %s", lock_key)

        try:
            current = await client.get(lock_key)
            if isinstance(current, bytes):
                current = current.decode()
            if current == owner_job_id:
                await client.delete(lock_key)
                logging.getLogger(__name__).info("Released input lock %s for job %s", lock_key, owner_job_id)
                return True
        except Exception:
            logger.debug("job_queue: get/delete fallback failed for lock %s", lock_key)

        return False
    finally:
        if close_client and client is not None:
            with contextlib.suppress(Exception):
                await client.close()
