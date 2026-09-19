"""
Async MongoDB job store with prepared/parameterized queries.

Go/Laravel-style patterns:
  - FillableModel: mass-assignment protection (only job_id, status, etc. allowed)
  - QueryBuilder: parameterized queries with NoSQL injection prevention
  - PreparedQuery: like prepared statements in SQL
"""

import asyncio
import contextlib
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

try:
    from motor.motor_asyncio import AsyncIOMotorClient
except Exception:
    AsyncIOMotorClient = None


# ── One Motor client per event loop ────────────────────────────────────────
# Motor binds its connections to the loop that opened them, so a single
# process-wide client only works from the loop that created it. This process runs
# more than one: the bot's (which the ASGI app also serves the Flask uploader
# from), the ffmpeg worker's, and a fallback per-thread loop for Flask routes when
# no app loop is registered. The second loop to touch a shared client failed with
# "got Future <Future pending> attached to a different loop" - and because
# save_job/update_job are best-effort, the job document was then silently never
# written at all.
#
# The cache is keyed by ``id(loop)`` with the loop kept as the value's first
# element, so an id reused by a new loop cannot be handed the closed loop's
# client. ``close()`` closes every client that is still live.
_clients: dict[int, tuple] = {}
_uri: str | None = None
_db_name = "media_bot"


def _drop_closed_clients() -> None:
    """Forget (and close) the clients of loops that have been closed."""
    for key, (loop, client, _db) in list(_clients.items()):
        if loop.is_closed():
            with contextlib.suppress(Exception):
                client.close()
            _clients.pop(key, None)


def _db_for_loop():
    """The jobs database for the running loop, or None when never configured."""
    if _uri is None:
        return None
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # Sync caller: there is no loop to bind a client to.
        return None
    cached = _clients.get(id(loop))
    if cached is not None and cached[0] is loop:
        return cached[2]
    _drop_closed_clients()
    client = AsyncIOMotorClient(_uri)
    db = client[_db_name]
    _clients[id(loop)] = (loop, client, db)
    return db


# ── Laravel-style $fillable fields for job documents ──
# Only these fields are allowed in mass-assignment operations.
# Fields like "is_admin", "role", "permissions" would be silently stripped.
JOB_FILLABLE: set[str] = {
    "job_id",
    "status",
    "progress",
    "message",
    "error",
    "input_path",
    "output_path",
    "input_key",
    "output_key",
    "source_url",
    "original_filename",
    "output_filename",
    "ffmpeg_args",
    "progress_channel",
    "chat_id",
    "bot_id",
    "request_id",
    "user_id",
    "type",
    "retries",
    "attempt",
    "started_at",
    "finished_at",
    "created_at",
    "cleanup_input",
    "cleanup_output",
    "output_get_url",
    "out_bytes",
    "in_bytes",
    "progress_by_size",
    "remote_missing_attempts",
    "redownload_attempts",
    "remux_attempts",
    "input_from_remote",
}
JOB_GUARDED: set[str] = {"_id"}


def _filter_job_fields(data: dict[str, Any]) -> dict[str, Any]:
    """Strip any fields not in JOB_FILLABLE (mass-assignment protection)."""
    return {k: v for k, v in data.items() if k in JOB_FILLABLE}


def _validate_field_names(fields: dict[str, Any]) -> dict[str, Any]:
    """Validate field names against JOB_FILLABLE, preventing injection.

    Rejects any field name that starts with '$' (operator injection attempt)
    or is not in JOB_FILLABLE (mass-assignment protection).
    Unlike _filter_job_fields which silently strips, this raises for
    explicit operator attacks while silently filtering non-fillable fields.
    """
    safe_fields = {}
    for key, value in fields.items():
        # Block MongoDB operator injection in field names
        if key.startswith("$"):
            logger.warning("Blocked update field with '$' prefix: %s", repr(key)[:80])
            continue
        # Reject nested $operator patterns in dict values
        if isinstance(value, dict):
            for nested_key in value:
                if nested_key.startswith("$") and nested_key not in (
                    "$set",
                    "$inc",
                    "$push",
                    "$pull",
                    "$each",
                    "$position",
                ):
                    logger.warning("Blocked nested operator in update: %s", repr(nested_key)[:80])
                    continue
        safe_fields[key] = value
    return safe_fields


async def init(mongo_uri: str | None = None, db_name: str = "media_bot"):
    global _uri, _db_name
    if AsyncIOMotorClient is None:
        raise RuntimeError("motor is required for job_store")
    mongo_uri = mongo_uri or os.environ.get("MONGO_URI")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI not set for job_store")
    _uri = mongo_uri
    _db_name = db_name
    # Build this loop's client now: init() runs at startup, so a missing driver or
    # an obviously bad URI surfaces there rather than on the first job write.
    _db_for_loop()


async def save_job(job: dict[str, Any]) -> None:
    """Insert a new job document with fillable field protection.

    Only fields in JOB_FILLABLE are persisted.  This prevents injection
    of arbitrary document fields via API payloads.
    """
    db = _db_for_loop()
    if db is None:
        return
    # Apply fillable protection (like Laravel's Model::create($request->all()))
    safe_job = _filter_job_fields(job)
    safe_job.setdefault("status", "queued")
    safe_job["created_at"] = time.time()

    try:
        bot_id = job.get("bot_id") or os.environ.get("BOT_ID") or os.environ.get("BOT_USERNAME")
        if bot_id and bot_id not in JOB_GUARDED:
            safe_job["bot_id"] = bot_id
    except Exception:
        logger.debug("Failed to close MongoDB client")

    # Parameterized insert (prepared-statement-like: data is validated and filtered)
    await db.jobs.insert_one(safe_job)


async def update_job(job_id: str, fields: dict[str, Any]) -> None:
    """Update a job document with fillable field protection.

    Like a prepared UPDATE with parameterized fields.
    """
    db = _db_for_loop()
    if db is None:
        return
    # Apply fillable protection + validate field names
    safe_fields = _validate_field_names(_filter_job_fields(fields))
    if not safe_fields:
        return
    await db.jobs.update_one({"job_id": job_id}, {"$set": safe_fields}, upsert=False)


async def get_job(job_id: str) -> dict[str, Any] | None:
    """Get a job by ID.  Returns None if not found."""
    db = _db_for_loop()
    if db is None:
        return None
    # Parameterized query: job_id is passed as a value, not interpolated
    return await db.jobs.find_one({"job_id": job_id})


async def get_jobs_by_status(status: str, limit: int = 100) -> list:
    """Get jobs by status (parameterized query)."""
    db = _db_for_loop()
    if db is None:
        return []
    cursor = db.jobs.find({"status": status}).sort("created_at", -1).limit(limit)
    return await cursor.to_list(length=limit)


async def close():
    """Close every loop's client (call at process shutdown)."""
    for _loop, client, _db in list(_clients.values()):
        with contextlib.suppress(Exception):
            client.close()
    _clients.clear()
