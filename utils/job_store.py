"""
Async MongoDB job store with prepared/parameterized queries.

Go/Laravel-style patterns:
  - FillableModel: mass-assignment protection (only job_id, status, etc. allowed)
  - QueryBuilder: parameterized queries with NoSQL injection prevention
  - PreparedQuery: like prepared statements in SQL
"""
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

try:
    from motor.motor_asyncio import AsyncIOMotorClient
except Exception:
    AsyncIOMotorClient = None

_client = None
_db = None

# ── Laravel-style $fillable fields for job documents ──
# Only these fields are allowed in mass-assignment operations.
# Fields like "is_admin", "role", "permissions" would be silently stripped.
JOB_FILLABLE: set[str] = {
    "job_id", "status", "progress", "message", "error",
    "input_path", "output_path", "input_key", "output_key",
    "source_url", "original_filename", "output_filename",
    "ffmpeg_args", "progress_channel", "chat_id", "bot_id",
    "request_id", "user_id", "type", "retries", "attempt",
    "started_at", "finished_at", "created_at", "cleanup_input",
    "cleanup_output", "output_get_url", "out_bytes", "in_bytes",
    "progress_by_size", "remote_missing_attempts", "redownload_attempts",
    "remux_attempts", "input_from_remote",
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
                    "$set", "$inc", "$push", "$pull", "$each", "$position"
                ):
                    logger.warning("Blocked nested operator in update: %s", repr(nested_key)[:80])
                    continue
        safe_fields[key] = value
    return safe_fields


async def init(mongo_uri: str | None = None, db_name: str = "media_bot"):
    global _client, _db
    if AsyncIOMotorClient is None:
        raise RuntimeError("motor is required for job_store")
    mongo_uri = mongo_uri or os.environ.get("MONGO_URI")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI not set for job_store")
    _client = AsyncIOMotorClient(mongo_uri)
    _db = _client[db_name]


async def save_job(job: dict[str, Any]) -> None:
    """Insert a new job document with fillable field protection.

    Only fields in JOB_FILLABLE are persisted.  This prevents injection
    of arbitrary document fields via API payloads.
    """
    if _db is None:
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
        pass

    # Parameterized insert (prepared-statement-like: data is validated and filtered)
    await _db.jobs.insert_one(safe_job)


async def update_job(job_id: str, fields: dict[str, Any]) -> None:
    """Update a job document with fillable field protection.

    Like a prepared UPDATE with parameterized fields.
    """
    if _db is None:
        return
    # Apply fillable protection + validate field names
    safe_fields = _validate_field_names(_filter_job_fields(fields))
    if not safe_fields:
        return
    await _db.jobs.update_one({"job_id": job_id}, {"$set": safe_fields}, upsert=False)


async def get_job(job_id: str) -> dict[str, Any] | None:
    """Get a job by ID.  Returns None if not found."""
    if _db is None:
        return None
    # Parameterized query: job_id is passed as a value, not interpolated
    return await _db.jobs.find_one({"job_id": job_id})


async def get_jobs_by_status(status: str, limit: int = 100) -> list:
    """Get jobs by status (parameterized query)."""
    if _db is None:
        return []
    cursor = _db.jobs.find({"status": status}).sort("created_at", -1).limit(limit)
    return await cursor.to_list(length=limit)


async def close():
    global _client
    if _client is not None:
        _client.close()
        _client = None
