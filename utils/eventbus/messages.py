"""Event envelope + job payload projection for the Kafka event log.

The envelope is deliberately small and versioned. Two rules matter:

* **Events carry a projection of the job, never the job itself.** A queued job
  dict can contain signed URLs, session strings, S3 keys and local paths;
  ``job_summary`` whitelists the fields that belong in an event log.
* **Event ids and timestamps are injectable** so the envelope can be unit tested
  without patching the clock.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid

EVENT_VERSION = 1

JOB_QUEUED = "job.queued"
JOB_STARTED = "job.started"
JOB_PROGRESS = "job.progress"
JOB_COMPLETED = "job.completed"
JOB_FAILED = "job.failed"
JOB_CANCELLED = "job.cancelled"

# Operational event, not a job lifecycle one: a single instance is written at
# startup to prove the log accepts writes (see ``verify_events_startup``). It
# carries no job id, so per-job readers never see it.
EVENTS_PROBE = "eventbus.probe"

TERMINAL_EVENTS = frozenset({JOB_COMPLETED, JOB_FAILED, JOB_CANCELLED})

KNOWN_EVENTS = frozenset({JOB_QUEUED, JOB_STARTED, JOB_PROGRESS, JOB_COMPLETED, JOB_FAILED, JOB_CANCELLED})

# Job fields that are safe to publish. Everything else stays in Redis/Mongo.
_SAFE_JOB_FIELDS = (
    "job_id",
    "request_id",
    "type",
    "status",
    "chat_id",
    "user_id",
    "output_ext",
    "size_bytes",
    "file_size",
    "duration",
    "attempt",
    "source",
)

# Keys that must never appear in an event payload, whatever the caller passes.
# ".items()"-style markers are substring matches, so they are chosen to catch a
# credential by name without swallowing ordinary fields: "url" and "presign" are
# here because a signed URL is a credential with an expiry date, and it is the
# most likely thing to end up in a job's extras.
_SECRET_MARKERS = (
    "secret",
    "token",
    "password",
    "passwd",
    "api_key",
    "apikey",
    "session",
    "credential",
    "signature",
    "presign",
    "url",
    "auth",
)


class EventDecodeError(ValueError):
    """Raised when a raw Kafka message is not a usable event envelope."""


def new_event(
    event_type: str,
    *,
    job_id: str = "",
    request_id: str = "",
    source: str = "",
    payload: dict | None = None,
    event_id: str | None = None,
    ts: float | None = None,
) -> dict:
    """Build an event envelope.

    ``event_id``/``ts`` are injectable for deterministic tests.
    """
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "type": str(event_type),
        "version": EVENT_VERSION,
        "ts": float(time.time() if ts is None else ts),
        "job_id": str(job_id or ""),
        "request_id": str(request_id or ""),
        "source": str(source or ""),
        "payload": dict(payload or {}),
    }


def encode(event: dict) -> bytes:
    """Encode an envelope for the wire (compact UTF-8 JSON)."""
    return json.dumps(event, separators=(",", ":"), ensure_ascii=False, sort_keys=True).encode("utf-8")


def decode(raw: bytes | str | None) -> dict:
    """Decode and validate an envelope, raising :class:`EventDecodeError` if unusable."""
    if raw is None:
        raise EventDecodeError("empty message")
    try:
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        event = json.loads(text)
    except Exception as exc:
        raise EventDecodeError(f"not valid JSON: {exc}") from exc
    if not isinstance(event, dict):
        raise EventDecodeError("envelope is not an object")
    for field in ("event_id", "type", "version", "ts"):
        if field not in event:
            raise EventDecodeError(f"missing field {field!r}")
    if not event.get("event_id") or not event.get("type"):
        raise EventDecodeError("empty event_id or type")
    try:
        event["ts"] = float(event["ts"])
        event["version"] = int(event["version"])
    except Exception as exc:
        raise EventDecodeError(f"bad ts/version: {exc}") from exc
    if not isinstance(event.get("payload"), dict):
        raise EventDecodeError("payload is not an object")
    event.setdefault("job_id", "")
    event.setdefault("request_id", "")
    event.setdefault("source", "")
    return event


def job_bucket(job_id: str, buckets: int = 100) -> int:
    """Map a job id to a stable bucket in ``0..buckets-1``.

    Uses SHA-256 rather than ``hash()`` because the decision has to be identical
    in every process (producers and consumers run in separate services, and
    ``hash()`` is salted per process).
    """
    digest = hashlib.sha256(str(job_id).encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % max(1, int(buckets))


def is_terminal(event_type: str) -> bool:
    """True when the event ends a job's lifecycle."""
    return str(event_type) in TERMINAL_EVENTS


def _is_secret_key(key: str) -> bool:
    lowered = str(key).lower()
    return any(marker in lowered for marker in _SECRET_MARKERS)


def job_summary(job: dict | None) -> dict:
    """Return the publishable projection of a job dict (whitelist + secret filter)."""
    summary: dict = {}
    if not isinstance(job, dict):
        return summary
    for field in _SAFE_JOB_FIELDS:
        if field not in job:
            continue
        value = job.get(field)
        if value is None or isinstance(value, (str, int, float, bool)):
            summary[field] = value
        else:
            # Anything structured is summarised, not forwarded: events are a
            # log, not a data transfer.
            try:
                summary[field] = str(value)
            except Exception:
                continue
    # Belt and braces: a whitelisted field name can never be a secret, but the
    # caller-supplied extras below could be.
    extras = job.get("extra") if isinstance(job.get("extra"), dict) else {}
    for key, value in (extras or {}).items():
        if _is_secret_key(key):
            continue
        summary[f"extra.{key}"] = value if isinstance(value, (str, int, float, bool)) else str(value)
    return summary
