"""Per-job access capabilities for the web job API.

Why this module exists
----------------------
``UPLOAD_SECRET`` is one shared secret handed to every user of the web uploader,
so it authorizes the *service* but says nothing about *which job* a caller may
read. The job-scoped read routes (``/status``, ``/download``, ``/events``,
``/ws``) took the job id from the URL and looked the job up by it, which made
possession of the id the entire authorization check - a classic IDOR latent in
any leak of a job id (a referrer header, a shared link, a log line).

Each job now carries its own capability token. The plaintext is returned exactly
once, to whoever created the job, and the job hash stores only a SHA-256 digest
of it, so a Redis dump does not yield a usable token. Real-time endpoints
(WebSocket, EventSource) cannot set request headers from a browser, which is why
the capability travels in the query string: it is per-job, unguessable and
short-lived, so it behaves like a signed URL rather than a long-lived secret.

Fail closed: a job with no recorded capability refuses every caller, so a job
created by a path that never minted one is not silently world-readable.

Usage::

    from utils.job_access import issue_job_token, job_token_ok, JOB_CAPABILITY_PARAM

    token = await issue_job_token(job_id)          # once, at job creation
    if not await job_token_ok(job_id, incoming):   # on every job-scoped read
        return unauthorized
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import secrets
import threading
import time

from utils.secure_compare import constant_time_eq

__all__ = [
    "JOB_ACCESS_FIELD",
    "JOB_CAPABILITY_PARAM",
    "issue_job_token",
    "job_token_digest",
    "job_token_ok",
    "remember_job_token",
]

logger = logging.getLogger(__name__)

# Hash field on ``ffmpeg:job:<id>`` holding the capability digest.
JOB_ACCESS_FIELD = "access_token_hash"
# Query parameter / form field carrying the plaintext capability. Named for the
# capability rather than as a "token" constant so the name reads as what it is:
# the wire name of a query parameter, not a credential stored in the source.
JOB_CAPABILITY_PARAM = "job_token"

# In-process mirror of the digests, plus the plaintext of tokens this process
# minted. The web app mounts the Flask UI and the FastAPI app in one process, and
# the bot mints tokens for jobs it created, so a local copy keeps a single job
# from minting a fresh token on every progress edit. Redis is still the shared
# source of truth across processes and across uvinstances; this is a bound, TTL'd
# cache (same rationale as WebRateLimiter.buckets).
_MAX_LOCAL = 5000
_LOCAL_TTL_SECONDS = 86400.0
_local: dict[str, tuple[str, str, float]] = {}  # job_id -> (plaintext, digest, minted_at)
_local_lock = threading.Lock()


def job_token_digest(token: object) -> str:
    """Return the SHA-256 hex digest stored for a token. Empty for no token."""
    if not token:
        return ""
    try:
        return hashlib.sha256(str(token).encode("utf-8")).hexdigest()
    except (TypeError, ValueError):
        return ""


def _prune_locked(now: float) -> None:
    """Drop aged entries, then enforce the hard cap. Caller holds ``_local_lock``."""
    if _LOCAL_TTL_SECONDS > 0:
        cutoff = now - _LOCAL_TTL_SECONDS
        for stale in [k for k, (_plain, _digest, minted) in _local.items() if minted < cutoff]:
            _local.pop(stale, None)
    while len(_local) > _MAX_LOCAL:
        oldest = min(_local.items(), key=lambda kv: kv[1][2])[0]
        _local.pop(oldest, None)


def remember_job_token(job_id: str, token: object) -> str:
    """Record a capability this process knows the plaintext of.

    Returns the digest, so a caller can put it on the job payload without
    re-deriving it. No-op (returns "") for a missing job id or token.
    """
    digest = job_token_digest(token)
    if not job_id or not digest:
        return ""
    now = time.time()
    with _local_lock:
        _local[job_id] = (str(token), digest, now)
        _prune_locked(now)
    return digest


def _known_digest(job_id: str) -> str:
    with _local_lock:
        entry = _local.get(job_id)
    return entry[1] if entry else ""


def _known_token(job_id: str) -> str:
    with _local_lock:
        entry = _local.get(job_id)
    return entry[0] if entry else ""


async def _redis():
    """The shared async Redis client, or None when unavailable."""
    try:
        from utils.job_queue import get_redis

        return await get_redis()
    except Exception:
        logger.debug("job_access: Redis unavailable; capability stays process-local")
        return None


async def issue_job_token(job_id: str) -> str:
    """Mint a capability for ``job_id``, persist its digest, return the plaintext.

    A token this process already minted for the job is reused, so calling this
    from a progress loop is idempotent instead of invalidating the link that was
    already sent.
    """
    if not job_id:
        return ""
    existing = _known_token(job_id)
    if existing:
        return existing

    token = secrets.token_urlsafe(32)
    digest = remember_job_token(job_id, token)

    r = await _redis()
    if r is None:
        # No Redis (tests, local Flask without a broker). The in-process mirror
        # still authorizes this process, which is where the routes live.
        return token
    try:
        await r.hset(f"ffmpeg:job:{job_id}", JOB_ACCESS_FIELD, digest)
    except Exception:
        logger.debug("job_access: failed to persist capability for job %s", job_id)
    finally:
        with contextlib.suppress(Exception):
            await r.close()
    return token


async def job_token_ok(job_id: str, incoming: object) -> bool:
    """Authorize access to one job. Fails closed when no capability is recorded."""
    digest = job_token_digest(incoming)
    if not job_id or not digest:
        return False

    stored = ""
    r = await _redis()
    if r is not None:
        try:
            raw = await r.hget(f"ffmpeg:job:{job_id}", JOB_ACCESS_FIELD)
            if raw:
                stored = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
        except Exception:
            stored = ""
        finally:
            with contextlib.suppress(Exception):
                await r.close()

    if not stored:
        stored = _known_digest(job_id)
    if not stored:
        return False
    return constant_time_eq(digest, stored)
