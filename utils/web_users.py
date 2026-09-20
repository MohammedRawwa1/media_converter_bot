"""Per-user web tokens: who a web caller is, not just that the service let them in.

``UPLOAD_SECRET`` (utils/web_auth.py) is one shared secret handed to every user
of the web uploader. It answers "is this a caller the service trusts?" but not
"which user is this?", so a job queued over the web carried no owner and a second
user could not be told apart from the first. A revoked or leaked secret is also
all-or-nothing: there is no way to cut off one user without cutting off everyone.

This module issues a token *per user id* - the same id the bot already keys
settings and sessions by - so a web request can be attributed to a user and the
jobs it creates can carry that owner. Only the digest of a token is stored (a
Redis dump does not yield a usable credential), the plaintext is returned exactly
once to whoever issues it, and verification compares digests in constant time.

Storage mirrors utils/job_access.py: Redis is the shared source of truth across
processes, and a bounded, TTL'd in-process mirror keeps a single process working
when Redis is absent (tests, a local Flask with no broker).

Usage::

    from utils.web_users import issue_user_token, resolve_user_token

    token = await issue_user_token(telegram_user_id)   # once, show it to the user
    user_id = await resolve_user_token(presented)      # None when not recognised
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import secrets
import threading
import time
from dataclasses import dataclass

from utils.secure_compare import constant_time_eq

__all__ = [
    "USER_TOKEN_PARAM",
    "issue_user_token",
    "revoke_user_token",
    "resolve_user_token",
    "user_token_digest",
    "user_token_ok",
]

logger = logging.getLogger(__name__)

# Wire name of the credential: a header (``X-User-Token``) or a form/JSON field.
# Named for the capability, not as a stored secret constant.
USER_TOKEN_PARAM = "user_token"  # noqa: S105  # the wire name of a query field, not a credential

# Redis keys. The per-user hash holds the digest; the index maps a digest back to
# its user so a presented token can be resolved without scanning every user.
_USER_KEY = "ffmpeg:web_user:{}"
_USER_HASH_FIELD = "token_hash"
_USER_MINTED_FIELD = "minted_at"
_USER_INDEX = "ffmpeg:web_user_tokens"

# In-process mirror of digests this process knows, bounded exactly like
# job_access's cache so a long-lived process cannot grow it without limit.
_MAX_LOCAL = 5000
_LOCAL_TTL_SECONDS = 86400.0


@dataclass(frozen=True)
class _KnownToken:
    user_id: int
    digest: str
    minted_at: float


_local_by_digest: dict[str, _KnownToken] = {}
_local_lock = threading.Lock()


def user_token_digest(token: object) -> str:
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
        for stale in [d for d, known in _local_by_digest.items() if known.minted_at < cutoff]:
            _local_by_digest.pop(stale, None)
    while len(_local_by_digest) > _MAX_LOCAL:
        oldest = min(_local_by_digest.items(), key=lambda kv: kv[1].minted_at)[0]
        _local_by_digest.pop(oldest, None)


def _remember(user_id: int, digest: str) -> None:
    """Mirror a digest this process just minted or resolved."""
    if not digest:
        return
    now = time.time()
    with _local_lock:
        _local_by_digest[digest] = _KnownToken(user_id=int(user_id), digest=digest, minted_at=now)
        _prune_locked(now)


def _local_user_for(digest: str) -> int | None:
    with _local_lock:
        known = _local_by_digest.get(digest)
    return known.user_id if known else None


def _local_digest_for(user_id: int) -> str:
    with _local_lock:
        for known in _local_by_digest.values():
            if known.user_id == int(user_id):
                return known.digest
    return ""


def _forget_user(user_id: int) -> bool:
    """Drop every local digest belonging to ``user_id``. Returns True if any."""
    removed = False
    with _local_lock:
        for digest in [d for d, known in _local_by_digest.items() if known.user_id == int(user_id)]:
            _local_by_digest.pop(digest, None)
            removed = True
    return removed


async def _redis():
    """The shared async Redis client, or None when unavailable."""
    try:
        from utils.job_queue import get_redis

        return await get_redis()
    except Exception:
        logger.debug("web_users: Redis unavailable; the token stays process-local")
        return None


async def _close(r) -> None:
    with contextlib.suppress(Exception):
        aclose = getattr(r, "aclose", None)
        if aclose is not None:
            await aclose()
        else:
            await r.close()


async def issue_user_token(user_id: int) -> str:
    """Mint (or rotate) a web token for ``user_id`` and return the plaintext once.

    A new token always replaces the old one - both in storage and in the digest
    index - so issuing again is how a user rotates a token they think leaked, and
    the previous value stops working immediately. Returns ``""`` for a missing id.
    """
    if user_id is None:
        return ""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return ""

    token = secrets.token_urlsafe(32)
    digest = user_token_digest(token)
    # Retire the previous token locally *before* remembering the new one, so a
    # process without Redis cannot keep resolving the rotated-out value.
    _forget_user(uid)
    _remember(uid, digest)

    r = await _redis()
    if r is None:
        return token
    try:
        previous = await r.hget(_USER_KEY.format(uid), _USER_HASH_FIELD)
        previous = previous.decode() if isinstance(previous, (bytes, bytearray)) else previous
        await r.hset(
            _USER_KEY.format(uid),
            mapping={
                _USER_HASH_FIELD: digest,
                _USER_MINTED_FIELD: str(int(time.time())),
            },
        )
        # Retire the old digest from the reverse index in the same round trip
        # group, so an old token cannot keep resolving after a rotation.
        if previous and previous != digest:
            await r.hdel(_USER_INDEX, previous)
        await r.hset(_USER_INDEX, digest, str(uid))
    except Exception:
        logger.debug("web_users: failed to persist a token for user %s", uid)
    finally:
        await _close(r)
    return token


async def revoke_user_token(user_id: int) -> bool:
    """Remove ``user_id``'s token. Returns True when one was there to remove."""
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False

    removed = _forget_user(uid)

    r = await _redis()
    if r is None:
        return removed
    try:
        key = _USER_KEY.format(uid)
        previous = await r.hget(key, _USER_HASH_FIELD)
        previous = previous.decode() if isinstance(previous, (bytes, bytearray)) else previous
        if previous:
            removed = True
            await r.hdel(_USER_INDEX, previous)
        await r.delete(key)
    except Exception:
        logger.debug("web_users: failed to revoke the token for user %s", uid)
    finally:
        await _close(r)
    return removed


async def resolve_user_token(incoming: object) -> int | None:
    """The user id a presented token belongs to, or ``None`` when unrecognised.

    Fails closed: an empty token, an unknown digest, or an unresolvable id all
    return ``None`` rather than a default user.
    """
    digest = user_token_digest(incoming)
    if not digest:
        return None

    r = await _redis()
    if r is not None:
        try:
            raw = await r.hget(_USER_INDEX, digest)
            if raw is not None:
                value = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
                try:
                    uid = int(value)
                except (TypeError, ValueError):
                    uid = None
                if uid is not None:
                    _remember(uid, digest)
                    return uid
        except Exception:
            logger.debug("web_users: token lookup failed; falling back to the local mirror")
        finally:
            await _close(r)

    return _local_user_for(digest)


async def user_token_ok(user_id: int, incoming: object) -> bool:
    """True when ``incoming`` is exactly the token stored for ``user_id``.

    Used when the caller states the user id it claims, so a token that resolves
    to a *different* user cannot be accepted for this one. When Redis is
    unreachable the local mirror is the only record; a process that never minted
    the token refuses, rather than allow.
    """
    digest = user_token_digest(incoming)
    if not digest:
        return False

    stored = ""
    r = await _redis()
    if r is not None:
        try:
            raw = await r.hget(_USER_KEY.format(int(user_id)), _USER_HASH_FIELD)
            if raw:
                stored = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
        except Exception:
            stored = ""
        finally:
            await _close(r)

    if not stored:
        stored = _local_digest_for(int(user_id))
    if not stored:
        return False
    return constant_time_eq(digest, stored)
