"""Shared token policy for the public web job API.

Every route in ``web/webapp.py`` and ``web/ws_fastapi.py`` that can enqueue work,
hand out a presigned S3 URL, read job output or dump diagnostics is gated on one
of three credentials:

* ``UPLOAD_SECRET`` — the job API (upload, presign, enqueue_from_url, status,
  download, events, WebSocket/SSE).
* ``DIAG_TOKEN`` — the diagnostics surface.
* ``DEBUG_SECRET`` — the log-dump surface.

The original implementation of each route was written as::

    if upload_secret:
        if incoming != upload_secret:
            return unauthorized

which **fails open**: with the variable unset — a fresh deploy, a renamed
platform variable, a typo — every one of those routes silently becomes public.
The checks here invert that default: an unset secret refuses the request.

An instance that deliberately wants a public job API (a throwaway demo, a local
playground) opts in explicitly with ``ALLOW_UNAUTHENTICATED_WEB=1``. Opting in is
a decision someone has to make on purpose and it is logged, instead of being the
accidental result of a missing variable.

Usage::

    from utils.web_auth import upload_token_ok

    if not upload_token_ok(incoming_token):
        return jsonify({"error": "unauthorized"}), 401
"""

from __future__ import annotations

import logging
import os

from utils.secure_compare import constant_time_eq

__all__ = [
    "allow_unauthenticated_web",
    "upload_token_ok",
    "diag_token_ok",
    "debug_token_ok",
    "missing_web_tokens",
]

logger = logging.getLogger(__name__)

_TRUTHY = {"1", "true", "yes", "on"}

# A missing token is a configuration fact, not a per-request event: warn once per
# token instead of on every request that hits the fail-closed branch.
_WARNED: set[str] = set()


def _warn_once(key: str, message: str, *args: object) -> None:
    if key in _WARNED:
        return
    _WARNED.add(key)
    logger.warning(message, *args)


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def allow_unauthenticated_web() -> bool:
    """True only when the operator explicitly opted into an open job API."""
    return _env("ALLOW_UNAUTHENTICATED_WEB").lower() in _TRUTHY


def missing_web_tokens() -> list[str]:
    """Configured-but-empty tokens, for a single startup warning."""
    return [name for name in ("UPLOAD_SECRET", "DIAG_TOKEN", "DEBUG_SECRET") if not _env(name)]


def _ok(name: str, incoming: object, *, opt_out: bool) -> bool:
    expected = _env(name)
    if not expected:
        if opt_out and allow_unauthenticated_web():
            _warn_once(
                f"{name}:optout",
                "%s is not set — requests are accepted because ALLOW_UNAUTHENTICATED_WEB=1",
                name,
            )
            return True
        _warn_once(f"{name}:closed", "%s is not configured — refusing requests (fail closed)", name)
        return False
    return constant_time_eq(incoming, expected)


def upload_token_ok(incoming: object) -> bool:
    """Authorize the job API. Unset ``UPLOAD_SECRET`` refuses unless opted out."""
    return _ok("UPLOAD_SECRET", incoming, opt_out=True)


def diag_token_ok(incoming: object) -> bool:
    """Authorize diagnostics. Fails closed; there is no public-diagnostics mode."""
    return _ok("DIAG_TOKEN", incoming, opt_out=False)


def debug_token_ok(incoming: object) -> bool:
    """Authorize the log-dump surface, accepting ``DEBUG_SECRET`` or ``DIAG_TOKEN``."""
    return _ok("DEBUG_SECRET", incoming, opt_out=False) or diag_token_ok(incoming)
