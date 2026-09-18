"""Constant-time comparison helpers for credentials.

Why this module exists
----------------------
Every token check in this project compares an incoming value with a configured
secret. Writing that as ``incoming != SECRET`` leaks information: ``str.__eq__``
returns as soon as it finds a differing byte, so the response time depends on how
many leading bytes were correct. Over many requests that is enough to recover a
token byte by byte, and it also short-circuits on length.

``hmac.compare_digest`` compares in time independent of the contents (and of the
length, other than the final length check), which is what a token comparison
needs. Using one shared helper keeps the reasoning in one place instead of
re-deriving it at every call site.

Usage::

    from utils.secure_compare import constant_time_eq

    if not constant_time_eq(incoming_token, UPLOAD_SECRET):
        return jsonify({"error": "unauthorized"}), 401
"""

from __future__ import annotations

import hmac

__all__ = ["constant_time_eq"]


def constant_time_eq(candidate: object, expected: object) -> bool:
    """Return True when both credentials are present and identical.

    A missing value on either side is always a failure — an unset secret must
    never authenticate anything (fail closed). Non-ASCII values are compared as
    their UTF-8 bytes so the function never raises on odd input from a request.
    """
    if not candidate or not expected:
        return False
    try:
        return hmac.compare_digest(str(candidate).encode("utf-8"), str(expected).encode("utf-8"))
    except (TypeError, ValueError, UnicodeError):
        return False
