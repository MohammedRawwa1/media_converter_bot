# utils/response.py
"""
Go-inspired standardized API response format.

Provides consistent JSON response structures across all endpoints.

Go-style patterns:
- Every response has a consistent envelope
- Errors are structured (code + message + details)
- Pagination is standardized
- Timestamps use ISO 8601

Usage:
    from utils.response import ok, error, paginated

    # Success
    return ok({"job_id": "abc", "status": "done"})

    # Error
    return error("not_found", "Job not found", status=404)

    # Paginated
    return paginated(results, total=100, page=1, per_page=10)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


def ok(
    data: Any = None,
    message: str = "success",
    status: int = 200,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any] | tuple:
    """Standard success response.

    Returns a dict for FastAPI auto-serialization, or a (dict, status)
    tuple that Flask's jsonify can convert.

    Go-style: consistent envelope with 'ok: true'
    """
    body: dict[str, Any] = {
        "ok": True,
        "message": message,
        "data": data,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    if meta:
        body["meta"] = meta

    return body


def error(
    code: str = "internal_error",
    message: str = "An internal error occurred",
    status: int = 500,
    details: Any | None = None,
) -> dict[str, Any] | tuple:
    """Standard error response.

    Go-style: structured error with code and message, never leaks internals.

    Examples:
        error("not_found", "Job not found", status=404)
        error("rate_limited", "Too many requests", status=429)
        error("validation_error", "Invalid input", status=400, details=field_errors)
    """
    body: dict[str, Any] = {
        "ok": False,
        "error": {
            "code": code,
            "message": message,
            "status": status,
        },
        "data": None,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    if details:
        body["error"]["details"] = details

    return body


def paginated(
    items: list[Any],
    total: int,
    page: int = 1,
    per_page: int = 10,
    message: str = "success",
) -> dict[str, Any] | tuple:
    """Standard paginated response.

    Go-style: consistent pagination metadata.
    """
    total_pages = max(1, (total + per_page - 1) // per_page)
    body: dict[str, Any] = {
        "ok": True,
        "message": message,
        "data": items,
        "pagination": {
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1,
        },
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    return body


def rate_limited(retry_after: int = 1) -> dict[str, Any] | tuple:
    """Standard 429 rate limit response.

    Includes Retry-After header guidance.
    """
    return error(
        code="rate_limited",
        message=f"Too many requests. Retry after {retry_after} seconds.",
        status=429,
    )


# ── Helper to create JSON response for different frameworks ───────────


def json_response(
    body: dict[str, Any] | tuple,
    status: int = 200,
    headers: dict[str, str] | None = None,
    framework: str = "auto",
) -> Any:
    """Create a framework-appropriate JSON response.

    For Flask routes, returns a (dict, status, headers) tuple.
    For FastAPI routes, returns a JSONResponse.

    Pass ``framework="flask"`` or ``framework="fastapi"`` explicitly for
    deterministic behavior.  When ``framework="auto"`` (default), the
    function tries to detect the active framework by checking ``sys.modules``.

    Note:
        Auto-detection via ``sys.modules`` is best-effort and can produce
        incorrect results when both frameworks are installed but only one
        is active.  Prefer the explicit ``framework=`` parameter.
    """
    if framework == "auto":
        import sys
        if "flask" in sys.modules:
            try:
                from flask import current_app
                if current_app:
                    framework = "flask"
            except Exception:
                pass
        if framework == "auto" and "fastapi" in sys.modules:
            framework = "fastapi"

    if framework == "flask":
        try:
            from flask import jsonify
            resp = jsonify(body)
            resp.status_code = status
            if headers:
                resp.headers.update(headers)
            return resp
        except Exception:
            pass

    if framework == "fastapi":
        try:
            from fastapi.responses import JSONResponse
            return JSONResponse(content=body, status_code=status, headers=headers)
        except Exception:
            pass

    # Fallback: plain tuple
    return body, status, headers
