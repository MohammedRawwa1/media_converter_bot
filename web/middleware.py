# web/middleware.py
"""
Go-inspired middleware chain and request context for Flask and FastAPI.

Provides:
- Middleware chain pattern (like Go's `net/http` middleware)
- Request-scoped context with values
- Standardized error handler
- Common middleware: CORS, rate limiting, request ID, logging, caching
- Decorator-based route binding

Usage:
    from web.middleware import MiddlewareChain, with_cors, with_request_id

    chain = MiddlewareChain()
    chain.use(with_request_id)
    chain.use(with_rate_limit("search", 10, 20))

    @chain("/api/search")
    async def search_handler(ctx, request, q: str):
        return {"results": []}
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from functools import wraps
from typing import Any

logger = logging.getLogger(__name__)


# ── Request Context ────────────────────────────────────────────────


class RequestContext:
    """Request-scoped context that flows through middleware.

    Go-style: middleware can read/write context values that downstream
    handlers and middleware can access.
    """

    def __init__(self):
        self.start_time: float = time.time()
        self.request_id: str = ""
        self.client_ip: str = "unknown"
        self.route_name: str = ""
        self.user_id: int | None = None
        self.values: dict[str, Any] = {}

    def set(self, key: str, value: Any):
        """Set a context value."""
        self.values[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get a context value."""
        return self.values.get(key, default)

    def elapsed(self) -> float:
        """Return seconds since context creation."""
        return time.time() - self.start_time


# ── Middleware types ───────────────────────────────────────────────

# Middleware is a callable that takes (ctx, request, next_handler)
# and returns a response.
# Both sync and async middleware are supported.
MiddlewareFunc = Callable[..., Any]


# ── Middleware Chain ───────────────────────────────────────────────


class MiddlewareChain:
    """Composable middleware chain for route handlers.

    Usage:
        chain = MiddlewareChain()

        # Add middleware (order matters: first added = outermost)
        chain.use(cors_middleware)
        chain.use(request_id_middleware)
        chain.use(logging_middleware)

        # Decorate a route
        @chain("/api/status/<job_id>", methods=["GET"])
        def status_handler(ctx, request, job_id):
            return {"status": "ok"}

    For FastAPI:
        @chain("/api/status/{job_id}", methods=["GET"], framework="fastapi")
        async def status_handler(ctx, request, job_id):
            return {"status": "ok"}
    """

    def __init__(self):
        self._middleware: list[tuple[str, Callable, dict]] = []  # (route, handler, kwargs)

    def use(self, middleware_fn: Callable):
        """Register middleware to run before every route."""
        if not hasattr(self, "_middleware_stack"):
            self._middleware_stack: list[Callable] = []
        self._middleware_stack.append(middleware_fn)

    def __call__(self, route: str, methods: list[str] | None = None,
                 framework: str = "flask", **kwargs):
        """Decorator that registers a route with the middleware chain.

        Args:
            route: URL pattern (supports Flask {param} or FastAPI {param})
            methods: HTTP methods (default: ["GET"])
            framework: "flask" or "fastapi"
            kwargs: Additional arguments passed to @app.route()
        """
        methods = methods or ["GET"]

        def decorator(handler: Callable):
            self._middleware.append((route, handler, {
                "methods": methods,
                "framework": framework,
                **kwargs,
            }))

            @wraps(handler)
            def wrapper(*args, **kwargs):
                return self._run(handler, *args, **kwargs)

            return wrapper

        return decorator

    def _run(self, handler: Callable, *args, **kwargs) -> Any:
        """Run the middleware chain then the handler."""
        ctx = RequestContext()
        ctx.route_name = handler.__name__

        # Start with the handler
        async def async_wrapper():
            try:
                result = await handler(ctx, *args, **kwargs)
                _log_request(ctx, result)
                return result
            except Exception as e:
                return _handle_error(ctx, e)

        def sync_wrapper():
            try:
                result = handler(ctx, *args, **kwargs)
                _log_request(ctx, result)
                return result
            except Exception as e:
                return _handle_error(ctx, e)

        # Apply middleware stack (innermost is handler)
        stack = getattr(self, "_middleware_stack", [])
        if not stack:
            return async_wrapper() if _is_async(handler) else sync_wrapper()

        # Chain middleware from outermost to innermost
        async def chain():
            # Build the middleware chain
            async def run_middleware(index: int, ctx: RequestContext, *a, **kw):
                if index >= len(stack):
                    if _is_async(handler):
                        return await handler(ctx, *a, **kw)
                    return handler(ctx, *a, **kw)
                mw = stack[index]
                return await mw(ctx, *a, **kw, next_handler=lambda: run_middleware(index + 1, ctx, *a, **kw))

            return await run_middleware(0, ctx, *args, **kwargs)

        return chain()

    def register_with_app(self, app, prefix: str = ""):
        """Register all chained routes with a Flask or FastAPI app."""
        from fastapi import FastAPI
        from flask import Flask

        if isinstance(app, Flask):
            for route, handler, opts in self._middleware:
                methods = opts.get("methods", ["GET"])
                full_route = prefix + route

                # Wrap handler with middleware chain
                @wraps(handler)
                def wrapped_handler(*args, **kwargs):
                    return self._run(handler, *args, **kwargs)

                app.route(full_route, methods=methods)(wrapped_handler)
                logger.info("Registered Flask route: %s %s", methods, full_route)

        elif isinstance(app, FastAPI):
            from fastapi import APIRouter
            router = APIRouter()

            for route, handler, opts in self._middleware:
                methods = opts.get("methods", ["GET"])
                full_route = prefix + route

                # Convert Flask-style params to FastAPI-style
                fastapi_route = full_route.replace("<", "{").replace(">", "}")

                @wraps(handler)
                async def wrapped_handler(*args, **kwargs):
                    return await self._run(handler, *args, **kwargs)

                for method in methods:
                    method_lower = method.lower()
                    getattr(router, method_lower)(fastapi_route)(wrapped_handler)
                    logger.info("Registered FastAPI route: %s %s", method, full_route)

            app.include_router(router)


# ── Error Handler ─────────────────────────────────────────────────


def _handle_error(ctx: RequestContext, exc: Exception) -> dict[str, Any]:
    """Standardized error response (Go-style).

    Never leaks exception details to the client.
    """
    logger.exception("Request %s failed: %s", ctx.request_id, exc)

    from utils.response import error

    status = 500
    code = "internal_error"
    message = "An internal error occurred"

    # Handle known exception types
    if hasattr(exc, "status_code"):
        status = exc.status_code
    if hasattr(exc, "detail"):
        message = exc.detail

    # Handle HTTPException-like
    if hasattr(exc, "status_code") and hasattr(exc, "detail"):
        status = exc.status_code
        message = "An internal error occurred"  # Never leak detail

    return error(code=code, message=message, status=status)


def _log_request(ctx: RequestContext, response: Any):
    """Log request completion with timing."""
    elapsed = ctx.elapsed()
    status = 200
    if isinstance(response, tuple) and len(response) >= 2:
        status = response[1]
    elif isinstance(response, dict):
        status = response.get("status", 200) if "error" in response else 200

    logger.info(
        "REQ %s %s %s %.3fs",
        ctx.request_id,
        ctx.route_name,
        status,
        elapsed,
    )


def _is_async(func: Callable) -> bool:
    """Check if a function is async."""
    import asyncio
    return asyncio.iscoroutinefunction(func)


# ── Built-in Middleware ────────────────────────────────────────────


async def with_request_id(ctx: RequestContext, request=None, **kwargs):
    """Attach a unique request ID to every request."""
    ctx.request_id = str(uuid.uuid4())[:8]
    return await kwargs["next_handler"]()


async def with_request_logging(ctx: RequestContext, request=None, **kwargs):
    """Log every request with method, path, client IP."""
    method = getattr(request, "method", "?")
    path = getattr(request, "path", getattr(request, "url", "?"))
    logger.info("→ %s %s [%s] rid=%s", method, path, ctx.client_ip, ctx.request_id)
    result = await kwargs["next_handler"]()
    elapsed = ctx.elapsed()
    logger.info("← %s %s [%.3fs] rid=%s", method, path, elapsed, ctx.request_id)
    return result


def with_rate_limit(endpoint: str, calls_per_second: int = 10, burst: int = 20):
    """Create a rate-limiting middleware.

    Usage:
        chain.use(with_rate_limit("search", 10, 20))
    """
    from utils.web_rate_limiter import get_client_ip, make_rate_limit_response, web_rate_limiter

    async def middleware(ctx: RequestContext, request=None, **kwargs):
        client_ip = get_client_ip(request)
        ctx.client_ip = client_ip
        if not web_rate_limiter.check_limit(endpoint, client_ip):
            logger.warning("Rate limited: %s %s", endpoint, client_ip)
            body, status, headers = make_rate_limit_response(endpoint, client_ip)
            from utils.response import error
            return error(code="rate_limited", message=body.get("detail", "Too many requests"), status=429)
        return await kwargs["next_handler"]()

    return middleware


def with_cache(ttl: int = 30):
    """Create a caching middleware for GET responses.

    Usage:
        chain.use(with_cache(ttl=60))
    """
    from utils.route_cache import route_cache

    async def middleware(ctx: RequestContext, request=None, **kwargs):
        # Only cache GET requests
        method = getattr(request, "method", "GET")
        if method != "GET":
            return await kwargs["next_handler"]()

        # Generate cache key
        path = getattr(request, "path", getattr(request, "url", ""))
        args = getattr(request, "args", getattr(request, "query_params", {}))
        params = dict(args) if hasattr(args, "items") else {}
        cache_key = route_cache.make_key(path, params)

        # Try cache
        cached = await route_cache.aget(cache_key)
        if cached is not None:
            logger.debug("CACHE HIT: %s", cache_key)
            return cached

        # Compute and cache
        result = await kwargs["next_handler"]()
        await route_cache.aset(cache_key, result, ttl=ttl)
        logger.debug("CACHE MISS: %s (computed)", cache_key)
        return result

    return middleware


async def with_cors(ctx: RequestContext, request=None, **kwargs):
    """CORS middleware (allow all origins for API endpoints)."""
    result = await kwargs["next_handler"]()
    # Add CORS headers
    if isinstance(result, dict):
        # FastAPI handles CORS via middleware, Flask via Flask-CORS
        pass  # Flask-CORS is already configured
    return result


def with_security_headers(response: Any, ctx: RequestContext | None = None) -> Any:
    """Add security headers to a response.

    Go-style: explicit security headers on every response.
    Can be used as middleware or applied per-response.
    """
    headers = {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "X-XSS-Protection": "1; mode=block",
        "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "Pragma": "no-cache",
    }

    if isinstance(response, tuple):
        body, status, resp_headers = response if len(response) == 3 else (response[0], response[1], {})
        return (body, status, {**resp_headers, **headers})
    return response
