"""
Web rate limiter for API endpoints.
Provides sync rate limiting for Flask and FastAPI endpoints
using an in-memory token bucket algorithm per-endpoint per-IP.

Usage:
    from utils.web_rate_limiter import web_rate_limiter, get_client_ip

    # Flask: call at start of route handler
    if not web_rate_limiter.check_limit("upload", get_client_ip()):
        return jsonify({"error": "rate_limited", "detail": "Too many requests"}), 429
"""

import logging
import threading
import time

logger = logging.getLogger(__name__)


class WebRateLimiter:
    """Token-bucket rate limiter shared across all web endpoints.

    Synchronous (thread-safe) implementation for Flask and FastAPI.
    Each endpoint gets its own bucket keyed by (endpoint_name, client_ip).
    """

    def __init__(self):
        self._lock = threading.Lock()
        # buckets: {(endpoint, ip) -> (tokens, last_refill)}
        self.buckets: dict[tuple[str, str], tuple[float, float]] = {}

        # Default rate limits per endpoint (requests per second, burst capacity)
        self.endpoint_limits = {
            # Public endpoints: strict limits
            "status":        (5,   10),    # 5 req/s, burst 10
            "download":      (2,   5),     # 2 req/s, burst 5
            "events":        (5,   15),    # 5 req/s, burst 15 (SSE reconnect)
            "search":        (10,  20),    # 10 req/s, burst 20
            "health":        (10,  30),    # 10 req/s, burst 30
            # Auth-protected endpoints: moderate limits
            "upload":        (3,   10),    # 3 req/s, burst 10
            "presign":       (3,   10),    # 3 req/s, burst 10
            "enqueue_url":   (2,   5),     # 2 req/s, burst 5
            "webhook":       (30,  60),    # 30 req/s (Telegram bursts)
            "get_input":     (2,   5),     # 2 req/s, burst 5
            "diag":          (1,   3),     # 1 req/s (diagnostic)
            "debug_log":     (2,   5),     # 2 req/s, burst 5
            # Default fallback
            "default":       (10,  20),    # 10 req/s, burst 20
        }

    def get_limit(self, endpoint: str) -> tuple[float, float]:
        """Get (calls_per_second, burst_capacity) for an endpoint.

        Public method (no dunder prefix) to avoid Python name-mangling issues
        when called from external functions like make_rate_limit_response().
        """
        return self.endpoint_limits.get(endpoint, self.endpoint_limits["default"])

    def check_limit(self, endpoint: str, client_ip: str = "global") -> bool:
        """Check if request is allowed. Returns True if allowed, False if rate limited."""
        if not client_ip:
            client_ip = "global"

        key = (endpoint, client_ip)
        calls_per_second, burst_capacity = self.get_limit(endpoint)

        with self._lock:
            now = time.time()
            tokens, last_refill = self.buckets.get(
                key, (float(burst_capacity), now)
            )

            # Refill tokens based on elapsed time
            elapsed = now - last_refill
            tokens = min(float(burst_capacity), tokens + (elapsed * calls_per_second))

            if tokens >= 1.0:
                self.buckets[key] = (tokens - 1.0, now)
                return True
            else:
                self.buckets[key] = (tokens, now)
                return False

    def get_retry_after(self, endpoint: str, client_ip: str = "global") -> float:
        """Get seconds until next token is available."""
        key = (endpoint, client_ip or "global")
        calls_per_second, _ = self.get_limit(endpoint)

        with self._lock:
            tokens, last_refill = self.buckets.get(key, (0.0, time.time()))
            if tokens >= 1.0:
                return 0.0
            if calls_per_second <= 0:
                return 1.0
            tokens_needed = 1.0 - tokens
            return tokens_needed / calls_per_second

    def get_stats(self) -> dict:
        """Return rate limiter statistics for monitoring."""
        stats = {}
        with self._lock:
            for (endpoint, ip), (tokens, last_refill) in list(self.buckets.items()):
                key = f"{endpoint}:{ip}"
                stats[key] = {
                    "tokens": round(tokens, 2),
                    "last_refill": round(last_refill, 2),
                    "age_seconds": round(time.time() - last_refill, 2),
                }
        return stats


# Global singleton
web_rate_limiter = WebRateLimiter()


def get_client_ip(request) -> str:
    """Extract client IP from request, respecting proxies.

    Handles None request gracefully (returns 'unknown').
    """
    if request is None:
        return "unknown"

    try:
        # Check X-Forwarded-For first (for reverse proxies)
        forwarded = request.headers.get("X-Forwarded-For", "")
        if forwarded and "," in forwarded:
            return forwarded.split(",")[0].strip()
        if forwarded:
            return forwarded.strip()

        # Check X-Real-IP
        real_ip = request.headers.get("X-Real-IP", "")
        if real_ip:
            return real_ip.strip()

        # Fall back to remote_addr
        if hasattr(request, "remote_addr") and request.remote_addr:
            return request.remote_addr

        # Flask and FastAPI compatibility
        if hasattr(request, "client") and request.client:
            host, port = request.client
            return host

        return "unknown"
    except Exception:
        return "unknown"


def make_rate_limit_response(endpoint: str, client_ip: str) -> tuple[dict, int, dict]:
    """Create a standardized 429 rate limit response with Retry-After header."""
    retry_after = web_rate_limiter.get_retry_after(endpoint, client_ip)
    rate_limit = web_rate_limiter.get_limit(endpoint)[0]
    headers = {
        "Retry-After": str(int(retry_after) + 1),
        "X-RateLimit-Limit": str(rate_limit),
        "X-RateLimit-Remaining": "0",
    }
    body = {
        "error": "rate_limited",
        "detail": f"Too many requests. Retry after {int(retry_after) + 1} seconds.",
    }
    return body, 429, headers
