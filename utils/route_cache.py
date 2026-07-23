# utils/route_cache.py
"""
API response caching layer for Flask and FastAPI endpoints.

Provides:
- Read-through cache for GET endpoints
- Cache invalidation on POST/PUT/DELETE
- Per-route TTL configuration
- Cache key generation from route + query params + request body
- Redis-backed with in-memory fallback

Usage:
    from utils.route_cache import RouteCache

    cache = RouteCache()

    # Cache a GET response
    @app.route("/status/<job_id>")
    def status(job_id):
        cached = cache.get(f"status:{job_id}")
        if cached:
            return cached
        result = compute_status(job_id)
        cache.set(f"status:{job_id}", result, ttl=30)
        return result

    # Invalidate on mutation
    @app.route("/upload", methods=["POST"])
    def upload():
        result = process_upload()
        cache.invalidate_prefix("status:")
        return result
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class RouteCache:
    """Route-level response cache with TTL and invalidation.

    Uses Redis when available, with in-memory fallback.
    Supports prefix-based invalidation (e.g., invalidate all 'status:*' keys).

    Thread-safe for Flask (sync) and async-safe for FastAPI (async).
    """

    def __init__(self, redis_client=None, default_ttl: int = 60):
        self._redis = redis_client
        self._default_ttl = default_ttl
        # In-memory fallback: {key: (value, expires_at)}
        self._memory: dict[str, tuple[Any, float]] = {}
        self._memory_max = 1000  # Max in-memory entries

    # ── Public API ────────────────────────────────────────────────────

    def get(self, key: str) -> Any | None:
        """Get a cached response by key. Returns None on miss."""
        # Try Redis first
        if self._redis:
            try:
                raw = self._redis.get(self._prefixed_key(key))
                if raw is not None:
                    return json.loads(raw)
            except Exception as e:
                logger.debug("RouteCache Redis GET failed: %s", e)

        # Fallback to in-memory
        return self._memory_get(key)

    async def aget(self, key: str) -> Any | None:
        """Async version of get()."""
        if self._redis:
            try:
                from utils.job_queue import get_redis
                r = await get_redis()
                raw = await r.get(self._prefixed_key(key))
                if raw is not None:
                    return json.loads(raw)
            except Exception as e:
                logger.debug("RouteCache async GET failed: %s", e)

        return self._memory_get(key)

    def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Cache a response with TTL (seconds). Returns True on success."""
        ttl = ttl or self._default_ttl
        prefixed = self._prefixed_key(key)
        serialized = json.dumps(value, default=str)

        if self._redis:
            try:
                self._redis.setex(prefixed, ttl, serialized)
                return True
            except Exception as e:
                logger.debug("RouteCache Redis SET failed: %s", e)

        # In-memory fallback
        self._memory_set(key, value, ttl)
        return True

    async def aset(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Async version of set()."""
        ttl = ttl or self._default_ttl
        prefixed = self._prefixed_key(key)
        serialized = json.dumps(value, default=str)

        if self._redis:
            try:
                from utils.job_queue import get_redis
                r = await get_redis()
                await r.setex(prefixed, ttl, serialized)
                return True
            except Exception as e:
                logger.debug("RouteCache async SET failed: %s", e)

        self._memory_set(key, value, ttl)
        return True

    def delete(self, key: str) -> bool:
        """Delete a specific cache entry."""
        prefixed = self._prefixed_key(key)
        if self._redis:
            with contextlib.suppress(Exception):
                self._redis.delete(prefixed)
        self._memory_delete(key)
        return True

    async def adelete(self, key: str) -> bool:
        """Async version of delete()."""
        prefixed = self._prefixed_key(key)
        if self._redis:
            try:
                from utils.job_queue import get_redis
                r = await get_redis()
                await r.delete(prefixed)
            except Exception:
                pass
        self._memory_delete(key)
        return True

    def invalidate_prefix(self, prefix: str) -> int:
        """Invalidate all cache entries with the given key prefix.

        Uses Redis SCAN to find matching keys.  For in-memory, scans
        the local dict.  Returns count of invalidated entries.

        This is the primary invalidation strategy: when a mutation
        happens (upload, delete, etc.), invalidate the relevant prefix.
        """
        count = 0
        prefixed_prefix = self._prefixed_key(prefix)

        if self._redis:
            try:
                cursor = 0
                while True:
                    cursor, keys = self._redis.scan(
                        cursor=cursor, match=f"{prefixed_prefix}*", count=100
                    )
                    if keys:
                        self._redis.delete(*keys)
                        count += len(keys)
                    if cursor == 0:
                        break
            except Exception as e:
                logger.debug("RouteCache SCAN failed: %s", e)

        # In-memory invalidation
        count += self._memory_invalidate_prefix(prefix)
        return count

    async def ainvalidate_prefix(self, prefix: str) -> int:
        """Async version of invalidate_prefix()."""
        count = 0
        prefixed_prefix = self._prefixed_key(prefix)

        if self._redis:
            try:
                from utils.job_queue import get_redis
                r = await get_redis()
                cursor = 0
                while True:
                    cursor, keys = await r.scan(
                        cursor=cursor, match=f"{prefixed_prefix}*", count=100
                    )
                    if keys:
                        await r.delete(*keys)
                        count += len(keys)
                    if cursor == 0:
                        break
            except Exception as e:
                logger.debug("RouteCache async SCAN failed: %s", e)

        count += self._memory_invalidate_prefix(prefix)
        return count

    def get_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], Any],
        ttl: int | None = None,
    ) -> Any:
        """Read-through cache: get or compute and cache."""
        cached = self.get(key)
        if cached is not None:
            return cached
        result = compute_fn()
        if result is not None:
            self.set(key, result, ttl=ttl)
        return result

    async def aget_or_compute(
        self,
        key: str,
        compute_fn: Callable[[], Any],
        ttl: int | None = None,
    ) -> Any:
        """Async read-through cache."""
        cached = await self.aget(key)
        if cached is not None:
            return cached
        result = await compute_fn()
        if result is not None:
            await self.aset(key, result, ttl=ttl)
        return result

    # ── Cache key generation ──────────────────────────────────────────

    @staticmethod
    def make_key(route: str, params: dict | None = None) -> str:
        """Generate a deterministic cache key from route + sorted params."""
        if params:
            sorted_items = sorted(
                (k, str(v)) for k, v in params.items() if v is not None
            )
            param_hash = hashlib.sha256(
                json.dumps(sorted_items, default=str).encode()
            ).hexdigest()[:16]
            return f"{route}:{param_hash}"
        return route

    @staticmethod
    def make_job_key(job_id: str) -> str:
        """Generate cache key for job status."""
        return f"job:{job_id}"

    @staticmethod
    def make_search_key(query: str, page: int = 1, per_page: int = 5) -> str:
        """Generate cache key for search queries."""
        qhash = hashlib.sha256(query.lower().encode()).hexdigest()[:16]
        return f"search:{qhash}:p{page}:pp{per_page}"

    # ── Internal helpers ──────────────────────────────────────────────

    def _prefixed_key(self, key: str) -> str:
        return f"routecache:{key}"

    def _memory_get(self, key: str) -> Any | None:
        entry = self._memory.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at and time.time() > expires_at:
            self._memory_delete(key)
            return None
        return value

    def _memory_set(self, key: str, value: Any, ttl: int):
        # LRU eviction if full
        if len(self._memory) >= self._memory_max:
            try:
                # Remove oldest 20% of entries
                items = sorted(self._memory.items(), key=lambda x: x[1][1])
                for old_key, _ in items[: len(items) // 5]:
                    self._memory_delete(old_key)
            except Exception:
                self._memory.clear()

        expires_at = time.time() + ttl if ttl > 0 else 0
        self._memory[key] = (value, expires_at)

    def _memory_delete(self, key: str):
        self._memory.pop(key, None)

    def _memory_invalidate_prefix(self, prefix: str) -> int:
        count = 0
        keys_to_delete = [k for k in self._memory if k.startswith(prefix)]
        for k in keys_to_delete:
            self._memory_delete(k)
            count += 1
        return count


# Global singleton
route_cache = RouteCache()
