"""Redis-backed caching layer for metadata, file info, and bot responses.

Provides a simple async cache with TTL support for:
- Job metadata and status
- File info (size, type, hash)
- Bot response caching
- User session data
- Media analysis results

Usage:
    from utils.cache import get_cache

    cache = await get_cache()
    await cache.set("job:abc123", {"status": "processing"}, ttl=3600)
    data = await cache.get("job:abc123")
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

logger = logging.getLogger(__name__)

# Default TTLs (seconds)
DEFAULT_TTL = 3600          # 1 hour
SHORT_TTL = 300             # 5 minutes
MEDIUM_TTL = 1800           # 30 minutes
LONG_TTL = 86400            # 24 hours

# Key prefixes
PREFIX_JOB = "cache:job:"
PREFIX_FILE = "cache:file:"
PREFIX_USER = "cache:user:"
PREFIX_META = "cache:meta:"
PREFIX_RESPONSE = "cache:resp:"


class RedisCache:
    """Async Redis-backed cache with TTL support."""

    def __init__(self, redis_url: Optional[str] = None):
        self._redis_url = redis_url or os.getenv("REDIS_URL") or ""
        self._client: Optional[aioredis.Redis] = None

    async def _get_client(self) -> Optional[aioredis.Redis]:
        """Lazy-initialize and return the Redis client."""
        if self._client is not None:
            return self._client
        if not self._redis_url or aioredis is None:
            return None
        try:
            self._client = aioredis.from_url(
                self._redis_url,
                decode_responses=True,
                max_connections=20,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            # Verify connection
            await self._client.ping()
            logger.info("Redis cache connected successfully")
            return self._client
        except Exception as e:
            logger.warning("Redis cache connection failed: %s", e)
            self._client = None
            return None

    async def get(self, key: str) -> Optional[Any]:
        """Get a cached value by key. Returns None on miss or error."""
        client = await self._get_client()
        if client is None:
            return None
        try:
            raw = await client.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as e:
            logger.debug("Cache GET failed for %s: %s", key, e)
            return None

    async def set(self, key: str, value: Any, ttl: int = DEFAULT_TTL) -> bool:
        """Set a cached value with TTL. Returns True on success."""
        client = await self._get_client()
        if client is None:
            return False
        try:
            serialized = json.dumps(value, default=str)
            await client.setex(key, ttl, serialized)
            return True
        except Exception as e:
            logger.debug("Cache SET failed for %s: %s", key, e)
            return False

    async def delete(self, key: str) -> bool:
        """Delete a cached value. Returns True on success."""
        client = await self._get_client()
        if client is None:
            return False
        try:
            await client.delete(key)
            return True
        except Exception as e:
            logger.debug("Cache DELETE failed for %s: %s", key, e)
            return False

    async def exists(self, key: str) -> bool:
        """Check if a key exists in cache."""
        client = await self._get_client()
        if client is None:
            return False
        try:
            return bool(await client.exists(key))
        except Exception:
            return False

    async def incr(self, key: str, amount: int = 1, ttl: int = DEFAULT_TTL) -> Optional[int]:
        """Increment a counter. Returns new value or None on error."""
        client = await self._get_client()
        if client is None:
            return None
        try:
            val = await client.incrby(key, amount)
            # Set TTL only if key is new (avoid resetting on every incr)
            if val == amount:
                await client.expire(key, ttl)
            return val
        except Exception as e:
            logger.debug("Cache INCR failed for %s: %s", key, e)
            return None

    async def get_many(self, keys: list[str]) -> Dict[str, Any]:
        """Get multiple values at once. Returns dict of key->value for non-None results."""
        client = await self._get_client()
        if client is None:
            return {}
        try:
            values = await client.mget(keys)
            result = {}
            for key, raw in zip(keys, values):
                if raw is not None:
                    try:
                        result[key] = json.loads(raw)
                    except Exception:
                        pass
            return result
        except Exception as e:
            logger.debug("Cache MGET failed: %s", e)
            return {}

    async def set_many(self, mapping: Dict[str, Any], ttl: int = DEFAULT_TTL) -> bool:
        """Set multiple values at once with TTL."""
        client = await self._get_client()
        if client is None:
            return False
        try:
            pipe = client.pipeline()
            for key, value in mapping.items():
                serialized = json.dumps(value, default=str)
                pipe.setex(key, ttl, serialized)
            await pipe.execute()
            return True
        except Exception as e:
            logger.debug("Cache MSET failed: %s", e)
            return False

    async def get_or_set(self, key: str, compute_fn, ttl: int = DEFAULT_TTL):
        """Get from cache or compute, store, and return. Single entry point for read-through caching."""
        cached = await self.get(key)
        if cached is not None:
            return cached
        result = await compute_fn()
        if result is not None:
            await self.set(key, result, ttl=ttl)
        return result

    async def close(self):
        """Close the Redis connection."""
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                try:
                    await self._client.close()
                except Exception:
                    pass
            self._client = None

    # ── Convenience methods for common patterns ──

    async def cache_job_metadata(self, job_id: str, metadata: Dict[str, Any], ttl: int = MEDIUM_TTL) -> bool:
        """Cache job metadata (status, progress, etc.)."""
        return await self.set(f"{PREFIX_JOB}{job_id}", metadata, ttl=ttl)

    async def get_job_metadata(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get cached job metadata."""
        return await self.get(f"{PREFIX_JOB}{job_id}")

    async def update_job_metadata(self, job_id: str, fields: Dict[str, Any], ttl: int = MEDIUM_TTL) -> bool:
        """Update specific fields in cached job metadata (read-modify-write)."""
        existing = await self.get_job_metadata(job_id) or {}
        existing.update(fields)
        return await self.cache_job_metadata(job_id, existing, ttl=ttl)

    async def cache_file_info(self, file_key: str, info: Dict[str, Any], ttl: int = LONG_TTL) -> bool:
        """Cache file metadata (size, type, hash, ffprobe output)."""
        return await self.set(f"{PREFIX_FILE}{file_key}", info, ttl=ttl)

    async def get_file_info(self, file_key: str) -> Optional[Dict[str, Any]]:
        """Get cached file metadata."""
        return await self.get(f"{PREFIX_FILE}{file_key}")

    async def cache_user_session(self, user_id: str, session_data: Dict[str, Any], ttl: int = LONG_TTL) -> bool:
        """Cache user session/preferences."""
        return await self.set(f"{PREFIX_USER}{user_id}", session_data, ttl=ttl)

    async def get_user_session(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get cached user session."""
        return await self.get(f"{PREFIX_USER}{user_id}")

    async def cache_response(self, key: str, response: Any, ttl: int = SHORT_TTL) -> bool:
        """Cache a bot response for deduplication."""
        return await self.set(f"{PREFIX_RESPONSE}{key}", response, ttl=ttl)

    async def get_cached_response(self, key: str) -> Optional[Any]:
        """Get a cached bot response."""
        return await self.get(f"{PREFIX_RESPONSE}{key}")

    async def cache_media_analysis(self, file_hash: str, analysis: Dict[str, Any], ttl: int = LONG_TTL) -> bool:
        """Cache ffprobe/media analysis results."""
        return await self.set(f"{PREFIX_META}analysis:{file_hash}", analysis, ttl=ttl)

    async def get_media_analysis(self, file_hash: str) -> Optional[Dict[str, Any]]:
        """Get cached media analysis."""
        return await self.get(f"{PREFIX_META}analysis:{file_hash}")


# ── Singleton access ──

_cache_singleton: Optional[RedisCache] = None


async def get_cache() -> RedisCache:
    """Return the shared Redis cache instance."""
    global _cache_singleton
    if _cache_singleton is None:
        _cache_singleton = RedisCache()
    return _cache_singleton


async def close_cache():
    """Close the shared Redis cache."""
    global _cache_singleton
    if _cache_singleton is not None:
        await _cache_singleton.close()
        _cache_singleton = None
