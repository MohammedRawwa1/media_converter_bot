# utils/rate_limiter.py
"""
Rate limiting utilities for Telegram API and bot operations.
"""

import asyncio
import logging
import time
from collections import defaultdict
from typing import Dict, Tuple
import os

try:
    # optional redis usage for distributed rate limiting
    from utils.job_queue import get_redis
except Exception:
    get_redis = None

logger = logging.getLogger(__name__)


class RateLimiter:
    """Rate limiter with token bucket algorithm."""

    def __init__(self, calls_per_second: float = 30, per_user: bool = False):
        """
        Initialize rate limiter.

        Args:
            calls_per_second: Maximum calls per second
            per_user: If True, rate limit is per user; if False, global
        """
        self.calls_per_second = calls_per_second
        self.per_user = per_user
        # Token bucket: {key -> (tokens, last_refill_time)}
        # Ensure capacity is at least 1 so the first operation is allowed.
        # Use `capacity` for refill cap (was incorrectly using calls_per_second).
        self.capacity = max(1.0, calls_per_second)
        initial_tokens = float(self.capacity)
        self.buckets: Dict[str, Tuple[float, float]] = defaultdict(lambda: (initial_tokens, time.time()))
        self._lock = asyncio.Lock()

    async def acquire(self, user_id: str = "global", tokens: float = 1.0) -> bool:
        """
        Try to acquire tokens from the bucket.

        Args:
            user_id: User identifier (only used if per_user=True)
            tokens: Number of tokens to acquire

        Returns:
            True if acquired, False if rate limited
        """
        async with self._lock:
            key = user_id if self.per_user else "global"
            current_tokens, last_time = self.buckets[key]

            now = time.time()
            elapsed = now - last_time

            # Refill tokens based on elapsed time (tokens per second)
            refill_rate = self.calls_per_second
            # Don't exceed bucket capacity when refilling
            try:
                new_tokens = min(self.capacity, current_tokens + (elapsed * refill_rate))
            except Exception:
                new_tokens = min(initial_tokens, current_tokens + (elapsed * refill_rate))

            if new_tokens >= tokens:
                self.buckets[key] = (new_tokens - tokens, now)
                return True
            else:
                self.buckets[key] = (new_tokens, now)
                return False

    async def wait_if_needed(self, user_id: str = "global", tokens: float = 1.0) -> float:
        """
        Wait until tokens are available and acquire them.

        Args:
            user_id: User identifier
            tokens: Number of tokens needed

        Returns:
            Wait time in seconds (0 if no wait needed)
        """
        start_time = time.time()

        while not await self.acquire(user_id, tokens):
            await asyncio.sleep(0.01)  # Small delay before retry

        waited = time.time() - start_time
        # Diagnostic log when wait exceeds a small threshold (helps find long rate-limit stalls)
        try:
            if waited > 2.0:
                logger.warning(
                    "RateLimiter.wait_if_needed waited %.2fs for key=%s (tokens=%s, capacity=%s, cps=%s)",
                    waited,
                    user_id,
                    tokens,
                    getattr(self, "capacity", "unknown"),
                    getattr(self, "calls_per_second", "unknown"),
                )
        except Exception:
            # Avoid raising from logging diagnostics
            pass

        return waited

    def get_stats(self, user_id: str = None) -> Dict:
        """Get rate limiter statistics."""
        stats = {}

        if user_id:
            tokens, last_time = self.buckets.get(user_id, (self.capacity, time.time()))
            # Compute time until at least one token is available
            if self.calls_per_second > 0:
                tokens_needed = max(0.0, 1.0 - tokens)
                seconds_until_refill = tokens_needed / self.calls_per_second
            else:
                seconds_until_refill = float("inf")

            stats[user_id] = {
                "available_tokens": tokens,
                "last_refill": last_time,
                "seconds_until_refill": max(0.0, seconds_until_refill),
            }
        else:
            for key, (tokens, last_time) in self.buckets.items():
                if self.calls_per_second > 0:
                    tokens_needed = max(0.0, 1.0 - tokens)
                    seconds_until_refill = tokens_needed / self.calls_per_second
                else:
                    seconds_until_refill = float("inf")

                stats[key] = {
                    "available_tokens": tokens,
                    "last_refill": last_time,
                    "seconds_until_refill": max(0.0, seconds_until_refill),
                }

        return stats


class TelegramAPIRateLimiter:
    """Specialized rate limiter for Telegram API calls."""

    # Telegram rate limits
    GENERAL_LIMIT = 30  # 30 calls per second globally
    PER_USER_LIMIT = 1  # 1 call per second per user

    def __init__(self):
        """Initialize Telegram API rate limiters."""
        self.global_limiter = RateLimiter(self.GENERAL_LIMIT, per_user=False)
        self.per_user_limiter = RateLimiter(self.PER_USER_LIMIT, per_user=True)

    async def acquire(self, user_id: str = "global") -> bool:
        """
        Try to acquire rate limit tokens for Telegram API call.

        Args:
            user_id: User ID making the request

        Returns:
            True if allowed, False if rate limited
        """
        # Check both global and per-user limits
        global_ok = await self.global_limiter.acquire(tokens=1)
        per_user_ok = await self.per_user_limiter.acquire(user_id=user_id, tokens=1)

        return global_ok and per_user_ok

    async def wait_if_needed(self, user_id: str = "global") -> Tuple[float, float]:
        """
        Wait until rate limit allows the call.

        Args:
            user_id: User ID making the request

        Returns:
            Tuple of (global_wait_time, per_user_wait_time)
        """
        global_wait = await self.global_limiter.wait_if_needed(tokens=1)
        per_user_wait = await self.per_user_limiter.wait_if_needed(user_id=user_id, tokens=1)

        # Diagnostic log when either wait is noticeable (>2s)
        try:
            if global_wait > 2.0 or per_user_wait > 2.0:
                logger.warning(
                    "TelegramAPIRateLimiter.wait_if_needed: user=%s global_wait=%.2fs per_user_wait=%.2fs",
                    user_id,
                    global_wait,
                    per_user_wait,
                )
        except Exception:
            pass

        return (global_wait, per_user_wait)

    def get_stats(self, user_id: str = None) -> Dict:
        """Get rate limiter statistics."""
        return {
            "global": self.global_limiter.get_stats(),
            "per_user": self.per_user_limiter.get_stats(user_id) if user_id else self.per_user_limiter.get_stats(),
        }


class ConversionRateLimiter:
    """Rate limiter specifically for media conversions."""

    def __init__(self, conversions_per_hour: int = 100):
        """
        Initialize conversion rate limiter.

        Args:
            conversions_per_hour: Max conversions per hour per user
        """
        self.conversions_per_hour = conversions_per_hour
        self.per_second = conversions_per_hour / 3600
        self.limiter = RateLimiter(self.per_second, per_user=True)
        self.conversion_history: Dict[str, list] = defaultdict(list)

    async def can_convert(self, user_id: str) -> Tuple[bool, str]:
        """
        Check if user can start a conversion.

        Args:
            user_id: User ID

        Returns:
            Tuple of (allowed: bool, message: str)
        """
        # Non-consuming check: only inspect recent conversion history
        now = time.time()
        cutoff = now - 3600
        history = self.conversion_history.get(user_id, [])
        recent = [t for t in history if t > cutoff]
        if len(recent) < self.conversions_per_hour:
            return True, "Conversion allowed"
        # compute approximate wait using oldest timestamp in window
        earliest = min(recent) if recent else now
        wait_time = max(0.0, (earliest + 3600) - now)
        return False, (
            f"❌ Rate limit reached ({len(recent)}/{self.conversions_per_hour} per hour)\n"
            f"Please wait {wait_time:.1f} seconds before next conversion"
        )

    async def mark_conversion_started(self, user_id: str) -> bool:
        """Consume quota and record that a conversion has actually started.

        Returns True if allowed and recorded, False if rate limited.
        """
        allowed = await self.limiter.acquire(user_id=user_id, tokens=1)
        if allowed:
            # Record conversion start
            self.conversion_history.setdefault(user_id, []).append(time.time())
            # Keep only last hour of history
            cutoff = time.time() - 3600
            self.conversion_history[user_id] = [t for t in self.conversion_history[user_id] if t > cutoff]
            return True
        return False

    def get_user_conversion_count(self, user_id: str) -> int:
        """Get number of conversions for user in last hour."""
        now = time.time()
        cutoff = now - 3600
        count = sum(1 for t in self.conversion_history.get(user_id, []) if t > cutoff)
        return count


class ConversionRateLimiterRedis:
    """Redis-backed conversion rate limiter suitable for multi-process deployments.

    Uses a sorted set per user to store timestamps of started conversions. Methods
    match the interface used in handlers: `can_convert(user_id)` and
    `mark_conversion_started(user_id)`.
    """

    def __init__(self, conversions_per_hour: int = 100, redis_key_prefix: str = "rl:conv:"):
        self.conversions_per_hour = int(conversions_per_hour)
        self.window = 3600
        self.prefix = redis_key_prefix

    def _key(self, user_id: str) -> str:
        return f"{self.prefix}{user_id}"

    async def can_convert(self, user_id: str) -> Tuple[bool, str]:
        """Non-consuming check whether user may convert (does not reserve).

        Returns (allowed: bool, message: str)
        """
        if get_redis is None:
            # fallback to permissive policy when redis not available
            return True, "Conversion allowed"

        try:
            r = await get_redis()
            key = self._key(user_id)
            now = int(time.time())
            cutoff = now - self.window
            try:
                # remove old entries for accurate count
                await r.zremrangebyscore(key, 0, cutoff)
            except Exception:
                pass
            cnt = await r.zcard(key)
            await r.close()
            if cnt < self.conversions_per_hour:
                return True, "Conversion allowed"
            # compute wait time until earliest entry expires
            try:
                r = await get_redis()
                vals = await r.zrange(key, 0, 0, withscores=True)
                await r.close()
                if vals and len(vals) > 0:
                    earliest_score = vals[0][1]
                    wait = max(0.0, (earliest_score + self.window) - now)
                else:
                    wait = 3600.0
            except Exception:
                wait = 3600.0

            return False, (
                f"❌ Rate limit reached (max {self.conversions_per_hour} per hour). "
                f"Please wait {wait:.1f} seconds before starting a conversion."
            )
        except Exception:
            return True, "Conversion allowed"

    async def mark_conversion_started(self, user_id: str) -> bool:
        """Attempt to record a started conversion for `user_id`.

        Returns True if recorded (allowed), False if rate limit prevents starting.
        This operation is atomic via a small Lua script that prunes old entries,
        checks the current count, and inserts the new timestamp if under limit.
        """
        if get_redis is None:
            return True

        try:
            r = await get_redis()
        except Exception:
            return True

        key = self._key(user_id)
        now = int(time.time())
        cutoff = now - self.window
        # Lua script: remove old, count, add if allowed, set expire
        script = (
            "redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1]);"
            "local cnt = redis.call('ZCARD', KEYS[1]);"
            "if tonumber(cnt) < tonumber(ARGV[2]) then "
            "redis.call('ZADD', KEYS[1], ARGV[3], ARGV[3]);"
            "redis.call('EXPIRE', KEYS[1], ARGV[4]);"
            "return 1;"
            "end;"
            "return 0;"
        )

        try:
            # expire slightly longer than window to ensure records persist long enough
            expire_seconds = self.window + 60
            res = await r.eval(script, 1, key, cutoff, self.conversions_per_hour, now, expire_seconds)
            await r.close()
            return bool(res)
        except Exception:
            try:
                await r.close()
            except Exception:
                pass
            return True

