# utils/rate_limiter.py
"""
Rate limiting utilities for Telegram API and bot operations.
"""

import asyncio
import logging
import time
from collections import defaultdict
from typing import Dict, Tuple

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
        self.buckets: Dict[str, Tuple[float, float]] = defaultdict(lambda: (calls_per_second, time.time()))
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

            # Refill tokens based on elapsed time
            refill_rate = self.calls_per_second
            new_tokens = min(self.calls_per_second, current_tokens + (elapsed * refill_rate))

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

        return time.time() - start_time

    def get_stats(self, user_id: str = None) -> Dict:
        """Get rate limiter statistics."""
        stats = {}

        if user_id:
            tokens, last_time = self.buckets.get(user_id, (self.calls_per_second, time.time()))
            stats[user_id] = {
                "available_tokens": tokens,
                "last_refill": last_time,
                "seconds_until_refill": max(0, (1.0 / self.calls_per_second) - (time.time() - last_time)),
            }
        else:
            for key, (tokens, last_time) in self.buckets.items():
                stats[key] = {
                    "available_tokens": tokens,
                    "last_refill": last_time,
                    "seconds_until_refill": max(0, (1.0 / self.calls_per_second) - (time.time() - last_time)),
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
        allowed = await self.limiter.acquire(user_id=user_id, tokens=1)

        if allowed:
            # Record conversion
            self.conversion_history[user_id].append(time.time())
            # Keep only last hour of history
            cutoff = time.time() - 3600
            self.conversion_history[user_id] = [t for t in self.conversion_history[user_id] if t > cutoff]
            return True, "Conversion allowed"
        else:
            stats = self.limiter.get_stats(user_id)
            wait_time = stats[user_id]["seconds_until_refill"]
            conversions_used = len(self.conversion_history[user_id])
            return False, (
                f"❌ Rate limit reached ({conversions_used}/{self.conversions_per_hour} per hour)\n"
                f"Please wait {wait_time:.1f} seconds before next conversion"
            )

    def get_user_conversion_count(self, user_id: str) -> int:
        """Get number of conversions for user in last hour."""
        now = time.time()
        cutoff = now - 3600
        count = sum(1 for t in self.conversion_history.get(user_id, []) if t > cutoff)
        return count
