# utils/rate_limiter.py
"""
Rate limiting utilities for Telegram API and bot operations.
"""

import asyncio
import contextlib
import logging
import os
import time
from collections import defaultdict

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
        self.buckets: dict[str, tuple[float, float]] = defaultdict(lambda: (initial_tokens, time.time()))
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
                new_tokens = min(self.capacity, current_tokens + (elapsed * refill_rate))

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
            logger.debug("RateLimiter: diagnostic log failed (non-fatal)")

        return waited

    def get_stats(self, user_id: str = None) -> dict:
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

    async def wait_if_needed(self, user_id: str = "global") -> tuple[float, float]:
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
            logger.debug("TelegramAPIRateLimiter: diagnostic log failed (non-fatal)")

        return (global_wait, per_user_wait)

    def get_stats(self, user_id: str = None) -> dict:
        """Get rate limiter statistics."""
        return {
            "global": self.global_limiter.get_stats(),
            "per_user": self.per_user_limiter.get_stats(user_id) if user_id else self.per_user_limiter.get_stats(),
        }


# ── Telegram flood-control gate ──────────────────────────────────────────────
# Telegram answers a burst of edits/sends with a 429 "Flood control exceeded.
# Retry in N seconds" where N is sometimes measured in *hours* (27 000+ has been
# observed on this bot). Sleeping that value inline parks whatever coroutine hit
# it - a handler, a watcher - for the whole window, which is how the bot ends up
# looking dead while getUpdates keeps answering 200.
#
# The gate records the window so callers can decide instead of blocking: drop a
# progress edit, give up on a send. Nothing is ever slept longer than
# ``inline_max``.

FLOOD_WAIT_INLINE_MAX = float(os.getenv("TELEGRAM_FLOOD_INLINE_MAX_SECONDS", "30"))

# Windows are mirrored into Redis so the bot and the worker - separate containers
# sharing one bot token and one Telegram budget - honour each other's penalties.
# Each process keeps its own copy as well: the local view answers a check without
# a round trip, and a missing Redis (or a dead one) degrades to process-local
# behaviour instead of blocking the write path.
FLOOD_KEY_PREFIX = os.getenv("TELEGRAM_FLOOD_KEY_PREFIX", "ffmpeg:flood:")
FLOOD_RESYNC_SECONDS = float(os.getenv("TELEGRAM_FLOOD_RESYNC_SECONDS", "1.0"))
FLOOD_REDIS_BACKOFF_SECONDS = float(os.getenv("TELEGRAM_FLOOD_REDIS_BACKOFF_SECONDS", "30"))

# Extend-only, in one round trip: the stored value is the window's absolute end
# (unix seconds) and its TTL is what is left of it. A later, smaller retry_after
# - Telegram's countdown decays on every 429 - must not reopen the gate early,
# and two processes noting the same flood must not shorten each other's window.
_FLOOD_EXTEND_LUA = """
local now = tonumber(ARGV[1])
local deadline = tonumber(ARGV[2])
local current = redis.call('GET', KEYS[1])
if current and tonumber(current) >= deadline then
    return math.ceil(tonumber(current) - now)
end
local ttl = math.ceil(deadline - now)
if ttl < 1 then ttl = 1 end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ttl)
return ttl
"""


async def shared_flood_redis():
    """Shared Redis client for the flood gate, or None when it is not available.

    Resolved through the module global at call time so ``job_queue.get_redis`` can
    be patched in tests, and so an unreachable Redis is reported as "no shared
    view" rather than raised.
    """
    factory = globals().get("get_redis")
    if factory is None:
        return None
    try:
        return await factory()
    except Exception:
        return None


def _flood_namespace() -> str:
    """Namespace shared keys per bot so two bots on one Redis never share windows.

    The Bot API id is the first segment of the token and is not a secret; only
    that segment is used.
    """
    token = os.getenv("BOT_TOKEN") or ""
    if ":" in token:
        return token.split(":", 1)[0]
    return "default"


class TelegramFloodGate:
    """Record of open Telegram flood-control windows, shared between processes.

    Windows are per chat, because that is how Telegram enforces them: a penalty
    earned in one busy chat must not silence every other chat, which is what
    folding a chat window into a shared scope would do. ``GLOBAL`` is only for
    callers that cannot name a chat at all.

    Every method is async and consults the shared backend; the class is only ever
    attached to Redis by a process that owns one (:meth:`attach_redis`), so tests
    and one-off scripts stay purely in memory.
    """

    GLOBAL = "global"

    def __init__(
        self,
        inline_max: float | None = None,
        redis_factory=None,
        resync_interval: float = FLOOD_RESYNC_SECONDS,
    ):
        self.inline_max = FLOOD_WAIT_INLINE_MAX if inline_max is None else float(inline_max)
        self.resync_interval = float(resync_interval)
        self._redis_factory = redis_factory
        # scope -> monotonic deadline, this process's view of the window
        self._until: dict[str, float] = {}
        # scope -> monotonic time of the last shared read (keeps the read rate down)
        self._last_read: dict[str, float] = {}
        # monotonic time until which the shared backend is considered unavailable
        self._backend_down_until = 0.0

    def attach_redis(self, factory=shared_flood_redis) -> None:
        """Share windows through ``factory`` (``async () -> client | None``).

        Called once at process start by the bot and the worker. Detached - the
        default - the gate never performs I/O.
        """
        self._redis_factory = factory

    @staticmethod
    def scope_for_chat(chat_id=None) -> str:
        """Return the gate scope for a chat id (``global`` when unknown)."""
        if chat_id is None:
            return TelegramFloodGate.GLOBAL
        return f"chat:{chat_id}"

    async def note(self, retry_after, scope: str = GLOBAL) -> float:
        """Record a flood window and return the seconds it spans.

        The window is published for the other processes and extended, never
        shortened: the longest window anyone holds wins.
        """
        try:
            seconds = float(retry_after)
        except (TypeError, ValueError):
            seconds = 0.0
        if seconds <= 0:
            return 0.0

        # A hard cap keeps a bogus retry_after from wedging the gate for days.
        seconds = min(seconds, 24 * 3600.0)
        self._remember(scope, seconds)
        shared = await self._publish(scope, seconds)
        if shared > seconds:
            # Another container is already holding a longer window for this chat.
            self._remember(scope, shared)
            seconds = shared
        return seconds

    async def remaining(self, scope: str = GLOBAL) -> float:
        """Seconds left on the longest window known for ``scope`` (0 when open)."""
        await self._sync(scope)
        now = time.monotonic()
        return max(0.0, self._until.get(scope, 0.0) - now)

    async def is_open(self, scope: str = GLOBAL) -> bool:
        return await self.remaining(scope) > 0.0

    async def should_drop_inline(self, scope: str = GLOBAL) -> bool:
        """Whether a caller should skip its API call entirely.

        True while a window longer than ``inline_max`` is open: the call would
        only collect another 429, so the caller is better off dropping it.
        """
        return await self.remaining(scope) > self.inline_max

    async def wait(self, scope: str = GLOBAL, max_wait: float | None = None) -> float:
        """Wait out a flood window, never longer than ``max_wait``.

        Returns the seconds still remaining once the wait returns: 0 when the
        window closed, > 0 when it is still open and the caller should give up.
        """
        limit = self.inline_max if max_wait is None else float(max_wait)
        left = await self.remaining(scope)
        if left <= 0.0:
            return 0.0
        if left > limit:
            return left
        await asyncio.sleep(left + 0.5)
        return await self.remaining(scope)

    def reset(self) -> None:
        """Clear the local view (used by tests; the shared keys keep their TTL)."""
        self._until.clear()
        self._last_read.clear()
        self._backend_down_until = 0.0

    def snapshot(self) -> dict[str, float]:
        """Remaining seconds per scope, for health/metrics output."""
        now = time.monotonic()
        return {k: round(max(0.0, v - now), 1) for k, v in self._until.items()}

    # ── shared backend ───────────────────────────────────────────────────────

    def _remember(self, scope: str, seconds: float) -> None:
        """Keep the longest window seen, in monotonic time."""
        deadline = time.monotonic() + seconds
        if deadline > self._until.get(scope, 0.0):
            self._until[scope] = deadline
        self._prune()

    def _key(self, scope: str) -> str:
        return f"{FLOOD_KEY_PREFIX}{_flood_namespace()}:{scope}"

    async def _client(self):
        """A shared client, or None while the backend is unattached/unreachable.

        A failure backs the gate off for a while: a dead Redis must not add a
        connection attempt to every progress edit.
        """
        if self._redis_factory is None:
            return None
        if time.monotonic() < self._backend_down_until:
            return None
        client = await self._redis_factory()
        if client is None:
            self._backend_down_until = time.monotonic() + FLOOD_REDIS_BACKOFF_SECONDS
        return client

    @staticmethod
    async def _release(client) -> None:
        """Give the shared client back (``job_queue``'s proxy close is a no-op)."""
        with contextlib.suppress(Exception):
            await client.close()

    async def _publish(self, scope: str, seconds: float) -> float:
        """Store this window for the other processes; returns its effective span."""
        client = await self._client()
        if client is None:
            return 0.0
        now = time.time()
        try:
            result = await client.eval(_FLOOD_EXTEND_LUA, 1, self._key(scope), now, now + seconds)
            return float(result or 0.0)
        except Exception:
            logger.debug("flood gate: could not publish a %.0fs window for %s", seconds, scope)
            self._backend_down_until = time.monotonic() + FLOOD_REDIS_BACKOFF_SECONDS
            return 0.0
        finally:
            await self._release(client)

    async def _sync(self, scope: str) -> None:
        """Adopt a window another process opened (at most one read per interval)."""
        if self._redis_factory is None:
            return
        now = time.monotonic()
        last = self._last_read.get(scope)
        if last is not None and (now - last) < self.resync_interval:
            return
        self._last_read[scope] = now
        if len(self._last_read) > 512:
            self._prune()

        client = await self._client()
        if client is None:
            return
        try:
            raw = await client.get(self._key(scope))
        except Exception:
            logger.debug("flood gate: could not read the shared window for %s", scope)
            self._backend_down_until = time.monotonic() + FLOOD_REDIS_BACKOFF_SECONDS
            return
        finally:
            await self._release(client)

        if not raw:
            return
        try:
            remaining = float(raw) - time.time()
        except (TypeError, ValueError):
            return
        if remaining > 0:
            self._remember(scope, remaining)

    def _prune(self) -> None:
        now = time.monotonic()
        for key, deadline in list(self._until.items()):
            if deadline <= now:
                self._until.pop(key, None)
        if len(self._last_read) > 512:
            cutoff = now - max(60.0, self.resync_interval * 10)
            for key, when in list(self._last_read.items()):
                if when < cutoff:
                    self._last_read.pop(key, None)


# One gate per process. The bot and the worker both attach it to Redis at start
# up so neither can keep writing while the other is serving a flood control.
telegram_flood_gate = TelegramFloodGate()


class TelegramEditCoalescer:
    """Keep several watchers from editing one message into a 429.

    Telegram counts an edit like a message, so N watchers each pacing themselves
    to 2-3s still exceed one edit per second on the same chat when they all
    render onto the same message. The allowance here is per ``(chat_id,
    message_id)``, so the watchers that share a message share it.

    What a message currently shows is only learned from :meth:`record`, which
    callers run *after* Telegram accepted the edit. Deciding a skip from a text
    this class merely predicted would suppress a real update - and a message id
    that has since been deleted and reposted would look like a duplicate of
    whatever used to sit there, which is why :meth:`forget` exists.
    """

    def __init__(self, min_interval: float = 2.5, max_tracked: int = 512):
        self.min_interval = float(min_interval)
        self.max_tracked = int(max_tracked)
        # (chat_id, message_id) -> (last_sent_monotonic, text_digest)
        self._last: dict[tuple, tuple[float, int]] = {}

    def should_skip(self, chat_id, message_id, text, min_interval: float | None = None, force: bool = False) -> bool:
        """Whether this edit should be dropped instead of sent.

        Skipped when it would repeat the text already on the message (Telegram
        answers an unchanged edit with a 400, so a repeat is dropped whatever the
        caller asked for), or when the message was last rendered less than
        ``min_interval`` ago. ``force`` lifts only that pacing, which is how a
        terminal status gets through even if it lands inside the interval of the
        update before it. A caller with no message to key on is never skipped.
        """
        if chat_id is None or message_id is None:
            return False

        last = self._last.get((chat_id, message_id))
        if last is None:
            return False

        last_time, last_digest = last
        if hash(text) == last_digest:
            return True

        gap = self.min_interval if min_interval is None else float(min_interval)
        return not force and (time.monotonic() - last_time) < gap

    def shows(self, chat_id, message_id, text) -> bool:
        """Whether the message is already showing exactly ``text``.

        Lets a caller tell a skip that lost nothing (the text is on screen) from
        one that has to be retried (the pacing or the flood gate dropped it).
        """
        if chat_id is None or message_id is None:
            return False
        last = self._last.get((chat_id, message_id))
        return last is not None and last[1] == hash(text)

    def record(self, chat_id, message_id, text) -> None:
        """Remember what Telegram now shows on the message."""
        if chat_id is None or message_id is None:
            return
        self._last[(chat_id, message_id)] = (time.monotonic(), hash(text))
        if len(self._last) > self.max_tracked:
            self._prune()

    def forget(self, chat_id, message_id) -> None:
        """Drop what is remembered about a message that no longer exists."""
        self._last.pop((chat_id, message_id), None)

    def reset(self) -> None:
        self._last.clear()

    def _prune(self) -> None:
        # Drop the stalest entries; a long-running bot renders many messages.
        now = time.monotonic()
        keep = max(60.0, self.min_interval * 20)
        for key, (when, _) in list(self._last.items()):
            if (now - when) > keep:
                self._last.pop(key, None)
        if len(self._last) > self.max_tracked:
            oldest = sorted(self._last.items(), key=lambda item: item[1][0])
            for key, _ in oldest[: len(self._last) - self.max_tracked]:
                self._last.pop(key, None)


# Shared across every watcher/handler in the process.
telegram_edit_coalescer = TelegramEditCoalescer()


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
        self.conversion_history: dict[str, list] = defaultdict(list)

    async def can_convert(self, user_id: str) -> tuple[bool, str]:
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

    async def can_convert(self, user_id: str) -> tuple[bool, str]:
        """Non-consuming check whether user may convert (does not reserve).

        Returns (allowed: bool, message: str)
        """
        if get_redis is None:
            # fallback to permissive policy when redis not available
            return True, "Conversion allowed"

        try:
            r = await get_redis()
            try:
                key = self._key(user_id)
                now = int(time.time())
                cutoff = now - self.window
                with contextlib.suppress(Exception):
                    # remove old entries for accurate count
                    await r.zremrangebyscore(key, 0, cutoff)
                cnt = await r.zcard(key)
            finally:
                await r.close()
            if cnt < self.conversions_per_hour:
                return True, "Conversion allowed"
            # compute wait time until earliest entry expires
            try:
                r = await get_redis()
                try:
                    vals = await r.zrange(key, 0, 0, withscores=True)
                finally:
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
            with contextlib.suppress(Exception):
                await r.close()
            return True
