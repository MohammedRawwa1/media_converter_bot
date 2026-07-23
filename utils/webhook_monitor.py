# utils/webhook_monitor.py
"""
Webhook heartbeat monitoring for bot reliability.
"""

import asyncio
import contextlib
import logging
import os
import time
from datetime import datetime
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger(__name__)


class WebhookMonitor:
    """Monitor webhook health and connectivity."""

    def __init__(self, webhook_url: str, check_interval: int = 300):
        """
        Initialize webhook monitor.

        Args:
            webhook_url: The webhook URL to monitor
            check_interval: Seconds between health checks (default 5 minutes)
        """
        self.webhook_url = webhook_url
        self.check_interval = check_interval
        self.is_healthy = True
        self.last_check = None
        self.check_count = 0
        self.failed_checks = 0
        self.consecutive_failures = 0
        self.monitor_task = None
        # Current sleep interval (supports exponential backoff on transient errors)
        self._current_interval = check_interval
        # Upper bound for backoff (seconds) (override via env WEBHOOK_MAX_BACKOFF)
        max_backoff_env = os.environ.get("WEBHOOK_MAX_BACKOFF")
        if max_backoff_env:
            try:
                self._max_backoff = int(max_backoff_env)
            except Exception:
                self._max_backoff = max(check_interval * 8, 3600)
        else:
            self._max_backoff = max(check_interval * 8, 3600)

        # Per-check timeouts (override via env)
        self.local_timeout = int(os.environ.get("WEBHOOK_LOCAL_TIMEOUT", "3"))
        self.external_timeout = int(os.environ.get("WEBHOOK_EXTERNAL_TIMEOUT", "10"))

        # Last observed HTTP status code or exception message (for diagnostics)
        self.last_status_code = None
        self.last_error = None
    async def health_check(self) -> bool:
        """
        Perform a health check on the webhook.

        Returns:
            True if webhook is healthy, False otherwise
        """
        try:
            # First try a local loopback probe to avoid external DNS/network timeouts
            try:
                parsed = urlparse(self.webhook_url)
                local_port = int(os.environ.get("PORT", "10000"))
                local_path = parsed.path or "/"
                local_url = f"http://127.0.0.1:{local_port}{local_path}"
                async with aiohttp.ClientSession() as session:
                    try:
                        async with session.head(local_url, timeout=aiohttp.ClientTimeout(total=self.local_timeout)) as resp:
                            status = resp.status
                    except Exception as e_head:
                        # HEAD can be rejected or may time out; try GET as a fallback
                        logger.debug("Local HEAD failed; attempting GET", exc_info=e_head)
                        try:
                            async with session.get(local_url, timeout=aiohttp.ClientTimeout(total=self.local_timeout + 2)) as resp:
                                status = resp.status
                        except Exception:
                            logger.debug("Local GET also failed; falling back to external check")
                            raise

                    self.check_count += 1
                    self.last_check = datetime.now()
                    self.last_status_code = status
                    if status in (200, 404, 405):
                        self.is_healthy = True
                        self.consecutive_failures = 0
                        if getattr(self, "_current_interval", None) is not None:
                            self._current_interval = self.check_interval
                        logger.debug(f"✅ Local webhook loopback check passed (status: {status})")
                        return True
                    logger.debug(f"Local loopback check returned {status}; falling back to external check")
            except Exception:
                # Local check failed; continue to external check below
                logger.debug("Local loopback check failed; trying external webhook URL")

            # External public check (original behavior)
            async with aiohttp.ClientSession() as session:
                try:
                    try:
                        async with session.head(self.webhook_url, timeout=aiohttp.ClientTimeout(total=self.external_timeout)) as response:
                            status = response.status
                    except Exception as e_head:
                        logger.debug("External HEAD failed; attempting GET", exc_info=e_head)
                        try:
                            async with session.get(self.webhook_url, timeout=aiohttp.ClientTimeout(total=self.external_timeout + 5)) as response:
                                status = response.status
                        except Exception:
                            logger.debug("External GET also failed")
                            raise

                    self.check_count += 1
                    self.last_check = datetime.now()
                    self.last_status_code = status
                    # Healthy responses (allow 200/404/405 as acceptable for probes)
                    if status in (200, 404, 405):
                        self.is_healthy = True
                        self.consecutive_failures = 0
                        # Reset interval after success
                        if getattr(self, "_current_interval", None) is not None:
                            self._current_interval = self.check_interval
                        logger.debug(f"✅ Webhook health check passed (status: {status})")
                        return True

                    # Rate-limited: back off exponentially instead of hammering endpoint
                    if status == 429:
                        self.is_healthy = False
                        self.failed_checks += 1
                        self.consecutive_failures += 1
                        # Increase backoff interval with small jitter
                        old = getattr(self, "_current_interval", self.check_interval)
                        new = min(old * 2, self._max_backoff)
                        jitter = min(5, new * 0.1) * 0.5  # deterministic jitter
                        self._current_interval = new + jitter
                        logger.warning(
                            f"⚠️ Webhook health check rate-limited (status: 429). Backing off from {old}s to {self._current_interval:.1f}s"
                        )
                        return False

                    # Other non-success statuses
                    self.is_healthy = False
                    self.failed_checks += 1
                    self.consecutive_failures += 1
                    logger.warning(f"⚠️ Webhook health check failed (status: {status})")
                    return False
                except Exception as e:
                    # Surface exception details for debugging
                    self.last_error = str(e)
                    raise

        except TimeoutError as e:
            self.is_healthy = False
            self.failed_checks += 1
            self.consecutive_failures += 1
            # Exponential backoff on timeout with small jitter
            try:
                old = getattr(self, "_current_interval", self.check_interval)
                new = min(old * 2, self._max_backoff)
                jitter = min(5, new * 0.2) * 0.5  # deterministic jitter
                self._current_interval = new + jitter
            except Exception:
                pass
            self.last_error = str(e)
            logger.error(f"❌ Webhook health check timeout for {self.webhook_url}; increasing backoff to {self._current_interval}s: {e}")
            return False

        except aiohttp.ClientConnectorError as e:
            self.is_healthy = False
            self.failed_checks += 1
            self.consecutive_failures += 1
            try:
                old = getattr(self, "_current_interval", self.check_interval)
                new = min(old * 2, self._max_backoff)
                jitter = min(5, new * 0.2) * 0.5  # deterministic jitter
                self._current_interval = new + jitter
            except Exception:
                pass
            self.last_error = str(e)
            logger.error(f"❌ Webhook connection error for {self.webhook_url}: {e}; increasing backoff to {self._current_interval}s")
            return False

        except Exception as e:
            self.is_healthy = False
            self.failed_checks += 1
            self.consecutive_failures += 1
            try:
                old = getattr(self, "_current_interval", self.check_interval)
                new = min(old * 2, self._max_backoff)
                jitter = min(5, new * 0.2) * 0.5  # deterministic jitter
                self._current_interval = new + jitter
            except Exception:
                pass
            self.last_error = str(e)
            logger.error(f"❌ Webhook health check error for {self.webhook_url}: {e}; increasing backoff to {self._current_interval}s")
            return False

    async def start_monitoring(self):
        """Start continuous webhook monitoring."""
        logger.info(f"🔍 Starting webhook monitoring (interval: {self.check_interval}s)")

        self.monitor_task = asyncio.create_task(self._monitor_loop())

    async def _monitor_loop(self):
        """Main monitoring loop."""
        while True:
            try:
                await self.health_check()

                # Alert if too many consecutive failures
                if self.consecutive_failures >= 3:
                    logger.critical(
                        f"🚨 CRITICAL: Webhook has failed {self.consecutive_failures} consecutive checks! "
                        f"Total failures: {self.failed_checks}/{self.check_count}"
                    )

                # Wait before next check (supporting current backoff interval)
                wait = getattr(self, "_current_interval", self.check_interval)
                await asyncio.sleep(wait)

            except asyncio.CancelledError:
                logger.info("Webhook monitoring stopped")
                break

            except aiohttp.ClientError as e:
                # Any aiohttp client errors (DNS, SSL, etc.) should back off but keep running
                self.is_healthy = False
                self.failed_checks += 1
                self.consecutive_failures += 1
                try:
                    old = getattr(self, "_current_interval", self.check_interval)
                    new = min(old * 2, self._max_backoff)
                    jitter = min(5, new * 0.2) * 0.5  # deterministic jitter
                    self._current_interval = new + jitter
                except Exception:
                    pass
                self.last_error = str(e)
                logger.error(f"❌ Webhook client error for {self.webhook_url}: {e}; increasing backoff to {self._current_interval}s")
                # Sleep for the backoff interval before retrying
                await asyncio.sleep(getattr(self, "_current_interval", self.check_interval))

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.check_interval)

    async def stop_monitoring(self):
        """Stop webhook monitoring."""
        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.monitor_task

        logger.info("Webhook monitoring stopped")

    def get_status(self) -> dict:
        """Get current webhook status."""
        return {
            "healthy": self.is_healthy,
            "url": self.webhook_url,
            "last_check": self.last_check.isoformat() if self.last_check else None,
            "total_checks": self.check_count,
            "failed_checks": self.failed_checks,
            "consecutive_failures": self.consecutive_failures,
            "success_rate": (
                (self.check_count - self.failed_checks) / self.check_count * 100 if self.check_count > 0 else 0
            ),
        }

    async def wait_until_healthy(self, timeout: int = 60) -> bool:
        """
        Wait until webhook is healthy or timeout.

        Args:
            timeout: Maximum seconds to wait

        Returns:
            True if healthy, False if timeout
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.is_healthy:
                return True
            await asyncio.sleep(1)

        return False


class WebhookRecoveryManager:
    """Manage webhook recovery and automatic restart."""

    def __init__(self, bot_application, webhook_url: str):
        """
        Initialize recovery manager.

        Args:
            bot_application: The Application instance
            webhook_url: The webhook URL
        """
        self.application = bot_application
        self.webhook_url = webhook_url
        self.monitor = WebhookMonitor(webhook_url)
        self.recovery_attempts = 0
        self.max_recovery_attempts = 3

    async def start(self):
        """Start webhook monitoring and recovery."""
        await self.monitor.start_monitoring()
        logger.info("Webhook recovery manager started")

    async def check_and_recover(self) -> bool:
        """
        Check webhook health and recover if needed.

        Returns:
            True if webhook is healthy or recovered, False if recovery failed
        """
        if self.monitor.is_healthy:
            self.recovery_attempts = 0
            return True

        # Try to recover
        logger.warning(
            f"🔧 Attempting webhook recovery (attempt {self.recovery_attempts + 1}/{self.max_recovery_attempts})"
        )

        try:
            # Try to reset webhook
            await self.application.bot.set_webhook(
                url=self.webhook_url, allowed_updates=["message", "callback_query", "edited_message"]
            )

            self.recovery_attempts += 1

            # Wait for health check
            if await self.monitor.wait_until_healthy(timeout=30):
                logger.info("✅ Webhook recovered successfully")
                self.recovery_attempts = 0
                return True
            else:
                logger.warning("⚠️ Webhook recovery failed - still not responding")
                return False

        except Exception as e:
            logger.error(f"❌ Webhook recovery error: {e}")
            return False

    async def stop(self):
        """Stop recovery manager."""
        await self.monitor.stop_monitoring()
        logger.info("Webhook recovery manager stopped")

    def get_stats(self) -> dict:
        """Get recovery statistics."""
        status = self.monitor.get_status()
        status["recovery_attempts"] = self.recovery_attempts
        status["max_recovery_attempts"] = self.max_recovery_attempts
        return status
