# utils/webhook_monitor.py
"""
Webhook heartbeat monitoring for bot reliability.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Dict

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

    async def health_check(self) -> bool:
        """
        Perform a health check on the webhook.

        Returns:
            True if webhook is healthy, False otherwise
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.head(self.webhook_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    is_healthy = response.status in [200, 404, 405]  # Accept various responses

                    self.check_count += 1
                    self.last_check = datetime.now()

                    if is_healthy:
                        self.is_healthy = True
                        self.consecutive_failures = 0
                        logger.debug(f"✅ Webhook health check passed (status: {response.status})")
                        return True
                    else:
                        self.is_healthy = False
                        self.failed_checks += 1
                        self.consecutive_failures += 1
                        logger.warning(f"⚠️ Webhook health check failed (status: {response.status})")
                        return False

        except asyncio.TimeoutError:
            self.is_healthy = False
            self.failed_checks += 1
            self.consecutive_failures += 1
            logger.error("❌ Webhook health check timeout")
            return False

        except aiohttp.ClientConnectorError as e:
            self.is_healthy = False
            self.failed_checks += 1
            self.consecutive_failures += 1
            logger.error(f"❌ Webhook connection error: {e}")
            return False

        except Exception as e:
            self.is_healthy = False
            self.failed_checks += 1
            self.consecutive_failures += 1
            logger.error(f"❌ Webhook health check error: {e}")
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

                # Wait before next check
                await asyncio.sleep(self.check_interval)

            except asyncio.CancelledError:
                logger.info("Webhook monitoring stopped")
                break

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.check_interval)

    async def stop_monitoring(self):
        """Stop webhook monitoring."""
        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass

        logger.info("Webhook monitoring stopped")

    def get_status(self) -> Dict:
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

    def get_stats(self) -> Dict:
        """Get recovery statistics."""
        status = self.monitor.get_status()
        status["recovery_attempts"] = self.recovery_attempts
        status["max_recovery_attempts"] = self.max_recovery_attempts
        return status
