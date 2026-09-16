"""Tell the admin when storage egress is heading for the free-egress cap.

IDrive e2 grants free egress worth a multiple of what you store (3x), and billing
the overage is the *good* outcome - the account this was written for was
suspended for exceeding it. The dashboard already shows the ratio, but a
dashboard is only read by somebody who already suspects a problem, so this
raises it instead: the cycle's egress is checked on an interval and the admin is
messaged when the ratio enters ``watch``, and again if it escalates to ``over``.

The alert is raised once per severity per billing cycle. The severity already
announced is remembered under a key derived from the cycle's period, so a restart
re-announces at most once, a month boundary starts clean, and a ratio that flaps
across the line cannot turn into a stream of messages.

Nothing here is allowed to raise: this runs beside the bot, and a metering
problem must never be the thing that takes the bot down.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

logger = logging.getLogger(__name__)

# How often the ratio is checked. The bucket scan behind it is shared with the
# dashboard's cache, so a check is normally one Redis read.
EGRESS_CHECK_INTERVAL = int(os.getenv("EGRESS_CHECK_INTERVAL", "900"))
# A floor between two alerts, so a ratio that flaps across the line stays quiet.
EGRESS_ALERT_MIN_INTERVAL = float(os.getenv("EGRESS_ALERT_MIN_INTERVAL", "3600"))
# Where the last announced severity for a cycle is remembered.
EGRESS_ALERT_KEY = "storage:egress:alert_state"

# Ordered, so "have we already said something at least this urgent?" is just a
# comparison. "unknown" is deliberately absent: without a stored reading there is
# no ratio to alert about.
_SEVERITY = {"ok": 0, "watch": 1, "over": 2}

_HEADLINE = {
    "watch": "⚠️ <b>Storage egress is approaching the free allowance</b>",
    "over": "🔴 <b>Storage egress has passed the free allowance</b>",
}


def _bytes_human(value) -> str:
    """A byte count with a unit that fits the magnitude (``?`` when unknown)."""
    try:
        number = int(value)
    except (TypeError, ValueError):
        return "?"
    sign = "-" if number < 0 else ""
    magnitude = abs(number)
    for unit, factor in (("TB", 1024**4), ("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)):
        if magnitude >= factor:
            return f"{sign}{magnitude / factor:.1f} {unit}"
    return f"{sign}{magnitude} B"


def format_egress_alert(egress: dict) -> str:
    """The admin message for a cycle that has reached a threshold.

    HTML, like the dashboard it links back to, so the numbers render the same way
    in both places.
    """
    period = str(egress.get("period") or "this cycle")
    status = egress.get("status")
    pulled = _bytes_human(egress.get("object_bytes"))
    allowance = _bytes_human(egress.get("allowance_bytes"))
    stored = _bytes_human(egress.get("stored_bytes"))
    percent = egress.get("percent")

    lines = [_HEADLINE.get(status, _HEADLINE["watch"]), ""]
    lines.append(f"• Cycle: <b>{period}</b>")
    if isinstance(percent, (int, float)):
        lines.append(f"• Egress: <b>{pulled}</b> of <b>{allowance}</b> free (<b>{percent:.0f}%</b>)")
    else:
        lines.append(f"• Egress: <b>{pulled}</b>")

    links = egress.get("links_issued")
    try:
        if int(links or 0) > 0:
            lines.append(f"• Presigned links handed out: <b>{int(links)}</b> (each fetch is more egress)")
    except (TypeError, ValueError):
        pass

    if stored != "?":
        lines.append(f"• Stored, which sets the allowance: <b>{stored}</b>")

    lines.extend(
        [
            "",
            "The free allowance is a multiple of what you store, so it grows with "
            "stored bytes and shrinks when the bucket is emptied. Past 100% the "
            "provider bills for the overage - and can suspend the account.",
            "",
            "Run /session_status for the full breakdown.",
        ]
    )
    if not egress.get("shared"):
        lines.append("<i>Counted in this process only (Redis was unavailable).</i>")
    return "\n".join(lines)


class EgressMonitor:
    """Periodically rates egress against the allowance and alerts the admin."""

    def __init__(
        self,
        check_interval: int = EGRESS_CHECK_INTERVAL,
        admin_user_id: int | None = None,
        bot_app=None,
    ):
        self.check_interval = check_interval
        self.admin_user_id = admin_user_id
        self.bot_app = bot_app

        self.is_running = False
        self._task: asyncio.Task | None = None

        # Fallback memory for when Redis is unreachable: the worst it costs is a
        # repeated alert after a restart, which is better than a missed one.
        self._announced: dict[str, int] = {}
        self._last_alert_at: float = 0.0

    # ── lifecycle ───────────────────────────────────────────────────────

    def start(self) -> asyncio.Task:
        """Start the loop as a background task (idempotent)."""
        if self._task is not None and not self._task.done():
            logger.debug("EgressMonitor is already running")
            return self._task
        self.is_running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("EgressMonitor started (interval=%ss)", self.check_interval)
        return self._task

    def stop(self) -> None:
        """Signal the loop to stop."""
        self.is_running = False
        if self._task is not None and not self._task.done():
            self._task.cancel()
        logger.info("EgressMonitor stop requested")

    async def _run_loop(self) -> None:
        # A short delay so startup first: the bot has enough to do without a
        # bucket scan and a possible Telegram send in the middle of it.
        await asyncio.sleep(30)
        while self.is_running:
            try:
                await self.check_and_alert()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("EgressMonitor: check iteration failed")
            try:
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
        logger.info("EgressMonitor loop stopped")

    # ── the check ───────────────────────────────────────────────────────

    async def check_and_alert(self) -> dict | None:
        """Rate this cycle's egress and alert if it warrants one.

        Returns the egress reading when an alert was sent, else ``None`` - which
        is what the tests assert on. Never raises.
        """
        try:
            from utils.session_status import storage_egress_report

            report = await storage_egress_report()
        except Exception:
            logger.debug("EgressMonitor: could not read the storage report")
            return None

        egress = (report or {}).get("egress") if isinstance(report, dict) else None
        if not isinstance(egress, dict):
            logger.debug("EgressMonitor: the report carried no egress reading")
            return None

        status = egress.get("status")
        severity = _SEVERITY.get(str(status))
        if severity is None:
            # No stored reading means no allowance, so there is no ratio to alert
            # about - just record why, so a silent monitor is explainable.
            logger.debug("EgressMonitor: no rating yet (status=%s)", status)
            return None
        if not egress.get("allowance_bytes"):
            logger.debug("EgressMonitor: no allowance yet (status=%s)", status)
            return None

        period = str(egress.get("period") or "")
        announced = await self._announced_severity(period)
        if severity <= announced:
            return None
        if time.time() - self._last_alert_at < EGRESS_ALERT_MIN_INTERVAL:
            logger.debug(
                "EgressMonitor: %s in %s is not re-announced yet (rate-limited)",
                status,
                period,
            )
            return None

        if not await self._send(format_egress_alert(egress)):
            # Not announced, so a later check still gets its chance.
            return None

        await self._remember(period, severity)
        logger.warning("EgressMonitor: alerted admin that %s egress is %s", period, status)
        return egress

    # ── announced state ─────────────────────────────────────────────────

    async def _announced_severity(self, period: str) -> int:
        """The worst severity already announced for *period*.

        Read from Redis so a restart does not repeat the alert; the process-local
        copy covers Redis being down.
        """
        worst = self._announced.get(period, 0)
        try:
            from utils.job_queue import get_redis

            client = await get_redis()
            raw = await client.get(EGRESS_ALERT_KEY)
        except Exception:
            logger.debug("EgressMonitor: could not read the announced state")
            return worst

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "replace")
        value = str(raw or "")
        # "<period>:<severity>"; anything else (or another cycle) means nothing
        # has been announced for this one.
        stored_period, _, stored_severity = value.partition(":")
        if stored_period != period:
            return worst
        try:
            return max(worst, int(stored_severity))
        except ValueError:
            return worst

    async def _remember(self, period: str, severity: int) -> None:
        """Record the worst severity announced for *period*. Best-effort."""
        self._announced[period] = max(self._announced.get(period, 0), severity)
        # A cycle's memory is only needed for that cycle.
        if len(self._announced) > 3:
            for stale in sorted(self._announced)[:-3]:
                self._announced.pop(stale, None)

        try:
            from utils.job_queue import get_redis

            client = await get_redis()
            await client.set(EGRESS_ALERT_KEY, f"{period}:{severity}", ex=60 * 60 * 24 * 45)
        except Exception:
            logger.debug("EgressMonitor: could not persist the announced state")

    # ── delivery ────────────────────────────────────────────────────────

    async def _send(self, text: str) -> bool:
        """Send *text* to the admin. Returns whether it went out."""
        if not self.admin_user_id:
            logger.warning("EgressMonitor: no admin_user_id configured; alert not sent")
            return False
        if self.bot_app is None:
            logger.warning("EgressMonitor: no bot application available; alert not sent")
            return False

        try:
            await self.bot_app.bot.send_message(
                chat_id=self.admin_user_id,
                text=text,
                parse_mode="HTML",
            )
        except Exception as exc:
            logger.warning("EgressMonitor: failed to send the alert: %s", exc)
            return False

        self._last_alert_at = time.time()
        return True


_monitor: EgressMonitor | None = None


def get_egress_monitor() -> EgressMonitor:
    """The process-wide monitor, created on first use."""
    global _monitor
    if _monitor is None:
        _monitor = EgressMonitor()
    return _monitor


def start_egress_monitor(
    admin_user_id: int | None = None,
    bot_app=None,
    check_interval: int | None = None,
) -> asyncio.Task:
    """Configure and start the monitor. Returns the background task."""
    monitor = get_egress_monitor()
    if admin_user_id is not None:
        monitor.admin_user_id = admin_user_id
    if bot_app is not None:
        monitor.bot_app = bot_app
    if check_interval is not None:
        monitor.check_interval = check_interval
    return monitor.start()


def stop_egress_monitor() -> None:
    """Stop the monitor loop."""
    get_egress_monitor().stop()
