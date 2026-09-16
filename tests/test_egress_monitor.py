"""The egress watchdog: alert once per severity per billing cycle, and never do
anything worse than log when it cannot.

The account this exists for was suspended for exceeding a free-egress allowance,
so the cases that matter are: reaching the line does send a message, reaching it
again does *not*, an escalation is louder rather than silent, and a new billing
cycle starts clean. The failure cases matter just as much - a monitor that raises
into the bot, or that repeats itself until it is muted, is worse than none.
"""

import asyncio
import contextlib
import re

import pytest

from utils import egress_monitor


class _FakeBot:
    def __init__(self):
        self.sent: list[dict] = []
        self.fail_with: Exception | None = None

    async def send_message(self, **kwargs):
        if self.fail_with is not None:
            raise self.fail_with
        self.sent.append(kwargs)
        return object()


class _FakeApp:
    def __init__(self):
        self.bot = _FakeBot()


def _egress(status="watch", **overrides):
    """A reading shaped like ``utils.storage.egress_snapshot``'s."""
    row = {
        "period": "2026-09",
        "object_bytes": 2500 * 1024**2,
        "links_issued": 4,
        "stored_bytes": 1000 * 1024**2,
        "allowance_bytes": 3000 * 1024**2,
        "percent": 83.0,
        "status": status,
        "shared": True,
    }
    row.update(overrides)
    return row


@pytest.fixture(autouse=True)
def _no_redis(monkeypatch):
    """Default to Redis being unreachable: the monitor must still work."""

    async def _boom():
        raise RuntimeError("REDIS_URL is not set")

    monkeypatch.setattr("utils.job_queue.get_redis", _boom)


def _monitor(monkeypatch, egress, *, admin=7, app=None, interval=3600):
    """A monitor whose storage report is a fixed reading."""
    app = app or _FakeApp()

    async def _report(*args, **kwargs):
        return {"backend": "s3", "bytes": egress.get("stored_bytes"), "egress": egress}

    monkeypatch.setattr("utils.session_status.storage_egress_report", _report)
    monkeypatch.setattr(egress_monitor, "EGRESS_ALERT_MIN_INTERVAL", interval)
    return egress_monitor.EgressMonitor(admin_user_id=admin, bot_app=app), app


# ── the alert itself ────────────────────────────────────────────────────


def test_the_message_carries_the_numbers_an_operator_acts_on():
    text = egress_monitor.format_egress_alert(_egress())

    assert "2.4 GB" in text  # what left the bucket
    assert "2.9 GB" in text  # 3x of stored - the actual free allowance
    assert "83%" in text
    assert "2026-09" in text
    assert "Presigned links handed out: <b>4</b>" in text


def test_the_message_is_telegram_safe_html():
    text = egress_monitor.format_egress_alert(_egress())

    for tag in re.finditer(r"<[^>]*>?", text):
        assert re.fullmatch(r"</?(b|i|u|s|code|pre|a)>", tag.group(0)), tag.group(0)
    for entity in re.finditer(r"&[^;\s]*;?", text):
        assert re.fullmatch(r"&(amp|lt|gt|quot);", entity.group(0)), entity.group(0)


def test_over_gets_a_louder_message_than_watch():
    watch = egress_monitor.format_egress_alert(_egress("watch"))
    over = egress_monitor.format_egress_alert(_egress("over", percent=120.0))

    assert "approaching" in watch
    assert "passed" in over


def test_a_reading_without_an_allowance_still_renders():
    """No stored reading means no ratio - the message must not invent one."""
    text = egress_monitor.format_egress_alert(
        _egress(allowance_bytes=None, percent=None, stored_bytes=None)
    )

    # The egress line reports the bytes and stops there - no invented ratio and
    # no "?" standing in for an allowance that was never read.
    assert "• Egress: <b>2.4 GB</b>" in text
    assert " of " not in text.split("\n")[3]


# ── when it fires ───────────────────────────────────────────────────────


def test_crossing_the_watch_line_alerts_the_admin(monkeypatch):
    monitor, app = _monitor(monkeypatch, _egress())

    sent = asyncio.run(monitor.check_and_alert())

    assert sent is not None
    assert len(app.bot.sent) == 1
    assert app.bot.sent[0]["chat_id"] == 7
    assert app.bot.sent[0]["parse_mode"] == "HTML"


def test_the_same_severity_is_not_repeated_within_the_cycle(monkeypatch):
    monitor, app = _monitor(monkeypatch, _egress())

    asyncio.run(monitor.check_and_alert())
    again = asyncio.run(monitor.check_and_alert())

    assert again is None
    assert len(app.bot.sent) == 1


def test_escalating_to_over_is_announced(monkeypatch):
    monitor, app = _monitor(monkeypatch, _egress("watch"))
    asyncio.run(monitor.check_and_alert())

    # The same monitor now sees the cycle go over, well after the rate floor.
    monitor._last_alert_at -= 10 * 3600
    monitor.bot_app.bot.sent.clear()

    async def _report(*args, **kwargs):
        return {"egress": _egress("over", percent=120.0)}

    monkeypatch.setattr("utils.session_status.storage_egress_report", _report)

    sent = asyncio.run(monitor.check_and_alert())

    assert sent is not None
    assert len(app.bot.sent) == 1
    assert "passed" in app.bot.sent[0]["text"]


def test_a_new_billing_cycle_starts_clean(monkeypatch):
    monitor, app = _monitor(monkeypatch, _egress())
    asyncio.run(monitor.check_and_alert())
    monitor._last_alert_at -= 10 * 3600

    async def _report(*args, **kwargs):
        return {"egress": _egress(period="2026-10")}

    monkeypatch.setattr("utils.session_status.storage_egress_report", _report)

    assert asyncio.run(monitor.check_and_alert()) is not None
    assert len(app.bot.sent) == 2


def test_a_ratio_below_the_line_never_alerts(monkeypatch):
    monitor, app = _monitor(monkeypatch, _egress("ok", percent=20.0))

    assert asyncio.run(monitor.check_and_alert()) is None
    assert app.bot.sent == []


def test_an_unknown_rating_is_silent_rather_than_guessed(monkeypatch):
    """No allowance yet means there is no ratio to alert about."""
    monitor, app = _monitor(
        monkeypatch, _egress("unknown", allowance_bytes=None, percent=None, stored_bytes=None)
    )

    assert asyncio.run(monitor.check_and_alert()) is None
    assert app.bot.sent == []


def test_a_flapping_ratio_cannot_spam(monkeypatch):
    """A higher severity inside the rate floor waits rather than messages."""
    monitor, app = _monitor(monkeypatch, _egress("watch"))
    asyncio.run(monitor.check_and_alert())

    async def _report(*args, **kwargs):
        return {"egress": _egress("over", percent=120.0)}

    monkeypatch.setattr("utils.session_status.storage_egress_report", _report)

    # Straight away: rate-limited, and deliberately not recorded as announced.
    assert asyncio.run(monitor.check_and_alert()) is None
    assert len(app.bot.sent) == 1

    # Once the floor has passed it goes out.
    monitor._last_alert_at -= 10 * 3600
    assert asyncio.run(monitor.check_and_alert()) is not None
    assert len(app.bot.sent) == 2


# ── when it cannot do its job ───────────────────────────────────────────


def test_a_failed_send_is_not_recorded_so_it_can_be_retried(monkeypatch):
    monitor, app = _monitor(monkeypatch, _egress())
    app.bot.fail_with = RuntimeError("telegram is down")

    assert asyncio.run(monitor.check_and_alert()) is None

    app.bot.fail_with = None
    monitor._last_alert_at -= 10 * 3600

    assert asyncio.run(monitor.check_and_alert()) is not None
    assert len(app.bot.sent) == 1


def test_no_admin_configured_only_logs(monkeypatch):
    monitor, app = _monitor(monkeypatch, _egress(), admin=None)

    assert asyncio.run(monitor.check_and_alert()) is None
    assert app.bot.sent == []


def test_a_broken_report_does_not_escape(monkeypatch):
    async def _boom(*args, **kwargs):
        raise RuntimeError("storage backend exploded")

    monkeypatch.setattr("utils.session_status.storage_egress_report", _boom)
    monitor = egress_monitor.EgressMonitor(admin_user_id=7, bot_app=_FakeApp())

    assert asyncio.run(monitor.check_and_alert()) is None


def test_a_report_without_egress_does_not_escape(monkeypatch):
    async def _empty(*args, **kwargs):
        return {"backend": "s3"}

    monkeypatch.setattr("utils.session_status.storage_egress_report", _empty)
    monitor = egress_monitor.EgressMonitor(admin_user_id=7, bot_app=_FakeApp())

    assert asyncio.run(monitor.check_and_alert()) is None


def test_the_announced_state_is_shared_through_redis(monkeypatch):
    """A restart must not re-announce the alert the admin already has."""
    store: dict[str, str] = {}

    class _Client:
        async def get(self, key):
            return store.get(key)

        async def set(self, key, value, ex=None):
            store[key] = value

    async def _get_redis():
        return _Client()

    monkeypatch.setattr("utils.job_queue.get_redis", _get_redis)
    monitor, app = _monitor(monkeypatch, _egress())

    assert asyncio.run(monitor.check_and_alert()) is not None
    assert store[egress_monitor.EGRESS_ALERT_KEY] == "2026-09:1"

    # A fresh process (new instance, empty memory) reads the state back.
    restarted, restarted_app = _monitor(monkeypatch, _egress())
    assert asyncio.run(restarted.check_and_alert()) is None
    assert restarted_app.bot.sent == []


# ── lifecycle ───────────────────────────────────────────────────────────


def test_start_is_idempotent_and_stop_cancels(monkeypatch):
    monitor, _ = _monitor(monkeypatch, _egress())

    async def _scenario():
        first = monitor.start()
        assert monitor.start() is first
        monitor.stop()
        with contextlib.suppress(asyncio.CancelledError):
            await first
        assert first.cancelled()

    asyncio.run(_scenario())
