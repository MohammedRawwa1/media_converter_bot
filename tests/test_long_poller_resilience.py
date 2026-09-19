"""Regression tests for the long-poller's lifecycle.

Three failures live in this area, all of which left the bot answering HTTP 200
while receiving nothing:

* the ASGI startup handler probed ``BOT_APPLICATION`` before ``main()`` had run,
  so it always read ``dispatcher_present=False``, started a fallback poller, and
  left the poller ``main()`` was about to create unstarted;
* a ``409 Conflict`` (normal during a rolling deploy) made that fallback poller
  ``break`` permanently, leaving no update consumer at all;
* nothing watched the consumer, and the Redis lock admitting one poller at a time
  was re-acquired against itself, so the other poller slept forever without ever
  polling.

These tests drive the real helpers with a stub lock so the behaviour is pinned,
not just the source text.
"""

import ast
import asyncio
import contextlib
import os
import time
import unittest
from unittest.mock import patch

from source_helpers import find_function, parse_source, read_source

import main as main_module


class _FakeLock:
    """Minimal stand-in for ``utils.redis_lock.RedisLock``."""

    def __init__(self, *, acquired=False, grant=True):
        self.is_acquired = acquired
        self._grant = grant
        self.acquire_calls = 0
        self.release_calls = 0
        self.renew_calls = 0

    async def acquire(self):
        self.acquire_calls += 1
        if self._grant:
            self.is_acquired = True
        return self._grant

    async def release(self):
        self.release_calls += 1
        self.is_acquired = False
        return True

    async def renew(self):
        self.renew_calls += 1
        return True


def _conflict_handler(tree: ast.Module, function: str) -> ast.ExceptHandler:
    """The ``except Conflict`` handler inside ``function``."""
    node = find_function(tree, function)
    for child in ast.walk(node):
        if not isinstance(child, ast.ExceptHandler):
            continue
        exc_type = child.type
        if isinstance(exc_type, ast.Name) and exc_type.id == "Conflict":
            return child
    raise AssertionError(f"no `except Conflict` handler in {function!r}")


class PollerConflictTests(unittest.TestCase):
    """The poller - whichever path started it - must survive a transient conflict."""

    @classmethod
    def setUpClass(cls):
        cls.tree = parse_source("main.py")

    def test_conflict_is_retried_and_never_ends_the_poller(self):
        handler = _conflict_handler(self.tree, "_long_poll_forever")

        self.assertEqual(
            [node for node in ast.walk(handler) if isinstance(node, ast.Break)],
            [],
            "a conflict must not break out of the poller: that removes the only "
            "update consumer and leaves the bot silent",
        )
        self.assertTrue(
            [node for node in ast.walk(handler) if isinstance(node, ast.Continue)],
            "the conflict handler must continue the polling loop",
        )

    def test_conflict_backs_off_before_retrying(self):
        handler = _conflict_handler(self.tree, "_long_poll_forever")

        calls = [node for node in ast.walk(handler) if isinstance(node, ast.Call)]
        self.assertTrue(
            any(isinstance(call.func, ast.Attribute) and call.func.attr == "sleep" for call in calls),
            "the conflict handler must back off before retrying getUpdates",
        )


class SinglePollerLoopTests(unittest.TestCase):
    """There is one getUpdates loop, shared by every path that polls.

    The resilience above used to be duplicated: it was found and fixed in the ASGI
    fallback first and only then in main()'s copy. One function is what keeps the
    next fix from landing in one path and missing the other.
    """

    @classmethod
    def setUpClass(cls):
        cls.src = read_source("main.py")
        cls.tree = parse_source("main.py")

    def _get_updates_calls(self) -> list[ast.Await]:
        """Every ``await <bot>.get_updates(...)`` in the module."""
        return [
            node
            for node in ast.walk(self.tree)
            if isinstance(node, ast.Await)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute)
            and node.value.func.attr == "get_updates"
        ]

    def test_only_one_loop_calls_get_updates(self):
        calls = self._get_updates_calls()

        self.assertEqual(len(calls), 2, "the dedicated-client and bot paths, both in the shared loop")
        loop = find_function(self.tree, "_long_poll_forever")
        for call in calls:
            self.assertIn(call, list(ast.walk(loop)), "getUpdates must only ever be awaited in the shared loop")

    def test_no_per_path_poller_closures_remain(self):
        self.assertNotIn("_longpoll_loop", self.src)
        self.assertNotIn("_asgi_longpoll_loop", self.src)

    def test_both_paths_start_the_shared_loop(self):
        self.assertIn('start_long_poller("main", bot=application.bot)', self.src)
        self.assertIn("start_long_poller(", self.src)
        self.assertIn('"ASGI fallback", ready_timeout=ASGI_BOT_SETTLE_TIMEOUT', self.src)

    def test_starting_twice_is_a_no_op(self):
        with patch.dict(main_module.__dict__, {"LONG_POLLER_STARTED": True}):
            self.assertIsNone(
                main_module.start_long_poller("test"),
                "a second caller must get None instead of a competing poller",
            )

    def test_watchdog_restart_reuses_the_original_source(self):
        started = []

        async def _stub(source, *, ready_timeout=None, bot=None):
            started.append(source)

        async def _scenario():
            with (
                patch.dict(main_module.__dict__, {"LONG_POLLER_STARTED": False, "LONG_POLLER_SOURCE": "main"}),
                patch.object(main_module, "_long_poll_forever", _stub),
            ):
                task = main_module.restart_long_poller()
                await task

        asyncio.run(_scenario())

        self.assertEqual(started, ["main"], "a restart must keep polling as the path that owned it")


class SettleBeforeFallbackTests(unittest.TestCase):
    """The fallback decision must wait for main() instead of guessing."""

    @classmethod
    def setUpClass(cls):
        cls.src = read_source("main.py")

    def test_startup_waits_for_the_polling_decision(self):
        self.assertIn("BOT_POLLING_DECIDED", self.src)
        self.assertIn("await asyncio.wait_for(BOT_POLLING_DECIDED.wait(), timeout=ASGI_BOT_SETTLE_TIMEOUT)", self.src)

    def test_fallback_is_gated_on_nothing_already_polling(self):
        self.assertIn('already_polling = bool(globals().get("LONG_POLLER_STARTED", False))', self.src)
        self.assertIn("needs_fallback = not already_polling and", self.src)

    def test_main_records_the_decision(self):
        # Once for the ASGI/background path, once per branch of the direct path.
        self.assertGreaterEqual(
            self.src.count("BOT_POLLING_DECIDED.set()"),
            3,
            "every path that settles an update consumer must signal it",
        )


class PollerLockTests(unittest.TestCase):
    """One poller at a time, and never re-acquire against our own lock."""

    def setUp(self):
        self._saved = os.environ.pop("FORCE_POLLING", None)

    def tearDown(self):
        if self._saved is not None:
            os.environ["FORCE_POLLING"] = self._saved
        else:
            os.environ.pop("FORCE_POLLING", None)

    def test_holding_the_lock_skips_the_reacquire(self):
        lock = _FakeLock(acquired=True)

        granted = asyncio.run(main_module._acquire_long_poller_lock(lock))

        self.assertTrue(granted)
        self.assertEqual(lock.acquire_calls, 0, "SET NX against our own key would fail")

    def test_a_granted_lock_is_renewed(self):
        lock = _FakeLock(acquired=False, grant=True)

        granted = asyncio.run(main_module._acquire_long_poller_lock(lock))

        self.assertTrue(granted)
        self.assertEqual(lock.acquire_calls, 1)
        self.assertEqual(lock.renew_calls, 1)

    def test_losing_the_lock_is_recorded_as_proof_of_life(self):
        lock = _FakeLock(acquired=False, grant=False)
        with patch.dict(main_module.__dict__, {"LONG_POLLER_DEFERRED_AT": 0.0}):
            granted = asyncio.run(main_module._acquire_long_poller_lock(lock))
            deferred = main_module.LONG_POLLER_DEFERRED_AT

        self.assertFalse(granted, "a peer holding the lock means this process must not poll")
        self.assertGreater(deferred, 0.0, "a peer's polling is proof the consumer is alive")

    def test_deferral_keeps_the_consumer_looking_healthy(self):
        with patch.dict(
            main_module.__dict__,
            {"LONG_POLLER_HEARTBEAT": 0.0, "LONG_POLLER_DEFERRED_AT": 0.0},
        ):
            self.assertIsNone(main_module.long_poller_idle_seconds())
            main_module.LONG_POLLER_DEFERRED_AT = time.time()
            idle = main_module.long_poller_idle_seconds()

        self.assertIsNotNone(idle)
        self.assertLess(idle, 5.0, "a fresh deferral stamp must count as alive")

    def test_the_shared_loop_takes_the_shared_lock(self):
        src = read_source("main.py")

        self.assertIn('LONG_POLLER_LOCK_NAME = "longpoller"', src)
        self.assertEqual(
            src.count("await _long_poller_lock()"),
            1,
            "every poller must go through the one loop, which takes the shared lock",
        )


class StalenessVerdictTests(unittest.TestCase):
    """``long_poller_is_stale`` is the single verdict behind health and recovery."""

    def setUp(self):
        self._saved = os.environ.pop("FORCE_POLLING", None)

    def tearDown(self):
        if self._saved is not None:
            os.environ["FORCE_POLLING"] = self._saved
        else:
            os.environ.pop("FORCE_POLLING", None)

    def test_webhook_mode_is_never_stale(self):
        # A webhook deployment has no poller to be stale; nothing may fail it.
        with (
            patch.object(main_module, "WEBHOOK_URL", "https://example.test/hook"),
            patch.object(main_module, "LONG_POLLER_HEARTBEAT", 0.0),
            patch.object(main_module, "LONG_POLLER_DEFERRED_AT", 0.0),
        ):
            self.assertFalse(main_module.polling_is_expected())
            self.assertFalse(main_module.long_poller_is_stale())

    def test_polling_with_a_warm_heartbeat_is_healthy(self):
        with (
            patch.object(main_module, "WEBHOOK_URL", ""),
            patch.object(main_module, "LONG_POLLER_HEARTBEAT", time.time()),
        ):
            self.assertTrue(main_module.polling_is_expected())
            self.assertFalse(main_module.long_poller_is_stale())

    def test_polling_with_a_cold_heartbeat_is_stale(self):
        with (
            patch.object(main_module, "WEBHOOK_URL", ""),
            patch.object(main_module, "LONG_POLLER_HEARTBEAT", time.time() - 10_000),
            patch.object(main_module, "LONG_POLLER_DEFERRED_AT", 0.0),
        ):
            self.assertTrue(main_module.long_poller_is_stale())

    def test_health_uses_the_verdict_and_can_answer_503(self):
        src = read_source("main.py")

        self.assertIn("if long_poller_is_stale():", src)
        self.assertIn("JSONResponse(status_code=503", src)


class WatchdogTests(unittest.TestCase):
    """The watchdog restarts the consumer instead of waiting for a redeploy."""

    def test_stale_consumer_is_restarted(self):
        calls = []

        def _restart():
            calls.append(time.time())
            return None  # pretend a poller task is already running

        async def _scenario():
            with (
                patch.object(main_module, "LONG_POLLER_WATCHDOG_INTERVAL", 0),
                patch.object(main_module, "long_poller_is_stale", lambda: True),
                patch.dict(main_module.__dict__, {"LONG_POLLER_RESTART": _restart}),
            ):
                task = asyncio.create_task(main_module.long_poller_watchdog())
                await asyncio.sleep(0.05)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        asyncio.run(_scenario())

        self.assertTrue(calls, "the watchdog must call the registered restart hook")

    def test_healthy_consumer_is_left_alone(self):
        calls = []

        async def _scenario():
            with (
                patch.object(main_module, "LONG_POLLER_WATCHDOG_INTERVAL", 0),
                patch.object(main_module, "long_poller_is_stale", lambda: False),
                patch.dict(main_module.__dict__, {"LONG_POLLER_RESTART": lambda: calls.append(1)}),
            ):
                task = asyncio.create_task(main_module.long_poller_watchdog())
                await asyncio.sleep(0.05)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        asyncio.run(_scenario())

        self.assertEqual(calls, [], "a healthy poller must not be restarted")


if __name__ == "__main__":
    unittest.main()
