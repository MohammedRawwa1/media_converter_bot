"""The Flask uploader runs its async work on the loop that owns the clients.

Flask is mounted inside FastAPI, so its routes are served by WSGI worker threads
that own no event loop - while every async client they touch (the queue's Redis
client, Motor in job_store, the Kafka/RabbitMQ adapters) is created on the loop the
ASGI app runs and binds to it.  Running a request's coroutines on a *second* loop
fails the way an unreadable session did - "got Future <Future pending> attached to
a different loop" - and because save_job/emit_event are best-effort, the write was
dropped without a trace.

These tests pin where `webapp._run_async` sends the work: to the registered app
loop when there is one, and otherwise to the persistent per-thread loop.
"""

import ast
import asyncio
import threading
import time
import unittest
from unittest.mock import patch

from source_helpers import find_function, parse_source, read_source

from web import webapp


async def _which_loop() -> int:
    """The identity of the loop a coroutine is actually running on."""
    return id(asyncio.get_running_loop())


class _AppLoopThread:
    """A loop running in its own thread, the way the ASGI app runs one."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        # `is_running()` is what `_run_async` checks before submitting, so wait for
        # the thread to actually reach run_forever instead of racing it.
        deadline = time.time() + 5
        while not self.loop.is_running() and time.time() < deadline:
            time.sleep(0.001)
        if not self.loop.is_running():
            raise AssertionError("the test's app loop never started")

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=10)
        self.loop.close()


class RunAsyncOnTheAppLoopTests(unittest.TestCase):
    def setUp(self):
        self._previous = webapp._app_loop
        webapp.set_app_loop(None)

    def tearDown(self):
        webapp.set_app_loop(self._previous)

    # ── the bug ─────────────────────────────────────────────────────────────

    def test_work_is_submitted_to_the_registered_app_loop(self):
        """A request coroutine must run where the clients were created."""
        app = _AppLoopThread()
        try:
            webapp.set_app_loop(app.loop)

            used = webapp._run_async(_which_loop())

            self.assertEqual(used, id(app.loop), "the request ran on a second loop")
        finally:
            app.stop()

    def test_the_callers_exception_reaches_it(self):
        async def _boom():
            raise ValueError("no such job")

        app = _AppLoopThread()
        try:
            webapp.set_app_loop(app.loop)

            with self.assertRaises(ValueError):
                webapp._run_async(_boom())
        finally:
            app.stop()

    def test_a_job_write_from_a_request_loop_is_the_app_loop(self):
        """The shape of the real path: enqueue_job -> save_job/emit_event.

        `_run_async` has to hand the coroutine to the loop that owns Motor and the
        Kafka producer, which is the whole point of registering it.
        """
        seen: list[int] = []

        async def _enqueue():
            seen.append(await _nested_identity())

        async def _nested_identity():
            return await _which_loop()

        app = _AppLoopThread()
        try:
            webapp.set_app_loop(app.loop)

            webapp._run_async(_enqueue())
        finally:
            app.stop()

        self.assertEqual(seen, [id(app.loop)])

    # ── what must stay true ─────────────────────────────────────────────────

    def test_without_an_app_loop_a_thread_still_gets_a_usable_loop(self):
        """A process that never runs the ASGI app serves requests all the same."""
        first = webapp._run_async(_which_loop())
        second = webapp._run_async(_which_loop())

        self.assertEqual(first, second, "the per-thread loop must be reused")
        self.assertEqual(first, id(webapp._ensure_loop()))

    def test_an_unregistered_loop_is_never_the_answer(self):
        app = _AppLoopThread()
        try:
            webapp.set_app_loop(None)

            used = webapp._run_async(_which_loop())

            self.assertNotEqual(used, id(app.loop))
        finally:
            app.stop()

    def test_a_loop_that_is_not_running_is_not_used(self):
        idle = asyncio.new_event_loop()
        try:
            webapp.set_app_loop(idle)

            used = webapp._run_async(_which_loop())
        finally:
            idle.close()

        self.assertNotEqual(used, id(idle))

    def test_a_closed_loop_is_not_used(self):
        closed = asyncio.new_event_loop()
        closed.close()
        webapp.set_app_loop(closed)

        used = webapp._run_async(_which_loop())

        self.assertNotEqual(used, id(closed))

    def test_a_request_on_the_app_loops_own_thread_does_not_deadlock(self):
        """It cannot await itself: `run_coroutine_threadsafe(...).result()` hangs."""
        app = _AppLoopThread()
        try:
            webapp.set_app_loop(app.loop)
            outcome: list[str] = []
            finished = threading.Event()

            def _from_the_loop_thread():
                coro = _which_loop()
                try:
                    webapp._run_async(coro)
                    outcome.append("returned")
                except Exception as exc:  # noqa: BLE001 - any loud failure beats a hang
                    outcome.append(type(exc).__name__)
                finally:
                    # Nothing awaited it, and an unawaited coroutine warns on GC.
                    coro.close()
                    finished.set()

            app.loop.call_soon_threadsafe(_from_the_loop_thread)
            self.assertTrue(
                finished.wait(timeout=10),
                "a request submitted from the loop's own thread deadlocked",
            )
        finally:
            app.stop()

        self.assertEqual(outcome, ["RuntimeError"])

    def test_a_wedged_loop_fails_the_request_instead_of_hanging(self):
        """A blocked loop must not hold the worker thread for ever."""
        app = _AppLoopThread()
        try:
            webapp.set_app_loop(app.loop)

            async def _never_answered():
                return True

            with patch.object(webapp, "_APP_LOOP_TIMEOUT_SECONDS", 0.05):
                # A blocking callback keeps the loop from reaching the coroutine.
                app.loop.call_soon_threadsafe(time.sleep, 0.4)
                with self.assertRaises(TimeoutError):
                    webapp._run_async(_never_answered())
                # Let the loop drain the blocked callback and the queued coroutine.
                time.sleep(0.6)
        finally:
            app.stop()


class OneLoopForTheUploaderSourceTests(unittest.TestCase):
    def test_the_module_has_no_second_loop_mechanism(self):
        src = read_source("web", "webapp.py")

        self.assertNotIn("_asyncio", src, "the asyncio.run alias is back")
        self.assertIn("asyncio.run_coroutine_threadsafe", src)

    def test_the_only_coroutine_run_on_a_private_loop_is_the_ffmpeg_runner(self):
        """`run_ffmpeg` owns its Redis client, so its own loop is self-consistent.

        Everything else has to reach a shared client (queue Redis, Motor, Kafka),
        and those belong to the app loop.
        """
        tree = parse_source("web", "webapp.py")
        run_calls = [
            ast.unparse(node.args[0])
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "run"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "asyncio"
            and node.args
        ]

        self.assertEqual(
            [call.split("(")[0] for call in run_calls],
            ["run_ffmpeg"],
            "a coroutine that touches a shared client is running on a private loop",
        )

    def test_the_app_loop_is_registered_at_startup_and_cleared_at_shutdown(self):
        tree = parse_source("main.py")

        startup = ast.unparse(find_function(tree, "_start_bot_background"))
        shutdown = ast.unparse(find_function(tree, "_stop_bot_background"))

        self.assertIn("set_app_loop(asyncio.get_running_loop())", startup)
        self.assertIn("set_app_loop(None)", shutdown)


if __name__ == "__main__":
    unittest.main()
