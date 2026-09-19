"""Persisted sessions are read and written on the loop the Mongo client owns.

A Motor client is bound to the event loop it was created on. The session *read*
used to dispatch the query into a fresh thread with ``asyncio.run(...)`` - a loop
of its own - so the await failed with::

    RuntimeError: Task <Task pending ...> got Future <Future pending> attached to
    a different loop

``models.load_session`` swallowed that and returned ``None``, and the callback
answered "Session expired. Please send a file first." with the session sitting in
Mongo the whole time. These tests pin the read to the caller's loop, the bound on
a slow Mongo, and the write that follows the same rule.
"""

import asyncio
import inspect
import json
import os
import tempfile
import unittest
from types import MethodType
from unittest.mock import patch

from source_helpers import read_source

import handlers as handlers_module
from handlers import EnhancedMediaHandler

Handler = EnhancedMediaHandler


class _FakeModel:
    """A Mongo model whose session calls are recorded, and can fail or hang."""

    def __init__(self, session=None, error=None, delay=0.0):
        self.session = session
        self.error = error
        self.delay = delay
        self.load_calls: list[int] = []
        self.saved: list[tuple[int, dict]] = []

    async def load_session(self, user_id, phone=None):
        self.load_calls.append(user_id)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return self.session

    async def save_session(self, user_id, data):
        self.saved.append((user_id, data))
        if self.error is not None:
            raise self.error
        return True


class _SessionStub:
    """Just enough of the handler for the session helpers under test."""

    def __init__(self, store_dir, model=None):
        self._session_store_dir = str(store_dir)
        self.db_model = model
        self.user_sessions: dict = {}
        self._session_writes: set = set()

    def _file(self, tmp, user_id=7):
        return os.path.join(self._session_store_dir, f"session_{user_id}.json")


def _load(stub, user_id=7):
    return asyncio.run(stub._load_persisted_session(user_id))


def _with_session_helpers(stub):
    """Bind the real session helpers to the stub, as the handler defines them."""
    for name in ("_session_file", "_load_persisted_session", "_persist_session", "_schedule_session_save"):
        setattr(stub, name, MethodType(getattr(Handler, name), stub))
    return stub


class SessionLoadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _stub(self, model=None):
        return _with_session_helpers(_SessionStub(self.tmp.name, model))

    def test_the_loader_is_a_coroutine_awaited_by_its_caller(self):
        self.assertTrue(inspect.iscoroutinefunction(Handler._load_persisted_session))
        self.assertIn("await self._load_persisted_session(user_id)", read_source("handlers.py"))

    def test_the_read_happens_on_the_callers_loop(self):
        """The regression: a thread with ``asyncio.run`` gave the query another loop."""
        model = _FakeModel(session={"current_file": {"id": "abc"}})

        loaded = _load(self._stub(model))

        self.assertEqual(model.load_calls, [7])
        # The coroutine ran to completion on the loop it was called on: had it been
        # handed to a foreign loop, the returned value could never have arrived.
        self.assertEqual(loaded["current_file"], {"id": "abc"})

    def test_a_mongo_failure_returns_none_instead_of_raising(self):
        model = _FakeModel(error=RuntimeError("got Future <Future pending> attached to a different loop"))

        self.assertIsNone(_load(self._stub(model)))

    def test_an_empty_mongo_answer_is_no_session(self):
        self.assertIsNone(_load(self._stub(_FakeModel(session=None))))

    def test_a_session_from_mongo_keeps_the_lists_the_menus_index(self):
        loaded = _load(self._stub(_FakeModel(session={"current_file": None})))

        self.assertEqual(loaded["merge_list"], [])
        self.assertEqual(loaded["bulk_list"], [])

    def test_the_json_file_is_read_without_asking_mongo(self):
        model = _FakeModel(session={"current_file": "from-mongo"})
        stub = self._stub(model)
        with open(stub._file(self.tmp), "w", encoding="utf-8") as fh:
            json.dump({"current_file": "from-file"}, fh)

        loaded = _load(stub)

        self.assertEqual(loaded["current_file"], "from-file")
        self.assertEqual(model.load_calls, [], "the file was there; Mongo must not be queried")

    def test_a_corrupt_json_file_is_not_a_crash(self):
        stub = self._stub(_FakeModel(session={"current_file": "from-mongo"}))
        with open(stub._file(self.tmp), "w", encoding="utf-8") as fh:
            fh.write("{ not json")

        # The file is unreadable, so the read falls back to the database.
        loaded = _load(stub)
        self.assertEqual(loaded["current_file"], "from-mongo")


class SessionLoadTimeoutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.timeout_patch = patch.object(handlers_module, "_SESSION_LOAD_TIMEOUT_SECONDS", 0.05)
        self.timeout_patch.start()

    def tearDown(self):
        self.timeout_patch.stop()
        self.tmp.cleanup()

    def test_a_slow_mongo_is_abandoned_after_the_bound(self):
        model = _FakeModel(session={"current_file": "late"}, delay=5.0)
        stub = _with_session_helpers(_SessionStub(self.tmp.name, model))

        loop = asyncio.new_event_loop()
        try:
            started = loop.time()
            loaded = loop.run_until_complete(stub._load_persisted_session(7))
            elapsed = loop.time() - started
        finally:
            loop.close()

        self.assertIsNone(loaded)
        self.assertLess(elapsed, 1.0, "the read must not wait for a hanging Mongo")


class SessionSaveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _stub(self, model=None, current_file=None):
        stub = _with_session_helpers(_SessionStub(self.tmp.name, model))
        stub.user_sessions[7] = {"current_file": current_file, "merge_list": [], "bulk_list": []}
        return stub

    def test_the_write_is_scheduled_on_the_running_loop(self):
        model = _FakeModel()
        stub = self._stub(model, current_file={"id": "abc"})

        async def _run():
            stub._persist_session(7)
            # Let the scheduled task run on this same loop.
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        asyncio.run(_run())

        self.assertEqual(len(model.saved), 1, "the session was never written to Mongo")
        user_id, payload = model.saved[0]
        self.assertEqual(user_id, 7)
        self.assertEqual(payload["current_file"], {"id": "abc"})

    def test_the_file_is_written_even_with_no_running_loop(self):
        model = _FakeModel()
        stub = self._stub(model, current_file={"id": "abc"})

        # Called from a plain sync context: no loop, so Mongo is skipped - but the
        # JSON record still lands, which is what the next read will find.
        stub._persist_session(7)

        self.assertEqual(model.saved, [], "a foreign loop must not be created for this")
        with open(stub._file(self.tmp), encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)["current_file"], {"id": "abc"})

    def test_a_failing_session_save_is_not_fatal(self):
        model = _FakeModel(error=RuntimeError("mongo down"))
        stub = self._stub(model, current_file={"id": "abc"})

        async def _run():
            stub._persist_session(7)
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        asyncio.run(_run())  # must not raise

        self.assertEqual(len(model.saved), 1)


class NoForeignLoopTests(unittest.TestCase):
    """The pattern that caused it is gone from the module, not just unused."""

    def test_handlers_never_build_a_loop_of_its_own(self):
        """The calls are gone (the comments explaining why may still name them)."""
        src = read_source("handlers.py")

        for forbidden in (
            "run_until_complete(",
            "new_event_loop(",
            "asyncio.run(",
            "threading.Thread(",
            "threading.Thread(target",
            "import threading",
            "import queue",
        ):
            self.assertNotIn(forbidden, src, f"handlers.py still builds a loop of its own: {forbidden}")


if __name__ == "__main__":
    unittest.main()
