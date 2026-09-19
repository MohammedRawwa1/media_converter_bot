"""The S3 backend's client comes from a session built on the calling loop.

aioboto3 keeps loop-affine state (the credential refresh lock, the connector
behind each client), so one session shared by every loop in the process fails the
same way a shared Redis client does once the Flask uploader's per-thread loop or
a worker loop uses it - "got Future <Future pending> attached to a different
loop". `S3AsyncBackend._session` is therefore a per-loop holder, and these tests
pin that with a fake `aioboto3` that records the loop each session was built on.
"""

import asyncio
import threading
import unittest
from unittest.mock import patch

from utils import storage


class _FakeSession:
    """Records the loop it was constructed on and the loop that asked for a client."""

    def __init__(self):
        self.built_on = _running_loop_identity()
        self.client_asked_from = []

    def client(self, *args, **kwargs):
        self.client_asked_from.append(_running_loop_identity())
        return _FakeClientContext(args, kwargs)


class _FakeClientContext:
    def __init__(self, args, kwargs):
        self.args = args
        self.kwargs = kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _FakeAioboto3:
    """Stands in for the aioboto3 module: ``Session()`` is what the backend calls."""

    def __init__(self):
        self.sessions: list[_FakeSession] = []

    def Session(self):  # noqa: N802 - mirrors the real module's name
        session = _FakeSession()
        self.sessions.append(session)
        return session


def _running_loop_identity():
    try:
        return id(asyncio.get_running_loop())
    except RuntimeError:
        return None


class PerLoopS3SessionTests(unittest.TestCase):
    def setUp(self):
        self.fake = _FakeAioboto3()
        self.patch = patch.object(storage, "aioboto3", self.fake)
        self.patch.start()
        self.backend = storage.S3AsyncBackend(bucket="a-bucket", endpoint_url="https://example.invalid")

    def tearDown(self):
        self.patch.stop()

    def test_aioboto3_is_used_and_looked_up_per_call(self):
        self.assertTrue(self.backend._use_aioboto3)
        self.assertIsInstance(self.backend._session, storage._PerLoopS3Session)
        # No session exists until a loop asks for a client.
        self.assertEqual(self.fake.sessions, [])

    def test_two_loops_get_two_sessions_built_on_the_loop_that_used_them(self):
        async def _ask():
            async with self.backend._session.client("s3"):
                return _running_loop_identity()

        first_loop = asyncio.new_event_loop()
        second_loop = asyncio.new_event_loop()
        try:
            used_first = first_loop.run_until_complete(_ask())
            used_second = second_loop.run_until_complete(_ask())
        finally:
            first_loop.close()
            second_loop.close()

        self.assertEqual(len(self.fake.sessions), 2, "one session was shared by both loops")
        built_first, built_second = self.fake.sessions
        self.assertEqual(built_first.built_on, used_first)
        self.assertEqual(built_second.built_on, used_second)
        self.assertNotEqual(built_first.built_on, built_second.built_on)

    def test_one_loop_reuses_its_session(self):
        async def _ask_twice():
            async with self.backend._session.client("s3"):
                pass
            async with self.backend._session.client("s3"):
                pass

        asyncio.run(_ask_twice())

        self.assertEqual(len(self.fake.sessions), 1, "a loop must not rebuild the session per call")

    def test_a_closed_loops_session_is_dropped(self):
        async def _ask():
            async with self.backend._session.client("s3"):
                pass

        loop = asyncio.new_event_loop()
        loop.run_until_complete(_ask())
        loop.close()
        self.assertEqual(len(self.backend._session._sessions), 1)

        asyncio.run(_ask())

        self.assertEqual(len(self.backend._session._sessions), 1, "the closed loop's session was kept")
        self.assertEqual(len(self.fake.sessions), 2)

    def test_two_threads_with_their_own_loops_do_not_share_a_session(self):
        """The web uploader's shape: one loop per WSGI thread."""
        seen: dict[str, object] = {}

        def _worker(name: str) -> None:
            loop = asyncio.new_event_loop()
            try:

                async def _ask():
                    async with self.backend._session.client("s3"):
                        return _running_loop_identity()

                seen[name] = loop.run_until_complete(_ask())
            finally:
                loop.close()

        threads = [threading.Thread(target=_worker, args=(name,)) for name in ("a", "b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(set(seen), {"a", "b"})
        self.assertNotEqual(seen["a"], seen["b"])
        self.assertEqual(
            sorted(session.built_on for session in self.fake.sessions),
            sorted([seen["a"], seen["b"]]),
            "each session must be built on the loop that used it",
        )

    def test_the_client_arguments_reach_the_session_untouched(self):
        async def _ask():
            async with self.backend._session.client("s3", region_name="eu-west-1") as client:
                return client.kwargs

        kwargs = asyncio.run(_ask())

        self.assertEqual(kwargs, {"region_name": "eu-west-1"})
        session = self.fake.sessions[0]
        # `client()` stays a plain call returning the context manager, asked from
        # the very loop the session was built on.
        self.assertEqual(session.client_asked_from, [session.built_on])


class Boto3FallbackTests(unittest.TestCase):
    """Without aioboto3 there is no session at all to share or to break."""

    def test_no_aioboto3_means_no_session(self):
        with patch.object(storage, "aioboto3", None):
            backend = storage.S3AsyncBackend(bucket="a-bucket", endpoint_url="https://example.invalid")

        self.assertFalse(backend._use_aioboto3)
        self.assertIsNone(backend._session)


class OneSessionSourceTests(unittest.TestCase):
    def test_a_session_is_never_built_outside_the_holder(self):
        from source_helpers import read_source

        src = read_source("utils", "storage.py")

        # The eager, loop-agnostic construction is what must not come back: the
        # factory is handed to the holder and called per loop instead.
        self.assertEqual(src.count("aioboto3.Session()"), 0, "an eager session is back")
        self.assertIn("_PerLoopS3Session(aioboto3.Session)", src)
        self.assertIn("self._factory()", src)


if __name__ == "__main__":
    unittest.main()
