"""Redis clients belong to the loop that uses them.

redis.asyncio binds a connection to the event loop that opened it. The job queue
used to keep *one* client for the whole process, so the second loop to touch it -
the Flask uploader's per-WSGI-thread loop, or a short-lived loop in a thread -
failed with "got Future <Future pending> attached to a different loop" while the
bot's own loop kept working. `get_redis()` now hands each loop its own client.

These tests stand in a fake `redis.asyncio` for the real one, so no server is
needed to see which loop got which client.
"""

import asyncio
import os
import threading
import unittest
from unittest.mock import patch

from utils import job_queue as jq

REDIS_URL = "redis://localhost:6379/0"


class _FakeClient:
    def __init__(self, url, **kwargs):
        self.url = url
        self.kwargs = kwargs
        self.closed = False

    async def aclose(self):
        self.closed = True

    async def hgetall(self, _key):
        return {}


class _FakeAioredis:
    """A ``redis.asyncio`` module: ``from_url`` records every client it builds."""

    def __init__(self):
        self.created: list[_FakeClient] = []

    def from_url(self, url, **kwargs):
        client = _FakeClient(url, **kwargs)
        self.created.append(client)
        return client


class RedisPerLoopTests(unittest.TestCase):
    def setUp(self):
        jq._redis_clients.clear()
        self.fake = _FakeAioredis()
        self.env = patch.dict(os.environ, {"REDIS_URL": REDIS_URL})
        self.module = patch.object(jq, "aioredis", self.fake)
        self.env.start()
        self.module.start()

    def tearDown(self):
        self.module.stop()
        self.env.stop()
        jq._redis_clients.clear()

    # ── the bug ─────────────────────────────────────────────────────────────

    def test_a_second_loop_gets_its_own_client(self):
        """The regression: the second loop was handed the first loop's client."""
        first = asyncio.run(jq.get_redis())
        second = asyncio.run(jq.get_redis())

        self.assertIsNot(first, second, "two loops shared one client")
        self.assertEqual(len(self.fake.created), 2)

    def test_two_threads_with_their_own_loops_each_get_a_client(self):
        """Exactly the web uploader's shape: one loop per WSGI thread."""
        got: dict[str, object] = {}

        def _worker(name: str) -> None:
            loop = asyncio.new_event_loop()
            try:
                got[name] = loop.run_until_complete(jq.get_redis())
            finally:
                loop.close()

        threads = [threading.Thread(target=_worker, args=(name,)) for name in ("a", "b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(set(got), {"a", "b"})
        self.assertIsNot(got["a"], got["b"])
        self.assertEqual(len(self.fake.created), 2)

    # ── what must stay true ─────────────────────────────────────────────────

    def test_one_loop_reuses_its_client(self):
        async def _run():
            return await jq.get_redis(), await jq.get_redis()

        first, second = asyncio.run(_run())

        self.assertIs(first, second, "a loop must not reconnect on every call")
        self.assertEqual(len(self.fake.created), 1)

    def test_the_client_is_built_from_the_configured_url(self):
        proxy = asyncio.run(jq.get_redis())

        self.assertEqual(proxy._client.url, REDIS_URL)
        self.assertTrue(proxy._client.kwargs.get("decode_responses"))

    def test_a_closed_loops_client_is_not_handed_out_again(self):
        asyncio.run(jq.get_redis())
        self.assertEqual(len(jq._redis_clients), 1)

        # asyncio.run() closed that loop; the next call must drop its entry and
        # build a client for the new one, not resurrect the dead one.
        newest = asyncio.run(jq.get_redis())

        self.assertEqual(len(jq._redis_clients), 1, "the closed loop's client was kept")
        self.assertEqual([proxy for _loop, proxy in jq._redis_clients.values()], [newest])

    def test_close_redis_closes_every_loops_client(self):
        proxy = asyncio.run(jq.get_redis())
        second = _FakeClient(REDIS_URL)

        async def _run():
            jq._redis_clients[999] = (asyncio.get_running_loop(), _Proxy(second))
            await jq.close_redis()

        asyncio.run(_run())

        self.assertTrue(proxy._client.closed, "the running loop's client was not closed")
        self.assertTrue(second.closed, "another loop's client was not closed")
        self.assertEqual(jq._redis_clients, {})

    def test_no_redis_url_is_still_an_error(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("REDIS_URL", None)
            with self.assertRaises(RuntimeError):
                asyncio.run(jq.get_redis())


class _Proxy:
    """Minimal stand-in for the real proxy in the close-all test."""

    def __init__(self, client):
        self._client = client

    def __getattr__(self, name):
        return getattr(self._client, name)


class RedisSingletonGoneTests(unittest.TestCase):
    """The process-wide client is gone, so a future edit cannot quietly restore it."""

    def test_the_queue_module_has_no_single_shared_client(self):
        from source_helpers import read_source

        src = read_source("utils", "job_queue.py")

        self.assertIn("_redis_clients", src)
        self.assertNotIn("global _redis_client", src)
        self.assertNotIn("global _redis_proxy", src)
        self.assertIn("asyncio.get_running_loop()", src)


if __name__ == "__main__":
    unittest.main()
