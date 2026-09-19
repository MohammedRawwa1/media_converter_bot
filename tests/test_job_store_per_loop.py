"""The job store's Mongo client belongs to the loop that used it.

Motor binds its connections to the event loop that opened them, and the job store
kept *one* client for the whole process - created by `init()` on whichever loop ran
first.  This process runs several loops (the bot's, which the ASGI app also serves
the Flask uploader from, the ffmpeg worker's, and a fallback per-thread loop for
Flask routes), so a `save_job` from any other one failed with "got Future <Future
pending> attached to a different loop".  Because the write is best-effort, the job
document was then silently never written.

These tests stand in a fake `motor` so the loop each client was built on is visible
without a MongoDB.
"""

import asyncio
import threading
import unittest
from unittest.mock import patch

from source_helpers import read_source

from utils import job_queue as jq
from utils import job_store as js

MONGO_URI = "mongodb://localhost:27017"


class _FakeCursor:
    def __init__(self, docs):
        self._docs = docs

    def sort(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    async def to_list(self, length=None):
        return list(self._docs)


class _FakeCollection:
    def __init__(self, docs):
        self.docs = docs

    async def insert_one(self, doc):
        self.docs.append(doc)

    async def update_one(self, query, update, upsert=False):
        for doc in self.docs:
            if doc.get("job_id") == query.get("job_id"):
                doc.update(update.get("$set", {}))
                return

    async def find_one(self, query):
        for doc in self.docs:
            if doc.get("job_id") == query.get("job_id"):
                return doc
        return None

    def find(self, query):
        return _FakeCursor([doc for doc in self.docs if doc.get("status") == query.get("status")])


class _FakeDatabase:
    def __init__(self, docs):
        self.jobs = _FakeCollection(docs)


class _FakeClient:
    """Stands in for the Motor client: records the loop it was built on."""

    def __init__(self, uri, docs):
        self.uri = uri
        self.closed = False
        self.built_on = _running_loop_identity()
        self.databases: dict[str, _FakeDatabase] = {}
        self._docs = docs

    def __getitem__(self, name):
        return self.databases.setdefault(name, _FakeDatabase(self._docs))

    def close(self):
        self.closed = True


class _FakeMotor:
    """The `motor.motor_asyncio` module: one callable, every client recorded."""

    def __init__(self):
        self.clients: list[_FakeClient] = []
        self.docs: list[dict] = []

    def __call__(self, uri, **kwargs):
        client = _FakeClient(uri, self.docs)
        self.clients.append(client)
        return client


def _running_loop_identity():
    try:
        return id(asyncio.get_running_loop())
    except RuntimeError:
        return None


class PerLoopJobStoreClientTests(unittest.TestCase):
    def setUp(self):
        self.fake = _FakeMotor()
        self.patch = patch.object(js, "AsyncIOMotorClient", self.fake)
        self.patch.start()
        self._uri, self._db_name = js._uri, js._db_name
        js._clients.clear()
        js._uri = None

    def tearDown(self):
        self.patch.stop()
        js._clients.clear()
        js._uri, js._db_name = self._uri, self._db_name

    # ── the bug ─────────────────────────────────────────────────────────────

    def test_two_loops_each_build_a_client_on_themselves(self):
        """The regression: the second loop was handed the first loop's client."""
        asyncio.run(js.init(MONGO_URI))
        first = self.fake.clients[0]

        asyncio.run(js.save_job({"job_id": "j1"}))

        self.assertEqual(len(self.fake.clients), 2, "one client was shared by both loops")
        second = self.fake.clients[1]
        self.assertIsNot(first, second)
        self.assertNotEqual(first.built_on, second.built_on)
        self.assertEqual(sorted([first.built_on, second.built_on]), sorted([c.built_on for c in self.fake.clients]))

    def test_a_job_written_on_one_loop_is_visible_from_another(self):
        """The user-visible symptom: the record could not be read back."""

        async def _write():
            await js.init(MONGO_URI)
            await js.save_job({"job_id": "j1", "status": "queued"})

        asyncio.run(_write())
        found = asyncio.run(js.get_job("j1"))  # a second loop: the web app's, say

        self.assertEqual(found["job_id"], "j1")
        self.assertEqual(len(self.fake.clients), 2, "each loop must have its own client")
        self.assertNotEqual(self.fake.clients[0].built_on, self.fake.clients[1].built_on)

    def test_two_threads_with_their_own_loops_each_get_a_client(self):
        """Exactly the web uploader's shape: one loop per WSGI thread."""
        seen: dict[str, int] = {}

        def _worker(name: str) -> None:
            loop = asyncio.new_event_loop()
            try:

                async def _use():
                    await js.save_job({"job_id": f"job-{name}"})
                    return _running_loop_identity()

                seen[name] = loop.run_until_complete(_use())
            finally:
                loop.close()

        asyncio.run(js.init(MONGO_URI))
        threads = [threading.Thread(target=_worker, args=(name,)) for name in ("a", "b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(set(seen), {"a", "b"})
        self.assertEqual(len(self.fake.docs), 2, "a write was lost")
        self.assertEqual(
            sorted(client.built_on for client in self.fake.clients if client.built_on in seen.values()),
            sorted(seen.values()),
            "a write went through a client built on another loop",
        )

    # ── what must stay true ─────────────────────────────────────────────────

    def test_one_loop_reuses_its_client(self):
        async def _run():
            await js.init(MONGO_URI)
            await js.save_job({"job_id": "j1"})
            await js.save_job({"job_id": "j2"})

        asyncio.run(_run())

        self.assertEqual(len(self.fake.clients), 1, "a loop must not reconnect per write")
        self.assertEqual(len(self.fake.docs), 2)

    def test_the_second_write_for_a_job_updates_the_same_document(self):
        async def _run():
            await js.init(MONGO_URI)
            await js.save_job({"job_id": "j1"})
            await js.update_job("j1", {"status": "done"})

        asyncio.run(_run())

        self.assertEqual(asyncio.run(js.get_job("j1"))["status"], "done")
        self.assertEqual(len(self.fake.docs), 1)

    def test_jobs_by_status_reads_from_the_running_loops_client(self):
        async def _run():
            await js.init(MONGO_URI)
            await js.save_job({"job_id": "j1", "status": "queued"})

        asyncio.run(_run())

        queued = asyncio.run(js.get_jobs_by_status("queued"))

        self.assertEqual([job["job_id"] for job in queued], ["j1"])

    def test_a_closed_loops_client_is_dropped_and_closed(self):
        asyncio.run(js.init(MONGO_URI))
        first = self.fake.clients[0]
        self.assertEqual(len(js._clients), 1)

        asyncio.run(js.save_job({"job_id": "j1"}))

        self.assertTrue(first.closed, "the closed loop's client was leaked")
        self.assertEqual(len(js._clients), 1, "the closed loop's entry was kept")

    def test_nothing_is_written_before_init(self):
        asyncio.run(js.save_job({"job_id": "j1"}))

        self.assertEqual(self.fake.clients, [])
        self.assertIsNone(asyncio.run(js.get_job("j1")))

    def test_a_sync_caller_has_no_database(self):
        js._uri = MONGO_URI

        self.assertIsNone(js._db_for_loop())
        self.assertEqual(self.fake.clients, [], "a client was built with no loop to bind it")

    def test_close_closes_every_loops_client(self):
        asyncio.run(js.init(MONGO_URI))
        asyncio.run(js.save_job({"job_id": "j1"}))

        asyncio.run(js.close())

        self.assertTrue(all(client.closed for client in self.fake.clients))
        self.assertEqual(js._clients, {})

    def test_init_requires_motor_and_a_uri(self):
        with patch.object(js, "AsyncIOMotorClient", None), self.assertRaises(RuntimeError):
            asyncio.run(js.init(MONGO_URI))

        with patch.dict("os.environ", {}, clear=True):
            js._uri = None
            with self.assertRaises(RuntimeError):
                asyncio.run(js.init(None))

    def test_the_uri_from_motor_is_the_configured_one(self):
        asyncio.run(js.init(MONGO_URI, db_name="other_db"))

        self.assertEqual(self.fake.clients[0].uri, MONGO_URI)
        self.assertIn("other_db", self.fake.clients[0].databases)


class JobWriteScheduleTests(unittest.TestCase):
    """The Mongo write that follows a queue push.

    `enqueue_job` used to fire that write off with an unreferenced task - which the
    collector can take mid-flight, losing the document - and, from a sync caller, on
    a loop of its own, where Motor then failed with "attached to a different loop".
    """

    def setUp(self):
        jq._job_writes.clear()

    def test_the_write_is_referenced_while_running_and_released_after(self):
        saved: list[dict] = []
        started = asyncio.Event()
        release = asyncio.Event()

        async def _save(job):
            started.set()
            await release.wait()
            saved.append(job)

        async def _run():
            jq._schedule_job_write(_save, {"job_id": "j1"})
            await started.wait()
            self.assertEqual(len(jq._job_writes), 1, "the write is not kept referenced")
            release.set()
            await asyncio.wait(list(jq._job_writes))
            await asyncio.sleep(0)  # let the done callback run

        asyncio.run(_run())

        self.assertEqual(saved, [{"job_id": "j1"}])
        self.assertEqual(jq._job_writes, set())

    def test_the_write_runs_on_the_calling_loop(self):
        seen: list[int] = []

        async def _save(job):
            seen.append(_running_loop_identity())

        async def _run():
            jq._schedule_job_write(_save, {"job_id": "j1"})
            await asyncio.sleep(0.05)

        asyncio.run(_run())

        self.assertEqual(len(seen), 1)
        self.assertIsNotNone(seen[0], "the write ran on no loop at all")

    def test_a_failing_write_is_swallowed(self):
        async def _save(job):
            raise RuntimeError("mongo is down")

        async def _run():
            jq._schedule_job_write(_save, {"job_id": "j1"})
            await asyncio.sleep(0.05)

        asyncio.run(_run())

        self.assertEqual(jq._job_writes, set())

    def test_no_running_loop_leaves_no_task_behind(self):
        async def _save(job):
            raise AssertionError("must never run")

        jq._schedule_job_write(_save, {"job_id": "j1"})  # sync context, no loop

        self.assertEqual(jq._job_writes, set())


class OneClientSourceTests(unittest.TestCase):
    def test_the_process_wide_client_is_gone(self):
        src = read_source("utils", "job_store.py")

        self.assertIn("_clients", src)
        self.assertIn("asyncio.get_running_loop()", src)
        self.assertNotIn("global _client", src)
        self.assertNotIn("global _db", src)
        self.assertNotIn("_client = None", src)

    def test_the_queue_no_longer_builds_a_loop_for_the_write(self):
        src = read_source("utils", "job_queue.py")

        self.assertIn("_schedule_job_write", src)
        self.assertIn("_job_writes", src)
        self.assertNotIn("loop.run_until_complete(save_job", src)
        self.assertNotIn("asyncio.new_event_loop()", src)


if __name__ == "__main__":
    unittest.main()
