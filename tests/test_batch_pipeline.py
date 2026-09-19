"""Sequential, memory-safe bulk batches: tagging, the per-batch lock, cleanup."""

import asyncio
import contextlib
import json
import os
import tempfile
import time
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import patch

from source_helpers import read_source

from utils import batch_pipeline

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TMP = tempfile.gettempdir()


def _returning(value):
    """An async accessor that always hands back ``value`` (stands in for get_redis)."""

    async def _getter():
        return value

    return _getter


class _FakeRedis:
    """Minimal async Redis stand-in recording the calls the module makes."""

    def __init__(self, set_result=True, exists_result=0, keys=None, values=None):
        self.set_result = set_result
        self.exists_result = exists_result
        self.keys = list(keys or [])
        self.values = dict(values or {})
        self.set_calls = []
        self.zadd_calls = []
        self.eval_calls = []
        self.exists_calls = []
        self.deleted = []
        self.closed = 0

    async def set(self, key, value, nx=False, px=None, ex=None):
        self.set_calls.append({"key": key, "value": value, "nx": nx, "px": px, "ex": ex})
        return self.set_result

    async def zadd(self, key, mapping):
        self.zadd_calls.append((key, mapping))
        return 1

    async def eval(self, script, numkeys, *args):
        self.eval_calls.append((script, numkeys, args))
        return 1

    async def exists(self, *keys):
        self.exists_calls.append(keys)
        return self.exists_result

    async def get(self, key):
        return self.values.get(key)

    async def getdel(self, key):
        return self.values.pop(key, None)

    async def delete(self, *keys):
        self.deleted.extend(keys)
        for key in keys:
            self.values.pop(key, None)
        return len(keys)

    async def scan_iter(self, match=None, count=None):
        if match and match.endswith("*"):
            prefix = match[:-1]
            for key in self.keys:
                if key.startswith(prefix):
                    yield key
        else:
            for key in self.keys:
                yield key

    async def close(self):
        self.closed += 1


class EnvNumberTests(unittest.TestCase):
    """An unset *or empty* env var must not take the worker down at import."""

    def test_unset_and_empty_fall_back(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(batch_pipeline._env_number("MISSING_VAR", 5), 5)
        with patch.dict(os.environ, {"EMPTY_VAR": ""}, clear=False):
            self.assertEqual(batch_pipeline._env_number("EMPTY_VAR", 5), 5)
            self.assertEqual(batch_pipeline._env_number("EMPTY_VAR", 2.5), 2.5)

    def test_parses_and_rejects_garbage(self):
        with patch.dict(os.environ, {"INT_VAR": "7", "FLOAT_VAR": "2.5", "BAD_VAR": "lots"}):
            self.assertEqual(batch_pipeline._env_number("INT_VAR", 1), 7)
            self.assertEqual(batch_pipeline._env_number("FLOAT_VAR", 1.0), 2.5)
            self.assertEqual(batch_pipeline._env_number("BAD_VAR", 3), 3)


class BatchTaggingTests(unittest.TestCase):
    def test_new_batch_id_is_unique_and_url_safe(self):
        first, second = batch_pipeline.new_batch_id(), batch_pipeline.new_batch_id()
        self.assertNotEqual(first, second)
        self.assertTrue(first.isalnum())

    def test_tag_and_read_back_the_batch(self):
        job = {}
        batch_pipeline.tag_batch_job(job, "b1", seq=2, total=5)
        self.assertEqual(job[batch_pipeline.BATCH_ID_FIELD], "b1")
        self.assertEqual(job[batch_pipeline.BATCH_SEQ_FIELD], 2)
        self.assertEqual(job[batch_pipeline.BATCH_TOTAL_FIELD], 5)
        self.assertEqual(batch_pipeline.job_batch_id(job), "b1")

    def test_a_plain_job_has_no_batch(self):
        self.assertIsNone(batch_pipeline.job_batch_id({}))
        self.assertIsNone(batch_pipeline.job_batch_id(None))
        self.assertIsNone(batch_pipeline.job_batch_id({"batch_id": ""}))


class BatchLockTests(unittest.IsolatedAsyncioTestCase):
    def test_lock_key_is_namespaced(self):
        self.assertEqual(batch_pipeline.batch_lock_key("abc"), "ffmpeg:batch:abc")

    def test_progress_key_does_not_collide_with_the_lock(self):
        self.assertEqual(batch_pipeline.batch_progress_key("abc"), "ffmpeg:batch:abc:done")
        self.assertNotEqual(batch_pipeline.batch_progress_key("abc"), batch_pipeline.batch_lock_key("abc"))

    async def test_acquires_when_free(self):
        redis = _FakeRedis(set_result=True)
        self.assertTrue(await batch_pipeline.try_acquire_batch_lock(redis, "b1", "job-1"))
        call = redis.set_calls[0]
        self.assertEqual(call["key"], "ffmpeg:batch:b1")
        self.assertEqual(call["value"], "job-1")
        self.assertTrue(call["nx"])
        self.assertGreater(call["px"], 0)

    async def test_refuses_when_held_by_another_job(self):
        redis = _FakeRedis(set_result=None)
        self.assertFalse(await batch_pipeline.try_acquire_batch_lock(redis, "b1", "job-2"))

    async def test_fails_open_when_redis_errors(self):
        class _Broken(_FakeRedis):
            async def set(self, *a, **k):
                raise RuntimeError("nope")

        # A Redis hiccup must not strand a user's job.
        self.assertTrue(await batch_pipeline.try_acquire_batch_lock(_Broken(), "b1", "job-3"))

    async def test_release_is_a_compare_and_delete(self):
        redis = _FakeRedis()
        self.assertTrue(await batch_pipeline.release_batch_lock(redis, "b1", "job-1"))
        script, numkeys, args = redis.eval_calls[0]
        self.assertIn("redis.call('del'", script)
        self.assertEqual(numkeys, 1)
        self.assertEqual(args, ("ffmpeg:batch:b1", "job-1"))

    async def test_defer_uses_the_delayed_job_set(self):
        from utils.job_queue import DELAYED_SET

        redis = _FakeRedis()
        job = {"job_id": "job-9"}
        before = time.time()
        self.assertTrue(await batch_pipeline.defer_batch_job(redis, job, delay=7))
        key, mapping = redis.zadd_calls[0]
        self.assertEqual(key, DELAYED_SET)
        member, score = next(iter(mapping.items()))
        self.assertEqual(json.loads(member), job)
        self.assertGreaterEqual(score, before + 7)


class FfmpegSlotTests(unittest.IsolatedAsyncioTestCase):
    """One ffmpeg at a time, everywhere: the cap lives in Redis, not a process."""

    def test_slot_key_is_namespaced(self):
        self.assertEqual(batch_pipeline.ffmpeg_slot_key(0), "ffmpeg:slot:0")

    def test_default_cap_is_one(self):
        self.assertGreaterEqual(batch_pipeline.MAX_CONCURRENT_FFMPEG, 1)

    async def test_takes_the_first_free_slot(self):
        redis = _FakeRedis(set_result=True)
        index = await batch_pipeline.acquire_ffmpeg_slot(redis, "job-1")
        self.assertEqual(index, 0)
        call = redis.set_calls[0]
        self.assertEqual(call["key"], "ffmpeg:slot:0")
        self.assertEqual(call["value"], "job-1")
        self.assertTrue(call["nx"])
        self.assertGreater(call["px"], 0)

    async def test_reports_busy_when_every_slot_is_taken(self):
        redis = _FakeRedis(set_result=None)
        self.assertIsNone(await batch_pipeline.acquire_ffmpeg_slot(redis, "job-2", slots=3))
        # Every slot is attempted. A read that finds no holder retries its SET on
        # the same key (the claim may have expired in between), so this is a set
        # of keys rather than an exact call count.
        self.assertEqual(
            {call["key"] for call in redis.set_calls},
            {
                "ffmpeg:slot:0",
                "ffmpeg:slot:1",
                "ffmpeg:slot:2",
            },
        )

    async def test_runs_without_a_slot_when_redis_errors(self):
        class _Broken(_FakeRedis):
            async def set(self, *a, **k):
                raise RuntimeError("nope")

        self.assertEqual(await batch_pipeline.acquire_ffmpeg_slot(_Broken(), "job-3"), -1)

    async def test_release_compares_and_deletes(self):
        redis = _FakeRedis()
        self.assertTrue(await batch_pipeline.release_ffmpeg_slot(redis, 2, "job-4"))
        _script, numkeys, args = redis.eval_calls[0]
        self.assertEqual(numkeys, 1)
        self.assertEqual(args, ("ffmpeg:slot:2", "job-4"))

    async def test_release_of_an_unheld_slot_is_a_noop(self):
        redis = _FakeRedis()
        self.assertFalse(await batch_pipeline.release_ffmpeg_slot(redis, -1, "job-5"))
        self.assertEqual(redis.eval_calls, [])

    def test_memory_ceiling_is_off_by_default(self):
        with patch.object(batch_pipeline, "MEMORY_CEILING_BYTES", 0):
            self.assertFalse(batch_pipeline.over_memory_ceiling())

    def test_memory_ceiling_triggers_above_the_limit(self):
        with (
            patch.object(batch_pipeline, "MEMORY_CEILING_BYTES", 100),
            patch.object(batch_pipeline, "rss_bytes", return_value=150),
        ):
            self.assertTrue(batch_pipeline.over_memory_ceiling())
        with (
            patch.object(batch_pipeline, "MEMORY_CEILING_BYTES", 100),
            patch.object(batch_pipeline, "rss_bytes", return_value=40),
        ):
            self.assertFalse(batch_pipeline.over_memory_ceiling())


class CapacityTelemetryTests(unittest.IsolatedAsyncioTestCase):
    """What the /session_status dashboard reads to show concurrency + headroom."""

    async def test_counts_only_the_slots_that_exist(self):
        redis = _FakeRedis(exists_result=1)
        self.assertEqual(await batch_pipeline.read_used_slots(redis, slots=1), 1)
        self.assertEqual(redis.exists_calls[0], ("ffmpeg:slot:0",))

    async def test_slot_count_is_none_when_redis_errors(self):
        class _Broken(_FakeRedis):
            async def exists(self, *a):
                raise RuntimeError("nope")

        self.assertIsNone(await batch_pipeline.read_used_slots(_Broken()))

    async def test_publishes_rss_under_a_ttl(self):
        redis = _FakeRedis()
        self.assertTrue(await batch_pipeline.publish_worker_rss(redis, worker_id="w1", rss=1234))
        call = redis.set_calls[0]
        self.assertEqual(call["key"], "ffmpeg:worker:rss:w1")
        self.assertEqual(call["value"], 1234)

    async def test_reads_every_live_worker_heartbeat(self):
        redis = _FakeRedis(
            keys=["ffmpeg:worker:rss:a", "ffmpeg:worker:rss:b", "ffmpeg:jobs"],
            values={"ffmpeg:worker:rss:a": 100, "ffmpeg:worker:rss:b": "250"},
        )
        self.assertEqual(await batch_pipeline.read_worker_rss(redis), {"a": 100, "b": 250})

    def test_worker_identity_is_stable_and_specific(self):
        first = batch_pipeline.worker_identity()
        self.assertEqual(first, batch_pipeline.worker_identity())
        self.assertIn(str(os.getpid()), first)
        # Plus a per-process token, so a redeploy that reuses the host name and
        # pid cannot re-publish the dead process's heartbeat - which would make
        # the claims it left behind look live forever.
        self.assertIn(batch_pipeline._WORKER_INSTANCE_TOKEN, first)


class _ProgressRedis:
    """The handful of Redis commands the batch progress reporter issues."""

    def __init__(self, initial=None):
        self.store = {key: str(value) for key, value in (initial or {}).items()}
        self.sets = {}
        self.closed = 0

    async def sadd(self, key, *members):
        # Faithful to Redis: returns only the count of *newly* added members, which
        # is what makes the batch progress count exactly-once per job.
        bucket = self.sets.setdefault(key, set())
        added = 0
        for member in members:
            if str(member) not in bucket:
                bucket.add(str(member))
                added += 1
        return added

    async def srem(self, key, *members):
        bucket = self.sets.get(key, set())
        removed = 0
        for member in members:
            if str(member) in bucket:
                bucket.discard(str(member))
                removed += 1
        return removed

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def exists(self, *keys):
        return sum(1 for key in keys if key in self.store or key in self.sets)

    async def incr(self, key):
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    async def expire(self, key, ttl):
        return True

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None, nx=False, px=None):
        self.store[key] = str(value)
        return True

    async def delete(self, *keys):
        for key in keys:
            self.store.pop(key, None)
        return len(keys)

    async def close(self):
        self.closed += 1


class _FakeBot:
    """An async-context-manager stand-in for telegram.Bot."""

    def __init__(self, token=None, **kwargs):
        self.token = token
        self.sent = []
        self.edited = []
        self.deleted = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def send_message(self, chat_id=None, text=None, **kwargs):
        self.sent.append((chat_id, text))
        return type("Sent", (), {"message_id": 900 + len(self.sent)})()

    async def edit_message_text(self, chat_id=None, message_id=None, text=None, **kwargs):
        self.edited.append((chat_id, message_id, text))
        # Real PTB returns the edited Message; a falsy result tells callers
        # the edit was dropped (e.g. flood control), which would spin the watcher.
        return type("Edited", (), {"message_id": message_id or 0})()

    async def delete_message(self, chat_id=None, message_id=None, **kwargs):
        self.deleted.append((chat_id, message_id))


class BatchProgressMessageTests(unittest.IsolatedAsyncioTestCase):
    """A batch shows one message, edited in place and taken down when it ends."""

    def _job(self, total=3, chat_id=5):
        # A fresh id per call, because every file of a batch is its own job and the
        # batch count is deduplicated per job id. Reusing one id made the helper
        # model something the producer never does.
        return batch_pipeline.tag_batch_job(
            {
                "job_id": f"j-{uuid.uuid4().hex[:8]}",
                "chat_id": chat_id,
                "original_filename": "a.mp4",
            },
            "b1",
            0,
            total,
        )

    async def _report(self, job, redis):
        from workers import ffmpeg_worker as worker

        # The bot publishes a batch's expected count before it queues its first
        # job, and the worker reports only for a batch that still owns its state,
        # so a real report always has that key behind it.
        redis.store.setdefault(
            batch_pipeline.batch_total_key("b1"),
            str(job.get(batch_pipeline.BATCH_TOTAL_FIELD) or 0),
        )
        bot = _FakeBot()
        with (
            patch.object(worker, "Bot", lambda *a, **k: bot),
            patch.object(worker, "get_redis", _returning(redis)),
            patch.object(worker.config, "BOT_TOKEN", "tok", create=True),
        ):
            await worker._report_batch_progress(job)
        return bot

    async def test_first_file_posts_one_message(self):
        redis = _ProgressRedis()
        bot = await self._report(self._job(total=3), redis)

        self.assertEqual(len(bot.sent), 1)
        self.assertEqual(bot.sent[0][0], 5)
        self.assertIn("1 of 3", bot.sent[0][1])
        # The message's location is remembered so the next file edits it.
        self.assertEqual(redis.store[batch_pipeline.batch_message_key("b1")], "5:901")
        self.assertEqual(bot.edited, [])
        self.assertEqual(bot.deleted, [])

    async def test_later_files_edit_the_same_message(self):
        redis = _ProgressRedis()
        await self._report(self._job(total=3), redis)
        bot = await self._report(self._job(total=3), redis)

        self.assertEqual(bot.sent, [])
        self.assertEqual(len(bot.edited), 1)
        self.assertEqual(bot.edited[0][0], 5)
        self.assertEqual(bot.edited[0][1], 901)
        self.assertIn("2 of 3", bot.edited[0][2])

    async def test_last_file_removes_the_message(self):
        redis = _ProgressRedis()
        await self._report(self._job(total=3), redis)
        await self._report(self._job(total=3), redis)
        bot = await self._report(self._job(total=3), redis)

        self.assertEqual(bot.deleted, [(5, 901)])
        self.assertNotIn(batch_pipeline.batch_message_key("b1"), redis.store)

    async def test_the_exact_enqueued_count_drives_completion(self):
        # The payload says 5 collected files, but only 2 jobs were enqueued (the
        # rest were skipped). Without the recorded total the message would be
        # left in the chat forever.
        redis = _ProgressRedis({batch_pipeline.batch_total_key("b1"): 2})
        first = await self._report(self._job(total=5), redis)
        second = await self._report(self._job(total=5), redis)

        # The message counts to the recorded two, not the payload's five...
        self.assertEqual(len(first.sent), 1)
        self.assertIn("1 of 2", first.sent[0][1])
        # ...so the batch finishes (and cleans up) on the second file.
        self.assertEqual(second.deleted, [(5, 901)])

    async def test_a_failed_job_still_advances_and_cleans_up(self):
        # Progress counting is independent of the job's outcome, so an errored
        # file must not strand the batch's message.
        redis = _ProgressRedis()
        await self._report(self._job(total=2), redis)
        bot = await self._report(self._job(total=2), redis)
        self.assertEqual(bot.deleted, [(5, 901)])

    async def test_a_deleted_message_is_reposted_rather_than_lost(self):
        from telegram.error import BadRequest

        redis = _ProgressRedis()
        await self._report(self._job(total=4), redis)
        bot = await self._report_with_failing_edit(redis, BadRequest("message to edit not found"))

        self.assertEqual(len(bot.sent), 1)
        self.assertIn("2 of 4", bot.sent[0][1])

    async def test_a_transient_edit_failure_does_not_duplicate_the_message(self):
        # A network blip or 429 must not spray a second progress message every
        # tick - keep the one we already have and try again next time.
        redis = _ProgressRedis()
        await self._report(self._job(total=4), redis)
        bot = await self._report_with_failing_edit(redis, RuntimeError("connection reset"))

        self.assertEqual(bot.sent, [])
        self.assertEqual(bot.edited, [])

    async def _report_with_failing_edit(self, redis, error):
        from workers import ffmpeg_worker as worker

        bot = _FakeBot()

        async def _fail(*args, **kwargs):
            raise error

        bot.edit_message_text = _fail
        with (
            patch.object(worker, "Bot", lambda *a, **k: bot),
            patch.object(worker, "get_redis", _returning(redis)),
            patch.object(worker.config, "BOT_TOKEN", "tok", create=True),
        ):
            await worker._report_batch_progress(self._job(total=4))
        return bot

    def test_the_message_carries_the_running_files_progress(self):
        from workers.ffmpeg_worker import _batch_progress_text

        running = _batch_progress_text(3, 12, name="clip.mp4", pct=47.6)
        self.assertIn("3 of 12 finished", running)
        self.assertIn("🔄 clip.mp4 — 47%", running)
        # A finished file is shown without a percentage - and only a caller that
        # counted it may say so.
        finished = _batch_progress_text(4, 12, name="clip.mp4", finished=True)
        self.assertIn("✅ clip.mp4", finished)
        self.assertNotIn("clip.mp4", _batch_progress_text(4, 12))

    def test_a_running_file_with_no_percentage_yet_is_not_shown_as_done(self):
        """A queued file is not a finished one.

        The live ticker runs for the file being *processed*, and the worker only
        writes a percentage once ffmpeg starts - so a job that has just been
        picked up (fetching its source, probing it, waiting for the slot) has none.
        Reading that as "finished" drew "✅ <name>" beside work that had not begun,
        which is how a file still queued came to be read as already done.
        """
        from workers.ffmpeg_worker import _batch_progress_text

        not_started = _batch_progress_text(3, 12, name="Module 02.mp4")
        self.assertIn("🔄 Module 02.mp4 — 0%", not_started)
        self.assertNotIn("✅", not_started)
        # And the ticker's own call never claims a finished file while it runs.
        src = read_source("workers", "ffmpeg_worker.py")
        ticker = src.index("async def _batch_live_progress(")
        body = src[ticker : src.index("async def _report_batch_progress(")]
        call_start = body.index("text = _batch_progress_text(")
        call = body[call_start : body.index(")", call_start)]
        self.assertNotIn("finished", call)
        # The end-of-job report is where it is earned.
        report = src[src.index("async def _report_batch_progress(") :]
        self.assertIn("finished=True", report)

    async def test_a_batch_that_was_taken_down_is_never_reported_again(self):
        # Nothing left in Redis for this batch: a cancel-all swept it as stale
        # after its last file. The INCR below would write its counter straight
        # back, and the message after that would post a bar nothing owns.
        from workers import ffmpeg_worker as worker

        redis = _ProgressRedis()
        bot = _FakeBot()
        with (
            patch.object(worker, "Bot", lambda *a, **k: bot),
            patch.object(worker, "get_redis", _returning(redis)),
            patch.object(worker.config, "BOT_TOKEN", "tok", create=True),
        ):
            await worker._report_batch_progress(self._job(total=3))

        self.assertEqual(bot.sent, [])
        self.assertEqual(redis.store, {})

    async def test_progress_never_raises_when_redis_is_down(self):
        from workers import ffmpeg_worker as worker

        async def _boom():
            raise RuntimeError("no redis")

        with patch.object(worker, "get_redis", _boom):
            await worker._report_batch_progress(self._job())


class ClaimHeartbeatTests(unittest.IsolatedAsyncioTestCase):
    """A worker that is busy is still alive, and has to keep saying so.

    The ghost-claim checks tell a claim whose owner died mid-encode from a live one
    by asking whether its worker still heartbeats. The idle loop is the only other
    publisher, and it is not running while a job is - so a conversion longer than
    the heartbeat's TTL used to make its own worker look dead, and the next job
    stole the live claim and started a second ffmpeg beside it.
    """

    async def test_a_running_job_publishes_the_heartbeat_on_its_own_timer(self):
        from workers import ffmpeg_worker as worker

        heartbeats: list[float] = []
        claims: list[str] = []
        locks: list[str] = []

        class _Redis:
            async def close(self):
                return None

        async def _get_redis():
            return _Redis()

        async def _publish(*args, **kwargs):
            heartbeats.append(time.time())

        async def _refresh(*args, **kwargs):
            claims.append("slot")
            return True

        async def _refresh_lock(*args, **kwargs):
            locks.append("batch")
            return True

        with (
            patch.object(worker, "get_redis", _get_redis),
            patch.object(worker, "_RSS_HEARTBEAT_SECONDS", 0.02),
            patch.object(worker, "_publish_worker_rss", _publish),
            patch.object(batch_pipeline, "CLAIM_HEARTBEAT_SECONDS", 0.05),
            patch.object(batch_pipeline, "refresh_ffmpeg_slot", _refresh),
            patch.object(batch_pipeline, "refresh_batch_lock", _refresh_lock),
        ):
            task = asyncio.create_task(
                worker._keep_claims_alive(batch_pipeline.tag_batch_job({"job_id": "j1"}, "b1", 0, 2), 0)
            )
            await asyncio.sleep(0.16)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        # The heartbeat keeps ticking for the whole job...
        self.assertGreaterEqual(len(heartbeats), 3)
        # ...while the slot and the batch lock are re-armed on their own, slower
        # timer, and neither is re-armed as often as the heartbeat.
        self.assertGreaterEqual(len(claims), 1)
        self.assertEqual(len(locks), len(claims))
        self.assertLess(len(claims), len(heartbeats))


class WorkerRestartRequestTests(unittest.IsolatedAsyncioTestCase):
    """An admin can ask the standalone worker to recycle for a clean heap."""

    async def test_request_writes_a_ttl_key_naming_the_caller(self):
        redis = _FakeRedis()
        self.assertTrue(await batch_pipeline.request_worker_restart(redis, requested_by=42))
        call = redis.set_calls[0]
        self.assertEqual(call["key"], batch_pipeline.WORKER_RESTART_KEY)
        self.assertIn("42@", call["value"])
        self.assertGreater(call["ex"], 0)

    async def test_consume_is_single_shot(self):
        redis = _FakeRedis(values={batch_pipeline.WORKER_RESTART_KEY: "42@now"})
        self.assertEqual(await batch_pipeline.consume_worker_restart(redis), "42@now")
        # A worker that comes back up must not find the same request waiting and
        # restart again in a loop.
        self.assertIsNone(await batch_pipeline.consume_worker_restart(redis))

    async def test_consume_without_a_request_is_none(self):
        self.assertIsNone(await batch_pipeline.consume_worker_restart(_FakeRedis()))

    async def test_request_reports_failure_instead_of_raising(self):
        class _Broken(_FakeRedis):
            async def set(self, *a, **k):
                raise RuntimeError("no redis")

        self.assertFalse(await batch_pipeline.request_worker_restart(_Broken()))

    def test_request_restart_sets_the_in_process_flag(self):
        batch_pipeline.reset_restart_request()
        try:
            self.assertTrue(batch_pipeline.request_restart("admin"))
            self.assertTrue(batch_pipeline.restart_requested())
        finally:
            batch_pipeline.reset_restart_request()


class FinalizeTests(unittest.IsolatedAsyncioTestCase):
    async def test_batch_job_releases_its_lock(self):
        redis = _FakeRedis()
        job = batch_pipeline.tag_batch_job({"job_id": "j1"}, "b1", 0, 3)
        with (
            patch.object(batch_pipeline, "reclaim_memory", return_value={}),
            patch.object(batch_pipeline, "sweep_temp_artifacts", return_value={}),
        ):
            summary = await batch_pipeline.finalize_job(job, source="redis", redis=redis)
        self.assertEqual(len(redis.eval_calls), 1)
        self.assertTrue(summary["batch_released"])

    async def test_plain_job_does_not_touch_a_lock(self):
        redis = _FakeRedis()
        with (
            patch.object(batch_pipeline, "reclaim_memory", return_value={}),
            patch.object(batch_pipeline, "sweep_temp_artifacts", return_value={}),
        ):
            summary = await batch_pipeline.finalize_job({"job_id": "j2"}, redis=redis)
        self.assertEqual(redis.eval_calls, [])
        self.assertNotIn("batch_released", summary)

    async def test_handing_back_the_ffmpeg_slot(self):
        redis = _FakeRedis()
        job = {"job_id": "j4"}
        with (
            patch.object(batch_pipeline, "reclaim_memory", return_value={}),
            patch.object(batch_pipeline, "sweep_temp_artifacts", return_value={}),
        ):
            summary = await batch_pipeline.finalize_job(job, redis=redis, ffmpeg_slot=3)
        self.assertTrue(summary["ffmpeg_slot_released"])
        self.assertEqual(redis.eval_calls[0][2], ("ffmpeg:slot:3", "j4"))
        # The slot is passed in, so the persisted payload carries no internal marker.
        self.assertTrue(all(not key.startswith("_") for key in job))

    async def test_never_raises_when_cleanup_fails(self):
        with patch.object(batch_pipeline, "reclaim_memory", side_effect=RuntimeError("boom")):
            summary = await batch_pipeline.finalize_job({"job_id": "j3"})
        self.assertIsInstance(summary, dict)


class MemoryReclaimTests(unittest.TestCase):
    def test_reclaim_collects_and_trims_and_reports_rss(self):
        with (
            patch.object(batch_pipeline, "_malloc_trim", return_value=True) as trim,
            patch.object(batch_pipeline, "rss_bytes", side_effect=[1000, 400]),
            patch.object(batch_pipeline.gc, "collect", return_value=42) as collect,
        ):
            result = batch_pipeline.reclaim_memory("test")
        collect.assert_called_once()
        trim.assert_called_once()
        self.assertEqual(result["before"], 1000)
        self.assertEqual(result["after"], 400)
        self.assertEqual(result["collected"], 42)
        self.assertTrue(result["trimmed"])

    def test_reclaim_is_safe_without_psutil_or_glibc(self):
        with patch.object(batch_pipeline, "rss_bytes", return_value=0):
            result = batch_pipeline.reclaim_memory("bare")
        self.assertEqual(result["before"], 0)
        self.assertIn("collected", result)

    def test_restart_threshold_defaults_to_disabled(self):
        with (
            patch.object(batch_pipeline, "RESTART_RSS_THRESHOLD", 0),
            patch.object(batch_pipeline, "rss_bytes", return_value=999_999_999),
        ):
            self.assertFalse(batch_pipeline.memory_pressure())

    def test_restart_is_requested_above_the_threshold(self):
        batch_pipeline.reset_restart_request()
        try:
            with (
                patch.object(batch_pipeline, "RESTART_RSS_THRESHOLD", 100),
                patch.object(batch_pipeline, "rss_bytes", return_value=500),
            ):
                self.assertTrue(batch_pipeline.request_restart_if_pressured())
                self.assertTrue(batch_pipeline.restart_requested())
        finally:
            batch_pipeline.reset_restart_request()

    def test_restart_is_not_requested_below_the_threshold(self):
        batch_pipeline.reset_restart_request()
        try:
            with (
                patch.object(batch_pipeline, "RESTART_RSS_THRESHOLD", 100),
                patch.object(batch_pipeline, "rss_bytes", return_value=50),
            ):
                self.assertFalse(batch_pipeline.request_restart_if_pressured())
                self.assertFalse(batch_pipeline.restart_requested())
        finally:
            batch_pipeline.reset_restart_request()


class _PrefixedArtifact:
    """A temp artifact with a worker prefix, used to exercise the sweep."""

    def __init__(self, name, old=False, is_dir=False):
        self.path = os.path.join(TMP, name)
        if is_dir:
            os.makedirs(self.path, exist_ok=True)
            with open(os.path.join(self.path, "f.bin"), "wb") as fh:
                fh.write(b"x" * 16)
        else:
            with open(self.path, "wb") as fh:
                fh.write(b"x" * 16)
        if old:
            stale = time.time() - 3600
            os.utime(self.path, (stale, stale))

    def cleanup(self):
        try:
            if os.path.isdir(self.path):
                import shutil

                shutil.rmtree(self.path, ignore_errors=True)
            else:
                os.remove(self.path)
        except Exception:
            pass


class TempSweepTests(unittest.TestCase):
    def test_old_prefixed_artifacts_are_removed(self):
        file_artifact = _PrefixedArtifact(f"worker_thumb_{os.getpid()}.jpg", old=True)
        dir_artifact = _PrefixedArtifact(f"worker_thumb_{os.getpid()}_d", old=True, is_dir=True)
        try:
            result = batch_pipeline.sweep_temp_artifacts(grace_seconds=60)
            self.assertGreaterEqual(result["removed"], 2)
            self.assertFalse(os.path.exists(file_artifact.path))
            self.assertFalse(os.path.exists(dir_artifact.path))
        finally:
            file_artifact.cleanup()
            dir_artifact.cleanup()

    def test_fresh_and_unrelated_artifacts_are_kept(self):
        fresh = _PrefixedArtifact(f"worker_thumb_{os.getpid()}_new.jpg", old=False)
        unrelated = os.path.join(TMP, f"not_ours_{os.getpid()}.txt")
        with open(unrelated, "wb") as fh:
            fh.write(b"keep me")
        try:
            batch_pipeline.sweep_temp_artifacts(grace_seconds=60)
            self.assertTrue(os.path.exists(fresh.path))
            self.assertTrue(os.path.exists(unrelated))
        finally:
            fresh.cleanup()
            with contextlib.suppress(OSError):
                os.remove(unrelated)


class WiringTests(unittest.TestCase):
    """The bot tags batch jobs and the worker runs/finalizes them one at a time."""

    def _read(self, *parts):
        return read_source(*parts)

    def test_bulk_apply_tags_every_job_with_one_batch(self):
        src = self._read("handlers.py")
        self.assertIn("from utils.batch_pipeline import new_batch_id, tag_batch_job", src)
        self.assertIn("_batch_id = new_batch_id()", src)
        # Both the slideshow job and every per-file job are tagged.
        self.assertIn("tag_batch_job(_ss_job, _batch_id, _batch_seq, _batch_total)", src)
        self.assertIn("tag_batch_job(job, _batch_id, _batch_seq, _batch_total)", src)

    def test_worker_gates_jobs_and_cleans_up_after_each(self):
        src = self._read("workers", "ffmpeg_worker.py")
        self.assertIn("_claim_execution_slot(job)", src)
        self.assertIn("batch_pipeline.acquire_ffmpeg_slot(", src)
        self.assertIn("batch_pipeline.finalize_job(job, source=source, ffmpeg_slot=slot)", src)
        self.assertIn("batch_pipeline.request_restart_if_pressured()", src)
        self.assertIn("batch_pipeline.over_memory_ceiling()", src)

    def test_worker_honours_a_restart_only_between_jobs(self):
        src = self._read("workers", "ffmpeg_worker.py")
        self.assertIn("_maybe_stop_for_restart(allow_restart)", src)
        self.assertIn("_maybe_stop_for_restart(allow_restart, force=True)", src)
        self.assertIn("batch_pipeline.consume_worker_restart(", src)
        # A planned restart must never truncate a job that is already running.
        self.assertIn("if not allow_restart or _jobs_in_flight:", src)

    def test_admin_can_request_a_restart_from_telegram(self):
        src = self._read("main.py")
        self.assertIn('CommandHandler("worker_restart"', src)
        self.assertIn("async def _perform_worker_restart(reply, update)", src)
        self.assertIn('elif action == "worker_restart":', src)
        self.assertIn("from utils.batch_pipeline import request_worker_restart", src)
        # Admin-gated like every other destructive command.
        self.assertIn("if not _admin_only(update):", src)

    def test_worker_reports_batch_progress_after_each_file(self):
        src = self._read("workers", "ffmpeg_worker.py")
        self.assertIn("_report_batch_progress(job)", src)
        self.assertIn("Processing one at a time — {done} of {total} finished", src)
        # One message edited in place, removed once the batch ends.
        self.assertIn("batch_pipeline.batch_message_key(", src)
        self.assertIn("_delete_batch_message(bot, r, msg_key, state[", src)

    def test_worker_shows_the_running_file_in_the_batch_message(self):
        src = self._read("workers", "ffmpeg_worker.py")
        self.assertIn("asyncio.create_task(_batch_live_progress(job))", src)
        # The ticker is stopped before the final figure is written, so the two
        # writers never race on the same message.
        self.assertIn("batch_progress_task.cancel()", src)
        self.assertIn('r.hget(f"ffmpeg:job:{job_id}", "progress")', src)

    def test_apply_records_the_exact_enqueued_total(self):
        src = self._read("handlers.py")
        self.assertIn("from utils.batch_pipeline import set_batch_total", src)
        self.assertIn("await set_batch_total(batch_id=_batch_id, total=enqueued)", src)

    def test_production_runs_a_single_replica_everywhere(self):
        for rel in ("railway.json", os.path.join("workers", "railway.json")):
            with open(os.path.join(PROJECT_ROOT, rel), encoding="utf-8") as fh:
                config = json.load(fh)
            self.assertEqual(
                config["environments"]["production"]["deploy"]["numReplicas"],
                1,
                msg=rel,
            )

    def test_standalone_worker_can_restart_for_a_clean_heap(self):
        src = self._read("workers", "ffmpeg_worker.py")
        self.assertIn("allow_restart=True", src)
        self.assertIn("if batch_pipeline.restart_requested():", src)


class ClaimTtlTests(unittest.TestCase):
    """How fast a dead worker's claim on a batch or a slot self-heals."""

    def test_lock_ttl_is_bounded_and_short(self):
        # The old value inherited the 6h job ceiling. That is what let a worker
        # that died holding a batch's lock fence that batch for six hours, with
        # every remaining job deferring and the bar stuck at "0 of N".
        self.assertGreater(batch_pipeline.BATCH_LOCK_TTL_SECONDS, 0)
        self.assertLessEqual(batch_pipeline.BATCH_LOCK_TTL_SECONDS, 3600)

    def test_batch_data_outlives_the_lock_by_a_long_way(self):
        # Counters must survive however long the batch really takes; only the
        # lock is short-lived, and only the lock is refreshed.
        self.assertGreater(batch_pipeline.BATCH_STATE_TTL_SECONDS, batch_pipeline.BATCH_LOCK_TTL_SECONDS * 10)

    def test_heartbeat_is_a_small_fraction_of_the_lock_ttl(self):
        self.assertLess(batch_pipeline.CLAIM_HEARTBEAT_SECONDS, batch_pipeline.BATCH_LOCK_TTL_SECONDS / 5)


class ClaimRefreshTests(unittest.IsolatedAsyncioTestCase):
    """A running job re-arms its own claims; anything else must not."""

    async def test_refresh_ffmpeg_slot_rearms_only_its_own_claim(self):
        redis = _FakeRedis()
        self.assertTrue(await batch_pipeline.refresh_ffmpeg_slot(redis, 1, "job-1"))
        script, numkeys, args = redis.eval_calls[0]
        self.assertIn("pexpire", script)
        self.assertEqual(numkeys, 1)
        self.assertEqual(args[0], "ffmpeg:slot:1")
        self.assertEqual(args[1], "job-1")

    async def test_refresh_of_an_unheld_slot_is_a_noop(self):
        redis = _FakeRedis()
        self.assertFalse(await batch_pipeline.refresh_ffmpeg_slot(redis, -1, "job-2"))
        self.assertEqual(redis.eval_calls, [])

    async def test_refresh_batch_lock_targets_the_batch_key(self):
        redis = _FakeRedis()
        await batch_pipeline.refresh_batch_lock(redis, "b1", "job-3")
        _script, _numkeys, args = redis.eval_calls[0]
        self.assertEqual(args[0], "ffmpeg:batch:b1")
        self.assertEqual(args[1], "job-3")


class ProgressBarTests(unittest.TestCase):
    def test_bar_is_a_fixed_width(self):
        for done, total in ((0, 10), (5, 10), (10, 10), (1, 3)):
            self.assertEqual(len(batch_pipeline.progress_bar(done, total)), 10)

    def test_bar_is_full_at_the_end_and_empty_at_the_start(self):
        self.assertEqual(batch_pipeline.progress_bar(0, 4), "░" * 10)
        self.assertEqual(batch_pipeline.progress_bar(4, 4), "█" * 10)

    def test_bar_tolerates_an_unknown_total_and_garbage(self):
        self.assertEqual(batch_pipeline.progress_bar(3, 0), "░" * 10)
        self.assertEqual(batch_pipeline.progress_bar("nope", None), "░" * 10)


class BatchDoneCounterTests(unittest.IsolatedAsyncioTestCase):
    """A file finished inside the handler still has to move the batch along."""

    async def test_marking_a_file_done_advances_the_counter(self):
        redis = _ProgressRedis()
        self.assertEqual(await batch_pipeline.mark_batch_file_done(redis, batch_id="b1"), 1)
        self.assertEqual(await batch_pipeline.mark_batch_file_done(redis, batch_id="b1"), 2)
        self.assertIn(batch_pipeline.batch_progress_key("b1"), redis.store)

    async def test_marking_without_a_batch_is_a_noop(self):
        self.assertIsNone(await batch_pipeline.mark_batch_file_done(_ProgressRedis(), batch_id=""))


class BatchProgressDedupTests(unittest.IsolatedAsyncioTestCase):
    """A redelivered job must not advance its batch twice.

    The broker retries a job whose handler raised, and every attempt runs the same
    end-of-job bookkeeping. A plain INCR would count one file twice, finish the
    batch a file early and take its bar down with work still queued.
    """

    async def test_first_report_wins_and_a_retry_does_not(self):
        redis = _ProgressRedis()
        self.assertTrue(await batch_pipeline.claim_batch_progress_slot(redis, "b1", "job-1"))
        self.assertFalse(await batch_pipeline.claim_batch_progress_slot(redis, "b1", "job-1"))

    async def test_distinct_jobs_each_get_their_slot(self):
        redis = _ProgressRedis()
        self.assertTrue(await batch_pipeline.claim_batch_progress_slot(redis, "b1", "job-1"))
        self.assertTrue(await batch_pipeline.claim_batch_progress_slot(redis, "b1", "job-2"))

    async def test_missing_identifiers_fail_open(self):
        # Nothing to deduplicate: count it rather than risk stalling a batch.
        redis = _ProgressRedis()
        self.assertTrue(await batch_pipeline.claim_batch_progress_slot(redis, "", "job-1"))
        self.assertTrue(await batch_pipeline.claim_batch_progress_slot(redis, "b1", None))

    def test_worker_claims_before_it_increments(self):
        from workers import ffmpeg_worker as worker

        src = read_source("workers", "ffmpeg_worker.py")
        claim = src.index('claim_batch_progress_slot(r, batch_id, job.get("job_id"))')
        increment = src.index("done = int(await r.incr(done_key))")
        self.assertLess(claim, increment)
        self.assertTrue(hasattr(worker, "_report_batch_progress"))

    def test_a_cancelled_pipeline_job_is_not_reported_as_completed(self):
        src = BulkFetchReportingTests._src()
        self.assertIn('_pipeline_status == "cancelled"', src)
        self.assertIn('file_info["_batch_cancelled"] = True', src)
        self.assertIn('file_info["_pipeline_failed"] = True', src)
        self.assertIn('f.get("_pipeline_failed")', src)


class BatchResumeTests(unittest.IsolatedAsyncioTestCase):
    """A batch interrupted by a restart must not redo the files it finished.

    The apply feeds the queue one file at a time (so 30 large sources never land
    on disk at once), which means a restart abandons the files it had not reached.
    Those are still in the user's collection - so the ones already finished have to
    be recorded, or the next Apply converts them all over again.
    """

    async def test_a_finished_entry_is_recorded_against_its_batch(self):
        redis = _ProgressRedis()
        await batch_pipeline.open_batch_resume(redis, batch_id="b1", user_id=7)
        self.assertTrue(await batch_pipeline.mark_batch_entry_finished(redis, "b1", "file-abc"))

        self.assertEqual(await batch_pipeline.read_finished_entries(redis, 7), {"b1": {"file-abc"}})

    async def test_an_unfinished_batch_reports_nothing(self):
        redis = _ProgressRedis()
        await batch_pipeline.open_batch_resume(redis, batch_id="b1", user_id=7)
        self.assertEqual(await batch_pipeline.read_finished_entries(redis, 7), {})

    async def test_records_are_keyed_per_user(self):
        redis = _ProgressRedis()
        await batch_pipeline.open_batch_resume(redis, batch_id="b1", user_id=7)
        await batch_pipeline.mark_batch_entry_finished(redis, "b1", "file-abc")
        self.assertEqual(await batch_pipeline.read_finished_entries(redis, 8), {})

    async def test_entries_of_several_batches_are_merged(self):
        redis = _ProgressRedis()
        for batch_id, key in (("b1", "file-a"), ("b2", "file-b")):
            await batch_pipeline.open_batch_resume(redis, batch_id=batch_id, user_id=7)
            await batch_pipeline.mark_batch_entry_finished(redis, batch_id, key)
        self.assertEqual(
            await batch_pipeline.read_finished_entries(redis, 7),
            {"b1": {"file-a"}, "b2": {"file-b"}},
        )

    async def test_consumed_entries_are_forgotten_so_a_resend_is_honoured(self):
        # Once an entry has been subtracted from the collection it must be
        # forgotten, or re-sending that same file later would be skipped.
        redis = _ProgressRedis()
        await batch_pipeline.open_batch_resume(redis, batch_id="b1", user_id=7)
        await batch_pipeline.mark_batch_entry_finished(redis, "b1", "file-a")
        finished = await batch_pipeline.read_finished_entries(redis, 7)
        await batch_pipeline.forget_finished_entries(redis, finished, {"file-a"})
        self.assertEqual(await batch_pipeline.read_finished_entries(redis, 7), {})

    async def test_forgetting_leaves_untouched_entries_alone(self):
        redis = _ProgressRedis()
        await batch_pipeline.open_batch_resume(redis, batch_id="b1", user_id=7)
        await batch_pipeline.mark_batch_entry_finished(redis, "b1", "file-a")
        await batch_pipeline.mark_batch_entry_finished(redis, "b1", "file-b")
        finished = await batch_pipeline.read_finished_entries(redis, 7)
        await batch_pipeline.forget_finished_entries(redis, finished, {"file-a"})
        self.assertEqual(await batch_pipeline.read_finished_entries(redis, 7), {"b1": {"file-b"}})

    async def test_closing_discards_the_record(self):
        redis = _ProgressRedis()
        await batch_pipeline.open_batch_resume(redis, batch_id="b1", user_id=7)
        await batch_pipeline.mark_batch_entry_finished(redis, "b1", "file-a")
        await batch_pipeline.close_batch_resume(redis, batch_id="b1", user_id=7)
        self.assertEqual(await batch_pipeline.read_finished_entries(redis, 7), {})

    async def test_missing_identifiers_are_a_noop(self):
        redis = _ProgressRedis()
        self.assertFalse(await batch_pipeline.open_batch_resume(redis, batch_id="", user_id=7))
        self.assertFalse(await batch_pipeline.open_batch_resume(redis, batch_id="b1", user_id=None))
        self.assertFalse(await batch_pipeline.mark_batch_entry_finished(redis, "b1", None))
        self.assertEqual(await batch_pipeline.read_finished_entries(redis, None), {})

    def test_entry_key_survives_a_restart(self):
        from handlers import _bulk_entry_key

        # The file id is what makes the record meaningful after a restart; a
        # process-local identity would not be.
        self.assertEqual(_bulk_entry_key({"id": "BAACAgQ"}), "BAACAgQ")
        self.assertEqual(_bulk_entry_key({"file_unique_id": "uniq"}), "uniq")
        _path = os.path.join(TMP, "clip.mp4")
        self.assertEqual(_bulk_entry_key(_path), _path)
        self.assertIsNone(_bulk_entry_key({}))
        self.assertIsNone(_bulk_entry_key(None))


class DuplicateDeliveryTests(unittest.IsolatedAsyncioTestCase):
    """A broker redelivery must not convert and send the same file twice."""

    class _JobRedis:
        def __init__(self, delivered=None):
            self.hashes = {"ffmpeg:job:j1": {"delivered": delivered} if delivered is not None else {}}

        async def hget(self, key, field):
            return self.hashes.get(key, {}).get(field)

    async def _delivered(self, job, redis):
        from workers import ffmpeg_worker as worker

        with patch.object(worker, "get_redis", _returning(redis)):
            return await worker._job_already_delivered(job)

    async def test_a_delivered_job_is_recognised(self):
        for value in ("1", b"1"):
            self.assertTrue(await self._delivered({"job_id": "j1"}, self._JobRedis(value)))

    async def test_an_undelivered_job_is_not(self):
        for value in (None, "0", b"0"):
            self.assertFalse(await self._delivered({"job_id": "j1"}, self._JobRedis(value)))

    async def test_an_unknown_job_is_not_treated_as_delivered(self):
        self.assertFalse(await self._delivered({"job_id": "other"}, self._JobRedis("1")))
        self.assertFalse(await self._delivered({}, self._JobRedis("1")))

    async def test_redis_trouble_counts_as_not_delivered(self):
        # Failing closed here would silently drop real work, so it fails open.
        from workers import ffmpeg_worker as worker

        async def _boom():
            raise RuntimeError("no redis")

        with patch.object(worker, "get_redis", _boom):
            self.assertFalse(await worker._job_already_delivered({"job_id": "j1"}))

    def test_delivery_is_recorded_and_checked_before_the_slot_is_taken(self):
        src = read_source("workers", "ffmpeg_worker.py")
        self.assertIn('"delivered": "1" if sent else "0"', src)
        guard = src.index("if await _job_already_delivered(job):")
        slot = src.index("slot = await _claim_execution_slot(job)")
        self.assertLess(guard, slot)


class ActiveBatchViewTests(unittest.IsolatedAsyncioTestCase):
    """One bar for every running batch, rendered from the counters that exist."""

    def _redis_with(self, batch_id, done, total):
        return _ProgressRedis(
            initial={
                batch_pipeline.batch_total_key(batch_id): total,
                batch_pipeline.batch_progress_key(batch_id): done,
            }
        )

    async def test_lists_a_registered_batch_with_its_counters(self):
        redis = self._redis_with("b1", 2, 4)
        await batch_pipeline.register_active_batch(redis, batch_id="b1")
        rows = await batch_pipeline.read_active_batches(redis)
        self.assertEqual(rows, [{"batch_id": "b1", "total": 4, "done": 2}])

    async def test_prunes_a_batch_whose_counters_are_gone(self):
        # A batch whose worker died must not leave a phantom row behind.
        redis = _ProgressRedis()
        await batch_pipeline.register_active_batch(redis, batch_id="gone")
        self.assertEqual(await batch_pipeline.read_active_batches(redis), [])
        self.assertEqual(await redis.smembers(batch_pipeline.ACTIVE_BATCHES_KEY), set())

    async def test_least_finished_batch_is_listed_first(self):
        redis = self._redis_with("ahead", 8, 10)
        for key, value in self._redis_with("behind", 1, 10).store.items():
            redis.store[key] = value
        await batch_pipeline.register_active_batch(redis, batch_id="ahead")
        await batch_pipeline.register_active_batch(redis, batch_id="behind")
        self.assertEqual(
            [row["batch_id"] for row in await batch_pipeline.read_active_batches(redis)],
            ["behind", "ahead"],
        )

    async def test_unregistering_removes_a_finished_batch(self):
        redis = self._redis_with("b1", 4, 4)
        await batch_pipeline.register_active_batch(redis, batch_id="b1")
        await batch_pipeline.unregister_active_batch(redis, batch_id="b1")
        self.assertEqual(await batch_pipeline.read_active_batches(redis), [])

    def test_worker_rows_show_a_bar_and_a_short_id(self):
        from workers import ffmpeg_worker as worker

        rows = worker._batch_view_rows([{"batch_id": "abcdef123456", "done": 2, "total": 4}])
        self.assertEqual(len(rows), 1)
        self.assertIn("2/4", rows[0])
        self.assertIn("#abcdef12", rows[0])

    def test_worker_rows_skip_a_batch_with_no_total(self):
        from workers import ffmpeg_worker as worker

        self.assertEqual(worker._batch_view_rows([{"batch_id": "x", "done": 0, "total": 0}]), [])


class BatchMessageRefTests(unittest.TestCase):
    def test_round_trips_chat_and_message(self):
        self.assertEqual(batch_pipeline.parse_batch_message_ref("5:901"), (5, 901))
        self.assertEqual(batch_pipeline.parse_batch_message_ref(b"5:901"), (5, 901))

    def test_garbage_is_none(self):
        self.assertIsNone(batch_pipeline.parse_batch_message_ref(None))
        self.assertIsNone(batch_pipeline.parse_batch_message_ref("nope"))


class BatchCancelReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_batch_is_never_cancelled(self):
        # Called with no client and no batch, so it must not touch Redis at all.
        self.assertFalse(await batch_pipeline.is_batch_cancelled(batch_id=""))
        self.assertFalse(await batch_pipeline.is_batch_cancelled(batch_id=None))

    async def test_reads_the_marker_from_a_supplied_client(self):
        redis = _FakeRedis(exists_result=1)
        self.assertTrue(await batch_pipeline.is_batch_cancelled(redis, "b1"))
        self.assertEqual(redis.exists_calls[0], (batch_pipeline.batch_cancel_key("b1"),))


class BulkFetchReportingTests(unittest.TestCase):
    """A stopped batch must never be summarised as a fetch failure."""

    @staticmethod
    def _src(name: str = "handlers.py") -> str:
        return read_source(name)

    def test_failure_reason_names_the_real_cause(self):
        from handlers import _bulk_failure_reason

        self.assertEqual(_bulk_failure_reason(Exception("File is too big")), "too large for the Bot API")
        self.assertEqual(_bulk_failure_reason(Exception("exceeds the limit")), "too large for the Bot API")
        self.assertEqual(_bulk_failure_reason(Exception("batch cancelled")), "stopped")
        self.assertEqual(_bulk_failure_reason(Exception("relay fallback failed")), "relay group failed")
        self.assertEqual(_bulk_failure_reason(Exception("userbot download failed")), "userbot download failed")
        self.assertEqual(_bulk_failure_reason(Exception("RetryAfter")), "Telegram rate limit")
        self.assertEqual(_bulk_failure_reason(Exception("timed out")), "timed out")
        self.assertEqual(_bulk_failure_reason(Exception("")), "download failed")

    def test_a_stopped_file_is_reported_as_stopped_not_unfetchable(self):
        src = self._src()
        self.assertIn('if f.get("_batch_cancelled"):', src)
        self.assertIn("⏹️ stopped with the batch", src)

    def test_completion_is_checked_before_looking_for_a_local_file(self):
        # A conversion the pipeline already queued has no path and no key of its
        # own yet, so checking for a path first misreported it as unfetchable and
        # left it out of the batch count entirely.
        src = self._src()
        completed = src.index('if f.pop("_bulk_pipeline_completed", False):')
        path_check = src.index('if not _file_path and not f.get("input_key"):')
        self.assertLess(completed, path_check)

    def test_a_cancelled_batch_is_checked_before_the_relay_forward(self):
        # The relay forward happens before the pipeline ingests, so without a
        # check here Stop leaves a forwarded copy and processes nothing.
        src = self._src()
        self.assertIn('current_file["_batch_cancelled"] = True', src)
        guard = src.index("_is_cancelled_now(batch_id=_pre_batch_id)")
        forward = src.index("Relay: forwarding message %s/%s to relay group %s")
        self.assertLess(guard, forward)


class _RelayBot:
    """Bot stand-in that records (and can fail) delete_message calls."""

    def __init__(self, fail=False):
        self.fail = fail
        self.deleted = []

    async def delete_message(self, chat_id=None, message_id=None, **kwargs):
        if self.fail:
            raise RuntimeError("no rights")
        self.deleted.append((chat_id, message_id))


class _RelayContext:
    def __init__(self, bot):
        self.bot = bot


class RelayCopyCleanupTests(unittest.IsolatedAsyncioTestCase):
    """A relay copy forwarded for a batch is removed when that batch is stopped.

    The relay forward happens *before* the pipeline can refuse a stopped batch, so
    without this the batch's in-flight file is left sitting in a shared chat.
    """

    async def test_discards_the_copy(self):
        from handlers import EnhancedMediaHandler

        bot = _RelayBot()
        # The method does not touch `self`, so a bare object is enough here.
        ok = await EnhancedMediaHandler._discard_relay_copy(
            object(), _RelayContext(bot), -100123, 42, "batch cancelled"
        )
        self.assertTrue(ok)
        self.assertEqual(bot.deleted, [(-100123, 42)])

    async def test_casts_string_ids(self):
        from handlers import EnhancedMediaHandler

        bot = _RelayBot()
        await EnhancedMediaHandler._discard_relay_copy(object(), _RelayContext(bot), "-100123", "42")
        self.assertEqual(bot.deleted, [(-100123, 42)])

    async def test_useless_ids_are_a_noop(self):
        from handlers import EnhancedMediaHandler

        bot = _RelayBot()
        for chat, message in ((None, None), ("x", 1), (-100123, None)):
            self.assertFalse(
                await EnhancedMediaHandler._discard_relay_copy(object(), _RelayContext(bot), chat, message)
            )
        self.assertEqual(bot.deleted, [])

    async def test_a_delete_failure_never_raises(self):
        from handlers import EnhancedMediaHandler

        ok = await EnhancedMediaHandler._discard_relay_copy(object(), _RelayContext(_RelayBot(fail=True)), -100123, 42)
        self.assertFalse(ok)

    def test_a_stopped_batch_discards_its_relay_copy(self):
        src = BulkFetchReportingTests._src()
        self.assertIn('"batch cancelled"', src)
        discard = src.index(
            "_discard_relay_copy(\n                                    context, _pipeline_chat, _pipeline_msg"
        )
        cancelled = src.index('if _ingest.error == "batch cancelled":')
        self.assertLess(cancelled, discard)


class UserbotRelayCleanupTests(unittest.IsolatedAsyncioTestCase):
    """The userbot's own relay forward is cleaned up when its download fails."""

    class _Client:
        def __init__(self, fail=False):
            self.fail = fail
            self.deleted = []

        async def delete_messages(self, *args, **kwargs):
            if self.fail:
                raise RuntimeError("no rights")
            self.deleted.append((args, kwargs))

    async def test_discards_via_each_client_api(self):
        from utils.userbot_downloader import _discard_relay_copy

        pyro = self._Client()
        await _discard_relay_copy(pyro, "pyrogram", -100123, 42)
        self.assertEqual(pyro.deleted, [((), {"chat_id": -100123, "message_ids": [42]})])

        tele = self._Client()
        await _discard_relay_copy(tele, "telethon", -100123, 42)
        self.assertEqual(tele.deleted, [((-100123, [42]), {})])

    async def test_missing_ids_are_a_noop(self):
        from utils.userbot_downloader import _discard_relay_copy

        client = self._Client()
        for chat, message in ((None, 1), (-100123, None), (-100123, 0)):
            self.assertFalse(await _discard_relay_copy(client, "pyrogram", chat, message))
        self.assertEqual(client.deleted, [])

    async def test_a_delete_failure_never_raises(self):
        from utils.userbot_downloader import _discard_relay_copy

        self.assertFalse(await _discard_relay_copy(self._Client(fail=True), "pyrogram", -100123, 42))


class BulkPipelineWatchTests(unittest.IsolatedAsyncioTestCase):
    """A batch file's pipeline job is waited out properly *and* watched live.

    Two separate faults are pinned here. The pipeline used to edit the apply's own
    message with its download progress and then replace it with "Large file (N MB)
    queued for processing", which is how the batch id and the Stop button vanished
    mid-batch. And the job the fetch queued was waited for *inside* the fetch
    bound, with no watchdog at all, so a conversion longer than that bound was
    abandoned mid-encode and reported as unfetchable while the worker carried on.
    """

    def _src(self):
        return read_source("handlers.py")

    def test_a_batch_files_stages_render_on_the_applys_message(self):
        src = self._src()
        # One message per batch: whoever knows the current stage edits the message
        # the apply owns, always re-attaching the batch id and the Stop button.
        self.assertIn("_pipeline_progress_msg = _batch_message", src)
        self.assertIn("reply_markup=_batch_stop_markup(_pipeline_batch_id)", src)
        self.assertIn("_batch_member_text(", src)

    def test_a_batch_posts_no_per_file_message(self):
        src = self._src()
        # The branch a menu press takes: a batch always arrives with the apply's
        # message, and that branch may only edit it.
        branch = src[src.index("if _batch_message is not None:") : src.index("elif update and update.message:")]
        self.assertNotIn("reply_text", branch)
        self.assertNotIn("send_message", branch)
        self.assertIn("edit_text", branch)
        # Nothing hands a per-file message to another watcher any more.
        self.assertNotIn("_pipeline_batch_progress_msgs", src)

    def test_the_job_wait_is_outside_the_fetch_bound(self):
        src = self._src()
        bound = src.index("timeout=_BULK_FETCH_TIMEOUT_SECONDS")
        wait = src.index("await self._await_bulk_pipeline_job(")
        self.assertLess(bound, wait)

    def test_the_stage_tracks_the_workers_own_vocabulary(self):
        from handlers import _batch_member_stage

        self.assertEqual(_batch_member_stage({"status": "queued", "message": "queued"}), ("⏳", "queued"))
        self.assertEqual(
            _batch_member_stage({"status": "processing", "message": "fetching source from storage (812 MB)"}),
            ("⬇️", "Fetching source from storage"),
        )
        self.assertEqual(
            _batch_member_stage({"status": "processing", "message": "encoding 42.0%", "progress": "42"}),
            ("🎬", "Encoding — 42%"),
        )
        self.assertEqual(
            _batch_member_stage({"status": "uploading", "message": "Uploading to Telegram: 10%", "progress": "10"}),
            ("📤", "Sending to Telegram — 10%"),
        )
        self.assertEqual(_batch_member_stage({"status": "done"}), ("✅", "delivered"))
        self.assertEqual(_batch_member_stage({"status": "error", "message": "boom"}), ("❌", "boom"))
        self.assertEqual(
            _batch_member_stage({"status": "waiting", "message": "waiting for batch lock"}),
            ("⏳", "waiting for batch lock"),
        )
        # An unknown state is never mistaken for an ending.
        self.assertEqual(_batch_member_stage(None), ("⏳", "queued"))

    def test_the_fetch_no_longer_waits_for_the_whole_conversion(self):
        src = self._src()
        fetch = src.index("async def _ensure_bulk_file_downloaded(")
        body = src[fetch : src.index("async def _watch_batch_member(")]
        self.assertNotIn("_await_job_finished(", body)
        self.assertIn('file_info["_bulk_pipeline_job_pending"] = pipeline_job_id', body)

    def test_a_batch_member_gets_the_stage_watcher(self):
        src = self._src()
        # The wait helper starts it, so every member the apply waits on is covered.
        waiter = src.index("async def _await_member_job(")
        body = src[waiter : src.index("async def _await_bulk_pipeline_job(")]
        self.assertIn("self._watch_batch_member(", body)
        self.assertIn("await self._await_job_finished(job_id)", body)
        # The stage watcher never posts or deletes: the apply's message outlives it.
        watch_body = src[src.index("async def _watch_batch_member(") : waiter]
        self.assertNotIn("send_message", watch_body)
        self.assertNotIn(".delete()", watch_body)
        self.assertIn("_batch_stop_markup(batch_id)", watch_body)

    async def _wait(self, file_info, status, *, query=None, watched=None):
        from handlers import EnhancedMediaHandler

        calls = []

        async def _finished(_self, job_id, **kwargs):
            calls.append(job_id)
            return status

        async def _watch(_self, query, **kwargs):
            if watched is not None:
                watched.append(kwargs)

        # An instance without __init__: the method only ever reads Redis through
        # `self`, and the class-level patches are what it must resolve.
        handler = object.__new__(EnhancedMediaHandler)
        with (
            patch.object(EnhancedMediaHandler, "_await_job_finished", _finished),
            patch.object(EnhancedMediaHandler, "_watch_batch_member", _watch),
        ):
            await handler._await_bulk_pipeline_job(file_info, query=query, index=1, total=9)
            # The watcher is a task; give it the shortest yield there is.
            await asyncio.sleep(0)
        return calls

    async def test_a_finished_pipeline_job_counts_as_completed(self):
        info = {"_bulk_pipeline_job_pending": "job-1"}
        calls = await self._wait(info, "done")
        self.assertTrue(info["_bulk_pipeline_completed"])
        self.assertNotIn("_bulk_pipeline_job_pending", info)
        self.assertEqual(calls, ["job-1"])

    async def test_a_stopped_pipeline_job_stops_the_batch(self):
        info = {"_bulk_pipeline_job_pending": "job-1"}
        await self._wait(info, "cancelled")
        self.assertTrue(info["_batch_cancelled"])
        self.assertFalse(info.get("_bulk_pipeline_completed"))

    async def test_an_errored_pipeline_job_is_never_enqueued_twice(self):
        info = {"_bulk_pipeline_job_pending": "job-1"}
        await self._wait(info, "error")
        self.assertTrue(info["_pipeline_failed"])

    async def test_a_wait_that_gave_up_leaves_the_file_pending(self):
        """Handed off is not finished.

        The job still exists and will deliver, so queueing a second one for the
        same file would be worse than waiting on the one already queued - but the
        file is *not* complete: marking it so put a merely-queued file into the
        "✅ finished" summary, remembered it as done in the resume record (so a
        later Apply skipped a file nobody had converted) and let the batch report
        itself finished while its work was still outstanding.
        """
        info = {"_bulk_pipeline_job_pending": "job-1"}
        await self._wait(info, None)

        self.assertFalse(info.get("_bulk_pipeline_completed"))
        self.assertEqual(info.get("_bulk_pipeline_pending"), "job-1")

    async def test_a_batch_that_still_has_queued_files_does_not_report_itself_finished(self):
        """The summary is the one place the user learns what actually happened."""
        src = read_source("handlers.py")
        assert_start = src.index('if f.get("_bulk_pipeline_pending"):')
        # Through the summary the user reads, which is built at the end of the apply.
        apply_body = src[assert_start : src.index("await self.safe_edit(query, _head, reply_markup=None)")]

        # A file the wait gave up on is reported as still queued, and counted
        # apart from the finished ones.
        self.assertIn('still queued · {f.get("_bulk_pipeline_pending")}', apply_body)
        self.assertIn("pending += 1", apply_body)
        # The header only claims "finished" when nothing is outstanding.
        self.assertIn("elif pending:", apply_body)
        self.assertIn("queued and will arrive on their own", apply_body)
        self.assertIn("Bulk apply handed off", apply_body)
        # And the batch is not closed out from under that work: its message stays
        # up while the job it describes is still running, and its resume record
        # survives (those entries are not finished).
        self.assertIn("if not pending:", apply_body)
        self.assertIn("if not (stopped or stalled or pending):", apply_body)

    def test_a_finished_member_is_not_left_reading_queued(self):
        """The queueing line is a placeholder, not the file's last word.

        The apply writes "📋 queued · <job>" before it waits, and only the pipeline
        path ever replaced that line - so every file the apply queued itself kept
        reading "queued" in the final summary, delivered or not: work the user had
        already received looked like work still waiting, and a file the worker had
        failed or a stop had cancelled looked the same.
        """
        src = self._src()
        assert_start = src.index("_result_idx = len(results)")
        # Through the summary the user reads, which is built at the end of the apply.
        apply_body = src[assert_start : src.index("await self.safe_edit(query, _head, reply_markup=None)")]

        # The outcome is written back onto that same line, per terminal status.
        self.assertIn('f"✅ completed · {job_id}"', apply_body)
        self.assertIn('f"❌ conversion failed · {job_id}"', apply_body)
        self.assertIn('f"⏹️ cancelled · {job_id}"', apply_body)
        self.assertIn("results[_result_idx] = (", apply_body)
        # A failure counts as one, and a cancellation is neither failed nor queued.
        self.assertIn("failed += 1", apply_body)
        self.assertIn("cancelled_members += 1", apply_body)
        # The header names cancellations too, and keeps its batch-id/one-at-a-time
        # lines off a run that has one.
        self.assertIn("file(s) were cancelled.", apply_body)
        self.assertIn("bool(cancelled_members)", apply_body)

    async def test_a_file_with_no_pipeline_job_is_left_alone(self):
        calls = await self._wait({}, "done")
        self.assertEqual(calls, [])

    async def test_the_stage_watcher_is_bound_to_the_batch_message(self):
        watched = []
        info = {"_bulk_pipeline_job_pending": "job-1", "_pipeline_batch_id": "batch-a", "name": "clip.mp4"}
        await self._wait(info, "done", query=object(), watched=watched)

        self.assertEqual(
            watched,
            [{"batch_id": "batch-a", "job_id": "job-1", "index": 1, "total": 9, "name": "clip.mp4"}],
        )

    async def test_no_watcher_is_started_without_a_batch_message(self):
        # A pipeline job somebody else owns (a single-file run, or a batch whose
        # entries never produced a message) must not spawn a watcher with no
        # message to render onto.
        watched = []
        info = {"_bulk_pipeline_job_pending": "job-1"}
        await self._wait(info, "done", query=None, watched=watched)

        self.assertEqual(watched, [])

    async def test_the_stage_watcher_renders_each_stage_then_stops(self):
        import handlers as handlers_module
        from handlers import EnhancedMediaHandler
        from utils import job_queue

        edited = []

        class _Query:
            async def edit_message_text(self, text, **kwargs):
                edited.append(text)
                # PTB returns the edited Message, and the stage watcher reads a
                # falsy result as "the edit was dropped, retry it" - so a fake that
                # returns None would look like a chat Telegram is refusing to edit.
                return SimpleNamespace(message_id=1)

        class _Redis:
            def __init__(self, rows):
                self.rows = list(rows)

            async def hgetall(self, key):
                return self.rows.pop(0) if self.rows else {"status": "done"}

            async def close(self):
                return None

        rows = [
            {"status": "processing", "message": "encoding 42.0%", "progress": "42"},
            {"status": "done", "message": "delivered", "progress": "100"},
        ]

        async def _get_redis():
            return _Redis(rows)

        handler = object.__new__(EnhancedMediaHandler)
        with (
            patch.object(job_queue, "get_redis", _get_redis),
            patch.object(handlers_module, "_BATCH_MEMBER_POLL_SECONDS", 0),
        ):
            await handler._watch_batch_member(
                _Query(), batch_id="batch-a", job_id="job-1", index=1, total=9, name="clip.mp4"
            )

        self.assertEqual(len(edited), 2)
        self.assertIn("▶️ Batch `batch-a`", edited[0])
        self.assertIn("File 1 of 9 — clip.mp4", edited[0])
        self.assertIn("🎬 Encoding — 42%", edited[0])
        self.assertIn("✅ delivered", edited[1])


class SourceFetchTests(unittest.IsolatedAsyncioTestCase):
    """A member fetching its source from storage must be legible and bounded.

    The first thing a pipeline job does is pull its source out of storage, minutes
    of transfer for a large video. That used to leave the job hash on the "queued"
    that ``enqueue_job`` wrote (so an in-flight fetch looked exactly like a member
    nobody had picked up - the watchdog sat on "queued / Progress: 0%") and had no
    timeout at all, so a stalled transfer held the batch lock and the only
    conversion slot for good and froze every later member of its batch.
    """

    def _src(self):
        return read_source("workers", "ffmpeg_worker.py")

    def test_the_fetch_is_reported_and_bounded(self):
        src = self._src()
        self.assertIn('_set_job_state(job_id, "processing", _fetch_note', src)
        self.assertIn("timeout=download_timeout", src)
        # A timeout is a failed attempt, so the retries below it still apply.
        self.assertIn("except TimeoutError:", src)

    def test_the_reported_bound_grows_with_the_source(self):
        from workers import ffmpeg_worker as worker

        floor = worker.STORAGE_DOWNLOAD_TIMEOUT_SECONDS
        self.assertEqual(worker._storage_download_timeout_seconds(0), floor)
        self.assertEqual(worker._storage_download_timeout_seconds(None), floor)
        self.assertEqual(worker._storage_download_timeout_seconds("nope"), floor)
        self.assertEqual(worker._storage_download_timeout_seconds(-5), floor)
        # A size that fits inside the floor keeps the floor; a bigger one gets more
        # room, and never more than the cap.
        self.assertEqual(worker._storage_download_timeout_seconds(2 * 1024**3), 8192)
        cap = worker.STORAGE_DOWNLOAD_MAX_SECONDS
        self.assertEqual(worker._storage_download_timeout_seconds(cap * 256 * 1024 * 4), cap)

    async def test_every_terminal_failure_reaches_the_hash(self):
        """The hash is what the bot polls; Mongo and the channel are not enough."""
        from workers import ffmpeg_worker as worker

        written = []

        class _Redis:
            async def hset(self, key, mapping=None, **kwargs):
                written.append((key, dict(mapping or {})))
                return 1

            async def close(self):
                return None

        async def _get_redis():
            return _Redis()

        with patch.object(worker, "get_redis", _get_redis):
            await worker._set_job_state("job-1", "error", "the source is missing from storage", progress=0)
            await worker._set_job_state("", "error", "ignored")

        self.assertEqual(
            written,
            [
                (
                    "ffmpeg:job:job-1",
                    {"status": "error", "message": "the source is missing from storage", "progress": "0"},
                )
            ],
        )

    def test_the_give_up_paths_write_the_hash(self):
        src = self._src()
        # The two source-fetch failures and the two give-up paths inside
        # handle_job's retry loop, plus the helper's own definition.
        self.assertGreaterEqual(src.count("_set_job_state("), 5)
        self.assertIn('_set_job_state(job_id, "error", "the source is missing from storage"', src)
        # The fetch failure still writes the hash; which wording it uses is
        # decided at the call site, because a job whose stored object is only a
        # probe header has to say where the media should have come from.
        self.assertIn('_set_job_state(job_id, "error", _fetch_error', src)
        self.assertIn('"could not fetch the source from storage"', src)
        self.assertIn('"processing failed", progress=0, channel=progress_channel', src)
        self.assertIn('str(info or "conversion failed"),', src)


class GhostClaimTests(unittest.IsolatedAsyncioTestCase):
    """A claim left behind by a dead worker must not fence the queue.

    The case these guard is the one no status can describe: a worker killed
    mid-encode (OOM kill, redeploy) still has a job hash reading ``processing``
    and never got to release the slot it took. Only the claiming worker's own
    heartbeat can tell that apart from a live, slow conversion.
    """

    class _Claims:
        """Claims, job hashes and worker heartbeats the ghost checks read."""

        def __init__(self, *, claims=None, jobs=None, workers=()):
            self.strings = dict(claims or {})
            self.hashes = {key: dict(value) for key, value in (jobs or {}).items()}
            self.workers = {f"{batch_pipeline.WORKER_RSS_KEY_PREFIX}{name}" for name in workers}
            self.hset_calls = []

        @staticmethod
        def _text(value):
            return value.decode() if isinstance(value, (bytes, bytearray)) else str(value)

        async def get(self, key):
            return self.strings.get(self._text(key))

        async def set(self, key, value, nx=False, px=None, ex=None):
            self.strings[self._text(key)] = str(value)
            return True

        async def hget(self, key, field):
            return self.hashes.get(self._text(key), {}).get(field)

        async def hset(self, key, mapping=None, **kwargs):
            fields = {**dict(mapping or {}), **kwargs}
            self.hashes.setdefault(self._text(key), {}).update({str(k): str(v) for k, v in fields.items()})
            self.hset_calls.append((self._text(key), dict(fields)))
            return len(fields)

        async def delete(self, *keys):
            removed = 0
            for key in keys:
                key = self._text(key)
                if key in self.strings:
                    del self.strings[key]
                    removed += 1
            return removed

        async def exists(self, *keys):
            return sum(
                1
                for key in keys
                if self._text(key) in self.strings or self._text(key) in self.workers or self._text(key) in self.hashes
            )

        async def eval(self, script, numkeys, *args):
            return 1

        def scan_iter(self, match="*", count=100):
            prefix = match[:-1] if match.endswith("*") else match
            keys = sorted(set(self.strings) | self.workers | set(self.hashes))

            async def _gen():
                for key in keys:
                    if key.startswith(prefix):
                        yield key

            return _gen()

        async def close(self):
            return None

    async def test_a_processing_claim_whose_worker_is_gone_is_a_ghost(self):
        # The hash still says "processing" - the worker was killed before it could
        # write anything else - so only the missing heartbeat gives it away.
        redis = self._Claims(
            claims={"ffmpeg:slot:0": "job-dead"},
            jobs={"ffmpeg:job:job-dead": {"status": "processing", "worker": "w-dead"}},
            workers=("w-alive",),
        )

        self.assertTrue(await batch_pipeline._slot_owner_is_gone(redis, "job-dead"))
        self.assertEqual(await batch_pipeline.sweep_ghost_claims(redis), {"slots": 1, "locks": 0, "dedup": 0})
        self.assertNotIn("ffmpeg:slot:0", redis.strings)

    async def test_a_live_workers_claim_is_never_taken(self):
        redis = self._Claims(
            claims={"ffmpeg:slot:0": "job-live"},
            jobs={"ffmpeg:job:job-live": {"status": "processing", "worker": "w-alive"}},
            workers=("w-alive",),
        )

        self.assertFalse(await batch_pipeline._slot_owner_is_gone(redis, "job-live"))
        self.assertEqual(await batch_pipeline.sweep_ghost_claims(redis), {"slots": 0, "locks": 0, "dedup": 0})
        self.assertEqual(redis.strings["ffmpeg:slot:0"], "job-live")

    async def test_a_finished_claim_is_left_to_the_worker_that_owns_it(self):
        # Terminal, but its worker is alive and about to release it. Stealing it
        # here would start a second conversion beside one that is still winding
        # down, which is what the one-slot cap exists to prevent.
        redis = self._Claims(
            claims={"ffmpeg:slot:0": "job-done"},
            jobs={"ffmpeg:job:job-done": {"status": "done", "worker": "w-alive"}},
            workers=("w-alive",),
        )

        self.assertFalse(await batch_pipeline._slot_owner_is_gone(redis, "job-done"))

    async def test_a_claim_from_before_the_stamp_falls_back_to_its_status(self):
        redis = self._Claims(jobs={"ffmpeg:job:old": {"status": "done"}})

        self.assertIsNone(await batch_pipeline._claiming_worker_is_gone(redis, "old"))
        self.assertTrue(await batch_pipeline._slot_owner_is_gone(redis, "old"))
        self.assertTrue(await batch_pipeline._slot_owner_is_gone(redis, "never-existed"))

    async def test_a_ghost_batch_lock_is_swept(self):
        # ``ffmpeg:batch:<id>`` is the lock; the old ``*:lock`` pattern matched
        # nothing, so a ghost one froze its batch for its whole TTL.
        redis = self._Claims(
            claims={
                "ffmpeg:batch:abc": "job-dead",
                "ffmpeg:batch:abc:done": "2",
                "ffmpeg:batch:abc:cancelled": "cancelled by admin|1",
                "ffmpeg:batch:resume:42": "member",
                "ffmpeg:batch:active": "member",
            },
            jobs={"ffmpeg:job:job-dead": {"status": "processing", "worker": "w-dead"}},
        )

        self.assertEqual(await batch_pipeline.sweep_ghost_claims(redis), {"slots": 0, "locks": 1, "dedup": 0})
        self.assertNotIn("ffmpeg:batch:abc", redis.strings)
        # Not one of the batch's own keys is a claim.
        for key in (
            "ffmpeg:batch:abc:done",
            "ffmpeg:batch:abc:cancelled",
            "ffmpeg:batch:resume:42",
            "ffmpeg:batch:active",
        ):
            self.assertIn(key, redis.strings)

    async def test_a_dedup_key_whose_owner_worker_died_is_dropped(self):
        """The file-level ghost: nothing behind the key, so the file is stuck.

        Unlike a claim, nothing releases a dedup key on the owner's behalf: the
        apply-time check treats a ``processing`` owner as live and skips the file,
        so the only signal that the worker is gone is its missing heartbeat.
        """
        key = f"{batch_pipeline.PIPELINE_DEDUP_PREFIX}42:BAhJ"
        redis = self._Claims(
            claims={key: "job-dead"},
            jobs={"ffmpeg:job:job-dead": {"status": "processing", "worker": "w-dead"}},
            workers=("w-alive",),
        )

        freed = await batch_pipeline.sweep_ghost_claims(redis)

        self.assertEqual(freed, {"slots": 0, "locks": 0, "dedup": 1})
        self.assertNotIn(key, redis.strings)

    async def test_a_dedup_key_of_a_live_worker_is_kept(self):
        key = f"{batch_pipeline.PIPELINE_DEDUP_PREFIX}42:BAhJ"
        redis = self._Claims(
            claims={key: "job-live"},
            jobs={"ffmpeg:job:job-live": {"status": "processing", "worker": "w-alive"}},
            workers=("w-alive",),
        )

        self.assertEqual(await batch_pipeline.sweep_ghost_claims(redis), {"slots": 0, "locks": 0, "dedup": 0})
        self.assertEqual(redis.strings[key], "job-live")

    async def test_a_queued_dedup_key_is_kept(self):
        # A job waiting in the queue owns its input, and it has no worker stamp yet
        # because it has not been claimed - dropping it would let the same file be
        # ingested twice.
        key = f"{batch_pipeline.PIPELINE_DEDUP_PREFIX}42:BAhJ"
        redis = self._Claims(
            claims={key: "job-queued"},
            jobs={"ffmpeg:job:job-queued": {"status": "queued"}},
        )

        self.assertEqual((await batch_pipeline.sweep_ghost_claims(redis))["dedup"], 0)
        self.assertIn(key, redis.strings)

    async def test_a_dedup_key_whose_owner_is_gone_is_dropped(self):
        gone = f"{batch_pipeline.PIPELINE_DEDUP_PREFIX}42:gone"
        delivered = f"{batch_pipeline.PIPELINE_DEDUP_PREFIX}42:delivered"
        finished = f"{batch_pipeline.PIPELINE_DEDUP_PREFIX}42:finished"
        redis = self._Claims(
            claims={gone: "job-that-never-existed", delivered: "job-done", finished: ""},
            jobs={"ffmpeg:job:job-done": {"status": "done", "worker": "w-dead"}},
        )

        self.assertEqual((await batch_pipeline.sweep_ghost_claims(redis))["dedup"], 3)
        self.assertEqual(redis.strings, {})

    async def test_a_pending_dedup_key_is_dropped(self):
        # A crashed ingest leaves this behind and only this placeholder blocks the
        # file: there is no owner for the apply-time check to inspect, and it treats
        # "pending" as live, so nothing else would ever clear it.
        key = f"{batch_pipeline.PIPELINE_DEDUP_PREFIX}42:BAhJ"
        redis = self._Claims(claims={key: batch_pipeline._PENDING_DEDUP_VALUE})

        self.assertEqual((await batch_pipeline.sweep_ghost_claims(redis))["dedup"], 1)
        self.assertNotIn(key, redis.strings)

    async def test_the_worker_stamps_its_identity_when_it_takes_a_claim(self):
        from workers import ffmpeg_worker as worker

        redis = self._Claims()

        with (
            patch.object(batch_pipeline, "MEMORY_CEILING_BYTES", 0),
            patch.object(worker, "get_redis", _returning(redis)),
        ):
            slot = await worker._claim_execution_slot({"job_id": "job-9"})

        self.assertEqual(slot, 0)
        self.assertEqual(redis.hashes["ffmpeg:job:job-9"]["worker"], batch_pipeline.worker_identity())


class ActiveConversionRaceTests(unittest.IsolatedAsyncioTestCase):
    """Two files for the same user must not clobber each other's tracking.

    Without a per-user counter, coroutine A finishing would pop the entry
    that coroutine B set, leaving B's conversion untracked.
    """

    def _handler(self):
        from handlers import EnhancedMediaHandler

        return object.__new__(EnhancedMediaHandler)

    async def test_overlapping_conversions_both_tracked(self):
        handler = self._handler()
        handler.converter = None
        handler.conversion_semaphore = asyncio.Semaphore(5)
        handler.active_conversions = {}
        handler._active_conversion_count = {}

        gate1 = asyncio.Event()
        gate2 = asyncio.Event()

        async def slow(gate):
            await gate.wait()

        # Start two conversions for the same user.
        t1 = asyncio.create_task(handler._run_with_concurrency_limit(42, "file_a", slow(gate1)))
        t2 = asyncio.create_task(handler._run_with_concurrency_limit(42, "file_b", slow(gate2)))
        await asyncio.sleep(0)  # let both acquire the semaphore

        # Both are running; the counter should be 2.
        self.assertEqual(handler._active_conversion_count.get(42), 2)
        self.assertIn(42, handler.active_conversions)
        self.assertEqual(len(handler.active_conversions), 1)

        # Finish the first; the second must still be tracked.
        gate1.set()
        await asyncio.sleep(0)
        await t1
        self.assertIn(42, handler.active_conversions)
        self.assertEqual(handler._active_conversion_count.get(42), 1)

        # Finish the second; entry fully cleaned up.
        gate2.set()
        await asyncio.sleep(0)
        await t2
        self.assertNotIn(42, handler.active_conversions)
        self.assertNotIn(42, handler._active_conversion_count)

    async def test_single_conversion_cleans_up_normally(self):
        handler = self._handler()
        handler.converter = None
        handler.conversion_semaphore = asyncio.Semaphore(5)
        handler.active_conversions = {}
        handler._active_conversion_count = {}

        async def quick():
            return "done"

        result = await handler._run_with_concurrency_limit(99, "single", quick())
        self.assertEqual(result, "done")
        self.assertNotIn(99, handler.active_conversions)
        self.assertNotIn(99, handler._active_conversion_count)

    async def test_second_file_overwrites_task_name_while_first_runs(self):
        handler = self._handler()
        handler.converter = None
        handler.conversion_semaphore = asyncio.Semaphore(5)
        handler.active_conversions = {}
        handler._active_conversion_count = {}

        gate1 = asyncio.Event()
        gate2 = asyncio.Event()

        async def slow(gate):
            await gate.wait()

        t1 = asyncio.create_task(handler._run_with_concurrency_limit(7, "file_1", slow(gate1)))
        await asyncio.sleep(0)

        # active_conversions shows file_1.
        self.assertEqual(handler.active_conversions[7], "file_1")
        self.assertEqual(handler._active_conversion_count[7], 1)

        # Second file starts — overwrites the name but increments the counter.
        t2 = asyncio.create_task(handler._run_with_concurrency_limit(7, "file_2", slow(gate2)))
        await asyncio.sleep(0)
        self.assertEqual(handler.active_conversions[7], "file_2")
        self.assertEqual(handler._active_conversion_count[7], 2)

        # First finishes — must NOT erase the entry.
        gate1.set()
        await asyncio.sleep(0)
        await t1
        self.assertIn(7, handler.active_conversions)
        self.assertEqual(handler._active_conversion_count[7], 1)

        # Second finishes — entry fully cleaned up.
        gate2.set()
        await asyncio.sleep(0)
        await t2
        self.assertNotIn(7, handler.active_conversions)
        self.assertNotIn(7, handler._active_conversion_count)

    async def test_different_users_do_not_interfere(self):
        handler = self._handler()
        handler.converter = None
        handler.conversion_semaphore = asyncio.Semaphore(5)
        handler.active_conversions = {}
        handler._active_conversion_count = {}

        gate = asyncio.Event()

        async def slow():
            await gate.wait()

        t1 = asyncio.create_task(handler._run_with_concurrency_limit(1, "a", slow()))
        t2 = asyncio.create_task(handler._run_with_concurrency_limit(2, "b", slow()))
        await asyncio.sleep(0)

        self.assertEqual(len(handler.active_conversions), 2)
        self.assertEqual(handler._active_conversion_count[1], 1)
        self.assertEqual(handler._active_conversion_count[2], 1)

        gate.set()
        await asyncio.sleep(0)
        await t1
        await t2

        self.assertEqual(len(handler.active_conversions), 0)


if __name__ == "__main__":
    unittest.main()
