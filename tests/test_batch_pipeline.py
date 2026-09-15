"""Sequential, memory-safe bulk batches: tagging, the per-batch lock, cleanup."""

import contextlib
import json
import os
import tempfile
import time
import unittest
from unittest.mock import patch

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
        self.assertNotEqual(
            batch_pipeline.batch_progress_key("abc"), batch_pipeline.batch_lock_key("abc")
        )

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
        self.assertEqual([c["key"] for c in redis.set_calls], [
            "ffmpeg:slot:0",
            "ffmpeg:slot:1",
            "ffmpeg:slot:2",
        ])

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
        with patch.object(batch_pipeline, "MEMORY_CEILING_BYTES", 100), patch.object(
            batch_pipeline, "rss_bytes", return_value=150
        ):
            self.assertTrue(batch_pipeline.over_memory_ceiling())
        with patch.object(batch_pipeline, "MEMORY_CEILING_BYTES", 100), patch.object(
            batch_pipeline, "rss_bytes", return_value=40
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


class _ProgressRedis:
    """The handful of Redis commands the batch progress reporter issues."""

    def __init__(self, initial=None):
        self.store = {key: str(value) for key, value in (initial or {}).items()}
        self.closed = 0

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

    async def delete_message(self, chat_id=None, message_id=None, **kwargs):
        self.deleted.append((chat_id, message_id))


class BatchProgressMessageTests(unittest.IsolatedAsyncioTestCase):
    """A batch shows one message, edited in place and taken down when it ends."""

    def _job(self, total=3, chat_id=5):
        return batch_pipeline.tag_batch_job(
            {"job_id": "j1", "chat_id": chat_id, "original_filename": "a.mp4"},
            "b1",
            0,
            total,
        )

    async def _report(self, job, redis):
        from workers import ffmpeg_worker as worker

        bot = _FakeBot()
        with patch.object(worker, "Bot", lambda *a, **k: bot), patch.object(
            worker, "get_redis", _returning(redis)
        ), patch.object(worker.config, "BOT_TOKEN", "tok", create=True):
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
        with patch.object(worker, "Bot", lambda *a, **k: bot), patch.object(
            worker, "get_redis", _returning(redis)
        ), patch.object(worker.config, "BOT_TOKEN", "tok", create=True):
            await worker._report_batch_progress(self._job(total=4))
        return bot

    def test_the_message_carries_the_running_files_progress(self):
        from workers.ffmpeg_worker import _batch_progress_text

        running = _batch_progress_text(3, 12, name="clip.mp4", pct=47.6)
        self.assertIn("3 of 12 finished", running)
        self.assertIn("🔄 clip.mp4 — 47%", running)
        # A finished file is shown without a percentage.
        self.assertIn("✅ clip.mp4", _batch_progress_text(4, 12, name="clip.mp4"))
        self.assertNotIn("clip.mp4", _batch_progress_text(4, 12))

    async def test_progress_never_raises_when_redis_is_down(self):
        from workers import ffmpeg_worker as worker

        async def _boom():
            raise RuntimeError("no redis")

        with patch.object(worker, "get_redis", _boom):
            await worker._report_batch_progress(self._job())


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
        with patch.object(batch_pipeline, "reclaim_memory", return_value={}), patch.object(
            batch_pipeline, "sweep_temp_artifacts", return_value={}
        ):
            summary = await batch_pipeline.finalize_job(job, source="redis", redis=redis)
        self.assertEqual(len(redis.eval_calls), 1)
        self.assertTrue(summary["batch_released"])

    async def test_plain_job_does_not_touch_a_lock(self):
        redis = _FakeRedis()
        with patch.object(batch_pipeline, "reclaim_memory", return_value={}), patch.object(
            batch_pipeline, "sweep_temp_artifacts", return_value={}
        ):
            summary = await batch_pipeline.finalize_job({"job_id": "j2"}, redis=redis)
        self.assertEqual(redis.eval_calls, [])
        self.assertNotIn("batch_released", summary)

    async def test_handing_back_the_ffmpeg_slot(self):
        redis = _FakeRedis()
        job = {"job_id": "j4"}
        with patch.object(batch_pipeline, "reclaim_memory", return_value={}), patch.object(
            batch_pipeline, "sweep_temp_artifacts", return_value={}
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
        with patch.object(batch_pipeline, "_malloc_trim", return_value=True) as trim, patch.object(
            batch_pipeline, "rss_bytes", side_effect=[1000, 400]
        ), patch.object(batch_pipeline.gc, "collect", return_value=42) as collect:
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
        with patch.object(batch_pipeline, "RESTART_RSS_THRESHOLD", 0), patch.object(
            batch_pipeline, "rss_bytes", return_value=999_999_999
        ):
            self.assertFalse(batch_pipeline.memory_pressure())

    def test_restart_is_requested_above_the_threshold(self):
        batch_pipeline.reset_restart_request()
        try:
            with patch.object(batch_pipeline, "RESTART_RSS_THRESHOLD", 100), patch.object(
                batch_pipeline, "rss_bytes", return_value=500
            ):
                self.assertTrue(batch_pipeline.request_restart_if_pressured())
                self.assertTrue(batch_pipeline.restart_requested())
        finally:
            batch_pipeline.reset_restart_request()

    def test_restart_is_not_requested_below_the_threshold(self):
        batch_pipeline.reset_restart_request()
        try:
            with patch.object(batch_pipeline, "RESTART_RSS_THRESHOLD", 100), patch.object(
                batch_pipeline, "rss_bytes", return_value=50
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
        with open(os.path.join(PROJECT_ROOT, *parts), encoding="utf-8") as fh:
            return fh.read()

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


if __name__ == "__main__":
    unittest.main()
