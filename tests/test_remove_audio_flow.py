"""🔉 Remove Audio: the same steps as every other action button.

The button used to diverge from its neighbours in three ways, and each one is
pinned here:

* it never declared what a pipeline fetch should produce, so a source past the
  Bot API ceiling was queued with no ffmpeg arguments and came back unchanged;
* it ran the removal inline, with no watcher on the job;
* it sent the result with its own ``_send_video_result`` call instead of the
  shared, size-tiered delivery.

These tests drive the real method with stub collaborators so the order of the
steps - download, pipeline/watchdog, validation, operation, result - is what is
checked, not the source text.
"""

import asyncio
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import handlers as handlers_module
from handlers import EnhancedMediaHandler

Handler = EnhancedMediaHandler


class _FakeMessage:
    def __init__(self, replies):
        self.replies = replies

    async def edit_text(self, text, **kwargs):
        self.replies.append((text, kwargs))
        return SimpleNamespace(message_id=1)


class _FakeUpdate:
    def __init__(self, replies):
        self.callback_query = SimpleNamespace(message=_FakeMessage(replies), data="remove_audio")
        self.effective_chat = SimpleNamespace(id=99)
        self.effective_user = SimpleNamespace(id=7)


class _FakeContext:
    def __init__(self):
        self.bot = SimpleNamespace()


class _Converter:
    def __init__(self):
        self.removed_from = None

    async def remove_audio(self, input_path, output_path):
        self.removed_from = (input_path, output_path)
        with open(output_path, "wb") as fh:
            fh.write(b"stripped")
        return True


class _Stub:
    """The handler surface remove_audio talks to, recording every hand-off."""

    def __init__(self, path, *, pipeline_job=None, queued=True, has_audio=True):
        self.converter = _Converter()
        self.path = path
        self.replies = []
        self.cancelled = []
        self.watched = []
        self.keyed_jobs = []
        self.worker_jobs = []
        self.delivered = []
        self.queued = queued
        self.has_audio = has_audio
        self.pipeline_at_download = None
        self.pipeline_job = pipeline_job

    async def _require_callback(self, update):
        return True

    async def _check_conversion_quota(self, update, context):
        return True

    async def safe_edit(self, query, text, **kwargs):
        self.replies.append((text, kwargs))
        return None

    async def _cancel_stale_pipeline_job(self, session, handler_name, user_id=None):
        self.cancelled.append(handler_name)
        return True

    async def _ensure_local_media(self, update, context, session, current_file, **kwargs):
        # The order under test: the pipeline must already know what to produce
        # before it is asked to fetch the media.
        self.pipeline_at_download = {
            "type": current_file.get("_pipeline_conversion_type"),
            "args": current_file.get("_pipeline_ffmpeg_args"),
            "ext": current_file.get("_pipeline_output_ext"),
        }
        if self.pipeline_job:
            current_file["_pipeline_job_id"] = self.pipeline_job
        return current_file, None

    def _local_copy(self, current_file):
        return self.path

    async def _watch_pipeline_job(self, update, context, job_id, summary, **kwargs):
        self.watched.append(("pipeline", job_id))

    async def _enqueue_keyed_job(self, update, context, current_file, **kwargs):
        self.keyed_jobs.append(kwargs)
        return self.queued

    async def _enqueue_worker_job(self, update, context, current_file, **kwargs):
        self.worker_jobs.append(kwargs)
        return self.queued

    async def _deliver_inline_result(self, update, context, output_path, current_file, **kwargs):
        self.delivered.append((output_path, kwargs))
        return True

    def bind_remove_audio(self):
        self.remove_audio = Handler.remove_audio.__get__(self, Handler)
        return self


def _video_file():
    return {
        "id": "abc123",
        "name": "clip.mp4",
        "path": None,
        "type": "video",
        "_source_metadata": {},
    }


class RemoveAudioFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output_patch = patch.object(handlers_module, "config", SimpleNamespace(OUTPUT_PATH=self.tmp.name))
        self.output_patch.start()

    def tearDown(self):
        self.output_patch.stop()
        self.tmp.cleanup()

    def _run(self, stub, session):
        asyncio.run(stub.remove_audio(_FakeUpdate(stub.replies), _FakeContext(), session))

    def test_the_pipeline_knows_what_to_produce_before_it_fetches(self):
        source = os.path.join(self.tmp.name, "clip.mp4")
        with open(source, "wb") as fh:
            fh.write(b"video")
        stub = _Stub(source, queued=False).bind_remove_audio()
        self._run(stub, {"current_file": _video_file()})

        self.assertEqual(stub.pipeline_at_download["type"], "remove_audio")
        self.assertEqual(stub.pipeline_at_download["args"], ["-an", "-c:v", "copy"])
        self.assertEqual(stub.pipeline_at_download["ext"], ".mp4")

    def test_a_pipeline_fetch_owns_the_conversion_and_is_only_watched(self):
        source = os.path.join(self.tmp.name, "clip.mp4")
        with open(source, "wb") as fh:
            fh.write(b"video")
        stub = _Stub(source, pipeline_job="job-1").bind_remove_audio()
        self._run(stub, {"current_file": _video_file()})

        self.assertEqual(stub.watched, [("pipeline", "job-1")])
        self.assertEqual(stub.worker_jobs, [], "the pipeline's own job was queued twice")
        self.assertEqual(stub.keyed_jobs, [])
        self.assertIsNone(stub.converter.removed_from)

    def test_a_silent_video_is_answered_before_any_work_runs(self):
        source = os.path.join(self.tmp.name, "clip.mp4")
        with open(source, "wb") as fh:
            fh.write(b"video")
        stub = _Stub(source, has_audio=False).bind_remove_audio()

        async def _no_audio(_path, _current_file=None):
            return False

        with patch.object(handlers_module, "_source_has_audio", _no_audio):
            self._run(stub, {"current_file": _video_file()})

        self.assertTrue(any("no audio track" in text for text, _ in stub.replies), stub.replies)
        self.assertEqual(stub.worker_jobs, [], "the removal was queued for a silent video")
        self.assertIsNone(stub.converter.removed_from)

    def test_a_local_source_is_queued_for_a_worker_with_a_watcher(self):
        source = os.path.join(self.tmp.name, "clip.mp4")
        with open(source, "wb") as fh:
            fh.write(b"video")
        stub = _Stub(source).bind_remove_audio()
        self._run(stub, {"current_file": _video_file()})

        self.assertEqual(len(stub.worker_jobs), 1, "expected one worker job")
        job = stub.worker_jobs[0]
        self.assertEqual(job["job_type"], "remove_audio")
        self.assertEqual(job["ffmpeg_args"], ["-an", "-c:v", "copy"])
        self.assertEqual(job["output_ext"], ".mp4")
        self.assertEqual(job["delivery_name"], "clip.mp4")
        self.assertIsNone(
            stub.converter.removed_from,
            "the inline removal must not run as well as the queued one",
        )

    def test_without_a_queue_the_removal_runs_inline_and_uses_the_shared_delivery(self):
        source = os.path.join(self.tmp.name, "clip.mp4")
        with open(source, "wb") as fh:
            fh.write(b"video")
        stub = _Stub(source, queued=False).bind_remove_audio()
        self._run(stub, {"current_file": _video_file()})

        self.assertIsNotNone(stub.converter.removed_from)
        self.assertEqual(len(stub.delivered), 1, "the result skipped the shared delivery")
        output_path, kwargs = stub.delivered[0]
        self.assertEqual(kwargs["kind"], "video")
        self.assertEqual(kwargs["delivery_name"], "clip.mp4")
        # The produced file is this process's temporary result, cleaned up after
        # delivery; the session's own source copy is left alone.
        self.assertFalse(os.path.exists(output_path))
        self.assertTrue(os.path.exists(source))
