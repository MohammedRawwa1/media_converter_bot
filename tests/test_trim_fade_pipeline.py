"""Trim and fade travel the pipeline like every other conversion.

Both used to clear a stale declaration and then fetch the source by hand, so a
large video was pulled onto the disk (or refused) instead of being fetched and
cut/faded in one worker job. Their work is a *field pair* rather than a set of
ffmpeg arguments, which is why the pipeline gate accepts a conversion type on
its own and why the fields ride along with the ingest job.

These tests drive the real handlers with stub collaborators, so the order -
declare, fetch, watch the pipeline job, and *not* queue a second one - is what
is checked.
"""

import asyncio
import tempfile
import unittest
from types import MethodType, SimpleNamespace
from unittest.mock import patch

import handlers as handlers_module
from handlers import EnhancedMediaHandler

Handler = EnhancedMediaHandler


class _FakeMessage:
    def __init__(self, replies):
        self.replies = replies

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))
        return SimpleNamespace(message_id=1)

    async def edit_text(self, text, **kwargs):
        self.replies.append((text, kwargs))
        return SimpleNamespace(message_id=1)


class _FakeUpdate:
    def __init__(self, replies, *, callback=True):
        self.message = _FakeMessage(replies)
        self.callback_query = SimpleNamespace(message=self.message, data="fade_both") if callback else None
        self.effective_chat = SimpleNamespace(id=99)
        self.effective_user = SimpleNamespace(id=7)


class _FakeContext:
    def __init__(self):
        self.bot = SimpleNamespace(send_message=None)


class _Converter:
    supported_formats = {"audio": [".mp3", ".wav"], "video": [".mp4", ".mkv"]}

    def __init__(self):
        self.trimmed = None
        self.faded = None

    async def trim_video(self, input_path, output_path, start, end):
        self.trimmed = (input_path, output_path, start, end)
        return True

    async def apply_fade(self, input_path, output_path, fade_in, fade_out):
        self.faded = (input_path, output_path, fade_in, fade_out)
        return True


class _Stub:
    def __init__(self, *, pipeline_job=None):
        self.converter = _Converter()
        self.replies = []
        self.declared = None
        self.watched = []
        self.cancelled = []
        self.keyed_jobs = []
        self.pipeline_job = pipeline_job

    async def _check_conversion_quota(self, update, context):
        return True

    async def _cancel_stale_pipeline_job(self, session, handler_name, user_id=None):
        self.cancelled.append(handler_name)
        return True

    async def _ensure_local_media(self, update, context, session, current_file, **kwargs):
        # Captured at the moment of the fetch: what the pipeline was told to
        # produce has to be on the file already.
        self.declared = dict(current_file)
        if self.pipeline_job:
            current_file["_pipeline_job_id"] = self.pipeline_job
        return current_file, None

    def _local_copy(self, current_file):
        return None

    async def _enqueue_keyed_job(self, *args, **kwargs):
        self.keyed_jobs.append(kwargs)
        return True

    async def _watch_pipeline_job(self, update, context, job_id, summary, **kwargs):
        self.watched.append((job_id, summary))

    async def safe_edit(self, query, text, **kwargs):
        self.replies.append((text, kwargs))

    def bind(self, *names):
        for name in names:
            setattr(self, name, MethodType(getattr(Handler, name), self))
        return self


def _video(name="clip.mp4"):
    return {"id": "abc123", "name": name, "type": "video", "_source_metadata": {}}


class TrimFadePipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output_patch = patch.object(handlers_module, "config", SimpleNamespace(OUTPUT_PATH=self.tmp.name))
        self.output_patch.start()

    def tearDown(self):
        self.output_patch.stop()
        self.tmp.cleanup()

    def test_trim_declares_its_cut_before_the_fetch(self):
        stub = _Stub(pipeline_job="job-trim").bind("_trim_current_media")
        updates = _FakeUpdate(stub.replies, callback=False)
        session = {"current_file": _video()}

        asyncio.run(stub._trim_current_media(updates, _FakeContext(), session, session["current_file"], "10", "20"))

        declared = stub.declared
        self.assertEqual(declared["_pipeline_conversion_type"], "trim")
        self.assertEqual(declared["_pipeline_output_ext"], ".mp4")
        self.assertIsNone(declared["_pipeline_ffmpeg_args"])
        # The cut itself is what has to reach the ingest job.
        self.assertEqual(declared["_pipeline_extra"]["start_time"], "00:00:10")
        self.assertEqual(declared["_pipeline_extra"]["end_time"], "00:00:20")
        self.assertEqual(declared["_pipeline_extra"]["output_filename"], "clip_trimmed.mp4")
        # The pipeline job is watched; no second keyed job, no inline trim.
        self.assertEqual([job for job, _ in stub.watched], ["job-trim"])
        self.assertEqual(stub.keyed_jobs, [])
        self.assertIsNone(stub.converter.trimmed)

    def test_fade_declares_its_curve_before_the_fetch(self):
        stub = _Stub(pipeline_job="job-fade").bind("_apply_fade")
        session = {"current_file": _video()}

        asyncio.run(stub._apply_fade(_FakeUpdate(stub.replies), _FakeContext(), session, fade_in=2.0, fade_out=3.0))

        declared = stub.declared
        self.assertEqual(declared["_pipeline_conversion_type"], "fade")
        self.assertIsNone(declared["_pipeline_ffmpeg_args"])
        self.assertEqual(declared["_pipeline_extra"]["fade_in"], 2.0)
        self.assertEqual(declared["_pipeline_extra"]["fade_out"], 3.0)
        self.assertEqual([job for job, _ in stub.watched], ["job-fade"])
        self.assertEqual(stub.keyed_jobs, [])
        self.assertIsNone(stub.converter.faded)


class PipelineDeclarationGateTests(unittest.TestCase):
    """The gate accepts an arguments pair *or* a conversion type on its own."""

    def test_ffmpeg_arguments_declare_an_encode(self):
        assert handlers_module._has_pipeline_declaration(
            {"_pipeline_ffmpeg_args": ["-an", "-c:v", "copy"], "_pipeline_conversion_type": "remove_audio"}
        )

    def test_a_conversion_type_alone_declares_a_field_driven_job(self):
        assert handlers_module._has_pipeline_declaration({"_pipeline_conversion_type": "extract_streams"})
        assert handlers_module._has_pipeline_declaration({"_pipeline_conversion_type": "trim"})
        assert handlers_module._has_pipeline_declaration({"_pipeline_conversion_type": "fade"})

    def test_no_declaration_means_no_pipeline(self):
        assert not handlers_module._has_pipeline_declaration({})
        assert not handlers_module._has_pipeline_declaration(
            {"_pipeline_ffmpeg_args": None, "_pipeline_conversion_type": None}
        )
        # Empty args are not a declaration either.
        assert not handlers_module._has_pipeline_declaration({"_pipeline_ffmpeg_args": []})
