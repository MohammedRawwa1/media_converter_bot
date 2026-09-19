"""Regression tests for the /usersettings custom-input and trim flows.

Three bugs sat in the same area:

* the audio Bitrate picker's "Custom" prompt armed ``awaiting_bitrate`` and then
  routed the typed value into ``adjust_bitrate``, which began with a
  ``_require_callback`` guard - so a plain-text reply hit an early return and the
  user got *no* reply at all;
* the same guard swallowed a custom CRF typed into the Compress prompt;
* trimming an audio file was routed through the video-only trimmer (hard-coded
  ``.mp4`` output, ``send_video`` delivery, "Failed to trim video").

These tests drive the real methods with a stub handler/update so the behaviour -
not just the source text - is pinned down.
"""

import asyncio
import os
import tempfile
import unittest
from types import MethodType, SimpleNamespace
from unittest.mock import patch

from source_helpers import called_methods, find_function, parse_source

import handlers as handlers_module
from handlers import EnhancedMediaHandler, _format_seconds_to_hhmmss
from utils.keyboard_utils import MediaMenuBuilder

Handler = EnhancedMediaHandler


class _FakeBot:
    def __init__(self, sent):
        self.sent = sent

    async def send_audio(self, **kwargs):
        self.sent["audio"] = kwargs
        return SimpleNamespace(message_id=1)

    async def send_message(self, **kwargs):
        self.sent["message"] = kwargs
        return SimpleNamespace(message_id=1)


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
    """A plain text-message update: no callback_query, as the prompts produce."""

    def __init__(self, replies, chat_id=99):
        self.callback_query = None
        self.message = _FakeMessage(replies)
        self.effective_chat = SimpleNamespace(id=chat_id)
        self.effective_user = SimpleNamespace(id=7)


class _FakeCallbackUpdate(_FakeUpdate):
    """A button press: ``callback_query`` present, as the menu handlers expect."""

    def __init__(self, replies, chat_id=99):
        super().__init__(replies, chat_id)
        self.callback_query = SimpleNamespace(message=self.message, data="fade_both")


class _FakeContext:
    def __init__(self, sent):
        self.bot = _FakeBot(sent)


class _StubHandler:
    """Just enough of the handler for the methods under test."""

    def __init__(self, converter, sent, replies=None):
        self.converter = converter
        self.sent = sent
        self.replies = replies if replies is not None else []
        self.quota = True

    async def _check_conversion_quota(self, update, context):
        return self.quota

    async def _ensure_current_file_downloaded(self, update, context, session):
        return None

    async def _send_video_result(self, bot, chat_id, file_path, caption="", **delivery_options):
        # Mirrors the real helper's signature, which also takes the delivered
        # name and the user's media/document preference.
        self.sent["video"] = file_path
        self.sent["video_options"] = delivery_options
        return "file-id"

    async def safe_edit(self, query, text, **kwargs):
        self.replies.append((text, kwargs))
        return None

    async def _watch_job_progress(self, *args, **kwargs):
        return None


class _TrimConverter:
    supported_formats = {"audio": [".mp3", ".wav", ".flac"], "video": [".mp4", ".mkv"]}

    def __init__(self):
        self.trim_call = None

    async def trim_video(self, input_path, output_path, start_time, end_time):
        self.trim_call = (input_path, output_path, start_time, end_time)
        with open(output_path, "wb") as fh:
            fh.write(b"trimmed")
        return True


class _BitrateConverter:
    supported_formats = {"audio": [".mp3"], "video": [".mp4"]}

    def __init__(self):
        self.ffmpeg_call = None

    async def execute_ffmpeg(self, cmd, input_path, output_path):
        self.ffmpeg_call = (cmd, input_path, output_path)
        with open(output_path, "wb") as fh:
            fh.write(b"re-encoded")
        return True, ""


def _stub(converter, sent, *, trim: bool = False, fade: bool = False, replies=None):
    handler = _StubHandler(converter, sent, replies)
    if trim:
        names = ("_trim_current_media", "_enqueue_keyed_job", "_enqueue_worker_job")
    elif fade:
        names = ("_apply_fade", "_enqueue_keyed_job", "_enqueue_worker_job")
    else:
        names = ("adjust_bitrate", "compress_video", "_enqueue_keyed_job", "_enqueue_worker_job")
    for name in names:
        # Bind the real implementation to the stub: a plain function assigned to
        # an instance does not become a bound method.
        setattr(handler, name, MethodType(getattr(Handler, name), handler))
    return handler


class TrimHelperTests(unittest.TestCase):
    """``_trim_current_media`` must follow the source type, not assume video."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output_patch = patch.object(handlers_module, "config", SimpleNamespace(OUTPUT_PATH=self.tmp.name))
        self.output_patch.start()

    def tearDown(self):
        self.output_patch.stop()
        self.tmp.cleanup()

    def _file(self, name, media_type):
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as fh:
            fh.write(b"source")
        return {
            "id": "abc123",
            "name": name,
            "path": path,
            "type": media_type,
            "_source_metadata": {},
        }

    def _run(self, current_file, start, end):
        converter = _TrimConverter()
        replies, sent = [], {}
        handler = _stub(converter, sent, trim=True)
        update = _FakeUpdate(replies)
        context = _FakeContext(sent)
        session = {"current_file": current_file}
        asyncio.run(handler._trim_current_media(update, context, session, current_file, start, end))
        return converter, replies, sent

    def test_audio_is_delivered_as_audio_with_the_source_extension(self):
        converter, replies, sent = self._run(self._file("song.wav", "audio"), "00:00:00", "00:57:00")

        self.assertIsNotNone(converter.trim_call, "the trim was never run")
        _inp, output_path, start, end = converter.trim_call
        self.assertTrue(output_path.endswith(".wav"), output_path)
        self.assertEqual((start, end), ("00:00:00", "00:57:00"))
        self.assertIn("audio", sent, "audio result must go out via send_audio")
        self.assertNotIn("video", sent)
        self.assertEqual(sent["audio"]["filename"], "song.wav")
        self.assertTrue(any("✅ Trim complete" in text for text, _ in replies), replies)

    def test_video_still_goes_through_the_video_delivery(self):
        converter, _replies, sent = self._run(self._file("clip.mp4", "video"), "00:00:10", "00:00:20")

        _inp, output_path, _start, _end = converter.trim_call
        self.assertTrue(output_path.endswith(".mp4"), output_path)
        self.assertEqual(sent.get("video", ""), output_path)
        self.assertNotIn("audio", sent)

    def test_mm_ss_times_are_normalized_for_ffmpeg(self):
        converter, _replies, _sent = self._run(self._file("song.mp3", "audio"), "01:00", "02:30")

        _inp, _out, start, end = converter.trim_call
        self.assertEqual(start, "00:01:00")
        self.assertEqual(end, "00:02:30")
        self.assertEqual(start, _format_seconds_to_hhmmss(60.0))

    def test_end_before_start_is_reported_and_nothing_is_trimmed(self):
        converter, replies, sent = self._run(self._file("song.mp3", "audio"), "00:10:00", "00:05:00")

        self.assertIsNone(converter.trim_call)
        self.assertEqual(sent, {})
        self.assertTrue(any("after the start" in text for text, _ in replies), replies)

    def test_missing_file_is_reported_instead_of_silently_failing(self):
        converter, replies, _sent = self._run(None, "00:00:00", "00:00:10")

        self.assertIsNone(converter.trim_call)
        self.assertTrue(any("No file available to trim" in text for text, _ in replies), replies)

    def test_invalid_time_is_reported(self):
        converter, replies, _sent = self._run(self._file("song.mp3", "audio"), "not-a-time", "00:00:10")

        self.assertIsNone(converter.trim_call)
        self.assertTrue(any("Invalid time format" in text for text, _ in replies), replies)


class RepeatTrimJobTests(unittest.TestCase):
    """A cache repeat must go through the stored object, not a fresh Pyrogram fetch."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output_patch = patch.object(handlers_module, "config", SimpleNamespace(OUTPUT_PATH=self.tmp.name))
        self.output_patch.start()
        self.jobs = []

        async def _record(job):
            self.jobs.append(job)

        self.enqueue_patch = patch.object(handlers_module, "enqueue_job", _record)
        self.enqueue_patch.start()

    def tearDown(self):
        self.enqueue_patch.stop()
        self.output_patch.stop()
        self.tmp.cleanup()

    def _run(self, media_type, name, input_key):
        class NoLocalConverter(_TrimConverter):
            async def trim_video(self, input_path, output_path, start_time, end_time):
                raise AssertionError("a repeat must not be trimmed locally")

        converter = NoLocalConverter()
        replies, sent = [], {}
        handler = _stub(converter, sent, trim=True)
        update = _FakeUpdate(replies)
        context = _FakeContext(sent)
        current_file = {
            "id": "abc123",
            "name": name,
            "path": None,
            "input_key": input_key,
            "type": media_type,
            "file_unique_id": "uid-1",
            "size": 1234,
            "_source_metadata": {},
        }
        session = {"current_file": current_file}
        asyncio.run(handler._trim_current_media(update, context, session, current_file, "00:00:00", "00:57:00"))
        return replies, sent

    def test_repeat_queues_a_keyed_trim_job_for_the_stored_object(self):
        replies, _sent = self._run("audio", "song.mp3", "inputs/lib/song")

        self.assertEqual(len(self.jobs), 1, "expected exactly one worker job")
        job = self.jobs[0]
        self.assertEqual(job["type"], "trim")
        self.assertEqual(job["input_key"], "inputs/lib/song")
        self.assertEqual(job["start_time"], "00:00:00")
        self.assertEqual(job["end_time"], "00:57:00")
        self.assertEqual(job["output_ext"], ".mp3")
        self.assertEqual(job["output_filename"], "song.mp3")
        self.assertEqual(job["chat_id"], 99)
        self.assertEqual(job["user_id"], 7)
        # Queuing the job is what tells the user the trim was accepted.
        self.assertTrue(any("Queued trim" in text for text, _ in replies), replies)

    def test_repeat_keeps_the_video_container(self):
        self._run("video", "clip.mkv", "inputs/lib/clip")

        job = self.jobs[0]
        self.assertEqual(job["output_ext"], ".mkv")
        self.assertEqual(job["output_filename"], "clip_trimmed.mkv")


class _MemorySettings:
    """An in-memory stand-in for the settings store.

    A bitrate chosen anywhere is kept as the user's setting (see
    ``_remember_audio_bitrate``), so a test that runs the real ``adjust_bitrate``
    would otherwise write ``storage/user_settings.json`` - a file the repository
    does not track, created by the suite and left behind by it.
    """

    def __init__(self):
        self.values: dict = {}

    def set_user_setting(self, user_id, key, value):
        self.values.setdefault(str(user_id), {})[key] = value
        return True

    def get_user_setting(self, user_id, key, default=None):
        return (self.values.get(str(user_id)) or {}).get(key, default)

    def get_user_settings(self, user_id):
        return dict(self.values.get(str(user_id)) or {})


class _SettingsStubCase(unittest.TestCase):
    """Base for the tests that drive a real conversion method.

    ``adjust_bitrate`` keeps the bitrate it is given as the user's *setting*, so a
    test that runs it against the real store writes ``storage/user_settings.json`` -
    a file the repository does not track, created by the suite and left behind by
    it. Every such case stands the store in for memory instead.
    """

    def setUp(self):
        super().setUp()
        self.settings = _MemorySettings()
        self.settings_patch = patch.object(handlers_module, "user_settings", self.settings)
        self.settings_patch.start()

    def tearDown(self):
        self.settings_patch.stop()
        super().tearDown()


class CustomBitrateTests(_SettingsStubCase):
    """The typed bitrate must actually be applied and acknowledged."""

    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.output_patch = patch.object(handlers_module, "config", SimpleNamespace(OUTPUT_PATH=self.tmp.name))
        self.output_patch.start()

    def tearDown(self):
        self.output_patch.stop()
        self.tmp.cleanup()
        super().tearDown()

    def _audio_file(self):
        path = os.path.join(self.tmp.name, "track.mp3")
        with open(path, "wb") as fh:
            fh.write(b"source")
        return {
            "id": "abc123",
            "name": "track.mp3",
            "path": path,
            "type": "audio",
            "_source_metadata": {},
        }

    def _run(self, bitrate, current_file=None):
        converter = _BitrateConverter()
        replies, sent = [], {}
        handler = _stub(converter, sent)
        update = _FakeUpdate(replies)
        context = _FakeContext(sent)
        user_data: dict = {}
        context.user_data = user_data
        session = {"current_file": current_file if current_file is not None else self._audio_file()}
        asyncio.run(handler.adjust_bitrate(update, context, session, bitrate))
        return converter, replies, sent, user_data

    def test_a_text_message_update_encodes_at_the_typed_bitrate(self):
        converter, replies, sent, _user_data = self._run("64k")

        self.assertIsNotNone(converter.ffmpeg_call, "the bitrate never reached ffmpeg")
        cmd, _input, output_path = converter.ffmpeg_call
        self.assertIn("64k", cmd)
        self.assertTrue(output_path.endswith("_64k.mp3"), output_path)
        self.assertIn("audio", sent, "the re-encoded file was never delivered")
        self.assertTrue(
            any("✅ Bitrate set to 64k" in text for text, _ in replies),
            f"no success acknowledgement was sent: {replies}",
        )

    def test_custom_button_arms_the_prompt(self):
        _converter, replies, _sent, user_data = self._run("custom")

        self.assertTrue(user_data.get("awaiting_bitrate"))
        self.assertTrue(any("Enter bitrate" in text for text, _ in replies), replies)

    def test_the_chosen_bitrate_is_kept_as_the_users_setting(self):
        """A bitrate picked here is the preference too, not only this file's argument.

        The picker is the same one the settings page drives, and a custom value has
        no preset button to mark - so a value that was only ever put on the file it
        was typed for could not be read back anywhere: the next conversion started
        from whatever the setting still said, and the menu had nothing to show.
        """
        self._run("64k")

        self.assertEqual(self.settings.get_user_setting(7, "audio_bitrate"), "64k")

    def test_an_out_of_range_bitrate_never_becomes_the_setting(self):
        # Sanitising falls back to the default, so what is stored is a value ffmpeg
        # can actually take, never the raw text.
        self._run("9999k")

        self.assertEqual(self.settings.get_user_setting(7, "audio_bitrate"), handlers_module._DEFAULT_AUDIO_BITRATE)

    def test_missing_file_is_answered_on_the_message_path(self):
        _converter, replies, _sent, _user_data = self._run("64k", current_file={})

        self.assertTrue(replies, "the user got no reply at all")
        self.assertTrue(any("No audio file found" in text for text, _ in replies), replies)


class RepeatBitrateJobTests(_SettingsStubCase):
    """Re-encoding a cached media must reuse its stored object, not Pyrogram."""

    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.output_patch = patch.object(handlers_module, "config", SimpleNamespace(OUTPUT_PATH=self.tmp.name))
        self.output_patch.start()
        self.jobs = []

        async def _record(job):
            self.jobs.append(job)

        self.enqueue_patch = patch.object(handlers_module, "enqueue_job", _record)
        self.enqueue_patch.start()

    def tearDown(self):
        self.enqueue_patch.stop()
        self.output_patch.stop()
        self.tmp.cleanup()
        super().tearDown()

    def test_bitrate_repeat_queues_a_keyed_job_without_a_local_encode(self):
        class NoLocalConverter(_BitrateConverter):
            async def execute_ffmpeg(self, cmd, input_path, output_path):
                raise AssertionError("a repeat must not re-encode from a local copy")

        converter = NoLocalConverter()
        replies, sent = [], {}
        handler = _stub(converter, sent)
        update = _FakeUpdate(replies)
        context = _FakeContext(sent)
        session = {
            "current_file": {
                "id": "abc123",
                "name": "song.mp3",
                "path": None,
                "input_key": "inputs/lib/song",
                "type": "audio",
                "file_unique_id": "uid-1",
                "size": 1234,
                "_source_metadata": {},
            }
        }

        asyncio.run(handler.adjust_bitrate(update, context, session, "64k"))

        self.assertEqual(len(self.jobs), 1, "expected one keyed worker job")
        job = self.jobs[0]
        self.assertEqual(job["type"], "format_audio")
        self.assertEqual(job["input_key"], "inputs/lib/song")
        self.assertEqual(job["output_ext"], ".mp3")
        self.assertIn("64k", job["ffmpeg_args"])
        self.assertEqual(job["chat_id"], 99)
        # The delivered name is the media's, not the transient output path's.
        self.assertEqual(job["original_filename"], "song.mp3")


class BitrateFailureTests(_SettingsStubCase):
    """A failed inline encode is moved to a worker, and says why if it cannot be.

    Re-encoding in the web process is bounded by whatever else that process holds
    and by the request that triggered it; the queue is the path every other
    conversion in this bot takes. Failing there used to end the action with a bare
    "Failed to adjust bitrate" and ffmpeg's own verdict discarded.
    """

    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.output_patch = patch.object(handlers_module, "config", SimpleNamespace(OUTPUT_PATH=self.tmp.name))
        self.output_patch.start()
        self.jobs = []

        async def _record(job):
            self.jobs.append(job)

        self.enqueue_patch = patch.object(handlers_module, "enqueue_job", _record)
        self.enqueue_patch.start()

    def tearDown(self):
        self.enqueue_patch.stop()
        self.output_patch.stop()
        self.tmp.cleanup()
        super().tearDown()

    def _source(self):
        path = os.path.join(self.tmp.name, "track.mp3")
        with open(path, "wb") as fh:
            fh.write(b"source")
        return {
            "id": "abc123",
            "name": "track.mp3",
            "path": path,
            "chat_id": 99,
            "msg_id": 4321,
            "type": "audio",
            "_source_metadata": {},
        }

    def test_a_failed_inline_encode_is_queued_for_a_worker(self):
        class FailingConverter(_BitrateConverter):
            async def execute_ffmpeg(self, cmd, input_path, output_path):
                return False, "Conversion failed\nOutput file #0 does not contain any stream"

        replies, sent = [], {}
        handler = _stub(FailingConverter(), sent)
        session = {"current_file": self._source()}

        asyncio.run(handler.adjust_bitrate(_FakeUpdate(replies), _FakeContext(sent), session, "64k"))

        self.assertEqual(len(self.jobs), 1, "the failed encode was not handed to a worker")
        job = self.jobs[0]
        self.assertEqual(job["type"], "format_audio")
        self.assertIn("64k", job["ffmpeg_args"])
        self.assertEqual(job["input_path"], session["current_file"]["path"])
        # A worker on another host can still reach the media: the Telegram
        # chat/message it came from travels with the job.
        self.assertEqual(job["source_chat_id"], 99)
        self.assertEqual(job["source_message_id"], 4321)
        # The session's own copy is not a worker's to delete.
        self.assertFalse(job["cleanup_input"])
        self.assertTrue(any("Queued" in text for text, _ in replies), replies)

    def test_a_source_with_no_audio_stream_is_answered_before_ffmpeg_runs(self):
        converter = _BitrateConverter()
        replies, sent = [], {}
        handler = _stub(converter, sent)
        session = {"current_file": self._source()}

        async def _no_audio(_path, _current_file=None):
            return False

        with patch.object(handlers_module, "_source_has_audio", _no_audio):
            asyncio.run(handler.adjust_bitrate(_FakeUpdate(replies), _FakeContext(sent), session, "64k"))

        self.assertIsNone(converter.ffmpeg_call, "ffmpeg ran on a source with no audio")
        self.assertTrue(any("no audio track" in text for text, _ in replies), replies)

    def test_the_ingest_verdict_answers_whether_there_is_audio(self):
        """The stored probe is used as-is: counting streams means reading the file."""

        async def _run():
            return await handlers_module._source_has_audio(
                "a-path-that-does-not-exist.mp3", {"_source_metadata": {"audio_streams": 1}}
            )

        self.assertTrue(asyncio.run(_run()))

        async def _none():
            return await handlers_module._source_has_audio(
                "a-path-that-does-not-exist.mp3", {"_source_metadata": {"audio_streams": 0}}
            )

        self.assertFalse(asyncio.run(_none()))

    def test_a_failure_that_cannot_be_queued_reports_what_ffmpeg_said(self):
        class FailingConverter(_BitrateConverter):
            async def execute_ffmpeg(self, cmd, input_path, output_path):
                return False, "Conversion failed\nOutput file #0 does not contain any stream"

        replies, sent = [], {}
        handler = _stub(FailingConverter(), sent)
        session = {"current_file": self._source()}

        # No Redis queue in this process: the enqueue raises and the reply is the
        # only thing the user has left.
        async def _boom(_job):
            raise RuntimeError("no broker")

        with patch.object(handlers_module, "enqueue_job", _boom):
            asyncio.run(handler.adjust_bitrate(_FakeUpdate(replies), _FakeContext(sent), session, "64k"))

        self.assertTrue(
            any("does not contain any stream" in text for text, _ in replies),
            f"the cause never reached the user: {replies}",
        )


class RepeatFadeJobTests(unittest.TestCase):
    """A fade on a cached media must reuse its stored object.

    A fade-out is anchored to the end of the media, so its duration has to be
    probed from the resolved source - which only the worker can do on a repeat.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output_patch = patch.object(handlers_module, "config", SimpleNamespace(OUTPUT_PATH=self.tmp.name))
        self.output_patch.start()
        self.jobs = []

        async def _record(job):
            self.jobs.append(job)

        self.enqueue_patch = patch.object(handlers_module, "enqueue_job", _record)
        self.enqueue_patch.start()

    def tearDown(self):
        self.enqueue_patch.stop()
        self.output_patch.stop()
        self.tmp.cleanup()

    def test_fade_repeat_queues_a_keyed_fade_job_without_a_local_encode(self):
        class NoLocalConverter(_BitrateConverter):
            async def apply_fade(self, input_path, output_path, fade_in=0.0, fade_out=0.0):
                raise AssertionError("a repeat must not fade from a local copy")

        converter = NoLocalConverter()
        replies, sent = [], {}
        handler = _stub(converter, sent, fade=True, replies=replies)
        update = _FakeCallbackUpdate(replies)
        context = _FakeContext(sent)
        session = {
            "current_file": {
                "id": "abc123",
                "name": "song.mp3",
                "path": None,
                "input_key": "inputs/lib/song",
                "type": "audio",
                "file_unique_id": "uid-1",
                "size": 1234,
                "_source_metadata": {},
            }
        }

        asyncio.run(handler._apply_fade(update, context, session, fade_in=3.0, fade_out=3.0))

        self.assertEqual(len(self.jobs), 1, "expected one keyed worker job")
        job = self.jobs[0]
        self.assertEqual(job["type"], "fade")
        self.assertEqual(job["input_key"], "inputs/lib/song")
        self.assertEqual(job["fade_in"], 3.0)
        self.assertEqual(job["fade_out"], 3.0)
        self.assertEqual(job["output_ext"], ".mp3")
        self.assertEqual(job["output_filename"], "song.mp3")
        self.assertTrue(any("Queued fade" in text for text, _ in replies), replies)

    def test_fade_repeat_for_video_keeps_the_container(self):
        class NoLocalConverter(_TrimConverter):
            async def apply_fade(self, input_path, output_path, fade_in=0.0, fade_out=0.0):
                raise AssertionError("a repeat must not fade from a local copy")

        replies, sent = [], {}
        handler = _stub(NoLocalConverter(), sent, fade=True, replies=replies)
        update = _FakeCallbackUpdate(replies)
        context = _FakeContext(sent)
        session = {
            "current_file": {
                "id": "v1",
                "name": "clip.mkv",
                "path": None,
                "input_key": "inputs/lib/clip",
                "type": "video",
                "_source_metadata": {},
            }
        }

        asyncio.run(handler._apply_fade(update, context, session, fade_out=3.0))

        job = self.jobs[0]
        self.assertEqual(job["output_ext"], ".mkv")
        self.assertEqual(job["output_filename"], "clip_faded.mkv")


class FadeTaskTests(unittest.TestCase):
    """The worker-side fade helper rejects a no-op before touching ffmpeg."""

    def test_no_duration_is_rejected(self):
        from tasks import apply_fade

        ok, reason = asyncio.run(apply_fade("in.mp3", "out.mp3"))
        self.assertFalse(ok)
        self.assertIn("no fade duration", reason)

    def test_worker_has_a_fade_job_type(self):
        from source_helpers import read_source

        src = read_source("workers", "ffmpeg_worker.py")
        self.assertIn('elif job_type == "fade":', src)
        self.assertIn('job.get("fade_in")', src)
        self.assertIn('job.get("fade_out")', src)


class CustomCrfTests(unittest.TestCase):
    """The custom Compress CRF used to hit the same silent callback guard."""

    def test_custom_crf_arms_the_prompt_from_a_message_update(self):
        src = parse_source("handlers.py")
        compress = find_function(src, "compress_video")
        called = called_methods(compress, "self")

        self.assertNotIn(
            "_require_callback",
            called,
            "compress_video must accept a plain message, not only callbacks",
        )

    def test_custom_crf_prompt_is_reachable(self):
        src = parse_source("handlers.py")
        adjust = called_methods(find_function(src, "adjust_bitrate"), "self")

        self.assertNotIn(
            "_require_callback",
            adjust,
            "adjust_bitrate must accept a plain message, not only callbacks",
        )


class AudioMenuButtonTests(unittest.TestCase):
    """The main menu must not send an audio file to video-only tools."""

    @staticmethod
    def _callbacks(markup):
        return [button.callback_data for row in markup.inline_keyboard for button in row]

    def test_audio_menu_uses_the_audio_trimmer_and_merger(self):
        callbacks = self._callbacks(MediaMenuBuilder.get_main_menu("audio"))
        self.assertIn("trim_audio", callbacks)
        self.assertIn("merge_audio", callbacks)
        self.assertNotIn("trim_video", callbacks)

    def test_video_menu_keeps_the_video_trimmer(self):
        callbacks = self._callbacks(MediaMenuBuilder.get_main_menu("video"))
        self.assertIn("trim_video", callbacks)
        self.assertNotIn("trim_audio", callbacks)

    def test_audio_tools_menu_trim_opens_the_audio_flow(self):
        callbacks = self._callbacks(MediaMenuBuilder.get_audio_tools_menu())
        self.assertIn("trim_audio", callbacks)


if __name__ == "__main__":
    unittest.main()
