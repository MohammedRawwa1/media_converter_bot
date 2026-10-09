"""The subtitle button: apply the file the user just sent, on any pipe.

A media Telegram hands over is registered *lazily* - nothing is downloaded until an
action needs the bytes. The subtitle flow resolved the video directly, so a video
the user had just sent had neither a local copy nor a stored object, and the button
answered "File not available on disk." for the exact file the user had just
uploaded. These tests drive the real handler: the shared fetch runs first, a stored
copy is read without a second download, and the converter muxes into the container
the output is actually written as.
"""

import asyncio
import os
import tempfile
import unittest
from types import MethodType, SimpleNamespace
from unittest.mock import patch

from source_helpers import read_source

import handlers as handlers_module
import media_converter as media_converter_module
from handlers import EnhancedMediaHandler
from media_converter import ExtendedMediaConverter
from utils.keyboard_utils import MediaMenuBuilder

Handler = EnhancedMediaHandler

_SRT = "1\n00:00:00,000 --> 00:00:03,000\nNobody is coming to save you.\n"
_SRT_BYTES = _SRT.encode("utf-8")


class _FakeSubtitleDownload:
    def __init__(self, file_id, name, payload):
        self.file_id = file_id
        self.file_name = name
        self.payload = payload
        self.downloads = 0

    async def download_to_drive(self, dest):
        self.downloads += 1
        with open(dest, "wb") as fh:
            fh.write(self.payload)


class _FakeBot:
    def __init__(self, payload=_SRT_BYTES):
        self.payload = payload
        self.get_file_calls = []

    async def get_file(self, file_id):
        self.get_file_calls.append(file_id)
        return _FakeSubtitleDownload(file_id, "30sec.srt", self.payload)

    async def send_message(self, **kwargs):
        return SimpleNamespace(message_id=1)


class _FakeMessage:
    def __init__(self, replies):
        self.replies = replies

    async def reply_text(self, text, **kwargs):
        self.replies.append(text)
        return SimpleNamespace(message_id=1)


class _FakeUpdate:
    def __init__(self, replies, chat_id=99):
        self.callback_query = None
        self.message = _FakeMessage(replies)
        self.effective_chat = SimpleNamespace(id=chat_id)
        self.effective_user = SimpleNamespace(id=7)


class _FakeContext:
    def __init__(self, payload=_SRT_BYTES):
        self.bot = _FakeBot(payload)
        self.user_data: dict = {}


class _RecordingConverter:
    """The converter, minus ffmpeg: it records the call and writes the output."""

    def __init__(self, ok=True):
        self.ok = ok
        self.calls: list[tuple] = []

    async def add_subtitles(self, video_path, subtitle_path, output_path):
        self.calls.append(("add", video_path, subtitle_path, output_path))
        if self.ok:
            with open(output_path, "wb") as fh:
                fh.write(b"muxed")
        return self.ok

    async def burn_subtitles(self, video_path, subtitle_path, output_path):
        self.calls.append(("burn", video_path, subtitle_path, output_path))
        if self.ok:
            with open(output_path, "wb") as fh:
                fh.write(b"burned")
        return self.ok


class _SubtitleHandler:
    """The real subtitle path, with only the fetch and the delivery swapped out."""

    def __init__(self, converter, fetched_path):
        self.converter = converter
        self.fetched_path = fetched_path
        self.fetches = 0
        self.delivered: list[str | None] = []
        self.userbot_sends = 0

    async def _check_conversion_quota(self, update, context):
        return True

    async def _ensure_current_file_downloaded(self, update, context, session):
        self.fetches += 1
        session["current_file"]["path"] = self.fetched_path

    async def _send_video_result(self, bot, chat_id, file_path, caption="", **delivery_options):
        self.delivered.append(file_path)
        return "file-id"

    async def _send_part_via_userbot(self, *args, **kwargs):
        self.userbot_sends += 1
        return True


class SubtitleMergeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.config_patch = patch.object(
            handlers_module,
            "config",
            SimpleNamespace(
                INPUT_PATH=os.path.join(self.tmp.name, "input"),
                OUTPUT_PATH=os.path.join(self.tmp.name, "output"),
                BOT_API_MAX_BYTES=50 * 1024 * 1024,
                ENABLE_USERBOT=True,
                get_storage_backend_name=lambda: "none",
            ),
        )
        self.config_patch.start()

    def tearDown(self):
        self.config_patch.stop()
        self.tmp.cleanup()

    def _video_path(self, name="clip.mkv"):
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as fh:
            fh.write(b"video-bytes")
        return path

    def _handler(self, converter, fetched_path):
        handler = _SubtitleHandler(converter, fetched_path)
        for name in (
            "_apply_subtitle_file",
            "_burn_subtitle_into_current",
            "_ensure_local_media",
            "_local_copy",
            "_resolve_local_source",
            "_adopt_stored_source",
            "_download_stored_source",
        ):
            setattr(handler, name, MethodType(getattr(Handler, name), handler))
        return handler

    def _run(self, handler, session, *, file_ext=".srt", burn=False, document=None, payload=None):
        replies: list[str] = []
        update = _FakeUpdate(replies)
        context = _FakeContext(payload) if payload is not None else _FakeContext()
        document = document or SimpleNamespace(file_id="srt-file-id", file_name=f"30sec{file_ext}")
        asyncio.run(handler._apply_subtitle_file(update, context, session, document, file_ext, burn=burn))
        return replies

    def _session(self, **overrides):
        current_file = {"id": "vid1", "name": "clip.mkv", "path": None, "type": "video"}
        current_file.update(overrides)
        return {"current_file": current_file}

    # ── the bug ─────────────────────────────────────────────────────────────

    def test_a_freshly_sent_video_is_fetched_then_subtitled(self):
        """The reported failure: no local copy, nothing stored, still not refused."""
        converter = _RecordingConverter()
        fetched = self._video_path()
        handler = self._handler(converter, fetched)

        replies = self._run(handler, self._session())

        self.assertEqual(handler.fetches, 1, "the freshly sent video was never fetched")
        self.assertEqual(len(converter.calls), 1, f"the subtitle was never applied: {replies}")
        self.assertNotIn("❌ File not available on disk.", replies, f"the old refusal came back: {replies}")
        self.assertTrue(handler.delivered, "the subtitled video was not delivered")

    def test_a_local_copy_is_used_without_fetching_anything(self):
        converter = _RecordingConverter()
        local = self._video_path()
        handler = self._handler(converter, local)

        self._run(handler, self._session(path=local))

        self.assertEqual(handler.fetches, 0, "a media already on disk must not be fetched again")
        self.assertEqual(len(converter.calls), 1)

    def test_a_killed_burn_reports_why_not_a_uniform_failure(self):
        """A process killed by the OOM killer must say so, not "the merge failed"."""
        converter = _RecordingConverter(ok=False)
        converter.last_ffmpeg_failure = "ffmpeg was killed by signal 9 (SIGKILL)"
        handler = self._handler(converter, self._video_path())

        replies = self._run(handler, self._session(), burn=True)

        self.assertTrue(any("SIGKILL" in text for text in replies), replies)

    def test_a_failed_fetch_reports_the_reason_and_does_nothing(self):
        converter = _RecordingConverter()
        handler = self._handler(converter, None)
        session = self._session()

        async def _fail(update, context, session):
            raise Exception("File too large (1200MB). Max allowed: 1000MB")

        handler._ensure_current_file_downloaded = _fail

        replies = self._run(handler, session)

        self.assertEqual(converter.calls, [], "a failed fetch still attempted the merge")
        self.assertTrue(any("Failed to download file" in text for text in replies), replies)

    def test_a_session_without_a_video_is_refused_before_downloading(self):
        converter = _RecordingConverter()
        handler = self._handler(converter, None)
        replies = self._run(handler, {"current_file": {"id": "a1", "name": "song.mp3", "type": "audio"}})

        self.assertTrue(any("No video available" in text for text in replies), replies)
        self.assertEqual(converter.calls, [])

    def test_a_subtitle_that_is_not_one_is_refused_before_ffmpeg(self):
        """A renamed video arrives as .srt; it must not reach the merge."""
        converter = _RecordingConverter()
        handler = self._handler(converter, self._video_path())

        replies = self._run(handler, self._session(), payload=b"\x00\x01not-a-subtitle")

        self.assertEqual(converter.calls, [], "ffmpeg was handed a file that is not a subtitle")
        self.assertTrue(any("not a usable subtitle" in text for text in replies), replies)

    # ── container correctness ───────────────────────────────────────────────

    def test_muxing_an_mkv_source_delivers_an_mp4(self):
        """``mov_text`` only lives in MP4, so a non-MP4 source is delivered as MP4."""
        converter = _RecordingConverter()
        handler = self._handler(converter, self._video_path("clip.mkv"))

        self._run(handler, self._session(name="clip.mkv"))

        _, _, _, output_path = converter.calls[0]
        self.assertTrue(output_path.endswith(".mp4"), output_path)

    def test_burning_always_delivers_an_mp4(self):
        """The burn re-encodes to H.264/AAC, so the container is MP4 whatever the source was."""
        converter = _RecordingConverter()
        handler = self._handler(converter, self._video_path("clip.mkv"))

        self._run(handler, self._session(name="clip.mkv"), burn=True)

        kind, _, _, output_path = converter.calls[0]
        self.assertEqual(kind, "burn")
        self.assertTrue(output_path.endswith(".mp4"), output_path)

    def test_the_add_button_also_hardcodes_the_subtitles(self):
        """➕ Add Subtitles merges into the frames, exactly like 🔥 Burn Subtitles."""
        converter = _RecordingConverter()
        handler = self._handler(converter, self._video_path())

        self._run(handler, self._session(), burn=False)

        kind = converter.calls[0][0]
        self.assertEqual(kind, "burn", "the Add button did not hardcode the subtitles")

    def test_an_over_limit_result_is_sent_through_the_userbot(self):
        converter = _RecordingConverter()
        handler = self._handler(converter, self._video_path())
        # Make the produced output larger than the (small) Bot API ceiling.
        patched = SimpleNamespace(
            INPUT_PATH=os.path.join(self.tmp.name, "input"),
            OUTPUT_PATH=os.path.join(self.tmp.name, "output"),
            BOT_API_MAX_BYTES=1,
            ENABLE_USERBOT=True,
            get_storage_backend_name=lambda: "none",
        )
        with patch.object(handlers_module, "config", patched):
            self._run(handler, self._session())

        self.assertEqual(handler.userbot_sends, 1, "an over-limit subtitle result skipped the userbot")
        self.assertEqual(handler.delivered, [], "the Bot API was still asked for an over-limit file")


class SubtitleValidationTests(unittest.TestCase):
    """An extension is a claim: the bytes are checked before ffmpeg sees them."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _write(self, name, payload):
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as fh:
            fh.write(payload)
        return path

    def test_a_real_srt_passes(self):
        path = self._write("ok.srt", _SRT_BYTES)
        self.assertEqual(handlers_module._validate_subtitle_file(path, ".srt"), (True, ""))

    def test_a_renamed_video_is_refused(self):
        path = self._write("fake.srt", b"\x00\x01not-subtitles")
        ok, reason = handlers_module._validate_subtitle_file(path, ".srt")
        self.assertFalse(ok)
        self.assertIn("valid", reason)

    def test_an_empty_file_is_refused(self):
        path = self._write("empty.srt", b"")
        ok, reason = handlers_module._validate_subtitle_file(path, ".srt")
        self.assertFalse(ok)
        self.assertIn("empty", reason)

    def test_a_webvtt_header_is_required(self):
        good = self._write("ok.vtt", b"WEBVTT\n\n00:00.000 --> 00:02.000\nhi\n")
        bad = self._write("bad.vtt", b"1\n00:00:00,000 --> 00:00:03,000\nhi\n")
        self.assertTrue(handlers_module._validate_subtitle_file(good, ".vtt")[0])
        self.assertFalse(handlers_module._validate_subtitle_file(bad, ".vtt")[0])

    def test_an_ass_script_marker_is_required(self):
        good = self._write("ok.ass", b"[Script Info]\nTitle: x\n\nDialogue: 0,0:00:00.00,hi\n")
        self.assertTrue(handlers_module._validate_subtitle_file(good, ".ass")[0])


class FfmpegKillTests(unittest.TestCase):
    """A killed encode has to be named as one, not reported as a generic failure.

    exit=-9 with an empty stderr is the OOM killer, and it looked exactly like any
    other ffmpeg error to the user ("the merge failed").
    """

    def test_signal_9_is_named(self):
        self.assertEqual(media_converter_module._signal_name(9), "signal 9 (SIGKILL)")

    def test_an_unknown_signal_number_still_names_the_signal(self):
        self.assertEqual(media_converter_module._signal_name(12345), "signal 12345")


class SubtitleConverterTests(unittest.TestCase):
    """The ffmpeg command itself: codec by container, streams mapped explicitly."""

    class _Capturing(ExtendedMediaConverter):
        def __init__(self):
            self.captured: list[str] = []

        async def execute_ffmpeg(self, cmd, input_path=None, output_path=None):
            self.captured = list(cmd)
            return True, ""

    def setUp(self):
        self.converter = self._Capturing()

    def test_mp4_output_muxes_mov_text(self):
        asyncio.run(self.converter.add_subtitles("v.mkv", "s.srt", "out.mp4"))
        self.assertIn("mov_text", self.converter.captured)
        self.assertIn("0:v:0", self.converter.captured)
        self.assertIn("1:0", self.converter.captured)

    def test_mkv_output_muxes_srt_not_mov_text(self):
        asyncio.run(self.converter.add_subtitles("v.mp4", "s.srt", "out.mkv"))
        self.assertIn("srt", self.converter.captured)
        self.assertNotIn("mov_text", self.converter.captured)

    def test_optional_audio_mapping_keeps_a_silent_video_working(self):
        asyncio.run(self.converter.add_subtitles("v.mp4", "s.srt", "out.mp4"))
        self.assertIn("0:a?", self.converter.captured)

    def test_burn_escapes_the_filter_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            subtitle = os.path.join(tmp, "my'subs.srt")
            with open(subtitle, "wb") as fh:
                fh.write(_SRT_BYTES)
            asyncio.run(self.converter.burn_subtitles("in.mp4", subtitle, "out.mp4"))

        cmd = self.converter.captured
        vf = cmd[cmd.index("-vf") + 1]
        self.assertTrue(vf.startswith("subtitles=filename="), vf)
        self.assertIn("\\'", vf)

    def test_burn_bounds_the_filter_and_encoder_threads(self):
        """Peak RAM is what kills a long merge, so both worker pools are capped."""
        with tempfile.TemporaryDirectory() as tmp:
            subtitle = os.path.join(tmp, "subs.srt")
            with open(subtitle, "wb") as fh:
                fh.write(_SRT_BYTES)
            asyncio.run(self.converter.burn_subtitles("in.mp4", subtitle, "out.mp4"))

        cmd = self.converter.captured
        self.assertEqual(cmd[cmd.index("-filter_threads") + 1], "1")
        self.assertEqual(cmd[cmd.index("-threads") + 1], "2")

    def test_burn_lets_env_restore_ffmpeg_defaults(self):
        """A bare "0" hands the thread choice back to ffmpeg."""
        with tempfile.TemporaryDirectory() as tmp:
            subtitle = os.path.join(tmp, "subs.srt")
            with open(subtitle, "wb") as fh:
                fh.write(_SRT_BYTES)
            with patch.dict(os.environ, {"FFMPEG_FILTER_THREADS": "0", "FFMPEG_THREADS": "0"}):
                asyncio.run(self.converter.burn_subtitles("in.mp4", subtitle, "out.mp4"))

        cmd = self.converter.captured
        self.assertNotIn("-filter_threads", cmd)
        self.assertNotIn("-threads", cmd)

    def test_burn_uses_the_memory_constrained_encode_settings(self):
        """veryfast x264 + AAC + faststart, the deployment's small-host preset."""
        with tempfile.TemporaryDirectory() as tmp:
            subtitle = os.path.join(tmp, "subs.srt")
            with open(subtitle, "wb") as fh:
                fh.write(_SRT_BYTES)
            asyncio.run(self.converter.burn_subtitles("in.mp4", subtitle, "out.mp4"))

        cmd = self.converter.captured
        self.assertIn("libx264", cmd)
        self.assertIn("aac", cmd)
        self.assertIn("+faststart", cmd)
        self.assertEqual(cmd[cmd.index("-preset") + 1], "veryfast")
        self.assertEqual(cmd[cmd.index("-crf") + 1], "23")
        self.assertEqual(cmd[cmd.index("-b:a") + 1], "128k")

    def test_mp4_mux_puts_the_index_first(self):
        asyncio.run(self.converter.add_subtitles("v.mp4", "s.srt", "out.mp4"))
        self.assertIn("+faststart", self.converter.captured)


class BatchSubtitleTests(unittest.TestCase):
    """The batch flow: collect every .srt first, then merge the videos 1 by 1."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.config_patch = patch.object(
            handlers_module,
            "config",
            SimpleNamespace(
                INPUT_PATH=os.path.join(self.tmp.name, "input"),
                OUTPUT_PATH=os.path.join(self.tmp.name, "output"),
                BOT_API_MAX_BYTES=50 * 1024 * 1024,
                ENABLE_USERBOT=True,
                get_storage_backend_name=lambda: "none",
            ),
        )
        self.config_patch.start()

    def tearDown(self):
        self.config_patch.stop()
        self.tmp.cleanup()

    def _write_srt(self, name):
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as fh:
            fh.write(_SRT_BYTES)
        return path

    def _video(self, name):
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as fh:
            fh.write(b"video")
        return {"id": name, "name": name, "path": path, "type": "video"}

    def _handler(self, converter):
        handler = _BatchHandler(converter)
        # Instance methods are bound; the class's staticmethods are attached as
        # plain functions so calling ``handler._batch_videos(sess)`` does not pass
        # an unexpected ``self``.
        for name in (
            "_burn_subtitle_into_current",
            "_ensure_local_media",
            "_local_copy",
            "_resolve_local_source",
            "_adopt_stored_source",
            "_download_stored_source",
            "_collect_batch_subtitle",
            "_run_batch_subtitles",
            "_start_batch_subtitles",
        ):
            setattr(handler, name, MethodType(getattr(Handler, name), handler))
        for name in ("_batch_videos", "_match_batch_subtitle", "_batch_subtitle_markup"):
            setattr(handler, name, getattr(Handler, name))
        return handler

    def test_the_bulk_menu_offers_batch_subtitles(self):
        markup = MediaMenuBuilder.get_bulk_menu({})
        data = [button.callback_data for row in markup.inline_keyboard for button in row]
        self.assertIn("bulk_subtitles", data)
        # The Apply Bulk route is a toggle too, so the batch action can be armed
        # from the bulk menu without leaving it.
        self.assertIn("bulk_toggle:bulk_subtitles", data)

    def test_matching_prefers_the_same_filename_then_the_send_order(self):
        subs = [
            {"file_id": "a", "name": "other.srt"},
            {"file_id": "b", "name": "Clip.srt"},
            {"file_id": "c", "name": "third.srt"},
        ]
        chosen = Handler._match_batch_subtitle({"name": "clip.mp4"}, subs, set())
        self.assertEqual(chosen["file_id"], "b", "a name match must win over send order")

        chosen = Handler._match_batch_subtitle({"name": "nomatch.mp4"}, subs, {"a", "b"})
        self.assertEqual(chosen["file_id"], "c", "the leftover subtitle is used in order")

        self.assertIsNone(Handler._match_batch_subtitle({"name": "x.mp4"}, subs, {"a", "b", "c"}))

    def test_collecting_adds_a_valid_subtitle_and_refuses_a_bad_one(self):
        converter = _RecordingConverter()
        handler = self._handler(converter)
        session = {"bulk_list": [self._video("clip.mp4")]}
        context = _FakeContext()
        update = _FakeUpdate([])
        document = SimpleNamespace(file_id="srt1", file_name="clip.srt", file_size=100)

        asyncio.run(handler._collect_batch_subtitle(update, context, session, document, ".srt"))

        self.assertEqual(len(session["subtitle_files"]), 1)
        self.assertTrue(os.path.exists(session["subtitle_files"][0]["path"]))

        bad = SimpleNamespace(file_id="srt2", file_name="bad.srt", file_size=10)
        context_bad = _FakeContext(payload=b"\x00 not a subtitle")
        asyncio.run(handler._collect_batch_subtitle(update, context_bad, session, bad, ".srt"))

        self.assertEqual(len(session["subtitle_files"]), 1, "an invalid subtitle was queued")

    def test_the_run_merges_each_video_in_order_and_summarises(self):
        converter = _RecordingConverter()
        handler = self._handler(converter)
        first = self._video("one.mp4")
        second = self._video("two.mp4")
        session = {
            "bulk_list": [first, second],
            "subtitle_files": [
                {"file_id": "s1", "name": "one.srt", "path": self._write_srt("one.srt")},
                {"file_id": "s2", "name": "two.srt", "path": self._write_srt("two.srt")},
            ],
        }
        query = _FakeQuery()
        update = _FakeUpdate([])

        asyncio.run(handler._run_batch_subtitles(update, _FakeContext(), session, query, 7))

        self.assertEqual(len(converter.calls), 2, "the batch did not merge every video")
        self.assertEqual(len(handler.delivered), 2, "not every merged video was delivered")
        self.assertIn("2/2 merged", query.text, "the run did not summarise itself")
        self.assertIn("one.mp4", query.text)
        self.assertEqual(session["bulk_list"], [], "the batch was not cleared after the run")
        self.assertNotIn("subtitle_files", session)

    def test_the_run_skips_a_video_with_no_subtitle_and_says_so(self):
        converter = _RecordingConverter()
        handler = self._handler(converter)
        session = {
            "bulk_list": [self._video("one.mp4"), self._video("two.mp4")],
            "subtitle_files": [{"file_id": "s1", "name": "one.srt", "path": self._write_srt("one.srt")}],
        }
        query = _FakeQuery()

        asyncio.run(handler._run_batch_subtitles(_FakeUpdate([]), _FakeContext(), session, query, 7))

        self.assertEqual(len(converter.calls), 1)
        self.assertIn("no matching subtitle", query.text)


class _BatchHandler:
    """The batch path with the fetch swapped out; everything else is the real code."""

    def __init__(self, converter):
        self.converter = converter
        self.delivered: list[str] = []
        self.edits: list[str] = []

    async def _check_conversion_quota(self, update, context):
        return True

    async def _ensure_current_file_downloaded(self, update, context, session):
        # The session's current_file is the video being worked on; a real fetch
        # would put its bytes here. Each entry already carries its own path.
        return None

    def _persist_session(self, user_id):
        return None

    async def _send_video_result(self, bot, chat_id, file_path, caption="", **delivery_options):
        self.delivered.append(file_path)
        return "file-id"

    async def _send_part_via_userbot(self, *args, **kwargs):
        return True

    async def safe_edit(self, query, text, **kwargs):
        self.edits.append(text)
        query.text = text
        return None


class _FakeQuery:
    def __init__(self):
        self.text = ""
        self.message = SimpleNamespace(message_id=1, edit_text=self._edit)

    async def _edit(self, text, **kwargs):
        self.text = text
        return SimpleNamespace(message_id=1)


class ButtonRopeConsistencyTests(unittest.TestCase):
    """Every button that needs the bytes asks the shared guard before resolving.

    A lazily-registered upload is the case that broke: resolving it directly
    answers "File not available on disk." no matter which button pressed it. The
    subtitle action and both screenshot prompts have to take the same step the
    rest of the buttons take.
    """

    def _branch(self, src, marker, end_marker):
        start = src.index(marker)
        end = src.index(end_marker, start)
        return src[start:end]

    def test_the_subtitle_path_fetches_before_it_resolves(self):
        src = read_source("handlers.py")
        body = src[src.index("async def _apply_subtitle_file(") : src.index("async def handle_document(")]
        self.assertIn("_ensure_local_media", body)
        self.assertLess(
            body.index("_ensure_local_media"),
            body.index("_resolve_local_source"),
            "the subtitle path resolved the media before fetching it",
        )

    def test_the_screenshot_prompts_fetch_before_they_resolve(self):
        src = read_source("handlers.py")
        for marker, end_marker in (
            ('context.user_data.get("awaiting_screenshot_time")', 'context.user_data.get("awaiting_screenshot_count")'),
            ('context.user_data.get("awaiting_screenshot_count")', 'context.user_data.get("awaiting_framerate")'),
        ):
            branch = self._branch(src, marker, end_marker)
            self.assertIn("_ensure_local_media", branch, branch)
            self.assertLess(branch.index("_ensure_local_media"), branch.index("_resolve_local_source"))


if __name__ == "__main__":
    unittest.main()
