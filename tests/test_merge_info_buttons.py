"""The ✂️/🔀/🖼️ buttons: merge variants, info, metadata, screenshots.

The 🔀 Merge button used to run a merge immediately and could only answer "need
at least 2 videos"; it now opens the merge variants (merge, merge + trim/compress/
convert), and the merge itself accepts a variant so the follow-up is one extra
pass. The Metadata Editor and Media Information buttons had their own defects -
the editor resolved the media without fetching it, and the info text wrote ``**``
markdown with no ``parse_mode`` - both pinned here.
"""

import asyncio
import os
import tempfile
import unittest
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock, patch

from source_helpers import read_source

import handlers as handlers_module
from handlers import EnhancedMediaHandler
from utils.keyboard_utils import MediaMenuBuilder

Handler = EnhancedMediaHandler


class _MergeConverter:
    supported_formats = {"video": [".mp4", ".mkv", ".mov"], "audio": [".mp3"]}

    def __init__(self, fail_follow_up=False):
        self.calls: list[tuple] = []
        self.fail_follow_up = fail_follow_up

    async def merge_videos(self, paths, output_path):
        self.calls.append(("merge", list(paths)))
        with open(output_path, "wb") as fh:
            fh.write(b"merged")
        return True

    async def merge_videos_compressed(self, paths, output_path):
        self.calls.append(("merge-compressed", list(paths)))
        with open(output_path, "wb") as fh:
            fh.write(b"compressed-merge")
        return True

    async def trim_video(self, input_path, output_path, start_time, end_time):
        self.calls.append(("trim", start_time, end_time))
        if self.fail_follow_up:
            return False
        with open(output_path, "wb") as fh:
            fh.write(b"trimmed")
        return True

    async def execute_ffmpeg(self, cmd, input_path, output_path):
        self.calls.append(("ffmpeg", list(cmd)))
        if self.fail_follow_up:
            return False, "boom"
        with open(output_path, "wb") as fh:
            fh.write(b"compressed")
        return True, ""

    async def convert_video_format(self, input_path, output_path, target_format):
        self.calls.append(("convert", target_format))
        if self.fail_follow_up:
            return False
        with open(output_path, "wb") as fh:
            fh.write(b"converted")
        return True


class _MergeHandler:
    def __init__(self, converter):
        self.converter = converter
        self.notices: list[str] = []
        self.delivered: list[str] = []

    async def notify(self, text, **kwargs):
        self.notices.append(text)

    async def _send_video_result(self, bot, chat_id, file_path, caption="", **options):
        self.delivered.append(file_path)
        return "file-id"

    async def _deliver_inline_result(self, update, context, file_path, current_file, **options):
        self.delivered.append(file_path)
        return True

    async def _run_with_concurrency_limit(self, user_id, task_name, coroutine):
        return await coroutine


class _FakeUpdate:
    def __init__(self):
        self.callback_query = SimpleNamespace(message=SimpleNamespace(message_id=1))
        self.message = SimpleNamespace(message_id=1)
        self.effective_chat = SimpleNamespace(id=99)
        self.effective_user = SimpleNamespace(id=7)


class MergeVariantTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.config_patch = patch.object(
            handlers_module,
            "config",
            SimpleNamespace(OUTPUT_PATH=self.tmp.name, TEMP_PATH=os.path.join(self.tmp.name, "temp")),
        )
        self.config_patch.start()

    def tearDown(self):
        self.config_patch.stop()
        self.tmp.cleanup()

    def _source(self, name):
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as fh:
            fh.write(b"video")
        return path

    def _run(self, post, converter, **kwargs):
        handler = _MergeHandler(converter)
        handler._merge_videos_core = MethodType(Handler._merge_videos_core, handler)
        handler._video_merge_ready = Handler._video_merge_ready

        async def _prepare(_self, _update, _context, session, _notify):
            return list(session["merge_list"]), []

        handler._prepare_video_merge_sources = MethodType(_prepare, handler)
        first = {"id": "first", "name": "a.mp4", "source_name": "a.mp4", "type": "video", "size": 5}
        second = {"id": "second", "name": "b.mp4", "source_name": "b.mp4", "type": "video", "size": 5}
        session = {
            "merge_list": [self._source("a.mp4"), self._source("b.mp4")],
            "video_merge_selection": [first, second],
            "current_file": {"id": "x", "name": "out.mp4", "type": "video"},
        }
        asyncio.run(
            handler._merge_videos_core(
                _FakeUpdate(), SimpleNamespace(bot=object()), session, notify=handler.notify, post=post, **kwargs
            )
        )
        return handler, session

    def test_the_review_menu_requires_two_valid_video_selections(self):
        markup = MediaMenuBuilder.get_video_merge_review_menu(["1", "2"], True)
        data = [button.callback_data for row in markup.inline_keyboard for button in row]
        for expected in ("merge_videos_start", "merge_trim", "merge_convert", "merge_remove:0", "merge_remove:1"):
            self.assertIn(expected, data)
        self.assertNotIn("merge_compress", data)
        blocked = MediaMenuBuilder.get_video_merge_review_menu(["1", "2", "3"], False)
        blocked_data = [button.callback_data for row in blocked.inline_keyboard for button in row]
        self.assertNotIn("merge_videos_start", blocked_data)

    def test_the_context_menu_merge_button_opens_the_options(self):
        markup = MediaMenuBuilder.get_main_menu("video")
        merge_buttons = [button for row in markup.inline_keyboard for button in row if button.text == "🔀 Merge"]
        self.assertEqual(len(merge_buttons), 1)
        self.assertEqual(merge_buttons[0].callback_data, "merge_options")

    def test_video_tools_no_longer_contains_the_merge_entry_point(self):
        markup = MediaMenuBuilder.get_video_tools_menu()
        callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]
        self.assertNotIn("merge_options", callbacks)

    def test_plain_merge_compresses_delivers_and_clears_the_selection(self):
        converter = _MergeConverter()
        handler, session = self._run(None, converter)

        self.assertEqual([call[0] for call in converter.calls], ["merge-compressed"])
        self.assertEqual(len(handler.delivered), 1)
        self.assertEqual(session["video_merge_selection"], [])

    def test_merge_and_trim_runs_the_cut_after_the_merge(self):
        converter = _MergeConverter()
        handler, _ = self._run("trim", converter, trim=("00:00:10", "00:00:40"))

        self.assertEqual([call[0] for call in converter.calls], ["merge-compressed", "trim"])
        self.assertEqual(converter.calls[1][1:], ("00:00:10", "00:00:40"))
        self.assertEqual(len(handler.delivered), 1)

    def test_merge_and_convert_changes_the_container(self):
        converter = _MergeConverter()
        handler, _ = self._run("convert", converter, target_format="mkv")

        self.assertEqual([call[0] for call in converter.calls], ["merge-compressed", "convert"])
        self.assertEqual(converter.calls[1][1], "mkv")
        self.assertTrue(handler.delivered[0].endswith(".mkv"), handler.delivered)

    def test_a_failed_follow_up_keeps_the_merged_file_for_a_retry(self):
        """The merge is a complete video; a failed cut must not throw it away."""
        converter = _MergeConverter(fail_follow_up=True)
        handler, session = self._run("trim", converter, trim=("00:00:10", "00:00:40"))

        self.assertEqual([call[0] for call in converter.calls], ["merge-compressed", "trim"])
        self.assertEqual(handler.delivered, [], "a failed follow-up was still delivered")
        kept = session["current_file"].get("path")
        self.assertTrue(kept and os.path.exists(kept), "the merged file was not kept")
        self.assertEqual(len(session["video_merge_selection"]), 2)
        self.assertTrue(any("kept" in note for note in handler.notices), handler.notices)

    def test_video_merge_candidates_ignore_non_video_bulk_items(self):
        session = {
            "bulk_list": [
                {"id": "v1", "type": "video", "name": "one.mp4", "size": 12},
                {"id": "a1", "type": "audio", "name": "song.mp3", "size": 20},
                {"id": "v2", "type": "video", "name": "two.mkv", "size": 12},
            ]
        }
        result = Handler._video_merge_candidates(session)

        self.assertEqual([item["id"] for item in result], ["v1", "v2"])
        self.assertIsNone(Handler._video_merge_item_error(result[0]))
        self.assertEqual(Handler._video_merge_item_error(result[1]), "MP4 only")

    def test_video_merge_candidates_enforce_the_per_file_size_ceiling(self):
        too_large = {
            "id": "large",
            "type": "video",
            "name": "large.mp4",
            "source_name": "large.mp4",
            "size": 500 * 1024 * 1024 + 1,
        }
        self.assertEqual(Handler._video_merge_item_error(too_large), "over 500 MB")

    def test_video_merge_requires_exactly_two_eligible_mp4_files(self):
        valid = [
            {"type": "video", "source_name": "one.mp4", "size": 10},
            {"type": "video", "source_name": "two.mp4", "size": 10},
        ]
        self.assertTrue(Handler._video_merge_ready({"video_merge_selection": valid}))
        self.assertFalse(Handler._video_merge_ready({"video_merge_selection": valid[:1]}))
        self.assertFalse(Handler._video_merge_ready({"video_merge_selection": valid + valid[:1]}))
        invalid_format = [valid[0], {**valid[1], "source_name": "two.mkv"}]
        self.assertFalse(Handler._video_merge_ready({"video_merge_selection": invalid_format}))

    def test_audio_merge_candidates_are_auto_detected_and_typed(self):
        session = {
            "bulk_list": [
                {"id": "a1", "type": "audio", "source_name": "one.mp3", "name": "one.mp3", "size": 10},
                {"id": "v1", "type": "video", "source_name": "clip.mp4", "size": 10},
                {"id": "a1", "type": "audio", "source_name": "one.mp3", "name": "one.mp3", "size": 10},
                {"id": "a2", "type": "audio", "source_name": "two.ogg", "name": "two.ogg", "size": 20},
            ]
        }

        result = Handler._audio_merge_candidates(session)

        self.assertEqual([item["id"] for item in result], ["a1", "a2"])
        self.assertTrue(Handler._audio_merge_ready({"audio_merge_selection": result}))
        self.assertFalse(Handler._audio_merge_ready({"audio_merge_selection": result[:1]}))
        self.assertFalse(
            Handler._audio_merge_ready(
                {"audio_merge_selection": [result[0], {**result[1], "source_name": "not-audio.mp4"}]}
            )
        )

    def test_audio_merge_review_has_remove_start_reset_not_manual_add_view_or_clear(self):
        markup = MediaMenuBuilder.get_audio_merge_review_menu(["1", "2"], True)
        callbacks = [button.callback_data for row in markup.inline_keyboard for button in row]

        self.assertIn("audio_merge_remove:0", callbacks)
        self.assertIn("audio_merge_remove:1", callbacks)
        self.assertIn("merge_audios_start", callbacks)
        self.assertIn("audio_merge_reset", callbacks)
        self.assertFalse({"merge_add", "merge_view", "merge_clear"} & set(callbacks))

    def test_audio_merge_refreshes_exact_sources_and_validates_streams(self):
        handler = object.__new__(Handler)
        refreshed_ids = []
        persisted_current_files = []
        previous = {"id": "unrelated", "type": "audio", "name": "unrelated.mp3"}

        async def _ensure(_self, _update, _context, session, *, force_refresh=False):
            self.assertTrue(force_refresh)
            item = session["current_file"]
            refreshed_ids.append(item["id"])
            path = os.path.join(self.tmp.name, f"{item['id']}.mp3")
            with open(path, "wb") as fh:
                fh.write(b"audio")
            item["path"] = path

        handler._ensure_current_file_downloaded = MethodType(_ensure, handler)
        handler._local_copy = MethodType(Handler._local_copy, handler)
        handler._persist_session = lambda _user_id: persisted_current_files.append(session.get("current_file"))
        session = {
            "current_file": previous,
            "audio_merge_selection": [
                {"id": "telegram-a1", "type": "audio", "name": "one.mp3", "source_name": "one.mp3", "size": 5},
                {"id": "telegram-a2", "type": "audio", "name": "two.mp3", "source_name": "two.mp3", "size": 5},
            ],
        }
        probe = AsyncMock(return_value={"audio_streams": 1})

        async def _notify(_text):
            return None

        with patch("utils.ffmpeg_runner.probe_media", probe):
            paths, owned = asyncio.run(
                Handler._prepare_audio_merge_sources(
                    handler, _FakeUpdate(), SimpleNamespace(bot=object()), session, _notify
                )
            )

        self.assertEqual(refreshed_ids, ["telegram-a1", "telegram-a2"])
        self.assertEqual(len(paths), 2)
        self.assertIs(session["current_file"], previous)
        self.assertEqual(persisted_current_files, [previous, previous])
        for path in owned:
            os.remove(path)

    def test_merge_fetches_the_exact_two_sources_sequentially_and_cleans_them(self):
        handler = object.__new__(Handler)
        fetched_ids = []
        persisted_current_files = []
        previous = {"id": "unrelated", "name": "other.mp4"}

        async def _adopt(_self, _item, **_kwargs):
            return None

        async def _ensure(_self, _update, _context, session, *, force_refresh=False):
            item = session["current_file"]
            self.assertTrue(force_refresh)
            fetched_ids.append(item["id"])
            path = os.path.join(self.tmp.name, f"{item['id']}_{len(fetched_ids)}.mp4")
            with open(path, "wb") as fh:
                fh.write(b"data")
            item["path"] = path

        handler._adopt_stored_source = MethodType(_adopt, handler)
        handler._ensure_current_file_downloaded = MethodType(_ensure, handler)
        handler._local_copy = MethodType(Handler._local_copy, handler)
        handler._persist_session = lambda _user_id: persisted_current_files.append(session.get("current_file"))
        session = {
            "current_file": previous,
            "video_merge_selection": [
                {"id": "telegram-1", "type": "video", "name": "one.mp4", "source_name": "one.mp4", "size": 4},
                {"id": "telegram-2", "type": "video", "name": "two.mp4", "source_name": "two.mp4", "size": 4},
            ],
        }
        probe = AsyncMock(return_value={"format_name": "mov,mp4,m4a", "video_streams": 1})

        async def _notify(_text):
            return None

        with patch("utils.ffmpeg_runner.probe_media", probe):
            paths, owned = asyncio.run(
                Handler._prepare_video_merge_sources(
                    handler, _FakeUpdate(), SimpleNamespace(bot=object()), session, _notify
                )
            )

        self.assertEqual(fetched_ids, ["telegram-1", "telegram-2"])
        self.assertEqual([os.path.getsize(path) for path in paths], [4, 4])
        self.assertIs(session["current_file"], previous)
        self.assertEqual(persisted_current_files, [previous, previous])
        for path in owned:
            os.remove(path)
        self.assertFalse(any(os.path.exists(path) for path in owned))

    def test_merge_fetches_exact_telegram_files_instead_of_trusting_size_only_s3(self):
        handler = object.__new__(Handler)
        refreshed_ids = []

        async def _ensure(_self, _update, _context, session, *, force_refresh=False):
            self.assertTrue(force_refresh)
            item = session["current_file"]
            refreshed_ids.append(item["id"])
            path = os.path.join(self.tmp.name, f"{item['id']}.mp4")
            with open(path, "wb") as fh:
                fh.write(b"data")
            item["path"] = path

        handler._adopt_stored_source = AsyncMock(
            side_effect=AssertionError("size-only S3 validation is not sufficient for merge inputs")
        )
        handler._ensure_current_file_downloaded = MethodType(_ensure, handler)
        handler._local_copy = MethodType(Handler._local_copy, handler)
        handler._persist_session = lambda _user_id: None
        session = {
            "video_merge_selection": [
                {
                    "id": f"telegram-{index}",
                    "file_unique_id": f"unique-{index}",
                    "type": "video",
                    "name": f"{index}.mp4",
                    "source_name": f"{index}.mp4",
                    "size": 4,
                }
                for index in (1, 2)
            ]
        }
        probe = AsyncMock(return_value={"format_name": "mov,mp4,m4a", "video_streams": 1})

        async def _notify(_text):
            return None

        with patch("utils.ffmpeg_runner.probe_media", probe):
            paths, owned = asyncio.run(
                Handler._prepare_video_merge_sources(
                    handler, _FakeUpdate(), SimpleNamespace(bot=object()), session, _notify
                )
            )

        self.assertEqual(refreshed_ids, ["telegram-1", "telegram-2"])
        handler._adopt_stored_source.assert_not_awaited()
        self.assertEqual(len(paths), 2)
        self.assertEqual(len(owned), 2)
        for path in owned:
            os.remove(path)

    def test_compressed_merge_is_one_low_thread_h264_crf28_pass(self):
        import tasks.conversion_tasks as conversion_tasks

        first = self._source("source's.mp4")
        second = self._source("second.mp4")
        output = os.path.join(self.tmp.name, "merged.mp4")
        commands = []
        manifests = []

        async def _run(command, **_kwargs):
            commands.append(command)
            manifest = command[command.index("-i") + 1]
            with open(manifest, encoding="utf-8") as fh:
                manifests.append(fh.read())
            return b"", b"", 0

        with (
            patch("tasks.conversion_tasks.run_subprocess_with_timeout", new=_run),
            patch.dict(os.environ, {"FFMPEG_THREADS": "1", "FFMPEG_FILTER_THREADS": "1"}),
        ):
            ok, _message = asyncio.run(conversion_tasks.merge_videos([first, second], output, reencode=True))

        command = commands[0]
        self.assertTrue(ok)
        self.assertIn("libx264", command)
        self.assertIn("28", command)
        self.assertIn("1", command)
        self.assertNotIn("copy", command)
        self.assertIn("'\\''", manifests[0])

    def test_merge_rejects_a_non_mp4_container_and_removes_its_staged_source(self):
        handler = object.__new__(Handler)
        staged = []

        async def _ensure(_self, _update, _context, session, *, force_refresh=False):
            self.assertTrue(force_refresh)
            item = session["current_file"]
            path = os.path.join(self.tmp.name, "wrong-container.mp4")
            with open(path, "wb") as fh:
                fh.write(b"data")
            item["path"] = path
            staged.append(path)

        handler._ensure_current_file_downloaded = MethodType(_ensure, handler)
        handler._local_copy = MethodType(Handler._local_copy, handler)
        session = {
            "video_merge_selection": [
                {"id": "exact-1", "type": "video", "source_name": "one.mp4", "name": "one.mp4", "size": 4},
                {"id": "exact-2", "type": "video", "source_name": "two.mp4", "name": "two.mp4", "size": 4},
            ]
        }

        async def _notify(_text):
            return None

        probe = AsyncMock(return_value={"format_name": "matroska,webm", "video_streams": 1})
        with (
            patch("utils.ffmpeg_runner.probe_media", probe),
            self.assertRaisesRegex(RuntimeError, "not a valid MP4 video"),
        ):
            asyncio.run(
                Handler._prepare_video_merge_sources(
                    handler, _FakeUpdate(), SimpleNamespace(bot=object()), session, _notify
                )
            )
        self.assertTrue(staged)
        self.assertFalse(any(os.path.exists(path) for path in staged))


class SourceFixTests(unittest.TestCase):
    """The editor/info/merge paths, pinned where the defect lived."""

    def test_the_metadata_editor_fetches_before_it_resolves(self):
        src = read_source("handlers.py")
        branch = src[src.index('context.user_data.get("awaiting_metadata")') :]
        branch = branch[: branch.index("json.JSONDecodeError")] if "json.JSONDecodeError" in branch else branch
        self.assertIn("_ensure_local_media", branch)
        self.assertLess(branch.index("_ensure_local_media"), branch.index("_resolve_local_source"))

    def test_media_information_renders_its_markup(self):
        src = read_source("handlers.py")
        body = src[src.index("async def show_full_info(") : src.index("async def create_archive(")]
        self.assertIn("<b>Full Media Analysis</b>", body)
        self.assertIn('parse_mode="HTML"', body)
        self.assertNotIn("**Full Media Analysis**", body)

    def test_media_information_coerces_what_ffprobe_reports(self):
        """ffprobe reports numbers as strings; ``//`` on one crashed the whole panel."""
        src = read_source("handlers.py")
        body = src[src.index("async def show_full_info(") : src.index("async def create_archive(")]
        self.assertIn('_probe_int(format_info.get("size"))', body)
        self.assertNotIn('format_info.get("size", 0) // 1024 // 1024', body)
        # The helpers themselves, including the shapes ffprobe actually sends:
        # a numeric string, a missing key, and an empty one.
        self.assertEqual(handlers_module._probe_int("43308482"), 43308482)
        self.assertEqual(handlers_module._probe_int(None), 0)
        self.assertEqual(handlers_module._probe_int(""), 0)
        self.assertEqual(handlers_module._probe_float("2109.396"), 2109.396)
        self.assertEqual(handlers_module._probe_float(None), 0.0)

    def test_the_metadata_editor_keeps_the_source_container(self):
        """A hardcoded ``.mp4`` made ``-c copy`` fail for every other input."""
        src = read_source("handlers.py")
        branch = src[src.index('context.user_data.get("awaiting_metadata")') :]
        self.assertIn('safe_extension(current_file.get("name")', branch)
        self.assertNotIn("_with_metadata.mp4", branch)

    def test_apply_bulk_routes_to_the_subtitle_run_when_the_toggle_is_on(self):
        src = read_source("handlers.py")
        body = src[src.index('elif data == "bulk_apply":') : src.index('elif data == "bulk_forward":')]
        self.assertIn('_bulk_settings.get("bulk_subtitles")', body)
        self.assertIn("_run_batch_subtitles", body)

    def test_merge_add_fetches_before_it_resolves(self):
        src = read_source("handlers.py")
        branch = src[src.index('elif data == "merge_add":') :]
        branch = branch[: branch.index("merge_list")] if "merge_list" in branch else branch
        self.assertIn("_ensure_local_media", branch)
        self.assertIn("_resolve_local_source", branch)


class ScreenshotButtonTests(unittest.TestCase):
    """🖼️ Manual Shots / Screenshots / Thumbnail Extractor share the fetch rope."""

    def test_the_screenshot_paths_fetch_before_they_resolve(self):
        src = read_source("handlers.py")
        for name, end in (
            ("_quick_screenshot", "async def take_screenshot("),
            ("take_screenshot", "async def create_thumbnail_grid("),
            ("create_thumbnail_grid", "async def extract_streams("),
        ):
            body = src[src.index(f"async def {name}(") : src.index(end)]
            self.assertIn("_ensure_local_media", body, name)
            self.assertLess(
                body.index("_ensure_local_media"),
                body.index("_resolve_local_source"),
                f"{name} resolved before it fetched",
            )

    def test_manual_shots_routes_to_a_handled_callback(self):
        src = read_source("handlers.py")
        self.assertIn('"manual_shots": "screenshot_custom"', src)
        # The generic branch that answers it exists and reads the option name.
        self.assertIn('data.startswith("screenshot_")', src)
        self.assertIn('option = data.split("_")[1]', src)

    def test_the_thumbnail_extractor_alias_is_wired(self):
        src = read_source("handlers.py")
        self.assertIn('"thumbnail_extractor": "thumbnail_grid"', src)
        self.assertIn("await self.create_thumbnail_grid(update, context, session)", src)


if __name__ == "__main__":
    unittest.main()
