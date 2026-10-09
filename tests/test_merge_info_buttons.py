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
from unittest.mock import patch

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
        session = {
            "merge_list": [self._source("a.mp4"), self._source("b.mp4")],
            "current_file": {"id": "x", "name": "out.mp4", "type": "video"},
        }
        asyncio.run(
            handler._merge_videos_core(
                _FakeUpdate(), SimpleNamespace(bot=object()), session, notify=handler.notify, post=post, **kwargs
            )
        )
        return handler, session

    def test_the_merge_menu_offers_the_variants(self):
        markup = MediaMenuBuilder.get_merge_options_menu()
        data = [button.callback_data for row in markup.inline_keyboard for button in row]
        for expected in ("merge_videos_start", "merge_trim", "merge_compress", "merge_convert"):
            self.assertIn(expected, data)

    def test_the_video_tools_merge_button_opens_the_options(self):
        markup = MediaMenuBuilder.get_video_tools_menu()
        merge_buttons = [button for row in markup.inline_keyboard for button in row if button.text == "🔀 Merge"]
        self.assertEqual(len(merge_buttons), 1)
        self.assertEqual(merge_buttons[0].callback_data, "merge_options")

    def test_plain_merge_delivers_and_clears_the_list(self):
        converter = _MergeConverter()
        handler, session = self._run(None, converter)

        self.assertEqual([call[0] for call in converter.calls], ["merge"])
        self.assertEqual(len(handler.delivered), 1)
        self.assertEqual(session["merge_list"], [])

    def test_merge_and_trim_runs_the_cut_after_the_merge(self):
        converter = _MergeConverter()
        handler, _ = self._run("trim", converter, trim=("00:00:10", "00:00:40"))

        self.assertEqual([call[0] for call in converter.calls], ["merge", "trim"])
        self.assertEqual(converter.calls[1][1:], ("00:00:10", "00:00:40"))
        self.assertEqual(len(handler.delivered), 1)

    def test_merge_and_compress_re_encodes_the_merged_file(self):
        converter = _MergeConverter()
        handler, _ = self._run("compress", converter)

        self.assertEqual([call[0] for call in converter.calls], ["merge", "ffmpeg"])
        self.assertIn("libx264", converter.calls[1][1])
        self.assertEqual(len(handler.delivered), 1)

    def test_merge_and_convert_changes_the_container(self):
        converter = _MergeConverter()
        handler, _ = self._run("convert", converter, target_format="mkv")

        self.assertEqual([call[0] for call in converter.calls], ["merge", "convert"])
        self.assertEqual(converter.calls[1][1], "mkv")
        self.assertTrue(handler.delivered[0].endswith(".mkv"), handler.delivered)

    def test_a_failed_follow_up_keeps_the_merged_file_for_a_retry(self):
        """The merge is a complete video; a failed cut must not throw it away."""
        converter = _MergeConverter(fail_follow_up=True)
        handler, session = self._run("trim", converter, trim=("00:00:10", "00:00:40"))

        self.assertEqual([call[0] for call in converter.calls], ["merge", "trim"])
        self.assertEqual(handler.delivered, [], "a failed follow-up was still delivered")
        kept = session["current_file"].get("path")
        self.assertTrue(kept and os.path.exists(kept), "the merged file was not kept")
        self.assertEqual(session["merge_list"], [])
        self.assertTrue(any("kept" in note for note in handler.notices), handler.notices)

    def test_each_merge_variant_button_carries_its_help(self):
        markup = MediaMenuBuilder.get_merge_options_menu()
        texts = {button.callback_data: button.text for row in markup.inline_keyboard for button in row}
        for data in ("merge_videos_start", "merge_trim", "merge_compress", "merge_convert"):
            self.assertIn(data, texts, data)
            self.assertIn("\n", texts[data], f"{data} has no help line: {texts[data]!r}")
        self.assertIn("veryfast", texts["merge_compress"])


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
