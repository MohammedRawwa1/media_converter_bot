"""Slideshow generation from queued photos, and the per-file Apply summary."""

import os
import tempfile
import unittest
from unittest.mock import patch

from source_helpers import read_source

from handlers import (
    _BULK_NAME_MAX,
    _BULK_SLIDESHOW_MAX,
    _BULK_SLIDESHOW_MIN,
    _BULK_SLIDESHOW_SECONDS,
    _BULK_SUMMARY_MAX_LINES,
    _IMAGE_EXTS,
    _bulk_batch_lines,
    _bulk_display_name,
    _bulk_slideshow_music,
    _sanitize_bulk_slideshow_seconds,
)
from tasks import conversion_tasks as ct

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TMP = tempfile.gettempdir()


def _write_image(name, data=b"jpegbytes"):
    path = os.path.join(TMP, name)
    with open(path, "wb") as fh:
        fh.write(data)
    return path


class _RecordingRunner:
    """Async stand-in for run_subprocess_with_timeout that records the command."""

    def __init__(self, returncode=0, stderr=b""):
        self.commands = []
        self.returncode = returncode
        self.stderr = stderr

    async def __call__(self, cmd, timeout_seconds=18000, operation_name="Operation"):
        self.commands.append(list(cmd))
        return b"", self.stderr, self.returncode


class CreateSlideshowTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, images, runner, output=None, seconds=3.0, music=None):
        output = output or os.path.join(TMP, "slideshow_out.mp4")
        with patch.object(ct, "run_subprocess_with_timeout", runner):
            result = await ct.create_slideshow(images, output, seconds_per_image=seconds, music_path=music)
        return result, runner.commands[-1] if runner.commands else None

    async def test_builds_one_looped_input_per_image(self):
        images = [_write_image(f"ss_a{i}.jpg") for i in range(3)]
        runner = _RecordingRunner()
        (ok, _msg), cmd = await self._run(images, runner, seconds=2)

        self.assertTrue(ok)
        for path in images:
            self.assertIn(path, cmd)
        # One "-loop 1 -t 2.0 -i <image>" trio per photo, in send order.
        self.assertEqual(cmd.count("-loop"), 3)
        self.assertEqual(cmd.count("2.0"), 3)
        self.assertLess(cmd.index(images[0]), cmd.index(images[1]))
        self.assertLess(cmd.index(images[1]), cmd.index(images[2]))

    async def test_scales_each_image_and_concatenates(self):
        images = [_write_image(f"ss_b{i}.png") for i in range(2)]
        runner = _RecordingRunner()
        (_ok, _msg), cmd = await self._run(images, runner)

        filter_complex = cmd[cmd.index("-filter_complex") + 1]
        self.assertIn("force_original_aspect_ratio=decrease", filter_complex)
        self.assertIn("pad=1280:720", filter_complex)
        self.assertIn("concat=n=2:v=1:a=0[slideshow]", filter_complex)
        self.assertIn("yuv420p", cmd)
        self.assertEqual(cmd[-1], os.path.join(TMP, "slideshow_out.mp4"))

    async def test_rejects_empty_and_missing_images(self):
        runner = _RecordingRunner()
        (ok, msg), _cmd = await self._run([], runner)
        self.assertFalse(ok)
        self.assertIn("No image paths", msg)

        (ok, msg), _cmd = await self._run([os.path.join(TMP, "definitely_missing.jpg")], runner)
        self.assertFalse(ok)
        self.assertIn("Invalid image", msg)
        self.assertEqual(runner.commands, [])

    async def test_clamps_a_non_positive_duration(self):
        image = _write_image("ss_c.jpg")
        runner = _RecordingRunner()
        for bad in (0, -5, "nope"):
            (_ok, _msg), cmd = await self._run([image], runner, seconds=bad)
            self.assertEqual(cmd[cmd.index("-t") + 1], f"{_BULK_SLIDESHOW_SECONDS}")

    async def test_reports_ffmpeg_failure(self):
        image = _write_image("ss_d.jpg")
        runner = _RecordingRunner(returncode=1, stderr=b"boom: bad dimensions")
        (ok, msg), _cmd = await self._run([image], runner)
        self.assertFalse(ok)
        self.assertIn("boom", msg)


class SlideshowMusicTests(unittest.IsolatedAsyncioTestCase):
    """A queued audio file is layered under the slideshow and cut with the video."""

    async def _run(self, images, runner, music):
        output = os.path.join(TMP, "slideshow_music_out.mp4")
        with patch.object(ct, "run_subprocess_with_timeout", runner):
            result = await ct.create_slideshow(images, output, music_path=music)
        return result, runner.commands[-1] if runner.commands else None

    async def test_music_is_looped_and_cut_at_the_video_end(self):
        images = [_write_image(f"ss_m{i}.jpg") for i in range(2)]
        music = _write_image("ss_song.mp3", b"id3")
        runner = _RecordingRunner()
        (ok, _msg), cmd = await self._run(images, runner, music)

        self.assertTrue(ok)
        self.assertEqual(cmd[cmd.index("-stream_loop") + 1], "-1")
        self.assertIn(music, cmd)
        # The music is the input right after the two images, so its audio stream
        # index is len(images).
        maps = [cmd[i + 1] for i, flag in enumerate(cmd) if flag == "-map"]
        self.assertIn("[slideshow]", maps)
        self.assertIn("2:a", maps)
        self.assertIn("-shortest", cmd)

    async def test_missing_music_is_silently_ignored(self):
        images = [_write_image("ss_n0.jpg")]
        runner = _RecordingRunner()
        (ok, _msg), cmd = await self._run(images, runner, os.path.join(TMP, "nope_song.mp3"))
        self.assertTrue(ok)
        self.assertNotIn("-stream_loop", cmd)
        self.assertNotIn("-shortest", cmd)


class SlideshowPickerTests(unittest.TestCase):
    """The bulk menu exposes the slideshow seconds picker and it is validated."""

    def _buttons(self, kb):
        return {b.callback_data: b.text for row in kb.inline_keyboard for b in row}

    def test_bulk_menu_has_the_slideshow_row(self):
        from utils.callbacks import BULK_SLIDESHOW_MENU
        from utils.keyboard_utils import MediaMenuBuilder

        rendered = self._buttons(MediaMenuBuilder.get_bulk_menu({"bulk_slideshow_seconds": 5}))
        self.assertIn(BULK_SLIDESHOW_MENU, rendered)
        self.assertIn("5s", rendered[BULK_SLIDESHOW_MENU])

    def test_bulk_menu_falls_back_to_the_default(self):
        from utils.callbacks import BULK_SLIDESHOW_DEFAULT, BULK_SLIDESHOW_MENU
        from utils.keyboard_utils import MediaMenuBuilder

        rendered = self._buttons(MediaMenuBuilder.get_bulk_menu({}))
        self.assertIn(f"{BULK_SLIDESHOW_DEFAULT:g}s", rendered[BULK_SLIDESHOW_MENU])
        corrupt = self._buttons(MediaMenuBuilder.get_bulk_menu({"bulk_slideshow_seconds": "lots"}))
        self.assertIn(f"{BULK_SLIDESHOW_DEFAULT:g}s", corrupt[BULK_SLIDESHOW_MENU])

    def test_picker_marks_the_active_choice(self):
        from utils.callbacks import BULK_SLIDESHOW_CHOICES, bulk_slideshow_key
        from utils.keyboard_utils import MediaMenuBuilder

        rendered = self._buttons(MediaMenuBuilder.get_bulk_slideshow_menu(5))
        self.assertTrue(rendered[bulk_slideshow_key(5.0)].startswith("✅"))
        self.assertFalse(rendered[bulk_slideshow_key(1.0)].startswith("✅"))
        for choice in BULK_SLIDESHOW_CHOICES:
            self.assertIn(bulk_slideshow_key(choice), rendered)

    def test_seconds_are_clamped_to_the_valid_range(self):
        self.assertEqual(_sanitize_bulk_slideshow_seconds(5), 5.0)
        self.assertEqual(_sanitize_bulk_slideshow_seconds("2.5"), 2.5)
        for bad in (0, -1, _BULK_SLIDESHOW_MIN / 2, _BULK_SLIDESHOW_MAX + 1, "nope", None, True):
            self.assertEqual(_sanitize_bulk_slideshow_seconds(bad), _BULK_SLIDESHOW_SECONDS, msg=repr(bad))

    def test_apply_uses_the_stored_seconds_and_names_the_music(self):
        src = read_source("handlers.py")
        self.assertIn('_bulk_settings.get("bulk_slideshow_seconds")', src)
        self.assertIn('"music_path": _music_path,', src)
        self.assertIn("bulk_slideshow_menu", src)
        self.assertIn("bulk_set_slideshow:", src)


class SlideshowMusicSelectionTests(unittest.TestCase):
    def test_first_queued_audio_becomes_the_music(self):
        entries = [
            {"id": "v1", "type": "video"},
            {"id": "p1", "type": "photo"},
            {"id": "a1", "name": "song.mp3", "type": "audio"},
            {"id": "a2", "name": "other.mp3", "type": "audio"},
        ]
        self.assertEqual(_bulk_slideshow_music(entries)["id"], "a1")
        self.assertIsNone(_bulk_slideshow_music([{"id": "v1", "type": "video"}]))
        self.assertIsNone(_bulk_slideshow_music([]))


class ImageDocumentTests(unittest.TestCase):
    """A photo sent uncompressed arrives as a document and must still be a photo."""

    def test_image_extensions_are_covered(self):
        for ext in (".jpg", ".jpeg", ".png", ".webp", ".bmp"):
            self.assertIn(ext, _IMAGE_EXTS)

    def test_handle_document_queues_images_as_photos(self):
        src = read_source("handlers.py")
        self.assertIn("if file_ext in _IMAGE_EXTS:", src)
        self.assertIn('_register_bulk_file(session, {**session["current_file"], "type": "photo"})', src)


class BulkPreviewTests(unittest.TestCase):
    def test_lines_show_name_and_type_in_order(self):
        entries = [
            {"name": "clip.mp4", "type": "video"},
            {"name": "song.mp3", "type": "audio"},
            {"name": "pic.jpg", "type": "photo"},
        ]
        lines = _bulk_batch_lines(entries)
        self.assertEqual(lines[0], "1. clip.mp4 · video")
        self.assertEqual(lines[1], "2. song.mp3 · audio")
        self.assertEqual(lines[2], "3. pic.jpg · photo")

    def test_lines_escape_html_and_truncate(self):
        lines = _bulk_batch_lines([{"name": "a<script>.mp4", "type": "video"}])
        self.assertNotIn("<script>", lines[0])
        self.assertIn("&lt;script&gt;", lines[0])

        capped = _bulk_batch_lines([{"name": f"f{i}.mp4", "type": "video"} for i in range(15)], limit=12)
        self.assertEqual(len(capped), 13)
        self.assertEqual(capped[-1], "… +3 more")


class SlideshowWiringTests(unittest.TestCase):
    """The worker must expose the slideshow job type the bot enqueues."""

    def test_worker_handles_the_slideshow_job_type(self):
        src = read_source("workers", "ffmpeg_worker.py")
        self.assertIn('job_type == "slideshow"', src)
        self.assertIn("create_slideshow", src)
        self.assertIn("seconds_per_image", src)

    def test_apply_groups_album_photos_into_one_slideshow(self):
        src = read_source("handlers.py")
        self.assertIn('"type": "slideshow",', src)
        self.assertIn("_slideshow_photos", src)
        # Two photos are the album case; a lone photo keeps the single-file path.
        self.assertIn("_photos if len(_photos) >= 2 else []", src)


class BulkSummaryTests(unittest.TestCase):
    def test_display_name_prefers_the_stored_name(self):
        self.assertEqual(_bulk_display_name({"name": "clip.mp4", "path": "/x/y.mp4"}), "clip.mp4")

    def test_display_name_falls_back_to_the_path_then_id(self):
        self.assertEqual(_bulk_display_name({"path": os.path.join("a", "b.mp4")}), "b.mp4")
        self.assertEqual(_bulk_display_name({"id": "file-7"}), "file-7")
        self.assertEqual(_bulk_display_name(None), "file")

    def test_display_name_elides_long_names_but_keeps_the_ext(self):
        long_name = "a" * 80 + ".mp4"
        short = _bulk_display_name({"name": long_name})
        self.assertLessEqual(len(short), _BULK_NAME_MAX)
        self.assertTrue(short.endswith(".mp4"))
        self.assertIn("…", short)

    def test_apply_prints_a_per_file_section_with_job_ids(self):
        src = read_source("handlers.py")
        self.assertIn("🗂 Per-file:", src)
        self.assertIn("📋 queued · {job_id}", src)
        self.assertIn("results[:_BULK_SUMMARY_MAX_LINES]", src)
        self.assertGreater(_BULK_SUMMARY_MAX_LINES, 0)


if __name__ == "__main__":
    unittest.main()
