"""Compress in normal and bulk mode: one setting, one recipe, both guarded.

The button and the batch are two pipelines, and a user must not get a different
encode - or a different CRF - depending on which one they pressed. Both read the
same stored preference (``COMPRESS_QUALITY_KEY`` / ``bulk_crf``) and build the
same ffmpeg recipe, and both refuse a file with no video stream to compress.
"""

import unittest

from source_helpers import read_source

from handlers import (
    _BULK_COMPRESS_AUDIO_ARGS,
    _BULK_COMPRESS_CRF_DEFAULT,
    _bulk_audio_supported,
    _bulk_photo_supported,
    _bulk_video_recipe,
    _resolve_bulk_plan,
    _sanitize_bulk_crf,
    _sanitize_bulk_extract_bitrate,
)


class BulkCompressPlanTests(unittest.TestCase):
    def test_the_batch_uses_the_stored_crf(self):
        plan = _resolve_bulk_plan({"bulk_compress": True, "bulk_crf": 31})
        self.assertEqual(plan["applied"], ["bulk_compress"])
        self.assertEqual(plan["output_ext"], ".mp4")
        self.assertEqual(plan["crf"], 31)
        self.assertEqual(plan["convert_type"], "ffmpeg")
        self.assertEqual(plan["ffmpeg_args"][-len(_BULK_COMPRESS_AUDIO_ARGS) :], _BULK_COMPRESS_AUDIO_ARGS)

    def test_an_unusable_crf_falls_back(self):
        plan = _resolve_bulk_plan({"bulk_compress": True, "bulk_crf": "junk"})
        self.assertEqual(plan["crf"], _sanitize_bulk_crf(None))
        self.assertEqual(plan["crf"], _BULK_COMPRESS_CRF_DEFAULT)

    def test_the_recipe_is_the_same_for_every_caller(self):
        video_args, audio_args = _bulk_video_recipe("bulk_compress", {"bulk_crf": 24})
        self.assertEqual(
            video_args,
            ["-c:v", "libx264", "-preset", "medium", "-crf", "24", "-movflags", "+faststart"],
        )
        plan = _resolve_bulk_plan({"bulk_compress": True, "bulk_crf": 24})
        self.assertEqual(plan["ffmpeg_args"], video_args + audio_args)


class CompressGuardTests(unittest.TestCase):
    def test_a_photo_can_be_compressed_but_audio_cannot(self):
        plan = _resolve_bulk_plan({"bulk_compress": True})
        self.assertTrue(_bulk_photo_supported(plan))
        self.assertFalse(_bulk_audio_supported(plan))

    def test_extract_audio_is_unaffected_by_the_compress_setting(self):
        """The two quality settings are independent: Extract Audio has its own."""
        plan = _resolve_bulk_plan({"bulk_extract_audio": True, "bulk_extract_bitrate": "96k"})
        self.assertEqual(plan["extract_bitrate"], "96k")
        self.assertEqual(plan["extract_bitrate"], _sanitize_bulk_extract_bitrate("96k"))


class CompressWiringTests(unittest.TestCase):
    def test_both_pipelines_read_the_one_preference(self):
        src = read_source("handlers.py")
        from utils.callbacks import COMPRESS_QUALITY_KEY

        self.assertEqual(COMPRESS_QUALITY_KEY, "bulk_crf")
        # The single button's menu is marked from the stored CRF...
        self.assertIn("get_compression_menu(_user_compress_crf(user_id))", src)
        # ...and the batch resolves the same key through the same validator.
        self.assertIn('crf = _sanitize_bulk_crf(settings.get("bulk_crf"))', src)
        self.assertIn("user_settings.get_user_setting(user_id, COMPRESS_QUALITY_KEY)", src)

    def test_the_single_button_refuses_a_non_video(self):
        src = read_source("handlers.py")
        self.assertIn('if not current_file or current_file["type"] != "video":', src)
        self.assertIn("❌ No video file found.", src)

    def test_a_rename_survives_the_compress_on_both_paths(self):
        """A compressed file arrives under the renamed stem, and the batch too."""
        src = read_source("handlers.py")
        self.assertIn("_video_delivery_name(current_file, output_path)", src)
        self.assertIn('_bulk_name = f.get("name") or os.path.basename(out_path)', src)


if __name__ == "__main__":
    unittest.main()
