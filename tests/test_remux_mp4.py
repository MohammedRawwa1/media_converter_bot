"""MP4 convert remuxes when the source codecs already fit, else re-encodes.

Converting an MKV to MP4 used to always re-encode with libx264, and the
"Convert to MP4" recipe did not even name a preset - so it ran libx264's
``medium`` default, which on the memory-constrained host was slow enough to hit
the job's runtime cap and never deliver. A container change whose streams are
already H.264/HEVC + an MP4-safe audio codec should instead be a stream copy.

These tests pin the decision helper, the bulk plan marker, and the fact that
every remaining convert path states its preset so nothing silently falls back
to ``medium``.
"""

import unittest

from source_helpers import flatten, read_source

from handlers import _resolve_bulk_plan
from utils.ffmpeg_runner import (
    MP4_REMUX_TARGET_EXTS,
    mp4_remux_args,
)


def _flat(path: str) -> str:
    return flatten(read_source(path))


class Mp4RemuxArgsTests(unittest.TestCase):
    def test_h264_aac_is_copied(self):
        args = mp4_remux_args({"video_codec": "h264", "audio_codec": "aac"})
        self.assertEqual(
            args,
            [
                "-map",
                "0:v:0",
                "-map",
                "0:a:0",
                "-c:v",
                "copy",
                "-c:a",
                "copy",
                "-movflags",
                "+faststart",
            ],
        )

    def test_hevc_and_mp3_family_are_copied(self):
        for video, audio in (("hevc", "mp3"), ("h264", "ac3"), ("mpeg4", "eac3")):
            with self.subTest(video=video, audio=audio):
                args = mp4_remux_args({"video_codec": video, "audio_codec": audio})
                self.assertIsNotNone(args)
                self.assertEqual(args[args.index("-c:v") + 1], "copy")
                self.assertEqual(args[args.index("-c:a") + 1], "copy")

    def test_a_silent_video_has_no_audio_mapping(self):
        args = mp4_remux_args({"video_codec": "h264", "audio_codec": ""})
        self.assertIsNotNone(args)
        self.assertNotIn("0:a:0", args)
        self.assertNotIn("-c:a", args)

    def test_incompatible_codecs_fall_back_to_re_encode(self):
        for probe in (
            {"video_codec": "vp9", "audio_codec": "aac"},
            {"video_codec": "h264", "audio_codec": "opus"},
            {"video_codec": "h264", "audio_codec": "vorbis"},
            {"video_codec": "", "audio_codec": "aac"},
            {},
            None,
        ):
            with self.subTest(probe=probe):
                self.assertIsNone(mp4_remux_args(probe))

    def test_the_target_containers_are_mp4_family(self):
        self.assertIn(".mp4", MP4_REMUX_TARGET_EXTS)
        self.assertIn(".m4v", MP4_REMUX_TARGET_EXTS)
        self.assertIn(".mov", MP4_REMUX_TARGET_EXTS)
        self.assertNotIn(".mkv", MP4_REMUX_TARGET_EXTS)
        self.assertNotIn(".webm", MP4_REMUX_TARGET_EXTS)


class BulkConvertRemuxTests(unittest.TestCase):
    def test_convert_asks_the_worker_to_remux(self):
        self.assertEqual(_resolve_bulk_plan({"bulk_convert_mp4": True})["remux_to"], "mp4")

    def test_an_empty_plan_defaults_to_convert_and_remux(self):
        self.assertEqual(_resolve_bulk_plan({})["remux_to"], "mp4")

    def test_compress_and_optimize_must_re_encode(self):
        self.assertIsNone(_resolve_bulk_plan({"bulk_compress": True})["remux_to"])
        self.assertIsNone(_resolve_bulk_plan({"bulk_optimize": True})["remux_to"])

    def test_extract_and_remove_audio_do_not_remux(self):
        self.assertIsNone(_resolve_bulk_plan({"bulk_extract_audio": True})["remux_to"])
        self.assertIsNone(_resolve_bulk_plan({"bulk_remove_audio": True})["remux_to"])


class ConvertPathsStateAPresetTests(unittest.TestCase):
    """No convert path may omit ``-preset`` and land on libx264's ``medium``."""

    def test_single_file_convert_carries_the_remux_marker(self):
        src = _flat("handlers.py")
        self.assertIn('"remux_to": current_file.get("_pipeline_remux_to")', src)
        self.assertIn('current_file["_pipeline_remux_to"] = _remux_to', src)

    def test_the_worker_remuxes_before_re_encoding(self):
        src = _flat("workers/ffmpeg_worker.py")
        self.assertIn("mp4_remux_args(await probe_media(input_path))", src)
        self.assertIn('job.get("remux_to")', src)

    def test_media_converter_states_a_preset_and_remuxes(self):
        src = _flat("media_converter.py")
        self.assertIn('"-preset", "veryfast", "-crf", "23"', src)
        self.assertIn("mp4_remux_args(await probe_media(input_path))", src)

    def test_change_resolution_states_a_preset(self):
        src = _flat("tasks/conversion_tasks.py")
        self.assertIn('"-c:v", "libx264", "-preset", "veryfast", "-crf", "23"', src)

    def test_change_bitrate_states_a_preset(self):
        src = _flat("tasks/conversion_tasks.py")
        self.assertIn('"-c:v", "libx264", "-preset", "veryfast", "-c:a", "aac"', src)

    def test_the_bigfile_pipeline_carries_the_remux_marker(self):
        src = _flat("utils/bigfile_pipeline.py")
        self.assertIn('job["remux_to"] = remux_to', src)


class PipelineThreadingTests(unittest.TestCase):
    """Both pipes and both bulk pools carry ``remux_to`` without breaking."""

    def test_ingest_large_file_accepts_remux_to_and_defaults_off(self):
        import inspect

        from utils.bigfile_pipeline import BigFilePipeline

        params = inspect.signature(BigFilePipeline.ingest_large_file).parameters
        self.assertIn("remux_to", params)
        self.assertIsNone(params["remux_to"].default, "existing callers must be unaffected")

    def test_remux_to_is_an_event_safe_field(self):
        from utils.eventbus import messages

        self.assertIn("remux_to", messages._SAFE_JOB_FIELDS)

    def test_the_worker_only_remuxes_a_real_local_input(self):
        # A probe header is a reference, never a source: the remux gate requires
        # an existing ``input_path``, so a header can never be stream-copied into
        # a tiny "MP4". The worker's header handling clears ``input_key`` and
        # fetches the media instead.
        src = _flat("workers/ffmpeg_worker.py")
        self.assertIn("and input_path and os.path.exists(input_path)", src)
        self.assertIn("input_header_only", src)

    def test_the_convert_handler_pipeline_call_passes_remux_to(self):
        src = _flat("handlers.py")
        self.assertIn('remux_to=current_file.get("_pipeline_remux_to")', src)

    def test_the_merge_convert_path_uses_the_fixed_converter(self):
        # Merge + Convert delegates to ExtendedMediaConverter.convert_video_format,
        # which now states its preset and can remux; pin that delegation.
        src = _flat("handlers.py")
        self.assertIn("await self.converter.convert_video_format(merged_path", src)


if __name__ == "__main__":
    unittest.main()
