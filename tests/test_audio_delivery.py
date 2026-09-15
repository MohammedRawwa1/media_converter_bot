"""Tests for audio extraction delivery: naming, bitrate and streamable audio."""

import ast
import contextlib
import inspect
import os
import re
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from handlers import (
    _BULK_COMPRESS_CRF_DEFAULT,
    _BULK_EXTRACT_BITRATE_DEFAULT,
    _BULK_LIST_LIMIT,
    _BULK_OPTIMIZE_DEFAULT,
    _BULK_OPTIMIZE_PRESETS,
    _audio_delivery_name,
    _bulk_photo_supported,
    _bulk_quality_label,
    _bulk_rename_filename,
    _metadata_caption,
    _normalize_bulk_item,
    _parse_bulk_crf,
    _read_bulk_settings,
    _register_bulk_file,
    _resolve_bulk_plan,
    _sanitize_audio_bitrate,
    _sanitize_bulk_extract_bitrate,
    _write_bulk_setting,
)
from utils import userbot_uploader as mod

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TMP = tempfile.gettempdir()


class AudioBitrateTests(unittest.TestCase):
    def test_accepts_valid_bitrates(self):
        self.assertEqual(_sanitize_audio_bitrate("128k"), "128k")
        self.assertEqual(_sanitize_audio_bitrate("128K"), "128k")
        self.assertEqual(_sanitize_audio_bitrate("128"), "128k")
        self.assertEqual(_sanitize_audio_bitrate("128kbps"), "128k")
        self.assertEqual(_sanitize_audio_bitrate(96), "96k")
        self.assertEqual(_sanitize_audio_bitrate(" 320k "), "320k")

    def test_falls_back_on_invalid_input(self):
        for value in ("high", "", None, "9999k", "16k", "-128k", "1e3"):
            self.assertEqual(_sanitize_audio_bitrate(value), "128k", msg=repr(value))

    def test_custom_default_is_honoured(self):
        self.assertEqual(_sanitize_audio_bitrate("nope", default=""), "")
        self.assertEqual(_sanitize_audio_bitrate(192, default=""), "192k")


class AudioDeliveryNameTests(unittest.TestCase):
    def test_preserves_original_name_and_swaps_extension(self):
        self.assertEqual(_audio_delivery_name("My Video.mp4", 1), "My Video.mp3")
        self.assertEqual(_audio_delivery_name("my.song.flac", 2), "my.song.mp3")
        self.assertEqual(_audio_delivery_name("dir/My Video.mkv"), "My Video.mp3")

    def test_falls_back_when_the_name_is_missing(self):
        self.assertEqual(_audio_delivery_name("", 42), "audio_42.mp3")
        self.assertEqual(_audio_delivery_name(None, None), "audio.mp3")

    def test_custom_extension_is_used(self):
        self.assertEqual(_audio_delivery_name("Track.wav", 3, extension=".m4a"), "Track.m4a")


class AudioDetectionTests(unittest.TestCase):
    def test_metadata_caption_prefers_source_tags(self):
        current_file = {"name": "My Video.mp4", "_source_metadata": {"title": "My Song", "performer": "Some Artist"}}
        self.assertEqual(_metadata_caption(current_file, "✅ Audio extracted"), "My Song — Some Artist")

    def test_metadata_caption_accepts_artists_alias(self):
        current_file = {"name": "My Video.mp4", "_source_metadata": {"title": "My Song", "artists": "Some Artist"}}
        self.assertEqual(_metadata_caption(current_file, "✅ Audio extracted"), "My Song — Some Artist")

    def test_metadata_caption_accepts_author_alias(self):
        current_file = {"name": "My Video.mp4", "_source_metadata": {"title": "My Song", "author": "Some Artist"}}
        self.assertEqual(_metadata_caption(current_file, "✅ Audio extracted"), "My Song — Some Artist")

    def test_audio_extensions_are_detected(self):
        for name in ("a.mp3", "a.M4A", "a.flac", "a.opus", "a.ogg"):
            self.assertTrue(mod.is_audio_delivery_output(os.path.join(TMP, name)), msg=name)

    def test_video_and_archive_extensions_are_not_audio(self):
        for name in ("a.mp4", "a.mkv", "a.zip", "a.srt"):
            self.assertFalse(mod.is_audio_delivery_output(os.path.join(TMP, name)), msg=name)

    def test_media_kind_from_job_metadata_wins(self):
        self.assertTrue(mod.is_audio_delivery_output(os.path.join(TMP, "a.bin"), "audio"))
        self.assertFalse(mod.is_audio_delivery_output(os.path.join(TMP, "a.mp3"), "video"))

    def test_audio_metadata_defaults(self):
        self.assertEqual(mod._audio_mime_type("song.mp3"), "audio/mpeg")
        self.assertEqual(mod._audio_mime_type("song.unknown"), "audio/mpeg")
        self.assertEqual(mod._audio_title(os.path.join(TMP, "My Song.mp3")), "My Song")


class BulkPlanTests(unittest.TestCase):
    """Bulk Apply must honor every toggle from the bulk menu."""

    def test_no_toggles_keeps_the_historic_mp4_default(self):
        plan = _resolve_bulk_plan({})
        self.assertEqual(plan["output_ext"], ".mp4")
        self.assertEqual(plan["applied"], ["bulk_convert_mp4"])
        self.assertIn("libx264", plan["ffmpeg_args"])

    def test_each_toggle_produces_the_matching_recipe(self):
        compress = _resolve_bulk_plan({"bulk_compress": True})
        self.assertIn("medium", compress["ffmpeg_args"])
        self.assertEqual(compress["applied"], ["bulk_compress"])

        optimize = _resolve_bulk_plan({"bulk_optimize": True})
        self.assertIn("slow", optimize["ffmpeg_args"])
        self.assertEqual(optimize["applied"], ["bulk_optimize"])

        convert = _resolve_bulk_plan({"bulk_convert_mp4": True})
        self.assertIn("+faststart", convert["ffmpeg_args"])
        self.assertEqual(convert["applied"], ["bulk_convert_mp4"])

        extract = _resolve_bulk_plan({"bulk_extract_audio": True})
        self.assertEqual(extract["output_ext"], ".mp3")
        self.assertEqual(extract["convert_type"], "extract_audio")
        self.assertIn("libmp3lame", extract["ffmpeg_args"])

    def test_remove_audio_drops_audio_args_and_reports_it(self):
        plan = _resolve_bulk_plan({"bulk_remove_audio": True})
        self.assertEqual(plan["ffmpeg_args"], ["-an", "-c:v", "copy"])
        self.assertEqual(plan["applied"], ["bulk_remove_audio"])

        combined = _resolve_bulk_plan({"bulk_compress": True, "bulk_remove_audio": True})
        self.assertIn("-an", combined["ffmpeg_args"])
        # The audio encoding args must be gone, not just overridden by ordering.
        self.assertNotIn("-c:a", combined["ffmpeg_args"])
        self.assertEqual(combined["applied"], ["bulk_compress", "bulk_remove_audio"])

    def test_conflicting_video_toggles_are_reported_as_skipped(self):
        plan = _resolve_bulk_plan({"bulk_compress": True, "bulk_optimize": True, "bulk_convert_mp4": True})
        self.assertEqual(plan["applied"], ["bulk_compress"])
        self.assertEqual(plan["ignored"], ["bulk_optimize", "bulk_convert_mp4"])

    def test_extract_audio_wins_and_rename_is_kept(self):
        plan = _resolve_bulk_plan(
            {"bulk_extract_audio": True, "bulk_compress": True, "bulk_remove_audio": True, "bulk_rename": True}
        )
        self.assertEqual(plan["output_ext"], ".mp3")
        self.assertIn("bulk_rename", plan["applied"])
        self.assertTrue(plan["rename"])
        self.assertEqual(plan["ignored"], ["bulk_compress", "bulk_remove_audio"])


class BulkQualityTests(unittest.TestCase):
    """Compress CRF and Optimize preset are user picks, not fixed defaults."""

    def _args(self, plan):
        return list(plan["ffmpeg_args"])

    def test_compress_defaults_match_the_single_file_menu(self):
        plan = _resolve_bulk_plan({"bulk_compress": True})
        self.assertIn("medium", plan["ffmpeg_args"])
        self.assertIn("28", plan["ffmpeg_args"])
        self.assertEqual(plan["crf"], 28)

    def test_compress_honors_the_chosen_crf(self):
        for crf in (18, 23, 35, 51):
            plan = _resolve_bulk_plan({"bulk_compress": True, "bulk_crf": crf})
            args = self._args(plan)
            self.assertEqual(args[args.index("-crf") + 1], str(crf))
            self.assertEqual(plan["crf"], crf)

    def test_unusable_crf_falls_back_to_the_default(self):
        for bad in ("nope", "", None, 999, 0, -5, True, [], "28; rm -rf /"):
            plan = _resolve_bulk_plan({"bulk_compress": True, "bulk_crf": bad})
            args = self._args(plan)
            self.assertEqual(args[args.index("-crf") + 1], "28", msg=repr(bad))
            self.assertEqual(plan["crf"], 28, msg=repr(bad))

    def test_optimize_defaults_to_web(self):
        plan = _resolve_bulk_plan({"bulk_optimize": True})
        self.assertEqual(plan["optimize_preset"], "web")
        args = self._args(plan)
        self.assertEqual(args[args.index("-preset") + 1], "slow")
        self.assertEqual(args[args.index("-crf") + 1], "23")
        self.assertEqual(args[args.index("-b:a") + 1], "128k")

    def test_optimize_honors_the_chosen_preset(self):
        cases = {
            "web": ("slow", "23", "128k"),
            "mobile": ("medium", "28", "96k"),
            "tv": ("slow", "20", "192k"),
            "storage": ("veryfast", "35", "64k"),
        }
        for preset, (encoder, crf, bitrate) in cases.items():
            plan = _resolve_bulk_plan({"bulk_optimize": True, "bulk_optimize_preset": preset})
            args = self._args(plan)
            self.assertEqual(plan["optimize_preset"], preset)
            self.assertEqual(args[args.index("-preset") + 1], encoder, msg=preset)
            self.assertEqual(args[args.index("-crf") + 1], crf, msg=preset)
            self.assertEqual(args[args.index("-b:a") + 1], bitrate, msg=preset)

    def test_unknown_preset_falls_back_to_web(self):
        for bad in ("ultra", "", None, 7, "WEB ", "slow"):
            plan = _resolve_bulk_plan({"bulk_optimize": True, "bulk_optimize_preset": bad})
            self.assertTrue(plan["optimize_preset"] in ("web", "storage"), msg=repr(bad))
        # A preset name that is not one of ours (even if it looks like an encoder) is rejected.
        self.assertEqual(
            _resolve_bulk_plan({"bulk_optimize": True, "bulk_optimize_preset": "fast"})["optimize_preset"],
            "web",
        )

    def test_quality_is_not_applied_to_other_toggles(self):
        convert = _resolve_bulk_plan({"bulk_convert_mp4": True, "bulk_crf": 18, "bulk_optimize_preset": "tv"})
        self.assertNotIn("18", convert["ffmpeg_args"])
        self.assertEqual(convert["applied"], ["bulk_convert_mp4"])

    def test_quality_still_works_with_remove_audio(self):
        plan = _resolve_bulk_plan({"bulk_compress": True, "bulk_crf": 18, "bulk_remove_audio": True})
        args = self._args(plan)
        self.assertEqual(args[args.index("-crf") + 1], "18")
        self.assertIn("-an", args)
        self.assertNotIn("-c:a", args)

    def test_summary_names_the_quality(self):
        self.assertEqual(_bulk_quality_label(_resolve_bulk_plan({"bulk_compress": True, "bulk_crf": 23})), "CRF 23")
        self.assertEqual(
            _bulk_quality_label(_resolve_bulk_plan({"bulk_optimize": True, "bulk_optimize_preset": "tv"})),
            "Optimize preset: tv",
        )
        self.assertEqual(_bulk_quality_label(_resolve_bulk_plan({"bulk_convert_mp4": True})), "")
        self.assertEqual(_bulk_quality_label(_resolve_bulk_plan({"bulk_extract_audio": True})), "MP3 128k")
        self.assertEqual(
            _bulk_quality_label(_resolve_bulk_plan({"bulk_extract_audio": True, "bulk_extract_bitrate": "320k"})),
            "MP3 320k",
        )

    def test_extract_bitrate_defaults_to_128k(self):
        plan = _resolve_bulk_plan({"bulk_extract_audio": True})
        args = self._args(plan)
        self.assertEqual(args[args.index("-ab") + 1], "128k")
        self.assertEqual(plan["extract_bitrate"], "128k")
        self.assertEqual(plan["output_ext"], ".mp3")

    def test_extract_bitrate_honors_the_chosen_value(self):
        for chosen, expected in (("64k", "64k"), ("96k", "96k"), ("192k", "192k"), ("320k", "320k"), (192, "192k"), (" 128K ", "128k"), ("128kbps", "128k")):
            plan = _resolve_bulk_plan({"bulk_extract_audio": True, "bulk_extract_bitrate": chosen})
            args = self._args(plan)
            self.assertEqual(args[args.index("-ab") + 1], expected, msg=repr(chosen))
            self.assertEqual(plan["extract_bitrate"], expected, msg=repr(chosen))

    def test_unusable_extract_bitrate_falls_back_to_128k(self):
        for bad in ("high", "", None, "9999k", "16k", "-128k", "1e3", True, [], "128k; rm -rf /"):
            plan = _resolve_bulk_plan({"bulk_extract_audio": True, "bulk_extract_bitrate": bad})
            args = self._args(plan)
            self.assertEqual(args[args.index("-ab") + 1], "128k", msg=repr(bad))
            self.assertEqual(plan["extract_bitrate"], "128k", msg=repr(bad))

    def test_extract_bitrate_is_not_applied_to_video_toggles(self):
        compress = _resolve_bulk_plan({"bulk_compress": True, "bulk_extract_bitrate": "320k"})
        self.assertNotIn("320k", compress["ffmpeg_args"])
        # The audio args keep their own fixed bitrate when Extract Audio is off.
        self.assertEqual(compress["ffmpeg_args"][compress["ffmpeg_args"].index("-b:a") + 1], "128k")

    def test_extract_bitrate_survives_rename(self):
        plan = _resolve_bulk_plan(
            {"bulk_extract_audio": True, "bulk_rename": True, "bulk_extract_bitrate": "96k"}
        )
        args = self._args(plan)
        self.assertEqual(args[args.index("-ab") + 1], "96k")
        self.assertTrue(plan["rename"])
        self.assertIn("bulk_rename", plan["applied"])

    def test_handlers_extract_bitrate_default_agrees_with_the_menu(self):
        from utils import callbacks

        self.assertEqual(_BULK_EXTRACT_BITRATE_DEFAULT, callbacks.BULK_BITRATE_DEFAULT)
        for choice in callbacks.MP3_QUALITY_CHOICES:
            self.assertEqual(_sanitize_bulk_extract_bitrate(choice), choice)

    def test_custom_crf_input_is_validated(self):
        self.assertEqual(_parse_bulk_crf("23"), 23)
        self.assertEqual(_parse_bulk_crf(" 18 "), 18)
        for bad in ("17", "52", "abc", "", None, True, "1.5"):
            self.assertIsNone(_parse_bulk_crf(bad), msg=repr(bad))

    def test_handlers_config_agrees_with_the_menu_constants(self):
        from utils import callbacks

        self.assertEqual(_BULK_COMPRESS_CRF_DEFAULT, callbacks.BULK_CRF_DEFAULT)
        self.assertEqual(set(_BULK_OPTIMIZE_PRESETS), set(callbacks.BULK_PRESET_CHOICES))
        self.assertEqual(_BULK_OPTIMIZE_DEFAULT, callbacks.BULK_PRESET_DEFAULT)


class BulkQualityMenuTests(unittest.TestCase):
    """The bulk menu must expose the quality picks and show the active values."""

    def _buttons(self, kb):
        return {b.callback_data: b.text for row in kb.inline_keyboard for b in row}

    def test_bulk_menu_has_the_quality_row(self):
        from utils.callbacks import BULK_CRF_MENU, BULK_PRESET_MENU
        from utils.keyboard_utils import MediaMenuBuilder

        rendered = self._buttons(MediaMenuBuilder.get_bulk_menu({"bulk_crf": 23, "bulk_optimize_preset": "tv"}))
        self.assertIn(BULK_CRF_MENU, rendered)
        self.assertIn(BULK_PRESET_MENU, rendered)
        self.assertIn("23", rendered[BULK_CRF_MENU])
        self.assertIn("TV", rendered[BULK_PRESET_MENU])

    def test_bulk_menu_shows_defaults_when_unset(self):
        from utils.callbacks import BULK_CRF_MENU, BULK_PRESET_MENU
        from utils.keyboard_utils import MediaMenuBuilder

        rendered = self._buttons(MediaMenuBuilder.get_bulk_menu({}))
        self.assertIn("28", rendered[BULK_CRF_MENU])
        self.assertIn("Web", rendered[BULK_PRESET_MENU])

    def test_bulk_menu_tolerates_a_corrupt_stored_crf(self):
        from utils.callbacks import BULK_CRF_MENU
        from utils.keyboard_utils import MediaMenuBuilder

        kb = MediaMenuBuilder.get_bulk_menu({"bulk_crf": "not-a-number"})
        self.assertIn("28", self._buttons(kb)[BULK_CRF_MENU])

    def test_crf_menu_marks_the_active_choice(self):
        from utils.callbacks import bulk_crf_key
        from utils.keyboard_utils import MediaMenuBuilder

        rendered = self._buttons(MediaMenuBuilder.get_bulk_crf_menu(23))
        self.assertTrue(rendered[bulk_crf_key(23)].startswith("✅"), rendered[bulk_crf_key(23)])
        self.assertFalse(rendered[bulk_crf_key(18)].startswith("✅"))
        self.assertIn(bulk_crf_key("custom"), rendered)

    def test_preset_menu_marks_the_active_choice(self):
        from utils.callbacks import bulk_preset_key
        from utils.keyboard_utils import MediaMenuBuilder

        rendered = self._buttons(MediaMenuBuilder.get_bulk_preset_menu("tv"))
        self.assertTrue(rendered[bulk_preset_key("tv")].startswith("✅"), rendered[bulk_preset_key("tv")])
        self.assertFalse(rendered[bulk_preset_key("web")].startswith("✅"))

    def test_bulk_menu_has_the_extract_bitrate_row(self):
        from utils.callbacks import BULK_BITRATE_MENU
        from utils.keyboard_utils import MediaMenuBuilder

        rendered = self._buttons(MediaMenuBuilder.get_bulk_menu({"bulk_extract_bitrate": "320k"}))
        self.assertIn(BULK_BITRATE_MENU, rendered)
        self.assertIn("320k", rendered[BULK_BITRATE_MENU])

    def test_bulk_menu_shows_the_default_bitrate_when_unset(self):
        from utils.callbacks import BULK_BITRATE_MENU
        from utils.keyboard_utils import MediaMenuBuilder

        kb = MediaMenuBuilder.get_bulk_menu({})
        self.assertIn("128k", self._buttons(kb)[BULK_BITRATE_MENU])

    def test_bitrate_menu_marks_the_active_choice(self):
        from utils.callbacks import MP3_QUALITY_CHOICES, bulk_bitrate_key
        from utils.keyboard_utils import MediaMenuBuilder

        rendered = self._buttons(MediaMenuBuilder.get_bulk_bitrate_menu("192k"))
        self.assertTrue(rendered[bulk_bitrate_key("192k")].startswith("✅"), rendered[bulk_bitrate_key("192k")])
        self.assertFalse(rendered[bulk_bitrate_key("128k")].startswith("✅"))
        self.assertIn(bulk_bitrate_key("custom"), rendered)
        # Every value the single-file MP3 picker offers is offered here too.
        for choice in MP3_QUALITY_CHOICES:
            self.assertIn(bulk_bitrate_key(choice), rendered)

    def test_bitrate_menu_uses_bulk_triggers_not_the_single_file_ones(self):
        """Choosing a bulk bitrate must never start a single-file conversion."""
        from utils.callbacks import MP3_QUALITY_PREFIX
        from utils.keyboard_utils import MediaMenuBuilder

        rendered = self._buttons(MediaMenuBuilder.get_bulk_bitrate_menu("128k"))
        self.assertTrue(rendered, "expected the bitrate picker to render buttons")
        for callback_data in rendered:
            if callback_data == "bulk_menu":
                continue
            self.assertTrue(callback_data.startswith("bulk_set_bitrate:"), callback_data)
            self.assertFalse(callback_data.startswith(MP3_QUALITY_PREFIX))


class BulkSettingsStoreTests(unittest.TestCase):
    """The quality picks are persisted next to the toggles so Apply sees them."""

    class StubStore:
        def __init__(self):
            self.data = {}

        def get_user_settings(self, user_id):
            return dict(self.data.get(user_id, {}))

        def set_user_setting(self, user_id, key, value):
            self.data.setdefault(user_id, {})[key] = value

    def test_round_trips_through_the_settings_store(self):
        import handlers as handlers_module

        store = self.StubStore()
        with patch.object(handlers_module, "user_settings", store):
            _write_bulk_setting(7, None, "bulk_crf", 18)
            _write_bulk_setting(7, None, "bulk_optimize_preset", "storage")
            _write_bulk_setting(7, None, "bulk_extract_bitrate", "192k")
            self.assertEqual(_read_bulk_settings(7, None)["bulk_crf"], 18)
            self.assertEqual(_read_bulk_settings(7, None)["bulk_optimize_preset"], "storage")
            self.assertEqual(_read_bulk_settings(7, None)["bulk_extract_bitrate"], "192k")
        self.assertEqual(store.data[7]["bulk_crf"], 18)

    def test_round_trips_through_the_session_when_there_is_no_store(self):
        import handlers as handlers_module

        session = {}
        with patch.object(handlers_module, "user_settings", None):
            _write_bulk_setting(7, session, "bulk_crf", 35)
            self.assertEqual(_read_bulk_settings(7, session)["bulk_crf"], 35)
        self.assertEqual(session["bulk_settings"]["bulk_crf"], 35)

    def test_read_failure_never_raises(self):
        import handlers as handlers_module

        class Boom:
            def get_user_settings(self, user_id):
                raise RuntimeError("store down")

        with patch.object(handlers_module, "user_settings", Boom()):
            self.assertEqual(_read_bulk_settings(7, None), {})

    def test_apply_summary_reports_the_quality(self):
        with open(os.path.join(PROJECT_ROOT, "handlers.py"), encoding="utf-8") as fh:
            src = fh.read()
        self.assertIn("_bulk_quality_label(_plan)", src)
        self.assertIn("_read_bulk_settings(user_id, sess)", src)

    def test_delivery_caption_uses_the_chosen_bitrate(self):
        """The message the user receives must not keep claiming 128k."""
        with open(os.path.join(PROJECT_ROOT, "handlers.py"), encoding="utf-8") as fh:
            src = fh.read()
        self.assertIn("f\"✅ Audio extracted ({_plan['extract_bitrate']})\"", src)
        self.assertNotIn('f"✅ Audio extracted ({_DEFAULT_AUDIO_BITRATE})"', src)


class BulkCollectTests(unittest.TestCase):
    """Sent files are collected for the next Apply Bulk, without duplicates."""

    def test_register_appends_the_file_once(self):
        session = {}
        f = {"id": "abc", "name": "a.mp4", "path": None}
        self.assertTrue(_register_bulk_file(session, f))
        self.assertFalse(_register_bulk_file(session, f))
        self.assertEqual(session["bulk_list"], [f])

    def test_register_keeps_distinct_files_in_send_order(self):
        session = {}
        for i in range(3):
            _register_bulk_file(session, {"id": f"id{i}", "name": f"f{i}.mp4"})
        self.assertEqual([x["id"] for x in session["bulk_list"]], ["id0", "id1", "id2"])

    def test_register_rejects_junk_and_caps_the_queue(self):
        session = {}
        self.assertFalse(_register_bulk_file(session, None))
        self.assertFalse(_register_bulk_file(session, {"name": "no id"}))
        for i in range(_BULK_LIST_LIMIT + 5):
            _register_bulk_file(session, {"id": f"id{i}"})
        self.assertEqual(len(session["bulk_list"]), _BULK_LIST_LIMIT)
        self.assertEqual(session["bulk_list"][0]["id"], "id5")

    def test_normalize_accepts_dicts_and_bare_paths(self):
        entry = {"id": "x", "path": os.path.join("tmp", "x.mp4")}
        self.assertIs(_normalize_bulk_item(entry), entry)
        normalized = _normalize_bulk_item(os.path.join("tmp", "song.mp3"))
        self.assertEqual(normalized["path"], os.path.join("tmp", "song.mp3"))
        self.assertEqual(normalized["name"], "song.mp3")
        for junk in (None, "", 0):
            self.assertIsNone(_normalize_bulk_item(junk))

    def test_bulk_apply_reads_the_collected_list(self):
        with open(os.path.join(PROJECT_ROOT, "handlers.py"), encoding="utf-8") as fh:
            src = fh.read()
        self.assertIn('sess.get("bulk_list") or sess.get("merge_list")', src)
        self.assertIn("_ensure_bulk_file_downloaded(", src)


class AlbumCollectTests(unittest.IsolatedAsyncioTestCase):
    """An album (media group) is collected into the batch and announced once.

    Telegram delivers an album as one update per item, so without buffering a
    10-video album produced ten separate "registered - choose an action" menus.
    The items are already registered in the batch by the caller; buffering only
    suppresses the per-file menu and emits a single announcement.
    """

    @staticmethod
    def _handler():
        import handlers as handlers_module

        handler = object.__new__(handlers_module.EnhancedMediaHandler)
        handler.user_sessions = {}
        return handler

    @staticmethod
    def _update(media_group_id, chat_id=7):
        message = SimpleNamespace(
            media_group_id=media_group_id,
            chat=SimpleNamespace(id=chat_id),
        )
        return SimpleNamespace(message=message, effective_user=SimpleNamespace(id=chat_id))

    async def test_standalone_send_is_not_an_album(self):
        handler = self._handler()
        session = {}
        self.assertFalse(
            await handler._buffer_album_item(
                self._update(None), SimpleNamespace(bot=None), session, 7, "video"
            )
        )
        self.assertFalse(session.get("album_batch"))

    async def test_album_items_are_counted_once(self):
        handler = self._handler()
        session = {}
        handler.user_sessions[7] = session
        context = SimpleNamespace(bot=SimpleNamespace())

        for _ in range(3):
            self.assertTrue(
                await handler._buffer_album_item(self._update("g1"), context, session, 7, "video")
            )

        entry = session["album_batch"]["g1"]
        self.assertEqual(entry["count"], 3)
        self.assertEqual(entry["kinds"], {"video"})
        self.assertEqual(len(session["album_batch_timers"]), 1)

    async def test_album_is_announced_once_with_the_batch_size(self):
        handler = self._handler()
        session = {"bulk_list": [{"id": "a"}, {"id": "b"}, {"id": "c"}]}
        handler.user_sessions[7] = session
        handler._persist_session = lambda _uid: None
        sent = []

        class FakeBot:
            async def send_message(self, **kwargs):
                sent.append(kwargs)

        context = SimpleNamespace(bot=FakeBot())
        for _ in range(3):
            await handler._buffer_album_item(self._update("g1"), context, session, 7, "video")

        await handler._flush_album_batch(7, "g1")
        await handler._flush_album_batch(7, "g1")  # a second flush is a no-op

        self.assertEqual(len(sent), 1)
        self.assertIn("3 video", sent[0]["text"])
        self.assertIn("Batch size: 3", sent[0]["text"])
        self.assertEqual(sent[0]["chat_id"], 7)

    def test_every_media_handler_registers_before_buffering(self):
        """The album is only a *view* of the batch; registration must come first."""
        with open(os.path.join(PROJECT_ROOT, "handlers.py"), encoding="utf-8") as fh:
            src = fh.read()
        self.assertEqual(src.count('_register_bulk_file(session, session["current_file"])'), 3)
        self.assertEqual(src.count("await self._buffer_album_item(update, context, session, user_id"), 4)

    def test_photos_join_the_batch_too(self):
        """Photos are queued like video/audio/document so one Apply covers all."""
        with open(os.path.join(PROJECT_ROOT, "handlers.py"), encoding="utf-8") as fh:
            src = fh.read()
        self.assertIn("_register_bulk_file(session, _photo_entry)", src)
        self.assertIn('"type": "photo",', src)


class PhotoBatchTests(unittest.TestCase):
    """A photo can only run a plan that actually encodes video."""

    def test_photo_runs_the_video_encodes(self):
        for settings in (
            {},
            {"bulk_convert_mp4": True},
            {"bulk_compress": True},
            {"bulk_optimize": True},
            {"bulk_rename": True},
            {"bulk_convert_mp4": True, "bulk_remove_audio": True},
        ):
            plan = _resolve_bulk_plan(settings)
            self.assertTrue(_bulk_photo_supported(plan), msg=repr(settings))

    def test_photo_is_skipped_for_audio_only_plans(self):
        for settings in (
            {"bulk_extract_audio": True},
            {"bulk_remove_audio": True},
            {"bulk_remove_audio": True, "bulk_rename": True},
        ):
            plan = _resolve_bulk_plan(settings)
            self.assertFalse(_bulk_photo_supported(plan), msg=repr(settings))
        self.assertFalse(_bulk_photo_supported(None))

    def test_apply_loop_guards_photos(self):
        with open(os.path.join(PROJECT_ROOT, "handlers.py"), encoding="utf-8") as fh:
            src = fh.read()
        self.assertIn('if f.get("type") == "photo" and not _photo_ok:', src)
        self.assertIn("Skipped {photo_skipped} photo(s)", src)


class BulkRenameTests(unittest.TestCase):
    def test_applies_prefix_suffix_and_words_to_remove(self):
        settings = {"prefix": "[Bot] ", "suffix": " HD", "words_remove": ["1080p", "x264"]}
        name, changed = _bulk_rename_filename("Movie 1080p x264.mp4", settings)
        self.assertEqual(name, "[Bot] Movie HD.mp4")
        self.assertTrue(changed)

    def test_keeps_extension_and_name_when_nothing_is_configured(self):
        name, changed = _bulk_rename_filename("song.mp3", {})
        self.assertEqual(name, "song.mp3")
        self.assertFalse(changed)

    def test_never_renames_to_an_empty_stem(self):
        name, _ = _bulk_rename_filename("1080p.mp4", {"words_remove": ["1080p"]})
        self.assertTrue(name.endswith(".mp4"))
        self.assertGreater(len(os.path.splitext(name)[0]), 0)

    def test_handles_missing_name(self):
        self.assertEqual(_bulk_rename_filename(None, {"prefix": "p_"}), ("", False))


class _FakePyrogramClient:
    """Stand-in for pyrogram.Client that records outgoing SendMedia requests."""

    def __init__(self):
        self.requests = []

    async def resolve_peer(self, target):
        return target

    @staticmethod
    def rnd_id():
        return 1

    def guess_mime_type(self, _name):
        return "audio/mpeg"

    async def invoke(self, request):
        self.requests.append(request)
        return SimpleNamespace(updates=[], users=[], chats=[])


class AudioRawSendTests(unittest.IsolatedAsyncioTestCase):
    async def test_audio_send_uses_document_attribute_audio(self):
        client = _FakePyrogramClient()
        uploaded = SimpleNamespace()  # stands in for raw InputFileBig

        with patch.object(mod, "PyrogramClient", object()):
            result = await mod._send_audio_raw(
                client,
                12345,
                os.path.join(TMP, "My Video.mp3"),
                uploaded,
                caption="✅ Audio extracted (128k)",
                audio_meta={"duration": 212, "performer": "Some Artist"},
                delivery_name="My Video.mp3",
            )

        # The fake returns no updates, so the helper reports "not sent" — we only
        # care that the media it built carries the audio attributes here.
        self.assertIsNone(result)
        media = client.requests[0].media
        attributes = {type(attr).__name__: attr for attr in media.attributes}

        audio = attributes["DocumentAttributeAudio"]
        self.assertEqual(audio.duration, 212)
        self.assertEqual(audio.title, "My Video")
        self.assertEqual(audio.performer, "Some Artist")
        self.assertFalse(audio.voice)
        self.assertEqual(attributes["DocumentAttributeFilename"].file_name, "My Video.mp3")
        self.assertEqual(media.mime_type, "audio/mpeg")

    async def test_audio_send_defaults_title_to_the_delivery_name(self):
        client = _FakePyrogramClient()

        with patch.object(mod, "PyrogramClient", object()):
            await mod._send_audio_raw(
                client,
                12345,
                os.path.join(TMP, "track.mp3"),
                SimpleNamespace(),
                audio_meta={},
                delivery_name="Original Name.mp3",
            )

        attributes = {type(attr).__name__: attr for attr in client.requests[0].media.attributes}
        self.assertEqual(attributes["DocumentAttributeAudio"].title, "Original Name")
        self.assertEqual(attributes["DocumentAttributeAudio"].duration, 0)
        self.assertEqual(attributes["DocumentAttributeFilename"].file_name, "Original Name.mp3")


class Mp3QualityMenuTests(unittest.TestCase):
    def test_menu_exposes_every_quality_and_custom_bitrate(self):
        from utils.keyboard_utils import MediaMenuBuilder

        kb = MediaMenuBuilder.get_mp3_quality_menu("128k")
        buttons = {b.callback_data: b.text for row in kb.inline_keyboard for b in row}

        for choice in ("64k", "96k", "128k", "192k", "256k", "320k"):
            self.assertIn(f"mp3q_{choice}", buttons)
        self.assertIn("mp3q_custom", buttons)

    def test_current_quality_is_marked(self):
        from utils.keyboard_utils import MediaMenuBuilder

        kb = MediaMenuBuilder.get_mp3_quality_menu("192k")
        buttons = {b.callback_data: b.text for row in kb.inline_keyboard for b in row}

        self.assertTrue(buttons["mp3q_192k"].startswith("✅"))
        self.assertEqual(buttons["mp3q_128k"], "128k")


class TelegramSendCallSitesTests(unittest.TestCase):
    """Every Bot API send/reply call must use keywords PTB actually accepts.

    Unsupported keywords raise TypeError at runtime, and those TypeErrors were
    swallowed by broad ``except`` blocks that fell back to a plain
    ``send_document`` — which is how streamable audio silently became a
    downloadable file.
    """

    FILES = ("handlers.py", "main.py", os.path.join("workers", "ffmpeg_worker.py"), os.path.join("web", "webapp.py"))

    @classmethod
    def setUpClass(cls):
        import telegram

        cls.methods = {}
        for cls_ in (telegram.Bot, telegram.Message):
            for name, fn in vars(cls_).items():
                if not (name.startswith(("send_", "reply_")) and callable(fn)):
                    continue
                with contextlib.suppress(TypeError, ValueError):
                    cls.methods[name] = set(inspect.signature(fn).parameters)

    def test_send_calls_only_use_supported_keywords(self):
        problems = []
        checked = 0
        for relative_path in self.FILES:
            path = os.path.join(PROJECT_ROOT, relative_path)
            if not os.path.exists(path):
                continue
            with open(path, encoding="utf-8") as fh:
                tree = ast.parse(fh.read(), filename=path)
            for node in ast.walk(tree):
                if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
                    continue
                accepted = self.methods.get(node.func.attr)
                if accepted is None:
                    continue
                checked += 1
                for keyword in node.keywords:
                    if keyword.arg and keyword.arg not in accepted:
                        problems.append(
                            f"{relative_path}:{node.lineno}: {node.func.attr}() -> unexpected kwarg {keyword.arg!r}"
                        )
        self.assertGreater(checked, 50, "expected to scan many Bot API call sites")
        self.assertEqual(problems, [], "\n".join(problems))


class FilenameFromUrlTests(unittest.TestCase):
    """URL jobs must be delivered under a readable name, not a job id."""

    def test_uses_last_path_segment(self):
        from utils.file_utils import filename_from_url

        self.assertEqual(
            filename_from_url("https://cdn.example.com/videos/My Holiday Clip.mp4?token=abc"),
            "My Holiday Clip.mp4",
        )
        self.assertEqual(filename_from_url("https://cdn.example.com/a/b/Song%20Name.mp3"), "Song Name.mp3")

    def test_query_and_fragment_are_ignored(self):
        from utils.file_utils import filename_from_url

        self.assertEqual(filename_from_url("https://host/video.mp4#t=10"), "video.mp4")
        self.assertEqual(filename_from_url("https://host/video.mp4?a=1&b=2"), "video.mp4")

    def test_unknown_extension_is_dropped_not_kept(self):
        from utils.file_utils import filename_from_url

        self.assertEqual(filename_from_url("https://host/myclip.php?file=a.mp4"), "myclip.mp4")
        self.assertEqual(filename_from_url("https://host/clip"), "clip.mp4")

    def test_non_media_extension_is_normalised(self):
        from utils.file_utils import filename_from_url

        self.assertEqual(filename_from_url("https://host/clip.webm"), "clip.webm")
        self.assertEqual(filename_from_url("https://host/clip.MP4"), "clip.mp4")

    def test_nameless_urls_get_a_readable_fallback(self):
        from utils.file_utils import filename_from_url

        for url in ("https://host/", "https://host", "https://host/download?id=123", "", None):
            name = filename_from_url(url)
            self.assertTrue(name.startswith("media_"), msg=repr(url))
            self.assertTrue(name.endswith(".mp4"), msg=repr(url))

    def test_encoded_traversal_can_never_escape_the_basename(self):
        from utils.file_utils import filename_from_url

        name = filename_from_url("https://host/..%2F..%2Fetc%2Fpasswd")
        self.assertNotIn("/", name)
        self.assertNotIn("\\", name)
        self.assertEqual(name, "passwd.mp4")


class JobNameAuditTests(unittest.TestCase):
    """Every job payload built in handlers.py must carry the source media name.

    The worker derives the delivered filename from ``original_filename``, so a job
    dict without it is delivered as an opaque ``{job_id}_...`` path. This walked
    into four real regressions (URL jobs, extraction archive, format conversion,
    archive creation), so the rule is enforced rather than reviewed.
    """

    def _job_dicts(self, filename):
        with open(os.path.join(PROJECT_ROOT, filename), encoding="utf-8") as fh:
            tree = ast.parse(fh.read(), filename=filename)

        def has(node, key):
            return any(isinstance(k, ast.Constant) and k.value == key for k in node.keys)

        return [node for node in ast.walk(tree) if isinstance(node, ast.Dict) and has(node, "job_id")], has

    def test_handlers_job_dicts_all_have_original_filename(self):
        dicts, has = self._job_dicts("handlers.py")
        self.assertGreaterEqual(len(dicts), 10, "expected to find the job payloads in handlers.py")
        missing = [node.lineno for node in dicts if not has(node, "original_filename")]
        self.assertEqual(missing, [], f"job dicts without original_filename at lines {missing}")

    def test_url_jobs_derive_the_name_from_the_url(self):
        with open(os.path.join(PROJECT_ROOT, "handlers.py"), encoding="utf-8") as fh:
            src = fh.read()
        # Both URL entry points: /bulk_url and the pasted-URL handler.
        self.assertGreaterEqual(src.count('"original_filename": filename_from_url(url)'), 2)

    def test_extraction_archive_names_the_zip(self):
        with open(os.path.join(PROJECT_ROOT, "handlers.py"), encoding="utf-8") as fh:
            src = fh.read()
        # extract_streams delivers ``archive_path``, not ``output_path``, so the
        # archive name has to be carried explicitly.
        self.assertIn('"output_filename": _streams_name,', src)


class StoredJobNamingTests(unittest.TestCase):
    """A requeue rebuilds the payload from the hash, so the name must come from there."""

    def test_extracts_both_naming_fields(self):
        from utils.job_queue import stored_job_naming

        naming = stored_job_naming(
            {"original_filename": "My Holiday Clip.mp4", "output_filename": "My Holiday Clip.mp3"}
        )
        self.assertEqual(
            naming,
            {"original_filename": "My Holiday Clip.mp4", "output_filename": "My Holiday Clip.mp3"},
        )

    def test_survives_bytes_and_empty_values(self):
        """A client without decode_responses returns bytes keys *and* values."""
        from utils.job_queue import stored_job_naming

        naming = stored_job_naming(
            {
                b"original_filename": b"Clip.mp4",
                b"output_filename": b"",
                "unrelated": "x",
            }
        )
        self.assertEqual(naming, {"original_filename": "Clip.mp4"})

        # str keys with bytes values (the common decode_responses=True case).
        self.assertEqual(
            stored_job_naming({"original_filename": b"Clip2.mp4"}),
            {"original_filename": "Clip2.mp4"},
        )

    def test_reads_the_legacy_original_name_alias(self):
        from utils.job_queue import stored_job_naming

        self.assertEqual(stored_job_naming({"original_name": "Legacy.mp4"}), {"original_filename": "Legacy.mp4"})
        # The canonical field wins when both are present.
        self.assertEqual(
            stored_job_naming({"original_filename": "New.mp4", "original_name": "Old.mp4"}),
            {"original_filename": "New.mp4"},
        )

    def test_missing_or_junk_hashes_yield_nothing(self):
        from utils.job_queue import stored_job_naming

        for stored in (None, {}, {"status": "queued"}, {"original_filename": ""}, {"original_filename": None}):
            self.assertEqual(stored_job_naming(stored), {}, msg=repr(stored))

    def test_carry_over_fills_an_empty_payload(self):
        from utils.job_queue import carry_over_job_naming

        job = carry_over_job_naming({"job_id": "j1"}, {"original_filename": "Clip.mp4"})
        self.assertEqual(job["original_filename"], "Clip.mp4")

    def test_carry_over_never_replaces_an_explicit_name(self):
        from utils.job_queue import carry_over_job_naming

        job = carry_over_job_naming(
            {"job_id": "j1", "original_filename": "Override.mp4"},
            {"original_filename": "Stored.mp4"},
        )
        self.assertEqual(job["original_filename"], "Override.mp4")

        forced = carry_over_job_naming(
            {"job_id": "j1", "original_filename": "Override.mp4"},
            {"original_filename": "Stored.mp4"},
            overwrite=True,
        )
        self.assertEqual(forced["original_filename"], "Stored.mp4")

    def test_carry_over_returns_the_same_payload(self):
        from utils.job_queue import carry_over_job_naming

        job = {"job_id": "j1"}
        self.assertIs(carry_over_job_naming(job, {"original_filename": "Clip.mp4"}), job)


class EnqueueNamingPersistenceTests(unittest.TestCase):
    """enqueue_job must persist the name without ever erasing a stored one."""

    class FakeRedis:
        def __init__(self, seeded=None):
            self.hashes = dict(seeded or {})
            self.lists = {}

        async def hset(self, key, mapping=None, **kwargs):
            fields = {**dict(mapping or {}), **kwargs}
            self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in fields.items()})
            return len(fields)

        async def expire(self, key, ttl):
            return True

        async def lpush(self, key, value):
            self.lists.setdefault(key, []).insert(0, value)
            return len(self.lists[key])

        async def close(self):
            return None

    def _enqueue(self, job, seeded=None):
        """Run enqueue_job against a fake Redis, with the event bus stubbed out.

        The bus is stubbed on purpose: this is a unit test of the hash mapping, and
        a real emit_event lazily spins up a Kafka producer whose lifecycle spans
        the event loop asyncio.run() tears down.
        """
        import asyncio

        import utils.eventbus as eventbus
        from utils import job_queue

        fake = self.FakeRedis(seeded)

        async def _get_redis():
            return fake

        async def _no_broker(_job):
            return False

        async def _no_event(*_args, **_kwargs):
            return None

        with (
            patch.object(job_queue, "get_redis", _get_redis),
            patch.object(eventbus, "publish_job", _no_broker),
            patch.object(eventbus, "emit_event", _no_event),
        ):
            asyncio.run(job_queue.enqueue_job(job))
        return fake

    def test_worker_restores_them_from_the_hash(self):
        """The worker's fallback must still read a requeue's missing name back."""
        with open(os.path.join(PROJECT_ROOT, "workers", "ffmpeg_worker.py"), encoding="utf-8") as fh:
            src = fh.read()
        self.assertIn('_sval("original_filename")', src)
        self.assertIn('_sval("output_filename")', src)

    def test_persists_a_name_carried_by_the_payload(self):
        fake = self._enqueue({"job_id": "j1", "original_filename": "Clip.mp4", "output_filename": "Clip.mp3"})
        stored = fake.hashes["ffmpeg:job:j1"]
        self.assertEqual(stored["original_filename"], "Clip.mp4")
        self.assertEqual(stored["output_filename"], "Clip.mp3")

    def test_a_nameless_requeue_does_not_erase_the_stored_name(self):
        """Regression: the requeue payload has no name; writing "" destroyed it."""
        seeded = {"ffmpeg:job:j1": {"original_filename": "Clip.mp4", "output_filename": "Clip.mp3"}}
        fake = self._enqueue({"job_id": "j1", "input_key": "uploads/x"}, seeded)
        stored = fake.hashes["ffmpeg:job:j1"]
        self.assertEqual(stored["original_filename"], "Clip.mp4")
        self.assertEqual(stored["output_filename"], "Clip.mp3")

    def test_a_nameless_requeue_does_not_erase_the_stored_output(self):
        """Regression: a requeue payload without output_path must not blank stored output."""
        seeded = {"ffmpeg:job:j1": {"output": "/data/storage/output/original.mp4"}}
        fake = self._enqueue({"job_id": "j1", "input_key": "uploads/x"}, seeded)
        stored = fake.hashes["ffmpeg:job:j1"]
        self.assertEqual(stored["output"], "/data/storage/output/original.mp4")

    def test_legacy_original_name_is_persisted_under_the_canonical_field(self):
        fake = self._enqueue({"job_id": "j1", "original_name": "Legacy.mp4"})
        self.assertEqual(fake.hashes["ffmpeg:job:j1"]["original_filename"], "Legacy.mp4")

    def test_the_owner_is_persisted_so_a_requeue_can_deliver(self):
        """Without chat_id in the hash a requeued job has nobody to deliver to."""
        fake = self._enqueue({"job_id": "j1", "chat_id": 4242, "user_id": 4242})
        stored = fake.hashes["ffmpeg:job:j1"]
        self.assertEqual(stored["chat_id"], "4242")
        self.assertEqual(stored["user_id"], "4242")

    def test_requeue_scripts_carry_the_name_instead_of_relying_on_the_hash(self):
        """The requeue tooling must pass the name on the payload it builds."""
        for relative in (
            os.path.join("scripts", "requeue_job.py"),
            os.path.join("scripts", "requeue_missing_jobs_once.py"),
            os.path.join("scripts", "forward_auto_reenrich.py"),
        ):
            with open(os.path.join(PROJECT_ROOT, relative), encoding="utf-8") as fh:
                src = fh.read()
            self.assertIn("carry_over_job_naming", src, relative)

    def test_requeue_lpush_fallback_keeps_the_naming_fields(self):
        """The fallback must push the full payload, not a stripped-down one."""
        with open(os.path.join(PROJECT_ROOT, "scripts", "requeue_missing_jobs_once.py"), encoding="utf-8") as fh:
            src = fh.read()
        self.assertNotIn('json.dumps({"job_id": job_id, "input_key": remote_key})', src)
        self.assertIn('json.dumps(job)', src)

    def test_requeue_job_script_exposes_a_name_override(self):
        with open(os.path.join(PROJECT_ROOT, "scripts", "requeue_job.py"), encoding="utf-8") as fh:
            src = fh.read()
        self.assertIn('"--name"', src)
        self.assertIn("stored_job_naming", src)

    def test_worker_bot_api_audio_send_includes_duration(self):
        with open(os.path.join(PROJECT_ROOT, "workers", "ffmpeg_worker.py"), encoding="utf-8") as fh:
            src = fh.read()
        self.assertIn("duration=int(_vid_duration) if _vid_duration is not None else None", src)


class StoredJobOwnerTests(unittest.TestCase):
    """A requeue must carry the owner, or the user never gets the job they queued."""

    def test_extracts_the_owner_fields(self):
        from utils.job_queue import stored_job_owners

        self.assertEqual(stored_job_owners({"chat_id": "7", "user_id": "7"}), {"chat_id": "7", "user_id": "7"})

    def test_survives_bytes_and_missing_values(self):
        from utils.job_queue import stored_job_owners

        self.assertEqual(stored_job_owners({b"chat_id": b"99"}), {"chat_id": "99"})
        for stored in (None, {}, {"status": "queued"}, {"chat_id": ""}):
            self.assertEqual(stored_job_owners(stored), {}, msg=repr(stored))

    def test_carry_over_restores_an_int_chat_id(self):
        from utils.job_queue import carry_over_job_owners

        job = carry_over_job_owners({"job_id": "j1"}, {"chat_id": "42", "user_id": "42"})
        self.assertEqual(job["chat_id"], 42)
        self.assertIsInstance(job["chat_id"], int)

    def test_carry_over_never_replaces_an_explicit_owner(self):
        from utils.job_queue import carry_over_job_owners

        job = carry_over_job_owners({"job_id": "j1", "chat_id": 1}, {"chat_id": "2"})
        self.assertEqual(job["chat_id"], 1)

    def test_requeue_scripts_carry_the_owner(self):
        for relative in (
            os.path.join("scripts", "requeue_job.py"),
            os.path.join("scripts", "requeue_missing_jobs_once.py"),
        ):
            with open(os.path.join(PROJECT_ROOT, relative), encoding="utf-8") as fh:
                src = fh.read()
            self.assertIn("carry_over_job_owners", src, relative)


class MergeAudiosDeliveryTests(unittest.TestCase):
    """merge_audios must caption from the session, not an undefined local."""

    def test_a_successful_merge_sends_the_metadata_caption(self):
        """Regression: the caption read ``current_file``, which only exists in
        other methods, so a successful merge raised NameError while delivering."""
        import asyncio

        import handlers as handlers_module

        handler = object.__new__(handlers_module.EnhancedMediaHandler)

        async def _yes(*_args, **_kwargs):
            return True

        async def _noop(*_args, **_kwargs):
            return None

        captured = {}

        class FakeConverter:
            async def merge_audios(self, paths, output_path):
                with open(output_path, "wb") as fh:
                    fh.write(b"merged")
                return True

        class FakeBot:
            async def send_audio(self, **kwargs):
                captured.update(kwargs)

        handler._require_callback = _yes
        handler._check_conversion_quota = _yes
        handler.safe_edit = _noop
        handler.converter = FakeConverter()

        session = {
            "current_file": {"name": "First.mp3", "_source_metadata": {"title": "Tagged"}},
            "merge_list": [os.path.join(TMP, "a.mp3"), os.path.join(TMP, "b.mp3")],
        }
        update = SimpleNamespace(
            callback_query=SimpleNamespace(message=None),
            effective_chat=SimpleNamespace(id=1),
        )
        context = SimpleNamespace(bot=FakeBot())

        asyncio.run(handler.merge_audios(update, context, session))

        self.assertEqual(captured.get("caption"), "Tagged")
        self.assertEqual(session["merge_list"], [])


class MenuTriggerCoverageTests(unittest.TestCase):
    """Every inline-button trigger from MediaMenuBuilder must be dispatched."""

    MENU_CALLS = (
        ('get_main_menu("video")', lambda b: b.get_main_menu("video")),
        ('get_main_menu("audio")', lambda b: b.get_main_menu("audio")),
        ("get_main_menu()", lambda b: b.get_main_menu()),
        ('get_format_menu("audio")', lambda b: b.get_format_menu("audio")),
        ('get_format_menu("video")', lambda b: b.get_format_menu("video")),
        ("get_compression_menu()", lambda b: b.get_compression_menu()),
        ("get_resolution_menu()", lambda b: b.get_resolution_menu()),
        ("get_audio_format_menu()", lambda b: b.get_audio_format_menu()),
        ('get_bitrate_menu("audio")', lambda b: b.get_bitrate_menu("audio")),
        ('get_bitrate_menu("video")', lambda b: b.get_bitrate_menu("video")),
        ('get_mp3_quality_menu("128k")', lambda b: b.get_mp3_quality_menu("128k")),
        ("get_screenshot_menu()", lambda b: b.get_screenshot_menu()),
        ("get_screenshots_menu()", lambda b: b.get_screenshots_menu()),
        ("get_trimmer_menu()", lambda b: b.get_trimmer_menu()),
        ("get_bulk_menu()", lambda b: b.get_bulk_menu()),
        ("get_bulk_crf_menu(28)", lambda b: b.get_bulk_crf_menu(28)),
        ('get_bulk_preset_menu("web")', lambda b: b.get_bulk_preset_menu("web")),
        ('get_bulk_bitrate_menu("128k")', lambda b: b.get_bulk_bitrate_menu("128k")),
        ('get_merge_menu("video")', lambda b: b.get_merge_menu("video")),
        ('get_merge_menu("audio")', lambda b: b.get_merge_menu("audio")),
        ("get_optimize_menu()", lambda b: b.get_optimize_menu()),
        ("get_extraction_menu()", lambda b: b.get_extraction_menu()),
        ("get_video_tools_menu()", lambda b: b.get_video_tools_menu()),
        ("get_audio_tools_menu()", lambda b: b.get_audio_tools_menu()),
        ("get_advanced_tools_menu()", lambda b: b.get_advanced_tools_menu()),
        ("get_fade_menu()", lambda b: b.get_fade_menu()),
        ("get_confirm_menu()", lambda b: b.get_confirm_menu()),
        ("get_back_button()", lambda b: b.get_back_button()),
    )

    @classmethod
    def setUpClass(cls):
        with open(os.path.join(PROJECT_ROOT, "handlers.py"), encoding="utf-8") as fh:
            src = fh.read()
        start = src.index("async def callback_handler")
        end = src.find("\n    async def ", start + 10)
        body = src[start : end if end != -1 else len(src)]

        cls.matchers = [("eq", m.group(1)) for m in re.finditer(r'data == "([^"]+)"', body)]
        cls.matchers += [("prefix", m.group(1)) for m in re.finditer(r'data\.startswith\("([^"]+)"\)', body)]

        cls.aliases = {}
        alias_match = re.search(r"aliases = \{(.*?)\n        \}", body, re.S)
        if alias_match:
            for m in re.finditer(r'"([^"]+)"\s*:\s*"([^"]+)"', alias_match.group(1)):
                cls.aliases[m.group(1)] = m.group(2)

    def _is_dispatched(self, callback_data):
        data = self.aliases.get(callback_data, callback_data)
        if data.startswith("vbitrate_"):
            data = "bitrate_" + data.split("_", 1)[1]
        for kind, value in self.matchers:
            if kind == "eq" and data == value:
                return True
            if kind == "prefix" and data.startswith(value):
                return True
        return False

    EXTRACTION_TARGETS = {
        "extract_audio_only": "extract_audio",
        "extract_video_only": "extract_video",
        "extract_all": "extract_all_streams",
        "extract_subtitles": "extract_subtitles",
    }

    def test_video_tools_menu_exposes_the_extraction_picker(self):
        from utils.callbacks import EXTRACT_MENU
        from utils.keyboard_utils import MediaMenuBuilder

        kb = MediaMenuBuilder.get_video_tools_menu()
        rendered = {button.callback_data for row in kb.inline_keyboard for button in row}
        self.assertIn(EXTRACT_MENU, rendered)

    def test_extraction_menu_actions_route_to_real_handlers(self):
        from utils.keyboard_utils import MediaMenuBuilder

        kb = MediaMenuBuilder.get_extraction_menu()
        rendered = {button.callback_data for row in kb.inline_keyboard for button in row}
        self.assertTrue(
            set(self.EXTRACTION_TARGETS) <= rendered,
            f"extraction menu is missing buttons: {sorted(set(self.EXTRACTION_TARGETS) - rendered)}",
        )

        for trigger, target in self.EXTRACTION_TARGETS.items():
            resolved = self.aliases.get(trigger, trigger)
            self.assertEqual(resolved, target, f"{trigger} should route to {target}")
            self.assertTrue(
                any(kind == "eq" and value == target for kind, value in self.matchers),
                f"{target} has no dispatch branch in callback_handler",
            )

    def test_no_dead_menu_triggers(self):
        from utils.keyboard_utils import MediaMenuBuilder

        unhandled = []
        for label, build in self.MENU_CALLS:
            kb = build(MediaMenuBuilder)
            for row in kb.inline_keyboard:
                for button in row:
                    if not self._is_dispatched(button.callback_data):
                        unhandled.append(f"{label}: {button.text!r} -> {button.callback_data!r}")
        self.assertEqual(unhandled, [], "unhandled menu triggers:\n" + "\n".join(unhandled))

    def test_every_dispatched_handler_method_exists(self):
        with open(os.path.join(PROJECT_ROOT, "handlers.py"), encoding="utf-8") as fh:
            src = fh.read()
        defined = set(re.findall(r"^\s*(?:async )?def (\w+)\(", src, re.M))
        start = src.index("async def callback_handler")
        end = src.find("\n    async def ", start + 10)
        body = src[start : end if end != -1 else len(src)]
        called = set(re.findall(r"self\.(\w+)\(", body))
        self.assertEqual(
            sorted(m for m in called if m not in defined),
            [],
            "callback_handler calls methods that do not exist",
        )


if __name__ == "__main__":
    unittest.main()
