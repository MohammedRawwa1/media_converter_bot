"""/usersettings is a settings panel, not another way to start a conversion.

The audio bitrate entry used to be the "Bitrate" action wearing a settings
label: picking 64k re-encoded the currently loaded file (and, for a large one,
queued a job), so the panel that exists to state a preference could not be used
at all without a loaded file - it answered "No audio file found".

Everything in the panel is pinned here: its buttons carry only ``settings_``
triggers, the bitrate picker marks the active value, and the media actions that
used to live behind it (Audio/Video/Advanced Tools, Video Metadata, Mp3 Tag
Editor, audio rename) are reachable from the menus that act on media instead.
"""

import ast
import unittest

from source_helpers import find_function, parse_source, read_source

from utils import user_settings
from utils.keyboard_utils import MediaMenuBuilder

# Triggers that start real work in the menus that own them - none of them may
# appear on a settings page.
_PROCESSING_TRIGGERS = {
    "menu_audio",
    "menu_video",
    "menu_advanced",
    "full_info",
    "mp3_tag_editor",
    "video_renamer",
}

# Callback prefixes that begin real work.
_PROCESSING_PREFIXES = (
    "bitrate_",
    "vbitrate_",
    "mp3q_",
    "compress_",
    "format_",
    "res_",
    "optimize_",
    "extract_",
    "trim",
    "fade_",
    "normalize_audio",
    "bulk_apply",
    "cancel_job:",
)


def _callbacks(markup):
    return [button.callback_data for row in markup.inline_keyboard for button in row]


def _labels(markup):
    return [button.text for row in markup.inline_keyboard for button in row]


class SettingsPanelTriggerTests(unittest.TestCase):
    """No button on a settings page may encode, download or queue anything."""

    @staticmethod
    def _pages():
        return {
            "page 1": MediaMenuBuilder.get_settings_page(1, {}),
            "page 2": MediaMenuBuilder.get_settings_page(2, {"audio_bitrate": "192k"}),
            "bitrate picker": MediaMenuBuilder.get_settings_bitrate_menu("192k"),
            "rename": MediaMenuBuilder.get_settings_rename_menu({}),
            "words": MediaMenuBuilder.get_settings_words_menu({}),
        }

    def test_no_settings_button_starts_a_conversion(self):
        offenders = []
        for page, markup in self._pages().items():
            for callback_data in _callbacks(markup):
                if callback_data in _PROCESSING_TRIGGERS or callback_data.startswith(_PROCESSING_PREFIXES):
                    offenders.append(f"{page}: {callback_data!r}")
        self.assertEqual(offenders, [], "settings pages exposed processing triggers:\n" + "\n".join(offenders))

    def test_upload_as_audio_is_gone_and_upload_as_video_remains(self):
        callbacks = {callback_data for markup in self._pages().values() for callback_data in _callbacks(markup)}
        labels = {label for markup in self._pages().values() for label in _labels(markup)}

        # "Upload as Audio" was a second route into Audio Tools, and is gone.
        self.assertNotIn("menu_audio", callbacks)
        self.assertNotIn("Upload as Audio", labels)
        # The video delivery format switch stays, because it is a real choice.
        self.assertTrue(any(c.startswith("settings_upload_mode:") for c in callbacks), callbacks)

    def test_the_actions_moved_to_the_menus_that_act_on_media(self):
        main = _callbacks(MediaMenuBuilder.get_main_menu("video"))
        self.assertIn("menu_audio", main)
        self.assertIn("menu_video", main)
        self.assertIn("menu_advanced", main)
        self.assertIn("mp3_tag_editor", _callbacks(MediaMenuBuilder.get_audio_tools_menu()))

    def test_the_panel_is_rendered_from_settings_alone(self):
        # The view takes the user and a page number - nothing to do with a
        # session, so /usersettings renders whether or not a file is loaded.
        view = find_function(parse_source("handlers.py"), "_settings_view")
        self.assertEqual([arg.arg for arg in view.args.args], ["self", "user_id", "page"])


class SettingsBitratePickerTests(unittest.TestCase):
    def test_marks_the_active_value(self):
        from utils.callbacks import settings_bitrate_key

        buttons = dict(
            zip(
                _callbacks(MediaMenuBuilder.get_settings_bitrate_menu("192k")),
                _labels(MediaMenuBuilder.get_settings_bitrate_menu("192k")),
                strict=True,
            )
        )
        self.assertTrue(buttons[settings_bitrate_key("192k")].startswith("✅"))
        self.assertFalse(buttons[settings_bitrate_key("128k")].startswith("✅"))
        self.assertIn(settings_bitrate_key("custom"), buttons)

    def test_uses_settings_triggers_not_the_conversion_ones(self):
        from utils.callbacks import SETTINGS_BITRATE_PREFIX

        rendered = _callbacks(MediaMenuBuilder.get_settings_bitrate_menu("128k"))
        self.assertEqual(rendered[-1], "settings_page:2")
        for callback_data in rendered[:-1]:
            self.assertTrue(callback_data.startswith(SETTINGS_BITRATE_PREFIX), callback_data)


class AudioBitratePreferenceTests(unittest.TestCase):
    """The stored bitrate is what the audio conversions fall back to."""

    def test_defaults_to_the_shared_mp3_default(self):
        from utils.callbacks import MP3_DEFAULT_BITRATE

        self.assertEqual(user_settings.DEFAULTS.get("audio_bitrate"), MP3_DEFAULT_BITRATE)

    def test_the_settings_branch_stores_the_value(self):
        self.assertIn(
            'user_settings.set_user_setting(user_id, "audio_bitrate", bitrate)',
            read_source("handlers.py"),
        )

    def test_conversions_fall_back_to_the_preference(self):
        text = "".join(read_source("handlers.py").split())

        # Both audio paths read the user's bitrate rather than the module
        # constant, and the helper reads it out of the settings store.
        self.assertIn("or_user_audio_bitrate(user_id)", text)
        self.assertIn("_user_audio_bitrate(update.effective_user.id)", text)
        self.assertIn('get_user_setting(user_id,"audio_bitrate")', text)

    def test_the_adjust_bitrate_action_is_still_driven_by_its_own_menu(self):
        # The action keeps working from Audio Tools; only the panel stopped
        # routing into it.
        tree = parse_source("handlers.py")
        callback = find_function(tree, "callback_handler")
        called = {
            node.func.attr
            for node in ast.walk(callback)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertIn("adjust_bitrate", called)


if __name__ == "__main__":
    unittest.main()
