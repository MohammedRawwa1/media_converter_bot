"""/usersettings is a settings panel, not another way to start a conversion.

The audio bitrate entry used to be the "Bitrate" action wearing a settings
label: picking 64k re-encoded the currently loaded file (and, for a large one,
queued a job), so the panel that exists to state a preference could not be used
at all without a loaded file - it answered "No audio file found".

Everything in the panel is pinned here: its buttons carry only ``settings_``
triggers, the pickers mark the active value, and the media actions that used to
live behind it (Audio/Video/Advanced Tools, Video Metadata, Mp3 Tag Editor, audio
rename) are reachable from the menus that act on media instead.

The panel is also paginated: the pages, their titles and the Prev/Next row all
come from one list, so a page cannot be reachable by a button that renders
something else. Its second page holds the conversion defaults - the quality and
preset a conversion starts from, under the same keys the bulk pickers write.
"""

import ast
import asyncio
import unittest
from types import MethodType, SimpleNamespace
from unittest.mock import patch

from source_helpers import find_function, parse_source, read_source

import handlers as handlers_module
from handlers import EnhancedMediaHandler
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
            "page 3": MediaMenuBuilder.get_settings_page(3, {"bulk_slideshow_seconds": 5}),
            "bitrate picker": MediaMenuBuilder.get_settings_bitrate_menu("192k"),
            "quality picker": MediaMenuBuilder.get_settings_quality_menu(23),
            "preset picker": MediaMenuBuilder.get_settings_preset_menu("web"),
            "slideshow picker": MediaMenuBuilder.get_settings_slideshow_menu(5),
            "batch bitrate picker": MediaMenuBuilder.get_settings_bulk_bitrate_menu("128k"),
            "rename": MediaMenuBuilder.get_settings_rename_menu({}),
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

    def test_the_words_button_is_gone(self):
        """It counted a list the panel could not act on - replaced by quality."""
        src = (
            read_source("handlers.py")
            + read_source("utils", "keyboard_utils.py")
            + read_source("utils", "callbacks.py")
        )
        self.assertNotIn("words_remove", src)
        self.assertNotIn("settings_words_menu", src)
        labels = {label for markup in self._pages().values() for label in _labels(markup)}
        self.assertFalse([label for label in labels if "Word" in label], labels)

    def test_the_panel_pages_number_themselves_from_one_list(self):
        from utils.callbacks import SETTINGS_PAGE_COUNT, SETTINGS_PAGES, settings_page_key

        self.assertEqual(SETTINGS_PAGE_COUNT, len(SETTINGS_PAGES))
        # Every page the pager can reach renders, with its own title.
        for number in range(1, SETTINGS_PAGE_COUNT + 1):
            markup = MediaMenuBuilder.get_settings_page(number, {})
            self.assertTrue(_callbacks(markup), f"page {number} rendered no buttons")
        self.assertEqual(settings_page_key(99), f"settings_page:{SETTINGS_PAGE_COUNT}")
        self.assertEqual(settings_page_key(0), "settings_page:1")


class SettingsPageNavigationTests(unittest.TestCase):
    """Prev/Next walk the real pages and never point off the end of them."""

    @staticmethod
    def _nav(markup):
        return [c for c in _callbacks(markup) if c.startswith("settings_page:")]

    def test_the_first_page_offers_next_but_no_prev(self):
        nav = self._nav(MediaMenuBuilder.get_settings_page(1, {}))
        self.assertEqual(nav, ["settings_page:2"])

    def test_the_last_page_offers_prev_but_no_next(self):
        from utils.callbacks import SETTINGS_PAGE_COUNT

        nav = self._nav(MediaMenuBuilder.get_settings_page(SETTINGS_PAGE_COUNT, {}))
        self.assertEqual(nav, [f"settings_page:{SETTINGS_PAGE_COUNT - 1}"])

    def test_every_page_before_the_last_can_move_forward_one(self):
        from utils.callbacks import SETTINGS_PAGE_COUNT

        for number in range(1, SETTINGS_PAGE_COUNT):
            nav = self._nav(MediaMenuBuilder.get_settings_page(number, {}))
            self.assertIn(f"settings_page:{number + 1}", nav, f"page {number} cannot go forward")

    def test_an_out_of_range_page_renders_a_real_one(self):
        from utils.callbacks import SETTINGS_PAGE_COUNT, settings_page_number

        self.assertEqual(settings_page_number(0), 1)
        self.assertEqual(settings_page_number("nonsense"), 1)
        self.assertEqual(settings_page_number(None), 1)
        self.assertEqual(settings_page_number(99), SETTINGS_PAGE_COUNT)
        # The buttons of a requested-but-missing page are a real page's buttons.
        self.assertEqual(
            _callbacks(MediaMenuBuilder.get_settings_page(99, {})),
            _callbacks(MediaMenuBuilder.get_settings_page(SETTINGS_PAGE_COUNT, {})),
        )

    def test_the_view_uses_the_same_page_number_as_the_keyboard(self):
        text = read_source("handlers.py")
        self.assertIn("page = settings_page_number(page)", text)


class ConversionQualityPreferenceTests(unittest.TestCase):
    """The panel's quality picks are the ones a conversion starts from."""

    def test_the_quality_choices_come_from_one_list(self):
        from utils.callbacks import BULK_CRF_CHOICES, COMPRESS_QUALITY_LABELS

        self.assertEqual(set(BULK_CRF_CHOICES), set(COMPRESS_QUALITY_LABELS))

    def test_the_handler_dispatches_the_pickers_own_triggers(self):
        """The handler compares literals; the menus build them from constants."""
        from utils.callbacks import SETTINGS_PRESET_PREFIX, SETTINGS_QUALITY_PREFIX

        src = read_source("handlers.py")
        self.assertIn(f'data.startswith("{SETTINGS_QUALITY_PREFIX}")', src)
        self.assertIn(f'data.startswith("{SETTINGS_PRESET_PREFIX}")', src)

    def test_the_panel_writes_the_same_keys_the_bulk_pickers_read(self):
        from utils.callbacks import COMPRESS_QUALITY_KEY, OPTIMIZE_PRESET_KEY

        # One key, two menus: a second key is how the panel and the bulk picker
        # would come to disagree about "the user's quality".
        self.assertEqual(COMPRESS_QUALITY_KEY, "bulk_crf")
        self.assertEqual(OPTIMIZE_PRESET_KEY, "bulk_optimize_preset")
        src = read_source("handlers.py")
        self.assertIn("user_settings.set_user_setting(user_id, COMPRESS_QUALITY_KEY, crf)", src)
        self.assertIn("user_settings.set_user_setting(user_id, OPTIMIZE_PRESET_KEY, preset)", src)

    def test_the_quality_label_names_every_offered_crf(self):
        from utils.callbacks import BULK_CRF_CHOICES, compress_quality_label

        for crf in BULK_CRF_CHOICES:
            self.assertNotIn("CRF", compress_quality_label(crf), f"CRF {crf} has no name")
        self.assertEqual(compress_quality_label(23), "🟡 Medium")
        # A custom CRF is shown as its number rather than a wrong name.
        self.assertEqual(compress_quality_label(31), "CRF 31")
        self.assertEqual(compress_quality_label("junk"), "CRF")

    def test_an_unusable_stored_quality_falls_back_to_the_default(self):
        from utils.callbacks import BULK_CRF_DEFAULT, compress_quality_label

        for bad in (None, "", "junk", 0, 99, True):
            markup = MediaMenuBuilder.get_settings_quality_menu(bad)
            labels = _labels(markup)
            marked = [label for label in labels if label.startswith("✅")]
            self.assertEqual(len(marked), 1, labels)
            self.assertIn(compress_quality_label(BULK_CRF_DEFAULT), marked[0], repr(bad))

    def test_a_missing_stored_preset_falls_back_to_the_default(self):
        from utils.callbacks import BULK_PRESET_DEFAULT, BULK_PRESET_LABELS

        labels = _labels(MediaMenuBuilder.get_settings_preset_menu("nonsense"))
        marked = [label for label in labels if label.startswith("✅")]
        self.assertEqual(len(marked), 1, labels)
        self.assertIn(BULK_PRESET_LABELS[BULK_PRESET_DEFAULT], marked[0])

    def test_the_action_menus_mark_the_stored_choice_as_the_default(self):
        compress = _labels(MediaMenuBuilder.get_compression_menu(28))
        self.assertTrue(any("Low" in label and "default" in label for label in compress), compress)
        optimize = _labels(MediaMenuBuilder.get_optimize_menu("tv"))
        self.assertTrue(any("TV" in label and "default" in label for label in optimize), optimize)
        # Nothing stored: no button claims to be the default.
        self.assertFalse(
            [label for label in _labels(MediaMenuBuilder.get_compression_menu(None)) if "default" in label]
        )
        self.assertFalse([label for label in _labels(MediaMenuBuilder.get_optimize_menu(None)) if "default" in label])


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

    def test_a_custom_value_is_shown_on_the_custom_row(self):
        """A bitrate that is not a preset has to be readable back somewhere.

        It used to mark the *default* preset instead and leave the Custom row
        unchanged, so a stored 100k showed a check beside 128k and nothing at all
        beside the value the user had just typed - the setting looked dropped.
        """
        from utils.callbacks import settings_bitrate_key

        buttons = dict(
            zip(
                _callbacks(MediaMenuBuilder.get_settings_bitrate_menu("100k")),
                _labels(MediaMenuBuilder.get_settings_bitrate_menu("100k")),
                strict=True,
            )
        )
        custom = buttons[settings_bitrate_key("custom")]
        self.assertTrue(custom.startswith("✅"), custom)
        self.assertIn("100k", custom)
        # And no preset claims to be the active one.
        for preset in ("64k", "96k", "128k", "192k", "256k", "320k"):
            self.assertFalse(buttons[settings_bitrate_key(preset)].startswith("✅"), preset)

    def test_an_unset_bitrate_still_marks_the_default(self):
        from utils.callbacks import MP3_DEFAULT_BITRATE, settings_bitrate_key

        buttons = dict(
            zip(
                _callbacks(MediaMenuBuilder.get_settings_bitrate_menu(None)),
                _labels(MediaMenuBuilder.get_settings_bitrate_menu(None)),
                strict=True,
            )
        )
        self.assertTrue(buttons[settings_bitrate_key(MP3_DEFAULT_BITRATE)].startswith("✅"))
        self.assertEqual(buttons[settings_bitrate_key("custom")], "✏️ Custom bitrate")

    def test_uses_settings_triggers_not_the_conversion_ones(self):
        from utils.callbacks import SETTINGS_BITRATE_PREFIX

        rendered = _callbacks(MediaMenuBuilder.get_settings_bitrate_menu("128k"))
        self.assertEqual(rendered[-1], "settings_page:2")
        for callback_data in rendered[:-1]:
            self.assertTrue(callback_data.startswith(SETTINGS_BITRATE_PREFIX), callback_data)


class _FakeSettingsStore:
    """A settings store that records instead of writing a file."""

    def __init__(self, values=None):
        self.values = dict(values or {})
        self.writes: list[tuple[int, str, object]] = []

    def get_user_settings(self, user_id):
        return dict(self.values)

    def get_user_setting(self, user_id, key, default=None):
        return self.values.get(key, default)

    def set_user_setting(self, user_id, key, value):
        self.writes.append((user_id, key, value))
        self.values[key] = value


class _FakeMessage:
    def __init__(self, replies, text=""):
        self.replies = replies
        self.text = text

    async def reply_text(self, text, **kwargs):
        self.replies.append(text)
        return SimpleNamespace(message_id=1)


class _FakeUpdate:
    def __init__(self, replies, text=""):
        self.callback_query = None
        self.message = _FakeMessage(replies, text)
        self.effective_chat = SimpleNamespace(id=99)
        self.effective_user = SimpleNamespace(id=7)


class _SettingsStubHandler:
    """Just enough of the handler for the methods under test."""

    def __init__(self, sessions=None):
        self.user_sessions = {7: {"current_file": None}} if sessions is None else sessions


class SettingsPanelViewTests(unittest.TestCase):
    """The two pages render from the settings store alone."""

    def _view(self, page, stored=None):
        store = _FakeSettingsStore(stored)
        handler = _SettingsStubHandler()
        handler._settings_view = MethodType(EnhancedMediaHandler._settings_view, handler)
        with patch.object(handlers_module, "user_settings", store):
            return handler._settings_view(7, page)

    def test_page_one_names_the_general_settings_only(self):
        text, markup = self._view(1, {"prefix": "p_", "suffix": "_s", "upload_mode": "file"})

        self.assertIn("General", text)
        self.assertIn("p_", text)
        self.assertIn("_s", text)
        self.assertIn("document", text)
        self.assertNotIn("Word", text)
        labels = [button.text for row in markup.inline_keyboard for button in row]
        self.assertFalse([label for label in labels if "Word" in label], labels)

    def test_page_two_names_the_conversion_defaults(self):
        text, _markup = self._view(2, {"bulk_crf": 23, "bulk_optimize_preset": "tv", "audio_bitrate": "192k"})

        self.assertIn("Quality", text)
        self.assertIn("Medium", text)
        self.assertIn("CRF 23", text)
        self.assertIn("For TV", text)
        self.assertIn("192k", text)

    def test_page_two_falls_back_to_the_defaults(self):
        text, _markup = self._view(2, {})

        self.assertIn("🔴 Low", text)
        self.assertIn("CRF 28", text)
        self.assertIn("For Web", text)
        self.assertIn("128k", text)

    def test_page_three_names_the_batch_defaults(self):
        text, _markup = self._view(3, {"bulk_slideshow_seconds": 5, "bulk_extract_bitrate": "256k"})

        self.assertIn("Batch", text)
        self.assertIn("5s", text)
        self.assertIn("256k", text)

    def test_page_three_falls_back_to_the_defaults(self):
        from utils.callbacks import BULK_SLIDESHOW_DEFAULT

        text, _markup = self._view(3, {})

        self.assertIn(f"{BULK_SLIDESHOW_DEFAULT:g}s", text)
        self.assertIn("128k", text)

    def test_every_page_states_which_page_it_is(self):
        from utils.callbacks import SETTINGS_PAGE_COUNT

        for page in range(1, SETTINGS_PAGE_COUNT + 1):
            text, _markup = self._view(page)
            self.assertIn(f"({page}/{SETTINGS_PAGE_COUNT})", text)

    def test_a_page_number_out_of_range_renders_a_real_page(self):
        from utils.callbacks import SETTINGS_PAGE_COUNT

        text, markup = self._view(7)

        self.assertIn(f"({SETTINGS_PAGE_COUNT}/{SETTINGS_PAGE_COUNT})", text)
        self.assertEqual(
            [button.callback_data for row in markup.inline_keyboard for button in row],
            [button.callback_data for row in self._view(SETTINGS_PAGE_COUNT)[1].inline_keyboard for button in row],
        )


class SettingsCustomQualityInputTests(unittest.TestCase):
    """The typed quality must be validated, stored and acknowledged."""

    def _run(self, typed, flag="awaiting_settings_quality", stored=None, sessions=None):
        store = _FakeSettingsStore(stored)
        replies: list[str] = []
        context = SimpleNamespace(user_data={flag: True})
        update = _FakeUpdate(replies, text=typed)
        handler = _SettingsStubHandler(sessions)
        for name in ("handle_custom_input", "_handle_settings_prompt", "_load_persisted_session"):
            setattr(handler, name, MethodType(getattr(EnhancedMediaHandler, name), handler))
        with patch.object(handlers_module, "user_settings", store):
            asyncio.run(handler.handle_custom_input(update, context))
        return store, replies, context

    def test_a_typed_crf_is_stored_under_the_shared_quality_key(self):
        from utils.callbacks import COMPRESS_QUALITY_KEY

        store, replies, context = self._run("31")

        self.assertEqual(store.writes, [(7, COMPRESS_QUALITY_KEY, 31)])
        self.assertTrue(any("Compress quality set" in text for text in replies), replies)
        self.assertFalse(context.user_data.get("awaiting_settings_quality"))

    def test_an_unusable_crf_is_refused_and_nothing_is_stored(self):
        for typed in ("99", "17", "abc", ""):
            store, replies, _context = self._run(typed)
            self.assertEqual(store.writes, [], f"{typed!r} was stored: {store.writes}")
            self.assertTrue(any("Invalid quality" in text for text in replies), replies)

    def test_a_settings_prompt_is_answered_without_a_loaded_file(self):
        """The panel renders with no session, so its prompts must answer there too."""
        from utils.callbacks import COMPRESS_QUALITY_KEY

        store, replies, _context = self._run("31", sessions={})

        self.assertEqual(store.writes, [(7, COMPRESS_QUALITY_KEY, 31)])
        self.assertFalse(
            any("Session expired" in text for text in replies),
            f"a preference must not need a loaded file: {replies}",
        )

    def test_a_typed_slideshow_length_is_stored_under_the_bulk_key(self):
        from utils.callbacks import SLIDESHOW_SECONDS_KEY

        store, replies, _context = self._run("4.5", flag="awaiting_settings_slideshow")

        self.assertEqual(store.writes, [(7, SLIDESHOW_SECONDS_KEY, 4.5)])
        self.assertTrue(any("Slideshow set to 4.5s" in text for text in replies), replies)

    def test_an_unusable_slideshow_length_is_refused(self):
        for typed in ("0", "-3", "lots", ""):
            store, replies, _context = self._run(typed, flag="awaiting_settings_slideshow")
            self.assertEqual(store.writes, [], f"{typed!r} was stored: {store.writes}")
            self.assertTrue(any("Invalid length" in text for text in replies), replies)

    def test_a_typed_batch_bitrate_is_stored_under_the_bulk_key(self):
        from utils.callbacks import BULK_EXTRACT_BITRATE_KEY

        store, replies, _context = self._run("192k", flag="awaiting_settings_bulk_bitrate")

        self.assertEqual(store.writes, [(7, BULK_EXTRACT_BITRATE_KEY, "192k")])
        self.assertTrue(any("Batch extract bitrate set to 192k" in text for text in replies), replies)

    def test_a_message_that_is_not_a_settings_prompt_still_goes_to_the_media_path(self):
        store, replies, _context = self._run("31", flag="awaiting_nothing_at_all", sessions={})

        # The settings helper only claims its own flags: everything else carries
        # on to the media prompts, session guard and all.
        self.assertEqual(store.writes, [])
        self.assertTrue(any("Session expired" in text for text in replies), replies)


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

        # Every audio path reads the user's bitrate through the one resolver,
        # rather than the module constant, and the helper reads it out of the
        # settings store.
        self.assertIn("or_user_audio_bitrate(user_id)", text)
        self.assertIn("_effective_audio_bitrate(", text)
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
