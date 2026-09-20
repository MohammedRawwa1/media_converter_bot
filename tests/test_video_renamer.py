"""The Media Renamer sets a delivery name - for audio and video alike.

Renaming is metadata-only: it changes the name every delivery is built from,
touches no bytes, and makes one stored-source check (``_adopt_rename_source``) so
the next action reuses the object already in the bucket instead of downloading
the media again. The prompt shows the current name verbatim so it can be copied
and edited rather than retyped.
"""

import os
import unittest

from source_helpers import read_source

from handlers import _document_delivery_name, _rename_prompt_text, _video_delivery_name
from utils.keyboard_utils import MediaMenuBuilder


def _labels_by_callback(menu) -> dict:
    return {button.callback_data: button.text for row in menu.inline_keyboard for button in row}


class MediaRenamerNamingTests(unittest.TestCase):
    def test_the_renamed_stem_keeps_the_output_extension(self):
        """Renaming changes the name, never the format the conversion produced."""
        current = {"name": "Old Name.mkv"}
        self.assertEqual(_video_delivery_name(current, os.path.join("out", "abc_compressed.mp4")), "Old Name.mp4")

    def test_a_pasted_path_is_reduced_to_its_name(self):
        current = {"name": os.path.join("some", "dir", "Clip.mp4")}
        self.assertEqual(_video_delivery_name(current, "out/x.mp4"), "Clip.mp4")

    def test_a_document_delivery_uses_the_same_name(self):
        current = {"name": "Renamed.mp4"}
        self.assertEqual(_document_delivery_name(current, os.path.join("out", "abc_optimized.mp4")), "Renamed.mp4")

    def test_no_name_falls_back_to_the_output_path(self):
        self.assertEqual(_video_delivery_name({}, "out/abc.mp4"), "abc.mp4")


class MediaRenamerPromptTests(unittest.TestCase):
    def test_the_prompt_shows_the_current_name_in_a_code_block(self):
        text = _rename_prompt_text({"name": "My Clip.mp4"})
        self.assertIn("<code>My Clip.mp4</code>", text)
        self.assertIn("Media Renamer", text)

    def test_the_prompt_escapes_html_in_the_name(self):
        text = _rename_prompt_text({"name": "a<b>.mp4"})
        self.assertIn("<code>a&lt;b&gt;.mp4</code>", text)

    def test_a_missing_name_falls_back_to_media(self):
        self.assertIn("<code>media</code>", _rename_prompt_text({}))
        self.assertIn("<code>media</code>", _rename_prompt_text(None))


class MediaRenamerAudioSupportTests(unittest.TestCase):
    def test_the_button_is_media_neutral(self):
        """Named for what it does, because it takes audio and video the same way."""
        for file_type in ("video", "audio"):
            with self.subTest(file_type=file_type):
                labels = _labels_by_callback(MediaMenuBuilder.get_main_menu(file_type))
                self.assertEqual(labels["video_renamer"], "✏️ Media Renamer")

    def test_the_audio_tools_menu_reaches_the_renamer(self):
        labels = _labels_by_callback(MediaMenuBuilder.get_audio_tools_menu())
        self.assertEqual(labels["video_renamer"], "✏️ Media Renamer")

    def test_the_splitter_and_trimmer_are_media_neutral(self):
        labels = {
            "video": _labels_by_callback(MediaMenuBuilder.get_main_menu("video")),
            "audio": _labels_by_callback(MediaMenuBuilder.get_main_menu("audio")),
        }
        self.assertEqual(labels["video"]["video_splitter"], "🔪 Media Splitter")
        self.assertEqual(labels["audio"]["video_splitter"], "🔪 Media Splitter")
        self.assertEqual(labels["video"]["trim_video"], "✂️ Media Trimmer")
        self.assertEqual(labels["audio"]["trim_audio"], "✂️ Media Trimmer")


class MediaRenamerWiringTests(unittest.TestCase):
    def test_an_empty_answer_is_refused_and_the_prompt_stays_armed(self):
        """An empty rename would blank the name every delivery is built from.

        Accepting it replaced the user's name with ``""``, and
        ``_video_delivery_name`` then fell back to the opaque output path - so the
        file arrived as ``<file id>_compressed.mp4``. The prompt has to survive the
        empty answer and ask again.
        """
        src = read_source("handlers.py")
        self.assertIn('elif context.user_data.get("awaiting_rename"):', src)
        self.assertIn("The filename cannot be empty", src)

    def test_the_rename_is_persisted(self):
        """The name lives in the session, so it must survive a restart."""
        src = read_source("handlers.py")
        self.assertIn('session["current_file"]["name"] = _new_name', src)
        self.assertIn("self._persist_session(update.effective_user.id)", src)
        # The refused branch returns before the awaiting flags are cleared.
        self.assertLess(
            src.index("The filename cannot be empty"),
            src.index('session["current_file"]["name"] = _new_name'),
        )

    def test_the_renamer_makes_one_stored_source_check(self):
        """Renaming must not fetch bytes - only record where they already are."""
        src = read_source("handlers.py")
        self.assertIn("await self._adopt_rename_source(current_file, session=session, user_id=user_id)", src)
        # The check is the shared stored-source derivation, not a private copy.
        self.assertIn("return await self._adopt_stored_source(current_file, session=session, user_id=user_id)", src)

    def test_the_prompt_is_rendered_as_html(self):
        src = read_source("handlers.py")
        self.assertIn('await self.safe_edit(query, _rename_prompt_text(current_file), parse_mode="HTML")', src)


if __name__ == "__main__":
    unittest.main()
