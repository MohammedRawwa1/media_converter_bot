"""The Video Renamer sets a delivery name - including when the answer is empty."""

import os
import unittest

from source_helpers import read_source

from handlers import _document_delivery_name, _video_delivery_name


class VideoRenamerNamingTests(unittest.TestCase):
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


class VideoRenamerWiringTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
