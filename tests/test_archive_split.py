"""The archive part-size preference parses, normalizes, and resolves as one value.

Create Archive splits a packed ZIP by this setting, /usersettings stores and
labels it, and both read it through ``utils.archive_split`` so a value chosen in
one screen cannot mean something else in the other.
"""

import unittest

from utils import archive_split


class ParseTests(unittest.TestCase):
    def test_a_size_needs_a_unit_and_keeps_its_bytes(self):
        self.assertEqual(archive_split.parse("500MB"), archive_split.size_value(500 * 1024**2))
        self.assertEqual(archive_split.parse("500mb"), archive_split.size_value(500 * 1024**2))
        self.assertEqual(archive_split.parse("1.5GB"), archive_split.size_value(int(1.5 * 1024**3)))
        self.assertEqual(archive_split.parse("2g"), archive_split.size_value(2 * 1024**3))

    def test_a_bare_integer_is_a_part_count(self):
        self.assertEqual(archive_split.parse("3"), archive_split.parts_value(3))
        self.assertEqual(archive_split.parse(" 12 "), archive_split.parts_value(12))

    def test_words_for_default_and_off(self):
        for word in ("", "default", "auto", "skip"):
            with self.subTest(word=word):
                self.assertEqual(archive_split.parse(word), archive_split.DEFAULT_VALUE)
        for word in ("off", "none", "no split", "disable", "disabled"):
            with self.subTest(word=word):
                self.assertEqual(archive_split.parse(word), archive_split.OFF_VALUE)

    def test_unusable_answers_raise(self):
        for raw in ("abc", "1", "0", "0.5MB", "500 megabytes"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                archive_split.parse(raw)

    def test_a_part_smaller_than_the_floor_is_refused(self):
        with self.assertRaises(ValueError):
            archive_split.parse("0.5MB")


class NormalizeTests(unittest.TestCase):
    def test_a_stored_value_is_canonicalized(self):
        self.assertEqual(archive_split.normalize("OFF"), archive_split.OFF_VALUE)
        self.assertEqual(archive_split.normalize("parts:3"), archive_split.parts_value(3))
        self.assertEqual(archive_split.normalize("size:1048576"), archive_split.size_value(1024**2))

    def test_anything_unusable_falls_back_to_default(self):
        for value in ("", None, "yolo", "size:5", "parts:1", "parts:x"):
            with self.subTest(value=value):
                self.assertEqual(archive_split.normalize(value), archive_split.DEFAULT_VALUE)


class LabelTests(unittest.TestCase):
    def test_labels(self):
        self.assertEqual(archive_split.label("off"), "No split — one .zip")
        self.assertEqual(archive_split.label(archive_split.size_value(500 * 1024**2)), "Parts of 500.0 MB")
        self.assertEqual(archive_split.label(archive_split.parts_value(4)), "4 equal parts")
        self.assertEqual(archive_split.label(""), "Auto — split only if over the cap")


class PlanTests(unittest.TestCase):
    def test_default_uses_the_configured_cap(self):
        self.assertEqual(archive_split.plan("default", cap_bytes=2 * 1024**3), (2 * 1024**3, 0))

    def test_default_with_no_cap_never_splits(self):
        self.assertEqual(archive_split.plan("default", cap_bytes=0), (0, 0))

    def test_off_never_splits(self):
        self.assertEqual(archive_split.plan("off", cap_bytes=2 * 1024**3), (0, 0))

    def test_an_explicit_size_overrides_the_cap(self):
        self.assertEqual(archive_split.plan(archive_split.size_value(1024**2), cap_bytes=0), (1024**2, 0))

    def test_a_part_count_rides_as_a_count(self):
        self.assertEqual(archive_split.plan(archive_split.parts_value(3), cap_bytes=2 * 1024**3), (0, 3))


class EstimateTests(unittest.TestCase):
    def test_default_estimates_from_the_cap(self):
        self.assertEqual(archive_split.estimate_parts("default", 3 * 1024**3, 2 * 1024**3), 2)
        self.assertEqual(archive_split.estimate_parts("default", 1024**3, 2 * 1024**3), 1)

    def test_a_size_estimates_from_the_known_total(self):
        self.assertEqual(archive_split.estimate_parts(archive_split.size_value(500 * 1024**2), 1024**3, 0), 3)

    def test_a_count_is_itself(self):
        self.assertEqual(archive_split.estimate_parts(archive_split.parts_value(4), 1024**3, 0), 4)

    def test_off_is_always_one(self):
        self.assertEqual(archive_split.estimate_parts("off", 5 * 1024**3, 0), 1)

    def test_an_unknown_total_is_no_estimate(self):
        self.assertIsNone(archive_split.estimate_parts("default", 0, 2 * 1024**3))
        self.assertIsNone(archive_split.estimate_parts(archive_split.size_value(1024**2), 0, 0))


class FormatSizeTests(unittest.TestCase):
    def test_labels(self):
        self.assertEqual(archive_split.format_size(0), "unknown")
        self.assertEqual(archive_split.format_size(None), "unknown")
        self.assertEqual(archive_split.format_size(512), "512 B")
        self.assertEqual(archive_split.format_size(1536), "1.5 KB")
        self.assertEqual(archive_split.format_size(3 * 1024**3), "3.0 GB")


if __name__ == "__main__":
    unittest.main()
