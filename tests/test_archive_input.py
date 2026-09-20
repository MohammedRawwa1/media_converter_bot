"""Archive inputs expand into media, and only into media, inside the destination.

Every defence here is load-bearing: an archive's contents are chosen by whoever
built it, so a name that escapes the destination, a symlink entry, a payload
wearing a media extension, or a small file that expands to fill the disk all have
to be refused *before* anything reaches the filesystem.
"""

import asyncio
import contextlib
import os
import shutil
import tempfile
import unittest
import zipfile
from unittest import mock

from source_helpers import read_source

import config
import handlers
from handlers import EnhancedMediaHandler
from utils.archive_input import (
    MEDIA_MEMBER_EXTENSIONS,
    expand_archive,
    is_archive,
)

# A zip entry that describes a symlink stores the mode in the high half of
# external_attr, the same way a Unix zip does.
_SYMLINK_ATTR = 0o120777 << 16


class IsArchiveTests(unittest.TestCase):
    def test_recognises_zip_whatever_the_case(self):
        self.assertTrue(is_archive("bundle.zip"))
        self.assertTrue(is_archive("BUNDLE.ZIP"))
        self.assertTrue(is_archive("nested/dir/bundle.Zip"))

    def test_media_and_empty_are_not_archives(self):
        for name in ("clip.mp4", "song.mp3", "", None, "noext", "archive.tar"):
            with self.subTest(name=name):
                self.assertFalse(is_archive(name))

    def test_zip_is_not_a_convertible_member(self):
        """A nested archive is skipped rather than handed to a later step."""
        self.assertNotIn(".zip", MEDIA_MEMBER_EXTENSIONS)
        self.assertNotIn(".bin", MEDIA_MEMBER_EXTENSIONS)
        self.assertIn(".mp4", MEDIA_MEMBER_EXTENSIONS)


class ExpandArchiveTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="archive_in_")
        self._dest = os.path.join(self._dir, "out")
        self._archive = os.path.join(self._dir, "bundle.zip")

    def _make(self, entries):
        """Write a zip; ``entries`` is a list of (name, bytes) or (ZipInfo, bytes)."""
        with zipfile.ZipFile(self._archive, "w", zipfile.ZIP_DEFLATED) as zf:
            for name_or_info, data in entries:
                zf.writestr(name_or_info, data)
        return self._archive

    def test_extracts_media_and_skips_everything_else(self):
        self._make([("clip.mp4", b"video"), ("song.mp3", b"audio"), ("notes.txt", b"nope")])
        result = expand_archive(self._archive, self._dest)
        self.assertTrue(result.ok)
        self.assertEqual(sorted(m.name for m in result.members), ["clip.mp4", "song.mp3"])
        for member in result.members:
            self.assertTrue(os.path.isfile(member.path))
            # Every member lands directly in the destination - no subdirectory is
            # ever created from an archive's own path.
            self.assertEqual(os.path.dirname(member.path), self._dest)
        # The text file is skipped silently - it is neither media nor an attack.
        self.assertEqual(result.skipped, 1)
        self.assertEqual(result.total_bytes, 10)

    def test_directory_members_are_not_files(self):
        with zipfile.ZipFile(self._archive, "w") as zf:
            zf.writestr("media/", b"")
            zf.writestr("media/clip.mp4", b"video")
        result = expand_archive(self._archive, self._dest)
        self.assertEqual([m.name for m in result.members], ["clip.mp4"])

    def test_a_traversal_name_never_reaches_the_filesystem(self):
        for bad in ("../evil.mp4", "..\\evil.mp4", "a/../../evil.mp4", "/etc/evil.mp4", "C:/evil.mp4"):
            with self.subTest(bad=bad):
                dest = os.path.join(self._dir, "trav")
                self._make([(bad, b"payload")])
                result = expand_archive(self._archive, dest)
                self.assertEqual(result.members, [])
                self.assertEqual(result.skipped, 1)
                # Nothing escaped: the payload is not sitting next to the archive.
                self.assertFalse(os.path.exists(os.path.join(self._dir, "evil.mp4")))

    def test_a_symlink_entry_is_skipped(self):
        info = zipfile.ZipInfo("link.mp4")
        info.external_attr = _SYMLINK_ATTR
        self._make([(info, b"../outside.mp4")])
        result = expand_archive(self._archive, self._dest)
        self.assertEqual(result.members, [])
        self.assertEqual(result.skipped, 1)

    def test_a_nested_zip_is_skipped(self):
        self._make([("inner.zip", b"PK\x03\x04not-really"), ("clip.mp4", b"v")])
        result = expand_archive(self._archive, self._dest)
        self.assertEqual([m.name for m in result.members], ["clip.mp4"])

    def test_duplicate_names_are_uniquified(self):
        self._make([("clip.mp4", b"one"), ("clip.mp4", b"two")])
        result = expand_archive(self._archive, self._dest)
        self.assertEqual(sorted(m.name for m in result.members), ["clip.mp4", "clip_1.mp4"])

    def test_member_cap_refuses_the_whole_archive(self):
        self._make([("a.mp4", b"1"), ("b.mp4", b"2"), ("c.mp4", b"3")])
        result = expand_archive(self._archive, self._dest, max_entries=2)
        self.assertEqual(result.members, [])
        self.assertTrue(any("entry cap" in e for e in result.errors))

    def test_per_file_cap_skips_only_that_file(self):
        self._make([("big.mp4", b"x" * 1024), ("small.mp3", b"y" * 10)])
        result = expand_archive(self._archive, self._dest, max_member_bytes=100)
        self.assertEqual([m.name for m in result.members], ["small.mp3"])
        self.assertTrue(any("per-file cap" in e for e in result.errors))

    def test_total_cap_stops_at_the_limit(self):
        self._make([("a.mp4", b"x" * 1024), ("b.mp4", b"y" * 1024)])
        result = expand_archive(self._archive, self._dest, max_total_bytes=1500)
        self.assertEqual([m.name for m in result.members], ["a.mp4"])
        self.assertTrue(any("total size cap" in e for e in result.errors))

    def test_a_highly_compressible_file_is_refused_as_a_bomb(self):
        # Over the ratio floor (8 MiB) of zeros compresses to almost nothing, the
        # shape of a zip bomb: a small archive that would expand to fill the disk.
        self._make([("bomb.mp4", b"\x00" * (9 * 1024 * 1024))])
        result = expand_archive(self._archive, self._dest, max_ratio=50)
        self.assertEqual(result.members, [])
        self.assertTrue(any("compressed size" in e for e in result.errors))

    def test_an_unreadable_archive_is_an_error_not_a_crash(self):
        with open(self._archive, "wb") as fh:
            fh.write(b"this is not a zip file")
        result = expand_archive(self._archive, self._dest)
        self.assertEqual(result.members, [])
        self.assertTrue(any("readable zip" in e for e in result.errors))

    def test_extension_matching_is_case_insensitive(self):
        self._make([("CLIP.MP4", b"video")])
        result = expand_archive(self._archive, self._dest)
        self.assertEqual([m.name for m in result.members], ["CLIP.MP4"])


class ArchiveWiringTests(unittest.TestCase):
    def test_the_uploader_expands_archives_instead_of_enqueuing_them(self):
        src = read_source("web", "webapp.py")
        self.assertIn("if is_archive(filename):", src)
        self.assertIn("_handle_archive_upload(input_path, filename, job_id, request_id, caller_user_id)", src)
        self.assertIn("expand_archive(archive_path, dest_dir)", src)

    def test_the_receipt_names_the_parent_and_each_member_gets_a_capability(self):
        src = read_source("web", "webapp.py")
        self.assertIn('"parent_job_id": parent_job_id}', src)
        self.assertIn('issue_job_token(job["job_id"])', src)

    def test_the_ui_shows_a_card_per_member(self):
        src = read_source("web", "static", "app.js")
        self.assertIn("data.archive.members.forEach", src)

    def test_the_bot_unpacks_archives_through_the_action_guard(self):
        src = read_source("handlers.py")
        self.assertIn("if is_archive(file_name):", src)
        self.assertIn("await self.handle_archive_document(", src)
        # The large-file path (userbot / big-file pipeline) is what fetches it.
        self.assertIn("await self._ensure_local_media(", src)
        self.assertIn("allowed_exts=frozenset((*_VIDEO_EXTS, *_AUDIO_EXTS, *_IMAGE_EXTS))", src)


class _FakeMessage:
    def __init__(self, replies):
        self._replies = replies

    async def reply_text(self, text, **kwargs):
        self._replies.append(text)


class _FakeUpdate:
    def __init__(self, replies):
        self.message = _FakeMessage(replies)


class _FakeDocument:
    def __init__(self, file_id, file_name, size):
        self.file_id = file_id
        self.file_name = file_name
        self.file_size = size
        self.file_unique_id = "uniq1"


class BotArchiveTests(unittest.TestCase):
    """The bot expands a sent archive and its media members join the batch."""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="bot_archive_")
        self._archive = os.path.join(self._dir, "bundle.zip")

    def tearDown(self):
        shutil.rmtree(self._dir, ignore_errors=True)

    def _make(self, entries):
        with zipfile.ZipFile(self._archive, "w", zipfile.ZIP_DEFLATED) as zf:
            for name, data in entries:
                zf.writestr(name, data)

    def _handle(self, session, *, file_name="bundle.zip", file_size=None, limit=None):
        """Run handle_archive_document with the fetch guard stubbed to the archive."""
        replies = []
        update = _FakeUpdate(replies)
        context = mock.Mock()
        document = _FakeDocument("doc1", file_name, file_size if file_size is not None else 8)
        handler = object.__new__(EnhancedMediaHandler)

        async def _ensure_local_media(_self, _update, _context, sess, *args, **kwargs):
            # Stand in for the real fetch: point the session's current file at the
            # prepared archive, so the expansion reads real bytes.
            sess["current_file"]["path"] = self._archive
            return sess["current_file"], None

        patcher = (
            mock.patch.object(handlers, "_BULK_LIST_LIMIT", limit) if limit is not None else contextlib.nullcontext()
        )
        with (
            mock.patch.object(config, "INPUT_PATH", self._dir, create=True),
            mock.patch.object(EnhancedMediaHandler, "_ensure_local_media", _ensure_local_media),
            patcher,
        ):
            asyncio.run(handler.handle_archive_document(update, context, session, document, 42, file_name))
        return replies

    def test_media_members_join_the_batch_as_local_files(self):
        self._make([("clip.mp4", b"v" * 32), ("song.mp3", b"a" * 16), ("notes.txt", b"nope")])
        session = {"bulk_list": [], "current_file": {"name": "before.mp4"}}
        replies = self._handle(session)

        by_name = {e["name"]: e for e in session["bulk_list"]}
        self.assertEqual(sorted(by_name), ["clip.mp4", "song.mp3"])
        self.assertEqual(by_name["clip.mp4"]["type"], "video")
        self.assertEqual(by_name["song.mp3"]["type"], "audio")
        for entry in session["bulk_list"]:
            self.assertTrue(os.path.isfile(entry["path"]))
            self.assertIsNone(entry["id"])
        # The file that was already in the session is put back, untouched.
        self.assertEqual(session["current_file"], {"name": "before.mp4"})
        self.assertTrue(any("added 2" in r for r in replies))

    def test_an_archive_with_no_media_adds_nothing(self):
        self._make([("notes.txt", b"nope"), ("inner.zip", b"PK\x03\x04")])
        session = {"bulk_list": []}
        replies = self._handle(session)
        self.assertEqual(session["bulk_list"], [])
        self.assertTrue(any("no supported media" in r for r in replies))

    def test_members_never_evict_a_file_already_in_the_batch(self):
        """A full batch must not drop the user's earlier file to make room."""
        self._make([("a.mp4", b"1"), ("b.mp4", b"2")])
        existing = {"name": "earlier.mp4", "path": os.path.join(self._dir, "earlier.mp4"), "id": "keepme"}
        session = {"bulk_list": [existing]}
        replies = self._handle(session, limit=2)
        self.assertIn(existing, session["bulk_list"])
        self.assertEqual(len(session["bulk_list"]), 2)
        self.assertTrue(any("did not fit" in r for r in replies))


if __name__ == "__main__":
    unittest.main()
