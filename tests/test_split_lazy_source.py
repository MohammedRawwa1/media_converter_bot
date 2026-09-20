"""Splitting the media the user just sent: fetch it, do not refuse it.

A media the bot receives is registered *lazily* - the upload is not downloaded at
all until an action needs the bytes. The splitter resolved its source from disk or
from the object already in storage, so the very first split of a fresh upload was
answered with "the source file is no longer on disk" and the button looked broken
for every new task. These tests drive the real handler: the fetch happens and the
parts come out, a stored object is used without re-downloading the media, and a
fetch that genuinely fails says why it did.
"""

import asyncio
import os
import tempfile
import unittest
from types import MethodType, SimpleNamespace
from unittest.mock import patch

import handlers as handlers_module
from handlers import EnhancedMediaHandler
from tasks import conversion_tasks

Handler = EnhancedMediaHandler


class _FakeSentMessage:
    """What ``reply_text`` hands back: something the handler can edit or delete."""

    def __init__(self, text):
        self.text = text
        self.edited = []
        self.deleted = False

    async def edit_text(self, text, **kwargs):
        self.edited.append(text)
        return self

    async def delete(self):
        self.deleted = True


class _FakeMessage:
    def __init__(self, replies):
        self.replies = replies
        self.sent: list[_FakeSentMessage] = []

    async def reply_text(self, text, **kwargs):
        self.replies.append(text)
        message = _FakeSentMessage(text)
        self.sent.append(message)
        return message


class _FakeUpdate:
    def __init__(self, replies, chat_id=99):
        self.callback_query = None
        self.message = _FakeMessage(replies)
        self.effective_chat = SimpleNamespace(id=chat_id)
        self.effective_user = SimpleNamespace(id=7)


class _FakeContext:
    def __init__(self):
        self.bot = None
        self.user_data: dict = {}


class _FakeBackend:
    """Stands in for the storage backend: downloads write the object to disk."""

    def __init__(self, payload=b"stored source"):
        self.payload = payload
        self.downloads: list[str] = []

    async def download_file(self, key, dest):
        self.downloads.append(key)
        with open(dest, "wb") as fh:
            fh.write(self.payload)
        return True


class _SplitHandler:
    """The real split, with only the fetch and the delivery swapped out."""

    def __init__(self, downloaded_path):
        self.downloaded_path = downloaded_path
        self.fetches = 0
        self.delivered = None

    async def _ensure_current_file_downloaded(self, update, context, session):
        self.fetches += 1
        if self.downloaded_path is None:
            raise Exception("File too large (1200MB). Max allowed: 1000MB")
        session["current_file"]["path"] = self.downloaded_path

    async def _deliver_split_parts(self, update, context, current_file, parts, **kwargs):
        self.delivered = list(parts)
        return len(parts)


class _FakeConversionLimiter:
    """The per-user conversion limiter the handler looks up on the application."""

    def __init__(self, allowed=True, message="❌ Rate limit reached (360/360 per hour)"):
        self.allowed = allowed
        self.message = message
        self.checked: list[str] = []

    async def can_convert(self, user_id):
        self.checked.append(user_id)
        return self.allowed, self.message


class SplitQuotaTests(unittest.TestCase):
    """The split is metered like the trimmer: a rate-limited user gets no work done.

    Split is a cheap stream copy, which is why it used to skip `_check_conversion_
    quota` - but the quota is per *conversion*, and a user who has burned their hour
    on trims should not get an unlimited split on the side.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output_patch = patch.object(
            handlers_module,
            "config",
            SimpleNamespace(OUTPUT_PATH=self.tmp.name, TEMP_PATH=os.path.join(self.tmp.name, "temp")),
        )
        self.output_patch.start()

    def tearDown(self):
        self.output_patch.stop()
        self.tmp.cleanup()

    def _source(self):
        path = os.path.join(self.tmp.name, "Album.mp3")
        with open(path, "wb") as fh:
            fh.write(b"source")
        return path

    def _handler(self, source):
        """The real handler, minus the converter and the delivery it would need."""
        handler = EnhancedMediaHandler.__new__(EnhancedMediaHandler)
        handler._session_writes = set()
        handler.fetches = 0
        handler.delivered = None

        async def _ensure(update, context, session):
            handler.fetches += 1
            session["current_file"]["path"] = source

        async def _deliver(update, context, current_file, parts, **kwargs):
            handler.delivered = list(parts)
            return len(parts)

        handler._ensure_current_file_downloaded = _ensure
        handler._deliver_split_parts = _deliver
        return handler

    def _run(self, handler, limiter):
        replies: list[str] = []
        update = _FakeUpdate(replies)
        context = _FakeContext()
        context.application = SimpleNamespace(bot_data={"conversion_rate_limiter": limiter})
        session = {"current_file": {"id": "abc123", "name": "Album.mp3", "path": None, "type": "audio"}}

        async def _split(input_path, output_dir, segment_seconds, *, ext=".mp4", stem="part", source_meta=None):
            os.makedirs(output_dir, exist_ok=True)
            part = os.path.join(output_dir, f"{stem}.001{ext}")
            with open(part, "wb") as fh:
                fh.write(b"part")
            return True, [part], ""

        with patch.object(conversion_tasks, "split_media_segments", _split):
            asyncio.run(handler._handle_split_request(update, context, session, "10:00"))
        return replies

    def test_a_rate_limited_user_is_told_and_nothing_is_done(self):
        source = self._source()
        handler = self._handler(source)
        limiter = _FakeConversionLimiter(allowed=False)

        replies = self._run(handler, limiter)

        self.assertEqual(limiter.checked, ["7"], "the split did not ask the limiter")
        self.assertEqual(handler.fetches, 0, "a rate-limited user still triggered a download")
        self.assertIsNone(handler.delivered, "a rate-limited user still got the split")
        self.assertTrue(any("Rate limit reached" in text for text in replies), replies)

    def test_the_limiter_is_asked_before_the_source_is_fetched(self):
        """Order matters: no download, and no stream copy, for a refused request."""
        handler = self._handler(self._source())
        order: list[str] = []
        limiter = _FakeConversionLimiter(allowed=False)
        original = handler._ensure_current_file_downloaded

        async def _tracked(update, context, session):
            order.append("fetch")
            return await original(update, context, session)

        async def _can_convert(user_id):
            order.append("quota")
            return False, "❌ Rate limit reached (360/360 per hour)"

        handler._ensure_current_file_downloaded = _tracked
        limiter.can_convert = _can_convert

        self._run(handler, limiter)

        self.assertEqual(order, ["quota"], "the source was fetched before the quota was checked")

    def test_an_allowed_user_still_gets_the_split(self):
        source = self._source()
        handler = self._handler(source)
        limiter = _FakeConversionLimiter(allowed=True)

        self._run(handler, limiter)

        self.assertEqual(limiter.checked, ["7"])
        self.assertIsNotNone(handler.delivered)
        self.assertEqual(handler.fetches, 1)

    def test_without_a_limiter_the_split_still_runs(self):
        """No limiter configured is the fail-open case, exactly as for the trimmer."""
        handler = self._handler(self._source())
        replies: list[str] = []
        update = _FakeUpdate(replies)
        session = {"current_file": {"id": "abc123", "name": "Album.mp3", "path": None, "type": "audio"}}

        with patch.object(conversion_tasks, "split_media_segments", SplitLazySourceTests._fake_split()):
            asyncio.run(handler._handle_split_request(update, _FakeContext(), session, "10:00"))

        self.assertIsNotNone(handler.delivered)


class SplitSummaryTests(unittest.TestCase):
    """What the splitter tells the user, when a copy could not do what was asked.

    Stream copying can only cut on the source's keyframes, so a request can come
    back as one whole file (no keyframe anywhere near the target) or as longer parts
    than asked for. The reply used to explain the first case as "the file is shorter
    than one part", which is false for anything that is simply keyframe-sparse.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output_patch = patch.object(
            handlers_module,
            "config",
            SimpleNamespace(OUTPUT_PATH=self.tmp.name, TEMP_PATH=os.path.join(self.tmp.name, "temp")),
        )
        self.output_patch.start()

    def tearDown(self):
        self.output_patch.stop()
        self.tmp.cleanup()

    def _handler(self, parts_to_write):
        handler = EnhancedMediaHandler.__new__(EnhancedMediaHandler)
        handler._session_writes = set()
        handler.delivered = None
        source = os.path.join(self.tmp.name, "src.bin")
        with open(source, "wb") as fh:
            fh.write(b"source")

        async def _ensure(update, context, session):
            session["current_file"]["path"] = source

        async def _deliver(update, context, current_file, parts, **kwargs):
            handler.delivered = list(parts)
            return len(parts)

        handler._ensure_current_file_downloaded = _ensure
        handler._deliver_split_parts = _deliver
        handler._parts_to_write = parts_to_write
        return handler

    def _run(self, handler, duration, request):
        text: list[str] = []
        update = _FakeUpdate(text)
        session = {
            "current_file": {
                "id": "abc123",
                "name": "Long.mp4",
                "path": None,
                "type": "video",
                "_source_metadata": {"duration": duration},
            }
        }
        count = handler._parts_to_write

        async def _split(input_path, output_dir, segment_seconds, *, ext=".mp4", stem="part", source_meta=None):
            os.makedirs(output_dir, exist_ok=True)
            parts = []
            for index in range(1, count + 1):
                part = os.path.join(output_dir, f"{stem}.{index:03d}{ext}")
                with open(part, "wb") as fh:
                    fh.write(b"part")
                parts.append(part)
            return True, parts, ""

        with patch.object(conversion_tasks, "split_media_segments", _split):
            asyncio.run(handler._handle_split_request(update, _FakeContext(), session, request))
        # The summary is an edit of the "✂️ Splitting…" status message, so both the
        # replies and the edits of every message this run produced are the answer.
        return text + [edited for message in update.message.sent for edited in message.edited]

    def test_a_media_longer_than_the_part_length_is_not_called_short(self):
        """One part out of a 24s video for "5s parts" is a keyframe problem."""
        handler = self._handler(1)

        replies = self._run(handler, duration=24.0, request="5")

        summary = replies[-1]
        self.assertIn("keyframe", summary, replies)
        self.assertNotIn("shorter than one part", summary, replies)
        self.assertEqual(len(handler.delivered), 1)

    def test_a_media_that_really_is_shorter_than_one_part_still_says_so(self):
        handler = self._handler(1)

        replies = self._run(handler, duration=2.0, request="10:00")

        self.assertIn("shorter than one part", replies[-1], replies)

    def test_a_short_count_than_was_asked_for_says_why(self):
        """30s asked as 5 parts needs 6s cuts; three longer ones came back."""
        handler = self._handler(3)

        replies = self._run(handler, duration=30.0, request="5")

        summary = replies[-1]
        self.assertIn("Split into 3 parts", summary)
        self.assertIn("keyframe", summary, replies)

    def test_a_split_that_matched_the_request_explains_nothing(self):
        handler = self._handler(5)

        replies = self._run(handler, duration=30.0, request="5")

        summary = replies[-1]
        self.assertIn("Split into 5 parts", summary)
        self.assertNotIn("keyframe", summary)


class SplitLazySourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output_patch = patch.object(
            handlers_module,
            "config",
            SimpleNamespace(OUTPUT_PATH=self.tmp.name, TEMP_PATH=os.path.join(self.tmp.name, "temp")),
        )
        self.output_patch.start()

    def tearDown(self):
        self.output_patch.stop()
        self.tmp.cleanup()

    def _media_path(self, name="Album.mp3"):
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as fh:
            fh.write(b"source")
        return path

    def _handler(self, downloaded_path):
        handler = _SplitHandler(downloaded_path)
        for name in (
            "_split_local_source",
            "_handle_split_request",
            "_check_conversion_quota",
            "_split_source_duration",
        ):
            # Bind the real implementations to the stub: a plain function set as an
            # instance attribute does not become a bound method.
            setattr(handler, name, MethodType(getattr(Handler, name), handler))
        return handler

    @staticmethod
    def _fake_split(parts_to_write=2):
        """The task, minus ffmpeg: it writes numbered parts where asked.

        Parts are written with a realistic minimum size (1KB) so junk-detection
        logic that filters sub-KB tail parts does not interfere with this test.
        """

        async def _split(input_path, output_dir, segment_seconds, *, ext=".mp4", stem="part", source_meta=None):
            os.makedirs(output_dir, exist_ok=True)
            parts = [os.path.join(output_dir, f"{stem}.{index:03d}{ext}") for index in range(1, parts_to_write + 1)]
            for part in parts:
                # 1KB: above the junk-detection threshold so the split mechanism can
                # be tested without the last-part filter kicking in.
                with open(part, "wb") as fh:
                    fh.write(b"\0" * 1024)
            return True, parts, ""

        return _split

    def _run(self, handler, current_file, request="10:00"):
        replies: list[str] = []
        update = _FakeUpdate(replies)
        session = {"current_file": current_file}
        with patch.object(conversion_tasks, "split_media_segments", self._fake_split()):
            asyncio.run(handler._handle_split_request(update, _FakeContext(), session, request))
        return replies

    # ── the bug ─────────────────────────────────────────────────────────────

    def test_a_freshly_sent_media_is_fetched_and_then_split(self):
        source = self._media_path()
        current_file = {"id": "abc123", "name": "Album.mp3", "path": None, "type": "audio"}
        handler = self._handler(source)

        replies = self._run(handler, current_file)

        self.assertEqual(handler.fetches, 1, "the uploaded media was never fetched")
        self.assertIsNotNone(handler.delivered, "the split delivered nothing")
        self.assertEqual(
            [os.path.basename(part) for part in handler.delivered],
            ["Album.001.mp3", "Album.002.mp3"],
        )
        self.assertFalse(
            any("could not get the source file" in text for text in replies),
            f"a fresh upload was refused instead of split: {replies}",
        )

    def test_a_failed_fetch_reports_its_own_reason(self):
        current_file = {"id": "abc123", "name": "Album.mp3", "path": None, "type": "audio"}
        handler = self._handler(None)

        replies = self._run(handler, current_file)

        self.assertEqual(handler.fetches, 1)
        self.assertIsNone(handler.delivered)
        refusal = next((text for text in replies if "could not get the source file" in text), "")
        self.assertIn("File too large", refusal, f"the reason for the failure was dropped: {replies}")

    # ── the paths that already worked ───────────────────────────────────────

    def test_a_local_copy_is_split_without_fetching_anything(self):
        source = self._media_path()
        current_file = {"id": "abc123", "name": "Album.mp3", "path": source, "type": "audio"}
        handler = self._handler(source)

        self._run(handler, current_file)

        self.assertEqual(handler.fetches, 0, "a media already on disk must not be fetched again")
        self.assertIsNotNone(handler.delivered)

    def test_a_stored_object_is_used_instead_of_a_new_download(self):
        backend = _FakeBackend()
        current_file = {
            "id": "abc123",
            "name": "Album.mp3",
            "path": None,
            "input_key": "inputs/lib/album",
            "type": "audio",
        }
        handler = self._handler(None)

        async def _get_backend():
            return backend

        with patch("utils.storage.get_storage_backend", _get_backend):
            self._run(handler, current_file)

        self.assertEqual(backend.downloads, ["inputs/lib/album"])
        self.assertEqual(handler.fetches, 0, "the stored object was there; Telegram must not be asked")
        self.assertEqual(
            [os.path.basename(part) for part in handler.delivered],
            ["Album.001.mp3", "Album.002.mp3"],
        )


if __name__ == "__main__":
    unittest.main()
