"""The one lazy-download guard every action button now calls.

Every button used to carry its own copy of the same few lines: look for a local
copy, fetch it the shared way, report the failure where the caller's message
lives. ``_ensure_local_media`` is that guard, once - these tests pin its three
outcomes: a media already on disk is left alone, a fetch that fails is reported
(and the caller told to stop), and a queued pipeline notice is handed back to the
caller that has to watch it.
"""

import asyncio
import os
import tempfile
import unittest
from types import MethodType

from handlers import EnhancedMediaHandler


class _StubHandler:
    """The real guard bound to a stub fetch, so no download happens."""

    def __init__(self, fetch=None):
        self.fetch = fetch
        self.fetches: list[dict] = []
        self.edits: list[str] = []

    async def _ensure_current_file_downloaded(self, update, context, session):
        self.fetches.append(dict(session.get("current_file") or {}))
        if self.fetch is None:
            return None
        return await self.fetch(update, context, session)

    async def safe_edit(self, query, text, **kwargs):
        self.edits.append(text)
        return None


def _guard(handler):
    """The real guard, bound to the stub (a plain function is not a method).

    The guard reads the local copy through the shared accessor, so that accessor
    is bound too - the stub would otherwise die on the guard's first line.
    """
    handler._local_copy = MethodType(EnhancedMediaHandler._local_copy, handler)
    return MethodType(EnhancedMediaHandler._ensure_local_media, handler)


class LocalMediaGuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _source(self):
        path = os.path.join(self.tmp.name, "song.mp3")
        with open(path, "wb") as fh:
            fh.write(b"source")
        return path

    def _run(self, handler, current_file, *, session=None, **kwargs):
        session = session if session is not None else {"current_file": current_file}
        return asyncio.run(_guard(handler)(None, None, session, current_file, **kwargs))

    def test_a_media_already_on_disk_is_not_fetched_again(self):
        source = self._source()
        handler = _StubHandler()
        file = {"id": "abc123", "name": "song.mp3", "path": source}

        current_file, notice = self._run(handler, file)

        self.assertEqual(handler.fetches, [], "a local media must not be fetched")
        self.assertIs(current_file, file)
        self.assertIsNone(notice)

    def test_a_vanished_local_path_is_fetched(self):
        """A path the session still names but the disk no longer has is a miss."""
        handler = _StubHandler()
        file = {"id": "abc123", "name": "song.mp3", "path": os.path.join(self.tmp.name, "gone.mp3")}

        self._run(handler, file)

        self.assertEqual(len(handler.fetches), 1)

    def test_a_failed_fetch_is_reported_and_stops_the_caller(self):
        async def _boom(*_args):
            raise Exception("File too large (1200MB). Max allowed: 1000MB")

        handler = _StubHandler(_boom)
        seen: list[str] = []

        async def _notify(text):
            seen.append(text)

        current_file, notice = self._run(handler, {"id": "abc123", "path": None}, notify=_notify)

        self.assertIsNone(current_file, "a failed fetch must stop the caller")
        self.assertIsNone(notice)
        self.assertEqual(seen, ["❌ Failed to download file: File too large (1200MB). Max allowed: 1000MB"])

    def test_a_failed_fetch_is_reported_on_the_callback_message(self):
        async def _boom(*_args):
            raise Exception("boom")

        handler = _StubHandler(_boom)

        current_file, _notice = self._run(handler, {"id": "abc123", "path": None}, query="the-query")

        self.assertIsNone(current_file)
        self.assertEqual(handler.edits, ["❌ Failed to download file: boom"])

    def test_a_queued_pipeline_notice_comes_back_with_the_refreshed_file(self):
        notice = object()
        refreshed = {"id": "abc123", "path": None, "_pipeline_job_id": "job-1"}
        session = {"current_file": {"id": "abc123", "name": "big.mp4", "path": None}}

        async def _fetch(update, context, sess):
            sess["current_file"] = refreshed
            return notice

        handler = _StubHandler(_fetch)

        current_file, returned_notice = self._run(handler, session["current_file"], session=session)

        self.assertIs(current_file, refreshed, "the caller has to see what the fetch wrote")
        self.assertIs(returned_notice, notice)

    def test_the_guard_reads_the_session_when_no_file_is_handed_in(self):
        session = {"current_file": {"id": "abc123", "path": None}}
        handler = _StubHandler()

        current_file, _notice = asyncio.run(_guard(handler)(None, None, session))

        self.assertEqual(current_file, session["current_file"])
        self.assertEqual(len(handler.fetches), 1)


if __name__ == "__main__":
    unittest.main()
