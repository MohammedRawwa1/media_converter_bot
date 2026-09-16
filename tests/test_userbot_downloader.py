import asyncio
import contextlib
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from utils import userbot_downloader as mod


@contextlib.contextmanager
def _stall_window(*, stall, poll):
    """Shrink the stall guard's timings for the duration of a test."""
    saved = (mod.DOWNLOAD_STALL_SECONDS, mod._STALL_POLL_SECONDS)
    mod.DOWNLOAD_STALL_SECONDS, mod._STALL_POLL_SECONDS = stall, poll
    try:
        yield
    finally:
        mod.DOWNLOAD_STALL_SECONDS, mod._STALL_POLL_SECONDS = saved


class UserbotDownloaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_prefers_pyrogram_when_session_string_is_configured(self):
        pyrogram_mock = AsyncMock(return_value=True)
        telethon_mock = AsyncMock(return_value=False)

        with (
            patch.object(mod, "_download_with_pyrogram", pyrogram_mock),
            patch.object(mod, "_download_with_telethon", telethon_mock),
            patch("utils.userbot_downloader.PyrogramClient", object()),
            patch(
                "utils.telethon_session.get_pyrogram_session_string_for_user",
                new=AsyncMock(return_value="session-string"),
            ),
        ):
            result = await mod.download_forward_via_userbot(123, 456, os.path.join(tempfile.gettempdir(), "test_file"))

        self.assertTrue(result)
        pyrogram_mock.assert_awaited_once()
        telethon_mock.assert_not_awaited()

    async def test_relay_fallback_retries_download_from_forwarded_message(self):
        forwarded_msg = SimpleNamespace(id=789, media=object())
        client = AsyncMock()
        client.forward_messages = AsyncMock(return_value=[forwarded_msg])
        client.get_messages = AsyncMock(return_value=[forwarded_msg])

        with patch.object(mod, "_download_and_ensure_path", AsyncMock(return_value=True)) as download_mock:
            result = await mod._try_relay_fallback(
                client,
                123,
                456,
                os.path.join(tempfile.gettempdir(), "test_file"),
                relay_chat_id=-100111,
                client_type="pyrogram",
            )

        self.assertTrue(result)
        client.forward_messages.assert_awaited_once()
        client.get_messages.assert_awaited_once()
        download_mock.assert_awaited_once()


    def test_the_telethon_path_feeds_the_stall_watch(self):
        """Telethon's progress callback is what proves a download is still alive.

        The watch only counts as activity what the callback it wraps reports, so a
        Telethon download that was never given one looked silent from byte one and
        was killed as "stalled" at DOWNLOAD_STALL_SECONDS while running perfectly
        well - and restarted from zero on the next attempt.
        """
        with open(mod.__file__, encoding="utf-8") as fh:
            source = fh.read()

        self.assertIn('"progress_callback": _watch.wrap(progress_callback)', source)
        # Once for the Pyrogram path, once for Telethon: every stall-guarded
        # download has to be wired to the watch, or the guard is a wall clock.
        self.assertEqual(source.count("_watch.wrap("), 2)

    async def test_a_download_that_keeps_reporting_is_never_called_stalled(self):
        """A slow transfer that is still receiving bytes must run to completion."""
        with _stall_window(stall=0.3, poll=0.05):
            watch = mod._ProgressWatch()
            progress = watch.wrap(None)

            async def crawling_download():
                # Much slower than the stall window, but never actually silent.
                for _ in range(8):
                    await asyncio.sleep(0.08)
                    progress(1, 1)
                return "crawled.mp4"

            result = await mod._wait_download_or_stall(
                asyncio.create_task(crawling_download()), watch
            )

        self.assertEqual(result, "crawled.mp4")

    async def test_a_silent_download_is_cancelled(self):
        """The other half: no bytes for the whole window is a real stall."""
        with _stall_window(stall=0.2, poll=0.05):
            watch = mod._ProgressWatch()

            async def silent_download():
                await asyncio.sleep(30)
                return "never.mp4"

            with self.assertRaises(TimeoutError):
                await mod._wait_download_or_stall(
                    asyncio.create_task(silent_download()), watch
                )


if __name__ == "__main__":
    unittest.main()
