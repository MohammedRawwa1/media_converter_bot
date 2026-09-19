import asyncio
import contextlib
import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from source_helpers import read_source

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


class ScanCandidateTests(unittest.TestCase):
    """A scan must not hand back a *different* message's media.

    The date and recent-history scans walk the chat and take the first thing with
    media near the requested instant - and near a forwarded audio that can be a
    photo the user sent in the same minute. A photo written into the audio's own
    path satisfied every check the fallback had (a file exists; ffprobe can read
    it), so the download was reported as a success, the chat got "✅ Download
    complete!", and the conversion that followed failed on a source with no audio
    track. These pin the gate that replaced that.
    """

    def _audio_attr(self):
        return type("DocumentAttributeAudio", (), {"voice": False})()

    def test_a_photo_is_never_the_requested_file(self):
        photo = SimpleNamespace(id=1, media=object(), photo=SimpleNamespace(id="p"))
        self.assertFalse(mod._scan_candidate_matches(photo, expected_size=49_000_000, want_audio=True))

    def test_a_file_of_another_size_is_refused(self):
        small = SimpleNamespace(id=2, media=object(), document=SimpleNamespace(size=100_000))
        self.assertFalse(mod._scan_candidate_matches(small, expected_size=49_000_000, want_audio=True))

    def test_the_requested_audio_is_accepted(self):
        audio = SimpleNamespace(
            id=3,
            media=object(),
            audio=SimpleNamespace(size=49_289_926),
            document=SimpleNamespace(size=49_289_926, attributes=[self._audio_attr()]),
        )
        self.assertTrue(mod._scan_candidate_matches(audio, expected_size=49_289_926, want_audio=True))

    def test_a_video_is_not_the_audio_that_was_asked_for(self):
        video = SimpleNamespace(
            id=4, media=object(), video=SimpleNamespace(size=49_289_926), document=SimpleNamespace(size=49_289_926)
        )
        self.assertFalse(mod._scan_candidate_matches(video, expected_size=49_289_926, want_audio=True))
        # The same message is a perfectly good answer when audio was not requested.
        self.assertTrue(mod._scan_candidate_matches(video, expected_size=49_289_926, want_audio=False))

    def test_a_document_carrying_audio_counts_as_an_audio(self):
        sent_as_file = SimpleNamespace(
            id=5,
            media=object(),
            document=SimpleNamespace(size=49_289_926, attributes=[self._audio_attr()]),
        )
        self.assertTrue(mod._scan_candidate_matches(sent_as_file, expected_size=49_289_926, want_audio=True))

    def test_no_expectation_means_no_size_constraint(self):
        anything = SimpleNamespace(id=6, media=object(), document=SimpleNamespace(size=12))
        self.assertTrue(mod._scan_candidate_matches(anything))
        self.assertFalse(mod._scan_candidate_matches(SimpleNamespace(id=7, media=object()), expected_size=99))

    def test_both_scans_go_through_the_gate(self):
        src = read_source("utils", "userbot_downloader.py")
        self.assertEqual(src.count("_scan_candidate_matches(m"), 3)


class ScanSkipsTheWrongMediaTests(unittest.IsolatedAsyncioTestCase):
    """End to end through the date scan: the photo is passed over, the audio taken."""

    async def test_the_scan_skips_a_nearby_photo_and_downloads_the_audio(self):
        photo = SimpleNamespace(id=1, media=object(), photo=SimpleNamespace(id="p"))
        audio = SimpleNamespace(
            id=2,
            media=object(),
            audio=SimpleNamespace(size=1_048_576),
            document=SimpleNamespace(size=1_048_576, attributes=[type("DocumentAttributeAudio", (), {})()]),
        )
        downloaded: list = []

        class _Client:
            async def start(self):
                return self

            async def disconnect(self):
                return None

            def iter_messages(self, target, **kwargs):
                async def _gen():
                    for message in (photo, audio):
                        yield message

                return _gen()

            async def download_media(self, message, file=None, **kwargs):
                downloaded.append(message.id)
                with open(file, "wb") as fh:
                    fh.write(b"x" * 1_048_576)
                return file

        with tempfile.TemporaryDirectory() as tmp:
            dest = os.path.join(tmp, "Module_02.mp3")
            with (
                patch.object(mod, "TelegramClient", object()),
                # Imported inside the function, so the session module is what has
                # to be patched (the module attribute is not on the downloader).
                patch("utils.telethon_session.build_telethon_client", lambda *a, **k: _Client()),
                patch(
                    "utils.telethon_session.get_telethon_session_string_for_user",
                    new=AsyncMock(return_value=None),
                ),
                patch.object(mod, "_normalize_target", AsyncMock(return_value=1405333465)),
                patch.object(mod, "_resolve_message_via_telethon", AsyncMock(return_value=None)),
                patch.object(mod, "_ffprobe_ok", AsyncMock(return_value=True)),
                patch("utils.telethon_session.get_db_model", return_value=None),
                patch(
                    "utils.telethon_session.get_userbot_credentials",
                    return_value=(1, "hash"),
                ),
            ):
                ok = await mod._download_with_telethon(
                    1405333465,
                    4460,
                    dest,
                    msg_date="2026-09-19T20:41:42+00:00",
                    expected_size=1_048_576,
                    want_audio=True,
                )

            self.assertTrue(ok)
            self.assertEqual(downloaded, [2], "the photo should never have been downloaded")
            self.assertTrue(os.path.exists(dest))


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
        source = read_source("utils", "userbot_downloader.py")

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

            result = await mod._wait_download_or_stall(asyncio.create_task(crawling_download()), watch)

        self.assertEqual(result, "crawled.mp4")

    async def test_a_silent_download_is_cancelled(self):
        """The other half: no bytes for the whole window is a real stall."""
        with _stall_window(stall=0.2, poll=0.05):
            watch = mod._ProgressWatch()

            async def silent_download():
                await asyncio.sleep(30)
                return "never.mp4"

            with self.assertRaises(TimeoutError):
                await mod._wait_download_or_stall(asyncio.create_task(silent_download()), watch)


class HeadReadTests(unittest.IsolatedAsyncioTestCase):
    """Reading a media's first bytes instead of the whole of it.

    This is what lets a question about a media - what bitrate does it carry? - be
    answered before the 47MB download the question exists to avoid. It is asked
    as a shortcut, so every way it can fail has to come back as a plain ``False``
    that leaves the caller on the path it would have taken anyway.
    """

    class _FakeClient:
        def __init__(self, chunks):
            self.chunks = chunks
            self.requests = []
            self.started = False
            self.disconnected = False

        async def start(self):
            self.started = True

        async def disconnect(self):
            self.disconnected = True

        async def iter_download(self, msg, offset=0, limit=None):
            self.requests.append((offset, limit))
            for chunk in self.chunks:
                yield chunk

    @contextlib.asynccontextmanager
    async def _client(self, chunks):
        """A fake userbot whose client records what it was asked for."""
        client = self._FakeClient(chunks)
        with (
            patch.object(mod, "TelegramClient", object()),
            patch.object(mod, "_normalize_target", AsyncMock(return_value="target")),
            patch.object(mod, "_resolve_message_via_telethon", AsyncMock(return_value=[object()])),
            patch("utils.telethon_session.get_userbot_credentials", return_value=(1, "hash")),
            patch(
                "utils.telethon_session.get_telethon_session_string_for_user",
                new=AsyncMock(return_value="session"),
            ),
            patch("utils.telethon_session.get_db_model", return_value=None),
            patch("utils.telethon_session.build_telethon_client", return_value=client),
        ):
            yield client

    def setUp(self):
        """Every read writes inside this test's own directory, never the repo.

        The destination is a path the code under test creates, so a case that
        fails to clean up would otherwise leave a file in whatever directory the
        suite was started from - a stray, untracked file in the project root is a
        test bug, not a result.
        """
        self._dir = tempfile.mkdtemp(prefix="headread_")

    def tearDown(self):
        with contextlib.suppress(Exception):
            shutil.rmtree(self._dir, ignore_errors=True)

    def _dest(self, name="head.bin"):
        return os.path.join(self._dir, name)

    async def test_only_the_head_is_read_and_kept(self):
        chunks = [b"a" * 100, b"b" * 100, b"c" * 100]
        dest = self._dest()

        async with self._client(chunks) as client:
            ok = await mod.download_head_via_userbot(123, 456, dest, max_bytes=250, user_id=7)

        self.assertTrue(ok)
        with open(dest, "rb") as fh:
            body = fh.read()
        # Two whole chunks and a slice of the third: the transfer stops at the
        # limit rather than walking the rest of a file nobody asked for.
        self.assertEqual(body, (b"a" * 100) + (b"b" * 100) + (b"c" * 50))
        self.assertEqual(client.requests, [(0, 250)])
        self.assertTrue(client.disconnected, "the client must not be left running")
        self.assertFalse(os.path.exists(f"{dest}.part"), "the staging file must be gone")

    async def test_a_short_file_is_read_whole_and_still_answers_yes(self):
        dest = self._dest()

        async with self._client([b"x" * 10]):
            ok = await mod.download_head_via_userbot(123, 456, dest, max_bytes=4096)

        self.assertTrue(ok)
        self.assertEqual(os.path.getsize(dest), 10)

    async def test_no_userbot_configured_is_a_no_not_an_error(self):
        with patch.object(mod, "TelegramClient", None):
            self.assertFalse(await mod.download_head_via_userbot(123, 456, self._dest()))

    async def test_a_missing_credential_is_a_no(self):
        with patch("utils.telethon_session.get_userbot_credentials", side_effect=RuntimeError("unset")):
            self.assertFalse(await mod.download_head_via_userbot(123, 456, self._dest()))

    async def test_a_message_that_cannot_be_resolved_is_a_no(self):
        dest = self._dest()
        with (
            patch.object(mod, "TelegramClient", object()),
            patch.object(mod, "_normalize_target", AsyncMock(return_value="target")),
            patch.object(mod, "_resolve_message_via_telethon", AsyncMock(return_value=[])),
            patch("utils.telethon_session.get_userbot_credentials", return_value=(1, "hash")),
            patch("utils.telethon_session.get_telethon_session_string_for_user", new=AsyncMock(return_value=None)),
            patch("utils.telethon_session.get_db_model", return_value=None),
            patch("utils.telethon_session.build_telethon_client", return_value=self._FakeClient([b"x"])),
        ):
            ok = await mod.download_head_via_userbot(123, 456, dest)

        self.assertFalse(ok)
        self.assertFalse(os.path.exists(dest), "nothing may be left behind for a read that found no media")

    async def test_a_transport_failure_is_a_no(self):
        async def _boom(*args, **kwargs):
            raise RuntimeError("connection reset")

        with (
            patch.object(mod, "TelegramClient", object()),
            patch.object(mod, "_normalize_target", _boom),
            patch("utils.telethon_session.get_userbot_credentials", return_value=(1, "hash")),
            patch("utils.telethon_session.get_telethon_session_string_for_user", new=AsyncMock(return_value=None)),
            patch("utils.telethon_session.get_db_model", return_value=None),
            patch("utils.telethon_session.build_telethon_client", return_value=self._FakeClient([b"x"])),
        ):
            self.assertFalse(await mod.download_head_via_userbot(123, 456, self._dest()))

    async def test_a_read_that_hangs_is_abandoned_and_leaves_nothing(self):
        """The abandoned read must not leave the file its caller checks for."""
        client = AsyncMock()
        client.start = AsyncMock()
        client.disconnect = AsyncMock()

        async def _hanging(*args, **kwargs):
            await asyncio.sleep(30)
            yield b"never"

        client.iter_download = _hanging
        dest = self._dest()

        with (
            patch.object(mod, "TelegramClient", object()),
            patch.object(mod, "_normalize_target", AsyncMock(return_value="target")),
            patch.object(mod, "_resolve_message_via_telethon", AsyncMock(return_value=[object()])),
            patch("utils.telethon_session.get_userbot_credentials", return_value=(1, "hash")),
            patch("utils.telethon_session.get_telethon_session_string_for_user", new=AsyncMock(return_value=None)),
            patch("utils.telethon_session.get_db_model", return_value=None),
            patch("utils.telethon_session.build_telethon_client", return_value=client),
        ):
            ok = await mod.download_head_via_userbot(123, 456, dest, timeout=0.05)

        self.assertFalse(ok)
        self.assertFalse(os.path.exists(dest))
        self.assertFalse(os.path.exists(f"{dest}.part"))


if __name__ == "__main__":
    unittest.main()
