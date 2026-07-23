import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from utils import userbot_downloader as mod


class UserbotDownloaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_prefers_pyrogram_when_session_string_is_configured(self):
        pyrogram_mock = AsyncMock(return_value=True)
        telethon_mock = AsyncMock(return_value=False)

        with patch.object(mod, "_download_with_pyrogram", pyrogram_mock), patch.object(
            mod, "_download_with_telethon", telethon_mock
        ), patch("utils.userbot_downloader.PyrogramClient", object()), patch(
            "utils.telethon_session.get_pyrogram_session_string", return_value="session-string"
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


if __name__ == "__main__":
    unittest.main()
