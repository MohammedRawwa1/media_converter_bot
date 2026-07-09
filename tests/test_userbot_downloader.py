import unittest
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
            result = await mod.download_forward_via_userbot(123, 456, "/tmp/file")

        self.assertTrue(result)
        pyrogram_mock.assert_awaited_once()
        telethon_mock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
