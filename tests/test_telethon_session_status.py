import asyncio
import os
from unittest.mock import patch

from utils import telethon_session


class FakeDbModel:
    def __init__(self, payload):
        self.payload = payload

    async def load_session(self, user_id):
        return self.payload


def test_async_telethon_status_uses_mongodb_session(monkeypatch):
    monkeypatch.delenv("API_SESSION", raising=False)
    monkeypatch.delenv("SESSION", raising=False)
    monkeypatch.delenv("TELETHON_SESSION", raising=False)
    monkeypatch.delenv("USERBOT_SESSION", raising=False)
    monkeypatch.delenv("TELETHON_SESSION_NAME", raising=False)
    monkeypatch.delenv("API_SESSION_NAME", raising=False)
    monkeypatch.delenv("SESSION_NAME", raising=False)
    monkeypatch.delenv("USERBOT_SESSION_NAME", raising=False)

    monkeypatch.setattr(telethon_session, "TelegramClient", object)

    with patch("os.path.exists", return_value=False):
        status = asyncio.run(
            telethon_session.get_telethon_session_status(user_id=42, db_model=FakeDbModel({"string_session": "abc"}))
        )

    assert status["ready"] is True
    assert status["source"] == "mongodb"


def test_get_telethon_session_string_for_user_uses_mongodb(monkeypatch):
    monkeypatch.delenv("API_SESSION", raising=False)
    monkeypatch.delenv("SESSION", raising=False)
    monkeypatch.delenv("TELETHON_SESSION", raising=False)
    monkeypatch.delenv("USERBOT_SESSION", raising=False)

    session_str = asyncio.run(
        telethon_session.get_telethon_session_string_for_user(user_id=42, db_model=FakeDbModel({"string_session": "abc"}))
    )

    assert session_str == "abc"
