import asyncio
from unittest.mock import patch

import pytest

from utils import telethon_session


class FakeDbModel:
    def __init__(self, payload):
        self.payload = payload

    async def load_session(self, user_id):
        return self.payload


class FakeMergedDbModel(FakeDbModel):
    async def load_sessions(self, user_id):
        return {
            "telethon_session": "telethon-from-phone-a",
            "pyrogram_session": "pyro-from-phone-b",
        }


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
        telethon_session.get_telethon_session_string_for_user(
            user_id=42, db_model=FakeDbModel({"string_session": "abc"})
        )
    )

    assert session_str == "abc"


def test_mongodb_resolution_merges_client_sessions(monkeypatch, tmp_path):
    _reset_session_env(monkeypatch, tmp_path)

    telethon_value, _ = asyncio.run(
        telethon_session._resolve_telethon_session_with_source(
            user_id=42, db_model=FakeMergedDbModel({})
        )
    )
    pyrogram_value, _ = asyncio.run(
        telethon_session._resolve_pyrogram_session_with_source(
            user_id=42, db_model=FakeMergedDbModel({})
        )
    )

    assert telethon_value == "telethon-from-phone-a"
    assert pyrogram_value == "pyro-from-phone-b"


# ── Resolution order (fresh persisted session must beat a stale env var) ──
_SESSION_ENV_KEYS = (
    "API_SESSION",
    "SESSION",
    "api_session",
    "USERBOT_SESSION",
    "userbot_session",
    "TELETHON_SESSION",
    "telethon_session",
    "PYROGRAM_SESSION",
    "pyrogram_session",
    "USERBOT_PYROGRAM_SESSION",
    "userbot_pyrogram_session",
    "TELETHON_SESSION_NAME",
    "API_SESSION_NAME",
    "SESSION_NAME",
    "USERBOT_SESSION_NAME",
)


def _reset_session_env(monkeypatch, tmp_path):
    """Clear every session env var and point the session dir at a temp folder."""
    for key in _SESSION_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("TELETHON_SESSION_DIR", str(tmp_path))
    telethon_session._invalidate_session_cache()


def test_per_user_json_outranks_stale_env(monkeypatch, tmp_path):
    """A freshly logged-in per-user session beats a stale env session string."""
    _reset_session_env(monkeypatch, tmp_path)
    monkeypatch.setenv("API_SESSION", "stale-env-session")

    asyncio.run(
        telethon_session.save_session_string_to_file_async("fresh-json-session", client_type="telethon", user_id=42)
    )

    value, source = asyncio.run(
        telethon_session._resolve_telethon_session_with_source(user_id=42, db_model=None)
    )

    assert value == "fresh-json-session"
    assert source == "json"


def test_pyrogram_per_user_json_outranks_stale_env(monkeypatch, tmp_path):
    _reset_session_env(monkeypatch, tmp_path)
    monkeypatch.setenv("PYROGRAM_SESSION", "stale-pyro-env")

    asyncio.run(
        telethon_session.save_session_string_to_file_async("fresh-pyro", client_type="pyrogram", user_id=42)
    )

    value, source = asyncio.run(
        telethon_session._resolve_pyrogram_session_with_source(user_id=42, db_model=None)
    )

    assert value == "fresh-pyro"
    assert source == "json"


def test_mongodb_session_outranks_stale_env(monkeypatch, tmp_path):
    _reset_session_env(monkeypatch, tmp_path)
    monkeypatch.setenv("API_SESSION", "stale-env-session")

    value, source = asyncio.run(
        telethon_session._resolve_telethon_session_with_source(
            user_id=7, db_model=FakeDbModel({"telethon_session": "fresh-mongo"})
        )
    )

    assert value == "fresh-mongo"
    assert source == "mongodb"


def test_global_json_is_not_used_for_a_scoped_user(monkeypatch, tmp_path):
    """Per-user lookups must never fall through to the legacy global JSON file."""
    _reset_session_env(monkeypatch, tmp_path)

    asyncio.run(telethon_session.save_session_string_to_file_async("global-only", client_type="telethon"))
    telethon_session._invalidate_session_cache()

    value, source = asyncio.run(
        telethon_session._resolve_telethon_session_with_source(user_id=99, db_model=None)
    )
    assert value is None
    assert source == "missing"

    # The unscoped (admin/background) lookup still sees it.
    value, source = asyncio.run(telethon_session._resolve_telethon_session_with_source(db_model=None))
    assert value == "global-only"
    assert source == "global-json"


class _FakeCursor:
    def __init__(self, docs):
        self._docs = docs

    def sort(self, *args, **kwargs):
        return self

    async def to_list(self, length=None):
        return list(self._docs)


class _FakeCollection:
    def __init__(self, docs):
        self._docs = docs

    def find(self, query, projection):
        return _FakeCursor(self._docs)


class FakeMongoModel:
    bot_id = None

    def __init__(self, docs):
        self._sessions_coll = _FakeCollection(docs)


def test_restore_per_user_session_files_rewrites_json_from_mongodb(monkeypatch, tmp_path):
    """Sessions stored in MongoDB are re-materialized as per-user JSON files."""
    _reset_session_env(monkeypatch, tmp_path)

    model = FakeMongoModel(
        [
            {"user_id": 5, "session": {"telethon_session": "t5", "pyrogram_session": "p5"}},
            {"user_id": 6, "session": {"string_session": "legacy6"}},
            {"user_id": None, "session": {"telethon_session": "skipped"}},
            {"user_id": 5, "session": {}},
        ]
    )

    restored = asyncio.run(telethon_session.restore_per_user_session_files(model))
    assert restored == 2

    tele5, _ = asyncio.run(telethon_session._resolve_telethon_session_with_source(user_id=5, db_model=None))
    pyro5, _ = asyncio.run(telethon_session._resolve_pyrogram_session_with_source(user_id=5, db_model=None))
    tele6, _ = asyncio.run(telethon_session._resolve_telethon_session_with_source(user_id=6, db_model=None))

    assert tele5 == "t5"
    assert pyro5 == "p5"
    # Legacy documents that only carry ``string_session`` are restored too.
    assert tele6 == "legacy6"


def test_restore_per_user_session_files_without_model_is_noop():
    assert asyncio.run(telethon_session.restore_per_user_session_files(None)) == 0


def test_restore_per_user_session_files_merges_per_phone_sessions(monkeypatch, tmp_path):
    """Separate phone documents preserve both Telethon and Pyrogram sessions."""
    _reset_session_env(monkeypatch, tmp_path)

    model = FakeMongoModel(
        [
            {"user_id": 5, "session": {"pyrogram_session": "pyro-phone"}},
            {"user_id": 5, "session": {"telethon_session": "telethon-phone"}},
        ]
    )

    assert asyncio.run(telethon_session.restore_per_user_session_files(model)) == 1
    pyro, _ = asyncio.run(telethon_session._resolve_pyrogram_session_with_source(user_id=5, db_model=None))
    tele, _ = asyncio.run(
        telethon_session._resolve_telethon_session_with_source(user_id=5, db_model=None)
    )

    assert pyro == "pyro-phone"
    assert tele == "telethon-phone"


# ── Registered db_model (used by the downloader/uploader, which pass none) ──


@pytest.fixture(autouse=True)
def _reset_registered_db_model():
    """Keep the module-level registry from leaking between tests."""
    telethon_session.set_db_model(None)
    yield
    telethon_session.set_db_model(None)


def test_registered_db_model_covers_callers_that_pass_none(monkeypatch, tmp_path):
    _reset_session_env(monkeypatch, tmp_path)
    telethon_session.set_db_model(FakeDbModel({"telethon_session": "from-registry"}))

    value, source = asyncio.run(
        telethon_session._resolve_telethon_session_with_source(user_id=11, db_model=None)
    )

    assert (value, source) == ("from-registry", "mongodb")


def test_registered_db_model_is_used_for_pyrogram(monkeypatch, tmp_path):
    _reset_session_env(monkeypatch, tmp_path)
    telethon_session.set_db_model(FakeDbModel({"pyrogram_session": "pyro-registry"}))

    value, source = asyncio.run(
        telethon_session._resolve_pyrogram_session_with_source(user_id=11, db_model=None)
    )

    assert (value, source) == ("pyro-registry", "mongodb")


def test_has_usable_telethon_session_async_sees_mongodb(monkeypatch, tmp_path):
    """The sync gate cannot see MongoDB; the async variant must."""
    _reset_session_env(monkeypatch, tmp_path)
    monkeypatch.setattr(telethon_session, "TelegramClient", object)
    telethon_session.set_db_model(FakeDbModel({"telethon_session": "mongo-only"}))

    assert telethon_session.has_usable_telethon_session(user_id=11) is False
    assert asyncio.run(telethon_session.has_usable_telethon_session_async(user_id=11, db_model=None)) is True


def test_restore_does_not_clobber_a_fresher_local_session(monkeypatch, tmp_path):
    """A local JSON session must not be overwritten by an older MongoDB one."""
    _reset_session_env(monkeypatch, tmp_path)

    asyncio.run(telethon_session.save_session_string_to_file_async("local-fresh", client_type="telethon", user_id=8))

    model = FakeMongoModel([{"user_id": 8, "session": {"telethon_session": "mongo-old"}}])
    restored = asyncio.run(telethon_session.restore_per_user_session_files(model))
    assert restored == 0

    value, _ = asyncio.run(telethon_session._resolve_telethon_session_with_source(user_id=8, db_model=None))
    assert value == "local-fresh"
