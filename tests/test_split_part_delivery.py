"""The large split part: sent as the requester, the way they asked for it.

A part that exceeds the Bot API limit goes out over MTProto instead, and that path
carried two per-user answers it had dropped: whose session sends it, and whether a
video belongs in the file view. The rest of the pipeline passes both - the worker
sends with ``user_id=job["user_id"]`` and honours ``upload_mode`` - so a very large
split part used to arrive from whichever account was configured first, as playable
media, for a user who had asked for documents.
"""

import asyncio
import os
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import handlers as handlers_module  # noqa: E402
from handlers import EnhancedMediaHandler  # noqa: E402

Handler = EnhancedMediaHandler


def _run(coro):
    return asyncio.run(coro)


class _DeliveryHandler(Handler):
    """The real handler, built without its ``__init__``: delivery needs no state.

    A subclass rather than a stand-in, so the real ``_deliver_split_parts`` really
    does call the real ``_send_video_result``/``_send_part_via_userbot`` - those are
    looked up on the instance, and a shim holding its own copies would silently keep
    calling the unstubbed originals.
    """

    def __init__(self):  # noqa: D107 - the parent's constructor wires the whole bot
        pass


class _FakeSentMessage:
    def __init__(self, text):
        self.text = text

    async def edit_text(self, text, **kwargs):
        self.text = text
        return self

    async def delete(self):
        return None


class _FakeUpdate:
    def __init__(self, chat_id=99, user_id=7):
        self.effective_chat = SimpleNamespace(id=chat_id)
        self.effective_user = SimpleNamespace(id=user_id)
        self.message = SimpleNamespace(reply_text=self._reply, sent=[])

    async def _reply(self, text, **kwargs):
        message = _FakeSentMessage(text)
        self.message.sent.append(message)
        return message


class _FakeSession:
    def __init__(self):
        self.messages: list[str] = []

    async def edit_text(self, text, **kwargs):
        self.messages.append(text)
        return self


class _UploadRecorder:
    def __init__(self, msg_id=11):
        self.msg_id = msg_id
        self.calls: list[dict] = []

    async def __call__(self, *args, **kwargs):
        self.calls.append({"args": args, "kwargs": kwargs})
        return self.msg_id


@pytest.fixture
def workdir():
    with tempfile.TemporaryDirectory() as tmp:
        yield tmp


def _big_part(workdir, name="Concert.001.mp4", size=4096):
    path = os.path.join(workdir, name)
    with open(path, "wb") as fh:
        fh.write(b"x" * size)
    return path


def _config(**overrides):
    values = {"BOT_API_MAX_BYTES": 1024, "ENABLE_USERBOT": True}
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.parametrize("upload_mode", ["file", "video"])
def test_a_large_part_is_sent_by_the_requester_and_the_way_they_asked(workdir, upload_mode):
    """Both per-user answers travel: whose session, and the upload preference."""
    part = _big_part(workdir)
    handler = _DeliveryHandler()
    recorded: list[dict] = []

    async def _send(self, chat_id, file_path, caption, delivery_name, as_audio, current_file, label, **kwargs):
        recorded.append({"chat_id": chat_id, "label": label, **kwargs})
        return True

    with (
        patch.object(handlers_module, "config", _config()),
        patch.object(handlers_module, "_user_upload_mode", lambda user_id: upload_mode),
        patch.object(Handler, "_send_part_via_userbot", _send),
    ):
        sent = _run(
            handler._deliver_split_parts(_FakeUpdate(), SimpleNamespace(bot=None), {"name": "Concert.mp4"}, [part])
        )

    assert sent == 1
    assert recorded[0]["user_id"] == 7
    # Audio is never a document; a video follows /usersettings.
    assert recorded[0]["as_document"] is (upload_mode == "file")


def test_an_audio_part_is_never_forced_into_the_document_view(workdir):
    part = _big_part(workdir, "Track.001.mp3")
    handler = _DeliveryHandler()
    recorded: list[dict] = []

    async def _send(self, chat_id, file_path, caption, delivery_name, as_audio, current_file, label, **kwargs):
        recorded.append({"as_audio": as_audio, **kwargs})
        return True

    with (
        patch.object(handlers_module, "config", _config()),
        patch.object(handlers_module, "_user_upload_mode", lambda user_id: "file"),
        patch.object(Handler, "_send_part_via_userbot", _send),
    ):
        _run(
            handler._deliver_split_parts(
                _FakeUpdate(), SimpleNamespace(bot=None), {"name": "Track.mp3"}, [part], as_audio=True
            )
        )

    assert recorded[0]["as_audio"] is True
    assert recorded[0]["as_document"] is False


def test_a_small_part_stays_on_the_bot_api(workdir):
    """Only the parts the Bot API cannot deliver take the MTProto path at all."""
    part = _big_part(workdir, size=16)
    handler = _DeliveryHandler()
    delivered: list[str] = []

    async def _send_video(self, bot, chat_id, file_path, **kwargs):
        delivered.append(file_path)

    with (
        patch.object(handlers_module, "config", _config()),
        patch.object(handlers_module, "_user_upload_mode", lambda user_id: "video"),
        patch.object(Handler, "_send_video_result", _send_video),
    ):
        sent = _run(
            handler._deliver_split_parts(_FakeUpdate(), SimpleNamespace(bot=None), {"name": "Concert.mp4"}, [part])
        )

    assert sent == 1
    assert delivered == [part]


def test_the_userbot_send_carries_the_session_and_the_upload_mode(workdir):
    """What the delivery hands over is what the uploader is asked for."""
    part = _big_part(workdir)
    recorder = _UploadRecorder()

    with patch("utils.userbot_uploader.send_file_via_userbot", recorder, create=True):
        ok = _run(
            _DeliveryHandler()._send_part_via_userbot(
                99,
                part,
                "caption",
                "Concert.001.mp4",
                True,
                {"name": "Concert.mp4"},
                "part 1/2",
                user_id=7,
                as_document=True,
            )
        )

    assert ok is True
    assert recorder.calls[0]["kwargs"]["user_id"] == 7
    # Audio: the player is the point, so the preference cannot turn it into a file.
    assert recorder.calls[0]["kwargs"]["as_document"] is False


def test_the_userbot_send_keeps_a_video_in_the_requested_view(workdir):
    part = _big_part(workdir)
    recorder = _UploadRecorder()

    with patch("utils.userbot_uploader.send_file_via_userbot", recorder, create=True):
        ok = _run(
            _DeliveryHandler()._send_part_via_userbot(
                99,
                part,
                "caption",
                "Concert.001.mp4",
                False,
                {"name": "Concert.mp4"},
                "part 1/2",
                user_id=None,
                as_document=True,
            )
        )

    assert ok is True
    assert recorder.calls[0]["kwargs"]["as_document"] is True
    # No session of its own is the uploader's fallback to make, not a pass-through.
    assert recorder.calls[0]["kwargs"]["user_id"] is None
