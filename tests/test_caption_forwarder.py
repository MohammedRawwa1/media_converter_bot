"""The Caption Editor and the Media Forwarder: one caption, one re-send.

Both buttons exist to hand the same media back with different words on it, so
both go through ``_redeliver_current_media``: it checks where the media already
lives in object storage (metadata only), re-sends from Telegram's own copy when
it has one (no upload, no bucket read), falls back to a local copy or the stored
object, and takes the userbot path when the media is too big for the Bot API.

The caption is what used to be broken: the editor wrote ``current_file["caption"]``
and answered "saved", while every delivery built its caption from
``_metadata_caption`` - so the caption looked accepted and appeared nowhere.
"""

import asyncio
import os
import tempfile
import unittest
from unittest import mock

from source_helpers import read_source

import config
import handlers
from handlers import (
    EnhancedMediaHandler,
    _caption_prompt_text,
    _metadata_caption,
    _redelivery_name,
)
from utils.keyboard_utils import MediaMenuBuilder


class _FakeUser:
    def __init__(self, user_id=42):
        self.id = user_id


class _FakeChat:
    def __init__(self, chat_id=7):
        self.id = chat_id


class _FakeMessage:
    def __init__(self, text=None):
        self.text = text
        self.replies = []

    async def reply_text(self, text, **_kwargs):
        self.replies.append(text)
        return self


class _FakeBot:
    """Records the messages posted into the chat (no message was replied to)."""

    def __init__(self):
        self.sent = []

    async def send_message(self, chat_id=None, text=None, **kwargs):
        self.sent.append({"chat_id": chat_id, "text": text, **kwargs})
        return self


class _FakeQuery:
    def __init__(self, data=None):
        self.data = data
        self.message = None
        self.answers = []

    async def answer(self, *args, **kwargs):
        self.answers.append((args, kwargs))
        return True


class _FakeUpdate:
    def __init__(self, user_id=42, chat_id=7, text=None, data=None, has_message=True):
        self.callback_query = _FakeQuery(data)
        self.effective_user = _FakeUser(user_id)
        self.effective_chat = _FakeChat(chat_id)
        # A callback press carries no ``update.message`` - the reported internal
        # error was reading it - so a press is faked the same way.
        self.message = _FakeMessage(text) if has_message else None


class _FakeContext:
    def __init__(self, bot=None):
        self.user_data = {}
        self.bot = bot or _FakeBot()


def _handler(edits=None):
    handler = object.__new__(EnhancedMediaHandler)
    handler.user_sessions = {}
    handler._persist_session = lambda *_a, **_k: None
    return handler


def _events(handler, **overrides):
    """Stub the re-send's collaborators and record what each one was asked."""
    seen = {"adopted": [], "downloaded": [], "sends": [], "userbot": [], "tokens": [], "forgot": []}

    async def _adopt(current_file, **_kwargs):
        seen["adopted"].append(current_file.get("id"))
        return current_file.get("input_key")

    async def _download(current_file, key):
        seen["downloaded"].append(key)
        return overrides.get("download_result")

    async def _token(kind, **_kwargs):
        seen["tokens"].append(kind)
        return overrides.get("cached_id")

    async def _forget(kind, **_kwargs):
        seen["forgot"].append(kind)

    def _recorder(kind):
        async def _send(_bot, _chat_id, path, **kwargs):
            seen["sends"].append({"kind": kind, "path": path, "kwargs": kwargs})
            return overrides.get("send_result", "file-id")

        return _send

    async def _userbot(*args, **kwargs):
        seen["userbot"].append({"args": args, "kwargs": kwargs})
        return overrides.get("userbot_result", True)

    handler._adopt_stored_source = _adopt
    handler._download_stored_source = _download
    handler._get_cached_file_id = _token
    handler._forget_cached_file_id = _forget
    handler._send_video_result = _recorder("video")
    handler._send_audio_result = _recorder("audio")
    handler._send_document_result = _recorder("document")
    handler._send_photo_result = _recorder("photo")
    handler._send_part_via_userbot = _userbot
    handler._send_audio_via_userbot = _userbot
    return seen


class CaptionOverrideTests(unittest.TestCase):
    """One stored caption is what every delivery of that media carries."""

    def test_the_stored_caption_wins_over_the_media_tags(self):
        current = {"_source_metadata": {"title": "My Song", "performer": "Some Artist"}, "caption": "Buy now"}
        self.assertEqual(_metadata_caption(current), "Buy now")

    def test_without_one_the_tags_still_build_the_caption(self):
        current = {"_source_metadata": {"title": "My Song", "performer": "Some Artist"}}
        self.assertEqual(_metadata_caption(current), "My Song — Some Artist")

    def test_a_blank_stored_caption_is_not_a_caption(self):
        current = {"name": "clip.mp4", "caption": "   "}
        self.assertEqual(_metadata_caption(current), "clip")

    def test_the_caption_is_what_the_delivery_helpers_read(self):
        # The override has to reach the delivery sites through the one function
        # they all call, not through a field only the forwarder happened to read.
        src = read_source("handlers.py")
        self.assertIn('_override = str(info.get("caption") or "").strip()', src)


class CaptionPromptTests(unittest.TestCase):
    def test_the_prompt_shows_the_current_caption_copyably(self):
        prompt = _caption_prompt_text({"name": "clip.mp4", "caption": "A <b>caption"})
        self.assertIn("<code>A &lt;b&gt;caption</code>", prompt)
        self.assertIn("Send <code>-</code>", prompt)

    def test_the_prompt_falls_back_to_the_metadata_caption(self):
        prompt = _caption_prompt_text({"_source_metadata": {"title": "T", "performer": "P"}})
        self.assertIn("<code>T — P</code>", prompt)


class CaptionEditorPromptTests(unittest.TestCase):
    """Pressing 💬 asks for the caption and arms the one text answer."""

    def _press(self, session=None):
        edits = []
        handler = _handler()
        handler.user_sessions = {42: session or {"current_file": {"name": "clip.mp4"}}}

        async def _edit(_query, text, **kwargs):
            edits.append((text, kwargs))
            return True

        handler.safe_edit = _edit
        context = _FakeContext()
        update = _FakeUpdate(data="caption_editor")
        asyncio.run(handler.callback_handler(update, context))
        return handler, edits, context

    def test_it_shows_the_caption_prompt_armed_for_text(self):
        session = {"current_file": {"name": "clip.mp4", "caption": "Old words"}}
        _, edits, context = self._press(session)
        text, kwargs = edits[-1]
        self.assertIn("Caption Editor", text)
        self.assertIn("<code>Old words</code>", text)
        self.assertEqual(kwargs.get("parse_mode"), "HTML")
        self.assertTrue(context.user_data.get("awaiting_caption"))


class CaptionEditorInputTests(unittest.TestCase):
    """The typed caption is stored on the file *and* sent back with the media."""

    def _submit(self, session, typed, *, redelivered=None):
        handler = _handler()
        handler.user_sessions = {42: session}

        async def _no_settings(*_a, **_k):
            return False

        handler._handle_settings_prompt = _no_settings
        calls = []

        async def _redeliver(update, context, sess, **kwargs):
            calls.append(kwargs)
            session["_redelivered_caption"] = handlers._metadata_caption(sess.get("current_file"))
            return True

        handler._redeliver_current_media = _redeliver
        context = _FakeContext()
        context.user_data["awaiting_caption"] = True
        update = _FakeUpdate(text=typed)
        asyncio.run(handler.handle_custom_input(update, context))
        return handler, session, context, update, calls

    def test_a_typed_caption_is_stored_and_the_media_is_re_sent_with_it(self):
        session = {"current_file": {"name": "clip.mp4", "_source_metadata": {"title": "T"}}}
        _, session, context, update, calls = self._submit(session, "My new caption")
        self.assertEqual(session["current_file"]["caption"], "My new caption")
        # The pipeline reads this one on a repeat, so both carry the edit.
        self.assertEqual(session["current_file"]["_pipeline_caption"], "My new caption")
        # The re-send's caption is the new words, which is the whole proof.
        self.assertEqual(session["_redelivered_caption"], "My new caption")
        self.assertEqual(len(calls), 1)
        self.assertIn("carries it", calls[0]["success_note"])
        self.assertFalse(context.user_data.get("awaiting_caption"))

    def test_a_hyphen_removes_the_caption_and_re_sends_the_media_own(self):
        session = {"current_file": {"name": "clip.mp4", "caption": "Wrong words"}}
        _, session, _, _, calls = self._submit(session, "-")
        self.assertNotIn("caption", session["current_file"])
        self.assertNotIn("_pipeline_caption", session["current_file"])
        self.assertEqual(session["_redelivered_caption"], "clip")
        self.assertEqual(len(calls), 1)

    def test_an_empty_caption_keeps_the_prompt_open(self):
        session = {"current_file": {"name": "clip.mp4"}}
        _, session, context, update, calls = self._submit(session, "   ")
        self.assertNotIn("caption", session["current_file"])
        self.assertEqual(calls, [])
        self.assertTrue(context.user_data.get("awaiting_caption"))
        self.assertTrue(any("cannot be empty" in r for r in update.message.replies))


class RedeliverTests(unittest.TestCase):
    """``_redeliver_current_media``: stored check, token first, bytes second."""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="redeliver_")
        self._local = os.path.join(self._dir, "clip.mp4")
        with open(self._local, "wb") as fh:
            fh.write(b"v" * 64)

    def tearDown(self):
        import shutil

        shutil.rmtree(self._dir, ignore_errors=True)

    def _redeliver(self, current_file, **overrides):
        handler = _handler()
        seen = _events(handler, **overrides)
        session = {"current_file": current_file}
        update = _FakeUpdate()
        context = _FakeContext()
        with mock.patch.object(config, "BOT_API_MAX_BYTES", overrides.get("limit", 50 * 1024**2), create=True):
            ok = asyncio.run(handler._redeliver_current_media(update, context, session))
        return ok, seen, update

    def test_the_stored_object_is_checked_before_anything_else(self):
        current = {
            "id": "f1",
            "type": "video",
            "name": "clip.mp4",
            "path": self._local,
            "input_key": "inputs/f1/source",
        }
        ok, seen, _ = self._redeliver(current)
        self.assertTrue(ok)
        # One metadata-only check of where the media already lives, then the send.
        self.assertEqual(seen["adopted"], ["f1"])
        self.assertEqual(seen["sends"][0]["kind"], "video")
        self.assertEqual(seen["sends"][0]["path"], self._local)
        # The token lookup is what makes a repeated send free.
        self.assertEqual(seen["tokens"], ["video"])

    def test_a_media_only_in_the_bucket_is_fetched_once(self):
        current = {"id": "f1", "type": "video", "name": "clip.mp4", "input_key": "inputs/f1/source"}
        ok, seen, _ = self._redeliver(current, download_result=self._local)
        self.assertTrue(ok)
        self.assertEqual(seen["downloaded"], ["inputs/f1/source"])
        self.assertEqual(seen["sends"][0]["path"], self._local)

    def test_telegrams_own_copy_is_used_without_any_bucket_read(self):
        # No path and no stored key: Telegram's cached file_id is all there is,
        # and the delivery helpers are the ones that try it (they get "").
        current = {"id": "f1", "type": "audio", "name": "song.mp3", "file_unique_id": "uid1"}
        ok, seen, _ = self._redeliver(current, cached_id="the-file-id")
        self.assertTrue(ok)
        self.assertEqual(seen["downloaded"], [])
        send = seen["sends"][0]
        self.assertEqual(send["kind"], "audio")
        self.assertEqual(send["kwargs"]["file_unique_id"], "uid1")

    def test_nothing_readable_anywhere_says_so(self):
        current = {"id": "f1", "type": "video", "name": "clip.mp4"}
        ok, seen, update = self._redeliver(current)
        self.assertFalse(ok)
        self.assertEqual(seen["sends"], [])
        self.assertTrue(any("no local copy and no stored object" in r for r in update.message.replies))

    def test_an_over_limit_media_takes_the_userbot_path(self):
        current = {"id": "f1", "type": "video", "name": "clip.mp4", "path": self._local}
        handler = _handler()
        seen = _events(handler)
        with (
            mock.patch.object(config, "BOT_API_MAX_BYTES", 1, create=True),
            mock.patch.object(config, "ENABLE_USERBOT", True, create=True),
        ):
            ok = asyncio.run(handler._redeliver_current_media(_FakeUpdate(), _FakeContext(), {"current_file": current}))
        self.assertTrue(ok)
        self.assertEqual(seen["sends"], [])
        self.assertEqual(len(seen["userbot"]), 1)
        # The caption and the media's own name ride the userbot send.
        self.assertEqual(seen["userbot"][0]["args"][2], "clip")
        self.assertEqual(seen["userbot"][0]["args"][3], "clip.mp4")

    def test_the_caption_it_was_given_is_the_one_the_send_carries(self):
        current = {"id": "f1", "type": "document", "name": "notes.pdf", "path": os.path.join(self._dir, "notes.pdf")}
        with open(current["path"], "wb") as fh:
            fh.write(b"p" * 16)
        handler = _handler()
        seen = _events(handler)
        asyncio.run(
            handler._redeliver_current_media(
                _FakeUpdate(), _FakeContext(), {"current_file": current}, caption="typed words"
            )
        )
        self.assertEqual(seen["sends"][0]["kind"], "document")
        self.assertEqual(seen["sends"][0]["kwargs"]["caption"], "typed words")
        self.assertEqual(seen["sends"][0]["kwargs"]["filename"], "notes.pdf")

    def test_the_file_upload_preference_is_honoured_and_reported_honestly(self):
        # The video helper answers no file_id for a document-view send, so a
        # successful copy was being read as a failure. The preference picks the
        # helper instead, and the document send answers for real.
        current = {"id": "f1", "type": "video", "name": "clip.mp4", "path": self._local}
        handler = _handler()
        seen = _events(handler)
        with mock.patch.object(handlers, "_user_upload_mode", lambda _uid: "file"):
            ok = asyncio.run(handler._redeliver_current_media(_FakeUpdate(), _FakeContext(), {"current_file": current}))
        self.assertTrue(ok)
        self.assertEqual(seen["sends"][0]["kind"], "document")
        self.assertEqual(seen["sends"][0]["kwargs"]["filename"], "clip.mp4")

    def test_an_over_limit_document_tells_the_userbot_it_is_one(self):
        current = {"id": "f1", "type": "document", "name": "book.pdf", "path": self._local}
        handler = _handler()
        seen = _events(handler)
        with (
            mock.patch.object(config, "BOT_API_MAX_BYTES", 1, create=True),
            mock.patch.object(config, "ENABLE_USERBOT", True, create=True),
        ):
            ok = asyncio.run(handler._redeliver_current_media(_FakeUpdate(), _FakeContext(), {"current_file": current}))
        self.assertTrue(ok)
        self.assertEqual(seen["userbot"][0]["kwargs"]["as_document"], True)
        self.assertEqual(seen["userbot"][0]["kwargs"]["media_kind"], "document")

    def test_the_delivery_name_is_the_medias_own_whatever_this_host_calls_it(self):
        # Named after a temp path, the copy would arrive as local_src_f1.mp4.
        current = {"id": "f1", "type": "video", "name": "My Clip.mkv"}
        local = os.path.join("storage", "temp", "local_src_f1.mp4")
        self.assertEqual(_redelivery_name(current, "video", local), "My Clip.mkv")


class ForwarderTests(unittest.TestCase):
    """📤 is one tap: a cover re-forward into the chat the media was sent in."""

    def test_it_re_sends_without_asking_for_a_target(self):
        handler = _handler()
        handler.user_sessions = {42: {"current_file": {"name": "clip.mp4", "path": "x"}}}
        edits = []
        redelivered = []

        async def _edit(_query, text, **_kwargs):
            edits.append(text)
            return True

        async def _redeliver(update, context, session, **kwargs):
            redelivered.append(kwargs)
            return True

        handler.safe_edit = _edit
        handler._redeliver_current_media = _redeliver
        context = _FakeContext()
        asyncio.run(handler.callback_handler(_FakeUpdate(data="media_forwarder"), context))
        self.assertIn("from the bot", redelivered[0]["success_note"])
        # Nothing is armed: no prompt, no target to type.
        self.assertFalse(any(k.startswith("awaiting_") for k in context.user_data))
        self.assertEqual(len(redelivered), 1)
        self.assertIn("Re-sending", edits[-1])

    def test_the_target_prompt_is_gone_from_the_handler(self):
        src = read_source("handlers.py")
        self.assertNotIn("awaiting_forward_to", src)
        self.assertNotIn("Send target chat id", src)


class ReplyTargetTests(unittest.TestCase):
    """``_reply_to_press``: a reply when there is a message, a post when there is not."""

    def test_a_real_message_is_answered_with_a_reply(self):
        handler = _handler()
        update = _FakeUpdate(text="typed words")
        asyncio.run(handler._reply_to_press(update, _FakeContext(), "hello"))
        self.assertEqual(update.message.replies, ["hello"])

    def test_a_press_is_answered_in_the_chat_instead(self):
        handler = _handler()
        bot = _FakeBot()
        asyncio.run(handler._reply_to_press(_FakeUpdate(has_message=False), _FakeContext(bot), "hello"))
        self.assertEqual(bot.sent, [{"chat_id": 7, "text": "hello"}])


class ForwarderPressTests(unittest.TestCase):
    """📤 pressed on a callback: the media is re-sent *and* the note still lands."""

    def test_the_button_re_sends_without_an_update_message(self):
        current = {"id": "f1", "type": "document", "name": "notes.pdf", "file_unique_id": "uid1"}
        handler = _handler()
        session = {"current_file": current}
        handler.user_sessions = {42: session}
        seen = _events(handler, cached_id="the-file-id")
        bot = _FakeBot()
        edits = []

        async def _edit(_query, text, **_kwargs):
            edits.append(text)
            return True

        handler.safe_edit = _edit
        asyncio.run(handler.callback_handler(_FakeUpdate(data="media_forwarder", has_message=False), _FakeContext(bot)))

        # Telegram's own copy, re-sent: no upload and no bucket read.
        self.assertEqual(seen["sends"][0]["kind"], "document")
        self.assertEqual(seen["downloaded"], [])
        # The note arrives as a new message, because there was no message to reply
        # to. Reading ``update.message`` here is what raised AttributeError.
        self.assertTrue(any("from the bot" in note["text"] for note in bot.sent), bot.sent)
        self.assertIn("Re-sending", edits[-1])


class BatchForwardTests(unittest.TestCase):
    """📤 Forward Batch: every collected media, one summary, batch left alone."""

    @staticmethod
    def _batch_handler(entries, results=None):
        handler = _handler()
        session = {"bulk_list": entries, "current_file": entries[0] if entries else {}}
        handler.user_sessions = {42: session}
        calls = []
        outcomes = list(results or [])

        async def _redeliver(_update, _context, sess, **kwargs):
            calls.append({"entry": sess.get("current_file"), "kwargs": kwargs})
            return outcomes.pop(0) if outcomes else True

        handler._redeliver_current_media = _redeliver
        return handler, session, calls

    @staticmethod
    def _forward(handler, session):
        bot = _FakeBot()
        asyncio.run(handler.forward_batch(_FakeUpdate(has_message=False), _FakeContext(bot), session))
        return bot

    def test_every_entry_is_re_sent_once_and_the_batch_is_left_alone(self):
        entries = [
            {"id": "a", "name": "clip.mp4", "type": "video"},
            {"id": "b", "name": "song.mp3", "type": "audio"},
        ]
        handler, session, calls = self._batch_handler(entries)
        bot = self._forward(handler, session)
        self.assertEqual([call["entry"]["id"] for call in calls], ["a", "b"])
        # No per-file chatter: the batch reports itself once.
        self.assertTrue(all(call["kwargs"]["announce"] is False for call in calls))
        self.assertEqual(len(bot.sent), 1)
        self.assertIn("sent 2 of 2", bot.sent[0]["text"])
        # Forwarding is not consumption - the list is still there to apply.
        self.assertEqual([entry["id"] for entry in session["bulk_list"]], ["a", "b"])

    def test_the_one_summary_names_what_could_not_be_re_sent(self):
        entries = [{"id": "a", "name": "clip.mp4"}, {"id": "b", "name": "song.mp3"}]
        handler, session, _ = self._batch_handler(entries, results=[True, False])
        bot = self._forward(handler, session)
        self.assertIn("sent 1 of 2", bot.sent[0]["text"])
        self.assertIn("song.mp3", bot.sent[0]["text"])

    def test_the_loaded_file_is_forwarded_when_the_batch_is_empty(self):
        handler = _handler()
        session = {"current_file": {"id": "solo", "name": "solo.mp4", "type": "video"}}
        handler.user_sessions = {42: session}
        calls = []

        async def _redeliver(_update, _context, sess, **_kwargs):
            calls.append(sess.get("current_file"))
            return True

        handler._redeliver_current_media = _redeliver
        bot = self._forward(handler, session)
        self.assertEqual([call["id"] for call in calls], ["solo"])
        self.assertIn("sent 1 of 1", bot.sent[0]["text"])

    def test_an_empty_batch_says_so_instead_of_sending_nothing(self):
        handler = _handler()
        session = {}
        handler.user_sessions = {}
        bot = self._forward(handler, session)
        self.assertIn("batch is empty", bot.sent[0]["text"])


class MenuTests(unittest.TestCase):
    def test_the_button_names_only_what_it_does(self):
        markup = MediaMenuBuilder.get_main_menu("video")
        labels = [b.text for row in markup.inline_keyboard for b in row]
        self.assertIn("💬 Caption Editor", labels)
        self.assertNotIn("✏️ Caption And Buttons Editor", labels)
        self.assertIn("📤 Media Forwarder", labels)

    def test_the_caption_editor_is_reachable_for_audio_too(self):
        markup = MediaMenuBuilder.get_main_menu("audio")
        labels = [b.text for row in markup.inline_keyboard for b in row]
        self.assertIn("💬 Caption Editor", labels)
        self.assertIn("📤 Media Forwarder", labels)

    def test_the_batch_menu_offers_the_batch_forward(self):
        items = [b for row in MediaMenuBuilder.get_bulk_menu({}).inline_keyboard for b in row]
        self.assertIn("📤 Forward Batch", [b.text for b in items])
        self.assertIn("bulk_forward", [b.callback_data for b in items])


class WiringTests(unittest.TestCase):
    def test_both_buttons_dispatched_and_the_editor_arms_one_answer(self):
        src = read_source("handlers.py")
        self.assertIn('elif data == "caption_editor":', src)
        self.assertIn('elif data == "media_forwarder":', src)
        self.assertIn("_caption_prompt_text(current_file)", src)
        self.assertIn('context.user_data["awaiting_caption"] = True', src)

    def test_the_old_dead_write_is_gone(self):
        src = read_source("handlers.py")
        # It wrote a field no delivery read and said "saved".
        self.assertNotIn('session["current_file"]["caption"] = user_input', src)
        self.assertNotIn('"✅ Caption saved."', src)

    def test_one_re_send_serves_all_three_buttons(self):
        src = read_source("handlers.py")
        self.assertIn("_redeliver_current_media(", src)
        self.assertIn("success_note=_note", src)
        # The batch forward is the same re-send, once per collected entry.
        self.assertIn('elif data == "bulk_forward":', src)
        self.assertIn("await self.forward_batch(update, context, session)", src)
        # One re-send, not three implementations of it.
        self.assertEqual(src.count("async def _redeliver_current_media("), 1)

    def test_the_re_send_notes_never_read_update_message_directly(self):
        # That read is the reported internal error: a callback press has no
        # ``update.message``. The one helper answers both kinds of update.
        src = read_source("handlers.py")
        start = src.index("async def _redeliver_current_media(")
        end = src.index("async def forward_batch(")
        self.assertNotIn("update.message.reply_text", src[start:end])
        self.assertIn("async def _reply_to_press(", src)


if __name__ == "__main__":
    unittest.main()
