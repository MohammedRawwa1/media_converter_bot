"""A queued pipeline job is watched on one message, whichever update queued it.

A callback names that message: the one the user pressed. A *typed* value - the
custom-bitrate prompt, a bitrate typed for a video - names nothing, and the only
message about the job was the pipeline's own "queued for processing" notice. No
watcher was started for it, so that notice stayed in the chat for good and the
"✅ Large file queued" message posted next to it kept its ❌ Cancel button - for a
job that had already delivered the user's file. Both now become the single
message the watcher owns: it renders the live progress (with the 📊 Progress
button that opens the job's page in the web UI) and is deleted once the job
reaches a terminal state.
"""

import ast
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from source_helpers import call_keywords, find_function, parse_source
from telegram.error import BadRequest
from test_settings_custom_inputs import _MemorySettings

import handlers as handlers_module

JOB_ID = "934df9c2-0000-0000-0000-000000000000"
SUMMARY = f"✅ Large file queued (Job: {JOB_ID[:8]}...). I'll send the MP3 when ready."
QUOTED_NOTICE = "Large file (47 MB) queued for processing.\nJob: 934df9c2... You will receive the result shortly."


class FakeMessage:
    """A bot message: it records what was sent to it and what it sent back."""

    def __init__(self, text="", chat_id=7, message_id=11):
        self.text = text
        self.chat = SimpleNamespace(id=chat_id)
        self.chat_id = chat_id
        self.message_id = message_id
        self.edits = []
        self.replies = []
        self.sent = []
        self.deleted = False

    async def edit_text(self, text, **kwargs):
        self.text = text
        self.edits.append((text, kwargs))

    async def delete(self):
        self.deleted = True

    async def reply_text(self, text, **kwargs):
        message = FakeMessage(text, self.chat_id, self.message_id + 1)
        self.replies.append((text, kwargs))
        self.sent.append(message)
        return message


class _RecordingWatcher:
    """Records the watcher a handler starts, instead of polling Redis."""

    def __init__(self):
        self.watched = []
        self.edited = []

    def attach(self, handler):
        async def _watch(query, job_id, **kwargs):
            self.watched.append(SimpleNamespace(query=query, job_id=job_id, **kwargs))

        async def _safe_edit(query, text, **kwargs):
            self.edited.append((query, text, kwargs))
            return True

        handler._watch_job_progress = _watch
        handler.safe_edit = _safe_edit
        return handler


def _handler():
    handler = object.__new__(handlers_module.EnhancedMediaHandler)
    recorder = _RecordingWatcher()
    return recorder.attach(handler), recorder


def _update(message=None, query=None, user_id=7):
    return SimpleNamespace(
        message=message,
        callback_query=query,
        effective_user=SimpleNamespace(id=user_id),
        effective_chat=SimpleNamespace(id=user_id),
    )


class PipelineJobWatchTests(unittest.IsolatedAsyncioTestCase):
    async def _start(self, handler, update, *, message=None, notice=None, query=None, superseded=None):
        await handler._watch_pipeline_job(
            update,
            SimpleNamespace(bot=SimpleNamespace()),
            JOB_ID,
            SUMMARY,
            query=query,
            message=message,
            notice=notice,
            superseded=superseded,
        )
        # The watcher is started with create_task, so let it run.
        await asyncio.sleep(0)

    async def test_a_typed_value_watches_the_notice_instead_of_posting_a_second_message(self):
        handler, rec = _handler()
        notice = FakeMessage(QUOTED_NOTICE)
        typed = FakeMessage()  # the message the user typed their bitrate into

        await self._start(handler, _update(message=typed), message=typed, notice=notice)

        # Nothing is posted next to the notice: the job owns one message.
        self.assertEqual(typed.replies, [])
        # It says what the button-driven path says, with the Cancel button the
        # watcher keeps alive and then removes.
        self.assertEqual(notice.text, SUMMARY)
        self.assertEqual(
            notice.edits[-1][1]["reply_markup"].inline_keyboard[0][0].callback_data,
            f"cancel_job:{JOB_ID}",
        )
        self.assertEqual(len(rec.watched), 1)
        self.assertIsNone(rec.watched[0].query)
        self.assertEqual(rec.watched[0].job_id, JOB_ID)
        self.assertIs(rec.watched[0].progress_msg, notice)

    async def test_a_typed_value_with_no_notice_still_watches_what_it_posts(self):
        """Reached when the session already carried a job id from an earlier request."""
        handler, rec = _handler()
        typed = FakeMessage()

        await self._start(handler, _update(message=typed), message=typed, notice=None)

        self.assertEqual([text for text, _ in typed.replies], [SUMMARY])
        self.assertEqual(len(rec.watched), 1)
        self.assertIsNone(rec.watched[0].query)
        self.assertEqual(rec.watched[0].job_id, JOB_ID)
        # The message it just posted is the one it watches - and the one the
        # watcher deletes, so nothing is left behind.
        self.assertEqual(rec.watched[0].progress_msg.text, SUMMARY)

    async def test_a_callback_still_watches_the_message_the_user_pressed(self):
        handler, rec = _handler()
        pressed = FakeMessage()
        query = SimpleNamespace(message=pressed)

        await self._start(handler, _update(query=query), query=query)

        self.assertEqual([text for _, text, _ in rec.edited], [SUMMARY])
        self.assertEqual(len(rec.watched), 1)
        self.assertIs(rec.watched[0].query, query)
        self.assertFalse(hasattr(rec.watched[0], "progress_msg"))

    async def test_a_notice_telegram_refuses_to_edit_is_still_watched(self):
        """The relabelling is cosmetic; the watcher is what cleans the message up."""
        handler, rec = _handler()
        typed = FakeMessage()

        class RefusingMessage(FakeMessage):
            async def edit_text(self, text, **kwargs):
                raise BadRequest("Message can't be edited")

        notice = RefusingMessage(QUOTED_NOTICE)
        await self._start(handler, _update(message=typed), message=typed, notice=notice)

        self.assertEqual(typed.replies, [])
        self.assertEqual(len(rec.watched), 1)
        self.assertIs(rec.watched[0].progress_msg, notice)

    async def test_with_no_message_to_watch_on_nothing_is_started(self):
        handler, rec = _handler()

        await self._start(handler, _update(), message=None, notice=None)

        self.assertEqual(rec.watched, [])

    async def test_the_requests_own_acknowledgement_goes_once_the_job_is_watched(self):
        """It spoke for the request; the watched message speaks for the job."""
        handler, rec = _handler()
        typed = FakeMessage()
        notice = FakeMessage(QUOTED_NOTICE)
        ack = await typed.reply_text("🎚️ Setting bitrate to 32k...")

        await self._start(
            handler,
            _update(message=typed),
            message=typed,
            notice=notice,
            superseded=ack,
        )

        self.assertTrue(ack.deleted)
        self.assertEqual(len(rec.watched), 1)
        self.assertIs(rec.watched[0].progress_msg, notice)

    async def test_a_superseded_message_is_kept_when_there_is_nothing_to_watch(self):
        """With no job message to replace it, the acknowledgement is all the user has."""
        handler, rec = _handler()
        typed = FakeMessage()
        ack = await typed.reply_text("🎚️ Setting bitrate to 32k...")

        await self._start(handler, _update(message=typed), message=None, notice=None, superseded=ack)

        self.assertFalse(ack.deleted)
        self.assertEqual(rec.watched, [])


class TypedBitratePipelineTests(unittest.IsolatedAsyncioTestCase):
    """The typed custom-bitrate prompt takes the button workflow's route.

    ``adjust_bitrate`` keeps the bitrate it is given as the user's *setting*, so
    running the real method would write ``storage/user_settings.json`` - a file
    the repository does not track, created by the suite and left behind by it.
    The store stands in for memory instead.
    """

    def setUp(self):
        super().setUp()
        self.settings_patch = patch.object(handlers_module, "user_settings", _MemorySettings())
        self.settings_patch.start()

    def tearDown(self):
        self.settings_patch.stop()
        super().tearDown()

    def _handler(self, notice):
        handler, rec = _handler()
        handler.safe_edit = AsyncMock()

        async def _quota(*_args, **_kwargs):
            return True

        async def _ensure(update, context, session):
            session["current_file"]["_pipeline_job_id"] = JOB_ID
            return notice

        handler._check_conversion_quota = _quota
        handler._ensure_current_file_downloaded = _ensure
        return handler, rec

    @staticmethod
    def _session():
        return {
            "current_file": {
                "id": "CQACAgQAA",
                "name": "Module 02.mp3",
                "type": "audio",
                "file_unique_id": "uniq",
                "size": 49289926,
            }
        }

    async def test_the_typed_bitrate_hands_the_notice_to_the_watcher(self):
        notice = FakeMessage(QUOTED_NOTICE)
        typed = FakeMessage()
        handler, rec = self._handler(notice)

        with patch.object(handlers_module, "_already_at_bitrate", AsyncMock(return_value=None)):
            await handler.adjust_bitrate(
                _update(message=typed),
                SimpleNamespace(user_data={}, bot=SimpleNamespace()),
                self._session(),
                "32k",
            )
        await asyncio.sleep(0)

        # The acknowledgement was posted and then removed: the queued job's own
        # message is taken over, so there is no second notice beside it, no
        # stale acknowledgement of a request that is over, and no Cancel button
        # left holding a job that already delivered.
        self.assertEqual([text for text, _ in typed.replies], ["🎚️ Setting bitrate to 32k..."])
        self.assertTrue(typed.sent[0].deleted, "the acknowledgement must not outlive the request")
        self.assertEqual(notice.text, SUMMARY)
        self.assertEqual(len(rec.watched), 1)
        self.assertIsNone(rec.watched[0].query)
        self.assertEqual(rec.watched[0].job_id, JOB_ID)
        self.assertIs(rec.watched[0].progress_msg, notice)

    async def test_an_audio_already_at_the_bitrate_is_still_skipped_before_the_fetch(self):
        """The compare-validate gate keeps working ahead of the handoff.

        The acknowledgement is posted first - the check reads the media's header,
        which can go over the network - and the answer replaces it rather than
        arriving as a second line beside it.
        """
        handler, rec = self._handler(notice=None)
        typed = FakeMessage()

        with patch.object(handlers_module, "_already_at_bitrate", AsyncMock(return_value=64000)):
            await handler.adjust_bitrate(
                _update(message=typed),
                SimpleNamespace(user_data={}, bot=SimpleNamespace()),
                self._session(),
                "64k",
            )
        await asyncio.sleep(0)

        self.assertEqual(rec.watched, [])
        self.assertEqual([text for text, _ in typed.replies], ["🎚️ Setting bitrate to 64k..."])
        self.assertEqual(typed.sent[0].text, "ℹ️ Already 64k — nothing to re-encode.")
        self.assertEqual([text for text, _ in typed.sent[0].edits], ["ℹ️ Already 64k — nothing to re-encode."])


class _FakePubSub:
    async def subscribe(self, *_args, **_kwargs):
        return None

    async def get_message(self, **_kwargs):
        return None

    async def unsubscribe(self, *_args, **_kwargs):
        return None


class _FakeRedis:
    def __init__(self, job: dict):
        self.job = job

    def pubsub(self):
        return _FakePubSub()

    async def hgetall(self, _key):
        return dict(self.job)

    async def close(self):
        return None


class _Coalescer:
    """Never paces anything: the test's message has to land the first time."""

    def should_skip(self, *_args, **_kwargs):
        return False

    def shows(self, *_args, **_kwargs):
        return True

    def record(self, *_args, **_kwargs):
        return None


class _Gate:
    inline_max = 0

    def scope_for_chat(self, chat_id):
        return f"chat:{chat_id}"

    async def should_drop_inline(self, *_args, **_kwargs):
        return False

    async def note(self, *_args, **_kwargs):
        return 0


class RealWatcherTests(unittest.IsolatedAsyncioTestCase):
    """The real watcher, driven the way a typed request drives it.

    This is the end of the chain the handoff tests stop at: no callback query,
    one message, and a job that has reached a terminal state. What the user gets
    is live progress with the 📊 Progress button on it, and then no message at
    all - instead of a queued notice and a ❌ Cancel button they are left holding.
    """

    async def test_a_notice_becomes_live_progress_and_is_deleted_at_the_end(self):
        import os

        import utils.job_access as job_access
        import utils.job_queue as job_queue
        import utils.rate_limiter as rate_limiter

        handler = object.__new__(handlers_module.EnhancedMediaHandler)
        notice = FakeMessage(QUOTED_NOTICE)
        redis = _FakeRedis({b"status": b"done", b"progress": b"100", b"message": b"delivered"})

        with (
            patch.object(job_queue, "get_redis", AsyncMock(return_value=redis)),
            patch.object(rate_limiter, "telegram_edit_coalescer", _Coalescer()),
            patch.object(rate_limiter, "telegram_flood_gate", _Gate()),
            patch.object(job_access, "issue_job_token", AsyncMock(return_value="capability")),
            patch.dict(os.environ, {"WEBAPP_URL": "https://example.test/flask/upload"}),
        ):
            await handler._watch_job_progress(None, JOB_ID, progress_msg=notice, bot=SimpleNamespace())

        rendered = notice.edits[0][0]
        self.assertIn("done", rendered)
        keyboard = notice.edits[0][1]["reply_markup"].inline_keyboard[0]
        self.assertEqual(keyboard[1].callback_data, f"cancel_job:{JOB_ID}")
        # The web UI: the job's own page, authorized by its capability. The
        # upload suffix is stripped, so the Flask mount keeps its prefix.
        self.assertIn(f"/status/{JOB_ID}?", keyboard[0].url)
        self.assertTrue(keyboard[0].url.startswith("https://example.test/"))
        self.assertIn("capability", keyboard[0].url)
        self.assertTrue(notice.deleted, "the queued message must not outlive the job")

    async def test_a_finished_job_leaves_no_cancel_button_behind(self):
        import utils.job_queue as job_queue
        import utils.rate_limiter as rate_limiter

        handler = object.__new__(handlers_module.EnhancedMediaHandler)
        notice = FakeMessage(QUOTED_NOTICE)
        redis = _FakeRedis({b"status": b"error", b"progress": b"0", b"message": b"the source is missing"})

        with (
            patch.object(job_queue, "get_redis", AsyncMock(return_value=redis)),
            patch.object(rate_limiter, "telegram_edit_coalescer", _Coalescer()),
            patch.object(rate_limiter, "telegram_flood_gate", _Gate()),
        ):
            await handler._watch_job_progress(None, JOB_ID, progress_msg=notice, bot=SimpleNamespace())

        self.assertIn("error", notice.edits[-1][0])
        self.assertTrue(notice.deleted)


class QueuedWorkerJobWatchTests(unittest.IsolatedAsyncioTestCase):
    """The worker-queue path a typed bitrate request actually takes.

    ``adjust_bitrate`` answers a repeat out of the media cache - ``path`` is
    ``None`` and only ``input_key`` is set - so the job goes through the worker,
    and on a *typed* request that path posts the job's only message itself. No
    watcher was started for it: the "⏳ Queued format audio — job ..." line stayed
    in the chat, ❌ Cancel button and all, for a job that had already delivered
    the file - and pressing that button then reported a cancellation of work
    that was finished.
    """

    async def _enqueue(self, handler, typed, *, query=None, notify=None, superseded=None):
        with patch.object(handlers_module, "enqueue_job", AsyncMock()):
            return await handler._enqueue_worker_job(
                _update(message=typed, query=query),
                SimpleNamespace(bot=SimpleNamespace()),
                {"id": "CQACAgQAA", "name": "Module 02.mp3", "input_key": "inputs/library/abc/source"},
                output_path="storage/output/Module_02.mp3",
                ffmpeg_args=["-c:a", "libmp3lame", "-b:a", "64k"],
                output_ext=".mp3",
                job_type="format_audio",
                caption="Module 02",
                delivery_name="Module 02.mp3",
                query=query,
                notify=notify,
                superseded=superseded,
            )

    async def test_a_typed_request_watches_the_queued_message_it_posted(self):
        handler, rec = _handler()
        typed = FakeMessage()
        ack = await typed.reply_text("🎚️ Setting bitrate to 64k...")

        async def _notify(text, **kwargs):
            return await typed.reply_text(text, **kwargs)

        queued = await self._enqueue(handler, typed, notify=_notify, superseded=ack)
        await asyncio.sleep(0)

        self.assertTrue(queued)
        self.assertTrue(typed.sent[-1].text.startswith("⏳ Queued format audio"))
        self.assertEqual(len(rec.watched), 1)
        self.assertIsNone(rec.watched[0].query)
        # The message it posted is the one it watches - and the one the watcher
        # deletes, so nothing is left holding a Cancel button.
        self.assertIs(rec.watched[0].progress_msg, typed.sent[-1])
        button = typed.replies[-1][1]["reply_markup"].inline_keyboard[0][0].callback_data
        self.assertEqual(rec.watched[0].job_id, button.split(":", 1)[1])
        # The request's own acknowledgement goes with the request.
        self.assertTrue(ack.deleted)

    async def test_a_callback_still_watches_the_message_the_user_pressed(self):
        handler, rec = _handler()
        pressed = FakeMessage()
        query = SimpleNamespace(message=pressed)

        await self._enqueue(handler, pressed, query=query)
        await asyncio.sleep(0)

        self.assertEqual(len(rec.edited), 1)
        self.assertTrue(rec.edited[0][1].startswith("⏳ Queued format audio"))
        self.assertEqual(len(rec.watched), 1)
        self.assertIs(rec.watched[0].query, query)
        self.assertFalse(hasattr(rec.watched[0], "progress_msg"))

    async def test_a_queued_message_that_cannot_be_edited_is_not_watched(self):
        """A ``notify`` that reports through the callback leaves nothing to watch."""
        handler, rec = _handler()
        typed = FakeMessage()

        async def _notify(text, **kwargs):
            return True

        await self._enqueue(handler, typed, notify=_notify)
        await asyncio.sleep(0)

        self.assertEqual(rec.watched, [])


class TypedBitrateWiringTests(unittest.TestCase):
    """Both typed-bitrate entry points must thread the notice through."""

    def test_the_typed_bitrate_handlers_pass_the_notice_to_the_watcher(self):
        notices = call_keywords(parse_source("handlers.py"), "_watch_pipeline_job", "notice")
        self.assertGreaterEqual(
            len(notices),
            2,
            "adjust_bitrate and convert_to_mp3 both have a typed trigger and must hand over the notice",
        )
        self.assertEqual(set(notices), {"_pipeline_notice"})

    def test_the_queued_notice_is_handed_back_to_the_caller(self):
        body = find_function(parse_source("handlers.py"), "_ensure_current_file_downloaded")
        returned = [
            node.value.id
            for node in ast.walk(body)
            if isinstance(node, ast.Return) and isinstance(node.value, ast.Name)
        ]
        self.assertIn("_queued_message", returned)
