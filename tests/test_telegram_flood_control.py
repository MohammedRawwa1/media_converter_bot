"""Tests for Telegram flood-control handling and progress-edit coalescing.

A long 429 ("Retry in 27565 seconds") used to be slept off inline, parking the
handler for hours and making the bot look dead. These cover the replacement: a
process-wide gate that records the window, handlers that drop the write instead
of blocking, an edit coalescer shared by the watchers that render onto one
message, and the userbot peer loop no longer stopping on a message whose media
is a link preview rather than a file.
"""

import asyncio
import os
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from source_helpers import read_source
from telegram.error import RetryAfter

from utils import rate_limiter
from utils import userbot_downloader as downloader

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class _FakeFloodRedis:
    """Minimal Redis stand-in for the gate: a string key with a TTL, plus eval.

    ``eval`` implements the extend-only semantics of the production Lua script
    (whose text is asserted separately), so a window set by one gate is what
    another gate sharing this client reads back.
    """

    def __init__(self):
        self.store: dict[str, float] = {}
        self.closed = 0
        self.calls = 0
        self.fail = False

    async def eval(self, script, numkeys, key, now, deadline):
        self.calls += 1
        if self.fail:
            raise ConnectionError("redis is down")
        current = self.store.get(key)
        if current is not None and current >= deadline:
            return int(round(current - now))
        self.store[key] = float(deadline)
        return int(round(deadline - now))

    async def get(self, key):
        self.calls += 1
        if self.fail:
            raise ConnectionError("redis is down")
        value = self.store.get(key)
        return None if value is None else str(value)

    async def close(self):
        self.closed += 1


class FloodGateTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.gate = rate_limiter.TelegramFloodGate()

    async def test_long_window_blocks_inline_and_stays_in_its_chat(self):
        scope = self.gate.scope_for_chat(-1004400932750)
        seconds = await self.gate.note(27565, scope)

        self.assertEqual(seconds, 27565)
        self.assertGreater(await self.gate.remaining(scope), 27000)
        self.assertTrue(await self.gate.should_drop_inline(scope))
        # One chat's penalty must not silence the rest of the bot.
        self.assertFalse(await self.gate.is_open(self.gate.scope_for_chat(-1009999999999)))
        self.assertFalse(await self.gate.is_open(self.gate.GLOBAL))

    async def test_short_window_does_not_block_inline(self):
        scope = self.gate.scope_for_chat(123)
        await self.gate.note(4, scope)

        self.assertFalse(await self.gate.should_drop_inline(scope))
        self.assertTrue(await self.gate.is_open(scope))

    async def test_note_only_extends_a_window(self):
        scope = self.gate.scope_for_chat(123)
        await self.gate.note(500, scope)
        await self.gate.note(10, scope)  # Telegram's countdown decays on every 429

        self.assertGreater(await self.gate.remaining(scope), 400)

    async def test_bogus_retry_after_is_ignored(self):
        scope = self.gate.scope_for_chat(123)
        self.assertEqual(await self.gate.note(None, scope), 0.0)
        self.assertEqual(await self.gate.note("nonsense", scope), 0.0)
        self.assertFalse(await self.gate.is_open(scope))

    async def test_watch_cap_is_never_exceeded(self):
        scope = self.gate.scope_for_chat(123)
        await self.gate.note(3600, scope)
        slept = []

        async def _fake_sleep(seconds):
            slept.append(seconds)

        with patch.object(rate_limiter.asyncio, "sleep", _fake_sleep):
            left = await self.gate.wait(scope)

        # A window far longer than the cap is reported, not slept.
        self.assertGreater(left, 3000)
        self.assertEqual(slept, [])

    async def test_detached_gate_makes_no_io(self):
        """Tests and scripts must never reach for Redis through the gate."""
        scope = self.gate.scope_for_chat(1)
        await self.gate.should_drop_inline(scope)
        await self.gate.note(60, scope)
        self.assertIsNone(self.gate._redis_factory)


class SharedFloodGateTests(unittest.IsolatedAsyncioTestCase):
    """The bot and the worker are separate containers sharing one Redis."""

    def setUp(self):
        self.redis = _FakeFloodRedis()

        async def _factory():
            return self.redis

        self.bot_gate = rate_limiter.TelegramFloodGate()
        self.worker_gate = rate_limiter.TelegramFloodGate()
        self.bot_gate.attach_redis(_factory)
        self.worker_gate.attach_redis(_factory)

    async def test_a_window_one_process_notes_stops_the_other(self):
        scope = self.bot_gate.scope_for_chat(-1004400932750)
        await self.bot_gate.note(27000, scope)

        self.assertTrue(await self.worker_gate.should_drop_inline(scope))
        self.assertGreater(await self.worker_gate.remaining(scope), 26000)

    async def test_shared_window_is_extend_only(self):
        scope = self.bot_gate.scope_for_chat(77)
        await self.bot_gate.note(600, scope)
        # The worker's decaying countdown must not shorten the bot's window.
        seconds = await self.worker_gate.note(30, scope)

        self.assertGreater(seconds, 500)
        self.assertGreater(await self.bot_gate.remaining(scope), 500)

    async def test_one_bot_namespace_does_not_leak_into_another(self):
        scope = self.bot_gate.scope_for_chat(5)
        with patch.dict("os.environ", {"BOT_TOKEN": "111:aaa"}):
            await self.bot_gate.note(600, scope)
            key = self.bot_gate._key(scope)
        with patch.dict("os.environ", {"BOT_TOKEN": "222:bbb"}):
            self.assertNotEqual(key, self.bot_gate._key(scope))

    async def test_unreachable_backend_degrades_to_local_and_backs_off(self):
        self.redis.fail = True
        scope = self.bot_gate.scope_for_chat(9)

        # The note is still recorded locally and nothing is raised.
        self.assertEqual(await self.bot_gate.note(120, scope), 120)
        self.assertTrue(await self.bot_gate.should_drop_inline(scope))

        # A dead Redis is not retried on every check.
        calls_after_failure = self.redis.calls
        for _ in range(5):
            await self.bot_gate.should_drop_inline(scope)
        self.assertEqual(self.redis.calls, calls_after_failure)

    async def test_bot_handler_sees_a_window_the_worker_noted(self):
        """End to end: a penalty earned in the worker stops the bot's edit path.

        This is the case the shared backend exists for - the two containers run
        the same bot token against the same chat, so a window one of them is
        serving has to hold the other off as well.
        """
        from handlers import EnhancedMediaHandler

        singleton = rate_limiter.telegram_flood_gate
        singleton.reset()
        singleton.attach_redis(self.worker_gate._redis_factory)
        try:
            scope = singleton.scope_for_chat(-1004400932750)
            await self.worker_gate.note(27565, scope)  # the worker earns the penalty
            singleton.reset()  # the bot has not seen it locally

            edit = AsyncMock(return_value="ok")
            query = SimpleNamespace(
                edit_message_text=edit,
                message=SimpleNamespace(chat=SimpleNamespace(id=-1004400932750), message_id=308),
                data=None,
            )
            result = await object.__new__(EnhancedMediaHandler).safe_edit(query, "progress")

            self.assertIsNone(result)
            self.assertEqual(edit.await_count, 0)
        finally:
            singleton.attach_redis(None)
            singleton.reset()

    async def test_extend_only_script_is_used(self):
        # The script is the contract with the other container; keep it honest.
        self.assertIn("redis.call('GET'", rate_limiter._FLOOD_EXTEND_LUA)
        self.assertIn("SET", rate_limiter._FLOOD_EXTEND_LUA)
        self.assertIn("if current and tonumber(current) >= deadline then", rate_limiter._FLOOD_EXTEND_LUA)


class EditCoalescerTests(unittest.TestCase):
    def setUp(self):
        self.coalescer = rate_limiter.TelegramEditCoalescer(min_interval=10.0)

    def test_nothing_is_skipped_before_an_edit_is_recorded(self):
        # A message whose text we have never seen rendered is always editable.
        self.assertFalse(self.coalescer.should_skip(1, 2, "job 42 10%"))
        self.assertFalse(self.coalescer.should_skip(1, 2, "job 42 10%"))

    def test_repeated_text_is_dropped(self):
        self.coalescer.record(1, 2, "job 42 10%")
        self.assertTrue(self.coalescer.should_skip(1, 2, "job 42 10%"))

    def test_new_text_inside_the_interval_is_dropped(self):
        self.coalescer.record(1, 2, "10%")
        self.assertTrue(self.coalescer.should_skip(1, 2, "20%"))

    def test_force_lets_a_terminal_status_through(self):
        self.coalescer.record(1, 2, "10%")
        self.assertTrue(self.coalescer.should_skip(1, 2, "20%"))
        self.assertFalse(self.coalescer.should_skip(1, 2, "done", force=True))

    def test_zero_interval_still_drops_a_repeat_but_not_a_change(self):
        # Discrete events (file 2 of 9) must land; an exact repeat must not.
        self.coalescer.record(1, 2, "1 of 9")
        self.assertFalse(self.coalescer.should_skip(1, 2, "2 of 9", min_interval=0))
        self.assertTrue(self.coalescer.should_skip(1, 2, "1 of 9", min_interval=0))

    def test_messages_do_not_share_an_allowance(self):
        self.coalescer.record(1, 2, "10%")
        self.assertFalse(self.coalescer.should_skip(1, 3, "10%"))

    def test_forget_clears_a_reposted_message_id(self):
        self.coalescer.record(1, 2, "1 of 9")
        self.coalescer.forget(1, 2)
        self.assertFalse(self.coalescer.should_skip(1, 2, "1 of 9"))

    def test_unknown_target_is_never_skipped(self):
        self.coalescer.record(None, None, "10%")
        self.assertFalse(self.coalescer.should_skip(None, None, "10%"))
        self.assertFalse(self.coalescer.should_skip(None, None, "10%"))


class SafeEditFloodTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        rate_limiter.telegram_flood_gate.reset()
        rate_limiter.telegram_edit_coalescer.reset()

    def tearDown(self):
        rate_limiter.telegram_flood_gate.reset()

    def _handler(self):
        from handlers import EnhancedMediaHandler

        return object.__new__(EnhancedMediaHandler)

    def _query(self, edit):
        return SimpleNamespace(
            edit_message_text=edit,
            message=SimpleNamespace(chat=SimpleNamespace(id=-1004400932750), message_id=308),
            data=None,
        )

    async def test_long_flood_drops_the_edit_instead_of_sleeping(self):
        slept = []
        real_sleep = asyncio.sleep

        async def _fake_sleep(seconds):
            slept.append(seconds)
            await real_sleep(0)

        edit = AsyncMock(side_effect=RetryAfter(27565))
        gate = rate_limiter.telegram_flood_gate

        with patch("handlers.asyncio.sleep", _fake_sleep):
            result = await self._handler().safe_edit(self._query(edit), "progress")

        self.assertIsNone(result)
        self.assertEqual(edit.await_count, 1)
        self.assertEqual(slept, [])
        self.assertTrue(await gate.should_drop_inline(gate.scope_for_chat(-1004400932750)))

    async def test_further_edits_are_skipped_while_the_window_is_open(self):
        gate = rate_limiter.telegram_flood_gate
        await gate.note(27565, gate.scope_for_chat(-1004400932750))

        edit = AsyncMock(return_value="ok")
        result = await self._handler().safe_edit(self._query(edit), "progress")

        self.assertIsNone(result)
        self.assertEqual(edit.await_count, 0)

    async def test_short_flood_is_waited_out_and_retried(self):
        slept = []
        real_sleep = asyncio.sleep

        async def _fake_sleep(seconds):
            slept.append(seconds)
            await real_sleep(0)

        calls = {"n": 0}

        async def _edit(text, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RetryAfter(3)
            return "edited"

        with patch("handlers.asyncio.sleep", _fake_sleep):
            result = await self._handler().safe_edit(self._query(_edit), "progress")

        self.assertEqual(result, "edited")
        self.assertEqual(slept, [3.5])


class UserbotMediaTests(unittest.IsolatedAsyncioTestCase):
    def test_document_and_photo_are_downloadable(self):
        self.assertTrue(downloader._has_downloadable_media(SimpleNamespace(id=1, media=object(), document=object())))
        self.assertTrue(downloader._has_downloadable_media(SimpleNamespace(id=1, media=object(), photo=object())))

    def test_link_preview_style_media_is_not_downloadable(self):
        # ``media`` is truthy (a poll / web page / location) but there is no file.
        msg = SimpleNamespace(id=4088, media=object(), document=None, photo=None, video=None)
        self.assertFalse(downloader._has_downloadable_media(msg))

    def test_empty_and_media_less_messages_are_rejected(self):
        self.assertFalse(downloader._has_downloadable_media(None))
        self.assertFalse(downloader._has_downloadable_media(SimpleNamespace(id=1, media=None)))
        self.assertFalse(
            downloader._has_downloadable_media(SimpleNamespace(id=0, media=object(), empty=True, document=object()))
        )

    async def test_non_downloadable_peer_falls_through_to_the_bot_dm(self):
        """The regression: a peer whose message carries non-downloadable media.

        The Bot API chat id (the user's own id) resolves inside the account to a
        peer that answers with the requested message id, but that message is a link
        preview. Breaking there meant the file was never fetched from the peer that
        actually holds it - the DM with the bot - and the caller fell back to the
        slow date-scan recovery.
        """
        bogus = SimpleNamespace(id=4088, media=object(), document=None, photo=None)
        real = SimpleNamespace(id=4088, media=object(), document=object(), photo=None)

        client = AsyncMock()
        client.start = AsyncMock()
        client.stop = AsyncMock()

        async def _get_messages(peer, message_ids=None, **kwargs):
            return [real if peer == 8323674784 else bogus]

        client.get_messages = AsyncMock(side_effect=_get_messages)
        download = AsyncMock(return_value=True)

        with (
            patch.object(downloader, "PyrogramClient", object()),
            patch.object(downloader, "_resolve_pyrogram_peer", AsyncMock(side_effect=lambda _c, peer: peer)),
            patch.object(downloader, "_get_bot_user_id", lambda: 8323674784),
            patch.object(downloader, "_download_and_ensure_path", download),
            patch("utils.telethon_session.get_pyrogram_session_string_for_user", AsyncMock(return_value="session")),
            patch("utils.telethon_session.get_db_model", lambda: None),
            patch("utils.telethon_session.get_userbot_credentials", lambda: (1, "hash")),
            patch("utils.telethon_session.build_pyrogram_client", lambda *a, **k: client),
        ):
            result = await downloader._download_with_pyrogram(1405333465, 4088, "storage/input/out.mp4")

        self.assertTrue(result)
        download.assert_awaited_once()
        # The bogus peer was skipped, the bot DM was the one downloaded from.
        self.assertEqual(download.await_args.args[1], real)


class FloodGatedRequestTests(unittest.IsolatedAsyncioTestCase):
    """The request layer is where a gated send fails fast instead of 429ing."""

    def setUp(self):
        from utils import telegram_flood_request as mod

        self.mod = mod
        self.gate = rate_limiter.telegram_flood_gate
        self.gate.reset()

    def tearDown(self):
        self.gate.reset()

    def _request(self):
        # No httpx client needed: every test patches the parent's do_request.
        return object.__new__(self.mod.FloodGatedRequest)

    def _data(self, **parameters):
        return SimpleNamespace(parameters=parameters)

    async def _call(self, method, data, chat_id=None):
        """Run the gated request with the parent call recorded, never performed."""
        sent = AsyncMock(return_value=(200, b'{"ok":true,"result":true}'))
        with patch.object(self.mod._PTBRequest, "do_request", sent):
            try:
                result = await self._request().do_request("http://tg/method", method, data)
                return result, sent.await_count, None
            except RetryAfter as exc:
                return None, sent.await_count, exc

    async def test_a_gated_chat_raises_without_touching_the_network(self):
        await self.gate.note(27565, self.gate.scope_for_chat(-1004400932750))

        _result, calls, exc = await self._call("sendMessage", self._data(chat_id=-1004400932750, text="hi"))

        self.assertEqual(calls, 0)
        self.assertIsInstance(exc, RetryAfter)
        self.assertGreater(exc.retry_after, 25000)

    async def test_an_open_chat_is_sent_normally(self):
        result, calls, exc = await self._call("sendMessage", self._data(chat_id=5, text="hi"))

        self.assertIsNone(exc)
        self.assertEqual(calls, 1)
        self.assertEqual(result, (200, b'{"ok":true,"result":true}'))

    async def test_reads_without_a_chat_keep_working_during_a_window(self):
        await self.gate.note(27565, self.gate.scope_for_chat(5))

        # getUpdates keeps the handler loop alive; the flood gate has no chat.
        for method, data in (
            ("getUpdates", self._data(offset=1, timeout=30)),
            ("getFile", self._data(file_id="abc")),
            ("answerCallbackQuery", self._data(callback_query_id="q1")),
            ("getChat", self._data(chat_id=5)),
            ("getChatMember", self._data(chat_id=5, user_id=9)),
        ):
            _result, calls, exc = await self._call(method, data)
            self.assertIsNone(exc, f"{method} must not be gated")
            self.assertEqual(calls, 1, f"{method} must reach the network")

    async def test_ptb_passes_the_exception_through_unwrapped(self):
        """PTB re-raises a TelegramError from do_request instead of masking it.

        The gate depends on this: if the layer wrapped our RetryAfter in a
        NetworkError, callers would treat a flood window as a transient network
        fault and retry into it.
        """
        data = SimpleNamespace(parameters={"chat_id": 1, "text": "x"}, multipart_data=None)
        req = object.__new__(self.mod.FloodGatedRequest)

        with (
            patch.object(self.mod._PTBRequest, "do_request", AsyncMock(side_effect=RetryAfter(9))),
            self.assertRaises(RetryAfter),
        ):
            await req._request_wrapper(url="http://tg", method="POST", request_data=data)

    async def test_a_one_to_one_chat_is_gated_by_its_user_id(self):
        """A DM's chat_id is the user's own id - the case from the incident."""
        await self.gate.note(26903, self.gate.scope_for_chat(1405333465))

        _result, calls, exc = await self._call("sendMessage", self._data(chat_id="1405333465", text="done"))

        self.assertEqual(calls, 0)
        self.assertIsInstance(exc, RetryAfter)

    def test_write_target_chat_id_only_gates_chat_writes(self):
        target = self.mod.write_target_chat_id

        self.assertEqual(target("sendMessage", self._data(chat_id=-100, text="x")), -100)
        self.assertEqual(target("editMessageText", self._data(chat_id="42")), 42)
        self.assertEqual(target("deleteMessage", self._data(chat_id=7, message_id=2)), 7)

        # No chat_id to key the window on.
        self.assertIsNone(target("getUpdates", self._data(offset=1)))
        self.assertIsNone(target("answerCallbackQuery", self._data(callback_query_id="q")))
        self.assertIsNone(target("sendMessage", self._data(chat_id="@channel", text="x")))
        # Reads that do carry a chat_id stay available.
        self.assertIsNone(target("getChatMember", self._data(chat_id=5, user_id=1)))
        # An unparseable payload must never turn into a blocked send.
        self.assertIsNone(target("sendMessage", object()))
        self.assertIsNone(target("sendMessage", self._data(chat_id=True)))
        self.assertIsNone(target("sendMessage", self._data(chat_id=None)))

    def test_the_gated_request_is_a_real_ptb_request(self):
        self.assertIsNotNone(self.mod._PTBRequest)
        self.assertTrue(issubclass(self.mod.FloodGatedRequest, self.mod._PTBRequest))

    def test_factory_degrades_instead_of_breaking_a_bot_build(self):
        """A PTB/httpx mismatch must cost the gating, never the bot.

        ``Bot(request=None)`` falls back to PTB's own default, so the factory
        answering None is a graceful downgrade.
        """
        built = self.mod.flood_gated_request()
        if built is not None:
            self.assertIsInstance(built, self.mod._PTBRequest)

        with patch.object(self.mod, "FloodGatedRequest", side_effect=RuntimeError("boom")):
            self.assertIsNone(self.mod.flood_gated_request())

    def test_the_bot_and_the_worker_both_use_it(self):
        """Wiring, asserted from source: a call site that forgets it is silent."""
        self.assertIn("FloodGatedRequest", read_source("main.py"))
        worker_src = read_source("workers", "ffmpeg_worker.py")
        self.assertIn("request=flood_gated_request()", worker_src)
        self.assertEqual(worker_src.count("Bot(token=bot_token,"), worker_src.count("request=flood_gated_request()"))


class BatchBarFloodTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        rate_limiter.telegram_flood_gate.reset()
        rate_limiter.telegram_edit_coalescer.reset()

    def tearDown(self):
        rate_limiter.telegram_flood_gate.reset()
        rate_limiter.telegram_edit_coalescer.reset()

    async def test_long_flood_keeps_the_existing_bar_without_sleeping(self):
        from workers import ffmpeg_worker

        slept = []
        real_sleep = asyncio.sleep

        async def _fake_sleep(seconds):
            slept.append(seconds)
            await real_sleep(0)

        bot = AsyncMock()
        bot.edit_message_text = AsyncMock(side_effect=RetryAfter(26903))

        with patch.object(ffmpeg_worker.asyncio, "sleep", _fake_sleep):
            result = await ffmpeg_worker._set_batch_message(
                bot, AsyncMock(), "batch-1", -1004400932750, "-1004400932750:308", "text"
            )

        self.assertEqual(result, "-1004400932750:308")
        self.assertEqual(slept, [])
        self.assertTrue(
            await rate_limiter.telegram_flood_gate.should_drop_inline(
                rate_limiter.telegram_flood_gate.scope_for_chat(-1004400932750)
            )
        )

    async def test_bar_is_not_edited_while_the_window_is_open(self):
        from workers import ffmpeg_worker

        gate = rate_limiter.telegram_flood_gate
        await gate.note(26903, gate.scope_for_chat(-1009876543210))

        bot = AsyncMock()
        bot.edit_message_text = AsyncMock(return_value=None)

        result = await ffmpeg_worker._set_batch_message(
            bot, AsyncMock(), "batch-2", -1009876543210, "-1009876543210:309", "text"
        )

        self.assertEqual(result, "-1009876543210:309")
        self.assertEqual(bot.edit_message_text.await_count, 0)


class _FakeQueueRedis:
    """Just enough Redis for ``utils.deferred_delivery``'s queue operations."""

    def __init__(self):
        self.strings: dict[str, str] = {}
        self.scores: dict[str, dict[str, float]] = {}

    async def set(self, key, value, ex=None):
        self.strings[key] = value

    async def get(self, key):
        return self.strings.get(key)

    async def delete(self, key):
        self.strings.pop(key, None)

    async def zadd(self, key, mapping):
        self.scores.setdefault(key, {}).update({str(k): float(v) for k, v in mapping.items()})

    async def zrem(self, key, member):
        self.scores.get(key, {}).pop(str(member), None)

    async def zcard(self, key):
        return len(self.scores.get(key, {}))

    async def zrangebyscore(self, key, low, high, start=0, num=5):
        rows = sorted(
            (m for m, s in self.scores.get(key, {}).items() if float(s) <= float(high)),
            key=lambda m: self.scores[key][m],
        )
        return rows[start : start + num]

    async def close(self):
        return None


class DeferredDeliveryTests(unittest.IsolatedAsyncioTestCase):
    """A delivery Telegram refused must survive until the window closes.

    Sending is the last step of a job, so finalizing it as "delivery failed" on a
    429 loses a file the user already paid to convert - even though the window
    closes by itself within hours. These cover the queue that holds that
    delivery, the worker hook that fills it, and the sweep that drains it.
    """

    def setUp(self):
        from utils import deferred_delivery
        from workers import ffmpeg_worker

        self.deferred = deferred_delivery
        self.worker = ffmpeg_worker
        self.redis = _FakeQueueRedis()

        async def _factory():
            return self.redis

        self.deferred.get_redis = _factory
        rate_limiter.telegram_flood_gate.reset()

    def tearDown(self):
        rate_limiter.telegram_flood_gate.reset()

    def _record(self, **overrides):
        record = {
            "job_id": "job-1",
            "chat_id": -1004400932750,
            "output": None,
            "output_key": "outputs/library/x/source.mp3",
            "delivery_name": "song.mp3",
            "media_kind": "audio",
            "caption": "here",
            "cleanup_output": False,
            "reason": "telegram flood control",
        }
        record.update(overrides)
        return record

    def _make_due(self):
        """Backdate the queue: a real sweep waits out the minimum due delay."""
        scores = self.redis.scores[self.deferred.DUE_KEY]
        for member in list(scores):
            scores[member] = time.time() - 1.0

    async def test_a_record_survives_a_claim_and_clear(self):
        self.assertTrue(await self.deferred.defer(self._record(), due_in=1, horizon=3600))
        self.assertEqual(await self.deferred.pending(), 1)
        self._make_due()

        due = await self.deferred.claim_due(limit=5)
        self.assertEqual([r["job_id"] for r in due], ["job-1"])
        self.assertEqual(due[0]["delivery_name"], "song.mp3")

        await self.deferred.clear("job-1")
        self.assertEqual(await self.deferred.pending(), 0)
        self.assertEqual(await self.deferred.claim_due(limit=5), [])

    async def test_a_claimed_record_is_not_handed_out_twice(self):
        await self.deferred.defer(self._record(), due_in=1, horizon=3600)
        self._make_due()
        first = await self.deferred.claim_due(limit=5, lease=600)
        second = await self.deferred.claim_due(limit=5, lease=600)

        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])

    async def test_an_expired_payload_drops_its_queue_entry(self):
        await self.deferred.defer(self._record(), due_in=1, horizon=3600)
        self.redis.strings.clear()  # the record's TTL ran out
        self._make_due()

        self.assertEqual(await self.deferred.claim_due(limit=5), [])
        self.assertEqual(await self.deferred.pending(), 0)

    async def test_no_open_window_means_no_deferral(self):
        """A real failure stays a failure; only a window this bot serves waits."""
        window = await self.worker._defer_delivery(
            self._record(),
            chat_id=-1004400932750,
            output=None,
            output_key="k",
            delivery_name="song.mp3",
            media_kind="audio",
            caption=None,
            reason="telegram flood control",
        )

        self.assertEqual(window, 0.0)
        self.assertEqual(await self.deferred.pending(), 0)

    async def test_a_refused_delivery_is_queued_for_the_window(self):
        gate = rate_limiter.telegram_flood_gate
        await gate.note(26903, gate.scope_for_chat(-1004400932750))

        window = await self.worker._defer_delivery(
            self._record(),
            chat_id=-1004400932750,
            output=None,
            output_key="outputs/library/x/source.mp3",
            delivery_name="song.mp3",
            media_kind="audio",
            caption=None,
            reason="telegram flood control",
        )

        self.assertGreater(window, 26000)
        self._make_due()
        due = await self.deferred.claim_due(limit=5, lease=1)
        self.assertEqual([r["job_id"] for r in due], ["job-1"])
        self.assertEqual(due[0]["chat_id"], -1004400932750)

    async def test_a_window_that_is_still_open_never_calls_telegram(self):
        gate = rate_limiter.telegram_flood_gate
        await gate.note(26903, gate.scope_for_chat(-1004400932750))
        await self.deferred.defer(self._record(), due_in=1, horizon=3600)

        send = AsyncMock(return_value=True)
        update = AsyncMock(return_value=True)
        with (
            patch.object(self.worker, "_send_deferred_output", send),
            patch.object(self.deferred, "update", update),
        ):
            outcome = await self.worker._deliver_deferred_record(self._record())

        self.assertEqual(outcome, "deferred")
        self.assertEqual(send.await_count, 0)
        self.assertGreater(update.await_args.kwargs["due_in"], 0)

    async def test_a_re_gated_send_does_not_spend_an_attempt(self):
        """The window can reopen between the check and the send."""
        gate = rate_limiter.telegram_flood_gate
        gate.remaining = AsyncMock(return_value=0)
        update = AsyncMock(return_value=True)
        finish = AsyncMock()

        with (
            patch.object(self.worker, "_resolve_deferred_output", AsyncMock(return_value="song.mp3")),
            patch.object(self.worker, "_send_deferred_output", AsyncMock(side_effect=RetryAfter(26900))),
            patch.object(self.deferred, "update", update),
            patch.object(self.worker, "_finish_deferred", finish),
        ):
            outcome = await self.worker._deliver_deferred_record(self._record())

        self.assertEqual(outcome, "deferred")
        self.assertEqual(finish.await_count, 0)
        self.assertNotIn("attempts", update.await_args.kwargs)
        self.assertGreater(update.await_args.kwargs["due_in"], 26000)
        del gate.remaining

    async def test_finishing_a_deferred_delivery_writes_the_terminal_status(self):
        """The ``processing`` the deferral left behind is what the bot polls."""
        await self.deferred.defer(self._record(), due_in=1, horizon=3600)
        hashes: dict = {}

        class _R:
            async def hset(self, key, mapping=None):
                hashes.setdefault(key, {}).update(mapping or {})

            async def close(self):
                return None

        with (
            patch.object(self.worker, "get_redis", AsyncMock(return_value=_R())),
            patch.object(self.worker, "publish_update", AsyncMock()),
            patch.object(self.worker, "emit_event", AsyncMock()),
        ):
            await self.worker._finish_deferred(self._record(), ok=True, message="delivered")

        state = hashes["ffmpeg:job:job-1"]
        self.assertEqual(state["status"], "done")
        # Tells a broker redelivery (and an operator) the file is out.
        self.assertEqual(state["delivered"], "1")
        self.assertEqual(state["delivery_deferred"], "0")
        self.assertEqual(await self.deferred.pending(), 0)

    async def test_an_abandoned_deferred_delivery_keeps_the_output(self):
        """The local file is the user's only copy; deleting it is permanent."""
        src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_deferred_keep.tmp")
        with open(src, "wb") as fh:
            fh.write(b"x")
        self.addCleanup(lambda: os.path.exists(src) and os.remove(src))

        with (
            patch.object(self.worker, "get_redis", AsyncMock(side_effect=RuntimeError("no redis"))),
            patch.object(self.worker, "publish_update", AsyncMock()),
            patch.object(self.worker, "emit_event", AsyncMock()),
        ):
            await self.worker._finish_deferred(
                self._record(output=src, cleanup_output=True), ok=False, message="gave up"
            )

        self.assertTrue(os.path.exists(src))

    async def test_the_sweeper_drains_and_counts(self):
        await self.deferred.defer(self._record(), due_in=1, horizon=3600)
        self._make_due()
        delivered = AsyncMock(return_value="delivered")

        with patch.object(self.worker, "_deliver_deferred_record", delivered):
            counts = await self.worker._sweep_deferred_deliveries(limit=5)

        self.assertEqual(counts["delivered"], 1)
        self.assertEqual(delivered.await_count, 1)

    async def test_the_sweeper_stands_down_while_a_conversion_runs(self):
        """One conversion plus one upload is this container's memory ceiling."""
        await self.deferred.defer(self._record(), due_in=1, horizon=3600)
        self._make_due()
        delivered = AsyncMock(return_value="delivered")

        with (
            patch.object(self.worker, "_deliver_deferred_record", delivered),
            patch.object(self.worker, "_jobs_in_flight", 1),
        ):
            counts = await self.worker._sweep_deferred_deliveries(limit=5)

        self.assertEqual(counts["skipped"], 1)
        self.assertEqual(delivered.await_count, 0)

    async def test_health_reports_an_open_window_without_reading_logs(self):
        """The bot looked healthy while every send to one chat was refused."""
        gate = rate_limiter.telegram_flood_gate
        await gate.note(26903, gate.scope_for_chat(-1004400932750))

        windows = {s: left for s, left in gate.snapshot().items() if left > 0}

        self.assertEqual(len(windows), 1)
        self.assertGreater(list(windows.values())[0], 26000)
        self.assertIn('"telegram_flood": flood', read_source("main.py"))

    async def test_the_health_count_follows_the_queue(self):
        self.assertEqual(await self.deferred.pending(), 0)
        await self.deferred.defer(self._record(), due_in=1, horizon=3600)

        self.assertEqual(await self.deferred.pending(), 1)

    def test_the_worker_never_fails_a_job_it_deferred(self):
        """Wiring, asserted from source: a deferral must not finalize as error."""
        src = read_source("workers", "ffmpeg_worker.py")

        self.assertIn("if _deferred_window:", src)
        self.assertIn('_final_status = "processing"', src)
        self.assertIn("delivery_deferred", src)
        # The bot's watcher only reaches the result if the hash says processing.
        handlers_src = read_source("handlers.py")
        self.assertIn("_terminal_pending_since", handlers_src)
        self.assertIn("_FLOOD_TERMINAL_MAX_WAIT_SECONDS", handlers_src)


class CoalescerGateMismatchTests(unittest.IsolatedAsyncioTestCase):
    """The coalescer is process-local; the gate is shared via Redis.

    The bot and the worker each have their own TelegramEditCoalescer instance.
    When the worker edits the batch bar, the bot's coalescer doesn't know about
    it, so the bot's stage watcher could try to edit the same message inside the
    coalescer's pacing window.  The flood gate catches this: a 429 sets the gate
    for the chat, and every handler (bot or worker) checks it before touching
    the wire.
    """

    def setUp(self):
        rate_limiter.telegram_flood_gate.reset()
        rate_limiter.telegram_edit_coalescer.reset()

    def tearDown(self):
        rate_limiter.telegram_flood_gate.reset()
        rate_limiter.telegram_edit_coalescer.reset()

    def test_two_coalescers_are_independent(self):
        """Each process has its own coalescer; an edit in one is invisible to the other."""
        bot_coalescer = rate_limiter.TelegramEditCoalescer(min_interval=10.0)
        worker_coalescer = rate_limiter.TelegramEditCoalescer(min_interval=10.0)

        # The worker edits the batch bar.
        worker_coalescer.record(-100, 1, "2 of 5 finished")
        # The bot's coalescer has never seen this message.
        self.assertFalse(bot_coalescer.should_skip(-100, 1, "2 of 5 finished"))
        # The worker coalescer would skip a repeat.
        self.assertTrue(worker_coalescer.should_skip(-100, 1, "2 of 5 finished"))

    def test_gate_catches_edits_from_both_processes(self):
        """A 429 sets the shared gate; both bot and worker respect it."""
        gate = rate_limiter.telegram_flood_gate
        scope = gate.scope_for_chat(-100)

        # Telegram sends a long 429 (over the inline_max threshold) after
        # the worker's edit.  should_drop_inline only fires for windows
        # exceeding inline_max (default 30 s) — short windows are waited
        # out, not dropped.
        asyncio.get_event_loop().run_until_complete(gate.note(60, scope))

        # Both the bot's safe_edit path and the worker's _set_batch_message
        # check the gate before editing.  The gate is open for both.
        self.assertTrue(asyncio.get_event_loop().run_until_complete(gate.should_drop_inline(scope)))

    def test_coalescer_skip_does_not_block_when_gate_is_open(self):
        """The coalescer pacing is process-local; the gate is the backstop."""
        coalescer = rate_limiter.TelegramEditCoalescer(min_interval=10.0)
        gate = rate_limiter.telegram_flood_gate
        scope = gate.scope_for_chat(-100)

        # The coalescer says "skip" (recent edit), but the gate is closed.
        coalescer.record(-100, 1, "old text")
        self.assertTrue(coalescer.should_skip(-100, 1, "new text"))
        self.assertFalse(asyncio.get_event_loop().run_until_complete(gate.should_drop_inline(scope)))

    def test_worker_edit_not_seen_by_bot_coalescer(self):
        """The worker edits the batch bar; the bot's coalescer doesn't pace it."""
        bot_coalescer = rate_limiter.TelegramEditCoalescer(min_interval=2.5)
        worker_coalescer = rate_limiter.TelegramEditCoalescer(min_interval=2.5)

        # Worker edits the message.
        worker_coalescer.record(-100, 1, "3 of 5 — encoding")
        # Bot coalescer has no memory of this edit — not paced, not deduped.
        self.assertFalse(bot_coalescer.should_skip(-100, 1, "3 of 5 — encoding"))
        self.assertFalse(bot_coalescer.should_skip(-100, 1, "same text again"))
        # The worker coalescer does pace and dedup locally.
        self.assertTrue(worker_coalescer.should_skip(-100, 1, "3 of 5 — encoding"))

    def test_gate_still_allows_edits_after_window_expires(self):
        """After the gate's window closes, both processes can edit again."""
        gate = rate_limiter.telegram_flood_gate
        scope = gate.scope_for_chat(-100)

        # Simulate a very short window.
        asyncio.get_event_loop().run_until_complete(gate.note(0.01, scope))
        import time

        time.sleep(0.02)

        self.assertFalse(asyncio.get_event_loop().run_until_complete(gate.should_drop_inline(scope)))

    def test_concurrent_updates_does_not_break_coalescer(self):
        """With concurrent_updates, two handlers for the same message are safe
        because the coalescer's dict ops are synchronous (no await between
        record/should_skip), so they are atomic under cooperative scheduling."""
        coalescer = rate_limiter.TelegramEditCoalescer(min_interval=0.0)
        # Record and immediately check — no interleaving possible.
        coalescer.record(-100, 1, "text")
        # Exact repeat is always dropped, even with zero interval.
        self.assertTrue(coalescer.should_skip(-100, 1, "text"))
        # Different text passes with zero interval.
        self.assertFalse(coalescer.should_skip(-100, 1, "different"))


if __name__ == "__main__":
    unittest.main()
