"""Upload progress: the chat message keeps moving while a big file goes out.

The progress callback is invoked by whichever thread the Telegram client picks,
and only one of them (Telethon) has a running event loop. Pyrogram hands it to
``loop.run_in_executor`` and the Bot API reads the file on an httpx thread, so a
callback that looks a loop up at call time raises there and its update is lost -
which is how a 789MB WAV upload left the "uploading" message frozen for minutes.
"""

import asyncio
import os
import sys
import unittest
from unittest.mock import patch

from workers import ffmpeg_worker as worker


class _FakeRedisClient:
    """The slice of ``redis.Redis`` the cancel check uses."""

    def __init__(self, cancel=b"0", fail=False):
        self.cancel = cancel
        self.fail = fail
        self.hget_calls = []
        self.closed = False

    def hget(self, key, field):
        self.hget_calls.append((key, field))
        if self.fail:
            raise RuntimeError("redis is unreachable")
        return self.cancel

    def close(self):
        self.closed = True


class _FakeRedisModule:
    """Stand-in for ``import redis`` inside the callback."""

    def __init__(self, client):
        self.client = client
        self.urls = []

    def from_url(self, url, **kwargs):
        self.urls.append(url)
        return self.client


def _recording_progress(seen):
    """An async ``_update_upload_progress`` stand-in that records its calls."""

    async def _record(job_id, progress_channel, pct, message):
        seen.append((job_id, progress_channel, pct, message))

    return _record


class UploadProgressCallbackTests(unittest.IsolatedAsyncioTestCase):
    """Behaviour of the sync callback handed to Telethon/Pyrogram/Bot API."""

    def _callback(self, seen, *, cancel=b"0", fail=False):
        """A callback whose updates are recorded, plus the fake redis behind it."""
        client = _FakeRedisClient(cancel=cancel, fail=fail)
        module = _FakeRedisModule(client)
        self.addCleanup(patch.stopall)
        patcher = patch.object(worker, "_update_upload_progress", _recording_progress(seen))
        patcher.start()
        redis_patcher = patch.dict(sys.modules, {"redis": module})
        redis_patcher.start()
        return worker._make_upload_progress_callback("job-1", "ffmpeg:progress:job-1"), client

    async def _call_from_a_thread(self, callback, sent, total):
        """Invoke the callback the way Pyrogram does: on an executor thread."""
        await asyncio.get_running_loop().run_in_executor(None, callback, sent, total)
        # The update is scheduled onto the loop, so let it have a turn.
        await asyncio.sleep(0.05)

    async def test_an_update_from_an_executor_thread_still_lands(self):
        seen = []
        callback, _client = self._callback(seen)
        with patch.dict(os.environ, {"REDIS_URL": ""}):
            await self._call_from_a_thread(callback, 40 * 1024 * 1024, 100 * 1024 * 1024)

        self.assertEqual(
            seen,
            [("job-1", "ffmpeg:progress:job-1", 40, "Uploading to Telegram: 40% (40MB / 100MB)")],
        )

    async def test_every_part_does_not_become_an_edit(self):
        # One 512KB part changes the percentage of a small file, so without
        # pacing a single upload would ask for hundreds of Telegram edits.
        seen = []
        callback, _client = self._callback(seen)
        with patch.dict(os.environ, {"REDIS_URL": ""}):
            for pct in range(1, 20):
                await self._call_from_a_thread(callback, pct, 100)

        self.assertEqual([entry[2] for entry in seen], [1])

    async def test_the_final_percentage_always_lands(self):
        # The last update is what tells the user the upload is over, so it is
        # not subject to the pacing that the intermediate ones are.
        seen = []
        callback, _client = self._callback(seen)
        with patch.dict(os.environ, {"REDIS_URL": ""}):
            callback(50, 100)
            callback(100, 100)
            await asyncio.sleep(0.05)

        self.assertEqual([entry[2] for entry in seen], [50, 100])

    async def test_a_cancelled_job_stops_the_upload(self):
        # Cancelling is only possible from inside the transfer: the callback
        # raises through the client's own upload loop.
        seen = []
        callback, client = self._callback(seen, cancel=b"1")
        with (
            patch.dict(os.environ, {"REDIS_URL": "redis://localhost:6379/0"}),
            self.assertRaises(asyncio.CancelledError),
        ):
            callback(40 * 1024 * 1024, 100 * 1024 * 1024)

        self.assertEqual(seen, [])
        self.assertEqual(client.hget_calls, [("ffmpeg:job:job-1", "cancel")])

    async def test_the_cancel_flag_is_polled_on_a_cadence(self):
        # A blocking Redis read per 512KB part would cost more than the upload.
        seen = []
        callback, client = self._callback(seen, cancel=b"0")
        with patch.dict(os.environ, {"REDIS_URL": "redis://localhost:6379/0"}):
            for _ in range(5):
                callback(1, 100)
            await asyncio.sleep(0.05)

        self.assertEqual(len(client.hget_calls), 1)

    async def test_cancelling_mid_upload_is_noticed_on_a_later_part(self):
        seen = []
        callback, client = self._callback(seen, cancel=b"0")
        with patch.dict(os.environ, {"REDIS_URL": "redis://localhost:6379/0"}):
            callback(10 * 1024 * 1024, 100 * 1024 * 1024)
            # The user hits Cancel while the file is still going out...
            client.cancel = b"1"
            # ...and the next part, not the next job, picks it up.
            with (
                patch.object(worker, "_UPLOAD_CANCEL_CHECK_INTERVAL", 0.0),
                self.assertRaises(asyncio.CancelledError),
            ):
                callback(20 * 1024 * 1024, 100 * 1024 * 1024)
            await asyncio.sleep(0.05)

        self.assertEqual([entry[2] for entry in seen], [10])

    async def test_an_unreachable_redis_never_costs_the_upload(self):
        seen = []
        callback, _client = self._callback(seen, fail=True)
        with patch.dict(os.environ, {"REDIS_URL": "redis://localhost:6379/0"}):
            callback(40 * 1024 * 1024, 100 * 1024 * 1024)
            await asyncio.sleep(0.05)

        self.assertEqual([entry[2] for entry in seen], [40])

    async def test_a_nonsense_total_is_ignored(self):
        seen = []
        callback, _client = self._callback(seen)
        with patch.dict(os.environ, {"REDIS_URL": ""}):
            callback(0, 0)
            await asyncio.sleep(0.02)

        self.assertEqual(seen, [])
