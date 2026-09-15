"""The bounds that stop a bulk apply from hanging on one file.

Two independent guards, because either alone leaves a hole:

* the Pyrogram download itself (``PIPELINE_DOWNLOAD_TIMEOUT_SECONDS``) - a dead
  connection used to sit there forever, and
* the per-file fetch in the apply (``_BULK_FETCH_TIMEOUT_SECONDS``) - the
  backstop that lets the loop move on to the next file whatever happens inside.

Also covered: the batch counter must be advanced by exactly one of the two
parties that can see a finished file (the worker for a tagged job, the handler
for a pipeline job that carries no tag). Counting both ends the batch half way
through and takes its progress message down while work is still queued.
"""

import asyncio
import os

import pytest

from handlers import EnhancedMediaHandler
from utils import bigfile_pipeline, job_queue, userbot_downloader


def _run(coro):
    return asyncio.run(coro)


# ── the download itself ────────────────────────────────────────────────


def test_pyrogram_download_times_out_and_clears_the_partial_file(tmp_path, monkeypatch):
    dest = tmp_path / "job_src.mp4"
    dest.write_bytes(b"x" * 128)

    async def _hang(**kwargs):
        await asyncio.sleep(30)
        return True

    monkeypatch.setattr(userbot_downloader, "download_forward_via_userbot", _hang)
    monkeypatch.setattr(bigfile_pipeline, "PIPELINE_DOWNLOAD_TIMEOUT_SECONDS", 0.05)

    ok = _run(bigfile_pipeline.BigFilePipeline()._download_via_pyrogram(1, 2, str(dest)))

    assert ok is False
    # A half-file left behind would be picked up as a finished download by the
    # next attempt, so the timeout has to take it away.
    assert not os.path.exists(dest)


def test_pyrogram_download_reports_success(tmp_path, monkeypatch):
    dest = tmp_path / "job_src.mp4"

    async def _ok(**kwargs):
        with open(kwargs["dest_path"], "wb") as handle:
            handle.write(b"whole")
        return True

    monkeypatch.setattr(userbot_downloader, "download_forward_via_userbot", _ok)
    monkeypatch.setattr(bigfile_pipeline, "PIPELINE_DOWNLOAD_TIMEOUT_SECONDS", 5)

    assert _run(bigfile_pipeline.BigFilePipeline()._download_via_pyrogram(1, 2, str(dest))) is True
    assert dest.read_bytes() == b"whole"


def test_a_cancelled_download_cleans_up_and_still_cancels(tmp_path, monkeypatch):
    dest = tmp_path / "job_src.mp4"
    dest.write_bytes(b"x" * 64)

    async def _cancelled(**kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(userbot_downloader, "download_forward_via_userbot", _cancelled)

    with pytest.raises(asyncio.CancelledError):
        _run(bigfile_pipeline.BigFilePipeline()._download_via_pyrogram(1, 2, str(dest)))

    assert not os.path.exists(dest)


def test_a_failed_download_clears_the_partial_file(tmp_path, monkeypatch):
    dest = tmp_path / "job_src.mp4"
    dest.write_bytes(b"x" * 64)

    async def _boom(**kwargs):
        raise RuntimeError("DC unreachable")

    monkeypatch.setattr(userbot_downloader, "download_forward_via_userbot", _boom)

    assert _run(bigfile_pipeline.BigFilePipeline()._download_via_pyrogram(1, 2, str(dest))) is False
    assert not os.path.exists(dest)


# ── who counts a finished file ──────────────────────────────────────────


class _Redis:
    def __init__(self, values):
        self.values = values
        self.closed = False

    async def hget(self, key, field):
        return self.values.get(key, {}).get(field)

    async def close(self):
        self.closed = True


def _patch_redis(monkeypatch, client=None, *, error=None):
    async def _get_redis():
        if error is not None:
            raise error
        return client

    monkeypatch.setattr(job_queue, "get_redis", _get_redis)


def test_a_job_tagged_with_this_batch_is_counted_by_the_worker(monkeypatch):
    _patch_redis(monkeypatch, _Redis({"ffmpeg:job:pipeline-1": {"batch_id": "batch-a"}}))

    counted = _run(EnhancedMediaHandler._batch_worker_counts_job(object(), "batch-a", "pipeline-1"))

    assert counted is True


def test_a_job_from_another_batch_is_not_ours_to_skip(monkeypatch):
    _patch_redis(monkeypatch, _Redis({"ffmpeg:job:pipeline-1": {"batch_id": "batch-b"}}))

    counted = _run(EnhancedMediaHandler._batch_worker_counts_job(object(), "batch-a", "pipeline-1"))

    assert counted is False


def test_an_untagged_pipeline_job_is_counted_by_the_handler(monkeypatch):
    # A pipeline job the user already had in flight: no batch tag, so nothing
    # else will ever advance this batch's counter but the handler.
    _patch_redis(monkeypatch, _Redis({"ffmpeg:job:pipeline-1": {}}))

    counted = _run(EnhancedMediaHandler._batch_worker_counts_job(object(), "batch-a", "pipeline-1"))

    assert counted is False


def test_an_unreadable_tag_defers_to_the_worker(monkeypatch):
    # Fails towards the worker: assuming the handler must count it too would
    # finish the batch early, while an unadvanced counter only misplaces the bar.
    _patch_redis(monkeypatch, _Redis({}))

    async def _boom(key, field):
        raise RuntimeError("redis gone")

    client = _Redis({})
    client.hget = _boom
    _patch_redis(monkeypatch, client)

    assert _run(EnhancedMediaHandler._batch_worker_counts_job(object(), "batch-a", "pipeline-1")) is True


def test_a_missing_job_or_batch_is_never_counted_by_the_worker():
    assert _run(EnhancedMediaHandler._batch_worker_counts_job(object(), None, "job-1")) is False
    assert _run(EnhancedMediaHandler._batch_worker_counts_job(object(), "batch-a", None)) is False
