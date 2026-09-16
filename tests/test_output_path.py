"""The output path must not round-trip through the bucket.

Two things are checked here, both of them about not paying for traffic nobody
asked for:

* a delivered result is only copied into the bucket when something remote will
  actually read it, and
* the thumbnail attached to a delivery comes from local disk when this run
  produced it, rather than being fetched back out of storage.

The cleanup half covers ``outputs/`` being swept at all - it was previously the
one prefix with no janitor, so every result the bot ever produced stayed in the
bucket as an object something could later download.

The worker's delivery block is a long, deeply nested call, so the wiring is
asserted against its source (as the other worker tests do) while the helper that
decides between local and remote is exercised for real.
"""

import asyncio
import os
import time

from tasks import cleanup_tasks
from workers import ffmpeg_worker


def _worker_src() -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "workers", "ffmpeg_worker.py"), encoding="utf-8") as fh:
        return fh.read()


# ── the thumbnail is delivered from disk ────────────────────────────────


def test_local_thumbnail_is_used_when_it_exists(tmp_path):
    local = tmp_path / "thumb.jpg"
    local.write_bytes(b"jpeg")

    found = ffmpeg_worker._local_thumb_candidate(
        {"_local_thumb": str(local), "thumbnail": "outputs/job/thumb.jpg"}
    )

    # The local path wins over the key, which is what avoids the download.
    assert found == str(local)


def test_no_local_thumbnail_leaves_the_storage_fallback_alone(tmp_path):
    """A retry in another container must still be able to fetch the object."""
    assert ffmpeg_worker._local_thumb_candidate({"thumbnail": "outputs/job/thumb.jpg"}) is None
    assert ffmpeg_worker._local_thumb_candidate({}) is None


def test_a_stale_local_thumbnail_falls_through_to_the_key(tmp_path):
    """A recorded path that no longer exists must not win over the bucket."""
    assert (
        ffmpeg_worker._local_thumb_candidate(
            {"_local_thumb": str(tmp_path / "gone.jpg"), "thumbnail": "outputs/job/thumb.jpg"}
        )
        is None
    )


def test_delivery_checks_disk_before_reaching_for_the_object():
    src = _worker_src()

    # Every delivery site (zip, video, other) seeds itself from disk first...
    assert src.count("thumb_path = _local_thumb_candidate(job)") == 3
    # ...and only then falls back to the job's field, which the download follows.
    assert src.count('cand = None if thumb_path else job.get("thumbnail")') == 3


def test_the_uploaded_thumbnail_key_never_becomes_the_delivery_field():
    src = _worker_src()

    # `thumbnail` is what delivery reads first, so pointing it at the key is what
    # made every send fetch back bytes the worker still had on disk.
    assert 'mapping["thumbnail"] = _thumb_s3_key' not in src
    # The durable pointer is kept, and the hash fallback can find it.
    assert 'mapping["thumb_key"] = _thumb_s3_key' in src
    assert 'stored.get("thumb_key")' in src


# ── a delivered result is not uploaded for nobody ───────────────────────


def test_output_upload_is_gated_on_something_remote_reading_it():
    src = _worker_src()

    assert '_needs_remote_copy = bool(config.ENABLE_LINK_SEND or not job.get("chat_id"))' in src
    assert "and _needs_remote_copy:" in src


def test_a_skipped_upload_still_reports_a_result():
    """The watcher keys the result message off `output`, so it must stay truthy.

    A `done` job with no `output` in its hash falls through to the bot's
    "finished with status: done" warning, which reads as a failure.
    """
    src = _worker_src()

    assert '"output": str(out),' in src


def test_skipping_the_upload_still_releases_the_local_input():
    """The input cleanup used the upload as its 'result is safe' proxy."""
    src = _worker_src()

    assert "if upload_success or not _needs_remote_copy:" in src


# ── the outputs prefix is swept ─────────────────────────────────────────


class _FakeBackend:
    """A storage backend that answers a listing and records the deletes."""

    def __init__(self, objects):
        self._objects = objects
        self.deleted: list[str] = []

    async def list_keys(self, prefix):
        return [row for row in self._objects if row["key"].startswith(prefix)]

    async def delete_keys(self, keys):
        self.deleted.extend(keys)
        return len(keys)


def _manager_with(monkeypatch, backend, *, backend_name="s3"):
    monkeypatch.setattr("utils.storage.get_storage_backend", lambda: _async(backend))
    monkeypatch.setattr(cleanup_tasks.config, "get_storage_backend_name", lambda: backend_name)
    return cleanup_tasks.CleanupManager()


async def _async(value):
    return value


def test_old_outputs_are_deleted_and_fresh_ones_kept(monkeypatch):
    now = time.time()
    backend = _FakeBackend(
        [
            {"key": "outputs/job-1/out.mp4", "last_modified": now - 90000, "size": 10},
            {"key": "outputs/job-2/out.mp4", "last_modified": now, "size": 10},
            {"key": "inputs/job-1/src.mp4", "last_modified": now - 90000, "size": 10},
        ]
    )
    manager = _manager_with(monkeypatch, backend)
    manager.s3_output_ttl = 24 * 3600

    deleted = asyncio.run(manager.cleanup_s3_outputs())

    assert deleted == 1
    # Only the stale result went, and nothing outside `outputs/` was touched.
    assert backend.deleted == ["outputs/job-1/out.mp4"]


def test_outputs_are_swept_with_everything_else(monkeypatch):
    """A prefix with no entry in cleanup_all is a prefix that never gets swept."""
    backend = _FakeBackend([])
    manager = _manager_with(monkeypatch, backend)

    async def _noop():
        return 0

    for name in (
        "cleanup_input_files",
        "cleanup_output_files",
        "cleanup_temp_files",
        "cleanup_thumbnails",
        "cleanup_stale_redis_jobs",
        "cleanup_stale_dedup_keys",
        "cleanup_stale_locks",
        "cleanup_empty_directories",
        "cleanup_s3_inputs",
        "cleanup_s3_uploads",
        "cleanup_s3_forwards",
    ):
        monkeypatch.setattr(manager, name, _noop)
    seen = {}

    async def _record():
        seen["ran"] = True
        return 0

    monkeypatch.setattr(manager, "cleanup_s3_outputs", _record)

    results = asyncio.run(manager.cleanup_all())

    assert seen.get("ran") is True
    assert "s3_outputs" in results


def test_the_output_ttl_is_configurable(monkeypatch):
    monkeypatch.setenv("S3_OUTPUTS_TTL", "600")

    assert cleanup_tasks.CleanupManager().s3_output_ttl == 600


def test_a_local_backend_does_not_run_the_remote_sweep(monkeypatch):
    """Nothing is uploaded for a local backend, so there is nothing to sweep."""
    backend = _FakeBackend([{"key": "outputs/job-1/out.mp4", "last_modified": 0, "size": 1}])
    manager = _manager_with(monkeypatch, backend, backend_name="local")

    assert asyncio.run(manager.cleanup_s3_outputs()) == 0
    assert backend.deleted == []
