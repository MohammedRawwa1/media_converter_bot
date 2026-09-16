"""Egress accounting: what leaves the bucket, and how close that is to the cap.

IDrive e2's free egress is a multiple of what you *store*, so the number that
matters is a ratio, not a total. These tests cover the three pieces that ratio
depends on: the counter moves when bytes are actually pulled, it survives Redis
being unreachable, and the rating against the stored total is arithmetic that
grades the way an operator would expect.

The S3 path is driven through a fake client, so nothing here reaches the network.
"""

import asyncio
import calendar

import pytest

from utils import session_status, storage


@pytest.fixture(autouse=True)
def _clean_counters(monkeypatch):
    """Start every case from an empty counter and no Redis.

    Both are per-test state: the process-local totals are module globals, and the
    default of "Redis is unreachable" is what makes these cases hermetic. The
    tests that exercise the shared counter re-patch ``get_redis`` themselves.
    """
    storage._EGRESS_LOCAL.clear()

    async def _no_redis():
        raise RuntimeError("REDIS_URL is not set")

    monkeypatch.setattr("utils.job_queue.get_redis", _no_redis)
    yield
    storage._EGRESS_LOCAL.clear()


# ── the counters ────────────────────────────────────────────────────────


def test_egress_records_bytes_and_accumulates_across_calls():
    assert asyncio.run(storage.record_egress(100)) == 100
    assert asyncio.run(storage.record_egress(50)) == 150

    snapshot = asyncio.run(storage.egress_snapshot())

    assert snapshot["object_bytes"] == 150
    # Redis was not reachable, so the reading is honest about its scope.
    assert snapshot["shared"] is False


def test_egress_ignores_nothing_sized_or_nonsense_values():
    """A zero-byte or unparseable count must not move the total or raise."""
    for value in (0, -5, None, "not-a-number"):
        assert asyncio.run(storage.record_egress(value)) == 0

    assert asyncio.run(storage.egress_snapshot())["object_bytes"] == 0


def test_egress_uses_the_shared_counter_when_redis_answers(monkeypatch):
    """With Redis up, the shared total wins over this process's own tally."""
    calls = []

    class _Client:
        async def incrby(self, key, amount):
            calls.append((key, amount))
            return 4242

        async def mget(self, *keys):
            return [4242, 7]

    async def _get_redis():
        return _Client()

    monkeypatch.setattr("utils.job_queue.get_redis", _get_redis)

    assert asyncio.run(storage.record_egress(10)) == 4242
    snapshot = asyncio.run(storage.egress_snapshot())

    assert snapshot["object_bytes"] == 4242
    assert snapshot["links_issued"] == 7
    assert snapshot["shared"] is True
    assert calls and calls[0][0].startswith(storage.EGRESS_REDIS_PREFIX)


def test_egress_warns_once_per_step_not_once_per_byte(monkeypatch, caplog):
    monkeypatch.setattr(storage, "EGRESS_WARN_STEP_BYTES", 100)

    with caplog.at_level("WARNING"):
        asyncio.run(storage.record_egress(99))  # still inside the first step
        assert caplog.text == ""
        asyncio.run(storage.record_egress(1))  # crosses it
        assert "egress" in caplog.text
        caplog.clear()
        asyncio.run(storage.record_egress(50))  # inside the next step: quiet
        assert caplog.text == ""


def test_period_is_the_utc_month():
    """A billing cycle is a calendar month, so the counter rolls over on the 1st."""

    def _at(*parts):
        return calendar.timegm((*parts, 0, 0, 0))

    assert storage.egress_period(_at(2026, 9, 1)) == "2026-09"
    assert storage.egress_period(_at(2026, 9, 30, 23, 59)) == "2026-09"
    assert storage.egress_period(_at(2026, 10, 1)) == "2026-10"


# ── the rating against the stored total ─────────────────────────────────


def test_snapshot_rates_egress_against_a_multiple_of_what_is_stored():
    """3x of 1000 stored bytes = 3000 free; the grade follows the ratio."""
    asyncio.run(storage.record_egress(1500))
    snapshot = asyncio.run(storage.egress_snapshot(stored_bytes=1000))

    assert snapshot["stored_bytes"] == 1000
    assert snapshot["allowance_bytes"] == 3000
    assert snapshot["percent"] == 50.0
    assert snapshot["status"] == "ok"


def test_snapshot_watches_then_flags_over():
    asyncio.run(storage.record_egress(2500))
    watch = asyncio.run(storage.egress_snapshot(stored_bytes=1000))
    assert watch["status"] == "watch"

    asyncio.run(storage.record_egress(1000))
    over = asyncio.run(storage.egress_snapshot(stored_bytes=1000))
    assert over["status"] == "over"


def test_snapshot_without_a_stored_reading_leaves_the_ratio_unknown():
    """No scan means no allowance to score against - say so, do not invent one."""
    asyncio.run(storage.record_egress(500))
    snapshot = asyncio.run(storage.egress_snapshot(stored_bytes=None))

    assert snapshot["object_bytes"] == 500
    assert snapshot["allowance_bytes"] is None
    assert snapshot["percent"] is None
    assert snapshot["status"] == "unknown"


def test_an_empty_bucket_has_no_free_egress_at_all():
    """The allowance scales with stored bytes, so an emptied bucket has none."""
    quiet = asyncio.run(storage.egress_snapshot(stored_bytes=0))
    assert quiet["status"] == "ok"

    asyncio.run(storage.record_egress(1))
    loud = asyncio.run(storage.egress_snapshot(stored_bytes=0))
    assert loud["status"] == "over"


# ── the backend hooks ───────────────────────────────────────────────────


def _s3_backend(monkeypatch, payload=b"x" * 64):
    """An S3 backend whose client writes *payload* instead of talking to a bucket."""

    class _FakeClient:
        def download_file(self, bucket, key, dest):
            with open(dest, "wb") as fh:
                fh.write(payload)

        def generate_presigned_url(self, operation, Params=None, ExpiresIn=None):
            return "https://example.invalid/presigned"

    fake_boto3 = type("FakeBoto3", (), {"client": staticmethod(lambda *a, **k: _FakeClient())})
    monkeypatch.setattr(storage, "boto3", fake_boto3)

    backend = storage.S3AsyncBackend(bucket="a-bucket", endpoint_url="https://example.invalid")
    backend._use_aioboto3 = False
    return backend


def test_download_records_the_bytes_written(monkeypatch, tmp_path):
    backend = _s3_backend(monkeypatch, payload=b"x" * 64)
    dest = tmp_path / "downloaded.bin"

    assert asyncio.run(backend.download_file("inputs/a.mp4", str(dest))) is True

    # The size on disk is the egress, so this is measured, not inferred.
    assert asyncio.run(storage.egress_snapshot())["object_bytes"] == 64


def test_a_failed_download_records_nothing(monkeypatch, tmp_path):
    class _Broken:
        def download_file(self, bucket, key, dest):
            raise OSError("no route to the bucket")

    fake_boto3 = type("FakeBoto3", (), {"client": staticmethod(lambda *a, **k: _Broken())})
    monkeypatch.setattr(storage, "boto3", fake_boto3)

    backend = storage.S3AsyncBackend(bucket="a-bucket", endpoint_url="https://example.invalid")
    backend._use_aioboto3 = False
    backend._boto_config = None

    with pytest.raises(OSError):
        asyncio.run(backend.download_file("inputs/a.mp4", str(tmp_path / "x.bin")))

    assert asyncio.run(storage.egress_snapshot())["object_bytes"] == 0


def test_presigned_get_records_a_link(monkeypatch):
    backend = _s3_backend(monkeypatch)

    url = asyncio.run(backend.generate_presigned_get("outputs/j/out.mp4"))

    assert url == "https://example.invalid/presigned"
    # A link is exposure, not measured traffic, so it is counted apart.
    snapshot = asyncio.run(storage.egress_snapshot())
    assert snapshot["links_issued"] == 1
    assert snapshot["object_bytes"] == 0


# ── the dashboard block ─────────────────────────────────────────────────


def _storage_block(backend="s3", **egress):
    row = {
        "period": "2026-09",
        "object_bytes": 0,
        "links_issued": 0,
        "allowance_bytes": None,
        "percent": None,
        "status": "unknown",
        "shared": True,
    }
    row.update(egress)
    return {"backend": backend, "egress": row}


def test_dashboard_shows_egress_against_the_allowance():
    text = "\n".join(
        session_status._egress_lines(
            _storage_block(
                object_bytes=1500 * 1024**2,
                allowance_bytes=3000 * 1024**2,
                percent=50.0,
                status="ok",
            )
        )
    )

    assert "Egress 2026-09" in text
    assert "1.5 GB" in text  # what left the bucket
    assert "2.9 GB" in text  # 3x of stored - the actual limit
    assert "50%" in text


def test_dashboard_flags_when_the_allowance_is_being_approached():
    text = "\n".join(
        session_status._egress_lines(
            _storage_block(
                object_bytes=2500,
                allowance_bytes=3000,
                percent=83.0,
                status="watch",
                links_issued=4,
            )
        )
    )

    assert "⚠️" in text
    assert "4" in text and "presigned link" in text
    # The point of the warning: the allowance tracks stored bytes, not traffic.
    assert "shrinks when the bucket is emptied" in text


def test_dashboard_says_when_the_count_is_only_process_local():
    text = "\n".join(session_status._egress_lines(_storage_block(shared=False, object_bytes=10)))

    assert "process-local" in text


def test_dashboard_has_no_egress_block_for_the_local_backend():
    """The policy belongs to the remote bucket, so a local tree shows nothing."""
    assert session_status._egress_lines(_storage_block(backend="local", object_bytes=10)) == []
    assert session_status._egress_lines({}) == []
