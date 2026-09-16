"""Bounds on the in-memory state the long-lived processes accumulate.

Every case here is the same shape: a mapping keyed by something a caller controls,
written on every request or job and only ever read back. Nothing evicted them, so
each one grew for the life of the process.
"""

import asyncio
import importlib
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils import storage  # noqa: E402
from utils.web_rate_limiter import WebRateLimiter  # noqa: E402
from workers import ffmpeg_worker  # noqa: E402

# ─── rate limiter buckets ────────────────────────────────────────────────────


def test_a_new_client_makes_a_bucket():
    limiter = WebRateLimiter()
    assert limiter.check_limit("upload", "1.2.3.4") is True
    assert ("upload", "1.2.3.4") in limiter.buckets


def test_idle_buckets_are_pruned():
    limiter = WebRateLimiter()
    limiter.bucket_ttl = 10
    limiter.max_buckets = 2
    old = time.time() - 3600
    for i in range(3):
        limiter.buckets[("upload", f"10.0.0.{i}")] = (1.0, old)

    limiter.check_limit("upload", "new-client")
    # The stale ones are gone and the caller's own bucket is still there.
    assert ("upload", "new-client") in limiter.buckets
    assert len(limiter.buckets) == 1


def test_the_cap_holds_under_a_burst_of_distinct_clients():
    """The cap is what stops an attacker-supplied client key from being a leak."""
    limiter = WebRateLimiter()
    limiter.bucket_ttl = 3600  # nothing is idle, so only the cap can help
    limiter.max_buckets = 64
    for i in range(500):
        limiter.check_limit("health", f"203.0.113.{i}")
    assert len(limiter.buckets) <= limiter.max_buckets


def test_prune_reports_what_it_dropped_without_touching_live_buckets():
    limiter = WebRateLimiter()
    limiter.bucket_ttl = 10
    limiter.check_limit("upload", "fresh-client")
    limiter.buckets[("upload", "stale")] = (1.0, time.time() - 100)

    assert limiter.prune() == 1
    assert ("upload", "fresh-client") in limiter.buckets


def test_prune_is_a_noop_on_an_empty_limiter():
    assert WebRateLimiter().prune() == 0


# ─── web fallback job store ──────────────────────────────────────────────────


@pytest.fixture()
def webapp():
    return importlib.import_module("web.webapp")


@pytest.fixture()
def store(webapp, monkeypatch):
    """A clean fallback store whose bounds are restored after the test."""
    monkeypatch.setattr(webapp, "JOB_STORE", {})
    return webapp


def test_job_store_entries_are_timestamped(store, monkeypatch):
    monkeypatch.setattr(store, "JOB_STORE_TTL_SECONDS", 60)
    now = time.time()
    store.JOB_STORE["a"] = {"job_id": "a", "created_at": now}
    store.JOB_STORE["b"] = {"job_id": "b", "created_at": now - 10_000}

    assert store._job_store_prune(now) == 1
    assert list(store.JOB_STORE) == ["a"]


def test_job_store_is_capped_keeping_the_newest(store, monkeypatch):
    monkeypatch.setattr(store, "JOB_STORE_TTL_SECONDS", 0)  # isolate the cap from the TTL
    monkeypatch.setattr(store, "JOB_STORE_MAX_ENTRIES", 4)
    base = time.time()
    for i in range(10):
        store.JOB_STORE[f"job{i}"] = {"job_id": f"job{i}", "created_at": base + i}

    store._job_store_prune(base + 10)

    assert len(store.JOB_STORE) == 4
    assert "job9" in store.JOB_STORE
    assert "job0" not in store.JOB_STORE


def test_job_store_missing_timestamp_is_treated_as_expired(store, monkeypatch):
    monkeypatch.setattr(store, "JOB_STORE_TTL_SECONDS", 60)
    store.JOB_STORE["ancient"] = {"job_id": "ancient"}
    store._job_store_prune(time.time())
    assert store.JOB_STORE == {}


# ─── counters keep a TTL ─────────────────────────────────────────────────────


class _FakeRedis:
    def __init__(self):
        self.expires = []
        self.values = {}

    async def incrby(self, key, amount):
        self.values[key] = self.values.get(key, 0) + int(amount)
        return self.values[key]

    async def incr(self, key):
        self.values[key] = self.values.get(key, 0) + 1
        return self.values[key]

    async def expire(self, key, ttl):
        self.expires.append((key, ttl))
        return True


def _fake_redis(monkeypatch, client):
    async def _get():
        return client

    monkeypatch.setattr("utils.job_queue.get_redis", _get)
    return client


def test_egress_counter_gets_a_ttl(monkeypatch):
    storage._EGRESS_LOCAL.clear()
    client = _fake_redis(monkeypatch, _FakeRedis())
    asyncio.run(storage.record_egress(1024))
    assert client.expires and client.expires[0][1] == storage.COUNTER_TTL_SECONDS


def test_source_cache_counters_get_a_ttl(monkeypatch):
    storage._SOURCE_CACHE_LOCAL.clear()
    client = _fake_redis(monkeypatch, _FakeRedis())
    asyncio.run(storage.record_source_cache(True, nbytes=10))
    expired = {key for key, _ttl in client.expires}
    assert any(k.endswith(":hits") for k in expired)
    assert any(k.endswith(":bytes") for k in expired)


def test_a_ttl_failure_does_not_lose_the_count(monkeypatch):
    storage._EGRESS_LOCAL.clear()
    client = _fake_redis(monkeypatch, _FakeRedis())

    async def _boom(key, ttl):
        raise RuntimeError("no expire")

    client.expire = _boom
    assert asyncio.run(storage.record_egress(1024)) == 1024


def test_counters_are_disabled_by_setting_the_ttl_to_zero(monkeypatch):
    storage._EGRESS_LOCAL.clear()
    client = _fake_redis(monkeypatch, _FakeRedis())
    monkeypatch.setattr(storage, "COUNTER_TTL_SECONDS", 0)
    asyncio.run(storage.record_egress(1024))
    assert client.expires == []


# ─── thumbnails are not left behind ──────────────────────────────────────────


def test_the_local_thumbnail_copy_is_removed_after_delivery(tmp_path, monkeypatch):
    monkeypatch.setattr(ffmpeg_worker.config, "TEMP_PATH", str(tmp_path))
    directory = tmp_path / "worker_thumb_abc"
    directory.mkdir()
    thumb = directory / "thumb.jpg"
    thumb.write_bytes(b"jpeg")

    job = {"_local_thumb": str(thumb)}
    ffmpeg_worker._cleanup_local_thumb(job)

    assert not directory.exists()
    assert "_local_thumb" not in job


def test_a_users_custom_thumbnail_is_never_deleted(tmp_path):
    custom = tmp_path / "thumbnails" / "mine.jpg"
    custom.parent.mkdir(parents=True)
    custom.write_bytes(b"jpeg")

    job = {"_local_thumb": str(custom)}
    ffmpeg_worker._cleanup_local_thumb(job)

    assert custom.exists()
    assert job["_local_thumb"] == str(custom)


def test_directory_cleanup_leaves_the_placeholders_alone(tmp_path):
    """README.md/.gitkeep keep a storage directory in a checkout; not stale data."""
    from tasks import cleanup_tasks as cleanup_mod

    old = time.time() - 10**6
    for name in ("README.md", ".gitkeep", "stale.bin"):
        path = tmp_path / name
        path.write_text("x")
        os.utime(path, (old, old))

    removed = asyncio.run(cleanup_mod.CleanupManager()._cleanup_directory(str(tmp_path), 3600))

    assert removed == 1
    assert sorted(p.name for p in tmp_path.iterdir()) == [".gitkeep", "README.md"]


def test_cleanup_tolerates_jobs_without_a_thumbnail():
    ffmpeg_worker._cleanup_local_thumb({})
    ffmpeg_worker._cleanup_local_thumb({"_local_thumb": None})


def test_a_cached_probe_does_not_hand_out_a_deleted_thumbnail(tmp_path, monkeypatch):
    out = tmp_path / "out.mp4"
    out.write_bytes(b"x")

    thumb = tmp_path / "probe_thumb.jpg"
    thumb.write_bytes(b"jpeg")

    async def _probe(path):
        return {"duration": 5}, str(thumb)

    monkeypatch.setattr("utils.ffmpeg_runner.probe_video_for_delivery", _probe)
    ffmpeg_worker._output_probe_cache.clear()

    meta, tp = asyncio.run(ffmpeg_worker._probe_output_metadata(str(out)))
    assert tp == str(thumb)

    # The consumer removes the thumbnail directory, as the delivery paths do.
    thumb.unlink()

    meta2, tp2 = asyncio.run(ffmpeg_worker._probe_output_metadata(str(out)))
    assert meta2 == {"duration": 5}
    assert tp2 is None
    # And the cache now holds the corrected entry, so the next call is right too.
    assert ffmpeg_worker._output_probe_cache[str(out)][1] is None
    ffmpeg_worker._output_probe_cache.clear()
