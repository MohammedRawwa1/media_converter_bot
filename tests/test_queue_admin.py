"""Backends behind ``/cancelall`` and ``/clear_cache``.

No Redis and no broker are needed: ``FakeRedis`` implements exactly the commands
``utils.queue_admin`` issues, and the broker is represented by a stub queue, so
these tests run in CI where neither service exists.

What is covered here: that a cancel-all drains *both* pipes (the Redis list, the
delayed zset, the job hashes a worker may already have popped, plus the broker
queues) while leaving finished jobs and live per-job keys alone, and that the
cache wipe covers every prefix the app writes but never touches job state.
"""

import asyncio
import json

import pytest

import utils.eventbus as eventbus  # patched by stub_broker: settings + queue lookup
import utils.storage as storage_module
from utils import job_queue, media_cache
from utils.eventbus import rabbit as rabbit_module
from utils.queue_admin import (
    DEDUP_PREFIX,
    JOB_HASH_PREFIX,
    LOCK_PREFIX,
    PROGRESS_PREFIX,
    cancel_all_jobs,
    clear_cache_keys,
)
from utils.route_cache import route_cache


class FakeRedis:
    """Minimal async Redis stand-in: only the commands queue_admin uses."""

    def __init__(self):
        self.strings: dict[str, str] = {}
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, list[str]] = {}
        self.hashes: dict[str, dict[str, str]] = {}

    async def lrange(self, key, start, stop):
        values = self.lists.get(key, [])
        return values[start:] if stop == -1 else values[start : stop + 1]

    async def zrange(self, key, start, stop):
        values = self.zsets.get(key, [])
        return values[start:] if stop == -1 else values[start : stop + 1]

    async def get(self, key):
        return self.strings.get(key)

    async def ping(self):
        return True

    async def llen(self, key):
        return len(self.lists.get(key, []))

    async def zcard(self, key):
        return len(self.zsets.get(key, []))

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def hset(self, key, mapping=None, **kwargs):
        fields = {**dict(mapping or {}), **kwargs}
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in fields.items()})
        return len(fields)

    async def delete(self, *keys):
        removed = 0
        for key in keys:
            for store in (self.strings, self.lists, self.zsets, self.hashes):
                if key in store:
                    del store[key]
                    removed += 1
        return removed

    def scan_iter(self, match="*", count=100):
        prefix = match[:-1] if match.endswith("*") else match
        keys = sorted(set(self.strings) | set(self.lists) | set(self.zsets) | set(self.hashes))
        matched = [key for key in keys if key.startswith(prefix)]

        async def _gen():
            for key in matched:
                yield key

        return _gen()

    async def eval(self, script, numkeys, key, owner):
        """Stands in for the lock-release Lua script (compare-and-delete)."""
        if self.strings.get(key) == owner:
            del self.strings[key]
            return 1
        return 0

    async def close(self):
        """No-op, matching the real ``job_queue.get_redis()`` proxy.

        Call sites such as ``cancel_job`` end with ``await r.close()``; the shared
        client's proxy swallows that so a caller can never close the connection out
        from under the rest of the process. Modelling it here keeps the fake honest
        instead of surfacing a spurious AttributeError from inside the command.
        """
        return None


def _job(job_id):
    return json.dumps({"job_id": job_id, "input_path": f"/data/{job_id}.mkv"})


@pytest.fixture
def fake_redis(monkeypatch, eventbus_env):
    """Point the shared Redis accessor at a fake client and neutralise the broker.

    ``eventbus_env`` clears the event-bus env vars and resets the settings cache.
    Without it a developer shell (or ``.env``) that carries a real ``RABBITMQ_URL``
    would make a cancel-all reach for a live broker, so the test would depend on
    - and hang against - whatever RabbitMQ happens to be configured.
    """
    client = FakeRedis()

    async def _get_redis():
        return client

    monkeypatch.setattr(job_queue, "get_redis", _get_redis)
    return client


@pytest.fixture
def stub_broker(monkeypatch):
    """Replace the event-bus settings and queue with controllable stubs."""

    def _install(*, consumes_rabbitmq: bool, purged: dict | None = None):
        purged = purged if purged is not None else {}
        calls: list[str] = []

        class StubSettings:
            def __init__(self):
                self.consumes_rabbitmq = consumes_rabbitmq

        class StubQueue:
            async def purge_queues(self):
                calls.append("purge_queues")
                return purged

        monkeypatch.setattr(eventbus, "get_settings", lambda: StubSettings())
        monkeypatch.setattr(rabbit_module, "get_queue", lambda: StubQueue())
        return calls

    return _install


def test_cancel_all_drains_the_redis_pipe_and_flags_running_jobs(fake_redis):
    r = fake_redis
    r.lists[job_queue.JOB_LIST] = [_job("queued-1"), _job("queued-2")]
    r.zsets[job_queue.DELAYED_SET] = [_job("delayed-1")]
    r.hashes[f"{JOB_HASH_PREFIX}running"] = {"status": "processing", "input_key": "k"}
    r.hashes[f"{JOB_HASH_PREFIX}done"] = {"status": "done"}
    r.strings[f"{PROGRESS_PREFIX}running"] = "50"
    r.strings[f"{LOCK_PREFIX}abc"] = "running"
    r.strings[f"{DEDUP_PREFIX}file-hash"] = "running"

    report = asyncio.run(cancel_all_jobs())

    assert (report.queued, report.delayed, report.in_flight) == (2, 1, 1)
    assert report.cancelled_jobs == 4
    assert report.progress_keys == 1
    assert report.locks_released == 1
    assert report.dedup_keys == 1
    assert report.errors == []

    # Both queue shapes are gone, so nothing can be popped or promoted later.
    assert job_queue.JOB_LIST not in r.lists
    assert job_queue.DELAYED_SET not in r.zsets

    # A running job stops only because its hash carries the flag.
    assert r.hashes[f"{JOB_HASH_PREFIX}running"]["cancel"] == "1"
    assert r.hashes[f"{JOB_HASH_PREFIX}running"]["status"] == "cancelled"
    assert "cancel" not in r.hashes[f"{JOB_HASH_PREFIX}done"]

    # Progress mirrors, the input lock and the now-stale dedup key are all cleared.
    assert r.strings == {}


def test_cancel_all_flags_queued_job_hashes_before_deleting_the_list(fake_redis):
    r = fake_redis
    r.lists[job_queue.JOB_LIST] = [_job("queued-1")]
    r.hashes[f"{JOB_HASH_PREFIX}queued-1"] = {"status": "queued"}

    asyncio.run(cancel_all_jobs())

    assert r.hashes[f"{JOB_HASH_PREFIX}queued-1"]["cancel"] == "1"


def test_cancel_all_keeps_a_pending_ingest_dedup_key(fake_redis):
    r = fake_redis
    r.strings[f"{DEDUP_PREFIX}mid-ingest"] = "pending"
    r.strings[f"{DEDUP_PREFIX}orphan"] = "job-that-never-existed"

    report = asyncio.run(cancel_all_jobs())

    # Only the dangling one goes: "pending" means the pipeline is still ingesting.
    assert report.dedup_keys == 1
    assert f"{DEDUP_PREFIX}mid-ingest" in r.strings
    assert f"{DEDUP_PREFIX}orphan" not in r.strings


def test_cancel_all_leaves_locks_owned_by_live_jobs_alone(fake_redis):
    r = fake_redis
    r.strings[f"{LOCK_PREFIX}live"] = "job-still-encoding"
    r.hashes[f"{JOB_HASH_PREFIX}job-still-encoding"] = {"status": "uploading"}

    report = asyncio.run(cancel_all_jobs())

    # "uploading" is active, so the hash is flagged... and its lock released with it.
    assert report.in_flight == 1
    assert report.locks_released == 1
    assert r.strings == {}


def test_cancel_all_purges_broker_queues_when_the_rabbitmq_pipe_is_configured(fake_redis, stub_broker):
    """No real broker is involved: the settings and queue objects are stubs."""
    purged = {"media.jobs.run": 3, "media.jobs.retry": 1, "media.jobs.dead": 0}
    calls = stub_broker(consumes_rabbitmq=True, purged=purged)

    report = asyncio.run(cancel_all_jobs())

    assert calls == ["purge_queues"]
    assert report.broker == purged


def test_cancel_all_skips_the_broker_on_the_redis_backend(fake_redis, stub_broker):
    calls = stub_broker(consumes_rabbitmq=False)

    report = asyncio.run(cancel_all_jobs())

    assert calls == []
    assert report.broker == {}


def test_cancel_all_reports_redis_failure_instead_of_raising(monkeypatch, eventbus_env):
    async def _boom():
        raise RuntimeError("REDIS_URL is not set")

    monkeypatch.setattr(job_queue, "get_redis", _boom)

    report = asyncio.run(cancel_all_jobs(purge_broker=False))

    assert report.cancelled_jobs == 0
    assert any("Redis unavailable" in err for err in report.errors)
    assert any("Redis unavailable" in line for line in report.as_lines())


def test_clear_cache_deletes_every_cache_prefix_but_never_job_state(fake_redis):
    r = fake_redis
    r.strings["cache:job:j1"] = "{}"
    r.strings["cache:file:bytes:file-unique-id"] = "payload"
    r.strings["cache:user:42"] = "{}"
    r.strings["cache:meta:analysis:abc"] = "{}"
    r.strings["cache:resp:menu"] = "{}"
    r.strings["routecache:status:j1"] = "{}"
    r.hashes[f"{JOB_HASH_PREFIX}j1"] = {"status": "done"}
    r.lists[job_queue.JOB_LIST] = [_job("queued-1")]

    report = asyncio.run(clear_cache_keys())

    assert report.prefixes == {
        "cache:job:": 1,
        "cache:user:": 1,
        "cache:meta:": 1,
        "cache:resp:": 1,
    }
    # The media cache is reported on its own: it is the descriptors *and* the
    # cached bodies under cache:file:*.
    assert report.media_cache == 1
    assert report.route_cache_keys == 1
    assert report.total_keys == 6
    assert report.errors == []

    # Job state is /cancelall's business, not the cache wipe's.
    assert r.hashes[f"{JOB_HASH_PREFIX}j1"] == {"status": "done"}
    assert r.lists[job_queue.JOB_LIST] == [_job("queued-1")]


def test_clear_cache_also_empties_the_in_memory_route_cache(fake_redis):
    route_cache.invalidate_prefix("")  # start from a clean slate
    route_cache.set("status:j1", {"ok": True}, ttl=60)
    assert route_cache.get("status:j1") is not None

    report = asyncio.run(clear_cache_keys())

    assert route_cache.get("status:j1") is None
    assert report.route_cache_memory >= 1


def test_clear_cache_reports_the_media_cache_on_its_own_line(fake_redis):
    fake_redis.strings["cache:file:some-uid"] = "{}"
    fake_redis.strings["cache:file:bytes:some-uid"] = "payload"

    report = asyncio.run(clear_cache_keys())

    assert report.media_cache == 2
    assert any("media cache" in line for line in report.as_lines())


def test_clear_cache_leaves_the_media_library_in_storage_by_default(fake_redis, monkeypatch):
    """Storage objects are not cache keys; a routine wipe must not delete them."""
    calls: list[str] = []

    class StubBackend:
        base_path = None

        async def list_keys(self, prefix):
            calls.append(f"list:{prefix}")
            return [{"key": "inputs/library/x/source"}]

        async def delete_keys(self, keys):
            calls.append("delete")
            return len(keys)

    monkeypatch.setattr(storage_module, "get_storage_backend", _backend(StubBackend()))

    report = asyncio.run(clear_cache_keys())

    assert calls == []
    assert report.media_storage_keys == 0


def test_clear_cache_purges_the_media_library_when_asked(fake_redis, monkeypatch):
    class StubBackend:
        base_path = None

        async def list_keys(self, prefix):
            assert prefix == media_cache.LIBRARY_KEY_PREFIX
            return [{"key": "inputs/library/a/source"}, {"key": "inputs/library/b/source"}]

        async def delete_keys(self, keys):
            assert sorted(keys) == ["inputs/library/a/source", "inputs/library/b/source"]
            return len(keys)

    monkeypatch.setattr(storage_module, "get_storage_backend", _backend(StubBackend()))

    report = asyncio.run(clear_cache_keys(clear_media_storage=True))

    assert report.media_storage_keys == 2
    assert any("media library objects in storage" in line for line in report.as_lines())


def test_clear_cache_walks_the_nested_local_media_library(fake_redis, monkeypatch, tmp_path):
    """LocalStorageBackend exposes its root as ``base``, and the library nests one
    level (``<hash>/source``) which a single-level ``list_keys`` cannot see."""
    import os

    nested = tmp_path / "inputs" / "library" / "abc"
    nested.mkdir(parents=True)
    (nested / "source").write_bytes(b"media")

    class LocalStub:
        base = str(tmp_path)

        async def list_keys(self, prefix):
            # Mirrors LocalStorageBackend: top-level files only, so the nested
            # object is invisible to it and only the os.walk fallback finds it.
            return []

        async def delete_keys(self, keys):
            removed = 0
            for key in keys:
                path = os.path.join(self.base, key)
                if os.path.exists(path):
                    os.remove(path)
                    removed += 1
            return removed

    monkeypatch.setattr(storage_module, "get_storage_backend", _backend(LocalStub()))

    report = asyncio.run(clear_cache_keys(clear_media_storage=True))

    assert report.media_storage_keys == 1
    assert not (nested / "source").exists()


def _backend(instance):
    async def _get_storage_backend():
        return instance

    return _get_storage_backend


def test_clear_cache_reports_redis_failure_instead_of_raising(monkeypatch, eventbus_env):
    async def _boom():
        raise RuntimeError("REDIS_URL is not set")

    monkeypatch.setattr(job_queue, "get_redis", _boom)

    report = asyncio.run(clear_cache_keys())

    assert report.total_keys == 0
    assert any("Redis unavailable" in err for err in report.errors)


def test_reports_render_every_counter_they_collect(fake_redis, stub_broker):
    stub_broker(consumes_rabbitmq=True, purged={"media.jobs.run": 2})

    queue_report = asyncio.run(cancel_all_jobs())
    cache_report = asyncio.run(clear_cache_keys())

    lines = queue_report.as_lines()
    assert any("media.jobs.run=2" in line for line in lines)
    assert any("Total jobs affected" in line for line in lines)

    cache_lines = cache_report.as_lines()
    assert any("cache:job:" in line for line in cache_lines)
    assert any("Total keys deleted" in line for line in cache_lines)
