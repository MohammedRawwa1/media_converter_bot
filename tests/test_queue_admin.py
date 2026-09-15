"""Backends behind ``/cancelall`` and ``/clear_cache``.

No Redis and no broker are needed: ``FakeRedis`` implements exactly the commands
``utils.queue_admin`` issues, and the broker is represented by a stub queue, so
these tests run in CI where neither service exists.

What is covered here: that a cancel-all drains *both* pipes (the Redis list, the
delayed zset, the job hashes a worker may already have popped, plus the broker
queues) while leaving finished jobs and live per-job keys alone, that it takes
the cancelled batches down with it (state, resume record and progress bar - which
is what used to need ``scripts/cleanup_stale_redis.py``), and that the cache wipe
covers every prefix the app writes but never touches job state.
"""

import asyncio
import json

import pytest

import utils.eventbus as eventbus  # patched by stub_broker: settings + queue lookup
import utils.storage as storage_module
from utils import batch_pipeline, job_queue, media_cache
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
        self.sets: dict[str, set[str]] = {}

    async def sadd(self, key, *values):
        self.sets.setdefault(key, set()).update(str(value) for value in values)
        return len(values)

    async def srem(self, key, *values):
        members = self.sets.get(key, set())
        before = len(members)
        members -= {str(value) for value in values}
        return before - len(members)

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    async def set(self, key, value, nx=False, px=None, ex=None):
        self.strings[key] = str(value)
        return True

    async def lrange(self, key, start, stop):
        values = self.lists.get(key, [])
        return values[start:] if stop == -1 else values[start : stop + 1]

    async def zrange(self, key, start, stop):
        values = self.zsets.get(key, [])
        return values[start:] if stop == -1 else values[start : stop + 1]

    async def get(self, key):
        return self.strings.get(key)

    async def exists(self, *keys):
        return sum(1 for key in keys if key in self.strings or key in self.sets or key in self.hashes)

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
            for store in (self.strings, self.lists, self.zsets, self.hashes, self.sets):
                if key in store:
                    del store[key]
                    removed += 1
        return removed

    def scan_iter(self, match="*", count=100):
        prefix = match[:-1] if match.endswith("*") else match
        keys = sorted(set(self.strings) | set(self.lists) | set(self.zsets) | set(self.hashes) | set(self.sets))
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


class _StubBot:
    """Records the batch progress messages a cancel-all removes from the chat."""

    def __init__(self, *, fail=False):
        self.deleted: list[tuple[int, int]] = []
        self.fail = fail

    async def delete_message(self, chat_id, message_id):
        if self.fail:
            raise RuntimeError("not enough rights")
        self.deleted.append((chat_id, message_id))


def _seed_batch(r, batch_id, *, status="processing", msg="-100:777", owner=42):
    r.hashes[f"{JOB_HASH_PREFIX}member-1"] = {"status": status, "batch_id": batch_id}
    r.sets[batch_pipeline.batch_jobs_key(batch_id)] = {"member-1"}
    r.strings[batch_pipeline.batch_total_key(batch_id)] = "3"
    r.strings[batch_pipeline.batch_progress_key(batch_id)] = "1"
    if msg is not None:
        r.strings[batch_pipeline.batch_message_key(batch_id)] = msg
    r.sets.setdefault(batch_pipeline.ACTIVE_BATCHES_KEY, set()).add(batch_id)
    if owner is not None:
        r.sets.setdefault(f"ffmpeg:batch:resume:{owner}", set()).add(batch_id)


def test_cancel_all_takes_the_batches_down_and_deletes_their_bars(fake_redis):
    r = fake_redis
    _seed_batch(r, "batch-a")
    bot = _StubBot()

    report = asyncio.run(cancel_all_jobs(bot=bot))

    assert report.batches == 1
    assert report.batch_messages == 1
    assert bot.deleted == [(-100, 777)]
    # State, location and index all go. The tombstone stays: a worker that is
    # still finishing the flagged member must not repost the bar.
    assert batch_pipeline.batch_total_key("batch-a") not in r.strings
    assert batch_pipeline.batch_message_key("batch-a") not in r.strings
    assert batch_pipeline.batch_jobs_key("batch-a") not in r.sets
    assert "batch-a" not in r.sets[batch_pipeline.ACTIVE_BATCHES_KEY]
    assert r.sets["ffmpeg:batch:resume:42"] == set()
    assert r.strings[batch_pipeline.batch_cancel_key("batch-a")]
    assert "Batches cleared:     1" in "\n".join(report.as_lines())
    assert report.errors == []


def test_cancel_all_counts_a_batch_whose_bar_is_already_gone(fake_redis):
    # The worker deletes a batch's message when it finishes, so a batch can be
    # perfectly stale with no bar left to remove.
    r = fake_redis
    _seed_batch(r, "batch-a", msg=None)
    bot = _StubBot()

    report = asyncio.run(cancel_all_jobs(bot=bot))

    assert report.batches == 1
    assert report.batch_messages == 0
    assert bot.deleted == []


def test_cancel_all_survives_a_bar_it_cannot_delete(fake_redis):
    r = fake_redis
    _seed_batch(r, "batch-a")

    report = asyncio.run(cancel_all_jobs(bot=_StubBot(fail=True)))

    # The count is what actually happened; the state still went.
    assert report.batches == 1
    assert report.batch_messages == 0
    assert batch_pipeline.batch_total_key("batch-a") not in r.strings
    assert report.errors == []


def test_cancel_all_without_a_bot_still_clears_the_batch_state(fake_redis):
    r = fake_redis
    _seed_batch(r, "batch-a")

    report = asyncio.run(cancel_all_jobs())

    assert report.batches == 1
    assert report.batch_messages == 0
    assert batch_pipeline.batch_total_key("batch-a") not in r.strings
    assert "Batch keys removed" in "\n".join(report.as_lines())


def test_a_second_cancel_all_does_not_reclear_the_batches_it_took_down(fake_redis):
    """Running /cancelall twice must not keep reporting the same dead batches.

    The sweep leaves every batch it takes down a tombstone on purpose - a worker
    still finishing one of its members must not put the bar back - and that
    tombstone sits under the same prefix the sweep scans for batch ids. Treating
    it as a batch again re-purged a batch that had nothing left *and* rewrote the
    tombstone, pushing its TTL out another 30 days: the same handful of long-dead
    batches showed up as "Batches cleared: 2" on every later run, while
    ``scripts/cleanup_stale_redis.py`` (which deletes the tombstone) cleared them
    for good.
    """
    r = fake_redis
    _seed_batch(r, "batch-a")
    _seed_batch(r, "batch-b")

    first = asyncio.run(cancel_all_jobs())
    second = asyncio.run(cancel_all_jobs())

    assert first.batches == 2
    assert second.batches == 0
    assert second.batch_keys == 0


def test_cancel_all_flags_the_running_member_before_it_sweeps_the_batch(fake_redis):
    """Order matters: the sweep must not see the member as still running."""
    r = fake_redis
    _seed_batch(r, "batch-a", status="uploading")

    report = asyncio.run(cancel_all_jobs())

    assert r.hashes[f"{JOB_HASH_PREFIX}member-1"]["status"] == "cancelled"
    assert report.in_flight == 1
    assert report.batches_kept == 0
    assert report.batches == 1


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
