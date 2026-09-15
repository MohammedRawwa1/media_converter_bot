"""The ``/session_status`` dashboard and the presence heartbeat behind it.

No Redis and no Telegram are involved: ``FakeRedis`` implements exactly the
commands the collector issues, and the presence store is driven through the same
patched accessor. What matters here is the arithmetic that operators act on -
which jobs are waiting vs running, and whose job is next in the queue - plus the
guarantee that a broken Redis degrades the report instead of hanging it.
"""

import asyncio
import json

import pytest

import utils.eventbus as eventbus
from utils import job_queue, presence
from utils.queue_admin import JOB_HASH_PREFIX
from utils.session_status import (
    collect_session_status,
    format_status,
    summarize,
)


class _FakePipeline:
    def __init__(self, client):
        self._client = client
        self._ops = []

    def hmget(self, key, fields):
        self._ops.append((key, fields))
        return self

    async def execute(self):
        results = [await self._client.hmget(key, fields) for key, fields in self._ops]
        self._ops = []
        return results


class FakeRedis:
    """Only the commands presence/job_queue/session_status actually use."""

    def __init__(self):
        self.strings: dict[str, str] = {}
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, list[str]] = {}
        self.hashes: dict[str, dict[str, str]] = {}

    async def ping(self):
        return True

    async def llen(self, key):
        return len(self.lists.get(key, []))

    async def zcard(self, key):
        return len(self.zsets.get(key, []))

    async def lrange(self, key, start, stop):
        values = self.lists.get(key, [])
        return list(values[start:]) if stop == -1 else list(values[start : stop + 1])

    async def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)
        return len(self.lists[key])

    async def get(self, key):
        return self.strings.get(key)

    async def setex(self, key, ttl, value):
        self.strings[key] = str(value)
        return True

    async def expire(self, key, ttl):
        return True

    async def hmget(self, key, fields):
        store = self.hashes.get(key, {})
        return [store.get(field) for field in fields]

    async def hset(self, key, mapping=None, **kwargs):
        fields = {**dict(mapping or {}), **kwargs}
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in fields.items()})
        return len(fields)

    def pipeline(self):
        return _FakePipeline(self)

    def scan_iter(self, match="*", count=100):
        prefix = match[:-1] if match.endswith("*") else match
        keys = sorted(set(self.strings) | set(self.lists) | set(self.zsets) | set(self.hashes))
        matched = [key for key in keys if key.startswith(prefix)]

        async def _gen():
            for key in matched:
                yield key

        return _gen()

    async def close(self):
        return None


@pytest.fixture
def fake_redis(monkeypatch, eventbus_env):
    """Point every shared Redis accessor at one fake client and quiet the broker."""
    client = FakeRedis()

    async def _get_redis():
        return client

    monkeypatch.setattr(job_queue, "get_redis", _get_redis)
    monkeypatch.setattr(eventbus, "get_settings", lambda: type("S", (), {"consumes_rabbitmq": False})())
    presence.reset_memory()
    yield client
    presence.reset_memory()


def _queued(job_id, owner):
    """A raw ``ffmpeg:jobs`` entry as the enqueue path writes it (head first)."""
    return json.dumps({"job_id": job_id, "chat_id": owner})


def _hash(redis, job_id, status, owner=None):
    fields = {"status": status}
    if owner is not None:
        fields["chat_id"] = str(owner)
    redis.hashes[f"{JOB_HASH_PREFIX}{job_id}"] = fields


# ── summarize (pure) ────────────────────────────────────────────────────


def test_summarize_counts_each_job_state():
    payload = summarize(
        jobs=[
            {"status": "processing", "owner": 1},
            {"status": "queued", "owner": 2},
            {"status": "done", "owner": 1},
            {"status": "error", "owner": 2},
            {"status": "cancelled", "owner": 3},
        ],
        queued=[{"job_id": "a", "owner": 2}],
        waiting=1,
        delayed=0,
        online_ids=[1, 2, 3],
    )

    assert payload["jobs"]["running"] == 1
    assert payload["jobs"]["done"] == 1
    assert payload["jobs"]["failed"] == 1
    assert payload["jobs"]["cancelled"] == 1
    assert payload["queues"]["jobs_online"] == 2  # 1 waiting + 1 running


def test_summarize_reports_the_turn_of_the_soonest_job():
    # The list is newest -> oldest, so the *last* entry runs next (turn 1).
    queued = [
        {"job_id": "newest", "owner": 10},
        {"job_id": "middle", "owner": 11},
        {"job_id": "oldest", "owner": 10},
    ]
    payload = summarize(jobs=[], queued=queued, waiting=3, delayed=0, online_ids=[10, 11], me_id=10)

    by_user = {row["user_id"]: row for row in payload["users"]["active"]}
    assert by_user[10]["waiting"] == 2
    assert by_user[10]["turn"] == 1  # its oldest entry is last in line = next to run
    assert by_user[11]["turn"] == 2
    assert payload["me"]["turn"] == 1


def test_summarize_ignores_entries_without_an_owner():
    payload = summarize(
        jobs=[{"status": "processing", "owner": None}],
        queued=[{"job_id": "x", "owner": None}],
        waiting=1,
        delayed=None,
        online_ids=[7],
    )

    assert payload["users"]["active"][0]["waiting"] == 0
    assert payload["users"]["active"][0]["running"] == 0
    assert payload["users"]["active"][0]["turn"] is None


def test_summarize_marks_truncated_scans():
    payload = summarize(
        jobs=[], queued=[], waiting=9, delayed=2, online_ids=[], jobs_truncated=True, queue_truncated=True
    )
    assert payload["queues"]["queue_truncated"] is True
    assert payload["queues"]["jobs_truncated"] is True


# ── format_status ───────────────────────────────────────────────────────


def test_format_shows_the_admin_dashboard():
    payload = summarize(
        jobs=[{"status": "processing", "owner": 5}],
        queued=[{"job_id": "j", "owner": 5}, {"job_id": "k", "owner": 6}],
        waiting=2,
        delayed=1,
        online_ids=[5, 6],
    )
    payload["redis"] = {"connected": True, "ping_ms": 1.2, "error": None}
    payload["queues"]["broker"] = {"backend": "redis", "configured": False, "queues": {}, "error": None}
    payload["users"]["source"] = "redis"
    payload["status"] = "ok"

    text = format_status(payload, is_admin=True)

    assert "Session &amp; Queue Status" in text
    assert "Waiting (queued): <b>2</b>" in text
    assert "Running: <b>1</b>" in text
    assert "Jobs online (waiting+running): <b>3</b>" in text
    assert "Users online:</b> <b>2</b>" in text
    assert "<code>5</code>" in text


def test_format_personal_view_hides_global_counts():
    payload = summarize(jobs=[], queued=[{"job_id": "j", "owner": 8}], waiting=1, delayed=0, online_ids=[8, 9], me_id=8)
    payload["redis"] = {"connected": True, "ping_ms": 0.5, "error": None}
    payload["users"]["source"] = "redis"

    text = format_status(payload, is_admin=False)

    assert "Your Session Status" in text
    assert "Your next job: <b>#1</b>" in text
    # A personal view must not leak the global queue/dashboard sections.
    assert "Waiting (queued)" not in text
    assert "Users online" not in text


def test_format_says_so_when_redis_is_down():
    payload = summarize(jobs=[], queued=[], waiting=None, delayed=None, online_ids=[])
    payload["redis"] = {"connected": False, "ping_ms": None, "error": "redis unavailable: boom"}
    payload["status"] = "degraded"
    payload["users"]["source"] = "memory"

    text = format_status(payload, is_admin=True)

    assert "Redis unreachable" in text
    assert "worker" in text
    assert "degraded" in text


# ── collect_session_status ──────────────────────────────────────────────


def test_collect_reads_queue_turn_and_online_users(fake_redis):
    r = fake_redis
    r.lists[job_queue.JOB_LIST] = [_queued("newest", 10), _queued("oldest", 10)]
    r.zsets[job_queue.DELAYED_SET] = ["delayed-1"]
    _hash(r, "run-1", "processing", owner=10)
    _hash(r, "done-1", "done", owner=11)
    r.strings[f"{presence.PRESENCE_PREFIX}10"] = "1"
    r.strings[f"{presence.PRESENCE_PREFIX}11"] = "1"

    payload = asyncio.run(collect_session_status(user_id=10, is_admin=True))

    assert payload["redis"]["connected"] is True
    assert payload["queues"]["waiting"] == 2
    assert payload["queues"]["delayed"] == 1
    assert payload["queues"]["running"] == 1
    assert payload["jobs"]["done"] == 1
    assert payload["users"]["online"] == 2
    assert payload["users"]["source"] == "redis"
    assert payload["me"]["turn"] == 1
    assert payload["status"] == "ok"


def test_collect_degrades_when_redis_is_unreachable(monkeypatch, eventbus_env):
    async def _boom():
        raise RuntimeError("REDIS_URL is not set")

    monkeypatch.setattr(job_queue, "get_redis", _boom)
    monkeypatch.setattr(eventbus, "get_settings", lambda: type("S", (), {"consumes_rabbitmq": False})())

    payload = asyncio.run(collect_session_status(user_id=1, is_admin=True))

    assert payload["status"] == "degraded"
    assert payload["redis"]["connected"] is False
    assert "redis unavailable" in payload["redis"]["error"]
    # The report still renders rather than the command blowing up.
    assert "Redis unreachable" in format_status(payload, is_admin=True)


# ── presence ────────────────────────────────────────────────────────────


def test_touch_then_snapshot_lists_the_user(fake_redis):
    asyncio.run(presence.touch(42))
    snap = asyncio.run(presence.snapshot())

    assert snap["source"] == "redis"
    assert snap["count"] == 1
    assert 42 in snap["user_ids"]
    assert fake_redis.strings[f"{presence.PRESENCE_PREFIX}42"]


def test_snapshot_falls_back_to_memory_without_redis(monkeypatch, eventbus_env):
    async def _boom():
        raise RuntimeError("no redis")

    monkeypatch.setattr(job_queue, "get_redis", _boom)
    presence.reset_memory()

    asyncio.run(presence.touch(5))
    snap = asyncio.run(presence.snapshot())

    assert snap["source"] == "memory"
    assert snap["count"] == 1
    assert snap["error"] and "no redis" in snap["error"]


def test_touch_throttles_repeat_writes(fake_redis, monkeypatch):
    monkeypatch.setattr(presence, "PRESENCE_TOUCH_INTERVAL", 3600)

    asyncio.run(presence.touch(7))
    first = fake_redis.strings[f"{presence.PRESENCE_PREFIX}7"]
    asyncio.run(presence.touch(7))

    assert fake_redis.strings[f"{presence.PRESENCE_PREFIX}7"] == first


def test_touch_ignores_junk_without_raising(fake_redis):
    for value in (None, "not-a-number", ""):
        asyncio.run(presence.touch(value))
    assert asyncio.run(presence.snapshot())["count"] == 0


# ── job hash attribution ────────────────────────────────────────────────


def test_enqueue_records_the_owner_in_the_job_hash(fake_redis, eventbus_env):
    """Per-user queue numbers depend on the hash carrying chat_id/user_id."""
    asyncio.run(job_queue.enqueue_job({"job_id": "j1", "chat_id": 99, "input_path": "storage/input/a.mp4"}))

    stored = fake_redis.hashes[f"{JOB_HASH_PREFIX}j1"]
    assert stored["chat_id"] == "99"
    assert stored["status"] == "queued"
