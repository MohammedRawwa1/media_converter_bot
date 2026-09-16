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
from utils import batch_pipeline, job_queue, presence, session_status, storage
from utils.queue_admin import JOB_HASH_PREFIX
from utils.session_status import (
    collect_session_status,
    format_status,
    parse_status_callback,
    refresh_data,
    restart_data,
    status_keyboard,
    summarize,
    summarize_capacity,
    summarize_memory,
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

    async def exists(self, *keys):
        return sum(1 for key in keys if key in self.strings)

    async def setex(self, key, ttl, value):
        self.strings[key] = str(value)
        return True

    async def set(self, key, value, nx=False, px=None, ex=None):
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


# ── capacity (ffmpeg concurrency + memory headroom) ─────────────────────


def test_capacity_reports_peak_rss_and_headroom(monkeypatch):
    monkeypatch.setattr(batch_pipeline, "MEMORY_CEILING_BYTES", 800)
    monkeypatch.setattr(batch_pipeline, "MAX_CONCURRENT_FFMPEG", 1)

    capacity = summarize_capacity(slots_used=1, worker_rss={"w1": 300, "w2": "500"})

    assert capacity["ffmpeg_running"] == 1
    assert capacity["ffmpeg_limit"] == 1
    assert capacity["workers_reporting"] == 2
    # The *busiest* worker is what decides the headroom - the others are not at risk.
    assert capacity["peak_rss_bytes"] == 500
    assert capacity["headroom_bytes"] == 300


def test_capacity_says_no_data_rather_than_zero(monkeypatch):
    monkeypatch.setattr(batch_pipeline, "MEMORY_CEILING_BYTES", 800)
    capacity = summarize_capacity(slots_used=None, worker_rss={})
    assert capacity["peak_rss_bytes"] is None
    assert capacity["headroom_bytes"] is None


def test_capacity_headroom_is_negative_over_the_ceiling(monkeypatch):
    monkeypatch.setattr(batch_pipeline, "MEMORY_CEILING_BYTES", 400)
    capacity = summarize_capacity(slots_used=0, worker_rss={"w1": 900})
    assert capacity["headroom_bytes"] == -500


def test_format_shows_capacity_on_the_admin_dashboard(monkeypatch):
    monkeypatch.setattr(batch_pipeline, "MEMORY_CEILING_BYTES", 800 * 1024 * 1024)
    monkeypatch.setattr(batch_pipeline, "MAX_CONCURRENT_FFMPEG", 1)
    payload = summarize(jobs=[], queued=[], waiting=0, delayed=0, online_ids=[])
    payload["redis"] = {"connected": True, "ping_ms": 1.0, "error": None}
    payload["capacity"] = summarize_capacity(slots_used=1, worker_rss={"w1": 300 * 1024 * 1024})

    text = format_status(payload, is_admin=True)

    assert "Capacity" in text
    assert "ffmpeg running: <b>1/1</b>" in text
    assert "headroom <b>500.0 MB</b>" in text


def test_format_warns_when_a_worker_is_over_the_ceiling(monkeypatch):
    monkeypatch.setattr(batch_pipeline, "MEMORY_CEILING_BYTES", 100 * 1024 * 1024)
    payload = summarize(jobs=[], queued=[], waiting=0, delayed=0, online_ids=[])
    payload["redis"] = {"connected": True, "ping_ms": 1.0, "error": None}
    payload["capacity"] = summarize_capacity(slots_used=1, worker_rss={"w1": 150 * 1024 * 1024})

    text = format_status(payload, is_admin=True)
    assert "over ceiling by <b>50.0 MB</b>" in text


def test_personal_view_does_not_leak_capacity(monkeypatch):
    monkeypatch.setattr(batch_pipeline, "MEMORY_CEILING_BYTES", 800)
    payload = summarize(jobs=[], queued=[], waiting=0, delayed=0, online_ids=[8], me_id=8)
    payload["redis"] = {"connected": True, "ping_ms": 1.0, "error": None}
    payload["capacity"] = summarize_capacity(slots_used=1, worker_rss={"w1": 10})

    text = format_status(payload, is_admin=False)
    assert "Capacity" not in text
    assert "ffmpeg running" not in text


def test_capacity_is_hidden_when_redis_is_down(monkeypatch):
    payload = summarize(jobs=[], queued=[], waiting=None, delayed=None, online_ids=[])
    payload["redis"] = {"connected": False, "ping_ms": None, "error": "boom"}
    payload["capacity"] = summarize_capacity(slots_used=None, worker_rss={})

    assert "Capacity" not in format_status(payload, is_admin=True)


# ── memory (this process + the box it runs on) ──────────────────────────


MB = 1024 * 1024
GB = 1024 * 1024 * 1024


def test_memory_prefers_the_container_limit_over_the_host():
    # The cgroup limit is what kills the process, so it wins over the host total.
    memory = summarize_memory(
        {
            "cgroup_limit_bytes": 1000,
            "cgroup_used_bytes": 250,
            "host_total_bytes": 9000,
            "host_used_bytes": 8000,
        }
    )

    assert memory["total_bytes"] == 1000
    assert memory["used_bytes"] == 250
    assert memory["free_bytes"] == 750
    assert memory["scope"] == "container"
    assert memory["percent"] == 25.0
    assert memory["pressure"] == "ok"


def test_memory_falls_back_to_the_host_without_a_cgroup():
    memory = summarize_memory({"host_total_bytes": 1000, "host_used_bytes": 900})

    assert memory["scope"] == "host"
    assert memory["percent"] == 90.0
    assert memory["pressure"] == "high"


def test_memory_flags_critical_pressure():
    memory = summarize_memory({"host_total_bytes": 100, "host_used_bytes": 97})
    assert memory["pressure"] == "critical"


def test_memory_never_pairs_a_cgroup_limit_with_the_hosts_usage():
    memory = summarize_memory({"cgroup_limit_bytes": 1000, "host_used_bytes": 900})

    assert memory["total_bytes"] == 1000
    assert memory["used_bytes"] is None
    assert memory["percent"] is None
    assert memory["pressure"] == "unknown"


def test_memory_is_unknown_rather_than_zero_without_readings():
    memory = summarize_memory({})

    assert memory["self_rss_bytes"] is None
    assert memory["total_bytes"] is None
    assert memory["percent"] is None
    assert memory["pressure"] == "unknown"


def test_format_shows_memory_on_the_admin_dashboard():
    payload = summarize(jobs=[], queued=[], waiting=0, delayed=0, online_ids=[])
    payload["redis"] = {"connected": True, "ping_ms": 1.0, "error": None}
    payload["memory"] = summarize_memory(
        {
            "self_rss_bytes": 200 * MB,
            "self_peak_bytes": 640 * MB,
            "cgroup_limit_bytes": 2 * GB,
            "cgroup_used_bytes": 1 * GB,
        }
    )

    text = format_status(payload, is_admin=True)

    assert "🧠 <b>Memory</b>" in text
    assert "Bot process: <b>200.0 MB</b> (peak 640.0 MB)" in text
    assert "Container: <b>1.0 GB</b> of 2.0 GB — <b>50%</b> used" in text
    assert "Free: <b>1.0 GB</b>" in text


def test_format_warns_when_memory_pressure_is_high():
    payload = summarize(jobs=[], queued=[], waiting=0, delayed=0, online_ids=[])
    payload["redis"] = {"connected": True, "ping_ms": 1.0, "error": None}
    payload["memory"] = summarize_memory({"host_total_bytes": 1000, "host_used_bytes": 950})

    text = format_status(payload, is_admin=True)

    assert "<b>95%</b> used 🔴" in text


def test_personal_view_does_not_leak_memory():
    payload = summarize(jobs=[], queued=[], waiting=0, delayed=0, online_ids=[8], me_id=8)
    payload["redis"] = {"connected": True, "ping_ms": 1.0, "error": None}
    payload["memory"] = summarize_memory({"self_rss_bytes": 10 * MB, "host_total_bytes": 100})

    assert "Memory" not in format_status(payload, is_admin=False)


# ── storage (object count + bytes) ──────────────────────────────────────


# The scan itself is exercised by the tests below; everything else only needs it
# to be hermetic, so the autouse stub stands in for it and the storage tests call
# the captured original explicitly.
_REAL_STORAGE_SNAPSHOT = session_status._storage_snapshot


@pytest.fixture(autouse=True)
def stub_storage_scan(monkeypatch):
    """Keep every test off the real storage backend."""

    async def _stub(*, force=False):
        return {
            "backend": "local",
            "location": "storage",
            "objects": 0,
            "bytes": 0,
            "groups": {},
            "truncated": False,
            "cached": False,
            "scanned_at": None,
            "error": None,
        }

    monkeypatch.setattr(session_status, "_storage_snapshot", _stub)
    yield


class _StubBackend:
    """A storage backend that reports whatever a test tells it to."""

    def __init__(self, readings=None, error=None):
        self.bucket = "stub-bucket"
        self._readings = readings
        self._error = error
        self.calls = 0

    async def usage(self, *, max_objects=5000):
        self.calls += 1
        if self._error is not None:
            raise self._error
        return self._readings


async def _no_cache():
    return None


async def _no_write(*args, **kwargs):
    return None


def _patch_backend(monkeypatch, backend):
    async def _get_backend():
        return backend

    monkeypatch.setattr(storage, "get_storage_backend", _get_backend)


def test_storage_snapshot_reads_the_backend(monkeypatch):
    backend = _StubBackend(
        {
            "backend": "s3",
            "location": "stub-bucket",
            "objects": 3,
            "bytes": 2048,
            "groups": {"uploads/": {"objects": 3, "bytes": 2048}},
            "truncated": False,
        }
    )
    _patch_backend(monkeypatch, backend)
    monkeypatch.setattr(session_status, "_read_storage_cache", _no_cache)
    monkeypatch.setattr(session_status, "_write_storage_cache", _no_write)

    snapshot = asyncio.run(_REAL_STORAGE_SNAPSHOT())

    assert snapshot["backend"] == "s3"
    assert snapshot["location"] == "stub-bucket"
    assert snapshot["objects"] == 3
    assert snapshot["bytes"] == 2048
    assert snapshot["cached"] is False
    assert snapshot["scanned_at"]
    assert snapshot["error"] is None


def test_storage_snapshot_reuses_a_cached_scan(monkeypatch):
    backend = _StubBackend({"backend": "s3", "objects": 99, "bytes": 99, "groups": {}})
    _patch_backend(monkeypatch, backend)

    async def _cached():
        return {
            "backend": "s3",
            "location": "bucket",
            "objects": 7,
            "bytes": 700,
            "groups": {},
            "truncated": False,
            "scanned_at": 1.0,
        }

    monkeypatch.setattr(session_status, "_read_storage_cache", _cached)

    snapshot = asyncio.run(_REAL_STORAGE_SNAPSHOT())

    assert snapshot["cached"] is True
    assert snapshot["objects"] == 7
    assert backend.calls == 0


def test_storage_snapshot_forces_a_fresh_scan(monkeypatch):
    backend = _StubBackend({"backend": "s3", "objects": 99, "bytes": 99, "groups": {}})
    _patch_backend(monkeypatch, backend)

    async def _cached():
        return {"objects": 7}

    monkeypatch.setattr(session_status, "_read_storage_cache", _cached)
    monkeypatch.setattr(session_status, "_write_storage_cache", _no_write)

    snapshot = asyncio.run(_REAL_STORAGE_SNAPSHOT(force=True))

    # The Refresh button must never answer with the cached number.
    assert backend.calls == 1
    assert snapshot["cached"] is False
    assert snapshot["objects"] == 99


def test_storage_snapshot_reports_a_failed_scan(monkeypatch):
    _patch_backend(monkeypatch, _StubBackend(error=RuntimeError("AccessDenied")))
    monkeypatch.setattr(session_status, "_read_storage_cache", _no_cache)

    snapshot = asyncio.run(_REAL_STORAGE_SNAPSHOT())

    assert "AccessDenied" in snapshot["error"]
    assert snapshot["objects"] is None


def test_storage_snapshot_explains_a_backend_that_cannot_count(monkeypatch):
    class _Opaque:
        base = "storage"

    _patch_backend(monkeypatch, _Opaque())
    monkeypatch.setattr(session_status, "_read_storage_cache", _no_cache)

    snapshot = asyncio.run(_REAL_STORAGE_SNAPSHOT())

    assert "cannot report its usage" in snapshot["error"]


def test_format_shows_storage_on_the_admin_dashboard():
    payload = summarize(jobs=[], queued=[], waiting=0, delayed=0, online_ids=[])
    payload["redis"] = {"connected": True, "ping_ms": 1.0, "error": None}
    payload["storage"] = {
        "backend": "s3",
        "location": "my-bucket",
        "objects": 42,
        "bytes": 3 * GB,
        "groups": {
            "inputs/": {"objects": 2, "bytes": 1 * GB},
            "uploads/": {"objects": 40, "bytes": 2 * GB},
        },
        "truncated": False,
        "cached": False,
        "error": None,
    }

    text = format_status(payload, is_admin=True)

    assert "🗄 <b>Storage</b>" in text
    assert "<code>my-bucket</code>" in text
    assert "Used: <b>3.0 GB</b> in <b>42</b> object(s)" in text
    # Biggest prefix first - the point is what is eating the space.
    assert text.index("uploads/") < text.index("inputs/")
    assert "2.0 GB in 40 object(s)" in text


def test_format_says_when_the_storage_scan_was_capped_or_cached():
    payload = summarize(jobs=[], queued=[], waiting=0, delayed=0, online_ids=[])
    payload["redis"] = {"connected": True, "ping_ms": 1.0, "error": None}
    payload["storage"] = {
        "backend": "s3",
        "location": "b",
        "objects": 5000,
        "bytes": 10,
        "groups": {},
        "truncated": True,
        "cached": True,
        "error": None,
    }

    text = format_status(payload, is_admin=True)

    assert "(scan capped)" in text
    assert "cached scan" in text


def test_format_explains_a_storage_error_instead_of_hiding_it():
    payload = summarize(jobs=[], queued=[], waiting=0, delayed=0, online_ids=[])
    payload["redis"] = {"connected": True, "ping_ms": 1.0, "error": None}
    payload["storage"] = {"error": "storage unavailable: boom"}

    text = format_status(payload, is_admin=True)

    assert "🗄 <b>Storage</b>" in text
    assert "storage unavailable: boom" in text


def test_personal_view_does_not_leak_storage():
    payload = summarize(jobs=[], queued=[], waiting=0, delayed=0, online_ids=[8], me_id=8)
    payload["redis"] = {"connected": True, "ping_ms": 1.0, "error": None}
    payload["storage"] = {"backend": "s3", "objects": 1, "bytes": 10, "groups": {}, "error": None}

    assert "Storage" not in format_status(payload, is_admin=False)


# ── dashboard buttons ───────────────────────────────────────────────────


def _keyboard_rows(keyboard):
    return [[button.callback_data for button in row] for row in keyboard.inline_keyboard]


def test_status_keyboard_offers_refresh_to_everyone():
    assert _keyboard_rows(status_keyboard(is_admin=False)) == [[refresh_data()]]


def test_status_keyboard_gives_admins_a_separate_restart_row():
    # Its own row, so a mistimed press cannot land on the recycle button.
    rows = _keyboard_rows(status_keyboard(is_admin=True))

    assert rows[0] == [refresh_data()]
    assert rows[1] == [restart_data()]


def test_refresh_keeps_the_live_mode_across_a_press():
    assert parse_status_callback(refresh_data(live=True)) == ("refresh", True)
    assert parse_status_callback(refresh_data()) == ("refresh", False)
    assert parse_status_callback(restart_data()) == ("restart", False)


def test_parse_status_callback_ignores_everything_else():
    for data in (None, 5, "cfm:worker_restart", "st:", "st:wipe", "menu_main"):
        assert parse_status_callback(data) is None


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


def test_collect_reads_slots_and_worker_memory(fake_redis, monkeypatch):
    monkeypatch.setattr(batch_pipeline, "MEMORY_CEILING_BYTES", 1000)
    monkeypatch.setattr(batch_pipeline, "MAX_CONCURRENT_FFMPEG", 1)
    r = fake_redis
    # One of the one global conversion slots is taken...
    r.strings["ffmpeg:slot:0"] = "job-1"
    # ...and a worker has reported its RSS.
    r.strings["ffmpeg:worker:rss:host:1"] = "400"

    payload = asyncio.run(collect_session_status(is_admin=True))

    assert payload["capacity"]["ffmpeg_running"] == 1
    assert payload["capacity"]["peak_rss_bytes"] == 400
    assert payload["capacity"]["headroom_bytes"] == 600
    assert "Capacity" in format_status(payload, is_admin=True)
    # The new blocks are collected alongside it, in the same call.
    assert payload["storage"]["backend"] == "local"
    assert payload["memory"]["pressure"] in ("ok", "high", "critical", "unknown")
    assert "Storage" in format_status(payload, is_admin=True)


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
