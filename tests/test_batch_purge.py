"""Taking a batch down: the sweep ``/cancelall`` runs after draining jobs.

Cancelling every job leaves no batch with a live member, so the batch state has
to go too - counters, membership, the progress message's location, the resume
record. Two things matter and are the reason this has its own file:

* a batch that is *genuinely* running must survive the sweep (a member whose job
  hash still looks active, or a batch that has not enqueued its first job yet),
  and
* a batch that is taken down is tombstoned first, so a worker still finishing one
  of its members cannot put the progress bar back.
"""

import asyncio
import json
import time

from utils import batch_pipeline, job_queue

FINISHED = "done"
RUNNING = "processing"


class FakeRedis:
    """Only the commands ``purge_batch``/``purge_stale_batches`` issue."""

    def __init__(self):
        self.strings: dict[str, str] = {}
        self.sets: dict[str, set[str]] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, list[str]] = {}
        self.unreadable_sets: set[str] = set()

    async def get(self, key):
        return self.strings.get(key)

    async def hset(self, key, mapping=None, **kwargs):
        fields = {**dict(mapping or {}), **kwargs}
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in fields.items()})
        return len(fields)

    async def eval(self, script, numkeys, *args):
        """Stands in for the prune script: drops this batch's queue members."""
        key, batch_id = str(args[0]), str(args[1])
        items = list(self.lists.get(key, []))
        kept = [item for item in items if str(json.loads(item).get("batch_id")) != batch_id]
        self.lists[key] = kept
        return len(items) - len(kept)

    async def zrange(self, key, start, stop):
        values = self.zsets.get(key, [])
        return values[start:] if stop == -1 else values[start : stop + 1]

    async def zrem(self, key, *values):
        members = self.zsets.get(key, [])
        before = len(members)
        self.zsets[key] = [item for item in members if item not in set(values)]
        return before - len(self.zsets[key])

    async def set(self, key, value, nx=False, px=None, ex=None):
        self.strings[key] = str(value)
        return True

    async def delete(self, *keys):
        removed = 0
        for key in keys:
            for store in (self.strings, self.sets, self.hashes):
                if key in store:
                    del store[key]
                    removed += 1
        return removed

    async def sadd(self, key, *values):
        self.sets.setdefault(key, set()).update(str(value) for value in values)
        return len(values)

    async def srem(self, key, *values):
        members = self.sets.get(key, set())
        before = len(members)
        members -= {str(value) for value in values}
        return before - len(members)

    async def smembers(self, key):
        if key in self.unreadable_sets:
            raise RuntimeError("membership unavailable")
        return set(self.sets.get(key, set()))

    async def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    async def exists(self, *keys):
        return sum(
            1
            for key in keys
            if key in self.strings or key in self.sets or key in self.hashes or key in self.lists
        )

    def scan_iter(self, match="*", count=None):
        prefix = match[:-1] if match and match.endswith("*") else match
        keys = sorted(set(self.strings) | set(self.sets) | set(self.hashes))
        matched = [key for key in keys if key.startswith(prefix)]

        async def _gen():
            for key in matched:
                yield key

        return _gen()

    async def close(self):
        return None


def _use(monkeypatch, client=None, *, error=None):
    async def _get_redis():
        if error is not None:
            raise error
        return client

    monkeypatch.setattr(job_queue, "get_redis", _get_redis)


def _seed_batch(
    r,
    batch_id,
    *,
    members=(),
    statuses=None,
    total=3,
    done=1,
    msg="1:2",
    started=None,
    owner=42,
):
    if members:
        r.sets[batch_pipeline.batch_jobs_key(batch_id)] = {str(member) for member in members}
    r.strings[batch_pipeline.batch_total_key(batch_id)] = str(total)
    r.strings[batch_pipeline.batch_progress_key(batch_id)] = str(done)
    r.strings[batch_pipeline.batch_message_key(batch_id)] = msg
    r.sets[batch_pipeline.batch_finished_keys(batch_id)] = {"entry-1"}
    r.sets.setdefault(batch_pipeline.ACTIVE_BATCHES_KEY, set()).add(batch_id)
    if started is not None:
        r.strings[batch_pipeline.batch_started_key(batch_id)] = str(started)
    if owner is not None:
        r.sets.setdefault(f"ffmpeg:batch:resume:{owner}", set()).add(batch_id)
    for job_id, status in (statuses or {}).items():
        r.hashes[f"ffmpeg:job:{job_id}"] = {"status": status}


# ── one batch ───────────────────────────────────────────────────────────


def test_purge_batch_drops_its_state_and_returns_the_message(monkeypatch):
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=["j1"], statuses={"j1": FINISHED}, msg="9:88")
    _use(monkeypatch, r)

    purged = asyncio.run(batch_pipeline.purge_batch(r, batch_id="batch-a"))

    assert purged["message"] == (9, 88)
    assert purged["keys"] >= 5
    for key in batch_pipeline.batch_state_keys("batch-a"):
        assert key not in r.strings
        assert key not in r.sets
    assert "batch-a" not in r.sets[batch_pipeline.ACTIVE_BATCHES_KEY]


def test_purge_batch_tombstones_before_forgetting_anything(monkeypatch):
    # The tombstone is what stops a worker that is still finishing a member from
    # reposting the bar, so it has to outlive the cleanup.
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=["j1"], statuses={"j1": FINISHED})
    _use(monkeypatch, r)

    asyncio.run(batch_pipeline.purge_batch(r, batch_id="batch-a", reason="cancelled by admin"))

    # The value carries when it was written, which is what later lets a sweep tell
    # an old tombstone from one a worker might still be depending on.
    stored = r.strings[batch_pipeline.batch_cancel_key("batch-a")]
    assert stored.startswith("cancelled by admin|")
    assert batch_pipeline._tombstone_written_at(stored) > 0


def test_purge_batch_without_an_id_is_a_no_op(monkeypatch):
    r = FakeRedis()
    _use(monkeypatch, r)

    assert asyncio.run(batch_pipeline.purge_batch(r, batch_id=None))["keys"] == 0


def test_purge_batch_can_skip_the_marker_nothing_needs(monkeypatch):
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=["j1"], statuses={"j1": FINISHED})
    _use(monkeypatch, r)

    purged = asyncio.run(batch_pipeline.purge_batch(r, batch_id="batch-a", fence=False))

    assert purged["keys"] > 0
    assert r.strings.get(batch_pipeline.batch_cancel_key("batch-a")) is None


# ── the sweep ───────────────────────────────────────────────────────────


def test_sweep_clears_a_batch_whose_jobs_are_all_finished(monkeypatch):
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=["j1", "j2"], statuses={"j1": FINISHED, "j2": "cancelled"})
    _use(monkeypatch, r)

    summary = asyncio.run(batch_pipeline.purge_stale_batches(r))

    assert summary["batches"] == ["batch-a"]
    assert summary["messages"] == [(1, 2)]
    assert summary["keys"] > 0
    # The resume record must not keep pointing at a batch that no longer exists.
    assert r.sets.get("ffmpeg:batch:resume:42") == set()


def test_sweep_leaves_no_marker_for_a_batch_that_counted_everything(monkeypatch):
    """The reported bug: ``/cancelall`` left a ``:cancelled`` key for a batch that
    was already over, and ``scripts/cleanup_stale_redis.py`` kept finding it.

    Every member finished and was counted, so no worker (and no apply feeding the
    batch) is left to stop: a marker here has nothing behind it, and the batch it
    names no longer exists - so no later run would ever clear it.
    """
    r = FakeRedis()
    _seed_batch(
        r,
        "batch-a",
        members=["j1", "j2"],
        statuses={"j1": FINISHED, "j2": FINISHED},
        total=2,
        done=2,
    )
    _use(monkeypatch, r)

    summary = asyncio.run(batch_pipeline.purge_stale_batches(r))

    assert summary["batches"] == ["batch-a"]
    assert not [key for key in r.strings if key.startswith("ffmpeg:batch:")]
    assert r.strings.get(batch_pipeline.batch_cancel_key("batch-a")) is None


def test_sweep_fences_a_batch_it_cancelled_a_member_of(monkeypatch):
    # Covers the run that stops a member itself: its hash is written terminal, so
    # only the member list the caller hands in still shows it could be running -
    # and a worker that is still winding that member down must find the marker.
    r = FakeRedis()
    _seed_batch(
        r,
        "batch-a",
        members=["j1", "j2"],
        statuses={"j1": "cancelled", "j2": FINISHED},
        total=2,
        done=2,
    )
    _use(monkeypatch, r)

    summary = asyncio.run(batch_pipeline.purge_stale_batches(r, cancelled_job_ids=["j1"]))

    assert summary["batches"] == ["batch-a"]
    assert r.strings.get(batch_pipeline.batch_cancel_key("batch-a"))


def test_sweep_fences_a_batch_that_has_not_counted_everything(monkeypatch):
    # ``:done`` short of ``:total`` is the apply still feeding the batch: it asks
    # about the marker before it queues the next file, so the marker has to be up.
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=["j1"], statuses={"j1": FINISHED}, total=3, done=1)
    _use(monkeypatch, r)

    asyncio.run(batch_pipeline.purge_stale_batches(r))

    assert r.strings.get(batch_pipeline.batch_cancel_key("batch-a"))


def test_sweep_keeps_a_batch_with_a_running_member(monkeypatch):
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=["j1", "j2"], statuses={"j1": FINISHED, "j2": RUNNING})
    _use(monkeypatch, r)

    summary = asyncio.run(batch_pipeline.purge_stale_batches(r))

    assert summary["batches"] == []
    assert summary["kept"] == 1
    assert batch_pipeline.batch_total_key("batch-a") in r.strings
    assert r.strings.get(batch_pipeline.batch_cancel_key("batch-a")) is None


def test_sweep_keeps_a_batch_that_has_not_queued_its_first_job_yet(monkeypatch):
    # An apply spends minutes fetching its first file, so a memberless batch is
    # not stale while it is still young.
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=[], started=time.time())
    _use(monkeypatch, r)

    assert asyncio.run(batch_pipeline.purge_stale_batches(r))["batches"] == []


def test_sweep_clears_an_old_memberless_batch(monkeypatch):
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=[], started=time.time() - batch_pipeline.BATCH_PURGE_GRACE_SECONDS - 60)
    _use(monkeypatch, r)

    assert asyncio.run(batch_pipeline.purge_stale_batches(r))["batches"] == ["batch-a"]


def test_sweep_clears_a_memberless_batch_with_no_timestamp(monkeypatch):
    # A batch from before the timestamp existed: nothing to age, and its members
    # would have been what protected it.
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=[])
    _use(monkeypatch, r)

    assert asyncio.run(batch_pipeline.purge_stale_batches(r))["batches"] == ["batch-a"]


def test_sweep_honours_jobs_the_caller_already_cancelled(monkeypatch):
    # Covers a cancel whose flag write failed: the caller's word beats the hash.
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=["j1"], statuses={"j1": RUNNING})
    _use(monkeypatch, r)

    summary = asyncio.run(batch_pipeline.purge_stale_batches(r, cancelled_job_ids=["j1"]))

    assert summary["batches"] == ["batch-a"]


def test_sweep_keeps_a_batch_whose_membership_cannot_be_read(monkeypatch):
    # Never tear down something that might be running because Redis hiccuped.
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=["j1"], statuses={"j1": FINISHED})
    r.unreadable_sets.add(batch_pipeline.batch_jobs_key("batch-a"))
    _use(monkeypatch, r)

    assert asyncio.run(batch_pipeline.purge_stale_batches(r))["batches"] == []
    assert batch_pipeline.batch_total_key("batch-a") in r.strings


def test_sweep_does_not_clear_the_same_dead_batch_twice(monkeypatch):
    """The tombstone left behind by a sweep must stop the batch coming back.

    Purging rewrites the tombstone with a fresh TTL, so re-sweeping a batch that
    a previous sweep had already taken down kept it alive for another 30 days:
    the same long-dead batches came back as "Batches cleared: N" on every later
    ``/cancelall``, while ``scripts/cleanup_stale_redis.py`` - which deletes the
    tombstone - cleared them for good.
    """
    r = FakeRedis()
    # Everything an earlier sweep removed is gone; only its tombstone is left.
    r.strings[batch_pipeline.batch_cancel_key("batch-a")] = "cancelled by admin"
    _use(monkeypatch, r)

    summary = asyncio.run(batch_pipeline.purge_stale_batches(r))

    assert summary["batches"] == []
    # Untouched, so its TTL keeps running down instead of being pushed out again.
    assert r.strings[batch_pipeline.batch_cancel_key("batch-a")] == "cancelled by admin"


def test_sweep_still_clears_a_stopped_batch_that_has_state_left(monkeypatch):
    # /cancelbatch tombstones a batch while its counters and bar are still there,
    # so a tombstone alone must not exempt a batch that still owns state.
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=["j1"], statuses={"j1": FINISHED})
    r.strings[batch_pipeline.batch_cancel_key("batch-a")] = "7"
    _use(monkeypatch, r)

    summary = asyncio.run(batch_pipeline.purge_stale_batches(r))

    assert summary["batches"] == ["batch-a"]
    assert batch_pipeline.batch_total_key("batch-a") not in r.strings


def test_sweep_still_clears_a_batch_listed_in_the_aggregate_view(monkeypatch):
    # Its counters expired but the active set still lists it, and that
    # membership is itself state to remove.
    r = FakeRedis()
    r.sets[batch_pipeline.ACTIVE_BATCHES_KEY] = {"batch-a"}
    r.strings[batch_pipeline.batch_cancel_key("batch-a")] = "cancelled by admin"
    _use(monkeypatch, r)

    summary = asyncio.run(batch_pipeline.purge_stale_batches(r))

    assert summary["batches"] == ["batch-a"]
    assert r.sets[batch_pipeline.ACTIVE_BATCHES_KEY] == set()


def test_sweep_never_treats_its_own_bookkeeping_as_a_batch(monkeypatch):
    # ``ffmpeg:batch:active`` and ``ffmpeg:batch:resume:<user>`` live under the
    # same prefix but are not batches.
    r = FakeRedis()
    r.sets[batch_pipeline.ACTIVE_BATCHES_KEY] = set()
    r.sets["ffmpeg:batch:resume:42"] = set()
    r.strings["ffmpeg:batch:resume:42:started"] = str(time.time())
    _use(monkeypatch, r)

    summary = asyncio.run(batch_pipeline.purge_stale_batches(r))

    assert summary["batches"] == []
    assert "ffmpeg:batch:resume:42" in r.sets


def test_sweep_survives_an_unreachable_redis(monkeypatch):
    _use(monkeypatch, error=RuntimeError("no redis"))

    summary = asyncio.run(batch_pipeline.purge_stale_batches())

    assert summary == {"batches": [], "keys": 0, "messages": [], "kept": 0, "tombstones": 0}


# ── ghosted old batches ─────────────────────────────────────────────────


def test_the_tombstone_value_carries_when_it_was_written():
    now = time.time()
    value = batch_pipeline.batch_tombstone_value("cancelled by admin", written_at=now)
    assert value.startswith("cancelled by admin|")
    # Millisecond precision is plenty for a grace window measured in hours.
    assert abs(batch_pipeline._tombstone_written_at(value) - now) < 0.01
    # A pre-timestamp tombstone has no age to reason about.
    assert batch_pipeline._tombstone_written_at("cancelled by admin") is None
    # A label may contain anything, so only the last segment is the stamp.
    assert batch_pipeline._tombstone_written_at(batch_pipeline.batch_tombstone_value("a|b", 5)) == 5


def test_an_old_tombstone_is_removed(monkeypatch):
    r = FakeRedis()
    stale = time.time() - batch_pipeline.BATCH_TOMBSTONE_GRACE_SECONDS - 60
    r.strings[batch_pipeline.batch_cancel_key("batch-a")] = batch_pipeline.batch_tombstone_value(
        "cancelled by admin", written_at=stale
    )
    _use(monkeypatch, r)

    summary = asyncio.run(batch_pipeline.purge_stale_batches(r))

    assert summary["batches"] == []
    assert summary["tombstones"] == 1
    assert batch_pipeline.batch_cancel_key("batch-a") not in r.strings


def test_a_fresh_tombstone_is_kept(monkeypatch):
    # A member of this batch may still be reporting - it has not finished counting
    # - and the tombstone is the only thing stopping it reposting the bar.
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=["j1"], statuses={"j1": FINISHED})
    _use(monkeypatch, r)

    summary = asyncio.run(batch_pipeline.purge_stale_batches(r))

    assert summary["batches"] == ["batch-a"]
    assert summary["tombstones"] == 0
    assert batch_pipeline.batch_cancel_key("batch-a") in r.strings


def test_a_stop_by_hand_keeps_the_marker_it_wrote(monkeypatch):
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=["j1"], statuses={"j1": "cancelled"}, total=3, done=1)
    r.strings[batch_pipeline.batch_cancel_key("batch-a")] = batch_pipeline.batch_tombstone_value(
        "cancelled by admin"
    )
    _use(monkeypatch, r)

    asyncio.run(batch_pipeline.purge_stale_batches(r))

    assert batch_pipeline.batch_cancel_key("batch-a") in r.strings


def test_a_stop_by_hand_marks_only_a_batch_that_still_has_work(monkeypatch):
    """The button path decides the same way ``/cancelall`` does.

    Stopping a batch that has counted every member - a stale bar, or a Stop pressed
    after the last file landed - has nobody left to stop, so it takes the state
    down without leaving a marker behind.
    """
    finished = FakeRedis()
    _seed_batch(
        finished,
        "batch-a",
        members=["j1"],
        statuses={"j1": FINISHED},
        total=1,
        done=1,
    )
    running = FakeRedis()
    _seed_batch(
        running, "batch-a", members=["j1"], statuses={"j1": RUNNING}, total=3, done=1
    )
    _use(monkeypatch, finished)

    asyncio.run(batch_pipeline.cancel_batch(finished, batch_id="batch-a", requested_by="user"))
    asyncio.run(batch_pipeline.cancel_batch(running, batch_id="batch-a", requested_by="user"))

    assert finished.strings.get(batch_pipeline.batch_cancel_key("batch-a")) is None
    assert running.strings.get(batch_pipeline.batch_cancel_key("batch-a"))


def test_a_tombstone_for_a_batch_with_a_live_member_is_kept(monkeypatch):
    r = FakeRedis()
    _seed_batch(r, "batch-a", members=["j1"], statuses={"j1": RUNNING})
    stale = time.time() - batch_pipeline.BATCH_TOMBSTONE_GRACE_SECONDS - 60
    r.strings[batch_pipeline.batch_cancel_key("batch-a")] = batch_pipeline.batch_tombstone_value(
        "cancelled by admin", written_at=stale
    )
    _use(monkeypatch, r)

    summary = asyncio.run(batch_pipeline.purge_stale_batches(r))

    assert summary["tombstones"] == 0
    assert batch_pipeline.batch_cancel_key("batch-a") in r.strings


def test_a_tombstone_from_before_the_timestamp_is_left_alone(monkeypatch):
    # Its age cannot be established, and guessing it risks the repost this exists
    # to prevent - so it is left to the offline script, which only runs where
    # nothing is live.
    r = FakeRedis()
    r.strings[batch_pipeline.batch_cancel_key("batch-a")] = "cancelled by admin"
    _use(monkeypatch, r)

    assert asyncio.run(batch_pipeline.purge_stale_tombstones(r)) == 0
    assert batch_pipeline.batch_cancel_key("batch-a") in r.strings
