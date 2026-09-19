"""Cancelling a job answers with what actually happened.

A job's terminal hash is kept on purpose - the progress watcher renders the
outcome from it - so a "❌ Cancel" button can outlive the job it belongs to. It
used to be answered by the same blind write whatever the job's state, which
reported a file that had already been delivered as "cancelled and removed from
queue" and rewrote the hash that recorded the delivery into a cancellation the
user never received.

``FakeRedis`` is the one from ``test_queue_admin``: no Redis and no broker are
involved, so this runs wherever the suite runs.
"""

import ast
import asyncio
import json

import pytest
from source_helpers import find_function, flatten, parse_source, read_source
from test_queue_admin import FakeRedis

from utils import job_queue


@pytest.fixture
def fake_redis(monkeypatch):
    client = FakeRedis()

    async def _get_redis():
        return client

    monkeypatch.setattr(job_queue, "get_redis", _get_redis)
    return client


def _hash(client, job_id, **fields):
    client.hashes[f"ffmpeg:job:{job_id}"] = fields


def _body(function: str) -> str:
    return flatten(ast.unparse(find_function(parse_source("handlers.py"), function)))


def test_a_job_that_already_delivered_is_not_cancelled(fake_redis):
    _hash(fake_redis, "j1", status="done", message="delivered to Telegram", delivered="1")

    assert asyncio.run(job_queue.cancel_job("j1")) == "done"

    # The hash is the record of what the job did; nothing rewrites it.
    assert fake_redis.hashes["ffmpeg:job:j1"] == {
        "status": "done",
        "message": "delivered to Telegram",
        "delivered": "1",
    }


def test_a_job_that_failed_is_not_rewritten_into_a_cancellation(fake_redis):
    _hash(fake_redis, "j1", status="error", message="delivery failed")

    assert asyncio.run(job_queue.cancel_job("j1")) == "error"
    assert fake_redis.hashes["ffmpeg:job:j1"]["status"] == "error"


def test_a_queued_job_is_cancelled(fake_redis):
    _hash(fake_redis, "j1", status="queued", input_key="inputs/x/source")

    assert asyncio.run(job_queue.cancel_job("j1")) == "cancelled"

    stored = fake_redis.hashes["ffmpeg:job:j1"]
    assert stored["cancel"] == "1"
    assert stored["status"] == "cancelled"
    # The watcher reads the hash in one go, so the flag that says the user was
    # already told rides in the same write as the status it is about.
    assert stored["cancel_notified"] == "1"


def test_a_job_taken_off_the_queue_list_before_a_worker_pops_it(fake_redis):
    async def _eval(script, numkeys, key, job_id):
        """Stands in for the real Lua scan-and-remove."""
        items = fake_redis.lists.get(key, [])
        kept = [item for item in items if json.loads(item).get("job_id") != job_id]
        fake_redis.lists[key] = kept
        return len(items) - len(kept)

    fake_redis.eval = _eval
    _hash(fake_redis, "j1", status="queued")
    fake_redis.lists[job_queue.JOB_LIST] = [json.dumps({"job_id": "j1"}), json.dumps({"job_id": "j2"})]

    assert asyncio.run(job_queue.cancel_job("j1")) == "cancelled"
    assert [json.loads(raw)["job_id"] for raw in fake_redis.lists[job_queue.JOB_LIST]] == ["j2"]


def test_a_job_this_deployment_has_no_record_of_is_reported_missing(fake_redis):
    assert asyncio.run(job_queue.cancel_job("gone")) == "missing"
    # Nothing is invented: no hash is created for a job that is not there.
    assert "ffmpeg:job:gone" not in fake_redis.hashes


def test_an_already_cancelled_job_says_so_without_rewriting_itself(fake_redis):
    _hash(fake_redis, "j1", status="cancelled", cancel="1")

    assert asyncio.run(job_queue.cancel_job("j1")) == "cancelled"
    assert fake_redis.hashes["ffmpeg:job:j1"] == {"status": "cancelled", "cancel": "1"}


def test_a_hash_that_cannot_be_read_is_cancelled_anyway(monkeypatch, fake_redis):
    """A Redis hiccup on the read must not turn the cancel into a no-op."""

    async def _boom(_key):
        raise RuntimeError("read failed")

    monkeypatch.setattr(fake_redis, "hgetall", _boom)

    assert asyncio.run(job_queue.cancel_job("j1")) == "cancelled"
    assert fake_redis.hashes["ffmpeg:job:j1"]["status"] == "cancelled"


def test_the_cancel_button_reports_the_outcome_it_was_given():
    """The message the button leaves behind has to match what happened.

    Read from the source: what the branch must not do is claim a cancellation
    before it knows there was one.
    """
    body = _body("callback_handler")

    assert 'if _outcome in ("done", "error")' in body
    assert "already finished — nothing to cancel." in body
    assert '_outcome == "missing"' in body
    assert body.index('if _outcome in ("done", "error")') < body.index("cancelled and removed from queue")


def test_the_cancel_flag_is_written_by_the_cancel_itself():
    """The flag rides with the status, so the handler cannot set one without the other."""
    body = read_source("handlers.py")

    assert 'await _r.hset(f"ffmpeg:job:{job_id}", "cancel_notified", "1")' not in body
    assert "from utils.job_queue import cancel_job, get_redis" not in body
    assert "cancel_notified" in read_source("utils", "job_queue.py")
