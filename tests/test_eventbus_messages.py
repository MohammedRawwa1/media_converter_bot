"""Event envelope, bucketing and the payload projection that keeps secrets out.

The projection test is the important one: jobs carry signed URLs, session strings
and credentials, and an event log outlives a job, so what goes into Kafka has to
be a whitelisted summary rather than the job dict.
"""

import json

import pytest

from utils.eventbus import messages


def test_envelope_has_a_versioned_shape():
    event = messages.new_event(
        messages.JOB_QUEUED,
        job_id="j1",
        request_id="r1",
        source="enqueue",
        payload={"queue": "rabbitmq"},
        event_id="e1",
        ts=123.5,
    )
    assert set(event) == {"event_id", "type", "version", "ts", "job_id", "request_id", "source", "payload"}
    assert event["version"] == messages.EVENT_VERSION
    assert event["type"] == "job.queued"
    assert event["ts"] == 123.5


def test_roundtrip_through_the_wire_format():
    event = messages.new_event(messages.JOB_COMPLETED, job_id="j1", payload={"status": "done"}, event_id="e2", ts=7.0)
    assert messages.decode(messages.encode(event)) == event


def test_decode_rejects_anything_unusable():
    with pytest.raises(messages.EventDecodeError):
        messages.decode(None)
    with pytest.raises(messages.EventDecodeError):
        messages.decode(b"{not json")
    with pytest.raises(messages.EventDecodeError):
        messages.decode(json.dumps({"type": "job.queued"}))  # no id/version/ts
    with pytest.raises(messages.EventDecodeError):
        messages.decode(json.dumps({"event_id": "", "type": "job.queued", "version": 1, "ts": 1.0}))
    with pytest.raises(messages.EventDecodeError):
        messages.decode(json.dumps({"event_id": "e", "type": "x", "version": 1, "ts": "soon", "payload": {}}))
    with pytest.raises(messages.EventDecodeError):
        messages.decode(json.dumps({"event_id": "e", "type": "x", "version": 1, "ts": 1.0, "payload": []}))


def test_terminal_events_are_the_ones_that_end_a_job():
    assert messages.is_terminal(messages.JOB_COMPLETED)
    assert messages.is_terminal(messages.JOB_FAILED)
    assert messages.is_terminal(messages.JOB_CANCELLED)
    assert not messages.is_terminal(messages.JOB_PROGRESS)
    assert not messages.is_terminal(messages.JOB_QUEUED)


def test_bucket_is_stable_bounded_and_spread():
    assert messages.job_bucket("job-42") == messages.job_bucket("job-42")
    buckets = [messages.job_bucket(f"job-{i}") for i in range(200)]
    assert all(0 <= bucket < 100 for bucket in buckets)
    # A hash that collapsed to a few buckets would make the rollout percentage a
    # lie, so require the sample to touch a wide part of the range.
    assert len(set(buckets)) > 50


def test_job_summary_whitelists_fields():
    job = {
        "job_id": "j1",
        "request_id": "r1",
        "type": "ffmpeg",
        "chat_id": 555,
        "user_id": 777,
        "output_ext": ".mp4",
        "size_bytes": 1024,
        "attempt": 2,
        "input_path": "/data/storage/input/movie.mkv",
        "output_path": "/data/storage/output/movie.mp4",
    }
    summary = messages.job_summary(job)
    assert summary["job_id"] == "j1"
    assert summary["chat_id"] == 555
    assert summary["size_bytes"] == 1024
    # Local paths are not part of an event log.
    assert "input_path" not in summary and "output_path" not in summary


def test_job_summary_never_carries_credentials():
    job = {
        "job_id": "j1",
        "chat_id": 555,
        "source_url": "https://cdn.example.com/a.mp4?token=leaked-token",
        "aws_secret_access_key": "super-secret-key",  # nosec B105  # fixture value; this test asserts it is never summarised
        "pyrogram_session": "session-string-value",
        "extra": {
            "note": "kept",
            "s3_presign": "https://bucket.s3/obj?X-Amz-Signature=leaked-signature",
            "api_key": "leaked-api-key",
            "upload_token": "leaked-upload-token",  # nosec B105  # fixture value; this test asserts it is never summarised
        },
    }
    blob = json.dumps(messages.job_summary(job))
    for leaked in (
        "leaked-token",
        "super-secret-key",
        "session-string-value",
        "leaked-signature",
        "leaked-api-key",
        "leaked-upload-token",
    ):
        assert leaked not in blob
    assert json.loads(blob)["extra.note"] == "kept"


def test_job_summary_tolerates_missing_fields_and_non_dicts():
    assert messages.job_summary(None) == {}
    assert messages.job_summary("not a job") == {}
    assert messages.job_summary({"job_id": "j1", "extra": "not a dict"}) == {"job_id": "j1"}
