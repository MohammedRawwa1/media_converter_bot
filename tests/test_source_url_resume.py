"""The URL fetch resumes an interrupted transfer instead of restarting it.

The web uploader collects a job through a URL (often a large presigned S3 link),
which the worker pulls onto disk before ffmpeg runs. That fetch had no retry and
deleted its `.part` on every failure, so a dropped connection meant the whole
object came down again. It now keeps the prefix, asks for the remainder with a
Range header, and bounds what it keeps through the worker's partial sweep.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_helpers import read_object_source  # noqa: E402

from workers import ffmpeg_worker  # noqa: E402

# A literal public IP, so the SSRF check takes its no-DNS branch and the test
# never depends on name resolution.
URL = "https://93.184.216.34/a.mp4"


class _FakeContent:
    def __init__(self, chunks):
        self._chunks = chunks

    async def iter_chunked(self, _size):
        for chunk in self._chunks:
            yield chunk


class _FakeResp:
    def __init__(self, status, chunks, headers):
        self.status = status
        self.headers = headers
        self.content = _FakeContent(chunks)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.requests = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return self._responses.pop(0)


async def _instant(*_args, **_kwargs):
    return None


def _patch_http(monkeypatch, responses):
    session = _FakeSession(responses)
    monkeypatch.setattr(ffmpeg_worker.aiohttp, "ClientSession", lambda *a, **k: session)
    monkeypatch.setattr(ffmpeg_worker.asyncio, "sleep", _instant)
    return session


def test_a_truncated_fetch_resumes_from_the_bytes_already_on_disk(monkeypatch, tmp_path):
    dest = tmp_path / "src.bin"
    session = _patch_http(
        monkeypatch,
        [
            # First attempt: the body is cut at 100 of the 200 declared bytes.
            _FakeResp(200, [b"x" * 100], {"Content-Length": "200"}),
            # Second attempt: the remainder, as a 206 with the full size.
            _FakeResp(206, [b"y" * 100], {"Content-Range": "bytes 100-199/200", "Content-Length": "100"}),
        ],
    )

    ok = asyncio.run(ffmpeg_worker._fetch_source_url_to_path(URL, str(dest)))

    assert ok is True
    # The prefix was kept and the remainder appended - no duplicated bytes.
    assert dest.read_bytes() == b"x" * 100 + b"y" * 100
    # The first attempt asked for the whole body, the second for the rest.
    assert session.requests[0][1]["headers"] == {}
    assert session.requests[1][1]["headers"] == {"Range": "bytes=100-"}
    # The partial is swapped in atomically, so it is gone on success.
    assert not (tmp_path / "src.bin.part").exists()


def test_a_server_that_ignores_the_range_does_not_duplicate_the_prefix(monkeypatch, tmp_path):
    dest = tmp_path / "src.bin"
    session = _patch_http(
        monkeypatch,
        [
            _FakeResp(200, [b"x" * 100], {"Content-Length": "200"}),
            # A 200 where a 206 was asked for is the whole body: replace, don't append.
            _FakeResp(200, [b"z" * 200], {"Content-Length": "200"}),
        ],
    )

    ok = asyncio.run(ffmpeg_worker._fetch_source_url_to_path(URL, str(dest)))

    assert ok is True
    assert dest.read_bytes() == b"z" * 200
    assert session.requests[1][1]["headers"] == {"Range": "bytes=100-"}


def test_a_partial_at_the_end_of_the_body_starts_over(monkeypatch, tmp_path):
    dest = tmp_path / "src.bin"
    dest.with_name("src.bin.part").write_bytes(b"x" * 200)
    session = _patch_http(
        monkeypatch,
        [
            # A Range from the end is a 416; the stale partial is dropped.
            _FakeResp(416, [], {}),
            _FakeResp(200, [b"z" * 200], {"Content-Length": "200"}),
        ],
    )

    ok = asyncio.run(ffmpeg_worker._fetch_source_url_to_path(URL, str(dest)))

    assert ok is True
    assert dest.read_bytes() == b"z" * 200
    # The retry after the 416 started from zero, with no Range header.
    assert session.requests[0][1]["headers"] == {"Range": "bytes=200-"}
    assert session.requests[1][1]["headers"] == {}


def test_a_transient_server_error_is_retried(monkeypatch, tmp_path):
    dest = tmp_path / "src.bin"
    session = _patch_http(
        monkeypatch,
        [
            _FakeResp(503, [], {}),
            _FakeResp(200, [b"z" * 32], {"Content-Length": "32"}),
        ],
    )

    ok = asyncio.run(ffmpeg_worker._fetch_source_url_to_path(URL, str(dest)))

    assert ok is True
    assert dest.read_bytes() == b"z" * 32
    assert len(session.requests) == 2


def test_a_redirect_is_never_followed_and_never_retried(monkeypatch, tmp_path):
    """A redirect could point at an internal address, so it fails outright."""
    dest = tmp_path / "src.bin"
    session = _patch_http(monkeypatch, [_FakeResp(302, [], {"Location": "http://127.0.0.1/steal"})])

    ok = asyncio.run(ffmpeg_worker._fetch_source_url_to_path(URL, str(dest)))

    assert ok is False
    assert len(session.requests) == 1
    assert not dest.exists()


# ─────────────────────────────────────────────────────────────────────────────
# Reporting into the job hash the batch line reads
# ─────────────────────────────────────────────────────────────────────────────


def test_the_url_fetch_reports_progress_and_the_resume_point(monkeypatch, tmp_path):
    dest = tmp_path / "src.bin"
    writes = []

    async def _fake_set(job_id, status, message, *, progress=None, channel=None):
        writes.append((job_id, status, message, progress, channel))

    monkeypatch.setattr(ffmpeg_worker, "_set_job_state", _fake_set)
    # A frozen clock keeps the throttle in play, so only the samples that are
    # meant to land do.
    monkeypatch.setattr(ffmpeg_worker.time, "time", lambda: 1000.0)
    _patch_http(
        monkeypatch,
        [
            # First attempt: cut at 100 of the 200 declared bytes.
            _FakeResp(200, [b"x" * 100], {"Content-Length": "200"}),
            # Second attempt: the remainder, as a 206 with the full size.
            _FakeResp(206, [b"y" * 100], {"Content-Range": "bytes 100-199/200", "Content-Length": "100"}),
        ],
    )

    ok = asyncio.run(
        ffmpeg_worker._fetch_source_url_to_path(
            URL, str(dest), job_id="job-1", progress_channel="chan-1", note="fetching the source url"
        )
    )

    assert ok is True
    assert dest.read_bytes() == b"x" * 100 + b"y" * 100
    # Live position first, then the point the retry picked up from - the same
    # two facts the storage fetch puts in the batch line.
    assert writes == [
        ("job-1", "processing", "fetching the source url", 50, "chan-1"),
        ("job-1", "processing", "fetching the source url (resuming from 50%)", 0, "chan-1"),
    ]


def test_without_a_job_id_the_fetch_reports_nothing(monkeypatch, tmp_path):
    dest = tmp_path / "src.bin"
    writes = []

    async def _fake_set(job_id, status, message, *, progress=None, channel=None):
        writes.append((job_id, progress))

    monkeypatch.setattr(ffmpeg_worker, "_set_job_state", _fake_set)
    _patch_http(monkeypatch, [_FakeResp(200, [b"z" * 32], {"Content-Length": "32"})])

    ok = asyncio.run(ffmpeg_worker._fetch_source_url_to_path(URL, str(dest)))

    assert ok is True
    assert writes == []


def test_both_call_sites_thread_the_job_context_into_the_fetch():
    src = read_object_source(ffmpeg_worker.handle_job)
    # Job pickup and the mid-run re-download both report onto the same job hash.
    assert src.count("progress_channel=progress_channel") >= 2
    assert src.count('note="fetching the source url"') >= 2
    assert src.count("job_id=job_id") >= 2
