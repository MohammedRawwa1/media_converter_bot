"""The web download endpoint resumes a dropped transfer.

A stored output used to leave the endpoint as one whole-body response (or as a
redirect that only works when presigning does), so an interrupted download
started again from zero. The endpoint now streams the window the client asked
for and answers 206 with ``Content-Range``, which is what lets a large download
resume.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_helpers import read_source  # noqa: E402

from web import webapp  # noqa: E402

PAYLOAD = bytes(range(256)) * 4  # 1024 bytes


class _BridgeBackend:
    """An async ranged reader, the shape the storage backend exposes."""

    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    async def head(self, key):
        return len(self.payload)

    async def get_file_size(self, key):
        return len(self.payload)

    async def iter_range(self, key, *, start=0, end=None, chunk_size=0):
        self.calls.append((key, start, end))
        window = self.payload[start : (end + 1) if end is not None else None]
        for index in range(0, len(window), 8):
            yield window[index : index + 8]


# ─────────────────────────────────────────────────────────────────────────────
# Parsing the Range header
# ─────────────────────────────────────────────────────────────────────────────


def test_no_range_means_the_whole_body():
    assert webapp._resolve_http_range(None, 1000) is None
    assert webapp._resolve_http_range("", 1000) is None
    # Without a known size there is nothing to clamp a range to.
    assert webapp._resolve_http_range("bytes=0-99", None) is None
    assert webapp._resolve_http_range("bytes=0-99", 0) is None


def test_a_closed_and_an_open_range_re_served():
    assert webapp._resolve_http_range("bytes=0-99", 1000) == (0, 99)
    assert webapp._resolve_http_range("bytes=500-", 1000) == (500, 999)
    # An end past the object is clamped rather than refused.
    assert webapp._resolve_http_range("bytes=900-5000", 1000) == (900, 999)


def test_a_suffix_range_is_the_final_bytes():
    assert webapp._resolve_http_range("bytes=-100", 1000) == (900, 999)
    assert webapp._resolve_http_range("bytes=-5000", 1000) == (0, 999)


def test_a_range_that_cannot_be_served_is_unsatisfiable():
    assert webapp._resolve_http_range("bytes=1000-", 1000) == "unsatisfiable"
    assert webapp._resolve_http_range("bytes=100-50", 1000) == "unsatisfiable"
    assert webapp._resolve_http_range("bytes=-0", 1000) == "unsatisfiable"


def test_a_multi_range_or_malformed_value_falls_back_to_the_whole_body():
    assert webapp._resolve_http_range("bytes=0-1,5-6", 1000) is None
    assert webapp._resolve_http_range("bytes=abc", 1000) is None
    assert webapp._resolve_http_range("items=0-5", 1000) is None


# ─────────────────────────────────────────────────────────────────────────────
# The async -> sync bridge
# ─────────────────────────────────────────────────────────────────────────────


def test_the_bridge_turns_an_async_stream_into_sync_chunks():
    backend = _BridgeBackend(PAYLOAD)

    body = b"".join(webapp._storage_range_chunks(backend, "outputs/x.mp4", 5, 24))

    assert body == PAYLOAD[5:25]
    assert backend.calls == [("outputs/x.mp4", 5, 24)]


def test_the_bridge_can_stream_to_the_end():
    backend = _BridgeBackend(PAYLOAD)

    body = b"".join(webapp._storage_range_chunks(backend, "outputs/x.mp4", 1000, None))

    assert body == PAYLOAD[1000:]
    assert backend.calls == [("outputs/x.mp4", 1000, None)]


# ─────────────────────────────────────────────────────────────────────────────
# The response the endpoint builds
# ─────────────────────────────────────────────────────────────────────────────


def _serve(monkeypatch, backend, *, headers=None, filename="x.mp4"):
    monkeypatch.setattr(webapp, "_run_async", lambda coro: asyncio.run(coro))
    with webapp.app.test_request_context(headers=headers or {}):
        return webapp._serve_storage_object(backend, "outputs/x.mp4", filename=filename)


def test_a_range_request_gets_a_206_and_the_window(monkeypatch):
    backend = _BridgeBackend(PAYLOAD)

    response = _serve(monkeypatch, backend, headers={"Range": "bytes=100-199"})

    assert response.status_code == 206
    assert response.headers["Content-Range"] == f"bytes 100-199/{len(PAYLOAD)}"
    assert response.headers["Content-Length"] == "100"
    assert response.headers["Accept-Ranges"] == "bytes"
    assert response.get_data() == PAYLOAD[100:200]


def test_a_plain_request_gets_the_whole_object(monkeypatch):
    backend = _BridgeBackend(PAYLOAD)

    response = _serve(monkeypatch, backend)

    assert response.status_code == 200
    assert "Content-Range" not in response.headers
    assert response.headers["Content-Length"] == str(len(PAYLOAD))
    assert response.headers["Accept-Ranges"] == "bytes"
    assert response.get_data() == PAYLOAD


def test_an_unsatisfiable_range_gets_a_416(monkeypatch):
    backend = _BridgeBackend(PAYLOAD)

    response = _serve(monkeypatch, backend, headers={"Range": f"bytes={len(PAYLOAD)}-"})

    assert response.status_code == 416
    assert response.headers["Content-Range"] == f"bytes */{len(PAYLOAD)}"
    # Nothing is streamed for a range that has no bytes in it.
    assert backend.calls == []


def test_the_endpoint_streams_a_stored_output_with_range():
    src = read_source("web", "webapp.py")
    # The stored object is streamed through the app (Range honoured) before any
    # presigned redirect, and the local path asks for Range explicitly.
    assert "supports_range_streaming" in src
    assert "_serve_storage_object(" in src
    assert "conditional=True" in src
