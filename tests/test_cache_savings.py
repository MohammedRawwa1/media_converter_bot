"""Cache-savings accounting and the shared key across every producer.

Two things are checked here:

* the hit/miss counters the dashboard renders (they must never raise, and must
  survive Redis being unavailable), and
* that the bot's handlers, the fetcher and the Telethon ingest all derive the
  *same* library key for the same media - one object, not one per route.
"""

import asyncio
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils import media_cache, session_status, storage  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_counters():
    storage._SOURCE_CACHE_LOCAL.clear()
    storage._EGRESS_LOCAL.clear()
    yield
    storage._SOURCE_CACHE_LOCAL.clear()


def _read(rel: str) -> str:
    """Source of a repo file, from the test file's own location."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, rel), encoding="utf-8") as fh:
        return fh.read()


def _no_redis(monkeypatch):
    """Make the Redis path fail, the way an unreachable Redis does."""

    async def _boom(*_args, **_kwargs):
        raise RuntimeError("no redis")

    monkeypatch.setattr("utils.job_queue.get_redis", _boom)


# ─── counters ────────────────────────────────────────────────────────────────


def test_hits_and_misses_accumulate(monkeypatch):
    _no_redis(monkeypatch)
    asyncio.run(storage.record_source_cache(True, nbytes=1000))
    asyncio.run(storage.record_source_cache(True, nbytes=500))
    asyncio.run(storage.record_source_cache(False))

    snap = asyncio.run(storage.source_cache_snapshot())
    assert snap["hits"] == 2
    assert snap["misses"] == 1
    assert snap["bytes_saved"] == 1500
    assert snap["fetches"] == 3
    assert snap["hit_percent"] == pytest.approx(66.67, abs=0.1)


def test_a_miss_never_counts_saved_bytes(monkeypatch):
    _no_redis(monkeypatch)
    asyncio.run(storage.record_source_cache(False, nbytes=9999))
    assert asyncio.run(storage.source_cache_snapshot())["bytes_saved"] == 0


def test_the_counter_never_raises_on_nonsense(monkeypatch):
    _no_redis(monkeypatch)
    asyncio.run(storage.record_source_cache(True, nbytes="a lot"))
    asyncio.run(storage.record_source_cache(True, nbytes=-5))
    snap = asyncio.run(storage.source_cache_snapshot())
    assert snap["hits"] == 2
    assert snap["bytes_saved"] == 0


def test_no_fetches_reports_no_ratio(monkeypatch):
    _no_redis(monkeypatch)
    snap = asyncio.run(storage.source_cache_snapshot())
    assert snap["fetches"] == 0
    assert snap["hit_percent"] is None


def test_periods_are_separate(monkeypatch):
    _no_redis(monkeypatch)
    asyncio.run(storage.record_source_cache(True, nbytes=10, period="2026-08"))
    asyncio.run(storage.record_source_cache(False, period="2026-09"))
    assert asyncio.run(storage.source_cache_snapshot(period="2026-08"))["hits"] == 1
    assert asyncio.run(storage.source_cache_snapshot(period="2026-09"))["misses"] == 1


# ─── dashboard rendering ─────────────────────────────────────────────────────


def _storage_block(**cache):
    return {"backend": "s3", "egress": {"period": "2026-09", "object_bytes": 100}, "source_cache": cache}


def test_dashboard_reports_the_cache_savings():
    lines = session_status._egress_lines(
        _storage_block(period="2026-09", hits=3, misses=1, fetches=4, hit_percent=75.0, bytes_saved=2 * 1024**3)
    )
    text = "\n".join(lines)
    assert "Source cache 2026-09" in text
    assert "3/4" in text
    assert "75%" in text
    assert "2.0 GB" in text
    assert "✅" in text


def test_dashboard_flags_a_cache_that_is_barely_helping():
    text = "\n".join(
        session_status._egress_lines(
            _storage_block(period="2026-09", hits=1, misses=9, fetches=10, hit_percent=10.0, bytes_saved=0)
        )
    )
    assert "⚠️" in text


def test_dashboard_stays_quiet_without_fetches():
    assert session_status._source_cache_lines(_storage_block(period="2026-09", hits=0, fetches=0)) == []
    assert session_status._source_cache_lines({}) == []
    assert session_status._source_cache_lines({"source_cache": "nonsense"}) == []


def test_dashboard_has_no_cache_block_for_the_local_backend():
    block = {"backend": "local", "egress": {"period": "2026-09"}, "source_cache": {"fetches": 4, "hits": 4}}
    assert session_status._egress_lines(block) == []


def test_process_local_only_is_stated():
    text = "\n".join(
        session_status._egress_lines(
            _storage_block(period="2026-09", hits=1, misses=1, fetches=2, hit_percent=50.0, bytes_saved=1, shared=False)
        )
    )
    assert "counted per process" in text


def test_report_attaches_the_cache_snapshot(monkeypatch):
    async def _storage_snapshot(force=False):
        return {"backend": "s3", "bytes": 100}

    async def _egress(stored):
        return {"period": "2026-09", "object_bytes": 1}

    async def _cache():
        return {"period": "2026-09", "hits": 2, "misses": 1, "fetches": 3, "hit_percent": 66.7, "bytes_saved": 5}

    monkeypatch.setattr(session_status, "_storage_snapshot", _storage_snapshot)
    monkeypatch.setattr(session_status, "_storage_egress", _egress)
    monkeypatch.setattr(session_status, "_source_cache_stats", _cache)

    report = asyncio.run(session_status.storage_egress_report())
    assert report["source_cache"]["hits"] == 2
    assert report["egress"]["period"] == "2026-09"


def test_the_stats_helper_swallows_a_failure(monkeypatch):
    """A dashboard must still render when the counters cannot be read."""

    def _boom():
        raise RuntimeError("redis down")

    monkeypatch.setattr(storage, "source_cache_snapshot", _boom)
    assert asyncio.run(session_status._source_cache_stats()) is None


def test_a_missing_cache_reading_does_not_break_the_report(monkeypatch):
    async def _storage_snapshot(force=False):
        return {"backend": "s3", "bytes": 100}

    async def _egress(stored):
        return {"period": "2026-09"}

    async def _none():
        return None

    monkeypatch.setattr(session_status, "_storage_snapshot", _storage_snapshot)
    monkeypatch.setattr(session_status, "_storage_egress", _egress)
    monkeypatch.setattr(session_status, "_source_cache_stats", _none)

    report = asyncio.run(session_status.storage_egress_report())
    assert report["source_cache"] is None
    # The egress block still renders.
    assert session_status._egress_lines(report) != []


# ─── one key, every producer ─────────────────────────────────────────────────


def test_shared_library_key_respects_the_cache_switch(monkeypatch):
    monkeypatch.setenv("MEDIA_CACHE_ENABLED", "1")
    assert media_cache.shared_library_key("abc") == media_cache.media_library_key("abc")
    monkeypatch.setenv("MEDIA_CACHE_ENABLED", "0")
    assert media_cache.shared_library_key("abc") is None


def test_all_producers_use_the_shared_library_key():
    """Same media through the bot, the fetcher or the ingest = one object."""
    for rel in ("handlers.py", "fetcher/service.py", "tools/telethon_ingest.py"):
        src = _read(rel)
        assert "media_library_key" in src or "shared_library_key" in src, rel


def test_fetcher_and_ingest_fall_back_to_a_per_job_key():
    fetcher = _read("fetcher/service.py")
    ingest = _read("tools/telethon_ingest.py")
    # Either the shared key or the old per-job key: the fallback must survive.
    assert re.search(r"key = _shared_library_key\(.*\) or", fetcher)
    assert re.search(r"key = _shared_library_key\(.*\) or", ingest)
    assert "uploads/" in fetcher and "uploads/" in ingest


def test_ingest_hands_the_worker_a_local_path_when_it_keeps_the_file():
    ingest = _read("tools/telethon_ingest.py")
    assert "_local_hint" in ingest
    assert '**_local_hint' in ingest


def test_ingest_derives_an_identity_from_a_message():
    from tools.telethon_ingest import _message_media_identity

    class _Doc:
        unique_id = 4242
        id = 99

    class _Msg:
        document = _Doc()
        video = None
        audio = None
        voice = None
        video_note = None
        sticker = None
        photo = None

    identity = _message_media_identity(_Msg())
    assert identity and "4242" in identity and identity.startswith("tl-document-")
    # No media at all: no identity, so the caller keeps its per-job key.
    assert _message_media_identity(object()) is None


def test_ingest_identity_falls_back_to_the_document_id():
    from tools.telethon_ingest import _message_media_identity

    class _Doc:
        id = 777

    class _Msg:
        document = _Doc()

    assert _message_media_identity(_Msg()) == "tl-document-777"
