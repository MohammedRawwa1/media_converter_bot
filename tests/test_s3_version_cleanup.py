"""The sweep has to free bytes, not just write tombstones.

A versioned bucket - which is every S3 bucket unless somebody turned versioning
off - answers ``delete_keys`` with a *delete marker*: the key disappears from the
``list_keys`` listing while the object and every earlier copy of it stay, still
counting against the plan. The marker then becomes that key's newest version, so
the next sweep sees a fresh object and leaves the whole history alone. A bucket
holding seven objects and twenty-three invisible versions filled an account and
started refusing every write, while every sweep logged ``deleted=1/1``.

What is pinned here is the half that returns the space: the version listing the
backend has to expose, the request shape that addresses a version rather than a
name, and the TTL rules the sweeper applies to superseded copies and markers.
"""

import asyncio
import time
from datetime import UTC, datetime, timedelta

import pytest

from tasks import cleanup_tasks
from utils import storage


@pytest.fixture(autouse=True)
def _no_noncurrent_override(monkeypatch):
    """Each test starts from "no override": only the one that sets it sees one."""
    monkeypatch.delenv("S3_NONCURRENT_TTL_SECONDS", raising=False)


# ── the backend's version listing ───────────────────────────────────────


def _version(key, *, version_id="v1", latest=False, size=0, age=0):
    return {
        "Key": key,
        "VersionId": version_id,
        "IsLatest": latest,
        "Size": size,
        "LastModified": datetime.now(UTC) - timedelta(seconds=age),
    }


def _marker(key, *, version_id="m1", latest=True, age=0):
    return {
        "Key": key,
        "VersionId": version_id,
        "IsLatest": latest,
        "LastModified": datetime.now(UTC) - timedelta(seconds=age),
    }


class _VersionListingClient:
    """A boto3-shaped client that answers the version listing and records deletes."""

    def __init__(self, versions=(), markers=()):
        self._payload = {"Versions": list(versions), "DeleteMarkers": list(markers), "IsTruncated": False}
        self.deleted: list[dict] = []

    def get_paginator(self, name):
        assert name == "list_object_versions", "only the version listing is asked for here"
        payload = self._payload

        class _Paginator:
            def paginate(self, **_kwargs):
                return iter([payload])

        return _Paginator()

    def delete_objects(self, Bucket, Delete):  # noqa: N803 - mirrors boto3
        self.deleted.append(Delete)
        return {"Deleted": [{"Key": entry["Key"]} for entry in Delete["Objects"]]}


def _s3_backend(monkeypatch, client):
    fake_boto3 = type("FakeBoto3", (), {"client": staticmethod(lambda *a, **k: client)})
    monkeypatch.setattr(storage, "boto3", fake_boto3)
    backend = storage.S3AsyncBackend(bucket="a-bucket", endpoint_url="https://example.invalid")
    backend._use_aioboto3 = False
    backend._boto_config = None
    return backend


def test_the_s3_backend_advertises_version_support(monkeypatch):
    """The sweeper gates on the flag, so a backend that keeps versions says so."""
    assert storage.S3AsyncBackend(bucket="b").supports_object_versions is True
    assert storage.LocalStorageBackend().supports_object_versions is False


def test_a_backend_without_versions_refuses_the_listing_rather_than_lying():
    """A local filesystem has no versions: an empty answer would read as one."""
    backend = storage.LocalStorageBackend()

    try:
        asyncio.run(backend.list_versions("inputs/"))
    except NotImplementedError:
        pass
    else:  # pragma: no cover - the backend must stay honest
        raise AssertionError("a backend without versions must not answer with an empty listing")


def test_the_listing_reports_superseded_copies_and_delete_markers(monkeypatch):
    client = _VersionListingClient(
        versions=[
            _version("inputs/library/abc/source", version_id="v1", age=90000, size=400),
            _version("inputs/library/abc/source", version_id="v2", latest=True, size=400),
        ],
        markers=[_marker("outputs/job-1/out.mp4", version_id="m1")],
    )
    backend = _s3_backend(monkeypatch, client)

    rows = asyncio.run(backend.list_versions("inputs/"))

    by_id = {row["version_id"]: row for row in rows}
    assert set(by_id) == {"v1", "v2", "m1"}
    assert by_id["v1"]["is_latest"] is False
    assert by_id["v2"]["is_latest"] is True
    assert by_id["v1"]["size"] == 400
    # A marker is reported, not dropped: a sweep that cannot see one can never
    # take it down.
    assert by_id["m1"]["is_delete_marker"] is True
    assert by_id["m1"]["size"] == 0
    assert isinstance(by_id["v1"]["last_modified"], float)


def test_deleting_a_version_addresses_the_version_not_the_name(monkeypatch):
    """Without ``VersionId`` S3 writes another marker and keeps the bytes."""
    client = _VersionListingClient()
    backend = _s3_backend(monkeypatch, client)

    removed = asyncio.run(backend.delete_versions([("inputs/a/source", "v1"), ("outputs/b/out.mp4", "m1")]))

    assert removed == 2
    assert client.deleted == [
        {
            "Objects": [
                {"Key": "inputs/a/source", "VersionId": "v1"},
                {"Key": "outputs/b/out.mp4", "VersionId": "m1"},
            ]
        }
    ]


def test_deleting_no_versions_asks_the_bucket_nothing(monkeypatch):
    client = _VersionListingClient()
    backend = _s3_backend(monkeypatch, client)

    assert asyncio.run(backend.delete_versions([])) == 0
    assert client.deleted == []


# ── the sweeper's use of it ─────────────────────────────────────────────


class _VersionedBackend:
    """A versioned backend: the listing it needs, plus the two deletes it makes."""

    supports_object_versions = True

    def __init__(self, objects=(), versions=()):
        self._objects = list(objects)
        self._versions = list(versions)
        self.deleted: list[str] = []
        self.deleted_versions: list[tuple[str, str]] = []
        self.version_listings = 0

    async def list_keys(self, prefix):
        return [row for row in self._objects if row["key"].startswith(prefix)]

    async def delete_keys(self, keys):
        self.deleted.extend(keys)
        return len(keys)

    async def list_versions(self, prefix):
        self.version_listings += 1
        return [row for row in self._versions if row["key"].startswith(prefix)]

    async def delete_versions(self, versions):
        self.deleted_versions.extend(versions)
        return len(versions)


class _PlainBackend:
    """What every test double in this suite looks like: no versions at all."""

    def __init__(self, objects=()):
        self._objects = list(objects)
        self.deleted: list[str] = []

    async def list_keys(self, prefix):
        return [row for row in self._objects if row["key"].startswith(prefix)]

    async def delete_keys(self, keys):
        self.deleted.extend(keys)
        return len(keys)


def _manager(monkeypatch, backend, *, backend_name="s3"):
    async def _get_backend():
        return backend

    monkeypatch.setattr("utils.storage.get_storage_backend", _get_backend)
    monkeypatch.setattr(cleanup_tasks.config, "get_storage_backend_name", lambda: backend_name)
    return cleanup_tasks.CleanupManager()


def _row(key, *, version_id, latest=False, marker=False, age=0, size=10):
    return {
        "key": key,
        "version_id": version_id,
        "is_latest": latest,
        "is_delete_marker": marker,
        "last_modified": time.time() - age,
        "size": size,
    }


def test_a_superseded_version_is_taken_down_once_it_is_past_the_ttl(monkeypatch):
    backend = _VersionedBackend(
        versions=[
            _row("outputs/job-1/out.mp4", version_id="v1", age=200000, size=900),
            _row("outputs/job-1/out.mp4", version_id="v2", latest=True, age=10, size=900),
            _row("outputs/job-2/out.mp4", version_id="v9", age=10, size=900),
        ]
    )
    manager = _manager(monkeypatch, backend)
    manager.s3_output_ttl = 24 * 3600

    assert asyncio.run(manager.cleanup_s3_outputs()) == 1

    # The older copy of the same key goes; the live one and a recent superseded
    # one stay (the TTL is the window they are kept for).
    assert backend.deleted_versions == [("outputs/job-1/out.mp4", "v1")]


def test_the_versions_of_an_expired_key_all_go_with_it(monkeypatch):
    """The plain delete only hid the key: its bytes and its tombstone remain."""
    now = time.time()
    backend = _VersionedBackend(
        objects=[{"key": "inputs/job-1/source.mp4", "last_modified": now - 200000, "size": 10}],
        versions=[
            _row("inputs/job-1/source.mp4", version_id="v1", age=200000),
            _row("inputs/job-1/source.mp4", version_id="v2", latest=True, age=200000),
            # The marker the delete above just wrote: young, but the key it hides
            # is exactly the one this sweep expired.
            _row("inputs/job-1/source.mp4", version_id="m3", latest=True, marker=True, age=0),
            _row("inputs/job-2/source.mp4", version_id="v1", latest=True, age=0),
        ],
    )
    manager = _manager(monkeypatch, backend)
    manager.s3_input_ttl = 24 * 3600

    deleted = asyncio.run(manager.cleanup_s3_inputs())

    assert backend.deleted == ["inputs/job-1/source.mp4"]
    assert backend.deleted_versions == [
        ("inputs/job-1/source.mp4", "v1"),
        ("inputs/job-1/source.mp4", "v2"),
        ("inputs/job-1/source.mp4", "m3"),
    ]
    # One key plus three versions cleared.
    assert deleted == 4


def test_a_marker_older_than_the_ttl_is_taken_down(monkeypatch):
    """A tombstone with nothing left to hide is pure residue."""
    backend = _VersionedBackend(
        versions=[
            _row("outputs/gone/out.mp4", version_id="m1", marker=True, age=200000),
            _row("outputs/fresh/out.mp4", version_id="m2", marker=True, age=10),
        ]
    )
    manager = _manager(monkeypatch, backend)
    manager.s3_output_ttl = 24 * 3600

    asyncio.run(manager.cleanup_s3_outputs())

    assert backend.deleted_versions == [("outputs/gone/out.mp4", "m1")]


def test_the_residue_is_swept_even_when_nothing_has_expired(monkeypatch):
    """Old versions outlive the key's own TTL, which is how a bucket fills up."""
    backend = _VersionedBackend(
        objects=[{"key": "outputs/job-3/out.mp4", "last_modified": time.time(), "size": 10}],
        versions=[
            _row("outputs/job-3/out.mp4", version_id="v1", age=200000),
            _row("outputs/job-3/out.mp4", version_id="v2", latest=True, age=5),
        ],
    )
    manager = _manager(monkeypatch, backend)
    manager.s3_output_ttl = 24 * 3600

    deleted = asyncio.run(manager.cleanup_s3_outputs())

    assert backend.deleted == [], "nothing was old enough to expire this time"
    assert backend.deleted_versions == [("outputs/job-3/out.mp4", "v1")]
    assert deleted == 1


def test_the_noncurrent_ttl_can_be_zeroed(monkeypatch):
    """``0`` is a real answer: a superseded version goes at the next sweep."""
    monkeypatch.setenv("S3_NONCURRENT_TTL_SECONDS", "0")
    backend = _VersionedBackend(
        versions=[
            _row("outputs/job-4/out.mp4", version_id="v1", age=1),
            _row("outputs/job-4/out.mp4", version_id="v2", latest=True, age=0),
        ]
    )
    manager = _manager(monkeypatch, backend)

    asyncio.run(manager.cleanup_s3_outputs())

    assert backend.deleted_versions == [("outputs/job-4/out.mp4", "v1")]


def test_a_garbage_noncurrent_ttl_falls_back_to_the_prefix_ttl(monkeypatch):
    monkeypatch.setenv("S3_NONCURRENT_TTL_SECONDS", "soon")

    assert cleanup_tasks.CleanupManager().s3_noncurrent_ttl is None


def test_the_library_cache_is_still_exempt(monkeypatch):
    """``inputs/`` must not sweep the nested prefix that has its own TTL."""
    backend = _VersionedBackend(
        versions=[
            _row("inputs/library/abc/source", version_id="v1", age=900000),
            _row("inputs/job-9/source.mp4", version_id="v1", age=900000),
        ]
    )
    manager = _manager(monkeypatch, backend)
    manager.s3_input_ttl = 24 * 3600

    asyncio.run(manager.cleanup_s3_inputs())

    assert backend.deleted_versions == [("inputs/job-9/source.mp4", "v1")]


def test_a_backend_without_versions_is_left_exactly_as_it_was(monkeypatch):
    """The plain sweep is what a local backend (and every test double) gets."""
    backend = _PlainBackend(objects=[{"key": "outputs/job-1/out.mp4", "last_modified": 0, "size": 10}])
    manager = _manager(monkeypatch, backend)

    assert asyncio.run(manager.cleanup_s3_outputs()) == 1
    assert backend.deleted == ["outputs/job-1/out.mp4"]


def test_a_version_listing_that_cannot_answer_does_not_break_the_sweep(monkeypatch):
    class _Opaque(_PlainBackend):
        supports_object_versions = True

        async def list_versions(self, prefix):
            raise NotImplementedError("no version listing here")

    backend = _Opaque(objects=[{"key": "outputs/job-1/out.mp4", "last_modified": 0, "size": 10}])
    manager = _manager(monkeypatch, backend)

    assert asyncio.run(manager.cleanup_s3_outputs()) == 1


def test_the_purge_failure_does_not_take_the_whole_sweep_down(monkeypatch):
    class _Broken(_VersionedBackend):
        async def delete_versions(self, versions):
            raise RuntimeError("the bucket said no")

    backend = _Broken(versions=[_row("outputs/job-1/out.mp4", version_id="v1", age=900000)])
    manager = _manager(monkeypatch, backend)

    assert asyncio.run(manager.cleanup_s3_outputs()) == 0
