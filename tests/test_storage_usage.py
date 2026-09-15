"""The storage usage scan behind the dashboard's **Storage** block.

Two things matter here: the totals are right, and the scan is *bounded*. A
bucket or a storage tree can grow without limit, so every case below checks that
the walk stops at ``max_objects`` and admits it did rather than quietly
reporting a partial number as the whole.

The S3 path is driven through a fake client so the suite never reaches the
network, and it is exercised in the ``boto3`` fallback mode on purpose: that
branch runs in a worker thread, which is the one that is easy to get wrong.
"""

import asyncio

from utils import storage


class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        return iter(self._pages)


class _FakeS3Client:
    def __init__(self, pages):
        self._pages = pages

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator(self._pages)


def _s3_backend(monkeypatch, pages):
    """An S3 backend whose client returns *pages* instead of talking to a bucket."""
    fake_boto3 = type(
        "FakeBoto3",
        (),
        {"client": staticmethod(lambda *args, **kwargs: _FakeS3Client(pages))},
    )
    monkeypatch.setattr(storage, "boto3", fake_boto3)

    backend = storage.S3AsyncBackend(bucket="a-bucket", endpoint_url="https://example.invalid")
    backend._use_aioboto3 = False
    return backend


def _page(*objects):
    return {"Contents": [{"Key": key, "Size": size} for key, size in objects]}


# ── S3 ──────────────────────────────────────────────────────────────────


def test_s3_usage_counts_and_groups_objects(monkeypatch):
    backend = _s3_backend(
        monkeypatch,
        [
            _page(
                ("uploads/a.mp4", 100),
                ("inputs/c.mkv", 25),
                ("uploads/b.mp4", 50),
            )
        ],
    )

    usage = asyncio.run(backend.usage())

    assert usage["backend"] == "s3"
    assert usage["location"] == "a-bucket"
    assert usage["objects"] == 3
    assert usage["bytes"] == 175
    assert usage["groups"]["uploads/"] == {"objects": 2, "bytes": 150}
    assert usage["groups"]["inputs/"] == {"objects": 1, "bytes": 25}
    assert usage["truncated"] is False


def test_s3_usage_stops_at_the_cap(monkeypatch):
    backend = _s3_backend(
        monkeypatch,
        [
            _page(*[(f"uploads/{index}.mp4", 1) for index in range(10)]),
            _page(("never/counted.mp4", 1)),
        ],
    )

    usage = asyncio.run(backend.usage(max_objects=4))

    # The cap stops the walk: 4 objects counted, and it says so.
    assert usage["objects"] == 4
    assert usage["bytes"] == 4
    assert usage["truncated"] is True


def test_s3_usage_treats_a_missing_size_as_zero(monkeypatch):
    backend = _s3_backend(monkeypatch, [{"Contents": [{"Key": "uploads/a.mp4"}]}])

    usage = asyncio.run(backend.usage())

    assert usage["objects"] == 1
    assert usage["bytes"] == 0


# ── local ───────────────────────────────────────────────────────────────


def test_local_usage_walks_the_tree(tmp_path):
    (tmp_path / "uploads").mkdir()
    (tmp_path / "uploads" / "a.mp4").write_bytes(b"x" * 100)
    (tmp_path / "uploads" / "b.mp4").write_bytes(b"x" * 50)
    (tmp_path / "inputs").mkdir()
    (tmp_path / "inputs" / "c.mkv").write_bytes(b"x" * 25)
    (tmp_path / "stray.bin").write_bytes(b"x" * 5)

    usage = asyncio.run(storage.LocalStorageBackend(base_path=str(tmp_path)).usage())

    assert usage["backend"] == "local"
    assert usage["objects"] == 4
    assert usage["bytes"] == 180
    assert usage["groups"]["uploads/"] == {"objects": 2, "bytes": 150}
    assert usage["groups"]["inputs/"] == {"objects": 1, "bytes": 25}
    # A file at the root still gets attributed rather than dropped from the groups.
    assert usage["groups"]["(root)"] == {"objects": 1, "bytes": 5}
    assert usage["truncated"] is False


def test_local_usage_stops_at_the_cap(tmp_path):
    for index in range(10):
        (tmp_path / f"{index}.bin").write_bytes(b"x")

    usage = asyncio.run(storage.LocalStorageBackend(base_path=str(tmp_path)).usage(max_objects=3))

    assert usage["objects"] == 3
    assert usage["truncated"] is True


def test_local_usage_of_a_missing_root_is_empty_not_an_error(tmp_path):
    usage = asyncio.run(storage.LocalStorageBackend(base_path=str(tmp_path / "nope")).usage())

    assert usage["objects"] == 0
    assert usage["bytes"] == 0
    assert usage["groups"] == {}


def test_local_usage_group_depth_can_be_widened(tmp_path):
    (tmp_path / "a" / "b").mkdir(parents=True)
    (tmp_path / "a" / "b" / "c.bin").write_bytes(b"x")

    usage = asyncio.run(storage.LocalStorageBackend(base_path=str(tmp_path)).usage(group_depth=2))

    assert usage["groups"]["a/b/"] == {"objects": 1, "bytes": 1}
