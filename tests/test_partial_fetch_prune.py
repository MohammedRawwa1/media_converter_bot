"""Bounding the disk the deliberately-kept fetch partials may hold.

A ``.part`` file is kept so an interrupted storage fetch can resume instead of
pulling the whole object again. Nothing else cleaned them, so a worker that kept
failing mid-fetch would leave a prefix per media until the disk filled. These
tests cover the prune helper and the worker's sweeper that runs it.
"""

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_helpers import read_source  # noqa: E402

from utils import file_utils  # noqa: E402


def _partial(root, name, *, size=64, age=0.0):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    if age:
        stamp = time.time() - age
        os.utime(path, (stamp, stamp))
    return path


def _prune(root, *, ttl=3600, max_files=8, max_bytes=4096):
    return file_utils.prune_partial_files(root, ttl_seconds=ttl, max_files=max_files, max_bytes=max_bytes)


def test_a_partial_past_the_ttl_is_dropped(tmp_path):
    root = tmp_path / "temp"
    old = _partial(root, "library/hash1/source.mp4.part", size=64, age=7200)

    removed, freed = _prune(root, ttl=3600)

    assert removed == 1
    assert freed == 64
    assert not old.exists()


def test_a_partial_inside_the_ttl_is_kept_for_resuming(tmp_path):
    root = tmp_path / "temp"
    fresh = _partial(root, "library/hash1/source.mp4.part", age=60)

    removed, freed = _prune(root, ttl=3600)

    assert (removed, freed) == (0, 0)
    assert fresh.exists()


def test_the_count_cap_drops_the_oldest_first(tmp_path):
    root = tmp_path / "temp"
    for index in range(5):
        path = _partial(root, f"s{index}.part", size=10)
        os.utime(path, (1000 + index, 1000 + index))

    removed, _ = _prune(root, ttl=10**12, max_files=2, max_bytes=10**12)

    assert removed == 3
    assert sorted(p.name for p in root.iterdir()) == ["s3.part", "s4.part"]


def test_the_byte_cap_drops_the_oldest_first(tmp_path):
    root = tmp_path / "temp"
    for index in range(3):
        path = _partial(root, f"s{index}.part", size=100)
        os.utime(path, (1000 + index, 1000 + index))

    removed, freed = _prune(root, ttl=10**12, max_files=100, max_bytes=250)

    # 300 bytes is over the 250-byte budget, so one 100-byte prefix goes.
    assert (removed, freed) == (1, 100)
    assert sorted(p.name for p in root.iterdir()) == ["s1.part", "s2.part"]


def test_only_partials_are_ever_touched(tmp_path):
    root = tmp_path / "temp"
    media = root / "library" / "hash1" / "source.mp4"
    media.parent.mkdir(parents=True)
    media.write_bytes(b"x" * 32)
    probe = _partial(root, "library/hash1/source.mp4.probe", size=8, age=10**6)
    os.utime(media, (0, 0))

    removed, _ = _prune(root, ttl=1, max_files=0, max_bytes=0)

    assert removed == 0
    # The finished media and the range-probe slice are not fetch partials.
    assert media.exists()
    assert probe.exists()


def test_a_missing_root_is_not_an_error(tmp_path):
    assert file_utils.prune_partial_files(str(tmp_path / "nope"), ttl_seconds=1, max_files=0, max_bytes=0) == (0, 0)
    assert file_utils.prune_partial_files("", ttl_seconds=1, max_files=0, max_bytes=0) == (0, 0)


def test_the_worker_sweeps_the_partials_it_keeps():
    src = read_source("workers", "ffmpeg_worker.py")
    assert "async def _partial_fetch_sweeper" in src
    assert "file_utils.prune_partial_files," in src
    assert "ttl_seconds=_PARTIAL_FETCH_TTL_SECONDS" in src
    assert "max_files=_PARTIAL_FETCH_MAX_FILES" in src
    assert "max_bytes=_PARTIAL_FETCH_MAX_BYTES" in src
    # Started at boot, so the first fetch already looks at a pruned directory.
    assert "asyncio.create_task(_partial_fetch_sweeper(stop_event))" in src
    assert "partial_sweep_task.cancel()" in src
