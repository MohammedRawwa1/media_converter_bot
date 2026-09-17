"""Tests for the parallel upload paths in :mod:`utils.userbot_uploader`.

A delivery used to read the whole output into RAM and then keep a second full
copy of every part in a list, so a large result was killed mid-upload: no error
in the log, the job's slot left held by a "gone" process, and the next boot had
to redo the job from the start. These tests pin the replacement - parts are read
one at a time straight off disk, and only the parts actually in flight are ever
resident.
"""

import asyncio
import hashlib

import pytest

from utils import userbot_uploader as mod

CHUNK = 1024


def _md5(data: bytes) -> str:
    # Telegram's protocol field for small-file uploads, not a security hash.
    return hashlib.md5(data).hexdigest()  # noqa: S324


def _make_file(path: str, size: int) -> bytes:
    payload = bytes((i * 7 + 3) % 251 for i in range(size))
    with open(path, "wb") as f:
        f.write(payload)
    return payload


class _TelethonRecorder:
    """Minimal Telethon client: records the parts and the peak in-flight count."""

    def __init__(self) -> None:
        self.parts: dict[int, bytes] = {}
        self.file_total_parts: int | None = None
        self.request_types: list[str] = []
        self.in_flight = 0
        self.peak_in_flight = 0

    async def __call__(self, request):
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            # Yield so the parts really do overlap, like a live upload.
            await asyncio.sleep(0)
            self.parts[request.file_part] = bytes(request.bytes)
            # SaveFilePartRequest (small files) carries no part count.
            self.file_total_parts = getattr(request, "file_total_parts", self.file_total_parts)
            self.request_types.append(type(request).__name__)
        finally:
            self.in_flight -= 1


class _PyrogramRecorder:
    """Minimal Pyrogram client: the parallel path invokes raw requests on it."""

    def __init__(self) -> None:
        self.parts: dict[int, bytes] = {}
        self.file_total_parts: int | None = None
        self.in_flight = 0
        self.peak_in_flight = 0

    async def invoke(self, request):
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            await asyncio.sleep(0)
            self.parts[request.file_part] = bytes(request.bytes)
            self.file_total_parts = getattr(request, "file_total_parts", self.file_total_parts)
        finally:
            self.in_flight -= 1


def test_part_count_rounds_up():
    assert mod._part_count(0, 100) == 1
    assert mod._part_count(100, 100) == 1
    assert mod._part_count(101, 100) == 2


def test_file_parts_cover_the_file_exactly_once():
    size = 10 * 100 + 37
    parts = mod._file_parts(size, 100)

    assert [index for index, _, _ in parts] == list(range(len(parts)))
    assert [offset for _, offset, _ in parts] == [index * 100 for index in range(len(parts))]
    assert sum(length for _, _, length in parts) == size
    assert parts[-1][2] == 37


def test_md5_hex_matches_hashlib(tmp_path):
    path = tmp_path / "sum.bin"
    payload = _make_file(str(path), 3 * CHUNK + 7)

    assert mod._md5_hex(str(path)) == _md5(payload)


def test_read_file_part_returns_exactly_the_requested_slice(tmp_path):
    path = tmp_path / "part.bin"
    payload = _make_file(str(path), 3 * CHUNK + 11)

    assert mod._read_file_part(str(path), 0, CHUNK) == payload[:CHUNK]
    assert mod._read_file_part(str(path), CHUNK, CHUNK) == payload[CHUNK : 2 * CHUNK]
    assert mod._read_file_part(str(path), 3 * CHUNK, 11) == payload[3 * CHUNK :]


@pytest.mark.asyncio
async def test_telethon_parallel_upload_reassembles_the_file(tmp_path):
    path = tmp_path / "out.bin"
    payload = _make_file(str(path), 5 * CHUNK + 137)
    client = _TelethonRecorder()

    uploaded = await mod._parallel_upload_file(client, str(path), len(payload), part_size=CHUNK, workers=3)

    assembled = b"".join(client.parts[index] for index in range(len(client.parts)))
    assert assembled == payload
    assert client.request_types == ["SaveFilePartRequest"] * 6
    assert uploaded.name == "out.bin"
    # SaveFilePart uploads need the checksum; omitting it used to raise.
    assert uploaded.md5_checksum == _md5(payload)


@pytest.mark.asyncio
async def test_big_files_carry_the_part_count(tmp_path):
    # part_size=1 pushes total_parts past 1024, which is what selects the
    # SaveBigFilePart request (and is the only one that carries the count).
    path = tmp_path / "big.bin"
    payload = _make_file(str(path), 2048)
    client = _TelethonRecorder()

    uploaded = await mod._parallel_upload_file(client, str(path), len(payload), part_size=1, workers=8)

    assert client.file_total_parts == 2048
    assert set(client.request_types) == {"SaveBigFilePartRequest"}
    assert len(client.parts) == 2048
    assert uploaded.parts == 2048


@pytest.mark.asyncio
async def test_pyrogram_parallel_upload_reassembles_the_file(tmp_path):
    path = tmp_path / "out.bin"
    payload = _make_file(str(path), 3 * CHUNK + 1)
    client = _PyrogramRecorder()

    uploaded = await mod._parallel_upload_file_pyrogram(client, str(path), len(payload), part_size=CHUNK, workers=3)

    assembled = b"".join(client.parts[index] for index in range(len(client.parts)))
    assert assembled == payload
    assert len(client.parts) == 4
    assert uploaded.name == "out.bin"
    assert uploaded.md5_checksum == _md5(payload)


@pytest.mark.asyncio
async def test_only_the_parts_in_flight_are_resident(tmp_path):
    path = tmp_path / "many.bin"
    payload = _make_file(str(path), 40 * CHUNK)
    client = _TelethonRecorder()

    await mod._parallel_upload_file(client, str(path), len(payload), part_size=CHUNK, workers=4)

    assert client.peak_in_flight <= 4
    assert len(client.parts) == 40


@pytest.mark.asyncio
async def test_every_part_is_read_on_its_own(tmp_path, monkeypatch):
    path = tmp_path / "stream.bin"
    payload = _make_file(str(path), 9 * CHUNK + 5)
    lengths: list[int] = []
    real_read = mod._read_file_part

    def spy(file_path, offset, length):
        lengths.append(length)
        return real_read(file_path, offset, length)

    monkeypatch.setattr(mod, "_read_file_part", spy)
    client = _TelethonRecorder()

    await mod._parallel_upload_file(client, str(path), len(payload), part_size=CHUNK, workers=2)

    assert lengths and max(lengths) <= CHUNK
    assert sum(lengths) == len(payload)
    assert len(lengths) == 10


@pytest.mark.asyncio
async def test_above_the_threshold_the_file_is_not_read_at_all(tmp_path, monkeypatch):
    path = tmp_path / "huge.bin"
    path.write_bytes(b"")
    reads: list[int] = []

    def spy(file_path, offset, length):
        reads.append(length)
        return b""

    monkeypatch.setattr(mod, "_read_file_part", spy)
    client = _TelethonRecorder()
    huge = mod._PARALLEL_MAX_MEMORY_BYTES + 1

    assert await mod._parallel_upload_file(client, str(path), huge, part_size=CHUNK, workers=2) is None
    assert await mod._parallel_upload_file_pyrogram(client, str(path), huge, part_size=CHUNK, workers=2) is None
    assert reads == []
    assert client.parts == {}
