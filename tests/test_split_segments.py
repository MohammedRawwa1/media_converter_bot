"""Splitting a media into parts: one command, one naming, one order.

The splitter is asked for a *timing* ("each part is an hour") and answers with
numbered files - ``Concert.001.mp4``, ``Concert.002.mp4`` - for a video and for an
audio file alike, because the only thing that differs between the two is the part
extension. These tests pin the ffmpeg command that does it, the parsing of what
the user typed, the metadata the parts keep, and the delivery that follows: the
parts are sent one after another, each named after the media it came from.
"""

import ast
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_helpers import find_function, flatten, parse_source, read_source  # noqa: E402

import handlers  # noqa: E402
from handlers import (  # noqa: E402
    _audio_tag_kwargs,
    _document_delivery_name,
    _metadata_caption,
    _parse_split_request,
    _safe_media_stem,
)
from tasks import conversion_tasks  # noqa: E402
from utils.media_time import (  # noqa: E402
    format_clock,
    parse_time_to_seconds,
    segment_seconds_for_parts,
)

SOURCE = b"payload"


def _run(coro):
    import asyncio

    return asyncio.run(coro)


# ─────────────────────────────────────────────────────────────────────────────
# What the user types
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "text,seconds",
    [
        ("90", 90.0),
        ("10:00", 600.0),
        ("01:00:00", 3600.0),
        ("1:02:03", 3723.0),
        ("30m", 1800.0),
        ("2h", 7200.0),
        ("1h30m", 5400.0),
        ("45s", 45.0),
        ("1.5h", 5400.0),
        (" 10:00 ", 600.0),
    ],
)
def test_a_part_length_is_read_in_every_form_a_user_types(text, seconds):
    assert parse_time_to_seconds(text) == pytest.approx(seconds)


@pytest.mark.parametrize("text", ["", "later", "10:00:00:00", "1h30", "abc", "10m nope"])
def test_a_nonsense_time_is_refused(text):
    with pytest.raises(ValueError):
        parse_time_to_seconds(text)


def test_equal_parts_are_a_part_length():
    assert segment_seconds_for_parts(3600, 4) == pytest.approx(900)
    # Never zero: ffmpeg reads a zero segment time as "cut wherever".
    assert segment_seconds_for_parts(3, 10) == pytest.approx(1.0)
    with pytest.raises(ValueError):
        segment_seconds_for_parts(0, 4)


def test_a_clock_is_formatted_for_the_reply():
    assert format_clock(3723) == "01:02:03"
    assert format_clock(0) == "00:00:00"


@pytest.mark.parametrize(
    "text,expected",
    [
        ("01:00:00", ("length", 3600.0, None)),
        ("30m", ("length", 1800.0, None)),
        ("4", ("parts", 4.0, None)),
        ("00:10-00:20", ("range", 10.0, 20.0)),
        ("10-30", ("range", 10.0, 30.0)),
    ],
)
def test_the_splitter_reads_a_length_a_count_or_one_range(text, expected):
    assert _parse_split_request(text) == expected


@pytest.mark.parametrize("text", ["", "1", "0", "20-10", "nope"])
def test_a_split_request_that_makes_no_sense_is_refused(text):
    with pytest.raises(ValueError):
        _parse_split_request(text)


# ─────────────────────────────────────────────────────────────────────────────
# The command
# ─────────────────────────────────────────────────────────────────────────────


class _FakeProcess:
    def __init__(self, returncode=0, stderr=b""):
        self.returncode = returncode
        self.stderr = stderr

    async def communicate(self):
        return b"", self.stderr


def _patch_spawn(monkeypatch, *, returncode=0, stderr=b"", parts=3, size=16):
    """Record the argv and write the part files ffmpeg would have written."""
    seen = {}

    async def _spawn(*cmd, **kwargs):
        seen["cmd"] = list(cmd)
        pattern = cmd[-1]
        if returncode == 0:
            for index in range(1, parts + 1):
                path = pattern.replace("%03d", f"{index:03d}")
                with open(path, "wb") as fh:
                    fh.write(b"x" * size)
        return _FakeProcess(returncode, stderr)

    monkeypatch.setattr(conversion_tasks, "_spawn_process", _spawn)
    return seen


def _source_file(tmp_path, name="Concert.mp4", payload=SOURCE):
    path = tmp_path / name
    path.write_bytes(payload)
    return str(path)


def test_the_split_is_one_stream_copy_with_the_users_timing(tmp_path, monkeypatch):
    src = _source_file(tmp_path)
    out_dir = tmp_path / "out"
    seen = _patch_spawn(monkeypatch)

    ok, parts, error = _run(conversion_tasks.split_media_segments(src, str(out_dir), 3600, ext=".mp4", stem="Concert"))

    assert ok and error == ""
    assert seen["cmd"] == [
        conversion_tasks.FFMPEG_PATH,
        "-y",
        "-i",
        src,
        "-c",
        "copy",
        "-map",
        "0",
        "-segment_time",
        "3600",
        "-f",
        "segment",
        "-segment_start_number",
        "1",
        "-reset_timestamps",
        "1",
        os.path.join(str(out_dir), "Concert.%03d.mp4"),
    ]
    assert [os.path.basename(p) for p in parts] == ["Concert.001.mp4", "Concert.002.mp4", "Concert.003.mp4"]


def test_the_parts_are_numbered_from_one_not_from_ffmpegs_zero(tmp_path, monkeypatch):
    """The segment muxer numbers from 000 unless it is told otherwise.

    ``Concert.000.mp4`` is what a real run produced, next to a "part 1/N" caption:
    the numbering the code, its replies and the users' expectations all describe as
    starting at 001, so the flag that makes ffmpeg agree has to reach the command.
    """
    src = _source_file(tmp_path)
    seen = _patch_spawn(monkeypatch)

    _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 60, stem="Concert"))

    cmd = seen["cmd"]
    assert cmd[cmd.index("-segment_start_number") + 1] == "1"


def test_a_fractional_length_reaches_ffmpeg_as_a_number(tmp_path, monkeypatch):
    src = _source_file(tmp_path)
    seen = _patch_spawn(monkeypatch)

    _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 90.5, stem="C"))

    assert seen["cmd"][seen["cmd"].index("-segment_time") + 1] == "90.5"


def test_the_same_split_serves_audio_with_its_own_extension(tmp_path, monkeypatch):
    """A recording is cut exactly like a video; only the parts differ."""
    src = _source_file(tmp_path, "Album.mp3")
    seen = _patch_spawn(monkeypatch)

    ok, parts, _error = _run(
        conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 600, ext=".mp3", stem="Album")
    )

    assert ok
    assert seen["cmd"][-1].endswith("Album.%03d.mp3")
    assert [os.path.basename(p) for p in parts] == ["Album.001.mp3", "Album.002.mp3", "Album.003.mp3"]


def test_only_the_parts_of_this_run_are_returned(tmp_path, monkeypatch):
    """A stray file in the output directory is not a part."""
    src = _source_file(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "leftover.001.mp4").write_bytes(b"old")
    _patch_spawn(monkeypatch, parts=2)

    ok, parts, _error = _run(conversion_tasks.split_media_segments(src, str(out_dir), 60, stem="Concert"))

    assert ok
    assert [os.path.basename(p) for p in parts] == ["Concert.001.mp4", "Concert.002.mp4"]


def test_an_empty_part_is_not_a_part(tmp_path, monkeypatch):
    src = _source_file(tmp_path)
    _patch_spawn(monkeypatch, parts=2, size=0)

    ok, parts, error = _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 60, stem="Concert"))

    assert ok is False
    assert parts == []
    assert "no parts" in error


@pytest.mark.parametrize("seconds", [0, -5, "soon"])
def test_a_useless_segment_length_is_refused(tmp_path, seconds):
    src = _source_file(tmp_path)
    ok, parts, error = _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), seconds, stem="C"))
    assert ok is False and parts == [] and "segment" in error


def test_a_missing_source_is_refused(tmp_path):
    ok, parts, error = _run(conversion_tasks.split_media_segments(str(tmp_path / "gone.mp4"), str(tmp_path), 60))
    assert ok is False and parts == []
    assert "not found" in error


def test_ffmpeg_failing_reports_its_own_error(tmp_path, monkeypatch):
    src = _source_file(tmp_path)
    _patch_spawn(monkeypatch, returncode=1, stderr=b"Invalid data found when processing input")

    ok, parts, error = _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 60, stem="C"))

    assert ok is False
    assert parts == []
    assert "Invalid data found" in error


def test_the_stem_cannot_escape_the_output_directory(tmp_path, monkeypatch):
    """The part name comes from the media's name, which is attacker-influenced."""
    src = _source_file(tmp_path)
    seen = _patch_spawn(monkeypatch)

    _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 60, stem="../../etc/passwd"))

    assert seen["cmd"][-1] == os.path.join(str(tmp_path / "out"), "passwd.%03d.mp4")


# ─────────────────────────────────────────────────────────────────────────────
# The names and the metadata the parts keep
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Concert.mp4", "Concert"),
        ("My Movie.2024.mkv", "My Movie.2024"),
        ("../../etc/passwd.mp4", "passwd"),
        ('we:ird*na?me"<>.mp4', "weirdname"),
        ("   .mp4", "media"),
        (None, "media"),
    ],
)
def test_the_media_name_becomes_a_safe_file_name(name, expected):
    assert _safe_media_stem(name) == expected


def test_a_document_is_named_after_the_media_not_after_its_path():
    current = {"name": "My Movie.2024.mkv"}
    assert _document_delivery_name(current, "/storage/output/12345_optimized.mp4") == "My Movie.2024.mp4"
    # No name to go on: the path is still better than nothing.
    assert _document_delivery_name({}, "/storage/output/x.bin") == "x.bin"
    assert _document_delivery_name({}, None) == "media.mp4"


def test_the_audio_tags_the_source_carries_are_kept():
    current = {"name": "track.mp3", "_source_metadata": {"title": "Real Title", "artist": "Real Artist"}}

    tags = _audio_tag_kwargs(current, "track.mp3")

    assert tags == {"title": "Real Title", "performer": "Real Artist"}


def test_an_untagged_audio_falls_back_to_the_delivered_name():
    """The old behaviour for a file that genuinely carries no tags."""
    assert _audio_tag_kwargs({"name": "track.mp3"}, "track.mp3") == {"title": "track"}


def test_the_caption_and_the_audio_tags_read_one_source():
    current = {"name": "a.mp3", "_source_metadata": {"title": "T", "performer": "P"}}
    assert _metadata_caption(current) == "T — P"
    assert _audio_tag_kwargs(current, "a.mp3") == {"title": "T", "performer": "P"}
    # One extraction, not one per delivery site.
    assert read_source("handlers.py").count('_first("performer"') == 1


# ─────────────────────────────────────────────────────────────────────────────
# The handler: video and audio, delivered in order
# ─────────────────────────────────────────────────────────────────────────────


def test_the_split_button_accepts_audio_as_well_as_video():
    src = read_source("handlers.py")
    # The button used to refuse anything but a video; the split itself never
    # needed one, so the guard is what decides.
    assert 'not in ("video", "audio")' in src
    assert "No video or audio file found to split." in src


def test_the_split_runs_the_shared_task_and_delivers_each_part(tmp_path, monkeypatch):
    """The handler hands the timing to the task and sends what it produced."""
    tree = parse_source("handlers.py")
    handler = find_function(tree, "_handle_split_request")
    body = flatten(ast.unparse(handler))

    assert "split_media_segments" in body
    assert "trim_media" in body
    assert "_deliver_split_parts" in body
    # The parts are named after the media, in a directory of this run's own.
    assert '_safe_media_stem(current_file.get("name"))' in body
    assert "stem=stem" in body

    deliver = find_function(tree, "_deliver_split_parts")
    deliver_body = flatten(ast.unparse(deliver))
    # One at a time, in order, and each under the part's own name.
    assert "for index, part in enumerate(parts, 1)" in deliver_body
    assert "delivery_name=part_name" in deliver_body
    assert "filename=part_name" in deliver_body
    # The tags the media carries travel to the parts, numbered by part.
    assert "_source_media_tags(current_file)" in deliver_body


def test_a_split_part_is_never_named_after_an_output_path():
    """The part names come from the splitter, and the fallback still uses the media name."""
    part = _run(_check_part_name())
    assert part == "Concert.001.mp4"


async def _check_part_name():
    return handlers._split_part_name({"name": "Concert.mp4"}, "/storage/output/split_ab/Concert.001.mp4", 1, 3)


def test_the_splitter_does_not_re_encode():
    """The whole point: cutting an hour-long file costs seconds."""
    src = read_source("tasks", "conversion_tasks.py")
    assert '"-c",\n        "copy"' in src or '"-c", "copy"' in src
    assert '"-f",\n        "segment"' in src or '"-f", "segment"' in src
    assert '"-reset_timestamps"' in src
    assert '"-map",\n        "0"' in src or '"-map", "0"' in src


def test_the_converter_delegates_instead_of_splitting_its_own_way():
    """A second copy of the command is how the old one came to take wrong arguments."""
    src = read_source("media_converter.py")
    assert "from tasks.conversion_tasks import split_media_segments" in src
    assert "-segment_time" not in src
