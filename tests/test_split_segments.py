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


def _patch_spawn(monkeypatch, *, returncode=0, stderr=b"", parts=3, size=16, stamp_returncode=0):
    """Record the argv and write the part files ffmpeg would have written.

    The source-tag probe is neutralised: these tests are about the cut itself,
    and the stand-in sources are not readable media, so what a real ffprobe makes
    of one of them is not something the command they pin should depend on. The
    tags the parts are given have their own tests.

    ``seen["cmd"]`` is the **cut** - the call that carries the ``%03d`` pattern -
    because a split of an audio container is followed by one tag pass per part
    (see ``_stamp_split_part``); those are collected in ``seen["tag_calls"]``.
    """
    seen = {}

    async def _spawn(*cmd, **kwargs):
        if "%03d" in str(cmd[-1]):
            seen["cmd"] = list(cmd)
            if returncode == 0:
                for index in range(1, parts + 1):
                    path = cmd[-1].replace("%03d", f"{index:03d}")
                    with open(path, "wb") as fh:
                        fh.write(b"x" * size)
            seen.setdefault("calls", []).append(list(cmd))
            return _FakeProcess(returncode, stderr)
        seen.setdefault("tag_calls", []).append(list(cmd))
        if stamp_returncode == 0:
            with open(cmd[-1], "wb") as fh:
                fh.write(b"tagged")
        return _FakeProcess(stamp_returncode, b"" if stamp_returncode == 0 else b"nothing to write")

    async def _no_tags(_path):
        return None

    monkeypatch.setattr(conversion_tasks, "_spawn_process", _spawn)
    monkeypatch.setattr(conversion_tasks, "_probe_split_source_meta", _no_tags)
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
        "-fflags",
        "+genpts",
        "-segment_format_options",
        "movflags=+faststart",
        # Stated, because ffmpeg's default copy does not carry every format field:
        # a real mp4 part came back without its ``creation_time``.
        "-map_metadata",
        "0",
        os.path.join(str(out_dir), "Concert.%03d.mp4"),
    ]
    assert [os.path.basename(p) for p in parts] == ["Concert.001.mp4", "Concert.002.mp4", "Concert.003.mp4"]


def test_every_part_carries_its_duration_in_its_own_header(tmp_path, monkeypatch):
    """A part's length must be readable before the part is fetched.

    The mov/mp4 family writes its index (``moov``) at the *end* of the file unless
    it is told otherwise, and a player that streams a part - Telegram's above all -
    reads the header first: every part then showed ``00:00 / 00:00`` until it had
    been downloaded whole, and only the one that happened to arrive completely
    showed a length. ``movflags=+faststart`` moves that index to the front, and it
    has to be handed over as ``-segment_format_options`` - the same flag applied to
    the ``segment`` muxer (``-movflags``) never reaches the muxer writing the part.
    """
    src = _source_file(tmp_path)
    seen = _patch_spawn(monkeypatch)

    _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 60, ext=".mp4", stem="Concert"))

    cmd = seen["cmd"]
    assert cmd[cmd.index("-segment_format_options") + 1] == "movflags=+faststart"
    # The flag belongs to the per-part muxer, not to the segment muxer itself.
    assert "-movflags" not in cmd
    # ``-write_index 1`` writes the index at the end of each part - the default,
    # and exactly what hid the durations.
    assert "-write_index" not in cmd


@pytest.mark.parametrize("ext", [".mkv", ".avi", ".webm"])
def test_containers_that_reject_the_flag_do_not_get_it(tmp_path, monkeypatch, ext):
    """``movflags`` is a mov/mp4 option: a container that refuses it fails the run."""
    src = _source_file(tmp_path, f"Concert{ext}")
    seen = _patch_spawn(monkeypatch)

    ok, parts, _error = _run(
        conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 60, ext=ext, stem="Concert")
    )

    assert ok and parts
    assert "-segment_format_options" not in seen["cmd"]


def test_an_audio_part_keeps_the_header_that_states_its_length(tmp_path, monkeypatch):
    """The part's own header is written for ID3v2.3 and never asked for a time.

    The splitter once rewrote every audio part with ``-metadata duration=…`` into
    a ``<part>.tmp`` file - which ffmpeg cannot even open (no output format for
    ``.tmp``), so the pass burned a process per part and fixed nothing. What
    states a part's length is the Xing/Info header the muxer writes for it.

    The parts *are* post-processed now, for the one thing the cut cannot state
    (each part's own title and track number) - and that pass keeps this promise:
    it is a stream copy into a sibling that keeps the part's extension, so the
    frames, and with them that header, are copied through untouched.
    """
    src = _source_file(tmp_path, "Album.mp3")
    seen = _patch_spawn(monkeypatch)

    _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 600, ext=".mp3", stem="Album"))

    cmd = seen["cmd"]
    assert cmd[cmd.index("-id3v2_version") + 1] == "3"
    assert cmd[cmd.index("-avoid_negative_ts") + 1] == "make_zero"
    # No part is ever asked to carry a duration, and no output is called ".tmp".
    assert not any(part.startswith("duration=") for part in cmd)
    for call in seen["calls"] + seen.get("tag_calls", []):
        assert not call[-1].endswith(".tmp")


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


# ─────────────────────────────────────────────────────────────────────────────
# The tags a part carries
# ─────────────────────────────────────────────────────────────────────────────


def test_a_media_with_no_tags_of_its_own_names_its_parts():
    """The gap ``-map_metadata 0`` cannot fill: there is nothing to copy.

    An ordinary untagged ``Module 02.mp3`` splits into parts whose only tag is the
    encoder's, so nothing about the media shows in a player - that is ffmpeg's
    default, and stating the name it does have is the way out of it.
    """
    assert conversion_tasks._split_metadata_args({"title": "", "performer": ""}, "Module 02") == [
        "-metadata",
        "title=Module 02",
    ]
    assert conversion_tasks._split_metadata_args({}, "Album") == ["-metadata", "title=Album"]


def test_the_tags_the_source_does_carry_are_restated_along_with_them():
    args = conversion_tasks._split_metadata_args({"title": "Real Title", "performer": "Real Artist"}, "Album")

    assert args == ["-metadata", "title=Real Title", "-metadata", "artist=Real Artist"]
    # The same thing under the other name the ingest verdicts use.
    assert conversion_tasks._split_metadata_args({"title": "T", "artist": "A"}, "Album") == [
        "-metadata",
        "title=T",
        "-metadata",
        "artist=A",
    ]


def test_a_source_whose_tags_cannot_be_read_is_left_to_ffmpeg():
    """A verdict nobody could read is not evidence of an empty one."""
    assert conversion_tasks._split_metadata_args(None, "Album") == []
    assert conversion_tasks._split_metadata_args("nonsense", "Album") == []


def test_a_media_nobody_can_read_is_described_with_nothing(tmp_path, monkeypatch):
    """An unreadable probe is not a media with no tags.

    When the verdict comes back empty - no ffprobe on the box, a file it refuses -
    the split has two options: state the file name as the title, or leave the tags
    alone. Stating it overwrites the real title ffmpeg is about to copy, which is
    the one outcome worse than the blank this exists to fix, so nothing is stated
    and the cut copies whatever the source has.
    """
    src = _source_file(tmp_path, "Module 02.mp3")
    seen = _patch_spawn(monkeypatch)

    async def _unreadable(_path):
        return {}

    monkeypatch.setattr(conversion_tasks, "_probe_split_source_meta", _unreadable)

    _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 600, ext=".mp3", stem="Module 02"))

    assert "-metadata" not in seen["cmd"]
    # Nothing is stated to the parts either: no pass is run at all.
    assert not seen.get("tag_calls")


def test_the_probe_answers_none_when_it_has_nothing(tmp_path, monkeypatch):
    """Both ways of having no answer are one answer: ``None``."""

    async def _empty(_path):
        return {}

    async def _read(_path):
        return {"title": "Real Title", "duration": 12.5}

    import utils.ffmpeg_runner as ffmpeg_runner

    monkeypatch.setattr(ffmpeg_runner, "probe_media", _empty)
    assert _run(conversion_tasks._probe_split_source_meta("whatever.mp3")) is None

    monkeypatch.setattr(ffmpeg_runner, "probe_media", _read)
    assert _run(conversion_tasks._probe_split_source_meta("whatever.mp3"))["title"] == "Real Title"

    async def _boom(_path):
        raise RuntimeError("no ffprobe here")

    monkeypatch.setattr(ffmpeg_runner, "probe_media", _boom)
    assert _run(conversion_tasks._probe_split_source_meta("whatever.mp3")) is None


def test_the_parts_are_told_what_the_media_is(tmp_path, monkeypatch):
    src = _source_file(tmp_path, "Module 02.mp3")
    seen = _patch_spawn(monkeypatch)

    async def _tags(_path):
        return {"title": "", "performer": "", "album": ""}

    monkeypatch.setattr(conversion_tasks, "_probe_split_source_meta", _tags)

    _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 600, ext=".mp3", stem="Module 02"))

    cmd = seen["cmd"]
    assert cmd[cmd.index("-metadata") + 1] == "title=Module 02"
    # ``-map_metadata 0`` still runs: a part keeps whatever else the source had.
    assert cmd[cmd.index("-map_metadata") + 1] == "0"
    # The cut is one run, and a stream copy.
    assert len(seen["calls"]) == 1
    assert [cmd[cmd.index("-c") + 1]] == ["copy"]


def test_every_audio_part_is_given_its_own_title_and_track_number(tmp_path, monkeypatch):
    """A player has to be able to list the parts separately, not the media twice.

    The segment muxer writes one set of global tags to every part - ffmpeg has no
    per-segment metadata option - so ``Module 02`` would arrive as three files a
    player shows under the same title with no number between them. Each part gets
    its own pass, and that pass is a stream copy: nothing is re-encoded.
    """
    src = _source_file(tmp_path, "Module 02.mp3")
    seen = _patch_spawn(monkeypatch, parts=3)

    async def _tags(_path):
        return {"title": "", "performer": "Real Artist"}

    monkeypatch.setattr(conversion_tasks, "_probe_split_source_meta", _tags)

    ok, parts, _error = _run(
        conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 600, ext=".mp3", stem="Module 02")
    )

    assert ok and len(parts) == 3
    assert len(seen["tag_calls"]) == 3
    for index, call in enumerate(seen["tag_calls"], 1):
        assert call[call.index("-c") + 1] == "copy"
        assert call[call.index("-metadata") + 1] == f"title=Module 02 (part {index}/3)"
        assert f"track={index}/3" in call
        # Every part names the same album and album artist, the pair a player
        # groups the three by.
        assert "album=Module 02" in call
        assert "album_artist=Real Artist" in call
        # The artist the media did carry is still carried.
        assert "artist=Real Artist" in call
        # Each pass writes into its own part, through a sibling that keeps the
        # extension (a `.tmp` name has no muxer ffmpeg would write).
        assert call[-1] == os.path.join(str(tmp_path / "out"), f"Module 02.{index:03d}.tagging.mp3")
        assert call[call.index("-i") + 1].endswith(f"Module 02.{index:03d}.mp3")


def test_a_stream_tagged_container_is_told_on_the_stream(tmp_path, monkeypatch):
    """Ogg/Opus keep their tags on the stream, where a global tag does not win.

    ``-map_metadata 0`` has just copied the cut's own title onto that stream, and
    the global ``-metadata`` is ignored for a key that is already there - the part
    would come back with the un-numbered title and only the track number would
    change. Verified against a real ogg: the global form silently kept
    ``Track Title``, the stream form replaced it.
    """
    src = _source_file(tmp_path, "Album.ogg")
    seen = _patch_spawn(monkeypatch, parts=2)

    async def _tags(_path):
        return {"title": "Album"}

    monkeypatch.setattr(conversion_tasks, "_probe_split_source_meta", _tags)

    _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 600, ext=".ogg", stem="Album"))

    assert len(seen["tag_calls"]) == 2
    for index, call in enumerate(seen["tag_calls"], 1):
        assert "-metadata:s:a:0" in call
        assert f"title=Album (part {index}/2)" in call
        assert "album=Album" in call
        # A global tag would be dropped here, so there must not be one at all.
        assert all(item != "-metadata" for item in call)


def test_the_parts_share_an_album_so_a_player_groups_them():
    """Track numbers alone leave the parts unrelated; an album names the media.

    An untagged media carries no album of its own, so its parts would be listed as
    loose files that happen to number 1/2 and 2/2. The media's own name is the
    album they belong to.
    """
    args = conversion_tasks._part_tag_args({"title": ""}, "Module 02", 1, 2)
    assert "album=Module 02" in args
    # n/total on its own means the part belongs to *some* media; the album says which.
    assert "track=1/2" in args


def test_an_album_the_media_carried_is_not_replaced_by_its_name():
    """A real album is worth more than a grouping invented from a file name."""
    args = conversion_tasks._part_tag_args({"title": "Track", "album": "Real Album"}, "Media", 2, 2)
    assert "album=Real Album" in args
    assert args.count("album=Media") == 0


def test_the_parts_carry_the_album_artist_the_media_carried():
    """Album and album artist are the pair a player groups a release by."""
    args = conversion_tasks._part_tag_args(
        {"title": "Track", "album": "Real Album", "album_artist": "The Band"}, "Media", 1, 2
    )
    assert "album=Real Album" in args
    assert "album_artist=The Band" in args
    # The probe's own spelling of the field, which is what an ingest writes.
    assert "album_artist=The Band" in conversion_tasks._part_tag_args({"albumartist": "The Band"}, "M", 1, 2)
    assert "album_artist=The Band" in conversion_tasks._part_tag_args({"band": "The Band"}, "M", 1, 2)


def test_a_media_with_one_artist_fills_in_the_album_artist_it_lacks():
    """One artist and no album-artist field is that artist's album.

    Left blank, a library groups such a release by its *track* artists and files
    it under "Various Artists"; the artist the media names is what it means.
    """
    args = conversion_tasks._part_tag_args({"title": "Track", "performer": "Real Artist"}, "Media", 1, 2)
    assert "album_artist=Real Artist" in args
    # And the album artist named outright is not overwritten by the track artist.
    args = conversion_tasks._part_tag_args({"performer": "Track Artist", "album_artist": "The Band"}, "M", 1, 2)
    assert "album_artist=The Band" in args
    assert "album_artist=Track Artist" not in args


def test_an_untagged_media_is_not_given_an_album_artist_it_never_had():
    """The media's *name* is not an artist, so it is not filed as one.

    The album it is grouped under is the name it does have; the field that names
    a performer stays empty rather than crediting the file to "Module 02".
    """
    args = conversion_tasks._part_tag_args({"title": "", "performer": ""}, "Module 02", 1, 2)
    assert "album=Module 02" in args
    assert not any(item.startswith("album_artist=") for item in args)
    assert not any(item.startswith("artist=") for item in args)


def test_the_tag_option_follows_the_container():
    assert conversion_tasks._part_tag_args({"title": "T"}, "Album", 1, 2) == [
        "-metadata",
        "title=T (part 1/2)",
        "-metadata",
        "track=1/2",
        "-metadata",
        "album=Album",
    ]
    assert conversion_tasks._part_tag_args({"title": "T"}, "Album", 1, 2, "-metadata:s:a:0") == [
        "-metadata:s:a:0",
        "title=T (part 1/2)",
        "-metadata:s:a:0",
        "track=1/2",
        "-metadata:s:a:0",
        "album=Album",
    ]
    # The artist the media carried is numbered with it, and nothing is invented
    # for a source whose tags could not be read.
    assert conversion_tasks._part_tag_args({"title": "T", "performer": "P"}, "A", 2, 2)[-2:] == [
        "-metadata",
        "artist=P",
    ]
    assert conversion_tasks._part_tag_args(None, "Album", 1, 2) == []


def test_a_video_split_is_not_tagged_part_by_part(tmp_path, monkeypatch):
    """Re-muxing a video part costs a second full copy for a field nothing shows."""
    src = _source_file(tmp_path, "Concert.mp4")
    seen = _patch_spawn(monkeypatch)

    _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 600, ext=".mp4", stem="Concert"))

    # One run, and no tag pass: ``tag_calls`` is only ever created by one.
    assert "tag_calls" not in seen
    assert len(seen["calls"]) == 1


def test_a_part_that_cannot_be_tagged_keeps_the_bytes_ffmpeg_wrote(tmp_path, monkeypatch):
    """The tags are a nicety; the media the user asked for is not."""
    src = _source_file(tmp_path, "Album.mp3")
    out_dir = tmp_path / "out"
    seen = _patch_spawn(monkeypatch, parts=2, size=16, stamp_returncode=1)

    async def _tags(_path):
        return {"title": "Album"}

    monkeypatch.setattr(conversion_tasks, "_probe_split_source_meta", _tags)

    ok, parts, error = _run(conversion_tasks.split_media_segments(src, str(out_dir), 600, ext=".mp3", stem="Album"))

    assert ok and error == ""
    assert len(seen["tag_calls"]) == 2
    # The parts are still the ones the cut produced, and no staging file is left
    # behind for the next split of the same name to mistake for a part.
    assert [os.path.getsize(part) for part in parts] == [16, 16]
    assert sorted(os.listdir(out_dir)) == ["Album.001.mp3", "Album.002.mp3"]


def test_a_verdict_the_caller_already_has_is_not_probed_again(tmp_path, monkeypatch):
    """The ingest probed every stored media: the split must not do it twice."""
    src = _source_file(tmp_path, "Album.mp3")
    seen = _patch_spawn(monkeypatch)

    async def _boom(_path):
        raise AssertionError("the caller's verdict is the read")

    monkeypatch.setattr(conversion_tasks, "_probe_split_source_meta", _boom)

    _run(
        conversion_tasks.split_media_segments(
            src,
            str(tmp_path / "out"),
            600,
            ext=".mp3",
            stem="Album",
            source_meta={"title": "Real Title", "performer": "Real Artist"},
        )
    )

    cmd = seen["cmd"]
    assert cmd[cmd.index("-metadata") + 1] == "title=Real Title"
    assert "artist=Real Artist" in cmd


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


# ─────────────────────────────────────────────────────────────────────────────
# The single range: the same media, cut once
# ─────────────────────────────────────────────────────────────────────────────


def _patch_trim_spawn(monkeypatch, payload=b"cut"):
    seen = {}

    async def _spawn(*cmd, **kwargs):
        seen["cmd"] = list(cmd)
        with open(cmd[-1], "wb") as fh:
            fh.write(payload)
        return _FakeProcess(0, b"")

    monkeypatch.setattr(conversion_tasks, "_spawn_process", _spawn)
    return seen


def test_a_range_cut_carries_the_metadata_and_the_streams(tmp_path, monkeypatch):
    """A range is a part of the media, so it keeps what the media had.

    The default stream selection picks one video and one audio track, so a range
    of a subtitled video arrived with no subtitle track at all - and ffmpeg's
    default metadata copy left out ``creation_time``, the date a player shows for
    the file. Both are stated instead, which is a metadata and a mapping flag: the
    cut is still ``-c copy``.
    """
    src = _source_file(tmp_path, "Concert.mp4")
    target = str(tmp_path / "out" / "Concert.001.mp4")
    os.makedirs(os.path.dirname(target), exist_ok=True)
    seen = _patch_trim_spawn(monkeypatch)

    ok, _message = _run(conversion_tasks.trim_media(src, target, "00:00:10", "00:00:40"))

    assert ok
    cmd = seen["cmd"]
    assert cmd[0] == conversion_tasks.FFMPEG_PATH
    assert cmd[cmd.index("-map") + 1] == "0"
    assert cmd[cmd.index("-map_metadata") + 1] == "0"
    assert cmd[cmd.index("-c") + 1] == "copy"
    assert cmd[cmd.index("-t") + 1] == "30.0"


def test_a_cut_into_another_container_does_not_map_every_stream(tmp_path, monkeypatch):
    """A container the source's streams do not belong to may refuse them.

    ``-map 0`` keeps a subtitle track in the source's own container, but the same
    mapping into a different one can fail the whole run (an MP4 cannot hold the
    subtitle codecs an MKV can) - a cut that drops a stream still produces the cut.
    """
    src = _source_file(tmp_path, "Concert.mkv")
    target = str(tmp_path / "Concert.001.mp4")
    seen = _patch_trim_spawn(monkeypatch)

    ok, _message = _run(conversion_tasks.trim_media(src, target, "00:00:00", "00:00:10"))

    assert ok
    assert "-map" not in seen["cmd"]
    # The metadata copy is not a stream mapping: it is asked for either way.
    assert seen["cmd"].count("-map_metadata") == 1


def test_the_audio_containers_this_bot_delivers_as_audio_are_numbered(tmp_path, monkeypatch):
    """A part whose container carries tags is told which part it is; ``.aac`` cannot."""
    assert ".wma" in conversion_tasks._SPLIT_TAG_AUDIO_EXTS
    assert ".aac" not in conversion_tasks._SPLIT_TAG_AUDIO_EXTS
    src = _source_file(tmp_path, "Track.wma")
    seen = _patch_spawn(monkeypatch, parts=2)

    async def _tags(_path):
        return {"title": "", "performer": ""}

    monkeypatch.setattr(conversion_tasks, "_probe_split_source_meta", _tags)

    _run(conversion_tasks.split_media_segments(src, str(tmp_path / "out"), 600, ext=".wma", stem="Track"))

    assert len(seen["tag_calls"]) == 2
    assert "track=1/2" in seen["tag_calls"][0]
    assert "album=Track" in seen["tag_calls"][0]
