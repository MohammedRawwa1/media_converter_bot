"""The compare → validate → already-exists gate, and the metadata it protects.

"Adjust Bitrate 64k" on a file that is already 64k has nothing to encode, and the
expensive half of answering it anyway is the *fetch*: a 47MB audio is past what
the Bot API will hand a bot, so the request used to fall through to the userbot
pipeline, download the whole media and re-encode it into an identical file.

What these tests pin is that the verdict is reached from what the source already
carries, that it can never *block* a conversion it could not prove (an unknown,
unreadable or mismatched verdict all answer "not already"), and that the tags a
delivered file keeps come from one shared recipe instead of from whatever ffmpeg
happened to default to in each command.
"""

import ast
import asyncio

import pytest
from source_helpers import find_function, flatten, parse_source, read_source

from tasks import conversion_tasks
from utils import audio_bitrate_gate as gate


def _run(coro):
    return asyncio.run(coro)


# ─────────────────────────────────────────────────────────────────────────────
# compare: reading the requested bitrate
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "value,bps",
    [
        ("64k", 64000),
        ("64K", 64000),
        ("64kbps", 64000),
        ("128k", 128000),
        (64, 64000),
        (64.0, 64000),
        (" 96k ", 96000),
    ],
)
def test_a_requested_bitrate_is_read_in_every_form_the_bot_stores(value, bps):
    assert gate.target_bps(value) == bps


@pytest.mark.parametrize("value", ["", None, "custom", "abc", "0k", 0, False, True, "-64k"])
def test_an_unreadable_request_is_never_a_match(value):
    assert gate.target_bps(value) is None


def test_rounding_is_tolerated_and_a_real_difference_is_not():
    # A 64k CBR MP3 probes as 64000; a container that rounds it is still 64k.
    assert gate.bitrates_agree(64000, 64000) is True
    assert gate.bitrates_agree(63999, 64000) is True
    assert gate.bitrates_agree("64100", 64000) is True
    # The next step up from 64k is 50% away, so nothing real lands in between.
    assert gate.bitrates_agree(96000, 64000) is False
    assert gate.bitrates_agree(128000, 64000) is False


@pytest.mark.parametrize("source", [None, "", "not a number", 0, -64])
def test_a_verdict_without_a_usable_bitrate_is_not_a_match(source):
    assert gate.bitrates_agree(source, 64000) is False


# ─────────────────────────────────────────────────────────────────────────────
# validate: the codec has to be the one the encode produces
# ─────────────────────────────────────────────────────────────────────────────


def test_only_an_mp3_source_matches_an_mp3_encode():
    current = {"name": "track.mp3"}
    assert gate.codec_is_target("mp3", current) is True
    assert gate.codec_is_target("mp3float", current) is True
    # AAC 64k -> MP3 64k is a real conversion, not a no-op.
    assert gate.codec_is_target("aac", current) is False
    assert gate.codec_is_target("flac", current) is False


def test_a_verdict_without_a_codec_falls_back_to_the_extension():
    """The only case where two pieces of evidence together still answer it."""
    assert gate.codec_is_target("", {"name": "track.mp3"}) is True
    assert gate.codec_is_target(None, {"name": "Track.MP3"}) is True
    assert gate.codec_is_target("", {"name": "track.m4a"}) is False
    assert gate.codec_is_target("", {}) is False


def test_an_earlier_probe_verdict_is_read_in_both_shapes():
    """What the ingest wrote, and what a flattened job/entry carries instead."""
    assert gate.known_verdict({"_source_metadata": {"audio_bitrate": 64000, "audio_codec": "mp3"}}) == (
        64000,
        "mp3",
    )
    assert gate.known_verdict({"source_metadata": {"audio_bitrate": 64000}}) == (64000, None)
    assert gate.known_verdict({"source_audio_bitrate": "64000", "source_audio_codec": "mp3"}) == (
        "64000",
        "mp3",
    )
    assert gate.known_verdict({}) == (None, None)
    assert gate.known_verdict(None) == (None, None)


# ─────────────────────────────────────────────────────────────────────────────
# exists: the three answers
# ─────────────────────────────────────────────────────────────────────────────


def _verdict(monkeypatch, bitrate, codec, calls=None):
    async def _fake(current_file, *, user_id=None):
        if calls is not None:
            calls.append(user_id)
        return bitrate, codec

    monkeypatch.setattr(gate, "source_verdict", _fake)


def test_a_matching_source_is_answered_without_touching_the_media(monkeypatch):
    calls = []
    _verdict(monkeypatch, 64000, "mp3", calls)

    found = _run(gate.already_at_bitrate({"name": "Module 02.mp3"}, "64k", user_id=7))

    assert found == 64000
    assert calls == [7]


def test_a_different_bitrate_is_not_a_match(monkeypatch):
    _verdict(monkeypatch, 128000, "mp3")
    assert _run(gate.already_at_bitrate({"name": "a.mp3"}, "64k")) is None


def test_the_same_bitrate_in_another_codec_is_not_a_match(monkeypatch):
    """The bitrate alone must never be trusted: the codec is the other half."""
    _verdict(monkeypatch, 64000, "aac")
    assert _run(gate.already_at_bitrate({"name": "a.m4a"}, "64k")) is None


def test_an_unknown_verdict_leaves_the_conversion_alone(monkeypatch):
    _verdict(monkeypatch, None, None)
    assert _run(gate.already_at_bitrate({"name": "a.mp3"}, "64k")) is None


def test_a_broken_probe_is_never_what_blocks_a_conversion(monkeypatch):
    async def _boom(current_file, *, user_id=None):
        raise RuntimeError("ffprobe exploded")

    monkeypatch.setattr(gate, "source_verdict", _boom)
    assert _run(gate.already_at_bitrate({"name": "a.mp3"}, "64k")) is None


def test_the_check_can_be_switched_off_without_a_code_change(monkeypatch):
    """A deployment that would rather always re-encode says so with an env var."""
    _verdict(monkeypatch, 64000, "mp3")
    monkeypatch.setenv(gate.GATE_ENV, "0")

    assert gate.gate_enabled() is False
    assert _run(gate.already_at_bitrate({"name": "a.mp3"}, "64k")) is None

    monkeypatch.setenv(gate.GATE_ENV, "1")
    assert gate.gate_enabled() is True
    assert _run(gate.already_at_bitrate({"name": "a.mp3"}, "64k")) == 64000


def test_an_unreadable_request_asks_nothing_at_all(monkeypatch):
    async def _boom(current_file, *, user_id=None):
        raise AssertionError("the source must not be read for a request with no bitrate")

    monkeypatch.setattr(gate, "source_verdict", _boom)
    assert _run(gate.already_at_bitrate({"name": "a.mp3"}, "custom")) is None


# ─────────────────────────────────────────────────────────────────────────────
# the probes, cheapest evidence first
# ─────────────────────────────────────────────────────────────────────────────


def test_a_verdict_already_on_the_file_short_circuits_every_probe(monkeypatch):
    async def _boom(*args, **kwargs):
        raise AssertionError("a recorded verdict must be trusted before any read")

    monkeypatch.setattr(gate, "_probe_file", _boom)
    monkeypatch.setattr(gate, "_probe_stored", _boom)
    monkeypatch.setattr(gate, "_probe_telegram", _boom)

    verdict = _run(gate.source_verdict({"_source_metadata": {"audio_bitrate": 64000, "audio_codec": "mp3"}}))

    assert verdict == (64000, "mp3")


def test_a_local_copy_is_probed_before_storage_is_read(monkeypatch, tmp_path):
    local = tmp_path / "src.mp3"
    local.write_bytes(b"x")
    order = []

    async def _file(path):
        order.append("local")
        return {"audio_bitrate": 64000, "audio_codec": "mp3"}

    async def _stored(*args, **kwargs):
        order.append("stored")
        return None

    monkeypatch.setattr(gate, "_probe_file", _file)
    monkeypatch.setattr(gate, "_probe_stored", _stored)

    assert _run(gate.source_verdict({"path": str(local), "input_key": "inputs/x/source"})) == (64000, "mp3")
    assert order == ["local"]


def test_a_stored_object_is_read_when_there_is_no_local_copy(monkeypatch):
    async def _stored(current_file, dest_dir):
        return {"audio_bitrate": 64000, "audio_codec": "mp3"}

    async def _no_telegram(*args, **kwargs):
        raise AssertionError("storage already answered")

    monkeypatch.setattr(gate, "_probe_stored", _stored)
    monkeypatch.setattr(gate, "_probe_telegram", _no_telegram)

    assert _run(gate.source_verdict({"input_key": "inputs/x/source"})) == (64000, "mp3")


def test_the_most_likely_telegram_pair_is_tried_first():
    """The origin forward, then the chat the bot received the media in."""
    pairs = gate._telegram_candidates(
        {
            "forward": {"chat_id": -100123, "message_id": 5},
            "chat_id": 42,
            "msg_id": 9,
        }
    )
    assert pairs == [(-100123, 5), (42, 9)]
    # No pair repeated, and nothing invented from half a pair.
    assert gate._telegram_candidates({"chat_id": 42}) == []
    assert gate._telegram_candidates({"chat_id": 42, "msg_id": 9, "forward": {"chat_id": 42, "message_id": 9}}) == [
        (42, 9)
    ]


# ─────────────────────────────────────────────────────────────────────────────
# where the gate is wired: the audio-only paths
# ─────────────────────────────────────────────────────────────────────────────

HANDLERS = "handlers.py"


def _method_body(name):
    return flatten(ast.unparse(find_function(parse_source(HANDLERS), name)))


def test_adjusting_an_audio_bitrate_asks_before_it_downloads():
    body = _method_body("adjust_bitrate")

    assert "_already_at_bitrate(current_file, audio_bitrate" in body
    assert "_already_at_bitrate_text(audio_bitrate)" in body
    # The verdict comes before the fetch it exists to avoid.
    assert body.index("_already_at_bitrate(current_file") < body.index("_ensure_current_file_downloaded")


def test_a_batch_answers_an_audio_file_that_is_already_at_the_bitrate():
    body = flatten(read_source(HANDLERS))

    assert '_plan["convert_type"] == "extract_audio" and f.get("type") == "audio"' in body
    assert 'await _already_at_bitrate(f, _plan["extract_bitrate"], user_id=user_id)' in body
    # Answered as its own outcome, never as a fetch failure or a skip.
    assert "already += 1" in body
    assert "nothing to re-encode" in body


def test_the_gate_is_only_applied_where_the_user_already_holds_the_file():
    """Extracting FROM a video produces a file the user has not got yet.

    Reporting "already 64k" there would hand the user nothing at all, so the gate
    belongs to the requests that re-encode a file they already have - the
    single-file bitrate picker and a batch's Extract Audio on an audio source.
    """
    for name in ("convert_to_mp3", "normalize_audio"):
        assert "_already_at_bitrate(" not in _method_body(name)


def test_the_verdict_reads_like_the_answer_the_user_asked_for():
    import handlers

    assert handlers._already_at_bitrate_text("64k") == "ℹ️ Already 64k — nothing to re-encode."


# ─────────────────────────────────────────────────────────────────────────────
# the metadata a delivered file keeps
# ─────────────────────────────────────────────────────────────────────────────


def test_every_mp3_command_copies_the_tags_as_the_version_readers_understand():
    """One recipe for the splitter and the bitrate change alike.

    ``-map_metadata 0`` states the copy instead of relying on an invisible
    default, and ``-id3v2_version 3`` is what Windows Explorer reads - ffmpeg
    writes ID3v2.4 otherwise, and an empty Properties panel follows.
    """
    assert conversion_tasks.MP3_METADATA_ARGS == ("-map_metadata", "0", "-id3v2_version", "3")


def test_the_splitter_pins_the_same_tags_for_an_mp3_part(tmp_path, monkeypatch):
    src = tmp_path / "Album.mp3"
    src.write_bytes(b"payload")
    seen = {}

    class _Proc:
        returncode = 0
        stderr = b""

        async def communicate(self):
            return b"", b""

    async def _spawn(*cmd, **kwargs):
        seen["cmd"] = list(cmd)
        pattern = cmd[-1]
        for index in (1, 2):
            with open(pattern.replace("%03d", f"{index:03d}"), "wb") as fh:
                fh.write(b"x" * 8)
        return _Proc()

    monkeypatch.setattr(conversion_tasks, "_spawn_process", _spawn)

    ok, parts, _error = _run(
        conversion_tasks.split_media_segments(str(src), str(tmp_path / "out"), 600, ext=".mp3", stem="Album")
    )

    assert ok and parts
    cmd = seen["cmd"]
    assert cmd[cmd.index("-map_metadata") + 1] == "0"
    assert cmd[cmd.index("-id3v2_version") + 1] == "3"


def test_the_bitrate_change_encodes_with_that_same_recipe():
    body = _method_body("adjust_bitrate")

    assert 'cmd = [*MP3_METADATA_ARGS, "-c:a", "libmp3lame", "-b:a", audio_bitrate]' in body


def test_the_batch_extraction_uses_it_too():
    body = flatten(ast.unparse(find_function(parse_source(HANDLERS), "_resolve_bulk_plan")))

    assert 'ffmpeg_args = [*MP3_METADATA_ARGS, "-vn", "-acodec", "libmp3lame", "-ab", extract_bitrate]' in body


def test_a_media_that_landed_on_disk_is_probed_before_it_is_described():
    """The captions and the player tags are built from this one field.

    Every path that puts a media on local disk - here both download fallbacks -
    has to record the verdict the object-storage ingest path records, or the
    delivery describes the file by its name instead of by its tags.
    """
    body = _method_body("_ensure_current_file_downloaded")

    assert "await _probe_downloaded_source(current_file, file_path)" in body


def test_the_fallback_probe_reads_the_media_and_never_overwrites_a_verdict(tmp_path, monkeypatch):
    import handlers

    src = tmp_path / "Module 02.mp3"
    src.write_bytes(b"payload")
    current = {}

    async def _probe(path):
        return {"audio_bitrate": 64000, "audio_codec": "mp3", "title": "Module 02", "performer": "Someone"}

    monkeypatch.setattr("utils.ffmpeg_runner.probe_media", _probe)

    meta = _run(handlers._probe_downloaded_source(current, str(src)))

    assert meta["performer"] == "Someone"
    assert current["_source_metadata"]["title"] == "Module 02"
    # A second call keeps what is already there - no second ffprobe, no overwrite.
    current["_source_metadata"] = {"title": "Real Title"}

    async def _boom(path):
        raise AssertionError("a recorded verdict must not be probed again")

    monkeypatch.setattr("utils.ffmpeg_runner.probe_media", _boom)
    assert _run(handlers._probe_downloaded_source(current, str(src))) == {"title": "Real Title"}


def test_a_missing_file_is_no_verdict_at_all(tmp_path):
    import handlers

    current = {}
    assert _run(handlers._probe_downloaded_source(current, str(tmp_path / "gone.mp3"))) == {}
    assert "_source_metadata" not in current


def test_a_reused_media_keeps_the_verdict_its_descriptor_carries():
    """A cache hit must not describe the media worse than its first request did."""
    import handlers

    current = {"name": "a.mp3"}
    handlers._merge_cached_source_meta(current, {"source_meta": {"title": "T", "performer": "P", "duration": 10}})

    assert current["_source_metadata"]["title"] == "T"
    assert current["_source_metadata"]["performer"] == "P"
    # What is already on the file wins over the stored copy.
    current["_source_metadata"]["title"] = "Local"
    handlers._merge_cached_source_meta(current, {"source_meta": {"title": "Stored"}})
    assert current["_source_metadata"]["title"] == "Local"
    # Nothing to merge is not an error.
    handlers._merge_cached_source_meta({"name": "b.mp3"}, None)
    handlers._merge_cached_source_meta(None, {"source_meta": {"title": "T"}})


def test_every_media_cache_tier_carries_the_verdict_across():
    body = read_source(HANDLERS)

    assert body.count("_merge_cached_source_meta(current_file, _entry)") == 3
