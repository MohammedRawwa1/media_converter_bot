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
import time
from types import SimpleNamespace

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


def test_the_media_has_to_already_be_in_the_container_the_target_names():
    """AAC and M4A are one codec in two containers - the container is the other half.

    An ``.aac`` source asked for as ``m4a`` is AAC in ADTS against AAC in MP4: the
    codec matches and the container does not, so it is a real conversion. The
    extension is the container the media was delivered in, and a source with none
    is never called a match.
    """
    assert gate.container_is_target({"name": "track.mp3"}, "mp3") is True
    assert gate.container_is_target({"name": "Track.MP3"}, "mp3") is True
    assert gate.container_is_target({"name": "track.m4a"}, "mp3") is False
    assert gate.container_is_target({"name": "track.aac"}, "m4a") is False
    assert gate.container_is_target({"name": "track.m4a"}, "m4a") is True
    assert gate.container_is_target({"name": "track"}, "mp3") is False
    assert gate.container_is_target({}, "mp3") is False
    # No container asked for is no constraint - the bitrate-only callers.
    assert gate.container_is_target({"name": "track.m4a"}, None) is True
    assert gate.container_is_target(None, "") is True


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


def test_the_same_bitrate_in_another_container_is_not_a_match(monkeypatch):
    """The codec is the same and the container is not - so it is a real conversion."""
    _verdict(monkeypatch, 64000, "aac")
    current = {"name": "track.aac"}

    assert _run(gate.already_at_bitrate(current, "64k", target_codec="aac")) == 64000
    assert _run(gate.already_at_bitrate(current, "64k", target_codec="aac", target_format="m4a")) is None
    assert _run(gate.already_at_bitrate({"name": "track.m4a"}, "64k", target_codec="aac", target_format="m4a")) == 64000


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


def test_the_media_cache_answers_a_media_that_was_already_ingested(monkeypatch):
    """Check one (where the object is) and check two (what it carries) in one read.

    The descriptor the ingest wrote says both: the key the bytes were stored under
    and the ffprobe verdict it captured. Reading it here is what makes a media
    this bot has already fetched answerable from a dictionary lookup instead of
    falling through to a userbot header read - and a header read that account
    cannot reach answers "nothing", which is how a file already at the bitrate
    used to be fetched over again and re-encoded into itself.
    """
    import utils.media_cache as media_cache

    async def _lookup(uid, *, expected_size=None):
        return {
            "input_key": "inputs/library/abc/source",
            "source_meta": {"audio_bitrate": 64000, "audio_codec": "mp3"},
        }

    async def _boom(*args, **kwargs):
        raise AssertionError("a descriptor that carries the verdict needs no read at all")

    monkeypatch.setattr(media_cache, "lookup", _lookup)
    monkeypatch.setattr(gate, "_probe_file", _boom)
    monkeypatch.setattr(gate, "_probe_stored", _boom)
    monkeypatch.setattr(gate, "_probe_telegram", _boom)

    assert _run(gate.source_verdict({"file_unique_id": "uid", "size": 49289926})) == (64000, "mp3")
    assert _run(gate.already_at_bitrate({"file_unique_id": "uid", "name": "Module 02.mp3"}, "64k")) == 64000


def test_a_descriptor_without_a_verdict_still_names_the_object_to_read(monkeypatch):
    """A descriptor that only knows where the media lives still saves the fetch."""
    import utils.media_cache as media_cache

    async def _lookup(uid, *, expected_size=None):
        return {"input_key": "inputs/library/abc/source"}

    seen = {}

    async def _stored(current_file, dest_dir, timeout=None):
        seen["key"] = current_file.get("input_key")
        return {"audio_bitrate": 64000, "audio_codec": "mp3"}

    monkeypatch.setattr(media_cache, "lookup", _lookup)
    monkeypatch.setattr(gate, "_probe_stored", _stored)

    assert _run(gate.source_verdict({"file_unique_id": "uid"})) == (64000, "mp3")
    assert seen["key"] == "inputs/library/abc/source"


def test_the_shared_library_key_names_the_object_without_any_descriptor():
    """The media's own identity names the object its bytes live under.

    That key is derived rather than remembered, so a media this deployment has
    ingested is still readable here after the Redis descriptor that named it has
    expired - and a key nothing was stored under just fails the ranged GET.
    """
    from utils.media_cache import media_library_key

    assert gate._storage_key({"file_unique_id": "AgADzRkAAtS8OVE"}) == media_library_key("AgADzRkAAtS8OVE")
    assert gate._storage_key({"input_key": "inputs/x/source", "file_unique_id": "u"}) == "inputs/x/source"
    assert gate._storage_key({}) is None
    assert gate._storage_key(None) is None


def test_a_cache_that_cannot_be_reached_is_not_a_blocker(monkeypatch):
    import utils.media_cache as media_cache

    async def _boom(uid, *, expected_size=None):
        raise RuntimeError("redis is gone")

    async def _telegram(*args, **kwargs):
        return {"audio_bitrate": 64000, "audio_codec": "mp3"}

    monkeypatch.setattr(media_cache, "lookup", _boom)
    monkeypatch.setattr(gate, "_probe_telegram", _telegram)

    assert _run(gate.source_verdict({"file_unique_id": "uid", "chat_id": 1, "msg_id": 2})) == (64000, "mp3")


def test_a_cached_descriptor_at_another_bitrate_is_still_a_conversion(monkeypatch):
    """Where the media is and what it carries are two separate answers."""
    import utils.media_cache as media_cache

    async def _lookup(uid, *, expected_size=None):
        return {
            "input_key": "inputs/library/abc/source",
            "source_meta": {"audio_bitrate": 128000, "audio_codec": "mp3"},
        }

    async def _nothing(*args, **kwargs):
        return None

    monkeypatch.setattr(media_cache, "lookup", _lookup)
    monkeypatch.setattr(gate, "_probe_stored", _nothing)
    monkeypatch.setattr(gate, "_probe_telegram", _nothing)

    current = {"file_unique_id": "uid", "name": "Module 02.mp3"}
    # What the media carries is still read - and it is a different bitrate, so
    # the request is a real conversion rather than a no-op.
    assert _run(gate.source_verdict(current)) == (128000, "mp3")
    assert _run(gate.already_at_bitrate(current, "64k")) is None


def test_a_deployment_with_no_media_identity_is_not_asked_for_one(monkeypatch):
    """No ``file_unique_id`` means no descriptor to look up - and no lookup."""
    import utils.media_cache as media_cache

    async def _boom(uid, *, expected_size=None):
        raise AssertionError("a media with no identity has no descriptor")

    async def _stored(current_file, dest_dir, timeout=None):
        return {"audio_bitrate": 64000, "audio_codec": "mp3"}

    monkeypatch.setattr(media_cache, "lookup", _boom)
    monkeypatch.setattr(gate, "_probe_stored", _stored)

    assert _run(gate.source_verdict({"input_key": "inputs/x/source"})) == (64000, "mp3")


def test_a_local_copy_is_probed_before_storage_is_read(monkeypatch, tmp_path):
    local = tmp_path / "src.mp3"
    local.write_bytes(b"x")
    order = []

    async def _file(path, timeout=None):
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
    async def _stored(current_file, dest_dir, timeout=None):
        return {"audio_bitrate": 64000, "audio_codec": "mp3"}

    async def _no_telegram(*args, **kwargs):
        raise AssertionError("storage already answered")

    monkeypatch.setattr(gate, "_probe_stored", _stored)
    monkeypatch.setattr(gate, "_probe_telegram", _no_telegram)

    assert _run(gate.source_verdict({"input_key": "inputs/x/source"})) == (64000, "mp3")


def test_an_exhausted_budget_reads_nothing_at_all(monkeypatch):
    """Past the budget the request takes the path it always took, at once."""

    async def _boom(*args, **kwargs):
        raise AssertionError("no tier may run once the budget is spent")

    monkeypatch.setattr(gate, "_probe_file", _boom)
    monkeypatch.setattr(gate, "_probe_stored", _boom)
    monkeypatch.setattr(gate, "_probe_telegram", _boom)

    assert _run(gate.source_verdict({"input_key": "inputs/x/source"}, budget_seconds=0)) == (None, None)


def test_every_tier_gets_only_what_is_left_of_one_budget(monkeypatch):
    """Three tiers must not add up to three timeouts inside a button press."""
    seen = []

    async def _stored(current_file, dest_dir, timeout=None):
        seen.append(timeout)
        return None

    async def _telegram(current_file, user_id, dest_dir, timeout=None):
        seen.append(timeout)
        return None

    monkeypatch.setattr(gate, "_probe_stored", _stored)
    monkeypatch.setattr(gate, "_probe_telegram", _telegram)

    assert _run(gate.source_verdict({"input_key": "inputs/x/source"}, budget_seconds=5)) == (None, None)
    assert len(seen) == 2
    assert all(0 < left <= 5 for left in seen)
    assert seen[1] <= seen[0], "the second attempt gets what the first left behind"


def test_a_patient_probe_is_abandoned_at_the_budget(monkeypatch, tmp_path):
    """The real ffprobe call is what the budget has to cut short."""
    import utils.ffmpeg_runner as runner

    local = tmp_path / "song.mp3"
    local.write_bytes(b"x" * 32)

    async def _hangs(path):
        await asyncio.sleep(30)
        return {"audio_bitrate": 64000}

    monkeypatch.setattr(runner, "probe_media", _hangs)

    started = time.monotonic()
    verdict = _run(gate.source_verdict({"path": str(local)}, budget_seconds=0.05))

    assert verdict == (None, None)
    assert time.monotonic() - started < 5, "the check must not wait the probe out"


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
    assert body.index("_already_at_bitrate(current_file") < body.index("_ensure_local_media")


def test_a_batch_answers_an_audio_file_that_is_already_at_the_bitrate():
    body = flatten(read_source(HANDLERS))

    assert '_plan["convert_type"] == "extract_audio" and f.get("type") == "audio"' in body
    # All three answers, the same ones the single-file button asks: the source's
    # own header, its bitrate, and its container (an MP3 target, named here).
    assert (
        'await _already_at_bitrate(f, _plan["extract_bitrate"], user_id=user_id, '
        'target_codec="mp3", target_format="mp3")' in body
    )
    # Answered as its own outcome, never as a fetch failure or a skip.
    assert "already += 1" in body
    assert "nothing to re-encode" in body


def test_the_gate_is_only_applied_where_the_user_already_holds_the_file():
    """Extracting FROM a video produces a file the user has not got yet.

    Reporting "already 64k" there would hand the user nothing at all, so the gate
    belongs to the requests that re-encode a file they already have - the
    single-file bitrate picker, a batch's Extract Audio on an audio source, and
    Convert Format *to MP3*, which asks for a codec the user's audio may already
    be, at the bitrate that branch encodes with.
    """
    for name in ("convert_to_mp3", "normalize_audio"):
        assert "_already_at_bitrate(" not in _method_body(name)


def test_the_audio_format_converter_checks_bitrate_format_and_container():
    """All three answers, and all three before the fetch.

    The button used to compare an MP3 request against a hard-coded 128k and leave
    every other target alone. It now reads the user's own bitrate setting - the one
    /usersettings stores - asks the source's codec against the target's, and asks
    its container against the target's format, so an MP3 that is already 64k in the
    container MP3 asks for is answered instead of fetched.
    """
    body = _method_body("convert_audio_format")

    assert 'if format_type == "mp3":' not in body, "the check is no longer MP3-only"
    # The bitrate is read through the one resolver every audio button shares, so
    # this button cannot carry a default of its own.
    assert "audio_bitrate = _effective_audio_bitrate(current_file, update.effective_user.id)" in body
    # The target's own codec and container travel with the request.
    assert "AUDIO_FORMAT_PROBE_CODECS.get(format_type)" in body
    assert "target_codec=_probe_codec" in body
    assert "target_format=format_type" in body
    # Only the targets whose encoder takes a bitrate: WAV and FLAC have none.
    assert "audio_format_takes_bitrate(format_type)" in body
    # Before the fetch it exists to avoid, like every other wired site.
    assert body.index("_already_at_bitrate(") < body.index("_ensure_local_media")
    assert body.count("_already_at_bitrate(") == 1


def test_the_format_converter_stops_instead_of_re_encoding_an_mp3(monkeypatch):
    import handlers as handlers_module

    handler = object.__new__(handlers_module.EnhancedMediaHandler)
    edits = []

    async def _yes(*_args, **_kwargs):
        return True

    async def _edit(query, text, **_kwargs):
        edits.append(text)
        return True

    async def _no_fetch(*_args, **_kwargs):
        raise AssertionError("an MP3 that is already an MP3 must not be fetched")

    async def _already(*_args, **_kwargs):
        return 128000

    handler._require_callback = _yes
    handler._check_conversion_quota = _yes
    handler.safe_edit = _edit
    handler._ensure_current_file_downloaded = _no_fetch
    monkeypatch.setattr(handlers_module, "_already_at_bitrate", _already)
    # The bitrate comes from /usersettings now; pin it so the answer does not
    # depend on a settings file some other test may have written.
    monkeypatch.setattr(handlers_module, "_user_audio_bitrate", lambda _uid: "128k")

    session = {"current_file": {"id": "x", "name": "song.mp3", "type": "audio"}}
    update = SimpleNamespace(
        callback_query=SimpleNamespace(message=SimpleNamespace()),
        effective_user=SimpleNamespace(id=7),
        effective_chat=SimpleNamespace(id=7),
    )

    _run(handler.convert_audio_format(update, SimpleNamespace(bot=SimpleNamespace()), session, "mp3"))

    assert edits == ["🔄 Converting to MP3...", "ℹ️ Already 128k — nothing to re-encode."]


def test_every_audio_button_works_under_the_one_resolved_bitrate(monkeypatch):
    """The file's own pick wins, then /usersettings, then the shared default.

    One resolver is what keeps a button from carrying an idea of its own about the
    quality: the number a picker marks and the number ffmpeg is given come from
    the same place.
    """
    import handlers as handlers_module

    monkeypatch.setattr(handlers_module, "_user_audio_bitrate", lambda _uid: "64k")

    resolve = handlers_module._effective_audio_bitrate
    # The settings menu's value, when the file has no pick of its own.
    assert resolve({}, 7) == "64k"
    assert resolve(None, 7) == "64k"
    # The file's own pick wins over the setting.
    assert resolve({"audio_bitrate": "192k"}, 7) == "192k"
    # An explicit argument (the video -> MP3 quality picker) wins over both.
    assert resolve({"audio_bitrate": "192k"}, 7, override="96k") == "96k"
    # An unusable value falls back to the shared default rather than reaching ffmpeg.
    assert resolve({"audio_bitrate": "nope"}, 7) == handlers_module._DEFAULT_AUDIO_BITRATE


def test_the_format_picker_states_the_bitrate_it_will_use(monkeypatch):
    """The audio picker names the constant, so the button visibly listens to it."""
    import handlers as handlers_module

    assert "_format_picker_prompt(" in read_source(HANDLERS)
    monkeypatch.setattr(handlers_module, "_user_audio_bitrate", lambda _uid: "64k")

    text = handlers_module._format_picker_prompt("Convert Format", "audio", {}, 7)
    assert "64k" in text
    assert "from /usersettings" in text
    # A pick that is only on the file is not the preference, and is not called one.
    per_file = handlers_module._format_picker_prompt("Convert Format", "audio", {"audio_bitrate": "192k"}, 7)
    assert "192k" in per_file
    assert "set for this file" in per_file
    # The video picker has no bitrate constant to name.
    assert "64k" not in handlers_module._format_picker_prompt("Convert Video Format", "video", {}, 7)


def test_the_audio_menus_state_the_bitrate_in_force_and_where_it_came_from(monkeypatch):
    """Video To Audio and Adjust Bitrate both name the value in force.

    The pickers mark a value; these menus also say which value that is, read
    through the one resolver - so the number the user sees is the number the
    encode uses.
    """
    import handlers as handlers_module

    monkeypatch.setattr(handlers_module, "_user_audio_bitrate", lambda _uid: "64k")

    assert handlers_module._audio_bitrate_source({}) == "from /usersettings"
    assert handlers_module._audio_bitrate_source({"audio_bitrate": "192k"}) == "set for this file"
    assert handlers_module._audio_bitrate_source({"audio_bitrate": "nope"}) == "from /usersettings"

    body = flatten(read_source(HANDLERS))
    # Video To Audio's quality picker, the Adjust Bitrate picker, and the batch
    # Extract Audio picker - each states the value in force and its source.
    assert "Bitrate in force: **{current}** ({_audio_bitrate_source(current_file)})" in body
    assert "Bitrate in force: **{_current_bitrate}** ({_audio_bitrate_source(current_file)})" in body
    assert "Bitrate in force: <b>{current}</b> (from /usersettings)" in body


def test_the_handler_wrapper_forwards_the_container_check(monkeypatch):
    """A keyword the wrapper does not accept would be swallowed, not raised.

    ``_already_at_bitrate`` answers "not already" for *any* failure - including a
    call it cannot make - so a container check that never reached the gate looks
    exactly like a file that is not a duplicate. Every keyword the gate takes has
    to be accepted here and passed on.
    """
    import handlers as handlers_module
    from utils import audio_bitrate_gate

    seen = {}

    async def _fake(current_file, target, *, user_id=None, target_codec="mp3", target_format=None):
        seen.update(target=target, user_id=user_id, target_codec=target_codec, target_format=target_format)
        return 64000

    monkeypatch.setattr(audio_bitrate_gate, "already_at_bitrate", _fake)

    found = _run(handlers_module._already_at_bitrate({}, "64k", user_id=7, target_codec="aac", target_format="m4a"))

    assert found == 64000
    assert seen == {"target": "64k", "user_id": 7, "target_codec": "aac", "target_format": "m4a"}


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
