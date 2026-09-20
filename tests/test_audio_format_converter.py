"""The audio converter button: every target it offers, encoded the same way everywhere.

Two tables used to answer "what codec is M4A?" - the in-process converter's and the
one the handler handed the worker - and they disagreed. The in-process one had no
``m4a`` entry and fell back to libmp3lame, which ffmpeg refuses to write into an
MP4 container at all ("Nothing was written into output file"), so the M4A button
failed outright for any file small enough to convert in process while the very same
media converted fine once it was large enough to be queued. The fallback was silent,
too: a target nobody knew was written as an MP3 under whatever extension was asked
for.

The other half is the delivery. A lossless target is large long before the source
is - about 10MB a minute for WAV, against a Bot API limit measured in tens of MB -
and the over-limit send failed *after* the encode with nothing said, which is what
left the button looking stuck. It now takes the road a large split part takes.
"""

import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_helpers import read_source  # noqa: E402

from tasks import conversion_tasks  # noqa: E402

#: What the audio-format picker offers, as its callback data spells it.
MENU_TARGETS = ("mp3", "wav", "aac", "flac", "ogg", "m4a")


# ─────────────────────────────────────────────────────────────────────────────
# The codec each target is written with
# ─────────────────────────────────────────────────────────────────────────────


def test_every_target_the_picker_offers_is_one_the_converter_can_encode():
    """The button and the table are one list, not two that happen to agree."""
    src = read_source("utils", "keyboard_utils.py")

    for target in MENU_TARGETS:
        assert f'callback_data="audio_{target}"' in src, f"the picker no longer offers {target}"
        assert target in conversion_tasks.AUDIO_FORMAT_CODECS, f"no encoder for the offered {target}"


def test_m4a_is_written_as_aac_because_ffmpeg_takes_nothing_else():
    """The target that was missing, and the failure its absence caused.

    ``m4a`` is AAC inside an MP4 container. With no entry the converter fell back
    to libmp3lame, and the ``ipod`` muxer rejects an MP3 stream outright - the run
    ended with "Nothing was written into output file, because at least one of its
    streams received no packets", i.e. the M4A button produced nothing.
    """
    args = conversion_tasks.audio_format_ffmpeg_args("m4a", "128k")

    assert args[:4] == ["-c:a", "aac", "-b:a", "128k"]
    assert "libmp3lame" not in args


def test_an_unknown_target_is_refused_rather_than_written_as_an_mp3(tmp_path):
    """The old fallback wrote an MP3 under whatever extension was asked for."""
    target = tmp_path / "out.xyz"

    assert conversion_tasks.audio_format_ffmpeg_args("xyz") is None

    ok, message = asyncio.run(conversion_tasks.convert_audio_format(str(tmp_path / "in.mp3"), str(target), "xyz"))

    assert ok is False
    assert "unsupported" in message
    assert not target.exists()


@pytest.mark.parametrize("target", ["wav", "flac"])
def test_a_target_with_no_bitrate_is_not_given_one(target):
    """PCM has no bitrate to set and flac refuses the option outright."""
    args = conversion_tasks.audio_format_ffmpeg_args(target)

    assert "-b:a" not in args
    assert args[0:2] == ["-c:a", conversion_tasks.AUDIO_FORMAT_CODECS[target]]


def test_the_tags_travel_with_the_codec():
    """The tags are part of the result: a converted file keeps the media's own.

    An MP3 is pinned to the ID3 version a properties panel reads (the same recipe
    the splitter and the bitrate change use), and every other target states the
    metadata copy instead of relying on an invisible default.
    """
    mp3 = conversion_tasks.audio_format_ffmpeg_args("mp3", "128k")
    assert mp3[-len(conversion_tasks.MP3_METADATA_ARGS) :] == list(conversion_tasks.MP3_METADATA_ARGS)

    for target in ("wav", "aac", "m4a", "flac", "ogg", "opus"):
        args = conversion_tasks.audio_format_ffmpeg_args(target, "128k")
        assert "-map_metadata" in args, target
        assert "-id3v2_version" not in args, target


# ─────────────────────────────────────────────────────────────────────────────
# One table, read by the in-process converter and the queued job alike
# ─────────────────────────────────────────────────────────────────────────────


def test_the_queued_job_and_the_in_process_converter_share_one_table():
    src = read_source("handlers.py")

    assert "from tasks.conversion_tasks import" in src
    assert "audio_format_ffmpeg_args" in src
    # The second table, and the copy fallback that hid a target nobody knew.
    assert '"-c:a", "libmp3lame", "-b:a", _DEFAULT_AUDIO_BITRATE' not in src
    assert '["-c:a", "copy"]' not in src


def test_a_small_file_and_a_large_one_are_encoded_at_the_same_bitrate():
    """The gate answers "already at this bitrate", so the encoder has to mean it too.

    The in-process converter mapped its quality default to 192k while the queued
    job encoded with another constant - one button, two answers. Both now read the
    one bitrate the settings menu chose, so the check and the encode agree.
    """
    src = read_source("handlers.py")

    assert "bitrate=audio_bitrate" in src
    assert "audio_format_ffmpeg_args(format_type, audio_bitrate)" in src


def test_every_offered_target_has_a_codec_a_probe_reports_and_a_bitrate_answer():
    """The "already in this format" check needs to know what a probe calls each target.

    ``libmp3lame`` is what ffmpeg writes with and ``mp3`` is what ffprobe reads
    back; ``libvorbis`` and ``vorbis`` are the same split. WAV and FLAC are named
    too, but their encoders take no bitrate - which is what keeps the check off
    the lossless targets.
    """
    for target in MENU_TARGETS:
        assert target in conversion_tasks.AUDIO_FORMAT_PROBE_CODECS, target

    assert conversion_tasks.AUDIO_FORMAT_PROBE_CODECS["mp3"] == "mp3"
    assert conversion_tasks.AUDIO_FORMAT_PROBE_CODECS["ogg"] == "vorbis"
    # m4a is AAC in an MP4 container, so both targets report one codec.
    assert conversion_tasks.AUDIO_FORMAT_PROBE_CODECS["m4a"] == "aac"
    assert conversion_tasks.AUDIO_FORMAT_PROBE_CODECS["aac"] == "aac"

    for target in ("mp3", "aac", "m4a", "ogg", "opus"):
        assert conversion_tasks.audio_format_takes_bitrate(target) is True, target
    for target in ("wav", "flac"):
        assert conversion_tasks.audio_format_takes_bitrate(target) is False, target


# ─────────────────────────────────────────────────────────────────────────────
# The delivery: a lossless result is bigger than the Bot API will carry
# ─────────────────────────────────────────────────────────────────────────────

#: Above the 1KB Bot API limit these tests configure, without writing 50MB.
OVER_LIMIT = 4096


class _Converter:
    """Writes the output the handler would have produced, and records the call."""

    def __init__(self, output_bytes: int):
        self.output_bytes = output_bytes
        self.calls: list[dict] = []

    async def convert_audio_format(self, input_path, output_path, *args, **kwargs):
        self.calls.append({"args": args, "kwargs": kwargs})
        with open(output_path, "wb") as fh:
            fh.write(b"x" * self.output_bytes)
        return True


class _Bot:
    def __init__(self):
        self.sent: list[dict] = []

    async def send_audio(self, **kwargs):
        self.sent.append(kwargs)
        return SimpleNamespace(audio=SimpleNamespace(file_id="file-id"))


def _run_conversion(monkeypatch, tmp_path, *, output_bytes=OVER_LIMIT, enable_userbot=True, direct_ok=True):
    import handlers as handlers_module

    source = tmp_path / "song.mp3"
    source.write_bytes(b"source")
    handler = object.__new__(handlers_module.EnhancedMediaHandler)
    converter = _Converter(output_bytes)
    bot = _Bot()
    edits: list[str] = []
    direct: list[dict] = []

    async def _yes(*_args, **_kwargs):
        return True

    async def _edit(_query, text, **_kwargs):
        edits.append(text)
        return True

    async def _send_direct(self, chat_id, file_path, caption, delivery_name, current_file, *, user_id=None):
        direct.append({"chat_id": chat_id, "name": delivery_name, "user_id": user_id, "path": file_path})
        return direct_ok

    handler._require_callback = _yes
    handler._check_conversion_quota = _yes
    handler.safe_edit = _edit
    handler.converter = converter

    # The bitrate now comes from /usersettings; pin it so the assertion below does
    # not depend on a settings file some other test may have written.
    monkeypatch.setattr(handlers_module, "_user_audio_bitrate", lambda _uid: "128k")

    monkeypatch.setattr(
        handlers_module,
        "config",
        SimpleNamespace(
            OUTPUT_PATH=str(tmp_path),
            TEMP_PATH=str(tmp_path),
            BOT_API_MAX_BYTES=1024,
            ENABLE_USERBOT=enable_userbot,
        ),
    )
    monkeypatch.setattr(handlers_module.EnhancedMediaHandler, "_send_audio_via_userbot", _send_direct)

    session = {"current_file": {"id": "abc", "name": "song.mp3", "type": "audio", "path": str(source)}}
    update = SimpleNamespace(
        callback_query=SimpleNamespace(message=SimpleNamespace()),
        effective_user=SimpleNamespace(id=7),
        effective_chat=SimpleNamespace(id=99),
    )
    asyncio.run(handler.convert_audio_format(update, SimpleNamespace(bot=bot), session, "wav"))

    return SimpleNamespace(edits=edits, direct=direct, sent=bot.sent, converter=converter)


def test_a_result_over_the_bot_api_limit_is_sent_over_mtproto(monkeypatch, tmp_path):
    """A WAV passes the Bot API limit at around five minutes of audio.

    Sending it anyway fails *after* the encode, and the failure used to be silent -
    the button simply stayed on "🔄 Converting to WAV…". The userbot carries it.
    """
    result = _run_conversion(monkeypatch, tmp_path)

    assert result.sent == [], "a file the Bot API cannot carry was sent to the Bot API anyway"
    assert len(result.direct) == 1
    assert result.direct[0]["user_id"] == 7
    assert result.direct[0]["name"] == "song.wav"
    assert any("sent directly" in text for text in result.edits), result.edits
    # The bitrate the menu promises travels to the encoder.
    assert result.converter.calls[0]["kwargs"] == {"bitrate": "128k"}


def test_an_over_limit_result_with_no_userbot_is_reported_instead_of_failing_silently(monkeypatch, tmp_path):
    result = _run_conversion(monkeypatch, tmp_path, enable_userbot=False)

    assert result.direct == []
    assert result.sent == []
    assert any("Bot API limit" in text for text in result.edits), result.edits


def test_a_result_the_bot_api_can_carry_still_goes_through_it(monkeypatch, tmp_path):
    result = _run_conversion(monkeypatch, tmp_path, output_bytes=16)

    assert result.direct == []
    assert len(result.sent) == 1
    assert result.sent[0]["filename"] == "song.wav"
