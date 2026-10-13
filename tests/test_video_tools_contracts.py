"""Video-tool outputs retain source identity and map only the intended tracks."""

import asyncio
import json
import os
from types import SimpleNamespace

from handlers import EnhancedMediaHandler, _source_metadata_ffmpeg_args, _video_delivery_name
from media_converter import ExtendedMediaConverter
from tasks import conversion_tasks
from utils import ffmpeg_runner
from utils.keyboard_utils import MediaMenuBuilder


def test_video_tool_delivery_name_keeps_the_source_stem_and_output_extension():
    assert (
        _video_delivery_name(
            {"name": "Course Lesson 01.mkv"},
            os.path.join("storage", "output", "opaque-id.mp4"),
        )
        == "Course Lesson 01.mp4"
    )


def test_video_tool_ffmpeg_args_retain_source_tags_and_chapters():
    assert _source_metadata_ffmpeg_args({"_source_metadata": {"title": "Lesson 01", "performer": "Instructor"}}) == [
        "-map_metadata",
        "0",
        "-map_chapters",
        "0",
        "-metadata",
        "title=Lesson 01",
        "-metadata",
        "artist=Instructor",
    ]


def test_video_tool_tags_do_not_request_unsupported_target_chapters():
    assert _source_metadata_ffmpeg_args(
        {"_source_metadata": {"title": "Lesson 01"}},
        include_chapters=False,
    ) == ["-map_metadata", "0", "-metadata", "title=Lesson 01"]


def test_add_audio_mixes_existing_and_selected_audio_and_keeps_video_duration(monkeypatch):
    converter = ExtendedMediaConverter()
    captured = {}

    async def execute_ffmpeg(args, input_path, output_path):
        captured.update(args=args, input_path=input_path, output_path=output_path)
        return True, "Success"

    async def probe_media(_path):
        return {"video_streams": 1, "audio_streams": 1}

    monkeypatch.setattr(converter, "execute_ffmpeg", execute_ffmpeg)
    monkeypatch.setattr(ffmpeg_runner, "probe_media", probe_media)
    result = asyncio.run(
        converter.merge_audio_video(
            "source-video.mkv",
            "selected-audio.m4a",
            "Lesson 01.mp4",
            video_file={"_source_metadata": {"title": "Lesson 01", "performer": "Instructor"}},
        )
    )

    assert result is True
    assert captured["input_path"] is None
    assert captured["args"][:4] == ["-i", "source-video.mkv", "-i", "selected-audio.m4a"]
    assert "[0:a:0][1:a:0]amix=inputs=2:duration=longest:dropout_transition=0,apad[aout]" in captured["args"]
    assert captured["args"][captured["args"].index("-map") + 1] == "0:v:0"
    assert captured["args"][captured["args"].index("-map", captured["args"].index("-map") + 1) + 1] == "[aout]"
    assert "-shortest" in captured["args"]
    assert captured["args"][captured["args"].index("-map_metadata") :][:2] == ["-map_metadata", "0"]
    assert "title=Lesson 01" in captured["args"]


def test_add_audio_handles_a_video_without_existing_audio(monkeypatch):
    converter = ExtendedMediaConverter()
    captured = {}

    async def execute_ffmpeg(args, input_path, output_path):
        captured["args"] = args
        return True, "Success"

    async def probe_media(_path):
        return {"video_streams": 1, "audio_streams": 0}

    monkeypatch.setattr(converter, "execute_ffmpeg", execute_ffmpeg)
    monkeypatch.setattr(ffmpeg_runner, "probe_media", probe_media)
    assert asyncio.run(converter.merge_audio_video("silent.mkv", "music.m4a", "out.mp4")) is True
    assert captured["args"][captured["args"].index("-map") + 1] == "0:v:0"
    assert captured["args"][captured["args"].index("-map", captured["args"].index("-map") + 1) + 1] == "1:a:0"
    assert captured["args"][captured["args"].index("-af") + 1] == "apad"
    assert "-shortest" in captured["args"]


def test_extraction_maps_absolute_stream_indexes_and_names_outputs_from_source(monkeypatch, tmp_path):
    input_path = tmp_path / "source.mkv"
    input_path.write_bytes(b"input")
    output_dir = tmp_path / "tracks"
    output_dir.mkdir()
    streams = [
        {"index": 4, "codec_type": "video"},
        {"index": 8, "codec_type": "audio"},
        {"index": 13, "codec_type": "subtitle"},
        {"index": 14, "codec_type": "data"},
    ]
    commands = []

    class FakeProcess:
        returncode = 0

        def __init__(self, output=None):
            self.output = output

        async def communicate(self):
            if self.output:
                with open(self.output, "wb") as output:
                    output.write(b"track")
            return (json.dumps({"streams": streams}).encode() if self.output is None else b"", b"")

    async def fake_spawn(*command, **_kwargs):
        commands.append(list(command))
        if command[0] == "ffprobe":
            return FakeProcess()
        return FakeProcess(command[-1])

    monkeypatch.setattr(conversion_tasks, "_validate_input_file", lambda _path: (True, ""))
    monkeypatch.setattr(conversion_tasks, "_spawn_process", fake_spawn)

    success, extracted = asyncio.run(
        conversion_tasks.extract_streams(
            str(input_path),
            str(output_dir),
            source_stem="Lesson: 01",
        )
    )

    assert success is True
    assert set(extracted) == {"video_4", "audio_8", "subtitle_13"}
    assert all(os.path.basename(path).startswith("Lesson_ 01_") for path in extracted.values())
    ffmpeg_commands = [command for command in commands if command[0] == "ffmpeg"]
    assert ffmpeg_commands[0][ffmpeg_commands[0].index("-map") :][:2] == ["-map", "0:4"]
    assert ffmpeg_commands[1][ffmpeg_commands[1].index("-map") :][:2] == ["-map", "0:8"]
    assert ffmpeg_commands[2][ffmpeg_commands[2].index("-map") :][:2] == ["-map", "0:13"]


def test_extraction_picker_describes_supported_media_tracks():
    keyboard = MediaMenuBuilder.get_extraction_menu()
    labels = [button.text for row in keyboard.inline_keyboard for button in row]
    assert "📦 Media Tracks" in labels
    assert "📦 All Streams" not in labels


def test_custom_resolution_and_framerate_inputs_enforce_quota(monkeypatch):
    quota_checks = []

    async def run_denied_input(awaiting_key, user_input):
        handler = object.__new__(EnhancedMediaHandler)
        handler.user_sessions = {7: {"current_file": {"type": "video"}}}

        async def settings_prompt(*_args):
            return False

        async def quota_check(*_args):
            quota_checks.append(awaiting_key)
            return False

        monkeypatch.setattr(handler, "_handle_settings_prompt", settings_prompt)
        monkeypatch.setattr(handler, "_check_conversion_quota", quota_check)
        replies = []

        async def reply_text(text, **_kwargs):
            replies.append(text)

        update = SimpleNamespace(
            message=SimpleNamespace(text=user_input, reply_text=reply_text),
            effective_user=SimpleNamespace(id=7),
        )
        context = SimpleNamespace(user_data={awaiting_key: True})
        await handler.handle_custom_input(update, context)
        return context.user_data, replies

    resolution_data, resolution_replies = asyncio.run(run_denied_input("awaiting_custom_resolution", "1280x720"))
    fps_data, fps_replies = asyncio.run(run_denied_input("awaiting_framerate", "23.976"))

    assert "awaiting_custom_resolution" not in resolution_data
    assert "awaiting_framerate" not in fps_data
    assert quota_checks == ["awaiting_custom_resolution", "awaiting_framerate"]
