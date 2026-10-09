# media_converter.py
import asyncio
import contextlib
import logging
import os
import signal
import tempfile

import config

try:
    import ffmpeg
except ImportError:
    ffmpeg = None

try:
    from utils.process_utils import create_checked_subprocess_exec
except Exception:
    create_checked_subprocess_exec = None

try:
    from PIL import Image
except ImportError:
    Image = None

logger = logging.getLogger(__name__)


# Named explicitly rather than through ``signal.Signals``: Windows has no SIGKILL
# constant, so the enum lookup silently loses the one name that matters most (the
# OOM killer) exactly where the message is most useful.
_SIGNAL_NAMES = {
    1: "SIGHUP",
    2: "SIGINT",
    6: "SIGABRT",
    9: "SIGKILL",
    11: "SIGSEGV",
    13: "SIGPIPE",
    15: "SIGTERM",
}


def _signal_name(number: int) -> str:
    """Name the signal that killed a subprocess ("signal 9 (SIGKILL)").

    ``returncode`` is negative exactly when a signal terminated the child, and
    the number alone tells a user nothing - SIGKILL (the OOM killer) versus
    SIGTERM (a shutdown) is the difference between "make it smaller" and "retry".
    """
    name = _SIGNAL_NAMES.get(number)
    if name is None:
        with contextlib.suppress(ValueError, AttributeError):
            name = signal.Signals(number).name
    return f"signal {number} ({name})" if name else f"signal {number}"


class ExtendedMediaConverter:
    """Extended converter with all features from FFmpeg commands."""

    def __init__(self):
        # The last ffmpeg failure's own words, so a caller that only gets a bool
        # back can still tell the user *why* (a killed process says so).
        self.last_ffmpeg_failure = ""
        self.supported_formats = {
            "video": [".mp4", ".avi", ".mov", ".mkv", ".flv", ".wmv", ".m4v", ".3gp", ".webm"],
            "audio": [".mp3", ".wav", ".aac", ".flac", ".ogg", ".m4a", ".wma", ".opus"],
            "subtitle": [".srt", ".ass", ".ssa", ".vtt"],
        }

    async def execute_ffmpeg(self, cmd: list[str], input_path: str = None, output_path: str = None) -> tuple[bool, str]:
        """Execute FFmpeg command with proper error handling."""
        try:
            # Build command
            # Use configured ffmpeg binary and reduce verbose output
            ffmpeg_bin = getattr(config, "FFMPEG_PATH", "ffmpeg") or "ffmpeg"
            full_cmd = [ffmpeg_bin, "-y", "-hide_banner", "-loglevel", "error"]
            if input_path:
                full_cmd.extend(["-i", input_path])
            full_cmd.extend(cmd)
            if output_path:
                full_cmd.append(output_path)

            logger.info(f"Executing: {' '.join(full_cmd)}")

            # Run process
            if create_checked_subprocess_exec is not None:
                process = await create_checked_subprocess_exec(
                    *full_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
            else:
                process = await asyncio.create_subprocess_exec(
                    *full_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )

            _, stderr = await process.communicate()

            if process.returncode == 0:
                self.last_ffmpeg_failure = ""
                return True, "Success"
            error_msg = stderr.decode("utf-8", errors="ignore")[:500]
            # ffmpeg's own words are what identify a failure (a source with no
            # stream to encode, an unwritable output, a process the platform
            # killed). They used to be returned to a caller that dropped them, so
            # a real failure left nothing but "❌ Failed to …" in the chat and
            # nothing at all in the log. The exit code tells the two kinds apart:
            # a normal error is 1, a signal (killed) is negative.
            if process.returncode < 0:
                # A negative code is not an ffmpeg error at all: something killed
                # the process (SIGKILL 9 is almost always the host OOM killer on a
                # long encode). ffmpeg writes nothing to stderr in that case, so
                # the reason has to be derived from the signal itself.
                error_msg = (
                    f"ffmpeg was killed by {_signal_name(-process.returncode)} - most often the "
                    "host's out-of-memory killer on a long or high-resolution encode "
                    "(see FFMPEG_THREADS / FFMPEG_FILTER_THREADS)"
                )
            logger.error(
                "FFmpeg failed (exit=%s): %s\n%s",
                process.returncode,
                " ".join(str(part) for part in full_cmd),
                error_msg.strip() or "(ffmpeg wrote nothing to stderr)",
            )
            self.last_ffmpeg_failure = error_msg.strip()
            return False, error_msg

        except Exception as e:
            logger.error(f"FFmpeg execution error: {e}")
            self.last_ffmpeg_failure = str(e)
            return False, str(e)

    # ========== VIDEO FEATURES ==========

    async def convert_video_format(self, input_path: str, output_path: str, target_format: str = "mp4") -> bool:
        """Convert video to different format with proper codec selection.

        Every H.264/AAC target states ``-preset veryfast -crf 23`` explicitly:
        without a preset libx264 silently runs its ``medium`` default, which is
        several times slower and peaks higher in RAM. An MP4-family target whose
        source codecs already fit is remuxed (stream copy) instead, so converting
        an MKV whose streams are H.264/AAC costs seconds rather than an encode.
        """
        try:
            _h264_aac = [
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "23",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
            ]
            format_configs = {
                "mp4": list(_h264_aac),
                "mkv": list(_h264_aac),
                "avi": ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "mp3"],
                "mov": list(_h264_aac),
                "webm": ["-c:v", "libvpx-vp9", "-c:a", "libvorbis"],
                "flv": ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac"],
                "m4v": list(_h264_aac),
            }

            if target_format not in format_configs:
                logger.error(f"Unsupported video format: {target_format}")
                return False

            cmd = format_configs[target_format]
            # A container change only: copy the streams when they already fit MP4.
            try:
                from utils.ffmpeg_runner import MP4_REMUX_TARGET_EXTS, mp4_remux_args, probe_media

                if os.path.splitext(output_path)[1].lower() in MP4_REMUX_TARGET_EXTS:
                    remux = mp4_remux_args(await probe_media(input_path))
                    if remux is not None:
                        logger.info(
                            "convert_video_format: %s streams are MP4-compatible; remuxing instead of re-encoding",
                            input_path,
                        )
                        cmd = remux
            except Exception:
                logger.debug("convert_video_format: remux probe failed; re-encoding")
            return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

        except Exception as e:
            logger.error(f"Video format conversion error: {e}")
            return False

    async def change_resolution(self, input_path: str, output_path: str, width: int, height: int) -> bool:
        """Change video resolution.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``.
        """
        from tasks.conversion_tasks import change_resolution as _change_res

        success, _ = await _change_res(input_path, output_path, width, height)
        return success

    async def change_framerate(self, input_path: str, output_path: str, fps: float) -> bool:
        """Change video framerate.

        ``-preset veryfast -crf 23`` is stated so the re-encode does not silently
        fall back to libx264's ``medium`` default.
        """
        cmd = ["-r", str(fps), "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "copy"]
        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

    async def adjust_bitrate(self, input_path: str, output_path: str, video_bitrate: str, audio_bitrate: str) -> bool:
        """Adjust video and audio bitrate.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``.
        """
        from tasks.conversion_tasks import adjust_bitrate as _adj_bitrate

        success, _ = await _adj_bitrate(input_path, output_path, video_bitrate, audio_bitrate)
        return success

    async def optimize_video(self, input_path: str, output_path: str, preset: str = "slow", crf: int = 23) -> bool:
        """Optimize video for web/streaming.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``.
        """
        from tasks.conversion_tasks import optimize_video as _opt_video

        success, _ = await _opt_video(input_path, output_path, preset=preset, crf=crf)
        return success

    async def extract_audio_from_video(
        self, input_path: str, output_path: str, fmt: str = "mp3", bitrate: str = "192k"
    ) -> bool:
        """Extract audio from video.

        Delegates to the canonical implementation in ``tasks.conversion_tasks.extract_audio``.
        """
        from tasks.conversion_tasks import extract_audio as _extract_audio

        success, _ = await _extract_audio(input_path, output_path, format=fmt, bitrate=bitrate)
        return success

    async def remove_audio(self, input_path: str, output_path: str) -> bool:
        """Remove audio from video."""
        cmd = ["-an", "-c:v", "copy"]  # No audio
        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

    async def merge_audio_video(self, video_path: str, audio_path: str, output_path: str) -> bool:
        """Merge audio and video tracks."""
        # Use complex filter for merging
        cmd = [
            "-i",
            video_path,
            "-i",
            audio_path,
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            "-strict",
            "experimental",
            "-shortest",
        ]
        return (await self.execute_ffmpeg(cmd, None, output_path))[0]

    async def merge_videos(self, video_paths: list[str], output_path: str) -> bool:
        """Merge multiple videos into one.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``.
        """
        from tasks.conversion_tasks import merge_videos as _merge_vids

        success, _ = await _merge_vids(video_paths, output_path)
        return success

    async def merge_audios(self, audio_paths: list[str], output_path: str) -> bool:
        """Merge multiple audio files.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``.
        """
        from tasks.conversion_tasks import merge_audios as _merge_auds

        success, _ = await _merge_auds(audio_paths, output_path)
        return success

    async def split_video(
        self, input_path: str, output_dir: str, segment_seconds: float = 3600, *, ext: str = ".mp4", stem: str = "part"
    ) -> list[str]:
        """Split a video or an audio file into numbered parts.

        Delegates to the canonical splitter (``tasks.conversion_tasks.split_media_segments``),
        which is the one implementation of the ``-f segment`` copy: a second copy
        here is how this method came to take arguments its callers never passed.
        Parts are named ``<stem>.001<ext>``, ``<stem>.002<ext>``, ...
        """
        from tasks.conversion_tasks import split_media_segments

        ok, parts, _message = await split_media_segments(input_path, output_dir, segment_seconds, ext=ext, stem=stem)
        return parts if ok else []

    async def split_video_range(self, input_path: str, start: float, end: float, output_path: str) -> bool:
        """Split a single range from video between start and end (seconds).

        Uses ffmpeg with -ss and -to (or -t) to cut the segment.
        """
        try:
            # Use -ss before -i for faster seeking then -to relative to the start
            # Build cmd such that execute_ffmpeg appends the output_path
            duration = end - start
            # Use precise seeking: -ss START -t DURATION -c copy
            cmd = ["-ss", str(start), "-t", str(duration), "-c", "copy"]
            return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]
        except Exception as e:
            logger.error(f"split_video_range error: {e}")
            return False

    async def trim_video(self, input_path: str, output_path: str, start_time: str, end_time: str) -> bool:
        """Trim a segment from input between `start_time` and `end_time`.

        Delegates to the canonical implementation in ``tasks.conversion_tasks.trim_media``.
        """
        from tasks.conversion_tasks import trim_media as _trim

        success, _ = await _trim(input_path, output_path, start_time, end_time)
        return success

    async def burn_subtitles(self, input_path: str, subtitle_path: str, output_path: str) -> bool:
        """Hardcode (burn) subtitles into the video using ffmpeg subtitles filter.

        The result is a single MP4 with the subtitles painted into the frames, so
        any player shows them because there is no subtitle stream left to select.
        The audio is re-encoded (not copied) because the MP4 muxer will not carry
        some source codecs, and ``+faststart`` moves the index to the front so the
        file streams while it downloads.

        The encoder settings are the deployment's own memory-constrained defaults
        (see ``utils.ffmpeg_runner``): ``veryfast`` x264 keeps RAM down on a small
        host, and the optional ``FFMPEG_MAXRATE``/``FFMPEG_BUFSIZE`` caps stop CRF
        from inflating the file size, which matters on a metered plan.

        Note: This requires ffmpeg built with libass or the subtitles filter
        available. The filter parses its own argument, so a Windows drive colon or
        an apostrophe in the path is escaped before it is handed over - otherwise
        ffmpeg reads the path as filter syntax and the burn fails ("No option name
        near ...").
        """
        try:
            abs_sub = os.path.abspath(subtitle_path).replace("\\", "/")
            filter_path = abs_sub.replace(":", "\\:").replace("'", "\\'")
            # ── Memory-bounded encode ──
            # libavfilter and x264 both size their worker pools by core count, and
            # on a small host that auto-threading is what drives the encode into
            # the OOM killer on a long 1080p source - the process dies with exit
            # -9 a few seconds in, which is what "FFmpeg failed (exit=-9)" with no
            # stderr means. Both pools are capped here; set either env to "0" to
            # hand the choice back to ffmpeg.
            filter_threads = os.getenv("FFMPEG_FILTER_THREADS", "1").strip()
            encoder_threads = os.getenv("FFMPEG_THREADS", "2").strip()
            cmd = []
            if filter_threads not in ("", "0"):
                cmd.extend(["-filter_threads", filter_threads, "-filter_complex_threads", filter_threads])
            cmd.extend(
                [
                    "-vf",
                    f"subtitles=filename='{filter_path}'",
                    "-c:v",
                    "libx264",
                    "-preset",
                    "veryfast",
                    "-crf",
                    "23",
                    "-c:a",
                    "aac",
                    "-b:a",
                    "128k",
                    "-movflags",
                    "+faststart",
                ]
            )
            if encoder_threads not in ("", "0"):
                cmd.extend(["-threads", encoder_threads])
            maxrate = os.getenv("FFMPEG_MAXRATE", "2M")
            if maxrate.strip().lower() not in ("0", "unlimited", "none", ""):
                cmd.extend(["-maxrate", maxrate, "-bufsize", os.getenv("FFMPEG_BUFSIZE", "4M")])
            return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]
        except Exception as e:
            logger.error(f"burn_subtitles error: {e}")
            return False

    async def extract_subtitles(self, input_path: str, output_path: str) -> bool:
        """Extract subtitles from video.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``.
        """
        from tasks.conversion_tasks import extract_subtitles as _extract_subs

        success, _ = await _extract_subs(input_path, output_path)
        return success

    async def add_subtitles(self, video_path: str, subtitle_path: str, output_path: str) -> bool:
        """Mux a subtitle file into the video as a soft (selectable) track.

        The subtitle codec is fixed by the output container - ``mov_text`` is the
        MP4/MOV answer, ``srt`` the Matroska one - so the same call works whether
        the media arrived as ``.mp4`` or ``.mkv``; asking for ``mov_text`` in an
        MKV is how the merge silently produced nothing. The streams are mapped
        explicitly (video and audio from the media, the subtitle from its own
        file) so a source with several tracks cannot make ffmpeg pick the wrong
        one, and the ``?`` on the audio keeps a silent video working.
        """
        ext = os.path.splitext(output_path)[1].lower()
        sub_codec = "mov_text" if ext in (".mp4", ".m4v", ".mov") else "srt"
        cmd = [
            "-i",
            video_path,
            "-i",
            subtitle_path,
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-map",
            "1:0",
            "-c:v",
            "copy",
            "-c:a",
            "copy",
            "-c:s",
            sub_codec,
            "-metadata:s:s:0",
            "language=eng",
            "-disposition:s:0",
            "default",
        ]
        if sub_codec == "mov_text":
            # MP4/MOV only: put the index in front so the file streams while it
            # downloads. The matroska muxer has no ``movflags``.
            cmd.extend(["-movflags", "+faststart"])
        return (await self.execute_ffmpeg(cmd, None, output_path))[0]

    async def extract_streams(self, input_path: str, output_dir: str) -> dict[str, str]:
        """Extract all streams (video, audio, subtitles).

        Delegates to the canonical implementation in ``tasks.conversion_tasks``.
        """
        from tasks.conversion_tasks import extract_streams as _extract_streams

        _, extracted = await _extract_streams(input_path, output_dir)
        return extracted

    async def repair_video(self, input_path: str, output_path: str) -> bool:
        """Attempt to repair corrupted video.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``.
        """
        from tasks.conversion_tasks import repair_video as _repair

        success, _ = await _repair(input_path, output_path)
        return success

    async def take_screenshot_at_time(self, input_path: str, output_path: str, time: str = "00:00:01") -> bool:
        """Take screenshot at specific time.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``.
        """
        from tasks.conversion_tasks import take_screenshot as _screenshot

        success, _ = await _screenshot(input_path, output_path, time=time)
        return success

    async def take_screenshot_grid(self, input_path: str, output_dir: str, count: int = 9) -> list[str]:
        """Take multiple screenshots at intervals."""
        # Get video duration
        probe = ffmpeg.probe(input_path)
        duration = float(probe["format"]["duration"])

        interval = duration / (count + 1)
        screenshots = []

        for i in range(1, count + 1):
            time_sec = interval * i
            time_str = f"{int(time_sec // 3600):02d}:{int((time_sec % 3600) // 60):02d}:{time_sec % 60:06.3f}"
            output = os.path.join(output_dir, f"screenshot_{i:02d}.jpg")

            success = await self.take_screenshot_at_time(input_path, output, time_str)
            if success:
                screenshots.append(output)

        return screenshots

    async def generate_sample(self, input_path: str, output_path: str, duration: int = 30) -> bool:
        """Generate sample/preview of video.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``.
        """
        from tasks.conversion_tasks import generate_sample as _gen_sample

        success, _ = await _gen_sample(input_path, output_path, duration)
        return success

    async def create_archive(self, file_paths: list[str], output_path: str) -> bool:
        """Create ZIP archive of files.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``.
        """
        from tasks.conversion_tasks import create_archive as _create_archive

        success, _ = await _create_archive(file_paths, output_path)
        return success

    async def edit_metadata(self, input_path: str, output_path: str, metadata: dict[str, str]) -> bool:
        """Edit video metadata.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``
        so there is a single source of truth for this operation.
        """
        from tasks.conversion_tasks import edit_metadata as _edit_meta

        success, _ = await _edit_meta(input_path, output_path, metadata)
        return success

    async def convert_audio_format(
        self,
        input_path: str,
        output_path: str,
        target_format: str = "mp3",
        quality: int = 2,
        bitrate: str | None = None,
    ) -> bool:
        """Convert audio between formats.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``,
        which owns the codec each target is written with, so this and the worker job
        for a larger source encode the same button the same way.

        *bitrate*, when given, is used as-is; otherwise the ``quality`` parameter is
        mapped to an approximate CBR bitrate. The Convert Format button states its
        own (``_DEFAULT_AUDIO_BITRATE``), because the bitrate the menu promises and
        the one the queued job encodes at have to be the same number - they were not
        while this mapped every call to 192k.
        """
        from tasks.conversion_tasks import convert_audio_format as _convert_audio

        # Map VBR quality (0-9, where 0=best) to approximate CBR bitrate
        _bitrate_map = {
            0: "320k",
            1: "256k",
            2: "192k",
            3: "160k",
            4: "128k",
            5: "96k",
            6: "80k",
            7: "64k",
            8: "48k",
            9: "32k",
        }
        bitrate = bitrate or _bitrate_map.get(quality, "192k")

        success, _ = await _convert_audio(input_path, output_path, target_format=target_format, bitrate=bitrate)
        return success

    async def screen_record(
        self, output_path: str, duration: int = 10, resolution: str = "1280x720", fps: int = 30
    ) -> bool:
        """Screen recording (simplified - requires platform-specific tools)."""
        # Note: This is a simplified version. Actual screen recording requires
        # platform-specific tools (gdigrab on Windows, x11grab on Linux, avfoundation on macOS)
        logger.warning("Screen recording requires platform-specific setup")
        return False

    async def extract_thumbnail_grid(self, input_path: str, output_path: str, rows: int = 3, cols: int = 3) -> bool:
        """Create thumbnail grid from video using PIL compositing."""
        import shutil

        # Get video duration for spacing
        try:
            probe = ffmpeg.probe(input_path)
            duration = float(probe["format"]["duration"])
        except Exception as e:
            logger.error("extract_thumbnail_grid: probe failed for %s: %s", input_path, e)
            return False

        total = rows * cols
        if total <= 0:
            return False

        # Create temporary screenshots
        temp_dir = tempfile.mkdtemp()
        screenshots = []

        try:
            # Take screenshots at evenly-spaced intervals (skip first and last)
            for i in range(total):
                time_sec = (duration * (i + 1)) / (total + 1)
                time_str = f"{int(time_sec // 3600):02d}:{int((time_sec % 3600) // 60):02d}:{time_sec % 60:06.3f}"
                temp_file = os.path.join(temp_dir, f"temp_{i:02d}.jpg")

                if await self.take_screenshot_at_time(input_path, temp_file, time_str):
                    screenshots.append(temp_file)

            if len(screenshots) == 0:
                logger.warning("extract_thumbnail_grid: no screenshots captured")
                return False

            # Compose into grid using PIL if available
            if Image is not None:
                imgs = [Image.open(p) for p in screenshots]
                # Resize all to the same dimensions (use first image size as reference)
                cell_w, cell_h = imgs[0].size
                imgs = [img.resize((cell_w, cell_h), Image.LANCZOS) for img in imgs]

                # Pad to full grid if some screenshots failed
                while len(imgs) < total:
                    imgs.append(Image.new("RGB", (cell_w, cell_h), (0, 0, 0)))

                grid_w = cell_w * cols
                grid_h = cell_h * rows
                grid = Image.new("RGB", (grid_w, grid_h), (0, 0, 0))

                for idx, img in enumerate(imgs[:total]):
                    r = idx // cols
                    c = idx % cols
                    grid.paste(img, (c * cell_w, r * cell_h))

                grid.save(output_path, quality=90)
            else:
                # Fallback: just copy the best available screenshot
                shutil.copy(screenshots[0], output_path)

            return True
        except Exception as e:
            logger.error("extract_thumbnail_grid failed: %s", e)
            return False
        finally:
            # Cleanup temp files
            try:
                import shutil

                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                logger.debug("Failed to generate thumbnail from video", exc_info=True)

    async def apply_fade(
        self, input_path: str, output_path: str, fade_in_duration: float = 0.0, fade_out_duration: float = 0.0
    ) -> bool:
        """Apply fade-in and/or fade-out to the audio track of a media file.

        Delegates to ``tasks.conversion_tasks.apply_fade`` - the function the
        worker's ``fade`` job type runs - so an inline fade and a cached repeat
        build the same filter chain from the same duration probe. The local copy
        of this used to probe the audio stream and fall back to a raw ffprobe,
        which could place a fade-out at a slightly different point than the
        worker's did for the very same file.
        """
        from tasks.conversion_tasks import apply_fade as _apply_fade

        success, _ = await _apply_fade(input_path, output_path, fade_in_duration, fade_out_duration)
        return success
