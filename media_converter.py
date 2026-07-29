# media_converter.py
import asyncio
import logging
import os
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


class ExtendedMediaConverter:
    """Extended converter with all features from FFmpeg commands."""

    def __init__(self):
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
                return True, "Success"
            else:
                error_msg = stderr.decode("utf-8", errors="ignore")[:500]
                return False, error_msg

        except Exception as e:
            logger.error(f"FFmpeg execution error: {e}")
            return False, str(e)

    # ========== VIDEO FEATURES ==========

    async def convert_video_format(self, input_path: str, output_path: str, target_format: str = "mp4") -> bool:
        """Convert video to different format with proper codec selection."""
        try:
            format_configs = {
                "mp4": ["-c:v", "libx264", "-c:a", "aac", "-strict", "experimental"],
                "mkv": ["-c:v", "libx264", "-c:a", "aac"],
                "avi": ["-c:v", "libx264", "-c:a", "mp3"],
                "mov": ["-c:v", "libx264", "-c:a", "aac"],
                "webm": ["-c:v", "libvpx-vp9", "-c:a", "libvorbis"],
                "flv": ["-c:v", "libx264", "-c:a", "aac"],
                "m4v": ["-c:v", "libx264", "-c:a", "aac", "-strict", "experimental"],
            }

            if target_format not in format_configs:
                logger.error(f"Unsupported video format: {target_format}")
                return False

            cmd = format_configs[target_format]
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
        """Change video framerate."""
        cmd = ["-r", str(fps), "-c:v", "libx264", "-c:a", "copy"]
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

    async def split_video(self, input_path: str, output_pattern: str, segment_time: str = "01:00:00") -> list[str]:
        """Split video into segments."""
        cmd = ["-c", "copy", "-map", "0", "-segment_time", segment_time, "-f", "segment", "-reset_timestamps", "1"]
        success = (await self.execute_ffmpeg(cmd, input_path, output_pattern))[0]

        if success:
            # Find generated files
            base_dir = os.path.dirname(output_pattern)
            prefix = os.path.basename(output_pattern).split("%03d")[0]
            return sorted([f for f in os.listdir(base_dir) if f.startswith(prefix)])
        return []

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

        Note: This requires ffmpeg built with libass or the subtitles filter available.
        """
        try:
            # Use vf subtitles filter; need to ensure subtitle_path is an absolute path
            abs_sub = os.path.abspath(subtitle_path)
            cmd = ["-vf", f"subtitles={abs_sub}", "-c:v", "libx264", "-c:a", "copy"]
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
        """Add subtitles to video."""
        cmd = [
            "-i",
            video_path,
            "-i",
            subtitle_path,
            "-c:v",
            "copy",
            "-c:a",
            "copy",
            "-c:s",
            "mov_text",
            "-metadata:s:s:0",
            "language=eng",
        ]
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
        self, input_path: str, output_path: str, target_format: str = "mp3", quality: int = 2
    ) -> bool:
        """Convert audio between formats.

        Delegates to the canonical implementation in ``tasks.conversion_tasks``.
        Maps the ``quality`` parameter to an approximate CBR bitrate.
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
        bitrate = _bitrate_map.get(quality, "192k")

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
        """Apply fade-in and/or fade-out to audio track of a media file.

        Uses the afade audio filter. Both durations are in seconds.
        At least one must be > 0.
        """
        if fade_in_duration <= 0 and fade_out_duration <= 0:
            logger.warning("apply_fade: both durations are zero, nothing to do")
            return False

        # Build the audio filter chain
        afilters = []
        if fade_in_duration > 0:
            afilters.append(f"afade=t=in:st=0:d={fade_in_duration}")

        if fade_out_duration > 0:
            # Probe the file for audio duration
            audio_duration = 0.0
            try:
                if ffmpeg is not None:
                    probe = ffmpeg.probe(input_path)
                    audio_stream = next(
                        (s for s in probe.get("streams", []) if s.get("codec_type") == "audio"),
                        None,
                    )
                    if audio_stream is not None:
                        audio_duration = float(audio_stream.get("duration", probe.get("format", {}).get("duration", 0)))
                    else:
                        audio_duration = float(probe.get("format", {}).get("duration", 0))
            except Exception as e:
                logger.error("apply_fade: probe failed: %s", e)

            # Fallback: use ffprobe directly
            if audio_duration <= 0:
                try:
                    import asyncio as _aio

                    proc = await _aio.create_subprocess_exec(
                        "ffprobe",
                        "-v",
                        "error",
                        "-show_entries",
                        "format=duration",
                        "-of",
                        "default=noprint_wrappers=1:nokey=1",
                        input_path,
                        stdout=_aio.subprocess.PIPE,
                        stderr=_aio.subprocess.PIPE,
                    )
                    stdout, _ = await proc.communicate()
                    audio_duration = float(stdout.decode().strip()) if stdout else 0.0
                except Exception:
                    logger.debug("ffprobe fallback failed for audio duration")

            if audio_duration <= 0:
                logger.error("apply_fade: cannot determine audio duration")
                return False

            fade_out_start = max(0.0, audio_duration - fade_out_duration)
            afilters.append(f"afade=t=out:st={fade_out_start}:d={fade_out_duration}")

        filter_str = ",".join(afilters)
        cmd = ["-af", filter_str, "-c:v", "copy"]
        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]
