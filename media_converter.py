# media_converter.py
import asyncio
import logging
import os
import tempfile
import zipfile
from typing import Dict, List, Tuple

import config

# Optional imports
try:
    import aiofiles  # noqa: F401
except ImportError:
    aiofiles = None

try:
    import ffmpeg  # noqa: F401
except ImportError:
    ffmpeg = None

try:
    from utils.process_utils import create_checked_subprocess_exec
except Exception:
    create_checked_subprocess_exec = None

try:
    from PIL import Image  # noqa: F401
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

    async def execute_ffmpeg(self, cmd: List[str], input_path: str = None, output_path: str = None) -> Tuple[bool, str]:
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
        """Change video resolution."""
        cmd = ["-filter:v", f"scale={width}:{height}", "-c:a", "copy"]
        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

    async def change_framerate(self, input_path: str, output_path: str, fps: float) -> bool:
        """Change video framerate."""
        cmd = ["-r", str(fps), "-c:v", "libx264", "-c:a", "copy"]
        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

    async def adjust_bitrate(self, input_path: str, output_path: str, video_bitrate: str, audio_bitrate: str) -> bool:
        """Adjust video and audio bitrate."""
        cmd = ["-b:v", video_bitrate, "-b:a", audio_bitrate, "-c:v", "libx264", "-c:a", "aac"]
        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

    async def optimize_video(self, input_path: str, output_path: str, preset: str = "slow", crf: int = 23) -> bool:
        """Optimize video for web/streaming."""
        cmd = [
            "-c:v",
            "libx264",
            "-preset",
            preset,
            "-crf",
            str(crf),
            "-movflags",
            "+faststart",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
        ]
        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

    async def extract_audio_from_video(
        self, input_path: str, output_path: str, fmt: str = "mp3", bitrate: str = "192k"
    ) -> bool:
        """Extract audio from video."""
        cmd = [
            "-vn",  # No video
            "-acodec",
            "libmp3lame" if fmt == "mp3" else "copy",
            "-ab",
            bitrate,
            "-ar",
            "48000",
            "-ac",
            "2",
        ]
        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

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

    async def merge_videos(self, video_paths: List[str], output_path: str) -> bool:
        """Merge multiple videos into one."""
        # Create concat file
        concat_content = "\n".join([f"file '{path}'" for path in video_paths])
        concat_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt")
        concat_file.write(concat_content)
        concat_file.close()

        try:
            cmd = ["-f", "concat", "-safe", "0", "-i", concat_file.name, "-c", "copy"]
            success = (await self.execute_ffmpeg(cmd, None, output_path))[0]
            os.unlink(concat_file.name)
            return success
        except Exception:
            os.unlink(concat_file.name)
            return False

    async def merge_audios(self, audio_paths: List[str], output_path: str) -> bool:
        """Merge multiple audio files."""
        # Create input string
        inputs = []
        filter_complex = ""

        for i, path in enumerate(audio_paths):
            inputs.extend(["-i", path])
            filter_complex += f"[{i}:a]"

        filter_complex += f"concat=n={len(audio_paths)}:v=0:a=1[out]"

        cmd = inputs + ["-filter_complex", filter_complex, "-map", "[out]", "-c:a", "libmp3lame", "-q:a", "2"]
        return (await self.execute_ffmpeg(cmd, None, output_path))[0]

    async def split_video(self, input_path: str, output_pattern: str, segment_time: str = "01:00:00") -> List[str]:
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

        Time strings should be in HH:MM:SS(.xxx) or MM:SS(.xxx) or seconds format.
        """
        try:
            # Parse times into seconds
            def _to_seconds(tstr: str) -> float:
                parts = tstr.split(":")
                if len(parts) == 3:
                    h = int(parts[0])
                    m = int(parts[1])
                    s = float(parts[2])
                    return h * 3600 + m * 60 + s
                elif len(parts) == 2:
                    m = int(parts[0])
                    s = float(parts[1])
                    return m * 60 + s
                else:
                    return float(parts[0])

            start_s = _to_seconds(start_time)
            end_s = _to_seconds(end_time)
            if end_s <= start_s:
                logger.error("trim_video: end_time must be greater than start_time")
                return False

            duration = end_s - start_s

            # Use copy where possible for speed; rely on execute_ffmpeg to build command
            cmd = ["-ss", str(start_time), "-t", str(duration), "-c", "copy"]
            return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]
        except Exception as e:
            logger.exception("trim_video failed: %s", e)
            return False

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
        """Extract subtitles from video."""
        cmd = ["-map", "0:s:0", "-c:s", "mov_text"]  # or 'copy' for original format
        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

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

    async def extract_streams(self, input_path: str, output_dir: str) -> Dict[str, str]:
        """Extract all streams (video, audio, subtitles)."""
        # Validate input and probe safely
        try:
            if not input_path or not os.path.exists(input_path):
                logger.error("extract_streams: input file missing: %s", input_path)
                return {}
            probe = ffmpeg.probe(input_path)
        except Exception as e:
            logger.error("extract_streams: probe failed for %s: %s", input_path, e)
            return {}
        streams = probe.get("streams", [])

        extracted = {}

        for i, stream in enumerate(streams):
            codec_type = stream.get("codec_type", "unknown")

            if codec_type == "video":
                output = os.path.join(output_dir, f"stream_video_{i}.h264")
                cmd = ["-map", f"0:v:{i}", "-c:v", "copy", "-an"]
            elif codec_type == "audio":
                output = os.path.join(output_dir, f"stream_audio_{i}.aac")
                cmd = ["-map", f"0:a:{i}", "-c:a", "copy", "-vn"]
            elif codec_type == "subtitle":
                output = os.path.join(output_dir, f"stream_subtitle_{i}.srt")
                cmd = ["-map", f"0:s:{i}", "-c:s", "srt"]
            else:
                continue

            success = (await self.execute_ffmpeg(cmd, input_path, output))[0]
            if success:
                extracted[f"{codec_type}_{i}"] = output

        return extracted

    async def repair_video(self, input_path: str, output_path: str) -> bool:
        """Attempt to repair corrupted video."""
        cmd = ["-c", "copy"]
        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

    async def take_screenshot_at_time(self, input_path: str, output_path: str, time: str = "00:00:01") -> bool:
        """Take screenshot at specific time."""
        cmd = ["-ss", time, "-vframes", "1", "-q:v", "2"]
        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

    async def take_screenshot_grid(self, input_path: str, output_dir: str, count: int = 9) -> List[str]:
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
        """Generate sample/preview of video."""
        # Prefer re-encoding to an H.264/AAC MP4 with faststart for Telegram
        # when the requested output is MP4; otherwise try to copy streams.
        try:
            if output_path.lower().endswith(".mp4"):
                cmd = [
                    "-t",
                    str(duration),
                    "-c:v",
                    "libx264",
                    "-preset",
                    "veryfast",
                    "-crf",
                    "28",
                    "-c:a",
                    "aac",
                    "-b:a",
                    "96k",
                    "-movflags",
                    "+faststart",
                ]
            else:
                cmd = ["-t", str(duration), "-c", "copy"]

            return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]
        except Exception as e:
            logger.exception("generate_sample failed: %s", e)
            # Fallback to copying a range
            cmd = ["-t", str(duration), "-c", "copy"]
            return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

    async def create_archive(self, file_paths: List[str], output_path: str) -> bool:
        """Create ZIP archive of files."""
        try:
            with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for file_path in file_paths:
                    arcname = os.path.basename(file_path)
                    zipf.write(file_path, arcname)
            return True
        except Exception as e:
            logger.error(f"Archive creation failed: {e}")
            return False

    async def edit_metadata(self, input_path: str, output_path: str, metadata: Dict[str, str]) -> bool:
        """Edit video metadata."""
        cmd = ["-c", "copy", "-map_metadata", "-1"]  # Remove all metadata

        # Add new metadata
        for key, value in metadata.items():
            cmd.extend(["-metadata", f"{key}={value}"])

        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

    async def convert_audio_format(
        self, input_path: str, output_path: str, target_format: str = "mp3", quality: int = 2
    ) -> bool:
        """Convert audio between formats."""
        if target_format == "mp3":
            cmd = ["-c:a", "libmp3lame", "-q:a", str(quality)]
        elif target_format == "wav":
            cmd = ["-c:a", "pcm_s16le"]
        elif target_format == "aac":
            cmd = ["-c:a", "aac", "-b:a", "128k"]
        elif target_format == "opus":
            cmd = ["-c:a", "libopus", "-b:a", "96k"]
        else:
            cmd = ["-c:a", "copy"]

        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]

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
                pass

    async def apply_fade(
        self, input_path: str, output_path: str,
        fade_in_duration: float = 0.0, fade_out_duration: float = 0.0
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
                        audio_duration = float(
                            audio_stream.get("duration",
                                probe.get("format", {}).get("duration", 0))
                        )
                    else:
                        audio_duration = float(
                            probe.get("format", {}).get("duration", 0)
                        )
            except Exception as e:
                logger.error("apply_fade: probe failed: %s", e)

            # Fallback: use ffprobe directly
            if audio_duration <= 0:
                try:
                    import asyncio as _aio
                    proc = await _aio.create_subprocess_exec(
                        "ffprobe", "-v", "error",
                        "-show_entries", "format=duration",
                        "-of", "default=noprint_wrappers=1:nokey=1",
                        input_path,
                        stdout=_aio.subprocess.PIPE,
                        stderr=_aio.subprocess.PIPE,
                    )
                    stdout, _ = await proc.communicate()
                    audio_duration = float(stdout.decode().strip()) if stdout else 0.0
                except Exception:
                    pass

            if audio_duration <= 0:
                logger.error("apply_fade: cannot determine audio duration")
                return False

            fade_out_start = max(0.0, audio_duration - fade_out_duration)
            afilters.append(f"afade=t=out:st={fade_out_start}:d={fade_out_duration}")

        filter_str = ",".join(afilters)
        cmd = ["-af", filter_str, "-c:v", "copy"]
        return (await self.execute_ffmpeg(cmd, input_path, output_path))[0]
