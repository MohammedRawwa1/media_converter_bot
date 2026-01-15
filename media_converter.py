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
        # First probe to get stream info
        probe = ffmpeg.probe(input_path)
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
        """Create thumbnail grid from video."""
        # Get video duration for spacing
        probe = ffmpeg.probe(input_path)
        duration = float(probe["format"]["duration"])

        # Create temporary screenshots
        temp_dir = tempfile.mkdtemp()
        screenshots = []

        # Take screenshots at intervals
        for i in range(rows * cols):
            time_sec = (duration * i) / (rows * cols)
            time_str = f"{int(time_sec // 3600):02d}:{int((time_sec % 3600) // 60):02d}:{time_sec % 60:06.3f}"
            temp_file = os.path.join(temp_dir, f"temp_{i:02d}.jpg")

            if await self.take_screenshot_at_time(input_path, temp_file, time_str):
                screenshots.append(temp_file)

        # Create grid using ImageMagick (simplified approach)
        if len(screenshots) == rows * cols:
            # This is simplified - actual implementation would use ImageMagick or PIL
            # For now, just return first screenshot
            import shutil

            shutil.copy(screenshots[0], output_path)

            # Cleanup
            for f in screenshots:
                os.unlink(f)
            os.rmdir(temp_dir)

            return True

        return False
