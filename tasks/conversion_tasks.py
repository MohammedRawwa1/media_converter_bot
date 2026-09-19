# tasks/conversion_tasks.py
import asyncio
import contextlib
import logging
import os
import tempfile

import config

# Use configured FFMPEG_PATH, fallback to FFMPEG_PATH
FFMPEG_PATH = getattr(config, "FFMPEG_PATH", "ffmpeg") or "ffmpeg"

logger = logging.getLogger(__name__)

# Import timeout utilities
try:
    from utils.async_timeout_wrapper import (
        DEFAULT_FFMPEG_TIMEOUT,
        run_subprocess_with_timeout,
    )
except ImportError:
    # Fallback if module not available
    async def run_subprocess_with_timeout(cmd, timeout_seconds=18000, operation_name="Operation"):
        if create_checked_subprocess_exec is not None:
            process = await create_checked_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
        else:
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
            return stdout, stderr, process.returncode
        except TimeoutError:
            process.kill()
            raise

    DEFAULT_FFMPEG_TIMEOUT = 18000

# Ensure `create_checked_subprocess_exec` is available even when
# `utils.async_timeout_wrapper` imported successfully above. Doing this
# here guarantees the symbol exists for the rest of this module.
try:
    from utils.process_utils import create_checked_subprocess_exec
except Exception:
    create_checked_subprocess_exec = None


async def _spawn_process(*cmd, **kwargs):
    """Spawn subprocess preferring the safe helper when available.

    This centralizes the checked/normalized subprocess creation so the
    rest of this module can call `_spawn_process(...)` and avoid
    duplicating the same conditional logic.
    """
    if "create_checked_subprocess_exec" in globals() and create_checked_subprocess_exec is not None:
        return await create_checked_subprocess_exec(*cmd, **kwargs)
    return await asyncio.create_subprocess_exec(*cmd, **kwargs)


def _validate_input_file(input_path: str) -> tuple[bool, str]:
    """Validate input file exists and is readable."""
    if not input_path:
        return False, "Input path cannot be empty"

    if not os.path.exists(input_path):
        return False, f"Input file not found: {input_path}"

    if not os.path.isfile(input_path):
        return False, f"Input path is not a file: {input_path}"

    if not os.access(input_path, os.R_OK):
        return False, f"Input file is not readable: {input_path}"

    return True, ""


def _validate_output_path(output_path: str) -> tuple[bool, str]:
    """Validate output path is writable."""
    if not output_path:
        return False, "Output path cannot be empty"

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
        except Exception as e:
            return False, f"Cannot create output directory: {str(e)}"

    return True, ""


async def convert_video_to_mp3(
    input_path: str, output_path: str, bitrate: str = "192k", timeout_seconds: int = 18000
) -> tuple[bool, str]:
    """Convert video file to MP3 audio asynchronously."""
    # Validate inputs
    valid, error = _validate_input_file(input_path)
    if not valid:
        logger.error(error)
        return False, error

    valid, error = _validate_output_path(output_path)
    if not valid:
        logger.error(error)
        return False, error

    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-vn",
            "-acodec",
            "libmp3lame",
            "-ab",
            bitrate,
            "-ar",
            "48000",
            "-ac",
            "2",
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        try:
            # Timeout protection - FFmpeg can hang if ffprobe crashes
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
        except TimeoutError:
            logger.warning(f"FFmpeg timeout after {timeout_seconds}s, killing process")
            process.kill()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                logger.error("Failed to kill FFmpeg process")
            return False, f"Conversion timeout (> {timeout_seconds}s)"

        if process.returncode == 0:
            logger.info(f"Successfully converted {input_path} to MP3")
            return True, "Conversion successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            logger.error(f"Conversion failed: {error}")
            return False, error

    except asyncio.CancelledError:
        logger.error("Conversion was cancelled")
        with contextlib.suppress(Exception):
            process.kill()
        return False, "Conversion was cancelled"
    except Exception as e:
        logger.error(f"Exception in convert_video_to_mp3: {e}")
        return False, str(e)


async def compress_video(
    input_path: str, output_path: str, preset: str = "medium", crf: int = 23, timeout_seconds: int = 18000
) -> tuple[bool, str]:
    """Compress video asynchronously using preset."""
    # Validate inputs
    valid, error = _validate_input_file(input_path)
    if not valid:
        logger.error(error)
        return False, error

    valid, error = _validate_output_path(output_path)
    if not valid:
        logger.error(error)
        return False, error

    # Validate CRF range
    if not (0 <= crf <= 51):
        logger.error(f"Invalid CRF value: {crf}. Must be 0-51")
        return False, "CRF must be between 0-51"

    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-c:v",
            "libx264",
            "-preset",
            preset,
            "-crf",
            str(crf),
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        try:
            # Timeout protection
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
        except TimeoutError:
            logger.warning(f"Compression timeout after {timeout_seconds}s")
            process.kill()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                logger.error("Failed to kill compression process")
            return False, f"Compression timeout (> {timeout_seconds}s)"

        if process.returncode == 0:
            logger.info(f"Successfully compressed {input_path}")
            return True, "Compression successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except asyncio.CancelledError:
        logger.error("Compression was cancelled")
        with contextlib.suppress(Exception):
            process.kill()
        return False, "Compression was cancelled"
    except Exception as e:
        logger.error(f"Exception in compress_video: {e}")
        return False, str(e)


async def extract_audio(
    input_path: str, output_path: str, format: str = "mp3", bitrate: str = "192k", timeout_seconds: int = 18000
) -> tuple[bool, str]:
    """Extract audio from video asynchronously with timeout protection."""
    # Validate inputs
    valid, error = _validate_input_file(input_path)
    if not valid:
        logger.error(error)
        return False, error

    valid, error = _validate_output_path(output_path)
    if not valid:
        logger.error(error)
        return False, error

    try:
        codec = "libmp3lame" if format == "mp3" else "copy"

        cmd = ["ffmpeg", "-y", "-i", input_path, "-vn", "-acodec", codec, "-ab", bitrate, output_path]

        try:
            stdout, stderr, returncode = await run_subprocess_with_timeout(
                cmd, timeout_seconds=timeout_seconds, operation_name="Audio Extraction"
            )
        except Exception as e:
            logger.error(f"Audio extraction subprocess error: {e}")
            return False, str(e)

        if returncode == 0:
            logger.info(f"Successfully extracted audio from {input_path}")
            return True, "Extraction successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            logger.error(f"Audio extraction failed: {error}")
            return False, error

    except asyncio.CancelledError:
        logger.error("Audio extraction was cancelled")
        return False, "Audio extraction was cancelled"
    except Exception as e:
        logger.error(f"Exception in extract_audio: {e}")
        return False, str(e)


async def merge_videos(video_paths: list[str], output_path: str, timeout_seconds: int = 18000) -> tuple[bool, str]:
    """Merge multiple videos asynchronously with timeout protection."""
    # Validate inputs
    if not video_paths:
        return False, "No video paths provided"

    for video_path in video_paths:
        valid, error = _validate_input_file(video_path)
        if not valid:
            return False, f"Invalid video file: {error}"

    valid, error = _validate_output_path(output_path)
    if not valid:
        return False, error

    concat_file = None
    try:
        # Create concat file
        concat_content = "\n".join([f"file '{os.path.abspath(path)}'" for path in video_paths])

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write(concat_content)
            concat_file = f.name

        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", concat_file, "-c", "copy", output_path]

        try:
            stdout, stderr, returncode = await run_subprocess_with_timeout(
                cmd, timeout_seconds=timeout_seconds, operation_name="Video Merge"
            )
        except Exception as e:
            logger.error(f"Video merge subprocess error: {e}")
            return False, str(e)

        if returncode == 0:
            logger.info(f"Successfully merged {len(video_paths)} videos")
            return True, "Merge successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            logger.error(f"Video merge failed: {error}")
            return False, error

    except asyncio.CancelledError:
        logger.error("Video merge was cancelled")
        return False, "Video merge was cancelled"
    except Exception as e:
        logger.error(f"Exception in merge_videos: {e}")
        return False, str(e)
    finally:
        if concat_file and os.path.exists(concat_file):
            with contextlib.suppress(Exception):
                os.unlink(concat_file)


async def merge_audios(audio_paths: list[str], output_path: str, timeout_seconds: int = 18000) -> tuple[bool, str]:
    """Merge multiple audio files asynchronously with timeout protection."""
    # Validate inputs
    if not audio_paths:
        return False, "No audio paths provided"

    for audio_path in audio_paths:
        valid, error = _validate_input_file(audio_path)
        if not valid:
            return False, f"Invalid audio file: {error}"

    valid, error = _validate_output_path(output_path)
    if not valid:
        return False, error

    concat_file = None
    try:
        cmd = ["ffmpeg", "-y"]
        for path in audio_paths:
            cmd.extend(["-i", path])

        # Build filter
        filter_complex = ""
        for i in range(len(audio_paths)):
            filter_complex += f"[{i}:a]"
        filter_complex += f"concat=n={len(audio_paths)}:v=0:a=1[out]"

        cmd.extend(["-filter_complex", filter_complex, "-map", "[out]", "-c:a", "libmp3lame", "-q:a", "2", output_path])

        try:
            stdout, stderr, returncode = await run_subprocess_with_timeout(
                cmd, timeout_seconds=timeout_seconds, operation_name="Audio Merge"
            )
        except Exception as e:
            logger.error(f"Audio merge subprocess error: {e}")
            return False, str(e)

        if returncode == 0:
            logger.info(f"Successfully merged {len(audio_paths)} audio files")
            return True, "Merge successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            logger.error(f"Audio merge failed: {error}")
            return False, error

    except asyncio.CancelledError:
        logger.error("Audio merge was cancelled")
        return False, "Audio merge was cancelled"
    except Exception as e:
        logger.error(f"Exception in merge_audios: {e}")
        return False, str(e)
    finally:
        if concat_file and os.path.exists(concat_file):
            with contextlib.suppress(Exception):
                os.unlink(concat_file)


async def create_slideshow(
    image_paths: list[str],
    output_path: str,
    seconds_per_image: float = 3.0,
    music_path: str | None = None,
    timeout_seconds: int = 18000,
) -> tuple[bool, str]:
    """Build a slideshow video from images, one shown for ``seconds_per_image``.

    Every image is looped for the fixed duration and scaled onto a common
    1280x720 canvas (letterboxed, never stretched) so images of different sizes
    can be concatenated in a single pass. The result is a widely-playable H.264
    MP4. When ``music_path`` names a readable audio file it is looped underneath
    the slideshow and cut at the video's end.
    """
    if not image_paths:
        return False, "No image paths provided"

    for image_path in image_paths:
        valid, error = _validate_input_file(image_path)
        if not valid:
            return False, f"Invalid image file: {error}"

    valid, error = _validate_output_path(output_path)
    if not valid:
        return False, error

    try:
        duration = float(seconds_per_image)
    except (TypeError, ValueError):
        duration = 3.0
    # A zero or negative duration would produce an empty video.
    duration = duration if duration > 0 else 3.0

    width, height = 1280, 720
    cmd = [FFMPEG_PATH, "-y"]
    for image_path in image_paths:
        cmd.extend(["-loop", "1", "-t", f"{duration}", "-i", image_path])

    # Optional background music. It is looped so a short track still covers the
    # whole slideshow, and -shortest ends the output with the video rather than
    # letting the audio keep running.
    use_music = bool(music_path) and os.path.isfile(str(music_path))
    if use_music:
        cmd.extend(["-stream_loop", "-1", "-i", music_path])

    chains = [
        (
            f"[{i}:v]scale={width}:{height}:force_original_aspect_ratio=decrease,"
            f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1,fps=30[v{i}]"
        )
        for i in range(len(image_paths))
    ]
    concat_inputs = "".join(f"[v{i}]" for i in range(len(image_paths)))
    chains.append(f"{concat_inputs}concat=n={len(image_paths)}:v=1:a=0[slideshow]")

    cmd.extend(
        [
            "-filter_complex",
            ";".join(chains),
            "-map",
            "[slideshow]",
            "-c:v",
            "libx264",
            "-preset",
            "medium",
            "-crf",
            "23",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
        ]
    )
    if use_music:
        # The music is the input right after the images, so its audio stream index
        # is len(image_paths).
        cmd.extend(["-map", f"{len(image_paths)}:a", "-c:a", "aac", "-b:a", "192k", "-shortest"])
    cmd.append(output_path)

    try:
        _stdout, stderr, returncode = await run_subprocess_with_timeout(
            cmd, timeout_seconds=timeout_seconds, operation_name="Slideshow"
        )
    except asyncio.CancelledError:
        logger.error("Slideshow was cancelled")
        return False, "Slideshow was cancelled"
    except Exception as e:
        logger.error(f"Slideshow subprocess error: {e}")
        return False, str(e)

    if returncode == 0:
        logger.info(f"Successfully built a slideshow from {len(image_paths)} image(s)")
        return True, "Slideshow created"

    error = stderr.decode("utf-8", errors="ignore")[:200]
    logger.error(f"Slideshow failed: {error}")
    return False, error


async def take_screenshot(input_path: str, output_path: str, time: str = "00:00:01") -> tuple[bool, str]:
    """Take screenshot from video asynchronously."""
    # Validate inputs
    valid, error = _validate_input_file(input_path)
    if not valid:
        logger.error(error)
        return False, error

    valid, error = _validate_output_path(output_path)
    if not valid:
        logger.error(error)
        return False, error

    try:
        cmd = ["ffmpeg", "-y", "-ss", time, "-i", input_path, "-vframes", "1", "-q:v", "2", output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Successfully took screenshot at {time}")
            return True, "Screenshot taken"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in take_screenshot: {e}")
        return False, str(e)


async def change_resolution(input_path: str, output_path: str, width: int, height: int) -> tuple[bool, str]:
    """Change video resolution asynchronously."""
    # Validate inputs
    valid, error = _validate_input_file(input_path)
    if not valid:
        logger.error(error)
        return False, error

    valid, error = _validate_output_path(output_path)
    if not valid:
        logger.error(error)
        return False, error

    # Validate dimensions
    if width <= 0 or height <= 0:
        error = f"Invalid dimensions: {width}x{height}. Both must be > 0"
        logger.error(error)
        return False, error

    if width % 2 != 0 or height % 2 != 0:
        logger.warning(f"Resolution {width}x{height} not even. FFmpeg may adjust.")

    try:
        cmd = ["ffmpeg", "-y", "-i", input_path, "-filter:v", f"scale={width}:{height}", "-c:a", "copy", output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Successfully changed resolution to {width}x{height}")
            return True, "Resolution changed"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in change_resolution: {e}")
        return False, str(e)


async def trim_media(input_path: str, output_path: str, start_time: str, end_time: str) -> tuple[bool, str]:
    """Trim video or audio asynchronously."""
    try:
        duration_parts = end_time.split(":")
        duration_seconds = int(duration_parts[0]) * 3600 + int(duration_parts[1]) * 60 + float(duration_parts[2])
        start_parts = start_time.split(":")
        start_seconds = int(start_parts[0]) * 3600 + int(start_parts[1]) * 60 + float(start_parts[2])
        duration = duration_seconds - start_seconds

        cmd = ["ffmpeg", "-y", "-ss", start_time, "-i", input_path, "-t", str(duration), "-c", "copy", output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Successfully trimmed media from {start_time} to {end_time}")
            return True, "Trim successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in trim_media: {e}")
        return False, str(e)


async def split_media_segments(
    input_path: str,
    output_dir: str,
    segment_seconds: float,
    *,
    ext: str = ".mp4",
    stem: str = "part",
) -> tuple[bool, list[str], str]:
    """Cut one media file into parts of *segment_seconds*, one file per part.

    This is the stream-copy split, and it is the same command for a video and an
    audio file - only the extension of the parts differs::

        ffmpeg -i in.mp4 -c copy -map 0 -segment_time 3600 -f segment \\
               -segment_start_number 1 -reset_timestamps 1 out/Concert.%03d.mp4

    ``-c copy`` means no re-encode: the parts are the original bytes, cut at
    segment boundaries, so splitting an hour-long file costs seconds, not hours.
    ``-map 0`` keeps every stream of the source (video, audio, subtitle tracks),
    ``-reset_timestamps 1`` restarts each part at 00:00 so a player shows the
    part's own duration instead of an hour-long timeline, and
    ``-segment_start_number 1`` numbers the parts from **001** rather than the
    segment muxer's own default of 000 - the numbering a user expects next to the
    "part 1/N" caption that goes out with each file.

    Cuts can only land on the source's keyframes, so a part can come out longer
    than *segment_seconds* (and a source whose keyframes are farther apart than
    that produces a single part). Re-encoding would fix the exact length and cost
    the whole point of this command, so the caller explains the result instead.

    Returns ``(ok, parts, error)``: the sorted list of files that were written, so
    a caller can deliver them in order, and the ffmpeg stderr tail when it failed.

    *stem* is the media's own name, so a split of ``Concert.mp4`` yields
    ``Concert.001.mp4``, ``Concert.002.mp4``, ... - the numbering Telegram users
    expect from a self-extracting archive, and the original name the user
    recognises instead of a storage path. The caller owns *output_dir* and is
    expected to give a fresh one per run, which is what keeps parts apart.
    """
    ok, message = _validate_input_file(input_path)
    if not ok:
        return False, [], message

    try:
        segment = float(segment_seconds)
    except (TypeError, ValueError):
        return False, [], "invalid segment length"
    if segment <= 0:
        return False, [], "invalid segment length"

    ext = ext if str(ext).startswith(".") else f".{ext}"
    # Traversal hardening: the stem ends up in a path, so only its file name part
    # is kept.
    safe_stem = os.path.basename(str(stem or "part")) or "part"
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        return False, [], f"cannot create the output directory: {e}"

    # A float-typed second is what ``-segment_time`` takes; ``3600`` and
    # ``01:00:00`` are the same to ffmpeg, and the number cannot be misread.
    segment_arg = f"{segment:.3f}".rstrip("0").rstrip(".")
    # ``Stem.%03d.ext``: ffmpeg numbers the parts itself, so the files on disk are
    # already named the way they are delivered - nothing has to be renamed later,
    # and no delivery can fall back to naming a part after its storage path.
    pattern = os.path.join(output_dir, f"{safe_stem}.%03d{ext}")
    cmd = [
        FFMPEG_PATH,
        "-y",
        "-i",
        input_path,
        "-c",
        "copy",
        "-map",
        "0",
        "-segment_time",
        segment_arg,
        "-f",
        "segment",
        # 001, not the segment muxer's default 000: the parts are delivered under
        # their own names, next to a "part 1/N" caption.
        "-segment_start_number",
        "1",
        "-reset_timestamps",
        "1",
        pattern,
    ]

    try:
        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _stdout, stderr = await process.communicate()
    except Exception as e:
        logger.error(f"Exception in split_media_segments: {e}")
        return False, [], str(e)

    if process.returncode != 0:
        error = stderr.decode("utf-8", errors="ignore")[-300:]
        logger.error(f"split_media_segments failed: {error}")
        return False, [], error

    prefix = f"{safe_stem}."
    parts = sorted(
        os.path.join(output_dir, name)
        for name in os.listdir(output_dir)
        if name.startswith(prefix) and name.endswith(ext) and os.path.getsize(os.path.join(output_dir, name)) > 0
    )
    if not parts:
        return False, [], "ffmpeg produced no parts"
    logger.info(f"Split media into {len(parts)} part(s) of ~{segment_arg}s")
    return True, parts, ""


async def repair_video(input_path: str, output_path: str) -> tuple[bool, str]:
    """Repair corrupted video asynchronously."""
    try:
        cmd = ["ffmpeg", "-y", "-i", input_path, "-c", "copy", output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully repaired video")
            return True, "Repair successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in repair_video: {e}")
        return False, str(e)


async def optimize_video(input_path: str, output_path: str, preset: str = "slow", crf: int = 23) -> tuple[bool, str]:
    """Optimize video for web asynchronously."""
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
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
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully optimized video")
            return True, "Optimization successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in optimize_video: {e}")
        return False, str(e)


async def create_thumbnail_grid(input_path: str, output_path: str, rows: int = 3, cols: int = 3) -> tuple[bool, str]:
    """Create thumbnail grid asynchronously."""
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-vf",
            f"select=not(mod(n\\,{(rows * cols) + 1})),scale=160:-1,tile={cols}x{rows}",
            "-frames:v",
            "1",
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully created thumbnail grid")
            return True, "Thumbnail grid created"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in create_thumbnail_grid: {e}")
        return False, str(e)


async def generate_sample(input_path: str, output_path: str, duration: int = 30) -> tuple[bool, str]:
    """Generate sample/preview asynchronously."""
    try:
        # For mp4 outputs, re-encode to H.264/AAC and add movflags for streaming
        if output_path.lower().endswith(".mp4"):
            cmd = [
                "ffmpeg",
                "-y",
                "-i",
                input_path,
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
                output_path,
            ]
        else:
            cmd = ["ffmpeg", "-y", "-i", input_path, "-t", str(duration), "-c", "copy", output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Successfully generated {duration}s sample")
            return True, "Sample generated"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in generate_sample: {e}")
        return False, str(e)


async def extract_streams(input_path: str, output_dir: str) -> tuple[bool, dict[str, str]]:
    """Extract all streams asynchronously."""
    try:
        # Validate input path before probing
        valid, error = _validate_input_file(input_path)
        if not valid:
            logger.error(error)
            return False, {}

        # First get probe info
        cmd_probe = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", input_path]

        process = await _spawn_process(*cmd_probe, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        import json

        probe_data = json.loads(stdout.decode())
        streams = probe_data.get("streams", [])

        extracted = {}

        for i, stream in enumerate(streams):
            codec_type = stream.get("codec_type", "unknown")

            if codec_type == "video":
                output = os.path.join(output_dir, f"stream_video_{i}.mp4")
                cmd = ["ffmpeg", "-y", "-i", input_path, "-map", f"0:v:{i}", "-c:v", "copy", "-an", output]
            elif codec_type == "audio":
                output = os.path.join(output_dir, f"stream_audio_{i}.aac")
                cmd = ["ffmpeg", "-y", "-i", input_path, "-map", f"0:a:{i}", "-c:a", "copy", "-vn", output]
            elif codec_type == "subtitle":
                output = os.path.join(output_dir, f"stream_subtitle_{i}.srt")
                cmd = ["ffmpeg", "-y", "-i", input_path, "-map", f"0:s:{i}", "-c:s", "srt", output]
            else:
                continue

            proc = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

            _, _ = await proc.communicate()

            if proc.returncode == 0:
                extracted[f"{codec_type}_{i}"] = output

        return True, extracted

    except Exception as e:
        logger.error(f"Exception in extract_streams: {e}")
        return False, {}


async def convert_audio_format(
    input_path: str, output_path: str, target_format: str = "mp3", bitrate: str = "192k"
) -> tuple[bool, str]:
    """Convert audio format asynchronously."""
    try:
        codec_map = {"mp3": "libmp3lame", "wav": "pcm_s16le", "aac": "aac", "flac": "flac", "ogg": "libvorbis"}

        codec = codec_map.get(target_format, "libmp3lame")

        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-c:a",
            codec,
            "-b:a",
            bitrate if codec != "pcm_s16le" else "1411k",
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Successfully converted to {target_format}")
            return True, "Conversion successful"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in convert_audio_format: {e}")
        return False, str(e)


async def adjust_bitrate(
    input_path: str, output_path: str, video_bitrate: str = "5000k", audio_bitrate: str = "128k"
) -> tuple[bool, str]:
    """Adjust video and audio bitrate asynchronously."""
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-b:v",
            video_bitrate,
            "-b:a",
            audio_bitrate,
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully adjusted bitrate")
            return True, "Bitrate adjusted"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in adjust_bitrate: {e}")
        return False, str(e)


async def normalize_audio(input_path: str, output_path: str) -> tuple[bool, str]:
    """Normalize audio volume asynchronously."""
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            input_path,
            "-filter:a",
            "loudnorm=I=-20:TP=-1.5:LRA=11",
            "-c:v",
            "copy",
            output_path,
        ]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully normalized audio")
            return True, "Audio normalized"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in normalize_audio: {e}")
        return False, str(e)


async def apply_fade(
    input_path: str,
    output_path: str,
    fade_in_duration: float = 0.0,
    fade_out_duration: float = 0.0,
) -> tuple[bool, str]:
    """Apply a fade-in and/or fade-out to the audio track of a media file.

    Counterpart of ``ExtendedMediaConverter.apply_fade`` for the worker, which
    has no converter instance. A fade-out starts relative to the end of the
    media, so the duration has to be probed from the resolved source first -
    that is why this runs as its own job type rather than as a static
    ``ffmpeg_args`` list built by the caller (which has no source to probe on a
    cache repeat).
    """
    try:
        fade_in_duration = float(fade_in_duration or 0.0)
        fade_out_duration = float(fade_out_duration or 0.0)
    except (TypeError, ValueError):
        return False, "invalid fade duration"

    if fade_in_duration <= 0 and fade_out_duration <= 0:
        logger.warning("apply_fade: both durations are zero, nothing to do")
        return False, "no fade duration"

    afilters = []
    if fade_in_duration > 0:
        afilters.append(f"afade=t=in:st=0:d={fade_in_duration}")

    if fade_out_duration > 0:
        duration = 0.0
        try:
            from utils.ffmpeg_runner import probe_duration

            duration = float(await probe_duration(input_path) or 0.0)
        except Exception:
            logger.debug("apply_fade: duration probe failed for %s", input_path)
        if duration <= 0:
            logger.error("apply_fade: cannot determine media duration for fade-out")
            return False, "cannot determine media duration"
        fade_out_start = max(0.0, duration - fade_out_duration)
        afilters.append(f"afade=t=out:st={fade_out_start}:d={fade_out_duration}")

    cmd = [
        FFMPEG_PATH,
        "-y",
        "-i",
        input_path,
        "-af",
        ",".join(afilters),
        "-c:v",
        "copy",
        output_path,
    ]

    try:
        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _stdout, stderr = await process.communicate()
    except Exception as e:
        logger.error(f"Exception in apply_fade: {e}")
        return False, str(e)

    if process.returncode == 0:
        logger.info("Successfully applied fade (%s)", ",".join(afilters))
        return True, "Fade applied"

    return False, stderr.decode("utf-8", errors="ignore")[:200]


async def extract_subtitles(input_path: str, output_path: str) -> tuple[bool, str]:
    """Extract subtitles from video asynchronously."""
    try:
        cmd = ["ffmpeg", "-y", "-i", input_path, "-map", "0:s:0", "-c:s", "srt", output_path]

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully extracted subtitles")
            return True, "Subtitles extracted"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in extract_subtitles: {e}")
        return False, str(e)


async def edit_metadata(input_path: str, output_path: str, metadata: dict[str, str]) -> tuple[bool, str]:
    """Edit media metadata asynchronously."""
    try:
        cmd = ["ffmpeg", "-y", "-i", input_path, "-c", "copy", "-map_metadata", "-1"]

        for key, value in metadata.items():
            cmd.extend(["-metadata", f"{key}={value}"])

        cmd.append(output_path)

        process = await _spawn_process(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info("Successfully edited metadata")
            return True, "Metadata updated"
        else:
            error = stderr.decode("utf-8", errors="ignore")[:200]
            return False, error

    except Exception as e:
        logger.error(f"Exception in edit_metadata: {e}")
        return False, str(e)


async def create_archive(file_paths: list[str], output_path: str) -> tuple[bool, str]:
    """Create ZIP archive of files asynchronously."""
    try:
        import zipfile

        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file_path in file_paths:
                arcname = os.path.basename(file_path)
                zipf.write(file_path, arcname)

        logger.info(f"Successfully created archive with {len(file_paths)} files")
        return True, "Archive created"

    except Exception as e:
        logger.error(f"Exception in create_archive: {e}")
        return False, str(e)
