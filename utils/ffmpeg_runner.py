import asyncio
import contextlib
import json
import logging
import os
import shlex
import signal
from collections.abc import Callable

import config

logger = logging.getLogger(__name__)

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None


CREATE_NEW_PROCESS_GROUP = 0x00000200 if os.name == "nt" else 0


async def probe_duration(path: str) -> float | None:
    """Probe media duration using ffprobe (sync subprocess wrapped)."""
    # Defensive checks: ensure caller provided a valid path
    if not path:
        raise ValueError("Input path missing before ffprobe")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file missing before ffprobe: {path}")
    ffprobe = getattr(config, "FFMPEG_PATH", "ffmpeg")
    ffprobe = ffprobe.replace("ffmpeg", "ffprobe") if "ffmpeg" in ffprobe else "ffprobe"
    try:
        from utils.process_utils import create_checked_subprocess_exec
    except Exception:
        create_checked_subprocess_exec = None

    if create_checked_subprocess_exec is not None:
        proc = await create_checked_subprocess_exec(
            ffprobe,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    else:
        proc = await asyncio.create_subprocess_exec(
            ffprobe,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    out, _ = await proc.communicate()
    try:
        return float(out.decode().strip())
    except Exception:
        return None


def _parse_out_time(timestr: str) -> float:
    try:
        parts = timestr.split(":")
        h = int(parts[0])
        m = int(parts[1])
        s = float(parts[2])
        return h * 3600 + m * 60 + s
    except Exception:
        try:
            return float(timestr)
        except Exception:
            return 0.0


async def probe_media(path: str) -> dict:
    """Run comprehensive ffprobe on a media file, returning structured metadata.

    Extracts all fields shown in the T4 timeline of the pipeline diagram:
    duration, fps, bitrate (video + audio), streams (codec, language, disposition),
    chapters, rotation, color metadata (space/primaries/transfer), SAR, DAR,
    creation_time.

    Returns a dict with all available fields; missing fields are empty strings.
    Never raises — returns empty dict on any error.
    """
    result: dict[str, str | int | float] = {}

    if not path or not os.path.exists(path):
        logger.warning("probe_media: path does not exist: %s", path)
        return result

    ffprobe = getattr(config, "FFMPEG_PATH", "ffmpeg").replace("ffmpeg", "ffprobe")
    if "ffprobe" not in ffprobe:
        ffprobe = "ffprobe"

    try:
        from utils.process_utils import create_checked_subprocess_exec
    except Exception:
        create_checked_subprocess_exec = None

    cmd = [
        ffprobe,
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        "-show_chapters",
        path,
    ]

    try:
        if create_checked_subprocess_exec is not None:
            proc = await create_checked_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
        else:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
    except Exception as e:
        logger.debug("probe_media: ffprobe subprocess error: %s", e)
        return result

    if proc.returncode != 0:
        logger.debug("probe_media: ffprobe returned %s", proc.returncode)
        return result

    try:
        data = json.loads(stdout.decode("utf-8", errors="replace"))
    except Exception as e:
        logger.debug("probe_media: JSON parse error: %s", e)
        return result

    fmt = data.get("format") or {}
    streams = data.get("streams") or []
    chapters = data.get("chapters") or []

    # ── Format-level metadata ──
    dur_str = fmt.get("duration", "")
    if dur_str:
        with contextlib.suppress(ValueError, TypeError):
            result["duration"] = float(dur_str)

    try:
        bitrate_str = fmt.get("bit_rate", "")
        if bitrate_str:
            result["format_bitrate"] = int(bitrate_str)
    except (ValueError, TypeError):
        pass

    result["format_name"] = fmt.get("format_name", "")
    result["size"] = int(fmt.get("size", 0)) if fmt.get("size") else 0

    # ── Chapters ──
    result["chapters"] = len(chapters) if chapters else 0

    # ── Per-stream metadata ──
    video_streams = [s for s in streams if s.get("codec_type") == "video"]
    audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
    subtitle_streams = [s for s in streams if s.get("codec_type") == "subtitle"]

    result["video_streams"] = len(video_streams)
    result["audio_streams"] = len(audio_streams)
    result["subtitle_streams"] = len(subtitle_streams)

    # ── Primary video stream (first) ──
    if video_streams:
        vs = video_streams[0]
        result["video_codec"] = vs.get("codec_name", "")
        result["video_codec_long"] = vs.get("codec_long_name", "")
        result["width"] = vs.get("width", 0)
        result["height"] = vs.get("height", 0)
        result["coded_width"] = vs.get("coded_width", 0)
        result["coded_height"] = vs.get("coded_height", 0)

        # FPS: prefer r_frame_rate, fall back to avg_frame_rate
        for fps_key in ("r_frame_rate", "avg_frame_rate"):
            fps_val = vs.get(fps_key, "")
            if fps_val and "/" in fps_val:
                try:
                    num, den = fps_val.split("/")
                    result["fps"] = round(float(num) / max(float(den), 1), 3)
                    break
                except (ValueError, ZeroDivisionError):
                    continue

        # Bitrate
        vbr = vs.get("bit_rate", "")
        if vbr:
            with contextlib.suppress(ValueError, TypeError):
                result["video_bitrate"] = int(vbr)

        # Rotation
        side_data = vs.get("side_data_list") or []
        rotation = vs.get("rotation", 0) or 0
        if not rotation:
            for sd in side_data:
                if sd.get("side_data_type") == "Display Matrix":
                    rotation = abs(int(sd.get("rotation", 0)))
                    break
        result["rotation"] = rotation

        # SAR / DAR
        sar = vs.get("sample_aspect_ratio", "")
        dar = vs.get("display_aspect_ratio", "")
        result["sar"] = sar
        result["dar"] = dar

        # Color metadata
        result["color_space"] = vs.get("color_space", "")
        result["color_primaries"] = vs.get("color_primaries", "")
        result["color_transfer"] = vs.get("color_transfer", "")
        result["color_range"] = vs.get("color_range", "")
        result["pix_fmt"] = vs.get("pix_fmt", "")

        # Pixel format
        result["pixel_format"] = vs.get("pix_fmt", "")

        # Creation time from video stream metadata
        result["creation_time"] = vs.get("tags", {}).get("creation_time", "")

        # Video stream disposition
        disposition = vs.get("disposition", {}) or {}
        result["default_stream"] = disposition.get("default", 0)
        result["forced_stream"] = disposition.get("forced", 0)
        result["original_stream"] = disposition.get("original", 0)

    # ── Primary audio stream (first) ──
    if audio_streams:
        audio = audio_streams[0]
        result["audio_codec"] = audio.get("codec_name", "")
        result["audio_codec_long"] = audio.get("codec_long_name", "")
        abr = audio.get("bit_rate", "")
        if abr:
            with contextlib.suppress(ValueError, TypeError):
                result["audio_bitrate"] = int(abr)
        result["audio_sample_rate"] = audio.get("sample_rate", "")
        result["audio_channels"] = audio.get("channels", 0)
        result["audio_channel_layout"] = audio.get("channel_layout", "")

        # Audio language from tags
        audio_tags = audio.get("tags", {}) or {}
        result["language"] = audio_tags.get("language", "")

        # Audio disposition
        audio_disposition = audio.get("disposition", {}) or {}
        result["audio_default"] = audio_disposition.get("default", 0)
        result["audio_forced"] = audio_disposition.get("forced", 0)

    # ── Format-level creation_time ──
    if not result.get("creation_time"):
        fmt_tags = fmt.get("tags", {}) or {}
        result["creation_time"] = fmt_tags.get("creation_time", "")

    logger.info(
        "probe_media: %s — dur=%s codec=%s %sx%s fps=%s rot=%s audio=%s ch=%s lang=%s chapters=%s",
        path,
        result.get("duration", "?"),
        result.get("video_codec", "?"),
        result.get("width", "?"),
        result.get("height", "?"),
        result.get("fps", "?"),
        result.get("rotation", "?"),
        result.get("audio_codec", "?"),
        result.get("audio_channels", "?"),
        result.get("language", "?"),
        result.get("chapters", "?"),
    )
    return result


async def run_ffmpeg(
    input_path: str,
    output_path: str,
    job_id: str,
    ffmpeg_args: list | None = None,
    redis_url: str | None = None,
    progress_channel: str | None = None,
    on_progress: Callable[[float, str], None] | None = None,
):
    """Run ffmpeg asynchronously, publish progress to Redis pubsub and call callback.

    - Builds a command using `config.FFMPEG_PATH`.
    - Uses `-progress pipe:1` to read key=value progress lines.
    - Publishes JSON updates to `progress_channel` (if provided) and writes job hash `ffmpeg:job:{job_id}`.
    """
    ffmpeg_bin = getattr(config, "FFMPEG_PATH", "ffmpeg") or "ffmpeg"
    # Use veryfast preset to reduce memory usage on memory-constrained hosts.
    # The -preset fast default was consuming too much RAM alongside multiple uvicorn workers.
    # veryfast uses ~30% less memory at the cost of slightly larger output files.
    #
    # ── Bitrate caps ─────────────────────────────────────────────────────
    # CRF 23 without a maximum bitrate can cause massive file size inflation
    # for certain content (e.g. 27 MB input → 165 MB output was observed).
    # The env vars below let operators tune the video bitrate cap.  The
    # defaults (2 Mbps video + 128 Kbps audio ≈ 2.15 Mbps total) prevent
    # outrageous bloat while preserving good visual quality for most content.
    # Set FFMPEG_MAXRATE to "0" or "unlimited" to disable the cap entirely.
    # ─────────────────────────────────────────────────────────────────────
    _maxrate = os.getenv("FFMPEG_MAXRATE", "2M")
    _bufsize = os.getenv("FFMPEG_BUFSIZE", "4M")
    _bitrate_caps = []
    if _maxrate.lower() not in ("0", "unlimited", "", "none"):
        _bitrate_caps.extend(["-maxrate", _maxrate, "-bufsize", _bufsize])

    ffmpeg_args = (
        ffmpeg_args
        or [
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
        + _bitrate_caps
    )

    # Validate input path before probing/starting ffmpeg
    if not input_path:
        logger.error("Input file missing before ffmpeg for job %s", job_id)
        return False, "input file missing"
    if not os.path.exists(input_path):
        logger.error("Input file not found before ffmpeg for job %s: %s", job_id, input_path)
        return False, "input file missing"

    duration = await probe_duration(input_path)

    # try to get input file size
    in_bytes = 0
    try:
        if os.path.exists(input_path):
            in_bytes = await asyncio.get_running_loop().run_in_executor(None, lambda p=input_path: os.path.getsize(p))
    except Exception:
        in_bytes = 0

    cmd = (
        [ffmpeg_bin, "-y", "-hide_banner", "-loglevel", "error", "-i", input_path]
        + ffmpeg_args
        + ["-progress", "pipe:1", "-nostats", output_path]
    )

    logger.info("Running ffmpeg: %s", " ".join(shlex.quote(p) for p in cmd))

    # choose platform-specific creation flags / preexec
    kwargs = {}
    if os.name == "nt":
        kwargs["creationflags"] = CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["preexec_fn"] = os.setsid

    # start process
    try:
        from utils.process_utils import create_checked_subprocess_exec
    except Exception:
        create_checked_subprocess_exec = None

    if create_checked_subprocess_exec is not None:
        proc = await create_checked_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, **kwargs
        )
    else:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, **kwargs
        )

    # Start a background task to capture stderr to a per-job file for debugging.
    stderr_task = None
    try:
        logs_dir = os.path.join(os.getcwd(), "logs")
        os.makedirs(logs_dir, exist_ok=True)
        stderr_path = os.path.join(logs_dir, f"ffmpeg_{job_id}.stderr")

        async def _drain_stderr(p, dst):
            try:
                if not p.stderr:
                    return
                # open file in binary append mode
                with open(dst, "ab") as fh:
                    while True:
                        chunk = await p.stderr.read(1024)
                        if not chunk:
                            break
                        try:
                            fh.write(chunk)
                            fh.flush()
                        except Exception:
                            logger.debug("ffmpeg_runner: failed to write stderr chunk for job %s", job_id)
            except Exception:
                logger.exception("ffmpeg_runner: stderr drain failed for %s", job_id)

        try:
            stderr_task = asyncio.create_task(_drain_stderr(proc, stderr_path))
        except Exception:
            stderr_task = None
    except Exception:
        stderr_task = None

    redis_client = None
    if aioredis and (redis_url or os.environ.get("REDIS_URL")):
        try:
            redis_client = aioredis.from_url(redis_url or os.environ.get("REDIS_URL"))
            # Initialize job hash so status is available immediately
            with contextlib.suppress(Exception):
                await redis_client.hset(
                    f"ffmpeg:job:{job_id}",
                    mapping={"status": "processing", "progress": 0, "message": "started", "in_bytes": str(in_bytes)},
                )
        except Exception:
            redis_client = None

    current_out_time = 0.0

    try:
        assert proc.stdout is not None
        # Read line by line (ffmpeg -progress emits key=value lines)
        async for raw in proc.stdout:
            line = raw.decode(errors="ignore").strip()
            if not line:
                continue
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            if key == "out_time":
                current_out_time = _parse_out_time(val)
                pct = (current_out_time / duration * 100.0) if duration and duration > 0 else 0.0
                message = f"encoding {pct:.1f}%"

                # Try to read the current output file size (non-blocking)
                out_bytes = 0
                try:
                    if os.path.exists(output_path):
                        out_bytes = await asyncio.get_running_loop().run_in_executor(
                            None, lambda p=output_path: os.path.getsize(p)
                        )
                except Exception:
                    out_bytes = 0

                progress_by_size = None
                try:
                    if in_bytes and in_bytes > 0:
                        progress_by_size = round((out_bytes / in_bytes) * 100.0, 2)
                except Exception:
                    progress_by_size = None

                payload = {
                    "job_id": job_id,
                    "progress": round(pct, 2),
                    "message": message,
                    "out_bytes": out_bytes,
                    "in_bytes": in_bytes,
                    "progress_by_size": progress_by_size,
                }

                # publish to redis
                if redis_client and progress_channel:
                    try:
                        await redis_client.publish(progress_channel, json.dumps(payload))
                        # store numeric values as strings to keep Redis simple
                        store_map = {
                            "progress": payload["progress"],
                            "message": message,
                            "status": "processing",
                            "out_bytes": str(out_bytes),
                            "in_bytes": str(in_bytes),
                        }
                        if progress_by_size is not None:
                            store_map["progress_by_size"] = str(progress_by_size)
                        await redis_client.hset(f"ffmpeg:job:{job_id}", mapping=store_map)
                    except Exception:
                        logger.debug("ffmpeg_runner: failed to publish progress to Redis for job %s", job_id)
                if on_progress:
                    with contextlib.suppress(Exception):
                        on_progress(payload["progress"], message)
            # periodically check for cancel flag in redis
            if redis_client:
                try:
                    cancel = await redis_client.hget(f"ffmpeg:job:{job_id}", "cancel")
                    if cancel:
                        # cancel requested - kill the whole process group where possible
                        try:
                            if os.name != "nt":
                                try:
                                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                                except Exception:
                                    proc.kill()
                            else:
                                # Best-effort for Windows: send CTRL_BREAK to process group
                                try:
                                    proc.send_signal(signal.CTRL_BREAK_EVENT)
                                except Exception:
                                    proc.kill()
                        except Exception:
                            with contextlib.suppress(Exception):
                                proc.kill()
                        with contextlib.suppress(Exception):
                            await redis_client.hset(
                                f"ffmpeg:job:{job_id}", mapping={"status": "cancelled", "message": "cancelled by user"}
                            )
                        return False, "cancelled"
                except Exception:
                    logger.debug("ffmpeg_runner: failed to check cancel flag for job %s", job_id)
            # Check for progress=end marker (separate if, not elif of redis check)
            if key == "progress" and val == "end":
                # finish marker
                break

        # wait for process exit
        await proc.wait()

        if proc.returncode == 0:
            # mark finished
            if redis_client:
                try:
                    # ensure final sizes are recorded
                    final_out = 0
                    try:
                        if os.path.exists(output_path):
                            final_out = await asyncio.get_running_loop().run_in_executor(
                                None, lambda p=output_path: os.path.getsize(p)
                            )
                    except Exception:
                        final_out = 0

                    finished_map = {
                        "progress": 100,
                        "message": "encoding finished",
                        "status": "processing",
                        "output": output_path,
                        "out_bytes": str(final_out),
                        "in_bytes": str(in_bytes),
                        "output_filename": os.path.basename(output_path),
                    }
                    await redis_client.hset(f"ffmpeg:job:{job_id}", mapping=finished_map)
                    if progress_channel:
                        await redis_client.publish(
                            progress_channel,
                            json.dumps(
                                {
                                    "job_id": job_id,
                                    "progress": 100,
                                    "message": "encoding finished",
                                    "output": output_path,
                                    "out_bytes": final_out,
                                    "in_bytes": in_bytes,
                                }
                            ),
                        )
                except Exception:
                    logger.debug("ffmpeg_runner: failed to publish job completion for %s", job_id)
            return True, output_path
        else:
            stderr = await proc.stderr.read() if proc.stderr else b""
            err = stderr.decode(errors="ignore")[:1000]
            if redis_client:
                try:
                    await redis_client.hset(
                        f"ffmpeg:job:{job_id}",
                        mapping={
                            "status": "error",
                            "message": err,
                            "out_bytes": str(out_bytes) if "out_bytes" in locals() else "0",
                            "in_bytes": str(in_bytes),
                        },
                    )
                    if progress_channel:
                        await redis_client.publish(
                            progress_channel,
                            json.dumps({"job_id": job_id, "progress": 0, "message": "error", "error": err}),
                        )
                except Exception:
                    logger.debug("ffmpeg_runner: failed to publish error for job %s", job_id)
            return False, err

    except asyncio.CancelledError:
        with contextlib.suppress(Exception):
            proc.kill()
        raise
    finally:
        try:
            if redis_client:
                try:
                    aclose = getattr(redis_client, "aclose", None)
                    if aclose is not None:
                        await aclose()
                    else:
                        await redis_client.close()
                except Exception:
                    logger.debug("ffmpeg_runner: failed to close Redis client")
        except Exception:
            logger.debug("ffmpeg_runner: error in finally block for job %s", job_id)


async def probe_video_for_delivery(file_path: str) -> tuple[dict | None, str | None]:
    """Probe a video file for metadata and generate a thumbnail for Telegram delivery.

    Shared utility used by both the bot handler (_send_video_result) and the
    worker (_probe_output_metadata) to avoid duplicating ffprobe + ffmpeg
    thumbnail logic.

    Extracts ALL metadata in a single ffprobe call: dimensions, duration,
    codec names, bitrates, and FPS — so callers don't need a second probe.

    Returns:
        Tuple of (video_meta dict or None, thumb_path str or None).
        ``video_meta`` has keys ``duration`` (int), ``width`` (int), ``height`` (int),
        plus optional ``video_codec``, ``video_bitrate``, ``fps``,
        ``audio_codec``, ``audio_bitrate``.
        ``thumb_path`` is a path to a JPEG thumbnail file (caller cleans up).
    """
    if not file_path or not os.path.exists(file_path):
        return None, None

    video_meta = None
    thumb_path = None

    # ── ffprobe: dimensions, duration, codec, bitrate, fps (single call) ──
    ffprobe_bin = getattr(config, "FFMPEG_PATH", "ffmpeg").replace("ffmpeg", "ffprobe")
    try:
        proc = await asyncio.create_subprocess_exec(
            ffprobe_bin,
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_streams",
            "-show_format",
            file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
        if proc.returncode == 0:
            data = json.loads(stdout.decode() or "{}")
            meta = {}
            for s in data.get("streams", []):
                if s.get("codec_type") == "video":
                    if "width" in s:
                        meta["width"] = s["width"]
                    if "height" in s:
                        meta["height"] = s["height"]
                    if s.get("codec_name"):
                        meta["video_codec"] = s["codec_name"]
                    if s.get("bit_rate"):
                        with contextlib.suppress(ValueError, TypeError):
                            meta["video_bitrate"] = int(s["bit_rate"])
                    if s.get("r_frame_rate"):
                        try:
                            _parts = str(s["r_frame_rate"]).split("/")
                            if len(_parts) == 2 and int(_parts[1]) > 0:
                                meta["fps"] = round(int(_parts[0]) / int(_parts[1]), 2)
                        except (ValueError, ZeroDivisionError):
                            pass
                elif s.get("codec_type") == "audio":
                    if s.get("codec_name"):
                        meta["audio_codec"] = s["codec_name"]
                    if s.get("bit_rate"):
                        with contextlib.suppress(ValueError, TypeError):
                            meta["audio_bitrate"] = int(s["bit_rate"])
            fmt = data.get("format", {})
            if fmt.get("duration"):
                with contextlib.suppress(ValueError, TypeError):
                    meta["duration"] = int(float(fmt["duration"]))
            if meta:
                video_meta = meta
    except Exception:
        logger.debug("probe_video_for_delivery: ffprobe failed for %s", file_path)

    # ── ffmpeg thumbnail at 1s ──
    import shutil as _shutil
    import tempfile as _tf

    _thumb_dir = _tf.mkdtemp(prefix="delivery_thumb_")
    _tp = os.path.join(_thumb_dir, "thumb.jpg")
    ffmpeg_bin = getattr(config, "FFMPEG_PATH", "ffmpeg")
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            ffmpeg_bin,
            "-y",
            "-ss",
            "00:00:01",
            "-i",
            file_path,
            "-vframes",
            "1",
            "-q:v",
            "2",
            _tp,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await asyncio.wait_for(proc.communicate(), timeout=30)
        except TimeoutError:
            logger.warning("probe_video_for_delivery: thumbnail ffmpeg timed out for %s, killing process", file_path)
            with contextlib.suppress(Exception):
                proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()
            _shutil.rmtree(_thumb_dir, ignore_errors=True)
            return video_meta, None
        if proc.returncode == 0 and os.path.exists(_tp) and os.path.getsize(_tp) > 0:
            thumb_path = _tp
        else:
            _shutil.rmtree(_thumb_dir, ignore_errors=True)
    except Exception:
        logger.debug("probe_video_for_delivery: thumbnail generation failed for %s", file_path)
        if proc is not None:
            with contextlib.suppress(Exception):
                proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()
        with contextlib.suppress(Exception):
            _shutil.rmtree(_thumb_dir, ignore_errors=True)

    return video_meta, thumb_path
