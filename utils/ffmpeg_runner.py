import asyncio
import os
import shlex
import json
import subprocess
import signal
from typing import Optional, Callable

import config
import logging

logger = logging.getLogger(__name__)

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None


CREATE_NEW_PROCESS_GROUP = 0x00000200 if os.name == "nt" else 0


async def probe_duration(path: str) -> Optional[float]:
    """Probe media duration using ffprobe (sync subprocess wrapped)."""
    ffprobe = getattr(config, "FFMPEG_PATH", "ffmpeg")
    ffprobe = ffprobe.replace("ffmpeg", "ffprobe") if "ffmpeg" in ffprobe else "ffprobe"
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


async def run_ffmpeg(
    input_path: str,
    output_path: str,
    job_id: str,
    ffmpeg_args: Optional[list] = None,
    redis_url: Optional[str] = None,
    progress_channel: Optional[str] = None,
    on_progress: Optional[Callable[[float, str], None]] = None,
):
    """Run ffmpeg asynchronously, publish progress to Redis pubsub and call callback.

    - Builds a command using `config.FFMPEG_PATH`.
    - Uses `-progress pipe:1` to read key=value progress lines.
    - Publishes JSON updates to `progress_channel` (if provided) and writes job hash `ffmpeg:job:{job_id}`.
    """
    ffmpeg_bin = getattr(config, "FFMPEG_PATH", "ffmpeg") or "ffmpeg"
    ffmpeg_args = ffmpeg_args or ["-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"]

    duration = await probe_duration(input_path)

    # try to get input file size
    in_bytes = 0
    try:
        if os.path.exists(input_path):
            loop = asyncio.get_event_loop()
            in_bytes = await loop.run_in_executor(None, lambda p=input_path: os.path.getsize(p))
    except Exception:
        in_bytes = 0

    cmd = [ffmpeg_bin, "-y", "-hide_banner", "-loglevel", "error", "-i", input_path] + ffmpeg_args + ["-progress", "pipe:1", "-nostats", output_path]

    logger.info("Running ffmpeg: %s", " ".join(shlex.quote(p) for p in cmd))

    # choose platform-specific creation flags / preexec
    kwargs = {}
    if os.name == "nt":
        kwargs["creationflags"] = CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["preexec_fn"] = os.setsid

    # start process
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, **kwargs)

    redis_client = None
    if aioredis and (redis_url or os.environ.get("REDIS_URL")):
        try:
            redis_client = aioredis.from_url(redis_url or os.environ.get("REDIS_URL"))
            # Initialize job hash so status is available immediately
            try:
                await redis_client.hset(f"ffmpeg:job:{job_id}", mapping={"status": "processing", "progress": 0, "message": "started", "in_bytes": str(in_bytes)})
            except Exception:
                pass
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
                    loop = asyncio.get_event_loop()
                    if os.path.exists(output_path):
                        out_bytes = await loop.run_in_executor(None, lambda p=output_path: os.path.getsize(p))
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
                        store_map = {"progress": payload["progress"], "message": message, "status": "processing", "out_bytes": str(out_bytes), "in_bytes": str(in_bytes)}
                        if progress_by_size is not None:
                            store_map["progress_by_size"] = str(progress_by_size)
                        await redis_client.hset(f"ffmpeg:job:{job_id}", mapping=store_map)
                    except Exception:
                        pass
                if on_progress:
                    try:
                        on_progress(payload["progress"], message)
                    except Exception:
                        pass
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
                            try:
                                proc.kill()
                            except Exception:
                                pass
                        try:
                            await redis_client.hset(f"ffmpeg:job:{job_id}", mapping={"status": "cancelled", "message": "cancelled by user"})
                        except Exception:
                            pass
                        return False, "cancelled"
                except Exception:
                    pass
            elif key == "progress" and val == "end":
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
                        loop = asyncio.get_event_loop()
                        if os.path.exists(output_path):
                            final_out = await loop.run_in_executor(None, lambda p=output_path: os.path.getsize(p))
                    except Exception:
                        final_out = 0

                    finished_map = {"progress": 100, "message": "finished", "status": "done", "output": output_path, "out_bytes": str(final_out), "in_bytes": str(in_bytes), "output_filename": os.path.basename(output_path)}
                    await redis_client.hset(f"ffmpeg:job:{job_id}", mapping=finished_map)
                    if progress_channel:
                        await redis_client.publish(progress_channel, json.dumps({"job_id": job_id, "progress": 100, "message": "finished", "output": output_path, "out_bytes": final_out, "in_bytes": in_bytes}))
                except Exception:
                    pass
            return True, output_path
        else:
            stderr = await proc.stderr.read() if proc.stderr else b""
            err = stderr.decode(errors="ignore")[:1000]
            if redis_client:
                try:
                    await redis_client.hset(f"ffmpeg:job:{job_id}", mapping={"status": "error", "message": err, "out_bytes": str(out_bytes) if 'out_bytes' in locals() else "0", "in_bytes": str(in_bytes)})
                    if progress_channel:
                        await redis_client.publish(progress_channel, json.dumps({"job_id": job_id, "progress": 0, "message": "error", "error": err}))
                except Exception:
                    pass
            return False, err

    except asyncio.CancelledError:
        try:
            proc.kill()
        except Exception:
            pass
        raise
    finally:
        try:
            if redis_client:
                await redis_client.close()
        except Exception:
            pass
