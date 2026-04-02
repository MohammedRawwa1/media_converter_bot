#!/usr/bin/env python3
"""Enqueue a local ffmpeg job pointing at a local input file.

Usage:
  python scripts/push_local_job.py [path/to/input.mp4]

Creates a job in Redis list `ffmpeg:jobs` and a job hash `ffmpeg:job:<job_id>`.
"""
import os
import sys
import os
import sys
import json
import time
import uuid
import shutil

try:
    import redis
except Exception as e:
    print("redis package is required: pip install redis", e)
    sys.exit(1)

try:
    import config
except Exception:
    config = None


def _ensure_sample_input(path: str):
    # Create parent dir
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # If ffmpeg available, create a short test video
    ffmpeg = shutil.which("ffmpeg") or os.environ.get("FFMPEG_PATH")
    if ffmpeg:
        cmd = [ffmpeg, "-f", "lavfi", "-i", "testsrc=duration=10:size=640x360:rate=25", "-c:v", "libx264", "-pix_fmt", "yuv420p", path]
        try:
            print("Creating sample test video at:", path)
            import subprocess

            subprocess.run(cmd, check=True)
            return True
        except Exception as e:
            print("Failed to create sample file with ffmpeg:", e)
            return False
    else:
        return False


def main():
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)

    input_dir = getattr(config, "INPUT_PATH", "storage/input") if config else "storage/input"
    output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"

    default_input = os.path.join(input_dir, "test.mp4")
    input_path = sys.argv[1] if len(sys.argv) > 1 else default_input

    if not os.path.exists(input_path):
        ok = _ensure_sample_input(input_path)
        if not ok:
            print("Input file not found:", input_path)
            print("Install ffmpeg or create a file at this path and retry.")
            sys.exit(2)

    job_id = str(uuid.uuid4())
    base = os.path.basename(input_path)
    name = os.path.splitext(base)[0]
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{name}_{job_id}.mp4")

    # Normalize to POSIX-style paths for portability across platforms
    try:
        from pathlib import Path
        posix_input = Path(input_path).as_posix()
        posix_output = Path(output_path).as_posix()
    except Exception:
        posix_input = input_path.replace("\\", "/")
        posix_output = output_path.replace("\\", "/")

    job = {
        "job_id": job_id,
        "input_path": posix_input,
        "output_path": posix_output,
        "ffmpeg_args": None,
        "progress_channel": f"ffmpeg:progress:{job_id}",
        "chat_id": 0,
        "caption": "local test",
        "cleanup_input": False,
        "cleanup_output": False,
    }

    try:
        r.lpush("ffmpeg:jobs", json.dumps(job))
        mapping = {
            "status": "queued",
            "progress": "0",
            "message": "queued",
            "input": job.get("input_path") or "",
            "input_key": "",
            "output": job.get("output_path") or "",
            "created_at": str(time.time()),
        }
        r.hset(f"ffmpeg:job:{job_id}", mapping=mapping)
        print("Enqueued job:", job_id)
        print("Input:", input_path)
        print("Output:", output_path)
        print("To watch progress: python scripts/subscribe_progress.py", job_id)
    except Exception as e:
        print("Failed to enqueue job:", e)
        sys.exit(1)


if __name__ == '__main__':
    main()
