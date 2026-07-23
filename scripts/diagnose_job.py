#!/usr/bin/env python3
"""Diagnostics helper for running ffprobe/remux/reencode/tail/redis checks

Designed to be run on the Render instance (or any deployment) to inspect
input files, attempt a fast remux to MKV, or re-encode when ffmpeg fails.

Usage examples:
  python scripts/diagnose_job.py --action ffprobe --file storage/input/name.mp4
  python scripts/diagnose_job.py --action remux --file storage/input/name.mp4
  python scripts/diagnose_job.py --action reencode --file storage/input/name.mp4
  python scripts/diagnose_job.py --action tail_logs --lines 200
  python scripts/diagnose_job.py --action job_info --job_id <JOB_ID>
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from typing import Any

try:
    import redis
except Exception:
    redis = None


def run_cmd(cmd, timeout=300) -> dict[str, Any]:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {"returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    except Exception as e:
        return {"error": str(e)}


def ffprobe_file(path: str) -> dict[str, Any]:
    ffprobe = os.getenv("FFPROBE_PATH") or os.getenv("FFMPEG_PATH", "ffmpeg").replace("ffmpeg", "ffprobe")
    cmd = [ffprobe, "-v", "error", "-print_format", "json", "-show_format", "-show_streams", path]
    return run_cmd(cmd, timeout=60)


def remux_to_mkv(src: str, dst: str) -> dict[str, Any]:
    ffmpeg = os.getenv("FFMPEG_PATH", "ffmpeg")
    cmd = [ffmpeg, "-y", "-hide_banner", "-loglevel", "error", "-i", src, "-c", "copy", dst]
    return run_cmd(cmd, timeout=600)


def reencode(src: str, dst: str) -> dict[str, Any]:
    ffmpeg = os.getenv("FFMPEG_PATH", "ffmpeg")
    cmd = [
        ffmpeg,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-fflags",
        "+genpts",
        "-i",
        src,
        "-c:v",
        "libx264",
        "-preset",
        "fast",
        "-crf",
        "23",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        dst,
    ]
    return run_cmd(cmd, timeout=1800)


def tail_logs(lines: int = 200) -> dict[str, Any]:
    logs = {}
    logs_dir = os.path.join(os.getcwd(), "logs")
    try:
        if os.path.isdir(logs_dir):
            for fname in sorted(os.listdir(logs_dir))[-10:]:
                path = os.path.join(logs_dir, fname)
                if os.path.isfile(path):
                    with open(path, encoding="utf-8", errors="replace") as fh:
                        content = fh.readlines()[-lines:]
                        logs[fname] = "".join(content)
    except Exception as e:
        return {"error": str(e)}
    # include worker log
    try:
        worker_log = os.path.join(tempfile.gettempdir(), "worker.log")
        if os.path.isfile(worker_log):
            with open(worker_log, encoding="utf-8", errors="replace") as fh:
                logs[os.path.basename(worker_log)] = "".join(fh.readlines()[-(lines * 5):])
    except Exception:
        pass
    return {"logs": logs}


def job_info(job_id: str) -> dict[str, Any]:
    red = os.getenv("REDIS_URL")
    if not red:
        return {"error": "REDIS_URL not set"}
    if redis is None:
        return {"error": "redis package not available in runtime"}
    try:
        r = redis.from_url(red, decode_responses=True)
        key = f"ffmpeg:job:{job_id}"
        data = r.hgetall(key)
        return {"job_id": job_id, "job_hash": data}
    except Exception as e:
        return {"error": str(e)}


def ps_top(n: int = 20) -> dict[str, Any]:
    cmd = ["ps", "aux", "--sort=-rss"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        out = proc.stdout.splitlines()[:n]
        return {"ps": out}
    except Exception as e:
        return {"error": str(e)}


def dump_env() -> dict[str, Any]:
    try:
        env = dict(os.environ)
        # return only keys to avoid leaking large/secret values unnecessarily
        return {"env_keys": list(env.keys())}
    except Exception as e:
        return {"error": str(e)}


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--action", required=True, choices=["ffprobe", "remux", "reencode", "tail_logs", "job_info", "ps", "env"]) 
    p.add_argument("--file", help="Path or basename of file in storage/input")
    p.add_argument("--out", help="Output path for remux/reencode (optional)")
    p.add_argument("--lines", type=int, default=200, help="Number of tail lines for logs")
    p.add_argument("--job_id", help="Redis job id to inspect")
    args = p.parse_args(argv)

    if args.action in ("ffprobe", "remux", "reencode") and not args.file:
        print(json.dumps({"error": "--file is required for this action"}))
        sys.exit(2)

    # normalize file path to storage/input when basename provided
    if args.file:
        if os.path.isabs(args.file):
            path = args.file
        else:
            path = os.path.join(os.getcwd(), args.file) if os.path.exists(os.path.join(os.getcwd(), args.file)) else os.path.join(os.getcwd(), "storage", "input", os.path.basename(args.file))
    else:
        path = None

    if args.action == "ffprobe":
        res = ffprobe_file(path)
        print(json.dumps(res))
        return

    if args.action == "remux":
        out = args.out or os.path.join(os.getcwd(), "storage", "temp", os.path.splitext(os.path.basename(path))[0] + ".mkv")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        res = remux_to_mkv(path, out)
        res["output"] = out
        print(json.dumps(res))
        return

    if args.action == "reencode":
        out = args.out or os.path.join(os.getcwd(), "storage", "output", os.path.splitext(os.path.basename(path))[0] + "_reencoded.mp4")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        res = reencode(path, out)
        res["output"] = out
        print(json.dumps(res))
        return

    if args.action == "tail_logs":
        res = tail_logs(args.lines)
        print(json.dumps(res))
        return

    if args.action == "job_info":
        if not args.job_id:
            print(json.dumps({"error": "--job_id required for job_info"}))
            sys.exit(2)
        res = job_info(args.job_id)
        print(json.dumps(res))
        return

    if args.action == "ps":
        res = ps_top()
        print(json.dumps(res))
        return

    if args.action == "env":
        res = dump_env()
        print(json.dumps(res))
        return


if __name__ == "__main__":
    main()
