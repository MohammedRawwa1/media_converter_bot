#!/usr/bin/env python3
"""Upload a small test video to configured S3 backend and enqueue a remote-key job.

Usage:
  python3 scripts/upload_and_enqueue_s3_test.py [--key <dest_key>]

Requires: ffmpeg, boto3, redis env vars set (REDIS_URL, S3_BUCKET, S3_ENDPOINT, AWS_*).
"""
import contextlib
import json
import os
import shutil
import subprocess
import sys
import time
import uuid

try:
    import boto3
    import botocore
except Exception:
    print("boto3 is required: pip install boto3")
    sys.exit(2)

try:
    import redis
except Exception:
    print("redis package is required: pip install redis")
    sys.exit(2)

try:
    import config
except Exception:
    config = None

# Default TTL for job metadata in Redis (24 hours)
JOB_METADATA_TTL = int(os.environ.get("JOB_METADATA_TTL", "86400"))


def create_sample(path: str) -> bool:
    ffmpeg = shutil.which("ffmpeg") or os.environ.get("FFMPEG_PATH")
    if not ffmpeg:
        print("ffmpeg not found; please install or set FFMPEG_PATH")
        return False
    cmd = [ffmpeg, "-f", "lavfi", "-i", "testsrc=duration=8:size=640x360:rate=25", "-c:v", "libx264", "-pix_fmt", "yuv420p", path]
    try:
        subprocess.run(cmd, check=True)
        return True
    except Exception as e:
        print("ffmpeg failed to create sample:", e)
        return False


def main():
    dest_key = None
    if len(sys.argv) > 1 and sys.argv[1] == "--key" and len(sys.argv) > 2:
        dest_key = sys.argv[2]

    bucket = os.environ.get("S3_BUCKET")
    endpoint = os.environ.get("S3_ENDPOINT")
    access = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    redis_url = os.environ.get("REDIS_URL")

    if not bucket or not endpoint or not access or not secret:
        print("Missing S3 configuration in environment (S3_BUCKET, S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
        sys.exit(2)
    if not redis_url:
        print("Missing REDIS_URL environment variable")
        sys.exit(2)

    tmp_dir = os.path.join(os.getcwd(), "storage", "temp")
    os.makedirs(tmp_dir, exist_ok=True)
    local_sample = os.path.join(tmp_dir, f"sample_{int(time.time())}.mp4")

    if not create_sample(local_sample):
        sys.exit(3)

    if not dest_key:
        dest_key = f"test_inputs/{os.path.basename(local_sample)}"

    # Prepare boto3 client
    kwargs = {}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if os.environ.get("S3_FORCE_PATH_STYLE", "").lower() in ("1", "true", "yes"):
        kwargs["config"] = botocore.client.Config(s3={'addressing_style': 'path'})

    client = boto3.client("s3", aws_access_key_id=access, aws_secret_access_key=secret, **kwargs)

    try:
        client.upload_file(local_sample, bucket, dest_key)
    except Exception as e:
        print("S3 upload failed:", e)
        sys.exit(4)

    job_id = str(uuid.uuid4())
    output_dir = getattr(config, "OUTPUT_PATH", os.path.join(getattr(config, 'STORAGE_PATH', 'storage'), 'output')) if config else os.path.join("storage", "output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{job_id}.mp4")

    job = {
        "job_id": job_id,
        "input_key": dest_key,
        "output_path": output_path,
        "original_filename": os.path.basename(local_sample),
        "output_filename": os.path.basename(output_path),
        "ffmpeg_args": None,
        "progress_channel": f"ffmpeg:progress:{job_id}",
        "cleanup_input": False,
        "request_id": str(uuid.uuid4()),
    }

    # enqueue via redis
    r = redis.from_url(redis_url, decode_responses=True)
    try:
        mapping = {
            "status": "queued",
            "progress": 0,
            "message": "queued",
            "input": dest_key,
            "input_key": dest_key,
            "output": output_path,
            "created_at": str(time.time()),
            "request_id": job.get("request_id"),
        }
        # create metadata before pushing job to avoid race where worker pops early
        try:
            r.hset(f"ffmpeg:job:{job_id}", mapping=mapping)
            if JOB_METADATA_TTL and JOB_METADATA_TTL > 0:
                with contextlib.suppress(Exception):
                    r.expire(f"ffmpeg:job:{job_id}", JOB_METADATA_TTL)
        except Exception:
            pass
        r.lpush("ffmpeg:jobs", json.dumps(job))
        print("Enqueued remote-key job:")
        print(" JOB_ID:", job_id)
        print(" S3 key:", dest_key)
        print(" Output path:", output_path)
        print(f"To monitor progress: redis-cli -u \"$REDIS_URL\" SUBSCRIBE \"ffmpeg:progress:{job_id}\"")
    except Exception as e:
        print("Failed to enqueue job:", e)
        sys.exit(5)


if __name__ == '__main__':
    main()
