import os
import json
import uuid
import time
import logging
from urllib.parse import urlparse

from flask import Flask, request, jsonify
import redis
import boto3

app = Flask(__name__)
logger = logging.getLogger("fetcher")
logging.basicConfig(level=logging.INFO)

REDIS_URL = os.environ.get("REDIS_URL")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL environment variable is required")

r = redis.from_url(REDIS_URL, decode_responses=True)
UPLOAD_SECRET = os.environ.get("UPLOAD_SECRET")

JOB_LIST = "ffmpeg:jobs"
FETCH_CHANNEL = "ffmpeg:fetch"


def _check_secret(req):
    if not UPLOAD_SECRET:
        return True
    auth = req.headers.get("Authorization") or req.args.get("secret") or req.json.get("secret") if req.is_json else None
    if not auth:
        return False
    if auth.startswith("Bearer "):
        token = auth.split(" ", 1)[1]
    else:
        token = auth
    return token == UPLOAD_SECRET


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/fetch_forward", methods=["POST"])
def fetch_forward():
    if not _check_secret(request):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    forward_hash = data.get("forward_hash")
    if not forward_hash:
        return jsonify({"error": "forward_hash required"}), 400

    job_id = uuid.uuid4().hex
    payload = {
        "job_id": job_id,
        "forward_hash": forward_hash,
        "meta": data.get("meta") or {},
        "created_at": time.time(),
    }
    # publish to a channel so an external Telethon fetcher can pick it up
    r.publish(FETCH_CHANNEL, json.dumps(payload))
    logger.info("Published forward fetch request %s", job_id)
    return jsonify({"status": "ok", "job_id": job_id}), 201


@app.route("/enqueue_from_url", methods=["POST"])
def enqueue_from_url():
    if not _check_secret(request):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json() or {}
    source_url = data.get("url") or data.get("source_url")
    if not source_url:
        return jsonify({"error": "url required"}), 400

    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "source_url": source_url,
        "original_filename": data.get("filename") or data.get("original_name"),
        "type": data.get("type") or "ffmpeg",
        "created_at": time.time(),
    }
    r.lpush(JOB_LIST, json.dumps(job))
    logger.info("Enqueued job from URL %s -> %s", job_id, source_url)
    return jsonify({"status": "ok", "job_id": job_id}), 201


@app.route("/presign", methods=["GET"])
def presign():
    if not _check_secret(request):
        return jsonify({"error": "unauthorized"}), 401
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        return jsonify({"error": "S3_BUCKET not configured"}), 500
    key = request.args.get("key") or request.args.get("filename") or f"uploads/{uuid.uuid4().hex}"
    expires = int(os.environ.get("PRESIGN_EXPIRES", "3600"))

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.environ.get("AWS_SESSION_TOKEN") or None,
            region_name=os.environ.get("AWS_REGION"),
            endpoint_url=os.environ.get("S3_ENDPOINT") or None,
        )
        post = s3.generate_presigned_post(Bucket=bucket, Key=key, ExpiresIn=expires)
        return jsonify({"url": post["url"], "fields": post["fields"], "key": key})
    except Exception as e:
        logger.exception("Failed to create presign: %s", e)
        return jsonify({"error": "presign_failed", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
