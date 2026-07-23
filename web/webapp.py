import asyncio
import json
import logging
import os
import re
import subprocess
import threading
import traceback
import uuid

import redis as redis_sync
from flask import Flask, jsonify, request, send_file, send_from_directory
from flask_cors import CORS

import config
from utils.url_validation import _validate_url_safe

# Rate limiting for DoS/DDoS protection (token bucket per-endpoint per-IP)
from utils.web_rate_limiter import get_client_ip, make_rate_limit_response, web_rate_limiter

try:
    import boto3
except Exception:
    boto3 = None

try:
    from utils.storage import get_storage_backend_sync
except Exception:
    get_storage_backend_sync = None

app = Flask(__name__, static_folder="static")
CORS(app)

logger = logging.getLogger(__name__)

# storage paths
INPUT_DIR = getattr(config, "INPUT_PATH", "storage/input")
OUTPUT_DIR = getattr(config, "OUTPUT_PATH", "storage/output")
os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Optional upload size cap (bytes) to protect memory from huge multipart uploads.
try:
    max_bytes = int(os.environ.get("MAX_CONTENT_LENGTH_BYTES", "0") or 0)
    if max_bytes and max_bytes > 0:
        app.config["MAX_CONTENT_LENGTH"] = max_bytes
except Exception:
    pass

# In-memory fallback job store when Redis is not available (best-effort)
JOB_STORE = {}

# Try to use async job queue helpers when available
try:
    from utils.job_queue import enqueue_job, get_redis
    aioredis_available = True
except Exception:
    enqueue_job = None
    get_redis = None
    aioredis_available = False

import contextlib

from flask import Response, stream_with_context

from utils import file_utils


@app.route("/", methods=["GET"])
def index():

    # Rate limiting: prevent DoS/DDoS
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("index", client_ip):
        body, status, headers = make_rate_limit_response("index", client_ip)
        return jsonify(body), status, headers

    return send_from_directory(app.static_folder, "index.html")


@app.route("/upload", methods=["POST"])
def upload():

    # Rate limiting: prevent DoS/DDoS
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("upload", client_ip):
        body, status, headers = make_rate_limit_response("upload", client_ip)
        return jsonify(body), status, headers

    # Optional upload token protection: when `UPLOAD_SECRET` is set in the
    # environment, require callers to include an `X-Upload-Token` header or
    # provide `upload_token` as a form/query parameter with the same value.
    upload_secret = os.environ.get("UPLOAD_SECRET")
    if upload_secret:
        incoming_token = (
            request.headers.get("X-Upload-Token")
            or request.form.get("upload_token")
            or request.args.get("upload_token")
        )
        if not incoming_token or incoming_token != upload_secret:
            return (
                jsonify({"error": "unauthorized", "detail": "missing or invalid upload token"}),
                401,
            )

    forward_hash = request.form.get("forward_hash") or request.args.get("forward_hash")
    f = request.files.get("file")
    # Require either a file upload or a forward_hash that the server can fetch
    if not f and not forward_hash:
        return jsonify({"error": "no file or forward_hash provided"}), 400

    # Allow empty filenames (e.g. telegram quick-preview). We'll try to
    # detect/sanitize a sensible original filename after saving.
    job_id = str(uuid.uuid4())
    # Correlation id for this HTTP request (carried into job for tracing)
    request_id = str(uuid.uuid4())
    if f:
        filename = f.filename or ""
        ext = os.path.splitext(filename)[1] or ".mp4"
        input_path = os.path.join(INPUT_DIR, f"{job_id}{ext}")
        f.save(input_path)
        # If configured with remote storage (S3/R2/MinIO), we'll upload the input
        # in a background thread and enqueue the job after upload completes.
        input_key = None
        backend_name = (os.getenv("STORAGE_BACKEND") or getattr(config, "STORAGE_BACKEND", "local")).lower()
        use_remote_backend = backend_name in ("s3", "r2") and get_storage_backend_sync is not None
        key = f"uploads/{job_id}_{os.path.basename(input_path)}" if use_remote_backend else None
    else:
        # Attempt to fetch forwarded message metadata persisted earlier
        with contextlib.suppress(Exception):
            logger.info("webapp.upload: received forward_hash=%s", forward_hash)
        try:
            from utils.forward_store import load_forward_metadata
        except Exception:
            return jsonify({"error": "failed to load forward metadata", "detail": "Check server logs for details."}), 500

        # Try to load persisted forward metadata. If it's not yet available
        # (e.g. upload to S3 is still in progress), schedule a background
        # poll that will retry fetching the metadata and then perform the
        # same fetch+enqueue flow once metadata becomes available. Return
        # HTTP 202 Accepted to indicate the request was accepted for
        # processing but is not complete yet.
        try:
            meta = asyncio.run(load_forward_metadata(forward_hash))
        except Exception:
            return jsonify({"error": "failed to load forward metadata", "detail": "Check server logs for details."}), 500

        if not meta:
            # default extension while we wait for metadata to appear
            ext = ".mp4"
            input_path = os.path.join(INPUT_DIR, f"{job_id}{ext}")

            def _poll_and_fetch(fid, inp_path, j_id, req_id, attempts=6, initial_delay=2):
                import asyncio as _asyncio
                import time
                try:
                    from utils.forward_store import delete_forward_metadata as _delete_forward
                    from utils.forward_store import load_forward_metadata as _load_forward
                except Exception:
                    logger.exception("Background poll: forward_store helpers unavailable")
                    return

                # Attempt to locate the forward metadata with exponential backoff
                for attempt in range(attempts):
                    try:
                        m = _asyncio.run(_load_forward(fid))
                    except Exception:
                        m = None

                    if m:
                        # import userbot downloader lazily inside thread
                        try:
                            from utils.userbot_downloader import download_forward_via_userbot
                        except Exception:
                            logger.exception("Background poll: userbot_downloader not available for %s", fid)
                            return

                        try:
                            ok_loc = _asyncio.run(
                                download_forward_via_userbot(
                                    m.get("chat_id"),
                                    m.get("message_id") or m.get("msg_id"),
                                    inp_path,
                                    msg_date=m.get("registered_at") or m.get("created_at"),
                                    file_unique_id=m.get("file_unique_id"),
                                )
                            )
                        except Exception:
                            logger.exception("Background userbot download failed for forward %s", m.get("chat_id"))
                            return

                        if not ok_loc or not os.path.exists(inp_path):
                            logger.error("Background userbot download did not produce file: %s", inp_path)
                            return

                        # Upload to remote storage if configured, else enqueue using local path
                        backend_name_loc = (os.getenv("STORAGE_BACKEND") or getattr(config, "STORAGE_BACKEND", "local")).lower()
                        use_remote_loc = backend_name_loc in ("s3", "r2") and get_storage_backend_sync is not None
                        key_loc = f"uploads/{j_id}_{os.path.basename(inp_path)}" if use_remote_loc else None
                        job_loc = None
                        if use_remote_loc and key_loc:
                            try:
                                b = get_storage_backend_sync()
                                _asyncio.run(b.upload_file(inp_path, key_loc))
                                if os.environ.get("KEEP_LOCAL_UPLOADS", "").lower() not in ("1", "true", "yes"):
                                    with contextlib.suppress(Exception):
                                        os.remove(inp_path)
                                job_loc = {
                                    "job_id": j_id,
                                    "input_key": key_loc,
                                    "output_path": os.path.join(OUTPUT_DIR, f"{j_id}.mp4"),
                                    "original_filename": m.get("name") or os.path.basename(inp_path),
                                    "output_filename": f"{j_id}.mp4",
                                    "ffmpeg_args": ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
                                    "progress_channel": f"ffmpeg:progress:{j_id}",
                                    "cleanup_input": True,
                                }
                            except Exception:
                                logger.exception("Background upload failed for fetched forward %s", inp_path)

                        if job_loc is None:
                            job_loc = {
                                "job_id": j_id,
                                "input_path": inp_path,
                                "output_path": os.path.join(OUTPUT_DIR, f"{j_id}.mp4"),
                                "original_filename": m.get("name") or os.path.basename(inp_path),
                                "output_filename": f"{j_id}.mp4",
                                "ffmpeg_args": ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
                                "progress_channel": f"ffmpeg:progress:{j_id}",
                                "cleanup_input": True,
                            }

                        job_loc["request_id"] = req_id
                        try:
                            _asyncio.run(enqueue_job(job_loc))
                        except Exception:
                            logger.exception("Failed to enqueue background fetched job %s", j_id)

                        # cleanup forward metadata to avoid duplicates
                        with contextlib.suppress(Exception):
                            _asyncio.run(_delete_forward(fid))

                        return

                    # not found yet: backoff then retry
                    with contextlib.suppress(Exception):
                        time.sleep(initial_delay * (2 ** attempt))

                logger.warning("Forward metadata still not found after %s attempts for %s", attempts, fid)

            t = threading.Thread(target=_poll_and_fetch, args=(forward_hash, input_path, job_id, request_id), daemon=True)
            with contextlib.suppress(Exception):
                logger.info("webapp.upload: starting background poll thread for forward %s -> %s", forward_hash, input_path)
            t.start()
            return jsonify({"status": "accepted", "detail": "forward metadata not yet available; background fetch scheduled"}), 202

        # Determine extension from original metadata or fallback to .mp4
        filename = meta.get("name") or ""
        ext = os.path.splitext(filename)[1] or ".mp4"
        input_path = os.path.join(INPUT_DIR, f"{job_id}{ext}")

        # Try to download using an opt-in userbot (Telethon) if available
        try:
            from utils.userbot_downloader import download_forward_via_userbot
        except Exception:
            return jsonify({"error": "userbot_downloader not available on server"}), 500

        # Schedule a background thread to download the forwarded media via
        # the opt-in userbot and then enqueue a metadata-only job. Keep this
        # implementation simple to avoid deep nested try/except blocks which
        # previously caused indentation/syntax issues.
        def _bg_fetch_and_enqueue(meta_obj, inp_path, j_id, req_id):
            import asyncio as _asyncio
            try:
                ok_loc = False
                try:
                    ok_loc = _asyncio.run(
                        download_forward_via_userbot(
                            meta_obj.get("chat_id"),
                            meta_obj.get("message_id") or meta_obj.get("msg_id"),
                            inp_path,
                            msg_date=meta_obj.get("registered_at") or meta_obj.get("created_at"),
                            file_unique_id=meta_obj.get("file_unique_id"),
                        )
                    )
                except Exception:
                    logger.exception("Background userbot download failed for forward %s", meta_obj.get("chat_id"))
                    return

                if not ok_loc or not os.path.exists(inp_path):
                    logger.error("Background userbot download did not produce file: %s", inp_path)
                    return

                # If configured, upload the input to remote storage and enqueue
                backend_name_loc = (os.getenv("STORAGE_BACKEND") or getattr(config, "STORAGE_BACKEND", "local")).lower()
                use_remote_loc = backend_name_loc in ("s3", "r2") and get_storage_backend_sync is not None
                key_loc = f"uploads/{j_id}_{os.path.basename(inp_path)}" if use_remote_loc else None
                if use_remote_loc and key_loc:
                    try:
                        b = get_storage_backend_sync()
                        _asyncio.run(b.upload_file(inp_path, key_loc))
                        if os.environ.get("KEEP_LOCAL_UPLOADS", "").lower() not in ("1", "true", "yes"):
                            with contextlib.suppress(Exception):
                                os.remove(inp_path)
                        job_loc = {
                            "job_id": j_id,
                            "input_key": key_loc,
                            "output_path": os.path.join(OUTPUT_DIR, f"{j_id}.mp4"),
                            "original_filename": meta_obj.get("name") or os.path.basename(inp_path),
                            "output_filename": f"{j_id}.mp4",
                            "ffmpeg_args": ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
                            "progress_channel": f"ffmpeg:progress:{j_id}",
                            "cleanup_input": True,
                        }
                    except Exception:
                        logger.exception("Background upload failed for fetched forward %s", inp_path)
                        job_loc = {
                            "job_id": j_id,
                            "input_path": inp_path,
                            "output_path": os.path.join(OUTPUT_DIR, f"{j_id}.mp4"),
                            "original_filename": meta_obj.get("name") or os.path.basename(inp_path),
                            "output_filename": f"{j_id}.mp4",
                            "ffmpeg_args": ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
                            "progress_channel": f"ffmpeg:progress:{j_id}",
                            "cleanup_input": True,
                        }
                else:
                    job_loc = {
                        "job_id": j_id,
                        "input_path": inp_path,
                        "output_path": os.path.join(OUTPUT_DIR, f"{j_id}.mp4"),
                        "original_filename": meta_obj.get("name") or os.path.basename(inp_path),
                        "output_filename": f"{j_id}.mp4",
                        "ffmpeg_args": ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
                        "progress_channel": f"ffmpeg:progress:{j_id}",
                        "cleanup_input": True,
                    }

                job_loc["request_id"] = req_id
                try:
                    _asyncio.run(enqueue_job(job_loc))
                except Exception:
                    logger.exception("Failed to enqueue background fetched job %s", j_id)
            except Exception:
                logger.exception("Unexpected error in background fetch/enqueue for forward %s", meta_obj.get("chat_id"))

        t = threading.Thread(target=_bg_fetch_and_enqueue, args=(meta, input_path, job_id, request_id), daemon=True)
        t.start()
        return jsonify({"job_id": job_id})

    # Detect or sanitize the original filename. Prefer the uploaded name,
    # otherwise probe the file to derive a sensible name.
    try:
        # Run sanitization/detection synchronously but keep it lightweight.
        if filename:
            original_filename = asyncio.run(file_utils.sanitize_filename(filename))
        else:
            original_filename = asyncio.run(file_utils.detect_filename(input_path))
    except Exception:
        original_filename = os.path.basename(input_path)

    # Prefer mp4 preview output; keep the original base name.
    base_name = os.path.splitext(original_filename)[0]
    output_filename = f"{base_name}.mp4"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    # Avoid clobbering existing files
    counter = 1
    while os.path.exists(output_path):
        output_filename = f"{base_name}_{counter}.mp4"
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        counter += 1

    logger.info(f"Enqueue job {job_id}: input={input_path} output={output_path} original={original_filename}")

    job = {
        "job_id": job_id,
        "input_path": input_path,
        # when using remote storage the worker will download `input_key` before processing
        **({"input_key": input_key} if input_key else {}),
        "output_path": output_path,
        "original_filename": original_filename,
        "output_filename": os.path.basename(output_path),
        "ffmpeg_args": ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
        "progress_channel": f"ffmpeg:progress:{job_id}",
        "cleanup_input": True,
        "cleanup_output": False,
    }
    # Enqueue job: do not block the request thread for uploads/enqueues.
    if enqueue_job:
        def _background_upload_and_enqueue(j, key, use_remote, req_id):
            try:
                # If remote backend is enabled, upload first then set input_key
                if use_remote and key:
                    b = get_storage_backend_sync()
                    # run the async upload in this background thread
                    try:
                        import asyncio as _asyncio

                        _asyncio.run(b.upload_file(input_path, key))
                        if os.environ.get("KEEP_LOCAL_UPLOADS", "").lower() not in ("1", "true", "yes"):
                            with contextlib.suppress(Exception):
                                os.remove(input_path)
                    except Exception:
                        logger.exception("Background upload failed for %s", input_path)
                        # fallthrough; enqueue with local path as a fallback
                    else:
                        j["input_key"] = key

                # attach request id for tracing
                try:
                    j["request_id"] = req_id
                except Exception:
                    j["request_id"] = None

                # enqueue the job (async helper run inside this thread)
                try:
                    import asyncio as _asyncio

                    _asyncio.run(enqueue_job(j))
                except Exception:
                    logger.exception("Background enqueue failed for job %s", j.get("job_id"))
            except Exception:
                logger.exception("Unexpected error in background upload/enqueue for job %s", j.get("job_id"))

        t = threading.Thread(target=_background_upload_and_enqueue, args=(job, key, use_remote_backend, request_id), daemon=True)
        t.start()
    else:
        # Fallback: start a background thread that runs a synchronous conversion
        try:
            from media_converter import ExtendedMediaConverter

            def _worker(j):
                jid = j["job_id"]
                JOB_STORE[jid] = {"job_id": jid, "progress": 0.0, "status": "processing"}
                try:
                    conv = ExtendedMediaConverter()
                    loop = asyncio.new_event_loop()
                    try:
                        asyncio.set_event_loop(loop)
                        # Use optimize_video as a sensible default to produce MP4 preview
                        success = loop.run_until_complete(conv.optimize_video(j["input_path"], j["output_path"]))
                    finally:
                        with contextlib.suppress(Exception):
                            loop.close()

                    if success and os.path.exists(j["output_path"]):
                        JOB_STORE[jid]["progress"] = 100.0
                        JOB_STORE[jid]["status"] = "done"
                        JOB_STORE[jid]["output"] = j["output_path"]
                        JOB_STORE[jid]["message"] = "done"
                    else:
                        JOB_STORE[jid]["progress"] = 0.0
                        JOB_STORE[jid]["status"] = "error"
                        JOB_STORE[jid]["message"] = "conversion_failed"
                except Exception as ex:
                    JOB_STORE[jid]["progress"] = 0.0
                    JOB_STORE[jid]["status"] = "error"
                    JOB_STORE[jid]["message"] = str(ex)

            t = threading.Thread(target=_worker, args=(job,), daemon=True)
            t.start()
        except Exception:
            return jsonify({"error": "job queue not available on server", "detail": "Internal error. Check server logs."}), 503
    return jsonify({"job_id": job_id})


@app.route("/debug/telethon-log", methods=["GET"])
def telethon_log():
    """
    # Rate limiting: prevent DoS/DDoS
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("debug_log", client_ip):
        body, status, headers = make_rate_limit_response("debug_log", client_ip)
        return jsonify(body), status, headers

Fetch the most recent Telethon ingest log from configured storage.

    Protected by `DEBUG_SECRET` if present in env. Returns plain text log
    or 404 if not found.
    """
    debug_secret = os.environ.get("DEBUG_SECRET")
    if debug_secret:
        token = request.headers.get("X-Debug-Token") or request.args.get("debug_token")
        if not token or token != debug_secret:
            return jsonify({"error": "unauthorized"}), 401

    # Prefer synchronous backend helper if present, else attempt async helper
    backend = None
    async_backend = None
    if get_storage_backend_sync is not None:
        try:
            backend = get_storage_backend_sync()
        except Exception as e:
            logger.exception("telethon_log: failed to init sync storage backend: %s", e)
            backend = None
    if backend is None:
        # Try to import async factory and instantiate it via asyncio
        try:
            import asyncio as _asyncio

            from utils.storage import get_storage_backend as _get_async_backend

            try:
                async_backend = _asyncio.run(_get_async_backend())
            except Exception as e:
                logger.exception("telethon_log: failed to init async storage backend: %s", e)
                async_backend = None
        except Exception:
            async_backend = None

    if backend is None and async_backend is None:
        return jsonify({"error": "no available storage backend to fetch logs"}), 500

    # Candidate key prefixes where telethon_ingest uploads logs
    prefixes = ["telethon/", "telethon_ingest/", "telethon/telethon_ingest"]
    # Try to list objects if backend exposes a list method, else try common names
    candidates = []
    try:
        if hasattr(backend, "list_keys"):
            for p in prefixes:
                try:
                    keys = backend.list_keys(prefix=p)
                    if keys:
                        candidates.extend(keys)
                except Exception:
                    pass
    except Exception:
        pass

    # If no candidates discovered, try some well-known names
    if not candidates:
        candidates = [
            "telethon/telethon_ingest.log",
            "telethon/telethon_ingest.started",
            "telethon/telethon_ingest_crash.log",
            "telethon/telethon_ingest.latest.log",
        ]

    # Prefer newest by timestamp encoded in key name if possible
    candidates = sorted(set(candidates), reverse=True)
    for key in candidates:
        try:
            tmpdir = os.path.join("storage", "temp")
            os.makedirs(tmpdir, exist_ok=True)
            dst = os.path.join(tmpdir, os.path.basename(key))
            # Try sync backend first
            if backend is not None:
                try:
                    backend.download_file(key, dst)
                except Exception:
                    try:
                        backend.get_file(key, dst)
                    except Exception:
                        logger.debug("telethon_log: sync backend cannot fetch %s", key)
                        continue
                if os.path.exists(dst):
                    return send_file(dst, mimetype="text/plain", as_attachment=False)
            # Try async backend via asyncio.run
            if async_backend is not None:
                try:
                    import asyncio as _asyncio

                    async def _dl(a_backend, a_key, a_dst):
                        try:
                            await a_backend.download_file(a_key, a_dst)
                            return True
                        except Exception:
                            try:
                                if hasattr(a_backend, "get_file"):
                                    await a_backend.get_file(a_key, a_dst)
                                    return True
                            except Exception:
                                return False
                        return False

                    ok = _asyncio.run(_dl(async_backend, key, dst))
                    if ok and os.path.exists(dst):
                        return send_file(dst, mimetype="text/plain", as_attachment=False)
                except Exception:
                    logger.exception("telethon_log: async backend failed to fetch %s", key)
                    continue
        except Exception:
            logger.exception("telethon_log: failed to fetch %s", key)
            continue

    return jsonify({"error": "telethon log not found in storage"}), 404


@app.route("/presign", methods=["POST"])
def presign():
    """
    # Rate limiting: prevent DoS/DDoS
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("presign", client_ip):
        body, status, headers = make_rate_limit_response("presign", client_ip)
        return jsonify(body), status, headers

Return a presigned S3 POST (or PUT) for client direct upload.

    Request JSON or form data: `filename`.
    Requires `UPLOAD_SECRET` when configured.
    """
    upload_secret = os.environ.get("UPLOAD_SECRET")
    if upload_secret:
        incoming_token = (
            request.headers.get("X-Upload-Token")
            or request.form.get("upload_token")
            or request.args.get("upload_token")
        )
        if not incoming_token or incoming_token != upload_secret:
            return (
                jsonify({"error": "unauthorized", "detail": "missing or invalid upload token"}),
                401,
            )

    filename = None
    try:
        data = request.get_json(silent=True) or {}
        filename = data.get("filename") or request.form.get("filename") or request.args.get("filename")
    except Exception:
        filename = request.form.get("filename") or request.args.get("filename")

    if not filename:
        return jsonify({"error": "filename required"}), 400

    if not boto3:
        return jsonify({"error": "boto3 not available on server"}), 501

    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        return jsonify({"error": "S3_BUCKET not configured"}), 500

    key = f"uploads/{uuid.uuid4().hex}_{os.path.basename(filename)}"

    try:
        # Build boto3 client kwargs and optional botocore Config for path-style addressing
        client_kwargs = {
            "region_name": os.environ.get("S3_REGION") or None,
            "endpoint_url": os.environ.get("S3_ENDPOINT") or None,
            "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            "aws_session_token": os.environ.get("AWS_SESSION_TOKEN") or None,
        }
        try:
            from botocore.config import Config as BotoConfig
            force_path = str(os.getenv("S3_FORCE_PATH_STYLE", "")).lower() in ("1", "true", "yes")
            if force_path:
                client_kwargs["config"] = BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"})
            else:
                client_kwargs["config"] = BotoConfig(signature_version="s3v4")
        except Exception:
            # botocore not available or config creation failed — proceed without explicit config
            pass

        s3 = boto3.client("s3", **client_kwargs)
        # prefer a POST form upload (fields + url)
        post = s3.generate_presigned_post(Bucket=bucket, Key=key, ExpiresIn=3600)
        # also provide a presigned GET url for later worker download reference
        get_url = s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600 * 24)
        return jsonify({"method": "POST", "url": post["url"], "fields": post["fields"], "key": key, "get_url": get_url})
    except Exception as e:
        logger.exception("Presign failed: %s", e)
    return jsonify({"error": "Presign failed. Check server logs."}), 500


@app.route("/enqueue_from_url", methods=["POST"])
def enqueue_from_url():
    """
    # Rate limiting: prevent DoS/DDoS
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("enqueue_url", client_ip):
        body, status, headers = make_rate_limit_response("enqueue_url", client_ip)
        return jsonify(body), status, headers

Enqueue a job that downloads from a public or presigned URL.

    Request JSON: `source_url` (required), `original_filename` (optional).
    Requires `UPLOAD_SECRET` when configured.
    """
    upload_secret = os.environ.get("UPLOAD_SECRET")
    if upload_secret:
        incoming_token = (
            request.headers.get("X-Upload-Token")
            or request.json.get("upload_token") if request.is_json else request.form.get("upload_token")
            or request.args.get("upload_token")
        )
        if not incoming_token or incoming_token != upload_secret:
            return (
                jsonify({"error": "unauthorized", "detail": "missing or invalid upload token"}),
                401,
            )

    data = request.get_json(silent=True) or {}
    source_url = data.get("source_url") or request.form.get("source_url") or request.args.get("source_url")
    original_filename = data.get("original_filename") or request.form.get("original_filename") or request.args.get("original_filename")

    if not source_url:
        return jsonify({"error": "source_url required"}), 400

    # SSRF protection: validate URL against private/loopback IPs
    if not _validate_url_safe(source_url):
        logger.warning("SSRF blocked: source_url=%s", source_url[:120] if source_url else "None")
        return jsonify({"error": "invalid source_url", "detail": "URL blocked for security reasons"}), 400


    job_id = str(uuid.uuid4())
    output_filename = (os.path.splitext(original_filename or job_id)[0] + ".mp4") if original_filename else f"{job_id}.mp4"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    job = {
        "job_id": job_id,
        "source_url": source_url,
        "output_path": output_path,
        "original_filename": original_filename or output_filename,
        "output_filename": output_filename,
        "ffmpeg_args": ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
        "progress_channel": f"ffmpeg:progress:{job_id}",
        "cleanup_input": True,
    }

    if enqueue_job:
        # Enqueue in background thread to avoid blocking the request thread
        def _bg_enqueue(j, req_id):
            try:
                try:
                    j["request_id"] = req_id
                except Exception:
                    j["request_id"] = None
                import asyncio as _asyncio

                _asyncio.run(enqueue_job(j))
            except Exception:
                logger.exception("Background enqueue failed for URL job %s", j.get("job_id"))

        t = threading.Thread(target=_bg_enqueue, args=(job, str(uuid.uuid4())), daemon=True)
        t.start()
        return jsonify({"job_id": job_id})
    else:
        return jsonify({"error": "job queue not available on server"}), 503


async def _get_job_hash(job_id: str):
    if not get_redis:
        return None
    try:
        r = await get_redis()
        data = await r.hgetall(f"ffmpeg:job:{job_id}")
        await r.close()
        if not data:
            return None
        decoded = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v for k, v in data.items()}
        return decoded
    except Exception:
        return None


@app.route("/status/<job_id>", methods=["GET"])
def status(job_id):

    # Rate limiting: prevent DoS/DDoS
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("status", client_ip):
        body, status, headers = make_rate_limit_response("status", client_ip)
        return jsonify(body), status, headers

    # Try to read Redis job hash
    try:
        job_hash = asyncio.run(_get_job_hash(job_id)) if aioredis_available else None
    except Exception:
        job_hash = None

    if job_hash:
        # normalize numeric fields coming from Redis (strings)
        progress = float(job_hash.get("progress") or 0.0)
        message = job_hash.get("message") or "queued"
        status = job_hash.get("status") or ("done" if job_hash.get("output") else "processing")
        out = job_hash.get("output")
        out_bytes = None
        in_bytes = None
        progress_by_size = None
        try:
            if job_hash.get("out_bytes") is not None:
                out_bytes = int(job_hash.get("out_bytes"))
        except Exception:
            out_bytes = None
        try:
            if job_hash.get("in_bytes") is not None:
                in_bytes = int(job_hash.get("in_bytes"))
        except Exception:
            in_bytes = None
        try:
            if job_hash.get("progress_by_size") is not None:
                progress_by_size = float(job_hash.get("progress_by_size"))
        except Exception:
            progress_by_size = None

        # Prefer explicit output_get_url/output_key when available
        output_key = job_hash.get("output_key")
        output_get_url = job_hash.get("output_get_url")
        out = job_hash.get("output")

        output_url = None
        if output_get_url:
            output_url = output_get_url
        elif output_key:
            # Try to generate a presigned GET if backend helper available
            try:
                if get_storage_backend_sync is not None:
                    backend = get_storage_backend_sync()
                    try:
                        output_url = asyncio.run(backend.generate_presigned_get(output_key))
                    except Exception:
                        output_url = None
            except Exception:
                output_url = None

        # If we have an available URL, prefer that for the `output` field
        if output_url:
            out = output_url

        resp = {"job_id": job_id, "progress": progress, "message": message, "status": status, "output": out}
        if output_key:
            resp["output_key"] = output_key
        if output_get_url:
            resp["output_get_url"] = output_get_url
        if output_url and not output_get_url:
            resp["output_url"] = output_url

        if out_bytes is not None:
            resp["out_bytes"] = out_bytes
        if in_bytes is not None:
            resp["in_bytes"] = in_bytes
        if progress_by_size is not None:
            resp["progress_by_size"] = progress_by_size

        return jsonify(resp)

    # Fallback: check in-memory JOB_STORE (when Redis/job queue not available)
    try:
        local = JOB_STORE.get(job_id)
    except Exception:
        local = None
    if local:
        resp = {
            "job_id": job_id,
            "progress": float(local.get("progress", 0.0)),
            "message": local.get("message", "processing" if local.get("status") != "done" else "done"),
            "status": local.get("status", "processing"),
            "output": local.get("output"),
        }
        return jsonify(resp)

    # Fallback: check if output file exists
    out_path = os.path.join(OUTPUT_DIR, f"{job_id}.mp4")
    if os.path.exists(out_path):
        return jsonify({"job_id": job_id, "progress": 100.0, "message": "done", "status": "done", "output": out_path})

    # otherwise queued
    return jsonify({"job_id": job_id, "progress": 0.0, "message": "queued", "status": "queued"})



@app.route("/download/<job_id>", methods=["GET"])
def download(job_id):

    # Rate limiting: prevent DoS/DDoS
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("download", client_ip):
        body, status, headers = make_rate_limit_response("download", client_ip)
        return jsonify(body), status, headers

    # Check Redis for output path
    try:
        job_hash = asyncio.run(_get_job_hash(job_id)) if aioredis_available else None
    except Exception:
        job_hash = None

    if job_hash:
        # If a presigned URL exists return a redirect to it
        try:
            url = job_hash.get("output_get_url")
            if url:
                from flask import redirect

                return redirect(url)
        except Exception:
            pass

        # If output is a local path we can send it directly
        try:
            output_val = job_hash.get("output")
            if output_val and os.path.exists(output_val):
                output_path = output_val
                filename = job_hash.get("output_filename") or os.path.basename(output_path)
                try:
                    return send_file(output_path, as_attachment=True, download_name=filename)
                except TypeError:
                    return send_file(output_path, as_attachment=True, attachment_filename=filename)
        except Exception:
            pass

        # If storage key present, attempt to generate presigned GET and redirect
        try:
            output_key = job_hash.get("output_key")
            if output_key and get_storage_backend_sync is not None:
                backend = get_storage_backend_sync()
                try:
                    url = asyncio.run(backend.generate_presigned_get(output_key))
                    from flask import redirect

                    return redirect(url)
                except Exception:
                    pass
        except Exception:
            pass

    # Check in-memory JOB_STORE for output path
    try:
        local = JOB_STORE.get(job_id)
    except Exception:
        local = None
    if local and local.get("output") and os.path.exists(local.get("output")):
        try:
            return send_file(local.get("output"), as_attachment=True, download_name=os.path.basename(local.get("output")))
        except TypeError:
            return send_file(local.get("output"), as_attachment=True, attachment_filename=os.path.basename(local.get("output")))

    # Fallback: look for storage/output/{job_id}.mp4
    out_path = os.path.join(OUTPUT_DIR, f"{job_id}.mp4")
    if os.path.exists(out_path):
        try:
            return send_file(out_path, as_attachment=True, download_name=os.path.basename(out_path))
        except TypeError:
            return send_file(out_path, as_attachment=True, attachment_filename=os.path.basename(out_path))

    return jsonify({"error": "output not available"}), 404


@app.route('/events/<job_id>')
def events(job_id):
    """
    # Rate limiting: prevent DoS/DDoS
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("events", client_ip):
        body, status, headers = make_rate_limit_response("events", client_ip)
        return jsonify(body), status, headers

Server-Sent Events endpoint that streams Redis progress pubsub messages
    published on channel `ffmpeg:progress:{job_id}` to the browser.
    """
    def gen():
        pub = None
        try:
            # initial state
            try:
                job_hash = asyncio.run(_get_job_hash(job_id)) if aioredis_available else None
            except Exception:
                job_hash = None

            if job_hash:
                yield f"data: {json.dumps(job_hash)}\n\n"

            red_url = os.environ.get('REDIS_URL')
            if not red_url:
                # No Redis configured for this deployment; finish after initial state
                return
            r = redis_sync.from_url(red_url, decode_responses=True)
            pub = r.pubsub(ignore_subscribe_messages=True)
            channel = f"ffmpeg:progress:{job_id}"
            pub.subscribe(channel)
            for message in pub.listen():
                if not message:
                    continue
                if message.get('type') != 'message':
                    continue
                data = message.get('data')
                # ensure string
                if isinstance(data, bytes):
                    try:
                        data = data.decode('utf-8')
                    except Exception:
                        data = str(data)
                yield f"data: {data}\n\n"
        except GeneratorExit:
            # client disconnected
            pass
        except Exception:
            pass
        finally:
            try:
                if pub:
                    pub.close()
            except Exception:
                pass

    return Response(stream_with_context(gen()), content_type='text/event-stream')


@app.route("/get_input", methods=["GET"]) 
def get_input():
    """
    # Rate limiting: prevent DoS/DDoS
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("get_input", client_ip):
        body, status, headers = make_rate_limit_response("get_input", client_ip)
        return jsonify(body), status, headers

Temporary token-protected endpoint to download files from the input folder.
    Protection: prefer `DIAG_TOKEN` (header `X-DIAG-TOKEN` or `?token=`),
    fallback to `UPLOAD_SECRET` (header `X-Upload-Token` or `?upload_token=`).
    Use only for short-term debugging; remove after use.
    Query params: `name` (filename in input dir).
    """
    diag_token = os.environ.get("DIAG_TOKEN")
    upload_secret = os.environ.get("UPLOAD_SECRET")

    incoming_diag = request.headers.get("X-DIAG-TOKEN") or request.args.get("token") or request.form.get("token")
    incoming_upload = request.headers.get("X-Upload-Token") or request.args.get("upload_token") or request.form.get("upload_token")

    # Validate token
    if diag_token:
        if incoming_diag != diag_token:
            return jsonify({"error": "unauthorized"}), 401
    else:
        # if DIAG_TOKEN not set, require upload secret as fallback
        if not upload_secret or incoming_upload != upload_secret:
            return jsonify({"error": "unauthorized (no DIAG_TOKEN configured)"}), 401

    name = request.args.get("name") or request.args.get("filename") or request.form.get("name")
    if not name:
        return jsonify({"error": "name required"}), 400

    # simple sanitization: no path traversal
    if ".." in name or name.startswith("/"):
        return jsonify({"error": "invalid filename"}), 400

    safe_name = os.path.basename(name)
    path = os.path.join(INPUT_DIR, safe_name)
    if not os.path.exists(path) or not os.path.isfile(path):
        return jsonify({"error": "not found"}), 404

    try:
        # send file as attachment
        try:
            return send_file(path, as_attachment=True, download_name=safe_name)
        except TypeError:
            return send_file(path, as_attachment=True, attachment_filename=safe_name)
    except Exception as e:
        logger.exception("Failed to send file: %s", e)
    return jsonify({"error": "Failed to send file. Check server logs."}), 500


@app.route("/internal/diag", methods=["GET", "POST"]) 
def internal_diag():
    """
    # Rate limiting: prevent DoS/DDoS
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("diag", client_ip):
        body, status, headers = make_rate_limit_response("diag", client_ip)
        return jsonify(body), status, headers

Token-protected diagnostic endpoint.
    Set `DIAG_TOKEN` in the environment (random string). Call with header
    `X-DIAG-TOKEN: <token>` or `?token=<token>`.
    Returns masked env, Redis health, sample job list, optional job hash,
    and last lines from app `logs/` directory.
    """
    token = os.environ.get("DIAG_TOKEN")
    incoming = request.headers.get("X-DIAG-TOKEN") or request.args.get("token") or request.form.get("token")
    if not token:
        return jsonify({"error": "DIAG_TOKEN not configured on server"}), 403
    if incoming != token:
        return jsonify({"error": "unauthorized"}), 401

    def mask_redis(u: str):
        if not u:
            return u
        # mask password between : and @
        return re.sub(r"(redis://[^:]*:)[^@]+@", r"\1****@", u)

    result = {"env": {}, "redis": {}, "logs": {}, "ps": None}
    # minimal env snapshot (mask secrets)
    for k in ("REDIS_URL", "WEB_UPLOAD_URL", "UPLOAD_SECRET", "S3_BUCKET", "AWS_ACCESS_KEY_ID"):
        v = os.environ.get(k)
        result["env"][k] = mask_redis(v) if k == "REDIS_URL" else ("****" if k == "UPLOAD_SECRET" and v else v)

    # Redis checks
    red_url = os.environ.get("REDIS_URL")
    if red_url:
        try:
            r = redis_sync.from_url(red_url, decode_responses=True)
            result["redis"]["ping"] = r.ping()
            try:
                result["redis"]["ffmpeg_jobs"] = r.lrange("ffmpeg:jobs", 0, 50)
            except Exception:
                result["redis"]["ffmpeg_jobs"] = []
            try:
                keys = r.keys("ffmpeg:job:*")
                result["redis"]["job_keys_count"] = len(keys)
                result["redis"]["job_keys_sample"] = keys[:50]
            except Exception:
                result["redis"]["job_keys_count"] = 0
            # optional job hash lookup
            job = request.args.get("job_id") or request.form.get("job_id")
            if job:
                try:
                    result["redis"]["job_hash"] = r.hgetall(f"ffmpeg:job:{job}")
                except Exception:
                    result["redis"]["job_hash"] = {}
        except Exception as e:
            result["redis"]["error"] = str(e)
    else:
        result["redis"]["error"] = "REDIS_URL not set"

    # tail local logs (project logs/ folder)
    try:
        logs_dir = os.path.join(os.getcwd(), "logs")
        if os.path.isdir(logs_dir):
            for fname in sorted(os.listdir(logs_dir))[-10:]:
                path = os.path.join(logs_dir, fname)
                if os.path.isfile(path):
                    with open(path, encoding="utf-8", errors="replace") as fh:
                        lines = fh.readlines()[-500:]
                        result["logs"][fname] = "".join(lines)
    except Exception:
        result["logs"]["error"] = traceback.format_exc()

    # process listing (best-effort)
    try:
        ps_out = subprocess.check_output(["/bin/ps", "aux"], stderr=subprocess.STDOUT, text=True)
        result["ps"] = "\n".join(ps_out.splitlines()[:200])
    except Exception:
        result["ps"] = None

    return jsonify(result)


if __name__ == "__main__":
    # Start WebSocket server for real-time updates (best-effort)
    try:
        from web.ws_server import start_in_thread

        try:
            ws_host = os.environ.get("WS_HOST", "127.0.0.1")
            start_in_thread(host=ws_host, port=int(os.environ.get("WS_PORT", "6789")))
        except Exception:
            logger.exception("Failed to start WS server in thread")
    except Exception:
        logger.debug("WebSocket server module not available")

    debug_mode = os.environ.get("FLASK_DEBUG", "0").lower() in ("1", "true", "yes")
    flask_host = os.environ.get("FLASK_HOST", "127.0.0.1")
    app.run(host=flask_host, port=int(os.environ.get("PORT", "5000")), debug=debug_mode)
