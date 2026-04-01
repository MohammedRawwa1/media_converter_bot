import os
import uuid
import asyncio
import json
import logging
from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_cors import CORS

import config
import redis as redis_sync
import threading
import subprocess
import traceback
import re
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

from utils import file_utils
from flask import Response, stream_with_context


@app.route("/", methods=["GET"])
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/upload", methods=["POST"])
def upload():
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
        try:
            from utils.forward_store import load_forward_metadata

            meta = load_forward_metadata(forward_hash)
            if not meta:
                return jsonify({"error": "invalid forward_hash"}), 400
        except Exception as e:
            return jsonify({"error": "failed to load forward metadata", "detail": str(e)}), 500

        # Determine extension from original metadata or fallback to .mp4
        filename = meta.get("name") or ""
        ext = os.path.splitext(filename)[1] or ".mp4"
        input_path = os.path.join(INPUT_DIR, f"{job_id}{ext}")

        # Try to download using an opt-in userbot (Telethon) if available
        try:
            from utils.userbot_downloader import download_forward_via_userbot
        except Exception:
            return jsonify({"error": "userbot_downloader not available on server"}), 500

        try:
            # Run userbot download in a background thread so the HTTP request
            # does not block the server worker thread. The background thread
            # will download the media, optionally upload to remote storage,
            # and enqueue the job when ready.
            def _bg_fetch_and_enqueue(meta_obj, inp_path, j_id, req_id):
                try:
                    import asyncio as _asyncio

                    ok_loc = False
                    try:
                        ok_loc = _asyncio.run(download_forward_via_userbot(meta_obj.get("chat_id"), meta_obj.get("message_id") or meta_obj.get("msg_id"), inp_path, msg_date=meta_obj.get("registered_at") or meta_obj.get("created_at"), file_unique_id=meta_obj.get("file_unique_id")))
                    except Exception:
                        logger.exception("Background userbot download failed for forward %s", meta_obj.get("chat_id"))
                        return

                    if not ok_loc or not os.path.exists(inp_path):
                        logger.error("Background userbot download did not produce file: %s", inp_path)
                        return

                    # Optionally upload to remote storage and then enqueue (reuse logic similar to upload)
                    backend_name_loc = (os.getenv("STORAGE_BACKEND") or getattr(config, "STORAGE_BACKEND", "local")).lower()
                    use_remote_loc = backend_name_loc in ("s3", "r2") and get_storage_backend_sync is not None
                    key_loc = f"uploads/{j_id}_{os.path.basename(inp_path)}" if use_remote_loc else None
                    if use_remote_loc and key_loc:
                        try:
                            b = get_storage_backend_sync()
                            try:
                                _asyncio.run(b.upload_file(inp_path, key_loc))
                                if os.environ.get("KEEP_LOCAL_UPLOADS", "").lower() not in ("1", "true", "yes"):
                                    try:
                                        os.remove(inp_path)
                                    except Exception:
                                        pass
                            except Exception:
                                logger.exception("Background upload of fetched forward failed for %s", inp_path)
                            else:
                                # build job with input_key
                                job_loc = {
                                    "job_id": j_id,
                                    "input_key": key_loc,
                                    "output_path": os.path.join(OUTPUT_DIR, f"{j_id}.mp4"),
                                    "original_filename": meta_obj.get("name") or os.path.basename(inp_path),
                                    "output_filename": f"{j_id}.mp4",
                                    "ffmpeg_args": ["-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
                                    "progress_channel": f"ffmpeg:progress:{j_id}",
                                    "cleanup_input": True,
                                }
                                try:
                                    job_loc["request_id"] = req_id
                                except Exception:
                                    job_loc["request_id"] = None
                                try:
                                    _asyncio.run(enqueue_job(job_loc))
                                except Exception:
                                    logger.exception("Failed to enqueue background fetched job %s", j_id)
                                return

                    # Fallback: enqueue job pointing at local input_path
                    job_loc = {
                        "job_id": j_id,
                        "input_path": inp_path,
                        "output_path": os.path.join(OUTPUT_DIR, f"{j_id}.mp4"),
                        "original_filename": meta_obj.get("name") or os.path.basename(inp_path),
                        "output_filename": f"{j_id}.mp4",
                        "ffmpeg_args": ["-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
                        "progress_channel": f"ffmpeg:progress:{j_id}",
                        "cleanup_input": True,
                    }
                    try:
                        job_loc["request_id"] = req_id
                    except Exception:
                        job_loc["request_id"] = None
                    try:
                        _asyncio.run(enqueue_job(job_loc))
                    except Exception:
                        logger.exception("Failed to enqueue background fetched job %s", j_id)
                except Exception:
                    logger.exception("Unexpected error in background fetch/enqueue for forward %s", meta_obj.get("chat_id"))

            t = threading.Thread(target=_bg_fetch_and_enqueue, args=(meta, input_path, job_id, request_id), daemon=True)
            t.start()
            return jsonify({"job_id": job_id})
        except Exception:
            return jsonify({"error": "failed to schedule userbot fetch"}), 500

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
        "ffmpeg_args": ["-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
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
                    try:
                        b = get_storage_backend_sync()
                        # run the async upload in this background thread
                        try:
                            import asyncio as _asyncio

                            _asyncio.run(b.upload_file(input_path, key))
                            if os.environ.get("KEEP_LOCAL_UPLOADS", "").lower() not in ("1", "true", "yes"):
                                try:
                                    os.remove(input_path)
                                except Exception:
                                    pass
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
                        try:
                            loop.close()
                        except Exception:
                            pass

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
        except Exception as e:
            return jsonify({"error": "job queue not available on server", "detail": str(e)}), 503
    return jsonify({"job_id": job_id})


@app.route("/presign", methods=["POST"])
def presign():
    """Return a presigned S3 POST (or PUT) for client direct upload.

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
        s3 = boto3.client(
            "s3",
            region_name=os.environ.get("S3_REGION") or None,
            endpoint_url=os.environ.get("S3_ENDPOINT") or None,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )
        # prefer a POST form upload (fields + url)
        post = s3.generate_presigned_post(Bucket=bucket, Key=key, ExpiresIn=3600)
        # also provide a presigned GET url for later worker download reference
        get_url = s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600 * 24)
        return jsonify({"method": "POST", "url": post["url"], "fields": post["fields"], "key": key, "get_url": get_url})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/enqueue_from_url", methods=["POST"])
def enqueue_from_url():
    """Enqueue a job that downloads from a public or presigned URL.

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

    job_id = str(uuid.uuid4())
    output_filename = (os.path.splitext(original_filename or job_id)[0] + ".mp4") if original_filename else f"{job_id}.mp4"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    job = {
        "job_id": job_id,
        "source_url": source_url,
        "output_path": output_path,
        "original_filename": original_filename or output_filename,
        "output_filename": output_filename,
        "ffmpeg_args": ["-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
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
    """Server-Sent Events endpoint that streams Redis progress pubsub messages
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
    """Temporary token-protected endpoint to download files from the input folder.
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
        return jsonify({"error": "failed to send file", "detail": str(e)}), 500


@app.route("/internal/diag", methods=["GET", "POST"]) 
def internal_diag():
    """Token-protected diagnostic endpoint.
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
                    with open(path, "r", encoding="utf-8", errors="replace") as fh:
                        lines = fh.readlines()[-500:]
                        result["logs"][fname] = "".join(lines)
    except Exception:
        result["logs"]["error"] = traceback.format_exc()

    # process listing (best-effort)
    try:
        ps_out = subprocess.check_output(["ps", "aux"], stderr=subprocess.STDOUT, text=True)
        result["ps"] = "\n".join(ps_out.splitlines()[:200])
    except Exception:
        result["ps"] = None

    return jsonify(result)


if __name__ == "__main__":
    # Start WebSocket server for real-time updates (best-effort)
    try:
        from web.ws_server import start_in_thread

        try:
            start_in_thread(host="0.0.0.0", port=int(os.environ.get("WS_PORT", "6789")))
        except Exception:
            logger.exception("Failed to start WS server in thread")
    except Exception:
        logger.debug("WebSocket server module not available")

    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)
