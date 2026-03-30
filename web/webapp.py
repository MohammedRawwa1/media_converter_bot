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
try:
    import boto3
except Exception:
    boto3 = None

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
    if f:
        filename = f.filename or ""
        ext = os.path.splitext(filename)[1] or ".mp4"
        input_path = os.path.join(INPUT_DIR, f"{job_id}{ext}")
        f.save(input_path)
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
            ok = asyncio.run(
                download_forward_via_userbot(
                    meta.get("chat_id"), meta.get("message_id") or meta.get("msg_id"), input_path, msg_date=meta.get("registered_at") or meta.get("created_at"), file_unique_id=meta.get("file_unique_id")
                )
            )
        except Exception as e:
            return jsonify({"error": "userbot download failed", "detail": str(e)}), 500

        if not ok or not os.path.exists(input_path):
            return jsonify({"error": "userbot failed to fetch forwarded media"}), 500

    # Detect or sanitize the original filename. Prefer the uploaded name,
    # otherwise probe the file to derive a sensible name.
    try:
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
        "output_path": output_path,
        "original_filename": original_filename,
        "output_filename": os.path.basename(output_path),
        "ffmpeg_args": ["-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
        "progress_channel": f"ffmpeg:progress:{job_id}",
        "cleanup_input": True,
        "cleanup_output": False,
    }

    if enqueue_job:
        try:
            # enqueue using async helper
            asyncio.run(enqueue_job(job))
        except Exception as e:
            return jsonify({"error": f"failed to enqueue: {e}"}), 500
    else:
        # Fallback: start a background thread that runs a synchronous conversion
        # using the local media converter so uploads work without Redis.
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
        try:
            asyncio.run(enqueue_job(job))
            return jsonify({"job_id": job_id})
        except Exception as e:
            return jsonify({"error": f"failed to enqueue: {e}"}), 500
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

        resp = {"job_id": job_id, "progress": progress, "message": message, "status": status, "output": out}
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

    if job_hash and job_hash.get("output") and os.path.exists(job_hash.get("output")):
        # prefer the preserved output filename when provided
        output_path = job_hash.get("output")
        filename = job_hash.get("output_filename") or os.path.basename(output_path)
        try:
            # Flask >=2.0 uses `download_name`
            return send_file(output_path, as_attachment=True, download_name=filename)
        except TypeError:
            # older Flask versions
            return send_file(output_path, as_attachment=True, attachment_filename=filename)

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
