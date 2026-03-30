import asyncio
import os
import json
import logging
import uuid
from aiohttp import web

try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

try:
    from utils.forward_store import load_forward_metadata, delete_forward_metadata
    from utils.userbot_downloader import download_forward_via_userbot
    from utils.job_queue import enqueue_job
except Exception:
    load_forward_metadata = None
    download_forward_via_userbot = None
    enqueue_job = None

logger = logging.getLogger(__name__)


async def process_forward_hash(forward_hash: str):
    if not load_forward_metadata:
        logger.error("fetcher: forward_store not available")
        return False

    meta = load_forward_metadata(forward_hash)
    if not meta:
        logger.error("fetcher: no metadata for forward_hash %s", forward_hash)
        return False

    input_dir = os.environ.get("INPUT_PATH") or os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage", "input")
    os.makedirs(input_dir, exist_ok=True)

    job_uuid = str(uuid.uuid4())
    ext = os.path.splitext(meta.get("name") or "")[1] or ".mp4"
    input_path = os.path.join(input_dir, f"{job_uuid}{ext}")

    if not download_forward_via_userbot:
        logger.error("fetcher: userbot_downloader not available; cannot fetch %s", forward_hash)
        return False

    try:
        ok = await download_forward_via_userbot(
            meta.get("chat_id"), meta.get("message_id") or meta.get("msg_id"), input_path, msg_date=meta.get("registered_at") or meta.get("created_at"), file_unique_id=meta.get("file_unique_id")
        )
        if not ok or not os.path.exists(input_path):
            logger.error("fetcher: download failed for %s", forward_hash)
            return False
    except Exception:
        logger.exception("fetcher: exception during download for %s", forward_hash)
        return False

    # build job and enqueue
    try:
        job_id = job_uuid
        output_dir = os.environ.get("OUTPUT_PATH") or os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage", "output")
        os.makedirs(output_dir, exist_ok=True)
        base_name = os.path.splitext(meta.get("name") or os.path.basename(input_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}_{job_id}.mp4")
        job = {
            "job_id": job_id,
            "input_path": input_path,
            "output_path": output_path,
            "original_filename": meta.get("name") or os.path.basename(input_path),
            "output_filename": os.path.basename(output_path),
            "ffmpeg_args": ["-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
            "progress_channel": f"ffmpeg:progress:{job_id}",
            "chat_id": meta.get("chat_id"),
            "cleanup_input": True,
            "cleanup_output": False,
        }

        if enqueue_job:
            await enqueue_job(job)
            logger.info("fetcher: enqueued job %s for forward %s", job_id, forward_hash)
        else:
            logger.error("fetcher: enqueue_job not available; cannot enqueue %s", forward_hash)
            return False
    except Exception:
        logger.exception("fetcher: failed to create/enqueue job for %s", forward_hash)
        return False

    try:
        delete_forward_metadata(forward_hash)
    except Exception:
        pass

    return True


async def redis_listener():
    if not aioredis:
        logger.warning("fetcher: redis.asyncio not installed; redis listener disabled")
        return

    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        logger.warning("fetcher: REDIS_URL not set; redis listener disabled")
        return

    r = aioredis.from_url(redis_url)
    pub = r.pubsub()
    await pub.subscribe("ffmpeg:fetch")
    logger.info("fetcher: subscribed to ffmpeg:fetch channel")

    async for msg in pub.listen():
        if not msg:
            continue
        if msg.get("type") != "message":
            continue
        data = msg.get("data")
        if isinstance(data, bytes):
            try:
                data = data.decode("utf-8")
            except Exception:
                data = str(data)
        try:
            payload = json.loads(data)
        except Exception:
            payload = {"forward_hash": data}
        fh = payload.get("forward_hash")
        if fh:
            asyncio.create_task(process_forward_hash(fh))


async def handle_http_fetch(request):
    try:
        data = await request.json()
    except Exception:
        data = dict(await request.post())
    fh = data.get("forward_hash") or request.query.get("forward_hash")
    if not fh:
        return web.json_response({"error": "forward_hash required"}, status=400)
    asyncio.create_task(process_forward_hash(fh))
    return web.json_response({"queued": True, "forward_hash": fh})


def main():
    logging.basicConfig(level=logging.INFO)
    app = web.Application()
    app.add_routes([web.post("/fetch", handle_http_fetch)])

    async def on_startup(app):
        app["redis_task"] = asyncio.create_task(redis_listener())

    async def on_cleanup(app):
        t = app.get("redis_task")
        if t:
            t.cancel()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    host = os.environ.get("FETCHER_HOST", "0.0.0.0")
    port = int(os.environ.get("FETCHER_PORT", "8765"))
    web.run_app(app, host=host, port=port)


if __name__ == "__main__":
    main()
