"""Telethon ingestion service.

Listens for incoming Telegram messages (via a userbot session), streams media
to a temp file, uploads the file to S3/R2 (via `utils.storage`), and enqueues a
Redis job containing lightweight metadata and the remote `input_key`.

Environment variables:
  API_ID, API_HASH - required for Telethon
  TELETHON_SESSION - optional string session (StringSession)
  TELETHON_SESSION_NAME - session filename when not using string session
  ENABLE_TELETHON_INGEST - if set to 1/true enables service (otherwise run manually)
  KEEP_LOCAL_UPLOADS - if set, keep local temp copies after upload
  STORAGE_BACKEND - must be 's3'/'r2' to upload to remote storage

Run: `python tools/telethon_ingest.py` (ensure TELETHON env vars present).
"""

import asyncio
import os
import logging
import uuid
import time
from typing import Optional
import sys
from pathlib import Path
import json

try:
    from telethon import TelegramClient, events
    from telethon.sessions import StringSession
except Exception:  # pragma: no cover - Telethon may be optional in some envs
    TelegramClient = None
    events = None
    StringSession = None

try:
    # Ensure project root is on sys.path so imports like `from utils...` work
    # even when this script is executed from the `tools/` directory.
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    # Prefer async factory but also import sync helper for robustness
    from utils.storage import get_storage_backend, get_storage_backend_sync
except Exception as e:
    # Surface import failures for diagnostics and fall back to None
    print("Failed to import storage factories from utils.storage:", e)
    get_storage_backend = None
    try:
        from utils.storage import get_storage_backend_sync
    except Exception:
        get_storage_backend_sync = None

try:
    from utils.job_queue import enqueue_job
except Exception:
    enqueue_job = None

# Optional Redis async client for triggered fetch handling
try:
    import redis.asyncio as aioredis
except Exception:
    aioredis = None

# Optional helpers used when processing remote-forward fetch requests
try:
    from utils.forward_store import load_forward_metadata
except Exception:
    load_forward_metadata = None

try:
    from utils.userbot_downloader import download_forward_via_userbot
except Exception:
    download_forward_via_userbot = None

import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telethon_ingest")


async def _get_backend_instance():
    """Return an async-capable storage backend instance or None.

    Tries the async factory first, then falls back to the synchronous helper.
    """
    global get_storage_backend, get_storage_backend_sync
    if get_storage_backend:
        try:
            return await get_storage_backend()
        except Exception:
            pass
    if get_storage_backend_sync:
        try:
            return get_storage_backend_sync()
        except Exception:
            pass
    # Last-ditch: attempt on-the-fly import
    try:
        from utils.storage import get_storage_backend as _g, get_storage_backend_sync as _gs

        get_storage_backend = _g
        get_storage_backend_sync = _gs
        if get_storage_backend:
            try:
                return await get_storage_backend()
            except Exception:
                pass
        if get_storage_backend_sync:
            return get_storage_backend_sync()
    except Exception:
        pass
    return None


async def _upload_and_enqueue(local_path: str, original_name: str, chat_id: Optional[int], message_id: Optional[int]):
    job_id = uuid.uuid4().hex
    size = None
    try:
        size = os.path.getsize(local_path)
    except Exception:
        size = None

    input_key = None
    try:
        backend = await _get_backend_instance()
        if backend is None:
            logger.error("Storage backend unavailable; cannot upload %s", local_path)
            return

        ts = time.gmtime()
        key = f"uploads/{ts.tm_year}/{ts.tm_mon:02d}/{job_id}_{os.path.basename(local_path)}"
        await backend.upload_file(local_path, key)
        input_key = key
        logger.info("Uploaded %s -> %s", local_path, input_key)
        # Only remove local temp copy if operator did NOT request to keep uploads.
        # Prefer leaving cleanup responsibility to the worker which also respects
        # KEEP_LOCAL_UPLOADS (we patched worker to honor this global flag).
        try:
            keep_local = os.environ.get("KEEP_LOCAL_UPLOADS", "").lower() in ("1", "true", "yes")
        except Exception:
            keep_local = False
        if not keep_local:
            try:
                os.remove(local_path)
            except Exception:
                pass
    except Exception:
        logger.exception("Failed to upload to storage for %s", local_path)

    # If upload failed (no input_key), do not enqueue an empty job.
    if not input_key:
        logger.error("Upload did not produce an input_key for %s; not enqueuing job %s", local_path, job_id)
        return

    # Build job metadata and enqueue to Redis (metadata-only)
    try:
        keep_local = os.environ.get("KEEP_LOCAL_UPLOADS", "").lower() in ("1", "true", "yes")
    except Exception:
        keep_local = False

    job = {
        "job_id": job_id,
        "input_key": input_key,
        "original_filename": original_name or os.path.basename(local_path),
        "size": size or 0,
        "chat_id": chat_id,
        "message_id": message_id,
        "progress_channel": f"ffmpeg:progress:{job_id}",
        # Let the worker decide whether to delete local input; here we
        # indicate whether the job should cleanup the input after processing.
        "cleanup_input": not keep_local,
    }

    # Optionally save metadata to MongoDB for Telethon ingestion (best-effort, non-blocking)
    try:
        if os.environ.get("TELETHON_MONGO_BRIDGE", "").lower() in ("1", "true", "yes"):
            try:
                from utils.telethon_mongo import save_telethon_forward

                try:
                    # Schedule in background so ingestion isn't delayed by DB latency
                    asyncio.create_task(save_telethon_forward(job))
                except Exception:
                    try:
                        loop = asyncio.get_event_loop()
                        loop.create_task(save_telethon_forward(job))
                    except Exception:
                        # best-effort only
                        pass
            except Exception:
                logger.exception("Telethon->Mongo bridge unavailable")
    except Exception:
        pass

    if enqueue_job is None:
        logger.error("enqueue_job not available; cannot enqueue %s", job_id)
        return

    try:
        await enqueue_job(job)
        logger.info("Enqueued job %s (input_key=%s)", job_id, input_key)
    except Exception:
        logger.exception("Failed to enqueue job %s", job_id)


async def _process_forward_hash(forward_hash: str):
    """Handle a published forward_hash: download via userbot and upload/enqueue."""
    if not load_forward_metadata:
        logger.error("telethon_ingest: forward_store not available; cannot process %s", forward_hash)
        return False

    meta = load_forward_metadata(forward_hash)
    if not meta:
        logger.error("telethon_ingest: no metadata for forward_hash %s", forward_hash)
        return False

    logger.info("telethon_ingest: processing forward %s meta_chat=%s meta_msg=%s", forward_hash, meta.get("chat_id"), meta.get("message_id") or meta.get("msg_id"))

    tmp = _make_temp_path(forward_hash, os.path.splitext(meta.get("name") or "")[1] or "")

    if not download_forward_via_userbot:
        logger.error("telethon_ingest: userbot_downloader not available; cannot fetch %s", forward_hash)
        return False

    try:
        ok = await download_forward_via_userbot(
            meta.get("chat_id"), meta.get("message_id") or meta.get("msg_id"), tmp, msg_date=meta.get("registered_at") or meta.get("created_at"), file_unique_id=meta.get("file_unique_id")
        )
        logger.info("telethon_ingest: userbot download for %s returned ok=%s exists=%s", forward_hash, bool(ok), os.path.exists(tmp))
        if not ok or not os.path.exists(tmp):
            logger.error("telethon_ingest: download failed for %s", forward_hash)
            return False
    except Exception:
        logger.exception("telethon_ingest: exception during download for %s", forward_hash)
        return False

    # Upload & enqueue using same helper
    try:
        await _upload_and_enqueue(tmp, meta.get("name"), meta.get("chat_id"), meta.get("message_id") or meta.get("msg_id"))
    except Exception:
        logger.exception("telethon_ingest: upload/enqueue failed for %s", forward_hash)
        return False

    # Optionally remove forward metadata (forward_store may handle this elsewhere)
    try:
        from utils.forward_store import delete_forward_metadata

        try:
            delete_forward_metadata(forward_hash)
        except Exception:
            pass
    except Exception:
        pass

    return True


async def redis_listener():
    """Subscribe to the fetch channel and process forward_hash messages."""
    if aioredis is None:
        logger.info("telethon_ingest: redis.asyncio not installed; fetch listener disabled")
        return

    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        logger.info("telethon_ingest: REDIS_URL not set; fetch listener disabled")
        return

    try:
        r = aioredis.from_url(redis_url, decode_responses=True)
        pub = r.pubsub()
        fetch_channel = os.environ.get("FETCH_CHANNEL", "ffmpeg:fetch")
        await pub.subscribe(fetch_channel)
        logger.info("telethon_ingest: subscribed to %s", fetch_channel)

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
                asyncio.create_task(_process_forward_hash(fh))
    except Exception:
        logger.exception("telethon_ingest: redis listener failed")


def _make_temp_path(msg_id: str, ext: str = "") -> str:
    base_dir = getattr(config, "TEMP_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage", "temp"))
    try:
        os.makedirs(base_dir, exist_ok=True)
    except Exception:
        pass
    return os.path.join(base_dir, f"{msg_id}{ext}")


async def main():
    if TelegramClient is None:
        logger.error("Telethon not installed. Add telethon to requirements to enable ingestion.")
        return

    api_id = os.getenv("API_ID") or os.getenv("USERBOT_API_ID")
    api_hash = os.getenv("API_HASH") or os.getenv("USERBOT_API_HASH")
    if not api_id or not api_hash:
        logger.error("API_ID and API_HASH environment variables are required for Telethon ingestion")
        return
    try:
        api_id = int(api_id)
    except Exception:
        logger.error("API_ID must be an integer")
        return

    session_str = os.getenv("TELETHON_SESSION")
    session_name = os.getenv("TELETHON_SESSION_NAME", "telethon_ingest")

    if session_str and StringSession is not None:
        session = StringSession(session_str)
    else:
        session = session_name

    client = TelegramClient(session, api_id, api_hash)

    @client.on(events.NewMessage(incoming=True))
    async def handler(event):
        try:
            msg = event.message
            if not getattr(msg, "media", None):
                return

            # process message in background to avoid blocking Telethon event loop
            asyncio.create_task(process_incoming(msg))
        except Exception:
            logger.exception("Error in Telethon handler")


    async def process_incoming(msg):
        # determine filename/extension safely
        fname = None
        try:
            # Telethon may expose a .file or .document with a name
            file_attr = getattr(msg, "file", None)
            if file_attr is not None:
                fname = getattr(file_attr, "name", None)
        except Exception:
            fname = None

        msg_id = getattr(msg, "id", str(uuid.uuid4()))
        # derive extension from file name or default to .mp4
        ext = os.path.splitext(fname or "")[1] or ""
        tmp = _make_temp_path(msg_id, ext)

        try:
            await client.download_media(msg, file=tmp)
            logger.info("Downloaded incoming media to %s", tmp)
        except Exception:
            logger.exception("Failed to download media from message %s", msg_id)
            return

        # Upload & enqueue
        try:
            await _upload_and_enqueue(tmp, fname, getattr(msg.chat, "id", None) or getattr(msg, "chat_id", None), getattr(msg, "id", None))
        except Exception:
            logger.exception("Failed to upload/enqueue for %s", tmp)

    # start client
    await client.start()
    logger.info("Telethon ingestion client started, listening for incoming media...")

    # Start Redis fetch listener (if available) so this single service can
    # both accept incoming messages and process published forward fetches
    try:
        asyncio.create_task(redis_listener())
    except Exception:
        logger.exception("telethon_ingest: failed to start redis listener")

    try:
        await client.run_until_disconnected()
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
