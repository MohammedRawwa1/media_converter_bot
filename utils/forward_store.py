import asyncio
import json
import os
import uuid
from datetime import datetime

try:
    import config
except Exception:
    config = None

import contextlib
import logging

from .storage import get_storage_backend, get_storage_backend_sync

logger = logging.getLogger(__name__)


def _publish_forward_notification(fid: str, extra: dict = None) -> None:
    """Best-effort notify Redis that a forward metadata object exists.
    Payload published on channel defined by `FORWARD_PUBLISH_CHANNEL` (default `ffmpeg:forwards`).
    Optionally also publishes a fetch request to `ffmpeg:fetch` when
    `AUTO_FETCH_FORWARDS` is enabled. This function never raises.

    Single-path publish: uses sync redis.client only (no async fallback that would
    double-publish the same notification to the same channels).
    """
    try:
        forward_channel = os.environ.get("FORWARD_PUBLISH_CHANNEL", "ffmpeg:forwards")
        fetch_channel = os.environ.get("FETCH_CHANNEL", "ffmpeg:fetch")
        do_auto_fetch = os.environ.get("AUTO_FETCH_FORWARDS", "").lower() in ("1", "true", "yes")

        redis_url = os.environ.get("REDIS_URL") or os.environ.get("REDIS_URI") or os.environ.get("REDIS")
        payload = {"fid": fid, "forward_hash": fid}
        if extra:
            payload.update(extra)

        with contextlib.suppress(Exception):
            logger.info("forward_store: publish -> forward_channel=%s fetch_channel=%s do_auto_fetch=%s payload=%s",
                        forward_channel, fetch_channel, do_auto_fetch, payload)

        # Single sync publish path (avoids double-publishing from async fallback)
        try:
            import redis as _redis
            if redis_url:
                client = _redis.from_url(redis_url, decode_responses=True)
            else:
                client = _redis.Redis(decode_responses=True)
            with contextlib.suppress(Exception):
                client.publish(forward_channel, json.dumps(payload))
            if do_auto_fetch:
                with contextlib.suppress(Exception):
                    client.publish(fetch_channel, json.dumps({"forward_hash": fid}))
        except Exception:
            pass
    except Exception:
        # never raise
        pass


def _local_forwards_dir() -> str:
    base = None
    if config is not None:
        base = getattr(config, "STORAGE_PATH", None)
    if not base:
        base = os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage")
    path = os.path.join(base, "forwards")
    with contextlib.suppress(Exception):
        os.makedirs(path, exist_ok=True)
    return path


def _local_path_for(fid: str) -> str:
    return os.path.join(_local_forwards_dir(), f"{fid}.json")


async def save_forward_metadata(metadata: dict) -> str:
    """Persist metadata about a forwarded (undownloadable) message and return a short id.

    When configured to use an S3-compatible backend, persist to `forwards/{fid}.json`.
    Otherwise, write to local storage path under `storage/forwards/`.

    This function is async so S3/R2 uploads can be properly awaited.
    Async callers should use ``await save_forward_metadata(metadata)``;
    sync callers should wrap with ``asyncio.run(save_forward_metadata(metadata))``.
    """
    fid = uuid.uuid4().hex
    data = dict(metadata)
    data.setdefault("created_at", datetime.utcnow().isoformat())

    backend_name = (os.getenv("STORAGE_BACKEND") or (config.STORAGE_BACKEND if config else "local")).lower()
    key = f"forwards/{fid}.json"

    if backend_name in ("s3", "r2"):
        try:
            get_storage_backend_sync()
            # write to a temp local file then upload synchronously via backend wrapper
            tmp = _local_path_for(fid)
            os.makedirs(os.path.dirname(tmp), exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False)
            # Upload via proper await instead of fire-and-forget
            with contextlib.suppress(Exception):
                await _upload_file_async(tmp, key)
            with contextlib.suppress(Exception):
                # Best-effort publish that forward metadata is available (include remote key)
                _publish_forward_notification(fid, {"remote_key": key, "file_id": data.get("file_id")})
            return fid
        except Exception:
            # fallback to local
            pass

    # default: write locally and return id
    p = _local_path_for(fid)
    try:
        with open(p, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
    except Exception:
        # best-effort: try to write somewhere else
        try:
            tmp = os.path.join(os.path.dirname(__file__), f"{fid}.json")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False)
            return fid
        except Exception:
            raise
    with contextlib.suppress(Exception):
        _publish_forward_notification(fid, {"local_path": p, "file_id": data.get("file_id")})
    return fid


async def _upload_file_async(local_path: str, key: str) -> None:
    try:
        backend = await get_storage_backend()
        await backend.upload_file(local_path, key)
        # optionally remove local copy after upload
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
        except Exception:
            pass
    except Exception:
        pass


async def load_forward_metadata(fid: str) -> dict | None:
    """Load persisted forward metadata from storage backend or local disk.

    This function is async so S3/R2 downloads can be properly awaited.
    Async callers should use ``await load_forward_metadata(fid)``;
    sync callers should wrap with ``asyncio.run(load_forward_metadata(fid))``.
    """
    backend_name = (os.getenv("STORAGE_BACKEND") or (config.STORAGE_BACKEND if config else "local")).lower()
    key = f"forwards/{fid}.json"

    if backend_name in ("s3", "r2"):
        try:
            # attempt to download to local temp path synchronously
            tmp = _local_path_for(fid)
            os.makedirs(os.path.dirname(tmp), exist_ok=True)
            # try to download synchronously via async backend
            try:
                b = get_storage_backend_sync()
                await b.download_file(key, tmp)
            except Exception:
                # fallback: try running the async helper
                with contextlib.suppress(Exception):
                    await _download_file_async(key, tmp)

            if os.path.exists(tmp):
                with open(tmp, encoding="utf-8") as fh:
                    return json.load(fh)
        except Exception:
            return None

    # default: read local file
    p = _local_path_for(fid)
    if not os.path.exists(p):
        alt = os.path.join(os.path.dirname(__file__), f"{fid}.json")
        if os.path.exists(alt):
            p = alt
        else:
            return None
    try:
        with open(p, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


async def _download_file_async(key: str, dest: str) -> None:
    try:
        backend = await get_storage_backend()
        # Retry/backoff for transient download errors
        retries = int(os.getenv("DOWNLOAD_RETRIES", "3"))
        backoff = float(os.getenv("DOWNLOAD_BACKOFF_BASE", "1"))
        for attempt in range(1, retries + 1):
            try:
                await backend.download_file(key, dest)
                # verify file exists and has size
                if os.path.exists(dest) and (os.path.getsize(dest) > 0):
                    return
            except Exception:
                pass

            if attempt < retries:
                await asyncio.sleep(backoff * (2 ** (attempt - 1)))
    except Exception:
        pass


async def delete_forward_metadata(fid: str) -> bool:
    """Delete persisted forward metadata from storage backend or local disk.

    This function is async so S3/R2 deletes can be properly awaited.
    Async callers should use ``await delete_forward_metadata(fid)``;
    sync callers should wrap with ``asyncio.run(delete_forward_metadata(fid))``.
    """
    # When debugging it may be useful to keep forward metadata in storage
    # for investigation. Honor `KEEP_FORWARD_METADATA=1|true|yes` to skip
    # removing the saved forward JSON.
    if os.environ.get("KEEP_FORWARD_METADATA", "").lower() in ("1", "true", "yes"):
        with contextlib.suppress(Exception):
            logger.info("KEEP_FORWARD_METADATA set; not deleting forward metadata %s", fid)
        return True

    # Provide caller context in logs to help root-cause analysis when
    # forwards are deleted unexpectedly.
    try:
        import traceback as _trace

        stack = _trace.format_stack(limit=6)
        logger.info("delete_forward_metadata called for %s; caller stack:\n%s", fid, "".join(stack))
    except Exception:
        pass

    backend_name = (os.getenv("STORAGE_BACKEND") or (config.STORAGE_BACKEND if config else "local")).lower()
    key = f"forwards/{fid}.json"

    # Optional archival/move behavior: if `FORWARDS_ARCHIVE_PREFIX` is set
    # we will attempt to copy the forward JSON to that prefix (e.g.
    # "forwards/archived") and then delete the original. This preserves
    # the metadata for later inspection while keeping the original key
    # namespace clean.
    archive_prefix = os.environ.get("FORWARDS_ARCHIVE_PREFIX") or os.environ.get("FORWARD_ARCHIVE_PREFIX")
    if archive_prefix:
        archive_key = archive_prefix.rstrip("/") + "/" + f"{fid}.json"
        if backend_name in ("s3", "r2"):
            try:
                b = get_storage_backend_sync()
                # Prefer server-side copy via boto3 when available
                try:
                    import boto3 as _boto3

                    client = _boto3.client("s3", **b._client_kwargs())
                    copy_source = {"Bucket": b.bucket, "Key": key}
                    client.copy_object(CopySource=copy_source, Bucket=b.bucket, Key=archive_key)
                    logger.info("Archived forward object %s -> %s", key, archive_key)
                    # delete original via backend helper (async-safe)
                    with contextlib.suppress(Exception):
                        await b.delete(key)
                    return True
                except Exception:
                    logger.exception("Archive copy via boto3 failed for %s -> %s; falling back to download/reupload", key, archive_key)
                    # Fallback: download then reupload
                    try:
                        import tempfile

                        fd, tmp = tempfile.mkstemp(suffix=".json")
                        os.close(fd)
                        try:
                            await b.download_file(key, tmp)
                            await b.upload_file(tmp, archive_key)
                        finally:
                            with contextlib.suppress(Exception):
                                os.remove(tmp)
                        # delete original
                        with contextlib.suppress(Exception):
                            await b.delete(key)
                        return True
                    except Exception:
                        logger.exception("Archive fallback (download/reupload) failed for %s", key)
                        # fall through to regular delete below
            except Exception:
                logger.exception("Failed to archive forward %s", fid)

    # Default deletion behavior: attempt backend delete (async-safe),
    # otherwise remove local copy.
    if backend_name in ("s3", "r2"):
        try:
            b = get_storage_backend_sync()
            await b.delete(key)
            return True
        except Exception:
            # fallback: attempt to delete local file if present
            pass

    p = _local_path_for(fid)
    try:
        if os.path.exists(p):
            os.remove(p)
            return True
    except Exception:
        pass
    return False
