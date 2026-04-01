import os
import json
import uuid
from datetime import datetime
from typing import Optional

try:
    import config
except Exception:
    config = None

from .storage import get_storage_backend, get_storage_backend_sync


def _local_forwards_dir() -> str:
    base = None
    if config is not None:
        base = getattr(config, "STORAGE_PATH", None)
    if not base:
        base = os.path.join(os.path.dirname(os.path.dirname(__file__)), "storage")
    path = os.path.join(base, "forwards")
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass
    return path


def _local_path_for(fid: str) -> str:
    return os.path.join(_local_forwards_dir(), f"{fid}.json")


def save_forward_metadata(metadata: dict) -> str:
    """Persist metadata about a forwarded (undownloadable) message and return a short id.

    When configured to use an S3-compatible backend, persist to `forwards/{fid}.json`.
    Otherwise, write to local storage path under `storage/forwards/`.
    """
    fid = uuid.uuid4().hex
    data = dict(metadata)
    data.setdefault("created_at", datetime.utcnow().isoformat())

    backend_name = (os.getenv("STORAGE_BACKEND") or (config.STORAGE_BACKEND if config else "local")).lower()
    key = f"forwards/{fid}.json"

    if backend_name in ("s3", "r2"):
        try:
            backend = get_storage_backend_sync()
            # write to a temp local file then upload synchronously via backend wrapper
            tmp = _local_path_for(fid)
            os.makedirs(os.path.dirname(tmp), exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False)
            # async backend but we use sync helper to upload in background
            try:
                # try to use async upload if loop available
                import asyncio

                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # schedule upload asynchronously
                    asyncio.ensure_future(_upload_file_async(tmp, key))
                else:
                    # run briefly
                    loop.run_until_complete(_upload_file_async(tmp, key))
            except Exception:
                # fallback: run synchronous attempt via sync backend helper
                try:
                    # if backend exposes upload_file, call in thread
                    from concurrent.futures import ThreadPoolExecutor

                    def _sync_upload():
                        # attempt to call async upload synchronously
                        import asyncio

                        b = get_storage_backend_sync()
                        try:
                            asyncio.run(b.upload_file(tmp, key))
                        except Exception:
                            pass

                    t = ThreadPoolExecutor(max_workers=1)
                    t.submit(_sync_upload)
                except Exception:
                    pass
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


def load_forward_metadata(fid: str) -> Optional[dict]:
    """Load persisted forward metadata from storage backend or local disk."""
    backend_name = (os.getenv("STORAGE_BACKEND") or (config.STORAGE_BACKEND if config else "local")).lower()
    key = f"forwards/{fid}.json"

    if backend_name in ("s3", "r2"):
        try:
            # attempt to download to local temp path synchronously
            tmp = _local_path_for(fid)
            os.makedirs(os.path.dirname(tmp), exist_ok=True)
            # try to download synchronously via async backend
            try:
                import asyncio

                b = get_storage_backend_sync()
                asyncio.run(b.download_file(key, tmp))
            except Exception:
                # fallback: try running the async helper
                try:
                    import asyncio

                    asyncio.run(_download_file_async(key, tmp))
                except Exception:
                    pass

            if os.path.exists(tmp):
                with open(tmp, "r", encoding="utf-8") as fh:
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
        with open(p, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


async def _download_file_async(key: str, dest: str) -> None:
    try:
        backend = await get_storage_backend()
        await backend.download_file(key, dest)
    except Exception:
        pass


def delete_forward_metadata(fid: str) -> bool:
    backend_name = (os.getenv("STORAGE_BACKEND") or (config.STORAGE_BACKEND if config else "local")).lower()
    key = f"forwards/{fid}.json"
    if backend_name in ("s3", "r2"):
        try:
            b = get_storage_backend_sync()
            import asyncio
            # If an event loop is currently running, schedule the async
            # delete coroutine on that loop to avoid creating an
            # un-awaited coroutine (which raises a RuntimeWarning).
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                try:
                    # Schedule the coroutine to run on the loop thread-safely.
                    loop.call_soon_threadsafe(lambda: asyncio.create_task(b.delete(key)))
                except Exception:
                    # Fallback: try creating the task directly (works when
                    # called from the loop thread).
                    try:
                        loop.create_task(b.delete(key))
                    except Exception:
                        pass
            else:
                # No running loop — safe to run synchronously to completion.
                try:
                    asyncio.run(b.delete(key))
                except Exception:
                    # Best-effort: ignore delete failures
                    pass

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
