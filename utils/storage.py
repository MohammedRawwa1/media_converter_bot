"""Asynchronous storage backend abstraction.

Provides a small async-friendly wrapper for local filesystem storage and
S3/S3-compatible (e.g. Cloudflare R2) using `aioboto3`.

Usage example:
    from utils.storage import get_storage_backend

    storage = await get_storage_backend()
    await storage.upload_file("/tmp/video.mp4", "uploads/job123/video.mp4")
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

try:
    import aioboto3
except Exception:  # pragma: no cover - aioboto3 may be optional
    aioboto3 = None

try:
    import boto3
except Exception:
    boto3 = None

try:
    from botocore.config import Config as BotoConfig
except Exception:
    BotoConfig = None

import config

logger = logging.getLogger(__name__)


class AsyncStorageBackend(ABC):
    @abstractmethod
    async def upload_file(self, src_path: str, dest_key: str) -> str:
        """Upload a local file at `src_path` to storage and return the storage key or path."""

    @abstractmethod
    async def download_file(self, key: str, dest_path: str) -> bool:
        """Download a storage object `key` to local `dest_path`. Return True on success."""

    @abstractmethod
    async def generate_presigned_post(self, key: str, expires: Optional[int] = None) -> Dict[str, Any]:
        """Return a dict with presigned POST upload info (url/fields) or raise when unsupported."""

    @abstractmethod
    async def generate_presigned_get(self, key: str, expires: Optional[int] = None) -> str:
        """Return a presigned GET URL for `key` or raise when unsupported."""

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete object at `key` from storage. Return True if deleted or not found."""


class LocalStorageBackend(AsyncStorageBackend):
    def __init__(self, base_path: Optional[str] = None):
        self.base = base_path or config.STORAGE_PATH

    def _abs_path(self, key: str) -> str:
        return os.path.join(self.base, key)

    async def upload_file(self, src_path: str, dest_key: str) -> str:
        dest = self._abs_path(dest_key)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        await asyncio.to_thread(shutil.copy2, src_path, dest)
        return dest

    async def download_file(self, key: str, dest_path: str) -> bool:
        src = self._abs_path(key)
        if not os.path.exists(src):
            return False
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        await asyncio.to_thread(shutil.copy2, src, dest_path)
        return True

    async def generate_presigned_post(self, key: str, expires: Optional[int] = None) -> Dict[str, Any]:
        raise NotImplementedError("Presigned uploads are not supported for local backend")

    async def generate_presigned_get(self, key: str, expires: Optional[int] = None) -> str:
        # Provide a file:// URL for convenience (may not be usable remotely)
        return "file://" + os.path.abspath(self._abs_path(key))

    async def delete(self, key: str) -> bool:
        p = self._abs_path(key)
        try:
            if os.path.exists(p):
                await asyncio.to_thread(os.remove, p)
                return True
            return True
        except Exception:
            return False


class S3AsyncBackend(AsyncStorageBackend):
    def __init__(
        self,
        bucket: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        use_ssl: bool = True,
    ):
        # Support both async aioboto3 (preferred) and sync boto3 (fallback).
        # If aioboto3 is present we will use it for non-blocking IO. Otherwise
        # we will call synchronous boto3 functions inside `asyncio.to_thread`.
        self._use_aioboto3 = aioboto3 is not None

        self.bucket = bucket or config.S3_BUCKET
        self.endpoint_url = endpoint_url or (config.S3_ENDPOINT or None)
        self.region = region or (config.S3_REGION or None)
        self.aws_access_key_id = aws_access_key_id or config.AWS_ACCESS_KEY_ID or None
        self.aws_secret_access_key = aws_secret_access_key or config.AWS_SECRET_ACCESS_KEY or None
        # Support temporary session tokens (AWS STS / assumed-role / R2 variants)
        self.aws_session_token = aws_session_token or os.getenv("AWS_SESSION_TOKEN") or None
        self.use_ssl = use_ssl

        # async session only when aioboto3 is available
        self._session = aioboto3.Session() if self._use_aioboto3 else None

        # optional botocore config (used for both aioboto3 and boto3 clients)
        self._boto_config = None
        if BotoConfig is not None:
            try:
                # Allow forcing path-style addressing for S3-compatible endpoints
                force_path = str(os.getenv("S3_FORCE_PATH_STYLE", "")).lower() in ("1", "true", "yes")
                if force_path:
                    try:
                        # prefer explicit addressing style when requested
                        self._boto_config = BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"})
                    except Exception:
                        # fallback to default config object
                        self._boto_config = BotoConfig(signature_version="s3v4")
                else:
                    self._boto_config = BotoConfig(signature_version="s3v4")
            except Exception:
                self._boto_config = None

    def _client_kwargs(self) -> Dict[str, Any]:
        kw = {}
        if self.region:
            kw["region_name"] = self.region
        if self.endpoint_url:
            # Ensure endpoint_url has a scheme (boto3 requires https:// or http://)
            ep = str(self.endpoint_url).strip()
            if ep and not ep.startswith("http://") and not ep.startswith("https://"):
                scheme = "https" if self.use_ssl else "http"
                ep = f"{scheme}://{ep}"
            # Strip any trailing slash
            ep = ep.rstrip("/")
            kw["endpoint_url"] = ep
        if self.aws_access_key_id:
            kw["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            kw["aws_secret_access_key"] = self.aws_secret_access_key
        if self._boto_config is not None:
            kw["config"] = self._boto_config
        if self.aws_session_token:
            kw["aws_session_token"] = self.aws_session_token
        return kw

    async def upload_file(self, src_path: str, dest_key: str) -> str:
        if not src_path or not os.path.exists(src_path):
            raise ValueError(f"Invalid src_path: {src_path}")

        if not dest_key:
            raise ValueError("dest_key must not be empty")

        if not self.bucket:
            raise ValueError(f"Invalid S3 bucket name: {self.bucket}")

        # Ensure the configured S3 bucket is a bucket name, not a URL
        if "http://" in str(self.bucket) or "https://" in str(self.bucket):
            raise ValueError(f"S3_BUCKET must be a bucket name, not a URL: {self.bucket}")

        # Retry/backoff parameters
        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))

        import random

        for attempt in range(1, retries + 1):
            try:
                # Masked diagnostics (do not log secrets). Show partial key and endpoint for debugging.
                masked_key = None
                if self.aws_access_key_id:
                    ak = str(self.aws_access_key_id)
                    masked_key = f"{ak[:4]}...{ak[-4:]}" if len(ak) > 8 else ak
                else:
                    masked_key = "(env)"

                logger.info(
                    "Uploading file → bucket=%s key=%s (attempt %s/%s) [ak=%s endpoint=%s]",
                    self.bucket,
                    dest_key,
                    attempt,
                    retries,
                    masked_key,
                    (self.endpoint_url or "default"),
                )

                # Async path (aioboto3)
                if self._use_aioboto3:
                    async with self._session.client("s3", **self._client_kwargs()) as client:
                        await client.upload_file(src_path, self.bucket, dest_key)
                    return dest_key

                # Sync fallback (boto3)
                if boto3 is None:
                    raise RuntimeError("boto3 is required when aioboto3 is not installed")

                def _sync_upload():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.upload_file(src_path, self.bucket, dest_key)

                await asyncio.to_thread(_sync_upload)
                return dest_key

            except Exception as e:
                logger.warning(
                    "S3 upload failed (attempt %s/%s): %s",
                    attempt,
                    retries,
                    e,
                )

                if attempt == retries:
                    logger.exception("S3 upload failed permanently for key=%s", dest_key)
                    raise

                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                await asyncio.sleep(backoff + random.random())

    async def upload_file_streaming(self, src_path: str, dest_key: str) -> str:
        if not os.path.exists(src_path):
            raise ValueError(f"File not found: {src_path}")

        # Async path (aioboto3)
        if self._use_aioboto3:
            async with self._session.client("s3", **self._client_kwargs()) as client:
                with open(src_path, "rb") as f:
                    await client.put_object(
                        Bucket=self.bucket,
                        Key=dest_key,
                        Body=f,
                    )
            return dest_key

        # Sync fallback
        if boto3 is None:
            raise RuntimeError("boto3 is required when aioboto3 is not installed")

        def _sync():
            client = boto3.client("s3", **self._client_kwargs())
            with open(src_path, "rb") as f:
                client.put_object(Bucket=self.bucket, Key=dest_key, Body=f)

        await asyncio.to_thread(_sync)
        return dest_key

    async def download_file(self, key: str, dest_path: str) -> bool:
        # Retry/backoff parameters
        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))

        import random

        for attempt in range(1, retries + 1):
            try:
                if self._use_aioboto3:
                    async with self._session.client("s3", **self._client_kwargs()) as client:
                        await client.download_file(self.bucket, key, dest_path)
                    return True

                if boto3 is None:
                    raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")

                def _sync_download():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.download_file(self.bucket, key, dest_path)

                await asyncio.to_thread(_sync_download)
                return True

            except Exception as e:
                logger.warning("S3 download attempt %s/%s failed for key %s: %s", attempt, retries, key, e)
                if attempt == retries:
                    logger.exception("S3 download failed after %s attempts for key %s", retries, key)
                    raise
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                await asyncio.sleep(backoff + random.random())

    async def generate_presigned_post(self, key: str, expires: Optional[int] = None) -> Dict[str, Any]:
        expires = expires or config.PRESIGN_EXPIRES
        if self._use_aioboto3:
            async with self._session.client("s3", **self._client_kwargs()) as client:
                # generate_presigned_post is a local signing operation (no network)
                post = client.generate_presigned_post(Bucket=self.bucket, Key=key, ExpiresIn=expires)
                get_url = client.generate_presigned_url("get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires * 24)
            return {"url": post["url"], "fields": post["fields"], "key": key, "get_url": get_url}

        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")
        def _sync_post():
            client = boto3.client("s3", **self._client_kwargs())
            post = client.generate_presigned_post(Bucket=self.bucket, Key=key, ExpiresIn=expires)
            get_url = client.generate_presigned_url("get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires * 24)
            return {"url": post["url"], "fields": post["fields"], "key": key, "get_url": get_url}
        return await asyncio.to_thread(_sync_post)

    async def generate_presigned_get(self, key: str, expires: Optional[int] = None) -> str:
        expires = expires or config.PRESIGN_EXPIRES
        if self._use_aioboto3:
            async with self._session.client("s3", **self._client_kwargs()) as client:
                url = client.generate_presigned_url("get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires)
            return url

        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 operations when aioboto3 is not installed")
        def _sync_get():
            client = boto3.client("s3", **self._client_kwargs())
            return client.generate_presigned_url("get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires)
        return await asyncio.to_thread(_sync_get)

    async def delete(self, key: str) -> bool:
        # Retry/backoff for deletes, but do not raise to avoid unhandled
        # exceptions when deletes are scheduled as fire-and-forget.
        retries = int(os.getenv("S3_OP_RETRIES", "3"))
        backoff_base = float(os.getenv("S3_OP_BACKOFF_BASE", "1"))
        max_backoff = float(os.getenv("S3_OP_BACKOFF_MAX", "60"))

        import random

        for attempt in range(1, retries + 1):
            try:
                if self._use_aioboto3:
                    async with self._session.client("s3", **self._client_kwargs()) as client:
                        await client.delete_object(Bucket=self.bucket, Key=key)
                    return True

                if boto3 is None:
                    logger.error("boto3 is required for S3 operations when aioboto3 is not installed")
                    return False

                def _sync_delete():
                    client = boto3.client("s3", **self._client_kwargs())
                    client.delete_object(Bucket=self.bucket, Key=key)

                await asyncio.to_thread(_sync_delete)
                return True

            except Exception as e:
                logger.warning("S3 delete attempt %s/%s failed for key %s: %s", attempt, retries, key, e)
                if attempt == retries:
                    logger.exception("S3 delete failed after %s attempts for key %s", retries, key)
                    return False
                backoff = min(max_backoff, backoff_base * (2 ** (attempt - 1)))
                await asyncio.sleep(backoff + random.random())


_STORAGE_SINGLETON: Optional[AsyncStorageBackend] = None


async def get_storage_backend() -> AsyncStorageBackend:
    """Return a shared AsyncStorageBackend instance based on configuration.

    This factory chooses between `local` and `s3`/`r2` backends depending on
    `config.STORAGE_BACKEND`. The result is cached for the lifetime of the
    process.
    """
    global _STORAGE_SINGLETON
    if _STORAGE_SINGLETON is not None:
        return _STORAGE_SINGLETON

    backend = (os.getenv("STORAGE_BACKEND") or config.STORAGE_BACKEND or "local").lower()
    if backend in ("s3", "r2"):
        _STORAGE_SINGLETON = S3AsyncBackend(bucket=config.S3_BUCKET, endpoint_url=(os.getenv("S3_ENDPOINT") or config.S3_ENDPOINT or None), region=(os.getenv("S3_REGION") or config.S3_REGION or None), aws_access_key_id=(os.getenv("AWS_ACCESS_KEY_ID") or config.AWS_ACCESS_KEY_ID or None), aws_secret_access_key=(os.getenv("AWS_SECRET_ACCESS_KEY") or config.AWS_SECRET_ACCESS_KEY or None), use_ssl=config.S3_USE_SSL)
    else:
        _STORAGE_SINGLETON = LocalStorageBackend(base_path=(os.getenv("STORAGE_PATH") or config.STORAGE_PATH))

    return _STORAGE_SINGLETON


def get_storage_backend_sync() -> AsyncStorageBackend:
    """Synchronous convenience wrapper to obtain a backend without awaiting.

    Note: callers should prefer `await get_storage_backend()` where possible.
    This helper will create the same singleton but will raise if S3 backend
    requires `aioboto3` and it's not installed.
    """
    global _STORAGE_SINGLETON
    if _STORAGE_SINGLETON is not None:
        return _STORAGE_SINGLETON

    backend = (os.getenv("STORAGE_BACKEND") or config.STORAGE_BACKEND or "local").lower()
    if backend in ("s3", "r2"):
        # create synchronously (may raise if aioboto3 missing)
        _STORAGE_SINGLETON = S3AsyncBackend(bucket=config.S3_BUCKET, endpoint_url=config.S3_ENDPOINT or None, region=config.S3_REGION or None, aws_access_key_id=config.AWS_ACCESS_KEY_ID or None, aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY or None, use_ssl=config.S3_USE_SSL)
    else:
        _STORAGE_SINGLETON = LocalStorageBackend(base_path=(os.getenv("STORAGE_PATH") or config.STORAGE_PATH))

    return _STORAGE_SINGLETON
