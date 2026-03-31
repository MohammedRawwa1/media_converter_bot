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
import os
import shutil
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

try:
    import aioboto3
except Exception:  # pragma: no cover - aioboto3 may be optional
    aioboto3 = None

try:
    from botocore.config import Config as BotoConfig
except Exception:
    BotoConfig = None

import config


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
        use_ssl: bool = True,
    ):
        if aioboto3 is None:
            raise RuntimeError("aioboto3 is required for S3 backend but is not installed")

        self.bucket = bucket or config.S3_BUCKET
        self.endpoint_url = endpoint_url or (config.S3_ENDPOINT or None)
        self.region = region or (config.S3_REGION or None)
        self.aws_access_key_id = aws_access_key_id or config.AWS_ACCESS_KEY_ID or None
        self.aws_secret_access_key = aws_secret_access_key or config.AWS_SECRET_ACCESS_KEY or None
        self.use_ssl = use_ssl
        self._session = aioboto3.Session()
        # optional botocore config
        self._boto_config = None
        if BotoConfig is not None:
            # prefer virtual hosted style by default; some S3-compatible services
            # require path-style addressing — users can supply a custom endpoint.
            try:
                self._boto_config = BotoConfig(signature_version="s3v4")
            except Exception:
                self._boto_config = None

    def _client_kwargs(self) -> Dict[str, Any]:
        kw = {}
        if self.region:
            kw["region_name"] = self.region
        if self.endpoint_url:
            kw["endpoint_url"] = self.endpoint_url
        if self.aws_access_key_id:
            kw["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            kw["aws_secret_access_key"] = self.aws_secret_access_key
        if self._boto_config is not None:
            kw["config"] = self._boto_config
        return kw

    async def upload_file(self, src_path: str, dest_key: str) -> str:
        async with self._session.client("s3", **self._client_kwargs()) as client:
            # aioboto3 exposes upload_file as an awaitable wrapper
            await client.upload_file(src_path, self.bucket, dest_key)
        return dest_key

    async def download_file(self, key: str, dest_path: str) -> bool:
        async with self._session.client("s3", **self._client_kwargs()) as client:
            await client.download_file(self.bucket, key, dest_path)
        return True

    async def generate_presigned_post(self, key: str, expires: Optional[int] = None) -> Dict[str, Any]:
        expires = expires or config.PRESIGN_EXPIRES
        async with self._session.client("s3", **self._client_kwargs()) as client:
            # generate_presigned_post is a local signing operation (no network)
            post = client.generate_presigned_post(Bucket=self.bucket, Key=key, ExpiresIn=expires)
            get_url = client.generate_presigned_url("get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires * 24)
        return {"url": post["url"], "fields": post["fields"], "key": key, "get_url": get_url}

    async def generate_presigned_get(self, key: str, expires: Optional[int] = None) -> str:
        expires = expires or config.PRESIGN_EXPIRES
        async with self._session.client("s3", **self._client_kwargs()) as client:
            url = client.generate_presigned_url("get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires)
        return url

    async def delete(self, key: str) -> bool:
        async with self._session.client("s3", **self._client_kwargs()) as client:
            await client.delete_object(Bucket=self.bucket, Key=key)
        return True


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
