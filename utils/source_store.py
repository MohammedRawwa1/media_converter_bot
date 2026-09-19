"""One place that decides what a *source* costs to keep in object storage.

Every path that produces a source — the big-file pipeline, the Bot API download
behind the bot's buttons, the fetcher and the web uploader — used to carry its own
copy of this decision: choose a key, upload the whole file, remember where it
went. That is how the same media ended up stored three different ways, and how a
mode that exists to keep the bucket small was honoured by exactly one of them.

The modes are the same wherever a source is stored (``PIPELINE_SOURCE_UPLOAD``):

  ``header`` - only the first :func:`header_bytes` of the file, which is every
               common container header (MP4 ``moov``, MKV ``SegmentInfo``, AVI
               ``RIFF``). The bucket then holds a *probe reference*, never the
               media, and a repeated ingest can prove it has seen this file
               before without pulling it down again.
  ``full``   - the whole file, written after the download finished.
  ``stream`` - the whole file, written *while* it downloads: the MTProto chunks
               go through a multipart sink straight into the bucket, so no full
               local copy is ever needed and storage is the source of truth for a
               worker on any host. Only the pipeline can do this, because only the
               pipeline owns the download.
  ``local``  - nothing at all; the source stays on disk and the job carries its
               path.

A header is not a source. Any mode that stores one therefore leaves the job's
``input_key`` empty and marks the job ``input_header_only``, so no worker can ever
encode a two-megabyte "video" (see the worker's own refusal to download one).
That only works when the job has a *second* way to reach the media - the relay
copy the pipeline can re-read over MTProto. A producer that has no such fallback
(a Bot API file id, a web upload) must store the media itself even in ``header``
mode: a probe header plus no reachable copy is not a smaller source, it is a lost
one. :func:`store_source` takes ``telegram_fallback`` for exactly that, and says so
in the log when it keeps the whole object instead.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

#: Every accepted value of ``PIPELINE_SOURCE_UPLOAD``.
SOURCE_UPLOAD_MODES = ("header", "full", "local", "stream")

#: What the code does when the environment says nothing (see the README).
DEFAULT_SOURCE_UPLOAD = "header"

SOURCE_UPLOAD_ENV = "PIPELINE_SOURCE_UPLOAD"
HEADER_BYTES_ENV = "PIPELINE_HEADER_BYTES"
DEFAULT_HEADER_BYTES = 2 * 1024 * 1024

_FALSEY = ("0", "false", "no", "off")


def _env_positive_int(name: str, default: int) -> int:
    try:
        value = int((os.getenv(name) or "").strip())
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def source_upload_mode() -> str:
    """The configured mode, read at call time so a deployment can flip it live.

    An unknown value is not an error: it falls back to the code default rather
    than leaving a source unstored because of a typo.
    """
    mode = (os.getenv(SOURCE_UPLOAD_ENV) or "").strip().lower()
    return mode if mode in SOURCE_UPLOAD_MODES else DEFAULT_SOURCE_UPLOAD


def header_bytes() -> int:
    """How many leading bytes a probe header keeps."""
    return _env_positive_int(HEADER_BYTES_ENV, DEFAULT_HEADER_BYTES)


def stores_whole_object(mode: str | None = None) -> bool:
    """True when this mode puts the media itself in the bucket."""
    return (mode or source_upload_mode()) in ("full", "stream")


def stores_nothing(mode: str | None = None) -> bool:
    """True when this mode keeps no object at all."""
    return (mode or source_upload_mode()) == "local"


def header_object_key(input_s3_key: str) -> str:
    """The storage key of a source's header-only probe object.

    A distinct name on purpose: a probe object must never be mistaken for - or
    overwrite - the full ``.../source`` object a previous run may have stored.
    """
    if not input_s3_key:
        return "header"
    if input_s3_key.endswith("/source"):
        return f"{input_s3_key[: -len('source')]}header"
    return f"{input_s3_key}/header"


def read_head_bytes(path: str, limit: int | None = None) -> bytes:
    """Read at most *limit* bytes from the front of a file.

    A container header is all the bucket needs to hold, so the whole file is
    never read into memory to make one.
    """
    with open(path, "rb") as fh:
        return fh.read(limit if limit is not None else header_bytes())


def source_library_key(file_unique_id: Any) -> str | None:
    """The one shared key a media with an identity is stored under.

    One media is one object: every producer has to derive the *same* key for the
    same file, or a repeat of that file is stored again (and read over Telegram
    again) instead of being found where the last run left it.
    """
    if not file_unique_id:
        return None
    try:
        from utils import media_cache  # imported here: no cycle, no cost when unused

        return media_cache.media_library_key(file_unique_id)
    except Exception:
        return None


@dataclass(frozen=True)
class SourceRef:
    """What a producer just stored, and what it may tell a job about it.

    ``key`` is where the object went (None when nothing was stored), ``job_key``
    is the only value that may travel as a job's ``input_key`` - it is None for a
    probe header, which is a reference rather than a source.
    """

    mode: str
    key: str | None = None
    job_key: str | None = None
    header_only: bool = False
    bytes: int = 0

    @property
    def stored(self) -> bool:
        return self.key is not None


async def store_source(
    backend: Any,
    local_path: str | None,
    *,
    key: str | None,
    mode: str | None = None,
    header_key: str | None = None,
    telegram_fallback: bool = False,
    head_limit: int | None = None,
    log_prefix: str = "source_store",
) -> SourceRef:
    """Store *local_path* under *key* the way the mode says, and report what it is.

    ``telegram_fallback`` is the producer promising the worker a second way to
    reach the media (the relay copy the pipeline hands over as
    ``source_chat_id``/``source_message_id``). Only then can ``header`` mode store
    a probe header; without it the whole object is stored, because nothing else
    could supply the bytes.
    """
    resolved = mode or source_upload_mode()
    if backend is None or not local_path or not key or stores_nothing(resolved):
        # Nothing is stored: no backend, no bytes to store, or ``local`` mode
        # where the job is fed from this very disk.
        return SourceRef(mode=resolved)

    if resolved == "header":
        if not telegram_fallback:
            logger.info(
                "%s: %s mode asked for a probe header, but this path has no Telegram "
                "fallback - storing the whole object as %s",
                log_prefix,
                resolved,
                key,
            )
        elif hasattr(backend, "upload_bytes"):
            stored = header_key or header_object_key(key)
            head = read_head_bytes(local_path, head_limit)
            await backend.upload_bytes(head, stored)
            logger.info(
                "%s: stored the %dKB header of the source at %s (the media itself stays on Telegram)",
                log_prefix,
                len(head) // 1024,
                stored,
            )
            # Deliberately *not* a job key: callers persist the key and reuse it
            # as a source, and a header is not a source.
            return SourceRef(mode=resolved, key=stored, job_key=None, header_only=True, bytes=len(head))
        else:
            # A backend without partial uploads (the local one) cannot hold a
            # header on its own. Storing the media is the honest fallback: a
            # source that is not stored at all is worse than a large one.
            logger.info(
                "%s: %s does not support partial uploads - storing the whole object as %s (%s mode)",
                log_prefix,
                type(backend).__name__,
                key,
                resolved,
            )

    await backend.upload_file(local_path, key)
    try:
        size = int(os.path.getsize(local_path))
    except OSError:
        size = 0
    logger.info("%s: uploaded the source to %s (%dKB, mode=%s)", log_prefix, key, size // 1024, resolved)
    return SourceRef(mode=resolved, key=key, job_key=key, header_only=False, bytes=size)


async def record_source(
    file_unique_id: Any,
    *,
    ref: SourceRef,
    size: Any,
    name: str | None = None,
    local_path: str | None = None,
    data: bytes | None = None,
    file_id: Any = None,
    duration: Any = None,
    source_meta: dict | None = None,
    remember_bytes: bool = False,
) -> bool:
    """Remember where this media lives, in the one shape every reader expects.

    A probe header goes in ``header_key``/``header_only`` and never in
    ``input_key``: the descriptor is what the next request's validation gate
    reads, and a header in the source slot would hand a job the wrong bytes.
    """
    if not file_unique_id:
        return False
    try:
        from utils import media_cache

        payload = data
        if remember_bytes and payload is None and local_path:
            with open(local_path, "rb") as fh:
                payload = fh.read()
        if payload is not None and len(payload) > media_cache.bytes_cache_limit():
            payload = None
        kwargs: dict[str, Any] = {
            "size": size,
            "name": name,
            "storage": "s3",
            "data": payload,
            "file_id": file_id,
            "duration": duration,
            "source_meta": source_meta or None,
        }
        if ref.header_only:
            kwargs["header_key"] = ref.key
            kwargs["header_only"] = True
        else:
            kwargs["input_key"] = ref.key
        return await media_cache.remember(file_unique_id, **kwargs)
    except Exception:
        logger.debug("source_store: could not remember the source descriptor")
        return False
