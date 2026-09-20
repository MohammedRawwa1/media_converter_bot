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
import uuid
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


async def stored_library_source(backend: Any, file_unique_id: Any, *, expected_size: Any = None) -> str | None:
    """The whole object already stored for this media's identity, if there is one.

    A descriptor is a *record* of what a producer stored, and a record can be
    missing: a path that stored the media never wrote one, both cache tiers can
    lose one, and a descriptor written in a header-keeping mode holds no reusable
    source at all. The key does not have that problem - it is derived from the
    media's own ``file_unique_id`` (``utils/media_cache.media_library_key``), so
    an object the last run stored under it is reachable without asking anyone.

    Only a *whole* object is ever found this way: a probe header is stored beside
    the source, never on it (see :func:`header_object_key`), so a key that answers
    here is media and never a two-megabyte reference to some.

    Stricter than the descriptor path on purpose. That one treats a backend it
    cannot question as a hit, because a record already proves a producer wrote
    the object and doubting it is what causes the re-download it exists to
    prevent. There is no such proof behind a *derived* key, so this one wants a
    positive answer: without it the caller downloads, which is exactly what it
    would have done before (and never a request that trusts the wrong bytes).
    """
    if backend is None or not file_unique_id:
        return None
    key = source_library_key(file_unique_id)
    if not key:
        return None
    try:
        from utils.storage import stored_object_is_intact

        if not await stored_object_is_intact(backend, key, expected_size=expected_size):
            return None
        # ``stored_object_is_intact`` is forgiving about a backend that cannot
        # answer at all; the size it reported (when it reported one) is the part
        # of its verdict that this path needs to stand on its own.
        if expected_size:
            stored_size = await backend.get_file_size(key)
            if stored_size is not None and int(stored_size) != int(expected_size):
                return None
    except Exception:
        logger.debug("source_store: could not check the stored object at %s", key)
        return None
    logger.info("source_store: the media itself is already stored at %s", key)
    return key


async def remember_fetched_source(
    current_file: dict | None,
    local_path: str | None,
    *,
    source_meta: dict | None = None,
    telegram_fallback: bool = False,
    log_prefix: str = "source_store",
) -> str | None:
    """Keep what a fetch just wrote to disk, and record where it now lives.

    The fetches that reach Telegram on a media's behalf - the userbot/relay
    download behind the bot's buttons, the auto-fetch that answers a large
    forward - used to hand the pipe a local file and nothing else. That left the
    media invisible to every reuse path: the *next* request for it downloaded the
    same bytes down the same road, and a worker on another host had no object to
    read, so it went back to Telegram too. One fetch of one media is enough once
    the producer records it, so these paths write what the piped Bot API branch
    writes: the object, then the descriptor that says where it is.

    Best-effort, because the caller already has its bytes: no backend, a storage
    backend that is down, ``PIPELINE_SOURCE_UPLOAD=local``, or a deployment whose
    store is this very disk all mean "no object to write" rather than a failed
    request - the local copy and its bytes are recorded either way, because that
    is the tier such a deployment reuses from. Returns the job-usable key (None
    when no object was stored).
    """
    file = current_file or {}
    if not local_path or not os.path.exists(local_path):
        return None
    try:
        size = int(os.path.getsize(local_path))
    except OSError:
        return None
    if size <= 0:
        return None

    # Only a remote store is worth *uploading* to. A local backend already *is*
    # this disk, so a copy of it would be the same bytes under another name - but
    # the fetch is recorded either way: the disk copy and its bytes are the tiers
    # a deployment with no object store reads, and the descriptor is what tells
    # the next request they are there.
    try:
        import config as _config

        _remote = _config.get_storage_backend_name() in ("s3", "r2")
    except Exception:
        _remote = False

    uid = file.get("file_unique_id")
    ext = os.path.splitext(str(file.get("name") or local_path))[1] or ".bin"
    # One media is one object: the identity key when there is an identity, so a
    # second route to the same media finds this copy instead of storing another.
    key = source_library_key(uid) or f"inputs/{uuid.uuid4().hex}/source{ext}"

    backend = None
    if _remote:
        try:
            from utils.storage import get_storage_backend

            if get_storage_backend is not None:
                backend = await get_storage_backend()
        except Exception:
            backend = None

    ref = SourceRef(mode=source_upload_mode())
    if backend is not None:
        try:
            ref = await store_source(
                backend,
                local_path,
                key=key,
                telegram_fallback=telegram_fallback,
                log_prefix=log_prefix,
            )
        except Exception:
            logger.warning("%s: could not store the fetched source at %s", log_prefix, key)

    if not uid:
        # No identity to record against. The key still goes back to the request
        # that is about to queue its job, which is the only reader it has.
        return ref.job_key

    if not ref.stored:
        # Nothing new was stored. Whatever a previous producer recorded is a
        # better answer than blanking the descriptor, so it is carried over.
        previous = {}
        try:
            from utils import media_cache

            previous = await media_cache.lookup(uid, expected_size=size) or {}
        except Exception:
            previous = {}
        if previous.get("header_only"):
            ref = SourceRef(mode=ref.mode, key=previous.get("header_key"), header_only=True)
        elif previous.get("input_key"):
            ref = SourceRef(mode=ref.mode, key=previous.get("input_key"), job_key=previous.get("input_key"))

    payload = None
    try:
        from utils import media_cache

        if size <= media_cache.bytes_cache_limit():
            with open(local_path, "rb") as fh:
                payload = fh.read()
    except Exception:
        payload = None

    await record_source(
        uid,
        ref=ref,
        size=size,
        name=file.get("name"),
        # Where this fetch put the bytes: the tier a repeat reads when the
        # object is gone (or was never written), instead of downloading again.
        local_path=local_path,
        data=payload,
        file_id=file.get("id"),
        source_meta=source_meta,
        storage="s3" if ref.stored else "local",
    )
    return ref.job_key


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
    storage: str = "s3",
) -> bool:
    """Remember where this media lives, in the one shape every reader expects.

    A probe header goes in ``header_key``/``header_only`` and never in
    ``input_key``: the descriptor is what the next request's validation gate
    reads, and a header in the source slot would hand a job the wrong bytes.

    ``local_path`` goes in ``path`` - the reuse tier a request reads after the
    stored key, and the only one a deployment with no object store has. A fetch
    that left its bytes on disk and wrote no key used to be told twice: nothing
    in the descriptor said where the first one had put the media, so the next
    request downloaded it again over the same road.

    ``storage`` names which of the two the descriptor is describing - ``s3`` for
    a stored object, ``local`` when only the disk copy exists - so the record
    does not claim an object that was never written.
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
            "storage": storage,
            "path": local_path,
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
