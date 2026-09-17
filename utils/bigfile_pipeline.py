"""Big files pipeline: Pyrogram download → S3 upload → Redis queue → Worker → S3 → Pyrogram delivery.

Handles files that exceed the Telegram Bot API 50MB limit by routing them
through a userbot-based download, S3 storage, and worker processing pipeline.

Usage from handlers.py:
    from utils.bigfile_pipeline import BigFilePipeline

    pipeline = BigFilePipeline()
    result = await pipeline.ingest_large_file(
        chat_id=chat_id,
        message_id=message_id,
        file_size=file_size,
        file_unique_id=file_unique_id,
        user_id=user_id,
    )
    if result.ok:
        # File is queued — inform user and return
    else:
        # Fallback to normal Bot API download
"""

import asyncio
import contextlib
import logging
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass

import config
from utils import file_utils, media_cache

logger = logging.getLogger(__name__)

# Default thresholds — use config.BOT_API_MAX_MB as the single source of truth.
DEFAULT_BOT_API_MAX_MB = config.BOT_API_MAX_MB
DEFAULT_BOT_API_MAX_BYTES = config.BOT_API_MAX_BYTES

# How long one Pyrogram download may take before it is treated as failed.
#
# This is the only bound on a download that hangs: Pyrogram can sit on a dead
# connection forever waiting for a chunk that never arrives, and because the
# caller awaits this inline, an unbounded wait is indistinguishable from a
# frozen bot. 30 minutes is ~0.6 MB/s for a 1 GB source - slower than that and
# the file is not going to arrive anyway. 0 disables the bound.
PIPELINE_DOWNLOAD_TIMEOUT_SECONDS = float(os.getenv("PIPELINE_DOWNLOAD_TIMEOUT_SECONDS", "1800"))


def _env_positive_int(name: str, default: int) -> int:
    """Read a positive int env var, tolerating an unset, empty or garbage value.

    ``int(os.getenv(...))`` raises on the empty string a ``.env`` file leaves
    behind, which would take the whole bot down at import time.
    """
    try:
        value = int(str(os.getenv(name) or "").strip())
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


# How much of a fetched source is kept in the bucket.
#
# The bucket is a *copy*, never the transport for a job. The media itself is
# already on Telegram (that is how the userbot fetched it, and the relay copy it
# came from stays in the relay chat), and Pyrogram/Telethon read it from there
# for free - so the only thing the bucket has to hold is a small reference:
#
#   header - the first PIPELINE_HEADER_BYTES of the file, which is every common
#            container header (MP4 ``moov``, MKV ``SegmentInfo``, AVI ``RIFF``).
#            Default: a 500 MB video no longer costs 500 MB of stored bytes and
#            the whole-file read-back that used to inflate the egress counter.
#   full   - the whole file, as before, plus the local path on the job so a
#            worker sharing this disk never reads it back out again.
#   stream - the whole file, but written *while it downloads*: Telethon's chunks
#            go through a multipart sink straight into the bucket, so no full
#            local copy is ever needed and storage is the source of truth for a
#            worker on any host. Costs one read-back; see PIPELINE_HEADER_BYTES
#            for the zero-egress default.
#   local  - nothing at all is uploaded; the job carries the local path only.
PIPELINE_SOURCE_UPLOAD = (os.getenv("PIPELINE_SOURCE_UPLOAD") or "header").strip().lower()
if PIPELINE_SOURCE_UPLOAD not in ("header", "full", "local", "stream"):
    PIPELINE_SOURCE_UPLOAD = "header"
PIPELINE_HEADER_BYTES = _env_positive_int("PIPELINE_HEADER_BYTES", 2 * 1024 * 1024)

# Promote on repeat: a media whose first ingest stored only a probe header gets
# its whole object written the *second* time it is requested. The first request
# then costs 2 MB of storage and nothing else, and only media that actually come
# back pay for a whole copy - which is the trade `header` mode exists to keep
# open. Once promoted, every later request is served from the bucket by the same
# validation gate, so the media stops being read over Telegram once per job.
# `PIPELINE_PROMOTE_ON_REPEAT=0` restores pure header behaviour: evidence only,
# every job reads the media over Telegram.
PIPELINE_PROMOTE_ON_REPEAT_ENV = "PIPELINE_PROMOTE_ON_REPEAT"


def _promote_on_repeat() -> bool:
    """Whether a repeat of a header-only media is promoted to a whole object.

    Read at call time rather than import, so a deployment can flip the switch
    without a code change and both branches stay reachable in the tests.
    """
    value = (os.getenv(PIPELINE_PROMOTE_ON_REPEAT_ENV) or "").strip().lower()
    if not value:
        return True
    return value not in ("0", "false", "no", "off")


# Files up to this size (200MB) get streamed through memory instead of temp disk
# Imports — all guarded for optional dependencies
try:
    from utils.storage import get_storage_backend, stored_object_is_intact

except Exception:
    get_storage_backend = None

    async def stored_object_is_intact(*_args, **_kwargs):  # pragma: no cover - storage is absent
        """No storage means there is nothing to validate against."""
        return False


try:
    from utils.cache import get_cache
except Exception:
    get_cache = None


def _remove_partial(path: str | None) -> None:
    """Delete a partially downloaded file, tolerating every failure mode."""
    if not path:
        return
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        logger.debug("BigFilePipeline: could not remove partial download %s", path)


def _read_head_bytes(path: str, limit: int) -> bytes:
    """Read at most *limit* bytes from the front of a file.

    A container header is all the bucket needs to hold, so the whole file is
    never read into memory to make one.
    """
    with open(path, "rb") as fh:
        return fh.read(limit)


def _flatten_source_meta(meta: dict) -> dict:
    """Flatten an ffprobe result into the job's ``source_*`` fields.

    Shared by every path that probes a source, so the worker always reads one
    shape no matter which mode produced the job.
    """
    if not meta:
        return {}
    return {
        "source_duration": str(meta.get("duration", "")),
        "source_fps": str(meta.get("fps", "")),
        "source_video_codec": str(meta.get("video_codec", "")),
        "source_audio_codec": str(meta.get("audio_codec", "")),
        "source_width": str(meta.get("width", "")),
        "source_height": str(meta.get("height", "")),
        "source_video_bitrate": str(meta.get("video_bitrate", "")),
        "source_audio_bitrate": str(meta.get("audio_bitrate", "")),
        "source_rotation": str(meta.get("rotation", "")),
        "source_creation_time": str(meta.get("creation_time", "")),
        "source_language": str(meta.get("language", "")),
        "source_chapters": str(meta.get("chapters", 0)),
        "source_format": str(meta.get("format_name", "")),
    }


def _header_object_key(input_s3_key: str) -> str:
    """The storage key of a source's header-only probe object.

    A distinct name on purpose: a probe object must never be mistaken for - or
    overwrite - the full ``.../source`` object a previous run may have stored.
    """
    if input_s3_key.endswith("/source"):
        return f"{input_s3_key[: -len('source')]}header"
    return f"{input_s3_key}/header"


@dataclass
class IngestResult:
    """Result of a big file ingestion attempt."""

    ok: bool
    job_id: str | None = None
    s3_key: str | None = None
    error: str | None = None
    # The ingest's own ffprobe of the source, verbatim. The caller puts it on the
    # session's current_file as ``_source_metadata``, which is what the captions
    # and the audio tags are built from - without it a large file is delivered
    # with a filename-derived caption instead of the title/performer it carries.
    source_metadata: dict | None = None


class BigFilePipeline:
    """Orchestrates the large file ingestion pipeline."""

    def __init__(self):
        self._storage = None
        self._cache = None
        self._init_lock = asyncio.Lock()

    async def _ensure_initialized(self):
        """Lazy-init storage and cache backends."""
        if self._storage is not None:
            return
        async with self._init_lock:
            if self._storage is not None:
                return
            try:
                if get_storage_backend is not None:
                    self._storage = await get_storage_backend()
            except Exception as e:
                logger.warning("BigFilePipeline: storage init failed: %s", e)
            try:
                if get_cache is not None:
                    self._cache = await get_cache()
            except Exception:
                logger.debug("BigFilePipeline: cache init failed", exc_info=True)

    async def ingest_large_file(
        self,
        chat_id: int,
        message_id: int,
        file_size: int,
        file_unique_id: str | None = None,
        user_id: int | None = None,
        original_filename: str | None = None,
        ffmpeg_args: list | None = None,
        conversion_type: str | None = None,
        output_ext: str | None = None,
        caption: str | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
        batch_id: str | None = None,
        batch_seq: int = 0,
        batch_total: int = 0,
        cancel_check: Callable[[], bool] | None = None,
    ) -> IngestResult:
        """Download a large file via Pyrogram userbot, upload to S3, enqueue a processing job.

        This path uses a single disk-based download (no in-memory streaming) to avoid
        double-Pyrogram downloads. For files up to 200MB the previous in-memory path
        often failed silently, causing a fallback disk download that looked like two
        separate Pyrogram calls.

        Args:
            chat_id: Telegram chat ID where the file was sent.
            message_id: Telegram message ID of the file.
            file_size: Size of the file in bytes.
            file_unique_id: Telegram file_unique_id for caching/dedup.
            user_id: User who sent the file.
            original_filename: Original filename if known.
            ffmpeg_args: Custom FFmpeg arguments for processing.
            conversion_type: Type of conversion (e.g., "ffmpeg", "compress", "to_mp3").
            output_ext: Output file extension (e.g., ".mp4", ".mkv") when different from original.
            caption: Caption text for the output message.
            progress_callback: Optional callable(current_bytes, total_bytes) for download progress.

        Returns:
            IngestResult with job_id and s3_key on success.
        """
        await self._ensure_initialized()

        job_id = uuid.uuid4().hex

        # Determine extension from filename. The extension is allowlisted
        # so traversal/arbitrary suffixes can't reach the on-disk temp path.
        ext = file_utils.safe_extension(original_filename or "", ".bin")

        # Where the source lands on local disk, and whether it is stored whole,
        # as a header, or not at all. Both are decided here because every path
        # that can produce a source - the disk download, the byte cache, a reuse
        # - has to be able to hand its path to the job.
        temp_dir = os.path.abspath(os.path.join(os.getenv("STORAGE_PATH", "storage"), "temp"))
        temp_path: str | None = os.path.join(temp_dir, f"{job_id}_src{ext}")
        _header_only = self._storage is not None and PIPELINE_SOURCE_UPLOAD == "header"
        # The same switch the Bot API path uses (handlers.REUSE_LOCAL_INPUT): the
        # worker usually runs in this container, so keeping the file lets it read
        # the source off disk instead of pulling it back out of the bucket. The
        # hint is safe to lose - the worker falls back to Telegram, then storage.
        _keep_local_input = (
            os.getenv("REUSE_LOCAL_INPUT", "1").strip().lower()
            not in (
                "0",
                "false",
                "no",
                "off",
            )
            or PIPELINE_SOURCE_UPLOAD == "local"
        )

        # Where the input lands. With the media cache on, the key is derived from
        # the media identity rather than the job id, so a *repeat* of the same
        # file maps to the same object and can be reused instead of downloaded
        # from Telegram and uploaded again.
        _library_key = media_cache.media_library_key(file_unique_id) if file_unique_id else None
        # Only a whole object can be shared between jobs as a *source*. A header
        # is a probe reference, not a source, so `header`/`local` runs keep the
        # whole-media library out of it entirely (their probe header still gets a
        # content-addressed key below). `stream` stores a whole object too, so it
        # gets the same shared key - the media only ever has to leave Telegram
        # once, whichever job asked for it first.
        _shared_input = bool(
            _library_key and media_cache.cache_enabled() and PIPELINE_SOURCE_UPLOAD in ("full", "stream")
        )
        input_s3_key = _library_key if _shared_input else f"inputs/{job_id}/source"
        # The probe header is content-addressed as well whenever the media has an
        # identity: one small object per media, at the same place the README
        # documents, instead of a per-job slice nobody can ever find again. That
        # is what makes "we have seen this media before" provable on a repeat -
        # the descriptor points at it, and existence plus a ranged read is the
        # evidence. A run without an identity keeps the per-job fallback key.
        _header_key = _header_object_key(_library_key) if _library_key and media_cache.cache_enabled() else None

        actual_size = 0
        s3_key = input_s3_key
        _reused = False
        _reuse_reason = None
        _local_reuse_path = None
        # Set by the gate below: this run re-fetches the media in order to store
        # it whole, rather than only proving it was seen before.
        _promote = False
        _bytes_hit = False
        _source_meta_fields = {}
        # The raw probe (title/performer included), carried out to the caller.
        _source_meta_raw: dict = {}

        # ── Validate before downloading ──
        # Same file_unique_id AND same byte size => the earlier copy is still the
        # right one, so Pyrogram is skipped entirely. Every tier is validated
        # against storage first (existence, and the stored size when the backend
        # can report it), because a descriptor whose object was swept or replaced
        # is worse than a miss: it would hand the job the wrong bytes.
        #
        #   1. a whole object  (``full``/``stream``)  -> served from the bucket
        #   2. a local copy    (``REUSE_LOCAL_INPUT``) -> read off this disk
        #   3. a probe header  (``header``)            -> proof the ingest already
        #      ran, so only the worker's own single read is left - the pipeline no
        #      longer downloads the whole file a second time just to prove it was
        #      here. A header is never handed over as a source. On a *repeat* it
        #      instead promotes the media (see PIPELINE_PROMOTE_ON_REPEAT): the
        #      second request is where a whole object starts paying for itself,
        #      because from then on no job has to read Telegram.
        if media_cache.cache_enabled() and file_unique_id:
            _entry = await media_cache.lookup(file_unique_id, expected_size=file_size)
            _stored_key = (_entry or {}).get("input_key")
            if _stored_key and self._storage is not None:
                if await stored_object_is_intact(self._storage, _stored_key, expected_size=file_size):
                    s3_key = _stored_key
                    actual_size = int(_entry.get("size") or file_size or 0)
                    _reused = True
                    _reuse_reason = "stored source"
            elif _stored_key and self._storage is None and os.path.exists(_stored_key):
                s3_key = _stored_key
                actual_size = int(_entry.get("size") or file_size or 0)
                _reused = True
                _reuse_reason = "stored source"
            if not _reused:
                _stored_path = (_entry or {}).get("path")
                if _stored_path and os.path.exists(_stored_path):
                    _local_reuse_path = _stored_path
                    # No *validated* storage object backs this run, so the job
                    # must not carry a key that may not exist: the path travels
                    # as ``input_path`` and Telegram stays the fallback.
                    s3_key = None
                    actual_size = int(_entry.get("size") or file_size or 0)
                    _reused = True
                    _reuse_reason = "local copy"
            if not _reused and self._storage is not None:
                _stored_header = (_entry or {}).get("header_key")
                if _stored_header and await stored_object_is_intact(self._storage, _stored_header):
                    actual_size = int(_entry.get("size") or file_size or 0)
                    if PIPELINE_SOURCE_UPLOAD == "header" and _library_key and _promote_on_repeat():
                        # Second request for this media: download it once more and
                        # store the whole object, so every later request (and
                        # every style applied to it) is a bucket read instead of
                        # a Telegram read. This is the only repeat that pays for
                        # the media; the header stays as the evidence it was.
                        _promote = True
                        logger.info(
                            "Job %s: %s came back - promoting it to a whole shared object at %s",
                            job_id,
                            file_unique_id,
                            _library_key,
                        )
                    else:
                        s3_key = None
                        _reused = True
                        _reuse_reason = "probe header"
            if _reused:
                # The ingest's own probe verdict travels with the descriptor: a
                # repeat must not lose the caption's duration/codecs/title just
                # because the download it used to do is gone. A descriptor that
                # somehow carries no verdict simply leaves the worker to probe.
                _cached_meta = (_entry or {}).get("source_meta")
                _source_meta = dict(_cached_meta) if isinstance(_cached_meta, dict) else {}
                _source_meta_raw = dict(_source_meta)
                _source_meta_fields = _flatten_source_meta(_source_meta)
                logger.info(
                    "Job %s: media cache HIT (%s) for %s (%dMB) - no Telegram download",
                    job_id,
                    _reuse_reason,
                    file_unique_id,
                    actual_size // (1024 * 1024),
                )

        # ── Byte cache (small media): skips Pyrogram even when the storage
        #    object has gone away ──
        if not _reused and self._cache and file_unique_id:
            try:
                cached_data = await media_cache.get_bytes(file_unique_id, expected_size=file_size)
                if cached_data:
                    logger.info(
                        "BigFilePipeline: byte cache HIT for %s (%dMB)",
                        file_unique_id,
                        len(cached_data) // (1024 * 1024),
                    )
                    actual_size = len(cached_data)
                    if PIPELINE_SOURCE_UPLOAD == "full":
                        if self._storage is not None:
                            await self._storage.upload_bytes(cached_data, input_s3_key)
                        s3_key = input_s3_key
                    else:
                        # Nothing but a probe reference goes to the bucket in
                        # these modes, and a header is not a source - so the
                        # cached body is written out for the local handoff.
                        os.makedirs(temp_dir, exist_ok=True)
                        with open(temp_path, "wb") as _fh:
                            _fh.write(cached_data)
                        s3_key = None
                    _bytes_hit = True
            except Exception as e:
                logger.debug("BigFilePipeline: byte cache check failed: %s", e)

        # ── Stream straight into storage (PIPELINE_SOURCE_UPLOAD=stream) ──
        # The MTProto download writes the bucket itself: Telethon hands each
        # chunk to a multipart sink, so the media never needs a full local copy
        # and the object becomes the source of truth for a worker on any host.
        # Only the container header is kept in memory long enough to probe it,
        # which is how the job still carries real ``source_*`` metadata without a
        # second read of anything. A failure here is not fatal: nothing partial is
        # left in the bucket and the disk path below takes over.
        _streamed = False
        if (
            not _reused
            and not _bytes_hit
            and PIPELINE_SOURCE_UPLOAD == "stream"
            and self._storage is not None
            and hasattr(self._storage, "open_upload_sink")
        ):
            try:
                _stream = await self._stream_source_to_storage(
                    chat_id=chat_id,
                    message_id=message_id,
                    input_s3_key=input_s3_key,
                    file_unique_id=file_unique_id,
                    original_filename=original_filename,
                    expected_size=file_size,
                    progress_callback=progress_callback,
                    cancel_check=cancel_check,
                    user_id=user_id,
                    temp_dir=temp_dir,
                )
            except asyncio.CancelledError:
                return IngestResult(ok=False, error="batch cancelled")
            except Exception as e:
                logger.warning("BigFilePipeline: streaming upload failed (%s); falling back to disk", e)
                _stream = None
            if _stream:
                _streamed = True
                s3_key = _stream["s3_key"]
                actual_size = _stream["size"]
                _source_meta_fields = _stream["meta_fields"]
                _source_meta_raw = dict(_stream.get("meta") or {})

        # ── Disk-based download (single Pyrogram call, always used) ──
        # The previous in-memory path (download_bytes_via_userbot) often failed for files
        # 20-200MB, causing a fallback disk download that looked like two Pyrogram calls.
        # Now we always use the single disk-based path with progress callback support.
        if not _reused and not _bytes_hit and not _streamed:
            try:
                os.makedirs(temp_dir, exist_ok=True)

                logger.info(
                    "BigFilePipeline: downloading via Pyrogram chat=%s msg=%s size=%dMB -> %s",
                    chat_id,
                    message_id,
                    file_size // (1024 * 1024),
                    temp_path,
                )

                def _download_progress(sent: int, total: int):
                    if cancel_check and cancel_check():
                        raise asyncio.CancelledError("batch cancelled during pipeline download")
                    if progress_callback:
                        progress_callback(sent, total)

                download_ok = await self._download_via_pyrogram(
                    chat_id,
                    message_id,
                    temp_path,
                    progress_callback=_download_progress,
                    user_id=user_id,
                )
                if not download_ok or not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
                    return IngestResult(
                        ok=False,
                        error="Pyrogram download failed",
                    )

                actual_size = os.path.getsize(temp_path)
                logger.info("BigFilePipeline: disk download complete, actual_size=%dMB", actual_size // (1024 * 1024))

                # ── T4: ffprobe source analysis ──
                _source_meta = {}
                try:
                    from utils.ffmpeg_runner import probe_media

                    _source_meta = await probe_media(temp_path)
                except Exception:
                    logger.debug("BigFilePipeline: source ffprobe failed, continuing without metadata")

                # Include source_metadata in the job payload dict for the worker
                # (will be merged into Redis hash by enqueue_job below — single atomic hset)
                if _source_meta:
                    _source_meta_raw = dict(_source_meta)
                    _source_meta_fields = _flatten_source_meta(_source_meta)

                # The media descriptor is written once, after the upload, by
                # media_cache.remember() below - it carries the storage key the
                # reuse path needs, which this legacy write did not.

            except asyncio.CancelledError:
                with contextlib.suppress(OSError):
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                return IngestResult(ok=False, error="batch cancelled")
            except Exception as e:
                logger.exception("BigFilePipeline: Pyrogram download error: %s", e)
                return IngestResult(ok=False, error=f"Download error: {e}")

            if cancel_check and cancel_check():
                return IngestResult(ok=False, error="batch cancelled")

            # ── Keep a reference in the bucket, hand the source to the job ──
            # What is stored depends on PIPELINE_SOURCE_UPLOAD: the whole file
            # (``full``), its container header (``header``, the default), or
            # nothing (``local``). The local path travels on the job either way,
            # so a worker that shares this disk re-reads the bytes it already has
            # instead of pulling a second copy of the media out of storage.
            #
            # ``stream`` never reaches here: its download already wrote the
            # object and the descriptor, which is what put it in the disk-block
            # above - so there is nothing left to store for it.
            _upload_enabled = self._storage is not None and PIPELINE_SOURCE_UPLOAD != "local"
            try:
                if _upload_enabled:
                    # What this run stores:
                    #   a promoted repeat -> the shared whole-object key
                    #   a header-only run -> the content-addressed probe header
                    #   anything else     -> this run's own key (a whole object)
                    if _promote and _library_key:
                        _stored_key = _library_key
                    elif _header_only:
                        _stored_key = _header_key or _header_object_key(input_s3_key)
                    else:
                        _stored_key = input_s3_key
                    if _header_only and not _promote:
                        _head = _read_head_bytes(temp_path, PIPELINE_HEADER_BYTES)
                        await self._storage.upload_bytes(_head, _stored_key)
                        # Deliberately *not* reported as this job's storage key:
                        # callers persist the key and reuse it as a source, and a
                        # header is not a source. Nothing downloads it either -
                        # the worker's own probe can, but only as a last resort.
                        s3_key = None
                        logger.info(
                            "BigFilePipeline: stored the %dKB header of the source at %s "
                            "(the media itself stays on Telegram)",
                            len(_head) // 1024,
                            _stored_key,
                        )
                    else:
                        logger.info("BigFilePipeline: uploading to S3 key=%s", _stored_key)
                        await self._storage.upload_file(temp_path, _stored_key)
                        s3_key = _stored_key
                        logger.info("BigFilePipeline: S3 upload complete")

                    # The descriptor is written in every mode, and it is what the
                    # next submission's validation gate reads. Only a whole object
                    # is handed over as ``input_key``: a header goes in
                    # ``header_key``, so it proves the media was already ingested
                    # without ever being mistaken for something to encode from.
                    # A promotion writes the whole object, so its descriptor is
                    # the whole-object one - the header is superseded.
                    if _header_only and not _promote:
                        await media_cache.remember(
                            file_unique_id,
                            size=actual_size,
                            header_key=_stored_key,
                            header_only=True,
                            name=original_filename,
                            storage="s3",
                            duration=_source_meta.get("duration"),
                            source_meta=_source_meta or None,
                        )
                    else:
                        _payload = None
                        if (
                            media_cache.cache_enabled()
                            and file_unique_id
                            and actual_size
                            and actual_size <= media_cache.bytes_cache_limit()
                        ):
                            with contextlib.suppress(Exception), open(temp_path, "rb") as _fh:
                                _payload = _fh.read()
                        await media_cache.remember(
                            file_unique_id,
                            size=actual_size,
                            input_key=_stored_key,
                            name=original_filename,
                            storage="s3",
                            data=_payload,
                            duration=_source_meta.get("duration"),
                            source_meta=_source_meta or None,
                        )
                elif self._storage is None:
                    # No S3 — keep the file locally
                    s3_key = temp_path
                    logger.info("BigFilePipeline: no S3 backend, using local path: %s", temp_path)
                    await media_cache.remember(
                        file_unique_id,
                        size=actual_size,
                        path=temp_path,
                        name=original_filename,
                        storage="local",
                        duration=_source_meta.get("duration"),
                    )
                else:
                    # ``local`` mode with a backend configured: deliberately
                    # nothing is stored. The job is fed from this disk.
                    s3_key = None
                    logger.info(
                        "BigFilePipeline: PIPELINE_SOURCE_UPLOAD=local - keeping the source on disk only (%s)",
                        temp_path,
                    )
            except Exception as e:
                logger.exception("BigFilePipeline: S3 upload failed: %s", e)
                s3_key = temp_path if self._storage is None else None

        # Step 3: Enqueue processing job
        try:
            from utils.job_queue import enqueue_job

            # Determine output extension: prefer caller-supplied output_ext,
            # otherwise fall back to original filename extension or .mp4
            # Output extension: prefer caller-supplied (handler-set) value,
            # otherwise derive from the allowlisted original-filename ext.
            _out_ext = output_ext or file_utils.safe_extension(original_filename or "", ".mp4")
            _source_name = original_filename or f"file_{job_id}{ext}"
            _safe_source_name = await file_utils.sanitize_filename(_source_name)
            _source_stem = os.path.splitext(_safe_source_name)[0] or f"file_{job_id}"
            _output_filename = f"{_source_stem}{_out_ext}"

            # Hand the local copy to the job as well as the stored key: a worker
            # that shares this filesystem reads the bytes it already has, and
            # only one that does not has to touch the bucket at all.
            _local_source = temp_path if (_keep_local_input and temp_path and os.path.exists(temp_path)) else None
            if not _local_source and _local_reuse_path and os.path.exists(_local_reuse_path):
                # A validated local copy from an earlier run: hand it over rather
                # than making the worker fetch the same bytes again.
                _local_source = _local_reuse_path
            if not _local_source:
                # Reuse is off: the copy has served its purpose (it is in the
                # bucket, or it was never wanted there) and is removed as before.
                with contextlib.suppress(Exception):
                    if temp_path and os.path.exists(temp_path):
                        os.remove(temp_path)
            # Build the job payload
            job = {
                "job_id": job_id,
                # A header-only object is left off the payload entirely: it is
                # not the media, and the worker's job is to encode the media. A
                # promoted run stored the media itself, so it hands the key over.
                "input_key": s3_key if (self._storage is not None and (not _header_only or _promote)) else None,
                "input_path": _local_source if _local_source else (s3_key if self._storage is None else None),
                # Where the userbot fetched this file from. The relay copy is
                # still on Telegram, so a worker that cannot see this disk can
                # read the media over MTProto (no bucket egress) before it
                # considers pulling a stored object back out.
                "source_chat_id": chat_id,
                "source_message_id": message_id,
                # A stored object that is only a header is a probe reference:
                # no worker may ever encode from it.
                "input_header_only": 1 if (_header_only and not _promote) else 0,
                # chat_id for delivery = user_id (the person who should receive
                # the processed result). The original chat_id was used for download
                # (may be a relay group) but the result must go to the user's DM.
                "chat_id": user_id or chat_id,
                "user_id": user_id,
                "message_id": message_id,
                "original_filename": _safe_source_name,
                "output_filename": _output_filename,
                "file_unique_id": file_unique_id,
                "file_size": actual_size,
                "progress_channel": f"ffmpeg:progress:{job_id}",
                # A shared input must survive this job so later jobs can reuse it.
                # With no storage backend the shared input IS the pipeline's own
                # local file, so it has to be kept. On S3/R2 the shared object is
                # never deleted by the worker anyway, and the worker's local temp
                # copy of it must still be cleaned up - hence cleanup stays on.
                "cleanup_input": not (_shared_input and self._storage is None),
                "type": conversion_type or "ffmpeg",
                "created_at": time.time(),
            }
            # Include conversion-specific metadata when provided by the caller
            if ffmpeg_args:
                job["ffmpeg_args"] = ffmpeg_args
            if caption:
                job["caption"] = caption
            # Include output_ext so the worker knows what format extension to use
            job["output_ext"] = _out_ext

            # For extract_streams, include output_dir and archive_path so the
            # worker's extract_streams branch can place extracted files and create the zip.
            if conversion_type == "extract_streams":
                _out_base = getattr(config, "OUTPUT_PATH", "storage/output")
                _streams_dir = os.path.join(_out_base, f"{job_id}_streams")
                job["output_dir"] = _streams_dir
                job["archive_path"] = f"{_streams_dir}.zip"

            if batch_id:
                from utils.batch_pipeline import tag_batch_job

                tag_batch_job(job, batch_id, batch_seq, batch_total)

            # Enqueue the job first — enqueue_job creates the Redis hash
            # with its own mapping (status=queued, progress=0, ...).
            if cancel_check and cancel_check():
                return IngestResult(ok=False, error="batch cancelled")
            await enqueue_job(job)

            # Write source metadata to the same Redis hash in a single atomic
            # hset AFTER enqueue_job so there is no write-ordering race.
            # hset is additive (does not clear other fields), so both
            # enqueue_job's mapping and these metadata fields coexist.
            if _source_meta_fields:
                try:
                    from utils.job_queue import get_redis as _get_r

                    _r = await _get_r()
                    try:
                        await _r.hset(f"ffmpeg:job:{job_id}", mapping=_source_meta_fields)
                    finally:
                        await _r.close()
                except Exception:
                    logger.debug("BigFilePipeline: failed to store source metadata in Redis hash")

            logger.info("BigFilePipeline: job %s enqueued (input_key=%s)", job_id, s3_key)

            return IngestResult(
                ok=True,
                job_id=job_id,
                s3_key=s3_key,
                source_metadata=_source_meta_raw or None,
            )

        except Exception as e:
            logger.exception("BigFilePipeline: enqueue failed: %s", e)
            return IngestResult(ok=False, error=f"Enqueue error: {e}")

    async def _stream_source_to_storage(
        self,
        *,
        chat_id: int,
        message_id: int,
        input_s3_key: str,
        file_unique_id: str | None,
        original_filename: str | None,
        expected_size: int,
        progress_callback: Callable[[int, int], None] | None,
        cancel_check: Callable[[], bool] | None,
        user_id: int | None,
        temp_dir: str,
    ) -> dict | None:
        """Stream the Telegram file into storage while it is still downloading.

        The bytes go through a multipart sink (:meth:`open_upload_sink`), so the
        media is never staged on local disk: storage holds the source of truth
        and a worker on any host can read it back. Only the container header is
        tapped on the way past, and it is probed from a throwaway file so the job
        still carries the ``source_*`` metadata the worker expects.

        Returns ``{"s3_key", "size", "meta_fields"}`` on success. On any failure
        the multipart upload is aborted - there is no half-written object for a
        later job to mistake for a source - and ``None`` is returned so the
        caller can fall back to the disk download.
        """
        from utils.storage import HeadCaptureSink
        from utils.userbot_downloader import download_media_to_sink

        sink = await self._storage.open_upload_sink(input_s3_key)
        head_sink = HeadCaptureSink(sink, PIPELINE_HEADER_BYTES)
        # A throwaway path for the header: the probe needs a real file, and this
        # one is removed again no matter how the probe ends.
        probe_path = os.path.join(temp_dir, f"stream_head_{uuid.uuid4().hex}.bin")
        try:
            os.makedirs(temp_dir, exist_ok=True)
            await head_sink.open()
            logger.info(
                "BigFilePipeline: streaming %s/%s straight into %s",
                chat_id,
                message_id,
                input_s3_key,
            )

            # Cancellation has to interrupt the transfer itself, not just be
            # noticed once it finishes: a batch cancel during a 500MB stream must
            # not wait for the whole file. This mirrors the disk path's wrapper.
            def _stream_progress(sent: int, total: int):
                if cancel_check and cancel_check():
                    raise asyncio.CancelledError("batch cancelled during stream upload")
                if progress_callback:
                    progress_callback(sent, total)

            ok = await download_media_to_sink(
                chat_id,
                message_id,
                head_sink,
                expected_size=expected_size or None,
                progress_callback=_stream_progress,
                user_id=user_id,
            )
            if not ok:
                await head_sink.abort()
                return None
            if cancel_check and cancel_check():
                await head_sink.abort()
                raise asyncio.CancelledError("batch cancelled during stream upload")

            size = int(head_sink.tell() or 0)
            if size <= 0:
                # Nothing to complete: an empty object is worse than no object,
                # because a job would later find it and treat it as a source.
                await head_sink.abort()
                return None
            key = await head_sink.close()

            head = head_sink.head
            meta: dict = {}
            if head:
                try:
                    with open(probe_path, "wb") as fh:
                        fh.write(head)
                    from utils.ffmpeg_runner import probe_media

                    meta = await probe_media(probe_path) or {}
                except Exception:
                    logger.debug("BigFilePipeline: header probe failed; the worker will probe the source it fetches")

            # The object is the whole source, so unlike a header-only run it can
            # be reused later - that is exactly what makes this the source of
            # truth. A small file whose entire body fit in the capture is also
            # worth keeping in Redis so a repeat needs storage at all.
            await media_cache.remember(
                file_unique_id,
                size=size,
                input_key=key,
                name=original_filename,
                storage="s3",
                data=head if head and size <= len(head) else None,
                duration=meta.get("duration"),
                source_meta=meta or None,
            )
            logger.info(
                "BigFilePipeline: streamed %dMB into storage as %s (no local copy)",
                size // (1024 * 1024),
                key,
            )
            return {
                "s3_key": key,
                "size": size,
                "meta_fields": _flatten_source_meta(meta),
                # The raw probe, so the caller can hand the media's own title and
                # performer to the caption builder.
                "meta": meta,
            }
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await head_sink.abort()
            raise
        except Exception:
            with contextlib.suppress(Exception):
                await head_sink.abort()
            raise
        finally:
            _remove_partial(probe_path)

    async def _download_via_pyrogram(
        self,
        chat_id: int,
        message_id: int,
        dest_path: str,
        progress_callback: Callable[[int, int], None] | None = None,
        user_id: int | None = None,
    ) -> bool:
        """Download a message using Pyrogram userbot.

        Args:
            chat_id: Telegram chat ID where the file is.
            message_id: Telegram message ID of the file.
            dest_path: Local path to save the downloaded file.
            progress_callback: Optional callable(current_bytes, total_bytes) for progress.
            user_id: Optional Telegram user ID for per-user session resolution.

        Returns True on success, False on failure. Never raises, and never waits
        forever: a download that outlives
        :data:`PIPELINE_DOWNLOAD_TIMEOUT_SECONDS` is abandoned and its partial
        file removed, so whoever is waiting on this is told it failed instead of
        being left hanging on a dead connection.
        """

        async def _download() -> bool:
            from utils.userbot_downloader import download_forward_via_userbot

            return await download_forward_via_userbot(
                chat_id=chat_id,
                message_id=message_id,
                dest_path=dest_path,
                progress_callback=progress_callback,
                user_id=user_id,
            )

        started = time.monotonic()
        try:
            if PIPELINE_DOWNLOAD_TIMEOUT_SECONDS > 0:
                ok = await asyncio.wait_for(_download(), timeout=PIPELINE_DOWNLOAD_TIMEOUT_SECONDS)
            else:
                ok = await _download()
            return bool(ok)
        except TimeoutError:
            # ``asyncio.wait_for`` raises the builtin (it is the alias of
            # ``asyncio.TimeoutError`` on 3.11+).
            logger.warning(
                "BigFilePipeline: Pyrogram download timed out after %.0fs (chat=%s msg=%s -> %s); "
                "treating it as a failed fetch",
                time.monotonic() - started,
                chat_id,
                message_id,
                dest_path,
            )
            _remove_partial(dest_path)
            return False
        except asyncio.CancelledError:
            # The caller gave up (a batch that stopped, or a fetch timeout): drop
            # whatever was written so a cancelled download cannot leave a
            # half-file behind for the next file to trip over.
            _remove_partial(dest_path)
            raise
        except Exception as e:
            logger.exception("BigFilePipeline: Pyrogram download failed: %s", e)
            _remove_partial(dest_path)
            return False
