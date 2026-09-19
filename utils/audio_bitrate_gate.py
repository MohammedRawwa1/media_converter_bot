"""Compare a source's own audio bitrate with the one a request asks for.

"Adjust Bitrate 64k" on a file that is already 64k has nothing to encode, and
the expensive part of answering it anyway is the *fetch*. A 47MB audio is past
what the Bot API will hand a bot, so acting on that request used to mean falling
through to the userbot pipeline, downloading the whole media and re-encoding it
into a file identical to the one the user sent.

The verdict is reached in three steps, in this order:

  compare   read what the source already carries - from the ingest's ffprobe
            verdict when the media has been through the pipe, otherwise from a
            small *header* read. Where that read is aimed comes first from the
            media cache descriptor the ingest wrote (it names the object the
            media was stored under, and carries the verdict it probed), and then
            from the shared library key the media's own identity derives - so a
            media this deployment already holds is answered from storage rather
            than from Telegram. A small *header* read is what answers the rest:
            the local file, a ranged GET of the stored object, or the first
            bytes over MTProto;
  validate  a bitrate alone is not a match. The source has to be the codec the
            request would produce, or "AAC 64k -> MP3 64k" would be read as a
            no-op when it is a real conversion. Anything unknown is not a match;
  exists    when both agree, the file the request would produce already exists,
            so the caller reports it and stops instead of fetching and encoding.

Nothing here is ever a blocker: an unreadable verdict, a failed probe or a
timeout all answer "not already", which leaves the caller on exactly the path it
would have taken before this module existed.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import tempfile
import time
import uuid

logger = logging.getLogger(__name__)

#: ``AUDIO_BITRATE_GATE=0`` turns the whole check off.
#:
#: Every request then takes exactly the path it took before this module existed.
#: It is here so a deployment that would rather always re-encode - or whose
#: userbot must not be asked to open a media - can say so without a code change.
GATE_ENV = "AUDIO_BITRATE_GATE"

#: How much of a source is read for a header probe.
#:
#: Every common container header sits well inside this, and an MP3's bitrate is
#: read from its very first frame - while the media this stands in for is
#: routinely two orders of magnitude larger. The whole point of the probe is to
#: be a rounding error next to the download it replaces.
HEAD_PROBE_BYTES = int(os.getenv("BITRATE_PROBE_BYTES", str(262144)))

#: A header probe is a nicety, never a blocker: one attempt is abandoned after
#: this and the caller converts exactly as it always did.
PROBE_TIMEOUT_SECONDS = float(os.getenv("BITRATE_PROBE_TIMEOUT_SECONDS", "60"))

#: The budget for the **whole** check, which is the number that actually matters.
#:
#: Every caller is interactive: the user has just pressed a bitrate button, or
#: answered the prompt it opened. The check reads the media's header over the
#: network to save a fetch that is minutes long, so it is worth a few seconds and
#: not one more - past the budget the tier stops and the request takes the path it
#: always took, which costs the fetch it was meant to avoid but never leaves
#: someone watching a button that appears to have done nothing. Each tier gets
#: what is left of it, not a fresh timeout of its own, or four tiers would add up
#: to four budgets.
PROBE_BUDGET_SECONDS = float(os.getenv("BITRATE_PROBE_BUDGET_SECONDS", "15"))

#: How close two bitrates have to be to count as the same one.
#:
#: Encoders and containers round what they report (a 64k CBR MP3 legitimately
#: probes as 63999), while a genuinely different target is never this close: the
#: next step up from 64k is 50% away. The floor keeps the window sane at the low
#: end of the range, where 2% of 32k is only 640 bps.
_TOLERANCE_RATIO = 0.02
_TOLERANCE_MIN_BPS = 1000

#: ffprobe reports an MP3 stream as ``mp3`` (``mp3float`` when it decoded the
#: stream through the float path). An encode that produces MP3 can only ever be
#: a no-op against a source that already is one.
_MP3_CODECS = frozenset({"mp3", "mp3float"})


def gate_enabled() -> bool:
    """Whether the check may run at all. Read at call time, not at import."""
    return (os.getenv(GATE_ENV) or "").strip().lower() not in ("0", "false", "no", "off")


def target_bps(value) -> int | None:
    """``"64k"`` / ``64`` / ``"64kbps"`` -> ``64000``, anything else -> ``None``.

    The pickers and the settings store speak in ``"<kbps>k"``, but a stored
    preference may have reached here as a bare number, so both are accepted. A
    value that cannot be read is ``None`` rather than a guess: the caller then
    leaves the request alone.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        kbps = float(value)
    else:
        text = str(value or "").strip().lower()
        for suffix in ("kbps", "k"):
            if text.endswith(suffix):
                text = text[: -len(suffix)].strip()
                break
        try:
            kbps = float(text)
        except (TypeError, ValueError):
            return None
    if kbps <= 0:
        return None
    return int(round(kbps * 1000))


def bitrates_agree(source_bps, wanted_bps) -> bool:
    """Whether a source already carries (within rounding) the wanted bitrate."""
    try:
        source = int(source_bps)
        wanted = int(wanted_bps)
    except (TypeError, ValueError):
        return False
    if source <= 0 or wanted <= 0:
        return False
    tolerance = max(_TOLERANCE_MIN_BPS, int(wanted * _TOLERANCE_RATIO))
    return abs(source - wanted) <= tolerance


def codec_is_target(source_codec, current_file: dict | None, target_codec: str = "mp3") -> bool:
    """Whether the source's audio codec already is the codec the encode produces.

    A verdict that carries no codec falls back to the media's own extension, and
    that fallback is only ever reached when the probe said nothing at all - an
    unknown codec paired with a matching extension is the one case where the two
    pieces of evidence together still answer the question.
    """
    wanted = str(target_codec or "").strip().lower()
    text = str(source_codec or "").strip().lower()
    if text:
        if wanted == "mp3":
            return text in _MP3_CODECS
        return text == wanted
    extension = os.path.splitext(str((current_file or {}).get("name") or ""))[1].lower().lstrip(".")
    return bool(wanted) and extension == wanted


def known_verdict(current_file: dict | None) -> tuple[object, object]:
    """The bitrate and codec an earlier probe already recorded, if any.

    Two shapes are read: the ingest's raw ffprobe verdict (``_source_metadata``,
    the same dict the captions are built from) and the flattened ``source_*``
    fields a job or a bulk entry may carry instead.
    """
    info = current_file or {}
    metadata = info.get("_source_metadata") or info.get("source_metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    bitrate = metadata.get("audio_bitrate")
    codec = metadata.get("audio_codec")
    if bitrate is None:
        bitrate = info.get("source_audio_bitrate")
    if not codec:
        codec = info.get("source_audio_codec")
    return bitrate, codec


def _time_left(deadline: float) -> float:
    """Seconds left of the check's budget, never negative."""
    return max(0.0, deadline - time.monotonic())


async def _cached_source(current_file: dict | None, timeout: float | None = None) -> dict | None:
    """The media-cache descriptor for this media, when one exists.

    The cheapest evidence there is, and the only tier that costs no egress at
    all: the descriptor the ingest wrote when it stored the media carries both
    the ffprobe verdict it captured (``source_meta``) and the key the bytes live
    under (``input_key``). Reading it here is what lets the check answer a media
    this deployment already holds *without* falling through to Telegram - and
    falling through to Telegram is how the check used to fail: a header read the
    userbot account cannot reach answers "nothing", so a verdict that was one
    dictionary lookup away was missed and the request went on to fetch and
    re-encode a file that was already the answer. Answers ``None`` for a lookup
    that misses, times out or fails, which leaves every caller on the path it
    had before this tier existed.
    """
    uid = (current_file or {}).get("file_unique_id")
    if not uid:
        return None
    try:
        from utils import media_cache

        if not media_cache.cache_enabled():
            return None
        entry = await asyncio.wait_for(
            media_cache.lookup(uid, expected_size=(current_file or {}).get("size")),
            timeout=min(timeout or PROBE_TIMEOUT_SECONDS, PROBE_TIMEOUT_SECONDS),
        )
    except TimeoutError:
        logger.debug("bitrate gate: the media-cache lookup for %s timed out", uid)
        return None
    except Exception:
        logger.debug("bitrate gate: the media-cache lookup for %s failed", uid)
        return None
    return entry if isinstance(entry, dict) and entry else None


def _source_view(current_file: dict | None, entry: dict | None) -> dict:
    """The media as the cache descriptor describes it, when there is one.

    A descriptor answers *where* the media already is - the object key it was
    stored under, or the local copy a previous request downloaded. Carrying
    those onto the file is what turns the byte probes below from "nothing to
    read" into a ranged GET of a few hundred kilobytes: a media that has never
    been fetched to this disk is exactly the one with no ``path`` of its own.
    """
    info = dict(current_file or {})
    if entry:
        if not info.get("input_key") and entry.get("input_key"):
            info["input_key"] = entry["input_key"]
        if not info.get("path") and entry.get("path"):
            info["path"] = entry["path"]
    return info


def _storage_key(info: dict | None) -> str | None:
    """The stored object to read this media's header from, or ``None``.

    An explicitly named key wins. Failing that, the media's own identity names
    the object its bytes were stored under: the shared library key is derived
    from ``file_unique_id``, so a media this deployment has ingested is still
    readable here after the Redis descriptor that named it has expired. A key
    nothing was ever stored under simply fails the ranged GET, which is the
    "not already" answer this whole module is careful to give.
    """
    key = (info or {}).get("input_key")
    if isinstance(key, str) and key:
        return key
    try:
        from utils.media_cache import shared_library_key

        return shared_library_key((info or {}).get("file_unique_id"))
    except Exception:
        return None


async def _probe_file(path: str, timeout: float | None = None) -> dict | None:
    """ffprobe a file, answering ``None`` rather than raising."""
    if not path or not os.path.exists(path):
        return None
    try:
        from utils.ffmpeg_runner import probe_media

        probe = probe_media(path)
        if timeout is not None:
            probe = asyncio.wait_for(probe, timeout=timeout)
        return await probe or None
    except TimeoutError:
        logger.debug("bitrate gate: ffprobe of %s ran out of budget", path)
        return None
    except Exception:
        logger.debug("bitrate gate: ffprobe failed for %s", path)
        return None


def _local_candidate(current_file: dict | None) -> str | None:
    """A path on this disk holding the source, whichever field names it.

    ``input_key`` is included because a deployment with no object storage stores
    the file under that name instead of a key - the same convention the worker's
    fetch path already reads.
    """
    info = current_file or {}
    for field in ("path", "_local_input_path", "input_key"):
        value = info.get(field)
        if isinstance(value, str) and value and os.path.exists(value):
            return value
    return None


async def _probe_stored(current_file: dict | None, dest_dir: str, timeout: float | None = None) -> dict | None:
    """Probe the first bytes of the media out of object storage (a Range GET)."""
    key = _storage_key(current_file)
    if not key:
        return None
    dest = os.path.join(dest_dir, f"bitrate_probe_{uuid.uuid4().hex}.bin")
    try:
        from utils.storage import get_storage_backend

        backend = await get_storage_backend()
        if backend is None:
            return None
        ok = await asyncio.wait_for(
            backend.download_range(key, dest, end=max(0, HEAD_PROBE_BYTES - 1)),
            timeout=min(timeout or PROBE_TIMEOUT_SECONDS, PROBE_TIMEOUT_SECONDS),
        )
        if not ok or not os.path.exists(dest) or os.path.getsize(dest) <= 0:
            return None
        return await _probe_file(dest, timeout=timeout)
    except TimeoutError:
        logger.debug("bitrate gate: the storage header read for %s timed out", key)
        return None
    except Exception:
        logger.debug("bitrate gate: the storage header read for %s failed", key)
        return None
    finally:
        with contextlib.suppress(OSError):
            if os.path.exists(dest):
                os.remove(dest)


def _telegram_candidates(current_file: dict | None) -> list[tuple[object, object]]:
    """The ``(chat_id, message_id)`` pairs the userbot may read this media from.

    The origin forward comes first - it is the copy the userbot was most likely
    invited to see - and the chat the bot itself received the message in second.
    Both are tried because which one works depends on the deployment: an account
    that is also the user's own can read the bot's DM, while one that is not
    needs the forward.
    """
    info = current_file or {}
    pairs: list[tuple[object, object]] = []

    def _add(chat, message):
        if chat and message:
            pair = (chat, message)
            if pair not in pairs:
                pairs.append(pair)

    forward = info.get("forward")
    if isinstance(forward, dict):
        _add(forward.get("chat_id"), forward.get("message_id"))
    _add(
        info.get("chat_id") or info.get("forward_chat_id"),
        info.get("msg_id") or info.get("message_id") or info.get("forward_message_id"),
    )
    return pairs


async def _probe_telegram(
    current_file: dict | None, user_id, dest_dir: str, timeout: float | None = None
) -> dict | None:
    """Probe the first bytes of the media straight off Telegram.

    This is what makes the check pay off for a media nothing has fetched yet -
    the case the gate exists for. A partial read of a few hundred kilobytes is
    ordered instead of the whole file, and a deployment with no userbot simply
    answers ``None`` here and keeps its old behaviour.
    """
    candidates = _telegram_candidates(current_file)
    if not candidates:
        return None
    try:
        from utils.userbot_downloader import download_head_via_userbot
    except Exception:
        logger.debug("bitrate gate: the userbot downloader is unavailable")
        return None

    for chat_id, message_id in candidates:
        dest = os.path.join(dest_dir, f"bitrate_head_{uuid.uuid4().hex}.bin")
        try:
            ok = await download_head_via_userbot(
                chat_id,
                message_id,
                dest,
                max_bytes=HEAD_PROBE_BYTES,
                user_id=user_id,
                # One candidate may not be readable at all, so each gets the
                # remaining budget rather than the whole of it.
                timeout=min(timeout or PROBE_TIMEOUT_SECONDS, PROBE_TIMEOUT_SECONDS),
            )
            if ok and os.path.exists(dest) and os.path.getsize(dest) > 0:
                return await _probe_file(dest, timeout=timeout)
        except Exception:
            logger.debug("bitrate gate: the Telegram header read failed for %s/%s", chat_id, message_id)
        finally:
            with contextlib.suppress(OSError):
                if os.path.exists(dest):
                    os.remove(dest)
    return None


async def source_verdict(
    current_file: dict | None,
    *,
    user_id=None,
    budget_seconds: float | None = None,
) -> tuple[object, object]:
    """``(audio_bitrate_bps, audio_codec)`` for the media, or ``(None, None)``.

    Cheapest evidence first: a verdict an earlier probe already recorded, then
    the media cache's own descriptor (which may answer outright, and which also
    says which object to read), then the local copy, then the stored object, and
    only then Telegram. Each step
    answers "nothing" instead of raising, so the worst case is the behaviour the
    callers had before this module - and each gets only what is left of
    :data:`PROBE_BUDGET_SECONDS`, so the whole check is bounded rather than every
    attempt in it.
    """
    bitrate, codec = known_verdict(current_file)
    if bitrate:
        return bitrate, codec

    budget = PROBE_BUDGET_SECONDS if budget_seconds is None else float(budget_seconds)
    deadline = time.monotonic() + max(0.0, budget)

    def left() -> float:
        """What is left of the budget, so no single tier can outlast the check."""
        return _time_left(deadline)

    dest_dir = tempfile.gettempdir()

    # Where the media already is, and what it was probed as when it got there:
    # a lookup rather than a read, so it comes before everything that costs
    # bytes - including the Telegram header read that an unreachable media
    # answers "nothing" to.
    entry = None
    if left() > 0:
        entry = await _cached_source(current_file, timeout=left())
        if entry:
            cached_bitrate, cached_codec = known_verdict({"source_metadata": entry.get("source_meta")})
            if cached_bitrate:
                return cached_bitrate, cached_codec
    source = _source_view(current_file, entry)

    local = _local_candidate(source)
    if local and left() > 0:
        meta = await _probe_file(local, timeout=left())
        if meta and meta.get("audio_bitrate"):
            return meta.get("audio_bitrate"), meta.get("audio_codec")

    if left() > 0:
        meta = await _probe_stored(source, dest_dir, timeout=left())
        if meta and meta.get("audio_bitrate"):
            return meta.get("audio_bitrate"), meta.get("audio_codec")

    if left() > 0:
        meta = await _probe_telegram(source, user_id, dest_dir, timeout=left())
        if meta and meta.get("audio_bitrate"):
            return meta.get("audio_bitrate"), meta.get("audio_codec")

    return None, None


async def already_at_bitrate(
    current_file: dict | None,
    target,
    *,
    user_id=None,
    target_codec: str = "mp3",
) -> int | None:
    """The source's own bitrate when the request asks for what it already has.

    Returns ``None`` for every other outcome - unknown, unreadable, a different
    bitrate, or a different codec - which is what keeps this a shortcut and never
    a refusal.
    """
    if not gate_enabled():
        return None
    wanted = target_bps(target)
    if not wanted:
        return None
    try:
        source, source_codec = await source_verdict(current_file, user_id=user_id)
    except Exception:
        logger.debug("bitrate gate: the source verdict could not be read")
        return None
    if not bitrates_agree(source, wanted):
        return None
    if not codec_is_target(source_codec, current_file, target_codec):
        return None
    logger.info(
        "bitrate gate: %s already carries %s bps (%s) - the %s request needs no re-encode",
        (current_file or {}).get("name") or (current_file or {}).get("id") or "media",
        source,
        source_codec or "codec unknown",
        target,
    )
    return int(source)
