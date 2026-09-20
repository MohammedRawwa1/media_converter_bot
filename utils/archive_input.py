"""Archive inputs: expand an uploaded archive into the media files it holds.

Telegram Desktop zips a dragged folder client-side and sends it "as a file", so
the bot receives a single ``.zip`` document - there is no folder object in the Bot
API, and no server-side folder-to-zip. The only way to use such an upload is to
unpack it.

Unpacking is also the one upload path where a single file can turn into many, and
where the *contents* are chosen by whoever built the archive rather than by the
sender of one file. ``zipfile.extract`` defends against none of it: a name that
escapes the destination (a "zip slip", ``../../etc/x``), a symlink entry that
points somewhere else, or a small archive that expands to fill the disk (a "zip
bomb"). So this module never calls ``extract`` - every entry is named, checked and
written by hand, under a cap on the number of entries, on the bytes each entry
claims, on the total bytes, and on how far one entry may expand relative to its
compressed size. The byte caps are enforced again against the bytes actually
read, so a lying header cannot slip past them.

Only media the pipeline can actually convert is written out. An archive that
delivers a payload under a media name - an ``.html``, a script, a nested ``.zip``
- has that entry left behind instead of handing it to a later step.

Usage::

    from utils.archive_input import expand_archive, is_archive

    if is_archive(filename):
        result = expand_archive(path, dest_dir)
        if not result.members:
            return refuse(result.errors)
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
import zipfile
from dataclasses import dataclass, field

from utils import file_utils

__all__ = [
    "ARCHIVE_EXTENSIONS",
    "MEDIA_MEMBER_EXTENSIONS",
    "ArchiveExpansion",
    "ExtractedMember",
    "expand_archive",
    "is_archive",
]

logger = logging.getLogger(__name__)

# Extensions treated as "an archive to unpack". Deliberately narrow: a tar or a
# 7z would each need their own safe reader, and guessing wrong here is how an
# unchecked extractor ends up in the codebase.
ARCHIVE_EXTENSIONS = frozenset({".zip"})

# What an archive member is allowed to be for the pipeline to take it. The shared
# allowlist minus the containers that cannot be a conversion *input*: another
# archive (no recursion), and ``.bin`` (no format of its own to convert).
MEDIA_MEMBER_EXTENSIONS = frozenset(file_utils.ALLOWED_EXTENSIONS - ARCHIVE_EXTENSIONS - {".bin"})

# A name is reduced to characters that are safe on disk, the same shape
# ``file_utils.sanitize_filename`` produces. Separators never survive this, so a
# member name cannot carry a path.
_UNSAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._\-()\[\] ]+")

# A zip entry stores symlinks/hardlinks as a Unix mode in the high half of
# ``external_attr``. Such an entry is a pointer, not content, so it is skipped.
_SYMLINK_MODE = 0o120000
_FILE_TYPE_MASK = 0o170000

# Only enforce the compression-ratio guard past this uncompressed size: tiny
# files compress to enormous ratios legitimately (a few bytes of zeros), so the
# ratio is meaningless below the floor and would refuse honest archives.
_RATIO_FLOOR_BYTES = 8 * 1024 * 1024

# Read size for the hand-rolled copy loop. Large enough to be fast, small enough
# that the byte caps are checked often.
_CHUNK_BYTES = 1 << 20


@dataclass(frozen=True)
class ExtractedMember:
    """One media file written out of an archive.

    ``name`` is the name it was written under, which is unique inside the
    destination: two entries called ``clip.mp4`` become ``clip.mp4`` and
    ``clip_1.mp4``, so two jobs built from one archive cannot collide on the same
    output name.
    """

    path: str
    name: str
    ext: str
    size: int


@dataclass
class ArchiveExpansion:
    """What an archive turned out to hold.

    ``members`` are the files written to disk; ``skipped`` counts entries left
    behind (a directory, a non-media payload, an unsafe path); ``errors`` names
    the entries that were skipped for a reason the caller may want to surface.
    """

    members: list[ExtractedMember] = field(default_factory=list)
    skipped: int = 0
    errors: list[str] = field(default_factory=list)
    total_bytes: int = 0

    @property
    def ok(self) -> bool:
        return bool(self.members)


class _LimitExceededError(Exception):
    """Raised internally when an entry outgrows the bytes it is allowed."""


def is_archive(name: object) -> bool:
    """True when ``name`` (a filename or path) names an archive we can unpack."""
    if not name:
        return False
    try:
        return os.path.splitext(str(name))[1].lower() in ARCHIVE_EXTENSIONS
    except (TypeError, ValueError):
        return False


def _env_int(name: str, default: int) -> int:
    """A positive integer cap from the environment, or ``default``."""
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        logger.warning("archive_input: %s=%r is not an integer; using %d", name, raw, default)
        return default
    return value if value > 0 else default


def _is_symlink(info: zipfile.ZipInfo) -> bool:
    return (info.external_attr >> 16) & _FILE_TYPE_MASK == _SYMLINK_MODE


def _safe_member_name(raw_name: object) -> str | None:
    """A basename-only, disk-safe name for a member, or ``None`` when unsafe.

    Backslashes are folded to ``/`` first because a Windows-built archive labels
    its paths that way, and a bare ``os.path.basename`` on a POSIX host would
    treat ``..\\..\\evil.mp4`` as one harmless-looking segment. Absolute paths,
    any ``..`` segment, drive letters and NUL are all refused outright rather
    than repaired, so nothing about an escape attempt reaches the filesystem.
    """
    if not raw_name:
        return None
    raw = str(raw_name).replace("\\", "/").strip()
    if not raw or raw.startswith("/") or raw.startswith("~"):
        return None
    if "\x00" in raw or ":" in raw:
        return None
    parts = [part for part in raw.split("/") if part not in ("", ".")]
    if not parts or any(part == ".." for part in parts):
        return None
    base = parts[-1]
    cleaned = _UNSAFE_NAME_RE.sub("_", base)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_. ")
    return cleaned or None


def _unique_path(dest_dir: str, name: str) -> str:
    """A path inside ``dest_dir`` that does not collide with one already there."""
    base, ext = os.path.splitext(name)
    candidate = os.path.join(dest_dir, name)
    counter = 1
    while os.path.exists(candidate):
        candidate = os.path.join(dest_dir, f"{base}_{counter}{ext}")
        counter += 1
    return candidate


def _write_member(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    target: str,
    *,
    member_cap: int,
    remaining_cap: int,
) -> int | None:
    """Copy one member to ``target``, bounded by the bytes actually written.

    Returns the number of bytes written, or ``None`` when the member exceeded a
    cap or could not be read - in which case the partial file is removed rather
    than left looking like a complete one.
    """
    limit = min(member_cap, remaining_cap)
    written = 0
    try:
        with archive.open(info) as src, open(target, "wb") as dst:
            while True:
                chunk = src.read(_CHUNK_BYTES)
                if not chunk:
                    break
                written += len(chunk)
                if written > limit:
                    raise _LimitExceededError
                dst.write(chunk)
    except _LimitExceededError:
        _discard(target)
        return None
    except Exception:
        logger.debug("archive_input: could not read member %s", info.filename)
        _discard(target)
        return None
    return written


def _discard(path: str) -> None:
    with contextlib.suppress(OSError):
        os.remove(path)


def expand_archive(
    archive_path: str,
    dest_dir: str,
    *,
    max_entries: int | None = None,
    max_member_bytes: int | None = None,
    max_total_bytes: int | None = None,
    max_ratio: int | None = None,
    allowed_exts: frozenset[str] | None = None,
) -> ArchiveExpansion:
    """Unpack the media in ``archive_path`` into ``dest_dir``, safely.

    Caps default to ``ARCHIVE_MAX_ENTRIES`` (100), ``ARCHIVE_MAX_MEMBER_BYTES``
    (1 GiB), ``ARCHIVE_MAX_TOTAL_BYTES`` (2 GiB) and ``ARCHIVE_MAX_RATIO`` (200);
    ``allowed_exts`` defaults to :data:`MEDIA_MEMBER_EXTENSIONS`. Every cap can be
    overridden here, which is what tests do. A failure to read the archive, or an
    archive over the entry cap, yields an :class:`ArchiveExpansion` with no
    members and an ``errors`` entry - never an exception - so a caller can answer
    the request without a second code path.
    """
    max_entries = _env_int("ARCHIVE_MAX_ENTRIES", 100) if max_entries is None else max_entries
    max_member_bytes = _env_int("ARCHIVE_MAX_MEMBER_BYTES", 1 << 30) if max_member_bytes is None else max_member_bytes
    max_total_bytes = _env_int("ARCHIVE_MAX_TOTAL_BYTES", 2 << 30) if max_total_bytes is None else max_total_bytes
    max_ratio = _env_int("ARCHIVE_MAX_RATIO", 200) if max_ratio is None else max_ratio
    allowed = MEDIA_MEMBER_EXTENSIONS if allowed_exts is None else allowed_exts

    try:
        os.makedirs(dest_dir, exist_ok=True)
    except OSError as exc:
        return ArchiveExpansion(errors=[f"cannot create the extraction directory: {exc}"])

    try:
        archive = zipfile.ZipFile(archive_path)
    except (zipfile.BadZipFile, OSError) as exc:
        logger.warning("archive_input: %s is not a readable zip: %s", archive_path, exc)
        return ArchiveExpansion(errors=["not a readable zip archive"])

    members: list[ExtractedMember] = []
    errors: list[str] = []
    skipped = 0
    total = 0

    with archive:
        entries = archive.infolist()
        if len(entries) > max_entries:
            return ArchiveExpansion(
                skipped=len(entries),
                errors=[f"archive holds more than the {max_entries} entry cap"],
            )

        for index, info in enumerate(entries):
            if info.is_dir() or _is_symlink(info):
                skipped += 1
                continue
            if info.flag_bits & 0x1:
                skipped += 1
                errors.append(f"{info.filename!r} is password-protected and was skipped")
                continue

            name = _safe_member_name(info.filename)
            if not name:
                skipped += 1
                errors.append(f"{info.filename!r} has an unsafe path and was skipped")
                continue

            ext = os.path.splitext(name)[1].lower()
            if ext not in allowed:
                skipped += 1
                continue

            declared = int(info.file_size or 0)
            if declared > max_member_bytes:
                skipped += 1
                errors.append(f"{name} is larger than the {max_member_bytes} byte per-file cap")
                continue
            if total + declared > max_total_bytes:
                # Nothing further can fit, so stop and account for the rest.
                skipped += len(entries) - index
                errors.append("archive expands beyond the total size cap; later entries were skipped")
                break
            compressed = int(info.compress_size or 0)
            if declared > _RATIO_FLOOR_BYTES and (compressed <= 0 or declared / compressed > max_ratio):
                skipped += 1
                errors.append(f"{name} expands suspiciously far from its compressed size and was skipped")
                continue

            target = _unique_path(dest_dir, name)
            written = _write_member(
                archive,
                info,
                target,
                member_cap=max_member_bytes,
                remaining_cap=max_total_bytes - total,
            )
            if written is None:
                skipped += 1
                errors.append(f"{name} could not be extracted within the size caps")
                continue

            total += written
            members.append(ExtractedMember(path=target, name=os.path.basename(target), ext=ext, size=written))

    return ArchiveExpansion(members=members, skipped=skipped, errors=errors, total_bytes=total)
