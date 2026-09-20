"""The archive part-size preference: how a packed ZIP is split before delivery.

Create Archive packs a batch into one ZIP. Telegram will not accept a file past
its per-send ceiling, so an archive larger than that is cut into numbered volumes
(``name.zip.001``, ``.002``, ...) that a recipient joins by opening the first.

Whether to split, and at what size, is a *preference*, not a property of any one
batch, so it lives in ``user_settings`` under ``archive_part`` - and this module
is the single place that reads, validates and explains it. Every consumer agrees:
the /usersettings picker stores what this parses, the Create Archive summary
labels what this formats, and the job the handler queues carries what this
resolves, so a value set in one screen cannot mean something else in another.

The stored value is a compact string, so a hand-edited settings file is read the
same way a picker's is:

  ``default``    - split only when the archive outgrows the configured cap
                   (``ARCHIVE_SPLIT_MAX_MB``).
  ``off``        - never split: one ``.zip`` however large it is.
  ``size:<n>``   - split into volumes of at most *n* bytes each.
  ``parts:<n>``  - split into *n* approximately equal volumes.
"""

from __future__ import annotations

import re

__all__ = [
    "DEFAULT_VALUE",
    "MIN_PART_BYTES",
    "OFF_VALUE",
    "PRESET_SIZES",
    "estimate_parts",
    "format_size",
    "label",
    "normalize",
    "parse",
    "plan",
    "preset_value",
]

DEFAULT_VALUE = "default"
OFF_VALUE = "off"

# Smallest part a user may ask for. Below a megabyte a "part" is all overhead,
# and a typo like "5" meant as a part count would otherwise become 5 bytes.
MIN_PART_BYTES = 1 * 1024 * 1024

# The sizes the /usersettings picker offers as one-tap choices.
PRESET_SIZES = (500 * 1024**2, 1024**3, 2 * 1024**3)

_UNIT_FACTORS = {
    "b": 1,
    "k": 1024,
    "kb": 1024,
    "ki": 1024,
    "kib": 1024,
    "m": 1024**2,
    "mb": 1024**2,
    "mi": 1024**2,
    "mib": 1024**2,
    "g": 1024**3,
    "gb": 1024**3,
    "gi": 1024**3,
    "gib": 1024**3,
    "t": 1024**4,
    "tb": 1024**4,
    "ti": 1024**4,
    "tib": 1024**4,
}

_SIZE_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*([kmgt]i?b?)")

# Text that means "leave it to the configured cap" or "never split", spelled the
# way people actually type them.
_DEFAULT_WORDS = frozenset({"", "default", "auto", "cap", "skip"})
# ("0" is deliberately not here: a bare number is read as a part count, and a
# count of zero is a mistake to report, not a way to switch splitting off.)
_OFF_WORDS = frozenset({"off", "none", "no", "no split", "nosplit", "disable", "disabled"})


def size_value(n) -> str:
    """The stored value for one explicit part size, in bytes."""
    return f"size:{int(n)}"


def parts_value(n) -> str:
    """The stored value for an explicit part count."""
    return f"parts:{int(n)}"


def preset_value(n) -> str:
    """The stored value for one of :data:`PRESET_SIZES`."""
    return size_value(n)


def format_size(total_bytes) -> str:
    """``1536`` -> ``"1.5 KB"``; a zero/unknown value reads ``"unknown"``."""
    try:
        value = float(total_bytes or 0)
    except (TypeError, ValueError):
        return "unknown"
    if value <= 0:
        return "unknown"
    for unit, factor in (("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)):
        if value >= factor:
            return f"{value / factor:.1f} {unit}"
    return f"{int(value)} B"


def parse(text) -> str:
    """Read the typed answer to the part-size prompt into a stored value.

    Accepts a size with a unit (``500MB``, ``1.5GB``, ``2g``) or a bare number of
    equal parts (``3``). Raises ``ValueError`` for anything else, so the caller
    can ask again rather than store a value it invented.
    """
    raw = str(text or "").strip().lower()
    if raw in _DEFAULT_WORDS:
        return DEFAULT_VALUE
    if raw in _OFF_WORDS:
        return OFF_VALUE

    # A bare integer is a *count* - the same reading the media Splitter gives it.
    # A size has to name its unit, or "500" would silently mean 500 bytes.
    if raw.isdigit():
        count = int(raw)
        if count < 2:
            raise ValueError("A split needs at least 2 parts")
        return parts_value(count)

    match = _SIZE_RE.fullmatch(raw)
    if not match:
        raise ValueError("Send a part size like `500MB` or `1.5GB`, or a number of parts like `3`")
    size = int(float(match.group(1)) * _UNIT_FACTORS[match.group(2)])
    if size < MIN_PART_BYTES:
        raise ValueError(f"A part has to be at least {format_size(MIN_PART_BYTES)}")
    return size_value(size)


def normalize(value) -> str:
    """Coerce a stored value to a canonical one; anything unusable is ``default``.

    Read at every use so a value written by an older build, or edited by hand,
    can never reach the splitter as something it does not understand.
    """
    raw = str(value or "").strip().lower()
    if raw == OFF_VALUE:
        return OFF_VALUE
    for prefix, floor, builder in (("size:", MIN_PART_BYTES, size_value), ("parts:", 2, parts_value)):
        if raw.startswith(prefix):
            try:
                number = int(raw.split(":", 1)[1])
            except (TypeError, ValueError):
                return DEFAULT_VALUE
            return builder(number) if number >= floor else DEFAULT_VALUE
    return DEFAULT_VALUE


def label(value) -> str:
    """One line describing the preference, for a menu button or a summary."""
    canonical = normalize(value)
    if canonical == OFF_VALUE:
        return "No split — one .zip"
    if canonical.startswith("size:"):
        return f"Parts of {format_size(int(canonical.split(':', 1)[1]))}"
    if canonical.startswith("parts:"):
        return f"{int(canonical.split(':', 1)[1])} equal parts"
    return "Auto — split only if over the cap"


def plan(value, *, cap_bytes) -> tuple[int, int]:
    """Resolve the preference to ``(split_max_bytes, split_parts)`` for a job.

    ``cap_bytes`` is the configured ceiling (``ARCHIVE_SPLIT_MAX_BYTES``);
    ``split_parts`` is left for the worker, which knows the packed size and
    converts a count into a per-volume byte size there.
    """
    canonical = normalize(value)
    if canonical == OFF_VALUE:
        return 0, 0
    if canonical.startswith("size:"):
        return int(canonical.split(":", 1)[1]), 0
    if canonical.startswith("parts:"):
        return 0, int(canonical.split(":", 1)[1])
    return max(0, int(cap_bytes or 0)), 0


def estimate_parts(value, total_bytes, cap_bytes) -> int | None:
    """How many volumes the preference would make of *total_bytes*, or ``None``.

    *total_bytes* is the sum of the batch's known member sizes - a floor, not the
    packed size - so the answer is an estimate, and ``None`` means the sizes were
    not known well enough to give one.
    """
    canonical = normalize(value)
    total = int(total_bytes or 0)
    if canonical == OFF_VALUE:
        return 1
    if canonical.startswith("parts:"):
        return int(canonical.split(":", 1)[1])
    if total <= 0:
        return None
    if canonical.startswith("size:"):
        cap = int(canonical.split(":", 1)[1])
    else:
        cap = max(0, int(cap_bytes or 0))
        if cap <= 0 or total <= cap:
            return 1
    if cap <= 0:
        return None
    return max(1, -(-total // cap))
