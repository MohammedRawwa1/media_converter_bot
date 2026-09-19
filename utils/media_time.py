"""One parser for the times a user types into the bot.

Three buttons ask for a time - the trimmer (a start and an end), the splitter (the
length of each part) - and each used to read it its own way, so ``1h30m`` worked in
one place and raised in another while ``10:00`` meant ten minutes here and ten
seconds there. Everything reads this instead.

The forms accepted, in the order they are tried:

  ``01:00:00`` / ``10:00`` / ``0:30``  - colon form, most significant field first
  ``2h`` / ``30m`` / ``1h30m`` / ``45s`` - unit form, any of ``h``/``m``/``s``
  ``90``                                - plain seconds

Anything else raises :class:`ValueError`, so a caller can answer the user with the
form it expected rather than passing garbage to ffmpeg.
"""

from __future__ import annotations

import re

_UNIT_SECONDS = {
    "h": 3600.0,
    "hr": 3600.0,
    "hrs": 3600.0,
    "m": 60.0,
    "min": 60.0,
    "mins": 60.0,
    "s": 1.0,
    "sec": 1.0,
    "secs": 1.0,
}

#: One ``<number><unit>`` group, e.g. the ``1h`` in ``1h30m``.
_UNIT_PART = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>h|hr|hrs|m|min|mins|s|sec|secs)", re.IGNORECASE)


def parse_time_to_seconds(text: object) -> float:
    """Parse a user-typed time into seconds.

    Raises ``ValueError`` when the text is not a time at all, which is the answer
    a caller needs to ask again rather than hand ffmpeg a nonsense argument.
    """
    raw = str(text or "").strip()
    if not raw:
        raise ValueError("Invalid time format: empty")

    if ":" in raw:
        fields = raw.split(":")
        if len(fields) > 3:
            raise ValueError(f"Invalid time format: {raw}")
        try:
            seconds = 0.0
            for field in fields:
                # Colon form is positional: each field is 60 times the one after
                # it, so 1:02:03 is 3723 seconds however many fields were typed.
                seconds = seconds * 60.0 + float(field)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid time format: {raw}") from exc
        return seconds

    if _UNIT_PART.search(raw):
        # Everything must be a unit group: "1h30m" yes, "1h30m nope" no.
        leftover = _UNIT_PART.sub("", raw).strip()
        if leftover:
            raise ValueError(f"Invalid time format: {raw}")
        return sum(
            float(match.group("value")) * _UNIT_SECONDS[match.group("unit").lower()]
            for match in _UNIT_PART.finditer(raw)
        )

    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid time format: {raw}") from exc


def segment_seconds_for_parts(duration_seconds: float, parts: int) -> float:
    """The part length that turns *duration_seconds* into exactly *parts* pieces.

    Never zero: ffmpeg treats ``-segment_time 0`` as "as fast as the muxer likes",
    which would cut a media into pieces of arbitrary length.
    """
    count = max(1, int(parts))
    try:
        duration = float(duration_seconds or 0.0)
    except (TypeError, ValueError):
        duration = 0.0
    if duration <= 0:
        raise ValueError("this media has no known duration")
    return max(1.0, duration / count)


def format_clock(seconds: float) -> str:
    """``3723`` -> ``01:02:03``, for the messages the bot sends back."""
    try:
        total = max(0, int(round(float(seconds))))
    except (TypeError, ValueError):
        total = 0
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"
