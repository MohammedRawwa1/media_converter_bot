# utils/time_utils.py
"""UTC timestamp helpers.

``datetime.utcnow()`` is deprecated and hands back a *naive* datetime, which
makes a timestamp ambiguous the moment it leaves the process - a stored
``registered_at`` or a log line has no way to say which zone it meant.

Callers that need a datetime object should use ``datetime.now(UTC)``. Callers
that need a string use :func:`utc_iso`.
"""

from __future__ import annotations

from datetime import UTC, datetime


def utc_iso(moment: datetime | None = None) -> str:
    """An ISO-8601 UTC timestamp ending in ``Z``.

    Defaults to now; pass ``moment`` to render an existing datetime instead. A
    naive ``moment`` is read as UTC - which is what it means when it came out of
    BSON or from a ``utcnow()``-era record - and a shifted zone is converted.

    ``datetime.isoformat()`` spells the UTC offset as ``+00:00``. Every JSON
    envelope, structured log line and stored metadata blob in this project uses
    the ``Z`` form, so the translation lives here rather than being re-derived
    - wrongly, as ``isoformat() + "Z"`` - at each call site.
    """
    stamp = moment or datetime.now(UTC)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=UTC)
    return stamp.astimezone(UTC).isoformat().replace("+00:00", "Z")
