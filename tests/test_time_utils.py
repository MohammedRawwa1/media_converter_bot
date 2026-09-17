"""Timestamps have to survive the trip out of the process.

``utc_iso`` exists because ``datetime.utcnow().isoformat() + "Z"`` looks
plausible and is wrong twice over: the value is naive, and once the source of
it became timezone-aware the same expression would emit ``+00:00Z``.
"""

import datetime
import time

from utils.time_utils import utc_iso


def test_utc_iso_ends_in_z_and_carries_no_offset():
    stamp = utc_iso()
    assert stamp.endswith("Z")
    assert "+00:00" not in stamp


def test_utc_iso_parses_back_as_an_aware_utc_datetime():
    parsed = datetime.datetime.fromisoformat(utc_iso())
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == datetime.timedelta(0)


def test_utc_iso_reports_utc_rather_than_the_hosts_local_time():
    """The whole point: the value is the UTC instant, not a naive local one."""
    parsed = datetime.datetime.fromisoformat(utc_iso())
    expected = datetime.datetime.fromtimestamp(time.time(), tz=datetime.UTC)
    assert abs((parsed - expected).total_seconds()) < 5


def test_utc_iso_renders_a_supplied_moment():
    moment = datetime.datetime(2026, 9, 16, 23, 42, 8, 267948, tzinfo=datetime.UTC)
    assert utc_iso(moment) == "2026-09-16T23:42:08.267948Z"


def test_utc_iso_reads_a_naive_moment_as_utc():
    """A naive value is what BSON hands back, and it means UTC."""
    naive = datetime.datetime(2026, 9, 16, 23, 42, 8, 267948)
    assert utc_iso(naive) == "2026-09-16T23:42:08.267948Z"


def test_utc_iso_converts_a_shifted_zone_rather_than_relabelling_it():
    moment = datetime.datetime(
        2026, 9, 17, 5, 12, 8, 267948, tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    )
    assert utc_iso(moment) == "2026-09-16T23:42:08.267948Z"
