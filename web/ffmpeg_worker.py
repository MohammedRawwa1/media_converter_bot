"""Synchronous ffmpeg helpers for the Flask web component.

Every one of these delegates to the canonical implementation in
``utils.ffmpeg_runner`` - the same runner the ffmpeg worker drives - so there is
exactly one progress parser, one duration probe and one ffmpeg invocation across
the project. This module used to carry its own copies of all three, which meant a
bug fixed in the worker's runner stayed broken here.

The functions stay synchronous and callback-shaped because Flask serves the
upload endpoints from plain worker threads, which own no event loop.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable

# ``_parse_out_time`` is re-exported rather than reimplemented: it is the same
# parser ``run_ffmpeg`` uses, and callers of this module have always been able to
# reach it here.
from utils.ffmpeg_runner import _parse_out_time, probe_duration, run_ffmpeg

logger = logging.getLogger(__name__)

__all__ = ["_parse_out_time", "convert_video", "get_duration"]


def _run_blocking(coro):
    """Run a runner coroutine from a thread that owns no event loop.

    Flask's request and background threads have no loop, so ``asyncio.run`` is the
    whole trick. Called from inside a *running* loop there is nothing safe to do -
    the nested loop cannot start and the caller would hang - so say so instead.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    # Nothing here can await it, and leaving it open warns on collection.
    with contextlib.suppress(Exception):
        coro.close()
    raise RuntimeError(
        "convert_video/get_duration are for plain threads; inside a running loop, "
        "await utils.ffmpeg_runner.run_ffmpeg / probe_duration directly"
    )


def get_duration(path: str) -> float | None:
    """Duration of ``path`` in seconds, or None if it cannot be probed."""
    return _run_blocking(probe_duration(path))


def convert_video(
    input_path: str,
    output_path: str,
    job_id: str,
    duration: float = 0.0,
    progress_cb: Callable[[float, str], None] | None = None,
    finished_cb: Callable[[str], None] | None = None,
) -> None:
    """Convert one file, reporting progress to the callbacks.

    ``duration`` is accepted for compatibility but unused: the runner probes the
    source itself, so a caller-supplied figure could only disagree with it. The
    progress percentages always describe the encode that actually ran.

    Raises ``RuntimeError`` when ffmpeg fails, as this helper always has.
    """
    ok, reason = _run_blocking(
        run_ffmpeg(
            input_path,
            output_path,
            job_id,
            progress_channel=None,
            on_progress=progress_cb,
        )
    )
    if not ok:
        raise RuntimeError(f"ffmpeg failed: {reason}")
    if finished_cb is not None:
        finished_cb(output_path)
