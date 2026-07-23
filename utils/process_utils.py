"""Process helper utilities.

Provide a safe wrapper around `asyncio.create_subprocess_exec` that validates
command arguments and converts PathLike objects to strings to avoid
`TypeError: expected str, bytes or os.PathLike object, not NoneType` when any
argument is None.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


async def create_checked_subprocess_exec(*cmd: Any, **kwargs):
    """Wrapper for asyncio.create_subprocess_exec that validates and normalizes
    command arguments.

    Raises ValueError when any command argument is None.
    """
    if not cmd:
        raise ValueError("No command provided to create_subprocess_exec")

    for a in cmd:
        if a is None:
            raise ValueError(f"Invalid subprocess command contains None: {cmd}")

    # Normalize PathLike objects
    normalized = []
    for a in cmd:
        if isinstance(a, (str, bytes)):
            normalized.append(a)
        elif isinstance(a, os.PathLike):
            normalized.append(str(a))
        else:
            # best-effort conversion
            normalized.append(str(a))

    return await asyncio.create_subprocess_exec(*normalized, **kwargs)
