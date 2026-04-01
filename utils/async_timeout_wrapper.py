# utils/async_timeout_wrapper.py
"""
Async timeout wrapper for FFmpeg and other long-running operations.
Ensures no operation hangs indefinitely.
"""

import asyncio
import logging
from functools import wraps
from typing import Any, Callable, Coroutine, Optional, Tuple

logger = logging.getLogger(__name__)

# Default timeout for FFmpeg operations (5 hours)
DEFAULT_FFMPEG_TIMEOUT = 18000

# Default timeout for other operations (30 minutes)
DEFAULT_OPERATION_TIMEOUT = 1800


class TimeoutError(Exception):
    """Raised when an operation exceeds its timeout."""

    pass


async def run_with_timeout(
    coro: Coroutine, timeout_seconds: int = DEFAULT_FFMPEG_TIMEOUT, operation_name: str = "Operation"
) -> Any:
    """
    Run a coroutine with timeout protection.

    Args:
        coro: The coroutine to run
        timeout_seconds: Timeout in seconds
        operation_name: Name of operation for logging

    Returns:
        Result of the coroutine

    Raises:
        TimeoutError: If operation exceeds timeout
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout_seconds)
    except asyncio.TimeoutError as e:
        logger.error(
            f"{operation_name} timeout after {timeout_seconds}s - " f"this operation took too long to complete"
        )
        raise TimeoutError(f"{operation_name} exceeded {timeout_seconds}s timeout limit") from e


async def run_subprocess_with_timeout(
    cmd: list, timeout_seconds: int = DEFAULT_FFMPEG_TIMEOUT, operation_name: str = "Subprocess"
) -> Tuple[bytes, bytes, int]:
    """
    Run a subprocess with timeout protection and process cleanup.

    Args:
        cmd: Command as list (for create_subprocess_exec)
        timeout_seconds: Timeout in seconds
        operation_name: Name of operation for logging

    Returns:
        Tuple of (stdout, stderr, returncode)

    Raises:
        TimeoutError: If process exceeds timeout
    """
    process = None
    try:
        # Basic sanity check: ensure command arguments are valid (no None values)
        for a in cmd:
            if a is None:
                raise ValueError(f"Invalid subprocess command contains None: {cmd}")
        # Start the subprocess
        try:
            from utils.process_utils import create_checked_subprocess_exec
        except Exception:
            create_checked_subprocess_exec = None

        if create_checked_subprocess_exec is not None:
            process = await create_checked_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                preexec_fn=None,  # Set process group for better cleanup if on Unix
            )
        else:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                preexec_fn=None,  # Set process group for better cleanup if on Unix
            )

        logger.debug(f"{operation_name} started (PID: {process.pid})")

        # Run with timeout
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
            return stdout, stderr, process.returncode

        except asyncio.TimeoutError:
            logger.warning(f"{operation_name} timeout after {timeout_seconds}s - killing process (PID: {process.pid})")

            # Attempt graceful termination
            if process and not process.returncode:
                try:
                    process.terminate()
                    # Give it 5 seconds to terminate gracefully
                    await asyncio.wait_for(process.wait(), timeout=5)
                    logger.info(f"{operation_name} process terminated gracefully")
                except asyncio.TimeoutError:
                    # Force kill if graceful termination failed
                    logger.warning(f"{operation_name} grace period expired - force killing")
                    if process:
                        process.kill()
                    try:
                        await asyncio.wait_for(process.wait(), timeout=5)
                    except asyncio.TimeoutError:
                        logger.error(f"Failed to kill {operation_name} process")
                except Exception as e:
                    logger.error(f"Error during process termination: {e}")

            raise TimeoutError(f"{operation_name} exceeded {timeout_seconds}s timeout limit")

    except asyncio.CancelledError:
        logger.warning(f"{operation_name} was cancelled")
        if process and not process.returncode:
            try:
                process.kill()
                await asyncio.wait_for(process.wait(), timeout=5)
            except Exception:
                pass
        raise

    except Exception as e:
        logger.error(f"Error during {operation_name}: {e}")
        if process and not process.returncode:
            try:
                process.kill()
                await asyncio.wait_for(process.wait(), timeout=5)
            except Exception:
                pass
        raise


def timeout_decorator(timeout_seconds: int = DEFAULT_FFMPEG_TIMEOUT, operation_name: Optional[str] = None):
    """
    Decorator to add timeout protection to async functions.

    Args:
        timeout_seconds: Timeout in seconds
        operation_name: Name for logging (defaults to function name)

    Usage:
        @timeout_decorator(timeout_seconds=3600, operation_name="Video Conversion")
        async def convert_video(...):
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            op_name = operation_name or func.__name__
            try:
                return await run_with_timeout(
                    func(*args, **kwargs), timeout_seconds=timeout_seconds, operation_name=op_name
                )
            except TimeoutError as e:
                logger.error(f"{op_name} failed: {e}")
                raise

        return wrapper

    return decorator
