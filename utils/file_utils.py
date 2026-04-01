# utils/file_utils.py
"""
File utilities for media conversion bot.
"""

import asyncio
import logging
import os
from typing import Dict, List, Tuple

# Optional async file operations
try:
    import aiofiles
except ImportError:
    aiofiles = None

logger = logging.getLogger(__name__)


class AsyncFileLock:
    """Async file locking mechanism to prevent concurrent access."""

    _locks: Dict[str, asyncio.Lock] = {}
    _lock_pool_lock = asyncio.Lock()

    @classmethod
    async def acquire(cls, file_path: str) -> asyncio.Lock:
        """Acquire a lock for a file path."""
        # Normalize path to ensure consistency
        normalized_path = os.path.normpath(os.path.abspath(file_path))

        async with cls._lock_pool_lock:
            if normalized_path not in cls._locks:
                cls._locks[normalized_path] = asyncio.Lock()
            lock = cls._locks[normalized_path]

        return lock

    @classmethod
    async def release(cls, file_path: str):
        """Release and cleanup lock if no waiters."""
        normalized_path = os.path.normpath(os.path.abspath(file_path))

        async with cls._lock_pool_lock:
            if normalized_path in cls._locks:
                lock = cls._locks[normalized_path]
                # Remove if no one is waiting
                if not lock._locked:
                    del cls._locks[normalized_path]

    @classmethod
    async def context_manager(cls, file_path: str):
        """Context manager for file locking."""
        lock = await cls.acquire(file_path)
        async with lock:
            yield
        await cls.release(file_path)

    @classmethod
    def get_lock_count(cls) -> int:
        """Get number of active locks."""
        return len(cls._locks)


async def download_file(file_path: str, download_path: str) -> Tuple[bool, str]:
    """Download file asynchronously."""
    try:
        if not os.path.exists(file_path):
            return False, f"File not found: {file_path}"

        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        if aiofiles:
            async with aiofiles.open(file_path, "rb") as f:
                content = await f.read()
            async with aiofiles.open(download_path, "wb") as f:
                await f.write(content)
        else:
            # Fallback to sync operations
            with open(file_path, "rb") as f:
                content = f.read()
            with open(download_path, "wb") as f:
                f.write(content)

        logger.info(f"Successfully downloaded file to {download_path}")
        return True, "Download successful"

    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return False, str(e)


async def save_uploaded_file(file_content: bytes, file_path: str) -> Tuple[bool, str]:
    """Save uploaded file asynchronously."""
    try:
        os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

        if aiofiles:
            async with aiofiles.open(file_path, "wb") as f:
                await f.write(file_content)
        else:
            # Fallback to sync operations
            with open(file_path, "wb") as f:
                f.write(file_content)

        logger.info(f"Successfully saved file to {file_path}")
        return True, "Save successful"

    except Exception as e:
        logger.error(f"Error saving file: {e}")
        return False, str(e)


async def cleanup_file(file_path: str) -> Tuple[bool, str]:
    """Delete a file asynchronously."""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Cleaned up file: {file_path}")
            return True, "File deleted"
        return False, "File not found"

    except Exception as e:
        logger.error(f"Error cleaning up file {file_path}: {e}")
        return False, str(e)


async def cleanup_directory(dir_path: str, recursive: bool = True) -> Tuple[bool, int]:
    """Delete all files in a directory asynchronously."""
    try:
        if not os.path.exists(dir_path):
            return False, 0

        # Run sync I/O in executor to prevent event loop blocking
        def sync_cleanup():
            deleted = 0
            if recursive:
                for root, dirs, files in os.walk(dir_path, topdown=False):
                    for file in files:
                        file_path = os.path.join(root, file)
                        try:
                            os.remove(file_path)
                            deleted += 1
                        except Exception as e:
                            logger.warning(f"Failed to delete {file_path}: {e}")

                    for dir_name in dirs:
                        dir_to_delete = os.path.join(root, dir_name)
                        try:
                            os.rmdir(dir_to_delete)
                        except Exception as e:
                            logger.warning(f"Failed to delete directory {dir_to_delete}: {e}")
            else:
                for file in os.listdir(dir_path):
                    file_path = os.path.join(dir_path, file)
                    if os.path.isfile(file_path):
                        try:
                            os.remove(file_path)
                            deleted += 1
                        except Exception as e:
                            logger.warning(f"Failed to delete {file_path}: {e}")

            return deleted

        loop = asyncio.get_event_loop()
        files_deleted = await loop.run_in_executor(None, sync_cleanup)

        logger.info(f"Cleaned up {files_deleted} files from {dir_path}")
        return True, files_deleted

    except Exception as e:
        logger.error(f"Error cleaning up directory {dir_path}: {e}")
        return False, 0


async def get_file_info(file_path: str) -> Dict[str, any]:
    """Get file information asynchronously."""
    try:
        if not os.path.exists(file_path):
            return {"error": "File not found"}

        file_stat = os.stat(file_path)

        info = {
            "path": file_path,
            "name": os.path.basename(file_path),
            "extension": os.path.splitext(file_path)[1],
            "size": file_stat.st_size,
            "size_mb": round(file_stat.st_size / (1024 * 1024), 2),
            "created": file_stat.st_ctime,
            "modified": file_stat.st_mtime,
            "is_file": os.path.isfile(file_path),
        }

        return info

    except Exception as e:
        logger.error(f"Error getting file info: {e}")
        return {"error": str(e)}


async def ensure_directories(*paths: str) -> Tuple[bool, str]:
    """Ensure directories exist asynchronously."""
    try:
        for path in paths:
            os.makedirs(path, exist_ok=True)

        logger.info(f"Ensured {len(paths)} directories exist")
        return True, "Directories ensured"

    except Exception as e:
        logger.error(f"Error ensuring directories: {e}")
        return False, str(e)


async def get_output_path(input_path: str, operation: str, output_extension: str = None) -> str:
    """Generate output path based on input and operation."""
    try:
        base_dir = os.path.dirname(input_path)
        file_name = os.path.basename(input_path)
        name_without_ext = os.path.splitext(file_name)[0]

        if output_extension is None:
            output_extension = os.path.splitext(file_name)[1]
        else:
            if not output_extension.startswith("."):
                output_extension = "." + output_extension

        output_name = f"{name_without_ext}_{operation}{output_extension}"
        output_path = os.path.join(base_dir, output_name)

        # Handle duplicates
        counter = 1
        while os.path.exists(output_path):
            output_name = f"{name_without_ext}_{operation}_{counter}{output_extension}"
            output_path = os.path.join(base_dir, output_name)
            counter += 1

        return output_path

    except Exception as e:
        logger.error(f"Error generating output path: {e}")
        return None


async def list_files(directory: str, extension: str = None) -> List[str]:
    """List files in directory asynchronously."""
    try:
        if not os.path.exists(directory):
            return []

        files = []
        for file in os.listdir(directory):
            file_path = os.path.join(directory, file)
            if os.path.isfile(file_path):
                if extension is None or file.endswith(extension):
                    files.append(file_path)

        return sorted(files)

    except Exception as e:
        logger.error(f"Error listing files: {e}")
        return []


async def move_file(source: str, destination: str) -> Tuple[bool, str]:
    """Move file asynchronously."""
    try:
        os.makedirs(os.path.dirname(destination), exist_ok=True)

        if aiofiles:
            # Read source
            async with aiofiles.open(source, "rb") as f:
                content = await f.read()

            # Write to destination
            async with aiofiles.open(destination, "wb") as f:
                await f.write(content)
        else:
            # Fallback to sync operations
            with open(source, "rb") as f:
                content = f.read()
            with open(destination, "wb") as f:
                f.write(content)

        # Delete source
        os.remove(source)

        logger.info(f"Moved file from {source} to {destination}")
        return True, "File moved"

    except Exception as e:
        logger.error(f"Error moving file: {e}")
        return False, str(e)


async def copy_file(source: str, destination: str) -> Tuple[bool, str]:
    """Copy file asynchronously."""
    try:
        os.makedirs(os.path.dirname(destination), exist_ok=True)

        if aiofiles:
            async with aiofiles.open(source, "rb") as f:
                content = await f.read()

            async with aiofiles.open(destination, "wb") as f:
                await f.write(content)
        else:
            # Fallback to sync operations
            with open(source, "rb") as f:
                content = f.read()
            with open(destination, "wb") as f:
                f.write(content)

        logger.info(f"Copied file from {source} to {destination}")
        return True, "File copied"

    except Exception as e:
        logger.error(f"Error copying file: {e}")
        return False, str(e)


async def get_directory_size(directory: str) -> int:
    """Get total size of directory in bytes."""
    try:
        total_size = 0

        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                total_size += os.path.getsize(filepath)

        return total_size

    except Exception as e:
        logger.error(f"Error calculating directory size: {e}")
        return 0


async def validate_file_extension(file_path: str, allowed_extensions: List[str]) -> bool:
    """Validate file extension."""
    try:
        _, ext = os.path.splitext(file_path)
        return ext.lower() in [e.lower() for e in allowed_extensions]

    except Exception as e:
        logger.error(f"Error validating extension: {e}")
        return False


async def sanitize_filename(name: str, max_len: int = 180) -> str:
    """Sanitize and normalize a filename string.

    This replaces whitespace and other non-allowed characters with underscores,
    collapses repeated underscores, and trims the length while preserving
    the extension.
    """
    try:
        import re

        # Remove path parts
        base = os.path.basename(name)

        # Replace any character that's not alnum, dot, underscore, hyphen, paren or brackets with underscore
        sanitized = re.sub(r"[^A-Za-z0-9._\-()\[\]]+", "_", base)

        # Collapse multiple underscores
        sanitized = re.sub(r"_+", "_", sanitized)

        # Strip leading/trailing underscores or dots
        sanitized = sanitized.strip("_. ")

        # Trim to max_len preserving extension
        if len(sanitized) > max_len:
            name_part, ext = os.path.splitext(sanitized)
            name_part = name_part[: max_len - len(ext)]
            sanitized = name_part + ext

        # Ensure there's at least some name
        if not sanitized:
            return "file"

        return sanitized
    except Exception as e:
        logger.exception("sanitize_filename failed: %s", e)
        return "file"


async def detect_filename(input_path: str, message=None) -> str:
    """Try to detect a sensible filename for an uploaded file.

    Strategy:
    - If `message.document.file_name` present, use it.
    - If caption contains a filename-like token, use it.
    - Probe file via ffprobe/ffmpeg to get title tag or container format for extension.
    - Fallback to basename of input_path.
    """
    # 1) document filename
    try:
        if message is not None:
            doc = getattr(message, "document", None)
            if doc and getattr(doc, "file_name", None):
                return await sanitize_filename(doc.file_name)

            # Caption heuristic
            caption = getattr(message, "caption", None) or getattr(message, "text", None) or ""
            import re

            m = re.search(
                r"([\w\- .]+\.(mp4|mkv|avi|mov|mp3|wav|aac|flac|ogg|m4a|srt|ass|vtt|zip))",
                caption,
                re.IGNORECASE,
            )
            if m:
                return await sanitize_filename(m.group(1))

        # 2) probe file for tags (offload blocking probe to thread)
        probe = None
        try:
            import ffmpeg

            try:
                probe = await asyncio.to_thread(ffmpeg.probe, input_path)
            except Exception:
                probe = None
        except Exception:
            probe = None

        if probe is None:
            # fallback to ffprobe CLI executed in a thread to avoid blocking
            try:
                import subprocess
                import json

                def _run_probe():
                    return subprocess.run(
                        [
                            "ffprobe",
                            "-v",
                            "quiet",
                            "-print_format",
                            "json",
                            "-show_format",
                            input_path,
                        ],
                        capture_output=True,
                    )

                p = await asyncio.to_thread(_run_probe)
                probe = json.loads(p.stdout.decode() or "{}")
            except Exception:
                probe = {}

        fmt = probe.get("format", {}) if isinstance(probe, dict) else {}
        tags = fmt.get("tags", {}) if isinstance(fmt, dict) else {}
        title = tags.get("title") or tags.get("TITLE") or tags.get("filename")
        if title:
            # try to add extension from format name
            ext = ""
            format_name = fmt.get("format_name", "")
            if format_name:
                # pick mp4 if present, otherwise first
                parts = format_name.split(",")
                if "mp4" in parts:
                    ext = ".mp4"
                else:
                    ext = "." + parts[0]
            name = title
            if not os.path.splitext(name)[1] and ext:
                name = name + ext
            return await sanitize_filename(name)

        # 3) derive from format name or input path
        if fmt.get("format_name"):
            parts = fmt.get("format_name").split(",")
            chosen = "mp4" if "mp4" in parts else parts[0]
            ext = "." + chosen
            base = os.path.splitext(os.path.basename(input_path))[0]
            return await sanitize_filename(base + ext)

        # 4) fallback to basename
        return await sanitize_filename(os.path.basename(input_path))
    except Exception:
        logger.exception("detect_filename failed")
        return "file"
