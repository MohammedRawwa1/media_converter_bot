# tasks/cleanup_tasks.py
import asyncio
import logging
import os
import shutil
import time
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


class CleanupManager:
    """Manages cleanup of temporary files and old data."""

    def __init__(self):
        self.cleanup_interval = 3600  # 1 hour
        self.max_file_age = 24 * 3600  # 24 hours
        self.max_temp_age = 1 * 3600  # 1 hour
        self.is_running = False

    async def start(self):
        """Start periodic cleanup tasks."""
        self.is_running = True
        logger.info("Cleanup manager started")

        while self.is_running:
            try:
                await self.cleanup_all()
                await asyncio.sleep(self.cleanup_interval)
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error

    def stop(self):
        """Stop cleanup tasks."""
        self.is_running = False
        logger.info("Cleanup manager stopped")

    async def cleanup_all(self) -> dict:
        """Run all cleanup operations."""
        results = {
            "input_files": await self.cleanup_input_files(),
            "output_files": await self.cleanup_output_files(),
            "temp_files": await self.cleanup_temp_files(),
            "thumbnails": await self.cleanup_thumbnails(),
            "empty_dirs": await self.cleanup_empty_directories(),
        }

        total_cleaned = sum(results.values())
        if total_cleaned > 0:
            logger.info(f"Cleanup completed: {results}")

        return results

    async def cleanup_input_files(self) -> int:
        """Clean up old input files."""
        return await self._cleanup_directory("storage/input", self.max_file_age)

    async def cleanup_output_files(self) -> int:
        """Clean up old output files."""
        return await self._cleanup_directory("storage/output", self.max_file_age)

    async def cleanup_temp_files(self) -> int:
        """Clean up temporary files."""
        return await self._cleanup_directory("storage/temp", self.max_temp_age)

    async def cleanup_thumbnails(self) -> int:
        """Clean up old thumbnails."""
        return await self._cleanup_directory("storage/thumbnails", self.max_file_age)

    async def _cleanup_directory(self, directory: str, max_age: int) -> int:
        """Clean up files in a directory older than max_age."""
        try:
            if not os.path.exists(directory):
                return 0

            current_time = time.time()
            files_removed = 0

            for item in os.listdir(directory):
                item_path = os.path.join(directory, item)

                if os.path.isfile(item_path):
                    file_age = current_time - os.path.getmtime(item_path)
                    if file_age > max_age:
                        try:
                            os.remove(item_path)
                            files_removed += 1
                            logger.debug(f"Removed old file: {item_path}")
                        except Exception as e:
                            logger.error(f"Error removing file {item_path}: {e}")

                elif os.path.isdir(item_path):
                    # Recursively cleanup subdirectories
                    sub_removed = await self._cleanup_directory(item_path, max_age)
                    files_removed += sub_removed

            return files_removed

        except Exception as e:
            logger.error(f"Error cleaning directory {directory}: {e}")
            return 0

    async def cleanup_empty_directories(self) -> int:
        """Remove empty directories."""
        directories = ["storage/input", "storage/output", "storage/temp", "storage/thumbnails"]

        removed_count = 0

        for directory in directories:
            try:
                if os.path.exists(directory):
                    for root, dirs, files in os.walk(directory, topdown=False):
                        for dir_name in dirs:
                            dir_path = os.path.join(root, dir_name)
                            try:
                                if not os.listdir(dir_path):
                                    os.rmdir(dir_path)
                                    removed_count += 1
                                    logger.debug(f"Removed empty directory: {dir_path}")
                            except Exception as e:
                                logger.error(f"Error checking directory {dir_path}: {e}")
            except Exception as e:
                logger.error(f"Error cleaning empty directories in {directory}: {e}")

        return removed_count

    async def force_cleanup(self, directory: str = None) -> int:
        """Force cleanup of specific directory or all."""
        if directory and os.path.exists(directory):
            return await self._cleanup_directory(directory, 0)  # Clean all files
        else:
            results = await self.cleanup_all()
            return sum(results.values())

    async def get_storage_stats(self) -> dict:
        """Get storage usage statistics."""
        stats = {}
        directories = ["storage/input", "storage/output", "storage/temp", "storage/thumbnails"]

        for directory in directories:
            size_bytes = 0
            file_count = 0

            try:
                if os.path.exists(directory):
                    for root, dirs, files in os.walk(directory):
                        for file in files:
                            file_path = os.path.join(root, file)
                            if os.path.exists(file_path):
                                size_bytes += os.path.getsize(file_path)
                                file_count += 1
            except Exception as e:
                logger.error(f"Error getting stats for {directory}: {e}")

            dir_name = directory.split("/")[-1]
            stats[dir_name] = {"size_mb": size_bytes / (1024 * 1024), "file_count": file_count}

        stats["total"] = {
            "size_mb": sum(d["size_mb"] for d in stats.values()),
            "file_count": sum(d["file_count"] for d in stats.values()),
        }

        return stats


# Global cleanup manager instance
cleanup_manager = CleanupManager()


async def start_cleanup_task():
    """Start the cleanup manager as a background task."""
    await cleanup_manager.start()


def stop_cleanup_task():
    """Stop the cleanup manager."""
    cleanup_manager.stop()
