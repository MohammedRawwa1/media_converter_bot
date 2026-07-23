# utils/progress_tracker.py
import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TaskProgress:
    """Track progress of a conversion task."""

    task_id: str
    user_id: int
    file_name: str
    total_size: int
    processed_size: int = 0
    status: str = "pending"  # pending, downloading, processing, uploading, completed, failed
    start_time: float | None = None
    end_time: float | None = None
    error_message: str | None = None

    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage."""
        if self.total_size == 0:
            return 0
        return (self.processed_size / self.total_size) * 100

    @property
    def elapsed_time(self) -> float:
        """Get elapsed time in seconds."""
        if self.start_time is None:
            return 0
        return (self.end_time or time.time()) - self.start_time

    @property
    def estimated_time_remaining(self) -> float:
        """Estimate remaining time in seconds."""
        if self.progress_percentage == 0:
            return 0
        elapsed = self.elapsed_time
        if elapsed == 0:
            return 0
        return (elapsed / self.progress_percentage) * (100 - self.progress_percentage)

    def update_progress(self, processed_size: int):
        """Update processed size."""
        self.processed_size = processed_size

    def start(self):
        """Mark task as started."""
        self.start_time = time.time()
        self.status = "processing"

    def complete(self):
        """Mark task as completed."""
        self.end_time = time.time()
        self.status = "completed"
        self.processed_size = self.total_size

    def fail(self, error_message: str):
        """Mark task as failed."""
        self.end_time = time.time()
        self.status = "failed"
        self.error_message = error_message

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "task_id": self.task_id,
            "user_id": self.user_id,
            "file_name": self.file_name,
            "total_size": self.total_size,
            "processed_size": self.processed_size,
            "progress_percentage": self.progress_percentage,
            "status": self.status,
            "elapsed_time": self.elapsed_time,
            "estimated_time_remaining": self.estimated_time_remaining,
            "error_message": self.error_message,
        }


class ProgressTracker:
    """Manage multiple task progress trackers."""

    def __init__(self):
        self.tasks: dict[str, TaskProgress] = {}
        self.callbacks: dict[str, Callable] = {}

    def create_task(self, task_id: str, user_id: int, file_name: str, total_size: int) -> TaskProgress:
        """Create a new progress tracker for a task."""
        task = TaskProgress(task_id=task_id, user_id=user_id, file_name=file_name, total_size=total_size)
        self.tasks[task_id] = task
        logger.info(f"Created task tracker: {task_id}")
        return task

    def get_task(self, task_id: str) -> TaskProgress | None:
        """Get task progress by ID."""
        return self.tasks.get(task_id)

    async def update_task_progress(self, task_id: str, processed_size: int):
        """Update progress for a task."""
        task = self.tasks.get(task_id)
        if task:
            task.update_progress(processed_size)
            await self._notify_callbacks(task_id, task)

    def start_task(self, task_id: str):
        """Mark task as started."""
        task = self.tasks.get(task_id)
        if task:
            task.start()
            logger.info(f"Started task: {task_id}")

    async def complete_task(self, task_id: str):
        """Mark task as completed."""
        task = self.tasks.get(task_id)
        if task:
            task.complete()
            logger.info(f"Completed task: {task_id}")
            await self._notify_callbacks(task_id, task)

    async def fail_task(self, task_id: str, error_message: str):
        """Mark task as failed."""
        task = self.tasks.get(task_id)
        if task:
            task.fail(error_message)
            logger.error(f"Task failed: {task_id} - {error_message}")
            await self._notify_callbacks(task_id, task)

    def remove_task(self, task_id: str):
        """Remove task from tracker."""
        if task_id in self.tasks:
            del self.tasks[task_id]
            logger.info(f"Removed task: {task_id}")

    def register_callback(self, task_id: str, callback: Callable):
        """Register a callback for task updates."""
        self.callbacks[task_id] = callback

    async def _notify_callbacks(self, task_id: str, task: TaskProgress):
        """Notify registered callbacks of task update asynchronously."""
        callback = self.callbacks.get(task_id)
        if not callback:
            return

        try:
            import inspect

            # Check if callback is async
            if inspect.iscoroutinefunction(callback):
                await callback(task)
            else:
                # Run sync callback in executor to avoid blocking
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, callback, task)

        except Exception as e:
            logger.error(f"Error executing callback for task {task_id}: {e}")

    def get_all_tasks(self) -> dict[str, TaskProgress]:
        """Get all active tasks."""
        return self.tasks

    def cleanup_old_tasks(self, max_age_hours: int = 24):
        """Clean up old completed/failed tasks."""
        current_time = time.time()
        tasks_to_remove = []

        for task_id, task in self.tasks.items():
            if task.end_time and (current_time - task.end_time) > (max_age_hours * 3600):
                tasks_to_remove.append(task_id)

        for task_id in tasks_to_remove:
            self.remove_task(task_id)

        return len(tasks_to_remove)


# Global progress tracker instance
progress_tracker = ProgressTracker()


async def send_progress_update(chat_id: int, bot, task: TaskProgress, message_id: int | None = None):
    """Send or update progress message."""
    try:
        progress_bar = "🟩" * int(task.progress_percentage / 10) + "⬜" * (10 - int(task.progress_percentage / 10))

        message_text = f"""
📊 **Progress Update**

📁 File: {task.file_name}
📈 Progress: {task.progress_percentage:.1f}%
{progress_bar}

⏱️ Elapsed: {task.elapsed_time:.0f}s
⏳ Remaining: {task.estimated_time_remaining:.0f}s
📊 Status: {task.status.title()}

🆔 Task ID: `{task.task_id}`
"""

        if message_id:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text)
        else:
            msg = await bot.send_message(chat_id=chat_id, text=message_text)
            return msg.message_id

    except Exception as e:
        logger.error(f"Error sending progress update: {e}")
        return None
