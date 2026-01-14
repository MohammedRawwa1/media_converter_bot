# utils/error_handler.py
"""
Comprehensive error handling and logging system for the bot.
Provides user-friendly error messages while logging technical details.
"""

import asyncio
import logging
import traceback
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


class BotErrorHandler:
    """Centralized error handling for the bot."""

    # Error message mappings for user-friendly responses
    ERROR_MESSAGES = {
        "timeout": "⏱️ Operation took too long. Please try a smaller file or simpler conversion.",
        "file_not_found": "📁 File not found. Please check that the file exists and try again.",
        "file_too_large": "📦 File is too large for conversion. Maximum size is 2GB.",
        "invalid_format": "❌ Invalid file format. Supported formats: MP4, MP3, AVI, MOV, WAV, etc.",
        "conversion_failed": "⚠️ Conversion failed. This might be due to corrupted file or unsupported codec.",
        "disk_full": "💾 Not enough disk space. Please free up space and try again.",
        "permission_denied": "🔒 Permission denied. Cannot access or create file in that location.",
        "ffmpeg_error": "🎬 FFmpeg error. The conversion process encountered an issue.",
        "network_error": "🌐 Network error. Please check your connection and try again.",
        "cancelled": "❌ Operation was cancelled.",
        "internal_error": "😢 An unexpected error occurred. Please try again later.",
    }

    # Severity levels for logging
    SEVERITY = {
        "critical": 4,
        "error": 3,
        "warning": 2,
        "info": 1,
        "debug": 0,
    }

    def __init__(self):
        self.error_log: list = []
        self.max_log_size = 1000

    @staticmethod
    def categorize_error(exception: Exception, context: Optional[str] = None) -> str:
        """Categorize exception to determine user-friendly message."""
        exc_type = type(exception).__name__
        exc_str = str(exception).lower()

        # Check for common error patterns
        if "timeout" in exc_str or "timeout" in context.lower() if context else False:
            return "timeout"
        elif "file not found" in exc_str or "no such file" in exc_str:
            return "file_not_found"
        elif "too large" in exc_str or "file size" in exc_str:
            return "file_too_large"
        elif "format" in exc_str or "codec" in exc_str:
            return "invalid_format"
        elif "disk" in exc_str or "space" in exc_str:
            return "disk_full"
        elif "permission" in exc_str:
            return "permission_denied"
        elif "ffmpeg" in exc_str.lower():
            return "ffmpeg_error"
        elif "network" in exc_str or "connection" in exc_str:
            return "network_error"
        else:
            return "internal_error"

    @staticmethod
    def get_user_friendly_message(error_category: str) -> str:
        """Get user-friendly message for error category."""
        return BotErrorHandler.ERROR_MESSAGES.get(error_category, BotErrorHandler.ERROR_MESSAGES["internal_error"])

    def log_error(
        self,
        exception: Exception,
        context: str,
        severity: str = "error",
        user_id: Optional[int] = None,
        additional_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Log an error with full context.

        Returns:
            Dictionary with error details and user message
        """
        timestamp = datetime.now().isoformat()
        error_category = self.categorize_error(exception, context)
        user_message = self.get_user_friendly_message(error_category)

        # Build detailed error log
        error_entry = {
            "timestamp": timestamp,
            "severity": severity,
            "category": error_category,
            "context": context,
            "exception_type": type(exception).__name__,
            "exception_message": str(exception),
            "traceback": traceback.format_exc(),
            "user_id": user_id,
            "additional_info": additional_info or {},
            "user_message": user_message,
        }

        # Log based on severity
        if severity == "critical":
            logger.critical(f"[{context}] {error_entry['exception_type']}: {error_entry['exception_message']}")
        elif severity == "error":
            logger.error(f"[{context}] {error_entry['exception_type']}: {error_entry['exception_message']}")
        elif severity == "warning":
            logger.warning(f"[{context}] {error_entry['exception_type']}: {error_entry['exception_message']}")
        else:
            logger.info(f"[{context}] {error_entry['exception_type']}: {error_entry['exception_message']}")

        # Keep in memory log (with size limit)
        self.error_log.append(error_entry)
        if len(self.error_log) > self.max_log_size:
            self.error_log = self.error_log[-self.max_log_size :]

        return error_entry

    def get_error_report(self, limit: int = 50) -> str:
        """Get recent error report for debugging."""
        if not self.error_log:
            return "No errors recorded."

        recent_errors = self.error_log[-limit:]
        report = f"Recent Errors ({len(recent_errors)} total):\n"
        report += "=" * 60 + "\n"

        for i, error in enumerate(recent_errors, 1):
            report += f"\n{i}. [{error['timestamp']}] {error['context']}\n"
            report += f"   Type: {error['exception_type']}\n"
            report += f"   Message: {error['exception_message']}\n"
            if error["user_id"]:
                report += f"   User: {error['user_id']}\n"

        return report


# Global error handler instance
_error_handler = BotErrorHandler()


def get_error_handler() -> BotErrorHandler:
    """Get the global error handler instance."""
    return _error_handler


async def handle_conversion_error(
    exception: Exception,
    context: str,
    update=None,
    user_id: Optional[int] = None,
    send_user_message: Optional[Callable] = None,
) -> Dict[str, Any]:
    """
    Handle conversion errors with logging and user notification.

    Args:
        exception: The exception that occurred
        context: Description of what was happening
        update: Telegram Update object (optional)
        user_id: Telegram user ID (optional)
        send_user_message: Async function to send message to user

    Returns:
        Error information dictionary
    """
    handler = get_error_handler()

    # Get user ID if available
    if not user_id and update and update.effective_user:
        user_id = update.effective_user.id

    # Log the error
    error_info = handler.log_error(
        exception, context, severity="error", user_id=user_id, additional_info={"update": bool(update)}
    )

    # Send user message if possible
    if send_user_message:
        try:
            await send_user_message(error_info["user_message"])
            logger.debug(f"Error message sent to user {user_id}")
        except Exception as e:
            logger.error(f"Failed to send error message to user {user_id}: {e}")

    return error_info


def async_error_handler(context: str, send_user_message_callback: Optional[Callable] = None, re_raise: bool = False):
    """
    Decorator for async functions to handle errors gracefully.

    Args:
        context: Description of the operation
        send_user_message_callback: Optional async function to notify user
        re_raise: Whether to re-raise the exception after logging

    Usage:
        @async_error_handler(
            context="Video Conversion",
            send_user_message_callback=send_error_to_user,
            re_raise=False
        )
        async def convert_video(...):
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except asyncio.CancelledError:
                logger.warning(f"{context} was cancelled")
                if re_raise:
                    raise
                return False, "Operation was cancelled"
            except Exception as e:
                # Try to extract update and user_id from arguments
                update = None
                user_id = None

                for arg in args:
                    if hasattr(arg, "effective_user"):
                        update = arg
                        user_id = arg.effective_user.id
                        break

                # Log and handle error
                error_info = await handle_conversion_error(
                    e, context, update=update, user_id=user_id, send_user_message=send_user_message_callback
                )

                if re_raise:
                    raise

                # Return error tuple (success=False, message)
                return False, error_info["user_message"]

        return wrapper

    return decorator


# Logging configuration helper
def setup_comprehensive_logging(
    log_file: str = "logs/bot.log", level: int = logging.INFO, max_bytes: int = 10485760, backup_count: int = 5  # 10MB
) -> None:
    """
    Setup comprehensive logging with rotation.

    Args:
        log_file: Path to log file
        level: Logging level
        max_bytes: Max size before rotation
        backup_count: Number of backup files
    """
    import os
    from logging.handlers import RotatingFileHandler

    # Ensure log directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    # Create rotating file handler
    file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setLevel(level)

    # Create formatter with detailed information
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s() - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)

    # Add handler to root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)
    root_logger.setLevel(level)

    # Optional remote integrations
    # 1) Sentry (DSN via SENTRY_DSN env var)
    try:
        sentry_dsn = __import__("os").environ.get("SENTRY_DSN")
        if sentry_dsn:
            try:
                from sentry_sdk import init as sentry_init
                from sentry_sdk.integrations.logging import LoggingIntegration

                # Integrate logging with Sentry
                logging_integration = LoggingIntegration(level, level)
                sentry_init(dsn=sentry_dsn, integrations=[logging_integration])
                logger.info("✅ Sentry logging initialized")
            except Exception:
                logger.exception("Failed to initialize Sentry integration")
    except Exception:
        logger.debug("SENTRY_DSN check skipped")

    # 2) AWS CloudWatch via watchtower (LOG_GROUP via AWS_LOG_GROUP env var)
    try:
        env = __import__("os").environ
        cw_group = env.get("AWS_LOG_GROUP")
        if cw_group:
            try:
                import watchtower
                import boto3

                aws_region = env.get("AWS_REGION")
                session_kwargs = {}
                if aws_region:
                    session_kwargs["region_name"] = aws_region

                # Create CloudWatch handler and attach to root logger
                cw_handler = watchtower.CloudWatchLogHandler(log_group=cw_group, **session_kwargs)
                cw_handler.setLevel(level)
                cw_handler.setFormatter(formatter)
                root_logger.addHandler(cw_handler)
                logger.info("✅ CloudWatch logging initialized (group=%s)", cw_group)
            except Exception:
                logger.exception("Failed to initialize CloudWatch logging (watchtower)" )
    except Exception:
        logger.debug("CloudWatch check skipped")

    logger.info("✅ Comprehensive logging initialized")
