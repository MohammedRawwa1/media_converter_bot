# main.py
"""
Main entry point for media conversion bot - Updated for PTB v20+
"""

import asyncio
import logging
import signal
import time
import subprocess
import threading
import os
import hashlib
import json
import aiohttp
from urllib.parse import urlparse
import functools
import inspect

from telegram import Update, Bot
from telegram.error import TelegramError, TimedOut, Conflict
import httpx
# Request location differs across PTB releases; try both locations and
# fall back to None so the application can continue using default Request.
try:
    from telegram.request import Request
except Exception:
    try:
        from telegram.utils.request import Request
    except Exception:
        Request = None
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

import config as cfg
from config import (
    ADMIN_USER_ID,
    ALLOWED_USER_IDS,
    BOT_TOKEN,
    FFMPEG_PATH,
    WEBHOOK_URL,
    WEBHOOK_SECRET,
    is_user_allowed,
    persist_allowed_users,
)
from handlers import EnhancedMediaHandler
from tasks import (
    start_cleanup_task,
    stop_cleanup_task,
)
from utils import (
    MediaMenuBuilder,
    ensure_directories,
)
from utils.error_handler import (
    get_error_handler,
    setup_comprehensive_logging,
)
from utils.rate_limiter import ConversionRateLimiter, TelegramAPIRateLimiter, ConversionRateLimiterRedis
from utils.webhook_monitor import WebhookRecoveryManager
from utils.job_queue import cancel_job
try:
    from utils.storage import get_storage_backend
except Exception:
    get_storage_backend = None

# Configure comprehensive logging with rotation
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure directories exist early (important for Render/ASGI import-time logging)
try:
    from setup_directory import setup_bot_directories

    try:
        setup_bot_directories()
    except Exception:
        # Best-effort; continue if cannot create here
        pass
except Exception:
    # setup_directory may not be present or importable in some test environments
    pass

    # Ensure storage directories from config exist (best-effort)
    try:
        os.makedirs(cfg.STORAGE_PATH, exist_ok=True)
        os.makedirs(cfg.INPUT_PATH, exist_ok=True)
        os.makedirs(cfg.OUTPUT_PATH, exist_ok=True)
        os.makedirs(cfg.TEMP_PATH, exist_ok=True)
        os.makedirs(cfg.THUMBNAIL_PATH, exist_ok=True)
    except Exception:
        logger.debug("Could not ensure storage directories; continuing")

# Bot application handle for metrics and introspection when started under ASGI
BOT_APPLICATION = None
BOT_STARTED_AT = None
START_TIME = time.time()
BOT_READY = asyncio.Event()
LOGIN_PENDING_USERS = set()
# Simple Prometheus-style in-memory metrics for ASGI endpoints and dispatch tracking
METRICS = {
    "webhooks_received": 0,
    "updates_dispatched": 0,
    "updates_queued": 0,
    "dispatch_failures": 0,
    "dispatch_attempts": 0,
}
METRICS_LOCK = threading.Lock()

class AwaitingLoginFilter(filters.MessageFilter):
    """Filter text messages only for users in the login flow."""

    def filter(self, message):
        try:
            user = getattr(message, "from_user", None)
            return bool(user and user.id in LOGIN_PENDING_USERS)
        except Exception:
            return False


async def _dispatch_update_task(update):
    """Dispatch a single Update to the Application or dispatcher, updating metrics.

    This helper is safe to schedule from background tasks and centralizes
    error handling and metrics increments used by webhook and ASGI consumers.
    """
    try:
        disp = getattr(BOT_APPLICATION, "dispatcher", None)
        if disp and hasattr(disp, "process_update"):
            try:
                with METRICS_LOCK:
                    METRICS["dispatch_attempts"] = METRICS.get("dispatch_attempts", 0) + 1
            except Exception:
                pass
            await disp.process_update(update)
            try:
                with METRICS_LOCK:
                    METRICS["updates_dispatched"] = METRICS.get("updates_dispatched", 0) + 1
            except Exception:
                pass
            return

        # Fall back to Application.process_update if available
        if hasattr(BOT_APPLICATION, "process_update"):
            try:
                with METRICS_LOCK:
                    METRICS["dispatch_attempts"] = METRICS.get("dispatch_attempts", 0) + 1
            except Exception:
                pass
            await BOT_APPLICATION.process_update(update)
            try:
                with METRICS_LOCK:
                    METRICS["updates_dispatched"] = METRICS.get("updates_dispatched", 0) + 1
            except Exception:
                pass
            return

        # As a last resort, try to enqueue back onto the application's update queue
        try:
            await BOT_APPLICATION.update_queue.put(update)
            try:
                with METRICS_LOCK:
                    METRICS["updates_queued"] = METRICS.get("updates_queued", 0) + 1
            except Exception:
                pass
            return
        except Exception:
            try:
                with METRICS_LOCK:
                    METRICS["dispatch_failures"] = METRICS.get("dispatch_failures", 0) + 1
            except Exception:
                pass
            logger.exception("Failed to dispatch or enqueue update")
    except Exception as exc:
        try:
            with METRICS_LOCK:
                METRICS["dispatch_failures"] = METRICS.get("dispatch_failures", 0) + 1
        except Exception:
            pass
        logger.exception("Error dispatching update: %s", exc)



async def check_ffmpeg_available() -> bool:
    """Return True if ffmpeg is callable from PATH or configured FFMPEG_PATH.

    This runs the check in a thread to avoid blocking the event loop.
    """
    def _probe():
        try:
            proc = subprocess.run([FFMPEG_PATH, "-version"], capture_output=True, text=True, timeout=5)
            return proc.returncode == 0
        except Exception:
            return False

    try:
        return await asyncio.to_thread(_probe)
    except Exception:
        return False


# Setup comprehensive logging with file rotation
try:
    setup_comprehensive_logging(log_file="logs/bot.log", level=logging.INFO, max_bytes=10485760, backup_count=5)  # 10MB
except Exception as e:
    logger.warning(f"Could not setup rotating file handler: {e}")

# Initialize Sentry if configured via SENTRY_DSN environment variable
try:
    SENTRY_DSN = __import__("os").environ.get("SENTRY_DSN")
    if SENTRY_DSN:
        try:
            import importlib

            sentry_sdk = importlib.import_module("sentry_sdk")
            sentry_sdk.init(dsn=SENTRY_DSN, traces_sample_rate=0.0)
            logger.info("Sentry initialized")
        except Exception as se:
            logger.warning(f"Failed to initialize Sentry: {se}")
except Exception:
    pass


# Command handlers
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start command handler."""
    user_id = update.effective_user.id
    # Enforce ACL for private bots
    try:
        if not is_user_allowed(user_id):
            await update.message.reply_text("Access denied. This bot is private.")
            return
    except Exception:
        await update.message.reply_text("Access denied. (ACL check failed)")
        return

    user_name = update.effective_user.first_name

    welcome_text = f"""
🎬 **Welcome to Media Conversion Bot** 🎧

Hello {user_name}! Send a media file and choose an action from the menu.

Available slash commands (exact):
/start - Show this welcome message
/help - Show feature list and usage
/settings - Open your user settings (aliases: /usettings, /usersettings)
/bulkmenu - Open bulk/URL tools
/cancel - Cancel current operation
/canceljob <job_id> - Request cancellation for a queued/running job (admin only)
/admin add|remove|list <user_id> - Manage allowed users (admin only)
/addthumb - Add default thumbnail (if enabled)
/delthumb - Remove default thumbnail (if enabled)

Send me a file to get started! 🚀
"""

    await update.message.reply_text(welcome_text, parse_mode="Markdown")
    logger.info(f"User {user_id} ({user_name}) started the bot")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Help command handler."""
    help_text = """
📚 **Complete Feature List:**

**🎬 VIDEO PROCESSING:**
• Convert to different formats (MP4, AVI, MOV, MKV, etc.)
• Convert to MP3/audio formats
• Compress with quality presets (High → Extreme)
• Change resolution (4K → 360p)
• Adjust framerate
• Trim start/end sections
• Merge multiple videos
• Remove/Add audio track
• Extract streams and subtitles
• Take screenshots (single or grid)
• Repair corrupted files
• Optimize for web/mobile/TV
• Change bitrate
• Edit metadata

**🎧 AUDIO PROCESSING:**
• Convert between formats (MP3, WAV, AAC, FLAC, OGG, M4A)
• Adjust bitrate (64k-320k)
• Normalize volume
• Trim segments
• Merge multiple files
• Extract from video
• Change sample rate
• Adjust channels (mono/stereo)

**🔧 UTILITIES:**
• Full media analysis and information
• Create ZIP archives
• Batch processing
• Progress tracking
• Background processing
• Auto-cleanup of old files

**File Limits:**
• Maximum file size: 4GB
• Processing time: Depends on file size
• Results auto-delete after sending

**Utility Commands:**
`/cancel` - Cancel current operation
`/admin` - Manage allowed users (admin only)

**Need help?** Just send a file and use the menus! 🎯
"""

    await update.message.reply_text(help_text, parse_mode="Markdown")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cancel current operation."""
    await update.message.reply_text(
        "❌ Operation cancelled.\n\n" "Send /start to see available options.",
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors in the application with comprehensive logging."""
    # Don't respond to cancelled tasks
    if isinstance(context.error, asyncio.CancelledError):
        logger.info("Task was cancelled")
        return

    # Get error handler instance
    error_handler_inst = get_error_handler()

    # Extract user info if available
    user_id = None
    if update and hasattr(update, "effective_user") and update.effective_user:
        user_id = update.effective_user.id

    # Log detailed error
    error_info = error_handler_inst.log_error(
        context.error,
        "Telegram Update Processing",
        severity="error",
        user_id=user_id,
        additional_info={
            "update_type": type(update).__name__ if update else "None",
        },
    )

    # Log full traceback
    logger.error("Exception while handling an update:", exc_info=context.error)

    # Handle Telegram-specific errors
    if isinstance(context.error, TelegramError):
        logger.warning(f"Telegram API error: {context.error}")

    # Get user-friendly message
    user_message = error_info["user_message"]

    # Try to send error message to user
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(user_message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Failed to send error message to user: {e}")
    elif update and update.effective_chat:
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=user_message,
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error(f"Failed to send error to chat: {e}")


def setup_handlers(application: Application) -> None:
    """Setup all bot handlers."""
    # Initialize handler manager
    handler_manager = EnhancedMediaHandler()

    # Helper: wrap handler callbacks to measure latency and log slow handlers.
    def latency_wrapper(fn, label: str | None = None):
        if label is None:
            try:
                label = fn.__name__
            except Exception:
                label = str(fn)

        threshold = float(os.getenv("HANDLER_LATENCY_THRESHOLD", "1.0"))

        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def _wrapped(*args, **kwargs):
                start = time.time()
                try:
                    return await fn(*args, **kwargs)
                finally:
                    dur = time.time() - start
                    if dur > threshold:
                        logger.warning("Handler '%s' slow: %.3fs", label, dur)
                    else:
                        logger.debug("Handler '%s' finished: %.3fs", label, dur)

            return _wrapped

        # Sync function fallback: run in executor to avoid blocking loop
        @functools.wraps(fn)
        async def _wrapped_sync(*args, **kwargs):
            start = time.time()
            try:
                return await asyncio.to_thread(functools.partial(fn, *args, **kwargs))
            finally:
                dur = time.time() - start
                if dur > threshold:
                    logger.warning("Sync handler '%s' slow: %.3fs", label, dur)
                else:
                    logger.debug("Sync handler '%s' finished: %.3fs", label, dur)

        return _wrapped_sync

    # Initialize MongoDB model if MONGO_URI provided
    try:
        import os

        # Resolve which canonical env var (if any) provides the Mongo URI.
        mongo_uri = None
        mongo_env_key = None
        for _key in ("MONGO_URI", "MONGODB_URL", "MONGODB_URI", "MONGO_URL"):
            _val = os.environ.get(_key)
            if _val:
                mongo_uri = _val
                mongo_env_key = _key
                break

        if mongo_uri:
            try:
                from motor.motor_asyncio import AsyncIOMotorClient
                from models import MediaConversionModel

                # Log which env var was used (only show host, never secrets)
                try:
                    from urllib.parse import urlparse

                    parsed = urlparse(mongo_uri)
                    host_display = parsed.hostname or mongo_uri.split("@")[-1].split("/")[0]
                except Exception:
                    host_display = "unknown-host"
                logger.info("Using Mongo env var %s (host=%s)", mongo_env_key, host_display)

                # Allow short server-selection/connect timeouts to fail fast
                # when MongoDB is unreachable. Values are in milliseconds.
                try:
                    srv_timeout = int(os.environ.get("MONGO_SERVER_SELECTION_TIMEOUT_MS", os.environ.get("MONGO_SERVER_TIMEOUT_MS", "5000")))
                except Exception:
                    srv_timeout = 5000
                try:
                    conn_timeout = int(os.environ.get("MONGO_CONNECT_TIMEOUT_MS", "5000"))
                except Exception:
                    conn_timeout = 5000

                try:
                    client = AsyncIOMotorClient(mongo_uri, serverSelectionTimeoutMS=srv_timeout, connectTimeoutMS=conn_timeout)
                    logger.info("Mongo client created with serverSelectionTimeoutMS=%sms connectTimeoutMS=%sms", srv_timeout, conn_timeout)
                except Exception:
                    # Fallback to default constructor when custom kwargs cause issues
                    client = AsyncIOMotorClient(mongo_uri)

                # Determine bot_id from environment if provided (BOT_ID or BOT_USERNAME)
                bot_id = os.environ.get("BOT_ID") or os.environ.get("BOT_USERNAME") or os.environ.get("BOT_NAME")
                model = MediaConversionModel(
                    client,
                    db_name=os.environ.get("MONGODB_NAME") or "media_conversion_bot",
                    bot_id=bot_id,
                    collection_prefix=os.environ.get("MONGODB_COLLECTION_PREFIX"),
                )
                # Schedule asynchronous index creation so failures are handled
                # inside the event loop rather than in background threads.
                try:
                    import asyncio

                    asyncio.create_task(model.ensure_indexes())
                except Exception:
                    logger.debug("Could not schedule async index creation for Mongo model")
                handler_manager.db_model = model
                application.bot_data["db_model"] = model
                logger.info("✅ MongoDB model initialized for logging conversions")
            except Exception:
                logger.exception("Failed to initialize MongoDB model (motor)")
    except Exception:
        logger.debug("MONGO_URI check skipped")

    # Command handlers (wrapped for latency tracing)
    application.add_handler(CommandHandler("start", latency_wrapper(start_command, "start_command")))
    application.add_handler(CommandHandler("help", latency_wrapper(help_command, "help_command")))
    application.add_handler(CommandHandler("cancel", latency_wrapper(cancel_command, "cancel_command")))

    async def loginstatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Admin-only diagnostic: show current login flow context (masked)."""
        try:
            user_id = update.effective_user.id
        except Exception:
            await update.message.reply_text("Could not determine user id")
            return
        if user_id != ADMIN_USER_ID:
            await update.message.reply_text("Unauthorized")
            return

        data = context.user_data
        sent_at = data.get("login_code_sent_at")
        sent_repr = data.get("login_code_sent_repr")
        resend_count = data.get("login_resend_count", 0)
        code_hash = data.get("login_code_hash")
        session_path = data.get("login_session_path")
        awaiting = {
            "phone": bool(data.get("awaiting_login_phone")),
            "code": bool(data.get("awaiting_login_code")),
            "password": bool(data.get("awaiting_login_password")),
        }
        # Mask code_hash
        masked_hash = None
        try:
            if code_hash:
                s = str(code_hash)
                masked_hash = s[:4] + "..." + s[-4:]
        except Exception:
            masked_hash = None

        lines = [
            f"awaiting: {awaiting}",
            f"sent_at: {sent_at}",
            f"resend_count: {resend_count}",
            f"code_hash: {masked_hash}",
            f"sent_repr: {str(sent_repr)[:200] if sent_repr else None}",
            f"session_path: {session_path}",
        ]
        await update.message.reply_text("\n".join(lines))

    application.add_handler(CommandHandler("loginstatus", latency_wrapper(loginstatus_command, "loginstatus_command")))

    # Media file handlers (videos, audio, documents)
    # Build the media filter defensively to support multiple PTB versions.
    # Build a resilient media filter using a shared helper (supports
    # multiple PTB versions and import variants).
    try:
        from utils.filter_utils import build_media_filter

        media_filter = build_media_filter(filters)
        if media_filter is None:
            # Preserve non-text media handling even when PTB-specific filters
            # cannot be resolved cleanly.
            media_filter = filters.ALL & ~filters.TEXT
    except Exception:
        # In case the helper isn't available for any reason, fall back safely.
        media_filter = filters.ALL & ~filters.TEXT

    application.add_handler(MessageHandler(media_filter, latency_wrapper(handler_manager.handle_media_message, "handle_media_message")))

    try:
        url_filter = filters.Regex(r"https?://") & ~filters.COMMAND
        application.add_handler(MessageHandler(url_filter, latency_wrapper(handler_manager.handle_media_message, "handle_media_url_message"), block=True))
    except Exception:
        logger.debug("URL text handler not registered; Regex filter unavailable")

    # Ensure a fallback handler is present for non-command, non-text messages.
    try:
        fallback_filter = filters.ALL & ~filters.COMMAND & ~filters.TEXT
        application.add_handler(MessageHandler(fallback_filter, latency_wrapper(handler_manager.handle_media_message, "handle_media_message_fallback")))
        logger.info("Fallback media handler registered for non-command non-text messages")
    except Exception:
        logger.debug("Fallback media handler not registered")

    # Callback query handler for menu interactions
    application.add_handler(CallbackQueryHandler(latency_wrapper(handler_manager.callback_handler, "callback_handler")))

    # Register custom thumbnail commands if module available
    try:
        from custom_thumbnail import add_thumb, del_thumb

        application.add_handler(CommandHandler("addthumb", latency_wrapper(add_thumb, "add_thumb")))
        application.add_handler(CommandHandler("delthumb", latency_wrapper(del_thumb, "del_thumb")))
        logger.info("Registered custom thumbnail commands (/addthumb, /delthumb)")
    except Exception:
        logger.debug("custom_thumbnail handlers not registered")

    # Admin commands (manage allowed users)
    async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        # Only admin may manage allowed users
        if ADMIN_USER_ID and user_id != ADMIN_USER_ID:
            await update.message.reply_text("Unauthorized: admin only")
            return

        args = context.args if hasattr(context, "args") else []
        if not args:
            await update.message.reply_text("Usage: /admin add|remove|list <user_id>")
            return

        cmd = args[0].lower()
        if cmd == "list":
            users = sorted(list(ALLOWED_USER_IDS))
            await update.message.reply_text(f"Allowed users: {users}")
            return

        if len(args) < 2:
            await update.message.reply_text("Specify a user id")
            return

        try:
            target = int(args[1])
        except Exception:
            await update.message.reply_text("Invalid user id")
            return

        if cmd == "add":
            cfg.ALLOWED_USER_IDS.add(target)
            persist_allowed_users()
            await update.message.reply_text(f"Added {target} to allowed users")
            return
        if cmd == "remove":
            cfg.ALLOWED_USER_IDS.discard(target)
            persist_allowed_users()
            await update.message.reply_text(f"Removed {target} from allowed users")
            return

        await update.message.reply_text("Unknown admin command")

    application.add_handler(CommandHandler("admin", latency_wrapper(admin_command, "admin_command")))

    async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if ADMIN_USER_ID and user_id != ADMIN_USER_ID:
            await update.message.reply_text("Unauthorized: admin only")
            return

        # We require API_ID/API_HASH for local Telethon login.
        api_id = os.getenv("API_ID") or os.getenv("USERBOT_API_ID")
        api_hash = os.getenv("API_HASH") or os.getenv("USERBOT_API_HASH")
        if not api_id or not api_hash:
            await update.message.reply_text(
                "Missing Telethon credentials. Set API_ID and API_HASH in the environment before using /login."
            )
            return

        try:
            await update.message.reply_text(
                "Please send the phone number for the userbot session in international format, e.g. +1234567890."
            )
            context.user_data["awaiting_login_phone"] = True
            LOGIN_PENDING_USERS.add(user_id)
            return
        except Exception:
            await update.message.reply_text("Failed to prompt for Telethon login phone number.")
            return

    application.add_handler(CommandHandler("login", latency_wrapper(login_command, "login_command")))

    async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if ADMIN_USER_ID and user_id != ADMIN_USER_ID:
            await update.message.reply_text("Unauthorized: admin only")
            return

        try:
            from utils.telethon_session import get_telethon_session_path

            session_path = get_telethon_session_path()
            removed = []
            if os.path.exists(session_path):
                try:
                    os.remove(session_path)
                    removed.append(session_path)
                except Exception:
                    pass
            for suffix in (".session", ".session-journal", ".session.lock"):
                path_with_suffix = session_path + suffix
                if os.path.exists(path_with_suffix):
                    try:
                        os.remove(path_with_suffix)
                        removed.append(path_with_suffix)
                    except Exception:
                        pass

            if removed:
                await update.message.reply_text(
                    f"✅ Logged out and removed Telethon session files:\n{chr(10).join(removed)}"
                )
            else:
                await update.message.reply_text(
                    "No local Telethon session file was found to remove."
                )
        except Exception as exc:
            logger.exception("/logout failed: %s", exc)
            await update.message.reply_text(
                "Failed to remove the Telethon session. Check server logs for details."
            )

    application.add_handler(CommandHandler("logout", latency_wrapper(logout_command, "logout_command")))

    async def canceljob_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        # restrict to admin or allowed users
        if ADMIN_USER_ID and user_id != ADMIN_USER_ID:
            await update.message.reply_text("Unauthorized: admin only")
            return

        args = context.args if hasattr(context, "args") else []
        if not args:
            await update.message.reply_text("Usage: /canceljob <job_id>")
            return

        job_id = args[0]
        try:
            await cancel_job(job_id)
            await update.message.reply_text(f"Requested cancellation for job {job_id}")
        except Exception as e:
            logger.exception("Failed to request cancel for job %s: %s", job_id, e)
            await update.message.reply_text(f"Failed to cancel job {job_id}: {e}")

    application.add_handler(CommandHandler("canceljob", latency_wrapper(canceljob_command, "canceljob_command")))

    # Settings command - forward to handler manager's show_settings
    async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            await handler_manager.show_settings(update, context)
        except Exception:
            await update.message.reply_text("⚠️ Failed to open settings.")

    application.add_handler(CommandHandler("settings", latency_wrapper(settings_command, "settings_command")))
    application.add_handler(CommandHandler("usettings", latency_wrapper(settings_command, "settings_command")))
    application.add_handler(CommandHandler("usersettings", latency_wrapper(settings_command, "settings_command")))

    async def bulk_url_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            await handler_manager.bulk_url_command(update, context)
        except Exception:
            await update.message.reply_text("⚠️ Failed to enqueue bulk URLs.")

    application.add_handler(CommandHandler("bulk_url", latency_wrapper(bulk_url_command, "bulk_url_command")))

    async def bulk_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            await handler_manager.show_bulk_menu(update, context)
        except Exception:
            await update.message.reply_text("⚠️ Failed to open bulk menu.")

    application.add_handler(CommandHandler("bulkmenu", latency_wrapper(bulk_menu_command, "bulk_menu_command")))

    def _clear_login_flow(user_id, context):
        try:
            LOGIN_PENDING_USERS.discard(user_id)
        except Exception:
            pass
        if context is not None and getattr(context, "user_data", None) is not None:
            for key in (
                "awaiting_login_phone",
                "awaiting_login_code",
                "awaiting_login_password",
                "login_phone",
                "login_client",
                "login_session_path",
            ):
                context.user_data.pop(key, None)

    async def _process_login_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = getattr(update.effective_user, "id", None)
        if not user_id:
            return

        if not (
            context.user_data.get("awaiting_login_phone")
            or context.user_data.get("awaiting_login_code")
            or context.user_data.get("awaiting_login_password")
        ):
            _clear_login_flow(user_id, context)
            return

        if context.user_data.get("awaiting_login_phone"):
            context.user_data["awaiting_login_phone"] = False
            phone = update.message.text.strip()
            await update.message.reply_text("Got phone number. Please wait while I generate the Telethon session...")
            try:
                from telethon import TelegramClient
            except Exception:
                await update.message.reply_text(
                    "Telethon is not installed on the server. Install telethon to use /login."
                )
                _clear_login_flow(user_id, context)
                return

            api_id = os.getenv("API_ID") or os.getenv("USERBOT_API_ID")
            api_hash = os.getenv("API_HASH") or os.getenv("USERBOT_API_HASH")
            try:
                api_id = int(api_id)
            except Exception:
                await update.message.reply_text("Configured API_ID is invalid. It must be an integer.")
                _clear_login_flow(user_id, context)
                return

            session_name = (
                os.getenv("API_SESSION_NAME")
                or os.getenv("SESSION_NAME")
                or os.getenv("USERBOT_SESSION_NAME")
                or os.getenv("TELETHON_SESSION_NAME")
                or "userbot_session"
            )
            session_dir = os.getenv("TELETHON_SESSION_DIR") or os.getenv("TEMP_PATH") or os.getcwd()
            os.makedirs(session_dir, exist_ok=True)
            session_path = os.path.join(session_dir, session_name)

            client = TelegramClient(session_path, api_id, api_hash)
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    # Capture the returned object from send_code_request so we can
                    # provide the phone_code_hash to sign_in when required by
                    # certain Telethon/server variants.
                    try:
                        sent = await client.send_code_request(phone)
                    except TypeError:
                        # Older/newer Telethon variants may have different call
                        # signatures; fall back to the simple call.
                        sent = await client.send_code_request(phone)

                    # Debug: record when the code was requested and returned hash
                    try:
                        logger.info(
                            "Requested login code for %s; sent_obj=%s",
                            phone,
                            repr(sent),
                        )
                        sent_time = time.time()
                        context.user_data["login_code_sent_at"] = sent_time
                        context.user_data["login_code_sent_repr"] = repr(sent)
                    except Exception:
                        pass

                    # Store the phone_code_hash if available for later sign_in.
                    try:
                        code_hash = getattr(sent, "phone_code_hash", None)
                    except Exception:
                        code_hash = None

                    if code_hash:
                        context.user_data["login_code_hash"] = code_hash

                    await update.message.reply_text(
                        "A login code has been sent. Please reply with the code you receive."
                    )
                    context.user_data["awaiting_login_code"] = True
                    context.user_data["login_phone"] = phone
                    context.user_data["login_client"] = client
                    context.user_data["login_session_path"] = session_path
                    return
                else:
                    await update.message.reply_text(
                        f"Telethon session is already authorized and saved to {session_path}. You can now use userbot fallback."
                    )
                    await client.disconnect()
                    _clear_login_flow(user_id, context)
                    return
            except Exception as exc:
                logger.exception("/login phone step failed: %s", exc)
                await update.message.reply_text(
                    "Failed to start Telethon login. Check API_ID/API_HASH and the phone number."
                )
                try:
                    await client.disconnect()
                except Exception:
                    pass
                _clear_login_flow(user_id, context)
                return

        if context.user_data.get("awaiting_login_code"):
            code = update.message.text.strip()
            client = context.user_data.get("login_client")
            phone = context.user_data.get("login_phone")
            if client is None or not phone:
                await update.message.reply_text(
                    "Session state lost. Please run /login again to start a fresh login."
                )
                _clear_login_flow(user_id, context)
                return

            try:
                # Normalize code input (handle Unicode digits and stray chars)
                trans_digits = str.maketrans(
                    {
                        "٠": "0",
                        "١": "1",
                        "٢": "2",
                        "٣": "3",
                        "٤": "4",
                        "٥": "5",
                        "٦": "6",
                        "٧": "7",
                        "٨": "8",
                        "٩": "9",
                        "۰": "0",
                        "۱": "1",
                        "۲": "2",
                        "۳": "3",
                        "۴": "4",
                        "۵": "5",
                        "۶": "6",
                        "۷": "7",
                        "۸": "8",
                        "۹": "9",
                    }
                )
                norm_code = (code or "").translate(trans_digits)
                # Remove any non-digit characters
                norm_code = "".join([c for c in norm_code if c.isdigit()])

                # Debug: record code usage context before attempting sign-in
                try:
                    entered_at = time.time()
                    sent_at = context.user_data.get("login_code_sent_at")
                    resend_count = context.user_data.get("login_resend_count", 0)
                    code_hash_preview = str(context.user_data.get("login_code_hash"))
                    client_session = getattr(getattr(client, 'session', None), 'filename', None) or repr(getattr(client, 'session', None))
                    masked_code = (norm_code[-2:].rjust(2, "*") if norm_code else "")
                    logger.info(
                        "Attempting sign_in: user=%s entered_at=%s sent_at=%s delta=%.3fs resend_count=%s code_hash=%s session=%s code_tail=%s",
                        user_id,
                        entered_at,
                        sent_at,
                        (entered_at - sent_at) if sent_at else -1,
                        resend_count,
                        code_hash_preview,
                        client_session,
                        masked_code,
                    )
                    # Admin-only: log the full normalized code for debugging
                    try:
                        if user_id == ADMIN_USER_ID:
                            logger.info("Admin sign-in code (normalized) for user=%s: %s", user_id, norm_code)
                    except Exception:
                        pass
                except Exception:
                    pass

                # Use stored phone_code_hash when available to match the
                # send_code_request response; fall back to alternate
                # sign_in signatures for different Telethon versions.
                code_hash = context.user_data.get("login_code_hash")
                if code_hash:
                    try:
                        await client.sign_in(phone=phone, code=norm_code, phone_code_hash=code_hash)
                    except TypeError:
                        try:
                            await client.sign_in(code=norm_code)
                        except TypeError:
                            await client.sign_in(phone=phone, code=norm_code)
                else:
                    try:
                        await client.sign_in(code=norm_code)
                    except TypeError:
                        await client.sign_in(phone=phone, code=norm_code)
                if await client.is_user_authorized():
                    session_path = context.user_data.get("login_session_path")
                    await update.message.reply_text(
                        "✅ Telethon userbot login successful. Session saved locally."
                        + (f"\nSaved session: {session_path}" if session_path else "")
                    )
                    await client.disconnect()
                    _clear_login_flow(user_id, context)
                    return
                else:
                    await update.message.reply_text(
                        "Login code accepted but the session is not authorized. Please reply with your password if 2FA is enabled."
                    )
                    context.user_data["awaiting_login_password"] = True
                    context.user_data.pop("awaiting_login_code", None)
                    return
            except Exception as exc:
                # Detect Telethon-specific exceptions and give actionable messages
                try:
                    from telethon.errors import (
                        SessionPasswordNeededError,
                        PhoneCodeInvalidError,
                        PhoneCodeExpiredError,
                        FloodWaitError,
                    )
                except Exception:
                    SessionPasswordNeededError = PhoneCodeInvalidError = PhoneCodeExpiredError = FloodWaitError = None

                # 2FA required
                if SessionPasswordNeededError and isinstance(exc, SessionPasswordNeededError):
                    await update.message.reply_text(
                        "Two-step verification is enabled. Please reply with your account password."
                    )
                    context.user_data["awaiting_login_password"] = True
                    context.user_data.pop("awaiting_login_code", None)
                    return

                # Specific error responses
                friendly = None
                try:
                    if PhoneCodeInvalidError and isinstance(exc, PhoneCodeInvalidError):
                        friendly = "The code you entered is invalid. Please request a new code and try again."
                    elif PhoneCodeExpiredError and isinstance(exc, PhoneCodeExpiredError):
                        # Attempt an automatic resend with a small retry cap to
                        # avoid triggering flood limits.
                        resend_count = context.user_data.get("login_resend_count", 0)
                        if resend_count < 3:
                            try:
                                # Send a fresh code and update stored code hash
                                try:
                                    sent = await client.send_code_request(phone)
                                except TypeError:
                                    sent = await client.send_code_request(phone)
                                new_hash = getattr(sent, "phone_code_hash", None)
                                try:
                                    logger.info(
                                        "Resent login code for %s; sent_obj=%s",
                                        phone,
                                        repr(sent),
                                    )
                                    context.user_data["login_code_sent_at"] = time.time()
                                    context.user_data["login_code_sent_repr"] = repr(sent)
                                except Exception:
                                    pass
                                if new_hash:
                                    context.user_data["login_code_hash"] = new_hash
                                context.user_data["login_resend_count"] = resend_count + 1
                                context.user_data["awaiting_login_code"] = True
                                context.user_data["login_client"] = client
                                context.user_data["login_phone"] = phone
                                context.user_data["login_session_path"] = context.user_data.get("login_session_path")
                                friendly = "The code expired — I sent a new code. Please reply with the fresh code you receive."
                            except Exception as e2:
                                logger.exception("Failed to resend login code: %s", e2)
                                friendly = "The code expired and I couldn't request a new one. Try /login again in a few minutes."
                        else:
                            friendly = "The code has expired. Please request a new code with /login and try again."
                    elif FloodWaitError and isinstance(exc, FloodWaitError):
                        wait = getattr(exc, 'seconds', None) or getattr(exc, 'timeout', None) or 'a while'
                        friendly = f"Too many attempts; please wait {wait} seconds before retrying."
                except Exception:
                    friendly = None

                # Log detailed exception information for debugging
                logger.exception("/login code step failed (user=%s): %s", user_id, exc)

                # Reply with a helpful message to admin users; others get a generic prompt
                try:
                    if friendly:
                        await update.message.reply_text(friendly)
                    else:
                        if user_id == ADMIN_USER_ID:
                            await update.message.reply_text(
                                f"Sign-in error: {exc.__class__.__name__}: {exc}\nSee logs for details."
                            )
                        else:
                            await update.message.reply_text(
                                "Failed to complete Telethon login. Please make sure the code is correct and try /login again."
                            )
                except Exception:
                    pass

                try:
                    await client.disconnect()
                except Exception:
                    pass
                _clear_login_flow(user_id, context)
                return

        if context.user_data.get("awaiting_login_password"):
            password = update.message.text.strip()
            client = context.user_data.get("login_client")
            if client is None:
                await update.message.reply_text(
                    "Session state lost. Please run /login again to start a fresh login."
                )
                _clear_login_flow(user_id, context)
                return

            try:
                await client.sign_in(password=password)
                if await client.is_user_authorized():
                    session_path = context.user_data.get("login_session_path")
                    await update.message.reply_text(
                        "✅ Telethon userbot login successful. Session saved locally."
                        + (f"\nSaved session: {session_path}" if session_path else "")
                    )
                else:
                    await update.message.reply_text(
                        "Password accepted but the session is not authorized. Please run /login again."
                    )
            except Exception as exc:
                logger.exception("/login password step failed: %s", exc)
                await update.message.reply_text(
                    "Failed to complete Telethon login with password. Please try /login again."
                )
            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass
                _clear_login_flow(user_id, context)
            return

        _clear_login_flow(user_id, context)
        return

    login_text_filter = filters.TEXT & ~filters.COMMAND & AwaitingLoginFilter()
    application.add_handler(
        MessageHandler(login_text_filter, latency_wrapper(_process_login_text, "process_login_text"), block=True)
    )

    # Store handler manager in bot_data for access in other handlers
    application.bot_data["handler_manager"] = handler_manager

    # Error handler (must be added last)
    application.add_error_handler(error_handler)

    logger.info("✅ All handlers registered successfully")


async def main(background: bool = False) -> None:
    """Start the bot."""
    # Run quick env validation (logs missing keys but never prints secrets)
    try:
        cfg.validate_env()
    except Exception:
        logger.debug("Env validation helper failed (non-fatal)")
    # Validate BOT_TOKEN
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN not set in environment variables!")
        raise ValueError("BOT_TOKEN is required. Set it in .env file.")

    # Create the Application for PTB v20+
    # Build a Bot with a custom Request to increase the HTTP connection pool
    # and avoid httpx PoolTimeouts when many concurrent API calls occur.
    try:
        http_pool_size = int(os.environ.get("HTTP_POOL_SIZE", "50"))
    except Exception:
        http_pool_size = 50
    try:
        http_pool_timeout = float(os.environ.get("HTTP_POOL_TIMEOUT", "30"))
    except Exception:
        http_pool_timeout = 30.0
    try:
        http_connect_timeout = float(os.environ.get("HTTP_CONNECT_TIMEOUT", "5"))
    except Exception:
        http_connect_timeout = 5.0
    try:
        http_read_timeout = float(os.environ.get("HTTP_READ_TIMEOUT", "30"))
    except Exception:
        http_read_timeout = 30.0

    try:
        req = Request(con_pool_size=http_pool_size, pool_timeout=http_pool_timeout, connect_timeout=http_connect_timeout, read_timeout=http_read_timeout)
        bot_instance = Bot(token=BOT_TOKEN, request=req)
        application = Application.builder().bot(bot_instance).build()
    except Exception:
        # Fallback to default behavior
        application = Application.builder().token(BOT_TOKEN).build()

    # Allow forcing polling even when WEBHOOK_URL is set (useful for local/dev runs)
    force_polling = os.environ.get("FORCE_POLLING", "").lower() in ("1", "true", "yes")

    # Initialize get_updates isolation primitives (semaphore + optional dedicated client)
    try:
        global GET_UPDATES_SEMAPHORE, GET_UPDATES_BOT
        try:
            GET_UPDATES_SEMAPHORE = asyncio.Semaphore(int(os.environ.get("GET_UPDATES_CONCURRENCY", "1")))
        except Exception:
            GET_UPDATES_SEMAPHORE = asyncio.Semaphore(1)
        GET_UPDATES_BOT = None
        if Request is not None:
            try:
                gu_pool_size = int(os.environ.get("GET_UPDATES_POOL_SIZE", "5"))
                gu_pool_timeout = float(os.environ.get("GET_UPDATES_POOL_TIMEOUT", str(http_pool_timeout)))
                gu_req = Request(con_pool_size=gu_pool_size, pool_timeout=gu_pool_timeout, connect_timeout=http_connect_timeout, read_timeout=http_read_timeout)
                GET_UPDATES_BOT = Bot(token=BOT_TOKEN, request=gu_req)
                logger.info("Dedicated get_updates client initialized (pool=%s)", gu_pool_size)
            except Exception as e:
                logger.warning("Failed to initialize dedicated get_updates client: %s", e)
                GET_UPDATES_BOT = None
    except Exception:
        GET_UPDATES_SEMAPHORE = asyncio.Semaphore(1)
        GET_UPDATES_BOT = None

    # If FORCE_POLLING is requested, remove any existing webhook immediately
    if force_polling:
        try:
            await application.bot.delete_webhook(drop_pending_updates=False)
            logger.info("FORCE_POLLING enabled at startup: deleted existing webhook")
        except Exception as e:
            logger.warning("Failed to delete webhook on startup for FORCE_POLLING: %s", e)

    # Expose external service config into application context
    try:
        application.bot_data["redis_url"] = getattr(cfg, "REDIS_URL", None)
        application.bot_data["ffmpeg_path"] = getattr(cfg, "FFMPEG_PATH", "ffmpeg")
    except Exception:
        pass

    # Expose application for ASGI metrics and introspection
    global BOT_APPLICATION, BOT_STARTED_AT
    BOT_APPLICATION = application
    BOT_STARTED_AT = time.time()

    # Expose ACL into application context
    try:
        application.bot_data["allowed_user_ids"] = ALLOWED_USER_IDS
        application.bot_data["admin_user_id"] = ADMIN_USER_ID
    except Exception:
        application.bot_data["allowed_user_ids"] = set()
        application.bot_data["admin_user_id"] = None

    # Initialize rate limiters
    api_limiter = TelegramAPIRateLimiter()
    # Set conversions_per_hour to 360 => ~1 conversion per 10 seconds
    # Use Redis-backed limiter when REDIS is configured so limits are enforced
    # across workers/processes. Otherwise fall back to in-memory limiter.
    try:
        if application.bot_data.get("redis_url"):
            conversion_limiter = ConversionRateLimiterRedis(conversions_per_hour=int(os.environ.get("CONVERSIONS_PER_HOUR", "360")))
        else:
            conversion_limiter = ConversionRateLimiter(conversions_per_hour=int(os.environ.get("CONVERSIONS_PER_HOUR", "360")))
    except Exception:
        conversion_limiter = ConversionRateLimiter(conversions_per_hour=int(os.environ.get("CONVERSIONS_PER_HOUR", "360")))

    # Attach rate limiters to application context
    application.bot_data["api_rate_limiter"] = api_limiter
    application.bot_data["conversion_rate_limiter"] = conversion_limiter

    # Optionally pre-initialize storage backend (fail-fast / diagnostics)
    try:
        if get_storage_backend is not None:
            try:
                storage_backend = await get_storage_backend()
                application.bot_data["storage_backend"] = storage_backend
                logger.info("Storage backend initialized: %s", (os.getenv("STORAGE_BACKEND") or getattr(cfg, "STORAGE_BACKEND", "local")))
            except Exception as e:
                logger.warning("Storage backend initialization failed: %s", e)
    except Exception:
        pass

    logger.info("Rate limiters initialized")
    logger.info(f"  - API limit: {TelegramAPIRateLimiter.GENERAL_LIMIT} calls/sec globally")
    logger.info(f"  - Per-user limit: {TelegramAPIRateLimiter.PER_USER_LIMIT} call/sec")
    logger.info(f"  - Conversion limit: {conversion_limiter.conversions_per_hour} conversions/hour per user")

    # Setup handlers
    setup_handlers(application)

    # Create directories (use configured storage paths when available)
    await ensure_directories(
        getattr(cfg, "STORAGE_PATH", "storage"),
        getattr(cfg, "INPUT_PATH", "storage/input"),
        getattr(cfg, "OUTPUT_PATH", "storage/output"),
        getattr(cfg, "TEMP_PATH", "storage/temp"),
        getattr(cfg, "THUMBNAIL_PATH", "storage/thumbnails"),
        "logs",
    )

    # Start cleanup manager
    try:
        asyncio.create_task(start_cleanup_task())
        logger.info("Cleanup manager started")
    except Exception as e:
        logger.error(f"Failed to start cleanup manager: {e}")

    # Check FFmpeg (binary) availability and ffmpeg-python binding; warn if missing
    try:
        available = await check_ffmpeg_available()
        if not available:
            logger.info("FFmpeg binary not found or not executable; falling back to CLI checks at runtime.")
        else:
            logger.info("FFmpeg binary is available")
    except Exception:
        logger.info("FFmpeg availability check failed; continuing")

    # Check ffmpeg-python binding availability (best-effort)
    try:
        import importlib

        importlib.import_module("ffmpeg")
        logger.info("ffmpeg-python (python binding) is available")
        # Reduce noisy logs from ffmpeg/ffmpeg-python internals where possible
        try:
            logging.getLogger("ffmpeg").setLevel(logging.ERROR)
            logging.getLogger("ffmpeg._core").setLevel(logging.ERROR)
        except Exception:
            pass
    except Exception:
        logger.info("ffmpeg-python not available; falling back to CLI ffmpeg calls")

    # Initialize webhook recovery manager if using webhooks (skip when forcing polling)
    webhook_manager = None
    if WEBHOOK_URL and not force_polling:
        webhook_manager = WebhookRecoveryManager(application, WEBHOOK_URL)
        try:
            await webhook_manager.start()
            logger.info("Webhook recovery manager started")
        except Exception as e:
            logger.error(f"Failed to start webhook recovery manager: {e}")

    # Setup graceful shutdown using signals (works on Unix and has Windows fallbacks)
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _request_shutdown(sig_name: str = None):
        logger.info(f"Shutdown requested via signal: {sig_name}")
        try:
            loop.call_soon_threadsafe(shutdown_event.set)
        except Exception:
            # last-resort: set result via asyncio.ensure_future
            asyncio.ensure_future(shutdown_event.set())

    try:
        loop.add_signal_handler(signal.SIGINT, lambda: _request_shutdown("SIGINT"))
        loop.add_signal_handler(signal.SIGTERM, lambda: _request_shutdown("SIGTERM"))
    except NotImplementedError:
        # Fallback for Windows or event loops that don't support add_signal_handler
        signal.signal(signal.SIGINT, lambda s, f: loop.call_soon_threadsafe(shutdown_event.set))
        try:
            signal.signal(signal.SIGTERM, lambda s, f: loop.call_soon_threadsafe(shutdown_event.set))
        except Exception:
            # SIGTERM may not be available on some platforms
            pass

    try:
        # Start the bot with PTB v20+ proper async context
        logger.info("Starting bot with PTB v20+...")

        # Background mode (used when running under ASGI/FastAPI):
        # initialize and start the Application but avoid using the
        # `async with application` context manager or blocking
        # `run_polling()` call which conflict with ASGI lifecycle.
        if background:
            await application.initialize()
            await application.start()
            try:
                BOT_READY.set()
            except Exception:
                pass

            # Auto-fallback: when running under ASGI the PTB dispatcher may
            # not be available in some hosting environments. If webhook mode
            # is configured but no dispatcher exists after start, enable the
            # FORCE_POLLING long-poller fallback so updates are still handled
            # via getUpdates. This prevents the bot from becoming unresponsive
            # when webhook dispatching can't be wired into the Application.
            try:
                if WEBHOOK_URL and not force_polling:
                    dispatcher = getattr(application, "dispatcher", None)
                    has_dispatcher_proc = bool(dispatcher and hasattr(dispatcher, "process_update"))
                    app_has_proc = hasattr(application, "process_update")
                    if not has_dispatcher_proc and not app_has_proc:
                        logger.warning("Dispatcher not available after Application.start(); enabling FORCE_POLLING fallback")
                        force_polling = True
            except Exception:
                logger.exception("Failed to evaluate dispatcher presence for FORCE_POLLING fallback")

            polling_task = None
            polling_task = None
            # Background ASGI mode: support either webhook mode or an opt-in
            # FORCE_POLLING long-poller (useful when running under ASGI but
            # developer wants getUpdates polling instead of webhooks).
            if WEBHOOK_URL and not force_polling:
                logger.info(f"🌐 Starting bot in webhook mode: {WEBHOOK_URL}")
                try:
                    await application.bot.set_webhook(
                        url=WEBHOOK_URL,
                        allowed_updates=["message", "callback_query", "edited_message"],
                        max_connections=100,
                        drop_pending_updates=False,
                        secret_token=WEBHOOK_SECRET or None,
                    )
                    logger.info(f"✅ Webhook set successfully: {WEBHOOK_URL}")
                except Exception as e:
                    logger.error(f"Failed to set webhook: {e}")
                    # let caller observe failure via exception
                    raise
            else:
                # FORCE_POLLING override: delete any existing webhook and
                # start a lightweight long-polling task that fetches updates
                # via getUpdates and enqueues them onto Application.update_queue
                if WEBHOOK_URL and force_polling:
                    try:
                        await application.bot.delete_webhook(drop_pending_updates=False)
                        logger.info("Deleted existing webhook to allow FORCE_POLLING long-poller")
                    except Exception as e:
                        logger.warning(f"Failed to delete webhook before long-poller: {e}")

                logger.info("Starting background long-poller (FORCE_POLLING enabled)")

                async def _longpoll_loop():
                    offset = None
                    bot = application.bot
                    while True:
                        try:
                            # Use a modest timeout so we can react to shutdown_event
                            sem = globals().get("GET_UPDATES_SEMAPHORE")
                            get_bot = globals().get("GET_UPDATES_BOT")
                            if sem is None:
                                sem = asyncio.Semaphore(1)
                            acquired = False
                            try:
                                await sem.acquire()
                                acquired = True
                                if get_bot:
                                    updates = await get_bot.get_updates(offset=offset, timeout=30)
                                else:
                                    updates = await bot.get_updates(offset=offset, timeout=30)
                            finally:
                                if acquired:
                                    try:
                                        sem.release()
                                    except Exception:
                                        pass
                            if updates:
                                for u in updates:
                                    try:
                                        if getattr(u, "update_id", None) is not None:
                                            offset = int(u.update_id) + 1
                                    except Exception:
                                        pass
                                    try:
                                        # Enqueue for ASGI consumer/dispatcher
                                        await BOT_APPLICATION.update_queue.put(u)
                                    except Exception:
                                        logger.exception("Failed to enqueue polled update")
                            else:
                                # no updates; brief pause before next long-poll
                                await asyncio.sleep(0.1)
                        except asyncio.CancelledError:
                            break
                        except (TimedOut, httpx.PoolTimeout) as e:
                            logger.warning("Long-poller timed out (pool exhausted): %s. Backing off 5s", e)
                            await asyncio.sleep(5)
                        except Conflict as e:
                            logger.error("Long-poller conflict (another getUpdates active): %s. Stopping long-poller", e)
                            break
                        except Exception as e:
                            logger.exception(f"Long-poller error: {e}")
                            await asyncio.sleep(1)

                try:
                    global LONG_POLLER_STARTED
                    if not globals().get("LONG_POLLER_STARTED", False):
                        globals()["LONG_POLLER_STARTED"] = True
                        polling_task = asyncio.create_task(_longpoll_loop())
                    else:
                        logger.info("Background long-poller already running; skipping duplicate start")
                except Exception:
                    logger.exception("Failed to start background long-poller")

            # Wait for shutdown_event or cancellation; FastAPI will cancel
            # this task on shutdown which will raise CancelledError here.
            try:
                await shutdown_event.wait()
                logger.info("Shutdown event received, stopping bot...")
            except asyncio.CancelledError:
                logger.info("Background bot task cancelled; stopping application")
            finally:
                if WEBHOOK_URL:
                    try:
                        await application.bot.delete_webhook(drop_pending_updates=False)
                        logger.info("Webhook deleted on shutdown")
                    except Exception as e:
                        logger.warning(f"Failed to delete webhook on shutdown: {e}")

                try:
                    stop_cleanup_task()
                    logger.info("Cleanup manager stop requested")
                except Exception as e:
                    logger.error(f"Error stopping cleanup manager: {e}")

                if polling_task:
                    try:
                        polling_task.cancel()
                        await polling_task
                    except Exception:
                        pass
                    finally:
                        try:
                            globals()["LONG_POLLER_STARTED"] = False
                        except Exception:
                            pass

                try:
                    await application.stop()
                finally:
                    try:
                        BOT_READY.clear()
                    except Exception:
                        pass
                # Close dedicated get_updates client if present
                try:
                    gu = globals().get("GET_UPDATES_BOT")
                    if gu is not None:
                        close_fn = getattr(gu, "close", None)
                        if close_fn:
                            try:
                                await close_fn()
                            except Exception:
                                pass
                except Exception:
                    pass

        else:
            # Non-ASGI mode: use the context manager as before which manages
            # the application's lifecycle (initialize/start/stop) and blocks
            # on polling or webhook mode until shutdown.
            async with application:
                await application.initialize()
                await application.start()
                try:
                    BOT_READY.set()
                except Exception:
                    pass

                if WEBHOOK_URL and not force_polling:
                    logger.info(f"🌐 Starting bot in webhook mode: {WEBHOOK_URL}")
                    try:
                        await application.bot.set_webhook(
                            url=WEBHOOK_URL,
                            allowed_updates=["message", "callback_query", "edited_message"],
                            max_connections=100,
                            drop_pending_updates=False,
                            secret_token=WEBHOOK_SECRET or None,
                        )
                        logger.info(f"✅ Webhook set successfully: {WEBHOOK_URL}")
                    except Exception as e:
                        logger.error(f"Failed to set webhook: {e}")
                        raise

                else:
                    # Either no WEBHOOK_URL configured, or FORCE_POLLING is enabled.
                    if WEBHOOK_URL and force_polling:
                        try:
                            await application.bot.delete_webhook(drop_pending_updates=False)
                            logger.info("Deleted existing webhook to allow polling (FORCE_POLLING enabled)")
                        except Exception as e:
                            logger.warning(f"Failed to delete existing webhook before polling: {e}")

                    logger.info("🚀 Starting bot in polling mode")
                    await application.run_polling(
                        allowed_updates=["message", "callback_query", "edited_message"], drop_pending_updates=False
                    )

    except KeyboardInterrupt:
        logger.info("⌨️  Bot interrupted by user (Ctrl+C)")
    except asyncio.CancelledError:
        logger.info("🔄 Bot cancellation requested")
    except Exception as e:
        logger.error(f"❌ Error starting bot: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application terminated")
    except Exception as e:
        logger.error(f"Fatal error: {e}")

# FastAPI app for webhook handling - PTB v20+ compatible
try:
    from fastapi import FastAPI, HTTPException, Request
    from fastapi.responses import Response, FileResponse
    from telegram import Update as TgUpdate

    app = FastAPI(title="Media Conversion Bot - PTB v20+")

    # Mount legacy Flask-based web UI (if present) under '/flask' so the
    # web uploader and static UI remain available when running under ASGI/uvicorn.
    try:
        from starlette.middleware.wsgi import WSGIMiddleware
        from fastapi.responses import RedirectResponse

        import web.webapp as flask_webapp

        # Mount the Flask app at /flask (flask routes like /upload become /flask/upload)
        app.mount("/flask", WSGIMiddleware(flask_webapp.app))

        # Provide a compatibility redirect so requests to /upload still work.
        @app.get("/upload")
        async def _upload_redirect():
            return RedirectResponse(url="/flask/upload")
        logger.info("Mounted Flask web UI at /flask and redirect /upload -> /flask/upload")

        # Expose a couple of convenient root-level endpoints that mirror the
        # Flask web UI's status and internal diag routes so external callers
        # don't need to include the /flask prefix. These are best-effort and
        # only created when the Flask app was successfully imported.
        try:
            import re
            import traceback

            @app.get("/status/{job_id}")
            async def root_status(job_id: str):
                        try:
                            job_hash = None

                            # 1) Try Flask helper if available
                            try:
                                if getattr(flask_webapp, "aioredis_available", False):
                                    try:
                                        job_hash = await flask_webapp._get_job_hash(job_id)
                                    except Exception:
                                        job_hash = None
                            except Exception:
                                job_hash = None

                            # 2) Fallback: try to read directly from Redis using job_queue.get_redis
                            if not job_hash:
                                try:
                                    from utils.job_queue import get_redis

                                    r = await get_redis()
                                    try:
                                        raw = await r.hgetall(f"ffmpeg:job:{job_id}")
                                    finally:
                                        try:
                                            await r.close()
                                        except Exception:
                                            pass
                                    if raw:
                                        # aioredis returns a dict possibly with bytes; decode keys/values
                                        decoded = {}
                                        for k, v in raw.items():
                                            key = k.decode() if isinstance(k, bytes) else k
                                            val = v.decode() if isinstance(v, bytes) else v
                                            decoded[key] = val
                                        job_hash = decoded
                                except Exception:
                                    job_hash = None

                            # 3) If we have a job hash, normalize and return
                            if job_hash:
                                progress = float(job_hash.get("progress") or 0.0)
                                message = job_hash.get("message") or "queued"
                                status = job_hash.get("status") or ("done" if job_hash.get("output") else "processing")
                                out = job_hash.get("output")
                                resp = {"job_id": job_id, "progress": progress, "message": message, "status": status, "output": out}
                                try:
                                    if job_hash.get("out_bytes") is not None:
                                        resp["out_bytes"] = int(job_hash.get("out_bytes"))
                                except Exception:
                                    pass
                                try:
                                    if job_hash.get("in_bytes") is not None:
                                        resp["in_bytes"] = int(job_hash.get("in_bytes"))
                                except Exception:
                                    pass
                                try:
                                    if job_hash.get("progress_by_size") is not None:
                                        resp["progress_by_size"] = float(job_hash.get("progress_by_size"))
                                except Exception:
                                    pass
                                return resp

                            # 4) Fallback to the in-memory JOB_STORE from the Flask app
                            try:
                                local = flask_webapp.JOB_STORE.get(job_id)
                            except Exception:
                                local = None
                            if local:
                                return {
                                    "job_id": job_id,
                                    "progress": float(local.get("progress", 0.0)),
                                    "message": local.get("message", "processing" if local.get("status") != "done" else "done"),
                                    "status": local.get("status", "processing"),
                                    "output": local.get("output"),
                                }

                            # 5) Check for a stored output file in Flask's OUTPUT_DIR if available
                            try:
                                out_dir = getattr(flask_webapp, "OUTPUT_DIR", None)
                            except Exception:
                                out_dir = None
                            if out_dir:
                                out_path = os.path.join(out_dir, f"{job_id}.mp4")
                                if os.path.exists(out_path):
                                    return {"job_id": job_id, "progress": 100.0, "message": "done", "status": "done", "output": out_path}

                            # 6) Default queued response
                            return {"job_id": job_id, "progress": 0.0, "message": "queued", "status": "queued"}
                        except Exception as e:
                            return {"error": str(e)}

            @app.get("/internal/diag")
            async def root_diag(request: Request, job_id: str | None = None, token: str | None = None):
                    # Token validation mirrors the Flask endpoint behavior
                    DIAG_TOKEN = os.environ.get("DIAG_TOKEN")
                    incoming = request.headers.get("X-DIAG-TOKEN") or token
                    if not DIAG_TOKEN:
                        raise HTTPException(status_code=403, detail="DIAG_TOKEN not configured on server")
                    if incoming != DIAG_TOKEN:
                        raise HTTPException(status_code=401, detail="unauthorized")

                    result = {"env": {}, "redis": {}, "logs": {}, "ps": None}
                    # Minimal masked env snapshot
                    for k in ("REDIS_URL", "WEB_UPLOAD_URL", "UPLOAD_SECRET", "S3_BUCKET", "AWS_ACCESS_KEY_ID"):
                        v = os.environ.get(k)
                        if k == "REDIS_URL" and v:
                            result["env"][k] = re.sub(r"(redis://[^:]*:)[^@]+@", r"\1****@", v)
                        elif k == "UPLOAD_SECRET":
                            result["env"][k] = "****" if v else None
                        else:
                            result["env"][k] = v

                    # Redis diagnostics (best-effort) - try async first, then sync fallback
                    try:
                        red_url = os.environ.get("REDIS_URL")
                        if red_url:
                            # try async redis helper
                            try:
                                from utils.job_queue import get_redis

                                r = await get_redis()
                                try:
                                        result["redis"]["ping"] = await r.ping()
                                        try:
                                            result["redis"]["ffmpeg_jobs"] = await r.lrange("ffmpeg:jobs", 0, 50)
                                        except Exception:
                                            result["redis"]["ffmpeg_jobs"] = []
                                        try:
                                            keys = await r.keys("ffmpeg:job:*")
                                            result["redis"]["job_keys_count"] = len(keys)
                                            result["redis"]["job_keys_sample"] = keys[:50]
                                        except Exception:
                                            result["redis"]["job_keys_count"] = 0
                                        if job_id:
                                            try:
                                                result["redis"]["job_hash"] = await r.hgetall(f"ffmpeg:job:{job_id}")
                                            except Exception:
                                                result["redis"]["job_hash"] = {}
                                finally:
                                    try:
                                        await r.close()
                                    except Exception:
                                        pass
                            except Exception:
                                # sync fallback
                                try:
                                    r2 = flask_webapp.redis_sync.from_url(red_url, decode_responses=True)
                                    result["redis"]["ping"] = r2.ping()
                                    try:
                                        result["redis"]["ffmpeg_jobs"] = r2.lrange("ffmpeg:jobs", 0, 50)
                                    except Exception:
                                        result["redis"]["ffmpeg_jobs"] = []
                                    try:
                                        keys = r2.keys("ffmpeg:job:*")
                                        result["redis"]["job_keys_count"] = len(keys)
                                        result["redis"]["job_keys_sample"] = keys[:50]
                                    except Exception:
                                        result["redis"]["job_keys_count"] = 0
                                    if job_id:
                                        try:
                                            result["redis"]["job_hash"] = r2.hgetall(f"ffmpeg:job:{job_id}")
                                        except Exception:
                                            result["redis"]["job_hash"] = {}
                                except Exception as e:
                                    result["redis"]["error"] = str(e)
                        else:
                            result["redis"]["error"] = "REDIS_URL not set"
                    except Exception as e:
                        result["redis"]["error"] = str(e)

                    # Tail project logs
                    try:
                        logs_dir = os.path.join(os.getcwd(), "logs")
                        if os.path.isdir(logs_dir):
                            for fname in sorted(os.listdir(logs_dir))[-10:]:
                                path = os.path.join(logs_dir, fname)
                                if os.path.isfile(path):
                                    with open(path, "r", encoding="utf-8", errors="replace") as fh:
                                        lines = fh.readlines()[-500:]
                                        result["logs"][fname] = "".join(lines)
                        # Also include worker log if present for quick debugging
                        try:
                            worker_log = "/tmp/worker.log"
                            if os.path.isfile(worker_log):
                                with open(worker_log, "r", encoding="utf-8", errors="replace") as fh:
                                    lines = fh.readlines()[-1000:]
                                    result["logs"]["worker.log"] = "".join(lines)
                        except Exception:
                            pass
                    except Exception:
                        result["logs"]["error"] = traceback.format_exc()

                    # Basic process list snapshot
                    try:
                        ps_out = subprocess.check_output(["ps", "aux"], stderr=subprocess.STDOUT, text=True)
                        result["ps"] = "\n".join(ps_out.splitlines()[:200])
                    except Exception:
                        result["ps"] = None

                    return result

            @app.post("/internal/diag/run")
            async def run_diag_action(request: Request):
                """Execute limited diagnostics actions (ffprobe/remux/reencode/tail_logs/job_info).

                Protected by `DIAG_TOKEN` header (X-DIAG-TOKEN). Intended for short-lived diagnostics
                on the running Render instance; commands have timeouts to avoid long blocking.
                """
                DIAG_TOKEN = os.environ.get("DIAG_TOKEN")
                incoming = request.headers.get("X-DIAG-TOKEN")
                if not DIAG_TOKEN:
                    raise HTTPException(status_code=403, detail="DIAG_TOKEN not configured on server")
                if incoming != DIAG_TOKEN:
                    raise HTTPException(status_code=401, detail="unauthorized")

                try:
                    payload = await request.json()
                except Exception:
                    raise HTTPException(status_code=400, detail="invalid json")

                action = payload.get("action")
                filename = payload.get("file")
                job_id = payload.get("job_id")
                out = {"action": action}

                # Resolve ffmpeg/ffprobe paths
                ffprobe = getattr(cfg, "FFPROBE_PATH", None) or (FFMPEG_PATH.replace("ffmpeg", "ffprobe") if "ffmpeg" in FFMPEG_PATH else "ffprobe")
                ffmpeg = FFMPEG_PATH

                # Helper to run commands without blocking the ASGI loop
                async def _run(cmd, timeout=600):
                    try:
                        proc = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, timeout=timeout)
                        return {"returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
                    except Exception as e:
                        return {"error": str(e)}

                # Sanitize file -> only allow basename under storage/input
                input_dir = getattr(cfg, "INPUT_PATH", os.path.join(os.getcwd(), "storage", "input"))
                if filename:
                    safe_name = os.path.basename(filename)
                    target_path = os.path.join(input_dir, safe_name)
                    if not os.path.isfile(target_path):
                        raise HTTPException(status_code=404, detail=f"file not found: {safe_name}")

                if action == "ffprobe":
                    cmd = [ffprobe, "-v", "error", "-print_format", "json", "-show_format", "-show_streams", target_path]
                    out["result"] = await _run(cmd, timeout=60)
                    return out

                if action == "remux":
                    dst = payload.get("out") or os.path.join(os.getcwd(), "storage", "temp", os.path.splitext(os.path.basename(target_path))[0] + ".mkv")
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    cmd = [ffmpeg, "-y", "-hide_banner", "-loglevel", "error", "-i", target_path, "-c", "copy", dst]
                    out["dst"] = dst
                    out["result"] = await _run(cmd, timeout=600)
                    return out

                if action == "reencode":
                    dst = payload.get("out") or os.path.join(os.getcwd(), "storage", "output", os.path.splitext(os.path.basename(target_path))[0] + "_reencoded.mp4")
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    cmd = [ffmpeg, "-y", "-hide_banner", "-loglevel", "error", "-fflags", "+genpts", "-i", target_path, "-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac", "-b:a", "128k", dst]
                    out["dst"] = dst
                    out["result"] = await _run(cmd, timeout=1800)
                    return out

                if action == "tail_logs":
                    try:
                        lines = int(payload.get("lines", 200))
                    except Exception:
                        lines = 200
                    logs = {}
                    logs_dir = os.path.join(os.getcwd(), "logs")
                    try:
                        if os.path.isdir(logs_dir):
                            for fname in sorted(os.listdir(logs_dir))[-10:]:
                                path = os.path.join(logs_dir, fname)
                                if os.path.isfile(path):
                                    with open(path, "r", encoding="utf-8", errors="replace") as fh:
                                        logs[fname] = "".join(fh.readlines()[-lines:])
                        worker_log = "/tmp/worker.log"
                        if os.path.isfile(worker_log):
                            with open(worker_log, "r", encoding="utf-8", errors="replace") as fh:
                                logs[os.path.basename(worker_log)] = "".join(fh.readlines()[-(lines * 5):])
                    except Exception as e:
                        raise HTTPException(status_code=500, detail=str(e))
                    out["logs"] = logs
                    return out

                # Probe the local webhook loopback to help diagnose webhook timeouts
                if action == "probe_local_webhook":
                    try:
                        parsed = urlparse(WEBHOOK_URL or "")
                        local_port = int(os.environ.get("PORT", "10000"))
                        local_path = parsed.path or "/"
                        local_url = f"http://127.0.0.1:{local_port}{local_path}"
                        async with aiohttp.ClientSession() as session:
                            try:
                                async with session.head(local_url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                                    out["local_url"] = local_url
                                    out["status"] = resp.status
                                    out["headers"] = dict(resp.headers)
                            except Exception as e:
                                out["error"] = str(e)
                    except Exception as e:
                        out["error"] = str(e)
                    return out

                # New admin/diagnostic actions: inspect and clear locks, remove queued job instances
                if action == "inspect_locks":
                    try:
                        from utils.job_queue import get_redis

                        r = await get_redis()
                        try:
                            keys = await r.keys("ffmpeg:lock:*")
                            locks = {}
                            for k in keys:
                                kstr = k.decode() if isinstance(k, bytes) else k
                                try:
                                    v = await r.get(k)
                                    vstr = v.decode() if isinstance(v, bytes) else v
                                except Exception:
                                    vstr = None
                                locks[kstr] = vstr
                        finally:
                            try:
                                await r.close()
                            except Exception:
                                pass
                        out["locks"] = locks
                    except Exception as e:
                        out["locks_error"] = str(e)
                    return out

                if action == "clear_lock":
                    # Accept either job_id or input path
                    jid = payload.get("job_id") or job_id
                    input_path_provided = payload.get("input") or payload.get("file")
                    removed = []
                    try:
                        from utils.job_queue import get_redis

                        r = await get_redis()
                        try:
                            # If job_id provided, remove any lock keys whose value == job_id
                            if jid:
                                keys = await r.keys("ffmpeg:lock:*")
                                for k in keys:
                                    try:
                                        v = await r.get(k)
                                        vstr = v.decode() if isinstance(v, bytes) else v
                                    except Exception:
                                        vstr = None
                                    if vstr and str(vstr) == str(jid):
                                        kstr = k.decode() if isinstance(k, bytes) else k
                                        try:
                                            await r.delete(k)
                                            removed.append(kstr)
                                        except Exception:
                                            pass

                            # If input path provided, compute expected lock key and remove it
                            if input_path_provided:
                                try:
                                    lock_hash = hashlib.sha256(input_path_provided.encode()).hexdigest()
                                    lock_key = f"ffmpeg:lock:{lock_hash}"
                                    try:
                                        await r.delete(lock_key)
                                        removed.append(lock_key)
                                    except Exception:
                                        pass
                                except Exception:
                                    pass
                        finally:
                            try:
                                await r.close()
                            except Exception:
                                pass
                        out["removed"] = removed
                        out["removed_count"] = len(removed)
                    except Exception as e:
                        out["error"] = str(e)
                    return out

                if action == "remove_job_instances":
                    # Remove queued job list entries that match job_id
                    jid = payload.get("job_id") or job_id
                    if not jid:
                        raise HTTPException(status_code=400, detail="job_id required")
                    removed = 0
                    try:
                        from utils.job_queue import get_redis, JOB_LIST

                        r = await get_redis()
                        try:
                            items = await r.lrange(JOB_LIST, 0, -1)
                            for it in items:
                                raw = it.decode() if isinstance(it, bytes) else it
                                try:
                                    j = json.loads(raw)
                                except Exception:
                                    continue
                                if str(j.get("job_id")) == str(jid):
                                    try:
                                        await r.lrem(JOB_LIST, 0, raw)
                                        removed += 1
                                    except Exception:
                                        pass
                        finally:
                            try:
                                await r.close()
                            except Exception:
                                pass
                        out["removed_job_instances"] = removed
                    except Exception as e:
                        out["error"] = str(e)
                    return out

                if action == "job_info":
                    if not job_id:
                        raise HTTPException(status_code=400, detail="job_id required for job_info")
                    try:
                        from utils.job_queue import get_redis

                        r = await get_redis()
                        try:
                            job_hash = await r.hgetall(f"ffmpeg:job:{job_id}")
                            out["job_hash"] = job_hash
                        finally:
                            try:
                                await r.close()
                            except Exception:
                                pass
                        return out
                    except Exception as e:
                        raise HTTPException(status_code=500, detail=str(e))

                if action == "cancel_job":
                    # Set the cancel flag for a job so running ffmpeg will terminate
                    if not job_id:
                        raise HTTPException(status_code=400, detail="job_id required for cancel_job")
                    try:
                        await cancel_job(job_id)
                        out["cancelled"] = True
                    except Exception as e:
                        out["error"] = str(e)
                    return out

                raise HTTPException(status_code=400, detail="unknown action")

            @app.get("/get_input")
            async def root_get_input(request: Request, name: str | None = None):
                """Serve input files from the server for short-term debugging.
                Protection: require `DIAG_TOKEN` or fallback to `UPLOAD_SECRET`.
                """
                DIAG_TOKEN = os.environ.get("DIAG_TOKEN")
                UPLOAD_SECRET = os.environ.get("UPLOAD_SECRET")
                incoming_diag = request.headers.get("X-DIAG-TOKEN") or request.query_params.get("token")
                incoming_upload = request.headers.get("X-Upload-Token") or request.query_params.get("upload_token")

                if DIAG_TOKEN:
                    if incoming_diag != DIAG_TOKEN:
                        raise HTTPException(status_code=401, detail="unauthorized")
                else:
                    if not UPLOAD_SECRET or incoming_upload != UPLOAD_SECRET:
                        raise HTTPException(status_code=401, detail="unauthorized (no DIAG_TOKEN configured)")

                if not name:
                    raise HTTPException(status_code=400, detail="name required")

                # sanitize
                if ".." in name or name.startswith("/"):
                    raise HTTPException(status_code=400, detail="invalid filename")

                try:
                    input_dir = getattr(cfg, "INPUT_PATH", os.path.join(os.getcwd(), "storage", "input"))
                except Exception:
                    input_dir = os.path.join(os.getcwd(), "storage", "input")

                safe_name = os.path.basename(name)
                path = os.path.join(input_dir, safe_name)
                if not os.path.exists(path) or not os.path.isfile(path):
                    raise HTTPException(status_code=404, detail="not found")

                return FileResponse(path, filename=safe_name)

            @app.get("/get_output")
            async def root_get_output(request: Request, name: str | None = None):
                """Serve output files from the server for short-term debugging.
                Protection: require `DIAG_TOKEN` or fallback to `UPLOAD_SECRET`.
                """
                DIAG_TOKEN = os.environ.get("DIAG_TOKEN")
                UPLOAD_SECRET = os.environ.get("UPLOAD_SECRET")
                incoming_diag = request.headers.get("X-DIAG-TOKEN") or request.query_params.get("token")
                incoming_upload = request.headers.get("X-Upload-Token") or request.query_params.get("upload_token")

                if DIAG_TOKEN:
                    if incoming_diag != DIAG_TOKEN:
                        raise HTTPException(status_code=401, detail="unauthorized")
                else:
                    if not UPLOAD_SECRET or incoming_upload != UPLOAD_SECRET:
                        raise HTTPException(status_code=401, detail="unauthorized (no DIAG_TOKEN configured)")

                if not name:
                    raise HTTPException(status_code=400, detail="name required")

                # sanitize
                if ".." in name or name.startswith("/"):
                    raise HTTPException(status_code=400, detail="invalid filename")

                try:
                    output_dir = getattr(cfg, "OUTPUT_PATH", os.path.join(os.getcwd(), "storage", "output"))
                except Exception:
                    output_dir = os.path.join(os.getcwd(), "storage", "output")

                safe_name = os.path.basename(name)
                path = os.path.join(output_dir, safe_name)
                if not os.path.exists(path) or not os.path.isfile(path):
                    raise HTTPException(status_code=404, detail="not found")

                return FileResponse(path, filename=safe_name)

        except Exception as _e:
            logger.warning(f"Could not create root convenience endpoints: {_e}")
    except Exception as e:
        logger.warning(f"Could not mount Flask web UI: {e}")

    @app.get("/health")
    async def health():
        dispatcher_ready = False
        try:
            dispatcher = getattr(BOT_APPLICATION, "dispatcher", None)
            dispatcher_ready = bool(dispatcher and hasattr(dispatcher, "process_update"))
        except Exception:
            dispatcher_ready = False

        return {
            "status": "ok",
            "bot_initialized": BOT_APPLICATION is not None,
            "bot_ready": BOT_READY.is_set(),
            "dispatcher_ready": dispatcher_ready,
            "startup_time": BOT_STARTED_AT,
            "error": getattr(app.state, "startup_error", None),
        }


    @app.get("/")
    async def root_index():
        """Root endpoint for platform health checks."""
        try:
            return {"status": "ok", "bot_ready": bool(BOT_READY.is_set())}
        except Exception:
            return {"status": "ok"}


    @app.head("/telegram/webhook")
    async def telegram_webhook_head(request: Request):
        """Lightweight HEAD handler for webhook probes to help health checks."""
        # Quick 200 response for monitoring probes
        return Response(status_code=200)

    @app.post("/telegram/webhook")
    async def telegram_webhook(request: Request):
        """PTB v20+ compatible webhook endpoint."""
        # Verify secret token header if configured
        try:
            if WEBHOOK_SECRET:
                incoming = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
                if not incoming or incoming != WEBHOOK_SECRET:
                    logger.warning("Invalid webhook secret token: %s", incoming)
                    raise HTTPException(status_code=403, detail="Invalid secret token")
        except HTTPException:
            raise
        except Exception:
            logger.exception("Error validating webhook secret token")
            raise HTTPException(status_code=500, detail="Webhook validation error")
        if not BOT_APPLICATION:
            logger.error("Bot application not initialized")
            raise HTTPException(status_code=503, detail="Bot not initialized")

        try:
            data = await request.json()
            logger.debug(f"Received webhook data: {data}")
        except Exception as e:
            logger.error(f"Invalid JSON in webhook: {e}")
            raise HTTPException(status_code=400, detail="Invalid JSON")

        try:
            # Build Update object early so we can retry dispatching even if the
            # application is still initializing.
            update = TgUpdate.de_json(data, BOT_APPLICATION.bot)
            if not update:
                raise ValueError("Failed to create Update object")
            logger.info(f"Received update {getattr(update, 'update_id', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to construct Update: {e}")
            raise HTTPException(status_code=400, detail="Invalid update payload")

        # Increment webhook counter
        try:
            with METRICS_LOCK:
                METRICS["webhooks_received"] += 1
        except Exception:
            pass

        # Helper: background retry dispatcher
        async def _background_retry_dispatch(u, attempts=12, delay=0.5):
            disp = getattr(BOT_APPLICATION, "dispatcher", None)
            for i in range(attempts):
                try:
                    disp = getattr(BOT_APPLICATION, "dispatcher", None)
                    if disp and hasattr(disp, "process_update"):
                        with METRICS_LOCK:
                            METRICS["dispatch_attempts"] += 1
                        await disp.process_update(u)
                        with METRICS_LOCK:
                            METRICS["updates_dispatched"] += 1
                        logger.info(
                            f"Background dispatched update {getattr(u, 'update_id', 'unknown')} on attempt {i+1}"
                        )
                        return True
                except Exception as e:
                    logger.debug(f"Background dispatch attempt {i+1} failed: {e}")
                    with METRICS_LOCK:
                        METRICS["dispatch_failures"] += 1
                await asyncio.sleep(delay)
            logger.error(f"Background dispatch exhausted for update {getattr(u, 'update_id', 'unknown')}")
            return False

        # Try immediate dispatch by scheduling a background dispatch task
        attempts = 6
        for i in range(attempts):
            dispatcher = getattr(BOT_APPLICATION, "dispatcher", None)
            if dispatcher and hasattr(dispatcher, "process_update"):
                try:
                    # Schedule non-blocking dispatch so webhook returns quickly
                    asyncio.create_task(_dispatch_update_task(update))
                    logger.info(
                        "Scheduled background dispatch task for update %s on attempt %s",
                        getattr(update, "update_id", "unknown"),
                        i + 1,
                    )
                    return {"ok": True, "update_id": getattr(update, "update_id", None), "dispatched": True}
                except Exception as e:
                    logger.warning(f"Failed to schedule dispatch task (attempt {i+1}): {e}")
                    try:
                        with METRICS_LOCK:
                            METRICS["dispatch_failures"] += 1
                    except Exception:
                        pass
            await asyncio.sleep(0.25)

        # Immediate dispatch not successful — try to enqueue
        try:
            await BOT_APPLICATION.update_queue.put(update)
            with METRICS_LOCK:
                METRICS["updates_queued"] += 1
            logger.info(f"Queued update {getattr(update, 'update_id', 'unknown')} after immediate attempts")
            return {"ok": True, "update_id": getattr(update, "update_id", None), "queued": True}
        except Exception as enqueue_exc:
            logger.warning(f"Enqueue failed: {enqueue_exc}; scheduling background retry and returning 200")
            # Schedule background retry but return 200 immediately (retry-accept policy)
            try:
                asyncio.create_task(_background_retry_dispatch(update))
            except Exception as e:
                logger.error(f"Failed to schedule background retry: {e}")
            return {"ok": True, "update_id": getattr(update, "update_id", None), "accepted": True}

    @app.get("/metrics")
    async def metrics():
        """Return Prometheus-style metrics as plain text."""
        uptime = time.time() - (BOT_STARTED_AT or START_TIME)
        allowed_total = len(ALLOWED_USER_IDS) if ALLOWED_USER_IDS else 0
        try:
            ffmpeg_ok = 1 if await check_ffmpeg_available() else 0
        except Exception:
            ffmpeg_ok = 0
        active_convs = 0
        try:
            if BOT_APPLICATION and BOT_APPLICATION.bot_data:
                mgr = BOT_APPLICATION.bot_data.get("handler_manager")
                if mgr:
                    active_convs = len(mgr.get_active_conversions())
        except Exception:
            active_convs = 0

        lines = [
            "# HELP media_bot_uptime_seconds Uptime seconds",
            "# TYPE media_bot_uptime_seconds gauge",
            f"media_bot_uptime_seconds {uptime}",
            "# HELP media_bot_allowed_users_total Total allowed users (ACL)",
            f"media_bot_allowed_users_total {allowed_total}",
            "# HELP media_bot_ffmpeg_available Whether ffmpeg is available (1/0)",
            f"media_bot_ffmpeg_available {ffmpeg_ok}",
            "# HELP media_bot_active_conversions Number of active conversions",
            f"media_bot_active_conversions {active_convs}",
            "# HELP media_bot_webhooks_received Total webhooks received",
            f"media_bot_webhooks_received {METRICS.get('webhooks_received', 0)}",
            "# HELP media_bot_updates_dispatched Total updates dispatched by dispatcher",
            f"media_bot_updates_dispatched {METRICS.get('updates_dispatched', 0)}",
            "# HELP media_bot_updates_queued Total updates queued to application",
            f"media_bot_updates_queued {METRICS.get('updates_queued', 0)}",
            "# HELP media_bot_dispatch_failures Total dispatch failures",
            f"media_bot_dispatch_failures {METRICS.get('dispatch_failures', 0)}",
            "# HELP media_bot_dispatch_attempts Total dispatch attempts",
            f"media_bot_dispatch_attempts {METRICS.get('dispatch_attempts', 0)}",
        ]

        return Response("\n".join(lines), media_type="text/plain; version=0.0.4")

    @app.get("/debug")
    async def debug_info():
        """Return debug information: startup error, dispatcher status, bot_data keys."""
        info = {
            "bot_initialized": BOT_APPLICATION is not None,
            "bot_ready": BOT_READY.is_set(),
            "startup_error": getattr(app.state, "startup_error", None),
            "bot_started_at": BOT_STARTED_AT,
        }

        try:
            info["bot_data_keys"] = (
                list(BOT_APPLICATION.bot_data.keys()) if BOT_APPLICATION and BOT_APPLICATION.bot_data else []
            )
        except Exception:
            info["bot_data_keys"] = None

        try:
            dispatcher = getattr(BOT_APPLICATION, "dispatcher", None)
            info["dispatcher_available"] = bool(dispatcher)
            info["dispatcher_has_process_update"] = bool(dispatcher and hasattr(dispatcher, "process_update"))
        except Exception:
            info["dispatcher_available"] = False
            info["dispatcher_has_process_update"] = False

        try:
            mgr = (
                BOT_APPLICATION.bot_data.get("handler_manager")
                if BOT_APPLICATION and BOT_APPLICATION.bot_data
                else None
            )
            info["active_conversions"] = len(mgr.get_active_conversions()) if mgr else 0
        except Exception:
            info["active_conversions"] = None

        # Telethon / userbot readiness diagnostics
        try:
            import importlib

            telethon_installed = False
            telethon_version = None
            telethon_import_error = None
            try:
                tmod = importlib.import_module("telethon")
                telethon_installed = True
                telethon_version = getattr(tmod, "__version__", None)
            except Exception as e:
                telethon_import_error = str(e)

            info.update(
                {
                    "telethon_installed": telethon_installed,
                    "telethon_version": telethon_version,
                    "telethon_import_error": telethon_import_error,
                    "enable_userbot_env": os.environ.get("ENABLE_USERBOT"),
                    "telethon_api_id_present": bool(os.environ.get("API_ID") or os.environ.get("USERBOT_API_ID")),
                    "telethon_api_hash_present": bool(os.environ.get("API_HASH") or os.environ.get("USERBOT_API_HASH")),
                    "telethon_session_present": bool(
                        os.environ.get("API_SESSION") or os.environ.get("TELETHON_SESSION") or os.environ.get("USERBOT_SESSION")
                    ),
                }
            )
        except Exception:
            pass

        return info

    @app.on_event("startup")
    async def _start_bot_background():
        # Launch main() as a background task so uvicorn also serves ASGI endpoints
        try:
            task = asyncio.create_task(main(background=True))
            app.state.bot_task = task

            def _on_done(t: asyncio.Task):
                try:
                    exc = t.exception()
                    if exc:
                        app.state.startup_error = repr(exc)
                        logger.error(f"Bot background task failed: {exc}")
                except asyncio.CancelledError:
                    pass

            task.add_done_callback(_on_done)

            logger.info("Background bot task started via ASGI startup event")

            # Start a lightweight update consumer only after the bot is ready.
            async def _update_consumer():
                await BOT_READY.wait()
                logger.info("Starting ASGI update consumer task")
                app.state.update_consumer_running = True
                try:
                    while True:
                        try:
                            update = await BOT_APPLICATION.update_queue.get()
                        except Exception:
                            await asyncio.sleep(0.1)
                            continue
                        try:
                            # Schedule dispatch in background so the consumer loop never
                            # blocks waiting for handler completion. The helper
                            # `_dispatch_update_task` updates metrics and logs errors.
                            try:
                                asyncio.create_task(_dispatch_update_task(update))
                            except Exception:
                                logger.exception("Failed to schedule dispatch task for update")
                        except Exception:
                            logger.exception("Unhandled error while scheduling dispatch task")
                except asyncio.CancelledError:
                    logger.info("ASGI update consumer task cancelled")
                finally:
                    app.state.update_consumer_running = False

            try:
                app.state.update_consumer = asyncio.create_task(_update_consumer())
            except Exception:
                logger.exception("Failed to start ASGI update consumer task")
            # If dispatcher isn't available (some hosting variants), start a
            # fallback long-poller that uses getUpdates and enqueues updates
            # onto the Application.update_queue so handlers still run.
            try:
                force_polling_env = os.environ.get("FORCE_POLLING", "").lower() in ("1", "true", "yes")
                dispatcher = getattr(BOT_APPLICATION, "dispatcher", None)
                has_dispatcher_proc = bool(dispatcher and hasattr(dispatcher, "process_update"))
                app_has_proc = hasattr(BOT_APPLICATION, "process_update")
                if force_polling_env or (not has_dispatcher_proc and not app_has_proc):
                    logger.warning("ASGI startup: starting fallback long-poller (FORCE_POLLING=%s, dispatcher_present=%s)", force_polling_env, has_dispatcher_proc)

                    async def _asgi_longpoll_loop():
                        offset = None
                        await BOT_READY.wait()
                        bot = BOT_APPLICATION.bot if BOT_APPLICATION is not None else None
                        if bot is None:
                            logger.error("ASGI long-poller could not start because BOT_APPLICATION is not initialized")
                            return
                        try:
                            while True:
                                try:
                                    sem = globals().get("GET_UPDATES_SEMAPHORE")
                                    get_bot = globals().get("GET_UPDATES_BOT")
                                    if sem is None:
                                        sem = asyncio.Semaphore(1)
                                    acquired = False
                                    try:
                                        await sem.acquire()
                                        acquired = True
                                        if get_bot:
                                            updates = await get_bot.get_updates(offset=offset, timeout=30)
                                        else:
                                            updates = await bot.get_updates(offset=offset, timeout=30)
                                    finally:
                                        if acquired:
                                            try:
                                                sem.release()
                                            except Exception:
                                                pass
                                    if updates:
                                        for u in updates:
                                            try:
                                                if getattr(u, "update_id", None) is not None:
                                                    offset = int(u.update_id) + 1
                                            except Exception:
                                                pass
                                            try:
                                                await BOT_APPLICATION.update_queue.put(u)
                                            except Exception:
                                                logger.exception("ASGI long-poller failed to enqueue update")
                                    else:
                                        await asyncio.sleep(0.1)
                                except asyncio.CancelledError:
                                    break
                                except (TimedOut, httpx.PoolTimeout) as e:
                                    logger.warning("ASGI long-poller timed out (pool exhausted): %s. Backing off 5s", e)
                                    await asyncio.sleep(5)
                                except Conflict as e:
                                    logger.error("ASGI long-poller conflict (another getUpdates active): %s. Stopping long-poller", e)
                                    break
                                except Exception as e:
                                    logger.exception("ASGI long-poller error: %s", e)
                                    await asyncio.sleep(1)
                        except Exception:
                            logger.exception("ASGI long-poller fatal error")

                    try:
                        global LONG_POLLER_STARTED
                        if not globals().get("LONG_POLLER_STARTED", False):
                            globals()["LONG_POLLER_STARTED"] = True
                            app.state.longpoll = asyncio.create_task(_asgi_longpoll_loop())
                            logger.info("ASGI long-poller started")
                        else:
                            logger.info("ASGI long-poller skipped; background poller already running")
                    except Exception:
                        logger.exception("Failed to start ASGI long-poller")
            except Exception:
                logger.exception("Failed to evaluate ASGI long-poller fallback")
        except Exception as e:
            logger.error(f"Failed to start bot in background: {e}")

    @app.on_event("shutdown")
    async def _stop_bot_background():
        # Cancel the background bot task if present
        try:
            task = getattr(app.state, "bot_task", None)
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.info("Background bot task cancelled on ASGI shutdown")
        except Exception as e:
            logger.error(f"Error stopping background bot task: {e}")
        # Cancel ASGI long-poller if started
        try:
            lp = getattr(app.state, "longpoll", None)
            if lp and not lp.done():
                lp.cancel()
                try:
                    await lp
                except asyncio.CancelledError:
                    logger.info("ASGI long-poller cancelled on shutdown")
        except Exception as e:
            logger.error(f"Error stopping ASGI long-poller: {e}")
        finally:
            try:
                globals()["LONG_POLLER_STARTED"] = False
            except Exception:
                pass
        # Cancel update consumer if present
        try:
            uc = getattr(app.state, "update_consumer", None)
            if uc and not uc.done():
                uc.cancel()
                try:
                    await uc
                except asyncio.CancelledError:
                    logger.info("ASGI update consumer cancelled on shutdown")
        except Exception as e:
            logger.error(f"Error stopping ASGI update consumer: {e}")

        # Close dedicated get_updates client if present
        try:
            gu = globals().get("GET_UPDATES_BOT")
            if gu is not None:
                close_fn = getattr(gu, "close", None)
                if close_fn:
                    try:
                        await close_fn()
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"Error closing GET_UPDATES_BOT: {e}")

except Exception as e:
    logger.warning(f"FastAPI not available or import failed: {e}")
    app = None
