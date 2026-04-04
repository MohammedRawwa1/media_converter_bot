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

from telegram import Update
from telegram.error import TelegramError
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

# Simple in-memory Prometheus-style counters
METRICS = {
    "webhooks_received": 0,
    "updates_dispatched": 0,
    "updates_queued": 0,
    "dispatch_failures": 0,
    "dispatch_attempts": 0,
}
METRICS_LOCK = threading.Lock()


async def _dispatch_update_task(u: Update) -> None:
    """Dispatch an Update in a background task and maintain metrics.

    Adds structured per-update logging (update_id, user) and measures dispatch duration.
    """
    update_id = getattr(u, "update_id", None)
    user_id = None
    username = None
    try:
        if getattr(u, "effective_user", None):
            user_id = getattr(u.effective_user, "id", None)
            username = getattr(u.effective_user, "username", None) or getattr(u.effective_user, "first_name", None)
    except Exception:
        pass

    start = time.time()
    try:
        logger.info(json.dumps({"event": "dispatch.start", "update_id": update_id, "user_id": user_id, "username": username, "type": type(u).__name__}))
    except Exception:
        logger.debug("dispatch.start (could not serialize structured log)")

    try:
        disp = getattr(BOT_APPLICATION, "dispatcher", None)
        if disp and hasattr(disp, "process_update"):
            with METRICS_LOCK:
                METRICS["dispatch_attempts"] += 1
            await disp.process_update(u)
            with METRICS_LOCK:
                METRICS["updates_dispatched"] += 1
            return

        if hasattr(BOT_APPLICATION, "process_update"):
            with METRICS_LOCK:
                METRICS["dispatch_attempts"] += 1
            await BOT_APPLICATION.process_update(u)
            with METRICS_LOCK:
                METRICS["updates_dispatched"] += 1
            return

        logger.warning("No dispatcher/application.process_update available to dispatch update")
    except Exception as exc:
        try:
            with METRICS_LOCK:
                METRICS["dispatch_failures"] += 1
        except Exception:
            pass
        try:
            logger.exception("Error dispatching update %s (user=%s): %s", update_id, user_id, exc)
        except Exception:
            logger.exception("Error dispatching update (exception logging failed)")
    finally:
        duration = time.time() - start
        try:
            logger.info(json.dumps({"event": "dispatch.end", "update_id": update_id, "user_id": user_id, "username": username, "duration_s": round(duration, 3)}))
        except Exception:
            logger.debug("dispatch.end (could not serialize structured log)")



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

    # Media file handlers (videos, audio, documents)
    # Build the media filter defensively to support multiple PTB versions.
    # Build a resilient media filter using a shared helper (supports
    # multiple PTB versions and import variants).
    try:
        from utils.filter_utils import build_media_filter

        media_filter = build_media_filter(filters)
        if media_filter is None:
            media_filter = filters.ALL
    except Exception:
        # In case the helper isn't available for any reason, fall back to ALL
        media_filter = filters.ALL

    application.add_handler(MessageHandler(media_filter, latency_wrapper(handler_manager.handle_media_message, "handle_media_message")))

    # Ensure a fallback handler is present for non-command messages. Some
    # environments or PTB build variants may not provide the expected media
    # filters; adding a permissive non-command fallback ensures file messages
    # still reach `handle_media_message`.
    try:
        fallback_filter = filters.ALL & ~filters.COMMAND
        application.add_handler(MessageHandler(fallback_filter, latency_wrapper(handler_manager.handle_media_message, "handle_media_message_fallback")))
        logger.info("Fallback media handler registered for non-command messages")
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

    # Admin: cancel a queued/running job by id
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
    application = Application.builder().token(BOT_TOKEN).build()

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

    # Initialize webhook recovery manager if using webhooks
    webhook_manager = None
    if WEBHOOK_URL:
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

            polling_task = None
            if WEBHOOK_URL:
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
                # Polling under an already-running event loop (ASGI/uvicorn)
                # can cause runtime errors because `run_polling()` tries to
                # manage the loop. Advise using a webhook or running the
                # bot in a non-ASGI process. We keep the application started
                # so webhook mode works; otherwise we won't start polling here.
                logger.warning(
                    "Polling under ASGI is unsafe — not starting polling. "
                    "Set WEBHOOK_URL for webhook mode or run the bot outside ASGI."
                )
                polling_task = None
                try:
                    BOT_READY.set()
                except Exception:
                    pass

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

                try:
                    await application.stop()
                finally:
                    try:
                        BOT_READY.clear()
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

                if WEBHOOK_URL:
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
                                            aclose = getattr(r, "aclose", None)
                                            if aclose is not None:
                                                await aclose()
                                            else:
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
                                        aclose = getattr(r, "aclose", None)
                                        if aclose is not None:
                                            await aclose()
                                        else:
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
                                aclose = getattr(r, "aclose", None)
                                if aclose is not None:
                                    await aclose()
                                else:
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
                                aclose = getattr(r, "aclose", None)
                                if aclose is not None:
                                    await aclose()
                                else:
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
        ffmpeg_ok = 1 if check_ffmpeg_available() else 0
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

            # Wait briefly for the bot to become ready to process updates
            try:
                await asyncio.wait_for(BOT_READY.wait(), timeout=15.0)
                logger.info("Bot signalled ready within startup window")
            except asyncio.TimeoutError:
                logger.warning("Bot did not become ready within 15s startup window")
            # Start a lightweight update consumer to ensure updates placed on
            # Application.update_queue are dispatched even if the internal
            # dispatcher task is not present in this ASGI-hosted environment.
            async def _update_consumer():
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

except Exception as e:
    logger.warning(f"FastAPI not available or import failed: {e}")
    app = None
