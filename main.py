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
from utils.rate_limiter import ConversionRateLimiter, TelegramAPIRateLimiter
from utils.webhook_monitor import WebhookRecoveryManager
from utils.job_queue import cancel_job

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


def check_ffmpeg_available() -> bool:
    """Return True if ffmpeg is callable from PATH or configured FFMPEG_PATH."""
    try:
        proc = subprocess.run([FFMPEG_PATH, "-version"], capture_output=True, text=True, timeout=5)
        return proc.returncode == 0
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

    # Initialize MongoDB model if MONGO_URI provided
    try:
        import os
        mongo_uri = os.environ.get("MONGO_URI")
        if mongo_uri:
            try:
                from motor.motor_asyncio import AsyncIOMotorClient
                from models import MediaConversionModel

                client = AsyncIOMotorClient(mongo_uri)
                model = MediaConversionModel(client)
                handler_manager.db_model = model
                application.bot_data["db_model"] = model
                logger.info("✅ MongoDB model initialized for logging conversions")
            except Exception:
                logger.exception("Failed to initialize MongoDB model (motor)")
    except Exception:
        logger.debug("MONGO_URI check skipped")

    # Command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("cancel", cancel_command))

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

    application.add_handler(MessageHandler(media_filter, handler_manager.handle_media_message))

    # Ensure a fallback handler is present for non-command messages. Some
    # environments or PTB build variants may not provide the expected media
    # filters; adding a permissive non-command fallback ensures file messages
    # still reach `handle_media_message`.
    try:
        fallback_filter = filters.ALL & ~filters.COMMAND
        application.add_handler(MessageHandler(fallback_filter, handler_manager.handle_media_message))
        logger.info("Fallback media handler registered for non-command messages")
    except Exception:
        logger.debug("Fallback media handler not registered")

    # Callback query handler for menu interactions
    application.add_handler(CallbackQueryHandler(handler_manager.callback_handler))

    # Register custom thumbnail commands if module available
    try:
        from custom_thumbnail import add_thumb, del_thumb

        application.add_handler(CommandHandler("addthumb", add_thumb))
        application.add_handler(CommandHandler("delthumb", del_thumb))
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

    application.add_handler(CommandHandler("admin", admin_command))

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

    application.add_handler(CommandHandler("canceljob", canceljob_command))

    # Settings command - forward to handler manager's show_settings
    async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            await handler_manager.show_settings(update, context)
        except Exception:
            await update.message.reply_text("⚠️ Failed to open settings.")

    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CommandHandler("usettings", settings_command))
    application.add_handler(CommandHandler("usersettings", settings_command))

    async def bulk_url_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            await handler_manager.bulk_url_command(update, context)
        except Exception:
            await update.message.reply_text("⚠️ Failed to enqueue bulk URLs.")

    application.add_handler(CommandHandler("bulk_url", bulk_url_command))

    async def bulk_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            await handler_manager.show_bulk_menu(update, context)
        except Exception:
            await update.message.reply_text("⚠️ Failed to open bulk menu.")

    application.add_handler(CommandHandler("bulkmenu", bulk_menu_command))

    # Store handler manager in bot_data for access in other handlers
    application.bot_data["handler_manager"] = handler_manager

    # Error handler (must be added last)
    application.add_error_handler(error_handler)

    logger.info("✅ All handlers registered successfully")


async def main(background: bool = False) -> None:
    """Start the bot."""
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
    conversion_limiter = ConversionRateLimiter(conversions_per_hour=100)

    # Attach rate limiters to application context
    application.bot_data["api_rate_limiter"] = api_limiter
    application.bot_data["conversion_rate_limiter"] = conversion_limiter

    logger.info("Rate limiters initialized")
    logger.info(f"  - API limit: {TelegramAPIRateLimiter.GENERAL_LIMIT} calls/sec globally")
    logger.info(f"  - Per-user limit: {TelegramAPIRateLimiter.PER_USER_LIMIT} call/sec")
    logger.info("  - Conversion limit: 100 conversions/hour per user")

    # Setup handlers
    setup_handlers(application)

    # Create directories
    await ensure_directories("storage", "storage/input", "storage/output", "storage/temp", "storage/thumbnails", "logs")

    # Start cleanup manager
    try:
        asyncio.create_task(start_cleanup_task())
        logger.info("Cleanup manager started")
    except Exception as e:
        logger.error(f"Failed to start cleanup manager: {e}")

    # Check FFmpeg (binary) availability and ffmpeg-python binding; warn if missing
    if not check_ffmpeg_available():
        logger.info("FFmpeg binary not found or not executable; falling back to CLI checks at runtime.")
    else:
        logger.info("FFmpeg binary is available")

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
    from fastapi.responses import Response
    from telegram import Update as TgUpdate

    app = FastAPI(title="Media Conversion Bot - PTB v20+")

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

    @app.post("/telegram/webhook")
    async def telegram_webhook(request: Request):
        """PTB v20+ compatible webhook endpoint."""
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

        # Try immediate dispatch with a few attempts (fast path)
        attempts = 6
        for i in range(attempts):
            dispatcher = getattr(BOT_APPLICATION, "dispatcher", None)
            if dispatcher and hasattr(dispatcher, "process_update"):
                try:
                    with METRICS_LOCK:
                        METRICS["dispatch_attempts"] += 1
                    await dispatcher.process_update(update)
                    with METRICS_LOCK:
                        METRICS["updates_dispatched"] += 1
                    update_id = getattr(update, "update_id", "unknown")
                    logger.info(
                        "Dispatched update %s via dispatcher.process_update on attempt %s",
                        update_id,
                        i + 1,
                    )
                    return {"ok": True, "update_id": getattr(update, "update_id", None), "dispatched": True}
                except Exception as e:
                    logger.warning(f"Immediate dispatcher attempt {i+1} failed: {e}")
                    with METRICS_LOCK:
                        METRICS["dispatch_failures"] += 1
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
