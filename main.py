# main.py
"""
Main entry point for media conversion bot - Updated for PTB v20+
"""

import asyncio
import functools
import hashlib
import html
import inspect
import json
import logging
import os
import signal
import subprocess
import tempfile
import threading
import time
from urllib.parse import urlparse

import aiohttp
import httpx
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, Conflict, TelegramError, TimedOut

# Request location differs across PTB releases; try both locations and
# fall back to None so the application can continue using default Request.
# PTB v20+ uses HTTPXRequest; older versions use Request from telegram.request.
try:
    from telegram.request import HTTPXRequest as Request
except Exception:
    try:
        from telegram.request import Request
    except Exception:
        try:
            from telegram.utils.request import Request
        except Exception:
            Request = None
import contextlib

from telegram.ext import (
    Application,
    ApplicationHandlerStop,
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
    WEBHOOK_SECRET,
    WEBHOOK_URL,
    is_admin_user,
    is_user_allowed,
    persist_allowed_users,
)
from handlers import EnhancedMediaHandler
from tasks import (
    start_cleanup_task,
    stop_cleanup_task,
)
from utils import (
    ensure_directories,
    presence,
)
from utils.confirm import NO_DATA, confirm_keyboard, is_cancel, parse_confirm, yes_data
from utils.egress_monitor import start_egress_monitor, stop_egress_monitor
from utils.error_handler import (
    get_error_handler,
    setup_comprehensive_logging,
)
from utils.job_queue import cancel_job
from utils.login_handler import cleanup_login_flow, register_login_handlers
from utils.markdown_utils import escape_markdown as _escape_markdown
from utils.queue_admin import cancel_all_jobs, clear_cache_keys
from utils.rate_limiter import ConversionRateLimiter, ConversionRateLimiterRedis, TelegramAPIRateLimiter
from utils.session_healthcheck import (
    get_session_healthchecker,
    start_session_healthcheck,
    stop_session_healthcheck,
)
from utils.session_status import (
    collect_session_status,
    format_status,
    parse_status_callback,
    status_keyboard,
)
from utils.webhook_monitor import WebhookRecoveryManager

try:
    from workers.ffmpeg_worker import create_worker_task
except Exception:
    create_worker_task = None
try:
    from utils.storage import get_storage_backend
except Exception:
    get_storage_backend = None

# Configure comprehensive logging with rotation
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure directories exist early (important for Railway/ASGI import-time logging)
try:
    from contextlib import suppress as _suppress

    from setup_directory import setup_bot_directories

    with _suppress(Exception):
        setup_bot_directories()
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
# Simple Prometheus-style in-memory metrics for ASGI endpoints and dispatch tracking
METRICS = {
    "webhooks_received": 0,
    "updates_dispatched": 0,
    "updates_queued": 0,
    "dispatch_failures": 0,
    "dispatch_attempts": 0,
}
METRICS_LOCK = threading.Lock()


async def _dispatch_update_task(update):
    """Dispatch a single Update to the Application or dispatcher, updating metrics.

    This helper is safe to schedule from background tasks and centralizes
    error handling and metrics increments used by webhook handlers and
    long-pollers.
    """
    try:
        disp = getattr(BOT_APPLICATION, "dispatcher", None)
        if disp and hasattr(disp, "process_update"):
            try:
                with METRICS_LOCK:
                    METRICS["dispatch_attempts"] = METRICS.get("dispatch_attempts", 0) + 1
            except Exception:
                logger.debug("main: operation failed")
            await disp.process_update(update)
            try:
                with METRICS_LOCK:
                    METRICS["updates_dispatched"] = METRICS.get("updates_dispatched", 0) + 1
            except Exception:
                logger.debug("main: operation failed")
            return

        # Fall back to Application.process_update if available
        if hasattr(BOT_APPLICATION, "process_update"):
            try:
                with METRICS_LOCK:
                    METRICS["dispatch_attempts"] = METRICS.get("dispatch_attempts", 0) + 1
            except Exception:
                logger.debug("main: Fall back to Application.process_update if available")
            await BOT_APPLICATION.process_update(update)
            try:
                with METRICS_LOCK:
                    METRICS["updates_dispatched"] = METRICS.get("updates_dispatched", 0) + 1
            except Exception:
                logger.debug("main: operation failed")
            return

        # No dispatcher or Application.process_update available - this should not
        # happen on supported PTB versions (v20/v21 both expose process_update),
        # so just log and count the failure instead of queueing the update.
        try:
            with METRICS_LOCK:
                METRICS["dispatch_failures"] = METRICS.get("dispatch_failures", 0) + 1
        except Exception:
            logger.debug("main: operation failed")
        logger.error(
            "No dispatch path available for update %s",
            getattr(update, "update_id", "unknown"),
        )
    except Exception as exc:
        try:
            with METRICS_LOCK:
                METRICS["dispatch_failures"] = METRICS.get("dispatch_failures", 0) + 1
        except Exception:
            logger.debug("main: operation failed")
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
    SENTRY_DSN = os.environ.get("SENTRY_DSN")
    if SENTRY_DSN:
        try:
            import importlib

            sentry_sdk = importlib.import_module("sentry_sdk")
            sentry_sdk.init(dsn=SENTRY_DSN, traces_sample_rate=0.0)
            logger.info("Sentry initialized")
        except Exception as se:
            logger.warning(f"Failed to initialize Sentry: {se}")
except Exception:
    logger.debug("main: Initialize Sentry if configured via SENTRY_DSN environment variable")


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
🎬 <b>Welcome to Media Conversion Bot</b> 🎧

Hello {html.escape(user_name)}! Send a media file and choose an action from the menu.

<b>⚡ Quick Commands:</b>
/help — Detailed feature guide
/usersettings — Your preferences
/cancel — Stop current operation
/canceljob <code>&lt;job_id&gt;</code> — Cancel a pending job (space between /canceljob and the job ID)

<b>🔐 Login (for large file support):</b>
/login <code>[phone]</code> — Login via Telethon (user account)
/loginpyro <code>[phone]</code> — Login via Pyrogram (better 2FA)
/logout — Log out Telethon session
/logoutpyro — Log out Pyrogram session
/loginstatus — Check live session health
/session_status — Queue, online users &amp; session health (<code>/session_status live</code> for a real check)

<b>📦 Bulk &amp; URLs:</b>
/bulkmenu — Bulk URL processing

Send me a file to get started! 🚀
"""

    await update.message.reply_text(welcome_text, parse_mode="HTML")
    logger.info(f"User {user_id} ({user_name}) started the bot")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Help command handler."""
    help_text = """
📚 <b>Complete Feature List:</b>

<b>🎬 VIDEO PROCESSING:</b>
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

<b>🎧 AUDIO PROCESSING:</b>
• Convert between formats (MP3, WAV, AAC, FLAC, OGG, M4A)
• Adjust bitrate (64k-320k)
• Normalize volume
• Trim segments
• Merge multiple files
• Extract from video
• Change sample rate
• Adjust channels (mono/stereo)

<b>🔧 UTILITIES:</b>
• Full media analysis and information
• Create ZIP archives
• Batch processing
• Progress tracking
• Background processing
• Auto-cleanup of old files

<b>File Limits:</b>
• Maximum file size: 4GB
• Processing time: Depends on file size
• Results auto-delete after sending

<b>🔐 Login Guide:</b>
Use <code>/login [phone]</code> (Telethon) or <code>/loginpyro [phone]</code> (Pyrogram) to sign in with a Telegram user account. This enables large file processing.

When you receive the verification code:
• Type the digits <b>with spaces between them</b> — e.g. <code>"1 2 3 4 5"</code>
• Typing them all together like <code>"12345"</code> <b>does not work</b> — you must use spaces
• If 2FA is enabled, just type your <b>password normally</b>
• The same applies to both Telethon (<code>/login</code>) and Pyrogram (<code>/loginpyro</code>) flows

<b>⚙️ Utility Commands:</b>
/cancel — Cancel current operation
/canceljob <code>&lt;job_id&gt;</code> — Cancel a queued or running job (space between /canceljob and the job ID)
/logout — Log out Telethon session
/logoutpyro — Log out Pyrogram session
/loginstatus — Check live session health
/session_status — Queue, online users &amp; session health (<code>/session_status live</code> for a real check)

<b>Need help?</b> Just send a file and use the menus! 🎯
"""

    await update.message.reply_text(help_text, parse_mode="HTML")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cancel current operation."""
    uid = update.effective_user.id
    # If there's an active login flow (background-task pattern), clean it up
    futures_map = context.application.bot_data.get("login_futures", {})
    if uid in futures_map and futures_map[uid].get("task") is not None and not futures_map[uid]["task"].done():
        logger.info("cancel: cleaning up active login flow for user %s", uid)
        await cleanup_login_flow(context, uid)
        await update.message.reply_text("❌ Login cancelled.")
        return

    await update.message.reply_text(
        "❌ Operation cancelled.\n\nSend /start to see available options.",
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
                    srv_timeout = int(
                        os.environ.get(
                            "MONGO_SERVER_SELECTION_TIMEOUT_MS", os.environ.get("MONGO_SERVER_TIMEOUT_MS", "5000")
                        )
                    )
                except Exception:
                    srv_timeout = 5000
                try:
                    conn_timeout = int(os.environ.get("MONGO_CONNECT_TIMEOUT_MS", "5000"))
                except Exception:
                    conn_timeout = 5000

                try:
                    client = AsyncIOMotorClient(
                        mongo_uri, serverSelectionTimeoutMS=srv_timeout, connectTimeoutMS=conn_timeout
                    )
                    logger.info(
                        "Mongo client created with serverSelectionTimeoutMS=%sms connectTimeoutMS=%sms",
                        srv_timeout,
                        conn_timeout,
                    )
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
                    asyncio.create_task(model.ensure_indexes())
                except Exception:
                    logger.debug("Could not schedule async index creation for Mongo model")
                handler_manager.db_model = model
                application.bot_data["db_model"] = model
                # Register the model so modules that only receive a ``user_id``
                # (the userbot downloader/uploader) can resolve sessions from
                # MongoDB instead of relying solely on the per-user JSON files.
                from utils.telethon_session import set_db_model

                set_db_model(model)
                logger.info("✅ MongoDB model initialized for logging conversions")
            except Exception:
                logger.exception("Failed to initialize MongoDB model (motor)")
    except Exception:
        logger.debug("MONGO_URI check skipped")

    # ── Register the login background-task handlers (no ConversationHandler)
    #     ``register_login_handlers`` adds ``/login`` + a high-priority text
    #     handler that intercepts phone/code/password input during login.
    register_login_handlers(application)

    # Command handlers (wrapped for latency tracing)
    application.add_handler(CommandHandler("start", latency_wrapper(start_command, "start_command")))
    application.add_handler(CommandHandler("help", latency_wrapper(help_command, "help_command")))
    # NOTE: global /cancel is registered here. It is only reached when
    # there is NO active login Conversation — the ConversationHandler
    # registered above consumes /cancel first when a login is in progress.
    application.add_handler(CommandHandler("cancel", latency_wrapper(cancel_command, "cancel_command")))

    # ── Presence: every update marks the sender active so /session_status can
    #    report who is using the bot. Group -1 runs before the real handlers and
    #    deliberately does not stop propagation, so no command is consumed here.
    try:
        from telegram.ext import TypeHandler

        async def _presence_touch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            user = getattr(update, "effective_user", None)
            if user is None:
                return
            # Fire-and-forget: a slow heartbeat must never delay a reply.
            try:
                context.application.create_task(presence.touch(getattr(user, "id", None)))
            except Exception:
                asyncio.create_task(presence.touch(getattr(user, "id", None)))

        # Group -2: it must run before the confirm dispatcher (group -1), because
        # PTB stops at the first matching handler *within* a group.
        application.add_handler(TypeHandler(Update, _presence_touch), group=-2)
    except Exception:
        logger.debug("Could not register the presence heartbeat handler")

    async def loginstatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Live session health check.

        Connects to Telegram and verifies each configured session is actually
        authorized, showing phone/DC/latency for working sessions or the
        real error for broken ones.
        """
        await update.message.reply_text("🩺 Testing sessions live — connecting to Telegram...")

        # ── Run live health check via the healthchecker ──
        # Pass the calling user's ID so per-user sessions are checked
        calling_user_id = update.effective_user.id
        try:
            checker = get_session_healthchecker()
            health = await checker.run_once(user_id=calling_user_id)
        except Exception as exc:
            logger.exception("loginstatus: health check failed: %s", exc)
            health = {}

        pyro = health.get("pyrogram", {})
        tele = health.get("telethon", {})

        # ── Persisted JSON file info (per-user only) ──
        per_user_json_session = None
        per_user_json_exists = False
        try:
            from utils.telethon_session import _get_persisted_session_path, _load_all_sessions_from_file_async

            # Per-user file for the calling user
            _per_user_path = _get_persisted_session_path(user_id=calling_user_id)
            per_user_json_exists = os.path.exists(_per_user_path)
            per_user_json_session = await _load_all_sessions_from_file_async(user_id=calling_user_id)
        except Exception:
            logger.debug("main: Per-user JSON file check failed")

        tele_per_user = (
            bool(per_user_json_session and per_user_json_session.get("telethon_session"))
            if per_user_json_session
            else False
        )
        pyro_per_user = (
            bool(per_user_json_session and per_user_json_session.get("pyrogram_session"))
            if per_user_json_session
            else False
        )

        # ── Credentials check ──
        has_api_id = bool(
            os.getenv("API_ID") or os.getenv("USERBOT_API_ID") or os.getenv("api_id") or os.getenv("userbot_api_id")
        )
        has_api_hash = bool(
            os.getenv("API_HASH")
            or os.getenv("USERBOT_API_HASH")
            or os.getenv("api_hash")
            or os.getenv("userbot_api_hash")
        )
        userbot_enabled = cfg.ENABLE_USERBOT

        # ── Format live check results ──
        def _session_line(name: str, result: dict) -> str:
            if not result:
                return f"❌ **{name}** — Check failed (no result)"
            alive = result.get("alive", False)
            if alive:
                phone = result.get("phone") or "?"
                dc = result.get("dc_id") or "?"
                latency = result.get("latency_ms") or "?"
                return f"✅ **{name}** — Working\n   Phone: `{phone}`\n   DC: `{dc}` | Latency: `{latency}ms`"
            else:
                # Escaped rather than wrapped in backticks: an error string is
                # free-form and a lone ``_``/``*`` in it would otherwise be an
                # unterminated entity and fail the whole send.
                err = _escape_markdown(result.get("error") or "Not configured")
                return f"❌ **{name}** — {err}"

        # Get healthcheck interval from the checker
        check_interval = getattr(checker, "check_interval", 3600)
        admin_alerts = "Enabled" if getattr(checker, "admin_user_id", None) else "Disabled"

        lines = [
            "\U0001f510 **Live Session Status**",
            "",
            "**Userbot enabled:** " + ("✅ Yes" if userbot_enabled else "❌ No"),
            "**API credentials:** " + ("✅ Set" if has_api_id and has_api_hash else "⚠️ Missing API_ID/API_HASH"),
            "",
            _session_line("Telethon", tele),
            "",
            _session_line("Pyrogram", pyro),
            "",
            "**Persisted JSON files (per-user):**",
            f"  Per-user ({calling_user_id}): {'✅ Exists' if per_user_json_exists else '❌ Not found'}",
            f"    Telethon: {'✅' if tele_per_user else '❌'}",
            f"    Pyrogram: {'✅' if pyro_per_user else '❌'}",
            "",
            f"🔔 Admin alerts: `{admin_alerts}`",
            f"🔄 Background check: every `{check_interval}s`",
        ]
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

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

    application.add_handler(
        MessageHandler(media_filter, latency_wrapper(handler_manager.handle_media_message, "handle_media_message"))
    )

    try:
        url_filter = filters.Regex(r"https?://") & ~filters.COMMAND
        application.add_handler(
            MessageHandler(
                url_filter,
                latency_wrapper(handler_manager.handle_media_message, "handle_media_url_message"),
                block=True,
            )
        )
    except Exception:
        logger.debug("URL text handler not registered; Regex filter unavailable")

    # Ensure a fallback handler is present for non-command, non-text messages.
    try:
        fallback_filter = filters.ALL & ~filters.COMMAND & ~filters.TEXT
        application.add_handler(
            MessageHandler(
                fallback_filter, latency_wrapper(handler_manager.handle_media_message, "handle_media_message_fallback")
            )
        )
        logger.info("Fallback media handler registered for non-command non-text messages")
    except Exception:
        logger.debug("Fallback media handler not registered")

    # Callback query handler for menu interactions
    application.add_handler(CallbackQueryHandler(latency_wrapper(handler_manager.callback_handler, "callback_handler")))

    # Register custom thumbnail commands if module available. ``perform_del_thumb``
    # is captured here so the shared confirmation dispatcher can run it too.
    perform_del_thumb = None
    try:
        from custom_thumbnail import add_thumb, del_thumb
        from custom_thumbnail import perform_del_thumb as _perform_del_thumb

        perform_del_thumb = _perform_del_thumb
        application.add_handler(CommandHandler("addthumb", latency_wrapper(add_thumb, "add_thumb")))
        application.add_handler(CommandHandler("delthumb", latency_wrapper(del_thumb, "del_thumb")))
        logger.info("Registered custom thumbnail commands (/addthumb, /delthumb)")
    except Exception:
        logger.debug("custom_thumbnail handlers not registered")

    # Admin commands (manage allowed users)
    async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        # Only admin may manage allowed users. Fails closed: with ADMIN_USER_ID
        # unset nobody is an admin, so this cannot be reached by accident.
        if not is_admin_user(user_id):
            await update.message.reply_text("Unauthorized: admin only")
            return

        args = [str(arg).strip() for arg in (context.args or [])]
        if not args:
            await update.message.reply_text("Usage: /admin add|remove|list <user_id>")
            return

        cmd = args[0].lower()
        if cmd == "list":
            users = sorted(list(ALLOWED_USER_IDS))
            await update.message.reply_text(f"Allowed users: {users}")
            return

        if cmd not in ("add", "remove"):
            await update.message.reply_text("Unknown admin command")
            return

        if len(args) < 2:
            await update.message.reply_text("Specify a user id")
            return

        try:
            target = int(args[1])
        except Exception:
            await update.message.reply_text("Invalid user id")
            return

        # Both directions ask first: `remove` locks a user out, and `add` hands
        # out access - the id comes from a human, and one wrong digit is a stranger.
        verb = "Grant access to" if cmd == "add" else "Revoke access for"
        await update.message.reply_text(
            f"⚠️ *{verb}* `{target}`?",
            parse_mode="Markdown",
            reply_markup=confirm_keyboard("admin", payload=f"{cmd}:{target}"),
        )

    application.add_handler(CommandHandler("admin", latency_wrapper(admin_command, "admin_command")))

    async def _perform_admin(reply, payload: str):
        cmd, _, target_raw = (payload or "").partition(":")
        try:
            target = int(target_raw)
        except (TypeError, ValueError):
            await reply.say("⚠️ Invalid user id in the confirmation.")
            return
        if cmd == "add":
            cfg.ALLOWED_USER_IDS.add(target)
            persist_allowed_users()
            await reply.say(f"✅ Added {target} to allowed users")
        elif cmd == "remove":
            cfg.ALLOWED_USER_IDS.discard(target)
            persist_allowed_users()
            await reply.say(f"✅ Removed {target} from allowed users")
        else:
            await reply.say("⚠️ Unknown admin action.")

    async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask before logging out and deleting the Telethon session."""
        await update.message.reply_text(
            "⚠️ *Log out and delete the Telethon session*?\n"
            "• Removes the session file, its journal/lock and your per-user copy\n"
            "• Clears the session stored in MongoDB for your account\n"
            "• You will have to run /login again, with a fresh Telegram code",
            parse_mode="Markdown",
            reply_markup=confirm_keyboard("logout"),
        )

    application.add_handler(CommandHandler("logout", latency_wrapper(logout_command, "logout_command")))

    async def _perform_logout(reply, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id

        # ── Clean up any active login flow before logging out ──
        futures_map = context.application.bot_data.get("login_futures", {})
        if (
            user_id in futures_map
            and futures_map[user_id].get("task") is not None
            and not futures_map[user_id]["task"].done()
        ):
            logger.info("logout: cleaning up active login flow for user %s", user_id)
            await cleanup_login_flow(context, user_id)

        try:
            from utils.telethon_session import (
                _get_persisted_session_path,
                _invalidate_session_cache,
                get_telethon_session_path,
            )

            session_path = get_telethon_session_path()
            removed = []

            # 1. Remove global session files (backward-compatible)
            if os.path.exists(session_path):
                try:
                    os.remove(session_path)
                    removed.append(session_path)
                except Exception:
                    logger.debug("main: operation failed")
            for suffix in (".session", ".session-journal", ".session.lock", ".session.json"):
                path_with_suffix = session_path + suffix
                if os.path.exists(path_with_suffix):
                    try:
                        os.remove(path_with_suffix)
                        removed.append(path_with_suffix)
                    except Exception:
                        logger.debug("main: operation failed")

            # 2. Remove per-user JSON session file
            per_user_json = _get_persisted_session_path(user_id=user_id)
            if os.path.exists(per_user_json):
                try:
                    os.remove(per_user_json)
                    removed.append(f"Per-user JSON ({os.path.basename(per_user_json)})")
                except Exception as exc:
                    logger.debug("logout: failed to remove per-user JSON %s: %s", per_user_json, exc)

            # 3. Remove per-user Telethon .session file on disk
            per_user_session = f"{session_path}.{user_id}.session"
            if os.path.exists(per_user_session):
                try:
                    os.remove(per_user_session)
                    removed.append(f"Per-user .session ({os.path.basename(per_user_session)})")
                except Exception as exc:
                    logger.debug("logout: failed to remove per-user session %s: %s", per_user_session, exc)

            # 4. Also clear any saved Telethon session from MongoDB
            try:
                db_model_lo = context.application.bot_data.get("db_model")
                if db_model_lo is not None:
                    await db_model_lo.delete_session(user_id)
                    removed.append("MongoDB session")
            except Exception:
                logger.debug("main: Also clear any saved Telethon session from MongoDB")

            # 5. Clear in-memory session cache for this user
            _invalidate_session_cache(user_id=user_id)

            if removed:
                await reply.say(f"✅ Logged out and removed Telethon session files:\n{chr(10).join(removed)}")
            else:
                await reply.say("No local Telethon session file was found to remove.")
        except Exception as exc:
            logger.exception("/logout failed: %s", exc)
            await reply.say("Failed to remove the Telethon session. Check server logs for details.")

    async def logoutpyro_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask before logging out of Pyrogram and clearing its session."""
        await update.message.reply_text(
            "⚠️ *Log out of Pyrogram and clear its session*?\n"
            "• Clears the Pyrogram session JSON (per-user and global)\n"
            "• Clears the Pyrogram session string stored in MongoDB\n"
            "• You will have to run /loginpyro again",
            parse_mode="Markdown",
            reply_markup=confirm_keyboard("logoutpyro"),
        )

    application.add_handler(CommandHandler("logoutpyro", latency_wrapper(logoutpyro_command, "logoutpyro_command")))

    async def _perform_logoutpyro(reply, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id

        # ── Clean up any active login flow before logging out ──
        futures_map = context.application.bot_data.get("login_futures", {})
        if (
            user_id in futures_map
            and futures_map[user_id].get("task") is not None
            and not futures_map[user_id]["task"].done()
        ):
            logger.info("logoutpyro: cleaning up active login flow for user %s", user_id)
            await cleanup_login_flow(context, user_id)

        try:
            removed = []

            # 1. Clear Pyrogram session from JSON file (per-user, then global)
            try:
                from utils.telethon_session import save_session_string_to_file_async

                if await save_session_string_to_file_async("", client_type="pyrogram", user_id=user_id):
                    removed.append("JSON file (pyrogram_session cleared)")
            except Exception as exc:
                logger.debug("logoutpyro: JSON per-user clear failed: %s", exc)
            # Also clear the legacy global JSON file for completeness
            try:
                if await save_session_string_to_file_async("", client_type="pyrogram"):
                    removed.append("Global JSON file (pyrogram_session cleared)")
            except Exception as exc:
                logger.debug("logoutpyro: JSON global clear failed: %s", exc)

            # 2. Clear Pyrogram session from MongoDB (saves "" for pyrogram_session,
            #    preserving telethon_session and string_session keys)
            try:
                db_model_lo = context.application.bot_data.get("db_model")
                if db_model_lo is not None:
                    await db_model_lo.save_session(user_id, {"pyrogram_session": ""})
                    removed.append("MongoDB (pyrogram_session cleared)")
            except Exception as exc:
                logger.debug("logoutpyro: MongoDB clear failed: %s", exc)

            if removed:
                await reply.say(f"✅ Logged out of Pyrogram and cleared session:\n{chr(10).join(removed)}")
            else:
                await reply.say("No Pyrogram session was found to clear.")
        except Exception as exc:
            logger.exception("/logoutpyro failed: %s", exc)
            await reply.say("Failed to clear the Pyrogram session. Check server logs for details.")

    async def recoverpyro_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask before recovering a stale per-user Pyrogram session."""
        await update.message.reply_text(
            "⚠️ *Recover a stale Pyrogram session*?\n"
            "• Clears the per-user Pyrogram JSON entry if it is stale\n"
            "• Re-resolves the session from persisted MongoDB\n"
            "• Restores the per-user JSON file so uploads/downloads work again",
            parse_mode="Markdown",
            reply_markup=confirm_keyboard("recoverpyro"),
        )

    application.add_handler(CommandHandler("recoverpyro", latency_wrapper(recoverpyro_command, "recoverpyro_command")))

    async def _perform_recoverpyro(reply, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id

        try:
            checker = get_session_healthchecker()
            session_str, source = await checker._invalidate_stale_pyrogram_session(user_id=user_id)

            if not session_str:
                await reply.say(
                    "⚠️ No persisted Pyrogram session could be recovered for your account. "
                    "Try `/loginpyro` again if you need a fresh login."
                )
                return

            from utils.telethon_session import save_session_string_to_file_async

            await save_session_string_to_file_async(session_str, client_type="pyrogram", user_id=user_id)
            await reply.say(
                f"✅ Recovered Pyrogram session for your account.\n"
                f"Source: `{source}`\n"
                f"The per-user JSON file has been restored so the bot can use it again."
            )
        except Exception as exc:
            logger.exception("/recoverpyro failed: %s", exc)
            await reply.say("Failed to recover the Pyrogram session. Check server logs for details.")

    async def canceljob_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask before cancelling one job."""
        args = [str(arg).strip() for arg in (context.args or [])]
        if not args:
            await update.message.reply_text("Usage: /canceljob <job_id>")
            return

        job_id = args[0]
        await update.message.reply_text(
            f"⚠️ *Cancel job* `{job_id}`?\nThe worker stops at its next checkpoint and the job is discarded.",
            parse_mode="Markdown",
            reply_markup=confirm_keyboard("canceljob", payload=job_id),
        )

    application.add_handler(CommandHandler("canceljob", latency_wrapper(canceljob_command, "canceljob_command")))

    async def cancelbatch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ask before cancelling one sequential bulk batch."""
        args = [str(arg).strip() for arg in (context.args or [])]
        if not args:
            await update.message.reply_text("Usage: /cancelbatch <batch_id>")
            return
        batch_id = args[0]
        await update.message.reply_text(
            f"⚠️ *Cancel batch* `{batch_id}`?\n"
            "The running job will stop at its next checkpoint and remaining jobs "
            "will be removed without requeueing.",
            parse_mode="Markdown",
            reply_markup=confirm_keyboard("cancelbatch", payload=batch_id),
        )

    application.add_handler(CommandHandler("cancelbatch", latency_wrapper(cancelbatch_command, "cancelbatch_command")))

    async def _perform_canceljob(reply, job_id: str):
        try:
            await cancel_job(job_id)
            await reply.say(f"✅ Requested cancellation for job {job_id}")
        except Exception as e:
            logger.exception("Failed to request cancel for job %s: %s", job_id, e)
            await reply.say(f"❌ Failed to cancel job {job_id}: {e}")

    async def _perform_cancelbatch(reply, batch_id: str):
        await reply.pending("⏹️ Cancelling batch and removing remaining jobs...")
        try:
            from utils.batch_pipeline import cancel_batch

            # cancel_batch takes the batch's whole Redis state with it (counters,
            # membership, the resume record and the members' dedup keys), so a
            # stopped batch is never left behind for the stale-Redis sweep to find.
            report = await cancel_batch(
                batch_id=batch_id, requested_by=getattr(reply.update.effective_user, "id", None)
            )
            _ref = report.get("message")
            if _ref:
                with contextlib.suppress(Exception):
                    await reply.context.bot.delete_message(chat_id=_ref[0], message_id=_ref[1])
            await reply.say(
                f"✅ Batch `{report['batch_id']}` cancelled.\n"
                f"Flagged {report['jobs']} job(s); removed {report['queued']} queued and "
                f"{report['delayed']} delayed job(s); dropped {report.get('dedup', 0)} dedup key(s). "
                f"No remaining member will be requeued.",
                parse_mode="Markdown",
            )
        except Exception:
            logger.exception("/cancelbatch failed for %s", batch_id)
            await reply.say("❌ /cancelbatch failed — check the logs for details.")

    class _Reply:
        """Send an action's output whether a command or a button triggered it.

        A command answers in a fresh message; a button press turns the prompt the
        user just answered into the result, then falls back to new messages so a
        multi-line outcome is never truncated.
        """

        def __init__(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            self.update = update
            self.context = context
            self.query = getattr(update, "callback_query", None)
            self._edited = False
            self._placeholder = None

        async def pending(self, text: str):
            """Show a placeholder the next :meth:`say` replaces."""
            if self.query is not None:
                return await self.say(text)
            self._placeholder = await self.update.message.reply_text(text)
            return self._placeholder

        async def say(self, text: str, **kwargs):
            if self._placeholder is not None:
                placeholder, self._placeholder = self._placeholder, None
                try:
                    return await placeholder.edit_text(text, **kwargs)
                except Exception:
                    logger.debug("confirm: could not update the progress message")

            if self.query is None:
                return await self.update.message.reply_text(text, **kwargs)

            message = getattr(self.query, "message", None)
            if not self._edited and message is not None:
                self._edited = True
                try:
                    return await message.edit_text(text, **kwargs)
                except Exception:
                    logger.debug("confirm: could not edit the prompt; sending a new message")

            chat_id = getattr(message, "chat_id", None)
            if chat_id is None and getattr(self.update, "effective_chat", None):
                chat_id = self.update.effective_chat.id
            return await self.context.bot.send_message(chat_id=chat_id, text=text, **kwargs)

    def _admin_only(update: Update) -> bool:
        """True when the caller is the configured admin.

        Delegates to ``config.is_admin_user`` so every admin-gated command shares
        one fail-closed rule instead of repeating an ``if ADMIN_USER_ID and ...``
        guard that authorises everybody when the id is unset.
        """
        user = getattr(update, "effective_user", None)
        return is_admin_user(getattr(user, "id", None))

    async def cancelall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel every job for every user on both pipes, and clear the batches.

        Cancelling the jobs leaves every batch with no live member, so the batch
        state (counters, membership, the progress message's location, the resume
        record) is taken down in the same run and the progress bars are deleted
        from the chat. That is what used to require
        ``scripts/cleanup_stale_redis.py`` afterwards.
        """
        if not _admin_only(update):
            await update.message.reply_text("Unauthorized: admin only")
            return

        await update.message.reply_text(
            "⚠️ *Cancel every job on both pipes*?\n"
            "• Redis queue: `ffmpeg:jobs` + `ffmpeg:delayed`\n"
            "• Running jobs: flagged `cancel=1` so workers stop\n"
            "• RabbitMQ: `media.jobs.run` / `.retry` / `.dead` purged\n"
            "• Progress keys, input locks and stale dedup keys cleared\n"
            "• Batches cleared, and their progress bars deleted from the chat\n\n"
            "This affects *all users*, not just yours.",
            parse_mode="Markdown",
            reply_markup=confirm_keyboard("cancelall"),
        )

    application.add_handler(CommandHandler("cancelall", latency_wrapper(cancelall_command, "cancelall_command")))

    async def _perform_cancelall(reply):
        await reply.pending("🧹 Draining both pipes, this can take a moment...")
        try:
            # The bot goes along so the cancelled batches' progress messages can
            # be deleted from the chat as well: clearing their Redis state alone
            # left a bar sitting there claiming work that no longer existed, which
            # is why the stale batch cleanup used to need the Redis script.
            report = await cancel_all_jobs(bot=getattr(reply.context, "bot", None))
            await reply.say("\n".join(report.as_lines()))
        except Exception:
            logger.exception("/cancelall failed")
            await reply.say("❌ /cancelall failed — check the logs for details.")

    async def clear_cache_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Wipe the Redis cache keys written by utils.cache and utils.route_cache."""
        if not _admin_only(update):
            await update.message.reply_text("Unauthorized: admin only")
            return

        # Two Yes buttons: the plain wipe only touches Redis, while the second
        # also deletes the shared media-library objects so the cached media really
        # stops existing instead of just losing its Redis descriptor.
        await update.message.reply_text(
            "🧼 *Clear Redis cache*?\n"
            "• `cache:job:`\n"
            "• `cache:user:` / `cache:meta:` / `cache:resp:`\n"
            "• `routecache:` and the in-memory route cache\n"
            "• the **media cache** (`cache:file:*`): the descriptors and the\n"
            "  cached audio/video bodies that let a repeat skip a download\n\n"
            "Job state (`ffmpeg:job:*`) is left alone — use `/cancelall` for that.\n"
            "*Objects in storage* are kept unless you pick **Yes + storage**.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("✅ Yes", callback_data=yes_data("clear_cache")),
                        InlineKeyboardButton("🧹 Yes + storage", callback_data=yes_data("clear_cache", "storage")),
                    ],
                    [InlineKeyboardButton("❌ No", callback_data=NO_DATA)],
                ]
            ),
        )

    application.add_handler(CommandHandler("clear_cache", latency_wrapper(clear_cache_command, "clear_cache_command")))

    async def _perform_clear_cache(reply, purge_storage: bool):
        await reply.pending("🧼 Clearing cache keys...")
        try:
            report = await clear_cache_keys(clear_media_storage=purge_storage)
            await reply.say("\n".join(report.as_lines()))
        except Exception:
            logger.exception("/clear_cache failed")
            await reply.say("❌ /clear_cache failed — check the logs for details.")

    async def confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Run the action behind an inline Yes/No confirmation.

        Registered in group ``-1`` so it answers before the menu callback handler
        (which accepts every callback data) can inspect the same press, and
        stopped afterwards so the menu never sees it.
        """
        query = update.callback_query
        data = getattr(query, "data", None)
        if is_cancel(data):
            with contextlib.suppress(Exception):
                await query.answer("Cancelled")
            with contextlib.suppress(Exception):
                await query.edit_message_text("❌ Cancelled.")
            raise ApplicationHandlerStop

        parsed = parse_confirm(data)
        if parsed is None:
            return
        action, payload = parsed

        with contextlib.suppress(Exception):
            await query.answer()
        reply = _Reply(update, context)
        try:
            if action == "cancelall":
                await _perform_cancelall(reply)
            elif action == "worker_restart":
                await _perform_worker_restart(reply, update)
            elif action == "clear_cache":
                await _perform_clear_cache(reply, payload == "storage")
            elif action == "canceljob":
                await _perform_canceljob(reply, payload or "")
            elif action == "cancelbatch":
                await _perform_cancelbatch(reply, payload or "")
            elif action == "admin":
                await _perform_admin(reply, payload or "")
            elif action == "logout":
                await _perform_logout(reply, update, context)
            elif action == "logoutpyro":
                await _perform_logoutpyro(reply, update, context)
            elif action == "recoverpyro":
                await _perform_recoverpyro(reply, update, context)
            elif action == "delthumb":
                if perform_del_thumb is None:
                    await reply.say("⚠️ Thumbnail handlers are unavailable.")
                else:
                    await perform_del_thumb(reply, update, context)
            else:
                await reply.say("⚠️ Unknown action.")
        except Exception:
            logger.exception("confirm action %r failed", action)
            with contextlib.suppress(Exception):
                await reply.say("❌ Action failed — check the logs for details.")
        # Nothing else may handle this press.
        raise ApplicationHandlerStop

    application.add_handler(CallbackQueryHandler(confirm_callback, pattern=r"^cf[mn]"), group=-1)

    # Settings command - forward to handler manager's show_settings
    async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            await handler_manager.show_settings(update, context)
        except Exception:
            await update.message.reply_text("⚠️ Failed to open settings.")

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

    async def session_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Queue depth, who is online, and session health in one view.

        Admins get the whole dashboard; everyone else sees their own jobs and
        queue turn. ``/session_status live`` additionally runs a real Telegram
        session check instead of reporting the last cached result.
        """
        args = [str(arg).lower() for arg in (getattr(context, "args", None) or [])]
        live = "live" in args
        is_admin = _admin_only(update)

        note = await update.message.reply_text(
            "🩺 Testing sessions live — connecting to Telegram..." if live else "📊 Collecting status..."
        )
        keyboard = status_keyboard(is_admin=is_admin, live=live)
        try:
            payload = await collect_session_status(
                user_id=getattr(update.effective_user, "id", None),
                is_admin=is_admin,
                live_sessions=live,
            )
            text = format_status(payload, is_admin=is_admin)
        except Exception:
            logger.exception("/session_status failed")
            text = "❌ /session_status failed — check the logs for details."
            keyboard = None

        try:
            await note.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        except Exception:
            with contextlib.suppress(Exception):
                await update.message.reply_text(text, parse_mode="HTML", reply_markup=keyboard)

    application.add_handler(
        CommandHandler("session_status", latency_wrapper(session_status_command, "session_status_command"))
    )
    application.add_handler(
        CommandHandler("sessionstatus", latency_wrapper(session_status_command, "session_status_command"))
    )

    async def worker_restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Recycle the standalone ffmpeg worker so it comes back with a clean heap.

        The worker finished job after job returns memory to the OS, so this is for
        the case memory does *not* come back down (a pathologically large file, or
        a leak) and you would rather hand the next job a fresh process than keep
        building on a dirty one.
        """
        if not _admin_only(update):
            await update.message.reply_text("Unauthorized: admin only")
            return

        await update.message.reply_text(
            "♻️ *Restart the ffmpeg worker?*\n"
            "• The worker exits at its next safe point — right after the job it is "
            "running, or while idle\n"
            "• Running work is *not* interrupted; it finishes first\n"
            "• Nothing is lost — `ffmpeg:jobs` and `ffmpeg:delayed` survive the "
            "restart\n"
            "• The platform starts the worker again with an empty heap\n\n"
            "Use this when memory does not come back down after a large file.",
            parse_mode="Markdown",
            reply_markup=confirm_keyboard("worker_restart"),
        )

    application.add_handler(
        CommandHandler("worker_restart", latency_wrapper(worker_restart_command, "worker_restart_command"))
    )

    async def _perform_worker_restart(reply, update):
        await reply.pending("♻️ Asking the worker to recycle...")
        try:
            from utils.batch_pipeline import request_worker_restart

            user = getattr(update, "effective_user", None)
            requested = await request_worker_restart(requested_by=getattr(user, "id", None))
        except Exception:
            logger.exception("/worker_restart failed")
            requested = False
        if not requested:
            await reply.say("❌ Could not reach Redis to request a restart.")
            return
        await reply.say(
            "✅ Restart requested.\n"
            "The worker exits once the job it is running finishes. Watch the worker "
            "logs for `Exiting (1) for a memory-clean worker restart`."
        )

    async def session_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Serve the ``/session_status`` buttons: refresh in place, or recycle the worker.

        Registered in group ``-1`` and stopped afterwards so a dashboard press is
        never also inspected by the menu callback handler, which is registered
        without a pattern and accepts every callback.
        """
        query = update.callback_query
        parsed = parse_status_callback(getattr(query, "data", None))
        if parsed is None:
            return
        action, live = parsed
        is_admin = _admin_only(update)
        user_id = getattr(update.effective_user, "id", None)
        note = None

        if action == "restart":
            if not is_admin:
                with contextlib.suppress(Exception):
                    await query.answer("Admin only.", show_alert=True)
                raise ApplicationHandlerStop
            try:
                from utils.batch_pipeline import request_worker_restart

                requested = await request_worker_restart(requested_by=user_id)
            except Exception:
                logger.exception("session_status: worker restart request failed")
                requested = False
            with contextlib.suppress(Exception):
                await query.answer(
                    "✅ Restart requested — the worker exits after the job it is running."
                    if requested
                    else "❌ Could not reach Redis to request a restart.",
                    show_alert=True,
                )
            # Answering in a popup keeps the dashboard itself intact, which is the
            # whole point of having the button here instead of in /worker_restart.
            note = (
                "♻️ <b>Worker restart requested</b> — it exits once the job it is "
                "running finishes; nothing is lost from the queue."
                if requested
                else "⚠️ <b>Restart not requested</b> — Redis was unreachable."
            )
        else:
            with contextlib.suppress(Exception):
                await query.answer("Refreshing...")

        try:
            payload = await collect_session_status(
                user_id=user_id,
                is_admin=is_admin,
                live_sessions=live,
                # A button press is a deliberate ask, so it pays for a fresh scan
                # instead of showing the cached one.
                force_storage=True,
            )
            text = format_status(payload, is_admin=is_admin, note=note)
        except Exception:
            logger.exception("/session_status refresh failed")
            with contextlib.suppress(Exception):
                await query.answer("Refresh failed — check the logs.", show_alert=True)
            raise ApplicationHandlerStop from None

        try:
            await query.edit_message_text(
                text,
                parse_mode="HTML",
                reply_markup=status_keyboard(is_admin=is_admin, live=live),
            )
        except BadRequest:
            # "message is not modified" - Refresh pressed twice in the same second.
            logger.debug("session_status: dashboard already up to date")
        except Exception:
            logger.debug("session_status: could not update the dashboard in place")
        raise ApplicationHandlerStop

    application.add_handler(
        CallbackQueryHandler(latency_wrapper(session_status_callback, "session_status_callback"), pattern=r"^st:"),
        group=-1,
    )

    # Store handler manager in bot_data for access in other handlers
    application.bot_data["handler_manager"] = handler_manager

    # Error handler (must be added last)
    application.add_error_handler(error_handler)

    logger.info("✅ All handlers registered successfully")


def _build_request(
    connection_pool_size: int,
    pool_timeout: float,
    connect_timeout: float,
    read_timeout: float,
):
    """Build an HTTPXRequest/Request tolerating PTB releases that reject
    the ``connection_pool_size`` kwarg.

    The kwarg exists across PTB v20-v22 (default 1, then 256 from v22.4),
    but some releases/forks reject it — retrying without it keeps the
    builder working on any supported version.

    The class used is the flood-gated variant, so every API call the bot makes
    is checked against the shared flood window before it goes out (see
    ``utils/telegram_flood_request``). Falls back to the plain request when that
    module cannot be imported.

    Returns ``None`` when no Request class is importable so callers can
    fall back to their default behavior.
    """
    request_class = Request
    try:
        from utils.telegram_flood_request import FloodGatedRequest

        if FloodGatedRequest is not None and Request is not None:
            request_class = FloodGatedRequest
    except Exception:
        logger.debug("Flood gate: gated request unavailable; using the plain request")

    if request_class is None:
        return None
    kwargs = {
        "pool_timeout": pool_timeout,
        "connect_timeout": connect_timeout,
        "read_timeout": read_timeout,
    }
    try:
        return request_class(connection_pool_size=connection_pool_size, **kwargs)
    except TypeError:
        # Some PTB releases reject connection_pool_size; fall back gracefully.
        logger.info("Request() rejected connection_pool_size; building request without it")
        return request_class(**kwargs)


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

    # Share Telegram flood-control windows with the worker process (same bot
    # token, same Redis): a penalty earned here has to stop the worker from
    # editing that chat's progress bar, and the reverse.
    try:
        from utils.rate_limiter import telegram_flood_gate

        telegram_flood_gate.attach_redis()
    except Exception:
        logger.debug("Flood gate: no shared backend attached (staying process-local)")

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
        req = _build_request(
            connection_pool_size=http_pool_size,
            pool_timeout=http_pool_timeout,
            connect_timeout=http_connect_timeout,
            read_timeout=http_read_timeout,
        )
        bot_instance = Bot(token=BOT_TOKEN, request=req)
        application = (
            Application.builder()
            .bot(bot_instance)
            .concurrent_updates(int(os.environ.get("CONCURRENT_UPDATES", "8")))
            .build()
        )
    except Exception:
        # Fallback to default behavior
        application = (
            Application.builder()
            .token(BOT_TOKEN)
            .concurrent_updates(int(os.environ.get("CONCURRENT_UPDATES", "8")))
            .build()
        )

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
                gu_req = _build_request(
                    connection_pool_size=gu_pool_size,
                    pool_timeout=gu_pool_timeout,
                    connect_timeout=http_connect_timeout,
                    read_timeout=http_read_timeout,
                )
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
        logger.debug("main: Expose external service config into application context")

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
            conversion_limiter = ConversionRateLimiterRedis(
                conversions_per_hour=int(os.environ.get("CONVERSIONS_PER_HOUR", "360"))
            )
        else:
            conversion_limiter = ConversionRateLimiter(
                conversions_per_hour=int(os.environ.get("CONVERSIONS_PER_HOUR", "360"))
            )
    except Exception:
        conversion_limiter = ConversionRateLimiter(
            conversions_per_hour=int(os.environ.get("CONVERSIONS_PER_HOUR", "360"))
        )

    # Attach rate limiters to application context
    application.bot_data["api_rate_limiter"] = api_limiter
    application.bot_data["conversion_rate_limiter"] = conversion_limiter

    # Optionally pre-initialize storage backend (fail-fast / diagnostics)
    try:
        if get_storage_backend is not None:
            try:
                storage_backend = await get_storage_backend()
                application.bot_data["storage_backend"] = storage_backend
                logger.info(
                    "Storage backend initialized: %s",
                    (os.getenv("STORAGE_BACKEND") or getattr(cfg, "STORAGE_BACKEND", "local")),
                )
            except Exception as e:
                logger.warning("Storage backend initialization failed: %s", e)
    except Exception:
        logger.debug("main: Optionally pre-initialize storage backend (fail-fast / diagnostics)")

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

    # ── Startup stale-temp cleanup: remove temp files older than 30 minutes ──
    #    This prevents stale files from previous runs (e.g. crashed workers) from
    #    being picked up by the pipeline or consuming disk space.
    try:
        from tasks.cleanup_tasks import cleanup_manager as _cleanup_mgr

        _cleaned = await _cleanup_mgr.startup_temp_cleanup(max_age=1800)  # 30 minutes
        if _cleaned > 0:
            logger.info("Startup temp cleanup: removed %d stale files", _cleaned)
    except Exception as _cleanup_exc:
        logger.warning("Startup temp cleanup failed (non-fatal): %s", _cleanup_exc)

    # Start session healthcheck (with MongoDB persistence for session strings)
    try:
        _shc_db_model = application.bot_data.get("db_model")
        start_session_healthcheck(
            admin_user_id=ADMIN_USER_ID,
            bot_app=application,
            db_model=_shc_db_model,
            check_interval=int(os.environ.get("SESSION_HEALTHCHECK_INTERVAL", "3600")),
        )
        logger.info(
            "Session healthcheck started (interval=%ss, db_model=%s)",
            os.environ.get("SESSION_HEALTHCHECK_INTERVAL", "3600"),
            "yes" if _shc_db_model else "no",
        )
    except Exception as e:
        logger.error(f"Failed to start session healthcheck: {e}")

    # Start the storage-egress watchdog. IDrive e2's free egress is a multiple of
    # what you store, and exceeding it suspends the account, so the admin is told
    # when the cycle crosses the watch line instead of finding out from the
    # provider. Alerts are once per severity per billing cycle.
    try:
        start_egress_monitor(
            admin_user_id=ADMIN_USER_ID,
            bot_app=application,
            check_interval=int(os.environ.get("EGRESS_CHECK_INTERVAL", "900")),
        )
        logger.info("Egress monitor started (interval=%ss)", os.environ.get("EGRESS_CHECK_INTERVAL", "900"))
    except Exception as e:
        logger.error(f"Failed to start egress monitor: {e}")

    # ── Eagerly persist env-var session strings to per-user JSON + MongoDB ──
    # After a redeploy the ephemeral per-user JSON files are empty, so this
    # seeds the owner's file right away instead of waiting for the healthcheck.
    #
    # This seeding is strictly **additive**: it must never overwrite a session
    # that is already stored (JSON or MongoDB).  A stale ``PYROGRAM_SESSION`` /
    # ``TELETHON_SESSION`` env var would otherwise re-poison the durable session
    # on every deploy and wipe out a fresher session created via ``/loginpyro``
    # or ``/login`` — which is exactly why the owner's Pyrogram session kept
    # expiring while every other user's persisted normally.
    try:
        from utils.telethon_session import (
            _load_all_sessions_from_file_async,
            _load_mongo_session,
            save_session_string_to_file_async,
        )

        _admin_persist_id = ADMIN_USER_ID
        _mongo_db = application.bot_data.get("db_model")

        async def _has_stored_session(key: str) -> bool:
            """True when a session of ``key`` is already stored for the admin."""
            if not _admin_persist_id:
                return False
            stored = await _load_all_sessions_from_file_async(user_id=_admin_persist_id)
            if isinstance(stored, dict) and stored.get(key):
                return True
            try:
                doc = await _load_mongo_session(_mongo_db, _admin_persist_id)
            except Exception:
                doc = None
            return bool(isinstance(doc, dict) and doc.get(key))

        _pyro_env = os.getenv("PYROGRAM_SESSION") or os.getenv("USERBOT_PYROGRAM_SESSION")
        if _pyro_env and not await _has_stored_session("pyrogram_session"):
            _existing_json = await _load_all_sessions_from_file_async()
            if _existing_json.get("pyrogram_session") != _pyro_env:
                await save_session_string_to_file_async(_pyro_env, client_type="pyrogram")
            if _admin_persist_id:
                await save_session_string_to_file_async(_pyro_env, client_type="pyrogram", user_id=_admin_persist_id)
                if _mongo_db is not None:
                    await _mongo_db.save_session(
                        _admin_persist_id,
                        {"pyrogram_session": _pyro_env},
                    )
        elif _pyro_env:
            logger.info("Startup: kept existing Pyrogram session (env seeding skipped)")

        _telethon_env = None
        for _k in (
            "API_SESSION",
            "SESSION",
            "api_session",
            "USERBOT_SESSION",
            "userbot_session",
            "TELETHON_SESSION",
            "telethon_session",
        ):
            _v = os.getenv(_k)
            if _v:
                _telethon_env = _v
                break

        if _telethon_env and not await _has_stored_session("telethon_session"):
            _existing_json = await _load_all_sessions_from_file_async()
            if _existing_json.get("telethon_session") != _telethon_env:
                await save_session_string_to_file_async(_telethon_env, client_type="telethon")
            if _admin_persist_id:
                await save_session_string_to_file_async(
                    _telethon_env, client_type="telethon", user_id=_admin_persist_id
                )
                if _mongo_db is not None:
                    await _mongo_db.save_session(
                        _admin_persist_id,
                        {"telethon_session": _telethon_env},
                    )
        elif _telethon_env:
            logger.info("Startup: kept existing Telethon session (env seeding skipped)")

        logger.info(
            "Startup: env-var session seeding complete (admin=%s)",
            _admin_persist_id,
        )
    except Exception as exc:
        logger.debug("Startup env->per-user JSON persistence skipped: %s", exc)

    # ── Startup: persist MongoDB sessions to per-user JSON files ──
    # When env vars are NOT set (pure remote login via /loginpyro or /login),
    # the JSON files start empty on a fresh deploy.  Read from MongoDB and write
    # to per-user JSON so the uploader/downloader and /loginstatus can find them
    # immediately without waiting for the healthchecker (~1 hour).
    try:
        _mongo_db = application.bot_data.get("db_model")

        if not os.getenv("PYROGRAM_SESSION") and ADMIN_USER_ID and _mongo_db:
            from utils.telethon_session import (
                _load_all_sessions_from_file_async,
                get_pyrogram_session_string_for_user,
                save_session_string_to_file_async,
            )

            _existing = await _load_all_sessions_from_file_async()
            if not _existing.get("pyrogram_session"):
                _mongo_pyro = await get_pyrogram_session_string_for_user(
                    user_id=ADMIN_USER_ID,
                    db_model=_mongo_db,
                )
                if _mongo_pyro:
                    await save_session_string_to_file_async(
                        _mongo_pyro,
                        client_type="pyrogram",
                        user_id=ADMIN_USER_ID,
                    )
                    logger.info("Startup: persisted Pyrogram session from MongoDB to per-user JSON file")

        _telethon_env_keys = (
            "API_SESSION",
            "SESSION",
            "api_session",
            "USERBOT_SESSION",
            "userbot_session",
            "TELETHON_SESSION",
            "telethon_session",
        )
        _has_telethon_env = any(os.getenv(k) for k in _telethon_env_keys)
        if not _has_telethon_env and ADMIN_USER_ID and _mongo_db:
            from utils.telethon_session import (
                _load_all_sessions_from_file_async,
                get_telethon_session_string_for_user,
                save_session_string_to_file_async,
            )

            _existing_tl = await _load_all_sessions_from_file_async()
            if not _existing_tl.get("telethon_session"):
                _mongo_tele = await get_telethon_session_string_for_user(
                    user_id=ADMIN_USER_ID,
                    db_model=_mongo_db,
                )
                if _mongo_tele:
                    await save_session_string_to_file_async(
                        _mongo_tele,
                        client_type="telethon",
                        user_id=ADMIN_USER_ID,
                    )
                    logger.info("Startup: persisted Telethon session from MongoDB to per-user JSON file")
    except Exception as exc:
        logger.debug("Startup MongoDB->JSON persistence skipped: %s", exc)

    # ── Restore per-user JSON session files for ALL users from MongoDB ──
    # The per-user JSON session files live on an ephemeral filesystem and are
    # wiped on every redeploy. Re-materialize each stored user's JSON file from
    # the durable MongoDB sessions collection so per-user sessions created via
    # /login or /loginpyro keep working immediately after a deployment.
    try:
        from utils.telethon_session import restore_per_user_session_files

        _restore_db_model = application.bot_data.get("db_model")
        _restored_count = await restore_per_user_session_files(_restore_db_model)
        if _restored_count:
            logger.info(
                "Startup: restored per-user JSON session files for %d user(s) from MongoDB",
                _restored_count,
            )
    except Exception as exc:
        logger.debug("Startup: per-user session file restore skipped: %s", exc)

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
            logger.debug("main: Reduce noisy logs from ffmpeg/ffmpeg-python internals where possible")
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

    # ── Signal handler helpers (named functions for strong refs) ──
    def _request_shutdown(sig_name: str = None):
        logger.info(f"Shutdown requested via signal: {sig_name}")
        try:
            loop.call_soon_threadsafe(shutdown_event.set)
        except Exception:
            # last-resort: set result via asyncio.ensure_future
            asyncio.ensure_future(shutdown_event.set())

    def _on_sigint(s, f):
        loop.call_soon_threadsafe(shutdown_event.set)

    # SIGTERM uses the same handler as SIGINT (triggers graceful shutdown)
    _on_sigterm = _on_sigint

    try:
        loop.add_signal_handler(signal.SIGINT, lambda: _request_shutdown("SIGINT"))
        loop.add_signal_handler(signal.SIGTERM, lambda: _request_shutdown("SIGTERM"))
    except (NotImplementedError, RuntimeError):
        # Fallback for Windows, non-running loops, or event loops that don't
        # support add_signal_handler (e.g. some ASGI startup sequences).
        # CPython's signal.signal() keeps a strong reference to the handler.
        signal.signal(signal.SIGINT, _on_sigint)
        with contextlib.suppress(Exception):
            signal.signal(signal.SIGTERM, _on_sigterm)

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
            with contextlib.suppress(Exception):
                BOT_READY.set()

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
                        logger.warning(
                            "Dispatcher not available after Application.start(); enabling FORCE_POLLING fallback"
                        )
                        force_polling = True
            except Exception:
                logger.exception("Failed to evaluate dispatcher presence for FORCE_POLLING fallback")

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
                # via getUpdates and dispatches them through Application.process_update
                if WEBHOOK_URL and force_polling:
                    try:
                        await application.bot.delete_webhook(drop_pending_updates=False)
                        logger.info("Deleted existing webhook to allow FORCE_POLLING long-poller")
                    except Exception as e:
                        logger.warning(f"Failed to delete webhook before long-poller: {e}")

                logger.info("Starting background long-poller (FORCE_POLLING enabled)")

                # Distributed lock to prevent multiple workers from polling simultaneously
                _longpoll_redis_lock = None
                try:
                    from utils.redis_lock import RedisLock

                    _longpoll_redis_lock = RedisLock("longpoller", ttl=35)
                except Exception:
                    _longpoll_redis_lock = None

                async def _longpoll_loop():
                    offset = None
                    bot = application.bot
                    while True:
                        try:
                            # Acquire distributed lock (only one worker polls at a time)
                            if _longpoll_redis_lock is not None:
                                if not await _longpoll_redis_lock.acquire():
                                    logger.debug("Long-poller: another worker holds the lock; sleeping")
                                    await asyncio.sleep(5)
                                    continue
                                with contextlib.suppress(Exception):
                                    await _longpoll_redis_lock.renew()
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
                                    with contextlib.suppress(Exception):
                                        sem.release()
                            if updates:
                                for u in updates:
                                    try:
                                        if getattr(u, "update_id", None) is not None:
                                            offset = int(u.update_id) + 1
                                    except Exception:
                                        logger.debug("main: operation failed")
                                    try:
                                        # Dispatch directly via process_update (non-deprecated API)
                                        asyncio.create_task(_dispatch_update_task(u))
                                    except Exception:
                                        logger.exception("Failed to schedule polled update dispatch")
                            else:
                                # no updates; brief pause before next long-poll
                                await asyncio.sleep(0.1)
                        except asyncio.CancelledError:
                            break
                        except (TimedOut, httpx.PoolTimeout) as e:
                            logger.warning("Long-poller timed out (pool exhausted): %s. Backing off 5s", e)
                            await asyncio.sleep(5)
                        except Conflict as e:
                            logger.warning("Long-poller conflict: %s. Releasing lock and retrying", e)
                            if _longpoll_redis_lock is not None:
                                with contextlib.suppress(Exception):
                                    await _longpoll_redis_lock.release()
                            await asyncio.sleep(10)
                            continue
                        except Exception as e:
                            logger.exception(f"Long-poller error: {e}")
                            await asyncio.sleep(1)
                        finally:
                            if _longpoll_redis_lock is not None and _longpoll_redis_lock.is_acquired:
                                with contextlib.suppress(Exception):
                                    await _longpoll_redis_lock.renew()

                try:
                    can_start = True
                    if _longpoll_redis_lock is not None:
                        can_start = await _longpoll_redis_lock.acquire()
                    if can_start and not globals().get("LONG_POLLER_STARTED", False):
                        globals()["LONG_POLLER_STARTED"] = True
                        polling_task = asyncio.create_task(_longpoll_loop())
                    elif globals().get("LONG_POLLER_STARTED", False):
                        logger.info("Background long-poller already running; skipping duplicate start")
                    else:
                        logger.info("Another worker holds long-poller lock; skipping")
                except Exception:
                    logger.exception("Failed to start background long-poller")

            # Fail loudly at startup when Kafka events are enabled but the
            # producer cannot publish: without this the layer reports
            # events_enabled=true and then drops every event from a debug log.
            # Raises only when EVENTBUS_REQUIRE_BROKERS is set.
            try:
                from utils import eventbus as _eventbus

                await _eventbus.verify_events_startup()
            except Exception:
                logger.exception("eventbus: startup verification failed")
                raise

            # Start the ffmpeg worker as a background task so the web service
            # can process jobs whenever it is awake (critical for free tier
            # where separate worker services may spin down).
            worker_task = None
            if create_worker_task is not None:
                try:
                    worker_task = create_worker_task(shutdown_event)
                    logger.info("Background ffmpeg worker task started")
                except Exception as e:
                    logger.error(f"Failed to start background worker task: {e}")
                    worker_task = None
            else:
                logger.info("create_worker_task not available; background worker disabled")

            # Start keep-alive heartbeat to prevent free tier spin-down.
            # Periodically makes an HTTP GET to our own /health endpoint,
            # which counts as inbound traffic and resets the inactivity timer.
            keep_alive_task = None
            _ka_enabled = os.environ.get("KEEP_ALIVE_DISABLED", "").lower() not in ("1", "true", "yes")
            if _ka_enabled:
                try:
                    _ka_url = os.environ.get("KEEP_ALIVE_URL") or ""
                    if not _ka_url:
                        _railway_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN") or ""
                        if _railway_domain:
                            # Railway provides RAILWAY_PUBLIC_DOMAIN as a hostname without protocol
                            _ka_url = f"https://{_railway_domain}"
                    if not _ka_url:
                        try:
                            from urllib.parse import urlparse as _urlparse

                            _parsed = _urlparse(WEBHOOK_URL or "")
                            if _parsed.netloc:
                                _ka_url = f"{_parsed.scheme}://{_parsed.netloc}"
                        except Exception:
                            _ka_url = ""

                    if _ka_url:
                        _ka_url = _ka_url.rstrip("/")
                        _health_url = f"{_ka_url}/health"
                        _ka_interval = max(60, min(840, int(os.environ.get("KEEP_ALIVE_INTERVAL", "600"))))

                        async def _keep_alive_loop():
                            """Periodically ping /health to prevent free tier spin-down."""
                            logger.info("Keep-alive heartbeat started: pinging %s every %ss", _health_url, _ka_interval)
                            try:
                                async with aiohttp.ClientSession() as _session:
                                    while True:
                                        try:
                                            async with _session.get(_health_url, timeout=10) as _resp:
                                                logger.debug("Keep-alive ping: %s", _resp.status)
                                        except (TimeoutError, aiohttp.ClientError, OSError) as _e:
                                            logger.debug("Keep-alive ping failed (harmless): %s", _e)

                                        # Wait for interval or shutdown event
                                        try:
                                            await asyncio.wait_for(shutdown_event.wait(), timeout=_ka_interval)
                                            break  # Shutdown requested
                                        except TimeoutError:
                                            continue  # Time to ping again
                                        except asyncio.CancelledError:
                                            break
                            except asyncio.CancelledError:
                                pass
                            logger.info("Keep-alive heartbeat stopped")

                        keep_alive_task = asyncio.create_task(_keep_alive_loop())
                    else:
                        logger.info(
                            "Keep-alive heartbeat disabled: no public URL available (set KEEP_ALIVE_URL, RAILWAY_PUBLIC_DOMAIN, or WEBHOOK_URL)"
                        )
                except Exception as _ka_err:
                    logger.warning("Failed to start keep-alive heartbeat: %s", _ka_err)
                    keep_alive_task = None
            else:
                logger.info("Keep-alive heartbeat disabled via KEEP_ALIVE_DISABLED=1")

            # Wait for shutdown_event or cancellation; FastAPI will cancel
            # this task on shutdown which will raise CancelledError here.
            try:
                await shutdown_event.wait()
                logger.info("Shutdown event received, stopping bot...")
            except asyncio.CancelledError:
                logger.info("Background bot task cancelled; stopping application")
            finally:
                # CRITICAL: Do NOT delete the webhook on graceful shutdown.
                # On some platforms the service spins down after inactivity
                # of inactivity. The webhook MUST persist so that Telegram's
                # next update POST can wake the service back up. If we delete
                # the webhook here, Telegram has no URL to send updates to,
                # and the service stays dead permanently with no way to be
                # woken by incoming bot messages.
                #
                # The startup code (set_webhook above) always re-verifies and
                # re-sets the webhook when the service starts, so a stale or
                # outdated webhook will be corrected on the next boot.
                # Don't delete it on the way out.

                try:
                    stop_cleanup_task()
                    logger.info("Cleanup manager stop requested")
                except Exception as e:
                    logger.error(f"Error stopping cleanup manager: {e}")

                try:
                    stop_session_healthcheck()
                    logger.info("Session healthcheck stop requested")
                except Exception as e:
                    logger.error(f"Error stopping session healthcheck: {e}")

                try:
                    stop_egress_monitor()
                    logger.info("Egress monitor stop requested")
                except Exception as e:
                    logger.error(f"Error stopping egress monitor: {e}")

                if polling_task:
                    try:
                        polling_task.cancel()
                        await polling_task
                    except Exception:
                        logger.debug("main: operation failed")
                    finally:
                        with contextlib.suppress(Exception):
                            globals()["LONG_POLLER_STARTED"] = False

                # Cancel the background worker task
                if worker_task is not None:
                    try:
                        worker_task.cancel()
                        await worker_task
                    except Exception:
                        logger.debug("main: Cancel the background worker task")

                # Cancel the keep-alive heartbeat task
                if keep_alive_task is not None:
                    try:
                        keep_alive_task.cancel()
                        await keep_alive_task
                    except Exception:
                        logger.debug("main: Cancel the keep-alive heartbeat task")

                try:
                    await application.stop()
                finally:
                    with contextlib.suppress(Exception):
                        BOT_READY.clear()
                # Close dedicated get_updates client if present
                try:
                    gu = globals().get("GET_UPDATES_BOT")
                    if gu is not None:
                        close_fn = getattr(gu, "close", None)
                        if close_fn:
                            with contextlib.suppress(Exception):
                                await close_fn()
                except Exception:
                    logger.debug("main: Close dedicated get_updates client if present")

        else:
            # Non-ASGI mode: use the context manager as before which manages
            # the application's lifecycle (initialize/start/stop) and blocks
            # on polling or webhook mode until shutdown.
            async with application:
                await application.initialize()
                await application.start()
                with contextlib.suppress(Exception):
                    BOT_READY.set()

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
    from fastapi.responses import FileResponse, Response
    from telegram import Update as TgUpdate

    from web.ws_fastapi import sse_router as _sse_router
    from web.ws_fastapi import start_ws_listener as _start_ws_listener
    from web.ws_fastapi import stop_ws_listener as _stop_ws_listener

    # WebSocket and SSE progress endpoints (same port as main app)
    from web.ws_fastapi import ws_router as _ws_router

    app = FastAPI(title="Media Conversion Bot - PTB v20+")

    app.include_router(_ws_router)
    app.include_router(_sse_router)
    logger.info("WebSocket /ws/{job_id} and SSE /events/{job_id} endpoints registered (same port)")

    # Mount legacy Flask-based web UI (if present) under '/flask' so the
    # web uploader and static UI remain available when running under ASGI/uvicorn.
    try:
        from fastapi.responses import RedirectResponse

        # Use a2wsgi (the official replacement for starlette's deprecated
        # WSGIMiddleware) when available; fall back to starlette otherwise.
        try:
            from a2wsgi import WSGIMiddleware
        except Exception:
            from starlette.middleware.wsgi import WSGIMiddleware

        import web.webapp as flask_webapp

        # Mount the Flask app at /flask (flask routes like /upload become /flask/upload)
        app.mount("/flask", WSGIMiddleware(flask_webapp.app))

        # Provide a compatibility redirect so requests to /upload still work.
        # Accept both GET (browser navigation) and POST (internal server fetch from handlers.py).
        @app.api_route("/upload", methods=["GET", "POST"])
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
                                with contextlib.suppress(Exception):
                                    await r.close()
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
                        resp = {
                            "job_id": job_id,
                            "progress": progress,
                            "message": message,
                            "status": status,
                            "output": out,
                        }
                        try:
                            if job_hash.get("out_bytes") is not None:
                                resp["out_bytes"] = int(job_hash.get("out_bytes"))
                        except Exception:
                            logger.debug("main: operation failed")
                        try:
                            if job_hash.get("in_bytes") is not None:
                                resp["in_bytes"] = int(job_hash.get("in_bytes"))
                        except Exception:
                            logger.debug("main: operation failed")
                        try:
                            if job_hash.get("progress_by_size") is not None:
                                resp["progress_by_size"] = float(job_hash.get("progress_by_size"))
                        except Exception:
                            logger.debug("main: operation failed")
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
                            return {
                                "job_id": job_id,
                                "progress": 100.0,
                                "message": "done",
                                "status": "done",
                                "output": out_path,
                            }

                    # 6) Default queued response
                    return {"job_id": job_id, "progress": 0.0, "message": "queued", "status": "queued"}
                except Exception as e:
                    logger.exception("Diagnostics error: %s", e)
                    return {"error": "Internal error. Check server logs."}

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
                for k in (
                    "REDIS_URL",
                    "WEB_UPLOAD_URL",
                    "UPLOAD_SECRET",
                    "S3_BUCKET",
                    "AWS_ACCESS_KEY_ID",
                    "MONGO_URI",
                    "MONGODB_URL",
                    "MONGODB_URI",
                    "MONGO_URL",
                ):
                    v = os.environ.get(k)
                    if k == "REDIS_URL" and v:
                        result["env"][k] = re.sub(r"(redis://[^:]*:)[^@]+@", r"\1****@", v)
                    elif k == "UPLOAD_SECRET":
                        result["env"][k] = "****" if v else None
                    elif k in ("MONGO_URI", "MONGODB_URL", "MONGODB_URI", "MONGO_URL") and v:
                        # Mask password in mongodb://user:pass@host or mongodb+srv://user:pass@host
                        result["env"][k] = re.sub(r"(mongodb(?:\+srv)?://[^:]*:)[^@]+@", r"\1****@", v)
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
                                with contextlib.suppress(Exception):
                                    await r.close()
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
                                with open(path, encoding="utf-8", errors="replace") as fh:
                                    lines = fh.readlines()[-500:]
                                    result["logs"][fname] = "".join(lines)
                    # Also include worker log if present for quick debugging
                    try:
                        worker_log = os.path.join(tempfile.gettempdir(), "worker.log")
                        if os.path.isfile(worker_log):
                            with open(worker_log, encoding="utf-8", errors="replace") as fh:
                                lines = fh.readlines()[-1000:]
                                result["logs"]["worker.log"] = "".join(lines)
                    except Exception:
                        logger.debug("main: Also include worker log if present for quick debugging")
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
                on the running instance; commands have timeouts to avoid long blocking.
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
                    raise HTTPException(status_code=400, detail="invalid json") from None

                action = payload.get("action")
                filename = payload.get("file")
                job_id = payload.get("job_id")
                out = {"action": action}

                # Resolve ffmpeg/ffprobe paths
                ffprobe = getattr(cfg, "FFPROBE_PATH", None) or (
                    FFMPEG_PATH.replace("ffmpeg", "ffprobe") if "ffmpeg" in FFMPEG_PATH else "ffprobe"
                )
                ffmpeg = FFMPEG_PATH

                # Helper to run commands without blocking the ASGI loop
                async def _run(cmd, timeout=600):
                    try:
                        proc = await asyncio.to_thread(
                            subprocess.run, cmd, capture_output=True, text=True, timeout=timeout
                        )
                        return {"returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
                    except Exception as e:
                        logger.exception("Diagnostic hash fetch error: %s", e)
                        return {"error": "Failed to fetch job hash"}

                # Sanitize file -> only allow basename under storage/input
                input_dir = getattr(cfg, "INPUT_PATH", os.path.join(os.getcwd(), "storage", "input"))
                if filename:
                    safe_name = os.path.basename(filename)
                    target_path = os.path.join(input_dir, safe_name)
                    if not os.path.isfile(target_path):
                        raise HTTPException(status_code=404, detail=f"file not found: {safe_name}")

                if action == "ffprobe":
                    cmd = [
                        ffprobe,
                        "-v",
                        "error",
                        "-print_format",
                        "json",
                        "-show_format",
                        "-show_streams",
                        target_path,
                    ]
                    out["result"] = await _run(cmd, timeout=60)
                    return out

                if action == "remux":
                    dst = payload.get("out") or os.path.join(
                        os.getcwd(), "storage", "temp", os.path.splitext(os.path.basename(target_path))[0] + ".mkv"
                    )
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    cmd = [ffmpeg, "-y", "-hide_banner", "-loglevel", "error", "-i", target_path, "-c", "copy", dst]
                    out["dst"] = dst
                    out["result"] = await _run(cmd, timeout=600)
                    return out

                if action == "reencode":
                    dst = payload.get("out") or os.path.join(
                        os.getcwd(),
                        "storage",
                        "output",
                        os.path.splitext(os.path.basename(target_path))[0] + "_reencoded.mp4",
                    )
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    cmd = [
                        ffmpeg,
                        "-y",
                        "-hide_banner",
                        "-loglevel",
                        "error",
                        "-fflags",
                        "+genpts",
                        "-i",
                        target_path,
                        "-c:v",
                        "libx264",
                        "-preset",
                        "veryfast",
                        "-crf",
                        "23",
                        "-c:a",
                        "aac",
                        "-b:a",
                        "128k",
                        dst,
                    ]
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
                                    with open(path, encoding="utf-8", errors="replace") as fh:
                                        logs[fname] = "".join(fh.readlines()[-lines:])
                        worker_log = os.path.join(tempfile.gettempdir(), "worker.log")
                        if os.path.isfile(worker_log):
                            with open(worker_log, encoding="utf-8", errors="replace") as fh:
                                logs[os.path.basename(worker_log)] = "".join(fh.readlines()[-(lines * 5) :])
                    except Exception as e:
                        logger.exception("Failed to fetch job metadata: %s", e)
                        raise HTTPException(
                            status_code=500, detail="Failed to fetch job metadata. Check server logs for details."
                        ) from e
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
                            with contextlib.suppress(Exception):
                                await r.close()
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
                                            logger.debug("main: operation failed")

                            # If input path provided, compute expected lock key and remove it
                            if input_path_provided:
                                try:
                                    lock_hash = hashlib.sha256(input_path_provided.encode()).hexdigest()
                                    lock_key = f"ffmpeg:lock:{lock_hash}"
                                    try:
                                        await r.delete(lock_key)
                                        removed.append(lock_key)
                                    except Exception:
                                        logger.debug("main: operation failed")
                                except Exception:
                                    logger.debug(
                                        "main: If input path provided, compute expected lock key and remove it"
                                    )
                        finally:
                            with contextlib.suppress(Exception):
                                await r.close()
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
                        from utils.job_queue import JOB_LIST, get_redis

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
                                        logger.debug("main: operation failed")
                        finally:
                            with contextlib.suppress(Exception):
                                await r.close()
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
                            with contextlib.suppress(Exception):
                                await r.close()
                    except Exception as e:
                        logger.exception("Failed to fetch job metadata: %s", e)
                        raise HTTPException(
                            status_code=500, detail="Failed to fetch job metadata. Check server logs for details."
                        ) from e
                    return out

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

        # ── Redis key counts for observability ──
        redis_keys = {"job": -1, "lock": -1, "pipeline_dedup": -1}
        try:
            from utils.job_queue import get_redis as _health_r

            _hr = await _health_r()
            try:
                redis_keys["job"] = 0
                redis_keys["lock"] = 0
                redis_keys["pipeline_dedup"] = 0
                _cursor = 0
                while True:
                    _cursor, _keys = await _hr.scan(_cursor, match="ffmpeg:job:*", count=500)
                    redis_keys["job"] += len(_keys)
                    if _cursor == 0:
                        break
                _cursor = 0
                while True:
                    _cursor, _keys = await _hr.scan(_cursor, match="ffmpeg:lock:*", count=500)
                    redis_keys["lock"] += len(_keys)
                    if _cursor == 0:
                        break
                _cursor = 0
                while True:
                    _cursor, _keys = await _hr.scan(_cursor, match="ffmpeg:pipeline_dedup:*", count=500)
                    redis_keys["pipeline_dedup"] += len(_keys)
                    if _cursor == 0:
                        break
            finally:
                with contextlib.suppress(Exception):
                    await _hr.close()
        except Exception:
            pass

        # Shared pipe report (status/redis/broker/eventbus) plus this bot's own
        # readiness fields. collect_health never raises and bounds every probe, so
        # a dead Redis or a hung broker degrades a field instead of the endpoint.
        from utils.health import collect_health

        try:
            payload = await collect_health()
        except Exception:
            logger.exception("health: probe collection failed")
            payload = {"status": "degraded", "redis": None, "broker": None, "eventbus": None}

        # A Telegram flood window is otherwise invisible: the bot looks healthy
        # while every send to one chat is being refused, and the only way to tell
        # was to read the logs. Same for the deliveries waiting on one.
        flood = {"open": 0, "scopes": {}, "deferred_deliveries": -1}
        try:
            from utils.rate_limiter import telegram_flood_gate

            windows = {scope: left for scope, left in telegram_flood_gate.snapshot().items() if left > 0}
            flood["open"] = len(windows)
            flood["scopes"] = dict(sorted(windows.items(), key=lambda item: -item[1])[:10])
        except Exception:
            logger.debug("health: flood gate snapshot failed")
        try:
            from utils import deferred_delivery

            flood["deferred_deliveries"] = await deferred_delivery.pending()
        except Exception:
            logger.debug("health: deferred delivery count failed")

        # Always HTTP 200: this endpoint backs the platform healthcheck and the
        # keep-alive ping, so a degraded pipe must be visible in the body rather
        # than restarting a container that is still converting files.
        payload.update(
            {
                "bot_initialized": BOT_APPLICATION is not None,
                "bot_ready": BOT_READY.is_set(),
                "dispatcher_ready": dispatcher_ready,
                "startup_time": BOT_STARTED_AT,
                "error": getattr(app.state, "startup_error", None),
                "redis_keys": redis_keys,
                "telegram_flood": flood,
            }
        )
        return payload

    @app.get("/")
    async def root_index(request: Request):
        """Root endpoint: redirects browsers to the web UI, returns health JSON for API clients."""
        accept = request.headers.get("accept", "")
        if "text/html" in accept or "application/xhtml" in accept:
            try:
                from fastapi.responses import RedirectResponse

                return RedirectResponse(url="/flask/")
            except Exception:
                pass
        try:
            return {"status": "ok", "bot_ready": bool(BOT_READY.is_set())}
        except Exception:
            return {"status": "ok"}

    @app.get("/events/{job_id}")
    async def events_sse(job_id: str):
        """Server-Sent Events endpoint streaming real-time conversion progress from Redis."""
        import json as _rj

        from fastapi.responses import StreamingResponse

        async def _event_gen():
            # 1) Emit initial job state from Redis
            try:
                from utils.job_queue import get_redis as _get_redis

                _r = await _get_redis()
                if _r:
                    try:
                        _data = await _r.hgetall(f"ffmpeg:job:{job_id}")
                    finally:
                        await _r.close()
                    if _data:
                        _decoded = {
                            _k.decode() if isinstance(_k, bytes) else _k: _v.decode() if isinstance(_v, bytes) else _v
                            for _k, _v in _data.items()
                        }
                        yield f"data: {_rj.dumps(_decoded)}\n\n"
            except Exception:
                pass

            # 2) Subscribe to Redis pub/sub for live updates
            _red_url = os.environ.get("REDIS_URL")
            if _red_url:
                try:
                    from utils.job_queue import get_redis as _get_redis2

                    _r2 = await _get_redis2()
                    _pub = _r2.pubsub()
                    await _pub.subscribe(f"ffmpeg:progress:{job_id}")
                    while True:
                        try:
                            _msg = await _pub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                            if _msg:
                                _d = _msg.get("data")
                                if isinstance(_d, (bytes, bytearray)):
                                    _d = _d.decode(errors="ignore")
                                if _d:
                                    yield f"data: {_d}\n\n"
                        except TimeoutError:
                            yield ": keepalive\n\n"
                        except Exception:
                            break
                    await _pub.close()
                    await _r2.close()
                except Exception:
                    pass

        return StreamingResponse(_event_gen(), media_type="text/event-stream")

    @app.get("/download/{job_id}")
    async def download_redirect(job_id: str):
        """Redirect to Flask's /flask/download endpoint for file downloads."""
        try:
            from fastapi.responses import RedirectResponse

            return RedirectResponse(url=f"/flask/download/{job_id}")
        except Exception:
            from fastapi.responses import JSONResponse

            return JSONResponse(status_code=500, content={"error": "Download endpoint unavailable"})

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
            raise HTTPException(status_code=500, detail="Webhook validation error") from None
        if not BOT_APPLICATION:
            logger.error("Bot application not initialized")
            raise HTTPException(status_code=503, detail="Bot not initialized")

        try:
            data = await request.json()
            logger.debug(f"Received webhook data: {data}")
        except Exception as e:
            logger.error(f"Invalid JSON in webhook: {e}")
            raise HTTPException(status_code=400, detail="Invalid JSON") from e

        try:
            # Build Update object early so we can retry dispatching even if the
            # application is still initializing.
            update = TgUpdate.de_json(data, BOT_APPLICATION.bot)
            if not update:
                raise ValueError("Failed to create Update object")
            logger.info(f"Received update {getattr(update, 'update_id', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to construct Update: {e}")
            raise HTTPException(status_code=400, detail="Invalid update payload") from e

        # Increment webhook counter
        try:
            with METRICS_LOCK:
                METRICS["webhooks_received"] += 1
        except Exception:
            logger.debug("main: Increment webhook counter")

        # Helper: background retry dispatcher
        async def _background_retry_dispatch(u, attempts=12, delay=0.5):
            for i in range(attempts):
                try:
                    disp = getattr(BOT_APPLICATION, "dispatcher", None)
                    if disp and hasattr(disp, "process_update"):
                        with METRICS_LOCK:
                            METRICS["dispatch_attempts"] += 1
                        await disp.process_update(u)
                    elif BOT_APPLICATION is not None and hasattr(BOT_APPLICATION, "process_update"):
                        # PTB v21+ has no dispatcher; Application.process_update is the
                        # non-deprecated API for feeding updates to the application.
                        with METRICS_LOCK:
                            METRICS["dispatch_attempts"] += 1
                        await BOT_APPLICATION.process_update(u)
                    else:
                        raise RuntimeError("No dispatch path available")
                    with METRICS_LOCK:
                        METRICS["updates_dispatched"] += 1
                    logger.info(f"Background dispatched update {getattr(u, 'update_id', 'unknown')} on attempt {i + 1}")
                    return True
                except Exception as e:
                    logger.debug(f"Background dispatch attempt {i + 1} failed: {e}")
                    with METRICS_LOCK:
                        METRICS["dispatch_failures"] += 1
                await asyncio.sleep(delay)
            logger.error(f"Background dispatch exhausted for update {getattr(u, 'update_id', 'unknown')}")
            return False

        # Try immediate dispatch by scheduling a background dispatch task.
        # Note: PTB v21 removed the dispatcher attribute, so also accept
        # Application.process_update here to avoid pointless retry sleeps.
        attempts = 6
        for i in range(attempts):
            dispatcher = getattr(BOT_APPLICATION, "dispatcher", None)
            has_dispatch_path = bool(dispatcher and hasattr(dispatcher, "process_update")) or (
                BOT_APPLICATION is not None and hasattr(BOT_APPLICATION, "process_update")
            )
            if has_dispatch_path:
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
                    logger.warning(f"Failed to schedule dispatch task (attempt {i + 1}): {e}")
                    try:
                        with METRICS_LOCK:
                            METRICS["dispatch_failures"] += 1
                    except Exception:
                        logger.debug("main: operation failed")
            await asyncio.sleep(0.25)

        # Immediate dispatch not successful — schedule background dispatch
        try:
            asyncio.create_task(_dispatch_update_task(update))
            with METRICS_LOCK:
                METRICS["updates_queued"] += 1
            logger.info(
                f"Scheduled background dispatch for update {getattr(update, 'update_id', 'unknown')} after immediate attempts"
            )
            return {"ok": True, "update_id": getattr(update, "update_id", None), "queued": True}
        except Exception as schedule_exc:
            logger.warning(f"Scheduling dispatch failed: {schedule_exc}; scheduling background retry and returning 200")
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
            "# HELP media_bot_updates_queued Total updates handed to background dispatch",
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
                        os.environ.get("API_SESSION")
                        or os.environ.get("TELETHON_SESSION")
                        or os.environ.get("USERBOT_SESSION")
                    ),
                }
            )
        except Exception:
            logger.debug("main: Telethon / userbot readiness diagnostics")

        return info

    @app.on_event("startup")
    async def _start_bot_background():
        # Start WebSocket Redis listener (same-port WebSocket for progress)
        try:
            _start_ws_listener(app)
        except Exception as _ws_e:
            logger.warning("Could not start WebSocket listener: %s", _ws_e)

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

            # If dispatcher isn't available (some hosting variants), start a
            # fallback long-poller that uses getUpdates and dispatches updates
            # via Application.process_update so handlers still run.
            try:
                force_polling_env = os.environ.get("FORCE_POLLING", "").lower() in ("1", "true", "yes")
                dispatcher = getattr(BOT_APPLICATION, "dispatcher", None)
                has_dispatcher_proc = bool(dispatcher and hasattr(dispatcher, "process_update"))
                app_has_proc = hasattr(BOT_APPLICATION, "process_update")
                if force_polling_env or (not has_dispatcher_proc and not app_has_proc):
                    logger.warning(
                        "ASGI startup: starting fallback long-poller (FORCE_POLLING=%s, dispatcher_present=%s)",
                        force_polling_env,
                        has_dispatcher_proc,
                    )

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
                                            with contextlib.suppress(Exception):
                                                sem.release()
                                    if updates:
                                        for u in updates:
                                            try:
                                                if getattr(u, "update_id", None) is not None:
                                                    offset = int(u.update_id) + 1
                                            except Exception:
                                                logger.debug("main: operation failed")
                                            try:
                                                # Dispatch directly via process_update (non-deprecated API)
                                                asyncio.create_task(_dispatch_update_task(u))
                                            except Exception:
                                                logger.exception("ASGI long-poller failed to schedule update dispatch")
                                    else:
                                        await asyncio.sleep(0.1)
                                except asyncio.CancelledError:
                                    break
                                except (TimedOut, httpx.PoolTimeout) as e:
                                    logger.warning("ASGI long-poller timed out (pool exhausted): %s. Backing off 5s", e)
                                    await asyncio.sleep(5)
                                except Conflict as e:
                                    logger.error(
                                        "ASGI long-poller conflict (another getUpdates active): %s. Stopping long-poller",
                                        e,
                                    )
                                    break
                                except Exception as e:
                                    logger.exception("ASGI long-poller error: %s", e)
                                    await asyncio.sleep(1)
                        except Exception:
                            logger.exception("ASGI long-poller fatal error")

                    try:
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
        # Stop WebSocket Redis listener and close connections (same-port WS)
        try:
            await _stop_ws_listener(app)
        except Exception as _ws_e:
            logger.warning("Error stopping WebSocket listener: %s", _ws_e)

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
            with contextlib.suppress(Exception):
                globals()["LONG_POLLER_STARTED"] = False
        # Close dedicated get_updates client if present
        try:
            gu = globals().get("GET_UPDATES_BOT")
            if gu is not None:
                close_fn = getattr(gu, "close", None)
                if close_fn:
                    with contextlib.suppress(Exception):
                        await close_fn()
        except Exception as e:
            logger.warning(f"Error closing GET_UPDATES_BOT: {e}")

except Exception as e:
    logger.warning(f"FastAPI not available or import failed: {e}")
    app = None
