# main.py
"""
Main entry point for media conversion bot.
"""

import asyncio
import logging
import signal
import sys
import subprocess
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
    ContextTypes
)
from telegram.error import TelegramError

from config import BOT_TOKEN, WEBHOOK_URL, MAX_FILE_SIZE, FFMPEG_PATH, ALLOWED_USER_IDS, ADMIN_USER_ID, is_user_allowed, persist_allowed_users
import config as cfg
from utils import (
    ensure_directories,
    MediaMenuBuilder,
    progress_tracker,
    cleanup_directory
)
from utils.rate_limiter import TelegramAPIRateLimiter, ConversionRateLimiter
from utils.webhook_monitor import WebhookRecoveryManager
from utils.error_handler import (
    setup_comprehensive_logging,
    get_error_handler,
    handle_conversion_error
)
from handlers import EnhancedMediaHandler
from tasks import (
    convert_video_to_mp3,
    compress_video,
    extract_audio,
    merge_videos,
    merge_audios,
    take_screenshot,
    change_resolution,
    trim_media,
    repair_video,
    optimize_video,
    create_thumbnail_grid,
    generate_sample,
    extract_streams,
    convert_audio_format,
    adjust_bitrate,
    normalize_audio,
    extract_subtitles,
    edit_metadata,
    create_archive,
    CleanupManager,
    start_cleanup_task,
    stop_cleanup_task
)

# Configure comprehensive logging with rotation
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
# Bot application handle for metrics and introspection when started under ASGI
BOT_APPLICATION = None
BOT_STARTED_AT = None
START_TIME = time.time()

def check_ffmpeg_available() -> bool:
    """Return True if ffmpeg is callable from PATH or configured FFMPEG_PATH."""
    try:
        proc = subprocess.run([FFMPEG_PATH, "-version"], capture_output=True, text=True, timeout=5)
        return proc.returncode == 0
    except Exception:
        return False

# Setup comprehensive logging with file rotation
try:
    setup_comprehensive_logging(
        log_file="logs/bot.log",
        level=logging.INFO,
        max_bytes=10485760,  # 10MB
        backup_count=5
    )
except Exception as e:
    logger.warning(f"Could not setup rotating file handler: {e}")

# Initialize Sentry if configured via SENTRY_DSN environment variable
try:
    SENTRY_DSN = __import__('os').environ.get('SENTRY_DSN')
    if SENTRY_DSN:
        try:
            import importlib
            sentry_sdk = importlib.import_module('sentry_sdk')
            sentry_sdk.init(dsn=SENTRY_DSN, traces_sample_rate=0.0)
            logger.info("Sentry initialized")
        except Exception as se:
            logger.warning(f"Failed to initialize Sentry: {se}")
except Exception:
    # Best-effort only; do not fail startup if Sentry init cannot be performed
    pass

# Conversation states
SELECT_TIME, SELECT_RESOLUTION, SELECT_BITRATE, MERGE_FILES, CUSTOM_INPUT = range(5)


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

Hello {user_name}! I can help you convert, compress, and process media files.

**📤 How to use:**
1. Send me any media file (video, audio, or document)
2. Choose from the interactive menu
3. Get your processed file!

**⚡ Quick Commands:**
/help - View all features
/convert - Convert file format
/compress - Reduce file size
/merge - Combine multiple files
/info - Show media information
/trim - Cut video/audio segments
/screenshot - Capture frames
/extract - Extract streams
/optimize - Optimize for web/mobile
/cancel - Cancel current operation
/admin - Admin management (if you're the admin)

**Supported Formats:**
🎬 Video: MP4, AVI, MOV, MKV, FLV, WEBM, WMV
🎧 Audio: MP3, WAV, AAC, FLAC, OGG, M4A
📄 Documents: All other file types

Send me a file to get started! 🚀
"""
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=MediaMenuBuilder.get_main_menu(),
        parse_mode='Markdown'
    )
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
    
    await update.message.reply_text(
        help_text,
        parse_mode='Markdown'
    )


async def convert_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Convert command handler."""
    await update.message.reply_text(
        "🔄 **Format Conversion**\n"
        "Send me a media file, then select conversion options from the menu.\n\n"
        "Supported formats:\n"
        "• Video: MP4, AVI, MOV, MKV, FLV, WEBM, WMV, 3GP\n"
        "• Audio: MP3, WAV, AAC, FLAC, OGG, M4A, WMA, OPUS\n\n"
        "Just send a file to get started! 📤",
        parse_mode='Markdown'
    )


async def compress_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Compress command handler."""
    await update.message.reply_text(
        "📉 **Video Compression**\n"
        "Send a video file to reduce its size while maintaining quality.\n\n"
        "**Options available:**\n"
        "• Quality presets (High → Extreme compression)\n"
        "• Resolution reduction (4K → 1080p, etc.)\n"
        "• Custom CRF values (18-51)\n"
        "• Bitrate adjustment\n\n"
        "Send a video to see compression options! 🎬",
        parse_mode='Markdown'
    )


async def merge_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Merge command handler."""
    await update.message.reply_text(
        "🔀 **File Merging**\n"
        "Combine multiple videos or audio files into one.\n\n"
        "**How to use:**\n"
        "1. Send first file\n"
        "2. Send second file\n"
        "3. Send more files (optional)\n"
        "4. Select 'Start Merge' from menu\n\n"
        "Send your first file to begin! 📁",
        parse_mode='Markdown'
    )


async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Info command handler."""
    await update.message.reply_text(
        "📊 **Media Information**\n"
        "Send any media file to get detailed analysis:\n\n"
        "**Information includes:**\n"
        "• File format & size\n"
        "• Duration & bitrate\n"
        "• Video: Resolution, FPS, codec\n"
        "• Audio: Channels, sample rate, codec\n"
        "• Metadata (title, artist, etc.)\n\n"
        "Send a file to analyze! 🔍",
        parse_mode='Markdown'
    )


async def screenshot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Screenshot command handler."""
    await update.message.reply_text(
        "🖼️ **Screenshot Capture**\n"
        "Take screenshots from videos at specific times.\n\n"
        "**Options:**\n"
        "• Single screenshot (start/middle/end/custom)\n"
        "• Multiple screenshots grid (2-20)\n"
        "• Thumbnail grid (3x3, 4x4)\n\n"
        "Send a video to capture frames! 🎞️",
        parse_mode='Markdown'
    )


async def trim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Trim command handler."""
    await update.message.reply_text(
        "✂️ **Media Trimming**\n"
        "Cut videos or audio files to specific segments.\n\n"
        "**How to use:**\n"
        "1. Send a video/audio file\n"
        "2. Select 'Trim' from menu\n"
        "3. Enter start time (HH:MM:SS)\n"
        "4. Enter end time (HH:MM:SS)\n"
        "5. Get trimmed file\n\n"
        "Supports frame-accurate trimming! ⏱️",
        parse_mode='Markdown'
    )


async def extract_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Extract command handler."""
    await update.message.reply_text(
        "🎞️ **Stream Extraction**\n"
        "Extract components from media files:\n\n"
        "**Can extract:**\n"
        "• Audio tracks from videos\n"
        "• Video stream (without audio)\n"
        "• Subtitles (SRT, ASS, VTT)\n"
        "• All streams separately\n\n"
        "Send a file to extract components! 📦",
        parse_mode='Markdown'
    )


async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Upload instruction handler for large files."""
    await update.message.reply_text(
        "📤 **Upload Instructions**\n"
        "For files larger than Telegram limits, provide a direct HTTP(S) download link or upload to a cloud storage (Google Drive, Dropbox, S3) and share the link.\n"
        "You can also split large files locally and send parts, or contact the admin for alternative upload methods.",
        parse_mode='Markdown'
    )


async def optimize_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Optimize command handler."""
    await update.message.reply_text(
        "⚡ **Media Optimization**\n"
        "Optimize files for specific use cases:\n\n"
        "**Presets available:**\n"
        "• 🌐 For Web - Fast loading\n"
        "• 📱 For Mobile - Small size\n"
        "• 📺 For TV - High quality\n"
        "• 💾 For Storage - Max compression\n"
        "• 🔧 Custom - Fine-tune\n\n"
        "Send a file to optimize! 🚀",
        parse_mode='Markdown'
    )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel current operation."""
    await update.message.reply_text(
        "❌ Operation cancelled.\n\n"
        "Send /start to see available options.",
        reply_markup=MediaMenuBuilder.get_main_menu()
    )
    return ConversationHandler.END


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
    if update and hasattr(update, 'effective_user') and update.effective_user:
        user_id = update.effective_user.id
    
    # Log detailed error
    error_info = error_handler_inst.log_error(
        context.error,
        "Telegram Update Processing",
        severity="error",
        user_id=user_id,
        additional_info={
            "update_type": type(update).__name__ if update else "None",
        }
    )
    
    # Log full traceback
    logger.error("Exception while handling an update:", exc_info=context.error)
    
    # Handle Telegram-specific errors
    if isinstance(context.error, TelegramError):
        logger.warning(f"Telegram API error: {context.error}")
    
    # Get user-friendly message
    user_message = error_info['user_message']
    
    # Try to send error message to user
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                user_message,
                reply_markup=MediaMenuBuilder.get_main_menu() if MediaMenuBuilder else None,
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Failed to send error message to user: {e}")
    elif update and update.effective_chat:
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=user_message,
                reply_markup=MediaMenuBuilder.get_main_menu() if MediaMenuBuilder else None,
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Failed to send error to chat: {e}")
    
    # Attempt cleanup if we have context
    if hasattr(context, 'user_data'):
        # Signal any running tasks to stop
        if 'conversion_task' in context.user_data:
            task = context.user_data['conversion_task']
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()


def setup_handlers(application: Application) -> None:
    """Setup all bot handlers."""
    # Initialize handler manager
    handler_manager = EnhancedMediaHandler()
    
    # Command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("convert", convert_command))
    application.add_handler(CommandHandler("compress", compress_command))
    application.add_handler(CommandHandler("merge", merge_command))
    application.add_handler(CommandHandler("info", info_command))
    application.add_handler(CommandHandler("screenshot", screenshot_command))
    application.add_handler(CommandHandler("trim", trim_command))
    application.add_handler(CommandHandler("extract", extract_command))
    application.add_handler(CommandHandler("optimize", optimize_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("upload", upload_command))
    
    # Media file handlers (videos, audio, documents)
    # Use PTB v20+ filter classes to avoid legacy attribute names
    application.add_handler(MessageHandler(filters.Video.ALL, handler_manager.handle_media_message))
    application.add_handler(MessageHandler(filters.Audio.ALL, handler_manager.handle_media_message))
    application.add_handler(MessageHandler(filters.Document.ALL, handler_manager.handle_media_message))
    # Fallback catch-all to help debug message dispatching (non-command updates)
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handler_manager.handle_media_message))
    
    # Callback query handler for menu interactions
    application.add_handler(CallbackQueryHandler(handler_manager.callback_handler))
    # Admin commands (manage allowed users)
    from telegram.ext import CommandHandler
    async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        # Only admin may manage allowed users
        if ADMIN_USER_ID and user_id != ADMIN_USER_ID:
            await update.message.reply_text("Unauthorized: admin only")
            return

        args = context.args if hasattr(context, 'args') else []
        if not args:
            await update.message.reply_text("Usage: /admin add|remove|list <user_id>")
            return

        cmd = args[0].lower()
        if cmd == 'list':
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

        if cmd == 'add':
            cfg.ALLOWED_USER_IDS.add(target)
            persist_allowed_users()
            await update.message.reply_text(f"Added {target} to allowed users")
            return
        if cmd == 'remove':
            cfg.ALLOWED_USER_IDS.discard(target)
            persist_allowed_users()
            await update.message.reply_text(f"Removed {target} from allowed users")
            return

        await update.message.reply_text("Unknown admin command")
    # Register admin command
    application.add_handler(CommandHandler('admin', admin_command))
    
    # Store handler manager in bot_data for access in other handlers
    application.bot_data['handler_manager'] = handler_manager
    
    # Error handler (must be added last)
    application.add_error_handler(error_handler)
    
    logger.info("✅ All handlers registered successfully")


async def shutdown(application: Application):
    """Graceful shutdown handler."""
    logger.info("Starting graceful shutdown...")
    
    try:
        # Cancel all pending tasks
        pending = asyncio.all_tasks()
        pending_count = len(pending)
        
        if pending_count > 0:
            logger.info(f"Cancelling {pending_count} pending tasks...")
            for task in pending:
                if not task.done():
                    task.cancel()
        
        # Wait for cancellation with timeout
        if pending_count > 0:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending, return_exceptions=True),
                    timeout=30
                )
                logger.info("All tasks cancelled gracefully")
            except asyncio.TimeoutError:
                logger.warning("Shutdown timeout - some tasks still running")
        
        # Cleanup resources
        try:
            stop_cleanup_task()
            logger.info("Cleanup manager stopped")
        except Exception as e:
            logger.error(f"Error stopping cleanup manager: {e}")
        
        logger.info("Graceful shutdown complete")
    
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


async def main() -> None:
    """Start the bot."""
    # Validate BOT_TOKEN
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN not set in environment variables!")
        raise ValueError("BOT_TOKEN is required. Set it in .env file.")
    
    # Create the Application
    application = Application.builder().token(BOT_TOKEN).build()
    # Expose application for ASGI metrics and introspection
    global BOT_APPLICATION, BOT_STARTED_AT
    BOT_APPLICATION = application
    BOT_STARTED_AT = time.time()
    # Expose ACL into application context
    try:
        application.bot_data['allowed_user_ids'] = ALLOWED_USER_IDS
        application.bot_data['admin_user_id'] = ADMIN_USER_ID
    except Exception:
        application.bot_data['allowed_user_ids'] = set()
        application.bot_data['admin_user_id'] = None
    
    # Initialize rate limiters
    api_limiter = TelegramAPIRateLimiter()
    conversion_limiter = ConversionRateLimiter(conversions_per_hour=100)
    
    # Attach rate limiters to application context
    application.bot_data['api_rate_limiter'] = api_limiter
    application.bot_data['conversion_rate_limiter'] = conversion_limiter
    
    logger.info("Rate limiters initialized")
    logger.info(f"  - API limit: {TelegramAPIRateLimiter.GENERAL_LIMIT} calls/sec globally")
    logger.info(f"  - Per-user limit: {TelegramAPIRateLimiter.PER_USER_LIMIT} call/sec")
    logger.info(f"  - Conversion limit: 100 conversions/hour per user")
    
    # Setup handlers
    setup_handlers(application)
    
    # Create directories
    await ensure_directories(
        "storage",
        "storage/input",
        "storage/output",
        "storage/temp",
        "storage/thumbnails",
        "logs"
    )
    
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
        ffmpeg_bind = importlib.import_module('ffmpeg')
        logger.info("ffmpeg-python (python binding) is available")
        # Reduce noisy logs from ffmpeg/ffmpeg-python internals where possible
        try:
            logging.getLogger('ffmpeg').setLevel(logging.ERROR)
            logging.getLogger('ffmpeg._core').setLevel(logging.ERROR)
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
    
    # Setup signal handlers for graceful shutdown
    shutdown_event = asyncio.Event()
    running_tasks = set()
    
    def signal_handler(signum, frame):
        """Handle signals (SIGINT, SIGTERM) for graceful shutdown."""
        try:
            signal_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
        except:
            signal_name = str(signum)
        
        logger.warning(f"🛑 Received {signal_name} - initiating graceful shutdown...")
        shutdown_event.set()
    
    try:
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)   # Handle Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # Handle termination
        if hasattr(signal, 'SIGBREAK'):  # Windows
            signal.signal(signal.SIGBREAK, signal_handler)
        
        logger.info("✅ Signal handlers registered (SIGINT, SIGTERM, SIGBREAK)")
    except Exception as e:
        logger.warning(f"⚠️  Could not register signal handlers: {e}")
    
    # Store shutdown event in application context for handlers to access
    application.bot_data['shutdown_event'] = shutdown_event
    application.bot_data['running_tasks'] = running_tasks
    
    async def shutdown_handler():
        """Handle graceful shutdown."""
        logger.info("⏳ Beginning graceful shutdown sequence...")
        
        # Cancel all running conversion tasks
        if running_tasks:
            logger.info(f"📋 Cancelling {len(running_tasks)} running tasks...")
            for task in running_tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to finish cancelling
            try:
                await asyncio.wait(running_tasks, timeout=10)
            except Exception as e:
                logger.error(f"Error waiting for tasks: {e}")
        
        # Stop webhook recovery manager
        if webhook_manager:
            try:
                await webhook_manager.stop()
                logger.info("✅ Webhook recovery manager stopped")
            except Exception as e:
                logger.error(f"Error stopping webhook manager: {e}")
        
        # Stop cleanup manager
        try:
            stop_cleanup_task()
            logger.info("✅ Cleanup manager stopped")
        except Exception as e:
            logger.error(f"Error stopping cleanup manager: {e}")
        
        # Delete webhook to prevent future updates
        if WEBHOOK_URL:
            try:
                await application.bot.delete_webhook()
                logger.info("✅ Telegram webhook deleted")
            except Exception as e:
                logger.error(f"Error deleting webhook: {e}")
        
        logger.info("✅ Graceful shutdown complete")
    
    try:
        # Start the bot
        logger.info("Starting bot...")
        
        if WEBHOOK_URL:
            # Webhook mode with proper async handling
            logger.info(f"🌐 Starting bot in webhook mode: {WEBHOOK_URL}")
            
            # Set webhook for Telegram with proper parameters
            try:
                webhook_info = await application.bot.set_webhook(
                    url=WEBHOOK_URL,
                    allowed_updates=["message", "callback_query", "edited_message"],
                    max_connections=100,
                    drop_pending_updates=False
                )
                logger.info(f"✅ Webhook set successfully: {webhook_info.url}")
            except Exception as e:
                logger.error(f"Failed to set webhook: {e}")
                raise
            
            # Start webhook server
            logger.info("🚀 Starting webhook server on port 8443...")
            await application.run_webhook(
                listen="0.0.0.0",
                port=8443,
                url_path="/telegram",
                webhook_url=WEBHOOK_URL,
                drop_pending_updates=False
            )
        else:
            # Polling mode
            logger.info("🚀 Starting bot in polling mode")
            await application.run_polling(
                allowed_updates=["message", "callback_query", "edited_message"],
                drop_pending_updates=False
            )
            
    except KeyboardInterrupt:
        logger.info("⌨️  Bot interrupted by user (Ctrl+C)")
    except asyncio.CancelledError:
        logger.info("🔄 Bot cancellation requested")
    except Exception as e:
        logger.error(f"❌ Error starting bot: {e}", exc_info=True)
        raise
    finally:
        # Execute graceful shutdown
        logger.info("⏹️  Stopping bot...")
        await shutdown_handler()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application terminated")
    except Exception as e:
        logger.error(f"Fatal error: {e}")

# Expose a minimal FastAPI app so the project can be started with Uvicorn
try:
    from fastapi import FastAPI

    app = FastAPI(title="Media Conversion Bot - Health")

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    # Telegram webhook endpoint: post JSON updates from Telegram here
    from fastapi import Request, HTTPException
    from telegram import Update as TgUpdate

    @app.post("/telegram/webhook")
    async def telegram_webhook(request: Request):
        if not BOT_APPLICATION:
            raise HTTPException(status_code=503, detail="Bot not initialized")
        try:
            data = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON")

        # Build Update object
        try:
            update = TgUpdate.de_json(data, BOT_APPLICATION.bot)
        except Exception:
            try:
                update = TgUpdate(**data)
            except Exception as e:
                logger.error(f"Failed to construct Update: {e}")
                raise HTTPException(status_code=400, detail="Invalid update payload")

        # Enqueue update to the Application for processing
        try:
            # Preferred queue attribute for Application
            if hasattr(BOT_APPLICATION, 'update_queue'):
                await BOT_APPLICATION.update_queue.put(update)
                logger.info("Enqueued Telegram update %s to Application.update_queue", getattr(update, 'update_id', None))
                # Also schedule dispatcher.process_update so updates are handled
                try:
                    if hasattr(BOT_APPLICATION, 'dispatcher') and hasattr(BOT_APPLICATION.dispatcher, 'process_update'):
                        asyncio.create_task(BOT_APPLICATION.dispatcher.process_update(update))
                        logger.debug("Scheduled dispatcher.process_update for update %s", getattr(update, 'update_id', None))
                except Exception as e_sched:
                    logger.debug("Could not schedule dispatcher processing: %s", e_sched)
            elif hasattr(BOT_APPLICATION, 'bot') and hasattr(BOT_APPLICATION.bot, 'process_update'):
                # last-resort synchronous processing (only used if no update_queue exists)
                await BOT_APPLICATION.bot.process_update(update)
            else:
                # Try dispatcher
                if hasattr(BOT_APPLICATION, 'dispatcher') and hasattr(BOT_APPLICATION.dispatcher, 'process_update'):
                    await BOT_APPLICATION.dispatcher.process_update(update)
                else:
                    logger.error("No known method to enqueue updates on Application")
                    raise HTTPException(status_code=500, detail="Server not configured to process updates")
        except Exception as e:
            logger.error(f"Error enqueuing Telegram update: {e}")
            raise HTTPException(status_code=500, detail="Failed to enqueue update")

        return {"ok": True}

    from fastapi import Response

    @app.get("/metrics")
    async def metrics():
        """Return Prometheus-style metrics as plain text."""
        uptime = time.time() - (BOT_STARTED_AT or START_TIME)
        allowed_total = len(ALLOWED_USER_IDS) if ALLOWED_USER_IDS else 0
        ffmpeg_ok = 1 if check_ffmpeg_available() else 0
        active_convs = 0
        try:
            if BOT_APPLICATION and BOT_APPLICATION.bot_data:
                mgr = BOT_APPLICATION.bot_data.get('handler_manager')
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
        ]

        return Response("\n".join(lines), media_type="text/plain; version=0.0.4")

    @app.on_event("startup")
    async def _start_bot_background():
        # Launch main() as a background task so uvicorn also serves ASGI endpoints
        try:
            app.state.bot_task = asyncio.create_task(main())
            logger.info("Background bot task started via ASGI startup event")
        except Exception as e:
            logger.error(f"Failed to start bot in background: {e}")

    @app.on_event("shutdown")
    async def _stop_bot_background():
        # Cancel the background bot task if present
        try:
            task = getattr(app.state, 'bot_task', None)
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.info("Background bot task cancelled on ASGI shutdown")
        except Exception as e:
            logger.error(f"Error stopping background bot task: {e}")

except Exception:
    # FastAPI not available or import failed; skip ASGI app export
    app = None
