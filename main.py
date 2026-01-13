# main.py
import asyncio
import logging
import time
import subprocess
from fastapi import FastAPI, Request, HTTPException
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import TelegramError

from config import (
    BOT_TOKEN,
    WEBHOOK_URL,
    ALLOWED_USER_IDS,
    ADMIN_USER_ID,
    is_user_allowed,
    persist_allowed_users,
)
from utils import ensure_directories, MediaMenuBuilder
from handlers import EnhancedMediaHandler

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Globals
# -------------------------------------------------------------------
BOT_APPLICATION: Application | None = None
BOT_STARTED_AT = time.time()

# -------------------------------------------------------------------
# Command handlers
# -------------------------------------------------------------------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not is_user_allowed(user_id):
        await update.message.reply_text("Access denied. This bot is private.")
        return

    await update.message.reply_text(
        "🎬 Welcome to Media Conversion Bot!",
        reply_markup=MediaMenuBuilder.get_main_menu(),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Send a media file to get started.")


# -------------------------------------------------------------------
# Error handler
# -------------------------------------------------------------------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled exception", exc_info=context.error)
    if update and getattr(update, "effective_message", None):
        try:
            await update.effective_message.reply_text("❌ An error occurred.")
        except Exception:
            pass


# -------------------------------------------------------------------
# Handlers setup
# -------------------------------------------------------------------
def setup_handlers(app: Application):
    media_handler = EnhancedMediaHandler()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))

    app.add_handler(MessageHandler(filters.VIDEO, media_handler.handle_media_message))
    app.add_handler(MessageHandler(filters.AUDIO, media_handler.handle_media_message))
    app.add_handler(MessageHandler(filters.DOCUMENT, media_handler.handle_media_message))

    app.add_handler(CallbackQueryHandler(media_handler.callback_handler))
    app.add_error_handler(error_handler)

    logger.info("✅ Handlers registered")


# -------------------------------------------------------------------
# FastAPI app
# -------------------------------------------------------------------
app = FastAPI(title="Media Conversion Bot")

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    if not BOT_APPLICATION:
        raise HTTPException(status_code=503, detail="Bot not initialized")

    data = await request.json()
    update = Update.de_json(data, BOT_APPLICATION.bot)

    # THIS IS THE CRITICAL LINE
    await BOT_APPLICATION.process_update(update)

    return {"ok": True}


# -------------------------------------------------------------------
# Startup / Shutdown
# -------------------------------------------------------------------
@app.on_event("startup")
async def startup():
    global BOT_APPLICATION

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")

    logger.info("🚀 Starting Telegram bot")

    application = Application.builder().token(BOT_TOKEN).build()
    setup_handlers(application)

    await ensure_directories(
        "storage",
        "storage/input",
        "storage/output",
        "storage/temp",
        "logs",
    )

    await application.initialize()
    await application.start()

    # Set webhook
    await application.bot.set_webhook(
        url=f"{WEBHOOK_URL}/telegram/webhook"
    )

    BOT_APPLICATION = application
    logger.info("✅ Bot started and webhook set")


@app.on_event("shutdown")
async def shutdown():
    global BOT_APPLICATION

    if BOT_APPLICATION:
        logger.info("🛑 Shutting down bot")
        await BOT_APPLICATION.stop()
        await BOT_APPLICATION.shutdown()
        BOT_APPLICATION = None
