import contextlib
import logging
import os

import aiofiles
from telegram import Update
from telegram.error import RetryAfter
from telegram.ext import CallbackContext, CommandHandler

import config
from utils.confirm import confirm_keyboard

logger = logging.getLogger(__name__)


async def add_thumb(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if update.message.reply_to_message and update.message.reply_to_message.photo:
        file_id = update.message.reply_to_message.photo[-1].file_id
        # get_file/download are Bot API calls drawn from the chat's flood budget.
        # A RetryAfter here means Telegram is already refusing writes, so answer
        # once with something the user can act on rather than retrying into it.
        try:
            file = await context.bot.get_file(file_id)
            file_bytes = await file.download_as_bytearray()
        except RetryAfter as exc:
            logger.warning(
                "add_thumb: Telegram flood wait (%ss) for user %s",
                getattr(exc, "retry_after", "?"),
                user_id,
            )
            await update.message.reply_text(
                "Telegram is rate-limiting this chat right now — please try again in a moment."
            )
            return
        # Use configured thumbnail path if available, else fallback
        thumb_dir = getattr(config, "THUMBNAIL_PATH", "storage/thumbnails")
        with contextlib.suppress(Exception):
            os.makedirs(thumb_dir, exist_ok=True)
        thumb_path = os.path.join(thumb_dir, f"{user_id}.jpg")
        async with aiofiles.open(thumb_path, "wb") as f:
            await f.write(file_bytes)
        await update.message.reply_text("Thumbnail added successfully!")
    else:
        await update.message.reply_text("Please reply to a photo with this command.")


async def del_thumb(update: Update, context: CallbackContext):
    """Ask before deleting the caller's custom thumbnail.

    The file is not recoverable from the bot, so a stray command would silently
    change the thumbnail of every later conversion - hence the inline Yes/No
    rather than an immediate delete.
    """
    await update.message.reply_text(
        "⚠️ *Delete your custom thumbnail*?\nLater conversions will fall back to the default thumbnail.",
        parse_mode="Markdown",
        reply_markup=confirm_keyboard("delthumb"),
    )


async def perform_del_thumb(reply, update: Update, context: CallbackContext):
    """Delete the thumbnail once the user has confirmed (via :func:`del_thumb`)."""
    user_id = update.effective_user.id
    thumb_dir = getattr(config, "THUMBNAIL_PATH", "storage/thumbnails")
    thumb_path = os.path.join(thumb_dir, f"{user_id}.jpg")
    if os.path.exists(thumb_path):
        with contextlib.suppress(Exception):
            os.remove(thumb_path)
        await reply.say("✅ Thumbnail deleted successfully!")
    else:
        await reply.say("You don't have a custom thumbnail set.")


async def setup_thumbnail_handlers(application):
    application.add_handler(CommandHandler("addthumb", add_thumb))
    application.add_handler(CommandHandler("delthumb", del_thumb))
