import contextlib
import os

import aiofiles
from telegram import Update
from telegram.ext import CallbackContext, CommandHandler

import config


async def add_thumb(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if update.message.reply_to_message and update.message.reply_to_message.photo:
        file_id = update.message.reply_to_message.photo[-1].file_id
        file = await context.bot.get_file(file_id)
        file_bytes = await file.download_as_bytearray()
        # Use configured thumbnail path if available, else fallback
        thumb_dir = getattr(config, "THUMBNAIL_PATH", "storage/thumbnails")
        with contextlib.suppress(Exception):
            os.makedirs(thumb_dir, exist_ok=True)
        thumb_path = os.path.join(thumb_dir, f"{user_id}.jpg")
        async with aiofiles.open(thumb_path, 'wb') as f:
            await f.write(file_bytes)
        await update.message.reply_text("Thumbnail added successfully!")
    else:
        await update.message.reply_text("Please reply to a photo with this command.")

async def del_thumb(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    thumb_dir = getattr(config, "THUMBNAIL_PATH", "storage/thumbnails")
    thumb_path = os.path.join(thumb_dir, f"{user_id}.jpg")
    if os.path.exists(thumb_path):
        with contextlib.suppress(Exception):
            os.remove(thumb_path)
        await update.message.reply_text("Thumbnail deleted successfully!")
    else:
        await update.message.reply_text("You don't have a custom thumbnail set.")

async def setup_thumbnail_handlers(application):
    application.add_handler(CommandHandler("addthumb", add_thumb))
    application.add_handler(CommandHandler("delthumb", del_thumb))
