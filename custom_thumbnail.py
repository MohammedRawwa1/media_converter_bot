import os
import shutil
import aiofiles
from PIL import Image
from telegram import Update
from telegram.ext import CallbackContext, CommandHandler
from database.mongo_handler import MongoDB
from handlers.db_connection import get_db

async def add_thumb(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if update.message.reply_to_message and update.message.reply_to_message.photo:
        file_id = update.message.reply_to_message.photo[-1].file_id
        file = await context.bot.get_file(file_id)
        file_path = await file.download_as_bytearray()
        thumb_path = f"thumbnails/{user_id}.jpg"
        os.makedirs(os.path.dirname(thumb_path), exist_ok=True)
        async with aiofiles.open(thumb_path, 'wb') as f:
            await f.write(file_path)
        await update.message.reply_text("Thumbnail added successfully!")
    else:
        await update.message.reply_text("Please reply to a photo with this command.")

async def del_thumb(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    thumb_path = f"thumbnails/{user_id}.jpg"
    if os.path.exists(thumb_path):
        os.remove(thumb_path)
        await update.message.reply_text("Thumbnail deleted successfully!")
    else:
        await update.message.reply_text("You don't have a custom thumbnail set.")

async def setup_thumbnail_handlers(application):
    application.add_handler(CommandHandler("addthumb", add_thumb))
    application.add_handler(CommandHandler("delthumb", del_thumb))
