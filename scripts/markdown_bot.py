#!/usr/bin/env python3
"""Minimal Telegram bot that converts markdown-style links [Title](URL)
to HTML anchors and sends them (useful to reveal MP4 filenames on mobile).

Set environment variable `TELEGRAM_BOT_TOKEN` before running.
"""
import os
import re
import html
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "<YOUR_BOT_TOKEN>")


def format_courses_html(raw_text: str) -> str:
    pattern = r"\[([^\]]+)\]\(([^)]+)\)"

    def repl(match):
        title = match.group(1)
        url = match.group(2)
        safe_title = html.escape(title)
        safe_url = html.escape(url)
        return f'<a href="{safe_url}">{safe_title}</a>\n'

    formatted = re.sub(pattern, repl, raw_text)
    return formatted


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hello! Send me a message containing markdown links like [Title](URL),\n"
        "and I'll format them so Telegram shows clickable titles on mobile."
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw_text = update.message.text or ""
    formatted_text = format_courses_html(raw_text)
    if not formatted_text.strip():
        await update.message.reply_text("No markdown links detected.")
        return

    await update.message.reply_text(
        formatted_text,
        parse_mode="HTML",
        disable_web_page_preview=False,
    )


def main():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
    print("Bot is running (press Ctrl+C to stop).")
    app.run_polling()


if __name__ == "__main__":
    main()
