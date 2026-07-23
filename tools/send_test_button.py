#!/usr/bin/env python3
import os
import sys

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup


def main():
    token = os.environ.get("BOT_TOKEN")
    chat = os.environ.get("TEST_CHAT_ID")
    if not token or not chat:
        print("Usage: set BOT_TOKEN and TEST_CHAT_ID environment variables and run this script")
        print("PowerShell example:")
        print("  $env:BOT_TOKEN='123:ABC' ; $env:TEST_CHAT_ID='123456789' ; python tools/send_test_button.py")
        sys.exit(1)

    bot = Bot(token=token)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_job:TEST")]])
    try:
        msg = bot.send_message(chat_id=int(chat), text="Test progress — cancel button", reply_markup=kb)
        print("Sent test message id:", getattr(msg, 'message_id', None))
    except Exception as e:
        print("Failed to send test message:", e)


if __name__ == "__main__":
    main()
