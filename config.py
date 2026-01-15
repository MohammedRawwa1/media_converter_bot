# config.py - Basic configuration for media conversion bot
import os
from typing import Set

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", "4")) * 1024 * 1024 * 1024
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", "5"))

STORAGE_PATH = os.getenv("STORAGE_PATH", "storage")
INPUT_PATH = os.path.join(STORAGE_PATH, "input")
OUTPUT_PATH = os.path.join(STORAGE_PATH, "output")
TEMP_PATH = os.path.join(STORAGE_PATH, "temp")
THUMBNAIL_PATH = os.path.join(STORAGE_PATH, "thumbnails")

FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))

ALLOWED_USER_IDS: Set[int] = set()
allowed_users_env = os.getenv("ALLOWED_USER_IDS", "")
if allowed_users_env:
    ALLOWED_USER_IDS.update(int(uid.strip()) for uid in allowed_users_env.split(",") if uid.strip())

def is_user_allowed(user_id: int) -> bool:
    if not ALLOWED_USER_IDS:
        return True
    return user_id in ALLOWED_USER_IDS
