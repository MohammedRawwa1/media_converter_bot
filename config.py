# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Telegram Bot Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", None)
WEBHOOK_URL = os.getenv("WEBHOOK_URL", None)

# Validate BOT_TOKEN
if not BOT_TOKEN:
    import warnings
    warnings.warn("BOT_TOKEN not set in environment variables. Bot will not start without it.")

# File Size Limits
MAX_FILE_SIZE = 4 * 1024**3  # 4GB
MAX_OUTPUT_SIZE = 2 * 1024**3  # 2GB
MAX_CONCURRENT_TASKS = 3

# Storage Paths
STORAGE_PATH = "storage"
INPUT_PATH = os.path.join(STORAGE_PATH, "input")
OUTPUT_PATH = os.path.join(STORAGE_PATH, "output")
TEMP_PATH = os.path.join(STORAGE_PATH, "temp")
THUMBNAIL_PATH = os.path.join(STORAGE_PATH, "thumbnails")

# FFmpeg Settings
FFMPEG_PATH = "ffmpeg"
FFPROBE_PATH = "ffprobe"

# Compression Presets
COMPRESSION_PRESETS = {
    "high": {"crf": 18, "preset": "slow"},
    "medium": {"crf": 23, "preset": "medium"},
    "low": {"crf": 28, "preset": "fast"},
    "extreme": {"crf": 35, "preset": "veryfast"}
}

# Audio Quality Presets
AUDIO_BITRATES = {
    "best": "320k",
    "high": "256k",
    "medium": "192k",
    "low": "128k"
}

# Supported Formats
SUPPORTED_FORMATS = {
    "video": [".mp4", ".avi", ".mov", ".mkv", ".flv", ".wmv", ".webm", ".m4v", ".3gp"],
    "audio": [".mp3", ".wav", ".aac", ".flac", ".ogg", ".m4a", ".wma", ".opus"],
    "image": [".jpg", ".jpeg", ".png", ".gif", ".bmp"]
}

# Access control for private bots: comma-separated user IDs or single admin ID
# Example: ALLOWED_USER_IDS="12345678,87654321"
def _parse_id_list(env_value: str):
    if not env_value:
        return set()
    parts = [p.strip() for p in env_value.replace(';', ',').split(',') if p.strip()]
    ids = set()
    for p in parts:
        try:
            ids.add(int(p))
        except Exception:
            continue
    return ids

ALLOWED_USER_IDS = _parse_id_list(os.getenv("ALLOWED_USER_IDS", ""))
_admin = os.getenv("ADMIN_USER_ID", None)
ADMIN_USER_ID = int(_admin) if _admin and _admin.isdigit() else None

def is_user_allowed(user_id: int) -> bool:
    """Return True if the user is allowed to use the bot.

    If no ACL is configured (empty `ALLOWED_USER_IDS` and no `ADMIN_USER_ID`),
    returns True to preserve default behavior.
    """
    if not ALLOWED_USER_IDS and ADMIN_USER_ID is None:
        return True
    if ADMIN_USER_ID and user_id == ADMIN_USER_ID:
        return True
    return user_id in ALLOWED_USER_IDS

# Try to load persisted allowed users from storage/allowed_users.json
_persist_path = os.path.join(STORAGE_PATH, "allowed_users.json")
try:
    if os.path.exists(_persist_path):
        import json
        with open(_persist_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                for x in data:
                    try:
                        ALLOWED_USER_IDS.add(int(x))
                    except Exception:
                        continue
except Exception:
    # Ignore errors while loading persisted ACL
    pass

def persist_allowed_users(path: str = None):
    """Persist current `ALLOWED_USER_IDS` set to disk.

    Returns the path written or None on failure.
    """
    try:
        import json
        p = path or _persist_path
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(sorted(list(ALLOWED_USER_IDS)), f)
        return p
    except Exception:
        return None
