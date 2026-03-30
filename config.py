"""Configuration helpers that read environment variables and persist ACL changes."""

import os
import json
from typing import Set, Optional

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Basic required configuration
BOT_TOKEN = os.getenv("BOT_TOKEN") or ""
WEBHOOK_URL = os.getenv("WEBHOOK_URL") or ""
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET") or os.getenv("TELEGRAM_SECRET_TOKEN") or os.getenv("WEBHOOK_SECRET_TOKEN") or ""

# Storage and limits
STORAGE_PATH = os.getenv("STORAGE_PATH", os.path.join(ROOT_DIR, "storage"))
INPUT_PATH = os.path.join(STORAGE_PATH, "input")
OUTPUT_PATH = os.path.join(STORAGE_PATH, "output")
TEMP_PATH = os.path.join(STORAGE_PATH, "temp")
THUMBNAIL_PATH = os.path.join(STORAGE_PATH, "thumbnails")

MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", "4")) * 1024 * 1024 * 1024
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", "5"))

# Binaries and external services
FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Admin and ACL
_allowed_file = os.path.join(STORAGE_PATH, "allowed_users.json")

def _load_allowed_users() -> Set[int]:
    s: Set[int] = set()
    # First, read from ALLOWED_USER_IDS env var if present
    env_val = os.getenv("ALLOWED_USER_IDS", "")
    if env_val:
        for part in env_val.split(","):
            try:
                v = int(part.strip())
                s.add(v)
            except Exception:
                continue

    # Next, read persisted file if exists
    try:
        if os.path.exists(_allowed_file):
            with open(_allowed_file, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    for v in data:
                        try:
                            s.add(int(v))
                        except Exception:
                            continue
    except Exception:
        # best-effort
        pass

    return s


ALLOWED_USER_IDS: Set[int] = _load_allowed_users()

def persist_allowed_users() -> None:
    """Persist current `ALLOWED_USER_IDS` to the storage file.

    This is a best-effort function; failures are logged by callers.
    """
    try:
        os.makedirs(STORAGE_PATH, exist_ok=True)
        with open(_allowed_file, "w", encoding="utf-8") as fh:
            json.dump(sorted(list(ALLOWED_USER_IDS)), fh)
    except Exception:
        # don't raise - caller should log
        pass


def _parse_optional_int(val: Optional[str]) -> Optional[int]:
    try:
        if val is None or val == "":
            return None
        return int(val)
    except Exception:
        return None


ADMIN_USER_ID = _parse_optional_int(os.getenv("ADMIN_USER_ID", ""))


def is_user_allowed(user_id: int) -> bool:
    """Return True if user is allowed by ACL or if ACL is empty (open bot).

    Admin user is always allowed.
    """
    try:
        if ADMIN_USER_ID and user_id == ADMIN_USER_ID:
            return True
        if not ALLOWED_USER_IDS:
            return True
        return user_id in ALLOWED_USER_IDS
    except Exception:
        return False
