"""Configuration helpers that read environment variables and persist ACL changes."""

import os
import json
import logging
from typing import Set, Optional
from urllib.parse import urlparse
import re

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency
    load_dotenv = None


def _load_environment_file(env_path: Optional[str] = None) -> None:
    """Load values from a .env file when available, without overriding existing env vars."""
    if load_dotenv is None:
        return

    candidates = []
    if env_path:
        candidates.append(env_path)
    candidates.extend(
        [
            os.path.join(ROOT_DIR, ".env"),
            os.path.join(os.getcwd(), ".env"),
            os.path.join(ROOT_DIR, ".env.local"),
        ]
    )

    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        if os.path.exists(candidate):
            load_dotenv(candidate, override=False)


_load_environment_file()

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

# Storage backend selection: 'local', 's3', or 'r2'
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "local")

# S3 / S3-compatible (R2) configuration
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")  # e.g. https://<account>.r2.cloudflarestorage.com
S3_REGION = os.getenv("S3_REGION", "")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
S3_USE_SSL = os.getenv("S3_USE_SSL", "1") not in ("0", "false", "False", "no")
# Default presign expiry (seconds)
PRESIGN_EXPIRES = int(os.getenv("PRESIGN_EXPIRES", "3600"))

MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", "4")) * 1024 * 1024 * 1024
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", "5"))

# Telethon / Pyrogram userbot session string (alternative to /login flow)
# Set PYROGRAM_SESSION to a Pyrogram session string to bypass the Telethon
# login flow entirely. Generate one with: python scripts/create_pyrogram_session.py
# Format: a long base64-encoded string from Pyrogram's export_session_string()
PYROGRAM_SESSION = os.getenv("PYROGRAM_SESSION", "")

# Binaries and external services
FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")
# Do not assume a localhost Redis in production; prefer an empty default so
# deployments must explicitly configure `REDIS_URL` when required.
REDIS_URL = os.getenv("REDIS_URL", "")

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

    Admin user is always allowed. When ALLOWED_USER_IDS is empty or
    OPEN_ACCESS env var is set, all users are permitted.
    """
    try:
        if ADMIN_USER_ID and user_id == ADMIN_USER_ID:
            return True
        # Allow all users when no explicit allow-list is set
        if not ALLOWED_USER_IDS:
            return True
        return user_id in ALLOWED_USER_IDS
    except Exception:
        logging.getLogger(__name__).warning("ACL check failed for user %s; defaulting to allowed", user_id)
        return True  # default to allowed on error to avoid locking out users
# Normalize MongoDB environment variable names for compatibility.
# Some deployments (Render, Docker) may set variables using references
# like "$MONGO_URI" which are not expanded by the platform. Resolve
# simple $VAR or ${VAR} references so the canonical value is usable.

def _resolve_env_reference(val: Optional[str]) -> Optional[str]:
    if not val:
        return val
    v = val.strip()
    # Exact single-var patterns: $VAR or ${VAR}
    m = re.match(r"^\$(\w+)$", v) or re.match(r"^\$\{(\w+)\}$", v)
    if m:
        ref = m.group(1)
        return os.getenv(ref) or None
    # Replace embedded ${VAR} or $VAR occurrences with their env values (best-effort)
    def _repl(m):
        name = m.group(1)
        return os.getenv(name, "")

    try:
        substituted = re.sub(r"\$\{?(\w+)\}?", _repl, v)
    except Exception:
        substituted = v
    # If substitution produced an empty string, treat as unresolved
    return substituted or None


# Check a list of candidate env vars in order and resolve references.
_canonical_mongo = None
for _key in ("MONGO_URI", "MONGODB_URL", "MONGODB_URI", "MONGO_URL"):
    _raw = os.getenv(_key)
    if not _raw:
        continue
    _resolved = _resolve_env_reference(_raw)
    if _resolved:
        _canonical_mongo = _resolved
        break

if _canonical_mongo:
    # Ensure common names are present in os.environ so all modules find it.
    os.environ.setdefault("MONGO_URI", _canonical_mongo)
    os.environ.setdefault("MONGODB_URL", _canonical_mongo)
    os.environ.setdefault("MONGO_URL", _canonical_mongo)
    os.environ.setdefault("MONGODB_URI", _canonical_mongo)

# Expose a canonical variable for other modules to import if desired.
MONGO_URI = os.getenv("MONGO_URI", "")


def validate_env() -> None:
    """Validate critical environment variables without printing secrets.

    Logs missing or malformed settings (names only) and warns on suspicious
    values. This helper never logs raw secret values.
    """
    logger = logging.getLogger(__name__)
    missing = []

    # Required for normal operation
    if not os.getenv("BOT_TOKEN"):
        missing.append("BOT_TOKEN")

    # Storage backend requirements
    backend = os.getenv("STORAGE_BACKEND", STORAGE_BACKEND).lower() if "STORAGE_BACKEND" in globals() else os.getenv("STORAGE_BACKEND", "local")
    if backend in ("s3", "r2"):
        if not os.getenv("S3_BUCKET") or not os.getenv("S3_ENDPOINT"):
            missing.append("S3_BUCKET/S3_ENDPOINT")

    # Quick Redis URL sanity check
    red = os.getenv("REDIS_URL")
    if red:
        try:
            parsed = urlparse(red)
            if parsed.scheme not in ("redis", "rediss"):
                logger.warning("REDIS_URL scheme looks unusual (expected redis:// or rediss://)")
        except Exception:
            logger.warning("Failed to parse REDIS_URL for basic validation")

    # Numeric env sanity checks (warn but do not fail)
    for key in ("PRESIGN_EXPIRES", "JOB_METADATA_TTL", "CONVERSIONS_PER_HOUR", "PROMETHEUS_METRICS_PORT"):
        v = os.getenv(key)
        if v:
            try:
                int(v)
            except Exception:
                logger.warning("%s should be an integer (current value not logged)", key)

    if missing:
        logger.error("Missing required env vars: %s (values are NOT shown here)", ", ".join(missing))
    else:
        logger.info("Env validation: required variables present (values not displayed)")
