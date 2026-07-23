import logging
import os

try:
    import config
except Exception:
    config = None

logger = logging.getLogger(__name__)


def setup_bot_directories():
    """Setup all required directories for the bot using configured paths."""
    base = getattr(config, "STORAGE_PATH", "storage") if config else "storage"
    directories = [
        base,
        getattr(config, "INPUT_PATH", os.path.join(base, "input")) if config else os.path.join(base, "input"),
        getattr(config, "OUTPUT_PATH", os.path.join(base, "output")) if config else os.path.join(base, "output"),
        getattr(config, "TEMP_PATH", os.path.join(base, "temp")) if config else os.path.join(base, "temp"),
        getattr(config, "THUMBNAIL_PATH", os.path.join(base, "thumbnails")) if config else os.path.join(base, "thumbnails"),
        "logs",
        os.path.join(base, "temp_sessions"),
    ]

    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Created directory: {directory}")
        except Exception as e:
            logger.error(f"Failed to create directory {directory}: {e}")
            return False
    return True


if __name__ == "__main__":
    ok = setup_bot_directories()
    print("Bot directories setup complete!" if ok else "Failed to setup directories")
