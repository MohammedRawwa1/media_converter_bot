import os
import logging

logger = logging.getLogger(__name__)


def setup_bot_directories():
    """Setup all required directories for the bot."""
    directories = [
        "storage",
        "storage/input",
        "storage/output",
        "storage/temp",
        "storage/thumbnails",
        "logs",
        "storage/temp_sessions",
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
    setup_bot_directories()
    print("Bot directories setup complete!")
