# __init__.py (root directory)
"""
Media Conversion Bot - Telegram bot for converting media files
"""

__version__ = "1.0.0"
__author__ = "Media Conversion Bot Team"

# Import configuration
from .config import (
    AUDIO_BITRATES,
    BOT_TOKEN,
    COMPRESSION_PRESETS,
    FFMPEG_PATH,
    FFPROBE_PATH,
    INPUT_PATH,
    MAX_CONCURRENT_TASKS,
    MAX_FILE_SIZE,
    MAX_OUTPUT_SIZE,
    OUTPUT_PATH,
    STORAGE_PATH,
    SUPPORTED_FORMATS,
    TEMP_PATH,
    THUMBNAIL_PATH,
    WEBHOOK_URL,
)

# Export main components
try:
    from .media_converter import ExtendedMediaConverter
except ImportError:
    ExtendedMediaConverter = None

try:
    from .models import MediaConversionModel
except ImportError:
    MediaConversionModel = None

__all__ = [
    "BOT_TOKEN",
    "WEBHOOK_URL",
    "MAX_FILE_SIZE",
    "MAX_OUTPUT_SIZE",
    "MAX_CONCURRENT_TASKS",
    "STORAGE_PATH",
    "INPUT_PATH",
    "OUTPUT_PATH",
    "TEMP_PATH",
    "THUMBNAIL_PATH",
    "FFMPEG_PATH",
    "FFPROBE_PATH",
    "COMPRESSION_PRESETS",
    "AUDIO_BITRATES",
    "SUPPORTED_FORMATS",
    "ExtendedMediaConverter",
    "MediaConversionModel",
]
