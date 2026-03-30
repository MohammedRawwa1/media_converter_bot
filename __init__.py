# __init__.py (root directory)
"""
Media Conversion Bot - Telegram bot for converting media files
"""

__version__ = "1.0.0"
__author__ = "Media Conversion Bot Team"

# Import configuration (guarded so tests can import package safely)
try:
    from . import config

    AUDIO_BITRATES = getattr(config, "AUDIO_BITRATES", [])
    BOT_TOKEN = getattr(config, "BOT_TOKEN", None)
    COMPRESSION_PRESETS = getattr(config, "COMPRESSION_PRESETS", {})
    FFMPEG_PATH = getattr(config, "FFMPEG_PATH", "ffmpeg")
    FFPROBE_PATH = getattr(config, "FFPROBE_PATH", "ffprobe")
    INPUT_PATH = getattr(config, "INPUT_PATH", "storage/input")
    MAX_CONCURRENT_TASKS = getattr(config, "MAX_CONCURRENT_TASKS", 2)
    MAX_FILE_SIZE = getattr(config, "MAX_FILE_SIZE", 0)
    MAX_OUTPUT_SIZE = getattr(config, "MAX_OUTPUT_SIZE", 0)
    OUTPUT_PATH = getattr(config, "OUTPUT_PATH", "storage/output")
    STORAGE_PATH = getattr(config, "STORAGE_PATH", "storage")
    SUPPORTED_FORMATS = getattr(config, "SUPPORTED_FORMATS", [])
    TEMP_PATH = getattr(config, "TEMP_PATH", "storage/temp")
    THUMBNAIL_PATH = getattr(config, "THUMBNAIL_PATH", "storage/thumbnails")
    WEBHOOK_URL = getattr(config, "WEBHOOK_URL", None)
except Exception:
    # Provide safe defaults for environments (tests, imports) where config
    # may not define all expected values.
    AUDIO_BITRATES = []
    BOT_TOKEN = None
    COMPRESSION_PRESETS = {}
    FFMPEG_PATH = "ffmpeg"
    FFPROBE_PATH = "ffprobe"
    INPUT_PATH = "storage/input"
    MAX_CONCURRENT_TASKS = 2
    MAX_FILE_SIZE = 0
    MAX_OUTPUT_SIZE = 0
    OUTPUT_PATH = "storage/output"
    STORAGE_PATH = "storage"
    SUPPORTED_FORMATS = []
    TEMP_PATH = "storage/temp"
    THUMBNAIL_PATH = "storage/thumbnails"
    WEBHOOK_URL = None

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
