"""Canonical callback names and helpers for Telegram inline keyboards.

Keep all callback identifiers here as the single source of truth.
"""

# Main menus
MENU_MAIN = "menu_main"
MENU_VIDEO = "menu_video"
MENU_AUDIO = "menu_audio"
MENU_ADVANCED = "menu_advanced"

# Actions
INFO = "info"
HELP = "help"
SEND_FILE = "send_file"

# Conversion/format prefixes
FORMAT_PREFIX = "format_"


def format_key(fmt: str) -> str:
    return f"{FORMAT_PREFIX}{fmt}"


# Compression
COMPRESS_PREFIX = "compress_"


def compress_key(crf: str) -> str:
    return f"{COMPRESS_PREFIX}{crf}"


# Resolution
RES_PREFIX = "res_"


def res_key(name: str) -> str:
    return f"{RES_PREFIX}{name}"


# Bitrate
BITRATE_PREFIX = "bitrate_"


def bitrate_key(val: str) -> str:
    return f"{BITRATE_PREFIX}{val}"


# Screenshot
SCREENSHOT_PREFIX = "screenshot_"


def screenshot_key(opt: str) -> str:
    return f"{SCREENSHOT_PREFIX}{opt}"


# Merge
MERGE_ADD = "merge_add"
MERGE_VIEW = "merge_view"
MERGE_CLEAR = "merge_clear"
MERGE_VIDEOS_START = "merge_videos_start"
MERGE_AUDIOS_START = "merge_audios_start"

# Other utilities
COMPRESS_MENU = "compress_menu"
RESOLUTION_MENU = "resolution_menu"
BITRATE_MENU = "bitrate_menu"
SCREENSHOTS_MENU = "screenshots_menu"
OPTIMIZE_MENU = "optimize_menu"
CONVERT_FORMAT_MENU = "convert_format_menu"
EXTRACT_AUDIO = "extract_audio"
EXTRACT_STREAMS = "extract_streams"
EXTRACT_SUBTITLES = "extract_subtitles"
EXTRACT_ALL_STREAMS = "extract_all_streams"
REMOVE_AUDIO = "remove_audio"
ADD_AUDIO = "merge_av_menu"
OPTIMIZE_PREFIX = "optimize_"
SAMPLE = "generate_sample"
THUMBNAIL_GRID = "thumbnail_grid"
EDIT_METADATA = "edit_metadata"
NORMALIZE_AUDIO = "normalize_audio"
TRIM_VIDEO = "trim_video"
TRIM_AUDIO = "trim_audio"

# Optimization presets
OPTIMIZE_WEB = "optimize_web"
OPTIMIZE_MOBILE = "optimize_mobile"
OPTIMIZE_TV = "optimize_tv"
OPTIMIZE_STORAGE = "optimize_storage"
OPTIMIZE_CUSTOM = "optimize_custom"

# Extraction variants
EXTRACT_AUDIO_ONLY = "extract_audio_only"
EXTRACT_VIDEO_ONLY = "extract_video_only"
EXTRACT_ALL = "extract_all"

# Merge/audio helpers
MERGE_AUDIO = "merge_audio"

# Other utilities found in keyboards
CREATE_ARCHIVE = "create_archive"
REPAIR_VIDEO = "repair_video"
FADE_MENU = "fade_menu"
FRAMERATE_MENU = "framerate_menu"

# Confirm/cancel
CONFIRM = "confirm"
CANCEL = "cancel"
