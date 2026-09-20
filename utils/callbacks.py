"""Canonical callback names and helpers for Telegram inline keyboards.

Keep all callback identifiers here as the single source of truth.
"""

# Main menus
MENU_MAIN = "menu_main"
MENU_VIDEO = "menu_video"
MENU_AUDIO = "menu_audio"
MENU_ADVANCED = "menu_advanced"
# Shorthand to show the merge menu (used by main menu)
MERGE_MENU = "merge_videos_menu"

# Actions
INFO = "info"
HELP = "help"
SEND_FILE = "send_file"

# Conversion/format prefixes
FORMAT_PREFIX = "format_"


def format_key(fmt: str) -> str:
    return f"{FORMAT_PREFIX}{fmt}"


# Video format conversions
FORMAT_MP4 = "format_mp4"
FORMAT_MKV = "format_mkv"
FORMAT_AVI = "format_avi"
FORMAT_MOV = "format_mov"
FORMAT_WEBM = "format_webm"
FORMAT_FLV = "format_flv"


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


# Video -> MP3 extraction quality.
# Kept separate from BITRATE_PREFIX because that one re-encodes an already
# imported audio file, while these start the video -> MP3 extraction itself.
MP3_QUALITY_PREFIX = "mp3q_"

# Default extraction bitrate: 128k keeps speech/music files small while still
# sounding fine, and keeps Telegram delivery fast.
MP3_DEFAULT_BITRATE = "128k"
MP3_QUALITY_CHOICES = ("64k", "96k", "128k", "192k", "256k", "320k")


def mp3_quality_key(val: str) -> str:
    return f"{MP3_QUALITY_PREFIX}{val}"


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
# Opens the extraction picker (Audio Only / Video Only / Subtitles / All Streams).
# The individual actions below are the triggers it renders.
EXTRACT_MENU = "extraction_menu"
EXTRACT_AUDIO = "extract_audio"
EXTRACT_VIDEO = "extract_video"
EXTRACT_STREAMS = "extract_streams"
EXTRACT_SUBTITLES = "extract_subtitles"
EXTRACT_ALL_STREAMS = "extract_all_streams"
REMOVE_AUDIO = "remove_audio"
ADD_AUDIO = "merge_av_menu"
OPTIMIZE_PREFIX = "optimize_"
THUMBNAIL_GRID = "thumbnail_grid"
EDIT_METADATA = "edit_metadata"
NORMALIZE_AUDIO = "normalize_audio"
TRIM_VIDEO = "trim_video"
TRIM_AUDIO = "trim_audio"

# Trimmer modes
TRIMMER_1 = "trimmer_1"
TRIMMER_2 = "trimmer_2"

# UI-friendly aliases (kept for keyboard builders)
THUMBNAIL_EXTRACTOR = THUMBNAIL_GRID
CAPTION_EDITOR = "caption_editor"
MEDIA_FORWARDER = "media_forwarder"
STREAM_REMOVER = "stream_remover"
STREAM_EXTRACTOR = EXTRACT_STREAMS
VIDEO_TRIMMER = TRIM_VIDEO
VIDEO_MERGER = MERGE_VIDEOS_START
VIDEOS_SPLITTER = "video_splitter"
MANUAL_SHOTS = "manual_shots"
VIDEO_TO_AUDIO = "video_to_audio"
SUBTITLE_MERGER = "subtitle_merger"
VIDEO_RENAMER = "video_renamer"
VIDEO_CONVERTER = CONVERT_FORMAT_MENU

# Optimization presets
OPTIMIZE_WEB = "optimize_web"
OPTIMIZE_MOBILE = "optimize_mobile"
OPTIMIZE_TV = "optimize_tv"
OPTIMIZE_STORAGE = "optimize_storage"
OPTIMIZE_CUSTOM = "optimize_custom"

# Extraction variants (rendered by get_extraction_menu())
EXTRACT_AUDIO_ONLY = "extract_audio_only"
EXTRACT_VIDEO_ONLY = "extract_video_only"
EXTRACT_ALL = "extract_all"

# Merge/audio helpers
MERGE_AUDIO = "merge_audio"

# Other utilities found in keyboards
CREATE_ARCHIVE = "create_archive"
# Create Archive is staged, not immediate: the button collects the batch and
# shows what would be packed, and these two answer that prompt.
ARCHIVE_CONFIRM = "archive_confirm"
ARCHIVE_CANCEL = "archive_cancel"
# Create Archive asks for a name before it shows the summary; this button accepts
# the one derived from the batch instead of one the user types.
ARCHIVE_NAME_DEFAULT = "archive_name_default"
# The part-size picker reached from the Create Archive summary. It stores the
# same per-user setting as /usersettings, but its Back returns to the summary
# rather than the settings page - the flow the user pressed it from.
ARCHIVE_PART_MENU = "archive_part_menu"
ARCHIVE_PART_BACK = "archive_part_back"
ARCHIVE_PART_PREFIX = "archive_set_part:"
REPAIR_VIDEO = "repair_video"
FADE_MENU = "fade_menu"
FRAMERATE_MENU = "framerate_menu"

# Confirm/cancel
CONFIRM = "confirm"
CANCEL = "cancel"

# Bulk menu and extra conversion targets
BULK_MENU = "bulk_menu"
VIDEO_REORDER = "video_reorder"
CONVERT_TO_FILE = "convert_to_file"
CONVERT_TO_VIDEO = "convert_to_video"
MP3_TAG_EDITOR = "mp3_tag_editor"

# Additional formats
FORMAT_M4V = "format_m4v"
FORMAT_OPUS = "format_opus"

# Fade options
FADE_IN = "fade_in"
FADE_OUT = "fade_out"
FADE_BOTH = "fade_both"

# Screenshot quick modes
SCREENSHOT_START = "screenshot_start"
SCREENSHOT_MIDDLE = "screenshot_middle"
SCREENSHOT_END = "screenshot_end"
SCREENSHOT_CUSTOM = "screenshot_custom"
SCREENSHOT_9GRID = "screenshot_9grid"
SCREENSHOT_MULTIPLE = "screenshot_multiple"

# Resolution custom
RES_CUSTOM = "res_custom"

# Bulk encoding quality: the bulk menu's Compress CRF and Optimize preset are
# per-user picks applied by the next "Apply Bulk", not fixed defaults.
BULK_CRF_MENU = "bulk_crf_menu"
BULK_PRESET_MENU = "bulk_preset_menu"
BULK_CRF_PREFIX = "bulk_set_crf:"
BULK_PRESET_PREFIX = "bulk_set_preset:"

# Compress quality choices, mirroring the single-file compression menu.
BULK_CRF_CHOICES = (18, 23, 28, 35)
BULK_CRF_MIN = 18
BULK_CRF_MAX = 51
BULK_CRF_DEFAULT = 28

#: The one label per CRF, so the Compress menu, the bulk picker and
#: /usersettings cannot describe the same number two different ways.
COMPRESS_QUALITY_LABELS = {
    18: "🟢 High Quality",
    23: "🟡 Medium",
    28: "🔴 Low",
    35: "⚫ Extreme",
}

#: Where the compress-quality and optimize-preset preferences live. They sit in
#: the same store the bulk pickers write, under the same keys, because
#: /usersettings and the bulk menu are two views of one choice - a second key
#: would let them disagree about what "the user's quality" is.
COMPRESS_QUALITY_KEY = "bulk_crf"
OPTIMIZE_PRESET_KEY = "bulk_optimize_preset"


def compress_quality_label(crf) -> str:
    """``28`` -> ``"🔴 Low"``; a custom CRF falls back to the plain number."""
    try:
        number = int(crf)
    except (TypeError, ValueError):
        return "CRF"
    return COMPRESS_QUALITY_LABELS.get(number, f"CRF {number}")


# Optimize presets, mirroring the single-file optimize presets.
BULK_PRESET_CHOICES = ("web", "mobile", "tv", "storage")
BULK_PRESET_DEFAULT = "web"
BULK_PRESET_LABELS = {
    "web": "For Web",
    "mobile": "For Mobile",
    "tv": "For TV",
    "storage": "For Storage",
}


def bulk_crf_key(crf) -> str:
    return f"{BULK_CRF_PREFIX}{crf}"


def bulk_preset_key(name: str) -> str:
    return f"{BULK_PRESET_PREFIX}{name}"


# Bulk Extract Audio bitrate. Shares MP3_QUALITY_CHOICES with the single-file
# video -> MP3 picker so both offer the same values, and defaults to the same
# MP3_DEFAULT_BITRATE.
BULK_BITRATE_MENU = "bulk_bitrate_menu"
BULK_BITRATE_PREFIX = "bulk_set_bitrate:"
BULK_BITRATE_DEFAULT = MP3_DEFAULT_BITRATE


def bulk_bitrate_key(val) -> str:
    return f"{BULK_BITRATE_PREFIX}{val}"


# Bulk slideshow: how long each queued photo is shown when two or more photos
# are collected. A per-user pick applied by the next "Apply Bulk".
BULK_SLIDESHOW_MENU = "bulk_slideshow_menu"
BULK_SLIDESHOW_PREFIX = "bulk_set_slideshow:"

BULK_SLIDESHOW_CHOICES = (1.0, 2.0, 3.0, 5.0, 10.0)
BULK_SLIDESHOW_DEFAULT = 3.0
BULK_SLIDESHOW_MIN = 0.5
BULK_SLIDESHOW_MAX = 30.0

#: Where the slideshow length and the batch extraction bitrate live. Same keys
#: and same reasoning as the compress quality above: the panel and the bulk
#: pickers write one preference, not two that can disagree.
SLIDESHOW_SECONDS_KEY = "bulk_slideshow_seconds"
BULK_EXTRACT_BITRATE_KEY = "bulk_extract_bitrate"


def bulk_slideshow_key(val) -> str:
    return f"{BULK_SLIDESHOW_PREFIX}{val}"


# /usersettings. Every trigger below only ever *stores* a preference - it never
# starts a conversion, so the panel stays a settings panel and opening it can
# never encode whatever file happens to be loaded. Actions live in the menus.
SETTINGS_RENAME_MENU = "settings_rename_menu"
SETTINGS_BITRATE_MENU = "settings_bitrate_menu"
SETTINGS_QUALITY_MENU = "settings_quality_menu"
SETTINGS_PRESET_MENU = "settings_preset_menu"
SETTINGS_SLIDESHOW_MENU = "settings_slideshow_menu"
SETTINGS_BULK_BITRATE_MENU = "settings_bulk_bitrate_menu"
SETTINGS_ARCHIVE_PART_MENU = "settings_archive_part_menu"
SETTINGS_BITRATE_PREFIX = "settings_set_bitrate:"
SETTINGS_QUALITY_PREFIX = "settings_set_quality:"
SETTINGS_PRESET_PREFIX = "settings_set_preset:"
SETTINGS_SLIDESHOW_PREFIX = "settings_set_slideshow:"
SETTINGS_BULK_BITRATE_PREFIX = "settings_set_bulk_bitrate:"
SETTINGS_ARCHIVE_PART_PREFIX = "settings_set_archive_part:"
SETTINGS_TOGGLE_PREFIX = "settings_toggle:"
SETTINGS_TOOL_PREFIX = "settings_tool:"

#: The panel's pages, in order. The page count and the Prev/Next row are built
#: from this one tuple, so adding a page means adding its rows and nothing else -
#: no page can end up reachable by "Next" but missing from the view.
#:
#: 1 general (delivery and naming), 2 quality (what a conversion encodes at),
#: 3 batch (what a multi-file run produces).
SETTINGS_PAGES = ("general", "quality", "batch")
SETTINGS_PAGE_COUNT = len(SETTINGS_PAGES)


def settings_page_number(page) -> int:
    """Clamp a requested page to one of the pages the panel actually has."""
    try:
        number = int(page or 1)
    except (TypeError, ValueError):
        number = 1
    return min(max(1, number), SETTINGS_PAGE_COUNT)


def settings_page_key(page) -> str:
    """The callback that opens a page (``settings_page:2``)."""
    return f"settings_page:{settings_page_number(page)}"


# The video delivery format: playable media (Telegram preview) or a document.
# Shares the ``upload_mode`` user setting, so switching it here and switching it
# in the settings panel are the same choice.
SETTINGS_UPLOAD_MODE_PREFIX = "settings_upload_mode:"
UPLOAD_MODE_VIDEO = "video"
UPLOAD_MODE_FILE = "file"
UPLOAD_MODES = (UPLOAD_MODE_VIDEO, UPLOAD_MODE_FILE)
UPLOAD_MODE_LABELS = {
    UPLOAD_MODE_VIDEO: "📺 Video (preview)",
    UPLOAD_MODE_FILE: "📄 File (document)",
}


def settings_bitrate_key(val) -> str:
    return f"{SETTINGS_BITRATE_PREFIX}{val}"


def settings_quality_key(crf) -> str:
    """A settings trigger for one compress quality (or the ``custom`` prompt)."""
    return f"{SETTINGS_QUALITY_PREFIX}{crf}"


def settings_preset_key(name: str) -> str:
    """A settings trigger for one optimize preset."""
    return f"{SETTINGS_PRESET_PREFIX}{name}"


def settings_slideshow_key(seconds) -> str:
    """A settings trigger for one slideshow length (or the ``custom`` prompt)."""
    return f"{SETTINGS_SLIDESHOW_PREFIX}{seconds}"


def settings_bulk_bitrate_key(val) -> str:
    """A settings trigger for one batch extraction bitrate."""
    return f"{SETTINGS_BULK_BITRATE_PREFIX}{val}"


def settings_archive_part_key(val) -> str:
    """A settings trigger for one archive part size (or the ``custom`` prompt)."""
    return f"{SETTINGS_ARCHIVE_PART_PREFIX}{val}"


def archive_part_key(val) -> str:
    """An archive-flow trigger for one part size (or the ``custom`` prompt)."""
    return f"{ARCHIVE_PART_PREFIX}{val}"


def settings_upload_mode_key(mode: str) -> str:
    return f"{SETTINGS_UPLOAD_MODE_PREFIX}{mode}"
