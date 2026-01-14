# tasks/__init__.py
"""
Background tasks package for media conversion bot
"""

from .cleanup_tasks import (
    CleanupManager,
    cleanup_manager,
    start_cleanup_task,
    stop_cleanup_task,
)
from .conversion_tasks import (
    adjust_bitrate,
    change_resolution,
    compress_video,
    convert_audio_format,
    convert_video_to_mp3,
    create_archive,
    create_thumbnail_grid,
    edit_metadata,
    extract_audio,
    extract_streams,
    extract_subtitles,
    generate_sample,
    merge_audios,
    merge_videos,
    normalize_audio,
    optimize_video,
    repair_video,
    take_screenshot,
    trim_media,
)

# Media schema imports are optional - database features may not be available
try:
    from .media_schema import (
        AUDIO_BITRATES,
        COMPRESSION_PRESETS,
        VIDEO_BITRATES,
        VIDEO_RESOLUTIONS,
        AudioConversionParams,
        CompressionParams,
        ConversionStatus,
        ConversionTask,
        MediaInfo,
        MediaSchemaValidator,
        MediaType,
        TrimParams,
        VideoConversionParams,
    )
except ImportError:
    MediaType = None
    ConversionStatus = None
    MediaInfo = None
    ConversionTask = None
    VideoConversionParams = None
    AudioConversionParams = None
    CompressionParams = None
    TrimParams = None
    MediaSchemaValidator = None
    COMPRESSION_PRESETS = None
    AUDIO_BITRATES = None
    VIDEO_RESOLUTIONS = None
    VIDEO_BITRATES = None

__all__ = [
    "convert_video_to_mp3",
    "compress_video",
    "extract_audio",
    "merge_videos",
    "merge_audios",
    "take_screenshot",
    "change_resolution",
    "trim_media",
    "repair_video",
    "optimize_video",
    "create_thumbnail_grid",
    "generate_sample",
    "extract_streams",
    "convert_audio_format",
    "adjust_bitrate",
    "normalize_audio",
    "extract_subtitles",
    "edit_metadata",
    "create_archive",
    "CleanupManager",
    "cleanup_manager",
    "start_cleanup_task",
    "stop_cleanup_task",
    "MediaType",
    "ConversionStatus",
    "MediaInfo",
    "ConversionTask",
    "VideoConversionParams",
    "AudioConversionParams",
    "CompressionParams",
    "TrimParams",
    "MediaSchemaValidator",
    "COMPRESSION_PRESETS",
    "AUDIO_BITRATES",
    "VIDEO_RESOLUTIONS",
    "VIDEO_BITRATES",
]
