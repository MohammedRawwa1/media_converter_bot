# utils/keyboard_utils.py
"""
Keyboard menu builders for Telegram bot.
"""

from typing import List

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .callbacks import (
    ADD_AUDIO,
    BITRATE_PREFIX,
    CANCEL,
    COMPRESS_MENU,
    CONFIRM,
    EDIT_METADATA,
    FORMAT_PREFIX,
    INFO,
    HELP,
    MENU_MAIN,
    MERGE_MENU,
    MERGE_ADD,
    MERGE_CLEAR,
    MERGE_VIDEOS_START,
    MERGE_AUDIOS_START,
    MERGE_VIEW,
    OPTIMIZE_MENU,
    REMOVE_AUDIO,
    RESOLUTION_MENU,
    SAMPLE,
    SCREENSHOTS_MENU,
    CONVERT_FORMAT_MENU,
    THUMBNAIL_GRID,
    THUMBNAIL_EXTRACTOR,
    CAPTION_EDITOR,
    MEDIA_FORWARDER,
    STREAM_REMOVER,
    STREAM_EXTRACTOR,
    VIDEOS_SPLITTER,
    MANUAL_SHOTS,
    VIDEO_TO_AUDIO,
    SUBTITLE_MERGER,
    VIDEO_RENAMER,
    CREATE_ARCHIVE,
    TRIM_VIDEO,
    TRIM_MENU,
    TRIMMER_1,
    TRIMMER_2,
    BULK_MENU,
    VIDEO_REORDER,
    CONVERT_TO_FILE,
    CONVERT_TO_VIDEO,
    MP3_TAG_EDITOR,
    FORMAT_M4V,
    FORMAT_OPUS,
    FORMAT_MP4,
)


class MediaMenuBuilder:
    """Builds interactive keyboards for media conversion options."""

    @staticmethod
    def get_main_menu(file_type: str = None) -> InlineKeyboardMarkup:
        """Get main menu based on file type."""
        # Shared helpers for common rows
        def row(left_label, left_cb, right_label, right_cb):
            return [
                InlineKeyboardButton(left_label, callback_data=left_cb),
                InlineKeyboardButton(right_label, callback_data=right_cb),
            ]

        # Build rows according to the UI image. Adapt some labels based on
        # the `file_type` so that video uploads show video-oriented tools.
        is_video = (file_type == "video")

        conv_label = "🎬 Video Converter" if is_video else "🎧 Audio Converter"
        # Use explicit `video_converter` callback so the UI shows the video converter
        # alias; handlers remap this alias to the canonical `convert_format_menu`.
        conv_cb = "video_converter"

        split_label = "🔪 Videos Splitter" if is_video else "🔪 Split"

        buttons: List[List[InlineKeyboardButton]] = [
            row("🖼️ Thumbnail Extractor", THUMBNAIL_EXTRACTOR, "✏️ Caption And Buttons Editor", CAPTION_EDITOR),
            row("📝 Metadata Editor", EDIT_METADATA, "📤 Media Forwarder", MEDIA_FORWARDER),
            row("🔇 Stream Remover", STREAM_REMOVER, "🎵 Stream Extractor", STREAM_EXTRACTOR),
            row("✂️ Video Trimmer", TRIM_VIDEO, "➕ Video Merger", MERGE_MENU),
            row("🔉 Remove Audio", REMOVE_AUDIO, "🔀 Merge And", MERGE_VIEW),
            row(conv_label, conv_cb, split_label, VIDEOS_SPLITTER),
            row("🖼️ Screenshots", SCREENSHOTS_MENU, "🖼️ Manual Shots", MANUAL_SHOTS),
            row("🎞️ Generate Sample", SAMPLE, "🎵 Video To Audio", VIDEO_TO_AUDIO),
            row("⚡ Video Optimizer", OPTIMIZE_MENU, "🔗 Subtitle Merger", SUBTITLE_MERGER),
            row("✏️ Video Renamer", VIDEO_RENAMER, "🛈 Media Information", INFO),
            # Single-button rows for create/archive and final cancel button
            [InlineKeyboardButton("📦 Create Archive", callback_data=CREATE_ARCHIVE)],
            # Add quick access to bulk actions
            [InlineKeyboardButton("📦 Bulk Actions", callback_data="bulk_menu")],
            [InlineKeyboardButton("❌ Cancel", callback_data=CANCEL)],
        ]

        # If file_type provided, you may want to prioritize tools, but keep
        # menu consistent regardless of type for this layout.
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_format_menu(media_type: str = "audio") -> InlineKeyboardMarkup:
        """Generic format menu used by handlers; supports 'audio' and 'video'."""
        if media_type == "audio":
            return MediaMenuBuilder.get_audio_format_menu()
        # FIXED: Video format menu with proper video formats
        buttons = [
            [
                InlineKeyboardButton("MP4", callback_data="format_mp4"),
                InlineKeyboardButton("MKV", callback_data="format_mkv"),
            ],
            [
                InlineKeyboardButton("AVI", callback_data="format_avi"),
                InlineKeyboardButton("MOV", callback_data="format_mov"),
            ],
            [
                InlineKeyboardButton("WEBM", callback_data="format_webm"),
                InlineKeyboardButton("FLV", callback_data="format_flv"),
            ],
            [InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_compression_menu() -> InlineKeyboardMarkup:
        """Get compression quality menu."""
        buttons = [
            [
                InlineKeyboardButton("🟢 High Quality", callback_data="compress_18"),
                InlineKeyboardButton("🟡 Medium", callback_data="compress_23"),
            ],
            [
                InlineKeyboardButton("🔴 Low", callback_data="compress_28"),
                InlineKeyboardButton("⚫ Extreme", callback_data="compress_35"),
            ],
            [InlineKeyboardButton("↩️ Back", callback_data="menu_main")],
        ]
        return InlineKeyboardMarkup(buttons)

    # Backwards-compatible alias used by handlers
    @staticmethod
    def get_screenshots_menu() -> InlineKeyboardMarkup:
        """Alias for get_screenshot_menu kept for compatibility."""
        return MediaMenuBuilder.get_screenshot_menu()

    @staticmethod
    def get_resolution_menu() -> InlineKeyboardMarkup:
        """Get resolution change menu."""
        buttons = [
            [
                InlineKeyboardButton("4K (3840x2160)", callback_data="res_3840_2160"),
                InlineKeyboardButton("1080p (1920x1080)", callback_data="res_1920_1080"),
            ],
            [
                InlineKeyboardButton("720p (1280x720)", callback_data="res_1280_720"),
                InlineKeyboardButton("480p (854x480)", callback_data="res_854_480"),
            ],
            [
                InlineKeyboardButton("360p (640x360)", callback_data="res_640_360"),
                InlineKeyboardButton("Custom", callback_data="res_custom"),
            ],
            [InlineKeyboardButton("↩️ Back", callback_data="menu_main")],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_audio_format_menu() -> InlineKeyboardMarkup:
        """Get audio format conversion menu."""
        buttons = [
            [
                InlineKeyboardButton("MP3", callback_data="audio_mp3"),
                InlineKeyboardButton("WAV", callback_data="audio_wav"),
            ],
            [
                InlineKeyboardButton("AAC", callback_data="audio_aac"),
                InlineKeyboardButton("FLAC", callback_data="audio_flac"),
            ],
            [
                InlineKeyboardButton("OGG", callback_data="audio_ogg"),
                InlineKeyboardButton("M4A", callback_data="audio_m4a"),
            ],
            [InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_bitrate_menu(media_type: str = "audio") -> InlineKeyboardMarkup:
        """Get bitrate adjustment menu."""
        if media_type == "audio":
            buttons = [
                [
                    InlineKeyboardButton("320k (Best)", callback_data="bitrate_320"),
                    InlineKeyboardButton("256k (High)", callback_data="bitrate_256"),
                ],
                [
                    InlineKeyboardButton("192k (Medium)", callback_data="bitrate_192"),
                    InlineKeyboardButton("128k (Low)", callback_data="bitrate_128"),
                ],
            ]
        else:  # video
            buttons = [
                [
                    InlineKeyboardButton("5000k", callback_data="vbitrate_5000"),
                    InlineKeyboardButton("3000k", callback_data="vbitrate_3000"),
                ],
                [
                    InlineKeyboardButton("2000k", callback_data="vbitrate_2000"),
                    InlineKeyboardButton("1000k", callback_data="vbitrate_1000"),
                ],
            ]

        buttons.append([InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)])
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_screenshot_menu() -> InlineKeyboardMarkup:
        """Get screenshot options menu."""
        buttons = [
            [
                InlineKeyboardButton("🎬 Start", callback_data="screenshot_start"),
                InlineKeyboardButton("⏱️ Middle", callback_data="screenshot_middle"),
            ],
            [
                InlineKeyboardButton("🎞️ End", callback_data="screenshot_end"),
                InlineKeyboardButton("⏰ Custom Time", callback_data="screenshot_custom"),
            ],
            [
                InlineKeyboardButton("🖼️ Grid (3x3)", callback_data="screenshot_grid_3"),
                InlineKeyboardButton("🖼️ Grid (4x4)", callback_data="screenshot_grid_4"),
            ],
            [InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_trimmer_menu() -> InlineKeyboardMarkup:
        """Get trimmer selection menu with two dynamic modes."""
        buttons = [
            [InlineKeyboardButton("Trimmer 1: Start -> End", callback_data=TRIMMER_1)],
            [InlineKeyboardButton("Trimmer 2: Start + Duration", callback_data=TRIMMER_2)],
            [InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_bulk_menu() -> InlineKeyboardMarkup:
        """Bulk mode action grid matching the pictured layout."""
        buttons = [
            [InlineKeyboardButton("📦 Create Archive", callback_data=CREATE_ARCHIVE), InlineKeyboardButton("🔇 Remove Audios", callback_data=REMOVE_AUDIO)],
            [InlineKeyboardButton("🔁 Video Reorder", callback_data=VIDEO_REORDER), InlineKeyboardButton("📼 Convert To M4V", callback_data=FORMAT_M4V)],
            [InlineKeyboardButton("🔉 Convert To AAC", callback_data="format_aac"), InlineKeyboardButton("🎬 Convert To MP4", callback_data=FORMAT_MP4)],
            [InlineKeyboardButton("🎵 Convert To MP3", callback_data="audio_mp3"), InlineKeyboardButton("🔊 Convert To OPUS", callback_data=FORMAT_OPUS)],
            [InlineKeyboardButton("🔗 Convert To File", callback_data=CONVERT_TO_FILE), InlineKeyboardButton("🎥 Convert To Video", callback_data=CONVERT_TO_VIDEO)],
            [InlineKeyboardButton("🔀 Audio Merger", callback_data=MERGE_AUDIOS_START), InlineKeyboardButton("🔀 Video Merger", callback_data=MERGE_VIDEOS_START)],
            [InlineKeyboardButton("✏️ Mp3 Tag Editor", callback_data=MP3_TAG_EDITOR), InlineKeyboardButton("🔎 Go to Help", callback_data=HELP)],
            [InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_merge_menu(media_type: str = "video") -> InlineKeyboardMarkup:
        """Get merge options menu."""
        buttons = [
            [
                InlineKeyboardButton("➕ Add File", callback_data=MERGE_ADD),
                InlineKeyboardButton("👀 View List", callback_data=MERGE_VIEW),
            ],
            [
                InlineKeyboardButton("▶️ Start Merge", callback_data=MERGE_VIDEOS_START),
                InlineKeyboardButton("🗑️ Clear List", callback_data=MERGE_CLEAR),
            ],
            [InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_optimize_menu() -> InlineKeyboardMarkup:
        """Get optimization presets menu."""
        buttons = [
            [
                InlineKeyboardButton("🌐 For Web", callback_data="optimize_web"),
                InlineKeyboardButton("📱 For Mobile", callback_data="optimize_mobile"),
            ],
            [
                InlineKeyboardButton("📺 For TV", callback_data="optimize_tv"),
                InlineKeyboardButton("💾 For Storage", callback_data="optimize_storage"),
            ],
            [InlineKeyboardButton("🔧 Custom", callback_data="optimize_custom")],
            [InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_extraction_menu() -> InlineKeyboardMarkup:
        """Get extraction options menu."""
        buttons = [
            [
                InlineKeyboardButton("🎧 Audio Only", callback_data="extract_audio_only"),
                InlineKeyboardButton("🎬 Video Only", callback_data="extract_video_only"),
            ],
            [
                InlineKeyboardButton("📝 Subtitles", callback_data="extract_subtitles"),
                InlineKeyboardButton("📦 All Streams", callback_data="extract_all"),
            ],
            [InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_video_tools_menu() -> InlineKeyboardMarkup:
        """Get video tools menu."""
        buttons = [
            [
                InlineKeyboardButton("🎬 Convert Format", callback_data=CONVERT_FORMAT_MENU),
                InlineKeyboardButton("📉 Compress", callback_data=COMPRESS_MENU),
            ],
            [
                InlineKeyboardButton("📐 Resolution", callback_data=RESOLUTION_MENU),
                InlineKeyboardButton("⏱️ Framerate", callback_data="framerate_menu"),
            ],
            [
                InlineKeyboardButton("✂️ Trim", callback_data=TRIM_VIDEO),
                InlineKeyboardButton("🔀 Merge", callback_data=MERGE_VIDEOS_START),
            ],
            [
                InlineKeyboardButton("🎧 Remove Audio", callback_data=REMOVE_AUDIO),
                InlineKeyboardButton("🔉 Add Audio", callback_data=ADD_AUDIO),
            ],
            [InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_audio_tools_menu() -> InlineKeyboardMarkup:
        """Get audio tools menu."""
        buttons = [
            [
                InlineKeyboardButton("🔄 Convert", callback_data=f"{FORMAT_PREFIX}audio"),
                InlineKeyboardButton("🎚️ Bitrate", callback_data=BITRATE_PREFIX + "menu"),
            ],
            [
                InlineKeyboardButton("📊 Normalize", callback_data="normalize_audio"),
                InlineKeyboardButton("✂️ Trim", callback_data="trim_audio"),
            ],
            [
                InlineKeyboardButton("🔀 Merge", callback_data="merge_audio"),
                InlineKeyboardButton("📈 Fade In/Out", callback_data="fade_menu"),
            ],
            [InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_advanced_tools_menu() -> InlineKeyboardMarkup:
        """Get advanced tools menu."""
        buttons = [
            [
                InlineKeyboardButton("📦 Create Archive", callback_data="create_archive"),
                InlineKeyboardButton("🔧 Repair", callback_data="repair_video"),
            ],
            [
                InlineKeyboardButton("📊 Media Info", callback_data=INFO),
                InlineKeyboardButton("🎞️ Sample", callback_data=SAMPLE),
            ],
            [
                InlineKeyboardButton("🖼️ Thumbnail", callback_data=THUMBNAIL_GRID),
                InlineKeyboardButton("✏️ Metadata", callback_data=EDIT_METADATA),
            ],
            [InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_confirm_menu() -> InlineKeyboardMarkup:
        """Get confirmation menu."""
        buttons = [
            [
                InlineKeyboardButton("✅ Confirm", callback_data=CONFIRM),
                InlineKeyboardButton("❌ Cancel", callback_data=CANCEL),
            ]
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_back_button() -> InlineKeyboardMarkup:
        """Get simple back button."""
        buttons = [[InlineKeyboardButton("🔙 Back", callback_data=MENU_MAIN)]]
        return InlineKeyboardMarkup(buttons)
