# utils/keyboard_utils.py
"""
Keyboard menu builders for Telegram bot.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from typing import List, Dict, Any
from .callbacks import (
    MENU_MAIN,
    MENU_VIDEO,
    INFO,
    HELP,
    SEND_FILE,
    COMPRESS_MENU,
    TRIM_VIDEO,
    SCREENSHOTS_MENU,
    RESOLUTION_MENU,
    OPTIMIZE_MENU,
    EXTRACT_AUDIO,
    MERGE_ADD,
    MERGE_VIEW,
    MERGE_CLEAR,
    MERGE_VIDEOS_START,
    FORMAT_PREFIX,
    BITRATE_PREFIX,
    CONFIRM,
    CANCEL,
    TRIM_AUDIO,
    NORMALIZE_AUDIO,
    REMOVE_AUDIO,
    ADD_AUDIO,
    SAMPLE,
    THUMBNAIL_GRID,
    EDIT_METADATA,
)


class MediaMenuBuilder:
    """Builds interactive keyboards for media conversion options."""
    
    @staticmethod
    def get_main_menu(file_type: str = None) -> InlineKeyboardMarkup:
        """Get main menu based on file type."""
        if file_type == 'video':
            buttons = [
                [
                    InlineKeyboardButton("🎬 Video Tools", callback_data=MENU_VIDEO),
                    InlineKeyboardButton("🎧 Extract Audio", callback_data=EXTRACT_AUDIO)
                ],
                [
                    InlineKeyboardButton("📉 Compress", callback_data=COMPRESS_MENU),
                    InlineKeyboardButton("✂️ Trim", callback_data=TRIM_VIDEO)
                ],
                [
                    InlineKeyboardButton("🖼️ Screenshots", callback_data=SCREENSHOTS_MENU),
                    InlineKeyboardButton("ℹ️ Info", callback_data=INFO)
                ],
                [
                    InlineKeyboardButton("🔀 Merge", callback_data=MERGE_VIDEOS_START),
                    InlineKeyboardButton("⚡ Optimize", callback_data=OPTIMIZE_MENU)
                ]
            ]
        elif file_type == 'audio':
            buttons = [
                [
                    InlineKeyboardButton("🎧 Convert Format", callback_data=f"{FORMAT_PREFIX}menu"),
                    InlineKeyboardButton("🎚️ Adjust Bitrate", callback_data=BITRATE_PREFIX + "menu")
                ],
                [
                    InlineKeyboardButton("✂️ Trim Audio", callback_data=TRIM_AUDIO),
                    InlineKeyboardButton("🔀 Merge", callback_data=MERGE_ADD)
                ],
                [
                    InlineKeyboardButton("📊 Normalize", callback_data=NORMALIZE_AUDIO),
                    InlineKeyboardButton("ℹ️ Audio Info", callback_data=INFO)
                ],
                [
                    InlineKeyboardButton("🔙 Back", callback_data=MENU_MAIN)
                ]
            ]
        else:
            buttons = [
                [InlineKeyboardButton("📤 Send Media File", callback_data=SEND_FILE)],
                [InlineKeyboardButton("ℹ️ Help", callback_data=HELP)],
                [InlineKeyboardButton("🚀 Quick Start", callback_data="quick_start")]
            ]
        
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_compression_menu() -> InlineKeyboardMarkup:
        """Get compression quality menu."""
        buttons = [
            [
                InlineKeyboardButton("🟢 High Quality", callback_data="compress_18"),
                InlineKeyboardButton("🟡 Medium", callback_data="compress_23")
            ],
            [
                InlineKeyboardButton("🔴 Low", callback_data="compress_28"),
                InlineKeyboardButton("⚫ Extreme", callback_data="compress_35")
            ],
            [
                InlineKeyboardButton("↩️ Back", callback_data="menu_main")
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_resolution_menu() -> InlineKeyboardMarkup:
        """Get resolution change menu."""
        buttons = [
            [
                InlineKeyboardButton("4K (3840x2160)", callback_data="res_3840_2160"),
                InlineKeyboardButton("1080p (1920x1080)", callback_data="res_1920_1080")
            ],
            [
                InlineKeyboardButton("720p (1280x720)", callback_data="res_1280_720"),
                InlineKeyboardButton("480p (854x480)", callback_data="res_854_480")
            ],
            [
                InlineKeyboardButton("360p (640x360)", callback_data="res_640_360"),
                InlineKeyboardButton("Custom", callback_data="res_custom")
            ],
            [
                InlineKeyboardButton("↩️ Back", callback_data="menu_main")
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_audio_format_menu() -> InlineKeyboardMarkup:
        """Get audio format conversion menu."""
        buttons = [
            [
                InlineKeyboardButton("MP3", callback_data="audio_mp3"),
                InlineKeyboardButton("WAV", callback_data="audio_wav")
            ],
            [
                InlineKeyboardButton("AAC", callback_data="audio_aac"),
                InlineKeyboardButton("FLAC", callback_data="audio_flac")
            ],
            [
                InlineKeyboardButton("OGG", callback_data="audio_ogg"),
                InlineKeyboardButton("M4A", callback_data="audio_m4a")
            ],
            [
                InlineKeyboardButton("↩️ Back", callback_data="menu_main")
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_bitrate_menu(media_type: str = "audio") -> InlineKeyboardMarkup:
        """Get bitrate adjustment menu."""
        if media_type == "audio":
            buttons = [
                [
                    InlineKeyboardButton("320k (Best)", callback_data="bitrate_320"),
                    InlineKeyboardButton("256k (High)", callback_data="bitrate_256")
                ],
                [
                    InlineKeyboardButton("192k (Medium)", callback_data="bitrate_192"),
                    InlineKeyboardButton("128k (Low)", callback_data="bitrate_128")
                ]
            ]
        else:  # video
            buttons = [
                [
                    InlineKeyboardButton("5000k", callback_data="vbitrate_5000"),
                    InlineKeyboardButton("3000k", callback_data="vbitrate_3000")
                ],
                [
                    InlineKeyboardButton("2000k", callback_data="vbitrate_2000"),
                    InlineKeyboardButton("1000k", callback_data="vbitrate_1000")
                ]
            ]
        
        buttons.append([InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)])
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_screenshot_menu() -> InlineKeyboardMarkup:
        """Get screenshot options menu."""
        buttons = [
            [
                InlineKeyboardButton("🎬 Start", callback_data="screenshot_start"),
                InlineKeyboardButton("⏱️ Middle", callback_data="screenshot_middle")
            ],
            [
                InlineKeyboardButton("🎞️ End", callback_data="screenshot_end"),
                InlineKeyboardButton("⏰ Custom Time", callback_data="screenshot_custom")
            ],
            [
                InlineKeyboardButton("🖼️ Grid (3x3)", callback_data="screenshot_grid_3"),
                InlineKeyboardButton("🖼️ Grid (4x4)", callback_data="screenshot_grid_4")
            ],
            [
                InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_merge_menu(media_type: str = "video") -> InlineKeyboardMarkup:
        """Get merge options menu."""
        buttons = [
            [
                InlineKeyboardButton("➕ Add File", callback_data=MERGE_ADD),
                InlineKeyboardButton("👀 View List", callback_data=MERGE_VIEW)
            ],
            [
                InlineKeyboardButton("▶️ Start Merge", callback_data=MERGE_VIDEOS_START),
                InlineKeyboardButton("🗑️ Clear List", callback_data=MERGE_CLEAR)
            ],
            [
                InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_optimize_menu() -> InlineKeyboardMarkup:
        """Get optimization presets menu."""
        buttons = [
            [
                InlineKeyboardButton("🌐 For Web", callback_data="optimize_web"),
                InlineKeyboardButton("📱 For Mobile", callback_data="optimize_mobile")
            ],
            [
                InlineKeyboardButton("📺 For TV", callback_data="optimize_tv"),
                InlineKeyboardButton("💾 For Storage", callback_data="optimize_storage")
            ],
            [
                InlineKeyboardButton("🔧 Custom", callback_data="optimize_custom")
            ],
            [
                InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_extraction_menu() -> InlineKeyboardMarkup:
        """Get extraction options menu."""
        buttons = [
            [
                InlineKeyboardButton("🎧 Audio Only", callback_data="extract_audio_only"),
                InlineKeyboardButton("🎬 Video Only", callback_data="extract_video_only")
            ],
            [
                InlineKeyboardButton("📝 Subtitles", callback_data="extract_subtitles"),
                InlineKeyboardButton("📦 All Streams", callback_data="extract_all")
            ],
            [
                InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_video_tools_menu() -> InlineKeyboardMarkup:
        """Get video tools menu."""
        buttons = [
            [
                InlineKeyboardButton("🎬 Convert Format", callback_data=f"{FORMAT_PREFIX}video"),
                InlineKeyboardButton("📉 Compress", callback_data=COMPRESS_MENU)
            ],
            [
                InlineKeyboardButton("📐 Resolution", callback_data=RESOLUTION_MENU),
                InlineKeyboardButton("⏱️ Framerate", callback_data="framerate_menu")
            ],
            [
                InlineKeyboardButton("✂️ Trim", callback_data=TRIM_VIDEO),
                InlineKeyboardButton("🔀 Merge", callback_data=MERGE_VIDEOS_START)
            ],
            [
                InlineKeyboardButton("🎧 Remove Audio", callback_data=REMOVE_AUDIO),
                InlineKeyboardButton("🔉 Add Audio", callback_data=ADD_AUDIO)
            ],
            [
                InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_audio_tools_menu() -> InlineKeyboardMarkup:
        """Get audio tools menu."""
        buttons = [
            [
                InlineKeyboardButton("🔄 Convert", callback_data=f"{FORMAT_PREFIX}audio"),
                InlineKeyboardButton("🎚️ Bitrate", callback_data=BITRATE_PREFIX + "menu")
            ],
            [
                InlineKeyboardButton("📊 Normalize", callback_data="normalize_audio"),
                InlineKeyboardButton("✂️ Trim", callback_data="trim_audio")
            ],
            [
                InlineKeyboardButton("🔀 Merge", callback_data="merge_audio"),
                InlineKeyboardButton("📈 Fade In/Out", callback_data="fade_menu")
            ],
            [
                InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_advanced_tools_menu() -> InlineKeyboardMarkup:
        """Get advanced tools menu."""
        buttons = [
            [
                InlineKeyboardButton("📦 Create Archive", callback_data="create_archive"),
                InlineKeyboardButton("🔧 Repair", callback_data="repair_video")
            ],
            [
                InlineKeyboardButton("📊 Media Info", callback_data=INFO),
                InlineKeyboardButton("🎞️ Sample", callback_data=SAMPLE)
            ],
            [
                InlineKeyboardButton("🖼️ Thumbnail", callback_data=THUMBNAIL_GRID),
                InlineKeyboardButton("✏️ Metadata", callback_data=EDIT_METADATA)
            ],
            [
                InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_confirm_menu() -> InlineKeyboardMarkup:
        """Get confirmation menu."""
        buttons = [
            [
                InlineKeyboardButton("✅ Confirm", callback_data=CONFIRM),
                InlineKeyboardButton("❌ Cancel", callback_data=CANCEL)
            ]
        ]
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def get_back_button() -> InlineKeyboardMarkup:
        """Get simple back button."""
        buttons = [
            [InlineKeyboardButton("🔙 Back", callback_data=MENU_MAIN)]
        ]
        return InlineKeyboardMarkup(buttons)
