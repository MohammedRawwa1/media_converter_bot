# utils/keyboard_utils.py
"""
Keyboard menu builders for Telegram bot.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from . import archive_split
from .callbacks import (
    ADD_AUDIO,
    ARCHIVE_CANCEL,
    ARCHIVE_CONFIRM,
    ARCHIVE_NAME_DEFAULT,
    ARCHIVE_PART_BACK,
    ARCHIVE_PART_MENU,
    BITRATE_PREFIX,
    BULK_BITRATE_DEFAULT,
    BULK_BITRATE_MENU,
    BULK_CRF_CHOICES,
    BULK_CRF_DEFAULT,
    BULK_CRF_MAX,
    BULK_CRF_MENU,
    BULK_CRF_MIN,
    BULK_EXTRACT_BITRATE_KEY,
    BULK_PRESET_CHOICES,
    BULK_PRESET_DEFAULT,
    BULK_PRESET_LABELS,
    BULK_PRESET_MENU,
    BULK_SLIDESHOW_CHOICES,
    BULK_SLIDESHOW_DEFAULT,
    BULK_SLIDESHOW_MAX,
    BULK_SLIDESHOW_MENU,
    BULK_SLIDESHOW_MIN,
    CANCEL,
    CAPTION_EDITOR,
    COMPRESS_MENU,
    COMPRESS_QUALITY_KEY,
    COMPRESS_QUALITY_LABELS,
    CONFIRM,
    CONVERT_FORMAT_MENU,
    CREATE_ARCHIVE,
    EDIT_METADATA,
    EXTRACT_MENU,
    FADE_BOTH,
    FADE_IN,
    FADE_OUT,
    FORMAT_PREFIX,
    INFO,
    MANUAL_SHOTS,
    MEDIA_FORWARDER,
    MENU_ADVANCED,
    MENU_AUDIO,
    MENU_MAIN,
    MENU_VIDEO,
    MERGE_ADD,
    MERGE_CLEAR,
    MERGE_MENU,
    MERGE_VIDEOS_START,
    MERGE_VIEW,
    MP3_DEFAULT_BITRATE,
    MP3_QUALITY_CHOICES,
    MP3_TAG_EDITOR,
    OPTIMIZE_MENU,
    OPTIMIZE_PRESET_KEY,
    REMOVE_AUDIO,
    RESOLUTION_MENU,
    SCREENSHOTS_MENU,
    SETTINGS_ARCHIVE_PART_MENU,
    SETTINGS_BITRATE_MENU,
    SETTINGS_BULK_BITRATE_MENU,
    SETTINGS_PAGE_COUNT,
    SETTINGS_PRESET_MENU,
    SETTINGS_QUALITY_MENU,
    SETTINGS_RENAME_MENU,
    SETTINGS_SLIDESHOW_MENU,
    SETTINGS_UPLOAD_MODE_PREFIX,
    SLIDESHOW_SECONDS_KEY,
    STREAM_EXTRACTOR,
    STREAM_REMOVER,
    SUBTITLE_MERGER,
    THUMBNAIL_EXTRACTOR,
    THUMBNAIL_GRID,
    TRIM_AUDIO,
    TRIM_VIDEO,
    TRIMMER_1,
    TRIMMER_2,
    UPLOAD_MODE_FILE,
    UPLOAD_MODE_LABELS,
    UPLOAD_MODE_VIDEO,
    VIDEO_RENAMER,
    VIDEO_TO_AUDIO,
    VIDEOS_SPLITTER,
    archive_part_key,
    bulk_bitrate_key,
    bulk_crf_key,
    bulk_preset_key,
    bulk_slideshow_key,
    compress_quality_label,
    mp3_quality_key,
    settings_archive_part_key,
    settings_bitrate_key,
    settings_bulk_bitrate_key,
    settings_page_key,
    settings_page_number,
    settings_preset_key,
    settings_quality_key,
    settings_slideshow_key,
)


def _bitrate_preset(current, choices=MP3_QUALITY_CHOICES) -> str | None:
    """The preset bitrate ``current`` is, or ``None`` when it is not a preset.

    Bitrates are compared as text because that is what all of these menus carry:
    the callback value, the stored setting and the ffmpeg argument are the same
    string (``"64k"``), and nothing here re-derives one from another.

    ``choices`` is the picker's *own* offer. The audio-bitrate picker is the one
    that differs: it has no 64k button, so 64k is a custom value there even though
    the other pickers list it as a preset.
    """
    value = str(current or "").strip()
    return value if value in choices else None


def _custom_bitrate(current, choices=MP3_QUALITY_CHOICES) -> str | None:
    """The bitrate ``current`` holds that is *not* one of the presets, if any."""
    value = str(current or "").strip()
    return value if value and _bitrate_preset(value, choices) is None else None


def _active_bitrate(current, default: str | None = None, choices=MP3_QUALITY_CHOICES) -> str | None:
    """Which preset a picker should mark as active.

    A custom value has no preset to mark - marking the default instead is what used
    to put a check beside 128k while the user's own 64k appeared nowhere - so it
    marks none, and the Custom row carries the value instead.
    """
    preset = _bitrate_preset(current, choices)
    if preset is not None:
        return preset
    if _custom_bitrate(current, choices) is not None:
        return None
    return default if default is not None else MP3_DEFAULT_BITRATE


def _custom_bitrate_button(
    current, callback_data: str, *, mark: str = "✏️ Custom bitrate", choices=MP3_QUALITY_CHOICES
) -> InlineKeyboardButton:
    """The Custom row of a bitrate picker, carrying the value when it is custom.

    A bitrate that is not one of the presets has nowhere else to be read back, so
    the row states it: ``✅ Custom: 64k``. Without that, choosing Custom and typing
    a value left the picker looking untouched - the only feedback was the "✅…set"
    reply, and reopening the menu showed a check beside a preset instead of the
    value the user had just entered.
    """
    custom = _custom_bitrate(current, choices)
    if custom:
        return InlineKeyboardButton(f"✅ Custom: {custom}", callback_data=callback_data)
    return InlineKeyboardButton(mark, callback_data=callback_data)


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
        is_video = file_type == "video"

        conv_label = "🎬 Video Converter" if is_video else "🎧 Audio Converter"
        # Use explicit `video_converter` callback so the UI shows the video converter
        # alias; handlers remap this alias to the canonical `convert_format_menu`.
        conv_cb = "video_converter"

        split_label = "🔪 Videos Splitter" if is_video else "🔪 Split"

        buttons: list[list[InlineKeyboardButton]] = [
            row("🖼️ Thumbnail Extractor", THUMBNAIL_EXTRACTOR, "🗑️ Delete Thumbnail", "delete_custom_thumb"),
            row("✏️ Caption And Buttons Editor", CAPTION_EDITOR, "🔀 Batch Process", "batch_process"),
            row("📝 Metadata Editor", EDIT_METADATA, "📤 Media Forwarder", MEDIA_FORWARDER),
            row("🔇 Stream Remover", STREAM_REMOVER, "🎵 Stream Extractor", STREAM_EXTRACTOR),
            # Trim and merge act on the media that is actually loaded: an audio
            # file must not be sent to the video-only trimmer/merger.
            row(
                "✂️ Audio Trimmer" if file_type == "audio" else "✂️ Video Trimmer",
                TRIM_AUDIO if file_type == "audio" else TRIM_VIDEO,
                "➕ Audio Merger" if file_type == "audio" else "➕ Video Merger",
                "merge_audio" if file_type == "audio" else MERGE_MENU,
            ),
            row("🔉 Remove Audio", REMOVE_AUDIO, "🔀 Merge And", MERGE_VIEW),
            row(conv_label, conv_cb, split_label, VIDEOS_SPLITTER),
            row("🖼️ Screenshots", SCREENSHOTS_MENU, "🖼️ Manual Shots", MANUAL_SHOTS),
            row("🎵 Video To Audio", VIDEO_TO_AUDIO, "📉 Compress", COMPRESS_MENU),
            row("⚡ Video Optimizer", OPTIMIZE_MENU, "🔗 Subtitle Merger", SUBTITLE_MERGER),
            row("✏️ Video Renamer", VIDEO_RENAMER, "🛈 Media Information", INFO),
            # The tool sub-menus. They live here rather than behind the settings
            # button: they all act on the loaded media, and /usersettings must
            # stay a preferences panel that never starts a conversion.
            row("🎧 Audio Tools", MENU_AUDIO, "🎬 Video Tools", MENU_VIDEO),
            [InlineKeyboardButton("🔧 Advanced Tools", callback_data=MENU_ADVANCED)],
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
    def get_compression_menu(current=None) -> InlineKeyboardMarkup:
        """Get compression quality menu.

        *current* is the quality the user stored in /usersettings: it is marked
        ``(default)`` so the number the panel shows is the one this menu offers,
        and choosing a quality here still applies to this file only.
        """
        default = _sanitize_quality(current, default=None)

        def _quality(crf: int) -> InlineKeyboardButton:
            mark = " (default)" if crf == default else ""
            return InlineKeyboardButton(f"{COMPRESS_QUALITY_LABELS[crf]}{mark}", callback_data=f"compress_{crf}")

        buttons = [
            [_quality(BULK_CRF_CHOICES[0]), _quality(BULK_CRF_CHOICES[1])],
            [_quality(BULK_CRF_CHOICES[2]), _quality(BULK_CRF_CHOICES[3])],
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
    def get_bitrate_menu(media_type: str = "audio", current: str | None = None) -> InlineKeyboardMarkup:
        """Get bitrate adjustment menu (re-encodes an existing audio file).

        ``current`` is the bitrate this file is currently set to re-encode to - the
        one the menu last set, or the user's own default bitrate - and it is marked
        in the labels, so the menu says which value is in force instead of leaving
        the user to remember it. A value that is not one of the presets is shown on
        the Custom row as itself (``✅ Custom: 64k``); see
        :func:`_custom_bitrate_button`.
        """
        presets = {
            "320": "320k (Best)",
            "256": "256k (Very High)",
            "192": "192k (High)",
            "128": "128k (Standard)",
            "96": "96k (Small)",
        }
        # This picker's own offer - it has no 64k button, so 64k is a custom value
        # here even though the other pickers list it among their presets.
        offered = tuple(f"{value}k" for value in presets)

        def _preset(value: str) -> InlineKeyboardButton:
            label = presets[value]
            return InlineKeyboardButton(
                f"{'✅ ' if _bitrate_preset(current, offered) == f'{value}k' else ''}{label}",
                callback_data=f"bitrate_{value}",
            )

        if media_type == "audio":
            buttons = [
                [_preset("320"), _preset("256")],
                [_preset("192"), _preset("128")],
                [
                    _preset("96"),
                    _custom_bitrate_button(current, "bitrate_custom", mark="✏️ Custom", choices=offered),
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
    def get_mp3_quality_menu(current: str | None = None) -> InlineKeyboardMarkup:
        """Get the MP3 quality menu used when extracting audio from a video.

        ``current`` is the bitrate that will be used if the user re-opens the
        menu, and is marked in the labels so it is obvious which one is active.
        A value that is not one of the presets is marked on the Custom row as
        itself, so a custom pick is readable back.
        """
        active = _active_bitrate(current)

        def _label(value: str) -> str:
            return f"{'✅ ' if value == active else ''}{value}"

        rows = [MP3_QUALITY_CHOICES[i : i + 2] for i in range(0, len(MP3_QUALITY_CHOICES), 2)]
        buttons = [[InlineKeyboardButton(_label(v), callback_data=mp3_quality_key(v)) for v in row] for row in rows]
        buttons.append([_custom_bitrate_button(current, mp3_quality_key("custom"))])
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
    def get_bulk_menu(settings: dict = None) -> InlineKeyboardMarkup:
        """Bulk mode toggles menu. `settings` is a dict of current user settings.

        This menu presents toggle switches for bulk-mode actions instead of
        immediate single-file actions, plus the encoding quality the next Apply
        will use (compress CRF / optimize preset) so those are not silent
        defaults.
        """
        # default empty settings
        s = settings or {}

        items = [
            ("Convert to MP4", "bulk_convert_mp4"),
            ("Compress", "bulk_compress"),
            ("Extract Audio", "bulk_extract_audio"),
            ("Remove Audio", "bulk_remove_audio"),
            ("Rename Files", "bulk_rename"),
            ("Optimize", "bulk_optimize"),
        ]

        buttons = []
        # build two-toggle rows
        row = []
        for _i, (label, key) in enumerate(items):
            val = bool(s.get(key))
            text = f"{label} — {'On' if val else 'Off'}"
            row.append(InlineKeyboardButton(text, callback_data=f"bulk_toggle:{key}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)

        # Encoding quality for the next Apply (shown so the pick is never a
        # hidden default).
        crf = s.get("bulk_crf")
        crf = crf if isinstance(crf, int) and not isinstance(crf, bool) else BULK_CRF_DEFAULT
        preset = s.get("bulk_optimize_preset") or BULK_PRESET_DEFAULT
        preset_label = BULK_PRESET_LABELS.get(preset, BULK_PRESET_LABELS[BULK_PRESET_DEFAULT])
        buttons.append(
            [
                InlineKeyboardButton(f"🎚️ Compress CRF: {crf}", callback_data=BULK_CRF_MENU),
                InlineKeyboardButton(f"⚡ Optimize: {preset_label}", callback_data=BULK_PRESET_MENU),
            ]
        )
        bitrate = s.get("bulk_extract_bitrate") or BULK_BITRATE_DEFAULT
        buttons.append([InlineKeyboardButton(f"🎵 Extract Audio: {bitrate}", callback_data=BULK_BITRATE_MENU)])
        try:
            slideshow = float(s.get("bulk_slideshow_seconds"))
        except (TypeError, ValueError):
            slideshow = BULK_SLIDESHOW_DEFAULT
        buttons.append(
            [InlineKeyboardButton(f"🎞️ Slideshow: {slideshow:g}s per photo", callback_data=BULK_SLIDESHOW_MENU)]
        )

        # actions: apply, clear the collected batch, and back
        buttons.append(
            [
                InlineKeyboardButton("▶️ Apply Bulk", callback_data="bulk_apply"),
                InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN),
            ]
        )
        buttons.append([InlineKeyboardButton("🗑️ Clear List", callback_data="bulk_clear")])
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_bulk_crf_menu(current=None) -> InlineKeyboardMarkup:
        """Compress quality (CRF) picker for bulk mode, marking the active value."""
        try:
            active = int(current)
        except (TypeError, ValueError):
            active = BULK_CRF_DEFAULT

        def _quality(crf: int) -> InlineKeyboardButton:
            mark = "✅ " if crf == active else ""
            return InlineKeyboardButton(
                f"{mark}{COMPRESS_QUALITY_LABELS[crf]} ({crf})", callback_data=bulk_crf_key(crf)
            )

        buttons = [
            [_quality(BULK_CRF_CHOICES[0]), _quality(BULK_CRF_CHOICES[1])],
            [_quality(BULK_CRF_CHOICES[2]), _quality(BULK_CRF_CHOICES[3])],
            [InlineKeyboardButton("✏️ Custom CRF", callback_data=bulk_crf_key("custom"))],
            [InlineKeyboardButton("↩️ Back", callback_data="bulk_menu")],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_bulk_bitrate_menu(current: str = None) -> InlineKeyboardMarkup:
        """Extract-audio bitrate picker for bulk mode, marking the active value.

        Same choices as the single-file video -> MP3 picker so the two never
        drift, but with bulk triggers so choosing one never starts a conversion.
        """
        active = _active_bitrate(current, default=BULK_BITRATE_DEFAULT)

        def _label(value: str) -> str:
            return f"{'✅ ' if value == active else ''}{value}"

        rows = [MP3_QUALITY_CHOICES[i : i + 2] for i in range(0, len(MP3_QUALITY_CHOICES), 2)]
        buttons = [[InlineKeyboardButton(_label(v), callback_data=bulk_bitrate_key(v)) for v in row] for row in rows]
        buttons.append([_custom_bitrate_button(current, bulk_bitrate_key("custom"))])
        buttons.append([InlineKeyboardButton("↩️ Back", callback_data="bulk_menu")])
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_bulk_preset_menu(current: str = None) -> InlineKeyboardMarkup:
        """Optimize preset picker for bulk mode, marking the active preset."""
        active = current if current in BULK_PRESET_CHOICES else BULK_PRESET_DEFAULT
        icons = {"web": "🌐", "mobile": "📱", "tv": "📺", "storage": "💾"}

        def _preset(name: str) -> InlineKeyboardButton:
            mark = "✅ " if name == active else ""
            icon = icons.get(name, "")
            label = BULK_PRESET_LABELS.get(name, name.title())
            return InlineKeyboardButton(f"{mark}{icon} {label}", callback_data=bulk_preset_key(name))

        buttons = [
            [_preset("web"), _preset("mobile")],
            [_preset("tv"), _preset("storage")],
            [InlineKeyboardButton("↩️ Back", callback_data="bulk_menu")],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_bulk_slideshow_menu(current=None) -> InlineKeyboardMarkup:
        """Slideshow seconds-per-photo picker for bulk mode, marking the active value."""
        try:
            active = float(current)
        except (TypeError, ValueError):
            active = BULK_SLIDESHOW_DEFAULT

        def _choice(value: float) -> InlineKeyboardButton:
            mark = "✅ " if abs(value - active) < 1e-9 else ""
            return InlineKeyboardButton(f"{mark}{value:g}s", callback_data=bulk_slideshow_key(value))

        rows = [BULK_SLIDESHOW_CHOICES[i : i + 3] for i in range(0, len(BULK_SLIDESHOW_CHOICES), 3)]
        buttons = [[_choice(v) for v in row] for row in rows]
        buttons.append([InlineKeyboardButton("↩️ Back", callback_data="bulk_menu")])
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
    def get_optimize_menu(current: str = None) -> InlineKeyboardMarkup:
        """Get optimization presets menu, marking the user's stored preset."""
        default = current if current in BULK_PRESET_CHOICES else None
        icons = {"web": "🌐", "mobile": "📱", "tv": "📺", "storage": "💾"}

        def _preset(name: str) -> InlineKeyboardButton:
            mark = " (default)" if name == default else ""
            label = BULK_PRESET_LABELS.get(name, name.title())
            return InlineKeyboardButton(f"{icons.get(name, '')} {label}{mark}", callback_data=f"optimize_{name}")

        buttons = [
            [_preset("web"), _preset("mobile")],
            [_preset("tv"), _preset("storage")],
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
                InlineKeyboardButton("🔄 Convert To Video", callback_data="convert_to_video"),
                InlineKeyboardButton("🔄 Convert To File", callback_data="convert_to_file"),
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
            # Opens the extraction picker: Audio Only / Video Only /
            # Subtitles / All Streams.
            [InlineKeyboardButton("🗂️ Extract Streams", callback_data=EXTRACT_MENU)],
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
            # The Mp3 tag editor used to be reachable only from /usersettings,
            # where it read the loaded file's tags and rewrote them. It is an
            # action, so it belongs with the other audio actions.
            [InlineKeyboardButton("🏷️ Mp3 Tag Editor", callback_data=MP3_TAG_EDITOR)],
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
                InlineKeyboardButton("🖼️ Thumbnail", callback_data=THUMBNAIL_GRID),
                InlineKeyboardButton("✏️ Metadata", callback_data=EDIT_METADATA),
            ],
            [InlineKeyboardButton("↩️ Back", callback_data=MENU_MAIN)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_fade_menu() -> InlineKeyboardMarkup:
        """Get fade in/out options menu."""
        buttons = [
            [
                InlineKeyboardButton("🔈 Fade In", callback_data=FADE_IN),
                InlineKeyboardButton("🔉 Fade Out", callback_data=FADE_OUT),
            ],
            [
                InlineKeyboardButton("🔊 Both", callback_data=FADE_BOTH),
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
    def get_archive_name_menu() -> InlineKeyboardMarkup:
        """The two answers to the Create Archive name prompt.

        The default is offered as a button because most archives are named after
        the first file anyway - typing a name should be a choice, not a toll on
        the way to an archive the user already described with the batch.
        """
        buttons = [
            [InlineKeyboardButton("✅ Use default name", callback_data=ARCHIVE_NAME_DEFAULT)],
            [InlineKeyboardButton("📐 Part size", callback_data=ARCHIVE_PART_MENU)],
            [InlineKeyboardButton("❌ Cancel", callback_data=ARCHIVE_CANCEL)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_archive_confirm_menu() -> InlineKeyboardMarkup:
        """The two answers to the Create Archive summary.

        Its own triggers (not the generic ``confirm``/``cancel``) because those
        are shared with other prompts: an answer meant for the archive must not
        be read by whatever asked last.
        """
        buttons = [
            [
                InlineKeyboardButton("📦 Pack archive", callback_data=ARCHIVE_CONFIRM),
                InlineKeyboardButton("📐 Part size", callback_data=ARCHIVE_PART_MENU),
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data=ARCHIVE_CANCEL)],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_archive_part_menu(current=None) -> InlineKeyboardMarkup:
        """The part-size picker reached from the Create Archive summary.

        Same choices and the same stored setting as /usersettings, but its Back
        returns to the archive summary rather than the settings page - the flow
        the user pressed the button from is the one they come back to.
        """
        active = archive_split.normalize(current)

        def _size_button(size) -> InlineKeyboardButton:
            value = archive_split.preset_value(size)
            mark = "✅ " if value == active else ""
            return InlineKeyboardButton(
                f"{mark}{archive_split.format_size(size)}", callback_data=archive_part_key(value)
            )

        buttons = [[_size_button(size) for size in archive_split.PRESET_SIZES]]
        buttons.append(
            [
                InlineKeyboardButton(
                    f"{'✅ ' if active == archive_split.DEFAULT_VALUE else ''}♻️ Auto (split over the cap)",
                    callback_data=archive_part_key(archive_split.DEFAULT_VALUE),
                )
            ]
        )
        buttons.append(
            [
                InlineKeyboardButton(
                    f"{'✅ ' if active == archive_split.OFF_VALUE else ''}🚫 No split (one .zip)",
                    callback_data=archive_part_key(archive_split.OFF_VALUE),
                )
            ]
        )
        buttons.append([InlineKeyboardButton("✏️ Custom (size or parts)", callback_data=archive_part_key("custom"))])
        buttons.append([InlineKeyboardButton("↩️ Back", callback_data=ARCHIVE_PART_BACK)])
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_back_button() -> InlineKeyboardMarkup:
        """Get simple back button."""
        buttons = [[InlineKeyboardButton("🔙 Back", callback_data=MENU_MAIN)]]
        return InlineKeyboardMarkup(buttons)

    # ── /usersettings ────────────────────────────────────────────────────────
    # Every button below stores a preference (and shows which one is active).
    # None of them encode, download or queue anything: the media actions live in
    # the main menu's tool sub-menus, so opening the settings panel can never
    # touch whatever file happens to be loaded.

    @staticmethod
    def get_settings_page(page: int = 1, settings: dict = None) -> InlineKeyboardMarkup:
        """One page of the preferences panel: 1 general, 2 quality, 3 batch.

        The pager is built here from ``SETTINGS_PAGE_COUNT`` rather than written
        out by hand, so adding a page means adding its rows and nothing else -
        the Prev/Next row follows, and no page is reachable only from a stale
        button on another one.
        """
        s = settings or {}
        page = settings_page_number(page)
        buttons: list[list[InlineKeyboardButton]] = []

        if page == 3:
            seconds = _sanitize_slideshow(s.get(SLIDESHOW_SECONDS_KEY))
            extract = s.get(BULK_EXTRACT_BITRATE_KEY) or BULK_BITRATE_DEFAULT
            buttons.append(
                [InlineKeyboardButton(f"🎞️ Slideshow: {seconds:g}s per photo", callback_data=SETTINGS_SLIDESHOW_MENU)]
            )
            buttons.append(
                [InlineKeyboardButton(f"🎵 Batch Extract Bitrate: {extract}", callback_data=SETTINGS_BULK_BITRATE_MENU)]
            )
            buttons.append(
                [
                    InlineKeyboardButton(
                        f"📦 Archive Part Size: {archive_split.label(s.get('archive_part'))}",
                        callback_data=SETTINGS_ARCHIVE_PART_MENU,
                    )
                ]
            )
        elif page == 2:
            crf = _sanitize_quality(s.get(COMPRESS_QUALITY_KEY))
            preset = s.get(OPTIMIZE_PRESET_KEY)
            preset = preset if preset in BULK_PRESET_CHOICES else BULK_PRESET_DEFAULT
            bitrate = s.get("audio_bitrate") or MP3_DEFAULT_BITRATE
            buttons.append(
                [
                    InlineKeyboardButton(
                        f"🎚️ Compress Quality: {compress_quality_label(crf)}", callback_data=SETTINGS_QUALITY_MENU
                    )
                ]
            )
            buttons.append(
                [
                    InlineKeyboardButton(
                        f"⚡ Optimize Preset: {BULK_PRESET_LABELS.get(preset, preset.title())}",
                        callback_data=SETTINGS_PRESET_MENU,
                    )
                ]
            )
            buttons.append([InlineKeyboardButton(f"🎧 Audio Bitrate: {bitrate}", callback_data=SETTINGS_BITRATE_MENU)])
        else:
            mode = str(s.get("upload_mode") or UPLOAD_MODE_VIDEO).lower()
            if mode not in (UPLOAD_MODE_VIDEO, UPLOAD_MODE_FILE):
                mode = UPLOAD_MODE_VIDEO
            other = UPLOAD_MODE_FILE if mode == UPLOAD_MODE_VIDEO else UPLOAD_MODE_VIDEO
            buttons.append(
                [
                    InlineKeyboardButton(
                        f"📤 Upload as Video: {UPLOAD_MODE_LABELS[mode]}",
                        callback_data=f"{SETTINGS_UPLOAD_MODE_PREFIX}{other}",
                    )
                ]
            )
            thumb = "On" if s.get("use_custom_thumbnail") else "Off"
            buttons.append(
                [
                    InlineKeyboardButton(
                        f"🖼️ Custom Thumbnail: {thumb}", callback_data="settings_toggle:use_custom_thumbnail"
                    )
                ]
            )
            buttons.append([InlineKeyboardButton("✏️ Rename Files", callback_data=SETTINGS_RENAME_MENU)])

        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=settings_page_key(page - 1)))
        if page < SETTINGS_PAGE_COUNT:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=settings_page_key(page + 1)))
        if nav:
            buttons.append(nav)

        buttons.append(
            [
                InlineKeyboardButton("♻️ Reset Settings", callback_data="reset_settings"),
                InlineKeyboardButton("✖️ Close", callback_data=MENU_MAIN),
            ]
        )
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_settings_quality_menu(current=None) -> InlineKeyboardMarkup:
        """Compress-quality (CRF) picker for /usersettings.

        The same four qualities the Compress menu and the bulk picker offer, with
        settings triggers so choosing one only stores it - it is the value a
        conversion starts from when nothing was picked for the file itself.
        """
        active = _sanitize_quality(current)

        def _quality(crf: int) -> InlineKeyboardButton:
            mark = "✅ " if crf == active else ""
            return InlineKeyboardButton(
                f"{mark}{COMPRESS_QUALITY_LABELS[crf]} ({crf})", callback_data=settings_quality_key(crf)
            )

        buttons = [
            [_quality(BULK_CRF_CHOICES[0]), _quality(BULK_CRF_CHOICES[1])],
            [_quality(BULK_CRF_CHOICES[2]), _quality(BULK_CRF_CHOICES[3])],
            [InlineKeyboardButton("✏️ Custom quality", callback_data=settings_quality_key("custom"))],
            [InlineKeyboardButton("↩️ Back", callback_data=settings_page_key(2))],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_settings_slideshow_menu(current=None) -> InlineKeyboardMarkup:
        """Slideshow seconds-per-photo picker for /usersettings.

        Same choices and same key as the bulk picker, so the length a batch uses is
        the one this page shows, and a dash of custom covers the values in between.
        """
        active = _sanitize_slideshow(current)

        def _choice(value: float) -> InlineKeyboardButton:
            mark = "✅ " if abs(value - active) < 1e-9 else ""
            return InlineKeyboardButton(f"{mark}{value:g}s", callback_data=settings_slideshow_key(value))

        rows = [BULK_SLIDESHOW_CHOICES[i : i + 3] for i in range(0, len(BULK_SLIDESHOW_CHOICES), 3)]
        buttons = [[_choice(v) for v in row] for row in rows]
        buttons.append([InlineKeyboardButton("✏️ Custom", callback_data=settings_slideshow_key("custom"))])
        buttons.append([InlineKeyboardButton("↩️ Back", callback_data=settings_page_key(3))])
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_settings_bulk_bitrate_menu(current: str = None) -> InlineKeyboardMarkup:
        """Batch extraction-bitrate picker for /usersettings, marking the active value."""
        active = current if current in MP3_QUALITY_CHOICES else BULK_BITRATE_DEFAULT

        def _label(value: str) -> str:
            return f"{'✅ ' if value == active else ''}{value}"

        rows = [MP3_QUALITY_CHOICES[i : i + 2] for i in range(0, len(MP3_QUALITY_CHOICES), 2)]
        buttons = [
            [InlineKeyboardButton(_label(v), callback_data=settings_bulk_bitrate_key(v)) for v in row] for row in rows
        ]
        buttons.append([InlineKeyboardButton("✏️ Custom bitrate", callback_data=settings_bulk_bitrate_key("custom"))])
        buttons.append([InlineKeyboardButton("↩️ Back", callback_data=settings_page_key(3))])
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_settings_archive_part_menu(current=None) -> InlineKeyboardMarkup:
        """Archive part-size picker for /usersettings.

        One-tap sizes for the common cases plus a *custom* prompt that accepts a
        typed size (``500MB``, ``1.5GB``) or a number of equal parts. The value
        is what Create Archive splits a packed ZIP by (utils/archive_split.py),
        so what is chosen here and what a delivery does are the same setting.
        """
        active = archive_split.normalize(current)

        def _size_button(size) -> InlineKeyboardButton:
            value = archive_split.preset_value(size)
            mark = "✅ " if value == active else ""
            return InlineKeyboardButton(
                f"{mark}{archive_split.format_size(size)}", callback_data=settings_archive_part_key(value)
            )

        buttons = [[_size_button(size) for size in archive_split.PRESET_SIZES]]
        buttons.append(
            [
                InlineKeyboardButton(
                    f"{'✅ ' if active == archive_split.DEFAULT_VALUE else ''}♻️ Auto (split over the cap)",
                    callback_data=settings_archive_part_key(archive_split.DEFAULT_VALUE),
                )
            ]
        )
        buttons.append(
            [
                InlineKeyboardButton(
                    f"{'✅ ' if active == archive_split.OFF_VALUE else ''}🚫 No split (one .zip)",
                    callback_data=settings_archive_part_key(archive_split.OFF_VALUE),
                )
            ]
        )
        buttons.append(
            [InlineKeyboardButton("✏️ Custom (size or parts)", callback_data=settings_archive_part_key("custom"))]
        )
        buttons.append([InlineKeyboardButton("↩️ Back", callback_data=settings_page_key(3))])
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_settings_preset_menu(current: str = None) -> InlineKeyboardMarkup:
        """Optimize-preset picker for /usersettings, marking the active preset."""
        active = current if current in BULK_PRESET_CHOICES else BULK_PRESET_DEFAULT
        icons = {"web": "🌐", "mobile": "📱", "tv": "📺", "storage": "💾"}

        def _preset(name: str) -> InlineKeyboardButton:
            mark = "✅ " if name == active else ""
            return InlineKeyboardButton(
                f"{mark}{icons.get(name, '')} {BULK_PRESET_LABELS.get(name, name.title())}",
                callback_data=settings_preset_key(name),
            )

        buttons = [
            [_preset("web"), _preset("mobile")],
            [_preset("tv"), _preset("storage")],
            [InlineKeyboardButton("↩️ Back", callback_data=settings_page_key(2))],
        ]
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_settings_bitrate_menu(current: str = None) -> InlineKeyboardMarkup:
        """Audio-bitrate picker for /usersettings, marking the active value.

        Same choices as the bulk and video -> MP3 pickers, but with settings
        triggers so choosing one only stores it.
        """
        active = _active_bitrate(current)

        def _label(value: str) -> str:
            return f"{'✅ ' if value == active else ''}{value}"

        rows = [MP3_QUALITY_CHOICES[i : i + 2] for i in range(0, len(MP3_QUALITY_CHOICES), 2)]
        buttons = [
            [InlineKeyboardButton(_label(v), callback_data=settings_bitrate_key(v)) for v in row] for row in rows
        ]
        buttons.append([_custom_bitrate_button(current, settings_bitrate_key("custom"))])
        buttons.append([InlineKeyboardButton("↩️ Back", callback_data="settings_page:2")])
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def get_settings_rename_menu(settings: dict = None) -> InlineKeyboardMarkup:
        """Prefix/suffix the bot applies to delivered filenames."""
        s = settings or {}
        prefix = s.get("prefix") or ""
        suffix = s.get("suffix") or ""
        buttons = [
            [InlineKeyboardButton(f"Prefix: {prefix or '(none)'}", callback_data="settings_set_prefix")],
            [InlineKeyboardButton(f"Suffix: {suffix or '(none)'}", callback_data="settings_set_suffix")],
            [InlineKeyboardButton("🗑️ Clear Both", callback_data="settings_clear_rename")],
            [InlineKeyboardButton("↩️ Back", callback_data="settings_page:1")],
        ]
        return InlineKeyboardMarkup(buttons)


def _sanitize_slideshow(value, default: float = BULK_SLIDESHOW_DEFAULT) -> float:
    """Coerce a stored slideshow length into the range this bot will encode with.

    The same bounds the bulk picker validates against, so a hand-edited settings
    file cannot put a zero- or hour-long photo on an ffmpeg command line.
    """
    if isinstance(value, bool):
        return default
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return default
    if not (BULK_SLIDESHOW_MIN <= seconds <= BULK_SLIDESHOW_MAX):
        return default
    return seconds


def _sanitize_quality(value, default: int = BULK_CRF_DEFAULT) -> int:
    """Coerce a stored compress quality to a CRF this bot may encode with.

    Anything unusable (missing, non-numeric, out of the accepted range) falls
    back to the default, so a hand-edited settings file can never put arbitrary
    text on an ffmpeg command line.
    """
    if isinstance(value, bool):
        return default
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    if not BULK_CRF_MIN <= number <= BULK_CRF_MAX:
        return default
    return number
