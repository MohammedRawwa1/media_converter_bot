"""Bilingual (English / Arabic) catalog for the media bot's user-facing text.

The bot was English-only: ``/start``, ``/help``, the whole media menu and every
access/usage message. This module holds the Arabic side in one place so a label
is not translated on one screen and left English on the next.

Language selection, in order: explicit language, then the script of the text
being handled (an Arabic caption gets Arabic), then ``BOT_LANG`` (``en`` by
default, ``ar`` to flip the UI). Arabic labels are only used when Arabic is
selected, so default behaviour - and every existing English test - is unchanged.

Only text lives here; callers do their own escaping.
"""

import os
import re

SUPPORTED = ("en", "ar")

_AR_LETTER_RE = re.compile("[\u0600-\u06ff\u0750-\u077f\u08a0-\u08ff\ufb50-\ufdff\ufe70-\ufeff]")
_LATIN_LETTER_RE = re.compile(r"[A-Za-z]")

MESSAGES: dict[str, dict[str, str]] = {
    "en": {
        # access / usage
        "access_denied": "Access denied. This bot is private.",
        "access_denied_acl": "Access denied. (ACL check failed)",
        "unauthorized_admin": "Unauthorized: admin only",
        "usage_admin": "Usage: /admin add|remove|list <user_id>",
        "invalid_user_id": "Invalid user id",
        "usage_canceljob": "Usage: /canceljob <job_id>",
        "usage_cancelbatch": "Usage: /cancelbatch <batch_id>",
        "cancelled": "Cancelled",
        # menu labels
        "menu_thumbnail_extractor": "\U0001f5bc\ufe0f Thumbnail Extractor",
        "menu_delete_thumbnail": "\U0001f5d1\ufe0f Delete Thumbnail",
        "menu_caption_editor": "\U0001f4ac Caption Editor",
        "menu_batch_process": "\U0001f500 Batch Process",
        "menu_metadata_editor": "\U0001f4dd Metadata Editor",
        "menu_media_forwarder": "\U0001f4e4 Media Forwarder",
        "menu_stream_remover": "\U0001f507 Stream Remover",
        "menu_stream_extractor": "\U0001f3b5 Stream Extractor",
        "menu_trim_media": "\u2702\ufe0f Media Trimmer",
        "menu_merge_audio": "\u2795 Audio Merger",
        "menu_merge": "\U0001f500 Merge",
        "menu_remove_audio": "\U0001f509 Remove Audio",
        "menu_video_converter": "\U0001f3ac Video Converter",
        "menu_audio_converter": "\U0001f3a7 Audio Converter",
        "menu_splitter": "\U0001f52a Media Splitter",
        "menu_screenshots": "\U0001f5bc\ufe0f Screenshots",
        "menu_manual_shots": "\U0001f5bc\ufe0f Manual Shots",
        "menu_video_to_audio": "\U0001f3b5 Video To Audio",
        "menu_compress": "\U0001f4c9 Compress",
        "menu_optimizer": "\u26a1 Video Optimizer",
        "menu_subtitle_merger": "\U0001f517 Subtitle Merger",
        "menu_renamer": "\u270f\ufe0f Media Renamer",
        "menu_info": "\U0001f6c8 Media Information",
        "menu_audio_tools": "\U0001f3a7 Audio Tools",
        "menu_video_tools": "\U0001f3ac Video Tools",
        "menu_create_archive": "\U0001f4e6 Create Archive",
        "menu_bulk_actions": "\U0001f4e6 Bulk Actions",
        "menu_cancel": "\u274c Cancel",
        "menu_back": "\u21a9\ufe0f Back",
        "menu_close": "Close",
        "menu_progress": "\U0001f4ca Progress",
        "menu_prev": "\u2b05\ufe0f Prev",
        "menu_next": "Next \u27a1\ufe0f",
        # prompts
        "send_name": "Send the new filename. Current name:",
        "send_caption": "Send the caption this media should be delivered with.",
        "rate_limited": "Please wait {seconds:.1f} seconds before next conversion",
        # Arabic side of /start and /help (English stays in main.py)
        "start_ar": (
            "\U0001f3ac <b>\u0645\u0631\u062d\u0628\u064b\u0627 \u0628\u0643 \u0641\u064a \u0628\u0648\u062a \u062a\u062d\u0648\u064a\u0644 \u0627\u0644\u0648\u0633\u0627\u0626\u0637</b> \U0001f3a7\n\n"
            "\u0645\u0631\u062d\u0628\u064b\u0627 {name}! \u0623\u0631\u0633\u0644 \u0645\u0644\u0641 \u0648\u0633\u0627\u0626\u0637 \u0648\u0627\u062e\u062a\u0631 \u0627\u0644\u0625\u062c\u0631\u0627\u0621 \u0645\u0646 \u0627\u0644\u0642\u0627\u0626\u0645\u0629.\n\n"
            "<b>\u26a1 \u0623\u0648\u0627\u0645\u0631 \u0633\u0631\u064a\u0639\u0629:</b>\n"
            "/help \u2014 \u062f\u0644\u064a\u0644 \u0627\u0644\u0645\u064a\u0632\u0627\u062a \u0627\u0644\u0645\u0641\u0635\u0651\u0644\n"
            "/usersettings \u2014 \u062a\u0641\u0636\u064a\u0644\u0627\u062a\u0643\n"
            "/cancel \u2014 \u0625\u064a\u0642\u0627\u0641 \u0627\u0644\u0639\u0645\u0644\u064a\u0629 \u0627\u0644\u062d\u0627\u0644\u064a\u0629\n"
            "/canceljob <code>&lt;job_id&gt;</code> \u2014 \u0625\u0644\u063a\u0627\u0621 \u0645\u0647\u0645\u0629 \u0645\u0646\u062a\u0638\u0631\u0629\n\n"
            "<b>\U0001f510 \u062a\u0633\u062c\u064a\u0644 \u0627\u0644\u062f\u062e\u0648\u0644 (\u0644\u0644\u0645\u0644\u0641\u0627\u062a \u0627\u0644\u0643\u0628\u064a\u0631\u0629):</b>\n"
            "/login <code>[\u0631\u0642\u0645 \u0627\u0644\u0647\u0627\u062a\u0641]</code> \u2014 \u062a\u0633\u062c\u064a\u0644 \u0628\u0627\u0644\u062a\u064a\u0644\u064a\u062c\u0631\u0627\u0645\n"
            "/loginpyro <code>[\u0631\u0642\u0645 \u0627\u0644\u0647\u0627\u062a\u0641]</code> \u2014 \u062a\u0633\u062c\u064a\u0644 \u0628\u0628\u0627\u064a\u0631\u0648\u062c\u0631\u0627\u0645\n"
            "/logout \u2014 \u062e\u0631\u0648\u062c \u062c\u0644\u0633\u0629 \u0627\u0644\u062a\u064a\u0644\u064a\u062c\u0631\u0627\u0645\n"
            "/logoutpyro \u2014 \u062e\u0631\u0648\u062c \u062c\u0644\u0633\u0629 \u0628\u0627\u064a\u0631\u0648\u062c\u0631\u0627\u0645\n"
            "/loginstatus \u2014 \u062d\u0627\u0644\u0629 \u0627\u0644\u062c\u0644\u0633\u0629\n\n"
            "<b>\U0001f4e6 \u0627\u0644\u062f\u0641\u0639\u0629 \u0648\u0627\u0644\u0631\u0648\u0627\u0628\u0637:</b>\n"
            "/bulkmenu \u2014 \u0645\u0639\u0627\u0644\u062c\u0629 \u062f\u0641\u0639\u0629 \u0645\u0646 \u0627\u0644\u0631\u0648\u0627\u0628\u0637\n\n"
            "\u0623\u0631\u0633\u0644 \u0645\u0644\u0641\u064b\u0627 \u0644\u0644\u0628\u062f\u0621! \U0001f680"
        ),
        "help_ar": (
            "\U0001f4da <b>\u0627\u0644\u0642\u0627\u0626\u0645\u0629 \u0627\u0644\u0643\u0627\u0645\u0644\u0629 \u0644\u0644\u0645\u064a\u0632\u0627\u062a:</b>\n\n"
            "<b>\U0001f3ac \u0645\u0639\u0627\u0644\u062c\u0629 \u0627\u0644\u0641\u064a\u062f\u064a\u0648:</b>\n"
            "\u2022 \u0627\u0644\u062a\u062d\u0648\u064a\u0644 \u0628\u064a\u0646 \u0627\u0644\u0635\u064a\u063a (MP4\u060c AVI\u060c MOV\u060c MKV...)\n"
            "\u2022 \u0627\u0644\u062a\u062d\u0648\u064a\u0644 \u0625\u0644\u0649 MP3 \u0648\u0635\u064a\u063a \u0627\u0644\u0635\u0648\u062a\n"
            "\u2022 \u0627\u0644\u0636\u063a\u0637 \u0628\u0625\u0639\u062f\u0627\u062f\u0627\u062a \u062c\u0648\u062f\u0629 \u0645\u062a\u062f\u0631\u062c\u0629\n"
            "\u2022 \u062a\u063a\u064a\u064a\u0631 \u0627\u0644\u062f\u0642\u0629 \u0648\u0645\u0639\u062f\u0644 \u0627\u0644\u0625\u0637\u0627\u0631\n"
            "\u2022 \u0627\u0644\u0642\u0635 \u0648\u0627\u0644\u062f\u0645\u062c \u0648\u0627\u0633\u062a\u062e\u0631\u0627\u062c \u0627\u0644\u0645\u0642\u0627\u0637\u0639\n"
            "\u2022 \u0627\u0644\u062a\u0642\u0627\u0637 \u0627\u0644\u0634\u0627\u0634\u0629 \u0648\u0625\u0635\u0644\u0627\u062d \u0627\u0644\u0645\u0644\u0641\u0627\u062a \u0627\u0644\u062a\u0627\u0644\u0641\u0629\n\n"
            "<b>\U0001f3a7 \u0645\u0639\u0627\u0644\u062c\u0629 \u0627\u0644\u0635\u0648\u062a:</b>\n"
            "\u2022 \u0627\u0644\u062a\u062d\u0648\u064a\u0644 \u0628\u064a\u0646 \u0627\u0644\u0635\u064a\u063a (MP3\u060c WAV\u060c AAC\u060c FLAC\u060c OGG\u060c M4A)\n"
            "\u2022 \u062a\u0639\u062f\u064a\u0644 \u0627\u0644\u0628\u062a\u0631\u064a\u062a \u0648\u0645\u0639\u062f\u0644 \u0627\u0644\u0639\u064a\u0646\u0627\u062a \u0648\u0627\u0644\u0642\u0646\u0648\u0627\u062a\n"
            "\u2022 \u0645\u0639\u0627\u064a\u0631\u0629 \u0627\u0644\u0645\u0633\u062a\u0648\u0649 \u0648\u0627\u0644\u0642\u0635 \u0648\u0627\u0644\u062f\u0645\u062c\n\n"
            "<b>\U0001f527 \u0623\u062f\u0648\u0627\u062a \u0625\u0636\u0627\u0641\u064a\u0629:</b>\n"
            "\u2022 \u0645\u0639\u0644\u0648\u0645\u0627\u062a \u0627\u0644\u0645\u0644\u0641 \u0627\u0644\u0643\u0627\u0645\u0644\u0629\u060c \u0625\u0646\u0634\u0627\u0621 ZIP\u060c \u0627\u0644\u0645\u0639\u0627\u0644\u062c\u0629 \u0627\u0644\u062f\u0641\u0639\u064a\u0629\n\n"
            "<b>\u062d\u062f\u0648\u062f \u0627\u0644\u0645\u0644\u0641\u0627\u062a:</b> \u0627\u0644\u062d\u062f \u0627\u0644\u0623\u0642\u0635\u0649 4 \u062c\u064a\u062c\u0627\u0628\u0627\u064a\u062a\u060c \u0648\u0627\u0644\u0646\u062a\u0627\u0626\u062c \u062a\u064f\u062d\u0630\u0641 \u0628\u0639\u062f \u0627\u0644\u0625\u0631\u0633\u0627\u0644.\n\n"
            "<b>\U0001f510 \u062f\u0644\u064a\u0644 \u062a\u0633\u062c\u064a\u0644 \u0627\u0644\u062f\u062e\u0648\u0644:</b>\n"
            '\u0627\u0633\u062a\u062e\u062f\u0645 <code>/login [\u0631\u0642\u0645 \u0627\u0644\u0647\u0627\u062a\u0641]</code> \u0623\u0648 <code>/loginpyro [\u0631\u0642\u0645 \u0627\u0644\u0647\u0627\u062a\u0641]</code>\u060c \u062b\u0645 \u0623\u062f\u062e\u0644 \u0631\u0645\u0632 \u0627\u0644\u062a\u062d\u0642\u0642 \u0628\u0623\u0631\u0642\u0627\u0645 \u0645\u0641\u0635\u0648\u0644\u0629 \u0628\u0645\u0633\u0627\u0641\u0627\u062a \u0645\u062b\u0644 <code>"1 2 3 4 5"</code>.'
        ),
    },
    "ar": {
        "access_denied": "\u0627\u0644\u0648\u0635\u0648\u0644 \u0645\u0631\u0641\u0648\u0636. \u0647\u0630\u0627 \u0627\u0644\u0628\u0648\u062a \u062e\u0627\u0635.",
        "access_denied_acl": "\u0627\u0644\u0648\u0635\u0648\u0644 \u0645\u0631\u0641\u0648\u0636. (\u0641\u0634\u0644 \u0641\u062d\u0635 \u0627\u0644\u0635\u0644\u0627\u062d\u064a\u0627\u062a)",
        "unauthorized_admin": "\u063a\u064a\u0631 \u0645\u0635\u0631\u0651\u062d: \u0644\u0644\u0645\u0634\u0631\u0641 \u0641\u0642\u0637",
        "usage_admin": "\u0627\u0644\u0627\u0633\u062a\u062e\u062f\u0627\u0645: /admin add|remove|list <user_id>",
        "invalid_user_id": "\u0645\u0639\u0631\u0651\u0641 \u0645\u0633\u062a\u062e\u062f\u0645 \u063a\u064a\u0631 \u0635\u0627\u0644\u062d",
        "usage_canceljob": "\u0627\u0644\u0627\u0633\u062a\u062e\u062f\u0627\u0645: /canceljob <job_id>",
        "usage_cancelbatch": "\u0627\u0644\u0627\u0633\u062a\u062e\u062f\u0627\u0645: /cancelbatch <batch_id>",
        "cancelled": "\u062a\u0645 \u0627\u0644\u0625\u0644\u063a\u0627\u0621",
        "menu_thumbnail_extractor": "\U0001f5bc\ufe0f \u0627\u0633\u062a\u062e\u0631\u0627\u062c \u0627\u0644\u0645\u0635\u063a\u0631\u0629",
        "menu_delete_thumbnail": "\U0001f5d1\ufe0f \u062d\u0630\u0641 \u0627\u0644\u0645\u0635\u063a\u0631\u0629",
        "menu_caption_editor": "\U0001f4ac \u062a\u062d\u0631\u064a\u0631 \u0627\u0644\u062a\u0639\u0644\u064a\u0642",
        "menu_batch_process": "\U0001f500 \u0645\u0639\u0627\u0644\u062c\u0629 \u062f\u0641\u0639\u064a\u0629",
        "menu_metadata_editor": "\U0001f4dd \u062a\u062d\u0631\u064a\u0631 \u0627\u0644\u0628\u064a\u0627\u0646\u0627\u062a",
        "menu_media_forwarder": "\U0001f4e4 \u0625\u0639\u0627\u062f\u0629 \u0625\u0631\u0633\u0627\u0644",
        "menu_stream_remover": "\U0001f507 \u0625\u0632\u0627\u0644\u0629 \u0645\u0633\u0627\u0631",
        "menu_stream_extractor": "\U0001f3b5 \u0627\u0633\u062a\u062e\u0631\u0627\u062c \u0645\u0633\u0627\u0631",
        "menu_trim_media": "\u2702\ufe0f \u0642\u0635 \u0627\u0644\u0648\u0633\u0627\u0626\u0637",
        "menu_merge_audio": "\u2795 \u062f\u0645\u062c \u0635\u0648\u062a",
        "menu_merge": "\U0001f500 \u062f\u0645\u062c",
        "menu_remove_audio": "\U0001f509 \u0625\u0632\u0627\u0644\u0629 \u0627\u0644\u0635\u0648\u062a",
        "menu_video_converter": "\U0001f3ac \u0645\u062d\u0648\u0651\u0644 \u0627\u0644\u0641\u064a\u062f\u064a\u0648",
        "menu_audio_converter": "\U0001f3a7 \u0645\u062d\u0648\u0651\u0644 \u0627\u0644\u0635\u0648\u062a",
        "menu_splitter": "\U0001f52a \u062a\u0642\u0633\u064a\u0645 \u0627\u0644\u0648\u0633\u0627\u0626\u0637",
        "menu_screenshots": "\U0001f5bc\ufe0f \u0644\u0642\u0637\u0627\u062a \u0627\u0644\u0634\u0627\u0634\u0629",
        "menu_manual_shots": "\U0001f5bc\ufe0f \u0644\u0642\u0637\u0627\u062a \u064a\u062f\u0648\u064a\u0629",
        "menu_video_to_audio": "\U0001f3b5 \u0645\u0646 \u0641\u064a\u062f\u064a\u0648 \u0625\u0644\u0649 \u0635\u0648\u062a",
        "menu_compress": "\U0001f4c9 \u0636\u063a\u0637",
        "menu_optimizer": "\u26a1 \u0645\u062d\u0633\u0651\u0646 \u0627\u0644\u0641\u064a\u062f\u064a\u0648",
        "menu_subtitle_merger": "\U0001f517 \u062f\u0645\u062c \u0627\u0644\u062a\u0631\u062c\u0645\u0629",
        "menu_renamer": "\u270f\ufe0f \u0625\u0639\u0627\u062f\u0629 \u062a\u0633\u0645\u064a\u0629",
        "menu_info": "\U0001f6c8 \u0645\u0639\u0644\u0648\u0645\u0627\u062a \u0627\u0644\u0645\u0644\u0641",
        "menu_audio_tools": "\U0001f3a7 \u0623\u062f\u0648\u0627\u062a \u0627\u0644\u0635\u0648\u062a",
        "menu_video_tools": "\U0001f3ac \u0623\u062f\u0648\u0627\u062a \u0627\u0644\u0641\u064a\u062f\u064a\u0648",
        "menu_create_archive": "\U0001f4e6 \u0625\u0646\u0634\u0627\u0621 \u0623\u0631\u0634\u064a\u0641",
        "menu_bulk_actions": "\U0001f4e6 \u0625\u062c\u0631\u0627\u0621\u0627\u062a \u062f\u0641\u0639\u064a\u0629",
        "menu_cancel": "\u274c \u0625\u0644\u063a\u0627\u0621",
        "menu_back": "\u21a9\ufe0f \u0631\u062c\u0648\u0639",
        "menu_close": "\u0625\u063a\u0644\u0627\u0642",
        "menu_progress": "\U0001f4ca \u0627\u0644\u062a\u0642\u062f\u0645",
        "menu_prev": "\u2b05\ufe0f \u0627\u0644\u0633\u0627\u0628\u0642",
        "menu_next": "\u0627\u0644\u062a\u0627\u0644\u064a \u27a1\ufe0f",
        "send_name": "\u0623\u0631\u0633\u0644 \u0627\u0633\u0645 \u0627\u0644\u0645\u0644\u0641 \u0627\u0644\u062c\u062f\u064a\u062f. \u0627\u0644\u0627\u0633\u0645 \u0627\u0644\u062d\u0627\u0644\u064a:",
        "send_caption": "\u0623\u0631\u0633\u0644 \u0627\u0644\u062a\u0639\u0644\u064a\u0642 \u0627\u0644\u0630\u064a \u062a\u0631\u064a\u062f \u0625\u0631\u0641\u0627\u0642\u0647 \u0645\u0639 \u0627\u0644\u0648\u0633\u064a\u0637.",
        "rate_limited": "\u064a\u0631\u062c\u0649 \u0627\u0644\u0627\u0646\u062a\u0638\u0627\u0631 {seconds:.1f} \u062b\u0627\u0646\u064a\u0629 \u0642\u0628\u0644 \u0627\u0644\u062a\u062d\u0648\u064a\u0644 \u0627\u0644\u062a\u0627\u0644\u064a",
    },
}


def detect_lang(text: str | None, *, threshold: float = 0.30) -> str:
    """``"ar"`` when *text* is Arabic (or a mix with enough Arabic), else ``"en"``."""
    if not text:
        return "en"
    arabic = len(_AR_LETTER_RE.findall(str(text)))
    latin = len(_LATIN_LETTER_RE.findall(str(text)))
    if arabic == 0:
        return "en"
    return "ar" if (arabic / max(1, arabic + latin)) >= threshold else "en"


def env_lang() -> str:
    lang = (os.getenv("BOT_LANG") or "en").strip().lower()
    return lang if lang in SUPPORTED else "en"


def resolve_lang(explicit: str | None = None, text: str | None = None) -> str:
    if explicit in SUPPORTED:
        return explicit
    if text and detect_lang(text) == "ar":
        return "ar"
    return env_lang()


def t(key: str, lang: str | None = None, text: str | None = None, **kwargs) -> str:
    """Translated label for *key* (English when the translation is missing)."""
    chosen = resolve_lang(lang, text)
    catalog = MESSAGES.get(chosen) or MESSAGES["en"]
    template = catalog.get(key) or MESSAGES["en"].get(key) or key
    try:
        return template.format(**kwargs)
    except (KeyError, IndexError, ValueError):
        return template


#: Command-menu entries as ``(command, {lang: description})``.
COMMANDS = [
    (
        "start",
        {
            "en": "Welcome & quick start",
            "ar": "\u0627\u0644\u062a\u0631\u062d\u064a\u0628 \u0648\u0627\u0644\u0628\u062f\u0621 \u0627\u0644\u0633\u0631\u064a\u0639",
        },
    ),
    (
        "help",
        {
            "en": "All features and commands",
            "ar": "\u0643\u0644 \u0627\u0644\u0645\u064a\u0632\u0627\u062a \u0648\u0627\u0644\u0623\u0648\u0627\u0645\u0631",
        },
    ),
    ("usersettings", {"en": "Your preferences", "ar": "\u062a\u0641\u0636\u064a\u0644\u0627\u062a\u0643"}),
    (
        "bulkmenu",
        {
            "en": "Process a batch of URLs",
            "ar": "\u0645\u0639\u0627\u0644\u062c\u0629 \u062f\u0641\u0639\u0629 \u0645\u0646 \u0627\u0644\u0631\u0648\u0627\u0628\u0637",
        },
    ),
    (
        "cancel",
        {
            "en": "Stop the current operation",
            "ar": "\u0625\u064a\u0642\u0627\u0641 \u0627\u0644\u0639\u0645\u0644\u064a\u0629 \u0627\u0644\u062d\u0627\u0644\u064a\u0629",
        },
    ),
    (
        "canceljob",
        {
            "en": "Cancel a pending job",
            "ar": "\u0625\u0644\u063a\u0627\u0621 \u0645\u0647\u0645\u0629 \u0645\u0646\u062a\u0638\u0631\u0629",
        },
    ),
    (
        "login",
        {
            "en": "Log in with Telethon",
            "ar": "\u062a\u0633\u062c\u064a\u0644 \u0627\u0644\u062f\u062e\u0648\u0644 \u0628\u0627\u0644\u062a\u064a\u0644\u064a\u062c\u0631\u0627\u0645",
        },
    ),
    (
        "loginpyro",
        {
            "en": "Log in with Pyrogram",
            "ar": "\u062a\u0633\u062c\u064a\u0644 \u0627\u0644\u062f\u062e\u0648\u0644 \u0628\u0628\u0627\u064a\u0631\u0648\u062c\u0631\u0627\u0645",
        },
    ),
    (
        "logout",
        {
            "en": "Log out the Telethon session",
            "ar": "\u062e\u0631\u0648\u062c \u062c\u0644\u0633\u0629 \u0627\u0644\u062a\u064a\u0644\u064a\u062c\u0631\u0627\u0645",
        },
    ),
    (
        "logoutpyro",
        {
            "en": "Log out the Pyrogram session",
            "ar": "\u062e\u0631\u0648\u062c \u062c\u0644\u0633\u0629 \u0628\u0627\u064a\u0631\u0648\u062c\u0631\u0627\u0645",
        },
    ),
    (
        "loginstatus",
        {"en": "Check session health", "ar": "\u062d\u0627\u0644\u0629 \u0627\u0644\u062c\u0644\u0633\u0629"},
    ),
    (
        "session_status",
        {
            "en": "Queue, users and session health",
            "ar": "\u0627\u0644\u0637\u0627\u0628\u0648\u0631 \u0648\u0627\u0644\u0645\u0633\u062a\u062e\u062f\u0645\u0648\u0646 \u0648\u062d\u0627\u0644\u0629 \u0627\u0644\u062c\u0644\u0633\u0629",
        },
    ),
    (
        "admin",
        {
            "en": "Manage allowed users (admin)",
            "ar": "\u0625\u062f\u0627\u0631\u0629 \u0627\u0644\u0645\u0633\u062a\u062e\u062f\u0645\u064a\u0646 \u0627\u0644\u0645\u0633\u0645\u0648\u062d \u0644\u0647\u0645 (\u0627\u0644\u0645\u0634\u0631\u0641)",
        },
    ),
    (
        "cancelall",
        {
            "en": "Cancel all jobs (admin)",
            "ar": "\u0625\u0644\u063a\u0627\u0621 \u0643\u0644 \u0627\u0644\u0645\u0647\u0627\u0645 (\u0627\u0644\u0645\u0634\u0631\u0641)",
        },
    ),
]


def command_menu(lang: str | None = None) -> list:
    """``[(command, description)]`` for the resolved UI language."""
    chosen = resolve_lang(lang)
    return [(cmd, desc.get(chosen) or desc["en"]) for cmd, desc in COMMANDS]
