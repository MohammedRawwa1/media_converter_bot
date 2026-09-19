import contextlib
import json
import logging
import os
from typing import Any

import config
from utils.callbacks import MP3_DEFAULT_BITRATE

logger = logging.getLogger(__name__)

DEFAULTS = {
    # "video" delivers a video as playable media (preview), "file" as a Telegram
    # document. A document is the only way to get the untouched bytes back, so
    # the two are a real choice rather than two labels for the same thing.
    "upload_mode": "video",  # options: video, file
    "prefix": "",
    "suffix": "",
    "words_remove": [],
    "save_thumbnail": False,
    "default_thumbnail": None,  # path or URL
    "use_custom_thumbnail": False,  # when True, use per-user custom thumbnail if set
    # The bitrate every audio conversion falls back to when the file carries no
    # quality of its own. Set from /usersettings - it is a preference, never a
    # conversion trigger.
    "audio_bitrate": MP3_DEFAULT_BITRATE,
}


def _settings_path() -> str:
    path = getattr(config, "STORAGE_PATH", "storage")
    with contextlib.suppress(Exception):
        os.makedirs(path, exist_ok=True)
    return os.path.join(path, "user_settings.json")


def _load_all() -> dict[str, Any]:
    p = _settings_path()
    if not os.path.exists(p):
        return {}
    try:
        with open(p, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def _save_all(data: dict[str, Any]) -> None:
    p = _settings_path()
    try:
        with open(p, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
    except Exception:
        logger.debug("Failed to save user settings")


def get_user_settings(user_id: int) -> dict[str, Any]:
    all_s = _load_all()
    s = all_s.get(str(user_id), {})
    result = DEFAULTS.copy()
    result.update(s or {})
    return result


def set_user_setting(user_id: int, key: str, value) -> None:
    all_s = _load_all()
    uid = str(user_id)
    user_s = all_s.get(uid, {})
    user_s[key] = value
    all_s[uid] = user_s
    _save_all(all_s)


def get_user_setting(user_id: int, key: str, default=None):
    s = get_user_settings(user_id)
    return s.get(key, default)


def toggle_user_setting(user_id: int, key: str) -> bool:
    """Toggle a boolean user setting and return the new value."""
    s = get_user_settings(user_id)
    current = bool(s.get(key))
    new = not current
    set_user_setting(user_id, key, new)
    return new


def clear_user_settings(user_id: int) -> None:
    all_s = _load_all()
    uid = str(user_id)
    if uid in all_s:
        del all_s[uid]
        _save_all(all_s)
