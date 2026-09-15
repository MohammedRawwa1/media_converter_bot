"""Inline Yes/No confirmation for every command that destroys something.

The destructive commands used to stop and ask the user to send the *same command
again* with the word ``confirm`` appended. That put the burden on the user to
retype a message from memory, and it meant a stray ``confirm`` in an argument
could arm a wipe.

Every one of them now sends its warning with two inline buttons instead. The Yes
button carries the exact action to run, so the confirmation cannot be typed, and a
button press can never be confused with the originating command.

Callback data is capped at 64 bytes by Telegram, so the encoding stays small:
``cfm:<action>[:<payload>]`` for Yes and ``cfn`` for No.
"""

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

YES_PREFIX = "cfm:"
NO_DATA = "cfn"
YES_LABEL = "✅ Yes"
NO_LABEL = "❌ No"


def yes_data(action: str, payload: str | None = None) -> str:
    """Callback data for a Yes button that runs ``action``."""
    return f"{YES_PREFIX}{action}" + (f":{payload}" if payload else "")


def confirm_keyboard(
    action: str,
    *,
    payload: str | None = None,
    yes_label: str = YES_LABEL,
    no_label: str = NO_LABEL,
) -> InlineKeyboardMarkup:
    """A single Yes/No row that runs ``action`` when confirmed."""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(yes_label, callback_data=yes_data(action, payload)),
                InlineKeyboardButton(no_label, callback_data=NO_DATA),
            ]
        ]
    )


def parse_confirm(data) -> tuple[str, str | None] | None:
    """Return ``(action, payload)`` for a Yes press, or ``None`` otherwise."""
    if not isinstance(data, str) or not data.startswith(YES_PREFIX):
        return None
    token = data[len(YES_PREFIX) :].strip()
    if not token:
        return None
    action, _, payload = token.partition(":")
    if not action:
        return None
    return action, (payload or None)


def is_cancel(data) -> bool:
    """Whether a callback is the No button."""
    return isinstance(data, str) and data == NO_DATA
