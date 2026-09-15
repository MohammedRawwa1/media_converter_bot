"""The inline Yes/No confirmation behind every destructive command.

Commands used to demand a second message containing the word ``confirm``. That is
gone: the prompt now carries buttons whose Yes is the only thing that can arm the
action. What matters here is that the callback data round-trips exactly (so the
button runs the right action with the right payload) and that nothing which is not
one of our callbacks can be mistaken for a confirmation.
"""

import pytest

from utils.confirm import NO_DATA, confirm_keyboard, is_cancel, parse_confirm, yes_data


def _buttons(markup):
    return [button for row in markup.inline_keyboard for button in row]


# ── encoding ────────────────────────────────────────────────────────────


def test_keyboard_has_a_yes_and_a_no():
    buttons = _buttons(confirm_keyboard("cancelall"))

    assert buttons[0].callback_data == "cfm:cancelall"
    assert buttons[1].callback_data == NO_DATA


def test_payload_rides_along_on_the_yes_button():
    buttons = _buttons(confirm_keyboard("canceljob", payload="job-123"))

    assert buttons[0].callback_data == "cfm:canceljob:job-123"


def test_yes_data_is_used_consistently():
    assert yes_data("clear_cache") == "cfm:clear_cache"
    assert yes_data("clear_cache", "storage") == "cfm:clear_cache:storage"


def test_callback_data_stays_within_telegrams_limit():
    """Telegram rejects callback data over 64 bytes, so a job id must still fit."""
    job_id = "12345678-1234-1234-1234-123456789abc"
    buttons = _buttons(confirm_keyboard("canceljob", payload=job_id))

    assert len(buttons[0].callback_data.encode()) <= 64


# ── decoding ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ("cfm:cancelall", ("cancelall", None)),
        ("cfm:canceljob:abc", ("canceljob", "abc")),
        ("cfm:clear_cache:storage", ("clear_cache", "storage")),
        ("cfm:delthumb", ("delthumb", None)),
    ],
)
def test_parse_confirm_round_trips(data, expected):
    assert parse_confirm(data) == expected


@pytest.mark.parametrize(
    "data",
    [
        None,
        "",
        "cfn",
        "cancelall",  # a stale command-shaped payload
        "confirm",  # the word that used to arm a wipe
        "cfm:",  # prefix with no action
        "menu_main",
        42,
    ],
)
def test_parse_confirm_rejects_everything_else(data):
    assert parse_confirm(data) is None


def test_a_word_in_an_argument_can_never_confirm():
    """The old footgun: a job id containing "confirm" must not arm anything."""
    assert parse_confirm("confirm-job-123") is None
    assert is_cancel("confirm") is False


def test_is_cancel_matches_only_the_no_button():
    assert is_cancel(NO_DATA) is True
    assert is_cancel("cfm:cancelall") is False
    assert is_cancel("cancel") is False
