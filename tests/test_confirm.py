"""The parser behind every command that demands an explicit confirmation.

``/cancelall``, ``/clear_cache`` and ``/canceljob`` all refuse to act until the
word ``confirm`` shows up, and they now share one parser so they cannot drift
apart. Only whole arguments count: a job id containing the letters must not
confirm anything on its own.
"""

import pytest

from utils.confirm import split_confirm


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        # Confirmed, in either order.
        (["abc", "confirm"], (True, ["abc"])),
        (["confirm", "abc"], (True, ["abc"])),
        (["Confirm"], (True, [])),
        (["a", "confirm", "b"], (True, ["a", "b"])),
        # Not confirmed.
        (["abc"], (False, ["abc"])),
        ([], (False, [])),
        (None, (False, [])),
        (["confirmed"], (False, ["confirmed"])),
        (["confirmable", "confirm"], (True, ["confirmable"])),
    ],
)
def test_split_confirm(args, expected):
    assert split_confirm(args) == expected


def test_whitespace_around_the_word_is_tolerated():
    """Telegram can hand back padded tokens; a trailing space must still confirm."""
    assert split_confirm(["  confirm  "]) == (True, [])
    assert split_confirm([" abc "]) == (False, ["abc"])


def test_a_job_id_containing_the_word_does_not_confirm():
    """The dangerous case: /canceljob <id-containing-confirm> must still ask."""
    confirmed, positional = split_confirm(["confirm-job-123"])

    assert confirmed is False
    assert positional == ["confirm-job-123"]


def test_only_one_confirmation_is_reported_for_repeated_words():
    confirmed, positional = split_confirm(["confirm", "confirm", "abc"])

    assert confirmed is True
    assert positional == ["abc"]
