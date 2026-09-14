"""Argument parsing shared by the commands that require an explicit confirmation.

``/cancelall``, ``/clear_cache`` and ``/canceljob`` all follow the reference bot's
rule: the bare command describes what it would destroy and stops; only an
invocation carrying the word ``confirm`` does the work. Sharing the parser keeps
those three behaving identically - before this, each one stripped the word in its
own way, which is how a command can end up accepting ``confirm`` in one place and
not in another.
"""

from __future__ import annotations

CONFIRM_WORD = "confirm"


def split_confirm(args) -> tuple[bool, list[str]]:
    """Split command arguments into ``(confirmed, positional)``.

    Every whole argument equal to ``confirm`` (case-insensitive) is removed and
    sets the confirmation flag. The word may come before or after the positional
    argument, so ``/canceljob <id> confirm`` and ``/canceljob confirm <id>`` both
    work and a user following an older prompt is not left guessing the order.

    Only whole arguments count: a job id or file name that merely contains the
    letters "confirm" is left untouched.
    """
    positional: list[str] = []
    confirmed = False
    for raw in args or []:
        token = str(raw).strip()
        if token.lower() == CONFIRM_WORD:
            confirmed = True
            continue
        positional.append(token)
    return confirmed, positional
