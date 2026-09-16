# utils/telegram_flood_request.py
"""A PTB request layer that fails fast on writes into a flood-limited chat.

Every Bot API call the bot makes funnels through one ``BaseRequest``. Putting the
flood gate there means a handler that answers a user during a window Telegram has
already closed gets a local ``RetryAfter`` straight away - no HTTP round trip, no
429 to parse, and no second doomed call from the error handler trying to
apologise for the first one.

Only methods that carry a ``chat_id`` are filtered, because that is the parameter
Telegram's per-chat budget is enforced against. ``getUpdates``, ``getFile`` and
``answerCallbackQuery`` keep working, which is what Telegram itself does during a
flood: the logs show those answering 200 OK while edits and sends were 429s.
"""

import logging

from telegram.error import RetryAfter

# PTB v20+ uses HTTPXRequest; older releases expose Request under different paths.
try:
    from telegram.request import HTTPXRequest as _PTBRequest
except Exception:  # pragma: no cover - depends on the installed PTB release
    try:
        from telegram.request import Request as _PTBRequest
    except Exception:
        try:
            from telegram.utils.request import Request as _PTBRequest
        except Exception:
            _PTBRequest = None

from utils.rate_limiter import telegram_flood_gate

logger = logging.getLogger(__name__)

# Bot API methods that consume a chat's write budget, matched by prefix so a new
# send*/edit* method is covered without a change here. The ``chat_id`` requirement
# in :func:`write_target_chat_id` is what keeps unrelated methods (setWebhook,
# deleteMyCommands, ...) out of it.
_WRITE_PREFIXES = (
    "send",
    "edit",
    "delete",
    "forward",
    "copy",
    "pin",
    "unpin",
    "stop",
    "set",
    "create",
    "close",
    "reopen",
    "export",
    "revoke",
    "leave",
    "ban",
    "unban",
    "restrict",
    "promote",
    "approve",
    "decline",
)

# Reads that do carry a chat_id and must never be blocked: refusing those would
# turn a busy chat into a chat the bot cannot even look at.
_READ_METHODS = frozenset(
    {
        "getChat",
        "getChatAdministrators",
        "getChatMember",
        "getChatMemberCount",
        "getChatMenuButton",
        "getForumTopic",
        "getUserChatBoosts",
    }
)


def write_target_chat_id(method: str, request_data) -> int | None:
    """The chat a *write* method targets, or None when the call is not gated.

    None is returned for reads, for methods that carry no ``chat_id`` at all
    (``getUpdates``, ``getFile``, ``answerCallbackQuery``, ...) and for anything
    that cannot be parsed: the gate must never turn an unexpected payload into a
    blocked send.
    """
    if not method or method in _READ_METHODS or not method.startswith(_WRITE_PREFIXES):
        return None

    parameters = getattr(request_data, "parameters", None)
    if not isinstance(parameters, dict):
        return None

    chat_id = parameters.get("chat_id")
    if isinstance(chat_id, bool):
        return None
    if isinstance(chat_id, int):
        return chat_id
    if isinstance(chat_id, str):
        try:
            return int(chat_id)
        except ValueError:
            # A @username target: the gate is keyed on numeric ids.
            return None
    return None


if _PTBRequest is not None:

    class FloodGatedRequest(_PTBRequest):
        """PTB request that raises ``RetryAfter`` locally while a chat is gated.

        The exception is the same one Telegram would have answered with, so every
        existing ``except RetryAfter`` keeps working - it just arrives without the
        API call, and without a multi-hour window being slept off.
        """

        async def do_request(self, url, method, request_data=None, **kwargs):
            chat_id = write_target_chat_id(method, request_data)
            if chat_id is not None:
                scope = telegram_flood_gate.scope_for_chat(chat_id)
                if await telegram_flood_gate.should_drop_inline(scope):
                    remaining = int(await telegram_flood_gate.remaining(scope)) or 1
                    logger.debug(
                        "flood gate: skipped %s for chat %s (%.0fs left in the window)",
                        method,
                        chat_id,
                        remaining,
                    )
                    raise RetryAfter(remaining)
            # Keyword forwarding: the parameter names match across PTB releases
            # even where their order does not.
            return await super().do_request(url=url, method=method, request_data=request_data, **kwargs)


    def flood_gated_request(**kwargs):
        """A gated request, or None when PTB's request class cannot be imported.

        ``Bot(request=None)`` falls back to PTB's own default, so callers can pass
        this straight through.
        """
        try:
            return FloodGatedRequest(**kwargs)
        except Exception:
            logger.debug("flood gate: could not build a gated request")
            return None

else:  # pragma: no cover - only reachable without a usable PTB

    class FloodGatedRequest:  # type: ignore[no-redef]
        """Placeholder so callers can reference the name on any PTB release."""

    def flood_gated_request(**kwargs):
        return None
