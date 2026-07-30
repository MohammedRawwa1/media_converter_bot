"""
ConversationHandler-based Telethon login flow.

Replaces the old ``_process_login_text`` + ``_do_start()`` background-task pattern
with a proper PTB ``ConversationHandler`` that manages auth state across
the phone → code → 2FA steps without asyncio.Future races.

Key improvements over the old design
------------------------------------
1. **No shared asyncio.Future** – each step is a self-contained handler that
   returns the next conversation state.  No background task escapes scope.
2. **No ``client.start()``** – the flow manually calls ``send_code_request``,
   ``sign_in(phone, code)`` and ``sign_in(password=password)``.  This avoids
   Telethon's internal ``_phone_code_hash`` cache and gives us full control
   over code-expiration retries.
3. **Fresh code on expiry** – ``PhoneCodeExpiredError`` triggers a fresh
   ``send_code_request()`` which returns a new ``phone_code_hash``.
4. **Built-in timeout** – ``conversation_timeout=300`` auto-cancels stale flows.
5. **No race conditions** – ``per_user=True, per_chat=True`` ensures messages
   from different users don't interfere.

Usage in main.py::

    from utils.login_handler import create_login_conversation_handler

    conv_handler = create_login_conversation_handler(ADMIN_USER_ID)
    application.add_handler(conv_handler)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

from telegram.ext import (
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)

logger = logging.getLogger(__name__)

# ── Conversation states ─────────────────────────────────────────────
PHONE, CODE, TWO_FA = range(3)

# ── Helper ──────────────────────────────────────────────────────────


def _normalize_code(text: str) -> str:
    """Normalize Unicode digits to ASCII and strip everything that is not a digit."""
    trans = str.maketrans(
        {
            "\u0660": "0",
            "\u0661": "1",
            "\u0662": "2",
            "\u0663": "3",
            "\u0664": "4",
            "\u0665": "5",
            "\u0666": "6",
            "\u0667": "7",
            "\u0668": "8",
            "\u0669": "9",
            "\u06f0": "0",
            "\u06f1": "1",
            "\u06f2": "2",
            "\u06f3": "3",
            "\u06f4": "4",
            "\u06f5": "5",
            "\u06f6": "6",
            "\u06f7": "7",
            "\u06f8": "8",
            "\u06f9": "9",
        }
    )
    return "".join(c for c in text.translate(trans) if c.isdigit())


def _get_api_credentials() -> tuple:
    """Return ``(api_id, api_hash, "")`` or ``("error message",)`` on failure."""
    api_id_str = (
        os.getenv("API_ID")
        or os.getenv("USERBOT_API_ID")
        or os.getenv("api_id")
        or os.getenv("userbot_api_id")
    )
    api_hash = (
        os.getenv("API_HASH")
        or os.getenv("USERBOT_API_HASH")
        or os.getenv("api_hash")
        or os.getenv("userbot_api_hash")
    )
    if not api_id_str or not api_hash:
        return (
            "Missing Telethon credentials. Set API_ID and API_HASH in the environment.",
        )
    try:
        api_id = int(api_id_str)
    except (TypeError, ValueError):
        return ("Configured API_ID is invalid. It must be an integer.",)
    return (api_id, api_hash, "")


# ── Factory ─────────────────────────────────────────────────────────


def create_login_conversation_handler() -> "ConversationHandler":
    """Create a ``ConversationHandler`` for the ``/login`` → phone → code → 2FA flow.

    Admin authorisation is delegated to ``application.bot_data["admin_user_id"]``
    (set in main.py before any handler runs).
    """
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("login", _login_start)],
        states={
            PHONE: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, _receive_phone
                ),
                CommandHandler("cancel", _cancel_login),
            ],
            CODE: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, _receive_code
                ),
                CommandHandler("cancel", _cancel_login),
            ],
            TWO_FA: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, _receive_2fa
                ),
                CommandHandler("cancel", _cancel_login),
            ],
            ConversationHandler.TIMEOUT: [
                MessageHandler(filters.ALL, _timeout_handler),
            ],
        },
        fallbacks=[CommandHandler("cancel", _cancel_login)],
        per_user=True,
        per_chat=True,
        name="userbot_login_flow",
        conversation_timeout=300,  # 5 minutes
    )
    return conv_handler


# ── Permission check ────────────────────────────────────────────────


async def _check_admin(update: "Update", context: "ContextTypes.DEFAULT_TYPE") -> bool:
    """Return ``True`` if the user is authorised to use ``/login``.

    The admin user ID is read from ``application.bot_data["admin_user_id"]``
    which is populated by ``main()`` before any handler is registered.
    """
    admin_user_id = context.application.bot_data.get("admin_user_id")
    if admin_user_id is not None:
        user_id = update.effective_user.id
        if user_id != admin_user_id:
            await update.message.reply_text("Unauthorized: admin only")
            return False
    return True


# ── Client cleanup helper ──────────────────────────────────────────


async def cleanup_login_flow(context: "ContextTypes.DEFAULT_TYPE"):
    """Disconnect the Telethon client and remove all login-related keys from ``user_data``.

    This is the public version of ``_cleanup_client`` intended for external
    callers (e.g. ``logout_command`` in main.py) that need to clean up an
    active login flow without returning a conversation state.

    The caller is responsible for ending the ConversationHandler conversation
    state in ``application.conversation_data`` if needed.
    """
    client = context.user_data.pop("login_client", None)
    if client is not None:
        try:
            await client.disconnect()
        except Exception:
            logger.debug("login: client disconnect failed", exc_info=True)

    # Remove all login-related keys from user_data
    for key in (
        "login_client",
        "login_phone",
        "login_api_id",
        "login_api_hash",
        "phone_code_hash",
        "code_sent_at",
        "_chat_id",
    ):
        context.user_data.pop(key, None)


async def _cleanup_client(context: "ContextTypes.DEFAULT_TYPE"):
    """Disconnect and remove the Telethon client from ``user_data``."""
    await cleanup_login_flow(context)


async def _timeout_handler(
    update: "Update", context: "ContextTypes.DEFAULT_TYPE"
) -> int:
    """Called when the conversation times out. Notify user and clean up."""
    # Save chat_id BEFORE cleaning up, since _cleanup_client removes it
    chat_id = context.user_data.get("_chat_id")
    await _cleanup_client(context)
    context.user_data.clear()
    if chat_id:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⏰ **Login timed out** after 5 minutes of inactivity.\n"
                "Please start again with /login.",
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.debug("login: failed to send timeout notification: %s", exc)
    return ConversationHandler.END  # type: ignore[return-value]


# ══════════════════════════════════════════════════════════════════════
# State handlers
# ══════════════════════════════════════════════════════════════════════


async def _login_start(
    update: "Update", context: "ContextTypes.DEFAULT_TYPE"
) -> int:
    """Entry point.  Check admin access, check credentials, prompt for phone."""
    if not await _check_admin(update, context):
        return ConversationHandler.END  # type: ignore[return-value]

    creds = _get_api_credentials()
    if isinstance(creds, tuple) and len(creds) == 1:
        await update.message.reply_text(str(creds[0]))
        return ConversationHandler.END  # type: ignore[return-value]

    # Store chat_id so the timeout handler can notify the user
    context.user_data["_chat_id"] = update.effective_chat.id

    await update.message.reply_text(
        "📱 Please send the phone number in international format,\n"
        "e.g. ``+1234567890``\n\n"
        "⏳ Your login flow will expire after **5 minutes** of inactivity.\n"
        "Type /cancel to abort the login flow at any time.",
        parse_mode="Markdown",
    )
    return PHONE


async def _receive_phone(
    update: "Update", context: "ContextTypes.DEFAULT_TYPE"
) -> int:
    """PHONE state: receive phone, create Telethon client, request code."""
    phone = update.message.text.strip()
    user_id = update.effective_user.id

    # Resolve credentials
    creds = _get_api_credentials()
    if isinstance(creds, tuple) and len(creds) == 1:
        await update.message.reply_text(str(creds[0]))
        return ConversationHandler.END  # type: ignore[return-value]
    api_id, api_hash, _ = creds

    # ── Check for an already-existing session before doing anything ──
    saved_session_str: str | None = None

    # 1. Persisted JSON file (written by healthchecker)
    try:
        from utils.telethon_session import _load_session_string_from_file_async

        saved_session_str = await _load_session_string_from_file_async(
            client_type="telethon"
        )
    except Exception:
        logger.debug("login: JSON session load failed", exc_info=True)

    # 2. Fall back to MongoDB
    if not saved_session_str:
        try:
            db_model = context.application.bot_data.get("db_model")
            if db_model is not None:
                sess_data = await db_model.load_session(user_id)
                if isinstance(sess_data, dict):
                    saved_session_str = sess_data.get("telethon_session") or sess_data.get(
                        "string_session"
                    )
        except Exception as exc:
            logger.warning("login: MongoDB session load failed: %s", exc)

    # ── Create and connect the Telethon client ──────────────────────
    from telethon import TelegramClient  # noqa: PLC0415
    from telethon.sessions import StringSession  # noqa: PLC0415

    try:
        # If there's a saved session string, validate it before use.
        # Telethon raises "Not a valid string" for corrupted/invalid
        # session strings — fall back to an empty session gracefully.
        if saved_session_str:
            try:
                client = TelegramClient(StringSession(str(saved_session_str)), api_id, api_hash)
            except Exception as sess_exc:
                logger.warning("login: stored session string invalid (%s), starting fresh", sess_exc)
                saved_session_str = None  # fall through to empty session

        if not saved_session_str:
            client = TelegramClient(StringSession(), api_id, api_hash)

        await client.connect()

        if await client.is_user_authorized():
            await update.message.reply_text(
                "✅ Telethon session is already authorised.\n"
                "No need to log in again."
            )
            await client.disconnect()
            return ConversationHandler.END  # type: ignore[return-value]
    except Exception as exc:
        logger.error("login: client create/connect failed: %s", exc)
        await update.message.reply_text(
            "Failed to initialise the Telethon client. Check server logs."
        )
        return ConversationHandler.END  # type: ignore[return-value]

    # ── Store state for subsequent steps ────────────────────────────
    context.user_data["login_client"] = client
    context.user_data["login_phone"] = phone
    context.user_data["login_api_id"] = api_id
    context.user_data["login_api_hash"] = api_hash

    # ── Request the verification code ───────────────────────────────
    try:
        sent = await client.send_code_request(phone)
        context.user_data["phone_code_hash"] = sent.phone_code_hash
        context.user_data["code_sent_at"] = time.time()
    except Exception as exc:
        exc_name = type(exc).__name__
        logger.error("login: send_code_request failed: %s", exc)

        if "FloodWaitError" in exc_name:
            wait = getattr(exc, "seconds", None) or getattr(exc, "timeout", None) or 60
            await update.message.reply_text(
                f"⏳ Telegram is rate-limiting code requests.\n"
                f"Please wait {int(wait)} seconds before trying /login again."
            )
        elif "PhoneNumberInvalidError" in exc_name:
            await update.message.reply_text(
                "❌ The phone number you entered is invalid.\n"
                "Make sure to use international format, e.g. ``+1234567890``.\n"
                "Please run /login again.",
                parse_mode="Markdown",
            )
        elif "PhoneNumberOccupiedError" in exc_name:
            await update.message.reply_text(
                "ℹ️ This phone number already has a logged-in session.\n"
                "Try using /logout first, then /login again."
            )
        else:
            await update.message.reply_text(
                f"Failed to send verification code: {exc}\n"
                "Please run /login again."
            )
        await _cleanup_client(context)
        return ConversationHandler.END  # type: ignore[return-value]

    logger.info("login: code sent to %s (hash=%s...)",
                phone, str(sent.phone_code_hash)[:8])
    await update.message.reply_text(
        "✅ Verification code sent to your Telegram app!\n"
        "Please enter the code you received (digits only).\n"
        "Type /cancel to abort."
    )
    return CODE


async def _receive_code(
    update: "Update", context: "ContextTypes.DEFAULT_TYPE"
) -> int:
    """CODE state: receive the verification code and call ``sign_in``."""
    code = _normalize_code(update.message.text.strip())
    client = context.user_data.get("login_client")
    phone = context.user_data.get("login_phone")
    phone_code_hash = context.user_data.get("phone_code_hash")

    if not client or not phone or not phone_code_hash:
        await update.message.reply_text(
            "Session expired. Please start again with /login."
        )
        await _cleanup_client(context)
        return ConversationHandler.END  # type: ignore[return-value]

    try:
        await client.sign_in(
            phone=phone,
            code=code,
            phone_code_hash=phone_code_hash,
        )

        # ✅ Login succeeded – save session and finish
        return await _finalize_login(update, context)

    except Exception as exc:
        exc_name = type(exc).__name__

        if "SessionPasswordNeededError" in exc_name:
            # 2FA is enabled → transition to password state
            await update.message.reply_text(
                "🔐 Two-step verification is enabled on this account.\n"
                "Please enter your account password:"
            )
            return TWO_FA

        if "PhoneCodeExpiredError" in exc_name:
            # Code expired – request a fresh one with a new phone_code_hash
            logger.warning("login: code expired for %s, requesting new one", phone)
            try:
                sent = await client.send_code_request(phone)
                context.user_data["phone_code_hash"] = sent.phone_code_hash
                context.user_data["code_sent_at"] = time.time()
                logger.info("login: resent code for %s after expiry (new hash=%s...)",
                            phone, str(sent.phone_code_hash)[:8])
            except Exception as send_exc:
                logger.error("login: resend after expiry failed: %s", send_exc)
                await update.message.reply_text(
                    f"Failed to resend code: {send_exc}\n"
                    "Please run /login again."
                )
                await _cleanup_client(context)
                return ConversationHandler.END  # type: ignore[return-value]

            await update.message.reply_text(
                "⏰ The previous code expired. A new one has been sent!\n"
                "Please enter the new code:"
            )
            return CODE  # Stay in CODE state

        if "PhoneCodeInvalidError" in exc_name:
            await update.message.reply_text(
                "❌ Invalid code. Please try again:"
            )
            return CODE  # Stay in CODE state

        if "FloodWaitError" in exc_name:
            wait = getattr(exc, "seconds", None) or getattr(exc, "timeout", None) or 60
            await update.message.reply_text(
                f"⏳ Too many attempts. Please wait {int(wait)} seconds."
            )
            logger.warning("FloodWait during login for %s: %ss", phone, wait)
            return CODE  # Stay in CODE state

        # Unhandled error
        logger.exception("login: sign_in failed: %s", exc)
        await update.message.reply_text(
            f"Login failed: {exc}\nPlease run /login again."
        )
        await _cleanup_client(context)
        return ConversationHandler.END  # type: ignore[return-value]


async def _receive_2fa(
    update: "Update", context: "ContextTypes.DEFAULT_TYPE"
) -> int:
    """TWO_FA state: receive the 2FA password and complete ``sign_in``."""
    password = update.message.text.strip()
    client = context.user_data.get("login_client")

    if not client:
        await update.message.reply_text(
            "Session expired. Please start again with /login."
        )
        return ConversationHandler.END  # type: ignore[return-value]

    try:
        await client.sign_in(password=password)
        return await _finalize_login(update, context)

    except Exception as exc:
        exc_name = type(exc).__name__

        if "PasswordHashInvalidError" in exc_name:
            await update.message.reply_text(
                "❌ Incorrect password. Please try again:"
            )
            return TWO_FA

        if "FloodWaitError" in exc_name:
            wait = getattr(exc, "seconds", None) or getattr(exc, "timeout", None) or 60
            await update.message.reply_text(
                f"⏳ Too many attempts. Please wait {int(wait)} seconds."
            )
            return TWO_FA

        logger.exception("login: 2FA failed: %s", exc)
        await update.message.reply_text(
            f"Password login failed: {exc}\nPlease run /login again."
        )
        await _cleanup_client(context)
        return ConversationHandler.END  # type: ignore[return-value]


# ══════════════════════════════════════════════════════════════════════
# Finalisation & cancellation
# ══════════════════════════════════════════════════════════════════════


async def _finalize_login(
    update: "Update", context: "ContextTypes.DEFAULT_TYPE"
) -> int:
    """Save the session string to MongoDB + JSON file and clean up."""
    client = context.user_data.get("login_client")
    phone = context.user_data.get("login_phone", "unknown")

    if not client or not await client.is_user_authorized():
        await update.message.reply_text(
            "Login completed but the session is not authorised.\n"
            "Please run /login again."
        )
        await _cleanup_client(context)
        return ConversationHandler.END  # type: ignore[return-value]

    # ── Extract the session string ──────────────────────────────
    session_str = client.session.save()
    saved_to: list[str] = []
    user_id = update.effective_user.id

    # ── Persist to MongoDB ──────────────────────────────────────
    try:
        db_model = context.application.bot_data.get("db_model")
        if db_model is not None:
            mongo_data: dict[str, str] = {
                "telethon_session": str(session_str),
                "string_session": str(session_str),
            }
            pyro_env = os.getenv("PYROGRAM_SESSION")
            if pyro_env:
                mongo_data["pyrogram_session"] = pyro_env
            await db_model.save_session(user_id, mongo_data)
            saved_to.append("MongoDB")
    except Exception as exc:
        logger.warning("login: MongoDB save failed: %s", exc)

    # ── Persist to local JSON file ──────────────────────────────
    try:
        from utils.telethon_session import save_session_string_to_file_async  # noqa: PLC0415

        if await save_session_string_to_file_async(str(session_str), client_type="telethon"):
            saved_to.append("JSON file")
    except Exception as exc:
        logger.warning("login: JSON file save failed: %s", exc)

    # Eagerly persist Pyrogram env var to JSON as well
    try:
        pyro_env = os.getenv("PYROGRAM_SESSION")
        if pyro_env:
            from utils.telethon_session import save_session_string_to_file_async as _pyro_save  # noqa: PLC0415

            if await _pyro_save(pyro_env, client_type="pyrogram"):
                saved_to.append("Pyrogram JSON")
    except Exception as exc:
        logger.debug("login: Pyrogram JSON save failed: %s", exc)

    # ── Clean up client ─────────────────────────────────────────
    await _cleanup_client(context)

    summary = ", ".join(saved_to) if saved_to else "memory (all saves failed)"
    await update.message.reply_text(
        f"✅ **Telethon login successful!**\n"
        f"Phone: `{phone}`\n"
        f"DC: {client.session.dc_id}\n"
        f"Saved to: {summary}",
        parse_mode="Markdown",
    )
    logger.info(
        "Login successful for %s (DC=%s, saved_to=%s)",
        phone,
        client.session.dc_id,
        summary,
    )
    return ConversationHandler.END  # type: ignore[return-value]


async def _cancel_login(
    update: "Update", context: "ContextTypes.DEFAULT_TYPE"
) -> int:
    """Cancel the login flow and clean up."""
    await _cleanup_client(context)
    context.user_data.clear()
    await update.message.reply_text("❌ Login cancelled.")
    return ConversationHandler.END  # type: ignore[return-value]
