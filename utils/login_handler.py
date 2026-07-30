"""
Background-task-based Telethon login flow.

Instead of a ConversationHandler (which has a polling gap between
``send_code_request`` and ``sign_in`` that causes codes to expire),
this module uses a **background Telethon task** + **asyncio.Future**
pattern:

1. ``/login`` → stores ``asyncio.Future`` s in ``bot_data["login_futures"]``,
   starts a background task that owns the Telethon ``TelegramClient``.
2. The background task calls ``send_code_request``, then **awaits**
   an ``asyncio.Future`` for the code — the future is resolved by
   ``_handle_login_text`` when the user's message arrives.
3. ``sign_in`` is called **in the same coroutine** immediately after
   the future resolves → **no polling gap**.
4. If 2FA is needed the task awaits a second future for the password.

Usage in main.py::

    from utils.login_handler import (
        cleanup_login_flow,
        login_command,
        handle_login_text,
        register_login_handlers,
    )

    register_login_handlers(application)
"""

from __future__ import annotations

import asyncio
import logging
import os

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes, MessageHandler, filters

logger = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────

def _normalize_code(text: str) -> str:
    """Normalize Unicode digits to ASCII and strip non-digit chars."""
    trans = str.maketrans(
        {
            "\u0660": "0", "\u0661": "1", "\u0662": "2", "\u0663": "3",
            "\u0664": "4", "\u0665": "5", "\u0666": "6", "\u0667": "7",
            "\u0668": "8", "\u0669": "9",
            "\u06f0": "0", "\u06f1": "1", "\u06f2": "2", "\u06f3": "3",
            "\u06f4": "4", "\u06f5": "5", "\u06f6": "6", "\u06f7": "7",
            "\u06f8": "8", "\u06f9": "9",
        }
    )
    return "".join(c for c in text.translate(trans) if c.isdigit())


def _pyrogram_warning_text() -> str:
    """Return a warning about Pyrogram session if one is configured."""
    if os.getenv("PYROGRAM_SESSION"):
        return (
            "\n\n⚠️ A Pyrogram session is configured. If it uses the same"
            " phone number, codes may be consumed by that session."
        )
    return ""


def _get_api_credentials() -> tuple:
    """Return ``(api_id, api_hash, "")`` or ``("error message",)``."""
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
        return ("Missing Telethon credentials. Set API_ID and API_HASH.",)
    try:
        api_id = int(api_id_str)
    except (TypeError, ValueError):
        return ("Configured API_ID is invalid. It must be an integer.",)
    return (api_id, api_hash, "")


# ── Public helpers ──────────────────────────────────────────────────────

async def cleanup_login_flow(context: ContextTypes.DEFAULT_TYPE, user_id: int | None = None):
    """Cancel any active login flow for *user_id* (or the calling user)."""
    if user_id is None:
        return
    futures_map: dict = context.application.bot_data.get("login_futures", {})
    entry = futures_map.pop(user_id, None)
    if entry is None:
        return
    # Signal cancellation to the background task
    entry["cancel"].set()
    if entry.get("task") is not None:
        entry["task"].cancel()
    # Disconnect the Telethon client
    client = entry.get("client")
    if client is not None:
        try:
            await client.disconnect()
        except Exception:
            pass


def register_login_handlers(application):
    """Register the ``/login`` command and the login-text message handler.

    Call this from ``setup_handlers()`` to wire up the login flow.
    """
    application.add_handler(CommandHandler("login", _login_command))
    # This handler catches ALL text messages that might be login input.
    # It runs *before* other text handlers and only consumes the message
    # if there is a pending login Future for that user.
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_login_text),
        group=0,
    )
    # NOTE: /cancel is NOT registered here — the global cancel_command in main.py
    # handles both login-flow cancellation and normal cancel, because it
    # checks bot_data["login_futures"] for an active background task.


# ── Permission check ────────────────────────────────────────────────────

async def _check_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    admin_id = context.application.bot_data.get("admin_user_id")
    if admin_id is not None:
        uid = update.effective_user.id
        if uid != admin_id:
            await update.message.reply_text("Unauthorized: admin only")
            return False
    return True


# ── Login state helpers ─────────────────────────────────────────────────

def _get_futures(context: ContextTypes.DEFAULT_TYPE, user_id: int, create: bool = False) -> dict | None:
    """Get the login futures dict for *user_id*, optionally creating it."""
    if "login_futures" not in context.application.bot_data:
        if not create:
            return None
        context.application.bot_data["login_futures"] = {}
    fm = context.application.bot_data["login_futures"]
    if user_id not in fm:
        if not create:
            return None
        fm[user_id] = {
            "phone": None,          # asyncio.Future or None
            "code": None,           # asyncio.Future or None
            "password": None,       # asyncio.Future or None
            "cancel": asyncio.Event(),
            "task": None,           # asyncio.Task or None
            "client": None,         # TelegramClient or None
            "chat_id": None,        # set by caller after creation
            "phone_number": None,   # str or None
            "phone_code_hash": None,
            "api_id": None,
            "api_hash": None,
        }
    return fm[user_id]


# ══════════════════════════════════════════════════════════════════════════
# /login command
# ══════════════════════════════════════════════════════════════════════════

async def _login_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """``/login [phone]`` — start the Telethon login flow."""
    if not await _check_admin(update, context):
        return

    creds = _get_api_credentials()
    if isinstance(creds, tuple) and len(creds) == 1:
        await update.message.reply_text(str(creds[0]))
        return

    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    api_id, api_hash, _ = creds

    # Check for existing active flow
    existing = _get_futures(context, uid)
    if existing and existing.get("task") is not None and not existing["task"].done():
        await update.message.reply_text(
            "⏳ You already have an active login flow. Use /cancel to abort it first."
        )
        return

    # Check for already-authorised session
    try:
        from telethon import TelegramClient  # noqa: PLC0415
        from telethon.sessions import StringSession  # noqa: PLC0415

        saved_session = await _load_any_session(context, uid)
        if saved_session:
            test_client = TelegramClient(StringSession(saved_session), api_id, api_hash)
            try:
                await test_client.connect()
                if await test_client.is_user_authorized():
                    me = await test_client.get_me()
                    await update.message.reply_text(
                        f"✅ Telethon session is already authorised as {me.first_name or me.phone}.\n"
                        "No need to log in again."
                    )
                    await test_client.disconnect()
                    return
                await test_client.disconnect()
            except Exception:
                await test_client.disconnect()
    except Exception:
        pass

    # Parse optional phone argument
    args = context.args if hasattr(context, "args") else []
    phone = " ".join(args).strip() if args else ""

    if phone:
        # Phone provided as argument — start immediately
        entry = _get_futures(context, uid, create=True)
        entry["api_id"] = api_id
        entry["api_hash"] = api_hash
        entry["phone_number"] = phone
        entry["chat_id"] = chat_id

        # Proactive warning
        warn = _pyrogram_warning_text()

        msg = await update.message.reply_text(
            f"📱 Phone: `{phone}`\n"
            "⏳ Connecting to Telegram and requesting verification code..."
            + warn,
            parse_mode="Markdown",
        )

        # Start background task
        entry["task"] = asyncio.create_task(
            _run_login_task(uid, phone, api_id, api_hash, entry, context, msg)
        )
    else:
        # No phone — ask for it, set up phone Future
        entry = _get_futures(context, uid, create=True)
        entry["api_id"] = api_id
        entry["api_hash"] = api_hash
        entry["chat_id"] = chat_id
        entry["phone"] = asyncio.get_running_loop().create_future()

        warn = _pyrogram_warning_text()

        await update.message.reply_text(
            "📱 Please send the phone number in international format,\n"
            "e.g. ``+1234567890``\n\n"
            "⏳ Your login flow will expire after **5 minutes** of inactivity.\n"
            "Type /cancel to abort."
            + warn,
            parse_mode="Markdown",
        )


async def _load_any_session(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> str | None:
    """Try to load a saved Telethon session from JSON or MongoDB."""
    try:
        from utils.telethon_session import _load_session_string_from_file_async
        saved = await _load_session_string_from_file_async(client_type="telethon")
        if saved:
            return saved
    except Exception:
        pass
    try:
        db_model = context.application.bot_data.get("db_model")
        if db_model is not None:
            sess = await db_model.load_session(user_id)
            if isinstance(sess, dict):
                return sess.get("telethon_session") or sess.get("string_session")
    except Exception:
        pass
    return None


# ══════════════════════════════════════════════════════════════════════════
# Text message handler (resolves Futures)
# ══════════════════════════════════════════════════════════════════════════

async def _handle_login_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Intercept text messages that may be login input (phone / code / password).

    This handler is registered with ``group=0`` so it fires before other
    text handlers.  It only *consumes* the message if a matching Future
    is pending; otherwise it returns without doing anything, allowing
    downstream handlers to process the message normally.
    """
    uid = update.effective_user.id
    entry = _get_futures(context, uid)
    if entry is None:
        return  # no active login flow; let other handlers process this message

    text = update.message.text.strip()

    # ── Phone ────────────────────────────────────────────────────
    phone_fut = entry.get("phone")
    if phone_fut is not None and not phone_fut.done():
        # Validate phone
        if not text.startswith("+"):
            await update.message.reply_text(
                "❌ Phone number must start with ``+`` and country code.\n"
                "e.g. ``+1234567890``\n\nPlease try again or type /cancel.",
                parse_mode="Markdown",
            )
            return  # consumed — don't pass to other handlers
        entry["phone_number"] = text
        phone_fut.set_result(text)
        # Start the background task NOW — it was waiting for the phone
        warn = _pyrogram_warning_text()
        msg = await update.message.reply_text(
            f"📱 Phone: `{text}`\n"
            "⏳ Connecting to Telegram and requesting verification code..."
            + warn,
            parse_mode="Markdown",
        )
        entry["task"] = asyncio.create_task(
            _run_login_task(
                uid, text, entry["api_id"], entry["api_hash"],
                entry, context, msg,
            )
        )
        return  # consumed

    # ── Code ─────────────────────────────────────────────────────
    code_fut = entry.get("code")
    if code_fut is not None and not code_fut.done():
        code = _normalize_code(text)
        if not code:
            await update.message.reply_text(
                "❌ No digits found. Please enter the code you received (digits only):"
            )
            return
        code_fut.set_result(code)
        return  # consumed

    # ── Password ─────────────────────────────────────────────────
    pwd_fut = entry.get("password")
    if pwd_fut is not None and not pwd_fut.done():
        if not text:
            await update.message.reply_text("❌ Password cannot be empty. Please try again:")
            return
        pwd_fut.set_result(text)
        return  # consumed

    # No matching future — let other handlers process this message
    return


# ══════════════════════════════════════════════════════════════════════════
# Background login task
# ══════════════════════════════════════════════════════════════════════════

TIMEOUT_SECONDS = 300  # 5 minutes per step


async def _run_login_task(
    user_id: int,
    phone: str,
    api_id: int,
    api_hash: str,
    entry: dict,
    context: ContextTypes.DEFAULT_TYPE,
    status_msg=None,
):
    """Background coroutine that owns the Telethon client and runs the login.

    ``send_code_request`` and ``sign_in`` run in this single coroutine,
    so there is **no polling gap** between them.  The user's code arrives
    via an ``asyncio.Future`` set by ``_handle_login_text``.
    """
    bot = context.bot
    chat_id = entry["chat_id"]

    try:
        from telethon import TelegramClient  # noqa: PLC0415
        from telethon.sessions import StringSession  # noqa: PLC0415

        client = TelegramClient(StringSession(), api_id, api_hash)
        entry["client"] = client
        await client.connect()

        # ── Send code request ────────────────────────────────────
        sent = await client.send_code_request(phone)
        entry["phone_code_hash"] = sent.phone_code_hash

        # Check 2FA upfront (best-effort)
        has_2fa = False
        pwd_hint = ""
        try:
            from telethon import functions  # noqa: PLC0415
            pwd_info = await client(functions.account.GetPasswordRequest())
            has_2fa = bool(getattr(pwd_info, "has_password", False))
            pwd_hint = str(getattr(pwd_info, "hint", "") or "")
        except Exception:
            pass  # best-effort

        logger.info("login: code sent to %s (hash=%s..., 2fa=%s)", phone, str(sent.phone_code_hash)[:8], has_2fa)

        # Notify user
        msg_text = "✅ Verification code sent to your Telegram app!\n\nPlease enter the code (digits only)."
        if has_2fa:
            msg_text += (
                "\n\n🔐 **Two-step verification (2FA)** is enabled on this account."
                "\nAfter entering the code, you'll need to enter your password."
            )
            if pwd_hint:
                msg_text += f"\nPassword hint: `{pwd_hint}`"
        if status_msg:
            await status_msg.edit_text(msg_text, parse_mode="Markdown")
        else:
            status_msg = await bot.send_message(chat_id, msg_text, parse_mode="Markdown")

        # ── Wait for code ────────────────────────────────────────
        entry["code"] = asyncio.get_running_loop().create_future()

        try:
            code = await asyncio.wait_for(entry["code"], timeout=TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            await _login_fail(bot, chat_id, status_msg, "⏰ Login timed out (no code received within 5 minutes).")
            return

        if entry["cancel"].is_set():
            return  # cancelled

        # ── Sign in with code (NO polling gap!) ──────────────────
        retry_count = 0
        while retry_count < 3:
            try:
                await client.sign_in(phone=phone, code=code)
                break  # success
            except Exception as exc:
                exc_name = type(exc).__name__

                if "SessionPasswordNeededError" in exc_name:
                    has_2fa = True
                    break

                if "PhoneCodeExpiredError" in exc_name:
                    retry_count += 1
                    if retry_count >= 3:
                        await _login_fail(
                            bot, chat_id, status_msg,
                            "❌ The verification codes keep expiring before they can be used.\n"
                            "This usually happens when two bots share API_ID/API_HASH with 2FA enabled.\n"
                            "Try using /logout first, then /login again."
                        )
                        return
                    # Resend
                    sent = await client.send_code_request(phone)
                    entry["phone_code_hash"] = sent.phone_code_hash
                    logger.info("login: resent code after expiry (attempt %d/3)", retry_count)
                    # Set up new code future
                    entry["code"] = asyncio.get_running_loop().create_future()
                    try:
                        if status_msg:
                            await status_msg.edit_text(
                                f"⏰ The previous code expired. A new one has been sent!\n\n"
                                f"*Resend #{retry_count}*\n"
                                "Please enter the new code:",
                                parse_mode="Markdown",
                            )
                    except Exception:
                        pass
                    try:
                        code = await asyncio.wait_for(entry["code"], timeout=TIMEOUT_SECONDS)
                    except asyncio.TimeoutError:
                        await _login_fail(bot, chat_id, status_msg, "⏰ Login timed out.")
                        return
                    continue  # retry sign_in with new code

                if "PhoneCodeInvalidError" in exc_name:
                    # Set up new code future
                    entry["code"] = asyncio.get_running_loop().create_future()
                    try:
                        if status_msg:
                            await status_msg.edit_text("❌ Invalid code. Please try again:", parse_mode="Markdown")
                    except Exception:
                        await bot.send_message(chat_id, "❌ Invalid code. Please try again:")
                    try:
                        code = await asyncio.wait_for(entry["code"], timeout=TIMEOUT_SECONDS)
                    except asyncio.TimeoutError:
                        await _login_fail(bot, chat_id, status_msg, "⏰ Login timed out.")
                        return
                    continue

                if "FloodWaitError" in exc_name:
                    wait = getattr(exc, "seconds", None) or getattr(exc, "timeout", None) or 60
                    await _login_fail(
                        bot, chat_id, status_msg,
                        f"⏳ Too many attempts. Please wait {int(wait)} seconds before trying /login again."
                    )
                    return

                # Unhandled error
                await _login_fail(bot, chat_id, status_msg, f"❌ Login failed: {exc}")
                logger.exception("login: sign_in failed: %s", exc)
                return

        else:
            # Loop exited without break (all retries exhausted) — handled inside loop
            return

        if has_2fa:
            msg_text = "🔐 Two-step verification is enabled. Please enter your account password:"
            if pwd_hint:
                msg_text += f"\nPassword hint: `{pwd_hint}`"
            try:
                await status_msg.edit_text(msg_text, parse_mode="Markdown")
            except Exception:
                status_msg = await bot.send_message(chat_id, msg_text, parse_mode="Markdown")

            entry["password"] = asyncio.get_running_loop().create_future()
            try:
                password = await asyncio.wait_for(entry["password"], timeout=TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                await _login_fail(bot, chat_id, status_msg, "⏰ Login timed out (no password received within 5 minutes).")
                return

            if entry["cancel"].is_set():
                return

            try:
                await client.sign_in(password=password)
            except Exception as exc:
                if "PasswordHashInvalidError" in type(exc).__name__:
                    # Allow retry
                    entry["password"] = asyncio.get_running_loop().create_future()
                    try:
                        await status_msg.edit_text("❌ Incorrect password. Please try again:", parse_mode="Markdown")
                    except Exception:
                        await bot.send_message(chat_id, "❌ Incorrect password. Please try again:")
                    try:
                        password = await asyncio.wait_for(entry["password"], timeout=TIMEOUT_SECONDS)
                    except asyncio.TimeoutError:
                        await _login_fail(bot, chat_id, status_msg, "⏰ Login timed out.")
                        return
                    await client.sign_in(password=password)
                else:
                    raise

        # ✅ Login succeeded — save session
        session_str = client.session.save()
        saved_to = []
        try:
            db_model = context.application.bot_data.get("db_model")
            if db_model is not None:
                await db_model.save_session(user_id, {
                    "telethon_session": session_str,
                    "string_session": session_str,
                })
                saved_to.append("MongoDB")
        except Exception as exc:
            logger.warning("login: MongoDB save failed: %s", exc)
        try:
            from utils.telethon_session import save_session_string_to_file_async  # noqa: PLC0415
            if await save_session_string_to_file_async(session_str, client_type="telethon"):
                saved_to.append("JSON file")
        except Exception as exc:
            logger.warning("login: JSON save failed: %s", exc)

        me = await client.get_me()
        uname = getattr(me, "first_name", "") or getattr(me, "username", "user")
        summary = ", ".join(saved_to) if saved_to else "memory"
        try:
            await status_msg.edit_text(
                f"✅ **Telethon login successful!**\n"
                f"User: {uname}\n"
                f"Phone: `{phone}`\n"
                f"DC: {client.session.dc_id}\n"
                f"Saved to: {summary}",
                parse_mode="Markdown",
            )
        except Exception:
            await bot.send_message(
                chat_id,
                f"✅ **Telethon login successful!**\n"
                f"User: {uname}\n"
                f"Phone: `{phone}`\n"
                f"DC: {client.session.dc_id}\n"
                f"Saved to: {summary}",
                parse_mode="Markdown",
            )
        logger.info("Login successful for %s (DC=%s)", phone, client.session.dc_id)

    except asyncio.CancelledError:
        logger.info("login: task cancelled for %s", phone)
    except Exception as exc:
        logger.exception("login: unexpected error: %s", exc)
        try:
            await bot.send_message(chat_id, f"❌ Login failed unexpectedly: {exc}")
        except Exception:
            pass
    finally:
        # Clean up
        client = entry.get("client")
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass
        # Remove from futures map
        futures_map = context.application.bot_data.get("login_futures", {})
        if user_id in futures_map:
            del futures_map[user_id]


async def _login_fail(bot, chat_id, status_msg, text: str):
    """Send failure message and clean up status_msg if possible."""
    try:
        if status_msg:
            await status_msg.edit_text(text, parse_mode="Markdown")
        else:
            await bot.send_message(chat_id, text, parse_mode="Markdown")
    except Exception:
        try:
            await bot.send_message(chat_id, text, parse_mode="Markdown")
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════
# Cancel handler
# ══════════════════════════════════════════════════════════════════════════

async def cancel_login_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """``/cancel`` inside a login flow — abort and clean up the background task."""
    uid = update.effective_user.id
    await cleanup_login_flow(context, uid)
    await update.message.reply_text("❌ Login cancelled.")
