    def _clear_login_flow(user_id, context):
        try:
            LOGIN_PENDING_USERS.discard(user_id)
        except Exception:
            pass
        if context is not None and getattr(context, "user_data", None) is not None:
            # Disconnect any active Telethon client before clearing
            try:
                client = context.user_data.get("login_client")
                if client is not None:
                    try:
                        import asyncio
                        try:
                            loop = asyncio.get_event_loop()
                            if loop.is_running():
                                asyncio.create_task(client.disconnect())
                        except RuntimeError:
                            pass
                    except Exception:
                        pass
            except Exception:
                pass
            for key in (
                "awaiting_login_phone",
                "awaiting_login_code",
                "awaiting_login_password",
                "login_phone",
                "login_client",
                "login_session_path",
                "login_code_sent_at",
                "login_code_sent_repr",
                "login_code_hash",
                "login_code_type",
                "login_flood_wait_until",
                "login_resend_count",
            ):
                context.user_data.pop(key, None)

    async def _process_login_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = getattr(update.effective_user, "id", None)
        if not user_id:
            return

        # Respect any outstanding FloodWait imposed earlier: block retries
        try:
            flood_until = context.user_data.get("login_flood_wait_until")
            if flood_until:
                now = time.time()
                if now < flood_until:
                    remaining = int(flood_until - now)
                    await update.message.reply_text(
                        f"Too many login attempts. Please wait {remaining} seconds before retrying."
                    )
                    return
                else:
                    # expired — clear stored flood info
                    context.user_data.pop("login_flood_wait_until", None)
        except Exception:
            pass

        if not (
            context.user_data.get("awaiting_login_phone")
            or context.user_data.get("awaiting_login_code")
            or context.user_data.get("awaiting_login_password")
        ):
            _clear_login_flow(user_id, context)
            return

        if context.user_data.get("awaiting_login_phone"):
            context.user_data["awaiting_login_phone"] = False
            phone = update.message.text.strip()
            await update.message.reply_text("Got phone number. Please wait while I generate the Telethon session...")
            try:
                from telethon import TelegramClient
            except Exception:
                await update.message.reply_text(
                    "Telethon is not installed on the server. Install telethon to use /login."
                )
                _clear_login_flow(user_id, context)
                return

            api_id = os.getenv("API_ID") or os.getenv("USERBOT_API_ID")
            api_hash = os.getenv("API_HASH") or os.getenv("USERBOT_API_HASH")
            try:
                api_id = int(api_id)
            except Exception:
                await update.message.reply_text("Configured API_ID is invalid. It must be an integer.")
                _clear_login_flow(user_id, context)
                return

            session_name = (
                os.getenv("API_SESSION_NAME")
                or os.getenv("SESSION_NAME")
                or os.getenv("USERBOT_SESSION_NAME")
                or os.getenv("TELETHON_SESSION_NAME")
                or "userbot_session"
            )
            session_dir = os.getenv("TELETHON_SESSION_DIR") or os.getenv("TEMP_PATH") or os.getcwd()
            os.makedirs(session_dir, exist_ok=True)
            session_path = os.path.join(session_dir, session_name)

            client = TelegramClient(session_path, api_id, api_hash)
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    # Capture the returned object from send_code_request so we can
                    # provide the phone_code_hash to sign_in when required by
                    # certain Telethon/server variants.
                    try:
                        # Request a login code. Avoid using deprecated `force_sms`.
                        try:
                            sent = await client.send_code_request(phone)
                        except TypeError:
                            # Older Telethon signatures may differ; try fallback.
                            sent = await client.send_code_request(phone)
                    except Exception as e:
                        # Detect FloodWaitError specifically to inform the user
                        try:
                            from telethon.errors import FloodWaitError

                            if isinstance(e, FloodWaitError):
                                wait = getattr(e, "seconds", None) or getattr(e, "timeout", None) or 60
                                until = time.time() + int(wait)
                                context.user_data["login_flood_wait_until"] = until
                                await update.message.reply_text(
                                    f"Too many requests; please wait {int(wait)} seconds before retrying."
                                )
                                logger.warning("FloodWait during send_code_request for %s: wait=%s", phone, wait)
                                try:
                                    await client.disconnect()
                                except Exception:
                                    pass
                                _clear_login_flow(user_id, context)
                                return
                        except Exception:
                            pass
                        # Final fallback (rare) — attempt raw call once more.
                        sent = await client.send_code_request(phone)

                    # Debug: record when the code was requested and returned hash
                    try:
                        sent_type = getattr(sent, "type", None)
                        # Determine a simple human-friendly type name
                        try:
                            sent_type_name = sent_type.__class__.__name__ if sent_type is not None else None
                        except Exception:
                            sent_type_name = repr(sent_type)

                        logger.info(
                            "Requested login code for %s; sent_obj=%s sent_type=%s",
                            phone,
                            repr(sent),
                            sent_type_name,
                        )
                        context.user_data["login_code_type"] = sent_type_name
                        sent_time = time.time()
                        context.user_data["login_code_sent_at"] = sent_time
                        context.user_data["login_code_sent_repr"] = repr(sent)
                    except Exception:
                        pass

                    # Store the phone_code_hash if available for later sign_in.
                    try:
                        code_hash = getattr(sent, "phone_code_hash", None)
                    except Exception:
                        code_hash = None

                    if code_hash:
                        context.user_data["login_code_hash"] = code_hash

                    # Inform the user where the code was delivered (app, SMS, flash call)
                    try:
                        st = context.user_data.get("login_code_type")
                        if st and "App" in st:
                            user_msg = "A login code was sent to your Telegram app. Open your Telegram app or desktop client and reply with the code."
                        elif st and ("Sms" in st or "SMS" in st):
                            user_msg = "A login code was sent via SMS. Reply with the code you receive by SMS."
                        elif st and "Flash" in st:
                            user_msg = "A login code was sent via flash call. Check the incoming call for the code and reply with it."
                        else:
                            user_msg = "A login code has been sent. Please reply with the code you receive."
                    except Exception:
                        user_msg = "A login code has been sent. Please reply with the code you receive."

                    await update.message.reply_text(user_msg)
                    context.user_data["awaiting_login_code"] = True
                    context.user_data["login_phone"] = phone
                    context.user_data["login_client"] = client
                    context.user_data["login_session_path"] = session_path
                    return
                else:
                    await update.message.reply_text(
                        f"Telethon session is already authorized and saved to {session_path}. You can now use userbot fallback."
                    )
                    await client.disconnect()
                    _clear_login_flow(user_id, context)
                    return
            except Exception as exc:
                logger.exception("/login phone step failed: %s", exc)
                await update.message.reply_text(
                    "Failed to start Telethon login. Check API_ID/API_HASH and the phone number."
                )
                try:
                    await client.disconnect()
                except Exception:
                    pass
                _clear_login_flow(user_id, context)
                return

        if context.user_data.get("awaiting_login_code"):
            code = update.message.text.strip()
            client = context.user_data.get("login_client")
            phone = context.user_data.get("login_phone")
            if client is None or not phone:
                await update.message.reply_text(
                    "Session state lost. Please run /login again to start a fresh login."
                )
                _clear_login_flow(user_id, context)
                return

            try:
                # Normalize code input (handle Unicode digits and stray chars)
                trans_digits = str.maketrans(
                    {
                        "٠": "0",
                        "١": "1",
                        "٢": "2",
                        "٣": "3",
                        "٤": "4",
                        "٥": "5",
                        "٦": "6",
                        "٧": "7",
                        "٨": "8",
                        "٩": "9",
                        "۰": "0",
                        "۱": "1",
                        "۲": "2",
                        "۳": "3",
                        "۴": "4",
                        "۵": "5",
                        "۶": "6",
                        "۷": "7",
                        "۸": "8",
                        "۹": "9",
                    }
                )
                norm_code = (code or "").translate(trans_digits)
                # Remove any non-digit characters
                norm_code = "".join([c for c in norm_code if c.isdigit()])

                # Debug: record code usage context before attempting sign-in
                try:
                    entered_at = time.time()
                    sent_at = context.user_data.get("login_code_sent_at")
                    resend_count = context.user_data.get("login_resend_count", 0)
                    code_hash_preview = str(context.user_data.get("login_code_hash"))
                    client_session = getattr(getattr(client, 'session', None), 'filename', None) or repr(getattr(client, 'session', None))
                    masked_code = (norm_code[-2:].rjust(2, "*") if norm_code else "")
                    logger.info(
                        "Attempting sign_in: user=%s entered_at=%s sent_at=%s delta=%.3fs resend_count=%s code_hash=%s session=%s code_tail=%s",
                        user_id,
                        entered_at,
                        sent_at,
                        (entered_at - sent_at) if sent_at else -1,
                        resend_count,
                        code_hash_preview,
                        client_session,
                        masked_code,
                    )
                    # Admin-only: log the full normalized code for debugging
                    try:
                        if user_id == ADMIN_USER_ID:
                            logger.info("Admin sign-in code (normalized) for user=%s: %s", user_id, norm_code)
                    except Exception:
                        pass
                except Exception:
                    pass

                # Do NOT add pre-wait delay; attempt sign_in immediately to minimize
                # time lost. Telegram's code validity window appears to be < 4 seconds
                # from send time. Every millisecond counts.

                # Use stored phone_code_hash when available to match the
                # send_code_request response. If the hash is stale (e.g. after
                # DC migration), try without it before giving up.
                code_hash = context.user_data.get("login_code_hash")

                # Retry sign_in up to 3 times with brief pauses, as codes can expire
                # in-flight due to network latency and DC migration delays.
                sign_in_attempts = 0
                last_error = None
                while sign_in_attempts < 3:
                    sign_in_attempts += 1
                    try:
                        if code_hash:
                            try:
                                await client.sign_in(phone=phone, code=norm_code, phone_code_hash=code_hash)
                            except ValueError as ve:
                                # Hash may be stale after DC migration and Telethon raises ValueError.
                                # Try without hash as fallback.
                                if "phone_code_hash" in str(ve):
                                    try:
                                        await client.sign_in(code=norm_code)
                                    except TypeError:
                                        await client.sign_in(phone=phone, code=norm_code)
                                else:
                                    raise
                        else:
                            # No stored hash (e.g. resumed session, older flow).
                            try:
                                await client.sign_in(code=norm_code)
                            except TypeError:
                                await client.sign_in(phone=phone, code=norm_code)
                        # Success — break the retry loop
                        break
                    except Exception as e:
                        last_error = e
                        # If this was not the last attempt, wait briefly and retry
                        if sign_in_attempts < 3:
                            logger.debug("Sign-in attempt %d failed; retrying in 0.1s: %s", sign_in_attempts, e)
                            await asyncio.sleep(0.1)
                        else:
                            # Last attempt failed; re-raise the error
                            raise

                if await client.is_user_authorized():
                    session_path = context.user_data.get("login_session_path")
                    await update.message.reply_text(
                        "✅ Telethon userbot login successful. Session saved locally."
                        + (f"\nSaved session: {session_path}" if session_path else "")
                    )
                    await client.disconnect()
                    _clear_login_flow(user_id, context)
                    return
                else:
                    await update.message.reply_text(
                        "Login code accepted but the session is not authorized. Please reply with your password if 2FA is enabled."
                    )
                    context.user_data["awaiting_login_password"] = True
                    context.user_data.pop("awaiting_login_code", None)
                    return
            except Exception as exc:
                # Detect Telethon-specific exceptions and give actionable messages
                try:
                    from telethon.errors import (
                        SessionPasswordNeededError,
                        PhoneCodeInvalidError,
                        PhoneCodeExpiredError,
                        FloodWaitError,
                    )
                except Exception:
                    SessionPasswordNeededError = PhoneCodeInvalidError = PhoneCodeExpiredError = FloodWaitError = None

                # 2FA required
                if SessionPasswordNeededError and isinstance(exc, SessionPasswordNeededError):
                    await update.message.reply_text(
                        "Two-step verification is enabled. Please reply with your account password."
                    )
                    context.user_data["awaiting_login_password"] = True
                    context.user_data.pop("awaiting_login_code", None)
                    return

                # Specific error responses
                friendly = None
                try:
                    if PhoneCodeInvalidError and isinstance(exc, PhoneCodeInvalidError):
                        friendly = "The code you entered is invalid. Please request a new code and try again."
                    elif PhoneCodeExpiredError and isinstance(exc, PhoneCodeExpiredError):
                        # Code has expired. Do NOT auto-resend, as this creates an infinite
                        # loop when the code validity window is very short (< 10s).
                        # With 2FA enabled, Telegram is especially strict about code timing.
                        # The expiry window is controlled by Telegram's server-side security policy.
                        # Even with the retry logic, if Telegram's window is < 5 seconds from the RPC request time,
                        # the code may expire in-flight before reaching the server.
                        friendly = (
                            "🔐 Your login code expired (Telegram's 2FA security policy has an extremely short code validity window).\n\n"
                            "ℹ️ We attempted the code 3 times with brief retries, but Telegram's server rejected it as expired.\n"
                            "This suggests Telegram's security setting on your account has a window of ~4-5 seconds from the backend RPC time.\n\n"
                            "Options:\n"
                            "• Try /login again (make sure your internet is stable)\n"
                            "• Temporarily disable 2FA in Telegram Settings to test if that increases the code window\n"
                            "• Contact Telegram support about your account's security policy"
                        )
                        logger.warning("PhoneCodeExpiredError after 3 sign_in attempts for %s; account has extremely strict code window", phone)
                    elif FloodWaitError and isinstance(exc, FloodWaitError):
                        wait = getattr(exc, 'seconds', None) or getattr(exc, 'timeout', None) or 60
                        until = time.time() + int(wait)
                        context.user_data["login_flood_wait_until"] = until
                        friendly = f"Too many attempts; please wait {int(wait)} seconds before retrying."
                except Exception:
                    friendly = None

                # Log detailed exception information for debugging
                logger.exception("/login code step failed (user=%s): %s", user_id, exc)

                # Reply with a helpful message to admin users; others get a generic prompt
                try:
                    if friendly:
                        await update.message.reply_text(friendly)
                    else:
                        if user_id == ADMIN_USER_ID:
                            await update.message.reply_text(
                                f"Sign-in error: {exc.__class__.__name__}: {exc}\nSee logs for details."
                            )
                        else:
                            await update.message.reply_text(
                                "Failed to complete Telethon login. Please make sure the code is correct and try /login again."
                            )
                except Exception:
                    pass

                try:
                    await client.disconnect()
                except Exception:
                    pass
                _clear_login_flow(user_id, context)
                return

        if context.user_data.get("awaiting_login_password"):
            password = update.message.text.strip()
            client = context.user_data.get("login_client")
            if client is None:
                await update.message.reply_text(
                    "Session state lost. Please run /login again to start a fresh login."
                )
                _clear_login_flow(user_id, context)
                return

            try:
                await client.sign_in(password=password)
                if await client.is_user_authorized():
                    session_path = context.user_data.get("login_session_path")
                    await update.message.reply_text(
                        "✅ Telethon userbot login successful. Session saved locally."
                        + (f"\nSaved session: {session_path}" if session_path else "")
                    )
                else:
                    await update.message.reply_text(
                        "Password accepted but the session is not authorized. Please run /login again."
                    )
            except Exception as exc:
                logger.exception("/login password step failed: %s", exc)
                await update.message.reply_text(
                    "Failed to complete Telethon login with password. Please try /login again."
                )
            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass
                _clear_login_flow(user_id, context)
            return

        _clear_login_flow(user_id, context)
        return

    login_text_filter = filters.TEXT & ~filters.COMMAND & AwaitingLoginFilter()
    application.add_handler(
        MessageHandler(login_text_filter, latency_wrapper(_process_login_text, "process_login_text"), block=True)
    )

    # Store handler manager in bot_data for access in other handlers
    application.bot_data["handler_manager"] = handler_manager

    # Error handler (must be added last)
    application.add_error_handler(error_handler)

    logger.info("✅ All handlers registered successfully")


