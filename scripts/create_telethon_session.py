#!/usr/bin/env python3
"""
Create a Telethon session and export a session string.

Run this script **LOCALLY** (on your own machine, NOT on the server)
to generate a Telethon session string. Then set it as an env var on
the server so the bot can use it without the broken interactive /login flow.

Why offline?
------------
The bot's interactive /login flow keeps failing because:
1. Two bot instances share the same API_ID/API_HASH
2. 2FA is enabled on the account
3. Verification codes expire in ~8 seconds on the server
4. `account.GetPasswordRequest` can't detect 2FA before sign_in

Running this script locally avoids all these issues — no competing
bot instances, no stale connections, and proper 2FA handling.

Usage:
    # Set API credentials
    export API_ID=12345
    export API_HASH=your_api_hash

    # Run the script (interactive — will prompt for phone/code/password)
    python scripts/create_telethon_session.py

    # Or export from an existing .session file
    python scripts/create_telethon_session.py --from-file my_session.session

    # Then on the server, set the output as:
    #   TELETHON_SESSION='<the session string>'

Requirements:
    - Telethon: pip install telethon
    - API_ID and API_HASH from https://my.telegram.org/apps
"""

import asyncio
import getpass
import logging
import os
import pathlib
import sys
import time

# Ensure project root is on sys.path
_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Set up verbose logging to see all Telethon internals
logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s - %(message)s",
    level=logging.DEBUG,
    stream=sys.stderr,
)
logger = logging.getLogger("session_creator")


def get_credentials() -> tuple:
    """Return (api_id, api_hash) from env or prompt."""
    api_id = (
        os.getenv("API_ID")
        or os.getenv("TELEGRAM_APP_ID")
    )
    api_hash = (
        os.getenv("API_HASH")
        or os.getenv("TELEGRAM_API_HASH")
    )

    if not api_id:
        api_id = input("Enter your API_ID: ").strip()
    if not api_hash:
        api_hash = input("Enter your API_HASH: ").strip()

    try:
        api_id = int(api_id)
    except (TypeError, ValueError):
        print("ERROR: API_ID must be an integer. Get yours from https://my.telegram.org/apps")
        sys.exit(1)

    if not api_hash:
        print("ERROR: API_HASH is required. Get yours from https://my.telegram.org/apps")
        sys.exit(1)

    return api_id, api_hash


async def create_session(api_id: int, api_hash: str):
    """Create a new Telethon session interactively and export the session string."""
    try:
        from telethon import TelegramClient, errors
        from telethon import functions
        from telethon.sessions import StringSession
        from telethon import utils as telethon_utils
    except ImportError:
        print("Telethon is not installed. Install it with:\n  pip install telethon")
        sys.exit(1)

    print(f"\n{'=' * 60}")
    print("Telethon Session Creator (VERBOSE MODE)")
    print(f"{'=' * 60}")
    print(f"\nAPI_ID: {api_id}")
    print("\nYou will be prompted to enter:")
    print("  1. Your phone number (international format, e.g. +1234567890)")
    print("  2. The login code sent to your Telegram app or SMS")
    print("  3. Your 2FA password (if enabled)")
    print("\nAll internal API calls will be logged below for debugging.")
    print(f"\n{'=' * 60}\n")

    # Create client with an in-memory StringSession so we can export it
    client = TelegramClient(StringSession(), api_id, api_hash)

    try:
        # ── Step 1: Connect ────────────────────────────────────────────
        logger.info("STEP 1: Connecting to Telegram DC...")
        await client.connect()
        logger.info("Connected! DC ID: %s", client.session.dc_id)

        # ── Check if already authorized ────────────────────────────────
        if await client.is_user_authorized():
            logger.warning("Already authorized! You don't need to login again.")
            session_string = client.session.save()
            print(f"\n📋 Existing session string:\n{session_string}")
            return

        # ── Step 2: Get phone number from user ─────────────────────────
        phone = input("\nPlease enter your phone (or bot token): ").strip()

        # ── Step 3: Send code request ──────────────────────────────────
        logger.info("STEP 2: Sending code request to %s...", phone)
        sent = await client.send_code_request(phone)

        logger.info("send_code_request response:")
        logger.info("  phone_code_hash: %s", sent.phone_code_hash)
        logger.info("  phone_registered: %s", getattr(sent, 'phone_registered', '?'))
        logger.info("  timeout: %s", getattr(sent, 'timeout', '?'))
        logger.info("  type: %s", type(sent).__name__)
        logger.info("  next_code_type: %s", getattr(sent, 'next_code_type', '?'))
        logger.info("  is_code_available: %s", getattr(sent, 'is_code_available', '?'))

        # Check what type of code was sent
        _type_str = str(getattr(sent, 'type', ''))
        if 'sms' in _type_str.lower() or 'Sms' in _type_str:
            logger.info("  Code type: SMS (check your SMS messages)")
        else:
            logger.info("  Code type: Telegram app notification")

        # Check Telethon's internal cache
        _parsed_phone = telethon_utils.parse_phone(phone)
        _cached_hash = client._phone_code_hash.get(_parsed_phone)
        logger.info("Telethon internal _phone_code_hash[%s] = %s", _parsed_phone, _cached_hash)
        logger.info("Our stored phone_code_hash = %s", sent.phone_code_hash)
        logger.info("Hashes match: %s", _cached_hash == sent.phone_code_hash)

        # ── Step 4: Try to detect 2FA via GetPasswordRequest ───────────
        logger.info("STEP 3: Attempting 2FA detection via GetPasswordRequest...")
        try:
            pwd_info = await client(functions.account.GetPasswordRequest())
            has_pwd = bool(getattr(pwd_info, 'has_password', False))
            hint = str(getattr(pwd_info, 'hint', '') or '')
            logger.info("GetPasswordRequest result:")
            logger.info("  has_password: %s", has_pwd)
            logger.info("  hint: '%s'", hint)
            if has_pwd:
                print(f"\n🔐 2FA detected! Password hint: '{hint}'")
        except Exception as pwd_exc:
            logger.warning("GetPasswordRequest FAILED: %s", pwd_exc)
            logger.warning("(This is expected — you can't check 2FA before sign_in on some setups)")

        # ── Step 5: Get code from user ─────────────────────────────────
        code = input("\nPlease enter the code you received: ").strip()

        # ── Step 6: Sign in with code ──────────────────────────────────
        logger.info("STEP 4: Signing in with code (hash=%s...)...", str(sent.phone_code_hash)[:10])

        # Show what sign_in will use internally
        _parsed_phone2 = telethon_utils.parse_phone(phone)
        _cached_hash2 = client._phone_code_hash.get(_parsed_phone2)
        logger.info("Before sign_in — internal hash for %s: %s", _parsed_phone2, _cached_hash2)

        try:
            result = await client.sign_in(
                phone=phone,
                code=code,
                # NOTE: We pass phone_code_hash explicitly, same as server
                phone_code_hash=sent.phone_code_hash,
            )
            logger.info("sign_in returned: %s", type(result).__name__)
            # If we get here, no 2FA needed
            logger.info("Login successful without 2FA!")

        except errors.SessionPasswordNeededError:
            logger.info("STEP 5: 2FA required! SessionPasswordNeededError raised.")
            print("\n🔐 Two-step verification is enabled.")
            print("Please enter your account password.")

            # ── Step 7: Get password from user ─────────────────────
            password = getpass.getpass("\nPlease enter your password: ").strip()

            # ── Step 8: Sign in with password ──────────────────────
            logger.info("STEP 6: Signing in with password...")
            try:
                # Check password hint first (this should work now)
                pwd_info2 = await client(functions.account.GetPasswordRequest())
                has_pwd2 = bool(getattr(pwd_info2, 'has_password', False))
                hint2 = str(getattr(pwd_info2, 'hint', '') or '')
                logger.info("GetPasswordRequest (after SessionPasswordNeeded):")
                logger.info("  has_password: %s", has_pwd2)
                logger.info("  hint: '%s'", hint2)
            except Exception as pwd_exc2:
                logger.warning("GetPasswordRequest after 2FA still failed: %s", pwd_exc2)

            await client.sign_in(password=password)
            logger.info("Password accepted!")

        except errors.PhoneCodeExpiredError:
            logger.error("❌ PhoneCodeExpiredError raised!")
            logger.error("This is what happens on the server.")
            logger.error("The phone_code_hash was: %s", sent.phone_code_hash)
            logger.error("Telethon's internal hash was: %s", client._phone_code_hash.get(telethon_utils.parse_phone(phone)))
            logger.error("DC ID: %s", client.session.dc_id)
            print("\n❌ Code expired! This is the same error the server gets.")
            print("The hash was valid when we received it, but expired before sign_in was called.")
            print("Try running the script again and enter the code faster.")
            await client.disconnect()
            return

        except errors.PhoneCodeInvalidError:
            logger.error("❌ PhoneCodeInvalidError — wrong code entered")
            print("\n❌ Wrong code. Please run the script again and enter the correct code.")
            await client.disconnect()
            return

        # ── Step 9: Success! ──────────────────────────────────────
        print(f"\n{'=' * 60}")
        print("✅ Login successful!")
        print(f"{'=' * 60}")

        # Extract the session string
        session_string = client.session.save()

        me = await client.get_me()
        print(f"\nUser: {me.first_name or ''} {me.last_name or ''}".strip())
        print(f"User ID: {me.id}")
        print(f"Phone: +{me.phone}")
        print(f"DC ID: {client.session.dc_id}")

        print(f"\n{'=' * 60}")
        print("📋 SESSION STRING (copy this):")
        print(f"{'=' * 60}")
        print(session_string)
        print(f"{'=' * 60}")
        print("\nSet this as an environment variable on your server:")
        print(f"  TELETHON_SESSION='{session_string[:50]}...'")
        print("\nOr add it as a secret environment variable on your hosting platform (Railway/Render/etc).")
        print("\nAfter setting the env var, restart the bot. The /login flow is no longer needed.")

        # Also save to a text file for convenience
        with open("telethon_session.txt", "w") as f:
            f.write(session_string)
        print("\n✅ Also saved to telethon_session.txt")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await client.disconnect()


async def export_from_file(api_id: int, api_hash: str, session_file: str):
    """Export a session string from an existing .session file."""
    try:
        from telethon import TelegramClient
    except ImportError:
        print("Telethon is not installed. Install it with: pip install telethon")
        sys.exit(1)

    if not os.path.exists(session_file):
        if not session_file.endswith(".session"):
            session_file += ".session"
        if not os.path.exists(session_file):
            print(f"ERROR: Session file not found: {session_file}")
            sys.exit(1)

    session_name = os.path.splitext(os.path.basename(session_file))[0]
    print(f"Exporting session string from: {session_file}")

    # Use file-based session so Telethon loads the existing .session file
    client = TelegramClient(session_name, api_id, api_hash)
    try:
        await client.start()
        session_string = client.session.save()

        print(f"\n{'=' * 60}")
        print("📋 SESSION STRING:")
        print(f"{'=' * 60}")
        print(session_string)
        print(f"{'=' * 60}")

        with open(f"{session_name}_exported.txt", "w") as f:
            f.write(session_string)
        print(f"\n✅ Saved to {session_name}_exported.txt")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    finally:
        await client.disconnect()


async def main():
    args = sys.argv[1:]

    from_file = None
    for i, arg in enumerate(args):
        if arg in ("--from-file", "--export") and i + 1 < len(args):
            from_file = args[i + 1]

    api_id, api_hash = get_credentials()

    if from_file:
        await export_from_file(api_id, api_hash, from_file)
    else:
        await create_session(api_id, api_hash)


if __name__ == "__main__":
    asyncio.run(main())
