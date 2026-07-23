#!/usr/bin/env python3
"""Test if the Pyrogram userbot can access the relay group."""

import asyncio
import os
import sys

# Ensure project root is on sys.path
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.telethon_session import (
    build_pyrogram_client,
    get_pyrogram_session_string,
    get_userbot_credentials,
)

OK = "[OK]"
FAIL = "[FAIL]"
WARN = "[WARN]"


async def test_relay_access():
    relay_chat_id = os.getenv("RELAY_CHAT_ID", "").strip()
    if not relay_chat_id:
        print(f"{FAIL} RELAY_CHAT_ID env var is not set!")
        return False

    try:
        relay_chat_id_int = int(relay_chat_id)
    except ValueError:
        print(f"{FAIL} RELAY_CHAT_ID is not a valid integer: {relay_chat_id}")
        return False

    session_str = get_pyrogram_session_string()
    if not session_str:
        print(f"{FAIL} PYROGRAM_SESSION is not set!")
        return False

    print(f"\n{'='*60}")
    print(f"Testing Pyrogram userbot access to relay group: {relay_chat_id}")
    print(f"{'='*60}")

    try:
        api_id, api_hash = get_userbot_credentials()
        print(f"{OK} API_ID / API_HASH found")
    except RuntimeError as e:
        print(f"{FAIL} {e}")
        return False

    client = build_pyrogram_client(api_id, api_hash)
    if client is None:
        print(f"{FAIL} Failed to build Pyrogram client")
        return False

    try:
        await client.start()
        print(f"{OK} Pyrogram client started")

        me = await client.get_me()
        print(f"{OK} Userbot: {me.first_name or ''} {me.last_name or ''} (ID: {me.id})")

        # Try to get the relay chat
        print(f"\n--> Attempting get_chat({relay_chat_id})...")
        try:
            chat = await client.get_chat(relay_chat_id_int)
            print(f"{OK} Chat found!")
            print(f"   Title: {getattr(chat, 'title', 'N/A')}")
            print(f"   Type: {getattr(chat, 'type', 'N/A')}")
            print(f"   Members: {getattr(chat, 'members_count', 'N/A')}")
        except Exception as e:
            print(f"{FAIL} get_chat({relay_chat_id}) -> {e}")
            print("")
            print("   The userbot is NOT a member of the relay group.")
            print("")
            print("   Fix: Add the userbot account (phone number used for")
            print(f"   PYROGRAM_SESSION) as a member of group {relay_chat_id}")
            return False

        # Try to get messages from the relay chat
        print("\n--> Fetching recent messages...")
        try:
            messages = await client.get_chat_history(relay_chat_id_int, limit=5)
            count = 0
            async for _ in messages:
                count += 1
            print(f"{OK} Can read {count} recent messages in the relay group")
        except Exception as e:
            print(f"{WARN} Can access group but cannot read messages: {e}")

        print(f"\n{'='*60}")
        print(f"{OK} RELAY GROUP ACCESS: WORKING")
        print(f"{'='*60}")
        print("\nThe userbot can access the relay group.")
        print("Large file downloads should work now.")
        return True

    except Exception as e:
        print(f"\n{FAIL} Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        try:
            await client.stop()
            print(f"{OK} Pyrogram client stopped")
        except Exception:
            pass


if __name__ == "__main__":
    success = asyncio.run(test_relay_access())
    sys.exit(0 if success else 1)
