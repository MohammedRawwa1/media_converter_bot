#!/usr/bin/env python3
"""Check who the userbot is logged in as (run on the Render server)."""

import asyncio
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.telethon_session import build_pyrogram_client, get_userbot_credentials


async def main():
    api_id, api_hash = get_userbot_credentials()
    client = build_pyrogram_client(api_id, api_hash)
    await client.start()
    me = await client.get_me()
    print("Userbot identity:")
    print(f"  ID: {me.id}")
    print(f"  Name: {me.first_name or ''} {me.last_name or ''}")
    if hasattr(me, 'phone_number'):
        print(f"  Phone: +{me.phone_number}")
    print("\nThis is the account that needs to be added to group -1004367325292")
    await client.stop()

asyncio.run(main())
