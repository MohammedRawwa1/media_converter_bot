#!/usr/bin/env python3
"""Issue, revoke or check a per-user web token (utils/web_users.py).

A web caller identifies with a token tied to a user id, instead of every caller
sharing ``UPLOAD_SECRET``. The token is shown exactly once here - only its digest
is stored - so copy it when it is printed and hand it to that user.

Usage:
  python3 scripts/mint_user_token.py --user 12345            # mint (or rotate)
  python3 scripts/mint_user_token.py --user 12345 --revoke   # cut the user off
  python3 scripts/mint_user_token.py --check TOKEN           # which user is this?

Rotating a token (--user on a user that already has one) replaces it, so the
previous value stops working immediately.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

# Ensure repository root is importable when running as a script.
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Load .env the same way the app does, so REDIS_URL is where the store expects it.
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(project_root, ".env"))
except Exception:
    pass

try:
    from utils.web_users import issue_user_token, resolve_user_token, revoke_user_token
except Exception:
    print("Failed to import utils.web_users. Run this from the repository root with dependencies installed.")
    import traceback

    traceback.print_exc()
    sys.exit(2)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mint_user_token")


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Issue or revoke a per-user web token.")
    parser.add_argument("--user", "-u", type=int, help="The user id the token belongs to")
    parser.add_argument("--revoke", action="store_true", help="Revoke instead of issuing")
    parser.add_argument("--check", metavar="TOKEN", help="Resolve a token to its user id")
    args = parser.parse_args(argv)

    if args.check:
        user_id = await resolve_user_token(args.check)
        if user_id is None:
            print("No user owns that token (or it was revoked).")
            return 1
        print(f"Token belongs to user {user_id}")
        return 0

    if args.user is None:
        parser.print_help()
        return 2

    if args.revoke:
        removed = await revoke_user_token(args.user)
        print(f"Revoked the token for user {args.user}" if removed else f"User {args.user} had no token")
        return 0

    token = await issue_user_token(args.user)
    if not token:
        print("Could not issue a token (check the user id and server logs).")
        return 1
    print(f"Web token for user {args.user} (shown once):")
    print(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
