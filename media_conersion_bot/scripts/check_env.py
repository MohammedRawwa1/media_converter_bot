#!/usr/bin/env python3
"""Simple environment and dependency checker for the Media Conversion Bot.

Checks for: BOT_TOKEN, ffmpeg binary availability, ffmpeg-python import,
and optional MongoDB URL.
"""
import os
import sys
import subprocess
import logging

from dotenv import load_dotenv


def check_bot_token():
    token = os.getenv('BOT_TOKEN')
    if not token:
        print('[ERROR] BOT_TOKEN is not set')
        return False
    print('[OK] BOT_TOKEN present')
    return True


def check_ffmpeg_binary():
    path = os.getenv('FFMPEG_PATH', 'ffmpeg')
    try:
        proc = subprocess.run([path, '-version'], capture_output=True, text=True, timeout=5)
        if proc.returncode == 0:
            print(f'[OK] ffmpeg binary found at "{path}"')
            return True
        else:
            print(f'[ERROR] ffmpeg binary returned non-zero exit: {proc.returncode}')
            return False
    except FileNotFoundError:
        print(f'[ERROR] ffmpeg binary not found at "{path}"')
        return False
    except Exception as e:
        print(f'[ERROR] ffmpeg binary check failed: {e}')
        return False


def check_ffmpeg_python():
    try:
        import importlib
        importlib.import_module('ffmpeg')
        print('[OK] ffmpeg-python binding available')
        return True
    except Exception:
        print('[WARN] ffmpeg-python binding not installed (some probe features will be unavailable)')
        return False


def check_mongodb():
    url = os.getenv('MONGODB_URL')
    if url:
        print('[OK] MONGODB_URL set')
        return True
    else:
        print('[INFO] MONGODB_URL not set; DB logging disabled')
        return False


def main():
    load_dotenv()

    ok = True
    if not check_bot_token():
        ok = False

    if not check_ffmpeg_binary():
        ok = False

    # ffmpeg-python optional
    check_ffmpeg_python()

    check_mongodb()

    if not ok:
        print('\nOne or more required items are missing. Fix the errors and try again.')
        sys.exit(1)
    else:
        print('\nEnvironment looks good for basic operation.')
        sys.exit(0)


if __name__ == '__main__':
    main()
