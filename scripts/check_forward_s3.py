import sys
import os
import asyncio
import traceback

# Ensure project root is on sys.path so local `utils` package is imported
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from utils.storage import get_storage_backend_sync

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/check_forward_s3.py <key>")
        sys.exit(2)
    key = sys.argv[1]
    try:
        b = get_storage_backend_sync()
    except Exception as e:
        print("Failed to get storage backend sync:", e)
        traceback.print_exc()
        sys.exit(1)

    print("Using backend:", type(b).__name__)
    dest_dir = os.path.join("storage", "temp")
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, "check_" + os.path.basename(key))

    try:
        ok = asyncio.run(b.download_file(key, dest))
        print("download_file returned:", ok)
        if ok:
            print("Downloaded to:", dest)
    except Exception as e:
        print("download_file raised:", e)
        traceback.print_exc()

    # try presigned URL generation
    try:
        url = asyncio.run(b.generate_presigned_get(key, expires=60))
        print("presigned_get:", url)
    except Exception as e:
        print("generate_presigned_get failed:", e)
        traceback.print_exc()
    # Try an HTTP GET on the presigned URL to validate accessibility
    try:
        try:
            import requests
            use_requests = True
        except Exception:
            import urllib.request as _ur
            use_requests = False

        if 'url' in locals() and url:
            print('\nAttempting HTTP GET on presigned URL...')
            if use_requests:
                try:
                    r = requests.get(url, stream=True, timeout=30)
                    print('HTTP GET status:', r.status_code)
                    print('Headers:', dict(r.headers))
                    r.close()
                except Exception as e:
                    print('requests.get failed:', e)
            else:
                try:
                    resp = _ur.urlopen(url, timeout=30)
                    print('HTTP GET status:', resp.getcode())
                    print('Headers:', dict(resp.getheaders()))
                except Exception as e:
                    print('urllib.request failed:', e)
    except Exception:
        traceback.print_exc()
