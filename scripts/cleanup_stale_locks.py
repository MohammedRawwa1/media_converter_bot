#!/usr/bin/env python3
"""Inspect and clean up stale ffmpeg input locks in Redis.

Usage:
  # List all current locks
  python scripts/cleanup_stale_locks.py --list

  # Remove all locks (stale or otherwise)
  python scripts/cleanup_stale_locks.py --all

  # Remove locks for a specific job_id
  python scripts/cleanup_stale_locks.py --job <job_id>

  # Remove a specific lock key
  python scripts/cleanup_stale_locks.py --key ffmpeg:lock:<hash>

  # Dry-run: show what would be removed without actually deleting
  python scripts/cleanup_stale_locks.py --all --dry-run
"""

import argparse
import hashlib
import os
import sys

# Ensure repository root is importable when running as a script
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)


def get_redis_client():
    """Create a sync Redis client from REDIS_URL env var."""
    red_url = os.environ.get("REDIS_URL")
    if not red_url:
        print("ERROR: REDIS_URL environment variable is not set")
        sys.exit(1)

    import redis as redis_sync

    try:
        client = redis_sync.from_url(red_url, decode_responses=True)
        client.ping()
        return client
    except Exception as e:
        print(f"ERROR: Failed to connect to Redis: {e}")
        sys.exit(2)


def list_locks(r) -> dict[str, str]:
    """Return a dict of {lock_key: job_id} for all ffmpeg:lock:* keys."""
    keys = r.keys("ffmpeg:lock:*")
    locks: dict[str, str] = {}
    for k in keys:
        kstr = k.decode() if isinstance(k, bytes) else k
        v = r.get(k)
        vstr = v.decode() if isinstance(v, bytes) else (v or "unknown")
        locks[kstr] = vstr
    return locks


def compute_lock_key(input_path_or_job_id: str) -> str:
    """Compute the ffmpeg:lock key for a given input path or job_id."""
    lock_hash = hashlib.sha256(input_path_or_job_id.encode()).hexdigest()
    return f"ffmpeg:lock:{lock_hash}"


def remove_locks(r, lock_keys: list[str], dry_run: bool) -> int:
    """Remove the given lock keys. Returns count of removed keys."""
    removed = 0
    for lk in lock_keys:
        if dry_run:
            print(f"  [DRY-RUN] Would remove: {lk}")
            removed += 1
        else:
            try:
                r.delete(lk)
                print(f"  Removed: {lk}")
                removed += 1
            except Exception as e:
                print(f"  ERROR removing {lk}: {e}")
    return removed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect and clean up stale ffmpeg input locks in Redis.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all current ffmpeg:lock:* keys with their owning job_id",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Remove ALL ffmpeg:lock:* keys (combine with --dry-run to preview)",
    )
    parser.add_argument(
        "--job",
        type=str,
        default=None,
        help="Remove lock(s) whose value matches the given job_id",
    )
    parser.add_argument(
        "--key",
        type=str,
        default=None,
        help="Remove a specific ffmpeg:lock:<hash> key",
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Remove lock for a specific input path (computes the SHA-256 hash automatically)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview removals without actually deleting any keys",
    )
    args = parser.parse_args()

    r = get_redis_client()
    locks = list_locks(r)

    if not locks:
        print("No ffmpeg:lock:* keys found in Redis.")
        return 0

    # --list mode: show all locks
    if args.list:
        print(f"\nFound {len(locks)} ffmpeg:lock key(s):\n")
        for lk, jid in sorted(locks.items()):
            print(f"  {lk}  ->  job_id={jid}")
        print()
        return 0

    # Collect which lock keys to remove
    to_remove: list[str] = []

    if args.all:
        to_remove = list(locks.keys())

    if args.job:
        for lk, jid in locks.items():
            if jid == args.job:
                to_remove.append(lk)
        if not to_remove:
            print(f"No lock found for job_id '{args.job}' in Redis.")

    if args.key:
        if args.key in locks:
            to_remove.append(args.key)
        elif args.key.startswith("ffmpeg:lock:"):
            print(f"WARNING: Lock key '{args.key}' not found in Redis.")
        else:
            # Try prefixing
            full_key = f"ffmpeg:lock:{args.key}"
            if full_key in locks:
                to_remove.append(full_key)
            else:
                print(f"WARNING: No lock found matching '{args.key}' or '{full_key}'.")
                # Still add for removal attempt
                to_remove.append(full_key)

    if args.input:
        computed_key = compute_lock_key(args.input)
        if computed_key in locks:
            to_remove.append(computed_key)
        else:
            print(f"WARNING: No lock found for input '{args.input}' (computed key: {computed_key})")
            to_remove.append(computed_key)

    if not to_remove:
        print("No locks matched the given criteria. Use --list to see all locks.")
        return 0

    # Remove duplicates while preserving order
    seen = set()
    to_remove = [x for x in to_remove if not (x in seen or seen.add(x))]

    print(f"\n{'[DRY-RUN] ' if args.dry_run else ''}Removing {len(to_remove)} lock(s):")
    removed = remove_locks(r, to_remove, dry_run=args.dry_run)
    print(f"\n{'[DRY-RUN] Would have removed' if args.dry_run else 'Removed'} {removed} lock key(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
