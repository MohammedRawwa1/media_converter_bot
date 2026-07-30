"""Clean up stale pipeline_dedup Redis keys.

Scans all ``ffmpeg:pipeline_dedup:*`` keys in Redis. For each one, checks
whether the referenced job is still active. If the job is stale (finished,
cancelled, errored, or the job hash no longer exists), the dedup key is
deleted so the same file can be re-processed.

Usage:
    python scripts/cleanup_stale_dedup_keys.py          # dry-run (no deletion)
    python scripts/cleanup_stale_dedup_keys.py --apply   # actually delete

The environment variable ``REDIS_URL`` must be set.
"""

import os
import sys

# ensure repo root on sys.path so config / utils can be imported if needed
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

import redis

# Active statuses — copied from the dedup check in ``_ensure_current_file_downloaded``
_ACTIVE_STATUSES = frozenset(
    {
        "processing",
        "queued",
        "waiting",
        "started",
        "uploading",
        "sending",
    }
)

_DEDUP_PREFIX = "ffmpeg:pipeline_dedup:"


def _is_job_active(r, stored_job_id: str) -> bool:
    """Check a job hash and return True if the job is still actively processing.

    ``"pending"`` is a special placeholder value set by the BigFilePipeline
    while a file is being ingested (before the real job_id is written).  We
    treat it as active to avoid deleting the dedup key mid-ingestion.

    On any Redis error we return True (conservative) so we never delete a
    key we aren't sure about.
    """
    if stored_job_id == "pending":
        return True  # mid-ingestion placeholder — treat as active

    try:
        job_hash = r.hgetall(f"ffmpeg:job:{stored_job_id}")
        if not job_hash:
            return False  # hash doesn't exist → stale
        status = job_hash.get(b"status") or job_hash.get("status")
        if not status:
            return False
        if isinstance(status, bytes):
            status = status.decode()
        return status in _ACTIVE_STATUSES
    except Exception as e:
        print(f"     ⚠️  Error checking job {stored_job_id}: {e}")
        return True  # conservative: assume active on error


def main():
    dry_run = "--apply" not in sys.argv

    red_url = os.environ.get("REDIS_URL")
    if not red_url:
        print("❌ REDIS_URL not set in environment")
        sys.exit(1)

    r = redis.from_url(red_url, decode_responses=False)
    print(f"🔗 Connected to Redis: {red_url[:20]}...\n")

    # Use SCAN to iterate over matching keys (avoids blocking on large Redis instances)
    cursor = 0
    total_scanned = 0
    stale_count = 0
    active_count = 0
    error_count = 0
    deleted_count = 0

    while True:
        cursor, keys = r.scan(cursor=cursor, match=f"{_DEDUP_PREFIX}*", count=500)
        for key in keys:
            total_scanned += 1
            if isinstance(key, bytes):
                key_str = key.decode()
            else:
                key_str = key

            # Extract the stored job_id from the dedup key value
            try:
                stored_val = r.get(key)
                if stored_val is None:
                    # Key disappeared between scan and get — skip
                    continue
                if isinstance(stored_val, bytes):
                    stored_job_id = stored_val.decode()
                else:
                    stored_job_id = str(stored_val)
            except Exception as e:
                print(f"  ⚠️  Error reading dedup key {key_str}: {e}")
                error_count += 1
                continue

            if _is_job_active(r, stored_job_id):
                print(f"  🔄 ACTIVE   {key_str} → job {stored_job_id}")
                active_count += 1
            else:
                print(f"  🗑️  STALE    {key_str} → job {stored_job_id}")
                stale_count += 1
                if not dry_run:
                    try:
                        r.delete(key)
                        deleted_count += 1
                    except Exception as e:
                        print(f"     ❌ Delete failed: {e}")
                        error_count += 1

        if cursor == 0:
            break

    # Summary
    print(f"\n{'=' * 50}")
    print(f"Scanned:     {total_scanned} dedup keys")
    print(f"Active:      {active_count}")
    print(f"Stale:       {stale_count}")
    print(f"Deleted:     {deleted_count}")
    print(f"Errors:      {error_count}")
    print(f"Mode:        {'DRY-RUN (no deletions)' if dry_run else 'APPLY (deletions performed)'}")
    print(f"{'=' * 50}")

    if dry_run and stale_count > 0:
        print(f"\n💡 Run with --apply to delete {stale_count} stale keys.")
    elif not dry_run:
        print(f"\n✅ Removed {deleted_count} stale dedup keys.")
    else:
        print("\n✅ No stale keys found.")


if __name__ == "__main__":
    main()
