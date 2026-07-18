import sys
import os
import json
import asyncio
import traceback
from uuid import uuid4

# Ensure repo root on sys.path
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

import boto3
from utils.storage import get_storage_backend_sync

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python scripts/restore_forward_and_enqueue.py <forward_id> [version_id]')
        sys.exit(2)
    fid = sys.argv[1]
    version_id = sys.argv[2] if len(sys.argv) > 2 else None
    key = f"forwards/{fid}.json"

    try:
        backend = get_storage_backend_sync()
    except Exception as e:
        print('Failed to get storage backend:', e)
        traceback.print_exc()
        sys.exit(1)

    kw = backend._client_kwargs()
    client = boto3.client('s3', **kw)

    try:
        if not version_id:
            print('No version_id provided; listing versions to pick first non-delete version...')
            resp = client.list_object_versions(Bucket=backend.bucket, Prefix=key)
            versions = resp.get('Versions', [])
            if not versions:
                print('No versions found for', key)
                sys.exit(1)
            # pick first version that's IsLatest==False or the latest real version
            version_id = versions[0].get('VersionId')
            print('Selected version_id =', version_id)

        print('Fetching object', key, 'version', version_id)
        resp = client.get_object(Bucket=backend.bucket, Key=key, VersionId=version_id)
        body = resp['Body'].read()
        data = json.loads(body)
        print('Loaded forward metadata:', data)
    except Exception as e:
        print('Failed to fetch forward metadata from S3:', e)
        traceback.print_exc()
        sys.exit(1)

    # Now attempt userbot download
    try:
        from utils.userbot_downloader import download_forward_via_userbot
    except Exception as e:
        print('userbot_downloader not available:', e)
        traceback.print_exc()
        sys.exit(1)

    try:
        chat_id = data.get('chat_id')
        message_id = data.get('message_id') or data.get('msg_id')
        if not chat_id or not message_id:
            print('Missing chat_id or message_id in metadata')
            sys.exit(1)

        jid = str(uuid4())
        ext = os.path.splitext(data.get('name') or '')[1] or '.mp4'
        input_dir = os.path.join(os.getcwd(), 'storage', 'input')
        os.makedirs(input_dir, exist_ok=True)
        dest_path = os.path.join(input_dir, f"{jid}{ext}")

        print('Downloading via userbot to', dest_path)
        ok = asyncio.run(download_forward_via_userbot(chat_id, int(message_id), dest_path, msg_date=data.get('registered_at')))
        print('download result:', ok)
        if not ok or not os.path.exists(dest_path):
            print('Download failed or file missing')
            sys.exit(1)

        # enqueue job
        try:
            from utils.job_queue import enqueue_job
        except Exception as e:
            print('enqueue_job not available:', e)
            traceback.print_exc()
            sys.exit(1)

        job_id = str(uuid4())
        output_dir = os.path.join(os.getcwd(), 'storage', 'output')
        os.makedirs(output_dir, exist_ok=True)
        base_name = os.path.splitext(data.get('name') or os.path.basename(dest_path))[0]
        output_path = os.path.join(output_dir, f"{base_name}_{job_id}.mp4")
        job = {
            'job_id': job_id,
            'input_path': dest_path,
            'output_path': output_path,
            'original_filename': data.get('name') or os.path.basename(dest_path),
            'output_filename': os.path.basename(output_path),
            'ffmpeg_args': ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
            'progress_channel': f"ffmpeg:progress:{job_id}",
            'cleanup_input': True,
        }
        print('Enqueuing job', job_id)
        asyncio.run(enqueue_job(job))
        print('Enqueued', job_id)
    except Exception as e:
        print('Error during download/enqueue:', e)
        traceback.print_exc()
        sys.exit(1)
