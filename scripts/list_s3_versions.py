import os
import sys
import traceback

# ensure repo root on path
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

import boto3
from utils.storage import get_storage_backend_sync

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python scripts/list_s3_versions.py <key>')
        sys.exit(2)
    key = sys.argv[1]
    try:
        backend = get_storage_backend_sync()
    except Exception as e:
        print('Failed to get backend:', e)
        traceback.print_exc()
        sys.exit(1)
    kw = backend._client_kwargs()
    try:
        client = boto3.client('s3', **kw)
    except Exception as e:
        print('Failed to create boto3 client:', e)
        traceback.print_exc()
        sys.exit(1)
    try:
        resp = client.list_object_versions(Bucket=backend.bucket, Prefix=key)
        print('Response keys:')
        versions = resp.get('Versions', [])
        del_markers = resp.get('DeleteMarkers', [])
        print('Versions:')
        for v in versions:
            print(' -', v.get('Key'), 'VersionId=', v.get('VersionId'), 'IsLatest=', v.get('IsLatest'))
        print('DeleteMarkers:')
        for d in del_markers:
            print(' -', d.get('Key'), 'VersionId=', d.get('VersionId'), 'IsLatest=', d.get('IsLatest'))
    except Exception as e:
        print('list_object_versions failed:', e)
        traceback.print_exc()
