import contextlib
import json
import os
import sys
import traceback

import botocore

# load .env (simple parser)
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if os.path.exists(env_path):
    with open(env_path, encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k,v=line.split('=',1)
                v=v.strip()
                if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                    v=v[1:-1]
                os.environ[k.strip()] = v

REAL_KEY = 'forwards/0fc1635084302b371df176db50e09.json'
PLACEHOLDER = '<INPUT_KEY>'

bucket = os.getenv('S3_BUCKET')
endpoint = os.getenv('S3_ENDPOINT')
region = os.getenv('S3_REGION')
access = os.getenv('AWS_ACCESS_KEY_ID')
secret = os.getenv('AWS_SECRET_ACCESS_KEY')

print('Checking S3 object:', REAL_KEY, 'in bucket', bucket)

try:
    import boto3
    kwargs = {}
    if endpoint:
        kwargs['endpoint_url'] = endpoint
    if region:
        kwargs['region_name'] = region
    if access:
        kwargs['aws_access_key_id'] = access
    if secret:
        kwargs['aws_secret_access_key'] = secret
    s3 = boto3.client('s3', **kwargs)
    try:
        s3.head_object(Bucket=bucket, Key=REAL_KEY)
        print('S3 object exists: head_object succeeded')
    except botocore.exceptions.ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        print('S3 head_object failed:', code, e)
        print('Aborting — object not found')
        sys.exit(2)
except Exception:
    print('Failed to create S3 client or head_object check:')
    traceback.print_exc()
    sys.exit(3)

# Now update Redis job hashes that reference PLACEHOLDER
try:
    import redis
    REDIS_URL = os.getenv('REDIS_URL')
    if not REDIS_URL:
        print('REDIS_URL not set; aborting')
        sys.exit(4)
    r = redis.from_url(REDIS_URL, socket_connect_timeout=5, socket_timeout=5)
    updated = []
    for key in r.scan_iter(match='ffmpeg:job:*', count=200):
        try:
            raw = r.hgetall(key)
            if not raw:
                continue
            decoded = { (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in raw.items() }
            if decoded.get('input_key') == PLACEHOLDER or decoded.get('input') == PLACEHOLDER:
                job_id = key.decode().split(':')[-1]
                print('Found placeholder in job:', job_id)
                mapping = {
                    'input_key': REAL_KEY,
                    'input': REAL_KEY,
                    'status': 'queued',
                    'progress': '0',
                    'message': 'requeued'
                }
                r.hset(key, mapping=mapping)
                # Build job JSON to push to queue (include minimal metadata)
                job_json = {
                    'job_id': job_id,
                    'input_key': REAL_KEY,
                    'original_filename': decoded.get('original_filename',''),
                    'size': int(decoded.get('size') or 0),
                    'chat_id': decoded.get('chat_id') or None,
                    'request_id': decoded.get('request_id') or ''
                }
                r.lpush('ffmpeg:jobs', json.dumps(job_json))
                updated.append(job_id)
        except Exception:
            traceback.print_exc()
    if updated:
        print('Updated jobs:', updated)
    else:
        print('No jobs found referencing the placeholder; nothing changed')
    with contextlib.suppress(Exception):
        r.close()
except Exception:
    print('Redis update failed:')
    traceback.print_exc()
    sys.exit(5)

print('Done')
