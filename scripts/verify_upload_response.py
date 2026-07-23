import contextlib
import json
import os
import sys

# load .env in project root
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if os.path.exists(env_path):
    with open(env_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k, v = line.split('=', 1)
                v = v.strip()
                if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                    v = v[1:-1]
                os.environ.setdefault(k.strip(), v)

RESP_FILE = os.path.join(os.path.dirname(__file__), '..', 'upload_response.json')

if not os.path.exists(RESP_FILE):
    print('Response file not found:', RESP_FILE)
    sys.exit(2)

with open(RESP_FILE, encoding='utf-8') as fh:
    try:
        resp = json.load(fh)
    except Exception:
        print('Failed to parse JSON response:')
        print(fh.read())
        sys.exit(3)

print('Server response:')
print(json.dumps(resp, indent=2))

job_id = resp.get('job_id') or resp.get('job')
if not job_id:
    print('No job_id in response; nothing to verify')
    sys.exit(0)

print('Verifying job in Redis:', job_id)
try:
    import redis
    REDIS_URL = os.environ.get('REDIS_URL')
    if not REDIS_URL:
        print('REDIS_URL not set in .env or environment')
        sys.exit(4)
    r = redis.from_url(REDIS_URL, socket_connect_timeout=5, socket_timeout=5)
    key = f'ffmpeg:job:{job_id}'
    raw = r.hgetall(key)
    if not raw:
        print('No job hash found for', job_id)
        sys.exit(5)
    decoded = { (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in raw.items() }
    print('Job hash content:')
    print(json.dumps(decoded, indent=2))
    with contextlib.suppress(Exception):
        r.close()
except Exception:
    print('Failed to query Redis:')
    import traceback
    traceback.print_exc()
    sys.exit(6)

print('Done')
