import json
import os
import sys
import traceback

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
import redis

red_url = os.environ.get('REDIS_URL')
if not red_url:
    print('REDIS_URL not set')
    sys.exit(1)
try:
    r = redis.from_url(red_url, decode_responses=True)
    jobs = r.lrange('ffmpeg:jobs', 0, 20)
    print('ffmpeg:jobs count:', len(jobs))
    for i, raw in enumerate(jobs[:20]):
        try:
            obj = json.loads(raw)
            print('\n--- job list item', i)
            print(json.dumps(obj, indent=2))
        except Exception:
            print('raw:', raw[:200])
except Exception as e:
    print('failed to read redis:', e)
    traceback.print_exc()
