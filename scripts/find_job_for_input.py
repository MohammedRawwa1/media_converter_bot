import os
import sys
import traceback

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

import redis

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python scripts/find_job_for_input.py <substring>')
        sys.exit(2)
    substr = sys.argv[1]
    red_url = os.environ.get('REDIS_URL')
    if not red_url:
        print('REDIS_URL not set')
        sys.exit(1)
    r = redis.from_url(red_url, decode_responses=True)
    try:
        keys = r.keys('ffmpeg:job:*')
    except Exception as e:
        print('Failed to list keys:', e)
        traceback.print_exc()
        sys.exit(1)
    found = []
    for k in keys:
        try:
            h = r.hgetall(k)
            for v in ('input', 'input_path', 'input_key', 'output'):
                val = h.get(v) or ''
                if substr in val:
                    found.append((k, h))
                    break
        except Exception:
            pass
    print('Found', len(found), 'jobs matching', substr)
    for k, h in found:
        print('\n', k)
        for kk, vv in h.items():
            print(' ', kk, ':', vv)
