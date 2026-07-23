import os
import sys
import traceback

# ensure repo root on sys.path
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

import redis

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python scripts/check_jobs_redis.py <job_id> [job_id2 ...]')
        sys.exit(2)
    red_url = os.environ.get('REDIS_URL')
    print('REDIS_URL:', red_url)
    if not red_url:
        print('REDIS_URL not set in environment')
        sys.exit(1)
    try:
        r = redis.from_url(red_url, decode_responses=True)
    except Exception as e:
        print('Failed to connect to Redis:', e)
        traceback.print_exc()
        sys.exit(1)
    for job in sys.argv[1:]:
        try:
            h = r.hgetall(f'ffmpeg:job:{job}')
            print('\nJob', job, 'hash:')
            if not h:
                print('(no hash found)')
            else:
                for k, v in h.items():
                    print(' ', k, ':', v)
        except Exception as e:
            print('Failed to fetch job', job, e)
            traceback.print_exc()
