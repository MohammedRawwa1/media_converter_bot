import os, sys, json, traceback
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
import redis

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: python scripts/update_queue_job_input.py <job_id> <input_path>')
        sys.exit(2)
    job_id = sys.argv[1]
    input_path = sys.argv[2]
    red_url = os.environ.get('REDIS_URL')
    if not red_url:
        print('REDIS_URL not set')
        sys.exit(1)
    r = redis.from_url(red_url, decode_responses=True)
    try:
        lst = r.lrange('ffmpeg:jobs', 0, -1)
    except Exception as e:
        print('Failed to read job list:', e)
        traceback.print_exc()
        sys.exit(1)
    idx = None
    for i, raw in enumerate(lst):
        try:
            obj = json.loads(raw)
            if obj.get('job_id') == job_id:
                idx = i
                break
        except Exception:
            continue
    if idx is None:
        print('Job not found in ffmpeg:jobs list')
        sys.exit(1)
    print('Found job at index', idx)
    try:
        obj['input_path'] = input_path
        # also set 'input' for status clarity
        obj['input'] = input_path
        new_raw = json.dumps(obj)
        r.lset('ffmpeg:jobs', idx, new_raw)
        print('Updated queue element at index', idx)
    except Exception as e:
        print('Failed to update element:', e)
        traceback.print_exc()
        sys.exit(1)
