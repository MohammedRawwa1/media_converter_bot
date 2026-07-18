import os, json, redis
r = redis.from_url(os.environ['REDIS_URL'], decode_responses=True)

# Check delayed set
delayed = r.zrange('ffmpeg:delayed', 0, -1)
print('Delayed items:', len(delayed))
for d in delayed[:20]:
    try:
        j = json.loads(d)
        print(f'  job_id={j.get("job_id","?")}  input={j.get("input_path","") or j.get("input_key","")}')
    except Exception as e:
        print(f'  (parse error: {e})')

# Check main queue
queue_len = r.llen('ffmpeg:jobs')
print('\nQueue length:', queue_len)
items = r.lrange('ffmpeg:jobs', 0, 20)
for i in items[:20]:
    try:
        j = json.loads(i)
        print(f'  job_id={j.get("job_id","?")}  input={j.get("input_path","") or j.get("input_key","")}')
    except Exception as e:
        print(f'  (parse error: {e})')

# Check locks
locks = r.keys('ffmpeg:lock:*')
print('\nLocks:', len(locks))
for k in locks:
    print(f'  {k} => {r.get(k)}')
