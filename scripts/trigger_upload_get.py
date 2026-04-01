import os, sys
from urllib import request, parse

# load .env
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if os.path.exists(env_path):
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k,v=line.split('=',1)
                v=v.strip()
                if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                    v=v[1:-1]
                os.environ.setdefault(k.strip(), v)

secret = os.environ.get('UPLOAD_SECRET')
if not secret:
    print('UPLOAD_SECRET not set in .env')
    sys.exit(2)

forward_hash = sys.argv[1] if len(sys.argv) > 1 else None
if not forward_hash:
    print('Usage: python trigger_upload_get.py <forward_hash>')
    sys.exit(1)

url = 'https://media-converter-bot-1.onrender.com/upload'
qs = parse.urlencode({'forward_hash': forward_hash})
full = f"{url}?{qs}"
req = request.Request(full, headers={'X-Upload-Token': secret}, method='GET')

try:
    with request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode('utf-8', errors='replace')
        print('HTTP', resp.status)
        print(body)
except Exception as e:
    print('Request failed:', e)
    import traceback
    traceback.print_exc()
    sys.exit(3)
