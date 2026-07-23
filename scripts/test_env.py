import os
import socket
import urllib.parse

# load .env
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if os.path.exists(env_path):
    with open(env_path,encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k,v=line.split('=',1)
                v=v.strip()
                # remove surrounding quotes
                if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                    v=v[1:-1]
                os.environ[k.strip()]=v
# Force KEEP_LOCAL_UPLOADS for this test
os.environ['KEEP_LOCAL_UPLOADS']='1'
print('KEEP_LOCAL_UPLOADS=',os.environ.get('KEEP_LOCAL_UPLOADS'))
# Print S3 related variables
for k in ('STORAGE_BACKEND','S3_BUCKET','S3_ENDPOINT','S3_REGION','AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','S3_USE_SSL'):
    print(k,':',os.environ.get(k))
# Parse endpoint host and try to connect to 443
endpoint = os.environ.get('S3_ENDPOINT')
if endpoint:
    parsed = urllib.parse.urlparse(endpoint if '://' in endpoint else 'https://'+endpoint)
    host = parsed.hostname
    port = 443 if parsed.scheme in ('https','') else (80 if parsed.scheme=='http' else 443)
    print('parsed S3 host:', host, 'port:', port)
    try:
        s = socket.create_connection((host, port), timeout=5)
        s.close()
        print('TCP CONNECT OK to', host, port)
    except Exception as e:
        print('TCP CONNECT FAIL to', host, port, '->', e)
else:
    print('No S3_ENDPOINT set')
