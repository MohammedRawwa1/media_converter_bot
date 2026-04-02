#!/usr/bin/env python3
import os, sys, json, uuid, asyncio, traceback

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
                os.environ[k.strip()] = v

# ensure project root is importable
proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if proj_root not in sys.path:
    sys.path.insert(0, proj_root)

try:
    from utils.userbot_downloader import download_forward_via_userbot
    from utils.job_queue import enqueue_job
except Exception:
    print('Failed importing project helpers; ensure you run this from the project root and dependencies are installed')
    traceback.print_exc()
    sys.exit(2)

try:
    import boto3, botocore
except Exception:
    print('boto3 is required for this helper')
    traceback.print_exc()
    sys.exit(3)

try:
    import config
except Exception:
    config = None


def download_forward_json_from_s3(key: str, bucket: str, endpoint: str, region: str, access: str, secret: str):
    kwargs = {}
    if endpoint:
        kwargs['endpoint_url'] = endpoint
    if region:
        kwargs['region_name'] = region
    if access:
        kwargs['aws_access_key_id'] = access
    if secret:
        kwargs['aws_secret_access_key'] = secret

    client = boto3.client('s3', **kwargs)
    try:
        import config
    except Exception:
        config = None
    local_dir = os.path.join(os.path.dirname(__file__), '..', getattr(config, 'STORAGE_PATH', 'storage'), 'forwards')
    os.makedirs(local_dir, exist_ok=True)
    fid = None
    if key.startswith('forwards/') and key.endswith('.json'):
        fid = os.path.basename(key)[:-5]
    else:
        fid = os.path.basename(key).split('.')[0]
    local_path = os.path.join(local_dir, f"{fid}.json")
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        body = resp['Body'].read()
        with open(local_path, 'wb') as fh:
            fh.write(body)
        with open(local_path, 'r', encoding='utf-8') as fh:
            return json.load(fh), fid, local_path
    except botocore.exceptions.ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        print('S3 get_object failed:', code, e)
        return None, None, None
    except Exception:
        traceback.print_exc()
        return None, None, None


async def main(key: str):
    bucket = os.getenv('S3_BUCKET')
    endpoint = os.getenv('S3_ENDPOINT')
    region = os.getenv('S3_REGION')
    access = os.getenv('AWS_ACCESS_KEY_ID')
    secret = os.getenv('AWS_SECRET_ACCESS_KEY')

    meta, fid, local_json = download_forward_json_from_s3(key, bucket, endpoint, region, access, secret)
    if not meta:
        print('Could not fetch forward metadata from S3 for key', key)
        return 2

    print('Loaded forward metadata:', json.dumps(meta, indent=2))

    chat_id = meta.get('chat_id')
    message_id = meta.get('message_id') or meta.get('msg_id')
    file_unique_id = meta.get('file_unique_id')
    msg_date = meta.get('registered_at') or meta.get('created_at')
    ext = os.path.splitext(meta.get('name') or '')[1] or '.mp4'

    input_dir = 'storage/input'
    try:
        if config is not None:
            input_dir = getattr(config, 'INPUT_PATH', getattr(config, 'DOWNLOAD_LOCATION', input_dir)) or input_dir
    except Exception:
        pass
    os.makedirs(input_dir, exist_ok=True)

    jid = str(uuid.uuid4())
    input_path = os.path.join(input_dir, f"{jid}{ext}")

    print('Attempting to download media via userbot to', input_path)
    try:
        ok = await download_forward_via_userbot(chat_id, message_id, input_path, msg_date=msg_date, file_unique_id=file_unique_id)
    except Exception:
        print('download_forward_via_userbot raised:')
        traceback.print_exc()
        return 3

    if not ok or not os.path.exists(input_path):
        print('Failed to download media via userbot')
        return 4

    print('Downloaded media to', input_path)

    job_id = str(uuid.uuid4())
    output_dir = getattr(config, 'OUTPUT_PATH', 'storage/output') if config else 'storage/output'
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.splitext(meta.get('name') or os.path.basename(input_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}_{job_id}.mp4")

    job = {
        'job_id': job_id,
        'input_path': input_path,
        'output_path': output_path,
        'original_filename': meta.get('name') or os.path.basename(input_path),
        'output_filename': os.path.basename(output_path),
        'ffmpeg_args': None,
        'progress_channel': f'ffmpeg:progress:{job_id}',
        'chat_id': None,
        'cleanup_input': True,
    }

    try:
        print('Enqueuing job', job_id)
        await enqueue_job(job)
        print('Enqueued job', job_id)
        return 0
    except Exception:
        print('Failed to enqueue job:')
        traceback.print_exc()
        return 5


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python scripts/fetch_forward_and_enqueue_v2.py <s3_key>')
        sys.exit(1)
    key = sys.argv[1]
    code = asyncio.run(main(key))
    sys.exit(code)
