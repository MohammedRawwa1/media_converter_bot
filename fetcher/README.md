Fetcher service

This lightweight Flask service accepts forward fetch requests, public URLs and returns S3 presigned posts.

Env vars

- `REDIS_URL` (required) – Redis URL used to LPUSH jobs and publish 'ffmpeg:fetch' requests.
- `UPLOAD_SECRET` (recommended) – simple shared secret (Bearer token) to protect endpoints.
- `S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` – for presign endpoint.

Run locally with Docker Compose

```bash
# build and start fetcher + redis
docker compose -f docker-compose.fetcher.yml up --build
```

Example requests

Enqueue a public URL for worker download:

```bash
curl -X POST http://localhost:8000/enqueue_from_url \
  -H "Authorization: Bearer $UPLOAD_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com/large.mp4", "filename": "large.mp4"}'
```

Request a presigned POST for S3 uploads:

```bash
curl "http://localhost:8000/presign?key=uploads/yourfile.mp4" \
  -H "Authorization: Bearer $UPLOAD_SECRET"
```

Trigger a Telethon fetch (publishes to `ffmpeg:fetch` channel):

```bash
curl -X POST http://localhost:8000/fetch_forward \
  -H "Authorization: Bearer $UPLOAD_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"forward_hash": "<forward_hash_here>"}'
```
