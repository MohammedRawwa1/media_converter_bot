Railway deployment steps — Media Conversion Bot

Prerequisites
- Have a Railway account (railway.app)
- Install the Railway CLI (optional, but recommended for local development)
- Ensure environment variables are ready: BOT_TOKEN, (optional) WEBHOOK_URL, MONGODB_URI, SENTRY_DSN, ADMIN_USER_ID, ALLOWED_USER_IDS

## MongoDB Setup

Railway provides MongoDB as a database service (not a plugin). Your app’s `config.py` already handles all common MongoDB env var names (`MONGO_URI`, `MONGODB_URI`, `MONGO_URL`, `MONGODB_URL`), so it works out of the box.

### Adding MongoDB to your Railway project

**Option A — Railway Dashboard (easiest):**
1. Open your project in the Railway dashboard
2. Click **+ New** (or press `Ctrl/Cmd + K`)
3. Select **Database** → **MongoDB**
4. Railway creates the MongoDB service and auto-generates a `MONGO_URL` environment variable
5. That’s it — your app automatically picks up `MONGO_URL` via the normalization in `config.py`

**Option B — Railway CLI:**
```bash
railway add
```
Then select MongoDB from the template list.

### What gets created
- A new MongoDB service appears on your Railway project canvas
- Railway auto-generates environment variables: `MONGO_URL`, `MONGOUSER`, `MONGOPASSWORD`, `MONGOHOST`, `MONGOPORT`
- The app reads `MONGO_URL` and normalizes it to `MONGO_URI`, `MONGODB_URI`, `MONGODB_URL`, and `MONGO_URL` — all modules find it regardless of which name they check

### Connecting from local development
Railway enables a **TCP Proxy** by default for database services:
1. Open the MongoDB service in Railway dashboard
2. Go to **Settings** → **TCP Proxy** (it's enabled by default)
3. Copy the connection string shown there
4. Add it to your local `.env` file as `MONGODB_URI=<connection-string>`

### Alternative: Use MongoDB Atlas
If you prefer a managed database, use MongoDB Atlas instead:
1. Create a free cluster at mongodb.com
2. Copy your connection string (e.g., `mongodb+srv://user:pass@cluster.example/dbname`)
3. Set it as `MONGODB_URI` in your Railway service environment variables

## Service Architecture

This project runs as a monorepo on Railway with the following services:

| Service | Root Directory | Start Command | Description |
|---------|---------------|--------------|-------------|
| **Web** | `.` (project root) | `/app/start.sh` or `uvicorn main:app --host 0.0.0.0 --port $PORT` | Main ASGI web service with in-process ffmpeg worker |
| **FFmpeg Worker** | `.` (project root) | `python -u -m workers.ffmpeg_worker` | Dedicated background worker for conversion jobs |
| **Fetcher** | `.` (project root) | `python -u fetcher/service.py` | Background worker for fetching forwarded media |
| **Telethon Ingest** | `.` (project root) | `python -u tools/telethon_ingest.py` | Background worker for Telethon message ingestion |

> **📦 Single Docker Image:** All 4 services (web, ffmpeg worker, fetcher, telethon ingest) are built from the **same `./Dockerfile`**. Railway builds the identical image once per service (Docker layer caching makes subsequent builds fast). This means only **1 Docker image** powers all your services — matching what Render did with a shared Python environment.
>
> 🟢 You will see 4 separate running containers on Railway (one per service). This is correct — each service runs a different process from the same image.

### Healthchecks

All 4 services expose a `/health` endpoint that returns `{"ok": true}`. Railway polls this endpoint to verify each service is alive. The healthcheck timeout is set to 300 seconds (5 minutes) in each `railway.json`.

| Service | Healthcheck Endpoint | Port |
|---------|---------------------|------|
| **Web** | `/health` (FastAPI) | `$PORT` |
| **FFmpeg Worker** | `/health` (aiohttp) | `$PORT` |
| **Fetcher** | `/health` (Flask) | `$PORT` |
| **Telethon Ingest** | `/health` (aiohttp) | `$PORT` (also exposes `/debug/health` with optional token) |

### Deployment Exclusions

The `.railwayignore` file excludes development files from deployments:
- `.git/`, `.github/`, `.vscode/`, `.idea/` — version control & IDE
- `__pycache__/`, `.venv/`, `venv/` — virtual environments & bytecode
- `.env`, `*.session*` — secrets & session files
- `storage/`, `logs/` — runtime data (created on startup)
- `tests/`, `coverage/` — test artifacts
- `*.whl`, `*.egg-info/`, `dist/`, `build/` — build artifacts

All essential runtime files (`main.py`, `handlers.py`, `config.py`, `utils/`, `workers/`, `fetcher/`, `tools/`, `requirements.txt`, `start.sh`, `Dockerfile`, all `railway.json` files) are preserved.

### Keep-Alive Heartbeat

The web service automatically starts a keep-alive heartbeat that pings its own `/health` endpoint every 10 minutes (configurable via `KEEP_ALIVE_INTERVAL`). This prevents free-tier spin-down on platforms that idle inactive services.

The URL is resolved in this order:
1. `KEEP_ALIVE_URL` env var (explicit override)
2. `RAILWAY_PUBLIC_DOMAIN` (auto-set by Railway)
3. `WEBHOOK_URL` (webhook mode URL)

To disable the keep-alive, set `KEEP_ALIVE_DISABLED=true`.

Recommended run mode on Railway
- Use ASGI (webhook mode) with Uvicorn so Railway can route HTTP traffic to the webhook endpoint.
- The project exposes `app` (FastAPI) in `main.py` and will start the bot in background on Uvicorn startup.

Steps
1. Push code to your GitHub repo.
2. On Railway: New Project → Deploy from GitHub repo → Connect your repo.
3. Create a **Web Service** from the repo:
   - Set Root Directory to the project root (`.`)
   - Railway will auto-detect the Dockerfile and build the service
   - Set environment variable `BOT_TOKEN`
   - If you plan to use webhooks, set `WEBHOOK_URL` to `https://<your-service>.up.railway.app/telegram/webhook`
   - (Optional, recommended) set `WEBHOOK_SECRET` to a random secret string
   - The health check endpoint is `/health`
4. Create additional services (for workers) as needed:
   - **FFmpeg Worker**: Root Directory `.` → Start Command `python -u -m workers.ffmpeg_worker`
   - **Fetcher**: Root Directory `.` → Start Command `python -u fetcher/service.py`
   - **Telethon Ingest**: Root Directory `.` → Start Command `python -u tools/telethon_ingest.py`
5. Set environment variables for each service (BOT_TOKEN, etc.) in the Railway dashboard.
6. Deploy and monitor logs.

Notes
- `BOT_TOKEN` is required; app raises an error if missing.
- Polling mode (`python main.py`) works but is not recommended on Railway.
- If using `WEBHOOK_URL`, make sure Railway's service URL is correctly used and reachable from Telegram.
- **FFmpeg** is pre-installed via the Dockerfile — no additional setup needed.
- Railway automatically provides the `PORT` environment variable; the app binds to `$PORT` by default.
- For the public URL, Railway sets `RAILWAY_PUBLIC_DOMAIN` automatically.

### Railway Environment Variables

Railway provides several useful environment variables automatically:
- `PORT` — The port your application must listen on
- `RAILWAY_PUBLIC_DOMAIN` — The public URL for your service (e.g., `my-app.up.railway.app`)
- `RAILWAY_PRIVATE_DOMAIN` — Internal networking URL for service-to-service communication
- `RAILWAY_PROJECT_NAME`, `RAILWAY_SERVICE_NAME`, `RAILWAY_ENVIRONMENT_NAME` — Metadata

### Environment Variables Required

| Variable | Required | Description |
|----------|----------|-------------|
| `BOT_TOKEN` | ✅ Yes | Your Telegram bot token |
| `WEBHOOK_URL` | Optional | Public URL for webhook mode |
| `WEBHOOK_SECRET` | Optional | Secret token for webhook security |
| `MONGODB_URI` | Optional | MongoDB connection string — Railway generates `MONGO_URL` when you add MongoDB; config.py normalizes it to all common names (see MongoDB Setup below) |
| `REDIS_URL` | Optional | Redis connection URL for job queue |
| `EVENTBUS_QUEUE_BACKEND` | Optional | `redis` (default) or `rabbitmq` — where jobs are queued |
| `EVENTBUS_QUEUE_ROLLOUT_PERCENT` | Optional | `0`–`100`, share of jobs sent to RabbitMQ (default `0` = off) |
| `EVENTBUS_EVENTS_BACKEND` | Optional | `off` (default) or `kafka` — job lifecycle events |
| `RABBITMQ_URL` | Conditional | Required when `EVENTBUS_QUEUE_BACKEND=rabbitmq` (use `amqps://` with a managed broker) |
| `KAFKA_BOOTSTRAP_SERVERS` | Conditional | Required when `EVENTBUS_EVENTS_BACKEND=kafka` |
| `KAFKA_SECURITY_PROTOCOL` | Conditional | `SASL_SSL` for most managed Kafka providers |
| `KAFKA_SASL_MECHANISM` | Conditional | e.g. `SCRAM-SHA-256` |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | Conditional | Managed Kafka credentials |
| `SENTRY_DSN` | Optional | Sentry DSN for error monitoring |
| `ADMIN_USER_ID` | Optional | Telegram user ID for admin |
| `ALLOWED_USER_IDS` | Optional | Comma-separated allowed user IDs |
| `STORAGE_BACKEND` | Optional | Storage backend: `local` (default), `s3`, or `r2` |
| `S3_BUCKET` | Conditional | Required when `STORAGE_BACKEND` is `s3` or `r2` |
| `S3_ENDPOINT` | Conditional | S3-compatible endpoint URL (e.g., Cloudflare R2) |
| `API_ID` | Optional | Telegram API ID for Telethon userbot /login flow |
| `API_HASH` | Optional | Telegram API hash for Telethon userbot /login flow |
| `PYROGRAM_SESSION` | Optional | Pyrogram session string for userbot uploads |
| `ENABLE_USERBOT` | Optional | Set to `true` to enable Telethon/Pyrogram userbot for large file delivery |
| `FORCE_POLLING` | Optional | Set to `true` to force polling mode (bypass webhook) |
| `KEEP_ALIVE_URL` | Optional | Custom URL for keep-alive heartbeat (overrides auto-detection) |
| `KEEP_ALIVE_DISABLED` | Optional | Set to `true` to disable the keep-alive heartbeat |
| `KEEP_ALIVE_INTERVAL` | Optional | Keep-alive ping interval in seconds (default: `600`, range: 60–840) |
| `HTTP_POOL_SIZE` | Optional | HTTP connection pool size for Telegram Bot API (default: `50`) |

## Optional Event Bus in Production (RabbitMQ + Kafka)

See the README section [“Optional event bus”](README.md#-optional-event-bus-rabbitmq--kafka) for what each broker owns. The
production-relevant points are below.

### Adding the brokers

Two options, and the application code is identical for both:

1. **Managed instances (recommended on Railway).** RabbitMQ and Kafka are not
   first-class Railway plugins, so point the two URLs at a managed provider —
   CloudAMQP (RabbitMQ) and Confluent Cloud / Redpanda Cloud / Upstash (Kafka)
   all expose exactly the values the two variables expect (an `amqps://` URL, and
   a bootstrap list + SASL credentials). Aiven offers both from one account.
2. **Self-hosted containers.** `docker-compose.eventbus.yml` in this repository
   is the local stack; the same images run as services anywhere Docker runs. Note
   that on a free Railway plan the extra containers compete with ffmpeg for the
   RAM budget — the managed option exists for that reason.

### Setting the variables

**Set them on every service, not just the worker.** All four processes both
produce and consume: the web service enqueues from Telegram, `fetcher/`
enqueues from forwards, `telethon_ingest` enqueues from channels, and the web
service plus the worker both run a consumer. A service left with the defaults
keeps enqueueing to Redis, which is safe — the worker always drains Redis — but
its share of the rollout is silently not being exercised.

### Rollout procedure

1. Deploy with `EVENTBUS_QUEUE_BACKEND=rabbitmq` and
   `EVENTBUS_QUEUE_ROLLOUT_PERCENT=0`. Nothing moves; the RabbitMQ consumer is
   not started.
2. Raise it to `5`, then `25`, watching `eventbus_queue_retries_total`,
   `eventbus_queue_dead_lettered_total` and the `media.jobs.dead` queue depth.
3. Raise to `100` once retries and dead-letters are quiet, and only then switch
   `EVENTBUS_EVENTS_BACKEND=kafka` if the event log is wanted.
4. **Rollback is `EVENTBUS_QUEUE_ROLLOUT_PERCENT=0`** — new jobs go back to
   Redis immediately, and anything already sitting in RabbitMQ is still consumed
   because the consumer keeps running until the queue is empty (set
   `EVENTBUS_QUEUE_BACKEND=redis` only once `media.jobs.run` shows 0 ready).

### Verifying before you raise the rollout

Run both checks against a local stack before pointing production at a broker:

```bash
python scripts/check_eventbus.py              # resolved config, connectivity, depths
python scripts/check_eventbus_integration.py  # delivery semantics (destructive, localhost only)
```

The integration check purges the configured queues, so it refuses to run when
`RABBITMQ_URL` is not localhost. The equivalent against a managed broker is a
throwaway queue name (set `RABBITMQ_JOBS_QUEUE` to something like
`media.jobs.canary`) rather than `--force` on the real queue.

### Operational notes

- **Two processes acked the same job? Not a problem, but know why.** The broker
  guarantees at-least-once, so a job that is redelivered after a crash can be
  handled twice. The existing `ffmpeg:lock:*` input lock and the job-hash dedup
  make the second attempt a no-op, which is why no new idempotency layer was
  added.
- **`RABBITMQ_PREFETCH` stays at `1` by default.** The worker processes one job
  at a time; raising it hands a single worker several unacked jobs, which is a
  deliberate throughput decision rather than a free win.
- **A missing client library is not an outage.** If `aio-pika` / `aiokafka` are
  not installed, or a URL is missing, the layer logs the reason and the
  deployment keeps running on Redis, and the reason is logged on startup.
  `python scripts/check_eventbus.py` reports the resolved settings and exits
  non-zero only when an *enabled* component is unreachable.
- **Dependency image size.** `aio-pika` and `aiokafka` are two small pure-Python
  packages; the event bus adds no system packages to the image.
