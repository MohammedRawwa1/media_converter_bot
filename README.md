# 🎬 Media Conversion Telegram Bot

**Status:** 🟢 PRODUCTION READY
**Python:** 3.12.8
**Database:** MongoDB (optional)
**Queue:** Redis (optional)
**Date:** July 30, 2026

---

## 📋 Features

- **Media Conversion:** Video/Audio format conversion, compression, resizing, trimming, merging, extraction
- **Userbot Integration:** Login via Telethon or Pyrogram for large file handling (bypasses 50MB Bot API limit)
- **Web UI:** File upload via browser with progress tracking (WebSocket + SSE)
- **Job Queue:** Redis-backed async job processing with ffmpeg workers
- **Optional Event Bus:** RabbitMQ work queue + Kafka job-event log, rolled out per job ([details](#-optional-event-bus-rabbitmq--kafka))
- **Session Persistence:** Sessions survive redeploys via MongoDB + JSON file fallback
- **Remote Storage:** Optional S3/MinIO/R2 backend for large files
- **Webhook/Polling:** Supports both webhook and long-polling modes
- **Rate Limiting:** Per-user conversion limits + Telegram API rate limiting
- **Automatic Cleanup:** Stale temp files, old job hashes, expired locks

---

## 🚀 Quick Start

```bash
# 1. Set bot token
export BOT_TOKEN="your_telegram_bot_token_here"

# 2. Run locally
python main.py

# 3. Send /start to bot in Telegram
```

### Railway Deployment

```bash
# 1. Push to GitHub
git push origin main

# 2. Create Railway Project → Deploy from GitHub repo
# 3. Set BOT_TOKEN environment variable
# 4. Deploy
```

---

## 📂 Project Structure

```
.
├── main.py                  # Bot entry point + command handlers
├── handlers.py              # EnhancedMediaHandler (all media/callback logic)
├── config.py                # Environment variable configuration
├── media_converter.py       # FFmpeg conversion logic
├── models.py                # MongoDB integration (async logging + session storage)
├── custom_thumbnail.py      # Per-user custom thumbnail commands
├── setup_directory.py       # Storage directory setup
│
├── utils/                   # 28 utility modules
│   ├── async_timeout_wrapper.py  # Async subprocess timeout
│   ├── bigfile_pipeline.py       # Large file S3 pipeline
│   ├── cache.py                  # Redis caching layer
│   ├── callbacks.py              # Callback data constants
│   ├── error_handler.py          # 11-category error system
│   ├── eventbus/                 # Optional RabbitMQ + Kafka event bus
│   │   ├── config.py             # Backend selection + rollout, degrades safely
│   │   ├── messages.py           # Versioned event envelope + job projection
│   │   ├── rabbit.py             # Job queue: durable, manual ack, retries, DLQ
│   │   └── kafka.py              # Job-event log: idempotent producer + replay
│   ├── ffmpeg_runner.py          # FFmpeg subprocess runner + progress
│   ├── file_utils.py             # File I/O helpers
│   ├── filter_utils.py           # Message filter builders
│   ├── forward_store.py          # Forward metadata storage
│   ├── job_queue.py              # Redis job queue
│   ├── job_store.py              # Job state persistence
│   ├── keyboard_utils.py         # Inline keyboard builders
│   ├── login_handler.py          # Telethon + Pyrogram login flows
│   ├── process_utils.py          # Subprocess creation helpers
│   ├── rate_limiter.py           # Conversion + API rate limiters
│   ├── redis_lock.py             # Distributed lock via Redis
│   ├── response.py               # Response helper
│   ├── route_cache.py            # Route response caching
│   ├── session_healthcheck.py    # Periodic session health verification
│   ├── storage.py                # Storage backend (local/S3/R2)
│   ├── telethon_mongo.py         # Telethon forward persistence
│   ├── telethon_session.py       # Session string persistence + client builders
│   ├── url_validation.py         # URL validation + media detection
│   ├── userbot_downloader.py     # Telethon/Pyrogram file download
│   ├── userbot_uploader.py       # Telethon/Pyrogram file upload
│   ├── user_settings.py          # Per-user settings JSON file
│   ├── web_rate_limiter.py       # Web upload rate limiter
│   └── webhook_monitor.py        # Webhook health monitoring
│
├── tasks/                   # Conversion task wrappers
│   ├── conversion_tasks.py  # Video/audio/document operations
│   ├── cleanup_tasks.py     # Automatic stale file/job cleanup
│   └── media_schema.py      # Media metadata schema
│
├── workers/
│   └── ffmpeg_worker.py     # Background ffmpeg job processor
│
├── web/                     # Web UI (Flask + FastAPI)
│   ├── webapp.py            # Flask web uploader
│   ├── ws_server.py         # WebSocket server
│   ├── ws_fastapi.py        # FastAPI WebSocket + SSE endpoints
│   ├── middleware.py         # ASGI middleware
│   ├── ffmpeg_worker.py     # Lightweight web worker
│   └── static/              # Frontend (index.html, app.js, styles.css)
│
├── fetcher/                 # Forward fetcher service
│   ├── app.py               # FastAPI forward fetcher
│   └── service.py           # Forward fetch + enqueue logic
│
├── scripts/                 # 40+ diagnostic/admin scripts
│   ├── create_pyrogram_session.py  # Generate Pyrogram session string
│   ├── create_telethon_session.py  # Generate Telethon session string
│   ├── check_sessions.py           # Check session status
│   ├── check_jobs_redis.py         # Inspect Redis job queue
│   ├── check_eventbus.py           # Inspect RabbitMQ + Kafka connectivity
│   ├── check_eventbus_integration.py  # Real-broker check: ack, retry, DLQ, events
│   ├── import_check.py             # Verify all modules import cleanly
│   └── ... (diagnostics, cleanup, migration)
│
├── tools/
│   ├── telethon_ingest.py   # Bulk forward ingestion tool
│   └── send_test_button.py  # Test inline keyboard
│
├── storage/
│   ├── input/               # Incoming media files
│   ├── output/              # Processed media files
│   ├── temp/                # Temporary processing files
│   ├── thumbnails/          # Generated thumbnails
│   ├── temp_sessions/       # Temp Telethon session files
│   └── forwards/            # Forward metadata storage
│
├── logs/                    # Bot logs (auto-created)
│
├── .github/workflows/
│   ├── lint-and-compile.yml    # Ruff lint + py_compile CI
│   └── security-scan.yml       # Bandit + pip-audit + TruffleHog
│
├── Dockerfile               # Container deployment
├── docker-compose.fetcher.yml
├── docker-compose.eventbus.yml   # Optional: local RabbitMQ + Kafka (KRaft)
├── railway.json             # Railway deployment manifest
├── Procfile                 # Process type definitions
├── runtime.txt              # Python 3.12.8
├── requirements.txt         # Production dependencies
└── requirements-dev.txt     # Dev dependencies (linting, security)
```

---

## 📟 Slash Commands

| Command | Description |
|---|---|
| `/start` | Welcome message with command list |
| `/help` | Detailed feature list |
| `/usersettings` | Open user settings |
| `/bulkmenu` | Open bulk/URL processing menu — files you send are collected automatically |
| `/cancel` | Cancel current operation or login flow |
| `/canceljob <job_id>` | Request cancellation for a queued/running job |
| `/admin add|remove|list <user_id>` | Manage allowed users (admin only) |
| `/addthumb` | Set a custom default thumbnail |
| `/delthumb` | Remove custom default thumbnail |
| `/loginstatus` | Live session health check (Telethon + Pyrogram) |
| `/session_status` | Queue depth, online users & session health (`live` arg forces a real check) |
| `/login [phone]` | Start Telethon login flow |
| `/loginpyro [phone]` | Start Pyrogram login flow (handles 2FA reliably) |
| `/logout` | Log out Telethon session (per-user) |
| `/logoutpyro` | Log out Pyrogram session (per-user) |

### Media Processing

Send any video, audio, or document file to access the full menu:
- **Video:** MP4, AVI, MOV, MKV, WebM, etc.
- **Audio:** MP3, WAV, AAC, FLAC, OGG, etc.
- **Document:** PDF, ZIP, etc.

Supported operations: Format conversion, compression, resolution change, framerate adjust, trimming, merging, audio extraction, stream extraction, screenshot, thumbnail generation, sample generation, repair, optimization, metadata editing, archive creation.

### Bulk Batches

Every video, audio, document, or photo you send is collected into a batch automatically (deduped by file id, capped at 30). Sending an **album** collects it as a group and announces it once instead of once per file. Open `/bulkmenu`, toggle the actions and quality, then press **▶️ Apply Bulk** to run the whole batch; **🗑️ Clear List** drops it and the batch clears itself after a successful apply. Two or more queued photos are combined into a single **slideshow video** (3 s per photo, letterboxed onto a 1280x720 canvas); a lone photo is encoded with the selected video action, and audio-only actions skip it. The Apply summary lists every queued file next to the job id it became.

---

## 🔧 Environment Variables

### Required
| Variable | Description |
|---|---|
| `BOT_TOKEN` | Your Telegram bot token |

### Important
| Variable | Default | Description |
|---|---|---|
| `ADMIN_USER_ID` | — | Telegram user ID for admin commands + health alerts |
| `ALLOWED_USER_IDS` | — | Comma-separated list of allowed user IDs (empty = open access) |
| `MONGO_URI` | — | MongoDB connection string (session persistence, logging) |
| `REDIS_URL` | — | Redis connection URL (job queue, caching, locks) |
| `ENABLE_USERBOT` | `false` | Enable userbot for large file upload/download |
| `API_ID` / `API_HASH` | — | Telegram API credentials (required for userbot) |
| `WEBHOOK_URL` | — | Public URL for webhook mode |
| `WEBHOOK_SECRET` | — | Telegram webhook secret token |
| `STORAGE_BACKEND` | `local` | Storage backend: `local`, `s3`, or `r2` |
| `MAX_FILE_SIZE` | `4` | Maximum file size in GB |
| `FORCE_POLLING` | `false` | Force polling mode even when WEBHOOK_URL is set |
| `MEDIA_CACHE_ENABLED` | `1` | Reuse media that already entered the pipe instead of re-downloading it |
| `MEDIA_CACHE_TTL_SECONDS` | `86400` | How long a media descriptor stays in Redis |
| `MEDIA_CACHE_BYTES_MAX_MB` | `32` | Largest media body kept verbatim in Redis (larger files reuse the storage key) |
| `PRESENCE_TTL_SECONDS` | `300` | How long a user counts as "online" after their last interaction |

### S3 / MinIO / R2
| Variable | Description |
|---|---|
| `S3_BUCKET` | Target bucket name |
| `S3_ENDPOINT` | Custom endpoint (required for MinIO/R2) |
| `S3_REGION` | Region name |
| `AWS_ACCESS_KEY_ID` | Access key |
| `AWS_SECRET_ACCESS_KEY` | Secret key |
| `PRESIGN_EXPIRES` | Presigned URL expiry in seconds (default `3600`) |

### Event bus (all optional — off by default)
| Variable | Default | Description |
|---|---|---|
| `EVENTBUS_QUEUE_BACKEND` | `redis` | Where *jobs* are queued: `redis` (existing list) or `rabbitmq` |
| `EVENTBUS_QUEUE_ROLLOUT_PERCENT` | `0` | Share of jobs sent to RabbitMQ when it is selected (`0`–`100`) |
| `EVENTBUS_EVENTS_BACKEND` | `off` | Whether *lifecycle events* also go to Kafka: `off` or `kafka` |
| `RABBITMQ_URL` | — | AMQP URL (`amqps://` for managed brokers) |
| `RABBITMQ_MAX_RETRIES` | `3` | Attempts before a job is dead-lettered |
| `RABBITMQ_PREFETCH` | `1` | Unacked messages per worker (1 matches the sequential worker) |
| `RABBITMQ_RETRY_TTL_MS` | `30000` | Base backoff; doubles per attempt, capped at 10× |
| `KAFKA_BOOTSTRAP_SERVERS` | — | Kafka brokers, comma-separated |
| `KAFKA_EVENTS_TOPIC` | `media.job.events` | Event-log topic (keyed by job id) |
| `KAFKA_EMIT_PROGRESS_EVENTS` | `false` | Also log progress events (throttled) |
| `KAFKA_PROGRESS_MIN_INTERVAL_MS` | `2000` | Progress throttle per job |
| `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SASL_MECHANISM` / `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | — | Managed-Kafka auth (SASL/SSL) |

### Full reference
See `.env.example` for the complete list of all supported environment variables.

---

## 🧪 CI/CD

| Workflow | Triggers | Checks |
|---|---|---|
| **Lint & Compile** | Every push + PR | Ruff linting, Ruff format check, `py_compile` syntax validation |
| **Security Scan** | Push/PR to `main` | Bandit static analysis, pip-audit dependency scan, Ruff linting, OWASP leak patterns, TruffleHog secret scanning |

---

## 🛠 Development

```bash
# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt   # linting + security tools

# Create a Pyrogram session (interactive)
python scripts/create_pyrogram_session.py

# Verify all modules import cleanly
python scripts/import_check.py

# Generate session string from env var
python scripts/create_pyrogram_session.py --session "$PYROGRAM_SESSION"
```

### Run Modes

```bash
# Polling mode
python main.py

# ASGI mode (webhook + health endpoint)
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4

# Web UI (Flask, standalone)
python -m web.webapp
```

---

## 🔐 Session Persistence

Session strings are persisted to **both** MongoDB and local JSON files, ensuring they survive redeploys:

1. **Env vars** (highest priority): `PYROGRAM_SESSION`, `TELETHON_SESSION`, `API_SESSION`
2. **Per-user JSON file**: `storage/temp_sessions/telethon_ingest.session.<user_id>.json`
3. **Global JSON file**: `storage/temp_sessions/telethon_ingest.session.json`
4. **MongoDB**: Stored under `sessions` collection with `{user_id, phone}` compound key
5. **File-based .session**: Telethon's native file persistence (backup)

The session healthchecker (`SessionHealthChecker`) runs every hour, verifies sessions are alive, and automatically persists working sessions to both MongoDB and JSON.

---

## 🔀 Optional event bus: RabbitMQ + Kafka

The bot can run its job queue through **RabbitMQ** and record every job's
lifecycle in **Kafka**. Both are **off by default**: with no configuration the
process behaves exactly as it did before (Redis list + progress pub/sub).

### Why two brokers — they do not own the same thing

| | RabbitMQ | Kafka |
|---|---|---|
| Owns | **job execution** | **job history** |
| Carries | one message per job to run | one event per lifecycle change (`job.queued`, `job.started`, `job.progress`, `job.completed` / `job.failed` / `job.cancelled`) |
| Mechanism | durable queue, manual ack, retries via a delay queue, dead-letter queue | idempotent producer, topic keyed by job id, replayable log |
| Replaces | the `LPUSH`/`BRPOP` list for the rollout share | Redis `PUBLISH` as the *record* of what happened (pub/sub stays the live notification path) |

The Redis queue remains in place and remains the fallback: it takes any job the
rollout does not select, and takes a job back if the broker refuses the publish.
`utils/eventbus/` is the only module that knows either broker exists; the rest of
the code calls `publish_job()` / `emit_event()` and never branches on
configuration.

### What the broker actually adds to job execution

The Redis list removes a job the moment it is handed to a worker, so a worker
killed mid-encode takes that job with it. RabbitMQ replaces that with:

- **manual acknowledgement** — the message is acked only after the handler
  returns, and redelivered if the worker dies first;
- **bounded retries through a delay queue** — a failed attempt is re-published
  with its attempt count in a header and dead-lettered back onto the work queue
  after a backoff (the broker waits, not the worker);
- **a dead-letter queue** (`media.jobs.dead`) — after `RABBITMQ_MAX_RETRIES` a
  message waits to be inspected or replayed instead of vanishing.

A consumer therefore has to be idempotent: at-least-once delivery means a job
can arrive twice. Nothing new is needed for that — the worker's input lock
(`ffmpeg:lock:*`) and job-hash dedup already make a repeated delivery a no-op.

### Reusing media that already entered the pipe

Submitting the same file again must not download it from Telegram a second
time. `utils/media_cache.py` keys on the Telegram `file_unique_id` and validates
with the **byte size**, so a repeat only reuses the earlier copy when it is
provably the same media (an id match with a different size is a miss and the
stale entry is dropped):

- **small media** (≤ `MEDIA_CACHE_BYTES_MAX_MB`) are stored verbatim in Redis, so
the repeat skips even the storage round trip;
- **large media** are stored once under a shared key
(`inputs/library/<hash>/source`) and that key is reused. Jobs fed from it are
marked `cleanup_input=False`, so the shared object survives for the next request
and is **not** deleted when one job finishes.

Because those shared inputs are intentionally not deleted per job, pair the
bucket with a **lifecycle rule** (e.g. expire `inputs/library/` after a few days)
so the library cannot grow without bound. Set `MEDIA_CACHE_ENABLED=0` to disable
reuse entirely and go back to per-job inputs.

The requeue tooling (`scripts/requeue_job.py`, `scripts/requeue_missing_jobs_once.py`)
carries the stored owner (`chat_id`/`user_id`) onto the payload it rebuilds —
without it a requeued job has nobody to deliver to. The worker's lock-collision
requeue goes back through the same pipe it arrived on (RabbitMQ or the Redis
delay set), and lands at the **back** of the queue so a requeued job never jumps
the turn of the jobs already waiting.

### Rolling it out

```bash
EVENTBUS_QUEUE_BACKEND=rabbitmq
EVENTBUS_QUEUE_ROLLOUT_PERCENT=10     # start here, watch, then raise to 100
RABBITMQ_URL=amqp://user:pass@host:5672/
EVENTBUS_EVENTS_BACKEND=kafka
KAFKA_BOOTSTRAP_SERVERS=host:9092
```

The decision is a SHA-256 bucket of the job id, so every producer (the bot,
`fetcher/`, `tools/telethon_ingest.py`) independently agrees on which queue a job
belongs to and `10` really is a tenth of the jobs. While the rollout is partial
**both** queues are drained by the same worker, and a job delivered by RabbitMQ
that hits a lock is requeued on RabbitMQ rather than being moved to the Redis
delay set. Rollback is `EVENTBUS_QUEUE_ROLLOUT_PERCENT=0`; jobs already in the
broker are still processed.

### Local stack and how to verify it

```bash
docker compose -f docker-compose.eventbus.yml up -d   # RabbitMQ + Kafka (KRaft)
python scripts/check_eventbus.py                      # config, connectivity, depths
python scripts/check_eventbus.py --publish-test       # also publish one throwaway job
python scripts/check_eventbus.py --replay <job_id>    # read a job's events back
python scripts/check_eventbus_integration.py          # 38 checks against the live brokers
pytest tests/ -q                                      # broker-free unit tests
```

`check_eventbus_integration.py` is the one that exercises delivery semantics rather
than connectivity: publisher confirms, ack-only-after-the-handler, retry with the
attempt counter going up, dead-lettering once the retries run out, a graceful stop
that keeps an unprocessed job in the queue, the worker's own consumer task, and the
lifecycle events read back from Kafka. It **purges the queues first** and refuses to
run against a non-localhost broker, so it cannot be aimed at production by accident.

`scripts/check_eventbus.py` exits non-zero when an *enabled* component is
unreachable and reports a disabled one as disabled (not as a failure). The
RabbitMQ management UI is at http://localhost:15672 once the local stack is up.

### Coverage, honestly stated

The unit tests in `tests/test_eventbus_*.py` cover the parts that can be checked
without a broker: backend selection and degradation, the rollout bucketing, the
retry/backoff/dead-letter decisions, the event envelope and its secret-safe job
projection, the progress throttle, and failure isolation (a broker error must
never surface in a job).

Broker behaviour is covered by `scripts/check_eventbus_integration.py`, run
against local RabbitMQ + Kafka (their defaults, `docker compose -f
docker-compose.eventbus.yml up -d`): 38 checks, all passing, covering publisher
confirms, manual ack, the retry/delay-queue path with its attempt counter, the
dead-letter queue, graceful shutdown, the worker's consumer task, and the event
log read back in order. What that still does **not** claim is production
behaviour: it is a single-node broker on one machine, not a HA cluster under real
load, and this repository has no measured throughput, uptime or p99 numbers.

No throughput or uptime number is claimed anywhere in this repository. What is
instrumented instead is what you would measure to make such a claim:
`eventbus_jobs_routed_total`, `eventbus_jobs_routed_fallback_total`,
`eventbus_events_published_total`, `eventbus_events_publish_failed_total`,
`eventbus_queue_retries_total` and `eventbus_queue_dead_lettered_total` on the
existing Prometheus endpoint.

---

## 📊 Architecture

```
Telegram User ←→ Bot API ←→ main.py (PTB v20+)
                                │
                    ┌───────────┼───────────┐
                    │           │           │
               handlers.py   web/      workers/
               (media +      (Flask +  (ffmpeg
                callback      FastAPI)   worker)
                logic)
                    │           │           │
                    └───────────┼───────────┘
                                │
                        ┌───────┴───────┐
                        │               │
                     MongoDB         Redis
                  (sessions,       (job queue,
                   logging)         locks, cache)
                        │
                  ┌─────┴─────┐
                  │           │
               Storage    Userbot
              (local/S3)  (Telethon/
                          Pyrogram)
```
