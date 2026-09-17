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
| `/cancelbatch <batch_id>` | Stop one bulk batch and drop its remaining files |
| `/cancelall` | Admin: cancel **every** job on both pipes, clear the cancelled batches and delete their progress bars |
| `/admin add|remove|list <user_id>` | Manage allowed users (admin only) |
| `/addthumb` | Set a custom default thumbnail |
| `/delthumb` | Remove custom default thumbnail |
| `/loginstatus` | Live session health check (Telethon + Pyrogram) |
| `/session_status` | Queue depth, memory, storage, online users & session health (`live` arg forces a real check) — comes with **🔄 Refresh** and **♻️ Restart worker** buttons |
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

#### Sequential, memory-safe processing

An Apply Bulk run does **not** process its files in parallel. Every job of the run is tagged with one `batch_id`, and a worker must hold that batch's Redis lock (`ffmpeg:batch:<id>`) before it runs one — so a 30-file batch works through a single file at a time even when several worker replicas are up. Redis holds the backlog, not the container's RAM; a job whose batch is busy is returned to the delayed set and retried, never dropped.

A second, global gate sits in front of that: conversion slots in Redis (`ffmpeg:slot:<n>`, `SET NX PX`) cap **total** concurrent ffmpeg processes at `MAX_CONCURRENT_FFMPEG` (default `1`). Because the slots are in Redis, the cap holds across replicas *and* across services — including the background worker the bot host runs — so two conversions never run at once anywhere. A worker that cannot take a slot defers its job and picks up other work.

After **every** finished job the worker runs an explicit cleanup — release the conversion slot and batch lock, drop the ffmpeg probe caches, `gc.collect()`, `malloc_trim(0)` and sweep leftover temp artifacts — and logs the RSS drop, so each file's memory is handed back before the next file starts ("finish → clean → next"). The worker keeps its side of the chat tidy with a **single** progress message per batch, edited in place for the whole run and deleted as soon as the last file is done — whether it succeeded, failed or was cancelled. While a file converts it shows that file's live percentage, so per-file progress costs no extra message:

```
📊 Processing one at a time — 3 of 12 finished
🔄 clip.mp4 — 47%
```

The worker is the only writer (it knows both the batch count and the running file), so the message never flaps. Its location lives in Redis (`ffmpeg:batch:<id>:msg`) so a worker that restarts mid-batch keeps editing the same one, and the count it aims for is the number of jobs the apply actually enqueued (`ffmpeg:batch:<id>:total`), not the number of collected files. `BATCH_PROGRESS_INTERVAL` (default `3`) paces the live refreshes.

Feeding the batch is serial too, and every wait in it is bounded. The apply fetches one file, queues it, waits for it, then fetches the next — so 30 large sources never land on disk at once. Two timers keep that from becoming a stall: `PIPELINE_DOWNLOAD_TIMEOUT_SECONDS` (default `1800`) abandons a Pyrogram download that outlives it and removes the partial file, and `BULK_FETCH_TIMEOUT_SECONDS` (default `2700`) is the backstop on the whole per-file fetch, so a file that cannot be fetched is reported per-file (`❌ could not fetch — timed out after 45m`) and the apply moves on to the next one instead of sitting on file 7 of 30 with the rest never queued. A run therefore shows exactly **two** messages: the worker's bar above, and the apply's own — which carries the batch id, the ⏹️ Stop batch button and the stage of the file it is currently on, from fetch to delivery (`⬇️ Fetching source from storage — 42%`, `🎬 Encoding — 47%`, `📤 Sending to Telegram — 90%`, `✅ delivered`). Every stage is rendered onto that one message by whoever knows it — the apply, the pipeline's download callback, and the member stage watcher (`_batch_member_text`) — and each edit re-attaches the Stop button, so a member never costs a second message and nothing is left behind in the chat for a later run to clean up. Stages come from the job hash the worker writes, which now also reports the source fetch from storage instead of leaving the hash on `queued` until ffmpeg starts.

Cancelling takes the whole batch with it. Stopping one batch writes a tombstone (`ffmpeg:batch:<id>:cancelled`) *before* it removes anything, because a worker that is still finishing a member asks about that marker before it edits or reposts the progress message — without it, the bar the user just stopped comes back. `/cancelall` does the same for every batch in one run: after it has flagged every queued, delayed and in-flight job, no batch has a live member left, so it takes each of them down — counters, membership and resume records, plus their place in the active set and their progress bars in the chat. That is what used to need `scripts/cleanup_stale_redis.py` run by hand; the script is now a preview/offline tool and says so.

The tombstone is written only while it has something to stop. A batch whose members have all reported is over — no worker is still finishing one, and no apply is still feeding it — so it is taken down without a marker, and a batch that *was* mid-flight keeps one until its last member can no longer be running (the marker is what the worker and the feeding apply both read; see `_batch_needs_tombstone`). Markers left from a cancel that raced a running member age out on their own, and a worker reporting for a batch that has been taken down (a redelivery, say) writes nothing back: the batch's state and its place in the view are both gone, and re-creating its counter would put the batch back for the next `/cancelall` to clear all over again.

A file that the pipeline already queued is counted toward the batch by the **worker** that runs that job, not by the apply — the apply only counts the job when it carries no tag of this batch (one the user already had in flight). Counting both ways used to finish the batch at half its files and take its progress message down while work was still queued.

A `WORKER_MEMORY_CEILING_BYTES` ceiling makes the worker refuse to start a conversion while the process is still above it (after a few bounded deferrals it runs anyway, so a mis-set ceiling degrades throughput rather than stalling the queue). `WORKER_RESTART_AFTER_JOB_BYTES` additionally makes the standalone worker exit for a clean container restart when it is still above the ceiling after cleanup; the worker's `restartPolicyMaxRetries` is `10` so those planned restarts cannot exhaust the budget and leave the worker down.

`/session_status` (admin) shows a **Capacity** block with this state: how many conversion slots are in use against `MAX_CONCURRENT_FFMPEG`, and the peak worker RSS against the ceiling with its remaining headroom. Workers publish their RSS to `ffmpeg:worker:rss:<host:pid>` (TTL'd, refreshed after every job and on a slow idle timer), so the figure is live even when the dashboard runs in a different service.

Two more blocks sit below it. **Memory** reports this process's RSS and its peak alongside what the container has left — the cgroup limit (`memory.max` / `memory.limit_in_bytes`) wins over the host total when both are readable, because that limit is what actually ends the process, and the percentage is flagged `⚠️` at `STATUS_MEMORY_HIGH_PERCENT` (default `80`) and `🔴` at `STATUS_MEMORY_CRITICAL_PERCENT` (default `92`). **Storage** reports the object count and total bytes in whichever backend holds them, broken down by top-level prefix so the biggest consumer is obvious:

```
🧠 Memory
• Bot process: 115.1 MB
• Container: 612.0 MB of 1.0 GB — 61.2% used
• Free: 397.1 MB

🗄 Storage
• Backend: s3 · s3save
• Used: 1.2 GB in 9 object(s)
   – inputs/: 1.2 GB in 3 object(s)
   – outputs/: 35.6 MB in 2 object(s)
```

A bucket listing costs a request per 1000 keys, so the scan stops at `STATUS_STORAGE_SCAN_MAX_OBJECTS` (default `5000`, reported as _(scan capped)_ when it does) and is cached in Redis for `STATUS_STORAGE_CACHE_SECONDS` (default `300`) — the cached line says so, and pressing **🔄 Refresh** forces a fresh scan. `STATUS_STORAGE_SCAN_TIMEOUT_SECONDS` (default `20`) bounds the whole scan, and a backend that cannot be listed or reached says so in the block rather than reporting zero.

The dashboard is served with an inline keyboard. **🔄 Refresh** re-collects everything and edits the same message in place (a refresh after `/session_status live` stays live), and admins also get **♻️ Restart worker** on its own row — the same clean-heap recycle as `/worker_restart`, confirmed in a popup so the report itself is left intact. Both are non-admin-safe: a non-admin sees only Refresh, and pressing Restart without the right gets an "Admin only." alert.

`/worker_restart` (admin) recycles the standalone worker on demand: it writes a request to `ffmpeg:worker:restart`, the worker consumes it at its next safe point — right after the job it is running, or while idle — and exits so the platform brings it back with an empty heap. Running work is never interrupted and the queue is untouched. Use it when memory does not come back down after a large file; the bot-hosted worker ignores the request, since exiting there would take the bot with it.

For Railway's 1 GB box the shipped `.env.example` is tuned to: `MAX_CONCURRENT_FFMPEG=1`, `JOB_MAX_SECONDS=3600`, `BATCH_LOCK_TTL_SECONDS` empty (it inherits `JOB_MAX_SECONDS`, so a slot can never expire mid-job), `WORKER_MEMORY_CEILING_BYTES=805306368` and `WORKER_RESTART_AFTER_JOB_BYTES=805306368` (~75% of the container). Production `numReplicas` is `1` for both the worker and the bot host.

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
| `MEDIA_REGISTRY_ENABLED` | `1` | Mirror every media descriptor into the durable MongoDB tier, so a Redis flush or restart cannot send a media back to Telegram for a second download |
| `MEDIA_REGISTRY_TTL_SECONDS` | `2592000` | How long a descriptor stays in the MongoDB media registry (30 days) |
| `PRESENCE_TTL_SECONDS` | `300` | How long a user counts as "online" after their last interaction |
| `REUSE_LOCAL_INPUT` | `1` | Keep the source on disk after uploading it, so a worker in the same container reads it instead of downloading it back out of S3 (one full copy of the media of egress saved per job) |
| `PIPELINE_SOURCE_UPLOAD` | `header` | What the big-file pipeline stores for a source: `header` keeps only its first `PIPELINE_HEADER_BYTES` as a probe reference, `full` stores the whole file after the download finishes, `stream` writes the whole file **while** it downloads (storage is the source of truth, no local copy), `local` stores nothing. In `header`/`local` the media is read over Telegram, so a large video costs no bucket egress at all. Code default is `header`; `stream` is what makes a **repeat** of a media cost no Telegram traffic at all (one shared object per media, served from the bucket once validated) |
| `PIPELINE_PROMOTE_ON_REPEAT` | `1` | On the second request for a media in `header` mode, store its whole object at the shared library key instead of only refreshing the probe header. First-time media keep costing 2 MB; media that come back stop being read over Telegram for every job. `0` restores pure header behaviour |
| `PIPELINE_HEADER_BYTES` | `2097152` | How much of a source the `header` object carries (2 MB covers MP4 `moov`, MKV `SegmentInfo` and AVI `RIFF` headers). In `stream` mode it is also how much of the stream is tapped for the ffprobe that fills the job's `source_*` metadata |
| `S3_UPLOAD_PARTS_IN_FLIGHT` | `4` | Multipart parts a streaming upload keeps in flight at once |
| `S3_UPLOAD_MAX_BUFFERED_MB` | `256` | Bytes a streaming upload may hold before it makes the producer wait (back-pressure) |
| `STORAGE_PROBE_TIMEOUT_SECONDS` | `60` | Bound on the worker's header range-GET when it has to probe storage itself (it is skipped entirely when the ingest already ffprobed the file) |

### S3 / MinIO / R2
| Variable | Description |
|---|---|
| `S3_BUCKET` | Target bucket name |
| `S3_ENDPOINT` | Custom endpoint (required for MinIO/R2) |
| `S3_REGION` | Region name |
| `AWS_ACCESS_KEY_ID` | Access key |
| `AWS_SECRET_ACCESS_KEY` | Secret key |
| `PRESIGN_EXPIRES` | Presigned URL expiry in seconds (default `3600`) |
| `S3_OUTPUTS_TTL` | How long a delivered result stays in the bucket before the hourly sweep removes it (default `86400`; `S3_INPUT_TTL`/`S3_UPLOADS_TTL`/`S3_FORWARDS_TTL` behave the same for their prefixes) |
| `S3_LIBRARY_TTL` | How long the shared one-object-per-media library (`inputs/library/`) is kept (default `2592000`, 30 days). Exempt from `S3_INPUT_TTL`, so repeats of the same media keep hitting the same object |
| `EGRESS_FREE_MULTIPLIER` | Free egress the provider grants, as a multiple of stored bytes (default `3`, which is IDrive e2's policy) |
| `EGRESS_WATCH_PERCENT` | Ratio of the allowance at which the dashboard flags it and the admin is alerted (default `80`) |
| `EGRESS_WARN_STEP_GB` | Log a warning every time this much egress accumulates in a billing cycle (default `50`) |
| `EGRESS_CHECK_INTERVAL` | Seconds between egress checks by the watchdog (default `900`) |
| `EGRESS_ALERT_MIN_INTERVAL` | Floor between two Telegram alerts, so a flapping ratio stays quiet (default `3600`) |

A result is only copied into the bucket when something remote will read it: a
job with no `chat_id` (the web uploader collects through a URL) or when
`ENABLE_LINK_SEND` hands the user a presigned GET. A result that is delivered to
Telegram from the same container is never uploaded, so it never becomes an
object somebody can later download. Thumbnails attach from local disk for the
same reason, falling back to the stored object only for a delivery that happens
in another container.

Egress is counted per billing cycle (UTC month) in Redis under
`storage:egress:<YYYY-MM>:<kind>` and shown in `/session_status` as *Egress … of …
free*, so the ratio the provider suspends on is visible before it is exceeded.
`object` counts bytes actually pulled out of the bucket; `link` counts presigned
GET URLs handed out (each fetch of one is another full copy as egress, and
happens outside this process, so it is counted as a count rather than bytes).

Those counters are per cycle and expire after `STORAGE_COUNTER_TTL_DAYS`
(default 60), so a long-lived bucket does not accumulate one key per month per
counter forever.

The dashboard only helps somebody who already suspects a problem, so a watchdog
(`utils/egress_monitor.py`) messages `ADMIN_USER_ID` when the cycle crosses
`EGRESS_WATCH_PERCENT`, and again if it escalates past 100%. Alerts are raised
once per severity per billing cycle - the announced severity is kept in Redis
under `storage:egress:alert_state`, so a restart cannot repeat an alert the admin
already has and a new month starts clean. The watchdog never raises: a metering
failure is logged and retried on the next check.

### Bounded in-memory state

Both long-lived processes keep small in-memory maps that are keyed by something a
caller controls, so each is explicitly bounded rather than evicted "when it gets
large" somewhere implicit:

| Where | Key | Bound |
|---|---|---|
| `utils/web_rate_limiter.py` | `(endpoint, client)` — the client key comes from `X-Forwarded-For`, i.e. the caller chooses it | idle buckets dropped after `WEB_RATE_LIMIT_BUCKET_TTL` (600s), hard cap `WEB_RATE_LIMIT_MAX_BUCKETS` (10000) |
| `web/webapp.py` fallback job store | one entry per fallback conversion | `WEB_JOB_STORE_TTL_SECONDS` (3600) and `WEB_JOB_STORE_MAX_ENTRIES` (200) |

The rate limiter prunes only once its map is at the cap (a full scan per request
would defeat the limiter), and `cleanup_all` calls `prune()` hourly as an
independent drain. The fallback store prunes on write, with the same hourly pass
reaching it when the web app is loaded in that process.

Other per-key maps were audited and are already bounded: `route_cache` (LRU at
1000), `presence` and the event bus's progress throttle (prune above 1000/5000),
the Telethon session cache (TTL), the WS client registries (removed on
disconnect plus a stale sweep), `handlers.active_conversions` and
`_download_cancel_flags` (try/finally), `METRICS` and `bad_callback_counts`
(fixed key sets), and `ffmpeg:delayed` (members are `zrem`-ed when promoted).

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
- **large media** are handed to the worker as the file the pipeline already
  downloaded (see `REUSE_LOCAL_INPUT`), then — if that copy is gone — fetched
  over Telegram, and only after that read from the bucket. With
  `PIPELINE_SOURCE_UPLOAD=header` (the default)
  the bucket never holds the video at all — just a 2 MB container header under
  `inputs/library/<hash>/header`, which a probe can inspect and nothing will ever
  try to encode. `PIPELINE_SOURCE_UPLOAD=full` restores the old behaviour: one
  whole object per media under `inputs/library/<hash>/source`, reused by every
  later operation on it (jobs fed from a shared key are marked
  `cleanup_input=False`, so the object survives for the next request), which is
  what a worker with no Telegram session to read it with needs.
  `PIPELINE_SOURCE_UPLOAD=stream` writes that same shared object *while* the
  download runs, which is the setting to prefer when repeats should never touch
  Telegram: the validation gate finds the object (HEAD existence + size), hands
  the job its key and skips the download, and the media leaves Telegram once,
  ever — no second pass over a finished local file either.

A media in `header` mode is **promoted on its second request**
(`PIPELINE_PROMOTE_ON_REPEAT`): that repeat re-fetches the media once and stores
it whole under the shared library key, so every later request — and every style
applied to it — is a bucket read instead of a Telegram read per job. That is the
middle ground between the two modes: media asked for once never cost more than
their 2 MB header, and only media that actually come back pay for a whole copy.

A `header` job whose Telegram copy is unreadable no longer fails outright. The
media registry is keyed on the Telegram identity the job carries, so the worker
can find a stored copy written by *any* producer — a promotion, `full`/`stream`,
or the Bot-API path — and adopts it as the source once it validates (the object
exists and its stored size agrees with the job's). Only when no such copy exists
does the job fail, and the error says so.

Every tier is **validated against storage before it is trusted** (see
`storage.stored_object_is_intact`): the object has to still exist, and when the
backend can report it, its stored size has to agree with the size Telegram just
reported. Both answers come from a HEAD, so validating a cached media costs no
egress — and a transient backend failure counts as "still there", because a
false negative here is exactly the re-download this is for. A descriptor that
fails the check is a miss: the media is downloaded and re-stored rather than
handed to a job as the wrong bytes.

The ingest leaves that evidence behind in **every** mode. `full`/`stream` store
a whole object and remember it as the reusable `input_key`. `header` — and
`local` — remember the media too, with the probe header in `header_key`
(`inputs/library/<hash>/header`) and `header_only` set: that is enough to prove
the media was already ingested and skip the redundant Pyrogram download on a
repeat (only the worker's own single read is left, plus the cached probe verdict
so the caption keeps its duration/title), while never letting a two-megabyte
header be mistaken for something to encode from.

The descriptor is kept in two tiers. Redis holds the hot copy for latency, and
the `media_registry` MongoDB collection holds the record: Redis is a cache, so
it may be flushed, evicted, or be unreachable, and a bare Redis miss used to
mean fetching a byte-identical file from Telegram again. A durable hit is
re-seeded into Redis and carries the Telegram location token (`file_id`) the
media was fetched with. `MEDIA_REGISTRY_ENABLED=0` leaves every Redis path
exactly as it was.

One object per media is what makes a repeat cheap, and three things used to
break it:

- the hourly sweep deleted everything under `inputs/`, library included, one
  TTL after the first upload. `S3_LIBRARY_TTL` (default `2592000`, 30 days) now
  governs it and `cleanup_s3_inputs()` **excludes** `inputs/library/`, so the
  object outlives the burst of operations on a media and no bucket lifecycle
  rule is needed;
- the worker pulled the whole media back out of the bucket for every job. It now
  keeps a local copy at `storage/temp/library/<hash>/source<ext>` and reuses it:
  the second and later operations read the source off disk, so those bytes leave
  the bucket **once per media** instead of once per style. The cache is a cache —
  the worker does not delete it when a job finishes, the temp sweep prunes it by
  age, and it lives in a subdirectory so the startup sweep (which only removes
  loose files) cannot race an in-flight job. Only shared library keys are cached;
  a per-job input never is.
- the worker used to decide whether a source was usable from a 2 MB ranged read
  of the bucket. A container whose `moov` atom sits at the end (an MP4 that was
  never faststart-ed) reports no duration in its first 2 MB while being perfectly
  playable, and a job whose probe said otherwise was failed as *corrupt* — which,
  in a bulk apply, looked exactly like "it finished the download and then started
  fetching the next file instead of encoding". The probe is now skipped whenever
  the ingest already ffprobed the whole file (its verdict travels on the job
  hash), and when it does run a duration-less slice is a warning, never a
  failure.

Set `MEDIA_CACHE_ENABLED=0` to disable reuse entirely and go back to per-job
inputs.

### Streaming a source into storage (`PIPELINE_SOURCE_UPLOAD=stream`)

The default (`header`) keeps a whole copy of the media out of the bucket at the
cost of needing a Telegram session on the worker's host. `stream` is the other
trade: the object in the bucket *is* the source of truth, so a worker on any
host can read it, and the media is written there by the download itself rather
than by a second pass over a finished local file.

What makes that possible is `AsyncStorageBackend.open_upload_sink()`. It hands
back an `UploadSink` — a writable target that completes to a storage key — and
`S3UploadSink` implements it as an S3 multipart upload: parts go up as they fill,
concurrently (up to `S3_UPLOAD_PARTS_IN_FLIGHT`), and the upload is *aborted* on
any failure so a half-written object is never left for a later job to mistake for
a source. Every other backend gets `BufferedUploadSink`, which stages the stream
in a temp file, so no backend is left without streaming support.

Telethon is what drives it: `client.download_media(..., file=sink)` writes each
MTProto chunk into the sink and awaits the back-pressure it returns, so a
download that outruns the bucket throttles the download instead of filling RAM
(`S3_UPLOAD_MAX_BUFFERED_MB`). **Pyrogram cannot do this** — its
`download_media` takes a path (`os.path.split`) and can only write to disk — so
this path is Telethon-only, and a failure anywhere in it falls back to the disk
download without leaving anything partial behind.

`HeadCaptureSink` taps the first `PIPELINE_HEADER_BYTES` of the stream on the way
past and writes them to a throwaway file, which the ingest ffprobes to fill the
job's `source_*` metadata. The tap never becomes the job's source: in `stream`
mode the job carries the whole-object key and no local path.

`stream` uses the same shared `inputs/library/<hash>/source` key as `full`, so
the media leaves Telegram once and every later operation on it reuses that
object.

### Thumbnail and audio metadata on delivery

Both are derived from the file the worker just encoded, and neither costs a
storage round trip:

- **thumbnails** are attached from local disk (`_local_thumb_candidate`). The
  user's own `/addthumb` cover is preferred over the worker's normalised copy, so
  what they picked is delivered untouched; the copy stored in the bucket is capped
  at `_THUMB_MAX_EDGE` (320px, the Bot API limit) and only ever downscaled. A
  retry that lands in another container still falls back to `thumb_key` in storage;
- **audio** player tags (`title`, `performer`, `duration`) are probed once in the
  worker and passed to the uploader as a **complete** dict, with the media's own
  name as the title fallback. A partial dict would be worse than none: the
  uploader only probes when `audio_meta is None`, so a stub would suppress its
  probe and lose the fields it fills in.

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
