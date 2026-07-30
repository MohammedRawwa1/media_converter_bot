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
| `/settings` | Open user settings (aliases: `/usettings`, `/usersettings`) |
| `/bulkmenu` | Open bulk/URL processing menu |
| `/cancel` | Cancel current operation or login flow |
| `/canceljob <job_id>` | Request cancellation for a queued/running job |
| `/admin add|remove|list <user_id>` | Manage allowed users (admin only) |
| `/addthumb` | Set a custom default thumbnail |
| `/delthumb` | Remove custom default thumbnail |
| `/loginstatus` | Live session health check (Telethon + Pyrogram) |
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

### S3 / MinIO / R2
| Variable | Description |
|---|---|
| `S3_BUCKET` | Target bucket name |
| `S3_ENDPOINT` | Custom endpoint (required for MinIO/R2) |
| `S3_REGION` | Region name |
| `AWS_ACCESS_KEY_ID` | Access key |
| `AWS_SECRET_ACCESS_KEY` | Secret key |
| `PRESIGN_EXPIRES` | Presigned URL expiry in seconds (default `3600`) |

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
