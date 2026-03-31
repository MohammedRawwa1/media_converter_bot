# Unzip-bot — Analysis Report

Date: 2026-03-31

## **Overview**
- Project: `unzip-bot` (v7.x) — a Telegram bot that receives archive files or URLs, extracts their contents (supporting split archives and password-protected archives), and uploads the extracted results back to users.
- Main runtime: Python 3.10+ / Pyrogram (pyrofork fork). The bot runs as a single asynchronous Pyrogram client, schedules tasks, and persists task metadata in MongoDB (Motor).

## **System Architecture**
- **Telegram Client**: single `unzipbot_client` Pyrogram Client (see [unzipbot/__init__.py](unzipbot/__init__.py)). Handles incoming messages, callback queries and runs registered plugin modules.
- **Handlers & Modules**: primary user-facing logic lives in `unzipbot/modules` (commands, callbacks, ext_script helpers). Message routing and UI use Pyrogram message handlers and callback queries.
- **Helpers / Workers**: extraction and upload orchestration performed in `unzipbot/modules/ext_script/*` and `unzipbot/helpers/*`. Long-running shell commands (7z, unrar, tar, ffmpeg) are executed via safe-limited shell runner that applies ulimit/cpulimit.
- **Storage**: files are stored temporarily on the local filesystem under `Config.DOWNLOAD_LOCATION` and thumbnails in `Config.THUMB_LOCATION` (see [config.py](config.py)). MongoDB (Motor) stores state: users, ongoing tasks, thumbnails, counters and flags.
- **External Tools**: `7z`, `unrar` (installed via `install_unrar.sh`), `ffmpeg`, `zstd`, `tar`, `cpulimit` and system `ulimit`. Dockerfile shows containerized build and runtime environment.
- **Scheduling / Cleanup**: uses `aiocron` for periodic cleanup (`unzipbot/helpers/start.py`). Task lifecycle tracked in DB collections (ongoing, merge, cancel).

## **Runtime Flow (user extract -> upload)**
1. User sends an archive file or a URL to the bot in private chat (`unzipbot/modules/commands.py` / `extract_archive`).
2. Bot replies with extraction mode options (URL vs file, password vs no password, thumbnails, rename).
3. If URL: bot performs HTTP HEAD/GET using `aiohttp` (`download_with_progress` in [unzipbot/modules/callbacks.py](unzipbot/modules/callbacks.py)). If Telegram upload: uses Pyrogram `.download()` with progress callback.
4. Download progress updates are shown via `progress_for_pyrogram` (`unzipbot/helpers/unzip_help.py`), using `Config.CHUNK_SIZE` for chunk reads.
5. After download, extraction uses `7z`/`unrar`/`tar`/`zstd` via `run_shell_cmds` and helpers in `unzipbot/modules/ext_script/ext_helper.py`. Split archives and merged flows are supported.
6. Post-extraction: user selects files via inline keyboard (`make_keyboard`) and the bot uploads chosen files using `send_file` in `unzipbot/modules/ext_script/up_helper.py`.
7. Upload uses Pyrogram methods: `send_document`, `send_video`, `send_audio`, `send_photo` with progress callbacks (`progress_for_pyrogram` / `progress_urls`). Audio metadata loaded by `mutagen` for proper audio fields.
8. After upload, temporary files and directories are cleaned up, DB counters updated (`helpers/database.py`).

## **Upload / Download — Technologies & Implementation Details**
- Telegram transport: Pyrogram (via `pyrofork` package mentioned in `pyproject.toml`). All sends/receives, keyboard UI and callback handling rely on Pyrogram.
- HTTP downloads: `aiohttp.ClientSession` + `aiofiles.open` for streaming download to disk (`download_with_progress` and `download` in [unzipbot/modules/callbacks.py](unzipbot/modules/callbacks.py)).
- Upload progress and display: `progress_for_pyrogram` and `progress_urls` in [unzipbot/helpers/unzip_help.py](unzipbot/helpers/unzip_help.py) produce periodic edits showing percent/speed/ETA and a cancel button.
- Local filesystem: `Config.DOWNLOAD_LOCATION` (default: `unzipbot/Downloaded` path) and `THUMB_LOCATION` for thumbnails.
- Extraction: `7z`, `unrar`, `tar`, `zstd` invoked from Python via `run_shell_cmds` which wraps them with `ulimit` and `cpulimit` to protect resource usage (see [unzipbot/modules/ext_script/ext_helper.py](unzipbot/modules/ext_script/ext_helper.py)).
- Thumbnail and audio metadata: `Pillow` for thumbnails, `ffmpeg`/`ffprobe` for video metadata and thumbnail capture (`up_helper.py`), `mutagen` for audio tags (`metadata_helper.py`).
- Database: MongoDB using Motor (`motor.motor_asyncio.AsyncIOMotorClient`) for asynchronous operations. Collections track users, ongoing tasks, cancel tasks, merge tasks, thumbnails, upload modes, uploaded counts.
- Concurrency: Fully async design (async/await), pyrogram event loop; some CPU-bound shell commands delegated to subprocesses and run under limited resources.

## **Security, Platform & Operational Notes**
- Many shell calls and `ulimit` / `cpulimit` commands assume a POSIX environment (Linux). The Dockerfile and start scripts reflect Linux-based deployment. Windows is unsupported without adaptations.
- Admin-only `eval` and `exec` commands exist (`/eval`, `/exec` in [unzipbot/modules/commands.py](unzipbot/modules/commands.py)); ensure `Config.BOT_OWNER` is set to trusted administrator(s).
- The system writes temporary files under `Config.DOWNLOAD_LOCATION`, and uses a `LOCKFILE` to prevent concurrent start race conditions.
- Resource limits: Config contains CPU/RAM caps and task duration limits (see [config.py](config.py)). These are enforced partially via `ulimit` and `cpulimit` wrappers.

## **Per-file summary (brief)**
- **[config.py](config.py)**: central configuration values (env-driven): credentials, paths, CHUNK_SIZE, resource limits, MongoDB URL/DB name, TG max size, thumb and download locations.
- **[unzipbot/__init__.py](unzipbot/__init__.py)**: Pyrogram client instantiation (`unzipbot_client`), logging setup and global constants (boottime, plugins dict).
- **[unzipbot/__main__.py](unzipbot/__main__.py)**: program entry; starts the client, performs boot-time initialization: lockfile, send startup message to `LOGS_CHANNEL`, schedule cron jobs, download thumbs, and handle graceful shutdown and signals.
- **[unzipbot/helpers/start.py](unzipbot/helpers/start.py)**: boot-time tasks: download missing thumbnails, set boot times, scheduled cleanup (aiocron), and restart/expired-task handling.
- **[unzipbot/helpers/database.py](unzipbot/helpers/database.py)**: MongoDB accessors/wrappers (users, banned users, ongoing_tasks, merge_tasks, cancel_tasks, thumbnails, vip/referrals). Encapsulates insert/update/delete and simple business logic.
- **[unzipbot/helpers/unzip_help.py](unzipbot/helpers/unzip_help.py)**: progress helpers, human-readable formatting (`humanbytes`, `TimeFormatter`), `progress_for_pyrogram` used throughout for progress updates and cancel handling; common extension lists and extraction error definitions.
- **[unzipbot/modules/commands.py](unzipbot/modules/commands.py)**: message handlers for `/start`, `/help`, `/merge`, `/clean`, `/stats`, `/broadcast`, `/eval`, `/exec`, `/restart`, admin utilities, and the main `extract_archive` message handler for incoming files/URLs.
- **[unzipbot/modules/callbacks.py](unzipbot/modules/callbacks.py)**: callback query handler `unzip_cb` — core logic that drives extraction flow from user selections, includes HTTP download functions (`download`, `download_with_progress`), ZIP HTTP introspection via `unzip-http` for listing zip members, merging and extraction flows, building keyboards for file selection, and invoking upload logic post-extraction.
- **[unzipbot/modules/ext_script/ext_helper.py](unzipbot/modules/ext_script/ext_helper.py)**: shell-running utilities and extraction helpers (`__extract_with_7z_helper`, `__extract_with_unrar_helper`, `__extract_with_zstd`), `extr_files`, `split_files`, `merge_files`, and keyboard generation helpers.
- **[unzipbot/modules/ext_script/up_helper.py](unzipbot/modules/ext_script/up_helper.py)**: upload helpers: `get_size`, `send_file` (decides send_document/send_video/send_audio/send_photo), `send_url_logs`, `forward_file`, splitting/merging helpers, and utilities to format filenames and thumbnails for uploads.
- **[unzipbot/modules/ext_script/metadata_helper.py](unzipbot/modules/ext_script/metadata_helper.py)**: audio metadata extraction and conversion helpers using `mutagen` and FFmpeg for format conversions.
- **[unzipbot/modules/ext_script/custom_thumbnail.py](unzipbot/modules/ext_script/custom_thumbnail.py)**: thumbnail management: add, delete, check if thumb exists, resize using Pillow, save temporary thumbnails, and update DB pointers.
- **[unzipbot/i18n/buttons.py](unzipbot/i18n/buttons.py)**: Inline keyboard definitions (`Buttons`) using localized strings from `unzipbot/i18n/lang/en.json`.
- **[unzipbot/i18n/messages.py](unzipbot/i18n/messages.py)**: message loader & formatter class that loads localized JSON files and formats messages for send/edit operations.
- **[unzipbot/i18n/lang/en.json](unzipbot/i18n/lang/en.json)**: English text resources for buttons, callbacks, commands and other localized UI strings.

### Top-level and deployment files
- **[README.md](README.md)**: project overview, deploy instructions, and features.
- **[pyproject.toml](pyproject.toml)**: project metadata and dependencies (lists `aiohttp`, `aiofiles`, `motor`, `mutagen`, `Pillow`, `pykeyboard`, `pyrofork`, `unzip-http` etc.).
- **[Dockerfile](Dockerfile)**: multi-stage build installing runtime dependencies (`ffmpeg`, `7zip`, zstd, cpulimit), builds a venv via `uv`, and sets `start.sh` as entrypoint.
- **[start.sh](start.sh)**: runtime wrapper that exports `.env` variables and runs `python -m unzipbot`.
- **[install_unrar.sh](install_unrar.sh)**: compiles and installs `unrar` binary (used in Docker build for rar support).
- **[Procfile](Procfile)**, **[heroku.yml](heroku.yml)**, **[app.json](app.json)**: Heroku/Procfile and deployment metadata for cloud deployment.
- **[LICENSE](LICENSE)**, **[CHANGELOG.md](CHANGELOG.md)**, **[AUTHORS](AUTHORS)** and other repo docs: standard metadata.
- **[uv.lock](uv.lock)** and other CI/workflow files under `.github/` and `.deepsource.toml` used for automation.

## **Key code references**
- Download (HTTP): [unzipbot/modules/callbacks.py](unzipbot/modules/callbacks.py) — `download_with_progress` and `download` using `aiohttp`.
- Telegram downloads/uploads and progress: Pyrogram usages across `callbacks.py`, `up_helper.py` and `helpers/start.py` (use of `.download`, `.send_document`, `.send_video`, `.send_audio` with `progress` callbacks).
- Extraction runner and resource limits: `run_shell_cmds` in [unzipbot/modules/ext_script/ext_helper.py](unzipbot/modules/ext_script/ext_helper.py) — wraps commands with `ulimit` and `cpulimit`.
- DB: all database operations centralized in [unzipbot/helpers/database.py](unzipbot/helpers/database.py).

## **Recommendations & Next Steps**
- Document environment & hard requirements (ffmpeg, unrar, 7z) in README prominently.
- Consider moving shell command invocations to safer worker pool or containerized sandbox to avoid potential exploits via crafted filenames or inputs.
- Add stricter validation for filenames and user-supplied URLs. Consider sanitizing inputs persisted to disk.
- Add more unit/integration tests around the extraction runner and network flows.

---
Report generated and saved to the repository: [ANALYSIS_REPORT.md](ANALYSIS_REPORT.md)

If you want, I can (a) commit this file, (b) open it in the editor, or (c) produce a shorter summary for sharing. Which do you prefer?