# handlers.py
import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, Optional

from telegram import InputMediaPhoto, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import ContextTypes, ConversationHandler

# Try to import from local modules
try:
    from media_converter import ExtendedMediaConverter
except ImportError:
    ExtendedMediaConverter = None

try:
    from utils.keyboard_utils import MediaMenuBuilder
    from utils.job_queue import enqueue_job
    import uuid
except ImportError:
    MediaMenuBuilder = None
    try:
        enqueue_job
    except NameError:
        enqueue_job = None
    try:
        uuid
    except NameError:
        uuid = None

try:
    from utils.file_utils import AsyncFileLock, detect_filename
except ImportError:
    AsyncFileLock = None

# Import config module if available (some code references `config.<NAME>`)
try:
    import config
except Exception:
    config = None

# Import ACL helper
try:
    from config import MAX_FILE_SIZE, is_user_allowed
except Exception:

    def is_user_allowed(_):
        return True

    MAX_FILE_SIZE = 4 * 1024**3

# Optional user settings helper
try:
    from utils import user_settings
except Exception:
    user_settings = None

logger = logging.getLogger(__name__)


def _parse_time_to_seconds(tstr: str) -> float:
    """Parse time strings like HH:MM:SS(.ms), MM:SS(.ms) or plain seconds -> seconds (float)."""
    try:
        parts = tstr.strip().split(":")
        if len(parts) == 3:
            h = int(parts[0])
            m = int(parts[1])
            s = float(parts[2])
            return h * 3600 + m * 60 + s
        elif len(parts) == 2:
            m = int(parts[0])
            s = float(parts[1])
            return m * 60 + s
        else:
            return float(parts[0])
    except Exception:
        raise ValueError(f"Invalid time format: {tstr}")


def _format_seconds_to_hhmmss(sec: float) -> str:
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec % 60
    # keep millisecond precision when present
    if abs(s - int(s)) > 0:
        return f"{h:02d}:{m:02d}:{s:06.3f}"
    else:
        return f"{h:02d}:{m:02d}:{int(s):02d}"

# Optional ffmpeg-python binding (best-effort)
try:
    import ffmpeg
except Exception:
    ffmpeg = None

# Probe helper for durations
try:
    from utils.ffmpeg_runner import probe_duration
except Exception:
    probe_duration = None

# Optional MongoDB model (best-effort import)
try:
    from models import MediaConversionModel
except Exception:
    MediaConversionModel = None

# Conversation states
SELECT_TIME, SELECT_RESOLUTION, SELECT_BITRATE, MERGE_FILES, CUSTOM_INPUT = (
    range(5)
)


class EnhancedMediaHandler:
    def __init__(self, max_concurrent_conversions: int = 5):
        if ExtendedMediaConverter is None:
            raise ImportError("ExtendedMediaConverter not available")
        self.converter = ExtendedMediaConverter()
        self.user_sessions: Dict[int, Dict] = {}
        self.session_timeouts: Dict[int, asyncio.TimerHandle] = {}
        self.db_model = None  # Optional MongoDB model
        self._session_timeout_seconds = 3600  # 1 hour inactivity timeout

        # Concurrency limiter for conversions
        self.conversion_semaphore = asyncio.Semaphore(
            max_concurrent_conversions
        )
        self._max_conversions = max_concurrent_conversions
        self.active_conversions: Dict[int, str] = {}  # user_id -> task_name
        # Telemetry for malformed callbacks
        self.bad_callback_counts: Dict[str, int] = {}

        # Ensure session persistence directory exists for multi-worker setups
        self._session_store_dir = os.path.join(os.path.dirname(__file__), "storage", "temp_sessions")
        try:
            os.makedirs(self._session_store_dir, exist_ok=True)
        except Exception:
            # Best-effort; continue if cannot create
            logger.debug("Could not create session store dir: %s", self._session_store_dir)

    async def _cleanup_session(self, user_id: int):
        """Cleanup user session asynchronously."""
        if user_id not in self.user_sessions:
            return

        session = self.user_sessions[user_id]

        try:
            # Clean temp files
            if "current_file" in session:
                temp_path = session["current_file"].get("path")
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                        logger.info(f"Cleaned up temp file: {temp_path}")
                    except Exception as e:
                        logger.error(f"Failed to cleanup {temp_path}: {e}")

            # Clean merge list files
            if "merge_list" in session:
                for file_info in session["merge_list"]:
                    temp_path = file_info.get("path")
                    if temp_path and os.path.exists(temp_path):
                        try:
                            os.remove(temp_path)
                        except Exception:
                            pass
        finally:
            # Remove session
            if user_id in self.user_sessions:
                del self.user_sessions[user_id]
            if user_id in self.session_timeouts:
                del self.session_timeouts[user_id]

            logger.info(f"Cleaned up session for user {user_id}")

    def _schedule_session_cleanup(self, user_id: int):
        """Schedule automatic cleanup for session."""
        # Cancel existing timer
        if user_id in self.session_timeouts:
            self.session_timeouts[user_id].cancel()

        # Schedule new cleanup
        try:
            loop = asyncio.get_event_loop()
            handle = loop.call_later(
                self._session_timeout_seconds,
                lambda: asyncio.create_task(self._cleanup_session(user_id)),
            )
            self.session_timeouts[user_id] = handle
        except RuntimeError:
            logger.error("Failed to schedule session cleanup - no event loop")

    async def _finalize_media_group(self, user_id: int, media_group_id: str):
        """Called after a short delay to finalize a media_group (album) and add
        its items to the user's merge_list with a single confirmation message.
        """
        try:
            session = self.user_sessions.get(user_id)
            if not session:
                return
            groups = session.setdefault("media_groups", {})
            items = groups.pop(media_group_id, [])
            if not items:
                return

            # Ensure merge_list exists
            if "merge_list" not in session:
                session["merge_list"] = []
            for it in items:
                session["merge_list"].append(it)

            # Persist session
            try:
                self._persist_session(user_id)
            except Exception:
                logger.debug("Could not persist session after finalizing media group")

            # Send a confirmation to the user via direct chat if available
            # We can't access the Update here; best-effort: log the result
            logger.info("Finalized media_group %s for user %s: %d items", media_group_id, user_id, len(items))
        except Exception:
            logger.exception("Failed to finalize media_group %s for user %s", media_group_id, user_id)

    async def _watch_job_progress(self, query, job_id: str, poll_interval: float = 1.0):
        """Background task: poll Redis job hash for progress and update the callback message."""
        try:
            try:
                from utils.job_queue import get_redis
            except Exception:
                get_redis = None

            if not get_redis:
                logger.debug("_watch_job_progress disabled: get_redis not available")
                return

            try:
                r = await get_redis()
            except Exception as e:
                logger.debug("_watch_job_progress could not connect to redis: %s", e)
                return
            last_text = None
            while True:
                try:
                    data = await r.hgetall(f"ffmpeg:job:{job_id}")
                    # hgetall returns bytes keys/values when using aioredis
                    if not data:
                        await asyncio.sleep(poll_interval)
                        continue
                    # decode
                    info = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v for k, v in data.items()}
                    status = info.get("status")
                    progress = info.get("progress")
                    message = info.get("message") or ""

                    text = f"🔄 Job {job_id} — {status or 'processing'}\nProgress: {progress or '0'}%\n{message}"
                    # Build an inline keyboard with Cancel and an optional Progress (web) link
                    status_url = None
                    try:
                        web_base = os.environ.get("WEB_UPLOAD_URL") or os.environ.get("WEBAPP_URL")
                        if web_base:
                            # strip common upload suffixes if present
                            for suf in ("/upload", "/upload/", "/flask/upload", "/flask/upload/"):
                                if web_base.endswith(suf):
                                    web_base = web_base[: -len(suf)]
                                    break
                            web_base = web_base.rstrip("/")
                            status_url = f"{web_base}/status/{job_id}"
                    except Exception:
                        status_url = None

                    if status_url:
                        kb = InlineKeyboardMarkup([
                            [
                                InlineKeyboardButton("📊 Progress", url=status_url),
                                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}"),
                            ]
                        ])
                    else:
                        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}")]])
                    if text != last_text:
                        await self.safe_edit(query, text, reply_markup=kb)
                        last_text = text

                    if status in ("done", "error", "cancelled"):
                        break

                except Exception:
                    logger.debug("Error polling job hash for %s", job_id)
                await asyncio.sleep(poll_interval)

            # final fetch for output or error
            try:
                data = await r.hgetall(f"ffmpeg:job:{job_id}")
                info = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v for k, v in data.items()} if data else {}
                status = info.get("status")
                output = info.get("output")
                if status == "done" and output:
                    await self.safe_edit(query, f"✅ Job {job_id} finished. Output: {output}")
                elif status == "cancelled":
                    await self.safe_edit(query, f"⏹️ Job {job_id} was cancelled.")
                else:
                    await self.safe_edit(query, f"⚠️ Job {job_id} finished with status: {status}")
            except Exception:
                pass
            try:
                await r.close()
            except Exception:
                pass
        except Exception:
            logger.exception("_watch_job_progress failed for %s", job_id)

    # ---------- Session persistence helpers (simple JSON store) ----------
    def _session_file(self, user_id: int) -> str:
        return os.path.join(self._session_store_dir, f"session_{user_id}.json")

    def _persist_session(self, user_id: int) -> None:
        """Persist minimal session info to disk for cross-worker retrieval."""
        try:
            session = self.user_sessions.get(user_id)
            if not session:
                # remove existing file if session cleared
                path = self._session_file(user_id)
                if os.path.exists(path):
                    try:
                        os.remove(path)
                    except Exception:
                        pass
                return
            minimal = {
                "current_file": session.get("current_file"),
                "merge_list": session.get("merge_list", []),
            }
            # Write locally for fast local recovery
            try:
                with open(self._session_file(user_id), "w", encoding="utf-8") as fh:
                    json.dump(minimal, fh, ensure_ascii=False)
            except Exception:
                logger.exception("Failed to write local session file for %s", user_id)

            # Persist to MongoDB asynchronously when available (best-effort)
            try:
                if getattr(self, "db_model", None):
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            asyncio.create_task(self.db_model.save_session(user_id, minimal))
                        else:
                            try:
                                loop.run_until_complete(self.db_model.save_session(user_id, minimal))
                            except Exception:
                                # best-effort: ignore failures
                                pass
                    except Exception:
                        logger.exception("Failed scheduling DB session save for %s", user_id)
            except Exception:
                logger.debug("No db_model available to persist session for %s", user_id)
        except Exception:
            logger.exception("Failed to persist session for user %s", user_id)

    def _load_persisted_session(self, user_id: int) -> Optional[Dict]:
        """Load persisted session if available. Returns session dict or None."""
        try:
            path = self._session_file(user_id)
            if not os.path.exists(path):
                # Try loading from MongoDB when available (best-effort)
                try:
                    if getattr(self, "db_model", None):
                        try:
                            import asyncio as _asyncio, threading, queue

                            q = queue.Queue()

                            def _runner():
                                try:
                                    res = _asyncio.run(self.db_model.load_session(user_id))
                                    q.put(res)
                                except Exception:
                                    q.put(None)

                            t = threading.Thread(target=_runner, daemon=True)
                            t.start()
                            try:
                                res = q.get(timeout=2)
                            except Exception:
                                res = None
                            return res
                        except Exception:
                            return None
                except Exception:
                    return None
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            # Ensure merge_list present
            if "merge_list" not in data:
                data["merge_list"] = []
            return data
        except Exception:
            logger.exception("Failed to load persisted session for user %s", user_id)
            return None

    async def _run_with_concurrency_limit(
        self, user_id: int, task_name: str, coroutine
    ):
        """Run a conversion task with concurrency limiting."""
        async with self.conversion_semaphore:
            self.active_conversions[user_id] = task_name
            try:
                return await coroutine
            finally:
                self.active_conversions.pop(user_id, None)

    def get_active_conversions(self) -> Dict[int, str]:
        """Get all active conversions."""
        return self.active_conversions.copy()

    async def safe_edit(self, query, text, **kwargs):
        """Safely edit a callback-query message, ignoring 'Message is not modified'.

        Returns the API result or None if the edit was a no-op.
        """
        try:
            return await query.edit_message_text(text, **kwargs)
        except BadRequest as e:
            msg = str(e)
            # Log full BadRequest details for debugging
            try:
                msg_obj = getattr(query, "message", None)
                chat_obj = getattr(msg_obj, "chat", None) if msg_obj else None
                chat_id = getattr(chat_obj, "id", None) if chat_obj else None

                await self._log_bad_callback(
                    "BadRequest_edit",
                    {
                        "error": msg,
                        "callback_data": getattr(query, "data", None),
                    },
                    getattr(getattr(query, "from_user", None), "id", None),
                    chat_id,
                    getattr(msg_obj, "message_id", None),
                )
            except Exception:
                logger.exception("Failed to log BadRequest in safe_edit")

            if (
                "Message is not modified" in msg
                or "specified new message content" in msg
            ):
                logger.debug("Ignored MessageNotModified error during edit")
                return None

            # Fall back to sending a new message if editing fails for other reasons
            try:
                if getattr(query, "message", None):
                    return await query.message.reply_text(text, **kwargs)
            except Exception:
                logger.exception("Fallback reply_text failed after edit BadRequest")

            # If fallback not possible, re-raise the original exception
            raise

    async def _require_callback(self, update) -> bool:
        """Ensure the update contains a callback_query. Return True if present."""
        if getattr(update, "callback_query", None) is None:
            logger.warning("Handler invoked without callback_query")
            return False
        return True

    async def _log_bad_callback(
        self,
        reason: str,
        data,
        user_id: Optional[int] = None,
        chat_id: Optional[int] = None,
        message_id: Optional[int] = None,
    ):
        """Log malformed or unexpected callback events for later inspection.

        Appends a JSON line to `logs/bad_callbacks.log` and increments an in-memory counter.
        """
        try:
            # Increment in-memory counter
            self.bad_callback_counts[reason] = (
                self.bad_callback_counts.get(reason, 0) + 1
            )

            # Ensure logs dir exists
            log_dir = os.path.join(os.path.dirname(__file__), "logs")
            os.makedirs(log_dir, exist_ok=True)
            log_path = os.path.join(log_dir, "bad_callbacks.log")

            entry = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "reason": reason,
                "data": repr(data),
                "user_id": user_id,
                "chat_id": chat_id,
                "message_id": message_id,
            }

            with open(log_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            logger.exception("Failed to log bad callback event")

    async def _check_conversion_quota(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> bool:
        """Enforce per-user conversion rate limits if configured.

        Returns True if the user may proceed, False if they are rate-limited
        (and an informational message has been sent).
        """
        try:
            user_id = update.effective_user.id
        except Exception:
            user_id = None

        try:
            conversion_limiter = None
            if context and getattr(context, "application", None):
                conversion_limiter = context.application.bot_data.get(
                    "conversion_rate_limiter"
                )
            if conversion_limiter and user_id is not None:
                allowed, message = await conversion_limiter.can_convert(
                    str(user_id)
                )
                if not allowed:
                    try:
                        if getattr(update, "callback_query", None):
                            await self.safe_edit(
                                update.callback_query, message
                            )
                        elif getattr(update, "message", None):
                            await update.message.reply_text(message)
                    except Exception:
                        logger.debug(
                            "Failed to notify user about conversion rate limit"
                        )
                return allowed
        except Exception:
            # On any error, allow the conversion to proceed (fail-open)
            logger.debug("Conversion quota check failed, allowing conversion")
        return True

    async def _ensure_current_file_downloaded(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Ensure the session's current_file is downloaded locally. Raises Exception on failure."""
        user_id = update.effective_user.id if update and update.effective_user else None
        current_file = session.get("current_file") if session else None
        if not current_file:
            raise Exception("No file in session")

        # If already downloaded, nothing to do
        path = current_file.get("path")
        if path and os.path.exists(path):
            return

        file_id = current_file.get("id") or current_file.get("file_id")
        if not file_id:
            raise Exception("No file identifier available to download")

        # Check size against MAX_FILE_SIZE if present
        try:
            max_size = int(MAX_FILE_SIZE)
        except Exception:
            max_size = 4 * 1024**3
        size = current_file.get("size")
        if size and size > max_size:
            raise Exception(f"File too large ({size//1024//1024}MB). Max allowed: {max_size//1024//1024}MB")

        # Prepare extension and paths early so fallback download (userbot) can use them
        ext = ""
        t = current_file.get("type")
        name = current_file.get("name") or ""
        if t == "video":
            ext = ".mp4"
        elif t == "audio":
            ext = os.path.splitext(name)[1] or ".mp3"
        else:
            ext = os.path.splitext(name)[1] or ""

        input_dir = getattr(config, "INPUT_PATH", "storage/input")
        try:
            os.makedirs(input_dir, exist_ok=True)
        except Exception:
            pass
        file_path = os.path.join(input_dir, f"{user_id}_{file_id}{ext}")

        # Attempt to fetch file via Telegram API (bot). If Telegram refuses due to
        # file size or access rules, optionally try a user-account (userbot) fallback
        # if configured via env (`ENABLE_USERBOT` + API_ID/API_HASH).
        try:
            file = await context.bot.get_file(file_id)
        except Exception as e:
            logger.exception("get_file failed for %s: %s", file_id, e)
            err_text = str(e) or ""
            upload_url = os.environ.get("WEB_UPLOAD_URL") or os.environ.get("WEBAPP_URL") or "<your-server>/upload"

            # If the error indicates the file is too large for the Bot API, handle
            # it centrally (persist forward metadata / trigger web fetch / user guidance)
            # regardless of whether a userbot is configured. This ensures forwards
            # are saved to S3/R2 when configured instead of failing silently.
            if "file is too big" in err_text.lower() or "too big" in err_text.lower():
                try:
                    await self._handle_large_forward(update, current_file, err_text, upload_url)
                    return
                except Exception:
                    # If handling the large forward fails, propagate a user-friendly
                    # error so callers can inform the user.
                    raise Exception(
                        "Telegram reports the file is too big to download via the bot. "
                        f"Please either upload the file via the web uploader (POST to {upload_url}) or provide a direct public URL to the file."
                    )

            # Opt-in userbot fallback: try origin-of-forward first (if present),
            # then fall back to downloading the forwarded message as it appears
            # in the bot chat (useful when forward metadata lacks origin IDs).
            enable_userbot = os.environ.get("ENABLE_USERBOT", "").lower() in ("1", "true", "yes")
            forward = current_file.get("forward") if current_file else None
            if enable_userbot and forward and forward.get("chat_id") and forward.get("message_id"):
                try:
                    from utils.userbot_downloader import download_forward_via_userbot

                    ok = await download_forward_via_userbot(
                        forward.get("chat_id"), forward.get("message_id"), file_path
                    )
                    if ok and os.path.exists(file_path):
                        current_file["path"] = file_path
                        session["current_file"] = current_file
                        try:
                            self._persist_session(user_id)
                        except Exception:
                            logger.debug("Could not persist session after userbot download")
                        return
                except Exception:
                    logger.exception("Userbot download fallback failed (origin forward)")

            # If the origin-forward attempt wasn't possible or failed, try to
            # download the forwarded message from the bot chat itself using
            # the message id/chat id we stored at registration time (or from
            # the provided `update` object).
            if enable_userbot:
                try:
                    bot_chat = current_file.get("chat_id")
                    bot_msg = current_file.get("msg_id")
                except Exception:
                    bot_chat = None
                    bot_msg = None

                # Try to extract from the update if not present in session
                if not bot_chat or not bot_msg:
                    try:
                        if getattr(update, "message", None) and getattr(update.message, "chat", None):
                            bot_chat = getattr(update.message.chat, "id", None)
                            bot_msg = getattr(update.message, "message_id", None)
                        elif getattr(update, "callback_query", None) and getattr(update.callback_query, "message", None):
                            bot_chat = getattr(update.callback_query.message.chat, "id", None)
                            bot_msg = getattr(update.callback_query.message, "message_id", None)
                    except Exception:
                        pass

                if bot_chat and bot_msg:
                    try:
                        from utils.userbot_downloader import download_forward_via_userbot

                        ok = await download_forward_via_userbot(bot_chat, bot_msg, file_path)
                        if ok and os.path.exists(file_path):
                            current_file["path"] = file_path
                            session["current_file"] = current_file
                            try:
                                self._persist_session(user_id)
                            except Exception:
                                logger.debug("Could not persist session after userbot download (bot chat)")
                            return
                    except Exception:
                        logger.exception("Userbot download fallback failed (bot chat)")

            # Other get_file errors: re-raise
            raise

        # Download with the bot (if we reached here, get_file succeeded)
        await file.download_to_drive(file_path)

        # Attempt to detect a better filename now that the file is present
        try:
            final_name = await detect_filename(file_path, getattr(update, "message", None))
            if final_name:
                current_file["name"] = final_name
        except Exception:
            logger.debug("detect_filename failed after download")

        current_file["path"] = file_path
        session["current_file"] = current_file
        try:
            self._persist_session(user_id)
        except Exception:
            logger.debug("Could not persist session after download")

    async def _handle_large_forward(self, update: Update, current_file: Dict, err_text: str, upload_url: str):
        """Persist forward metadata, optionally auto-fetch and enqueue, or raise an instruction.

        This centralizes the previous inline logic for handling "file is too big" errors
        so handlers can call it non-blockingly and keep the download flow readable.
        """
        try:
            from utils.forward_store import save_forward_metadata, delete_forward_metadata

            metadata = {
                "chat_id": current_file.get("chat_id"),
                "message_id": current_file.get("msg_id") or current_file.get("message_id"),
                "file_id": current_file.get("id") or current_file.get("file_id"),
                "file_unique_id": current_file.get("file_unique_id"),
                "name": current_file.get("name"),
                "size": current_file.get("size"),
                "type": current_file.get("type"),
                "registered_at": datetime.utcnow().isoformat(),
            }
            fh = save_forward_metadata(metadata)
            try:
                logger.info("Saved forward metadata id=%s for file_id=%s", fh, metadata.get("file_id"))
            except Exception:
                pass

            auto_fetch = os.environ.get("AUTO_FETCH_FORWARDS", "").lower() in ("1", "true", "yes")
            web_upload_url = os.environ.get("WEB_UPLOAD_URL") or os.environ.get("WEBAPP_URL")

            if auto_fetch:
                # Prepare local paths
                try:
                    import uuid as _uuid

                    jid = str(_uuid.uuid4())
                except Exception:
                    jid = None

                input_dir = getattr(config, "INPUT_PATH", "storage/input") if config else "storage/input"
                try:
                    os.makedirs(input_dir, exist_ok=True)
                except Exception:
                    pass

                ext = os.path.splitext(metadata.get("name") or "")[1] or ".mp4"
                input_path = os.path.join(input_dir, f"{jid}{ext}") if jid else os.path.join(input_dir, f"{fh}{ext}")

                fetched = False

                # Try local userbot downloader first when enabled
                try:
                    if os.environ.get("ENABLE_USERBOT", "").lower() in ("1", "true", "yes"):
                        try:
                            from utils.userbot_downloader import download_forward_via_userbot
                        except Exception:
                            download_forward_via_userbot = None

                        if download_forward_via_userbot is not None:
                            try:
                                ok = await download_forward_via_userbot(
                                    metadata.get("chat_id"), metadata.get("message_id") or metadata.get("msg_id"), input_path, msg_date=metadata.get("registered_at") or metadata.get("created_at"), file_unique_id=metadata.get("file_unique_id")
                                )
                                if ok and os.path.exists(input_path):
                                    fetched = True
                            except Exception:
                                logger.exception("auto-fetch via userbot failed for %s", fh)
                except Exception:
                    logger.exception("auto-fetch userbot path error for %s", fh)

                # If local fetch failed and web upload endpoint is configured, ask webapp to fetch
                if not fetched and web_upload_url:
                    try:
                        def _post_fetch():
                            try:
                                import requests
                            except Exception:
                                return None
                            headers = {}
                            upload_secret = os.environ.get("UPLOAD_SECRET")
                            if upload_secret:
                                headers["X-Upload-Token"] = upload_secret
                            try:
                                resp = requests.post(web_upload_url, data={"forward_hash": fh}, headers=headers, timeout=60)
                                return resp
                            except Exception:
                                return None

                        resp = await asyncio.get_event_loop().run_in_executor(None, _post_fetch)
                        if resp is not None and getattr(resp, "status_code", None) == 200:
                            try:
                                j = resp.json()
                                queued_job = j.get("job_id")
                                # notify user
                                try:
                                    if getattr(update, "callback_query", None):
                                        await self.safe_edit(update.callback_query, f"✅ Server fetched and queued conversion (job {queued_job}).")
                                    elif getattr(update, "message", None):
                                        await update.message.reply_text(f"✅ Server fetched and queued conversion (job {queued_job}).")
                                except Exception:
                                    pass

                                # delete saved forward metadata to avoid duplicates
                                try:
                                    delete_forward_metadata(fh)
                                except Exception:
                                    pass
                                return
                            except Exception:
                                logger.exception("Failed to parse webapp enqueue response for %s", fh)
                    except Exception:
                        logger.exception("Webapp fetch request failed for %s", fh)

                # If we successfully fetched locally, enqueue the job directly
                if fetched:
                    try:
                        import uuid as _uuid

                        job_id = str(_uuid.uuid4())
                        output_dir = getattr(config, "OUTPUT_PATH", "storage/output") if config else "storage/output"
                        try:
                            os.makedirs(output_dir, exist_ok=True)
                        except Exception:
                            pass
                        base_name = os.path.splitext(metadata.get("name") or os.path.basename(input_path))[0]
                        output_path = os.path.join(output_dir, f"{base_name}_{job_id}.mp4")
                        job = {
                            "job_id": job_id,
                            "input_path": input_path,
                            "output_path": output_path,
                            "original_filename": metadata.get("name") or os.path.basename(input_path),
                            "output_filename": os.path.basename(output_path),
                            "ffmpeg_args": ["-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
                            "progress_channel": f"ffmpeg:progress:{job_id}",
                            "chat_id": update.effective_chat.id if update and getattr(update, 'effective_chat', None) else None,
                            "cleanup_input": True,
                            "cleanup_output": False,
                        }
                        try:
                            from utils.job_queue import enqueue_job as _enqueue

                            await _enqueue(job)
                            # notify user (prefer editing the callback message when available)
                            try:
                                q = getattr(update, "callback_query", None)
                                if q is not None:
                                    await self.safe_edit(q, f"✅ Fetched forwarded media and queued conversion (job {job_id}).")
                                    try:
                                        asyncio.create_task(self._watch_job_progress(q, job_id))
                                    except Exception:
                                        pass
                                else:
                                    # fallback to replying in chat when no callback_query
                                    if getattr(update, "message", None):
                                        try:
                                            await update.message.reply_text(f"✅ Fetched forwarded media and queued conversion (job {job_id}).")
                                        except Exception:
                                            pass
                            except Exception:
                                logger.exception("Failed to notify user after enqueue for %s", fh)
                            # cleanup saved forward metadata
                            try:
                                delete_forward_metadata(fh)
                            except Exception:
                                pass
                            return
                        except Exception:
                            logger.exception("Failed to enqueue job after fetch for %s", fh)
                    except Exception:
                        logger.exception("Failed to create job after fetch for %s", fh)

            # Fallback: return instruction to user with forward-hash and web upload link
            raise Exception(
                "Telegram reports the file is too big to download via the bot. "
                f"You can either upload the file via the web uploader, or use this forward-hash to let the server fetch it via an opt-in userbot: {fh} -- visit {upload_url}?forward_hash={fh}"
            )
        except Exception:
            # Re-raise to allow caller to provide a simpler fallback message
            raise

    async def show_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Display and start interactive settings flow for the user."""
        user_id = update.effective_user.id
        if user_settings is None:
            await update.message.reply_text("⚠️ Settings not available (missing module).")
            return

        # Build a two-page settings keyboard with toggle switches
        s = user_settings.get_user_settings(user_id)

        def bool_label(k):
            return "On" if s.get(k) else "Off"

        page = 1
        # If called via callback with page param, the caller will handle; default to page 1
        # Build text and keyboard to match the requested control panel style
        text = "⚙️ <b>Config Bot Settings</b>\n\n"
        text += f"• Bulk Mode : {bool_label('bulk_mode')}\n"
        text += f"• Thumbnail : {'Yes' if s.get('use_custom_thumbnail') else 'No'}\n"
        text += f"• Rename File : {'Yes' if s.get('prefix') or s.get('suffix') else 'No'}\n"

        kb_page1 = [
            [InlineKeyboardButton(f"Bulk Mode : { 'On' if s.get('bulk_mode') else 'Off' }", callback_data="toggle_bulk_mode")],
            [InlineKeyboardButton(f"Thumbnail : { 'Yes' if s.get('use_custom_thumbnail') else 'No' }", callback_data="settings_page:2")],
            [InlineKeyboardButton(f"Rename File : { 'Yes' if s.get('prefix') or s.get('suffix') else 'No' }", callback_data="video_renamer")],
            [InlineKeyboardButton("Upload as Audio", callback_data="menu_audio")],
            [InlineKeyboardButton("Upload as Video", callback_data="menu_video")],
            [InlineKeyboardButton("Stream Mapper", callback_data="menu_advanced")],
            [InlineKeyboardButton("Video Metadata", callback_data="full_info")],
            [InlineKeyboardButton("Mp3 Tag Setting", callback_data="mp3_tag_editor")],
            [InlineKeyboardButton("Audio Settings", callback_data="menu_audio")],
            [InlineKeyboardButton("Reset Settings", callback_data="reset_settings")],
            [InlineKeyboardButton("Close Settings", callback_data="menu_main")],
        ]

        await update.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb_page1))
        context.user_data.clear()
        context.user_data["settings_page"] = 1

    async def show_bulk_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show the bulk-mode action menu (either as reply or edit)."""
        user_id = update.effective_user.id if update and update.effective_user else None
        s = user_settings.get_user_settings(user_id) if user_settings else {}
        status = "On" if s.get("bulk_mode") else "Off"
        text = f"📦 <b>Bulk Mode Actions</b>\n\nCurrent Status >> Bulk Mode : {status}\n\nPlease select your preferred action below 👇"
        # Build keyboard defensively. MediaMenuBuilder may be missing or raise,
        # so capture failures and still show a helpful message.
        kb = None
        try:
            if MediaMenuBuilder and hasattr(MediaMenuBuilder, "get_bulk_menu"):
                try:
                    kb = MediaMenuBuilder.get_bulk_menu(s)
                except Exception:
                    logger.exception("MediaMenuBuilder.get_bulk_menu() raised an exception")
                    kb = None
        except Exception:
            # If the import resolved to None or something unexpected, continue
            kb = None

        try:
            if getattr(update, "callback_query", None):
                if kb:
                    await self.safe_edit(update.callback_query, text, reply_markup=kb, parse_mode="HTML")
                else:
                    await self.safe_edit(update.callback_query, text, parse_mode="HTML")
            elif getattr(update, "message", None):
                if kb:
                    await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")
                else:
                    await update.message.reply_text(text, parse_mode="HTML")
            else:
                logger.warning("show_bulk_menu called without message or callback")
                return
        except Exception as e:
            logger.exception("Failed to show bulk menu: %s", e)
            # Try a resilient fallback: notify the user directly via any available channel
            try:
                chat_id = None
                if getattr(update, "callback_query", None):
                    try:
                        await update.callback_query.answer()
                    except Exception:
                        pass
                    if getattr(update.callback_query, "message", None) and getattr(update.callback_query.message, "chat", None):
                        chat_id = update.callback_query.message.chat.id
                elif getattr(update, "message", None) and getattr(update.message, "chat", None):
                    chat_id = update.message.chat.id

                # Prefer sending through context.bot if available
                if chat_id and getattr(context, "bot", None):
                    try:
                        await context.bot.send_message(chat_id=chat_id, text="⚠️ Failed to open bulk menu.")
                    except Exception:
                        logger.exception("Fallback send_message failed for bulk menu")
                else:
                    # Try to message the user directly
                    try:
                        if getattr(update, "effective_user", None) and getattr(context, "bot", None):
                            await context.bot.send_message(chat_id=update.effective_user.id, text="⚠️ Failed to open bulk menu.")
                    except Exception:
                        logger.exception("Secondary fallback for bulk menu failed")
            except Exception:
                logger.exception("Secondary fallback for bulk menu failed")

    async def bulk_url_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enqueue one or more URLs provided as command arguments for processing."""
        user_id = update.effective_user.id
        if user_settings is None:
            await update.message.reply_text("⚠️ Settings not available (missing module).")
            return

        args = context.args if hasattr(context, "args") else []
        if not args:
            await update.message.reply_text("Usage: /bulk_url <url1> [url2 ...]\nYou can also paste multiple URLs in a message.")
            return

        enqueued = 0
        for url in args:
            if not isinstance(url, str) or not url.startswith("http"):
                continue
            job_id = str(uuid.uuid4())
            job = {
                "job_id": job_id,
                "source_url": url,
                "progress_channel": f"ffmpeg:progress:{job_id}",
                "chat_id": update.effective_chat.id if update and update.effective_chat else None,
                "cleanup_input": True,
            }
            try:
                await enqueue_job(job)
                enqueued += 1
            except Exception:
                logger.exception("Failed to enqueue bulk URL %s", url)

        await update.message.reply_text(f"✅ Enqueued {enqueued} URL(s) for processing.")

    async def convert_video_format(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: Dict,
        target_format: str,
    ):
        """Convert video to different format."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")
        user_id = update.effective_user.id

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        await self.safe_edit(query, f"🎬 Queuing conversion to {target_format.upper()}...")

        # Ensure file is available locally (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        # Enqueue conversion job to Redis so a worker handles heavy lifting
        input_path = current_file["path"]
        output_ext = f".{target_format}"
        output_path = f"storage/output/{current_file['id']}_converted{output_ext}"
        job_id = str(uuid.uuid4())
        job = {
            "job_id": job_id,
            "input_path": input_path,
            "output_path": output_path,
            "ffmpeg_args": None,  # let worker infer from extension or use default
            "progress_channel": f"ffmpeg:progress:{job_id}",
            "chat_id": update.effective_chat.id if update and update.effective_chat else None,
            "caption": f"Conversion to {target_format.upper()} finished",
            "cleanup_input": True,
            "cleanup_output": False,
        }

        try:
            await enqueue_job(job)
        except Exception as e:
            logger.exception("Failed to enqueue job")
            await self.safe_edit(query, "❌ Failed to queue conversion.")
            return

        # Inform user job queued and provide a cancel button
        try:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}")]])
            await self.safe_edit(query, f"✅ Job queued (ID: {job_id}). I'll send the file when ready.", reply_markup=kb)
            try:
                asyncio.create_task(self._watch_job_progress(query, job_id))
            except Exception:
                pass
        except Exception:
            await self.safe_edit(query, f"✅ Job queued (ID: {job_id}). I'll send the file when ready.")
        return

    async def handle_media_message(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """Main entry point for media messages."""
        # Log incoming update for debugging dispatch issues
        try:
            user_id = update.effective_user.id
        except Exception:
            user_id = None

        try:
            update_id = getattr(update, "update_id", None)
            msg_id = getattr(
                getattr(update, "message", None), "message_id", None
            )
            has_video = bool(
                getattr(getattr(update, "message", None), "video", None)
            )
            has_document = bool(
                getattr(getattr(update, "message", None), "document", None)
            )
            has_audio = bool(
                getattr(getattr(update, "message", None), "audio", None)
            )
            text_preview = (
                getattr(getattr(update, "message", None), "text", None) or ""
            )[:200]

            fmt = (
                "Incoming message update: user_id=%s update_id=%s msg_id=%s "
                "has_video=%s has_document=%s has_audio=%s text=%s"
            )
            logger.info(
                fmt,
                user_id,
                update_id,
                msg_id,
                has_video,
                has_document,
                has_audio,
                text_preview,
            )
        except Exception:
            logger.exception("Failed to log incoming message update")
        # Enforce access control for private bots
        try:
            if not is_user_allowed(user_id):
                await update.message.reply_text("Access denied. This bot is private.")
                return
        except Exception:
            # If ACL check fails for any reason, default to deny-safe
            try:
                await update.message.reply_text(
                    "Access denied. (ACL check failed)"
                )
            except Exception:
                pass
            return

        # Initialize user session
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = {
                "files": {},
                "current_file": None,
                "merge_list": [],
                "processing": False,
            }

        session = self.user_sessions[user_id]

        # Schedule session cleanup (resets timer on each interaction)
        self._schedule_session_cleanup(user_id)

        # Respect Telegram API rate limits if a limiter is provided in bot_data
        try:
            api_limiter = None
            if context and getattr(context, "bot_data", None) is not None:
                api_limiter = context.bot_data.get("api_rate_limiter")
            if api_limiter and user_id is not None:
                try:
                    await api_limiter.wait_if_needed(str(user_id))
                except Exception:
                    # If rate limiter fails, continue but log
                    logger.debug("API rate limiter wait failed or was skipped")
        except Exception:
            pass

        # If user is in settings flow and sends a photo, treat as thumbnail upload
        try:
            if getattr(context, "user_data", {}).get("awaiting_settings") and getattr(update.message, "photo", None):
                photos = update.message.photo
                if photos:
                    # choose largest
                    file_obj = photos[-1]
                    await update.message.reply_text("📥 Downloading thumbnail photo and saving as default...")
                    file = await context.bot.get_file(file_obj.file_id)
                    thumb_dir = config.THUMBNAIL_PATH if hasattr(config, "THUMBNAIL_PATH") else "storage/thumbnails"
                    try:
                        os.makedirs(thumb_dir, exist_ok=True)
                    except Exception:
                        pass
                    thumb_path = os.path.join(thumb_dir, f"{user_id}_{file_obj.file_id}.jpg")
                    await file.download_to_drive(thumb_path)
                    if user_settings:
                        user_settings.set_user_setting(user_id, "default_thumbnail", thumb_path)
                        user_settings.set_user_setting(user_id, "save_thumbnail", True)
                    await update.message.reply_text("✅ Default thumbnail saved.")
                    # clear awaiting flag
                    for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
                    return
        except Exception:
            logger.exception("Failed to handle thumbnail photo upload")

        # If user is providing mp3 tag JSON while in mp3 tag editor flow
        try:
            if getattr(context, "user_data", {}).get("awaiting_mp3_tags") and getattr(update.message, "text", None):
                text = update.message.text.strip()
                try:
                    import json as _json

                    tags = _json.loads(text)
                except Exception:
                    await update.message.reply_text("❌ Invalid JSON. Send a JSON object with tag keys and values.")
                    return

                # Apply tags to current file if available
                session = self.user_sessions.get(update.effective_user.id, {})
                current_file = session.get("current_file") if session else None
                if not current_file or current_file.get("type") != "audio":
                    await update.message.reply_text("❌ No audio file selected to apply tags.")
                    # clear awaiting flag
                    context.user_data.pop("awaiting_mp3_tags", None)
                    return

                input_path = current_file.get("path")
                output_path = input_path + ".tagged" + os.path.splitext(input_path)[1]
                try:
                    ok = await self.converter.edit_metadata(input_path, output_path, tags)
                    if ok:
                        await update.message.reply_text("✅ Tags applied. I'll replace the current file with the tagged version.")
                        # replace current file path
                        current_file["path"] = output_path
                    else:
                        await update.message.reply_text("❌ Failed to apply tags.")
                except Exception:
                    logger.exception("Failed to apply mp3 tags")
                    await update.message.reply_text("❌ Error while applying tags.")

                # clear awaiting flag
                context.user_data.pop("awaiting_mp3_tags", None)
                return
        except Exception:
            logger.exception("Failed to handle awaiting_mp3_tags message")

        # If message contains a photo (normal incoming photo, not settings thumbnail),
        # save it to storage and add to the user's merge_list so multiple pasted
        # photos are collected automatically.
        try:
            if getattr(update.message, "photo", None) and not getattr(context, "user_data", {}).get("awaiting_settings"):
                photos = update.message.photo
                if photos:
                    # choose largest size variant
                    file_obj = photos[-1]
                    file = await context.bot.get_file(file_obj.file_id)
                    input_dir = getattr(config, "INPUT_PATH", "storage/input")
                    try:
                        os.makedirs(input_dir, exist_ok=True)
                    except Exception:
                        pass
                    photo_path = os.path.join(input_dir, f"{user_id}_{file_obj.file_id}.jpg")
                    await file.download_to_drive(photo_path)

                    # If part of an album (media_group_id), collect into temporary group
                    mgid = getattr(update.message, "media_group_id", None)
                    if mgid:
                        groups = session.setdefault("media_groups", {})
                        lst = groups.setdefault(mgid, [])
                        lst.append({"path": photo_path, "type": "photo"})
                        # schedule a finalize in 1s if not scheduled
                        timers = session.setdefault("media_group_timers", {})
                        if mgid not in timers:
                            try:
                                loop = asyncio.get_event_loop()
                                handle = loop.call_later(1.0, lambda: asyncio.create_task(self._finalize_media_group(user_id, mgid)))
                                timers[mgid] = handle
                            except Exception:
                                # best-effort: finalize immediately
                                await self._finalize_media_group(user_id, mgid)
                        # reply lightly that album item saved (silent)
                        await update.message.reply_text(f"➕ Photo added to album buffer (media_group).")
                        return

                    # Non-album single photo: append directly
                    if "merge_list" not in session:
                        session["merge_list"] = []
                    session["merge_list"].append({"path": photo_path, "type": "photo"})
                    try:
                        self._persist_session(user_id)
                    except Exception:
                        logger.debug("Could not persist session after photo download")
                    await update.message.reply_text(f"✅ Photo saved to merge list. Total items: {len(session['merge_list'])}")
                    return
        except Exception:
            logger.exception("Failed to auto-handle incoming photo")

        # Check if message has video
        if update.message.video:
            await self.handle_video(update, context, session)
        elif update.message.document:
            await self.handle_document(update, context, session)
        elif update.message.audio:
            await self.handle_audio(update, context, session)
        else:
            # Detect URLs in text and support bulk URL conversion
            text = getattr(update.message, "text", "") or ""
            import re

            urls = re.findall(r"(https?://\S+)", text)
            if urls:
                queued = 0
                for url in urls:
                    job_id = str(uuid.uuid4())
                    output_path = f"storage/output/{job_id}.mp4"
                    job = {
                        "job_id": job_id,
                        "source_url": url,
                        "output_path": output_path,
                        "ffmpeg_args": ["-c:v", "libx264", "-preset", "fast", "-crf", "23", "-c:a", "aac", "-b:a", "128k"],
                        "progress_channel": f"ffmpeg:progress:{job_id}",
                        "chat_id": update.effective_chat.id if update and update.effective_chat else None,
                        "caption": f"✅ Converted from URL: {url}",
                        "cleanup_input": True,
                        "cleanup_output": False,
                    }
                    try:
                        await enqueue_job(job)
                        queued += 1
                    except Exception:
                        logger.exception("Failed to enqueue URL job: %s", url)
                await update.message.reply_text(f"✅ Queued {queued} URL job(s).")
                return

            await update.message.reply_text(
                "Please send a video, audio, or document file. You can also paste one or more URLs (http/https) to enqueue conversions."
            )

    async def handle_video(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Handle incoming video files."""
        video = update.message.video
        user_id = update.effective_user.id

        # Check file size (configurable)
        try:
            max_size = int(MAX_FILE_SIZE)
        except Exception:
            max_size = 4 * 1024**3
        if video.file_size > max_size:
            await update.message.reply_text(
                "❌ File too large (max 4GB).\n"
                "For larger files, use the /upload command."
            )
            return

        # Register file lazily (do not download yet). We'll download on-demand
        file_id = video.file_id
        ext = ".mp4"
        default_name = f"{user_id}_{file_id}{ext}"
        final_name = default_name
        thumb = None
        try:
            if user_settings:
                s = user_settings.get_user_settings(user_id)
                prefix = s.get("prefix") or ""
                suffix = s.get("suffix") or ""
                final_name = f"{prefix}{final_name}{suffix}"
                if s.get("save_thumbnail") and s.get("default_thumbnail"):
                    thumb = s.get("default_thumbnail")
        except Exception:
            logger.exception("Failed to apply user settings to video name")

        # Capture forward metadata when available (useful for userbot fallback)
        forward_info = None
        try:
            fch = getattr(update.message, "forward_from_chat", None)
            f_msg_id = getattr(update.message, "forward_from_message_id", None)
            if fch or f_msg_id:
                tmp = {}
                if fch:
                    tmp["chat_id"] = getattr(fch, "id", None) or getattr(fch, "username", None)
                if f_msg_id:
                    tmp["message_id"] = f_msg_id
                forward_info = tmp
        except Exception:
            forward_info = None

        # capture message date and file unique id for better userbot fallback
        msg_date = None
        try:
            if getattr(update, "message", None) and getattr(update.message, "date", None):
                msg_date = update.message.date.isoformat()
        except Exception:
            msg_date = None

        file_unique_id = getattr(video, "file_unique_id", None)

        session["current_file"] = {
            "path": None,
            "type": "video",
            "id": file_id,
            "size": video.file_size,
            "name": final_name,
            "thumbnail": thumb,
            "forward": forward_info,
            "chat_id": getattr(update.message, "chat", None) and getattr(update.message.chat, "id", None),
            "msg_id": getattr(update.message, "message_id", None),
            "msg_date": msg_date,
            "file_unique_id": file_unique_id,
        }

        try:
            self._persist_session(user_id)
        except Exception:
            logger.debug("Could not persist session after registering video")

        # Log to MongoDB if needed
        await self.log_media_to_db(user_id, session["current_file"])

        # Show main menu immediately (lazy download)
        await update.message.reply_text(
            f"✅ Video registered!\n"
            f"📦 Size: {video.file_size // 1024 // 1024} MB\n"
            f"Choose an action:",
            reply_markup=MediaMenuBuilder.get_main_menu("video"),
        )

    async def handle_audio(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Handle incoming audio files."""
        audio = update.message.audio
        user_id = update.effective_user.id

        # Register audio lazily (do not download yet).
        ext = ".mp3"
        if audio.mime_type:
            ext_map = {
                "audio/mpeg": ".mp3",
                "audio/wav": ".wav",
                "audio/x-wav": ".wav",
                "audio/aac": ".aac",
                "audio/flac": ".flac",
                "audio/ogg": ".ogg",
            }
            ext = ext_map.get(audio.mime_type, ".mp3")

        default_name = audio.title or f"{user_id}_{audio.file_id}{ext}"
        final_name = default_name
        thumb = None
        try:
            if user_settings:
                s = user_settings.get_user_settings(user_id)
                for w in s.get("words_remove") or []:
                    final_name = final_name.replace(w, "")
                final_name = final_name.strip()
                if not os.path.splitext(final_name)[1]:
                    final_name += ext
                final_name = f"{s.get('prefix') or ''}{final_name}{s.get('suffix') or ''}"
                if s.get("save_thumbnail") and s.get("default_thumbnail"):
                    thumb = s.get("default_thumbnail")
        except Exception:
            logger.exception("Failed to apply user settings to audio name")

        # Capture forward metadata when available (useful for userbot fallback)
        forward_info = None
        try:
            fch = getattr(update.message, "forward_from_chat", None)
            f_msg_id = getattr(update.message, "forward_from_message_id", None)
            if fch or f_msg_id:
                tmp = {}
                if fch:
                    tmp["chat_id"] = getattr(fch, "id", None) or getattr(fch, "username", None)
                if f_msg_id:
                    tmp["message_id"] = f_msg_id
                forward_info = tmp
        except Exception:
            forward_info = None

        msg_date = None
        try:
            if getattr(update, "message", None) and getattr(update.message, "date", None):
                msg_date = update.message.date.isoformat()
        except Exception:
            msg_date = None

        file_unique_id = getattr(audio, "file_unique_id", None)

        session["current_file"] = {
            "path": None,
            "type": "audio",
            "id": audio.file_id,
            "size": audio.file_size,
            "name": final_name,
            "thumbnail": thumb,
            "forward": forward_info,
            "chat_id": getattr(update.message, "chat", None) and getattr(update.message.chat, "id", None),
            "msg_id": getattr(update.message, "message_id", None),
            "msg_date": msg_date,
            "file_unique_id": file_unique_id,
        }

        await update.message.reply_text(
            f"✅ Audio registered!\n"
            f"🎵 {audio.title or 'Unknown title'}\n"
            f"Choose an action:",
            reply_markup=MediaMenuBuilder.get_main_menu("audio"),
        )

    async def handle_document(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Handle document files (could be video/audio)."""
        document = update.message.document
        user_id = update.effective_user.id

        # Check file extension
        file_name = document.file_name or f"file_{document.file_id}"
        file_ext = os.path.splitext(file_name)[1].lower()

        # If the user was asked to send a subtitle file, handle specially
        awaiting_sub = context.user_data.pop("awaiting_subtitle_file", False)
        awaiting_burn = context.user_data.pop("awaiting_burn_subtitle", False)

        subtitle_exts = {".srt", ".ass", ".vtt"}
        if (awaiting_sub or awaiting_burn) and file_ext in subtitle_exts:
            await update.message.reply_text("📥 Downloading subtitle file...")
            file = await context.bot.get_file(document.file_id)
            subtitle_path = f"storage/input/{user_id}_{document.file_id}{file_ext}"
            await file.download_to_drive(subtitle_path)

            # Ensure we have a current video for burning/adding
            current = session.get("current_file")
            if not current or current.get("type") != "video":
                await update.message.reply_text("❌ No video available in session to apply subtitles.")
                return

            video_path = current["path"]
            out_path = f"storage/output/{user_id}_subtitled_{os.path.basename(video_path)}"

            if awaiting_burn:
                await update.message.reply_text("🔧 Burning subtitles into video (this may take a while)...")
                ok = await self.converter.burn_subtitles(video_path, subtitle_path, out_path)
            else:
                await update.message.reply_text("🔧 Adding subtitles as a separate stream (soft subtitles)...")
                ok = await self.converter.add_subtitles(video_path, subtitle_path, out_path)

            if ok and os.path.exists(out_path):
                await update.message.reply_text("✅ Subtitles applied. Sending file...")
                try:
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=open(out_path, "rb"))
                except Exception:
                    await update.message.reply_text("⚠️ Failed to send file; try downloading from the server.")
            else:
                await update.message.reply_text("❌ Failed to apply subtitles. See logs for details.")

            return

        # Determine file type
        if file_ext in self.converter.supported_formats["video"]:
            file_type = "video"
        elif file_ext in self.converter.supported_formats["audio"]:
            file_type = "audio"
        else:
            await update.message.reply_text(
                f"❌ Unsupported file format: {file_ext}\n"
                f"Supported formats:\n"
                f"Video: {', '.join(self.converter.supported_formats['video'][:5])}\n"
                f"Audio: {', '.join(self.converter.supported_formats['audio'][:5])}"
            )
            return

        # Check file size (if provided) to avoid calling get_file on huge files
        try:
            max_size = int(MAX_FILE_SIZE)
        except Exception:
            max_size = 4 * 1024**3

        doc_size = getattr(document, "file_size", None)
        if doc_size and doc_size > max_size:
            await update.message.reply_text(
                f"❌ File too large ({doc_size // 1024 // 1024} MB). "
                f"Maximum allowed is {max_size // 1024 // 1024} MB.\n"
                "For large files please provide a direct download URL or use the web upload endpoint."
            )
            return

        # For subtitle flows we still need to download immediately (handled above).
        # Otherwise register the document lazily and show the menu.
        final_name = file_name
        thumb = None
        try:
            if user_settings:
                s = user_settings.get_user_settings(user_id)
                for w in s.get("words_remove") or []:
                    final_name = final_name.replace(w, "")
                final_name = final_name.strip()
                if not os.path.splitext(final_name)[1] and file_ext:
                    final_name += file_ext
                final_name = f"{s.get('prefix') or ''}{final_name}{s.get('suffix') or ''}"
                if s.get("save_thumbnail") and s.get("default_thumbnail"):
                    thumb = s.get("default_thumbnail")
        except Exception:
            logger.exception("Failed to apply user settings to document name")

        # Capture forward metadata when available (useful for userbot fallback)
        forward_info = None
        try:
            fch = getattr(update.message, "forward_from_chat", None)
            f_msg_id = getattr(update.message, "forward_from_message_id", None)
            if fch or f_msg_id:
                tmp = {}
                if fch:
                    tmp["chat_id"] = getattr(fch, "id", None) or getattr(fch, "username", None)
                if f_msg_id:
                    tmp["message_id"] = f_msg_id
                forward_info = tmp
        except Exception:
            forward_info = None

        msg_date = None
        try:
            if getattr(update, "message", None) and getattr(update.message, "date", None):
                msg_date = update.message.date.isoformat()
        except Exception:
            msg_date = None

        # document may be a photo, file or other; attempt to extract unique id
        file_unique_id = None
        try:
            if getattr(document, "file_unique_id", None):
                file_unique_id = document.file_unique_id
            else:
                # photos stored in message.photo list
                photos = getattr(update.message, "photo", None)
                if photos:
                    file_unique_id = getattr(photos[-1], "file_unique_id", None)
        except Exception:
            file_unique_id = None

        session["current_file"] = {
            "path": None,
            "type": file_type,
            "id": document.file_id,
            "size": document.file_size,
            "name": final_name,
            "thumbnail": thumb,
            "forward": forward_info,
            "chat_id": getattr(update.message, "chat", None) and getattr(update.message.chat, "id", None),
            "msg_id": getattr(update.message, "message_id", None),
            "msg_date": msg_date,
            "file_unique_id": file_unique_id,
        }

        try:
            self._persist_session(user_id)
        except Exception:
            logger.debug("Could not persist session after registering document")

        await update.message.reply_text(
            f"✅ {file_type.capitalize()} registered!\n"
            f"📁 {file_name}\n"
            f"Choose an action:",
            reply_markup=MediaMenuBuilder.get_main_menu(file_type),
        )

    async def callback_handler(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """Handle all callback queries with enhanced features."""
        query = update.callback_query
        logger.info(
            "Callback received: user=%s data=%s message_id=%s",
            getattr(update.effective_user, "id", None),
            getattr(query, "data", None),
            getattr(getattr(query, "message", None), "message_id", None),
        )
        # Defensive: ensure we have a callback_query
        if query is None:
            logger.warning("callback_handler called without callback_query")
            # Log and persist this event
            await self._log_bad_callback(
                "missing_query",
                None,
                getattr(update.effective_user, "id", None),
                getattr(update.effective_chat, "id", None),
                None,
            )
            return
        await query.answer()

        user_id = update.effective_user.id
        data = query.data

        # Validate callback payload
        if not isinstance(data, str):
            try:
                await query.answer()
            except Exception:
                pass
            await self.safe_edit(query, "⚠️ Invalid button payload.")
            logger.warning(
                f"Invalid callback data type: {type(data)} data={data}"
            )
            # Persist bad callback event
            await self._log_bad_callback(
                "invalid_payload",
                data,
                user_id,
                getattr(update.effective_chat, "id", None),
                getattr(getattr(query, "message", None), "message_id", None),
            )
            return
        # Accept older/alternate callback names from `MediaMenuBuilder` by mapping
        # them to the canonical names expected by this handler. This keeps
        # `utils/keyboard_utils.py` unchanged while ensuring callbacks are handled.
        aliases = {
            # Main/menu aliases
            "video_tools": "menu_video",
            "back_to_main": "menu_main",
            "media_info": "info",
            "send_file": "menu_main",
            "help": "menu_main",
            "quick_start": "menu_main",
            # Conversion / format aliases
            "convert_audio": "convert_format_menu",
            "convert_video": "convert_format_menu",
            "audio_mp3": "format_mp3",
            "audio_wav": "format_wav",
            "audio_aac": "format_aac",
            "audio_flac": "format_flac",
            "audio_ogg": "format_ogg",
            "audio_m4a": "format_m4a",
            # Merge aliases
            "merge_audio": "merge_audios_menu",
            "merge_start": "merge_videos_start",
            # individual merge menu actions are handled via UI flow; map sensible
            "merge_add": "merge_add",
            "merge_view": "merge_view",
            "merge_clear": "merge_clear",
            # Resolution presets: map explicit WxH to handler-friendly keys
            "res_3840_2160": "res_4k",
            "res_1920_1080": "res_1080",
            "res_1280_720": "res_720",
            "res_854_480": "res_480",
            "res_640_360": "res_360",
            # Screenshot menu differences
            "screenshot_grid_3": "screenshot_9grid",
            "screenshot_grid_4": "screenshot_multiple",
            # Extraction aliases
            "extract_audio_only": "extract_audio",
            "extract_video_only": "extract_streams",
            "extract_all": "extract_all_streams",
            # Misc small mappings
            "add_audio": "merge_av_menu",
            # UI-friendly names mapping to canonical handler keys
            "thumbnail_grid": "thumbnail_grid",
            "thumbnail_extractor": "thumbnail_grid",
            "caption_editor": "caption_editor",
            "media_forwarder": "media_forwarder",
            "stream_remover": "remove_audio",
            "stream_extractor": "extract_streams",
            "video_splitter": "video_splitter",
            "manual_shots": "screenshot_custom",
            "video_to_audio": "convert_mp3",
            "subtitle_merger": "add_subtitles",
            "video_renamer": "video_renamer",
            "video_converter": "convert_format_menu",
        }

        # Remap data if an alias exists
        data = aliases.get(data, data)

        # Wrap handler dispatch in try/except to catch unexpected errors
        # Import canonical callback names for comparison when needed
        # canonical callback names (if ever needed) are provided by `utils.callbacks`.
        # We don't import them here to avoid unused-name noise from linters.
        try:

            # Map video bitrate shortcuts to generic bitrate handler
            if isinstance(data, str) and data.startswith("vbitrate_"):
                data = "bitrate_" + data.split("_", 1)[1]

            # Ensure session exists
            if user_id not in self.user_sessions:
                # Try to load persisted session (useful when running multiple workers)
                persisted = self._load_persisted_session(user_id)
                if persisted:
                    self.user_sessions[user_id] = {
                        "files": {},
                        "current_file": persisted.get("current_file"),
                        "merge_list": persisted.get("merge_list", []),
                    }
                else:
                    self.user_sessions[user_id] = {"files": {}, "current_file": None}

            session = self.user_sessions[user_id]
            current_file = session.get("current_file")

            # Main menu navigation
            if data == "menu_main":
                await self.safe_edit(
                    query,
                    "🎬 **Media Conversion Bot**\nSelect a category:",
                    reply_markup=MediaMenuBuilder.get_main_menu(
                        current_file["type"] if current_file else None
                    ),
                )

            elif data == "menu_video":
                await self.safe_edit(
                    query,
                    "🎬 **Video Tools**\nChoose an action:",
                    reply_markup=MediaMenuBuilder.get_video_tools_menu(),
                )

            elif data == "menu_audio":
                await self.safe_edit(
                    query,
                    "🎧 **Audio Tools**\nChoose an action:",
                    reply_markup=MediaMenuBuilder.get_audio_tools_menu(),
                )

            elif data == "menu_advanced":
                await self.safe_edit(
                    query,
                    "🔧 **Advanced Tools**\nChoose an action:",
                    reply_markup=MediaMenuBuilder.get_advanced_tools_menu(),
                )

            # Video tools
            elif data == "convert_mp3":
                await self.convert_to_mp3(update, context, session)

            elif data == "compress_menu":
                await self.safe_edit(
                    query,
                    "📉 **Compression Options**\nSelect quality preset:",
                    reply_markup=MediaMenuBuilder.get_compression_menu(),
                )

            elif isinstance(data, str) and data.startswith("compress_"):
                crf = data.split("_")[1]
                await self.compress_video(update, context, session, crf)

            elif data == "trim_video":
                # Open trimmer selection menu with two dynamic modes
                await self.safe_edit(
                    query,
                    "✂️ **Video Trimming**\nChoose a trimmer mode:",
                    reply_markup=MediaMenuBuilder.get_trimmer_menu(),
                )

            elif data == "trimmer_1":
                # Trimmer 1: ask for start then end time
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_trimmer"] = "trimmer1_start"
                await self.safe_edit(
                    query,
                    "✂️ Trimmer 1 selected.\nSend START time (HH:MM:SS[.ms])\nExample: 00:01:00",
                )
                return

            elif data == "trimmer_2":
                # Trimmer 2: ask for start then duration
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_trimmer"] = "trimmer2_start"
                await self.safe_edit(
                    query,
                    "✂️ Trimmer 2 selected.\nSend START time (HH:MM:SS[.ms])\nExample: 00:05:00",
                )
                return

            elif data == "merge_videos_menu":
                await self.safe_edit(
                    query,
                    "🔀 **Merge Videos**\nSend multiple video files, then click 'Start Merge':",
                    reply_markup=MediaMenuBuilder.get_merge_menu("video"),
                )

            elif data == "merge_videos_start":
                await self.merge_videos(update, context, session)

            elif data == "remove_audio":
                await self.remove_audio(update, context, session)

            elif data == "merge_av_menu":
                await self.safe_edit(
                    query,
                    "🎵 **Merge Audio with Video**\nFirst send the audio file, then select this option again.",
                )

            elif data == "resolution_menu":
                await self.safe_edit(
                    query,
                    "📐 **Change Resolution**\nSelect preset:",
                    reply_markup=MediaMenuBuilder.get_resolution_menu(),
                )

            elif isinstance(data, str) and data.startswith("res_"):
                resolution = data.split("_")[1]
                await self.change_resolution(update, context, session, resolution)

            elif data == "optimize_menu":
                await self.safe_edit(
                    query,
                    "⚡ **Optimize Video**\nSelect optimization preset:",
                    reply_markup=MediaMenuBuilder.get_optimize_menu(),
                )

            elif isinstance(data, str) and data.startswith("optimize_"):
                preset = data.split("_")[1]
                await self.optimize_video(update, context, session, preset)

            elif data == "repair_video":
                await self.repair_video(update, context, session)

            elif data == "screenshots_menu":
                await self.safe_edit(
                    query,
                    "🖼️ **Screenshot Options**\nChoose an option:",
                    reply_markup=MediaMenuBuilder.get_screenshots_menu(),
                )

            elif isinstance(data, str) and data.startswith("screenshot_"):
                option = data.split("_")[1]
                await self.take_screenshot(update, context, session, option)

            elif data == "extract_streams":
                await self.extract_streams(update, context, session)

            elif data == "extract_audio":
                await self.extract_audio(update, context, session)

            # Audio tools
            elif data == "convert_format_menu":
                # Determine appropriate media type for format menu (video vs audio)
                media_type = "audio"
                try:
                    if current_file and current_file.get("type") == "video":
                        media_type = "video"
                except Exception:
                    media_type = "audio"

                await self.safe_edit(
                    query,
                    "🔄 **Convert Format**\nSelect target format:",
                    reply_markup=MediaMenuBuilder.get_format_menu(media_type),
                )

            elif isinstance(data, str) and data.startswith("format_"):
                format_type = data.split("_")[1]
                # Route to video or audio conversion depending on current file type
                try:
                    if current_file and current_file.get("type") == "video":
                        await self.convert_video_format(update, context, session, format_type)
                    else:
                        await self.convert_audio_format(update, context, session, format_type)
                except Exception:
                    # Fallback to audio conversion to preserve previous behavior
                    await self.convert_audio_format(update, context, session, format_type)

            elif data == "bitrate_menu":
                await self.safe_edit(
                    query,
                    "🎚️ **Adjust Bitrate**\nSelect bitrate:",
                    reply_markup=MediaMenuBuilder.get_bitrate_menu(),
                )

            # Merge list interactions
            elif data == "merge_add":
                # Add the current file to the merge list
                current_file = session.get("current_file")
                if not current_file:
                    await self.safe_edit(
                        query, "❌ No current file to add. Send a file first."
                    )
                    return
                path = current_file.get("path")
                if not path or not os.path.exists(path):
                    await self.safe_edit(query, "❌ File not available to add.")
                    return
                # Ensure merge_list stores file paths
                if "merge_list" not in session:
                    session["merge_list"] = []
                session["merge_list"].append(path)
                # Persist session after update
                try:
                    self._persist_session(user_id)
                except Exception:
                    logger.debug("Could not persist session after merge_add")
                await self.safe_edit(
                    query,
                    f"➕ Added to merge list. Total files: {len(session['merge_list'])}",
                )
                try:
                    await query.answer("Added to merge list")
                except Exception:
                    pass

            elif isinstance(data, str) and data.startswith("merge_view"):
                # Support pagination: callback forms: 'merge_view' or 'merge_view:2'
                try:
                    parts = data.split(":")
                    page = int(parts[1]) if len(parts) > 1 else 1
                except Exception:
                    page = 1

                merge_list = session.get("merge_list") or []
                if not merge_list:
                    await self.safe_edit(query, "🗒️ Merge list is empty.")
                else:
                    per_page = 5
                    total = len(merge_list)
                    last_page = max(1, (total + per_page - 1) // per_page)
                    page = max(1, min(page, last_page))
                    start = (page - 1) * per_page
                    end = start + per_page
                    slice_items = merge_list[start:end]

                    text_lines = [f"🗒️ Merge list ({page}/{last_page}):\n"]
                    for idx, p in enumerate(slice_items, start=start + 1):
                        text_lines.append(f"{idx}. {os.path.basename(p)}")

                    # Build navigation buttons
                    nav_buttons = []
                    if page > 1:
                        nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"merge_view:{page-1}"))
                    if page < last_page:
                        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"merge_view:{page+1}"))

                    control_row = [InlineKeyboardButton("🗑️ Clear", callback_data="merge_clear"), InlineKeyboardButton("↩️ Back", callback_data="menu_main")]

                    kb = [ [InlineKeyboardButton(os.path.basename(p), callback_data="noop") ] for p in slice_items ]
                    if nav_buttons:
                        kb.append(nav_buttons)
                    kb.append(control_row)

                    await self.safe_edit(query, "\n".join(text_lines), reply_markup=InlineKeyboardMarkup(kb))

            elif data == "merge_clear":
                session["merge_list"] = []
                try:
                    self._persist_session(user_id)
                except Exception:
                    logger.debug("Could not persist session after merge_clear")
                await self.safe_edit(query, "🗑️ Merge list cleared.")
                try:
                    await query.answer("Merge list cleared")
                except Exception:
                    pass

            elif data == "framerate_menu":
                await self.safe_edit(
                    query,
                    "⏱️ **Change Framerate**\nEnter target FPS (e.g., 24, 30, 60).",
                )
                context.user_data["awaiting_framerate"] = True
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

            elif data == "fade_menu":
                await self.safe_edit(query, "📈 Fade In/Out: feature coming soon.")

            elif data == "cancel":
                # Clear any awaiting inputs and notify user
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                await self.safe_edit(
                    query,
                    "❌ Operation cancelled.",
                    reply_markup=MediaMenuBuilder.get_main_menu(
                        current_file["type"] if current_file else None
                    ),
                )

            elif data == "confirm":
                await self.safe_edit(
                    query,
                    "✅ Confirmed.",
                    reply_markup=MediaMenuBuilder.get_main_menu(
                        current_file["type"] if current_file else None
                    ),
                )

            elif isinstance(data, str) and data.startswith("bitrate_"):
                bitrate = data.split("_")[1]
                await self.adjust_bitrate(update, context, session, bitrate)

            elif data == "trim_audio":
                await self.safe_edit(
                    query, "✂️ **Trim Audio**\nSend start time (HH:MM:SS):"
                )
                context.user_data["awaiting_trim"] = "start"
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

            elif data == "caption_editor":
                # Ask user to send a new caption for the current file
                current_file = session.get("current_file")
                if not current_file:
                    await self.safe_edit(query, "❌ No file found to caption.")
                    return
                await self.safe_edit(query, "✏️ Send the new caption text:")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_caption"] = True

            elif data == "video_renamer":
                current_file = session.get("current_file")
                if not current_file:
                    await self.safe_edit(query, "❌ No file found to rename.")
                    return
                await self.safe_edit(query, "✏️ Send new filename (include extension):")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_rename"] = True

            elif data == "video_splitter":
                current_file = session.get("current_file")
                if not current_file or current_file.get("type") != "video":
                    await self.safe_edit(query, "❌ No video file found to split.")
                    return
                await self.safe_edit(
                    query,
                    "📌 Send split as either 'start-end' in seconds (e.g. 10-30) or 'n' for number of equal parts:",
                )
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_split"] = True

            elif data == "media_forwarder":
                current_file = session.get("current_file")
                if not current_file:
                    await self.safe_edit(query, "❌ No file to forward.")
                    return
                await self.safe_edit(query, "➡️ Send target chat id or @username to forward the file to:")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_forward_to"] = True

            elif data == "merge_audios_menu":
                await self.safe_edit(
                    query,
                    "🔀 **Merge Audio Files**\nSend multiple audio files, then click 'Start Merge':",
                    reply_markup=MediaMenuBuilder.get_merge_menu("audio"),
                )

            elif data == "merge_audios_start":
                await self.merge_audios(update, context, session)

            elif data == "normalize_audio":
                await self.normalize_audio(update, context, session)

            # Advanced tools
            elif data == "extract_all_streams":
                await self.extract_all_streams(update, context, session)

            elif data == "extract_subtitles":
                await self.extract_subtitles(update, context, session)

            elif data == "edit_metadata":
                await self.safe_edit(
                    query,
                    "🏷️ Edit metadata: send JSON (example in README).",
                )
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
                context.user_data["awaiting_metadata"] = True

            elif data == "full_info":
                await self.show_full_info(update, context, session)

            elif data == "create_archive":
                await self.create_archive(update, context, session)

            elif data == "bulk_menu":
                # Open the bulk action menu
                await self.show_bulk_menu(update, context)

            elif isinstance(data, str) and data.startswith("bulk_toggle:"):
                # Toggle a boolean bulk setting for the user
                try:
                    key = data.split(":", 1)[1]
                except Exception:
                    key = None
                if not key:
                    await self.safe_edit(query, "⚠️ Invalid toggle request.")
                    return

                try:
                    if user_settings:
                        new = user_settings.toggle_user_setting(user_id, key)
                    else:
                        # fallback to session-scoped bulk settings
                        sess = session or self.user_sessions.setdefault(user_id, {})
                        b = sess.setdefault("bulk_settings", {})
                        cur = bool(b.get(key))
                        new = not cur
                        b[key] = new

                    await self.safe_edit(query, f"✅ {key.replace('_', ' ').title()}: {'On' if new else 'Off'}")
                    # re-render the bulk menu to show updated status
                    await self.show_bulk_menu(update, context)
                except Exception:
                    logger.exception("Failed to toggle bulk setting %s", key)
                    await self.safe_edit(query, "⚠️ Failed to toggle setting")

            elif data == "bulk_apply":
                # Apply bulk actions to files in session.merge_list or current_file
                try:
                    sess = session or self.user_sessions.get(user_id, {})
                    files = sess.get("merge_list") or []
                    if not files and sess.get("current_file"):
                        files = [sess.get("current_file")]

                    if not files:
                        await self.safe_edit(query, "❌ No files selected for bulk processing.")
                        return

                    enqueued = 0
                    for f in list(files):
                        try:
                            # Ensure file is downloaded locally (best-effort)
                            if not f.get("path") or not os.path.exists(f.get("path") or ""):
                                try:
                                    await self._ensure_current_file_downloaded(update, context, sess)
                                except Exception:
                                    # try next file if download failed
                                    logger.debug("bulk: download failed for %s", f.get("id"))
                                    continue

                            if not f.get("path"):
                                continue

                            job_id = str(uuid.uuid4()) if uuid else None
                            out_path = f"storage/output/{f.get('id')}_bulk.mp4"
                            job = {
                                "job_id": job_id,
                                "input_path": f.get("path"),
                                "output_path": out_path,
                                "ffmpeg_args": None,
                                "progress_channel": f"ffmpeg:progress:{job_id}",
                                "chat_id": update.effective_chat.id if update and getattr(update, 'effective_chat', None) else None,
                                "caption": f"Bulk conversion finished for {f.get('name') or f.get('id')}",
                                "cleanup_input": True,
                                "cleanup_output": False,
                            }

                            if enqueue_job:
                                try:
                                    await enqueue_job(job)
                                    enqueued += 1
                                except Exception:
                                    logger.exception("Failed to enqueue bulk job for %s", f.get('id'))
                            else:
                                sess.setdefault('queued_bulk_jobs', []).append(job)
                                enqueued += 1
                        except Exception:
                            logger.exception("Failed processing bulk file %s", f.get('id'))

                    await self.safe_edit(query, f"✅ Bulk apply queued for {enqueued} file(s).")
                except Exception:
                    logger.exception("bulk_apply failed")
                    await self.safe_edit(query, "⚠️ Failed to apply bulk actions.")

            elif data == "video_reorder":
                # Placeholder for video reorder feature
                await self.safe_edit(query, "🔁 Video Reorder\n\nThis feature is coming soon — you can manage order via the merge list for now.")

            elif data == "mp3_tag_editor":
                # Simple entry point for mp3 tag edits (advanced editor may be added later)
                try:
                    await self.safe_edit(query, "✏️ Mp3 Tag Editor\n\nSend a JSON object with tag keys and values (example: {\"title\":\"Song\"}).")
                    # mark awaiting state so next message can be treated as metadata
                    context.user_data["awaiting_mp3_tags"] = True
                except Exception:
                    await self.safe_edit(query, "⚠️ Failed to open Mp3 Tag Editor.")

            elif data == "convert_to_video":
                # Show video format menu
                await self.safe_edit(query, "🔄 Convert To Video\nSelect target container:", reply_markup=MediaMenuBuilder.get_format_menu("video"))

            elif data == "convert_to_file":
                # Generic convert menu
                await self.safe_edit(query, "🔄 Convert To File\nSelect target format:", reply_markup=MediaMenuBuilder.get_format_menu())

            elif data == "batch_process":
                await self.safe_edit(
                    query,
                    "🔀 **Batch Processing**\nComing soon! Send multiple files to process.",
                )

            # Settings pagination and toggle handlers
            elif isinstance(data, str) and data.startswith("settings_page:"):
                # Show a specific settings page
                try:
                    page = int(data.split(":", 1)[1])
                except Exception:
                    page = 1

                s = user_settings.get_user_settings(user_id) if user_settings else {}
                if page == 1:
                    text = "⚙️ <b>Your Settings — Page 1</b>\n\n"
                    text += f"• Upload mode: {s.get('upload_mode')}\n"
                    text += f"• Prefix: {s.get('prefix')!s}\n"
                    text += f"• Suffix: {s.get('suffix')!s}\n"
                    kb = [
                        [InlineKeyboardButton(f"Toggle Save Thumb: {'On' if s.get('save_thumbnail') else 'Off'}", callback_data="toggle_save_thumbnail")],
                        [InlineKeyboardButton(f"Toggle Bulk Mode: {'On' if s.get('bulk_mode') else 'Off'}", callback_data="toggle_bulk_mode")],
                        [InlineKeyboardButton("Next ➡️", callback_data="settings_page:2")],
                        [InlineKeyboardButton("Close", callback_data="menu_main")],
                    ]
                    await self.safe_edit(query, text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
                else:
                    text = "⚙️ <b>Your Settings — Page 2</b>\n\n"
                    text += f"• Words to remove: {', '.join(s.get('words_remove') or [])}\n"
                    text += f"• Default thumbnail: {s.get('default_thumbnail')}\n"
                    kb = [
                        [InlineKeyboardButton("⬅️ Prev", callback_data="settings_page:1")],
                        [InlineKeyboardButton("Close", callback_data="menu_main")],
                    ]
                    await self.safe_edit(query, text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")

            elif isinstance(data, str) and data.startswith("toggle_"):
                # toggle_<key>
                key = data.split("_", 1)[1]
                if user_settings is None:
                    await self.safe_edit(query, "⚠️ Settings not available.")
                else:
                    try:
                        new = user_settings.toggle_user_setting(user_id, key)
                        await self.safe_edit(query, f"✅ `{key}` set to {new}", parse_mode="Markdown")
                        try:
                            await query.answer("Setting updated")
                        except Exception:
                            pass
                    except Exception:
                        logger.exception("Failed to toggle setting %s for user %s", key, user_id)
                        await self.safe_edit(query, "⚠️ Failed to change setting.")

            elif data == "reset_settings":
                # Reset user's settings to defaults (confirmation shown)
                if user_settings is None:
                    await self.safe_edit(query, "⚠️ Settings not available.")
                else:
                    try:
                        user_settings.clear_user_settings(user_id)
                        await self.safe_edit(query, "✅ Your settings have been reset to defaults.")
                        try:
                            await query.answer("Settings reset")
                        except Exception:
                            pass
                    except Exception:
                        logger.exception("Failed to reset settings for user %s", user_id)
                        await self.safe_edit(query, "⚠️ Failed to reset settings.")

            elif isinstance(data, str) and data.startswith("cancel_job:"):
                # User pressed a Cancel button for a queued job
                try:
                    job_id = data.split(":", 1)[1]
                except Exception:
                    await self.safe_edit(query, "⚠️ Invalid cancel request.")
                    return

                try:
                    from utils.job_queue import cancel_job

                    await cancel_job(job_id)
                    await self.safe_edit(query, f"⏹️ Cancellation requested for job {job_id}.")
                    try:
                        await query.answer("Cancellation requested")
                    except Exception:
                        pass
                except Exception:
                    logger.exception("Failed to request cancellation for job %s", job_id)
                    await self.safe_edit(query, "⚠️ Failed to request cancellation.")

            elif data == "add_thumb_instruction":
                # Provide instructions to the user for adding a custom thumbnail
                await self.safe_edit(
                    query,
                    "To add a custom thumbnail: reply to a photo with the command /addthumb",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Close", callback_data="menu_main")]]),
                )

            elif data == "delete_custom_thumb":
                # Delete per-user thumbnail file if present
                try:
                    thumb_path = os.path.join(os.path.dirname(__file__), "thumbnails", f"{user_id}.jpg")
                    if os.path.exists(thumb_path):
                        os.remove(thumb_path)
                        await self.safe_edit(query, "🗑️ Custom thumbnail deleted.")
                    else:
                        await self.safe_edit(query, "⚠️ No custom thumbnail set.")
                except Exception:
                    logger.exception("Failed to delete custom thumbnail for user %s", user_id)
                    await self.safe_edit(query, "⚠️ Failed to delete thumbnail.")

            elif data == "thumbnail_grid":
                await self.create_thumbnail_grid(update, context, session)

            elif data == "generate_sample":
                await self.generate_sample(update, context, session)

            elif data == "add_subtitles":
                await self.safe_edit(
                    query, "➕ **Add Subtitles**\nSend subtitle file (.srt, .ass):"
                )
                # Expect next document upload to be subtitle file to attach
                context.user_data["awaiting_subtitle_file"] = True

            elif data == "burn_subtitles":
                # Ask user to send subtitle file to burn into current video
                current_file = session.get("current_file")
                if not current_file or current_file.get("type") != "video":
                    await self.safe_edit(query, "❌ No video file found to burn subtitles into.")
                else:
                    await self.safe_edit(
                        query,
                        (
                            "✏️ **Burn Subtitles**\n"
                            "Send subtitle file (.srt, .ass) to burn into the current video:"
                        ),
                    )
                    context.user_data["awaiting_burn_subtitle"] = True

            # Information
            elif data == "info":
                await self.show_media_info(update, context, session)

            elif data == "noop":
                # Non-actionable placeholder button pressed; acknowledge silently.
                try:
                    await query.answer()
                except Exception:
                    pass
                return

            else:
                await self.safe_edit(query, f"Unknown command: {data}")
        except Exception as e:
            # Log unexpected exceptions along with callback metadata
            try:
                await self._log_bad_callback(
                    "callback_handler_exception",
                    {
                        "exception": repr(e),
                        "callback_data": data,
                    },
                    getattr(update.effective_user, "id", None),
                    getattr(update.effective_chat, "id", None),
                    getattr(getattr(query, "message", None), "message_id", None),
                )
            except Exception:
                logger.exception("Failed to persist callback_handler exception")

            # Optional debug dump of full Update JSON when enabled via env
            try:
                if os.environ.get("DEBUG_DUMP_UPDATES", "0").lower() in ("1", "true", "yes"):
                    dump_dir = os.path.join(os.path.dirname(__file__), "logs")
                    os.makedirs(dump_dir, exist_ok=True)
                    dump_path = os.path.join(dump_dir, "update_dumps.log")
                    try:
                        # Prefer structured dict if Update supports it
                        if hasattr(update, "to_dict"):
                            update_data = update.to_dict()
                        elif hasattr(update, "to_json"):
                            # to_json may return a JSON string
                            try:
                                update_data = json.loads(update.to_json())
                            except Exception:
                                update_data = {"repr": repr(update)}
                        else:
                            update_data = {"repr": repr(update)}

                        entry = {
                            "timestamp": datetime.utcnow().isoformat() + "Z",
                            "update_id": getattr(update, "update_id", None),
                            "callback_data": data,
                            "exception": repr(e),
                            "update": update_data,
                        }
                        with open(dump_path, "a", encoding="utf-8") as fh:
                            fh.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
                        logger.info("Wrote update dump to %s", dump_path)
                    except Exception:
                        logger.exception("Failed writing update dump for callback exception")
            except Exception:
                logger.exception("Failed to evaluate DEBUG_DUMP_UPDATES")

            logger.exception("Unhandled exception in callback_handler: %s", e)
            try:
                await self.safe_edit(query, "⚠️ Internal error while handling the button. Try again later.")
            except Exception:
                # Best-effort only
                logger.exception("Failed to notify user after callback_handler exception")
            return

    # ========== IMPLEMENTATION METHODS ==========

    async def convert_to_mp3(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Convert video to MP3."""
        # Defensive: ensure this was invoked via a callback query
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")
        user_id = update.effective_user.id

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        # Conversion quota enforcement
        if not await self._check_conversion_quota(update, context):
            return

        # Check rate limiting
        conversion_limiter = context.application.bot_data.get(
            "conversion_rate_limiter"
        )
        if conversion_limiter:
            allowed, message = await conversion_limiter.can_convert(
                str(user_id)
            )
            if not allowed:
                await self.safe_edit(query, message)
                return

        # Check queue status
        active_count = len(self.active_conversions)
        max_conversions = getattr(self, "_max_conversions", 1)

        if active_count >= max_conversions:
            queue_position = active_count - max_conversions + 1
            await self.safe_edit(
                query,
                f"⏳ Queue position: #{queue_position}\n"
                f"Active conversions: {active_count}/{max_conversions}\n"
                f"Your conversion will start soon...",
            )
        else:
            await self.safe_edit(query, "🎵 Converting to MP3...")

        async def do_conversion():
            # Lock the input file to prevent concurrent access
            output_path = f"storage/output/{current_file['id']}_audio.mp3"

            if AsyncFileLock:
                lock = await AsyncFileLock.acquire(current_file["path"])
                async with lock:
                    success = await self.converter.extract_audio_from_video(
                        current_file["path"], output_path, "mp3", "192k"
                    )

                    if success and os.path.exists(output_path):
                        file_size = os.path.getsize(output_path)
                        if file_size > 50 * 1024 * 1024:  # 50MB Telegram limit
                            await self.safe_edit(
                                query,
                                f"❌ File too large ({file_size//1024//1024}MB).\n"
                                "Try compression first.",
                            )
                            os.remove(output_path)
                        else:
                            with open(output_path, "rb") as audio_file:
                                await context.bot.send_audio(
                                    chat_id=update.effective_chat.id,
                                    audio=audio_file,
                                    caption="✅ Converted to MP3",
                                    title=current_file["name"].replace(
                                        ".mp4", ".mp3"
                                    ),
                                    performer="Media Bot",
                                )
                            os.remove(output_path)
                    else:
                        await self.safe_edit(query, "❌ Conversion failed.")

                    await AsyncFileLock.release(current_file["path"])
            else:
                # Fallback without locking
                success = await self.converter.extract_audio_from_video(
                    current_file["path"], output_path, "mp3", "192k"
                )

                if success and os.path.exists(output_path):
                    file_size = os.path.getsize(output_path)
                    if file_size > 50 * 1024 * 1024:  # 50MB Telegram limit
                        await self.safe_edit(
                            query,
                            f"❌ File too large ({file_size//1024//1024}MB).\n"
                            "Try compression first.",
                        )
                        os.remove(output_path)
                    else:
                        with open(output_path, "rb") as audio_file:
                            await context.bot.send_audio(
                                chat_id=update.effective_chat.id,
                                audio=audio_file,
                                caption="✅ Converted to MP3",
                                title=current_file["name"].replace(
                                    ".mp4", ".mp3"
                                ),
                                performer="Media Bot",
                            )
                        os.remove(output_path)
                else:
                    await self.safe_edit(query, "❌ Conversion failed.")

        # Ensure file downloaded before conversion (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        await self._run_with_concurrency_limit(
            user_id, "mp3_conversion", do_conversion()
        )

    async def compress_video(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: Dict,
        crf: str,
    ):
        """Compress video with specified CRF."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        user_id = update.effective_user.id

        if not await self._check_conversion_quota(update, context):
            return

        if crf == "custom":
            await self.safe_edit(
                query, "Enter CRF value (18-51, lower=better quality):"
            )
            context.user_data["awaiting_crf"] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        current_file = session.get("current_file")
        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        active_count = len(self.active_conversions)
        max_conversions = getattr(self, "_max_conversions", 1)

        if active_count >= max_conversions:
            queue_position = active_count - max_conversions + 1
            await self.safe_edit(
                query,
                f"⏳ Queue position: #{queue_position}\n"
                f"Active conversions: {active_count}/{max_conversions}\n"
                f"Your compression will start soon...",
            )
        else:
            await self.safe_edit(query, f"📉 Compressing with CRF {crf}...")

        async def do_compression():
            output_path = f"storage/output/{current_file['id']}_compressed.mp4"

            # Map resolution presets
            resolution_map = {
                "4k_to_1080": ("1920", "1080"),
                "1080_to_720": ("1280", "720"),
            }

            if crf in resolution_map:
                width, height = resolution_map[crf]
                success = await self.converter.change_resolution(
                    current_file["path"], output_path, int(width), int(height)
                )
            else:
                # default optimize path: treat crf as an integer when possible
                crf_value = (
                    int(crf) if isinstance(crf, str) and crf.isdigit() else 28
                )
                success = await self.converter.optimize_video(
                    current_file["path"], output_path, "medium", crf_value
                )

            if success and os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                if file_size > 2 * 1024**3:  # 2GB
                    await self.safe_edit(
                        query,
                        f"❌ Compressed file still too large ({file_size//1024//1024}MB).\n"
                        "Try higher compression.",
                    )
                    os.remove(output_path)
                else:
                    with open(output_path, "rb") as video_file:
                        await context.bot.send_video(
                            chat_id=update.effective_chat.id,
                            video=video_file,
                            caption=f"✅ Compressed (CRF {crf})",
                            supports_streaming=True,
                        )
                    os.remove(output_path)
            else:
                await self.safe_edit(query, "❌ Compression failed.")

        # Ensure file downloaded before compression (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        await self._run_with_concurrency_limit(
            user_id, "compression", do_compression()
        )

    async def merge_videos(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Merge multiple videos."""
        if not await self._require_callback(update):
            return
        query = update.callback_query

        if not await self._check_conversion_quota(update, context):
            return

        if "merge_list" not in session or len(session["merge_list"]) < 2:
            await self.safe_edit(
                query,
                "❌ Need at least 2 videos to merge.\n"
                "Send video files first, then click 'Start Merge'.",
            )
            return

        await self.safe_edit(
            query, f"🔀 Merging {len(session['merge_list'])} videos..."
        )

        output_path = (
            f"storage/output/merged_{int(datetime.now().timestamp())}.mp4"
        )
        success = await self.converter.merge_videos(
            session["merge_list"], output_path
        )

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as video_file:
                await context.bot.send_video(
                    chat_id=update.effective_chat.id,
                    video=video_file,
                    caption=f"✅ Merged {len(session['merge_list'])} videos",
                    supports_streaming=True,
                )

            # Cleanup
            os.remove(output_path)
            for file_path in session["merge_list"]:
                if os.path.exists(file_path):
                    os.remove(file_path)
            session["merge_list"] = []
        else:
            await self.safe_edit(query, "❌ Merge failed.")

    async def merge_audios(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Merge multiple audio files."""
        if not await self._require_callback(update):
            return
        query = update.callback_query

        if not await self._check_conversion_quota(update, context):
            return

        if "merge_list" not in session or len(session["merge_list"]) < 2:
            await self.safe_edit(
                query,
                "❌ Need at least 2 audio files to merge.\n"
                "Send audio files first, then click 'Start Merge'.",
            )
            return

        await self.safe_edit(
            query, f"🔀 Merging {len(session['merge_list'])} audio files..."
        )

        output_path = (
            f"storage/output/merged_{int(datetime.now().timestamp())}.mp3"
        )
        success = await self.converter.merge_audios(
            session["merge_list"], output_path
        )

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption=f"✅ Merged {len(session['merge_list'])} audio files",
                    title="Merged Audio",
                )

            # Cleanup
            os.remove(output_path)
            for file_path in session["merge_list"]:
                if os.path.exists(file_path):
                    os.remove(file_path)
            session["merge_list"] = []
        else:
            await self.safe_edit(query, "❌ Merge failed.")

    async def remove_audio(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Remove audio from video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        await self.safe_edit(query, "🔉 Removing audio...")

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        output_path = f"storage/output/{current_file['id']}_no_audio.mp4"
        success = await self.converter.remove_audio(
            current_file["path"], output_path
        )

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as video_file:
                await context.bot.send_video(
                    chat_id=update.effective_chat.id,
                    video=video_file,
                    caption="✅ Audio removed",
                    supports_streaming=True,
                )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Failed to remove audio.")

    async def change_resolution(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: Dict,
        resolution: str,
    ):
        """Change video resolution."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        # Resolution mapping
        res_map = {
            "4k": (3840, 2160),
            "2k": (2560, 1440),
            "1080": (1920, 1080),
            "720": (1280, 720),
            "480": (854, 480),
            "360": (640, 360),
            "mobile": (480, 854),  # Portrait mobile
        }

        if resolution == "custom":
            await self.safe_edit(
                query, "Enter resolution (WIDTHxHEIGHT):\nExample: 1280x720"
            )
            context.user_data["awaiting_resolution"] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        if not await self._check_conversion_quota(update, context):
            return

        if resolution not in res_map:
            await self.safe_edit(query, "❌ Invalid resolution.")
            return

        width, height = res_map[resolution]
        await self.safe_edit(
            query, f"📐 Changing resolution to {width}x{height}..."
        )

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        output_path = (
            f"storage/output/{current_file['id']}_{width}x{height}.mp4"
        )
        success = await self.converter.change_resolution(
            current_file["path"], output_path, width, height
        )

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as video_file:
                await context.bot.send_video(
                    chat_id=update.effective_chat.id,
                    video=video_file,
                    caption=f"✅ Resolution: {width}x{height}",
                    supports_streaming=True,
                )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Failed to change resolution.")

    async def optimize_video(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: Dict,
        preset: str,
    ):
        """Optimize video for specific use case."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        # Preset mapping
        preset_map = {
            "web": ("slow", 23, "128k"),
            "mobile": ("medium", 28, "96k"),
            "tv": ("slow", 20, "192k"),
            "storage": ("veryfast", 35, "64k"),
            "fast": ("veryfast", 28, "128k"),
        }

        if preset == "custom":
            await self.safe_edit(
                query,
                "Enter optimization settings:\n"
                "Format: preset,crf,bitrate\n"
                "Example: slow,23,128k",
            )
            context.user_data["awaiting_optimize"] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        if preset not in preset_map:
            await self.safe_edit(query, "❌ Invalid preset.")
            return

        encoder_preset, crf, bitrate = preset_map[preset]
        await self.safe_edit(query, f"⚡ Optimizing for {preset}...")

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        output_path = f"storage/output/{current_file['id']}_optimized.mp4"

        # Use FFmpeg command for optimization
        cmd = [
            "-c:v",
            "libx264",
            "-preset",
            encoder_preset,
            "-crf",
            str(crf),
            "-c:a",
            "aac",
            "-b:a",
            bitrate,
            "-movflags",
            "+faststart",
        ]

        # Try to enqueue as a background job to get progress + cancel support
        if enqueue_job and uuid:
            job_id = str(uuid.uuid4())
            job = {
                "job_id": job_id,
                "input_path": current_file["path"],
                "output_path": output_path,
                "ffmpeg_args": cmd,
                "progress_channel": f"ffmpeg:progress:{job_id}",
                "chat_id": update.effective_chat.id if update and update.effective_chat else None,
                "caption": f"✅ Optimized for {preset}",
                "cleanup_input": True,
                "cleanup_output": True,
            }
            try:
                await enqueue_job(job)
            except Exception:
                logger.exception("Failed to enqueue optimization job")
                await self.safe_edit(query, "❌ Failed to queue optimization job.")
                return

            # Inform user and provide cancel button
            try:
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}")]])
                await self.safe_edit(query, f"✅ Optimization job queued (ID: {job_id}). I'll update you with progress.", reply_markup=kb)
                try:
                    asyncio.create_task(self._watch_job_progress(query, job_id))
                except Exception:
                    pass
            except Exception:
                await self.safe_edit(query, f"✅ Optimization job queued (ID: {job_id}).")
            return

        # Fallback: inline execution if no job queue available
        success, _ = await self.converter.execute_ffmpeg(cmd, current_file["path"], output_path)

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as video_file:
                await context.bot.send_video(
                    chat_id=update.effective_chat.id,
                    video=video_file,
                    caption=f"✅ Optimized for {preset}",
                    supports_streaming=True,
                )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Optimization failed.")

    async def repair_video(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Attempt to repair corrupted video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        # Conversion quota enforcement
        if not await self._check_conversion_quota(update, context):
            return

        await self.safe_edit(query, "🔧 Attempting to repair video...")
        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        # enqueue repair job
        job_id = str(uuid.uuid4())
        output_path = f"storage/output/{current_file['id']}_repaired.mp4"
        job = {
            "job_id": job_id,
            "input_path": current_file["path"],
            "output_path": output_path,
            "ffmpeg_args": ["-c", "copy"],
            "progress_channel": f"ffmpeg:progress:{job_id}",
            "chat_id": update.effective_chat.id if update and update.effective_chat else None,
            "caption": "✅ Video repaired (if possible)",
            "cleanup_input": True,
            "cleanup_output": True,
        }

        try:
            await enqueue_job(job)
            await self.safe_edit(query, f"✅ Repair job queued (ID: {job_id}). I'll send the file when ready.")
            try:
                asyncio.create_task(self._watch_job_progress(query, job_id))
            except Exception:
                pass
        except Exception:
            logger.exception("Failed to enqueue repair job")
            await self.safe_edit(query, "❌ Failed to queue repair job.")
        return

    async def take_screenshot(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: Dict,
        option: str,
    ):
        """Take screenshot(s) from video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        # Conversion quota enforcement
        if not await self._check_conversion_quota(update, context):
            return

        # Get video duration for calculations (try ffmpeg-python binding, fallback to error)
        ffmpeg_mod = ffmpeg
        if ffmpeg_mod is None:
            try:
                import importlib

                ffmpeg_mod = importlib.import_module("ffmpeg")
            except Exception:
                ffmpeg_mod = None

        if not ffmpeg_mod:
            await self.safe_edit(
                query,
                "FFmpeg-python binding is not available on the server. This operation requires ffmpeg-python.",
            )
            logger.info(
                "ffmpeg-python not available for take_screenshot; falling back to CLI where possible"
            )
            return

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        try:
            probe = ffmpeg_mod.probe(current_file["path"])
            duration = float(probe["format"]["duration"])
        except Exception as e:
            logger.warning(f"ffmpeg.probe failed: {e}")
            await self.safe_edit(
                query, "Failed to read media info for screenshot operation."
            )
            return

        if option == "custom":
            await self.safe_edit(query, "Enter time (HH:MM:SS or seconds):")
            context.user_data["awaiting_screenshot_time"] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        # Calculate time based on option
        def _fmt_time(seconds: float) -> str:
            secs = max(0.0, float(seconds))
            hours = int(secs // 3600)
            mins = int((secs % 3600) // 60)
            rem = secs % 60
            return f"{hours:02d}:{mins:02d}:{rem:06.3f}"

        middle = duration / 2
        end_time = max(0.0, duration - 1)

        time_map = {
            "start": "00:00:01",
            "middle": _fmt_time(middle),
            "end": _fmt_time(end_time),
        }

        if option == "9grid":
            await self.create_thumbnail_grid(update, context, session)
            return
        elif option == "multiple":
            await self.safe_edit(query, "How many screenshots? (2-20)")
            context.user_data["awaiting_screenshot_count"] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        time_str = time_map.get(option, "00:00:01")
        await self.safe_edit(query, f"🖼️ Taking screenshot at {time_str}...")

        output_path = f"storage/output/{current_file['id']}_screenshot.jpg"
        success = await self.converter.take_screenshot_at_time(
            current_file["path"], output_path, time_str
        )

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as photo_file:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=photo_file,
                    caption=f"✅ Screenshot at {time_str}",
                )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Failed to take screenshot.")

    async def create_thumbnail_grid(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Create thumbnail grid from video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        await self.safe_edit(query, "🖼️ Creating thumbnail grid...")

        output_path = f"storage/output/{current_file['id']}_grid.jpg"
        success = await self.converter.extract_thumbnail_grid(
            current_file["path"], output_path, 3, 3
        )

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as photo_file:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=photo_file,
                    caption="✅ Thumbnail grid (3x3)",
                )
            os.remove(output_path)
        else:
            # Fallback to single screenshot
            await self.take_screenshot(update, context, session, "middle")

    async def extract_streams(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Extract all streams from video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        await self.safe_edit(query, "🎞️ Extracting streams...")

        # enqueue extract_streams job to worker for progress/cancel support
        job_id = str(uuid.uuid4())
        out_dir = f"storage/output/{current_file['id']}_streams"
        archive_path = f"{out_dir}.zip"
        job = {
            "job_id": job_id,
            "type": "extract_streams",
            "input_path": current_file["path"],
            "output_dir": out_dir,
            "archive_path": archive_path,
            "progress_channel": f"ffmpeg:progress:{job_id}",
            "chat_id": update.effective_chat.id if update and update.effective_chat else None,
            "cleanup_input": True,
        }

        await enqueue_job(job)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}")]])
        await self.safe_edit(query, f"⏳ Job queued: {job_id} — extracting streams", reply_markup=kb)
        try:
            asyncio.create_task(self._watch_job_progress(query, job_id))
        except Exception:
            pass
        return

    async def convert_audio_format(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: Dict,
        format_type: str,
    ):
        """Convert audio to different format."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "audio":
            await self.safe_edit(query, "❌ No audio file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        await self.safe_edit(
            query, f"🔄 Converting to {format_type.upper()}..."
        )

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        output_path = (
            f"storage/output/{current_file['id']}_converted.{format_type}"
        )
        success = await self.converter.convert_audio_format(
            current_file["path"], output_path, format_type
        )

        if success and os.path.exists(output_path):
            mime_type = {
                "mp3": "audio/mpeg",
                "wav": "audio/wav",
                "aac": "audio/aac",
                "flac": "audio/flac",
                "ogg": "audio/ogg",
                "m4a": "audio/mp4",
            }.get(format_type, "audio/mpeg")

            with open(output_path, "rb") as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption=f"✅ Converted to {format_type.upper()}",
                    title=f"{os.path.splitext(current_file['name'])[0]}.{format_type}",
                    mime_type=mime_type,
                )
            os.remove(output_path)
        else:
            await self.safe_edit(
                query, f"❌ Failed to convert to {format_type}."
            )

    async def adjust_bitrate(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        session: Dict,
        bitrate: str,
    ):
        """Adjust audio bitrate."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "audio":
            await self.safe_edit(query, "❌ No audio file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        if bitrate == "custom":
            await self.safe_edit(query, "Enter bitrate (e.g., 128k, 320k):")
            context.user_data["awaiting_bitrate"] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        await self.safe_edit(query, f"🎚️ Setting bitrate to {bitrate}...")

        output_path = f"storage/output/{current_file['id']}_{bitrate}.mp3"

        # Convert with specific bitrate
        cmd = ["-c:a", "libmp3lame", "-b:a", bitrate]

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        success, _ = await self.converter.execute_ffmpeg(
            cmd, current_file["path"], output_path
        )

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption=f"✅ Bitrate: {bitrate}",
                    title=f"{current_file['name']}_{bitrate}",
                )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Failed to adjust bitrate.")

    async def normalize_audio(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Normalize audio volume."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "audio":
            await self.safe_edit(query, "❌ No audio file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        await self.safe_edit(query, "🔊 Normalizing audio...")

        output_path = f"storage/output/{current_file['id']}_normalized.mp3"

        # Use loudnorm filter for normalization
        cmd = [
            "-filter:a",
            "loudnorm=I=-16:TP=-1.5:LRA=11",
            "-c:a",
            "libmp3lame",
            "-b:a",
            "192k",
        ]

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        success, _ = await self.converter.execute_ffmpeg(
            cmd, current_file["path"], output_path
        )

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption="✅ Audio normalized",
                    title=f"{current_file['name']}_normalized",
                )
            os.remove(output_path)
        else:
            await self.safe_edit(query, "❌ Failed to normalize audio.")

    async def extract_all_streams(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Extract all streams (video, audio, subtitles)."""
        if not await self._require_callback(update):
            return
        await self.extract_streams(update, context, session)

    async def extract_subtitles(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Extract subtitles from video."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file or current_file["type"] != "video":
            await self.safe_edit(query, "❌ No video file found.")
            return

        await self.safe_edit(query, "📝 Extracting subtitles...")

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        output_path = f"storage/output/{current_file['id']}_subtitles.srt"
        success = await self.converter.extract_subtitles(
            current_file["path"], output_path
        )

        if success and os.path.exists(output_path):
            with open(output_path, "rb") as sub_file:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=sub_file,
                    caption="✅ Subtitles extracted",
                    filename=f"{current_file['name']}_subtitles.srt",
                )
            os.remove(output_path)
        else:
            await self.safe_edit(
                query, "❌ No subtitles found or extraction failed."
            )

    async def show_full_info(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Show full media information."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file:
            await self.safe_edit(query, "❌ No file found.")
            return

        await self.safe_edit(query, "📊 Analyzing media...")

        ffmpeg_mod = ffmpeg
        if ffmpeg_mod is None:
            try:
                import importlib

                ffmpeg_mod = importlib.import_module("ffmpeg")
            except Exception:
                ffmpeg_mod = None

        if not ffmpeg_mod:
            await self.safe_edit(
                query,
                "FFmpeg-python binding is not available on the server. Full media analysis requires ffmpeg-python.",
            )
            logger.warning("ffmpeg-python not available for media analysis")
            return

        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        try:
            probe = ffmpeg_mod.probe(current_file["path"])

            # Format information
            format_info = probe.get("format", {})
            streams = probe.get("streams", [])

            info_text = "📊 **Full Media Analysis**\n\n"
            info_text += f"📁 **File:** {current_file['name']}\n"
            info_text += f"📦 **Size:** {format_info.get('size', 0) // 1024 // 1024} MB\n"
            info_text += (
                f"🎞️ **Format:** {format_info.get('format_name', 'N/A')}\n"
            )
            info_text += f"⏱️ **Duration:** {float(format_info.get('duration', 0)):.2f}s\n"
            bitrate_kbps = int(format_info.get("bit_rate", 0)) // 1000
            info_text += f"📈 **Bitrate:** {bitrate_kbps} kbps\n\n"

            # Streams information
            info_text += f"🎬 **Streams ({len(streams)}):**\n"

            for i, stream in enumerate(streams):
                codec_type = stream.get("codec_type", "unknown")
                info_text += f"\n**Stream {i+1} ({codec_type}):**\n"

                if codec_type == "video":
                    info_text += (
                        f"  Codec: {stream.get('codec_name', 'N/A')}\n"
                    )
                    info_text += f"  Resolution: {stream.get('width', 'N/A')}x{stream.get('height', 'N/A')}\n"
                    num, den = stream.get("avg_frame_rate", "0/1").split("/")
                    fps = float(num) / float(den) if float(den) != 0 else 0
                    info_text += f"  FPS: {fps:.2f}\n"
                    sb = stream.get("bit_rate")
                    sb_kbps = f"{int(sb) // 1000} kbps" if sb else "N/A"
                    info_text += f"  Bitrate: {sb_kbps}\n"

                elif codec_type == "audio":
                    info_text += (
                        f"  Codec: {stream.get('codec_name', 'N/A')}\n"
                    )
                    info_text += (
                        f"  Channels: {stream.get('channels', 'N/A')}\n"
                    )
                    info_text += f"  Sample Rate: {stream.get('sample_rate', 'N/A')} Hz\n"
                    sb = stream.get("bit_rate")
                    sb_kbps = f"{int(sb) // 1000} kbps" if sb else "N/A"
                    info_text += f"  Bitrate: {sb_kbps}\n"

                elif codec_type == "subtitle":
                    info_text += (
                        f"  Codec: {stream.get('codec_name', 'N/A')}\n"
                    )
                    info_text += f"  Language: {stream.get('tags', {}).get('language', 'N/A')}\n"

            await self.safe_edit(
                query, info_text[:4000]
            )  # Telegram message limit

        except Exception as e:
            logger.error(f"Error analyzing media: {e}")
            await self.safe_edit(query, "❌ Failed to analyze media.")

    async def create_archive(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Create archive of processed files."""
        if not await self._require_callback(update):
            return
        query = update.callback_query

        # Get all files in output directory for this user
        user_id = update.effective_user.id
        output_dir = "storage/output"
        user_files = [
            f for f in os.listdir(output_dir) if f.startswith(str(user_id))
        ]

        if not user_files:
            await self.safe_edit(query, "❌ No files to archive.")
            return

        await self.safe_edit(query, "📦 Creating archive...")

        # enqueue create_archive job so worker handles packaging and progress
        file_paths = [os.path.join(output_dir, f) for f in user_files]
        archive_path = f"storage/output/{user_id}_archive.zip"
        job_id = str(uuid.uuid4())
        job = {
            "job_id": job_id,
            "type": "create_archive",
            "files": file_paths,
            "output_path": archive_path,
            "progress_channel": f"ffmpeg:progress:{job_id}",
            "chat_id": update.effective_chat.id if update and update.effective_chat else None,
        }

        await enqueue_job(job)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}")]])
        await self.safe_edit(query, f"⏳ Job queued: {job_id} — creating archive", reply_markup=kb)
        try:
            asyncio.create_task(self._watch_job_progress(query, job_id))
        except Exception:
            pass

    async def generate_sample(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Generate sample/preview of media."""
        if not await self._require_callback(update):
            return
        query = update.callback_query
        current_file = session.get("current_file")

        if not current_file:
            await self.safe_edit(query, "❌ No file found.")
            return

        if not await self._check_conversion_quota(update, context):
            return

        await self.safe_edit(query, "🎬 Generating 30-second sample...")
        # Ensure file downloaded (lazy-download)
        if not current_file.get("path") or not os.path.exists(current_file.get("path") or ""):
            try:
                await self._ensure_current_file_downloaded(update, context, session)
                current_file = session.get("current_file")
            except Exception as e:
                await self.safe_edit(query, f"❌ Failed to download file: {e}")
                return

        job_id = str(uuid.uuid4())
        output_path = f"storage/output/{current_file['id']}_sample"
        if current_file["type"] == "video":
            output_path += ".mp4"
        else:
            output_path += ".mp3"

        job = {
            "job_id": job_id,
            "type": "generate_sample",
            "input_path": current_file["path"],
            "output_path": output_path,
            "duration": 30,
            "progress_channel": f"ffmpeg:progress:{job_id}",
            "chat_id": update.effective_chat.id if update and update.effective_chat else None,
        }

        await enqueue_job(job)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_job:{job_id}")]])
        await self.safe_edit(query, f"⏳ Job queued: {job_id} — generating sample", reply_markup=kb)
        try:
            asyncio.create_task(self._watch_job_progress(query, job_id))
        except Exception:
            pass

    async def show_media_info(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict
    ):
        """Show basic media information."""
        if not await self._require_callback(update):
            return
        await self.show_full_info(update, context, session)

    async def log_media_to_db(self, user_id: int, file_info: Dict):
        """Log media processing to MongoDB."""
        try:
            if MediaConversionModel is None:
                logger.info("MongoDB model not available; skipping DB logging")
                return

            # If a MongoDB URL is not configured, skip logging
            import os

            mongo_url = os.environ.get("MONGODB_URL")
            if not mongo_url:
                logger.info("MONGODB_URL not set; skipping DB logging")
                return

            # Lazy-import motor and initialize model
            try:
                from motor.motor_asyncio import AsyncIOMotorClient

                client = AsyncIOMotorClient(mongo_url)
                db_model = MediaConversionModel(
                    client,
                    db_name=os.environ.get("MONGODB_NAME", None)
                    or "media_conversion_bot",
                )

                log_entry = {
                    "user_id": user_id,
                    "file_name": file_info["name"],
                    "file_type": file_info["type"],
                    "file_size": file_info["size"],
                    "timestamp": datetime.now(),
                    "action": "upload",
                }

                await db_model.log_conversion(log_entry)
                logger.info(f"Logged media upload for user {user_id}")
            except Exception as e:
                logger.error(f"Failed to log to MongoDB: {e}")
        except Exception as e:
            logger.error(f"Failed to log to MongoDB: {e}")

    async def handle_custom_input(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """Handle custom user input for various operations."""
        user_input = update.message.text.strip()
        user_id = update.effective_user.id

        if user_id not in self.user_sessions:
            await update.message.reply_text(
                "❌ Session expired. Please send a file first."
            )
            return ConversationHandler.END

        session = self.user_sessions[user_id]
        current_file = session.get("current_file")

        # --- Dynamic trimmer flow (Trimmer 1 & 2) ---
        if context.user_data.get("awaiting_trimmer"):
            mode = context.user_data.get("awaiting_trimmer")
            try:
                if mode == "trimmer1_start":
                    # Validate start time
                    _ = _parse_time_to_seconds(user_input)
                    context.user_data["trimmer_start"] = user_input.strip()
                    context.user_data["awaiting_trimmer"] = "trimmer1_end"
                    await update.message.reply_text(
                        "📥 Start time saved. Now send END time (HH:MM:SS[.ms])\nExample: 00:10:00"
                    )
                    return

                elif mode == "trimmer1_end":
                    start = context.user_data.get("trimmer_start")
                    if not start:
                        await update.message.reply_text("❌ Missing start time. Please restart Trimmer.")
                        for k in list(context.user_data.keys()):
                            if k.startswith("awaiting_") or k.startswith("trimmer_"):
                                del context.user_data[k]
                        return

                    # Parse times
                    start_s = _parse_time_to_seconds(start)
                    end_s = _parse_time_to_seconds(user_input)
                    if end_s <= start_s:
                        await update.message.reply_text("❌ End time must be after start time. Send END time again.")
                        return

                    # Perform trim
                    await update.message.reply_text(f"✂️ Trimming from {start} to {user_input}...")
                    output_path = f"storage/output/{current_file['id']}_trim_{int(start_s)}_{int(end_s)}.mp4"
                    success = await self.converter.trim_video(current_file["path"], output_path, start, user_input.strip())

                    if success and os.path.exists(output_path):
                        with open(output_path, "rb") as vf:
                            await context.bot.send_video(
                                chat_id=update.effective_chat.id,
                                video=vf,
                                caption=f"✅ Trimmed {start} to {user_input}",
                                supports_streaming=True,
                            )
                        os.remove(output_path)
                    else:
                        await update.message.reply_text("❌ Failed to trim video.")

                    # Cleanup state
                    for k in list(context.user_data.keys()):
                        if k.startswith("awaiting_") or k.startswith("trimmer_") or k.startswith("trimmer"):
                            del context.user_data[k]
                    return

                elif mode == "trimmer2_start":
                    # Save start and ask for duration
                    _ = _parse_time_to_seconds(user_input)
                    context.user_data["trimmer_start"] = user_input.strip()
                    context.user_data["awaiting_trimmer"] = "trimmer2_duration"
                    await update.message.reply_text(
                        "📥 Start time saved. Now send DURATION (HH:MM:SS[.ms] or seconds)\nExample: 00:10:00"
                    )
                    return

                elif mode == "trimmer2_duration":
                    start = context.user_data.get("trimmer_start")
                    if not start:
                        await update.message.reply_text("❌ Missing start time. Please restart Trimmer.")
                        for k in list(context.user_data.keys()):
                            if k.startswith("awaiting_") or k.startswith("trimmer_"):
                                del context.user_data[k]
                        return

                    try:
                        start_s = _parse_time_to_seconds(start)
                        dur_s = _parse_time_to_seconds(user_input)
                        end_s = start_s + dur_s
                        end_str = _format_seconds_to_hhmmss(end_s)
                    except ValueError:
                        await update.message.reply_text("❌ Invalid duration format. Use HH:MM:SS or seconds.")
                        return

                    await update.message.reply_text(f"✂️ Trimming from {start} for duration {user_input} (to {end_str})...")
                    output_path = f"storage/output/{current_file['id']}_trim_{int(start_s)}_{int(end_s)}.mp4"
                    success = await self.converter.trim_video(current_file["path"], output_path, start, end_str)

                    if success and os.path.exists(output_path):
                        with open(output_path, "rb") as vf:
                            await context.bot.send_video(
                                chat_id=update.effective_chat.id,
                                video=vf,
                                caption=f"✅ Trimmed {start} + {user_input}",
                                supports_streaming=True,
                            )
                        os.remove(output_path)
                    else:
                        await update.message.reply_text("❌ Failed to trim video.")

                    # Cleanup
                    for k in list(context.user_data.keys()):
                        if k.startswith("awaiting_") or k.startswith("trimmer_") or k.startswith("trimmer"):
                            del context.user_data[k]
                    return
            except ValueError as e:
                await update.message.reply_text(str(e))
                return

        # Check what we're waiting for
        if context.user_data.get("awaiting_settings"):
            if user_settings is None:
                await update.message.reply_text("⚠️ Settings backend not available.")
            else:
                cmd = user_input.strip()
                lower = cmd.lower()
                try:
                    if lower.startswith("set prefix:"):
                        val = cmd.split(":", 1)[1].strip()
                        user_settings.set_user_setting(user_id, "prefix", val)
                        await update.message.reply_text(f"✅ Prefix set to: {val}")
                    elif lower.startswith("set suffix:"):
                        val = cmd.split(":", 1)[1].strip()
                        user_settings.set_user_setting(user_id, "suffix", val)
                        await update.message.reply_text(f"✅ Suffix set to: {val}")
                    elif lower.startswith("set upload_mode:"):
                        val = cmd.split(":", 1)[1].strip().lower()
                        if val in ("video", "file", "zip"):
                            user_settings.set_user_setting(user_id, "upload_mode", val)
                            await update.message.reply_text(f"✅ Upload mode set to: {val}")
                        else:
                            await update.message.reply_text("❌ Invalid upload_mode. Choose video|file|zip")
                    elif lower == "toggle save_thumbnail":
                        cur = user_settings.get_user_settings(user_id).get("save_thumbnail", False)
                        user_settings.set_user_setting(user_id, "save_thumbnail", not cur)
                        await update.message.reply_text(f"✅ save_thumbnail set to: {not cur}")
                    elif lower.startswith("set thumb_url:"):
                        val = cmd.split(":", 1)[1].strip()
                        user_settings.set_user_setting(user_id, "default_thumbnail", val)
                        user_settings.set_user_setting(user_id, "save_thumbnail", True)
                        await update.message.reply_text("✅ Default thumbnail saved.")
                    elif lower == "clear_thumb":
                        user_settings.set_user_setting(user_id, "default_thumbnail", None)
                        user_settings.set_user_setting(user_id, "save_thumbnail", False)
                        await update.message.reply_text("✅ Default thumbnail cleared.")
                    elif lower.startswith("add_word:"):
                        word = cmd.split(":", 1)[1].strip()
                        s = user_settings.get_user_settings(user_id)
                        words = list(s.get("words_remove") or [])
                        if word and word not in words:
                            words.append(word)
                            user_settings.set_user_setting(user_id, "words_remove", words)
                            await update.message.reply_text(f"✅ Added word to remove: {word}")
                        else:
                            await update.message.reply_text("⚠️ Word empty or already present.")
                    elif lower.startswith("remove_word:"):
                        word = cmd.split(":", 1)[1].strip()
                        s = user_settings.get_user_settings(user_id)
                        words = list(s.get("words_remove") or [])
                        if word in words:
                            words.remove(word)
                            user_settings.set_user_setting(user_id, "words_remove", words)
                            await update.message.reply_text(f"✅ Removed word: {word}")
                        else:
                            await update.message.reply_text("⚠️ Word not found in list.")
                    elif lower == "list_words":
                        s = user_settings.get_user_settings(user_id)
                        words = s.get("words_remove") or []
                        await update.message.reply_text("Words to remove: " + (", ".join(words) if words else "(none)"))
                    elif lower == "clear_words":
                        user_settings.set_user_setting(user_id, "words_remove", [])
                        await update.message.reply_text("✅ Cleared words remover list.")
                    else:
                        await update.message.reply_text("❓ Unknown settings command. Send /settings for instructions.")
                except Exception:
                    await update.message.reply_text("⚠️ Failed to update settings.")

            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        elif context.user_data.get("awaiting_crf"):
            if user_input.isdigit() and 18 <= int(user_input) <= 51:
                await self.compress_video(update, context, session, user_input)
            else:
                await update.message.reply_text("❌ Invalid CRF. Enter 18-51.")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_resolution"):
            if "x" in user_input:
                try:
                    width, height = map(int, user_input.split("x"))
                    await update.message.reply_text(
                        f"📐 Changing resolution to {width}x{height}..."
                    )

                    output_path = f"storage/output/{current_file['id']}_{width}x{height}.mp4"
                    success = await self.converter.change_resolution(
                        current_file["path"], output_path, width, height
                    )

                    if success and os.path.exists(output_path):
                        with open(output_path, "rb") as video_file:
                            await context.bot.send_video(
                                chat_id=update.effective_chat.id,
                                video=video_file,
                                caption=f"✅ Resolution: {width}x{height}",
                                supports_streaming=True,
                            )
                        os.remove(output_path)
                    else:
                        await update.message.reply_text(
                            "❌ Failed to change resolution."
                        )
                except Exception:
                    logger.exception(
                        "Invalid resolution input while parsing WIDTHxHEIGHT"
                    )
                    await update.message.reply_text(
                        "❌ Invalid format. Use WIDTHxHEIGHT."
                    )
            else:
                await update.message.reply_text(
                    "❌ Invalid format. Use WIDTHxHEIGHT."
                )

            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        elif context.user_data.get("awaiting_caption"):
            # Store caption in session and confirm
            if not current_file:
                await update.message.reply_text("❌ No file in session.")
            else:
                session["current_file"]["caption"] = user_input
                await update.message.reply_text("✅ Caption saved.")
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        elif context.user_data.get("awaiting_rename"):
            if not current_file:
                await update.message.reply_text("❌ No file in session.")
            else:
                # Only change stored name, do not move files on disk here
                session["current_file"]["name"] = user_input
                await update.message.reply_text(f"✅ Filename set to: {user_input}")
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        elif context.user_data.get("awaiting_split"):
            if not current_file or current_file.get("type") != "video":
                await update.message.reply_text("❌ No video available to split.")
            else:
                # Basic placeholder: accept 'start-end' or integer parts
                try:
                    if "-" in user_input:
                        start_s, end_s = user_input.split("-", 1)
                        start = float(start_s.strip())
                        end = float(end_s.strip())
                        await update.message.reply_text(
                            f"✅ Split request queued for {start}s to {end}s. Processing..."
                        )
                        # Try to call converter.split if available
                        try:
                            out = f"storage/output/{current_file['id']}_split_{int(start)}_{int(end)}.mp4"
                            if hasattr(self.converter, "split_video"):
                                success = await self.converter.split_video(current_file["path"], start, end, out)
                                if success and os.path.exists(out):
                                    with open(out, "rb") as vf:
                                        await context.bot.send_video(
                                            chat_id=update.effective_chat.id,
                                            video=vf,
                                            caption="✅ Split part",
                                        )
                                    os.remove(out)
                                else:
                                    await update.message.reply_text("⚠️ Split finished but no file produced.")
                        except Exception:
                            logger.exception("split_video failed")
                    else:
                        parts = int(user_input.strip())
                        await update.message.reply_text(
                            f"✅ Split into {parts} parts queued (placeholder)."
                        )
                except Exception:
                    await update.message.reply_text(
                        "❌ Invalid split format. Use 'start-end' or an integer number of parts."
                    )
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        elif context.user_data.get("awaiting_forward_to"):
            if not current_file:
                await update.message.reply_text("❌ No file to forward.")
            else:
                target = user_input.strip()
                path = current_file.get("path")
                if not path or not os.path.exists(path):
                    await update.message.reply_text("❌ Source file not available on disk.")
                else:
                    # Resolve & validate target chat (username or id)
                    try:
                        # Normalize username (allow with or without @)
                        if target.startswith("@"):
                            lookup = target
                        else:
                            # try integer id first
                            try:
                                lookup = int(target)
                            except Exception:
                                lookup = target

                        # This will raise if bot cannot access the chat or it's invalid
                        dest_chat = await context.bot.get_chat(lookup)
                    except Exception as e:
                        logger.warning("Invalid forward target or inaccessible chat: %s", e)
                        await update.message.reply_text(
                            "❌ Invalid target or bot cannot access that chat/user. "
                            "Provide a numeric chat id or ensure the user has started the bot (use @username)."
                        )
                        for key in list(context.user_data.keys()):
                            if key.startswith("awaiting_"):
                                del context.user_data[key]
                        return

                    # Try sending with validation and robust error handling
                    try:
                        # Choose send method; document is a safer fallback for large files
                        caption = current_file.get("caption", "")

                        if current_file.get("type") == "video":
                            # Prefer send_video; fallback to send_document on failure
                            try:
                                with open(path, "rb") as f:
                                    await context.bot.send_video(chat_id=dest_chat.id, video=f, caption=caption)
                            except Exception:
                                logger.exception("send_video failed, trying send_document as fallback")
                                with open(path, "rb") as f:
                                    await context.bot.send_document(chat_id=dest_chat.id, document=f, caption=caption)

                        elif current_file.get("type") == "audio":
                            try:
                                with open(path, "rb") as f:
                                    await context.bot.send_audio(chat_id=dest_chat.id, audio=f, caption=caption)
                            except Exception:
                                logger.exception("send_audio failed, trying send_document as fallback")
                                with open(path, "rb") as f:
                                    await context.bot.send_document(chat_id=dest_chat.id, document=f, caption=caption)

                        else:
                            with open(path, "rb") as f:
                                await context.bot.send_document(chat_id=dest_chat.id, document=f, caption=caption)

                        await update.message.reply_text("✅ Forwarded file successfully.")
                    except Exception as e:
                        logger.exception("Failed to forward file to %s: %s", getattr(dest_chat, 'id', lookup), e)
                        await update.message.reply_text(f"❌ Failed to forward: {e}")

            # Clear awaiting flag regardless of outcome to avoid stuck state
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
            else:
                await update.message.reply_text(
                    "❌ Invalid format. Use WIDTHxHEIGHT."
                )
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_trim"):
            # Handle trim time input
            context.user_data["trim_time"] = user_input
            if context.user_data["awaiting_trim"] == "start":
                context.user_data["start_time"] = user_input
                context.user_data["awaiting_trim"] = "end"
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
            else:
                # Perform trim
                start_time = context.user_data.get("start_time", "00:00:00")
                end_time = user_input

                await update.message.reply_text(
                    f"✂️ Trimming from {start_time} to {end_time}..."
                )

                if not await self._check_conversion_quota(update, context):
                    return

                output_path = (
                    f"storage/output/{current_file['id']}_trimmed.mp4"
                )
                success = await self.converter.trim_video(
                    current_file["path"], output_path, start_time, end_time
                )

                if success and os.path.exists(output_path):
                    with open(output_path, "rb") as video_file:
                        await context.bot.send_video(
                            chat_id=update.effective_chat.id,
                            video=video_file,
                            caption=f"✅ Trimmed {start_time}-{end_time}",
                            supports_streaming=True,
                        )
                    os.remove(output_path)
                else:
                    await update.message.reply_text("❌ Failed to trim video.")

        elif context.user_data.get("awaiting_screenshot_time"):
            # Handle screenshot time
            await update.message.reply_text(
                f"🖼️ Taking screenshot at {user_input}..."
            )

            output_path = f"storage/output/{current_file['id']}_screenshot.jpg"
            success = await self.converter.take_screenshot_at_time(
                current_file["path"], output_path, user_input
            )

            if success and os.path.exists(output_path):
                with open(output_path, "rb") as photo_file:
                    await context.bot.send_photo(
                        chat_id=update.effective_chat.id,
                        photo=photo_file,
                        caption=f"✅ Screenshot at {user_input}",
                    )
                os.remove(output_path)
            else:
                await update.message.reply_text(
                    "❌ Failed to take screenshot."
                )

        elif context.user_data.get("awaiting_screenshot_count"):
            # Handle multiple screenshots
            if user_input.isdigit() and 2 <= int(user_input) <= 20:
                count = int(user_input)
                await update.message.reply_text(
                    f"🖼️ Taking {count} screenshots..."
                )

                screenshots = await self.converter.take_screenshot_grid(
                    current_file["path"],
                    f"storage/output/{current_file['id']}_grid",
                    count,
                )

                if screenshots:
                    # Send as album
                    media_group = []
                    for i, screenshot_path in enumerate(screenshots):
                        with open(screenshot_path, "rb") as photo_file:
                            media_group.append(
                                InputMediaPhoto(
                                    photo_file,
                                    caption=(
                                        f"Screenshot {i+1}" if i == 0 else ""
                                    ),
                                )
                            )

                    await context.bot.send_media_group(
                        chat_id=update.effective_chat.id, media=media_group
                    )

                    # Cleanup
                    for screenshot_path in screenshots:
                        if os.path.exists(screenshot_path):
                            os.remove(screenshot_path)
                else:
                    await update.message.reply_text(
                        "❌ Failed to take screenshots."
                    )
            else:
                await update.message.reply_text("❌ Enter number 2-20.")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_framerate"):
            # Handle custom framerate input
            try:
                fps = float(user_input)
                await update.message.reply_text(
                    f"⏱️ Changing framerate to {fps} fps..."
                )
                output_path = (
                    f"storage/output/{current_file['id']}_fr_{int(fps)}.mp4"
                )
                success = await self.converter.change_framerate(
                    current_file["path"], output_path, fps
                )

                if success and os.path.exists(output_path):
                    with open(output_path, "rb") as video_file:
                        await context.bot.send_video(
                            chat_id=update.effective_chat.id,
                            video=video_file,
                            caption=f"✅ Framerate changed to {fps} fps",
                            supports_streaming=True,
                        )
                    os.remove(output_path)
                else:
                    await update.message.reply_text(
                        "❌ Failed to change framerate."
                    )
            except Exception:
                await update.message.reply_text(
                    "❌ Invalid FPS value. Use a number like 24 or 29.97."
                )
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_bitrate"):
            # Handle custom bitrate
            if user_input.endswith("k"):
                await self.adjust_bitrate(update, context, session, user_input)
            else:
                await update.message.reply_text(
                    "❌ Invalid bitrate. Use format like 128k, 320k."
                )
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_optimize"):
            # Handle custom optimization
            try:
                preset, crf, bitrate = user_input.split(",")
                await update.message.reply_text(
                    f"⚡ Optimizing with preset={preset}, crf={crf}, bitrate={bitrate}..."
                )

                output_path = (
                    f"storage/output/{current_file['id']}_optimized.mp4"
                )
                cmd = [
                    "-c:v",
                    "libx264",
                    "-preset",
                    preset.strip(),
                    "-crf",
                    crf.strip(),
                    "-c:a",
                    "aac",
                    "-b:a",
                    bitrate.strip(),
                    "-movflags",
                    "+faststart",
                ]

                success, _ = await self.converter.execute_ffmpeg(
                    cmd, current_file["path"], output_path
                )

                if success and os.path.exists(output_path):
                    with open(output_path, "rb") as video_file:
                        await context.bot.send_video(
                            chat_id=update.effective_chat.id,
                            video=video_file,
                            caption="✅ Custom optimization",
                            supports_streaming=True,
                        )
                    os.remove(output_path)
                else:
                    await update.message.reply_text("❌ Optimization failed.")
            except Exception:
                logger.exception(
                    "Invalid custom optimize input; expected preset,crf,bitrate"
                )
                await update.message.reply_text(
                    "❌ Invalid format. Use: preset,crf,bitrate"
                )
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        elif context.user_data.get("awaiting_metadata"):
            # Handle metadata JSON
            try:
                metadata = json.loads(user_input)
                output_path = (
                    f"storage/output/{current_file['id']}_with_metadata.mp4"
                )

                success = await self.converter.edit_metadata(
                    current_file["path"], output_path, metadata
                )

                if success and os.path.exists(output_path):
                    with open(output_path, "rb") as video_file:
                        await context.bot.send_video(
                            chat_id=update.effective_chat.id,
                            video=video_file,
                            caption="✅ Metadata updated",
                            supports_streaming=True,
                        )
                    os.remove(output_path)
                else:
                    await update.message.reply_text(
                        "❌ Failed to update metadata."
                    )
            except json.JSONDecodeError:
                await update.message.reply_text("❌ Invalid JSON format.")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]

        # Clear context
        for key in list(context.user_data.keys()):
            if key.startswith("awaiting_"):
                del context.user_data[key]

        return ConversationHandler.END
