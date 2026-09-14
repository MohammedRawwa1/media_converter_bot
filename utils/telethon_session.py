import asyncio
import contextlib
import json
import logging
import os
import threading
import time

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except Exception:  # pragma: no cover - optional dependency
    TelegramClient = None
    StringSession = None

try:
    from pyrogram import Client as PyrogramClient
except Exception:  # pragma: no cover - optional dependency
    PyrogramClient = None

logger = logging.getLogger(__name__)


def _get_env_value(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value.strip()
    return None


def get_telethon_session_name() -> str:
    return (
        _get_env_value(
            "API_SESSION_NAME",
            "SESSION_NAME",
            "USERBOT_SESSION_NAME",
            "TELETHON_SESSION_NAME",
        )
        or "userbot_session"
    )


def get_telethon_session_dir() -> str:
    return _get_env_value("TELETHON_SESSION_DIR") or os.getenv("TEMP_PATH") or os.getcwd()


def get_telethon_session_path() -> str:
    session_dir = get_telethon_session_dir()
    with contextlib.suppress(Exception):
        os.makedirs(session_dir, exist_ok=True)
    return os.path.join(session_dir, get_telethon_session_name())


# ── JSON file persistence bridge ─────────────────────────────────
#
# When a session is used via ``StringSession`` / Pyrogram in-memory,
# Telegram's client library does **not** create a file on disk that can
# be reused on restart.  If the env var is lost, there is no fallback.
#
# To bridge this gap, the session healthchecker periodically writes the
# live session strings to a shared JSON file at the same path as the
# Telethon session name.  Both ``telethon_session`` and ``pyrogram_session``
# are stored in the same file under separate keys.
# ------------------------------------------------------------------

# Keys used inside the JSON dict
_KEY_TELETHON = "telethon_session"
_KEY_PYROGRAM = "pyrogram_session"

# ── In-memory cache for session file reads ────────────────────────
#
# Both Telethon and Pyrogram checks in the healthchecker read JSON
# files.  To avoid redundant disk I/O within a single cycle, the
# file contents are cached in-memory with a short TTL.  The cache is
# invalidated whenever a write occurs.
#
# The cache is keyed by a string identifier: ``"__global__"`` for the
# legacy shared file, or ``str(user_id)`` for per-user files.
#
# A ``threading.Lock`` protects access to the module-level globals
# because the cache functions are called from thread pool workers
# (via ``asyncio.to_thread``) when the async readers are used.
# ------------------------------------------------------------------
_SESSION_CACHE_DATA: dict[str, dict] = {}
_SESSION_CACHE_EXPIRES: dict[str, float] = {}
_SESSION_CACHE_TTL = 60  # seconds
_SESSION_CACHE_LOCK = threading.Lock()
_GLOBAL_CACHE_KEY = "__global__"


def _cache_key(user_id: int | None = None) -> str:
    """Return the in-memory cache key for a given user_id."""
    if user_id is not None:
        return str(user_id)
    return _GLOBAL_CACHE_KEY


# ── In-memory cache for MongoDB-resolved Pyrogram session ───────────
#
# ``get_pyrogram_session_string()`` is synchronous and cannot call
# ``db_model.load_session()`` directly.  To still allow the sync path
# to find MongoDB-persisted sessions, we maintain a simple in-memory
# cache that is populated by the async
# ``get_pyrogram_session_string_for_user()`` whenever it successfully
# loads a session from MongoDB.
#
# This cache is NOT TTL-based; it is invalidated explicitly whenever
# the session is cleared (e.g. via ``/logoutpyro``) or when a new save
# replaces the old value.  The cache is a single string because there
# is at most one active Pyrogram session (admin user).
# --------------------------------------------------------------------
_PYROGRAM_MONGO_CACHE: str | None = None
_PYROGRAM_MONGO_CACHE_LOCK = threading.Lock()


def _get_cached_sessions(user_id: int | None = None) -> dict | None:
    """Return cached session dict for a given user if still fresh, else None."""
    k = _cache_key(user_id)
    with _SESSION_CACHE_LOCK:
        entry = _SESSION_CACHE_DATA.get(k)
        expires = _SESSION_CACHE_EXPIRES.get(k, 0.0)
        if entry is not None and time.time() < expires:
            return entry
        return None


def _set_cached_sessions(data: dict, user_id: int | None = None):
    """Cache session data with the module-level TTL for a given user."""
    k = _cache_key(user_id)
    with _SESSION_CACHE_LOCK:
        _SESSION_CACHE_DATA[k] = data
        _SESSION_CACHE_EXPIRES[k] = time.time() + _SESSION_CACHE_TTL


def _invalidate_session_cache(user_id: int | None = None):
    """Clear the in-memory cache for a given user after a write.

    When ``user_id`` is ``None``, clears ALL caches (global + per-user).
    """
    with _SESSION_CACHE_LOCK:
        if user_id is not None:
            k = _cache_key(user_id)
            _SESSION_CACHE_DATA.pop(k, None)
            _SESSION_CACHE_EXPIRES.pop(k, None)
        else:
            _SESSION_CACHE_DATA.clear()
            _SESSION_CACHE_EXPIRES.clear()


def _clear_pyrogram_mongo_cache():
    """Clear the in-memory MongoDB Pyrogram session cache."""
    with _PYROGRAM_MONGO_CACHE_LOCK:
        global _PYROGRAM_MONGO_CACHE
        _PYROGRAM_MONGO_CACHE = None


def _set_pyrogram_mongo_cache(session_str: str):
    """Set the in-memory MongoDB Pyrogram session cache."""
    with _PYROGRAM_MONGO_CACHE_LOCK:
        global _PYROGRAM_MONGO_CACHE
        _PYROGRAM_MONGO_CACHE = session_str


def _get_pyrogram_mongo_cache() -> str | None:
    """Return the cached MongoDB Pyrogram session, if any."""
    with _PYROGRAM_MONGO_CACHE_LOCK:
        return _PYROGRAM_MONGO_CACHE


def _get_persisted_session_path(user_id: int | None = None) -> str:
    """Return the path to the JSON file used for session string persistence.

    When ``user_id`` is provided, the file is scoped to that user
    (``telethon_ingest.{user_id}.session.json``) enabling per-phone
    session isolation.

    When ``user_id`` is ``None``, the legacy shared file is returned
    (``telethon_ingest.session.json``), preserving backward compatibility
    with existing single-phone deployments.
    """
    base = get_telethon_session_path() + ".session"
    if user_id is not None:
        return f"{base}.{user_id}.json"
    return base + ".json"


def _load_all_sessions_from_file(user_id: int | None = None) -> dict:
    """Read the full persisted JSON dict from disk (synchronous).

    When ``user_id`` is provided, the per-user JSON file is read.
    When ``user_id`` is ``None``, the legacy global file is read.

    Returns a dict (possibly empty) on success, or an empty dict on failure.

    Results are cached in-memory for ``_SESSION_CACHE_TTL`` seconds to
    avoid redundant reads within a single healthcheck cycle.

    For async contexts, prefer ``_load_all_sessions_from_file_async``
    which runs the I/O in a thread to avoid blocking the event loop.
    """
    # Check in-memory cache first to avoid redundant disk I/O
    cached = _get_cached_sessions(user_id=user_id)
    if cached is not None:
        return cached

    path = _get_persisted_session_path(user_id=user_id)
    if not os.path.exists(path):
        _set_cached_sessions({}, user_id=user_id)
        return {}
    try:
        with open(path) as f:
            data = json.load(f)
        result = data if isinstance(data, dict) else {}
        _set_cached_sessions(result, user_id=user_id)
        return result
    except Exception as exc:
        logger.debug("session: failed to read persisted session file %s: %s", path, exc)
        # DO NOT cache the empty result here!  A transient read error (e.g. temporary
        # file lock, incomplete write from another process, or a JSON decode glitch)
        # would otherwise poison the in-memory cache with {} for the next 60 seconds.
        # Any call to ``save_session_string_to_file`` during that window would then
        # read the cached {}, update only one key, and silently drop the other — which
        # is exactly how ``telethon_session`` kept disappearing from the JSON file.
        return {}


async def _load_all_sessions_from_file_async(user_id: int | None = None) -> dict:
    """Async version of ``_load_all_sessions_from_file``.

    Runs the sync file I/O in a thread via ``asyncio.to_thread`` so the
    event loop is not blocked during disk reads.  Intended for callers
    in async contexts (healthchecker).
    """
    return await asyncio.to_thread(_load_all_sessions_from_file, user_id)


def save_session_string_to_file(session_str: str, client_type: str = "telethon", user_id: int | None = None) -> bool:
    """Persist a session string to a JSON file (synchronous).

    Both Telethon and Pyrogram session strings are stored in the same file
    under different keys (``telethon_session`` / ``pyrogram_session``).
    The ``client_type`` parameter determines which key is updated.

    When ``user_id`` is provided, the file is scoped to that user
    (per-phone isolation).  When ``user_id`` is ``None``, the legacy
    shared file is used (backward-compatible).

    Best-effort: returns True on success, False on failure (logged).

    For async contexts, prefer ``save_session_string_to_file_async``
    which runs the I/O in a thread to avoid blocking the event loop.

    Important
    ---------
    This function reads existing session data **directly from disk**,
    bypassing the in-memory ``_SESSION_CACHE``.  This avoids a subtle
    clobbering bug: if ``_load_all_sessions_from_file`` cached ``{}``
    after a transient read error, a subsequent write here would
    silently drop the other session key written by the other client.
    """
    path = _get_persisted_session_path(user_id=user_id)
    try:
        # Read existing data directly from disk, bypassing the in-memory cache.
        # Using the cache here is dangerous: if a transient read error poisoned
        # the cache with {}, we would silently drop the other session key.
        existing = {}
        if os.path.exists(path):
            try:
                with open(path) as f:
                    _raw = json.load(f)
                if isinstance(_raw, dict):
                    existing = _raw
            except Exception as exc:
                logger.debug(
                    "session: failed to read existing data from %s before write: %s",
                    path,
                    exc,
                )
        key = _KEY_TELETHON if client_type == "telethon" else _KEY_PYROGRAM
        existing[key] = session_str

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(existing, f)
        logger.info(
            "session: persisted %s session string to %s (%d chars)",
            client_type,
            path,
            len(session_str),
        )
        # Invalidate in-memory cache so subsequent reads see the new data
        _invalidate_session_cache(user_id=user_id)
        # If we just cleared the Pyrogram session, also clear the MongoDB cache
        if client_type == "pyrogram" and not session_str:
            _clear_pyrogram_mongo_cache()
        return True
    except Exception as exc:
        logger.debug(
            "session: failed to persist %s session string to %s: %s",
            client_type,
            path,
            exc,
        )
        return False


async def save_session_string_to_file_async(
    session_str: str, client_type: str = "telethon", user_id: int | None = None
) -> bool:
    """Async version of ``save_session_string_to_file``.

    Runs the sync file I/O in a thread via ``asyncio.to_thread`` so the
    event loop is not blocked during disk writes.  Intended for callers
    in async contexts (healthchecker, login flow).
    """
    return await asyncio.to_thread(
        save_session_string_to_file,
        session_str,
        client_type=client_type,
        user_id=user_id,
    )


def _load_session_string_from_file(client_type: str = "telethon", user_id: int | None = None) -> str | None:
    """Load a session string previously persisted by the healthchecker (synchronous).

    When ``user_id`` is provided, reads from the per-user JSON file.
    When ``user_id`` is ``None``, reads from the legacy global file.

    Parameters
    ----------
    client_type:
        ``"telethon"`` (default) or ``"pyrogram"``.

    Returns the string or None if the file is missing or the key not found.

    For async contexts, prefer ``_load_session_string_from_file_async``
    which runs the I/O in a thread to avoid blocking the event loop.
    """
    data = _load_all_sessions_from_file(user_id=user_id)
    if not data:
        return None
    key = _KEY_TELETHON if client_type == "telethon" else _KEY_PYROGRAM
    session_str = data.get(key)
    if session_str:
        logger.info(
            "session: loaded %s session string from %s (%d chars)",
            client_type,
            _get_persisted_session_path(user_id=user_id),
            len(session_str),
        )
        return session_str
    return None


async def _load_session_string_from_file_async(client_type: str = "telethon", user_id: int | None = None) -> str | None:
    """Async version of ``_load_session_string_from_file``.

    Runs the sync file I/O in a thread via ``asyncio.to_thread`` so the
    event loop is not blocked during disk reads.  Intended for callers
    in async contexts (healthchecker).
    """
    data = await _load_all_sessions_from_file_async(user_id=user_id)
    if not data:
        return None
    key = _KEY_TELETHON if client_type == "telethon" else _KEY_PYROGRAM
    session_str = data.get(key)
    if session_str:
        logger.info(
            "session: loaded %s session string from %s (%d chars)",
            client_type,
            _get_persisted_session_path(user_id=user_id),
            len(session_str),
        )
        return session_str
    return None


def _get_configured_session_string(user_id: int | None = None) -> str | None:
    """Return a Telethon session string, preferring persisted over env.

    Resolution order (matches the reference header-extractor flow):
    1. Per-user JSON file — a session written by ``/login`` or the healthcheck
       must outrank a possibly stale environment session string.
    2. Environment variable (admin-configured, e.g. ``API_SESSION``).
    3. Legacy global JSON file — only when no ``user_id`` is scoped, so one
       user's session is never served to another.
    """
    # 1. Per-user JSON file first (freshest persisted session)
    if user_id is not None:
        file_str = _load_session_string_from_file(client_type="telethon", user_id=user_id)
        if file_str:
            return file_str

    # 2. Environment variable (admin-configured fallback)
    env_str = _get_env_value(
        "API_SESSION",
        "SESSION",
        "api_session",
        "USERBOT_SESSION",
        "userbot_session",
        "TELETHON_SESSION",
        "telethon_session",
    )
    if env_str:
        return env_str

    # 3. Legacy global JSON file (unscoped/admin path only)
    if user_id is None:
        file_str = _load_session_string_from_file(client_type="telethon")
        if file_str:
            return file_str

    return None


def _extract_session_value(saved_session: object, keys: tuple[str, ...]) -> str | None:
    """Return the first non-empty session string from a stored MongoDB document.

    ``keys`` is ordered by preference; the first key holding a truthy value wins.
    """
    if not isinstance(saved_session, dict):
        return None
    for key in keys:
        value = saved_session.get(key)
        if value:
            return str(value)
    return None


# ── Registered MongoDB model ────────────────────────────────────────
#
# The downloader/uploader run in code paths that only receive a ``user_id``
# (a conversion worker/task, not a PTB handler holding ``application.bot_data``),
# so they cannot look up ``bot_data["db_model"]`` themselves.  Registering the
# model once at startup lets every session-resolution path consult MongoDB
# instead of relying solely on the per-user JSON files.
_REGISTERED_DB_MODEL: object | None = None


def set_db_model(db_model: object | None) -> None:
    """Register the bot's MongoDB model so session resolution can reach MongoDB."""
    global _REGISTERED_DB_MODEL
    _REGISTERED_DB_MODEL = db_model


def get_db_model() -> object | None:
    """Return the model registered via :func:`set_db_model`, if any."""
    return _REGISTERED_DB_MODEL


async def _load_mongo_session(db_model: object | None, user_id: int | None) -> object | None:
    """Best-effort load of a user's stored session document from MongoDB.

    Prefers ``db_model.load_session()`` (the bot's ``MediaConversionModel``) and
    falls back to the reference repo's ``utils.db.get_user_session()`` when that
    module is available.  When ``db_model`` is ``None`` the model registered via
    :func:`set_db_model` is used, so callers that only hold a ``user_id`` still
    reach MongoDB. Returns ``None`` on any failure.
    """
    if user_id is None:
        return None
    if db_model is None:
        db_model = get_db_model()
    if db_model is None:
        return None
    if hasattr(db_model, "load_session"):
        try:
            if hasattr(db_model, "load_sessions"):
                return await db_model.load_sessions(user_id)
            return await db_model.load_session(user_id)
        except Exception as exc:
            logger.warning("Failed to inspect MongoDB session for user %s: %s", user_id, exc)
            return None
    try:
        from utils.db import get_user_session  # noqa: PLC0415

        return await get_user_session(user_id)
    except Exception as exc:
        logger.warning("Failed to inspect MongoDB session for user %s: %s", user_id, exc)
        return None


async def _resolve_telethon_session_with_source(
    user_id: int | None = None, db_model: object | None = None
) -> tuple[str | None, str]:
    """Resolve a Telethon session string and label the source it came from.

    Resolution order (the reference header-extractor flow):
    1. ``json``         per-user JSON file — freshest, written on login/healthcheck
    2. ``mongodb``      per-user session document
    3. ``env``          ``API_SESSION`` / ``TELETHON_SESSION`` env var
    4. ``global-json``  legacy shared JSON file (unscoped lookups only)

    Persisted sessions deliberately outrank the environment variable: a stale
    env session string must never mask a freshly logged-in session (that is what
    produced ``AUTH_KEY_UNREGISTERED`` after a successful re-login).
    """
    # 1. Per-user JSON file (strictly scoped to this user)
    if user_id is not None:
        file_str = _load_session_string_from_file(client_type="telethon", user_id=user_id)
        if file_str:
            return file_str, "json"

    # 2. MongoDB-persisted session for the given user
    saved_session = await _load_mongo_session(db_model, user_id)
    session_value = _extract_session_value(saved_session, ("telethon_session", "string_session", "session_string"))
    if session_value:
        logger.info("session: loaded Telethon session string from MongoDB for user %s", user_id)
        return session_value, "mongodb"

    # 3. Environment variable
    env_str = _get_env_value(
        "API_SESSION",
        "SESSION",
        "api_session",
        "USERBOT_SESSION",
        "userbot_session",
        "TELETHON_SESSION",
        "telethon_session",
    )
    if env_str:
        return env_str, "env"

    # 4. Legacy global JSON file — unscoped lookups only (isolation safety)
    if user_id is None:
        file_str = _load_session_string_from_file(client_type="telethon")
        if file_str:
            return file_str, "global-json"

    logger.debug("session: no Telethon session string found for user %s", user_id)
    return None, "missing"


async def get_telethon_session_string_for_user(
    user_id: int | None = None, db_model: object | None = None
) -> str | None:
    """Return a usable Telethon session string for the given user, if available.

    Delegates to :func:`_resolve_telethon_session_with_source` so a freshly
    logged-in per-user or MongoDB session always outranks a stale env var.  The
    legacy global JSON file is only consulted for unscoped lookups, so user A's
    session can never be served to user B in a multi-user bot.
    """
    value, _source = await _resolve_telethon_session_with_source(user_id, db_model)
    return value


async def get_telethon_session_status(user_id: int | None = None, db_model: object | None = None) -> dict:
    """Return a diagnostic summary for Telethon session availability.

    Reports the same sources the bot can actually use for login fallback:
    - the per-user JSON file
    - a MongoDB-persisted session for a specific user
    - an explicit session string in env vars
    - a local .session file on disk
    """
    session_path = get_telethon_session_path()
    session_str, source = await _resolve_telethon_session_with_source(user_id=user_id, db_model=db_model)

    if session_str:
        details = {
            "json": "Telethon session persisted in the per-user JSON file",
            "mongodb": "Telethon session persisted in MongoDB",
            "env": "Telethon session string configured in environment",
            "global-json": "Telethon session persisted in the legacy global JSON file",
        }.get(source, "Telethon session string available")
        return {"ready": True, "source": source, "session_path": session_path, "details": details}

    if os.path.exists(session_path) or os.path.exists(session_path + ".session"):
        return {
            "ready": True,
            "source": "file",
            "session_path": session_path,
            "details": "Telethon session file exists on disk",
        }

    return {
        "ready": False,
        "source": "missing",
        "session_path": session_path,
        "details": "No Telethon session configured or persisted",
    }


def build_telethon_client(api_id: int, api_hash: str, session_str: str | None = None):
    """Build a Telethon client with session persistence.

    Session resolution order:
    1. ``session_str`` parameter (explicit call-site override, e.g. from MongoDB)
    2. ``_get_configured_session_string()``: per-user JSON file, then
       ``TELETHON_SESSION`` / ``API_SESSION`` env var, then the global JSON file
    3. File-based ``.session`` file on disk (persistent, auto-saved by Telethon)

    When a StringSession is explicitly configured but fails to load, the
    function falls back to a file-based session.  File-based sessions are
    automatically saved by Telethon on every state change, keeping them alive
    across restarts until the device is manually revoked from Telegram.

    Timeout/retry parameters are read from environment variables:
      - ``TELETHON_TIMEOUT`` (default 120): per-request timeout in seconds.
      - ``TELETHON_REQUEST_RETRIES`` (default 10): retries on request failure.
      - ``TELETHON_CONNECTION_RETRIES`` (default 5): retries on connection failure.
      - ``TELETHON_RETRY_DELAY`` (default 3): seconds between retries.
    """
    if TelegramClient is None:
        raise RuntimeError("Telethon is not installed. Install telethon to use userbot fallback.")

    # Read timeout/retry configuration from env vars (tuned for large-file downloads)
    try:
        _timeout = int(os.getenv("TELETHON_TIMEOUT", "120"))
    except (TypeError, ValueError):
        _timeout = 120
    try:
        _req_retries = int(os.getenv("TELETHON_REQUEST_RETRIES", "10"))
    except (TypeError, ValueError):
        _req_retries = 10
    try:
        _conn_retries = int(os.getenv("TELETHON_CONNECTION_RETRIES", "5"))
    except (TypeError, ValueError):
        _conn_retries = 5
    try:
        _retry_delay = int(os.getenv("TELETHON_RETRY_DELAY", "3"))
    except (TypeError, ValueError):
        _retry_delay = 3

    # Resolve session: explicit parameter > env var > file-based fallback
    resolved_session = session_str or _get_configured_session_string()

    if resolved_session:
        if StringSession is None:
            raise RuntimeError(
                "Telethon StringSession is not available but a session string "
                "is provided. Ensure telethon is installed."
            )
        try:
            logger.info(
                "session: building Telethon client with StringSession (%d chars)",
                len(resolved_session),
            )
            return TelegramClient(
                StringSession(resolved_session),
                api_id,
                api_hash,
                timeout=_timeout,
                request_retries=_req_retries,
                connection_retries=_conn_retries,
                retry_delay=_retry_delay,
            )
        except Exception:
            logger.exception(
                "session: StringSession failed to load; falling back to file-based session at %s.session",
                get_telethon_session_path(),
            )
            # Fall through to file-based session below

    # No session string or StringSession failed — use file-based session.
    # File-based .session files are automatically saved by Telethon on state
    # changes, making them persistent across restarts until the device is
    # manually revoked from Telegram.
    session_path = get_telethon_session_path()
    logger.info(
        "session: building Telethon client with file-based session at %s.session",
        session_path,
    )
    return TelegramClient(
        session_path,
        api_id,
        api_hash,
        timeout=_timeout,
        request_retries=_req_retries,
        connection_retries=_conn_retries,
        retry_delay=_retry_delay,
    )


def get_pyrogram_session_string(user_id: int | None = None) -> str | None:
    """Return a Pyrogram session string, preferring persisted over env.

    Resolution order (matches ``_resolve_pyrogram_session_with_source``):
    1. Per-user JSON file (freshest persisted session)
    2. Environment variable (``PYROGRAM_SESSION``, admin-configured)
    3. In-memory MongoDB cache / legacy global JSON file (unscoped only)

    The legacy global JSON file is only consulted when ``user_id`` is ``None``,
    so one user's session is never served to another.
    """
    # 1. Per-user JSON file first (freshest persisted session)
    if user_id is not None:
        file_str = _load_session_string_from_file(client_type="pyrogram", user_id=user_id)
        if file_str:
            return file_str

    # 2. Environment variable (admin-configured fallback)
    env_str = _get_env_value(
        "PYROGRAM_SESSION",
        "pyrogram_session",
        "USERBOT_PYROGRAM_SESSION",
        "userbot_pyrogram_session",
    )
    if env_str:
        return env_str

    # 3. Unscoped-only fallbacks: in-memory MongoDB cache, then global JSON
    if user_id is None:
        mongo_str = _get_pyrogram_mongo_cache()
        if mongo_str:
            return mongo_str
        return _load_session_string_from_file(client_type="pyrogram")

    return None


async def _resolve_pyrogram_session_with_source(
    user_id: int | None = None, db_model: object | None = None
) -> tuple[str | None, str]:
    """Resolve a Pyrogram session string and label the source it came from.

    Resolution order (the reference header-extractor flow):
    1. ``json``          per-user JSON file — freshest, written on login/healthcheck
    2. ``mongodb``       per-user session document (also cached for the sync path)
    3. ``env``           ``PYROGRAM_SESSION`` env var
    4. ``mongodb-cache`` / ``global-json`` — unscoped lookups only

    Persisted sessions deliberately outrank the environment variable so a stale
    env session string cannot mask a freshly logged-in session.
    """
    # 1. Per-user JSON file (strictly scoped to this user)
    if user_id is not None:
        file_str = _load_session_string_from_file(client_type="pyrogram", user_id=user_id)
        if file_str:
            return file_str, "json"

    # 2. MongoDB-persisted session for the given user
    saved_session = await _load_mongo_session(db_model, user_id)
    session_value = _extract_session_value(saved_session, ("pyrogram_session",))
    if session_value:
        logger.info("session: loaded Pyrogram session string from MongoDB for user %s", user_id)
        # Cache it so the synchronous ``get_pyrogram_session_string()`` can use it.
        _set_pyrogram_mongo_cache(session_value)
        return session_value, "mongodb"

    # 3. Environment variable
    env_str = _get_env_value(
        "PYROGRAM_SESSION",
        "pyrogram_session",
        "USERBOT_PYROGRAM_SESSION",
        "userbot_pyrogram_session",
    )
    if env_str:
        return env_str, "env"

    # 4. Unscoped-only fallbacks (isolation safety)
    if user_id is None:
        cached = _get_pyrogram_mongo_cache()
        if cached:
            return cached, "mongodb-cache"
        file_str = _load_session_string_from_file(client_type="pyrogram")
        if file_str:
            return file_str, "global-json"

    logger.debug("session: no Pyrogram session string found for user %s", user_id)
    return None, "missing"


async def get_pyrogram_session_string_for_user(
    user_id: int | None = None, db_model: object | None = None
) -> str | None:
    """Return a usable Pyrogram session string for the given user, if available.

    Delegates to :func:`_resolve_pyrogram_session_with_source` so a freshly
    logged-in per-user or MongoDB session outranks a stale env var.  The legacy
    global JSON file is only consulted for unscoped lookups.  When a session is
    found in MongoDB it is also cached in-memory via ``_set_pyrogram_mongo_cache``
    so the sync ``get_pyrogram_session_string()`` can benefit from it without an
    async MongoDB call.
    """
    value, _source = await _resolve_pyrogram_session_with_source(user_id, db_model)
    return value


async def resolve_session_string(
    client_type: str,
    session_str: str | None = None,
    user_id: int | None = None,
    db_model: object | None = None,
) -> str | None:
    """Resolve a session string for either client, honouring an explicit override."""
    if session_str is not None:
        return session_str
    if client_type == "telethon":
        return await get_telethon_session_string_for_user(user_id=user_id, db_model=db_model)
    if client_type == "pyrogram":
        return await get_pyrogram_session_string_for_user(user_id=user_id, db_model=db_model)
    raise ValueError(f"Unknown client_type: {client_type!r}")


async def restore_per_user_session_files(db_model: object | None = None) -> int:
    """Re-materialize per-user JSON session files from the durable store.

    The per-user JSON session files live on an ephemeral filesystem and are
    wiped on every redeploy.  This walks the MongoDB session documents and
    rewrites each user's JSON file so sessions created via ``/login`` or
    ``/loginpyro`` keep working immediately after a deployment instead of
    waiting for the healthchecker to notice.

    ``db_model`` is the bot's ``MediaConversionModel`` (or any object exposing a
    ``_sessions_coll`` raw collection / ``sessions`` QueryBuilder).  Only session
    keys missing from a user's local JSON file are written, so a fresher local
    file is never clobbered by an older MongoDB document.  Returns the number of
    users restored; best-effort and never raises.
    """
    if db_model is None:
        return 0

    restored = 0
    try:
        # Prefer the raw motor collection (supports a plain find over all docs).
        coll = getattr(db_model, "_sessions_coll", None)
        if coll is None or not hasattr(coll, "find"):
            qb = getattr(db_model, "sessions", None)
            coll = getattr(qb, "collection", None)
        if coll is None or not hasattr(coll, "find"):
            logger.debug("session: no MongoDB sessions collection available to restore from")
            return 0

        query: dict = {}
        bot_id = getattr(db_model, "bot_id", None)
        if bot_id is not None:
            query["bot_id"] = bot_id

        projection = {"_id": 0, "user_id": 1, "session": 1}
        cursor = coll.find(query, projection).sort("updated_at", -1)
        docs = await cursor.to_list(length=None)

        sessions_by_user: dict[int, dict[str, str]] = {}
        for doc in docs or []:
            if not isinstance(doc, dict):
                continue
            uid = doc.get("user_id")
            try:
                uid = int(uid)
            except (TypeError, ValueError):
                continue
            sess = doc.get("session")
            if not isinstance(sess, dict):
                continue

            tele = sess.get("telethon_session")
            pyro = sess.get("pyrogram_session")
            if not tele and not pyro:
                # Legacy documents that predate the typed-key split.
                tele = sess.get("string_session")
            if not tele and not pyro:
                continue

            # Sessions are stored per (user_id, phone). Merge all phone documents
            # so Telethon and Pyrogram, or two separately logged-in accounts, do
            # not cause the later document to hide keys from the earlier one.
            merged = sessions_by_user.setdefault(uid, {})
            if tele and "telethon_session" not in merged:
                merged["telethon_session"] = str(tele)
            if pyro and "pyrogram_session" not in merged:
                merged["pyrogram_session"] = str(pyro)

        for uid, merged in sessions_by_user.items():
            tele = merged.get("telethon_session")
            pyro = merged.get("pyrogram_session")

            # Only fill keys that are MISSING locally.  On a persistent volume the
            # local JSON can be fresher than MongoDB (e.g. a login whose Mongo
            # write failed), and clobbering it would reintroduce the very
            # "not authorized" breakage this restore exists to prevent.
            local = await _load_all_sessions_from_file_async(user_id=uid)
            local = local if isinstance(local, dict) else {}

            written = False
            if tele and not local.get(_KEY_TELETHON):
                written = (
                    await save_session_string_to_file_async(str(tele), client_type="telethon", user_id=uid)
                ) or written
            if pyro and not local.get(_KEY_PYROGRAM):
                written = (
                    await save_session_string_to_file_async(str(pyro), client_type="pyrogram", user_id=uid)
                ) or written
            if written:
                restored += 1

        if restored:
            logger.info(
                "session: restored per-user JSON session files for %d user(s) from MongoDB",
                restored,
            )
        return restored
    except Exception as exc:
        logger.debug("session: restore per-user session files from MongoDB failed: %s", exc)
        return 0


def build_pyrogram_client(api_id: int, api_hash: str, session_str: str | None = None) -> object | None:
    """Build a Pyrogram client from a session string (sync).

    Parameters
    ----------
    api_id:
        Telegram API ID.
    api_hash:
        Telegram API hash.
    session_str:
        Optional explicit session string.  If not provided, the function
        resolves the session from the per-user JSON file, then env vars
        (same resolution as ``get_pyrogram_session_string()``).

    When ``session_str`` is provided explicitly, the internal resolution
    is skipped entirely, avoiding redundant file I/O — useful when the
    caller has already loaded the session string asynchronously.

    For async callers that need MongoDB fallback too, prefer
    ``build_pyrogram_client_async()`` which first checks per-user JSON →
    MongoDB → env before building the client.

    Reads the following env vars for retry/timeout configuration:
      - PYROGRAM_SLEEP_THRESHOLD (default 30): seconds to sleep before retrying
        on flood-wait or transient server errors.
      - PYROGRAM_MAX_RETRIES (default 10): max RPC retries per request.

    Returns a Pyrogram Client ready for ``client.start()``, or None if no
    session string is available (or Pyrogram is not installed).
    """
    if PyrogramClient is None:
        logger.debug("Pyrogram is not installed; cannot use Pyrogram session string.")
        return None

    if session_str is None:
        session_str = get_pyrogram_session_string()
    if not session_str:
        return None

    # Read retry/timeout configuration from env vars
    try:
        sleep_threshold = int(os.getenv("PYROGRAM_SLEEP_THRESHOLD", "30"))
    except (TypeError, ValueError):
        sleep_threshold = 30
    try:
        max_retries = int(os.getenv("PYROGRAM_MAX_RETRIES", "10"))
    except (TypeError, ValueError):
        max_retries = 10

    try:
        client = PyrogramClient(
            "pyrogram_userbot_session",
            api_id=api_id,
            api_hash=api_hash,
            session_string=session_str,
            in_memory=True,
            sleep_threshold=sleep_threshold,
        )
        # Increase the session-level RPC retry limit
        client.MAX_RETRIES = max_retries
        logger.info(
            "userbot: Pyrogram client configured with sleep_threshold=%s max_retries=%s",
            sleep_threshold,
            max_retries,
        )
        return client
    except Exception:
        logger.exception("Failed to create Pyrogram client from session string")
        return None


async def build_pyrogram_client_async(
    api_id: int,
    api_hash: str,
    user_id: int | None = None,
    db_model: object | None = None,
) -> object | None:
    """Build a Pyrogram client from a session string, checking MongoDB too (async).

    Resolution order:
    1. Environment variable (``PYROGRAM_SESSION`` etc.)
    2. Persisted JSON file (``pyrogram_session`` key)
    3. MongoDB (``pyrogram_session`` key, saved by ``/loginpyro`` or healthchecker)

    Parameters
    ----------
    api_id:
        Telegram API ID.
    api_hash:
        Telegram API hash.
    user_id:
        User ID for MongoDB session lookup (typically ``admin_user_id``).
        When ``None``, MongoDB lookup is skipped.
    db_model:
        MongoDB model with ``load_session`` method.  Required for MongoDB lookup.

    Returns a Pyrogram Client ready for ``client.start()``, or None if no
    session string is available.

    Same retry/timeout config as ``build_pyrogram_client()`` (reads env vars).
    """
    session_str = await get_pyrogram_session_string_for_user(user_id=user_id, db_model=db_model)
    if not session_str:
        return None
    return build_pyrogram_client(api_id, api_hash, session_str=session_str)


def is_pyrogram_available(user_id: int | None = None) -> bool:
    """Return True if Pyrogram is installed and a session string is configured.

    Checks the per-user JSON file first, then env vars, then the persisted
    global JSON file written by the healthchecker.
    """
    if PyrogramClient is None:
        return False
    return bool(get_pyrogram_session_string(user_id=user_id))


def has_usable_telethon_session(user_id: int | None = None) -> bool:
    """Return True when Telethon can use a pre-existing session without prompting for login.

    Resolution order matches the session resolvers: per-user JSON file, then env
    vars, then file-based sessions.  When ``user_id`` is provided, only that
    user's persisted data and ``.session`` file are considered (per-phone
    isolation); the legacy global files are used for unscoped lookups only.
    """
    if TelegramClient is None:
        return False

    # 1. Per-user JSON file (freshest persisted session)
    if user_id is not None:
        file_str = _load_session_string_from_file(client_type="telethon", user_id=user_id)
        if file_str:
            return True

    # 2. Environment variable
    session_str = _get_env_value(
        "API_SESSION",
        "SESSION",
        "api_session",
        "USERBOT_SESSION",
        "userbot_session",
        "TELETHON_SESSION",
        "telethon_session",
    )
    if session_str:
        return True

    # 3. File-based .session files on disk
    if user_id is not None:
        per_user_path = get_telethon_session_path() + f".{user_id}.session"
        return bool(os.path.exists(per_user_path))

    session_path = get_telethon_session_path()
    return os.path.exists(session_path) or os.path.exists(session_path + ".session")


async def has_usable_telethon_session_async(
    user_id: int | None = None, db_model: object | None = None
) -> bool:
    """Async ``has_usable_telethon_session`` that also consults MongoDB.

    The synchronous variant can only inspect the JSON files, env vars and
    ``.session`` files, so a session that lives only in MongoDB looks
    "unconfigured" to it.  This checks MongoDB as well, which is what the
    downloader/uploader need outside the healthcheck.
    """
    if has_usable_telethon_session(user_id=user_id):
        return True
    session_str = await get_telethon_session_string_for_user(user_id=user_id, db_model=db_model)
    return bool(session_str)


def is_telethon_available(user_id: int | None = None) -> bool:
    """Return True if Telethon is installed and configured."""
    return has_usable_telethon_session(user_id=user_id)


def get_preferred_client_type(user_id: int | None = None) -> str:
    """Return 'pyrogram' if Pyrogram session is available, else 'telethon'."""
    if is_pyrogram_available(user_id=user_id):
        return "pyrogram"
    return "telethon"


def get_userbot_credentials():
    """Return (api_id, api_hash) from env vars.

    Raises RuntimeError if either is missing or api_id is not an integer.
    """
    api_id = os.getenv("API_ID") or os.getenv("api_id") or os.getenv("USERBOT_API_ID") or os.getenv("userbot_api_id")
    api_hash = (
        os.getenv("API_HASH") or os.getenv("api_hash") or os.getenv("USERBOT_API_HASH") or os.getenv("userbot_api_hash")
    )
    if not api_id or not api_hash:
        raise RuntimeError("API_ID and API_HASH must be set to use userbot fallback")
    try:
        api_id = int(api_id)
    except (TypeError, ValueError) as e:
        raise RuntimeError("API_ID must be an integer") from e
    return api_id, api_hash


def normalize_target(chat_id: int | str) -> int | str:
    """Normalize a chat_id to a form usable by both Telethon and Pyrogram."""
    if isinstance(chat_id, str) and chat_id.startswith("@"):
        return chat_id
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return chat_id
