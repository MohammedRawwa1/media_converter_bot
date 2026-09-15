"""The ``/session_status`` dashboard: who is using the bot and what is queued.

Answers the three questions the existing ``/loginstatus`` does not:

1. How busy is the bot?      -> waiting / running / delayed jobs, plus broker depths
2. Who is using it right now? -> the Redis presence heartbeat (``utils.presence``)
3. When does my work run?    -> the queue turn, i.e. the position of a user's
                                oldest waiting job in the FIFO job list

The admin view also carries a **Capacity** block: how many of the global
conversion slots (``ffmpeg:slot:*``, see ``utils.batch_pipeline``) are in use
against ``MAX_CONCURRENT_FFMPEG``, and how much headroom the busiest worker has
under ``WORKER_MEMORY_CEILING_BYTES`` before it stops taking new work. It is
followed by a **Memory** block - this process's RSS and its peak, plus what the
container has left - and a **Storage** block with the object count and total
bytes in whichever backend holds them (S3/R2 or the local ``storage/`` tree).

The dashboard is served with an inline keyboard: **🔄 Refresh** re-collects and
edits the same message in place (forcing a fresh storage scan), and admins also
get **♻️ Restart worker** for the same clean-heap recycle as ``/worker_restart``.

A storage scan costs a request per 1000 objects, so it is cached in Redis for
``STATUS_STORAGE_CACHE_SECONDS`` and only a button press asks for a fresh one.

The same contract as ``utils.health`` applies: every probe is bounded by a
timeout and nothing here raises, so a dead Redis degrades one section of the
report instead of leaving the command hanging. Session health is read from the
background checker's last result (instant); ``live_sessions=True`` asks for a
real Telegram connection check instead.

The aggregation and the rendering are kept as pure functions
(:func:`summarize`, :func:`format_status`) so the interesting logic is testable
without Redis or Telegram.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import sys
import time
from collections import Counter

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from utils import batch_pipeline, presence
from utils.queue_admin import JOB_HASH_PREFIX, TERMINAL_STATUSES

logger = logging.getLogger(__name__)

PROBE_TIMEOUT_SECONDS = 2.0
BROKER_TIMEOUT_SECONDS = 3.0

# A job hash the worker currently owns. ``processing`` is written by the worker
# when it starts a job; the two upload phases keep the job on its worker too.
RUNNING_STATUSES = frozenset({"processing", "uploading", "sending"})
DONE_STATUSES = frozenset({"done", "completed"})
FAILED_STATUSES = frozenset({"error", "failed"})
CANCELLED_STATUSES = frozenset({"cancelled", "canceled"})

# Bounds so a status call stays fast even with a very long queue or many hashes.
QUEUE_SCAN_LIMIT = int(os.getenv("STATUS_QUEUE_SCAN_LIMIT", "2000"))
HASH_SCAN_LIMIT = int(os.getenv("STATUS_HASH_SCAN_LIMIT", "5000"))
FETCH_CHUNK = 200
ACTIVE_LIST_LIMIT = 10

# ── Memory ──────────────────────────────────────────────────────────────
# Warn while there is still room to act: at 80% the next 1 GB source is a risk,
# at 92% the OOM killer is the next thing to happen.
MEMORY_HIGH_PERCENT = float(os.getenv("STATUS_MEMORY_HIGH_PERCENT", "80"))
MEMORY_CRITICAL_PERCENT = float(os.getenv("STATUS_MEMORY_CRITICAL_PERCENT", "92"))
# cgroup v1/v2 report "no limit" as a number near 2**63 rather than "max".
_CGROUP_UNLIMITED_BYTES = 1 << 60

# ── Storage ─────────────────────────────────────────────────────────────
# A bucket listing costs a request per page, so the scan is cached and only a
# Refresh press passes ``force=True``.
STORAGE_CACHE_KEY = "status:storage:usage"
STORAGE_CACHE_SECONDS = int(os.getenv("STATUS_STORAGE_CACHE_SECONDS", "300"))
STORAGE_SCAN_MAX_OBJECTS = int(os.getenv("STATUS_STORAGE_SCAN_MAX_OBJECTS", "5000"))
STORAGE_SCAN_TIMEOUT_SECONDS = float(os.getenv("STATUS_STORAGE_SCAN_TIMEOUT_SECONDS", "20"))
STORAGE_GROUP_LIMIT = 6

# ── Dashboard buttons ───────────────────────────────────────────────────
# ``st:`` keeps these away from the menu callback handler, which is registered
# without a pattern and would otherwise inspect every press (see main.py).
STATUS_CALLBACK_PREFIX = "st:"
REFRESH_ACTION = "refresh"
RESTART_ACTION = "restart"
REFRESH_LABEL = "🔄 Refresh"
RESTART_WORKER_LABEL = "♻️ Restart worker"


def _text(value) -> str:
    """Return a Redis value as ``str`` whether the client decodes or not."""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    return "" if value is None else str(value)


def _int_or_none(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ── Collection ──────────────────────────────────────────────────────────


async def collect_session_status(
    *, user_id=None, is_admin=False, live_sessions=False, force_storage=False
) -> dict:
    """Gather the whole dashboard. Never raises; degrades field by field.

    ``force_storage`` skips the cached storage scan - the Refresh button passes
    it so a press always shows a number measured just now.
    """
    redis_snapshot = await _redis_snapshot()
    broker_snapshot = await _broker_snapshot()
    online_snapshot = await presence.snapshot()
    sessions = await _sessions_snapshot(user_id=user_id, live=live_sessions)
    memory = summarize_memory(_memory_snapshot())
    storage = await _storage_snapshot(force=force_storage)

    me_id = _int_or_none(user_id)
    payload = summarize(
        jobs=redis_snapshot["jobs"],
        queued=redis_snapshot["queued"],
        waiting=redis_snapshot["waiting"],
        delayed=redis_snapshot["delayed"],
        online_ids=online_snapshot.get("user_ids") or [],
        me_id=me_id,
        jobs_truncated=redis_snapshot["jobs_truncated"],
        queue_truncated=redis_snapshot["queue_truncated"],
    )
    payload["redis"] = {
        "connected": redis_snapshot["connected"],
        "ping_ms": redis_snapshot["ping_ms"],
        "error": redis_snapshot["error"],
    }
    payload["queues"]["broker"] = broker_snapshot
    payload["capacity"] = summarize_capacity(
        slots_used=redis_snapshot["slots_used"],
        worker_rss=redis_snapshot["worker_rss"],
    )
    payload["users"]["source"] = online_snapshot.get("source")
    payload["users"]["error"] = online_snapshot.get("error")
    payload["sessions"] = sessions
    payload["memory"] = memory
    payload["storage"] = storage
    payload["generated_at"] = time.time()
    payload["status"] = _overall_status(redis_snapshot, broker_snapshot)
    return payload


async def _redis_snapshot() -> dict:
    """Read the queue shape and the job hashes, or explain why we could not."""
    from utils.job_queue import DELAYED_SET, JOB_LIST, get_redis

    snapshot = {
        "connected": False,
        "ping_ms": None,
        "error": None,
        "waiting": None,
        "delayed": None,
        "queued": [],
        "jobs": [],
        "queue_truncated": False,
        "jobs_truncated": False,
        "slots_used": None,
        "worker_rss": {},
    }
    try:
        client = await asyncio.wait_for(get_redis(), timeout=PROBE_TIMEOUT_SECONDS)
    except Exception as exc:
        snapshot["error"] = f"redis unavailable: {exc}"
        return snapshot

    try:
        started = time.perf_counter()
        await asyncio.wait_for(client.ping(), timeout=PROBE_TIMEOUT_SECONDS)
        snapshot["ping_ms"] = round((time.perf_counter() - started) * 1000, 2)
        snapshot["connected"] = True

        snapshot["waiting"] = int(await asyncio.wait_for(client.llen(JOB_LIST), timeout=PROBE_TIMEOUT_SECONDS))
        snapshot["delayed"] = int(await asyncio.wait_for(client.zcard(DELAYED_SET), timeout=PROBE_TIMEOUT_SECONDS))

        raw = await asyncio.wait_for(client.lrange(JOB_LIST, 0, -1), timeout=PROBE_TIMEOUT_SECONDS)
        raw = list(raw or [])
        # The tail of the list is the *oldest* end, and that is the end workers
        # pop from - so it is the part that carries a meaningful turn number.
        if len(raw) > QUEUE_SCAN_LIMIT:
            raw = raw[-QUEUE_SCAN_LIMIT:]
            snapshot["queue_truncated"] = True
        snapshot["queued"] = [_parse_queue_entry(entry) for entry in raw]

        snapshot["jobs"], snapshot["jobs_truncated"] = await _scan_job_hashes(client)

        # Capacity: how much of the ffmpeg pool and the memory ceiling is in use.
        # Probed in its own try so a slow or older client leaves these two
        # fields empty instead of taking the whole snapshot down with it.
        try:
            snapshot["slots_used"] = await asyncio.wait_for(
                batch_pipeline.read_used_slots(client), timeout=PROBE_TIMEOUT_SECONDS
            )
            snapshot["worker_rss"] = await asyncio.wait_for(
                batch_pipeline.read_worker_rss(client), timeout=PROBE_TIMEOUT_SECONDS
            )
        except Exception as exc:
            logger.debug("session_status: capacity probe fell over: %s", exc)
    except Exception as exc:
        snapshot["error"] = f"{type(exc).__name__}: {exc}"
        logger.debug("session_status: redis probe fell over: %s", exc)
    finally:
        with contextlib.suppress(Exception):
            await client.close()
    return snapshot


def _parse_queue_entry(entry) -> dict:
    """Extract ``{"job_id", "owner"}`` from a raw ``ffmpeg:jobs`` list entry."""
    try:
        parsed = json.loads(_text(entry))
    except Exception:
        return {"job_id": "", "owner": None}
    if not isinstance(parsed, dict):
        return {"job_id": "", "owner": None}
    owner = _int_or_none(parsed.get("user_id")) or _int_or_none(parsed.get("chat_id"))
    return {"job_id": str(parsed.get("job_id") or ""), "owner": owner}


async def _scan_job_hashes(client) -> tuple[list[dict], bool]:
    """Read status + owner from every ``ffmpeg:job:*`` hash, in batched pipelines."""
    keys: list[str] = []
    truncated = False
    async for key in client.scan_iter(match=f"{JOB_HASH_PREFIX}*", count=500):
        keys.append(_text(key))
        if len(keys) >= HASH_SCAN_LIMIT:
            truncated = True
            break

    jobs: list[dict] = []
    for start in range(0, len(keys), FETCH_CHUNK):
        chunk = keys[start : start + FETCH_CHUNK]
        rows: list = []
        try:
            pipe = client.pipeline()
            for key in chunk:
                pipe.hmget(key, ["status", "user_id", "chat_id"])
            rows = await pipe.execute()
        except Exception:
            # A client without pipelining still has to answer: fall back to
            # one round trip per key rather than dropping the whole section.
            rows = []
            for key in chunk:
                try:
                    rows.append(await client.hmget(key, ["status", "user_id", "chat_id"]))
                except Exception:
                    rows.append([])
        for key, row in zip(chunk, rows, strict=False):
            row = list(row or [])
            status = _text(row[0]) if len(row) > 0 else ""
            user = _text(row[1]) if len(row) > 1 else ""
            chat = _text(row[2]) if len(row) > 2 else ""
            jobs.append(
                {
                    "job_id": key[len(JOB_HASH_PREFIX) :],
                    "status": status,
                    "owner": _int_or_none(user) or _int_or_none(chat),
                }
            )
    return jobs, truncated


async def _broker_snapshot() -> dict:
    """Report the broker pipe the same way ``utils.health`` does."""
    try:
        from utils.eventbus import get_settings

        settings = get_settings()
    except Exception as exc:
        return {"backend": "unknown", "configured": None, "queues": {}, "error": f"settings unavailable: {exc}"}

    if not settings.consumes_rabbitmq:
        return {"backend": "redis", "configured": False, "queues": {}, "error": None}

    try:
        from utils.eventbus.rabbit import get_queue

        queues = await asyncio.wait_for(get_queue().stats(), timeout=BROKER_TIMEOUT_SECONDS)
        return {"backend": "rabbitmq", "configured": True, "queues": queues or {}, "error": None}
    except Exception as exc:
        return {"backend": "rabbitmq", "configured": True, "queues": {}, "error": f"broker unavailable: {exc}"}


async def _sessions_snapshot(*, user_id, live: bool) -> dict | None:
    """Cached (or freshly checked) userbot session health."""
    try:
        from utils.session_healthcheck import get_session_healthchecker

        checker = get_session_healthchecker()
        if live:
            health = await checker.run_once(user_id=user_id)
        else:
            health = getattr(checker, "last_health", None) or {}
    except Exception as exc:
        logger.debug("session_status: session health unavailable: %s", exc)
        return None

    if not health:
        return {"checked": False, "live": live, "sessions": {}}
    return {
        "checked": True,
        "live": live,
        "sessions": {
            name: {
                "alive": bool(result.get("alive")),
                "latency_ms": result.get("latency_ms"),
                "error": result.get("error"),
                "source": result.get("source"),
            }
            for name, result in health.items()
            if isinstance(result, dict)
        },
    }


def _memory_snapshot() -> dict:
    """Raw memory readings for this process and the box it runs on.

    Synchronous and never raising: every source is optional, so a platform that
    hides ``/proc`` or ships without ``psutil`` still gets whatever the others
    can report.
    """
    raw = {
        "self_rss_bytes": None,
        "self_peak_bytes": None,
        "host_total_bytes": None,
        "host_used_bytes": None,
        "cgroup_limit_bytes": None,
        "cgroup_used_bytes": None,
        "source": None,
    }

    try:
        rss = int(batch_pipeline.rss_bytes() or 0)
        raw["self_rss_bytes"] = rss or None
    except Exception:
        logger.debug("session_status: could not read this process's RSS")

    try:
        import resource

        peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        if peak > 0:
            # Linux reports KiB here, macOS bytes.
            raw["self_peak_bytes"] = peak if sys.platform == "darwin" else peak * 1024
    except Exception:
        logger.debug("session_status: could not read the process peak RSS")

    try:
        import psutil

        vm = psutil.virtual_memory()
        raw["host_total_bytes"] = int(vm.total)
        raw["host_used_bytes"] = int(vm.total - vm.available)
        raw["source"] = "psutil"
    except Exception:
        raw.update(_meminfo_fallback())

    limit, used = _cgroup_memory()
    raw["cgroup_limit_bytes"] = limit
    raw["cgroup_used_bytes"] = used
    return raw


def _meminfo_fallback() -> dict:
    """Read ``/proc/meminfo`` when ``psutil`` is unavailable (Linux only)."""
    out = {"host_total_bytes": None, "host_used_bytes": None, "source": None}
    try:
        info: dict[str, str] = {}
        with open("/proc/meminfo", encoding="ascii") as fh:
            for line in fh:
                key, _, rest = line.partition(":")
                info[key.strip()] = rest.strip()
        total = _meminfo_bytes(info.get("MemTotal"))
        available = _meminfo_bytes(info.get("MemAvailable")) or _meminfo_bytes(info.get("MemFree"))
        if total:
            out["host_total_bytes"] = total
            out["host_used_bytes"] = max(0, total - (available or 0))
            out["source"] = "proc"
    except Exception:
        logger.debug("session_status: /proc/meminfo unavailable")
    return out


def _meminfo_bytes(value) -> int | None:
    """Turn a ``/proc/meminfo`` value (``"8123456 kB"``) into bytes."""
    if not value:
        return None
    parts = str(value).split()
    try:
        number = int(parts[0])
    except (IndexError, ValueError):
        return None
    unit = parts[1].lower() if len(parts) > 1 else "kb"
    factor = {"kb": 1024, "mb": 1024**2, "gb": 1024**3, "b": 1}.get(unit, 1024)
    return number * factor


def _cgroup_memory() -> tuple[int | None, int | None]:
    """``(limit, used)`` for this container's cgroup, or ``(None, None)``.

    The cgroup limit - not the host's RAM - is what actually kills the process
    on a container platform, so it is the number worth showing. cgroup v2 is
    checked first, then v1; a negative or ~2**63 limit means "unlimited".
    """

    def _read(path: str) -> int | None:
        try:
            with open(path, encoding="ascii") as fh:
                text = fh.read().strip()
        except OSError:
            return None
        if not text or text.lower() == "max":
            return None
        try:
            value = int(text)
        except ValueError:
            return None
        if value <= 0 or value >= _CGROUP_UNLIMITED_BYTES:
            return None
        return value

    for limit_path, used_path in (
        ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current"),
        ("/sys/fs/cgroup/memory/memory.limit_in_bytes", "/sys/fs/cgroup/memory/memory.usage_in_bytes"),
    ):
        limit = _read(limit_path)
        if limit is not None:
            return limit, _read(used_path)
    return None, None


async def _storage_snapshot(*, force: bool = False) -> dict:
    """Object count and total bytes in the configured storage backend.

    Never raises: a storage backend that cannot be reached, or cannot list its
    contents, reports why in ``error`` so the dashboard still renders.
    """
    snapshot = {
        "backend": None,
        "location": None,
        "objects": None,
        "bytes": None,
        "groups": {},
        "truncated": False,
        "cached": False,
        "scanned_at": None,
        "error": None,
    }

    try:
        from utils.storage import get_storage_backend

        backend = await get_storage_backend()
    except Exception as exc:
        snapshot["error"] = f"storage unavailable: {exc}"
        return snapshot

    snapshot["backend"] = "s3" if "S3" in type(backend).__name__ else "local"
    snapshot["location"] = getattr(backend, "bucket", None) or getattr(backend, "base", None)

    scanner = getattr(backend, "usage", None)
    if not callable(scanner):
        snapshot["error"] = "this storage backend cannot report its usage"
        return snapshot

    if not force:
        cached = await _read_storage_cache()
        if cached is not None:
            snapshot.update(cached)
            snapshot["cached"] = True
            snapshot["error"] = None
            return snapshot

    try:
        readings = await asyncio.wait_for(
            scanner(max_objects=STORAGE_SCAN_MAX_OBJECTS), timeout=STORAGE_SCAN_TIMEOUT_SECONDS
        )
    except Exception as exc:
        snapshot["error"] = f"{type(exc).__name__}: {exc}"
        logger.debug("session_status: storage scan failed: %s", exc)
        return snapshot

    if not isinstance(readings, dict):
        snapshot["error"] = "storage backend returned no usage data"
        return snapshot

    snapshot["backend"] = readings.get("backend") or snapshot["backend"]
    snapshot["location"] = readings.get("location") or snapshot["location"]
    snapshot["objects"] = _int_or_none(readings.get("objects"))
    snapshot["bytes"] = _int_or_none(readings.get("bytes"))
    snapshot["truncated"] = bool(readings.get("truncated"))
    groups = readings.get("groups")
    snapshot["groups"] = groups if isinstance(groups, dict) else {}
    snapshot["scanned_at"] = time.time()

    await _write_storage_cache(snapshot)
    return snapshot


async def _read_storage_cache() -> dict | None:
    """The last scan, if one is still within its TTL. ``None`` when there is not."""
    raw = await _storage_cache_get(STORAGE_CACHE_KEY)
    if not raw:
        return None
    try:
        data = json.loads(_text(raw))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


async def _write_storage_cache(snapshot: dict) -> None:
    """Remember this scan so the next dashboard does not pay for another one."""
    if STORAGE_CACHE_SECONDS <= 0:
        return
    payload = {
        "backend": snapshot.get("backend"),
        "location": snapshot.get("location"),
        "objects": snapshot.get("objects"),
        "bytes": snapshot.get("bytes"),
        "groups": snapshot.get("groups") or {},
        "truncated": snapshot.get("truncated"),
        "scanned_at": snapshot.get("scanned_at"),
    }
    await _storage_cache_set(STORAGE_CACHE_KEY, json.dumps(payload), STORAGE_CACHE_SECONDS)


async def _storage_cache_get(key: str):
    """A cached value, or ``None`` - Redis being down is not an error here."""
    try:
        from utils.job_queue import get_redis

        client = await asyncio.wait_for(get_redis(), timeout=PROBE_TIMEOUT_SECONDS)
        return await asyncio.wait_for(client.get(key), timeout=PROBE_TIMEOUT_SECONDS)
    except Exception:
        logger.debug("session_status: storage cache read failed")
        return None


async def _storage_cache_set(key: str, value: str, ttl: int) -> None:
    """Best-effort cache write; losing it only costs the next scan."""
    try:
        from utils.job_queue import get_redis

        client = await asyncio.wait_for(get_redis(), timeout=PROBE_TIMEOUT_SECONDS)
        await asyncio.wait_for(client.set(key, value, ex=max(1, int(ttl))), timeout=PROBE_TIMEOUT_SECONDS)
    except Exception:
        logger.debug("session_status: storage cache write failed")


def _overall_status(redis_snapshot: dict, broker_snapshot: dict) -> str:
    """``degraded`` when a pipe that should be serving cannot, else ``ok``."""
    if not redis_snapshot.get("connected"):
        return "degraded"
    if broker_snapshot.get("configured") and broker_snapshot.get("queues") == {} and broker_snapshot.get("error"):
        return "degraded"
    return "ok"


# ── Aggregation (pure) ──────────────────────────────────────────────────


def summarize_memory(raw: dict) -> dict:
    """Fold the raw memory readings into the numbers the dashboard shows.

    Pure, so the arithmetic can be explained without a live box. The cgroup
    limit is preferred over the host total when both are readable, because on a
    container platform that limit is what actually ends the process. Every
    value stays ``None`` rather than zero when it is unknown, so "no data" is
    never rendered as "using nothing".
    """
    raw = raw or {}
    limit = _int_or_none(raw.get("cgroup_limit_bytes"))
    cgroup_used = _int_or_none(raw.get("cgroup_used_bytes"))
    host_total = _int_or_none(raw.get("host_total_bytes"))
    host_used = _int_or_none(raw.get("host_used_bytes"))

    total = limit or host_total
    scope = "container" if limit else ("host" if host_total else None)
    # A cgroup limit without a cgroup reading would report the *host's* use
    # against the container's limit, which is worse than saying nothing.
    used = cgroup_used if limit else host_used
    percent = round(used / total * 100, 1) if used is not None and total else None

    pressure = "unknown"
    if percent is not None:
        if percent >= MEMORY_CRITICAL_PERCENT:
            pressure = "critical"
        elif percent >= MEMORY_HIGH_PERCENT:
            pressure = "high"
        else:
            pressure = "ok"

    return {
        "self_rss_bytes": _int_or_none(raw.get("self_rss_bytes")),
        "self_peak_bytes": _int_or_none(raw.get("self_peak_bytes")),
        "total_bytes": total,
        "used_bytes": used,
        "free_bytes": (total - used) if total is not None and used is not None else None,
        "percent": percent,
        "scope": scope,
        "source": raw.get("source"),
        "pressure": pressure,
    }


def summarize_capacity(*, slots_used, worker_rss=None) -> dict:
    """Fold the slot count and worker heartbeats into the capacity view.

    Pure, so the dashboard's numbers can be explained without a live Redis: the
    caller passes the raw reads and this does the arithmetic (peak RSS, headroom
    against the ceiling). ``peak_rss_bytes`` is ``None`` when no worker has
    reported, and stays ``None`` - rather than zero - so "no data" is never
    mistaken for "using no memory".
    """
    readings = [parsed for parsed in (_int_or_none(value) for value in (worker_rss or {}).values()) if parsed]
    peak = max(readings) if readings else None
    ceiling = int(batch_pipeline.MEMORY_CEILING_BYTES)
    headroom = ceiling - peak if ceiling > 0 and peak is not None else None
    return {
        "ffmpeg_running": slots_used,
        "ffmpeg_limit": int(batch_pipeline.MAX_CONCURRENT_FFMPEG),
        "workers_reporting": len(readings),
        "peak_rss_bytes": peak,
        "ceiling_bytes": ceiling,
        "headroom_bytes": headroom,
    }


def summarize(
    *,
    jobs: list[dict],
    queued: list[dict],
    waiting: int | None,
    delayed: int | None,
    online_ids: list,
    me_id: int | None = None,
    jobs_truncated: bool = False,
    queue_truncated: bool = False,
) -> dict:
    """Turn raw hash/queue reads into the numbers the report shows."""
    counts = Counter()
    running_by_owner: Counter = Counter()
    for job in jobs or []:
        status = str(job.get("status") or "")
        if status in RUNNING_STATUSES:
            counts["running"] += 1
            owner = job.get("owner")
            if owner is not None:
                running_by_owner[owner] += 1
        elif status in DONE_STATUSES:
            counts["done"] += 1
        elif status in FAILED_STATUSES:
            counts["failed"] += 1
        elif status in CANCELLED_STATUSES:
            counts["cancelled"] += 1
        elif status in TERMINAL_STATUSES:
            counts["other"] += 1

    # ``queued`` is ordered newest -> oldest, so the last entry is next to run.
    # Its turn is 1; everything else counts back from there.
    span = len(queued or [])
    waiting_by_owner: Counter = Counter()
    turn_by_owner: dict[int, int] = {}
    for index, entry in enumerate(queued or []):
        owner = entry.get("owner")
        if owner is None:
            continue
        waiting_by_owner[owner] += 1
        turn = span - index
        if owner not in turn_by_owner or turn < turn_by_owner[owner]:
            turn_by_owner[owner] = turn

    total_waiting = waiting if waiting is not None else span
    active = [
        {
            "user_id": int(uid),
            "waiting": int(waiting_by_owner.get(int(uid), 0)),
            "running": int(running_by_owner.get(int(uid), 0)),
            "turn": turn_by_owner.get(int(uid)),
        }
        for uid in (online_ids or [])
        if _int_or_none(uid) is not None
    ]
    active.sort(key=lambda row: (-row["running"], -row["waiting"], row["user_id"]))

    me = None
    if me_id is not None:
        me = {
            "user_id": me_id,
            "waiting": int(waiting_by_owner.get(me_id, 0)),
            "running": int(running_by_owner.get(me_id, 0)),
            "turn": turn_by_owner.get(me_id),
        }

    running = int(counts["running"])
    return {
        "queues": {
            "waiting": total_waiting,
            "running": running,
            "delayed": delayed if delayed is not None else None,
            "jobs_online": int(total_waiting or 0) + running,
            "broker": None,  # filled in by the collector
            "queue_truncated": queue_truncated,
            "jobs_truncated": jobs_truncated,
        },
        "jobs": {
            "running": running,
            "done": int(counts["done"]),
            "failed": int(counts["failed"]),
            "cancelled": int(counts["cancelled"]),
            "scanned": len(jobs or []),
        },
        "users": {
            "online": len(active),
            "active": active,
            "source": None,  # filled in by the collector
            "error": None,
        },
        "me": me,
        "redis": {},
        "sessions": None,
        "generated_at": time.time(),
        "status": "ok",
    }


# ── Rendering (pure) ────────────────────────────────────────────────────


def format_status(payload: dict, *, is_admin: bool, note: str | None = None) -> str:
    """Render the dashboard as Telegram HTML.

    ``note`` appends one already-formatted line above the timestamp - used by
    the buttons to report what a press did without replacing the report.
    """
    lines: list[str] = []
    status = payload.get("status") or "unknown"
    badge = {"ok": "🟢", "degraded": "🟠"}.get(status, "⚪")
    title = "📊 <b>Session &amp; Queue Status</b>" if is_admin else "📊 <b>Your Session Status</b>"
    lines.append(f"{title} {badge} <i>{status}</i>")

    redis_info = payload.get("redis") or {}
    if not redis_info.get("connected"):
        lines.append("")
        lines.append("🔴 <b>Redis unreachable</b> — queue numbers unavailable")
        if redis_info.get("error"):
            lines.append(f"<i>{_esc(redis_info['error'])}</i>")
    else:
        lat = redis_info.get("ping_ms")
        lines.append(f"🔗 Redis: <code>{lat if lat is not None else '?'} ms</code>")

    if is_admin:
        lines.extend(_admin_sections(payload))
    else:
        lines.extend(_personal_section(payload))

    lines.extend(_session_section(payload))
    if note:
        lines.append("")
        lines.append(note)
    lines.append("")
    lines.append(f"<i>updated {time.strftime('%H:%M:%S')}</i>")
    return "\n".join(lines)


def _admin_sections(payload: dict) -> list[str]:
    queues = payload.get("queues") or {}
    jobs = payload.get("jobs") or {}
    users = payload.get("users") or {}
    lines: list[str] = []

    lines.append("")
    lines.append("📦 <b>Queue</b>")
    lines.append(f"• Waiting (queued): <b>{_num(queues.get('waiting'))}</b>")
    lines.append(f"• Running: <b>{_num(queues.get('running'))}</b>")
    lines.append(f"• Delayed/retry: <b>{_num(queues.get('delayed'))}</b>")
    lines.append(f"• Jobs online (waiting+running): <b>{_num(queues.get('jobs_online'))}</b>")

    broker = queues.get("broker") or {}
    if broker.get("backend") == "rabbitmq":
        if broker.get("error"):
            lines.append(f"• Broker (rabbitmq): ⚠️ {_esc(broker['error'])}")
        else:
            depths = ", ".join(f"{_esc(name)}={count}" for name, count in sorted((broker.get("queues") or {}).items()))
            lines.append(f"• Broker (rabbitmq): {_esc(depths) or 'no queues reported'}")
    elif broker.get("backend") == "redis":
        lines.append("• Broker: not configured (Redis-only)")

    if queues.get("queue_truncated") or queues.get("jobs_truncated"):
        lines.append("<i>⚠️ counts truncated at the scan cap</i>")

    capacity = payload.get("capacity") or {}
    if capacity and (payload.get("redis") or {}).get("connected"):
        lines.extend(_capacity_section(capacity))

    memory = payload.get("memory") or {}
    if memory.get("self_rss_bytes") is not None or memory.get("total_bytes") is not None:
        lines.extend(_memory_section(memory))

    storage = payload.get("storage")
    if isinstance(storage, dict):
        lines.extend(_storage_section(storage))

    lines.append("")
    lines.append("🧮 <b>Recent jobs</b> (hash window)")
    lines.append(
        f"• Done: <b>{_num(jobs.get('done'))}</b> · Failed: <b>{_num(jobs.get('failed'))}</b>"
        f" · Cancelled: <b>{_num(jobs.get('cancelled'))}</b> · scanned: {_num(jobs.get('scanned'))}"
    )

    lines.append("")
    lines.append(f"👥 <b>Users online:</b> <b>{_num(users.get('online'))}</b>")
    if users.get("source") == "memory":
        lines.append("<i>⚠️ Redis presence unavailable — this count only covers this worker</i>")
    active = users.get("active") or []
    for row in active[:ACTIVE_LIST_LIMIT]:
        turn = row.get("turn")
        turn_txt = f", next #{turn}" if turn else ""
        lines.append(
            f"• <code>{row['user_id']}</code> — {row['waiting']} waiting, {row['running']} running{turn_txt}"
        )
    if len(active) > ACTIVE_LIST_LIMIT:
        lines.append(f"<i>…and {len(active) - ACTIVE_LIST_LIMIT} more</i>")
    if not active:
        lines.append("<i>No active users in the heartbeat window.</i>")
    return lines


def _capacity_section(capacity: dict) -> list[str]:
    """Render ffmpeg concurrency and memory-ceiling headroom."""
    lines = ["", "⚙️ <b>Capacity</b>"]
    limit = capacity.get("ffmpeg_limit")
    used = capacity.get("ffmpeg_running")
    if limit:
        lines.append(f"• ffmpeg running: <b>{_num(used) if used is not None else '?'}/{limit}</b>")

    peak = capacity.get("peak_rss_bytes")
    ceiling = capacity.get("ceiling_bytes") or 0
    if peak is None:
        lines.append("• Worker memory: <i>no worker heartbeat yet</i>")
    elif ceiling > 0:
        headroom = capacity.get("headroom_bytes")
        if headroom is not None and headroom < 0:
            lines.append(
                f"• Worker RSS: <b>{_mb(peak)}</b> of {_mb(ceiling)}"
                f" — ⚠️ over ceiling by <b>{_mb(-headroom)}</b>"
            )
        else:
            lines.append(
                f"• Worker RSS: <b>{_mb(peak)}</b> of {_mb(ceiling)}"
                f" — headroom <b>{_mb(headroom)}</b>"
            )
    else:
        lines.append(f"• Worker RSS: <b>{_mb(peak)}</b> (no memory ceiling set)")

    workers = capacity.get("workers_reporting") or 0
    if workers > 1:
        lines.append(f"<i>{workers} workers reporting</i>")
    return lines


def _memory_section(memory: dict) -> list[str]:
    """Render this process's RSS and how much room the box has left."""
    lines = ["", "🧠 <b>Memory</b>"]

    rss = memory.get("self_rss_bytes")
    peak = memory.get("self_peak_bytes")
    if rss is None:
        lines.append("• Bot process: <i>unavailable</i>")
    elif peak:
        lines.append(f"• Bot process: <b>{_bytes_human(rss)}</b> (peak {_bytes_human(peak)})")
    else:
        lines.append(f"• Bot process: <b>{_bytes_human(rss)}</b>")

    total = memory.get("total_bytes")
    used = memory.get("used_bytes")
    if total and used is not None:
        scope = "Container" if memory.get("scope") == "container" else "System"
        percent = memory.get("percent")
        percent_txt = "?" if percent is None else f"{percent:g}"
        badge = {"high": " ⚠️", "critical": " 🔴"}.get(memory.get("pressure"), "")
        lines.append(
            f"• {scope}: <b>{_bytes_human(used)}</b> of {_bytes_human(total)}"
            f" — <b>{percent_txt}%</b> used{badge}"
        )
        free = memory.get("free_bytes")
        if free is not None:
            lines.append(f"• Free: <b>{_bytes_human(free)}</b>")
    else:
        lines.append("• Total memory: <i>unavailable</i>")
    return lines


def _storage_section(storage: dict) -> list[str]:
    """Render the object count and bytes held in the storage backend."""
    lines = ["", "🗄 <b>Storage</b>"]
    if storage.get("error"):
        lines.append(f"• ⚠️ {_esc(storage['error'])}")
        return lines

    backend = storage.get("backend") or "unknown"
    location = storage.get("location")
    lines.append(f"• Backend: <b>{_esc(backend)}</b>" + (f" · <code>{_esc(location)}</code>" if location else ""))

    objects = storage.get("objects")
    if objects is None:
        lines.append("• Usage: <i>unavailable</i>")
    else:
        size = _bytes_human(storage.get("bytes"))
        capped = " <i>(scan capped)</i>" if storage.get("truncated") else ""
        lines.append(f"• Used: <b>{size}</b> in <b>{_num(objects)}</b> object(s){capped}")

    groups = {name: row for name, row in (storage.get("groups") or {}).items() if isinstance(row, dict)}
    # Biggest first: the point of the breakdown is what is eating the space.
    ranked = sorted(groups.items(), key=lambda item: -(_int_or_none(item[1].get("bytes")) or 0))
    for name, row in ranked[:STORAGE_GROUP_LIMIT]:
        lines.append(
            f"   – <code>{_esc(name)}</code>: {_bytes_human(row.get('bytes'))}"
            f" in {_num(_int_or_none(row.get('objects')))} object(s)"
        )
    if len(ranked) > STORAGE_GROUP_LIMIT:
        lines.append(f"<i>…and {len(ranked) - STORAGE_GROUP_LIMIT} more prefixes</i>")

    if storage.get("cached"):
        lines.append(f"<i>cached scan — press {REFRESH_LABEL} for a fresh one</i>")
    return lines


def _personal_section(payload: dict) -> list[str]:
    me = payload.get("me") or {}
    lines = ["", "🙋 <b>Your jobs</b>"]
    lines.append(f"• Waiting: <b>{_num(me.get('waiting'))}</b>")
    lines.append(f"• Running: <b>{_num(me.get('running'))}</b>")
    turn = me.get("turn")
    if turn:
        lines.append(f"• Your next job: <b>#{turn}</b> in the queue")
    else:
        lines.append("• Your next job: <i>nothing waiting</i>")
    return lines


def _session_section(payload: dict) -> list[str]:
    sessions = payload.get("sessions")
    lines = ["", "🔐 <b>Userbot sessions</b>"]
    if not sessions or not sessions.get("checked"):
        lines.append("<i>No recent session check — run <code>/session_status live</code>.</i>")
        return lines

    found = sessions.get("sessions") or {}
    for name in ("telethon", "pyrogram"):
        result = found.get(name)
        if result is None:
            lines.append(f"• {name.capitalize()}: <i>not configured</i>")
            continue
        if result.get("alive"):
            lines.append(f"• {name.capitalize()}: ✅ working ({_num(result.get('latency_ms'))} ms)")
        else:
            lines.append(f"• {name.capitalize()}: ❌ {_esc(result.get('error') or 'unhealthy')}")
    label = "live" if sessions.get("live") else "cached"
    lines.append(f"<i>({label} check)</i>")
    return lines


def status_keyboard(*, is_admin: bool, live: bool = False) -> InlineKeyboardMarkup:
    """The dashboard's inline buttons.

    Refresh is always offered; the worker recycle is admin-only, and lives on
    its own row so a mistimed press cannot land on it.
    """
    rows = [[InlineKeyboardButton(REFRESH_LABEL, callback_data=refresh_data(live=live))]]
    if is_admin:
        rows.append([InlineKeyboardButton(RESTART_WORKER_LABEL, callback_data=restart_data())])
    return InlineKeyboardMarkup(rows)


def refresh_data(*, live: bool = False) -> str:
    """Callback data for Refresh, carrying the ``live`` mode across the press."""
    return f"{STATUS_CALLBACK_PREFIX}{REFRESH_ACTION}" + (":live" if live else "")


def restart_data() -> str:
    """Callback data for the admin Restart-worker button."""
    return f"{STATUS_CALLBACK_PREFIX}{RESTART_ACTION}"


def parse_status_callback(data) -> tuple[str, bool] | None:
    """Return ``(action, live)`` for a dashboard button press, or ``None``.

    ``live`` is whatever the message was rendered with, so pressing Refresh
    after ``/session_status live`` keeps running the real session check instead
    of quietly downgrading to the cached one.
    """
    if not isinstance(data, str) or not data.startswith(STATUS_CALLBACK_PREFIX):
        return None
    action, _, flag = data[len(STATUS_CALLBACK_PREFIX) :].strip().partition(":")
    if action not in (REFRESH_ACTION, RESTART_ACTION):
        return None
    return action, flag.strip().lower() == "live"


def _num(value) -> str:
    if value is None:
        return "?"
    return str(value)


def _bytes_human(value) -> str:
    """Render a byte count with a unit that fits the magnitude (``?`` if unknown)."""
    number = _int_or_none(value)
    if number is None:
        return "?"
    sign = "-" if number < 0 else ""
    magnitude = abs(number)
    for unit, factor in (("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)):
        if magnitude >= factor:
            return f"{sign}{magnitude / factor:.1f} {unit}"
    return f"{sign}{magnitude} B"


def _mb(value) -> str:
    """Render a byte count as a short MB string (``?`` when unknown)."""
    try:
        number = int(value)
    except (TypeError, ValueError):
        return "?"
    sign = "-" if number < 0 else ""
    return f"{sign}{abs(number) / 1024 / 1024:.1f} MB"


def _esc(value) -> str:
    """Escape the few characters that would break Telegram HTML."""
    text = "" if value is None else str(value)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
