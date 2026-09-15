"""The ``/session_status`` dashboard: who is using the bot and what is queued.

Answers the three questions the existing ``/loginstatus`` does not:

1. How busy is the bot?      -> waiting / running / delayed jobs, plus broker depths
2. Who is using it right now? -> the Redis presence heartbeat (``utils.presence``)
3. When does my work run?    -> the queue turn, i.e. the position of a user's
                                oldest waiting job in the FIFO job list

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
import time
from collections import Counter

from utils import presence
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


async def collect_session_status(*, user_id=None, is_admin=False, live_sessions=False) -> dict:
    """Gather the whole dashboard. Never raises; degrades field by field."""
    redis_snapshot = await _redis_snapshot()
    broker_snapshot = await _broker_snapshot()
    online_snapshot = await presence.snapshot()
    sessions = await _sessions_snapshot(user_id=user_id, live=live_sessions)

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
    payload["users"]["source"] = online_snapshot.get("source")
    payload["users"]["error"] = online_snapshot.get("error")
    payload["sessions"] = sessions
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


def _overall_status(redis_snapshot: dict, broker_snapshot: dict) -> str:
    """``degraded`` when a pipe that should be serving cannot, else ``ok``."""
    if not redis_snapshot.get("connected"):
        return "degraded"
    if broker_snapshot.get("configured") and broker_snapshot.get("queues") == {} and broker_snapshot.get("error"):
        return "degraded"
    return "ok"


# ── Aggregation (pure) ──────────────────────────────────────────────────


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


def format_status(payload: dict, *, is_admin: bool) -> str:
    """Render the dashboard as Telegram HTML."""
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


def _num(value) -> str:
    if value is None:
        return "?"
    return str(value)


def _esc(value) -> str:
    """Escape the few characters that would break Telegram HTML."""
    text = "" if value is None else str(value)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
