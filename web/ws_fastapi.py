"""
FastAPI WebSocket & SSE manager for real-time conversion progress.

Provides two real-time progress endpoints on the same port as the main app:

- ``/ws/{job_id}`` (WebSocket) — preferred, bidirectional
- ``/events/{job_id}`` (Server-Sent Events) — async replacement for the legacy
  Flask ``/events/<job_id>`` endpoint which blocked a WSGI thread per connection.

Replaces the standalone websockets server (``web/ws_server.py``) which ran on a
separate port (6789) that was inaccessible in production environments like
Railway.

Usage in main.py::

    from web.ws_fastapi import (
        ws_router,
        start_ws_listener,
        stop_ws_listener,
        sse_router,
    )

    app.include_router(ws_router)
    app.include_router(sse_router)

    @app.on_event("startup")
    async def startup():
        start_ws_listener(app)

    @app.on_event("shutdown")
    async def shutdown():
        await stop_ws_listener(app)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections import defaultdict

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)

# ── Client registry ────────────────────────────────────────────────────────
# Mapping: job_id -> set of active WebSocket connections.
_clients: dict[str, set[WebSocket]] = defaultdict(set)


async def _register(ws: WebSocket, job_id: str) -> None:
    _clients[job_id].add(ws)


async def _unregister(ws: WebSocket, job_id: str) -> None:
    s = _clients.get(job_id)
    if s:
        s.discard(ws)
        if not s:
            _clients.pop(job_id, None)


# ── WebSocket endpoint ─────────────────────────────────────────────────────

ws_router = APIRouter()


@ws_router.websocket("/ws/{job_id}")
async def websocket_progress(websocket: WebSocket, job_id: str):
    await websocket.accept()
    await _register(websocket, job_id)
    logger.info("WebSocket connected for job %s", job_id)
    try:
        while True:
            # Keep the connection alive by waiting for messages (or disconnect).
            # We don't expect incoming messages from the client — this just
            # lets us detect when the client disconnects.
            await websocket.receive_text()
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for job %s", job_id)
    except Exception:
        logger.debug("WebSocket error for job %s", job_id)
    finally:
        await _unregister(websocket, job_id)


# ── Server-Sent Events endpoint (async Redis, replaces legacy Flask /events) ──

sse_router = APIRouter()


@sse_router.get("/events/{job_id}")
async def sse_events(job_id: str):
    """Stream progress events for a job via Server-Sent Events.

    Uses async Redis pubsub to subscribe to ``ffmpeg:progress:{job_id}``.
    Falls back to HTTP polling if Redis is unavailable.

    This is the async equivalent of the legacy Flask ``/events/<job_id>``
    endpoint (``web/webapp.py``). Unlike the Flask version, it does NOT
    consume a WSGI thread — ``StreamingResponse`` works with async generators
    directly in the event loop, making it much more scalable.
    """

    async def _event_generator():
        """Async generator that yields SSE-formatted data.

        Uses a single Redis connection for both the initial hash fetch and
        the long-lived pubsub subscription, avoiding the two-connection
        overhead of the previous implementation.
        """
        r = None
        pub = None
        try:
            from utils.job_queue import get_redis as _get_async_redis

            if _get_async_redis is None:
                yield f"data: {json.dumps({'status': 'queued', 'progress': 0, 'message': 'Redis not available, use HTTP polling'})}\n\n"
                return

            # Open a single Redis connection for both initial state + pubsub
            r = await _get_async_redis()

            # ── Emit current job hash state ──
            try:
                _initial = await r.hgetall(f"ffmpeg:job:{job_id}")
                if _initial:
                    _decoded = {
                        k.decode() if isinstance(k, bytes) else k:
                        v.decode() if isinstance(v, bytes) else v
                        for k, v in _initial.items()
                    }
                    yield f"data: {json.dumps(_decoded)}\n\n"
            except Exception:
                pass

            # ── Subscribe to the progress channel on the same connection ──
            try:
                pub = r.pubsub()
                await pub.subscribe(f"ffmpeg:progress:{job_id}")
            except Exception as e:
                logger.debug("sse_events: Redis pubsub unavailable for %s: %s", job_id, e)
                yield f"data: {json.dumps({'status': 'queued', 'progress': 0, 'message': 'Redis unavailable'})}\n\n"
                return

            # Generator loop: read messages from Redis pubsub
            while True:
                try:
                    msg = await pub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if msg is None:
                        # Send keepalive comment to prevent proxy timeouts
                        yield ": keepalive\n\n"
                        continue

                    data = msg.get("data")
                    if isinstance(data, (bytes, bytearray)):
                        data = data.decode("utf-8", errors="replace")
                    if data:
                        yield f"data: {data}\n\n"

                except asyncio.CancelledError:
                    logger.debug("sse_events: client disconnected for %s", job_id)
                    break
                except Exception:
                    logger.debug("sse_events: error reading message for %s", job_id)
                    await asyncio.sleep(1)
                    continue

        except asyncio.CancelledError:
            logger.debug("sse_events: generator cancelled for %s", job_id)
        except Exception:
            logger.exception("sse_events: unexpected error for %s", job_id)
        finally:
            try:
                if pub is not None:
                    await pub.close()
            except Exception:
                pass
            try:
                if r is not None:
                    await r.close()
            except Exception:
                pass

    return StreamingResponse(
        _event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


# ── Background Redis listener (for WebSocket broadcast) ─────────────────────


def start_ws_listener(app):
    """Start the background Redis pub/sub listener as a FastAPI lifecycle task.

    Stores the task on ``app.state.ws_listener_task`` so it can be cancelled
    during shutdown. The listener subscribes to ``ffmpeg:progress:*`` and
    broadcasts messages to all connected WebSocket clients.
    """
    try:
        task = asyncio.create_task(_redis_listener())
        app.state.ws_listener_task = task
        logger.info("WebSocket Redis listener started")
    except Exception as e:
        logger.warning("Failed to start WebSocket Redis listener: %s", e)


async def stop_ws_listener(app):
    """Cancel the background Redis listener on shutdown."""
    task = getattr(app.state, "ws_listener_task", None)
    if task is not None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        logger.info("WebSocket Redis listener stopped")

    # Close all remaining WebSocket connections
    for job_id, clients in list(_clients.items()):
        for ws in list(clients):
            with contextlib.suppress(Exception):
                await ws.close()
        _clients.pop(job_id, None)


async def _redis_listener():
    """Subscribe to ``ffmpeg:progress:*`` and broadcast to WebSocket clients."""
    try:
        from utils.job_queue import get_redis
    except Exception:
        logger.warning("job_queue not available; WebSocket Redis listener disabled")
        return

    if get_redis is None:
        logger.warning("No async redis available for WebSocket listener")
        return

    r = None
    pub = None
    try:
        r = await get_redis()
        pub = r.pubsub()
        await pub.psubscribe("ffmpeg:progress:*")

        while True:
            msg = await pub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if not msg:
                await asyncio.sleep(0.01)
                continue

            try:
                channel = msg.get("channel") or ""
                data = msg.get("data")
                if isinstance(channel, (bytes, bytearray)):
                    channel = channel.decode(errors="ignore")
                if isinstance(data, (bytes, bytearray)):
                    try:
                        data = data.decode("utf-8")
                    except Exception:
                        data = str(data)

                # Parse job_id from channel: ffmpeg:progress:{job_id}
                if not channel:
                    continue
                parts = channel.split(":")
                job_id = parts[-1]

                # Broadcast to all connected clients for this job
                targets = list(_clients.get(job_id, set()))
                if not targets:
                    continue

                for ws in targets:
                    with contextlib.suppress(Exception):
                        await ws.send_text(data)
            except Exception:
                logger.exception("Error processing Redis message for WebSocket")
                continue

    except asyncio.CancelledError:
        logger.debug("WebSocket Redis listener cancelled")
        raise
    except Exception:
        logger.exception("WebSocket Redis listener error")
    finally:
        try:
            if pub is not None:
                await pub.close()
        except Exception:
            logger.debug("ws_fastapi: failed to close pubsub")
        with contextlib.suppress(Exception):
            if r is not None:
                await r.close()
