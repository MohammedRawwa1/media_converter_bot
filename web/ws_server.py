import asyncio
import contextlib
import logging
import re
import threading
from collections import defaultdict

logger = logging.getLogger(__name__)

try:
    import websockets
except Exception:
    websockets = None

# Use the async redis helper used elsewhere
try:
    from utils.job_queue import get_redis
except Exception:
    get_redis = None

# Mapping from job_id -> set of websocket connections
_clients = defaultdict(set)
_clients_lock = asyncio.Lock()


async def _register(ws, job_id: str):
    async with _clients_lock:
        _clients[job_id].add(ws)


async def _unregister(ws, job_id: str):
    async with _clients_lock:
        s = _clients.get(job_id)
        if s and ws in s:
            s.remove(ws)
        if s and len(s) == 0:
            _clients.pop(job_id, None)


async def _ws_handler(websocket, path):
    # Expect path like /ws/<job_id>
    m = re.match(r"^/ws/(?P<job_id>[^/]+)$", path)
    if not m:
        with contextlib.suppress(Exception):
            await websocket.close()
        return

    job_id = m.group("job_id")
    await _register(websocket, job_id)
    logger.info("WebSocket connected for job %s", job_id)
    try:
        await websocket.wait_closed()
    finally:
        await _unregister(websocket, job_id)
        logger.info("WebSocket disconnected for job %s", job_id)


async def _redis_listener():
    if get_redis is None:
        logger.warning("No async redis available for ws server")
        await asyncio.sleep(1)
        return

    try:
        r = await get_redis()
    except Exception as e:
        logger.exception("Failed to create redis client for ws server: %s", e)
        return

    pub = None
    try:
        pub = r.pubsub()
        await pub.psubscribe("ffmpeg:progress:*")

        while True:
            msg = await pub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if not msg:
                await asyncio.sleep(0.01)
                continue

            # msg may be of type pmessage when using pattern subscribe
            # normalize channel and data
            try:
                msg.get("type")
                channel = msg.get("channel") or msg.get("pattern") or msg.get("channel")
                data = msg.get("data")
                if isinstance(channel, (bytes, bytearray)):
                    channel = channel.decode(errors="ignore")
                if isinstance(data, (bytes, bytearray)):
                    try:
                        data = data.decode("utf-8")
                    except Exception:
                        data = str(data)

                # parse job id from channel
                if not channel:
                    continue
                parts = channel.split(":")
                job_id = parts[-1]

                # broadcast to clients for this job
                async with _clients_lock:
                    targets = list(_clients.get(job_id, set()))

                if not targets:
                    continue

                for ws in targets:
                    with contextlib.suppress(Exception):
                        await ws.send(data)
            except Exception:
                logger.exception("Error processing redis message for ws server")
                continue

    finally:
        try:
            if pub:
                await pub.close()
        except Exception:
            pass
        with contextlib.suppress(Exception):
            await r.close()


async def _serve(host: str = "127.0.0.1", port: int = 6789):
    if websockets is None:
        logger.error("websockets package not available; cannot start ws server")
        return

    server = await websockets.serve(_ws_handler, host, port)
    logger.info("WebSocket server started on %s:%s", host, port)

    # Run redis listener concurrently
    listener = asyncio.create_task(_redis_listener())

    # wait forever
    try:
        await asyncio.Future()
    finally:
        listener.cancel()
        server.close()
        await server.wait_closed()


def start_in_thread(host: str = "127.0.0.1", port: int = 6789):
    """Start the websocket server in a background thread and return the Thread object."""

    def _run_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_serve(host=host, port=port))
        except Exception:
            logger.exception("WS server loop error")
        finally:
            with contextlib.suppress(Exception):
                loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    t = threading.Thread(target=_run_loop, daemon=True)
    t.start()
    return t
