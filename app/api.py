import asyncio
import contextlib
import html
import os
import re
import sys
from asyncio.subprocess import PIPE

import httpx
from app.config.settings import settings
from app.services.tokenizer import tokenize_query
from app.utils.logger import logger
from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from motor.motor_asyncio import AsyncIOMotorClient

# Go-style standardized responses with consistent envelope
from utils.response import error, ok, paginated

# Route caching (read-through cache for GET responses)
from utils.route_cache import route_cache

# ── Go/Laravel-style security patterns ──
# Rate limiting for DoS/DDoS protection
from utils.web_rate_limiter import get_client_ip, make_rate_limit_response, web_rate_limiter

# Go-style middleware chain with built-in middleware
from web.middleware import (
    MiddlewareChain,
    RequestContext,
    with_request_id,
    with_request_logging,
)

app = FastAPI(title="TG File Index API")

# ── Go-style middleware chain (order matters: first = outermost) ──
# Rate limiting is handled per-endpoint (inline) to preserve different limits
# per route. The chain handles cross-cutting concerns:
chain = MiddlewareChain()
chain.use(with_request_id)            # unique request ID per call
chain.use(with_request_logging)        # log every request/response with timing
# with_rate_limit and with_cache are handled per-endpoint for granularity

# mount static for favicon
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


def _get_db():
    uri = settings.MONGO_URI
    client = AsyncIOMotorClient(uri)
    db = client[settings.DB_NAME]
    return db


@app.on_event("startup")
async def startup():
    # Try to establish a working MongoDB connection during startup.
    uri = settings.MONGO_URI
    if not uri:
        logger.warning("No MONGO_URI configured; running without DB")
        app.state.db = None
        return

    # Create client with a short selection timeout so startup fails fast when DB unreachable
    client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=2000, connectTimeoutMS=2000)
    try:
        # attempt a lightweight ping to verify connectivity
        await client.admin.command("ping")
        app.state.db = client[settings.DB_NAME]
        # keep client accessible for shutdown (store client separately)
        app.state.mongo_client = client
        logger.info("Connected to MongoDB")
    except Exception as exc:
        logger.warning("MongoDB not reachable at startup, continuing without DB: {}", exc)
        with contextlib.suppress(Exception):
            client.close()
        app.state.db = None


@app.on_event("shutdown")
async def shutdown():
    mongo_client = getattr(app.state, "mongo_client", None)
    if mongo_client:
        with contextlib.suppress(Exception):
            mongo_client.close()


@app.get("/favicon.ico")
@chain("/favicon.ico", methods=["GET"], framework="fastapi")
async def favicon(ctx: RequestContext, request: Request = None):
    """Favicon endpoint (Go-style: middleware + standardized response)."""
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("health", client_ip):
        body, status_code, headers = make_rate_limit_response("health", client_ip)
        return JSONResponse(status_code=status_code, content=body, headers=headers)
    path = os.path.join(os.path.dirname(__file__), "static", "favicon.ico")
    if os.path.exists(path):
        return FileResponse(path)
    return error("not_found", "Favicon not found", 404)


@app.get("/")
@chain("/", methods=["GET"], framework="fastapi")
async def root_get(ctx: RequestContext, request: Request):
    """Root health check (Go-style: middleware + consistent envelope)."""
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("search", client_ip):
        return make_rate_limit_response("search", client_ip)
    return ok({"service": "tg-index-search-bot"}, message="healthy")


@app.post("/")
@chain("/", methods=["POST"], framework="fastapi")
async def root_post(ctx: RequestContext, request: Request = None, payload: dict = None):
    """Root POST endpoint - forwards Telegram updates to webhook handler."""
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("webhook", client_ip):
        return make_rate_limit_response("webhook", client_ip)
    logger.debug("root_post received payload keys: {}", list(payload.keys()) if isinstance(payload, dict) else type(payload))
    if payload and ("message" in payload or "edited_message" in payload):
        token = None
        for c in settings.API_CREDENTIALS:
            if c.get("bot_token"):
                token = c.get("bot_token")
                break
        if token:
            try:
                logger.info("Forwarding root POST update to webhook handler (background)")
                try:
                    asyncio.create_task(telegram_webhook(token, payload))
                except Exception:
                    await telegram_webhook(token, payload)
                return ok(None, message="forwarded")
            except Exception:
                logger.exception("Error forwarding root POST to webhook")
                return ok(None, message="acknowledged")
    return ok(None, message="acknowledged")



@app.post("/webhook/{token}")
@chain("/webhook/{token}", methods=["POST"], framework="fastapi")
async def telegram_webhook(ctx: RequestContext, token: str, update: dict, request: Request = None):
    """Process Telegram Bot API webhook updates for configured bot tokens.
    
    Go-style: middleware + standardized envelope. Rate limiting per-endpoint.
    """
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("webhook", client_ip):
        return make_rate_limit_response("webhook", client_ip)
    creds = [c for c in settings.API_CREDENTIALS if c.get("bot_token") == token]
    if not creds:
        logger.warning("Received webhook for unknown token")
        return error("unknown_token", "Unknown bot token", 404)

    message = update.get("message") or update.get("edited_message") or {}
    update_id = update.get("update_id")
    text = (message.get("text") or message.get("caption") or "").strip()
    chat_id = (message.get("chat") or {}).get("id")
    text_trunc = (text[:120] + "...") if len(text) > 120 else text
    logger.info("webhook token={} update_id={} chat={} text={}", "[REDACTED]", update_id, chat_id, text_trunc)
    
    if not text:
        logger.debug("No text/caption in incoming update, ignoring")
        return ok(None, message="ignored")

    cmd = text.split(maxsplit=1)[0].split("@", 1)[0]

    if text.startswith("/search"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await _send_tg(token, chat_id, "Usage: /search &lt;query&gt;")
        else:
            query = parts[1].strip()
            try:
                asyncio.create_task(_process_search_and_send(token, chat_id, query))
            except Exception:
                await _process_search_and_send(token, chat_id, query)
        return ok(None, message="search_scheduled")

    if cmd == "/start":
        await _send_tg(token, chat_id, "Hi — I can search indexed files. Use /search <query> to search.")
        return ok(None, message="started")

    if cmd == "/help":
        await _send_tg(token, chat_id, "Commands: /search <query>, /stats, /reindex <chat_id>, /health")
        return ok(None, message="help_sent")

    if cmd == "/stats":
        db = getattr(app.state, "db", None)
        if db is None:
            await _send_tg(token, chat_id, "Stats: DB unavailable")
        else:
            try:
                total = await db.get_collection("files").estimated_document_count()
                await _send_tg(token, chat_id, f"Total files: {total}")
            except Exception:
                logger.exception("/stats handler failed")
                await _send_tg(token, chat_id, "Stats: error")
        return ok(None, message="stats_sent")

    if cmd == "/health":
        mongo_client = getattr(app.state, "mongo_client", None)
        if mongo_client is not None:
            try:
                await mongo_client.admin.command("ping")
                await _send_tg(token, chat_id, "Health: OK")
            except Exception:
                logger.exception("/health handler failed")
                await _send_tg(token, chat_id, "Health: error")
        else:
            await _send_tg(token, chat_id, "Health: DB unavailable")
        return ok(None, message="health_sent")

    if cmd == "/reindex":
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await _send_tg(token, chat_id, "Usage: /reindex <chat_id>")
            return ok(None, message="usage_sent")
        target = parts[1].strip()
        try:
            target_chat_id = int(target)
        except Exception:
            await _send_tg(token, chat_id, "Invalid chat id")
            return ok(None, message="invalid_chat_id")

        try:
            cmd = [sys.executable, "scripts/backfill.py"]
            env = os.environ.copy()
            env["TARGET_CHAT_ID"] = str(target_chat_id)
            project_root = os.path.dirname(os.path.dirname(__file__))
            prev = env.get("PYTHONPATH", "")
            env["PYTHONPATH"] = project_root + (os.pathsep + prev if prev else "")

            async def _spawn_and_log(cmd, env, cwd):
                proc = await asyncio.create_subprocess_exec(*cmd, env=env, cwd=cwd, stdout=PIPE, stderr=PIPE)
                async def _drain(stream, level="info"):
                    try:
                        while True:
                            line = await stream.readline()
                            if not line:
                                break
                            text = line.decode(errors="replace").rstrip()
                            if not text:
                                continue
                            (logger.info if level == "info" else logger.error)("[backfill] {}", text)
                    except Exception:
                        logger.exception("Error reading subprocess stream")
                asyncio.create_task(_drain(proc.stdout, "info"))
                asyncio.create_task(_drain(proc.stderr, "error"))

            asyncio.create_task(_spawn_and_log(cmd, env, project_root))
            await _send_tg(token, chat_id, f"Reindex scheduled for {target_chat_id}")
        except Exception:
            logger.exception("Failed to schedule reindex")
            await _send_tg(token, chat_id, "Failed to schedule reindex")
        return ok(None, message="reindex_scheduled")

    return ok(None, message="acknowledged")


async def _process_search_and_send(token: str, chat_id: int, query: str) -> None:
    """Background task: run search and send reply via Telegram HTTP API."""
    try:
        res = await api_search(q=query, page=1, per_page=5)
    except Exception as exc:
        logger.exception("background search failed: {}", exc)
        res = None

    if res is None:
        reply_text = "Temporary error: search backend unavailable"
    else:
        results = res.get("results", [])
        total = res.get("total", 0)
        if not results:
            reply_text = f"<b>No results</b> for {html.escape(query)}"
        else:
            lines = [f"<b>Search:</b> {html.escape(query)} — {total} results"]
            for i, r in enumerate(results, start=1):
                fname = r.get("filename", "-")
                safe = html.escape(fname).replace("\n", " ")
                display = safe if len(safe) <= 80 else safe[:77] + "..."
                lines.append(f"{i}) {display}")
            reply_text = "\n".join(lines)

    tg_url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": reply_text, "parse_mode": "HTML"}
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(tg_url, json=payload, timeout=10)
            logger.info("Telegram sendMessage response: status={}, body={}", resp.status_code, resp.text[:400])
        except Exception as exc:
            logger.exception("Failed to POST to Telegram API (background): {}", exc)


def _score_search_results(docs: list, tokens: list, query: str, fallback: bool = False) -> list:
    """Score search results by relevance (pure function, no side effects)."""
    results = []
    qlower = query.lower()
    for doc in docs:
        score = 0
        doc_titles = [t.lower() for t in doc.get("title_tokens", [])]

        if fallback:
            matched = sum(1 for t in tokens if any(tt.startswith(t.lower()) for tt in doc_titles))
            score += matched * 8
            if qlower and qlower in doc.get("filename", "").lower():
                score += 2
            fname_len = len(doc.get("filename", ""))
            score -= fname_len / 300.0
        else:
            matched = sum(1 for t in tokens if t.lower() in doc_titles)
            score += matched * 10
            for qt in doc.get("quality_tokens", []):
                if qt and any(qt == t.lower() for t in tokens):
                    score += 6
            for cd in doc.get("codec_tokens", []):
                if cd and any(cd == t.lower() for t in tokens):
                    score += 5
            if doc.get("year") and any(str(doc.get("year")) == t for t in tokens):
                score += 8
            if qlower and qlower in doc.get("filename", "").lower():
                score += 3
            fname_len = len(doc.get("filename", ""))
            score -= fname_len / 200.0

        doc["_score"] = score
        results.append(doc)
    return results


def _make_tg_link(chat_id, message_id) -> str:
    """Generate a Telegram message link from chat_id and message_id."""
    if not chat_id or not message_id:
        return ""
    s = str(chat_id)
    base = s[4:] if s.startswith("-100") else s.lstrip("-")
    return f"https://t.me/c/{base}/{message_id}"


async def _send_tg(token: str, chat_id: int, text: str, parse_mode: str | None = None) -> None:
    """Utility to send a message via Telegram Bot API."""
    if chat_id is None:
        logger.warning("_send_tg called with no chat_id")
        return
    tg_url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode is not None:
        payload["parse_mode"] = parse_mode
    # simple retry loop for transient errors
    retries = 3
    backoff = 0.5
    async with httpx.AsyncClient() as client:
        for attempt in range(1, retries + 1):
            try:
                resp = await client.post(tg_url, json=payload, timeout=10)
                logger.info("Telegram sendMessage response: status={} body={}", resp.status_code, resp.text[:400])
                return
            except Exception as exc:
                logger.warning("Telegram send attempt %d failed: %s", attempt, exc)
                if attempt == retries:
                    logger.exception("Failed to POST to Telegram API after retries: {}", exc)
                else:
                    await asyncio.sleep(backoff * attempt)


@app.get("/health")
@chain("/health", methods=["GET"], framework="fastapi")
async def health(ctx: RequestContext, request: Request):
    """Health check endpoint (Go-style: middleware + cache + envelope)."""
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("health", client_ip):
        return make_rate_limit_response("health", client_ip)
    cache_key = "health"
    cached = await route_cache.aget(cache_key)
    if cached is not None:
        return cached

    mongo_client = getattr(app.state, "mongo_client", None)
    db = getattr(app.state, "db", None)
    try:
        if mongo_client is None or db is None:
            return error("service_unavailable", "Database not configured", status=503)
        await mongo_client.admin.command("ping")
        files = await db.get_collection("files").estimated_document_count()
        result = ok({"files": files}, message="healthy")
        await route_cache.aset(cache_key, result, ttl=30)
        return result
    except Exception:
        logger.exception("Health check failed")
        return error("health_check_failed", "Health check failed", status=500)


@app.get("/search")
@chain("/search", methods=["GET"], framework="fastapi")
async def api_search(
    ctx: RequestContext,
    q: str = Query(..., min_length=1, max_length=128),
    page: int = 1,
    per_page: int = 5,
    output_format: str = Query("json"),
    request: Request = None,
):
    """
    Go-style search endpoint with:
    - Per-endpoint rate limiting
    - Read-through route cache
    - Go-style standardized response envelope
    - Parameterized DB queries (no interpolation)
    - Fillable projection (no SELECT *)
    - Regex injection prevention (re.escape)
    """
    client_ip = get_client_ip(request)
    if not web_rate_limiter.check_limit("search", client_ip):
        return make_rate_limit_response("search", client_ip)
    cache_key = route_cache.make_search_key(q, page, per_page)
    cached = await route_cache.aget(cache_key)
    if cached is not None:
        return cached

    tokens = tokenize_query(q)
    if not tokens:
        result = ok({"results": [], "total": 0}, message="no tokens")
        await route_cache.aset(cache_key, result, ttl=30)
        return result

    db = getattr(app.state, "db", None)
    if db is None:
        logger.warning("api_search: DB unavailable for query=%s", q)
        return ok({"results": [], "total": 0}, message="db_unavailable")

    try:
        coll = db.get_collection("files")
    except Exception:
        logger.exception("api_search: failed to get collection")
        return ok({"results": [], "total": 0}, message="collection_error")

    # Fillable projection: only whitelisted fields (like SELECT specific columns)
    FILLABLE_PROJECTION = {
        "_id": 0, "chat_id": 1, "message_id": 1, "filename": 1,
        "timestamp": 1, "title_tokens": 1, "quality_tokens": 1,
        "codec_tokens": 1, "year": 1,
    }

    # Parameterized query (prepared-statement pattern)
    strict_filter = {"title_tokens": {"$all": tokens}}
    try:
        docs = await coll.find(strict_filter, FILLABLE_PROJECTION).to_list(length=1000)
    except Exception:
        logger.exception("api_search: DB query failed")
        return ok({"results": [], "total": 0}, message="query_error")

    results = _score_search_results(docs, tokens, q)

    if not results:
        or_clauses = []
        for t in tokens:
            escaped = re.escape(t)
            or_clauses.append({"title_tokens": {"$elemMatch": {"$regex": f'^{escaped}', "$options": "i"}}})
            or_clauses.append({"filename": {"$regex": f'{escaped}', "$options": "i"}})
        try:
            docs2 = await coll.find({"$or": or_clauses}, FILLABLE_PROJECTION).to_list(length=500)
            results2 = _score_search_results(docs2, tokens, q, fallback=True)
            results.extend(results2)
        except Exception:
            logger.exception("api_search: fallback query failed")

    results.sort(key=lambda r: (r.get("_score", 0), r.get("timestamp")), reverse=True)
    total = len(results)
    start = (page - 1) * per_page
    end = start + per_page
    page_results = results[start:end]

    if str(output_format).lower() in ("md", "markdown"):
        lines = []
        for r in page_results:
            display = str(r.get("filename", "-")).replace("\n", " ").strip()
            url = _make_tg_link(r.get("chat_id"), r.get("message_id"))
            lines.append(f"[{display}]({url})" if url else f"- {display}")
        return PlainTextResponse("\n".join(lines), media_type="text/markdown")

    result = paginated(page_results, total=total, page=page, per_page=per_page, message="search_results")
    await route_cache.aset(cache_key, result, ttl=30)
    return result
