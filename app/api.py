from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
import os
from motor.motor_asyncio import AsyncIOMotorClient
from app.config.settings import settings
from app.services.tokenizer import tokenize_query
import sys
import httpx
import html
from app.utils.logger import logger
import asyncio
from asyncio.subprocess import PIPE

app = FastAPI(title="TG File Index API")

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
        try:
            client.close()
        except Exception:
            pass
        app.state.db = None


@app.on_event("shutdown")
async def shutdown():
    mongo_client = getattr(app.state, "mongo_client", None)
    if mongo_client:
        try:
            mongo_client.close()
        except Exception:
            pass


@app.get("/favicon.ico")
async def favicon():
    path = os.path.join(os.path.dirname(__file__), "static", "favicon.ico")
    if os.path.exists(path):
        return FileResponse(path)
    raise HTTPException(status_code=404)


@app.get("/")
async def root_get():
    return {"ok": True, "service": "tg-index-search-bot"}


@app.post("/")
async def root_post(payload: dict = None):
    # If Telegram sends updates to the root path (no token in URL),
    # forward to the webhook handler using the configured bot token
    # when available. Otherwise, acknowledge.
    logger.debug("root_post received payload keys: {}", list(payload.keys()) if isinstance(payload, dict) else type(payload))
    if payload and ("message" in payload or "edited_message" in payload):
        # try to find a configured bot token
        token = None
        for c in settings.API_CREDENTIALS:
            if c.get("bot_token"):
                token = c.get("bot_token")
                break
        if token:
            try:
                logger.info("Forwarding root POST update to webhook handler using configured bot token (background)")
                # schedule the webhook handler in background and return immediately
                try:
                    asyncio.create_task(telegram_webhook(token, payload))
                except Exception:
                    # fallback: call without scheduling if loop unavailable
                    await telegram_webhook(token, payload)
                return {"ok": True}
            except Exception as exc:
                logger.exception("Error forwarding root POST to webhook: {}", exc)
                # swallow errors to ensure Telegram gets 200
                return {"ok": True}
    # Basic acknowledgement for other POSTs
    return {"ok": True}



@app.post("/webhook/{token}")
async def telegram_webhook(token: str, update: dict):
    """Process Telegram Bot API webhook updates for configured bot tokens.

    Supports a minimal subset: responds to `/search <query>` by running the
    same search logic and sending a message via the Bot HTTP API.
    """
    # validate token exists in configured credentials
    creds = [c for c in settings.API_CREDENTIALS if c.get("bot_token") == token]
    if not creds:
        logger.warning("Received webhook for unknown token")
        return JSONResponse(status_code=404, content={"ok": False, "error": "unknown token"})

    # find message payload
    message = update.get("message") or update.get("edited_message") or {}
    update_id = update.get("update_id")
    text = (message.get("text") or message.get("caption") or "").strip()
    chat_id = (message.get("chat") or {}).get("id")
    text_trunc = (text[:120] + "...") if len(text) > 120 else text
    logger.info("webhook token={} update_id={} chat={} text={}", "[REDACTED]", update_id, chat_id, text_trunc)
    if not text:
        logger.debug("No text/caption in incoming update, ignoring")
        return {"ok": True}

    # handle /search command
    if text.startswith("/search"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            # Escape angle brackets to avoid Telegram HTML parse errors
            reply_text = "Usage: /search &lt;query&gt;"
        else:
            query = parts[1].strip()
            # schedule search+reply in background so webhook returns quickly
            try:
                asyncio.create_task(_process_search_and_send(token, chat_id, query))
            except Exception:
                # if loop not available, run inline (rare)
                await _process_search_and_send(token, chat_id, query)
            # acknowledge immediately
            return {"ok": True}

    # handle other commands
    # normalize command without botname suffix
    cmd = text.split(maxsplit=1)[0].split("@", 1)[0]
    if cmd == "/start":
        welcome = "Hi — I can search indexed files. Use /search <query> to search."
        await _send_tg(token, chat_id, welcome)
        return {"ok": True}

    if cmd == "/help":
        help_text = "Commands:\n/search <query> — Search files\n/stats — Indexed file counts\n/reindex <chat_id> — Backfill chat history\n/health — DB health"
        await _send_tg(token, chat_id, help_text)
        return {"ok": True}

    if cmd == "/stats":
        db = getattr(app.state, "db", None)
        if db is None:
            await _send_tg(token, chat_id, "Stats: DB unavailable")
        else:
            try:
                total = await db.get_collection("files").estimated_document_count()
                dups = await db.get_collection("files").count_documents({"is_duplicate": True})
                await _send_tg(token, chat_id, f"Total files: {total}\nDuplicates: {dups}")
            except Exception as exc:
                logger.exception("/stats handler failed: {}", exc)
                await _send_tg(token, chat_id, "Stats: error")
        return {"ok": True}

    if cmd == "/health":
        mongo_client = getattr(app.state, "mongo_client", None)
        db = getattr(app.state, "db", None)
        if mongo_client is None or db is None:
            await _send_tg(token, chat_id, "Health: DB unavailable")
        else:
            try:
                await mongo_client.admin.command("ping")
                await _send_tg(token, chat_id, "Health: OK")
            except Exception as exc:
                logger.exception("/health handler failed: {}", exc)
                await _send_tg(token, chat_id, f"Health: error: {exc}")
        return {"ok": True}

    if cmd == "/reindex":
        # /reindex <chat_id> optionally. We'll spawn scripts/backfill.py as a subprocess
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await _send_tg(token, chat_id, "Usage: /reindex <chat_id>")
            return {"ok": True}
        target = parts[1].strip()
        try:
            target_chat_id = int(target)
        except Exception:
            await _send_tg(token, chat_id, "Invalid chat id")
            return {"ok": True}

        # spawn backfill script in background using same python executable
        try:
            cmd = [sys.executable, "scripts/backfill.py"]
            # pass target via env var TARGET_CHAT_ID for the script
            env = os.environ.copy()
            env["TARGET_CHAT_ID"] = str(target_chat_id)
            # Ensure subprocess can import local package `app`
            project_root = os.path.dirname(os.path.dirname(__file__))
            prev = env.get("PYTHONPATH", "")
            env["PYTHONPATH"] = project_root + (os.pathsep + prev if prev else "")

            async def _spawn_and_log(cmd, env, cwd):
                proc = await asyncio.create_subprocess_exec(*cmd, env=env, cwd=cwd, stdout=PIPE, stderr=PIPE)

                # notify owner immediately that the subprocess has started
                try:
                    owner = settings.OWNER_ID
                    if owner and int(owner) != int(chat_id):
                        try:
                            await _send_tg(token, int(owner), f"Backfill subprocess started for {target_chat_id} (requested by {chat_id})")
                        except Exception:
                            pass
                except Exception:
                    pass

                async def _drain(stream, level="info"):
                    try:
                        while True:
                            line = await stream.readline()
                            if not line:
                                break
                            text = line.decode(errors="replace").rstrip()
                            if not text:
                                continue
                            if level == "info":
                                logger.info("[backfill] {}", text)
                            else:
                                logger.error("[backfill] {}", text)
                    except Exception:
                        logger.exception("Error reading subprocess stream")

                # schedule draining stdout and stderr
                asyncio.create_task(_drain(proc.stdout, "info"))
                asyncio.create_task(_drain(proc.stderr, "error"))
                # don't await proc here; let it run independently

            asyncio.create_task(_spawn_and_log(cmd, env, project_root))
            await _send_tg(token, chat_id, f"Reindex scheduled for {target_chat_id}")
            # notify owner that reindex was scheduled
            try:
                owner = settings.OWNER_ID
                if owner and int(owner) != int(chat_id):
                    await _send_tg(token, int(owner), f"Reindex scheduled for {target_chat_id} by {chat_id}")
            except Exception:
                pass
        except Exception as exc:
            logger.exception("Failed to schedule reindex: {}", exc)
            await _send_tg(token, chat_id, "Failed to schedule reindex")
            try:
                owner = settings.OWNER_ID
                if owner:
                    await _send_tg(token, int(owner), f"Failed to schedule reindex for {target}: {exc}")
            except Exception:
                pass
        return {"ok": True}

    return {"ok": True}


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
async def health():
    mongo_client = getattr(app.state, "mongo_client", None)
    db = getattr(app.state, "db", None)
    try:
        if mongo_client is None or db is None:
            raise Exception("MongoDB not configured")
        await mongo_client.admin.command("ping")
        files = await db.get_collection("files").estimated_document_count()
        return {"ok": True, "files": files}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.get("/search")
async def api_search(
    q: str = Query(..., min_length=1, max_length=128),
    page: int = 1,
    per_page: int = 5,
    output_format: str = Query("json"),
):
    tokens = tokenize_query(q)
    if not tokens:
        return {"results": [], "total": 0}

    db = getattr(app.state, "db", None)
    if db is None:
        logger.warning("api_search: MongoDB unavailable, returning empty results for query=%s", q)
        return {"results": [], "total": 0}

    try:
        coll = db.get_collection("files")
    except Exception as exc:
        logger.exception("api_search: failed to get collection: {}", exc)
        return {"results": [], "total": 0}

    # strict match
    strict_filter = {"title_tokens": {"$all": tokens}}
    projection = {
        "_id": 0,
        "chat_id": 1,
        "message_id": 1,
        "filename": 1,
        "timestamp": 1,
        "title_tokens": 1,
        "quality_tokens": 1,
        "codec_tokens": 1,
        "year": 1,
    }
    try:
        docs = await coll.find(strict_filter, projection).to_list(length=1000)
    except Exception as exc:
        logger.exception("api_search: DB query failed: {}", exc)
        return {"results": [], "total": 0}

    results = []
    qlower = q.lower()
    for doc in docs:
        score = 0
        doc_titles = [t.lower() for t in doc.get("title_tokens", [])]
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

    # fallback
    if not results:
        or_clauses = []
        for t in tokens:
            or_clauses.append({"title_tokens": {"$elemMatch": {"$regex": f'^{t}', "$options": "i"}}})
            or_clauses.append({"filename": {"$regex": f'{t}', "$options": "i"}})
        try:
            docs2 = await coll.find({"$or": or_clauses}, projection).to_list(length=500)
        except Exception as exc:
            logger.exception("api_search: fallback DB query failed: {}", exc)
            return {"results": [], "total": 0}
        for doc in docs2:
            score = 0
            doc_titles = [t.lower() for t in doc.get("title_tokens", [])]
            matched = sum(1 for t in tokens if any(tt.startswith(t.lower()) for tt in doc_titles))
            score += matched * 8
            if qlower and qlower in doc.get("filename", "").lower():
                score += 2
            fname_len = len(doc.get("filename", ""))
            score -= fname_len / 300.0
            doc["_score"] = score
            results.append(doc)

    results.sort(key=lambda r: (r.get("_score", 0), r.get("timestamp")), reverse=True)
    total = len(results)
    start = (page - 1) * per_page
    end = start + per_page

    page_results = results[start:end]

    if str(output_format).lower() in ("md", "markdown"):
        lines = []
        for r in page_results:
            fname = r.get("filename", "-")
            display = str(fname).replace("\n", " ").strip()
            url = ""
            try:
                chat_id = r.get("chat_id")
                message_id = r.get("message_id")
                if chat_id and message_id:
                    s = str(chat_id)
                    base = s[4:] if s.startswith("-100") else s.lstrip("-")
                    url = f"https://t.me/c/{base}/{message_id}"
            except Exception:
                url = ""
            if url:
                lines.append(f"[{display}]({url})")
            else:
                lines.append(f"- {display}")
        md_text = "\n".join(lines)
        return PlainTextResponse(md_text, media_type="text/markdown")

    return {"results": page_results, "total": total, "page": page, "per_page": per_page}
