Render deployment steps — Media Conversion Bot

Prerequisites
- Have a Render account
- Add repository to Render
- Ensure environment variables ready: BOT_TOKEN, (optional) WEBHOOK_URL, MONGODB_URI, SENTRY_DSN, ADMIN_USER_ID, ALLOWED_USER_IDS

Recommended run mode on Render
- Use ASGI (webhook mode) with Uvicorn so Render can route HTTP traffic to the webhook endpoint.
- The project exposes `app` (FastAPI) in `main.py` and will start the bot in background on Uvicorn startup.

Steps
1. Push code to your GitHub repo.
2. On Render: New → Web Service → Connect your repo.
3. In "Environment": set `BOT_TOKEN`.
   - If you plan to use webhooks, set `WEBHOOK_URL` to https://<your-service>.onrender.com/telegram/webhook
   - (Optional, recommended) set `WEBHOOK_SECRET` to a random secret string. The app will configure Telegram with this `secret_token` and validate incoming requests using the `X-Telegram-Bot-Api-Secret-Token` header.
4. build command: leave empty (Render auto-detects) or set `pip install -r requirements.txt`
5. start command: `uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4`
6. Set health check to `/health`.
7. Deploy and monitor logs. If `FFmpeg` is not present or you encounter conversion errors, consider using a custom build image or preinstalling FFmpeg via a Dockerfile (optional).

Notes
- `BOT_TOKEN` is required; app raises an error if missing.
- Polling mode (`python main.py`) works but is not recommended on Render.
- If using `WEBHOOK_URL`, make sure Render's service URL is correctly used and reachable from Telegram.
- Ensure `ffmpeg` binary is available on the host; Render default containers may not include `ffmpeg`.

Advanced: If you require `ffmpeg` on Render without Docker, use a build script in `render.yaml` to apt-get install ffmpeg during build (see Render docs).
