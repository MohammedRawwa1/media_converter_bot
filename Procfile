# ── Memory-conscious configuration ──
# Render free tier has ~512MB RAM. Using 2 workers (instead of 4) leaves
# headroom for the in-process ffmpeg worker, OS overhead, and Redis/Mongo
# connections. Each uvicorn worker consumes ~50-80MB.
# Upgrade to workers=4 on paid plans (1GB+ RAM).
web: uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2
fetcher: python -u fetcher/service.py
telethon_ingest: python -u tools/telethon_ingest.py
