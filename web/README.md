# Web component for media conversion

This folder contains a minimal Flask backend and a small responsive frontend to upload media, run ffmpeg conversions, and download results.

Requirements
- system: `ffmpeg` and `ffprobe` available on PATH
- Python packages: see `requirements_web.txt`

Run (development):

```powershell
python -m pip install -r web\requirements_web.txt
python web\webapp.py
```

Open http://localhost:5000 in your browser.
