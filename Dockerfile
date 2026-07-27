FROM python:3.12-slim

# Install ffmpeg, certificates, and build tools required for native Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for Docker layer caching
# This way, pip install only re-runs when requirements.txt changes
COPY requirements.txt ./

# Install Python deps
RUN pip install --upgrade pip setuptools wheel && pip install -r requirements.txt

# Copy the rest of the project
COPY . /app

# Environment defaults
ENV FFMPEG_PATH=/usr/bin/ffmpeg FFPROBE_PATH=/usr/bin/ffprobe PORT=10000 HEALTHCHECK_PORT=9000

RUN useradd -m botuser && chown -R botuser /app

EXPOSE 10000

# Ensure start script is executable and use it as the container entrypoint.
RUN chmod +x /app/start.sh || true
USER botuser
CMD ["/app/start.sh"]
