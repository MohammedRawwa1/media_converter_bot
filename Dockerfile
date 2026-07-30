# ── Builder stage: compile native Python packages ──
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only requirements for Docker layer caching
COPY requirements.txt ./

# Install all deps (no --user flag — default prefix /usr/local is in sys.path)
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt


# ── Runtime stage: slim image with only runtime deps ──
FROM python:3.12-slim

# Install only runtime OS packages (no build-essential!)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy pre-compiled wheels from builder stage
# /usr/local/lib/python3.12/site-packages is in Python's default sys.path
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy the rest of the project
COPY . /app

# Environment defaults
ENV FFMPEG_PATH=/usr/bin/ffmpeg \
    FFPROBE_PATH=/usr/bin/ffprobe \
    PORT=10000 \
    HEALTHCHECK_PORT=9001

RUN useradd -m botuser && chown -R botuser /app

EXPOSE 10000

# Ensure start script is executable
RUN chmod +x /app/start.sh || true
USER botuser
CMD ["/app/start.sh"]
