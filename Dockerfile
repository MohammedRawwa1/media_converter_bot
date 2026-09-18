# ── Builder stage: compile native Python packages ──
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only the dependency manifests, for Docker layer caching.
# requirements.lock is the hash-pinned closure of requirements.txt, resolved for
# this image (linux/amd64, Python 3.12) — installed with --require-hashes so a
# tampered or substituted wheel cannot install. Regenerate it with the command in
# its header after editing requirements.txt; CI fails if the two drift apart.
COPY requirements.txt requirements.lock ./

# Install all deps (no --user flag — default prefix /usr/local is in sys.path).
# pip/setuptools/wheel stay unpinned: they bootstrap the locked install below and
# are not part of the application's dependency closure.
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir --require-hashes -r requirements.lock


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
