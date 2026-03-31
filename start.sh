#!/bin/sh
set -e

# Optional environment check script
if [ -f ./scripts/check_env.py ]; then
  python ./scripts/check_env.py || exit 1
fi

# Optional: gate starting the background worker with START_WORKER (default: true)
START_WORKER="${START_WORKER:-true}"

if [ "$START_WORKER" = "true" ]; then
  echo "Starting ffmpeg worker in background..."
  # Start worker in background and capture logs
  python -u -m workers.ffmpeg_worker > /tmp/worker.log 2>&1 &
  WORKER_PID=$!

  # Ensure worker logs are visible via `docker logs` by tailing the worker log
  tail -n +1 -f /tmp/worker.log &
else
  echo "START_WORKER not true; skipping ffmpeg worker start."
fi

echo "Starting web server (uvicorn)..."
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-10000}" --log-level info
