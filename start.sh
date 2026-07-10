#!/bin/sh
set -e

# Optional environment check script
if [ -f ./scripts/check_env.py ]; then
  python ./scripts/check_env.py || exit 1
fi

# Optional: gate starting the background worker with START_WORKER (default: true)
START_WORKER="${START_WORKER:-true}"

if [ "$START_WORKER" = "true" ]; then
  echo "Starting ffmpeg worker in background with supervision..."
  
  # Function to start the worker and return its PID
  start_worker() {
    # Use a new log file each time (appends date)
    python -u -m workers.ffmpeg_worker >> /tmp/worker.log 2>&1 &
    echo $!
  }
  
  # Start the worker initially
  WORKER_PID=$(start_worker)
  echo "Worker started with PID $WORKER_PID"
  
  # Ensure worker logs are visible via `docker logs` by tailing the worker log
  tail -n +1 -f /tmp/worker.log &
  TAIL_PID=$!
  
  # Monitor the worker process and restart it if it crashes
  # This runs in the background to not block uvicorn startup
  (
    while true; do
      # Check if worker process is still alive
      if ! kill -0 $WORKER_PID 2>/dev/null; then
        echo "Worker process (PID $WORKER_PID) has exited. Restarting in 3 seconds..."
        sleep 3
        WORKER_PID=$(start_worker)
        echo "Worker restarted with new PID $WORKER_PID"
      fi
      sleep 5
    done
  ) &
  MONITOR_PID=$!
  
  echo "Worker supervisor started (monitor PID: $MONITOR_PID)"
else
  echo "START_WORKER not true; skipping ffmpeg worker start."
fi

echo "Starting web server (uvicorn)..."
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-10000}" --log-level info
