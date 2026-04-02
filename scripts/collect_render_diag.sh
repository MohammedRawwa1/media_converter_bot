#!/bin/sh
# Collect a set of diagnostics on the host for a given job id.
# Usage: ./scripts/collect_render_diag.sh <JOB_ID>
# Optional env: DIAG_TOKEN, HOST (default localhost:5000)

JOB_ID="$1"
HOST="${HOST:-127.0.0.1:5000}"
DIAG_TOKEN="${DIAG_TOKEN:-$DIAG_TOKEN}"

if [ -z "$JOB_ID" ]; then
  echo "Usage: $0 <JOB_ID>"
  exit 1
fi

echo "=== PS (top processes by RSS) ==="
python3 scripts/diagnose_job.py --action ps || echo "ps action failed"

echo
echo "=== REDIS JOB HASH (ffmpeg:job:$JOB_ID) ==="
python3 scripts/diagnose_job.py --action job_info --job_id "$JOB_ID" || echo "job_info failed"

echo
echo "=== LAST LOGS ==="
python3 scripts/diagnose_job.py --action tail_logs --lines 500 || echo "tail_logs failed"

echo
if [ -n "$DIAG_TOKEN" ]; then
  echo "=== INTERNAL /internal/diag (HTTP) ==="
  # try local path first
  curl -s -H "X-DIAG-TOKEN: $DIAG_TOKEN" "http://$HOST/internal/diag?job_id=$JOB_ID" || echo "curl to http://$HOST/internal/diag failed"
else
  echo "Skipping internal diag HTTP call (DIAG_TOKEN not set)"
fi

echo
echo "Diagnostics collection complete."
