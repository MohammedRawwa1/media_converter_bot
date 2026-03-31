#!/usr/bin/env bash
# Remote diagnostic script for media_conversion bot
# Usage: ./diagnose_remote.sh [JOB_ID]
# Example: sudo bash ./diagnose_remote.sh 38e7f445-1970-49e1-9f01-298daa6e914d

set -u

JOB_ID="${1-}"
TS=$(date -u +%Y%m%dT%H%M%SZ)
OUTDIR="/tmp/media_conv_diagnostic_$TS"
mkdir -p "$OUTDIR"

mask_redis_url() {
  local url="$1"
  if [[ -z "$url" ]]; then
    echo ""
    return
  fi
  # mask password part between : and @, e.g. redis://:pass@ -> redis://:****@
  echo "$url" | sed -E 's#(redis://[^:]*:)[^@]+@#\1****@#; s#(redis://):[^@]+@#\1:****@#'
}

mask_stream() {
  # mask likely redis URL appearances in a stream
  sed -E 's#(redis://[^:]*:)[^@]+@#\1****@#; s#(REDIS_URL=).*#\1****#'
}

# header
hostname > "$OUTDIR/host.txt" 2>&1 || true
uname -a > "$OUTDIR/uname.txt" 2>&1 || true
id > "$OUTDIR/id.txt" 2>&1 || true
whoami > "$OUTDIR/whoami.txt" 2>&1 || true
uptime > "$OUTDIR/uptime.txt" 2>&1 || true

# disk/memory
df -h > "$OUTDIR/df-h.txt" 2>&1 || true
free -h > "$OUTDIR/free-h.txt" 2>&1 || true

# env for current user (masked)
env | sort | mask_stream > "$OUTDIR/env_masked.txt" 2>&1 || true

# Print any present REDIS_URL (masked)
if [[ -n "${REDIS_URL-}" ]]; then
  echo "Current shell REDIS_URL: $(mask_redis_url "$REDIS_URL")" > "$OUTDIR/redis_detected.txt"
else
  echo "Current shell REDIS_URL: <none>" > "$OUTDIR/redis_detected.txt"
fi

# process scan
ps aux | egrep -i 'python|worker|ffmpeg|gunicorn|uvicorn|web|fetcher' | egrep -v 'egrep' > "$OUTDIR/ps_misc.txt" 2>&1 || true

# try to collect /proc/*/environ for matching processes (requires permissions)
mkdir -p "$OUTDIR/proc_environs"
awk '{print $2}' "$OUTDIR/ps_misc.txt" | grep -E '^[0-9]+$' | sort -u | while read -r PID; do
  if [[ -r "/proc/$PID/environ" ]]; then
    tr '\0' '\n' < "/proc/$PID/environ" 2>/dev/null | mask_stream > "$OUTDIR/proc_environs/$PID.env" || true
  fi
done || true

# systemd services (if systemctl present)
if command -v systemctl >/dev/null 2>&1; then
  systemctl list-units --type=service --all > "$OUTDIR/systemctl_units.txt" 2>&1 || true
  # capture candidate services that look relevant
  systemctl list-units --type=service --all | egrep -i 'media|worker|bot|web|fetcher' | awk '{print $1}' | sort -u > "$OUTDIR/candidate_services.txt" 2>&1 || true
  while read -r SVC; do
    [[ -z "$SVC" ]] && continue
    echo "---- $SVC ----" > "$OUTDIR/service_${SVC}_status.txt"
    systemctl status "$SVC" -n 200 --no-pager >> "$OUTDIR/service_${SVC}_status.txt" 2>&1 || true
    echo "Unit file for $SVC:" >> "$OUTDIR/service_${SVC}_status.txt" 2>&1 || true
    systemctl cat "$SVC" >> "$OUTDIR/service_${SVC}_status.txt" 2>&1 || true
    # try to extract Environment= lines
    systemctl show -p Environment "$SVC" >> "$OUTDIR/service_${SVC}_status.txt" 2>&1 || true
  done < "$OUTDIR/candidate_services.txt" || true
fi

# docker containers (if docker present)
if command -v docker >/dev/null 2>&1; then
  docker ps --no-trunc --format '{{.Names}}|{{.Image}}|{{.ID}}' > "$OUTDIR/docker_ps.txt" 2>&1 || true
  mkdir -p "$OUTDIR/docker_inspect"
  awk -F'|' '{print $1}' "$OUTDIR/docker_ps.txt" | while read -r C; do
    [[ -z "$C" ]] && continue
    echo "Inspecting container: $C" > "$OUTDIR/docker_inspect/${C}.txt"
    docker inspect "$C" >> "$OUTDIR/docker_inspect/${C}.txt" 2>&1 || true
    # extract env lines masked
    docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$C" 2>/dev/null | mask_stream > "$OUTDIR/docker_inspect/${C}.env" || true
    # grab last 200 logs
    docker logs --tail 200 "$C" > "$OUTDIR/docker_inspect/${C}.logs" 2>&1 || true
  done || true
fi

# locate common app log folders
if [[ -d ./logs ]]; then
  mkdir -p "$OUTDIR/project_logs"
  for f in ./logs/*; do
    [[ -f "$f" ]] && tail -n 500 "$f" > "$OUTDIR/project_logs/$(basename "$f")" 2>&1 || true
  done
fi

# redis diagnostics (if redis-cli present)
if command -v redis-cli >/dev/null 2>&1; then
  REDIS_CLI="redis-cli"
  # prefer explicit REDIS_URL if present in env or discovered from /proc or docker
  CANDIDATE_URL="${REDIS_URL-}"
  if [[ -z "$CANDIDATE_URL" ]]; then
    # search collected env files
    grep -H "REDIS_URL" -R "$OUTDIR" || true
    # try to pick first REDIS_URL from collected envs
    CANDIDATE_URL=$(grep -Rho "REDIS_URL=[^\n]*" "$OUTDIR" | sed -E 's#REDIS_URL=##' | head -n1 || true)
  fi
  if [[ -n "$CANDIDATE_URL" ]]; then
    echo "Using detected REDIS_URL: $(mask_redis_url "$CANDIDATE_URL")" > "$OUTDIR/redis_used.txt"
    # try to ping
    echo "PING -> " > "$OUTDIR/redis_ping.txt"
    $REDIS_CLI -u "$CANDIDATE_URL" PING > "$OUTDIR/redis_ping.txt" 2>&1 || $REDIS_CLI -u "$CANDIDATE_URL" PING >> "$OUTDIR/redis_ping.txt" 2>&1 || true
    # list ffmpeg:jobs
    echo "LRANGE ffmpeg:jobs 0 50" > "$OUTDIR/redis_ffmpeg_jobs.txt"
    $REDIS_CLI -u "$CANDIDATE_URL" LRANGE ffmpeg:jobs 0 50 >> "$OUTDIR/redis_ffmpeg_jobs.txt" 2>&1 || true
    # list ffmpeg:job:* keys (use SCAN if many)
    echo "KEYS ffmpeg:job:* (first 500)" > "$OUTDIR/redis_ffmpeg_job_keys.txt"
    $REDIS_CLI -u "$CANDIDATE_URL" --raw KEYS "ffmpeg:job:*" | sed -n '1,500p' >> "$OUTDIR/redis_ffmpeg_job_keys.txt" 2>&1 || true
  else
    echo "No REDIS_URL detected in environment. Attempting local redis-cli behavior." > "$OUTDIR/redis_used.txt"
    $REDIS_CLI PING > "$OUTDIR/redis_ping.txt" 2>&1 || true
    $REDIS_CLI LRANGE ffmpeg:jobs 0 50 > "$OUTDIR/redis_ffmpeg_jobs.txt" 2>&1 || true
    $REDIS_CLI --raw KEYS "ffmpeg:job:*" > "$OUTDIR/redis_ffmpeg_job_keys.txt" 2>&1 || true
  fi
  # if JOB_ID provided, inspect it
  if [[ -n "$JOB_ID" ]]; then
    echo "HGETALL ffmpeg:job:$JOB_ID" > "$OUTDIR/redis_job_${JOB_ID}.txt"
    if [[ -n "$CANDIDATE_URL" ]]; then
      $REDIS_CLI -u "$CANDIDATE_URL" HGETALL "ffmpeg:job:$JOB_ID" >> "$OUTDIR/redis_job_${JOB_ID}.txt" 2>&1 || true
    else
      $REDIS_CLI HGETALL "ffmpeg:job:$JOB_ID" >> "$OUTDIR/redis_job_${JOB_ID}.txt" 2>&1 || true
    fi
  fi
else
  echo "redis-cli not found on PATH; skipping redis checks" > "$OUTDIR/redis_missing.txt"
fi

# systemd journal tail for likely services
if command -v journalctl >/dev/null 2>&1; then
  for S in media-worker.service media-bot.service fetcher.service webapp.service; do
    journalctl -u "$S" -n 300 --no-pager > "$OUTDIR/journal_${S}.txt" 2>&1 || true
  done
fi

# search recent errors in saved logs
grep -R "ERROR\|Exception\|Traceback" "$OUTDIR" | sed -n '1,500p' > "$OUTDIR/errors_found.txt" || true

# create tarball
TARFILE="$OUTDIR.tar.gz"
( cd /tmp && tar -czf "$TARFILE" "$(basename "$OUTDIR")" ) 2>/dev/null || tar -czf "$TARFILE" -C "$OUTDIR" . || true

cat > "$OUTDIR/README.txt" <<EOF
Diagnostic collected at: $(date -u)
Output dir: $OUTDIR
Archive: $TARFILE
Notes:
- Sensitive values such as REDIS_URL were masked in visible outputs when possible.
- If you want raw env dumps for debugging, run the script with sudo and examine files under $OUTDIR/proc_environs and $OUTDIR/docker_inspect.
- To share results, upload $TARFILE to a secure location and paste link here.
EOF

echo
echo "Diagnostic collection complete. Archive: $TARFILE"
echo "Upload or SCP that tar.gz back for inspection."

exit 0
