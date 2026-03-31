#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 /path/to/file [WEB_UPLOAD_URL] [UPLOAD_SECRET]
WEB_UPLOAD_URL defaults to env WEB_UPLOAD_URL. UPLOAD_SECRET defaults to env UPLOAD_SECRET.
Example:
  $0 ./video.mp4 https://media-converter-bot-1.onrender.com "8pAV..."
EOF
  exit 1
}

if [[ $# -lt 1 ]]; then
  usage
fi

LOCAL_FILE="$1"
WEB_URL="${2:-${WEB_UPLOAD_URL:-}}"
UPLOAD_SECRET_ARG="${3:-${UPLOAD_SECRET:-}}"

if [[ ! -f "$LOCAL_FILE" ]]; then
  echo "Local file not found: $LOCAL_FILE" >&2
  exit 2
fi
if [[ -z "$WEB_URL" ]]; then
  echo "WEB_UPLOAD_URL not provided and WEB_UPLOAD_URL env var not set" >&2
  exit 3
fi

FNAME=$(basename "$LOCAL_FILE")

# Request presign
if [[ -n "$UPLOAD_SECRET_ARG" ]]; then
  echo "Requesting presign from $WEB_URL/presign (using upload token)"
  RESP=$(curl -sS -H "X-Upload-Token: $UPLOAD_SECRET_ARG" -H "Content-Type: application/json" -d "{\"filename\": \"$FNAME\"}" "$WEB_URL/presign" ) || true
else
  echo "Requesting presign from $WEB_URL/presign"
  RESP=$(curl -sS -H "Content-Type: application/json" -d "{\"filename\": \"$FNAME\"}" "$WEB_URL/presign" ) || true
fi

if [[ -z "$RESP" ]]; then
  echo "Empty response from presign endpoint. Run with -v or check server logs." >&2
  exit 4
fi

# Show server response for debugging if not JSON
if ! echo "$RESP" | jq . >/dev/null 2>&1; then
  echo "Server returned (not JSON):" >&2
  echo "$RESP" >&2
  exit 5
fi

URL=$(echo "$RESP" | jq -r .url)
if [[ -z "$URL" || "$URL" == "null" ]]; then
  echo "Presign response missing 'url' field. Server response:" >&2
  echo "$RESP" | jq . >&2
  exit 6
fi

# Build curl args from returned form fields
declare -a CURL_ARGS
CURL_ARGS+=("$URL")
while IFS=$'\t' read -r k v; do
  CURL_ARGS+=(-F "${k}=${v}")
done < <(echo "$RESP" | jq -r '.fields | to_entries[] | "\(.key)\t\(.value)"')

CURL_ARGS+=(-F "file=@${LOCAL_FILE}")

echo "Uploading file to S3 using presigned POST..."
# Execute upload and capture HTTP response
HTTP_OUT=$(mktemp)
"$(command -v curl)" -sS "${CURL_ARGS[@]}" -o "$HTTP_OUT" -w "%{http_code}" > /tmp/presign_http_code || true
HTTP_CODE=$(cat /tmp/presign_http_code || true)

if [[ "$HTTP_CODE" != "200" && "$HTTP_CODE" != "204" && "$HTTP_CODE" != "201" ]]; then
  echo "Upload returned HTTP $HTTP_CODE" >&2
  echo "Response body:" >&2
  sed -n '1,200p' "$HTTP_OUT" >&2
  echo
  echo "Full presign response was:" >&2
  echo "$RESP" | jq . >&2
  exit 7
fi

echo "Upload OK (HTTP $HTTP_CODE)."
GET_URL=$(echo "$RESP" | jq -r .get_url // empty)
if [[ -n "$GET_URL" && "$GET_URL" != "null" ]]; then
  echo "Object accessible at (temporary GET URL): $GET_URL"
fi

exit 0
