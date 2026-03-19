#!/usr/bin/env bash
# Sync recorder NDJSON files from remote Ubuntu machine to local logs/recorder/.
# Usage: ./tools/sync-recorder.sh <ip> [pem_key] [user] [remote_path]

set -euo pipefail

IP="${1:?Usage: $0 <ip> [pem_key] [user] [remote_path]}"
PEM="${2:-~/.ssh/key.pem}"
USER="${3:-ubuntu}"
REMOTE_PATH="${4:-./logs/recorder/}"
LOCAL_PATH="$(cd "$(dirname "$0")/.." && pwd)/logs/recorder/"

mkdir -p "$LOCAL_PATH"

echo "syncing ${USER}@${IP}:${REMOTE_PATH} → ${LOCAL_PATH}"

rsync -avz --progress \
  -e "ssh -i ${PEM}" \
  "${USER}@${IP}:${REMOTE_PATH}" \
  "$LOCAL_PATH"

echo "done — $(ls "$LOCAL_PATH"/data-*.ndjson 2>/dev/null | wc -l | tr -d ' ') files in $LOCAL_PATH"
