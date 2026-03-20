#!/usr/bin/env bash
# archive-recorder.sh — Upload completed recorder NDJSON files to S3 and delete local copies.
# Usage: ./tools/archive-recorder.sh [data_dir] [s3_path]
# Cron:  0 * * * * /path/to/archive-recorder.sh ./logs/recorder s3://pm-recorder-data/logs/recorder
set -euo pipefail

DATA_DIR="${1:?Usage: archive-recorder.sh <data_dir> <s3_path>}"
S3_PATH="${2:?Usage: archive-recorder.sh <data_dir> <s3_path>}"

# Current 15-minute slot's file is still being written — skip it.
# Compute slot start: truncate current minute to nearest 15.
CURRENT_MIN=$(date -u +%M)
SLOT_MIN=$(printf "%02d" $(( (CURRENT_MIN / 15) * 15 )))
CURRENT_FILE="data-$(date -u +%Y-%m-%d-%H)-${SLOT_MIN}.ndjson"

archived=0
deleted_empty=0

for file in "$DATA_DIR"/data-*.ndjson; do
    [ -e "$file" ] || continue

    name="$(basename "$file")"

    # Skip the active file.
    if [ "$name" = "$CURRENT_FILE" ]; then
        continue
    fi

    # Delete empty files.
    if [ ! -s "$file" ]; then
        rm "$file"
        deleted_empty=$((deleted_empty + 1))
        continue
    fi

    # Upload to S3.
    if aws s3 cp "$file" "$S3_PATH/$name" --quiet; then
        rm "$file"
        archived=$((archived + 1))
    else
        echo "ERROR: failed to upload $name" >&2
    fi
done

echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) archived=$archived deleted_empty=$deleted_empty"
