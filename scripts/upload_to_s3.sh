#!/bin/bash
# Usage: ./scripts/upload_to_s3.sh <local_data_dir> <s3_bucket_path>
# Example: ./scripts/upload_to_s3.sh data/ s3://my-bucket/weather-data/

set -euo pipefail

LOCAL_DATA_DIR=${1:-data/}
S3_BUCKET_PATH=${2:-}

if [[ -z "$S3_BUCKET_PATH" ]]; then
  echo "Usage: $0 <local_data_dir> <s3_bucket_path>"
  exit 1
fi

# Exclude Spark checkpoint and temp files
EXCLUDES=(--exclude "*/_spark_metadata/*" --exclude "*/_temporary/*" --exclude "*/checkpoint/*")

# Sync Parquet files only
aws s3 sync "$LOCAL_DATA_DIR" "$S3_BUCKET_PATH" --exclude "*" --include "*.parquet" "${EXCLUDES[@]}"

if [[ $? -eq 0 ]]; then
  echo "Upload to $S3_BUCKET_PATH completed successfully."
else
  echo "Upload to $S3_BUCKET_PATH failed." >&2
  exit 2
fi 