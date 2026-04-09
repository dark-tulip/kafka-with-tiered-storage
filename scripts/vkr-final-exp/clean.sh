#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_DATA_DIR="${SCRIPT_DIR}/../kafka/data"
MINIO_DATA_DIR="${SCRIPT_DIR}/../minio/data"

echo "[clean_hard] Stopping compose stack and removing volumes..."
docker compose down -v

for dir in "$KAFKA_DATA_DIR" "$MINIO_DATA_DIR"; do
  if [[ ! -d "$dir" ]]; then
    echo "[clean_hard] Directory not found: $dir"
    exit 1
  fi
done

echo "[clean_hard] Removing Kafka data: $KAFKA_DATA_DIR"
find "$KAFKA_DATA_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +

echo "[clean_hard] Removing MinIO data: $MINIO_DATA_DIR"
find "$MINIO_DATA_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +

echo "[clean_hard] Done."