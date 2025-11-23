#!/bin/sh
set -e

echo "Waiting for MinIO..."
until mc alias set local http://minio:9000 minioadmin minioadmin123; do
  sleep 2
done

mc mb -p local/kafka-remote || true
mc anonymous set public local/kafka-remote || true
echo "Bucket ready."
