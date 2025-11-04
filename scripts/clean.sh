#!/usr/bin/env bash
set -euo pipefail
docker compose down -v || true
rm -rf data/kafka-1 data/kafka-2 data/kafka-3 data/minio || true
mkdir -p data/kafka-1 data/kafka-2 data/kafka-3
