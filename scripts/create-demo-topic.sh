#!/usr/bin/env bash
set -euo pipefail
# Включаем tiered storage на уровне топика
docker exec -it kafka-1 bash -lc '
kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --create --topic demo.rsm \
  --replication-factor 3 --partitions 3 \
  --config remote.storage.enable=true \
  --config segment.bytes=134217728 \
  --config local.retention.bytes=1073741824
'
