#!/usr/bin/env bash
set -euo pipefail
# немного данных кинем и прочитаем
docker exec -i kafka-1 bash -lc 'seq 1 1000 | kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic demo.rsm >/dev/null'
docker exec -it kafka-2 bash -lc 'kafka-console-consumer.sh --bootstrap-server kafka-2:9092 --topic demo.rsm --from-beginning --max-messages 10'
