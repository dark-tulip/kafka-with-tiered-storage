docker exec -e KAFKA_OPTS="" -it kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic demo.load \
  --num-records 200000 \
  --record-size 512 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=kafka-1:9092 \
    acks=1 \
    linger.ms=5 \
    batch.size=131072 \
    compression.type=lz4

