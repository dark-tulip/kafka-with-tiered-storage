docker exec -e KAFKA_OPTS="" -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --create --topic demo.load --partitions 12 --replication-factor 1
