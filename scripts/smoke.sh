docker exec -e KAFKA_OPTS="" -it kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
                                         --topic test-tiered \
                                         --num-records 100000 \
                                         --throughput -1 \
                                         --record-size 1000 \
                                         --producer-props acks=1 batch.size=16384 bootstrap.servers=kafka:9092


