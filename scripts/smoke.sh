docker exec \
  -e JMX_PORT= -e KAFKA_JMX_PORT= -e RMI_HOSTNAME= -e KAFKA_JMX_HOSTNAME= -e KAFKA_OPTS= -it kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
                                         --topic test-tiered \
                                         --num-records 1000000 \
                                         --throughput -1 \
                                         --record-size 10000 \
                                         --producer-props acks=1 batch.size=16384 bootstrap.servers=kafka:9092


