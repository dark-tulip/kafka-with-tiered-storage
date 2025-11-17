docker exec -e KAFKA_OPTS="" -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
                                         --create --topic test-tiered \
                                         --partitions 1 --replication-factor 1 \
                                         --config remote.storage.enable=true \
                                         --config segment.bytes=524288 \
                                         --config local.retention.bytes=1 \
                                         --config retention.bytes=10000000000000



#docker exec -e KAFKA_OPTS="" -it kafka  /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 \
#    --describe --topic demo.load2  | egrep -i 'remote.storage.enable|segment.bytes|local.retention.bytes'

