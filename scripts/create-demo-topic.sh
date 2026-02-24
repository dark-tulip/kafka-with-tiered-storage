#docker exec -e JMX_PORT= -e KAFKA_JMX_PORT= -e RMI_HOSTNAME= -e KAFKA_JMX_HOSTNAME= -e KAFKA_OPTS=  -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
#                                         --create --topic exp-tiered-2 \
#                                         --partitions 1 --replication-factor 1 \
#                                         --config remote.storage.enable=true
#                                         --config segment.bytes=524288 \
#                                         --config local.retention.bytes=1 \
#                                         --config retention.bytes=10000000000000



docker exec -e JMX_PORT= -e KAFKA_JMX_PORT= -e RMI_HOSTNAME= -e KAFKA_JMX_HOSTNAME= -e KAFKA_OPTS=  -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
                                         --create --topic exp-tiered-3 \
                                         --partitions 3 --replication-factor 1 \
                                         --config remote.storage.enable=true \
                                         --config segment.ms=5000


#docker exec -e JMX_PORT= -e KAFKA_JMX_PORT= -e RMI_HOSTNAME= -e KAFKA_JMX_HOSTNAME= -e KAFKA_OPTS=  -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
#  --create \
#  --topic exp-tiered-1 \
#  --partitions 3 \
#  --replication-factor 1

#docker exec -e KAFKA_OPTS="" -it kafka  /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 \
#    --describe --topic demo.load2  | egrep -i 'remote.storage.enable|segment.bytes|local.retention.bytes'

