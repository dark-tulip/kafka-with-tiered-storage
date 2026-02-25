from time import time, sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:19092",
    value_serializer=lambda v: dumps(v).encode("utf-8"),
)

topic = "exp-tiered-1"
rate = 5000  # сообщений в секунду
interval = 1.0 / rate

while True:
    now = time()
    msg = {"ts": now, "payload": "x" * 500}  # 500 байт полезной нагрузки
    producer.send(topic, value=msg)
    sleep(interval)