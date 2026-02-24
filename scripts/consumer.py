from time import time
from json import loads
from kafka import KafkaConsumer
import statistics

consumer = KafkaConsumer(
    "exp-tiered-3",
    bootstrap_servers="localhost:19092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: loads(v.decode("utf-8")),
)

latencies = []

for msg in consumer:
    sent_ts = msg.value["ts"]
    l = time() - sent_ts
    latencies.append(l)

    if len(latencies) >= 1000:
        p50 = statistics.median(latencies)
        p95 = statistics.quantiles(latencies, n=20)[18]  # грубый p95
        print(f"P50={p50:.3f}s P95={p95:.3f}s, samples={len(latencies)}")
        latencies = []
