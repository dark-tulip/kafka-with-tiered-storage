from time import time, sleep
from json import dumps
from kafka import KafkaProducer
import random
import os

TOPIC = os.gegtenv('TOPIC_NAME')
print(f"Value is: {TOPIC}")


producer = KafkaProducer(
    bootstrap_servers="localhost:19092",
    linger_ms=5,
    acks=1,
)

topic = TOPIC

MIN_RATE = 3000
MAX_RATE = 7000
UPDATE_PROB = 0.05

DURATION_SEC = 10 * 60  # 10 минут

next_id = 1
seen_ids = []

start = time()
end = start + DURATION_SEC

sent = 0
bytes_sent = 0
last_report = start

try:
    while time() < end:
        now = time()

        payload_size = random.randint(1024, 4096)
        payload = "x" * payload_size

        if seen_ids and random.random() < UPDATE_PROB:
            event_id = random.choice(seen_ids)
            op = "update"
        else:
            event_id = next_id
            next_id += 1
            seen_ids.append(event_id)
            op = "insert"

        msg = {"op": op, "id": event_id, "ts": now, "payload": payload}

        raw = dumps(msg).encode("utf-8")   # ТОЧНО то, что уйдет в Kafka
        bytes_sent += len(raw)

        producer.send(topic, value=raw)
        sent += 1

        # необязательный прогресс раз в ~5 сек
        if now - last_report >= 5:
            elapsed = now - start
            lam_w = bytes_sent / max(0.001, elapsed)
            print(f"Progress: sent={sent}, λw={lam_w/1024/1024:.2f} MiB/s")
            last_report = now

        rate = random.randint(MIN_RATE, MAX_RATE)
        sleep(1.0 / rate)
finally:
    producer.flush(timeout=30)
    producer.close(timeout=30)

elapsed_total = max(0.001, time() - start)
lam_w_bps = bytes_sent / elapsed_total
lam_w_mib = lam_w_bps / 1024 / 1024

print("\n=== PRODUCER FINAL (10 min) ===")
print(f"Messages: {sent}")
print(f"Total bytes (exact): {bytes_sent}")
print(f"Write throughput λw: {lam_w_bps:.0f} B/s ({lam_w_mib:.2f} MiB/s)")