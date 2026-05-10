from __future__ import annotations

import json
import os
import random
from time import time, perf_counter, sleep
from kafka import KafkaProducer


def getenv_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value is not None else default


def getenv_str(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value is not None else default


def format_seconds(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


TOPIC = getenv_str("TOPIC_NAME")
if not TOPIC:
    raise RuntimeError("TOPIC_NAME is not set")

BOOTSTRAP_SERVERS = getenv_str("BOOTSTRAP_SERVERS", "localhost:19092")
LOAD_PROFILE = getenv_str("LOAD_PROFILE", "write-heavy")

PROFILE_TO_RATE = {
    "write-heavy": 5000,
    "mixed": 2000,
}

if LOAD_PROFILE not in PROFILE_TO_RATE:
    raise RuntimeError(
        f"Unsupported LOAD_PROFILE={LOAD_PROFILE!r}. "
        f"Use one of: {', '.join(PROFILE_TO_RATE)}"
    )

TARGET_RATE_MSG_S = PROFILE_TO_RATE[LOAD_PROFILE]
DURATION_SEC = getenv_int("DURATION_SEC", 60 * 60)

PAYLOAD_MIN_BYTES = getenv_int("PAYLOAD_MIN_BYTES", 1024)
PAYLOAD_MAX_BYTES = getenv_int("PAYLOAD_MAX_BYTES", 4096)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    linger_ms=5,
    acks=1,
)

print("=== PRODUCER CONFIG ===")
print(f"TOPIC={TOPIC}")
print(f"BOOTSTRAP_SERVERS={BOOTSTRAP_SERVERS}")
print(f"LOAD_PROFILE={LOAD_PROFILE}")
print(f"TARGET_RATE_MSG_S={TARGET_RATE_MSG_S}")
print(f"DURATION_SEC={DURATION_SEC}")
print(f"PAYLOAD_RANGE={PAYLOAD_MIN_BYTES}-{PAYLOAD_MAX_BYTES} bytes")
print()

next_id = 1
sent = 0
bytes_sent = 0

start_mono = perf_counter()
end_mono = start_mono + DURATION_SEC

last_report_mono = start_mono
next_send_mono = start_mono

try:
    while perf_counter() < end_mono:
        now_wall = time()
        now_mono = perf_counter()

        if now_mono < next_send_mono:
            sleep(next_send_mono - now_mono)

        event_id = next_id
        next_id += 1

        payload_size = random.randint(PAYLOAD_MIN_BYTES, PAYLOAD_MAX_BYTES)
        payload = "x" * payload_size

        msg = {
            "op": "insert",
            "id": event_id,
            "ts": now_wall,
            "payload": payload,
        }

        raw = json.dumps(msg, ensure_ascii=False).encode("utf-8")
        bytes_sent += len(raw)

        producer.send(
            TOPIC,
            key=str(event_id).encode("utf-8"),
            value=raw,
        )
        sent += 1

        next_send_mono += 1.0 / TARGET_RATE_MSG_S

        current_mono = perf_counter()
        if current_mono - last_report_mono >= 5.0:
            elapsed = max(0.001, current_mono - start_mono)
            remaining = max(0.0, end_mono - current_mono)
            progress_pct = min(100.0, 100.0 * elapsed / DURATION_SEC)

            actual_msg_rate = sent / elapsed
            lam_w_bps = bytes_sent / elapsed
            lam_w_mib = lam_w_bps / 1024 / 1024

            print(
                f"Progress: {progress_pct:5.1f}% | "
                f"sent={sent} | "
                f"msg_rate={actual_msg_rate:.1f} msg/s | "
                f"lambda_w={lam_w_mib:.2f} MiB/s | "
                f"elapsed={format_seconds(elapsed)} | "
                f"remaining={format_seconds(remaining)}"
            )
            last_report_mono = current_mono

finally:
    producer.flush()
    producer.close()

elapsed_total = max(0.001, perf_counter() - start_mono)
lam_w_bps = bytes_sent / elapsed_total
lam_w_mib = lam_w_bps / 1024 / 1024
actual_msg_rate = sent / elapsed_total

print("\n=== PRODUCER FINAL ===")
print(f"Messages: {sent}")
print(f"Total bytes (exact payload): {bytes_sent}")
print(f"Message rate: {actual_msg_rate:.1f} msg/s")
print(f"Write throughput λw: {lam_w_bps:.0f} B/s ({lam_w_mib:.2f} MiB/s)")
print(f"Elapsed: {format_seconds(elapsed_total)}")