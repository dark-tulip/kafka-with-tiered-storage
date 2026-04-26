from typing import Optional, List
import csv
import json
import os
import random
from time import time, sleep

from kafka import KafkaConsumer, TopicPartition


def getenv_str(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    return value if value is not None else default


def getenv_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value is not None else default


def getenv_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value is not None else default


def percentile_from_sorted(data: List[float], p: float) -> float:
    if not data:
        return float("nan")
    if len(data) == 1:
        return data[0]
    idx = (len(data) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(data) - 1)
    frac = idx - lo
    return data[lo] * (1.0 - frac) + data[hi] * frac


class Reservoir:
    def __init__(self, size: int):
        self.size = size
        self.data: List[float] = []
        self.n_seen = 0

    def add(self, x: float) -> None:
        self.n_seen += 1
        if len(self.data) < self.size:
            self.data.append(x)
            return

        j = random.randint(1, self.n_seen)
        if j <= self.size:
            self.data[j - 1] = x

    def snapshot_sorted(self) -> List[float]:
        return sorted(self.data)


def wait_for_topic_partitions(
        consumer: KafkaConsumer,
        topic: str,
        timeout_sec: int = 60,
) -> List[int]:
    deadline = time() + timeout_sec
    last_parts = None

    while time() < deadline:
        consumer.poll(timeout_ms=1000)
        parts = consumer.partitions_for_topic(topic)
        if parts:
            return sorted(parts)
        last_parts = parts
        sleep(0.2)

    raise RuntimeError(
        "Topic metadata was not established in time. "
        "Topic={topic}, known_partitions={parts}".format(topic=topic, parts=last_parts)
    )


TOPIC = getenv_str("TOPIC_NAME")
if not TOPIC:
    raise RuntimeError("TOPIC_NAME is not set")

BOOTSTRAP = getenv_str("BOOTSTRAP_SERVERS", "localhost:19092")
EXPERIMENT_ID = getenv_str("EXPERIMENT_ID", TOPIC)
CONSUMER_MODE = getenv_str("CONSUMER_MODE", "steady-state")  # steady-state | cold-read

WARMUP_SEC = getenv_int("WARMUP_SEC", 15 * 60)
MEASURE_SEC = getenv_int("MEASURE_SEC", 45 * 60)
SLO_SEC = getenv_float("SLO_SEC", 0.2)
PRINT_EVERY = getenv_int("PRINT_EVERY", 5000)
CSV_PATH = getenv_str("CSV_PATH", "consumer_{exp}.csv".format(exp=EXPERIMENT_ID))
RESERVOIR_SIZE = getenv_int("RESERVOIR_SIZE", 200000)
AUTO_OFFSET_RESET = getenv_str("AUTO_OFFSET_RESET", "earliest")
POLL_TIMEOUT_MS = getenv_int("POLL_TIMEOUT_MS", 1000)

if CONSUMER_MODE not in {"steady-state", "cold-read"}:
    raise RuntimeError("CONSUMER_MODE must be 'steady-state' or 'cold-read'")

metric_label = "latency_read_s" if CONSUMER_MODE == "steady-state" else "message_age_s"

print("=== CONSUMER CONFIG ===")
print("EXPERIMENT_ID={}".format(EXPERIMENT_ID))
print("TOPIC={}".format(TOPIC))
print("BOOTSTRAP={}".format(BOOTSTRAP))
print("CONSUMER_MODE={}".format(CONSUMER_MODE))
print("WARMUP_SEC={}".format(WARMUP_SEC))
print("MEASURE_SEC={}".format(MEASURE_SEC))
print("SLO_SEC={}".format(SLO_SEC))
print("AUTO_OFFSET_RESET={}".format(AUTO_OFFSET_RESET))
print("CSV_PATH={}".format(CSV_PATH))
print("METRIC_LABEL={}".format(metric_label))
print()

consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset=AUTO_OFFSET_RESET,
    enable_auto_commit=False,
)

total = 0
within_slo = 0
bytes_total = 0
min_metric = float("inf")
max_metric = 0.0
skipped_old = 0

reservoir = Reservoir(RESERVOIR_SIZE)

start = time()
warmup_end = start + WARMUP_SEC
measure_end = warmup_end + MEASURE_SEC

last_report_total = 0
measure_start_wall = None

csv_file = open(CSV_PATH, "w", newline="", buffering=1)
csv_writer = csv.writer(csv_file)
csv_writer.writerow([
    "ts",
    "phase",
    "mode",
    "metric_kind",
    "elapsed_measure_s",
    "total_msgs",
    "skipped_old_msgs",
    "bytes_total",
    "lambda_r_bps",
    "within_slo",
    "slo_ratio",
    "p50_s",
    "p95_s",
    "p99_s",
    "min_s",
    "max_s",
])


def current_stats(now_ts: float):
    if measure_start_wall is None:
        elapsed_measure = 0.001
    else:
        elapsed_measure = max(0.001, now_ts - measure_start_wall)

    lam_r = bytes_total / elapsed_measure if total else 0.0
    slo_ratio = (within_slo / total) if total else 0.0

    sample = reservoir.snapshot_sorted()
    if sample:
        p50 = percentile_from_sorted(sample, 0.50)
        p95 = percentile_from_sorted(sample, 0.95)
        p99 = percentile_from_sorted(sample, 0.99)
    else:
        p50 = p95 = p99 = float("nan")

    mn = min_metric if total else float("nan")
    mx = max_metric if total else float("nan")

    return elapsed_measure, lam_r, slo_ratio, p50, p95, p99, mn, mx


try:

    partitions = wait_for_topic_partitions(consumer, TOPIC, timeout_sec=60)
    topic_partitions = [TopicPartition(TOPIC, p) for p in partitions]
    consumer.assign(topic_partitions)

    print("Assigned partitions: {}".format(partitions))

    # Warmup
    if CONSUMER_MODE == "steady-state":
        print("Warmup phase started (steady-state consumer polls and discards backlog)...")
        while time() < warmup_end:
            consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=2000)

        print("Seeking to end before measurement to avoid backlog distortion...")
        consumer.seek_to_end(*topic_partitions)
        measure_start_wall = time()

    elif CONSUMER_MODE == "cold-read":
        print("Warmup phase started (cold-read waits while data accumulates)...")
        while time() < warmup_end:
            consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            sleep(0.2)

        print("Seeking to beginning for cold-read measurement...")
        consumer.seek_to_beginning(*topic_partitions)
        measure_start_wall = time()

    print("Measurement phase started...")

    while True:
        now = time()
        if now >= measure_end:
            break

        records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=2000)
        if not records:
            continue

        for _, msgs in records.items():
            for msg in msgs:
                now = time()
                if now >= measure_end:
                    break

                raw = msg.value
                if raw is None:
                    continue

                bytes_total += len(raw)

                try:
                    val = json.loads(raw.decode("utf-8"))
                except Exception:
                    continue

                sent_ts = val.get("ts")
                if sent_ts is None:
                    continue

                # steady-state: меряем только свежие сообщения, пришедшие после старта измерения
                if CONSUMER_MODE == "steady-state":
                    if measure_start_wall is not None and sent_ts < measure_start_wall:
                        skipped_old += 1
                        continue
                    metric_value = now - sent_ts
                else:
                    # cold-read: это возраст сообщения на момент чтения
                    metric_value = now - sent_ts

                reservoir.add(metric_value)

                total += 1
                if metric_value <= SLO_SEC:
                    within_slo += 1

                if metric_value < min_metric:
                    min_metric = metric_value
                if metric_value > max_metric:
                    max_metric = metric_value

                if total - last_report_total >= PRINT_EVERY:
                    (
                        elapsed_measure,
                        lam_r,
                        slo_ratio,
                        p50,
                        p95,
                        p99,
                        mn,
                        mx,
                    ) = current_stats(now)

                    print(
                        "Progress: total={total}, skipped_old={skipped_old}, "
                        "P50={p50:.3f}s P95={p95:.3f}s P99={p99:.3f}s, "
                        "lambda_r={lam_r:.2f} MiB/s".format(
                            total=total,
                            skipped_old=skipped_old,
                            p50=p50,
                            p95=p95,
                            p99=p99,
                            lam_r=lam_r / 1024 / 1024,
                        )
                    )

                    csv_writer.writerow([
                        "{:.3f}".format(now),
                        "measure",
                        CONSUMER_MODE,
                        metric_label,
                        "{:.3f}".format(elapsed_measure),
                        total,
                        skipped_old,
                        bytes_total,
                        "{:.3f}".format(lam_r),
                        within_slo,
                        "{:.6f}".format(slo_ratio),
                        "{:.6f}".format(p50),
                        "{:.6f}".format(p95),
                        "{:.6f}".format(p99),
                        "{:.6f}".format(mn),
                        "{:.6f}".format(mx),
                    ])

                    last_report_total = total

            if time() >= measure_end:
                break

finally:
    consumer.close()
    csv_file.close()

if measure_start_wall is None:
    elapsed_total = 0.001
else:
    elapsed_total = max(0.001, time() - measure_start_wall)

lam_r_bps = bytes_total / elapsed_total if total else 0.0
lam_r_mib = lam_r_bps / 1024 / 1024
slo_ratio = (within_slo / total) if total else 0.0

sample = reservoir.snapshot_sorted()
if sample:
    p50 = percentile_from_sorted(sample, 0.50)
    p95 = percentile_from_sorted(sample, 0.95)
    p99 = percentile_from_sorted(sample, 0.99)
    mn = min_metric
    mx = max_metric
else:
    p50 = p95 = p99 = mn = mx = float("nan")

print("\n=== FINAL REPORT ===")
print("Experiment ID: {}".format(EXPERIMENT_ID))
print("Topic: {}".format(TOPIC))
print("Mode: {}".format(CONSUMER_MODE))
print("Metric kind: {}".format(metric_label))
print("Messages: {}".format(total))
print("Skipped old backlog messages: {}".format(skipped_old))
print(
    "Metric: min={mn:.3f}s p50={p50:.3f}s p95={p95:.3f}s "
    "p99={p99:.3f}s max={mx:.3f}s".format(
        mn=mn, p50=p50, p95=p95, p99=p99, mx=mx
    )
)
print("SLO <= {:.3f}s : {:.2f}%".format(SLO_SEC, slo_ratio * 100))
print("Read throughput λr: {:.0f} B/s ({:.2f} MiB/s)".format(lam_r_bps, lam_r_mib))
print("Total bytes (exact): {}".format(bytes_total))
print("CSV saved to: {}".format(CSV_PATH))