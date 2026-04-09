import csv
import json
import os
import random
import statistics
from time import time, sleep
from kafka import KafkaConsumer


def getenv_str(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value is not None else default


def getenv_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value is not None else default


def getenv_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value is not None else default


def percentile_from_sorted(data, p: float) -> float:
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
        self.data = []
        self.n_seen = 0

    def add(self, x: float):
        self.n_seen += 1
        if len(self.data) < self.size:
            self.data.append(x)
            return
        j = random.randint(1, self.n_seen)
        if j <= self.size:
            self.data[j - 1] = x

    def snapshot_sorted(self):
        return sorted(self.data)


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
CSV_PATH = getenv_str("CSV_PATH", f"consumer_{EXPERIMENT_ID}.csv")
RESERVOIR_SIZE = getenv_int("RESERVOIR_SIZE", 200000)

AUTO_OFFSET_RESET = getenv_str("AUTO_OFFSET_RESET", "earliest")
GROUP_ID = getenv_str("GROUP_ID", f"{EXPERIMENT_ID}-{CONSUMER_MODE}")

if CONSUMER_MODE not in {"steady-state", "cold-read"}:
    raise RuntimeError("CONSUMER_MODE must be 'steady-state' or 'cold-read'")

print("=== CONSUMER CONFIG ===")
print(f"EXPERIMENT_ID={EXPERIMENT_ID}")
print(f"TOPIC={TOPIC}")
print(f"BOOTSTRAP={BOOTSTRAP}")
print(f"CONSUMER_MODE={CONSUMER_MODE}")
print(f"WARMUP_SEC={WARMUP_SEC}")
print(f"MEASURE_SEC={MEASURE_SEC}")
print(f"SLO_SEC={SLO_SEC}")
print(f"GROUP_ID={GROUP_ID}")
print(f"AUTO_OFFSET_RESET={AUTO_OFFSET_RESET}")
print(f"CSV_PATH={CSV_PATH}")
print()

consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset=AUTO_OFFSET_RESET,
    enable_auto_commit=False,
    group_id=GROUP_ID,
)

consumer.subscribe([TOPIC])

# exact counters
total = 0
within_slo = 0
bytes_total = 0
min_latency = float("inf")
max_latency = 0.0

# approximate percentile sample
reservoir = Reservoir(RESERVOIR_SIZE)

start = time()
warmup_end = start + WARMUP_SEC
measure_end = warmup_end + MEASURE_SEC

last_report_total = 0

csv_file = open(CSV_PATH, "w", newline="", buffering=1)
csv_writer = csv.writer(csv_file)
csv_writer.writerow([
    "ts",
    "phase",
    "elapsed_measure_s",
    "total_msgs",
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
    elapsed_measure = max(0.001, now_ts - warmup_end)
    lam_r = bytes_total / elapsed_measure if now_ts > warmup_end else 0.0
    slo_ratio = (within_slo / total) if total else 0.0

    sample = reservoir.snapshot_sorted()
    if sample:
        p50 = percentile_from_sorted(sample, 0.50)
        p95 = percentile_from_sorted(sample, 0.95)
        p99 = percentile_from_sorted(sample, 0.99)
    else:
        p50 = p95 = p99 = float("nan")

    mn = min_latency if total else float("nan")
    mx = max_latency if total else float("nan")

    return elapsed_measure, lam_r, slo_ratio, p50, p95, p99, mn, mx


try:
    # --------------------------------------------------------
    # Warmup
    # --------------------------------------------------------
    if CONSUMER_MODE == "steady-state":
        print("Warmup phase started (steady-state consumer is polling and discarding metrics)...")
        while time() < warmup_end:
            consumer.poll(timeout_ms=1000, max_records=2000)

    elif CONSUMER_MODE == "cold-read":
        print("Warmup phase started (cold-read mode waits for data accumulation/offload)...")
        while time() < warmup_end:
            sleep(1.0)

        # Получаем assignment и уходим в старые offsets
        print("Acquiring assignment and seeking to beginning for cold-read measurement...")
        deadline = time() + 30
        while not consumer.assignment():
            consumer.poll(timeout_ms=1000)
            if time() > deadline:
                raise RuntimeError("Consumer assignment was not established in time")

        consumer.seek_to_beginning(*consumer.assignment())

    print("Measurement phase started...")

    # --------------------------------------------------------
    # Measurement
    # --------------------------------------------------------
    while True:
        now = time()
        if now >= measure_end:
            break

        records = consumer.poll(timeout_ms=1000, max_records=2000)
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

                latency = now - sent_ts
                reservoir.add(latency)

                total += 1
                if latency <= SLO_SEC:
                    within_slo += 1

                if latency < min_latency:
                    min_latency = latency
                if latency > max_latency:
                    max_latency = latency

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
                        f"Progress: total={total}, "
                        f"P50={p50:.3f}s P95={p95:.3f}s P99={p99:.3f}s, "
                        f"lambda_r={lam_r / 1024 / 1024:.2f} MiB/s"
                    )

                    csv_writer.writerow([
                        f"{now:.3f}",
                        "measure",
                        f"{elapsed_measure:.3f}",
                        total,
                        bytes_total,
                        f"{lam_r:.3f}",
                        within_slo,
                        f"{slo_ratio:.6f}",
                        f"{p50:.6f}",
                        f"{p95:.6f}",
                        f"{p99:.6f}",
                        f"{mn:.6f}",
                        f"{mx:.6f}",
                    ])

                    last_report_total = total

            if time() >= measure_end:
                break

finally:
    consumer.close()
    csv_file.close()

elapsed_total = max(0.001, time() - warmup_end)
lam_r_bps = bytes_total / elapsed_total if total else 0.0
lam_r_mib = lam_r_bps / 1024 / 1024
slo_ratio = (within_slo / total) if total else 0.0

sample = reservoir.snapshot_sorted()
if sample:
    p50 = percentile_from_sorted(sample, 0.50)
    p95 = percentile_from_sorted(sample, 0.95)
    p99 = percentile_from_sorted(sample, 0.99)
    mn = min_latency
    mx = max_latency
else:
    p50 = p95 = p99 = mn = mx = float("nan")

print("\n=== FINAL REPORT ===")
print(f"Experiment ID: {EXPERIMENT_ID}")
print(f"Topic: {TOPIC}")
print(f"Mode: {CONSUMER_MODE}")
print(f"Messages: {total}")
print(f"Latency: min={mn:.3f}s p50={p50:.3f}s p95={p95:.3f}s p99={p99:.3f}s max={mx:.3f}s")
print(f"SLO <= {SLO_SEC:.3f}s : {slo_ratio * 100:.2f}%")
print(f"Read throughput λr: {lam_r_bps:.0f} B/s ({lam_r_mib:.2f} MiB/s)")
print(f"Total bytes (exact): {bytes_total}")
print(f"CSV saved to: {CSV_PATH}")