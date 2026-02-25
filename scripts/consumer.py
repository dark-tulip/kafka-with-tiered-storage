from time import time
from json import loads
from kafka import KafkaConsumer
import statistics
import os

TOPIC = os.gegtenv('TOPIC_NAME')
print(f"Value is: {TOPIC}")

BOOTSTRAP = "localhost:19092"

DURATION_SEC = 10 * 60
SLO_SEC = 0.2
PRINT_EVERY = 5000
CSV_PATH = "consumer_run.csv"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",  # чтобы мерить текущий прогон; поменяй на earliest если нужно
    enable_auto_commit=True,
)

latencies = []
total = 0
within_slo = 0
bytes_total = 0

start = time()
end = start + DURATION_SEC

# --- CSV: 1) открыть файл и написать заголовок ---
csv = open(CSV_PATH, "w", buffering=1)  # line-buffered
csv.write("ts,elapsed_s,total_msgs,bytes_total,lambda_r_bps,within_slo,slo_ratio,p50_s,p95_s,p99_s\n")

try:
    while True:
        now = time()
        if now >= end:
            break

        records = consumer.poll(timeout_ms=1000, max_records=2000)
        if not records:
            continue

        for _, msgs in records.items():
            for msg in msgs:
                now = time()
                if now >= end:
                    break

                raw = msg.value
                if raw is None:
                    continue

                bytes_total += len(raw)

                try:
                    val = loads(raw.decode("utf-8"))
                except Exception:
                    continue

                sent_ts = val.get("ts")
                if sent_ts is None:
                    continue

                l = now - sent_ts
                latencies.append(l)

                total += 1
                if l <= SLO_SEC:
                    within_slo += 1

                if total % PRINT_EVERY == 0 and latencies:
                    elapsed = now - start
                    p50 = statistics.median(latencies)
                    p95 = statistics.quantiles(latencies, n=20, method="inclusive")[18]
                    p99 = statistics.quantiles(latencies, n=100, method="inclusive")[98]
                    lam_r = bytes_total / max(0.001, elapsed)
                    slo_ratio = within_slo / total

                    print(f"Progress: total={total}, P50={p50:.3f}s P95={p95:.3f}s P99={p99:.3f}s")

                    # --- CSV: 2) дописать строку прогресса ---
                    csv.write(
                        f"{now:.3f},{elapsed:.3f},{total},{bytes_total},{lam_r:.3f},"
                        f"{within_slo},{slo_ratio:.6f},{p50:.6f},{p95:.6f},{p99:.6f}\n"
                    )

            if time() >= end:
                break

finally:
    consumer.close()
    csv.close()

# финальный отчёт (как раньше)
elapsed_total = max(0.001, time() - start)
slo_ratio = (within_slo / total) if total else 0.0

if latencies:
    lat_sorted = sorted(latencies)
    p50 = statistics.median(lat_sorted)
    p95 = statistics.quantiles(lat_sorted, n=20, method="inclusive")[18]
    p99 = statistics.quantiles(lat_sorted, n=100, method="inclusive")[98]
    mn, mx = lat_sorted[0], lat_sorted[-1]
else:
    p50 = p95 = p99 = mn = mx = float("nan")

lam_r_bps = bytes_total / elapsed_total
lam_r_mib = lam_r_bps / 1024 / 1024

print("\n=== FINAL REPORT (10 min) ===")
print(f"Messages: {total}")
print(f"Latency: min={mn:.3f}s p50={p50:.3f}s p95={p95:.3f}s p99={p99:.3f}s max={mx:.3f}s")
print(f"SLO<= {SLO_SEC:.3f}s : {slo_ratio*100:.2f}%")
print(f"Read throughput λr: {lam_r_bps:.0f} B/s ({lam_r_mib:.2f} MiB/s)")
print(f"Total bytes (exact): {bytes_total}")
print(f"CSV saved to: {CSV_PATH}")