#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from collections import defaultdict
import argparse


# ------------------------------------------------------------
# Базовый LHS-manifest БЕЗ exp_id и topic
# Формат:
# (b_mb, h_local_gb, k_upl, t_seg_min)
# ------------------------------------------------------------
RAW_MANIFEST = [
    (256, 2, 2, 15),
    (64, 1, 2, 15),
    (128, 1, 2, 15),
    (256, 5, 4, 15),
    (256, 1, 2, 1),
    (256, 1, 1, 1),
    (256, 2, 1, 1),
    (256, 2, 4, 5),
    (128, 5, 1, 5),
    (256, 1, 1, 5),
    (64, 5, 2, 1),
    (64, 5, 1, 15),
    (128, 1, 2, 5),
    (128, 2, 4, 1),
    (64, 2, 4, 15),
    (128, 2, 1, 15),
    (64, 5, 1, 1),
    (128, 5, 2, 5),
    (64, 1, 4, 1),
    (64, 2, 1, 5),
    (256, 5, 4, 5),
    (64, 1, 4, 5),
    (128, 5, 4, 15),
    (128, 2, 2, 1),
]


def mb_to_bytes(x: int) -> int:
    return x * 1024 * 1024


def gb_to_bytes(x: int) -> int:
    return x * 1024 * 1024 * 1024


def min_to_ms(x: int) -> int:
    return x * 60 * 1000


def build_sorted_manifest() -> list[tuple[str, str, int, int, int, int]]:
    """
    Возвращает список:
    (exp_id, topic, b_mb, h_gb, k_upl, t_min)

    Сортировка:
    1) K_upl
    2) B
    3) H_local
    4) T_seg
    """
    sorted_rows = sorted(RAW_MANIFEST, key=lambda x: (x[2], x[0], x[1], x[3]))

    manifest = []
    for idx, (b_mb, h_gb, k_upl, t_min) in enumerate(sorted_rows, start=1):
        exp_id = f"EXP-{idx:03d}"
        topic = f"exp-{idx:03d}"
        manifest.append((exp_id, topic, b_mb, h_gb, k_upl, t_min))
    return manifest


def build_markdown(
    bootstrap_servers: str,
    kafka_container: str,
    partitions: int,
    replication_factor: int,
    remote_storage_enable: str,
    broker_k_upl_key: str,
    load_profile: str,
    duration_sec: int,
    producer_script: str,
) -> str:
    manifest = build_sorted_manifest()

    grouped: dict[int, list[tuple[str, str, int, int, int, int]]] = defaultdict(list)
    for row in manifest:
        grouped[row[4]].append(row)

    lines: list[str] = []

    lines.append("# Эксперименты LHS для лабораторного стенда")
    lines.append("")
    lines.append("Файл сгенерирован автоматически.")
    lines.append("")
    lines.append("## Правило сортировки и нумерации")
    lines.append("")
    lines.append("`Experiment ID` и `topic` присваиваются после сортировки конфигураций по следующим полям:")
    lines.append("")
    lines.append("1. `K_upl`")
    lines.append("2. `B`")
    lines.append("3. `H_local`")
    lines.append("4. `T_seg`")
    lines.append("")
    lines.append("Таким образом, `EXP-001` соответствует первой конфигурации в этом упорядоченном списке, а не порядку записи в исходном LHS-manifest.")
    lines.append("")
    lines.append("## Фиксированные параметры стенда")
    lines.append("")
    lines.append(f"- brokers: `3`")
    lines.append(f"- partitions: `{partitions}`")
    lines.append(f"- replication.factor: `{replication_factor}`")
    lines.append(f"- remote.storage.enable: `{remote_storage_enable}`")
    lines.append(f"- bootstrap.servers: `{bootstrap_servers}`")
    lines.append(f"- producer script: `{producer_script}`")
    lines.append(f"- default LOAD_PROFILE: `{load_profile}`")
    lines.append(f"- default DURATION_SEC: `{duration_sec}`")
    lines.append(f"- broker-level key for K_upl: `{broker_k_upl_key}`")
    lines.append("")
    lines.append("## Общая таблица экспериментов")
    lines.append("")
    lines.append("| № | Experiment ID | Topic | B (MB) | H_local (GB/partition) | K_upl | T_seg (min) | segment.bytes | local.retention.bytes | segment.ms |")
    lines.append("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|")

    for i, (exp_id, topic, b_mb, h_gb, k_upl, t_min) in enumerate(manifest, start=1):
        lines.append(
            f"| {i} | `{exp_id}` | `{topic}` | {b_mb} | {h_gb} | {k_upl} | {t_min} | "
            f"`{mb_to_bytes(b_mb)}` | `{gb_to_bytes(h_gb)}` | `{min_to_ms(t_min)}` |"
        )

    lines.append("")
    lines.append("## Группы по K_upl")
    lines.append("")

    for k_upl in sorted(grouped):
        rows = grouped[k_upl]

        lines.append(f"## Group K_upl = {k_upl}")
        lines.append("")
        lines.append("### Таблица группы")
        lines.append("")
        lines.append("| № | Experiment ID | Topic | B (MB) | H_local (GB/partition) | T_seg (min) |")
        lines.append("|---:|---|---|---:|---:|---:|")
        for idx, (exp_id, topic, b_mb, h_gb, _, t_min) in enumerate(rows, start=1):
            lines.append(f"| {idx} | `{exp_id}` | `{topic}` | {b_mb} | {h_gb} | {t_min} |")
        lines.append("")

        lines.append("### План обновления broker-level конфигурации")
        lines.append("")
        lines.append("1. Остановить/подготовить Kafka brokers к смене глобального параметра.")
        lines.append(f"2. Установить `{broker_k_upl_key}={k_upl}` в конфигурации брокеров.")
        lines.append("3. Перезапустить Kafka brokers.")
        lines.append("4. Дождаться готовности кластера и проверить health-check.")
        lines.append("5. После этого запустить все эксперименты данной группы.")
        lines.append("")

        lines.append("```bash")
        lines.append(f"export BROKER_K_UPL_KEY={broker_k_upl_key}")
        lines.append(f"export BROKER_K_UPL_VALUE={k_upl}")
        lines.append("```")
        lines.append("")

        lines.append("### Эксперименты группы")
        lines.append("")

        for exp_id, topic, b_mb, h_gb, _, t_min in rows:
            segment_bytes = mb_to_bytes(b_mb)
            local_retention_bytes = gb_to_bytes(h_gb)
            segment_ms = min_to_ms(t_min)

            lines.append(f"### {exp_id}")
            lines.append("")
            lines.append(f"- topic: `{topic}`")
            lines.append(f"- B: `{b_mb} MB`")
            lines.append(f"- H_local: `{h_gb} GB/partition`")
            lines.append(f"- K_upl: `{k_upl}`")
            lines.append(f"- T_seg: `{t_min} min`")
            lines.append("")

            lines.append("#### 1. Создание topic")
            lines.append("")
            lines.append("```bash")
            lines.append(
                f"docker exec -it {kafka_container} /opt/kafka/bin/kafka-topics.sh "
                f"--bootstrap-server {bootstrap_servers} \\"
            )
            lines.append(f"  --create --if-not-exists --topic {topic} \\")
            lines.append(f"  --partitions {partitions} --replication-factor {replication_factor} \\")
            lines.append(f"  --config remote.storage.enable={remote_storage_enable} \\")
            lines.append(f"  --config segment.bytes={segment_bytes} \\")
            lines.append(f"  --config local.retention.bytes={local_retention_bytes} \\")
            lines.append(f"  --config segment.ms={segment_ms}")
            lines.append("```")
            lines.append("")

            lines.append("#### 2. Обновление env для генератора нагрузки")
            lines.append("")
            lines.append("```bash")
            lines.append(f"export TOPIC_NAME={topic}")
            lines.append(f"export BOOTSTRAP_SERVERS={bootstrap_servers}")
            lines.append(f"export LOAD_PROFILE={load_profile}")
            lines.append(f"export DURATION_SEC={duration_sec}")
            lines.append(f"export EXPERIMENT_ID={exp_id}")
            lines.append("```")
            lines.append("")

            lines.append("#### 3. Запуск генератора нагрузки")
            lines.append("")
            lines.append("```bash")
            lines.append(f"python3 {producer_script}")
            lines.append("```")
            lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate EXPERIMENTS.md for 24 LHS lab experiments")
    parser.add_argument("--out", default="EXPERIMENTS_LHS_LAB.md", help="Output markdown file")
    parser.add_argument("--bootstrap-servers", default="localhost:19092")
    parser.add_argument("--kafka-container", default="kafka")
    parser.add_argument("--partitions", type=int, default=12)
    parser.add_argument("--replication-factor", type=int, default=2)
    parser.add_argument("--remote-storage-enable", default="true")
    parser.add_argument(
        "--broker-k-upl-key",
        default="remote.log.manager.thread.pool.size",
        help="Kafka 3.9: remote.log.manager.thread.pool.size; Kafka 4.2+: remote.log.manager.follower.thread.pool.size",
    )
    parser.add_argument("--load-profile", default="write-heavy")
    parser.add_argument("--duration-sec", type=int, default=3600)
    parser.add_argument("--producer-script", default="producer_load.py")
    args = parser.parse_args()

    md = build_markdown(
        bootstrap_servers=args.bootstrap_servers,
        kafka_container=args.kafka_container,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        remote_storage_enable=args.remote_storage_enable,
        broker_k_upl_key=args.broker_k_upl_key,
        load_profile=args.load_profile,
        duration_sec=args.duration_sec,
        producer_script=args.producer_script,
    )

    out_path = Path(args.out)
    out_path.write_text(md, encoding="utf-8")
    print(f"Generated: {out_path}")


if __name__ == "__main__":
    main()