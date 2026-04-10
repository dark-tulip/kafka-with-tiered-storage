#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from collections import defaultdict
import argparse


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
    sorted_rows = sorted(RAW_MANIFEST, key=lambda x: (x[2], x[0], x[1], x[3]))
    manifest = []
    for idx, (b_mb, h_gb, k_upl, t_min) in enumerate(sorted_rows, start=1):
        exp_id = f"EXP-{idx:03d}"
        topic = f"exp-{idx:03d}"
        manifest.append((exp_id, topic, b_mb, h_gb, k_upl, t_min))
    return manifest


def build_markdown(
    bootstrap_servers: str,
    topic_bootstrap_servers: str,
    kafka_container: str,
    partitions: int,
    replication_factor: int,
    remote_storage_enable: str,
    broker_k_upl_key: str,
    load_profile: str,
    duration_sec: int,
    producer_script: str,
    consumer_script: str,
    clean_script: str,
    consumer_mode: str,
    warmup_sec: int,
    measure_sec: int,
    slo_sec: float,
    compose_file: str,
) -> str:
    manifest = build_sorted_manifest()

    grouped: dict[int, list[tuple[str, str, int, int, int, int]]] = defaultdict(list)
    for row in manifest:
        grouped[row[4]].append(row)

    lines: list[str] = []
    add = lines.append

    add("# Эксперименты LHS для лабораторного стенда")
    add("")
    add("Файл сгенерирован автоматически.")
    add("")
    add("## Правило сортировки и нумерации")
    add("")
    add("`Experiment ID` и `topic` присваиваются после сортировки конфигураций по следующим полям:")
    add("")
    add("1. `K_upl`")
    add("2. `B`")
    add("3. `H_local`")
    add("4. `T_seg`")
    add("")
    add("Таким образом, `EXP-001` соответствует первой конфигурации в этом упорядоченном списке, а не порядку записи в исходном LHS-manifest.")
    add("")
    add("## Фиксированные параметры стенда")
    add("")
    add("- brokers: `3`")
    add(f"- partitions: `{partitions}`")
    add(f"- replication.factor: `{replication_factor}`")
    add(f"- remote.storage.enable: `{remote_storage_enable}`")
    add(f"- client bootstrap.servers: `{bootstrap_servers}`")
    add(f"- topic bootstrap.servers (inside docker exec): `{topic_bootstrap_servers}`")
    add(f"- producer script: `{producer_script}`")
    add(f"- consumer script: `{consumer_script}`")
    add(f"- clean script: `{clean_script}`")
    add(f"- compose file: `{compose_file}`")
    add(f"- default LOAD_PROFILE: `{load_profile}`")
    add(f"- default DURATION_SEC: `{duration_sec}`")
    add(f"- default CONSUMER_MODE: `{consumer_mode}`")
    add(f"- default WARMUP_SEC: `{warmup_sec}`")
    add(f"- default MEASURE_SEC: `{measure_sec}`")
    add(f"- default SLO_SEC: `{slo_sec}`")
    add(f"- broker-level key for K_upl: `{broker_k_upl_key}`")
    add("")
    add("## Важное замечание по `kafka-topics.sh`")
    add("")
    add("Команда создания topic запускается через `docker exec` с очисткой `KAFKA_OPTS` и JMX-переменных.")
    add("Это нужно, чтобы CLI-утилита не пыталась повторно поднять JMX exporter внутри контейнера Kafka.")
    add("")
    add("## Общая таблица экспериментов")
    add("")
    add("| № | Experiment ID | Topic | B (MB) | H_local (GB/partition) | K_upl | T_seg (min) | segment.bytes | local.retention.bytes | segment.ms |")
    add("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|")

    for i, (exp_id, topic, b_mb, h_gb, k_upl, t_min) in enumerate(manifest, start=1):
        add(
            f"| {i} | `{exp_id}` | `{topic}` | {b_mb} | {h_gb} | {k_upl} | {t_min} | "
            f"`{mb_to_bytes(b_mb)}` | `{gb_to_bytes(h_gb)}` | `{min_to_ms(t_min)}` |"
        )

    add("")
    add("## Группы по K_upl")
    add("")

    for k_upl in sorted(grouped):
        rows = grouped[k_upl]
        add(f"## Group K_upl = {k_upl}")
        add("")
        add("### Таблица группы")
        add("")
        add("| № | Experiment ID | Topic | B (MB) | H_local (GB/partition) | T_seg (min) |")
        add("|---:|---|---|---:|---:|---:|")
        for idx, (exp_id, topic, b_mb, h_gb, _, t_min) in enumerate(rows, start=1):
            add(f"| {idx} | `{exp_id}` | `{topic}` | {b_mb} | {h_gb} | {t_min} |")
        add("")

        add("### План обновления broker-level конфигурации")
        add("")
        add("1. Остановить/подготовить Kafka brokers к смене глобального параметра.")
        add(f"2. Установить `{broker_k_upl_key}={k_upl}` в конфигурации брокеров.")
        add("3. Перезапустить Kafka brokers.")
        add("4. Дождаться готовности кластера и проверить health-check.")
        add("5. После этого запустить все эксперименты данной группы.")
        add("")
        add("```bash")
        add(f"export BROKER_K_UPL_KEY={broker_k_upl_key}")
        add(f"export BROKER_K_UPL_VALUE={k_upl}")
        add("# apply the value in broker config and restart Kafka brokers")
        add("```")
        add("")
        add("### Эксперименты группы")
        add("")

        for exp_id, topic, b_mb, h_gb, _, t_min in rows:
            segment_bytes = mb_to_bytes(b_mb)
            local_retention_bytes = gb_to_bytes(h_gb)
            segment_ms = min_to_ms(t_min)

            add(f"### {exp_id}")
            add("")
            add(f"- topic: `{topic}`")
            add(f"- B: `{b_mb} MB`")
            add(f"- H_local: `{h_gb} GB/partition`")
            add(f"- K_upl: `{k_upl}`")
            add(f"- T_seg: `{t_min} min`")
            add("")

            add("#### 0. Сброс остаточного состояния")
            add("")
            add("```bash")
            add(f"bash {clean_script}")
            add(f"docker compose -f {compose_file} up -d")
            add("```")
            add("")

            add("#### 1. Создание topic")
            add("")
            add("```bash")
            add(
                f"docker exec "
                f"-e JMX_PORT= "
                f"-e KAFKA_JMX_PORT= "
                f"-e RMI_HOSTNAME= "
                f"-e KAFKA_JMX_HOSTNAME= "
                f"-e KAFKA_OPTS= "
                f"-it {kafka_container} /opt/kafka/bin/kafka-topics.sh "
                f"--bootstrap-server {topic_bootstrap_servers} \\"
            )
            add(f"  --create --if-not-exists --topic {topic} \\")
            add(f"  --partitions {partitions} --replication-factor {replication_factor} \\")
            add(f"  --config remote.storage.enable={remote_storage_enable} \\")
            add(f"  --config segment.bytes={segment_bytes} \\")
            add(f"  --config local.retention.bytes={local_retention_bytes} \\")
            add(f"  --config segment.ms={segment_ms}")
            add("```")
            add("")

            add("#### 2. Обновление env для producer")
            add("")
            add("```bash")
            add(f"export TOPIC_NAME={topic}")
            add(f"export BOOTSTRAP_SERVERS={bootstrap_servers}")
            add(f"export LOAD_PROFILE={load_profile}")
            add(f"export DURATION_SEC={duration_sec}")
            add(f"export EXPERIMENT_ID={exp_id}")
            add("```")
            add("")

            add("#### 3. Запуск producer")
            add("")
            add("```bash")
            add(f"python3 {producer_script}")
            add("```")
            add("")

            add("#### 4. Обновление env для consumer")
            add("")
            add("```bash")
            add(f"export TOPIC_NAME={topic}")
            add(f"export BOOTSTRAP_SERVERS={bootstrap_servers}")
            add(f"export EXPERIMENT_ID={exp_id}")
            add(f"export CONSUMER_MODE={consumer_mode}")
            add(f"export WARMUP_SEC={warmup_sec}")
            add(f"export MEASURE_SEC={measure_sec}")
            add(f"export SLO_SEC={slo_sec}")
            add("```")
            add("")

            add("#### 5. Запуск consumer")
            add("")
            add("```bash")
            add(f"python3 {consumer_script}")
            add("```")
            add("")

            add("#### 6. Комментарий по прогону")
            add("")
            add("После завершения прогона сохранить метрики, при необходимости очистить остаточное состояние и только затем переходить к следующему эксперименту.")
            add("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate EXPERIMENTS.md for 24 LHS lab experiments")
    parser.add_argument("--out", default="EXPERIMENTS_LHS_LAB.md", help="Output markdown file")
    parser.add_argument("--bootstrap-servers", default="localhost:19092", help="Bootstrap servers for producer/consumer from host")
    parser.add_argument("--topic-bootstrap-servers", default="localhost:9092", help="Bootstrap servers for kafka-topics.sh inside docker exec")
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
    parser.add_argument("--consumer-script", default="consumer_load.py")
    parser.add_argument("--clean-script", default="clean.sh")
    parser.add_argument("--consumer-mode", default="steady-state")
    parser.add_argument("--warmup-sec", type=int, default=900)
    parser.add_argument("--measure-sec", type=int, default=2700)
    parser.add_argument("--slo-sec", type=float, default=0.2)
    parser.add_argument("--compose-file", default="../../docker-compose.yml")
    args = parser.parse_args()

    md = build_markdown(
        bootstrap_servers=args.bootstrap_servers,
        topic_bootstrap_servers=args.topic_bootstrap_servers,
        kafka_container=args.kafka_container,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        remote_storage_enable=args.remote_storage_enable,
        broker_k_upl_key=args.broker_k_upl_key,
        load_profile=args.load_profile,
        duration_sec=args.duration_sec,
        producer_script=args.producer_script,
        consumer_script=args.consumer_script,
        clean_script=args.clean_script,
        consumer_mode=args.consumer_mode,
        warmup_sec=args.warmup_sec,
        measure_sec=args.measure_sec,
        slo_sec=args.slo_sec,
        compose_file=args.compose_file,
    )

    out_path = Path(args.out)
    out_path.write_text(md, encoding="utf-8")
    print(f"Generated: {out_path}")


if __name__ == "__main__":
    main()
