# Эксперименты LHS для лабораторного стенда

Файл сгенерирован автоматически.

## Правило сортировки и нумерации

`Experiment ID` и `topic` присваиваются после сортировки конфигураций по следующим полям:

1. `K_upl`
2. `B`
3. `H_local`
4. `T_seg`

Таким образом, `EXP-001` соответствует первой конфигурации в этом упорядоченном списке, а не порядку записи в исходном LHS-manifest.

## Фиксированные параметры стенда

- brokers: `3`
- partitions: `12`
- replication.factor: `2`
- remote.storage.enable: `true`
- bootstrap.servers: `localhost:19092`
- producer script: `producer_load.py`
- default LOAD_PROFILE: `write-heavy`
- default DURATION_SEC: `3600`
- broker-level key for K_upl: `remote.log.manager.thread.pool.size`

## Общая таблица экспериментов

| № | Experiment ID | Topic | B (MB) | H_local (GB/partition) | K_upl | T_seg (min) | segment.bytes | local.retention.bytes | segment.ms |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | `EXP-001` | `exp-001` | 64 | 2 | 1 | 5 | `67108864` | `2147483648` | `300000` |
| 2 | `EXP-002` | `exp-002` | 64 | 5 | 1 | 1 | `67108864` | `5368709120` | `60000` |
| 3 | `EXP-003` | `exp-003` | 64 | 5 | 1 | 15 | `67108864` | `5368709120` | `900000` |
| 4 | `EXP-004` | `exp-004` | 128 | 2 | 1 | 15 | `134217728` | `2147483648` | `900000` |
| 5 | `EXP-005` | `exp-005` | 128 | 5 | 1 | 5 | `134217728` | `5368709120` | `300000` |
| 6 | `EXP-006` | `exp-006` | 256 | 1 | 1 | 1 | `268435456` | `1073741824` | `60000` |
| 7 | `EXP-007` | `exp-007` | 256 | 1 | 1 | 5 | `268435456` | `1073741824` | `300000` |
| 8 | `EXP-008` | `exp-008` | 256 | 2 | 1 | 1 | `268435456` | `2147483648` | `60000` |
| 9 | `EXP-009` | `exp-009` | 64 | 1 | 2 | 15 | `67108864` | `1073741824` | `900000` |
| 10 | `EXP-010` | `exp-010` | 64 | 5 | 2 | 1 | `67108864` | `5368709120` | `60000` |
| 11 | `EXP-011` | `exp-011` | 128 | 1 | 2 | 5 | `134217728` | `1073741824` | `300000` |
| 12 | `EXP-012` | `exp-012` | 128 | 1 | 2 | 15 | `134217728` | `1073741824` | `900000` |
| 13 | `EXP-013` | `exp-013` | 128 | 2 | 2 | 1 | `134217728` | `2147483648` | `60000` |
| 14 | `EXP-014` | `exp-014` | 128 | 5 | 2 | 5 | `134217728` | `5368709120` | `300000` |
| 15 | `EXP-015` | `exp-015` | 256 | 1 | 2 | 1 | `268435456` | `1073741824` | `60000` |
| 16 | `EXP-016` | `exp-016` | 256 | 2 | 2 | 15 | `268435456` | `2147483648` | `900000` |
| 17 | `EXP-017` | `exp-017` | 64 | 1 | 4 | 1 | `67108864` | `1073741824` | `60000` |
| 18 | `EXP-018` | `exp-018` | 64 | 1 | 4 | 5 | `67108864` | `1073741824` | `300000` |
| 19 | `EXP-019` | `exp-019` | 64 | 2 | 4 | 15 | `67108864` | `2147483648` | `900000` |
| 20 | `EXP-020` | `exp-020` | 128 | 2 | 4 | 1 | `134217728` | `2147483648` | `60000` |
| 21 | `EXP-021` | `exp-021` | 128 | 5 | 4 | 15 | `134217728` | `5368709120` | `900000` |
| 22 | `EXP-022` | `exp-022` | 256 | 2 | 4 | 5 | `268435456` | `2147483648` | `300000` |
| 23 | `EXP-023` | `exp-023` | 256 | 5 | 4 | 5 | `268435456` | `5368709120` | `300000` |
| 24 | `EXP-024` | `exp-024` | 256 | 5 | 4 | 15 | `268435456` | `5368709120` | `900000` |

## Группы по K_upl

## Group K_upl = 1

### Таблица группы

| № | Experiment ID | Topic | B (MB) | H_local (GB/partition) | T_seg (min) |
|---:|---|---|---:|---:|---:|
| 1 | `EXP-001` | `exp-001` | 64 | 2 | 5 |
| 2 | `EXP-002` | `exp-002` | 64 | 5 | 1 |
| 3 | `EXP-003` | `exp-003` | 64 | 5 | 15 |
| 4 | `EXP-004` | `exp-004` | 128 | 2 | 15 |
| 5 | `EXP-005` | `exp-005` | 128 | 5 | 5 |
| 6 | `EXP-006` | `exp-006` | 256 | 1 | 1 |
| 7 | `EXP-007` | `exp-007` | 256 | 1 | 5 |
| 8 | `EXP-008` | `exp-008` | 256 | 2 | 1 |

### План обновления broker-level конфигурации

1. Остановить/подготовить Kafka brokers к смене глобального параметра.
2. Установить `remote.log.manager.thread.pool.size=1` в конфигурации брокеров.
3. Перезапустить Kafka brokers.
4. Дождаться готовности кластера и проверить health-check.
5. После этого запустить все эксперименты данной группы.

```bash
export BROKER_K_UPL_KEY=remote.log.manager.thread.pool.size
export BROKER_K_UPL_VALUE=1
```

### Эксперименты группы

### EXP-001

- topic: `exp-001`
- B: `64 MB`
- H_local: `2 GB/partition`
- K_upl: `1`
- T_seg: `5 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-001 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=67108864 \
  --config local.retention.bytes=2147483648 \
  --config segment.ms=300000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-001
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-001
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-002

- topic: `exp-002`
- B: `64 MB`
- H_local: `5 GB/partition`
- K_upl: `1`
- T_seg: `1 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-002 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=67108864 \
  --config local.retention.bytes=5368709120 \
  --config segment.ms=60000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-002
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-002
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-003

- topic: `exp-003`
- B: `64 MB`
- H_local: `5 GB/partition`
- K_upl: `1`
- T_seg: `15 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-003 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=67108864 \
  --config local.retention.bytes=5368709120 \
  --config segment.ms=900000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-003
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-003
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-004

- topic: `exp-004`
- B: `128 MB`
- H_local: `2 GB/partition`
- K_upl: `1`
- T_seg: `15 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-004 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=134217728 \
  --config local.retention.bytes=2147483648 \
  --config segment.ms=900000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-004
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-004
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-005

- topic: `exp-005`
- B: `128 MB`
- H_local: `5 GB/partition`
- K_upl: `1`
- T_seg: `5 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-005 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=134217728 \
  --config local.retention.bytes=5368709120 \
  --config segment.ms=300000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-005
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-005
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-006

- topic: `exp-006`
- B: `256 MB`
- H_local: `1 GB/partition`
- K_upl: `1`
- T_seg: `1 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-006 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=268435456 \
  --config local.retention.bytes=1073741824 \
  --config segment.ms=60000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-006
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-006
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-007

- topic: `exp-007`
- B: `256 MB`
- H_local: `1 GB/partition`
- K_upl: `1`
- T_seg: `5 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-007 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=268435456 \
  --config local.retention.bytes=1073741824 \
  --config segment.ms=300000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-007
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-007
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-008

- topic: `exp-008`
- B: `256 MB`
- H_local: `2 GB/partition`
- K_upl: `1`
- T_seg: `1 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-008 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=268435456 \
  --config local.retention.bytes=2147483648 \
  --config segment.ms=60000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-008
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-008
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

## Group K_upl = 2

### Таблица группы

| № | Experiment ID | Topic | B (MB) | H_local (GB/partition) | T_seg (min) |
|---:|---|---|---:|---:|---:|
| 1 | `EXP-009` | `exp-009` | 64 | 1 | 15 |
| 2 | `EXP-010` | `exp-010` | 64 | 5 | 1 |
| 3 | `EXP-011` | `exp-011` | 128 | 1 | 5 |
| 4 | `EXP-012` | `exp-012` | 128 | 1 | 15 |
| 5 | `EXP-013` | `exp-013` | 128 | 2 | 1 |
| 6 | `EXP-014` | `exp-014` | 128 | 5 | 5 |
| 7 | `EXP-015` | `exp-015` | 256 | 1 | 1 |
| 8 | `EXP-016` | `exp-016` | 256 | 2 | 15 |

### План обновления broker-level конфигурации

1. Остановить/подготовить Kafka brokers к смене глобального параметра.
2. Установить `remote.log.manager.thread.pool.size=2` в конфигурации брокеров.
3. Перезапустить Kafka brokers.
4. Дождаться готовности кластера и проверить health-check.
5. После этого запустить все эксперименты данной группы.

```bash
export BROKER_K_UPL_KEY=remote.log.manager.thread.pool.size
export BROKER_K_UPL_VALUE=2
```

### Эксперименты группы

### EXP-009

- topic: `exp-009`
- B: `64 MB`
- H_local: `1 GB/partition`
- K_upl: `2`
- T_seg: `15 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-009 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=67108864 \
  --config local.retention.bytes=1073741824 \
  --config segment.ms=900000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-009
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-009
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-010

- topic: `exp-010`
- B: `64 MB`
- H_local: `5 GB/partition`
- K_upl: `2`
- T_seg: `1 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-010 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=67108864 \
  --config local.retention.bytes=5368709120 \
  --config segment.ms=60000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-010
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-010
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-011

- topic: `exp-011`
- B: `128 MB`
- H_local: `1 GB/partition`
- K_upl: `2`
- T_seg: `5 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-011 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=134217728 \
  --config local.retention.bytes=1073741824 \
  --config segment.ms=300000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-011
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-011
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-012

- topic: `exp-012`
- B: `128 MB`
- H_local: `1 GB/partition`
- K_upl: `2`
- T_seg: `15 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-012 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=134217728 \
  --config local.retention.bytes=1073741824 \
  --config segment.ms=900000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-012
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-012
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-013

- topic: `exp-013`
- B: `128 MB`
- H_local: `2 GB/partition`
- K_upl: `2`
- T_seg: `1 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-013 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=134217728 \
  --config local.retention.bytes=2147483648 \
  --config segment.ms=60000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-013
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-013
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-014

- topic: `exp-014`
- B: `128 MB`
- H_local: `5 GB/partition`
- K_upl: `2`
- T_seg: `5 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-014 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=134217728 \
  --config local.retention.bytes=5368709120 \
  --config segment.ms=300000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-014
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-014
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-015

- topic: `exp-015`
- B: `256 MB`
- H_local: `1 GB/partition`
- K_upl: `2`
- T_seg: `1 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-015 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=268435456 \
  --config local.retention.bytes=1073741824 \
  --config segment.ms=60000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-015
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-015
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-016

- topic: `exp-016`
- B: `256 MB`
- H_local: `2 GB/partition`
- K_upl: `2`
- T_seg: `15 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-016 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=268435456 \
  --config local.retention.bytes=2147483648 \
  --config segment.ms=900000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-016
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-016
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

## Group K_upl = 4

### Таблица группы

| № | Experiment ID | Topic | B (MB) | H_local (GB/partition) | T_seg (min) |
|---:|---|---|---:|---:|---:|
| 1 | `EXP-017` | `exp-017` | 64 | 1 | 1 |
| 2 | `EXP-018` | `exp-018` | 64 | 1 | 5 |
| 3 | `EXP-019` | `exp-019` | 64 | 2 | 15 |
| 4 | `EXP-020` | `exp-020` | 128 | 2 | 1 |
| 5 | `EXP-021` | `exp-021` | 128 | 5 | 15 |
| 6 | `EXP-022` | `exp-022` | 256 | 2 | 5 |
| 7 | `EXP-023` | `exp-023` | 256 | 5 | 5 |
| 8 | `EXP-024` | `exp-024` | 256 | 5 | 15 |

### План обновления broker-level конфигурации

1. Остановить/подготовить Kafka brokers к смене глобального параметра.
2. Установить `remote.log.manager.thread.pool.size=4` в конфигурации брокеров.
3. Перезапустить Kafka brokers.
4. Дождаться готовности кластера и проверить health-check.
5. После этого запустить все эксперименты данной группы.

```bash
export BROKER_K_UPL_KEY=remote.log.manager.thread.pool.size
export BROKER_K_UPL_VALUE=4
```

### Эксперименты группы

### EXP-017

- topic: `exp-017`
- B: `64 MB`
- H_local: `1 GB/partition`
- K_upl: `4`
- T_seg: `1 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-017 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=67108864 \
  --config local.retention.bytes=1073741824 \
  --config segment.ms=60000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-017
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-017
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-018

- topic: `exp-018`
- B: `64 MB`
- H_local: `1 GB/partition`
- K_upl: `4`
- T_seg: `5 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-018 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=67108864 \
  --config local.retention.bytes=1073741824 \
  --config segment.ms=300000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-018
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-018
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-019

- topic: `exp-019`
- B: `64 MB`
- H_local: `2 GB/partition`
- K_upl: `4`
- T_seg: `15 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-019 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=67108864 \
  --config local.retention.bytes=2147483648 \
  --config segment.ms=900000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-019
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-019
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-020

- topic: `exp-020`
- B: `128 MB`
- H_local: `2 GB/partition`
- K_upl: `4`
- T_seg: `1 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-020 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=134217728 \
  --config local.retention.bytes=2147483648 \
  --config segment.ms=60000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-020
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-020
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-021

- topic: `exp-021`
- B: `128 MB`
- H_local: `5 GB/partition`
- K_upl: `4`
- T_seg: `15 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-021 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=134217728 \
  --config local.retention.bytes=5368709120 \
  --config segment.ms=900000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-021
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-021
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-022

- topic: `exp-022`
- B: `256 MB`
- H_local: `2 GB/partition`
- K_upl: `4`
- T_seg: `5 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-022 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=268435456 \
  --config local.retention.bytes=2147483648 \
  --config segment.ms=300000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-022
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-022
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-023

- topic: `exp-023`
- B: `256 MB`
- H_local: `5 GB/partition`
- K_upl: `4`
- T_seg: `5 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-023 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=268435456 \
  --config local.retention.bytes=5368709120 \
  --config segment.ms=300000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-023
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-023
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```

### EXP-024

- topic: `exp-024`
- B: `256 MB`
- H_local: `5 GB/partition`
- K_upl: `4`
- T_seg: `15 min`

#### 1. Создание topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --if-not-exists --topic exp-024 \
  --partitions 12 --replication-factor 2 \
  --config remote.storage.enable=true \
  --config segment.bytes=268435456 \
  --config local.retention.bytes=5368709120 \
  --config segment.ms=900000
```

#### 2. Обновление env для генератора нагрузки

```bash
export TOPIC_NAME=exp-024
export BOOTSTRAP_SERVERS=localhost:19092
export LOAD_PROFILE=write-heavy
export DURATION_SEC=3600
export EXPERIMENT_ID=EXP-024
```

#### 3. Запуск генератора нагрузки

```bash
python producer_load.py
```
