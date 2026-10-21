# wiki-stream-lab — Technical Design

Laptop-scale real-time pipeline: **live Wikipedia edits → Apache Kafka → Apache Flink SQL**, all in Docker. This document is the reference for how it's built. For a guided walkthrough see the tutorial in [`README.md`](../README.md). For scope and phases see [`PRD.md`](PRD.md).

> **Status:** Phase 1. The Compose stack and producer are written but have **not yet been run end to end**. The Flink SQL in §6 is the design for phase 1 and isn't committed to `sql/` yet.

---

## Contents

1. [Architecture](#1-architecture)
2. [Components & versions](#2-components--versions)
3. [Repository layout](#3-repository-layout)
4. [Data contracts](#4-data-contracts)
5. [Kafka design](#5-kafka-design)
6. [Stream processing (Flink SQL)](#6-stream-processing-flink-sql)
7. [Docker setup](#7-docker-setup)
8. [Operations cheat sheet](#8-operations-cheat-sheet)
9. [Troubleshooting](#9-troubleshooting)
10. [Known limitations](#10-known-limitations)
11. [Future phases](#11-future-phases)

---

## 1. Architecture

### Phase 1

```
                    Docker Compose network (service names resolve as DNS)
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                                                                          │
 │  ┌────────────┐  JSON, key=wiki   ┌──────────────────────┐               │
 │  │  producer  │ ────────────────▶ │        kafka         │◀── kafka-ui   │
 │  │  (Python)  │   kafka:29092     │   KRaft, 1 broker    │    :8080      │
 │  └─────▲──────┘                   │                      │               │
 │        │                          │  wiki.recentchange   │               │
 │        │                          │  wiki.edits.clean    │               │
 │        │                          └────▲────────┬────────┘               │
 │        │                         sink  │        │ source                 │
 │        │                               │        ▼                        │
 │        │                          ┌────┴─────────────────┐               │
 │        │                          │ Flink JobManager     │ :8081 web UI  │
 │        │                          │ Flink TaskManager    │ 4 slots, p=2  │
 │        │                          │ job: clean_edits     │               │
 │        │                          │ + ad-hoc SELECTs     │               │
 │        │                          └──────────────────────┘               │
 └────────┼─────────────────────────────────────────────────────────────────┘
          │ SSE over HTTPS
 stream.wikimedia.org/v2/stream/recentchange   (~20–50 events/s)
```

**Data flow**

1. `producer` holds a long-lived HTTP connection to Wikimedia EventStreams and receives one Server-Sent Event per change on any Wikimedia project.
2. Each event is trimmed to a flat JSON record and produced to `wiki.recentchange`, keyed by `wiki`.
3. Flink SQL exposes the topic as a table (`recentchange`). You can query it interactively, and the long-running job `clean_edits` writes a filtered, typed version to `wiki.edits.clean`.
4. You see data in three places: Kafka UI (both topics), the SQL client (live `SELECT`), and the Flink UI (job graph and metrics).

### Target architecture (after phase 3)

```
producer → wiki.recentchange → [clean_edits] → wiki.edits.clean ─┬─▶ [to_iceberg] → Iceberg table → DuckDB   (phase 2)
                                                                 └─▶ [windowed jobs] → wiki.* result topics    (phase 3)
```

Keeping `wiki.edits.clean` as a stable intermediate topic means later phases add consumers without touching ingestion.

---

## 2. Components & versions

All versions are pinned.

| Component | Image / package | Version | Why |
|---|---|---|---|
| Kafka broker | `apache/kafka` | 3.9.0 | Official image; KRaft (no ZooKeeper) in one container |
| Kafka UI | `kafbat/kafka-ui` | v1.0.0 | Browse topics, partitions, messages, consumer lag |
| Flink | `flink` | 1.20.1-scala_2.12-java17 | 1.20 is the 1.x LTS line; mature connector ecosystem |
| Flink Kafka connector | `flink-sql-connector-kafka` | 3.3.0-1.20 | Must match the Flink minor version (`-1.20`) |
| Producer runtime | `python` | 3.12-slim | Prebuilt `confluent-kafka` wheels |
| Kafka client | `confluent-kafka` | 2.6.1 | librdkafka-based, standard producer configs |
| HTTP client | `requests` | 2.32.3 | Streaming responses for SSE |

**Why Flink SQL** rather than the DataStream API or PyFlink: SQL reaches the core streaming ideas without a Java build loop, and PyFlink lags behind current Python releases.

---

## 3. Repository layout

```
wiki-stream-lab/
├── docker-compose.yml        # the whole stack
├── flink/
│   └── Dockerfile            # flink:1.20.1 + Kafka SQL connector JAR + checkpoint dir
├── producer/
│   ├── Dockerfile            # python:3.12-slim image for the producer
│   ├── requirements.txt
│   └── producer.py           # SSE → Kafka
├── sql/                      # Flink SQL scripts; bind-mounted to /opt/sql in the JobManager
├── docs/
│   ├── PRD.md                # product requirements and phases
│   └── TECHNICAL_DESIGN.md   # this document
└── README.md                 # step-by-step tutorial (start here)
```

---

## 4. Data contracts

### 4.1 Source: Wikimedia EventStreams

- Endpoint: `https://stream.wikimedia.org/v2/stream/recentchange`
- Protocol: [Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html), an endless stream of `event:` / `id:` / `data:` lines over HTTPS.
- No auth. Wikimedia **requires a descriptive `User-Agent`**: `wiki-stream-lab/0.1 (https://github.com/xiaotied/wiki-stream-lab)`.
- Volume: ~20–50 events/s across all wikis. Wikidata bot edits are a large share.
- Resume: sending the last `id` as `Last-Event-ID` resumes from that point (limited server-side history).
- `meta.domain == "canary"` events are synthetic health checks and are dropped.

### 4.2 Topic `wiki.recentchange` (raw)

Written by the producer (`producer.py:trim`). Key = `wiki` (UTF-8 string). Value = UTF-8 JSON:

| Field | Type | Source field | Notes |
|---|---|---|---|
| `id` | long \| null | `id` | Recent-changes ID; unique **per wiki**, not globally; null for some log events |
| `type` | string | `type` | `edit`, `new`, `log`, `categorize` |
| `title` | string | `title` | Page title incl. namespace prefix |
| `user` | string | `user` | Username, or IP for anonymous edits |
| `bot` | boolean | `bot` | Flagged bot account |
| `wiki` | string | `wiki` | e.g. `enwiki`, `dewiki`, `wikidatawiki`, `commonswiki` |
| `server_name` | string | `server_name` | e.g. `en.wikipedia.org` |
| `namespace` | int | `namespace` | 0 = article, 1 = talk, 2 = user, … |
| `minor` | boolean \| null | `minor` | Edits only |
| `length_old` | int \| null | `length.old` | Bytes before change (null for new pages) |
| `length_new` | int \| null | `length.new` | Bytes after change |
| `event_time` | string | `meta.dt` | ISO-8601 UTC, e.g. `2026-10-20T12:34:56Z` |

Example:

```json
{"id": 1834567890, "type": "edit", "title": "Apache Kafka", "user": "ExampleUser",
 "bot": false, "wiki": "enwiki", "server_name": "en.wikipedia.org", "namespace": 0,
 "minor": false, "length_old": 41210, "length_new": 41388, "event_time": "2026-10-20T12:34:56Z"}
```

### 4.3 Topic `wiki.edits.clean` (Flink output)

Written by Flink job `clean_edits`. Only `type IN ('edit', 'new')`. Key = `wiki` (raw string). Value = JSON:

| Field | Type | Derivation |
|---|---|---|
| `event_ts` | timestamp, ISO-8601 | parsed from `event_time` |
| `wiki` | string | as is |
| `server_name` | string | as is |
| `title` | string | as is |
| `user_name` | string | `user` (renamed: `USER` is reserved in SQL) |
| `is_bot` | boolean | `bot` |
| `is_new_page` | boolean | `type = 'new'` |
| `is_minor` | boolean | `COALESCE(minor, false)` |
| `namespace` | int | as is |
| `length_new` | int \| null | as is |
| `byte_delta` | int \| null | `length_new - COALESCE(length_old, 0)` |

Both contracts are schemaless JSON on purpose. A typed contract (Avro + Schema Registry) is a later-phase item.

---

## 5. Kafka design

### 5.1 Broker

One container plays both `broker` and `controller` (`KAFKA_PROCESS_ROLES: broker,controller`), so there is no ZooKeeper. With a single broker, every replication factor is 1, and durability depends on the `kafka-data` volume.

### 5.2 Listeners

A client connects to a bootstrap address, and the broker then returns the **advertised** address to use from then on. That address must be reachable from where the client runs, so the broker exposes two client listeners:

| Listener | Bound on | Advertised as | Used by |
|---|---|---|---|
| `INTERNAL` | `:29092` | `kafka:29092` | Containers: producer, Flink, Kafka UI |
| `EXTERNAL` | `:9092` (published) | `localhost:9092` | Tools on the host |
| `CONTROLLER` | `:9093` | — | KRaft quorum only |

Symptom if wrong: the client connects, then fails with "connection to node 1 (localhost/127.0.0.1:9092) could not be established" from inside a container.

### 5.3 Topics

| Topic | Partitions | Key | Retention | Writer → Reader |
|---|---|---|---|---|
| `wiki.recentchange` | 3 | `wiki` | 24 h | producer → Flink |
| `wiki.edits.clean` | 3 | `wiki` | 24 h | Flink `clean_edits` → Kafka UI / console (phase 2: Iceberg job) |

Both are auto-created on first write with the broker defaults (`KAFKA_NUM_PARTITIONS: 3`, `KAFKA_LOG_RETENTION_HOURS: 24`).

**Key skew:** keying by `wiki` keeps each wiki's events ordered on one partition, but `wikidatawiki` dominates its partition. This is kept on purpose so the skew is visible.

### 5.4 Producer settings

| Setting | Value | Effect |
|---|---|---|
| `acks` | `all` | Leader waits for all in-sync replicas (one here) |
| `linger.ms` | 50 | Batch for up to 50 ms → fewer, larger requests |
| `compression.type` | `lz4` | Cheap CPU, good ratio on JSON |
| delivery callback | logs failures | Visible in `docker compose logs producer` |

Delivery is **at-least-once**: on reconnect, events near the `Last-Event-ID` boundary may be re-sent. Idempotence is off.

---

## 6. Stream processing (Flink SQL)

### 6.1 Cluster configuration

Set via `FLINK_PROPERTIES` in `docker-compose.yml` (shared by both Flink services through a YAML anchor):

| Key | Value | Notes |
|---|---|---|
| `jobmanager.rpc.address` | `jobmanager` | Compose service name |
| `taskmanager.numberOfTaskSlots` | 4 | |
| `parallelism.default` | 2 | Compare with 3 source partitions |
| `taskmanager.memory.process.size` | 2048m | |
| `jobmanager.memory.process.size` | 1024m | |
| `state.checkpoints.dir` | `file:///opt/flink/checkpoints` | Shared `flink-checkpoints` volume |

### 6.2 Source table (`sql/01_source.sql`, planned)

```sql
CREATE TABLE recentchange (
  id          BIGINT,
  `type`      STRING,
  title       STRING,
  `user`      STRING,          -- USER is reserved in Flink SQL
  bot         BOOLEAN,
  wiki        STRING,
  server_name STRING,
  namespace   INT,
  minor       BOOLEAN,
  length_old  INT,
  length_new  INT,
  event_time  STRING,
  -- '2026-10-20T12:34:56Z' -> TIMESTAMP(3)
  event_ts AS TO_TIMESTAMP(REPLACE(REPLACE(event_time, 'T', ' '), 'Z', ''))
) WITH (
  'connector' = 'kafka',
  'topic' = 'wiki.recentchange',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-wiki',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);
```

There's no `WATERMARK` in phase 1, because nothing depends on event-time progress yet. Phase 3 adds `WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND`.

### 6.3 Sink table + job `clean_edits` (`sql/02_clean_edits.sql`, planned)

```sql
SET 'execution.checkpointing.interval' = '30s';
SET 'pipeline.name' = 'clean_edits';

CREATE TABLE edits_clean (
  event_ts     TIMESTAMP(3),
  wiki         STRING,
  server_name  STRING,
  title        STRING,
  user_name    STRING,
  is_bot       BOOLEAN,
  is_new_page  BOOLEAN,
  is_minor     BOOLEAN,
  namespace    INT,
  length_new   INT,
  byte_delta   INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'wiki.edits.clean',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'raw',
  'key.fields' = 'wiki',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601'
);

INSERT INTO edits_clean
SELECT
  event_ts,
  wiki,
  server_name,
  title,
  `user`                               AS user_name,
  bot                                  AS is_bot,
  `type` = 'new'                       AS is_new_page,
  COALESCE(minor, FALSE)               AS is_minor,
  namespace,
  length_new,
  length_new - COALESCE(length_old, 0) AS byte_delta
FROM recentchange
WHERE `type` IN ('edit', 'new');
```

Design notes:

- **Stateless job:** filter + projection only. No windows or state, so the output is an append-only stream.
- **Checkpointing (30 s)** is on so the Kafka source commits offsets to group `flink-wiki`, which makes consumer lag visible in Kafka UI. The Kafka sink's default delivery guarantee is at-least-once (it flushes on checkpoint).
- **Ad-hoc queries** (`SELECT … FROM recentchange`) run in the SQL client session and stop when you cancel them. `INSERT INTO` submits a detached job that keeps running in the cluster.
- **Loading files:** `./bin/sql-client.sh -i /opt/sql/01_source.sql` runs DDL at startup. The SQL client has no `SOURCE` command.

---

## 7. Docker setup

| Service | Build / image | Host ports | Volumes | Depends on |
|---|---|---|---|---|
| `kafka` | `apache/kafka:3.9.0` | 9092 | `kafka-data` → `/var/lib/kafka/data` | — |
| `kafka-ui` | `kafbat/kafka-ui:v1.0.0` | 8080 | — | `kafka` healthy |
| `producer` | `./producer` | — | — | `kafka` healthy |
| `jobmanager` | `./flink` | 8081 | `flink-checkpoints`, `./sql` → `/opt/sql` | — |
| `taskmanager` | `./flink` | — | `flink-checkpoints` | `jobmanager` |

Docker concepts in use:

- **Service DNS:** containers reach each other by service name on the default Compose network.
- **Port publishing:** only the UIs and the external Kafka listener are published to `localhost`.
- **Named volumes** (`kafka-data`, `flink-checkpoints`) survive `down` and are deleted by `down -v`.
- **Bind mount** `./sql` → `/opt/sql`: edit SQL on the host, run it in the container.
- **Custom images:** `flink/Dockerfile` adds the connector JAR to the official image. `producer/Dockerfile` packages the app with layer caching (deps before code).
- **Healthcheck + `depends_on: condition: service_healthy`:** dependents wait until Kafka actually answers.
- **Restart policy:** the producer uses `restart: unless-stopped`.
- **YAML anchor** `&flink-props` shares the Flink config.

Prerequisites: Docker Desktop (or any engine with Compose v2), ≥ 6 GB memory for Docker, and outbound HTTPS to `stream.wikimedia.org`.

---

## 8. Operations cheat sheet

```sh
# Lifecycle
docker compose up -d --build
docker compose ps
docker compose logs -f <service>
docker compose down            # keep volumes
docker compose down -v         # delete Kafka data + checkpoints

# Topics & messages
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic wiki.recentchange
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic wiki.edits.clean --max-messages 5 --property print.key=true --property print.partition=true

# Consumer lag for the Flink job (requires checkpointing)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group flink-wiki

# Flink
docker compose exec jobmanager ./bin/sql-client.sh
docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/01_source.sql
docker compose exec jobmanager ./bin/flink list
docker compose exec jobmanager ./bin/flink cancel <job-id>

# Resources
docker stats
```

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop not running | Open it and wait for "Engine running" |
| Producer logs `403` / `429` | Missing/blocked User-Agent or rate limited | Keep a descriptive UA; back off |
| Producer logs nothing | Kafka not healthy, or no network egress | `docker compose ps`; `curl -I https://stream.wikimedia.org` |
| Client connects then hangs / "node 1 (localhost…) could not be established" | Wrong listener for where the client runs | Containers use `kafka:29092`, the host uses `localhost:9092` (§5.2) |
| `Could not find any factory for identifier 'kafka'` | Connector JAR missing | `docker compose build --no-cache jobmanager taskmanager` |
| SQL parse error near `user` | `USER` is reserved | Quote as `` `user` `` |
| `SELECT` shows no rows | `latest-offset` + producer not running | Check producer logs; or set `'scan.startup.mode' = 'earliest-offset'` |
| Consumer group `flink-wiki` missing / no lag shown | Offsets are only committed on checkpoints | `SET 'execution.checkpointing.interval' = '30s';` before `INSERT` |
| Job stuck in `CREATED` | Not enough task slots | Lower parallelism or add slots |
| Containers OOM-killed | Docker memory too low | Docker Desktop → Resources → Memory ≥ 6 GB |
| Port in use (8080/8081/9092) | Another local service | Change the host side of `ports:` |

---

## 10. Known limitations

- **Single broker, RF = 1:** no Kafka fault tolerance.
- **Gap on producer restart:** `Last-Event-ID` is in memory only, so a restarted container resumes from "now".
- **At-least-once:** duplicates are possible around producer reconnects and Flink restarts. Dedup comes in phase 2.
- **Schemaless JSON:** fields can drift silently, and the Flink source ignores parse errors.
- **Key skew:** `wikidatawiki` dominates one partition.
- **No persistence beyond Kafka retention (24 h)** in phase 1.
- **Not production-hardened:** no TLS, auth, resource limits or monitoring.

---

## 11. Future phases

Details and acceptance criteria are in [`PRD.md` §6](PRD.md#6-later-phases-outline).

| Phase | Adds | Key design questions |
|---|---|---|
| **2 — Lakehouse** | Flink job `wiki.edits.clean` → Apache Iceberg (Hadoop catalog on local disk → MinIO + REST catalog), DuckDB via the `iceberg` extension, event UUID (`meta.id`) in the contract | Hadoop classpath for Flink. Absolute paths in Iceberg metadata vs host paths. Commit interval vs small files. Dedup strategy |
| **3 — Real-time analytics** | Watermarks; tumbling windows; sliding-window Top-N (append-only) vs all-time Top-N (changelog → `upsert-kafka`); spike detection; failure experiments | Watermark delay vs completeness. Idle partitions (`table.exec.source.idle-timeout`). Default restart strategy is *none* without checkpointing |
