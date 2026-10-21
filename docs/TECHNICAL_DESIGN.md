# wiki-stream-lab — Technical Design

A hands-on, laptop-scale real-time data pipeline: **live Wikipedia edits → Apache Kafka → Apache Flink SQL**, everything running in Docker.

The goal is learning, not production: each component is configured explicitly so the concepts (partitions, keys, listeners, event time, watermarks, windows, state, checkpoints, container networking) are visible and easy to break on purpose.

> **Status:** scaffold only — the Compose stack and producer are written but have **not yet been run end to end**. Flink SQL jobs are designed (see [§6](#6-stream-processing-flink-sql)) but not yet committed to `sql/`. Product scope and milestones live in [`PRD.md`](PRD.md). New here? Follow the step-by-step tutorial in [`README.md`](../README.md).

---

## Contents

1. [Architecture](#1-architecture)
2. [Components & versions](#2-components--versions)
3. [Repository layout](#3-repository-layout)
4. [Data source & event contract](#4-data-source--event-contract)
5. [Kafka design](#5-kafka-design)
6. [Stream processing (Flink SQL)](#6-stream-processing-flink-sql)
7. [Docker setup](#7-docker-setup)
8. [Getting started](#8-getting-started)
9. [Operations cheat sheet](#9-operations-cheat-sheet)
10. [Failure experiments](#10-failure-experiments)
11. [Troubleshooting](#11-troubleshooting)
12. [Known limitations](#12-known-limitations)
13. [Roadmap](#13-roadmap)

---

## 1. Architecture

```
                       Docker Compose network (service names resolve as DNS)
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                                                                               │
 │  ┌──────────────┐  JSON, key=wiki   ┌─────────────────────┐                   │
 │  │   producer   │ ────────────────▶ │        kafka        │◀──── kafka-ui     │
 │  │  (Python)    │   kafka:29092     │  KRaft, 1 broker    │      :8080        │
 │  └──────▲───────┘                   │                     │                   │
 │         │ SSE                       │ wiki.recentchange   │                   │
 │         │                           │   (3 partitions)    │                   │
 │         │                           │ wiki.* result topics│                   │
 │         │                           └───────▲─────┬───────┘                   │
 │         │                                   │     │ source                    │
 │         │                              sink │     ▼                           │
 │         │                           ┌───────┴─────────────┐                   │
 │         │                           │  Flink JobManager   │  :8081 web UI     │
 │         │                           │  Flink TaskManager  │  4 slots, p=2     │
 │         │                           │  (SQL jobs)         │──▶ checkpoints vol│
 │         │                           └─────────────────────┘                   │
 └─────────┼─────────────────────────────────────────────────────────────────────┘
           │
 stream.wikimedia.org/v2/stream/recentchange   (public Server-Sent Events, ~20–50 events/s)
```

**Data flow**

1. `producer` opens a long-lived HTTP connection to Wikimedia EventStreams and receives one Server-Sent Event per change made on any Wikimedia project.
2. Each event is trimmed to a small, flat JSON record and produced to Kafka topic `wiki.recentchange`, keyed by `wiki` (e.g. `enwiki`).
3. Flink SQL reads the topic as an unbounded table, assigns event time from the record's `event_time`, and runs windowed / stateful queries.
4. Results go back to Kafka topics (`wiki.*`) or are printed in the SQL client.

---

## 2. Components & versions

All versions are pinned so the stack is reproducible.

| Component | Image / package | Version | Why this choice |
|---|---|---|---|
| Kafka broker | `apache/kafka` | 3.9.0 | Official image; KRaft mode (no ZooKeeper) in a single container |
| Kafka UI | `kafbat/kafka-ui` | v1.0.0 | Browse topics, partitions, messages, consumer lag |
| Flink | `flink` | 1.20.1-scala_2.12-java17 | 1.20 is the LTS line of 1.x; connector ecosystem and docs are mature |
| Flink Kafka connector | `flink-sql-connector-kafka` | 3.3.0-1.20 | Must match the Flink minor version (`-1.20`) |
| Producer runtime | `python` | 3.12-slim | Has prebuilt `confluent-kafka` wheels |
| Kafka client | `confluent-kafka` | 2.6.1 | librdkafka-based, fast, exposes standard producer configs |
| HTTP client | `requests` | 2.32.3 | Streaming responses for SSE |

**Why Flink SQL instead of the DataStream API or PyFlink?** SQL covers the core streaming ideas (event time, watermarks, windows, joins, state, changelogs) without a Java build loop, which matters in a ~2 hour session. PyFlink is avoided because it lags behind current Python releases.

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
│   ├── PRD.md                # product requirements: goals, scope, milestones, acceptance criteria
│   └── TECHNICAL_DESIGN.md   # this document
└── README.md                 # step-by-step tutorial (start here)
```

---

## 4. Data source & event contract

### 4.1 Wikimedia EventStreams

- Endpoint: `https://stream.wikimedia.org/v2/stream/recentchange`
- Protocol: [Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html) over HTTPS. The body is an endless stream of lines:
  ```
  event: message
  id: [{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":123...}]
  data: {"$schema":"/mediawiki/recentchange/1.0.0","meta":{...},"type":"edit",...}
  ```
- No authentication. Wikimedia **requires a descriptive `User-Agent`**; requests without one may be blocked. The producer sends `wiki-stream-lab/0.1 (https://github.com/xiaotied/wiki-stream-lab)`.
- Volume: roughly 20–50 events/s across all wikis (Wikidata bot edits are a large share).
- Resuming: sending the last seen `id` back as the `Last-Event-ID` header resumes the stream from that point (the server keeps a limited history).
- `meta.domain == "canary"` events are synthetic health checks from Wikimedia and are dropped.

### 4.2 Record produced to Kafka

The producer flattens and trims the upstream event (`producer.py:trim`). Topic `wiki.recentchange`, value is UTF-8 JSON:

| Field | Type | Source field | Notes |
|---|---|---|---|
| `id` | long \| null | `id` | Recent-changes ID; **unique per wiki, not globally**; null for some log events |
| `type` | string | `type` | `edit`, `new`, `log`, `categorize` |
| `title` | string | `title` | Page title incl. namespace prefix (e.g. `Talk:Foo`) |
| `user` | string | `user` | Username or IP for anonymous edits |
| `bot` | boolean | `bot` | Flagged bot account |
| `wiki` | string | `wiki` | DB name, e.g. `enwiki`, `dewiki`, `wikidatawiki`, `commonswiki` |
| `server_name` | string | `server_name` | e.g. `en.wikipedia.org` |
| `namespace` | int | `namespace` | 0 = article, 1 = talk, 2 = user, … |
| `minor` | boolean \| null | `minor` | Only present for edits |
| `length_old` | int \| null | `length.old` | Page size in bytes before change |
| `length_new` | int \| null | `length.new` | Page size in bytes after change |
| `event_time` | string | `meta.dt` | ISO-8601 UTC, e.g. `2026-10-20T12:34:56Z`; **the event-time column in Flink** |

Message **key** = `wiki` (UTF-8 string). Example value:

```json
{"id": 1834567890, "type": "edit", "title": "Apache Kafka", "user": "ExampleUser",
 "bot": false, "wiki": "enwiki", "server_name": "en.wikipedia.org", "namespace": 0,
 "minor": false, "length_old": 41210, "length_new": 41388, "event_time": "2026-10-20T12:34:56Z"}
```

The contract is schemaless JSON on purpose (fewer moving parts). Adding Schema Registry + Avro is a roadmap item.

---

## 5. Kafka design

### 5.1 Broker (KRaft, single node)

One container plays both `broker` and `controller` roles (`KAFKA_PROCESS_ROLES: broker,controller`), so there is no ZooKeeper. Because there is a single broker, every internal topic uses replication factor 1, and data durability depends on the `kafka-data` volume.

### 5.2 Listeners — the one networking concept to really understand

A Kafka client first connects to a bootstrap address, then the broker replies with the **advertised** address it must use from then on. That address must be reachable *from where the client runs*, so the broker exposes two client listeners:

| Listener | Bound on | Advertised as | Used by |
|---|---|---|---|
| `INTERNAL` | `:29092` | `kafka:29092` | Containers on the Compose network (producer, Flink, Kafka UI) |
| `EXTERNAL` | `:9092` (published to host) | `localhost:9092` | Tools on your Mac |
| `CONTROLLER` | `:9093` | — | KRaft controller quorum (internal only) |

Classic symptom of getting this wrong: the client connects, then hangs or errors with "connection to node 1 (localhost/127.0.0.1:9092) could not be established" from inside a container.

### 5.3 Topics

| Topic | Partitions | Key | Cleanup | Writer → Reader |
|---|---|---|---|---|
| `wiki.recentchange` | 3 (broker default) | `wiki` | delete, 24 h retention | producer → Flink |
| `wiki.edits_per_wiki_1m` *(planned)* | 3 | `wiki` | delete | Flink → console / UI |
| `wiki.top_pages_10m` *(planned)* | 3 | none | delete | Flink → console / UI |
| `wiki.top_pages_live` *(planned)* | 3 | `rn` | compact (upsert) | Flink → console / UI |
| `wiki.edit_spikes` *(planned)* | 3 | `wiki,title` | delete | Flink → console / UI |

Topics are auto-created on first write (`auto.create.topics.enable` defaults to true) and inherit `KAFKA_NUM_PARTITIONS: 3`.

**Partitioning consequence:** keying by `wiki` keeps all events of one wiki in order on one partition, but the load is skewed — `wikidatawiki` alone can dominate a partition. This is intentional, so the skew shows up in Kafka UI and in Flink's per-subtask metrics.

### 5.4 Producer settings

| Setting | Value | Effect |
|---|---|---|
| `acks` | `all` | Leader waits for all in-sync replicas (just one here) |
| `linger.ms` | 50 | Waits up to 50 ms to fill batches → fewer, larger requests |
| `compression.type` | `lz4` | Cheap CPU, good ratio on repetitive JSON |
| delivery callback | logs failures | Errors are visible in `docker compose logs producer` |

Delivery semantics are **at-least-once**: on reconnect the producer resumes via `Last-Event-ID`, and an event near the boundary may be re-sent. Idempotence is not enabled. Downstream queries treat counts as approximate.

---

## 6. Stream processing (Flink SQL)

### 6.1 Cluster configuration

Set via `FLINK_PROPERTIES` in `docker-compose.yml` (shared by JobManager and TaskManager through a YAML anchor):

| Key | Value | Notes |
|---|---|---|
| `jobmanager.rpc.address` | `jobmanager` | Compose service name |
| `taskmanager.numberOfTaskSlots` | 4 | Max parallel subtasks per TaskManager |
| `parallelism.default` | 2 | Default operator parallelism; compare with 3 source partitions |
| `taskmanager.memory.process.size` | 2048m | Fits comfortably on a 16 GB laptop |
| `jobmanager.memory.process.size` | 1024m | |
| `state.checkpoints.dir` | `file:///opt/flink/checkpoints` | Backed by the shared `flink-checkpoints` volume |

Checkpointing is **not enabled by default**; it is switched on per session (`SET 'execution.checkpointing.interval' = '10s';`) during the failure experiments so its effect is observable.

### 6.2 Source table (planned `sql/01_source.sql`)

```sql
CREATE TABLE recentchange (
  id          BIGINT,
  `type`      STRING,
  title       STRING,
  `user`      STRING,          -- USER is a reserved word in Flink SQL
  bot         BOOLEAN,
  wiki        STRING,
  server_name STRING,
  namespace   INT,
  minor       BOOLEAN,
  length_old  INT,
  length_new  INT,
  event_time  STRING,
  -- '2026-10-20T12:34:56Z' -> TIMESTAMP(3)
  ts AS TO_TIMESTAMP(REPLACE(REPLACE(event_time, 'T', ' '), 'Z', '')),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
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

- **Event time vs processing time:** `ts` comes from the edit itself, not from when Flink sees it, so windows are correct even when the producer lags or replays.
- **Watermark:** "I believe no event older than `max(ts) - 5s` will still arrive." A window closes once the watermark passes its end. Later events are dropped by windowed aggregations.
- **Idle partitions:** if one of the 3 partitions gets no data, the watermark cannot advance. If windows never emit, set `SET 'table.exec.source.idle-timeout' = '10s';`.

### 6.3 Planned jobs

| # | Job | Flink concepts | Output |
|---|---|---|---|
| J1 | Edits per wiki per minute | `TUMBLE` window TVF, event time, append stream | `wiki.edits_per_wiki_1m` |
| J2 | Bot vs human ratio per minute | conditional aggregation in windows | console |
| J3 | Top 5 most-edited pages, 10 min window sliding every 1 min | `HOP` window + Window Top-N (`ROW_NUMBER()` partitioned by window). Emits final rows once per window, so **append-only** | `wiki.top_pages_10m` |
| J3b | All-time top 10 pages since job start | regular (non-windowed) Top-N → **changelog** (updates/retractions) → `upsert-kafka` keyed by rank | `wiki.top_pages_live` |
| J4 | Edit-spike detector (> N edits on one page in 1 min) | windowed aggregation + `HAVING`, keyed state | `wiki.edit_spikes` |
| J5 | Biggest byte deltas (vandalism / blanking candidates) | stateless projection + filter | console |

Sketch of J1:

```sql
SELECT window_start, window_end, wiki, COUNT(*) AS edits
FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, wiki;
```

Sketch of J3 (Top-N over a sliding window):

```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY edits DESC) AS rn
  FROM (
    SELECT window_start, window_end, wiki, title, COUNT(*) AS edits
    FROM TABLE(HOP(TABLE recentchange, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '10' MINUTE))
    WHERE `type` = 'edit'
    GROUP BY window_start, window_end, wiki, title
  )
) WHERE rn <= 5;
```

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

- **Service DNS:** containers address each other by service name (`kafka`, `jobmanager`) on the default Compose network.
- **Port publishing:** only UIs and the external Kafka listener are published to `localhost`.
- **Named volumes** (`kafka-data`, `flink-checkpoints`) survive `docker compose down`, are wiped by `down -v`, and are shared between JobManager and TaskManager for checkpoints.
- **Bind mount** (`./sql`): edit SQL on the host and run it in the container without rebuilding.
- **Custom images:** `flink/Dockerfile` extends the official image (the connector JAR is added with `ADD <url>`). `producer/Dockerfile` packages the Python app.
- **Healthchecks + `depends_on: condition: service_healthy`:** the producer and UI wait until Kafka actually answers, not just until its container started.
- **Restart policy:** the producer uses `restart: unless-stopped`, so a crash is retried automatically.
- **YAML anchors:** `&flink-props` / `*flink-props` share one Flink config block.

---

## 8. Getting started

### 8.1 Prerequisites

- macOS (Apple Silicon or Intel) or Linux
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (`brew install --cask docker`, then open it once). Any Docker engine with Compose v2 works.
- ~6 GB free RAM for Docker (Flink ≈ 3 GB, Kafka ≈ 1 GB, rest small)
- Outbound HTTPS to `stream.wikimedia.org`

### 8.2 Run

```sh
git clone https://github.com/xiaotied/wiki-stream-lab.git
cd wiki-stream-lab
docker compose up -d --build       # first run downloads images + builds two
docker compose ps                  # kafka should be "healthy"
docker compose logs -f producer    # expect "NNN events in last 10s (xx.x/s)"
```

Open:
- Kafka UI → http://localhost:8080 (topic `wiki.recentchange`, Messages tab)
- Flink UI → http://localhost:8081

### 8.3 Run SQL

```sh
docker compose exec jobmanager ./bin/sql-client.sh                          # empty session
docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/01_source.sql # run DDL file on startup
```

```sql
-- inside the SQL client
SET 'sql-client.execution.result-mode' = 'tableau';
SELECT wiki, title, `user` FROM recentchange;
```

---

## 9. Operations cheat sheet

```sh
# Topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic wiki.recentchange

# Read messages (with keys and partitions)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic wiki.recentchange --max-messages 5 \
  --property print.key=true --property print.partition=true

# Consumer groups / lag (Flink's group is flink-wiki)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group flink-wiki

# Flink jobs
docker compose exec jobmanager ./bin/flink list
docker compose exec jobmanager ./bin/flink cancel <job-id>

# Containers
docker compose logs -f <service>
docker compose restart producer
docker stats                        # live CPU / memory per container

# Tear down
docker compose down                 # keep volumes
docker compose down -v              # delete Kafka data + checkpoints too
```

Note: Flink commits Kafka offsets only on checkpoints, so `flink-wiki` lag is only meaningful once checkpointing is enabled.

---

## 10. Failure experiments

These are the point of the lab. Run each while a windowed job (J1) is running and watch the Flink UI.

| Experiment | How | What to observe |
|---|---|---|
| TaskManager crash, no checkpoints | `docker compose kill taskmanager`, then `start taskmanager` | With checkpointing off the default restart strategy is *none*: the job goes to `FAILED` and must be resubmitted, losing all state |
| TaskManager crash, with checkpoints | `SET 'execution.checkpointing.interval' = '10s';`, resubmit, then kill | Job goes `RESTARTING`, then restores state and Kafka offsets from the last checkpoint once a TaskManager is back (may take up to ~1 min) |
| Producer outage | `docker compose stop producer` for 1–2 min, then `start` | Watermark stalls, windows stop emitting, then catch up. Note the gap (see §12) |
| Late data | Lower watermark delay to `0` seconds and compare counts | Out-of-order events get dropped by windows |
| Parallelism vs partitions | `SET 'parallelism.default' = '4';` with 3 partitions | One source subtask is idle; watermark stalls without `idle-timeout` |
| Broker restart | `docker compose restart kafka` | Producer and Flink reconnect; data persists via `kafka-data` |

---

## 11. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker Desktop not running | Open Docker Desktop and wait for "Engine running" |
| Producer logs `403` / `429` from Wikimedia | Missing or blocked User-Agent, or rate limited | Keep a descriptive UA; back off |
| Producer logs nothing | Kafka not healthy yet, or no network egress | `docker compose ps`; check `curl -I https://stream.wikimedia.org` |
| `Could not find any factory for identifier 'kafka'` | Connector JAR missing from `/opt/flink/lib` | `docker compose build --no-cache jobmanager taskmanager` |
| Windowed query prints nothing | Watermark not advancing (idle partition, or no new data) | `SET 'table.exec.source.idle-timeout' = '10s';`; check the watermark in the Flink UI |
| SQL parse error near `user` | `USER` is reserved | Quote as `` `user` `` |
| Flink job stuck in `CREATED` | Not enough task slots | Lower parallelism or raise `taskmanager.numberOfTaskSlots` |
| Containers OOM-killed | Docker Desktop memory limit too low | Settings → Resources → Memory ≥ 6 GB |
| Port already in use (8080/8081/9092) | Another local service | Change the left side of the `ports:` mapping |

---

## 12. Known limitations

- **Single broker, RF = 1:** no fault tolerance in Kafka itself. Fine for a laptop lab.
- **Gap on producer restart:** `Last-Event-ID` is kept in memory only, so a container restart resumes from "now" and drops the events in between. A fix is to persist the last ID (e.g. in a volume or a compacted Kafka topic).
- **At-least-once ingestion:** duplicates are possible around reconnects. No dedup by `(wiki, id)` is done yet.
- **Schemaless JSON:** fields can drift silently. Parse errors are ignored by the Flink source.
- **Key skew:** `wikidatawiki` dominates one partition.
- **Not production-hardened:** no TLS, no auth, no resource limits, no monitoring stack.

---

## 13. Roadmap

See [`PRD.md`](PRD.md) for milestones and acceptance criteria.

- [x] M0 — Compose stack scaffold (Kafka, Kafka UI, Flink, producer)
- [ ] M0 — First successful `docker compose up` with events flowing
- [ ] M1 — Source table + J1/J2 in `sql/`
- [ ] M2 — J3/J3b/J4 with Kafka sinks
- [ ] M3 — Failure experiments documented with observations
- [ ] Stretch — persist `Last-Event-ID`; dedup; Avro + Schema Registry; small live dashboard; one job ported to the Java DataStream API
