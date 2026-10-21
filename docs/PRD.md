# PRD — wiki-stream-lab

| | |
|---|---|
| **Owner** | @xiaotied |
| **Status** | Phase 1 complete (verified 2026-10-20) |
| **Last updated** | 2026-10-20 |
| **Technical design** | [`TECHNICAL_DESIGN.md`](TECHNICAL_DESIGN.md) · **Tutorial** | [`README.md`](../README.md) |

---

## 1. Summary

Build a small but real streaming pipeline that ingests live Wikipedia edits into Kafka and processes them with Flink, running fully in Docker on a laptop. The product is **hands-on experience**: the owner learns Kafka, Flink and Docker from things they built and broke themselves.

The work is split into three phases. **Phase 1 (today, ≤ 60 min)** gets data flowing end to end and visible live. Storage (Iceberg) and analytics (windows, Top-N) come later.

## 2. Problem & motivation

Kafka and Flink are easy to read about and hard to *feel*. Tutorials often use synthetic data, hide configuration behind managed services, or stop at "hello world". This lab uses:

- **Real, continuous, free data** (Wikimedia EventStreams, ~20–50 events/s) with real-world shape: skewed keys, bots, bursts, out-of-order timestamps.
- **Explicit local infrastructure**, so every concept (listeners, partitions, consumer groups, jobs, volumes) is a line of config you can change.
- **Small phases**, each of which ends with something working.

## 3. Target user

A single learner: a software / data engineer comfortable with Python and SQL, new to Kafka, Flink and Docker, working on a 16 GB macOS laptop with limited time per session.

## 4. Phases

| Phase | Theme | Outcome | Timebox |
|---|---|---|---|
| **1 — Live pipeline** | Ingest + stream processing | Edits flow Wikipedia → Kafka → Flink. Visible live in Kafka UI and the Flink SQL client. A Flink job publishes a cleaned topic | **≤ 60 min** |
| **2 — Lakehouse** | Durable analytical storage | Flink writes the cleaned stream to an **Apache Iceberg** table. DuckDB queries it | ~2 h |
| **3 — Real-time analytics** | Time & state | Event-time windows, watermarks, Top-N, spike detection, checkpoint/failure experiments | ~2 h |

## 5. Phase 1 scope

### 5.1 Goals

| ID | Goal |
|---|---|
| G1 | Start the full stack with **one command** (`docker compose up -d --build`) |
| G2 | See real Wikipedia edits arriving in Kafka (Kafka UI + CLI) |
| G3 | Query the live stream with Flink SQL and watch results update |
| G4 | Run one long-lived Flink job that transforms the raw topic into a cleaned topic |
| G5 | Learn the Docker, Kafka and Flink basics needed for all of the above |

### 5.2 Non-goals (phase 1)

- Persisting data outside Kafka (database, files, lake) → phase 2
- Windows, watermarks, aggregations over time, Top-N, alerting → phase 3
- Failure and recovery experiments → phase 3
- Deduplication, schemas (Avro / Schema Registry) → phase 2+
- Production readiness: HA, security, multi-broker, monitoring. Out of scope for all phases

### 5.3 User stories

| ID | As the learner, I want to… | So that… |
|---|---|---|
| US1 | start everything with one command and see events within 2 minutes | setup friction doesn't eat the session |
| US2 | browse raw events in Kafka UI and with CLI tools | I see what topics, partitions, keys and offsets look like |
| US3 | run a SQL query on the live stream and watch rows appear | I understand that Flink tables are views over unbounded streams |
| US4 | submit a job that writes a cleaned topic and see it running in the Flink UI | I understand a pipeline stage: source → transform → sink |
| US5 | stop and restart everything cheaply | experiments are repeatable |

### 5.4 Functional requirements

| ID | Requirement | Priority |
|---|---|---|
| FR1 | Consume `https://stream.wikimedia.org/v2/stream/recentchange` via SSE with a descriptive User-Agent | P0 |
| FR2 | Drop Wikimedia `canary` events | P0 |
| FR3 | Publish a trimmed, flat JSON record to `wiki.recentchange`, keyed by `wiki` (contract: TECHNICAL_DESIGN §4.2) | P0 |
| FR4 | Reconnect automatically on stream errors, resuming via `Last-Event-ID` | P0 |
| FR5 | Log producer throughput every 10 s | P1 |
| FR6 | Single-node Kafka (KRaft) reachable from containers and from the host | P0 |
| FR7 | Kafka UI for topics, messages and consumer groups | P0 |
| FR8 | Flink SQL source table over `wiki.recentchange` with a parsed timestamp column | P0 |
| FR9 | Flink job `clean_edits`: keep `edit` / `new` events, convert types, compute `byte_delta`, write to `wiki.edits.clean` keyed by `wiki` (contract: TECHNICAL_DESIGN §4.3) | P0 |
| FR10 | Checkpointing enabled for the job so Kafka consumer offsets are committed and lag is visible | P1 |
| FR11 | SQL saved in `sql/`, runnable via the SQL client without rebuilding images | P1 |
| FR12 | All image and dependency versions pinned | P0 |

### 5.5 Non-functional requirements

| ID | Requirement | Target |
|---|---|---|
| NFR1 | Resource footprint | ≤ 6 GB RAM in Docker on a 16 GB laptop |
| NFR2 | Startup time | Events visible in Kafka ≤ 2 min after `up` (warm image cache) |
| NFR3 | Latency | Raw event visible in Flink SQL and in `wiki.edits.clean` ≤ 5 s after Wikimedia emits it |
| NFR4 | Setup time | Fresh machine → G1–G4 done in ≤ 60 min, including installing Docker and downloading images |
| NFR5 | Reproducibility | Fresh clone + README → working pipeline, no undocumented steps |
| NFR6 | Etiquette | Respect Wikimedia's User-Agent policy. Single connection, ≥ 3 s reconnect backoff |

### 5.6 Milestones & acceptance criteria

| Milestone | Timebox | Done when |
|---|---|---|
| **M0 — Infra up** | 25 min | Docker Desktop running. `docker compose ps` shows all services up and Kafka `healthy`. Kafka UI (`:8080`) and Flink UI (`:8081`) load |
| **M1 — Ingestion** | 5 min | Producer logs ≥ 10 events/s. Kafka UI shows messages in `wiki.recentchange` across 3 partitions, keyed by wiki name |
| **M2 — Live query** | 10 min | `SELECT` on the source table in the SQL client prints live rows |
| **M3 — Pipeline job** | 10 min | `clean_edits` is RUNNING in the Flink UI. `wiki.edits.clean` receives records matching the contract. Consumer group `flink-wiki` shows lag in Kafka UI |
| **M4 — Docs reconciled** | 5 min | README / TECHNICAL_DESIGN corrected for anything that behaved differently on the real run |

### 5.7 Success criteria (learning outcomes)

At the end of phase 1 the owner can, without notes:

1. Explain the difference between an image, a container and a volume, and how Compose services find each other.
2. Explain topics, partitions, keys, offsets and consumer groups, and point to each in Kafka UI.
3. Explain why Kafka in Docker advertises two listeners.
4. Explain what a Flink `CREATE TABLE` over Kafka is (metadata, not storage) and the difference between an interactive `SELECT` and a submitted `INSERT INTO` job.

## 6. Later phases (outline)

### Phase 2 — Lakehouse with Iceberg
- Flink → **Iceberg** table (Hadoop catalog on a local volume first, then MinIO + REST catalog), partitioned by day, reading from `wiki.edits.clean`.
- DuckDB queries via the `iceberg` extension. Inspect the raw Parquet and metadata files to see what Iceberg adds.
- Add Wikimedia's event UUID (`meta.id`) to the producer contract. Deduplicate at query time, then try upserts.
- Topics: checkpoint-driven commits, partitioning, small files and compaction, schema evolution, time travel.
- Known risks: Hadoop classpath for Flink, absolute paths in Iceberg metadata when reading from the host.

### Phase 3 — Real-time analytics & failure modes
- Event time and watermarks. Tumbling windows (edits per wiki per minute). Bot ratio.
- Sliding-window Top-N (append-only) vs all-time Top-N (changelog → `upsert-kafka`).
- Spike detection (> N edits / page / minute).
- Failure experiments: TaskManager kill with and without checkpoints, producer outage, late data, idle partitions, broker restart.

## 7. Risks & mitigations (phase 1)

| Risk | Impact | Mitigation |
|---|---|---|
| Docker Desktop install / first image pulls slow | Eats the timebox | Install first. Images total ~1.5 GB |
| Kafka listener misconfiguration | Clients connect then hang | Two listeners documented. First thing to check in troubleshooting |
| Flink ↔ connector version mismatch | Source table fails to load | Versions pinned together (`3.3.0-1.20` ↔ `1.20.1`) |
| Wikimedia blocks or throttles the client | No data | Descriptive User-Agent, single connection, backoff |
| Scope creep into phase 2/3 | Phase 1 not finished | Non-goals listed explicitly above |

## 8. Open questions

- Phase 2: start with a Hadoop catalog on local disk, or go straight to MinIO + REST catalog?
- Phase 2: should Flink filter out Wikidata bot edits before storage to cut volume?
- Phase 3: is a small live dashboard worth it, or are the terminal and Kafka UI enough?
