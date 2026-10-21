# PRD — wiki-stream-lab

| | |
|---|---|
| **Owner** | @xiaotied |
| **Status** | Draft — M0 scaffold in progress |
| **Last updated** | 2026-10-20 |
| **Technical design** | [`TECHNICAL_DESIGN.md`](TECHNICAL_DESIGN.md) · **Tutorial** | [`README.md`](../README.md) |

---

## 1. Summary

Build a small but real streaming pipeline that ingests live Wikipedia edits into Kafka and analyzes them with Flink, running fully in Docker on a laptop. The product is **hands-on experience**: by the end, the owner should be able to explain and demonstrate the core mechanics of Kafka, Flink and Docker Compose from things they built and broke themselves.

## 2. Problem & motivation

Kafka and Flink are easy to read about and hard to *feel*. Tutorials often use synthetic data generators, hide configuration behind managed services, or stop at "hello world". This lab uses:

- **Real, continuous, free data** (Wikimedia EventStreams), with enough volume (~20–50 events/s) and enough shape (skewed keys, bots, bursts, out-of-order timestamps) to make streaming concepts matter.
- **Explicit, local infrastructure** so every concept (listeners, partitions, watermarks, checkpoints, volumes) is a line of config you can change.
- **A tight timebox** (~2 hours for the core) so it gets finished.

## 3. Target user

A single learner: a software / data engineer comfortable with Python and SQL, new to Kafka, Flink and Docker, working on a 16 GB macOS laptop.

## 4. Goals

| ID | Goal |
|---|---|
| G1 | Run a complete pipeline with **one command** (`docker compose up -d --build`) |
| G2 | Learn Kafka fundamentals: topics, partitions, keys, producer configs, consumer groups, offsets, listeners |
| G3 | Learn Flink fundamentals: dynamic tables, event time, watermarks, tumbling/sliding windows, Top-N, state, changelog vs append, checkpoints and recovery |
| G4 | Learn Docker fundamentals: images and Dockerfiles, Compose services, networking/DNS, port publishing, volumes vs bind mounts, healthchecks, logs/exec |
| G5 | Produce a repo that is reproducible by someone else from the README alone |

## 5. Non-goals

- Production readiness (HA, security, multi-broker, SLAs, monitoring stack)
- Cloud deployment or Kubernetes
- Exactly-once end-to-end guarantees
- A polished UI (a minimal dashboard is a stretch goal only)
- Long-term storage or a data lake/warehouse sink

## 6. User stories

| ID | As the learner, I want to… | So that… |
|---|---|---|
| US1 | start everything with one command and see events flowing within 2 minutes | setup friction doesn't eat the session |
| US2 | browse raw events in a UI and with CLI tools | I can see what partitions, keys and offsets look like |
| US3 | query the live stream with SQL | I get results fast without a build loop |
| US4 | see edits per wiki per minute computed on event time | I understand windows and watermarks |
| US5 | see the top edited pages in a sliding window, written back to Kafka | I understand stateful Top-N and upsert/changelog streams |
| US6 | get alerted when a page is edited abnormally often | I build a simple real-time detection use case |
| US7 | kill components and watch the system recover (or not) | I understand checkpoints, offsets and failure semantics |
| US8 | tear everything down and start fresh | experiments are cheap and repeatable |

## 7. Functional requirements

### 7.1 Ingestion

| ID | Requirement | Priority |
|---|---|---|
| FR1 | Consume `https://stream.wikimedia.org/v2/stream/recentchange` via SSE with a descriptive User-Agent | P0 |
| FR2 | Drop Wikimedia `canary` events | P0 |
| FR3 | Publish a trimmed, flat JSON record (contract in TECHNICAL_DESIGN §4.2) to `wiki.recentchange` | P0 |
| FR4 | Key messages by `wiki` | P0 |
| FR5 | Reconnect automatically on stream errors, resuming via `Last-Event-ID` | P0 |
| FR6 | Log throughput (events/s) every 10 s | P1 |
| FR7 | Persist `Last-Event-ID` across producer restarts | P2 |

### 7.2 Messaging

| ID | Requirement | Priority |
|---|---|---|
| FR8 | Single-node Kafka in KRaft mode, reachable from containers and from the host | P0 |
| FR9 | Source topic with 3 partitions and 24 h retention | P0 |
| FR10 | Web UI for topics, messages and consumer groups | P1 |

### 7.3 Processing

| ID | Requirement | Priority |
|---|---|---|
| FR11 | Flink SQL source table over `wiki.recentchange` with event-time column and bounded-out-of-orderness watermark | P0 |
| FR12 | J1: edits per wiki per 1-minute tumbling window | P0 |
| FR13 | J2: bot vs human edit ratio per minute | P1 |
| FR14 | J3: top 5 pages per 10-minute window sliding every minute, sunk to Kafka. J3b: all-time top 10 sunk to an `upsert-kafka` topic | P0 |
| FR15 | J4: spike detection (> N edits per page per minute), sunk to Kafka | P1 |
| FR16 | J5: largest byte-size changes | P2 |
| FR17 | Checkpointing can be enabled per job, with checkpoints on a persistent volume | P0 |

### 7.4 Developer experience

| ID | Requirement | Priority |
|---|---|---|
| FR18 | All SQL lives in `sql/` and is runnable from the SQL client without rebuilding images | P0 |
| FR19 | README tutorial teaches the stack step by step. TECHNICAL_DESIGN documents architecture, contracts, ops commands, experiments and troubleshooting | P0 |
| FR20 | All image and dependency versions pinned | P0 |

## 8. Non-functional requirements

| ID | Requirement | Target |
|---|---|---|
| NFR1 | Resource footprint | ≤ 6 GB RAM total in Docker; runs on a 16 GB laptop alongside normal work |
| NFR2 | Startup time | Events visible in Kafka ≤ 2 min after `up` (warm image cache) |
| NFR3 | End-to-end latency | Raw event visible in Flink SQL ≤ 5 s after Wikimedia emits it. Window results emitted ≤ watermark delay + a few seconds after window end |
| NFR4 | Recoverability | With checkpointing on, a TaskManager kill recovers without manual steps and without resetting window state |
| NFR5 | Reproducibility | Fresh clone + README tutorial → working pipeline, no undocumented steps |
| NFR6 | Etiquette | Respect Wikimedia's User-Agent policy. Single connection, no aggressive reconnects (≥ 3 s backoff) |

## 9. Milestones & acceptance criteria

The core (M0–M3) is timeboxed to about 2 hours.

| Milestone | Timebox | Scope | Done when |
|---|---|---|---|
| **M0 — Infra up** | 15 min | Docker Desktop installed. Compose stack builds and starts | `docker compose ps` shows all services up and Kafka healthy. Flink UI and Kafka UI load |
| **M1 — Ingestion** | 20 min | Producer streaming into Kafka | Producer logs ≥ 10 events/s. Messages visible in Kafka UI across 3 partitions. Keys = wiki names |
| **M2 — First Flink job** | 25 min | `sql/01_source.sql`, J1, J2 | `SELECT` on source shows live rows. J1 emits one row per wiki per minute, with watermark visible in Flink UI |
| **M3 — Stateful jobs** | 30 min | J3 (+ upsert-kafka sink), J4 | `wiki.top_pages_10m`, `wiki.top_pages_live` and `wiki.edit_spikes` receive records. Consuming `wiki.top_pages_live` shows repeated updates per rank key |
| **M4 — Break it** | 15 min | Failure experiments (TECHNICAL_DESIGN §10) | Each experiment run, with a one-line observation recorded in TECHNICAL_DESIGN §10 |
| **M5 — Stretch** | open | Persist Last-Event-ID, dedup, Avro + Schema Registry, dashboard, Java DataStream port | Each item merged with docs |

## 10. Success metrics (learning outcomes)

At the end, the owner can, without notes:

1. Explain why Kafka needs both an internal and an external advertised listener in Docker.
2. Explain how a message key maps to a partition, and show the skew it causes here.
3. Explain the difference between event time and processing time, and what a watermark of `ts - 5s` means for late data.
4. Explain why a Top-N query produces a changelog (updates/retractions) and why it needs an upsert sink.
5. Demonstrate the difference in recovery behavior with and without checkpointing.
6. Write a `docker-compose.yml` service with a build, ports, volume, healthcheck and dependency from memory.

## 11. Risks & mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Docker Desktop install/licensing friction | Blocks M0 | Personal use is free. OrbStack or Colima are drop-in fallbacks with the same CLI |
| Flink ↔ connector version mismatch | Source table fails to load | Versions pinned together (`3.3.0-1.20` ↔ `1.20.1`) and documented |
| Wikimedia blocks or throttles the client | No data | Descriptive User-Agent, single connection, backoff. Fall back to replaying a captured sample file |
| Watermarks don't advance (idle partitions) | Windowed jobs emit nothing, confusing | `table.exec.source.idle-timeout` documented in troubleshooting |
| Laptop memory pressure | Containers OOM | Explicit Flink memory settings. Docker memory ≥ 6 GB |
| Scope creep | Core not finished in 2 h | Stretch work explicitly gated behind M4 |

## 12. Open questions

- Should results be shown in a small live dashboard, or is the terminal/Kafka UI enough? (Currently stretch.)
- Keep Flink 1.20 LTS, or move to Flink 2.x once the core works, as a migration exercise?
- Is it worth adding Schema Registry + Avro to learn schema evolution, or stay with JSON?
