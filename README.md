# wiki-stream-lab

**Learn real-time data pipelines by building one:** live Wikipedia edits → Kafka → Flink, all in Docker, on your laptop.

This README is a **hands-on tutorial**. Work through the labs in order. Each one introduces a few concepts, has you run real commands, and ends with questions to check you understood. Reference material (exact configs, data contracts, design decisions) is in [`docs/TECHNICAL_DESIGN.md`](docs/TECHNICAL_DESIGN.md). Goals and phases are in [`docs/PRD.md`](docs/PRD.md).

> **Status:** Phase 1, verified end to end on 2026-10-20 (Docker Desktop 4.96, Apple Silicon). If a command doesn't behave as described, that's a bug in the lab, so please fix it or open an issue.

---

## Roadmap

| Phase | What you build | Status |
|---|---|---|
| **1 — Live pipeline** | Wikipedia → Kafka → Flink. Watch data live, and run one Flink job that publishes a cleaned topic | **this tutorial** (~60 min) |
| 2 — Lakehouse | Flink → Apache Iceberg table, queried with DuckDB | planned |
| 3 — Real-time analytics | Event time, watermarks, windows, Top-N, spike detection, failure experiments | planned |

## What you'll build in phase 1

```
 Wikipedia (every edit, worldwide)
        │  Server-Sent Events over HTTPS
        ▼
 ┌─────────────┐      ┌───────────────────────┐      ┌─────────────────────┐
 │  producer   │ ───▶ │         Kafka         │ ───▶ │        Flink        │
 │  (Python)   │      │  wiki.recentchange    │      │  job: clean_edits   │
 └─────────────┘      │  wiki.edits.clean  ◀──┼───── │  + live SELECTs     │
                      └───────────────────────┘      └─────────────────────┘
        └──────────────── all running in Docker Compose ─────────────────┘

 You watch it in:  Kafka UI (localhost:8080) · Flink UI (localhost:8081) · Flink SQL client
```

By the end you'll be able to:

- **Docker:** read a `docker-compose.yml`, build images, explain how containers find each other, and debug with `logs` / `exec`.
- **Kafka:** explain topics, partitions, keys, offsets and consumer groups, and why Kafka in Docker needs two listeners.
- **Flink:** query an infinite stream with SQL, and run a long-lived job that reads one topic, transforms it, and writes another.

## Course map

| Lab | Topic | Core path | You'll learn |
|---|---|---|---|
| [0](#lab-0--setup--docker-basics) | Setup & Docker basics | 15 min | images, containers, ports, volumes, Compose |
| [1](#lab-1--start-the-stack-and-see-the-data) | Start the stack & see the data | 15 min | Compose up, the producer, topics, partitions, keys |
| [2](#lab-2--query-the-live-stream-with-flink-sql) | Query the live stream with Flink SQL | 15 min | dynamic tables, JobManager/TaskManager, append vs update |
| [3](#lab-3--your-first-pipeline-job) | Your first pipeline job | 10 min | source → transform → sink, detached jobs, consumer groups |
| [4](#lab-4--clean-up) | Clean up | 2 min | teardown |

Sections marked **(optional)** deepen understanding but aren't required to finish within 60 minutes.

**Conventions:**
- `$` lines run in your Mac terminal, from the repo root.
- `Flink SQL>` lines run inside the Flink SQL client.
- 💡 = concept · ✅ = what you should see · 🧠 = check yourself · 🏋️ = optional challenge

---

## Five-minute primer: what is a real-time pipeline?

A **batch** pipeline runs on a schedule over a finished dataset ("yesterday's edits"). A **streaming** pipeline processes events continuously as they happen ("each edit, a second after it happened").

Most streaming systems have three layers:

1. **Sources** produce events: apps, sensors, database change logs, or here, Wikipedia.
2. **A log / message broker (Kafka)** stores events durably and in order, and lets many readers consume and replay them independently. It decouples producers from consumers.
3. **A stream processor (Flink)** reads from the log and computes continuously: filters, transformations, and later (phase 3) windows and aggregations.

**Docker** runs all of this locally without installing Java, Kafka or Flink on your machine. Each component runs in its own container.

---

## Lab 0 — Setup & Docker basics

**Goal:** get Docker running and learn the handful of commands you'll use all day.

### 0.1 Install Docker Desktop

```sh
$ brew install --cask docker
```

Open **Docker Desktop** from Applications once, accept the terms, and wait for "Engine running". Then check **Settings → Resources → Memory ≥ 6 GB**.

```sh
$ docker version          # shows Client AND Server; if Server errors, the engine isn't running
$ docker compose version  # Compose v2 is built in
```

💡 **Why Docker Desktop on a Mac?** Containers are a Linux feature. Docker Desktop runs a small Linux VM and forwards the `docker` CLI to it.

### 0.2 Start the downloads now

The first build downloads ~1.5 GB of images. Run these in a second terminal and continue with the basics while they finish. Compose commands must run **from the repo folder**, because that's where it looks for `docker-compose.yml` (otherwise: `no configuration file provided`).

```sh
$ cd ~/wiki-stream-lab                   # wherever you cloned it
$ docker compose pull kafka kafka-ui     # pre-fetch published images
$ docker compose build                   # build our two images (producer, flink)
```

### 0.3 Images, containers, ports, logs, exec

```sh
$ docker run hello-world
```

💡 An **image** is a read-only template (filesystem + default command), like a class. A **container** is a running instance of it, like an object. `docker run` pulls the image if needed, then creates and starts a container.

```sh
$ docker run -d --name web -p 8888:80 nginx:1.27
$ curl -s localhost:8888 | head -5      # your Mac → port 8888 → container port 80
$ docker ps                             # running containers
$ docker logs web                       # its stdout/stderr
$ docker exec -it web sh                # a shell INSIDE the container; `exit` to leave
$ docker rm -f web                      # stop + delete
```

💡 Containers are isolated: a port inside a container is only reachable from your Mac if you **publish** it with `-p host:container`.

### 0.4 Volumes (optional)

```sh
$ docker volume create demo
$ docker run --rm -v demo:/data alpine sh -c 'echo hello > /data/msg'
$ docker run --rm -v demo:/data alpine cat /data/msg      # → hello, read by a NEW container
$ docker volume rm demo
```

💡 A container's filesystem disappears with the container. **Named volumes** live on separately: Kafka's data uses one here. A **bind mount** (`./sql:/opt/sql`) maps a host folder into a container, so you can edit files on your Mac.

### 0.5 Read the Compose file

Open [`docker-compose.yml`](docker-compose.yml). For each service, find:

- `image:` or `build:`: a published image, or one built from a local `Dockerfile`
- `ports:`: what your Mac can reach (`localhost:8080`, `:8081`, `:9092`)
- `volumes:`: what persists
- `depends_on:` + `healthcheck:`: start order (the producer waits until Kafka is *healthy*, not just started)
- `environment:`: configuration passed into the container

💡 **Service names are hostnames.** Compose puts all services on one private network with built-in DNS, so the producer reaches Kafka at `kafka:29092`. Your Mac isn't on that network, so it uses `localhost` plus the published ports.

🧠 **Check yourself**
1. What's the difference between an image and a container?
2. Why can the producer use `kafka` as a hostname, while you use `localhost`?
3. Which data survives `docker compose down`? Which survives `down -v`?

---

## Lab 1 — Start the stack and see the data

**Goal:** get real Wikipedia edits into Kafka and understand what you're looking at.

### 1.1 Peek at the raw source

```sh
$ curl -s -H 'User-Agent: wiki-stream-lab-tutorial (https://github.com/xiaotied/wiki-stream-lab)' \
    https://stream.wikimedia.org/v2/stream/recentchange | head -12
```

💡 This is **Server-Sent Events (SSE)**: a never-ending HTTP response where each event is `id: …` + `data: {json}`. Every edit on every Wikimedia site appears here, roughly 20–50 per second. Wikimedia asks every client to send a descriptive **User-Agent**.

### 1.2 Start everything

```sh
$ docker compose up -d --build
$ docker compose ps                    # kafka must show (healthy)
$ docker compose logs -f producer      # Ctrl-C to stop following
```

✅ The producer prints `NNN events in last 10s (xx.x/s)` every 10 seconds.

### 1.3 Look inside Kafka

Open **Kafka UI → http://localhost:8080** → Topics → `wiki.recentchange`.

- **Messages** tab: real edits. Each message has a **key** (the wiki name, e.g. `enwiki`), a **partition** and an **offset**.
- **Overview**: message counts per partition.

💡 **Kafka in one paragraph.** A **topic** is split into **partitions**, and each partition is an append-only, ordered log. Every message gets an **offset** (its position in that partition). Messages aren't removed when read: they stay until retention expires (24 h here), so any number of consumers can read and re-read them.

💡 **Keys → partitions.** The partition is `hash(key) % partitions`. Same key means same partition, so order is guaranteed *per key*, not across the topic. Here, all `enwiki` edits stay in order on one partition.

✅ The partition counts are **uneven**. The partition holding `wikidatawiki` (mostly bot edits) dominates. That's **key skew**, a real-world problem: a hot key overloads one partition and therefore one consumer.

The same thing from the CLI:

```sh
$ docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --topic wiki.recentchange
$ docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic wiki.recentchange --max-messages 5 \
    --property print.key=true --property print.partition=true --property print.offset=true
```

### 1.4 How the producer works

Open [`producer/producer.py`](producer/producer.py):

- `stream_events()` reads SSE lines and remembers the last `id`, so it can **resume** after a disconnect (`Last-Event-ID`).
- `trim()` flattens each big upstream event into ~12 fields. This is the **data contract** ([TECHNICAL_DESIGN §4.2](docs/TECHNICAL_DESIGN.md#42-topic-wikirecentchange-raw)).
- `producer.produce(..., key=event["wiki"])` is the key choice you just saw in Kafka UI.
- Config: `acks=all` (wait for a durable write), `linger.ms=50` (batch messages for 50 ms), `compression.type=lz4`, and `partitioner=murmur2_random`.

💡 **Real gotcha found while building this lab:** the Python client (librdkafka) hashes keys with CRC32 by default, while Java clients, including Flink, use murmur2. Without `partitioner=murmur2_random`, `commonswiki` landed on partition 0 in the raw topic and partition 2 in Flink's output topic. "Same key → same partition" only holds when every client uses the same hash function.

Then look at [`producer/Dockerfile`](producer/Dockerfile): base image → install deps → copy code → command. `requirements.txt` is copied *before* the code, so editing `producer.py` reuses the cached `pip install` layer.

### 1.5 Two listeners: why Kafka in Docker is tricky

Find `KAFKA_LISTENERS` / `KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml`.

💡 A Kafka client connects to a bootstrap address, and the broker replies "from now on, talk to me at *X*". *X* must be reachable from where the client runs. Containers need `kafka:29092`, and your Mac needs `localhost:9092`, so the broker advertises both on separate listeners. If you get this wrong, clients connect and then hang.

🧠 **Check yourself**
1. Why is the producer configured with `kafka:29092`, not `localhost:9092`?
2. What do you gain and lose by keying on `wiki`?
3. If the producer container restarts, are any edits lost? (Hint: where is `Last-Event-ID` kept?)

🏋️ **Challenge (optional):** create a scratch topic and produce keyed messages by hand:
```sh
$ docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic lab.hello --property parse.key=true --property key.separator=:
> enwiki:edit 1
> dewiki:edit 2
> enwiki:edit 3
```
Consume it with `--property print.partition=true` and confirm the same key always lands on the same partition.

---

## Lab 2 — Query the live stream with Flink SQL

**Goal:** see Flink consume from Kafka, using nothing but SQL.

### 2.1 Meet Flink

Open **Flink UI → http://localhost:8081**. You should see 1 TaskManager with 4 slots.

💡 **Flink architecture.** The **JobManager** plans and coordinates jobs. **TaskManagers** do the work in **task slots**. Every query becomes a **job**: a graph of operators (source → filter → sink), each running as N parallel **subtasks** (2 by default here).

[`flink/Dockerfile`](flink/Dockerfile) extends the official image with the Kafka connector JAR. Connectors are plugins, and their version must match Flink's (`3.3.0-1.20` ↔ Flink `1.20`).

### 2.2 Open the SQL client

```sh
$ docker compose exec jobmanager ./bin/sql-client.sh
```

```sql
Flink SQL> SET 'sql-client.execution.result-mode' = 'tableau';
```

### 2.3 Declare the Kafka topic as a table

Type this in yourself, since writing it helps it stick:

```sql
Flink SQL> CREATE TABLE recentchange (
  id          BIGINT,
  `type`      STRING,
  title       STRING,
  `user`      STRING,          -- USER is a reserved word, so backticks
  bot         BOOLEAN,
  wiki        STRING,
  server_name STRING,
  namespace   INT,
  minor       BOOLEAN,
  length_old  INT,
  length_new  INT,
  event_time  STRING,
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

💡 `CREATE TABLE` stores **no data**. It's metadata: "this Kafka topic, read as JSON, looks like this table." `event_ts` is a **computed column** that parses the ISO timestamp string into a real `TIMESTAMP`. `latest-offset` means "start from new messages only".

### 2.4 Run a query that never ends

```sql
Flink SQL> SELECT event_ts, wiki, title, `user`, bot FROM recentchange;
```

✅ Rows keep appearing, a second or so after people edit Wikipedia. While it runs, open the Flink UI → **Running Jobs**: your `SELECT` is a real job. Click it to see the job graph and records per second. Press `Ctrl-C` in the client to stop it.

Try a filter and a computed column:

```sql
Flink SQL> SELECT title, `user`, length_new - length_old AS delta
           FROM recentchange
           WHERE wiki = 'enwiki' AND `type` = 'edit' AND NOT bot;
```

### 2.5 Appends vs updates (optional)

```sql
Flink SQL> SELECT wiki, COUNT(*) AS edits FROM recentchange GROUP BY wiki;
```

✅ You see `+I` (insert), then `-U` / `+U` (update before/after) as counts change.

💡 **Dynamic tables.** Flink treats a stream as a table that keeps growing, and a query as a **continuously updating result**. A filter only *appends* rows. A `GROUP BY` without a time window has to *update* rows it already emitted, which makes its output a **changelog**. Windows (phase 3) are how you get final, append-only aggregates.

🧠 **Check yourself**
1. Where is the data of table `recentchange` actually stored?
2. What happens to the `SELECT` job when you press `Ctrl-C`?
3. Why does `GROUP BY wiki` emit `-U/+U` rows, while the `WHERE` query only emits `+I`?

---

## Lab 3 — Your first pipeline job

**Goal:** turn the interactive query into a real pipeline stage that keeps running: **read raw → clean → write a new topic**.

### 3.1 Turn on checkpointing and name the job

```sql
Flink SQL> SET 'execution.checkpointing.interval' = '30s';
Flink SQL> SET 'pipeline.name' = 'clean_edits';
```

💡 A **checkpoint** periodically snapshots the job's progress. For now, the useful effect is that Flink **commits its Kafka offsets** on each checkpoint, so Kafka UI can show how far behind the job is (consumer lag). Crash recovery with checkpoints is phase 3.

### 3.2 Declare the output topic

```sql
Flink SQL> CREATE TABLE edits_clean (
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
```

💡 Same idea as before: a table definition pointing at a topic, now used as a **sink**. `key.fields = 'wiki'` keeps the same partitioning as the raw topic.

### 3.3 Submit the job

```sql
Flink SQL> INSERT INTO edits_clean
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

✅ The client prints a **Job ID** and returns **immediately**. Unlike `SELECT`, an `INSERT INTO` is submitted to the cluster and runs on its own. You can close the SQL client and it keeps going.

### 3.4 Verify the pipeline

1. **Flink UI** → Running Jobs → `clean_edits` is **RUNNING**. Open it and check the records received and sent per operator. After 30 s, the **Checkpoints** tab shows completed checkpoints.
2. **Kafka UI** → Topics → `wiki.edits.clean` → Messages: cleaned records with `event_ts`, `user_name`, `byte_delta`, keyed by wiki.
3. **Kafka UI** → Consumers → `flink-wiki`: the job's committed offsets and lag on `wiki.recentchange`.

✅ Lag shows a few hundred messages and drops after each checkpoint. That's expected: Flink commits offsets **only on checkpoints** (every 30 s), so the committed position trails the real one by up to 30 s of data (~800 events). The consumer-groups CLI also shows no `CONSUMER-ID`, because Flink assigns partitions itself instead of joining the group like a normal consumer. The group is only used to store offsets.

From the CLI:

```sh
$ docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic wiki.edits.clean --max-messages 5 --property print.key=true
$ docker compose exec jobmanager ./bin/flink list
```

💡 **You just built a pipeline stage.** Raw topic → stateless transform → clean topic is the most common streaming pattern. Downstream systems now consume a stable, cleaned contract ([TECHNICAL_DESIGN §4.3](docs/TECHNICAL_DESIGN.md#43-topic-wikieditsclean-flink-output)) instead of raw upstream data. In phase 2, an Iceberg sink will read from `wiki.edits.clean`.

💡 **Stream vs batch transformation.** Transforming in Flink isn't the only option. Many teams land raw Kafka data in a data lake and clean it later with batch tools (Glue, Spark, dbt): simpler and cheaper, but minutes to hours stale. Streaming transforms like this one are used where seconds matter, and most companies run both. See [TECHNICAL_DESIGN: Where to transform](docs/TECHNICAL_DESIGN.md#where-to-transform-two-industry-patterns).

### 3.5 Save your SQL (optional)

The statements from this tutorial are already saved in [`sql/01_source.sql`](sql/01_source.sql) and [`sql/02_clean_edits.sql`](sql/02_clean_edits.sql). Next time:

```sh
# interactive session with the source table preloaded
$ docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/01_source.sql

# or submit the whole job non-interactively
$ docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/01_source.sql -f /opt/sql/02_clean_edits.sql
```

⚠️ In SQL files, put comments on their own line. The client only ends a statement at a `;` that ends the line, so `SET ...;  -- comment` merges with the next statement and fails with `only single statement supported`.

(`./sql` on your Mac is bind-mounted to `/opt/sql` in the container, so there's no rebuild.)

🧠 **Check yourself**
1. Why keep both a raw and a clean topic, instead of cleaning inside the producer?
2. Why did `INSERT INTO` return immediately while `SELECT` kept your terminal busy?
3. What would happen to `wiki.edits.clean` if you stopped the producer? And if you cancelled the job?
4. Why does consumer group `flink-wiki` only show offsets after checkpointing is enabled?

🏋️ **Challenge (optional):** cancel the job (`./bin/flink cancel <job-id>`), add a filter that drops Wikidata bot edits (`NOT (wiki = 'wikidatawiki' AND bot)`), and resubmit. Compare message rates in Kafka UI.

---

## Lab 4 — Clean up

```sh
$ docker compose down        # stop and remove containers; volumes (Kafka data) kept
$ docker compose down -v     # also delete volumes, for a completely fresh start
$ docker system df           # how much disk Docker is using
```

Next session, `docker compose up -d` brings everything back. The Flink job does **not** come back on its own: resubmit it from `sql/`.

---

## What's next

- **Phase 2 — Lakehouse:** write `wiki.edits.clean` to an **Apache Iceberg** table with Flink and query it with **DuckDB**. You'll learn why table formats exist, partitioning, checkpoint-driven commits and small files. See [PRD §6](docs/PRD.md#6-later-phases-outline).
- **Phase 3 — Real-time analytics:** event time, watermarks, tumbling and sliding windows, Top-N, spike detection, and failure/recovery experiments.

---

## Glossary

| Term | Meaning |
|---|---|
| **Image / container** | Template / running instance |
| **Volume / bind mount** | Docker-managed persistent storage / a host folder mapped into a container |
| **Topic / partition / offset** | Named stream / ordered shard of it / position within a shard |
| **Key** | Determines the partition, so ordering is per key |
| **Consumer group** | Consumers sharing a topic's partitions, with committed offsets |
| **Lag** | How far a consumer group is behind the latest offset |
| **Listener / advertised listener** | Address Kafka binds to / address it tells clients to use |
| **Dynamic table** | Flink's view of a stream as a continuously changing table |
| **Source / sink** | Where a Flink job reads from / writes to |
| **Append / changelog stream** | Only inserts / inserts, updates and deletes |
| **Checkpoint** | Periodic snapshot of a job's progress (offsets and state) |

## Further reading

- Kafka: [Introduction](https://kafka.apache.org/intro)
- Flink: [Dynamic tables](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/concepts/dynamic_tables/) · [Kafka SQL connector](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/kafka/) · [SQL client](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sqlclient/)
- Docker: [Get started](https://docs.docker.com/get-started/) · [Compose file reference](https://docs.docker.com/reference/compose-file/)
- Wikimedia: [EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service)
