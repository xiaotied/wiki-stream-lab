# wiki-stream-lab

**Learn real-time data pipelines by building one:** live Wikipedia edits → Kafka → Flink, all in Docker, on your laptop.

This README is a **hands-on tutorial**. Work through the labs in order. Each one introduces a few concepts, has you run real commands, and ends with questions to check you understood. For reference material (exact configs, data contract, design decisions) see [`docs/TECHNICAL_DESIGN.md`](docs/TECHNICAL_DESIGN.md). For goals and scope see [`docs/PRD.md`](docs/PRD.md).

> **Status:** the stack hasn't been run end to end yet. If a command doesn't behave as described, that's a bug in the lab, so please open an issue or fix it.

---

## What you'll build

```
 Wikipedia (every edit, worldwide)
        │  Server-Sent Events over HTTPS
        ▼
 ┌─────────────┐      ┌──────────────────────┐      ┌──────────────────────┐
 │  producer   │ ───▶ │        Kafka         │ ───▶ │        Flink         │
 │  (Python)   │      │  topic: wiki.recent- │      │  SQL: windows, Top-N │
 └─────────────┘      │  change (3 parts)    │ ◀─── │  spike detection     │
                      └──────────────────────┘      └──────────────────────┘
        └──────────────────── all running in Docker Compose ────────────────┘
```

By the end you'll be able to:

- **Docker:** write and read a `docker-compose.yml`, build images, wire containers together over a network, persist data with volumes, and debug with `logs` / `exec`.
- **Kafka:** explain topics, partitions, keys, offsets and consumer groups, and why Kafka in Docker needs two listeners.
- **Flink:** query an infinite stream with SQL, use event time and watermarks, build tumbling/sliding windows and Top-N, and recover from crashes with checkpoints.

## Course map

| Lab | Topic | Time | You'll learn |
|---|---|---|---|
| [0](#lab-0--setup--docker-basics) | Setup & Docker basics | 15 min | images, containers, ports, volumes, exec/logs |
| [1](#lab-1--kafka-by-hand) | Kafka by hand | 15 min | topics, partitions, keys, offsets, consumer groups |
| [2](#lab-2--stream-real-data-into-kafka) | Stream real data into Kafka | 15 min | SSE, producers, batching, key skew, custom images |
| [3](#lab-3--first-steps-with-flink-sql) | First steps with Flink SQL | 15 min | dynamic tables, JobManager/TaskManager, job graph |
| [4](#lab-4--time-and-windows) | Time and windows | 20 min | event time, watermarks, tumbling windows |
| [5](#lab-5--state-top-n-and-writing-back-to-kafka) | State, Top-N, sinks | 20 min | sliding windows, append vs changelog, upsert-kafka |
| [6](#lab-6--break-it-on-purpose) | Break it on purpose | 15 min | checkpoints, restarts, late data, idle partitions |
| [7](#lab-7--clean-up--next-steps) | Clean up & next steps | 5 min | teardown, where to go from here |

**Conventions:**
- `$` lines run in your Mac terminal, from the repo root.
- `Flink SQL>` lines run inside the Flink SQL client.
- 💡 = concept explanation · ✅ = checkpoint: what you should see · 🧠 = check yourself · 🏋️ = optional challenge

---

## Five-minute primer: what is a real-time pipeline?

A **batch** pipeline runs on a schedule over a finished dataset ("yesterday's edits"). A **streaming** pipeline processes events continuously as they happen ("edits in the last minute, updated every minute").

Most streaming systems have three layers:

1. **Sources** produce events: apps, sensors, change logs, or here, Wikipedia.
2. **A log / message broker (Kafka)** stores events durably and in order, and lets many readers consume them independently and replay them. It decouples producers from consumers.
3. **A stream processor (Flink)** reads from the log and computes things continuously (counts, windows, joins, alerts), keeping **state** that survives crashes.

**Docker** is how we run all of this locally without installing Java, Kafka or Flink on your machine. Each component runs in its own isolated container.

---

## Lab 0 — Setup & Docker basics

**Goal:** get Docker running and learn the five commands you'll use all day.

### 0.1 Install Docker Desktop

```sh
$ brew install --cask docker
```

Open **Docker Desktop** from Applications once, accept the terms, and wait for "Engine running". Then give it enough memory: **Settings → Resources → Memory ≥ 6 GB**.

```sh
$ docker version          # shows Client AND Server; if Server errors, the engine isn't running
$ docker compose version  # Compose v2 is built in
```

💡 **Why Docker Desktop on a Mac?** Containers are a Linux feature. On macOS, Docker Desktop runs a small Linux VM and forwards the `docker` CLI to it. Your containers run inside that VM.

### 0.2 Images vs containers

```sh
$ docker run hello-world
```

💡 An **image** is a read-only template (filesystem + default command), like a class. A **container** is a running instance of an image, like an object. `docker run` = pull the image if missing, then create and start a container.

### 0.3 Ports, logs, exec

Run a web server in the background (`-d`) and publish container port 80 on your Mac's port 8888 (`-p host:container`):

```sh
$ docker run -d --name web -p 8888:80 nginx:1.27
$ curl -s localhost:8888 | head -5      # your Mac → port 8888 → container port 80
$ docker ps                             # running containers
$ docker logs web                       # its stdout/stderr (your curl shows up here)
$ docker exec -it web sh                # a shell INSIDE the container; type `exit` to leave
$ docker rm -f web                      # stop + delete
```

💡 Containers are isolated: a port inside a container isn't reachable from your Mac unless you **publish** it with `-p`.

### 0.4 Volumes: data that outlives containers

```sh
$ docker volume create demo
$ docker run --rm -v demo:/data alpine sh -c 'echo hello > /data/msg'
$ docker run --rm -v demo:/data alpine cat /data/msg      # → hello (a NEW container read it)
$ docker volume rm demo
```

💡 A container's own filesystem disappears when the container is removed. **Named volumes** live separately and can be attached to any container. Kafka's data and Flink's checkpoints use volumes in this project. A **bind mount** (`./sql:/opt/sql`) maps a host folder into the container instead, which is handy for editing files on your Mac.

### 0.5 Read the Compose file

Open [`docker-compose.yml`](docker-compose.yml). Compose describes many containers ("services") in one file, and `docker compose up` starts them all. For each service, find:

- `image:` or `build:`: use a published image, or build one from a local `Dockerfile`
- `ports:`: what's reachable from your Mac (`localhost:8080`, `:8081`, `:9092`)
- `volumes:`: what persists
- `depends_on:` + `healthcheck:`: start order (the producer waits until Kafka is *healthy*, not just started)
- `environment:`: configuration passed into the container

💡 **Service names are hostnames.** Compose puts all services on one private network with built-in DNS, so the producer reaches Kafka at `kafka:29092` and the TaskManager reaches `jobmanager`. Your Mac isn't on that network, so it uses `localhost` and the published ports.

🧠 **Check yourself**
1. What's the difference between an image and a container?
2. Why does `localhost:8888` reach nginx only when you pass `-p 8888:80`?
3. Which data in this project survives `docker compose down`? Which survives `down -v`?

---

## Lab 1 — Kafka by hand

**Goal:** understand Kafka before any code touches it.

### 1.1 Start only Kafka

```sh
$ docker compose up -d kafka
$ docker compose ps                   # wait until kafka shows (healthy)
$ docker compose logs kafka | tail    # look for "Kafka Server started"
```

A small helper so the commands stay short (run it in each new terminal):

```sh
$ k() { docker compose exec kafka /opt/kafka/bin/"$@" --bootstrap-server localhost:9092; }
```

💡 **Kafka in one paragraph.** Kafka stores **topics**. A topic is split into **partitions**, and each partition is an append-only, ordered log. Every message gets an **offset** (its position in that partition). Messages aren't deleted when read: they stay until retention expires (24 h here), so any number of consumers can read and re-read them.

### 1.2 Create a topic and look at it

```sh
$ k kafka-topics.sh --create --topic lab.hello --partitions 3
$ k kafka-topics.sh --describe --topic lab.hello
```

✅ Three partitions, each with `Leader: 1` (our only broker) and `Replicas: 1`.

### 1.3 Produce messages with keys

Open **terminal A** and start a console producer that reads `key:value` lines:

```sh
$ docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic lab.hello \
    --property parse.key=true --property key.separator=:
> enwiki:edit 1
> dewiki:edit 2
> enwiki:edit 3
> frwiki:edit 4
> enwiki:edit 5
```

### 1.4 Consume them

In **terminal B**:

```sh
$ docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic lab.hello --from-beginning \
    --property print.key=true --property print.partition=true --property print.offset=true
```

✅ All `enwiki` messages share **one partition** and appear in order (offsets increasing). Different keys may land on different partitions.

💡 **Keys → partitions.** The partition is `hash(key) % partitions`. Same key means same partition, which means order is guaranteed *per key*. There's no global ordering across partitions. This is the most important Kafka design decision you make.

### 1.5 Consumer groups & offsets

Stop the consumer (Ctrl-C). Now start **two** consumers in the **same group**, one in terminal B and one in terminal C:

```sh
$ docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic lab.hello --group lab-group \
    --property print.partition=true
```

Type more messages in terminal A with different keys and watch which consumer gets them. Then:

```sh
$ k kafka-consumer-groups.sh --describe --group lab-group
```

✅ Each partition is assigned to exactly one consumer in the group, and you see `CURRENT-OFFSET`, `LOG-END-OFFSET` and `LAG`.

💡 A **consumer group** shares the work: partitions are split among its members. Different groups each get *all* messages independently. The group **commits offsets** so that after a restart it continues where it left off. **Lag** = how far behind the consumers are.

🧠 **Check yourself**
1. You have 3 partitions and start 4 consumers in one group. What does the 4th do?
2. Why is ordering guaranteed per key but not across the topic?
3. What happens to messages after they're consumed?

🏋️ **Challenge:** restart a consumer in `lab-group` *without* `--from-beginning`. Does it re-read old messages? Why or why not?

---

## Lab 2 — Stream real data into Kafka

**Goal:** connect a real event source and see what production-like data looks like.

### 2.1 Look at the raw source first

```sh
$ curl -s -H 'User-Agent: wiki-stream-lab-tutorial (https://github.com/xiaotied/wiki-stream-lab)' \
    https://stream.wikimedia.org/v2/stream/recentchange | head -12
```

💡 This is **Server-Sent Events (SSE)**: a never-ending HTTP response where each event is `id: …` + `data: {json}`. Every edit on every Wikimedia site shows up here, roughly 20–50 per second. Wikimedia asks every client to send a descriptive **User-Agent**.

### 2.2 Read the producer

Open [`producer/producer.py`](producer/producer.py) and find:

- `stream_events()`: reads SSE lines and remembers the last `id` so it can **resume** after a disconnect (`Last-Event-ID`).
- `trim()`: flattens the big upstream event into ~12 fields (the **data contract**, see [TECHNICAL_DESIGN §4.2](docs/TECHNICAL_DESIGN.md#42-record-produced-to-kafka)).
- `producer.produce(..., key=event["wiki"])`: the key choice from Lab 1, now for real.
- Producer config: `acks=all` (wait for durable write), `linger.ms=50` (wait 50 ms to batch messages), `compression.type=lz4`.

Then open [`producer/Dockerfile`](producer/Dockerfile): base image → install deps → copy code → default command. Docker builds it layer by layer and caches each layer. Because `requirements.txt` is copied *before* the code, editing `producer.py` doesn't re-run `pip install`.

### 2.3 Build and run it

```sh
$ docker compose up -d --build producer kafka-ui
$ docker compose logs -f producer
```

✅ Every 10 s: `NNN events in last 10s (xx.x/s)`.

Now open **Kafka UI → http://localhost:8080** → Topics → `wiki.recentchange`:

- **Messages** tab: real edits, with the key = wiki name.
- **Overview**: message counts per partition.

✅ The partitions are **uneven**. One partition holds `wikidatawiki` (mostly bot edits), which dominates.

💡 **Key skew** is a real-world problem: a "hot key" overloads one partition and therefore one consumer. We keep it on purpose so you can see it. Fixes include choosing a different key, salting keys, or accepting it.

### 2.4 Two listeners: why Kafka in Docker is tricky

Look at `KAFKA_LISTENERS` / `KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml`.

💡 A Kafka client connects to a bootstrap address, then the broker says "from now on talk to me at *X*". *X* must be reachable from where the client runs. Containers need `kafka:29092`, and your Mac needs `localhost:9092`, so the broker advertises both on separate listeners. If you get this wrong, clients connect and then hang.

🧠 **Check yourself**
1. Why is the producer configured with `kafka:29092` and not `localhost:9092`?
2. What do you gain and lose by keying on `wiki`?
3. If the producer container restarts, are any edits lost? (Hint: where is `Last-Event-ID` stored?)

🏋️ **Challenge:** change `linger.ms` to `0`, rebuild (`docker compose up -d --build producer`), and compare throughput and CPU with `docker stats`.

---

## Lab 3 — First steps with Flink SQL

**Goal:** query an infinite stream with SQL and understand what Flink is doing.

### 3.1 Start Flink

```sh
$ docker compose up -d --build jobmanager taskmanager
```

Open **Flink UI → http://localhost:8081**. You should see 1 TaskManager and 4 available slots.

💡 **Flink architecture.** The **JobManager** plans jobs and coordinates checkpoints. **TaskManagers** do the work in **task slots**. A query becomes a **job graph** of operators (source → filter → aggregate → sink), and each operator runs as N parallel **subtasks** (parallelism, 2 by default here).

Open [`flink/Dockerfile`](flink/Dockerfile). It extends the official image with the Kafka connector JAR. Flink connectors are plugins, and the connector version must match the Flink version.

### 3.2 Open the SQL client

```sh
$ docker compose exec jobmanager ./bin/sql-client.sh
```

```sql
Flink SQL> SET 'sql-client.execution.result-mode' = 'tableau';
```

### 3.3 Declare the Kafka topic as a table

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

💡 `CREATE TABLE` stores **no data**. It's metadata that says "this Kafka topic, read as JSON, looks like this table". Ignore `ts` and `WATERMARK` for now, because Lab 4 is all about them.

💡 Tables in the SQL client only live for the session. Once you've saved the DDL to `sql/01_source.sql`, you can start the client with it preloaded: `./bin/sql-client.sh -i /opt/sql/01_source.sql`.

### 3.4 Run queries that never end

```sql
Flink SQL> SELECT wiki, title, `user`, bot FROM recentchange;
```

✅ Rows keep appearing. Press `Ctrl-C` to stop the query. Now try a filter and a projection:

```sql
Flink SQL> SELECT title, `user`, length_new - length_old AS delta
           FROM recentchange
           WHERE wiki = 'enwiki' AND `type` = 'edit' AND NOT bot;
```

And an aggregation without windows:

```sql
Flink SQL> SELECT wiki, COUNT(*) AS edits FROM recentchange GROUP BY wiki;
```

✅ In tableau mode you see rows marked `+I` (insert), then `-U`/`+U` (update before/after) as counts change.

💡 **Dynamic tables.** Flink treats a stream as a table that keeps growing, and a query over it as a **continuously updating result**. A filter only ever *appends* rows. A `GROUP BY` without a window has to *update* earlier results, which makes it a **changelog**. That distinction matters again in Lab 5.

While a query runs, open the Flink UI → Running Jobs → click the job. You'll see the job graph, records per second per operator, and backpressure.

🧠 **Check yourself**
1. Where is the data of table `recentchange` stored?
2. Why does `GROUP BY wiki` emit `-U/+U` rows, while the `WHERE` query only emits `+I`?
3. That unbounded `GROUP BY` keeps one counter per wiki forever. What would happen with `GROUP BY title`?

---

## Lab 4 — Time and windows

**Goal:** the core idea of stream processing, which is computing over time correctly.

### 4.1 Event time vs processing time

💡 **Processing time** = when Flink sees an event. **Event time** = when it actually happened (`meta.dt` from Wikipedia, our `ts` column). If the producer stalls for a minute and then catches up, processing-time windows would put a minute's worth of edits into the wrong bucket. Event-time windows stay correct, but Flink then needs a way to know when a window is complete.

💡 **Watermarks** answer that question. `WATERMARK FOR ts AS ts - INTERVAL '5' SECOND` means: "after seeing an event at time T, assume nothing older than T − 5 s will still arrive." When the watermark passes the end of a window, the window **closes and emits**. Events that arrive later than that are **late** and get dropped by windowed aggregations. It's a trade-off between latency (small delay) and completeness (large delay).

### 4.2 Tumbling window: edits per wiki per minute

```sql
Flink SQL> SELECT window_start, window_end, wiki, COUNT(*) AS edits
           FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
           GROUP BY window_start, window_end, wiki;
```

✅ Nothing appears for up to about a minute. Then a burst of rows for one window arrives, all `+I`, and no updates.

💡 A **tumbling window** is fixed-size and non-overlapping. Because results are only emitted once the window is complete, the output is **append-only**, unlike the unbounded `GROUP BY` in Lab 3.

### 4.3 See the watermark

With the query running: Flink UI → the job → click the source operator → **Watermarks** tab. Compare the watermark with the current time (UTC).

### 4.4 Bot vs human ratio

```sql
Flink SQL> SELECT window_start,
                  COUNT(*)                                     AS total,
                  SUM(CASE WHEN bot THEN 1 ELSE 0 END)         AS bot_edits,
                  ROUND(AVG(CASE WHEN bot THEN 1.0 ELSE 0 END), 2) AS bot_ratio
           FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
           GROUP BY window_start, window_end;
```

🔧 **If windows never emit:** a source partition with no data holds back the watermark for the whole job. Fix it with `SET 'table.exec.source.idle-timeout' = '10s';` and re-run.

🧠 **Check yourself**
1. With a 5 s watermark delay, when exactly does the 12:00–12:01 window emit?
2. What happens to an edit stamped 12:00:30 that arrives after that window emitted?
3. Why are tumbling-window results append-only?

🏋️ **Challenge:** write a query for the five wikis with the largest **total bytes added** (`length_new - length_old`) per 5-minute window.

---

## Lab 5 — State, Top-N, and writing back to Kafka

**Goal:** build stateful queries and publish results as new Kafka topics, the way real pipelines chain together.

### 5.1 Sliding window + Window Top-N

Top 5 most-edited pages over the last 10 minutes, refreshed every minute:

```sql
Flink SQL> SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY edits DESC) AS rn
  FROM (
    SELECT window_start, window_end, wiki, title, COUNT(*) AS edits
    FROM TABLE(HOP(TABLE recentchange, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '10' MINUTE))
    WHERE `type` = 'edit'
    GROUP BY window_start, window_end, wiki, title
  )
) WHERE rn <= 5;
```

💡 A **hop (sliding) window** is 10 minutes long and starts every minute, so windows overlap and each event belongs to 10 of them. Flink keeps per-window **state** (a count per page) until each window closes. Because the ranking is partitioned by the window, it's computed once per window, so this is still **append-only**.

### 5.2 Write it to a Kafka topic

Create a **sink table** (again, `CREATE TABLE` is just metadata):

```sql
Flink SQL> CREATE TABLE top_pages_10m (
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  wiki STRING, title STRING, edits BIGINT, rn BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'wiki.top_pages_10m',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json'
);
```

Then submit the query as an `INSERT INTO`:

```sql
Flink SQL> INSERT INTO top_pages_10m
SELECT window_start, window_end, wiki, title, edits, rn FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY edits DESC) AS rn
  FROM (
    SELECT window_start, window_end, wiki, title, COUNT(*) AS edits
    FROM TABLE(HOP(TABLE recentchange, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '10' MINUTE))
    WHERE `type` = 'edit'
    GROUP BY window_start, window_end, wiki, title
  )
) WHERE rn <= 5;
```

✅ The client prints a **Job ID** and returns immediately. The job now runs *in the cluster*, not in your terminal (check the Flink UI). Look at `wiki.top_pages_10m` in Kafka UI.

💡 This is how pipelines compose: Flink's output topic can be the next system's input (a dashboard, another Flink job, a database sink).

### 5.3 A changelog: all-time Top-N with upsert-kafka

Remove the window, and the ranking can change with every event:

```sql
Flink SQL> CREATE TABLE top_pages_live (
  rn BIGINT, wiki STRING, title STRING, edits BIGINT,
  PRIMARY KEY (rn) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'wiki.top_pages_live',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

Flink SQL> INSERT INTO top_pages_live
SELECT rn, wiki, title, edits FROM (
  SELECT *, ROW_NUMBER() OVER (ORDER BY edits DESC) AS rn
  FROM (SELECT wiki, title, COUNT(*) AS edits FROM recentchange WHERE `type` = 'edit' GROUP BY wiki, title)
) WHERE rn <= 10;
```

Watch the topic with keys:

```sh
$ docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic wiki.top_pages_live --property print.key=true
```

✅ The same keys (`{"rn":1}`, `{"rn":2}`, …) appear again and again with new values.

💡 **Append vs changelog.** A plain `kafka` sink can only append, so it rejects queries that update earlier results. **`upsert-kafka`** writes each change as "latest value for this key". A consumer (or Kafka log compaction) keeps only the newest value per key, which is how you materialize a live table in Kafka.

### 5.4 Spike detector (alerts)

Write this one yourself: emit `(window_start, wiki, title, edits)` when a page gets **more than 5 edits in one minute**, into a `kafka` sink table `edit_spikes` on topic `wiki.edit_spikes`. Hint: a `TUMBLE` aggregation + `HAVING COUNT(*) > 5`.

🧠 **Check yourself**
1. Why did the plain `kafka` connector work for 5.2 but would fail for 5.3?
2. What state does the 5.3 query keep, and does it ever shrink?
3. Why did `INSERT INTO` return immediately while `SELECT` kept your terminal busy?

🏋️ **Challenge:** stop the 5.3 job, then read `wiki.top_pages_live` from the beginning. How would you get the *current* top 10 from that topic alone?

---

## Lab 6 — Break it on purpose

**Goal:** learn how streaming systems fail and recover. Keep the 5.2 `INSERT` job running and the Flink UI open.

### 6.1 Crash without checkpoints

```sh
$ docker compose kill taskmanager
```

✅ The job goes to **FAILED**. With checkpointing off, Flink's default is to not restart, and all window state is gone.

```sh
$ docker compose start taskmanager
```

### 6.2 Crash with checkpoints

In the SQL client, enable checkpointing, then resubmit the 5.2 `INSERT`:

```sql
Flink SQL> SET 'execution.checkpointing.interval' = '10s';
```

Wait for a few completed checkpoints (job → **Checkpoints** tab), then kill the TaskManager again and start it back up.

✅ The job goes **RESTARTING → RUNNING** by itself (it can take up to ~1 min) and continues from the last checkpoint, with its window state and Kafka offsets restored.

💡 A **checkpoint** is a consistent snapshot of all operator state plus the source offsets, written to `state.checkpoints.dir` (our `flink-checkpoints` volume). On failure Flink rewinds to it: it resets state and re-reads Kafka from the saved offsets. This is why Kafka's replayability matters.

### 6.3 Producer outage → watermark stall

```sh
$ docker compose stop producer      # wait ~2 minutes
$ docker compose start producer
```

✅ While the producer is stopped, no new windows emit (the watermark can't advance without new events). After the restart, windows emit again. But because `Last-Event-ID` is only kept in memory, the edits made during the outage were never fetched, so there's a **gap**. Real systems persist their source position for exactly this reason.

### 6.4 Late data and parallelism (optional)

- Recreate the source table with `WATERMARK FOR ts AS ts` (zero delay) and compare the J1 counts. Some out-of-order edits are now dropped as late.
- `SET 'parallelism.default' = '4';` with only 3 partitions: one source subtask has no partition, so it's idle and blocks the watermark unless `table.exec.source.idle-timeout` is set.

### 6.5 Restart the broker

```sh
$ docker compose restart kafka
```

✅ The producer and Flink reconnect, and the data is still there thanks to the `kafka-data` volume.

🧠 **Check yourself**
1. What exactly is inside a Flink checkpoint?
2. Why does recovery depend on Kafka keeping old messages?
3. After 6.2's recovery, could some results be written to Kafka twice? (Look up "exactly-once sink" and `sink.delivery-guarantee`.)

---

## Lab 7 — Clean up & next steps

```sh
$ docker compose down        # stop and remove containers; volumes kept
$ docker compose down -v     # also delete Kafka data and checkpoints (fresh start)
$ docker system df           # how much disk Docker uses
```

**Save your work:** put the DDL and queries you wrote into `sql/01_source.sql`, `sql/02_windows.sql`, etc., so the next session starts with `./bin/sql-client.sh -i /opt/sql/01_source.sql`.

**Where to go next**
- Persist `Last-Event-ID` so producer restarts leave no gap
- Deduplicate by `(wiki, id)` in Flink
- Avro + Schema Registry for a typed, evolvable contract
- A small live dashboard reading `wiki.top_pages_live`
- Rewrite one job in the Java **DataStream API** to see what SQL hides
- Multi-broker Kafka with replication factor 3, then kill a broker

---

## Glossary

| Term | Meaning |
|---|---|
| **Image / container** | Template / running instance |
| **Volume / bind mount** | Docker-managed persistent storage / a host folder mapped into a container |
| **Topic / partition / offset** | Named stream / ordered shard of it / position within a shard |
| **Key** | Determines the partition, so ordering is per key |
| **Consumer group** | Consumers sharing a topic's partitions, with committed offsets |
| **Listener / advertised listener** | Address Kafka binds to / address it tells clients to use |
| **Dynamic table** | Flink's view of a stream as a continuously changing table |
| **Event time / processing time** | When it happened / when it's processed |
| **Watermark** | "No events older than this are expected anymore" |
| **Tumbling / hop window** | Fixed non-overlapping / fixed overlapping time buckets |
| **Append / changelog stream** | Only inserts / inserts, updates and deletes |
| **Upsert** | Write "latest value for key" |
| **Checkpoint** | Consistent snapshot of state and offsets, used for recovery |

## Further reading

- Kafka: [Introduction](https://kafka.apache.org/intro) · [Design](https://kafka.apache.org/documentation/#design)
- Flink: [Timely stream processing](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/time/) · [Window TVFs](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/window-tvf/) · [Dynamic tables](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/concepts/dynamic_tables/) · [Checkpointing](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/checkpointing/)
- Docker: [Get started](https://docs.docker.com/get-started/) · [Compose file reference](https://docs.docker.com/reference/compose-file/)
- Wikimedia: [EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service)
- Background: Tyler Akidau, *Streaming 101 / 102* (O'Reilly Radar), the best intro to event time and watermarks
