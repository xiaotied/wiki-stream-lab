# wiki-stream-lab

Wikipedia edits (Wikimedia EventStreams) → Kafka → Flink SQL, all in Docker.

| Service     | URL / address          |
|-------------|------------------------|
| Kafka       | `localhost:9092` (host), `kafka:29092` (containers) |
| Kafka UI    | http://localhost:8080  |
| Flink UI    | http://localhost:8081  |

## Run

```sh
docker compose up -d --build          # build images, start everything
docker compose ps                     # status + health
docker compose logs -f producer       # watch events/sec
```

## Peek at Kafka

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic wiki.recentchange
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wiki.recentchange --max-messages 5
```

## Flink SQL client

```sh
docker compose exec jobmanager ./bin/sql-client.sh
# inside: SOURCE '/opt/sql/01_source.sql';  (SQL files in ./sql are mounted at /opt/sql)
```

## Tear down

```sh
docker compose down        # stop, keep volumes (Kafka data, checkpoints)
docker compose down -v     # also delete volumes
```
