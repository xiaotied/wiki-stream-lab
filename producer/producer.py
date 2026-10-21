"""Read Wikimedia's recentchange SSE stream and publish trimmed events to Kafka."""

import json
import os
import time

import requests
from confluent_kafka import Producer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia requires a descriptive User-Agent
USER_AGENT = "wiki-stream-lab/0.1 (https://github.com/xiaotied/wiki-stream-lab)"

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "wiki.recentchange")

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "linger.ms": 50,            # batch small messages for throughput
    "compression.type": "lz4",
    # librdkafka defaults to CRC32 key hashing; Java clients (incl. Flink) use murmur2.
    # Match them so a key lands on the same partition number in every topic.
    "partitioner": "murmur2_random",
})


def trim(e: dict) -> dict:
    length = e.get("length") or {}
    return {
        "id": e.get("id"),
        "type": e.get("type"),            # edit | new | log | categorize
        "title": e.get("title"),
        "user": e.get("user"),
        "bot": e.get("bot"),
        "wiki": e.get("wiki"),            # e.g. enwiki, dewiki, wikidatawiki
        "server_name": e.get("server_name"),
        "namespace": e.get("namespace"),
        "minor": e.get("minor"),
        "length_old": length.get("old"),
        "length_new": length.get("new"),
        "event_time": e["meta"]["dt"],    # ISO-8601 UTC; used as Flink event time
    }


def on_delivery(err, msg):
    if err:
        print(f"delivery failed: {err}")


def stream_events(last_event_id: str | None):
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id   # resume where we left off
    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as r:
        r.raise_for_status()
        event_id = None
        for line in r.iter_lines(decode_unicode=True):
            if line.startswith("id: "):
                event_id = line[4:]
            elif line.startswith("data: "):
                yield event_id, json.loads(line[6:])


def main():
    last_id, sent, t0 = None, 0, time.time()
    while True:
        try:
            for last_id, event in stream_events(last_id):
                if event.get("meta", {}).get("domain") == "canary":
                    continue
                producer.produce(
                    TOPIC,
                    key=event.get("wiki", ""),       # same wiki -> same partition
                    value=json.dumps(trim(event)),
                    on_delivery=on_delivery,
                )
                producer.poll(0)
                sent += 1
                if time.time() - t0 >= 10:
                    print(f"{sent} events in last 10s ({sent / 10:.1f}/s)")
                    sent, t0 = 0, time.time()
        except (requests.RequestException, json.JSONDecodeError) as exc:
            print(f"stream error, reconnecting in 3s: {exc}")
            producer.flush(5)
            time.sleep(3)


if __name__ == "__main__":
    main()
