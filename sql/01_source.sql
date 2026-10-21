-- Source table over the raw topic. Load with:
--   docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/01_source.sql

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
  -- '2026-10-21T02:11:17.687Z' -> TIMESTAMP(3); also handles values without millis
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
