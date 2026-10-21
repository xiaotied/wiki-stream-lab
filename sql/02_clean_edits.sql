-- Job clean_edits: wiki.recentchange -> wiki.edits.clean (contract: docs/TECHNICAL_DESIGN.md §4.3)
-- Submit with:
--   docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/01_source.sql -f /opt/sql/02_clean_edits.sql

-- Checkpoints commit Kafka offsets, so consumer lag is visible.
-- (Comments go on their own line: the SQL client only ends a statement at a ';' that ends the line.)
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
