-- Read-side table over the clean topic, for querying. Load together with the raw table (sql-client takes one -i, so use the wrapper):
--   ./sql-shell.sh                                   (loads 01_source.sql + 03_clean_source.sql)
-- Reads from the start of the topic (24 h retention). Its own group id keeps it separate from the clean_edits job.

CREATE TABLE edits_clean_read (
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
  'properties.group.id' = 'adhoc-query',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.ignore-parse-errors' = 'true'
);
