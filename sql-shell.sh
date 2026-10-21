#!/usr/bin/env bash
# Open the Flink SQL client with one or more files from ./sql preloaded.
# sql-client.sh accepts a single -i file, so the files are concatenated inside the container.
#   ./sql-shell.sh                          # raw + clean read tables (default)
#   ./sql-shell.sh 01_source.sql            # just the raw table
#   ./sql-shell.sh 01_source.sql -f /opt/sql/02_clean_edits.sql   # extra args go to sql-client.sh
set -euo pipefail
cd "$(dirname "$0")"

files=()
while [[ $# -gt 0 && "$1" == *.sql ]]; do files+=("/opt/sql/$1"); shift; done
[[ ${#files[@]} -eq 0 ]] && files=(/opt/sql/01_source.sql /opt/sql/03_clean_source.sql)

docker compose exec jobmanager bash -c \
  'cat "$@" > /tmp/init.sql' _ "${files[@]}"
exec docker compose exec jobmanager ./bin/sql-client.sh -i /tmp/init.sql "$@"
