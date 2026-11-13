#!/usr/bin/env bash
set -euo pipefail

PG="${1:-18}"
MODE="${2:-full}"
DIR="$(cd "$(dirname "$0")" && pwd)"
SPICEDB_SRC="${SPICEDB_SRC:-$HOME/code/oss/spicedb}"
NAME="headrev-bench-pg$PG"
PORT="54$PG"
OUT="$DIR/out"
mkdir -p "$OUT"
LOG="$OUT/pg$PG-$MODE-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1

echo "log: $LOG"
read -r CPUS MEM < <(docker info --format '{{.NCPU}} {{.MemTotal}}')
echo "docker VM: $CPUS CPUs, $((MEM / 1024 / 1024 / 1024)) GiB"
if [ "$MODE" = full ] && { [ "$CPUS" -lt 6 ] || [ "$MEM" -lt $((10 * 1024 * 1024 * 1024)) ]; }; then
  echo "need at least 6 CPUs and 10 GiB in the docker VM for the full run"
  exit 1
fi

echo "building load tool and spicedb ..."
GOARCH="$(docker info --format "{{.Architecture}}" | sed -e s/aarch64/arm64/ -e s/x86_64/amd64/)"
(cd "$DIR" && go mod tidy >/dev/null && GOOS=linux GOARCH="$GOARCH" CGO_ENABLED=0 go build -o "$OUT/headrevbench" .)
(cd "$SPICEDB_SRC" && go build -o "$OUT/spicedb" ./cmd/spicedb)
echo "spicedb at $(cd "$SPICEDB_SRC" && git rev-parse --short HEAD) ($(cd "$SPICEDB_SRC" && git branch --show-current))"

echo "starting postgres $PG as $NAME on 127.0.0.1:$PORT ..."
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -e POSTGRES_PASSWORD=bench -p "127.0.0.1:$PORT:5432" --shm-size=1g \
  "mirror.gcr.io/library/postgres:$PG" \
  -c max_connections=1200 -c shared_buffers=1GB \
  -c shared_preload_libraries=pg_stat_statements -c pg_stat_statements.track=top >/dev/null
for _ in $(seq 1 90); do
  if docker exec "$NAME" psql -U postgres -h 127.0.0.1 -Atc 'select 1' >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$NAME" psql -U postgres -h 127.0.0.1 -Atc "select version()"

docker exec "$NAME" psql -U postgres -q -c 'CREATE DATABASE spicedb' -c 'CREATE DATABASE noise'
docker exec "$NAME" psql -U postgres -q -d noise -c 'CREATE TABLE noise_t (id bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now())'
docker exec "$NAME" psql -U postgres -q -d spicedb -c 'CREATE EXTENSION pg_stat_statements'
"$OUT/spicedb" datastore migrate head --datastore-engine postgres \
  --datastore-conn-uri "postgres://postgres:bench@127.0.0.1:$PORT/spicedb?sslmode=disable" --log-level warn

docker cp "$OUT/headrevbench" "$NAME:/headrevbench" >/dev/null
TTY=-i
[ -t 0 ] && TTY=-it
docker exec $TTY "$NAME" /headrevbench -pg "$PG" -mode "$MODE" "${@:3}"

echo "container $NAME left running for inspection; remove with: docker rm -f $NAME"
