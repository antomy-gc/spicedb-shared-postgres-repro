#!/usr/bin/env bash
# SpiceDB write load on a quiet Postgres instance, stable-head-revision off vs on.
set -euo pipefail
cd "$(dirname "$0")"

export SPICEDB_IMAGE=${SPICEDB_IMAGE:?set to a SpiceDB image built from PR #3488}
ZIPF=${ZIPF:-1.5}
DURATION=${DURATION:-60s}
RATES=${RATES:-0 50 200 500}
COMPOSE="docker compose -f docker-compose.yaml -f docker-compose.pr3488.yaml"

(cd loadgen && go build -o loadgen .)
for flag in false true; do
  export STABLE_HEAD_REVISION=$flag
  $COMPOSE down -v >/dev/null 2>&1 || true
  $COMPOSE up -d --wait
  ./loadgen/loadgen -seed
  for rate in $RATES; do
    interval=0
    [ "$rate" -gt 0 ] && interval="$((1000000 / rate))us"
    echo "### stable-head-revision=$flag, spicedb writes=$rate/s, consistency=fully_consistent"
    ./loadgen/loadgen -mode fully_consistent -zipf "$ZIPF" -warmup 15s -duration "$DURATION" -write-interval "$interval"
  done
done
$COMPOSE down -v >/dev/null
