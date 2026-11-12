#!/usr/bin/env bash
# Reproduces the effect of a shared Postgres instance on SpiceDB caches under fully_consistent.
# Requires: docker compose, go 1.24+. Runs for ~8 minutes.
set -euo pipefail
cd "$(dirname "$0")"

ZIPF=${ZIPF:-1.5}
DURATION=${DURATION:-60s}

(cd loadgen && go build -o loadgen .)
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --wait
docker compose exec -T postgres psql -U spicedb -d spicedb -c 'CREATE DATABASE noise' >/dev/null
docker compose exec -T -e PGPASSWORD=spicedb postgres pgbench -i -s 20 -q -U spicedb -h localhost noise >/dev/null 2>&1
./loadgen/loadgen -seed

run() { ./loadgen/loadgen -mode "$1" -zipf "$ZIPF" -warmup 15s -duration "$DURATION"; }

docker compose --profile noise up -d noise >/dev/null
sleep 10
for mode in fully_consistent minimize_latency at_least_as_fresh; do
	echo "### neighbouring database writing ~650 tx/s, consistency=$mode"
	run "$mode"
done

docker compose --profile noise stop noise >/dev/null
sleep 5
for mode in fully_consistent at_least_as_fresh; do
	echo "### quiet instance, consistency=$mode"
	run "$mode"
done

docker compose --profile noise down -v >/dev/null
