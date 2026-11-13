# HeadRevision cost on the Postgres datastore

Measures what `HeadRevision` costs on the Postgres datastore in three forms:

- `current`: `pg_current_snapshot()`, what SpiceDB runs today;
- `bounded`: the transaction-table query of [authzed/spicedb#3488](https://github.com/authzed/spicedb/pull/3488), probing the head row, the snapshot's in-progress list and the rows past its `xmax`;
- `range`: the naive transaction-table form, a range scan from the snapshot's `xmin`.

The queries are copied verbatim into `queries.go` so that the harness has no dependency on SpiceDB at runtime; the SpiceDB binary is only used to migrate the schema.

## Running

```
./run.sh 18          # full plan on Postgres 18, ~35 minutes
./run.sh 15 smoke    # three short points
./run.sh 18 full -only xip=400
```

Requirements: Docker with at least 6 CPUs and 10 GiB for the full plan, Go, and a SpiceDB checkout at `$SPICEDB_SRC` (default `~/code/oss/spicedb`) that contains the migrations. Progress is printed to the terminal and appended to `out/pg<version>-<mode>-<timestamp>.log`; each measured point is also written there as a `RESULT` JSON line. The Postgres container is left running after the run.

## What is measured

The harness runs inside the Postgres container and talks to it over the unix socket. Every point seeds the transaction table to the requested size, starts the background load, then runs each query form for 3 s of warm-up and 20 s of measurement from the requested number of callers. Per form it reports:

- server-side mean time per call and its standard deviation, from `pg_stat_statements`;
- buffers and heap fetches per call, the median of 15 `EXPLAIN (ANALYZE, BUFFERS)` runs;
- client-side p50, p99, p99.9 and throughput.

Background load, held constant within a point: idle connections, foreign write transactions kept open for the whole point, SpiceDB-style journal writes at a fixed rate, foreign commits in a second database at a fixed rate, and optionally a long foreign transaction opened before a batch of journal rows is written.

## Plan

The base point is 100 000 journal rows, 10 open foreign transactions, 50 idle connections, 8 callers, 10 SpiceDB writes/s and 650 foreign commits/s. Each section varies one axis from the base:

| section | values |
|---|---|
| `journal` (rows) | 1 000, 1 000 000, 10 000 000 |
| `xip` (open foreign write transactions) | 0, 10, 50, 100, 200, 400 |
| `idle` (connections) | 10, 200, 800 |
| `callers` | 1, 8, 32, 64 |
| `spicedb-writes` (per second) | 0, 10, 100, 1 000 |
| `foreign-commits` (per second) | 0, 650, 5 000 |
| `rows-under-open-txs` | 1 000, 10 000, 100 000 |

## Results

Server-side time and buffers per call for the `bounded` form, Postgres 18 and 15 on an Apple M3 Pro (Docker VM with 8 CPUs):

| open foreign write transactions | `current` | `bounded`, PG 18 | `bounded`, PG 15 |
|---|---|---|---|
| 0 | 2 µs | 9 µs / 7 buffers | 6 µs / 5 buffers |
| 10 | 2 µs | 9 µs / 9 | 11 µs / 25 |
| 50 | 4 µs | 12 µs / 9 | 47 µs / 105 |
| 100 | 6 µs | 14 µs / 9 | 150 µs / 206 |
| 200 | 12 µs | 19 µs / 9 | 404 µs / 405 |
| 400 | 23 µs | 35 µs / 9 | 962 µs / 805 |

Postgres 17 and later read the in-progress entries in one index pass; 15 and 16 do one index descent per entry, about 2.5 µs each.

The `bounded` cost does not change with idle connections, callers, foreign commit rate, rows under a long foreign transaction or journal size. At 1 000 SpiceDB writes/s the past-`xmax` probe starts to find concurrent writes: 13 µs / 23 buffers on PG 18, 17 µs / 67 on PG 15.

The `range` form grows with the rows under a long foreign transaction: 14 ms per call at 100 000 rows on both versions, and 16 ms at 1 000 SpiceDB writes/s.

The summary table of every run is printed at the end of its log.
