# fully_consistent on a shared Postgres instance

Two SpiceDB v1.56.1 nodes with cluster dispatch between them (`dns:///spicedb:50053`,
both nodes in the hashring), one Postgres 16 instance with two databases: `spicedb`
and `noise`. `pgbench` writes to `noise` at 650 transactions per second; SpiceDB
receives one relationship write every 3 seconds. The load generator sends 20
`CheckBulkPermissions` calls per second (10 items each) across both nodes, with
resources and subjects drawn from a Zipf distribution.

```
./run.sh
```

Each run prints, for the measured 60 seconds: Postgres xid growth, SpiceDB
transaction journal growth, hit ratios of the `namespace`, `dispatch` and
`cluster_dispatch` caches, dispatch counts and datastore query counts summed over
both nodes, and client-side latency.

## Why the caches stop working

`fully_consistent` resolves the revision with `pg_current_snapshot()`. Transaction
ids are shared by every database of a Postgres instance, so writes to `noise`
advance the revision ~650 times per second. The revision is part of every
dispatch cache key and of the namespace cache key, so a check at
`fully_consistent` almost never shares a key with the previous one.

`minimize_latency` and `at_least_as_fresh` use the quantized revision, derived
from SpiceDB's own transaction journal, which only moves with SpiceDB writes.

## Results (Apple M3 Pro, Docker Desktop, zipf=1.5)

| run | namespace cache | dispatch cache | cluster cache | datastore queries | p50 | p99 |
|---|---|---|---|---|---|---|
| noise, fully_consistent | 81% | 0.2% | 0.1% | 1197/s | 12.4 ms | 30.7 ms |
| noise, minimize_latency | 99% | 5.6% | 1.5% | 703/s | 9.0 ms | 21.7 ms |
| noise, at_least_as_fresh(last write) | 99% | 4.1% | 2.1% | 743/s | 9.1 ms | 22.8 ms |
| quiet, fully_consistent | 99% | 4.8% | 2.6% | 692/s | 7.7 ms | 18.0 ms |
| quiet, at_least_as_fresh(last write) | 99% | 4.9% | 2.6% | 690/s | 7.4 ms | 18.9 ms |

`at_least_as_fresh` with the ZedToken of the latest write gives the same cache
behaviour on the shared instance as `fully_consistent` on a quiet one, with the
same visibility guarantee for everything written through SpiceDB.
