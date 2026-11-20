# Phase 9: Redis worker state and leases

## What was built

- `include/chronos/persistence/redis_worker_store.hpp` defines the hiredis
  store API.
- `src/redis_worker_store.cpp` writes worker info/load hashes, expiring job
  membership sets, heartbeat TTLs, and token-owned leases.
- `tests/redis_worker_store_test.cpp` verifies heartbeat expiry, assignment
  membership, and lease ownership/refresh/release against Redis.

Worker keys use the requested `worker:{id}:info`, `worker:{id}:load`,
`worker:{id}:jobs`, and `worker:{id}:lease` forms. Lua scripts make the
heartbeat-plus-expiry, load-plus-expiry, and token-check-plus-lease mutations
atomic. Hiredis replies and connection ownership are released through RAII;
one mutex serializes access because a hiredis connection is not safe for
concurrent commands.

## Verification and limitations

The Redis option requires the real hiredis development package:
`-DCHRONOS_ENABLE_REDIS=ON`. The combined PostgreSQL/Redis-enabled build passed
all 43 tests, including Redis 7 and PostgreSQL 16 integration tests.

This store is available to services but is not yet wired into the local
`WorkerManager` heartbeat path or API rate limiter. Expiring keys are the
recovery mechanism; Redis persistence/replication settings are deployment
decisions and are not configured here.
