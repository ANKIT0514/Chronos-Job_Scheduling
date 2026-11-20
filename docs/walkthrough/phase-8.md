# Phase 8: PostgreSQL persistence

## What was built

- `sql/001_initial_schema.sql` creates jobs, workflows, dependencies, workers,
  attempts, idempotent transition events, and audit-log tables and indexes.
- `include/chronos/persistence/job_repository.hpp` exposes typed job
  persistence, versioned state transitions, and worker upserts.
- `src/job_repository.cpp` implements the repository with libpqxx transactions.
- `src/postgres_demo.cpp` applies the migration and demonstrates an idempotent
  insert/query.
- `tests/postgres_repository_test.cpp` verifies real database persistence,
  idempotency, invalid/stale transitions, worker upserts, and 64-bit resource
  values.

Transitions lock the job row, check the event ID, validate the expected version
and allowed state transition, and update the job/event/audit/attempt rows in a
single transaction. Duplicate event IDs with matching content are idempotent;
reusing an ID for different content is reported as an idempotency conflict.
The optimistic version check protects callers that observed stale state.

## Build and verification

PostgreSQL support is an explicit build option:

```sh
cmake -S . -B build -DCHRONOS_ENABLE_POSTGRES=ON
```

It requires `pkg-config` and the real `libpqxx` development package. Three
GoogleTests passed against a running PostgreSQL 16 container, and
`chronos_postgres_demo` applied the schema and persisted/read a job.

The repository currently opens a PostgreSQL connection per operation for
thread safety and simplicity. Connection pooling and wiring repository
transitions into the REST/Kafka event flow remain follow-up integration work.
