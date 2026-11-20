# Phase 4: Resource-aware worker scoring

## What was built

- `include/chronos/domain/worker_score.hpp` reports a worker's score and
  eligibility.
- `include/chronos/scheduler/worker_selector.hpp` defines the four scheduling
  strategies and ranking/selection API.
- `src/worker_selector.cpp` filters unhealthy or resource-insufficient workers,
  scores eligible candidates, and sorts the results.
- `tests/worker_selector_test.cpp` verifies strategy trade-offs, GPU capacity,
  overcommit rejection, and worker health.

The least-loaded strategy scores the average free share across nonzero
CPU/memory/GPU capacities. Resource-aware scoring uses the dominant requested
resource fraction plus the average requested fraction, favoring a tight fit
that preserves larger workers. Priority-based scoring interpolates between
best-fit and least-loaded using a priority clamped to 0-100. Deadline-aware
scoring uses the supplied expected start delay and runtime, with extra penalty
for predicted deadline misses.

Filtering, scoring, and sorting cost O(w log w) for w workers; capacity checks
and each score calculation are O(1). Unknown or unavailable capacity is not
silently treated as eligible; selection throws if no healthy candidate fits.

## Concurrency

The scorer is a pure function over a snapshot of jobs and workers; it owns no
locks. A caller that shares mutable worker accounting must snapshot or protect
it while invoking the scorer, and apply a selected reservation atomically.

## Verification

All 17 GoogleTests and the demo passed with the native Windows MSYS2 GCC 16.2.0
build. The matching Ubuntu 24.04/GCC 13.3.0 build was also run.
