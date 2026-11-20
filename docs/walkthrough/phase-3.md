# Phase 3: In-memory scheduler and worker pool

## What was built

- `include/chronos/domain/job_state.hpp` defines the lifecycle states and the
  printable state name.
- `include/chronos/scheduler/in_memory_scheduler.hpp` exposes submit, cancel,
  status, and wait operations.
- `src/in_memory_scheduler.cpp` owns the priority queue, job records, mutex,
  condition variables, and worker threads.
- `src/job_state.cpp` maps state values to their external names.
- `tests/in_memory_scheduler_test.cpp` covers completion, queue priority,
  failure capture, pending cancellation, duplicate IDs, and input validation.
- `src/main.cpp` submits and waits for four demo jobs.

The queue and job records are protected by one mutex so a job cannot be
removed from the queue while concurrently being cancelled or transitioned.
Workers wait on a condition variable with a predicate rather than polling.
They release the mutex before calling user code, avoiding lock convoying and
preventing callbacks from deadlocking scheduler operations.

Queue insertion/removal is O(log n), job lookup is O(log m) using the ordered
map, and waiting for terminal state is O(1) scheduler work plus the duration of
the job. Shutdown stops accepting submissions and drains queued jobs before
joining all workers.

## Concurrency and failure behavior

All state transitions and cancellation checks occur under the scheduler mutex.
Condition-variable predicates tolerate spurious wakeups. A pending job is
cancelled without running; cancelling a running job requests cancellation but
cannot interrupt arbitrary user code, so its state becomes `CANCELLED` only
after the callback returns. Exceptions are converted to `FAILED` with their
message retained; unknown exceptions receive an explicit diagnostic.

## Verification

The native Windows build used MSYS2 GCC 16.2.0 and passed all 11 GoogleTests;
the same build and tests passed under Ubuntu 24.04 with GCC 13.3.0. Running the
demo completed all four submitted jobs.
