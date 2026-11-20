# Phase 2: Core domain and priority queue

## What was built

- `include/chronos/domain/job.hpp` defines a job's identity, priority, optional
  deadline, and CPU/memory/GPU request.
- `include/chronos/domain/worker.hpp` defines worker identity, resource
  capacity/allocation, and health.
- `include/chronos/domain/worker_score.hpp` carries a worker identifier and
  numeric scheduling score.
- `include/chronos/scheduler/job_priority_queue.hpp` declares the queue API.
- `src/job_priority_queue.cpp` orders jobs by descending priority, then jobs
  with earlier deadlines, then FIFO sequence for stable ties.
- `tests/job_priority_queue_test.cpp` checks ordering, stable ties, deadline
  ordering, size/top, and empty-queue errors with GoogleTest.

The queue uses `std::priority_queue`, so insertion and removal are
O(log n), while inspecting the top, emptiness, and size are O(1). Queue
operations are not internally synchronized; the scheduler must protect shared
access with its mutex.

## Concurrency and failure handling

The queue is deliberately a single-owner data structure. The next phase places
all queue accesses under the scheduler's mutex; adding locks here as well would
create redundant lock ownership and make lock ordering harder to reason about.
Empty `pop`/`top` operations throw `std::out_of_range` instead of returning a
plausible but invalid job.

## Verification

The C++17 build and all five GoogleTests passed using GCC 13.3.0 in an Ubuntu
24.04 Docker container. The host's preinstalled GCC 6.3.0 is a legacy
MinGW.org build without standard-library threading support; it is not used as
the project test compiler.
