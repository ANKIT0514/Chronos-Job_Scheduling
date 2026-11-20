# Phase 5: Heartbeats, failure detection, and retries

## What was built

- `include/chronos/worker/worker_manager.hpp` and
  `src/worker_manager.cpp` register workers, refresh monotonic heartbeats,
  reserve/release resources, detect timeouts, and return lost jobs for
  reassignment.
- `include/chronos/scheduler/retry_policy.hpp` and `src/retry_policy.cpp`
  enforce a total-attempt limit and capped exponential backoff.
- `src/in_memory_scheduler.cpp` schedules delayed retries on a dedicated timer
  thread instead of sleeping inside execution workers. Exhausted jobs enter
  `DEAD_LETTER`.
- `tests/worker_manager_test.cpp`, `tests/retry_policy_test.cpp`, and the
  scheduler recovery tests cover timeouts, reassignment, reservation safety,
  backoff, recovery, and dead-letter behavior.

Worker state and assignment maps are protected by one manager mutex, so
capacity reservations and heartbeat expiration are atomic with respect to one
another. The retry delay queue is an ordered map: inserting a retry costs
O(log r), and the timer waits for the earliest due entry using a condition
variable. Worker timeout scans cost O(w + a), where w is workers and a is
their in-flight assignments.

## Failure semantics and limitations

Heartbeat expiration uses `steady_clock` so wall-clock corrections do not
cause false timeouts. The manager returns the full lost-job descriptions and
releases stale reservations; the caller must keep task payloads and resubmit
those jobs to another healthy worker. The local scheduler retries thrown task
failures, but it cannot forcibly terminate arbitrary C++ callbacks. Shutdown
promotes retries already waiting in the delay queue so they can drain, while
new failures observed during shutdown go directly to the dead-letter state.

This phase is process-local: it does not persist leases or send heartbeat
events through Redis/Kafka. Those integrations remain in the Roadmap.

## Verification

All 27 GoogleTests passed with MSYS2 GCC 16.2.0 on Windows. The same 17-test
core suite passed earlier under Ubuntu 24.04/GCC 13.3.0; the retry and
heartbeat additions were then validated on the native Windows build.
