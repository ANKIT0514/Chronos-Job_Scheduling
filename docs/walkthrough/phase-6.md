# Phase 6: DAG workflows

## What was built

- `include/chronos/workflow/workflow_graph.hpp` and
  `src/workflow_graph.cpp` store jobs and prerequisite edges, return stable
  topological levels/order, and reject cycles.
- `include/chronos/workflow/workflow_executor.hpp` and
  `src/workflow_executor.cpp` submit independent jobs in a level concurrently
  and wait for every job at that level before submitting dependent work.
- `tests/workflow_graph_test.cpp` covers deterministic order, duplicate edges,
  invalid references, cycles, successful dependency order, failure handling,
  and preflight validation.

The graph uses ordered maps/sets to make results deterministic. Kahn's
topological sort is O((V + E) log V), including ordered ready-set operations.
The executor uses the existing scheduler pool, allowing jobs in the same
topological level to run concurrently while never starting a child before its
parents succeed.

## Concurrency and failure behavior

The graph itself is immutable during sorting/execution. The executor validates
all tasks and scheduler IDs before submission. It waits for every job in a
level, then stops the workflow on the first failed level; descendants are not
submitted. A scheduler shared with other submitters can still reject work
between preflight and submit, so a workflow is not a transactional boundary.

## Verification

The native Windows C++17 build passed all 33 GoogleTests and the four-job demo.
