# Chronos architecture

## Target distributed design

```text
Clients / SDK
     |
     v
API Gateway -- API key, request validation, rate limit, Prometheus
     |
     +---- PostgreSQL: job metadata, versions, attempts, audit
     +---- Kafka: submission / cancellation events
                        |
                        v
             Scheduler Service <---- Redis worker inventory / leases
              |          |
              |          +---- Worker Manager: heartbeat and capacity
              v
       Kafka job-assignments
              |
              v
       C++ Worker Agents ---- worker-events / status updates
              |
              +---- Redis TTL state and assignment membership
              +---- Object storage (planned logs/results)
```

Kafka topics used by the event bus:

- `job-submission`
- `job-cancellation`
- `job-status-updates`
- `worker-heartbeats`
- `worker-events`
- `job-assignments`
- `job-dead-letter`

## Components and current boundaries

| Component | Responsibility | Failure mode and recovery |
|---|---|---|
| API Gateway | Authenticates API-key requests, validates bounded JSON, applies a fixed-window rate limit, accepts/cancels/queries jobs, and exports metrics. | Invalid or unauthorized requests receive explicit 4xx responses; backend errors return 503. Gateway state is not the source of truth. |
| Scheduler | Ranks eligible workers, reserves capacity, queues jobs, and emits assignments. The local scheduler uses a stable priority queue and condition-variable worker pool. | Retries task/worker failures with capped exponential backoff; attempts exhausted enter dead letter. In-memory state is lost on process restart. |
| Worker Manager | Tracks worker heartbeat deadlines, allocated resources, assigned jobs, and health. | `steady_clock` detects missed heartbeat windows; lost assignments are released and returned for reassignment. Redis heartbeats expire by TTL. |
| Job Manager / repository | Persists lifecycle state, attempts, event IDs, versions, and audit records. | PostgreSQL transactions and expected versions reject stale updates; unique event IDs make repeat deliveries idempotent. |
| Dependency Service | Topologically orders workflow DAGs and releases a level only after its parents succeed. | Cycles fail validation before execution; failed levels prevent descendants from being submitted. |
| Retry and failure service | Applies bounded attempt counts and exponential backoff; terminal exhaustion is dead-lettered. | Delayed retries do not occupy execution threads. Side-effecting tasks still need idempotent task semantics. |
| Notification service | Publishes job-status and worker events for clients/observers. | Kafka is at-least-once with consumer acknowledgement after processing; duplicate event handling is required. |
| Worker Agent | Registers/heartbeats, consumes assignments, executes tasks, and reports running/success/failure states. | Heartbeats expire and assignments can be fenced by attempt number; a worker cannot forcibly undo external side effects. |
| Redis | Low-latency worker info/load/job membership, heartbeat TTLs, and token-owned leases. | Redis loss removes ephemeral state; PostgreSQL/Kafka are needed to reconstruct durable work. |
| PostgreSQL | Durable metadata, versions, attempts, worker snapshots, and audit events. | Database outage should fail closed for writes; retrying requests requires idempotency keys. |
| Kafka | Durable event transport and consumer-group distribution. | Broker/partition failures delay work; producer acks and consumer replay improve delivery, but are not a distributed DB transaction. |
| Object storage | Planned artifact/log/result retention. | Independent availability and retention policy; not implemented. |
| Prometheus | Scrapes gateway counters; dashboards/alerts are deployment follow-ups. | Metrics can be unavailable without affecting job correctness. |

## State machine and consistency

```text
SUBMITTED -> PENDING -> SCHEDULED -> RUNNING -> SUCCEEDED
                                  |          \-> FAILED -> RETRYING -> PENDING
                                  |                        \-> DEAD_LETTER
                                  +----------------------------> CANCELLED
```

PostgreSQL state changes include an expected version and event ID in one
transaction. Re-delivery of the same event returns a duplicate result;
different event content under a reused ID is rejected. Redis leases use Lua
compare-token operations. Kafka and PostgreSQL are not enlisted in one
transaction, so a production deployment should add a transactional outbox and
reconciliation worker.

## Scheduling and complexity

The local job heap is O(log n) for push/pop and O(1) for top. The worker
selector checks each of w worker snapshots in O(w) and sorts candidates in
O(w log w). Resource feasibility rejects unhealthy, overcommitted, or
CPU/memory/GPU-insufficient workers. Strategies include least-loaded,
resource-aware best-fit, priority-weighted, and deadline-aware expected
completion.

The DAG topological level calculation uses ordered maps/sets and costs
O((V + E) log V). Jobs in a level can execute concurrently; a dependent level
starts only after all jobs in the previous level succeed.

## Concurrency and failure controls

- The in-memory scheduler protects queue and job records with one mutex.
- Workers release that mutex before invoking job code.
- Condition-variable predicates handle spurious wakeups; no busy-wait loop is
  used for work or retry timers.
- Worker heartbeat expiration uses a monotonic clock.
- Redis connection use is serialized because a hiredis connection is not safe
  for concurrent commands.
- C++ callbacks cannot be forcefully cancelled safely; cancellation of a
  running local task is cooperative and observed after the callback returns.
- Distributed side effects are at-least-once and require application-level
  idempotency.

## Alternatives considered

- **Crow vs cpp-httplib:** cpp-httplib is a small, header-only HTTP surface
  suitable for this service and tested over loopback.
- **redis-plus-plus vs hiredis:** hiredis is the official low-level client,
  avoids an extra C++ wrapper dependency, and supports atomic Lua scripts.
- **cppkafka vs librdkafka:** librdkafka is the maintained C client with
  explicit acknowledgement and delivery controls; it avoids a second wrapper.
- **SQL polling vs Kafka:** Kafka decouples submitters and schedulers, while
  PostgreSQL remains the durable metadata source; an outbox is needed to close
  the dual-write gap.
- **Single global lock vs per-worker locks:** the current small in-memory
  manager uses one lock for atomic reservations. At larger scale, sharded
  worker state or a transactional reservation service is needed.
- **Polling vs condition variables:** local workers sleep until notified,
  reducing idle CPU; distributed consumers use Kafka's poll API because it
  also drives group membership and rebalances.

## Implemented versus planned

The local C++ domain, scheduler, scoring, retries, heartbeat manager, DAG
executor, HTTP gateway, PostgreSQL repository, Redis store, Kafka client
wrappers, Prometheus gateway metrics, and benchmark tool have focused tests or
demos. The Docker-based distributed stack and broker-backed worker-kill
recovery were not verified in this environment. Object storage, full service
reconciliation, consumer lag/alert dashboards, and a transactional outbox
remain planned.
