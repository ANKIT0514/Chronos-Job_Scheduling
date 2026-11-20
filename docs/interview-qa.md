# Chronos interview questions and answers

These answers describe the current implementation and distinguish it from
unfinished distributed-system work. See [the architecture overview](./architecture.md)
and [phase walkthroughs](./walkthrough/) for more detail.

## Scheduling and data structures

1. **Why use a priority queue for pending jobs?**  
   It makes the next eligible job available without sorting the entire queue.
   The comparator orders priority first and then deadline/FIFO tie-breakers.

2. **What are the heap operation costs?**  
   Reading the top is O(1); insertion and removal are O(log n), where n is the
   number of queued jobs.

3. **How do equal-priority jobs behave?**  
   The queue preserves deterministic tie-breaking, including FIFO order when
   priority and deadline are equal.

4. **How does worker selection scale?**  
   It evaluates worker snapshots in O(w) and sorts candidates in O(w log w),
   where w is the number of workers. A large deployment may need indexed
   capacity classes or partitioned schedulers.

5. **How is resource fit determined?**  
   A candidate must be healthy and have enough unallocated CPU, memory, and
   GPU capacity. The configured scoring strategy ranks only eligible workers.

6. **Which placement strategies are available?**  
   Least-loaded, resource-aware best-fit, priority-weighted, and
   deadline-aware scoring are represented and covered by tests.

7. **How are deadlines used?**  
   Deadline-aware scoring compares estimated completion, including an
   estimated start delay, against the job deadline. A score is not a hard
   real-time guarantee.

8. **Why track reservations?**  
   Reservations ensure simultaneous placements do not overcommit the same
   worker snapshot. Completion or loss releases reserved resources.

9. **Why use a heap rather than a sorted vector?**  
   A heap offers logarithmic insertion/removal as queue size grows. A sorted
   vector may be simpler and faster for tiny mostly-static workloads, but
   insertion costs O(n).

10. **How would you avoid starvation?**  
    The current priority-first ordering does not promise a formal starvation
    bound. Aging, weighted fair queues, or per-tenant quotas would be
    reasonable additions.

## Concurrency

11. **Why use a condition variable instead of busy-waiting?**  
    Workers sleep while there is no work, so idle threads do not repeatedly
    consume CPU. Producers notify a worker when work becomes available.

12. **What prevents lost wakeups?**  
    Queue state is protected by a mutex, and workers wait with a predicate.
    They re-check the predicate after waking, including spurious wakeups.

13. **What is the scheduler's locking strategy?**  
    Shared queue and job records use a mutex. The scheduler releases it before
    running user callbacks so a slow task does not block unrelated state
    operations.

14. **How are deadlocks reduced?**  
    User work is not invoked while holding the scheduler state lock, and
    condition-variable waits release their associated mutex while asleep.
    Future multi-lock code should define and enforce a lock order.

15. **Can a running local task be forcibly cancelled?**  
    No. C++ cannot safely kill an arbitrary callback thread. Cancellation is
    cooperative; a running task must return or inspect a cancellation signal.

16. **What is the role of `steady_clock`?**  
    Timeouts and retry delays use a monotonic clock so wall-clock corrections
    do not make elapsed-time calculations jump backward or forward.

17. **How is shared worker state protected?**  
    The in-memory manager serializes updates to its worker/assignment state.
    Redis access is also serialized because a hiredis connection is not safe
    for concurrent use.

## Failure handling and workflows

18. **How does heartbeat-based failure detection work?**  
    A worker is considered timed out when its last heartbeat exceeds a
    configured duration. The manager marks it unhealthy and identifies its
    lost assignments for release and reassignment.

19. **Can heartbeat detection distinguish a crash from a network partition?**  
    No. A timeout is a suspicion, not proof of failure. It trades detection
    speed against false positives and should be tuned for expected network and
    scheduling delays.

20. **How does retry backoff work?**  
    Failed work is retried after exponentially increasing delays bounded by a
    configured maximum. Attempts are limited; exhausted jobs enter a
    dead-letter state.

21. **Why cap exponential backoff?**  
    A cap avoids unbounded delay and arithmetic growth while still spacing
    repeated attempts to reduce pressure on a recovering dependency.

22. **What is the difference between retry and reassignment?**  
    Retry is a policy decision to run a failed attempt again. Reassignment
    releases a job whose worker became unavailable and makes it eligible for
    another worker; durable distributed coordination is not yet complete.

23. **What happens when retries are exhausted?**  
    The in-memory scheduler marks the job dead-lettered. A durable operational
    replay/inspection workflow for distributed dead-letter records remains a
    follow-up.

24. **How are DAG dependencies validated?**  
    Topological ordering detects cycles before execution. Invalid references
    and self-dependencies are rejected before jobs are scheduled.

25. **What is the DAG algorithm's complexity?**  
    It is O((V + E) log V) with ordered maps/sets, where V is jobs and E is
    dependency edges. A hash-based implementation could approach O(V + E)
    average time but would need deterministic ordering handled separately.

26. **What happens if a prerequisite fails?**  
    Descendant jobs are not submitted. Independent jobs within an already
    released level may have run concurrently.

27. **Are workflows persisted and distributed today?**  
    DAG validation and dependency-ordered execution are implemented in the
    local core. Durable, distributed workflow scheduling across the services
    is not complete.

## Persistence, messaging, and consistency

28. **What does PostgreSQL store?**  
    The schema covers workflows, jobs, dependencies, workers, attempts, job
    events, and audit records. Repository operations use transactions and
    version checks.

29. **How is a duplicate state update handled?**  
    Event IDs and job versions provide idempotency checks in the repository.
    Re-delivery of an already accepted event should not apply the transition a
    second time.

30. **What does idempotency not solve?**  
    It cannot roll back external side effects performed by a task. Task
    handlers still need idempotent operations or their own deduplication keys.

31. **Why use Redis in addition to PostgreSQL?**  
    Redis holds low-latency, expiring worker state, load, membership, and
    token-owned leases. PostgreSQL is used for durable metadata and history.

32. **How do leases avoid another owner releasing them?**  
    Lease release is guarded by the owner token with an atomic Redis script;
    a mismatched token does not release the lease.

33. **What happens to Redis state after a Redis restart?**  
    Ephemeral worker state may be lost or expire. Durable job state can be
    read from PostgreSQL, but complete assignment reconstruction/reconciliation
    is a known missing distributed capability.

34. **What delivery semantics does the Kafka wrapper provide?**  
    The producer waits for delivery acknowledgements and requests idempotence.
    Consumers disable automatic commit and explicitly acknowledge after
    processing, enabling at-least-once replay.

35. **Does Kafka producer idempotence make the whole system exactly-once?**  
    No. It scopes duplicate suppression to producer retries/session semantics.
    It does not atomically combine Kafka and PostgreSQL or Redis operations.

36. **What is the dual-write problem here?**  
    The API persists a submitted job and then publishes to Kafka. A crash
    between those operations can leave a database row without an event. A
    transactional outbox plus replay/reconciliation is needed to close this
    gap.

37. **Why not claim exactly-once processing?**  
    End-to-end exactly-once across independent databases, brokers, and task
    side effects is not established. The realistic design is at-least-once
    delivery with idempotent state transitions and application-level
    deduplication.

38. **What happens on Kafka consumer failure before acknowledgement?**  
    The message remains uncommitted and can be delivered again. Handlers must
    therefore tolerate duplicate processing.

39. **Has Kafka broker integration been verified?**  
    The client wrappers and binaries compile and configuration tests pass.
    Broker-backed tests and distributed end-to-end execution were unavailable
    in the development environment.

## API, availability, and scaling

40. **How is the REST API protected?**  
    Job routes require an API key, validate request fields, and apply a
    fixed-window rate limit. This is a prototype key model, not a complete
    identity or tenant authorization system.

41. **Why expose `/metrics` without API-key auth?**  
    Prometheus can scrape it without application credentials. It should remain
    on a trusted network or gain a separate scrape-access policy before
    public exposure.

42. **What is the system's CAP trade-off?**  
    During a partition, durable writes should fail rather than pretend that
    PostgreSQL/Kafka state is committed. Redis worker state is ephemeral and
    availability can be reduced until it is reconciled. The current prototype
    does not implement a formally complete CAP policy.

43. **What are likely scaling bottlenecks?**  
    A global in-memory scheduler lock, scanning/sorting workers, one scheduler
    queue, Kafka partition throughput, and PostgreSQL write contention are
    likely limits. Metrics and load tests should establish which is dominant.

44. **How might the scheduler scale out?**  
    Partition jobs by tenant or key, use Kafka consumer groups, shard worker
    inventories, and coordinate reservations with durable ownership/fencing.
    The outbox and reconciliation work should precede claiming safe horizontal
    scheduler failover.

45. **What is the benchmark actually measuring?**  
    It measures local in-memory queue-to-start latency, throughput, callback
    wall-time utilization, and injected-failure retry completion at 1, 2, 4,
    and 8 worker threads. It does not measure distributed Kafka or host CPU
    utilization.

46. **Why are scheduling latency numbers large relative to task duration?**  
    The benchmark submits a batch of 1,000 tasks. Queue-to-start latency
    includes waiting behind earlier work, particularly with fewer worker
    threads.

47. **What do you need to repeat a benchmark fairly?**  
    Record hardware, OS, compiler, job count, task duration, and load. Run
    multiple trials on an otherwise quiet host and report distributions rather
    than treating one run as universal performance.

48. **Has the Docker failure-recovery demo been completed?**  
    No. The local benchmark injects a first-attempt failure, but Docker
    Compose, broker-backed coordination, and killing a worker process during
    a running task have not been verified. This distinction is explicitly
    recorded in the README.

49. **What would you implement next?**  
    Add a PostgreSQL transactional outbox, make scheduler assignment state
    durable and fenced, reconcile work after restarts, then validate real
    broker and worker-loss scenarios in Compose.

50. **What would you do differently for production?**  
    Define service-level objectives and tenancy/isolation requirements first;
    add authentication/authorization, resource quotas, schema evolution,
    durable reconciliation, observability/alerts, security hardening, and
    chaos/load tests before expanding features.
