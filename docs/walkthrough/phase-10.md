# Phase 10: Kafka event streaming and service binaries

## What was built

- `include/chronos/streaming/kafka_event_bus.hpp` and
  `src/kafka_event_bus.cpp` define the Kafka topics plus librdkafka producer
  and consumer wrappers.
- Producer delivery waits for broker acknowledgements; producer idempotence
  and `acks=all` are enabled.
- Consumers disable auto-commit and expose an explicit acknowledgement method.
  The application must acknowledge only after processing succeeds; an
  unacknowledged message remains available for replay.
- `src/api_main.cpp` can publish submitted jobs and cancellation requests to
  Kafka while reading durable status from PostgreSQL.
- `src/scheduler_service_main.cpp`, `src/worker_manager_service_main.cpp`, and
  `src/worker_agent_main.cpp` are separate service entry points for assignment
  and worker lifecycle handling.
- `tests/kafka_event_bus_test.cpp` verifies configuration validation and has a
  real-broker publish/consume test when `CHRONOS_TEST_KAFKA_BROKERS` is set.

The event schemas are JSON. Consumers use stable job IDs, event IDs, and
database versions to make status updates idempotent and fence results from
workers whose assignment has expired.

## Delivery semantics and limitations

Kafka provides at-least-once processing when a consumer commits after work.
Kafka producer idempotence prevents duplicate producer retries within a
producer session; it does not make PostgreSQL updates, Redis writes, and Kafka
publishes one atomic transaction. A transactional outbox and coordinated
replay/recovery are production follow-ups. A service restart also needs
reconciliation of active assignments from durable stores.

The Kafka client and worker service targets are buildable with
`-DCHRONOS_ENABLE_KAFKA=ON`. The local environment's Docker engine became
unresponsive while starting a Kafka broker, so no broker-backed end-to-end
test or full distributed execution was verified in this session. Keep broker
deployment and distributed worker recovery under Roadmap until the Compose
stack and failure demo pass.
