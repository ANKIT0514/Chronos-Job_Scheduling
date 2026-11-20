# Phase 7: REST API gateway

## What was built

- `include/chronos/api/api_gateway.hpp` defines the gateway configuration and
  lifecycle.
- `src/api_gateway.cpp` implements authenticated submission, cancellation,
  status queries, payload validation, and request throttling.
- `src/api_main.cpp` starts the service and requires an API key from the
  environment.
- `tests/api_gateway_test.cpp` exercises real loopback HTTP requests against
  the running C++ server.

The gateway uses cpp-httplib for HTTP and nlohmann/json for bounded JSON
parsing. `POST /jobs` accepts a validated ID, priority, resource request,
runtime, estimate, and optional deadline; `GET /jobs/{id}` returns state,
attempt count, and failure text; `DELETE /jobs/{id}` requests cancellation.
Requests require `X-API-Key`, and a process-wide fixed-window limit bounds
request volume.

## Concurrency and validation

cpp-httplib handles concurrent requests. A small mutex protects the single
rate-limit window; scheduler operations use their own lock. Request parsing
rejects invalid IDs, negative or oversized resource values, oversized payloads,
and jobs above the configured runtime cap. API-key comparison avoids an
early-exit string comparison. No request handler holds the rate-limit mutex
while executing a job.

## Verification and limitations

All 37 GoogleTests passed on native Windows, including authenticated,
unauthenticated, malformed, duplicate, rate-limited, query, and cancellation
requests over loopback HTTP. This gateway currently submits into the
in-process scheduler; it does not yet publish Kafka events or persist jobs to
PostgreSQL.
