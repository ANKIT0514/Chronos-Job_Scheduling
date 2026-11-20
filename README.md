# Chronos

Chronos is a C++17 workload scheduler prototype with a thread-pool scheduler,
resource-aware placement, retry and workflow primitives, an HTTP API, and
optional PostgreSQL, Redis, and Kafka integrations.

The local scheduler and HTTP gateway are runnable and tested. The integrations
and service binaries build where their dependencies are available, but the
complete multi-container stack and distributed worker-failure recovery have
not yet been verified.

## Architecture

```text
Client -> REST API -> PostgreSQL + Kafka -> Scheduler -> Kafka assignments
                          Redis <- Worker Manager <- heartbeats / events
                                                     Worker Agent
```

The API authenticates with an API key, validates requests, rate-limits clients,
and exposes Prometheus counters. PostgreSQL stores durable job metadata and
audit/attempt history; Redis stores expiring worker state; Kafka carries job
and worker events. The local scheduler uses a priority heap, a condition
variable, and a C++ thread pool. Resource scoring, bounded retries, heartbeat
tracking, and DAG execution are available in the core library.

For component responsibilities, failure modes, consistency boundaries, and
alternatives, see [docs/architecture.md](./docs/architecture.md). Each build
phase is described in [docs/walkthrough/](./docs/walkthrough/).

## Build and run locally

Requirements: CMake 3.16+, a C++17 compiler, pthreads, and network access on
the first configure to fetch cpp-httplib, nlohmann-json, and GoogleTest.

```powershell
cmake -S . -B build
cmake --build build --parallel
ctest --test-dir build --output-on-failure
.\build\chronos.exe
```

The `chronos` executable runs a four-job local demo. The API gateway can run
independently with an API key:

```powershell
$env:CHRONOS_API_KEY = "local-development-key"
.\build\chronos_api_gateway.exe
```

Example requests (PowerShell):

```powershell
$headers = @{ "X-API-Key" = "local-development-key" }
$body = @{
  id = "example-1"
  priority = 10
  resources = @{ cpu_millis = 500; memory_bytes = 1048576; gpu_count = 0 }
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:8080/jobs `
  -Headers $headers -ContentType "application/json" -Body $body
Invoke-RestMethod -Uri http://localhost:8080/jobs/example-1 -Headers $headers
Invoke-RestMethod -Method Delete -Uri http://localhost:8080/jobs/example-1 `
  -Headers $headers
Invoke-WebRequest -Uri http://localhost:8080/metrics
```

Routes are `POST /jobs`, `GET /jobs/{id}`, `DELETE /jobs/{id}`, and
`GET /metrics`. The API key is required for job routes. `/metrics` is
intentionally unauthenticated for scraping; keep it on a trusted network.

## Optional integrations

Enable the dependency-backed components with CMake options:

```powershell
cmake -S . -B build-integrations `
  -DCHRONOS_ENABLE_POSTGRES=ON `
  -DCHRONOS_ENABLE_REDIS=ON `
  -DCHRONOS_ENABLE_KAFKA=ON
cmake --build build-integrations --parallel
ctest --test-dir build-integrations --output-on-failure
```

Install the corresponding development packages (`libpqxx` 7.x, hiredis,
librdkafka) and pkg-config metadata first. Chronos targets C++17; libpqxx 8.x
requires C++20 and is rejected by CMake. Set `CHRONOS_TEST_DATABASE_URL`,
`CHRONOS_TEST_REDIS_HOST` / `CHRONOS_TEST_REDIS_PORT`, or
`CHRONOS_TEST_KAFKA_BROKERS` to run broker/store integration tests against
real services. Without those services, the related integration tests are
skipped; a skip is not evidence that the service works.

## Docker Compose

Docker Compose describes Kafka (KRaft), Redis, PostgreSQL, topic initialization,
the API, scheduler, worker manager, worker agent, and Prometheus. Configure
credentials and start it from the repository root:

```powershell
Copy-Item .env.example .env
# Edit .env and replace CHRONOS_API_KEY with a long random value.
docker compose up --build
```

In another terminal, use the same API key from `.env` to submit and query a
job using the requests above. Prometheus is exposed at
`http://localhost:9090`; the API metrics endpoint is
`http://localhost:8080/metrics`. Stop the stack with `Ctrl+C`, or run
`docker compose down` (persistent database and Redis volumes remain).

**Verification status:** Docker Desktop's engine was returning API errors and
could not be restarted without elevation in the development environment.
Consequently, the Compose stack, API-to-Kafka flow, Prometheus scrape, and a
worker-kill/reassignment demo were not run here. `CHRONOS_FAIL_FIRST_JOBS`
injects a failed first attempt in the worker agent; it is not equivalent to
killing a worker process. Do not treat Compose instructions as verified
end-to-end recovery.

System architecture : <img width="1536" height="1024" alt="4db42093-46f0-4302-995a-aff65057bc8b" src="https://github.com/user-attachments/assets/20cc8a38-3281-4323-bae1-78ca2131aec4" />




