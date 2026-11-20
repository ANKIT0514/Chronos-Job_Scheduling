# Phase 11: Docker packaging

## What was added

- `Dockerfile` builds the C++17 application and optional PostgreSQL, Redis, and
  Kafka targets in Ubuntu 24.04, then copies the executables into a non-root
  runtime image.
- `docker-compose.yml` describes PostgreSQL, Redis, Kafka KRaft, topic
  initialization, the REST gateway, scheduler, worker manager, worker agent,
  and Prometheus.
- `.env.example` lists local-only environment variables to configure.

Compose health dependencies protect the service start sequence. PostgreSQL,
Redis, and Kafka data are kept in named volumes.

## Limitations

The Docker engine stopped responding with API 500 errors during Kafka image
startup; Docker Desktop's service was stopped and could not be restarted
without elevated permissions. Consequently, the Docker image and Compose
service graph were not built or run here. The Compose stack and worker-kill
demo are unverified and remain Roadmap items.
