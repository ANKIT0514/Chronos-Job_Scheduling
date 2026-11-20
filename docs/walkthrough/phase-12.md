# Phase 12: Metrics and structured logs

## What was built

- `GET /metrics` exposes Prometheus text-format request, rejection,
  submission, and cancellation counters.
- API job submission, cancellation, and backend failures emit JSON log records
  with event name and relevant job ID; the scheduler emits structured
  transition and worker-loss events.
- `deploy/prometheus.yml` scrapes the gateway metrics endpoint.

Counters use atomics so multiple cpp-httplib request threads can update them
without extending scheduler or rate-limit lock duration. The metrics endpoint
is unauthenticated so a Prometheus scraper can reach it; do not expose it
outside a trusted network without a scrape-access policy.

## Verification

The API loopback tests assert Prometheus content types and the exact request,
rejection, and submission counter values. Docker-based Prometheus scraping was
not run.
