# Phase 13: Benchmark tool

## What was built

- `src/benchmark_main.cpp` runs workloads with 1, 2, 4, and 8 scheduler
  workers.
- It records throughput, queue-to-start scheduling-latency p50/p95/p99,
  worker-time utilization, and the time from the first injected failure to
  successful retry completion.
- Raw data is saved in `docs/benchmarks/chronos_benchmark.csv`; detected host
  specifications are saved in `docs/benchmarks/machine-specs.json`.
- The tool accepts `--jobs`, `--duration-ms`, and `--output-dir`. It fails
  explicitly if it cannot complete the recovery scenario or save measurements.

Percentiles use the nearest-rank method. Utilization is aggregate callback
wall-time divided by worker count and total elapsed time; it is not host CPU
utilization or a resource-saturation measurement.

## Verification and interpretation

The tool was run on the Windows host with GCC 16.2.0 and 1,000 one-millisecond
tasks per worker count. These measurements are real but reflect this
in-memory test workload and the loaded host at run time; scheduling latency
includes queueing behind the deliberately submitted batch.
