# Benchmarks

> **Status: not yet measured.** This file holds real numbers once Phase 3 lands.
> The point of this project is measured performance work — so these results,
> with the before/after per optimization, are the centerpiece.

## How to run

1. Start the stack and the service:
   ```bash
   make up && make migrate && make run
   ```
2. In another terminal, run the load test:
   ```bash
   k6 run loadtest/ingest.js        # the real test
   # or the quick Go generator:
   make loadtest
   ```
3. Record throughput and latency from the k6 summary into the table below.

## Environment

- **Machine:** _your laptop — CPU model, cores, RAM_
- **Go:** _version (`go version`)_
- **Postgres:** 16 (Docker)

## Results

| Change | Throughput (events/sec) | p50 latency | p99 latency |
|--------|-------------------------|-------------|-------------|
| Phase 0 — synchronous insert per request | _TBD_ | _TBD_ | _TBD_ |
| Phase 1 — buffered queue + batched writes | _TBD_ | _TBD_ | _TBD_ |
| Phase 1 — tuned batch size / connection pool | _TBD_ | _TBD_ | _TBD_ |

## Notes

For each row, write one or two sentences on what changed and what the profiler
(`pprof`) showed was the bottleneck. That story — "I saw X in the flame graph,
changed Y, and throughput went from A to B" — is the thing interviewers remember.
