# Benchmarks

> **Status: tooling ready, numbers not yet measured.** Everything below is
> runnable today; the tables get filled in on real hardware. No numbers are
> published until they're real — that's the point of this file.

## How to run

```bash
make up && make migrate        # infra
make run                       # the service (in another terminal)

# Quick pass — built-in generator (reports throughput + p50/p95/p99):
go run ./cmd/loadgen -rate 5000 -duration 30s

# Real pass — k6 ramping load:
k6 run loadtest/ingest.js
```

### Profiling (finding the next bottleneck)

While a load test is running:

```bash
# 30s CPU profile, then open the flame graph in the browser
go tool pprof -http=:9999 http://localhost:8080/debug/pprof/profile?seconds=30

# Heap snapshot
go tool pprof -http=:9999 http://localhost:8080/debug/pprof/heap
```

Screenshot the flame graph for any optimization you make — "the profile
showed X, so I changed Y, and the number moved from A to B" is the story
this whole file exists to tell.

### Verifying zero loss under load

After any run: total accepted (loadgen's `sent=`) must equal
`sum(count_today)` across event names (`curl localhost:8080/v1/stats`), and
`tally_events_dropped_total` on /metrics must be `0`.

## Environment

Record these once per machine so numbers are comparable:

- **Machine:** _CPU model, cores, RAM_ (`sysctl -n machdep.cpu.brand_string`, `sysctl -n hw.ncpu`)
- **Go:** _`go version`_
- **Postgres:** 16 (Docker), **Redpanda:** v24.1.7 (Docker), same machine

## Results — in-memory queue (QUEUE=memory)

| Change | Throughput (events/sec) | p50 | p99 | Notes |
|---|---|---|---|---|
| Baseline: batch=1000, flush=200ms, workers=4 | _TBD_ | _TBD_ | _TBD_ | |
| Tuned batch size (____) | _TBD_ | _TBD_ | _TBD_ | what the profile showed: |
| Tuned pool / connections (____) | _TBD_ | _TBD_ | _TBD_ | |

## Results — durable queue (QUEUE=kafka)

| Change | Throughput (events/sec) | p50 | p99 | Notes |
|---|---|---|---|---|
| Baseline, MODE=all | _TBD_ | _TBD_ | _TBD_ | durability tax vs memory: |
| Split ingest + worker processes | _TBD_ | _TBD_ | _TBD_ | |

## Chaos result (scripts/chaos.sh)

| Scenario | Sent | Stored | Lost |
|---|---|---|---|
| SIGKILL worker mid-stream, replacement drains backlog | _TBD_ | _TBD_ | must be **0** |

## Reading the numbers honestly

- Loadgen and service on the same laptop compete for CPU — note it.
- p99 through Docker networking is noisier than bare metal — fine, just
  compare like with like (before/after on the same setup).
- The interesting deliverable is the **delta per change**, not the absolute
  number.
