# 4. Two protection layers: per-client rate limits and global backpressure

- **Status:** accepted
- **Date:** 2026-07-11

## Context

Two different overload problems need two different answers:

1. **One noisy client** hammering the API must not degrade service for
   everyone else (fairness).
2. **Legitimate total load exceeding capacity** must slow senders down
   instead of silently losing events (survival).

## Decision

- **Fairness → per-key rate limiting (429 Too Many Requests).** Each client —
  identified by `X-API-Key`, falling back to IP — gets a token bucket:
  `RATE_LIMIT_RPS` sustained with `RATE_LIMIT_BURST` headroom. In-memory
  buckets (`golang.org/x/time/rate`) on a single instance; with `REDIS_ADDR`
  set, a Redis fixed 1-second window enforces the limit globally across all
  ingest replicas. The Redis path **fails open**: if Redis is down we briefly
  lose rate limiting rather than reject all traffic — the less bad failure.
- **Survival → queue-full backpressure (503 + Retry-After).** When the belt
  is full, the API refuses new events fast and tells callers when to retry.
  Refusing loudly at the front door is the only honest option left once
  buffering is exhausted; dropping silently would corrupt analytics.

The status codes are deliberately different — 429 means "YOU are over your
limit", 503 means "WE are saturated" — so a client can react correctly
(back off per-key vs back off globally).

## Consequences

- The token bucket allows short bursts (real traffic is bursty) while
  holding the long-run average to the configured rate.
- The Redis window is coarser than a token bucket (no burst shaping across
  the window boundary) — the accepted cost of a global limit with one round
  trip per request.
- Rate limiting defaults to OFF (`RATE_LIMIT_RPS=0`) so local dev and load
  tests measure the pipeline, not the limiter.
