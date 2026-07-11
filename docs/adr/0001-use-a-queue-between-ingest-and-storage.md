# 1. Put a queue between ingestion and storage

- **Status:** accepted
- **Date:** 2026-07-11

## Context

The ingest API must accept events far faster than the database can durably
write them. If the API writes to Postgres synchronously on every request, total
throughput is capped by database write latency, and a momentarily slow database
stalls the API or forces it to drop incoming events.

## Decision

Accept events at the API, hand them to a queue, and let a pool of workers drain
the queue and write to Postgres in **batches**. Start with a buffered Go channel
(Phase 1); move to a durable broker — NATS or Redpanda (Kafka API) — when the
queue must survive a process restart (Phase 2).

## Consequences

- The API responds in roughly constant time regardless of database speed.
- Delivery becomes **at-least-once**, so workers must be **idempotent**. We dedupe
  on `event_id` via `INSERT ... ON CONFLICT (event_id) DO NOTHING`, so a replayed
  batch after a crash cannot double-count.
- Batching (e.g. 1,000 rows or every 200ms, whichever comes first) turns many
  tiny writes into a few large ones — the single biggest throughput win.
- Cost: more moving parts to run and monitor (a broker, a worker pool), and a
  small window where accepted-but-not-yet-stored events live only in the queue.

## Why not exactly-once?

"Exactly-once delivery" across a network is effectively a myth — a sender can
never be sure a receiver got a message without a reply, and that reply can
itself be lost, forcing a resend. The practical answer is at-least-once delivery
plus idempotent writes, which produces exactly-once *effects*. That is what Tally
does.
