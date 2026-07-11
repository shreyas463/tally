# 3. Redpanda for durability; commit offsets only after the insert

- **Status:** accepted
- **Date:** 2026-07-11

## Context

The Phase 1 in-memory queue is fast but has one hole: events sitting on the
belt die with the process. To survive crashes and restarts — and to run
ingest and workers as separate, independently scalable processes — the queue
must live outside the process.

## Decision

Add a second queue backend (`QUEUE=kafka`) speaking the Kafka protocol, run
as **Redpanda** in development (single container, no JVM/ZooKeeper — much
lighter than a Kafka cluster, wire-compatible with it). The memory backend
remains the default for simple local runs.

Two ordering rules carry the entire crash-safety guarantee:

1. **Workers commit consumer offsets only AFTER a batch is stored.**
   Poll → insert (retry until success) → commit. A worker killed at any point
   before the commit means the broker redelivers that batch to another worker.
2. **Redelivery is harmless because the insert is idempotent** (unique
   `event_id`, `ON CONFLICT DO NOTHING`, rollups counted from actually-inserted
   rows only — see ADR 0002 reasoning inside `internal/store`).

Together: at-least-once delivery + idempotent consumption = counted exactly
once, even through a SIGKILL. `scripts/chaos.sh` demonstrates this end to end.

## Honest limits (know these for the interview)

- **The producer is asynchronous.** The API returns 202 once the event is
  buffered in the producer; the broker ack happens in the background (with
  internal retries and an idempotent producer). A machine-level crash in that
  tiny window can lose buffered events. Closing that window means acking
  after the broker confirms — a latency/durability trade we call out rather
  than hide.
- **Kafka mode trades latency for durability.** An extra network hop and
  broker fsync sit between accept and store. BENCHMARKS.md will quantify the
  difference between the two backends.

## Alternatives considered

- **NATS JetStream** — excellent and lighter, but the Kafka protocol is the
  industry lingua franca and worth demonstrating; Redpanda gives that without
  cluster-operations pain.
- **Postgres-as-queue (SKIP LOCKED)** — viable at modest scale and one less
  moving part, but it points the firehose at the same database we are trying
  to protect, and it teaches less.
