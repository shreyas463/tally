# 5. Count unique users with HyperLogLog sketches, not exact sets

- **Status:** accepted
- **Date:** 2026-07-11

## Context

"How many events happened?" is solved by the rollup counters. "How many
DIFFERENT users did it?" is a much harder question, because distinctness has
memory: to know whether this user already counted, *something* has to
remember every user seen. The obvious options both fail at scale:

- `COUNT(DISTINCT distinct_id)` over raw events — rescans an ever-growing
  table on every query.
- An exact set (or unique index) per (event, day) — storage grows linearly
  with users, and merging sets across days/shards is expensive.

## Decision

Keep one **HyperLogLog sketch** per (event name, UTC day): a fixed ~16 KB
structure that estimates distinct counts with ~0.8–1% typical error and
supports lossless **merging** (register-wise max). Workers fold each batch's
`distinct_id`s into the day's sketch inside the same transaction as the batch
insert; queries deserialize one row and read the estimate in microseconds.

The implementation ([internal/hll](../../internal/hll)) is written from
scratch — ~100 lines: hash each id, use the top 14 bits to pick one of 16,384
registers, keep the longest run of leading zeros per register, estimate via
the bias-corrected harmonic mean with linear counting for small cardinalities.
Accuracy is enforced by tests from 100 to 1,000,000 distinct values.

This is the same trade Redis (`PFCOUNT`), BigQuery (`APPROX_COUNT_DISTINCT`),
and every serious analytics store makes: give up ~1% accuracy, gain bounded
memory and mergeability.

## Concurrency design (the subtle part)

Multiple workers update the same (name, day) sketch concurrently. The
read-merge-write cycle uses:

1. `INSERT ... ON CONFLICT DO NOTHING` (ensure the row exists — without
   this, two first-writers race and one sketch silently overwrites the
   other: a lost update);
2. `SELECT ... FOR UPDATE` (take the row lock);
3. merge in Go, plain `UPDATE`.

Rows are processed in sorted (name, day) order — the same deterministic
lock-ordering rule that fixed the rollup deadlock (see the load-test-found
deadlock fix in `internal/store`). Cost: concurrent flushes serialize per
(name, day) row for a few milliseconds. With batch-level writes that
contention is bounded and measured, not accidental.

## Consequences

- Unique counts are **estimates** (~1% error), labeled `≈` in the dashboard
  and documented in the API. Exact-when-small comes free: linear counting
  makes low cardinalities near-exact.
- Sketches merge, so "uniques this week" is a cheap fold over 7 rows —
  future work, but the storage design already supports it.
- Events without a `distinct_id` are excluded from uniques (they have no
  user to count) but still counted in event totals.
