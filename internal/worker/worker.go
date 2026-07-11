// Package worker drains the queue and writes events to Postgres in batches.
//
// Phase 1: a pool of goroutines that pull events off the queue, group them
// into batches (e.g. 1,000 or every 200ms, whichever comes first), and insert
// them in one round-trip. Handles graceful shutdown so a batch in progress is
// flushed before the process exits — no lost events.
//
// Empty for now — this file marks where that work goes.
package worker
