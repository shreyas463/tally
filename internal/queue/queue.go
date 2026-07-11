// Package queue is the conveyor belt between the ingest API and the workers.
//
// Phase 1: replace the ingest handler's direct database write with a buffered
// Go channel here, so the API can accept events faster than Postgres can store
// them. Phase 2: swap the channel for a durable broker (NATS or Redpanda) so
// queued events survive a restart.
//
// Empty for now — this file marks where that work goes.
package queue
