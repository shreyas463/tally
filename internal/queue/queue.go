// Package queue is the conveyor belt between the ingest API and the workers.
//
// The API drops events here and returns immediately; workers drain it in
// batches. When the belt is full, Enqueue fails fast with ErrFull instead of
// blocking — the API turns that into a 503 so callers know to back off
// (backpressure) rather than silently losing data.
//
// This in-memory implementation is Phase 1: fast, simple, but queued events
// die with the process. Phase 2 adds a durable broker (Redpanda) behind the
// same shape so a restart loses nothing.
package queue

import (
	"errors"
	"sync/atomic"

	"github.com/shreyas463/tally/internal/store"
)

// ErrFull means the queue is at capacity — the caller should retry later.
var ErrFull = errors.New("queue is full")

// ErrClosed means the queue is shutting down and accepts no new events.
var ErrClosed = errors.New("queue is closed")

// Memory is a bounded in-process queue backed by a buffered channel.
//
// Shutdown contract: the HTTP server must be stopped BEFORE Close is called,
// so no Enqueue can race with the channel closing.
type Memory struct {
	ch     chan store.Event
	closed atomic.Bool
}

// NewMemory creates a queue that holds up to size events.
func NewMemory(size int) *Memory {
	return &Memory{ch: make(chan store.Event, size)}
}

// Enqueue adds one event without blocking. It returns ErrFull when the queue
// is at capacity and ErrClosed during shutdown.
func (q *Memory) Enqueue(e store.Event) error {
	if q.closed.Load() {
		return ErrClosed
	}
	select {
	case q.ch <- e:
		return nil
	default:
		return ErrFull
	}
}

// Events is the channel workers consume from. It is closed by Close, which
// lets workers drain everything left and then exit cleanly.
func (q *Memory) Events() <-chan store.Event { return q.ch }

// Depth reports how many events are currently waiting — the queue's fill level.
func (q *Memory) Depth() int { return len(q.ch) }

// Close marks the queue closed and closes the channel. Safe to call once no
// producers remain (see the shutdown contract above).
func (q *Memory) Close() {
	if q.closed.CompareAndSwap(false, true) {
		close(q.ch)
	}
}
