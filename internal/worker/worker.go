// Package worker drains the queue and writes events to Postgres in batches.
//
// Batching is THE throughput trick: inserting 1,000 events in one database
// round-trip is enormously faster than 1,000 separate inserts. Each worker
// collects events until the batch is full OR a flush interval passes —
// whichever comes first — so heavy traffic gets big efficient batches and
// light traffic still lands within ~one interval.
package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/shreyas463/tally/internal/store"
)

// BatchInserter is what a worker needs from the storage layer. It returns how
// many events were newly inserted (duplicates excluded) and must be
// idempotent, because at-least-once delivery means a batch can be retried.
type BatchInserter interface {
	InsertBatch(ctx context.Context, events []store.Event) (int64, error)
}

// FlushInfo describes one completed flush, for metrics/logging hooks.
type FlushInfo struct {
	BatchSize  int
	Inserted   int64
	Duplicates int64
	Took       time.Duration
	Err        error // non-nil only when the batch was dropped after retries
}

// Config tunes the pool.
type Config struct {
	Workers       int           // how many concurrent workers
	BatchSize     int           // flush when a batch reaches this many events
	FlushInterval time.Duration // ...or when this much time has passed
	MaxRetries    int           // insert attempts per batch before giving up
	OnFlush       func(FlushInfo)
}

// Pool consumes events from a channel and batch-inserts them.
type Pool struct {
	cfg   Config
	src   <-chan store.Event
	sink  BatchInserter
	wg    sync.WaitGroup
	clock func() time.Time
}

// New creates a pool reading from src and writing to sink.
func New(src <-chan store.Event, sink BatchInserter, cfg Config) *Pool {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 200 * time.Millisecond
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	return &Pool{cfg: cfg, src: src, sink: sink, clock: time.Now}
}

// Start launches the workers. They run until the source channel is closed and
// drained, flushing any partial batch before exiting — that is what makes
// graceful shutdown lose nothing.
func (p *Pool) Start() {
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.run(i)
	}
}

// Wait blocks until every worker has drained and exited.
func (p *Pool) Wait() { p.wg.Wait() }

func (p *Pool) run(id int) {
	defer p.wg.Done()

	batch := make([]store.Event, 0, p.cfg.BatchSize)
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case e, ok := <-p.src:
			if !ok {
				// Queue closed: flush whatever is left, then exit.
				p.flush(&batch)
				return
			}
			batch = append(batch, e)
			if len(batch) >= p.cfg.BatchSize {
				p.flush(&batch)
			}
		case <-ticker.C:
			p.flush(&batch)
		}
	}
}

// flush writes the current batch with retries. Uses a background context on
// purpose: a flush during shutdown must still complete.
func (p *Pool) flush(batch *[]store.Event) {
	if len(*batch) == 0 {
		return
	}
	events := *batch
	start := p.clock()

	var inserted int64
	var err error
	for attempt := 1; attempt <= p.cfg.MaxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		inserted, err = p.sink.InsertBatch(ctx, events)
		cancel()
		if err == nil {
			break
		}
		log.Printf("worker: batch insert failed (attempt %d/%d): %v", attempt, p.cfg.MaxRetries, err)
		if attempt < p.cfg.MaxRetries {
			time.Sleep(time.Duration(attempt) * 250 * time.Millisecond)
		}
	}
	if err != nil {
		// In memory-queue mode there is nowhere safe to put a failed batch, so
		// after exhausting retries it is dropped (and surfaced via OnFlush).
		// Phase 2's broker mode fixes this properly: offsets aren't committed
		// until the insert succeeds, so the batch is redelivered instead.
		log.Printf("worker: DROPPING batch of %d after %d attempts: %v", len(events), p.cfg.MaxRetries, err)
	}

	if p.cfg.OnFlush != nil {
		info := FlushInfo{
			BatchSize: len(events),
			Took:      p.clock().Sub(start),
			Err:       err,
		}
		if err == nil {
			info.Inserted = inserted
			info.Duplicates = int64(len(events)) - inserted
		}
		p.cfg.OnFlush(info)
	}

	*batch = (*batch)[:0]
}
