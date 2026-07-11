package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/shreyas463/tally/internal/store"
)

// fakeStore records every batch it receives.
type fakeStore struct {
	mu      sync.Mutex
	batches [][]store.Event
	failN   int // fail this many calls before succeeding
}

func (f *fakeStore) InsertBatch(_ context.Context, events []store.Event) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failN > 0 {
		f.failN--
		return 0, errors.New("simulated db failure")
	}
	cp := make([]store.Event, len(events))
	copy(cp, events)
	f.batches = append(f.batches, cp)
	return int64(len(events)), nil
}

func (f *fakeStore) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, b := range f.batches {
		n += len(b)
	}
	return n
}

func (f *fakeStore) maxBatch() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := 0
	for _, b := range f.batches {
		if len(b) > m {
			m = len(b)
		}
	}
	return m
}

func TestAllEventsDeliveredInBatches(t *testing.T) {
	const total = 2500
	const batchSize = 100

	ch := make(chan store.Event, total)
	fs := &fakeStore{}
	p := New(ch, fs, Config{Workers: 3, BatchSize: batchSize, FlushInterval: 50 * time.Millisecond})
	p.Start()

	for i := 0; i < total; i++ {
		ch <- store.Event{EventID: fmt.Sprintf("evt-%d", i)}
	}
	close(ch)
	p.Wait()

	if got := fs.total(); got != total {
		t.Fatalf("delivered %d events, want %d", got, total)
	}
	if got := fs.maxBatch(); got > batchSize {
		t.Fatalf("saw a batch of %d, must never exceed %d", got, batchSize)
	}
}

func TestPartialBatchFlushedOnClose(t *testing.T) {
	ch := make(chan store.Event, 10)
	fs := &fakeStore{}
	p := New(ch, fs, Config{Workers: 1, BatchSize: 1000, FlushInterval: time.Hour})
	p.Start()

	// Far fewer events than the batch size, and a flush interval that will
	// never fire — only the close-drain path can save these.
	for i := 0; i < 7; i++ {
		ch <- store.Event{EventID: fmt.Sprintf("evt-%d", i)}
	}
	close(ch)
	p.Wait()

	if got := fs.total(); got != 7 {
		t.Fatalf("delivered %d events, want 7 (partial batch must flush on close)", got)
	}
}

func TestTimerFlushesLightTraffic(t *testing.T) {
	ch := make(chan store.Event, 10)
	fs := &fakeStore{}
	p := New(ch, fs, Config{Workers: 1, BatchSize: 1000, FlushInterval: 20 * time.Millisecond})
	p.Start()

	ch <- store.Event{EventID: "lonely"}

	// Wait for the ticker to fire — well past one interval.
	deadline := time.Now().Add(2 * time.Second)
	for fs.total() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := fs.total(); got != 1 {
		t.Fatalf("timer flush delivered %d events, want 1", got)
	}

	close(ch)
	p.Wait()
}

func TestRetriesThenSucceeds(t *testing.T) {
	ch := make(chan store.Event, 10)
	fs := &fakeStore{failN: 2} // first two attempts fail, third succeeds
	var flushErr error
	p := New(ch, fs, Config{
		Workers: 1, BatchSize: 5, FlushInterval: time.Hour, MaxRetries: 3,
		OnFlush: func(fi FlushInfo) { flushErr = fi.Err },
	})
	p.Start()

	for i := 0; i < 5; i++ {
		ch <- store.Event{EventID: fmt.Sprintf("evt-%d", i)}
	}
	close(ch)
	p.Wait()

	if got := fs.total(); got != 5 {
		t.Fatalf("delivered %d events, want 5 (should survive 2 failures)", got)
	}
	if flushErr != nil {
		t.Fatalf("final flush reported error: %v", flushErr)
	}
}
