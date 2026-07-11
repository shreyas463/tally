package queue

import (
	"errors"
	"testing"

	"github.com/shreyas463/tally/internal/store"
)

func TestEnqueueUntilFull(t *testing.T) {
	q := NewMemory(2)

	if err := q.Enqueue(store.Event{EventID: "1"}); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if err := q.Enqueue(store.Event{EventID: "2"}); err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if err := q.Enqueue(store.Event{EventID: "3"}); !errors.Is(err, ErrFull) {
		t.Fatalf("expected ErrFull, got %v", err)
	}
	if got := q.Depth(); got != 2 {
		t.Fatalf("depth = %d, want 2", got)
	}
}

func TestCloseDrainsAndRejects(t *testing.T) {
	q := NewMemory(4)
	for i := 0; i < 3; i++ {
		if err := q.Enqueue(store.Event{EventID: string(rune('a' + i))}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	q.Close()

	if err := q.Enqueue(store.Event{EventID: "late"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed after Close, got %v", err)
	}

	// Everything already queued is still readable, then the channel closes.
	var drained int
	for range q.Events() {
		drained++
	}
	if drained != 3 {
		t.Fatalf("drained %d events, want 3", drained)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	q := NewMemory(1)
	q.Close()
	q.Close() // must not panic
}
