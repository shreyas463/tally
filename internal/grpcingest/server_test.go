package grpcingest

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	tallypb "github.com/shreyas463/tally/gen/tally/v1"
	"github.com/shreyas463/tally/internal/queue"
	"github.com/shreyas463/tally/internal/store"
)

type fakeQueue struct {
	events []store.Event
	err    error
	failAt int // return err starting at this enqueue (0 = always, -1 = never)
}

func (f *fakeQueue) Enqueue(e store.Event) error {
	if f.err != nil && (f.failAt <= 0 || len(f.events) >= f.failAt) {
		return f.err
	}
	f.events = append(f.events, e)
	return nil
}

// dial spins up the service on an in-memory listener and returns a real
// gRPC client connected to it.
func dial(t *testing.T, q *fakeQueue) tallypb.TallyServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	New(q, nil).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return tallypb.NewTallyServiceClient(conn)
}

func TestPublishBatch(t *testing.T) {
	fq := &fakeQueue{failAt: -1}
	client := dial(t, fq)

	resp, err := client.Publish(context.Background(), &tallypb.PublishRequest{
		Events: []*tallypb.Event{
			{EventId: "e1", Name: "buy_click", DistinctId: "u1", Properties: map[string]string{"src": "sdk"}},
			{EventId: "e2", Name: "page_view", DistinctId: "u2"},
		},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if resp.Accepted != 2 || len(resp.Rejected) != 0 {
		t.Fatalf("accepted=%d rejected=%d, want 2/0", resp.Accepted, len(resp.Rejected))
	}
	if len(fq.events) != 2 || fq.events[0].EventID != "e1" || fq.events[0].Properties["src"] != "sdk" {
		t.Fatalf("events not enqueued faithfully: %+v", fq.events)
	}
	if fq.events[0].TS.IsZero() {
		t.Fatal("timestamp was not set")
	}
}

func TestPublishReportsInvalidPerIndex(t *testing.T) {
	fq := &fakeQueue{failAt: -1}
	client := dial(t, fq)

	resp, err := client.Publish(context.Background(), &tallypb.PublishRequest{
		Events: []*tallypb.Event{
			{EventId: "", Name: "x"},           // missing event_id
			{EventId: "e2", Name: ""},          // missing name
			{EventId: "e3", Name: "buy_click"}, // valid
		},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if resp.Accepted != 1 || len(resp.Rejected) != 2 {
		t.Fatalf("accepted=%d rejected=%d, want 1/2", resp.Accepted, len(resp.Rejected))
	}
	if resp.Rejected[0].Index != 0 || resp.Rejected[1].Index != 1 {
		t.Fatalf("rejected indexes wrong: %+v", resp.Rejected)
	}
}

func TestPublishQueueFullIsResourceExhausted(t *testing.T) {
	// Queue accepts 1 event then reports full mid-batch.
	fq := &fakeQueue{err: queue.ErrFull, failAt: 1}
	client := dial(t, fq)

	_, err := client.Publish(context.Background(), &tallypb.PublishRequest{
		Events: []*tallypb.Event{
			{EventId: "e1", Name: "a"},
			{EventId: "e2", Name: "b"},
		},
	})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("err = %v, want RESOURCE_EXHAUSTED", err)
	}
}

// denyAll rejects every rate-limit check.
type denyAll struct{}

func (denyAll) Allow(string) bool { return false }

func TestPublishRateLimited(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	New(&fakeQueue{failAt: -1}, denyAll{}).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	_, err = tallypb.NewTallyServiceClient(conn).Publish(context.Background(),
		&tallypb.PublishRequest{Events: []*tallypb.Event{{EventId: "e1", Name: "x"}}})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("err = %v, want RESOURCE_EXHAUSTED when rate limited", err)
	}
}
