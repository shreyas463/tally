package ingest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shreyas463/tally/internal/queue"
	"github.com/shreyas463/tally/internal/store"
)

type fakeQueue struct {
	events []store.Event
	err    error
}

func (f *fakeQueue) Enqueue(e store.Event) error {
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, e)
	return nil
}

type fakeStats struct{ count int64 }

func (f *fakeStats) CountToday(context.Context, string) (int64, error) { return f.count, nil }
func (f *fakeStats) TotalsToday(context.Context) ([]store.NameCount, error) {
	return []store.NameCount{{Name: "buy_click", Count: f.count}}, nil
}
func (f *fakeStats) Series(context.Context, time.Duration) ([]store.MinutePoint, error) {
	return []store.MinutePoint{}, nil
}

func newServer(q Enqueuer, s StatsStore) *httptest.Server {
	mux := http.NewServeMux()
	New(q, s).Register(mux)
	return httptest.NewServer(mux)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url+"/v1/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestPostEventAccepted(t *testing.T) {
	fq := &fakeQueue{}
	srv := newServer(fq, &fakeStats{})
	defer srv.Close()

	resp := post(t, srv.URL, `{"event_id":"e1","name":"buy_click","distinct_id":"u1"}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if len(fq.events) != 1 || fq.events[0].EventID != "e1" {
		t.Fatalf("event not enqueued correctly: %+v", fq.events)
	}
	if fq.events[0].TS.IsZero() {
		t.Fatal("timestamp was not set")
	}
}

func TestPostEventValidation(t *testing.T) {
	srv := newServer(&fakeQueue{}, &fakeStats{})
	defer srv.Close()

	cases := []struct {
		name string
		body string
	}{
		{"invalid json", `not json`},
		{"missing event_id", `{"name":"x"}`},
		{"missing name", `{"event_id":"e1"}`},
	}
	for _, tc := range cases {
		resp := post(t, srv.URL, tc.body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", tc.name, resp.StatusCode)
		}
	}
}

func TestPostEventBackpressure(t *testing.T) {
	srv := newServer(&fakeQueue{err: queue.ErrFull}, &fakeStats{})
	defer srv.Close()

	resp := post(t, srv.URL, `{"event_id":"e1","name":"x"}`)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when queue is full", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("503 must carry a Retry-After header")
	}
}

func TestGetCount(t *testing.T) {
	srv := newServer(&fakeQueue{}, &fakeStats{count: 42})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/counts?event=buy_click")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestGetCountRequiresEventParam(t *testing.T) {
	srv := newServer(&fakeQueue{}, &fakeStats{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/counts")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestGetStats(t *testing.T) {
	srv := newServer(&fakeQueue{}, &fakeStats{count: 7})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/stats")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}
