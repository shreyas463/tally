// Package ingest is Tally's front door: the HTTP handlers that receive events
// and answer count questions.
//
// The ingest path never touches the database. It validates the event, drops
// it on the queue, and returns 202 immediately — that is what keeps the API
// fast no matter how slow storage is at that moment. If the queue is full it
// answers 503 + Retry-After (backpressure) instead of losing the event.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/shreyas463/tally/internal/queue"
	"github.com/shreyas463/tally/internal/store"
)

// Enqueuer is where accepted events go (the queue).
type Enqueuer interface {
	Enqueue(e store.Event) error
}

// StatsStore is the read side used by the query endpoints.
type StatsStore interface {
	CountToday(ctx context.Context, name string) (int64, error)
	TotalsToday(ctx context.Context) ([]store.NameCount, error)
	Series(ctx context.Context, window time.Duration) ([]store.MinutePoint, error)
}

// Handler wires the HTTP routes to the queue (writes) and store (reads).
type Handler struct {
	queue Enqueuer
	stats StatsStore
}

// New builds a Handler.
func New(q Enqueuer, s StatsStore) *Handler { return &Handler{queue: q, stats: s} }

// Register attaches Tally's routes to the given mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/events", h.postEvent)
	mux.HandleFunc("GET /v1/counts", h.getCount)
	mux.HandleFunc("GET /v1/stats", h.getStats)
	mux.HandleFunc("GET /healthz", h.health)
}

type eventRequest struct {
	EventID    string         `json:"event_id"`
	Name       string         `json:"name"`
	DistinctID string         `json:"distinct_id"`
	Properties map[string]any `json:"properties"`
}

// postEvent receives one event, validates it, and queues it.
func (h *Handler) postEvent(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB cap

	var req eventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.EventID == "" {
		http.Error(w, "event_id is required", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	e := store.Event{
		EventID:    req.EventID,
		Name:       req.Name,
		DistinctID: req.DistinctID,
		Properties: req.Properties,
		TS:         time.Now().UTC(),
	}

	switch err := h.queue.Enqueue(e); {
	case err == nil:
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
	case errors.Is(err, queue.ErrFull):
		// Backpressure: tell the caller to slow down and retry, rather than
		// silently dropping their event.
		w.Header().Set("Retry-After", "1")
		http.Error(w, "overloaded, retry shortly", http.StatusServiceUnavailable)
	case errors.Is(err, queue.ErrClosed):
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
	default:
		http.Error(w, "failed to accept event", http.StatusInternalServerError)
	}
}

// getCount answers "how many <event> happened today?"
func (h *Handler) getCount(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("event")
	if name == "" {
		http.Error(w, "the 'event' query parameter is required", http.StatusBadRequest)
		return
	}
	n, err := h.stats.CountToday(r.Context(), name)
	if err != nil {
		http.Error(w, "failed to query count", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"event": name, "count_today": n})
}

// getStats returns today's totals plus a recent per-minute series — the data
// behind the live dashboard.
func (h *Handler) getStats(w http.ResponseWriter, r *http.Request) {
	totals, err := h.stats.TotalsToday(r.Context())
	if err != nil {
		http.Error(w, "failed to query totals", http.StatusInternalServerError)
		return
	}
	series, err := h.stats.Series(r.Context(), 15*time.Minute)
	if err != nil {
		http.Error(w, "failed to query series", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"totals_today": totals,
		"series":       series,
	})
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
