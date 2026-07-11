// Package ingest is Tally's front door: the HTTP handlers that receive events
// and answer count questions.
package ingest

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/shreyas463/tally/internal/store"
)

// Handler wires the HTTP routes to the store.
type Handler struct {
	store *store.Store
}

// New builds a Handler backed by the given store.
func New(s *store.Store) *Handler { return &Handler{store: s} }

// Register attaches Tally's routes to the given mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/events", h.postEvent)
	mux.HandleFunc("GET /v1/counts", h.getCount)
	mux.HandleFunc("GET /healthz", h.health)
}

type eventRequest struct {
	EventID    string         `json:"event_id"`
	Name       string         `json:"name"`
	DistinctID string         `json:"distinct_id"`
	Properties map[string]any `json:"properties"`
}

// postEvent receives one event and stores it.
func (h *Handler) postEvent(w http.ResponseWriter, r *http.Request) {
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
	if err := h.store.Insert(r.Context(), e); err != nil {
		http.Error(w, "failed to store event", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

// getCount answers "how many <event> happened today?"
func (h *Handler) getCount(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("event")
	if name == "" {
		http.Error(w, "the 'event' query parameter is required", http.StatusBadRequest)
		return
	}
	n, err := h.store.CountToday(r.Context(), name)
	if err != nil {
		http.Error(w, "failed to query count", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"event": name, "count_today": n})
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
