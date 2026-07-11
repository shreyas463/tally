// Package dashboard serves Tally's built-in live dashboard: a single embedded
// HTML page that polls /v1/stats and renders totals and a per-minute chart.
// No build step, no node_modules — the whole UI ships inside the Go binary.
package dashboard

import (
	_ "embed"
	"net/http"
)

//go:embed index.html
var indexHTML []byte

// Register mounts the dashboard at the site root.
func Register(mux *http.ServeMux) {
	// "GET /{$}" matches ONLY "/" — it won't swallow other paths.
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
}
