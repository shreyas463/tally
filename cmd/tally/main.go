// Command tally is the Tally service: it receives events over HTTP, stores
// them in Postgres, and answers count queries.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shreyas463/tally/internal/ingest"
	"github.com/shreyas463/tally/internal/store"
)

func main() {
	dsn := getenv("DATABASE_URL", "postgres://tally:tally@localhost:5432/tally?sslmode=disable")
	addr := getenv("ADDR", ":8080")

	// Connect to Postgres.
	st, err := store.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("connecting to postgres: %v", err)
	}
	defer st.Close()

	// Wire up the HTTP routes.
	mux := http.NewServeMux()
	ingest.New(st).Register(mux)

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Start the server in the background.
	go func() {
		log.Printf("tally listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Wait for Ctrl+C or SIGTERM, then shut down gracefully so in-flight
	// requests get a chance to finish instead of being cut off.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
	log.Println("bye")
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
