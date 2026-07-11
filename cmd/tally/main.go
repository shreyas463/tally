// Command tally is the Tally service: it receives events over HTTP, queues
// them, batch-writes them to Postgres, and answers count queries.
//
// The data path is:  HTTP ingest -> queue -> worker pool -> Postgres.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/shreyas463/tally/internal/dashboard"
	"github.com/shreyas463/tally/internal/ingest"
	"github.com/shreyas463/tally/internal/metrics"
	"github.com/shreyas463/tally/internal/queue"
	"github.com/shreyas463/tally/internal/store"
	"github.com/shreyas463/tally/internal/worker"
)

type config struct {
	addr          string
	databaseURL   string
	queueSize     int
	batchSize     int
	flushInterval time.Duration
	workers       int
}

func loadConfig() config {
	return config{
		addr:          getenv("ADDR", ":8080"),
		databaseURL:   getenv("DATABASE_URL", "postgres://tally:tally@localhost:5432/tally?sslmode=disable"),
		queueSize:     getenvInt("QUEUE_SIZE", 100_000),
		batchSize:     getenvInt("BATCH_SIZE", 1000),
		flushInterval: getenvDuration("FLUSH_INTERVAL", 200*time.Millisecond),
		workers:       getenvInt("WORKERS", 4),
	}
}

func main() {
	cfg := loadConfig()

	// Storage.
	st, err := store.New(context.Background(), cfg.databaseURL)
	if err != nil {
		log.Fatalf("connecting to postgres: %v", err)
	}
	defer st.Close()

	// The conveyor belt.
	q := queue.NewMemory(cfg.queueSize)

	// The workers draining it.
	pool := worker.New(q.Events(), st, worker.Config{
		Workers:       cfg.workers,
		BatchSize:     cfg.batchSize,
		FlushInterval: cfg.flushInterval,
		OnFlush: func(fi worker.FlushInfo) {
			metrics.BatchSize.Observe(float64(fi.BatchSize))
			metrics.FlushDuration.Observe(fi.Took.Seconds())
			if fi.Err != nil {
				metrics.EventsDropped.Add(float64(fi.BatchSize))
				log.Printf("flush FAILED: batch=%d err=%v", fi.BatchSize, fi.Err)
				return
			}
			metrics.EventsInserted.Add(float64(fi.Inserted))
			metrics.EventsDuplicate.Add(float64(fi.Duplicates))
		},
	})
	pool.Start()

	// Sample the queue depth for the tally_queue_depth gauge.
	depthDone := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				metrics.QueueDepth.Set(float64(q.Depth()))
			case <-depthDone:
				return
			}
		}
	}()

	// HTTP.
	mux := http.NewServeMux()
	ingest.New(q, st).Register(mux)
	dashboard.Register(mux)
	mux.Handle("GET /metrics", promhttp.Handler())
	// pprof, for profiling under load (see BENCHMARKS.md). The Index handler
	// also serves the named profiles (heap, goroutine, ...) under this prefix.
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		Addr:         cfg.addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("tally listening on %s (queue=%d batch=%d flush=%s workers=%d)",
			cfg.addr, cfg.queueSize, cfg.batchSize, cfg.flushInterval, cfg.workers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Graceful shutdown. Order matters — this is what guarantees accepted
	// events are never lost:
	//   1. stop the HTTP server        -> no new events can arrive
	//   2. close the queue             -> workers see the channel close
	//   3. wait for the workers        -> they drain and flush everything left
	//   4. close the database pool
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("shutting down: draining requests...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful http shutdown failed: %v", err)
	}

	log.Println("shutting down: draining queue...")
	close(depthDone)
	q.Close()
	pool.Wait()

	log.Println("bye")
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Fatalf("invalid %s: %q is not a number", key, os.Getenv(key))
	}
	return def
}

func getenvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Fatalf("invalid %s: %q is not a duration (e.g. 200ms, 1s)", key, os.Getenv(key))
	}
	return def
}
