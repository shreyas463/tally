// Command tally is the Tally service: it receives events over HTTP, queues
// them, batch-writes them to Postgres, and answers count queries.
//
// Two queue backends (QUEUE env var):
//
//	memory  (default) — fast in-process channel; simplest to run.
//	kafka             — durable broker (Redpanda/Kafka); queued events
//	                    survive restarts, and ingest/worker can run as
//	                    SEPARATE processes (MODE=ingest / MODE=worker),
//	                    which is what the chaos demo kills and revives.
//
// Data path:  HTTP ingest -> queue -> workers -> Postgres (batch + rollup).
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
	"strings"
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
	queueBackend  string // memory | kafka
	mode          string // all | ingest | worker  (kafka only)
	queueSize     int
	batchSize     int
	flushInterval time.Duration
	workers       int
	kafkaBrokers  []string
	kafkaTopic    string
	kafkaGroup    string
}

func loadConfig() config {
	cfg := config{
		addr:          getenv("ADDR", ":8080"),
		databaseURL:   getenv("DATABASE_URL", "postgres://tally:tally@localhost:5432/tally?sslmode=disable"),
		queueBackend:  getenv("QUEUE", "memory"),
		mode:          getenv("MODE", "all"),
		queueSize:     getenvInt("QUEUE_SIZE", 100_000),
		batchSize:     getenvInt("BATCH_SIZE", 1000),
		flushInterval: getenvDuration("FLUSH_INTERVAL", 200*time.Millisecond),
		workers:       getenvInt("WORKERS", 4),
		kafkaBrokers:  strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ","),
		kafkaTopic:    getenv("KAFKA_TOPIC", "tally.events"),
		kafkaGroup:    getenv("KAFKA_GROUP", "tally-workers"),
	}
	if cfg.queueBackend != "memory" && cfg.queueBackend != "kafka" {
		log.Fatalf("QUEUE must be 'memory' or 'kafka', got %q", cfg.queueBackend)
	}
	if cfg.mode != "all" && cfg.mode != "ingest" && cfg.mode != "worker" {
		log.Fatalf("MODE must be 'all', 'ingest' or 'worker', got %q", cfg.mode)
	}
	if cfg.queueBackend == "memory" && cfg.mode != "all" {
		log.Fatalf("MODE=%s requires QUEUE=kafka (the memory queue cannot span processes)", cfg.mode)
	}
	return cfg
}

// onFlush feeds worker results into the metrics.
func onFlush(fi worker.FlushInfo) {
	metrics.BatchSize.Observe(float64(fi.BatchSize))
	metrics.FlushDuration.Observe(fi.Took.Seconds())
	if fi.Err != nil {
		metrics.EventsDropped.Add(float64(fi.BatchSize))
		log.Printf("flush FAILED: batch=%d err=%v", fi.BatchSize, fi.Err)
		return
	}
	metrics.EventsInserted.Add(float64(fi.Inserted))
	metrics.EventsDuplicate.Add(float64(fi.Duplicates))
}

func main() {
	cfg := loadConfig()

	st, err := store.New(context.Background(), cfg.databaseURL)
	if err != nil {
		log.Fatalf("connecting to postgres: %v", err)
	}
	defer st.Close()

	// consumeCtx cancels the kafka consumer on shutdown.
	consumeCtx, stopConsuming := context.WithCancel(context.Background())
	defer stopConsuming()

	var (
		enq         ingest.Enqueuer = queue.Reject{} // worker-only default
		shutdownFns []func()                         // run in order on shutdown
		consumerErr = make(chan error, 1)
	)

	switch cfg.queueBackend {
	case "memory":
		q := queue.NewMemory(cfg.queueSize)
		pool := worker.New(q.Events(), st, worker.Config{
			Workers:       cfg.workers,
			BatchSize:     cfg.batchSize,
			FlushInterval: cfg.flushInterval,
			OnFlush:       onFlush,
		})
		pool.Start()
		enq = q

		depthDone := make(chan struct{})
		go sampleDepth(q.Depth, depthDone)

		shutdownFns = append(shutdownFns, func() {
			log.Println("shutting down: draining queue...")
			close(depthDone)
			q.Close()
			pool.Wait()
		})

	case "kafka":
		if cfg.mode == "all" || cfg.mode == "ingest" {
			producer, err := queue.NewKafkaProducer(cfg.kafkaBrokers, cfg.kafkaTopic, cfg.queueSize)
			if err != nil {
				log.Fatalf("connecting kafka producer: %v", err)
			}
			enq = producer
			shutdownFns = append(shutdownFns, func() {
				log.Println("shutting down: flushing producer...")
				producer.Close()
			})
		}
		if cfg.mode == "all" || cfg.mode == "worker" {
			consumer, err := queue.NewKafkaConsumer(cfg.kafkaBrokers, cfg.kafkaTopic, cfg.kafkaGroup, cfg.batchSize)
			if err != nil {
				log.Fatalf("connecting kafka consumer: %v", err)
			}
			go func() { consumerErr <- consumer.Run(consumeCtx, st, onFlush) }()
			shutdownFns = append(shutdownFns, func() {
				log.Println("shutting down: stopping consumer (uncommitted work will be redelivered)...")
				stopConsuming()
				select {
				case <-consumerErr:
				case <-time.After(15 * time.Second):
					log.Println("consumer did not stop in time")
				}
			})
		}
	}

	// HTTP surface (all modes serve queries, metrics, dashboard).
	mux := http.NewServeMux()
	ingest.New(enq, st).Register(mux)
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
		log.Printf("tally listening on %s (queue=%s mode=%s batch=%d flush=%s workers=%d)",
			cfg.addr, cfg.queueBackend, cfg.mode, cfg.batchSize, cfg.flushInterval, cfg.workers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Graceful shutdown. Order matters — this is what guarantees accepted
	// events are never lost:
	//   1. stop the HTTP server  -> no new events arrive
	//   2. run backend shutdown  -> memory: drain queue + workers;
	//                               kafka: flush producer, stop consumer
	//   3. close the database pool (deferred)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("shutting down: draining requests...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful http shutdown failed: %v", err)
	}

	for _, fn := range shutdownFns {
		fn()
	}
	log.Println("bye")
}

// sampleDepth publishes the queue depth gauge once a second.
func sampleDepth(depth func() int, done <-chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			metrics.QueueDepth.Set(float64(depth()))
		case <-done:
			return
		}
	}
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
