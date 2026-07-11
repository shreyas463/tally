// Package metrics defines Tally's Prometheus instrumentation in one place.
//
// The four questions these answer: how much is coming in (accepted/rejected),
// how full is the belt (queue depth), how well are writes going (batch size,
// flush duration, inserted vs duplicate), and is anything being lost
// (dropped total — which must stay at zero).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Ingest side.
	EventsAccepted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tally_events_accepted_total",
		Help: "Events accepted by the ingest API and enqueued.",
	})
	EventsRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tally_events_rejected_total",
		Help: "Events rejected by the ingest API, by reason.",
	}, []string{"reason"}) // invalid | queue_full | rate_limited | shutdown
	IngestDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "tally_ingest_duration_seconds",
		Help:    "Time to validate and enqueue one event.",
		Buckets: prometheus.ExponentialBuckets(0.00005, 2, 14), // 50µs .. ~400ms
	})

	// Queue.
	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tally_queue_depth",
		Help: "Events currently waiting in the queue.",
	})

	// Worker side.
	EventsInserted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tally_events_inserted_total",
		Help: "Events newly written to storage.",
	})
	EventsDuplicate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tally_events_duplicate_total",
		Help: "Events skipped as duplicates (idempotency working as intended).",
	})
	EventsDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tally_events_dropped_total",
		Help: "Events dropped after exhausting insert retries. Should be zero.",
	})
	BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "tally_batch_size",
		Help:    "Events per flushed batch.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1 .. 2048
	})
	FlushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "tally_flush_duration_seconds",
		Help:    "Time to write one batch to storage.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14), // 0.5ms .. ~4s
	})
)
