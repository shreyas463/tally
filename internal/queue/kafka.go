// Kafka-backed queue (Phase 2).
//
// The in-memory queue is fast but dies with the process. This backend swaps
// the buffered channel for a Kafka-compatible broker (we run Redpanda in
// dev): the ingest API produces to a topic, and workers consume it in
// batches. Because consumer offsets are committed ONLY after a batch is
// safely in Postgres, killing a worker mid-batch just means the broker
// redelivers those events to the next worker — and the store's idempotent
// insert makes the redelivery harmless. That pair of decisions is the whole
// "at-least-once delivery + idempotent consumer" story.
package queue

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/shreyas463/tally/internal/store"
	"github.com/shreyas463/tally/internal/worker"
)

// KafkaProducer implements the ingest side: Enqueue produces to the topic.
type KafkaProducer struct {
	cl     *kgo.Client
	topic  string
	maxBuf int64
}

// NewKafkaProducer connects a producer. maxBuffered bounds the in-flight
// buffer; beyond it Enqueue reports ErrFull (backpressure, same contract as
// the memory queue).
func NewKafkaProducer(brokers []string, topic string, maxBuffered int) (*KafkaProducer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.MaxBufferedRecords(maxBuffered),
		kgo.ProducerLinger(5*time.Millisecond), // tiny linger => bigger produce batches
	)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{cl: cl, topic: topic, maxBuf: int64(maxBuffered)}, nil
}

// Enqueue produces one event without blocking the request path. The broker
// client retries and acks internally (idempotent producer); a terminal
// produce failure is logged — by then the caller already got a 202, which is
// the honest cost of an async producer (documented in the ADR).
func (p *KafkaProducer) Enqueue(e store.Event) error {
	if p.cl.BufferedProduceRecords() >= p.maxBuf {
		return ErrFull
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	p.cl.Produce(context.Background(), &kgo.Record{
		Topic: p.topic,
		Key:   []byte(e.Name), // same event name -> same partition -> ordered per name
		Value: b,
	}, func(_ *kgo.Record, err error) {
		if err != nil {
			log.Printf("kafka: produce failed (event lost at broker): %v", err)
		}
	})
	return nil
}

// Close flushes everything buffered, then closes the client.
func (p *KafkaProducer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := p.cl.Flush(ctx); err != nil {
		log.Printf("kafka: flush on close: %v", err)
	}
	p.cl.Close()
}

// KafkaConsumer is the worker side: it polls batches, writes them to the
// store, and only then commits offsets.
type KafkaConsumer struct {
	cl        *kgo.Client
	batchSize int
}

// NewKafkaConsumer joins the consumer group. Auto-commit is disabled on
// purpose — committing before the insert would turn a crash into data loss.
func NewKafkaConsumer(brokers []string, topic, group string, batchSize int) (*KafkaConsumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &KafkaConsumer{cl: cl, batchSize: batchSize}, nil
}

// Run consumes until ctx is cancelled. Order per batch is strict:
// poll -> insert (retry until success) -> commit. A crash anywhere before the
// commit means redelivery, and the idempotent insert absorbs the replay.
func (c *KafkaConsumer) Run(ctx context.Context, sink worker.BatchInserter, onFlush func(worker.FlushInfo)) error {
	defer c.cl.Close()

	for {
		fetches := c.cl.PollRecords(ctx, c.batchSize)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return ctx.Err()
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("kafka: fetch error on %s/%d: %v", topic, partition, err)
		})

		recs := fetches.Records()
		if len(recs) == 0 {
			continue
		}

		// Decode; a malformed record is skipped (logged), never allowed to
		// wedge the whole partition.
		events := make([]store.Event, 0, len(recs))
		for _, r := range recs {
			var e store.Event
			if err := json.Unmarshal(r.Value, &e); err != nil {
				log.Printf("kafka: skipping malformed record at %s/%d offset %d: %v",
					r.Topic, r.Partition, r.Offset, err)
				continue
			}
			events = append(events, e)
		}

		// Insert with patient retries. We must NOT commit until this
		// succeeds; if shutdown interrupts us, the uncommitted batch is
		// simply redelivered later.
		start := time.Now()
		var inserted int64
		for attempt := 1; ; attempt++ {
			insCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			var err error
			inserted, err = sink.InsertBatch(insCtx, events)
			cancel()
			if err == nil {
				break
			}
			log.Printf("kafka worker: insert failed (attempt %d): %v", attempt, err)
			select {
			case <-ctx.Done():
				return ctx.Err() // uncommitted -> broker will redeliver
			case <-time.After(min(time.Duration(attempt)*250*time.Millisecond, 5*time.Second)):
			}
		}

		if onFlush != nil {
			onFlush(worker.FlushInfo{
				BatchSize:  len(events),
				Inserted:   inserted,
				Duplicates: int64(len(events)) - inserted,
				Took:       time.Since(start),
			})
		}

		// Commit only now. If THIS fails, the batch may be redelivered —
		// which the idempotent insert turns into counted-once anyway.
		if err := c.cl.CommitRecords(ctx, recs...); err != nil {
			log.Printf("kafka worker: offset commit failed (safe, insert is idempotent): %v", err)
		}
	}
}
