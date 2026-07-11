// Package store is Tally's data layer: it turns events into rows in Postgres
// and answers count queries. Everything that touches the database lives here.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is one thing that happened — a click, a view, a purchase.
type Event struct {
	EventID    string         `json:"event_id"`    // unique id; duplicates are ignored
	Name       string         `json:"name"`        // e.g. "buy_click"
	DistinctID string         `json:"distinct_id"` // who did it, e.g. "user_42"
	Properties map[string]any `json:"properties"`  // any extra details
	TS         time.Time      `json:"ts"`          // when it happened
}

// NameCount is a total for one event name.
type NameCount struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

// MinutePoint is the count for one event name in one minute bucket.
type MinutePoint struct {
	Name   string    `json:"name"`
	Minute time.Time `json:"minute"`
	Count  int64     `json:"count"`
}

// Store holds a pool of database connections.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres and verifies the connection works. Because
// Postgres may still be starting up (common under Docker/Kubernetes), it
// retries the first connection for up to ~30s instead of failing instantly.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		lastErr = pool.Ping(pingCtx)
		cancel()
		if lastErr == nil {
			return &Store{pool: pool}, nil
		}
		if attempt == 1 {
			log.Printf("store: waiting for postgres to become ready...")
		}
		time.Sleep(time.Second)
	}
	pool.Close()
	return nil, fmt.Errorf("postgres not reachable after retries: %w", lastErr)
}

// Close releases all database connections.
func (s *Store) Close() { s.pool.Close() }

// insertBatchSQL inserts a whole batch and updates the per-minute rollups in
// ONE atomic statement:
//
//   - "incoming" unpacks the arrays into rows,
//   - "inserted" writes the raw events, silently skipping duplicates
//     (ON CONFLICT on event_id — this is the idempotency guarantee),
//   - "rolled" increments the per-minute counters, but only for rows that
//     were ACTUALLY inserted — so a replayed batch can never double-count,
//   - the final SELECT reports how many rows were new.
//
// Because it is a single statement, it is atomic: a crash mid-batch leaves
// either both tables updated or neither. There is no window where raw events
// and rollup counts disagree.
const insertBatchSQL = `
WITH incoming AS (
    SELECT * FROM unnest(
        $1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[]
    ) AS t(event_id, name, distinct_id, properties, ts)
),
inserted AS (
    INSERT INTO events (event_id, name, distinct_id, properties, ts)
    SELECT event_id, name, distinct_id, properties::jsonb, ts
    FROM incoming
    -- Same reasoning as the rollup below: order the inserts by event_id so
    -- concurrent batches that share duplicate ids take the unique-index locks
    -- in the same order and queue instead of deadlocking.
    ORDER BY event_id
    ON CONFLICT (event_id) DO NOTHING
    RETURNING name, ts
),
rolled AS (
    INSERT INTO event_counts_minute (name, minute, n)
    SELECT name, date_trunc('minute', ts), count(*)
    FROM inserted
    GROUP BY 1, 2
    -- Deterministic lock order (name, minute). Without this, two worker
    -- batches touching the same minute buckets can grab the per-row locks
    -- in opposite orders and deadlock under concurrency — which a load test
    -- reproduced. Ordering the upsert makes every batch take locks in the
    -- same order, so they queue instead of deadlocking.
    ORDER BY 1, 2
    ON CONFLICT (name, minute)
    DO UPDATE SET n = event_counts_minute.n + EXCLUDED.n
)
SELECT (SELECT count(*) FROM inserted)`

// InsertBatch stores a batch of events and updates the rollup counts in one
// atomic, idempotent statement. It returns how many events were newly
// inserted (batch size minus duplicates).
func (s *Store) InsertBatch(ctx context.Context, events []Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}

	ids := make([]string, len(events))
	names := make([]string, len(events))
	distinctIDs := make([]string, len(events))
	props := make([]string, len(events))
	timestamps := make([]time.Time, len(events))

	for i, e := range events {
		if e.Properties == nil {
			e.Properties = map[string]any{}
		}
		p, err := json.Marshal(e.Properties)
		if err != nil {
			return 0, err
		}
		ids[i] = e.EventID
		names[i] = e.Name
		distinctIDs[i] = e.DistinctID
		props[i] = string(p)
		timestamps[i] = e.TS
	}

	var inserted int64
	err := s.pool.QueryRow(ctx, insertBatchSQL,
		ids, names, distinctIDs, props, timestamps,
	).Scan(&inserted)
	return inserted, err
}

// CountToday returns how many events with the given name happened today (UTC),
// read from the rollup table so it is fast no matter how many raw events exist.
func (s *Store) CountToday(ctx context.Context, name string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(sum(n), 0)::bigint
		 FROM event_counts_minute
		 WHERE name = $1 AND minute >= date_trunc('day', now())`,
		name,
	).Scan(&n)
	return n, err
}

// TotalsToday returns today's count for every event name, biggest first.
func (s *Store) TotalsToday(ctx context.Context) ([]NameCount, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, sum(n)::bigint
		 FROM event_counts_minute
		 WHERE minute >= date_trunc('day', now())
		 GROUP BY name
		 ORDER BY 2 DESC
		 LIMIT 20`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []NameCount{}
	for rows.Next() {
		var nc NameCount
		if err := rows.Scan(&nc.Name, &nc.Count); err != nil {
			return nil, err
		}
		out = append(out, nc)
	}
	return out, rows.Err()
}

// Series returns per-minute counts for the last `window` of time, oldest
// first — the data behind the dashboard's live chart.
func (s *Store) Series(ctx context.Context, window time.Duration) ([]MinutePoint, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, minute, n
		 FROM event_counts_minute
		 WHERE minute >= date_trunc('minute', now()) - make_interval(mins => $1)
		 ORDER BY minute ASC, name ASC`,
		int(window.Minutes()),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []MinutePoint{}
	for rows.Next() {
		var mp MinutePoint
		if err := rows.Scan(&mp.Name, &mp.Minute, &mp.Count); err != nil {
			return nil, err
		}
		out = append(out, mp)
	}
	return out, rows.Err()
}
