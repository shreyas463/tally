// Package store is Tally's data layer: it turns events into rows in Postgres
// and answers count queries. Everything that touches the database lives here.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shreyas463/tally/internal/hll"
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

	// One transaction covers the event insert + rollups (the CTE) AND the
	// unique-user sketches, so a crash can never leave the two disagreeing.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var inserted int64
	if err := tx.QueryRow(ctx, insertBatchSQL,
		ids, names, distinctIDs, props, timestamps,
	).Scan(&inserted); err != nil {
		return 0, err
	}

	if err := updateUniques(ctx, tx, events); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return inserted, nil
}

// uniqueKey identifies one sketch row: an event name on a UTC day.
type uniqueKey struct {
	name string
	day  string // YYYY-MM-DD
}

// updateUniques folds the batch's distinct_ids into the per-(name, day)
// HyperLogLog sketches.
//
// Concurrency pattern (each step matters):
//  1. ensure the row exists (INSERT .. DO NOTHING) — otherwise two
//     transactions could both see "no row", both insert-or-update, and one
//     sketch would silently overwrite the other (lost update);
//  2. SELECT .. FOR UPDATE to take the row lock;
//  3. merge in Go, write back with a plain UPDATE.
//
// Keys are processed in sorted order so concurrent batches always take row
// locks in the same order — same deadlock-avoidance rule the batch insert
// itself follows (see the ORDER BY comments in insertBatchSQL).
func updateUniques(ctx context.Context, tx pgx.Tx, events []Event) error {
	groups := make(map[uniqueKey][]string)
	for _, e := range events {
		if e.DistinctID == "" {
			continue // no user attached to this event
		}
		k := uniqueKey{name: e.Name, day: e.TS.UTC().Format("2006-01-02")}
		groups[k] = append(groups[k], e.DistinctID)
	}
	if len(groups) == 0 {
		return nil
	}

	keys := make([]uniqueKey, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}
		return keys[i].day < keys[j].day
	})

	empty := hll.New().Bytes()
	for _, k := range keys {
		if _, err := tx.Exec(ctx,
			`INSERT INTO event_uniques_day (name, day, sketch) VALUES ($1, $2::date, $3)
			 ON CONFLICT (name, day) DO NOTHING`,
			k.name, k.day, empty,
		); err != nil {
			return err
		}

		var raw []byte
		if err := tx.QueryRow(ctx,
			`SELECT sketch FROM event_uniques_day WHERE name = $1 AND day = $2::date FOR UPDATE`,
			k.name, k.day,
		).Scan(&raw); err != nil {
			return err
		}
		sketch, err := hll.FromBytes(raw)
		if err != nil {
			return fmt.Errorf("corrupt sketch for %s/%s: %w", k.name, k.day, err)
		}

		for _, id := range groups[k] {
			sketch.Add(id)
		}

		if _, err := tx.Exec(ctx,
			`UPDATE event_uniques_day SET sketch = $3 WHERE name = $1 AND day = $2::date`,
			k.name, k.day, sketch.Bytes(),
		); err != nil {
			return err
		}
	}
	return nil
}

// UniquesToday estimates how many distinct users produced the named event
// today (UTC). Returns 0 when the event hasn't been seen today.
func (s *Store) UniquesToday(ctx context.Context, name string) (uint64, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx,
		`SELECT sketch FROM event_uniques_day WHERE name = $1 AND day = current_date`,
		name,
	).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	sketch, err := hll.FromBytes(raw)
	if err != nil {
		return 0, err
	}
	return sketch.Estimate(), nil
}

// UniquesTodayAll returns today's unique-user estimate for every event name.
func (s *Store) UniquesTodayAll(ctx context.Context) ([]NameCount, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, sketch FROM event_uniques_day WHERE day = current_date`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []NameCount{}
	for rows.Next() {
		var name string
		var raw []byte
		if err := rows.Scan(&name, &raw); err != nil {
			return nil, err
		}
		sketch, err := hll.FromBytes(raw)
		if err != nil {
			return nil, fmt.Errorf("corrupt sketch for %s: %w", name, err)
		}
		out = append(out, NameCount{Name: name, Count: int64(sketch.Estimate())})
	}
	return out, rows.Err()
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
