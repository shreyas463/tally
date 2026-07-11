// Package store is Tally's data layer: it turns events into rows in Postgres
// and answers count queries. Everything that touches the database lives here.
package store

import (
	"context"
	"encoding/json"
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

// Store holds a pool of database connections.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres and verifies the connection works.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Close releases all database connections.
func (s *Store) Close() { s.pool.Close() }

// Insert stores one event. It is idempotent: sending the same event_id twice
// stores it only once, so retries can never inflate the counts.
func (s *Store) Insert(ctx context.Context, e Event) error {
	if e.Properties == nil {
		e.Properties = map[string]any{}
	}
	props, err := json.Marshal(e.Properties)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO events (event_id, name, distinct_id, properties, ts)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.Name, e.DistinctID, props, e.TS,
	)
	return err
}

// CountToday returns how many events with the given name happened today.
func (s *Store) CountToday(ctx context.Context, name string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM events WHERE name = $1 AND ts::date = current_date`,
		name,
	).Scan(&n)
	return n, err
}
