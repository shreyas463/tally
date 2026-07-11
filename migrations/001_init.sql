-- Phase 0 schema: one row per event.
-- event_id is UNIQUE so a duplicate send (retries happen) is ignored on insert,
-- which is how Tally avoids double-counting.

CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL PRIMARY KEY,
    event_id    TEXT        NOT NULL UNIQUE,
    name        TEXT        NOT NULL,
    distinct_id TEXT        NOT NULL,
    properties  JSONB       NOT NULL DEFAULT '{}',
    ts          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Speeds up "how many <name> events today" queries.
CREATE INDEX IF NOT EXISTS idx_events_name_ts ON events (name, ts);
