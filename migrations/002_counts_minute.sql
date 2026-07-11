-- Phase 1: per-minute rollup counts.
--
-- Workers tally each batch into this table as they insert, so "how many X
-- today?" is answered by summing a few hundred small rows instead of scanning
-- millions of raw events. Counts are incremented only for rows that were
-- actually inserted (duplicates excluded), so totals stay exactly correct.

CREATE TABLE IF NOT EXISTS event_counts_minute (
    name   TEXT        NOT NULL,
    minute TIMESTAMPTZ NOT NULL,
    n      BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (name, minute)
);

CREATE INDEX IF NOT EXISTS idx_counts_minute_minute ON event_counts_minute (minute);
