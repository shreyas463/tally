-- Unique-user sketches: one HyperLogLog per (event name, day).
--
-- "How many DIFFERENT people did X today?" can't come from the rollup
-- counters (those count events, not people), and COUNT(DISTINCT) over raw
-- events rescans an ever-growing table. Instead workers fold each batch's
-- distinct_ids into a fixed ~16 KB sketch, merged transactionally with the
-- batch insert. Estimates carry ~1% error by design — see docs/adr/0005.

CREATE TABLE IF NOT EXISTS event_uniques_day (
    name   TEXT  NOT NULL,
    day    DATE  NOT NULL,
    sketch BYTEA NOT NULL,
    PRIMARY KEY (name, day)
);
