# Media

Diagrams and demo recordings for the README live here.

## Already here

- **`hll-accuracy.svg`** — HyperLogLog estimate vs exact counts, generated from
  `internal/hll` (static, renders on GitHub). Regenerate by running a small
  program that imports the `hll` package; the accuracy it shows is asserted by
  `internal/hll/hll_test.go`.

## GIFs to record

Filenames the README expects (uncomment the matching `<!-- ![...] -->` lines in
`README.md` once each file is here):

1. **`dashboard.gif`** — the dashboard's totals and per-minute chart moving live
   while a load test runs.
2. **`loadtest.gif`** — `k6 run loadtest/ingest.js` in the terminal, showing
   throughput and latency percentiles.
3. **`chaos.gif`** — `make chaos`: a worker is `SIGKILL`ed mid-batch, then the
   final count is shown to still be exact (no events lost).

## How to record (macOS)

- Record the screen with [Kap](https://getkap.co/) (free) or QuickTime.
- Export/convert to GIF (Kap exports GIF directly; or use `ffmpeg` + `gifski`).
- Drop the files in this folder and uncomment the matching line in `README.md`.
