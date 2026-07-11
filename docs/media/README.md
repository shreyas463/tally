# Media

Screenshots and demo GIFs for the README live here.

## What to capture (once Phases 3–4 are done)

1. **`demo.gif`** — the dashboard's totals ticking up live while the load test runs.
2. **`loadtest.gif`** — the k6 run in the terminal showing thousands of events/sec.
3. **`chaos.gif`** — killing a worker mid-batch, then showing the final count is
   still correct (no events lost).

## How to record (macOS)

- Record the screen with [Kap](https://getkap.co/) (free) or QuickTime.
- Export/convert to GIF (Kap exports GIF directly; or use `ffmpeg` + `gifski`).
- Drop the files in this folder and uncomment the image line at the top of the
  main `README.md`.
