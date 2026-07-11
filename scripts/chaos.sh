#!/usr/bin/env bash
# Chaos demo: prove that killing a worker mid-batch loses ZERO events.
#
# What it does:
#   1. starts an ingest-only process and a worker-only process (kafka mode)
#   2. fires a known number of events at the ingest API
#   3. kills the worker with SIGKILL (no chance to clean up) mid-stream
#   4. starts a replacement worker
#   5. compares events sent vs events stored — they must match exactly
#
# Why it works: worker offsets are committed only AFTER a batch is in
# Postgres, so the killed worker's in-flight batch is redelivered to the new
# worker; the idempotent insert absorbs any overlap. See docs/adr/0003.
#
# Prerequisites (run these first):
#   docker compose --profile kafka up -d     # postgres + redis + redpanda
#   make migrate
#   go build -o bin/tally ./cmd/tally && go build -o bin/loadgen ./cmd/loadgen

set -euo pipefail
cd "$(dirname "$0")/.."

EVENTS=${EVENTS:-20000}
RATE=${RATE:-2000}
INGEST_ADDR=:8080
WORKER_ADDR=:8081

say() { printf '\n\033[1;36m%s\033[0m\n' "$*"; }

cleanup() {
  kill "${INGEST_PID:-}" "${WORKER_PID:-}" "${WORKER2_PID:-}" 2>/dev/null || true
}
trap cleanup EXIT

say "1/6 starting ingest-only instance on $INGEST_ADDR"
QUEUE=kafka MODE=ingest ADDR=$INGEST_ADDR ./bin/tally &
INGEST_PID=$!

say "2/6 starting worker-only instance on $WORKER_ADDR"
QUEUE=kafka MODE=worker ADDR=$WORKER_ADDR ./bin/tally &
WORKER_PID=$!
sleep 3

say "3/6 sending $EVENTS events at ~$RATE/sec (chaos_test only, unique ids)"
DURATION=$(( EVENTS / RATE ))s
./bin/loadgen -rate "$RATE" -duration "$DURATION" -event chaos_test -exact &
LOADGEN_PID=$!

sleep 3
say "4/6 KILLING the worker mid-stream (SIGKILL — no graceful anything)"
kill -9 "$WORKER_PID"

sleep 2
say "5/6 starting a replacement worker"
QUEUE=kafka MODE=worker ADDR=$WORKER_ADDR ./bin/tally &
WORKER2_PID=$!

wait "$LOADGEN_PID"
SENT=$(cat /tmp/tally_loadgen_sent 2>/dev/null || echo "?")

say "waiting for the replacement worker to drain the backlog..."
sleep 10

STORED=$(curl -s "http://localhost:8080/v1/counts?event=chaos_test" | sed 's/.*"count_today"://; s/}.*//')

say "6/6 RESULT: sent=$SENT stored=$STORED"
if [ "$SENT" = "$STORED" ]; then
  printf '\033[1;32mNO EVENTS LOST. Worker died mid-batch; broker redelivered; idempotent insert deduped.\033[0m\n'
else
  printf '\033[1;31mMISMATCH — investigate (did the drain need more time? rerun the count query).\033[0m\n'
  exit 1
fi
