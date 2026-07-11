// k6 load test for Tally's ingest endpoint.
//
// Install k6: https://k6.io/docs/get-started/installation/
// Run:        k6 run loadtest/ingest.js
//
// This ramps traffic up to 5,000 events/sec and reports throughput and latency.
// Bump the `target` values as you optimize and record the numbers in BENCHMARKS.md.

import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: 500,
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 1000,
      stages: [
        { target: 2000, duration: '30s' },
        { target: 5000, duration: '1m' },
        { target: 5000, duration: '1m' },
      ],
    },
  },
};

const names = ['buy_click', 'page_view', 'signup', 'song_play', 'add_to_cart'];

export default function () {
  const body = JSON.stringify({
    event_id: `${Date.now()}-${Math.random()}`,
    name: names[Math.floor(Math.random() * names.length)],
    distinct_id: `user_${Math.floor(Math.random() * 10000)}`,
    properties: { source: 'k6' },
  });

  const res = http.post('http://localhost:8080/v1/events', body, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, { 'status is 202': (r) => r.status === 202 });
}
