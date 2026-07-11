// Command loadgen is a fake-traffic generator. It pretends to be many users
// and fires events at Tally, then reports throughput and latency percentiles.
// For heavier, scriptable testing use the k6 script in loadtest/ instead.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	target := flag.String("target", "http://localhost:8080/v1/events", "ingest endpoint")
	rate := flag.Int("rate", 1000, "events per second to aim for")
	duration := flag.Duration("duration", 10*time.Second, "how long to run")
	workers := flag.Int("workers", 50, "number of concurrent senders")
	eventName := flag.String("event", "", "send only this event name (default: a random mix)")
	dupes := flag.Int("dupes", 0, "percent of events to send twice (proves idempotency)")
	exact := flag.Bool("exact", false, "write the sent count to /tmp/tally_loadgen_sent (used by chaos.sh)")
	flag.Parse()

	names := []string{"buy_click", "page_view", "signup", "song_play", "add_to_cart"}
	if *eventName != "" {
		names = []string{*eventName}
	}

	var sent, failed, retried int64
	var mu sync.Mutex
	var latencies []time.Duration

	jobs := make(chan struct{}, *rate)
	client := &http.Client{Timeout: 5 * time.Second}

	send := func(body []byte) bool {
		start := time.Now()
		resp, err := client.Post(*target, "application/json", bytes.NewReader(body))
		took := time.Since(start)
		if err != nil {
			atomic.AddInt64(&failed, 1)
			return false
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusServiceUnavailable {
			// Backpressure: honor it with one quick retry.
			atomic.AddInt64(&retried, 1)
			time.Sleep(200 * time.Millisecond)
			resp2, err := client.Post(*target, "application/json", bytes.NewReader(body))
			if err != nil {
				atomic.AddInt64(&failed, 1)
				return false
			}
			resp2.Body.Close()
			if resp2.StatusCode >= 300 {
				atomic.AddInt64(&failed, 1)
				return false
			}
		} else if resp.StatusCode >= 300 {
			atomic.AddInt64(&failed, 1)
			return false
		}
		atomic.AddInt64(&sent, 1)
		mu.Lock()
		latencies = append(latencies, took)
		mu.Unlock()
		return true
	}

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for range jobs {
				id := fmt.Sprintf("%d-%d", time.Now().UnixNano(), rng.Int63())
				body, _ := json.Marshal(map[string]any{
					"event_id":    id,
					"name":        names[rng.Intn(len(names))],
					"distinct_id": fmt.Sprintf("user_%d", rng.Intn(10000)),
					"properties":  map[string]any{"source": "loadgen"},
				})
				if send(body) && *dupes > 0 && rng.Intn(100) < *dupes {
					// Send the exact same event again. The duplicate is
					// accepted (202) but must NOT increase any count.
					send(body)
					atomic.AddInt64(&sent, -1) // count unique events only
				}
			}
		}(int64(i) + time.Now().UnixNano())
	}

	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()
	deadline := time.After(*duration)
	start := time.Now()

	log.Printf("firing ~%d events/sec at %s for %s", *rate, *target, *duration)
loop:
	for {
		select {
		case <-deadline:
			break loop
		case <-ticker.C:
			select {
			case jobs <- struct{}{}:
			default: // senders saturated — skip this tick
			}
		}
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)

	// Report.
	s := atomic.LoadInt64(&sent)
	log.Printf("done in %s: sent=%d failed=%d backpressure_retries=%d achieved=%.0f events/sec",
		elapsed.Round(time.Millisecond), s, atomic.LoadInt64(&failed),
		atomic.LoadInt64(&retried), float64(s)/elapsed.Seconds())

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		pct := func(p float64) time.Duration {
			idx := int(p * float64(len(latencies)-1))
			return latencies[idx]
		}
		log.Printf("latency: p50=%s p95=%s p99=%s max=%s",
			pct(0.50).Round(time.Microsecond), pct(0.95).Round(time.Microsecond),
			pct(0.99).Round(time.Microsecond), latencies[len(latencies)-1].Round(time.Microsecond))
	}

	if *exact {
		_ = os.WriteFile("/tmp/tally_loadgen_sent", []byte(fmt.Sprintf("%d", s)), 0o644)
	}
}
