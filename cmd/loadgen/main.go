// Command loadgen is a simple fake-traffic generator. It pretends to be many
// users and fires random events at Tally so you can watch it work under load.
// For serious throughput testing, use the k6 script in loadtest/ instead.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	target := flag.String("target", "http://localhost:8080/v1/events", "ingest endpoint")
	rate := flag.Int("rate", 1000, "events per second to aim for")
	duration := flag.Duration("duration", 10*time.Second, "how long to run")
	workers := flag.Int("workers", 50, "number of concurrent senders")
	flag.Parse()

	names := []string{"buy_click", "page_view", "signup", "song_play", "add_to_cart"}

	var sent, failed int64
	jobs := make(chan struct{}, *rate)
	client := &http.Client{Timeout: 5 * time.Second}

	// Each worker pulls a "job" off the channel and sends one random event.
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				body, _ := json.Marshal(map[string]any{
					"event_id":    fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Int63()),
					"name":        names[rand.Intn(len(names))],
					"distinct_id": fmt.Sprintf("user_%d", rand.Intn(10000)),
					"properties":  map[string]any{"source": "loadgen"},
				})
				resp, err := client.Post(*target, "application/json", bytes.NewReader(body))
				if err != nil {
					atomic.AddInt64(&failed, 1)
					continue
				}
				resp.Body.Close()
				if resp.StatusCode >= 300 {
					atomic.AddInt64(&failed, 1)
				} else {
					atomic.AddInt64(&sent, 1)
				}
			}
		}()
	}

	// The ticker tries to enqueue `rate` jobs every second.
	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()
	deadline := time.After(*duration)

	log.Printf("firing ~%d events/sec at %s for %s", *rate, *target, *duration)
loop:
	for {
		select {
		case <-deadline:
			break loop
		case <-ticker.C:
			select {
			case jobs <- struct{}{}:
			default: // can't keep up right now — skip this tick
			}
		}
	}
	close(jobs)
	wg.Wait()

	log.Printf("done: sent=%d failed=%d", atomic.LoadInt64(&sent), atomic.LoadInt64(&failed))
}
