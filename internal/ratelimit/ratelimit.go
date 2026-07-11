// Package ratelimit protects Tally from any single client hogging the whole
// service. Each client (API key, or IP as a fallback) gets a token bucket:
// steady traffic up to N events/sec with short bursts allowed; beyond that
// the ingest API answers 429 + Retry-After instead of degrading for everyone.
//
// Two implementations:
//
//	Memory — per-key token buckets (golang.org/x/time/rate). Right answer
//	         for a single instance; zero dependencies.
//	Redis  — fixed 1-second window shared across instances. Coarser than a
//	         token bucket, but limits are enforced globally when several
//	         ingest replicas run behind a load balancer (see ADR 0004).
package ratelimit

import (
	"context"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

// Limiter answers one question: may this key send one more event right now?
type Limiter interface {
	Allow(key string) bool
}

// ClientKey identifies the caller: the X-API-Key header if present,
// otherwise the client IP.
func ClientKey(r *http.Request) string {
	if k := r.Header.Get("X-API-Key"); k != "" {
		return k
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ---- In-memory token buckets ----

type entry struct {
	lim  *rate.Limiter
	last time.Time
}

// Memory holds one token bucket per key.
type Memory struct {
	mu      sync.Mutex
	rps     rate.Limit
	burst   int
	buckets map[string]*entry
}

// NewMemory allows rps events/sec per key with the given burst. A janitor
// evicts buckets idle for 10+ minutes so the map cannot grow forever.
func NewMemory(rps float64, burst int) *Memory {
	m := &Memory{rps: rate.Limit(rps), burst: burst, buckets: make(map[string]*entry)}
	go m.janitor()
	return m
}

// Allow takes one token from key's bucket, creating it on first sight.
func (m *Memory) Allow(key string) bool {
	m.mu.Lock()
	e, ok := m.buckets[key]
	if !ok {
		e = &entry{lim: rate.NewLimiter(m.rps, m.burst)}
		m.buckets[key] = e
	}
	e.last = time.Now()
	m.mu.Unlock()
	return e.lim.Allow()
}

func (m *Memory) janitor() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for range t.C {
		cutoff := time.Now().Add(-10 * time.Minute)
		m.mu.Lock()
		for k, e := range m.buckets {
			if e.last.Before(cutoff) {
				delete(m.buckets, k)
			}
		}
		m.mu.Unlock()
	}
}

// ---- Redis fixed window (shared across instances) ----

// allowScript atomically counts this key's requests in the current 1s window.
// KEYS[1] window counter, ARGV[1] limit. Returns 1 if allowed.
const allowScript = `
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('PEXPIRE', KEYS[1], 1999)
end
if n <= tonumber(ARGV[1]) then
  return 1
end
return 0`

// Redis enforces rps per key across ALL ingest instances.
type Redis struct {
	cl     *redis.Client
	limit  int
	script *redis.Script
}

// NewRedis connects and verifies with a ping.
func NewRedis(addr string, rps int) (*Redis, error) {
	cl := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cl.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return &Redis{cl: cl, limit: rps, script: redis.NewScript(allowScript)}, nil
}

// Allow counts one request in the current window. On Redis failure it FAILS
// OPEN — losing rate limiting briefly beats rejecting all traffic.
func (r *Redis) Allow(key string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	windowKey := "rl:" + key + ":" + time.Now().UTC().Format("15:04:05")
	n, err := r.script.Run(ctx, r.cl, []string{windowKey}, r.limit).Int()
	if err != nil {
		log.Printf("ratelimit: redis error (failing open): %v", err)
		return true
	}
	return n == 1
}
