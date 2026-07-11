package ratelimit

import (
	"net/http/httptest"
	"testing"
)

func TestMemoryBurstThenDeny(t *testing.T) {
	// 1 event/sec with a burst of 3: the first 3 pass, the 4th is denied.
	m := NewMemory(1, 3)
	for i := 1; i <= 3; i++ {
		if !m.Allow("key-a") {
			t.Fatalf("request %d should be allowed (within burst)", i)
		}
	}
	if m.Allow("key-a") {
		t.Fatal("request 4 should be denied (burst exhausted)")
	}
}

func TestMemoryKeysAreIndependent(t *testing.T) {
	m := NewMemory(1, 1)
	if !m.Allow("key-a") {
		t.Fatal("key-a first request should pass")
	}
	if m.Allow("key-a") {
		t.Fatal("key-a second request should be denied")
	}
	// A different client is unaffected by key-a's exhaustion.
	if !m.Allow("key-b") {
		t.Fatal("key-b must not be affected by key-a's limit")
	}
}

func TestClientKey(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/events", nil)
	r.RemoteAddr = "203.0.113.9:51234"
	if got := ClientKey(r); got != "203.0.113.9" {
		t.Fatalf("ClientKey = %q, want IP without port", got)
	}

	r.Header.Set("X-API-Key", "team-42")
	if got := ClientKey(r); got != "team-42" {
		t.Fatalf("ClientKey = %q, want the API key to win over IP", got)
	}
}
