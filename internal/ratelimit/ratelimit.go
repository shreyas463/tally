// Package ratelimit protects Tally from being overwhelmed by any single client.
//
// Phase 4: a token-bucket limiter backed by Redis, keyed per API key. Over-limit
// requests get a 429 with a Retry-After header instead of being silently dropped
// or taking the whole service down.
//
// Empty for now — this file marks where that work goes.
package ratelimit
