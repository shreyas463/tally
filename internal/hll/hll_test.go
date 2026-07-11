package hll

import (
	"fmt"
	"math"
	"testing"
)

// withinPct fails the test if got is more than pct% away from want.
func withinPct(t *testing.T, want, got uint64, pct float64) {
	t.Helper()
	diff := math.Abs(float64(got)-float64(want)) / float64(want) * 100
	if diff > pct {
		t.Fatalf("estimate %d is %.2f%% off from true %d (allowed %.1f%%)", got, diff, want, pct)
	}
}

func TestAccuracyAcrossScales(t *testing.T) {
	// p=14 gives ~0.8% typical error; 5% tolerance keeps the test unflaky
	// while still catching real implementation mistakes.
	for _, n := range []uint64{100, 1_000, 10_000, 100_000, 1_000_000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			s := New()
			for i := uint64(0); i < n; i++ {
				s.Add(fmt.Sprintf("user_%d", i))
			}
			withinPct(t, n, s.Estimate(), 5)
		})
	}
}

func TestDuplicatesDoNotInflate(t *testing.T) {
	s := New()
	for round := 0; round < 10; round++ {
		for i := 0; i < 5000; i++ {
			s.Add(fmt.Sprintf("user_%d", i))
		}
	}
	// 50,000 Adds, but only 5,000 distinct values.
	withinPct(t, 5000, s.Estimate(), 5)
}

func TestMergeEstimatesUnion(t *testing.T) {
	a, b := New(), New()
	// a: users 0..9999, b: users 5000..14999 -> union is 15000.
	for i := 0; i < 10000; i++ {
		a.Add(fmt.Sprintf("user_%d", i))
	}
	for i := 5000; i < 15000; i++ {
		b.Add(fmt.Sprintf("user_%d", i))
	}
	a.Merge(b)
	withinPct(t, 15000, a.Estimate(), 5)
}

func TestEmptySketchEstimatesZero(t *testing.T) {
	if got := New().Estimate(); got != 0 {
		t.Fatalf("empty sketch estimates %d, want 0", got)
	}
}

func TestSerializationRoundTrip(t *testing.T) {
	s := New()
	for i := 0; i < 20000; i++ {
		s.Add(fmt.Sprintf("user_%d", i))
	}
	restored, err := FromBytes(s.Bytes())
	if err != nil {
		t.Fatalf("FromBytes: %v", err)
	}
	if restored.Estimate() != s.Estimate() {
		t.Fatalf("round trip changed the estimate: %d != %d", restored.Estimate(), s.Estimate())
	}
}

func TestFromBytesRejectsGarbage(t *testing.T) {
	for _, b := range [][]byte{nil, {}, {9, 9, 9}, make([]byte, 100)} {
		if _, err := FromBytes(b); err == nil {
			t.Fatalf("FromBytes accepted invalid input of len %d", len(b))
		}
	}
}
