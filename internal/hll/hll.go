// Package hll is a from-scratch HyperLogLog: it estimates how many DISTINCT
// values it has seen using a few KB of memory, no matter whether that's a
// hundred values or a hundred million.
//
// Why Tally needs it: "how many unique users clicked Buy today?" cannot be
// answered from the rollup counters (those count events, not people), and
// COUNT(DISTINCT ...) over raw events re-scans an ever-growing table. Keeping
// the exact set of user ids per event name would grow without bound. A
// HyperLogLog sketch is a fixed 16 KB per (event, day), mergeable, with
// ~0.8% typical error — the same trade Redis' PFCOUNT and every serious
// analytics store makes. See docs/adr/0005.
//
// How it works, in one breath: hash every id; use the top bits of the hash to
// pick one of m registers; in that register remember the LONGEST run of
// leading zero bits ever seen. Rare long runs are evidence of many distinct
// hashes, and averaging that evidence across m independent registers turns
// "rare luck" into a tight cardinality estimate (Flajolet et al., 2007).
package hll

import (
	"errors"
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	// Precision p: m = 2^p registers. p=14 -> 16384 registers = 16 KB per
	// sketch and a typical error of 1.04/sqrt(m) ≈ 0.8%.
	Precision = 14
	m         = 1 << Precision

	formatVersion = 1
)

// Sketch is one HyperLogLog. The zero-value is not usable; call New.
type Sketch struct {
	regs []uint8
}

// New returns an empty sketch.
func New() *Sketch { return &Sketch{regs: make([]uint8, m)} }

// Add records one value. Adding the same value again never changes anything —
// that's what makes the estimate a count of DISTINCT values.
func (s *Sketch) Add(value string) {
	h := hash64(value)
	idx := h >> (64 - Precision) // top p bits pick the register
	rest := h << Precision       // remaining bits provide the evidence
	rank := uint8(bits.LeadingZeros64(rest)) + 1
	if rank > 64-Precision+1 {
		rank = 64 - Precision + 1
	}
	if rank > s.regs[idx] {
		s.regs[idx] = rank
	}
}

// Estimate returns the approximate number of distinct values added.
func (s *Sketch) Estimate() uint64 {
	sum := 0.0
	zeros := 0
	for _, r := range s.regs {
		sum += 1 / float64(uint64(1)<<r)
		if r == 0 {
			zeros++
		}
	}
	alpha := 0.7213 / (1 + 1.079/float64(m))
	e := alpha * float64(m) * float64(m) / sum

	// Small-range correction: with many empty registers, linear counting is
	// far more accurate than the raw HLL formula.
	if e <= 2.5*float64(m) && zeros > 0 {
		e = float64(m) * math.Log(float64(m)/float64(zeros))
	}
	return uint64(e + 0.5)
}

// Merge folds other into s, after which s estimates the size of the UNION of
// both sets. Register-wise max is all it takes — this is why sketches can be
// aggregated across days, workers, or shards.
func (s *Sketch) Merge(other *Sketch) {
	for i, r := range other.regs {
		if r > s.regs[i] {
			s.regs[i] = r
		}
	}
}

// Bytes serializes the sketch: [version][precision][registers...].
func (s *Sketch) Bytes() []byte {
	out := make([]byte, 2+m)
	out[0] = formatVersion
	out[1] = Precision
	copy(out[2:], s.regs)
	return out
}

// FromBytes restores a sketch produced by Bytes.
func FromBytes(b []byte) (*Sketch, error) {
	if len(b) != 2+m || b[0] != formatVersion || b[1] != Precision {
		return nil, errors.New("hll: invalid or incompatible sketch encoding")
	}
	s := New()
	copy(s.regs, b[2:])
	return s, nil
}

// hash64 hashes a string to 64 well-mixed bits. FNV-1a alone has weak
// avalanche behavior for HLL's bit-pattern needs, so its output is passed
// through a splitmix64 finalizer. Deterministic across processes on purpose:
// sketches built by different workers must agree on where each id lands.
func hash64(value string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(value))
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
