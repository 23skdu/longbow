package metrics

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// FuzzNextPowerOfTwo checks the shard-count rounding helper: the result is
// always a power of two, never smaller than the request, never more than
// double it, and is a fixed point.
func FuzzNextPowerOfTwo(f *testing.F) {
	for _, n := range []int{0, 1, 2, 3, 4, 5, 1000, 1 << 20} {
		f.Add(n)
	}

	f.Fuzz(func(t *testing.T, n int) {
		// nextPowerOfTwo doubles until p >= n; beyond 1<<40 the result is
		// meaningless as a shard count and approaches the shift overflow
		// that would spin the loop forever.
		if n < 0 || n > 1<<40 {
			return
		}
		p := nextPowerOfTwo(n)
		if p < 1 {
			t.Fatalf("nextPowerOfTwo(%d) = %d, want >= 1", n, p)
		}
		if p&(p-1) != 0 {
			t.Errorf("nextPowerOfTwo(%d) = %d, not a power of two", n, p)
		}
		if p < n {
			t.Errorf("nextPowerOfTwo(%d) = %d, smaller than the input", n, p)
		}
		if n > 0 && p > 2*n {
			t.Errorf("nextPowerOfTwo(%d) = %d, more than double the input", n, p)
		}
		if q := nextPowerOfTwo(p); q != p {
			t.Errorf("nextPowerOfTwo is not idempotent: %d -> %d -> %d", n, p, q)
		}
	})
}

// fuzzCounterValues decodes up to 64 float64 values from seed/fuzz bytes.
func fuzzCounterValues(b []byte) []float64 {
	n := len(b) / 8
	if n > 64 {
		n = 64
	}
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		out[i] = math.Float64frombits(binary.LittleEndian.Uint64(b[i*8:]))
	}
	return out
}

// fuzzCounterSeedBytes packs values for f.Add.
func fuzzCounterSeedBytes(vals ...float64) []byte {
	b := make([]byte, 8*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint64(b[i*8:], math.Float64bits(v))
	}
	return b
}

func newFuzzCounter(t *testing.T, shards int) *ShardedCounter {
	t.Helper()
	target := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "longbow_fuzz_sharded_counter_total",
		Help: "Fuzz target accumulator for FuzzShardedCounterAccumulate",
	})
	return NewShardedCounter(target, ShardedCounterOptions{
		Name:          "fuzz",
		Shards:        shards,
		FlushInterval: time.Hour, // never started: no background flusher
	})
}

// fuzzAssertPanic requires fn to panic with exactly want.
func fuzzAssertPanic(t *testing.T, want any, fn func()) {
	t.Helper()
	defer func() {
		if got := recover(); got != want {
			t.Errorf("panic = %v, want %v", got, want)
		}
	}()
	fn()
}

// FuzzShardedCounterAccumulate checks the accumulator math: Pending equals
// the reference sum of the added values (exactly for integral inputs),
// Flush publishes the same total and zeroes the accumulator, and the shard
// layout is a bounded power of two. Negative values must be rejected with
// the documented panic; NaN/Inf and huge magnitudes are out of range for
// the exactness assertions and are skipped.
func FuzzShardedCounterAccumulate(f *testing.F) {
	f.Add(8, fuzzCounterSeedBytes(1, 2.5, 3, 0))
	f.Add(0, fuzzCounterSeedBytes(10))            // zero shards -> GOMAXPROCS
	f.Add(1, fuzzCounterSeedBytes(-1))            // negative -> panic contract
	f.Add(3, fuzzCounterSeedBytes(0.1, 0.2, 0.3)) // fractional path (CAS)
	f.Add(1024, fuzzCounterSeedBytes())           // no values at all
	f.Add(5, fuzzCounterSeedBytes(math.Inf(1)))   // out of range
	f.Add(2, fuzzCounterSeedBytes(1e6, 1, -1e6))  // negative at the magnitude bound

	f.Fuzz(func(t *testing.T, shards int, raw []byte) {
		if shards < 0 || shards > 1024 {
			return
		}
		if len(raw) > 8*64 {
			return
		}
		values := fuzzCounterValues(raw)

		// Classify the input first: non-finite values and magnitudes beyond
		// 1e6 fall outside the exactness assertions below.
		for _, v := range values {
			if math.IsNaN(v) || math.IsInf(v, 0) || v > 1e6 || v < -1e6 {
				return
			}
		}

		// Negative values are contract violations: both Add and AddInt must
		// panic with the documented sentinel.
		for _, v := range values {
			if v < 0 {
				c := newFuzzCounter(t, shards)
				fuzzAssertPanic(t, ErrShardedCounterDecrease, func() { c.Add(v) })
				fuzzAssertPanic(t, ErrShardedCounterDecrease, func() { c.AddInt(-1) })
				return
			}
		}

		c := newFuzzCounter(t, shards)

		// Layout invariants: bounded power of two, stable identity.
		s := c.Shards()
		if s < 1 || s&(s-1) != 0 {
			t.Errorf("Shards() = %d, want a power of two >= 1", s)
		}
		if shards > 0 && (s < shards || s > 1024) {
			t.Errorf("Shards() = %d for requested %d, want within [%d, 1024]", s, shards, shards)
		}
		if c.Name() != "fuzz" {
			t.Errorf("Name() = %q, want %q", c.Name(), "fuzz")
		}
		if c.FlushInterval() != time.Hour {
			t.Errorf("FlushInterval() = %v, want %v", c.FlushInterval(), time.Hour)
		}

		// Accumulate and compare against a simple reference sum.
		want := 0.0
		for _, v := range values {
			c.Add(v)
			want += v
		}
		got := c.Pending()
		tol := 1e-9 * math.Max(1, math.Abs(want))
		if diff := math.Abs(got - want); diff > tol {
			t.Errorf("Pending() = %v, want %v (diff %v > tol %v)", got, want, diff, tol)
		}
		allIntegral := true
		for _, v := range values {
			if v != math.Trunc(v) {
				allIntegral = false
				break
			}
		}
		if allIntegral && got != want {
			t.Errorf("Pending() = %v, want exact %v for integral inputs", got, want)
		}

		// Flush publishes the same total and drains the accumulators.
		flushed := c.Flush()
		if diff := math.Abs(flushed - want); diff > tol {
			t.Errorf("Flush() = %v, want %v (diff %v)", flushed, want, diff)
		}
		if p := c.Pending(); p != 0 {
			t.Errorf("Pending() after Flush = %v, want 0", p)
		}
		if p := c.Flush(); p != 0 {
			t.Errorf("second Flush() = %v, want 0", p)
		}
		if target := testutil.ToFloat64(c.target); math.Abs(target-want) > tol {
			t.Errorf("target counter = %v, want %v", target, want)
		}
	})
}

// FuzzLabelValuesKey checks that the label-value key encoding round-trips:
// splitting the key on the separator must reproduce the exact tuple, which
// is what keeps distinct label tuples from colliding.
func FuzzLabelValuesKey(f *testing.F) {
	f.Add("a,b,c")
	f.Add("")
	f.Add("single")
	f.Add("with,space and\x01control")
	f.Add("xff,\xff,marker") // separator inside a value is out of range
	f.Add("long,value")

	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 512 {
			return
		}
		// The separator must never appear inside a label value, otherwise
		// two different tuples could encode to the same key.
		if strings.Contains(s, "\xff") {
			return
		}
		values := strings.Split(s, ",")
		key := labelValuesKey(values)

		got := strings.Split(key, "\xff")
		if len(got) != len(values) {
			t.Fatalf("key %q decoded to %d parts, want %d", key, len(got), len(values))
		}
		for i := range values {
			if got[i] != values[i] {
				t.Errorf("part %d = %q, want %q", i, got[i], values[i])
			}
		}
	})
}
