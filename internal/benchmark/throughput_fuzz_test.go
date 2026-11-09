package benchmark

import (
	"math"
	"testing"
	"time"
)

// computeThroughput mirrors the throughput math used by the throughput and
// latency benchmarks: ops/sec = ops / elapsed, bytes/sec = ops/sec * size.
func computeThroughput(ops int, elapsed time.Duration, size int) (opsPerSec, bytesPerSec float64) {
	opsPerSec = float64(ops) / elapsed.Seconds()
	bytesPerSec = opsPerSec * float64(size)
	return opsPerSec, bytesPerSec
}

func FuzzThroughputMetrics(f *testing.F) {
	f.Add(100, int64(time.Second), 1024)
	f.Add(0, int64(time.Second), 0)
	f.Add(1, int64(time.Nanosecond), 1)
	f.Add(1_000_000, int64(time.Millisecond), 4096)
	f.Add(-5, int64(time.Second), 100) // out of range: negative ops
	f.Add(10, int64(0), 10)            // out of range: zero elapsed

	f.Fuzz(func(t *testing.T, ops int, elapsedNs int64, size int) {
		if ops < 0 || size < 0 {
			return
		}
		if ops > 1<<40 || size > 1<<40 {
			return
		}
		if elapsedNs <= 0 || elapsedNs > 1<<50 {
			return
		}

		elapsed := time.Duration(elapsedNs)
		opsPerSec, bytesPerSec := computeThroughput(ops, elapsed, size)

		// Results must be finite and non-negative.
		if math.IsNaN(opsPerSec) || math.IsInf(opsPerSec, 0) {
			t.Fatalf("opsPerSec not finite: %v", opsPerSec)
		}
		if math.IsNaN(bytesPerSec) || math.IsInf(bytesPerSec, 0) {
			t.Fatalf("bytesPerSec not finite: %v", bytesPerSec)
		}
		if opsPerSec < 0 || bytesPerSec < 0 {
			t.Fatalf("negative throughput: ops=%v bytes=%v", opsPerSec, bytesPerSec)
		}
		if ops > 0 && opsPerSec <= 0 {
			t.Fatalf("positive ops produced zero throughput: %v", opsPerSec)
		}

		// Consistency: bytes/sec is ops/sec scaled by the operation size.
		if want := opsPerSec * float64(size); bytesPerSec != want {
			t.Fatalf("bytesPerSec = %v, want %v", bytesPerSec, want)
		}

		// Monotonicity: more ops (same elapsed) never lowers ops/sec...
		nextOps, _ := computeThroughput(ops+1, elapsed, size)
		if nextOps < opsPerSec {
			t.Fatalf("ops/sec not monotonic in ops: %v -> %v", opsPerSec, nextOps)
		}
		// ...and a larger operation size never lowers bytes/sec.
		_, nextBytes := computeThroughput(ops, elapsed, size+1)
		if nextBytes < bytesPerSec {
			t.Fatalf("bytes/sec not monotonic in size: %v -> %v", bytesPerSec, nextBytes)
		}
	})
}
