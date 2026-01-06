package store


import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkHNSW_LockContention simulates high-concurrency insertions to trigger lock contention.
func BenchmarkHNSW_LockContention(b *testing.B) {
	// Setup
	dims := 32
	capacity := 100000
	h := NewArrowHNSW(nil, DefaultArrowHNSWConfig(), nil)
	h.Grow(capacity, 0)

	// Pre-fill some data to ensure graph connectivity and non-trivial traversals
	// We do this serially to avoid benchmarking setup
	prefill := 1000
	for i := 0; i < prefill; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = rand.Float32()
		}
		if err := h.InsertWithVector(uint32(i), vec, int(rand.Int31n(4))); err != nil {
			b.Fatalf("setup insert failed: %v", err)
		}
	}

	b.ResetTimer()

	// Concurrent Inserts
	concurrency := 32 // High concurrency
	totalOps := b.N
	opsPerG := totalOps / concurrency

	var wg sync.WaitGroup
	var idCounter atomic.Uint32
	idCounter.Store(uint32(prefill))

	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vec := make([]float32, dims)
			for k := 0; k < opsPerG; k++ {
				id := idCounter.Add(1)
				// Randomize vector
				for j := 0; j < dims; j++ {
					vec[j] = rand.Float32()
				}
				if err := h.InsertWithVector(id, vec, int(rand.Int31n(4))); err != nil {
					// Ignore error for bench? or panic?
					_ = err
				}
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/sec")
}
