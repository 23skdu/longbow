package index_test

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
)

// BenchmarkHNSW_LockContention simulates high-concurrency insertions to trigger lock contention.
func BenchmarkHNSW_LockContention(b *testing.B) {
	// Setup
	dims := 32
	capacity := 100000
	cfg := types.DefaultArrowHNSWConfig()
	h := index.NewArrowHNSW(nil, &cfg, nil)
	_ = h.Grow(capacity, 0)

	// Pre-fill some data to ensure graph connectivity and non-trivial traversals
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
	concurrency := 32
	totalOps := b.N
	opsPerG := totalOps / concurrency

	var wg sync.WaitGroup
	var idCounter atomic.Uint32
	idCounter.Store(uint32(prefill))

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vec := make([]float32, dims)
			for k := 0; k < opsPerG; k++ {
				id := idCounter.Add(1)
				for j := 0; j < dims; j++ {
					vec[j] = rand.Float32()
				}
				if err := h.InsertWithVector(id, vec, int(rand.Int31n(4))); err != nil {
					_ = err
				}
			}
		}()
	}
	wg.Wait()
}
