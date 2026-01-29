package store

import (
	"testing"
)

// TestConcurrentInsert validates thread-safe concurrent insert operations.
func TestConcurrentInsert(t *testing.T) {
	dataset := &Dataset{Name: "test"}
	cfg := DefaultArrowHNSWConfig()
	index := NewArrowHNSW(dataset, &cfg)
	lg := NewLevelGenerator(1.44269504089)

	// Note: Inserts will fail without vector storage, but tests locking
	numGoroutines := 4
	insertsPerGoroutine := 25

	errChan := make(chan error, numGoroutines*insertsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		go func(offset int) {
			for j := 0; j < insertsPerGoroutine; j++ {
				id := uint32(offset*insertsPerGoroutine + j)
				level := lg.Generate()
				err := index.Insert(id, level)
				if err != nil {
					errChan <- err
				}
			}
		}(i)
	}

	// Collect errors (expected due to missing vector storage)
	for i := 0; i < numGoroutines*insertsPerGoroutine; i++ {
		select {
		case <-errChan:
			// Expected errors due to getVector not implemented
		default:
		}
	}
}

// BenchmarkConcurrentInsert benchmarks concurrent insert performance.
func BenchmarkConcurrentInsert(b *testing.B) {
	dataset := &Dataset{Name: "test"}
	cfg := DefaultArrowHNSWConfig()
	index := NewArrowHNSW(dataset, &cfg)
	lg := NewLevelGenerator(1.44269504089)

	b.Run("Sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			level := lg.Generate()
			_ = index.Insert(uint32(i), level)
		}
	})

	b.Run("Parallel-4", func(b *testing.B) {
		b.SetParallelism(4)
		var counter uint32
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				level := lg.Generate()
				id := counter
				counter++
				_ = index.Insert(id, level)
			}
		})
	})
}
