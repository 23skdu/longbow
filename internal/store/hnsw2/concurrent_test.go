package hnsw2

import (
	"sync"
	"testing"
	"time"
	
	"github.com/23skdu/longbow/internal/store"
)

// TestConcurrentSearch validates thread-safe concurrent search operations.
func TestConcurrentSearch(t *testing.T) {
	dataset := &store.Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultConfig())
	
	// Create a simple index with a few nodes
	// Note: This test validates the locking mechanism works
	// Full concurrent search testing requires vector storage integration
	
	query := []float32{1.0, 2.0, 3.0}
	
	// Run concurrent searches
	var wg sync.WaitGroup
	numGoroutines := 10
	numSearches := 100
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numSearches; j++ {
				_, err := index.Search(query, 10, 20)
				if err != nil {
					t.Errorf("concurrent search failed: %v", err)
				}
			}
		}()
	}
	
	wg.Wait()
}

// TestConcurrentSearchAndInsert validates concurrent search during inserts.
func TestConcurrentSearchAndInsert(t *testing.T) {
	dataset := &store.Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultConfig())
	
	query := []float32{1.0, 2.0, 3.0}
	
	var wg sync.WaitGroup
	
	// Searchers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = index.Search(query, 10, 20)
				time.Sleep(time.Microsecond)
			}
		}()
	}
	
	// Inserters (will fail without vector storage, but tests locking)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = index.Insert(uint32(id*10+j), 0)
				time.Sleep(time.Microsecond * 10)
			}
		}(i)
	}
	
	wg.Wait()
}

// BenchmarkConcurrentSearch benchmarks concurrent search performance.
func BenchmarkConcurrentSearch(b *testing.B) {
	dataset := &store.Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultConfig())
	
	query := []float32{1.0, 2.0, 3.0}
	
	b.Run("Sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = index.Search(query, 10, 20)
		}
	})
	
	b.Run("Parallel-4", func(b *testing.B) {
		b.SetParallelism(4)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = index.Search(query, 10, 20)
			}
		})
	})
	
	b.Run("Parallel-8", func(b *testing.B) {
		b.SetParallelism(8)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = index.Search(query, 10, 20)
			}
		})
	})
}

// BenchmarkSearchLatency benchmarks search latency distribution.
func BenchmarkSearchLatency(b *testing.B) {
	dataset := &store.Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultConfig())
	
	query := make([]float32, 384)
	for i := range query {
		query[i] = float32(i)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = index.Search(query, 10, 20)
	}
}

// BenchmarkInsertThroughput benchmarks insert throughput.
func BenchmarkInsertThroughput(b *testing.B) {
	dataset := &store.Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultConfig())
	
	lg := NewLevelGenerator(1.44269504089)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		level := lg.Generate()
		_ = index.Insert(uint32(i), level)
	}
}
