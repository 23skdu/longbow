package store

import (
	"fmt"
	"testing"
)

func BenchmarkShardedHNSWSearchParallel(b *testing.B) {
	dims := 128
	numVectors := 10000
	config := DefaultShardedHNSWConfig()
	config.NumShards = 8 // Fixed shards for consistent benchmarking

	// Initialize index
	idx := NewShardedHNSW(config, nil)

	// Pre-fill with vectors
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = float32(i + j)
		}
		_, _ = idx.AddVector(Location{BatchIdx: 0, RowIdx: i}, vec)
	}

	query := make([]float32, dims)
	for i := 0; i < dims; i++ {
		query[i] = 1.0
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = idx.SearchVectors(query, 10)
		}
	})

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "searches/sec")
}

func BenchmarkShardedHNSWSearch_VariousK(b *testing.B) {
	dims := 128
	numVectors := 10000
	config := DefaultShardedHNSWConfig()
	idx := NewShardedHNSW(config, nil)

	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = float32(i + j)
		}
		_, _ = idx.AddVector(Location{BatchIdx: 0, RowIdx: i}, vec)
	}

	query := make([]float32, dims)
	for i := 0; i < dims; i++ {
		query[i] = 1.0
	}

	for _, k := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("k=%d", k), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = idx.SearchVectors(query, k)
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "searches/sec")
		})
	}
}
