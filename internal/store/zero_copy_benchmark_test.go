package store_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// generateRandomF16Vectors creates random float16 vectors.
func generateRandomF16Vectors(count, dim int) [][]float16.Num {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	vecs := make([][]float16.Num, count)
	for i := 0; i < count; i++ {
		vecs[i] = make([]float16.Num, dim)
		for j := 0; j < dim; j++ {
			vecs[i][j] = float16.New(rng.Float32())
		}
	}
	return vecs
}

func BenchmarkHNSW_ZeroCopy_Float16(b *testing.B) {
	// Configuration
	numVectors := 5000
	dim := 128
	k := 10

	// 1. Prepare Data
	vecsF16 := generateRandomF16Vectors(numVectors, dim)

	// Create Index
	config := store.DefaultArrowHNSWConfig()
	config.M = 16
	config.EfConstruction = 100
	config.Float16Enabled = true
	config.DataType = store.VectorTypeFloat16

	idx := store.NewArrowHNSW(nil, config, nil)
	// Populate
	start := time.Now()
	for i := 0; i < numVectors; i++ {
		// Use InsertWithVector generic
		idx.InsertWithVector(uint32(i), vecsF16[i], -1)
	}
	b.Logf("Inserted %d vectors in %v", numVectors, time.Since(start))

	b.ResetTimer()

	// 2. Run Benchmark
	query := vecsF16[0]

	b.Run("Search/ZeroCopy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Zero-copy search
			_, err := idx.Search(query, k, 100, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Compare with converted Float32 query (simulating old behavior)
	queryF32 := make([]float32, dim)
	for i, v := range query {
		queryF32[i] = v.Float32()
	}

	b.Run("Search/LegacyConverted", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Passing []float32 triggers conversion in resolveHNSWComputer
			_, err := idx.Search(queryF32, k, 100, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
