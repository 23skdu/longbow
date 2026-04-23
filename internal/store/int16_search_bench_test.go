package store_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/internal/store/types"
)

func generateRandomInt16Vectors(count, dim int) [][]int16 {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	vecs := make([][]int16, count)
	for i := 0; i < count; i++ {
		vecs[i] = make([]int16, dim)
		for j := 0; j < dim; j++ {
			vecs[i][j] = int16(rng.Intn(65536) - 32768)
		}
	}
	return vecs
}

func generateRandomInt64Vectors(count, dim int) [][]int64 {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	vecs := make([][]int64, count)
	for i := 0; i < count; i++ {
		vecs[i] = make([]int64, dim)
		for j := 0; j < dim; j++ {
			vecs[i][j] = int64(rng.Int63n(1<<62) - (1 << 61))
		}
	}
	return vecs
}

func generateRandomFloat32Vectors(count, dim int) [][]float32 {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	vecs := make([][]float32, count)
	for i := 0; i < count; i++ {
		vecs[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vecs[i][j] = rng.Float32()
		}
	}
	return vecs
}

func BenchmarkSearch_Int16(b *testing.B) {
	for _, numVectors := range []int{1000, 5000, 10000} {
		for _, dim := range []int{64, 128, 384} {
			b.Run(fmt.Sprintf("Vectors_%d_Dim_%d", numVectors, dim), func(b *testing.B) {
				vecs := generateRandomInt16Vectors(numVectors, dim)

				config := store.DefaultArrowHNSWConfig()
				config.M = 16
				config.EfConstruction = 100
				config.DataType = types.VectorTypeInt16

				idx := store.NewArrowHNSW(nil, &config, nil)

				start := time.Now()
				for i := 0; i < numVectors; i++ {
					_ = idx.InsertWithVector(uint32(i), vecs[i], -1)
				}
				b.Logf("Inserted %d int16 vectors (dim=%d) in %v", numVectors, dim, time.Since(start))

				query := vecs[0]

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := idx.Search(context.Background(), query, 10, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkSearch_Int64(b *testing.B) {
	for _, numVectors := range []int{1000, 5000, 10000} {
		for _, dim := range []int{64, 128, 384} {
			b.Run(fmt.Sprintf("Vectors_%d_Dim_%d", numVectors, dim), func(b *testing.B) {
				vecs := generateRandomInt64Vectors(numVectors, dim)

				config := store.DefaultArrowHNSWConfig()
				config.M = 16
				config.EfConstruction = 100
				config.DataType = types.VectorTypeInt64

				idx := store.NewArrowHNSW(nil, &config, nil)

				start := time.Now()
				for i := 0; i < numVectors; i++ {
					_ = idx.InsertWithVector(uint32(i), vecs[i], -1)
				}
				b.Logf("Inserted %d int64 vectors (dim=%d) in %v", numVectors, dim, time.Since(start))

				query := vecs[0]

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := idx.Search(context.Background(), query, 10, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkSearch_Float32(b *testing.B) {
	for _, numVectors := range []int{1000, 5000, 10000} {
		for _, dim := range []int{64, 128, 384} {
			b.Run(fmt.Sprintf("Vectors_%d_Dim_%d", numVectors, dim), func(b *testing.B) {
				vecs := generateRandomFloat32Vectors(numVectors, dim)

				config := store.DefaultArrowHNSWConfig()
				config.M = 16
				config.EfConstruction = 100
				config.DataType = types.VectorTypeFloat32

				idx := store.NewArrowHNSW(nil, &config, nil)

				start := time.Now()
				for i := 0; i < numVectors; i++ {
					_ = idx.InsertWithVector(uint32(i), vecs[i], -1)
				}
				b.Logf("Inserted %d float32 vectors (dim=%d) in %v", numVectors, dim, time.Since(start))

				query := vecs[0]

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := idx.Search(context.Background(), query, 10, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkSearch_Uint16(b *testing.B) {
	for _, numVectors := range []int{1000, 5000, 10000} {
		for _, dim := range []int{64, 128, 384} {
			b.Run(fmt.Sprintf("Vectors_%d_Dim_%d", numVectors, dim), func(b *testing.B) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				vecs := make([][]uint16, numVectors)
				for i := 0; i < numVectors; i++ {
					vecs[i] = make([]uint16, dim)
					for j := 0; j < dim; j++ {
						vecs[i][j] = uint16(rng.Intn(65536))
					}
				}

				config := store.DefaultArrowHNSWConfig()
				config.M = 16
				config.EfConstruction = 100
				config.DataType = types.VectorTypeUint16

				idx := store.NewArrowHNSW(nil, &config, nil)

				start := time.Now()
				for i := 0; i < numVectors; i++ {
					_ = idx.InsertWithVector(uint32(i), vecs[i], -1)
				}
				b.Logf("Inserted %d uint16 vectors (dim=%d) in %v", numVectors, dim, time.Since(start))

				query := vecs[0]

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := idx.Search(context.Background(), query, 10, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestSearch_Int16VsInt64(t *testing.T) {
	numVectors := 100
	dim := 128

	vecsI16 := generateRandomInt16Vectors(numVectors, dim)
	vecsI64 := generateRandomInt64Vectors(numVectors, dim)

	configI16 := store.DefaultArrowHNSWConfig()
	configI16.M = 16
	configI16.EfConstruction = 100
	configI16.DataType = types.VectorTypeInt16

	configI64 := store.DefaultArrowHNSWConfig()
	configI64.M = 16
	configI64.EfConstruction = 100
	configI64.DataType = types.VectorTypeInt64

	idxI16 := store.NewArrowHNSW(nil, &configI16, nil)
	idxI64 := store.NewArrowHNSW(nil, &configI64, nil)

	idxI16.SetDimension(dim)
	idxI64.SetDimension(dim)

	for i := 0; i < numVectors; i++ {
		_ = idxI16.InsertWithVector(uint32(i), vecsI16[i], -1)
		_ = idxI64.InsertWithVector(uint32(i), vecsI64[i], -1)
	}

	for i := 0; i < 10; i++ {
		_, _ = idxI16.Search(context.Background(), vecsI16[i%numVectors], 5, nil)
		_, _ = idxI64.Search(context.Background(), vecsI64[i%numVectors], 5, nil)
	}

	startI16 := time.Now()
	for i := 0; i < 100; i++ {
		_, _ = idxI16.Search(context.Background(), vecsI16[i%numVectors], 5, nil)
	}
	timeI16 := time.Since(startI16)

	startI64 := time.Now()
	for i := 0; i < 100; i++ {
		_, _ = idxI64.Search(context.Background(), vecsI64[i%numVectors], 5, nil)
	}
	timeI64 := time.Since(startI64)

	t.Logf("int16: %v for 100 searches, int64: %v for 100 searches", timeI16, timeI64)

	resultsI16, err := idxI16.Search(context.Background(), vecsI16[0], 5, nil)
	if err != nil {
		t.Fatalf("int16 search failed: %v", err)
	}

	resultsI64, err := idxI64.Search(context.Background(), vecsI64[0], 5, nil)
	if err != nil {
		t.Fatalf("int64 search failed: %v", err)
	}

	t.Logf("int16 results: %v", resultsI16)
	t.Logf("int64 results: %v", resultsI64)

	// Basic sanity: at least one search should return results for 100 indexed vectors
	if len(resultsI16) == 0 && len(resultsI64) == 0 {
		t.Fatal("both int16 and int64 searches returned no results - indexing may have failed")
	}
}